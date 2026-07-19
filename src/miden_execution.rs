use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, Deserializable, Serializable,
    account::AccountId,
    keystore::FilesystemKeyStore,
    transaction::{TransactionRequestBuilder, TransactionStoreUpdate},
};
use miden_core::{Felt, Word};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use alloy_primitives::U256;

use crate::{
    assembly_utils::{link_math, link_operator, link_pool},
    deployment::{AssetInfo, Deployment},
    execution_script::make_exec_script,
    execution_store::ExecutionStore,
    intent::is_expired_at,
    lp_store::LpStore,
    message_broker::message_broker::{AmmEvent, MessageBroker},
    miden_env::MidenNetwork,
    oracle_sse::{fetch_price_feeds, oracle_base_url, validate_asset_feeds},
    order::{Order, OrderFailureReason, OrderUpdate, Orders, Processed},
    pool::{
        PoolBalances, PoolMetadata, PoolSettings, PoolState, fetch_vault_user_placement_storage,
        fetch_vault_user_registration_storage,
    },
    pool_registry::PoolRegistry,
    test_utils::{get_pool_client_for, vault_foreign_account},
    vault::{user_pool_from_storage, vault_user_registration},
};

/// Miden vault FPI allows at most 64 storage-map keys per slot; stay under that.
const MAX_FPI_ASSET_USER_PAIRS: usize = 48;
const DEFAULT_MAX_ORDERS_PER_SHARD_TX: usize = 16;

/// Soft cap on swaps packed into one pool tx. Override with `MAX_ORDERS_PER_SHARD_TX`.
pub fn max_orders_per_shard_tx() -> usize {
    env::var("MAX_ORDERS_PER_SHARD_TX")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_MAX_ORDERS_PER_SHARD_TX)
        .clamp(1, MAX_FPI_ASSET_USER_PAIRS)
}

fn execute_fpi_probe_enabled() -> bool {
    matches!(
        env::var("EXECUTE_FPI_PROBE").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes")
    )
}

struct ShardSubmit {
    tx_hash: String,
    orders: Vec<Order<Processed>>,
}

pub struct MidenExecution {
    /// One client (own store) per pool so shards can execute concurrently.
    /// The vault must stay untracked: for tracked foreign accounts the client fetches
    /// with `VaultFetch::IfChangedFrom`, which can omit the asset list and break the
    /// kernel foreign-account commitment check once the vault holds assets.
    pool_clients: HashMap<AccountId, Client<FilesystemKeyStore>>,
    message_broker: Arc<MessageBroker>,
    prover_timeout: Duration,
    cycle: u64,
    assets: Vec<AssetInfo>,
    pool_registry: Arc<PoolRegistry>,
    vault_id: AccountId,
    orders: Orders,
    /// Vault-assigned pool shard per trader, filled lazily from vault storage.
    user_pools: HashMap<AccountId, AccountId>,
    pool_states: HashMap<AccountId, PoolState>,
    execution_store: Arc<ExecutionStore>,
    worker_id: String,
}

impl MidenExecution {
    pub async fn initialize(message_broker: Arc<MessageBroker>) -> Result<Self> {
        const DEFAULT_TX_PROVER_TIMEOUT_SECS: u64 = 30;

        let network = MidenNetwork::from_env();
        let tx_prover_url = env::var("TX_PROVER_URL")
            .ok()
            .or_else(|| network.tx_prover_url());
        let tx_prover_timeout_secs = env::var("TX_PROVER_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_TX_PROVER_TIMEOUT_SECS);

        let prover_timeout = Duration::from_secs(tx_prover_timeout_secs);

        if let Some(ref url) = tx_prover_url {
            info!(
                network = network.as_str(),
                prover = %url,
                timeout_secs = tx_prover_timeout_secs,
                "Using Miden network with remote prover"
            );
        } else {
            info!(
                network = network.as_str(),
                "Using Miden network with local prover"
            );
        }

        // The server never deploys anything; it attaches to the accounts recorded by the
        // `spawn` / `deposit_pools` binaries. `Deployment::load` errors with a pointer to
        // `cargo run --bin spawn` when the file is missing.
        let deployment = Deployment::load()?;
        let vault_id = deployment.vault_id;
        let assets = deployment.assets.clone();
        let pools = deployment.pools.clone();
        let oracle_url = oracle_base_url()?;
        let price_feeds = fetch_price_feeds(&oracle_url).await?;
        validate_asset_feeds(&assets, &price_feeds)?;
        info!(
            vault = %vault_id.to_hex(),
            assets = assets.len(),
            pools = pools.len(),
            "Loaded deployment config"
        );

        let mut pool_clients = HashMap::with_capacity(pools.len());
        for pool_id in &pools {
            let mut client = get_pool_client_for(*pool_id).await?;
            client.ensure_genesis_in_place().await?;
            // Vault must stay untracked (see `pool_clients` field docs).
            client.import_account_by_id(*pool_id).await?;
            client.sync_state().await?;
            pool_clients.insert(*pool_id, client);
        }
        println!(
            "Attached to vault {} and {} pool shards (per-pool clients).",
            vault_id.to_hex(),
            pools.len()
        );

        // Rebuild the baseline from liquidity operations, then replace assets for which a
        // finalized execution snapshot exists.
        let mut pool_states = HashMap::with_capacity(assets.len());
        for asset in &assets {
            pool_states.insert(
                asset.faucet_id,
                PoolState::new(
                    PoolSettings::default(),
                    PoolBalances::default(),
                    0,
                    PoolMetadata {
                        name: "Deployment asset",
                        asset_decimals: asset.decimals,
                    },
                ),
            );
        }
        for record in &deployment.deposits {
            let pool = pool_states.get_mut(&record.faucet_id).ok_or_else(|| {
                anyhow!(
                    "deposit record references unknown faucet {}",
                    record.faucet_id.to_hex()
                )
            })?;
            let (_, new_supply, new_balances) =
                pool.get_deposit_lp_amount_out(U256::from(record.amount))?;
            pool.update_state(new_balances, new_supply);
        }
        let lp_store = LpStore::open_from_env()?;
        for operation in lp_store.applied_operations()? {
            let faucet_id = AccountId::from_hex(&operation.faucet_id)?;
            let pool = pool_states.get_mut(&faucet_id).ok_or_else(|| {
                anyhow!(
                    "LP journal references unknown faucet {}",
                    operation.faucet_id
                )
            })?;
            if operation.kind == "deposit" {
                let (_, supply, balances) =
                    pool.get_deposit_lp_amount_out(U256::from(operation.asset_amount))?;
                pool.update_state(balances, supply);
            } else {
                let (_, supply, balances) =
                    pool.get_withdraw_asset_amount_out(U256::from(operation.lp_shares))?;
                pool.update_state(balances, supply);
            }
        }
        let execution_store = Arc::new(ExecutionStore::open_from_env()?);
        let restored_states = execution_store.latest_pool_states()?;
        let restored_count = restored_states.len();
        for (faucet_id, state) in restored_states {
            if pool_states.contains_key(&faucet_id) {
                pool_states.insert(faucet_id, state);
            }
        }
        info!(
            deposits = deployment.deposits.len(),
            restored_snapshots = restored_count,
            "Rebuilt pool states from liquidity and finalized swap snapshots"
        );

        Ok(Self {
            cycle: 0,
            pool_registry: Arc::new(PoolRegistry::new(pools)),
            vault_id,
            pool_clients,
            message_broker,
            prover_timeout,
            assets,
            user_pools: HashMap::new(),
            orders: Orders::default(),
            pool_states,
            execution_store,
            worker_id: format!("miden-{}", uuid::Uuid::new_v4()),
        })
    }

    /// Import + sync a pool client when the shard is listed but not yet attached locally.
    async fn ensure_pool_attached(&mut self, pool_id: AccountId) -> Result<()> {
        if self.pool_clients.contains_key(&pool_id) {
            self.pool_registry
                .acknowledge(pool_id, crate::pool_registry::PoolWorker::Execution);
            return Ok(());
        }
        if !self.pool_registry.contains(&pool_id)
            && !self.pool_registry.ensure_from_deployment(pool_id)?
        {
            return Err(anyhow!(
                "user is assigned to unlisted pool {}",
                pool_id.to_hex()
            ));
        }
        let mut client = get_pool_client_for(pool_id).await?;
        client.ensure_genesis_in_place().await?;
        // Vault must stay untracked (see `pool_clients` field docs).
        client.import_account_by_id(pool_id).await?;
        client.sync_state().await?;
        self.pool_clients.insert(pool_id, client);
        self.pool_registry
            .acknowledge(pool_id, crate::pool_registry::PoolWorker::Execution);
        info!(
            pool = %pool_id.to_hex(),
            "attached pool client for published shard"
        );
        Ok(())
    }

    async fn attach_pending_pools(&mut self) {
        for pool_id in self.pool_registry.pending_attach() {
            if let Err(error) = self.ensure_pool_attached(pool_id).await {
                warn!(
                    pool = %pool_id.to_hex(),
                    %error,
                    "failed to attach published pool shard"
                );
            }
        }
    }

    /// Resolves a trader's vault-assigned pool shard, caching the answer.
    async fn resolve_user_pool(&mut self, user_id: AccountId) -> Result<AccountId> {
        if let Some(pool_id) = self.user_pools.get(&user_id) {
            return Ok(*pool_id);
        }
        let storage = fetch_vault_user_placement_storage(self.vault_id, user_id).await?;
        vault_user_registration(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} is not registered on the vault", user_id.to_hex()))?;
        let pool_id = user_pool_from_storage(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} has no assigned pool", user_id.to_hex()))?;
        if let Err(error) = self.ensure_pool_attached(pool_id).await {
            return Err(anyhow!(
                "user {} is assigned to unlisted pool {}: {error}",
                user_id.to_hex(),
                pool_id.to_hex()
            ));
        }
        self.user_pools.insert(user_id, pool_id);
        Ok(pool_id)
    }

    pub async fn start(&mut self) -> Result<()> {
        // Processing may have recovered an LP note after this worker was initialized but
        // before the startup gate opened. Reload the atomic snapshots before accepting
        // execution work so those recovered curve mutations cannot be missed.
        for (faucet_id, state) in self.execution_store.latest_pool_states()? {
            if self.pool_states.contains_key(&faucet_id) {
                self.pool_states.insert(faucet_id, state);
            }
        }
        let mut orders_rx = self.message_broker.subscribe_order_updates();
        let mut pool_state_rx = self.message_broker.subscribe_pool_state();
        let mut processed_rx = self.message_broker.subscribe_processed_batch();
        let mut registry_rx = self.pool_registry.subscribe();
        let mut durable_poll = tokio::time::interval(Duration::from_millis(250));

        loop {
            tokio::select! {
                _ = durable_poll.tick() => {
                    self.poll_durable_work().await;
                    self.attach_pending_pools().await;
                }
                changed = registry_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.attach_pending_pools().await;
                }
                batch = processed_rx.recv() => {
                    match batch {
                        Ok(_) => {
                            // Wake immediately when Processing commits a batch; durable
                            // claim remains authoritative (payload comes from the store).
                            self.claim_and_handle_pending_batch().await;
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!(lagged = n, "processed_batch lagged; polling durable store");
                            self.claim_and_handle_pending_batch().await;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                orders = orders_rx.recv() => {
                    match orders {
                        Ok(ev) => self.orders.apply_order_update(ev),
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("orders lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                pool_states = pool_state_rx.recv() => {
                    match pool_states {
                        Ok(ev) => {
                            for (faucet_id, new_pool_state) in ev.pool_states.iter() {
                                self.pool_states.insert(*faucet_id, *new_pool_state);
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("pool_states lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            };
        }
        Err(anyhow!("Termination of miden execution."))
    }

    async fn poll_durable_work(&mut self) {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if let Err(error) = self.retry_pending_local_applies(now).await {
            error!(%error, "Failed to retry local transaction applies");
        }
        self.claim_and_handle_pending_batch().await;
    }

    async fn claim_and_handle_pending_batch(&mut self) {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        match self
            .execution_store
            .claim_pending_batch(&self.worker_id, now, 600_000)
        {
            Ok(Some(batch)) => self.handle_durable_batch(batch.id, batch.orders).await,
            Ok(None) => {}
            Err(error) => error!(%error, "Failed to claim durable execution batch"),
        }
    }

    /// Route one logical batch to its pool shards and release the processing gate once all
    /// shard submissions have succeeded or failed.
    pub async fn handle_durable_batch(
        &mut self,
        batch_id: uuid::Uuid,
        orders: Vec<Order<Processed>>,
    ) {
        self.handle_batch_inner(Some(batch_id), orders).await;
    }

    /// Direct execution helper retained for chain-level integration tests. Production work enters
    /// through `handle_durable_batch` after the execution store has committed the batch.
    pub async fn handle_batch(&mut self, orders: Vec<Order<Processed>>) {
        self.handle_batch_inner(None, orders).await;
    }

    async fn handle_batch_inner(
        &mut self,
        batch_id: Option<uuid::Uuid>,
        orders: Vec<Order<Processed>>,
    ) {
        info!(
            batch_id = ?batch_id,
            trades = orders.len(),
            "Execution engine claimed trading batch"
        );
        let started = Instant::now();
        let mut shard_orders: HashMap<AccountId, Vec<Order<Processed>>> = HashMap::new();
        for order in orders {
            let user_id = order.user_id();
            match self.resolve_user_pool(user_id).await {
                Ok(pool_id) => shard_orders.entry(pool_id).or_default().push(order),
                Err(e) => {
                    error!(user = %user_id.to_hex(), "Failed to resolve user pool: {e:?}");
                    if let Some(batch_id) = batch_id
                        && let Err(store_error) = self.execution_store.mark_pre_submission_failed(
                            batch_id,
                            &self.worker_id,
                            &[order.id],
                            &e.to_string(),
                            chrono::Utc::now().timestamp_millis() as u64,
                        )
                    {
                        error!(order_id = %order.id, %store_error, "Failed to persist placement failure");
                    }
                }
            }
        }

        // Distinct pools execute concurrently (one client each). Chunks within a pool
        // stay sequential (shared account nonce).
        let max_orders = max_orders_per_shard_tx();
        let mut pool_jobs = Vec::new();
        for pool_id in self.pool_registry.pools() {
            let Some(shard) = shard_orders.remove(&pool_id) else {
                continue;
            };
            let Some(client) = self.pool_clients.remove(&pool_id) else {
                error!(pool = %pool_id.to_hex(), "No pool client configured for shard");
                continue;
            };
            pool_jobs.push((pool_id, client, shard));
        }
        let pools_in_batch = pool_jobs.len();
        let execution_store = self.execution_store.clone();
        let message_broker = self.message_broker.clone();
        let worker_id = self.worker_id.clone();
        let vault_id = self.vault_id;
        let mut cycle_base = self.cycle;

        let pool_outcomes = futures_util::future::join_all(pool_jobs.into_iter().map(
            |(pool_id, mut client, shard)| {
                let execution_store = execution_store.clone();
                let message_broker = message_broker.clone();
                let worker_id = worker_id.clone();
                let cycle = {
                    let c = cycle_base;
                    cycle_base = cycle_base.saturating_add(1);
                    c
                };
                async move {
                    let mut chunk_results = Vec::new();
                    let mut local_cycle = cycle;
                    for chunk in chunk_orders_for_fpi(shard, max_orders) {
                        let chunk_len = chunk.len();
                        let result = execute_shard_on_client(
                            &mut client,
                            &execution_store,
                            &message_broker,
                            &worker_id,
                            vault_id,
                            batch_id,
                            pool_id,
                            local_cycle,
                            chunk,
                        )
                        .await;
                        local_cycle = local_cycle.saturating_add(1);
                        chunk_results.push((chunk_len, result));
                    }
                    (pool_id, client, chunk_results)
                }
            },
        ))
        .await;
        self.cycle = cycle_base;

        let mut submitted_txs = Vec::new();
        let mut submitted_trades = 0_usize;
        let mut failed_chunks = 0_usize;
        let mut chunks = 0_usize;
        for (pool_id, client, chunk_results) in pool_outcomes {
            self.pool_clients.insert(pool_id, client);
            for (chunk_len, result) in chunk_results {
                chunks += 1;
                match result {
                    Ok(ShardSubmit { tx_hash, orders }) => {
                        if tx_hash.is_empty() || orders.is_empty() {
                            continue;
                        }
                        submitted_trades += orders.len();
                        submitted_txs.push(tx_hash.clone());
                        self.publish_submitted_notifications(&orders, &tx_hash);
                    }
                    Err(e) => {
                        failed_chunks += 1;
                        error!(
                            pool = %pool_id.to_hex(),
                            count = chunk_len,
                            "Shard execution failed: {e:?}"
                        );
                    }
                }
            }
        }

        let submit_ms = started.elapsed().as_millis();
        let ms_per_trade = if submitted_trades == 0 {
            0
        } else {
            submit_ms / submitted_trades as u128
        };

        let Some(batch_id) = batch_id else {
            info!(
                trades = submitted_trades,
                txs = submitted_txs.len(),
                chunks,
                pools = pools_in_batch,
                failed_chunks,
                submit_ms,
                ms_per_trade,
                "Direct integration batch submission finished"
            );
            return;
        };
        let now = chrono::Utc::now().timestamp_millis() as u64;
        match self.execution_store.fail_remaining_batched_orders(
            batch_id,
            &self.worker_id,
            "shard preparation ended before transaction submission",
            now,
        ) {
            Ok(order_ids) if !order_ids.is_empty() => {
                failed_chunks += 1;
                warn!(
                    %batch_id,
                    orders = order_ids.len(),
                    "Failed residual unsubmitted orders before reconciliation"
                );
            }
            Ok(_) => {}
            Err(error) => {
                error!(%batch_id, %error, "Failed to clean up unsubmitted batch orders");
            }
        }

        let mut reconciliation =
            self.execution_store
                .begin_reconciliation(batch_id, &self.worker_id, now);
        if let Err(first_error) = &reconciliation {
            error!(
                %batch_id,
                error = %first_error,
                "Initial batch reconciliation failed; attempting unsubmitted-order recovery"
            );
            let recovery_reason = format!("batch reconciliation recovery: {first_error}");
            reconciliation = match self.execution_store.fail_remaining_batched_orders(
                batch_id,
                &self.worker_id,
                &recovery_reason,
                chrono::Utc::now().timestamp_millis() as u64,
            ) {
                Ok(order_ids) => {
                    warn!(
                        %batch_id,
                        orders = order_ids.len(),
                        "Recovered residual unsubmitted orders"
                    );
                    self.execution_store.begin_reconciliation(
                        batch_id,
                        &self.worker_id,
                        chrono::Utc::now().timestamp_millis() as u64,
                    )
                }
                Err(error) => Err(error),
            };
        }

        match reconciliation {
            Ok(true) => {
                // Admit gate may release while finality runs. A batch containing only
                // pre-submission failures is finalized by begin_reconciliation and
                // replayed through the outbox.
                let _ = self.message_broker.broadcast_amm(AmmEvent::BatchSubmitted);
            }
            Ok(false) => {
                warn!(%batch_id, "Execution batch lease was no longer owned");
                return;
            }
            Err(error) => {
                error!(
                    %batch_id,
                    %error,
                    "Failed to commit execution batch outcome after recovery"
                );
                return;
            }
        }
        info!(
            batch_id = %batch_id,
            trades = submitted_trades,
            chunks,
            pools = pools_in_batch,
            failed_chunks,
            tx_hashes = %submitted_txs.join(","),
            submit_ms,
            ms_per_trade,
            "Execution batch submitted; awaiting finality"
        );
    }

    fn publish_submitted_notifications(&self, orders: &[Order<Processed>], tx_hash: &str) {
        for order in orders {
            let update = OrderUpdate::Submitted(order.clone().submitted(tx_hash.to_owned()));
            let _ = self.message_broker.broadcast_order_update(update);
        }
    }

    /// Retry `apply_transaction_update` on the execute/prove clients when submit-time
    /// apply failed. Chain confirmation runs on `FinalityObserver`.
    async fn retry_pending_local_applies(&mut self, now: u64) -> Result<()> {
        let retry_millis = env::var("FINALITY_RETRY_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(2)
            .saturating_mul(1_000);
        let submissions: Vec<_> = self
            .execution_store
            .submitted_transactions()?
            .into_iter()
            .filter(|submission| {
                !submission.local_applied
                    && now.saturating_sub(submission.updated_at) >= retry_millis
            })
            .collect();
        for submission in submissions {
            let Some(client) = self.pool_clients.get_mut(&submission.pool_id) else {
                self.execution_store.record_reconciliation_attempt(
                    &submission.tx_hash,
                    Some("no pool client for submission pool"),
                    now,
                )?;
                continue;
            };
            match TransactionStoreUpdate::read_from_bytes(&submission.transaction_update) {
                Ok(update) => match client.apply_transaction_update(update).await {
                    Ok(()) => self
                        .execution_store
                        .mark_submission_local_applied(&submission.tx_hash, now)?,
                    Err(error) => {
                        self.execution_store.record_reconciliation_attempt(
                            &submission.tx_hash,
                            Some(&format!("local apply: {error}")),
                            now,
                        )?;
                    }
                },
                Err(error) => {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        &format!("persisted transaction update is invalid: {error}"),
                        now,
                    )?;
                }
            }
        }
        Ok(())
    }

    pub fn pool_id(&self) -> AccountId {
        self.pool_registry.pools()[0]
    }

    pub fn pools(&self) -> Vec<AccountId> {
        self.pool_registry.pools()
    }

    pub fn pool_registry(&self) -> Arc<PoolRegistry> {
        self.pool_registry.clone()
    }

    pub fn vault_id(&self) -> AccountId {
        self.vault_id
    }

    pub fn asset0(&self) -> AccountId {
        self.assets[0].faucet_id
    }

    pub fn asset1(&self) -> AccountId {
        self.assets[1].faucet_id
    }

    pub fn assets(&self) -> Vec<AssetInfo> {
        self.assets.clone()
    }

    pub fn pool_states(&self) -> HashMap<AccountId, PoolState> {
        self.pool_states.clone()
    }
}

async fn filter_live_orders_for_client(
    execution_store: &ExecutionStore,
    message_broker: &MessageBroker,
    worker_id: &str,
    batch_id: Option<uuid::Uuid>,
    orders: Vec<Order<Processed>>,
) -> Vec<Order<Processed>> {
    let now_secs = chrono::Utc::now().timestamp() as u64;
    let mut live = Vec::with_capacity(orders.len());
    for order in orders {
        if is_expired_at(order.intent().expires_at, now_secs) {
            warn!(order_id = %order.id, "Signed intent expired before execution");
            if let Some(batch_id) = batch_id
                && let Err(store_error) = execution_store.mark_pre_submission_failed(
                    batch_id,
                    worker_id,
                    &[order.id],
                    "signed intent expired before execution",
                    chrono::Utc::now().timestamp_millis() as u64,
                )
            {
                error!(order_id = %order.id, %store_error, "Failed to persist expiry failure");
            }
            let update = OrderUpdate::Failed(order.failed(OrderFailureReason::Expired, None));
            let _ = message_broker.broadcast_order_update(update);
            continue;
        }
        live.push(order);
    }
    live
}

async fn fail_chunk_orders(
    execution_store: &ExecutionStore,
    worker_id: &str,
    batch_id: Option<uuid::Uuid>,
    orders: &[Order<Processed>],
    reason: &str,
) {
    let Some(batch_id) = batch_id else {
        return;
    };
    let now = chrono::Utc::now().timestamp_millis() as u64;
    for order in orders {
        if let Err(store_error) = execution_store.mark_pre_submission_failed(
            batch_id,
            worker_id,
            &[order.id],
            reason,
            now,
        ) {
            error!(order_id = %order.id, %store_error, "Failed to persist pre-submission failure");
        }
    }
}

async fn sync_before_execution(client: &mut Client<FilesystemKeyStore>) -> Result<()> {
    const SYNC_TIMEOUT: Duration = Duration::from_secs(30);
    const TIP_RACE_RETRIES: usize = 3;

    for attempt in 0..TIP_RACE_RETRIES {
        match tokio::time::timeout(SYNC_TIMEOUT, client.sync_state()).await {
            Ok(Ok(_)) => return Ok(()),
            Ok(Err(error)) => {
                let message = format!("{error:#}").to_ascii_lowercase();
                let tip_race =
                    message.contains("block_to") && message.contains("greater than chain tip");
                if tip_race && attempt + 1 < TIP_RACE_RETRIES {
                    warn!(
                        attempt = attempt + 1,
                        "Miden sync raced the chain tip; retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }

                warn!(%error, "sync_state failed before execution; falling back to sync_chain");
                return tokio::time::timeout(SYNC_TIMEOUT, client.sync_chain())
                    .await
                    .map_err(|_| anyhow!("sync_chain timed out after sync_state failure"))?
                    .map_err(|fallback| {
                        anyhow!("sync_state failed: {error:#}; sync_chain failed: {fallback:#}")
                    })
                    .map(|_| ());
            }
            Err(_) => {
                warn!("sync_state timed out before execution; falling back to sync_chain");
                return tokio::time::timeout(SYNC_TIMEOUT, client.sync_chain())
                    .await
                    .map_err(|_| anyhow!("sync_chain timed out after sync_state timeout"))?
                    .map_err(|error| anyhow!("sync_chain failed: {error:#}"))
                    .map(|_| ());
            }
        }
    }

    unreachable!("sync retry loop always returns")
}

#[allow(clippy::too_many_arguments)]
async fn execute_shard_on_client(
    pool_client: &mut Client<FilesystemKeyStore>,
    execution_store: &ExecutionStore,
    message_broker: &MessageBroker,
    worker_id: &str,
    vault_id: AccountId,
    batch_id: Option<uuid::Uuid>,
    pool_id: AccountId,
    cycle: u64,
    orders: Vec<Order<Processed>>,
) -> Result<ShardSubmit> {
    let orders =
        filter_live_orders_for_client(execution_store, message_broker, worker_id, batch_id, orders)
            .await;
    if orders.is_empty() {
        return Ok(ShardSubmit {
            tx_hash: String::new(),
            orders: Vec::new(),
        });
    }

    let instant = Instant::now();
    let mut intents = Vec::with_capacity(orders.len());
    let mut advice_data = vec![];
    let mut fpi_asset_user_pairs = Vec::with_capacity(orders.len());
    let mut unique_users = Vec::new();

    for order in &orders {
        let user_id = order.user_id();
        let details = order.details();
        fpi_asset_user_pairs.push((details.asset_in, user_id));
        if !unique_users.contains(&user_id) {
            unique_users.push(user_id);
        }

        let intent = order.intent();
        let msg = intent.message_word();
        let prepared: Vec<Felt> = order.signed_order().to_prepared_signature(msg);
        advice_data.extend_from_slice(&prepared);
        intents.push(intent);
    }
    let fpi_pairs = fpi_asset_user_pairs.len();
    let unique_user_count = unique_users.len();

    let tx_script = make_exec_script(intents);
    let sync_started = Instant::now();
    if let Err(error) = sync_before_execution(pool_client).await {
        fail_chunk_orders(
            execution_store,
            worker_id,
            batch_id,
            &orders,
            &error.to_string(),
        )
        .await;
        return Err(error);
    }
    let sync_ms = sync_started.elapsed().as_millis();

    let compile_started = Instant::now();
    let tx_script = match (|| -> Result<_> {
        let cb = link_math(pool_client.code_builder())?;
        let cb = link_operator(cb)?;
        let cb = link_pool(cb)?;
        Ok(cb.compile_tx_script(tx_script)?)
    })() {
        Ok(script) => script,
        Err(error) => {
            fail_chunk_orders(
                execution_store,
                worker_id,
                batch_id,
                &orders,
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };
    let compile_ms = compile_started.elapsed().as_millis();

    let advice_map_key = Word::from([Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::ONE]);
    let tx_req = match (|| -> Result<_> {
        Ok(TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .extend_advice_map([(advice_map_key, advice_data)])
            .foreign_accounts(vec![vault_foreign_account(
                vault_id,
                &fpi_asset_user_pairs,
            )?])
            .build()?)
    })() {
        Ok(request) => request,
        Err(error) => {
            fail_chunk_orders(
                execution_store,
                worker_id,
                batch_id,
                &orders,
                &error.to_string(),
            )
            .await;
            return Err(error);
        }
    };

    let fpi_probe_ms = if execute_fpi_probe_enabled() {
        let probe_started = Instant::now();
        // Proxy for vault FPI RPC cost (targeted registration read).
        let _ = fetch_vault_user_registration_storage(vault_id, orders[0].user_id()).await;
        Some(probe_started.elapsed().as_millis())
    } else {
        None
    };

    let execute_started = Instant::now();
    let tx_result = match pool_client.execute_transaction(pool_id, tx_req).await {
        Ok(result) => result,
        Err(error) => {
            fail_chunk_orders(
                execution_store,
                worker_id,
                batch_id,
                &orders,
                &error.to_string(),
            )
            .await;
            return Err(error.into());
        }
    };
    let execute_ms = execute_started.elapsed().as_millis();
    let measurements = tx_result.executed_transaction().measurements();
    let prove_started = Instant::now();
    let proven_transaction = match pool_client
        .prove_transaction_with(&tx_result, pool_client.prover())
        .await
    {
        Ok(proven) => proven,
        Err(error) => {
            fail_chunk_orders(
                execution_store,
                worker_id,
                batch_id,
                &orders,
                &error.to_string(),
            )
            .await;
            return Err(error.into());
        }
    };
    let prove_ms = prove_started.elapsed().as_millis();
    let submit_started = Instant::now();
    let submission_height = match pool_client
        .submit_proven_transaction(proven_transaction, &tx_result)
        .await
    {
        Ok(height) => height,
        Err(error) => {
            fail_chunk_orders(
                execution_store,
                worker_id,
                batch_id,
                &orders,
                &error.to_string(),
            )
            .await;
            return Err(error.into());
        }
    };
    let submit_ms = submit_started.elapsed().as_millis();
    let apply_started = Instant::now();
    let transaction_update = pool_client
        .get_transaction_store_update(&tx_result, submission_height)
        .await?;
    let executed = tx_result.executed_transaction();
    let tx_hash = executed.id().to_hex().to_string();
    let order_ids: Vec<_> = orders.iter().map(|order| order.id).collect();
    if let Some(batch_id) = batch_id
        && !execution_store.record_submission(
            batch_id,
            worker_id,
            &tx_hash,
            &executed.id().to_bytes(),
            pool_id,
            &order_ids,
            &transaction_update.to_bytes(),
            &executed.initial_account().initial_commitment().to_bytes(),
            &executed.final_account().to_commitment().to_bytes(),
            submission_height.as_u32(),
            executed.expiration_block_num().as_u32(),
            chrono::Utc::now().timestamp_millis() as u64,
        )?
    {
        return Err(anyhow!("execution batch lease was lost after submission"));
    }
    if let Err(e) = pool_client
        .apply_transaction_update(transaction_update)
        .await
    {
        warn!(
            pool = %pool_id.to_hex(),
            transaction = %tx_result.id().to_hex(),
            "Shard transaction was submitted but local apply failed: {e:?}"
        );
    } else if batch_id.is_some()
        && let Err(error) = execution_store
            .mark_submission_local_applied(&tx_hash, chrono::Utc::now().timestamp_millis() as u64)
    {
        warn!(%tx_hash, %error, "Failed to persist local-apply completion");
    }
    let apply_ms = apply_started.elapsed().as_millis();

    let post_sync_started = Instant::now();
    if let Err(e) = pool_client.sync_state().await {
        warn!(%tx_hash, "Shard transaction submitted but client sync failed: {e:?}");
    }
    let post_sync_ms = post_sync_started.elapsed().as_millis();

    let total_ms = instant.elapsed().as_millis();
    let trades = orders.len();
    let total_cycles = measurements.total_cycles();
    let ms_per_trade = if trades == 0 {
        0
    } else {
        total_ms / trades as u128
    };
    let cycles_per_trade = if trades == 0 {
        0
    } else {
        total_cycles / trades
    };

    info!(
        cycle,
        pool = %pool_id.to_hex(),
        trades,
        fpi_pairs,
        unique_users = unique_user_count,
        tx_hash = %tx_hash,
        total_cycles,
        cycles_per_trade,
        auth_cycles = measurements.auth_procedure,
        sync_ms,
        compile_ms,
        fpi_probe_ms,
        execute_ms,
        prove_ms,
        submit_ms,
        apply_ms,
        post_sync_ms,
        total_ms,
        ms_per_trade,
        "Trading cycle: shard proven and submitted"
    );
    Ok(ShardSubmit { tx_hash, orders })
}

/// Pack orders into chunks that stay under vault FPI storage-map key limits.
fn chunk_orders_for_fpi(
    orders: Vec<Order<Processed>>,
    max_orders: usize,
) -> Vec<Vec<Order<Processed>>> {
    if orders.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = Vec::new();
    let mut pairs = Vec::new();
    for order in orders {
        let pair = (order.details().asset_in, order.user_id());
        let would_add_pair = !pairs.contains(&pair);
        let over_pair_budget = would_add_pair && pairs.len() >= MAX_FPI_ASSET_USER_PAIRS;
        let over_order_budget = current.len() >= max_orders;
        if !current.is_empty() && (over_pair_budget || over_order_budget) {
            chunks.push(std::mem::take(&mut current));
            pairs.clear();
        }
        if would_add_pair {
            pairs.push(pair);
        }
        current.push(order);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::{chunk_orders_for_fpi, max_orders_per_shard_tx};
    use crate::order::{Order, OrderExecutionResult};
    use miden_client::{account::AccountId, auth::AuthSecretKey};
    use uuid::Uuid;

    fn processed_order(user: AccountId, asset_in: AccountId) -> Order<crate::order::Processed> {
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let asset_out = AccountId::from_hex("0x1e7e8af77fc5f2f1631d5c5ce35471").unwrap();
        let intent = crate::intent::Intent::new_swap(
            user,
            asset_in,
            10,
            asset_out,
            9,
            Uuid::new_v4(),
            4_000_000_000,
        );
        let signature = key.sign(intent.message_word());
        Order::new(
            signature,
            user,
            crate::order::OrderDetails::new(asset_in, 10, asset_out, 9),
            key.public_key(),
            intent,
        )
        .start_processing()
        .processed(OrderExecutionResult { amount_out: 9 })
    }

    #[test]
    fn chunks_respect_max_orders_per_shard_tx() {
        let user = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        let btc = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let max_orders = max_orders_per_shard_tx();
        let orders = (0..(max_orders + 3))
            .map(|_| processed_order(user, btc))
            .collect();
        let chunks = chunk_orders_for_fpi(orders, max_orders);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), max_orders);
        assert_eq!(chunks[1].len(), 3);
    }
}

/// The depositor's user-id word: `[id_prefix, id_suffix, 0, 0]`. This is the raw `StorageMap` key
/// under which the operator stores/looks up this depositor's pubkey commitment (Plan 2 Q1).
pub fn user_id_word(account_id: AccountId) -> Word {
    Word::from([
        account_id.prefix().as_felt(),
        account_id.suffix(),
        0u32.into(),
        0u32.into(),
    ])
}
