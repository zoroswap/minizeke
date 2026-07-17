use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, account::AccountId, keystore::FilesystemKeyStore,
    transaction::TransactionRequestBuilder,
};
use miden_core::{Felt, Word};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use alloy_primitives::U256;

use crate::{
    assembly_utils::{link_math, link_operator, link_pool},
    deployment::{AssetInfo, Deployment},
    execution_script::make_exec_script,
    intent::Intent,
    lp_store::LpStore,
    message_broker::message_broker::{AmmEvent, MessageBroker},
    miden_env::MidenNetwork,
    oracle_sse::{fetch_price_feeds, oracle_base_url, validate_asset_feeds},
    order::{Order, OrderFailureReason, OrderUpdate, Orders, Processed},
    pool::{PoolBalances, PoolMetadata, PoolSettings, PoolState, fetch_account_storage_from_rpc},
    test_utils::{get_pool_client, vault_foreign_account},
    vault::{user_pool_from_storage, vault_user_registration},
};

pub struct MidenExecution {
    /// Separate client (own store) for pool-native swap txs. The vault must stay
    /// untracked here: for tracked foreign accounts the client fetches the vault with
    /// `VaultFetch::IfChangedFrom`, the node then omits the asset list and the
    /// reconstructed foreign account fails the kernel's commitment check once the vault
    /// holds assets.
    pool_client: Client<FilesystemKeyStore>,
    message_broker: Arc<MessageBroker>,
    prover_timeout: Duration,
    cycle: u64,
    assets: Vec<AssetInfo>,
    pools: Vec<AccountId>,
    vault_id: AccountId,
    orders: Orders,
    /// Vault-assigned pool shard per trader, filled lazily from vault storage.
    user_pools: HashMap<AccountId, AccountId>,
    pool_states: HashMap<AccountId, PoolState>,
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

        let mut pool_client = get_pool_client().await?;
        pool_client.ensure_genesis_in_place().await?;
        pool_client.sync_state().await?;

        println!("Clients ready.");

        // The vault must stay untracked in the pool client (see the `pool_client` field docs).
        for pool_id in &pools {
            pool_client.import_account_by_id(*pool_id).await?;
        }
        pool_client.sync_state().await?;
        println!(
            "Attached to vault {} and {} pool shards.",
            vault_id.to_hex(),
            pools.len()
        );

        // Rebuild server-side pool state deterministically by replaying the recorded
        // liquidity deposits through the curve's deposit math.
        // Known limitation: pool state mutated by past swaps is not recovered on restart —
        // pool-state persistence is a follow-up.
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
        info!(
            deposits = deployment.deposits.len(),
            "Rebuilt pool states from recorded deposits"
        );

        Ok(Self {
            cycle: 0,
            pools,
            vault_id,
            pool_client,
            message_broker,
            prover_timeout,
            assets,
            user_pools: HashMap::new(),
            orders: Orders::default(),
            pool_states,
        })
    }

    /// Resolves a trader's vault-assigned pool shard, caching the answer.
    async fn resolve_user_pool(&mut self, user_id: AccountId) -> Result<AccountId> {
        if let Some(pool_id) = self.user_pools.get(&user_id) {
            return Ok(*pool_id);
        }
        let storage = fetch_account_storage_from_rpc(self.vault_id).await?;
        vault_user_registration(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} is not registered on the vault", user_id.to_hex()))?;
        let pool_id = user_pool_from_storage(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} has no assigned pool", user_id.to_hex()))?;
        if !self.pools.contains(&pool_id) {
            return Err(anyhow!(
                "user {} is assigned to unlisted pool {}",
                user_id.to_hex(),
                pool_id.to_hex()
            ));
        }
        self.user_pools.insert(user_id, pool_id);
        Ok(pool_id)
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut orders_rx = self.message_broker.subscribe_order_updates();
        let mut pool_state_rx = self.message_broker.subscribe_pool_state();
        let mut processed_batch_rx = self.message_broker.subscribe_processed_batch();

        loop {
            tokio::select! {
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
                batch = processed_batch_rx.recv() => {
                    match batch {
                        Ok(orders) => self.handle_batch(orders).await,
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("processed batch lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            };
        }
        Err(anyhow!("Termination of miden execution."))
    }

    /// Route one logical batch to its pool shards and release the processing gate once all
    /// shard submissions have succeeded or failed.
    pub async fn handle_batch(&mut self, orders: Vec<Order<Processed>>) {
        info!(
            count = orders.len(),
            "Received processed batch for execution"
        );
        let started = Instant::now();
        let mut shard_orders: HashMap<AccountId, Vec<Order<Processed>>> = HashMap::new();
        for order in orders {
            let user_id = order.user_id();
            match self.resolve_user_pool(user_id).await {
                Ok(pool_id) => shard_orders.entry(pool_id).or_default().push(order),
                Err(e) => {
                    error!(user = %user_id.to_hex(), "Failed to resolve user pool: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError, None);
                    let _ = self
                        .message_broker
                        .broadcast_order_update(OrderUpdate::Failed(failed));
                }
            }
        }

        // Deployment order gives deterministic sequential shard submission.
        for pool_id in self.pools.clone() {
            let Some(shard) = shard_orders.remove(&pool_id) else {
                continue;
            };
            if let Err(e) = self.execute_shard(pool_id, shard.clone()).await {
                error!(
                    pool = %pool_id.to_hex(),
                    count = shard.len(),
                    "Shard execution failed: {e:?}"
                );
                for order in shard {
                    let failed = order.failed(OrderFailureReason::ExecutionError, None);
                    let _ = self
                        .message_broker
                        .broadcast_order_update(OrderUpdate::Failed(failed));
                }
            }
        }

        if let Err(e) = self.message_broker.broadcast_amm(AmmEvent::OrdersExecuted) {
            error!("Failed to release processing gate: {e:?}");
        }
        info!(elapsed = ?started.elapsed(), "Logical batch execution finished");
    }

    async fn execute_shard(
        &mut self,
        pool_id: AccountId,
        orders: Vec<Order<Processed>>,
    ) -> Result<()> {
        info!(
            cycle = self.cycle,
            pool = %pool_id.to_hex(),
            count = orders.len(),
            "Executing batch on pool shard"
        );
        self.cycle += 1;

        if orders.is_empty() {
            return Ok(());
        }

        let instant = Instant::now();
        let mut intents = Vec::with_capacity(orders.len());
        let mut advice_data = vec![];
        let mut fpi_asset_user_pairs = Vec::with_capacity(orders.len());

        for order in &orders {
            let user_id = order.user_id();
            let details = order.details();
            fpi_asset_user_pairs.push((details.asset_in, user_id));

            let intent = Intent {
                user_suffix: user_id.suffix().as_canonical_u64(),
                user_prefix: user_id.prefix().as_u64(),
                sell_asset_suffix: details.asset_in.suffix().as_canonical_u64(),
                sell_asset_prefix: details.asset_in.prefix().as_u64(),
                sell_amount: details.amount_in,
                buy_asset_suffix: details.asset_out.suffix().as_canonical_u64(),
                buy_asset_prefix: details.asset_out.prefix().as_u64(),
                buy_amount: details.min_amount_out,
            };

            let msg = intent.message_word();
            let prepared: Vec<Felt> = order.signed_order().to_prepared_signature(msg);
            advice_data.extend_from_slice(&prepared);
            intents.push(intent);
        }

        let tx_script = make_exec_script(intents);
        self.pool_client.sync_state().await?;

        let cb = link_math(self.pool_client.code_builder())?;
        let cb = link_operator(cb)?;
        let cb = link_pool(cb)?;
        let tx_script = cb.compile_tx_script(tx_script)?;

        let advice_map_key = Word::from([Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::ONE]);
        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .extend_advice_map([(advice_map_key, advice_data)])
            .foreign_accounts(vec![vault_foreign_account(
                self.vault_id,
                &fpi_asset_user_pairs,
            )?])
            .build()?;

        let submit_started = Instant::now();
        let tx_result = self
            .pool_client
            .execute_transaction(pool_id, tx_req)
            .await?;
        let measurements = tx_result.executed_transaction().measurements();
        info!(
            total_cycles = measurements.total_cycles(),
            auth_cycles = measurements.auth_procedure,
            "Transaction cycle count",
        );
        let prove_started = Instant::now();
        let proven_transaction = self
            .pool_client
            .prove_transaction_with(&tx_result, self.pool_client.prover())
            .await?;
        let prove_elapsed = prove_started.elapsed();
        let submission_height = self
            .pool_client
            .submit_proven_transaction(proven_transaction, &tx_result)
            .await?;
        if let Err(e) = self
            .pool_client
            .apply_transaction(&tx_result, submission_height)
            .await
        {
            warn!(
                pool = %pool_id.to_hex(),
                transaction = %tx_result.id().to_hex(),
                "Shard transaction was submitted but local apply failed: {e:?}"
            );
        }
        info!(
            elapsed = ?submit_started.elapsed(),
            prove_elapsed = ?prove_elapsed,
            "Shard transaction proven and submitted"
        );

        let tx_hash = tx_result.id().to_hex().to_string();
        if let Err(e) = self.pool_client.sync_state().await {
            warn!(%tx_hash, "Shard transaction submitted but client sync failed: {e:?}");
        }

        for order in orders {
            let execution_result = order.execution_result();
            let executed = order.executed(tx_hash.clone(), execution_result);
            if let Err(e) = self
                .message_broker
                .broadcast_order_update(OrderUpdate::Executed(executed))
            {
                error!(%tx_hash, "Failed to broadcast executed order: {e:?}");
            }
        }

        info!(
            pool = %pool_id.to_hex(),
            elapsed = ?instant.elapsed(),
            "Shard batch executed"
        );
        Ok(())
    }

    pub fn pool_id(&self) -> AccountId {
        self.pools[0]
    }

    pub fn pools(&self) -> Vec<AccountId> {
        self.pools.clone()
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
