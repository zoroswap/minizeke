use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, account::AccountId, asset::FungibleAsset, keystore::FilesystemKeyStore,
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
    message_broker::message_broker::{AmmEvent, MessageBroker, PoolStateEvent},
    miden_env::MidenNetwork,
    oracle_sse::{fetch_price_feeds, oracle_base_url, validate_asset_feeds},
    order::{Order, OrderFailureReason, OrderUpdate, Orders, Processed},
    pool::{
        LpLedger, PoolBalances, PoolMetadata, PoolSettings, PoolState,
        fetch_account_storage_from_rpc,
    },
    test_utils::{
        consume_all_notes_for, deposit_liquidity_on_vault, get_client, get_pool_client,
        vault_foreign_account, withdraw_liquidity_from_vault,
    },
    vault::{
        checkpoint_lp_entitlement_on_vault, get_vault_lp_info, user_pool_from_storage,
        vault_user_registration,
    },
};

/// How often the operator checkpoints LP entitlements on the vault (fees accrued since the
/// last checkpoint become self-custodially withdrawable).
const DEFAULT_LP_CHECKPOINT_INTERVAL_SECS: u64 = 600;

pub struct MidenExecution {
    client: Client<FilesystemKeyStore>,
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
    operator_id: AccountId,
    orders: Orders,
    /// Vault-assigned pool shard per trader, filled lazily from vault storage.
    user_pools: HashMap<AccountId, AccountId>,
    pool_states: HashMap<AccountId, PoolState>,
    /// Per-depositor LP shares, server-side only. On-chain the vault keeps entitlement /
    /// withdrawn counters per (asset, lp) so withdrawals stay self-custodial.
    lp_ledger: LpLedger,
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
        let operator_id = deployment.operator_account_id;
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

        let mut client = get_client().await?;
        client.ensure_genesis_in_place().await?;
        client.sync_state().await?;

        let mut pool_client = get_pool_client().await?;
        pool_client.ensure_genesis_in_place().await?;
        pool_client.sync_state().await?;

        println!("Clients ready.");

        // Attach idempotently: `import_account_by_id` fetches the public account from the
        // network and overwrites any already-tracked copy. The vault must stay untracked
        // in the pool client (see the `pool_client` field docs).
        client.import_account_by_id(operator_id).await?;
        client.import_account_by_id(vault_id).await?;
        for asset in &assets {
            client.import_account_by_id(asset.faucet_id).await?;
        }
        for pool_id in &pools {
            pool_client.import_account_by_id(*pool_id).await?;
        }
        client.sync_state().await?;
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
        let mut lp_ledger = LpLedger::default();

        for record in &deployment.deposits {
            let pool = pool_states.get_mut(&record.faucet_id).ok_or_else(|| {
                anyhow!(
                    "deposit record references unknown faucet {}",
                    record.faucet_id.to_hex()
                )
            })?;
            let (lp_amount, new_supply, new_balances) =
                pool.get_deposit_lp_amount_out(U256::from(record.amount))?;
            pool.update_state(new_balances, new_supply);
            if let Some(lp_id) = deployment.lp_account_id {
                lp_ledger.mint(record.faucet_id, lp_id, lp_amount.saturating_to::<u64>());
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
            operator_id,
            client,
            pool_client,
            message_broker,
            prover_timeout,
            assets,
            user_pools: HashMap::new(),
            orders: Orders::default(),
            pool_states,
            lp_ledger,
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

        let checkpoint_interval_secs = env::var("LP_CHECKPOINT_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_LP_CHECKPOINT_INTERVAL_SECS);
        let mut checkpoint_interval =
            tokio::time::interval(Duration::from_secs(checkpoint_interval_secs));
        // don't fire immediately on startup
        checkpoint_interval.reset();

        loop {
            tokio::select! {
                _ = checkpoint_interval.tick() => {
                    if let Err(e) = self.checkpoint_lp_entitlements().await {
                        error!("LP entitlement checkpoint failed: {e:?}");
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

    /// Deposits `asset` from `lp_id` into the vault (DEPOSIT note: the vault takes custody
    /// and credits the LP's on-chain entitlement with the principal), then mints LP shares
    /// into the server-side ledger and applies the curve's deposit math to the pool state.
    ///
    /// Returns the amount of LP shares minted.
    pub async fn deposit_liquidity(
        &mut self,
        lp_id: AccountId,
        asset: FungibleAsset,
    ) -> Result<u64> {
        let faucet_id = asset.faucet_id();
        let pool = self
            .pool_states
            .get(&faucet_id)
            .ok_or_else(|| anyhow!("no pool for asset {}", faucet_id.to_hex()))?;

        let (lp_amount, new_lp_total_supply, new_balances) =
            pool.get_deposit_lp_amount_out(U256::from(asset.amount().as_u64()))?;
        let lp_amount = lp_amount.saturating_to::<u64>();

        // on-chain leg first: only account for the deposit once custody has moved
        deposit_liquidity_on_vault(&mut self.client, self.vault_id, lp_id, asset).await?;

        if let Some(pool) = self.pool_states.get_mut(&faucet_id) {
            pool.update_state(new_balances, new_lp_total_supply);
        }
        self.lp_ledger.mint(faucet_id, lp_id, lp_amount);
        self.broadcast_pool_states()?;

        info!(
            lp = %lp_id.to_hex(),
            asset = %faucet_id.to_hex(),
            amount = asset.amount().as_u64(),
            lp_shares = lp_amount,
            "Liquidity deposited"
        );
        Ok(lp_amount)
    }

    /// Redeems `lp_shares` of `lp_id`'s position in the `faucet_id` pool: burns the shares
    /// server-side, makes sure the on-chain entitlement covers the payout (checkpointing it
    /// if fees have accrued past the principal), then runs the self-custodial WITHDRAW note
    /// and consumes the P2ID payout as the LP.
    ///
    /// Returns the paid-out asset amount.
    pub async fn withdraw_liquidity(
        &mut self,
        lp_id: AccountId,
        faucet_id: AccountId,
        lp_shares: u64,
    ) -> Result<u64> {
        let pool = self
            .pool_states
            .get(&faucet_id)
            .ok_or_else(|| anyhow!("no pool for asset {}", faucet_id.to_hex()))?;

        let (payout, new_lp_total_supply, new_balances) =
            pool.get_withdraw_asset_amount_out(U256::from(lp_shares))?;
        let payout = payout.saturating_to::<u64>();

        // validates the LP owns enough shares
        self.lp_ledger.burn(faucet_id, lp_id, lp_shares)?;

        // if accrued fees pushed the position's value past the checkpointed entitlement,
        // raise it so the vault accepts the withdrawal
        let lp_info = get_vault_lp_info(&self.client, self.vault_id, faucet_id, lp_id).await?;
        if lp_info.withdrawable() < payout {
            checkpoint_lp_entitlement_on_vault(
                &mut self.client,
                self.operator_id,
                self.vault_id,
                faucet_id,
                lp_id,
                lp_info.withdrawn + payout,
            )
            .await?;
        }

        let payout_asset = FungibleAsset::new(faucet_id, payout)
            .map_err(|e| anyhow!("invalid payout asset: {e:?}"))?;
        withdraw_liquidity_from_vault(&mut self.client, self.vault_id, lp_id, payout_asset).await?;
        // consume the P2ID payout as the LP
        consume_all_notes_for(&mut self.client, lp_id).await?;

        if let Some(pool) = self.pool_states.get_mut(&faucet_id) {
            pool.update_state(new_balances, new_lp_total_supply);
        }
        self.broadcast_pool_states()?;

        info!(
            lp = %lp_id.to_hex(),
            asset = %faucet_id.to_hex(),
            lp_shares,
            payout,
            "Liquidity withdrawn"
        );
        Ok(payout)
    }

    /// Periodic operator maintenance: for every LP position, raises the vault's entitlement
    /// counter to `withdrawn + current value of the LP's shares` so accrued fees become
    /// self-custodially withdrawable even if the operator later disappears.
    pub async fn checkpoint_lp_entitlements(&mut self) -> Result<()> {
        for faucet_id in self.assets.iter().map(|asset| asset.faucet_id) {
            let Some(pool) = self.pool_states.get(&faucet_id) else {
                continue;
            };
            let pool = *pool;

            for (lp_id, shares) in self.lp_ledger.depositors(faucet_id) {
                if shares == 0 {
                    continue;
                }
                // quote (not apply): value of the shares at the current pool state
                let (value, _, _) = pool.get_withdraw_asset_amount_out(U256::from(shares))?;
                let value = value.saturating_to::<u64>();

                let lp_info =
                    get_vault_lp_info(&self.client, self.vault_id, faucet_id, lp_id).await?;
                let target = lp_info.withdrawn.saturating_add(value);
                if target <= lp_info.entitlement {
                    continue; // entitlement only ever goes up
                }
                checkpoint_lp_entitlement_on_vault(
                    &mut self.client,
                    self.operator_id,
                    self.vault_id,
                    faucet_id,
                    lp_id,
                    target,
                )
                .await?;
                info!(
                    lp = %lp_id.to_hex(),
                    asset = %faucet_id.to_hex(),
                    entitlement = target,
                    "LP entitlement checkpointed"
                );
            }
        }
        Ok(())
    }

    fn broadcast_pool_states(&self) -> Result<()> {
        self.message_broker.broadcast_pool_state(PoolStateEvent {
            pool_states: self.pool_states.clone(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        })?;
        Ok(())
    }

    pub fn lp_ledger(&self) -> &LpLedger {
        &self.lp_ledger
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
