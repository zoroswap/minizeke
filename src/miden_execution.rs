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
use tracing::{error, info};

use alloy_primitives::U256;

use crate::{
    assembly_utils::{link_math, link_operator, link_pool},
    deployment::Deployment,
    execution_script::make_exec_script,
    intent::Intent,
    message_broker::message_broker::{AmmEvent, MessageBroker, PoolStateEvent},
    miden_env::MidenNetwork,
    order::{Order, OrderFailureReason, OrderUpdate, Orders, Processed},
    pool::{LpLedger, PoolState, fetch_account_storage_from_rpc, get_user_trades_slot_name},
    test_utils::{
        consume_all_notes_for, deposit_liquidity_on_vault, get_client, get_pool_client,
        vault_foreign_account, withdraw_liquidity_from_vault,
    },
    vault::{checkpoint_lp_entitlement_on_vault, get_vault_lp_info, vault_user_registration},
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
    asset0: AccountId,
    asset1: AccountId,
    pool_id: AccountId,
    vault_id: AccountId,
    orders: Orders,
    /// Vault-assigned user index per trader, filled lazily from the vault's registration
    /// map (orders arrive fully signed; the server never holds user keys).
    user_indices: HashMap<AccountId, u16>,
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
        let vault_id = deployment.vault_id;
        let pool_id = deployment.pool_id;
        let asset0 = deployment.asset0;
        let asset1 = deployment.asset1;
        info!(
            vault = %vault_id.to_hex(),
            pool = %pool_id.to_hex(),
            asset0 = %asset0.to_hex(),
            asset1 = %asset1.to_hex(),
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
        client.import_account_by_id(vault_id).await?;
        client.import_account_by_id(asset0).await?;
        client.import_account_by_id(asset1).await?;
        pool_client.import_account_by_id(pool_id).await?;
        client.sync_state().await?;
        pool_client.sync_state().await?;
        println!(
            "Attached to vault {} and pool {}.",
            vault_id.to_hex(),
            pool_id.to_hex()
        );

        // Rebuild server-side pool state deterministically by replaying the recorded
        // liquidity deposits through the curve's deposit math.
        // Known limitation: pool state mutated by past swaps is not recovered on restart —
        // pool-state persistence is a follow-up.
        let mut pool_states = HashMap::with_capacity(2);
        pool_states.insert(asset0, PoolState::default());
        pool_states.insert(asset1, PoolState::default());
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
            pool_id,
            vault_id,
            client,
            pool_client,
            message_broker,
            prover_timeout,
            asset0,
            asset1,
            user_indices: HashMap::new(),
            orders: Orders::default(),
            pool_states,
            lp_ledger,
        })
    }

    /// Resolves a trader's vault-assigned user index, caching the answer. On a cache miss
    /// the vault's registration map is read over RPC; an unregistered user is an error.
    async fn resolve_user_index(&mut self, user_id: AccountId) -> Result<u16> {
        if let Some(index) = self.user_indices.get(&user_id) {
            return Ok(*index);
        }
        let storage = fetch_account_storage_from_rpc(self.vault_id).await?;
        let (index, _pubkey) = vault_user_registration(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} is not registered on the vault", user_id.to_hex()))?;
        let index = u16::try_from(index)
            .map_err(|_| anyhow!("user index {index} out of range for {}", user_id.to_hex()))?;
        self.user_indices.insert(user_id, index);
        Ok(index)
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

    /// Execute a processed batch on the pool. On failure, mark every order in the
    /// batch as failed and still release the processing gate so the engine never
    /// deadlocks waiting for a settlement that will never come.
    pub async fn handle_batch(&mut self, orders: Vec<Order<Processed>>) {
        info!(
            count = orders.len(),
            "Received processed batch for execution"
        );
        let started = Instant::now();
        if let Err(e) = self.execute_on_pool(orders.clone()).await {
            error!(
                elapsed = ?started.elapsed(),
                "Batch execution failed: {e:?}"
            );
            for order in orders {
                let failed = order.failed(OrderFailureReason::ExecutionError, None);
                let _ = self
                    .message_broker
                    .broadcast_order_update(OrderUpdate::Failed(failed));
            }
            // Always release the gate held by the Processing engine, even on failure,
            // so the pipeline never deadlocks.
            let _ = self.message_broker.broadcast_amm(AmmEvent::OrdersExecuted);
        }
    }

    async fn execute_on_pool(&mut self, orders: Vec<Order<Processed>>) -> Result<()> {
        info!(
            cycle = self.cycle,
            count = orders.len(),
            "Executing batch on pool"
        );
        self.cycle += 1;

        if orders.is_empty() {
            // Nothing to execute; release the processing gate immediately.
            self.message_broker
                .broadcast_amm(AmmEvent::OrdersExecuted)?;
            return Ok(());
        }

        let instant = Instant::now();

        // let pool_state_deltas = vec![
        //     PoolStateDelta {
        //         pool_index: sell_pool_index,
        //         set_amount: sell_pool_balance,
        //     },
        //     PoolStateDelta {
        //         pool_index: buy_pool_index,
        //         set_amount: buy_pool_balance,
        //     },
        // ];

        let mut intents = Vec::with_capacity(orders.len());
        let mut advice_data = vec![];
        let mut fpi_asset_user_pairs = Vec::with_capacity(orders.len());
        let mut executable = Vec::with_capacity(orders.len());

        let asset0 = self.asset0;
        for order in orders {
            let user_id = order.user_id();
            // an unregistered (or unresolvable) user fails only its own order
            let user_index = match self.resolve_user_index(user_id).await {
                Ok(index) => index,
                Err(e) => {
                    error!(user = %user_id.to_hex(), "Failed to resolve user index: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError, None);
                    self.message_broker
                        .broadcast_order_update(OrderUpdate::Failed(failed))?;
                    continue;
                }
            };
            let details = order.details();

            let buy_idx = if details.asset_out.eq(&asset0) { 0 } else { 1 };
            let sell_idx = if details.asset_in.eq(&asset0) { 0 } else { 1 };
            let amount_out = order.execution_result().amount_out;

            // the swap FPIs into the vault for the sell asset's totals + the registration
            fpi_asset_user_pairs.push((details.asset_in, user_id));

            let user_suffix: u64 = user_id.suffix().as_canonical_u64();
            let user_prefix: u64 = user_id.prefix().as_u64();
            let user_slot_key = get_user_trades_slot_name(user_index);

            let intent = Intent {
                user_suffix,
                user_prefix,
                user_key_prefix: user_slot_key.id().prefix().as_canonical_u64(),
                user_key_suffix: user_slot_key.id().suffix().as_canonical_u64(),
                sell_idx,
                buy_idx,
                sell_amount: details.amount_in,
                buy_amount: amount_out,
            };

            let signed_order = order.signed_order();
            let pubkey = order.pubkey();

            let msg = intent.message_word();
            let pk_comm: Word = pubkey.to_commitment().into();

            info!(
                "pk_comm: {pk_comm:?}, msg: {:?}, user suffix: {}, user prefix: {}",
                intent.message_word(),
                intent.user_suffix,
                intent.user_prefix
            );

            let prepared: Vec<Felt> = signed_order.to_prepared_signature(msg); // [PK[9], SIG[17]]

            info!("prepared len: {}", prepared.len());

            // advice_data.extend_from_slice(msg.as_elements()); // MSG (4) — consumed first
            // advice_data.extend_from_slice(pk_comm.as_elements()); // PK_COMM (4)
            advice_data.extend_from_slice(&prepared); // PK[9], SIG[17]

            intents.push(intent);
            executable.push(order);
        }

        if executable.is_empty() {
            // every order failed individually; release the processing gate
            self.message_broker
                .broadcast_amm(AmmEvent::OrdersExecuted)?;
            return Ok(());
        }

        let tx_script = make_exec_script(intents);

        info!("SCRIPT \n\n{tx_script}\n\n");

        // refresh the pool client's anchor block so the vault FPI proof covers the
        // latest funding state
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
        let pool_id = self.pool_id;

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
        self.pool_client
            .apply_transaction(&tx_result, submission_height)
            .await?;
        println!("Elapsed: {prove_elapsed:?}");
        self.pool_client.sync_state().await?;

        // let res = self
        //     .client
        //     .submit_new_transaction(pool_id, tx_req)
        //     .await
        //     .map_err(|_| {
        //         anyhow!(
        //             "transaction prove/submit timed out after {:?}",
        //             self.prover_timeout
        //         )
        //     })?;

        info!(
            elapsed = ?submit_started.elapsed(),
            "Transaction proven and submitted"
        );

        let tx_hash = tx_result.id().to_hex().to_string();

        self.pool_client.sync_state().await?;
        info!(%tx_hash, "Client state synced");

        let mut executed_count = 0usize;
        for order in executable {
            let execution_result = order.execution_result();
            let executed = order.executed(tx_hash.clone(), execution_result);
            self.message_broker
                .broadcast_order_update(OrderUpdate::Executed(executed))?;
            executed_count += 1;
        }

        // Release the gate held by the Processing engine. We do not wait for
        // on-chain settlement; the order lifecycle ends at Executed.
        self.message_broker
            .broadcast_amm(AmmEvent::OrdersExecuted)?;

        info!(
            count = executed_count,
            elapsed = ?instant.elapsed(),
            "Batch executed"
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
        for faucet_id in [self.asset0, self.asset1] {
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
        self.pool_id
    }

    pub fn vault_id(&self) -> AccountId {
        self.vault_id
    }

    pub fn asset0(&self) -> AccountId {
        self.asset0
    }

    pub fn asset1(&self) -> AccountId {
        self.asset1
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
