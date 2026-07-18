use crate::{
    analytics_store::{
        AnalyticsStore, FinalizedSwap, LpCashFlow, LpCashFlowKind, OracleMark, PoolSnapshotInput,
    },
    curve::get_curve_amount_out_with_volatility_fee,
    deployment::AssetInfo,
    execution_store::{ExecutionStore, ProposedSwap, SwapFinalization},
    fee_store::{FeeStore, apply_fee_states},
    intent::is_expired_at,
    lp_store::LpStore,
    message_broker::message_broker::{
        AmmEvent, AnalyticsEvent, LpChainEvent, LpOperationKind, MessageBroker, MessageBrokerEvent,
        PoolStateEvent, UserEvent, VaultCashFlowEvent, VaultCashFlowKind,
    },
    oracle_sse::OraclePricing,
    order::{Created, Order, OrderExecutionResult, OrderFailureReason, OrderUpdate, Orders},
    pool::{
        LpLedger, PoolBalances, PoolState, fetch_account_storage_from_rpc,
        get_user_available_balance_from_pool,
    },
    vault::user_pool_from_storage,
};

use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use miden_client::account::AccountId;
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tokio::{
    select,
    sync::broadcast::{Receiver, error::RecvError},
};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct PendingSwap {
    user_id: AccountId,
    sell_faucet: AccountId,
    buy_faucet: AccountId,
    amount_in: u64,
    amount_out: u64,
    sell_before: PoolBalances,
    sell_after: PoolBalances,
    buy_before: PoolBalances,
    buy_after: PoolBalances,
    analytics_swap: FinalizedSwap,
}

pub struct Processing {
    message_broker: Arc<MessageBroker>,
    oracle_pricing: OraclePricing,
    orders: Orders,
    pool_states: HashMap<AccountId, PoolState>,
    engine_busy: bool,
    vault_id: AccountId,
    asset_ids: HashSet<AccountId>,
    pool_ids: HashSet<AccountId>,
    user_pools: HashMap<AccountId, AccountId>,
    /// Lazy per-(user, faucet) spendable-balance mirror for preflight quoting.
    /// Opening balance is fetched from chain on first miss; FUND / INIT_REDEEM and
    /// swap deltas keep it warm. On-chain FPI remains the settlement authority.
    balances: HashMap<(AccountId, AccountId), u64>,
    lp_ledger: LpLedger,
    lp_store: Arc<LpStore>,
    lp_chain_rx: Receiver<LpChainEvent>,
    vault_cashflow_rx: Receiver<VaultCashFlowEvent>,
    lp_recovery_pending: bool,
    execution_store: Arc<ExecutionStore>,
    fee_store: Arc<FeeStore>,
    pending_swaps: HashMap<Uuid, PendingSwap>,
    execution_finished: bool,
    /// Set when a reconciling batch fails; cleared after reloading pool states from store.
    pool_resync_required: bool,
    analytics_store: Arc<AnalyticsStore>,
    worker_id: String,
}

impl Processing {
    pub async fn new(
        message_broker: Arc<MessageBroker>,
        pool_states: HashMap<AccountId, PoolState>,
        vault_id: AccountId,
        assets: Vec<AssetInfo>,
        pools: Vec<AccountId>,
        lp_store: Arc<LpStore>,
        execution_store: Arc<ExecutionStore>,
        fee_store: Arc<FeeStore>,
    ) -> Result<Self> {
        let oracle_pricing = OraclePricing::new(&assets);
        let orders = Orders::default();
        let analytics_store = Arc::new(AnalyticsStore::open_from_env()?);

        // A pool snapshot may have committed immediately before a crash while the
        // separate LP journal acknowledgement did not. Reconcile those markers first
        // so the in-memory ledger is rebuilt from authoritative positions.
        for operation in lp_store.confirmed_operations()? {
            if let Some(lp_shares) = execution_store.applied_lp_shares(&operation.note_id)? {
                lp_store.apply_operation(
                    &operation.note_id,
                    lp_shares,
                    chrono::Utc::now().timestamp_millis() as u64,
                )?;
            }
        }

        let mut lp_ledger = LpLedger::default();
        for position in lp_store.positions()? {
            let lp_id = AccountId::from_hex(&position.lp_id)?;
            let faucet_id = AccountId::from_hex(&position.faucet_id)?;
            lp_ledger.mint(faucet_id, lp_id, position.shares);
        }

        let lp_chain_rx = message_broker.subscribe_lp_chain();
        let vault_cashflow_rx = message_broker.subscribe_vault_cashflow();
        let mut processing = Self {
            oracle_pricing,
            message_broker,
            orders,
            pool_states,
            engine_busy: false,
            vault_id,
            asset_ids: assets.into_iter().map(|asset| asset.faucet_id).collect(),
            pool_ids: pools.into_iter().collect(),
            user_pools: HashMap::new(),
            balances: HashMap::new(),
            lp_ledger,
            lp_store,
            lp_chain_rx,
            vault_cashflow_rx,
            lp_recovery_pending: false,
            execution_store,
            fee_store,
            pending_swaps: HashMap::new(),
            execution_finished: false,
            pool_resync_required: false,
            analytics_store,
            worker_id: format!("processing-{}", Uuid::new_v4()),
        };
        processing.recover_unapplied_lp_operations()?;
        Ok(processing)
    }

    pub fn pool_states(&self) -> HashMap<AccountId, PoolState> {
        self.pool_states.clone()
    }

    /// Returns the user's spendable balance for `faucet_id`. Uses the local mirror when
    /// warm; on a cold miss, hydrates once from chain.
    async fn balance_of(&mut self, user_id: AccountId, faucet_id: AccountId) -> Result<u64> {
        if let Some(balance) = self.balances.get(&(user_id, faucet_id)) {
            return Ok(*balance);
        }
        let balance = self.fetch_balance_from_chain(user_id, faucet_id).await?;
        self.balances.insert((user_id, faucet_id), balance);
        Ok(balance)
    }

    fn apply_vault_cashflow_event(&mut self, event: VaultCashFlowEvent) {
        apply_vault_cashflow_to_balances(&mut self.balances, &event);
    }

    async fn fetch_balance_from_chain(
        &mut self,
        user_id: AccountId,
        faucet_id: AccountId,
    ) -> Result<u64> {
        if !self.asset_ids.contains(&faucet_id) {
            return Err(anyhow!("unknown pool asset {}", faucet_id.to_hex()));
        }
        let pool_id = self.resolve_user_pool(user_id).await?;
        get_user_available_balance_from_pool(pool_id, self.vault_id, faucet_id, user_id).await
    }

    async fn resolve_user_pool(&mut self, user_id: AccountId) -> Result<AccountId> {
        if let Some(pool_id) = self.user_pools.get(&user_id) {
            return Ok(*pool_id);
        }
        let storage = fetch_account_storage_from_rpc(self.vault_id).await?;
        let pool_id = user_pool_from_storage(&storage, user_id)?
            .ok_or_else(|| anyhow!("user {} has no assigned pool", user_id.to_hex()))?;
        if !self.pool_ids.contains(&pool_id) {
            return Err(anyhow!(
                "user {} is assigned to unlisted pool {}",
                user_id.to_hex(),
                pool_id.to_hex()
            ));
        }
        self.user_pools.insert(user_id, pool_id);
        Ok(pool_id)
    }

    pub async fn start(&mut self) {
        let mut price_rx = self.message_broker.subscribe_oracle_prices();
        let mut orders_rx = self.message_broker.subscribe_order_updates();
        let mut pool_state_rx = self.message_broker.subscribe_pool_state();
        let mut amm_rx = self.message_broker.subscribe_amm();
        let mut fee_state_rx = self.message_broker.subscribe_fee_state();
        let mut durable_poll = tokio::time::interval(std::time::Duration::from_millis(250));

        loop {
            let event = select! {
                _ = durable_poll.tick() => {
                    self.try_start_batch().await;
                    continue;
                }
                prices = price_rx.recv() => {
                    match prices {
                        Ok(ev) => MessageBrokerEvent::OraclePrice(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("oracle", n);
                            warn!("orders lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                orders = orders_rx.recv() => {
                    match orders {
                        Ok(ev) => MessageBrokerEvent::Order(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("order", n);
                            warn!("orders lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                pool_states = pool_state_rx.recv() => {
                    match pool_states {
                        Ok(ev) => MessageBrokerEvent::PoolState(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("pool_state", n);
                            eprintln!("pool_states lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                amm = amm_rx.recv() => {
                    match amm {
                        Ok(ev) => MessageBrokerEvent::Amm(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("amm", n);
                            eprintln!("amm lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                lp = self.lp_chain_rx.recv() => {
                    match lp {
                        Ok(ev) => MessageBrokerEvent::LpChain(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("lp", n);
                            warn!("LP chain events lagged behind by {n} messages");
                            if !self.pending_swaps.is_empty() {
                                self.lp_recovery_pending = true;
                            } else if let Err(error) = self.recover_unapplied_lp_operations() {
                                error!(%error, "failed to recover lagged LP chain events");
                            }
                            continue;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                cashflow = self.vault_cashflow_rx.recv() => {
                    match cashflow {
                        Ok(ev) => MessageBrokerEvent::VaultCashFlow(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("vault_cashflow", n);
                            warn!(
                                "vault cashflow events lagged behind by {n} messages; \
                                 warm balances may be stale until next cold hydrate"
                            );
                            continue;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                fees = fee_state_rx.recv() => {
                    match fees {
                        Ok(ev) => MessageBrokerEvent::FeeState(ev),
                        Err(RecvError::Lagged(n)) => {
                            self.message_broker.record_lag("fee_state", n);
                            let now = chrono::Utc::now().timestamp() as u64;
                            match self.fee_store.active_states(now) {
                                Ok(states) => apply_fee_states(&mut self.pool_states, &states),
                                Err(error) => {
                                    error!(%error, "failed to recover fee state after broker lag")
                                }
                            }
                            continue;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }

            };
            self.handle_event(event).await;
        }
    }

    async fn handle_event(&mut self, event: MessageBrokerEvent) {
        match event {
            MessageBrokerEvent::Order(ev) => {
                let is_new = matches!(ev, OrderUpdate::New(_));
                match &ev {
                    OrderUpdate::Confirmed(order) => {
                        // Production lifecycle: Submitted → Confirmed is chain-observed terminal
                        // success. Executed/Settled are not emitted by the current backend.
                        let now = chrono::Utc::now().timestamp_millis() as u64;
                        let pending = self.pending_swaps.remove(&order.id).or_else(|| {
                            self.execution_store
                                .swap_finalization(&order.id.to_string())
                                .ok()
                                .flatten()
                                .and_then(|stored| self.restore_pending_swap(stored).ok())
                                .map(|pending| {
                                    self.apply_pending_swap(&pending);
                                    pending
                                })
                        });
                        if let Some(pending) = pending {
                            if let Err(error) =
                                self.analytics_store.record_swap(&pending.analytics_swap)
                            {
                                error!(order_id = %order.id, %error, "failed to record finalized swap analytics");
                            }
                            let _ = self.message_broker.broadcast_analytics(AnalyticsEvent {
                                user_id: pending.user_id,
                                timestamp: now,
                            });
                            let _ = self.message_broker.broadcast_pool_state(PoolStateEvent {
                                pool_states: self.pool_states.clone(),
                                timestamp: now,
                            });
                        }
                    }
                    OrderUpdate::Failed(order) => {
                        if let Some(pending) = self.pending_swaps.remove(&order.id) {
                            self.rollback_pending_swap(pending);
                            let _ = self.message_broker.broadcast_pool_state(PoolStateEvent {
                                pool_states: self.pool_states.clone(),
                                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                            });
                            let now = chrono::Utc::now().timestamp_millis() as u64;
                            if let Err(error) =
                                self.execution_store.fail_swap(&order.id.to_string(), now)
                            {
                                error!(order_id = %order.id, %error, "failed to mark swap accounting failed");
                            }
                        }
                    }
                    _ => {}
                }
                self.orders.apply_order_update(ev);
                self.persist_final_pool_states_if_ready();
                if is_new {
                    self.try_start_batch().await;
                }
            }
            MessageBrokerEvent::PoolState(ev) => {
                for (faucet_id, new_pool_state) in ev.pool_states.iter() {
                    self.pool_states.insert(*faucet_id, *new_pool_state);
                }
            }
            MessageBrokerEvent::OraclePrice(ev) => {
                self.oracle_pricing.update_from_price_event(ev.clone());
                if let Some(asset) = self
                    .pool_states
                    .iter()
                    .find(|(faucet_id, _)| faucet_id.to_hex() == ev.faucet_id)
                {
                    let scale = 10_u128.pow(u32::from(asset.1.metadata().asset_decimals));
                    let _ = self.analytics_store.record_mark(&OracleMark {
                        event_id: format!("oracle:{}:{}:{}", ev.faucet_id, ev.timestamp, ev.price),
                        asset_id: ev.faucet_id,
                        quote_asset: "oracle_usd".to_owned(),
                        price_numerator: u128::from(ev.price),
                        price_scale: scale,
                        event_time: ev.timestamp,
                    });
                }
            }
            MessageBrokerEvent::Amm(ev) => match ev {
                AmmEvent::BatchSubmitted => {
                    // Prove/submit finished; admit gate can overlap with finality.
                    info!("Batch submitted; releasing admit gate for finality overlap");
                    self.engine_busy = false;
                    self.try_start_batch().await;
                }
                AmmEvent::BatchFailed => {
                    warn!(
                        "Reconciling batch failed; will reload pool states before next claim"
                    );
                    self.pool_resync_required = true;
                    // Pause further claims until resync runs in try_start_batch.
                    self.engine_busy = true;
                    self.reload_pool_states_from_store();
                    self.pool_resync_required = false;
                    self.engine_busy = false;
                    self.try_start_batch().await;
                }
                AmmEvent::OrdersExecuted => {
                    info!("Batch terminal; persistence checkpoint");
                    self.execution_finished = true;
                    self.persist_final_pool_states_if_ready();
                    // Safety net if BatchSubmitted was missed (lag / restart).
                    if self.engine_busy {
                        self.engine_busy = false;
                        self.try_start_batch().await;
                    }
                }
                _ => {}
            },
            MessageBrokerEvent::LpChain(ev) => {
                if !self.pending_swaps.is_empty() {
                    self.lp_recovery_pending = true;
                    return;
                }
                // The broadcast is only a wake-up. Read the ordered durable journal so
                // dropped or reordered notifications cannot choose accounting order.
                if let Err(error) = self.recover_unapplied_lp_operations() {
                    error!(note_id = %ev.note_id, %error, "failed to apply LP chain event");
                    self.lp_recovery_pending = true;
                }
            }
            MessageBrokerEvent::VaultCashFlow(ev) => {
                self.apply_vault_cashflow_event(ev);
            }
            MessageBrokerEvent::FeeState(ev) => {
                apply_fee_states(&mut self.pool_states, &ev.fee_states);
                let _ = self.message_broker.broadcast_pool_state(PoolStateEvent {
                    pool_states: self.pool_states.clone(),
                    timestamp: ev.timestamp,
                });
            }
            _ => {}
        }
        self.retry_pending_lp_recovery();
    }

    fn recover_unapplied_lp_operations(&mut self) -> Result<()> {
        for operation in self.lp_store.confirmed_operations()? {
            if let Some(lp_shares) = self.execution_store.applied_lp_shares(&operation.note_id)? {
                // During startup this was already reconciled before the ledger rebuild.
                // During normal operation this closes an acknowledgement retry window.
                self.lp_store.apply_operation(
                    &operation.note_id,
                    lp_shares,
                    chrono::Utc::now().timestamp_millis() as u64,
                )?;
                continue;
            }
            self.apply_lp_chain_event(self.lp_event_from_operation(&operation)?)?;
        }
        Ok(())
    }

    fn lp_event_from_operation(
        &self,
        operation: &crate::lp_store::LpOperation,
    ) -> Result<LpChainEvent> {
        let kind = if operation.kind == "deposit" {
            LpOperationKind::Deposit
        } else {
            LpOperationKind::Withdraw
        };
        let lp_id = AccountId::from_hex(&operation.lp_id)?;
        let faucet_id = AccountId::from_hex(&operation.faucet_id)?;
        let shares_hint = if kind == LpOperationKind::Withdraw {
            self.lp_store
                .position(lp_id, faucet_id)?
                .filter(|position| {
                    position.checkpoint_value != 0 && position.checkpoint_shares != 0
                })
                .map(|position| {
                    let numerator =
                        u128::from(operation.asset_amount) * u128::from(position.checkpoint_shares);
                    u64::try_from(
                        numerator
                            .div_ceil(u128::from(position.checkpoint_value))
                            .min(u128::from(position.shares)),
                    )
                    .map_err(anyhow::Error::from)
                })
                .transpose()?
        } else {
            None
        };
        Ok(LpChainEvent {
            note_id: operation.note_id.clone(),
            kind,
            lp_id,
            faucet_id,
            asset_amount: operation.asset_amount,
            shares_hint,
        })
    }

    fn retry_pending_lp_recovery(&mut self) {
        if !self.pending_swaps.is_empty() {
            return;
        }
        if !self.lp_recovery_pending {
            return;
        }
        self.lp_recovery_pending = false;
        if let Err(error) = self.recover_unapplied_lp_operations() {
            error!(%error, "failed to recover queued LP chain events");
            self.lp_recovery_pending = true;
        }
    }

    fn apply_lp_chain_event(&mut self, event: LpChainEvent) -> Result<()> {
        if let Some(lp_shares) = self.execution_store.applied_lp_shares(&event.note_id)? {
            self.lp_store.apply_operation(
                &event.note_id,
                lp_shares,
                chrono::Utc::now().timestamp_millis() as u64,
            )?;
            self.record_lp_analytics(&event, lp_shares);
            return Ok(());
        }

        let pool = self
            .pool_states
            .get(&event.faucet_id)
            .copied()
            .ok_or_else(|| anyhow!("no pool for LP asset {}", event.faucet_id.to_hex()))?;
        let (lp_shares, new_supply, new_balances) = match event.kind {
            LpOperationKind::Deposit => {
                let (shares, supply, balances) =
                    pool.get_deposit_lp_amount_out(U256::from(event.asset_amount))?;
                (shares.saturating_to::<u64>(), supply, balances)
            }
            LpOperationKind::Withdraw => {
                let owned = self.lp_ledger.shares_of(event.faucet_id, event.lp_id);
                let shares = if let Some(shares) = event
                    .shares_hint
                    .filter(|shares| *shares > 0 && *shares <= owned)
                {
                    shares
                } else {
                    self.shares_for_withdrawal(pool, event.asset_amount, owned)?
                };
                let (payout, supply, balances) =
                    pool.get_withdraw_asset_amount_out(U256::from(shares))?;
                if payout.saturating_to::<u64>() < event.asset_amount {
                    return Err(anyhow!(
                        "LP shares value {} is below confirmed withdrawal {}",
                        payout,
                        event.asset_amount
                    ));
                }
                (shares, supply, balances)
            }
        };

        // Build the next state off to the side. Nothing becomes visible in RAM until
        // SQLite has atomically committed both the snapshot and note marker.
        let mut next_pool_states = self.pool_states.clone();
        next_pool_states
            .get_mut(&event.faucet_id)
            .expect("validated LP pool exists")
            .update_state(new_balances, new_supply);
        let mut next_lp_ledger = self.lp_ledger.clone();
        match event.kind {
            LpOperationKind::Deposit => {
                next_lp_ledger.mint(event.faucet_id, event.lp_id, lp_shares);
            }
            LpOperationKind::Withdraw => {
                next_lp_ledger.burn(event.faucet_id, event.lp_id, lp_shares)?;
            }
        }
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let inserted = self.execution_store.save_lp_application(
            &event.note_id,
            event.faucet_id,
            lp_shares,
            &next_pool_states,
            now,
        )?;
        if inserted {
            self.pool_states = next_pool_states;
            self.lp_ledger = next_lp_ledger;
        }
        self.message_broker.broadcast_pool_state(PoolStateEvent {
            pool_states: self.pool_states.clone(),
            timestamp: now,
        })?;
        // This acknowledgement may crash independently. The durable marker makes the
        // next startup reconcile only the journal without touching the curve again.
        self.lp_store
            .apply_operation(&event.note_id, lp_shares, now)?;
        self.record_lp_analytics(&event, lp_shares);
        Ok(())
    }

    fn shares_for_withdrawal(
        &self,
        pool: PoolState,
        asset_amount: u64,
        owned_shares: u64,
    ) -> Result<u64> {
        if owned_shares == 0 {
            return Err(anyhow!("LP owns no shares"));
        }
        let mut low = 1_u64;
        let mut high = owned_shares;
        while low < high {
            let middle = low + (high - low) / 2;
            let (payout, _, _) = pool.get_withdraw_asset_amount_out(U256::from(middle))?;
            if payout >= U256::from(asset_amount) {
                high = middle;
            } else {
                low = middle + 1;
            }
        }
        let (payout, _, _) = pool.get_withdraw_asset_amount_out(U256::from(low))?;
        if payout < U256::from(asset_amount) {
            return Err(anyhow!(
                "confirmed withdrawal exceeds the LP position value"
            ));
        }
        Ok(low)
    }

    fn record_lp_analytics(&self, event: &LpChainEvent, lp_shares: u64) {
        let _ = self.analytics_store.record_lp_cash_flow(&LpCashFlow {
            event_id: format!("lp:{}", event.note_id),
            kind: match event.kind {
                LpOperationKind::Deposit => LpCashFlowKind::Deposit,
                LpOperationKind::Withdraw => LpCashFlowKind::Withdrawal,
            },
            lp_id: event.lp_id.to_hex(),
            pool_id: event.faucet_id.to_hex(),
            asset_id: event.faucet_id.to_hex(),
            amount: event.asset_amount,
            shares: lp_shares,
            event_time: chrono::Utc::now().timestamp_millis() as u64,
        });
    }

    /// Start processing a batch of new orders if prove/submit is idle.
    /// The admit gate is released on `AmmEvent::BatchSubmitted` (overlap with finality).
    async fn try_start_batch(&mut self) {
        if self.engine_busy {
            return;
        }
        if self.pool_resync_required {
            self.reload_pool_states_from_store();
            self.pool_resync_required = false;
        }
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let batch =
            match self
                .execution_store
                .claim_admitted_orders(&self.worker_id, now, 600_000, 32)
            {
                Ok(batch) => batch,
                Err(error) => {
                    error!(%error, "Failed to claim durable admitted orders");
                    return;
                }
            };
        if batch.is_empty() {
            return;
        }
        info!(trades = batch.len(), "Trading cycle: quoting admitted orders");
        self.engine_busy = true;
        self.execution_finished = false;
        let started = Instant::now();
        let trade_count = batch.len();
        if let Err(e) = self.process_orders(batch).await {
            error!("Failed to process orders: {e:?}");
            self.engine_busy = false;
        } else {
            info!(
                trades = trade_count,
                process_ms = started.elapsed().as_millis(),
                "Trading cycle: quotes ready, batch handed to execution"
            );
        }
    }

    fn reload_pool_states_from_store(&mut self) {
        match self.execution_store.latest_pool_states() {
            Ok(states) => {
                let mut reloaded = 0_usize;
                for (faucet_id, state) in states {
                    if self.pool_states.contains_key(&faucet_id) {
                        self.pool_states.insert(faucet_id, state);
                        reloaded += 1;
                    }
                }
                info!(reloaded, "Reloaded pool states from execution store");
                let _ = self.message_broker.broadcast_pool_state(PoolStateEvent {
                    pool_states: self.pool_states.clone(),
                    timestamp: chrono::Utc::now().timestamp_millis() as u64,
                });
            }
            Err(error) => {
                error!(%error, "Failed to reload pool states after batch failure");
            }
        }
    }

    /// Quotes one swap on the curve using the current pool states and oracle prices.
    ///
    /// Returns a structured quote with resulting pool balances and fee attribution.
    fn quote_swap(
        &self,
        sell_faucet: AccountId,
        buy_faucet: AccountId,
        amount_in: u64,
    ) -> Result<crate::curve::SwapQuote> {
        let sell_pool = self
            .pool_states
            .get(&sell_faucet)
            .ok_or_else(|| anyhow!("no pool state for sell asset {}", sell_faucet.to_hex()))?;
        let buy_pool = self
            .pool_states
            .get(&buy_faucet)
            .ok_or_else(|| anyhow!("no pool state for buy asset {}", buy_faucet.to_hex()))?;

        let sell_price = self
            .oracle_pricing
            .get_price_for_asset(sell_faucet)
            .ok_or_else(|| anyhow!("no oracle price for sell asset {}", sell_faucet.to_hex()))?;
        let buy_price = self
            .oracle_pricing
            .get_price_for_asset(buy_faucet)
            .ok_or_else(|| anyhow!("no oracle price for buy asset {}", buy_faucet.to_hex()))?;
        // sell asset priced in buy asset, scaled by 1e12
        let price = sell_price.quote_with(buy_price.price);

        let now = chrono::Utc::now().timestamp() as u64;
        let sell_volatility_fee = if sell_pool.settings().volatility_fee_valid_until >= now {
            sell_pool.settings().volatility_fee_in
        } else {
            U256::ZERO
        };
        let buy_volatility_fee = if buy_pool.settings().volatility_fee_valid_until >= now {
            buy_pool.settings().volatility_fee_out
        } else {
            U256::ZERO
        };
        get_curve_amount_out_with_volatility_fee(
            sell_pool,
            buy_pool,
            U256::from(sell_pool.metadata().asset_decimals),
            U256::from(buy_pool.metadata().asset_decimals),
            U256::from(amount_in),
            price,
            sell_volatility_fee.max(buy_volatility_fee),
        )
    }

    fn publish_order_update(&self, update: OrderUpdate) -> Result<()> {
        if let OrderUpdate::Failed(order) = &update {
            let snapshot = update.snapshot();
            let reason = snapshot
                .failure_reason
                .map(|reason| format!("{reason:?}"))
                .unwrap_or_else(|| "ExecutionError".to_owned());
            self.execution_store.fail_claimed_order(
                order.id,
                &self.worker_id,
                &reason,
                chrono::Utc::now().timestamp_millis() as u64,
            )?;
        }
        self.orders.apply_order_update(update.clone());
        self.message_broker.broadcast_order_update(update)
    }

    async fn process_orders(&mut self, batch: Vec<Order<Created>>) -> Result<()> {
        let orders: Vec<_> = batch.into_iter().map(|o| o.start_processing()).collect();

        for order in &orders {
            self.publish_order_update(OrderUpdate::StartedProcessing(order.clone()))?;
        }

        let mut processed_batch = Vec::with_capacity(orders.len());
        let mut proposed_swaps = Vec::with_capacity(orders.len());
        let mut pool_states_changed = false;
        for order in orders {
            let details = order.details();
            let buy_faucet = details.asset_out;
            let sell_faucet = details.asset_in;
            let user_id = order.user_id();

            if is_expired_at(order.expires_at(), chrono::Utc::now().timestamp() as u64) {
                warn!(order_id = %order.id, "Signed intent expired before processing");
                let failed = order.failed(OrderFailureReason::Expired);
                self.publish_order_update(OrderUpdate::Failed(failed))?;
                continue;
            }

            if sell_faucet == buy_faucet
                || !self.asset_ids.contains(&sell_faucet)
                || !self.asset_ids.contains(&buy_faucet)
            {
                warn!(
                    order_id = %order.id,
                    "Order uses identical or unlisted assets"
                );
                let failed = order.failed(OrderFailureReason::ExecutionError);
                self.publish_order_update(OrderUpdate::Failed(failed))?;
                continue;
            }

            // Quote the swap on the curve against the current pool states.
            let quote = match self.quote_swap(sell_faucet, buy_faucet, details.amount_in) {
                Ok(quote) => quote,
                Err(e) => {
                    warn!(order_id = %order.id, "Swap quote failed: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError);
                    self.publish_order_update(OrderUpdate::Failed(failed))?;
                    continue;
                }
            };
            let amount_out = quote.amount_out.saturating_to::<u64>();

            if amount_out < details.min_amount_out {
                warn!(
                    order_id = %order.id,
                    amount_out,
                    min_amount_out = details.min_amount_out,
                    "Swap quote below min_amount_out"
                );
                let failed = order.failed(OrderFailureReason::MinOutNotMet);
                self.publish_order_update(OrderUpdate::Failed(failed))?;
                continue;
            }
            let quote = quote.credit_to(U256::from(details.min_amount_out));

            // Local mirror is server preflight truth; vault cashflows + swap deltas keep
            // it warm. Cold miss hydrates once from chain; FPI remains settlement authority.
            let sell_balance = match self.balance_of(user_id, sell_faucet).await {
                Ok(balance) => balance,
                Err(e) => {
                    warn!(order_id = %order.id, "Sell balance fetch failed: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError);
                    self.publish_order_update(OrderUpdate::Failed(failed))?;
                    continue;
                }
            };
            if sell_balance < details.amount_in {
                warn!(
                    order_id = %order.id,
                    available = sell_balance,
                    requested = details.amount_in,
                    "Insufficient sell balance"
                );
                let failed = order.failed(OrderFailureReason::InsufficientBalance);
                self.publish_order_update(OrderUpdate::Failed(failed))?;
                continue;
            }
            let buy_balance = match self.balance_of(user_id, buy_faucet).await {
                Ok(buy) => buy,
                Err(e) => {
                    warn!(order_id = %order.id, "Buy balance fetch failed: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError);
                    self.publish_order_update(OrderUpdate::Failed(failed))?;
                    continue;
                }
            };
            let sell_before = *self
                .pool_states
                .get(&sell_faucet)
                .expect("validated sell pool exists")
                .balances();
            let buy_before = *self
                .pool_states
                .get(&buy_faucet)
                .expect("validated buy pool exists")
                .balances();

            // A swap debits the sell asset and credits the buy asset.
            let sell_balance = sell_balance - details.amount_in;
            // The signed intent binds the minimum output. Credit exactly that amount so
            // the off-chain mirror, history, and on-chain counters all agree.
            let executed_amount_out = quote.credited_amount_out.saturating_to::<u64>();
            let buy_balance = buy_balance.saturating_add(executed_amount_out);
            self.balances.insert((user_id, sell_faucet), sell_balance);
            self.balances.insert((user_id, buy_faucet), buy_balance);

            // Commit the curve's new balances so the next order in the batch quotes
            // against the updated pools.
            if let Some(pool) = self.pool_states.get_mut(&sell_faucet) {
                pool.update_balances(quote.new_base_pool_balances);
            }
            if let Some(pool) = self.pool_states.get_mut(&buy_faucet) {
                pool.update_balances(quote.new_quote_pool_balances);
            }
            pool_states_changed = true;

            self.message_broker.broadcast_user(UserEvent {
                user_id,
                faucet_id: buy_faucet,
                amount: buy_balance,
            })?;
            self.message_broker.broadcast_user(UserEvent {
                user_id,
                faucet_id: sell_faucet,
                amount: sell_balance,
            })?;

            let processed = order.processed(OrderExecutionResult {
                amount_out: executed_amount_out,
            });
            let sell_price = self
                .oracle_pricing
                .get_price_for_asset(sell_faucet)
                .map(|price| price.price);
            let buy_price = self
                .oracle_pricing
                .get_price_for_asset(buy_faucet)
                .map(|price| price.price);
            self.pending_swaps.insert(
                processed.id,
                PendingSwap {
                    user_id,
                    sell_faucet,
                    buy_faucet,
                    amount_in: details.amount_in,
                    amount_out: executed_amount_out,
                    sell_before,
                    sell_after: quote.new_base_pool_balances,
                    buy_before,
                    buy_after: quote.new_quote_pool_balances,
                    analytics_swap: FinalizedSwap {
                        event_id: processed.id.to_string(),
                        user_id: user_id.to_hex(),
                        pool_id: buy_faucet.to_hex(),
                        asset_in: sell_faucet.to_hex(),
                        asset_out: buy_faucet.to_hex(),
                        quote_asset: "oracle_usd".to_owned(),
                        amount_in: details.amount_in,
                        amount_out: executed_amount_out,
                        quote_value: oracle_value(
                            details.amount_in,
                            sell_price,
                            self.pool_states
                                .get(&sell_faucet)
                                .map(|pool| pool.metadata().asset_decimals)
                                .unwrap_or(0),
                        ),
                        lp_fee_quote: oracle_value_u256(
                            quote.fees.lp_fee,
                            buy_price,
                            self.pool_states
                                .get(&buy_faucet)
                                .map(|pool| pool.metadata().asset_decimals)
                                .unwrap_or(0),
                        ),
                        protocol_fee_quote: oracle_value_u256(
                            quote.fees.protocol_fee,
                            buy_price,
                            self.pool_states
                                .get(&buy_faucet)
                                .map(|pool| pool.metadata().asset_decimals)
                                .unwrap_or(0),
                        ),
                        backstop_fee_quote: oracle_value_u256(
                            quote.fees.backstop_fee,
                            buy_price,
                            self.pool_states
                                .get(&buy_faucet)
                                .map(|pool| pool.metadata().asset_decimals)
                                .unwrap_or(0),
                        ),
                        volatility_fee_quote: oracle_value_u256(
                            quote.fees.volatility_fee,
                            buy_price,
                            self.pool_states
                                .get(&buy_faucet)
                                .map(|pool| pool.metadata().asset_decimals)
                                .unwrap_or(0),
                        ),
                        requested_amount_out: Some(details.min_amount_out),
                        quoted_amount_out: Some(amount_out),
                        event_time: chrono::Utc::now().timestamp_millis() as u64,
                    },
                },
            );
            let fee_version = self
                .pool_states
                .get(&sell_faucet)
                .map(|pool| pool.settings().volatility_fee_version)
                .unwrap_or(0)
                .max(
                    self.pool_states
                        .get(&buy_faucet)
                        .map(|pool| pool.settings().volatility_fee_version)
                        .unwrap_or(0),
                );
            proposed_swaps.push(ProposedSwap {
                order_id: processed.id.to_string(),
                user_id,
                asset_in: sell_faucet,
                asset_out: buy_faucet,
                amount_in: details.amount_in,
                amount_out: executed_amount_out,
                quoted_amount_out: quote.amount_out,
                raw_amount_out: quote.fees.raw_amount_out,
                lp_fee: quote.fees.lp_fee,
                backstop_fee: quote.fees.backstop_fee,
                protocol_fee: quote.fees.protocol_fee,
                volatility_fee: quote.fees.volatility_fee,
                oracle_price_in: sell_price,
                oracle_price_out: buy_price,
                fee_version,
                finalization: SwapFinalization {
                    user_id: user_id.to_hex(),
                    sell_faucet: sell_faucet.to_hex(),
                    buy_faucet: buy_faucet.to_hex(),
                    amount_in: details.amount_in,
                    amount_out: executed_amount_out,
                    sell_before: balances_to_strings(sell_before),
                    sell_after: balances_to_strings(quote.new_base_pool_balances),
                    buy_before: balances_to_strings(buy_before),
                    buy_after: balances_to_strings(quote.new_quote_pool_balances),
                    analytics_swap: self
                        .pending_swaps
                        .get(&processed.id)
                        .expect("pending swap was inserted")
                        .analytics_swap
                        .clone(),
                },
            });
            processed_batch.push(processed);
        }

        let _ = pool_states_changed;

        let batch_id = self.execution_store.create_execution_batch(
            &self.worker_id,
            &processed_batch,
            &proposed_swaps,
            chrono::Utc::now().timestamp_millis() as u64,
        )?;
        for processed in &processed_batch {
            self.publish_order_update(OrderUpdate::Processed(processed.clone()))?;
        }
        if batch_id.is_some() {
            // Notification-only fast path. Miden execution claims the committed batch.
            self.message_broker
                .broadcast_processed_batch(processed_batch)?;
        } else {
            self.engine_busy = false;
        }

        Ok(())
    }

    fn rollback_pending_swap(&mut self, pending: PendingSwap) {
        if let Some(pool) = self.pool_states.get_mut(&pending.sell_faucet) {
            pool.update_balances(revert_balances(
                *pool.balances(),
                pending.sell_before,
                pending.sell_after,
            ));
        }
        if let Some(pool) = self.pool_states.get_mut(&pending.buy_faucet) {
            pool.update_balances(revert_balances(
                *pool.balances(),
                pending.buy_before,
                pending.buy_after,
            ));
        }
        if let Some(balance) = self
            .balances
            .get_mut(&(pending.user_id, pending.sell_faucet))
        {
            *balance = balance.saturating_add(pending.amount_in);
        }
        if let Some(balance) = self
            .balances
            .get_mut(&(pending.user_id, pending.buy_faucet))
        {
            *balance = balance.saturating_sub(pending.amount_out);
        }
    }

    fn restore_pending_swap(&self, stored: SwapFinalization) -> Result<PendingSwap> {
        Ok(PendingSwap {
            user_id: AccountId::from_hex(&stored.user_id)?,
            sell_faucet: AccountId::from_hex(&stored.sell_faucet)?,
            buy_faucet: AccountId::from_hex(&stored.buy_faucet)?,
            amount_in: stored.amount_in,
            amount_out: stored.amount_out,
            sell_before: balances_from_strings(stored.sell_before)?,
            sell_after: balances_from_strings(stored.sell_after)?,
            buy_before: balances_from_strings(stored.buy_before)?,
            buy_after: balances_from_strings(stored.buy_after)?,
            analytics_swap: stored.analytics_swap,
        })
    }

    fn apply_pending_swap(&mut self, pending: &PendingSwap) {
        if let Some(pool) = self.pool_states.get_mut(&pending.sell_faucet) {
            pool.update_balances(revert_balances(
                *pool.balances(),
                pending.sell_after,
                pending.sell_before,
            ));
        }
        if let Some(pool) = self.pool_states.get_mut(&pending.buy_faucet) {
            pool.update_balances(revert_balances(
                *pool.balances(),
                pending.buy_after,
                pending.buy_before,
            ));
        }
    }

    fn persist_final_pool_states_if_ready(&self) {
        if !self.execution_finished || !self.pending_swaps.is_empty() {
            return;
        }
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if let Err(error) = self
            .execution_store
            .save_pool_states(&self.pool_states, now)
        {
            error!(%error, "failed to persist finalized pool states");
        }
        for (faucet_id, pool) in &self.pool_states {
            let price = self
                .oracle_pricing
                .get_price_for_asset(*faucet_id)
                .map(|value| value.price);
            let nav = oracle_value_u256(
                pool.balances().reserve,
                price,
                pool.metadata().asset_decimals,
            );
            let liabilities = oracle_value_u256(
                pool.balances().total_liabilities,
                price,
                pool.metadata().asset_decimals,
            );
            let _ = self
                .analytics_store
                .record_pool_snapshot(&PoolSnapshotInput {
                    event_id: format!("pool:{}:{now}", faucet_id.to_hex()),
                    pool_id: faucet_id.to_hex(),
                    asset_id: faucet_id.to_hex(),
                    quote_asset: "oracle_usd".to_owned(),
                    nav_quote: nav.saturating_sub(liabilities),
                    tvl_quote: nav,
                    inventory_quantity: i128::try_from(pool.balances().reserve)
                        .unwrap_or(i128::MAX),
                    inventory_cost_quote: i128::try_from(liabilities).unwrap_or(i128::MAX),
                    inventory_value_quote: i128::try_from(nav).unwrap_or(i128::MAX),
                    event_time: now,
                });
        }
    }
}

fn oracle_value(amount: u64, price: Option<u64>, decimals: u8) -> u128 {
    u128::from(amount).saturating_mul(u128::from(price.unwrap_or(0)))
        / 10_u128.pow(u32::from(decimals))
}

fn oracle_value_u256(amount: U256, price: Option<u64>, decimals: u8) -> u128 {
    oracle_value(amount.saturating_to::<u64>(), price, decimals)
}

fn balances_to_strings(balances: PoolBalances) -> [String; 3] {
    [
        balances.reserve.to_string(),
        balances.reserve_with_slippage.to_string(),
        balances.total_liabilities.to_string(),
    ]
}

fn balances_from_strings(values: [String; 3]) -> Result<PoolBalances> {
    Ok(PoolBalances {
        reserve: U256::from_str(&values[0])?,
        reserve_with_slippage: U256::from_str(&values[1])?,
        total_liabilities: U256::from_str(&values[2])?,
    })
}

fn revert_balances(
    current: PoolBalances,
    before: PoolBalances,
    after: PoolBalances,
) -> PoolBalances {
    PoolBalances {
        reserve: revert_u256(current.reserve, before.reserve, after.reserve),
        reserve_with_slippage: revert_u256(
            current.reserve_with_slippage,
            before.reserve_with_slippage,
            after.reserve_with_slippage,
        ),
        total_liabilities: revert_u256(
            current.total_liabilities,
            before.total_liabilities,
            after.total_liabilities,
        ),
    }
}

fn revert_u256(current: U256, before: U256, after: U256) -> U256 {
    if after >= before {
        current.saturating_sub(after - before)
    } else {
        current.saturating_add(before - after)
    }
}

/// Apply a vault cashflow to the spendable-balance mirror used for quote preflight.
fn apply_vault_cashflow_to_balances(
    balances: &mut HashMap<(AccountId, AccountId), u64>,
    event: &VaultCashFlowEvent,
) {
    let key = (event.user_id, event.faucet_id);
    match event.kind {
        VaultCashFlowKind::Fund => {
            let entry = balances.entry(key).or_insert(0);
            *entry = entry.saturating_add(event.amount);
        }
        VaultCashFlowKind::InitRedeem => {
            if let Some(balance) = balances.get_mut(&key) {
                *balance = balance.saturating_sub(event.amount);
            }
        }
        VaultCashFlowKind::Redeem => {
            // Available was already reduced at INIT_REDEEM; completed redeem is a no-op.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{VaultCashFlowEvent, VaultCashFlowKind, apply_vault_cashflow_to_balances};
    use miden_client::account::AccountId;
    use std::collections::HashMap;

    fn user() -> AccountId {
        AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap()
    }

    fn faucet() -> AccountId {
        AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap()
    }

    fn event(kind: VaultCashFlowKind, amount: u64) -> VaultCashFlowEvent {
        VaultCashFlowEvent {
            user_id: user(),
            faucet_id: faucet(),
            amount,
            kind,
        }
    }

    #[test]
    fn fund_inserts_and_increments_available() {
        let mut balances = HashMap::new();
        apply_vault_cashflow_to_balances(&mut balances, &event(VaultCashFlowKind::Fund, 1_000));
        assert_eq!(balances.get(&(user(), faucet())), Some(&1_000));
        apply_vault_cashflow_to_balances(&mut balances, &event(VaultCashFlowKind::Fund, 250));
        assert_eq!(balances.get(&(user(), faucet())), Some(&1_250));
    }

    #[test]
    fn init_redeem_reduces_warm_balance_and_leaves_cold_untouched() {
        let mut balances = HashMap::new();
        apply_vault_cashflow_to_balances(
            &mut balances,
            &event(VaultCashFlowKind::InitRedeem, 100),
        );
        assert!(balances.is_empty());

        apply_vault_cashflow_to_balances(&mut balances, &event(VaultCashFlowKind::Fund, 500));
        apply_vault_cashflow_to_balances(
            &mut balances,
            &event(VaultCashFlowKind::InitRedeem, 200),
        );
        assert_eq!(balances.get(&(user(), faucet())), Some(&300));
    }

    #[test]
    fn redeem_does_not_double_subtract() {
        let mut balances = HashMap::new();
        apply_vault_cashflow_to_balances(&mut balances, &event(VaultCashFlowKind::Fund, 500));
        apply_vault_cashflow_to_balances(
            &mut balances,
            &event(VaultCashFlowKind::InitRedeem, 200),
        );
        apply_vault_cashflow_to_balances(&mut balances, &event(VaultCashFlowKind::Redeem, 200));
        assert_eq!(balances.get(&(user(), faucet())), Some(&300));
    }
}
