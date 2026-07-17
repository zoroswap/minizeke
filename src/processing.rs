use crate::{
    analytics_store::{
        AnalyticsStore, FinalizedSwap, LpCashFlow, LpCashFlowKind, OracleMark, PoolSnapshotInput,
    },
    curve::get_curve_amount_out_with_volatility_fee,
    deployment::AssetInfo,
    execution_store::ExecutionStore,
    fee_store::apply_fee_states,
    lp_store::LpStore,
    message_broker::message_broker::{
        AmmEvent, AnalyticsEvent, LpAppliedEvent, LpChainEvent, LpOperationKind, MessageBroker,
        MessageBrokerEvent, PoolStateEvent, UserEvent,
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
    sync::Arc,
};
use tokio::{select, sync::broadcast::error::RecvError};
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
    /// Lazy per-(user, faucet) balance mirror: the opening balance is derived from chain
    /// on first use, then deltas are applied locally. This is only a pre-flight check —
    /// the on-chain FPI assert in the swap tx remains the real enforcement.
    balances: HashMap<(AccountId, AccountId), u64>,
    lp_ledger: LpLedger,
    processed_lp_notes: HashMap<String, u64>,
    execution_store: Arc<ExecutionStore>,
    pending_swaps: HashMap<Uuid, PendingSwap>,
    execution_finished: bool,
    analytics_store: Arc<AnalyticsStore>,
}

impl Processing {
    pub async fn new(
        message_broker: Arc<MessageBroker>,
        pool_states: HashMap<AccountId, PoolState>,
        vault_id: AccountId,
        assets: Vec<AssetInfo>,
        pools: Vec<AccountId>,
        lp_store: Arc<LpStore>,
    ) -> Result<Self> {
        let oracle_pricing = OraclePricing::new(&assets);
        let orders = Orders::default();
        let execution_store = Arc::new(ExecutionStore::open_from_env()?);
        let analytics_store = Arc::new(AnalyticsStore::open_from_env()?);

        let mut lp_ledger = LpLedger::default();
        for position in lp_store.positions()? {
            let lp_id = AccountId::from_hex(&position.lp_id)?;
            let faucet_id = AccountId::from_hex(&position.faucet_id)?;
            lp_ledger.mint(faucet_id, lp_id, position.shares);
        }

        Ok(Self {
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
            processed_lp_notes: HashMap::new(),
            execution_store,
            pending_swaps: HashMap::new(),
            execution_finished: false,
            analytics_store,
        })
    }

    /// Returns the user's balance for `faucet_id`, fetching the opening balance from
    /// chain on the first request for this (user, faucet) pair.
    async fn balance_of(&mut self, user_id: AccountId, faucet_id: AccountId) -> Result<u64> {
        if let Some(balance) = self.balances.get(&(user_id, faucet_id)) {
            return Ok(*balance);
        }
        let balance = self.fetch_balance_from_chain(user_id, faucet_id).await?;
        self.balances.insert((user_id, faucet_id), balance);
        Ok(balance)
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
        let mut lp_chain_rx = self.message_broker.subscribe_lp_chain();
        let mut fee_state_rx = self.message_broker.subscribe_fee_state();

        loop {
            let event = select! {
                prices = price_rx.recv() => {
                    match prices {
                        Ok(ev) => MessageBrokerEvent::OraclePrice(ev),
                        Err(RecvError::Lagged(n)) => {
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
                            eprintln!("amm lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                lp = lp_chain_rx.recv() => {
                    match lp {
                        Ok(ev) => MessageBrokerEvent::LpChain(ev),
                        Err(RecvError::Lagged(n)) => {
                            warn!("LP chain events lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                fees = fee_state_rx.recv() => {
                    match fees {
                        Ok(ev) => MessageBrokerEvent::FeeState(ev),
                        Err(RecvError::Lagged(n)) => {
                            warn!("fee state events lagged behind by {n} messages");
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
                let snapshot = ev.snapshot();
                match &ev {
                    OrderUpdate::Executed(order) => {
                        let now = chrono::Utc::now().timestamp_millis() as u64;
                        if let Some(pending) = self.pending_swaps.remove(&order.id) {
                            if let Err(error) =
                                self.analytics_store.record_swap(&pending.analytics_swap)
                            {
                                error!(order_id = %order.id, %error, "failed to record finalized swap analytics");
                            }
                            let _ = self.message_broker.broadcast_analytics(AnalyticsEvent {
                                user_id: pending.user_id,
                                timestamp: now,
                            });
                        }
                        if !self
                            .execution_store
                            .executed_swap(&order.id.to_string())
                            .unwrap_or(false)
                            && let Err(error) = self.execution_store.finalize_swap(
                                &order.id.to_string(),
                                snapshot.tx_hash.as_deref(),
                                now,
                            )
                        {
                            error!(order_id = %order.id, %error, "failed to finalize swap accounting");
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
                AmmEvent::OrdersExecuted => {
                    // The in-flight batch has been submitted; release the gate and
                    // pick up any orders that arrived while we were busy.
                    info!("Batch executed; engine is idle again");
                    self.execution_finished = true;
                    self.persist_final_pool_states_if_ready();
                    self.engine_busy = false;
                    self.try_start_batch().await;
                }
                _ => {}
            },
            MessageBrokerEvent::LpChain(ev) => {
                if let Err(error) = self.apply_lp_chain_event(ev.clone()) {
                    error!(note_id = %ev.note_id, %error, "failed to apply LP chain event");
                    let _ = self.message_broker.broadcast_lp_applied(LpAppliedEvent {
                        note_id: ev.note_id,
                        lp_shares: 0,
                        error: Some(error.to_string()),
                    });
                }
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
    }

    fn apply_lp_chain_event(&mut self, event: LpChainEvent) -> Result<()> {
        if let Some(lp_shares) = self.processed_lp_notes.get(&event.note_id).copied() {
            self.message_broker.broadcast_lp_applied(LpAppliedEvent {
                note_id: event.note_id.clone(),
                lp_shares,
                error: None,
            })?;
            self.record_lp_analytics(&event, lp_shares);
            return Ok(());
        }

        let result = (|| {
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

            match event.kind {
                LpOperationKind::Deposit => {
                    self.lp_ledger.mint(event.faucet_id, event.lp_id, lp_shares);
                }
                LpOperationKind::Withdraw => {
                    self.lp_ledger
                        .burn(event.faucet_id, event.lp_id, lp_shares)?;
                }
            }
            self.pool_states
                .get_mut(&event.faucet_id)
                .unwrap()
                .update_state(new_balances, new_supply);
            self.message_broker.broadcast_pool_state(PoolStateEvent {
                pool_states: self.pool_states.clone(),
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            })?;
            if self.pending_swaps.is_empty() {
                self.execution_store.save_pool_states(
                    &self.pool_states,
                    chrono::Utc::now().timestamp_millis() as u64,
                )?;
            }
            self.processed_lp_notes
                .insert(event.note_id.clone(), lp_shares);
            self.message_broker.broadcast_lp_applied(LpAppliedEvent {
                note_id: event.note_id.clone(),
                lp_shares,
                error: None,
            })?;
            self.record_lp_analytics(&event, lp_shares);
            Ok(())
        })();

        if result.is_err() {
            self.processed_lp_notes.remove(&event.note_id);
        }
        result
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

    /// Start processing a batch of new orders if the engine is idle.
    /// Only one batch is allowed in flight at a time; the gate is released when
    /// the execution engine reports `AmmEvent::OrdersSettled`.
    async fn try_start_batch(&mut self) {
        if self.engine_busy {
            return;
        }
        let batch = self.orders.orders_new();
        if batch.is_empty() {
            return;
        }
        info!(count = batch.len(), "Starting processing batch");
        self.engine_busy = true;
        self.execution_finished = false;
        // Informational event for WebSocket clients.
        let _ = self.message_broker.broadcast_amm(AmmEvent::StartProcessing);
        if let Err(e) = self.process_orders(batch).await {
            error!("Failed to process orders: {e:?}");
            self.engine_busy = false;
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
        self.orders.apply_order_update(update.clone());
        self.message_broker.broadcast_order_update(update)
    }

    async fn process_orders(&mut self, batch: Vec<Order<Created>>) -> Result<()> {
        let orders: Vec<_> = batch.into_iter().map(|o| o.start_processing()).collect();

        for order in &orders {
            self.publish_order_update(OrderUpdate::StartedProcessing(order.clone()))?;
        }

        let mut processed_batch = Vec::with_capacity(orders.len());
        let mut pool_states_changed = false;
        for order in orders {
            let details = order.details();
            let buy_faucet = details.asset_out;
            let sell_faucet = details.asset_in;
            let user_id = order.user_id();

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

            // Always refresh the spendable sell balance. INIT_REDEEM can reduce available
            // funds without notifying this process, so a cached value is not authoritative.
            let sell_balance = match self.fetch_balance_from_chain(user_id, sell_faucet).await {
                Ok(balance) => {
                    self.balances.insert((user_id, sell_faucet), balance);
                    balance
                }
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
                    "Insufficient sell balance after chain refresh"
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
            self.execution_store.record_proposed_swap(
                &processed.id.to_string(),
                user_id,
                sell_faucet,
                buy_faucet,
                details.amount_in,
                executed_amount_out,
                quote.amount_out,
                quote.fees.raw_amount_out,
                quote.fees.lp_fee,
                quote.fees.backstop_fee,
                quote.fees.protocol_fee,
                quote.fees.volatility_fee,
                sell_price,
                buy_price,
                fee_version,
                chrono::Utc::now().timestamp_millis() as u64,
            )?;
            self.publish_order_update(OrderUpdate::Processed(processed.clone()))?;
            processed_batch.push(processed);
        }

        if pool_states_changed {
            self.message_broker.broadcast_pool_state(PoolStateEvent {
                pool_states: self.pool_states.clone(),
                timestamp: chrono::Utc::now().timestamp_millis() as u64,
            })?;
        }

        self.message_broker
            .broadcast_amm(AmmEvent::OrdersProcessed)?;
        self.message_broker
            .broadcast_processed_batch(processed_batch)?;

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
