use crate::{
    curve::get_curve_amount_out,
    message_broker::message_broker::{
        AmmEvent, MessageBroker, MessageBrokerEvent, PoolStateEvent, UserEvent,
    },
    oracle_sse::OraclePricing,
    order::{Created, Order, OrderExecutionResult, OrderFailureReason, OrderUpdate, Orders},
    pool::{PoolState, get_user_balance_from_pool},
};

use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use miden_client::account::AccountId;
use std::{collections::HashMap, sync::Arc};
use tokio::{select, sync::broadcast::error::RecvError};
use tracing::{error, info, warn};

pub struct Processing {
    message_broker: Arc<MessageBroker>,
    oracle_pricing: OraclePricing,
    orders: Orders,
    pool_states: HashMap<AccountId, PoolState>,
    engine_busy: bool,
    pool_id: AccountId,
    vault_id: AccountId,
    asset0: AccountId,
    asset1: AccountId,
    /// Lazy per-(user, faucet) balance mirror: the opening balance is derived from chain
    /// on first use, then deltas are applied locally. This is only a pre-flight check —
    /// the on-chain FPI assert in the swap tx remains the real enforcement.
    balances: HashMap<(AccountId, AccountId), u64>,
}

impl Processing {
    pub async fn new(
        message_broker: Arc<MessageBroker>,
        pool_states: HashMap<AccountId, PoolState>,
        pool_id: AccountId,
        vault_id: AccountId,
        asset0: AccountId,
        asset1: AccountId,
    ) -> Result<Self> {
        let oracle_pricing = OraclePricing::new();
        let orders = Orders::default();

        Ok(Self {
            oracle_pricing,
            message_broker,
            orders,
            pool_states,
            engine_busy: false,
            pool_id,
            vault_id,
            asset0,
            asset1,
            balances: HashMap::new(),
        })
    }

    /// Returns the user's balance for `faucet_id`, fetching the opening balance from
    /// chain on the first request for this (user, faucet) pair.
    async fn balance_of(&mut self, user_id: AccountId, faucet_id: AccountId) -> Result<u64> {
        if let Some(balance) = self.balances.get(&(user_id, faucet_id)) {
            return Ok(*balance);
        }
        let asset_index = if faucet_id == self.asset0 {
            0
        } else if faucet_id == self.asset1 {
            1
        } else {
            return Err(anyhow!("unknown pool asset {}", faucet_id.to_hex()));
        };
        let balance = get_user_balance_from_pool(
            self.pool_id,
            self.vault_id,
            faucet_id,
            asset_index,
            user_id,
        )
        .await?;
        self.balances.insert((user_id, faucet_id), balance);
        Ok(balance)
    }

    pub async fn start(&mut self) {
        let mut price_rx = self.message_broker.subscribe_oracle_prices();
        let mut orders_rx = self.message_broker.subscribe_order_updates();
        let mut pool_state_rx = self.message_broker.subscribe_pool_state();
        let mut amm_rx = self.message_broker.subscribe_amm();

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

            };
            self.handle_event(event).await;
        }
    }

    async fn handle_event(&mut self, event: MessageBrokerEvent) {
        match event {
            MessageBrokerEvent::Order(ev) => {
                let is_new = matches!(ev, OrderUpdate::New(_));
                self.orders.apply_order_update(ev);
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
                self.oracle_pricing.update_from_price_event(ev);
            }
            MessageBrokerEvent::Amm(ev) => match ev {
                AmmEvent::OrdersExecuted => {
                    // The in-flight batch has been submitted; release the gate and
                    // pick up any orders that arrived while we were busy.
                    info!("Batch executed; engine is idle again");
                    self.engine_busy = false;
                    self.try_start_batch().await;
                }
                _ => {}
            },
            _ => {}
        }
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
        // Informational event for WebSocket clients.
        let _ = self.message_broker.broadcast_amm(AmmEvent::StartProcessing);
        if let Err(e) = self.process_orders(batch).await {
            error!("Failed to process orders: {e:?}");
            self.engine_busy = false;
        }
    }

    /// Quotes one swap on the curve using the current pool states and oracle prices.
    ///
    /// Returns `(amount_out, new_sell_pool_balances, new_buy_pool_balances)`.
    fn quote_swap(
        &self,
        sell_faucet: AccountId,
        buy_faucet: AccountId,
        amount_in: u64,
    ) -> Result<(u64, crate::pool::PoolBalances, crate::pool::PoolBalances)> {
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

        let (amount_out, new_sell_balances, new_buy_balances) = get_curve_amount_out(
            sell_pool,
            buy_pool,
            U256::from(sell_pool.metadata().asset_decimals),
            U256::from(buy_pool.metadata().asset_decimals),
            U256::from(amount_in),
            price,
        )?;

        Ok((
            amount_out.saturating_to::<u64>(),
            new_sell_balances,
            new_buy_balances,
        ))
    }

    async fn process_orders(&mut self, batch: Vec<Order<Created>>) -> Result<()> {
        let orders: Vec<_> = batch.into_iter().map(|o| o.start_processing()).collect();

        for order in &orders {
            self.message_broker
                .broadcast_order_update(OrderUpdate::StartedProcessing(order.clone()))?;
        }

        let mut processed_batch = Vec::with_capacity(orders.len());
        let mut pool_states_changed = false;
        for order in orders {
            let details = order.details();
            let buy_faucet = details.asset_out;
            let sell_faucet = details.asset_in;
            let user_id = order.user_id();

            // Quote the swap on the curve against the current pool states.
            let (amount_out, new_sell_balances, new_buy_balances) =
                match self.quote_swap(sell_faucet, buy_faucet, details.amount_in) {
                    Ok(quote) => quote,
                    Err(e) => {
                        warn!(order_id = %order.id, "Swap quote failed: {e:?}");
                        let failed = order.failed(OrderFailureReason::ExecutionError);
                        self.message_broker
                            .broadcast_order_update(OrderUpdate::Failed(failed))?;
                        continue;
                    }
                };

            if amount_out < details.min_amount_out {
                warn!(
                    order_id = %order.id,
                    amount_out,
                    min_amount_out = details.min_amount_out,
                    "Swap quote below min_amount_out"
                );
                let failed = order.failed(OrderFailureReason::MinOutNotMet);
                self.message_broker
                    .broadcast_order_update(OrderUpdate::Failed(failed))?;
                continue;
            }

            // Pre-flight balance check against the lazily-fetched chain balance. A fetch
            // failure or an insufficient balance fails only this order.
            let (sell_balance, buy_balance) = match self
                .balance_of(user_id, sell_faucet)
                .await
                .and_then(|sell| {
                    if sell < details.amount_in {
                        Err(anyhow!(
                            "insufficient balance: has {sell}, selling {}",
                            details.amount_in
                        ))
                    } else {
                        Ok(sell)
                    }
                }) {
                Ok(sell) => match self.balance_of(user_id, buy_faucet).await {
                    Ok(buy) => (sell, buy),
                    Err(e) => {
                        warn!(order_id = %order.id, "Buy balance fetch failed: {e:?}");
                        let failed = order.failed(OrderFailureReason::ExecutionError);
                        self.message_broker
                            .broadcast_order_update(OrderUpdate::Failed(failed))?;
                        continue;
                    }
                },
                Err(e) => {
                    warn!(order_id = %order.id, "Sell balance check failed: {e:?}");
                    let failed = order.failed(OrderFailureReason::ExecutionError);
                    self.message_broker
                        .broadcast_order_update(OrderUpdate::Failed(failed))?;
                    continue;
                }
            };

            // A swap debits the sell asset and credits the buy asset.
            let sell_balance = sell_balance - details.amount_in;
            let buy_balance = buy_balance.saturating_add(amount_out);
            self.balances.insert((user_id, sell_faucet), sell_balance);
            self.balances.insert((user_id, buy_faucet), buy_balance);

            // Commit the curve's new balances so the next order in the batch quotes
            // against the updated pools.
            if let Some(pool) = self.pool_states.get_mut(&sell_faucet) {
                pool.update_balances(new_sell_balances);
            }
            if let Some(pool) = self.pool_states.get_mut(&buy_faucet) {
                pool.update_balances(new_buy_balances);
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

            let processed = order.processed(OrderExecutionResult { amount_out });
            self.message_broker
                .broadcast_order_update(OrderUpdate::Processed(processed.clone()))?;
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
}
