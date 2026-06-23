use crate::{
    message_broker::message_broker::{AmmEvent, MessageBroker, MessageBrokerEvent, UserEvent},
    oracle_sse::OraclePricing,
    order::{Created, Order, OrderExecutionResult, OrderUpdate, Orders},
    pool::PoolState,
    user::Users,
};

use anyhow::Result;
use miden_client::account::AccountId;
use std::{collections::HashMap, sync::Arc};
use tokio::{select, sync::broadcast::error::RecvError};
use tracing::{error, info, warn};

pub struct Processing {
    message_broker: Arc<MessageBroker>,
    oracle_pricing: OraclePricing,
    users: Users,
    orders: Orders,
    pool_states: HashMap<AccountId, PoolState>,
    engine_busy: bool,
}

impl Processing {
    pub async fn new(
        message_broker: Arc<MessageBroker>,
        users: Users,
        pool_states: HashMap<AccountId, PoolState>,
    ) -> Result<Self> {
        let oracle_pricing = OraclePricing::new();
        let orders = Orders::default();

        Ok(Self {
            oracle_pricing,
            message_broker,
            orders,
            pool_states,
            users,
            engine_busy: false,
        })
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
            self.handle_event(event);
        }
    }

    fn handle_event(&mut self, event: MessageBrokerEvent) {
        match event {
            MessageBrokerEvent::Order(ev) => {
                let is_new = matches!(ev, OrderUpdate::New(_));
                self.orders.apply_order_update(ev);
                if is_new {
                    self.try_start_batch();
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
                    self.try_start_batch();
                }
                _ => {}
            },
            _ => {}
        }
    }

    /// Start processing a batch of new orders if the engine is idle.
    /// Only one batch is allowed in flight at a time; the gate is released when
    /// the execution engine reports `AmmEvent::OrdersSettled`.
    fn try_start_batch(&mut self) {
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
        if let Err(e) = self.process_orders(batch) {
            error!("Failed to process orders: {e:?}");
            self.engine_busy = false;
        }
    }

    fn process_orders(&mut self, batch: Vec<Order<Created>>) -> Result<()> {
        let orders: Vec<_> = batch.into_iter().map(|o| o.start_processing()).collect();

        for order in &orders {
            self.message_broker
                .broadcast_order_update(OrderUpdate::StartedProcessing(order.clone()))?;
        }

        let mut processed_batch = Vec::with_capacity(orders.len());
        for order in orders {
            let details = order.details();
            let buy_faucet = details.asset_out;
            let sell_faucet = details.asset_in;
            let user_id = order.user_id();
            let amount_out = details.min_amount_out;

            // A swap debits the sell asset and credits the buy asset.
            self.users
                .sub_from_balance(user_id, sell_faucet, details.amount_in)?;
            self.users.add_to_balance(user_id, buy_faucet, amount_out)?;

            let buy_balance = self.users.user_balance(user_id, buy_faucet)?;
            let sell_balance = self.users.user_balance(user_id, sell_faucet)?;

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

        self.message_broker
            .broadcast_amm(AmmEvent::OrdersProcessed)?;
        self.message_broker
            .broadcast_processed_batch(processed_batch)?;

        Ok(())
    }
}
