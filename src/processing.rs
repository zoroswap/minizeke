use crate::{
    message_broker::message_broker::{AmmEvent, MessageBroker, MessageBrokerEvent, UserEvent},
    oracle_sse::OraclePricing,
    order::{OrderExecutionResult, OrderUpdate, Orders},
    pool::PoolState,
    user::Users,
};

use anyhow::Result;
use miden_client::account::AccountId;
use std::{collections::HashMap, sync::Arc};
use tokio::{select, sync::broadcast::error::RecvError};
use tracing::warn;

pub struct Processing {
    message_broker: Arc<MessageBroker>,
    oracle_pricing: OraclePricing,
    users: Users,
    orders: Orders,
    pool_states: HashMap<AccountId, PoolState>,
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
                self.orders.apply_order_update(ev);
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
                AmmEvent::StartProcessing => {
                    self.process_orders();
                }
                _ => {}
            },
            _ => {}
        }
    }

    fn process_orders(&mut self) -> Result<()> {
        let orders = self.orders.orders_new();
        let orders: Vec<_> = orders.into_iter().map(|o| o.start_processing()).collect();

        for order in &orders {
            self.message_broker
                .broadcast_order_update(OrderUpdate::StartedProcessing(order.clone()))?;
        }

        for order in orders {
            let details = order.details();
            let buy_faucet = details.asset_out;
            let sell_faucet = details.asset_in;
            let user_id = order.user_id();
            self.users
                .sub_from_balance(order.user_id(), buy_faucet, details.min_amount_out);
            self.users
                .add_to_balance(order.user_id(), sell_faucet, details.amount_in);

            let balance0 = self.users.user_balance(user_id, buy_faucet)?;
            let balance1 = self.users.user_balance(user_id, sell_faucet)?;

            self.message_broker.broadcast_user(UserEvent {
                user_id,
                faucet_id: buy_faucet,
                amount: balance0,
            })?;
            self.message_broker.broadcast_user(UserEvent {
                user_id,
                faucet_id: buy_faucet,
                amount: balance1,
            })?;
            self.message_broker
                .broadcast_order_update(OrderUpdate::Processed(order.processed(
                    OrderExecutionResult {
                        amount_out: details.min_amount_out,
                    },
                )))?;
        }

        self.message_broker
            .broadcast_amm(AmmEvent::OrdersProcessed)?;

        Ok(())
    }
}
