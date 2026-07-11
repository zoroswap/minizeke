use std::collections::HashMap;

use anyhow::Result;
use chrono::Utc;
use miden_client::account::AccountId;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    order::{Order, OrderStats, OrderUpdate, Processed},
    pool::PoolState,
};

#[derive(Debug, Clone)]
pub struct PoolStateEvent {
    pub pool_states: HashMap<AccountId, PoolState>,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct OraclePriceEvent {
    pub oracle_id: String,
    pub faucet_id: String,
    pub price: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub struct StatsEvent {
    pub stats: OrderStats,
    pub timestamp: u64,
}

impl StatsEvent {
    pub fn now(stats: OrderStats) -> Self {
        Self {
            stats,
            timestamp: Utc::now().timestamp_millis() as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum AmmEvent {
    StartProcessing,
    OrdersProcessed,
    OrdersExecuted,
    OrdersSettled,
}

#[derive(Debug, Clone)]
pub struct UserEvent {
    pub user_id: AccountId,
    pub faucet_id: AccountId,
    pub amount: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeEvent {
    pub order_id: String,
    pub pair: String,
    pub asset_in: String,
    pub asset_out: String,
    pub amount_in: u64,
    pub amount_out: u64,
    pub price: u64,
    pub timestamp: u64,
}

pub enum MessageBrokerEvent {
    Order(OrderUpdate),
    PoolState(PoolStateEvent),
    OraclePrice(OraclePriceEvent),
    Stats(StatsEvent),
    Amm(AmmEvent),
    User(UserEvent),
    Trade(TradeEvent),
}

#[derive(Clone)]
pub struct MessageBroker {
    pub order_updates_tx: broadcast::Sender<OrderUpdate>,
    pub pool_state_tx: broadcast::Sender<PoolStateEvent>,
    pub oracle_prices_tx: broadcast::Sender<OraclePriceEvent>,
    pub stats_tx: broadcast::Sender<StatsEvent>,
    pub user_tx: broadcast::Sender<UserEvent>,
    pub amm_tx: broadcast::Sender<AmmEvent>,
    pub processed_batch_tx: broadcast::Sender<Vec<Order<Processed>>>,
    pub trades_tx: broadcast::Sender<TradeEvent>,
}

impl MessageBroker {
    /// Create a new MessageBroker with specified channel capacities
    pub fn new() -> Self {
        // Buffer sizes based on expected message frequency
        let (order_updates_tx, _) = broadcast::channel(1000); // High volume
        let (pool_state_tx, _) = broadcast::channel(100);
        let (oracle_prices_tx, _) = broadcast::channel(100);
        let (stats_tx, _) = broadcast::channel(10);
        let (amm_tx, _) = broadcast::channel(100);
        let (user_tx, _) = broadcast::channel(100);
        let (processed_batch_tx, _) = broadcast::channel(100);
        let (trades_tx, _) = broadcast::channel(1000);

        Self {
            order_updates_tx,
            pool_state_tx,
            oracle_prices_tx,
            stats_tx,
            amm_tx,
            user_tx,
            processed_batch_tx,
            trades_tx,
        }
    }

    /// Broadcast an order update event
    pub fn broadcast_order_update(&self, event: OrderUpdate) -> Result<()> {
        match self.order_updates_tx.send(event) {
            Ok(receiver_count) => {
                if receiver_count == 0 {
                    // No subscribers, but this is normal
                }
                Ok(())
            }
            Err(e) => {
                warn!("Failed to broadcast order update: {}", e);
                Ok(()) // Don't fail the operation if broadcast fails
            }
        }
    }

    /// Broadcast a pool state update event
    pub fn broadcast_pool_state(&self, event: PoolStateEvent) -> Result<()> {
        match self.pool_state_tx.send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast pool state: {}", e);
                Ok(())
            }
        }
    }

    /// Broadcast an oracle price update event
    pub fn broadcast_oracle_price(&self, event: OraclePriceEvent) -> Result<()> {
        match self.oracle_prices_tx.send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast oracle price: {}", e);
                Ok(())
            }
        }
    }

    /// Broadcast a stats update event
    pub fn broadcast_stats(&self, event: StatsEvent) -> Result<()> {
        match self.stats_tx.send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast stats: {}", e);
                Ok(())
            }
        }
    }

    /// Broadcast AMM state
    pub fn broadcast_amm(&self, event: AmmEvent) -> Result<()> {
        match self.amm_tx.send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast amm event: {}", e);
                Ok(())
            }
        }
    }

    /// Broadcast AMM state
    pub fn broadcast_user(&self, event: UserEvent) -> Result<()> {
        match self.user_tx.send(event) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast amm event: {}", e);
                Ok(())
            }
        }
    }

    pub fn broadcast_trade(&self, event: TradeEvent) -> Result<()> {
        if let Err(error) = self.trades_tx.send(event) {
            warn!("Failed to broadcast trade event: {error}");
        }
        Ok(())
    }

    /// Broadcast a batch of processed orders to the execution engine
    pub fn broadcast_processed_batch(&self, batch: Vec<Order<Processed>>) -> Result<()> {
        match self.processed_batch_tx.send(batch) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to broadcast processed batch: {}", e);
                Ok(())
            }
        }
    }

    /// Subscribe to processed order batches
    pub fn subscribe_processed_batch(&self) -> broadcast::Receiver<Vec<Order<Processed>>> {
        self.processed_batch_tx.subscribe()
    }

    /// Subscribe to order updates
    pub fn subscribe_order_updates(&self) -> broadcast::Receiver<OrderUpdate> {
        self.order_updates_tx.subscribe()
    }

    /// Subscribe to pool state updates
    pub fn subscribe_pool_state(&self) -> broadcast::Receiver<PoolStateEvent> {
        self.pool_state_tx.subscribe()
    }

    /// Subscribe to oracle price updates
    pub fn subscribe_oracle_prices(&self) -> broadcast::Receiver<OraclePriceEvent> {
        self.oracle_prices_tx.subscribe()
    }

    /// Subscribe to stats updates
    pub fn subscribe_stats(&self) -> broadcast::Receiver<StatsEvent> {
        self.stats_tx.subscribe()
    }

    /// Subscribe to stats updates
    pub fn subscribe_amm(&self) -> broadcast::Receiver<AmmEvent> {
        self.amm_tx.subscribe()
    }

    /// Subscribe to stats updates
    pub fn subscribe_user(&self) -> broadcast::Receiver<UserEvent> {
        self.user_tx.subscribe()
    }

    pub fn subscribe_trades(&self) -> broadcast::Receiver<TradeEvent> {
        self.trades_tx.subscribe()
    }
}

impl Default for MessageBroker {
    fn default() -> Self {
        Self::new()
    }
}
