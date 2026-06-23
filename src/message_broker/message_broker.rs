use std::collections::HashMap;

use anyhow::Result;
use miden_client::account::AccountId;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{order::OrderUpdate, pool::PoolState};

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
    pub open_orders: usize,
    pub closed_orders: usize,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum AmmEvent {
    StartProcessing,
    OrdersProcessed,
    OrdersExecuted,
    OrdersSettled,
}

pub enum MessageBrokerEvent {
    Order(OrderUpdate),
    PoolState(PoolStateEvent),
    OraclePrice(OraclePriceEvent),
    Stats(StatsEvent),
    Amm(AmmEvent),
}

#[derive(Clone)]
pub struct MessageBroker {
    pub order_updates_tx: broadcast::Sender<OrderUpdate>,
    pub pool_state_tx: broadcast::Sender<PoolStateEvent>,
    pub oracle_prices_tx: broadcast::Sender<OraclePriceEvent>,
    pub stats_tx: broadcast::Sender<StatsEvent>,
    pub amm_tx: broadcast::Sender<AmmEvent>,
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

        Self {
            order_updates_tx,
            pool_state_tx,
            oracle_prices_tx,
            stats_tx,
            amm_tx,
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
}

impl Default for MessageBroker {
    fn default() -> Self {
        Self::new()
    }
}
