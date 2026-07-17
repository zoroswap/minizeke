use std::{
    collections::HashMap,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use chrono::Utc;
use miden_client::account::AccountId;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::warn;

use crate::{
    fee_store::AssetFeeState,
    order::{Order, OrderStats, OrderUpdate, Processed},
    pool::PoolState,
};

#[derive(Debug, Clone)]
pub struct FeeStateEvent {
    pub fee_states: HashMap<AccountId, AssetFeeState>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LpOperationKind {
    Deposit,
    Withdraw,
}

#[derive(Debug, Clone)]
pub struct LpChainEvent {
    pub note_id: String,
    pub kind: LpOperationKind,
    pub lp_id: AccountId,
    pub faucet_id: AccountId,
    pub asset_amount: u64,
    /// Checkpoint-derived share burn for an offline withdrawal. Processing validates it
    /// against the live ledger and falls back to curve inversion when unavailable.
    pub shares_hint: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct LpAppliedEvent {
    pub note_id: String,
    pub lp_shares: u64,
    pub error: Option<String>,
}

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

#[derive(Debug, Clone)]
pub struct AnalyticsEvent {
    pub user_id: AccountId,
    pub timestamp: u64,
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
    LpChain(LpChainEvent),
    FeeState(FeeStateEvent),
    Analytics(AnalyticsEvent),
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
    pub lp_chain_tx: broadcast::Sender<LpChainEvent>,
    pub lp_applied_tx: broadcast::Sender<LpAppliedEvent>,
    pub fee_state_tx: broadcast::Sender<FeeStateEvent>,
    pub analytics_tx: broadcast::Sender<AnalyticsEvent>,
    metrics: Arc<BrokerMetrics>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelClass {
    /// Notification only; consumers recover authoritative payloads from durable stores.
    DurableWakeup,
    /// Latest state supersedes older values and lag may be coalesced.
    BestEffortCoalesced,
    /// Ephemeral lifecycle signal with bounded loss tolerance.
    BestEffort,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BrokerMetricSnapshot {
    pub lagged_messages: u64,
    pub dropped_without_receivers: u64,
}

#[derive(Default)]
struct BrokerMetrics {
    lagged_messages: AtomicU64,
    dropped_without_receivers: AtomicU64,
}

impl MessageBroker {
    /// Create a new MessageBroker with specified channel capacities
    pub fn new() -> Self {
        let (order_updates_tx, _) = broadcast::channel(capacity("BROKER_ORDER_CAPACITY", 1000));
        let (pool_state_tx, _) = broadcast::channel(capacity("BROKER_POOL_STATE_CAPACITY", 100));
        let (oracle_prices_tx, _) = broadcast::channel(capacity("BROKER_ORACLE_CAPACITY", 100));
        let (stats_tx, _) = broadcast::channel(capacity("BROKER_STATS_CAPACITY", 10));
        let (amm_tx, _) = broadcast::channel(capacity("BROKER_AMM_CAPACITY", 100));
        let (user_tx, _) = broadcast::channel(capacity("BROKER_USER_CAPACITY", 100));
        let (processed_batch_tx, _) =
            broadcast::channel(capacity("BROKER_PROCESSED_BATCH_CAPACITY", 100));
        let (trades_tx, _) = broadcast::channel(capacity("BROKER_TRADE_CAPACITY", 1000));
        let (lp_chain_tx, _) = broadcast::channel(capacity("BROKER_LP_CAPACITY", 100));
        let (lp_applied_tx, _) = broadcast::channel(capacity("BROKER_LP_APPLIED_CAPACITY", 100));
        let (fee_state_tx, _) = broadcast::channel(capacity("BROKER_FEE_STATE_CAPACITY", 20));
        let (analytics_tx, _) = broadcast::channel(capacity("BROKER_ANALYTICS_CAPACITY", 100));

        Self {
            order_updates_tx,
            pool_state_tx,
            oracle_prices_tx,
            stats_tx,
            amm_tx,
            user_tx,
            processed_batch_tx,
            trades_tx,
            lp_chain_tx,
            lp_applied_tx,
            fee_state_tx,
            analytics_tx,
            metrics: Arc::new(BrokerMetrics::default()),
        }
    }

    pub fn channel_class(channel: &str) -> Option<ChannelClass> {
        match channel {
            "order" | "processed_batch" | "lp" | "lp_applied" | "fee_state" => {
                Some(ChannelClass::DurableWakeup)
            }
            "oracle" | "stats" | "pool_state" => Some(ChannelClass::BestEffortCoalesced),
            "amm" | "user" | "trade" | "analytics" => Some(ChannelClass::BestEffort),
            _ => None,
        }
    }

    pub fn record_lag(&self, channel: &'static str, skipped: u64) {
        self.metrics
            .lagged_messages
            .fetch_add(skipped, Ordering::Relaxed);
        warn!(channel, skipped, "broker subscriber lagged");
    }

    pub fn metrics(&self) -> BrokerMetricSnapshot {
        BrokerMetricSnapshot {
            lagged_messages: self.metrics.lagged_messages.load(Ordering::Relaxed),
            dropped_without_receivers: self
                .metrics
                .dropped_without_receivers
                .load(Ordering::Relaxed),
        }
    }

    fn record_unobserved(&self) {
        self.metrics
            .dropped_without_receivers
            .fetch_add(1, Ordering::Relaxed);
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
                self.record_unobserved();
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
                self.record_unobserved();
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
                self.record_unobserved();
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
                self.record_unobserved();
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
                self.record_unobserved();
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
                self.record_unobserved();
                warn!("Failed to broadcast amm event: {}", e);
                Ok(())
            }
        }
    }

    pub fn broadcast_trade(&self, event: TradeEvent) -> Result<()> {
        if let Err(error) = self.trades_tx.send(event) {
            self.record_unobserved();
            warn!("Failed to broadcast trade event: {error}");
        }
        Ok(())
    }

    pub fn broadcast_lp_chain(&self, event: LpChainEvent) -> Result<()> {
        if let Err(error) = self.lp_chain_tx.send(event) {
            self.record_unobserved();
            warn!("Failed to broadcast LP chain event: {error}");
        }
        Ok(())
    }

    pub fn broadcast_lp_applied(&self, event: LpAppliedEvent) -> Result<()> {
        if let Err(error) = self.lp_applied_tx.send(event) {
            self.record_unobserved();
            warn!("Failed to broadcast LP applied event: {error}");
        }
        Ok(())
    }

    pub fn broadcast_fee_state(&self, event: FeeStateEvent) -> Result<()> {
        if let Err(error) = self.fee_state_tx.send(event) {
            self.record_unobserved();
            warn!("Failed to broadcast fee state event: {error}");
        }
        Ok(())
    }

    /// Broadcast a batch of processed orders to the execution engine
    pub fn broadcast_processed_batch(&self, batch: Vec<Order<Processed>>) -> Result<()> {
        match self.processed_batch_tx.send(batch) {
            Ok(_) => Ok(()),
            Err(e) => {
                self.record_unobserved();
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

    pub fn subscribe_lp_chain(&self) -> broadcast::Receiver<LpChainEvent> {
        self.lp_chain_tx.subscribe()
    }

    pub fn subscribe_lp_applied(&self) -> broadcast::Receiver<LpAppliedEvent> {
        self.lp_applied_tx.subscribe()
    }

    pub fn subscribe_fee_state(&self) -> broadcast::Receiver<FeeStateEvent> {
        self.fee_state_tx.subscribe()
    }

    pub fn broadcast_analytics(&self, event: AnalyticsEvent) -> Result<()> {
        if let Err(error) = self.analytics_tx.send(event) {
            self.record_unobserved();
            warn!("Failed to broadcast analytics event: {error}");
        }
        Ok(())
    }

    pub fn subscribe_analytics(&self) -> broadcast::Receiver<AnalyticsEvent> {
        self.analytics_tx.subscribe()
    }
}

impl Default for MessageBroker {
    fn default() -> Self {
        Self::new()
    }
}

fn capacity(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        .max(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_classes_separate_recoverable_and_coalesced_payloads() {
        assert_eq!(
            MessageBroker::channel_class("lp"),
            Some(ChannelClass::DurableWakeup)
        );
        assert_eq!(
            MessageBroker::channel_class("oracle"),
            Some(ChannelClass::BestEffortCoalesced)
        );
        assert_eq!(
            MessageBroker::channel_class("stats"),
            Some(ChannelClass::BestEffortCoalesced)
        );
    }

    #[test]
    fn unobserved_sends_are_counted() {
        let broker = MessageBroker::new();
        broker
            .broadcast_oracle_price(OraclePriceEvent {
                oracle_id: "oracle".into(),
                faucet_id: "asset".into(),
                price: 1,
                timestamp: 1,
            })
            .unwrap();
        assert_eq!(broker.metrics().dropped_without_receivers, 1);

        broker.record_lag("oracle", 3);
        broker.record_lag("order", 2);
        assert_eq!(broker.metrics().lagged_messages, 5);
    }
}
