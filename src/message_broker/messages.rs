use serde::{Deserialize, Serialize};

use crate::{
    message_broker::message_broker::AmmEvent,
    order::{OrderStats, OrderStatus, OrderStatusCounts},
    pool::PoolState,
};

/// Messages sent from client to server
#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ClientMessage {
    Subscribe { channels: Vec<SubscriptionChannel> },
    Unsubscribe { channels: Vec<SubscriptionChannel> },
    Ping,
}

/// Messages sent from server to client
#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type")]
pub enum ServerMessage {
    // Subscription confirmations
    Subscribed {
        channel: SubscriptionChannel,
    },
    Unsubscribed {
        channel: SubscriptionChannel,
    },

    // Data updates
    OrderUpdate {
        order_id: String,
        status: OrderStatus,
        timestamp: u64,
    },
    PoolStateUpdate {
        faucet_id: String,
        balances: PoolState,
        timestamp: u64,
    },
    OraclePriceUpdate {
        oracle_id: String,
        faucet_id: String,
        price: u64,
        timestamp: u64,
    },
    StatsUpdate {
        total_orders: usize,
        open_orders: usize,
        closed_orders: usize,
        by_status: OrderStatusCounts,
        timestamp: u64,
    },
    UserUpdate {
        user_id: String,
        faucet_id: String,
        amount: u64,
    },
    Trade {
        order_id: String,
        pair: String,
        asset_in: String,
        asset_out: String,
        amount_in: u64,
        amount_out: u64,
        price: u64,
        timestamp: u64,
    },
    AmmUpdate {
        status: AmmEvent,
    },

    // Control messages
    Pong,
    Error {
        message: String,
    },
}

/// Subscription channels that clients can subscribe to
#[derive(Debug, Deserialize, Serialize, Clone, Hash, Eq, PartialEq)]
#[serde(tag = "channel")]
pub enum SubscriptionChannel {
    #[serde(rename = "order_updates")]
    OrderUpdates {
        #[serde(default)]
        order_id: Option<String>,
    },
    #[serde(rename = "pool_state")]
    PoolState {
        #[serde(default)]
        faucet_id: Option<String>,
    },
    #[serde(rename = "oracle_prices")]
    OraclePrices {
        #[serde(default)]
        oracle_id: Option<String>,
    },
    #[serde(rename = "user")]
    UserEvent {
        #[serde(default)]
        user_id: Option<String>,
    },
    #[serde(rename = "amm")]
    AmmEvent {},
    #[serde(rename = "stats")]
    Stats,
    #[serde(rename = "trades")]
    Trades,
}

impl SubscriptionChannel {
    /// Check if a specific subscription matches this channel
    pub fn matches(&self, other: &SubscriptionChannel) -> bool {
        match (self, other) {
            (
                SubscriptionChannel::OrderUpdates {
                    order_id: Some(id1),
                },
                SubscriptionChannel::OrderUpdates {
                    order_id: Some(id2),
                },
            ) => id1 == id2,
            (
                SubscriptionChannel::OrderUpdates { order_id: None },
                SubscriptionChannel::OrderUpdates { .. },
            ) => true,
            (
                SubscriptionChannel::PoolState {
                    faucet_id: Some(id1),
                },
                SubscriptionChannel::PoolState {
                    faucet_id: Some(id2),
                },
            ) => id1 == id2,
            (
                SubscriptionChannel::PoolState { faucet_id: None },
                SubscriptionChannel::PoolState { .. },
            ) => true,
            (
                SubscriptionChannel::OraclePrices {
                    oracle_id: Some(id1),
                },
                SubscriptionChannel::OraclePrices {
                    oracle_id: Some(id2),
                },
            ) => id1 == id2,
            (
                SubscriptionChannel::OraclePrices { oracle_id: None },
                SubscriptionChannel::OraclePrices { .. },
            ) => true,
            (SubscriptionChannel::Stats, SubscriptionChannel::Stats) => true,
            (SubscriptionChannel::Trades, SubscriptionChannel::Trades) => true,
            (SubscriptionChannel::AmmEvent {}, SubscriptionChannel::AmmEvent {}) => true,
            (
                SubscriptionChannel::UserEvent { user_id: Some(id1) },
                SubscriptionChannel::UserEvent { user_id: Some(id2) },
            ) => id1 == id2,
            (
                SubscriptionChannel::UserEvent { user_id: None },
                SubscriptionChannel::UserEvent { .. },
            ) => true,
            _ => false,
        }
    }
}

impl ServerMessage {
    pub fn stats_update(stats: OrderStats, timestamp: u64) -> Self {
        Self::StatsUpdate {
            total_orders: stats.total,
            open_orders: stats.open,
            closed_orders: stats.closed,
            by_status: stats.by_status,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_channel_matching() {
        // Test order_updates matching
        let all_orders = SubscriptionChannel::OrderUpdates { order_id: None };
        let specific_order = SubscriptionChannel::OrderUpdates {
            order_id: Some("123".to_string()),
        };

        assert!(all_orders.matches(&specific_order));
        assert!(all_orders.matches(&all_orders));
        assert!(specific_order.matches(&specific_order));
        assert!(!specific_order.matches(&all_orders));

        // Test different channels don't match
        let pool_sub = SubscriptionChannel::PoolState { faucet_id: None };
        assert!(!all_orders.matches(&pool_sub));
    }

    #[test]
    fn test_message_serialization() {
        let msg = ServerMessage::OrderUpdate {
            order_id: "test-123".to_string(),
            status: OrderStatus::Executed,
            timestamp: 1234567890,
        };

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("OrderUpdate"));
        assert!(json.contains("executed"));
        assert!(json.contains("order_id"));
    }

    #[test]
    fn test_stats_update_serialization() {
        use crate::order::{OrderStats, OrderStatusCounts};

        let stats = OrderStats {
            total: 3,
            open: 2,
            closed: 1,
            by_status: OrderStatusCounts {
                created: 1,
                processing: 1,
                processed: 0,
                executed: 1,
                settled: 0,
                failed: 0,
            },
        };
        let msg = ServerMessage::stats_update(stats, 1234567890);
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("StatsUpdate"));
        assert!(json.contains("total_orders"));
        assert!(json.contains("\"created\":1"));
    }

    #[test]
    fn test_client_message_deserialization() {
        let json = r#"{"type":"Subscribe","channels":[{"channel":"stats"}]}"#;
        let msg: ClientMessage = serde_json::from_str(json).unwrap();

        match msg {
            ClientMessage::Subscribe { channels } => {
                assert_eq!(channels.len(), 1);
                assert!(matches!(channels[0], SubscriptionChannel::Stats));
            }
            _ => panic!("Expected Subscribe message"),
        }
    }
}
