use axum::extract::ws::Message;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{broadcast::error::RecvError, mpsc};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::{
    message_broker::{
        message_broker::{MessageBroker, OraclePriceEvent},
        messages::{ServerMessage, SubscriptionChannel},
    },
    order::{OrderStatus, OrderUpdate},
    websocket::oracle_throttle::OracleWsThrottle,
};

/// Metadata about a WebSocket connection
pub struct ConnectionMetadata {
    pub connected_at: DateTime<Utc>,
    pub last_pong: Arc<Mutex<DateTime<Utc>>>,
    pub ip_address: Option<String>,
    pub authenticated_user: Arc<Mutex<Option<String>>>,
}

impl ConnectionMetadata {
    pub fn new(ip_address: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            connected_at: now,
            last_pong: Arc::new(Mutex::new(now)),
            ip_address,
            authenticated_user: Arc::new(Mutex::new(None)),
        }
    }
}

/// WebSocket sender with metadata
pub struct WebSocketSender {
    pub tx: mpsc::UnboundedSender<Message>,
    pub metadata: ConnectionMetadata,
}

/// Subscription statistics
pub struct SubscriptionStats {
    pub total_connections: usize,
    pub subscriptions_by_channel: HashMap<String, usize>,
}

/// Manages WebSocket connections and subscriptions
pub struct ConnectionManager {
    connections: Arc<DashMap<Uuid, WebSocketSender>>,
    subscriptions: Arc<DashMap<SubscriptionChannel, HashSet<Uuid>>>,
    message_broker: Option<Arc<MessageBroker>>,
}

impl ConnectionManager {
    /// Create a new ConnectionManager without event broadcasting (for tests)
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            message_broker: None,
        }
    }

    /// Create a new ConnectionManager with event broadcasting
    pub fn with_message_broker(message_broker: Arc<MessageBroker>) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            message_broker: Some(message_broker),
        }
    }

    /// Add a new WebSocket connection
    pub fn add_connection(
        &self,
        conn_id: Uuid,
        tx: mpsc::UnboundedSender<Message>,
        ip_address: Option<String>,
    ) {
        let metadata = ConnectionMetadata::new(ip_address.clone());
        self.connections
            .insert(conn_id, WebSocketSender { tx, metadata });
        debug!(
            conn_id = %conn_id,
            ip = ?ip_address,
            "WebSocket connection established"
        );
    }

    /// Remove a WebSocket connection and all its subscriptions
    pub fn remove_connection(&self, conn_id: Uuid) {
        self.connections.remove(&conn_id);

        // Remove from all subscriptions
        for mut entry in self.subscriptions.iter_mut() {
            entry.value_mut().remove(&conn_id);
        }

        debug!(conn_id = %conn_id, "WebSocket connection removed");
    }

    /// Subscribe a connection to a channel
    pub fn subscribe(&self, conn_id: Uuid, channel: SubscriptionChannel) {
        self.subscriptions
            .entry(channel.clone())
            .or_default()
            .insert(conn_id);

        debug!(
            conn_id = %conn_id,
            channel = ?channel,
            total_subscribers = self.subscriptions.get(&channel).map(|s| s.len()).unwrap_or(0),
            "Subscription added"
        );
    }

    pub fn set_authenticated_user(&self, conn_id: Uuid, user_id: String) {
        if let Some(connection) = self.connections.get(&conn_id)
            && let Ok(mut authenticated) = connection.metadata.authenticated_user.lock()
        {
            *authenticated = Some(user_id);
        }
    }

    pub fn authenticated_user(&self, conn_id: Uuid) -> Option<String> {
        let connection = self.connections.get(&conn_id)?;
        let authenticated = connection.metadata.authenticated_user.lock().ok()?;
        authenticated.clone()
    }

    /// Unsubscribe a connection from a channel
    pub fn unsubscribe(&self, conn_id: Uuid, channel: SubscriptionChannel) {
        if let Some(mut subscribers) = self.subscriptions.get_mut(&channel) {
            subscribers.remove(&conn_id);
            debug!(
                conn_id = %conn_id,
                channel = ?channel,
                "Subscription removed"
            );
        }
    }

    /// Check if a connection is subscribed to a channel
    pub fn is_subscribed(&self, conn_id: Uuid, channel: &SubscriptionChannel) -> bool {
        self.subscriptions
            .get(channel)
            .map(|subscribers| subscribers.contains(&conn_id))
            .unwrap_or(false)
    }

    /// Send a message to a specific connection
    pub fn send_to_connection(&self, conn_id: Uuid, msg: ServerMessage) {
        if let Some(conn) = self.connections.get(&conn_id) {
            let json = match serde_json::to_string(&msg) {
                Ok(json) => json,
                Err(e) => {
                    warn!("Failed to serialize message: {}", e);
                    serde_json::to_string(&ServerMessage::Error {
                        message: "Failed to serialize message".to_string(),
                    })
                    .unwrap_or_default()
                }
            };

            if let Err(e) = conn.tx.send(Message::Text(json.into())) {
                warn!("Failed to send to connection {}: {}", conn_id, e);
            }
        }
    }

    /// Broadcast a message to all connections subscribed to a channel
    pub fn broadcast_to_channel(&self, channel: &SubscriptionChannel, msg: ServerMessage) {
        let subscribers = match self.subscriptions.get(channel) {
            Some(subs) => subs.clone(),
            None => {
                trace!(channel = ?channel, "No subscribers for channel");
                return; // No subscribers
            }
        };

        if subscribers.is_empty() {
            trace!(channel = ?channel, "Channel has empty subscriber list");
            return;
        }

        let json = match serde_json::to_string(&msg) {
            Ok(json) => json,
            Err(e) => {
                warn!("Failed to serialize broadcast message: {}", e);
                return;
            }
        };

        trace!(
            channel = ?channel,
            recipients = subscribers.len(),
            "Broadcasting message to subscribers"
        );

        for conn_id in subscribers.iter() {
            if let Some(conn) = self.connections.get(conn_id)
                && let Err(e) = conn.tx.send(Message::Text(json.clone().into()))
            {
                warn!("Failed to broadcast to connection {}: {}", conn_id, e);
            }
        }
    }

    /// Update the last pong time for a connection
    pub fn update_last_pong(&self, conn_id: Uuid) {
        if let Some(conn) = self.connections.get(&conn_id) {
            *conn.metadata.last_pong.lock().unwrap() = Utc::now();
        }
    }

    /// Get the number of active connections
    pub fn get_connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get subscription statistics
    pub fn get_subscription_stats(&self) -> HashMap<String, usize> {
        self.subscriptions
            .iter()
            .map(|entry| (format!("{:?}", entry.key()), entry.value().len()))
            .collect()
    }

    /// Start a heartbeat task that checks for stale connections
    pub fn start_heartbeat_task(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                interval.tick().await;
                self.check_stale_connections();
            }
        });
    }

    /// Start event forwarding tasks that subscribe to MessageBroker
    /// and forward events to WebSocket clients
    pub fn start_event_forwarding(self: Arc<Self>) {
        let Some(message_broker) = &self.message_broker else {
            warn!("No message_broker configured, skipping event forwarding");
            return;
        };

        // Order updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                debug!("Order updates forwarding task started");
                let mut rx = message_broker.subscribe_order_updates();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let timestamp = Utc::now().timestamp_millis();
                            let (id, status) = match event {
                                OrderUpdate::New(e) => (e.id, OrderStatus::Created),
                                OrderUpdate::StartedProcessing(e) => {
                                    (e.id, OrderStatus::Processing)
                                }
                                OrderUpdate::Processed(e) => (e.id, OrderStatus::Processed),
                                OrderUpdate::Executed(e) => (e.id, OrderStatus::Executed),
                                OrderUpdate::Settled(e) => (e.id, OrderStatus::Settled),
                                OrderUpdate::Failed(e) => (e.id, OrderStatus::Failed),
                            };
                            let message = ServerMessage::OrderUpdate {
                                order_id: id.to_string(),
                                status,
                                timestamp: timestamp as u64,
                            };
                            conn_mgr.broadcast_to_channel(
                                &SubscriptionChannel::OrderUpdates { order_id: None },
                                message.clone(),
                            );
                            conn_mgr.broadcast_to_channel(
                                &SubscriptionChannel::OrderUpdates {
                                    order_id: Some(id.to_string()),
                                },
                                message,
                            );
                        }
                        Err(RecvError::Lagged(n)) => warn!("Order updates lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Order updates channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // Oracle price updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_oracle_prices();
                let mut throttle = OracleWsThrottle::from_env();
                loop {
                    let received = if let Some(deadline) = throttle.next_deadline() {
                        tokio::select! {
                            result = rx.recv() => Some(result),
                            _ = tokio::time::sleep_until(deadline.into()) => {
                                for event in throttle.flush_due(Instant::now()) {
                                    conn_mgr.broadcast_oracle_price_event(event);
                                }
                                None
                            }
                        }
                    } else {
                        Some(rx.recv().await)
                    };

                    let Some(received) = received else {
                        continue;
                    };
                    match received {
                        Ok(event) => {
                            if let Some(event) = throttle.push(event, Instant::now()) {
                                conn_mgr.broadcast_oracle_price_event(event);
                            }
                        }
                        Err(RecvError::Lagged(n)) => warn!("Oracle prices lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Oracle prices channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // Pool state updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_pool_state();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            for (faucet_id, pool_state) in event.pool_states {
                                let timestamp = Utc::now().timestamp_millis();
                                let message = ServerMessage::PoolStateUpdate {
                                    faucet_id: faucet_id.to_hex(),
                                    balances: pool_state,
                                    timestamp: timestamp as u64,
                                };
                                conn_mgr.broadcast_to_channel(
                                    &SubscriptionChannel::PoolState { faucet_id: None },
                                    message.clone(),
                                );
                                conn_mgr.broadcast_to_channel(
                                    &SubscriptionChannel::PoolState {
                                        faucet_id: Some(faucet_id.to_hex()),
                                    },
                                    message,
                                );
                            }
                        }
                        Err(RecvError::Lagged(n)) => warn!("Pool state lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Pool state channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // Stats updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_stats();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let message = ServerMessage::stats_update(event.stats, event.timestamp);
                            conn_mgr.broadcast_to_channel(&SubscriptionChannel::Stats, message);
                        }
                        Err(RecvError::Lagged(n)) => warn!("Stats lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Stats channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // Amm updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_amm();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let message = ServerMessage::AmmUpdate { status: event };
                            conn_mgr
                                .broadcast_to_channel(&SubscriptionChannel::AmmEvent {}, message);
                        }
                        Err(RecvError::Lagged(n)) => warn!("Amm lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Amm channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // User updates
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_user();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let user_id = event.user_id.to_hex();
                            let message = ServerMessage::UserUpdate {
                                user_id: user_id.clone(),
                                faucet_id: event.faucet_id.to_hex(),
                                amount: event.amount,
                            };
                            conn_mgr.broadcast_to_channel(
                                &SubscriptionChannel::UserEvent { user_id: None },
                                message.clone(),
                            );
                            conn_mgr.broadcast_to_channel(
                                &SubscriptionChannel::UserEvent {
                                    user_id: Some(user_id),
                                },
                                message,
                            );
                        }
                        Err(RecvError::Lagged(n)) => warn!("User updates lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("User channel closed");
                            break;
                        }
                    }
                }
            });
        }

        // User analytics invalidation notifications
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_analytics();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let user_id = event.user_id.to_hex();
                            conn_mgr.broadcast_to_channel(
                                &SubscriptionChannel::Analytics {
                                    user_id: Some(user_id.clone()),
                                },
                                ServerMessage::AnalyticsUpdate {
                                    user_id,
                                    timestamp: event.timestamp,
                                },
                            );
                        }
                        Err(RecvError::Lagged(n)) => {
                            warn!("Analytics updates lagged, skipped {n}")
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            });
        }

        // Executed curve fills
        {
            let message_broker = message_broker.clone();
            let conn_mgr = self.clone();
            tokio::spawn(async move {
                let mut rx = message_broker.subscribe_trades();
                loop {
                    match rx.recv().await {
                        Ok(event) => {
                            let message = ServerMessage::Trade {
                                order_id: event.order_id,
                                pair: event.pair,
                                asset_in: event.asset_in,
                                asset_out: event.asset_out,
                                amount_in: event.amount_in,
                                amount_out: event.amount_out,
                                price: event.price,
                                timestamp: event.timestamp,
                            };
                            conn_mgr.broadcast_to_channel(&SubscriptionChannel::Trades, message);
                        }
                        Err(RecvError::Lagged(n)) => warn!("Trades lagged, skipped {n}"),
                        Err(RecvError::Closed) => {
                            error!("Trades channel closed");
                            break;
                        }
                    }
                }
            });
        }

        debug!("Event forwarding tasks started");
    }

    fn broadcast_oracle_price_event(&self, event: OraclePriceEvent) {
        let message = ServerMessage::OraclePriceUpdate {
            oracle_id: event.oracle_id.clone(),
            faucet_id: event.faucet_id,
            price: event.price,
            timestamp: event.timestamp,
        };
        self.broadcast_to_channel(
            &SubscriptionChannel::OraclePrices { oracle_id: None },
            message.clone(),
        );
        self.broadcast_to_channel(
            &SubscriptionChannel::OraclePrices {
                oracle_id: Some(event.oracle_id),
            },
            message,
        );
    }

    /// Check for and remove stale connections
    fn check_stale_connections(&self) {
        let now = Utc::now();
        let timeout = Duration::from_secs(60);

        let stale: Vec<Uuid> = self
            .connections
            .iter()
            .filter_map(|entry| {
                let last_pong = *entry.metadata.last_pong.lock().unwrap();
                if now
                    .signed_duration_since(last_pong)
                    .to_std()
                    .unwrap_or_default()
                    > timeout
                {
                    Some(*entry.key())
                } else {
                    None
                }
            })
            .collect();

        for conn_id in stale {
            debug!("Removing stale connection: {}", conn_id);
            self.remove_connection(conn_id);
        }
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_remove_connection() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();

        manager.add_connection(conn_id, tx, Some("127.0.0.1".to_string()));
        assert_eq!(manager.get_connection_count(), 1);

        manager.remove_connection(conn_id);
        assert_eq!(manager.get_connection_count(), 0);
    }

    #[test]
    fn test_subscription_tracking() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let channel = SubscriptionChannel::Stats;

        manager.add_connection(conn_id, tx, None);
        manager.subscribe(conn_id, channel.clone());
        assert!(manager.is_subscribed(conn_id, &channel));

        manager.unsubscribe(conn_id, channel.clone());
        assert!(!manager.is_subscribed(conn_id, &channel));
    }

    #[test]
    fn test_remove_connection_clears_subscriptions() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::unbounded_channel();
        let channel = SubscriptionChannel::Stats;

        manager.add_connection(conn_id, tx, None);
        manager.subscribe(conn_id, channel.clone());
        assert!(manager.is_subscribed(conn_id, &channel));

        manager.remove_connection(conn_id);
        assert!(!manager.is_subscribed(conn_id, &channel));
    }
}
