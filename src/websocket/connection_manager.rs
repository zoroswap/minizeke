use axum::extract::ws::Message;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{
    broadcast::error::RecvError,
    mpsc::{self, error::TrySendError},
};
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use crate::{
    ingress::IngressConfig,
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
    pub session_token: Arc<Mutex<Option<String>>>,
}

impl ConnectionMetadata {
    pub fn new(ip_address: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            connected_at: now,
            last_pong: Arc::new(Mutex::new(now)),
            ip_address,
            authenticated_user: Arc::new(Mutex::new(None)),
            session_token: Arc::new(Mutex::new(None)),
        }
    }
}

/// WebSocket sender with metadata
pub struct WebSocketSender {
    pub tx: mpsc::Sender<Message>,
    pub coalesced: Arc<Mutex<HashMap<String, Message>>>,
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
    config: IngressConfig,
    admission: Mutex<()>,
}

impl ConnectionManager {
    /// Create a new ConnectionManager without event broadcasting (for tests)
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            message_broker: None,
            config: IngressConfig::from_env(),
            admission: Mutex::new(()),
        }
    }

    /// Create a new ConnectionManager with event broadcasting
    pub fn with_message_broker(message_broker: Arc<MessageBroker>) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            message_broker: Some(message_broker),
            config: IngressConfig::from_env(),
            admission: Mutex::new(()),
        }
    }

    /// Add a new WebSocket connection
    pub fn can_accept(&self, ip_address: Option<&str>) -> bool {
        self.connections.len() < self.config.ws_global_cap
            && ip_address.is_none_or(|ip| {
                self.connections
                    .iter()
                    .filter(|entry| entry.metadata.ip_address.as_deref() == Some(ip))
                    .count()
                    < self.config.ws_per_ip_cap
            })
    }

    pub fn add_connection(
        &self,
        conn_id: Uuid,
        tx: mpsc::Sender<Message>,
        coalesced: Arc<Mutex<HashMap<String, Message>>>,
        ip_address: Option<String>,
    ) -> bool {
        let Ok(_admission) = self.admission.lock() else {
            return false;
        };
        if !self.can_accept(ip_address.as_deref()) {
            return false;
        }
        let metadata = ConnectionMetadata::new(ip_address.clone());
        self.connections.insert(
            conn_id,
            WebSocketSender {
                tx,
                coalesced,
                metadata,
            },
        );
        debug!(
            conn_id = %conn_id,
            ip = ?ip_address,
            "WebSocket connection established"
        );
        true
    }

    /// Remove a WebSocket connection and all its subscriptions
    pub fn remove_connection(&self, conn_id: Uuid) {
        self.connections.remove(&conn_id);

        let mut empty = Vec::new();
        for mut entry in self.subscriptions.iter_mut() {
            entry.value_mut().remove(&conn_id);
            if entry.value().is_empty() {
                empty.push(entry.key().clone());
            }
        }
        for channel in empty {
            self.subscriptions.remove(&channel);
        }

        debug!(conn_id = %conn_id, "WebSocket connection removed");
    }

    /// Subscribe a connection to a channel
    pub fn subscribe(&self, conn_id: Uuid, channel: SubscriptionChannel) -> bool {
        let count = self
            .subscriptions
            .iter()
            .filter(|entry| entry.value().contains(&conn_id))
            .count();
        if count >= self.config.ws_max_subscriptions && !self.is_subscribed(conn_id, &channel) {
            return false;
        }
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
        true
    }

    pub fn set_authenticated_user(&self, conn_id: Uuid, user_id: String, token: String) {
        if let Some(connection) = self.connections.get(&conn_id)
            && let Ok(mut authenticated) = connection.metadata.authenticated_user.lock()
        {
            *authenticated = Some(user_id);
            if let Ok(mut session_token) = connection.metadata.session_token.lock() {
                *session_token = Some(token);
            }
        }
    }

    pub fn disconnect_session(&self, token: &str) {
        let ids = self
            .connections
            .iter()
            .filter_map(|entry| {
                entry
                    .metadata
                    .session_token
                    .lock()
                    .ok()
                    .and_then(|value| (value.as_deref() == Some(token)).then_some(*entry.key()))
            })
            .collect::<Vec<_>>();
        for id in ids {
            self.remove_connection(id);
        }
    }

    pub fn take_coalesced(&self, conn_id: Uuid) -> Vec<Message> {
        self.connections
            .get(&conn_id)
            .and_then(|connection| {
                connection
                    .coalesced
                    .lock()
                    .ok()
                    .map(|mut pending| pending.drain().map(|(_, message)| message).collect())
            })
            .unwrap_or_default()
    }

    pub fn authenticated_user(&self, conn_id: Uuid) -> Option<String> {
        let connection = self.connections.get(&conn_id)?;
        let authenticated = connection.metadata.authenticated_user.lock().ok()?;
        authenticated.clone()
    }

    pub fn session_token(&self, conn_id: Uuid) -> Option<String> {
        let connection = self.connections.get(&conn_id)?;
        let token = connection.metadata.session_token.lock().ok()?;
        token.clone()
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

            if let Err(e) = conn.tx.try_send(Message::Text(json.into())) {
                warn!("Disconnecting slow connection {}: {}", conn_id, e);
                drop(conn);
                self.remove_connection(conn_id);
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

        let coalescing = matches!(
            channel,
            SubscriptionChannel::OraclePrices { .. }
                | SubscriptionChannel::PoolState { .. }
                | SubscriptionChannel::Stats
        );
        let key = format!("{channel:?}");
        let mut slow = Vec::new();
        for conn_id in subscribers.iter() {
            if let Some(conn) = self.connections.get(conn_id) {
                let message = Message::Text(json.clone().into());
                if let Err(error) = conn.tx.try_send(message.clone()) {
                    match error {
                        TrySendError::Full(_) if coalescing => {
                            if let Ok(mut pending) = conn.coalesced.lock() {
                                pending.insert(key.clone(), message);
                            }
                        }
                        TrySendError::Full(_) | TrySendError::Closed(_) => slow.push(*conn_id),
                    }
                }
            }
        }
        for conn_id in slow {
            warn!(%conn_id, "disconnecting slow WebSocket client");
            self.remove_connection(conn_id);
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
            let mut interval = tokio::time::interval(self.config.ws_ping_interval);
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
                                OrderUpdate::Submitted(e) => (e.id, OrderStatus::Submitted),
                                OrderUpdate::Confirmed(e) => (e.id, OrderStatus::Confirmed),
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
        let timeout = self.config.ws_pong_timeout;

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
        for connection in self.connections.iter() {
            if connection
                .tx
                .try_send(Message::Ping(Vec::new().into()))
                .is_err()
            {
                debug!(conn_id = %connection.key(), "WebSocket ping queue is full");
            }
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
        let (tx, _rx) = mpsc::channel(4);

        manager.add_connection(
            conn_id,
            tx,
            Arc::new(Mutex::new(HashMap::new())),
            Some("127.0.0.1".to_string()),
        );
        assert_eq!(manager.get_connection_count(), 1);

        manager.remove_connection(conn_id);
        assert_eq!(manager.get_connection_count(), 0);
    }

    #[test]
    fn test_subscription_tracking() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        let channel = SubscriptionChannel::Stats;

        manager.add_connection(conn_id, tx, Arc::new(Mutex::new(HashMap::new())), None);
        manager.subscribe(conn_id, channel.clone());
        assert!(manager.is_subscribed(conn_id, &channel));

        manager.unsubscribe(conn_id, channel.clone());
        assert!(!manager.is_subscribed(conn_id, &channel));
    }

    #[test]
    fn test_remove_connection_clears_subscriptions() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        let channel = SubscriptionChannel::Stats;

        manager.add_connection(conn_id, tx, Arc::new(Mutex::new(HashMap::new())), None);
        manager.subscribe(conn_id, channel.clone());
        assert!(manager.is_subscribed(conn_id, &channel));

        manager.remove_connection(conn_id);
        assert!(!manager.is_subscribed(conn_id, &channel));
    }

    #[test]
    fn connection_caps_are_enforced() {
        let mut manager = ConnectionManager::new();
        manager.config.ws_global_cap = 1;
        manager.config.ws_per_ip_cap = 1;
        let (tx, _rx) = mpsc::channel(1);
        assert!(manager.add_connection(
            Uuid::new_v4(),
            tx,
            Arc::new(Mutex::new(HashMap::new())),
            Some("127.0.0.1".to_owned()),
        ));
        assert!(!manager.can_accept(Some("127.0.0.1")));
        assert!(!manager.can_accept(Some("127.0.0.2")));
    }

    #[test]
    fn subscription_cap_and_session_disconnect_are_enforced() {
        let mut manager = ConnectionManager::new();
        manager.config.ws_max_subscriptions = 1;
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(4);
        assert!(manager.add_connection(
            conn_id,
            tx,
            Arc::new(Mutex::new(HashMap::new())),
            Some("127.0.0.1".to_owned()),
        ));
        manager.set_authenticated_user(conn_id, "user".to_owned(), "session-secret".to_owned());

        assert!(manager.subscribe(conn_id, SubscriptionChannel::Stats));
        assert!(manager.subscribe(conn_id, SubscriptionChannel::Stats));
        assert!(!manager.subscribe(conn_id, SubscriptionChannel::AmmEvent {}));
        assert_eq!(manager.get_connection_count(), 1);

        manager.disconnect_session("session-secret");
        assert_eq!(manager.get_connection_count(), 0);
        assert!(!manager.is_subscribed(conn_id, &SubscriptionChannel::Stats));
    }

    #[test]
    fn ephemeral_updates_coalesce_when_queue_is_full() {
        let manager = ConnectionManager::new();
        let conn_id = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(1);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        manager.add_connection(conn_id, tx.clone(), pending.clone(), None);
        manager.subscribe(conn_id, SubscriptionChannel::Stats);
        tx.try_send(Message::Ping(Vec::new().into())).unwrap();
        manager.broadcast_to_channel(
            &SubscriptionChannel::Stats,
            ServerMessage::stats_update(
                crate::order::OrderStats {
                    total: 0,
                    open: 0,
                    closed: 0,
                    by_status: crate::order::OrderStatusCounts {
                        created: 0,
                        processing: 0,
                        processed: 0,
                        submitted: 0,
                        confirmed: 0,
                        executed: 0,
                        settled: 0,
                        failed: 0,
                    },
                },
                1,
            ),
        );
        assert_eq!(pending.lock().unwrap().len(), 1);
        assert_eq!(manager.get_connection_count(), 1);
    }
}
