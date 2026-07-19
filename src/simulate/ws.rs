//! WebSocket client for waiting on order terminal status (`Confirmed` / `Failed`).

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::timeout,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    message_broker::messages::{ClientMessage, SubscriptionChannel},
    order::OrderStatus,
};

/// Derive `ws://…/ws` or `wss://…/ws` from the HTTP API base URL.
pub fn ws_url_from_api(api_url: &str) -> Result<String> {
    let trimmed = api_url.trim_end_matches('/');
    let ws = if let Some(rest) = trimmed.strip_prefix("https://") {
        format!("wss://{rest}/ws")
    } else if let Some(rest) = trimmed.strip_prefix("http://") {
        format!("ws://{rest}/ws")
    } else {
        bail!("unsupported api url for websocket: {api_url}");
    };
    Ok(ws)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalOrderStatus {
    Confirmed,
    Failed,
}

#[derive(Clone)]
pub struct SettlementTracker {
    commands: mpsc::Sender<TrackerCommand>,
    order_timeout: Duration,
}

struct PendingOrder {
    deadline: Instant,
    result: oneshot::Sender<std::result::Result<TerminalOrderStatus, String>>,
}

enum TrackerCommand {
    Track {
        order_id: Uuid,
        deadline: Instant,
        result: oneshot::Sender<std::result::Result<TerminalOrderStatus, String>>,
    },
    UpdateToken(String),
}

impl SettlementTracker {
    pub fn spawn(
        ws_url: String,
        access_token: String,
        timeout_secs: u64,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let (commands, receiver) = mpsc::channel(16);
        tokio::spawn(run_settlement_tracker(
            ws_url,
            access_token,
            receiver,
            shutdown,
        ));
        Self {
            commands,
            order_timeout: Duration::from_secs(timeout_secs),
        }
    }

    pub async fn track(&self, order_id: Uuid) -> Result<TerminalOrderStatus> {
        let (result, receiver) = oneshot::channel();
        self.commands
            .send(TrackerCommand::Track {
                order_id,
                deadline: Instant::now() + self.order_timeout,
                result,
            })
            .await
            .context("settlement tracker stopped")?;
        receiver
            .await
            .context("settlement tracker dropped order")?
            .map_err(anyhow::Error::msg)
    }

    pub async fn update_token(&self, access_token: String) {
        let _ = self
            .commands
            .send(TrackerCommand::UpdateToken(access_token))
            .await;
    }
}

async fn run_settlement_tracker(
    ws_url: String,
    mut access_token: String,
    mut commands: mpsc::Receiver<TrackerCommand>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut pending = HashMap::<Uuid, PendingOrder>::new();
    loop {
        loop {
            match commands.try_recv() {
                Ok(TrackerCommand::Track {
                    order_id,
                    deadline,
                    result,
                }) => {
                    pending.insert(order_id, PendingOrder { deadline, result });
                }
                Ok(TrackerCommand::UpdateToken(token)) => access_token = token,
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    fail_pending(&mut pending, "settlement tracker stopped");
                    return;
                }
            }
        }
        if *shutdown.borrow() {
            fail_pending(&mut pending, "simulator shutting down");
            return;
        }
        expire_pending(&mut pending);
        let connected = timeout(Duration::from_secs(5), connect_async(&ws_url)).await;
        let (stream, _) = match connected {
            Ok(Ok(connected)) => connected,
            Ok(Err(error)) => {
                debug!(%error, "settlement websocket connect failed; retrying");
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            fail_pending(&mut pending, "simulator shutting down");
                            return;
                        }
                    }
                }
                continue;
            }
            Err(_) => {
                debug!("settlement websocket connect timed out; retrying");
                continue;
            }
        };
        let (mut write, mut read) = stream.split();
        if let Err(error) = send_json(
            &mut write,
            &ClientMessage::Authenticate {
                token: access_token.clone(),
            },
        )
        .await
        {
            debug!(%error, "settlement websocket auth send failed; reconnecting");
            continue;
        }
        let authenticated = timeout(Duration::from_secs(15), async {
            loop {
                let message = read
                    .next()
                    .await
                    .ok_or_else(|| anyhow!("websocket closed during auth"))?
                    .context("websocket read during auth")?;
                match parse_server_text(&message)? {
                    Some(WsInbound::Authenticated { .. }) => return Ok(()),
                    Some(WsInbound::Error { message }) => {
                        bail!("websocket auth failed: {message}");
                    }
                    _ => {}
                }
            }
        })
        .await;
        match authenticated {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, "settlement websocket authentication failed");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(_) => {
                debug!("settlement websocket authentication timed out");
                continue;
            }
        }
        if !pending.is_empty()
            && send_subscriptions(&mut write, pending.keys().copied())
                .await
                .is_err()
        {
            continue;
        }

        let mut expiry = tokio::time::interval(Duration::from_secs(1));
        expiry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut reconnect = false;
        while !reconnect {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(TrackerCommand::Track { order_id, deadline, result }) => {
                        pending.insert(order_id, PendingOrder { deadline, result });
                        if send_subscriptions(&mut write, [order_id]).await.is_err() {
                            reconnect = true;
                        }
                    }
                    Some(TrackerCommand::UpdateToken(token)) => {
                        access_token = token;
                        reconnect = true;
                    }
                    None => {
                        fail_pending(&mut pending, "settlement tracker stopped");
                        return;
                    }
                },
                message = read.next() => match message {
                    Some(Ok(Message::Ping(payload))) => {
                        if write.send(Message::Pong(payload)).await.is_err() {
                            reconnect = true;
                        }
                    }
                    Some(Ok(message)) => match parse_server_text(&message) {
                        Ok(Some(WsInbound::OrderUpdate { order_id, status, .. })) => {
                            if let Ok(order_id) = Uuid::parse_str(&order_id) {
                                let terminal = match status {
                                    OrderStatus::Confirmed => Some(TerminalOrderStatus::Confirmed),
                                    OrderStatus::Failed => Some(TerminalOrderStatus::Failed),
                                    _ => None,
                                };
                                if let (Some(status), Some(order)) =
                                    (terminal, pending.remove(&order_id))
                                {
                                    let _ = order.result.send(Ok(status));
                                }
                            }
                        }
                        Ok(Some(WsInbound::Error { message })) => {
                            debug!(%message, "settlement websocket server error");
                        }
                        Ok(_) => {}
                        Err(error) => debug!(%error, "invalid settlement websocket message"),
                    },
                    Some(Err(error)) => {
                        debug!(%error, "settlement websocket disconnected; reconnecting");
                        reconnect = true;
                    }
                    None => reconnect = true,
                },
                _ = expiry.tick() => expire_pending(&mut pending),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        fail_pending(&mut pending, "simulator shutting down");
                        return;
                    }
                }
            }
        }
    }
}

async fn send_subscriptions<S>(
    write: &mut S,
    order_ids: impl IntoIterator<Item = Uuid>,
) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let message = subscription_message(order_ids);
    send_json(write, &message).await
}

fn subscription_message(order_ids: impl IntoIterator<Item = Uuid>) -> ClientMessage {
    ClientMessage::Subscribe {
        channels: order_ids
            .into_iter()
            .map(|order_id| SubscriptionChannel::OrderUpdates {
                order_id: Some(order_id.to_string()),
            })
            .collect(),
    }
}

fn expire_pending(pending: &mut HashMap<Uuid, PendingOrder>) {
    let now = Instant::now();
    let expired = pending
        .iter()
        .filter_map(|(order_id, order)| (order.deadline <= now).then_some(*order_id))
        .collect::<Vec<_>>();
    for order_id in expired {
        if let Some(order) = pending.remove(&order_id) {
            let _ = order
                .result
                .send(Err(format!("order {order_id} settlement timed out")));
        }
    }
}

fn fail_pending(pending: &mut HashMap<Uuid, PendingOrder>, reason: &str) {
    for (order_id, order) in pending.drain() {
        let _ = order
            .result
            .send(Err(format!("order {order_id}: {reason}")));
    }
}

async fn send_json<S>(write: &mut S, message: &ClientMessage) -> Result<()>
where
    S: SinkExt<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let json = serde_json::to_string(message).context("serialize websocket client message")?;
    write
        .send(Message::Text(json.into()))
        .await
        .context("send websocket client message")?;
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum WsInbound {
    Authenticated {
        #[allow(dead_code)]
        user_id: String,
        #[allow(dead_code)]
        expires_at: u64,
    },
    Subscribed {
        #[serde(default)]
        #[allow(dead_code)]
        channel: serde_json::Value,
    },
    OrderUpdate {
        order_id: String,
        status: OrderStatus,
        #[allow(dead_code)]
        timestamp: u64,
    },
    Pong,
    Error {
        message: String,
    },
    #[serde(other)]
    Other,
}

fn parse_server_text(message: &Message) -> Result<Option<WsInbound>> {
    let Message::Text(text) = message else {
        return Ok(None);
    };
    let inbound: WsInbound =
        serde_json::from_str(text).with_context(|| format!("decode websocket message: {text}"))?;
    Ok(Some(inbound))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        time::{Duration, Instant},
    };

    use tokio::sync::oneshot;
    use uuid::Uuid;

    use super::{PendingOrder, expire_pending, subscription_message};

    #[tokio::test]
    async fn reconnect_bookkeeping_resubscribes_only_pending_orders() {
        let expired_id = Uuid::new_v4();
        let pending_id = Uuid::new_v4();
        let (expired_tx, expired_rx) = oneshot::channel();
        let (pending_tx, _pending_rx) = oneshot::channel();
        let mut pending = HashMap::from([
            (
                expired_id,
                PendingOrder {
                    deadline: Instant::now() - Duration::from_secs(1),
                    result: expired_tx,
                },
            ),
            (
                pending_id,
                PendingOrder {
                    deadline: Instant::now() + Duration::from_secs(60),
                    result: pending_tx,
                },
            ),
        ]);

        expire_pending(&mut pending);

        assert!(expired_rx.await.unwrap().is_err());
        assert!(!pending.contains_key(&expired_id));
        let json = serde_json::to_string(&subscription_message(pending.keys().copied())).unwrap();
        assert!(json.contains(&pending_id.to_string()));
        assert!(!json.contains(&expired_id.to_string()));
    }
}
