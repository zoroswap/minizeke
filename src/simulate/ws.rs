//! WebSocket client for waiting on order terminal status (`Confirmed` / `Failed`).

use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::time::timeout;
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

/// Long-lived (per-wait) WS session: authenticate, subscribe to one order, wait for terminal.
pub async fn wait_for_order_terminal(
    ws_url: &str,
    access_token: &str,
    order_id: Uuid,
    timeout_secs: u64,
) -> Result<TerminalOrderStatus> {
    let (stream, _) = connect_async(ws_url)
        .await
        .with_context(|| format!("connect websocket {ws_url}"))?;
    let (mut write, mut read) = stream.split();

    send_json(
        &mut write,
        &ClientMessage::Authenticate {
            token: access_token.to_owned(),
        },
    )
    .await?;

    // Wait for Authenticated (or Error) before subscribing.
    let auth_deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() >= auth_deadline {
            bail!("websocket auth timed out");
        }
        let remaining = auth_deadline.saturating_duration_since(Instant::now());
        let msg = timeout(remaining, read.next())
            .await
            .map_err(|_| anyhow!("websocket auth timed out"))?
            .ok_or_else(|| anyhow!("websocket closed during auth"))?
            .context("websocket read during auth")?;
        match parse_server_text(&msg)? {
            Some(WsInbound::Authenticated { .. }) => break,
            Some(WsInbound::Error { message }) => {
                bail!("websocket auth failed: {message}");
            }
            Some(other) => {
                debug!(?other, "ignoring pre-auth websocket message");
            }
            None => {}
        }
    }

    send_json(
        &mut write,
        &ClientMessage::Subscribe {
            channels: vec![SubscriptionChannel::OrderUpdates {
                order_id: Some(order_id.to_string()),
            }],
        },
    )
    .await?;

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if Instant::now() >= deadline {
            bail!("order {order_id} websocket wait timed out after {timeout_secs}s");
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let msg = timeout(remaining, read.next())
            .await
            .map_err(|_| anyhow!("order {order_id} websocket wait timed out"))?
            .ok_or_else(|| anyhow!("websocket closed while waiting for order {order_id}"))?
            .context("websocket read while waiting for order")?;

        if let Message::Ping(payload) = msg {
            write
                .send(Message::Pong(payload))
                .await
                .context("websocket pong")?;
            continue;
        }

        match parse_server_text(&msg)? {
            Some(WsInbound::OrderUpdate {
                order_id: update_id,
                status,
                ..
            }) if update_id == order_id.to_string() => match status {
                OrderStatus::Confirmed => return Ok(TerminalOrderStatus::Confirmed),
                OrderStatus::Failed => return Ok(TerminalOrderStatus::Failed),
                other => {
                    debug!(%order_id, ?other, "order status update");
                }
            },
            Some(WsInbound::Error { message }) => {
                warn!(%order_id, %message, "websocket error while waiting for order");
            }
            Some(WsInbound::Subscribed { .. }) | Some(WsInbound::Pong) => {}
            Some(_) | None => {}
        }
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
