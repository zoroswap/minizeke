use axum::{
    extract::{
        ConnectInfo, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    api::AppState,
    message_broker::messages::{ClientMessage, ServerMessage, SubscriptionChannel},
    order::OrderStatus,
};

fn is_expected_peer_disconnect(error: &axum::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    [
        "connection reset without closing handshake",
        "connection closed",
        "connection reset by peer",
        "broken pipe",
        "unexpected eof",
    ]
    .iter()
    .any(|expected| message.contains(expected))
}

/// WebSocket upgrade handler
pub async fn websocket_handler(
    State(state): State<AppState>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> axum::response::Response {
    let ip = state
        .ingress
        .client_ip(&headers, Some(peer))
        .map(|value| value.to_string());
    if !state.connection_manager.can_accept(ip.as_deref()) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "5")],
            "WebSocket connection capacity reached",
        )
            .into_response();
    }
    let max_message_bytes = state.ingress.ws_message_bytes;
    ws.max_message_size(max_message_bytes)
        .max_frame_size(max_message_bytes)
        .on_upgrade(|socket| handle_websocket_connection(socket, state, ip))
}

/// Handle a WebSocket connection
async fn handle_websocket_connection(socket: WebSocket, state: AppState, ip: Option<String>) {
    let conn_id = Uuid::new_v4();
    let (mut ws_sender, mut ws_receiver) = socket.split();

    let (tx, mut rx) = mpsc::channel::<Message>(state.ingress.ws_queue_capacity);
    let coalesced = Arc::new(Mutex::new(HashMap::new()));

    // Register connection
    debug!(conn_id = %conn_id, "New WebSocket connection established");
    if !state
        .connection_manager
        .add_connection(conn_id, tx, coalesced, ip)
    {
        return;
    }

    let manager = state.connection_manager.clone();
    let write_timeout = state.ingress.ws_write_timeout;
    let (closed_tx, mut closed_rx) = tokio::sync::oneshot::channel();
    let sender_task = tokio::spawn(async move {
        let mut flush = tokio::time::interval(std::time::Duration::from_millis(100));
        'send: loop {
            tokio::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    if !tokio::time::timeout(write_timeout, ws_sender.send(message))
                        .await
                        .is_ok_and(|result| result.is_ok())
                    {
                        break;
                    }
                },
                _ = flush.tick() => {
                    for message in manager.take_coalesced(conn_id) {
                        if !tokio::time::timeout(write_timeout, ws_sender.send(message))
                            .await
                            .is_ok_and(|result| result.is_ok())
                        {
                            break 'send;
                        }
                    }
                }
            }
        }
        let _ = closed_tx.send(());
    });

    let mut session_check = tokio::time::interval(state.ingress.ws_session_recheck);
    loop {
        let msg_result = tokio::select! {
            message = ws_receiver.next() => match message {
                Some(message) => message,
                None => break,
            },
            _ = session_check.tick() => {
                if !revalidate_session(conn_id, &state).await {
                    break;
                }
                continue;
            },
            _ = &mut closed_rx => break,
        };
        match msg_result {
            Ok(Message::Text(text)) => {
                if text.len() > state.ingress.ws_message_bytes {
                    warn!(conn_id = %conn_id, "WebSocket message exceeded configured limit");
                    break;
                }
                debug!(conn_id = %conn_id, "Received text message");
                if let Err(e) = handle_text_message(&text, conn_id, &state).await {
                    error!("Error handling text message: {}", e);
                }
            }
            Ok(Message::Ping(_)) => {
                debug!(conn_id = %conn_id, "Received ping");
                state.connection_manager.update_last_pong(conn_id);
            }
            Ok(Message::Pong(_)) => {
                debug!(conn_id = %conn_id, "Received pong");
                state.connection_manager.update_last_pong(conn_id);
            }
            Ok(Message::Close(_)) => {
                debug!(conn_id = %conn_id, "Client closed connection");
                break;
            }
            Ok(Message::Binary(_)) => {
                warn!(conn_id = %conn_id, "Received unexpected binary message");
            }
            Err(e) => {
                if is_expected_peer_disconnect(&e) {
                    debug!(conn_id = %conn_id, "WebSocket peer disconnected: {}", e);
                } else {
                    error!(conn_id = %conn_id, "WebSocket error: {}", e);
                }
                break;
            }
        }
    }

    // Cleanup
    state.connection_manager.remove_connection(conn_id);
    sender_task.abort();
    debug!(conn_id = %conn_id, "WebSocket connection closed");
}

#[cfg(test)]
mod disconnect_tests {
    use super::*;

    #[test]
    fn classifies_expected_peer_disconnects() {
        for message in [
            "Connection reset without closing handshake",
            "connection reset by peer",
            "broken pipe",
            "unexpected EOF",
        ] {
            let error = axum::Error::new(std::io::Error::other(message));
            assert!(is_expected_peer_disconnect(&error), "{message}");
        }
    }

    #[test]
    fn retains_unexpected_websocket_errors() {
        let error = axum::Error::new(std::io::Error::other("invalid websocket opcode"));
        assert!(!is_expected_peer_disconnect(&error));
    }
}

async fn revalidate_session(conn_id: Uuid, state: &AppState) -> bool {
    let Some(token) = state.connection_manager.session_token(conn_id) else {
        return true;
    };
    let store = state.auth_store.clone();
    let lookup_token = token.clone();
    let now = chrono::Utc::now().timestamp() as u64;
    match state
        .work_limits
        .database(move || Ok(store.lookup_session(&lookup_token, now)?))
        .await
    {
        Ok(Some(_)) => true,
        Ok(None) => {
            state.connection_manager.disconnect_session(&token);
            false
        }
        Err(error) => {
            warn!(%error, "WebSocket session revalidation unavailable");
            false
        }
    }
}

/// Handle a text message from the client
async fn handle_text_message(text: &str, conn_id: Uuid, state: &AppState) -> anyhow::Result<()> {
    let client_msg: ClientMessage = serde_json::from_str(text)?;
    handle_client_message(client_msg, conn_id, state).await;
    Ok(())
}

/// Handle a parsed client message
async fn handle_client_message(msg: ClientMessage, conn_id: Uuid, state: &AppState) {
    match msg {
        ClientMessage::Authenticate { token } => {
            let now = chrono::Utc::now().timestamp() as u64;
            let store = state.auth_store.clone();
            let lookup_token = token.clone();
            match state
                .work_limits
                .database(move || Ok(store.lookup_session(&lookup_token, now)?))
                .await
            {
                Ok(Some(session)) => {
                    let user_id = session.user_id.to_hex();
                    state.connection_manager.set_authenticated_user(
                        conn_id,
                        user_id.clone(),
                        token,
                    );
                    state.connection_manager.send_to_connection(
                        conn_id,
                        ServerMessage::Authenticated {
                            user_id,
                            expires_at: session.expires_at,
                        },
                    );
                }
                Ok(None) => state.connection_manager.send_to_connection(
                    conn_id,
                    ServerMessage::Error {
                        message: "expired or invalid session".to_owned(),
                    },
                ),
                Err(error) => state.connection_manager.send_to_connection(
                    conn_id,
                    ServerMessage::Error {
                        message: format!("authentication unavailable: {error}"),
                    },
                ),
            }
        }
        ClientMessage::Subscribe { channels } => {
            debug!(conn_id = %conn_id, "Client subscribing to {} channels", channels.len());
            for channel in channels {
                let authenticated = state.connection_manager.authenticated_user(conn_id);
                let mut current_order = None;
                let private_allowed = match &channel {
                    SubscriptionChannel::UserEvent {
                        user_id: Some(user_id),
                    } => authenticated.as_deref() == Some(user_id.as_str()),
                    SubscriptionChannel::UserEvent { user_id: None } => false,
                    SubscriptionChannel::Analytics {
                        user_id: Some(user_id),
                    } => authenticated.as_deref() == Some(user_id.as_str()),
                    SubscriptionChannel::Analytics { user_id: None } => false,
                    SubscriptionChannel::OrderUpdates {
                        order_id: Some(order_id),
                    } => {
                        if let Ok(id) = uuid::Uuid::parse_str(order_id) {
                            let history = state.history.clone();
                            let order = state
                                .work_limits
                                .database(move || Ok(history.order(id)?))
                                .await
                                .ok()
                                .flatten();
                            order.is_some_and(|order| {
                                let allowed =
                                    authenticated.as_deref() == Some(order.user_id.as_str());
                                if allowed {
                                    let status = match order.status.as_str() {
                                        "created" => Some(OrderStatus::Created),
                                        "processing" => Some(OrderStatus::Processing),
                                        "processed" => Some(OrderStatus::Processed),
                                        "submitted" => Some(OrderStatus::Submitted),
                                        "confirmed" => Some(OrderStatus::Confirmed),
                                        "executed" => Some(OrderStatus::Executed),
                                        "settled" => Some(OrderStatus::Settled),
                                        "failed" => Some(OrderStatus::Failed),
                                        _ => None,
                                    };
                                    current_order =
                                        status.map(|status| ServerMessage::OrderUpdate {
                                            order_id: order.id,
                                            status,
                                            timestamp: order.last_updated_at,
                                        });
                                }
                                allowed
                            })
                        } else {
                            false
                        }
                    }
                    SubscriptionChannel::OrderUpdates { order_id: None } => false,
                    _ => true,
                };
                if !private_allowed {
                    state.connection_manager.send_to_connection(
                        conn_id,
                        ServerMessage::Error {
                            message: "authentication does not authorize this subscription"
                                .to_owned(),
                        },
                    );
                    continue;
                }
                debug!(conn_id = %conn_id, channel = ?channel, "Subscribing to channel");
                if !state.connection_manager.subscribe(conn_id, channel.clone()) {
                    state.connection_manager.send_to_connection(
                        conn_id,
                        ServerMessage::Error {
                            message: "subscription limit exceeded".to_owned(),
                        },
                    );
                    continue;
                }
                state.connection_manager.send_to_connection(
                    conn_id,
                    ServerMessage::Subscribed {
                        channel: channel.clone(),
                    },
                );
                if let Some(update) = current_order {
                    state.connection_manager.send_to_connection(conn_id, update);
                }

                if matches!(channel, SubscriptionChannel::Stats) {
                    let stats = state.store.order_stats();
                    let timestamp = chrono::Utc::now().timestamp_millis() as u64;
                    state
                        .connection_manager
                        .send_to_connection(conn_id, ServerMessage::stats_update(stats, timestamp));
                }
            }
        }
        ClientMessage::Unsubscribe { channels } => {
            for channel in channels {
                state
                    .connection_manager
                    .unsubscribe(conn_id, channel.clone());
                state
                    .connection_manager
                    .send_to_connection(conn_id, ServerMessage::Unsubscribed { channel });
            }
        }
        ClientMessage::Ping => {
            state
                .connection_manager
                .send_to_connection(conn_id, ServerMessage::Pong);
            state.connection_manager.update_last_pong(conn_id);
        }
    }
}
