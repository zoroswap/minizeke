use axum::{
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};
use uuid::Uuid;

use crate::{
    api::AppState,
    message_broker::messages::{ClientMessage, ServerMessage, SubscriptionChannel},
};

/// WebSocket upgrade handler
pub async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_websocket_connection(socket, state))
}

/// Handle a WebSocket connection
async fn handle_websocket_connection(socket: WebSocket, state: AppState) {
    let conn_id = Uuid::new_v4();
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // Create channel for this connection
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Register connection
    debug!(conn_id = %conn_id, "New WebSocket connection established");
    state.connection_manager.add_connection(conn_id, tx, None); // TODO: Extract IP address from request

    // Spawn sender task: forwards messages from channel to WebSocket
    let sender_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if ws_sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Receiver loop: handle messages from client
    while let Some(msg_result) = ws_receiver.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
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
                error!(conn_id = %conn_id, "WebSocket error: {}", e);
                break;
            }
        }
    }

    // Cleanup
    state.connection_manager.remove_connection(conn_id);
    sender_task.abort();
    debug!(conn_id = %conn_id, "WebSocket connection closed");
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
            match state.auth_store.lookup_session(&token, now) {
                Ok(Some(session)) => {
                    let user_id = session.user_id.to_hex();
                    state
                        .connection_manager
                        .set_authenticated_user(conn_id, user_id.clone());
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
                    } => uuid::Uuid::parse_str(order_id)
                        .ok()
                        .and_then(|id| state.history.order(id).ok().flatten())
                        .is_some_and(|order| {
                            authenticated.as_deref() == Some(order.user_id.as_str())
                        }),
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
                state.connection_manager.subscribe(conn_id, channel.clone());
                state.connection_manager.send_to_connection(
                    conn_id,
                    ServerMessage::Subscribed {
                        channel: channel.clone(),
                    },
                );

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
