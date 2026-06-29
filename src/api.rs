use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose};
use chrono::Utc;
use miden_client::{
    Deserializable,
    account::AccountId,
    auth::{PublicKey, Signature},
};
use reqwest::header;
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    message_broker::message_broker::MessageBroker,
    order::{Order, OrderDetails, OrderType, SerializableOrder},
    serde::{deserialize_account_id, serialize_account_id},
    store::Store,
    websocket::{connection_manager::ConnectionManager, handlers::websocket_handler},
};

#[derive(Clone)]
pub struct AppState {
    pub connection_manager: Arc<ConnectionManager>,
    pub message_broker: Arc<MessageBroker>,
    pub store: Arc<Store>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewOrderRequest {
    id: Uuid,
    details: OrderDetails,
    order_type: OrderType,
    #[serde(serialize_with = "serialize_account_id")]
    #[serde(deserialize_with = "deserialize_account_id")]
    user_id: AccountId,
    signed_intent: String,
    pubkey: String,
}

#[derive(Serialize, Deserialize)]
struct ApiResponse<T: Serialize> {
    data: T,
}

#[derive(Debug)]
struct ApiError(anyhow::Error);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response<Body> {
        match self {
            ApiError(e) => {
                error!("Api error: {e:?}");
                (StatusCode::INTERNAL_SERVER_ERROR).into_response()
            }
        }
    }
}

pub async fn start(
    connection_manager: Arc<ConnectionManager>,
    message_broker: Arc<MessageBroker>,
    store: Arc<Store>,
) -> Result<()> {
    let server_url: &'static str = env::var("SERVER_URL").unwrap().leak();
    let app = create_router(AppState {
        connection_manager,
        message_broker,
        store,
    });
    let listener = tokio::net::TcpListener::bind(server_url)
        .await
        .unwrap_or_else(|err| panic!("Failed to bind TCP listener to {}: {err:?}", server_url));
    info!("Server listening on {}", server_url);
    println!("\n🚀 Zoro server is running!");
    println!("📡 Available endpoints:");
    println!("  GET  /health                    - Health check");
    println!("  GET  /users                     - List engine user account IDs");
    println!("  GET  /stats                     - Order count statistics");
    println!("  POST /orders/new                - Submit a new order");
    println!("  POST /withdraw/submit           - Submit a new withdrawal");
    println!("  POST /faucets/mint              - Mint from a faucet");
    println!("  GET  /ws                        - WebSocket connection");
    println!("🌐 Server address: {}", server_url);
    println!("📊 Example: {}/health", server_url);
    println!(
        "🔌 WebSocket: ws://{}/ws\n",
        server_url.replace("http://", "")
    );

    if let Err(e) = axum::serve(listener, app).await {
        error!("Critical error on server: {e}. Exiting with status 1.");
        std::process::exit(1);
    };
    Ok(())
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/pools/info", get(pool_info))
        .route("/users", get(users))
        .route("/stats", get(stats))
        .route("/ws", get(websocket_handler))
        .route("/orders/new", post(order_new))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health_check() -> impl IntoResponse {
    let response = serde_json::json!({
        "status": "healthy",
        "timestamp": Utc::now()
    });

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    (headers, Json(response))
}

async fn users(State(state): State<AppState>) -> impl IntoResponse {
    let users: Vec<_> = state
        .store
        .serialized_users()
        .into_iter()
        .map(|user| {
            serde_json::json!({
                "user_id": user.id,
                "private_key": user.signing_key,
                "index": user.index,
            })
        })
        .collect();

    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=60, must-revalidate"),
    );
    (headers, Json(serde_json::json!({ "users": users })))
}

async fn stats(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.store.order_stats();
    let timestamp = Utc::now().timestamp_millis() as u64;
    let response = serde_json::json!({
        "total_orders": stats.total,
        "open_orders": stats.open,
        "closed_orders": stats.closed,
        "by_status": stats.by_status,
        "timestamp": timestamp,
    });

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    (headers, Json(response))
}

async fn pool_info(State(state): State<AppState>) -> impl IntoResponse {
    let liq_pools = state.store.pool_states().clone();
    let mut liq_pools = liq_pools.iter();
    let pool0 = liq_pools.next();
    let pool1 = liq_pools.next();
    let (pool0_addr, pool0_state) = pool0.unwrap();
    let (pool1_addr, pool1_state) = pool1.unwrap();

    let response = serde_json::json!({
        "pool_account_id": state.store.pool_id().to_hex(),
        "liq_pools": vec![(pool0_addr.to_hex(), pool0_state), (pool1_addr.to_hex(), pool1_state)]
    });
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=120, must-revalidate"),
    );
    (headers, Json(response))
}

#[derive(Clone, Debug, Serialize)]
struct PoolBalancesResponse {
    faucet_id: String,
    reserve: String,
    reserve_with_slippage: String,
    total_liabilities: String,
}
#[derive(Clone, Debug, Serialize)]
struct PoolSettingsResponse {
    faucet_id: String,
    swap_fee: String,
    backstop_fee: String,
    protocol_fee: String,
}

async fn order_new(State(state): State<AppState>, body: Bytes) -> Result<Response<Body>, ApiError> {
    let raw = String::from_utf8_lossy(&body);
    // info!(body = %raw, "POST /orders/new received");

    let payload: NewOrderRequest = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(e) => {
            warn!(error = %e, body = %raw, "Rejected new order: could not parse payload");
            return Ok((
                StatusCode::BAD_REQUEST,
                format!("invalid order payload: {e}"),
            )
                .into_response());
        }
    };

    // info!(
    //     order_id = %payload.id,
    //     user_id = %payload.user_id.to_hex(),
    //     order_type = ?payload.order_type,
    //     asset_in = %payload.details.asset_in.to_hex(),
    //     amount_in = payload.details.amount_in,
    //     asset_out = %payload.details.asset_out.to_hex(),
    //     min_amount_out = payload.details.min_amount_out,
    //     "Accepted new order",
    // );

    let sig_bytes = general_purpose::STANDARD
        .decode(payload.signed_intent)
        .map_err(|e| ApiError(anyhow!("Failed to decode signature: {}", e)))?;

    let signature = Signature::read_from_bytes(&sig_bytes)
        .map_err(|e| ApiError(anyhow!("Failed to read signature from bytes: {}", e)))?;

    let pubkey_bytes = general_purpose::STANDARD
        .decode(payload.pubkey)
        .map_err(|e| ApiError(anyhow!("Failed to decode signature: {}", e)))?;

    let pubkey = PublicKey::read_from_bytes(&pubkey_bytes)
        .map_err(|e| ApiError(anyhow!("Failed to read signature from bytes: {}", e)))?;

    let order = Order::new(signature, payload.user_id, payload.details, pubkey);
    state
        .message_broker
        .broadcast_order_update(order.clone().into())
        .map_err(ApiError)?;

    let serialized_order: SerializableOrder = order.into();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=1, must-revalidate"),
    );

    Ok((
        headers,
        Json(ApiResponse {
            data: serialized_order,
        }),
    )
        .into_response())
}
