use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use chrono::Utc;
use miden_client::account::AccountId;
use reqwest::header;
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tower_http::cors::CorsLayer;
use tracing::{debug, error, info};
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
struct SubmitNoteRequest {
    pub note_data: String, // Base64 encoded serialized note
}

#[derive(Debug, Serialize, Deserialize)]
struct MintRequest {
    pub address: String,
    pub faucet_id: String,
}
#[derive(Debug, Serialize, Deserialize)]
struct NewOrderRequest {
    id: Uuid,
    details: OrderDetails,
    order_type: OrderType,
    #[serde(serialize_with = "serialize_account_id")]
    #[serde(deserialize_with = "deserialize_account_id")]
    user_id: AccountId,
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

async fn order_new(
    State(state): State<AppState>,
    Json(payload): Json<NewOrderRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let order = Order::new(payload.pubkey, payload.user_id, payload.details);
    state
        .message_broker
        .broadcast_order_update(order.clone().into());
    let serialized_order: SerializableOrder = order.into();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=1, must-revalidate"),
    );

    Ok(Json(ApiResponse {
        data: serialized_order,
    }))
}
