use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, Query, State},
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
    faucet::{DEFAULT_FAUCET_SERVER_URL, MintRequest},
    history::HistoryStore,
    market::derive_depth,
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
    pub history: Arc<HistoryStore>,
    /// Fixed internal endpoint of the separately-run faucet process. The main server
    /// never holds faucet keys or connects to its Miden client/store.
    pub faucet_server_url: String,
    pub faucet_http: reqwest::Client,
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
    history: Arc<HistoryStore>,
) -> Result<()> {
    let server_url: &'static str = env::var("SERVER_URL").unwrap().leak();
    let faucet_server_url = normalize_http_url(
        &env::var("FAUCET_SERVER_URL").unwrap_or_else(|_| DEFAULT_FAUCET_SERVER_URL.to_string()),
    );
    let app = create_router(AppState {
        connection_manager,
        message_broker,
        store,
        history,
        faucet_server_url,
        faucet_http: reqwest::Client::new(),
    });
    let listener = tokio::net::TcpListener::bind(server_url)
        .await
        .unwrap_or_else(|err| panic!("Failed to bind TCP listener to {}: {err:?}", server_url));
    info!("Server listening on {}", server_url);
    println!("\n🚀 Zoro server is running!");
    println!("📡 Available endpoints:");
    println!("  GET  /health                    - Health check");
    println!("  GET  /pools/info                - Pool states and asset ids");
    println!("  GET  /stats                     - Order count statistics");
    println!("  GET  /candles                   - Historical OHLCV candles");
    println!("  GET  /trades                    - Recent trade history");
    println!("  GET  /orders                    - Historical orders");
    println!("  GET  /orders/{{id}}             - Historical order by id");
    println!("  GET  /depth                     - Curve-derived market depth");
    println!("  POST /orders/new                - Submit a new order");
    println!("  POST /withdraw/submit           - Submit a new withdrawal");
    println!("  POST /mint                      - Request a faucet mint");
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

fn normalize_http_url(value: &str) -> String {
    let value = value.trim().trim_end_matches('/');
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else {
        format!("http://{value}")
    }
}

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/pools/info", get(pool_info))
        .route("/stats", get(stats))
        .route("/candles", get(candles))
        .route("/trades", get(trades))
        .route("/orders", get(orders))
        .route("/orders/{id}", get(order_by_id))
        .route("/depth", get(depth))
        .route("/mint", post(proxy_mint))
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

/// Proxies mint requests to the separately-run faucet service. This boundary keeps the
/// faucet's Miden client and signing keys out of the main trading/custody process.
async fn proxy_mint(
    State(state): State<AppState>,
    Json(request): Json<MintRequest>,
) -> Response<Body> {
    let endpoint = format!("{}/mint", state.faucet_server_url);
    match state.faucet_http.post(endpoint).json(&request).send().await {
        Ok(response) => {
            let status =
                StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            match response.text().await {
                Ok(body) => (
                    status,
                    [(reqwest::header::CONTENT_TYPE, "application/json")],
                    body,
                )
                    .into_response(),
                Err(error) => (
                    StatusCode::BAD_GATEWAY,
                    Json(serde_json::json!({
                        "success": false,
                        "message": format!("failed to read faucet response: {error}")
                    })),
                )
                    .into_response(),
            }
        }
        Err(error) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "success": false,
                "message": format!(
                    "faucet service is unavailable at {}: {error}",
                    state.faucet_server_url
                )
            })),
        )
            .into_response(),
    }
}

async fn stats(State(state): State<AppState>) -> Result<Response<Body>, ApiError> {
    let stats = state.store.order_stats();
    let trading = state.history.stats().map_err(ApiError)?;
    let timestamp = Utc::now().timestamp_millis() as u64;
    let response = serde_json::json!({
        "total_orders": stats.total,
        "open_orders": stats.open,
        "closed_orders": stats.closed,
        "by_status": stats.by_status,
        "trading": trading,
        "timestamp": timestamp,
    });

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    Ok((headers, Json(response)).into_response())
}

#[derive(Debug, Deserialize)]
struct CandlesQuery {
    #[serde(default = "default_candle_source")]
    source: String,
    pair: Option<String>,
    #[serde(default = "default_interval")]
    interval: String,
    from: Option<u64>,
    to: Option<u64>,
    limit: Option<u64>,
}

fn default_candle_source() -> String {
    "trades".to_string()
}

fn default_interval() -> String {
    "1m".to_string()
}

fn parse_interval(value: &str) -> Option<u64> {
    match value {
        "1m" => Some(60),
        "5m" => Some(300),
        "15m" => Some(900),
        "1h" => Some(3_600),
        "4h" => Some(14_400),
        _ => None,
    }
}

async fn candles(
    State(state): State<AppState>,
    Query(query): Query<CandlesQuery>,
) -> Response<Body> {
    let Some(interval) = parse_interval(&query.interval) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "interval must be one of 1m, 5m, 15m, 1h, 4h"
            })),
        )
            .into_response();
    };
    match state.history.candles(
        &query.source,
        query.pair.as_deref(),
        interval,
        query.from,
        query.to,
        query.limit.unwrap_or(500).clamp(1, 5_000),
    ) {
        Ok(candles) => Json(ApiResponse { data: candles }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    before: Option<u64>,
    limit: Option<u64>,
}

async fn trades(State(state): State<AppState>, Query(query): Query<PageQuery>) -> Response<Body> {
    match state
        .history
        .trades(query.before, query.limit.unwrap_or(100).clamp(1, 1_000))
    {
        Ok(trades) => Json(ApiResponse { data: trades }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct OrdersQuery {
    user_id: Option<String>,
    status: Option<String>,
    before: Option<u64>,
    limit: Option<u64>,
}

async fn orders(State(state): State<AppState>, Query(query): Query<OrdersQuery>) -> Response<Body> {
    match state.history.orders(
        query.user_id.as_deref(),
        query.status.as_deref(),
        query.before,
        query.limit.unwrap_or(100).clamp(1, 1_000),
    ) {
        Ok(orders) => Json(ApiResponse { data: orders }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

async fn order_by_id(State(state): State<AppState>, Path(id): Path<Uuid>) -> Response<Body> {
    match state.history.order(id) {
        Ok(Some(order)) => Json(ApiResponse { data: order }).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct DepthQuery {
    levels: Option<usize>,
}

async fn depth(State(state): State<AppState>, Query(query): Query<DepthQuery>) -> Response<Body> {
    let levels = query.levels.unwrap_or(20);
    if !(1..=100).contains(&levels) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "levels must be between 1 and 100"})),
        )
            .into_response();
    }

    let asset0 = state.store.asset0();
    let asset1 = state.store.asset1();
    let pools = state.store.pool_states();
    let Some(pool0) = pools.get(&asset0) else {
        return ApiError(anyhow!("base pool state unavailable")).into_response();
    };
    let Some(pool1) = pools.get(&asset1) else {
        return ApiError(anyhow!("quote pool state unavailable")).into_response();
    };
    let Some(price0) = state.store.oracle_price(asset0) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "base oracle price unavailable"})),
        )
            .into_response();
    };
    let Some(price1) = state.store.oracle_price(asset1) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "quote oracle price unavailable"})),
        )
            .into_response();
    };

    match derive_depth(
        asset0.to_hex(),
        asset1.to_hex(),
        pool0,
        pool1,
        price0,
        price1,
        levels,
        Utc::now().timestamp_millis() as u64,
    ) {
        Ok(depth) => Json(ApiResponse { data: depth }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
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
        "liq_pools": vec![(pool0_addr.to_hex(), pool0_state), (pool1_addr.to_hex(), pool1_state)],
        "asset0": state.store.asset0().to_hex(),
        "asset1": state.store.asset1().to_hex()
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

#[cfg(test)]
mod tests {
    use super::normalize_http_url;

    #[test]
    fn faucet_bind_address_becomes_an_http_url() {
        assert_eq!(
            normalize_http_url("127.0.0.1:7800"),
            "http://127.0.0.1:7800"
        );
        assert_eq!(
            normalize_http_url("https://faucet.example/"),
            "https://faucet.example"
        );
    }
}
