use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, Response, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose};
use chrono::Utc;
use miden_client::{
    Deserializable, Serializable,
    account::AccountId,
    asset::FungibleAsset,
    auth::{PublicKey, Signature},
};
use miden_core::Word;
use reqwest::header;
use serde::{Deserialize, Serialize};
use std::{env, sync::Arc};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    analytics_store::{AnalyticsStore, Pagination},
    auth::AuthStore,
    faucet::{DEFAULT_FAUCET_SERVER_URL, MintRequest},
    fee_store::{FeeBatchRequest, FeeStore, FeeUpdateSource},
    history::HistoryStore,
    intent::Intent,
    lp::LpService,
    market::derive_depth,
    message_broker::message_broker::{FeeStateEvent, MessageBroker},
    order::{Order, OrderDetails, OrderType, SerializableOrder},
    pool::{fetch_account_storage_from_rpc, pool_cell_allocation_from_storage},
    serde::{deserialize_account_id, serialize_account_id},
    store::Store,
    vault::{user_placement_from_storage, vault_user_registration},
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
    pub lp_service: LpService,
    pub fee_store: Arc<FeeStore>,
    pub auth_store: Arc<AuthStore>,
    pub analytics_store: Arc<AnalyticsStore>,
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
    lp_service: LpService,
    fee_store: Arc<FeeStore>,
    auth_store: Arc<AuthStore>,
    analytics_store: Arc<AnalyticsStore>,
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
        lp_service,
        fee_store,
        auth_store,
        analytics_store,
    });
    let listener = tokio::net::TcpListener::bind(server_url)
        .await
        .unwrap_or_else(|err| panic!("Failed to bind TCP listener to {}: {err:?}", server_url));
    info!("Server listening on {}", server_url);
    println!("Server: {server_url}");
    println!("GET  /health /pools/info /pools/analytics /stats /candles /trades /depth");
    println!("GET  /orders /orders/{{id}} /users/{{id}}/placement /users/me/analytics /ws");
    println!("GET  /lp/operations/{{note_id}} /lp/positions/{{lp_id}}/{{faucet_id}}");
    println!("POST /auth/challenge /auth/login /orders/new /mint /lp/deposits/note");

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
        .route("/auth/challenge", post(auth_challenge))
        .route("/auth/login", post(auth_login))
        .route("/users/me/analytics", get(user_analytics))
        .route("/users/me/pnl", get(user_analytics))
        .route("/users/me/positions", get(user_positions))
        .route("/users/me/events", get(user_analytics_events))
        .route("/pools/analytics", get(pool_analytics_all))
        .route("/pools/{faucet_id}/analytics", get(pool_analytics))
        .route("/pools/info", get(pool_info))
        .route("/stats", get(stats))
        .route("/candles", get(candles))
        .route("/trades", get(trades))
        .route("/orders", get(orders))
        .route("/orders/{id}/events", get(order_events))
        .route("/orders/{id}", get(order_by_id))
        .route("/users/{id}/placement", get(user_placement))
        .route("/depth", get(depth))
        .route("/mint", post(proxy_mint))
        .route("/lp/deposits/note", post(build_lp_deposit_note))
        .route("/lp/operations/{note_id}", get(lp_operation))
        .route("/lp/positions/{lp_id}/{faucet_id}", get(lp_position))
        .route("/ws", get(websocket_handler))
        .route("/orders/new", post(order_new))
        .route("/internal/fees/batch", post(apply_automatic_fee_batch))
        .route("/admin/fees/override", post(apply_manual_fee_batch))
        .route("/admin/fees/clear", post(clear_manual_fee_batch))
        .layer(cors_layer())
        .with_state(state)
}

fn cors_layer() -> CorsLayer {
    let origins: Vec<HeaderValue> = env::var("CORS_ALLOWED_ORIGINS")
        .unwrap_or_else(|_| "http://localhost:3000,http://localhost:5173".to_owned())
        .split(',')
        .filter_map(|origin| origin.trim().parse().ok())
        .collect();
    CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])
}

#[derive(Debug, Deserialize)]
struct AuthChallengeRequest {
    user_id: String,
}

#[derive(Debug, Serialize)]
struct AuthChallengeResponse {
    challenge_id: String,
    user_id: String,
    nonce: String,
    message: [u64; 4],
    domain: String,
    network: String,
    issued_at: u64,
    expires_at: u64,
}

#[derive(Debug, Deserialize)]
struct AuthLoginRequest {
    challenge_id: String,
    user_id: String,
    pubkey: String,
    signature: String,
}

#[derive(Debug, Serialize)]
struct AuthLoginResponse {
    access_token: String,
    token_type: &'static str,
    expires_at: u64,
    user_id: String,
}

async fn auth_challenge(
    State(state): State<AppState>,
    Json(request): Json<AuthChallengeRequest>,
) -> Response<Body> {
    let user_id = match AccountId::from_hex(&request.user_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid user_id: {error}")),
    };
    let storage = match fetch_account_storage_from_rpc(state.store.vault_id()).await {
        Ok(storage) => storage,
        Err(error) => return service_unavailable(format!("vault unavailable: {error}")),
    };
    let commitment = match vault_user_registration(&storage, user_id) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return not_found("user has no registered trading key"),
        Err(error) => return service_unavailable(error.to_string()),
    };
    let now = Utc::now().timestamp() as u64;
    let challenge = match state.auth_store.issue_challenge(user_id, commitment, now) {
        Ok(challenge) => challenge,
        Err(error) => return service_unavailable(error.to_string()),
    };
    let mut message = [0_u64; 4];
    for (index, felt) in challenge.message.into_iter().enumerate() {
        message[index] = felt.as_canonical_u64();
    }
    Json(ApiResponse {
        data: AuthChallengeResponse {
            challenge_id: challenge.id,
            user_id: user_id.to_hex(),
            nonce: general_purpose::URL_SAFE_NO_PAD.encode(challenge.nonce),
            message,
            domain: env::var("AUTH_DOMAIN").unwrap_or_else(|_| "minizeke".to_owned()),
            network: env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_owned()),
            issued_at: challenge.issued_at,
            expires_at: challenge.expires_at,
        },
    })
    .into_response()
}

async fn auth_login(
    State(state): State<AppState>,
    Json(request): Json<AuthLoginRequest>,
) -> Response<Body> {
    let user_id = match AccountId::from_hex(&request.user_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid user_id: {error}")),
    };
    let pubkey = match general_purpose::STANDARD
        .decode(request.pubkey)
        .ok()
        .and_then(|bytes| PublicKey::read_from_bytes(&bytes).ok())
    {
        Some(value) => value,
        None => return bad_request("invalid public key"),
    };
    let signature = match general_purpose::STANDARD
        .decode(request.signature)
        .ok()
        .and_then(|bytes| Signature::read_from_bytes(&bytes).ok())
    {
        Some(value) => value,
        None => return bad_request("invalid signature"),
    };
    let storage = match fetch_account_storage_from_rpc(state.store.vault_id()).await {
        Ok(storage) => storage,
        Err(error) => return service_unavailable(format!("vault unavailable: {error}")),
    };
    let commitment = match vault_user_registration(&storage, user_id) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return not_found("user has no registered trading key"),
        Err(error) => return service_unavailable(error.to_string()),
    };
    let now = Utc::now().timestamp() as u64;
    match state.auth_store.authenticate(
        &request.challenge_id,
        user_id,
        commitment,
        pubkey,
        signature,
        now,
    ) {
        Ok(session) => Json(ApiResponse {
            data: AuthLoginResponse {
                access_token: session.bearer_token,
                token_type: "Bearer",
                expires_at: session.session.expires_at,
                user_id: session.session.user_id.to_hex(),
            },
        })
        .into_response(),
        Err(error) => error_response(StatusCode::UNAUTHORIZED, error.to_string()),
    }
}

fn authenticated_user(state: &AppState, headers: &HeaderMap) -> Result<AccountId, Response<Body>> {
    let token = crate::auth::parse_bearer(headers)
        .map_err(|_| error_response(StatusCode::UNAUTHORIZED, "missing or invalid bearer token"))?;
    let now = Utc::now().timestamp() as u64;
    state
        .auth_store
        .lookup_session(token.as_str(), now)
        .map_err(|error| service_unavailable(error.to_string()))?
        .map(|session| session.user_id)
        .ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "expired or revoked session"))
}

#[derive(Debug, Deserialize)]
struct AnalyticsQuery {
    offset: Option<u64>,
    limit: Option<u32>,
}

async fn user_analytics(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    let user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return response,
    };
    let now = Utc::now().timestamp_millis() as u64;
    match state
        .analytics_store
        .user_summary(&user.to_hex(), "oracle_usd", now)
    {
        Ok(summary) => match state.history.user_order_stats(&user.to_hex()) {
            Ok(order_stats) => Json(ApiResponse {
                data: serde_json::json!({
                    "pnl": summary,
                    "orders": order_stats,
                }),
            })
            .into_response(),
            Err(error) => ApiError(error).into_response(),
        },
        Err(error) => ApiError(error).into_response(),
    }
}

async fn user_positions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AnalyticsQuery>,
) -> Response<Body> {
    let user = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return response,
    };
    match state.analytics_store.positions(
        &user.to_hex(),
        "oracle_usd",
        Utc::now().timestamp_millis() as u64,
        Pagination {
            offset: query.offset.unwrap_or(0),
            limit: query.limit.unwrap_or(50).clamp(1, 500),
        },
    ) {
        Ok(positions) => Json(ApiResponse { data: positions }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

async fn user_analytics_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AnalyticsQuery>,
) -> Response<Body> {
    let user = match authenticated_user(&state, &headers) {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    match state.analytics_store.events_for_subject(
        &user,
        Pagination {
            offset: query.offset.unwrap_or(0),
            limit: query.limit.unwrap_or(50).clamp(1, 500),
        },
    ) {
        Ok(events) => Json(ApiResponse { data: events }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

async fn pool_analytics_all(State(state): State<AppState>) -> Response<Body> {
    let now = Utc::now().timestamp_millis() as u64;
    let mut analytics = Vec::new();
    for asset in state.store.assets() {
        match state
            .analytics_store
            .pool_summary(&asset.faucet_id.to_hex(), now)
        {
            Ok(Some(summary)) => analytics.push(summary),
            Ok(None) => {}
            Err(error) => return ApiError(error).into_response(),
        }
    }
    Json(ApiResponse { data: analytics }).into_response()
}

async fn pool_analytics(
    State(state): State<AppState>,
    Path(faucet_id): Path<String>,
) -> Response<Body> {
    match state
        .analytics_store
        .pool_summary(&faucet_id, Utc::now().timestamp_millis() as u64)
    {
        Ok(Some(summary)) => Json(ApiResponse { data: summary }).into_response(),
        Ok(None) => not_found("pool analytics unavailable"),
        Err(error) => ApiError(error).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct LpDepositNoteRequest {
    lp_id: String,
    faucet_id: String,
    amount: u64,
}

#[derive(Debug, Serialize)]
struct LpDepositNoteResponse {
    note_id: String,
    note: String,
    expected_lp_shares: u64,
    minimum_deposit: u64,
    pricing: &'static str,
}

async fn build_lp_deposit_note(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<LpDepositNoteRequest>,
) -> Response<Body> {
    let lp_id = match AccountId::from_hex(&request.lp_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid lp_id: {error}")),
    };
    let authenticated = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return response,
    };
    if authenticated != lp_id {
        return error_response(StatusCode::FORBIDDEN, "cannot create another LP's deposit");
    }
    let faucet_id = match AccountId::from_hex(&request.faucet_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid faucet_id: {error}")),
    };
    let asset = match FungibleAsset::new(faucet_id, request.amount) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid deposit asset: {error}")),
    };
    let pool_states = state.store.pool_states();
    let Some(pool) = pool_states.get(&faucet_id) else {
        return bad_request("unsupported LP asset");
    };
    let expected_lp_shares = match pool.get_deposit_lp_amount_out(U256::from(request.amount)) {
        Ok((shares, _, _)) => shares.saturating_to::<u64>(),
        Err(error) => return service_unavailable(format!("deposit quote unavailable: {error}")),
    };
    let note = match state.lp_service.build_deposit_note(lp_id, asset) {
        Ok(note) => note,
        Err(error) => return bad_request(error.to_string()),
    };
    Json(ApiResponse {
        data: LpDepositNoteResponse {
            note_id: note.note().id().to_hex(),
            note: general_purpose::STANDARD.encode(note.note().to_bytes()),
            expected_lp_shares,
            minimum_deposit: state.lp_service.minimum_deposit(),
            pricing: "execution_time",
        },
    })
    .into_response()
}

async fn lp_operation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(note_id): Path<String>,
) -> Response<Body> {
    let authenticated = match authenticated_user(&state, &headers) {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    match state.lp_service.store().operation(&note_id) {
        Ok(Some(operation)) if operation.lp_id == authenticated => {
            Json(ApiResponse { data: operation }).into_response()
        }
        Ok(Some(_)) => error_response(
            StatusCode::FORBIDDEN,
            "LP operation belongs to another user",
        ),
        Ok(None) => not_found("LP operation not found"),
        Err(error) => service_unavailable(format!("LP journal unavailable: {error}")),
    }
}

async fn lp_position(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((lp_id, faucet_id)): Path<(String, String)>,
) -> Response<Body> {
    let lp_id = match AccountId::from_hex(&lp_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid lp_id: {error}")),
    };
    let authenticated = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return response,
    };
    if authenticated != lp_id {
        return error_response(StatusCode::FORBIDDEN, "cannot access another LP's position");
    }
    let faucet_id = match AccountId::from_hex(&faucet_id) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid faucet_id: {error}")),
    };
    match state.lp_service.position(lp_id, faucet_id) {
        Ok(Some(position)) => Json(ApiResponse { data: position }).into_response(),
        Ok(None) => not_found("LP position not found"),
        Err(error) => service_unavailable(format!("LP journal unavailable: {error}")),
    }
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
    pair: String,
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
    if query.pair.trim().is_empty() {
        return bad_request("pair must not be empty");
    }
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
        Some(&query.pair),
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
struct TradesQuery {
    pair: Option<String>,
    user_id: Option<String>,
    before: Option<u64>,
    limit: Option<u64>,
}

async fn trades(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<TradesQuery>,
) -> Response<Body> {
    let pair = query
        .pair
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let requested_user_id = query
        .user_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if pair.is_none() && requested_user_id.is_none() {
        return bad_request("pair or user_id must be provided");
    }
    let user_id = if let Some(requested) = requested_user_id {
        let authenticated = match authenticated_user(&state, &headers) {
            Ok(user) => user,
            Err(response) => return response,
        };
        if requested != authenticated.to_hex() {
            return error_response(StatusCode::FORBIDDEN, "cannot access another user's trades");
        }
        Some(requested)
    } else {
        None
    };
    match state.history.trades(
        pair,
        user_id,
        query.before,
        query.limit.unwrap_or(100).clamp(1, 1_000),
    ) {
        Ok(trades) if user_id.is_some() => Json(ApiResponse { data: trades }).into_response(),
        Ok(trades) => {
            let redacted: Vec<_> = trades
                .into_iter()
                .map(|trade| {
                    serde_json::json!({
                        "order_id": trade.order_id,
                        "pair": trade.pair,
                        "asset_in": trade.asset_in,
                        "asset_out": trade.asset_out,
                        "amount_in": trade.amount_in,
                        "amount_out": trade.amount_out,
                        "price": trade.price,
                        "oracle_price": trade.oracle_price,
                        "timestamp": trade.timestamp,
                    })
                })
                .collect();
            Json(ApiResponse { data: redacted }).into_response()
        }
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

async fn orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<OrdersQuery>,
) -> Response<Body> {
    let user_id = match authenticated_user(&state, &headers) {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    if query
        .user_id
        .as_deref()
        .is_some_and(|value| value != user_id)
    {
        return error_response(StatusCode::FORBIDDEN, "cannot access another user's orders");
    }
    match state.history.orders(
        Some(&user_id),
        query.status.as_deref(),
        query.before,
        query.limit.unwrap_or(100).clamp(1, 1_000),
    ) {
        Ok(orders) => Json(ApiResponse { data: orders }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

async fn order_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response<Body> {
    let user_id = match authenticated_user(&state, &headers) {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    match state.history.order(id) {
        Ok(Some(order)) if order.user_id == user_id => {
            Json(ApiResponse { data: order }).into_response()
        }
        Ok(Some(_)) => error_response(StatusCode::FORBIDDEN, "order belongs to another user"),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

async fn order_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response<Body> {
    let user_id = match authenticated_user(&state, &headers) {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    match state.history.order(id) {
        Ok(Some(order)) if order.user_id == user_id => {}
        Ok(Some(_)) => {
            return error_response(StatusCode::FORBIDDEN, "order belongs to another user");
        }
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return ApiError(error).into_response(),
    }
    match state.history.order_events(id) {
        Ok(events) => Json(ApiResponse { data: events }).into_response(),
        Err(error) => ApiError(error).into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct DepthQuery {
    base: String,
    quote: String,
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

    let base = match AccountId::from_hex(&query.base) {
        Ok(asset) => asset,
        Err(error) => {
            return bad_request(format!("invalid base account id: {error}"));
        }
    };
    let quote = match AccountId::from_hex(&query.quote) {
        Ok(asset) => asset,
        Err(error) => {
            return bad_request(format!("invalid quote account id: {error}"));
        }
    };
    if base == quote {
        return bad_request("base and quote must be distinct assets");
    }
    if !state
        .store
        .assets()
        .iter()
        .any(|asset| asset.faucet_id == base)
    {
        return bad_request("base asset is not listed");
    }
    if !state
        .store
        .assets()
        .iter()
        .any(|asset| asset.faucet_id == quote)
    {
        return bad_request("quote asset is not listed");
    }

    let pools = state.store.pool_states();
    let Some(base_pool) = pools.get(&base) else {
        return service_unavailable("base pool state unavailable");
    };
    let Some(quote_pool) = pools.get(&quote) else {
        return service_unavailable("quote pool state unavailable");
    };
    let Some(base_price) = state.store.oracle_price(base) else {
        return service_unavailable("base oracle price unavailable");
    };
    let Some(quote_price) = state.store.oracle_price(quote) else {
        return service_unavailable("quote oracle price unavailable");
    };

    match derive_depth(
        base.to_hex(),
        quote.to_hex(),
        base_pool,
        quote_pool,
        base_price,
        quote_price,
        levels,
        Utc::now().timestamp_millis() as u64,
    ) {
        Ok(depth) => Json(ApiResponse { data: depth }).into_response(),
        Err(error) => service_unavailable(format!("market depth unavailable: {error}")),
    }
}

async fn pool_info(State(state): State<AppState>) -> Response<Body> {
    let pool_states = state.store.pool_states();
    let mut assets = Vec::with_capacity(state.store.assets().len());
    for asset in state.store.assets() {
        let Some(pool_state) = pool_states.get(&asset.faucet_id) else {
            return service_unavailable(format!(
                "pool state unavailable for asset {}",
                asset.faucet_id.to_hex()
            ));
        };
        assets.push(serde_json::json!({
            "asset": asset,
            "pool_state": pool_state,
            "fees": {
                "base_swap": pool_state.settings().swap_fee.to_string(),
                "base_backstop": pool_state.settings().backstop_fee.to_string(),
                "base_protocol": pool_state.settings().protocol_fee.to_string(),
                "volatility_fee_in": pool_state.settings().volatility_fee_in.to_string(),
                "volatility_fee_out": pool_state.settings().volatility_fee_out.to_string(),
                "maximum_effective_fee_out": (
                    pool_state.settings().swap_fee
                        + pool_state.settings().backstop_fee
                        + pool_state.settings().protocol_fee
                        + pool_state.settings().volatility_fee_out
                ).to_string(),
                "valid_until": pool_state.settings().volatility_fee_valid_until,
                "version": pool_state.settings().volatility_fee_version,
                "source": pool_state.settings().volatility_fee_source,
                "precision": 1_000_000u64,
            },
        }));
    }
    let response = serde_json::json!({
        "assets": assets,
        "pool_shard_ids": state.store.pools().iter().map(|id| id.to_hex()).collect::<Vec<_>>(),
    });
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    (headers, Json(response)).into_response()
}

async fn apply_automatic_fee_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<FeeBatchRequest>,
) -> Response<Body> {
    request.source = FeeUpdateSource::Automatic;
    apply_fee_batch(state, headers, request).await
}

async fn apply_manual_fee_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<FeeBatchRequest>,
) -> Response<Body> {
    request.source = FeeUpdateSource::Manual;
    apply_fee_batch(state, headers, request).await
}

async fn clear_manual_fee_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response<Body> {
    if !valid_fee_admin_token(&headers) {
        return (StatusCode::UNAUTHORIZED, "invalid fee updater token").into_response();
    }
    let now = Utc::now().timestamp() as u64;
    match state.fee_store.clear_manual(now) {
        Ok(cleared) => {
            let _ = state.message_broker.broadcast_fee_state(FeeStateEvent {
                fee_states: state.fee_store.active_states(now).unwrap_or_default(),
                timestamp: Utc::now().timestamp_millis() as u64,
            });
            Json(ApiResponse {
                data: serde_json::json!({"cleared": cleared}),
            })
            .into_response()
        }
        Err(error) => service_unavailable(error.to_string()),
    }
}

async fn apply_fee_batch(
    state: AppState,
    headers: HeaderMap,
    request: FeeBatchRequest,
) -> Response<Body> {
    if !valid_fee_admin_token(&headers) {
        return (StatusCode::UNAUTHORIZED, "invalid fee updater token").into_response();
    }
    let now = Utc::now().timestamp() as u64;
    let assets: Vec<_> = state
        .store
        .assets()
        .iter()
        .map(|asset| asset.faucet_id)
        .collect();
    let applied = match state.fee_store.apply_batch(&request, &assets, now) {
        Ok(applied) => applied,
        Err(error) => return bad_request(error.to_string()),
    };
    let fee_states = match state.fee_store.active_states(now) {
        Ok(states) => states,
        Err(error) => return service_unavailable(error.to_string()),
    };
    let _ = state.message_broker.broadcast_fee_state(FeeStateEvent {
        fee_states,
        timestamp: Utc::now().timestamp_millis() as u64,
    });
    Json(ApiResponse { data: applied }).into_response()
}

fn valid_fee_admin_token(headers: &HeaderMap) -> bool {
    let Ok(expected) = env::var("FEE_UPDATER_TOKEN") else {
        return false;
    };
    !expected.is_empty()
        && headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            == Some(expected.as_str())
}

async fn user_placement(State(state): State<AppState>, Path(id): Path<String>) -> Response<Body> {
    let user_id = match AccountId::from_hex(&id) {
        Ok(user_id) => user_id,
        Err(error) => return bad_request(format!("invalid user account id: {error}")),
    };
    let vault_storage = match fetch_account_storage_from_rpc(state.store.vault_id()).await {
        Ok(storage) => storage,
        Err(error) => {
            warn!(%error, "failed to fetch vault storage for placement");
            return service_unavailable("vault storage is unavailable");
        }
    };
    let pool_id = match user_placement_from_storage(&vault_storage, user_id) {
        Ok(Some(pool_id)) => pool_id,
        Ok(None) => return not_found("user is not registered"),
        Err(error) => {
            error!(%error, "invalid user placement in vault storage");
            return service_unavailable("user placement is unavailable");
        }
    };
    if !state.store.pools().contains(&pool_id) {
        return service_unavailable("assigned pool shard is not configured");
    }
    let pool_storage = match fetch_account_storage_from_rpc(pool_id).await {
        Ok(storage) => storage,
        Err(error) => {
            warn!(%error, "failed to fetch pool storage for placement");
            return service_unavailable("assigned pool shard is unavailable");
        }
    };

    let mut assets = Vec::with_capacity(state.store.assets().len());
    for asset in state.store.assets() {
        let allocation =
            match pool_cell_allocation_from_storage(&pool_storage, asset.faucet_id, user_id) {
                Ok(allocation) => allocation,
                Err(error) => {
                    error!(%error, asset = %asset.faucet_id.to_hex(), "failed to read pool cell");
                    return service_unavailable("pool placement storage is invalid");
                }
            };
        assets.push(serde_json::json!({
            "asset_id": asset.faucet_id.to_hex(),
            "cell_slot_id": allocation.as_ref().map(|value| &value.slot_id),
            "bought": allocation.as_ref().map(|value| value.bought),
            "sold": allocation.as_ref().map(|value| value.sold),
        }));
    }

    Json(ApiResponse {
        data: serde_json::json!({
            "user_id": user_id.to_hex(),
            "pool_shard_id": pool_id.to_hex(),
            "assets": assets,
        }),
    })
    .into_response()
}

fn error_response(status: StatusCode, message: impl Into<String>) -> Response<Body> {
    (status, Json(serde_json::json!({"error": message.into()}))).into_response()
}

fn bad_request(message: impl Into<String>) -> Response<Body> {
    error_response(StatusCode::BAD_REQUEST, message)
}

fn not_found(message: impl Into<String>) -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, message)
}

fn service_unavailable(message: impl Into<String>) -> Response<Body> {
    error_response(StatusCode::SERVICE_UNAVAILABLE, message)
}

async fn order_new(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response<Body>, ApiError> {
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
    let authenticated = match authenticated_user(&state, &headers) {
        Ok(user) => user,
        Err(response) => return Ok(response),
    };
    if authenticated != payload.user_id {
        return Ok(error_response(
            StatusCode::FORBIDDEN,
            "order user_id does not match bearer session",
        ));
    }

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

    let vault_storage = fetch_account_storage_from_rpc(state.store.vault_id())
        .await
        .map_err(ApiError)?;
    let registered = vault_user_registration(&vault_storage, payload.user_id)
        .map_err(ApiError)?
        .ok_or_else(|| ApiError(anyhow!("user has no registered trading key")))?;
    if Word::from(pubkey.to_commitment()) != registered {
        return Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "public key is not registered for this user",
        ));
    }
    let intent = Intent {
        user_suffix: payload.user_id.suffix().as_canonical_u64(),
        user_prefix: payload.user_id.prefix().as_u64(),
        sell_asset_suffix: payload.details.asset_in.suffix().as_canonical_u64(),
        sell_asset_prefix: payload.details.asset_in.prefix().as_u64(),
        sell_amount: payload.details.amount_in,
        buy_asset_suffix: payload.details.asset_out.suffix().as_canonical_u64(),
        buy_asset_prefix: payload.details.asset_out.prefix().as_u64(),
        buy_amount: payload.details.min_amount_out,
    };
    if !pubkey.verify(intent.message_word(), signature.clone()) {
        return Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "invalid signed intent",
        ));
    }

    let order = Order::new_with_id(
        payload.id,
        signature,
        payload.user_id,
        payload.details,
        pubkey,
    );
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
