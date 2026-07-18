use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, HeaderValue, Method, Response, StatusCode},
    middleware,
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
use std::{env, net::SocketAddr, sync::Arc};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::{limit::RequestBodyLimitLayer, timeout::TimeoutLayer};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    analytics_store::{AnalyticsStore, Pagination},
    auth::AuthStore,
    execution_store::{ExecutionStore, IntentReservation},
    faucet::{DEFAULT_FAUCET_SERVER_URL, MintRequest},
    fee_store::{FeeBatchRequest, FeeStore, FeeUpdateSource},
    history::HistoryStore,
    ingress::{IngressConfig, IngressState, WorkLimits},
    intent::{INTENT_DOMAIN_TAG, INTENT_VERSION, Intent, TESTNET_NETWORK_TAG, is_expired_at},
    lp::LpService,
    market::derive_depth,
    message_broker::message_broker::{FeeStateEvent, MessageBroker},
    order::{Order, OrderDetails, OrderType, SerializableOrder},
    pool::{fetch_account_storage_from_rpc, pool_cell_allocation_from_storage},
    serde::{deserialize_account_id, serialize_account_id},
    service_auth::ServiceCredentials,
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
    pub faucet_credentials: ServiceCredentials,
    pub public_mint_enabled: bool,
    pub lp_service: LpService,
    pub fee_store: Arc<FeeStore>,
    pub auth_store: Arc<AuthStore>,
    pub analytics_store: Arc<AnalyticsStore>,
    pub execution_store: Arc<ExecutionStore>,
    pub ingress: IngressConfig,
    pub work_limits: WorkLimits,
    pub fee_updater_credentials: ServiceCredentials,
    pub fee_admin_credentials: ServiceCredentials,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NewOrderRequest {
    version: u8,
    client_order_id: Uuid,
    expires_at: u64,
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
    execution_store: Arc<ExecutionStore>,
) -> Result<()> {
    let server_url: &'static str = env::var("SERVER_URL").unwrap().leak();
    let faucet_server_url = normalize_http_url(
        &env::var("FAUCET_SERVER_URL").unwrap_or_else(|_| DEFAULT_FAUCET_SERVER_URL.to_string()),
    );
    ensure_loopback_url(&faucet_server_url, "FAUCET_SERVER_URL")?;
    let faucet_credentials =
        ServiceCredentials::from_env("FAUCET_SERVICE_TOKEN", "FAUCET_SERVICE_TOKEN_NEXT")?;
    let public_mint_enabled = env::var("PUBLIC_MINT_ENABLED")
        .ok()
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1");
    let fee_updater_credentials =
        ServiceCredentials::from_env("FEE_UPDATER_TOKEN", "FEE_UPDATER_TOKEN_NEXT")?;
    let fee_admin_credentials =
        ServiceCredentials::from_env("FEE_ADMIN_TOKEN", "FEE_ADMIN_TOKEN_NEXT")?;
    let ingress = IngressConfig::from_env();
    let work_limits = WorkLimits::new(&ingress);
    let state = AppState {
        connection_manager,
        message_broker,
        store,
        history,
        faucet_server_url,
        faucet_http: reqwest::Client::new(),
        faucet_credentials,
        public_mint_enabled,
        lp_service,
        fee_store,
        auth_store,
        analytics_store,
        execution_store,
        ingress,
        work_limits,
        fee_updater_credentials,
        fee_admin_credentials,
    };
    let app = create_router(state.clone());
    let admin_app = create_admin_router(state);
    let listener = tokio::net::TcpListener::bind(server_url)
        .await
        .unwrap_or_else(|err| panic!("Failed to bind TCP listener to {}: {err:?}", server_url));
    info!("Server listening on {}", server_url);
    println!("Server: {server_url}");
    println!("GET  /health /pools/info /pools/analytics /stats /candles /trades /depth");
    println!("GET  /orders /orders/{{id}} /users/{{id}}/placement /users/me/analytics /ws");
    println!("GET  /lp/operations/{{note_id}} /lp/positions/{{lp_id}}/{{faucet_id}}");
    println!("POST /auth/challenge /auth/login /orders/new /mint /lp/deposits/note");

    let admin_url = env::var("ADMIN_SERVER_URL").unwrap_or_else(|_| "127.0.0.1:7801".to_owned());
    let admin_address: SocketAddr = admin_url.parse()?;
    if !admin_address.ip().is_loopback() {
        return Err(anyhow!("ADMIN_SERVER_URL must bind to a loopback address"));
    }
    let admin_listener = tokio::net::TcpListener::bind(&admin_url).await?;
    info!("Private admin server listening on {}", admin_url);
    tokio::try_join!(
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>()
        ),
        axum::serve(
            admin_listener,
            admin_app.into_make_service_with_connect_info::<SocketAddr>()
        )
    )?;
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

fn ensure_loopback_url(value: &str, name: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)?;
    let loopback = url.host().is_some_and(|host| match host {
        url::Host::Domain(domain) => domain.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(address) => address.is_loopback(),
        url::Host::Ipv6(address) => address.is_loopback(),
    });
    if !loopback {
        return Err(anyhow!("{name} must target localhost"));
    }
    Ok(())
}

pub fn create_router(state: AppState) -> Router {
    let ingress = state.ingress.clone();
    let ingress_state = IngressState::new(ingress.clone());
    Router::new()
        .route("/health", get(health_check))
        .route("/auth/challenge", post(auth_challenge))
        .route("/auth/login", post(auth_login))
        .route("/auth/logout", post(auth_logout))
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
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(ingress.request_body_bytes))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            ingress.request_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            ingress_state,
            crate::ingress::enforce,
        ))
        .layer(cors_layer(&ingress))
        .with_state(state)
}

pub fn create_admin_router(state: AppState) -> Router {
    let ingress = state.ingress.clone();
    Router::new()
        .route("/health", get(health_check))
        .route("/internal/fees/batch", post(apply_automatic_fee_batch))
        .route("/admin/fees/override", post(apply_manual_fee_batch))
        .route("/admin/fees/clear", post(clear_manual_fee_batch))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(ingress.request_body_bytes))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            ingress.request_timeout,
        ))
        .with_state(state)
}

fn cors_layer(config: &IngressConfig) -> CorsLayer {
    let origins: Vec<HeaderValue> = config
        .allowed_origins
        .iter()
        .filter_map(|origin| origin.parse().ok())
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
    let storage = match state
        .work_limits
        .rpc(fetch_account_storage_from_rpc(state.store.vault_id()))
        .await
    {
        Ok(storage) => storage,
        Err(error) => return service_unavailable_retry(format!("vault unavailable: {error}")),
    };
    let commitment = match vault_user_registration(&storage, user_id) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return not_found("user has no registered trading key"),
        Err(error) => return service_unavailable(error.to_string()),
    };
    let now = Utc::now().timestamp() as u64;
    let auth_store = state.auth_store.clone();
    let challenge = match state
        .work_limits
        .database(move || Ok(auth_store.issue_challenge(user_id, commitment, now)?))
        .await
    {
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
    let storage = match state
        .work_limits
        .rpc(fetch_account_storage_from_rpc(state.store.vault_id()))
        .await
    {
        Ok(storage) => storage,
        Err(error) => return service_unavailable_retry(format!("vault unavailable: {error}")),
    };
    let commitment = match vault_user_registration(&storage, user_id) {
        Ok(Some(commitment)) => commitment,
        Ok(None) => return not_found("user has no registered trading key"),
        Err(error) => return service_unavailable(error.to_string()),
    };
    let now = Utc::now().timestamp() as u64;
    let auth_store = state.auth_store.clone();
    let challenge_id = request.challenge_id;
    match state
        .work_limits
        .database(move || {
            Ok(auth_store.authenticate(
                &challenge_id,
                user_id,
                commitment,
                pubkey,
                signature,
                now,
            )?)
        })
        .await
    {
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

async fn auth_logout(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    let token = match crate::auth::parse_bearer(&headers) {
        Ok(token) => token.into_inner(),
        Err(_) => {
            return error_response(StatusCode::UNAUTHORIZED, "missing or invalid bearer token");
        }
    };
    let store = state.auth_store.clone();
    let revoke_token = token.clone();
    let now = Utc::now().timestamp() as u64;
    match state
        .work_limits
        .database(move || Ok(store.revoke_session(&revoke_token, now)?))
        .await
    {
        Ok(true) => {
            state.connection_manager.disconnect_session(&token);
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(false) => error_response(StatusCode::UNAUTHORIZED, "expired or revoked session"),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn authenticated_user(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AccountId, Response<Body>> {
    let token = crate::auth::parse_bearer(headers)
        .map_err(|_| error_response(StatusCode::UNAUTHORIZED, "missing or invalid bearer token"))?;
    let now = Utc::now().timestamp() as u64;
    let store = state.auth_store.clone();
    let token = token.into_inner();
    state
        .work_limits
        .database(move || Ok(store.lookup_session(&token, now)?))
        .await
        .map_err(|error| service_unavailable_retry(error.to_string()))?
        .map(|session| session.user_id)
        .ok_or_else(|| error_response(StatusCode::UNAUTHORIZED, "expired or revoked session"))
}

#[derive(Debug, Deserialize)]
struct AnalyticsQuery {
    offset: Option<u64>,
    limit: Option<u32>,
}

async fn user_analytics(State(state): State<AppState>, headers: HeaderMap) -> Response<Body> {
    let user = match authenticated_user(&state, &headers).await {
        Ok(user) => user,
        Err(response) => return response,
    };
    let now = Utc::now().timestamp_millis() as u64;
    let analytics = state.analytics_store.clone();
    let history = state.history.clone();
    let user = user.to_hex();
    match state
        .work_limits
        .database(move || {
            Ok((
                analytics.user_summary(&user, "oracle_usd", now)?,
                history.user_order_stats(&user)?,
            ))
        })
        .await
    {
        Ok((summary, order_stats)) => Json(ApiResponse {
            data: serde_json::json!({"pnl": summary, "orders": order_stats}),
        })
        .into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn user_positions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AnalyticsQuery>,
) -> Response<Body> {
    let user = match authenticated_user(&state, &headers).await {
        Ok(user) => user,
        Err(response) => return response,
    };
    let store = state.analytics_store.clone();
    let user = user.to_hex();
    let pagination = Pagination {
        offset: query.offset.unwrap_or(0),
        limit: query.limit.unwrap_or(50).clamp(1, 500),
    };
    match state
        .work_limits
        .database(move || {
            Ok(store.positions(
                &user,
                "oracle_usd",
                Utc::now().timestamp_millis() as u64,
                pagination,
            )?)
        })
        .await
    {
        Ok(positions) => Json(ApiResponse { data: positions }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn user_analytics_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<AnalyticsQuery>,
) -> Response<Body> {
    let user = match authenticated_user(&state, &headers).await {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    let store = state.analytics_store.clone();
    let pagination = Pagination {
        offset: query.offset.unwrap_or(0),
        limit: query.limit.unwrap_or(50).clamp(1, 500),
    };
    match state
        .work_limits
        .database(move || Ok(store.events_for_subject(&user, pagination)?))
        .await
    {
        Ok(events) => Json(ApiResponse { data: events }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn pool_analytics_all(State(state): State<AppState>) -> Response<Body> {
    let now = Utc::now().timestamp_millis() as u64;
    let ids = state
        .store
        .assets()
        .iter()
        .map(|asset| asset.faucet_id.to_hex())
        .collect::<Vec<_>>();
    let store = state.analytics_store.clone();
    match state
        .work_limits
        .database(move || {
            let mut values = Vec::new();
            for id in ids {
                if let Some(value) = store.pool_summary(&id, now)? {
                    values.push(value);
                }
            }
            Ok(values)
        })
        .await
    {
        Ok(analytics) => Json(ApiResponse { data: analytics }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn pool_analytics(
    State(state): State<AppState>,
    Path(faucet_id): Path<String>,
) -> Response<Body> {
    let store = state.analytics_store.clone();
    match state
        .work_limits
        .database(move || Ok(store.pool_summary(&faucet_id, Utc::now().timestamp_millis() as u64)?))
        .await
    {
        Ok(Some(summary)) => Json(ApiResponse { data: summary }).into_response(),
        Ok(None) => not_found("pool analytics unavailable"),
        Err(error) => service_unavailable_retry(error.to_string()),
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
    let authenticated = match authenticated_user(&state, &headers).await {
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
    let authenticated = match authenticated_user(&state, &headers).await {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    let store = state.lp_service.store();
    match state
        .work_limits
        .database(move || Ok(store.operation(&note_id)?))
        .await
    {
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
    let authenticated = match authenticated_user(&state, &headers).await {
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
    let service = state.lp_service.clone();
    match state
        .work_limits
        .database(move || Ok(service.position(lp_id, faucet_id)?))
        .await
    {
        Ok(Some(position)) => Json(ApiResponse { data: position }).into_response(),
        Ok(None) => not_found("LP position not found"),
        Err(error) => service_unavailable(format!("LP journal unavailable: {error}")),
    }
}

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let broker = state.message_broker.metrics();
    let response = serde_json::json!({
        "status": "healthy",
        "timestamp": Utc::now(),
        "broker": {
            "lagged_messages": broker.lagged_messages,
            "dropped_without_receivers": broker.dropped_without_receivers,
        }
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
    if !state.public_mint_enabled {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "success": false,
                "message": "public mint is disabled"
            })),
        )
            .into_response();
    }
    let endpoint = format!("{}/mint", state.faucet_server_url);
    match state
        .faucet_http
        .post(endpoint)
        .bearer_auth(state.faucet_credentials.primary())
        .json(&request)
        .send()
        .await
    {
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

async fn stats(State(state): State<AppState>) -> Response<Body> {
    let stats = state.store.order_stats();
    let history = state.history.clone();
    let trading = match state.work_limits.database(move || history.stats()).await {
        Ok(value) => value,
        Err(error) => return service_unavailable_retry(error.to_string()),
    };
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
    (headers, Json(response)).into_response()
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
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || {
            Ok(history.candles(
                &query.source,
                Some(&query.pair),
                interval,
                query.from,
                query.to,
                query.limit.unwrap_or(500).clamp(1, 5_000),
            )?)
        })
        .await
    {
        Ok(candles) => Json(ApiResponse { data: candles }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
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
        let authenticated = match authenticated_user(&state, &headers).await {
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
    let pair = pair.map(ToOwned::to_owned);
    let user_id = user_id.map(ToOwned::to_owned);
    let private = user_id.is_some();
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || {
            Ok(history.trades(
                pair.as_deref(),
                user_id.as_deref(),
                query.before,
                query.limit.unwrap_or(100).clamp(1, 1_000),
            )?)
        })
        .await
    {
        Ok(trades) if private => Json(ApiResponse { data: trades }).into_response(),
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
        Err(error) => service_unavailable_retry(error.to_string()),
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
    let user_id = match authenticated_user(&state, &headers).await {
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
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || {
            Ok(history.orders(
                Some(&user_id),
                query.status.as_deref(),
                query.before,
                query.limit.unwrap_or(100).clamp(1, 1_000),
            )?)
        })
        .await
    {
        Ok(orders) => Json(ApiResponse { data: orders }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn order_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response<Body> {
    let user_id = match authenticated_user(&state, &headers).await {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || Ok(history.order(id)?))
        .await
    {
        Ok(Some(order)) if order.user_id == user_id => {
            Json(ApiResponse { data: order }).into_response()
        }
        Ok(Some(_)) => error_response(StatusCode::FORBIDDEN, "order belongs to another user"),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
    }
}

async fn order_events(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> Response<Body> {
    let user_id = match authenticated_user(&state, &headers).await {
        Ok(user) => user.to_hex(),
        Err(response) => return response,
    };
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || Ok(history.order(id)?))
        .await
    {
        Ok(Some(order)) if order.user_id == user_id => {}
        Ok(Some(_)) => {
            return error_response(StatusCode::FORBIDDEN, "order belongs to another user");
        }
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(error) => return ApiError(error).into_response(),
    }
    let history = state.history.clone();
    match state
        .work_limits
        .database(move || Ok(history.order_events(id)?))
        .await
    {
        Ok(events) => Json(ApiResponse { data: events }).into_response(),
        Err(error) => service_unavailable_retry(error.to_string()),
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
    if !state.fee_updater_credentials.authorizes(&headers) {
        return (StatusCode::UNAUTHORIZED, "invalid fee updater token").into_response();
    }
    request.source = FeeUpdateSource::Automatic;
    apply_fee_batch(state, request).await
}

async fn apply_manual_fee_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut request): Json<FeeBatchRequest>,
) -> Response<Body> {
    if !state.fee_admin_credentials.authorizes(&headers) {
        return (StatusCode::UNAUTHORIZED, "invalid fee admin token").into_response();
    }
    request.source = FeeUpdateSource::Manual;
    apply_fee_batch(state, request).await
}

async fn clear_manual_fee_batch(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response<Body> {
    if !state.fee_admin_credentials.authorizes(&headers) {
        return (StatusCode::UNAUTHORIZED, "invalid fee admin token").into_response();
    }
    let now = Utc::now().timestamp() as u64;
    let store = state.fee_store.clone();
    match state
        .work_limits
        .database(move || {
            let cleared = store.clear_manual(now)?;
            let states = store.active_states(now)?;
            Ok((cleared, states))
        })
        .await
    {
        Ok((cleared, fee_states)) => {
            let _ = state.message_broker.broadcast_fee_state(FeeStateEvent {
                fee_states,
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

async fn apply_fee_batch(state: AppState, request: FeeBatchRequest) -> Response<Body> {
    let now = Utc::now().timestamp() as u64;
    let assets: Vec<_> = state
        .store
        .assets()
        .iter()
        .map(|asset| asset.faucet_id)
        .collect();
    let store = state.fee_store.clone();
    let (applied, fee_states) = match state
        .work_limits
        .database(move || {
            let applied = store.apply_batch(&request, &assets, now)?;
            let states = store.active_states(now)?;
            Ok((applied, states))
        })
        .await
    {
        Ok(result) => result,
        Err(error) => return service_unavailable_retry(error.to_string()),
    };
    let _ = state.message_broker.broadcast_fee_state(FeeStateEvent {
        fee_states,
        timestamp: Utc::now().timestamp_millis() as u64,
    });
    Json(ApiResponse { data: applied }).into_response()
}

async fn user_placement(State(state): State<AppState>, Path(id): Path<String>) -> Response<Body> {
    let user_id = match AccountId::from_hex(&id) {
        Ok(user_id) => user_id,
        Err(error) => return bad_request(format!("invalid user account id: {error}")),
    };
    let vault_storage = match state
        .work_limits
        .rpc(fetch_account_storage_from_rpc(state.store.vault_id()))
        .await
    {
        Ok(storage) => storage,
        Err(error) => {
            warn!(%error, "failed to fetch vault storage for placement");
            return service_unavailable_retry("vault storage is unavailable");
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
    let pool_storage = match state
        .work_limits
        .rpc(fetch_account_storage_from_rpc(pool_id))
        .await
    {
        Ok(storage) => storage,
        Err(error) => {
            warn!(%error, "failed to fetch pool storage for placement");
            return service_unavailable_retry("assigned pool shard is unavailable");
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

fn service_unavailable_retry(message: impl Into<String>) -> Response<Body> {
    let mut response = error_response(StatusCode::SERVICE_UNAVAILABLE, message);
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
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
    if payload.version != INTENT_VERSION {
        return Ok(error_response(
            StatusCode::BAD_REQUEST,
            "only signed intent version 2 is accepted",
        ));
    }
    if payload.order_type != OrderType::Spot {
        return Ok(bad_request("v2 currently supports only spot swap intents"));
    }
    if env::var("AUTH_DOMAIN").unwrap_or_else(|_| "minizeke".to_owned()) != "minizeke"
        || env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_owned()) != "testnet"
    {
        return Ok(service_unavailable(
            "v2 pool intents require AUTH_DOMAIN=minizeke and MIDEN_NETWORK=testnet",
        ));
    }
    let now_secs = Utc::now().timestamp() as u64;
    if is_expired_at(payload.expires_at, now_secs) {
        return Ok(bad_request("signed intent has expired"));
    }
    let created_at_ms = Utc::now().timestamp_millis() as u64;
    let authenticated = match authenticated_user(&state, &headers).await {
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

    let sig_bytes = match general_purpose::STANDARD.decode(payload.signed_intent) {
        Ok(value) => value,
        Err(error) => return Ok(bad_request(format!("invalid signature encoding: {error}"))),
    };
    let signature = match Signature::read_from_bytes(&sig_bytes) {
        Ok(value) => value,
        Err(error) => return Ok(bad_request(format!("invalid signature: {error}"))),
    };
    let pubkey_bytes = match general_purpose::STANDARD.decode(payload.pubkey) {
        Ok(value) => value,
        Err(error) => return Ok(bad_request(format!("invalid public key encoding: {error}"))),
    };
    let pubkey = match PublicKey::read_from_bytes(&pubkey_bytes) {
        Ok(value) => value,
        Err(error) => return Ok(bad_request(format!("invalid public key: {error}"))),
    };

    let vault_storage = match state
        .work_limits
        .rpc(fetch_account_storage_from_rpc(state.store.vault_id()))
        .await
    {
        Ok(storage) => storage,
        Err(error) => return Ok(service_unavailable_retry(error.to_string())),
    };
    let registered = vault_user_registration(&vault_storage, payload.user_id)
        .map_err(ApiError)?
        .ok_or_else(|| ApiError(anyhow!("user has no registered trading key")))?;
    if Word::from(pubkey.to_commitment()) != registered {
        return Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "public key is not registered for this user",
        ));
    }
    let intent = Intent::new_swap(
        payload.user_id,
        payload.details.asset_in,
        payload.details.amount_in,
        payload.details.asset_out,
        payload.details.min_amount_out,
        payload.client_order_id,
        payload.expires_at,
    );
    debug_assert_eq!(intent.domain, INTENT_DOMAIN_TAG);
    debug_assert_eq!(intent.network, TESTNET_NETWORK_TAG);
    if !pubkey.verify(intent.message_word(), signature.clone()) {
        return Ok(error_response(
            StatusCode::UNAUTHORIZED,
            "invalid signed intent",
        ));
    }

    let intent_commitment = intent
        .message_word()
        .as_elements()
        .iter()
        .map(|felt| format!("{:016x}", felt.as_canonical_u64()))
        .collect::<String>();
    let candidate = Order::new_with_id(
        Uuid::new_v4(),
        signature.clone(),
        payload.user_id,
        payload.details,
        pubkey.clone(),
        intent,
    );
    let execution_store = state.execution_store.clone();
    let candidate_for_db = candidate.clone();
    let client_order_id = payload.client_order_id;
    let reservation = match state
        .work_limits
        .database(move || {
            Ok(execution_store.admit_order(
                client_order_id,
                &intent_commitment,
                &candidate_for_db,
                created_at_ms,
            )?)
        })
        .await
    {
        Ok(reservation) => reservation,
        Err(error) => return Ok(service_unavailable_retry(error.to_string())),
    };
    let (lifecycle_id, should_publish) = match reservation {
        IntentReservation::New { order_id } => (order_id, true),
        IntentReservation::Existing { order_id } => (order_id, false),
        IntentReservation::Conflict => {
            return Ok(error_response(
                StatusCode::CONFLICT,
                "client_order_id is already reserved for a different signed intent",
            ));
        }
    };
    let order = if lifecycle_id == candidate.id {
        candidate
    } else {
        Order::new_with_id(
            lifecycle_id,
            signature,
            payload.user_id,
            payload.details,
            pubkey,
            intent,
        )
    };
    if should_publish {
        state
            .message_broker
            .broadcast_order_update(order.clone().into())
            .map_err(ApiError)?;
    }

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
    use super::{NewOrderRequest, ensure_loopback_url, normalize_http_url};

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

    #[test]
    fn faucet_service_must_remain_on_loopback() {
        assert!(ensure_loopback_url("http://127.0.0.1:7800", "test").is_ok());
        assert!(ensure_loopback_url("http://[::1]:7800", "test").is_ok());
        assert!(ensure_loopback_url("https://faucet.example", "test").is_err());
    }

    #[test]
    fn legacy_v1_order_payload_is_rejected() {
        let legacy = serde_json::json!({
            "id": "00112233-4455-6677-8899-aabbccddeeff",
            "details": {
                "asset_in": "0x57a179f33b726c315fcfd5e0ff3309",
                "amount_in": 10,
                "asset_out": "0x1e7e8af77fc5f2f1631d5c5ce35471",
                "min_amount_out": 9
            },
            "order_type": "Spot",
            "user_id": "0x5a17d92af11620613414ead24f1fce",
            "signed_intent": "legacy",
            "pubkey": "legacy"
        });
        assert!(serde_json::from_value::<NewOrderRequest>(legacy).is_err());
    }
}
