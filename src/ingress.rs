//! Shared bounded-ingress policy for HTTP, WebSocket, database and Miden RPC work.

use std::{
    collections::hash_map::DefaultHasher,
    env,
    future::Future,
    hash::{Hash, Hasher},
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use axum::{
    body::Body,
    extract::{ConnectInfo, State},
    http::{HeaderMap, Request, Response, StatusCode, header},
    middleware::Next,
};
use dashmap::DashMap;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Debug)]
pub struct IngressConfig {
    pub allowed_origins: Arc<[String]>,
    pub request_body_bytes: usize,
    pub request_timeout: Duration,
    pub db_concurrency: usize,
    pub rpc_concurrency: usize,
    pub ws_queue_capacity: usize,
    pub ws_global_cap: usize,
    pub ws_per_ip_cap: usize,
    pub ws_message_bytes: usize,
    pub ws_max_subscriptions: usize,
    pub ws_session_recheck: Duration,
    pub ws_ping_interval: Duration,
    pub ws_pong_timeout: Duration,
    pub ws_write_timeout: Duration,
    pub trust_proxy: bool,
    pub proxy_tls_terminated: bool,
    public_rate: u32,
    auth_rate: u32,
    mutation_rate: u32,
    max_rate_keys: usize,
    window: Duration,
}

impl IngressConfig {
    pub fn from_env() -> Self {
        Self {
            allowed_origins: csv(
                "CORS_ALLOWED_ORIGINS",
                "http://localhost:3000,http://localhost:5173",
            )
            .into(),
            request_body_bytes: number("HTTP_MAX_BODY_BYTES", 1_048_576),
            request_timeout: Duration::from_secs(number("HTTP_REQUEST_TIMEOUT_SECS", 15)),
            db_concurrency: number("DB_MAX_CONCURRENCY", 8).max(1),
            rpc_concurrency: number("MIDEN_RPC_MAX_CONCURRENCY", 8).max(1),
            ws_queue_capacity: number("WS_QUEUE_CAPACITY", 128).max(1),
            ws_global_cap: number("WS_GLOBAL_CONNECTION_CAP", 2_000).max(1),
            ws_per_ip_cap: number("WS_PER_IP_CONNECTION_CAP", 256).max(1),
            ws_message_bytes: number("WS_MAX_MESSAGE_BYTES", 65_536).max(1),
            ws_max_subscriptions: number("WS_MAX_SUBSCRIPTIONS", 64).max(1),
            ws_session_recheck: Duration::from_secs(number("WS_SESSION_RECHECK_SECS", 30)),
            ws_ping_interval: Duration::from_secs(number("WS_PING_INTERVAL_SECS", 20)),
            ws_pong_timeout: Duration::from_secs(number("WS_PONG_TIMEOUT_SECS", 60)),
            ws_write_timeout: Duration::from_secs(number("WS_WRITE_TIMEOUT_SECS", 10)),
            trust_proxy: flag("TRUST_PROXY_HEADERS", false),
            proxy_tls_terminated: flag("TRUST_PROXY_TLS_TERMINATED", false),
            public_rate: number("RATE_LIMIT_PUBLIC_PER_MINUTE", 240),
            auth_rate: number("RATE_LIMIT_AUTH_PER_MINUTE", 20),
            mutation_rate: number("RATE_LIMIT_MUTATION_PER_MINUTE", 60),
            max_rate_keys: number("RATE_LIMIT_MAX_KEYS", 100_000).max(1),
            window: Duration::from_secs(60),
        }
    }

    pub fn origin_allowed(&self, headers: &HeaderMap) -> bool {
        let Some(origin) = headers.get(header::ORIGIN) else {
            return true;
        };
        origin
            .to_str()
            .ok()
            .is_some_and(|origin| self.allowed_origins.iter().any(|allowed| allowed == origin))
    }

    pub fn client_ip(&self, headers: &HeaderMap, peer: Option<SocketAddr>) -> Option<IpAddr> {
        if self.trust_proxy && self.proxy_tls_terminated {
            if let Some(ip) = headers
                .get("x-forwarded-for")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.split(',').next())
                .and_then(|value| value.trim().parse().ok())
            {
                return Some(ip);
            }
        }
        peer.map(|peer| peer.ip())
    }
}

fn number<T>(name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn flag(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn csv(name: &str, default: &str) -> Vec<String> {
    env::var(name)
        .unwrap_or_else(|_| default.to_owned())
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Clone)]
pub struct WorkLimits {
    db: Arc<Semaphore>,
    rpc: Arc<Semaphore>,
}

impl WorkLimits {
    pub fn new(config: &IngressConfig) -> Self {
        Self {
            db: Arc::new(Semaphore::new(config.db_concurrency)),
            rpc: Arc::new(Semaphore::new(config.rpc_concurrency)),
        }
    }

    pub async fn database<F, T>(&self, work: F) -> Result<T>
    where
        F: FnOnce() -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let permit = acquire(self.db.clone()).await?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            work()
        })
        .await
        .map_err(|error| anyhow!("database worker failed: {error}"))?
    }

    pub async fn rpc<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let _permit = acquire(self.rpc.clone()).await?;
        future.await
    }
}

async fn acquire(semaphore: Arc<Semaphore>) -> Result<OwnedSemaphorePermit> {
    semaphore
        .try_acquire_owned()
        .map_err(|_| anyhow!("bounded worker pool is saturated"))
}

#[derive(Clone)]
pub struct IngressState {
    config: IngressConfig,
    buckets: Arc<DashMap<String, Bucket>>,
}

#[derive(Clone, Copy)]
struct Bucket {
    started: Instant,
    used: u32,
}

impl IngressState {
    pub fn new(config: IngressConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(DashMap::new()),
        }
    }

    fn allow(&self, key: String, limit: u32) -> Option<u64> {
        let now = Instant::now();
        if !self.buckets.contains_key(&key) && self.buckets.len() >= self.config.max_rate_keys {
            self.buckets
                .retain(|_, bucket| now.duration_since(bucket.started) < self.config.window);
            if self.buckets.len() >= self.config.max_rate_keys {
                return Some(1);
            }
        }
        let mut bucket = self.buckets.entry(key).or_insert(Bucket {
            started: now,
            used: 0,
        });
        if now.duration_since(bucket.started) >= self.config.window {
            *bucket = Bucket {
                started: now,
                used: 0,
            };
        }
        if bucket.used >= limit {
            return Some(
                self.config
                    .window
                    .saturating_sub(now.duration_since(bucket.started))
                    .as_secs()
                    .max(1),
            );
        }
        bucket.used += 1;
        None
    }
}

pub async fn enforce(
    State(state): State<IngressState>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    if !state.config.origin_allowed(request.headers()) {
        return response(StatusCode::FORBIDDEN, "origin is not allowed", None);
    }

    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|value| value.0);
    let ip = state
        .config
        .client_ip(request.headers(), peer)
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let path = request.uri().path();
    let (class, limit) = if path.starts_with("/auth/") {
        ("auth", state.config.auth_rate)
    } else if request.method() != axum::http::Method::GET {
        ("mutation", state.config.mutation_rate)
    } else {
        ("public", state.config.public_rate)
    };
    let identity = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(token_fingerprint)
        .unwrap_or_default();
    let key = format!("{class}:{ip}:{identity}");
    if let Some(retry_after) = state.allow(key, limit) {
        return response(
            StatusCode::TOO_MANY_REQUESTS,
            "rate limit exceeded",
            Some(retry_after),
        );
    }
    next.run(request).await
}

pub fn unavailable(message: &str) -> Response<Body> {
    response(StatusCode::SERVICE_UNAVAILABLE, message, Some(1))
}

fn response(status: StatusCode, message: &str, retry_after: Option<u64>) -> Response<Body> {
    let mut response = Response::new(Body::from(message.to_owned()));
    *response.status_mut() = status;
    if let Some(seconds) = retry_after {
        if let Ok(value) = seconds.to_string().parse() {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
    }
    response
}

fn token_fingerprint(token: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    token.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwarded_ip_requires_explicit_tls_proxy_trust() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "203.0.113.9, 10.0.0.1".parse().unwrap());
        let peer = Some("127.0.0.1:1234".parse().unwrap());
        let mut config = IngressConfig::from_env();
        config.trust_proxy = true;
        config.proxy_tls_terminated = false;
        assert_eq!(
            config.client_ip(&headers, peer).unwrap().to_string(),
            "127.0.0.1"
        );
        config.proxy_tls_terminated = true;
        assert_eq!(
            config.client_ip(&headers, peer).unwrap().to_string(),
            "203.0.113.9"
        );
    }

    #[test]
    fn origin_allowlist_is_exact() {
        let mut config = IngressConfig::from_env();
        config.allowed_origins = vec!["https://app.example".to_owned()].into();
        let mut headers = HeaderMap::new();
        headers.insert(header::ORIGIN, "https://app.example.evil".parse().unwrap());
        assert!(!config.origin_allowed(&headers));
        headers.insert(header::ORIGIN, "https://app.example".parse().unwrap());
        assert!(config.origin_allowed(&headers));
    }

    #[test]
    fn route_bucket_is_bounded_and_reports_retry() {
        let mut config = IngressConfig::from_env();
        config.max_rate_keys = 1;
        let state = IngressState::new(config);
        assert_eq!(state.allow("auth:ip:user".to_owned(), 1), None);
        assert!(state.allow("auth:ip:user".to_owned(), 1).is_some());
        assert!(state.allow("auth:other:user".to_owned(), 1).is_some());
        assert_eq!(state.buckets.len(), 1);
    }

    #[tokio::test]
    async fn worker_gate_rejects_saturation_without_waiters() {
        let mut config = IngressConfig::from_env();
        config.db_concurrency = 1;
        config.rpc_concurrency = 1;
        let limits = WorkLimits::new(&config);
        let permit = limits.db.clone().try_acquire_owned().unwrap();
        let result = limits.database(|| Ok::<_, anyhow::Error>(())).await;
        assert!(result.is_err());
        drop(permit);
        assert!(limits.database(|| Ok::<_, anyhow::Error>(())).await.is_ok());

        let permit = limits.rpc.clone().try_acquire_owned().unwrap();
        let result = limits.rpc(async { Ok::<_, anyhow::Error>(()) }).await;
        assert!(result.is_err());
        drop(permit);
        assert!(
            limits
                .rpc(async { Ok::<_, anyhow::Error>(()) })
                .await
                .is_ok()
        );
    }
}
