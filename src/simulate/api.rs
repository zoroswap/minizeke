use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use miden_client::{
    Serializable,
    account::AccountId,
    auth::{AuthSecretKey, PublicKey},
};
use miden_core::{Felt, Word};
use reqwest::{Client, StatusCode, header};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::warn;
use uuid::Uuid;

use crate::{faucet::MintRequest, intent::INTENT_VERSION, order::OrderDetails};

/// Caps concurrent challenge+login pairs so we do not stampede the server's
/// `MIDEN_RPC_MAX_CONCURRENCY` vault lookups (default 8) or auth rate bucket.
const AUTH_CONCURRENCY: usize = 4;
/// Let the faucet receive enough requests together to build multi-recipient transactions.
/// The faucet worker itself serializes prove/submit and caps each transaction's batch size.
const FAUCET_MINT_CONCURRENCY: usize = 64;
/// Per-request wait for a slow faucet prove/submit under staging load.
const FAUCET_MINT_HTTP_TIMEOUT: Duration = Duration::from_secs(300);
/// Cap how long one (recipient, faucet) mint may retry before failing the onboard.
const FAUCET_MINT_RETRY_BUDGET: Duration = Duration::from_secs(900);

#[derive(Clone)]
pub struct SimulationApi {
    client: Client,
    api_url: String,
    faucet_url: String,
    faucet_token: Option<String>,
    auth_slots: Arc<Semaphore>,
    mint_slots: Arc<Semaphore>,
}

impl SimulationApi {
    pub fn new(
        api_url: impl Into<String>,
        faucet_url: impl Into<String>,
        faucet_token: Option<String>,
    ) -> Result<Self> {
        // Default timeout for auth/orders; mint overrides per-request.
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .context("build simulation HTTP client")?;
        Ok(Self {
            client,
            api_url: normalize_url(api_url.into()),
            faucet_url: normalize_url(faucet_url.into()),
            faucet_token,
            auth_slots: Arc::new(Semaphore::new(AUTH_CONCURRENCY)),
            mint_slots: Arc::new(Semaphore::new(FAUCET_MINT_CONCURRENCY)),
        })
    }

    /// Mint once. Retries faucet cooldown and transient transport/timeouts until success
    /// or [`FAUCET_MINT_RETRY_BUDGET`] elapses.
    pub async fn mint(&self, user_id: AccountId, faucet_id: AccountId) -> Result<Duration> {
        let started = Instant::now();
        let deadline = Instant::now() + FAUCET_MINT_RETRY_BUDGET;
        let mut attempt = 0_u32;
        loop {
            attempt = attempt.saturating_add(1);
            let _slot = self
                .mint_slots
                .acquire()
                .await
                .map_err(|_| anyhow!("faucet mint concurrency closed"))?;
            match self.mint_once(user_id, faucet_id).await {
                Ok(()) => return Ok(started.elapsed()),
                Err(error) => {
                    drop(_slot);
                    let message = format!("{error:#}");
                    if let Some(wait_secs) = parse_cooldown_secs(&message) {
                        warn!(
                            recipient = %user_id.to_hex(),
                            faucet = %faucet_id.to_hex(),
                            wait_secs,
                            attempt,
                            "faucet mint cooldown; waiting"
                        );
                        tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                        continue;
                    }
                    if is_transient_mint_error(&message) && Instant::now() < deadline {
                        let wait_secs = 2_u64.saturating_pow(attempt.min(5)).min(30);
                        warn!(
                            recipient = %user_id.to_hex(),
                            faucet = %faucet_id.to_hex(),
                            attempt,
                            wait_secs,
                            %error,
                            "faucet mint transient failure; retrying"
                        );
                        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    return Err(error).with_context(|| {
                        format!(
                            "faucet mint failed for recipient {} faucet {} after {attempt} attempt(s)",
                            user_id.to_hex(),
                            faucet_id.to_hex()
                        )
                    });
                }
            }
        }
    }

    async fn mint_once(&self, user_id: AccountId, faucet_id: AccountId) -> Result<()> {
        let token = self
            .faucet_token
            .as_deref()
            .ok_or_else(|| anyhow!("faucet token is unavailable"))?;
        let response = self
            .client
            .post(format!("{}/mint", self.faucet_url))
            .timeout(FAUCET_MINT_HTTP_TIMEOUT)
            .bearer_auth(token)
            .json(&MintRequest {
                address: user_id.to_hex(),
                faucet_id: faucet_id.to_hex(),
            })
            .send()
            .await
            .with_context(|| {
                format!(
                    "send faucet mint request (recipient={} faucet={})",
                    user_id.to_hex(),
                    faucet_id.to_hex()
                )
            })?;
        ensure_success(response, "faucet mint").await
    }

    pub async fn authenticate(
        &self,
        user_id: AccountId,
        key: &AuthSecretKey,
    ) -> Result<(Session, AuthTiming)> {
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut http = Duration::ZERO;
        let mut wait_total = Duration::ZERO;
        loop {
            // Hold the slot only around the HTTP round-trip, not during Retry-After sleeps.
            let slot = self
                .auth_slots
                .acquire()
                .await
                .map_err(|_| anyhow!("auth concurrency closed"))?;
            let attempt_started = Instant::now();
            let result = self.authenticate_once(user_id, key).await;
            drop(slot);
            match result {
                Ok(session) => {
                    http += attempt_started.elapsed();
                    return Ok((
                        session,
                        AuthTiming {
                            http,
                            wait: wait_total,
                        },
                    ));
                }
                Err(error) => {
                    let Some(wait) = retry_after_from_error(&error) else {
                        return Err(error);
                    };
                    if Instant::now() + wait > deadline {
                        return Err(error).context("auth retries exhausted");
                    }
                    warn!(
                        user = %user_id.to_hex(),
                        wait_secs = wait.as_secs().max(1),
                        %error,
                        "auth saturated; retrying"
                    );
                    wait_total += wait;
                    tokio::time::sleep(wait).await;
                }
            }
        }
    }

    async fn authenticate_once(&self, user_id: AccountId, key: &AuthSecretKey) -> Result<Session> {
        let challenge_response = self
            .client
            .post(format!("{}/auth/challenge", self.api_url))
            .json(&serde_json::json!({ "user_id": user_id.to_hex() }))
            .send()
            .await
            .context("request auth challenge")?;
        let challenge: ApiResponse<AuthChallenge> =
            decode_success(challenge_response, "auth challenge").await?;
        let [a, b, c, d] = challenge.data.message;
        let message = Word::new([
            Felt::new(a).context("invalid auth challenge felt")?,
            Felt::new(b).context("invalid auth challenge felt")?,
            Felt::new(c).context("invalid auth challenge felt")?,
            Felt::new(d).context("invalid auth challenge felt")?,
        ]);
        let signature = key.sign(message);
        let pubkey = key.public_key();
        let login_response = self
            .client
            .post(format!("{}/auth/login", self.api_url))
            .json(&serde_json::json!({
                "challenge_id": challenge.data.challenge_id,
                "user_id": user_id.to_hex(),
                "pubkey": general_purpose::STANDARD.encode(pubkey.to_bytes()),
                "signature": general_purpose::STANDARD.encode(signature.to_bytes()),
            }))
            .send()
            .await
            .context("submit auth login")?;
        let login: ApiResponse<AuthLogin> = decode_success(login_response, "auth login").await?;
        Ok(Session {
            access_token: login.data.access_token,
            expires_at: login.data.expires_at,
        })
    }

    pub async fn reserve_order_nonce(&self, session: &Session) -> Result<(u32, Uuid)> {
        let response = self
            .client
            .post(format!("{}/orders/nonce", self.api_url))
            .bearer_auth(&session.access_token)
            .send()
            .await
            .context("request order nonce lease")?;
        let lease: ApiResponse<NonceLeaseResponse> =
            decode_success(response, "order nonce lease").await?;
        Ok((lease.data.nonce, lease.data.client_order_id))
    }

    pub async fn submit_order(
        &self,
        session: &Session,
        user_id: AccountId,
        pubkey: PublicKey,
        signature: miden_client::auth::Signature,
        client_order_id: Uuid,
        expires_at: u64,
        details: OrderDetails,
    ) -> Result<OrderResponse> {
        let started = Instant::now();
        let response = self
            .client
            .post(format!("{}/orders/new", self.api_url))
            .bearer_auth(&session.access_token)
            .json(&NewOrderRequest {
                version: INTENT_VERSION,
                client_order_id,
                expires_at,
                details,
                order_type: "Spot",
                user_id: user_id.to_hex(),
                signed_intent: general_purpose::STANDARD.encode(signature.to_bytes()),
                pubkey: general_purpose::STANDARD.encode(pubkey.to_bytes()),
            })
            .send()
            .await
            .context("submit order request")?;
        let latency = started.elapsed();
        let status = response.status();
        let retry_after = retry_after_header(&response);
        let body = response.text().await.context("read order response")?;
        let (outcome, order_id) = if status.is_success() {
            let order_id = parse_admitted_order_id(&body);
            (OrderOutcome::Accepted, order_id)
        } else if status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::SERVICE_UNAVAILABLE
        {
            // Ingress rate limits and execution-queue backpressure both mean "retry later".
            (OrderOutcome::RateLimited, None)
        } else if status.is_client_error() {
            (OrderOutcome::Rejected, None)
        } else {
            (OrderOutcome::Failed, None)
        };
        Ok(OrderResponse {
            outcome,
            status,
            body,
            latency,
            retry_after,
            order_id,
        })
    }

    pub fn api_url(&self) -> &str {
        &self.api_url
    }
}

#[derive(Debug, Deserialize)]
struct AdmittedOrderData {
    id: Uuid,
}

fn parse_admitted_order_id(body: &str) -> Option<Uuid> {
    serde_json::from_str::<ApiResponse<AdmittedOrderData>>(body)
        .ok()
        .map(|response| response.data.id)
}

#[derive(Debug, Clone)]
pub struct Session {
    pub access_token: String,
    pub expires_at: u64,
}

/// Split so retry/backoff sleeps are not mistaken for slow challenge/login HTTP.
#[derive(Debug, Clone, Copy)]
pub struct AuthTiming {
    pub http: Duration,
    pub wait: Duration,
}

impl Session {
    pub fn needs_refresh(&self, now: u64) -> bool {
        self.expires_at <= now.saturating_add(30)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderOutcome {
    /// HTTP admit succeeded (pre-WS). Prefer Confirmed after fill wait.
    Accepted,
    /// WebSocket reported Confirmed.
    Confirmed,
    RateLimited,
    Rejected,
    /// Admit failure, execution Failed, or WS timeout.
    Failed,
    /// Admitted then WebSocket Failed.
    ExecutionFailed,
    /// Admitted but no terminal WS update before timeout.
    TimedOut,
}

#[derive(Debug)]
pub struct OrderResponse {
    pub outcome: OrderOutcome,
    pub status: StatusCode,
    pub body: String,
    pub latency: Duration,
    /// From `Retry-After` on 429/503; used by the trader loop to back off.
    pub retry_after: Option<u64>,
    /// Server lifecycle id from admit response (`data.id`), when accepted.
    pub order_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct AuthChallenge {
    challenge_id: String,
    message: [u64; 4],
}

#[derive(Debug, Deserialize)]
struct AuthLogin {
    access_token: String,
    expires_at: u64,
}

#[derive(Debug, Deserialize)]
struct NonceLeaseResponse {
    nonce: u32,
    client_order_id: Uuid,
}

#[derive(Serialize)]
struct NewOrderRequest<'a> {
    version: u8,
    client_order_id: Uuid,
    expires_at: u64,
    details: OrderDetails,
    order_type: &'a str,
    user_id: String,
    signed_intent: String,
    pubkey: String,
}

async fn ensure_success(response: reqwest::Response, operation: &str) -> Result<()> {
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let retry_after = retry_after_header(&response);
    let body = response.text().await.unwrap_or_default();
    Err(http_error(operation, status, &body, retry_after))
}

async fn decode_success<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    operation: &str,
) -> Result<T> {
    let status = response.status();
    let retry_after = retry_after_header(&response);
    let body = response.text().await.context("read HTTP response")?;
    if !status.is_success() {
        return Err(http_error(operation, status, &body, retry_after));
    }
    serde_json::from_str(&body).with_context(|| format!("decode {operation} response"))
}

fn retry_after_header(response: &reqwest::Response) -> Option<u64> {
    response
        .headers()
        .get(header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

fn http_error(
    operation: &str,
    status: StatusCode,
    body: &str,
    retry_after: Option<u64>,
) -> anyhow::Error {
    match retry_after {
        Some(secs) => anyhow!("{operation} returned {status}: {body} (retry-after={secs})"),
        None => anyhow!("{operation} returned {status}: {body}"),
    }
}

/// Transient ingress pressure: 429 rate limits and 503 worker saturation.
fn retry_after_from_error(error: &anyhow::Error) -> Option<Duration> {
    let message = error.to_string();
    let retryable =
        message.contains("429 Too Many Requests") || message.contains("503 Service Unavailable");
    if !retryable {
        return None;
    }
    let secs = message
        .rsplit_once("retry-after=")
        .and_then(|(_, rest)| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .ok()
        })
        .unwrap_or(1)
        .max(1);
    Some(Duration::from_secs(secs))
}

fn normalize_url(url: String) -> String {
    url.trim_end_matches('/').to_owned()
}

/// Parses faucet messages like `mint cooldown active; retry in 42s`.
fn parse_cooldown_secs(message: &str) -> Option<u64> {
    let marker = "retry in ";
    let start = message.find(marker)? + marker.len();
    let rest = &message[start..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.parse().ok()
}

fn is_transient_mint_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("connection")
        || lower.contains("connect")
        || lower.contains("reset")
        || lower.contains("broken pipe")
        || lower.contains("temporarily unavailable")
        || lower.contains("503")
        || lower.contains("502")
        || lower.contains("504")
        || lower.contains("429")
}

#[cfg(test)]
mod tests {
    use super::{parse_cooldown_secs, retry_after_from_error};
    use anyhow::anyhow;
    use std::time::Duration;

    #[test]
    fn parses_faucet_cooldown_message() {
        assert_eq!(
            parse_cooldown_secs("mint cooldown active; retry in 42s"),
            Some(42)
        );
        assert_eq!(parse_cooldown_secs("unrelated failure"), None);
    }

    #[test]
    fn parses_auth_retry_after() {
        let err = anyhow!(
            "auth challenge returned 503 Service Unavailable: vault unavailable \
             (retry-after=1)"
        );
        assert_eq!(retry_after_from_error(&err), Some(Duration::from_secs(1)));
        let err = anyhow!(
            "auth login returned 429 Too Many Requests: rate limit exceeded (retry-after=42)"
        );
        assert_eq!(retry_after_from_error(&err), Some(Duration::from_secs(42)));
        assert_eq!(
            retry_after_from_error(&anyhow!("auth login returned 401")),
            None
        );
    }
}
