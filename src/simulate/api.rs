use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use base64::{Engine as _, engine::general_purpose};
use miden_client::{
    Serializable,
    account::AccountId,
    auth::{AuthSecretKey, PublicKey},
};
use miden_core::{Felt, Word};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::warn;
use uuid::Uuid;

use crate::{faucet::MintRequest, intent::INTENT_VERSION, order::OrderDetails};

#[derive(Clone)]
pub struct SimulationApi {
    client: Client,
    api_url: String,
    faucet_url: String,
    faucet_token: Option<String>,
}

impl SimulationApi {
    pub fn new(
        api_url: impl Into<String>,
        faucet_url: impl Into<String>,
        faucet_token: Option<String>,
    ) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .context("build simulation HTTP client")?;
        Ok(Self {
            client,
            api_url: normalize_url(api_url.into()),
            faucet_url: normalize_url(faucet_url.into()),
            faucet_token,
        })
    }

    /// Mint once. On faucet cooldown, sleeps the reported wait and retries until success.
    pub async fn mint(&self, user_id: AccountId, faucet_id: AccountId) -> Result<Duration> {
        let started = Instant::now();
        loop {
            match self.mint_once(user_id, faucet_id).await {
                Ok(()) => return Ok(started.elapsed()),
                Err(error) => {
                    let message = error.to_string();
                    if let Some(wait_secs) = parse_cooldown_secs(&message) {
                        warn!(
                            recipient = %user_id.to_hex(),
                            faucet = %faucet_id.to_hex(),
                            wait_secs,
                            "faucet mint cooldown; waiting"
                        );
                        tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                        continue;
                    }
                    return Err(error);
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
            .bearer_auth(token)
            .json(&MintRequest {
                address: user_id.to_hex(),
                faucet_id: faucet_id.to_hex(),
            })
            .send()
            .await
            .context("send faucet mint request")?;
        ensure_success(response, "faucet mint").await
    }

    pub async fn authenticate(
        &self,
        user_id: AccountId,
        key: &AuthSecretKey,
    ) -> Result<(Session, Duration)> {
        let started = Instant::now();
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
        Ok((
            Session {
                access_token: login.data.access_token,
                expires_at: login.data.expires_at,
            },
            started.elapsed(),
        ))
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
        let body = response.text().await.context("read order response")?;
        let outcome = if status.is_success() {
            OrderOutcome::Accepted
        } else if status == StatusCode::TOO_MANY_REQUESTS {
            OrderOutcome::RateLimited
        } else if status.is_client_error() {
            OrderOutcome::Rejected
        } else {
            OrderOutcome::Failed
        };
        Ok(OrderResponse {
            outcome,
            status,
            body,
            latency,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    pub access_token: String,
    pub expires_at: u64,
}

impl Session {
    pub fn needs_refresh(&self, now: u64) -> bool {
        self.expires_at <= now.saturating_add(30)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderOutcome {
    Accepted,
    RateLimited,
    Rejected,
    Failed,
}

#[derive(Debug)]
pub struct OrderResponse {
    pub outcome: OrderOutcome,
    pub status: StatusCode,
    pub body: String,
    pub latency: Duration,
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
    let body = response.text().await.unwrap_or_default();
    Err(anyhow!("{operation} returned {status}: {body}"))
}

async fn decode_success<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    operation: &str,
) -> Result<T> {
    let status = response.status();
    let body = response.text().await.context("read HTTP response")?;
    if !status.is_success() {
        return Err(anyhow!("{operation} returned {status}: {body}"));
    }
    serde_json::from_str(&body).with_context(|| format!("decode {operation} response"))
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

#[cfg(test)]
mod tests {
    use super::parse_cooldown_secs;

    #[test]
    fn parses_faucet_cooldown_message() {
        assert_eq!(
            parse_cooldown_secs("mint cooldown active; retry in 42s"),
            Some(42)
        );
        assert_eq!(parse_cooldown_secs("unrelated failure"), None);
    }
}
