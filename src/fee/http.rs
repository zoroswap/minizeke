use reqwest::{Client, Request, Url};
use serde::{Deserialize, Serialize};

use super::config::HttpConfig;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    Automatic,
    Manual,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FeeUpdate {
    pub faucet_id: String,
    pub volatility_fee_in: u16,
    pub volatility_fee_out: u16,
    pub volatility_fee_valid_until: u64,
    pub volatility_fee_source: FeeSource,
}

impl FeeUpdate {
    pub fn automatic(faucet_id: impl Into<String>, fee: u16, valid_until: u64) -> Self {
        Self {
            faucet_id: faucet_id.into(),
            volatility_fee_in: fee,
            volatility_fee_out: fee,
            volatility_fee_valid_until: valid_until,
            volatility_fee_source: FeeSource::Automatic,
        }
    }

    pub fn manual(
        faucet_id: impl Into<String>,
        fee_in: u16,
        fee_out: u16,
        valid_until: u64,
    ) -> Self {
        Self {
            faucet_id: faucet_id.into(),
            volatility_fee_in: fee_in,
            volatility_fee_out: fee_out,
            volatility_fee_valid_until: valid_until,
            volatility_fee_source: FeeSource::Manual,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BatchFeeUpdateRequest {
    pub updates: Vec<FeeUpdate>,
}

impl BatchFeeUpdateRequest {
    pub fn automatic(
        faucet_ids: impl IntoIterator<Item = impl Into<String>>,
        fee: u16,
        valid_until: u64,
    ) -> Self {
        Self {
            updates: faucet_ids
                .into_iter()
                .map(|id| FeeUpdate::automatic(id, fee, valid_until))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BatchFeeUpdateResponse {
    pub updated: usize,
    pub version: u64,
}

#[derive(Clone)]
pub struct MinizekeFeeClient {
    client: Client,
    batch_url: Url,
    admin_token: Option<String>,
}

impl MinizekeFeeClient {
    pub fn new(config: HttpConfig) -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(config.request_timeout).build()?;
        Ok(Self::with_client(client, config))
    }

    pub fn with_client(client: Client, config: HttpConfig) -> Self {
        let mut batch_url = config.server_url;
        batch_url.set_path(&config.batch_path);
        batch_url.set_query(None);
        batch_url.set_fragment(None);
        Self {
            client,
            batch_url,
            admin_token: config.admin_token,
        }
    }

    pub fn batch_url(&self) -> &Url {
        &self.batch_url
    }

    pub fn build_push_request(
        &self,
        payload: &BatchFeeUpdateRequest,
    ) -> Result<Request, reqwest::Error> {
        let mut builder = self.client.post(self.batch_url.clone()).json(payload);
        if let Some(token) = &self.admin_token {
            builder = builder.bearer_auth(token);
        }
        builder.build()
    }

    pub async fn push_batch(
        &self,
        payload: &BatchFeeUpdateRequest,
    ) -> Result<BatchFeeUpdateResponse, reqwest::Error> {
        self.client
            .execute(self.build_push_request(payload)?)
            .await?
            .error_for_status()?
            .json()
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::{Method, header::AUTHORIZATION};

    use super::*;

    fn config(token: Option<&str>) -> HttpConfig {
        HttpConfig {
            server_url: Url::parse("http://127.0.0.1:3000/ignored?old=1").unwrap(),
            batch_path: "/admin/fees/batch".to_owned(),
            admin_token: token.map(str::to_owned),
            request_timeout: Duration::from_secs(10),
        }
    }

    #[test]
    fn serializes_batch_payload_and_sources() {
        let payload = BatchFeeUpdateRequest {
            updates: vec![
                FeeUpdate::automatic("0xaaa", 850, 1_000),
                FeeUpdate::manual("0xbbb", 100, 200, 2_000),
            ],
        };
        assert_eq!(
            serde_json::to_value(payload).unwrap(),
            serde_json::json!({
                "updates": [
                    {
                        "faucet_id": "0xaaa",
                        "volatility_fee_in": 850,
                        "volatility_fee_out": 850,
                        "volatility_fee_valid_until": 1000,
                        "volatility_fee_source": "automatic"
                    },
                    {
                        "faucet_id": "0xbbb",
                        "volatility_fee_in": 100,
                        "volatility_fee_out": 200,
                        "volatility_fee_valid_until": 2000,
                        "volatility_fee_source": "manual"
                    }
                ]
            })
        );
    }

    #[test]
    fn creates_uniform_automatic_batch() {
        let payload = BatchFeeUpdateRequest::automatic(["a", "b", "c"], 42, 600);
        assert_eq!(payload.updates.len(), 3);
        assert!(payload.updates.iter().all(|update| {
            update.volatility_fee_in == 42
                && update.volatility_fee_out == 42
                && update.volatility_fee_source == FeeSource::Automatic
        }));
    }

    #[test]
    fn builds_authenticated_post_request() {
        let client = MinizekeFeeClient::new(config(Some("secret"))).unwrap();
        let request = client
            .build_push_request(&BatchFeeUpdateRequest::automatic(["asset"], 10, 20))
            .unwrap();
        assert_eq!(request.method(), Method::POST);
        assert_eq!(
            request.url().as_str(),
            "http://127.0.0.1:3000/admin/fees/batch"
        );
        assert_eq!(request.headers()[AUTHORIZATION], "Bearer secret");
        assert!(request.body().is_some());
    }

    #[test]
    fn supports_an_unauthenticated_adapter_configuration() {
        let client = MinizekeFeeClient::new(config(None)).unwrap();
        let request = client
            .build_push_request(&BatchFeeUpdateRequest::automatic(["asset"], 10, 20))
            .unwrap();
        assert!(!request.headers().contains_key(AUTHORIZATION));
    }
}
