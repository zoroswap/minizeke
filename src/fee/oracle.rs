use std::{error::Error, fmt, sync::Arc, time::Duration};

use reqwest::{Client, Url};
use serde::Deserialize;
use tokio::{
    sync::RwLock,
    time::{MissedTickBehavior, interval},
};
use tracing::warn;

use super::{
    config::OracleConfig,
    volatility::{VolatilityEstimator, VolatilitySnapshot},
};

const LATEST_PRICES_PATH: &str = "/v1/updates/price/latest";

#[derive(Clone, Debug, PartialEq)]
pub struct OracleSample {
    pub feed_id: String,
    pub reference_asset: String,
    pub price: f64,
    pub publish_time: u64,
}

#[derive(Debug)]
pub enum OracleError {
    Request(reqwest::Error),
    MissingFeed(String),
    InvalidPrice { feed_id: String, price: u64 },
}

impl fmt::Display for OracleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "oracle request failed: {error}"),
            Self::MissingFeed(feed_id) => {
                write!(
                    formatter,
                    "oracle response omitted configured feed {feed_id}"
                )
            }
            Self::InvalidPrice { feed_id, price } => {
                write!(formatter, "invalid oracle price for {feed_id}: {price}")
            }
        }
    }
}

impl Error for OracleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Request(error) => Some(error),
            _ => None,
        }
    }
}

impl From<reqwest::Error> for OracleError {
    fn from(error: reqwest::Error) -> Self {
        Self::Request(error)
    }
}

#[derive(Clone)]
pub struct OracleClient {
    client: Client,
    latest_url: Url,
    feed_id: String,
    reference_asset: String,
}

#[derive(Debug, Deserialize)]
struct LatestPriceResponse {
    parsed: Vec<PriceMetadata>,
}

#[derive(Debug, Deserialize)]
struct PriceMetadata {
    id: String,
    price: OraclePrice,
}

#[derive(Debug, Deserialize)]
struct OraclePrice {
    price: u64,
    publish_time: u64,
}

impl OracleClient {
    pub fn new(config: OracleConfig) -> Result<Self, reqwest::Error> {
        let client = Client::builder().timeout(config.request_timeout).build()?;
        Ok(Self::with_client(client, config))
    }

    pub fn with_client(client: Client, config: OracleConfig) -> Self {
        let mut latest_url = config.base_url;
        latest_url.set_path(LATEST_PRICES_PATH);
        latest_url.set_query(None);
        latest_url.set_fragment(None);
        latest_url
            .query_pairs_mut()
            .append_pair("ids[]", &config.feed_id);
        Self {
            client,
            latest_url,
            feed_id: config.feed_id,
            reference_asset: config.reference_asset,
        }
    }

    pub fn latest_url(&self) -> &Url {
        &self.latest_url
    }

    pub async fn fetch(&self) -> Result<OracleSample, OracleError> {
        let response = self
            .client
            .get(self.latest_url.clone())
            .send()
            .await?
            .error_for_status()?
            .json::<LatestPriceResponse>()
            .await?;
        parse_sample(response, &self.feed_id, &self.reference_asset)
    }

    pub async fn sample_once(
        &self,
        estimator: &mut VolatilityEstimator,
    ) -> Result<Option<VolatilitySnapshot>, OracleError> {
        let sample = self.fetch().await?;
        estimator
            .update(sample.price)
            .map_err(|_| OracleError::InvalidPrice {
                feed_id: sample.feed_id,
                price: sample.price.max(0.0) as u64,
            })
    }

    pub async fn run_sampler(
        self,
        estimator: Arc<RwLock<VolatilityEstimator>>,
        sample_interval: Duration,
    ) {
        let mut ticker = interval(sample_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match self.fetch().await {
                Ok(sample) => {
                    if let Err(error) = estimator.write().await.update(sample.price) {
                        warn!(
                            reference_asset = %self.reference_asset,
                            feed_id = %self.feed_id,
                            %error,
                            "rejected fee reference oracle sample"
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        reference_asset = %self.reference_asset,
                        feed_id = %self.feed_id,
                        %error,
                        "failed to sample fee reference oracle"
                    );
                }
            }
        }
    }
}

fn parse_sample(
    response: LatestPriceResponse,
    feed_id: &str,
    reference_asset: &str,
) -> Result<OracleSample, OracleError> {
    let metadata = response
        .parsed
        .into_iter()
        .find(|metadata| metadata.id == feed_id)
        .ok_or_else(|| OracleError::MissingFeed(feed_id.to_owned()))?;
    if metadata.price.price == 0 {
        return Err(OracleError::InvalidPrice {
            feed_id: feed_id.to_owned(),
            price: metadata.price.price,
        });
    }
    Ok(OracleSample {
        feed_id: metadata.id,
        reference_asset: reference_asset.to_owned(),
        price: metadata.price.price as f64,
        publish_time: metadata.price.publish_time,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> OracleConfig {
        OracleConfig {
            base_url: Url::parse("https://oracle.example/ignored?old=1").unwrap(),
            feed_id: "btc-feed".to_owned(),
            reference_asset: "BTC".to_owned(),
            request_timeout: Duration::from_secs(10),
        }
    }

    fn response(json: &str) -> LatestPriceResponse {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn builds_minizeke_latest_price_url() {
        let client = OracleClient::new(config()).unwrap();
        assert_eq!(
            client.latest_url().as_str(),
            "https://oracle.example/v1/updates/price/latest?ids%5B%5D=btc-feed"
        );
    }

    #[test]
    fn selects_configured_feed_and_preserves_metadata() {
        let sample = parse_sample(
            response(
                r#"{"parsed":[
                    {"id":"eth-feed","price":{"price":2000,"publish_time":10}},
                    {"id":"btc-feed","price":{"price":65000,"publish_time":11}}
                ]}"#,
            ),
            "btc-feed",
            "BTC",
        )
        .unwrap();
        assert_eq!(
            sample,
            OracleSample {
                feed_id: "btc-feed".to_owned(),
                reference_asset: "BTC".to_owned(),
                price: 65_000.0,
                publish_time: 11,
            }
        );
    }

    #[test]
    fn rejects_missing_and_zero_prices() {
        assert!(matches!(
            parse_sample(response(r#"{"parsed":[]}"#), "btc-feed", "BTC"),
            Err(OracleError::MissingFeed(_))
        ));
        assert!(matches!(
            parse_sample(
                response(r#"{"parsed":[{"id":"btc-feed","price":{"price":0,"publish_time":11}}]}"#),
                "btc-feed",
                "BTC"
            ),
            Err(OracleError::InvalidPrice { .. })
        ));
    }
}
