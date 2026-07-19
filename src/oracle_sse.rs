use crate::{
    deployment::AssetInfo,
    message_broker::message_broker::{MessageBroker, OraclePriceEvent},
    price::PriceData,
    store::Store,
};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use miden_client::account::AccountId;
use reqwest_eventsource::{Event, EventSource};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, str::FromStr, sync::Arc};
use tracing::{error, info};
use url::Url;

const PRICE_FEEDS_PATH: &str = "/v1/price_feeds";
const LATEST_PRICES_PATH: &str = "/v1/updates/price/latest";
const PRICE_STREAM_PATH: &str = "/v1/updates/price/stream";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Binary {
    pub data: Vec<String>,
    pub encoding: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Price {
    pub price: u64,
    pub publish_time: u64,
}

impl From<Price> for PriceData {
    fn from(value: Price) -> Self {
        PriceData {
            price: value.price,
            timestamp: value.publish_time,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceMetadata {
    pub id: String,
    pub metadata: serde_json::Value,
    pub price: Price,
}

#[derive(Deserialize, Debug)]
pub struct ParsedEvent {
    pub parsed: Vec<PriceMetadata>,
    pub binary: Binary,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PriceFeedAttributes {
    pub base: String,
    pub quote_currency: String,
    pub description: String,
    pub display_symbol: String,
    pub generic_symbol: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PriceFeedInfo {
    pub id: String,
    pub attributes: PriceFeedAttributes,
}

pub fn oracle_base_url() -> Result<String> {
    env::var("ORACLE_URL").context("ORACLE_URL is required")
}

fn oracle_endpoint(base_url: &str, path: &str) -> Result<Url> {
    let mut url = Url::parse(base_url).context("ORACLE_URL must be a valid URL")?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn add_feed_query(url: &mut Url, ids: impl IntoIterator<Item = String>) {
    url.query_pairs_mut()
        .extend_pairs(ids.into_iter().map(|id| ("ids[]", id)));
}

pub async fn fetch_price_feeds(oracle_url: &str) -> Result<Vec<PriceFeedInfo>> {
    let url = oracle_endpoint(oracle_url, PRICE_FEEDS_PATH)?;
    reqwest::get(url)
        .await?
        .error_for_status()?
        .json()
        .await
        .context("failed to decode oracle price feed catalog")
}

pub fn resolve_feed_id(feeds: &[PriceFeedInfo], symbol: &str) -> Result<String> {
    let matches = feeds
        .iter()
        .filter(|feed| feed.attributes.base.eq_ignore_ascii_case(symbol))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [feed] => Ok(feed.id.clone()),
        [] => Err(anyhow!("oracle has no price feed for {symbol}")),
        _ => Err(anyhow!("oracle has multiple price feeds for {symbol}")),
    }
}

pub fn validate_asset_feeds(assets: &[AssetInfo], feeds: &[PriceFeedInfo]) -> Result<()> {
    for asset in assets {
        let feed = feeds
            .iter()
            .find(|feed| feed.id == asset.oracle_feed_id)
            .ok_or_else(|| {
                anyhow!(
                    "oracle feed {} for {} is no longer listed",
                    asset.oracle_feed_id,
                    asset.symbol
                )
            })?;
        if !feed.attributes.base.eq_ignore_ascii_case(&asset.symbol) {
            return Err(anyhow!(
                "oracle feed {} is for {}, deployment assigns it to {}",
                feed.id,
                feed.attributes.base,
                asset.symbol
            ));
        }
    }
    Ok(())
}

pub struct OracleSSEClient {
    state: Arc<Store>,
    message_broker: Arc<MessageBroker>,
    oracle_assets: HashMap<AccountId, String>,
}

impl OracleSSEClient {
    pub fn new(
        state: Arc<Store>,
        message_broker: Arc<MessageBroker>,
        assets: &[AssetInfo],
    ) -> Self {
        Self {
            state,
            message_broker,
            oracle_assets: oracle_asset_map(assets),
        }
    }

    pub async fn start(&mut self) -> Result<String> {
        let base_url = oracle_base_url()?;
        let mut url = oracle_endpoint(&base_url, PRICE_STREAM_PATH)?;
        let assets_to_listen_for: Vec<String> = self.oracle_assets.values().cloned().collect();
        add_feed_query(&mut url, assets_to_listen_for);
        loop {
            let mut es = EventSource::get(url.clone());
            while let Some(event) = es.next().await {
                match event {
                    Ok(Event::Open) => info!("Oracle connection open!"),
                    Ok(Event::Message(message)) => {
                        if let Err(e) = self.update_prices(&message.data) {
                            error!("Failed to update oracle prices in state: {e:?}");
                        };
                    }
                    Err(err) => {
                        error!("Oracle SSE event error: {err:?}");
                        es.close();
                    }
                }
            }
            error!("Oracle SSE connection dropped.");
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }

    pub async fn init_prices(&self) -> Result<()> {
        let base_url = oracle_base_url()?;
        let url = oracle_endpoint(&base_url, LATEST_PRICES_PATH)?;
        let assets_to_listen_for: Vec<String> = self.oracle_assets.values().cloned().collect();
        let prices = get_oracle_prices(url.as_str(), assets_to_listen_for).await?;

        for price_update in prices {
            if let Some(faucet_id) = oracle_id_to_faucet_id(&self.oracle_assets, &price_update.id) {
                let _ = self
                    .message_broker
                    .broadcast_oracle_price(OraclePriceEvent {
                        oracle_id: price_update.id.clone(),
                        faucet_id: faucet_id.to_hex(),
                        price: price_update.price.price,
                        timestamp: price_update.price.publish_time,
                    });
            }
        }
        Ok(())
    }

    fn update_prices(&self, message: &str) -> Result<()> {
        let parsed_json = serde_json::from_str::<ParsedEvent>(message)?;
        for price_update in parsed_json.parsed {
            if let Some(faucet_id) = oracle_id_to_faucet_id(&self.oracle_assets, &price_update.id) {
                let _ = self
                    .message_broker
                    .broadcast_oracle_price(OraclePriceEvent {
                        oracle_id: price_update.id.clone(),
                        faucet_id: faucet_id.to_hex(),
                        price: price_update.price.price,
                        timestamp: price_update.price.publish_time,
                    });
            }
        }
        Ok(())
    }
}

pub async fn get_oracle_prices(oracle_url: &str, ids: Vec<String>) -> Result<Vec<PriceMetadata>> {
    let mut url = Url::from_str(oracle_url)?;
    add_feed_query(&mut url, ids);
    let event = reqwest::get(url)
        .await?
        .error_for_status()?
        .json::<ParsedEvent>()
        .await?;
    Ok(event.parsed)
}

#[derive(Debug)]
pub struct OraclePricing {
    oracle_prices: DashMap<AccountId, PriceData>,
    oracle_assets: HashMap<AccountId, String>,
    last_updated: DateTime<Utc>,
}

impl OraclePricing {
    pub fn new(assets: &[AssetInfo]) -> Self {
        Self {
            oracle_prices: DashMap::new(),
            oracle_assets: oracle_asset_map(assets),
            last_updated: Utc::now(),
        }
    }

    pub fn supported_assets(&self) -> &HashMap<AccountId, String> {
        &self.oracle_assets
    }

    pub fn oracle_id_to_faucet_id(&self, id: &str) -> Result<AccountId> {
        oracle_id_to_faucet_id(&self.oracle_assets, id).ok_or(anyhow!("Asset not found"))
    }

    pub fn update_oracle_prices(&self, updates: Vec<PriceMetadata>) {
        for price_update in updates {
            match self.oracle_id_to_faucet_id(&price_update.id) {
                Ok(faucet_id) => {
                    self.oracle_prices.insert(
                        faucet_id,
                        PriceData::new(price_update.price.publish_time, price_update.price.price),
                    );
                }
                Err(e) => {
                    error!(
                        "Failed to map oracle id '{}' to faucet id: {e:?}",
                        price_update.id
                    )
                }
            }
        }
    }

    pub fn update_from_price_event(&self, price_update: OraclePriceEvent) {
        match self.oracle_id_to_faucet_id(&price_update.oracle_id) {
            Ok(faucet_id) => {
                self.oracle_prices.insert(
                    faucet_id,
                    PriceData::new(price_update.timestamp, price_update.price),
                );
            }
            Err(e) => {
                error!(
                    "Failed to map oracle id '{}' to faucet id: {e:?}",
                    price_update.oracle_id
                )
            }
        }
    }

    pub fn get_prices(&self) -> HashMap<AccountId, PriceData> {
        self.oracle_prices.clone().into_iter().collect()
    }

    pub fn get_price_for_asset(&self, asset: AccountId) -> Option<PriceData> {
        let price = self.oracle_prices.get(&asset);
        if let Some(price) = price {
            Some(*price)
        } else {
            None
        }
    }
}

fn oracle_asset_map(assets: &[AssetInfo]) -> HashMap<AccountId, String> {
    assets
        .iter()
        .map(|asset| (asset.faucet_id, asset.oracle_feed_id.clone()))
        .collect()
}

fn oracle_id_to_faucet_id(
    oracle_assets: &HashMap<AccountId, String>,
    id: &str,
) -> Option<AccountId> {
    oracle_assets
        .iter()
        .find_map(|(faucet_id, oracle_id)| (oracle_id == id).then_some(*faucet_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(base: &str, id: &str) -> PriceFeedInfo {
        PriceFeedInfo {
            id: id.to_string(),
            attributes: PriceFeedAttributes {
                base: base.to_string(),
                quote_currency: "USD".to_string(),
                description: String::new(),
                display_symbol: format!("{base}/USD"),
                generic_symbol: format!("{base}USD"),
            },
        }
    }

    #[test]
    fn resolves_feed_by_base_symbol() -> Result<()> {
        let feeds = [feed("BTC", "btc-feed"), feed("USDC", "usdc-feed")];
        assert_eq!(resolve_feed_id(&feeds, "btc")?, "btc-feed");
        assert!(resolve_feed_id(&feeds, "ETH").is_err());
        Ok(())
    }

    #[test]
    fn validates_deployment_feed_ids_and_symbols() -> Result<()> {
        let feeds = [feed("BTC", "btc-feed"), feed("ETH", "eth-feed")];
        let asset = AssetInfo {
            faucet_id: AccountId::try_from(
                miden_client::testing::account_id::ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1,
            )?,
            symbol: "BTC".to_string(),
            decimals: 8,
            oracle_feed_id: "btc-feed".to_string(),
        };
        validate_asset_feeds(std::slice::from_ref(&asset), &feeds)?;

        let mut wrong_symbol = asset.clone();
        wrong_symbol.symbol = "ETH".to_string();
        assert!(validate_asset_feeds(&[wrong_symbol], &feeds).is_err());

        let mut missing_feed = asset;
        missing_feed.oracle_feed_id = "missing-feed".to_string();
        assert!(validate_asset_feeds(&[missing_feed], &feeds).is_err());
        Ok(())
    }

    #[test]
    fn derives_oracle_paths_from_base_url() -> Result<()> {
        let cases = [
            (PRICE_FEEDS_PATH, "https://oracle.example/v1/price_feeds"),
            (
                LATEST_PRICES_PATH,
                "https://oracle.example/v1/updates/price/latest",
            ),
            (
                PRICE_STREAM_PATH,
                "https://oracle.example/v1/updates/price/stream",
            ),
        ];
        for (path, expected) in cases {
            assert_eq!(
                oracle_endpoint("https://oracle.example/old/path?ignored=1", path)?.as_str(),
                expected
            );
        }
        Ok(())
    }
}
