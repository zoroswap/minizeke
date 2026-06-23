use crate::{
    message_broker::message_broker::{MessageBroker, OraclePriceEvent},
    price::PriceData,
    store::Store,
};
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use miden_client::{
    account::AccountId,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
};
use reqwest_eventsource::{Event, EventSource};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, str::FromStr, sync::Arc};
use tracing::{error, info};
use url::Url;

type OracleId = &'static str;

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

pub struct OracleSSEClient {
    state: Arc<Store>,
    message_broker: Arc<MessageBroker>,
}

impl OracleSSEClient {
    pub fn new(state: Arc<Store>, message_broker: Arc<MessageBroker>) -> Self {
        Self {
            state,
            message_broker,
        }
    }

    pub async fn start(&mut self) -> Result<String> {
        let url = env::var("ORACLE_SSE").unwrap();
        let mut url = Url::from_str(&url).expect("Failed parsing Oracle SSE endpoint string");
        let assets_to_listen_for: Vec<String> = OraclePricing::supported_assets()
            .values()
            .cloned()
            .collect();

        let query = assets_to_listen_for
            .iter()
            .map(|asset| format!("ids[]={}", asset))
            .collect::<Vec<String>>()
            .join("&");
        url.set_query(Some(&query));
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
        let url = env::var("ORACLE_HTTPS").unwrap();
        let assets_to_listen_for: Vec<String> = OraclePricing::supported_assets()
            .values()
            .cloned()
            .collect();
        let prices = get_oracle_prices(&url, assets_to_listen_for).await?;

        for price_update in prices {
            if let Ok(faucet_id) = OraclePricing::oracle_id_to_faucet_id(&price_update.id) {
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
            if let Ok(faucet_id) = OraclePricing::oracle_id_to_faucet_id(&price_update.id) {
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
    let query = ids
        .iter()
        .map(|id| format!("ids[]={}", id))
        .collect::<Vec<String>>()
        .join("&");
    url.set_query(Some(&query));
    let resp = reqwest::get(url).await?.text().await?;
    let parsed_event = serde_json::from_str::<ParsedEvent>(&resp)?;
    Ok(parsed_event.parsed)
}

#[derive(Debug)]
pub struct OraclePricing {
    oracle_prices: DashMap<AccountId, PriceData>,
    last_updated: DateTime<Utc>,
}

impl OraclePricing {
    pub fn new() -> Self {
        Self {
            oracle_prices: DashMap::new(),
            last_updated: Utc::now(),
        }
    }

    pub fn supported_assets() -> HashMap<AccountId, String> {
        let mut hashmap = HashMap::with_capacity(2);
        let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap();
        let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap();
        let asset0_oracle =
            "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43".to_string();
        let asset1_oracle =
            "ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace".to_string();
        hashmap.insert(asset0, asset0_oracle);
        hashmap.insert(asset1, asset1_oracle);
        hashmap
    }

    pub fn oracle_id_to_faucet_id(id: &String) -> Result<AccountId> {
        Self::supported_assets()
            .iter()
            .find_map(|(faucet_id, oracle_id)| {
                if oracle_id.eq(id) {
                    Some(faucet_id.clone())
                } else {
                    None
                }
            })
            .ok_or(anyhow!("Asset not found"))
    }

    pub fn update_oracle_prices(&self, updates: Vec<PriceMetadata>) {
        for price_update in updates {
            match Self::oracle_id_to_faucet_id(&price_update.id) {
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
        match Self::oracle_id_to_faucet_id(&price_update.oracle_id) {
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
