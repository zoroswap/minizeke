use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::Client;
use url::Url;

use crate::{deployment::AssetInfo, oracle_sse::ParsedEvent};

#[derive(Clone)]
pub struct OracleClient {
    client: Client,
    latest_url: Url,
}

impl OracleClient {
    pub fn new(base_url: &str) -> Result<Self> {
        let mut latest_url = Url::parse(base_url).context("oracle URL must be valid")?;
        latest_url.set_path("/v1/updates/price/latest");
        latest_url.set_query(None);
        latest_url.set_fragment(None);
        Ok(Self {
            client: Client::new(),
            latest_url,
        })
    }

    pub async fn prices(
        &self,
        sell: &AssetInfo,
        buy: &AssetInfo,
    ) -> Result<(AssetPrices, Duration)> {
        let mut url = self.latest_url.clone();
        url.query_pairs_mut()
            .append_pair("ids[]", &sell.oracle_feed_id)
            .append_pair("ids[]", &buy.oracle_feed_id);
        let started = std::time::Instant::now();
        let event: ParsedEvent = self
            .client
            .get(url)
            .send()
            .await
            .context("request oracle prices")?
            .error_for_status()
            .context("oracle latest-price request failed")?
            .json()
            .await
            .context("decode oracle latest-price response")?;
        let latency = started.elapsed();
        let prices = event
            .parsed
            .into_iter()
            .map(|item| (item.id, item.price.price))
            .collect::<HashMap<_, _>>();
        let sell_price = prices
            .get(&sell.oracle_feed_id)
            .copied()
            .ok_or_else(|| anyhow!("oracle omitted {} price", sell.symbol))?;
        let buy_price = prices
            .get(&buy.oracle_feed_id)
            .copied()
            .ok_or_else(|| anyhow!("oracle omitted {} price", buy.symbol))?;
        if sell_price == 0 || buy_price == 0 {
            bail!("oracle returned a zero price");
        }
        Ok((
            AssetPrices {
                sell: sell_price,
                buy: buy_price,
            },
            latency,
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AssetPrices {
    pub sell: u64,
    pub buy: u64,
}

pub fn minimum_amount_out(
    amount_in: u64,
    sell_decimals: u8,
    buy_decimals: u8,
    prices: AssetPrices,
    slippage_bps: u16,
) -> Result<u64> {
    let sell_scale = 10_u128
        .checked_pow(u32::from(sell_decimals))
        .ok_or_else(|| anyhow!("sell asset decimal scale overflow"))?;
    let buy_scale = 10_u128
        .checked_pow(u32::from(buy_decimals))
        .ok_or_else(|| anyhow!("buy asset decimal scale overflow"))?;
    let numerator = u128::from(amount_in)
        .checked_mul(u128::from(prices.sell))
        .and_then(|value| value.checked_mul(buy_scale))
        .and_then(|value| value.checked_mul(u128::from(10_000 - slippage_bps)))
        .ok_or_else(|| anyhow!("oracle quote overflow"))?;
    let denominator = u128::from(prices.buy)
        .checked_mul(sell_scale)
        .and_then(|value| value.checked_mul(10_000))
        .ok_or_else(|| anyhow!("oracle quote denominator overflow"))?;
    let output = numerator / denominator;
    u64::try_from(output.max(1)).context("minimum output does not fit u64")
}

#[cfg(test)]
mod tests {
    use super::{AssetPrices, minimum_amount_out};

    #[test]
    fn quote_accounts_for_decimals_and_slippage() {
        let amount = minimum_amount_out(
            100_000_000,
            8,
            6,
            AssetPrices {
                sell: 2_000,
                buy: 1,
            },
            500,
        )
        .unwrap();
        assert_eq!(amount, 1_900_000_000);
    }
}
