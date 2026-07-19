use alloy_primitives::U256;
use anyhow::{Result, anyhow};
use serde::Serialize;

use crate::{
    curve::get_curve_amount_out,
    history::{PRICE_SCALE, canonical_oracle_price},
    pool::PoolState,
};

#[derive(Debug, Clone, Serialize)]
pub struct DepthLevel {
    /// Quote-asset units per base-asset unit, scaled by 1e12.
    pub price: u64,
    /// Marginal base-asset quantity available at this level.
    pub amount: u64,
    /// Cumulative base-asset quantity through this level.
    pub cumulative: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct DepthResponse {
    pub base_asset: String,
    pub quote_asset: String,
    pub mid_price: u64,
    pub timestamp: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

pub fn derive_depth(
    base_asset: String,
    quote_asset: String,
    base_pool: &PoolState,
    quote_pool: &PoolState,
    base_oracle_price: u64,
    quote_oracle_price: u64,
    levels: usize,
    timestamp: u64,
) -> Result<DepthResponse> {
    if levels == 0 || levels > 100 {
        return Err(anyhow!("levels must be between 1 and 100"));
    }
    let mid_price = canonical_oracle_price(base_oracle_price, quote_oracle_price)
        .ok_or_else(|| anyhow!("invalid zero quote oracle price"))?;
    let base_step = depth_step(base_pool, levels)?;
    let quote_step = depth_step(quote_pool, levels)?;

    let bids = derive_side(base_pool, quote_pool, base_step, levels, mid_price, true)?;
    let inverse_mid = u64::try_from(
        PRICE_SCALE
            .checked_mul(PRICE_SCALE)
            .ok_or_else(|| anyhow!("inverse price overflow"))?
            / mid_price as u128,
    )?;
    let asks = derive_side(
        quote_pool,
        base_pool,
        quote_step,
        levels,
        inverse_mid,
        false,
    )?;

    Ok(DepthResponse {
        base_asset,
        quote_asset,
        mid_price,
        timestamp,
        bids,
        asks,
    })
}

fn depth_step(pool: &PoolState, levels: usize) -> Result<u64> {
    // Display one percent of liabilities over the requested number of levels.
    let step =
        (pool.balances().total_liabilities / U256::from(100 * levels)).saturating_to::<u64>();
    if step == 0 {
        return Err(anyhow!("pool liabilities are too small to derive depth"));
    }
    Ok(step)
}

fn derive_side(
    input_pool: &PoolState,
    output_pool: &PoolState,
    input_step: u64,
    levels: usize,
    curve_price: u64,
    input_is_base: bool,
) -> Result<Vec<DepthLevel>> {
    let mut result = Vec::with_capacity(levels);
    let mut previous_input = 0u64;
    let mut previous_output = 0u64;

    for index in 1..=levels {
        let cumulative_input = input_step
            .checked_mul(index as u64)
            .ok_or_else(|| anyhow!("depth input overflow"))?;
        let quote = get_curve_amount_out(
            input_pool,
            output_pool,
            U256::from(input_pool.metadata().asset_decimals),
            U256::from(output_pool.metadata().asset_decimals),
            U256::from(cumulative_input),
            U256::from(curve_price),
        )?;
        let cumulative_output = quote.amount_out.saturating_to::<u64>();
        if cumulative_output == 0 || cumulative_output <= previous_output {
            break;
        }

        let marginal_input = cumulative_input - previous_input;
        let marginal_output = cumulative_output - previous_output;
        let (base_amount, cumulative_base, quote_amount) = if input_is_base {
            (marginal_input, cumulative_input, marginal_output)
        } else {
            (marginal_output, cumulative_output, marginal_input)
        };
        if base_amount == 0 {
            break;
        }
        let price = u64::try_from(
            (quote_amount as u128)
                .checked_mul(PRICE_SCALE)
                .ok_or_else(|| anyhow!("depth price overflow"))?
                / base_amount as u128,
        )?;
        result.push(DepthLevel {
            price,
            amount: base_amount,
            cumulative: cumulative_base,
        });
        previous_input = cumulative_input;
        previous_output = cumulative_output;
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::I256;

    use super::*;
    use crate::pool::{PoolBalances, PoolMetadata, PoolSettings};

    fn balanced_pool() -> PoolState {
        PoolState::new(
            PoolSettings {
                beta: I256::from_dec_str("10000000000000000").unwrap(),
                c: I256::from_dec_str("16000000000000000000").unwrap(),
                swap_fee: U256::from(200),
                backstop_fee: U256::from(300),
                protocol_fee: U256::ZERO,
                ..PoolSettings::default()
            },
            PoolBalances {
                reserve: U256::from(100_000_000_000u64),
                reserve_with_slippage: U256::from(100_000_000_000u64),
                total_liabilities: U256::from(100_000_000_000u64),
            },
            1_000_000_000,
            PoolMetadata {
                name: "test",
                asset_decimals: 8,
            },
        )
    }

    #[test]
    fn depth_has_monotonic_cumulative_base_amounts() {
        let pool = balanced_pool();
        let depth =
            derive_depth("base".into(), "quote".into(), &pool, &pool, 100, 100, 10, 1).unwrap();
        assert!(!depth.bids.is_empty());
        assert!(!depth.asks.is_empty());
        assert!(
            depth
                .bids
                .windows(2)
                .all(|w| w[0].cumulative < w[1].cumulative)
        );
        assert!(
            depth
                .asks
                .windows(2)
                .all(|w| w[0].cumulative < w[1].cumulative)
        );
    }
}
