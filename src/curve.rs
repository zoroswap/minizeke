// # Important
//
// This is our proprietary curve!
// Never publish or open source this file!

use alloy_primitives::{I256, U256};
use anyhow::{Result, anyhow};
use std::cmp::min;
use std::ops::Div;
use std::str::FromStr;
use tracing::debug;

use crate::pool::{PoolBalances, PoolState};

pub fn mul_u(a: U256, b: U256) -> U256 {
    a.saturating_mul(b).div(U256::from(ZoroCurve::MANTISSA))
}

pub fn mul_i(a: I256, b: I256) -> I256 {
    a.saturating_mul(b)
        .saturating_div(I256::from_raw(U256::from(ZoroCurve::MANTISSA)))
}

pub fn div(a: U256, b: U256) -> U256 {
    a.saturating_mul(U256::from(ZoroCurve::MANTISSA)).div(b)
}

pub fn div_i(a: I256, b: I256) -> I256 {
    a.saturating_mul(I256::from_raw(U256::from(ZoroCurve::MANTISSA)))
        .saturating_div(b)
}

fn u256_to_i256(value: U256) -> I256 {
    I256::from_str(&value.to_string())
        .unwrap_or_else(|err| panic!("Failed to convert U256 to I256: {err:?}"))
}

/// ZoroCurve implementation for mathematical curve calculations.
///
/// Provides methods for calculating curve parameters using high-precision
/// arithmetic with 18 decimal places.
#[derive(Debug, Clone, Copy)]
pub struct ZoroCurve {
    pub beta: U256,
    pub c: U256,
}

impl ZoroCurve {
    /// Number of decimal places used for internal precision.
    pub const DECIMALS: u128 = 18;

    /// Mantissa for scaling calculations (10^18).
    pub const MANTISSA: u128 = 1_000_000_000_000_000_000;

    pub fn new(beta: U256, c: U256) -> Self {
        Self { beta, c }
    }

    /// Calculates the psi value for given parameters.
    pub fn psi(&self, b: U256, l: U256, decimals: U256) -> U256 {
        let i_b = self.convert_to_internal_decimals(b, decimals);
        let i_l = self.convert_to_internal_decimals(l, decimals);
        if i_b == U256::ZERO && i_l == U256::ZERO {
            U256::ZERO
        } else {
            let diff = if i_b > i_l { i_b - i_l } else { i_l - i_b };
            let diff_squared = mul_u(diff, diff);
            let psi = div(mul_u(self.beta, diff_squared), i_b + mul_u(self.c, i_l)) + i_b;
            self.convert_to_external_decimals(psi, decimals)
        }
    }

    /// Calculates the inverse diagonal value.
    pub fn inverse_diagonal(&self, b: U256, l: U256, capital_b: U256, decimals: U256) -> U256 {
        let i_b = self.convert_to_internal_decimals(b, decimals);
        let i_l = self.convert_to_internal_decimals(l, decimals);
        let i_capital_b = self.convert_to_internal_decimals(capital_b, decimals);

        let quadratic_a = U256::from(Self::MANTISSA) + self.c;

        let quadratic_b = i_b + mul_u(self.c, i_l) - mul_u(i_capital_b - i_b, quadratic_a);
        let max_i256 =
            U256::from_str(&I256::MAX.to_string()).expect("parsing I256::MAX: must work");
        let quadratic_b = if quadratic_b > max_i256 {
            max_i256
        } else {
            quadratic_b
        };

        let factor = mul_u(i_b - i_l, i_b - i_l);

        let quadratic_c = u256_to_i256(mul_u(self.beta, factor))
            - u256_to_i256(mul_u(i_capital_b - i_b, i_b + mul_u(self.c, i_l)));

        let t = self.solve_quadratic(
            u256_to_i256(quadratic_a),
            u256_to_i256(quadratic_b),
            quadratic_c,
        );
        self.convert_to_external_decimals(t, decimals)
    }

    /// Calculates the inverse horizontal value.
    pub fn inverse_horizontal(&self, b: I256, l: I256, capital_b: I256, decimals: U256) -> U256 {
        let i_b = I256::from_str(
            &self
                .convert_to_internal_decimals(
                    U256::from_str(&b.to_string())
                        .unwrap_or_else(|err| panic!("Failed to parse b: {err:?}")),
                    decimals,
                )
                .to_string(),
        )
        .unwrap_or_else(|err| panic!("Failed to parse i_b: {err:?}"));
        let i_l = I256::from_str(
            &self
                .convert_to_internal_decimals(
                    U256::from_str(&l.to_string())
                        .unwrap_or_else(|err| panic!("Failed to parse l: {err:?}")),
                    decimals,
                )
                .to_string(),
        )
        .unwrap_or_else(|err| panic!("Failed to parse i_l: {err:?}"));
        let i_capital_b = I256::from_str(
            &self
                .convert_to_internal_decimals(
                    U256::from_str(&capital_b.to_string())
                        .unwrap_or_else(|err| panic!("Failed to parse capital_b: {err:?}")),
                    decimals,
                )
                .to_string(),
        )
        .unwrap_or_else(|err| panic!("Failed to parse i_capital_b: {err:?}"));

        let quadratic_a = U256::from(Self::MANTISSA) + self.beta;
        let quadratic_b = mul_i(
            I256::from_str("2").expect("Parsing '2' must work")
                * I256::from_str(&self.beta.to_string())
                    .unwrap_or_else(|err| panic!("Failed to parse beta: {err:?}")),
            i_b - i_l,
        ) - i_capital_b
            + (I256::from_str("2").expect("Parsing '2' must work") * i_b)
            + mul_i(
                I256::from_str(&self.c.to_string())
                    .unwrap_or_else(|err| panic!("Failed to parse c: {err:?}")),
                i_l,
            );

        let factor = mul_i(i_b - i_l, i_b - i_l);
        let quadratic_c = mul_i(
            I256::from_str(&self.beta.to_string())
                .unwrap_or_else(|err| panic!("Failed to parse beta: {err:?}")),
            factor,
        ) - (mul_i(
            i_capital_b - i_b,
            i_b + mul_i(
                I256::from_str(&self.c.to_string())
                    .unwrap_or_else(|err| panic!("Failed to parse c: {err:?}")),
                i_l,
            ),
        ));

        let t: U256 = self.solve_quadratic(u256_to_i256(quadratic_a), quadratic_b, quadratic_c);
        self.convert_to_external_decimals(t, decimals)
    }

    /// Converts a value between different decimal precisions.
    fn convert_to_external_decimals(&self, value: U256, decimals: U256) -> U256 {
        if decimals > U256::from(Self::DECIMALS) {
            value * U256::from(10).pow(decimals - U256::from(Self::DECIMALS))
        } else {
            value / U256::from(10).pow(U256::from(Self::DECIMALS) - decimals)
        }
    }

    fn convert_to_internal_decimals(&self, value: U256, decimals: U256) -> U256 {
        if decimals > U256::from(Self::DECIMALS) {
            value / U256::from(10).pow(decimals - U256::from(Self::DECIMALS))
        } else {
            value * U256::from(10).pow(U256::from(Self::DECIMALS) - decimals)
        }
    }

    /// Solves a quadratic equation, returning the positive solution to ax² + bx + c = 0.
    fn solve_quadratic(&self, a: I256, b: I256, c: I256) -> U256 {
        let signed_discriminant =
            mul_i(b, b) - (mul_i(I256::from_str("4").expect("Parsing '4' must work") * a, c));
        let discriminant = if signed_discriminant < I256::ZERO {
            U256::ZERO
        } else {
            signed_discriminant.into_raw()
        };
        let sqrt_discriminant = u256_to_i256(self.sqrt(discriminant));

        let almost_solution = div_i(
            sqrt_discriminant - b,
            I256::from_str("2").expect("Parsing '2' must work") * a,
        );
        if almost_solution < I256::ZERO {
            U256::ZERO
        } else {
            almost_solution.into_raw()
        }
    }

    /// Calculates the square root using the Newton-Raphson method.
    fn sqrt(&self, a: U256) -> U256 {
        let scaled_a = a * U256::from(Self::MANTISSA);
        if scaled_a == U256::ZERO {
            return U256::ZERO;
        }

        let mut result = U256::from(1) << (self.log2(scaled_a) >> U256::from(1));

        // Newton-Raphson iterations (7 times)
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        result = (result + scaled_a / result) >> 1;
        min(result, scaled_a / result)
    }

    /// Calculates the base-2 logarithm of a value.
    fn log2(&self, mut value: U256) -> U256 {
        let mut result = U256::ZERO;

        if value >> 128 > U256::ZERO {
            value >>= 128;
            result += U256::from(128);
        }
        if value >> 64 > U256::ZERO {
            value >>= 64;
            result += U256::from(64);
        }
        if value >> 32 > U256::ZERO {
            value >>= 32;
            result += U256::from(32);
        }
        if value >> 16 > U256::ZERO {
            value >>= 16;
            result += U256::from(16);
        }
        if value >> 8 > U256::ZERO {
            value >>= 8;
            result += U256::from(8);
        }
        if value >> 4 > U256::ZERO {
            value >>= 4;
            result += U256::from(4);
        }
        if value >> 2 > U256::ZERO {
            value >>= 2;
            result += U256::from(2);
        }
        if value >> 1 > U256::ZERO {
            result += U256::from(1);
        }
        result
    }
}

/// Constants for fee calculations
const FEE_PRECISION: U256 = U256::from_limbs([1_000_000, 0, 0, 0]); // 10^6
pub const PRICE_SCALING_FACTOR: i128 = 1e12 as i128;

/// Calculates the amount out for a swap.
///
/// Implements the protocol's swap calculation logic, taking into account pool
/// imbalances, fees, and slippage.
///
/// # Returns
/// `(amount_out, new_base_pool_balances, new_quote_pool_balances)`
///
/// # Errors
/// Returns an error if the amount out exceeds the reserve.
pub fn get_curve_amount_out(
    base_pool: &PoolState,
    quote_pool: &PoolState,
    asset_decimals_in: U256,
    asset_decimals_out: U256,
    amount_in: U256,
    price: U256,
) -> Result<(U256, PoolBalances, PoolBalances)> {
    let price_scaling_factor = U256::from(PRICE_SCALING_FACTOR);
    let fee = quote_pool.settings().backstop_fee + quote_pool.settings().protocol_fee;
    let lp_fee = quote_pool.settings().swap_fee;
    // Initialize curves by direction
    let curve_in = ZoroCurve::new(
        base_pool.settings().beta.into_raw(),
        base_pool.settings().c.into_raw(),
    );
    let curve_out = ZoroCurve::new(
        quote_pool.settings().beta.into_raw(),
        quote_pool.settings().c.into_raw(),
    );

    // COMPUTE
    // ADJUST FOR IN TOKEN POOL IMBALANCE
    debug!(
        base_pool_reserve = %base_pool.balances().reserve,
        base_pool_total_liabilities = %base_pool.balances().total_liabilities,
        base_pool_reserve_with_slippage = %base_pool.balances().reserve_with_slippage,
        amount_in = %amount_in,
        "Curve swap input pool state"
    );
    let effective_amount_in = curve_in.inverse_horizontal(
        I256::from_str(&base_pool.balances().reserve.to_string())
            .unwrap_or_else(|err| panic!("Failed to parse base_pool.reserve: {err:?}")),
        I256::from_str(&base_pool.balances().total_liabilities.to_string())
            .unwrap_or_else(|err| panic!("Failed to parse base_pool.total_liabilities: {err:?}")),
        I256::from_str(&(base_pool.balances().reserve_with_slippage + amount_in).to_string())
            .unwrap_or_else(|err| {
                panic!("Failed to parse reserve_with_slippage + amount_in: {err:?}")
            }),
        asset_decimals_in,
    );

    debug!(
        base_pool_reserve = %base_pool.balances().reserve,
        effective_amount_in = %effective_amount_in,
        reserve_plus_effective = %(base_pool.balances().reserve + effective_amount_in),
        total_liabilities = %base_pool.balances().total_liabilities,
        "Curve swap effective amount calculation"
    );
    if (base_pool.balances().reserve + effective_amount_in)
        > (U256::from(2) * base_pool.balances().total_liabilities)
    {
        return Ok((U256::ZERO, *base_pool.balances(), *quote_pool.balances()));
    }

    // AMOUNT OUT BEFORE FEES AND OUT TOKEN POOL IMBALANCE
    let scaling_factor = if asset_decimals_in > asset_decimals_out {
        price_scaling_factor * U256::from(10).pow(asset_decimals_in - asset_decimals_out)
    } else {
        price_scaling_factor / U256::from(10).pow(asset_decimals_out - asset_decimals_in)
    };

    debug!(
        effective_amount_in = %effective_amount_in,
        price = %price,
        scaling_factor = %scaling_factor,
        "Curve swap scaling"
    );
    let raw_amount_out = effective_amount_in * price / scaling_factor;

    // COMPUTE FEES
    let fee_amount = raw_amount_out * fee / FEE_PRECISION;
    let max_lp_fee = raw_amount_out * lp_fee / FEE_PRECISION;

    // ADJUST FOR OUT TOKEN POOL IMBALANCE

    // COMPUTE ACTUAL LP FEE
    let reduced_reserve_out = quote_pool.balances().reserve - raw_amount_out + fee_amount;

    debug!(
        reduced_reserve_out = %reduced_reserve_out,
        quote_pool_total_liabilities = %quote_pool.balances().total_liabilities,
        quote_pool_reserve_with_slippage = %quote_pool.balances().reserve_with_slippage,
        "Curve swap output pool state"
    );

    let mut actual_lp_fee_amount = curve_out.inverse_diagonal(
        reduced_reserve_out,
        quote_pool.balances().total_liabilities,
        quote_pool.balances().reserve_with_slippage,
        asset_decimals_out,
    );

    actual_lp_fee_amount = actual_lp_fee_amount.min(max_lp_fee);

    // COMPUTE ACTUAL REDUCED RESERVE AND TOTAL LIABILITIES
    let actual_reduced_reserve_out = reduced_reserve_out + actual_lp_fee_amount;
    let actual_total_liabilities_out =
        quote_pool.balances().total_liabilities + actual_lp_fee_amount;

    // COMPUTE EFFECTIVE RESERVE WITH SLIPPAGE AFTER AMOUNT OUT
    let mut reserve_with_slippage_after_amount_out = curve_out.psi(
        actual_reduced_reserve_out,
        actual_total_liabilities_out,
        asset_decimals_out,
    );

    // COMPUTE ACTUAL AMOUNT OUT
    reserve_with_slippage_after_amount_out =
        reserve_with_slippage_after_amount_out.min(quote_pool.balances().reserve_with_slippage);

    if reserve_with_slippage_after_amount_out <= U256::ZERO {
        return Err(anyhow!("Amount out exceeds reserve"));
    }

    let amount_out =
        quote_pool.balances().reserve_with_slippage - reserve_with_slippage_after_amount_out;

    debug!(
        effective_amount_in = %effective_amount_in,
        raw_amount_out = %raw_amount_out,
        reserve_with_slippage_out = %quote_pool.balances().reserve_with_slippage,
        reserve_with_slippage_after_amount_out = %reserve_with_slippage_after_amount_out,
        amount_out = %amount_out,
        "Curve swap calculation"
    );

    let new_pool_balances_base = PoolBalances {
        reserve: base_pool.balances().reserve + effective_amount_in,
        reserve_with_slippage: base_pool.balances().reserve_with_slippage + amount_in,
        total_liabilities: base_pool.balances().total_liabilities,
    };

    let new_pool_balances_quote = PoolBalances {
        reserve: actual_reduced_reserve_out,
        reserve_with_slippage: reserve_with_slippage_after_amount_out,
        total_liabilities: actual_total_liabilities_out,
    };

    Ok((amount_out, new_pool_balances_base, new_pool_balances_quote))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::{PoolMetadata, PoolSettings};

    fn parse_ether(s: &str) -> U256 {
        U256::from_str(s).unwrap() * U256::from(10).pow(U256::from(18))
    }

    #[test]
    fn test_mul_div_functions() {
        let a = U256::from(1000);
        let b = U256::from(2000);

        let mul_result = mul_u(a, b);
        let div_result = div(a, b);

        assert!(mul_result < a * b);
        assert!(div_result > a / b);
    }

    #[test]
    fn test_zoro_curve_creation() {
        let curve = ZoroCurve::new(U256::from(1000), U256::from(2000));
        assert_eq!(curve.beta, U256::from(1000));
        assert_eq!(curve.c, U256::from(2000));
    }

    #[test]
    fn test_psi_calculation() {
        let curve = ZoroCurve::new(U256::from(1000), U256::from(2000));
        let result = curve.psi(U256::from(1000), U256::from(500), U256::from(18));
        assert!(result >= U256::ZERO);
    }

    #[test]
    fn test_inverse_diagonal() {
        let curve = ZoroCurve::new(U256::from(1000), U256::from(2000));
        let result = curve.inverse_diagonal(
            U256::from(1000),
            U256::from(500),
            U256::from(1500),
            U256::from(18),
        );
        assert!(result >= U256::ZERO);
    }

    #[test]
    fn test_inverse_horizontal() {
        let curve = ZoroCurve::new(U256::from(1000), U256::from(2000));
        let result = curve.inverse_horizontal(
            I256::from_str("1000").unwrap(),
            I256::from_str("500").unwrap(),
            I256::from_str("1500").unwrap(),
            U256::from(18),
        );
        assert!(result >= U256::ZERO);
    }

    #[test]
    fn test_curve_constants() {
        assert_eq!(ZoroCurve::DECIMALS, 18);
        assert_eq!(ZoroCurve::MANTISSA, 1_000_000_000_000_000_000);
    }

    #[test]
    fn test_zero_inputs() {
        let curve = ZoroCurve::new(U256::from(1000), U256::from(2000));
        let psi_result = curve.psi(U256::ZERO, U256::ZERO, U256::from(18));
        assert_eq!(psi_result, U256::ZERO);
    }

    #[test]
    fn test_get_curve_amount_out_basic() {
        let base_pool = PoolState::new(
            PoolSettings {
                beta: I256::from_str("5000000000000000").unwrap(),
                c: I256::from_str("17075887234393789126").unwrap(),
                swap_fee: U256::from(200),
                backstop_fee: U256::from(300),
                protocol_fee: U256::from(300),
            },
            PoolBalances {
                reserve: parse_ether("1000"),
                reserve_with_slippage: parse_ether("1000"),
                total_liabilities: parse_ether("1000"),
            },
            parse_ether("1000").saturating_to::<u64>(),
            PoolMetadata {
                name: "test",
                asset_decimals: 18,
            },
        );
        let quote_pool = base_pool;
        let result = get_curve_amount_out(
            &base_pool,
            &quote_pool,
            U256::from(18), // asset_decimals_in
            U256::from(18), // asset_decimals_out
            parse_ether("10"),
            U256::from(10).pow(U256::from(12)), // price = 1.0
        );

        assert!(result.is_ok());
        let amount_out = result.unwrap().0;
        // Reference value produced by the proprietary zoro-curve crate with these
        // exact pool settings (verified against ../zoro-curve directly).
        let expected_amount_out = U256::from(9993944727768167277u64);
        assert_eq!(amount_out, expected_amount_out);
    }

    #[test]
    fn test_get_curve_amount_out_zero_input() {
        let base_pool = PoolState::new(
            PoolSettings {
                beta: I256::from_str("1000").unwrap(),
                c: I256::from_str("2000").unwrap(),
                swap_fee: U256::from(200),
                backstop_fee: U256::from(300),
                protocol_fee: U256::from(300),
            },
            PoolBalances {
                reserve: U256::from(5_000_000_000_000_000_000u64),
                reserve_with_slippage: U256::from(9_000_000_000_000_000_000u64),
                total_liabilities: U256::from(5_000_000_000_000_000_000u64),
            },
            parse_ether("1000").saturating_to::<u64>(),
            PoolMetadata {
                name: "test",
                asset_decimals: 18,
            },
        );
        let quote_pool = base_pool;
        let result = get_curve_amount_out(
            &base_pool,
            &quote_pool,
            U256::from(18),
            U256::from(18),
            U256::ZERO, // 0 input
            U256::from(1_000_000_000_000_000_000u64),
        );

        assert!(result.is_ok());
        let amount_out = result.unwrap().0;
        assert_eq!(amount_out, U256::ZERO);
    }
}
