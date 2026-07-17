use std::{error::Error, fmt};

use super::config::FeeCurveConfig;

#[derive(Clone, Debug)]
pub struct FeeCurve {
    config: FeeCurveConfig,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FeeCurveError(pub f64);

impl fmt::Display for FeeCurveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "volatility must be finite and non-negative, got {}",
            self.0
        )
    }
}

impl Error for FeeCurveError {}

impl FeeCurve {
    pub fn new(config: FeeCurveConfig) -> Self {
        Self { config }
    }

    pub fn target_fee_bps(&self, sigma_pct_day: f64) -> Result<f64, FeeCurveError> {
        if !sigma_pct_day.is_finite() || sigma_pct_day < 0.0 {
            return Err(FeeCurveError(sigma_pct_day));
        }
        Ok((self.config.a + self.config.b * sigma_pct_day.ln_1p())
            .clamp(self.config.floor_bps, self.config.max_bps))
    }

    pub fn surge_fee_bps(&self, sigma_pct_day: f64) -> Result<f64, FeeCurveError> {
        Ok((self.target_fee_bps(sigma_pct_day)? - self.config.floor_bps).max(0.0))
    }

    pub fn config(&self) -> &FeeCurveConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn curve() -> FeeCurve {
        FeeCurve::new(FeeCurveConfig {
            a: -5.0,
            b: 12.4,
            floor_bps: 6.5,
            max_bps: 33.0,
        })
    }

    #[test]
    fn matches_reference_ladder() {
        for (sigma, expected) in [
            (2.0, 8.6),
            (4.0, 15.0),
            (6.0, 19.1),
            (10.0, 24.7),
            (15.0, 29.4),
        ] {
            let actual = curve().target_fee_bps(sigma).unwrap();
            assert!(
                (actual - expected).abs() <= 0.1,
                "sigma={sigma}: expected {expected}, got {actual}"
            );
        }
    }

    #[test]
    fn clamps_and_reports_surge_above_floor() {
        assert_eq!(curve().target_fee_bps(0.0).unwrap(), 6.5);
        assert_eq!(curve().surge_fee_bps(0.0).unwrap(), 0.0);
        assert_eq!(curve().target_fee_bps(100.0).unwrap(), 33.0);
        assert_eq!(curve().surge_fee_bps(100.0).unwrap(), 26.5);
    }

    #[test]
    fn rejects_invalid_volatility() {
        assert!(curve().target_fee_bps(f64::NAN).is_err());
        assert!(curve().target_fee_bps(f64::INFINITY).is_err());
        assert!(curve().target_fee_bps(-1.0).is_err());
    }
}
