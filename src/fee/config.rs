use std::{env, fmt::Display, str::FromStr, time::Duration};

use anyhow::{Context, Result, bail};
use url::Url;

#[derive(Clone, Debug)]
pub struct Config {
    pub oracle: OracleConfig,
    pub volatility: VolatilityConfig,
    pub fee_curve: FeeCurveConfig,
    pub update_policy: UpdatePolicyConfig,
    pub initial_fee_bps: f64,
    pub validity_period: Duration,
    pub refresh_fraction: f64,
    pub http: HttpConfig,
}

#[derive(Clone, Debug)]
pub struct OracleConfig {
    pub base_url: Url,
    pub feed_id: String,
    pub reference_asset: String,
    pub request_timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct VolatilityConfig {
    pub sample_interval: Duration,
    pub half_life: Duration,
    pub warmup_samples: u64,
    pub max_sample_age: Duration,
}

#[derive(Clone, Debug)]
pub struct FeeCurveConfig {
    pub a: f64,
    pub b: f64,
    pub floor_bps: f64,
    pub max_bps: f64,
}

#[derive(Clone, Debug)]
pub struct UpdatePolicyConfig {
    pub interval: Duration,
    pub deadband_bps: f64,
    pub spike_ratio: f64,
    pub spike_window: Duration,
}

#[derive(Clone)]
pub struct HttpConfig {
    pub server_url: Url,
    pub batch_path: String,
    pub admin_token: Option<String>,
    pub request_timeout: Duration,
}

impl std::fmt::Debug for HttpConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HttpConfig")
            .field("server_url", &self.server_url)
            .field("batch_path", &self.batch_path)
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let oracle_url = required(&mut lookup, "ORACLE_URL")?;
        let feed_id = required(&mut lookup, "FEE_ORACLE_FEED_ID")?;
        let reference_asset = required(&mut lookup, "FEE_REFERENCE_ASSET")?;

        let config = Self {
            oracle: OracleConfig {
                base_url: oracle_url.parse().context("invalid ORACLE_URL")?,
                feed_id,
                reference_asset,
                request_timeout: seconds(&mut lookup, "FEE_ORACLE_TIMEOUT_SECS", 10)?,
            },
            volatility: VolatilityConfig {
                sample_interval: seconds(&mut lookup, "SAMPLE_INTERVAL_SECS", 30)?,
                half_life: seconds(&mut lookup, "EWMA_HALF_LIFE_SECS", 1_800)?,
                warmup_samples: value(&mut lookup, "VOLATILITY_WARMUP_SAMPLES", 20_u64)?,
                max_sample_age: seconds(&mut lookup, "MAX_SAMPLE_AGE_SECS", 90)?,
            },
            fee_curve: FeeCurveConfig {
                a: value(&mut lookup, "CURVE_A", -5.0_f64)?,
                b: value(&mut lookup, "CURVE_B", 12.4_f64)?,
                floor_bps: value(&mut lookup, "MIN_FEE_BPS", 6.5_f64)?,
                max_bps: value(&mut lookup, "MAX_FEE_BPS", 33.0_f64)?,
            },
            update_policy: UpdatePolicyConfig {
                interval: seconds(&mut lookup, "UPDATE_INTERVAL_SECS", 60)?,
                deadband_bps: value(&mut lookup, "DEADBAND_BPS", 1.5_f64)?,
                spike_ratio: value(&mut lookup, "SPIKE_RATIO", 1.5_f64)?,
                spike_window: seconds(&mut lookup, "SPIKE_WINDOW_SECS", 600)?,
            },
            initial_fee_bps: value(&mut lookup, "INITIAL_FEE_BPS", 6.5_f64)?,
            // Minizeke's short-lived server policy intentionally differs from
            // fee_updater's one-hour on-chain default.
            validity_period: seconds(&mut lookup, "VALIDITY_PERIOD_SECS", 600)?,
            refresh_fraction: value(&mut lookup, "REFRESH_FRACTION", 0.8_f64)?,
            http: HttpConfig {
                server_url: value(
                    &mut lookup,
                    "FEE_SERVER_URL",
                    "http://127.0.0.1:3000".to_owned(),
                )?
                .parse()
                .context("invalid FEE_SERVER_URL")?,
                batch_path: value(
                    &mut lookup,
                    "FEE_BATCH_PATH",
                    "/internal/fees/batch".to_owned(),
                )?,
                admin_token: lookup("FEE_UPDATER_TOKEN").filter(|token| !token.trim().is_empty()),
                request_timeout: seconds(&mut lookup, "FEE_HTTP_TIMEOUT_SECS", 10)?,
            },
        };
        config.validate()?;
        Ok(config)
    }

    pub fn refresh_after(&self) -> Duration {
        self.validity_period.mul_f64(self.refresh_fraction)
    }

    fn validate(&self) -> Result<()> {
        for (name, text) in [
            ("FEE_ORACLE_FEED_ID", self.oracle.feed_id.as_str()),
            ("FEE_REFERENCE_ASSET", self.oracle.reference_asset.as_str()),
            ("FEE_BATCH_PATH", self.http.batch_path.as_str()),
        ] {
            if text.trim().is_empty() {
                bail!("{name} must not be empty");
            }
        }
        if !self.http.batch_path.starts_with('/') {
            bail!("FEE_BATCH_PATH must start with '/'");
        }
        if [
            self.oracle.request_timeout,
            self.volatility.sample_interval,
            self.volatility.half_life,
            self.volatility.max_sample_age,
            self.update_policy.interval,
            self.update_policy.spike_window,
            self.validity_period,
            self.http.request_timeout,
        ]
        .iter()
        .any(Duration::is_zero)
        {
            bail!("all configured durations must be greater than zero");
        }
        if self.volatility.warmup_samples == 0 {
            bail!("VOLATILITY_WARMUP_SAMPLES must be greater than zero");
        }
        for (name, value) in [
            ("CURVE_A", self.fee_curve.a),
            ("CURVE_B", self.fee_curve.b),
            ("MIN_FEE_BPS", self.fee_curve.floor_bps),
            ("MAX_FEE_BPS", self.fee_curve.max_bps),
            ("DEADBAND_BPS", self.update_policy.deadband_bps),
            ("SPIKE_RATIO", self.update_policy.spike_ratio),
            ("INITIAL_FEE_BPS", self.initial_fee_bps),
            ("REFRESH_FRACTION", self.refresh_fraction),
        ] {
            if !value.is_finite() {
                bail!("{name} must be finite");
            }
        }
        if self.fee_curve.b < 0.0 {
            bail!("CURVE_B must be non-negative");
        }
        if self.fee_curve.floor_bps < 0.0 || self.fee_curve.max_bps < self.fee_curve.floor_bps {
            bail!("fee bounds must satisfy 0 <= MIN_FEE_BPS <= MAX_FEE_BPS");
        }
        if self.update_policy.deadband_bps < 0.0 {
            bail!("DEADBAND_BPS must be non-negative");
        }
        if self.update_policy.spike_ratio <= 1.0 {
            bail!("SPIKE_RATIO must be greater than 1");
        }
        if !(self.fee_curve.floor_bps..=self.fee_curve.max_bps).contains(&self.initial_fee_bps) {
            bail!("INITIAL_FEE_BPS must be within the configured fee bounds");
        }
        if !(0.0..1.0).contains(&self.refresh_fraction) {
            bail!("REFRESH_FRACTION must satisfy 0 < value < 1");
        }
        if self.fee_curve.max_bps * super::FEE_UNITS_PER_BPS > u16::MAX as f64 {
            bail!("MAX_FEE_BPS exceeds the uint16 Minizeke fee scale");
        }
        Ok(())
    }
}

fn required(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str) -> Result<String> {
    lookup(name)
        .filter(|text| !text.trim().is_empty())
        .with_context(|| format!("{name} is required"))
}

fn seconds(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
    default: u64,
) -> Result<Duration> {
    Ok(Duration::from_secs(value(lookup, name, default)?))
}

fn value<T>(lookup: &mut impl FnMut(&str) -> Option<String>, name: &str, default: T) -> Result<T>
where
    T: FromStr,
    T::Err: Display + Send + Sync + 'static,
{
    lookup(name).map_or(Ok(default), |text| {
        text.parse()
            .map_err(|error| anyhow::anyhow!("{error}"))
            .with_context(|| format!("invalid value for {name}"))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn config_with(extra: &[(&str, &str)]) -> Result<Config> {
        let mut values = HashMap::from([
            ("ORACLE_URL".to_owned(), "https://oracle.example".to_owned()),
            ("FEE_ORACLE_FEED_ID".to_owned(), "feed-1".to_owned()),
            ("FEE_REFERENCE_ASSET".to_owned(), "BTC".to_owned()),
        ]);
        values.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
        );
        Config::from_lookup(|name| values.get(name).cloned())
    }

    #[test]
    fn preserves_policy_defaults_with_shorter_validity() -> Result<()> {
        let config = config_with(&[])?;
        assert_eq!(config.volatility.sample_interval, Duration::from_secs(30));
        assert_eq!(config.volatility.half_life, Duration::from_secs(1_800));
        assert_eq!(config.volatility.warmup_samples, 20);
        assert_eq!(config.volatility.max_sample_age, Duration::from_secs(90));
        assert_eq!(config.update_policy.interval, Duration::from_secs(60));
        assert_eq!(config.update_policy.deadband_bps, 1.5);
        assert_eq!(config.update_policy.spike_ratio, 1.5);
        assert_eq!(config.update_policy.spike_window, Duration::from_secs(600));
        assert_eq!(config.validity_period, Duration::from_secs(600));
        assert_eq!(config.refresh_fraction, 0.8);
        assert_eq!(config.refresh_after(), Duration::from_secs(480));
        Ok(())
    }

    #[test]
    fn reads_overrides_and_redacts_token() -> Result<()> {
        let config = config_with(&[
            ("VALIDITY_PERIOD_SECS", "120"),
            ("REFRESH_FRACTION", "0.5"),
            ("FEE_UPDATER_TOKEN", "secret"),
        ])?;
        assert_eq!(config.refresh_after(), Duration::from_secs(60));
        let debug = format!("{:?}", config.http);
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("secret"));
        Ok(())
    }

    #[test]
    fn rejects_missing_or_invalid_required_values() {
        assert!(Config::from_lookup(|_| None).is_err());
        assert!(config_with(&[("REFRESH_FRACTION", "1")]).is_err());
        assert!(config_with(&[("SPIKE_RATIO", "1")]).is_err());
        assert!(config_with(&[("FEE_BATCH_PATH", "admin/fees")]).is_err());
    }
}
