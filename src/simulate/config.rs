use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{ArgAction, Parser};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "simulate_traders",
    about = "Run synthetic traders against a minizeke staging deployment",
    long_about = "Creates and funds synthetic Miden wallets, then continuously submits \
                  oracle-priced swaps through the production HTTP API. Runs until Ctrl+C unless \
                  --duration is set.\n\nSetup mints each asset once to a bank wallet (raising \
                  FAUCET_MINT_AMOUNT or waiting through FAUCET_MINT_COOLDOWN_SECS when \
                  num_traders * fund_amount exceeds one mint), then distributes via public P2ID \
                  notes before vault register/fund.\n\nStaging example:\n  MIDEN_NETWORK=testnet \
                  SIMULATE_API_URL=https://staging-api.example \
                  SIMULATE_FAUCET_URL=https://staging-faucet.example \
                  ORACLE_URL=https://oracle.zoroswap.com FAUCET_SERVICE_TOKEN=... \
                  cargo run --bin simulate_traders -- 50"
)]
pub struct Config {
    /// Number of concurrent traders. There is no hard maximum.
    #[arg(default_value_t = 10)]
    pub num_traders: usize,

    /// Relative share of low-frequency traders.
    #[arg(long, default_value_t = 1.0 / 3.0)]
    pub low_ratio: f64,
    /// Relative share of average-frequency traders.
    #[arg(long, default_value_t = 1.0 / 3.0)]
    pub avg_ratio: f64,
    /// Relative share of high-frequency traders.
    #[arg(long, default_value_t = 1.0 / 3.0)]
    pub hf_ratio: f64,

    /// Base interval for low-frequency traders, in seconds.
    #[arg(long, default_value_t = 300)]
    pub low_interval: u64,
    /// Base interval for average-frequency traders, in seconds.
    #[arg(long, default_value_t = 60)]
    pub avg_interval: u64,
    /// Base interval for high-frequency traders, in seconds.
    #[arg(long, default_value_t = 20)]
    pub hf_interval: u64,
    /// Uniform interval jitter as a fraction (0.2 means +/-20%).
    #[arg(long, default_value_t = 0.2)]
    pub jitter: f64,

    /// Input amount for every swap, in asset base units.
    #[arg(long, default_value_t = 1_000)]
    pub trade_amount: u64,
    /// Amount funded into the vault per trader and asset. Default fits 10 traders in one
    /// FAUCET_MINT_AMOUNT of 10_000_000.
    #[arg(long, default_value_t = 1_000_000)]
    pub fund_amount: u64,
    /// Oracle-price cushion applied to min_amount_out.
    #[arg(long, default_value_t = 500)]
    pub slippage_bps: u16,

    /// Minizeke public API base URL.
    #[arg(
        long,
        env = "SIMULATE_API_URL",
        default_value = "http://127.0.0.1:7799"
    )]
    pub api_url: String,
    /// Faucet service base URL.
    #[arg(
        long,
        env = "SIMULATE_FAUCET_URL",
        default_value = "http://127.0.0.1:7800"
    )]
    pub faucet_url: String,
    /// Zeke oracle base URL.
    #[arg(
        long,
        env = "ORACLE_URL",
        default_value = "https://oracle.zoroswap.com"
    )]
    pub oracle_url: String,
    /// Scoped credential accepted by the staging faucet.
    #[arg(long, env = "FAUCET_SERVICE_TOKEN", hide_env_values = true)]
    pub faucet_token: Option<String>,

    /// JSON file holding resumable trader identities.
    #[arg(long, default_value = "simulate_traders.state.json")]
    pub state_file: PathBuf,
    /// Isolated directory for trader signing keys.
    #[arg(long, default_value = "simulate_keystore")]
    pub keystore_dir: PathBuf,
    /// Isolated Miden client SQLite store.
    #[arg(long, default_value = "simulate.store.sqlite3")]
    pub store_path: PathBuf,

    /// Create, register, and fund traders, then exit.
    #[arg(long, conflicts_with = "skip_setup")]
    pub setup_only: bool,
    /// Load traders from --state-file without onboarding.
    #[arg(long)]
    pub skip_setup: bool,
    /// Stop after this many seconds. By default the simulator runs until Ctrl+C.
    #[arg(long)]
    pub duration: Option<u64>,
    /// Interval between aggregate metric summaries, in seconds.
    #[arg(long, default_value_t = 30)]
    pub summary_interval: u64,
    /// Increase logging verbosity (-v or -vv).
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.num_traders == 0 {
            bail!("NUM_TRADERS must be at least 1");
        }
        let ratios = [self.low_ratio, self.avg_ratio, self.hf_ratio];
        if ratios
            .iter()
            .any(|ratio| !ratio.is_finite() || *ratio < 0.0)
            || ratios.iter().sum::<f64>() <= 0.0
        {
            bail!("tier ratios must be finite, non-negative, and not all zero");
        }
        if self.jitter < 0.0 || self.jitter > 1.0 || !self.jitter.is_finite() {
            bail!("--jitter must be between 0 and 1");
        }
        if self.slippage_bps >= 10_000 {
            bail!("--slippage-bps must be less than 10000");
        }
        if self.trade_amount == 0 || self.fund_amount == 0 {
            bail!("trade and funding amounts must be non-zero");
        }
        if self.trade_amount > self.fund_amount {
            bail!("--trade-amount cannot exceed --fund-amount");
        }
        if [self.low_interval, self.avg_interval, self.hf_interval].contains(&0)
            || self.summary_interval == 0
            || self.duration == Some(0)
        {
            bail!("intervals and duration must be non-zero");
        }
        if !self.skip_setup && self.faucet_token.as_deref().is_none_or(str::is_empty) {
            bail!("--faucet-token or FAUCET_SERVICE_TOKEN is required during setup");
        }
        Ok(())
    }

    pub fn tier_assignments(&self) -> Vec<TraderTier> {
        assign_tiers(
            self.num_traders,
            [self.low_ratio, self.avg_ratio, self.hf_ratio],
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraderTier {
    Low,
    Average,
    HighFrequency,
}

impl TraderTier {
    pub fn label(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Average => "average",
            Self::HighFrequency => "high_frequency",
        }
    }

    pub fn interval_secs(self, config: &Config) -> u64 {
        match self {
            Self::Low => config.low_interval,
            Self::Average => config.avg_interval,
            Self::HighFrequency => config.hf_interval,
        }
    }
}

fn assign_tiers(count: usize, ratios: [f64; 3]) -> Vec<TraderTier> {
    let total = ratios.iter().sum::<f64>();
    let exact = ratios.map(|ratio| ratio / total * count as f64);
    let mut counts = exact.map(|value| value.floor() as usize);
    let assigned = counts.iter().sum::<usize>();
    let mut remainders = [
        (exact[0] - counts[0] as f64, 0),
        (exact[1] - counts[1] as f64, 1),
        (exact[2] - counts[2] as f64, 2),
    ];
    remainders.sort_by(|left, right| {
        right
            .0
            .total_cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
    });
    for (_, index) in remainders.into_iter().take(count - assigned) {
        counts[index] += 1;
    }

    let mut tiers = Vec::with_capacity(count);
    tiers.extend(std::iter::repeat_n(TraderTier::Low, counts[0]));
    tiers.extend(std::iter::repeat_n(TraderTier::Average, counts[1]));
    tiers.extend(std::iter::repeat_n(TraderTier::HighFrequency, counts[2]));
    tiers
}

#[cfg(test)]
mod tests {
    use super::{TraderTier, assign_tiers};

    #[test]
    fn largest_remainder_assigns_every_trader() {
        let tiers = assign_tiers(10, [1.0, 1.0, 1.0]);
        assert_eq!(tiers.len(), 10);
        assert_eq!(
            tiers
                .iter()
                .filter(|tier| **tier == TraderTier::HighFrequency)
                .count(),
            4
        );
    }

    #[test]
    fn zero_ratio_omits_a_tier() {
        let tiers = assign_tiers(7, [1.0, 0.0, 1.0]);
        assert!(!tiers.contains(&TraderTier::Average));
    }
}
