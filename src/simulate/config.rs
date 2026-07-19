use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Parser;
use serde::{Deserialize, Serialize};

const DEFAULT_TRADE_INTERVAL_SECS: u64 = 10;
const DEFAULT_JITTER: f64 = 0.2;
const DEFAULT_AUTH_WARMUP_GAP_MS: u64 = 3_500;
const DEFAULT_TRADE_AMOUNT: u64 = 1_000;
const DEFAULT_FUND_AMOUNT: u64 = 1_000_000;
const DEFAULT_SLIPPAGE_BPS: u16 = 500;
const DEFAULT_INTENT_TTL_SECS: u64 = 1_800;
const DEFAULT_SETUP_CONCURRENCY: usize = 8;
const DEFAULT_MAX_USERS_PER_POOL: usize = 16;
const DEFAULT_SUMMARY_INTERVAL_SECS: u64 = 30;
const DEFAULT_ORDER_TIMEOUT_SECS: u64 = 120;
const DEFAULT_GROW_INTERVAL_SECS: u64 = 60;
const DEFAULT_VAULT_CYCLE_INTERVAL_SECS: u64 = 180;
const DEFAULT_VAULT_CYCLE_AMOUNT: u64 = 10_000;

#[derive(Debug, Clone, Parser)]
#[command(
    name = "simulate_traders",
    about = "Run high-frequency synthetic traders against a minizeke staging deployment",
    long_about = "Creates and funds synthetic Miden wallets, then continuously submits \
                  oracle-priced swaps through the production HTTP API. All MAX traders are \
                  pre-staged; START are activated initially and the rest ramp in fixed stages. \
                  Runs until Ctrl+C.\n\nUsage:\n  cargo run --bin simulate_traders -- \
                  <START> <MAX>\n\nExample (20 traders at start, grow to 100):\n  \
                  MIDEN_NETWORK=testnet FAUCET_SERVICE_TOKEN=... \
                  cargo run --bin simulate_traders -- 20 100"
)]
pub struct Config {
    /// Traders to activate at start.
    #[arg(default_value_t = 20)]
    pub num_traders: usize,

    /// Traders to pre-stage (must be >= START).
    #[arg(default_value_t = 100)]
    pub max_traders: usize,

    /// Minizeke public API base URL.
    #[arg(
        env = "SIMULATE_API_URL",
        default_value = "http://127.0.0.1:7799",
        hide = true
    )]
    pub api_url: String,
    /// Faucet service base URL.
    #[arg(
        env = "SIMULATE_FAUCET_URL",
        default_value = "http://127.0.0.1:7800",
        hide = true
    )]
    pub faucet_url: String,
    /// Zeke oracle base URL.
    #[arg(
        env = "ORACLE_URL",
        default_value = "https://oracle.zoroswap.com",
        hide = true
    )]
    pub oracle_url: String,
    /// Scoped credential accepted by the staging faucet.
    #[arg(env = "FAUCET_SERVICE_TOKEN", hide_env_values = true, hide = true)]
    pub faucet_token: Option<String>,

    #[arg(skip = PathBuf::from("simulation_stores/traders.json"))]
    pub state_file: PathBuf,
    #[arg(skip = PathBuf::from("simulation_stores/keystore"))]
    pub keystore_dir: PathBuf,
    #[arg(skip = PathBuf::from("simulation_stores/simulate.store.sqlite3"))]
    pub store_path: PathBuf,

    #[arg(skip = DEFAULT_TRADE_INTERVAL_SECS)]
    pub trade_interval_secs: u64,
    #[arg(skip = DEFAULT_JITTER)]
    pub jitter: f64,
    #[arg(skip = DEFAULT_AUTH_WARMUP_GAP_MS)]
    pub auth_warmup_gap_ms: u64,
    #[arg(skip = DEFAULT_TRADE_AMOUNT)]
    pub trade_amount: u64,
    #[arg(skip = DEFAULT_FUND_AMOUNT)]
    pub fund_amount: u64,
    #[arg(skip = DEFAULT_SLIPPAGE_BPS)]
    pub slippage_bps: u16,
    #[arg(skip = DEFAULT_INTENT_TTL_SECS)]
    pub intent_ttl_secs: u64,
    #[arg(skip = DEFAULT_SETUP_CONCURRENCY)]
    pub setup_concurrency: usize,
    #[arg(skip = DEFAULT_MAX_USERS_PER_POOL)]
    pub max_users_per_pool: usize,
    #[arg(skip = DEFAULT_SUMMARY_INTERVAL_SECS)]
    pub summary_interval: u64,
    #[arg(skip = DEFAULT_ORDER_TIMEOUT_SECS)]
    pub order_timeout_secs: u64,
    #[arg(skip = DEFAULT_GROW_INTERVAL_SECS)]
    pub grow_interval_secs: u64,
    #[arg(skip = DEFAULT_VAULT_CYCLE_INTERVAL_SECS)]
    pub vault_cycle_interval_secs: u64,
    #[arg(skip = DEFAULT_VAULT_CYCLE_AMOUNT)]
    pub vault_cycle_amount: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.num_traders == 0 {
            bail!("START traders must be at least 1");
        }
        if self.max_traders < self.num_traders {
            bail!("MAX traders must be >= START traders");
        }
        if self.faucet_token.as_deref().is_none_or(str::is_empty) {
            bail!("FAUCET_SERVICE_TOKEN is required");
        }
        Ok(())
    }

    /// Grow live when the max cohort is larger than the starting cohort.
    pub fn should_grow(&self) -> bool {
        self.max_traders > self.num_traders
    }

    pub fn tier_assignments(&self) -> Vec<TraderTier> {
        vec![TraderTier::HighFrequency; self.max_traders]
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
        // All live traders are high-frequency; keep the match for persisted state.
        let _ = self;
        config.trade_interval_secs
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use super::{Config, TraderTier};

    #[test]
    fn all_traders_are_high_frequency() {
        let config = Config {
            num_traders: 10,
            max_traders: 100,
            api_url: "http://127.0.0.1:7799".into(),
            faucet_url: "http://127.0.0.1:7800".into(),
            oracle_url: "https://oracle.zoroswap.com".into(),
            faucet_token: Some("token".into()),
            state_file: "simulate_traders.state.json".into(),
            keystore_dir: "simulate_keystore".into(),
            store_path: "simulate.store.sqlite3".into(),
            trade_interval_secs: 20,
            jitter: 0.5,
            auth_warmup_gap_ms: 3_500,
            trade_amount: 1_000,
            fund_amount: 1_000_000,
            slippage_bps: 500,
            intent_ttl_secs: 1_800,
            setup_concurrency: 8,
            max_users_per_pool: 16,
            summary_interval: 30,
            order_timeout_secs: 120,
            grow_interval_secs: 5,
            vault_cycle_interval_secs: 180,
            vault_cycle_amount: 10_000,
        };
        let tiers = config.tier_assignments();
        assert_eq!(tiers.len(), 100);
        assert!(tiers.iter().all(|tier| *tier == TraderTier::HighFrequency));
        assert!(config.should_grow());
    }

    #[test]
    fn equal_start_and_max_disables_growth() {
        let config = Config {
            num_traders: 20,
            max_traders: 20,
            api_url: "http://127.0.0.1:7799".into(),
            faucet_url: "http://127.0.0.1:7800".into(),
            oracle_url: "https://oracle.zoroswap.com".into(),
            faucet_token: Some("token".into()),
            state_file: "simulate_traders.state.json".into(),
            keystore_dir: "simulate_keystore".into(),
            store_path: "simulate.store.sqlite3".into(),
            trade_interval_secs: 20,
            jitter: 0.5,
            auth_warmup_gap_ms: 3_500,
            trade_amount: 1_000,
            fund_amount: 1_000_000,
            slippage_bps: 500,
            intent_ttl_secs: 1_800,
            setup_concurrency: 8,
            max_users_per_pool: 16,
            summary_interval: 30,
            order_timeout_secs: 120,
            grow_interval_secs: 5,
            vault_cycle_interval_secs: 180,
            vault_cycle_amount: 10_000,
        };
        assert!(!config.should_grow());
    }

    #[test]
    fn command_line_uses_fixed_load_profile() {
        let config = Config::try_parse_from(["simulate_traders", "20", "100"]).unwrap();
        assert_eq!(config.trade_interval_secs, 10);
        assert_eq!(config.jitter, 0.2);
        assert_eq!(config.grow_interval_secs, 60);
        assert_eq!(
            config.state_file,
            PathBuf::from("simulation_stores/traders.json")
        );
    }
}
