use std::{
    collections::HashMap,
    env,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, anyhow, bail};
use miden_client::account::AccountId;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

use alloy_primitives::U256;

use crate::pool::{FeeSource, PoolState};

pub const DEFAULT_FEE_VALIDITY_SECS: u64 = 600;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeBatchRequest {
    pub batch_id: String,
    pub issued_at: u64,
    #[serde(default = "default_validity")]
    pub validity_secs: u64,
    pub source: FeeUpdateSource,
    pub trigger: Option<String>,
    /// Parts-per-million, matching PoolSettings fee precision.
    pub volatility_fee_in: u64,
    /// Parts-per-million, matching PoolSettings fee precision.
    pub volatility_fee_out: u64,
    pub sigma_pct_day: Option<f64>,
    pub target_fee_bps: Option<f64>,
}

pub fn apply_fee_states(
    pool_states: &mut HashMap<AccountId, PoolState>,
    states: &HashMap<AccountId, AssetFeeState>,
) {
    for (faucet_id, pool) in pool_states {
        let state = states.get(faucet_id).copied().unwrap_or_default();
        let mut settings = *pool.settings();
        settings.volatility_fee_in = U256::from(state.volatility_fee_in);
        settings.volatility_fee_out = U256::from(state.volatility_fee_out);
        settings.volatility_fee_valid_until = state.valid_until;
        settings.volatility_fee_version = state.version;
        settings.volatility_fee_source = state.source;
        pool.update_settings(settings);
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeeUpdateSource {
    Automatic,
    Manual,
}

impl From<FeeUpdateSource> for FeeSource {
    fn from(value: FeeUpdateSource) -> Self {
        match value {
            FeeUpdateSource::Automatic => FeeSource::Automatic,
            FeeUpdateSource::Manual => FeeSource::Manual,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct AssetFeeState {
    pub volatility_fee_in: u64,
    pub volatility_fee_out: u64,
    pub valid_until: u64,
    pub version: u64,
    pub source: FeeSource,
}

impl Default for AssetFeeState {
    fn default() -> Self {
        Self {
            volatility_fee_in: 0,
            volatility_fee_out: 0,
            valid_until: 0,
            version: 0,
            source: FeeSource::None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AppliedFeeBatch {
    pub batch_id: String,
    pub valid_until: u64,
    pub version: u64,
    pub assets_updated: usize,
    pub idempotent: bool,
}

pub struct FeeStore {
    connection: Mutex<Connection>,
}

impl FeeStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open fee sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS fee_batches (
                batch_id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                trigger TEXT,
                issued_at INTEGER NOT NULL,
                validity_secs INTEGER NOT NULL,
                valid_until INTEGER NOT NULL,
                fee_in INTEGER NOT NULL,
                fee_out INTEGER NOT NULL,
                sigma_pct_day REAL,
                target_fee_bps REAL,
                version INTEGER NOT NULL UNIQUE,
                status TEXT NOT NULL,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fee_assets (
                batch_id TEXT NOT NULL,
                faucet_id TEXT NOT NULL,
                fee_in INTEGER NOT NULL,
                fee_out INTEGER NOT NULL,
                PRIMARY KEY (batch_id, faucet_id),
                FOREIGN KEY (batch_id) REFERENCES fee_batches(batch_id)
            );
            CREATE INDEX IF NOT EXISTS idx_fee_batches_status
                ON fee_batches(status, version DESC);
            "#,
        )?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn open_from_env() -> Result<Self> {
        let path = env::var("FEE_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_string());
            format!("fees.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("fee database lock poisoned"))
    }

    pub fn apply_batch(
        &self,
        request: &FeeBatchRequest,
        assets: &[AccountId],
        now: u64,
    ) -> Result<AppliedFeeBatch> {
        if request.validity_secs == 0 {
            bail!("fee validity must be greater than zero");
        }
        if request.volatility_fee_in > u64::from(u16::MAX)
            || request.volatility_fee_out > u64::from(u16::MAX)
        {
            bail!("volatility fee exceeds the configured uint16 precision range");
        }
        if request.issued_at > now.saturating_add(60) {
            bail!("fee batch issued_at is in the future");
        }
        let valid_until = request
            .issued_at
            .checked_add(request.validity_secs)
            .ok_or_else(|| anyhow!("fee validity overflow"))?;
        if valid_until <= now {
            bail!("fee batch is already expired");
        }
        let mut connection = self.connection()?;
        connection.execute(
            "UPDATE fee_batches SET status = 'expired'
             WHERE status = 'active' AND valid_until <= ?1",
            [to_i64(now)?],
        )?;
        if let Some((version, existing_until)) = connection
            .query_row(
                "SELECT version, valid_until FROM fee_batches WHERE batch_id = ?1",
                [&request.batch_id],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?
        {
            return Ok(AppliedFeeBatch {
                batch_id: request.batch_id.clone(),
                valid_until: u64::try_from(existing_until)?,
                version: u64::try_from(version)?,
                assets_updated: assets.len(),
                idempotent: true,
            });
        }

        if let Some((issued_at, source)) = connection
            .query_row(
                "SELECT issued_at, source FROM fee_batches WHERE status = 'active'
                 ORDER BY version DESC LIMIT 1",
                [],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            if request.issued_at < u64::try_from(issued_at)? {
                bail!("stale fee batch");
            }
            if source == "manual" && request.source == FeeUpdateSource::Automatic {
                bail!("manual fee override is active");
            }
        }

        let version: u64 = connection
            .query_row(
                "SELECT COALESCE(MAX(version), 0) + 1 FROM fee_batches",
                [],
                |row| row.get::<_, i64>(0),
            )?
            .try_into()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "UPDATE fee_batches SET status = 'superseded' WHERE status = 'active'",
            [],
        )?;
        transaction.execute(
            r#"
            INSERT INTO fee_batches (
                batch_id, source, trigger, issued_at, validity_secs, valid_until,
                fee_in, fee_out, sigma_pct_day, target_fee_bps, version, status, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'active', ?12)
            "#,
            params![
                request.batch_id,
                source_str(request.source),
                request.trigger,
                to_i64(request.issued_at)?,
                to_i64(request.validity_secs)?,
                to_i64(valid_until)?,
                to_i64(request.volatility_fee_in)?,
                to_i64(request.volatility_fee_out)?,
                request.sigma_pct_day,
                request.target_fee_bps,
                to_i64(version)?,
                to_i64(now)?,
            ],
        )?;
        for faucet_id in assets {
            transaction.execute(
                "INSERT INTO fee_assets (batch_id, faucet_id, fee_in, fee_out)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    request.batch_id,
                    faucet_id.to_hex(),
                    to_i64(request.volatility_fee_in)?,
                    to_i64(request.volatility_fee_out)?,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(AppliedFeeBatch {
            batch_id: request.batch_id.clone(),
            valid_until,
            version,
            assets_updated: assets.len(),
            idempotent: false,
        })
    }

    pub fn active_states(&self, now: u64) -> Result<HashMap<AccountId, AssetFeeState>> {
        let connection = self.connection()?;
        let Some((batch_id, source, valid_until, version)) = connection
            .query_row(
                "SELECT batch_id, source, valid_until, version
                 FROM fee_batches WHERE status = 'active' AND valid_until > ?1
                 ORDER BY version DESC LIMIT 1",
                [to_i64(now)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
        else {
            return Ok(HashMap::new());
        };
        let mut statement = connection
            .prepare("SELECT faucet_id, fee_in, fee_out FROM fee_assets WHERE batch_id = ?1")?;
        let source = if source == "manual" {
            FeeSource::Manual
        } else {
            FeeSource::Automatic
        };
        let rows = statement.query_map([batch_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut states = HashMap::new();
        for row in rows {
            let (faucet_id, fee_in, fee_out) = row?;
            states.insert(
                AccountId::from_hex(&faucet_id)?,
                AssetFeeState {
                    volatility_fee_in: u64::try_from(fee_in)?,
                    volatility_fee_out: u64::try_from(fee_out)?,
                    valid_until: u64::try_from(valid_until)?,
                    version: u64::try_from(version)?,
                    source,
                },
            );
        }
        Ok(states)
    }

    pub fn expire(&self, now: u64) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE fee_batches SET status = 'expired'
             WHERE status = 'active' AND valid_until <= ?1",
            [to_i64(now)?],
        )? > 0)
    }

    pub fn clear_manual(&self, now: u64) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE fee_batches SET status = 'cleared', valid_until = MIN(valid_until, ?1)
             WHERE status = 'active' AND source = 'manual'",
            [to_i64(now)?],
        )? > 0)
    }
}

fn source_str(source: FeeUpdateSource) -> &'static str {
    match source {
        FeeUpdateSource::Automatic => "automatic",
        FeeUpdateSource::Manual => "manual",
    }
}

fn default_validity() -> u64 {
    env::var("FEE_VALIDITY_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FEE_VALIDITY_SECS)
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds sqlite INTEGER")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn account(hex: &str) -> AccountId {
        AccountId::from_hex(hex).unwrap()
    }

    #[test]
    fn applies_idempotently_and_expires() {
        let store = FeeStore::open(":memory:").unwrap();
        let assets = [account("0x57a179f33b726c315fcfd5e0ff3309")];
        let request = FeeBatchRequest {
            batch_id: "batch-1".into(),
            issued_at: 100,
            validity_secs: 600,
            source: FeeUpdateSource::Automatic,
            trigger: Some("deadband".into()),
            volatility_fee_in: 850,
            volatility_fee_out: 850,
            sigma_pct_day: Some(4.2),
            target_fee_bps: Some(15.0),
        };
        let applied = store.apply_batch(&request, &assets, 100).unwrap();
        assert!(!applied.idempotent);
        assert!(
            store
                .apply_batch(&request, &assets, 101)
                .unwrap()
                .idempotent
        );
        assert_eq!(store.active_states(101).unwrap().len(), 1);
        assert!(store.expire(700).unwrap());
        assert!(store.active_states(700).unwrap().is_empty());
    }

    #[test]
    fn manual_override_blocks_automatic_until_cleared() {
        let store = FeeStore::open(":memory:").unwrap();
        let assets = [account("0x57a179f33b726c315fcfd5e0ff3309")];
        let manual = FeeBatchRequest {
            batch_id: "manual".into(),
            issued_at: 100,
            validity_secs: 600,
            source: FeeUpdateSource::Manual,
            trigger: Some("operator".into()),
            volatility_fee_in: 900,
            volatility_fee_out: 1_000,
            sigma_pct_day: None,
            target_fee_bps: None,
        };
        store.apply_batch(&manual, &assets, 100).unwrap();
        let automatic = FeeBatchRequest {
            batch_id: "automatic".into(),
            issued_at: 101,
            validity_secs: 600,
            source: FeeUpdateSource::Automatic,
            trigger: Some("deadband".into()),
            volatility_fee_in: 500,
            volatility_fee_out: 500,
            sigma_pct_day: Some(1.0),
            target_fee_bps: Some(11.5),
        };
        assert!(store.apply_batch(&automatic, &assets, 101).is_err());
        assert!(store.clear_manual(102).unwrap());
        let applied = store.apply_batch(&automatic, &assets, 102).unwrap();
        assert_eq!(applied.version, 2);
        let state = store.active_states(102).unwrap()[&assets[0]];
        assert_eq!(state.volatility_fee_in, 500);
        assert_eq!(state.source, FeeSource::Automatic);
    }
}
