use std::{
    env,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, anyhow};
use miden_client::account::AccountId;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LpOperationKind {
    Deposit,
    Withdraw,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct NoteCursor {
    pub block_num: u32,
    pub note_id: String,
}

impl LpOperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deposit => "deposit",
            Self::Withdraw => "withdraw",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LpOperation {
    pub note_id: String,
    pub kind: String,
    pub lp_id: String,
    pub faucet_id: String,
    pub asset_amount: u64,
    pub lp_shares: u64,
    pub block_num: u32,
    pub status: String,
    pub created_at: u64,
    pub applied_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LpPosition {
    pub lp_id: String,
    pub faucet_id: String,
    pub shares: u64,
    pub checkpoint_shares: u64,
    pub checkpoint_value: u64,
    pub checkpoint_withdrawn: u64,
    pub updated_at: u64,
}

/// Durable LP journal. A consumed Miden note is recorded first and its accounting delta is
/// applied in a second, idempotent transaction after the processing engine accepts it.
pub struct LpStore {
    connection: Mutex<Connection>,
}

impl LpStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open LP sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS lp_operations (
                note_id TEXT PRIMARY KEY,
                nullifier TEXT UNIQUE,
                kind TEXT NOT NULL CHECK(kind IN ('deposit', 'withdraw')),
                lp_id TEXT NOT NULL,
                faucet_id TEXT NOT NULL,
                asset_amount INTEGER NOT NULL,
                lp_shares INTEGER NOT NULL DEFAULT 0,
                block_num INTEGER NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('confirmed', 'applied', 'failed')),
                failure_reason TEXT,
                created_at INTEGER NOT NULL,
                applied_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_lp_operations_status_block
                ON lp_operations(status, block_num, created_at);

            CREATE TABLE IF NOT EXISTS lp_positions (
                lp_id TEXT NOT NULL,
                faucet_id TEXT NOT NULL,
                shares INTEGER NOT NULL,
                checkpoint_shares INTEGER NOT NULL DEFAULT 0,
                checkpoint_value INTEGER NOT NULL DEFAULT 0,
                checkpoint_withdrawn INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(lp_id, faucet_id)
            );

            CREATE TABLE IF NOT EXISTS lp_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn open_from_env() -> Result<Self> {
        let path = env::var("LP_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_string());
            format!("lp.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("LP database lock poisoned"))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_confirmed(
        &self,
        note_id: &str,
        nullifier: Option<&str>,
        kind: LpOperationKind,
        lp_id: AccountId,
        faucet_id: AccountId,
        asset_amount: u64,
        block_num: u32,
        created_at: u64,
    ) -> Result<bool> {
        let changed = self.connection()?.execute(
            r#"
            INSERT OR IGNORE INTO lp_operations (
                note_id, nullifier, kind, lp_id, faucet_id, asset_amount,
                block_num, status, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'confirmed', ?8)
            "#,
            params![
                note_id,
                nullifier,
                kind.as_str(),
                lp_id.to_hex(),
                faucet_id.to_hex(),
                to_sql_u64(asset_amount)?,
                block_num,
                to_sql_u64(created_at)?,
            ],
        )?;
        Ok(changed == 1)
    }

    pub fn confirmed_operations(&self) -> Result<Vec<LpOperation>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT note_id, kind, lp_id, faucet_id, asset_amount, lp_shares,
                   block_num, status, created_at, applied_at
            FROM lp_operations
            WHERE status = 'confirmed'
            ORDER BY block_num ASC, created_at ASC
            "#,
        )?;
        let rows = statement.query_map([], operation_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn applied_operations(&self) -> Result<Vec<LpOperation>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT note_id, kind, lp_id, faucet_id, asset_amount, lp_shares,
                   block_num, status, created_at, applied_at
            FROM lp_operations
            WHERE status = 'applied'
            ORDER BY block_num ASC, created_at ASC
            "#,
        )?;
        let rows = statement.query_map([], operation_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn operation(&self, note_id: &str) -> Result<Option<LpOperation>> {
        self.connection()?
            .query_row(
                r#"
                SELECT note_id, kind, lp_id, faucet_id, asset_amount, lp_shares,
                       block_num, status, created_at, applied_at
                FROM lp_operations WHERE note_id = ?1
                "#,
                [note_id],
                operation_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn apply_operation(&self, note_id: &str, lp_shares: u64, applied_at: u64) -> Result<bool> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let operation = transaction
            .query_row(
                "SELECT kind, lp_id, faucet_id, status FROM lp_operations WHERE note_id = ?1",
                [note_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| anyhow!("unknown LP operation {note_id}"))?;
        if operation.3 == "applied" {
            return Ok(false);
        }
        if operation.3 != "confirmed" {
            return Err(anyhow!("LP operation {note_id} is {}", operation.3));
        }

        let current_shares: u64 = transaction
            .query_row(
                "SELECT shares FROM lp_positions WHERE lp_id = ?1 AND faucet_id = ?2",
                params![operation.1, operation.2],
                |row| from_sql_u64(row.get(0)?),
            )
            .optional()?
            .unwrap_or(0);
        let new_shares = match operation.0.as_str() {
            "deposit" => current_shares
                .checked_add(lp_shares)
                .ok_or_else(|| anyhow!("LP share balance overflow"))?,
            "withdraw" => current_shares
                .checked_sub(lp_shares)
                .ok_or_else(|| anyhow!("withdraw burns more LP shares than owned"))?,
            kind => return Err(anyhow!("unknown LP operation kind {kind}")),
        };
        transaction.execute(
            r#"
            INSERT INTO lp_positions (lp_id, faucet_id, shares, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(lp_id, faucet_id) DO UPDATE SET
                shares = excluded.shares,
                updated_at = excluded.updated_at
            "#,
            params![
                operation.1,
                operation.2,
                to_sql_u64(new_shares)?,
                to_sql_u64(applied_at)?,
            ],
        )?;
        transaction.execute(
            r#"
            UPDATE lp_operations
            SET lp_shares = ?2, status = 'applied', applied_at = ?3
            WHERE note_id = ?1
            "#,
            params![note_id, to_sql_u64(lp_shares)?, to_sql_u64(applied_at)?,],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn mark_failed(&self, note_id: &str, reason: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE lp_operations SET status = 'failed', failure_reason = ?2 WHERE note_id = ?1",
            params![note_id, reason],
        )?;
        Ok(())
    }

    pub fn seed_position(
        &self,
        lp_id: AccountId,
        faucet_id: AccountId,
        shares: u64,
        updated_at: u64,
    ) -> Result<()> {
        self.connection()?.execute(
            r#"
            INSERT OR IGNORE INTO lp_positions (lp_id, faucet_id, shares, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                lp_id.to_hex(),
                faucet_id.to_hex(),
                to_sql_u64(shares)?,
                to_sql_u64(updated_at)?,
            ],
        )?;
        Ok(())
    }

    pub fn position(&self, lp_id: AccountId, faucet_id: AccountId) -> Result<Option<LpPosition>> {
        self.connection()?
            .query_row(
                r#"
                SELECT lp_id, faucet_id, shares, checkpoint_shares, checkpoint_value,
                       checkpoint_withdrawn, updated_at
                FROM lp_positions WHERE lp_id = ?1 AND faucet_id = ?2
                "#,
                params![lp_id.to_hex(), faucet_id.to_hex()],
                position_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn positions(&self) -> Result<Vec<LpPosition>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT lp_id, faucet_id, shares, checkpoint_shares, checkpoint_value,
                   checkpoint_withdrawn, updated_at
            FROM lp_positions ORDER BY lp_id, faucet_id
            "#,
        )?;
        let rows = statement.query_map([], position_from_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_checkpoint(
        &self,
        lp_id: AccountId,
        faucet_id: AccountId,
        shares: u64,
        value: u64,
        withdrawn: u64,
        updated_at: u64,
    ) -> Result<()> {
        self.connection()?.execute(
            r#"
            UPDATE lp_positions
            SET checkpoint_shares = ?3, checkpoint_value = ?4,
                checkpoint_withdrawn = ?5, updated_at = ?6
            WHERE lp_id = ?1 AND faucet_id = ?2
            "#,
            params![
                lp_id.to_hex(),
                faucet_id.to_hex(),
                to_sql_u64(shares)?,
                to_sql_u64(value)?,
                to_sql_u64(withdrawn)?,
                to_sql_u64(updated_at)?,
            ],
        )?;
        Ok(())
    }

    pub fn sync_cursor(&self) -> Result<NoteCursor> {
        let connection = self.connection()?;
        let block_num = connection
            .query_row(
                "SELECT value FROM lp_meta WHERE key = 'sync_cursor_block'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| value.parse().context("parse LP sync cursor block"))
            .transpose()?
            .unwrap_or(0);
        let note_id = connection
            .query_row(
                "SELECT value FROM lp_meta WHERE key = 'sync_cursor_note'",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or_default();
        Ok(NoteCursor { block_num, note_id })
    }

    pub fn set_sync_cursor(&self, cursor: &NoteCursor) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            r#"
            INSERT INTO lp_meta(key, value) VALUES ('sync_cursor_block', ?1)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            "#,
            [cursor.block_num.to_string()],
        )?;
        transaction.execute(
            r#"
            INSERT INTO lp_meta(key, value) VALUES ('sync_cursor_note', ?1)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            "#,
            [&cursor.note_id],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

fn operation_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LpOperation> {
    Ok(LpOperation {
        note_id: row.get(0)?,
        kind: row.get(1)?,
        lp_id: row.get(2)?,
        faucet_id: row.get(3)?,
        asset_amount: from_sql_u64(row.get(4)?)?,
        lp_shares: from_sql_u64(row.get(5)?)?,
        block_num: row.get(6)?,
        status: row.get(7)?,
        created_at: from_sql_u64(row.get(8)?)?,
        applied_at: row
            .get::<_, Option<i64>>(9)?
            .map(from_sql_u64)
            .transpose()?,
    })
}

fn position_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<LpPosition> {
    Ok(LpPosition {
        lp_id: row.get(0)?,
        faucet_id: row.get(1)?,
        shares: from_sql_u64(row.get(2)?)?,
        checkpoint_shares: from_sql_u64(row.get(3)?)?,
        checkpoint_value: from_sql_u64(row.get(4)?)?,
        checkpoint_withdrawn: from_sql_u64(row.get(5)?)?,
        updated_at: from_sql_u64(row.get(6)?)?,
    })
}

fn to_sql_u64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow!("{value} exceeds SQLite INTEGER range"))
}

fn from_sql_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use alloy_primitives::U256;
    use miden_client::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };

    use super::*;
    use crate::{
        execution_store::ExecutionStore,
        pool::{PoolBalances, PoolMetadata, PoolSettings, PoolState},
    };

    fn ids() -> (AccountId, AccountId) {
        (
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE).unwrap(),
            AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap(),
        )
    }

    #[test]
    fn operations_are_applied_exactly_once() {
        let store = LpStore::open(":memory:").unwrap();
        let (lp_id, faucet_id) = ids();
        assert!(
            store
                .record_confirmed(
                    "note-1",
                    Some("nullifier-1"),
                    LpOperationKind::Deposit,
                    lp_id,
                    faucet_id,
                    100,
                    7,
                    10,
                )
                .unwrap()
        );
        assert!(
            !store
                .record_confirmed(
                    "note-1",
                    Some("nullifier-1"),
                    LpOperationKind::Deposit,
                    lp_id,
                    faucet_id,
                    100,
                    7,
                    10,
                )
                .unwrap()
        );
        assert!(store.apply_operation("note-1", 90, 11).unwrap());
        assert!(!store.apply_operation("note-1", 90, 12).unwrap());
        assert_eq!(
            store.position(lp_id, faucet_id).unwrap().unwrap().shares,
            90
        );
    }

    #[test]
    fn marker_recovery_acknowledges_journal_without_reapplying_curve() {
        let lp_store = LpStore::open(":memory:").unwrap();
        let execution_store = ExecutionStore::open(":memory:").unwrap();
        let (lp_id, faucet_id) = ids();
        lp_store
            .record_confirmed(
                "note-restart",
                None,
                LpOperationKind::Deposit,
                lp_id,
                faucet_id,
                100,
                7,
                10,
            )
            .unwrap();
        let state = PoolState::new(
            PoolSettings::default(),
            PoolBalances {
                reserve: U256::from(100),
                reserve_with_slippage: U256::from(100),
                total_liabilities: U256::from(100),
            },
            100,
            PoolMetadata {
                name: "test",
                asset_decimals: 8,
            },
        );
        execution_store
            .save_lp_application(
                "note-restart",
                faucet_id,
                100,
                &HashMap::from([(faucet_id, state)]),
                11,
            )
            .unwrap();

        let recovered_shares = execution_store
            .applied_lp_shares("note-restart")
            .unwrap()
            .unwrap();
        assert!(
            lp_store
                .apply_operation("note-restart", recovered_shares, 12)
                .unwrap()
        );
        assert!(
            !lp_store
                .apply_operation("note-restart", recovered_shares, 13)
                .unwrap()
        );
        assert_eq!(
            lp_store.position(lp_id, faucet_id).unwrap().unwrap().shares,
            100
        );
        assert_eq!(
            execution_store.latest_pool_states().unwrap()[&faucet_id].lp_total_supply(),
            100
        );
    }

    #[test]
    fn checkpoint_snapshot_is_persisted() {
        let store = LpStore::open(":memory:").unwrap();
        let (lp_id, faucet_id) = ids();
        store.seed_position(lp_id, faucet_id, 100, 1).unwrap();
        store
            .record_checkpoint(lp_id, faucet_id, 100, 125, 20, 2)
            .unwrap();
        let position = store.position(lp_id, faucet_id).unwrap().unwrap();
        assert_eq!(position.checkpoint_shares, 100);
        assert_eq!(position.checkpoint_value, 125);
        assert_eq!(position.checkpoint_withdrawn, 20);
    }

    #[test]
    fn sync_cursor_orders_notes_within_a_block() {
        let store = LpStore::open(":memory:").unwrap();
        let cursor = NoteCursor {
            block_num: 42,
            note_id: "note-b".into(),
        };
        store.set_sync_cursor(&cursor).unwrap();
        assert_eq!(store.sync_cursor().unwrap(), cursor);
        assert!(
            NoteCursor {
                block_num: 42,
                note_id: "note-c".into()
            } > cursor
        );
    }
}
