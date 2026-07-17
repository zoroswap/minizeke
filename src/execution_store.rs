use std::{
    collections::HashMap,
    env,
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
};

use alloy_primitives::{I256, U256};
use anyhow::{Context, Result, anyhow};
use miden_client::account::AccountId;
use rusqlite::{Connection, OptionalExtension, params};

use crate::pool::{PoolBalances, PoolMetadata, PoolSettings, PoolState};

pub struct ExecutionStore {
    connection: Mutex<Connection>,
}

impl ExecutionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open execution sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS pool_snapshots (
                faucet_id TEXT PRIMARY KEY,
                reserve TEXT NOT NULL,
                reserve_with_slippage TEXT NOT NULL,
                total_liabilities TEXT NOT NULL,
                lp_total_supply INTEGER NOT NULL,
                beta TEXT NOT NULL,
                c TEXT NOT NULL,
                swap_fee TEXT NOT NULL,
                backstop_fee TEXT NOT NULL,
                protocol_fee TEXT NOT NULL,
                asset_decimals INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS swap_accounting (
                order_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                asset_in TEXT NOT NULL,
                asset_out TEXT NOT NULL,
                amount_in INTEGER NOT NULL,
                amount_out INTEGER NOT NULL,
                quoted_amount_out TEXT NOT NULL,
                raw_amount_out TEXT NOT NULL,
                lp_fee TEXT NOT NULL,
                backstop_fee TEXT NOT NULL,
                protocol_fee TEXT NOT NULL,
                volatility_fee TEXT NOT NULL,
                retained_surplus TEXT NOT NULL,
                oracle_price_in INTEGER,
                oracle_price_out INTEGER,
                fee_version INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL,
                tx_hash TEXT,
                created_at INTEGER NOT NULL,
                finalized_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_swap_accounting_user_time
                ON swap_accounting(user_id, created_at DESC);
            "#,
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn open_from_env() -> Result<Self> {
        let path = env::var("EXECUTION_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_string());
            format!("execution.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("execution database lock poisoned"))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_proposed_swap(
        &self,
        order_id: &str,
        user_id: AccountId,
        asset_in: AccountId,
        asset_out: AccountId,
        amount_in: u64,
        amount_out: u64,
        quoted_amount_out: U256,
        raw_amount_out: U256,
        lp_fee: U256,
        backstop_fee: U256,
        protocol_fee: U256,
        volatility_fee: U256,
        oracle_price_in: Option<u64>,
        oracle_price_out: Option<u64>,
        fee_version: u64,
        created_at: u64,
    ) -> Result<()> {
        let retained_surplus = quoted_amount_out.saturating_sub(U256::from(amount_out));
        self.connection()?.execute(
            r#"
            INSERT INTO swap_accounting (
                order_id, user_id, asset_in, asset_out, amount_in, amount_out,
                quoted_amount_out, raw_amount_out, lp_fee, backstop_fee, protocol_fee,
                volatility_fee, retained_surplus, oracle_price_in, oracle_price_out,
                fee_version, status, created_at
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, 'proposed', ?17
            )
            ON CONFLICT(order_id) DO NOTHING
            "#,
            params![
                order_id,
                user_id.to_hex(),
                asset_in.to_hex(),
                asset_out.to_hex(),
                to_i64(amount_in)?,
                to_i64(amount_out)?,
                quoted_amount_out.to_string(),
                raw_amount_out.to_string(),
                lp_fee.to_string(),
                backstop_fee.to_string(),
                protocol_fee.to_string(),
                volatility_fee.to_string(),
                retained_surplus.to_string(),
                oracle_price_in.map(to_i64).transpose()?,
                oracle_price_out.map(to_i64).transpose()?,
                to_i64(fee_version)?,
                to_i64(created_at)?,
            ],
        )?;
        Ok(())
    }

    pub fn finalize_swap(
        &self,
        order_id: &str,
        tx_hash: Option<&str>,
        finalized_at: u64,
    ) -> Result<()> {
        self.connection()?.execute(
            "UPDATE swap_accounting
             SET status = 'executed', tx_hash = COALESCE(?2, tx_hash), finalized_at = ?3
             WHERE order_id = ?1",
            params![order_id, tx_hash, to_i64(finalized_at)?],
        )?;
        Ok(())
    }

    pub fn save_pool_states(
        &self,
        pool_states: &HashMap<AccountId, PoolState>,
        updated_at: u64,
    ) -> Result<()> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        for (faucet_id, state) in pool_states {
            upsert_pool_state(&transaction, *faucet_id, *state, updated_at)?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn fail_swap(&self, order_id: &str, finalized_at: u64) -> Result<()> {
        self.connection()?.execute(
            "UPDATE swap_accounting SET status = 'failed', finalized_at = ?2 WHERE order_id = ?1",
            params![order_id, to_i64(finalized_at)?],
        )?;
        Ok(())
    }

    pub fn latest_pool_states(&self) -> Result<HashMap<AccountId, PoolState>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT faucet_id, reserve, reserve_with_slippage, total_liabilities,
                    lp_total_supply, beta, c, swap_fee, backstop_fee, protocol_fee,
                    asset_decimals
             FROM pool_snapshots",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, String>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, i64>(10)?,
            ))
        })?;
        let mut states = HashMap::new();
        for row in rows {
            let (
                faucet_id,
                reserve,
                reserve_with_slippage,
                total_liabilities,
                supply,
                beta,
                c,
                swap_fee,
                backstop_fee,
                protocol_fee,
                decimals,
            ) = row?;
            states.insert(
                AccountId::from_hex(&faucet_id)?,
                PoolState::new(
                    PoolSettings {
                        beta: I256::from_str(&beta)?,
                        c: I256::from_str(&c)?,
                        swap_fee: U256::from_str(&swap_fee)?,
                        backstop_fee: U256::from_str(&backstop_fee)?,
                        protocol_fee: U256::from_str(&protocol_fee)?,
                        ..PoolSettings::default()
                    },
                    PoolBalances {
                        reserve: U256::from_str(&reserve)?,
                        reserve_with_slippage: U256::from_str(&reserve_with_slippage)?,
                        total_liabilities: U256::from_str(&total_liabilities)?,
                    },
                    u64::try_from(supply).context("negative LP supply")?,
                    PoolMetadata {
                        name: "Restored pool",
                        asset_decimals: u8::try_from(decimals).context("invalid asset decimals")?,
                    },
                ),
            );
        }
        Ok(states)
    }

    pub fn executed_swap(&self, order_id: &str) -> Result<bool> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT status FROM swap_accounting WHERE order_id = ?1",
                [order_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .is_some_and(|status| status == "executed"))
    }
}

fn upsert_pool_state(
    transaction: &rusqlite::Transaction<'_>,
    faucet_id: AccountId,
    state: PoolState,
    updated_at: u64,
) -> Result<()> {
    transaction.execute(
        r#"
        INSERT INTO pool_snapshots (
            faucet_id, reserve, reserve_with_slippage, total_liabilities,
            lp_total_supply, beta, c, swap_fee, backstop_fee, protocol_fee,
            asset_decimals, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
        ON CONFLICT(faucet_id) DO UPDATE SET
            reserve = excluded.reserve,
            reserve_with_slippage = excluded.reserve_with_slippage,
            total_liabilities = excluded.total_liabilities,
            lp_total_supply = excluded.lp_total_supply,
            beta = excluded.beta,
            c = excluded.c,
            swap_fee = excluded.swap_fee,
            backstop_fee = excluded.backstop_fee,
            protocol_fee = excluded.protocol_fee,
            asset_decimals = excluded.asset_decimals,
            updated_at = excluded.updated_at
        "#,
        params![
            faucet_id.to_hex(),
            state.balances().reserve.to_string(),
            state.balances().reserve_with_slippage.to_string(),
            state.balances().total_liabilities.to_string(),
            to_i64(state.lp_total_supply())?,
            state.settings().beta.to_string(),
            state.settings().c.to_string(),
            state.settings().swap_fee.to_string(),
            state.settings().backstop_fee.to_string(),
            state.settings().protocol_fee.to_string(),
            i64::from(state.metadata().asset_decimals),
            to_i64(updated_at)?,
        ],
    )?;
    Ok(())
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds sqlite INTEGER")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalized_pool_snapshot_survives_reopen_semantics() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let faucet = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let state = PoolState::new(
            PoolSettings::default(),
            PoolBalances {
                reserve: U256::from(123),
                reserve_with_slippage: U256::from(120),
                total_liabilities: U256::from(100),
            },
            77,
            PoolMetadata {
                name: "test",
                asset_decimals: 8,
            },
        );
        store
            .save_pool_states(&HashMap::from([(faucet, state)]), 10)
            .unwrap();
        let restored = store.latest_pool_states().unwrap();
        assert_eq!(restored[&faucet].balances(), state.balances());
        assert_eq!(restored[&faucet].lp_total_supply(), 77);
    }

    #[test]
    fn swap_status_is_idempotently_finalized() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let user = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        let asset_in = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let asset_out = AccountId::from_hex("0x1e7e8af77fc5f2f1631d5c5ce35471").unwrap();
        store
            .record_proposed_swap(
                "order",
                user,
                asset_in,
                asset_out,
                10,
                9,
                U256::from(9),
                U256::from(10),
                U256::from(1),
                U256::ZERO,
                U256::ZERO,
                U256::ZERO,
                Some(1),
                Some(1),
                0,
                1,
            )
            .unwrap();
        store.finalize_swap("order", Some("tx"), 2).unwrap();
        store.finalize_swap("order", Some("tx"), 3).unwrap();
        assert!(store.executed_swap("order").unwrap());
    }
}
