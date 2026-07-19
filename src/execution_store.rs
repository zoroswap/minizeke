use std::{
    collections::{HashMap, HashSet},
    env,
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
};

use alloy_primitives::{I256, U256};
use anyhow::{Context, Result, anyhow};
use miden_client::account::AccountId;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

use crate::{
    analytics_store::FinalizedSwap,
    order::{DurableOrder, Order, Processed},
    pool::{PoolBalances, PoolMetadata, PoolSettings, PoolState},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentReservation {
    New { order_id: uuid::Uuid },
    Existing { order_id: uuid::Uuid },
    Conflict,
}

/// Soft cap on pre-execution backlog (`admitted` + `processing_claimed`).
pub fn max_admitted_orders() -> usize {
    env::var("MAX_ADMITTED_ORDERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(64)
        .max(1)
}

/// Max age of an admitted order before quote fails it as `stale_queue`.
pub fn max_admitted_age_ms() -> u64 {
    env::var("MAX_ADMITTED_AGE_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(60_000)
        .max(1)
}

/// Typed admit rejection (mapped to HTTP 503 by the API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitError {
    QueueFull,
}

impl std::fmt::Display for AdmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueFull => write!(f, "execution queue is full"),
        }
    }
}

impl std::error::Error for AdmitError {}

#[derive(Debug, Clone)]
pub struct ProposedSwap {
    pub order_id: String,
    pub user_id: AccountId,
    pub asset_in: AccountId,
    pub asset_out: AccountId,
    pub amount_in: u64,
    pub amount_out: u64,
    pub quoted_amount_out: U256,
    pub raw_amount_out: U256,
    pub lp_fee: U256,
    pub backstop_fee: U256,
    pub protocol_fee: U256,
    pub volatility_fee: U256,
    pub oracle_price_in: Option<u64>,
    pub oracle_price_out: Option<u64>,
    pub fee_version: u64,
    pub finalization: SwapFinalization,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapFinalization {
    pub user_id: String,
    pub sell_faucet: String,
    pub buy_faucet: String,
    pub amount_in: u64,
    pub amount_out: u64,
    pub sell_before: [String; 3],
    pub sell_after: [String; 3],
    pub buy_before: [String; 3],
    pub buy_after: [String; 3],
    pub analytics_swap: FinalizedSwap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableBatchOrder {
    order: DurableOrder,
    amount_out: u64,
}

#[derive(Debug)]
pub struct ClaimedExecutionBatch {
    pub id: uuid::Uuid,
    pub orders: Vec<Order<Processed>>,
}

#[derive(Debug)]
pub struct RecoveredExecutionBatch {
    pub id: uuid::Uuid,
    pub orders: Vec<Order<Processed>>,
    pub outcomes: Vec<BatchOrderOutcome>,
}

#[derive(Debug, Clone)]
pub struct BatchOrderOutcome {
    pub order_id: uuid::Uuid,
    pub tx_hash: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SubmittedTransaction {
    pub tx_hash: String,
    pub tx_id: Vec<u8>,
    pub pool_id: AccountId,
    pub order_ids: Vec<uuid::Uuid>,
    pub transaction_update: Vec<u8>,
    pub expected_initial_state: Vec<u8>,
    pub expected_final_state: Vec<u8>,
    pub submission_height: u32,
    pub expiration_height: u32,
    pub local_applied: bool,
    pub attempts: u32,
    pub submitted_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxEntry {
    pub id: i64,
    pub topic: String,
    pub aggregate_id: String,
}

pub struct ExecutionStore {
    connection: Mutex<Connection>,
}

impl ExecutionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open execution sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
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
            CREATE TABLE IF NOT EXISTS applied_lp_notes (
                note_id TEXT PRIMARY KEY,
                faucet_id TEXT NOT NULL,
                lp_shares INTEGER NOT NULL,
                applied_at INTEGER NOT NULL
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
                finalization_payload TEXT,
                status TEXT NOT NULL,
                tx_hash TEXT,
                created_at INTEGER NOT NULL,
                finalized_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_swap_accounting_user_time
                ON swap_accounting(user_id, created_at DESC);
            CREATE TABLE IF NOT EXISTS intent_reservations (
                client_order_id TEXT PRIMARY KEY,
                intent_commitment TEXT NOT NULL,
                order_id TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS durable_orders (
                order_id TEXT PRIMARY KEY,
                client_order_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                claim_owner TEXT,
                claim_until INTEGER,
                batch_id TEXT,
                terminal_error TEXT,
                tx_hash TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_durable_orders_claim
                ON durable_orders(state, claim_until, created_at);
            CREATE TABLE IF NOT EXISTS order_lifecycle_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                detail TEXT,
                dedupe_key TEXT NOT NULL UNIQUE,
                created_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_order_lifecycle_order
                ON order_lifecycle_events(order_id, id);
            CREATE TABLE IF NOT EXISTS execution_batches (
                batch_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                state TEXT NOT NULL,
                claim_owner TEXT,
                claim_until INTEGER,
                terminal_error TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            -- At most one pending and one claimed batch (separate single-flight per
            -- state). Reconciling does not take either slot, so a successor pending
            -- batch can exist while a prior claimed batch is still proving/submitting.
            CREATE UNIQUE INDEX IF NOT EXISTS idx_single_pending_execution_batch
                ON execution_batches((1))
                WHERE state = 'pending';
            CREATE UNIQUE INDEX IF NOT EXISTS idx_single_claimed_execution_batch
                ON execution_batches((1))
                WHERE state = 'claimed';
            CREATE TABLE IF NOT EXISTS execution_submissions (
                tx_hash TEXT PRIMARY KEY,
                tx_id BLOB NOT NULL,
                batch_id TEXT NOT NULL,
                pool_id TEXT NOT NULL,
                order_ids TEXT NOT NULL,
                transaction_update BLOB NOT NULL,
                expected_initial_state BLOB NOT NULL,
                expected_final_state BLOB NOT NULL,
                submission_height INTEGER NOT NULL,
                expiration_height INTEGER NOT NULL,
                state TEXT NOT NULL,
                local_applied INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                submitted_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                confirmed_block INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_execution_submissions_state
                ON execution_submissions(state, submitted_at);
            CREATE TABLE IF NOT EXISTS execution_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                delivered_at INTEGER,
                UNIQUE(topic, aggregate_id)
            );
            "#,
        )?;
        if !column_exists(&connection, "swap_accounting", "finalization_payload")? {
            connection.execute(
                "ALTER TABLE swap_accounting ADD COLUMN finalization_payload TEXT",
                [],
            )?;
        }
        // Upgrade: split the combined active-batch unique index so pending and claimed
        // each allow at most one row (a claimed batch no longer blocks a new pending).
        connection.execute_batch(
            "DROP INDEX IF EXISTS idx_single_active_execution_batch;
             DROP INDEX IF EXISTS idx_single_pending_execution_batch;
             DROP INDEX IF EXISTS idx_single_claimed_execution_batch;
             CREATE UNIQUE INDEX idx_single_pending_execution_batch
                ON execution_batches((1))
                WHERE state = 'pending';
             CREATE UNIQUE INDEX idx_single_claimed_execution_batch
                ON execution_batches((1))
                WHERE state = 'claimed';",
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

    /// Atomically reserves a signed client UUID before an order is published. The
    /// reservation intentionally survives processing/execution failure: a signed nonce
    /// may identify at most one server lifecycle forever.
    pub fn reserve_intent(
        &self,
        client_order_id: uuid::Uuid,
        intent_commitment: &str,
        proposed_order_id: uuid::Uuid,
        created_at: u64,
    ) -> Result<IntentReservation> {
        let connection = self.connection()?;
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO intent_reservations (
                client_order_id, intent_commitment, order_id, created_at
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                client_order_id.to_string(),
                intent_commitment,
                proposed_order_id.to_string(),
                to_i64(created_at)?,
            ],
        )?;
        if inserted == 1 {
            return Ok(IntentReservation::New {
                order_id: proposed_order_id,
            });
        }

        let existing = connection
            .query_row(
                "SELECT intent_commitment, order_id
                 FROM intent_reservations WHERE client_order_id = ?1",
                [client_order_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((existing_commitment, order_id)) = existing else {
            return Err(anyhow!(
                "intent reservation insert conflicted without a UUID row"
            ));
        };
        if existing_commitment != intent_commitment {
            return Ok(IntentReservation::Conflict);
        }
        Ok(IntentReservation::Existing {
            order_id: uuid::Uuid::parse_str(&order_id).context("invalid reserved order UUID")?,
        })
    }

    /// Atomically reserves the v2 nonce, commits the complete order, appends its first
    /// lifecycle event, and creates a durable wakeup. Returning `New` means all four
    /// records are committed.
    pub fn admit_order(
        &self,
        client_order_id: uuid::Uuid,
        intent_commitment: &str,
        order: &Order<crate::order::Created>,
        created_at: u64,
    ) -> Result<IntentReservation> {
        self.admit_order_with_limit(
            client_order_id,
            intent_commitment,
            order,
            created_at,
            max_admitted_orders(),
        )
    }

    fn admit_order_with_limit(
        &self,
        client_order_id: uuid::Uuid,
        intent_commitment: &str,
        order: &Order<crate::order::Created>,
        created_at: u64,
        queue_limit: usize,
    ) -> Result<IntentReservation> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing = tx
            .query_row(
                "SELECT intent_commitment, order_id FROM intent_reservations
                 WHERE client_order_id = ?1",
                [client_order_id.to_string()],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((commitment, order_id)) = existing {
            let durable_exists: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM durable_orders WHERE order_id = ?1)",
                [&order_id],
                |row| row.get(0),
            )?;
            if commitment == intent_commitment && !durable_exists {
                let order_id = uuid::Uuid::parse_str(&order_id)?;
                let mut durable = order.durable();
                durable.id = order_id;
                tx.execute(
                    "INSERT INTO durable_orders
                     (order_id, client_order_id, payload, state, created_at, updated_at)
                     VALUES (?1, ?2, ?3, 'admitted', ?4, ?4)",
                    params![
                        order_id.to_string(),
                        client_order_id.to_string(),
                        serde_json::to_string(&durable)?,
                        to_i64(created_at)?
                    ],
                )?;
                append_event(
                    &tx,
                    order_id,
                    "admitted",
                    Some("recovered reservation"),
                    &format!("order:{order_id}:admitted"),
                    created_at,
                )?;
                enqueue_outbox(&tx, "order_admitted", &order_id.to_string(), created_at)?;
            }
            tx.commit()?;
            if commitment != intent_commitment {
                return Ok(IntentReservation::Conflict);
            }
            return Ok(IntentReservation::Existing {
                order_id: uuid::Uuid::parse_str(&order_id)?,
            });
        }

        let queued: i64 = tx.query_row(
            "SELECT COUNT(*) FROM durable_orders
             WHERE state IN ('admitted', 'processing_claimed')",
            [],
            |row| row.get(0),
        )?;
        if queued >= i64::try_from(queue_limit)? {
            tx.commit()?;
            return Err(AdmitError::QueueFull.into());
        }

        let order_id = order.id;
        tx.execute(
            "INSERT INTO intent_reservations
             (client_order_id, intent_commitment, order_id, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                client_order_id.to_string(),
                intent_commitment,
                order_id.to_string(),
                to_i64(created_at)?
            ],
        )?;
        tx.execute(
            "INSERT INTO durable_orders
             (order_id, client_order_id, payload, state, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'admitted', ?4, ?4)",
            params![
                order_id.to_string(),
                client_order_id.to_string(),
                serde_json::to_string(&order.durable())?,
                to_i64(created_at)?
            ],
        )?;
        append_event(
            &tx,
            order_id,
            "admitted",
            None,
            &format!("order:{order_id}:admitted"),
            created_at,
        )?;
        enqueue_outbox(&tx, "order_admitted", &order_id.to_string(), created_at)?;
        tx.commit()?;
        Ok(IntentReservation::New { order_id })
    }

    /// Claims admitted work when no `pending` execution batch exists. A batch that is
    /// `claimed` (prove/submit) or `reconciling` (awaiting finality) does not block the
    /// next claim so Created→Processing can overlap with submission and confirmation.
    /// Expired processing claims are eligible, allowing crash-after-admit and
    /// crash-after-claim recovery.
    pub fn claim_admitted_orders(
        &self,
        worker: &str,
        now: u64,
        lease_ms: u64,
        limit: usize,
    ) -> Result<Vec<Order<crate::order::Created>>> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pending: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_batches
             WHERE state = 'pending')",
            [],
            |row| row.get(0),
        )?;
        if pending {
            tx.commit()?;
            return Ok(Vec::new());
        }
        let claimed_inflight: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_batches
             WHERE state = 'claimed')",
            [],
            |row| row.get(0),
        )?;
        let mut statement = tx.prepare(
            "SELECT order_id, payload, created_at FROM durable_orders
             WHERE state = 'admitted'
                OR (state = 'processing_claimed' AND claim_until < ?1)
             ORDER BY created_at, order_id LIMIT ?2",
        )?;
        let rows = statement
            .query_map(params![to_i64(now)?, i64::try_from(limit)?], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    from_sql_u64(row.get(2)?)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        let claim_until = now.saturating_add(lease_ms);
        let mut orders = Vec::with_capacity(rows.len());
        let mut oldest_created_at = now;
        for (id, payload, created_at) in rows {
            oldest_created_at = oldest_created_at.min(created_at);
            tx.execute(
                "UPDATE durable_orders SET state = 'processing_claimed',
                    claim_owner = ?2, claim_until = ?3, updated_at = ?4
                 WHERE order_id = ?1",
                params![id, worker, to_i64(claim_until)?, to_i64(now)?],
            )?;
            let order_id = uuid::Uuid::parse_str(&id)?;
            append_event(
                &tx,
                order_id,
                "processing_claimed",
                Some(worker),
                &format!("order:{id}:processing-claim:{worker}:{now}"),
                now,
            )?;
            orders.push(
                serde_json::from_str::<DurableOrder>(&payload)?
                    .into_created()?
                    .with_admitted_at_ms(created_at),
            );
            tx.execute(
                "UPDATE execution_outbox SET delivered_at = ?2
                 WHERE topic = 'order_admitted' AND aggregate_id = ?1
                   AND delivered_at IS NULL",
                params![id, to_i64(now)?],
            )?;
        }
        tx.commit()?;
        if !orders.is_empty() {
            if claimed_inflight {
                tracing::info!(
                    count = orders.len(),
                    claim_wait_ms = now.saturating_sub(oldest_created_at),
                    "Claimed admitted orders while prior batch is claimed (quote/execute overlap)"
                );
            } else {
                tracing::info!(
                    count = orders.len(),
                    claim_wait_ms = now.saturating_sub(oldest_created_at),
                    "Claimed admitted orders"
                );
            }
        }
        Ok(orders)
    }

    pub fn fail_claimed_order(
        &self,
        order_id: uuid::Uuid,
        worker: &str,
        reason: &str,
        now: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE durable_orders SET state = 'failed', terminal_error = ?3,
                claim_owner = NULL, claim_until = NULL, updated_at = ?4
             WHERE order_id = ?1 AND state = 'processing_claimed' AND claim_owner = ?2",
            params![order_id.to_string(), worker, reason, to_i64(now)?],
        )?;
        if changed == 1 {
            append_event(
                &tx,
                order_id,
                "failed",
                Some(reason),
                &format!("order:{order_id}:terminal"),
                now,
            )?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    /// Fails admitted / processing_claimed orders that are incompatible with the
    /// current deployment (wrong assets, unlisted pool, or unregistered user).
    pub fn fail_stale_prebatch_orders<F>(
        &self,
        mut is_stale: F,
        reason: &str,
        now: u64,
    ) -> Result<usize>
    where
        F: FnMut(&DurableOrder) -> bool,
    {
        let mut connection = self.connection()?;
        let rows = {
            let mut stmt = connection.prepare(
                "SELECT order_id, payload FROM durable_orders
                 WHERE state IN ('admitted', 'processing_claimed')",
            )?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        };

        let mut stale_ids = Vec::new();
        for (order_id, payload) in rows {
            let order: DurableOrder = match serde_json::from_str(&payload) {
                Ok(order) => order,
                Err(_) => {
                    // Unreadable payloads from a prior schema cannot be settled; drop them.
                    if let Ok(id) = uuid::Uuid::parse_str(&order_id) {
                        stale_ids.push(id);
                    }
                    continue;
                }
            };
            if is_stale(&order) {
                stale_ids.push(order.id);
            }
        }
        if stale_ids.is_empty() {
            return Ok(0);
        }

        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut failed = 0usize;
        for order_id in stale_ids {
            let changed = tx.execute(
                "UPDATE durable_orders SET state = 'failed', terminal_error = ?2,
                    claim_owner = NULL, claim_until = NULL, updated_at = ?3
                 WHERE order_id = ?1 AND state IN ('admitted', 'processing_claimed')",
                params![order_id.to_string(), reason, to_i64(now)?],
            )?;
            if changed == 1 {
                append_event(
                    &tx,
                    order_id,
                    "failed",
                    Some(reason),
                    &format!("order:{order_id}:terminal"),
                    now,
                )?;
                failed += 1;
            }
        }
        tx.commit()?;
        Ok(failed)
    }

    /// Commits all proposed accounting rows and the executable batch in one transaction.
    pub fn create_execution_batch(
        &self,
        worker: &str,
        orders: &[Order<Processed>],
        swaps: &[ProposedSwap],
        now: u64,
    ) -> Result<Option<uuid::Uuid>> {
        if orders.is_empty() {
            return Ok(None);
        }
        if orders.len() != swaps.len() {
            return Err(anyhow!(
                "processed orders and proposed swaps differ in length"
            ));
        }
        for (order, swap) in orders.iter().zip(swaps) {
            if swap.order_id != order.id.to_string() {
                return Err(anyhow!(
                    "proposed swap {} does not match processed order {}",
                    swap.order_id,
                    order.id
                ));
            }
        }
        let batch_id = uuid::Uuid::new_v4();
        let payload: Vec<_> = orders
            .iter()
            .map(|order| DurableBatchOrder {
                order: order.durable(),
                amount_out: order.execution_result().amount_out,
            })
            .collect();
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for order in orders {
            let changed = tx.execute(
                "UPDATE durable_orders SET state = 'batched', batch_id = ?3,
                    claim_owner = NULL, claim_until = NULL, updated_at = ?4
                 WHERE order_id = ?1 AND state = 'processing_claimed' AND claim_owner = ?2",
                params![
                    order.id.to_string(),
                    worker,
                    batch_id.to_string(),
                    to_i64(now)?
                ],
            )?;
            if changed != 1 {
                return Err(anyhow!("order {} is not claimed by {worker}", order.id));
            }
            append_event(
                &tx,
                order.id,
                "batched",
                Some(&batch_id.to_string()),
                &format!("order:{}:batched", order.id),
                now,
            )?;
        }
        for swap in swaps {
            insert_proposed_swap(&tx, swap, now)?;
        }
        tx.execute(
            "INSERT INTO execution_batches
             (batch_id, payload, state, created_at, updated_at)
             VALUES (?1, ?2, 'pending', ?3, ?3)",
            params![
                batch_id.to_string(),
                serde_json::to_string(&payload)?,
                to_i64(now)?
            ],
        )?;
        enqueue_outbox(&tx, "batch_pending", &batch_id.to_string(), now)?;
        tx.commit()?;
        Ok(Some(batch_id))
    }

    pub fn claim_pending_batch(
        &self,
        worker: &str,
        now: u64,
        lease_ms: u64,
    ) -> Result<Option<ClaimedExecutionBatch>> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Prefer reclaiming an expired claimed lease. Promote pending only when the
        // claimed slot is free — `idx_single_claimed_execution_batch` allows one
        // claimed row, while pending may coexist with an in-flight claimed batch.
        let row = tx
            .query_row(
                "SELECT batch_id, payload FROM execution_batches
                 WHERE (state = 'claimed' AND claim_until < ?1)
                    OR (
                        state = 'pending'
                        AND NOT EXISTS (
                            SELECT 1 FROM execution_batches AS claimed
                            WHERE claimed.state = 'claimed'
                              AND (claimed.claim_until IS NULL OR claimed.claim_until >= ?1)
                        )
                    )
                 ORDER BY CASE WHEN state = 'claimed' THEN 0 ELSE 1 END, created_at
                 LIMIT 1",
                [to_i64(now)?],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((id, payload)) = row else {
            tx.commit()?;
            return Ok(None);
        };
        tx.execute(
            "UPDATE execution_batches SET state = 'claimed', claim_owner = ?2,
                claim_until = ?3, updated_at = ?4 WHERE batch_id = ?1",
            params![
                id,
                worker,
                to_i64(now.saturating_add(lease_ms))?,
                to_i64(now)?
            ],
        )?;
        tx.execute(
            "UPDATE execution_outbox SET delivered_at = ?2
             WHERE topic = 'batch_pending' AND aggregate_id = ?1
               AND delivered_at IS NULL",
            params![id, to_i64(now)?],
        )?;
        tx.commit()?;
        drop(connection);
        let batch_id = uuid::Uuid::parse_str(&id)?;
        let entries = serde_json::from_str::<Vec<DurableBatchOrder>>(&payload)?;
        let connection = self.connection()?;
        let mut orders = Vec::new();
        for entry in entries {
            let state: String = connection.query_row(
                "SELECT state FROM durable_orders WHERE order_id = ?1 AND batch_id = ?2",
                params![entry.order.id.to_string(), id],
                |row| row.get(0),
            )?;
            if state == "batched" {
                orders.push(entry.order.into_processed(entry.amount_out)?);
            }
        }
        drop(connection);
        if orders.is_empty() {
            self.begin_reconciliation(batch_id, worker, now)?;
            return Ok(None);
        }
        Ok(Some(ClaimedExecutionBatch {
            id: batch_id,
            orders,
        }))
    }

    pub fn complete_batch(
        &self,
        batch_id: uuid::Uuid,
        worker: &str,
        outcomes: &[BatchOrderOutcome],
        now: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let payload = tx
            .query_row(
                "SELECT payload FROM execution_batches
                 WHERE batch_id = ?1 AND state = 'claimed' AND claim_owner = ?2",
                params![batch_id.to_string(), worker],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(payload) = payload else {
            tx.commit()?;
            return Ok(false);
        };
        let expected: HashSet<_> = serde_json::from_str::<Vec<DurableBatchOrder>>(&payload)?
            .into_iter()
            .map(|entry| entry.order.id)
            .collect();
        let actual: HashSet<_> = outcomes.iter().map(|outcome| outcome.order_id).collect();
        if expected != actual || outcomes.len() != expected.len() {
            return Err(anyhow!(
                "batch {batch_id} cannot become terminal until every order has one outcome"
            ));
        }
        for outcome in outcomes {
            let (state, event) = if outcome.error.is_some() {
                ("failed", "failed")
            } else {
                ("executed", "executed")
            };
            tx.execute(
                "UPDATE durable_orders SET state = ?2, tx_hash = ?3, terminal_error = ?4,
                    updated_at = ?5 WHERE order_id = ?1 AND batch_id = ?6
                    AND state = 'batched'",
                params![
                    outcome.order_id.to_string(),
                    state,
                    outcome.tx_hash,
                    outcome.error,
                    to_i64(now)?,
                    batch_id.to_string()
                ],
            )?;
            append_event(
                &tx,
                outcome.order_id,
                event,
                outcome.error.as_deref().or(outcome.tx_hash.as_deref()),
                &format!("order:{}:terminal", outcome.order_id),
                now,
            )?;
            if outcome.error.is_some() {
                tx.execute(
                    "UPDATE swap_accounting SET status = 'failed', finalized_at = ?2
                     WHERE order_id = ?1 AND status = 'proposed'",
                    params![outcome.order_id.to_string(), to_i64(now)?],
                )?;
            } else {
                tx.execute(
                    "UPDATE swap_accounting SET status = 'executed',
                        tx_hash = COALESCE(?2, tx_hash), finalized_at = ?3
                     WHERE order_id = ?1 AND status = 'proposed'",
                    params![outcome.order_id.to_string(), outcome.tx_hash, to_i64(now)?],
                )?;
            }
        }
        let terminal_error = outcomes.iter().find_map(|outcome| outcome.error.as_deref());
        tx.execute(
            "UPDATE execution_batches SET state = 'terminal', terminal_error = ?3,
                claim_owner = NULL, claim_until = NULL, updated_at = ?4
             WHERE batch_id = ?1 AND claim_owner = ?2",
            params![batch_id.to_string(), worker, terminal_error, to_i64(now)?],
        )?;
        enqueue_outbox(&tx, "batch_terminal", &batch_id.to_string(), now)?;
        tx.commit()?;
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_submission(
        &self,
        batch_id: uuid::Uuid,
        worker: &str,
        tx_hash: &str,
        tx_id: &[u8],
        pool_id: AccountId,
        order_ids: &[uuid::Uuid],
        transaction_update: &[u8],
        expected_initial_state: &[u8],
        expected_final_state: &[u8],
        submission_height: u32,
        expiration_height: u32,
        now: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owned: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM execution_batches
             WHERE batch_id = ?1 AND state = 'claimed' AND claim_owner = ?2)",
            params![batch_id.to_string(), worker],
            |row| row.get(0),
        )?;
        if !owned {
            tx.commit()?;
            return Ok(false);
        }
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO execution_submissions
             (tx_hash, tx_id, batch_id, pool_id, order_ids, transaction_update,
              expected_initial_state, expected_final_state, submission_height,
              expiration_height, state, submitted_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'submitted', ?11, ?11)",
            params![
                tx_hash,
                tx_id,
                batch_id.to_string(),
                pool_id.to_hex(),
                serde_json::to_string(order_ids)?,
                transaction_update,
                expected_initial_state,
                expected_final_state,
                i64::from(submission_height),
                i64::from(expiration_height),
                to_i64(now)?,
            ],
        )?;
        if inserted == 0 {
            tx.commit()?;
            return Ok(true);
        }
        for order_id in order_ids {
            let changed = tx.execute(
                "UPDATE durable_orders SET state = 'submitted', tx_hash = ?2, updated_at = ?3
                 WHERE order_id = ?1 AND batch_id = ?4 AND state = 'batched'",
                params![
                    order_id.to_string(),
                    tx_hash,
                    to_i64(now)?,
                    batch_id.to_string()
                ],
            )?;
            if changed != 1 {
                return Err(anyhow!("order {order_id} was not awaiting submission"));
            }
            tx.execute(
                "UPDATE swap_accounting SET status = 'submitted', tx_hash = ?2
                 WHERE order_id = ?1 AND status = 'proposed'",
                params![order_id.to_string(), tx_hash],
            )?;
            append_event(
                &tx,
                *order_id,
                "submitted",
                Some(tx_hash),
                &format!("order:{order_id}:submitted"),
                now,
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    pub fn mark_pre_submission_failed(
        &self,
        batch_id: uuid::Uuid,
        worker: &str,
        order_ids: &[uuid::Uuid],
        reason: &str,
        now: u64,
    ) -> Result<()> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for order_id in order_ids {
            let changed = tx.execute(
                "UPDATE durable_orders SET state = 'failed', terminal_error = ?2, updated_at = ?3
                 WHERE order_id = ?1 AND batch_id = ?4 AND state = 'batched'
                   AND EXISTS(SELECT 1 FROM execution_batches WHERE batch_id = ?4
                              AND state = 'claimed' AND claim_owner = ?5)",
                params![
                    order_id.to_string(),
                    reason,
                    to_i64(now)?,
                    batch_id.to_string(),
                    worker
                ],
            )?;
            if changed == 1 {
                tx.execute(
                    "UPDATE swap_accounting SET status = 'failed', finalized_at = ?2
                     WHERE order_id = ?1 AND status = 'proposed'",
                    params![order_id.to_string(), to_i64(now)?],
                )?;
                append_event(
                    &tx,
                    *order_id,
                    "failed",
                    Some(reason),
                    &format!("order:{order_id}:terminal"),
                    now,
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Fail every order that is still awaiting submission in a batch owned by `worker`.
    ///
    /// This is a batch-level safety net for preparation paths which fail before a shard can
    /// record a submission. It allows reconciliation to release the single claimed-batch slot.
    pub fn fail_remaining_batched_orders(
        &self,
        batch_id: uuid::Uuid,
        worker: &str,
        reason: &str,
        now: u64,
    ) -> Result<Vec<uuid::Uuid>> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let order_ids = {
            let mut statement = tx.prepare(
                "SELECT order_id FROM durable_orders
                 WHERE batch_id = ?1 AND state = 'batched'
                   AND EXISTS(SELECT 1 FROM execution_batches WHERE batch_id = ?1
                              AND state = 'claimed' AND claim_owner = ?2)
                 ORDER BY created_at, order_id",
            )?;
            statement
                .query_map(params![batch_id.to_string(), worker], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut failed = Vec::with_capacity(order_ids.len());
        for order_id in order_ids {
            let order_id = uuid::Uuid::parse_str(&order_id)?;
            let changed = tx.execute(
                "UPDATE durable_orders SET state = 'failed', terminal_error = ?2, updated_at = ?3
                 WHERE order_id = ?1 AND batch_id = ?4 AND state = 'batched'",
                params![
                    order_id.to_string(),
                    reason,
                    to_i64(now)?,
                    batch_id.to_string()
                ],
            )?;
            if changed != 1 {
                continue;
            }
            tx.execute(
                "UPDATE swap_accounting SET status = 'failed', finalized_at = ?2
                 WHERE order_id = ?1 AND status = 'proposed'",
                params![order_id.to_string(), to_i64(now)?],
            )?;
            append_event(
                &tx,
                order_id,
                "failed",
                Some(reason),
                &format!("order:{order_id}:terminal"),
                now,
            )?;
            failed.push(order_id);
        }
        tx.commit()?;
        Ok(failed)
    }

    /// Ends transaction submission and moves the batch to `reconciling`. Each of
    /// `pending` and `claimed` remains single-flight; the admit gate only holds while a
    /// batch is `pending`, so Processing can claim new orders during prove/submit and
    /// finality.
    pub fn begin_reconciliation(
        &self,
        batch_id: uuid::Uuid,
        worker: &str,
        now: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let remaining: i64 = tx.query_row(
            "SELECT COUNT(*) FROM durable_orders WHERE batch_id = ?1 AND state = 'batched'",
            [batch_id.to_string()],
            |row| row.get(0),
        )?;
        if remaining != 0 {
            return Err(anyhow!("batch {batch_id} still has unsubmitted orders"));
        }
        let changed = tx.execute(
            "UPDATE execution_batches SET state = 'reconciling', claim_owner = NULL,
                claim_until = NULL, updated_at = ?3
             WHERE batch_id = ?1 AND state = 'claimed' AND claim_owner = ?2",
            params![batch_id.to_string(), worker, to_i64(now)?],
        )?;
        if changed == 1 {
            finalize_batch_if_ready(&tx, batch_id, now)?;
        }
        tx.commit()?;
        Ok(changed == 1)
    }

    pub fn submitted_transactions(&self) -> Result<Vec<SubmittedTransaction>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT tx_hash, tx_id, pool_id, order_ids, transaction_update,
                    expected_initial_state, expected_final_state, submission_height,
                    expiration_height, local_applied, attempts, submitted_at, updated_at
             FROM execution_submissions WHERE state = 'submitted'
             ORDER BY submitted_at, tx_hash",
        )?;
        let rows = statement.query_map([], |row| {
            let order_ids: String = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, String>(2)?,
                order_ids,
                row.get::<_, Vec<u8>>(4)?,
                row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, bool>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, i64>(11)?,
                row.get::<_, i64>(12)?,
            ))
        })?;
        let mut submissions = Vec::new();
        for row in rows {
            let (
                tx_hash,
                tx_id,
                pool_id,
                order_ids,
                transaction_update,
                initial,
                final_state,
                submission_height,
                expiration_height,
                local_applied,
                attempts,
                submitted_at,
                updated_at,
            ) = row?;
            submissions.push(SubmittedTransaction {
                tx_hash,
                tx_id,
                pool_id: AccountId::from_hex(&pool_id)?,
                order_ids: serde_json::from_str(&order_ids)?,
                transaction_update,
                expected_initial_state: initial,
                expected_final_state: final_state,
                submission_height: u32::try_from(submission_height)?,
                expiration_height: u32::try_from(expiration_height)?,
                local_applied,
                attempts: u32::try_from(attempts)?,
                submitted_at: u64::try_from(submitted_at)?,
                updated_at: u64::try_from(updated_at)?,
            });
        }
        Ok(submissions)
    }

    pub fn mark_submission_local_applied(&self, tx_hash: &str, now: u64) -> Result<()> {
        self.connection()?.execute(
            "UPDATE execution_submissions SET local_applied = 1, last_error = NULL,
                updated_at = ?2 WHERE tx_hash = ?1 AND state = 'submitted'",
            params![tx_hash, to_i64(now)?],
        )?;
        Ok(())
    }

    pub fn record_reconciliation_attempt(
        &self,
        tx_hash: &str,
        error: Option<&str>,
        now: u64,
    ) -> Result<()> {
        self.connection()?.execute(
            "UPDATE execution_submissions SET attempts = attempts + 1, last_error = ?2,
                updated_at = ?3 WHERE tx_hash = ?1 AND state = 'submitted'",
            params![tx_hash, error, to_i64(now)?],
        )?;
        Ok(())
    }

    pub fn confirm_submission(&self, tx_hash: &str, block_num: u32, now: u64) -> Result<bool> {
        self.resolve_submission(tx_hash, true, Some(block_num), None, now)
    }

    pub fn fail_submission(&self, tx_hash: &str, reason: &str, now: u64) -> Result<bool> {
        self.resolve_submission(tx_hash, false, None, Some(reason), now)
    }

    fn resolve_submission(
        &self,
        tx_hash: &str,
        confirmed: bool,
        confirmed_block: Option<u32>,
        reason: Option<&str>,
        now: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT batch_id, order_ids FROM execution_submissions
                 WHERE tx_hash = ?1 AND state = 'submitted'",
                [tx_hash],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let Some((batch_id, order_ids)) = row else {
            tx.commit()?;
            return Ok(false);
        };
        let batch_id = uuid::Uuid::parse_str(&batch_id)?;
        let order_ids: Vec<uuid::Uuid> = serde_json::from_str(&order_ids)?;
        let state = if confirmed { "confirmed" } else { "failed" };
        tx.execute(
            "UPDATE execution_submissions SET state = ?2, confirmed_block = ?3,
                last_error = ?4, updated_at = ?5 WHERE tx_hash = ?1",
            params![
                tx_hash,
                state,
                confirmed_block.map(i64::from),
                reason,
                to_i64(now)?
            ],
        )?;
        for order_id in order_ids {
            tx.execute(
                "UPDATE durable_orders SET state = ?2, terminal_error = ?3, updated_at = ?4
                 WHERE order_id = ?1 AND state = 'submitted' AND tx_hash = ?5",
                params![order_id.to_string(), state, reason, to_i64(now)?, tx_hash],
            )?;
            tx.execute(
                "UPDATE swap_accounting SET status = ?2, finalized_at = ?3
                 WHERE order_id = ?1 AND status = 'submitted'",
                params![order_id.to_string(), state, to_i64(now)?],
            )?;
            append_event(
                &tx,
                order_id,
                state,
                reason.or(Some(tx_hash)),
                &format!("order:{order_id}:terminal"),
                now,
            )?;
        }
        finalize_batch_if_ready(&tx, batch_id, now)?;
        tx.commit()?;
        Ok(true)
    }

    pub fn pending_outbox(&self, limit: usize) -> Result<Vec<OutboxEntry>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, topic, aggregate_id FROM execution_outbox
             WHERE delivered_at IS NULL ORDER BY id LIMIT ?1",
        )?;
        Ok(statement
            .query_map([i64::try_from(limit)?], |row| {
                Ok(OutboxEntry {
                    id: row.get(0)?,
                    topic: row.get(1)?,
                    aggregate_id: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recover_terminal_batch(
        &self,
        batch_id: uuid::Uuid,
    ) -> Result<Option<RecoveredExecutionBatch>> {
        let connection = self.connection()?;
        let payload = connection
            .query_row(
                "SELECT payload FROM execution_batches
                 WHERE batch_id = ?1 AND state = 'terminal'",
                [batch_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(payload) = payload else {
            return Ok(None);
        };
        let entries = serde_json::from_str::<Vec<DurableBatchOrder>>(&payload)?;
        let mut orders = Vec::with_capacity(entries.len());
        let mut outcomes = Vec::with_capacity(entries.len());
        for entry in entries {
            let order_id = entry.order.id;
            let (state, tx_hash, error) = connection.query_row(
                "SELECT state, tx_hash, terminal_error FROM durable_orders
                 WHERE order_id = ?1 AND batch_id = ?2",
                params![order_id.to_string(), batch_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )?;
            if state != "executed" && state != "confirmed" && state != "failed" {
                return Err(anyhow!(
                    "terminal batch contains non-terminal order {order_id}"
                ));
            }
            orders.push(entry.order.into_processed(entry.amount_out)?);
            outcomes.push(BatchOrderOutcome {
                order_id,
                tx_hash,
                error,
            });
        }
        Ok(Some(RecoveredExecutionBatch {
            id: batch_id,
            orders,
            outcomes,
        }))
    }

    pub fn mark_outbox_delivered(&self, id: i64, delivered_at: u64) -> Result<bool> {
        Ok(self.connection()?.execute(
            "UPDATE execution_outbox SET delivered_at = ?2
             WHERE id = ?1 AND delivered_at IS NULL",
            params![id, to_i64(delivered_at)?],
        )? == 1)
    }

    #[cfg(test)]
    fn order_state(&self, order_id: uuid::Uuid) -> Result<Option<String>> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT state FROM durable_orders WHERE order_id = ?1",
                [order_id.to_string()],
                |row| row.get(0),
            )
            .optional()?)
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

    /// Durably commits an LP curve mutation. The note marker and every resulting pool
    /// snapshot share one SQLite transaction, so recovery can use the marker as the
    /// authority for whether the curve mutation has already happened.
    pub fn save_lp_application(
        &self,
        note_id: &str,
        faucet_id: AccountId,
        lp_shares: u64,
        pool_states: &HashMap<AccountId, PoolState>,
        applied_at: u64,
    ) -> Result<bool> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let already_applied = transaction
            .query_row(
                "SELECT 1 FROM applied_lp_notes WHERE note_id = ?1",
                [note_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if already_applied {
            return Ok(false);
        }
        for (snapshot_faucet_id, state) in pool_states {
            upsert_pool_state(&transaction, *snapshot_faucet_id, *state, applied_at)?;
        }
        transaction.execute(
            "INSERT INTO applied_lp_notes (note_id, faucet_id, lp_shares, applied_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                note_id,
                faucet_id.to_hex(),
                to_i64(lp_shares)?,
                to_i64(applied_at)?,
            ],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    pub fn applied_lp_shares(&self, note_id: &str) -> Result<Option<u64>> {
        self.connection()?
            .query_row(
                "SELECT lp_shares FROM applied_lp_notes WHERE note_id = ?1",
                [note_id],
                |row| {
                    let shares = row.get::<_, i64>(0)?;
                    u64::try_from(shares).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Integer,
                            Box::new(error),
                        )
                    })
                },
            )
            .optional()
            .map_err(Into::into)
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

    pub fn swap_finalization(&self, order_id: &str) -> Result<Option<SwapFinalization>> {
        let payload = self
            .connection()?
            .query_row(
                "SELECT finalization_payload FROM swap_accounting WHERE order_id = ?1",
                [order_id],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        payload
            .map(|payload| serde_json::from_str(&payload).context("decode swap finalization"))
            .transpose()
    }
}

fn append_event(
    tx: &Transaction<'_>,
    order_id: uuid::Uuid,
    event_type: &str,
    detail: Option<&str>,
    dedupe_key: &str,
    created_at: u64,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO order_lifecycle_events
         (order_id, event_type, detail, dedupe_key, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            order_id.to_string(),
            event_type,
            detail,
            dedupe_key,
            to_i64(created_at)?
        ],
    )?;
    Ok(())
}

fn enqueue_outbox(
    tx: &Transaction<'_>,
    topic: &str,
    aggregate_id: &str,
    created_at: u64,
) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO execution_outbox
         (topic, aggregate_id, created_at) VALUES (?1, ?2, ?3)",
        params![topic, aggregate_id, to_i64(created_at)?],
    )?;
    Ok(())
}

fn finalize_batch_if_ready(tx: &Transaction<'_>, batch_id: uuid::Uuid, now: u64) -> Result<bool> {
    let nonterminal: i64 = tx.query_row(
        "SELECT COUNT(*) FROM durable_orders
         WHERE batch_id = ?1 AND state NOT IN ('confirmed', 'executed', 'failed')",
        [batch_id.to_string()],
        |row| row.get(0),
    )?;
    if nonterminal != 0 {
        return Ok(false);
    }
    let terminal_error = tx
        .query_row(
            "SELECT terminal_error FROM durable_orders
             WHERE batch_id = ?1 AND terminal_error IS NOT NULL LIMIT 1",
            [batch_id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .optional()?;
    let changed = tx.execute(
        "UPDATE execution_batches SET state = 'terminal', terminal_error = ?2,
            claim_owner = NULL, claim_until = NULL, updated_at = ?3
         WHERE batch_id = ?1 AND state IN ('claimed', 'reconciling')",
        params![batch_id.to_string(), terminal_error, to_i64(now)?],
    )?;
    if changed == 1 {
        enqueue_outbox(tx, "batch_terminal", &batch_id.to_string(), now)?;
    }
    Ok(changed == 1)
}

fn insert_proposed_swap(tx: &Transaction<'_>, swap: &ProposedSwap, created_at: u64) -> Result<()> {
    let retained_surplus = swap
        .quoted_amount_out
        .saturating_sub(U256::from(swap.amount_out));
    tx.execute(
        r#"
        INSERT INTO swap_accounting (
            order_id, user_id, asset_in, asset_out, amount_in, amount_out,
            quoted_amount_out, raw_amount_out, lp_fee, backstop_fee, protocol_fee,
            volatility_fee, retained_surplus, oracle_price_in, oracle_price_out,
            fee_version, finalization_payload, status, created_at
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
            ?14, ?15, ?16, ?17, 'proposed', ?18
        )
        ON CONFLICT(order_id) DO NOTHING
        "#,
        params![
            swap.order_id,
            swap.user_id.to_hex(),
            swap.asset_in.to_hex(),
            swap.asset_out.to_hex(),
            to_i64(swap.amount_in)?,
            to_i64(swap.amount_out)?,
            swap.quoted_amount_out.to_string(),
            swap.raw_amount_out.to_string(),
            swap.lp_fee.to_string(),
            swap.backstop_fee.to_string(),
            swap.protocol_fee.to_string(),
            swap.volatility_fee.to_string(),
            retained_surplus.to_string(),
            swap.oracle_price_in.map(to_i64).transpose()?,
            swap.oracle_price_out.map(to_i64).transpose()?,
            to_i64(swap.fee_version)?,
            serde_json::to_string(&swap.finalization)?,
            to_i64(created_at)?,
        ],
    )?;
    Ok(())
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

fn from_sql_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn column_exists(connection: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(names.iter().any(|name| name == column))
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_client::auth::AuthSecretKey;
    use miden_core::Word;

    fn test_order(client_order_id: uuid::Uuid) -> Order<crate::order::Created> {
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let user = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        let asset_in = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let asset_out = AccountId::from_hex("0x1e7e8af77fc5f2f1631d5c5ce35471").unwrap();
        let intent = crate::intent::Intent::new_swap(
            user,
            asset_in,
            10,
            asset_out,
            9,
            client_order_id,
            u64::MAX,
        );
        Order::new(
            key.sign(Word::default()),
            user,
            crate::order::OrderDetails::new(asset_in, 10, asset_out, 9),
            key.public_key(),
            intent,
        )
    }

    fn proposed(order: &Order<Processed>) -> ProposedSwap {
        let details = order.details();
        ProposedSwap {
            order_id: order.id.to_string(),
            user_id: order.user_id(),
            asset_in: details.asset_in,
            asset_out: details.asset_out,
            amount_in: details.amount_in,
            amount_out: order.execution_result().amount_out,
            quoted_amount_out: U256::from(9),
            raw_amount_out: U256::from(10),
            lp_fee: U256::from(1),
            backstop_fee: U256::ZERO,
            protocol_fee: U256::ZERO,
            volatility_fee: U256::ZERO,
            oracle_price_in: Some(1),
            oracle_price_out: Some(1),
            fee_version: 0,
            finalization: SwapFinalization {
                user_id: order.user_id().to_hex(),
                sell_faucet: details.asset_in.to_hex(),
                buy_faucet: details.asset_out.to_hex(),
                amount_in: details.amount_in,
                amount_out: order.execution_result().amount_out,
                sell_before: ["10".into(), "10".into(), "10".into()],
                sell_after: ["11".into(), "11".into(), "11".into()],
                buy_before: ["10".into(), "10".into(), "10".into()],
                buy_after: ["9".into(), "9".into(), "9".into()],
                analytics_swap: FinalizedSwap {
                    event_id: order.id.to_string(),
                    user_id: order.user_id().to_hex(),
                    pool_id: details.asset_out.to_hex(),
                    asset_in: details.asset_in.to_hex(),
                    asset_out: details.asset_out.to_hex(),
                    quote_asset: "oracle_usd".into(),
                    amount_in: details.amount_in,
                    amount_out: order.execution_result().amount_out,
                    quote_value: 10,
                    lp_fee_quote: 1,
                    protocol_fee_quote: 0,
                    backstop_fee_quote: 0,
                    volatility_fee_quote: 0,
                    requested_amount_out: Some(details.min_amount_out),
                    quoted_amount_out: Some(order.execution_result().amount_out),
                    event_time: 1,
                },
            },
        }
    }

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

    fn lp_test_state(reserve: u64, supply: u64) -> PoolState {
        PoolState::new(
            PoolSettings::default(),
            PoolBalances {
                reserve: U256::from(reserve),
                reserve_with_slippage: U256::from(reserve),
                total_liabilities: U256::from(reserve),
            },
            supply,
            PoolMetadata {
                name: "test",
                asset_decimals: 8,
            },
        )
    }

    #[test]
    fn lp_snapshot_and_marker_are_atomic_and_restart_idempotent() {
        let path = std::env::temp_dir().join(format!(
            "minizeke-lp-atomic-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let faucet = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        let first = lp_test_state(100, 100);
        {
            let store = ExecutionStore::open(&path).unwrap();
            assert!(
                store
                    .save_lp_application(
                        "note-atomic",
                        faucet,
                        100,
                        &HashMap::from([(faucet, first)]),
                        10,
                    )
                    .unwrap()
            );
        }

        let store = ExecutionStore::open(&path).unwrap();
        assert_eq!(store.applied_lp_shares("note-atomic").unwrap(), Some(100));
        assert_eq!(
            store.latest_pool_states().unwrap()[&faucet].lp_total_supply(),
            100
        );
        assert!(
            !store
                .save_lp_application(
                    "note-atomic",
                    faucet,
                    100,
                    &HashMap::from([(faucet, lp_test_state(200, 200))]),
                    11,
                )
                .unwrap()
        );
        assert_eq!(
            store.latest_pool_states().unwrap()[&faucet].lp_total_supply(),
            100
        );
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn lp_snapshot_rolls_back_when_marker_write_fails() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let faucet = AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap();
        store
            .connection()
            .unwrap()
            .execute_batch(
                "CREATE TRIGGER reject_lp_marker BEFORE INSERT ON applied_lp_notes
                 BEGIN SELECT RAISE(ABORT, 'fault injection'); END;",
            )
            .unwrap();

        assert!(
            store
                .save_lp_application(
                    "note-fault",
                    faucet,
                    50,
                    &HashMap::from([(faucet, lp_test_state(50, 50))]),
                    10,
                )
                .is_err()
        );
        assert!(store.latest_pool_states().unwrap().is_empty());
        assert_eq!(store.applied_lp_shares("note-fault").unwrap(), None);
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

    #[test]
    fn intent_reservation_is_idempotent_and_rejects_uuid_rebinding() {
        let path =
            std::env::temp_dir().join(format!("minizeke-intents-{}.sqlite3", uuid::Uuid::new_v4()));
        let client_id = uuid::Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let order_id = uuid::Uuid::parse_str("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee").unwrap();

        {
            let store = ExecutionStore::open(&path).unwrap();
            assert_eq!(
                store
                    .reserve_intent(client_id, "commitment-a", order_id, 1)
                    .unwrap(),
                IntentReservation::New { order_id }
            );
        }
        let store = ExecutionStore::open(&path).unwrap();
        assert_eq!(
            store
                .reserve_intent(client_id, "commitment-a", uuid::Uuid::new_v4(), 2)
                .unwrap(),
            IntentReservation::Existing { order_id }
        );
        assert_eq!(
            store
                .reserve_intent(client_id, "commitment-b", uuid::Uuid::new_v4(), 3)
                .unwrap(),
            IntentReservation::Conflict
        );
        drop(store);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn admit_rejects_when_pre_execution_queue_is_full() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let first = test_order(uuid::Uuid::new_v4());
        let second = test_order(uuid::Uuid::new_v4());
        let third = test_order(uuid::Uuid::new_v4());
        assert!(matches!(
            store
                .admit_order_with_limit(first.client_order_id(), "c1", &first, 1, 2)
                .unwrap(),
            IntentReservation::New { .. }
        ));
        assert!(matches!(
            store
                .admit_order_with_limit(second.client_order_id(), "c2", &second, 2, 2)
                .unwrap(),
            IntentReservation::New { .. }
        ));
        let err = store
            .admit_order_with_limit(third.client_order_id(), "c3", &third, 3, 2)
            .unwrap_err();
        assert!(
            err.downcast_ref::<AdmitError>() == Some(&AdmitError::QueueFull),
            "expected QueueFull, got {err:#}"
        );
        // Idempotent re-admit of an existing order still succeeds.
        assert!(matches!(
            store
                .admit_order_with_limit(first.client_order_id(), "c1", &first, 4, 2)
                .unwrap(),
            IntentReservation::Existing { .. }
        ));
        // Free a slot by claiming then failing one order.
        let claimed = store
            .claim_admitted_orders("processing", 5, 100, 10)
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(
            store
                .fail_claimed_order(claimed[0].id, "processing", "stale_queue", 6)
                .unwrap()
        );
        let fourth = test_order(uuid::Uuid::new_v4());
        assert!(matches!(
            store
                .admit_order_with_limit(fourth.client_order_id(), "c4", &fourth, 7, 2)
                .unwrap(),
            IntentReservation::New { .. }
        ));
    }

    #[test]
    fn admission_is_idempotent_and_enqueues_one_wakeup() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let client_id = uuid::Uuid::new_v4();
        let order = test_order(client_id);
        assert_eq!(
            store
                .admit_order(client_id, "commitment", &order, 1)
                .unwrap(),
            IntentReservation::New { order_id: order.id }
        );
        assert_eq!(
            store
                .admit_order(client_id, "commitment", &order, 2)
                .unwrap(),
            IntentReservation::Existing { order_id: order.id }
        );
        assert_eq!(
            store.admit_order(client_id, "other", &order, 3).unwrap(),
            IntentReservation::Conflict
        );
        let entries = store.pending_outbox(10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].topic, "order_admitted");
        assert!(store.mark_outbox_delivered(entries[0].id, 4).unwrap());
        assert!(!store.mark_outbox_delivered(entries[0].id, 5).unwrap());
    }

    #[test]
    fn crash_after_admit_recovers_from_reopened_database() {
        let path =
            std::env::temp_dir().join(format!("minizeke-admit-{}.sqlite3", uuid::Uuid::new_v4()));
        let client_id = uuid::Uuid::new_v4();
        let expected_id;
        {
            let store = ExecutionStore::open(&path).unwrap();
            let order = test_order(client_id);
            expected_id = order.id;
            store
                .admit_order(client_id, "commitment", &order, 1)
                .unwrap();
        }
        {
            let store = ExecutionStore::open(&path).unwrap();
            let claimed = store
                .claim_admitted_orders("processing-b", 2, 100, 10)
                .unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(claimed[0].id, expected_id);
        }
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn stale_processing_claim_is_reclaimed() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let client_id = uuid::Uuid::new_v4();
        let order = test_order(client_id);
        store
            .admit_order(client_id, "commitment", &order, 1)
            .unwrap();
        assert_eq!(
            store
                .claim_admitted_orders("worker-a", 10, 10, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .claim_admitted_orders("worker-b", 20, 10, 10)
                .unwrap()
                .is_empty()
        );
        let reclaimed = store.claim_admitted_orders("worker-b", 21, 10, 10).unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].id, order.id);
    }

    #[test]
    fn pending_batch_recovers_and_gate_releases_only_after_terminal_commit() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let first_client = uuid::Uuid::new_v4();
        let first = test_order(first_client);
        store.admit_order(first_client, "first", &first, 1).unwrap();
        let claimed = store
            .claim_admitted_orders("processing", 2, 100, 10)
            .unwrap()
            .pop()
            .unwrap();
        let processed = claimed
            .start_processing()
            .processed(crate::order::OrderExecutionResult { amount_out: 9 });
        let batch_id = store
            .create_execution_batch(
                "processing",
                std::slice::from_ref(&processed),
                &[proposed(&processed)],
                3,
            )
            .unwrap()
            .unwrap();

        let second_client = uuid::Uuid::new_v4();
        let second = test_order(second_client);
        store
            .admit_order(second_client, "second", &second, 4)
            .unwrap();
        assert!(
            store
                .claim_admitted_orders("processing", 5, 100, 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .pending_outbox(10)
                .unwrap()
                .iter()
                .any(|entry| entry.topic == "batch_pending")
        );

        let claimed_batch = store
            .claim_pending_batch("executor-a", 10, 10)
            .unwrap()
            .unwrap();
        assert_eq!(claimed_batch.id, batch_id);
        assert!(
            store
                .claim_pending_batch("executor-b", 20, 10)
                .unwrap()
                .is_none()
        );
        let recovered = store
            .claim_pending_batch("executor-b", 21, 10)
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id, batch_id);
        assert!(
            store
                .complete_batch(batch_id, "executor-b", &[], 22)
                .is_err()
        );
        // Claimed no longer blocks admit; only pending does.
        assert_eq!(
            store
                .claim_admitted_orders("processing", 22, 100, 10)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .complete_batch(
                    batch_id,
                    "executor-b",
                    &[BatchOrderOutcome {
                        order_id: processed.id,
                        tx_hash: Some("tx".to_owned()),
                        error: None,
                    }],
                    23,
                )
                .unwrap()
        );
        assert!(
            !store
                .complete_batch(batch_id, "executor-b", &[], 24)
                .unwrap()
        );
        assert_eq!(
            store.order_state(processed.id).unwrap().as_deref(),
            Some("executed")
        );
        let topics: Vec<_> = store
            .pending_outbox(10)
            .unwrap()
            .into_iter()
            .map(|entry| entry.topic)
            .collect();
        assert!(!topics.contains(&"batch_pending".to_owned()));
        assert!(topics.contains(&"batch_terminal".to_owned()));
    }

    #[test]
    fn claim_waits_for_claimed_slot_when_pending_successor_exists() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let (batch_a, processed_a) = create_claimed_test_batch(&store, "processing");

        // While A is still claimed, Processing may create a successor pending batch.
        let second_client = uuid::Uuid::new_v4();
        let second = test_order(second_client);
        store
            .admit_order(second_client, "second", &second, 10)
            .unwrap();
        let claimed = store
            .claim_admitted_orders("processing", 11, 100, 10)
            .unwrap()
            .pop()
            .unwrap();
        let processed_b = claimed
            .start_processing()
            .processed(crate::order::OrderExecutionResult { amount_out: 9 });
        let batch_b = store
            .create_execution_batch(
                "processing",
                std::slice::from_ref(&processed_b),
                &[proposed(&processed_b)],
                12,
            )
            .unwrap()
            .unwrap();
        assert_ne!(batch_a, batch_b);

        // Must return None (not UNIQUE constraint error) while A's claimed lease is live.
        assert!(
            store
                .claim_pending_batch("executor", 13, 100)
                .unwrap()
                .is_none()
        );

        // Free the claimed slot by submitting A's orders and reconciling.
        let pool_id = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        assert!(
            store
                .record_submission(
                    batch_a,
                    "executor",
                    "tx-a",
                    &[1; 32],
                    pool_id,
                    &[processed_a.id],
                    &[2, 3],
                    &[4],
                    &[5],
                    10,
                    20,
                    14,
                )
                .unwrap()
        );
        assert!(store.begin_reconciliation(batch_a, "executor", 15).unwrap());

        let claimed_b = store
            .claim_pending_batch("executor", 16, 100)
            .unwrap()
            .unwrap();
        assert_eq!(claimed_b.id, batch_b);
    }

    #[test]
    fn pre_submission_failure_terminalizes_batch_and_releases_claimed_slot() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let (batch_a, processed_a) = create_claimed_test_batch(&store, "processing");

        let second_client = uuid::Uuid::new_v4();
        let second = test_order(second_client);
        store
            .admit_order(second_client, "second", &second, 10)
            .unwrap();
        let claimed = store
            .claim_admitted_orders("processing", 11, 100, 10)
            .unwrap()
            .pop()
            .unwrap();
        let processed_b = claimed
            .start_processing()
            .processed(crate::order::OrderExecutionResult { amount_out: 9 });
        let batch_b = store
            .create_execution_batch(
                "processing",
                std::slice::from_ref(&processed_b),
                &[proposed(&processed_b)],
                12,
            )
            .unwrap()
            .unwrap();

        assert!(
            store
                .claim_pending_batch("executor", 13, 100)
                .unwrap()
                .is_none()
        );
        let failed = store
            .fail_remaining_batched_orders(
                batch_a,
                "executor",
                "transient sync failed before submission",
                14,
            )
            .unwrap();
        assert_eq!(failed, vec![processed_a.id]);
        assert_eq!(
            store.order_state(processed_a.id).unwrap().as_deref(),
            Some("failed")
        );
        assert!(store.begin_reconciliation(batch_a, "executor", 15).unwrap());
        assert!(store.pending_outbox(10).unwrap().iter().any(|entry| {
            entry.topic == "batch_terminal" && entry.aggregate_id == batch_a.to_string()
        }));

        let claimed_b = store
            .claim_pending_batch("executor", 16, 100)
            .unwrap()
            .unwrap();
        assert_eq!(claimed_b.id, batch_b);
    }

    fn create_claimed_test_batch(
        store: &ExecutionStore,
        worker: &str,
    ) -> (uuid::Uuid, Order<Processed>) {
        let client_id = uuid::Uuid::new_v4();
        let order = test_order(client_id);
        store
            .admit_order(client_id, "commitment", &order, 1)
            .unwrap();
        let claimed = store
            .claim_admitted_orders(worker, 2, 100, 10)
            .unwrap()
            .pop()
            .unwrap();
        let processed = claimed
            .start_processing()
            .processed(crate::order::OrderExecutionResult { amount_out: 9 });
        let batch_id = store
            .create_execution_batch(
                worker,
                std::slice::from_ref(&processed),
                &[proposed(&processed)],
                3,
            )
            .unwrap()
            .unwrap();
        let claimed = store
            .claim_pending_batch("executor", 4, 100)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.id, batch_id);
        (batch_id, processed)
    }

    #[test]
    fn submitted_confirmation_releases_admit_gate_while_reconciling() {
        let store = ExecutionStore::open(":memory:").unwrap();
        let (batch_id, processed) = create_claimed_test_batch(&store, "processing");
        let pool_id = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
        assert!(
            store
                .record_submission(
                    batch_id,
                    "executor",
                    "tx-1",
                    &[1; 32],
                    pool_id,
                    &[processed.id],
                    &[2, 3],
                    &[4],
                    &[5],
                    10,
                    20,
                    5,
                )
                .unwrap()
        );
        assert_eq!(
            store.order_state(processed.id).unwrap().as_deref(),
            Some("submitted")
        );
        // Claimed (prove/submit): admit gate does not block — only pending does.
        let next_client = uuid::Uuid::new_v4();
        let next = test_order(next_client);
        store.admit_order(next_client, "next", &next, 7).unwrap();
        assert_eq!(
            store
                .claim_admitted_orders("processing", 8, 100, 10)
                .unwrap()
                .len(),
            1
        );

        assert!(store.begin_reconciliation(batch_id, "executor", 6).unwrap());
        // Reconciling: further admitted orders may still be claimed (overlap with finality).
        let third_client = uuid::Uuid::new_v4();
        let third = test_order(third_client);
        store.admit_order(third_client, "third", &third, 9).unwrap();
        assert_eq!(
            store
                .claim_admitted_orders("processing", 10, 100, 10)
                .unwrap()
                .len(),
            1
        );

        assert!(store.confirm_submission("tx-1", 12, 9).unwrap());
        assert_eq!(
            store.order_state(processed.id).unwrap().as_deref(),
            Some("confirmed")
        );
    }

    #[test]
    fn submitted_metadata_and_retry_state_survive_restart() {
        let path = std::env::temp_dir().join(format!(
            "minizeke-finality-restart-{}.sqlite3",
            uuid::Uuid::new_v4()
        ));
        let expected_order;
        {
            let store = ExecutionStore::open(&path).unwrap();
            let (batch_id, processed) = create_claimed_test_batch(&store, "processing");
            expected_order = processed.id;
            let pool_id = AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap();
            store
                .record_submission(
                    batch_id,
                    "executor",
                    "tx-restart",
                    &[9; 32],
                    pool_id,
                    &[processed.id],
                    &[1, 2, 3],
                    &[4, 5],
                    &[6, 7],
                    11,
                    22,
                    5,
                )
                .unwrap();
            store.begin_reconciliation(batch_id, "executor", 6).unwrap();
            store
                .record_reconciliation_attempt("tx-restart", Some("sync unavailable"), 7)
                .unwrap();
        }
        {
            let store = ExecutionStore::open(&path).unwrap();
            let submissions = store.submitted_transactions().unwrap();
            assert_eq!(submissions.len(), 1);
            assert_eq!(submissions[0].order_ids, vec![expected_order]);
            assert_eq!(submissions[0].transaction_update, vec![1, 2, 3]);
            assert_eq!(submissions[0].attempts, 1);
            assert!(
                store
                    .fail_submission("tx-restart", "confirmation timeout", 8)
                    .unwrap()
            );
            assert_eq!(
                store.order_state(expected_order).unwrap().as_deref(),
                Some("failed")
            );
        }
        let _ = std::fs::remove_file(path);
    }
}
