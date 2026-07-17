//! Durable, idempotent analytics journal and integer-only accounting projections.
//!
//! Prices and quote values are caller-defined integer units. A mark represents
//! `price_numerator / price_scale` quote units per smallest asset unit.

use std::{
    env,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CashFlowKind {
    Fund,
    InitRedeem,
    Redeem,
}

impl CashFlowKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Fund => "fund",
            Self::InitRedeem => "init_redeem",
            Self::Redeem => "redeem",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LpCashFlowKind {
    Deposit,
    Withdrawal,
}

impl LpCashFlowKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deposit => "lp_deposit",
            Self::Withdrawal => "lp_withdrawal",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FinalizedSwap {
    pub event_id: String,
    pub user_id: String,
    pub pool_id: String,
    pub asset_in: String,
    pub asset_out: String,
    /// Common valuation currency used by all quote-valued fields.
    pub quote_asset: String,
    pub amount_in: u64,
    pub amount_out: u64,
    /// Event-time fair value of either leg, in `quote_asset` smallest units.
    pub quote_value: u128,
    pub lp_fee_quote: u128,
    pub protocol_fee_quote: u128,
    pub backstop_fee_quote: u128,
    pub volatility_fee_quote: u128,
    /// Optional amount requested by the user. Enables fill-rate metrics.
    pub requested_amount_out: Option<u64>,
    /// Optional pre-execution quoted output. Enables price-improvement/slippage metrics.
    pub quoted_amount_out: Option<u64>,
    pub event_time: u64,
}

impl FinalizedSwap {
    fn total_fee(&self) -> Result<u128> {
        self.lp_fee_quote
            .checked_add(self.protocol_fee_quote)
            .and_then(|v| v.checked_add(self.backstop_fee_quote))
            .and_then(|v| v.checked_add(self.volatility_fee_quote))
            .ok_or_else(|| anyhow!("swap fee total overflow"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CashFlow {
    pub event_id: String,
    pub kind: CashFlowKind,
    pub user_id: String,
    pub asset_id: String,
    pub amount: u64,
    pub event_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LpCashFlow {
    pub event_id: String,
    pub kind: LpCashFlowKind,
    pub lp_id: String,
    pub pool_id: String,
    pub asset_id: String,
    pub amount: u64,
    pub shares: u64,
    pub event_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleMark {
    pub event_id: String,
    pub asset_id: String,
    pub quote_asset: String,
    pub price_numerator: u128,
    pub price_scale: u128,
    pub event_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpeningPosition {
    pub event_id: String,
    pub user_id: String,
    pub asset_id: String,
    pub quote_asset: String,
    pub quantity: u64,
    pub cost_quote: u128,
    pub realized_pnl_quote: i128,
    pub as_of: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSnapshotInput {
    pub event_id: String,
    pub pool_id: String,
    pub asset_id: String,
    pub quote_asset: String,
    pub nav_quote: u128,
    pub tvl_quote: u128,
    pub inventory_quantity: i128,
    pub inventory_cost_quote: i128,
    pub inventory_value_quote: i128,
    pub event_time: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoverageMetadata {
    pub partial: bool,
    pub opening_snapshot: bool,
    pub coverage_start: Option<u64>,
    pub as_of: u64,
    pub trades_covered: bool,
    pub cash_flows_covered: bool,
    pub marks_covered: bool,
    pub fill_metrics_covered: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PositionAnalytics {
    pub asset_id: String,
    pub quote_asset: String,
    pub quantity: u64,
    pub cost_quote: u128,
    pub average_cost_numerator: u128,
    pub average_cost_scale: u128,
    pub realized_pnl_quote: i128,
    pub unrealized_pnl_quote: Option<i128>,
    pub total_pnl_quote: Option<i128>,
    pub mark_price_numerator: Option<u128>,
    pub mark_price_scale: Option<u128>,
    pub mark_time: Option<u64>,
    pub volume_quote: u128,
    pub fee_total_quote: u128,
    pub fills: u64,
    pub requested_amount_out: u128,
    pub filled_amount_out: u128,
    pub quoted_amount_out: u128,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserSummary {
    pub user_id: String,
    pub quote_asset: String,
    pub realized_pnl_quote: i128,
    pub unrealized_pnl_quote: Option<i128>,
    pub total_pnl_quote: Option<i128>,
    pub deposits: u128,
    pub initiated_withdrawals: u128,
    pub withdrawals: u128,
    pub volume_quote: u128,
    pub fee_total_quote: u128,
    pub fills: u64,
    pub requested_amount_out: u128,
    pub filled_amount_out: u128,
    pub quoted_amount_out: u128,
    pub fill_rate_numerator: Option<u128>,
    pub fill_rate_scale: Option<u128>,
    pub coverage: CoverageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolAnalytics {
    pub pool_id: String,
    pub asset_id: String,
    pub quote_asset: String,
    pub nav_quote: u128,
    pub tvl_quote: u128,
    pub inventory_quantity: i128,
    pub inventory_cost_quote: i128,
    pub inventory_value_quote: i128,
    pub inventory_pnl_quote: i128,
    pub lp_deposits: u128,
    pub lp_withdrawals: u128,
    pub swap_volume_quote: u128,
    pub lp_fee_quote: u128,
    pub protocol_fee_quote: u128,
    pub backstop_fee_quote: u128,
    pub volatility_fee_quote: u128,
    pub snapshot_time: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Pagination {
    pub offset: u64,
    pub limit: u32,
}

impl Default for Pagination {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: 50,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_offset: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalEvent {
    pub event_id: String,
    pub kind: String,
    pub subject_id: String,
    pub asset_id: Option<String>,
    pub pool_id: Option<String>,
    pub event_time: u64,
    pub payload: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct NoteCursor {
    pub block_num: u32,
    pub note_id: String,
}

/// SQLite-backed event journal. Every event insert and projection update commits atomically.
pub struct AnalyticsStore {
    connection: Mutex<Connection>,
}

impl AnalyticsStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open analytics sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(SCHEMA)?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn open_from_env() -> Result<Self> {
        let path = env::var("ANALYTICS_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_owned());
            format!("analytics.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("analytics database lock poisoned"))
    }

    pub fn record_swap(&self, swap: &FinalizedSwap) -> Result<bool> {
        if swap.asset_in == swap.asset_out {
            bail!("swap assets must differ");
        }
        let payload = serde_json::to_string(swap)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &swap.event_id,
            "swap",
            &swap.user_id,
            Some(&swap.asset_in),
            Some(&swap.pool_id),
            swap.event_time,
            &payload,
        )? {
            return duplicate_result(&tx, &swap.event_id, "swap", &payload);
        }

        let fee = swap.total_fee()?;
        dispose(
            &tx,
            &swap.user_id,
            &swap.asset_in,
            &swap.quote_asset,
            swap.amount_in,
            swap.quote_value,
            fee,
            swap.event_time,
        )?;
        acquire(
            &tx,
            &swap.user_id,
            &swap.asset_out,
            &swap.quote_asset,
            swap.amount_out,
            swap.quote_value,
            swap.event_time,
        )?;
        update_fill_metrics(&tx, swap)?;
        update_pool_swap_totals(&tx, swap)?;
        update_coverage(
            &tx,
            &swap.user_id,
            swap.event_time,
            true,
            false,
            false,
            swap.requested_amount_out.is_some() || swap.quoted_amount_out.is_some(),
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn record_cash_flow(&self, flow: &CashFlow) -> Result<bool> {
        let payload = serde_json::to_string(flow)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &flow.event_id,
            flow.kind.as_str(),
            &flow.user_id,
            Some(&flow.asset_id),
            None,
            flow.event_time,
            &payload,
        )? {
            return duplicate_result(&tx, &flow.event_id, flow.kind.as_str(), &payload);
        }
        let column = match flow.kind {
            CashFlowKind::Fund => "deposits",
            CashFlowKind::InitRedeem => "initiated_withdrawals",
            CashFlowKind::Redeem => "withdrawals",
        };
        let mark = latest_mark_for_asset(&tx, &flow.asset_id, flow.event_time)?;
        let (quote_asset, value) = mark
            .map(|(quote, numerator, scale)| {
                let value = u128::from(flow.amount)
                    .checked_mul(numerator)
                    .ok_or_else(|| anyhow!("cash-flow mark value overflow"))?
                    / scale;
                Ok::<_, anyhow::Error>((quote, value))
            })
            .transpose()?
            .unwrap_or_else(|| (flow.asset_id.clone(), u128::from(flow.amount)));
        increment_cash_total(
            &tx,
            &flow.user_id,
            &quote_asset,
            column,
            value,
            flow.event_time,
        )?;
        if flow.kind != CashFlowKind::InitRedeem {
            match flow.kind {
                CashFlowKind::Fund => acquire(
                    &tx,
                    &flow.user_id,
                    &flow.asset_id,
                    &quote_asset,
                    flow.amount,
                    value,
                    flow.event_time,
                )?,
                CashFlowKind::Redeem => {
                    let existing = load_position(&tx, &flow.user_id, &flow.asset_id, &quote_asset)?;
                    let available = existing.as_ref().map(|state| state.quantity).unwrap_or(0);
                    if available < flow.amount {
                        // Historical note coverage can begin after an account was funded. Seed the
                        // missing quantity at zero cost and expose partial coverage to callers.
                        acquire(
                            &tx,
                            &flow.user_id,
                            &flow.asset_id,
                            &quote_asset,
                            flow.amount - available,
                            0,
                            flow.event_time,
                        )?;
                    }
                    dispose(
                        &tx,
                        &flow.user_id,
                        &flow.asset_id,
                        &quote_asset,
                        flow.amount,
                        value,
                        0,
                        flow.event_time,
                    )?;
                }
                CashFlowKind::InitRedeem => unreachable!(),
            }
        }
        update_coverage(
            &tx,
            &flow.user_id,
            flow.event_time,
            false,
            true,
            false,
            false,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn record_lp_cash_flow(&self, flow: &LpCashFlow) -> Result<bool> {
        let payload = serde_json::to_string(flow)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &flow.event_id,
            flow.kind.as_str(),
            &flow.lp_id,
            Some(&flow.asset_id),
            Some(&flow.pool_id),
            flow.event_time,
            &payload,
        )? {
            return duplicate_result(&tx, &flow.event_id, flow.kind.as_str(), &payload);
        }
        let column = match flow.kind {
            LpCashFlowKind::Deposit => "lp_deposits",
            LpCashFlowKind::Withdrawal => "lp_withdrawals",
        };
        increment_pool_total(
            &tx,
            &flow.pool_id,
            &flow.asset_id,
            column,
            u128::from(flow.amount),
            flow.event_time,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn record_mark(&self, mark: &OracleMark) -> Result<bool> {
        if mark.price_scale == 0 {
            bail!("oracle mark price_scale must be non-zero");
        }
        let payload = serde_json::to_string(mark)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &mark.event_id,
            "oracle_mark",
            &mark.asset_id,
            Some(&mark.asset_id),
            None,
            mark.event_time,
            &payload,
        )? {
            return duplicate_result(&tx, &mark.event_id, "oracle_mark", &payload);
        }
        tx.execute(
            "INSERT INTO analytics_marks
             (event_id, asset_id, quote_asset, price_numerator, price_scale, event_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                mark.event_id,
                mark.asset_id,
                mark.quote_asset,
                u128_text(mark.price_numerator),
                u128_text(mark.price_scale),
                to_i64(mark.event_time)?,
            ],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn has_mark(&self, asset_id: &str) -> Result<bool> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT 1 FROM analytics_marks WHERE asset_id = ?1 LIMIT 1",
                [asset_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    pub fn note_cursor(&self, source: &str) -> Result<NoteCursor> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT block_num, note_id FROM analytics_note_cursors WHERE source = ?1",
                [source],
                |row| {
                    Ok(NoteCursor {
                        block_num: u32::try_from(row.get::<_, i64>(0)?).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        })?,
                        note_id: row.get(1)?,
                    })
                },
            )
            .optional()
            .map(|value| value.unwrap_or_default())
            .map_err(Into::into)
    }

    pub fn set_note_cursor(&self, source: &str, cursor: &NoteCursor) -> Result<()> {
        self.connection()?.execute(
            "INSERT INTO analytics_note_cursors(source, block_num, note_id)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(source) DO UPDATE SET
               block_num=excluded.block_num, note_id=excluded.note_id",
            params![source, cursor.block_num, cursor.note_id],
        )?;
        Ok(())
    }

    /// Seeds pre-journal holdings. It does not manufacture historical volume or cash flows.
    pub fn record_opening_position(&self, opening: &OpeningPosition) -> Result<bool> {
        let payload = serde_json::to_string(opening)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &opening.event_id,
            "opening_position",
            &opening.user_id,
            Some(&opening.asset_id),
            None,
            opening.as_of,
            &payload,
        )? {
            return duplicate_result(&tx, &opening.event_id, "opening_position", &payload);
        }
        let existing = load_position(
            &tx,
            &opening.user_id,
            &opening.asset_id,
            &opening.quote_asset,
        )?;
        if existing.is_some() {
            bail!("opening position must precede journal activity for this user/asset");
        }
        save_position(
            &tx,
            &PositionState {
                user_id: opening.user_id.clone(),
                asset_id: opening.asset_id.clone(),
                quote_asset: opening.quote_asset.clone(),
                quantity: opening.quantity,
                cost_quote: opening.cost_quote,
                realized_pnl_quote: opening.realized_pnl_quote,
                volume_quote: 0,
                fee_total_quote: 0,
                fills: 0,
                requested_amount_out: 0,
                filled_amount_out: 0,
                quoted_amount_out: 0,
                updated_at: opening.as_of,
            },
        )?;
        tx.execute(
            "INSERT INTO analytics_coverage
             (subject_id, coverage_start, opening_snapshot, trades_covered, cash_flows_covered, marks_covered, fill_metrics_covered)
             VALUES (?1, ?2, 1, 0, 0, 0, 0)
             ON CONFLICT(subject_id) DO UPDATE SET
               coverage_start = MIN(coverage_start, excluded.coverage_start),
               opening_snapshot = 1",
            params![opening.user_id, to_i64(opening.as_of)?],
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn record_pool_snapshot(&self, snapshot: &PoolSnapshotInput) -> Result<bool> {
        let payload = serde_json::to_string(snapshot)?;
        let mut connection = self.connection()?;
        let tx = connection.transaction()?;
        if !insert_event(
            &tx,
            &snapshot.event_id,
            "pool_snapshot",
            &snapshot.pool_id,
            Some(&snapshot.asset_id),
            Some(&snapshot.pool_id),
            snapshot.event_time,
            &payload,
        )? {
            return duplicate_result(&tx, &snapshot.event_id, "pool_snapshot", &payload);
        }
        tx.execute(
            "INSERT INTO analytics_pool_snapshots
             (event_id, pool_id, asset_id, quote_asset, nav_quote, tvl_quote,
              inventory_quantity, inventory_cost_quote, inventory_value_quote, event_time)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                snapshot.event_id,
                snapshot.pool_id,
                snapshot.asset_id,
                snapshot.quote_asset,
                u128_text(snapshot.nav_quote),
                u128_text(snapshot.tvl_quote),
                i128_text(snapshot.inventory_quantity),
                i128_text(snapshot.inventory_cost_quote),
                i128_text(snapshot.inventory_value_quote),
                to_i64(snapshot.event_time)?,
            ],
        )?;
        ensure_pool_totals(
            &tx,
            &snapshot.pool_id,
            &snapshot.asset_id,
            snapshot.event_time,
        )?;
        tx.commit()?;
        Ok(true)
    }

    pub fn positions(
        &self,
        user_id: &str,
        quote_asset: &str,
        as_of: u64,
        pagination: Pagination,
    ) -> Result<Page<PositionAnalytics>> {
        validate_page(pagination)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT user_id, asset_id, quote_asset, quantity, cost_quote, realized_pnl_quote,
                    volume_quote, fee_total_quote, fills, requested_amount_out,
                    filled_amount_out, quoted_amount_out, updated_at
             FROM analytics_positions
             WHERE user_id = ?1 AND quote_asset = ?2
             ORDER BY asset_id LIMIT ?3 OFFSET ?4",
        )?;
        let requested = u64::from(pagination.limit) + 1;
        let rows = statement.query_map(
            params![
                user_id,
                quote_asset,
                to_i64(requested)?,
                to_i64(pagination.offset)?
            ],
            map_position_state,
        )?;
        let states = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = states.len() > pagination.limit as usize;
        let mut items = Vec::with_capacity(states.len().min(pagination.limit as usize));
        for state in states.into_iter().take(pagination.limit as usize) {
            items.push(position_analytics(&connection, state, as_of)?);
        }
        Ok(Page {
            items,
            next_offset: has_more.then(|| pagination.offset + u64::from(pagination.limit)),
        })
    }

    pub fn user_summary(
        &self,
        user_id: &str,
        quote_asset: &str,
        as_of: u64,
    ) -> Result<UserSummary> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT user_id, asset_id, quote_asset, quantity, cost_quote, realized_pnl_quote,
                    volume_quote, fee_total_quote, fills, requested_amount_out,
                    filled_amount_out, quoted_amount_out, updated_at
             FROM analytics_positions WHERE user_id = ?1 AND quote_asset = ?2",
        )?;
        let states = statement
            .query_map(params![user_id, quote_asset], map_position_state)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut realized = 0i128;
        let mut unrealized = 0i128;
        let mut all_marked = true;
        let mut volume = 0u128;
        let mut fees = 0u128;
        let mut fills = 0u64;
        let mut requested = 0u128;
        let mut filled = 0u128;
        let mut quoted = 0u128;
        for state in states {
            let position = position_analytics(&connection, state, as_of)?;
            realized = checked_iadd(realized, position.realized_pnl_quote)?;
            volume = checked_uadd(volume, position.volume_quote)?;
            fees = checked_uadd(fees, position.fee_total_quote)?;
            fills = fills
                .checked_add(position.fills)
                .ok_or_else(|| anyhow!("fill count overflow"))?;
            requested = checked_uadd(requested, position.requested_amount_out)?;
            filled = checked_uadd(filled, position.filled_amount_out)?;
            quoted = checked_uadd(quoted, position.quoted_amount_out)?;
            match position.unrealized_pnl_quote {
                Some(value) => unrealized = checked_iadd(unrealized, value)?,
                None if position.quantity != 0 => all_marked = false,
                None => {}
            }
        }
        // Each swap updates both legs; metrics are stored only on the acquired leg.
        let cash = load_cash_totals(&connection, user_id, quote_asset)?;
        let coverage = load_coverage(&connection, user_id, as_of, all_marked)?;
        let unrealized = all_marked.then_some(unrealized);
        let total = unrealized
            .map(|value| checked_iadd(realized, value))
            .transpose()?;
        Ok(UserSummary {
            user_id: user_id.to_owned(),
            quote_asset: quote_asset.to_owned(),
            realized_pnl_quote: realized,
            unrealized_pnl_quote: unrealized,
            total_pnl_quote: total,
            deposits: cash.0,
            initiated_withdrawals: cash.1,
            withdrawals: cash.2,
            volume_quote: volume,
            fee_total_quote: fees,
            fills,
            requested_amount_out: requested,
            filled_amount_out: filled,
            quoted_amount_out: quoted,
            fill_rate_numerator: (requested != 0).then_some(filled),
            fill_rate_scale: (requested != 0).then_some(requested),
            coverage,
        })
    }

    pub fn pool_summary(&self, pool_id: &str, as_of: u64) -> Result<Option<PoolAnalytics>> {
        let connection = self.connection()?;
        let snapshot = connection
            .query_row(
                "SELECT asset_id, quote_asset, nav_quote, tvl_quote, inventory_quantity,
                        inventory_cost_quote, inventory_value_quote, event_time
                 FROM analytics_pool_snapshots
                 WHERE pool_id = ?1 AND event_time <= ?2
                 ORDER BY event_time DESC, sequence DESC LIMIT 1",
                params![pool_id, to_i64(as_of)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        parse_u128_col(row, 2)?,
                        parse_u128_col(row, 3)?,
                        parse_i128_col(row, 4)?,
                        parse_i128_col(row, 5)?,
                        parse_i128_col(row, 6)?,
                        from_i64(row.get(7)?)?,
                    ))
                },
            )
            .optional()?;
        let Some((asset, quote, nav, tvl, inventory, cost, value, snapshot_time)) = snapshot else {
            return Ok(None);
        };
        let totals = load_pool_totals(&connection, pool_id, &asset)?;
        Ok(Some(PoolAnalytics {
            pool_id: pool_id.to_owned(),
            asset_id: asset,
            quote_asset: quote,
            nav_quote: nav,
            tvl_quote: tvl,
            inventory_quantity: inventory,
            inventory_cost_quote: cost,
            inventory_value_quote: value,
            inventory_pnl_quote: value
                .checked_sub(cost)
                .ok_or_else(|| anyhow!("inventory PnL overflow"))?,
            lp_deposits: totals.0,
            lp_withdrawals: totals.1,
            swap_volume_quote: totals.2,
            lp_fee_quote: totals.3,
            protocol_fee_quote: totals.4,
            backstop_fee_quote: totals.5,
            volatility_fee_quote: totals.6,
            snapshot_time,
        }))
    }

    pub fn events(&self, pagination: Pagination) -> Result<Page<JournalEvent>> {
        validate_page(pagination)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT event_id, kind, subject_id, asset_id, pool_id, event_time, payload
             FROM analytics_events ORDER BY sequence ASC LIMIT ?1 OFFSET ?2",
        )?;
        let requested = u64::from(pagination.limit) + 1;
        let rows = statement.query_map(
            params![to_i64(requested)?, to_i64(pagination.offset)?],
            |row| {
                Ok(JournalEvent {
                    event_id: row.get(0)?,
                    kind: row.get(1)?,
                    subject_id: row.get(2)?,
                    asset_id: row.get(3)?,
                    pool_id: row.get(4)?,
                    event_time: from_i64(row.get(5)?)?,
                    payload: row.get(6)?,
                })
            },
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > pagination.limit as usize;
        items.truncate(pagination.limit as usize);
        Ok(Page {
            items,
            next_offset: has_more.then(|| pagination.offset + u64::from(pagination.limit)),
        })
    }

    pub fn events_for_subject(
        &self,
        subject_id: &str,
        pagination: Pagination,
    ) -> Result<Page<JournalEvent>> {
        validate_page(pagination)?;
        let connection = self.connection()?;
        let requested = u64::from(pagination.limit) + 1;
        let mut statement = connection.prepare(
            "SELECT event_id, kind, subject_id, asset_id, pool_id, event_time, payload
             FROM analytics_events WHERE subject_id = ?1
             ORDER BY sequence ASC LIMIT ?2 OFFSET ?3",
        )?;
        let rows = statement.query_map(
            params![subject_id, to_i64(requested)?, to_i64(pagination.offset)?],
            |row| {
                Ok(JournalEvent {
                    event_id: row.get(0)?,
                    kind: row.get(1)?,
                    subject_id: row.get(2)?,
                    asset_id: row.get(3)?,
                    pool_id: row.get(4)?,
                    event_time: from_i64(row.get(5)?)?,
                    payload: row.get(6)?,
                })
            },
        )?;
        let mut items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let has_more = items.len() > pagination.limit as usize;
        items.truncate(pagination.limit as usize);
        Ok(Page {
            items,
            next_offset: has_more.then(|| pagination.offset + u64::from(pagination.limit)),
        })
    }
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS analytics_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    asset_id TEXT,
    pool_id TEXT,
    event_time INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analytics_events_subject_time
    ON analytics_events(subject_id, event_time, sequence);

CREATE TABLE IF NOT EXISTS analytics_note_cursors (
    source TEXT PRIMARY KEY,
    block_num INTEGER NOT NULL,
    note_id TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_positions (
    user_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    cost_quote TEXT NOT NULL,
    realized_pnl_quote TEXT NOT NULL,
    volume_quote TEXT NOT NULL,
    fee_total_quote TEXT NOT NULL,
    fills INTEGER NOT NULL,
    requested_amount_out TEXT NOT NULL,
    filled_amount_out TEXT NOT NULL,
    quoted_amount_out TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(user_id, asset_id, quote_asset)
);

CREATE TABLE IF NOT EXISTS analytics_cash_totals (
    user_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    deposits TEXT NOT NULL,
    initiated_withdrawals TEXT NOT NULL,
    withdrawals TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(user_id, asset_id)
);

CREATE TABLE IF NOT EXISTS analytics_marks (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    asset_id TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    price_numerator TEXT NOT NULL,
    price_scale TEXT NOT NULL,
    event_time INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analytics_marks_lookup
    ON analytics_marks(asset_id, quote_asset, event_time DESC, sequence DESC);

CREATE TABLE IF NOT EXISTS analytics_pool_totals (
    pool_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    lp_deposits TEXT NOT NULL,
    lp_withdrawals TEXT NOT NULL,
    swap_volume_quote TEXT NOT NULL,
    lp_fee_quote TEXT NOT NULL,
    protocol_fee_quote TEXT NOT NULL,
    backstop_fee_quote TEXT NOT NULL,
    volatility_fee_quote TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(pool_id, asset_id)
);

CREATE TABLE IF NOT EXISTS analytics_pool_snapshots (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    pool_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    quote_asset TEXT NOT NULL,
    nav_quote TEXT NOT NULL,
    tvl_quote TEXT NOT NULL,
    inventory_quantity TEXT NOT NULL,
    inventory_cost_quote TEXT NOT NULL,
    inventory_value_quote TEXT NOT NULL,
    event_time INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analytics_pool_snapshot_lookup
    ON analytics_pool_snapshots(pool_id, event_time DESC, sequence DESC);

CREATE TABLE IF NOT EXISTS analytics_coverage (
    subject_id TEXT PRIMARY KEY,
    coverage_start INTEGER NOT NULL,
    opening_snapshot INTEGER NOT NULL,
    trades_covered INTEGER NOT NULL,
    cash_flows_covered INTEGER NOT NULL,
    marks_covered INTEGER NOT NULL,
    fill_metrics_covered INTEGER NOT NULL
);
"#;

#[derive(Debug)]
struct PositionState {
    user_id: String,
    asset_id: String,
    quote_asset: String,
    quantity: u64,
    cost_quote: u128,
    realized_pnl_quote: i128,
    volume_quote: u128,
    fee_total_quote: u128,
    fills: u64,
    requested_amount_out: u128,
    filled_amount_out: u128,
    quoted_amount_out: u128,
    updated_at: u64,
}

fn insert_event(
    tx: &Transaction<'_>,
    event_id: &str,
    kind: &str,
    subject_id: &str,
    asset_id: Option<&str>,
    pool_id: Option<&str>,
    event_time: u64,
    payload: &str,
) -> Result<bool> {
    if event_id.is_empty() {
        bail!("event_id must not be empty");
    }
    Ok(tx.execute(
        "INSERT OR IGNORE INTO analytics_events
         (event_id, kind, subject_id, asset_id, pool_id, event_time, payload)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            event_id,
            kind,
            subject_id,
            asset_id,
            pool_id,
            to_i64(event_time)?,
            payload
        ],
    )? == 1)
}

fn duplicate_result(
    tx: &Transaction<'_>,
    event_id: &str,
    kind: &str,
    payload: &str,
) -> Result<bool> {
    let existing: (String, String) = tx.query_row(
        "SELECT kind, payload FROM analytics_events WHERE event_id = ?1",
        [event_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    if existing.0 != kind || existing.1 != payload {
        bail!("event_id {event_id} was replayed with different content");
    }
    Ok(false)
}

fn load_position(
    tx: &Transaction<'_>,
    user: &str,
    asset: &str,
    quote: &str,
) -> Result<Option<PositionState>> {
    tx.query_row(
        "SELECT user_id, asset_id, quote_asset, quantity, cost_quote, realized_pnl_quote,
                volume_quote, fee_total_quote, fills, requested_amount_out,
                filled_amount_out, quoted_amount_out, updated_at
         FROM analytics_positions WHERE user_id = ?1 AND asset_id = ?2 AND quote_asset = ?3",
        params![user, asset, quote],
        map_position_state,
    )
    .optional()
    .map_err(Into::into)
}

fn latest_mark_for_asset(
    tx: &Transaction<'_>,
    asset: &str,
    as_of: u64,
) -> Result<Option<(String, u128, u128)>> {
    tx.query_row(
        "SELECT quote_asset, price_numerator, price_scale
         FROM analytics_marks WHERE asset_id = ?1 AND event_time <= ?2
         ORDER BY event_time DESC, sequence DESC LIMIT 1",
        params![asset, to_i64(as_of)?],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                parse_u128_col(row, 1)?,
                parse_u128_col(row, 2)?,
            ))
        },
    )
    .optional()
    .map_err(Into::into)
}

fn empty_position(user: &str, asset: &str, quote: &str) -> PositionState {
    PositionState {
        user_id: user.to_owned(),
        asset_id: asset.to_owned(),
        quote_asset: quote.to_owned(),
        quantity: 0,
        cost_quote: 0,
        realized_pnl_quote: 0,
        volume_quote: 0,
        fee_total_quote: 0,
        fills: 0,
        requested_amount_out: 0,
        filled_amount_out: 0,
        quoted_amount_out: 0,
        updated_at: 0,
    }
}

fn acquire(
    tx: &Transaction<'_>,
    user: &str,
    asset: &str,
    quote: &str,
    amount: u64,
    cost: u128,
    at: u64,
) -> Result<()> {
    let mut position = load_position(tx, user, asset, quote)?
        .unwrap_or_else(|| empty_position(user, asset, quote));
    position.quantity = position
        .quantity
        .checked_add(amount)
        .ok_or_else(|| anyhow!("position quantity overflow"))?;
    position.cost_quote = checked_uadd(position.cost_quote, cost)?;
    position.updated_at = position.updated_at.max(at);
    save_position(tx, &position)
}

fn dispose(
    tx: &Transaction<'_>,
    user: &str,
    asset: &str,
    quote: &str,
    amount: u64,
    proceeds: u128,
    fee: u128,
    at: u64,
) -> Result<()> {
    let mut position = load_position(tx, user, asset, quote)?
        .ok_or_else(|| anyhow!("cannot dispose untracked position {user}/{asset}"))?;
    if amount > position.quantity {
        bail!("disposal exceeds position for {user}/{asset}");
    }
    let allocated_cost = if amount == position.quantity {
        position.cost_quote
    } else {
        position
            .cost_quote
            .checked_mul(u128::from(amount))
            .ok_or_else(|| anyhow!("WAC allocation overflow"))?
            / u128::from(position.quantity)
    };
    let net_proceeds = i128::try_from(proceeds)?
        .checked_sub(i128::try_from(fee)?)
        .ok_or_else(|| anyhow!("net proceeds overflow"))?;
    let delta = net_proceeds
        .checked_sub(i128::try_from(allocated_cost)?)
        .ok_or_else(|| anyhow!("realized PnL overflow"))?;
    position.quantity -= amount;
    position.cost_quote -= allocated_cost;
    position.realized_pnl_quote = checked_iadd(position.realized_pnl_quote, delta)?;
    position.updated_at = position.updated_at.max(at);
    save_position(tx, &position)
}

fn update_fill_metrics(tx: &Transaction<'_>, swap: &FinalizedSwap) -> Result<()> {
    let mut position = load_position(tx, &swap.user_id, &swap.asset_out, &swap.quote_asset)?
        .ok_or_else(|| anyhow!("acquired position projection missing"))?;
    position.volume_quote = checked_uadd(position.volume_quote, swap.quote_value)?;
    position.fee_total_quote = checked_uadd(position.fee_total_quote, swap.total_fee()?)?;
    position.fills = position
        .fills
        .checked_add(1)
        .ok_or_else(|| anyhow!("fill count overflow"))?;
    position.filled_amount_out =
        checked_uadd(position.filled_amount_out, u128::from(swap.amount_out))?;
    if let Some(value) = swap.requested_amount_out {
        position.requested_amount_out =
            checked_uadd(position.requested_amount_out, u128::from(value))?;
    }
    if let Some(value) = swap.quoted_amount_out {
        position.quoted_amount_out = checked_uadd(position.quoted_amount_out, u128::from(value))?;
    }
    save_position(tx, &position)
}

fn save_position(tx: &Transaction<'_>, position: &PositionState) -> Result<()> {
    tx.execute(
        "INSERT INTO analytics_positions
         (user_id, asset_id, quote_asset, quantity, cost_quote, realized_pnl_quote,
          volume_quote, fee_total_quote, fills, requested_amount_out, filled_amount_out,
          quoted_amount_out, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
         ON CONFLICT(user_id, asset_id, quote_asset) DO UPDATE SET
           quantity=excluded.quantity, cost_quote=excluded.cost_quote,
           realized_pnl_quote=excluded.realized_pnl_quote, volume_quote=excluded.volume_quote,
           fee_total_quote=excluded.fee_total_quote, fills=excluded.fills,
           requested_amount_out=excluded.requested_amount_out,
           filled_amount_out=excluded.filled_amount_out,
           quoted_amount_out=excluded.quoted_amount_out, updated_at=excluded.updated_at",
        params![
            position.user_id,
            position.asset_id,
            position.quote_asset,
            to_i64(position.quantity)?,
            u128_text(position.cost_quote),
            i128_text(position.realized_pnl_quote),
            u128_text(position.volume_quote),
            u128_text(position.fee_total_quote),
            to_i64(position.fills)?,
            u128_text(position.requested_amount_out),
            u128_text(position.filled_amount_out),
            u128_text(position.quoted_amount_out),
            to_i64(position.updated_at)?,
        ],
    )?;
    Ok(())
}

fn map_position_state(row: &rusqlite::Row<'_>) -> rusqlite::Result<PositionState> {
    Ok(PositionState {
        user_id: row.get(0)?,
        asset_id: row.get(1)?,
        quote_asset: row.get(2)?,
        quantity: from_i64(row.get(3)?)?,
        cost_quote: parse_u128_col(row, 4)?,
        realized_pnl_quote: parse_i128_col(row, 5)?,
        volume_quote: parse_u128_col(row, 6)?,
        fee_total_quote: parse_u128_col(row, 7)?,
        fills: from_i64(row.get(8)?)?,
        requested_amount_out: parse_u128_col(row, 9)?,
        filled_amount_out: parse_u128_col(row, 10)?,
        quoted_amount_out: parse_u128_col(row, 11)?,
        updated_at: from_i64(row.get(12)?)?,
    })
}

fn position_analytics(
    connection: &Connection,
    state: PositionState,
    as_of: u64,
) -> Result<PositionAnalytics> {
    let mark = connection
        .query_row(
            "SELECT price_numerator, price_scale, event_time FROM analytics_marks
             WHERE asset_id = ?1 AND quote_asset = ?2 AND event_time <= ?3
             ORDER BY event_time DESC, sequence DESC LIMIT 1",
            params![state.asset_id, state.quote_asset, to_i64(as_of)?],
            |row| {
                Ok((
                    parse_u128_col(row, 0)?,
                    parse_u128_col(row, 1)?,
                    from_i64(row.get(2)?)?,
                ))
            },
        )
        .optional()?;
    let unrealized = mark
        .map(|(price, scale, _)| {
            let value = u128::from(state.quantity)
                .checked_mul(price)
                .ok_or_else(|| anyhow!("mark valuation overflow"))?
                / scale;
            i128::try_from(value)?
                .checked_sub(i128::try_from(state.cost_quote)?)
                .ok_or_else(|| anyhow!("unrealized PnL overflow"))
        })
        .transpose()?;
    let total = unrealized
        .map(|value| checked_iadd(state.realized_pnl_quote, value))
        .transpose()?;
    let (mark_price_numerator, mark_price_scale, mark_time) = match mark {
        Some((price, scale, time)) => (Some(price), Some(scale), Some(time)),
        None => (None, None, None),
    };
    Ok(PositionAnalytics {
        asset_id: state.asset_id,
        quote_asset: state.quote_asset,
        quantity: state.quantity,
        cost_quote: state.cost_quote,
        average_cost_numerator: state.cost_quote,
        average_cost_scale: u128::from(state.quantity.max(1)),
        realized_pnl_quote: state.realized_pnl_quote,
        unrealized_pnl_quote: unrealized,
        total_pnl_quote: total,
        mark_price_numerator,
        mark_price_scale,
        mark_time,
        volume_quote: state.volume_quote,
        fee_total_quote: state.fee_total_quote,
        fills: state.fills,
        requested_amount_out: state.requested_amount_out,
        filled_amount_out: state.filled_amount_out,
        quoted_amount_out: state.quoted_amount_out,
        updated_at: state.updated_at,
    })
}

fn increment_cash_total(
    tx: &Transaction<'_>,
    user: &str,
    asset: &str,
    column: &str,
    amount: u128,
    at: u64,
) -> Result<()> {
    let (mut deposits, mut initiated, mut withdrawals) = load_cash_totals(tx, user, asset)?;
    match column {
        "deposits" => deposits = checked_uadd(deposits, amount)?,
        "initiated_withdrawals" => initiated = checked_uadd(initiated, amount)?,
        "withdrawals" => withdrawals = checked_uadd(withdrawals, amount)?,
        _ => bail!("invalid cash total column"),
    }
    tx.execute(
        "INSERT INTO analytics_cash_totals
         (user_id, asset_id, deposits, initiated_withdrawals, withdrawals, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(user_id, asset_id) DO UPDATE SET deposits=excluded.deposits,
           initiated_withdrawals=excluded.initiated_withdrawals,
           withdrawals=excluded.withdrawals, updated_at=MAX(updated_at, excluded.updated_at)",
        params![
            user,
            asset,
            u128_text(deposits),
            u128_text(initiated),
            u128_text(withdrawals),
            to_i64(at)?
        ],
    )?;
    Ok(())
}

fn load_cash_totals(
    connection: &Connection,
    user: &str,
    asset: &str,
) -> Result<(u128, u128, u128)> {
    Ok(connection
        .query_row(
            "SELECT deposits, initiated_withdrawals, withdrawals
             FROM analytics_cash_totals WHERE user_id=?1 AND asset_id=?2",
            params![user, asset],
            |row| {
                Ok((
                    parse_u128_col(row, 0)?,
                    parse_u128_col(row, 1)?,
                    parse_u128_col(row, 2)?,
                ))
            },
        )
        .optional()?
        .unwrap_or((0, 0, 0)))
}

fn ensure_pool_totals(tx: &Transaction<'_>, pool: &str, asset: &str, at: u64) -> Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO analytics_pool_totals
         (pool_id, asset_id, lp_deposits, lp_withdrawals, swap_volume_quote,
          lp_fee_quote, protocol_fee_quote, backstop_fee_quote, volatility_fee_quote, updated_at)
         VALUES (?1, ?2, '0', '0', '0', '0', '0', '0', '0', ?3)",
        params![pool, asset, to_i64(at)?],
    )?;
    Ok(())
}

fn increment_pool_total(
    tx: &Transaction<'_>,
    pool: &str,
    asset: &str,
    column: &str,
    amount: u128,
    at: u64,
) -> Result<()> {
    ensure_pool_totals(tx, pool, asset, at)?;
    let mut totals = load_pool_totals(tx, pool, asset)?;
    match column {
        "lp_deposits" => totals.0 = checked_uadd(totals.0, amount)?,
        "lp_withdrawals" => totals.1 = checked_uadd(totals.1, amount)?,
        _ => bail!("invalid pool total column"),
    }
    save_pool_totals(tx, pool, asset, totals, at)
}

fn update_pool_swap_totals(tx: &Transaction<'_>, swap: &FinalizedSwap) -> Result<()> {
    ensure_pool_totals(tx, &swap.pool_id, &swap.asset_out, swap.event_time)?;
    let mut totals = load_pool_totals(tx, &swap.pool_id, &swap.asset_out)?;
    totals.2 = checked_uadd(totals.2, swap.quote_value)?;
    totals.3 = checked_uadd(totals.3, swap.lp_fee_quote)?;
    totals.4 = checked_uadd(totals.4, swap.protocol_fee_quote)?;
    totals.5 = checked_uadd(totals.5, swap.backstop_fee_quote)?;
    totals.6 = checked_uadd(totals.6, swap.volatility_fee_quote)?;
    save_pool_totals(tx, &swap.pool_id, &swap.asset_out, totals, swap.event_time)
}

type PoolTotals = (u128, u128, u128, u128, u128, u128, u128);

fn load_pool_totals(connection: &Connection, pool: &str, asset: &str) -> Result<PoolTotals> {
    Ok(connection
        .query_row(
            "SELECT lp_deposits, lp_withdrawals, swap_volume_quote, lp_fee_quote,
                    protocol_fee_quote, backstop_fee_quote, volatility_fee_quote
             FROM analytics_pool_totals WHERE pool_id=?1 AND asset_id=?2",
            params![pool, asset],
            |row| {
                Ok((
                    parse_u128_col(row, 0)?,
                    parse_u128_col(row, 1)?,
                    parse_u128_col(row, 2)?,
                    parse_u128_col(row, 3)?,
                    parse_u128_col(row, 4)?,
                    parse_u128_col(row, 5)?,
                    parse_u128_col(row, 6)?,
                ))
            },
        )
        .optional()?
        .unwrap_or((0, 0, 0, 0, 0, 0, 0)))
}

fn save_pool_totals(
    tx: &Transaction<'_>,
    pool: &str,
    asset: &str,
    values: PoolTotals,
    at: u64,
) -> Result<()> {
    tx.execute(
        "UPDATE analytics_pool_totals SET lp_deposits=?3, lp_withdrawals=?4,
         swap_volume_quote=?5, lp_fee_quote=?6, protocol_fee_quote=?7,
         backstop_fee_quote=?8, volatility_fee_quote=?9, updated_at=MAX(updated_at, ?10)
         WHERE pool_id=?1 AND asset_id=?2",
        params![
            pool,
            asset,
            u128_text(values.0),
            u128_text(values.1),
            u128_text(values.2),
            u128_text(values.3),
            u128_text(values.4),
            u128_text(values.5),
            u128_text(values.6),
            to_i64(at)?,
        ],
    )?;
    Ok(())
}

fn update_coverage(
    tx: &Transaction<'_>,
    subject: &str,
    at: u64,
    trades: bool,
    cash: bool,
    marks: bool,
    fills: bool,
) -> Result<()> {
    tx.execute(
        "INSERT INTO analytics_coverage
         (subject_id, coverage_start, opening_snapshot, trades_covered,
          cash_flows_covered, marks_covered, fill_metrics_covered)
         VALUES (?1, ?2, 0, ?3, ?4, ?5, ?6)
         ON CONFLICT(subject_id) DO UPDATE SET
          coverage_start=MIN(coverage_start, excluded.coverage_start),
          trades_covered=MAX(trades_covered, excluded.trades_covered),
          cash_flows_covered=MAX(cash_flows_covered, excluded.cash_flows_covered),
          marks_covered=MAX(marks_covered, excluded.marks_covered),
          fill_metrics_covered=MAX(fill_metrics_covered, excluded.fill_metrics_covered)",
        params![subject, to_i64(at)?, trades, cash, marks, fills],
    )?;
    Ok(())
}

fn load_coverage(
    connection: &Connection,
    subject: &str,
    as_of: u64,
    all_marked: bool,
) -> Result<CoverageMetadata> {
    let row = connection
        .query_row(
            "SELECT coverage_start, opening_snapshot, trades_covered,
                    cash_flows_covered, marks_covered, fill_metrics_covered
             FROM analytics_coverage WHERE subject_id=?1",
            [subject],
            |row| {
                Ok((
                    from_i64(row.get(0)?)?,
                    row.get::<_, bool>(1)?,
                    row.get::<_, bool>(2)?,
                    row.get::<_, bool>(3)?,
                    row.get::<_, bool>(4)?,
                    row.get::<_, bool>(5)?,
                ))
            },
        )
        .optional()?;
    let Some((start, opening, trades, cash, explicit_marks, fills)) = row else {
        return Ok(CoverageMetadata {
            partial: true,
            opening_snapshot: false,
            coverage_start: None,
            as_of,
            trades_covered: false,
            cash_flows_covered: false,
            marks_covered: false,
            fill_metrics_covered: false,
        });
    };
    // Mark coverage is query-time and requires every non-zero position to have an event-time mark.
    let marks = explicit_marks || all_marked;
    Ok(CoverageMetadata {
        partial: opening || !trades || !cash || !marks,
        opening_snapshot: opening,
        coverage_start: Some(start),
        as_of,
        trades_covered: trades,
        cash_flows_covered: cash,
        marks_covered: marks,
        fill_metrics_covered: fills,
    })
}

fn validate_page(page: Pagination) -> Result<()> {
    if page.limit == 0 || page.limit > 500 {
        bail!("pagination limit must be between 1 and 500");
    }
    Ok(())
}

fn checked_uadd(left: u128, right: u128) -> Result<u128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("unsigned accounting overflow"))
}

fn checked_iadd(left: i128, right: i128) -> Result<i128> {
    left.checked_add(right)
        .ok_or_else(|| anyhow!("signed accounting overflow"))
}

fn u128_text(value: u128) -> String {
    value.to_string()
}

fn i128_text(value: i128) -> String {
    value.to_string()
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds SQLite INTEGER range")
}

fn from_i64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn parse_u128_col(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<u128> {
    let value = row.get::<_, String>(index)?;
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn parse_i128_col(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<i128> {
    let value = row.get::<_, String>(index)?;
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opening(store: &AnalyticsStore, asset: &str, quantity: u64, cost: u128) {
        store
            .record_opening_position(&OpeningPosition {
                event_id: format!("opening-{asset}"),
                user_id: "user".into(),
                asset_id: asset.into(),
                quote_asset: "USD".into(),
                quantity,
                cost_quote: cost,
                realized_pnl_quote: 0,
                as_of: 1,
            })
            .unwrap();
    }

    fn swap(
        id: &str,
        asset_in: &str,
        asset_out: &str,
        amount_in: u64,
        amount_out: u64,
        value: u128,
    ) -> FinalizedSwap {
        FinalizedSwap {
            event_id: id.into(),
            user_id: "user".into(),
            pool_id: "pool".into(),
            asset_in: asset_in.into(),
            asset_out: asset_out.into(),
            quote_asset: "USD".into(),
            amount_in,
            amount_out,
            quote_value: value,
            lp_fee_quote: 2,
            protocol_fee_quote: 1,
            backstop_fee_quote: 0,
            volatility_fee_quote: 0,
            requested_amount_out: Some(amount_out + 1),
            quoted_amount_out: Some(amount_out + 2),
            event_time: 10,
        }
    }

    #[test]
    fn weighted_average_cost_and_realized_pnl_are_integer_exact() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        opening(&store, "A", 10, 100);
        opening(&store, "USD-CASH", 1_000, 1_000);
        store
            .record_swap(&swap("buy-a", "USD-CASH", "A", 100, 10, 100))
            .unwrap();
        store
            .record_swap(&swap("sell-a", "A", "B", 5, 50, 75))
            .unwrap();
        let positions = store
            .positions("user", "USD", 100, Pagination::default())
            .unwrap();
        let a = positions.items.iter().find(|p| p.asset_id == "A").unwrap();
        assert_eq!(a.quantity, 15);
        assert_eq!(a.cost_quote, 150);
        assert_eq!(a.average_cost_numerator, 150);
        assert_eq!(a.average_cost_scale, 15);
        assert_eq!(a.realized_pnl_quote, 22); // 75 proceeds - 3 fees - 50 WAC.
    }

    #[test]
    fn exact_replay_is_idempotent_and_conflicting_replay_is_rejected() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        opening(&store, "A", 10, 100);
        let event = swap("swap-1", "A", "B", 5, 50, 75);
        assert!(store.record_swap(&event).unwrap());
        assert!(!store.record_swap(&event).unwrap());
        let mut conflicting = event;
        conflicting.amount_out += 1;
        assert!(store.record_swap(&conflicting).is_err());
        let summary = store.user_summary("user", "USD", 100).unwrap();
        assert_eq!(summary.volume_quote, 75);
        assert_eq!(summary.fee_total_quote, 3);
        assert_eq!(summary.fills, 1);
    }

    #[test]
    fn cash_flows_count_redeem_only_as_completed_withdrawal() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        for (id, kind, amount) in [
            ("fund", CashFlowKind::Fund, 500),
            ("init", CashFlowKind::InitRedeem, 120),
            ("redeem", CashFlowKind::Redeem, 100),
        ] {
            store
                .record_cash_flow(&CashFlow {
                    event_id: id.into(),
                    kind,
                    user_id: "user".into(),
                    asset_id: "USD".into(),
                    amount,
                    event_time: 2,
                })
                .unwrap();
        }
        let summary = store.user_summary("user", "USD", 100).unwrap();
        assert_eq!(summary.deposits, 500);
        assert_eq!(summary.initiated_withdrawals, 120);
        assert_eq!(summary.withdrawals, 100);
        assert!(summary.coverage.cash_flows_covered);
    }

    #[test]
    fn fees_marks_pool_totals_and_inventory_pnl_are_reported() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        opening(&store, "A", 10, 100);
        store
            .record_swap(&swap("swap", "A", "B", 5, 50, 75))
            .unwrap();
        store
            .record_lp_cash_flow(&LpCashFlow {
                event_id: "lp-in".into(),
                kind: LpCashFlowKind::Deposit,
                lp_id: "lp".into(),
                pool_id: "pool".into(),
                asset_id: "B".into(),
                amount: 1_000,
                shares: 900,
                event_time: 11,
            })
            .unwrap();
        store
            .record_mark(&OracleMark {
                event_id: "mark".into(),
                asset_id: "B".into(),
                quote_asset: "USD".into(),
                price_numerator: 2,
                price_scale: 1,
                event_time: 12,
            })
            .unwrap();
        store
            .record_pool_snapshot(&PoolSnapshotInput {
                event_id: "snapshot".into(),
                pool_id: "pool".into(),
                asset_id: "B".into(),
                quote_asset: "USD".into(),
                nav_quote: 1_500,
                tvl_quote: 1_400,
                inventory_quantity: 50,
                inventory_cost_quote: 75,
                inventory_value_quote: 100,
                event_time: 12,
            })
            .unwrap();
        let pool = store.pool_summary("pool", 12).unwrap().unwrap();
        assert_eq!(pool.inventory_pnl_quote, 25);
        assert_eq!(pool.lp_deposits, 1_000);
        assert_eq!(pool.swap_volume_quote, 75);
        assert_eq!(pool.lp_fee_quote, 2);
        let summary = store.user_summary("user", "USD", 12).unwrap();
        assert_eq!(summary.fee_total_quote, 3);
        assert_eq!(summary.unrealized_pnl_quote, None); // A has no mark, so aggregate is explicit.
    }

    #[test]
    fn opening_snapshot_and_pagination_expose_partial_coverage() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        opening(&store, "A", 1, 10);
        opening(&store, "B", 1, 20);
        let first = store
            .positions(
                "user",
                "USD",
                5,
                Pagination {
                    offset: 0,
                    limit: 1,
                },
            )
            .unwrap();
        assert_eq!(first.items.len(), 1);
        assert_eq!(first.next_offset, Some(1));
        let summary = store.user_summary("user", "USD", 5).unwrap();
        assert!(summary.coverage.partial);
        assert!(summary.coverage.opening_snapshot);
        assert_eq!(summary.coverage.coverage_start, Some(1));
        assert!(!summary.coverage.marks_covered);
    }

    #[test]
    fn note_cursor_is_stable_within_a_block() {
        let store = AnalyticsStore::open(":memory:").unwrap();
        let cursor = NoteCursor {
            block_num: 9,
            note_id: "note-02".into(),
        };
        store.set_note_cursor("vault", &cursor).unwrap();
        assert_eq!(store.note_cursor("vault").unwrap(), cursor);
        assert!(
            NoteCursor {
                block_num: 9,
                note_id: "note-03".into()
            } > cursor
        );
    }
}
