use std::{
    collections::HashMap,
    env,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use dashmap::DashMap;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter, types::Value};
use serde::Serialize;
use uuid::Uuid;

use crate::{
    message_broker::message_broker::{MessageBroker, OraclePriceEvent, TradeEvent},
    order::{OrderSnapshot, OrderStatus, OrderUpdate},
};

pub const CANDLE_INTERVALS: [u64; 5] = [60, 300, 900, 3_600, 14_400];
pub const PRICE_SCALE: u128 = 1_000_000_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct Candle {
    pub source: String,
    pub pair: String,
    pub interval_secs: u64,
    pub bucket_start: u64,
    pub open: u64,
    pub high: u64,
    pub low: u64,
    pub close: u64,
    pub volume: u64,
    pub trade_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradeRecord {
    pub order_id: String,
    pub user_id: String,
    pub pair: String,
    pub asset_in: String,
    pub asset_out: String,
    pub amount_in: u64,
    pub amount_out: u64,
    pub price: u64,
    pub oracle_price: Option<u64>,
    pub tx_hash: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderRecord {
    pub id: String,
    pub user_id: String,
    pub asset_in: String,
    pub amount_in: u64,
    pub asset_out: String,
    pub min_amount_out: u64,
    pub order_type: String,
    pub status: String,
    pub amount_out: Option<u64>,
    pub tx_hash: Option<String>,
    pub failure_reason: Option<String>,
    pub created_at: u64,
    pub processed_at: Option<u64>,
    pub executed_at: Option<u64>,
    pub failed_at: Option<u64>,
    pub last_updated_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderEventRecord {
    pub seq: u64,
    pub order_id: String,
    pub status: String,
    pub failure_reason: Option<String>,
    pub amount_out: Option<u64>,
    pub tx_hash: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AssetVolume {
    pub asset: String,
    pub volume: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TradingStats {
    pub trades_24h: u64,
    pub trades_all_time: u64,
    pub volume_24h: Vec<AssetVolume>,
    pub volume_all_time: Vec<AssetVolume>,
    pub executed_orders: u64,
    pub failed_orders: u64,
    pub fill_rate: f64,
    pub failures_by_reason: HashMap<String, u64>,
}

pub struct HistoryStore {
    connection: Mutex<Connection>,
}

pub fn start_history_service(
    history: std::sync::Arc<HistoryStore>,
    message_broker: std::sync::Arc<MessageBroker>,
) {
    let prices = std::sync::Arc::new(DashMap::<String, u64>::new());
    let mut order_rx = message_broker.subscribe_order_updates();
    let mut price_rx = message_broker.subscribe_oracle_prices();

    {
        let history = history.clone();
        let message_broker = message_broker.clone();
        let prices = prices.clone();
        tokio::spawn(async move {
            loop {
                match order_rx.recv().await {
                    Ok(update) => {
                        let snapshot = update.snapshot();
                        if let Err(error) = history.persist_order_update(&update) {
                            tracing::error!(%error, order_id = %snapshot.id, "failed to persist order");
                            continue;
                        }
                        if snapshot.status == OrderStatus::Processed {
                            let asset_in = snapshot.details.asset_in;
                            let asset_out = snapshot.details.asset_out;
                            let oracle_price = prices
                                .get(&asset_in.to_hex())
                                .zip(prices.get(&asset_out.to_hex()))
                                .and_then(|(price_in, price_out)| {
                                    canonical_oracle_price(*price_in, *price_out)
                                });
                            match history.record_trade(&snapshot, oracle_price) {
                                Ok(Some(trade)) => {
                                    let _ = message_broker.broadcast_trade(TradeEvent {
                                        order_id: trade.order_id,
                                        pair: trade.pair,
                                        asset_in: trade.asset_in,
                                        asset_out: trade.asset_out,
                                        amount_in: trade.amount_in,
                                        amount_out: trade.amount_out,
                                        price: trade.price,
                                        timestamp: trade.timestamp,
                                    });
                                }
                                Ok(None) => {}
                                Err(error) => tracing::error!(
                                    %error,
                                    order_id = %snapshot.id,
                                    "failed to persist trade"
                                ),
                            }
                        } else if snapshot.status == OrderStatus::Executed
                            && let Some(tx_hash) = &snapshot.tx_hash
                            && let Err(error) = history.mark_trade_executed(snapshot.id, tx_hash)
                        {
                            tracing::error!(
                                %error,
                                order_id = %snapshot.id,
                                "failed to attach transaction hash to trade"
                            );
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "history order subscriber lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    tokio::spawn(async move {
        loop {
            match price_rx.recv().await {
                Ok(event) => {
                    prices.insert(event.faucet_id.clone(), event.price);
                    if let Err(error) = history.record_oracle_price(&event) {
                        tracing::error!(%error, "failed to persist oracle candle");
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "history oracle subscriber lagged");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}

impl HistoryStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open history sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                asset_in TEXT NOT NULL,
                amount_in INTEGER NOT NULL,
                asset_out TEXT NOT NULL,
                min_amount_out INTEGER NOT NULL,
                order_type TEXT NOT NULL,
                status TEXT NOT NULL,
                amount_out INTEGER,
                tx_hash TEXT,
                failure_reason TEXT,
                created_at INTEGER NOT NULL,
                processed_at INTEGER,
                executed_at INTEGER,
                failed_at INTEGER,
                last_updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_orders_user_updated
                ON orders(user_id, last_updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_orders_status_updated
                ON orders(status, last_updated_at DESC);

            CREATE TABLE IF NOT EXISTS order_events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL,
                status TEXT NOT NULL,
                failure_reason TEXT,
                amount_out INTEGER,
                tx_hash TEXT,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_order_events_order_seq
                ON order_events(order_id, seq);

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT NOT NULL UNIQUE,
                user_id TEXT NOT NULL,
                pair TEXT NOT NULL,
                asset_in TEXT NOT NULL,
                asset_out TEXT NOT NULL,
                amount_in INTEGER NOT NULL,
                amount_out INTEGER NOT NULL,
                price INTEGER NOT NULL,
                oracle_price INTEGER,
                tx_hash TEXT,
                timestamp INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_trades_pair_time
                ON trades(pair, timestamp DESC);

            CREATE TABLE IF NOT EXISTS candles (
                source TEXT NOT NULL,
                pair TEXT NOT NULL,
                interval_secs INTEGER NOT NULL,
                bucket_start INTEGER NOT NULL,
                open INTEGER NOT NULL,
                high INTEGER NOT NULL,
                low INTEGER NOT NULL,
                close INTEGER NOT NULL,
                volume INTEGER NOT NULL DEFAULT 0,
                trade_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(source, pair, interval_secs, bucket_start)
            );
            CREATE INDEX IF NOT EXISTS idx_candles_lookup
                ON candles(source, pair, interval_secs, bucket_start DESC);
            "#,
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn open_from_env() -> Result<Self> {
        let path = env::var("HISTORY_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_string());
            format!("history.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("history database lock poisoned"))
    }

    pub fn persist_order_update(&self, update: &OrderUpdate) -> Result<()> {
        let snapshot = update.snapshot();
        let failure_reason = snapshot
            .failure_reason
            .as_ref()
            .map(|reason| format!("{reason:?}").to_ascii_lowercase());
        let order_type = format!("{:?}", snapshot.order_type).to_ascii_lowercase();
        let timing = &snapshot.timing;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            r#"
            INSERT INTO orders (
                id, user_id, asset_in, amount_in, asset_out, min_amount_out,
                order_type, status, amount_out, tx_hash, failure_reason,
                created_at, processed_at, executed_at, failed_at, last_updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                amount_out = COALESCE(excluded.amount_out, orders.amount_out),
                tx_hash = COALESCE(excluded.tx_hash, orders.tx_hash),
                failure_reason = COALESCE(excluded.failure_reason, orders.failure_reason),
                processed_at = COALESCE(excluded.processed_at, orders.processed_at),
                executed_at = COALESCE(excluded.executed_at, orders.executed_at),
                failed_at = COALESCE(excluded.failed_at, orders.failed_at),
                last_updated_at = excluded.last_updated_at
            "#,
            params![
                snapshot.id.to_string(),
                snapshot.user_id.to_hex(),
                snapshot.details.asset_in.to_hex(),
                to_i64(snapshot.details.amount_in)?,
                snapshot.details.asset_out.to_hex(),
                to_i64(snapshot.details.min_amount_out)?,
                order_type,
                snapshot.status.as_str(),
                snapshot
                    .execution_result
                    .as_ref()
                    .map(|result| to_i64(result.amount_out))
                    .transpose()?,
                snapshot.tx_hash,
                failure_reason,
                timing.created_at.timestamp_millis(),
                timing.processed.map(|value| value.timestamp_millis()),
                timing.executed.map(|value| value.timestamp_millis()),
                timing.failed.map(|value| value.timestamp_millis()),
                timing.last_updated_at.timestamp_millis(),
            ],
        )?;
        transaction.execute(
            r#"
            INSERT INTO order_events (
                order_id, status, failure_reason, amount_out, tx_hash, timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                snapshot.id.to_string(),
                snapshot.status.as_str(),
                failure_reason,
                snapshot
                    .execution_result
                    .as_ref()
                    .map(|result| to_i64(result.amount_out))
                    .transpose()?,
                snapshot.tx_hash,
                timing.last_updated_at.timestamp_millis(),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn record_trade(
        &self,
        snapshot: &OrderSnapshot,
        oracle_price: Option<u64>,
    ) -> Result<Option<TradeRecord>> {
        let Some(result) = &snapshot.execution_result else {
            return Ok(None);
        };
        let price = directed_trade_price(snapshot.details.amount_in, result.amount_out)?;
        let timestamp = snapshot
            .timing
            .processed
            .unwrap_or(snapshot.timing.last_updated_at)
            .timestamp_millis() as u64;
        let pair = format!(
            "{}/{}",
            snapshot.details.asset_in.to_hex(),
            snapshot.details.asset_out.to_hex()
        );
        let trade = TradeRecord {
            order_id: snapshot.id.to_string(),
            user_id: snapshot.user_id.to_hex(),
            pair: pair.clone(),
            asset_in: snapshot.details.asset_in.to_hex(),
            asset_out: snapshot.details.asset_out.to_hex(),
            amount_in: snapshot.details.amount_in,
            amount_out: result.amount_out,
            price,
            oracle_price,
            tx_hash: snapshot.tx_hash.clone(),
            timestamp,
        };
        let connection = self.connection()?;
        let inserted = connection.execute(
            r#"
            INSERT OR IGNORE INTO trades (
                order_id, user_id, pair, asset_in, asset_out, amount_in,
                amount_out, price, oracle_price, tx_hash, timestamp
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                trade.order_id,
                trade.user_id,
                trade.pair,
                trade.asset_in,
                trade.asset_out,
                to_i64(trade.amount_in)?,
                to_i64(trade.amount_out)?,
                to_i64(trade.price)?,
                trade.oracle_price.map(to_i64).transpose()?,
                trade.tx_hash,
                to_i64(trade.timestamp)?,
            ],
        )?;
        if inserted == 0 {
            return Ok(None);
        }

        let volume = snapshot.details.amount_in;
        upsert_all_candles(&connection, "trades", &pair, timestamp, price, volume, 1)?;
        Ok(Some(trade))
    }

    pub fn mark_trade_executed(&self, order_id: Uuid, tx_hash: &str) -> Result<()> {
        self.connection()?.execute(
            "UPDATE trades SET tx_hash = ?1 WHERE order_id = ?2",
            params![tx_hash, order_id.to_string()],
        )?;
        Ok(())
    }

    pub fn record_oracle_price(&self, event: &OraclePriceEvent) -> Result<()> {
        let source = format!("oracle:{}", event.faucet_id);
        let connection = self.connection()?;
        upsert_all_candles(
            &connection,
            &source,
            &event.faucet_id,
            event.timestamp,
            event.price,
            0,
            0,
        )
    }

    pub fn candles(
        &self,
        source: &str,
        pair: Option<&str>,
        interval_secs: u64,
        from: Option<u64>,
        to: Option<u64>,
        limit: u64,
    ) -> Result<Vec<Candle>> {
        let mut sql = String::from(
            "SELECT source, pair, interval_secs, bucket_start, open, high, low, close, volume, trade_count
             FROM candles WHERE source = ? AND interval_secs = ?",
        );
        let mut values = vec![
            Value::Text(source.to_string()),
            Value::Integer(to_i64(interval_secs)?),
        ];
        if let Some(pair) = pair {
            sql.push_str(" AND pair = ?");
            values.push(Value::Text(pair.to_string()));
        }
        if let Some(from) = from {
            sql.push_str(" AND bucket_start >= ?");
            values.push(Value::Integer(to_i64(timestamp_seconds(from))?));
        }
        if let Some(to) = to {
            sql.push_str(" AND bucket_start <= ?");
            values.push(Value::Integer(to_i64(timestamp_seconds(to))?));
        }
        sql.push_str(" ORDER BY bucket_start DESC LIMIT ?");
        values.push(Value::Integer(to_i64(limit)?));

        let connection = self.connection()?;
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(values), map_candle)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn trades(
        &self,
        pair: Option<&str>,
        user_id: Option<&str>,
        before: Option<u64>,
        limit: u64,
    ) -> Result<Vec<TradeRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT order_id, user_id, pair, asset_in, asset_out, amount_in,
                   amount_out, price, oracle_price, tx_hash, timestamp
            FROM trades
            WHERE (?1 IS NULL OR pair = ?1)
              AND (?2 IS NULL OR user_id = ?2)
              AND (?3 IS NULL OR timestamp < ?3)
            ORDER BY timestamp DESC
            LIMIT ?4
            "#,
        )?;
        let rows = statement.query_map(
            params![
                pair,
                user_id,
                before.map(timestamp_millis).map(to_i64).transpose()?,
                to_i64(limit)?
            ],
            map_trade,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn orders(
        &self,
        user_id: Option<&str>,
        status: Option<&str>,
        before: Option<u64>,
        limit: u64,
    ) -> Result<Vec<OrderRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT id, user_id, asset_in, amount_in, asset_out, min_amount_out,
                   order_type, status, amount_out, tx_hash, failure_reason,
                   created_at, processed_at, executed_at, failed_at, last_updated_at
            FROM orders
            WHERE (?1 IS NULL OR user_id = ?1)
              AND (?2 IS NULL OR status = ?2)
              AND (?3 IS NULL OR last_updated_at < ?3)
            ORDER BY last_updated_at DESC
            LIMIT ?4
            "#,
        )?;
        let rows = statement.query_map(
            params![
                user_id,
                status,
                before.map(timestamp_millis).map(to_i64).transpose()?,
                to_i64(limit)?
            ],
            map_order,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn order(&self, id: Uuid) -> Result<Option<OrderRecord>> {
        self.connection()?
            .query_row(
                r#"
                SELECT id, user_id, asset_in, amount_in, asset_out, min_amount_out,
                       order_type, status, amount_out, tx_hash, failure_reason,
                       created_at, processed_at, executed_at, failed_at, last_updated_at
                FROM orders WHERE id = ?1
                "#,
                [id.to_string()],
                map_order,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn order_events(&self, id: Uuid) -> Result<Vec<OrderEventRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT seq, order_id, status, failure_reason, amount_out, tx_hash, timestamp
            FROM order_events
            WHERE order_id = ?1
            ORDER BY seq ASC
            "#,
        )?;
        let rows = statement.query_map([id.to_string()], map_order_event)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn stats(&self) -> Result<TradingStats> {
        let connection = self.connection()?;
        let since = Utc::now().timestamp_millis() - 86_400_000;
        let trades_24h = query_count(
            &connection,
            "SELECT COUNT(*) FROM trades WHERE timestamp >= ?1",
            Some(since),
        )?;
        let trades_all_time = query_count(&connection, "SELECT COUNT(*) FROM trades", None)?;
        let executed_orders = query_count(
            &connection,
            "SELECT COUNT(*) FROM orders WHERE status IN ('executed', 'settled')",
            None,
        )?;
        let failed_orders = query_count(
            &connection,
            "SELECT COUNT(*) FROM orders WHERE status = 'failed'",
            None,
        )?;
        let closed = executed_orders + failed_orders;
        let fill_rate = if closed == 0 {
            0.0
        } else {
            executed_orders as f64 / closed as f64
        };

        Ok(TradingStats {
            trades_24h,
            trades_all_time,
            volume_24h: query_volumes(&connection, Some(since))?,
            volume_all_time: query_volumes(&connection, None)?,
            executed_orders,
            failed_orders,
            fill_rate,
            failures_by_reason: query_failures(&connection)?,
        })
    }
}

pub fn bucket_start(timestamp: u64, interval_secs: u64) -> u64 {
    let timestamp = timestamp_seconds(timestamp);
    timestamp - timestamp % interval_secs
}

fn timestamp_seconds(timestamp: u64) -> u64 {
    if timestamp > 10_000_000_000 {
        timestamp / 1_000
    } else {
        timestamp
    }
}

fn timestamp_millis(timestamp: u64) -> u64 {
    if timestamp <= 10_000_000_000 {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}

fn upsert_all_candles(
    connection: &Connection,
    source: &str,
    pair: &str,
    timestamp: u64,
    price: u64,
    volume: u64,
    trade_count: u64,
) -> Result<()> {
    for interval in CANDLE_INTERVALS {
        connection.execute(
            r#"
            INSERT INTO candles (
                source, pair, interval_secs, bucket_start,
                open, high, low, close, volume, trade_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?5, ?5, ?6, ?7)
            ON CONFLICT(source, pair, interval_secs, bucket_start) DO UPDATE SET
                high = MAX(high, excluded.high),
                low = MIN(low, excluded.low),
                close = excluded.close,
                volume = volume + excluded.volume,
                trade_count = trade_count + excluded.trade_count
            "#,
            params![
                source,
                pair,
                to_i64(interval)?,
                to_i64(bucket_start(timestamp, interval))?,
                to_i64(price)?,
                to_i64(volume)?,
                to_i64(trade_count)?,
            ],
        )?;
    }
    Ok(())
}

fn directed_trade_price(amount_in: u64, amount_out: u64) -> Result<u64> {
    if amount_in == 0 || amount_out == 0 {
        return Err(anyhow!("cannot calculate price for a zero-sized fill"));
    }
    let value = (amount_out as u128)
        .checked_mul(PRICE_SCALE)
        .ok_or_else(|| anyhow!("trade price overflow"))?
        / amount_in as u128;
    u64::try_from(value).context("trade price does not fit in u64")
}

pub fn canonical_oracle_price(price0: u64, price1: u64) -> Option<u64> {
    if price1 == 0 {
        return None;
    }
    u64::try_from((price0 as u128).checked_mul(PRICE_SCALE)? / price1 as u128).ok()
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).context("value exceeds SQLite INTEGER range")
}

fn to_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn map_candle(row: &rusqlite::Row<'_>) -> rusqlite::Result<Candle> {
    Ok(Candle {
        source: row.get(0)?,
        pair: row.get(1)?,
        interval_secs: to_u64(row.get(2)?)?,
        bucket_start: to_u64(row.get(3)?)?,
        open: to_u64(row.get(4)?)?,
        high: to_u64(row.get(5)?)?,
        low: to_u64(row.get(6)?)?,
        close: to_u64(row.get(7)?)?,
        volume: to_u64(row.get(8)?)?,
        trade_count: to_u64(row.get(9)?)?,
    })
}

fn map_trade(row: &rusqlite::Row<'_>) -> rusqlite::Result<TradeRecord> {
    Ok(TradeRecord {
        order_id: row.get(0)?,
        user_id: row.get(1)?,
        pair: row.get(2)?,
        asset_in: row.get(3)?,
        asset_out: row.get(4)?,
        amount_in: to_u64(row.get(5)?)?,
        amount_out: to_u64(row.get(6)?)?,
        price: to_u64(row.get(7)?)?,
        oracle_price: row.get::<_, Option<i64>>(8)?.map(to_u64).transpose()?,
        tx_hash: row.get(9)?,
        timestamp: to_u64(row.get(10)?)?,
    })
}

fn map_order(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrderRecord> {
    Ok(OrderRecord {
        id: row.get(0)?,
        user_id: row.get(1)?,
        asset_in: row.get(2)?,
        amount_in: to_u64(row.get(3)?)?,
        asset_out: row.get(4)?,
        min_amount_out: to_u64(row.get(5)?)?,
        order_type: row.get(6)?,
        status: row.get(7)?,
        amount_out: row.get::<_, Option<i64>>(8)?.map(to_u64).transpose()?,
        tx_hash: row.get(9)?,
        failure_reason: row.get(10)?,
        created_at: to_u64(row.get(11)?)?,
        processed_at: row.get::<_, Option<i64>>(12)?.map(to_u64).transpose()?,
        executed_at: row.get::<_, Option<i64>>(13)?.map(to_u64).transpose()?,
        failed_at: row.get::<_, Option<i64>>(14)?.map(to_u64).transpose()?,
        last_updated_at: to_u64(row.get(15)?)?,
    })
}

fn map_order_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrderEventRecord> {
    Ok(OrderEventRecord {
        seq: to_u64(row.get(0)?)?,
        order_id: row.get(1)?,
        status: row.get(2)?,
        failure_reason: row.get(3)?,
        amount_out: row.get::<_, Option<i64>>(4)?.map(to_u64).transpose()?,
        tx_hash: row.get(5)?,
        timestamp: to_u64(row.get(6)?)?,
    })
}

fn query_count(connection: &Connection, sql: &str, value: Option<i64>) -> Result<u64> {
    let count: i64 = match value {
        Some(value) => connection.query_row(sql, [value], |row| row.get(0))?,
        None => connection.query_row(sql, [], |row| row.get(0))?,
    };
    to_u64(count).map_err(Into::into)
}

fn query_volumes(connection: &Connection, since: Option<i64>) -> Result<Vec<AssetVolume>> {
    let sql = if since.is_some() {
        "SELECT asset_in, COALESCE(SUM(amount_in), 0) FROM trades WHERE timestamp >= ?1 GROUP BY asset_in"
    } else {
        "SELECT asset_in, COALESCE(SUM(amount_in), 0) FROM trades GROUP BY asset_in"
    };
    let mut statement = connection.prepare(sql)?;
    let map = |row: &rusqlite::Row<'_>| {
        Ok(AssetVolume {
            asset: row.get(0)?,
            volume: to_u64(row.get(1)?)?,
        })
    };
    let rows = match since {
        Some(since) => statement.query_map([since], map)?,
        None => statement.query_map([], map)?,
    };
    rows.collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn query_failures(connection: &Connection) -> Result<HashMap<String, u64>> {
    let mut statement = connection.prepare(
        "SELECT COALESCE(failure_reason, 'unknown'), COUNT(*)
         FROM orders WHERE status = 'failed' GROUP BY failure_reason",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, to_u64(row.get(1)?)?))
    })?;
    rows.collect::<rusqlite::Result<HashMap<_, _>>>()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_millisecond_timestamps() {
        assert_eq!(bucket_start(1_725_000_123_456, 60), 1_725_000_120);
        assert_eq!(bucket_start(1_725_000_123, 300), 1_725_000_000);
    }

    #[test]
    fn candle_upsert_preserves_open_and_updates_ohlc() {
        let store = HistoryStore::open(":memory:").unwrap();
        store
            .record_oracle_price(&OraclePriceEvent {
                oracle_id: "test".into(),
                faucet_id: "asset".into(),
                price: 100,
                timestamp: 1_725_000_121,
            })
            .unwrap();
        store
            .record_oracle_price(&OraclePriceEvent {
                oracle_id: "test".into(),
                faucet_id: "asset".into(),
                price: 90,
                timestamp: 1_725_000_122,
            })
            .unwrap();

        let candles = store
            .candles("oracle:asset", Some("asset"), 60, None, None, 10)
            .unwrap();
        assert_eq!(candles.len(), 1);
        assert_eq!(candles[0].open, 100);
        assert_eq!(candles[0].high, 100);
        assert_eq!(candles[0].low, 90);
        assert_eq!(candles[0].close, 90);
        assert_eq!(candles[0].trade_count, 0);
    }

    #[test]
    fn trades_filter_by_pair_and_user() {
        let store = HistoryStore::open(":memory:").unwrap();
        let connection = store.connection().unwrap();
        for (order_id, user_id, pair, timestamp) in [
            ("order-1", "user-a", "BTC/USDC", 100_i64),
            ("order-2", "user-b", "BTC/USDC", 200_i64),
            ("order-3", "user-a", "ETH/USDC", 300_i64),
        ] {
            connection
                .execute(
                    r#"
                    INSERT INTO trades (
                        order_id, user_id, pair, asset_in, asset_out, amount_in,
                        amount_out, price, oracle_price, tx_hash, timestamp
                    ) VALUES (?1, ?2, ?3, 'in', 'out', 10, 20, 2, NULL, NULL, ?4)
                    "#,
                    rusqlite::params![order_id, user_id, pair, timestamp],
                )
                .unwrap();
        }
        drop(connection);

        let user_trades = store.trades(None, Some("user-a"), None, 10).unwrap();
        assert_eq!(user_trades.len(), 2);
        assert!(user_trades.iter().all(|trade| trade.user_id == "user-a"));

        let pair_trades = store.trades(Some("BTC/USDC"), None, None, 10).unwrap();
        assert_eq!(pair_trades.len(), 2);
        assert!(pair_trades.iter().all(|trade| trade.pair == "BTC/USDC"));

        let filtered = store
            .trades(Some("BTC/USDC"), Some("user-a"), None, 10)
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].order_id, "order-1");
    }
}
