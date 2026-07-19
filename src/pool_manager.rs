//! Server-owned pool shard provisioning.
//!
//! Monitors the vault's active-pool registration fill, deploys spare shards ahead of
//! capacity exhaustion, eagerly attaches execution/finality workers, then activates the
//! shard on-chain. The simulator never mutates topology.

use std::{
    env,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use miden_client::account::AccountId;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::{
    assembly_utils::storage_slot_name,
    deployment::Deployment,
    pool::{MAX_POOL_CELLS, NEXT_CELL_SLOT, deploy_pool, fetch_targeted_account_storage},
    pool_registry::{PoolRegistry, SharedPoolRegistry},
    test_utils::{get_client, get_pool_client, get_pool_client_for},
    vault::{
        POOL_USER_COUNTS_SLOT, active_pool_from_storage, add_pool_to_vault,
        pool_user_capacity_from_storage, pool_user_count_from_storage, vault_pool_key,
    },
};
use miden_client::{account::StorageMapKey, rpc::domain::account::AccountStorageRequirements};

const DEFAULT_POLL_SECS: u64 = 5;
const DEFAULT_RESERVE_USERS: u32 = 4;
const DEFAULT_ATTACH_TIMEOUT_SECS: u64 = 180;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisionStatus {
    Deploying,
    Published,
    Attached,
    Activated,
    Failed,
}

impl ProvisionStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Deploying => "deploying",
            Self::Published => "published",
            Self::Attached => "attached",
            Self::Activated => "activated",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "deploying" => Ok(Self::Deploying),
            "published" => Ok(Self::Published),
            "attached" => Ok(Self::Attached),
            "activated" => Ok(Self::Activated),
            "failed" => Ok(Self::Failed),
            other => bail!("unknown provision status {other}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProvisionRecord {
    pub id: i64,
    pub pool_id: Option<AccountId>,
    pub status: ProvisionStatus,
    pub error: Option<String>,
    pub created_at: u64,
    pub updated_at: u64,
}

/// Durable journal for in-flight shard provisioning.
pub struct PoolProvisionStore {
    connection: Mutex<Connection>,
}

impl PoolProvisionStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let connection = Connection::open(path).context("open pool provision sqlite database")?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS pool_provisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pool_id TEXT UNIQUE,
                status TEXT NOT NULL CHECK(status IN (
                    'deploying', 'published', 'attached', 'activated', 'failed'
                )),
                error TEXT,
                claim_owner TEXT,
                claim_until INTEGER,
                -- Non-null only while in-flight; unique so at most one provision runs.
                inflight_guard INTEGER,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS idx_single_inflight_provision
                ON pool_provisions(inflight_guard)
                WHERE inflight_guard = 1;
            CREATE TABLE IF NOT EXISTS pool_provision_meta (
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
        let path = env::var("POOL_PROVISION_DB_PATH").unwrap_or_else(|_| {
            let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_string());
            format!("pool_provision.{}.sqlite3", network.to_ascii_lowercase())
        });
        Self::open(path)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow!("pool provision database lock poisoned"))
    }

    pub fn inflight(&self) -> Result<Option<ProvisionRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id, pool_id, status, error, created_at, updated_at
                 FROM pool_provisions
                 WHERE status IN ('deploying', 'published', 'attached')
                 ORDER BY id ASC LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, i64>(5)?,
                    ))
                },
            )
            .optional()?
            .map(|(id, pool_id, status, error, created_at, updated_at)| {
                Ok(ProvisionRecord {
                    id,
                    pool_id: pool_id
                        .map(|hex| AccountId::from_hex(&hex))
                        .transpose()
                        .map_err(|e| anyhow!("invalid pool_id in provision journal: {e}"))?,
                    status: ProvisionStatus::parse(&status)?,
                    error,
                    created_at: created_at as u64,
                    updated_at: updated_at as u64,
                })
            })
            .transpose()
    }

    pub fn begin_deploying(&self, worker_id: &str, now: u64) -> Result<Option<i64>> {
        let mut connection = self.connection()?;
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM pool_provisions
                WHERE status IN ('deploying', 'published', 'attached')
            )",
            [],
            |row| row.get(0),
        )?;
        if exists {
            tx.commit()?;
            return Ok(None);
        }
        tx.execute(
            "INSERT INTO pool_provisions (
                pool_id, status, claim_owner, claim_until, inflight_guard, created_at, updated_at
             ) VALUES (NULL, 'deploying', ?1, ?2, 1, ?3, ?3)",
            params![worker_id, now.saturating_add(600_000) as i64, now as i64],
        )?;
        let id = tx.last_insert_rowid();
        tx.commit()?;
        Ok(Some(id))
    }

    pub fn set_pool_id(&self, id: i64, pool_id: AccountId, now: u64) -> Result<()> {
        let connection = self.connection()?;
        let changed = connection.execute(
            "UPDATE pool_provisions SET pool_id = ?1, updated_at = ?2 WHERE id = ?3 AND status = 'deploying'",
            params![pool_id.to_hex(), now as i64, id],
        )?;
        if changed != 1 {
            bail!("failed to set pool_id for provision {id}");
        }
        Ok(())
    }

    pub fn advance(&self, id: i64, status: ProvisionStatus, now: u64) -> Result<()> {
        let connection = self.connection()?;
        let inflight_guard = matches!(
            status,
            ProvisionStatus::Deploying | ProvisionStatus::Published | ProvisionStatus::Attached
        )
        .then_some(1);
        let changed = connection.execute(
            "UPDATE pool_provisions
             SET status = ?1, updated_at = ?2, error = NULL, inflight_guard = ?3
             WHERE id = ?4",
            params![status.as_str(), now as i64, inflight_guard, id],
        )?;
        if changed != 1 {
            bail!("failed to advance provision {id} to {}", status.as_str());
        }
        Ok(())
    }

    pub fn fail(&self, id: i64, error: &str, now: u64) -> Result<()> {
        let connection = self.connection()?;
        connection.execute(
            "UPDATE pool_provisions
             SET status = 'failed', error = ?1, updated_at = ?2, inflight_guard = NULL
             WHERE id = ?3",
            params![error, now as i64, id],
        )?;
        Ok(())
    }

    pub fn latest_error(&self) -> Result<Option<String>> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT error FROM pool_provisions
                 WHERE status = 'failed' AND error IS NOT NULL
                 ORDER BY id DESC LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn activated_count(&self) -> Result<usize> {
        let connection = self.connection()?;
        let count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM pool_provisions WHERE status = 'activated'",
            [],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }
}

#[derive(Debug, Clone)]
pub struct PoolCapacitySnapshot {
    pub active_pool: Option<AccountId>,
    pub user_count: u32,
    pub user_capacity: u32,
    pub next_cell: u32,
    pub max_cells: u32,
    pub pools: Vec<AccountId>,
    pub pending_attach: Vec<AccountId>,
    pub last_provision_error: Option<String>,
}

/// Deploys one additional pool for `vault_id` and warms its local execution store.
pub async fn deploy_pool_shard(vault_id: AccountId) -> Result<AccountId> {
    let mut deploy_client = get_pool_client().await?;
    deploy_client.ensure_genesis_in_place().await?;
    deploy_client.sync_state().await?;
    let pool_id = deploy_pool(&mut deploy_client, vault_id).await?.id();

    let mut shard_client = get_pool_client_for(pool_id).await?;
    shard_client.ensure_genesis_in_place().await?;
    shard_client.import_account_by_id(pool_id).await?;
    shard_client.sync_state().await?;
    Ok(pool_id)
}

/// Authorizes `pool_id` on the vault and makes it the active registration target.
pub async fn activate_pool_shard(
    operator_id: AccountId,
    vault_id: AccountId,
    pool_id: AccountId,
) -> Result<()> {
    let mut operator = get_client().await?;
    operator.ensure_genesis_in_place().await?;
    operator.sync_state().await?;
    add_pool_to_vault(&mut operator, operator_id, vault_id, pool_id).await
}

/// Appends `pool_id` to the deployment file if missing.
pub fn publish_pool_to_deployment(pool_id: AccountId) -> Result<()> {
    let mut deployment = Deployment::load()?;
    if !deployment.pools.contains(&pool_id) {
        deployment.pools.push(pool_id);
        deployment.save()?;
    }
    Ok(())
}

pub async fn fetch_vault_capacity_storage(
    vault_id: AccountId,
    active_pool: Option<AccountId>,
) -> Result<miden_client::account::AccountStorage> {
    // Value slots (active_pool, capacity) are always returned; request the count map key
    // when the active pool is known.
    match active_pool {
        Some(pool_id) => {
            let key = StorageMapKey::new(vault_pool_key(pool_id));
            fetch_targeted_account_storage(
                vault_id,
                AccountStorageRequirements::new([(
                    storage_slot_name(POOL_USER_COUNTS_SLOT),
                    std::slice::from_ref(&key),
                )]),
            )
            .await
        }
        None => fetch_targeted_account_storage(vault_id, empty_storage_requirements()).await,
    }
}

fn empty_storage_requirements() -> AccountStorageRequirements {
    AccountStorageRequirements::new(std::iter::empty::<(
        miden_client::account::StorageSlotName,
        &'static [StorageMapKey],
    )>())
}

pub async fn fetch_pool_next_cell(pool_id: AccountId) -> Result<u32> {
    let storage = fetch_targeted_account_storage(pool_id, empty_storage_requirements()).await?;
    let word = storage
        .get_item(&storage_slot_name(NEXT_CELL_SLOT))
        .map_err(|e| anyhow!("failed to read {NEXT_CELL_SLOT}: {e:?}"))?;
    let next = word[0].as_canonical_u64();
    u32::try_from(next).map_err(|_| anyhow!("{NEXT_CELL_SLOT} value {next} does not fit in u32"))
}

pub async fn read_capacity_snapshot(
    vault_id: AccountId,
    registry: &PoolRegistry,
    store: &PoolProvisionStore,
) -> Result<PoolCapacitySnapshot> {
    // First fetch to learn active pool (value slots always present).
    let header = fetch_targeted_account_storage(vault_id, empty_storage_requirements()).await?;
    let active_pool = active_pool_from_storage(&header)?;
    let storage = fetch_vault_capacity_storage(vault_id, active_pool).await?;
    let user_capacity = pool_user_capacity_from_storage(&storage)?;
    let user_count = match active_pool {
        Some(pool_id) => pool_user_count_from_storage(&storage, pool_id)?,
        None => 0,
    };
    let next_cell = match active_pool {
        Some(pool_id) => fetch_pool_next_cell(pool_id).await.unwrap_or(0),
        None => 0,
    };
    Ok(PoolCapacitySnapshot {
        active_pool,
        user_count,
        user_capacity,
        next_cell,
        max_cells: MAX_POOL_CELLS as u32,
        pools: registry.pools(),
        pending_attach: registry.pending_attach(),
        last_provision_error: store.latest_error()?,
    })
}

fn reserve_users(capacity: u32) -> u32 {
    env::var("POOL_PROVISION_RESERVE_USERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_RESERVE_USERS)
        .min(capacity.saturating_sub(1).max(1))
}

fn poll_interval() -> Duration {
    let secs = env::var("POOL_PROVISION_POLL_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_POLL_SECS)
        .max(1);
    Duration::from_secs(secs)
}

fn attach_timeout() -> Duration {
    let secs = env::var("POOL_PROVISION_ATTACH_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_ATTACH_TIMEOUT_SECS)
        .max(1);
    Duration::from_secs(secs)
}

fn now_millis() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

/// Background control loop that keeps spare registration capacity available.
pub struct PoolProvisioner {
    registry: SharedPoolRegistry,
    store: Arc<PoolProvisionStore>,
    vault_id: AccountId,
    operator_id: AccountId,
    worker_id: String,
    status_tx: watch::Sender<Option<PoolCapacitySnapshot>>,
}

impl PoolProvisioner {
    pub fn new(
        registry: SharedPoolRegistry,
        store: Arc<PoolProvisionStore>,
        vault_id: AccountId,
        operator_id: AccountId,
    ) -> (Self, watch::Receiver<Option<PoolCapacitySnapshot>>) {
        let (status_tx, status_rx) = watch::channel(None);
        (
            Self {
                registry,
                store,
                vault_id,
                operator_id,
                worker_id: format!("pool-provisioner-{}", uuid::Uuid::new_v4()),
                status_tx,
            },
            status_rx,
        )
    }

    pub fn status_receiver(&self) -> watch::Receiver<Option<PoolCapacitySnapshot>> {
        self.status_tx.subscribe()
    }

    pub async fn start(mut self) -> Result<()> {
        self.reconcile_startup().await?;
        let mut interval = tokio::time::interval(poll_interval());
        loop {
            interval.tick().await;
            if let Err(error) = self.tick().await {
                error!(%error, "pool provisioner tick failed");
            }
        }
    }

    async fn tick(&mut self) -> Result<()> {
        if let Some(inflight) = self.store.inflight()? {
            self.resume(inflight).await?;
            return Ok(());
        }

        let snapshot = read_capacity_snapshot(self.vault_id, &self.registry, &self.store).await?;
        let _ = self.status_tx.send(Some(snapshot.clone()));

        let Some(active) = snapshot.active_pool else {
            warn!("vault has no active pool; skipping provision check");
            return Ok(());
        };
        let remaining = snapshot.user_capacity.saturating_sub(snapshot.user_count);
        let reserve = reserve_users(snapshot.user_capacity);
        if remaining > reserve {
            return Ok(());
        }
        info!(
            active = %active.to_hex(),
            user_count = snapshot.user_count,
            capacity = snapshot.user_capacity,
            remaining,
            reserve,
            "active pool nearing registration capacity; provisioning spare shard"
        );
        self.provision_new_shard().await
    }

    async fn reconcile_startup(&mut self) -> Result<()> {
        // Ensure every deployment pool is listed; workers will attach via their own loops.
        let deployment = Deployment::load()?;
        for pool_id in &deployment.pools {
            self.registry.publish(*pool_id);
            // Startup pools were imported during worker initialize; mark ready if already known.
            if self.registry.is_ready(*pool_id) {
                continue;
            }
        }
        if let Some(inflight) = self.store.inflight()? {
            info!(
                provision = inflight.id,
                status = inflight.status.as_str(),
                "resuming in-flight pool provision after restart"
            );
            self.resume(inflight).await?;
        }
        let snapshot = read_capacity_snapshot(self.vault_id, &self.registry, &self.store).await?;
        let _ = self.status_tx.send(Some(snapshot));
        Ok(())
    }

    async fn provision_new_shard(&mut self) -> Result<()> {
        let now = now_millis();
        let Some(id) = self.store.begin_deploying(&self.worker_id, now)? else {
            return Ok(());
        };
        match self.run_provision(id, None).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let message = format!("{error:#}");
                self.store.fail(id, &message, now_millis())?;
                Err(error)
            }
        }
    }

    async fn resume(&mut self, record: ProvisionRecord) -> Result<()> {
        match self.run_provision(record.id, record.pool_id).await {
            Ok(()) => Ok(()),
            Err(error) => {
                let message = format!("{error:#}");
                self.store.fail(record.id, &message, now_millis())?;
                Err(error)
            }
        }
    }

    async fn run_provision(&mut self, id: i64, existing_pool: Option<AccountId>) -> Result<()> {
        let pool_id = match existing_pool {
            Some(pool_id) => pool_id,
            None => {
                info!(provision = id, "deploying pool shard");
                let pool_id = deploy_pool_shard(self.vault_id).await?;
                self.store.set_pool_id(id, pool_id, now_millis())?;
                pool_id
            }
        };

        // Publish to deployment + registry before activation.
        publish_pool_to_deployment(pool_id)?;
        self.registry.publish(pool_id);
        self.store
            .advance(id, ProvisionStatus::Published, now_millis())?;

        // Wait for eager attach acknowledgements.
        info!(provision = id, pool = %pool_id.to_hex(), "waiting for worker attach");
        tokio::time::timeout(attach_timeout(), self.registry.wait_ready(pool_id))
            .await
            .map_err(|_| {
                anyhow!(
                    "timed out waiting for workers to attach {}",
                    pool_id.to_hex()
                )
            })??;
        self.store
            .advance(id, ProvisionStatus::Attached, now_millis())?;

        info!(provision = id, pool = %pool_id.to_hex(), "activating pool as ACTIVE_POOL");
        activate_pool_shard(self.operator_id, self.vault_id, pool_id).await?;
        self.store
            .advance(id, ProvisionStatus::Activated, now_millis())?;
        info!(provision = id, pool = %pool_id.to_hex(), "pool shard activated");

        let snapshot = read_capacity_snapshot(self.vault_id, &self.registry, &self.store).await?;
        let _ = self.status_tx.send(Some(snapshot));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_enforces_single_inflight() {
        let dir =
            std::env::temp_dir().join(format!("pool-provision-{}.sqlite3", uuid::Uuid::new_v4()));
        let store = PoolProvisionStore::open(&dir).unwrap();
        let first = store.begin_deploying("w1", 1).unwrap();
        assert!(first.is_some());
        let second = store.begin_deploying("w2", 2).unwrap();
        assert!(second.is_none());
        let id = first.unwrap();
        let pool = AccountId::from_hex("0x728a07f8eae52ab17a3eb37e5a3bf1").unwrap();
        store.set_pool_id(id, pool, 3).unwrap();
        store.advance(id, ProvisionStatus::Published, 4).unwrap();
        store.advance(id, ProvisionStatus::Attached, 5).unwrap();
        store.advance(id, ProvisionStatus::Activated, 6).unwrap();
        assert!(store.inflight().unwrap().is_none());
        let third = store.begin_deploying("w3", 7).unwrap();
        assert!(third.is_some());
        let _ = std::fs::remove_file(dir);
    }
}
