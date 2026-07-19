use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use anyhow::Result;
use miden_client::account::AccountId;
use tokio::sync::watch;
use tracing::info;

use crate::deployment::Deployment;

/// Which server worker has finished attaching a newly published shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolWorker {
    Execution,
    Finality,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct WorkerReady {
    execution: bool,
    finality: bool,
}

impl WorkerReady {
    fn is_marked(self, worker: PoolWorker) -> bool {
        match worker {
            PoolWorker::Execution => self.execution,
            PoolWorker::Finality => self.finality,
        }
    }

    fn mark(&mut self, worker: PoolWorker) {
        match worker {
            PoolWorker::Execution => self.execution = true,
            PoolWorker::Finality => self.finality = true,
        }
    }

    fn is_ready(self) -> bool {
        self.execution && self.finality
    }
}

/// Shared list of pool shards the server is willing to serve, plus attach readiness.
///
/// Topology changes are published explicitly by the pool provisioner (or startup load).
/// Lazy `ensure_from_deployment` remains only for restart reconciliation.
pub struct PoolRegistry {
    pools: RwLock<Vec<AccountId>>,
    readiness: RwLock<HashMap<AccountId, WorkerReady>>,
    notify_tx: watch::Sender<u64>,
}

impl PoolRegistry {
    pub fn new(pools: Vec<AccountId>) -> Self {
        let mut readiness = HashMap::with_capacity(pools.len());
        for pool_id in &pools {
            // Pools present at process start are treated as already attached by workers
            // that import them during initialize().
            readiness.insert(
                *pool_id,
                WorkerReady {
                    execution: true,
                    finality: true,
                },
            );
        }
        let (notify_tx, _) = watch::channel(0);
        Self {
            pools: RwLock::new(pools),
            readiness: RwLock::new(readiness),
            notify_tx,
        }
    }

    pub fn pools(&self) -> Vec<AccountId> {
        self.pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    pub fn contains(&self, pool_id: &AccountId) -> bool {
        self.pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(pool_id)
    }

    /// Subscribe to generation bumps whenever a pool is published or readiness changes.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.notify_tx.subscribe()
    }

    fn bump(&self) {
        let next = self.notify_tx.borrow().saturating_add(1);
        let _ = self.notify_tx.send(next);
    }

    /// Publish a newly deployed shard into the in-memory registry (not yet ready).
    pub fn publish(&self, pool_id: AccountId) -> bool {
        let mut pools = self
            .pools
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pools.contains(&pool_id) {
            return false;
        }
        pools.push(pool_id);
        self.readiness
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(pool_id)
            .or_default();
        info!(
            pool = %pool_id.to_hex(),
            pools = pools.len(),
            "published pool shard to registry"
        );
        drop(pools);
        self.bump();
        true
    }

    /// Pools that still need worker attach acknowledgements.
    pub fn pending_attach(&self) -> Vec<AccountId> {
        let pools = self
            .pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let readiness = self
            .readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pools
            .into_iter()
            .filter(|pool_id| {
                !readiness
                    .get(pool_id)
                    .copied()
                    .unwrap_or_default()
                    .is_ready()
            })
            .collect()
    }

    pub fn is_ready(&self, pool_id: AccountId) -> bool {
        self.readiness
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&pool_id)
            .copied()
            .unwrap_or_default()
            .is_ready()
    }

    pub fn acknowledge(&self, pool_id: AccountId, worker: PoolWorker) {
        let mut readiness = self
            .readiness
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = readiness.entry(pool_id).or_default();
        if entry.is_marked(worker) {
            return;
        }
        entry.mark(worker);
        let ready = entry.is_ready();
        info!(
            pool = %pool_id.to_hex(),
            ?worker,
            ready,
            "pool attach acknowledgement"
        );
        drop(readiness);
        self.bump();
    }

    /// Wait until execution and finality have acknowledged `pool_id`.
    pub async fn wait_ready(&self, pool_id: AccountId) -> Result<()> {
        let mut rx = self.subscribe();
        loop {
            if self.is_ready(pool_id) {
                return Ok(());
            }
            if rx.changed().await.is_err() {
                anyhow::bail!(
                    "pool registry closed while waiting for {}",
                    pool_id.to_hex()
                );
            }
        }
    }

    /// Ensures `pool_id` is registered when it is listed in the on-disk deployment.
    ///
    /// Returns `true` when the pool is (now) in the registry, `false` when the
    /// deployment does not list it. Used for restart reconciliation of pools that
    /// appeared in deployment while this process was down or mid-provision.
    pub fn ensure_from_deployment(&self, pool_id: AccountId) -> Result<bool> {
        if self.contains(&pool_id) {
            return Ok(true);
        }
        let deployment = Deployment::load()?;
        if !deployment.pools.contains(&pool_id) {
            return Ok(false);
        }
        self.publish(pool_id);
        Ok(true)
    }
}

/// Shared handle used across API / processing / execution / finality / provisioner.
pub type SharedPoolRegistry = Arc<PoolRegistry>;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_and_ack_marks_ready() {
        let registry = PoolRegistry::new(Vec::new());
        let pool = AccountId::from_hex("0x728a07f8eae52ab17a3eb37e5a3bf1").unwrap();
        assert!(registry.publish(pool));
        assert!(!registry.is_ready(pool));
        registry.acknowledge(pool, PoolWorker::Execution);
        assert!(!registry.is_ready(pool));
        registry.acknowledge(pool, PoolWorker::Finality);
        assert!(registry.is_ready(pool));
        registry.wait_ready(pool).await.unwrap();
    }

    #[test]
    fn duplicate_acknowledge_is_idempotent() {
        let registry = PoolRegistry::new(Vec::new());
        let pool = AccountId::from_hex("0x728a07f8eae52ab17a3eb37e5a3bf1").unwrap();
        assert!(registry.publish(pool));
        let mut rx = registry.subscribe();
        rx.borrow_and_update();

        registry.acknowledge(pool, PoolWorker::Execution);
        assert!(rx.has_changed().unwrap());
        rx.borrow_and_update();

        registry.acknowledge(pool, PoolWorker::Execution);
        assert!(!rx.has_changed().unwrap());
        assert!(!registry.is_ready(pool));

        registry.acknowledge(pool, PoolWorker::Finality);
        assert!(rx.has_changed().unwrap());
        rx.borrow_and_update();
        assert!(registry.is_ready(pool));

        registry.acknowledge(pool, PoolWorker::Execution);
        registry.acknowledge(pool, PoolWorker::Finality);
        assert!(!rx.has_changed().unwrap());
        assert!(registry.is_ready(pool));
    }
}
