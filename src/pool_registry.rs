use std::sync::RwLock;

use anyhow::Result;
use miden_client::account::AccountId;
use tracing::info;

use crate::deployment::Deployment;

/// Shared list of pool shards the server is willing to serve.
///
/// Hot-attaches pools that appear in `Deployment` after process start (e.g. sim
/// spawning a second shard) so Store / Processing / MidenExecution stay aligned.
pub struct PoolRegistry {
    pools: RwLock<Vec<AccountId>>,
}

impl PoolRegistry {
    pub fn new(pools: Vec<AccountId>) -> Self {
        Self {
            pools: RwLock::new(pools),
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

    /// Ensures `pool_id` is registered when it is listed in the on-disk deployment.
    ///
    /// Returns `true` when the pool is (now) in the registry, `false` when the
    /// deployment does not list it.
    pub fn ensure_from_deployment(&self, pool_id: AccountId) -> Result<bool> {
        if self.contains(&pool_id) {
            return Ok(true);
        }
        let deployment = Deployment::load()?;
        if !deployment.pools.contains(&pool_id) {
            return Ok(false);
        }
        let mut pools = self
            .pools
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !pools.contains(&pool_id) {
            pools.push(pool_id);
            info!(
                pool = %pool_id.to_hex(),
                pools = pools.len(),
                "hot-attached pool shard from deployment"
            );
        }
        Ok(true)
    }
}
