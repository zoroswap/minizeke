use crate::{
    deployment::AssetInfo,
    order::{OrderUpdate, Orders},
    pool::PoolState,
    pool_registry::PoolRegistry,
};
use anyhow::Result;
use dashmap::DashMap;
use miden_client::account::AccountId;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

pub struct Store {
    orders: Orders,
    vault_id: AccountId,
    assets: Vec<AssetInfo>,
    pool_registry: Arc<PoolRegistry>,
    pool_states: DashMap<AccountId, PoolState>,
    oracle_prices: DashMap<AccountId, u64>,
}

impl Store {
    pub fn new(
        vault_id: AccountId,
        assets: Vec<AssetInfo>,
        pool_registry: Arc<PoolRegistry>,
        pool_states: HashMap<AccountId, PoolState>,
    ) -> Self {
        let store = Self {
            vault_id,
            assets,
            pool_registry,
            orders: Orders::default(),
            pool_states: DashMap::new(),
            oracle_prices: DashMap::new(),
        };
        store.set_pool_states(pool_states);
        store
    }

    pub fn apply_order_update(&self, order_update: crate::order::OrderUpdate) {
        self.orders.apply_order_update(order_update);
    }

    pub fn set_pool_states(&self, new_pool_state: HashMap<AccountId, PoolState>) {
        for (faucet_id, new_pool_state) in new_pool_state.iter() {
            self.pool_states.insert(*faucet_id, *new_pool_state);
        }
    }

    pub fn order_stats(&self) -> crate::order::OrderStats {
        self.orders.stats()
    }

    pub fn get_order(&self, id: Uuid) -> Option<OrderUpdate> {
        self.orders.get_order(&id)
    }

    pub fn pool_id(&self) -> AccountId {
        self.pool_registry.pools()[0]
    }

    pub fn vault_id(&self) -> AccountId {
        self.vault_id
    }

    pub fn pools(&self) -> Vec<AccountId> {
        self.pool_registry.pools()
    }

    pub fn has_pool(&self, pool_id: &AccountId) -> bool {
        self.pool_registry.contains(pool_id)
    }

    /// Hot-attach a pool listed in deployment.json but missing at process start.
    pub fn ensure_pool_listed(&self, pool_id: AccountId) -> Result<bool> {
        self.pool_registry.ensure_from_deployment(pool_id)
    }

    pub fn assets(&self) -> &[AssetInfo] {
        &self.assets
    }

    pub fn pool_states(&self) -> HashMap<AccountId, PoolState> {
        self.pool_states
            .clone()
            .into_iter()
            .collect::<HashMap<AccountId, PoolState>>()
    }

    pub fn set_oracle_price(&self, faucet_id: AccountId, price: u64) {
        self.oracle_prices.insert(faucet_id, price);
    }

    pub fn oracle_price(&self, faucet_id: AccountId) -> Option<u64> {
        self.oracle_prices.get(&faucet_id).map(|price| *price)
    }
}
