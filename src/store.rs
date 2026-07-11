use crate::{
    order::{OrderUpdate, Orders},
    pool::PoolState,
};
use dashmap::DashMap;
use miden_client::account::AccountId;
use std::collections::HashMap;
use uuid::Uuid;

pub struct Store {
    orders: Orders,
    pool_account_id: AccountId,
    asset0: AccountId,
    asset1: AccountId,
    pool_states: DashMap<AccountId, PoolState>,
    oracle_prices: DashMap<AccountId, u64>,
}

impl Store {
    pub fn new(
        pool_acc: AccountId,
        asset0: AccountId,
        asset1: AccountId,
        pool_states: HashMap<AccountId, PoolState>,
    ) -> Self {
        let store = Self {
            pool_account_id: pool_acc,
            asset0,
            asset1,
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
        self.pool_account_id.clone()
    }

    pub fn pool_states(&self) -> HashMap<AccountId, PoolState> {
        self.pool_states
            .clone()
            .into_iter()
            .collect::<HashMap<AccountId, PoolState>>()
    }

    pub fn asset0(&self) -> AccountId {
        self.asset0
    }

    pub fn asset1(&self) -> AccountId {
        self.asset1
    }

    pub fn set_oracle_price(&self, faucet_id: AccountId, price: u64) {
        self.oracle_prices.insert(faucet_id, price);
    }

    pub fn oracle_price(&self, faucet_id: AccountId) -> Option<u64> {
        self.oracle_prices.get(&faucet_id).map(|price| *price)
    }
}
