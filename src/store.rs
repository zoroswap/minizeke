use crate::{
    order::{OrderUpdate, Orders},
    pool::PoolState,
    user::Users,
};
use dashmap::DashMap;
use miden_client::account::AccountId;
use std::collections::HashMap;
use uuid::Uuid;

pub struct Store {
    orders: Orders,
    pool_account_id: AccountId,
    pool_states: DashMap<AccountId, PoolState>,
    users: Users,
}

impl Store {
    pub fn new(
        pool_acc: AccountId,
        users: Users,
        pool_states: HashMap<AccountId, PoolState>,
    ) -> Self {
        let store = Self {
            pool_account_id: pool_acc,
            orders: Orders::default(),
            pool_states: DashMap::new(),
            users,
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
}
