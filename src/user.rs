use std::collections::HashMap;

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use miden_client::{
    Client,
    account::{AccountBuilder, AccountId, AccountType, component::BasicWallet},
    keystore::FilesystemKeyStore,
};
use miden_protocol::testing::noop_auth_component::NoopAuthComponent;
use rand::RngCore;

#[derive(Debug, Clone)]
pub struct Users {
    balances: DashMap<AccountId, DashMap<AccountId, u64>>, // faucet_id, user_id
    user_to_index: HashMap<AccountId, u16>,
}

impl Users {
    pub fn new(
        initial_users: Vec<AccountId>,
        initial_amount: u64,
        faucets: Vec<AccountId>,
    ) -> Self {
        let balances: DashMap<AccountId, DashMap<AccountId, u64>> =
            DashMap::with_capacity(faucets.len());
        let mut user_to_index = HashMap::with_capacity(initial_users.len());
        for faucet in &faucets {
            balances.insert(*faucet, DashMap::with_capacity(initial_users.len()));
        }
        for (index, user) in initial_users.iter().enumerate() {
            for faucet in &faucets {
                if let Some(map) = balances.get(faucet) {
                    map.insert(*user, initial_amount);
                }
            }
            user_to_index.insert(*user, index as u16);
        }
        Self {
            balances,
            user_to_index,
        }
    }

    pub fn sub_from_balance(&self, user: AccountId, faucet: AccountId, amount: u64) -> Result<()> {
        let f = self
            .balances
            .get(&faucet)
            .ok_or_else(|| anyhow!("faucet {} not found", faucet.to_hex()))?;
        let u = *f
            .get(&user)
            .ok_or_else(|| anyhow!("user {} not found for faucet {}", user.to_hex(), faucet.to_hex()))?;
        let new_balance = u.checked_sub(amount).ok_or_else(|| {
            anyhow!(
                "insufficient balance for user {} on faucet {}: have {}, need {}",
                user.to_hex(),
                faucet.to_hex(),
                u,
                amount
            )
        })?;
        f.insert(user, new_balance);
        Ok(())
    }

    pub fn add_to_balance(&self, user: AccountId, faucet: AccountId, amount: u64) -> Result<()> {
        let f = self
            .balances
            .get(&faucet)
            .ok_or_else(|| anyhow!("faucet {} not found", faucet.to_hex()))?;
        let u = *f
            .get(&user)
            .ok_or_else(|| anyhow!("user {} not found for faucet {}", user.to_hex(), faucet.to_hex()))?;
        let new_balance = u.checked_add(amount).ok_or_else(|| {
            anyhow!(
                "balance overflow for user {} on faucet {}",
                user.to_hex(),
                faucet.to_hex()
            )
        })?;
        f.insert(user, new_balance);
        Ok(())
    }

    pub fn user_balance(&self, user: AccountId, faucet: AccountId) -> Result<u64> {
        let f = self
            .balances
            .get(&faucet)
            .ok_or(anyhow!("Faucet not found"))?;
        let u = *f.get(&user).ok_or(anyhow!("Faucet not found"))?;
        Ok(u)
    }

    pub fn get_user_index(&self, user_id: &AccountId) -> Option<u16> {
        self.user_to_index.get(user_id).copied()
    }

    /// Returns all known user account IDs paired with their index, ordered by index.
    pub fn users_with_index(&self) -> Vec<(AccountId, u16)> {
        let mut users: Vec<(AccountId, u16)> =
            self.user_to_index.iter().map(|(id, idx)| (*id, *idx)).collect();
        users.sort_by_key(|(_, idx)| *idx);
        users
    }
}

pub async fn get_users(n: u32, client: &mut Client<FilesystemKeyStore>) -> Result<Vec<AccountId>> {
    let mut users = Vec::with_capacity(n as usize);
    println!("Making up {n} users");
    for _ in 0..n {
        // Draw a fresh seed per account, otherwise every account is built from the
        // same seed and ends up with an identical AccountId.
        let mut init_seed = [0_u8; 32];
        client.rng().fill_bytes(&mut init_seed);
        let builder = AccountBuilder::new(init_seed)
            .account_type(AccountType::Public)
            .with_auth_component(NoopAuthComponent)
            .with_component(BasicWallet);
        let account = builder.build()?;
        users.push(account.id());
    }
    Ok(users)
}
