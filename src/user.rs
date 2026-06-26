use std::collections::HashMap;

use anyhow::{Result, anyhow};
use base64::{Engine, engine::general_purpose};
use dashmap::DashMap;
use miden_client::{
    Client, Serializable,
    account::{AccountBuilder, AccountId, AccountType, component::BasicWallet},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig, PublicKey},
    keystore::FilesystemKeyStore,
};
use rand::RngCore;

#[derive(Debug, Clone)]
pub struct Users {
    balances: DashMap<AccountId, DashMap<AccountId, u64>>, // faucet_id, user_id
    user_to_index: HashMap<AccountId, u16>,
    keypairs: HashMap<AccountId, AuthSecretKey>,
}

impl Users {
    pub fn new(initial_users: Vec<User>, initial_amount: u64, faucets: Vec<AccountId>) -> Self {
        let balances: DashMap<AccountId, DashMap<AccountId, u64>> =
            DashMap::with_capacity(faucets.len());
        let mut user_to_index = HashMap::with_capacity(initial_users.len());
        let mut keypairs = HashMap::with_capacity(initial_users.len());
        for faucet in &faucets {
            balances.insert(*faucet, DashMap::with_capacity(initial_users.len()));
        }
        for (index, user) in initial_users.iter().enumerate() {
            for faucet in &faucets {
                if let Some(map) = balances.get(faucet) {
                    map.insert(*&user.id, initial_amount);
                }
            }
            user_to_index.insert(*&user.id, index as u16);
            keypairs.insert(*&user.id, user.key_pair.clone());
        }
        Self {
            balances,
            user_to_index,
            keypairs,
        }
    }

    pub fn sub_from_balance(&self, user: AccountId, faucet: AccountId, amount: u64) -> Result<()> {
        let f = self
            .balances
            .get(&faucet)
            .ok_or_else(|| anyhow!("faucet {} not found", faucet.to_hex()))?;
        let u = *f.get(&user).ok_or_else(|| {
            anyhow!(
                "user {} not found for faucet {}",
                user.to_hex(),
                faucet.to_hex()
            )
        })?;
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
        let u = *f.get(&user).ok_or_else(|| {
            anyhow!(
                "user {} not found for faucet {}",
                user.to_hex(),
                faucet.to_hex()
            )
        })?;
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
        let mut users: Vec<(AccountId, u16)> = self
            .user_to_index
            .iter()
            .map(|(id, idx)| (*id, *idx))
            .collect();
        users.sort_by_key(|(_, idx)| *idx);
        users
    }
}

#[derive(Debug, Clone)]
pub struct User {
    id: AccountId,
    key_pair: AuthSecretKey,
}

impl User {
    pub fn id(&self) -> AccountId {
        self.id
    }
    pub fn pubkey(&self) -> PublicKey {
        self.key_pair.public_key()
    }
}

impl TryFrom<User> for SerializedUser {
    type Error = anyhow::Error;
    fn try_from(value: User) -> std::result::Result<Self, Self::Error> {
        let privkey = match &value.key_pair {
            AuthSecretKey::EcdsaK256Keccak(signing_key) => Some(signing_key.to_bytes()),
            _ => None,
        }
        .ok_or_else(|| anyhow!("Error serializing keypair for user {}", value.id.to_hex()))?;
        let privkey = general_purpose::STANDARD.encode(privkey);
        Ok(Self {
            id: value.id.to_hex(),
            privkey,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SerializedUser {
    id: String,
    privkey: String,
}

pub async fn get_users(n: u32, client: &mut Client<FilesystemKeyStore>) -> Result<Vec<User>> {
    let mut users = Vec::with_capacity(n as usize);
    println!("Making up {n} users");
    for _ in 0..n {
        // Draw a fresh seed per account, otherwise every account is built from the
        // same seed and ends up with an identical AccountId.
        let mut init_seed = [0_u8; 32];
        client.rng().fill_bytes(&mut init_seed);

        let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
        let builder = AccountBuilder::new(init_seed)
            .account_type(AccountType::Public)
            .with_auth_component(AuthSingleSig::new(
                key_pair.public_key().to_commitment(),
                AuthScheme::EcdsaK256Keccak,
            ))
            .with_component(BasicWallet);
        let account = builder.build()?;

        users.push(User {
            id: account.id(),
            key_pair,
        });
    }
    Ok(users)
}
