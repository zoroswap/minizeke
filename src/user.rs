use std::collections::HashMap;

use anyhow::{Result, anyhow};
use base64::{Engine, engine::general_purpose};
use dashmap::DashMap;
use miden_client::{
    Client, Serializable,
    account::{AccountBuilder, AccountId, AccountType, component::BasicWallet},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig, PublicKey, Signature},
    keystore::{FilesystemKeyStore, Keystore},
};
use miden_core::Word;
use rand::RngCore;

use crate::pool::get_user_trades_slot_name;

#[derive(Debug, Clone)]
pub struct Users {
    balances: DashMap<AccountId, DashMap<AccountId, u64>>, // faucet_id, user_id
    by_account_id: HashMap<AccountId, User>,
}

impl Users {
    pub fn new(initial_users: Vec<User>, initial_amount: u64, faucets: Vec<AccountId>) -> Self {
        let balances: DashMap<AccountId, DashMap<AccountId, u64>> =
            DashMap::with_capacity(faucets.len());
        let mut by_account_id = HashMap::with_capacity(initial_users.len());

        for faucet in &faucets {
            balances.insert(*faucet, DashMap::with_capacity(initial_users.len()));
        }
        for user in initial_users {
            for faucet in &faucets {
                if let Some(balance_map) = balances.get(faucet) {
                    balance_map.insert(*&user.id, initial_amount);
                }
            }
            by_account_id.insert(*&user.id, user.clone());
        }
        Self {
            balances,
            by_account_id,
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

    // UNSAFE: will be removed when we stop hosting users on be
    pub fn get_user_index(&self, user_id: &AccountId) -> u16 {
        self.by_account_id.get(user_id).unwrap().index
    }

    // UNSAFE: will be removed when we stop hosting users on be
    pub fn serialized_users(&self) -> Vec<SerializedUser> {
        let mut users: Vec<SerializedUser> = self
            .by_account_id
            .iter()
            .map(|(_, u)| SerializedUser::try_from(u.clone()).unwrap())
            .collect();
        users.sort_by_key(|u| u.index);
        users
    }

    pub fn by_account_id(&self) -> HashMap<AccountId, User> {
        self.by_account_id.clone()
    }
}

#[derive(Debug, Clone)]
pub struct User {
    id: AccountId,
    key_pair: AuthSecretKey,
    index: u16,
}

impl User {
    pub fn id(&self) -> AccountId {
        self.id
    }
    pub fn pubkey(&self) -> PublicKey {
        self.key_pair.public_key()
    }
    pub fn sign(&self, msg: Word) -> Signature {
        self.key_pair.sign(msg)
    }
}

impl TryFrom<User> for SerializedUser {
    type Error = anyhow::Error;
    fn try_from(value: User) -> std::result::Result<Self, Self::Error> {
        let signing_key = general_purpose::STANDARD.encode(value.key_pair.to_bytes());
        let user_slot_key = get_user_trades_slot_name(value.index);
        Ok(Self {
            id: value.id.to_hex(),
            index: value.index,
            user_prefix: value.id.prefix().as_u64(),
            user_suffix: value.id.suffix().as_canonical_u64(),
            signing_key,
            balance_slot_prefix: user_slot_key.id().prefix().as_canonical_u64(),
            balance_slot_suffix: user_slot_key.id().suffix().as_canonical_u64(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct SerializedUser {
    pub id: String,
    pub index: u16,
    pub user_prefix: u64,
    pub user_suffix: u64,
    pub balance_slot_prefix: u64,
    pub balance_slot_suffix: u64,
    pub signing_key: String,
}

pub async fn get_users(n: u32, client: &mut Client<FilesystemKeyStore>) -> Result<Vec<User>> {
    let keystore = FilesystemKeyStore::new("keystore".into())?;
    let mut users = Vec::with_capacity(n as usize);
    println!("Creating {n} user accounts");
    for i in 0..n {
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

        // deploy: users need on-chain accounts to send REGISTER/FUND notes
        client.add_account(&account, false).await?;
        keystore
            .add_key(&key_pair, account.id())
            .await
            .map_err(|e| anyhow!("failed to add user key: {e:?}"))?;

        users.push(User {
            id: account.id(),
            key_pair,
            index: i as u16,
        });
    }
    client.sync_state().await?;
    Ok(users)
}
