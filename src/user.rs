//! Test-only user machinery. In production, orders arrive fully signed over the API and
//! the server never holds user keys; these helpers exist so integration tests can create,
//! register and sign for their own users.

use anyhow::{Result, anyhow};
use miden_client::account::AccountId;
use miden_client::{
    Client,
    account::{AccountBuilder, AccountType, component::BasicWallet},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig, PublicKey, Signature},
    keystore::{FilesystemKeyStore, Keystore},
};
use miden_core::Word;
use rand::RngCore;

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
    pub fn sign(&self, msg: Word) -> Signature {
        self.key_pair.sign(msg)
    }
}

pub async fn get_users(n: u32, client: &mut Client<FilesystemKeyStore>) -> Result<Vec<User>> {
    let keystore = FilesystemKeyStore::new("keystore".into())?;
    let mut users = Vec::with_capacity(n as usize);
    println!("Creating {n} user accounts");
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

        // deploy: users need on-chain accounts to send REGISTER/FUND notes
        client.add_account(&account, false).await?;
        keystore
            .add_key(&key_pair, account.id())
            .await
            .map_err(|e| anyhow!("failed to add user key: {e:?}"))?;

        users.push(User {
            id: account.id(),
            key_pair,
        });
    }
    client.sync_state().await?;
    Ok(users)
}
