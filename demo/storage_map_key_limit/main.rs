//! Demo: hit the node `storage_map_key` limit via the FPI foreign-account fetch path.
//!
//! ```sh
//! cargo run --bin demo_storage_map_key_limit
//! ```
//!
//! Builds a `ForeignAccount::public(...)` with N map keys and runs
//! `Client::execute_transaction`, which calls `retrieve_foreign_account_inputs` →
//! `get_account(StorageMapFetch::Slots(...))` — the same RPC the vault FPI path uses.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use miden_client::{
    Client,
    account::{
        AccountBuilder, AccountId, AccountType, StorageMapKey, StorageSlotName, StorageSlotType,
        component::BasicWallet,
    },
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    builder::ClientBuilder,
    keystore::{FilesystemKeyStore, Keystore},
    rpc::{
        Endpoint, GrpcClient, NodeRpcClient,
        domain::account::{
            AccountStorageRequirements, GetAccountRequest, StorageMapFetch, VaultFetch,
        },
    },
    transaction::{ForeignAccount, TransactionRequestBuilder},
};
use miden_client_sqlite_store::SqliteStore;
use rand::RngCore;

const LIMIT: usize = 64;

/// Public fungible faucet on Miden testnet (has storage map slots).
const FOREIGN: &str = "0x478b3ea29d51b2116864e98ee62e99";

#[tokio::main]
async fn main() -> Result<()> {
    let foreign_id = AccountId::from_hex(FOREIGN)?;
    let slot = discover_map_slot(foreign_id).await?;

    println!("demo: FPI foreign-account fetch → storage_map_key limit ({LIMIT})");
    println!("foreign {}", foreign_id.to_hex());
    println!("slot    {slot}");
    println!();

    let (mut client, keystore) = build_client().await?;
    client.sync_chain().await.context("sync_chain")?;
    let account_id = create_local_account(&mut client, &keystore).await?;

    fpi_with_keys(&mut client, account_id, foreign_id, &slot, LIMIT).await?;
    println!();
    fpi_with_keys(&mut client, account_id, foreign_id, &slot, LIMIT + 1).await?;

    Ok(())
}

async fn fpi_with_keys(
    client: &mut Client<FilesystemKeyStore>,
    account_id: AccountId,
    foreign_id: AccountId,
    slot: &StorageSlotName,
    n: usize,
) -> Result<()> {
    println!("--- FPI with {n} storage map keys ---");

    let keys: Vec<StorageMapKey> = (0..n as u32).map(StorageMapKey::from_index).collect();
    let requirements = AccountStorageRequirements::new([(slot.clone(), keys.iter())]);
    let foreign =
        ForeignAccount::public(foreign_id, requirements).context("ForeignAccount::public")?;

    let tx = TransactionRequestBuilder::new()
        .foreign_accounts([foreign])
        .build()
        .context("build tx")?;

    match client.execute_transaction(account_id, tx).await {
        Ok(_) => {
            println!("ok ({n} keys fetched for FPI)");
            if n > LIMIT {
                bail!("expected over-limit FPI fetch to fail");
            }
            Ok(())
        }
        Err(err) => {
            println!("{err:#?}");
            let msg = format!("{err:?}");
            if n > LIMIT && msg.contains("storage_map_key") && msg.contains(&LIMIT.to_string()) {
                Ok(())
            } else if n <= LIMIT {
                Err(err).with_context(|| format!("expected {n}-key FPI fetch to succeed"))
            } else {
                Err(err).context("over-limit FPI failed without storage_map_key limiter error")
            }
        }
    }
}

async fn discover_map_slot(account_id: AccountId) -> Result<StorageSlotName> {
    let rpc = GrpcClient::new(&Endpoint::testnet(), 30_000);
    let (_block, proof) = rpc
        .get_account(
            account_id,
            GetAccountRequest::new()
                .with_storage(StorageMapFetch::Skip)
                .with_vault(VaultFetch::Skip),
        )
        .await
        .context("discover map slot")?;
    proof
        .storage_header()
        .context("no storage header")?
        .slots()
        .find(|s| s.slot_type() == StorageSlotType::Map)
        .map(|s| s.name().clone())
        .context("no map slots on foreign account")
}

async fn build_client() -> Result<(Client<FilesystemKeyStore>, Arc<FilesystemKeyStore>)> {
    let dir = std::env::temp_dir().join(format!(
        "demo_fpi_limit_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    std::fs::create_dir_all(&dir)?;
    let store = Arc::new(SqliteStore::new(dir.join("store.sqlite3")).await?);
    let keystore = Arc::new(FilesystemKeyStore::new(dir.join("keystore"))?);
    let client = ClientBuilder::for_testnet()
        .store(store)
        .authenticator(keystore.clone())
        .build()
        .await?;
    Ok((client, keystore))
}

async fn create_local_account(
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<AccountId> {
    let mut seed = [0u8; 32];
    client.rng().fill_bytes(&mut seed);
    let key = AuthSecretKey::new_ecdsa_k256_keccak();
    let account = AccountBuilder::new(seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key.public_key().to_commitment(),
            AuthScheme::EcdsaK256Keccak,
        ))
        .with_component(BasicWallet)
        .build()?;
    client.add_account(&account, false).await?;
    keystore.add_key(&key, account.id()).await?;
    Ok(account.id())
}
