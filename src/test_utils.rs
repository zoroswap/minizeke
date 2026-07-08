use std::{cell::OnceCell, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use dotenv::dotenv;
use miden_client::{
    Client, RemoteTransactionProver,
    account::{
        AccountBuilder, AccountId, AccountType,
        component::{
            BasicWallet, BurnPolicyConfig, FungibleFaucet, MintPolicyConfig, PolicyRegistration,
            TokenName, TokenPolicyManager,
        },
    },
    address::NetworkId,
    asset::{AssetAmount, FungibleAsset, TokenSymbol},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
    note::NoteType,
    testing::{
        account_id::{ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2},
        common::wait_for_blocks,
    },
    transaction::TransactionRequestBuilder,
};
use miden_client_sqlite_store::SqliteStore;
use rand::RngCore;
use tracing::info;

use crate::{
    message_broker::message_broker::MessageBroker, miden_env::MidenNetwork,
    miden_execution::MidenExecution, vault::deploy_vault,
};

const ASSET_0: OnceCell<AccountId> = OnceCell::new();
const ASSET_1: OnceCell<AccountId> = OnceCell::new();

const USERS: OnceCell<AccountId> = OnceCell::new();

pub fn get_asset0() -> AccountId {
    *ASSET_0.get_or_init(|| AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap())
}

pub fn get_asset1() -> AccountId {
    *ASSET_1.get_or_init(|| AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap())
}

pub async fn get_client() -> Result<Client<FilesystemKeyStore>> {
    dotenv().ok();

    const DEFAULT_TX_PROVER_TIMEOUT_SECS: u64 = 30;

    let network = MidenNetwork::from_env();
    let tx_prover_url = env::var("TX_PROVER_URL")
        .ok()
        .or_else(|| network.tx_prover_url());
    let tx_prover_timeout_secs = env::var("TX_PROVER_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TX_PROVER_TIMEOUT_SECS);

    let prover_timeout = Duration::from_secs(tx_prover_timeout_secs);

    let sqlite_store = SqliteStore::new(network.store_path().into()).await?;
    let store = Arc::new(sqlite_store);
    let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

    let mut client_builder = MidenNetwork::client_builder()
        .in_debug_mode(true.into())
        .store(store)
        .authenticator(keystore);

    if let Some(ref url) = tx_prover_url {
        let remote_prover =
            Arc::new(RemoteTransactionProver::new(url.clone()).with_timeout(prover_timeout));
        info!(
            network = network.as_str(),
            prover = %url,
            timeout_secs = tx_prover_timeout_secs,
            "Using Miden network with remote prover"
        );
        client_builder = client_builder.prover(remote_prover);
    } else {
        info!(
            network = network.as_str(),
            "Using Miden network with local prover"
        );
    }

    let client = client_builder.build().await?;
    Ok(client)
}

pub async fn get_miden_execution() -> Result<MidenExecution> {
    dotenv().ok();
    let message_broker = Arc::new(MessageBroker::new());
    let miden_execution = MidenExecution::initialize(message_broker).await?;
    Ok(miden_execution)
}

pub async fn get_vault(client: &mut Client<FilesystemKeyStore>) -> Result<AccountId> {
    dotenv().ok();
    let vault = deploy_vault(client).await?;
    Ok(vault.id())
}

pub async fn get_user(client: &mut Client<FilesystemKeyStore>) -> Result<AccountId> {
    let keystore_path = PathBuf::from("./keystore");
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_path).unwrap());

    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());

    // Build the account
    let acc = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::Falcon512Poseidon2,
        ))
        .with_component(BasicWallet)
        .build()
        .unwrap();

    info!(acc_id = acc.id().to_hex(), "New user account");

    // Add the account to the client
    client.add_account(&acc, false).await?;

    // Add the key pair to the keystore
    keystore.add_key(&key_pair, acc.id()).await.unwrap();

    client.sync_state().await?;

    Ok(acc.id())
}

pub async fn get_faucet(
    client: &mut Client<FilesystemKeyStore>,
    symbol: &str,
) -> Result<AccountId> {
    let mut init_seed = [0u8; 32];
    client.rng().fill_bytes(&mut init_seed);
    let keystore_path = PathBuf::from("./keystore");
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_path).unwrap());
    // Faucet parameters
    let name = TokenName::new(symbol).unwrap();
    let symbol = TokenSymbol::new(symbol).unwrap();
    let decimals = 8;
    let max_supply = AssetAmount::new(10_000_000_000).unwrap();

    // Generate key pair
    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());
    let faucet_account = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::Falcon512Poseidon2,
        ))
        .with_component(
            FungibleFaucet::builder()
                .name(name)
                .symbol(symbol)
                .decimals(decimals)
                .max_supply(max_supply)
                .build()
                .unwrap(),
        )
        .with_components(
            TokenPolicyManager::new()
                .with_mint_policy(MintPolicyConfig::AllowAll, PolicyRegistration::Active)
                .unwrap()
                .with_burn_policy(BurnPolicyConfig::AllowAll, PolicyRegistration::Active)
                .unwrap(),
        )
        .build()
        .unwrap();

    // Add the faucet to the client
    client.add_account(&faucet_account, false).await?;

    // Add the key pair to the keystore
    use miden_client::keystore::Keystore;
    keystore
        .add_key(&key_pair, faucet_account.id())
        .await
        .unwrap();

    let faucet_account_id_bech32 = faucet_account.id().to_bech32(NetworkId::Testnet);
    println!("Faucet account ID: {:?}", faucet_account_id_bech32);

    // Resync to show newly deployed faucet
    client.sync_state().await?;

    Ok(faucet_account.id())
}

pub async fn get_funded_user(
    client: &mut Client<FilesystemKeyStore>,
) -> Result<(AccountId, AccountId)> {
    let faucet_id = get_faucet(client, "TEST").await?;
    let user_id = get_user(client).await?;
    let fungible_asset = FungibleAsset::new(faucet_id, 10_000).unwrap();
    let transaction_request = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(fungible_asset, user_id, NoteType::Public, client.rng())
        .unwrap();

    println!("mint tx request built");

    client
        .submit_new_transaction(faucet_id, transaction_request)
        .await?;

    loop {
        // Resync to get the latest data
        client.sync_state().await?;

        let consumable_notes = client.get_consumable_notes(Some(user_id)).await?;
        let notes = consumable_notes
            .iter()
            .map(|(note, _)| note.clone().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        if !notes.is_empty() {
            println!("Consuming notes now...");
            let transaction_request = TransactionRequestBuilder::new()
                .build_consume_notes(notes)
                .unwrap();

            let tx_id = client
                .submit_new_transaction(user_id, transaction_request)
                .await?;
            println!("Consume minted tokens TX: {:?}", tx_id);
            break;
        } else {
            println!("Waiting...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok((user_id, faucet_id))
}

pub async fn touch_account(
    client: &mut Client<FilesystemKeyStore>,
    account_id: &AccountId,
) -> Result<()> {
    let tx_req = TransactionRequestBuilder::new().build()?;
    client.submit_new_transaction(*account_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(client, 1).await;
    Ok(())
}
