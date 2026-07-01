use std::{cell::OnceCell, env, sync::Arc, time::Duration};

use anyhow::Result;
use miden_client::{
    Client, RemoteTransactionProver,
    account::AccountId,
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
};
use miden_client_sqlite_store::SqliteStore;
use tracing::info;

use crate::{
    message_broker::message_broker::MessageBroker,
    miden_execution::{self, MidenExecution},
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
    const DEFAULT_TX_PROVER_URL: &str = "https://tx-prover.testnet.miden.io";
    const DEFAULT_TX_PROVER_TIMEOUT_SECS: u64 = 30;

    let tx_prover_url =
        env::var("TX_PROVER_URL").unwrap_or_else(|_| DEFAULT_TX_PROVER_URL.to_string());
    let tx_prover_timeout_secs = env::var("TX_PROVER_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TX_PROVER_TIMEOUT_SECS);

    let remote_prover = Arc::new(
        RemoteTransactionProver::new(tx_prover_url.clone())
            .with_timeout(Duration::from_secs(tx_prover_timeout_secs)),
    );

    info!(
        prover = %tx_prover_url,
        timeout_secs = tx_prover_timeout_secs,
        "Using Miden testnet (rpc.testnet.miden.io)"
    );

    let sqlite_store = SqliteStore::new("store.testnet.sqlite3".into()).await?;
    let store = Arc::new(sqlite_store);
    let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

    let client = ClientBuilder::for_localhost()
        // .prover(remote_prover)
        .in_debug_mode(true.into())
        .store(store)
        .authenticator(keystore)
        .build()
        .await?;
    Ok(client)
}

pub async fn get_miden_execution() -> Result<MidenExecution> {
    let message_broker = Arc::new(MessageBroker::new());
    let miden_execution = MidenExecution::initialize(message_broker).await?;
    Ok(miden_execution)
}
