use std::{cell::OnceCell, env, sync::Arc, time::Duration};

use anyhow::Result;
use miden_client::{
    Client, RemoteTransactionProver,
    account::AccountId,
    keystore::FilesystemKeyStore,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
};
use miden_client_sqlite_store::SqliteStore;
use dotenv::dotenv;
use tracing::info;

use crate::{
    message_broker::message_broker::MessageBroker,
    miden_env::MidenNetwork,
    miden_execution::MidenExecution,
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
        let remote_prover = Arc::new(
            RemoteTransactionProver::new(url.clone()).with_timeout(prover_timeout),
        );
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
