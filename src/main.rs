use std::sync::Arc;

use anyhow::Result;
use miden_client::{
    RemoteTransactionProver,
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    rpc::{Endpoint, GrpcClient},
    transaction::TransactionRequestBuilder,
};
use miden_client_sqlite_store::SqliteStore;

use crate::{execution::make_exec_script, pool::deploy_pool, user::get_users};

mod execution;
mod pool;
mod user;

#[tokio::main]
async fn main() -> Result<()> {
    // miden client
    let remote_prover = Arc::new(RemoteTransactionProver::new(
        "https://tx-prover.testnet.miden.io",
    ));
    let sqlite_store = SqliteStore::new("store.sqlite3".into()).await?;
    let store = Arc::new(sqlite_store);
    let rpc_client = Arc::new(GrpcClient::new(&Endpoint::testnet(), 30_000));
    let keystore = Arc::new(FilesystemKeyStore::new("store.sqlite3".into())?);

    // Build client with remote prover as default
    let mut client = ClientBuilder::new()
        .prover(remote_prover.clone())
        .store(store)
        .rpc(rpc_client)
        .authenticator(keystore)
        .build()
        .await?;

    // spawn the user accounts
    let users = get_users(10, &mut client).await?;

    // spawn the pool account

    let (pool, pool_component) = deploy_pool(&mut client, users)?;

    let sim_runs = 10;

    for _ in 0..sim_runs {
        let trades = Vec::new();
        let pool_state_deltas = Vec::new();
        let tx_script = make_exec_script(trades, pool_state_deltas);

        println!("SCRIPT \n\n{tx_script}\n\n");
        // run simulation
        let tx_script = client
            .code_builder()
            .with_dynamically_linked_library(pool_component.component_code())?
            .compile_tx_script(tx_script)?;

        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .build()?;
        client.submit_new_transaction(pool.id(), tx_req).await?;
        client.sync_state().await?;
    }

    Ok(())
}
