use std::{sync::Arc, thread::sleep, time::Duration};

use anyhow::Result;
use miden_client::{
    RemoteTransactionProver,
    account::AccountId,
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    rpc::{Endpoint, GrpcClient},
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
    transaction::TransactionRequestBuilder,
};
use miden_client_sqlite_store::SqliteStore;

use crate::{
    execution::{PoolStateDelta, Trade, make_exec_script},
    pool::{deploy_pool, link_pool, link_storage_utils, read_masm_file},
    user::get_users,
};

mod execution;
mod pool;
mod user;

#[tokio::main]
async fn main() -> Result<()> {
    // miden client
    // let remote_prover = Arc::new(RemoteTransactionProver::new(
    //     "https://tx-prover.testnet.miden.io",
    // ));
    let sqlite_store = SqliteStore::new("store.sqlite3".into()).await?;
    let store = Arc::new(sqlite_store);
    let rpc_client = Arc::new(GrpcClient::new(&Endpoint::testnet(), 30_000));
    let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

    // Build client with remote prover as default
    let mut client = ClientBuilder::new()
        // .prover(remote_prover.clone())
        .store(store)
        .rpc(rpc_client)
        .authenticator(keystore)
        .build()
        .await?;

    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;

    println!("Client ready.");

    // spawn the user accounts
    let users = get_users(10, &mut client).await?;

    // spawn the pool account
    let (pool, pool_component) = deploy_pool(&mut client, users.clone()).await?;

    println!(
        "Pool deployed. BECH32: {}, HEX: {}",
        pool.id().to_bech32(Endpoint::testnet().to_network_id()),
        pool.id().to_hex()
    );

    let tx = TransactionRequestBuilder::new().build()?;
    client.add_account(&pool, true).await?;
    client.submit_new_transaction(pool.id(), tx).await?;
    client.sync_state().await?;

    // sleep(Duration::from_secs(4));

    println!("Pool touched.");

    let sim_runs = 10;
    let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
    let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;

    for n in 0..sim_runs {
        println!("SIM RUN {n}");

        let mut trades = Vec::new();
        let sell_asset = if n % 2 == 0 { asset0 } else { asset1 };
        let buy_asset = if n % 2 == 0 { asset1 } else { asset0 };
        let trade_amount = 100;
        let pool_state_deltas = vec![
            PoolStateDelta {
                asset: sell_asset,
                add_amount: 0,
                sub_amount: users.len() as u64 * trade_amount,
            },
            PoolStateDelta {
                asset: buy_asset,
                add_amount: users.len() as u64 * trade_amount,
                sub_amount: 0,
            },
        ];
        for user in &users {
            let trade = Trade {
                user: *user,
                sell_asset,
                buy_asset,
                sell_amount: trade_amount,
                buy_amount: trade_amount,
            };
            trades.push(trade);
        }

        let tx_script = make_exec_script(trades, pool_state_deltas);

        println!("SCRIPT \n\n{tx_script}\n\n");
        // run simulation

        let cb = link_pool(client.code_builder())?;
        let tx_script = cb
            // .with_linked_module("zoro_miden::pool", code)?
            // .with_dynamically_linked_library(pool_component.component_code())?
            .compile_tx_script(tx_script)?;

        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .build()?;
        client.submit_new_transaction(pool.id(), tx_req).await?;
        client.sync_state().await?;
    }

    Ok(())
}
