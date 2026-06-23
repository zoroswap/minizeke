use std::{
    sync::Arc,
    thread::sleep,
    time::{Duration, Instant},
};

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
    //miden client
    let remote_prover = Arc::new(RemoteTransactionProver::new(
        "https://tx-prover.testnet.miden.io",
    ));
    let sqlite_store = SqliteStore::new("store.sqlite3".into()).await?;
    let store = Arc::new(sqlite_store);
    let rpc_client = Arc::new(GrpcClient::new(&Endpoint::testnet(), 30_000));
    let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

    // Build client with remote prover as default
    let mut client = ClientBuilder::new()
        //.in_debug_mode(true.into())
        .prover(remote_prover.clone())
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

    let pool_0_balance = 10_000_000;
    let pool_1_balance = 10_000_000;
    // spawn the pool account
    let (pool, pool_component) = deploy_pool(
        &mut client,
        users.clone(),
        pool_0_balance.clone(),
        pool_1_balance.clone(),
    )
    .await?;

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

    let sim_runs = 5;
    let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
    let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;

    let mut current_pool_0_balance = pool_0_balance;
    let mut current_pool_1_balance = pool_1_balance;
    for n in 0..sim_runs {
        let instant = Instant::now();
        println!("SIM RUN {n}");

        let mut trades = Vec::new();
        let (sell_asset, sell_pool_index) = if n % 2 == 0 { (asset0, 0) } else { (asset1, 1) };
        let (buy_asset, buy_pool_index) = if n % 2 == 0 { (asset1, 1) } else { (asset0, 0) };
        let trade_amount = 10;

        let mut sell_pool_balance = 0;
        let mut buy_pool_balance = 0;
        if sell_pool_index == 0 {
            current_pool_0_balance -= users.len() as u64 * trade_amount;
            sell_pool_balance = current_pool_0_balance;

            current_pool_1_balance += users.len() as u64 * trade_amount;
            buy_pool_balance = current_pool_1_balance;
        } else {
            current_pool_1_balance -= users.len() as u64 * trade_amount;
            sell_pool_balance = current_pool_1_balance;

            current_pool_0_balance += users.len() as u64 * trade_amount;
            buy_pool_balance = current_pool_0_balance;
        }
        let pool_state_deltas = vec![
            PoolStateDelta {
                pool_index: sell_pool_index,
                set_amount: sell_pool_balance,
            },
            PoolStateDelta {
                pool_index: buy_pool_index,
                set_amount: buy_pool_balance,
            },
        ];
        for (user_index, _) in users.iter().enumerate() {
            let trade = Trade {
                user_index: user_index as u64,
                sell_asset_index: sell_pool_index as u64,
                buy_asset_index: buy_pool_index as u64,
                sell_amount: trade_amount,
                buy_amount: trade_amount,
            };
            trades.push(trade);
        }

        let tx_script = make_exec_script(trades, pool_state_deltas);

        println!("SCRIPT \n\n{tx_script}\n\n");
        // println!("SCRIPT \n\n{tx_script}\n\n");
        // run simulation

        let cb = link_pool(client.code_builder())?;
        let tx_script = cb
            // .with_linked_module("zoro_miden::pool", code)?
            // .with_dynamically_linked_library(pool_component.component_code())?
            .compile_tx_script(tx_script)?;

        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .build()?;

        // let tx = client.submit_new_transaction(pool.id(), tx_req).await?;

        let tx_result = client.execute_transaction(pool.id(), tx_req).await?;
        let measurements = tx_result.executed_transaction().measurements();
        println!(
            "Cycle count: {}, auth: {}",
            measurements.total_cycles(),
            measurements.auth_procedure
        );
        let prove_started = Instant::now();
        let proven_transaction = client
            .prove_transaction_with(&tx_result, client.prover())
            .await?;
        let prove_elapsed = prove_started.elapsed();
        let submission_height = client
            .submit_proven_transaction(proven_transaction, &tx_result)
            .await?;
        client
            .apply_transaction(&tx_result, submission_height)
            .await?;
        println!("Elapsed: {prove_elapsed:?}");
        client.sync_state().await?;
    }

    Ok(())
}
