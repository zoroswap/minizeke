use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use dotenv::dotenv;
use miden_client::account::AccountId;
use tokio::runtime::Builder;

use crate::{
    message_broker::message_broker::MessageBroker, miden_execution::MidenExecution,
    oracle_sse::OracleSSEClient, pool::PoolState, processing::Processing, store::Store,
    user::Users, websocket::connection_manager::ConnectionManager,
};

mod api;
mod execution;
mod message_broker;
mod miden_execution;
mod oracle_sse;
mod order;
mod pool;
mod price;
mod processing;
mod serde;
mod store;
mod user;
mod websocket;

fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,minizeke=debug")),
        )
        .with_target(false)
        .init();

    let message_broker = Arc::new(MessageBroker::new());

    println!("[INIT] Initializing Miden components");
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|err| panic!("Failed building runtime for trading engine: {err:?}"));
    let mut miden_execution = rt.block_on(async {
        MidenExecution::initialize(message_broker.clone())
            .await
            .unwrap()
    });

    let initial_users = miden_execution.users();
    let initial_pool_states = miden_execution.pool_states();
    let pool_id = miden_execution.pool_id();

    std::thread::scope(|s| {
        s.spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|err| {
                    panic!("Failed building runtime for trading engine: {err:?}")
                });
            rt.block_on(async {
                println!("[RUN] Starting Miden execution");
                if let Err(e) = miden_execution.start().await {
                    eprintln!("Critical error on miden_execution: {e}. Exiting with status 1.");
                    std::process::exit(1);
                }
            });
        });

        let _ = main_tokio(initial_users, initial_pool_states, pool_id, message_broker);
    });
}

#[tokio::main]
async fn main_tokio(
    initial_users: Users,
    initial_pool_states: HashMap<AccountId, PoolState>,
    pool_id: AccountId,
    message_broker: Arc<MessageBroker>,
) -> Result<()> {
    println!("[INIT] Connection manager");
    let connection_manager = Arc::new(ConnectionManager::with_message_broker(
        message_broker.clone(),
    ));

    println!("[INIT] Initializing Store");
    let store = Arc::new(Store::new(
        pool_id,
        initial_users.clone(),
        initial_pool_states.clone(),
    ));

    let mut oracle_client = OracleSSEClient::new(store.clone(), message_broker.clone());
    println!("[INIT] Initializing oracle prices");
    oracle_client.init_prices().await?;

    println!("[INIT] Initializing Processing");
    let mut processing = Processing::new(
        message_broker.clone(),
        initial_users.clone(),
        initial_pool_states.clone(),
    )
    .await?;

    println!("[RUN] Starting Processing");
    tokio::spawn(async move {
        processing.start().await;
    });

    println!("[RUN] Starting oracle listener");
    tokio::spawn(async move {
        if let Err(e) = oracle_client.start().await {
            eprintln!("Critical error on oracle client: {e}. Exiting with status 1.");
            std::process::exit(1);
        }
    });

    println!("[RUN] Starting WebSocket heartbeat task");
    connection_manager.clone().start_heartbeat_task();

    // Start event forwarding from MessageBroker to WebSocket clients
    println!("[RUN] Starting WebSocket event forwarding");
    connection_manager.clone().start_event_forwarding();

    println!("[RUN] Starting ZEKE server");
    api::start(connection_manager, message_broker, store).await?;

    Ok(())
}
