use std::sync::Arc;

use anyhow::Result;
use dotenv::dotenv;

use crate::{
    message_broker::message_broker::MessageBroker, miden_execution::MidenExecution,
    oracle_sse::OracleSSEClient, processing::Processing, store::Store,
    websocket::connection_manager::ConnectionManager,
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

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let message_broker = Arc::new(MessageBroker::new());
    let connection_manager = Arc::new(ConnectionManager::with_message_broker(
        message_broker.clone(),
    ));
    println!("[INIT] Initializing Miden components");
    let mut miden_execution = MidenExecution::initialize(message_broker.clone()).await?;

    let initial_users = miden_execution.users();
    let initial_pool_states = miden_execution.pool_states();
    let pool_id = miden_execution.pool_id();

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

    println!("[RUN] Starting Miden Execution");
    tokio::spawn(async move {
        if let Err(e) = miden_execution.start().await {
            eprintln!("Critical error on miden_execution: {e}. Exiting with status 1.");
            std::process::exit(1);
        }
    });

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
