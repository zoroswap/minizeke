use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use dotenv::dotenv;
use miden_client::account::AccountId;
use tokio::runtime::Builder;
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;

mod api;
mod execution_script;
mod intent;
mod message_broker;
mod miden_env;
mod miden_execution;
mod oracle_sse;
mod order;
mod pool;
mod price;
mod processing;
mod serde;
mod store;
pub mod test_utils;
mod user;
mod websocket;

use crate::{
    message_broker::message_broker::{MessageBroker, StatsEvent},
    miden_execution::MidenExecution,
    oracle_sse::OracleSSEClient,
    pool::PoolState,
    processing::Processing,
    store::Store,
    user::Users,
    websocket::connection_manager::ConnectionManager,
};

fn main() {
    dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new("info,minizeke=debug,miden_core=off,log=warn")
            }),
        )
        .with_target(false)
        .init();

    let message_broker = Arc::new(MessageBroker::new());
    let (init_tx, init_rx) = std::sync::mpsc::sync_channel(1);

    std::thread::scope(|s| {
        let message_broker_for_miden = message_broker.clone();
        s.spawn(move || {
            let rt = Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap_or_else(|err| {
                    panic!("Failed building runtime for Miden execution: {err:?}")
                });
            rt.block_on(async move {
                println!("[INIT] Initializing Miden components");
                let mut miden_execution = MidenExecution::initialize(message_broker_for_miden)
                    .await
                    .unwrap();

                init_tx
                    .send((
                        miden_execution.users(),
                        miden_execution.pool_states(),
                        miden_execution.pool_id(),
                    ))
                    .unwrap();

                println!("[RUN] Starting Miden execution");
                if let Err(e) = miden_execution.start().await {
                    eprintln!("Critical error on miden_execution: {e}. Exiting with status 1.");
                    std::process::exit(1);
                }
            });
        });

        let (initial_users, initial_pool_states, pool_id) = init_rx
            .recv()
            .expect("Miden init thread failed before sending init data");

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

    println!("[RUN] Starting stats updater");
    {
        let store_for_stats = store.clone();
        let message_broker_for_stats = message_broker.clone();
        tokio::spawn(async move {
            let mut rx = message_broker_for_stats.subscribe_order_updates();
            loop {
                match rx.recv().await {
                    Ok(update) => {
                        store_for_stats.apply_order_update(update);
                        let stats = store_for_stats.order_stats();
                        let _ = message_broker_for_stats.broadcast_stats(StatsEvent::now(stats));
                    }
                    Err(RecvError::Lagged(n)) => {
                        warn!("stats updater lagged behind by {n} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    println!("[RUN] Starting ZEKE server");
    api::start(connection_manager, message_broker, store).await?;

    Ok(())
}
