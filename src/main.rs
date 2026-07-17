use std::{collections::HashMap, env, fs, path::PathBuf, sync::Arc};

use anyhow::Result;
use dotenv::dotenv;
use miden_client::account::AccountId;
use tokio::runtime::Builder;
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use minizeke::*;

use crate::{
    analytics_store::AnalyticsStore,
    auth::{AuthConfig, AuthStore},
    deployment::AssetInfo,
    fee_store::{FeeStore, apply_fee_states},
    history::{HistoryStore, start_history_service},
    lp::LpService,
    message_broker::message_broker::{FeeStateEvent, MessageBroker, StatsEvent},
    miden_execution::MidenExecution,
    oracle_sse::OracleSSEClient,
    pool::PoolState,
    processing::Processing,
    store::Store,
    websocket::connection_manager::ConnectionManager,
};

/// Deployment ids + initial pool states handed from the Miden init thread to the server.
struct InitData {
    pool_states: HashMap<AccountId, PoolState>,
    vault_id: AccountId,
    assets: Vec<AssetInfo>,
    pools: Vec<AccountId>,
}

fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new("info,minizeke=debug,miden_core=off,log=warn")
    });
    let log_dir = env::var("LOG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("logs"));
    fs::create_dir_all(&log_dir).unwrap_or_else(|error| {
        panic!(
            "failed to create log directory {}: {error}",
            log_dir.display()
        )
    });
    let file_appender = tracing_appender::rolling::daily(log_dir, "minizeke.log");
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(false)
                .with_writer(file_writer),
        )
        .init();
    guard
}

fn main() {
    dotenv().ok();

    let _log_guard = init_tracing();

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
                    .send(InitData {
                        pool_states: miden_execution.pool_states(),
                        vault_id: miden_execution.vault_id(),
                        assets: miden_execution.assets(),
                        pools: miden_execution.pools(),
                    })
                    .unwrap();

                println!("[RUN] Starting Miden execution");
                if let Err(e) = miden_execution.start().await {
                    eprintln!("Critical error on miden_execution: {e}. Exiting with status 1.");
                    std::process::exit(1);
                }
            });
        });

        let init_data = init_rx
            .recv()
            .expect("Miden init thread failed before sending init data");

        let _ = main_tokio(init_data, message_broker);
    });
}

#[tokio::main]
async fn main_tokio(mut init_data: InitData, message_broker: Arc<MessageBroker>) -> Result<()> {
    println!("[INIT] Initializing authentication database");
    let auth_config = AuthConfig::new(
        env::var("AUTH_DOMAIN").unwrap_or_else(|_| "minizeke".to_owned()),
        env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_owned()),
        env::var("AUTH_CHALLENGE_TTL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(60),
        env::var("AUTH_SESSION_TTL_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1_800),
    )?;
    let auth_path = env::var("AUTH_DB_PATH").unwrap_or_else(|_| {
        let network = env::var("MIDEN_NETWORK").unwrap_or_else(|_| "testnet".to_owned());
        format!("auth.{}.sqlite3", network.to_ascii_lowercase())
    });
    let auth_store = Arc::new(AuthStore::open(auth_path, auth_config)?);
    println!("[INIT] Initializing analytics database");
    let analytics_store = Arc::new(AnalyticsStore::open_from_env()?);
    analytics::initialize(analytics_store.clone()).await?;
    println!("[INIT] Initializing dynamic fee database");
    let fee_store = Arc::new(FeeStore::open_from_env()?);
    let now = chrono::Utc::now().timestamp() as u64;
    let active_fee_states = fee_store.active_states(now)?;
    apply_fee_states(&mut init_data.pool_states, &active_fee_states);
    println!("[INIT] Connection manager");
    let connection_manager = Arc::new(ConnectionManager::with_message_broker(
        message_broker.clone(),
    ));

    println!("[INIT] Initializing Store");
    let store = Arc::new(Store::new(
        init_data.vault_id,
        init_data.assets.clone(),
        init_data.pools.clone(),
        init_data.pool_states.clone(),
    ));

    println!("[INIT] Initializing history database");
    let history = Arc::new(HistoryStore::open_from_env()?);
    start_history_service(history.clone(), message_broker.clone());

    {
        let store = store.clone();
        let mut rx = message_broker.subscribe_oracle_prices();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => match AccountId::from_hex(&event.faucet_id) {
                        Ok(faucet_id) => store.set_oracle_price(faucet_id, event.price),
                        Err(error) => warn!(
                            faucet_id = %event.faucet_id,
                            %error,
                            "ignoring oracle event with invalid faucet id"
                        ),
                    },
                    Err(RecvError::Lagged(n)) => {
                        warn!("oracle store updater lagged behind by {n} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    let mut oracle_client =
        OracleSSEClient::new(store.clone(), message_broker.clone(), &init_data.assets);
    println!("[INIT] Initializing oracle prices");
    oracle_client.init_prices().await?;

    println!("[INIT] Initializing Processing");
    println!("[INIT] Initializing LP worker");
    let lp_service: LpService =
        lp::initialize(message_broker.clone(), init_data.pool_states.clone()).await?;
    let mut processing = Processing::new(
        message_broker.clone(),
        init_data.pool_states.clone(),
        init_data.vault_id,
        init_data.assets,
        init_data.pools,
        lp_service.store(),
    )
    .await?;

    println!("[RUN] Starting Processing");
    tokio::spawn(async move {
        processing.start().await;
    });

    println!("[RUN] Starting volatility-fee expiry task");
    {
        let fee_store = fee_store.clone();
        let message_broker = message_broker.clone();
        tokio::spawn(async move {
            let interval_secs = env::var("FEE_EXPIRY_CHECK_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(5);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let now = chrono::Utc::now().timestamp() as u64;
                match fee_store.expire(now) {
                    Ok(true) => {
                        let _ = message_broker.broadcast_fee_state(FeeStateEvent {
                            fee_states: HashMap::new(),
                            timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        });
                    }
                    Ok(false) => {}
                    Err(error) => warn!(%error, "failed to expire volatility fee"),
                }
            }
        });
    }

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

    println!("[RUN] Starting pool-state store updater");
    {
        let store = store.clone();
        let message_broker = message_broker.clone();
        tokio::spawn(async move {
            let mut rx = message_broker.subscribe_pool_state();
            loop {
                match rx.recv().await {
                    Ok(event) => store.set_pool_states(event.pool_states),
                    Err(RecvError::Lagged(n)) => {
                        warn!("pool-state store updater lagged behind by {n} messages");
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }

    println!("[RUN] Starting ZEKE server");
    api::start(
        connection_manager,
        message_broker,
        store,
        history,
        lp_service,
        fee_store,
        auth_store,
        analytics_store,
    )
    .await?;

    Ok(())
}
