use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use dotenv::dotenv;
use miden_client::account::AccountId;
use tokio::runtime::Builder;
use tokio::sync::broadcast::error::RecvError;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use minizeke::*;

use crate::{
    analytics_store::AnalyticsStore,
    auth::{AuthConfig, AuthStore},
    deployment::AssetInfo,
    execution_store::ExecutionStore,
    fee_store::{FeeStore, apply_fee_states},
    finality_observer::FinalityObserver,
    history::{HistoryStore, start_history_service},
    lp::LpService,
    message_broker::message_broker::{FeeStateEvent, MessageBroker, StatsEvent},
    miden_execution::MidenExecution,
    oracle_sse::OracleSSEClient,
    pool::{PoolState, fetch_vault_user_placement_storage},
    pool_registry::PoolRegistry,
    processing::Processing,
    store::Store,
    vault::user_pool_from_storage,
    websocket::connection_manager::ConnectionManager,
};

/// Deployment ids + initial pool states handed from the Miden init thread to the server.
struct InitData {
    pool_states: HashMap<AccountId, PoolState>,
    vault_id: AccountId,
    assets: Vec<AssetInfo>,
    pools: Vec<AccountId>,
    pool_registry: Arc<PoolRegistry>,
}

fn init_tracing() -> tracing_appender::non_blocking::WorkerGuard {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::new(
            "info,minizeke=info,miden_client=warn,miden_tx_prover=warn,miden_prover=warn,\
             miden_core=off,tower_http=warn,hyper=warn,reqwest=warn,log=warn",
        )
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
    let (miden_start_tx, miden_start_rx) = std::sync::mpsc::sync_channel(1);

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
                        pool_registry: miden_execution.pool_registry(),
                    })
                    .unwrap();

                miden_start_rx
                    .recv()
                    .expect("main runtime stopped before workers became ready");
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

        let _ = main_tokio(init_data, message_broker, miden_start_tx);
    });
}

#[tokio::main]
async fn main_tokio(
    mut init_data: InitData,
    message_broker: Arc<MessageBroker>,
    miden_start_tx: std::sync::mpsc::SyncSender<()>,
) -> Result<()> {
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
    {
        let auth_store = auth_store.clone();
        tokio::spawn(async move {
            let interval_secs = env::var("AUTH_PURGE_INTERVAL_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(300_u64)
                .max(1);
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                let store = auth_store.clone();
                let now = chrono::Utc::now().timestamp() as u64;
                match tokio::task::spawn_blocking(move || store.purge(now)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => warn!(%error, "failed to purge auth records"),
                    Err(error) => warn!(%error, "auth purge worker failed"),
                }
            }
        });
    }
    println!("[INIT] Initializing analytics database");
    let analytics_store = Arc::new(AnalyticsStore::open_from_env()?);
    analytics::initialize(analytics_store.clone(), message_broker.clone()).await?;
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
        init_data.pool_registry.clone(),
        init_data.pool_states.clone(),
    ));

    println!("[INIT] Initializing history database");
    let history = Arc::new(HistoryStore::open_from_env()?);
    start_history_service(history.clone(), message_broker.clone());
    println!("[INIT] Initializing execution database");
    let execution_store = Arc::new(ExecutionStore::open_from_env()?);
    {
        let allowed_assets: HashSet<AccountId> = init_data
            .assets
            .iter()
            .map(|asset| asset.faucet_id)
            .collect();
        let allowed_pools: HashSet<AccountId> = init_data.pools.iter().copied().collect();
        let now = chrono::Utc::now().timestamp_millis() as u64;
        let vault_id = init_data.vault_id;
        // Collect candidates first so async targeted vault reads stay outside the sync
        // SQLite predicate, then fail any order whose user is unbound or on a removed shard.
        let (candidates, mut stale_ids) = execution_store.list_prebatch_orders()?;
        for order in candidates {
            if order.details.asset_in == order.details.asset_out
                || !allowed_assets.contains(&order.details.asset_in)
                || !allowed_assets.contains(&order.details.asset_out)
            {
                stale_ids.push(order.id);
                continue;
            }
            match fetch_vault_user_placement_storage(vault_id, order.user_id).await {
                Ok(storage) => match user_pool_from_storage(&storage, order.user_id) {
                    Ok(Some(pool_id)) if allowed_pools.contains(&pool_id) => {}
                    Ok(Some(_)) | Ok(None) | Err(_) => stale_ids.push(order.id),
                },
                Err(error) => {
                    warn!(
                        %error,
                        user = %order.user_id.to_hex(),
                        "vault placement unavailable during stale-order purge; keeping order"
                    );
                }
            }
        }
        let purged = execution_store.fail_orders_by_ids(&stale_ids, "stale_after_redeploy", now)?;
        if purged > 0 {
            info!(
                purged,
                "failed stale admitted/processing_claimed orders after redeploy"
            );
        }
    }

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
    let pool_registry = init_data.pool_registry.clone();
    let mut processing = Processing::new(
        message_broker.clone(),
        init_data.pool_states.clone(),
        init_data.vault_id,
        init_data.assets,
        init_data.pool_registry,
        lp_service.store(),
        execution_store.clone(),
        fee_store.clone(),
    )
    .await?;
    store.set_pool_states(processing.pool_states());

    println!("[RUN] Starting Processing");
    std::thread::Builder::new()
        .name("processing-db-worker".to_owned())
        .spawn(move || {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("processing runtime");
            runtime.block_on(async move {
                processing.start().await;
            });
        })
        .expect("spawn processing worker");

    println!("[RUN] Starting Finality observer");
    let pool_registry_for_provisioner = pool_registry.clone();
    {
        let message_broker = message_broker.clone();
        let execution_store = execution_store.clone();
        std::thread::Builder::new()
            .name("finality-observer".to_owned())
            .spawn(move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("finality observer runtime");
                runtime.block_on(async move {
                    let mut observer = FinalityObserver::initialize(
                        message_broker,
                        execution_store,
                        pool_registry,
                    )
                    .await
                    .unwrap_or_else(|error| {
                        eprintln!("Critical error initializing finality observer: {error}");
                        std::process::exit(1);
                    });
                    if let Err(error) = observer.start().await {
                        eprintln!(
                            "Critical error on finality observer: {error}. Exiting with status 1."
                        );
                        std::process::exit(1);
                    }
                });
            })
            .expect("spawn finality observer");
    }

    miden_start_tx
        .send(())
        .expect("Miden execution thread stopped during startup");

    println!("[INIT] Initializing pool provisioner");
    let deployment = minizeke::deployment::Deployment::load()?;
    let pool_provision_store =
        Arc::new(minizeke::pool_manager::PoolProvisionStore::open_from_env()?);
    let (provisioner, pool_capacity_rx) = minizeke::pool_manager::PoolProvisioner::new(
        pool_registry_for_provisioner,
        pool_provision_store,
        init_data.vault_id,
        deployment.operator_account_id,
    );
    {
        std::thread::Builder::new()
            .name("pool-provisioner".to_owned())
            .spawn(move || {
                let runtime = Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("pool provisioner runtime");
                runtime.block_on(async move {
                    if let Err(error) = provisioner.start().await {
                        eprintln!(
                            "Critical error on pool provisioner: {error}. Exiting with status 1."
                        );
                        std::process::exit(1);
                    }
                });
            })
            .expect("spawn pool provisioner");
    }

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
                let store = fee_store.clone();
                match tokio::task::spawn_blocking(move || store.expire(now)).await {
                    Ok(Ok(true)) => {
                        let _ = message_broker.broadcast_fee_state(FeeStateEvent {
                            fee_states: HashMap::new(),
                            timestamp: chrono::Utc::now().timestamp_millis() as u64,
                        });
                    }
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => warn!(%error, "failed to expire volatility fee"),
                    Err(error) => warn!(%error, "fee expiry worker failed"),
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
                        // Public statistics advance only when a chain-observed confirmation is
                        // published. Earlier lifecycle states are retained in owner-only history.
                        if matches!(update, order::OrderUpdate::Confirmed(_)) {
                            store_for_stats.apply_order_update(update);
                            let stats = store_for_stats.order_stats();
                            let _ =
                                message_broker_for_stats.broadcast_stats(StatsEvent::now(stats));
                        }
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
        execution_store,
        pool_capacity_rx,
    )
    .await?;

    Ok(())
}
