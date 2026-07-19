use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use minizeke::{
    deployment::Deployment,
    simulate::{
        api::SimulationApi,
        config::Config,
        metrics::Metrics,
        oracle::OracleClient,
        trader::{
            build_simulation_client, load_traders, migrate_legacy_setup_artifacts,
            reset_setup_artifacts, run_activation_ramp, run_trader, setup_traders,
            validate_deployment, warm_auth_sessions,
        },
    },
};
use tokio::{sync::watch, task::JoinSet};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let config = Config::parse();
    config.validate()?;
    init_tracing();

    let mut deployment = Deployment::load()?;
    validate_deployment(&deployment)?;
    let api = SimulationApi::new(
        &config.api_url,
        &config.faucet_url,
        config.faucet_token.clone(),
    )?;
    let oracle = OracleClient::new(&config.oracle_url)?;
    let metrics = Metrics::default();

    migrate_legacy_setup_artifacts(&config)?;
    let mut traders = if config.state_file.exists() {
        let (miden_client, keystore) = build_simulation_client(&config).await?;
        match load_traders(&config, &deployment, &keystore).await {
            Ok(traders) => {
                info!(
                    traders = traders.len(),
                    state = %config.state_file.display(),
                    "reusing saved trader cohort"
                );
                traders
            }
            Err(error) => {
                warn!(
                    error = %format!("{error:#}"),
                    "saved trader cohort is unusable; rebuilding once"
                );
                drop(miden_client);
                drop(keystore);
                reset_setup_artifacts(&config)?;
                let (mut miden_client, keystore) = build_simulation_client(&config).await?;
                setup_traders(
                    &config,
                    &mut deployment,
                    &api,
                    &metrics,
                    &mut miden_client,
                    &keystore,
                )
                .await?
            }
        }
    } else {
        reset_setup_artifacts(&config)?;
        let (mut miden_client, keystore) = build_simulation_client(&config).await?;
        setup_traders(
            &config,
            &mut deployment,
            &api,
            &metrics,
            &mut miden_client,
            &keystore,
        )
        .await?
    };
    let staged = traders.split_off(config.num_traders);

    info!(
        active = traders.len(),
        staged = staged.len(),
        max_traders = config.max_traders,
        interval_secs = config.trade_interval_secs,
        "warming auth sessions (all high-frequency)"
    );
    let sessions = warm_auth_sessions(
        &traders,
        &api,
        Duration::from_millis(config.auth_warmup_gap_ms),
    )
    .await?;

    info!(
        active = traders.len(),
        max_traders = config.max_traders,
        "starting open-loop simulation"
    );

    let config = Arc::new(config);
    let deployment = Arc::new(deployment);
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    metrics.set_active_traders(traders.len());
    for (trader, session) in traders.into_iter().zip(sessions) {
        tasks.spawn(run_trader(
            trader,
            config.clone(),
            deployment.clone(),
            api.clone(),
            oracle.clone(),
            metrics.clone(),
            Some(session),
            shutdown_rx.clone(),
        ));
    }

    if !staged.is_empty() {
        info!(
            max_traders = config.max_traders,
            stage_interval_secs = 60,
            "starting activation ramp"
        );
        tasks.spawn(run_activation_ramp(
            config.clone(),
            deployment,
            api.clone(),
            oracle.clone(),
            metrics.clone(),
            staged,
            shutdown_rx.clone(),
        ));
    }

    let mut summary = tokio::time::interval(Duration::from_secs(config.summary_interval));
    summary.tick().await;

    loop {
        tokio::select! {
            _ = summary.tick() => {
                metrics.print_summary(false);
            }
            result = tokio::signal::ctrl_c() => {
                let _ = result;
                // Immediate hard kill — do not drain traders / onboarding.
                eprintln!("Ctrl+C");
                std::process::exit(130);
            }
        }
    }
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,\
                     miden_client=warn,\
                     miden_client::rpc::tonic_client::retry=off,\
                     miden_core=off,\
                     miden_processor=warn,\
                     miden_prover=warn,\
                     rusqlite_migration=warn,\
                     reqwest=warn,\
                     hyper=warn,\
                     tungstenite=warn,\
                     tokio_tungstenite=warn,\
                     log=warn",
                )
            }),
        )
        .with_target(false)
        .init();
}
