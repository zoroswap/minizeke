use std::{future::pending, sync::Arc, time::Duration};

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
            TraderRegistry, build_simulation_client, load_traders, reset_setup_artifacts,
            run_growth_loop, run_trader, run_vault_cycle_loop, setup_traders, validate_deployment,
            warm_auth_sessions,
        },
    },
};
use tokio::{
    sync::{Mutex, Semaphore, watch},
    task::JoinSet,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let config = Config::parse();
    config.validate()?;
    init_tracing(config.verbose);

    let mut deployment = Deployment::load()?;
    validate_deployment(&deployment)?;
    let api = SimulationApi::new(
        &config.api_url,
        &config.faucet_url,
        config.faucet_token.clone(),
    )?;
    let oracle = OracleClient::new(&config.oracle_url)?;
    let metrics = Metrics::default();

    if !config.skip_setup {
        reset_setup_artifacts(&config)?;
    }

    let (mut miden_client, keystore) = build_simulation_client(&config).await?;

    let traders = if config.skip_setup {
        load_traders(&config, &deployment, &keystore).await?
    } else {
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
    if config.setup_only {
        metrics.print_summary(true);
        return Ok(());
    }

    let tier_counts = traders.iter().fold([0_usize; 3], |mut counts, trader| {
        match trader.tier {
            minizeke::simulate::config::TraderTier::Low => counts[0] += 1,
            minizeke::simulate::config::TraderTier::Average => counts[1] += 1,
            minizeke::simulate::config::TraderTier::HighFrequency => counts[2] += 1,
        }
        counts
    });
    info!(
        traders = traders.len(),
        low = tier_counts[0],
        average = tier_counts[1],
        high_frequency = tier_counts[2],
        "warming auth sessions"
    );
    let sessions = warm_auth_sessions(
        &traders,
        &api,
        Duration::from_millis(config.auth_warmup_gap_ms),
    )
    .await?;

    info!(traders = traders.len(), "starting simulation");

    let config = Arc::new(config);
    let deployment = Arc::new(deployment);
    let registry: TraderRegistry = Arc::new(Mutex::new(traders.clone()));
    let prove_slots = Arc::new(Semaphore::new(config.setup_concurrency));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
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

    if config.keep_increasing {
        info!(
            max_traders = config.max_traders,
            grow_interval_secs = config.grow_interval_secs,
            "starting live trader growth"
        );
        tasks.spawn(run_growth_loop(
            config.clone(),
            api.clone(),
            oracle.clone(),
            metrics.clone(),
            registry.clone(),
            prove_slots.clone(),
            shutdown_rx.clone(),
        ));
    }

    if config.vault_cycle_interval_secs > 0 {
        info!(
            interval_secs = config.vault_cycle_interval_secs,
            amount = config.vault_cycle_amount,
            "starting vault fund/redeem cycles"
        );
        tasks.spawn(run_vault_cycle_loop(
            config.clone(),
            api.clone(),
            metrics.clone(),
            registry,
            prove_slots,
            shutdown_rx.clone(),
        ));
    }

    let mut summary = tokio::time::interval(Duration::from_secs(config.summary_interval));
    summary.tick().await;
    let duration = async {
        match config.duration {
            Some(seconds) => tokio::time::sleep(Duration::from_secs(seconds)).await,
            None => pending::<()>().await,
        }
    };
    tokio::pin!(duration);

    loop {
        tokio::select! {
            _ = summary.tick() => metrics.print_summary(false),
            result = tokio::signal::ctrl_c() => {
                let _ = result;
                // Immediate hard kill — do not drain traders / onboarding.
                eprintln!("Ctrl+C");
                std::process::exit(130);
            }
            _ = &mut duration => {
                info!("configured simulation duration elapsed");
                break;
            }
        }
    }

    let _ = shutdown_tx.send(true);
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "trader task failed");
        }
    }
    metrics.print_summary(true);
    Ok(())
}

fn init_tracing(verbosity: u8) {
    let default_filter = match verbosity {
        0 => "info,miden_core=off,log=warn",
        1 => "debug,miden_core=off",
        _ => "trace",
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_target(false)
        .init();
}
