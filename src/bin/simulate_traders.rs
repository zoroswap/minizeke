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
            build_simulation_client, load_traders, run_trader, setup_traders, validate_deployment,
        },
    },
};
use tokio::{sync::watch, task::JoinSet};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    let config = Config::parse();
    config.validate()?;
    init_tracing(config.verbose);

    let deployment = Deployment::load()?;
    validate_deployment(&deployment)?;
    let api = SimulationApi::new(
        &config.api_url,
        &config.faucet_url,
        config.faucet_token.clone(),
    )?;
    let oracle = OracleClient::new(&config.oracle_url)?;
    let metrics = Metrics::default();
    let (mut miden_client, keystore) = build_simulation_client(&config).await?;

    let traders = if config.skip_setup {
        load_traders(&config, &deployment, &keystore).await?
    } else {
        setup_traders(
            &config,
            &deployment,
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
        "starting simulation"
    );

    let config = Arc::new(config);
    let deployment = Arc::new(deployment);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut tasks = JoinSet::new();
    for trader in traders {
        tasks.spawn(run_trader(
            trader,
            config.clone(),
            deployment.clone(),
            api.clone(),
            oracle.clone(),
            metrics.clone(),
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
                result?;
                info!("Ctrl+C received; stopping traders");
                break;
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
