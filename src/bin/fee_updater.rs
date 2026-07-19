use std::{env, time::Instant};

use anyhow::{Context, Result};
use minizeke::{
    fee::{
        Config, Evaluation, FeeCurve, FeePolicy, OracleClient, VolatilityEstimator,
        policy::UpdateTrigger,
    },
    fee_store::{FeeBatchRequest, FeeUpdateSource},
};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,minizeke=debug".into()),
        )
        .init();

    let config = Config::from_env()?;
    let oracle = OracleClient::new(config.oracle.clone())?;
    let mut estimator = VolatilityEstimator::new(
        config.volatility.sample_interval,
        config.volatility.half_life,
        config.volatility.warmup_samples,
        config.update_policy.spike_ratio,
        config.update_policy.spike_window,
    );
    let mut policy = FeePolicy::new(
        FeeCurve::new(config.fee_curve.clone()),
        config.update_policy.clone(),
        config.volatility.max_sample_age,
        config.validity_period,
        config.refresh_fraction,
    );
    let token = env::var("FEE_UPDATER_TOKEN")
        .context("FEE_UPDATER_TOKEN is required by the standalone fee updater")?;
    let mut endpoint = config.http.server_url.clone();
    endpoint.set_path("/internal/fees/batch");
    endpoint.set_query(None);
    endpoint.set_fragment(None);
    let http = reqwest::Client::builder()
        .timeout(config.http.request_timeout)
        .build()?;

    let mut current_fee_bps = config.initial_fee_bps;
    let mut sample_tick = interval(config.volatility.sample_interval);
    sample_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut update_tick = interval(config.update_policy.interval);
    update_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = sample_tick.tick() => {
                match oracle.fetch().await {
                    Ok(sample) => {
                        if let Err(error) = estimator.update(sample.price) {
                            warn!(%error, "rejected oracle sample");
                        }
                    }
                    Err(error) => warn!(%error, "failed to sample reference oracle"),
                }
            }
            _ = update_tick.tick() => {
                let now = Instant::now();
                match policy.evaluate(estimator.latest(), current_fee_bps, now)? {
                    Evaluation::Push {
                        target_fee_bps,
                        surge_fee_units,
                        trigger,
                        spike_detected,
                    } => {
                        let issued_at = chrono::Utc::now().timestamp() as u64;
                        let request = FeeBatchRequest {
                            batch_id: Uuid::new_v4().to_string(),
                            issued_at,
                            validity_secs: config.validity_period.as_secs(),
                            source: FeeUpdateSource::Automatic,
                            trigger: Some(trigger_name(trigger).to_owned()),
                            volatility_fee_in: u64::from(surge_fee_units),
                            volatility_fee_out: u64::from(surge_fee_units),
                            sigma_pct_day: estimator.latest().map(|sample| sample.sigma_pct_day),
                            target_fee_bps: Some(target_fee_bps),
                        };
                        let response = http
                            .post(endpoint.clone())
                            .bearer_auth(&token)
                            .json(&request)
                            .send()
                            .await;
                        match response {
                            Ok(response) => {
                                if let Err(error) = response.error_for_status_ref() {
                                    warn!(%error, "fee batch rejected; retaining previous keeper state");
                                } else {
                                    policy.mark_pushed(now, spike_detected);
                                    current_fee_bps = target_fee_bps;
                                    info!(
                                        target_fee_bps,
                                        surge_fee_units,
                                        validity_secs = config.validity_period.as_secs(),
                                        "published volatility fee"
                                    );
                                }
                            }
                            Err(error) => warn!(%error, "failed to publish volatility fee"),
                        }
                    }
                    evaluation => {
                        info!(?evaluation, "fee policy did not publish");
                    }
                }
            }
        }
    }
}

fn trigger_name(trigger: UpdateTrigger) -> &'static str {
    match trigger {
        UpdateTrigger::Deadband => "deadband",
        UpdateTrigger::VolatilitySpike => "volatility_spike",
        UpdateTrigger::DeadbandAndVolatilitySpike => "deadband_and_volatility_spike",
        UpdateTrigger::ValidityRefresh => "validity_refresh",
    }
}
