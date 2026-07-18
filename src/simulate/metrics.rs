use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::simulate::{api::OrderOutcome, config::TraderTier};

const MAX_SAMPLES_PER_TRADER: usize = 2_048;

#[derive(Clone, Default)]
pub struct Metrics {
    inner: Arc<Mutex<MetricsInner>>,
}

impl Metrics {
    pub fn record_setup(
        &self,
        trader_index: usize,
        tier: TraderTier,
        phase: SetupPhase,
        latency: Duration,
    ) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let trader = inner
            .traders
            .entry(trader_index)
            .or_insert_with(|| TraderMetrics::new(tier));
        match phase {
            SetupPhase::Mint => trader.setup_mint.push(latency),
            SetupPhase::Register => trader.setup_register.push(latency),
            SetupPhase::Fund => trader.setup_fund.push(latency),
        }
    }

    pub fn record_trade(&self, measurement: TradeMeasurement) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let trader = inner
            .traders
            .entry(measurement.trader_index)
            .or_insert_with(|| TraderMetrics::new(measurement.tier));
        trader.attempted += 1;
        match measurement.outcome {
            OrderOutcome::Accepted => trader.accepted += 1,
            OrderOutcome::RateLimited => trader.rate_limited += 1,
            OrderOutcome::Rejected => trader.rejected += 1,
            OrderOutcome::Failed => trader.failed += 1,
        }
        trader.oracle.push(measurement.oracle);
        if let Some(auth) = measurement.auth {
            trader.auth.push(auth);
        }
        if let Some(auth_wait) = measurement.auth_wait {
            trader.auth_wait.push(auth_wait);
        }
        trader.order.push(measurement.order);
        trader.cycle.push(measurement.cycle);
    }

    pub fn record_cycle_failure(&self, trader_index: usize, tier: TraderTier, cycle: Duration) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let trader = inner
            .traders
            .entry(trader_index)
            .or_insert_with(|| TraderMetrics::new(tier));
        trader.attempted += 1;
        trader.failed += 1;
        trader.cycle.push(cycle);
    }

    pub fn print_summary(&self, final_summary: bool) {
        let inner = self.inner.lock().expect("metrics mutex poisoned");
        let label = if final_summary { "FINAL" } else { "ROLLING" };
        let totals = inner
            .traders
            .values()
            .fold(OutcomeCounts::default(), |mut total, trader| {
                total.attempted += trader.attempted;
                total.accepted += trader.accepted;
                total.rate_limited += trader.rate_limited;
                total.rejected += trader.rejected;
                total.failed += trader.failed;
                total
            });
        println!(
            "[{label}] traders={} attempted={} accepted={} rate_limited={} rejected={} failed={}",
            inner.traders.len(),
            totals.attempted,
            totals.accepted,
            totals.rate_limited,
            totals.rejected,
            totals.failed,
        );
        for (index, trader) in &inner.traders {
            if trader.attempted == 0 && !final_summary {
                continue;
            }
            println!(
                "  trader={index} tier={} trades={} ok={} 429={} rejected={} failed={} \
                 oracle[p50/p95]={}/{}ms auth={}/{}ms auth_wait={}/{}ms order={}/{}ms cycle={}/{}ms",
                trader.tier.label(),
                trader.attempted,
                trader.accepted,
                trader.rate_limited,
                trader.rejected,
                trader.failed,
                trader.oracle.percentile(50),
                trader.oracle.percentile(95),
                trader.auth.percentile(50),
                trader.auth.percentile(95),
                trader.auth_wait.percentile(50),
                trader.auth_wait.percentile(95),
                trader.order.percentile(50),
                trader.order.percentile(95),
                trader.cycle.percentile(50),
                trader.cycle.percentile(95),
            );
            if final_summary {
                println!(
                    "    setup mint={}/{}ms register={}/{}ms fund={}/{}ms (p50/p95)",
                    trader.setup_mint.percentile(50),
                    trader.setup_mint.percentile(95),
                    trader.setup_register.percentile(50),
                    trader.setup_register.percentile(95),
                    trader.setup_fund.percentile(50),
                    trader.setup_fund.percentile(95),
                );
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SetupPhase {
    Mint,
    Register,
    Fund,
}

pub struct TradeMeasurement {
    pub trader_index: usize,
    pub tier: TraderTier,
    pub outcome: OrderOutcome,
    pub oracle: Duration,
    pub auth: Option<Duration>,
    pub auth_wait: Option<Duration>,
    pub order: Duration,
    pub cycle: Duration,
}

#[derive(Default)]
struct MetricsInner {
    traders: BTreeMap<usize, TraderMetrics>,
}

struct TraderMetrics {
    tier: TraderTier,
    attempted: u64,
    accepted: u64,
    rate_limited: u64,
    rejected: u64,
    failed: u64,
    oracle: Samples,
    auth: Samples,
    auth_wait: Samples,
    order: Samples,
    cycle: Samples,
    setup_mint: Samples,
    setup_register: Samples,
    setup_fund: Samples,
}

impl TraderMetrics {
    fn new(tier: TraderTier) -> Self {
        Self {
            tier,
            attempted: 0,
            accepted: 0,
            rate_limited: 0,
            rejected: 0,
            failed: 0,
            oracle: Samples::default(),
            auth: Samples::default(),
            auth_wait: Samples::default(),
            order: Samples::default(),
            cycle: Samples::default(),
            setup_mint: Samples::default(),
            setup_register: Samples::default(),
            setup_fund: Samples::default(),
        }
    }
}

#[derive(Default)]
struct Samples(VecDeque<u64>);

impl Samples {
    fn push(&mut self, value: Duration) {
        if self.0.len() == MAX_SAMPLES_PER_TRADER {
            self.0.pop_front();
        }
        self.0
            .push_back(value.as_millis().try_into().unwrap_or(u64::MAX));
    }

    fn percentile(&self, percentile: usize) -> u64 {
        if self.0.is_empty() {
            return 0;
        }
        let mut values = self.0.iter().copied().collect::<Vec<_>>();
        values.sort_unstable();
        let index = ((values.len() - 1) * percentile).div_ceil(100);
        values[index]
    }
}

#[derive(Default)]
struct OutcomeCounts {
    attempted: u64,
    accepted: u64,
    rate_limited: u64,
    rejected: u64,
    failed: u64,
}

pub fn jittered_interval(base: Duration, jitter: f64) -> Duration {
    let factor = 1.0 + ((rand::random::<f64>() * 2.0 - 1.0) * jitter);
    Duration::from_secs_f64((base.as_secs_f64() * factor).max(0.001))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Samples, jittered_interval};

    #[test]
    fn jitter_stays_in_configured_bounds() {
        for _ in 0..1_000 {
            let value = jittered_interval(Duration::from_secs(20), 0.2);
            assert!(value >= Duration::from_secs(16));
            assert!(value <= Duration::from_secs(24));
        }
    }

    #[test]
    fn samples_report_percentiles() {
        let mut samples = Samples::default();
        for value in 1..=100 {
            samples.push(Duration::from_millis(value));
        }
        assert_eq!(samples.percentile(50), 51);
        assert_eq!(samples.percentile(95), 96);
    }
}
