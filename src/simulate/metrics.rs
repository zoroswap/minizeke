use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::simulate::{api::OrderOutcome, config::TraderTier};

const MAX_SAMPLES_PER_TRADER: usize = 2_048;
const TRADE_INTERVAL_SECS: f64 = 10.0;
const SATURATION_ERROR_PERCENT: u64 = 5;
const SATURATION_SETTLE_P95_MS: u64 = 30_000;

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
        if let Some(settle) = measurement.settle {
            inner.window_settle.push(settle);
        }
        let trader = inner
            .traders
            .entry(measurement.trader_index)
            .or_insert_with(|| TraderMetrics::new(measurement.tier));
        trader.attempted += 1;
        match measurement.outcome {
            OrderOutcome::Accepted => trader.accepted += 1,
            OrderOutcome::Confirmed => trader.confirmed += 1,
            OrderOutcome::RateLimited => trader.rate_limited += 1,
            OrderOutcome::Rejected => trader.rejected += 1,
            OrderOutcome::Failed => trader.failed += 1,
            OrderOutcome::ExecutionFailed => trader.execution_failed += 1,
            OrderOutcome::TimedOut => trader.timed_out += 1,
        }
        trader.oracle.push(measurement.oracle);
        if let Some(auth) = measurement.auth {
            trader.auth.push(auth);
        }
        if let Some(auth_wait) = measurement.auth_wait {
            trader.auth_wait.push(auth_wait);
        }
        trader.order.push(measurement.order);
        if let Some(settle) = measurement.settle {
            trader.settle.push(settle);
        }
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

    pub fn record_vault_cycle(&self, ok: bool) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        if ok {
            inner.vault_cycle_ok += 1;
        } else {
            inner.vault_cycle_failed += 1;
        }
    }

    pub fn set_active_traders(&self, active: usize) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        if inner.active == 0 && inner.scheduled == 0 {
            inner.last_report = Instant::now();
        }
        if inner.active != active {
            inner.consecutive_unhealthy = 0;
            inner.consecutive_healthy = 0;
        }
        inner.active = active;
    }

    pub fn record_schedule(&self) {
        self.inner.lock().expect("metrics mutex poisoned").scheduled += 1;
    }

    pub fn record_skipped_schedule(&self) {
        self.inner.lock().expect("metrics mutex poisoned").skipped += 1;
    }

    pub fn record_submitted(&self) {
        self.inner.lock().expect("metrics mutex poisoned").submitted += 1;
    }

    pub fn order_started(&self) {
        self.inner.lock().expect("metrics mutex poisoned").in_flight += 1;
    }

    pub fn order_finished(&self) {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        inner.in_flight = inner.in_flight.saturating_sub(1);
    }

    pub fn is_saturated(&self) -> bool {
        self.inner.lock().expect("metrics mutex poisoned").saturated
    }

    pub fn print_summary(&self, final_summary: bool) -> LoadSnapshot {
        let mut inner = self.inner.lock().expect("metrics mutex poisoned");
        let totals = inner
            .traders
            .values()
            .fold(OutcomeCounts::default(), |mut total, trader| {
                total.attempted += trader.attempted;
                total.accepted += trader.accepted;
                total.confirmed += trader.confirmed;
                total.rate_limited += trader.rate_limited;
                total.rejected += trader.rejected;
                total.failed += trader.failed;
                total.execution_failed += trader.execution_failed;
                total.timed_out += trader.timed_out;
                total
            });
        let elapsed = inner.last_report.elapsed().as_secs_f64().max(0.001);
        let scheduled = inner.scheduled.saturating_sub(inner.last.scheduled);
        let skipped = inner.skipped.saturating_sub(inner.last.skipped);
        let submitted = inner.submitted.saturating_sub(inner.last.submitted);
        let confirmed = totals
            .confirmed
            .saturating_sub(inner.last.outcomes.confirmed);
        let rate_limited = totals
            .rate_limited
            .saturating_sub(inner.last.outcomes.rate_limited);
        let rejected = totals.rejected.saturating_sub(inner.last.outcomes.rejected);
        let failed = totals
            .failed
            .saturating_sub(inner.last.outcomes.failed)
            .saturating_add(
                totals
                    .execution_failed
                    .saturating_sub(inner.last.outcomes.execution_failed),
            );
        let timed_out = totals
            .timed_out
            .saturating_sub(inner.last.outcomes.timed_out);
        let settle_p50_ms = inner.window_settle.percentile(50);
        let settle_p95_ms = inner.window_settle.percentile(95);
        let unhealthy_count = skipped
            .saturating_add(rate_limited)
            .saturating_add(rejected)
            .saturating_add(failed)
            .saturating_add(timed_out);
        let unhealthy = (scheduled > 0
            && unhealthy_count.saturating_mul(100)
                > scheduled.saturating_mul(SATURATION_ERROR_PERCENT))
            || settle_p95_ms > SATURATION_SETTLE_P95_MS;
        if unhealthy {
            inner.consecutive_unhealthy += 1;
            inner.consecutive_healthy = 0;
        } else {
            inner.consecutive_unhealthy = 0;
            inner.consecutive_healthy += 1;
            if inner.consecutive_healthy >= 2 {
                inner.last_healthy_active = inner.active;
            }
        }
        if inner.consecutive_unhealthy >= 2 {
            inner.saturated = true;
            let active = inner.active;
            inner.first_saturated_active.get_or_insert(active);
        }
        let snapshot = LoadSnapshot {
            active: inner.active,
            target_rate: inner.active as f64 / TRADE_INTERVAL_SECS,
            submitted_rate: submitted as f64 / elapsed,
            confirmed_rate: confirmed as f64 / elapsed,
            confirmed,
            in_flight: inner.in_flight,
            skipped,
            rate_limited,
            rejected,
            failed,
            timed_out,
            settle_p50_ms,
            settle_p95_ms,
            saturated: inner.saturated,
        };
        println!(
            "[LOAD] active={} target={:.1}/s submitted={:.1}/s confirmed={:.1}/s \
             inflight={} skipped={} 429/503={} rejected={} failed={} timeouts={} \
             settle_p50={}ms settle_p95={}ms",
            snapshot.active,
            snapshot.target_rate,
            snapshot.submitted_rate,
            snapshot.confirmed_rate,
            snapshot.in_flight,
            snapshot.skipped,
            snapshot.rate_limited,
            snapshot.rejected,
            snapshot.failed,
            snapshot.timed_out,
            snapshot.settle_p50_ms,
            snapshot.settle_p95_ms,
        );
        if final_summary || (inner.saturated && !inner.limit_reported) {
            println!(
                "[LIMIT] last_healthy={} first_saturated={} achieved={:.1}/s",
                inner.last_healthy_active,
                inner.first_saturated_active.unwrap_or(inner.active),
                snapshot.submitted_rate,
            );
            inner.limit_reported = true;
        }
        inner.last = WindowBaseline {
            scheduled: inner.scheduled,
            skipped: inner.skipped,
            submitted: inner.submitted,
            outcomes: totals,
        };
        inner.window_settle = Samples::default();
        inner.last_report = Instant::now();
        snapshot
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LoadSnapshot {
    pub active: usize,
    pub target_rate: f64,
    pub submitted_rate: f64,
    pub confirmed_rate: f64,
    pub confirmed: u64,
    pub in_flight: usize,
    pub skipped: u64,
    pub rate_limited: u64,
    pub rejected: u64,
    pub failed: u64,
    pub timed_out: u64,
    pub settle_p50_ms: u64,
    pub settle_p95_ms: u64,
    pub saturated: bool,
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
    /// Admit → WS terminal latency when applicable.
    pub settle: Option<Duration>,
    pub cycle: Duration,
}

struct MetricsInner {
    traders: BTreeMap<usize, TraderMetrics>,
    vault_cycle_ok: u64,
    vault_cycle_failed: u64,
    active: usize,
    scheduled: u64,
    skipped: u64,
    submitted: u64,
    in_flight: usize,
    last: WindowBaseline,
    last_report: Instant,
    window_settle: Samples,
    consecutive_unhealthy: u8,
    consecutive_healthy: u8,
    saturated: bool,
    last_healthy_active: usize,
    first_saturated_active: Option<usize>,
    limit_reported: bool,
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            traders: BTreeMap::new(),
            vault_cycle_ok: 0,
            vault_cycle_failed: 0,
            active: 0,
            scheduled: 0,
            skipped: 0,
            submitted: 0,
            in_flight: 0,
            last: WindowBaseline::default(),
            last_report: Instant::now(),
            window_settle: Samples::default(),
            consecutive_unhealthy: 0,
            consecutive_healthy: 0,
            saturated: false,
            last_healthy_active: 0,
            first_saturated_active: None,
            limit_reported: false,
        }
    }
}

struct TraderMetrics {
    _tier: TraderTier,
    attempted: u64,
    accepted: u64,
    confirmed: u64,
    rate_limited: u64,
    rejected: u64,
    failed: u64,
    execution_failed: u64,
    timed_out: u64,
    oracle: Samples,
    auth: Samples,
    auth_wait: Samples,
    order: Samples,
    settle: Samples,
    cycle: Samples,
    setup_mint: Samples,
    setup_register: Samples,
    setup_fund: Samples,
}

impl TraderMetrics {
    fn new(tier: TraderTier) -> Self {
        Self {
            _tier: tier,
            attempted: 0,
            accepted: 0,
            confirmed: 0,
            rate_limited: 0,
            rejected: 0,
            failed: 0,
            execution_failed: 0,
            timed_out: 0,
            oracle: Samples::default(),
            auth: Samples::default(),
            auth_wait: Samples::default(),
            order: Samples::default(),
            settle: Samples::default(),
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

#[derive(Default, Clone, Copy)]
struct OutcomeCounts {
    attempted: u64,
    accepted: u64,
    confirmed: u64,
    rate_limited: u64,
    rejected: u64,
    failed: u64,
    execution_failed: u64,
    timed_out: u64,
}

#[derive(Default)]
struct WindowBaseline {
    scheduled: u64,
    skipped: u64,
    submitted: u64,
    outcomes: OutcomeCounts,
}

pub fn jittered_interval(base: Duration, jitter: f64) -> Duration {
    let factor = 1.0 + ((rand::random::<f64>() * 2.0 - 1.0) * jitter);
    Duration::from_secs_f64((base.as_secs_f64() * factor).max(0.001))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{Metrics, Samples, jittered_interval};

    #[test]
    fn jitter_stays_in_configured_bounds() {
        for _ in 0..1_000 {
            let value = jittered_interval(Duration::from_secs(10), 0.2);
            assert!(value >= Duration::from_secs(8));
            assert!(value <= Duration::from_secs(12));
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

    #[test]
    fn saturation_requires_two_bad_windows() {
        let metrics = Metrics::default();
        metrics.set_active_traders(20);
        for _ in 0..2 {
            for _ in 0..100 {
                metrics.record_schedule();
            }
            for _ in 0..6 {
                metrics.record_skipped_schedule();
            }
            metrics.print_summary(false);
        }
        assert!(metrics.is_saturated());
    }
}
