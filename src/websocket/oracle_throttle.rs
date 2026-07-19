use std::{
    collections::HashMap,
    env,
    time::{Duration, Instant},
};

use crate::message_broker::message_broker::OraclePriceEvent;

const DEFAULT_MIN_INTERVAL_MS: u64 = 1_000;
const DEFAULT_BPS_THRESHOLD: u64 = 20;
const BPS_DENOMINATOR: u128 = 10_000;

#[derive(Debug, Clone, Copy)]
pub(crate) struct OracleWsThrottleConfig {
    min_interval: Duration,
    bps_threshold: u64,
}

impl OracleWsThrottleConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            min_interval: Duration::from_millis(env_u64(
                "ORACLE_WS_MIN_INTERVAL_MS",
                DEFAULT_MIN_INTERVAL_MS,
            )),
            bps_threshold: env_u64("ORACLE_WS_BPS_THRESHOLD", DEFAULT_BPS_THRESHOLD),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

struct AssetState {
    last_emitted_price: u64,
    last_emitted_at: Instant,
    pending: Option<OraclePriceEvent>,
}

pub(crate) struct OracleWsThrottle {
    config: OracleWsThrottleConfig,
    assets: HashMap<String, AssetState>,
}

impl OracleWsThrottle {
    pub(crate) fn from_env() -> Self {
        Self::new(OracleWsThrottleConfig::from_env())
    }

    fn new(config: OracleWsThrottleConfig) -> Self {
        Self {
            config,
            assets: HashMap::new(),
        }
    }

    /// Accepts an oracle event and returns it when it should be forwarded now.
    /// Events not returned are retained as the newest pending event for the asset.
    pub(crate) fn push(
        &mut self,
        event: OraclePriceEvent,
        now: Instant,
    ) -> Option<OraclePriceEvent> {
        let key = event.faucet_id.clone();
        let Some(state) = self.assets.get_mut(&key) else {
            self.assets.insert(
                key,
                AssetState {
                    last_emitted_price: event.price,
                    last_emitted_at: now,
                    pending: None,
                },
            );
            return Some(event);
        };

        let interval_elapsed =
            now.saturating_duration_since(state.last_emitted_at) >= self.config.min_interval;
        if interval_elapsed
            || movement_reaches_threshold(
                state.last_emitted_price,
                event.price,
                self.config.bps_threshold,
            )
        {
            state.last_emitted_price = event.price;
            state.last_emitted_at = now;
            state.pending = None;
            Some(event)
        } else {
            state.pending = Some(event);
            None
        }
    }

    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.assets
            .values()
            .filter(|state| state.pending.is_some())
            .map(|state| state.last_emitted_at + self.config.min_interval)
            .min()
    }

    /// Returns the newest pending event for every asset whose interval expired.
    pub(crate) fn flush_due(&mut self, now: Instant) -> Vec<OraclePriceEvent> {
        let mut due = Vec::new();
        for state in self.assets.values_mut() {
            if state.pending.is_some()
                && now.saturating_duration_since(state.last_emitted_at) >= self.config.min_interval
            {
                let event = state.pending.take().expect("pending event checked above");
                state.last_emitted_price = event.price;
                state.last_emitted_at = now;
                due.push(event);
            }
        }
        due
    }
}

fn movement_reaches_threshold(previous: u64, current: u64, threshold_bps: u64) -> bool {
    if previous == 0 {
        return current != 0 || threshold_bps == 0;
    }

    u128::from(previous.abs_diff(current)) * BPS_DENOMINATOR
        >= u128::from(previous) * u128::from(threshold_bps)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(faucet_id: &str, price: u64, timestamp: u64) -> OraclePriceEvent {
        OraclePriceEvent {
            oracle_id: format!("oracle-{faucet_id}"),
            faucet_id: faucet_id.to_owned(),
            price,
            timestamp,
        }
    }

    fn throttle(interval_ms: u64, bps_threshold: u64) -> OracleWsThrottle {
        OracleWsThrottle::new(OracleWsThrottleConfig {
            min_interval: Duration::from_millis(interval_ms),
            bps_threshold,
        })
    }

    #[test]
    fn first_event_is_immediate_and_small_moves_are_coalesced() {
        let start = Instant::now();
        let mut throttle = throttle(1_000, 20);

        assert_eq!(
            throttle
                .push(event("a", 10_000, 1), start)
                .unwrap()
                .timestamp,
            1
        );
        assert!(
            throttle
                .push(event("a", 10_001, 2), start + Duration::from_millis(100))
                .is_none()
        );
        assert!(
            throttle
                .push(event("a", 10_002, 3), start + Duration::from_millis(200))
                .is_none()
        );

        let flushed = throttle.flush_due(start + Duration::from_secs(1));
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].price, 10_002);
        assert_eq!(flushed[0].timestamp, 3);
    }

    #[test]
    fn threshold_move_emits_immediately_and_clears_pending() {
        let start = Instant::now();
        let mut throttle = throttle(1_000, 20);
        throttle.push(event("a", 10_000, 1), start).unwrap();
        assert!(
            throttle
                .push(event("a", 10_010, 2), start + Duration::from_millis(100))
                .is_none()
        );

        let emitted = throttle
            .push(event("a", 10_020, 3), start + Duration::from_millis(200))
            .unwrap();
        assert_eq!(emitted.timestamp, 3);
        assert!(
            throttle
                .flush_due(start + Duration::from_secs(2))
                .is_empty()
        );
    }

    #[test]
    fn assets_have_independent_windows() {
        let start = Instant::now();
        let mut throttle = throttle(1_000, 20);
        throttle.push(event("a", 100, 1), start).unwrap();
        throttle
            .push(event("b", 200, 2), start + Duration::from_millis(500))
            .unwrap();
        throttle.push(event("a", 100, 3), start + Duration::from_millis(600));
        throttle.push(event("b", 200, 4), start + Duration::from_millis(600));

        let first = throttle.flush_due(start + Duration::from_secs(1));
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].faucet_id, "a");
        assert_eq!(
            throttle.next_deadline(),
            Some(start + Duration::from_millis(1_500))
        );
    }

    #[test]
    fn bps_math_handles_zero_and_u64_extremes() {
        assert!(!movement_reaches_threshold(0, 0, 20));
        assert!(movement_reaches_threshold(0, 1, 20));
        assert!(movement_reaches_threshold(u64::MAX, 0, 10_000));
        assert!(movement_reaches_threshold(100_000, 100_200, 20));
        assert!(!movement_reaches_threshold(100_000, 100_199, 20));
    }
}
