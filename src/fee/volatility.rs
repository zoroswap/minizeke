use std::{
    collections::VecDeque,
    error::Error,
    fmt,
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct VolatilitySnapshot {
    pub sigma_pct_day: f64,
    pub return_samples: u64,
    pub sampled_at: Instant,
    pub warmed_up: bool,
    pub spike_detected: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VolatilityError(pub f64);

impl fmt::Display for VolatilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "price must be finite and greater than zero, got {}",
            self.0
        )
    }
}

impl Error for VolatilityError {}

#[derive(Debug)]
pub struct VolatilityEstimator {
    lambda: f64,
    daily_intervals: f64,
    warmup_samples: u64,
    spike_ratio: f64,
    spike_window: Duration,
    previous_price: Option<f64>,
    variance: Option<f64>,
    return_samples: u64,
    history: VecDeque<(Instant, f64)>,
    latest: Option<VolatilitySnapshot>,
}

impl VolatilityEstimator {
    pub fn new(
        sample_interval: Duration,
        half_life: Duration,
        warmup_samples: u64,
        spike_ratio: f64,
        spike_window: Duration,
    ) -> Self {
        let sample_seconds = sample_interval.as_secs_f64();
        Self {
            lambda: 0.5_f64.powf(sample_seconds / half_life.as_secs_f64()),
            daily_intervals: Duration::from_secs(86_400).as_secs_f64() / sample_seconds,
            warmup_samples,
            spike_ratio,
            spike_window,
            previous_price: None,
            variance: None,
            return_samples: 0,
            history: VecDeque::new(),
            latest: None,
        }
    }

    pub fn update(&mut self, price: f64) -> Result<Option<VolatilitySnapshot>, VolatilityError> {
        self.update_at(price, Instant::now())
    }

    pub fn update_at(
        &mut self,
        price: f64,
        sampled_at: Instant,
    ) -> Result<Option<VolatilitySnapshot>, VolatilityError> {
        if !price.is_finite() || price <= 0.0 {
            return Err(VolatilityError(price));
        }

        let Some(previous_price) = self.previous_price.replace(price) else {
            return Ok(None);
        };
        let log_return = (price / previous_price).ln();
        let squared_return = log_return * log_return;
        let variance = self.variance.map_or(squared_return, |previous| {
            self.lambda * previous + (1.0 - self.lambda) * squared_return
        });
        self.variance = Some(variance);
        self.return_samples += 1;

        let sigma_pct_day = variance.sqrt() * self.daily_intervals.sqrt() * 100.0;
        self.history.push_back((sampled_at, sigma_pct_day));
        self.prune_history(sampled_at);

        let warmed_up = self.return_samples >= self.warmup_samples;
        let spike_detected = warmed_up && self.is_spike(sigma_pct_day);
        let snapshot = VolatilitySnapshot {
            sigma_pct_day,
            return_samples: self.return_samples,
            sampled_at,
            warmed_up,
            spike_detected,
        };
        self.latest = Some(snapshot);
        Ok(Some(snapshot))
    }

    pub fn latest(&self) -> Option<VolatilitySnapshot> {
        self.latest
    }

    fn prune_history(&mut self, now: Instant) {
        while self
            .history
            .front()
            .is_some_and(|(sampled_at, _)| now.duration_since(*sampled_at) > self.spike_window)
        {
            self.history.pop_front();
        }
    }

    fn is_spike(&self, current_sigma: f64) -> bool {
        let minimum_sigma = self
            .history
            .iter()
            .map(|(_, sigma)| *sigma)
            .fold(f64::INFINITY, f64::min);
        minimum_sigma > 0.0 && current_sigma >= minimum_sigma * self.spike_ratio
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn estimator(warmup_samples: u64) -> VolatilityEstimator {
        VolatilityEstimator::new(
            Duration::from_secs(30),
            Duration::from_secs(1_800),
            warmup_samples,
            1.5,
            Duration::from_secs(600),
        )
    }

    #[test]
    fn computes_daily_equivalent_volatility() {
        let mut estimator = estimator(1);
        let now = Instant::now();
        assert!(estimator.update_at(100.0, now).unwrap().is_none());
        let snapshot = estimator
            .update_at(100.0 * 0.01_f64.exp(), now + Duration::from_secs(30))
            .unwrap()
            .unwrap();
        let expected = 0.01 * 2_880_f64.sqrt() * 100.0;
        assert!((snapshot.sigma_pct_day - expected).abs() < 1e-10);
        assert!(snapshot.warmed_up);
    }

    #[test]
    fn applies_reference_ewma_decay() {
        let mut estimator = estimator(1);
        let now = Instant::now();
        estimator.update_at(100.0, now).unwrap();
        estimator
            .update_at(100.0 * 0.01_f64.exp(), now + Duration::from_secs(30))
            .unwrap();
        let snapshot = estimator
            .update_at(100.0 * 0.01_f64.exp(), now + Duration::from_secs(60))
            .unwrap()
            .unwrap();
        let lambda = 0.5_f64.powf(30.0 / 1_800.0);
        let expected = (lambda * 0.01_f64.powi(2)).sqrt() * 2_880_f64.sqrt() * 100.0;
        assert!((snapshot.sigma_pct_day - expected).abs() < 1e-10);
    }

    #[test]
    fn enforces_warmup_and_detects_spikes() {
        let mut estimator = estimator(2);
        let now = Instant::now();
        estimator.update_at(100.0, now).unwrap();
        let first = estimator
            .update_at(100.001, now + Duration::from_secs(30))
            .unwrap()
            .unwrap();
        assert!(!first.warmed_up);
        let second = estimator
            .update_at(101.0, now + Duration::from_secs(60))
            .unwrap()
            .unwrap();
        assert!(second.warmed_up);
        assert!(second.spike_detected);
    }

    #[test]
    fn prunes_old_values_from_spike_window() {
        let mut estimator = estimator(1);
        let now = Instant::now();
        estimator.update_at(100.0, now).unwrap();
        estimator
            .update_at(100.001, now + Duration::from_secs(30))
            .unwrap();
        let snapshot = estimator
            .update_at(101.0, now + Duration::from_secs(631))
            .unwrap()
            .unwrap();
        assert!(!snapshot.spike_detected);
    }

    #[test]
    fn rejects_invalid_prices_without_mutating_state() {
        let mut estimator = estimator(1);
        assert!(estimator.update(0.0).is_err());
        assert!(estimator.update(f64::NAN).is_err());
        assert!(estimator.latest().is_none());
        assert!(estimator.update(100.0).unwrap().is_none());
    }
}
