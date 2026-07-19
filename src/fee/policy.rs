use std::time::{Duration, Instant};

use anyhow::{Result, bail};

use super::{
    FEE_UNITS_PER_BPS, config::UpdatePolicyConfig, curve::FeeCurve, volatility::VolatilitySnapshot,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateTrigger {
    Deadband,
    VolatilitySpike,
    DeadbandAndVolatilitySpike,
    ValidityRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Evaluation {
    WaitingForSample,
    WarmingUp {
        target_fee_bps: f64,
        return_samples: u64,
    },
    Stale {
        sample_age: Duration,
    },
    NoChange {
        target_fee_bps: f64,
        difference_bps: f64,
    },
    Push {
        target_fee_bps: f64,
        surge_fee_units: u16,
        trigger: UpdateTrigger,
        spike_detected: bool,
    },
}

#[derive(Debug)]
pub struct FeePolicy {
    curve: FeeCurve,
    policy: UpdatePolicyConfig,
    max_sample_age: Duration,
    refresh_after: Duration,
    last_push_at: Option<Instant>,
    spike_active: bool,
}

impl FeePolicy {
    pub fn new(
        curve: FeeCurve,
        policy: UpdatePolicyConfig,
        max_sample_age: Duration,
        validity_period: Duration,
        refresh_fraction: f64,
    ) -> Self {
        Self {
            curve,
            policy,
            max_sample_age,
            refresh_after: validity_period.mul_f64(refresh_fraction),
            last_push_at: None,
            spike_active: false,
        }
    }

    pub fn evaluate(
        &mut self,
        snapshot: Option<VolatilitySnapshot>,
        current_fee_bps: f64,
        now: Instant,
    ) -> Result<Evaluation> {
        let Some(snapshot) = snapshot else {
            return Ok(Evaluation::WaitingForSample);
        };
        let target_fee_bps = self.curve.target_fee_bps(snapshot.sigma_pct_day)?;
        if !snapshot.warmed_up {
            return Ok(Evaluation::WarmingUp {
                target_fee_bps,
                return_samples: snapshot.return_samples,
            });
        }
        let sample_age = now.saturating_duration_since(snapshot.sampled_at);
        if sample_age > self.max_sample_age {
            return Ok(Evaluation::Stale { sample_age });
        }
        if !current_fee_bps.is_finite() || current_fee_bps < 0.0 {
            bail!("current fee must be finite and non-negative, got {current_fee_bps}");
        }

        let difference_bps = (target_fee_bps - current_fee_bps).abs();
        let spike_event = snapshot.spike_detected && !self.spike_active;
        let outside_deadband = difference_bps >= self.policy.deadband_bps;
        let refresh_due = self
            .last_push_at
            .is_none_or(|last_push| now.saturating_duration_since(last_push) >= self.refresh_after);

        if outside_deadband || spike_event || refresh_due {
            let trigger = match (outside_deadband, spike_event, refresh_due) {
                (true, true, _) => UpdateTrigger::DeadbandAndVolatilitySpike,
                (true, false, _) => UpdateTrigger::Deadband,
                (false, true, _) => UpdateTrigger::VolatilitySpike,
                (false, false, true) => UpdateTrigger::ValidityRefresh,
                (false, false, false) => unreachable!(),
            };
            let surge_bps = (target_fee_bps - self.curve.config().floor_bps).max(0.0);
            let surge_fee_units = (surge_bps * FEE_UNITS_PER_BPS).round();
            if !(0.0..=u16::MAX as f64).contains(&surge_fee_units) {
                bail!("calculated surge fee does not fit the Minizeke fee scale");
            }
            return Ok(Evaluation::Push {
                target_fee_bps,
                surge_fee_units: surge_fee_units as u16,
                trigger,
                spike_detected: snapshot.spike_detected,
            });
        }

        self.spike_active = snapshot.spike_detected;
        Ok(Evaluation::NoChange {
            target_fee_bps,
            difference_bps,
        })
    }

    /// Records a successfully persisted push. Failed pushes must not call this,
    /// preserving the spike edge and refresh retry behavior.
    pub fn mark_pushed(&mut self, now: Instant, spike_detected: bool) {
        self.last_push_at = Some(now);
        self.spike_active = spike_detected;
    }

    pub fn last_push_at(&self) -> Option<Instant> {
        self.last_push_at
    }

    pub fn refresh_after(&self) -> Duration {
        self.refresh_after
    }
}

#[cfg(test)]
mod tests {
    use super::super::config::FeeCurveConfig;
    use super::*;

    fn policy() -> FeePolicy {
        FeePolicy::new(
            FeeCurve::new(FeeCurveConfig {
                a: -5.0,
                b: 12.4,
                floor_bps: 6.5,
                max_bps: 33.0,
            }),
            UpdatePolicyConfig {
                interval: Duration::from_secs(60),
                deadband_bps: 1.5,
                spike_ratio: 1.5,
                spike_window: Duration::from_secs(600),
            },
            Duration::from_secs(90),
            Duration::from_secs(600),
            0.8,
        )
    }

    fn snapshot(
        now: Instant,
        sigma_pct_day: f64,
        warmed_up: bool,
        spike_detected: bool,
    ) -> VolatilitySnapshot {
        VolatilitySnapshot {
            sigma_pct_day,
            return_samples: if warmed_up { 20 } else { 19 },
            sampled_at: now,
            warmed_up,
            spike_detected,
        }
    }

    #[test]
    fn gates_missing_warmup_and_stale_samples() -> Result<()> {
        let now = Instant::now();
        let mut policy = policy();
        assert_eq!(
            policy.evaluate(None, 6.5, now)?,
            Evaluation::WaitingForSample
        );
        assert!(matches!(
            policy.evaluate(Some(snapshot(now, 2.0, false, false)), 6.5, now)?,
            Evaluation::WarmingUp { .. }
        ));
        assert_eq!(
            policy.evaluate(
                Some(snapshot(now - Duration::from_secs(91), 2.0, true, false)),
                6.5,
                now
            )?,
            Evaluation::Stale {
                sample_age: Duration::from_secs(91)
            }
        );
        Ok(())
    }

    #[test]
    fn pushes_outside_deadband_and_converts_only_surge() -> Result<()> {
        let now = Instant::now();
        let mut policy = policy();
        let evaluation = policy.evaluate(Some(snapshot(now, 4.0, true, false)), 6.5, now)?;
        let Evaluation::Push {
            target_fee_bps,
            surge_fee_units,
            trigger,
            ..
        } = evaluation
        else {
            panic!("expected push");
        };
        assert!((target_fee_bps - 14.957).abs() < 0.01);
        assert_eq!(surge_fee_units, 846);
        assert_eq!(trigger, UpdateTrigger::Deadband);
        Ok(())
    }

    #[test]
    fn refreshes_at_fraction_of_validity() -> Result<()> {
        let now = Instant::now();
        let mut policy = policy();
        let stable = snapshot(now, 0.0, true, false);
        let first = policy.evaluate(Some(stable), 6.5, now)?;
        assert!(matches!(
            first,
            Evaluation::Push {
                trigger: UpdateTrigger::ValidityRefresh,
                ..
            }
        ));
        policy.mark_pushed(now, false);
        assert!(matches!(
            policy.evaluate(
                Some(snapshot(now + Duration::from_secs(479), 0.0, true, false)),
                6.5,
                now + Duration::from_secs(479)
            )?,
            Evaluation::NoChange { .. }
        ));
        assert!(matches!(
            policy.evaluate(
                Some(snapshot(now + Duration::from_secs(480), 0.0, true, false)),
                6.5,
                now + Duration::from_secs(480)
            )?,
            Evaluation::Push {
                trigger: UpdateTrigger::ValidityRefresh,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn spike_fires_only_on_rising_edge() -> Result<()> {
        let now = Instant::now();
        let mut policy = policy();
        policy.mark_pushed(now, false);
        let spike = snapshot(now + Duration::from_secs(1), 0.0, true, true);
        let first = policy.evaluate(Some(spike), 6.5, now + Duration::from_secs(1))?;
        assert!(matches!(
            first,
            Evaluation::Push {
                trigger: UpdateTrigger::VolatilitySpike,
                ..
            }
        ));
        policy.mark_pushed(now + Duration::from_secs(1), true);
        let continuing = snapshot(now + Duration::from_secs(2), 0.0, true, true);
        assert!(matches!(
            policy.evaluate(Some(continuing), 6.5, now + Duration::from_secs(2))?,
            Evaluation::NoChange { .. }
        ));

        let clear = snapshot(now + Duration::from_secs(3), 0.0, true, false);
        policy.evaluate(Some(clear), 6.5, now + Duration::from_secs(3))?;
        let next_edge = snapshot(now + Duration::from_secs(4), 0.0, true, true);
        assert!(matches!(
            policy.evaluate(Some(next_edge), 6.5, now + Duration::from_secs(4))?,
            Evaluation::Push {
                trigger: UpdateTrigger::VolatilitySpike,
                ..
            }
        ));
        Ok(())
    }

    #[test]
    fn failed_push_remains_due_and_invalid_current_fee_is_rejected() -> Result<()> {
        let now = Instant::now();
        let mut policy = policy();
        let stable = snapshot(now, 0.0, true, false);
        assert!(matches!(
            policy.evaluate(Some(stable), 6.5, now)?,
            Evaluation::Push { .. }
        ));
        assert!(matches!(
            policy.evaluate(Some(stable), 6.5, now)?,
            Evaluation::Push { .. }
        ));
        assert!(policy.evaluate(Some(stable), f64::NAN, now).is_err());
        Ok(())
    }
}
