use tokio::time::{Duration, Instant, sleep};
use tracing;

#[derive(Debug)]
pub(crate) struct SessionRegulator {
  started_at: Instant,
  min_lifespan: Duration,
}

impl SessionRegulator {
  pub fn new(min_lifespan: Duration) -> Self {
    Self {
      started_at: Instant::now(),
      min_lifespan,
    }
  }

  /// Checks the session uptime against the minimum lifespan.
  /// If the session is dying too young (churn), this method sleeps
  /// for the remainder of the required time.
  pub async fn enforce_min_lifespan(&self) {
    let uptime = self.started_at.elapsed();
    if uptime < self.min_lifespan {
      let penalty = self.min_lifespan - uptime;
      tracing::warn!(
        uptime_ms = uptime.as_millis(),
        penalty_ms = penalty.as_millis(),
        "SessionRegulator: Session died too quickly (churn detected). Enforcing penalty wait."
      );
      sleep(penalty).await;
    }
  }
}

#[cfg(test)]
mod additional_regulator_tests {
  use super::*;
  use std::time::Instant;

  #[tokio::test]
  async fn test_regulator_under_lifespan_penalty() {
    let regulator = SessionRegulator::new(Duration::from_millis(100));

    let start = Instant::now();
    regulator.enforce_min_lifespan().await;
    let elapsed = start.elapsed();

    assert!(
      elapsed >= Duration::from_millis(90),
      "Should block for most of the penalty, got {:?}",
      elapsed
    );
  }

  #[tokio::test]
  async fn test_regulator_over_lifespan_bypass() {
    let regulator = SessionRegulator::new(Duration::from_millis(10));

    tokio::time::sleep(Duration::from_millis(15)).await;

    let start = Instant::now();
    regulator.enforce_min_lifespan().await;
    let elapsed = start.elapsed();

    assert!(
      elapsed < Duration::from_millis(5),
      "Should bypass wait, got {:?}",
      elapsed
    );
  }
}
