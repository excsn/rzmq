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
