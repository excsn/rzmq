//! An adaptive, probabilistic throttle for managing I/O fairness in async actors.
//!
//! This module provides an `AdaptiveThrottle` that allows an actor to balance its
//! time between different workloads (e.g., incoming vs. outgoing data) without
//! explicit, complex scheduling logic in the actor itself.
//!
//! # Model
//!
//! The throttle uses a "DEBT/CREDIT" system, represented by a single `balance`
//! counter.
//! - **Ingress** (work coming in) adds to the balance, creating "debt."
//! - **Egress** (work going out) subtracts from the balance, paying it off.
//!
//! The throttle learns the normal operating balance for a given workload and only
//! intervenes when the system deviates significantly from this learned norm. The
//! intervention is a probabilistic `yield`, giving other tasks a chance to run.
//!
//! # Usage
//!
//! The API is designed around a simple, two-step cycle for each I/O operation:
//!
//! 1.  Call `throttle.begin_work()` before the operation to get a `ThrottleGuard`.
//!     This performs an anticipatory update to the throttle's state.
//! 2.  After the operation, call `guard.should_throttle().await`. This will
//!     run the probabilistic logic and potentially yield to the async runtime.
//!
//! ```rust,ignore
//! // Inside an actor's select! loop arm
//!
//! // 1. Announce the work and get the guard.
//! let guard = self.throttle.begin_work(Direction::Ingress);
//!
//! // 2. Do the I/O work. The outcome is not tracked by the throttle.
//! self.handle_incoming_message(message).await;
//!
//! // 3. Finalize the cycle, potentially yielding.
//! guard.should_throttle().await;
//! ```

pub mod strategies;
pub mod types;

pub use types::{AdaptiveThrottleConfig, Direction};
use types::ThrottleStateView;

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::Arc;

use atomic_float::AtomicF64;
use rand::random;

/// This struct holds both the mutable atomic state and the immutable config.
/// It is the single unit that will be shared via an `Arc` for cheap, thread-safe access.
#[derive(Debug)]
struct InternalSharedState {
  current_balance: AtomicI32,
  learned_balance: AtomicF64,
  consecutive_ingress: AtomicU32,
  consecutive_egress: AtomicU32,
  ops_since_nudge: AtomicU32,
  config: AdaptiveThrottleConfig,
}

// --- Main Throttle Struct ---

/// An adaptive, probabilistic throttle that can be shared across tasks.
///
/// It learns the natural I/O balance of a workload and gently yields CPU time
/// when the workload deviates significantly from its norm, preventing starvation
/// while maximizing throughput for bursty traffic.
#[derive(Debug, Clone)]
pub struct AdaptiveThrottle {
  shared: Arc<InternalSharedState>,
}

impl AdaptiveThrottle {
  /// Creates a new `AdaptiveThrottle` with the specified configuration.
  pub fn new(config: AdaptiveThrottleConfig) -> Self {
    let mut cfg = config.clone();
    // clamp learning rate
    if cfg.adaptive_learning_rate < 0.01 {
      cfg.adaptive_learning_rate = 0.01;
    }
    if cfg.adaptive_learning_rate > 0.2 {
      cfg.adaptive_learning_rate = 0.2;
    }
    let state = InternalSharedState {
      current_balance: AtomicI32::new(0),
      learned_balance: AtomicF64::new(0.0),
      consecutive_ingress: AtomicU32::new(0),
      consecutive_egress: AtomicU32::new(0),
      ops_since_nudge: AtomicU32::new(0),
      config: cfg,
    };
    Self {
      shared: Arc::new(state),
    }
  }

  /// Records the intent to perform an I/O operation and returns a `ThrottleGuard`.
  ///
  /// This method performs an "anticipatory" update to the throttle's internal
  /// state, reflecting the work that is about to happen. The returned guard
  /// must be used to complete the work cycle.
  pub fn begin_work(&self, dir: Direction) -> ThrottleGuard {
    let delta = self.shared.config.credit_per_message;
    let new_balance = match dir {
      Direction::Ingress => {
        self
          .shared
          .current_balance
          .fetch_add(delta, Ordering::Relaxed)
          + delta
      }
      Direction::Egress => {
        self
          .shared
          .current_balance
          .fetch_sub(delta, Ordering::Relaxed)
          - delta
      }
    };

    // Periodic EMA nudge
    let since = self.shared.ops_since_nudge.fetch_add(1, Ordering::Relaxed) + 1;
    if since >= self.shared.config.nudge_interval_ops {
      let α = self.shared.config.adaptive_learning_rate;
      let old_learned = self.shared.learned_balance.load(Ordering::Relaxed);
      let updated = α * (new_balance as f64) + (1.0 - α) * old_learned;
      self
        .shared
        .learned_balance
        .store(updated, Ordering::Relaxed);
      self.shared.ops_since_nudge.store(0, Ordering::Relaxed);
    }

    // Track direction-specific streaks
    match dir {
      Direction::Ingress => {
        self
          .shared
          .consecutive_ingress
          .fetch_add(1, Ordering::Relaxed);
        self.shared.consecutive_egress.store(0, Ordering::Relaxed);
      }
      Direction::Egress => {
        self
          .shared
          .consecutive_egress
          .fetch_add(1, Ordering::Relaxed);
        self.shared.consecutive_ingress.store(0, Ordering::Relaxed);
      }
    }

    ThrottleGuard {
      shared: self.shared.clone(),
      direction: dir,
    }
  }
}

// --- Throttle Guard ---

/// A temporary guard representing an in-progress I/O operation.
/// Its purpose is to call the throttling logic when the operation is complete.
pub struct ThrottleGuard {
  shared: Arc<InternalSharedState>,
  direction: Direction,
}

impl ThrottleGuard {
  pub fn get_current_balance(&self) -> i32 {
    return self.shared.current_balance.load(Ordering::Relaxed);
  }

  /// Finalizes the work cycle by checking the throttle's state and potentially
  /// yielding control to the async runtime.
  ///
  /// This method should be called after every I/O operation that was started
  /// with `begin_work`.
  pub fn should_throttle(&self) -> bool {
    let cfg = &self.shared.config;

    // Hard cap per-direction
    let cons = match self.direction {
      Direction::Ingress => self.shared.consecutive_ingress.load(Ordering::Relaxed),
      Direction::Egress => self.shared.consecutive_egress.load(Ordering::Relaxed),
    };
    if cons >= cfg.yield_after_n_consecutive {
      self.shared.consecutive_ingress.store(0, Ordering::Relaxed);
      self.shared.consecutive_egress.store(0, Ordering::Relaxed);
      return true;
    }

    // Probabilistic
    let state = ThrottleStateView {
      current_balance: self.shared.current_balance.load(Ordering::Relaxed),
      learned_balance: self.shared.learned_balance.load(Ordering::Relaxed),
      config: cfg,
    };
    let mut p = (cfg.strategy)(&state);
    // piecewise: beyond 2× max_imbalance, always yield
    let dev = (state.current_balance as f64 - state.learned_balance).abs();
    let hard_cut = (cfg.max_imbalance as f64) * 2.0;
    if dev >= hard_cut {
      p = 1.0;
    }

    if random::<f64>() < p {
      self.shared.consecutive_ingress.store(0, Ordering::Relaxed);
      self.shared.consecutive_egress.store(0, Ordering::Relaxed);
      return true;
    }
    
    false
  }
}
