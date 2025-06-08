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

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};
use std::sync::Arc;
use strategies::ThrottlingStrategy;

use atomic_float::AtomicF64;

// --- Public Configuration ---

/// Configuration for the `AdaptiveThrottle`.
///
/// These parameters allow tuning the throttle's behavior to prioritize
/// either low-latency responsiveness or high-throughput bursting.
#[derive(Debug, Clone)]
pub struct AdaptiveThrottleConfig {
  /// The unit of change for the balance counter per operation. Represents the
  /// "weight" of a single message.
  pub credit_per_message: i32,
  /// Defines a "zone of tolerance" around the learned balance. No probabilistic
  /// throttling occurs if the current balance is within this distance of the
  /// learned average. A larger value allows for larger, uninterrupted bursts.
  pub healthy_balance_width: u32,
  /// The distance from the healthy zone at which throttling probability becomes 100%.
  /// This acts as a hard safety limit to prevent starvation.
  pub max_imbalance: u32,
  /// A hard rule to guarantee fairness. The throttle will always yield after
  /// this many consecutive operations, regardless of the balance.
  pub yield_after_n_consecutive: u32,
  /// Controls how quickly the `learned_balance` adapts to the `current_balance`.
  /// A smaller value results in a slower, more stable moving average.
  /// Value should be between 0.0 and 1.0.
  pub adaptive_learning_rate: f64,
  /// A function pointer to the chosen probabilistic strategy. The library provides
  /// several default strategies in the `strategies` module.
  pub strategy: ThrottlingStrategy,
}

impl Default for AdaptiveThrottleConfig {
  fn default() -> Self {
    Self {
      credit_per_message: 10,
      healthy_balance_width: 1024,
      max_imbalance: 250,
      yield_after_n_consecutive: 16,
      adaptive_learning_rate: 0.005,
      strategy: strategies::power_curve_strategy,
    }
  }
}

// --- Internal State & Types ---

/// The direction of work relative to the actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
  /// Work coming into the actor (e.g., reading from a network socket).
  Ingress,
  /// Work going out from the actor (e.g., writing to a network socket).
  Egress,
}

/// This struct holds both the mutable atomic state and the immutable config.
/// It is the single unit that will be shared via an `Arc` for cheap, thread-safe access.
#[derive(Debug)]
struct InternalSharedState {
  // Atomics for mutable state
  current_balance: AtomicI32,
  learned_balance: AtomicF64,
  consecutive_count: AtomicU32,
  // Configuration
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
  shared_state: Arc<InternalSharedState>,
}

impl AdaptiveThrottle {
  /// Creates a new `AdaptiveThrottle` with the specified configuration.
  pub fn new(config: AdaptiveThrottleConfig) -> Self {
    let internal_state = InternalSharedState {
      current_balance: AtomicI32::new(0),
      learned_balance: AtomicF64::new(0.0),
      consecutive_count: AtomicU32::new(0),
      config,
    };
    Self {
      shared_state: Arc::new(internal_state),
    }
  }

  /// Records the intent to perform an I/O operation and returns a `ThrottleGuard`.
  ///
  /// This method performs an "anticipatory" update to the throttle's internal
  /// state, reflecting the work that is about to happen. The returned guard
  /// must be used to complete the work cycle.
  pub fn begin_work(&self, direction: Direction) -> ThrottleGuard {
    let balance_before_update = self.shared_state.current_balance.load(Ordering::Relaxed);

    let new_balance = match direction {
      Direction::Ingress => {
        self.shared_state.current_balance.fetch_add(
          self.shared_state.config.credit_per_message,
          Ordering::Relaxed,
        ) + self.shared_state.config.credit_per_message
      }
      Direction::Egress => {
        self.shared_state.current_balance.fetch_sub(
          self.shared_state.config.credit_per_message,
          Ordering::Relaxed,
        ) - self.shared_state.config.credit_per_message
      }
    };

    self
      .shared_state
      .consecutive_count
      .fetch_add(1, Ordering::Relaxed);

    // --- Adaptive Learning Logic ---
    // If the balance just crossed back into the healthy zone, it's a good
    // time to update our learned average.
    let learned_balance = self.shared_state.learned_balance.load(Ordering::Relaxed);
    let healthy_width = self.shared_state.config.healthy_balance_width as f64;
    let deviation_before = (balance_before_update as f64 - learned_balance).abs();
    let deviation_after = (new_balance as f64 - learned_balance).abs();

    if deviation_before > healthy_width && deviation_after <= healthy_width {
      let new_learned_balance = (1.0 - self.shared_state.config.adaptive_learning_rate)
        * learned_balance
        + self.shared_state.config.adaptive_learning_rate * (new_balance as f64);
      self
        .shared_state
        .learned_balance
        .store(new_learned_balance, Ordering::Relaxed);
    }

    ThrottleGuard {
      shared_state: self.shared_state.clone(),
    }
  }
}

// --- Throttle Guard ---

/// A temporary guard representing an in-progress I/O operation.
/// Its purpose is to call the throttling logic when the operation is complete.
pub struct ThrottleGuard {
  shared_state: Arc<InternalSharedState>,
}

impl ThrottleGuard {
  pub fn get_current_balance(&self) -> i32 {
    return self.shared_state.current_balance.load(Ordering::Relaxed);
  }
  
  /// Finalizes the work cycle by checking the throttle's state and potentially
  /// yielding control to the async runtime.
  ///
  /// This method should be called after every I/O operation that was started
  /// with `begin_work`.
  pub fn should_throttle(&self) -> bool {
    // Hard Rule: Check for consecutive actions.
    let consecutive = self.shared_state.consecutive_count.load(Ordering::Relaxed);
    if consecutive >= self.shared_state.config.yield_after_n_consecutive {
      // Reset the counter and yield.
      self
        .shared_state
        .consecutive_count
        .store(0, Ordering::Relaxed);
      return true;
    }

    // Probabilistic Rule: Use the configured strategy.
    let state_view = strategies::ThrottleStateView {
      current_balance: self.shared_state.current_balance.load(Ordering::Relaxed),
      learned_balance: self.shared_state.learned_balance.load(Ordering::Relaxed),
      config: &self.shared_state.config,
    };

    let probability = (self.shared_state.config.strategy)(&state_view);

    if rand::random::<f64>() < probability {
      // Reset the counter when yielding to give the other direction a fair chance.
      self
        .shared_state
        .consecutive_count
        .store(0, Ordering::Relaxed);
      
      return true;
    }

    return false;
  }
}
