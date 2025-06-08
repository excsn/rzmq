use crate::throttle::strategies::{power_curve_strategy, ThrottlingStrategy};

/// Defines which direction of I/O should be prioritized by the throttle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
  /// Prioritize outgoing data (Egress). Typical for servers.
  Egress,
  /// Prioritize incoming data (Ingress). Typical for clients.
  Ingress,
  /// No specific priority; treat both directions equally.
  None,
}

/// A read-only snapshot of the throttle's state, passed to strategy functions.
pub struct ThrottleStateView<'a> {
  /// The current real-time debt/credit balance.
  pub current_balance: i32,
  /// The learned historical average of the balance.
  pub learned_balance: f64,
  /// A reference to the throttle's configuration for accessing parameters.
  pub config: &'a AdaptiveThrottleConfig,
}

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
  /// Interval in operations to force a periodic EMA nudge.
  pub nudge_interval_ops: u32,
  /// Controls how quickly the `learned_balance` adapts to the `current_balance`.
  /// A smaller value results in a slower, more stable moving average.
  /// Value should be between 0.0 and 1.0.
  pub adaptive_learning_rate: f64,
  /// Exponent for probability curve (e.g. 2.0 = quadratic).
  pub curve_factor: f64,
  /// A function pointer to the chosen probabilistic strategy. The library provides
  /// several default strategies in the `strategies` module.
  pub strategy: ThrottlingStrategy,
  /// The prioritized direction of I/O.
  /// The throttle will be more likely to yield for work in the non-prioritized
  /// direction when an imbalance occurs.
  pub priority: Priority,
  /// A multiplier applied to the yield probability for non-prioritized work.
  /// Values greater than 1.0 (e.g., 2.0 or 3.0) make it more likely to yield.
  pub priority_boost_factor: f64,
}

impl Default for AdaptiveThrottleConfig {
  fn default() -> Self {
    let mut lr = 0.05;
    if lr < 0.01 {
      lr = 0.01;
    }
    if lr > 0.2 {
      lr = 0.2;
    }

    Self {
      credit_per_message: 1,
      healthy_balance_width: 100,
      max_imbalance: 1000,
      yield_after_n_consecutive: 32,
      nudge_interval_ops: 100,
      adaptive_learning_rate: lr,
      curve_factor: 2.0, // Quadratic curve
      strategy: power_curve_strategy,
      priority: Priority::None,
      priority_boost_factor: 2.5, // A sensible default.
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
