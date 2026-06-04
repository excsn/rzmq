//! A collection of default strategies for the `AdaptiveThrottle`.
//!
//! A throttling strategy is a function that takes a read-only view of the
//! throttle's current state and returns a yield probability (from 0.0 to 1.0).
//! This allows the core throttling logic to be pluggable.

use super::ThrottleStateView;

/// Defines the signature for any pluggable throttling strategy function.
pub type ThrottlingStrategy = fn(state: &ThrottleStateView) -> f64;

/// A powerful default strategy using a power curve.
///
/// This strategy is lenient with small imbalances but reacts aggressively to large
/// ones, providing a good balance between burst throughput and fairness. The yield
/// probability follows a quadratic (`x^2`) curve.

pub fn power_curve_strategy(state: &ThrottleStateView) -> f64 {
  let curve = state.config.curve_factor;
  let deviation = (state.current_balance as f64 - state.learned_balance).abs();
  let severity = (deviation - state.config.healthy_balance_width as f64).max(0.0);

  let max_sev = (state.config.max_imbalance - state.config.healthy_balance_width) as f64;
  if max_sev <= 0.0 {
    return if severity > 0.0 { 1.0 } else { 0.0 };
  }

  let x = (severity / max_sev).clamp(0.0, 1.0);
  x.powf(curve)
}

/// A simpler strategy that maps imbalance to yield probability linearly.
///
/// A 50% deviation outside the healthy zone will result in a 50% chance of yielding.
pub fn linear_strategy(state: &ThrottleStateView) -> f64 {
  let deviation = (state.current_balance as f64 - state.learned_balance).abs();
  let severity = (deviation - state.config.healthy_balance_width as f64).max(0.0);

  let max_possible_severity =
    (state.config.max_imbalance - state.config.healthy_balance_width) as f64;

  if max_possible_severity <= 0.0 {
    return if severity > 0.0 { 1.0 } else { 0.0 };
  }

  (severity / max_possible_severity).clamp(0.0, 1.0)
}

#[cfg(test)]
mod additional_strategy_tests {
  use super::*;
  use crate::throttle::types::{AdaptiveThrottleConfig, ThrottleStateView};

  #[test]
  fn test_linear_strategy_scaling() {
    let mut config = AdaptiveThrottleConfig::default();
    config.healthy_balance_width = 10;
    config.max_imbalance = 110; // severity space = 100

    // Within healthy zone: deviation 5 < width 10
    let state_healthy = ThrottleStateView { current_balance: 5, learned_balance: 0.0, config: &config };
    assert_eq!(linear_strategy(&state_healthy), 0.0);

    // 50% severity: deviation 60, severity = 50, max_sev = 100
    let state_half = ThrottleStateView { current_balance: 60, learned_balance: 0.0, config: &config };
    assert_eq!(linear_strategy(&state_half), 0.5);

    // Beyond max: deviation 200, severity = 190, clamped to 1.0
    let state_clamped = ThrottleStateView { current_balance: 200, learned_balance: 0.0, config: &config };
    assert_eq!(linear_strategy(&state_clamped), 1.0);
  }

  #[test]
  fn test_power_curve_strategy_scaling() {
    let mut config = AdaptiveThrottleConfig::default();
    config.healthy_balance_width = 0;
    config.max_imbalance = 100;
    config.curve_factor = 2.0;

    // deviation 50: x = 0.5, p = 0.5^2 = 0.25
    let state = ThrottleStateView { current_balance: 50, learned_balance: 0.0, config: &config };
    assert_eq!(power_curve_strategy(&state), 0.25);
  }
}
