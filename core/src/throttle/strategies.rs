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
