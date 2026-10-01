use crate::error::ZmqError;
use crate::socket::options::{ADAPTIVE_THROTTLE, ADAPTIVE_THROTTLE_STATS};
use crate::socket::types::{FromBytes, ToBytes};
use crate::throttle::strategies::{linear_strategy, power_curve_strategy, ThrottlingStrategy};
use crate::throttle::types::{AdaptiveThrottleConfig, Priority};

const ENCODING_VERSION: u8 = 1;
const CONFIG_ENCODED_LEN: usize = 48;

/// Probability curve the throttle uses to decide whether to yield.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ThrottleStrategy {
  /// Yield probability grows as `x^exponent` of the imbalance beyond the healthy zone.
  PowerCurve { exponent: f64 },
  /// Yield probability grows linearly with the imbalance beyond the healthy zone.
  Linear,
}

/// Which I/O direction the throttle favours when the balance drifts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottlePriority {
  /// Egress for connections accepted by a listener, Ingress for outbound connections.
  Auto,
  Egress,
  Ingress,
  None,
}

/// Adaptive throttle settings for a socket, set and read through `ADAPTIVE_THROTTLE`.
#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveThrottleSocketConfig {
  pub enabled: bool,
  /// Balance change per message.
  pub credit_per_message: i32,
  /// Distance from the learned balance within which no probabilistic yield happens.
  pub healthy_balance_width: u32,
  /// Distance beyond the healthy zone at which the yield probability reaches 1.0.
  pub max_imbalance: u32,
  /// Operations in one direction after which the throttle always yields.
  pub yield_after_n_consecutive: u32,
  /// Operations between updates of the learned balance.
  pub nudge_interval_ops: u32,
  /// Weight of the current balance in the learned-balance moving average, clamped to `0.01..=0.2`.
  pub adaptive_learning_rate: f64,
  pub strategy: ThrottleStrategy,
  pub priority: ThrottlePriority,
  /// Multiplier on the yield probability for work in the non-prioritised direction.
  pub priority_boost_factor: f64,
}

impl Default for AdaptiveThrottleSocketConfig {
  fn default() -> Self {
    Self {
      enabled: true,
      credit_per_message: 5,
      healthy_balance_width: 1_024_000,
      max_imbalance: 6_553_600,
      yield_after_n_consecutive: 256,
      nudge_interval_ops: 100,
      adaptive_learning_rate: 0.05,
      strategy: ThrottleStrategy::PowerCurve { exponent: 2.0 },
      priority: ThrottlePriority::Auto,
      priority_boost_factor: 5.0,
    }
  }
}

/// `Auto` maps to `Priority::None`; the session actor resolves it from its role.
impl From<&AdaptiveThrottleSocketConfig> for AdaptiveThrottleConfig {
  fn from(cfg: &AdaptiveThrottleSocketConfig) -> Self {
    let (strategy, curve_factor) = match cfg.strategy {
      ThrottleStrategy::PowerCurve { exponent } => (power_curve_strategy as ThrottlingStrategy, exponent),
      ThrottleStrategy::Linear => (linear_strategy as ThrottlingStrategy, 0.0),
    };
    AdaptiveThrottleConfig {
      credit_per_message: cfg.credit_per_message,
      healthy_balance_width: cfg.healthy_balance_width,
      max_imbalance: cfg.max_imbalance,
      yield_after_n_consecutive: cfg.yield_after_n_consecutive,
      nudge_interval_ops: cfg.nudge_interval_ops,
      adaptive_learning_rate: cfg.adaptive_learning_rate,
      curve_factor,
      strategy,
      priority: match cfg.priority {
        ThrottlePriority::Auto | ThrottlePriority::None => Priority::None,
        ThrottlePriority::Egress => Priority::Egress,
        ThrottlePriority::Ingress => Priority::Ingress,
      },
      priority_boost_factor: cfg.priority_boost_factor,
      enabled: cfg.enabled,
    }
  }
}

impl From<Priority> for ThrottlePriority {
  fn from(p: Priority) -> Self {
    match p {
      Priority::Egress => ThrottlePriority::Egress,
      Priority::Ingress => ThrottlePriority::Ingress,
      Priority::None => ThrottlePriority::None,
    }
  }
}

/// State of one connection's throttle, read through `ADAPTIVE_THROTTLE_STATS`.
#[derive(Debug, Clone, PartialEq)]
pub struct ThrottleStats {
  pub endpoint_uri: String,
  /// The priority in effect, never `Auto`.
  pub priority: ThrottlePriority,
  pub current_balance: i32,
  pub learned_balance: f64,
  pub consecutive_ingress: u32,
  pub consecutive_egress: u32,
}

fn priority_tag(p: ThrottlePriority) -> u8 {
  match p {
    ThrottlePriority::Auto => 0,
    ThrottlePriority::Egress => 1,
    ThrottlePriority::Ingress => 2,
    ThrottlePriority::None => 3,
  }
}

fn priority_from_tag(tag: u8, option_id: i32) -> Result<ThrottlePriority, ZmqError> {
  match tag {
    0 => Ok(ThrottlePriority::Auto),
    1 => Ok(ThrottlePriority::Egress),
    2 => Ok(ThrottlePriority::Ingress),
    3 => Ok(ThrottlePriority::None),
    _ => Err(ZmqError::InvalidOptionValue(option_id)),
  }
}

struct Reader<'a> {
  buf: &'a [u8],
  option_id: i32,
}

impl<'a> Reader<'a> {
  fn take<const N: usize>(&mut self) -> Result<[u8; N], ZmqError> {
    if self.buf.len() < N {
      return Err(ZmqError::InvalidOptionValue(self.option_id));
    }
    let (head, rest) = self.buf.split_at(N);
    self.buf = rest;
    Ok(head.try_into().unwrap())
  }
  fn u8(&mut self) -> Result<u8, ZmqError> {
    Ok(self.take::<1>()?[0])
  }
  fn i32(&mut self) -> Result<i32, ZmqError> {
    Ok(i32::from_ne_bytes(self.take()?))
  }
  fn u32(&mut self) -> Result<u32, ZmqError> {
    Ok(u32::from_ne_bytes(self.take()?))
  }
  fn f64(&mut self) -> Result<f64, ZmqError> {
    Ok(f64::from_ne_bytes(self.take()?))
  }
  fn bytes(&mut self, len: usize) -> Result<&'a [u8], ZmqError> {
    if self.buf.len() < len {
      return Err(ZmqError::InvalidOptionValue(self.option_id));
    }
    let (head, rest) = self.buf.split_at(len);
    self.buf = rest;
    Ok(head)
  }
  fn version(&mut self) -> Result<(), ZmqError> {
    if self.u8()? != ENCODING_VERSION {
      return Err(ZmqError::InvalidOptionValue(self.option_id));
    }
    Ok(())
  }
  fn finish(self) -> Result<(), ZmqError> {
    if self.buf.is_empty() {
      Ok(())
    } else {
      Err(ZmqError::InvalidOptionValue(self.option_id))
    }
  }
}

impl ToBytes for AdaptiveThrottleSocketConfig {
  fn to_bytes(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(CONFIG_ENCODED_LEN);
    out.push(ENCODING_VERSION);
    out.push(self.enabled as u8);
    out.extend_from_slice(&self.credit_per_message.to_ne_bytes());
    out.extend_from_slice(&self.healthy_balance_width.to_ne_bytes());
    out.extend_from_slice(&self.max_imbalance.to_ne_bytes());
    out.extend_from_slice(&self.yield_after_n_consecutive.to_ne_bytes());
    out.extend_from_slice(&self.nudge_interval_ops.to_ne_bytes());
    out.extend_from_slice(&self.adaptive_learning_rate.to_ne_bytes());
    let (strategy_tag, exponent) = match self.strategy {
      ThrottleStrategy::PowerCurve { exponent } => (0u8, exponent),
      ThrottleStrategy::Linear => (1u8, 0.0),
    };
    out.push(strategy_tag);
    out.extend_from_slice(&exponent.to_ne_bytes());
    out.push(priority_tag(self.priority));
    out.extend_from_slice(&self.priority_boost_factor.to_ne_bytes());
    out
  }
}

impl ToBytes for &AdaptiveThrottleSocketConfig {
  fn to_bytes(&self) -> Vec<u8> {
    (*self).to_bytes()
  }
}

impl FromBytes for AdaptiveThrottleSocketConfig {
  fn from_bytes(bytes: &[u8]) -> Result<Self, ZmqError> {
    let mut r = Reader { buf: bytes, option_id: ADAPTIVE_THROTTLE };
    r.version()?;
    let enabled = match r.u8()? {
      0 => false,
      1 => true,
      _ => return Err(ZmqError::InvalidOptionValue(ADAPTIVE_THROTTLE)),
    };
    let credit_per_message = r.i32()?;
    let healthy_balance_width = r.u32()?;
    let max_imbalance = r.u32()?;
    let yield_after_n_consecutive = r.u32()?;
    let nudge_interval_ops = r.u32()?;
    let adaptive_learning_rate = r.f64()?;
    let strategy_tag = r.u8()?;
    let exponent = r.f64()?;
    let strategy = match strategy_tag {
      0 => ThrottleStrategy::PowerCurve { exponent },
      1 => ThrottleStrategy::Linear,
      _ => return Err(ZmqError::InvalidOptionValue(ADAPTIVE_THROTTLE)),
    };
    let priority = priority_from_tag(r.u8()?, ADAPTIVE_THROTTLE)?;
    let priority_boost_factor = r.f64()?;
    r.finish()?;
    Ok(Self {
      enabled,
      credit_per_message,
      healthy_balance_width,
      max_imbalance,
      yield_after_n_consecutive,
      nudge_interval_ops,
      adaptive_learning_rate,
      strategy,
      priority,
      priority_boost_factor,
    })
  }
}

pub(crate) fn encode_throttle_stats(stats: &[ThrottleStats]) -> Vec<u8> {
  let mut out = Vec::new();
  out.push(ENCODING_VERSION);
  out.extend_from_slice(&(stats.len() as u32).to_ne_bytes());
  for s in stats {
    out.extend_from_slice(&(s.endpoint_uri.len() as u32).to_ne_bytes());
    out.extend_from_slice(s.endpoint_uri.as_bytes());
    out.push(priority_tag(s.priority));
    out.extend_from_slice(&s.current_balance.to_ne_bytes());
    out.extend_from_slice(&s.learned_balance.to_ne_bytes());
    out.extend_from_slice(&s.consecutive_ingress.to_ne_bytes());
    out.extend_from_slice(&s.consecutive_egress.to_ne_bytes());
  }
  out
}

impl FromBytes for Vec<ThrottleStats> {
  fn from_bytes(bytes: &[u8]) -> Result<Self, ZmqError> {
    let id = ADAPTIVE_THROTTLE_STATS;
    let mut r = Reader { buf: bytes, option_id: id };
    r.version()?;
    let count = r.u32()? as usize;
    let mut out = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
      let uri_len = r.u32()? as usize;
      let endpoint_uri = String::from_utf8(r.bytes(uri_len)?.to_vec())
        .map_err(|_| ZmqError::InvalidOptionValue(id))?;
      out.push(ThrottleStats {
        endpoint_uri,
        priority: priority_from_tag(r.u8()?, id)?,
        current_balance: r.i32()?,
        learned_balance: r.f64()?,
        consecutive_ingress: r.u32()?,
        consecutive_egress: r.u32()?,
      });
    }
    r.finish()?;
    Ok(out)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn config_round_trips() {
    let cfg = AdaptiveThrottleSocketConfig {
      enabled: false,
      credit_per_message: 3,
      healthy_balance_width: 10,
      max_imbalance: 50,
      yield_after_n_consecutive: 7,
      nudge_interval_ops: 9,
      adaptive_learning_rate: 0.1,
      strategy: ThrottleStrategy::Linear,
      priority: ThrottlePriority::Ingress,
      priority_boost_factor: 2.0,
    };
    let bytes = cfg.to_bytes();
    assert_eq!(bytes.len(), CONFIG_ENCODED_LEN);
    assert_eq!(AdaptiveThrottleSocketConfig::from_bytes(&bytes).unwrap(), cfg);

    let default = AdaptiveThrottleSocketConfig::default();
    assert_eq!(AdaptiveThrottleSocketConfig::from_bytes(&default.to_bytes()).unwrap(), default);
  }

  #[test]
  fn config_rejects_bad_encodings() {
    let good = AdaptiveThrottleSocketConfig::default().to_bytes();

    assert!(AdaptiveThrottleSocketConfig::from_bytes(&good[..good.len() - 1]).is_err());

    let mut long = good.clone();
    long.push(0);
    assert!(AdaptiveThrottleSocketConfig::from_bytes(&long).is_err());

    let mut bad_version = good.clone();
    bad_version[0] = 99;
    assert!(AdaptiveThrottleSocketConfig::from_bytes(&bad_version).is_err());

    let mut bad_strategy = good.clone();
    bad_strategy[30] = 7;
    assert!(AdaptiveThrottleSocketConfig::from_bytes(&bad_strategy).is_err());

    let mut bad_priority = good;
    bad_priority[39] = 7;
    assert!(AdaptiveThrottleSocketConfig::from_bytes(&bad_priority).is_err());
  }

  #[test]
  fn stats_round_trip() {
    let stats = vec![
      ThrottleStats {
        endpoint_uri: "tcp://127.0.0.1:5555".into(),
        priority: ThrottlePriority::Egress,
        current_balance: -40,
        learned_balance: -12.5,
        consecutive_ingress: 0,
        consecutive_egress: 8,
      },
      ThrottleStats {
        endpoint_uri: "ipc:///tmp/x".into(),
        priority: ThrottlePriority::Ingress,
        current_balance: 15,
        learned_balance: 3.0,
        consecutive_ingress: 3,
        consecutive_egress: 0,
      },
    ];
    let bytes = encode_throttle_stats(&stats);
    assert_eq!(Vec::<ThrottleStats>::from_bytes(&bytes).unwrap(), stats);
    assert_eq!(Vec::<ThrottleStats>::from_bytes(&encode_throttle_stats(&[])).unwrap(), vec![]);
    assert!(Vec::<ThrottleStats>::from_bytes(&bytes[..bytes.len() - 1]).is_err());
  }
}
