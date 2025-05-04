// src/socket/options.rs

use std::time::Duration;

use crate::{Blob, ZmqError};

// <<< ADDED ZMQ OPTION CONSTANTS (Examples) >>>
// Use values consistent with libzmq where possible
pub const SNDHWM: i32 = 23;
pub const RCVHWM: i32 = 24;
pub const LINGER: i32 = 17;
pub const SUBSCRIBE: i32 = 6;
pub const UNSUBSCRIBE: i32 = 7;
pub const ROUTING_ID: i32 = 5; // Often called ZMQ_IDENTITY
pub const RCVTIMEO: i32 = 27;
pub const SNDTIMEO: i32 = 28;
pub const TCP_KEEPALIVE: i32 = 34;
pub const TCP_KEEPALIVE_IDLE: i32 = 35;
pub const TCP_KEEPALIVE_CNT: i32 = 36;
pub const TCP_KEEPALIVE_INTVL: i32 = 37;
// Security Options
pub const ZAP_DOMAIN: i32 = 55;
pub const PLAIN_SERVER: i32 = 44;
pub const PLAIN_USERNAME: i32 = 45;
pub const PLAIN_PASSWORD: i32 = 46;
pub const CURVE_SERVER: i32 = 47;
pub const CURVE_PUBLICKEY: i32 = 48;
pub const CURVE_SECRETKEY: i32 = 49;
pub const CURVE_SERVERKEY: i32 = 50;
// Add more constants as needed...
// <<< ADDED ZMQ OPTION CONSTANTS END >>>

// <<< MOVED/ADDED SocketOptions STRUCT >>>
/// Holds parsed and validated socket options.
#[derive(Debug, Clone)]
pub(crate) struct SocketOptions {
  // High Water Marks (applied to Pipes / internal queues)
  pub rcvhwm: usize,
  pub sndhwm: usize,
  // Timeouts (used by SocketCore/ISocket recv/send logic?)
  pub rcvtimeo: Option<Duration>, // ZMQ uses -1 for none, 0 for immediate, >0 for ms
  pub sndtimeo: Option<Duration>,
  // Connection Behavior
  pub linger: Option<Duration>, // ZMQ uses -1 for infinite, 0 for immediate, >0 for ms -> map to Duration
  // pub reconnect_ivl: Duration, // TODO
  // pub reconnect_ivl_max: Duration, // TODO
  // pub backlog: Option<u32>, // TODO - For listener
  // Identity
  pub routing_id: Option<Blob>,
  // TCP Specific (mirrored for setting on stream)
  pub tcp_keepalive_enabled: i32, // ZMQ standard: -1 off, 0 system, 1 on
  pub tcp_keepalive_idle: Option<Duration>,
  pub tcp_keepalive_count: Option<u32>,
  pub tcp_keepalive_interval: Option<Duration>,
  pub tcp_nodelay: bool, // Usually enabled by default
                         // Add other commonly used options as needed
                         // pub plain_username: Option<String>, // Security options stored here?
                         // pub plain_password: Option<String>,
                         // pub curve_secret_key: Option<[u8; 32]>, // Fixed size keys
}

impl Default for SocketOptions {
  fn default() -> Self {
    Self {
      // ZMQ Defaults:
      rcvhwm: 1000,
      sndhwm: 1000,
      rcvtimeo: None,                         // -1 in ZMQ
      sndtimeo: None,                         // -1 in ZMQ
      linger: Some(Duration::from_millis(0)), // 0 in ZMQ (different from socket default!)
      routing_id: None,
      tcp_keepalive_enabled: 0, // 0 (use system default) in ZMQ
      tcp_keepalive_idle: None,
      tcp_keepalive_count: None,
      tcp_keepalive_interval: None,
      tcp_nodelay: true, // Common default for messaging
    }
  }
}
// <<< MOVED/ADDED SocketOptions STRUCT END >>>

// <<< ADDED TcpConfig STRUCT (placeholder, refine later) >>>
// Config specific to TCP transport, potentially influenced by socket options
#[derive(Debug, Clone, Default)]
pub(crate) struct ZmtpTcpConfig {
  pub use_cork: bool,    // Default: true?
  pub tcp_nodelay: bool, // Default: true
  pub keepalive_time: Option<Duration>,
  pub keepalive_interval: Option<Duration>,
  pub keepalive_count: Option<u32>,
  #[cfg(feature = "io-uring")]
  pub use_send_zerocopy: bool,
  #[cfg(feature = "io-uring")]
  pub use_recv_multishot: bool,
}
// <<< ADDED TcpConfig STRUCT END >>>

// --- Helper functions for parsing option values ---
/// Parses a byte slice representing an integer option (like HWM, linger).
pub(crate) fn parse_i32_option(value: &[u8]) -> Result<i32, ZmqError> {
  let arr: [u8; 4] = value
    .try_into()
    .map_err(|_| ZmqError::InvalidOptionValue(0))?; // Use generic error for now
                                                    // Assuming native endianness for socket options based on ZMQ C API usage
  Ok(i32::from_ne_bytes(arr))
}

/// Parses a byte slice representing a boolean option (0 or 1).
pub(crate) fn parse_bool_option(value: &[u8]) -> Result<bool, ZmqError> {
  Ok(parse_i32_option(value)? == 1)
}

/// Parses a byte slice representing a timeout or linger value in milliseconds.
/// ZMQ uses -1 for infinite, 0 for immediate (no linger), >0 for duration.
pub(crate) fn parse_duration_ms_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value)?;
  match val {
    -1 => Ok(None),                                     // Infinite timeout / linger
    0.. => Ok(Some(Duration::from_millis(val as u64))), // Non-negative -> Duration
    // Negative values other than -1 are invalid for timeouts/linger
    _ => Err(ZmqError::InvalidOptionValue(0)), // Use generic error
  }
}

/// Parses a byte slice representing a duration in seconds for TCP Keepalive options.
/// ZMQ uses integers for seconds. 0 might mean "use system default".
pub(crate) fn parse_secs_duration_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value)?;
  match val {
    0..=i32::MAX => Ok(Some(Duration::from_secs(val as u64))),
    // Negative values invalid? Or does -1 mean system default? Check ZMQ spec/impl. Assume invalid for now.
    _ => Err(ZmqError::InvalidOptionValue(0)),
  }
}

pub(crate) fn parse_linger_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  // <<< MODIFIED END >>>
  let val = parse_i32_option(value)?;
  match val {
    -1 => Ok(None),                                     // None represents infinite linger
    0.. => Ok(Some(Duration::from_millis(val as u64))), // Non-negative -> Duration
    _ => Err(ZmqError::InvalidOptionValue(LINGER)),     // Other negative values invalid
  }
}

/// Parses a byte slice representing a count for TCP Keepalive.
pub(crate) fn parse_u32_option(value: &[u8]) -> Result<Option<u32>, ZmqError> {
  let val = parse_i32_option(value)?; // ZMQ uses int
  match val {
    0..=i32::MAX => Ok(Some(val as u32)),
    _ => Err(ZmqError::InvalidOptionValue(0)),
  }
}

/// Parses the ZMQ_TCP_KEEPALIVE option (-1, 0, 1).
pub(crate) fn parse_keepalive_mode_option(value: &[u8]) -> Result<i32, ZmqError> {
  let val = parse_i32_option(value)?;
  if val >= -1 && val <= 1 {
    Ok(val)
  } else {
    Err(ZmqError::InvalidOptionValue(TCP_KEEPALIVE))
  }
}

/// Parses a byte slice into a Blob (for ROUTING_ID).
pub(crate) fn parse_blob_option(value: &[u8]) -> Result<Blob, ZmqError> {
  // ZMQ identities have length limits (max 255 bytes)
  if value.len() > 255 {
    Err(ZmqError::InvalidOptionValue(ROUTING_ID)) // Or specific error
  } else {
    Ok(Blob::from(value.to_vec())) // Clone into Blob
  }
}
