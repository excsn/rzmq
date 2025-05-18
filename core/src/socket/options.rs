// src/socket/options.rs

use std::time::Duration;

use crate::{Blob, ZmqError};

// Use values consistent with libzmq where possible
pub const SNDHWM: i32 = 23;
pub const RCVHWM: i32 = 24;
pub const LINGER: i32 = 17;
pub const SUBSCRIBE: i32 = 6;
pub const UNSUBSCRIBE: i32 = 7;
pub const ROUTING_ID: i32 = 5; // Often called ZMQ_IDENTITY
pub const RECONNECT_IVL: i32 = 18; // ZMQ_RECONNECT_IVL
pub const RECONNECT_IVL_MAX: i32 = 21; // ZMQ_RECONNECT_IVL_MAX
pub const RCVTIMEO: i32 = 27;
pub const SNDTIMEO: i32 = 28;
pub const TCP_KEEPALIVE: i32 = 34;
pub const TCP_KEEPALIVE_IDLE: i32 = 35;
pub const TCP_KEEPALIVE_CNT: i32 = 36;
pub const TCP_KEEPALIVE_INTVL: i32 = 37;
pub const HEARTBEAT_IVL: i32 = 38; // ZMQ_HEARTBEAT_IVL
pub const HEARTBEAT_TIMEOUT: i32 = 39; // ZMQ_HEARTBEAT_TIMEOUT
pub const HEARTBEAT_TTL: i32 = 40; // ZMQ_HEARTBEAT_TTL (Often derived from TIMEOUT)

pub const ROUTER_MANDATORY: i32 = 33;

// Security Options
pub const ZAP_DOMAIN: i32 = 55;
pub const PLAIN_SERVER: i32 = 44;
pub const PLAIN_USERNAME: i32 = 45;
pub const PLAIN_PASSWORD: i32 = 46;

#[cfg(feature = "curve")]
pub const CURVE_SERVER: i32 = 47;
#[cfg(feature = "curve")]
pub const CURVE_PUBLICKEY: i32 = 48;
#[cfg(feature = "curve")]
pub const CURVE_SECRETKEY: i32 = 49;
#[cfg(feature = "curve")]
pub const CURVE_SERVERKEY: i32 = 50;

// Key Length Constants (from libsodium)
#[cfg(feature = "curve")]
pub const CURVE_KEY_LEN: usize = 32;

#[cfg(feature = "io-uring")]
pub const IO_URING_SNDZEROCOPY: i32 = 1170;
#[cfg(feature = "io-uring")]
pub const IO_URING_RCVMULTISHOT: i32 = 1171;

// Add more constants as needed...

/// Holds parsed and validated socket options.
#[derive(Debug, Clone)]
pub(crate) struct SocketOptions {
  // High Water Marks (applied to Pipes / internal queues)
  pub rcvhwm: usize,
  pub sndhwm: usize,
  // Timeouts: None = -1 (infinite), Some(ZERO) = 0 (immediate), Some(>0) = timeout
  pub rcvtimeo: Option<Duration>,
  pub sndtimeo: Option<Duration>,
  // Connection Behavior
  pub linger: Option<Duration>, // ZMQ uses -1 for infinite, 0 for immediate, >0 for ms -> map to Duration
  pub reconnect_ivl: Option<Duration>,     // Initial reconnect interval (None = ZMQ default, often 0 = no reconnect)
  pub reconnect_ivl_max: Option<Duration>, // Max reconnect interval (for exponential backoff)
  // pub backlog: Option<u32>, // TODO - For listener
  // Identity
  pub routing_id: Option<Blob>,
  pub socket_type_name: String, // e.g., "REQ", "REP" - needed for READY cmd
  // TCP Specific (mirrored for setting on stream)
  pub tcp_keepalive_enabled: i32, // ZMQ standard: -1 off, 0 system, 1 on
  pub tcp_keepalive_idle: Option<Duration>,
  pub tcp_keepalive_count: Option<u32>,
  pub tcp_keepalive_interval: Option<Duration>,
  pub tcp_nodelay: bool, // Usually enabled by default
  
  /// Interval between sending ZMTP PING probes if no traffic received.
  /// `None` disables PINGs.
  pub heartbeat_ivl: Option<Duration>,
  /// Time to wait for PONG reply before considering connection dead.
  /// `None` uses a default derived from `heartbeat_ivl`.
  pub heartbeat_timeout: Option<Duration>,

  /// ROUTER behavior when routing ID is unknown.
  /// Default false (drop message). True = return EHOSTUNREACH.
  pub router_mandatory: bool,
  
  pub zap_domain: Option<String>, // ZAP Domain
  // Add other commonly used options as needed
  pub plain_server: Option<bool>, // Role override
  pub plain_username: Option<String>, // Security options stored here?
  pub plain_password: Option<String>,
  // Curve Auth
  #[cfg(feature = "curve")]
  pub curve_server: Option<bool>, // Role override
  #[cfg(feature = "curve")]
  pub curve_public_key: Option<[u8; CURVE_KEY_LEN]>,
  #[cfg(feature = "curve")]
  pub curve_secret_key: Option<[u8; CURVE_KEY_LEN]>, // Note: Store securely!
  #[cfg(feature = "curve")]
  pub curve_server_key: Option<[u8; CURVE_KEY_LEN]>, // Client uses this
  // pub heartbeat_ttl: Option<Duration>, // TTL often derived from timeout
  pub io_uring_send_zerocopy: bool,
  pub io_uring_recv_multishot: bool,
}

impl Default for SocketOptions {
  fn default() -> Self {
    Self {
      // ZMQ Defaults:
      rcvhwm: 1000,
      sndhwm: 1000,
      rcvtimeo: None,               // -1 in ZMQ
      sndtimeo: None,               // -1 in ZMQ
      linger: Some(Duration::ZERO), // 0 in ZMQ (different from socket default!)
      reconnect_ivl: Some(Duration::from_millis(100)), // ZMQ default is 100ms
      reconnect_ivl_max: Some(Duration::ZERO), // ZMQ default is 0 (disable max/backoff)
      routing_id: None,
      socket_type_name: "UNKNOWN".to_string(), // Default, should be set on creation
      tcp_keepalive_enabled: 0,                // 0 (use system default) in ZMQ
      tcp_keepalive_idle: None,
      tcp_keepalive_count: None,
      tcp_keepalive_interval: None,
      tcp_nodelay: true, // Common default for messaging
      heartbeat_ivl: None, // Disabled by default
      heartbeat_timeout: None,
      router_mandatory: false, // Default ZMQ behavior is to drop silently
      zap_domain: None,
      plain_server: None,
      plain_username: None,
      plain_password: None,
      #[cfg(feature = "curve")]
      curve_server: None,
      #[cfg(feature = "curve")]
      curve_public_key: None,
      #[cfg(feature = "curve")]
      curve_secret_key: None,
      #[cfg(feature = "curve")]
      curve_server_key: None,
      io_uring_send_zerocopy: false,
      io_uring_recv_multishot: false,
    }
  }
}

/// Configuration passed to TCP Listener/Connecter for initial socket setup.
#[derive(Debug, Clone, Default)]
pub(crate) struct TcpTransportConfig {
  pub tcp_nodelay: bool,
  pub keepalive_time: Option<Duration>,
  pub keepalive_interval: Option<Duration>,
  pub keepalive_count: Option<u32>,
  // Add other options settable BEFORE connect/accept if needed (e.g., SO_REUSEADDR?)
}

// Config specific to TCP transport, potentially influenced by socket options
#[derive(Debug, Clone, Default)]
pub(crate) struct ZmtpEngineConfig {
  /// Identity to present in READY command (for Client role)
  pub routing_id: Option<Blob>,
  /// Socket type name to include in READY command
  pub socket_type_name: String,
  pub heartbeat_ivl: Option<Duration>,
  pub heartbeat_timeout: Option<Duration>,
  // Add security mechanism choice later if needed
  // pub security_mechanism: PlannedMechanismEnum,

  // io-uring specific options
  pub use_send_zerocopy: bool, // TODO: Get from SocketOptions
  pub use_recv_multishot: bool, // TODO: Get from SocketOptions
  #[cfg(feature = "io-uring")]
  pub use_cork: bool, // Example io-uring related option
}
// <<< ADDED TcpConfig STRUCT END >>>

// --- Helper functions for parsing option values ---
/// Parses a byte slice representing an integer option (like HWM, linger).
pub(crate) fn parse_i32_option(value: &[u8]) -> Result<i32, ZmqError> {
  let arr: [u8; 4] = value.try_into().map_err(|_| ZmqError::InvalidOptionValue(0))?; // Use generic error for now
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

/// Parses a byte slice representing a timeout or linger value in milliseconds.
/// ZMQ uses -1 for infinite, 0 for immediate/no-wait, >0 for duration.
/// Maps to Option<Duration>: None=-1, Some(ZERO)=0, Some(>0)=millis.
pub(crate) fn parse_timeout_option(value: &[u8], option_id: i32) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option_id))?;
  match val {
    -1 => Ok(None),                                     // Infinite timeout
    0 => Ok(Some(Duration::ZERO)),                      // Zero timeout (non-blocking indication)
    1.. => Ok(Some(Duration::from_millis(val as u64))), // Positive timeout
    _ => Err(ZmqError::InvalidOptionValue(option_id)),  // Other negative values invalid
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

/// Parses heartbeat interval/timeout values in milliseconds.
/// ZMQ uses 0 to disable. Negative is invalid.
pub(crate) fn parse_heartbeat_option(value: &[u8], option_id: i32) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option_id))?;
  match val {
    0 => Ok(None), // 0 disables heartbeat
    1.. => Ok(Some(Duration::from_millis(val as u64))), // Positive timeout
    _ => Err(ZmqError::InvalidOptionValue(option_id)), // Negative values invalid
  }
}

#[cfg(feature = "curve")]
/// Parses a fixed-length binary key option.
pub(crate) fn parse_key_option<const N: usize>(value: &[u8], option_id: i32) -> Result<[u8; N], ZmqError> {
  value.try_into().map_err(|_| {
      tracing::error!(option=option_id, expected_len=N, actual_len=value.len(), "Invalid key length");
      ZmqError::InvalidOptionValue(option_id)
  })
}

pub(crate) fn parse_reconnect_ivl_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value)?;
  match val {
      -1 => Ok(None), // Treat -1 as disable? ZMQ uses 0. Let's use 0.
      0 => Ok(None), // 0 disables reconnect according to ZMQ docs for IVL
      1.. => Ok(Some(Duration::from_millis(val as u64))),
      _ => Err(ZmqError::InvalidOptionValue(RECONNECT_IVL)),
  }
}

pub(crate) fn parse_reconnect_ivl_max_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value)?;
  match val {
      0 => Ok(Some(Duration::ZERO)), // 0 disables max/backoff according to ZMQ docs
      1.. => Ok(Some(Duration::from_millis(val as u64))),
      _ => Err(ZmqError::InvalidOptionValue(RECONNECT_IVL_MAX)),
  }
}