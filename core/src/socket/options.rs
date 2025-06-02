use std::time::Duration;

use crate::{Blob, CoreState, ZmqError};

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
pub const LAST_ENDPOINT: i32 = 32;
pub const TCP_KEEPALIVE: i32 = 34;
pub const TCP_KEEPALIVE_IDLE: i32 = 35;
pub const TCP_KEEPALIVE_CNT: i32 = 36;
pub const TCP_KEEPALIVE_INTVL: i32 = 37;
pub const HEARTBEAT_IVL: i32 = 38; // ZMQ_HEARTBEAT_IVL
pub const HEARTBEAT_TIMEOUT: i32 = 39; // ZMQ_HEARTBEAT_TIMEOUT
pub const HEARTBEAT_TTL: i32 = 40; // ZMQ_HEARTBEAT_TTL (Often derived from TIMEOUT)
pub const HANDSHAKE_IVL: i32 = 41; // ZMQ_HANDSHAKE_IVL

pub const ROUTER_MANDATORY: i32 = 33;

// Security Options
pub const ZAP_DOMAIN: i32 = 55;
pub const PLAIN_SERVER: i32 = 44;
pub const PLAIN_USERNAME: i32 = 45;
pub const PLAIN_PASSWORD: i32 = 46;

// Security/Noise XX
pub const NOISE_XX_ENABLED: i32 = 1202; // Boolean (0 or 1)
pub const NOISE_XX_STATIC_SECRET_KEY: i32 = 1200; // Expects 32-byte secret key
pub const NOISE_XX_REMOTE_STATIC_PUBLIC_KEY: i32 = 1201; // Client uses this for server's PK, expects 32-byte public key
                                                         // Optional: For server, a list of allowed client public keys (if not using ZAP for this)
                                                         // pub const NOISE_XX_ALLOWED_PEERS: i32 = 1203; // Would take a list of PKs

pub const MAX_CONNECTIONS: i32 = 1000;

// IO Uring Options
#[cfg(feature = "io-uring")]
pub const IO_URING_SNDZEROCOPY: i32 = 1170;
#[cfg(feature = "io-uring")]
pub const IO_URING_RCVMULTISHOT: i32 = 1171;

/// Socket option: Enable/disable TCP_CORK (Linux only).
/// Value is i32 (0 or 1).
pub const TCP_CORK: i32 = 1172;

/// Socket option (i32): Number of buffers to use in the io_uring multishot receive pool.
/// Only effective if `IO_URING_RCVMULTISHOT` (the boolean flag) is also enabled.
/// Default: 16. Min: 1.
#[cfg(feature = "io-uring")]
pub const IO_URING_RECV_BUFFER_COUNT: i32 = 1173;

#[cfg(feature = "io-uring")]
pub const IO_URING_RECV_BUFFER_SIZE: i32 = 1174;

pub const IO_URING_SESSION_ENABLED: i32 = 1175;

#[cfg(feature = "io-uring")]
pub const IO_URING_SND_BUFFER_COUNT: i32 = 1176;
#[cfg(feature = "io-uring")]
pub const IO_URING_SND_BUFFER_SIZE: i32 = 1177;

pub const DEFAULT_RECONNECT_IVL_MS: u64 = 1000;

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
  pub reconnect_ivl: Option<Duration>, // Initial reconnect interval (None = ZMQ default, often 0 = no reconnect)
  pub reconnect_ivl_max: Option<Duration>, // Max reconnect interval (for exponential backoff)
  pub backlog: Option<u32>,
  // Identity
  pub routing_id: Option<Blob>,
  pub socket_type_name: String, // e.g., "REQ", "REP" - needed for READY cmd
  // TCP Specific (mirrored for setting on stream)
  pub tcp_keepalive_enabled: i32, // ZMQ standard: -1 off, 0 system, 1 on
  pub tcp_keepalive_idle: Option<Duration>,
  pub tcp_keepalive_count: Option<u32>,
  pub tcp_keepalive_interval: Option<Duration>,
  pub tcp_nodelay: bool, // Usually enabled by default
  pub max_connections: Option<usize>,

  /// Interval between sending ZMTP PING probes if no traffic received.
  /// `None` disables PINGs.
  pub heartbeat_ivl: Option<Duration>,
  /// Time to wait for PONG reply before considering connection dead.
  /// `None` uses a default derived from `heartbeat_ivl`.
  pub heartbeat_timeout: Option<Duration>,
  pub handshake_ivl: Option<Duration>,

  /// ROUTER behavior when routing ID is unknown.
  /// Default false (drop message). True = return EHOSTUNREACH.
  pub router_mandatory: bool,
  // Add other commonly used options as needed
  // pub heartbeat_ttl: Option<Duration>, // TTL often derived from timeout
  pub tcp_cork: bool,
  pub io_uring: IOURingSocketOptions,
  pub zap_domain: Option<String>, // ZAP Domain
  pub plain_options: PlainMechanismSocketOptions,
  #[cfg(feature = "noise_xx")]
  pub noise_xx_options: NoiseXxSocketOptions,
}

impl Default for SocketOptions {
  fn default() -> Self {
    Self {
      // ZMQ Defaults:
      rcvhwm: 256,
      sndhwm: 256,
      rcvtimeo: None,               // -1 in ZMQ
      sndtimeo: None,               // -1 in ZMQ
      linger: Some(Duration::ZERO), // 0 in ZMQ (different from socket default!)
      reconnect_ivl: Some(Duration::from_millis(DEFAULT_RECONNECT_IVL_MS)),
      reconnect_ivl_max: Some(Duration::ZERO), // ZMQ default is 0 (disable max/backoff)
      backlog: None,
      routing_id: None,
      socket_type_name: "UNKNOWN".to_string(), // Default, should be set on creation
      tcp_keepalive_enabled: 0,                // 0 (use system default) in ZMQ
      tcp_keepalive_idle: None,
      tcp_keepalive_count: None,
      tcp_keepalive_interval: None,
      tcp_nodelay: true, // Common default for messaging
      max_connections: Some(1024),
      heartbeat_ivl: None, // Disabled by default
      heartbeat_timeout: None,
      handshake_ivl: None,
      router_mandatory: false, // Default ZMQ behavior is to drop silently
      tcp_cork: false,
      io_uring: Default::default(),
      zap_domain: None,
      plain_options: Default::default(),
      #[cfg(feature = "noise_xx")]
      noise_xx_options: NoiseXxSocketOptions::default(),
    }
  }
}

#[derive(Debug, Clone)]
pub struct IOURingSocketOptions {
  pub send_zerocopy: bool,
  pub recv_multishot: bool,
  pub session_enabled: bool,
}

impl Default for IOURingSocketOptions {
  fn default() -> Self {
    Self {
      session_enabled: false,
      send_zerocopy: false,
      recv_multishot: false,
    }
  }
}

#[cfg(feature = "noise_xx")]
#[derive(Debug, Clone, Default)]
pub struct NoiseXxSocketOptions {
  pub enabled: bool,
  pub static_secret_key_bytes: Option<[u8; 32]>,
  pub remote_static_public_key_bytes: Option<[u8; 32]>,
  // pub allowed_peers: Option<Vec<[u8; 32]>>, // If you add this later
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

#[derive(Debug, Clone, Default)]
pub struct PlainMechanismSocketOptions {
  pub enabled: bool,
  pub server_role: Option<bool>,     // Role override
  pub username: Option<String>, // Security options stored here?
  pub password: Option<String>,
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
  pub handshake_timeout: Option<Duration>,
  // Add security mechanism choice later if needed
  // pub security_mechanism: PlannedMechanismEnum,

  // io-uring specific options
  pub use_send_zerocopy: bool,
  pub use_recv_multishot: bool,
  // TCP Corking
  pub use_cork: bool,
  #[cfg(feature = "noise_xx")]
  pub use_noise_xx: bool, // Derived from noise_xx_options.enabled
  #[cfg(feature = "noise_xx")]
  pub noise_xx_local_sk_bytes_for_engine: Option<[u8; 32]>, // Renamed for clarity
  #[cfg(feature = "noise_xx")]
  pub noise_xx_remote_pk_bytes_for_engine: Option<[u8; 32]>,
  pub use_plain: bool,
  pub plain_username_for_engine: Option<String>,
  pub plain_password_for_engine: Option<String>,
}

// --- Helper functions for parsing option values ---
/// Parses a byte slice representing an integer option (like HWM, linger).
pub(crate) fn parse_i32_option(value: &[u8]) -> Result<i32, ZmqError> {
  let arr: [u8; 4] = value.try_into().map_err(|_| ZmqError::InvalidOptionValue(0))?; // Use generic error for now

  Ok(i32::from_ne_bytes(arr)) // Assuming native endianness for socket options based on ZMQ C API usage
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
    0 => Ok(None),                                      // 0 disables heartbeat
    1.. => Ok(Some(Duration::from_millis(val as u64))), // Positive timeout
    _ => Err(ZmqError::InvalidOptionValue(option_id)),  // Negative values invalid
  }
}

pub(crate) fn parse_handshake_option(value: &[u8], option_id: i32) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option_id))?;
  match val {
    0 => Ok(None),
    1.. => Ok(Some(Duration::from_millis(val as u64))), // Positive timeout
    _ => Err(ZmqError::InvalidOptionValue(option_id)),  // Negative values invalid
  }
}

pub(crate) fn parse_reconnect_ivl_option(value: &[u8]) -> Result<Option<Duration>, ZmqError> {
  let val = parse_i32_option(value)?;
  match val {
    -1 => Ok(None), // Treat -1 as disable? ZMQ uses 0. Let's use 0.
    0 => Ok(None),  // 0 disables reconnect according to ZMQ docs for IVL
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

pub(crate) fn parse_max_connections_option(value: &[u8], option_id: i32) -> Result<Option<usize>, ZmqError> {
  let val = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option_id))?;
  match val {
    -1 => Ok(None),                                    // ZMQ often uses -1 for "no limit" or "system default"
    0 => Err(ZmqError::InvalidOptionValue(option_id)), // 0 is invalid for max connections
    1.. => Ok(Some(val as usize)),
    _ => Err(ZmqError::InvalidOptionValue(option_id)),
  }
}

/// Parses a fixed-length binary key option from a byte slice.
///
/// # Arguments
/// * `value`: The byte slice containing the key data.
/// * `option_id`: The integer ID of the socket option being parsed (for error reporting).
/// * `N`: A const generic representing the expected length of the key in bytes.
///
/// # Returns
/// `Ok([u8; N])` if the value has the correct length.
/// `Err(ZmqError::InvalidOptionValue)` if the value's length does not match `N`.
pub(crate) fn parse_key_option<const N: usize>(value: &[u8], option_id: i32) -> Result<[u8; N], ZmqError> {
  value.try_into().map_err(|_e| {
    // The error from try_into (TryFromSliceError) doesn't carry much info itself,
    // so we create our own ZmqError.
    tracing::error!(
      option_id = option_id,
      expected_len = N,
      actual_len = value.len(),
      "Invalid key length provided for socket option."
    );
    ZmqError::InvalidOptionValue(option_id) // Indicate which option had the invalid value
  })
}

pub(crate) fn parse_string_option(value: &[u8], option_id: i32) -> Result<String, ZmqError> {
  String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option_id))
}


// --- New Helper Functions for Applying/Retrieving Core Options ---

/// Applies a core-level socket option value to the `SocketOptions` struct.
/// This function centralizes the logic for parsing and setting options that
/// are managed by `SocketCore` or affect its underlying configuration.
/// Pattern-specific options (like SUBSCRIBE for SUB) are handled by `ISocket::set_pattern_option`.
pub(crate) fn apply_core_option_value(
    options: &mut SocketOptions, // Mutable reference to update
    option_id: i32,
    value: &[u8],
) -> Result<(), ZmqError> {
    tracing::debug!(option_id, value_len=value.len(), "Applying core socket option");
    match option_id {
        SNDHWM => options.sndhwm = parse_i32_option(value)?.max(0) as usize,
        RCVHWM => options.rcvhwm = parse_i32_option(value)?.max(0) as usize,
        LINGER => options.linger = parse_linger_option(value)?,
        ROUTING_ID => options.routing_id = Some(parse_blob_option(value)?),
        RECONNECT_IVL => options.reconnect_ivl = parse_reconnect_ivl_option(value)?,
        RECONNECT_IVL_MAX => options.reconnect_ivl_max = parse_reconnect_ivl_max_option(value)?,
        RCVTIMEO => options.rcvtimeo = parse_timeout_option(value, option_id)?,
        SNDTIMEO => options.sndtimeo = parse_timeout_option(value, option_id)?,
        TCP_KEEPALIVE => options.tcp_keepalive_enabled = parse_keepalive_mode_option(value)?,
        TCP_KEEPALIVE_IDLE => options.tcp_keepalive_idle = parse_secs_duration_option(value)?,
        TCP_KEEPALIVE_CNT => options.tcp_keepalive_count = parse_u32_option(value)?,
        TCP_KEEPALIVE_INTVL => options.tcp_keepalive_interval = parse_secs_duration_option(value)?,
        HEARTBEAT_IVL => options.heartbeat_ivl = parse_heartbeat_option(value, option_id)?,
        HEARTBEAT_TIMEOUT => options.heartbeat_timeout = parse_heartbeat_option(value, option_id)?,
        HANDSHAKE_IVL => options.handshake_ivl = parse_handshake_option(value, option_id)?,
        MAX_CONNECTIONS => options.max_connections = parse_max_connections_option(value, option_id)?,
        TCP_CORK => options.tcp_cork = parse_bool_option(value)?,
        ZAP_DOMAIN => options.zap_domain = Some(parse_string_option(value, option_id)?),
        PLAIN_SERVER => {
            options.plain_options.server_role = Some(parse_bool_option(value)?);
            options.plain_options.enabled = true; // Enable PLAIN if server role is set
        }
        PLAIN_USERNAME => {
            options.plain_options.username = Some(parse_string_option(value, option_id)?);
            options.plain_options.enabled = true;
        }
        PLAIN_PASSWORD => {
            options.plain_options.password = Some(parse_string_option(value, option_id)?);
            options.plain_options.enabled = true;
        }

        #[cfg(feature = "noise_xx")]
        NOISE_XX_ENABLED => options.noise_xx_options.enabled = parse_bool_option(value)?,
        #[cfg(feature = "noise_xx")]
        NOISE_XX_STATIC_SECRET_KEY => options.noise_xx_options.static_secret_key_bytes = Some(parse_key_option::<32>(value, option_id)?),
        #[cfg(feature = "noise_xx")]
        NOISE_XX_REMOTE_STATIC_PUBLIC_KEY => options.noise_xx_options.remote_static_public_key_bytes = Some(parse_key_option::<32>(value, option_id)?),

        #[cfg(feature = "io-uring")]
        IO_URING_SESSION_ENABLED => options.io_uring.session_enabled = parse_bool_option(value)?,

        #[cfg(feature = "io-uring")]
        IO_URING_SNDZEROCOPY => options.io_uring.send_zerocopy = parse_bool_option(value)?,

        #[cfg(feature = "io-uring")]
        IO_URING_RCVMULTISHOT => options.io_uring.recv_multishot = parse_bool_option(value)?,

        // Options handled by pattern logic (ISocket) or read-only, or not applicable for set_option
        SUBSCRIBE | UNSUBSCRIBE | LAST_ENDPOINT  /* Pattern specific */ | ROUTER_MANDATORY |
        16 /* ZMQ_TYPE (read-only) */ => return Err(ZmqError::UnsupportedOption(option_id)),

        _ => return Err(ZmqError::InvalidOption(option_id)), // Unknown option ID
    }
    Ok(())
}

/// Retrieves a core-level socket option value from the `SocketOptions` and `CoreState` structs.
pub(crate) fn retrieve_core_option_value(
    options: &SocketOptions,       // Read reference
    core_s_reader: &CoreState, // Read reference to CoreState for things like LAST_ENDPOINT
    option_id: i32,
) -> Result<Vec<u8>, ZmqError> {
    match option_id {
        SNDHWM => Ok((options.sndhwm as i32).to_ne_bytes().to_vec()),
        RCVHWM => Ok((options.rcvhwm as i32).to_ne_bytes().to_vec()),
        LINGER => Ok(options.linger.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
        ROUTING_ID => options.routing_id.as_ref().map(|b| b.to_vec()).ok_or(ZmqError::Internal("Option ROUTING_ID not set".into())),
        RECONNECT_IVL => Ok(options.reconnect_ivl.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()), // 0 if None
        RECONNECT_IVL_MAX => Ok(options.reconnect_ivl_max.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()), // 0 if None
        RCVTIMEO => Ok(options.rcvtimeo.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
        SNDTIMEO => Ok(options.sndtimeo.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
        LAST_ENDPOINT => Ok(core_s_reader.last_bound_endpoint.as_deref().unwrap_or("").as_bytes().to_vec()),
        TCP_KEEPALIVE => Ok(options.tcp_keepalive_enabled.to_ne_bytes().to_vec()),
        TCP_KEEPALIVE_IDLE => Ok(options.tcp_keepalive_idle.map_or(0, |d| d.as_secs() as i32).to_ne_bytes().to_vec()),
        TCP_KEEPALIVE_CNT => Ok(options.tcp_keepalive_count.map_or(0, |c| c as i32).to_ne_bytes().to_vec()),
        TCP_KEEPALIVE_INTVL => Ok(options.tcp_keepalive_interval.map_or(0, |d| d.as_secs() as i32).to_ne_bytes().to_vec()),
        HEARTBEAT_IVL => Ok(options.heartbeat_ivl.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
        HEARTBEAT_TIMEOUT => Ok(options.heartbeat_timeout.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
        HANDSHAKE_IVL => Ok(options.handshake_ivl.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
        MAX_CONNECTIONS => Ok(options.max_connections.map_or(-1, |v| v as i32).to_ne_bytes().to_vec()),
        TCP_CORK => Ok((options.tcp_cork as i32).to_ne_bytes().to_vec()),
        ZAP_DOMAIN => options.zap_domain.as_ref().map(|s| s.as_bytes().to_vec()).ok_or(ZmqError::Internal("Option ZAP_DOMAIN not set".into())),
        PLAIN_SERVER => options.plain_options.server_role.map(|b| (b as i32).to_ne_bytes().to_vec()).ok_or(ZmqError::Internal("Option PLAIN_SERVER not set".into())),
        PLAIN_USERNAME => options.plain_options.username.as_ref().map(|s| s.as_bytes().to_vec()).ok_or(ZmqError::Internal("Option PLAIN_USERNAME not set".into())),
        PLAIN_PASSWORD => Err(ZmqError::PermissionDenied("PLAIN_PASSWORD is write-only".into())),


        #[cfg(feature = "noise_xx")]
        NOISE_XX_ENABLED => Ok((options.noise_xx_options.enabled as i32).to_ne_bytes().to_vec()),
        #[cfg(feature = "noise_xx")]
        NOISE_XX_STATIC_SECRET_KEY => Err(ZmqError::PermissionDenied("NOISE_XX_STATIC_SECRET_KEY is write-only".into())),
        #[cfg(feature = "noise_xx")]
        NOISE_XX_REMOTE_STATIC_PUBLIC_KEY => options.noise_xx_options.remote_static_public_key_bytes.map(|k| k.to_vec()).ok_or(ZmqError::Internal("Option NOISE_XX_REMOTE_STATIC_PUBLIC_KEY not set".into())),

        #[cfg(feature = "io-uring")]
        IO_URING_SESSION_ENABLED => Ok((options.io_uring.session_enabled as i32).to_ne_bytes().to_vec()),
        
        #[cfg(feature = "io-uring")]
        IO_URING_SNDZEROCOPY => Ok((options.io_uring.send_zerocopy as i32).to_ne_bytes().to_vec()),

        #[cfg(feature = "io-uring")]
        IO_URING_RCVMULTISHOT => Ok((options.io_uring.recv_multishot as i32).to_ne_bytes().to_vec()),

        // Options handled by pattern logic or read-only by nature
        16 /* ZMQ_TYPE */ => Ok((core_s_reader.socket_type as i32).to_ne_bytes().to_vec()),
        SUBSCRIBE | UNSUBSCRIBE | ROUTER_MANDATORY => Err(ZmqError::UnsupportedOption(option_id)), // Pattern specific

        _ => Err(ZmqError::InvalidOption(option_id)),
    }
}