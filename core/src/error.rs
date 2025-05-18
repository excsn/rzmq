// src/error.rs

use std::io;
use thiserror::Error;
// tokio::sync::mpsc and oneshot are not directly used for error definitions themselves,
// but errors related to them (like SendError) might be wrapped or lead to ZmqError.
// For now, no direct import is needed here.

/// Defines the custom error type for the rzmq library.
/// This enum covers various categories of errors that can occur,
/// from I/O issues to protocol violations and internal problems.
///
/// It derives `Clone` to allow `SystemEvent`s carrying errors (like `ActorStopping`
/// or `ConnectionAttemptFailed`) to also be `Clone` for use with `tokio::sync::broadcast`.
/// To achieve this, variants holding non-Clone types like `std::io::Error`
/// store essential information (kind, message string) instead of the original error.
#[derive(Error, Debug, Clone)] // Added Clone for use in clonable SystemEvents.
#[non_exhaustive] // Indicates that more error variants may be added in the future without breaking API.
pub enum ZmqError {
  // --- I/O Errors ---
  /// Wraps an underlying I/O error, storing its kind and a descriptive message.
  /// This is used instead of `#[from] io::Error` to maintain `Clone` capability.
  #[error("I/O error: {kind} - {message}")]
  IoError {
    kind: io::ErrorKind, // The kind of I/O error (e.g., NotFound, PermissionDenied).
    message: String,     // The error message string from the original io::Error.
  },

  /// Indicates an invalid argument was provided to a function or method,
  /// not related to socket options (which have their own specific errors).
  /// Corresponds to POSIX `EINVAL` for non-option related issues.
  #[error("Invalid argument provided: {0}")]
  InvalidArgument(String),

  // --- Timeouts ---
  /// Signals that an operation timed out before completion.
  /// Corresponds to POSIX `ETIMEDOUT`.
  #[error("Operation timed out")]
  Timeout,

  // --- Connection/Binding Errors (often mapping to OS-level network errors) ---
  /// The requested address (endpoint) is already in use.
  /// Corresponds to POSIX `EADDRINUSE`.
  #[error("Address already in use: {0}")]
  AddrInUse(String), // Argument is the endpoint string.
  /// The requested address is not available on the local machine or cannot be assigned.
  /// Corresponds to POSIX `EADDRNOTAVAIL`.
  #[error("Address not available: {0}")]
  AddrNotAvailable(String),
  /// The connection was refused by the peer.
  /// Corresponds to POSIX `ECONNREFUSED`.
  #[error("Connection refused by peer: {0}")]
  ConnectionRefused(String),
  /// The destination host is unreachable.
  /// Corresponds to POSIX `EHOSTUNREACH`.
  #[error("Host is unreachable: {0}")]
  HostUnreachable(String),
  /// The network is unreachable.
  /// Corresponds to POSIX `ENETUNREACH`.
  #[error("Network is unreachable: {0}")]
  NetworkUnreachable(String),
  /// The connection was closed by the peer or the underlying transport unexpectedly.
  /// Can map to `EPIPE`, `ECONNRESET`.
  #[error("Connection closed by peer or transport")]
  ConnectionClosed,
  /// Permission was denied for the requested operation on an endpoint (e.g., binding to a privileged port).
  /// Corresponds to POSIX `EACCES` or `EPERM`.
  #[error("Permission denied for endpoint: {0}")]
  PermissionDenied(String),

  // --- Endpoint Errors (related to parsing or resolving endpoint strings) ---
  /// The provided endpoint string has an invalid format (e.g., missing "://", bad address part).
  #[error("Invalid endpoint format: {0}")]
  InvalidEndpoint(String),
  /// Failed to resolve the endpoint (e.g., DNS lookup failure for a hostname).
  #[error("Endpoint resolution failed: {0}")]
  EndpointResolutionFailed(String),

  // --- Socket Option Errors ---
  /// An invalid socket option ID was provided to `set_option` or `get_option`.
  /// Corresponds to POSIX `EINVAL` for option errors.
  #[error("Invalid socket option ID: {0}")]
  InvalidOption(i32), // Argument is the integer option ID.
  /// An invalid value was provided for a valid socket option ID.
  /// Corresponds to POSIX `EINVAL`.
  #[error("Invalid value provided for option ID {0}")]
  InvalidOptionValue(i32), // Argument is the integer option ID.

  // --- State Errors (operation not valid for current socket type or state) ---
  /// The attempted operation is not valid for the type of the socket
  /// (e.g., trying to `recv` on a PUB socket).
  #[error("Operation is invalid for the socket type ({0})")]
  InvalidSocketType(&'static str), // Argument is the socket type name.
  /// The attempted operation is not valid for the current internal state of the socket
  /// (e.g., a REQ socket trying to `send` twice without a `recv`).
  /// Corresponds to ZMQ's `EFSM` (Error due to Finite State Machine).
  #[error("Operation is invalid for the current socket state: {0}")]
  InvalidState(&'static str), // Argument describes the state violation.

  // --- Protocol Errors (related to ZMTP) ---
  /// A violation of the ZMTP (ZeroMQ Message Transport Protocol) specification was detected.
  /// Corresponds to POSIX `EPROTO`.
  #[error("ZMTP protocol violation: {0}")]
  ProtocolViolation(String), // Argument describes the specific violation.
  /// A message was received or is being sent that has an invalid format for the
  /// current operation or socket pattern (e.g., ROUTER expecting identity frame first).
  #[error("Invalid message format for operation: {0}")]
  InvalidMessage(String),
  /// A generic error related to security mechanism processing during ZMTP handshake or data transfer.
  #[error("Security error: {0}")]
  SecurityError(String),

  // --- Security Errors (more specific security issues) ---
  /// Authentication with the peer failed (e.g., ZAP failure, incorrect PLAIN credentials).
  #[error("Authentication failed: {0}")]
  AuthenticationFailure(String),
  /// An error occurred during encryption or decryption of messages (e.g., with CURVE).
  #[error("Encryption/Decryption error: {0}")]
  EncryptionError(String),

  // --- Resource Limits ---
  /// A resource limit was reached, preventing the operation from completing immediately.
  /// This is often used for non-blocking operations that would otherwise block,
  /// such as sending when HWM is met and SNDTIMEO is 0.
  /// Corresponds to POSIX `EAGAIN` / `EWOULDBLOCK`.
  #[error("Resource limit reached (e.g., HWM)")]
  ResourceLimitReached,

  // --- Unsupported Features/Operations ---
  /// The requested transport scheme (e.g., "udp://", "tipc://") is not supported or enabled.
  /// Corresponds to POSIX `EPROTONOSUPPORT`.
  #[error("Transport scheme not supported or enabled: {0}")]
  UnsupportedTransport(String),
  /// The specified socket option is not supported by this rzmq implementation or socket type.
  /// Corresponds to POSIX `ENOTSUP`.
  #[error("Socket option not supported: {0}")]
  UnsupportedOption(i32),
  /// A requested feature (not necessarily a socket option or transport) is not supported or enabled.
  #[error("Feature not supported or enabled: {0}")]
  UnsupportedFeature(&'static str),

  // --- Internal Errors (indicating bugs or unexpected conditions within rzmq) ---
  /// A generic internal error within the rzmq library.
  /// This usually indicates a logic error or an unexpected state that should not occur.
  #[error("Internal library error: {0}")]
  Internal(String),
  // MailboxSendError and TaskJoinError variants could be added later if more specific
  // internal error reporting for actor communication or task management is needed.
  // e.g., #[error("Internal actor mailbox send error: {0}")] MailboxSendError(String),
  // e.g., #[error("Internal actor task join error")] TaskJoinError,

  // Placeholder for transport-specific errors if features like io_uring are added
  // and expose their own error types that need to be wrapped.
  // #[cfg(feature = "io-uring")]
  // #[error("io_uring error: {0}")]
  // Uring( /* io_uring::Error or its string representation */ ),
}

/// Manual implementation of `From<std::io::Error>` for `ZmqError`.
/// This is necessary because `std::io::Error` is not `Clone`, so we cannot use
/// `#[from] io::Error` directly in the `ZmqError` enum if we want `ZmqError` to be `Clone`.
/// Instead, we extract the `io::ErrorKind` and the error message string.
impl From<io::Error> for ZmqError {
  fn from(e: io::Error) -> Self {
    ZmqError::IoError {
      kind: e.kind(),        // Store theErrorKind.
      message: e.to_string(), // Store the String representation of the error.
    }
  }
}

impl ZmqError {
  /// Helper function to map common `std::io::Error` kinds encountered during
  /// endpoint operations (bind, connect) to more specific `ZmqError` variants.
  ///
  /// # Arguments
  /// * `e` - The `std::io::Error` to map.
  /// * `endpoint` - The endpoint string associated with the I/O operation, for context in error messages.
  pub fn from_io_endpoint(e: io::Error, endpoint: &str) -> Self {
    match e.kind() {
      io::ErrorKind::AddrInUse => ZmqError::AddrInUse(endpoint.to_string()),
      io::ErrorKind::AddrNotAvailable => ZmqError::AddrNotAvailable(endpoint.to_string()),
      io::ErrorKind::ConnectionRefused => ZmqError::ConnectionRefused(endpoint.to_string()),
      io::ErrorKind::PermissionDenied => ZmqError::PermissionDenied(endpoint.to_string()),
      io::ErrorKind::TimedOut => ZmqError::Timeout, // Map I/O timeout to ZMQ timeout.
      // EPIPE (BrokenPipe) and ECONNRESET (ConnectionReset) often mean the connection is gone.
      io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => ZmqError::ConnectionClosed,
      // Other io::ErrorKinds that might have ZMQ equivalents (e.g., HostUnreachable, NetworkUnreachable)
      // are not always directly represented by io::ErrorKind variants consistently across platforms.
      // For other kinds, fall back to the generic ZmqError::from(io::Error) conversion.
      _ => ZmqError::from(e), // Use the From<io::Error> impl for other cases.
    }
  }
}

/// A type alias for `std::result::Result<T, ZmqError>`.
/// This is the standard result type used for operations that can fail within the rzmq library.
pub type ZmqResult<T, E = ZmqError> = std::result::Result<T, E>;