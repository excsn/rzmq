use std::io;
use thiserror::Error;
// use tokio::sync::{mpsc, oneshot}; // Add later when needed

#[derive(Error, Debug)]
#[non_exhaustive] // Allows adding more variants later without breaking change
pub enum ZmqError {
  // --- I/O Errors ---
  #[error("I/O error: {0}")]
  Io(#[from] io::Error), // Allows easy conversion from std::io::Error

  #[error("Invalid argument provided: {0}")]
  InvalidArgument(String), // Corresponds to EINVAL for non-option errors
  
  // --- Timeouts ---
  #[error("Operation timed out")]
  Timeout, // Corresponds to ETIMEDOUT

  // --- Connection/Binding Errors ---
  #[error("Address already in use: {0}")]
  AddrInUse(String), // Endpoint string EADDRINUSE
  #[error("Address not available: {0}")]
  AddrNotAvailable(String), // Endpoint string EADDRNOTAVAIL
  #[error("Connection refused by peer: {0}")]
  ConnectionRefused(String), // Endpoint string ECONNREFUSED
  #[error("Host is unreachable: {0}")]
  HostUnreachable(String), // Endpoint string EHOSTUNREACH
  #[error("Network is unreachable: {0}")]
  NetworkUnreachable(String), // Endpoint string ENETUNREACH
  #[error("Connection closed by peer or transport")]
  ConnectionClosed, // EPIPE, ECONNRESET?
  #[error("Permission denied for endpoint: {0}")]
  PermissionDenied(String), // EACCES, EPERM

  // --- Endpoint Errors ---
  #[error("Invalid endpoint format: {0}")]
  InvalidEndpoint(String), // EINVAL or custom
  #[error("Endpoint resolution failed: {0}")]
  EndpointResolutionFailed(String), // DNS or similar error

  // --- Option Errors ---
  #[error("Invalid socket option ID: {0}")]
  InvalidOption(i32), // EINVAL
  #[error("Invalid value provided for option ID {0}")]
  InvalidOptionValue(i32), // EINVAL

  // --- State Errors ---
  #[error("Operation is invalid for the socket type ({0})")]
  InvalidSocketType(&'static str), // Custom error
  #[error("Operation is invalid for the current socket state: {0}")]
  InvalidState(&'static str), // EFSM

  // --- Protocol Errors ---
  #[error("ZMTP protocol violation: {0}")]
  ProtocolViolation(String), // EPROTO
  
  #[error("Invalid message format for operation: {0}")]
  InvalidMessage(String),

  #[error("Security error: {0}")]
  SecurityError(String),
  
  // --- Security Errors ---
  #[error("Authentication failed: {0}")]
  AuthenticationFailure(String),
  #[error("Encryption/Decryption error: {0}")]
  EncryptionError(String),

  // --- Resource Limits ---
  #[error("Resource limit reached (e.g., HWM)")]
  ResourceLimitReached, // EAGAIN / EWOULDBLOCK equivalent

  // --- Unsupported ---
  #[error("Transport scheme not supported or enabled: {0}")]
  UnsupportedTransport(String), // EPROTONOSUPPORT
  #[error("Socket option not supported: {0}")]
  UnsupportedOption(i32), // ENOTSUP
  #[error("Feature not supported or enabled: {0}")]
  UnsupportedFeature(&'static str),

  // --- Internal Errors ---
  #[error("Internal library error: {0}")]
  Internal(String),
  // #[error("Internal actor mailbox send error: {0}")] // Add later
  // MailboxSendError(String),
  // #[error("Internal actor task join error")] // Add later
  // TaskJoinError, // Consider wrapping tokio::task::JoinError

  // --- Placeholder for Transport Specific ---
  // #[cfg(feature = "io-uring")]
  // #[error("io_uring error: {0}")]
  // Uring(#[from] io_uring::Error), // Map io_uring::Error later
}

// Helper function to map common std::io::Error kinds
impl ZmqError {
  pub fn from_io_endpoint(e: io::Error, endpoint: &str) -> Self {
    match e.kind() {
      io::ErrorKind::AddrInUse => ZmqError::AddrInUse(endpoint.to_string()),
      io::ErrorKind::AddrNotAvailable => ZmqError::AddrNotAvailable(endpoint.to_string()),
      io::ErrorKind::ConnectionRefused => ZmqError::ConnectionRefused(endpoint.to_string()),
      io::ErrorKind::PermissionDenied => ZmqError::PermissionDenied(endpoint.to_string()),
      io::ErrorKind::TimedOut => ZmqError::Timeout,
      io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe => ZmqError::ConnectionClosed,
      // TODO: Add more mappings like HostUnreachable etc. if std::io provides them directly
      _ => ZmqError::Io(e), // Default fallback
    }
  }
}
