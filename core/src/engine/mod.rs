// src/engine/mod.rs

pub(crate) mod core;
pub mod zmtp_tcp;
#[cfg(feature = "ipc")]
pub mod zmtp_ipc;
#[cfg(feature = "io-uring")]
mod uring_recv;

use crate::error::ZmqError;
use crate::message::Msg;
use crate::security::MechanismStatus; // Needs security module defined

use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// Defines the behavior of an Engine actor, handling protocol and transport I/O.
#[async_trait]
pub trait IEngine: Send + Sync + 'static {
  /// Sends a message over the transport.
  /// The Engine is responsible for ZMTP encoding and handling transport writes.
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError>;

  /// Initiates the ZMTP handshake process (greeting, security mechanism).
  /// This is typically called by the Session after the Engine is attached.
  async fn start_handshake(&self) -> Result<(), ZmqError>;

  // --- ZAP Interaction Callbacks (called by Session) ---

  /// Processes the reply received from the ZAP authenticator.
  /// Allows the security Mechanism within the Engine to proceed or fail.
  async fn process_zap_reply(&self, reply_frames: Vec<Vec<u8>>) -> Result<(), ZmqError>;

  // --- Status ---

  /// Returns the current status of the security mechanism being used.
  fn get_security_status(&self) -> MechanismStatus;
}

/// Trait alias for streams usable by ZMTP engines.
pub(crate) trait ZmtpStream: AsyncRead + AsyncWrite + Unpin + Send + std::fmt::Debug + 'static {}
// Implement the marker trait for Tokio's streams
impl ZmtpStream for tokio::net::TcpStream {}
#[cfg(feature = "ipc")]
impl ZmtpStream for tokio::net::UnixStream {}

#[cfg(feature = "io-uring")]
impl ZmtpStream for tokio_uring::net::TcpStream {}

// Optional: if you want io_uring for IPC as well (deferred for now)
// #[cfg(all(feature = "ipc", feature = "io-uring"))]
// impl ZmtpStream for tokio_uring::net::UnixStream {}