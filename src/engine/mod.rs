// src/engine/mod.rs

pub mod zmtp_tcp;
#[cfg(feature = "ipc")]
pub mod zmtp_ipc;

use crate::error::ZmqError;
use crate::message::Msg;
use crate::security::MechanismStatus; // Needs security module defined
use async_trait::async_trait;

pub(crate) use zmtp_tcp::ZmtpTcpEngine;

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