use super::ZmqError;
use crate::security::framer::ISecureFramer;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MechanismStatus {
  Initializing,   // Start state
  Handshaking,    // Tokens being exchanged
  Authenticating, // Waiting for ZAP reply
  Ready,          // Handshake successful
  Error,          // Handshake failed
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessTokenAction {
  /// No immediate action is required. The handler should wait for the next event.
  ContinueWaiting,
  /// The mechanism now has a token ready to send immediately.
  ProduceAndSend,
  /// The handshake is now complete.
  HandshakeComplete,
}

/// Trait for security mechanisms (NULL, PLAIN, etc.).
/// Drives the security handshake state machine.
pub(crate) trait Mechanism: Send + Sync + fmt::Debug + 'static {
  // Needs to be Send + Sync if held by Engine actor

  /// Processes an incoming ZMTP security token (part of handshake).
  /// Updates the internal state machine.
  /// Returns Ok(ProcessTokenAction) on success, Err(ZmqError::SecurityError) on failure.
  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError>;

  /// Produces the next ZMTP security token to be sent, based on current state.
  /// Returns None if no token needs to be sent currently (e.g., waiting for peer).
  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError>;

  /// Returns the current status of the mechanism handshake.
  fn status(&self) -> MechanismStatus;

  /// Returns true if the handshake completed successfully (status is Ready).
  fn is_complete(&self) -> bool {
    self.status() == MechanismStatus::Ready
  }

  /// Returns true if the handshake resulted in an error.
  fn is_error(&self) -> bool {
    self.status() == MechanismStatus::Error
  }

  /// Returns the reason for the error state, if available.
  fn error_reason(&self) -> Option<&str>;

  /// Called after handshake is Ready. If this mechanism provides data-phase encryption,
  /// it consumes itself and returns an ISecureFramer and the established peer identity.
  /// If it's a non-encrypting mechanism (like NULL), it can return an error or a
  /// specific indicator that no cipher is needed (engine then uses raw stream).
  /// For simplicity, let's have it always return a Result. Non-encrypting mechanisms
  /// would return a pass-through cipher.
  fn into_framer(
    self: Box<Self>,
    max_msg_size: i64,
    sndbatch_count: usize,
    sndbatch_bytes_physical: usize,
  ) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError>;
}
