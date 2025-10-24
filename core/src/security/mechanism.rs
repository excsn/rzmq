use super::{IDataCipher, ZmqError};
use crate::message::Metadata;
use std::fmt;

// Define MechanismStatus enum properly here
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
pub trait Mechanism: Send + Sync + fmt::Debug + 'static {
  // Needs to be Send + Sync if held by Engine actor

  /// Returns the ASCII name of the mechanism (e.g., "NULL", "PLAIN").
  fn name(&self) -> &'static str;

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

  /// Returns the identity of the peer, if established by the mechanism.
  /// For Non-PLAIN, this is the peer's public key. For PLAIN, potentially the User-Id.
  fn peer_identity(&self) -> Option<Vec<u8>>;

  /// Provides any additional metadata derived from the handshake (e.g., ZAP User-Id).
  /// This can be merged into outgoing/incoming messages.
  fn metadata(&self) -> Option<Metadata>;

  fn as_any(&self) -> &dyn std::any::Any;

  /// Sets the mechanism's internal state to Error.
  /// Called by the engine core when transport errors occur during handshake.
  fn set_error(&mut self, reason: String);

  /// Returns the reason for the error state, if available.
  fn error_reason(&self) -> Option<&str>;

  // --- ZAP Related Methods ---

  /// Checks if the mechanism currently requires a ZAP request to be sent.
  /// Returns the ZAP request frames if needed, otherwise None.
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>>;

  /// Processes the ZAP reply received from the authenticator.
  /// Updates the internal state machine based on the ZAP outcome.
  fn process_zap_reply(&mut self, reply_frames: &[Vec<u8>]) -> Result<(), ZmqError>;

  /// Called after handshake is Ready. If this mechanism provides data-phase encryption,
  /// it consumes itself and returns an IDataCipher and the established peer identity.
  /// If it's a non-encrypting mechanism (like NULL), it can return an error or a
  /// specific indicator that no cipher is needed (engine then uses raw stream).
  /// For simplicity, let's have it always return a Result. Non-encrypting mechanisms
  /// would return a pass-through cipher.
  fn into_data_cipher_parts(self: Box<Self>) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError>;
  
}
