// src/security/mod.rs

pub mod mechanism;
pub mod zap;

// pub use mechanism::{MechanismStatus, NullMechanism, PlainMechanism, CurveMechanism}; // Export concrete impls
// pub use zap::ZapClient;

use crate::error::ZmqError;
use crate::message::Metadata;

/// Trait for security mechanisms (NULL, PLAIN, CURVE).
/// Drives the security handshake state machine.
pub trait Mechanism: Send + Sync + 'static {
  // Needs to be Send + Sync if held by Engine actor

  /// Returns the ASCII name of the mechanism (e.g., "NULL", "PLAIN").
  fn name(&self) -> &'static str;

  /// Processes an incoming ZMTP security token (part of handshake).
  /// Updates the internal state machine.
  /// Returns Ok(()) on success, Err(ZmqError::SecurityError) on failure.
  fn process_token(&mut self, token: &[u8]) -> Result<(), ZmqError>;

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
  /// For CURVE, this is the peer's public key. For PLAIN, potentially the User-Id.
  fn peer_identity(&self) -> Option<Vec<u8>>;

  /// Provides any additional metadata derived from the handshake (e.g., ZAP User-Id).
  /// This can be merged into outgoing/incoming messages.
  fn metadata(&self) -> Option<Metadata>;

  // --- ZAP Related Methods ---

  /// Checks if the mechanism currently requires a ZAP request to be sent.
  /// Returns the ZAP request frames if needed, otherwise None.
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>>;

  /// Processes the ZAP reply received from the authenticator.
  /// Updates the internal state machine based on the ZAP outcome.
  fn process_zap_reply(&mut self, reply_frames: &[Vec<u8>]) -> Result<(), ZmqError>;
}

// Define MechanismStatus enum properly here
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MechanismStatus {
  Initializing,   // Start state
  Handshaking,    // Tokens being exchanged
  Authenticating, // Waiting for ZAP reply
  Ready,          // Handshake successful
  Error,          // Handshake failed
}

// Define these properly in mechanism.rs later
pub struct NullMechanism;
impl NullMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"; // Padded
  pub const NAME: &'static str = "NULL";
}

impl Mechanism for NullMechanism {
  fn name(&self) -> &'static str {
    Self::NAME
  }

  fn process_token(&mut self, _token: &[u8]) -> Result<(), ZmqError> {
    Ok(())
  }
  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    Ok(None)
  }
  fn status(&self) -> MechanismStatus {
    MechanismStatus::Ready
  } // Null is always ready
  fn peer_identity(&self) -> Option<Vec<u8>> {
    None
  }
  fn metadata(&self) -> Option<Metadata> {
    None
  }
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    None
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    Ok(())
  }
}
pub struct PlainMechanism; // Placeholder
impl PlainMechanism { pub const NAME: &'static str = "PLAIN"; }
impl Mechanism for PlainMechanism {
  fn name(&self) -> &'static str { Self::NAME }
  
  /* Placeholder impls */
  fn process_token(&mut self, _token: &[u8]) -> Result<(), ZmqError> {
    unimplemented!()
  }
  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    unimplemented!()
  }
  fn status(&self) -> MechanismStatus {
    unimplemented!()
  }
  fn peer_identity(&self) -> Option<Vec<u8>> {
    unimplemented!()
  }
  fn metadata(&self) -> Option<Metadata> {
    unimplemented!()
  }
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    unimplemented!()
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    unimplemented!()
  }
}
pub struct CurveMechanism; // Placeholder
impl CurveMechanism { pub const NAME: &'static str = "CURVE"; }
impl Mechanism for CurveMechanism {
  fn name(&self) -> &'static str { Self::NAME }

  /* Placeholder impls */
  fn process_token(&mut self, _token: &[u8]) -> Result<(), ZmqError> {
    unimplemented!()
  }
  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    unimplemented!()
  }
  fn status(&self) -> MechanismStatus {
    unimplemented!()
  }
  fn peer_identity(&self) -> Option<Vec<u8>> {
    unimplemented!()
  }
  fn metadata(&self) -> Option<Metadata> {
    unimplemented!()
  }
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    unimplemented!()
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    unimplemented!()
  }
}

// Define properly in zap.rs later
#[derive(Debug, Clone)]
pub struct ZapClient;
