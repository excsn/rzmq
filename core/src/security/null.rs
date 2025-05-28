use crate::{Metadata, ZmqError};

use super::{cipher::PassThroughDataCipher, IDataCipher, Mechanism, MechanismStatus};

#[derive(Debug)]
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

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }

  fn set_error(&mut self, _reason: String) {
    // Null mechanism doesn't really have a failure mode during handshake itself.
    // If the engine calls this due to transport error, there's no state to change here.
  }
  fn error_reason(&self) -> Option<&str> {
    None // No error state stored
  }

  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    None
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    Ok(())
  }

  fn into_data_cipher_parts(self: Box<Self>) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError> {
    Ok((Box::new(PassThroughDataCipher::default()), None))
  }
}