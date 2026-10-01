use crate::{
  security::{
    framer::{ISecureFramer, NullFramer},
    mechanism::ProcessTokenAction,
  },
  ZmqError,
};

use super::{Mechanism, MechanismStatus};

#[derive(Debug)]
pub struct NullMechanism;

impl NullMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"; // Padded
  pub const NAME: &'static str = "NULL";
}

impl Mechanism for NullMechanism {
  fn process_token(&mut self, _token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    // NULL mechanism does nothing with tokens and is always ready.
    Ok(ProcessTokenAction::HandshakeComplete)
  }
  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    Ok(None)
  }
  fn status(&self) -> MechanismStatus {
    MechanismStatus::Ready
  } // Null is always ready

  fn error_reason(&self) -> Option<&str> {
    None // No error state stored
  }

  fn into_framer(
    self: Box<Self>,
    max_msg_size: i64,
    sndbatch_count: usize,
    sndbatch_bytes_physical: usize,
  ) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError> {
    Ok((Box::new(NullFramer::new(max_msg_size, sndbatch_count, sndbatch_bytes_physical)), None))
  }
}
