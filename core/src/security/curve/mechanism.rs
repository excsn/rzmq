use async_trait::async_trait;
use dryoc::types::Bytes;

use crate::error::ZmqError;
use crate::security::curve::handshake::{CurveHandshake, CurveHandshakePhase};
use crate::security::framer::{ISecureFramer, LengthPrefixedFramer};
use crate::security::mechanism::ProcessTokenAction;
use crate::security::{IDataCipher, Mechanism, MechanismStatus, Metadata};

/// The public-facing wrapper for the CurveZMQ security mechanism.
/// This struct implements the `Mechanism` trait and holds the underlying handshake state machine.
#[derive(Debug)]
pub(crate) struct CurveMechanism {
  pub(crate) handshake: CurveHandshake,
  pub(crate) status: MechanismStatus,
  pub(crate) error_reason: Option<String>,
}

impl CurveMechanism {
  pub const NAME: &'static str = "CURVE";
  // ZMTP mechanism names are 20 bytes, padded with nulls.
  pub const NAME_BYTES: &'static [u8; 20] = b"CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
}

#[async_trait]
impl Mechanism for CurveMechanism {
  fn name(&self) -> &'static str {
    Self::NAME
  }

  fn status(&self) -> MechanismStatus {
    self.status
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    match self.handshake.phase {
      CurveHandshakePhase::ClientStart => {
        let hello_command_body = self.handshake.build_client_hello()?;
        self.handshake.phase = CurveHandshakePhase::ClientSentHello;
        Ok(Some(hello_command_body))
      }
      CurveHandshakePhase::ServerReadyToProduceWelcome => {
        let welcome_command_body = self.handshake.build_server_welcome()?;
        self.handshake.phase = CurveHandshakePhase::ServerAwaitingInitiate;
        Ok(Some(welcome_command_body))
      }
      CurveHandshakePhase::ClientReadyToProduceInitiate => {
        let initiate_command_body = self.handshake.build_client_initiate()?;
        // After sending INITIATE, the client handshake is complete from its perspective.
        self.handshake.phase = CurveHandshakePhase::Complete;
        self.status = MechanismStatus::Ready;
        Ok(Some(initiate_command_body))
      }
      // In all other states, we are waiting for the peer.
      _ => Ok(None),
    }
  }

  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    match self.handshake.phase {
      CurveHandshakePhase::ServerStart => {
        self.handshake.process_client_hello(token)?;
        self.handshake.phase = CurveHandshakePhase::ServerReadyToProduceWelcome;
        Ok(ProcessTokenAction::ProduceAndSend)
      }
      CurveHandshakePhase::ClientSentHello => {
        self.handshake.process_server_welcome(token)?;
        self.handshake.phase = CurveHandshakePhase::ClientReadyToProduceInitiate;
        Ok(ProcessTokenAction::ProduceAndSend)
      }
      CurveHandshakePhase::ServerAwaitingInitiate => {
        self.handshake.process_client_initiate(token)?;
        self.handshake.phase = CurveHandshakePhase::Complete;
        self.status = MechanismStatus::Ready;
        Ok(ProcessTokenAction::HandshakeComplete)
      }
      _ => Err(ZmqError::InvalidState(
        "Received handshake token at an unexpected time.",
      )),
    }
  }

  fn into_framer(self: Box<Self>) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError> {
    if self.status != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState("Curve handshake is not complete."));
    }

    let peer_identity = self
      .handshake
      .remote_static_public_key
      .as_ref()
      .map(|pk| pk.as_slice().to_vec());

    // 1. Get the pure cipher from the completed handshake
    let cipher = self.handshake.into_data_cipher()?;

    // 2. Wrap the cipher in the LengthPrefixedFramer
    let framer = Box::new(LengthPrefixedFramer::new(cipher));

    Ok((framer, peer_identity))
  }

  fn set_error(&mut self, reason: String) {
    if self.status != MechanismStatus::Error {
      self.status = MechanismStatus::Error;
      self.error_reason = Some(reason);
      self.handshake.phase = CurveHandshakePhase::Error;
    }
  }

  fn error_reason(&self) -> Option<&str> {
    self.error_reason.as_deref()
  }

  fn peer_identity(&self) -> Option<Vec<u8>> {
    self
      .handshake
      .remote_static_public_key
      .as_ref()
      .map(|pk| pk.as_slice().to_vec())
  }

  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    None
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    Ok(())
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }

  fn metadata(&self) -> Option<Metadata> {
    None
  }
}
