use std::collections::HashMap;

use crate::error::ZmqError;
use crate::security::curve::framer::CurveMessageFramer;
use crate::security::curve::handshake::{CurveHandshake, CurveHandshakePhase};
use crate::security::framer::ISecureFramer;
use crate::security::mechanism::{Mechanism, MechanismStatus, ProcessTokenAction};

#[derive(Debug)]
pub(crate) struct CurveMechanism {
  pub(crate) handshake: CurveHandshake,
  error: Option<ZmqError>,
  error_reason: Option<String>,
}

impl CurveMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
  pub const NAME: &'static str = "CURVE";

  pub(crate) fn new(handshake: CurveHandshake) -> Self {
    Self {
      handshake,
      error: None,
      error_reason: None,
    }
  }

  fn fail(&mut self, error: ZmqError) -> ZmqError {
    tracing::error!(mechanism = Self::NAME, %error, "Handshake error");
    self.error_reason = Some(error.to_string());
    self.error = Some(error.clone());
    self.handshake.phase = CurveHandshakePhase::Error;
    error
  }
}

impl Mechanism for CurveMechanism {
  fn status(&self) -> MechanismStatus {
    match self.handshake.phase {
      CurveHandshakePhase::Complete => MechanismStatus::Ready,
      CurveHandshakePhase::Error => MechanismStatus::Error,
      _ => MechanismStatus::Handshaking,
    }
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    let built = match self.handshake.phase {
      CurveHandshakePhase::ClientSendHello => self.handshake.build_client_hello(),
      CurveHandshakePhase::ServerSendWelcome => self.handshake.build_server_welcome(),
      CurveHandshakePhase::ClientSendInitiate => self.handshake.build_client_initiate(),
      CurveHandshakePhase::ServerSendReady => self.handshake.build_server_ready(),
      _ => return Ok(None),
    };
    match built {
      Ok(token) => Ok(Some(token)),
      Err(e) => Err(self.fail(e)),
    }
  }

  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    if !self.handshake.is_server && token.starts_with(b"\x05ERROR") {
      let error = self.handshake.process_error(token);
      return Err(self.fail(error));
    }
    let outcome = match self.handshake.phase {
      CurveHandshakePhase::ServerExpectHello => self
        .handshake
        .process_client_hello(token)
        .map(|_| ProcessTokenAction::ProduceAndSend),
      CurveHandshakePhase::ClientExpectWelcome => self
        .handshake
        .process_server_welcome(token)
        .map(|_| ProcessTokenAction::ProduceAndSend),
      CurveHandshakePhase::ServerExpectInitiate => self
        .handshake
        .process_client_initiate(token)
        .map(|_| ProcessTokenAction::ProduceAndSend),
      CurveHandshakePhase::ClientExpectReady => self
        .handshake
        .process_server_ready(token)
        .map(|_| ProcessTokenAction::HandshakeComplete),
      _ => Err(ZmqError::SecurityError("Unexpected CURVE command for the current state".into())),
    };
    outcome.map_err(|e| self.fail(e))
  }

  fn error_reason(&self) -> Option<&str> {
    self.error_reason.as_deref()
  }

  fn error(&self) -> Option<ZmqError> {
    self.error.clone()
  }

  fn peer_metadata(&self) -> Option<&HashMap<String, Vec<u8>>> {
    self.handshake.peer_metadata.as_ref()
  }

  fn into_framer(
    self: Box<Self>,
    max_msg_size: i64,
    sndbatch_count: usize,
    sndbatch_bytes_physical: usize,
  ) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError> {
    if self.status() != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState("Curve handshake is not complete."));
    }
    let is_server = self.handshake.is_server;
    let (precom, cn_nonce, cn_peer_nonce) = self.handshake.into_session()?;
    let framer = CurveMessageFramer::new(
      is_server,
      precom,
      cn_nonce,
      cn_peer_nonce,
      max_msg_size,
      sndbatch_count,
      sndbatch_bytes_physical,
    );
    Ok((Box::new(framer), None))
  }
}
