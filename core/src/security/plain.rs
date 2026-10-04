use std::collections::HashMap;

use bytes::{Buf, BufMut, BytesMut};

use crate::{
  protocol::zmtp::ZmtpReady,
  security::{
    framer::{ISecureFramer, NullFramer},
    mechanism::ProcessTokenAction,
  },
  ZmqError,
};

use super::{Mechanism, MechanismStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainState {
  ClientSendHello,
  ClientExpectWelcome,
  ClientSendInitiate,
  ClientExpectReady,
  ServerExpectHello,
  ServerSendWelcome,
  ServerExpectInitiate,
  ServerSendReady,
  ServerSendError,
  Ready,
  Error,
}

/// ZMTP PLAIN (RFC 27): HELLO, WELCOME, INITIATE, READY, with ERROR from the server.
/// The peers' metadata travels in INITIATE and READY, so the engine runs no READY
/// exchange of its own after this mechanism.
#[derive(Debug)]
pub struct PlainMechanism {
  is_server: bool,
  state: PlainState,
  username: Option<Vec<u8>>,
  password: Option<Vec<u8>>,
  expected_username: Option<Vec<u8>>,
  expected_password: Option<Vec<u8>>,
  local_metadata: HashMap<String, Vec<u8>>,
  peer_metadata: Option<HashMap<String, Vec<u8>>>,
  error: Option<ZmqError>,
  error_reason: Option<String>,
}

impl PlainMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
  pub const NAME: &'static str = "PLAIN";
  const HELLO: &'static [u8] = b"\x05HELLO";
  const WELCOME: &'static [u8] = b"\x07WELCOME";
  const INITIATE: &'static [u8] = b"\x08INITIATE";
  const READY: &'static [u8] = b"\x05READY";
  const ERROR: &'static [u8] = b"\x05ERROR";
  /// libzmq spells its PLAIN ERROR prefix as the C string "\x05ERROR", which the
  /// compiler reads as 0x5E "RROR" (the hex escape swallows the E). libzmq through
  /// 4.3.6 sends this form and recognizes only this form, so it is what goes out.
  const ERROR_LIBZMQ: &'static [u8] = b"\x5eRROR";
  /// ZAP status code libzmq reports as an authentication failure.
  const ERROR_REASON_AUTH: &'static [u8] = b"400";
  const AUTH_FAILURE: &'static str = "Invalid username or password";

  pub fn new(is_server: bool) -> Self {
    Self {
      is_server,
      state: if is_server {
        PlainState::ServerExpectHello
      } else {
        PlainState::ClientSendHello
      },
      username: None,
      password: None,
      expected_username: None,
      expected_password: None,
      local_metadata: HashMap::new(),
      peer_metadata: None,
      error: None,
      error_reason: None,
    }
  }

  pub fn set_client_credentials(&mut self, username: Option<Vec<u8>>, password: Option<Vec<u8>>) {
    if !self.is_server {
      self.username = username;
      self.password = password;
    }
  }

  pub fn set_server_expected_credentials(
    &mut self,
    username: Option<Vec<u8>>,
    password: Option<Vec<u8>>,
  ) {
    if self.is_server {
      self.expected_username = username;
      self.expected_password = password;
    }
  }

  /// Properties sent to the peer in INITIATE (client) or READY (server).
  pub fn set_local_metadata(&mut self, metadata: HashMap<String, Vec<u8>>) {
    self.local_metadata = metadata;
  }

  /// Splits a command frame body into its length-prefixed name and the rest,
  /// accepting libzmq's malformed ERROR prefix.
  fn split_command(token: &[u8]) -> Option<(&[u8], &[u8])> {
    if token.starts_with(Self::ERROR_LIBZMQ) {
      return Some((Self::ERROR, &token[Self::ERROR_LIBZMQ.len()..]));
    }
    let name_len = *token.first()? as usize;
    if token.len() < 1 + name_len {
      return None;
    }
    Some(token.split_at(1 + name_len))
  }

  /// HELLO body: username-len(1) username password-len(1) password.
  fn parse_hello_body(body: &[u8]) -> Result<(Vec<u8>, Vec<u8>), ZmqError> {
    let malformed = || ZmqError::SecurityError("Malformed PLAIN HELLO".into());
    let mut cursor = std::io::Cursor::new(body);
    if cursor.remaining() < 1 {
      return Err(malformed());
    }
    let user_len = cursor.get_u8() as usize;
    if cursor.remaining() < user_len {
      return Err(malformed());
    }
    let username = cursor.copy_to_bytes(user_len).to_vec();
    if cursor.remaining() < 1 {
      return Err(malformed());
    }
    let pass_len = cursor.get_u8() as usize;
    if cursor.remaining() != pass_len {
      return Err(malformed());
    }
    let password = cursor.copy_to_bytes(pass_len).to_vec();
    Ok((username, password))
  }

  /// ERROR body: reason-len(1) reason.
  fn parse_error_reason(body: &[u8]) -> String {
    match body.split_first() {
      Some((len, rest)) if rest.len() >= *len as usize => {
        String::from_utf8_lossy(&rest[..*len as usize]).into_owned()
      }
      _ => String::from_utf8_lossy(body).into_owned(),
    }
  }

  fn frame_with_metadata(&self, prefix: &[u8]) -> Vec<u8> {
    let mut frame = BytesMut::new();
    frame.put_slice(prefix);
    ZmtpReady {
      properties: self.local_metadata.clone(),
    }
    .encode_properties(&mut frame);
    frame.to_vec()
  }

  fn create_hello(&self) -> Vec<u8> {
    let username = self.username.as_deref().unwrap_or(b"");
    let password = self.password.as_deref().unwrap_or(b"");
    let user_len = username.len().min(255);
    let pass_len = password.len().min(255);
    let mut frame = BytesMut::with_capacity(Self::HELLO.len() + 2 + user_len + pass_len);
    frame.put_slice(Self::HELLO);
    frame.put_u8(user_len as u8);
    frame.put_slice(&username[..user_len]);
    frame.put_u8(pass_len as u8);
    frame.put_slice(&password[..pass_len]);
    frame.to_vec()
  }

  fn create_error(&self) -> Vec<u8> {
    let mut frame = BytesMut::with_capacity(Self::ERROR_LIBZMQ.len() + 1 + Self::ERROR_REASON_AUTH.len());
    frame.put_slice(Self::ERROR_LIBZMQ);
    frame.put_u8(Self::ERROR_REASON_AUTH.len() as u8);
    frame.put_slice(Self::ERROR_REASON_AUTH);
    frame.to_vec()
  }

  fn fail(&mut self, error: ZmqError) -> ZmqError {
    tracing::error!(mechanism = Self::NAME, %error, "Handshake error");
    self.error_reason = Some(error.to_string());
    self.error = Some(error.clone());
    self.state = PlainState::Error;
    error
  }

  fn unexpected(&mut self, command: &[u8]) -> ZmqError {
    let name = String::from_utf8_lossy(command.get(1..).unwrap_or(command)).into_owned();
    self.fail(ZmqError::SecurityError(format!(
      "Unexpected PLAIN command {} in state {:?}",
      name, self.state
    )))
  }

  fn metadata(&mut self, body: &[u8]) -> Result<(), ZmqError> {
    match ZmtpReady::parse_properties(body) {
      Ok(ready) => {
        self.peer_metadata = Some(ready.properties);
        Ok(())
      }
      Err(e) => Err(self.fail(ZmqError::SecurityError(format!("Malformed PLAIN metadata: {}", e)))),
    }
  }
}

impl Mechanism for PlainMechanism {
  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    let Some((command, body)) = Self::split_command(token) else {
      return Err(self.fail(ZmqError::SecurityError("Malformed command frame".into())));
    };

    match (self.state, command) {
      (PlainState::ServerExpectHello, Self::HELLO) => {
        let (username, password) = match Self::parse_hello_body(body) {
          Ok(parsed) => parsed,
          Err(e) => return Err(self.fail(e)),
        };
        let accepted = self.expected_username.as_deref() == Some(username.as_slice())
          && self.expected_password.as_deref() == Some(password.as_slice());
        if accepted {
          self.username = Some(username);
          self.password = Some(password);
          self.state = PlainState::ServerSendWelcome;
        } else {
          tracing::warn!(mechanism = Self::NAME, "PLAIN server rejected credentials");
          self.error = Some(ZmqError::AuthenticationFailure(Self::AUTH_FAILURE.into()));
          self.error_reason = Some(Self::AUTH_FAILURE.into());
          self.state = PlainState::ServerSendError;
        }
        Ok(ProcessTokenAction::ProduceAndSend)
      }
      (PlainState::ServerExpectInitiate, Self::INITIATE) => {
        self.metadata(body)?;
        self.state = PlainState::ServerSendReady;
        Ok(ProcessTokenAction::ProduceAndSend)
      }
      (PlainState::ClientExpectWelcome, Self::WELCOME) => {
        self.state = PlainState::ClientSendInitiate;
        Ok(ProcessTokenAction::ProduceAndSend)
      }
      (PlainState::ClientExpectReady, Self::READY) => {
        self.metadata(body)?;
        self.state = PlainState::Ready;
        Ok(ProcessTokenAction::HandshakeComplete)
      }
      (PlainState::ClientExpectWelcome | PlainState::ClientExpectReady, Self::ERROR) => {
        let reason = Self::parse_error_reason(body);
        Err(self.fail(ZmqError::AuthenticationFailure(format!(
          "Received ERROR from server: {}",
          reason
        ))))
      }
      _ => Err(self.unexpected(command)),
    }
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    let token = match self.state {
      PlainState::ClientSendHello => {
        self.state = PlainState::ClientExpectWelcome;
        self.create_hello()
      }
      PlainState::ClientSendInitiate => {
        self.state = PlainState::ClientExpectReady;
        self.frame_with_metadata(Self::INITIATE)
      }
      PlainState::ServerSendWelcome => {
        self.state = PlainState::ServerExpectInitiate;
        Self::WELCOME.to_vec()
      }
      PlainState::ServerSendReady => {
        self.state = PlainState::Ready;
        self.frame_with_metadata(Self::READY)
      }
      PlainState::ServerSendError => {
        self.state = PlainState::Error;
        self.create_error()
      }
      _ => return Ok(None),
    };
    Ok(Some(token))
  }

  fn status(&self) -> MechanismStatus {
    match self.state {
      PlainState::Ready => MechanismStatus::Ready,
      PlainState::Error => MechanismStatus::Error,
      _ => MechanismStatus::Handshaking,
    }
  }

  fn error_reason(&self) -> Option<&str> {
    self.error_reason.as_deref()
  }

  fn error(&self) -> Option<ZmqError> {
    self.error.clone()
  }

  fn peer_metadata(&self) -> Option<&HashMap<String, Vec<u8>>> {
    self.peer_metadata.as_ref()
  }

  fn into_framer(
    self: Box<Self>,
    max_msg_size: i64,
    sndbatch_count: usize,
    sndbatch_bytes_physical: usize,
  ) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError> {
    if self.status() != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "PLAIN handshake not complete.".into(),
      ));
    }
    Ok((
      Box::new(NullFramer::new(max_msg_size, sndbatch_count, sndbatch_bytes_physical)),
      self.username.clone(),
    ))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn pair() -> (PlainMechanism, PlainMechanism) {
    let mut client = PlainMechanism::new(false);
    client.set_client_credentials(Some(b"user".to_vec()), Some(b"pass".to_vec()));
    client.set_local_metadata(HashMap::from([("Socket-Type".to_string(), b"REQ".to_vec())]));
    let mut server = PlainMechanism::new(true);
    server.set_server_expected_credentials(Some(b"user".to_vec()), Some(b"pass".to_vec()));
    server.set_local_metadata(HashMap::from([("Socket-Type".to_string(), b"REP".to_vec())]));
    (client, server)
  }

  #[test]
  fn full_handshake_exchanges_metadata() {
    let (mut client, mut server) = pair();
    let hello = client.produce_token().unwrap().unwrap();
    assert!(hello.starts_with(b"\x05HELLO\x04user\x04pass"));
    assert_eq!(server.process_token(&hello).unwrap(), ProcessTokenAction::ProduceAndSend);
    let welcome = server.produce_token().unwrap().unwrap();
    assert_eq!(welcome, b"\x07WELCOME");
    assert_eq!(client.process_token(&welcome).unwrap(), ProcessTokenAction::ProduceAndSend);
    let initiate = client.produce_token().unwrap().unwrap();
    assert!(initiate.starts_with(b"\x08INITIATE\x0bSocket-Type\x00\x00\x00\x03REQ"));
    assert_eq!(server.process_token(&initiate).unwrap(), ProcessTokenAction::ProduceAndSend);
    let ready = server.produce_token().unwrap().unwrap();
    assert!(ready.starts_with(b"\x05READY\x0bSocket-Type\x00\x00\x00\x03REP"));
    assert!(server.is_complete());
    assert_eq!(client.process_token(&ready).unwrap(), ProcessTokenAction::HandshakeComplete);
    assert!(client.is_complete());
    assert_eq!(client.peer_metadata().unwrap()["Socket-Type"], b"REP");
    assert_eq!(server.peer_metadata().unwrap()["Socket-Type"], b"REQ");
  }

  #[test]
  fn wrong_password_sends_libzmq_error_then_fails() {
    let (mut client, mut server) = pair();
    client.set_client_credentials(Some(b"user".to_vec()), Some(b"nope".to_vec()));
    let hello = client.produce_token().unwrap().unwrap();
    assert_eq!(server.process_token(&hello).unwrap(), ProcessTokenAction::ProduceAndSend);
    let error = server.produce_token().unwrap().unwrap();
    assert_eq!(error, b"\x5eRROR\x03400");
    assert!(server.is_error());
    assert!(matches!(server.error(), Some(ZmqError::AuthenticationFailure(m)) if m == "Invalid username or password"));
    assert!(matches!(client.process_token(&error), Err(ZmqError::AuthenticationFailure(m)) if m == "Received ERROR from server: 400"));
  }

  #[test]
  fn client_accepts_spec_error_prefix_too() {
    let (mut client, _) = pair();
    client.produce_token().unwrap();
    assert!(matches!(client.process_token(b"\x05ERROR\x03500"), Err(ZmqError::AuthenticationFailure(m)) if m.ends_with("500")));
  }

  #[test]
  fn server_without_credentials_rejects_everyone() {
    let (mut client, mut server) = pair();
    server.set_server_expected_credentials(None, None);
    let hello = client.produce_token().unwrap().unwrap();
    server.process_token(&hello).unwrap();
    assert!(server.produce_token().unwrap().unwrap().starts_with(b"\x5eRROR"));
    assert!(server.is_error());
  }

  #[test]
  fn ready_before_welcome_is_rejected() {
    let (mut client, _) = pair();
    client.produce_token().unwrap();
    assert!(matches!(client.process_token(b"\x05READY"), Err(ZmqError::SecurityError(_))));
    assert!(client.is_error());
  }
}
