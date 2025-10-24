use bytes::{Buf, BufMut, BytesMut};

use crate::{security::mechanism::ProcessTokenAction, Metadata, ZmqError};

use super::{cipher::PassThroughDataCipher, IDataCipher, Mechanism, MechanismStatus};

/// State for the PLAIN security mechanism handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainState {
  Initializing, // Start state (not strictly used as we set initial state in new())
  // Client states
  ClientSendHello,     // Client -> Server: HELLO containing PLAIN details
  ClientExpectWelcome, // Client waiting for WELCOME from server
  // Server states
  ServerExpectHello, // Server waiting for HELLO from client
  ServerSendWelcome, // Server -> Client: WELCOME confirming PLAIN (after HELLO or ZAP)
  // End states
  Ready, // Handshake successful
  Error, // Handshake failed
}

/// Implements the ZMTP PLAIN security mechanism.
/// See RFC 27: https://rfc.zeromq.org/spec/27/
#[derive(Debug)]
pub struct PlainMechanism {
  is_server: bool,
  state: PlainState,
  // Store credentials (client uses its own, server stores received from client)
  username: Option<Vec<u8>>,
  password: Option<Vec<u8>>,
  // Optional ZAP metadata received (placeholder for future ZAP impl)
  _zap_metadata: Option<Metadata>,
  error_reason: Option<String>,
}

impl PlainMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"; // Padded
  pub const NAME: &'static str = "PLAIN";
  const CMD_HELLO: &'static [u8] = b"HELLO";
  const CMD_WELCOME: &'static [u8] = b"WELCOME";
  // const CMD_INITIATE: &'static [u8] = b"INITIATE"; // PLAIN doesn't use INITIATE
  const CMD_ERROR: &'static [u8] = b"ERROR";
  // const CMD_READY: &'static [u8] = b"READY"; // READY is part of handshake too

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
      _zap_metadata: None,
      error_reason: None,
    }
  }

  /// Called by the client-side engine to set credentials before handshake starts.
  pub fn set_client_credentials(&mut self, username: Option<Vec<u8>>, password: Option<Vec<u8>>) {
    if !self.is_server {
      self.username = username;
      self.password = password;
    } else {
      tracing::warn!("set_client_credentials called on a server-side PLAIN mechanism. Ignoring.");
    }
  }

  /// Parses the HELLO command body (client -> server).
  /// Body format: <username-len(1)><username><password-len(1)><password>
  /// Returns Ok((username, password)) or Error.
  fn parse_hello_body(body: &[u8]) -> Result<(Vec<u8>, Vec<u8>), ZmqError> {
    let mut cursor = std::io::Cursor::new(body);
    if body.len() < 2 {
      // Need at least length bytes
      return Err(ZmqError::SecurityError("PLAIN HELLO body too short".into()));
    }

    // Read username
    let user_len = cursor.get_u8() as usize;
    if cursor.remaining() < user_len + 1 {
      // Check space for username and password length byte
      return Err(ZmqError::SecurityError("Invalid PLAIN HELLO username length".into()));
    }
    let mut username = vec![0u8; user_len];
    cursor.copy_to_slice(&mut username);

    // Read password
    let pass_len = cursor.get_u8() as usize;
    if cursor.remaining() < pass_len {
      return Err(ZmqError::SecurityError("Invalid PLAIN HELLO password length".into()));
    }
    let mut password = vec![0u8; pass_len];
    cursor.copy_to_slice(&mut password);

    Ok((username, password))
  }

  /// Creates the HELLO command body.
  fn create_hello_body(username: &[u8], password: &[u8]) -> Vec<u8> {
    let user_len = username.len().min(255) as u8;
    let pass_len = password.len().min(255) as u8;

    let mut body = BytesMut::with_capacity(1 + user_len as usize + 1 + pass_len as usize);
    body.put_u8(user_len);
    body.put_slice(&username[..user_len as usize]);
    body.put_u8(pass_len);
    body.put_slice(&password[..pass_len as usize]);
    body.to_vec()
  }

  fn set_error_internal(&mut self, reason: String) {
    tracing::error!(mechanism = Self::NAME, %reason, "Handshake error");
    self.error_reason = Some(reason);
    self.state = PlainState::Error;
  }
}

impl Mechanism for PlainMechanism {
  fn name(&self) -> &'static str {
    Self::NAME
  }

  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    if token.is_empty() {
      self.set_error_internal("Received empty security token".into());
      return Err(ZmqError::SecurityError("Empty token".into()));
    }

    let command_len = token[0] as usize;
    if token.len() < 1 + command_len {
      self.set_error_internal("Invalid command frame received".into());
      return Err(ZmqError::SecurityError("Malformed command frame".into()));
    }
    let command_name = &token[1..1 + command_len];
    let body = &token[1 + command_len..];

    if self.is_server {
      match self.state {
        PlainState::ServerExpectHello => {
          if command_name == Self::CMD_HELLO {
            tracing::debug!(mechanism = Self::NAME, "Server received HELLO");
            match Self::parse_hello_body(body) {
              Ok((username, password)) => {
                self.username = Some(username);
                self.password = Some(password);
                
                // Directly move to SendWelcome, bypassing ZAP.
                tracing::debug!("PLAIN Server: Auto-accepting HELLO (ZAP bypassed for now).");
                self.state = PlainState::ServerSendWelcome;

                // INSTRUCTION TO CALLER: I have a response ready for you.
                Ok(ProcessTokenAction::ProduceAndSend)
              }
              Err(e) => {
                self.set_error_internal(format!("Failed to parse HELLO: {}", e));
                Err(e)
              }
            }
          } else {
            self.set_error_internal(format!("Expected HELLO, got {}", String::from_utf8_lossy(command_name)));
            Err(ZmqError::SecurityError("Unexpected command".into()))
          }
        }
        _ => {
          self.set_error_internal(format!(
            "Unexpected command received by server in state {:?}: {}",
            self.state,
            String::from_utf8_lossy(command_name)
          ));
          Err(ZmqError::SecurityError("Unexpected command in server state".into()))
        }
      }
    } else {
      // Client side
      match self.state {
        PlainState::ClientExpectWelcome => {
          if command_name == Self::CMD_WELCOME {
            tracing::debug!(mechanism = Self::NAME, "Client received WELCOME");
            self.state = PlainState::Ready;

            // INSTRUCTION TO CALLER: We are done.
            Ok(ProcessTokenAction::HandshakeComplete)
          } else if command_name == Self::CMD_ERROR {
            let reason = String::from_utf8_lossy(body).to_string();
            self.set_error_internal(format!("Received ERROR from server: {}", reason));
            Err(ZmqError::AuthenticationFailure(reason))
          } else {
            self.set_error_internal(format!(
              "Expected WELCOME or ERROR, got {}",
              String::from_utf8_lossy(command_name)
            ));
            Err(ZmqError::SecurityError("Unexpected command from server".into()))
          }
        }
        _ => {
          self.set_error_internal(format!(
            "Unexpected command received by client in state {:?}: {}",
            self.state,
            String::from_utf8_lossy(command_name)
          ));
          Err(ZmqError::SecurityError("Unexpected command in client state".into()))
        }
      }
    }
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    match self.state {
      PlainState::ClientSendHello => {
        let username_bytes = self.username.as_deref().unwrap_or(b"");
        let password_bytes = self.password.as_deref().unwrap_or(b"");
        let body = Self::create_hello_body(username_bytes, password_bytes);

        let mut frame = BytesMut::new();
        let cmd_name = Self::CMD_HELLO;
        frame.put_u8(cmd_name.len() as u8);
        frame.put_slice(cmd_name);
        frame.put_slice(&body);

        self.state = PlainState::ClientExpectWelcome;
        tracing::debug!(mechanism = Self::NAME, "Client sending HELLO");
        Ok(Some(frame.to_vec()))
      }
      PlainState::ServerSendWelcome => {
        let mut frame = BytesMut::new();
        let cmd_name = Self::CMD_WELCOME;
        frame.put_u8(cmd_name.len() as u8);
        frame.put_slice(cmd_name);
        // WELCOME body is empty for PLAIN

        self.state = PlainState::Ready;
        tracing::debug!(mechanism = Self::NAME, "Server sending WELCOME");
        Ok(Some(frame.to_vec()))
      }
      _ => Ok(None), // No tokens produced in other states
    }
  }

  fn status(&self) -> MechanismStatus {
    match self.state {
      PlainState::Initializing // Should not typically be in this observable state from outside
      | PlainState::ClientSendHello
      | PlainState::ClientExpectWelcome
      | PlainState::ServerExpectHello
      | PlainState::ServerSendWelcome => MechanismStatus::Handshaking,
      PlainState::Ready => MechanismStatus::Ready,
      PlainState::Error => MechanismStatus::Error,
    }
  }

  fn peer_identity(&self) -> Option<Vec<u8>> {
    // For PLAIN, the 'identity' is the username.
    // Server gets it from HELLO, client has it from config.
    self.username.clone()
  }

  fn metadata(&self) -> Option<Metadata> {
    // Return ZAP metadata if authentication provided any (future ZAP impl)
    self._zap_metadata.clone()
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }

  fn set_error(&mut self, reason: String) {
    self.set_error_internal(reason);
  }

  fn error_reason(&self) -> Option<&str> {
    self.error_reason.as_deref()
  }

  // --- ZAP Related Methods (Simplified for now) ---
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    // For now, PLAIN server will not use ZAP explicitly via this mechanism.
    // It transitions directly to ServerSendWelcome after HELLO.
    // If ZAP were used, this would be called when state is ServerAuthenticating.
    None
  }

  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    // If ZAP were used, this would parse the reply and transition state.
    // Since we are bypassing ZAP for now, this method will not be called
    // if zap_request_needed() returns None.
    // If it were called, and state was ServerAuthenticating:
    //   Parse reply -> if OK, self.state = ServerSendWelcome;
    //   else -> self.set_error_internal("ZAP auth failed"); return Err(...)
    Ok(())
  }

  fn into_data_cipher_parts(self: Box<Self>) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError> {
    if self.status() != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState("PLAIN handshake not complete.".into()));
    }
    // For PLAIN, the peer identity is typically the authenticated username.
    Ok((Box::new(PassThroughDataCipher::default()), self.username.clone()))
  }
}
