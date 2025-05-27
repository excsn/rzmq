// src/security/mechanism.rs

use super::ZmqError;
use crate::message::Metadata;
use bytes::{Buf, BufMut, BytesMut};
use std::fmt;

/// Trait for security mechanisms (NULL, PLAIN, etc.).
/// Drives the security handshake state machine.
pub trait Mechanism: Send + Sync + fmt::Debug + 'static {
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
  /// For Non-PLAIN, this is the peer's public key. For PLAIN, potentially the User-Id.
  fn peer_identity(&self) -> Option<Vec<u8>>;

  /// Provides any additional metadata derived from the handshake (e.g., ZAP User-Id).
  /// This can be merged into outgoing/incoming messages.
  fn metadata(&self) -> Option<Metadata>;

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

/// State for the PLAIN security mechanism handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlainState {
  Initializing, // Start state
  // Client states
  SendHello,     // Client -> Server: HELLO containing PLAIN details
  ExpectWelcome, // Client waiting for WELCOME from server
  // Server states
  ExpectHello, // Server waiting for HELLO from client
  SendWelcome, // Server -> Client: WELCOME confirming PLAIN
  // ZAP states (Placeholder)
  Authenticating, // Waiting for ZAP reply (both client/server might need this)
  // End states
  Ready, // Handshake successful
  Error, // Handshake failed
}

#[derive(Debug)] // Add Debug
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
}

/// Implements the ZMTP PLAIN security mechanism.
/// See RFC 27: https://rfc.zeromq.org/spec/27/
#[derive(Debug)]
pub struct PlainMechanism {
  is_server: bool,
  state: PlainState,
  // Store credentials (client only) or received credentials (server)
  // For simplicity, storing as Vec<u8> for now. Real impl needs secure handling.
  username: Option<Vec<u8>>,
  password: Option<Vec<u8>>,
  // Optional ZAP metadata received
  zap_metadata: Option<Metadata>, // Placeholder for metadata from ZAP
  error_reason: Option<String>,
}

impl PlainMechanism {
  pub const NAME_BYTES: &'static [u8; 20] = b"PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"; // Padded
  pub const NAME: &'static str = "PLAIN";
  const CMD_HELLO: &'static [u8] = b"HELLO";
  const CMD_WELCOME: &'static [u8] = b"WELCOME";
  const CMD_INITIATE: &'static [u8] = b"INITIATE"; // PLAIN doesn't use INITIATE
  const CMD_ERROR: &'static [u8] = b"ERROR";
  const CMD_READY: &'static [u8] = b"READY"; // READY is part of handshake too

  pub fn new(is_server: bool /* TODO: Add credentials if client */) -> Self {
    Self {
      is_server,
      state: if is_server {
        PlainState::ExpectHello
      } else {
        PlainState::SendHello
      },
      username: None, // TODO: Client should get from options
      password: None, // TODO: Client should get from options
      zap_metadata: None,
      error_reason: None,
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

    // Ensure no trailing data? ZMQ spec doesn't forbid it, but maybe good practice.
    // if cursor.has_remaining() { Err(...) }

    Ok((username, password))
  }

  /// Creates the HELLO command body.
  fn create_hello_body(username: &[u8], password: &[u8]) -> Vec<u8> {
    // Limit lengths? ZMQ limits usually 255.
    let user_len = username.len().min(255) as u8;
    let pass_len = password.len().min(255) as u8;

    let mut body = BytesMut::with_capacity(1 + user_len as usize + 1 + pass_len as usize);
    body.put_u8(user_len);
    body.put_slice(&username[..user_len as usize]);
    body.put_u8(pass_len);
    body.put_slice(&password[..pass_len as usize]);
    body.to_vec()
  }

  /// Sets the mechanism state to Error. (Already existed, keep it)
  fn set_error_internal(&mut self, reason: String) {
    // Rename to avoid conflict
    tracing::error!(mechanism = Self::NAME, %reason, "Handshake error");
    self.error_reason = Some(reason);
    self.state = PlainState::Error;
  }
}

impl Mechanism for PlainMechanism {
  fn name(&self) -> &'static str {
    Self::NAME
  }

  fn process_token(&mut self, token: &[u8]) -> Result<(), ZmqError> {
    // Token is the body of a ZMTP COMMAND frame
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
        PlainState::ExpectHello => {
          if command_name == Self::CMD_HELLO {
            tracing::debug!(mechanism = Self::NAME, "Server received HELLO");
            match Self::parse_hello_body(body) {
              Ok((username, password)) => {
                self.username = Some(username);
                self.password = Some(password);
                // TODO: Trigger ZAP request here!
                // For now, assume success and move to SendWelcome
                tracing::warn!("PLAIN: Skipping ZAP authentication check (TODO)");
                self.state = PlainState::SendWelcome; // Move state *before* produce_token is called
                Ok(())
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
        // Server should not receive other commands during PLAIN handshake
        _ => {
          self.set_error_internal(format!(
            "Unexpected command received by server in state {:?}: {}",
            self.state,
            String::from_utf8_lossy(command_name)
          ));
          Err(ZmqError::SecurityError("Unexpected command".into()))
        }
      }
    } else {
      // Client side
      match self.state {
        PlainState::ExpectWelcome => {
          if command_name == Self::CMD_WELCOME {
            tracing::debug!(mechanism = Self::NAME, "Client received WELCOME");
            // Handshake successful
            self.state = PlainState::Ready;
            Ok(())
          } else if command_name == Self::CMD_ERROR {
            // TODO: Parse reason string from body
            let reason = String::from_utf8_lossy(body).to_string();
            self.set_error_internal(format!("Received ERROR from server: {}", reason));
            Err(ZmqError::AuthenticationFailure(reason))
          } else {
            self.set_error_internal(format!(
              "Expected WELCOME or ERROR, got {}",
              String::from_utf8_lossy(command_name)
            ));
            Err(ZmqError::SecurityError("Unexpected command".into()))
          }
        }
        // Client should not receive other commands during PLAIN handshake
        _ => {
          self.set_error_internal(format!(
            "Unexpected command received by client in state {:?}: {}",
            self.state,
            String::from_utf8_lossy(command_name)
          ));
          Err(ZmqError::SecurityError("Unexpected command".into()))
        }
      }
    }
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    match self.state {
      // Client sends HELLO initially
      PlainState::SendHello => {
        let username = self.username.as_deref().unwrap_or(b""); // TODO: Get from actual config
        let password = self.password.as_deref().unwrap_or(b""); // TODO: Get from actual config
        let body = Self::create_hello_body(username, password);

        let mut frame = BytesMut::new();
        let cmd_name = Self::CMD_HELLO;
        frame.put_u8(cmd_name.len() as u8);
        frame.put_slice(cmd_name);
        frame.put_slice(&body);

        self.state = PlainState::ExpectWelcome; // Move state
        tracing::debug!(mechanism = Self::NAME, "Client sending HELLO");
        Ok(Some(frame.to_vec()))
      }
      // Server sends WELCOME after validating HELLO (and ZAP)
      PlainState::SendWelcome => {
        let mut frame = BytesMut::new();
        let cmd_name = Self::CMD_WELCOME;
        frame.put_u8(cmd_name.len() as u8);
        frame.put_slice(cmd_name);
        // WELCOME body is empty for PLAIN

        self.state = PlainState::Ready; // Move state
        tracing::debug!(mechanism = Self::NAME, "Server sending WELCOME");
        Ok(Some(frame.to_vec()))
      }
      // No tokens produced in other states
      PlainState::Initializing
      | PlainState::ExpectWelcome
      | PlainState::ExpectHello
      | PlainState::Authenticating
      | PlainState::Ready
      | PlainState::Error => Ok(None),
    }
  }

  fn status(&self) -> MechanismStatus {
    match self.state {
      PlainState::Initializing
      | PlainState::SendHello
      | PlainState::ExpectWelcome
      | PlainState::ExpectHello
      | PlainState::SendWelcome => MechanismStatus::Handshaking,
      PlainState::Authenticating => MechanismStatus::Authenticating,
      PlainState::Ready => MechanismStatus::Ready,
      PlainState::Error => MechanismStatus::Error,
    }
  }

  fn peer_identity(&self) -> Option<Vec<u8>> {
    // PLAIN doesn't inherently establish a cryptographic peer identity.
    // We could return the authenticated username if available.
    self.username.clone()
  }

  fn metadata(&self) -> Option<Metadata> {
    // Return ZAP metadata if authentication provided any
    self.zap_metadata.clone()
  }

  /// Sets the mechanism's internal state to Error.
  fn set_error(&mut self, reason: String) {
    // Call the internal helper method
    self.set_error_internal(reason);
  }

  /// Returns the reason for the error state, if available.
  fn error_reason(&self) -> Option<&str> {
    self.error_reason.as_deref() // Return borrow from Option<String>
  }

  // --- ZAP Related Methods (Placeholders) ---
  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    if self.is_server && self.state == PlainState::SendWelcome {
      // If we just received HELLO and moved to SendWelcome, we *should* check ZAP
      // Before sending WELCOME. Change state to Authenticating.
      tracing::info!("PLAIN: ZAP request needed (TODO: Implement)");
      // self.state = PlainState::Authenticating;
      // Construct ZAP request frames here based on self.username/password
      // Example: vec![ b"Version", b"Request-Id", b"Domain", b"Address", b"Identity",
      //                b"Mechanism", b"Credentials" ]
      // return Some(zap_frames);
      None // Placeholder: Return None until ZAP implemented
    } else {
      None
    }
  }

  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    if self.is_server && self.state == PlainState::Authenticating {
      tracing::info!("PLAIN: Processing ZAP reply (TODO: Implement)");
      // Parse ZAP reply (Status-Code, Status-Text, User-Id, Metadata)
      // Based on Status-Code:
      // If "200": move state to SendWelcome, store User-Id/Metadata
      // If "300": Transient error? Maybe retry? For PLAIN likely fail. Set error.
      // If "400": Auth failed. Set error with Status-Text.
      // If "500": Server error. Set error.
      // Example (Success Case):
      // self.zap_metadata = Some(parsed_metadata);
      // self.state = PlainState::SendWelcome;
      // Example (Failure Case):
      // self.set_error(format!("ZAP authentication failed: {}", status_text));
      // return Err(ZmqError::AuthenticationFailure(status_text));
      Ok(()) // Placeholder: Assume success for now
    } else {
      // Should not happen
      tracing::warn!("process_zap_reply called in unexpected state: {:?}", self.state);
      Ok(())
    }
  }
}
