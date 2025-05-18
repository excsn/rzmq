// src/protocol/zmtp/command.rs

// <<< ADDED ZMTP FRAMING CONSTANTS >>>

use std::collections::HashMap;

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{Msg, MsgFlags, ZmqError};

// --- ZMTP Frame Flags ---
// Located in the first byte of the length field for messages/commands.
pub const ZMTP_FLAG_LONG: u8 = 0b0000_0010; // Indicates 8-byte length instead of 1-byte
pub const ZMTP_FLAG_MORE: u8 = 0b0000_0001; // Indicates more frames follow (like rzmq::MsgFlags::MORE)
pub const ZMTP_FLAG_COMMAND: u8 = 0b0000_0100; // Indicates this is a ZMTP command frame

// --- ZMTP Command Names ---
// Used as the first part of a COMMAND frame's body.
pub const ZMTP_CMD_READY_NAME: &[u8] = b"READY";
pub const ZMTP_CMD_ERROR_NAME: &[u8] = b"ERROR";
pub const ZMTP_CMD_SUBSCRIBE_NAME: &[u8] = b"SUBSCRIBE";
pub const ZMTP_CMD_CANCEL_NAME: &[u8] = b"CANCEL";
pub const ZMTP_CMD_PING_NAME: &[u8] = b"PING";
pub const ZMTP_CMD_PONG_NAME: &[u8] = b"PONG";
// Security mechanisms define others (HELLO, INITIATE, etc.)

/// Represents known ZMTP commands relevant to basic operation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ZmtpCommand {
  Ping(Bytes),      // Contains TTL and Context
  Pong(Bytes),      // Contains Context
  Ready(ZmtpReady), // Store parsed READY info
  Error,            // TODO: Add reason Vec<u8>
  // Add other commands like Subscribe, Cancel as needed
  Unknown(Bytes), // For commands we don't handle specifically
}

impl ZmtpCommand {
  /// Parses the body of a Msg marked with MsgFlags::COMMAND.
  pub fn parse(msg: &Msg) -> Option<Self> {
    if !msg.is_command() || msg.is_more() {
      return None; // Not a valid single-frame command
    }
    let body = msg.data()?; // Get command body bytes

    // Check command name (first part, length-prefixed)
    if body.starts_with(b"\x04PING") && body.len() >= 5 {
      // PING command format: <length=4>PING<TTL(2)><Context(0+)>
      // Extract context after "PING" + TTL
      let context = Bytes::copy_from_slice(&body[5 + 2..]);
      Some(ZmtpCommand::Ping(context))
    } else if body.starts_with(b"\x04PONG") && body.len() >= 5 {
      // PONG command format: <length=4>PONG<Context(0+)>
      // Extract context after "PONG"
      let context = Bytes::copy_from_slice(&body[5..]);
      Some(ZmtpCommand::Pong(context))
    } else if body.starts_with(b"\x05READY") && body.len() >= 6 {
      // Body = <length=5>READY<Properties...>
      match ZmtpReady::parse_properties(&body[6..]) {
        Ok(ready_cmd) => Some(ZmtpCommand::Ready(ready_cmd)),
        Err(e) => {
          tracing::error!("Failed to parse READY properties: {}", e);
          None // Treat parse failure as unknown/error
        }
      }
    } else if body.starts_with(b"\x05ERROR") {
      // ERROR command format: <length=5>ERROR<Reason>
      // TODO: Parse reason later
      Some(ZmtpCommand::Error)
    } else {
      Some(ZmtpCommand::Unknown(Bytes::copy_from_slice(body)))
    }
    // Note: This parsing is basic and assumes single-frame commands for PING/PONG etc.
    // ZMTP allows multi-frame commands, handling that adds complexity.
  }

  /// Creates a PING command message.
  pub fn create_ping(ttl: u16, context: &[u8]) -> Msg {
    let mut body = Vec::with_capacity(5 + 2 + context.len());
    body.extend_from_slice(b"\x04PING");
    body.extend_from_slice(&ttl.to_be_bytes()); // TTL is big-endian u16
    body.extend_from_slice(context);

    let mut msg = Msg::from_vec(body);
    msg.set_flags(MsgFlags::COMMAND);
    msg
  }

  /// Creates a PONG command message.
  pub fn create_pong(context: &[u8]) -> Msg {
    let mut body = Vec::with_capacity(5 + context.len());
    body.extend_from_slice(b"\x04PONG");
    body.extend_from_slice(context);

    let mut msg = Msg::from_vec(body);
    msg.set_flags(MsgFlags::COMMAND);
    msg
  }
}

/// Represents a parsed ZMTP READY command.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ZmtpReady {
  /// Key-value properties map (e.g., "Socket-Type", "Identity"). Keys are case-sensitive.
  pub properties: HashMap<String, Vec<u8>>, // Store values as bytes for flexibility
}

impl ZmtpReady {
  /// Parses the properties map from the body of a READY command frame.
  /// Expects ZMTP metadata format: (name-len(u8) name(str) value-len(u32) value(bytes))...
  pub fn parse_properties(body: &[u8]) -> Result<Self, ZmqError> {
    let mut props = HashMap::new();
    let mut cursor = std::io::Cursor::new(body);

    while cursor.position() < body.len() as u64 {
      // Read name
      let name_len = cursor.get_u8() as usize;
      if cursor.remaining() < name_len {
        return Err(ZmqError::ProtocolViolation("Invalid metadata name length".into()));
      }
      let name_bytes = cursor.copy_to_bytes(name_len);
      let name = String::from_utf8(name_bytes.to_vec())
        .map_err(|_| ZmqError::ProtocolViolation("Metadata name not valid UTF-8".into()))?;

      // Read value
      if cursor.remaining() < 4 {
        return Err(ZmqError::ProtocolViolation("Invalid metadata value length".into()));
      }
      let value_len = cursor.get_u32() as usize; // Big Endian from get_u32
      if cursor.remaining() < value_len {
        return Err(ZmqError::ProtocolViolation("Invalid metadata value length".into()));
      }
      let value_bytes = cursor.copy_to_bytes(value_len);

      props.insert(name, value_bytes.to_vec());
    }

    Ok(Self { properties: props })
  }

  /// Encodes the READY command properties into a buffer for sending.
  pub fn encode_properties(&self, dst: &mut BytesMut) {
    for (name, value) in &self.properties {
      let name_bytes = name.as_bytes();
      // ZMQ limits name length
      if name_bytes.len() > 255 {
        tracing::warn!(
          "Skipping ZMTP metadata property with name longer than 255 bytes: {}",
          name
        );
        continue;
      }
      dst.put_u8(name_bytes.len() as u8);
      dst.put_slice(name_bytes);
      // ZMQ limits value length to u32::MAX
      dst.put_u32(value.len() as u32); // Big Endian
      dst.put_slice(value);
    }
  }

  /// Creates a READY command Msg with the given properties.
  pub fn create_msg(properties: HashMap<String, Vec<u8>>) -> Msg {
    let cmd = ZmtpReady { properties };
    let mut body = BytesMut::new();
    // Prepend command name (length prefixed)
    let name = ZMTP_CMD_READY_NAME;
    body.put_u8(name.len() as u8); // Not ZMTP standard? Check spec 4.1 Command frame
                                   // Re-checking: Yes, command name is length-prefixed string in body.
    body.put_slice(name);
    // Append encoded properties
    cmd.encode_properties(&mut body);

    let mut msg = Msg::from_bytes(body.freeze());
    msg.set_flags(MsgFlags::COMMAND);
    msg
  }
}
