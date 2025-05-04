// src/protocol/zmtp/command.rs

// <<< ADDED ZMTP FRAMING CONSTANTS >>>

use bytes::Bytes;

use crate::{Msg, MsgFlags};

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
  Ping(Bytes), // Contains TTL and Context
  Pong(Bytes), // Contains Context
  Ready,       // TODO: Add properties map later
  Error,       // TODO: Add error reason later
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
    } else if body.starts_with(b"\x05READY") {
      // READY command format: <length=5>READY<Properties>
      // TODO: Parse properties map later
      Some(ZmtpCommand::Ready)
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
