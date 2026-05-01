use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::command::{ZMTP_FLAG_COMMAND, ZMTP_FLAG_LONG, ZMTP_FLAG_MORE};
use bytes::{Buf, BufMut, BytesMut};

#[derive(Debug, Default, Clone, Copy)]
enum ManualDecodingState {
  #[default]
  ReadHeader,
  ReadBody {
    flags: u8,
    size: usize,
  },
}

#[derive(Debug)]
pub struct ZmtpManualParser {
  state: ManualDecodingState,
  max_msg_size: i64,
}

impl ZmtpManualParser {
  pub fn new(max_msg_size: i64) -> Self {
    Self {
      state: ManualDecodingState::default(),
      max_msg_size,
    }
  }

  /// Attempts to decode a single ZMTP message from the provided buffer.
  ///
  /// The buffer `src` is modified by consuming bytes for any successfully decoded message.
  /// If a partial message is present, `src` will retain the unconsumed part.
  ///
  /// # Returns
  /// - `Ok(Some(Msg))` if a full message was decoded.
  /// - `Ok(None)` if more data is needed to decode a complete message.
  /// - `Err(ZmqError)` if a protocol violation or decoding error occurs.
  pub fn decode_from_buffer(&mut self, src: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    loop {
      match self.state {
        ManualDecodingState::ReadHeader => {
          if src.is_empty() {
            return Ok(None); // Need at least flags byte
          }
          let frame_flags_byte = src[0]; // Peek
          let is_long = (frame_flags_byte & ZMTP_FLAG_LONG) != 0;
          let header_len = if is_long { 1 + 8 } else { 1 + 1 };

          if src.len() < header_len {
            return Ok(None); // Not enough data for full header
          }

          // Consume header
          let flags = src.get_u8(); // Consumes the flags byte
          let raw_size = if is_long {
            src.get_u64()
          } else {
            src.get_u8() as u64
          };
          if self.max_msg_size >= 0 && raw_size > self.max_msg_size as u64 {
            return Err(ZmqError::ProtocolViolation(format!(
              "frame size {} exceeds ZMQ_MAXMSGSIZE {}",
              raw_size, self.max_msg_size
            )));
          }
          let size = raw_size as usize;

          self.state = ManualDecodingState::ReadBody { flags, size };
        }
        ManualDecodingState::ReadBody { flags, size } => {
          if src.len() < size {
            return Ok(None); // Not enough data for body
          }

          let body_bytes = src.split_to(size).freeze(); // Consumes body from src

          // Reset state for next message
          self.state = ManualDecodingState::ReadHeader;

          // Construct rzmq Msg
          let mut msg = Msg::from_bytes(body_bytes);
          let mut rz_flags = MsgFlags::empty();
          if (flags & ZMTP_FLAG_MORE) != 0 {
            rz_flags |= MsgFlags::MORE;
          }
          if (flags & ZMTP_FLAG_COMMAND) != 0 {
            rz_flags |= MsgFlags::COMMAND;
          }
          msg.set_flags(rz_flags);
          return Ok(Some(msg));
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn long_frame_header(size: u64) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_u8(ZMTP_FLAG_LONG);
    buf.put_u64(size);
    buf
  }

  #[test]
  fn oversized_frame_returns_protocol_violation() {
    let mut parser = ZmtpManualParser::new(64);
    let result = parser.decode_from_buffer(&mut long_frame_header(128));
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn exact_limit_does_not_error() {
    let mut parser = ZmtpManualParser::new(64);
    let result = parser.decode_from_buffer(&mut long_frame_header(64));
    assert!(matches!(result, Ok(None)));
  }

  #[test]
  fn unlimited_allows_max_u64_header() {
    let mut parser = ZmtpManualParser::new(-1);
    let result = parser.decode_from_buffer(&mut long_frame_header(u64::MAX));
    assert!(matches!(result, Ok(None)));
  }

  #[test]
  fn zero_limit_blocks_any_nonzero_frame() {
    let mut parser = ZmtpManualParser::new(0);
    let result = parser.decode_from_buffer(&mut long_frame_header(1));
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn full_message_within_limit_decoded_correctly() {
    let limit = 16i64;
    let mut parser = ZmtpManualParser::new(limit);
    let payload = b"hello rzmq";
    let mut buf = BytesMut::new();
    buf.put_u8(0x00); // short frame, no flags
    buf.put_u8(payload.len() as u8);
    buf.put_slice(payload);
    let result = parser.decode_from_buffer(&mut buf);
    let msg = result.expect("should succeed").expect("should have message");
    assert_eq!(msg.data().unwrap(), payload);
  }
}
