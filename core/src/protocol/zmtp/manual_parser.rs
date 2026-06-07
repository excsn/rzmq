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

  /// Parse one ZMTP frame from `src` without mutating parser state.
  /// `src` must begin at a frame boundary (flags byte).
  /// Returns `Ok(Some((msg, bytes_consumed)))` when a complete frame is available,
  /// `Ok(None)` when more data is needed, or `Err` on protocol violation.
  pub fn decode_frame_from_slice(&self, src: &[u8]) -> Result<Option<(Msg, usize)>, ZmqError> {
    if src.len() < 2 {
      return Ok(None);
    }
    let flags = src[0];
    let is_long = (flags & ZMTP_FLAG_LONG) != 0;
    let header_len = if is_long { 9 } else { 2 };
    if src.len() < header_len {
      return Ok(None);
    }
    let raw_size = if is_long {
      let mut len_bytes = [0u8; 8];
      len_bytes.copy_from_slice(&src[1..9]);
      u64::from_be_bytes(len_bytes)
    } else {
      src[1] as u64
    };
    if self.max_msg_size >= 0 && raw_size > self.max_msg_size as u64 {
      return Err(ZmqError::ProtocolViolation(format!(
        "frame size {} exceeds ZMQ_MAXMSGSIZE {}",
        raw_size, self.max_msg_size
      )));
    }
    let size = raw_size as usize;
    let total = header_len + size;
    if src.len() < total {
      return Ok(None);
    }
    // ONE copy: kernel ring buffer → final Msg payload allocation
    let mut msg = Msg::from_vec(src[header_len..total].to_vec());
    let mut rz_flags = MsgFlags::empty();
    if (flags & ZMTP_FLAG_MORE) != 0 {
      rz_flags |= MsgFlags::MORE;
    }
    if (flags & ZMTP_FLAG_COMMAND) != 0 {
      rz_flags |= MsgFlags::COMMAND;
    }
    msg.set_flags(rz_flags);
    Ok(Some((msg, total)))
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

          // Non-destructive length parsing to avoid mutating the accumulator on partial reads
          let raw_size = if is_long {
            let mut len_bytes = [0u8; 8];
            len_bytes.copy_from_slice(&src[1..9]);
            u64::from_be_bytes(len_bytes)
          } else {
            src[1] as u64
          };

          if self.max_msg_size >= 0 && raw_size > self.max_msg_size as u64 {
            return Err(ZmqError::ProtocolViolation(format!(
              "frame size {} exceeds ZMQ_MAXMSGSIZE {}",
              raw_size, self.max_msg_size
            )));
          }
          let size = raw_size as usize;

          // Guard: Verify if the full body is present as well.
          // Use subtraction (not addition) to avoid overflow when size == usize::MAX.
          // Safe because src.len() >= header_len is guaranteed by the early-return above.
          if src.len() - header_len < size {
            return Ok(None); // Return early without mutating accumulator or state
          }

          // Consume the header now that we are guaranteed to have the complete frame
          let flags = src.get_u8();
          let _ = src.split_to(header_len - 1); // Discard length bytes
          let body_bytes = src.split_to(size).freeze();

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
    let msg = result
      .expect("should succeed")
      .expect("should have message");
    assert_eq!(msg.data().unwrap(), payload);
  }
}

#[cfg(test)]
mod additional_robustness_tests {
  use super::*;

  #[test]
  fn test_fragmented_stream_parsing() {
    let mut parser = ZmtpManualParser::new(1024);
    let payload = b"stream-fragmentation-test-payload";

    let mut raw_bytes = BytesMut::new();
    raw_bytes.put_u8(0x00); // short frame, no flags
    raw_bytes.put_u8(payload.len() as u8);
    raw_bytes.put_slice(payload);

    let mut accumulator = BytesMut::new();
    let total_len = raw_bytes.len();

    for i in 0..total_len {
      accumulator.put_u8(raw_bytes[i]);
      let res = parser.decode_from_buffer(&mut accumulator);

      if i < total_len - 1 {
        assert!(
          matches!(res, Ok(None)),
          "Expected Ok(None) for incomplete frame at byte {}",
          i
        );
      } else {
        let msg = res
          .expect("Should parse successfully on final byte")
          .expect("Should yield a Msg on final byte");
        assert_eq!(msg.data().unwrap(), payload.as_ref());
        assert!(!msg.is_more());
      }
    }
  }

  #[test]
  fn test_adversarial_header_parsing() {
    let mut parser = ZmtpManualParser::new(1024);

    // A command frame with 0-byte body followed by trailing garbage.
    // The parser treats this as a valid empty command and returns Ok(Some(msg));
    // the trailing bytes remain unconsumed. Verify no panic occurs.
    let mut bad_buf = BytesMut::new();
    bad_buf.put_u8(ZMTP_FLAG_COMMAND);
    bad_buf.put_u8(0);
    bad_buf.put_slice(b"garbage");

    let res = parser.decode_from_buffer(&mut bad_buf);
    assert!(res.is_ok());
  }

  #[test]
  fn test_integer_overflow_protection() {
    let mut parser = ZmtpManualParser::new(1024);

    // Long-frame header claiming u64::MAX bytes — must exceed max_msg_size check.
    let mut bad_buf = BytesMut::new();
    bad_buf.put_u8(ZMTP_FLAG_LONG);
    bad_buf.put_u64(u64::MAX);

    let res = parser.decode_from_buffer(&mut bad_buf);
    assert!(
      matches!(res, Err(ZmqError::ProtocolViolation(_))),
      "Expected ProtocolViolation for u64::MAX frame size, got {:?}",
      res
    );
  }
}
