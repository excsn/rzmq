use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::command::{ZMTP_FLAG_COMMAND, ZMTP_FLAG_LONG, ZMTP_FLAG_MORE};
use bytes::{Buf, BytesMut};

#[derive(Debug, Default, Clone, Copy)]
enum ManualDecodingState {
  #[default]
  ReadHeader,
  ReadBody {
    flags: u8,
    size: usize,
  },
}

#[derive(Debug, Default)]
pub struct ZmtpManualParser {
  state: ManualDecodingState,
  // Internal buffer to hold partial data if `next_message` is called with insufficient bytes
  // for a full frame, but this is usually managed by the caller feeding `decode_from_buffer`.
}

impl ZmtpManualParser {
  pub fn new() -> Self {
    Self::default()
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
          let size = if is_long {
            src.get_u64() as usize
          } else {
            src.get_u8() as usize
          };

          self.state = ManualDecodingState::ReadBody { flags, size };
          // Continue loop to try reading body immediately
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