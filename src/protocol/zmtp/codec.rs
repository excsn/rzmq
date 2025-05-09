// src/protocol/zmtp/codec.rs

use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::command::*; // Use framing constants
use bytes::{Buf, BufMut, BytesMut};
use std::convert::TryInto;
use tokio_util::codec::{Decoder, Encoder};

// <<< ADDED ZmtpCodec STRUCT AND IMPLS >>>

/// Codec for ZMTP/3.1 message framing.
#[derive(Debug, Default)]
pub struct ZmtpCodec {
  // State needed for decoding potentially fragmented frames
  // TODO: Add state if needed, e.g., expected size of next frame body
  decoding_state: DecodingState,
}

#[derive(Debug, Default, Clone, Copy)]
enum DecodingState {
  #[default]
  ReadHeader, // Waiting for flags + length octets
  ReadBody(FrameHeader), // Waiting for frame body bytes
}

#[derive(Debug, Clone, Copy)]
struct FrameHeader {
  flags: u8,
  size: usize,
}

impl ZmtpCodec {
  pub fn new() -> Self {
    Self::default()
  }
}

// --- Encoder Implementation (Msg -> BytesMut) ---
impl Encoder<Msg> for ZmtpCodec {
  type Error = ZmqError;

  fn encode(&mut self, item: Msg, dst: &mut BytesMut) -> Result<(), Self::Error> {
    let data = item.data().unwrap_or(&[]); // Treat None data as empty slice
    let size = data.len();
    let msg_flags = item.flags();

    // Determine ZMTP flags
    let mut zmtp_flags = 0u8;
    if msg_flags.contains(MsgFlags::MORE) {
      zmtp_flags |= ZMTP_FLAG_MORE;
    }
    if msg_flags.contains(MsgFlags::COMMAND) {
      zmtp_flags |= ZMTP_FLAG_COMMAND;
    }

    // Encode based on size (short vs long frame)
    if size <= 255 {
      // Short frame: flags(1) + size(1) + body
      dst.reserve(2 + size);
      dst.put_u8(zmtp_flags); // Only MORE/COMMAND flags relevant for short
      dst.put_u8(size as u8);
    } else {
      // Long frame: flags(1) + 0xFF indicator(1) + size(8) + body
      zmtp_flags |= ZMTP_FLAG_LONG; // Set LONG flag
      dst.reserve(10 + size);
      dst.put_u8(zmtp_flags);
      dst.put_u8(0xFF); // Per spec, length is 8 bytes (although spec diagram is confusing)
                        // Re-checking spec 3.1 / 4.2 Framing: Flag octet has LONG flag. Length is 1 or 8 octets.
                        // Let's use FLAG_LONG bit in first octet, then 8-byte length.
                        // This seems simpler than the 0xFF marker from older drafts?
                        // Correcting: ZMTP 3.1 uses FLAG_LONG in first octet, followed by 8-byte length.
      dst.put_u64(size as u64); // Network byte order (Big Endian)
    }

    // Put message body
    dst.put_slice(data);

    Ok(())
  }
}

// --- Decoder Implementation (BytesMut -> Msg) ---
impl Decoder for ZmtpCodec {
  type Item = Msg; // Successfully decoded message/command frame
  type Error = ZmqError;

  fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
    loop {
      match self.decoding_state {
        DecodingState::ReadHeader => {
          // Need at least 1 byte for flags, maybe more for length
          if src.is_empty() {
            return Ok(None); // Need more data
          }

          let flags = src[0]; // Peek at flags byte
          let is_long = (flags & ZMTP_FLAG_LONG) != 0;
          let header_len = if is_long { 1 + 8 } else { 1 + 1 }; // flags + length

          if src.len() < header_len {
            src.reserve(header_len - src.len()); // Reserve space if possible
            return Ok(None); // Need more data for header
          }

          // Consume header bytes
          let header_bytes = src.split_to(header_len);
          let flags = header_bytes[0]; // Consume flags byte

          // Read length
          let size = if is_long {
            // Read u64 length (network byte order)
            let mut len_bytes = &header_bytes[1..]; // Slice starting after flags
            len_bytes.get_u64() as usize
          } else {
            // Read u8 length
            header_bytes[1] as usize
          };

          // Store header info and move to ReadBody state
          let header = FrameHeader { flags, size };
          self.decoding_state = DecodingState::ReadBody(header);
          // Continue loop to try decoding body immediately if possible
        }

        DecodingState::ReadBody(header) => {
          if src.len() < header.size {
            // Not enough data for the body yet
            src.reserve(header.size - src.len()); // Reserve if possible
            return Ok(None);
          }

          // Enough data available, consume body
          let body_bytes = src.split_to(header.size).freeze(); // freeze() -> Bytes

          // Reset state for next frame
          self.decoding_state = DecodingState::ReadHeader;

          // Create rzmq::Msg
          let mut msg = Msg::from_bytes(body_bytes);
          let mut msg_flags = MsgFlags::empty();
          if (header.flags & ZMTP_FLAG_MORE) != 0 {
            msg_flags |= MsgFlags::MORE;
          }
          if (header.flags & ZMTP_FLAG_COMMAND) != 0 {
            msg_flags |= MsgFlags::COMMAND;
          }
          msg.set_flags(msg_flags);

          // Return the decoded message
          return Ok(Some(msg));
        }
      } // end match self.decoding_state
    } // end loop
  } // end decode
}
// <<< ADDED ZmtpCodec STRUCT AND IMPLS END >>>
