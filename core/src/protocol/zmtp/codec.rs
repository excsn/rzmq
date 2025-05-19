use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::command::*;
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

/// Codec for ZMTP/3.1 message framing.
#[derive(Debug, Default)]
pub struct ZmtpCodec {
  // State needed for decoding potentially fragmented frames
  // TODO: Add state if needed, e.g., expected size of next frame body
  decoding_state: DecodingState,
  prefix_bytes: Option<BytesMut>,
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
    Self {
      decoding_state: DecodingState::default(),
      prefix_bytes: None,
    }
  }

  pub fn prime_with_prefix(&mut self, prefix: BytesMut) {
    if !prefix.is_empty() {
      tracing::trace!(prefix_len = prefix.len(), "ZmtpCodec primed with prefix bytes");
      self.prefix_bytes = Some(prefix);
    }
  }

  /// Encodes only the ZMTP frame header (flags and length) for the given message
  /// into the destination `BytesMut` buffer.
  ///
  /// The actual message payload from `item.data()` is NOT written by this method.
  /// This is intended for use with vectored/zerocopy sends where the payload
  /// is sent from a separate buffer.
  ///
  /// # Arguments
  /// * `item`: A reference to the `Msg` whose header is to be encoded.
  /// * `dst`: The `BytesMut` buffer to write the header into.
  ///
  /// # Returns
  /// `Ok(())` on success, or a `ZmqError` if an issue occurs (though unlikely for header encoding).
  pub fn encode_header_only(&self, item: &Msg, dst: &mut BytesMut) -> Result<(), ZmqError> {
    let data_size = item.size(); // This is the size of the payload that *would* be sent
    let msg_flags = item.flags();

    let mut zmtp_flags_byte = 0u8;
    if msg_flags.contains(MsgFlags::MORE) {
      zmtp_flags_byte |= ZMTP_FLAG_MORE;
    }
    if msg_flags.contains(MsgFlags::COMMAND) {
      zmtp_flags_byte |= ZMTP_FLAG_COMMAND;
    }

    // ZMTP 3.1 framing:
    // Short frame: flags(1) + size(1-byte u8)
    // Long frame:  flags(1) | ZMTP_FLAG_LONG + size(8-byte u64)
    // Note: ZMTP_FLAG_COMMAND is OR'd into the flags byte.
    // ZMTP_FLAG_LONG is also OR'd if it's a long message.

    if data_size <= 255 {
      // Max payload size for a short frame
      dst.reserve(1 + 1); // 1 byte for flags, 1 byte for u8 length
      dst.put_u8(zmtp_flags_byte); // Combined flags (MORE, COMMAND)
      dst.put_u8(data_size as u8);
    } else {
      zmtp_flags_byte |= ZMTP_FLAG_LONG; // Set the LONG flag bit
      dst.reserve(1 + 8); // 1 byte for flags, 8 bytes for u64 length
      dst.put_u8(zmtp_flags_byte); // Combined flags (MORE, COMMAND, LONG)
      dst.put_u64(data_size as u64); // Message length as u64
    }
    Ok(())
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
      dst.reserve(9 + size);
      dst.put_u8(zmtp_flags);
      dst.put_u64(size as u64);
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
    if let Some(prefix) = self.prefix_bytes.take() {
      if !prefix.is_empty() {
        tracing::trace!(
          prefix_len = prefix.len(),
          src_len_before = src.len(),
          "ZmtpCodec::decode: Prepending stored prefix bytes to src buffer"
        );
        // Ensure src has enough capacity. This might reallocate.
        src.reserve(prefix.len());
        // Efficiently prepend: copy current src content to after prefix, then put prefix at start
        let original_src_content = src.split(); // Empties src, returns its content
        src.put(prefix); // Put prefix first
        src.put(original_src_content); // Append original content
        tracing::trace!(
          src_len_after = src.len(),
          "ZmtpCodec::decode: Finished prepending prefix."
        );
      }
    }

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
