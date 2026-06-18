use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::protocol::zmtp::{ZmtpCodec, manual_parser::ZmtpManualParser};
use crate::security::IDataCipher;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

/// Handles the framing, parsing, and crypto for secure messages from a raw byte stream.
pub(crate) trait ISecureFramer: Send + Sync + 'static {
  /// Attempts to read one complete, decrypted ZMTP message from the network buffer.
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError>;

  /// Frames and encrypts a multi-part ZMTP message into a single byte buffer for the wire.
  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError>;

  /// Frames and coalesces a batch of multiple logical ZMQ messages into a single wire buffer.
  /// This eliminates per-message allocation and enables single-syscall transmission.
  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError>;

  /// Returns `true` when the framer performs no encryption — wire bytes ARE the message bytes.
  /// `false` (default) gates off the zero-copy lease path for encrypted connections.
  fn is_passthrough(&self) -> bool {
    false
  }

  /// Frames a single Msg and returns `(header, Some(payload))` for vectored I/O, the
  /// header contains only the ZMTP frame prefix (≤9 bytes) and payload is a zero-copy
  /// ref to the message data. Encrypted framers fall back to `(merged_bytes, None)`.
  fn write_msg_split(&mut self, msg: Msg) -> Result<(Bytes, Option<Bytes>), ZmqError> {
    let mut fb = FrameBatch::new();
    fb.push(msg);
    let merged = self.write_msg_multipart(fb)?;
    Ok((merged, None))
  }

  /// Frames a batch of message groups and returns a flat list of owned `Bytes` objects
  /// suitable for `ZmtpWriteHalf::write_owned` — no payload copying for null framers.
  ///
  /// Layout: `[hdr₁, payload₁, hdr₂, payload₂, ...]` where empty payloads are omitted.
  /// `NullFramer` overrides this for true zero-copy (payload is an Arc refcount increment).
  /// Encrypted framers use this default which falls back to a single encrypted buffer.
  fn frame_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
    Ok(vec![self.write_msg_batch(batch)?])
  }

  /// Parse as many complete ZMTP frames as possible from an owned `Bytes` buffer and return
  /// them as a batch. Frames are returned as `Msg` objects holding sub-slices of `data`
  /// (zero-copy for `NullFramer`; decrypted copy for encrypted framers).
  ///
  /// If a frame straddles the end of `data`, the partial header bytes (max 9) are appended
  /// to `leftover` for assembly with the next incoming `Bytes`.
  fn try_read_msgs_from_bytes(
    &mut self,
    data: Bytes,
    accumulator: &mut BytesMut,
  ) -> Result<Vec<Msg>, ZmqError> {
    // Default: append to accumulator and parse via the existing try_read_msg path.
    // NullFramer overrides this for zero-copy slice parsing within `data`.
    accumulator.extend_from_slice(&data);
    let mut msgs = Vec::new();
    while let Some(msg) = self.try_read_msg(accumulator)? {
      msgs.push(msg);
    }
    Ok(msgs)
  }
}

pub(crate) struct NullFramer {
  parser: ZmtpManualParser,
  coalesce_buffer: BytesMut,
  header_slab: BytesMut,
}

impl NullFramer {
  pub(crate) fn new(max_msg_size: i64) -> Self {
    Self {
      parser: ZmtpManualParser::new(max_msg_size),
      coalesce_buffer: BytesMut::with_capacity(65536),
      header_slab: BytesMut::with_capacity(4096),
    }
  }
}

impl ISecureFramer for NullFramer {
  fn is_passthrough(&self) -> bool {
    true
  }

  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    self.parser.decode_from_buffer(network_buffer)
  }

  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError> {
    let mut codec = ZmtpCodec::new();
    let mut buffer = BytesMut::new();
    for msg in msgs {
      codec.encode(msg, &mut buffer)?;
    }
    Ok(buffer.freeze())
  }

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    self.coalesce_buffer.clear(); // Important for anti corruption

    // 1. Pre-calculate required capacity to do exactly ONE allocation per batch
    let mut total_len = 0;
    for msgs in batch {
      for msg in msgs {
        let len = msg.size();
        total_len += if len <= 255 { 2 + len } else { 9 + len };
      }
    }
    self.coalesce_buffer.reserve(total_len);

    // 2. Direct byte-writing loop (bypassing ZmtpCodec and Msg::clone entirely)
    for msgs in batch {
      for msg in msgs {
        let data = msg.data().unwrap_or(&[]);
        let size = data.len();
        let flags = msg.flags();

        let mut zmtp_flags = 0u8;
        if flags.contains(MsgFlags::MORE) {
          zmtp_flags |= crate::protocol::zmtp::command::ZMTP_FLAG_MORE;
        }
        if flags.contains(MsgFlags::COMMAND) {
          zmtp_flags |= crate::protocol::zmtp::command::ZMTP_FLAG_COMMAND;
        }

        if size <= 255 {
          self.coalesce_buffer.put_u8(zmtp_flags);
          self.coalesce_buffer.put_u8(size as u8);
        } else {
          zmtp_flags |= crate::protocol::zmtp::command::ZMTP_FLAG_LONG;
          self.coalesce_buffer.put_u8(zmtp_flags);
          self.coalesce_buffer.put_u64(size as u64);
        }
        self.coalesce_buffer.put_slice(data);
      }
    }
    Ok(self.coalesce_buffer.split().freeze())
  }

  fn write_msg_split(&mut self, msg: Msg) -> Result<(Bytes, Option<Bytes>), ZmqError> {
    let payload = msg.data_bytes().unwrap_or_default();
    let payload_len = payload.len();
    let is_more = msg.flags().contains(MsgFlags::MORE);

    // Build ZMTP frame header: 2 bytes (short) or 9 bytes (long)
    let mut hdr = BytesMut::with_capacity(9);
    if payload_len <= 255 {
      hdr.put_u8(if is_more { 0x01 } else { 0x00 }); // flags: MORE | (no LONG)
      hdr.put_u8(payload_len as u8);
    } else {
      hdr.put_u8(if is_more { 0x03 } else { 0x02 }); // flags: LONG (+ optional MORE)
      hdr.put_u64(payload_len as u64);
    }

    Ok((hdr.freeze(), Some(payload)))
  }

  fn frame_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
    let total_msgs: usize = batch.iter().map(|g| g.len()).sum();
    let mut out = Vec::with_capacity(total_msgs * 2);
    for group in batch {
      for msg in group {
        let payload = msg.data_bytes().unwrap_or_default();
        let payload_len = payload.len();
        let is_more = msg.flags().contains(MsgFlags::MORE);

        // Carve ZMTP frame header from the slab — one allocation per slab refill, not per message.
        self.header_slab.reserve(9);
        if payload_len <= 255 {
          self.header_slab.put_u8(if is_more { 0x01 } else { 0x00 });
          self.header_slab.put_u8(payload_len as u8);
        } else {
          self.header_slab.put_u8(if is_more { 0x03 } else { 0x02 });
          self.header_slab.put_u64(payload_len as u64);
        }
        out.push(self.header_slab.split().freeze());

        if !payload.is_empty() {
          out.push(payload); // zero-copy: Arc refcount increment from Msg
        }
      }
    }
    Ok(out)
  }
}

pub(crate) struct LengthPrefixedFramer {
  cipher: Box<dyn IDataCipher>,
  parser: ZmtpManualParser,
  decrypted_buffer: BytesMut,
  coalesce_buffer: BytesMut,
}

impl LengthPrefixedFramer {
  pub(crate) fn new(cipher: Box<dyn IDataCipher>, max_msg_size: i64) -> Self {
    Self {
      cipher,
      parser: ZmtpManualParser::new(max_msg_size),
      decrypted_buffer: BytesMut::with_capacity(65536 * 2),
      coalesce_buffer: BytesMut::with_capacity(65536),
    }
  }
}

impl ISecureFramer for LengthPrefixedFramer {
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    loop {
      // First, try to parse a ZMTP message from any already-decrypted data.
      if let Some(msg) = self.parser.decode_from_buffer(&mut self.decrypted_buffer)? {
        return Ok(Some(msg));
      }

      // If not enough decrypted data, try to decrypt a new secure frame from the network.
      if network_buffer.len() < 2 {
        return Ok(None); // Not enough data for length prefix.
      }

      let len = network_buffer.as_ref().get_u16() as usize;
      if network_buffer.len() < 2 + len {
        return Ok(None); // Not enough data for the full secure frame.
      }

      // We have a full frame, so consume and decrypt it.
      network_buffer.advance(2); // Consume length prefix
      let encrypted_frame = network_buffer.split_to(len);

      let plaintext = self.cipher.decrypt(&encrypted_frame)?;
      self.decrypted_buffer.extend_from_slice(&plaintext);

      // Loop immediately to try parsing from the newly added plaintext.
    }
  }

  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError> {
    let mut codec = ZmtpCodec::new();
    let mut plaintext_buffer = BytesMut::new();
    for msg in msgs {
      codec.encode(msg, &mut plaintext_buffer)?;
    }

    let ciphertext = self.cipher.encrypt(&plaintext_buffer)?;

    let mut final_buffer = BytesMut::with_capacity(2 + ciphertext.len());
    final_buffer.put_u16(ciphertext.len() as u16);
    final_buffer.extend_from_slice(&ciphertext);

    Ok(final_buffer.freeze())
  }

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    self.coalesce_buffer.clear();
    let mut codec = ZmtpCodec::new();
    for msgs in batch {
      for msg in msgs {
        codec.encode(msg.clone(), &mut self.coalesce_buffer)?;
      }
    }
    let ciphertext = self.cipher.encrypt(&self.coalesce_buffer)?;
    let mut out = BytesMut::with_capacity(2 + ciphertext.len());
    out.put_u16(ciphertext.len() as u16);
    out.extend_from_slice(&ciphertext);
    Ok(out.freeze())
  }
}
