pub(crate) mod encoder;

use self::encoder::ZmtpFrameEncoder;

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::security::IDataCipher;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Handles the framing, parsing, and crypto for secure messages from a raw byte stream.
pub(crate) trait ISecureFramer: Send + Sync + 'static {
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError>;

  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError>;

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError>;

  fn is_passthrough(&self) -> bool {
    false
  }

  fn write_msg_split(&mut self, msg: Msg) -> Result<(Bytes, Option<Bytes>), ZmqError> {
    let mut fb = FrameBatch::new();
    fb.push(msg);
    let merged = self.write_msg_multipart(fb)?;
    Ok((merged, None))
  }

  fn frame_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
    Ok(vec![self.write_msg_batch(batch)?])
  }

  fn try_read_msgs_from_bytes(
    &mut self,
    data: Bytes,
    accumulator: &mut BytesMut,
  ) -> Result<Vec<Msg>, ZmqError> {
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
  /// Write-side framing engine — handles all ZMTP serialization and buffer reuse.
  framer: ZmtpFrameEncoder,
}

impl NullFramer {
  pub(crate) fn new(max_msg_size: i64, sndbatch_count: usize, sndbatch_bytes_physical: usize) -> Self {
    Self {
      parser: ZmtpManualParser::new(max_msg_size),
      // header_slab: worst-case 9 bytes per frame (1-byte flags + 8-byte length)
      // coalesce_buffer: sized to hold a full physical batch without reallocation
      framer: ZmtpFrameEncoder::new(sndbatch_count * 9, sndbatch_bytes_physical),
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
    self.framer.frame_contiguous(&[msgs])
  }

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    self.framer.frame_contiguous(batch)
  }

  fn write_msg_split(&mut self, msg: Msg) -> Result<(Bytes, Option<Bytes>), ZmqError> {
    let payload = msg.data_bytes().unwrap_or_default();
    let payload_len = payload.len();
    let is_more = msg.flags().contains(MsgFlags::MORE);

    let mut hdr = BytesMut::with_capacity(9);
    if payload_len <= 255 {
      hdr.put_u8(if is_more { 0x01 } else { 0x00 });
      hdr.put_u8(payload_len as u8);
    } else {
      hdr.put_u8(if is_more { 0x03 } else { 0x02 });
      hdr.put_u64(payload_len as u64);
    }

    Ok((hdr.freeze(), Some(payload)))
  }

  fn frame_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
    self.framer.frame_vectored(batch)
  }
}

pub(crate) struct LengthPrefixedFramer {
  cipher: Box<dyn IDataCipher>,
  parser: ZmtpManualParser,
  decrypted_buffer: BytesMut,
  /// Write-side framing engine — serializes plaintext before encryption.
  framer: ZmtpFrameEncoder,
}

impl LengthPrefixedFramer {
  pub(crate) fn new(cipher: Box<dyn IDataCipher>, max_msg_size: i64, sndbatch_count: usize, sndbatch_bytes_physical: usize) -> Self {
    Self {
      cipher,
      parser: ZmtpManualParser::new(max_msg_size),
      // Sized to hold a full decrypted batch without reallocation
      decrypted_buffer: BytesMut::with_capacity(sndbatch_bytes_physical),
      // header_slab: worst-case 9 bytes per frame; coalesce_buffer: full physical batch
      framer: ZmtpFrameEncoder::new(sndbatch_count * 9, sndbatch_bytes_physical),
    }
  }
}

impl ISecureFramer for LengthPrefixedFramer {
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    loop {
      if let Some(msg) = self.parser.decode_from_buffer(&mut self.decrypted_buffer)? {
        return Ok(Some(msg));
      }

      if network_buffer.len() < 2 {
        return Ok(None);
      }

      let len = network_buffer.as_ref().get_u16() as usize;
      if network_buffer.len() < 2 + len {
        return Ok(None);
      }

      network_buffer.advance(2);
      let encrypted_frame = network_buffer.split_to(len);

      let plaintext = self.cipher.decrypt(&encrypted_frame)?;
      self.decrypted_buffer.extend_from_slice(&plaintext);
    }
  }

  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError> {
    let plaintext = self.framer.frame_contiguous(&[msgs])?;
    let ciphertext = self.cipher.encrypt(&plaintext)?;
    let mut out = BytesMut::with_capacity(2 + ciphertext.len());
    out.put_u16(ciphertext.len() as u16);
    out.extend_from_slice(&ciphertext);
    Ok(out.freeze())
  }

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    let plaintext = self.framer.frame_contiguous(batch)?;
    let ciphertext = self.cipher.encrypt(&plaintext)?;
    let mut out = BytesMut::with_capacity(2 + ciphertext.len());
    out.put_u16(ciphertext.len() as u16);
    out.extend_from_slice(&ciphertext);
    Ok(out.freeze())
  }
}
