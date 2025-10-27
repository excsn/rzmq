use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::{ZmtpCodec, manual_parser::ZmtpManualParser};
use crate::security::IDataCipher;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::Encoder;

/// Handles the framing, parsing, and crypto for secure messages from a raw byte stream.
pub(crate) trait ISecureFramer: Send + Sync + 'static {
  /// Attempts to read one complete, decrypted ZMTP message from the network buffer.
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError>;

  /// Frames and encrypts a multi-part ZMTP message into a single byte buffer for the wire.
  fn write_msg_multipart(&mut self, msgs: Vec<Msg>) -> Result<Bytes, ZmqError>;
}

pub(crate) struct NullFramer {
  parser: ZmtpManualParser,
}

impl NullFramer {
  pub(crate) fn new() -> Self {
    Self {
      parser: ZmtpManualParser::new(),
    }
  }
}

impl ISecureFramer for NullFramer {
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    self.parser.decode_from_buffer(network_buffer)
  }

  fn write_msg_multipart(&mut self, msgs: Vec<Msg>) -> Result<Bytes, ZmqError> {
    let mut codec = ZmtpCodec::new();
    let mut buffer = BytesMut::new();
    for msg in msgs {
      codec.encode(msg, &mut buffer)?;
    }
    Ok(buffer.freeze())
  }
}

pub(crate) struct LengthPrefixedFramer {
  cipher: Box<dyn IDataCipher>,
  parser: ZmtpManualParser,
  decrypted_buffer: BytesMut,
}

impl LengthPrefixedFramer {
  pub(crate) fn new(cipher: Box<dyn IDataCipher>) -> Self {
    Self {
      cipher,
      parser: ZmtpManualParser::new(),
      decrypted_buffer: BytesMut::with_capacity(65536 * 2),
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

  fn write_msg_multipart(&mut self, msgs: Vec<Msg>) -> Result<Bytes, ZmqError> {
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
}
