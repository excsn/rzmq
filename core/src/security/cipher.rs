use crate::error::ZmqError;
use bytes::{Bytes, BytesMut};

/// Defines operations for encrypting and decrypting full ZMTP frames
/// after a security handshake is complete.
pub trait IDataCipher: Send + Sync + 'static {
  /// Encrypts a single, fully formed ZMTP frame.
  ///
  /// # Arguments
  /// * `plaintext_zmtp_frame`: `Bytes` containing the complete ZMTP frame (header + payload)
  ///   as it would be sent if no encryption were active.
  ///
  /// # Returns
  /// `Ok(Bytes)` containing the complete encrypted message ready for wire transmission
  /// (e.g., for Noise, this includes its length prefix, encrypted ZMTP frame, and tag).
  /// `Err(ZmqError)` if encryption fails.
  fn encrypt_zmtp_frame(&mut self, plaintext_zmtp_frame: Bytes) -> Result<Bytes, ZmqError>;

  /// Attempts to decrypt one secure message from the provided buffer of encrypted wire data.
  ///
  /// # Arguments
  /// * `encrypted_wire_data`: A mutable buffer (`BytesMut`) containing bytes read directly
  ///   from the network. This method will attempt to parse one complete secure message
  ///   (e.g., a Noise message identified by its length prefix) from the beginning of this buffer.
  ///
  /// # Returns
  /// * `Ok(Some(Bytes))`: If a complete secure message was found, decrypted successfully.
  ///   The returned `Bytes` contains the plaintext ZMTP frame. The corresponding
  ///   bytes for the secure message are consumed from `encrypted_wire_data`.
  /// * `Ok(None)`: If `encrypted_wire_data` does not currently contain enough data
  ///   for a complete secure message. `encrypted_wire_data` is left unchanged.
  /// * `Err(ZmqError)`: If a decryption error (e.g., bad MAC, malformed secure message)
  ///   or other fatal error occurs. The state of `encrypted_wire_data` might be
  ///   partially consumed or left as is depending on the point of failure.
  fn decrypt_wire_data_to_zmtp_frame(&mut self, encrypted_wire_data: &mut BytesMut) -> Result<Option<Bytes>, ZmqError>;
}

// --- PassThroughDataCipher (for NULL and PLAIN) ---
#[derive(Debug, Default)]
pub(crate) struct PassThroughDataCipher;

impl IDataCipher for PassThroughDataCipher {
  fn encrypt_zmtp_frame(&mut self, plaintext_zmtp_frame: Bytes) -> Result<Bytes, ZmqError> {
    // No encryption, just pass through.
    Ok(plaintext_zmtp_frame)
  }

  fn decrypt_wire_data_to_zmtp_frame(&mut self, wire_data: &mut BytesMut) -> Result<Option<Bytes>, ZmqError> {
    // For NULL/PLAIN, the wire_data IS the ZMTP frame.
    // This cipher's job is just to "unwrap" it. Since there's no security protocol framing
    // (like Noise's length prefix), we assume the engine's ZmtpManualParser will handle
    // ZMTP framing from whatever is in wire_data.
    //
    // The engine will read bytes from the socket into its own `raw_receive_buffer`.
    // If data_cipher is Some(PassThroughDataCipher), the engine will pass its
    // `raw_receive_buffer` to this method. This method should then just give back
    // the contents of `raw_receive_buffer` for the ZmtpManualParser.
    //
    // This means the ZmtpManualParser will be the one to determine if a full ZMTP frame
    // is present in what this method returns.
    // So, if wire_data is not empty, we return all of it.
    if wire_data.is_empty() {
      Ok(None)
    } else {
      // Consume the entire buffer and return it as a single "decrypted" chunk.
      // The engine's ZmtpManualParser will then parse ZMTP frames from this chunk.
      let all_data = wire_data.split().freeze(); // Consumes all data from wire_data
      Ok(Some(all_data))
    }
  }
}
