use crate::error::ZmqError;
use crate::security::IDataCipher;
use bytes::{Bytes, BytesMut};
use dryoc::classic::crypto_box::{
  crypto_box_detached_afternm, crypto_box_open_detached_afternm, Mac, Nonce,
};
use dryoc::constants::CRYPTO_BOX_MACBYTES;
use dryoc::dryocbox::NewByteArray;
use dryoc::types::{ByteArray, MutByteArray, MutBytes, StackByteArray};

/// Implements the IDataCipher trait for CurveZMQ's data-phase encryption.
#[derive(Debug)]
pub(crate) struct CurveDataCipher {
  // Pre-computed shared key derived from the handshake.
  precomputed_key: [u8; 32],

  // Nonces for data-phase messages.
  // Note: ZMTP's Curve security layer uses a 64-bit little-endian counter
  // for the high part of the nonce, prefixed by 'CurveZMQ' for the low part.
  send_nonce_counter: u64,
  recv_nonce_counter: u64,
}

impl CurveDataCipher {
  const NONCE_PREFIX: &'static [u8; 16] = b"CurveZMQ-Encrypt";

  /// Creates a new cipher instance with the shared key from the handshake.
  pub(crate) fn new(precomputed_key: [u8; 32]) -> Self {
    Self {
      precomputed_key,
      send_nonce_counter: 1,
      recv_nonce_counter: 1,
    }
  }

  /// Constructs a full 24-byte ZMTP CURVE nonce from a counter.
  /// Format: 16-byte prefix + 8-byte little-endian counter.
  fn construct_nonce(counter: u64) -> StackByteArray<24> {
    let mut nonce = StackByteArray::<24>::new();
    nonce.as_mut_slice()[..16].copy_from_slice(Self::NONCE_PREFIX);
    nonce.as_mut_slice()[16..].copy_from_slice(&counter.to_le_bytes());
    nonce
  }
}

impl IDataCipher for CurveDataCipher {
  fn encrypt_zmtp_frame(&mut self, plaintext_zmtp_frame: Bytes) -> Result<Bytes, ZmqError> {
    let nonce = Self::construct_nonce(self.send_nonce_counter);
    let mut mac = Mac::new_byte_array();
    let mut ciphertext = BytesMut::with_capacity(plaintext_zmtp_frame.len());
    ciphertext.resize(plaintext_zmtp_frame.len(), 0);

    crypto_box_detached_afternm(
      &mut ciphertext,
      mac.as_mut_array(),
      &plaintext_zmtp_frame,
      nonce.as_array(),
      &self.precomputed_key,
    );

    self.send_nonce_counter += 1;

    // Combine MAC and ciphertext for the final wire format
    let mut wire_frame = BytesMut::with_capacity(CRYPTO_BOX_MACBYTES + ciphertext.len());
    wire_frame.extend_from_slice(mac.as_slice());
    wire_frame.extend_from_slice(&ciphertext);

    Ok(wire_frame.freeze())
  }

  fn decrypt_wire_data_to_zmtp_frame(
    &mut self,
    encrypted_wire_data: &mut BytesMut,
  ) -> Result<Option<Bytes>, ZmqError> {
    if encrypted_wire_data.len() < CRYPTO_BOX_MACBYTES {
      // Not enough data for a full MAC, so can't be a valid message
      return Ok(None);
    }

    let nonce = Self::construct_nonce(self.recv_nonce_counter);
    let mac_slice = &encrypted_wire_data[..CRYPTO_BOX_MACBYTES];
    let ciphertext_slice = &encrypted_wire_data[CRYPTO_BOX_MACBYTES..];

    // Safely convert slice to Mac array type
    let mac = Mac::try_from(mac_slice)
      .map_err(|_| ZmqError::InvalidMessage("Invalid MAC length in frame".into()))?;

    let mut decrypted = BytesMut::with_capacity(ciphertext_slice.len());
    decrypted.resize(ciphertext_slice.len(), 0);

    crypto_box_open_detached_afternm(
      &mut decrypted,
      mac.as_array(),
      ciphertext_slice,
      nonce.as_array(),
      &self.precomputed_key,
    )
    .map_err(|e| ZmqError::AuthenticationFailure(e.to_string()))?;

    self.recv_nonce_counter += 1;

    // We have successfully processed the entire buffer. Clear it.
    encrypted_wire_data.clear();
    Ok(Some(decrypted.freeze()))
  }
}