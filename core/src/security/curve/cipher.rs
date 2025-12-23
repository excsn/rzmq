use crate::error::ZmqError;
use crate::security::IDataCipher;
use bytes::{Bytes, BytesMut};
use dryoc::classic::crypto_box::{
  crypto_box_detached_afternm, crypto_box_open_detached_afternm, Mac, Nonce,
};
use dryoc::constants::CRYPTO_BOX_MACBYTES;
use dryoc::dryocbox::NewByteArray;
use dryoc::types::{ByteArray, MutByteArray, MutBytes, StackByteArray};

/// Implements the pure IDataCipher trait for CurveZMQ's data-phase encryption.
/// This struct is now a pure cryptographic engine and has no knowledge of ZMTP
/// framing or network buffers. It expects to be given a single, complete message
/// to encrypt or decrypt.
#[derive(Debug)]
pub(crate) struct CurveDataCipher {
  // Separate keys for RX (decode) and TX (encode)
  encode_key: [u8; 32],
  decode_key: [u8; 32],

  // Nonces for data-phase messages.
  send_nonce_counter: u64,
  recv_nonce_counter: u64,
}

impl CurveDataCipher {
  const NONCE_PREFIX: &'static [u8; 16] = b"CurveZMQ-Encrypt";

  /// Creates a new cipher instance with distinct keys for encoding and decoding.
  pub(crate) fn new(encode_key: [u8; 32], decode_key: [u8; 32]) -> Self {
    Self {
      encode_key,
      decode_key,
      send_nonce_counter: 1,
      recv_nonce_counter: 1,
    }
  }

  /// Constructs a full 24-byte ZMTP CURVE nonce from a counter.
  fn construct_nonce(counter: u64) -> StackByteArray<24> {
    let mut nonce = StackByteArray::<24>::new();
    nonce.as_mut_slice()[..16].copy_from_slice(Self::NONCE_PREFIX);
    nonce.as_mut_slice()[16..].copy_from_slice(&counter.to_le_bytes());
    nonce
  }
}

impl IDataCipher for CurveDataCipher {
  /// Encrypts a single block of plaintext (e.g., one or more ZMTP frames).
  /// Returns a Vec containing [MAC][Ciphertext].
  fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, ZmqError> {
    let nonce = Self::construct_nonce(self.send_nonce_counter);
    let mut mac = Mac::new_byte_array();
    let mut ciphertext = vec![0u8; plaintext.len()];

    crypto_box_detached_afternm(
      &mut ciphertext,
      mac.as_mut_array(),
      plaintext,
      nonce.as_array(),
      &self.encode_key, // Use the encode key
    );

    self.send_nonce_counter += 1;

    // Combine MAC and ciphertext for the final wire format.
    let mut wire_frame = Vec::with_capacity(CRYPTO_BOX_MACBYTES + ciphertext.len());
    wire_frame.extend_from_slice(mac.as_slice());
    wire_frame.extend_from_slice(&ciphertext);

    Ok(wire_frame)
  }

  /// Decrypts a single block of ciphertext, which MUST be in the format [MAC][Ciphertext].
  /// Returns the plaintext Vec<u8> on success.
  fn decrypt(&mut self, ciphertext_with_mac: &[u8]) -> Result<Vec<u8>, ZmqError> {
    if ciphertext_with_mac.len() < CRYPTO_BOX_MACBYTES {
        return Err(ZmqError::InvalidMessage(
            "Ciphertext too short to contain a MAC".into(),
        ));
    }

    let nonce = Self::construct_nonce(self.recv_nonce_counter);
    let mac_slice = &ciphertext_with_mac[..CRYPTO_BOX_MACBYTES];
    let ciphertext_only_slice = &ciphertext_with_mac[CRYPTO_BOX_MACBYTES..];

    let mac = Mac::try_from(mac_slice)
      .map_err(|_| ZmqError::InvalidMessage("Invalid MAC length in frame".into()))?;

    let mut decrypted = vec![0u8; ciphertext_only_slice.len()];

    crypto_box_open_detached_afternm(
      &mut decrypted,
      mac.as_array(),
      ciphertext_only_slice,
      nonce.as_array(),
      &self.decode_key, // Use the decode key
    )
    .map_err(|e| ZmqError::AuthenticationFailure(e.to_string()))?;

    self.recv_nonce_counter += 1;

    Ok(decrypted)
  }
}