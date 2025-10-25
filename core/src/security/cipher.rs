use crate::error::ZmqError;

/// Defines operations for encrypting and decrypting full ZMTP frames
/// after a security handshake is complete.
pub trait IDataCipher: Send + Sync + 'static {
  /// Encrypts a single, complete block of plaintext bytes.
  fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, ZmqError>;

  /// Decrypts a single, complete block of ciphertext bytes.
  fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, ZmqError>;
}

// --- PassThroughDataCipher (for NULL and PLAIN) ---
/// A pure "cipher" that performs no encryption or decryption. It simply
/// passes data through unmodified, conforming to the IDataCipher trait.
/// It is used by the NullFramer.
#[derive(Debug, Default)]
pub(crate) struct PassThroughDataCipher;

impl IDataCipher for PassThroughDataCipher {
  /// "Encrypts" by cloning the plaintext into a new Vec.
  fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, ZmqError> {
    Ok(plaintext.to_vec())
  }

  /// "Decrypts" by cloning the ciphertext into a new Vec.
  fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, ZmqError> {
    Ok(ciphertext.to_vec())
  }
}
