use crate::error::ZmqError;

/// Defines operations for encrypting and decrypting full ZMTP frames
/// after a security handshake is complete.
pub trait IDataCipher: Send + Sync + 'static {
  /// Encrypts a single, complete block of plaintext bytes.
  fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, ZmqError>;

  /// Decrypts a single, complete block of ciphertext bytes.
  fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, ZmqError>;
}

