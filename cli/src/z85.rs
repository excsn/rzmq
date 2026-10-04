//! Z85 (ZeroMQ RFC 32): 4 bytes to 5 printable characters, the text form of
//! CURVE keys that libzmq's curve_keygen prints and zmq_z85_decode reads. A copy
//! of rzmq::security::z85, kept here so the cli does not depend on the library.

const ALPHABET: &[u8; 85] =
  b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

/// Encodes a byte slice whose length is a multiple of 4. `None` otherwise.
pub fn encode(bytes: &[u8]) -> Option<String> {
  if bytes.len() % 4 != 0 {
    return None;
  }
  let mut out = String::with_capacity(bytes.len() / 4 * 5);
  for chunk in bytes.chunks_exact(4) {
    let mut value = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
    let mut digits = [0u8; 5];
    for d in digits.iter_mut().rev() {
      *d = ALPHABET[(value % 85) as usize];
      value /= 85;
    }
    out.push_str(std::str::from_utf8(&digits).unwrap());
  }
  Some(out)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rfc_32_test_vector() {
    assert_eq!(encode(&[0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B]).unwrap(), "HelloWorld");
  }

  #[test]
  fn key_is_forty_characters() {
    assert_eq!(encode(&[7u8; 32]).unwrap().len(), 40);
    assert!(encode(&[1, 2, 3]).is_none());
  }
}
