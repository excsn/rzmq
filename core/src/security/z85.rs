//! Z85 (ZeroMQ RFC 32): 4 bytes to 5 printable characters.

const ALPHABET: &[u8; 85] =
  b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#";

fn digit_value(c: u8) -> Option<u32> {
  ALPHABET.iter().position(|&a| a == c).map(|p| p as u32)
}

/// Encodes a byte slice whose length is a multiple of 4.
#[cfg(test)]
pub(crate) fn encode(bytes: &[u8]) -> Option<String> {
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

/// Decodes a string whose length is a multiple of 5.
pub(crate) fn decode(text: &str) -> Option<Vec<u8>> {
  let bytes = text.as_bytes();
  if bytes.len() % 5 != 0 {
    return None;
  }
  let mut out = Vec::with_capacity(bytes.len() / 5 * 4);
  for chunk in bytes.chunks_exact(5) {
    let mut value: u32 = 0;
    for &c in chunk {
      value = value.checked_mul(85)?.checked_add(digit_value(c)?)?;
    }
    out.extend_from_slice(&value.to_be_bytes());
  }
  Some(out)
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn rfc_32_test_vector() {
    let bytes = [0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B];
    assert_eq!(encode(&bytes).unwrap(), "HelloWorld");
    assert_eq!(decode("HelloWorld").unwrap(), bytes);
  }

  #[test]
  fn thirty_two_byte_key_round_trips() {
    let key: Vec<u8> = (0..32).map(|i| i as u8 * 7).collect();
    let text = encode(&key).unwrap();
    assert_eq!(text.len(), 40);
    assert_eq!(decode(&text).unwrap(), key);
  }

  #[test]
  fn rejects_bad_lengths_and_characters() {
    assert!(encode(&[1, 2, 3]).is_none());
    assert!(decode("Hello").is_some());
    assert!(decode("Hell").is_none());
    assert!(decode("Hell~").is_none());
  }
}
