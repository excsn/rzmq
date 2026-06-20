use crate::error::ZmqError;
use bytes::{BufMut, BytesMut};
use std::convert::TryInto;
use tracing;

// --- Constants ---
pub const GREETING_LENGTH: usize = 64;
pub const MECHANISM_LENGTH: usize = 20;

/// Length of the version-agnostic ZMTP signature: `0xFF` + 8 bytes + `0x7F`.
/// This is the prefix every ZMTP greeting (v1/v2/v3) starts with, used to
/// detect a peer before its protocol revision is known.
pub const SIGNATURE_LENGTH: usize = 10;

// ZMTP version this implementation sends and expects from peers.
pub const GREETING_VERSION_MAJOR_BYTE: u8 = 0x03;
pub const GREETING_VERSION_MINOR_BYTE: u8 = 0x00;

/// Protocol revision (greeting byte 10) for ZMTP/2.0.
pub const V2_REVISION: u8 = 0x01;
/// Protocol revision (greeting byte 10) for ZMTP/3.x.
pub const V3_REVISION: u8 = 0x03;

// --- ZMTP/2.0 socket-type codes (greeting byte 11). ---
// These mirror the historical libzmq socket-type IDs that ZMTP/2.0 carries as a
// single byte in the greeting (RFC 15.3 / ZMTP-2.0).
pub const V2_SOCKET_TYPE_PAIR: u8 = 0;
pub const V2_SOCKET_TYPE_PUB: u8 = 1;
pub const V2_SOCKET_TYPE_SUB: u8 = 2;
pub const V2_SOCKET_TYPE_REQ: u8 = 3;
pub const V2_SOCKET_TYPE_REP: u8 = 4;
pub const V2_SOCKET_TYPE_DEALER: u8 = 5; // historically XREQ
pub const V2_SOCKET_TYPE_ROUTER: u8 = 6; // historically XREP
pub const V2_SOCKET_TYPE_PULL: u8 = 7;
pub const V2_SOCKET_TYPE_PUSH: u8 = 8;
pub const V2_SOCKET_TYPE_XPUB: u8 = 9;
pub const V2_SOCKET_TYPE_XSUB: u8 = 10;

// Byte offsets within the 64-byte greeting.
const VERSION_MAJOR_OFFSET: usize = 10;
const VERSION_MINOR_OFFSET: usize = 11;
pub const MECHANISM_OFFSET: usize = 12;
pub const AS_SERVER_OFFSET: usize = MECHANISM_OFFSET + MECHANISM_LENGTH; // 32
const PADDING_OFFSET: usize = AS_SERVER_OFFSET + 1; // 33
const PADDING_LENGTH: usize = GREETING_LENGTH - PADDING_OFFSET; // 31

/// Writes the 10-byte version-agnostic ZMTP signature (`0xFF` + 8 zero bytes +
/// `0x7F`) into `buffer`. Shared by the staged greeting send and the full
/// `ZmtpGreeting::encode`.
pub fn encode_signature(buffer: &mut BytesMut) {
  buffer.reserve(SIGNATURE_LENGTH);
  buffer.put_u8(0xFF);
  buffer.put_bytes(0, 8);
  buffer.put_u8(0x7F);
}

/// Encodes the version-specific tail of a ZMTP/3.x greeting: everything after the
/// 10-byte signature and 1-byte revision — i.e. `minor + mechanism[20] + as_server
/// + filler[31]`. Used by the staged greeting send once the peer is confirmed v3.
pub fn encode_v3_tail(mechanism: &[u8; MECHANISM_LENGTH], as_server: bool, buffer: &mut BytesMut) {
  buffer.reserve(GREETING_LENGTH - SIGNATURE_LENGTH - 1);
  buffer.put_u8(GREETING_VERSION_MINOR_BYTE);
  buffer.put_slice(mechanism);
  buffer.put_u8(as_server as u8);
  buffer.put_bytes(0, PADDING_LENGTH);
}

/// Peeks the peer's protocol revision (greeting byte 10) from at least the first
/// `SIGNATURE_LENGTH + 1` bytes. Validates the signature markers (`0xFF` … `0x7F`)
/// before returning the revision byte.
pub fn peek_revision(buf: &[u8]) -> Result<u8, ZmqError> {
  if buf.len() < SIGNATURE_LENGTH + 1 {
    return Err(ZmqError::ProtocolViolation(
      "Greeting too short to determine protocol revision".into(),
    ));
  }
  if buf[0] != 0xFF || buf[SIGNATURE_LENGTH - 1] != 0x7F {
    return Err(ZmqError::ProtocolViolation(
      "Invalid ZMTP signature in greeting".into(),
    ));
  }
  Ok(buf[VERSION_MAJOR_OFFSET])
}

/// Maps a socket-type name (e.g. "DEALER") to its ZMTP/2.0 greeting byte.
pub fn socket_type_code(name: &str) -> Option<u8> {
  Some(match name {
    "PAIR" => V2_SOCKET_TYPE_PAIR,
    "PUB" => V2_SOCKET_TYPE_PUB,
    "SUB" => V2_SOCKET_TYPE_SUB,
    "REQ" => V2_SOCKET_TYPE_REQ,
    "REP" => V2_SOCKET_TYPE_REP,
    "DEALER" => V2_SOCKET_TYPE_DEALER,
    "ROUTER" => V2_SOCKET_TYPE_ROUTER,
    "PULL" => V2_SOCKET_TYPE_PULL,
    "PUSH" => V2_SOCKET_TYPE_PUSH,
    "XPUB" => V2_SOCKET_TYPE_XPUB,
    "XSUB" => V2_SOCKET_TYPE_XSUB,
    _ => return None,
  })
}

/// Maps a ZMTP/2.0 greeting socket-type byte back to its canonical name.
pub fn socket_type_name_from_code(code: u8) -> Option<&'static str> {
  Some(match code {
    V2_SOCKET_TYPE_PAIR => "PAIR",
    V2_SOCKET_TYPE_PUB => "PUB",
    V2_SOCKET_TYPE_SUB => "SUB",
    V2_SOCKET_TYPE_REQ => "REQ",
    V2_SOCKET_TYPE_REP => "REP",
    V2_SOCKET_TYPE_DEALER => "DEALER",
    V2_SOCKET_TYPE_ROUTER => "ROUTER",
    V2_SOCKET_TYPE_PULL => "PULL",
    V2_SOCKET_TYPE_PUSH => "PUSH",
    V2_SOCKET_TYPE_XPUB => "XPUB",
    V2_SOCKET_TYPE_XSUB => "XSUB",
    _ => return None,
  })
}

/// Represents the parsed content of a ZMTP/3.1 greeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZmtpGreeting {
  pub version: (u8, u8),
  pub mechanism: [u8; MECHANISM_LENGTH], // ASCII mechanism name padded with nulls
  pub as_server: bool,
}

impl ZmtpGreeting {
  /// Creates a canonical greeting message to be sent.
  pub fn encode(mechanism: &[u8; MECHANISM_LENGTH], as_server: bool, buffer: &mut BytesMut) {
    buffer.reserve(GREETING_LENGTH);

    // Start byte (0xFF) + 8 zero bytes (canonical reserved area) + final marker (0x7F)
    encode_signature(buffer);

    // Version bytes (at indices 10 and 11)
    buffer.put_u8(GREETING_VERSION_MAJOR_BYTE);
    buffer.put_u8(GREETING_VERSION_MINOR_BYTE);

    // Mechanism
    buffer.put_slice(mechanism);

    // As-server flag
    buffer.put_u8(as_server as u8);

    // Padding to complete 64 bytes
    let current_len = buffer.len();
    let padding_len = GREETING_LENGTH - current_len;
    if padding_len > 0 {
      buffer.put_bytes(0, padding_len);
    }

    debug_assert_eq!(buffer.len(), GREETING_LENGTH);
  }

  /// Parses a received greeting message with improved tolerance for implementation variations.
  pub fn decode(buffer: &mut BytesMut) -> Result<Option<Self>, ZmqError> {
    if buffer.len() < GREETING_LENGTH {
      return Ok(None); // Need more data
    }

    let data = buffer.split_to(GREETING_LENGTH); // Consume the 64 bytes

    // 1. Validate fixed signature markers (more robust than a full prefix match).
    // The spec guarantees the first byte is 0xFF, but the middle bytes have varied.
    // We check the first byte and can optionally check the 10th byte (0x7F), though it was also part of the issue.
    // For maximum compatibility, checking only the first byte and padding is often sufficient.
    if data[0] != 0xFF {
      tracing::error!("Greeting does not start with 0xFF (got {:#04x})", data[0]);
      return Err(ZmqError::ProtocolViolation(
        "Greeting does not start with 0xFF".into(),
      ));
    }

    // 2. Validate that the required padding area is all zeros.
    // The ZMTP spec (RFC 23, Section 3.1) requires these bytes to be zero.
    for i in 0..PADDING_LENGTH {
      let idx = PADDING_OFFSET + i;
      if data[idx] != 0x00 {
        tracing::error!(
          "Invalid ZMTP greeting: non-zero padding byte at index {}: {:#04x}",
          idx,
          data[idx]
        );
        return Err(ZmqError::ProtocolViolation(
          "Non-zero byte in greeting padding".into(),
        ));
      }
    }

    // 3. Extract and Validate Version (at fixed offsets).
    let major_version = data[VERSION_MAJOR_OFFSET];
    let minor_version = data[VERSION_MINOR_OFFSET];

    if major_version != GREETING_VERSION_MAJOR_BYTE {
      return Err(ZmqError::ProtocolViolation(format!(
        "Unsupported ZMTP major version {}.{}",
        major_version, minor_version
      )));
    }
    let version = (major_version, minor_version);

    // 4. Extract Mechanism.
    let mechanism_slice = &data[MECHANISM_OFFSET..MECHANISM_OFFSET + MECHANISM_LENGTH];
    let mechanism: [u8; MECHANISM_LENGTH] = mechanism_slice.try_into().unwrap();

    // 5. Extract As-Server Flag.
    let as_server_byte = data[AS_SERVER_OFFSET];
    let as_server = match as_server_byte {
      0x00 => false,
      0x01 => true,
      _ => return Err(ZmqError::ProtocolViolation("Invalid as-server flag".into())),
    };

    tracing::debug!(?version, mechanism_name = %std::str::from_utf8(&mechanism).unwrap_or("").trim_end_matches('\0'), as_server, "Parsed ZMTP Greeting");
    Ok(Some(Self {
      version,
      mechanism,
      as_server,
    }))
  }

  /// Helper to get mechanism name as &str (trimming nulls).
  pub fn mechanism_name(&self) -> &str {
    let first_null = self
      .mechanism
      .iter()
      .position(|&b| b == 0)
      .unwrap_or(MECHANISM_LENGTH);
    std::str::from_utf8(&self.mechanism[..first_null]).unwrap_or("<invalid_utf8>")
  }
}

#[cfg(test)]
mod additional_greeting_tests {
  use super::*;
  use bytes::BytesMut;

  #[test]
  fn test_decode_valid_greeting() {
    let mut buf = BytesMut::new();
    let mechanism = [0u8; MECHANISM_LENGTH];
    ZmtpGreeting::encode(&mechanism, true, &mut buf);

    let result = ZmtpGreeting::decode(&mut buf)
      .expect("Decoding valid greeting should succeed")
      .expect("Should return Some(ZmtpGreeting)");

    assert_eq!(result.version, (3, 0));
    assert_eq!(result.mechanism, mechanism);
    assert!(result.as_server);
  }

  #[test]
  fn test_decode_invalid_signature() {
    let mut buf = BytesMut::zeroed(64);
    buf[0] = 0x00; // Invalid: must be 0xFF

    let result = ZmtpGreeting::decode(&mut buf);
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_decode_dirty_padding() {
    let mut buf = BytesMut::new();
    let mechanism = [0u8; MECHANISM_LENGTH];
    ZmtpGreeting::encode(&mechanism, false, &mut buf);

    // Corrupt a byte in the padding zone (PADDING_OFFSET=33 to 63)
    buf[45] = 0xAA;

    let result = ZmtpGreeting::decode(&mut buf);
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_decode_unsupported_version() {
    let mut buf = BytesMut::new();
    let mechanism = [0u8; MECHANISM_LENGTH];
    ZmtpGreeting::encode(&mechanism, false, &mut buf);

    // Override major version at VERSION_MAJOR_OFFSET=10
    buf[10] = 2;

    let result = ZmtpGreeting::decode(&mut buf);
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_decode_invalid_as_server_flag() {
    let mut buf = BytesMut::new();
    let mechanism = [0u8; MECHANISM_LENGTH];
    ZmtpGreeting::encode(&mechanism, false, &mut buf);

    // Override as-server byte at AS_SERVER_OFFSET=32 to invalid value
    buf[32] = 0x02;

    let result = ZmtpGreeting::decode(&mut buf);
    assert!(matches!(result, Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_encode_signature_is_canonical() {
    let mut buf = BytesMut::new();
    encode_signature(&mut buf);
    assert_eq!(buf.len(), SIGNATURE_LENGTH);
    assert_eq!(buf[0], 0xFF);
    assert!(buf[1..9].iter().all(|&b| b == 0));
    assert_eq!(buf[9], 0x7F);
  }

  #[test]
  fn test_peek_revision_v3() {
    let mut buf = BytesMut::new();
    encode_signature(&mut buf);
    buf.put_u8(V3_REVISION);
    assert_eq!(peek_revision(&buf).unwrap(), V3_REVISION);
  }

  #[test]
  fn test_peek_revision_v2() {
    let mut buf = BytesMut::new();
    encode_signature(&mut buf);
    buf.put_u8(V2_REVISION);
    assert_eq!(peek_revision(&buf).unwrap(), V2_REVISION);
  }

  #[test]
  fn test_peek_revision_too_short() {
    let mut buf = BytesMut::new();
    encode_signature(&mut buf); // only 10 bytes, need 11
    assert!(matches!(peek_revision(&buf), Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_peek_revision_bad_signature() {
    let mut buf = BytesMut::zeroed(11);
    buf[0] = 0x00; // not 0xFF
    assert!(matches!(peek_revision(&buf), Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_socket_type_code_roundtrip() {
    for name in [
      "PAIR", "PUB", "SUB", "REQ", "REP", "DEALER", "ROUTER", "PULL", "PUSH", "XPUB", "XSUB",
    ] {
      let code = socket_type_code(name).expect("known type has a code");
      assert_eq!(socket_type_name_from_code(code), Some(name));
    }
    assert!(socket_type_code("UNKNOWN").is_none());
    assert!(socket_type_name_from_code(200).is_none());
  }

  #[test]
  fn test_encode_v3_tail_completes_greeting() {
    // signature + revision + v3 tail must reconstruct a decodable 64-byte greeting.
    let mut buf = BytesMut::new();
    encode_signature(&mut buf);
    buf.put_u8(V3_REVISION);
    encode_v3_tail(&[0u8; MECHANISM_LENGTH], true, &mut buf);
    assert_eq!(buf.len(), GREETING_LENGTH);

    let decoded = ZmtpGreeting::decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.version.0, V3_REVISION);
    assert!(decoded.as_server);
  }
}
