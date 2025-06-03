use crate::error::ZmqError;
use bytes::{BufMut, BytesMut};
use std::convert::TryInto;

pub const GREETING_PREFIX: &[u8; 10] = &[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x7F];
pub const GREETING_VERSION_MAJOR: u8 = 3;
pub const GREETING_VERSION_MINOR: u8 = 0; // We send 3.0, but accept 3.x (specifically 3.1 from peers often)
pub const GREETING_VERSION_MAJOR_BYTE: u8 = 0x03; // Actual byte value for ZMTP 3.x major
pub const GREETING_VERSION_MINOR_BYTE: u8 = 0x00; // Actual byte value for ZMTP x.0 (e.g., 3.0)
                                                  // Or 0x01 for ZMTP x.1 if rzmq aims for 3.1

pub const GREETING_LENGTH: usize = 64;
pub const MECHANISM_OFFSET: usize = GREETING_PREFIX.len() + 2; // Starts after prefix(10) + version(1)
pub const MECHANISM_LENGTH: usize = 20;
pub const AS_SERVER_OFFSET: usize = MECHANISM_OFFSET + MECHANISM_LENGTH; // After mechanism

// Validate that the reserved/padding area (last 31 bytes) is all zeros, as per typical implementations.
// The ZMTP spec says these are "reserved for future use and MUST be zero." (RFC 23, Section 3.1)
// Offset of padding: 10 (sig) + 1 (major) + 1 (minor) + 20 (mech) + 1 (as-server) = 33.
// Padding length = 31. So, bytes from index 33 to 63 (inclusive).
const PADDING_OFFSET: usize = AS_SERVER_OFFSET + 1; // 32 + 1 = 33
const PADDING_LENGTH: usize = GREETING_LENGTH - PADDING_OFFSET; // 64 - 33 = 31

/// Represents the parsed content of a ZMTP/3.1 greeting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZmtpGreeting {
  pub version: (u8, u8),
  pub mechanism: [u8; MECHANISM_LENGTH], // ASCII mechanism name padded with nulls
  pub as_server: bool,
}

impl ZmtpGreeting {
  /// Creates a greeting message to be sent.
  pub fn encode(mechanism: &[u8; MECHANISM_LENGTH], as_server: bool, buffer: &mut BytesMut) {
    buffer.reserve(GREETING_LENGTH);
    buffer.put_slice(GREETING_PREFIX);
    buffer.put_u8(GREETING_VERSION_MAJOR_BYTE); // Send 0x03 for major version
    buffer.put_u8(GREETING_VERSION_MINOR_BYTE); // Send 0x00 for minor version (for ZMTP 3.0)
    buffer.put_slice(mechanism);
    buffer.put_u8(as_server as u8); // 0x00 for client, 0x01 for server
                                    // Padding (31 bytes)

    // Total Length (64) - Signature (10) - MajorV (1) - MinorV (1) - Mech (20) - AsServer (1) = Padding (31)
    let current_len = GREETING_PREFIX.len() + 2 + MECHANISM_LENGTH + 1;
    let padding_len = GREETING_LENGTH - current_len;

    if padding_len > 0 {
      // Should always be > 0
      buffer.put_bytes(0, padding_len);
    }

    debug_assert_eq!(buffer.len(), GREETING_LENGTH);
  }

  /// Parses a received greeting message.
  pub fn decode(buffer: &mut BytesMut) -> Result<Option<Self>, ZmqError> {
    if buffer.len() < GREETING_LENGTH {
      return Ok(None); // Need more data
    }

    let data = buffer.split_to(GREETING_LENGTH); // Consume the 64 bytes

    // 1. Validate Signature Prefix
    if &data[0..GREETING_PREFIX.len()] != GREETING_PREFIX {
      // Check only the first 10 bytes
      tracing::error!(
        "Invalid ZMTP greeting prefix received. Expected: {:?}, Got: {:?}",
        GREETING_PREFIX,
        &data[0..GREETING_PREFIX.len()]
      );
      // For debugging, print the full received greeting if it fails here
      tracing::debug!("Full received greeting data (64 bytes): {:?}", &data[..]);
      return Err(ZmqError::ProtocolViolation("Invalid greeting prefix".into()));
    }

    for i in 0..PADDING_LENGTH {
      if data[PADDING_OFFSET + i] != 0x00 {
        tracing::error!(
                "Invalid ZMTP greeting: Non-zero byte found in reserved padding area at offset {}. Expected 0x00, Got: {:#04x}",
                PADDING_OFFSET + i, data[PADDING_OFFSET + i]
            );
        return Err(ZmqError::ProtocolViolation("Non-zero byte in greeting padding".into()));
      }
    }

    // 2. Extract and Validate Version
    let major_version = data[GREETING_PREFIX.len()]; // 11th byte (index 10)
    let minor_version = data[GREETING_PREFIX.len() + 1]; // 12th byte (index 11)

    if major_version != GREETING_VERSION_MAJOR_BYTE {
      // Expect 0x03
      return Err(ZmqError::ProtocolViolation(format!(
        "Unsupported ZMTP major version {}.{}",
        major_version, minor_version
      )));
    }

    // rzmq can decide which minor versions it supports. Usually, being liberal is fine.
    // We are compatible with 3.x (mainly 3.1 which uses this format)
    let version = (major_version, minor_version);

    // 3. Extract Mechanism

    let mechanism_slice = &data[MECHANISM_OFFSET..MECHANISM_OFFSET + MECHANISM_LENGTH];
    let mechanism: [u8; MECHANISM_LENGTH] = mechanism_slice.try_into().unwrap();

    // 4. Extract As-Server Flag
    let as_server_byte = data[AS_SERVER_OFFSET];
    let as_server = match as_server_byte {
      0x00 => false,
      0x01 => true,
      _ => return Err(ZmqError::ProtocolViolation("Invalid as-server flag".into())),
    };

    tracing::debug!(?version, ?mechanism, as_server, "Parsed ZMTP Greeting");
    Ok(Some(Self {
      version,
      mechanism,
      as_server,
    }))
  }

  /// Helper to get mechanism name as &str (trimming nulls).
  pub fn mechanism_name(&self) -> &str {
    let first_null = self.mechanism.iter().position(|&b| b == 0).unwrap_or(MECHANISM_LENGTH);
    std::str::from_utf8(&self.mechanism[..first_null]).unwrap_or("<invalid_utf8>")
  }
}
