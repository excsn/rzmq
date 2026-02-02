use crate::error::ZmqError;
use bytes::{BufMut, BytesMut};
use std::convert::TryInto;
use tracing;

// --- Constants ---
pub const GREETING_LENGTH: usize = 64;
pub const MECHANISM_LENGTH: usize = 20;

// ZMTP version this implementation sends and expects from peers.
pub const GREETING_VERSION_MAJOR_BYTE: u8 = 0x03;
pub const GREETING_VERSION_MINOR_BYTE: u8 = 0x00;

// Byte offsets within the 64-byte greeting.
const VERSION_MAJOR_OFFSET: usize = 10;
const VERSION_MINOR_OFFSET: usize = 11;
pub const MECHANISM_OFFSET: usize = 12;
pub const AS_SERVER_OFFSET: usize = MECHANISM_OFFSET + MECHANISM_LENGTH; // 32
const PADDING_OFFSET: usize = AS_SERVER_OFFSET + 1; // 33
const PADDING_LENGTH: usize = GREETING_LENGTH - PADDING_OFFSET; // 31

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
    buffer.put_u8(0xFF);
    buffer.put_bytes(0, 8); // Bytes 1-8 are zeroed
    buffer.put_u8(0x7F); // Byte 9 is the final marker

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
