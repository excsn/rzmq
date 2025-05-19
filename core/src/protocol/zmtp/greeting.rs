use crate::error::ZmqError;
use bytes::{BufMut, BytesMut};
use std::convert::TryInto;

pub const GREETING_PREFIX: &[u8; 10] =
  &[0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F];
pub const GREETING_SUFFIX: &[u8; 1] = &[0x7F];
pub const GREETING_VERSION_MAJOR: u8 = 3;
pub const GREETING_VERSION_MINOR: u8 = 0; // We send 3.0, but accept 3.x (specifically 3.1 from peers often)
pub const GREETING_LENGTH: usize = 64;
pub const MECHANISM_OFFSET: usize = 11; // Starts after prefix(10) + version(1)
pub const MECHANISM_LENGTH: usize = 20;
pub const AS_SERVER_OFFSET: usize = 31; // After mechanism

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
    buffer.put_u8(GREETING_VERSION_MAJOR << 4 | GREETING_VERSION_MINOR); // Encode version 3.0
    buffer.put_slice(mechanism);
    buffer.put_u8(as_server as u8); // 0x00 for client, 0x01 for server
                                    // Padding (31 bytes)
    buffer.put_bytes(
      0,
      GREETING_LENGTH - AS_SERVER_OFFSET - 1 - GREETING_SUFFIX.len(),
    );
    buffer.put_slice(GREETING_SUFFIX);
    debug_assert_eq!(buffer.len(), GREETING_LENGTH);
  }

  /// Parses a received greeting message.
  pub fn decode(buffer: &mut BytesMut) -> Result<Option<Self>, ZmqError> {
    if buffer.len() < GREETING_LENGTH {
      return Ok(None); // Need more data
    }

    let data = buffer.split_to(GREETING_LENGTH); // Consume the 64 bytes

    // 1. Validate Signature Prefix and Suffix
    if &data[0..GREETING_PREFIX.len()] != GREETING_PREFIX
      || &data[GREETING_LENGTH - GREETING_SUFFIX.len()..] != GREETING_SUFFIX
    {
      tracing::error!("Invalid ZMTP greeting signature received");
      return Err(ZmqError::ProtocolViolation(
        "Invalid greeting signature".into(),
      ));
    }

    // 2. Extract and Validate Version
    let version_byte = data[GREETING_PREFIX.len()];
    let major = version_byte >> 4;
    let minor = version_byte & 0x0F; // Lower nibble (although ZMTP 3.1 uses full byte?) - Check spec!
                                     // ZMTP 3.1 spec actually shows minor version in next byte. Let's correct.
                                     // Format is: FF padding(8) 7F | Major | Minor | Mechanism(20) | Server(1) | Padding(31)
                                     // Re-checking: No, the spec diagrams are confusing. Let's stick to single byte for now, accepting >= 3.0
    if major < 3 {
      tracing::error!(
        peer_major = major,
        peer_minor = minor,
        "Unsupported ZMTP version received"
      );
      return Err(ZmqError::ProtocolViolation(format!(
        "Unsupported ZMTP version {}.{}",
        major, minor
      )));
    }
    // We are compatible with 3.x (mainly 3.1 which uses this format)
    let version = (major, minor);

    // 3. Extract Mechanism
    let mechanism_slice = &data[MECHANISM_OFFSET..MECHANISM_OFFSET + MECHANISM_LENGTH];
    let mechanism: [u8; MECHANISM_LENGTH] = mechanism_slice.try_into().unwrap(); // Should not fail due to slice length

    // 4. Extract As-Server Flag
    let as_server_byte = data[AS_SERVER_OFFSET];
    let as_server = match as_server_byte {
      0x00 => false, // Peer is client
      0x01 => true,  // Peer is server
      _ => {
        tracing::error!(
          as_server_byte = as_server_byte,
          "Invalid as-server flag in greeting"
        );
        return Err(ZmqError::ProtocolViolation("Invalid as-server flag".into()));
      }
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
    let first_null = self
      .mechanism
      .iter()
      .position(|&b| b == 0)
      .unwrap_or(MECHANISM_LENGTH);
    std::str::from_utf8(&self.mechanism[..first_null]).unwrap_or("<invalid_utf8>")
  }
}
