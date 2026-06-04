use crate::message::flags::MsgFlags;
use crate::message::metadata::Metadata;
use bytes::Bytes;
use std::fmt;

/// Represents a single message part (frame).
#[derive(Clone, Default)]
pub struct Msg {
  // Use Bytes for efficient slicing and cloning (reference counted)
  data: Option<Bytes>,
  flags: MsgFlags,
  metadata: Metadata, // Cloning Metadata is cheap (Arc)
}

impl Msg {
  /// Creates an empty message with no data.
  pub fn new() -> Self {
    Self::default()
  }

  /// Creates a message from a `Vec<u8>`, taking ownership.
  pub fn from_vec(data: Vec<u8>) -> Self {
    Self {
      data: Some(Bytes::from(data)),
      ..Default::default()
    }
  }

  /// Creates a message from `bytes::Bytes`.
  pub fn from_bytes(data: Bytes) -> Self {
    Self {
      data: Some(data),
      ..Default::default()
    }
  }

  /// Creates a message from a static byte slice (zero-copy).
  pub fn from_static(data: &'static [u8]) -> Self {
    Self {
      data: Some(Bytes::from_static(data)),
      ..Default::default()
    }
  }

  /// Returns a reference to the message payload bytes, if any.
  pub fn data(&self) -> Option<&[u8]> {
    self.data.as_deref()
  }

  /// Returns the size of the message payload in bytes.
  pub fn size(&self) -> usize {
    self.data.as_ref().map_or(0, |d| d.len())
  }

  /// Returns the flags associated with the message.
  pub fn flags(&self) -> MsgFlags {
    self.flags
  }

  /// Sets the flags for the message (e.g., `MsgFlags::MORE`).
  pub fn set_flags(&mut self, flags: MsgFlags) {
    self.flags = flags;
  }

  /// Returns an immutable reference to the message metadata map.
  pub fn metadata(&self) -> &Metadata {
    &self.metadata
  }

  /// Returns a mutable reference to the message metadata map.
  /// Note: Modifying metadata requires awaiting async lock internally.
  pub fn metadata_mut(&mut self) -> &mut Metadata {
    &mut self.metadata
  }

  // --- Flag Helpers ---

  /// Checks if the `MORE` flag is set.
  pub fn is_more(&self) -> bool {
    self.flags.contains(MsgFlags::MORE)
  }

  /// Checks if the `COMMAND` flag is set.
  pub fn is_command(&self) -> bool {
    self.flags.contains(MsgFlags::COMMAND)
  }

  /// Returns the internal `Bytes` object if data is present.
  ///
  /// This is useful for operations that need to take ownership or a clone
  /// of the underlying `Bytes` object, such as for zerocopy send operations.
  /// Cloning `Bytes` is cheap as it is reference-counted.
  pub fn data_bytes(&self) -> Option<Bytes> {
    self.data.clone()
  }
}

#[cfg(test)]
mod additional_msg_tests {
  use super::*;

  #[test]
  fn test_msg_zero_copy_clone() {
    let payload = bytes::Bytes::from_static(b"shared-payload");
    let msg_a = Msg::from_bytes(payload.clone());
    let msg_b = msg_a.clone();

    // Bytes is reference-counted: both messages share the same allocation.
    assert_eq!(
      msg_a.data().unwrap().as_ptr(),
      msg_b.data().unwrap().as_ptr()
    );
    assert_eq!(msg_a.size(), msg_b.size());
  }

  #[test]
  fn test_msg_flag_mutations() {
    let mut msg = Msg::new();
    assert!(!msg.is_more());
    assert!(!msg.is_command());

    msg.set_flags(MsgFlags::MORE);
    assert!(msg.is_more());
    assert!(!msg.is_command());

    msg.set_flags(MsgFlags::COMMAND);
    assert!(!msg.is_more());
    assert!(msg.is_command());
  }

  #[test]
  fn test_msg_empty_payload() {
    let msg = Msg::new();
    assert_eq!(msg.size(), 0);
    assert!(msg.data().is_none());
    assert!(msg.data_bytes().is_none());
  }
}

impl fmt::Debug for Msg {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("Msg")
      .field("size", &self.size())
      .field("flags", &self.flags)
      .field("data", &self.data().map(|d| format!("{} bytes", d.len()))) // Avoid printing large data
      .field("metadata", &self.metadata) // Relies on Metadata::Debug
      .finish()
  }
}
