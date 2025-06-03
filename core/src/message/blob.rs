use bytes::Bytes;
use std::fmt;
use std::ops::Deref;

/// An immutable, cheaply cloneable byte sequence (e.g., for Identities, Subscriptions).
#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Blob {
  inner: Bytes,
}

impl Blob {
  /// Creates an empty blob.
  pub fn new() -> Self {
    Self { inner: Bytes::new() }
  }

  /// Creates a blob from `bytes::Bytes`.
  pub fn from_bytes(bytes: Bytes) -> Self {
    Self { inner: bytes }
  }

  /// Creates a blob from a static byte slice.
  pub fn from_static(data: &'static [u8]) -> Self {
    Self {
      inner: Bytes::from_static(data),
    }
  }

  /// Returns the size of the blob.
  pub fn size(&self) -> usize {
    self.inner.len()
  }

  /// Returns true if the blob is empty.
  pub fn is_empty(&self) -> bool {
    self.inner.is_empty()
  }
}

impl Deref for Blob {
  type Target = [u8];
  fn deref(&self) -> &Self::Target {
    &self.inner
  }
}

impl AsRef<[u8]> for Blob {
  fn as_ref(&self) -> &[u8] {
    &self.inner
  }
}

impl From<Vec<u8>> for Blob {
  fn from(vec: Vec<u8>) -> Self {
    Self {
      inner: Bytes::from(vec),
    }
  }
}

impl From<&'static [u8]> for Blob {
  fn from(data: &'static [u8]) -> Self {
    Self::from_static(data)
  }
}

impl fmt::Debug for Blob {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Simple debug, avoid large output
    f.debug_struct("Blob").field("len", &self.inner.len()).finish()
  }
}
