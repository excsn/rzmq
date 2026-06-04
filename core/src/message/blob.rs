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

#[cfg(test)]
mod additional_blob_tests {
  use super::*;
  use std::collections::hash_map::DefaultHasher;
  use std::hash::{Hash, Hasher};

  #[test]
  fn test_blob_creation_and_deref() {
    let static_data: &'static [u8] = b"static-routing-id";
    let blob_static = Blob::from_static(static_data);
    assert_eq!(&*blob_static, static_data);
    assert_eq!(blob_static.size(), static_data.len());
    assert!(!blob_static.is_empty());

    let vec_data = vec![1u8, 2, 3, 4];
    let blob_vec = Blob::from(vec_data.clone());
    assert_eq!(&*blob_vec, vec_data.as_slice());
  }

  #[test]
  fn test_blob_equality_and_hashing() {
    let blob_a = Blob::from_static(b"common-id");
    let blob_b = Blob::from(b"common-id".to_vec());

    assert_eq!(blob_a, blob_b);

    let mut hasher_a = DefaultHasher::new();
    let mut hasher_b = DefaultHasher::new();
    blob_a.hash(&mut hasher_a);
    blob_b.hash(&mut hasher_b);

    assert_eq!(hasher_a.finish(), hasher_b.finish());
  }
}
