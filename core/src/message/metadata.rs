use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr; // Example key type
use std::sync::Arc;

/// A type map for associating arbitrary typed data with a `Msg`.
#[derive(Default, Clone)]
pub struct Metadata {
  // Use Arc for cheap cloning of metadata map itself
  inner: Arc<tokio::sync::RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>,
}

impl Metadata {
  /// Creates an empty metadata map.
  pub fn new() -> Self {
    Self::default()
  }

  /// Inserts a typed value into the map.
  /// If the map did not have this type present, `None` is returned.
  /// If the map did have this type present, the value is updated,
  /// and the old value is returned.
  pub async fn insert_typed<T: Any + Send + Sync>(
    &self, // Changed to &self as inner is Arc<RwLock>
    value: T,
  ) -> Option<Arc<dyn Any + Send + Sync>> {
    let mut map = self.inner.write().await; // Async write lock
    map.insert(TypeId::of::<T>(), Arc::new(value))
  }

  /// Gets an immutable reference to a typed value if present.
  pub async fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
    let map = self.inner.read().await; // Async read lock
    map.get(&TypeId::of::<T>()).and_then(|arc_any| {
      // Use Arc::downcast for safe casting
      arc_any.clone().downcast::<T>().ok()
    })
  }

  /// Checks if a value of type T is present.
  pub async fn contains<T: Any + Send + Sync>(&self) -> bool {
    let map = self.inner.read().await;
    map.contains_key(&TypeId::of::<T>())
  }

  /// Removes a value of type T, returning it.
  pub async fn remove<T: Any + Send + Sync>(&self) -> Option<Arc<dyn Any + Send + Sync>> {
    let mut map = self.inner.write().await;
    map.remove(&TypeId::of::<T>())
  }

  /// Checks if the metadata map is empty.
  pub async fn is_empty(&self) -> bool {
    self.inner.read().await.is_empty()
  }

  /// Returns the number of entries in the metadata map.
  pub async fn len(&self) -> usize {
    self.inner.read().await.len()
  }
}

// Manual debug implementation as RwLock doesn't impl Debug well
impl fmt::Debug for Metadata {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // Best effort: just indicate presence without locking in Debug
    f.debug_struct("Metadata").finish_non_exhaustive()
  }
}

// --- Example Standard Metadata Key Types ---

/// Metadata key for the network address of the peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddress(pub SocketAddr);

/// Metadata key for the authenticated User ID (e.g., from ZAP).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ZapUserId(pub String);

// Add other standard keys as needed (e.g., RoutingId)

#[cfg(test)]
mod additional_metadata_tests {
  use super::*;
  use std::net::SocketAddr;

  #[tokio::test]
  async fn test_metadata_concurrent_type_safety() {
    let metadata = Metadata::new();

    let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
    metadata.insert_typed(PeerAddress(addr)).await;

    let meta_clone1 = metadata.clone();
    let writer_task = tokio::spawn(async move {
      for i in 0..100 {
        meta_clone1
          .insert_typed(ZapUserId(format!("user-{}", i)))
          .await;
        tokio::task::yield_now().await;
      }
    });

    let meta_clone2 = metadata.clone();
    let reader_task = tokio::spawn(async move {
      for _ in 0..100 {
        let addr_opt = meta_clone2.get::<PeerAddress>().await;
        assert!(addr_opt.is_some(), "PeerAddress should always be present");
        let _user_opt = meta_clone2.get::<ZapUserId>().await;
        tokio::task::yield_now().await;
      }
    });

    let (res_w, res_r) = tokio::join!(writer_task, reader_task);
    assert!(res_w.is_ok());
    assert!(res_r.is_ok());
  }
}
