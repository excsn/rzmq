use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

/// A type map for associating arbitrary typed data with a `Msg`.
#[derive(Clone)]
pub struct Metadata {
  // OnceLock ensures zero-allocation until get_or_init is called.
  inner: OnceLock<Arc<tokio::sync::RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>>>,
}

impl Default for Metadata {
  fn default() -> Self {
    Self {
      inner: OnceLock::new(), // Zero cost, no heap allocation
    }
  }
}

impl Metadata {
  /// Creates an empty metadata map.
  pub fn new() -> Self {
    Self::default()
  }

  /// Inserts a typed value into the map.
  pub async fn insert_typed<T: Any + Send + Sync>(
    &self,
    value: T,
  ) -> Option<Arc<dyn Any + Send + Sync>> {
    // Safely lazily allocate the Arc<RwLock<HashMap>> only on the first write
    let map_arc = self
      .inner
      .get_or_init(|| Arc::new(tokio::sync::RwLock::new(HashMap::new())));

    let mut map = map_arc.write().await;
    map.insert(TypeId::of::<T>(), Arc::new(value))
  }

  /// Gets an immutable reference to a typed value if present.
  pub async fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
    let map_arc = self.inner.get()?; // Returns None instantly if uninitialized
    let map = map_arc.read().await;
    map
      .get(&TypeId::of::<T>())
      .and_then(|arc_any| arc_any.clone().downcast::<T>().ok())
  }

  /// Checks if a value of type T is present.
  pub async fn contains<T: Any + Send + Sync>(&self) -> bool {
    let map_arc = match self.inner.get() {
      Some(arc) => arc,
      None => return false,
    };
    let map = map_arc.read().await;
    map.contains_key(&TypeId::of::<T>())
  }

  /// Removes a value of type T, returning it.
  pub async fn remove<T: Any + Send + Sync>(&self) -> Option<Arc<dyn Any + Send + Sync>> {
    let map_arc = self.inner.get()?;
    let mut map = map_arc.write().await;
    map.remove(&TypeId::of::<T>())
  }

  /// Checks if the metadata map is empty.
  pub async fn is_empty(&self) -> bool {
    let map_arc = match self.inner.get() {
      Some(arc) => arc,
      None => return true,
    };
    map_arc.read().await.is_empty()
  }

  /// Returns the number of entries in the metadata map.
  pub async fn len(&self) -> usize {
    let map_arc = match self.inner.get() {
      Some(arc) => arc,
      None => return 0,
    };
    map_arc.read().await.len()
  }
}

impl fmt::Debug for Metadata {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    if self.inner.get().is_some() {
      f.debug_struct("Metadata")
        .field("initialized", &true)
        .finish_non_exhaustive()
    } else {
      f.debug_struct("Metadata")
        .field("initialized", &false)
        .finish_non_exhaustive()
    }
  }
}

// --- Standard Metadata Key Types ---
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerAddress(pub SocketAddr);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ZapUserId(pub String);

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
