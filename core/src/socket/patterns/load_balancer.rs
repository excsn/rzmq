use crate::ZmqError;
use crate::socket::connection_iface::ISocketConnection;
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct Peer {
  pub uri: String,
  pub iface: Arc<dyn ISocketConnection>,
}

/// Distributes access to available connections in a round-robin fashion.
pub(crate) struct LoadBalancer {
  /// Available connections. Writes only on add/remove; the send hot path
  /// takes a shared read lock.
  peers: RwLock<Vec<Arc<Peer>>>,
  /// Monotonic round-robin cursor; `fetch_add % len` selects the next peer,
  /// so selection needs no exclusive lock.
  next_idx: AtomicUsize,
  notify_waiters: Arc<Notify>,
  deactivated: AtomicBool,
}

impl std::fmt::Debug for LoadBalancer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("LoadBalancer")
      .field("peer_count", &self.peers.read().len())
      .finish()
  }
}

impl Default for LoadBalancer {
  fn default() -> Self {
    Self {
      peers: RwLock::new(Vec::new()),
      next_idx: AtomicUsize::new(0),
      notify_waiters: Arc::new(Notify::new()),
      deactivated: AtomicBool::new(false),
    }
  }
}

impl LoadBalancer {
  /// Creates a new, empty load balancer.
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds a connection to the set available for load balancing.
  pub fn add_connection(&self, endpoint_uri: String, iface: Arc<dyn ISocketConnection>) {
    let mut peers = self.peers.write();
    if !peers.iter().any(|p| p.uri == endpoint_uri) {
      peers.push(Arc::new(Peer {
        uri: endpoint_uri.clone(),
        iface,
      }));
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer added connection");
      self.notify_waiters.notify_waiters();
    } else {
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer: Connection already present, not adding again.");
    }
  }

  /// Removes a connection (by its endpoint URI) from the set.
  ///
  /// The round-robin cursor is not adjusted: selection is `cursor % len`, so a
  /// membership change perturbs the rotation by at most one slot.
  pub fn remove_connection(&self, endpoint_uri: &str) {
    let mut peers = self.peers.write();
    if let Some(pos) = peers.iter().position(|p| p.uri == endpoint_uri) {
      peers.remove(pos);
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer removed connection");
    } else {
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer: Connection not found for removal.");
    }
  }

  /// Selects the next connection for sending using round-robin.
  /// Returns `None` if no connections are available.
  pub fn get_next_connection(&self) -> Option<Arc<Peer>> {
    let peers = self.peers.read();
    let len = peers.len();
    if len == 0 {
      return None;
    }

    // Lock-free rotation: the shared read lock only guards the Vec itself.
    let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % len;

    // Cheap atomic increment on the Arc instead of a deep String clone
    Some(Arc::clone(&peers[idx]))
  }

  /// Waits until at least one connection is available in the balancer.
  pub async fn wait_for_connection(&self) -> Result<(), ZmqError> {
    let notify = self.notify_waiters.clone();
    loop {
      if self.deactivated.load(Ordering::Acquire) {
        return Err(ZmqError::InvalidState("Socket closed".into()));
      }
      if !self.peers.read().is_empty() {
        return Ok(());
      }
      notify.notified().await;
    }
  }

  /// Checks if any connections are currently registered.
  pub fn has_connections(&self) -> bool {
    !self.peers.read().is_empty()
  }

  /// Returns the current number of connections being managed by the load balancer.
  pub fn connection_count(&self) -> usize {
    self.peers.read().len()
  }

  /// Deactivates the load balancer and unblocks all waiting senders.
  pub fn deactivate(&self) {
    self.deactivated.store(true, Ordering::Release);
    self.notify_waiters.notify_waiters();
  }
}
