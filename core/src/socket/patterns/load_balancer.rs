use crate::ZmqError;
use crate::socket::connection_iface::ISocketConnection;
use parking_lot::Mutex;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Clone)]
pub(crate) struct Peer {
  pub uri: String,
  pub iface: Arc<dyn ISocketConnection>,
}

struct BalancerState {
  peers: Vec<Arc<Peer>>,
  next_idx: usize,
}

/// Distributes access to available connections in a round-robin fashion.
pub(crate) struct LoadBalancer {
  /// Stores the available connections and current round-robin index.
  state: Mutex<BalancerState>,
  notify_waiters: Arc<Notify>,
  deactivated: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for LoadBalancer {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("LoadBalancer")
      .field("peer_count", &self.state.lock().peers.len())
      .finish()
  }
}

impl Default for LoadBalancer {
  fn default() -> Self {
    Self {
      state: Mutex::new(BalancerState {
        peers: Vec::new(),
        next_idx: 0,
      }),
      notify_waiters: Arc::new(Notify::new()),
      deactivated: std::sync::atomic::AtomicBool::new(false),
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
    let mut state = self.state.lock();
    if !state.peers.iter().any(|p| p.uri == endpoint_uri) {
      state.peers.push(Arc::new(Peer {
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
  pub fn remove_connection(&self, endpoint_uri: &str) {
    let mut state = self.state.lock();
    if let Some(pos) = state.peers.iter().position(|p| p.uri == endpoint_uri) {
      state.peers.remove(pos);

      // Adjust the index so we don't skip the next peer or go out of bounds
      if pos < state.next_idx && state.next_idx > 0 {
        state.next_idx -= 1;
      } else if state.next_idx >= state.peers.len() {
        state.next_idx = 0;
      }
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer removed connection");
    } else {
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer: Connection not found for removal.");
    }
  }

  /// Selects the next connection for sending using round-robin.
  /// Returns `None` if no connections are available.
  pub fn get_next_connection(&self) -> Option<Arc<Peer>> {
    let mut state = self.state.lock();
    let len = state.peers.len();
    if len == 0 {
      return None;
    }

    // Bounds check to ensure safety if list shrank
    if state.next_idx >= len {
      state.next_idx = 0;
    }

    // Cheap atomic increment on the Arc instead of a deep String clone
    let peer = Arc::clone(&state.peers[state.next_idx]);

    // Advance the round-robin index
    state.next_idx = (state.next_idx + 1) % len;

    Some(peer)
  }

  /// Waits until at least one connection is available in the balancer.
  pub async fn wait_for_connection(&self) -> Result<(), ZmqError> {
    let notify = self.notify_waiters.clone();
    loop {
      if self.deactivated.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ZmqError::InvalidState("Socket closed".into()));
      }
      if !self.state.lock().peers.is_empty() {
        return Ok(());
      }
      notify.notified().await;
    }
  }

  /// Checks if any connections are currently registered.
  pub fn has_connections(&self) -> bool {
    !self.state.lock().peers.is_empty()
  }

  /// Returns the current number of connections being managed by the load balancer.
  pub fn connection_count(&self) -> usize {
    self.state.lock().peers.len()
  }

  /// Deactivates the load balancer and unblocks all waiting senders.
  pub fn deactivate(&self) {
    self
      .deactivated
      .store(true, std::sync::atomic::Ordering::Release);
    self.notify_waiters.notify_waiters();
  }
}
