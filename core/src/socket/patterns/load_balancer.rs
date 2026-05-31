use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Notify;
use crate::ZmqError;

/// Distributes access to available connections in a round-robin fashion.
#[derive(Debug)]
pub(crate) struct LoadBalancer {
  /// Stores the endpoint URIs of available connections.
  connection_uris: Mutex<VecDeque<String>>,
  notify_waiters: Arc<Notify>,
  deactivated: std::sync::atomic::AtomicBool,
}

impl Default for LoadBalancer {
  fn default() -> Self {
    Self {
      connection_uris: Mutex::new(VecDeque::new()),
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

  /// Adds a connection (by its endpoint URI) to the set available for load balancing.
  pub fn add_connection(&self, endpoint_uri: String) {
    let mut uris_guard = self.connection_uris.lock();
    if !uris_guard.contains(&endpoint_uri) {
      uris_guard.push_back(endpoint_uri.clone()); // Store the clone
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer added connection URI");
      self.notify_waiters.notify_one();
    } else {
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer: Connection URI already present, not adding again.");
    }
  }

  /// Removes a connection (by its endpoint URI) from the set.
  pub fn remove_connection(&self, endpoint_uri: &str) {
    let mut uris_guard = self.connection_uris.lock();
    if let Some(pos) = uris_guard.iter().position(|uri| uri == endpoint_uri) {
      uris_guard.remove(pos);
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer removed connection URI");
    } else {
      tracing::trace!(uri = %endpoint_uri, "LoadBalancer: Connection URI not found for removal.");
    }
  }

  /// Selects the next connection URI for sending using round-robin.
  /// Returns `None` if no connections are available.
  /// The returned String is cloned.
  pub fn get_next_connection_uri(&self) -> Option<String> {
    let mut uris_guard = self.connection_uris.lock();
    if let Some(uri) = uris_guard.pop_front() {
      uris_guard.push_back(uri.clone()); // Rotate: put clone back at the end
      Some(uri) // Return the original popped String
    } else {
      None
    }
  }

  /// Waits until at least one connection is available in the balancer.
  pub async fn wait_for_connection(&self) -> Result<(), ZmqError> {
    let notify = self.notify_waiters.clone();
    loop {
      if self.deactivated.load(std::sync::atomic::Ordering::Acquire) {
        return Err(ZmqError::InvalidState("Socket closed".into()));
      }
      if !self.connection_uris.lock().is_empty() {
        return Ok(());
      }
      notify.notified().await;
    }
  }

  /// Checks if any connections are currently registered.
  pub fn has_connections(&self) -> bool {
    !self.connection_uris.lock().is_empty()
  }

  /// Returns the current number of connections being managed by the load balancer.
  pub async fn connection_count(&self) -> usize {
    self.connection_uris.lock().len()
  }

  /// Deactivates the load balancer and unblocks all waiting senders.
  pub fn deactivate(&self) {
    self.deactivated.store(true, std::sync::atomic::Ordering::Release);
    self.notify_waiters.notify_waiters();
  }
}