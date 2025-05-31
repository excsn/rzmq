// core/src/socket/patterns/load_balancer.rs

use std::{collections::VecDeque, sync::Arc};
use parking_lot::Mutex;
use tokio::sync::{Notify}; // Notify remains tokio::sync

/// Distributes access to available connections in a round-robin fashion.
#[derive(Debug, Default)]
pub(crate) struct LoadBalancer {
  /// Stores the endpoint URIs of available connections.
  connection_uris: Mutex<VecDeque<String>>, 
  notify_waiters: Arc<Notify>,
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
  pub async fn wait_for_connection(&self) {
    
    let notify = self.notify_waiters.clone();
    loop {
      if !self.connection_uris.lock().is_empty() {
        return; 
      }
      notify.notified().await;
    }
  }

  /// Checks if any connections are currently registered.
  pub fn has_connections(&self) -> bool {
    
    !self.connection_uris.lock().is_empty()
  }
  
  /// Returns the current number of connections being managed by the load balancer.
  /// Note: This is an async fn because it locks the mutex. In practice, this lock
  /// is very short-lived for just getting the length. If this becomes a performance
  /// concern for extremely high-frequency calls not involving .await, alternative
  /// atomic counting could be considered, but for now, this is simplest and correct.
  pub async fn connection_count(&self) -> usize {
    self.connection_uris.lock().len()
  }
}