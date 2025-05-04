// src/socket/patterns/load_balancer.rs

use std::collections::VecDeque;
use tokio::sync::Mutex; // Use Mutex for interior mutability needed by ISocket::send

/// Distributes messages to available pipes in a round-robin fashion.
#[derive(Debug, Default)]
pub struct LoadBalancer {
  // Store the pipe IDs (specifically, the IDs used to look up the
  // sending channel `AsyncSender<Msg>` in SocketCore's `pipes_tx` map).
  pipes: Mutex<VecDeque<usize>>, // VecDeque supports efficient pop_front/push_back
}

impl LoadBalancer {
  /// Creates a new, empty load balancer.
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds a pipe (by its write ID) to the set available for load balancing.
  pub async fn add_pipe(&self, pipe_write_id: usize) {
    let mut pipes_guard = self.pipes.lock().await;
    // Avoid duplicates if called multiple times for same pipe
    if !pipes_guard.contains(&pipe_write_id) {
      pipes_guard.push_back(pipe_write_id);
      tracing::trace!(pipe_id = pipe_write_id, "LoadBalancer added pipe");
    }
  }

  /// Removes a pipe (by its write ID) from the set.
  pub async fn remove_pipe(&self, pipe_write_id: usize) {
    let mut pipes_guard = self.pipes.lock().await;
    // Efficiently remove the element if present
    if let Some(pos) = pipes_guard.iter().position(|&id| id == pipe_write_id) {
      pipes_guard.remove(pos);
      tracing::trace!(pipe_id = pipe_write_id, "LoadBalancer removed pipe");
    }
  }

  /// Selects the next pipe ID for sending using round-robin.
  /// Returns `None` if no pipes are available.
  ///
  /// Note: This takes `&self` but modifies internal state under the Mutex.
  pub async fn get_next_pipe(&self) -> Option<usize> {
    let mut pipes_guard = self.pipes.lock().await;
    if let Some(pipe_id) = pipes_guard.pop_front() {
      // Rotate: put it back at the end
      pipes_guard.push_back(pipe_id);
      Some(pipe_id)
    } else {
      None // No available pipes
    }
  }

  /// Checks if any pipes are currently registered.
  pub async fn has_pipes(&self) -> bool {
    !self.pipes.lock().await.is_empty()
  }
}
