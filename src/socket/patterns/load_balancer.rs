// src/socket/patterns/load_balancer.rs

use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{Mutex, Notify}; // Use Mutex for interior mutability needed by ISocket::send

/// Distributes messages to available pipes in a round-robin fashion.
#[derive(Debug, Default)]
pub(crate) struct LoadBalancer {
  // Store the pipe IDs (specifically, the IDs used to look up the
  // sending channel `AsyncSender<Msg>` in SocketCore's `pipes_tx` map).
  pipes: Mutex<VecDeque<usize>>, // VecDeque supports efficient pop_front/push_back
  // Use Arc because multiple tasks might wait on the same notifier instance.
  notify_waiters: Arc<Notify>,
}

impl LoadBalancer {
  /// Creates a new, empty load balancer.
  pub fn new() -> Self {
    Self {
      pipes: Mutex::new(VecDeque::new()),
      notify_waiters: Arc::new(Notify::new()),
    }
  }

  /// Adds a pipe (by its write ID) to the set available for load balancing.
  pub async fn add_pipe(&self, pipe_write_id: usize) {
    let mut pipes_guard = self.pipes.lock().await;
    // Avoid duplicates if called multiple times for same pipe
    if !pipes_guard.contains(&pipe_write_id) {
      pipes_guard.push_back(pipe_write_id);
      tracing::trace!(pipe_id = pipe_write_id, "LoadBalancer added pipe");
      // Notify *after* inserting, while holding the lock might be okay here,
      // but notifying after release is generally safer if notified logic re-acquires locks.
      // Let's notify after adding but before releasing lock for simplicity.
      self.notify_waiters.notify_one(); // Wake up one waiting task
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

  /// Waits until at least one pipe is available in the balancer.
  ///
  /// This clones the `Notify` handle for the waiting task.
  pub async fn wait_for_pipe(&self) {
    // Clone notify handle before potentially complex async logic/loops
    let notify = self.notify_waiters.clone();

    loop {
      // Check if a pipe exists *without* rotating/popping yet
      if !self.pipes.lock().await.is_empty() {
        return; // A pipe is available, no need to wait
      }

      // No pipe available, wait for a notification
      // notified() consumes the permit, so loop is needed in case of spurious wakeup
      // or if another task grabs the pipe first after notification.
      notify.notified().await;
      // After notification, loop back to check pipes again
    }
  }

  /// Checks if any pipes are currently registered.
  pub async fn has_pipes(&self) -> bool {
    !self.pipes.lock().await.is_empty()
  }
}
