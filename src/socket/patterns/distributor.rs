// src/socket/patterns/distributor.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::MailboxSender; // Needed if sending commands back for cleanup
use crate::CoreState; // Need CoreState to get Senders
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex; // Use Tokio Mutex if state needs internal mutation

/// Distributes messages to a set of connected pipes (write IDs).
#[derive(Debug, Default)]
pub(crate) struct Distributor {
  // Store pipe WRITE IDs (Core -> Session channel ID)
  peers: Mutex<HashSet<usize>>,
}

impl Distributor {
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds a peer pipe (by its write ID).
  pub async fn add_pipe(&self, pipe_write_id: usize) {
    let mut peers_guard = self.peers.lock().await;
    if peers_guard.insert(pipe_write_id) {
      tracing::trace!(pipe_id = pipe_write_id, "Distributor added pipe");
    }
  }

  /// Removes a peer pipe (by its write ID).
  pub async fn remove_pipe(&self, pipe_write_id: usize) {
    let mut peers_guard = self.peers.lock().await;
    if peers_guard.remove(&pipe_write_id) {
      tracing::trace!(pipe_id = pipe_write_id, "Distributor removed pipe");
    }
  }

  /// Sends a message to all currently registered peer pipes.
  /// Acquires the necessary senders from CoreState.
  /// Errors are collected, but sending continues to other peers.
  pub async fn send_to_all(
    &self,
    msg: &Msg,                     // Send borrowed message
    core_state: &Mutex<CoreState>, // Need access to get pipe senders
  ) -> Result<(), Vec<(usize, ZmqError)>> {
    // Return collected errors
    let core_state_guard = core_state.lock().await;
    let peers_guard = self.peers.lock().await;

    if peers_guard.is_empty() {
      tracing::trace!("Distributor: No peers to send to.");
      return Ok(());
    }

    let mut errors = Vec::new();
    let mut send_futures = Vec::new();

    for &pipe_write_id in peers_guard.iter() {
      if let Some(sender) = core_state_guard.get_pipe_sender(pipe_write_id) {
        // Clone message for each send? No, Bytes is cheap to clone.
        let msg_clone = msg.clone();
        send_futures.push(async move {
          // Use send().await to respect backpressure (SNDHWM)
          if let Err(_e) = sender.send(msg_clone).await {
            // Return error tuple (pipe_id, error)
            // Don't trace error here, let caller decide based on collected errors
            // Session/Engine closing is expected, ZmqError::ConnectionClosed is suitable
            Some((pipe_write_id, ZmqError::ConnectionClosed))
          } else {
            None // Indicate success
          }
        });
      } else {
        tracing::warn!(
          pipe_id = pipe_write_id,
          "Distributor: Pipe sender not found in core state during send."
        );
        // Indicates inconsistency - pipe likely detached but not removed from distributor?
        errors.push((
          pipe_write_id,
          ZmqError::Internal("Distributor found stale pipe ID".into()),
        ));
      }
    }

    // Release locks before awaiting futures
    drop(peers_guard);
    drop(core_state_guard);

    // Execute all sends concurrently
    let results = futures::future::join_all(send_futures).await;

    // Collect errors from send results
    for result in results {
      if let Some(error_tuple) = result {
        errors.push(error_tuple);
      }
    }

    if errors.is_empty() {
      Ok(())
    } else {
      Err(errors)
    }
  }
}
