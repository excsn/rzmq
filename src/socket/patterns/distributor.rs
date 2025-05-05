// src/socket/patterns/distributor.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::MailboxSender;
use crate::socket::core::send_msg_with_timeout;
// Needed if sending commands back for cleanup
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

  /// Returns a snapshot of the current peer pipe write IDs.
  pub async fn get_peer_ids(&self) -> Vec<usize> {
    let peers_guard = self.peers.lock().await;
    peers_guard.iter().copied().collect() // Clone IDs into a new Vec
  }

  /// Sends a message to all currently registered peer pipes.
  /// Acquires the necessary senders from CoreState.
  /// Errors are collected, but sending continues to other peers.
  pub async fn send_to_all(
    &self,
    msg: &Msg,                     // Send borrowed message
    core_state_mutex: &Mutex<CoreState>, // Need access to get pipe senders
  ) -> Result<(), Vec<(usize, ZmqError)>> {
    let peer_ids = self.get_peer_ids().await;
    if peer_ids.is_empty() {
      return Ok(());
    }

    let core_state_guard = core_state_mutex.lock().await;
    let mut failed_pipes = Vec::new(); // Store pipes that failed fatally
    let mut send_futures = Vec::new();
    let timeout_opt = core_state_guard.options.sndtimeo;
    // Need core handle for logging inside helper call
    let core_handle = core_state_guard.socket_type as usize; // Hacky: use socket type num as handle? No. Need real handle.
                                                             // TODO: Pass core_handle properly to Distributor methods or get from state?

    for pipe_write_id in peer_ids {
      if let Some(sender) = core_state_guard.get_pipe_sender(pipe_write_id) {
        let msg_clone = msg.clone();
        // Pass dummy handle for now
        send_futures.push(async move {
          match send_msg_with_timeout(&sender, msg_clone, timeout_opt, 0/*core_handle*/, pipe_write_id).await {
              Ok(()) => Ok(pipe_write_id), // Indicate success for this pipe
              Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                  tracing::trace!(/*handle=core_handle,*/ pipe_id=pipe_write_id, "PUB dropping message due to HWM/Timeout");
                  Err(None) // Indicate non-fatal drop, no cleanup needed
              }
              Err(e @ ZmqError::ConnectionClosed) => {
                  tracing::debug!(/*handle=core_handle,*/ pipe_id=pipe_write_id, "PUB peer disconnected during send");
                  Err(Some((pipe_write_id, e))) // Indicate fatal error for cleanup
              }
              Err(e) => {
                  tracing::error!(/*handle=core_handle,*/ pipe_id=pipe_write_id, error=%e, "PUB send encountered unexpected error");
                  Err(Some((pipe_write_id, e))) // Indicate fatal error for cleanup
              }
          }
        });
      } else {
        tracing::warn!(
          pipe_id = pipe_write_id,
          "Distributor: Pipe sender not found in core state during send."
        );
        // Treat as fatal error for this pipe ID?
        failed_pipes.push((
          pipe_write_id,
          ZmqError::Internal("Distributor found stale pipe ID".into()),
        ));
      }
    }
    drop(core_state_guard); // Release lock

    // Execute sends concurrently
    let results = futures::future::join_all(send_futures).await;

    // Collect fatal errors/closed pipes
    for result in results {
      if let Err(Some(error_tuple)) = result {
        failed_pipes.push(error_tuple);
      }
      // Ignore Ok(_) and Err(None)
    }

    if failed_pipes.is_empty() {
      Ok(())
    } else {
      Err(failed_pipes) // Return list of pipes needing cleanup
    }
  }
}
