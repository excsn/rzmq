use crate::error::ZmqError;
use crate::message::Msg;
use crate::socket::core::send_msg_with_timeout;
use crate::CoreState;
use std::collections::HashSet;
use tokio::sync::RwLock;

/// Distributes messages to a set of connected pipes (write IDs).
#[derive(Debug, Default)]
pub(crate) struct Distributor {
  // Store pipe WRITE IDs (Core -> Session channel ID)
  peers: RwLock<HashSet<usize>>,
}

impl Distributor {
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds a peer pipe (by its write ID).
  pub async fn add_pipe(&self, pipe_write_id: usize) {
    let mut peers_guard = self.peers.write().await;
    if peers_guard.insert(pipe_write_id) {
      tracing::trace!(pipe_id = pipe_write_id, "Distributor added pipe");
    }
  }

  /// Removes a peer pipe (by its write ID).
  pub async fn remove_pipe(&self, pipe_write_id: usize) {
    let mut peers_guard = self.peers.write().await;
    if peers_guard.remove(&pipe_write_id) {
      tracing::trace!(pipe_id = pipe_write_id, "Distributor removed pipe");
    }
  }

  /// Returns a snapshot of the current peer pipe write IDs.
  pub async fn get_peer_ids(&self) -> Vec<usize> {
    let peers_guard = self.peers.read().await;
    peers_guard.iter().copied().collect() // Clone IDs into a new Vec
  }

  /// Sends a message to all currently registered peer pipes.
  /// Acquires the necessary senders from CoreState.
  /// Errors are collected, but sending continues to other peers.
  pub async fn send_to_all(
    &self,
    msg: &Msg,
    core_handle: usize,
    core_state_mutex: &parking_lot::RwLock<CoreState>,
  ) -> Result<(), Vec<(usize, ZmqError)>> {
    let peer_ids = self.get_peer_ids().await;
    tracing::debug!(handle = core_handle, ?peer_ids, "Distributor::send_to_all: Distributing to peer_ids");
    if peer_ids.is_empty() {
      return Ok(());
    }

    let mut failed_pipes = Vec::new();
    let mut send_futures = Vec::new();
    let timeout_opt = core_state_mutex.read().options.sndtimeo;

    for pipe_write_id in peer_ids {
      if let Some(sender) = core_state_mutex.read().get_pipe_sender(pipe_write_id) {
        let msg_clone = msg.clone();
        let payload_preview_str = msg_clone.data()
            .map(|d| String::from_utf8_lossy(&d.iter().take(20).copied().collect::<Vec<_>>()).into_owned()) // Convert Cow to owned String
            .unwrap_or_else(|| "<empty_payload>".to_string());

        tracing::trace!(
            handle = core_handle,
            target_pipe_id = pipe_write_id,
            msg_payload_preview = %payload_preview_str, // Use the owned String
            "Distributor::send_to_all: Preparing send future for pipe"
        );
        send_futures.push(async move {
          match send_msg_with_timeout(&sender, msg_clone, timeout_opt, core_handle, pipe_write_id).await {
              Ok(()) => {
                tracing::trace!(handle = core_handle, pipe_id = pipe_write_id, "Distributor: send_msg_with_timeout successful for pipe.");
                Ok(pipe_write_id)},
              Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
                tracing::trace!(handle=core_handle, pipe_id=pipe_write_id, "PUB (Distributor) dropping message due to HWM/Timeout for pipe");
                Err(None) 
              }
              Err(e @ ZmqError::ConnectionClosed) => {
                tracing::debug!(handle=core_handle, pipe_id=pipe_write_id, "PUB (Distributor) peer disconnected during send to pipe");
                Err(Some((pipe_write_id, e)))
              }
              Err(e) => {
                tracing::error!(handle=core_handle, pipe_id=pipe_write_id, error=%e, "PUB (Distributor) send to pipe encountered unexpected error");
                Err(Some((pipe_write_id, e)))
              }
          }
        });
      } else {
        tracing::warn!(
          handle = core_handle, // Use the passed core_handle for logging
          pipe_id = pipe_write_id,
          "Distributor: Pipe sender not found in core state during send."
        );
        failed_pipes.push((
          pipe_write_id,
          ZmqError::Internal("Distributor found stale pipe ID".into()),
        ));
      }
    }

    let results = futures::future::join_all(send_futures).await;

    for result in results {
      if let Err(Some(error_tuple)) = result {
        failed_pipes.push(error_tuple);
      }
    }

    if failed_pipes.is_empty() {
      Ok(())
    } else {
      Err(failed_pipes)
    }
  }
}
