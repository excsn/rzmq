use std::sync::Arc;

use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::patterns::LoadBalancer;

#[derive(Debug, Default)]
pub(crate) struct OutgoingMessageOrchestrator {
  load_balancer: LoadBalancer,
}

impl OutgoingMessageOrchestrator {
  pub fn new() -> Self {
    Self::default()
  }

  pub fn add_connection(&self, uri: String, iface: Arc<dyn ISocketConnection>) {
    self.load_balancer.add_connection(uri, iface);
  }

  pub fn remove_connection(&self, uri: &str) {
    self.load_balancer.remove_connection(uri);
  }

  pub fn deactivate(&self) {
    self.load_balancer.deactivate();
  }

  pub fn has_connections(&self) -> bool {
    self.load_balancer.has_connections()
  }

  pub async fn wait_for_connection(&self) -> Result<(), ZmqError> {
    self.load_balancer.wait_for_connection().await
  }

  /// Synchronous single-pass route attempt. Rotates the load balancer and tries each peer
  /// once; returns ownership of the batch immediately if all peers are full or absent.
  /// Never blocks, never allocates. Callers fall back to `route_message` on `Err`.
  pub fn try_route_sync(&self, mut msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    let count = self.load_balancer.connection_count();
    if count == 0 {
      return Err((msgs, ZmqError::ResourceLimitReached));
    }
    for _ in 0..count {
      let peer = match self.load_balancer.get_next_connection() {
        Some(p) => p,
        None => return Err((msgs, ZmqError::ResourceLimitReached)),
      };
      match peer.iface.try_send_multipart_owned_sync(msgs) {
        Ok(()) => return Ok(()),
        Err((returned, ZmqError::ResourceLimitReached)) => msgs = returned,
        Err(e) => return Err(e),
      }
    }
    Err((msgs, ZmqError::ResourceLimitReached))
  }

  /// Route `msgs` to the next available peer, skipping full ones (write-ready skip).
  ///
  /// Returns ownership of the batch on failure so callers can re-queue without cloning.
  /// `wait_for_peer`: if true and no peer exists at all, blocks until one connects.
  pub async fn route_message(
    &self,
    mut msgs: FrameBatch,
    wait_for_peer: bool,
  ) -> Result<(), (FrameBatch, ZmqError)> {
    let mut max_attempts = self.load_balancer.connection_count().max(1);
    let mut attempts: usize = 0;

    loop {
      let peer = match self.load_balancer.get_next_connection() {
        Some(p) => p,
        None => {
          if !wait_for_peer {
            return Err((msgs, ZmqError::ResourceLimitReached));
          }
          match self.load_balancer.wait_for_connection().await {
            Ok(()) => {
              max_attempts = self.load_balancer.connection_count().max(1);
              attempts = 0;
              continue;
            }
            Err(e) => return Err((msgs, e)),
          }
        }
      };

      // FAST PATH: Zero-allocation synchronous push
      match peer.iface.try_send_multipart_owned_sync(msgs) {
        Ok(()) => return Ok(()),
        Err((returned, ZmqError::ResourceLimitReached)) => {
          msgs = returned;
          attempts += 1;
          if attempts >= max_attempts {
            // SLOW PATH: every peer was full on this sweep, so we must block.
            //
            // Pick a *fresh* peer from the round-robin rotation to block on rather
            // than reusing the sweep-terminal `peer`. A full sweep calls
            // `get_next_connection` exactly `count` times, which wraps `next_idx`
            // back to where the sweep started — so without this extra advance every
            // saturated message would deterministically block on the same terminal
            // peer, funneling a disproportionate share to it. The extra rotation
            // makes the blocking target drift across all peers (net advance of 1 per
            // saturated message), restoring fair distribution under backpressure.
            let block_peer = match self.load_balancer.get_next_connection() {
              Some(p) => p,
              None => return Err((msgs, ZmqError::ResourceLimitReached)),
            };
            match block_peer.iface.send_multipart_owned(msgs).await {
              Ok(()) => return Ok(()),
              Err((returned, ZmqError::ResourceLimitReached)) => {
                return Err((returned, ZmqError::ResourceLimitReached));
              }
              Err((_, e)) => return Err((FrameBatch::new(), e)),
            }
          }
        }
        Err((_, e)) => return Err((FrameBatch::new(), e)),
      }
    }
  }
}
