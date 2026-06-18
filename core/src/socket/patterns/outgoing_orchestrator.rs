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
            // SLOW PATH: All peers are full. Fall back to the async path to properly block.
            match peer.iface.send_multipart_owned(msgs).await {
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
