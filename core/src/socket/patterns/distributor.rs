use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::socket::connection_iface::ISocketConnection;

use std::sync::Arc;

use parking_lot::RwLock;

/// Distributes messages to all connected peers (PUB fan-out).
///
/// Peers are stored as resolved `(uri, connection)` pairs, maintained at pipe
/// attach/detach. The per-message send path takes an `Arc` snapshot and
/// iterates — no endpoint-map lookups, no lock held across sends, no URI
/// cloning per message (unlike the old URI-set design that re-resolved every
/// peer through `CoreState` on every send).
#[derive(Debug, Default)]
pub(crate) struct Distributor {
  peers: RwLock<Arc<Vec<(String, Arc<dyn ISocketConnection>)>>>,
}

impl Distributor {
  pub fn new() -> Self {
    Self::default()
  }

  /// Registers a peer connection for distribution. Idempotent per URI.
  pub fn add_peer(&self, endpoint_uri: String, conn: Arc<dyn ISocketConnection>) {
    let mut guard = self.peers.write();
    if guard.iter().any(|(uri, _)| *uri == endpoint_uri) {
      return;
    }
    let mut next = Vec::with_capacity(guard.len() + 1);
    next.extend(guard.iter().cloned());
    next.push((endpoint_uri, conn));
    *guard = Arc::new(next);
  }

  /// Removes a peer URI from the set.
  pub fn remove_peer_uri(&self, endpoint_uri: &str) {
    let mut guard = self.peers.write();
    if !guard.iter().any(|(uri, _)| uri == endpoint_uri) {
      return;
    }
    let next: Vec<_> = guard
      .iter()
      .filter(|(uri, _)| uri != endpoint_uri)
      .cloned()
      .collect();
    *guard = Arc::new(next);
  }

  fn snapshot(&self) -> Arc<Vec<(String, Arc<dyn ISocketConnection>)>> {
    self.peers.read().clone()
  }

  /// Sends a single-frame message to all registered peers.
  pub async fn send_to_all(
    &self,
    msg: &Msg,
    core_handle: usize,
  ) -> Result<(), Vec<(String, ZmqError)>> {
    let mut fb = FrameBatch::new();
    fb.push(msg.clone());
    self.send_to_all_multipart(fb, core_handle).await
  }

  /// Sends a logical ZMQ message (one or more ZMTP frames) to all peers.
  ///
  /// Fast path is the synchronous `try_send_multipart_owned_sync`; the boxed
  /// async send only runs for peers that are actually backpressured, where it
  /// preserves the SNDTIMEO block/timeout semantics. HWM/timeout drops the
  /// message for that peer (standard PUB behavior); closed or errored peers
  /// are reported back for removal.
  pub async fn send_to_all_multipart(
    &self,
    zmtp_frames: FrameBatch,
    core_handle: usize,
  ) -> Result<(), Vec<(String, ZmqError)>> {
    let peers = self.snapshot();
    if peers.is_empty() || zmtp_frames.is_empty() {
      return Ok(());
    }

    let mut failed_uris: Vec<(String, ZmqError)> = Vec::new();
    let last = peers.len() - 1;
    // The last peer takes the original batch; earlier peers get clones
    // (Bytes refcounts). With a single subscriber this path never clones.
    let mut original = Some(zmtp_frames);

    for (i, (uri, conn)) in peers.iter().enumerate() {
      let batch = if i == last {
        original.take().expect("original batch consumed early")
      } else {
        original.as_ref().expect("original batch present").clone()
      };

      match conn.try_send_multipart_owned_sync(batch) {
        Ok(()) => {}
        Err((returned, ZmqError::ResourceLimitReached)) => {
          match conn.send_multipart_owned(returned).await {
            Ok(()) => {}
            Err((_, ZmqError::ResourceLimitReached)) | Err((_, ZmqError::Timeout)) => {
              tracing::trace!(
                handle = core_handle, uri = %uri,
                "PUB (Distributor) dropping message due to HWM/Timeout for URI"
              );
            }
            Err((_, e @ ZmqError::ConnectionClosed)) => {
              tracing::debug!(
                handle = core_handle, uri = %uri,
                "PUB (Distributor) peer disconnected during send to URI"
              );
              failed_uris.push((uri.clone(), e));
            }
            Err((_, e)) => {
              tracing::error!(
                handle = core_handle, uri = %uri, error = %e,
                "PUB (Distributor) send to URI encountered unexpected error"
              );
              failed_uris.push((uri.clone(), e));
            }
          }
        }
        Err((_, e @ ZmqError::ConnectionClosed)) => {
          tracing::debug!(
            handle = core_handle, uri = %uri,
            "PUB (Distributor) peer disconnected during send to URI"
          );
          failed_uris.push((uri.clone(), e));
        }
        Err((_, e)) => {
          tracing::error!(
            handle = core_handle, uri = %uri, error = %e,
            "PUB (Distributor) send to URI encountered unexpected error"
          );
          failed_uris.push((uri.clone(), e));
        }
      }
    }

    if failed_uris.is_empty() {
      Ok(())
    } else {
      Err(failed_uris)
    }
  }
}
