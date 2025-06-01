use crate::error::ZmqError;
use crate::message::Msg;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::CoreState;

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;

/// Distributes messages to a set of connected peer URIs.
#[derive(Debug, Default)]
pub(crate) struct Distributor {
  peer_uris: RwLock<HashSet<String>>,
}

impl Distributor {
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds a peer URI to the set for distribution.
  pub fn add_peer_uri(&self, endpoint_uri: String) {
    let mut peers_guard = self.peer_uris.write();
    if peers_guard.insert(endpoint_uri.clone()) {
      // Clone since insert takes ownership
      tracing::trace!(uri = %endpoint_uri, "Distributor added peer URI");
    }
  }

  /// Removes a peer URI from the set.
  pub fn remove_peer_uri(&self, endpoint_uri: &str) {
    let mut peers_guard = self.peer_uris.write();
    if peers_guard.remove(endpoint_uri) {
      tracing::trace!(uri = %endpoint_uri, "Distributor removed peer URI");
    }
  }

  /// Returns a snapshot of the current peer URIs.
  pub fn get_peer_uris(&self) -> Vec<String> {
    let peers_guard = self.peer_uris.read();
    peers_guard.iter().cloned().collect() // Clone URIs into a new Vec
  }

  /// Sends a message to all currently registered peer URIs.
  /// Looks up `ISocketConnection` from `CoreState` for each URI.
  /// Errors are collected, but sending continues to other peers.
  pub async fn send_to_all(
    &self,
    msg: &Msg,
    core_handle: usize,
    core_state_accessor: &parking_lot::RwLock<CoreState>, // Direct access to CoreState
  ) -> Result<(), Vec<(String, ZmqError)>> {
    // Error now returns URI and ZmqError
    let uris_to_send_to = self.get_peer_uris(); // This is synchronous as RwLock is parking_lot
    tracing::debug!(
      handle = core_handle,
      num_peers = uris_to_send_to.len(),
      "Distributor::send_to_all: Distributing to peer URIs"
    );
    if uris_to_send_to.is_empty() {
      return Ok(());
    }

    let mut failed_uris = Vec::new(); // Store (URI, Error) for failures

    // We need to iterate and await sends. To avoid holding core_state_accessor lock across awaits,
    // collect ISocketConnection interfaces first, or re-fetch per send.
    // Re-fetching per send is safer against stale EndpointInfo but might be slightly less performant.
    // Given minimal changes, let's re-fetch.

    for uri_to_send in uris_to_send_to {
      let conn_iface_opt: Option<Arc<dyn ISocketConnection>> = {
        // Short scope for read lock
        let core_s_read = core_state_accessor.read();
        core_s_read
          .endpoints
          .get(&uri_to_send)
          .map(|ep_info| ep_info.connection_iface.clone())
      };

      if let Some(conn_iface) = conn_iface_opt {
        let msg_clone = msg.clone(); // Clone message for each send
                                     // ISocketConnection.send_message() handles SNDTIMEO internally
        match conn_iface.send_message(msg_clone).await {
          Ok(()) => {
            tracing::trace!(handle = core_handle, uri = %uri_to_send, "Distributor: send_message successful for URI.");
          }
          Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
            tracing::trace!(handle = core_handle, uri = %uri_to_send, "PUB (Distributor) dropping message due to HWM/Timeout for URI");
            // For PUB, these are not considered fatal errors for the Distributor's list of peers.
            // The message is just dropped for this peer.
          }
          Err(e @ ZmqError::ConnectionClosed) => {
            tracing::debug!(handle = core_handle, uri = %uri_to_send, "PUB (Distributor) peer disconnected during send to URI");
            failed_uris.push((uri_to_send.clone(), e)); // URI needs to be removed from distributor
          }
          Err(e) => {
            tracing::error!(handle = core_handle, uri = %uri_to_send, error = %e, "PUB (Distributor) send to URI encountered unexpected error");
            failed_uris.push((uri_to_send.clone(), e)); // URI needs to be removed if error is persistent
          }
        }
      } else {
        tracing::warn!(
            handle = core_handle,
            uri = %uri_to_send,
            "Distributor: ISocketConnection not found for URI. Stale URI?"
        );
        // This URI is stale in the distributor, mark for removal.
        failed_uris.push((
          uri_to_send.clone(),
          ZmqError::Internal("Distributor found stale URI".into()),
        ));
      }
    }

    if failed_uris.is_empty() {
      Ok(())
    } else {
      Err(failed_uris)
    }
  }

  /// Sends a logical ZMQ message (composed of one or more ZMTP frames) to all peers.
  pub async fn send_to_all_multipart(
    &self,
    zmtp_frames: Vec<Msg>, // These are the ZMTP frames for one logical ZMQ message
    core_handle: usize,
    core_state_accessor: &parking_lot::RwLock<CoreState>, 
  ) -> Result<(), Vec<(String, ZmqError)>> { // Error returns URI and ZmqError
    let uris_to_send_to = self.get_peer_uris();
    tracing::debug!(
      handle = core_handle,
      num_peers = uris_to_send_to.len(),
      num_zmtp_frames = zmtp_frames.len(),
      "Distributor::send_to_all_multipart: Distributing to peer URIs"
    );
    if uris_to_send_to.is_empty() || zmtp_frames.is_empty() {
      return Ok(());
    }

    let mut failed_uris = Vec::new(); 

    for uri_to_send in uris_to_send_to {
      let conn_iface_opt: Option<Arc<dyn ISocketConnection>> = {
        let core_s_read = core_state_accessor.read();
        core_s_read
          .endpoints
          .get(&uri_to_send)
          .map(|ep_info| ep_info.connection_iface.clone())
      };

      if let Some(conn_iface) = conn_iface_opt {
        // Clone the Vec<Msg> for each peer, as send_multipart might be consumed or held by handler
        let frames_for_this_peer = zmtp_frames.clone(); 
        match conn_iface.send_multipart(frames_for_this_peer).await {
          Ok(()) => {
            tracing::trace!(handle = core_handle, uri = %uri_to_send, "Distributor: send_multipart successful for URI.");
          }
          Err(ZmqError::ResourceLimitReached) | Err(ZmqError::Timeout) => {
            tracing::trace!(handle = core_handle, uri = %uri_to_send, "PUB (Distributor) send_multipart dropping message due to HWM/Timeout for URI");
          }
          Err(e @ ZmqError::ConnectionClosed) => {
            tracing::debug!(handle = core_handle, uri = %uri_to_send, "PUB (Distributor) send_multipart: peer disconnected during send to URI");
            failed_uris.push((uri_to_send.clone(), e)); 
          }
          Err(e) => {
            tracing::error!(handle = core_handle, uri = %uri_to_send, error = %e, "PUB (Distributor) send_multipart to URI encountered unexpected error");
            failed_uris.push((uri_to_send.clone(), e)); 
          }
        }
      } else {
        tracing::warn!(
            handle = core_handle,
            uri = %uri_to_send,
            "Distributor: ISocketConnection not found for URI during send_multipart. Stale URI?"
        );
        failed_uris.push((
          uri_to_send.clone(),
          ZmqError::Internal("Distributor found stale URI for multipart send".into()),
        ));
      }
    }

    if failed_uris.is_empty() {
      Ok(())
    } else {
      Err(failed_uris)
    }
  }
}
