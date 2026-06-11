use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
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
    zmtp_frames: FrameBatch,
    core_handle: usize,
    core_state_accessor: &parking_lot::RwLock<CoreState>,
  ) -> Result<(), Vec<(String, ZmqError)>> {
    // Error returns URI and ZmqError
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
        // Clone the FrameBatch for each peer
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

#[cfg(test)]
mod additional_distributor_tests {
  use super::*;
  use crate::message::Msg;
  use crate::runtime::mailbox::mailbox;
  use crate::socket::connection_iface::ISocketConnection;
  use crate::socket::core::state::{CoreState, EndpointInfo, EndpointType};
  use crate::socket::options::SocketOptions;
  use crate::socket::types::SocketType;
  use async_trait::async_trait;
  use bytes::Bytes;
  use std::any::Any;
  use std::sync::Mutex;

  #[derive(Debug)]
  struct MockConnection {
    sent_messages: Arc<Mutex<Vec<Msg>>>,
    should_fail: bool,
  }

  #[async_trait]
  impl ISocketConnection for MockConnection {
    async fn send_multipart(&self, msgs: FrameBatch) -> Result<(), ZmqError> {
      if self.should_fail {
        return Err(ZmqError::ConnectionClosed);
      }
      self.sent_messages.lock().unwrap().extend(msgs);
      Ok(())
    }

    async fn close_connection(&self) -> Result<(), ZmqError> {
      Ok(())
    }

    fn as_any(&self) -> &dyn Any {
      self
    }
  }

  fn make_endpoint_info(uri: &str, conn: Arc<dyn ISocketConnection>, id: usize) -> EndpointInfo {
    let (mailbox_tx, _) = mailbox(1);
    EndpointInfo {
      mailbox: mailbox_tx,
      task_handle: None,
      endpoint_type: EndpointType::Session,
      endpoint_uri: uri.to_string(),
      pipe_ids: None,
      handle_id: id,
      target_endpoint_uri: None,
      is_outbound_connection: false,
      peer_socket_type: None,
      connection_iface: conn,
    }
  }

  #[tokio::test]
  async fn test_distributor_fanout() {
    let distributor = Distributor::new();
    let core_state = parking_lot::RwLock::new(CoreState::new(1, SocketType::Pub, SocketOptions::default()));

    let store1 = Arc::new(Mutex::new(Vec::new()));
    let store2 = Arc::new(Mutex::new(Vec::new()));

    let uri1 = "tcp://127.0.0.1:5555".to_string();
    let uri2 = "tcp://127.0.0.1:5556".to_string();

    let conn1: Arc<dyn ISocketConnection> =
      Arc::new(MockConnection { sent_messages: store1.clone(), should_fail: false });
    let conn2: Arc<dyn ISocketConnection> =
      Arc::new(MockConnection { sent_messages: store2.clone(), should_fail: false });

    {
      let mut state = core_state.write();
      state.endpoints.insert(uri1.clone(), make_endpoint_info(&uri1, conn1, 10));
      state.endpoints.insert(uri2.clone(), make_endpoint_info(&uri2, conn2, 11));
    }

    distributor.add_peer_uri(uri1);
    distributor.add_peer_uri(uri2);

    let msg = Msg::from_bytes(Bytes::from_static(b"broadcast-payload"));
    distributor
      .send_to_all(&msg, 1, &core_state)
      .await
      .expect("Distribution should succeed");

    assert_eq!(
      store1.lock().unwrap()[0].data().unwrap(),
      b"broadcast-payload"
    );
    assert_eq!(
      store2.lock().unwrap()[0].data().unwrap(),
      b"broadcast-payload"
    );
  }

  #[tokio::test]
  async fn test_distributor_error_isolation() {
    let distributor = Distributor::new();
    let core_state = parking_lot::RwLock::new(CoreState::new(1, SocketType::Pub, SocketOptions::default()));

    let store_healthy = Arc::new(Mutex::new(Vec::new()));

    let uri_broken = "tcp://127.0.0.1:9999".to_string();
    let uri_healthy = "tcp://127.0.0.1:8888".to_string();

    let conn_broken: Arc<dyn ISocketConnection> = Arc::new(MockConnection {
      sent_messages: Arc::new(Mutex::new(Vec::new())),
      should_fail: true,
    });
    let conn_healthy: Arc<dyn ISocketConnection> =
      Arc::new(MockConnection { sent_messages: store_healthy.clone(), should_fail: false });

    {
      let mut state = core_state.write();
      state.endpoints.insert(
        uri_broken.clone(),
        make_endpoint_info(&uri_broken, conn_broken, 20),
      );
      state.endpoints.insert(
        uri_healthy.clone(),
        make_endpoint_info(&uri_healthy, conn_healthy, 21),
      );
    }

    distributor.add_peer_uri(uri_broken.clone());
    distributor.add_peer_uri(uri_healthy.clone());

    let msg = Msg::from_bytes(Bytes::from_static(b"isolated-payload"));
    let res = distributor.send_to_all(&msg, 1, &core_state).await;

    assert!(res.is_err(), "Expected error due to broken peer");
    let failures = res.unwrap_err();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, uri_broken);
    assert!(
      matches!(failures[0].1, ZmqError::ConnectionClosed),
      "Expected ConnectionClosed, got {:?}",
      failures[0].1
    );

    assert_eq!(
      store_healthy.lock().unwrap()[0].data().unwrap(),
      b"isolated-payload"
    );
  }
}
