//! Handshake outcomes for both I/O backends. The tokio session reports through
//! the event bus (`PeerIdentityEstablished`, `HandshakeFailed`) and io_uring
//! through `UringConnectionEstablished` and `UringFdError`; each lands here, so
//! the state changes and monitor events are the same whichever backend ran the
//! connection. The `Disconnected` that follows a failure comes from
//! `pipe_manager::cleanup_stopped_child_resources`.

use crate::message::Blob;
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::events::SocketEvent;

use std::sync::Arc;

/// Records the peer of a completed handshake on `pipe_read_id` and reports
/// `HandshakeSucceeded` for `endpoint_uri`. While the socket is shutting down
/// only the event is reported.
pub(crate) async fn handshake_succeeded(
  core_arc: &Arc<SocketCore>,
  socket_logic: &Arc<dyn ISocket>,
  core_running: bool,
  pipe_read_id: usize,
  endpoint_uri: String,
  peer_identity: Option<Blob>,
  peer_socket_type: Option<String>,
) {
  if core_running {
    {
      let mut cs = core_arc.core_state.write();
      if let Some(uri) = cs.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned() {
        if let Some(ep) = cs.endpoints.get_mut(&uri) {
          ep.peer_socket_type = peer_socket_type;
        }
        if let Some(recon_state) = cs.reconnect_states.get_mut(&uri) {
          recon_state.on_connection_success();
        }
      }
    }
    socket_logic.update_peer_identity(pipe_read_id, peer_identity).await;
  }
  core_arc
    .core_state
    .read()
    .send_monitor_event(SocketEvent::HandshakeSucceeded { endpoint: endpoint_uri });
}

/// Reports `HandshakeFailed` for a connection that ended before its handshake completed.
pub(crate) fn handshake_failed(core_arc: &Arc<SocketCore>, endpoint_uri: String, error_msg: String) {
  core_arc
    .core_state
    .read()
    .send_monitor_event(SocketEvent::HandshakeFailed { endpoint: endpoint_uri, error_msg });
}
