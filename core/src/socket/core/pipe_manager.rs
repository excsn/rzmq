use crate::Command;
use crate::error::ZmqError;
use crate::runtime::{ActorType, system_events::ConnectionInteractionModel};
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::core::SocketCore;
use crate::socket::{ISocket, SocketEvent};

#[cfg(feature = "inproc")]
use crate::message::FrameBatch;
#[cfg(feature = "inproc")]
use crate::transport::inproc::{
  DirectInprocConnection, InprocHandshakeRequest, InprocHandshakeResponse,
  handshake::validate_socket_compatibility,
};
#[cfg(feature = "inproc")]
use fibre::mpsc::bounded_async;

use std::sync::Arc;
use tokio::task::JoinHandle;

pub(crate) async fn cleanup_stopped_child_resources(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  stopped_child_actor_id: usize,
  stopped_child_actor_type: ActorType,
  endpoint_uri_opt: Option<&str>,
  error_opt: Option<&ZmqError>,
  is_full_core_shutdown: bool,
) -> bool {
  let core_handle = core_arc.handle;
  tracing::debug!(
    parent_core_handle = core_handle,
    stopped_child_id = stopped_child_actor_id,
    ?stopped_child_actor_type,
    uri = ?endpoint_uri_opt,
    error = ?error_opt,
    "Cleaning up resources for stopped child actor."
  );

  let mut removed_endpoint_info: Option<EndpointInfo> = None;
  let mut detached_pipe_read_id: Option<usize> = None;
  let mut should_consider_reconnect = false;

  // Find and remove the EndpointInfo from the main map.
  // This is the most reliable way to get all associated info (URI, pipe IDs, etc.).
  let mut key_to_remove: Option<String> = None;
  {
    let core_s_read = core_arc.core_state.read();
    if let Some(uri_str) = endpoint_uri_opt {
      // Fast path: if URI is provided, check if the handle matches.
      if let Some(ep_info) = core_s_read.endpoints.get(uri_str) {
        if ep_info.handle_id == stopped_child_actor_id {
          key_to_remove = Some(uri_str.to_string());
        }
      }
    }

    // Fallback: If no URI or handle didn't match, iterate to find by handle_id.
    if key_to_remove.is_none() {
      for (uri, info) in core_s_read.endpoints.iter() {
        if info.handle_id == stopped_child_actor_id {
          key_to_remove = Some(uri.clone());
          break;
        }
      }
    }
  } // Read lock is dropped here.

  if let Some(key) = key_to_remove {
    if let Some(ep_info) = core_arc.core_state.write().endpoints.remove(&key) {
      tracing::debug!(handle=core_handle, child_id=stopped_child_actor_id, uri=%key, "Removed EndpointInfo for stopped child.");
      removed_endpoint_info = Some(ep_info);
    }
  }

  if let Some(ep_info) = &removed_endpoint_info {
    // Abort the task handle if it exists and isn't finished.
    if let Some(task_handle) = &ep_info.task_handle {
      if !task_handle.is_finished() {
        task_handle.abort();
        tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Aborted task_handle for stopped child.");
      }
    }

    // Clean up pipe state if it exists.
    if let Some((core_write_id, core_read_id)) = ep_info.pipe_ids {
      core_arc
        .core_state
        .write()
        .remove_pipe_state(core_write_id, core_read_id);
      detached_pipe_read_id = Some(core_read_id);
      tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Removed pipe state for stopped child.");
    }

    // Send monitor event for the disconnection/closure.
    let monitor_event = match (ep_info.endpoint_type, error_opt) {
      (EndpointType::Session, Some(e @ &ZmqError::SecurityError(_)))
      | (EndpointType::Session, Some(e @ &ZmqError::AuthenticationFailure(_))) => {
        SocketEvent::HandshakeFailed {
          endpoint: ep_info.endpoint_uri.clone(),
          error_msg: e.to_string(),
        }
      }
      (EndpointType::Session, _) => SocketEvent::Disconnected {
        endpoint: ep_info.endpoint_uri.clone(),
      },
      (EndpointType::Listener, _) => SocketEvent::Closed {
        endpoint: ep_info.endpoint_uri.clone(),
      },
    };
    core_arc.core_state.read().send_monitor_event(monitor_event);

    // Determine if a reconnect should be considered.
    if !is_full_core_shutdown
        && error_opt.is_some() // Reconnect only on error, not clean disconnect
        && ep_info.endpoint_type == EndpointType::Session
        && ep_info.is_outbound_connection
    {
      let reconnect_ivl_is_positive = core_arc
        .core_state
        .read()
        .options
        .reconnect_ivl
        .map_or(false, |d| !d.is_zero());

      if reconnect_ivl_is_positive
        && !crate::transport::tcp::is_fatal_connect_error(error_opt.unwrap())
      {
        should_consider_reconnect = true;
      }
    }
  } else {
    tracing::debug!(
      handle = core_handle,
      child_id = stopped_child_actor_id,
      ?stopped_child_actor_type,
      "No EndpointInfo found to remove for stopped child (might be PipeReader or already cleaned up)."
    );
  }

  // Notify the ISocket logic that its pipe has been detached.
  if let Some(read_id) = detached_pipe_read_id {
    socket_logic_strong.pipe_detached(read_id).await;
    tracing::debug!(
      handle = core_handle,
      child_id = stopped_child_actor_id,
      pipe_read_id = read_id,
      "Notified ISocket of pipe detachment."
    );
  }

  // Return the flag.
  should_consider_reconnect
}

// --- Inproc Specific Pipe Management ---
#[cfg(feature = "inproc")]
pub(crate) async fn process_inproc_binding_request_event(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  connector_uri: String,
  handshake_request: Arc<std::sync::Mutex<Option<InprocHandshakeRequest>>>,
) -> Result<(), ZmqError> {
  let binder_core_handle = core_arc.handle;
  tracing::debug!(
    binder_handle = binder_core_handle,
    %connector_uri,
    "SocketCore (binder) processing InprocBindingRequest — direct channel path."
  );

  let request = match handshake_request.lock().unwrap().take() {
    Some(r) => r,
    None => {
      tracing::error!(binder_handle = binder_core_handle, "InprocBindingRequest: handshake_request already taken.");
      return Err(ZmqError::Internal("handshake_request already taken".into()));
    }
  };

  let (binder_socket_type, binder_identity, rcvhwm) = {
    let s = core_arc.core_state.read();
    (s.socket_type, s.options.routing_id.clone(), s.options.rcvhwm.max(1))
  };

  // Validate compatibility before creating any channels.
  if let Err(e) = validate_socket_compatibility(request.connector_socket_type, binder_socket_type) {
    tracing::warn!(binder_handle = binder_core_handle, %connector_uri, "Inproc socket type mismatch: {}", e);
    let _ = request.reply_tx.send(Err(e.clone()));
    return Err(e);
  }

  // Channel on which the binder receives frames from the connector.
  let (tx_to_binder, rx_for_binder) = bounded_async::<FrameBatch>(rcvhwm);

  let peer_identity = request.connector_identity.clone();
  let response = InprocHandshakeResponse {
    binder_id: binder_core_handle,
    binder_socket_type,
    binder_identity,
    binder_rx_sender: tx_to_binder,
  };

  // Unblock the connector — it now has the sender to reach us.
  let _ = request.reply_tx.send(Ok(response));

  let (monitor_tx, sndtimeo) = {
    let s = core_arc.core_state.read();
    (s.get_monitor_sender_clone(), s.options.sndtimeo)
  };
  let direct_conn = DirectInprocConnection {
    connection_id: binder_core_handle,
    target_endpoint_uri: connector_uri.clone(),
    peer_queue_sender: request.connector_rx_sender,
    monitor_tx,
    is_congested: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    sndtimeo,
  };

  let cmd = Command::NewConnectionEstablished {
    endpoint_uri: connector_uri.clone(),
    target_endpoint_uri: connector_uri.clone(),
    connection_iface: Some(Arc::new(direct_conn)),
    interaction_model: ConnectionInteractionModel::ViaDirectInproc {
      local_rx: Arc::new(std::sync::Mutex::new(Some(rx_for_binder))),
      peer_identity,
    },
    managing_actor_task_id: None,
  };
  if socket_logic_strong.mailbox().send(cmd).await.is_err() {
    tracing::error!(binder_handle = binder_core_handle, "Failed to send NewConnectionEstablished to binder socket core.");
    return Err(ZmqError::Internal("binder socket core closed".into()));
  }

  if let Some(monitor_tx) = core_arc.core_state.read().get_monitor_sender_clone() {
    let _ = monitor_tx.try_send(SocketEvent::Accepted {
      endpoint: connector_uri.clone(),
      peer_addr: format!("inproc-connector-{}", connector_uri),
    });
  }

  tracing::info!(binder_handle = binder_core_handle, %connector_uri, "Direct inproc connection established.");
  Ok(())
}