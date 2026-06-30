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
use fibre::mpmc::bounded_async;

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

// TODO cleanup
pub(crate) async fn cleanup_session_state_by_uri(
  core_arc: Arc<SocketCore>,
  endpoint_uri: &str,
  socket_logic_strong: &Arc<dyn ISocket>,
  _expected_handle_id_if_known: Option<usize>,
) -> Option<EndpointInfo> {
  let core_handle = core_arc.handle;
  tracing::debug!(
    parent_core_handle = core_handle,
    uri_to_cleanup = %endpoint_uri,
    "Attempting to cleanup session state by URI."
  );

  let ep_info_to_process: EndpointInfo;
  let mut detached_pipe_read_id: Option<usize> = None;

  {
    // Scope for the write lock
    let mut core_s_write = core_arc.core_state.write();

    // Attempt to remove the EndpointInfo from the map
    let removed_info = match core_s_write.endpoints.remove(endpoint_uri) {
      Some(info) => info,
      None => {
        tracing::warn!(handle = core_handle, uri=%endpoint_uri, "Cleanup by URI: EndpointInfo not found.");
        return None; // Early exit if not found
      }
    };

    // Validate and process the removed info while still under lock
    if removed_info.endpoint_type != EndpointType::Session {
      tracing::error!(handle = core_handle, uri = %endpoint_uri, "Cleanup by URI: Expected Session type, found {:?}. Reinserting.", removed_info.endpoint_type);
      core_s_write
        .endpoints
        .insert(endpoint_uri.to_string(), removed_info);
      return None;
    }

    tracing::info!(handle = core_handle, uri = %endpoint_uri, "Removed EndpointInfo by URI during proactive cleanup.");

    if let Some((core_write_id, core_read_id)) = removed_info.pipe_ids {
      core_s_write.remove_pipe_state(core_write_id, core_read_id);
      detached_pipe_read_id = Some(core_read_id); // Store for later async call
      tracing::debug!(handle = core_handle, uri=%endpoint_uri, "Removed pipe state for session.");
    }

    core_s_write.send_monitor_event(SocketEvent::Disconnected {
      endpoint: endpoint_uri.to_string(),
    });

    // Assign the fully processed info to our variable outside the lock scope
    ep_info_to_process = removed_info;
  } // RwLockWriteGuard is dropped here

  // --- Perform async operations outside the lock ---

  tracing::debug!(handle = core_handle, uri = %ep_info_to_process.endpoint_uri, "Cleanup: Calling close_connection().");
  if let Err(e) = ep_info_to_process.connection_iface.close_connection().await {
    tracing::warn!(handle = core_handle, uri = %ep_info_to_process.endpoint_uri, "Error in close_connection(): {}", e);
  }

  if let Some(task_handle) = &ep_info_to_process.task_handle {
    if !task_handle.is_finished() {
      task_handle.abort();
      tracing::debug!(handle = core_handle, uri=%ep_info_to_process.endpoint_uri, "Aborted task_handle during cleanup.");
    }
  }

  if let Some(read_id) = detached_pipe_read_id {
    socket_logic_strong.pipe_detached(read_id).await;
    tracing::debug!(handle = core_handle, uri=%ep_info_to_process.endpoint_uri, pipe_read_id = read_id, "Notified ISocket of pipe detachment.");
  }

  // Return the EndpointInfo struct that was removed and processed
  Some(ep_info_to_process)
}

// TODO cleanup
pub(crate) async fn cleanup_session_state_by_pipe(
  core_arc: Arc<SocketCore>,
  pipe_read_id: usize,
  socket_logic_strong: &Arc<dyn ISocket>,
) -> Option<String> {
  let core_handle = core_arc.handle;
  tracing::debug!(
    handle = core_handle,
    pipe_read_id,
    "Cleaning up state by pipe_read_id."
  );

  let mut endpoint_uri_to_remove: Option<String> = None;
  let mut target_uri_for_reconnect: Option<String> = None;
  let mut removed_ep_type: Option<EndpointType> = None;
  let mut removed_ep_uri_for_event: Option<String> = None;
  let mut task_handle_to_abort: Option<JoinHandle<()>> = None;
  let mut pipe_ids_of_removed: Option<(usize, usize)> = None;
  {
    let core_s_read = core_arc.core_state.read();
    if let Some(uri) = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id) {
      endpoint_uri_to_remove = Some(uri.clone());
      if let Some(ep_info) = core_s_read.endpoints.get(uri) {
        target_uri_for_reconnect = ep_info.target_endpoint_uri.clone();
        removed_ep_type = Some(ep_info.endpoint_type);
        removed_ep_uri_for_event = Some(ep_info.endpoint_uri.clone());
        pipe_ids_of_removed = ep_info.pipe_ids;
      }
    }
  }

  if let Some(ref uri_key) = endpoint_uri_to_remove {
    let mut core_s_write = core_arc.core_state.write();
    if let Some(removed_ep_info_struct) = core_s_write.endpoints.remove(uri_key) {
      tracing::info!(handle=core_handle, uri=%uri_key, pipe_read_id, "Removed EndpointInfo during cleanup_session_state_by_pipe.");
      task_handle_to_abort = removed_ep_info_struct.task_handle;
      if let Some((write_id, _read_id_from_epinfo)) = pipe_ids_of_removed {
        core_s_write.remove_pipe_state(write_id, pipe_read_id);
      }
      // uring_fd_to_endpoint_uri removed.
      if let (Some(ep_type), Some(ep_uri_event)) = (removed_ep_type, removed_ep_uri_for_event) {
        let event = match ep_type {
          EndpointType::Session => SocketEvent::Disconnected {
            endpoint: ep_uri_event,
          },
          EndpointType::Listener => SocketEvent::Closed {
            endpoint: ep_uri_event,
          },
        };
        core_s_write.send_monitor_event(event);
      }
    } else {
      tracing::warn!(
        handle = core_handle,
        pipe_read_id,
        "cleanup_session_state_by_pipe: EndpointInfo for URI '{}' not found during write, though pipe_read_id mapping existed.",
        uri_key
      );
    }
  } else {
    tracing::warn!(
      handle = core_handle,
      pipe_read_id,
      "cleanup_session_state_by_pipe: No endpoint_uri found for pipe_read_id. Might have been cleaned up already."
    );
    core_arc
      .core_state
      .write()
      .pipe_reader_task_handles
      .remove(&pipe_read_id)
      .map(|h| h.abort());
  }

  if let Some(th) = task_handle_to_abort {
    if removed_ep_type == Some(EndpointType::Listener) {
      th.abort();
    }
  }
  socket_logic_strong.pipe_detached(pipe_read_id).await;
  target_uri_for_reconnect
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