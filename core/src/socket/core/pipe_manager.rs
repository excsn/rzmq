use crate::context::Context as RzmqContext;
use crate::error::ZmqError;
use crate::error::ZmqResult;
use crate::message::Msg;
use crate::runtime::ActorDropGuard;
use crate::runtime::ActorType;
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::core::{command_processor, SocketCore};
use crate::socket::{ISocket, SocketEvent};
use crate::Command;

use fibre::mpmc::{AsyncReceiver, AsyncSender};
#[cfg(feature = "inproc")]
use fibre::oneshot;
use fibre::{SendError, TrySendError};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;


pub(crate) async fn run_pipe_reader_task(
  context: RzmqContext,
  core_handle: usize,
  // core_command_mailbox is no longer needed as we call ISocket directly
  // core_command_mailbox: MailboxSender,
  socket_logic_strong: Arc<dyn ISocket>, // This is the direct reference
  pipe_read_id: usize,
  pipe_receiver: AsyncReceiver<Msg>,
) {
// <<< MODIFIED END >>>
  let pipe_reader_task_handle_id = context.inner().next_handle();
  let pipe_reader_actor_type = ActorType::PipeReader; // This ActorType might be removed soon

  let mut actor_drop_guard = ActorDropGuard::new(
    context.clone(),
    pipe_reader_task_handle_id,
    pipe_reader_actor_type,
    None, // PipeReader is not associated with a specific user-facing URI
    Some(core_handle),
  );

  tracing::debug!(
    core_handle = core_handle,
    pipe_read_id = pipe_read_id,
    pipe_reader_task_id = pipe_reader_task_handle_id,
    "PipeReaderTask started."
  );

  let mut final_error_for_stopping: Option<ZmqError> = None;

  loop {
    // We can check if the socket logic is still alive if needed, but recv() will
    // fail if the other end of the pipe is dropped, which is the primary signal.
    // if socket_logic_strong.is_closed() { ... break; ... } // ISocket doesn't have is_closed()

    match pipe_receiver.recv().await {
      Ok(msg) => {
        // <<< MODIFIED [Call ISocket::handle_pipe_event directly] >>>
        let cmd_for_isocket = Command::PipeMessageReceived {
          pipe_id: pipe_read_id,
          msg,
        };
        if let Err(e) = socket_logic_strong
          .handle_pipe_event(pipe_read_id, cmd_for_isocket)
          .await
        {
          tracing::error!(
            core_handle = core_handle,
            pipe_reader_task_id = pipe_reader_task_handle_id,
            pipe_read_id = pipe_read_id,
            "PipeReaderTask: Error from ISocket::handle_pipe_event: {}. Stopping reader.",
            e
          );
          if final_error_for_stopping.is_none() { final_error_for_stopping = Some(e); }
          break;
        }
        // <<< MODIFIED END >>>
      }
      Err(_) => { // RecvError implies channel is closed and empty
        tracing::debug!(
          core_handle = core_handle,
          pipe_reader_task_id = pipe_reader_task_handle_id,
          pipe_read_id = pipe_read_id,
          "PipeReaderTask: Data pipe closed by peer. Notifying ISocket."
        );
        // <<< MODIFIED [Call ISocket::handle_pipe_event directly for closed pipe] >>>
        let cmd_closed_for_isocket = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
        if let Err(e) = socket_logic_strong
          .handle_pipe_event(pipe_read_id, cmd_closed_for_isocket)
          .await
        {
          tracing::warn!(
            core_handle = core_handle,
            "PipeReaderTask: Error from ISocket::handle_pipe_event for PipeClosedByPeer: {}.",
            e
          );
          if final_error_for_stopping.is_none() { final_error_for_stopping = Some(e); }
        }
        // <<< MODIFIED END >>>
        break;
      }
    }
  }

  if let Some(err) = final_error_for_stopping.take() {
    actor_drop_guard.set_error(err);
  } else {
    actor_drop_guard.waive();
  }
}

pub(crate) async fn cleanup_stopped_child_resources(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  stopped_child_actor_id: usize,
  stopped_child_actor_type: ActorType,
  endpoint_uri_opt: Option<&str>,
  error_opt: Option<&ZmqError>,
  is_full_core_shutdown: bool,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(
    parent_core_handle = core_handle,
    stopped_child_id = stopped_child_actor_id,
    ?stopped_child_actor_type,
    uri = ?endpoint_uri_opt,
    error = ?error_opt,
    full_core_shutdown = is_full_core_shutdown,
    "Cleaning up resources for stopped child actor."
  );

  let mut removed_endpoint_info: Option<EndpointInfo> = None;
  let mut detached_pipe_read_id: Option<usize> = None;

  if let Some(uri_key_to_remove) = endpoint_uri_opt {
    let mut core_s_write = core_arc.core_state.write();
    if let Some(ep_info) = core_s_write.endpoints.get(uri_key_to_remove) {
      if ep_info.handle_id == stopped_child_actor_id {
        removed_endpoint_info = core_s_write.endpoints.remove(uri_key_to_remove);
        tracing::debug!(handle=core_handle, child_id=stopped_child_actor_id, uri=%uri_key_to_remove, "Removed EndpointInfo by URI for stopped child.");
      } else {
        tracing::warn!(handle=core_handle, child_id=stopped_child_actor_id, uri=%uri_key_to_remove, "URI found but handle_id mismatch. Will iterate.");
      }
    }
  }

  if removed_endpoint_info.is_none() {
    let mut core_s_write = core_arc.core_state.write();
    let mut key_of_endpoint_to_remove: Option<String> = None;
    for (uri, info) in core_s_write.endpoints.iter() {
      if info.handle_id == stopped_child_actor_id {
        key_of_endpoint_to_remove = Some(uri.clone());
        break;
      }
    }
    if let Some(key) = key_of_endpoint_to_remove {
      removed_endpoint_info = core_s_write.endpoints.remove(&key);
      tracing::debug!(handle=core_handle, child_id=stopped_child_actor_id, uri=%key, "Removed EndpointInfo by iteration for stopped child.");
    }
  }

  if let Some(ep_info) = &removed_endpoint_info {
    if let Some(task_handle) = &ep_info.task_handle {
      if !task_handle.is_finished() {
        task_handle.abort();
        tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Aborted task_handle for stopped child.");
      }
    }

    if let Some((core_write_id, core_read_id)) = ep_info.pipe_ids {
      core_arc
        .core_state
        .write()
        .remove_pipe_state(core_write_id, core_read_id);
      detached_pipe_read_id = Some(core_read_id);
      tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Removed pipe state for stopped child.");

      #[cfg(feature = "io-uring")]
      if ep_info
        .connection_iface
        .as_any()
        .is::<crate::socket::connection_iface::UringFdConnection>()
      {
        let fd = ep_info.handle_id as std::os::unix::io::RawFd;
        core_arc.core_state.write().uring_fd_to_endpoint_uri.remove(&fd);
        crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(fd);
        tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, %fd, "Unregistered UringFD state for stopped child.");
      }
    }

    let monitor_event = match ep_info.endpoint_type {
      EndpointType::Session => SocketEvent::Disconnected {
        endpoint: ep_info.endpoint_uri.clone(),
      },
      EndpointType::Listener => SocketEvent::Closed {
        endpoint: ep_info.endpoint_uri.clone(),
      },
    };
    core_arc.core_state.read().send_monitor_event(monitor_event);
  } else {
    tracing::debug!(
      handle = core_handle,
      child_id = stopped_child_actor_id,
      ?stopped_child_actor_type,
      "No EndpointInfo found to remove for stopped child (might be PipeReader or already cleaned up)."
    );
  }

  if let Some(read_id) = detached_pipe_read_id {
    socket_logic_strong.pipe_detached(read_id).await;
    tracing::debug!(
      handle = core_handle,
      child_id = stopped_child_actor_id,
      pipe_read_id = read_id,
      "Notified ISocket of pipe detachment."
    );
  }

  if !is_full_core_shutdown
    && error_opt.is_some()
    && matches!(stopped_child_actor_type, ActorType::Session)
  {
    if let Some(ep_info) = removed_endpoint_info {
      if ep_info.endpoint_type == EndpointType::Session && ep_info.is_outbound_connection {
        if let Some(target_uri_to_reconnect) = ep_info.target_endpoint_uri {
          if !target_uri_to_reconnect.starts_with("inproc://") {
            let reconnect_enabled = core_arc
              .core_state
              .read()
              .options
              .reconnect_ivl
              .map_or(false, |d| !d.is_zero());
            let is_fatal_for_reconnect = error_opt.map_or(false, crate::transport::tcp::is_fatal_connect_error);

            if reconnect_enabled && !is_fatal_for_reconnect {
              tracing::info!(
                handle = core_handle,
                target_uri = %target_uri_to_reconnect,
                "Unexpected session/engine stop for outbound connection. Initiating reconnect..."
              );
              command_processor::respawn_connecter_actor(core_arc.clone(),  socket_logic_strong.clone(), target_uri_to_reconnect).await;
            } else {
              tracing::warn!(
                handle = core_handle,
                target_uri = %target_uri_to_reconnect,
                reconnect_enabled,
                is_fatal_for_reconnect,
                "Reconnect SKIPPED for stopped outbound session."
              );
            }
          }
        }
      }
    }
  }
}

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

  { // Scope for the write lock
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
      core_s_write.endpoints.insert(endpoint_uri.to_string(), removed_info);
      return None;
    }
    
    tracing::info!(handle = core_handle, uri = %endpoint_uri, "Removed EndpointInfo by URI during proactive cleanup.");
    
    if let Some((core_write_id, core_read_id)) = removed_info.pipe_ids {
      core_s_write.remove_pipe_state(core_write_id, core_read_id);
      detached_pipe_read_id = Some(core_read_id); // Store for later async call
      tracing::debug!(handle = core_handle, uri=%endpoint_uri, "Removed pipe state for session.");
    }
    
    #[cfg(feature = "io-uring")]
    if removed_info.connection_iface.as_any().is::<crate::socket::connection_iface::UringFdConnection>() {
      let fd = removed_info.handle_id as RawFd;
      core_s_write.uring_fd_to_endpoint_uri.remove(&fd);
      crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(fd);
      tracing::debug!(handle = core_handle, uri=%endpoint_uri, %fd, "Unregistered UringFD state.");
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

pub(crate) async fn cleanup_session_state_by_pipe(
  core_arc: Arc<SocketCore>,
  pipe_read_id: usize,
  socket_logic_strong: &Arc<dyn ISocket>,
) -> Option<String> {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, pipe_read_id, "Cleaning up state by pipe_read_id.");

  let mut endpoint_uri_to_remove: Option<String> = None;
  let mut target_uri_for_reconnect: Option<String> = None;
  let mut removed_ep_type: Option<EndpointType> = None;
  let mut removed_ep_uri_for_event: Option<String> = None;
  let mut task_handle_to_abort: Option<JoinHandle<()>> = None;
  let mut pipe_ids_of_removed: Option<(usize, usize)> = None;
  #[cfg(feature = "io-uring")]
  let mut fd_to_unregister_opt: Option<RawFd> = None;

  {
    let core_s_read = core_arc.core_state.read();
    if let Some(uri) = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id) {
      endpoint_uri_to_remove = Some(uri.clone());
      if let Some(ep_info) = core_s_read.endpoints.get(uri) {
        target_uri_for_reconnect = ep_info.target_endpoint_uri.clone();
        removed_ep_type = Some(ep_info.endpoint_type);
        removed_ep_uri_for_event = Some(ep_info.endpoint_uri.clone());
        pipe_ids_of_removed = ep_info.pipe_ids;
        #[cfg(feature = "io-uring")]
        if ep_info
          .connection_iface
          .as_any()
          .is::<crate::socket::connection_iface::UringFdConnection>()
        {
          fd_to_unregister_opt = Some(ep_info.handle_id as RawFd);
        }
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
      #[cfg(feature = "io-uring")]
      if let Some(fd_to_unregister) = fd_to_unregister_opt {
        core_s_write.uring_fd_to_endpoint_uri.remove(&fd_to_unregister);
        crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(fd_to_unregister);
      }
      if let (Some(ep_type), Some(ep_uri_event)) = (removed_ep_type, removed_ep_uri_for_event) {
        let event = match ep_type {
          EndpointType::Session => SocketEvent::Disconnected { endpoint: ep_uri_event },
          EndpointType::Listener => SocketEvent::Closed { endpoint: ep_uri_event },
        };
        core_s_write.send_monitor_event(event);
      }
    } else {
      tracing::warn!(handle=core_handle, pipe_read_id, "cleanup_session_state_by_pipe: EndpointInfo for URI '{}' not found during write, though pipe_read_id mapping existed.", uri_key);
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
  pipe_rx_for_binder_to_receive_from_connector: AsyncReceiver<Msg>,
  pipe_tx_for_binder_to_send_to_connector: AsyncSender<Msg>,
  binder_write_id_for_this_connection: usize,
  binder_read_id_for_this_connection: usize,
  reply_tx_to_connector: oneshot::Sender<ZmqResult<()>>,
) -> Result<(), ZmqError> {
  use crate::socket::{
    core::state::{EndpointInfo, EndpointType},
    SocketEvent,
  };

  let binder_core_handle = core_arc.handle;
  tracing::debug!(
    binder_handle = binder_core_handle,
    connector_uri = %connector_uri,
    binder_write_pipe_id = binder_write_id_for_this_connection,
    binder_read_pipe_id = binder_read_id_for_this_connection,
    "SocketCore (binder) processing InprocBindingRequest event."
  );

  let accept_result: Result<(), ZmqError> = Ok(());

  if accept_result.is_ok() {
    let pipe_reader_task = tokio::spawn(run_pipe_reader_task(
      core_arc.context.clone(),
      binder_core_handle,
      socket_logic_strong.clone(),
      binder_read_id_for_this_connection,
      pipe_rx_for_binder_to_receive_from_connector,
    ));

    let inproc_endpoint_entry_handle_id = core_arc.context.inner().next_handle();

    // <<< MODIFIED START [InprocConnection for binder side] >>>
    let binder_side_inproc_iface = Arc::new(crate::socket::connection_iface::InprocConnection::new(
      inproc_endpoint_entry_handle_id,                 // connection_id for this EndpointInfo
      binder_write_id_for_this_connection,             // Binder's local_pipe_write_id_to_peer (connector's read ID)
      binder_read_id_for_this_connection,              // Binder's local_pipe_read_id_from_peer (connector's write ID)
      connector_uri.clone(),                           // peer_inproc_name_or_uri is the connector's URI
      core_arc.context.clone(),                        // Binder's context
      pipe_tx_for_binder_to_send_to_connector.clone(), // data_tx_to_peer is the sender to the connector
      core_arc.core_state.read().get_monitor_sender_clone(), // Binder's monitor
      core_arc.core_state.read().options.clone(),      // Binder's socket options
    ));
    // <<< MODIFIED END >>>

    let endpoint_info_for_binder = EndpointInfo {
      mailbox: core_arc.command_sender(),
      task_handle: None,
      endpoint_type: EndpointType::Session,
      endpoint_uri: connector_uri.clone(),
      pipe_ids: Some((binder_write_id_for_this_connection, binder_read_id_for_this_connection)),
      handle_id: inproc_endpoint_entry_handle_id,
      target_endpoint_uri: Some(connector_uri.clone()),
      is_outbound_connection: false,
      // <<< MODIFIED [Use the specific binder-side interface] >>>
      connection_iface: binder_side_inproc_iface,
    };

    {
      let mut binder_core_state = core_arc.core_state.write();
      binder_core_state.pipes_tx.insert(
        binder_write_id_for_this_connection,
        pipe_tx_for_binder_to_send_to_connector,
      );
      binder_core_state
        .pipe_reader_task_handles
        .insert(binder_read_id_for_this_connection, pipe_reader_task);
      binder_core_state
        .endpoints
        .insert(connector_uri.clone(), endpoint_info_for_binder);
      binder_core_state
        .pipe_read_id_to_endpoint_uri
        .insert(binder_read_id_for_this_connection, connector_uri.clone());
    }

    if let Some(monitor_tx) = core_arc.core_state.read().get_monitor_sender_clone() {
      let _ = monitor_tx.try_send(SocketEvent::Accepted {
        endpoint: connector_uri.clone(),
        peer_addr: format!("inproc-connector-{}", connector_uri),
      });
    }

    socket_logic_strong
      .pipe_attached(
        binder_read_id_for_this_connection,
        binder_write_id_for_this_connection,
        None,
      )
      .await;
    tracing::info!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Inproc connection accepted by binder.");
  }

  if reply_tx_to_connector.send(accept_result.clone()).is_err() {
    tracing::warn!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Failed to send InprocBindingRequest reply to connector (already taken/dropped).");
  }
  accept_result
}

#[cfg(feature = "inproc")]
pub(crate) async fn handle_inproc_pipe_peer_closed_event(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  // This is the ID the *binder* uses to WRITE to the (now closed) connector.
  // It corresponds to the *connector's* `pipe_read_id`.
  closed_connector_read_id_from_event: usize, // Renamed for clarity
) {
  let binder_core_handle = core_arc.handle;
  tracing::debug!(
    binder_handle = binder_core_handle,
    id_connector_reads_on_this_is_closed = closed_connector_read_id_from_event,
    "SocketCore (binder) handling InprocPipePeerClosed event."
  );

  // <<< MODIFIED START [Corrected logic to find binder's read pipe] >>>
  let mut binder_read_pipe_id_to_cleanup: Option<usize> = None;
  let mut endpoint_uri_of_closed_conn: Option<String> = None;

  {
    let core_s_read = core_arc.core_state.read();
    for (uri, ep_info) in core_s_read.endpoints.iter() {
      if ep_info.endpoint_type == EndpointType::Session {
        if let Some((binder_writes_here, binder_reads_here)) = ep_info.pipe_ids {
          // The event's closed_connector_read_id is what the *binder writes to*.
          if binder_writes_here == closed_connector_read_id_from_event {
            binder_read_pipe_id_to_cleanup = Some(binder_reads_here);
            endpoint_uri_of_closed_conn = Some(uri.clone());
            break;
          }
        }
      }
    }
  }
  // <<< MODIFIED END >>>

  if let Some(read_id_to_clean) = binder_read_pipe_id_to_cleanup {
    tracing::debug!(
      binder_handle = binder_core_handle,
      binder_read_pipe_id_to_clean = read_id_to_clean,
      uri = %endpoint_uri_of_closed_conn.as_deref().unwrap_or("N/A"),
      "Found binder's read pipe to clean up for closed inproc connection."
    );
    let _ = cleanup_session_state_by_pipe(core_arc.clone(), read_id_to_clean, socket_logic_strong).await;
  } else {
    tracing::warn!(
      binder_handle = binder_core_handle,
      binder_write_id_that_peer_closed = closed_connector_read_id_from_event,
      "SocketCore (binder) could not find its corresponding read pipe for InprocPipePeerClosed event. Cleanup may be incomplete or connection already gone."
    );
  }
}

pub(crate) async fn send_msg_with_timeout(
  pipe_tx: &AsyncSender<Msg>,
  msg: Msg,
  timeout_opt: Option<Duration>,
  socket_core_handle: usize,
  pipe_target_id: usize,
) -> Result<(), ZmqError> {
  match timeout_opt {
    None => {
      tracing::trace!(
        core_handle = socket_core_handle,
        pipe_id = pipe_target_id,
        "Sending message via pipe (blocking on HWM)"
      );
      pipe_tx.send(msg).await.map_err(|_| {
        tracing::debug!(
          core_handle = socket_core_handle,
          pipe_id = pipe_target_id,
          "Pipe send failed (ConnectionClosed)"
        );
        ZmqError::ConnectionClosed
      })
    }
    Some(d) if d.is_zero() => {
      tracing::trace!(
        core_handle = socket_core_handle,
        pipe_id = pipe_target_id,
        "Attempting non-blocking send via pipe"
      );

      match pipe_tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_failed_msg_back)) => {
          tracing::trace!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Non-blocking pipe send failed (HWM - ResourceLimitReached)"
          );
          Err(ZmqError::ResourceLimitReached)
        }
        Err(TrySendError::Closed(_failed_msg_back)) => {
          tracing::debug!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Non-blocking pipe send failed (ConnectionClosed)"
          );
          Err(ZmqError::ConnectionClosed)
        }
        _ => unreachable!(),
      }
    }
    Some(timeout_duration) => {
      tracing::trace!(
        core_handle = socket_core_handle,
        pipe_id = pipe_target_id,
        send_timeout_duration = ?timeout_duration,
        "Attempting timed send via pipe"
      );
      match timeout(timeout_duration, pipe_tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(SendError::Closed)) => {
          tracing::debug!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Timed pipe send failed (ConnectionClosed)"
          );
          Err(ZmqError::ConnectionClosed)
        }
        Err(_timeout_elapsed_error) => {
          tracing::trace!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Timed pipe send failed (Timeout on HWM)"
          );
          Err(ZmqError::Timeout)
        }
        _ => unreachable!(),
      }
    }
  }
}
