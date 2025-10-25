use crate::Command;
use crate::context::Context as RzmqContext;
use crate::error::ZmqError;
use crate::error::ZmqResult;
use crate::message::Msg;
use crate::runtime::ActorDropGuard;
use crate::runtime::ActorType;
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::core::{SocketCore, command_processor};
use crate::socket::{ISocket, SocketEvent};

use fibre::mpmc::{AsyncReceiver, AsyncSender};
#[cfg(feature = "inproc")]
use fibre::oneshot;
use fibre::{SendError, TrySendError};
#[cfg(feature = "io-uring")]
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub(crate) async fn run_pipe_reader_task(
  context: RzmqContext,
  core_handle: usize,
  socket_logic_strong: Arc<dyn ISocket>,
  pipe_read_id: usize,
  pipe_receiver: AsyncReceiver<Msg>,
) {
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
          if final_error_for_stopping.is_none() {
            final_error_for_stopping = Some(e);
          }
          break;
        }
      }
      Err(_) => {
        // RecvError implies channel is closed and empty
        tracing::debug!(
          core_handle = core_handle,
          pipe_reader_task_id = pipe_reader_task_handle_id,
          pipe_read_id = pipe_read_id,
          "PipeReaderTask: Data pipe closed by peer. Notifying ISocket."
        );

        let cmd_closed_for_isocket = Command::PipeClosedByPeer {
          pipe_id: pipe_read_id,
        };
        if let Err(e) = socket_logic_strong
          .handle_pipe_event(pipe_read_id, cmd_closed_for_isocket)
          .await
        {
          tracing::warn!(
            core_handle = core_handle,
            "PipeReaderTask: Error from ISocket::handle_pipe_event for PipeClosedByPeer: {}.",
            e
          );
          if final_error_for_stopping.is_none() {
            final_error_for_stopping = Some(e);
          }
        }

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

    // Clean up io_uring specific mappings if this was a uring connection.
    #[cfg(feature = "io-uring")]
    if ep_info
      .connection_iface
      .as_any()
      .is::<crate::socket::connection_iface::UringFdConnection>()
    {
      let fd = ep_info.handle_id as std::os::unix::io::RawFd;
      core_arc
        .core_state
        .write()
        .uring_fd_to_endpoint_uri
        .remove(&fd);
      crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(fd);
      tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, %fd, "Unregistered UringFD state for stopped child.");
    }

    // Send monitor event for the disconnection/closure.
    let monitor_event = match (ep_info.endpoint_type, error_opt) {
      // A session terminated with a security-related error, which indicates a handshake failure.
      (EndpointType::Session, Some(e @ &ZmqError::SecurityError(_)))
      | (EndpointType::Session, Some(e @ &ZmqError::AuthenticationFailure(_))) => {
        SocketEvent::HandshakeFailed {
          endpoint: ep_info.endpoint_uri.clone(),
          error_msg: e.to_string(),
        }
      }
      // A session terminated for any other reason (cleanly or non-security error).
      (EndpointType::Session, _) => SocketEvent::Disconnected {
        endpoint: ep_info.endpoint_uri.clone(),
      },
      // A listener has terminated.
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

    #[cfg(feature = "io-uring")]
    if removed_info
      .connection_iface
      .as_any()
      .is::<crate::socket::connection_iface::UringFdConnection>()
    {
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
        core_s_write
          .uring_fd_to_endpoint_uri
          .remove(&fd_to_unregister);
        crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(fd_to_unregister);
      }
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
  pipe_rx_for_binder_to_receive_from_connector: AsyncReceiver<Vec<Msg>>,
  pipe_tx_for_binder_to_send_to_connector: AsyncSender<Vec<Msg>>,
  binder_write_id_for_this_connection: usize,
  binder_read_id_for_this_connection: usize,
  reply_tx_to_connector: oneshot::Sender<ZmqResult<()>>,
) -> Result<(), ZmqError> {
  use crate::socket::{
    SocketEvent,
    core::state::{EndpointInfo, EndpointType},
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
    let socket_logic = socket_logic_strong.clone();
    tokio::spawn(async move {
      loop {
        match pipe_rx_for_binder_to_receive_from_connector.recv().await {
          Ok(msgs_vec) => {
            for msg in msgs_vec {
              let cmd_for_isocket = Command::PipeMessageReceived {
                pipe_id: binder_read_id_for_this_connection,
                msg,
              };
              if socket_logic
                .handle_pipe_event(binder_read_id_for_this_connection, cmd_for_isocket)
                .await
                .is_err()
              {
                break;
              }
            }
          }
          Err(_) => {
            let cmd_closed = Command::PipeClosedByPeer {
              pipe_id: binder_read_id_for_this_connection,
            };
            let _ = socket_logic
              .handle_pipe_event(binder_read_id_for_this_connection, cmd_closed)
              .await;
            break;
          }
        }
      }
    });

    let inproc_endpoint_entry_handle_id = core_arc.context.inner().next_handle();

    // InprocConnection for binder side
    let binder_side_inproc_iface =
      Arc::new(crate::socket::connection_iface::InprocConnection::new(
        inproc_endpoint_entry_handle_id, // connection_id for this EndpointInfo
        binder_write_id_for_this_connection, // Binder's local_pipe_write_id_to_peer (connector's read ID)
        binder_read_id_for_this_connection, // Binder's local_pipe_read_id_from_peer (connector's write ID)
        connector_uri.clone(),              // peer_inproc_name_or_uri is the connector's URI
        core_arc.context.clone(),           // Binder's context
        pipe_tx_for_binder_to_send_to_connector.clone(), // data_tx_to_peer is the sender to the connector
        core_arc.core_state.read().get_monitor_sender_clone(), // Binder's monitor
        core_arc.core_state.read().options.clone(),      // Binder's socket options
      ));

    let endpoint_info_for_binder = EndpointInfo {
      mailbox: core_arc.command_sender(),
      task_handle: None,
      endpoint_type: EndpointType::Session,
      endpoint_uri: connector_uri.clone(),
      pipe_ids: Some((
        binder_write_id_for_this_connection,
        binder_read_id_for_this_connection,
      )),
      handle_id: inproc_endpoint_entry_handle_id,
      target_endpoint_uri: Some(connector_uri.clone()),
      is_outbound_connection: false,
      connection_iface: binder_side_inproc_iface,
    };

    {
      let mut binder_core_state = core_arc.core_state.write();
      binder_core_state.pipes_tx.insert(
        binder_write_id_for_this_connection,
        pipe_tx_for_binder_to_send_to_connector,
      );
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

  if let Some(read_id_to_clean) = binder_read_pipe_id_to_cleanup {
    tracing::debug!(
      binder_handle = binder_core_handle,
      binder_read_pipe_id_to_clean = read_id_to_clean,
      uri = %endpoint_uri_of_closed_conn.as_deref().unwrap_or("N/A"),
      "Found binder's read pipe to clean up for closed inproc connection."
    );
    let _ =
      cleanup_session_state_by_pipe(core_arc.clone(), read_id_to_clean, socket_logic_strong).await;
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
