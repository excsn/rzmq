// core/src/socket/core/command_processor.rs

use crate::context::Context as RzmqContext;
use crate::error::ZmqError;
use crate::runtime::{ActorType, Command, MailboxSender, SystemEvent};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::{EndpointInfo, EndpointType, ShutdownPhase};
use crate::socket::core::{pipe_manager, shutdown, SocketCore}; // For calling other helpers
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{self, *};
use crate::socket::ISocket;
use crate::transport::endpoint::{parse_endpoint, Endpoint};
#[cfg(feature = "inproc")]
use crate::transport::inproc; // For inproc bind/connect directly
#[cfg(feature = "ipc")]
use crate::transport::ipc::{IpcConnecter, IpcListener};
use crate::transport::tcp::{TcpConnecter, TcpListener}; // For TCP bind/connect

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;

/// Processes a command received on SocketCore's command mailbox.
/// Returns Ok(()) if processed, or Err(ZmqError) for fatal errors that should stop SocketCore.
pub(crate) async fn process_socket_command(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  command: Command,
) -> Result<(), ZmqError> {
  let core_handle = core_arc.handle;

  let command_name_str = command.variant_name();
  // Log command only at trace level or if it's an unexpected one.
  // tracing::trace!(handle = core_handle, cmd_name = %command_name_str, "SocketCore processing command");

  // Check shutdown phase before processing most commands
  let current_shutdown_phase = core_arc.shutdown_coordinator.lock().await.state;
  if current_shutdown_phase != ShutdownPhase::Running {
    match command {
      // Allow UserClose and Stop even if shutting down to ensure shutdown completes.
      Command::UserClose { reply_tx } => {
        tracing::info!(handle = core_handle, "Processing UserClose command during shutdown.");
        shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
        let _ = reply_tx.send(Ok(()));
      }
      Command::Stop => {
        tracing::info!(handle = core_handle, "Processing Stop command during shutdown.");
        shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
      }
      // For other commands, if shutting down, reply with an error or ignore.
      Command::UserBind { reply_tx, .. }
      | Command::UserConnect { reply_tx, .. }
      | Command::UserDisconnect { reply_tx, .. }
      | Command::UserUnbind { reply_tx, .. }
      | Command::UserSetOpt { reply_tx, .. }
      | Command::UserMonitor { reply_tx, .. } => {
        tracing::warn!(handle = core_handle, cmd_name = %command_name_str, "Command ignored: SocketCore is shutting down.");
        let _ = reply_tx.send(Err(ZmqError::InvalidState("Socket is shutting down".into())));
      }
      Command::UserRecv { reply_tx } => {
        let _ = reply_tx.send(Err(ZmqError::InvalidState("Socket is shutting down".into())));
      }
      Command::UserGetOpt { reply_tx, .. } => {
        let _ = reply_tx.send(Err(ZmqError::InvalidState("Socket is shutting down".into())));
      }
      // UserSend is fire-and-forget, log and drop.
      Command::UserSend { .. } => {
        tracing::warn!(handle = core_handle, cmd_name = %command_name_str, "UserSend ignored: SocketCore is shutting down.");
      }
      // Internal commands like PipeClosedByPeer might still need processing if they arrive.
      // However, PipeReaderTasks should stop if SocketCore is shutting down.
      // For now, these are primarily handled by ISocket::handle_pipe_event.
      _ => {
        tracing::warn!(handle = core_handle, cmd_name = %command_name_str, "Unhandled or unexpected command during shutdown.");
      }
    }
    return Ok(()); // Command processed (or ignored) due to shutdown state.
  }

  // --- SocketCore is Running ---
  match command {
    Command::UserBind { endpoint, reply_tx } => {
      handle_user_bind(core_arc, endpoint, reply_tx).await;
    }
    Command::UserConnect { endpoint, reply_tx } => {
      handle_user_connect(core_arc, endpoint, reply_tx).await;
    }
    Command::UserDisconnect { endpoint, reply_tx } => {
      handle_user_disconnect(core_arc, socket_logic_strong, endpoint, reply_tx).await;
    }
    Command::UserUnbind { endpoint, reply_tx } => {
      handle_user_unbind(core_arc, socket_logic_strong, endpoint, reply_tx).await;
    }
    Command::UserSend { msg } => {
      if let Err(e) = socket_logic_strong.send(msg).await {
        tracing::debug!(handle = core_handle, "UserSend ISocket::send error: {}", e);
        // Errors from send (like HWM, Timeout) are typically returned to the user by send() itself
        // if it's a blocking send, or handled by options. Here, we just log.
      }
    }
    Command::UserRecv { reply_tx } => {
      let result = socket_logic_strong.recv().await;
      if let Err(ref e) = result {
        tracing::debug!(handle = core_handle, "UserRecv ISocket::recv error: {}", e);
      }
      let _ = reply_tx.send(result);
    }
    Command::UserSetOpt {
      option,
      value,
      reply_tx,
    } => {
      let _ = reply_tx.send(handle_set_option(core_arc.clone(), socket_logic_strong, option, &value).await);
    }
    Command::UserGetOpt { option, reply_tx } => {
      let _ = reply_tx.send(handle_get_option(core_arc.clone(), socket_logic_strong, option).await);
    }
    Command::UserMonitor { monitor_tx, reply_tx } => {
      handle_user_monitor(core_arc.clone(), monitor_tx, reply_tx).await;
    }
    Command::UserClose { reply_tx } => {
      tracing::info!(handle = core_handle, "SocketCore received UserClose command.");
      // Publish event first, then initiate shutdown
      shutdown::publish_socket_closing_event(&core_arc.context, core_handle).await;
      shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
      let _ = reply_tx.send(Ok(())); // Acknowledge close initiation
    }
    Command::Stop => {
      // Direct stop command to SocketCore
      tracing::info!(handle = core_handle, "SocketCore received direct Stop command.");
      // Publish event first, then initiate shutdown
      shutdown::publish_socket_closing_event(&core_arc.context, core_handle).await;
      shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
    }

    // --- Uring Specific Commands ---
    #[cfg(feature = "io-uring")]
    Command::UringFdMessage { fd, msg } => {
      let endpoint_uri_opt = core_arc.core_state.read().uring_fd_to_endpoint_uri.get(&fd).cloned();
      if let Some(uri) = endpoint_uri_opt {
        // The synthetic_read_id is what ISocket knows this FD by.
        // We need to find it from the EndpointInfo associated with this FD/URI.
        let synthetic_read_id_opt = core_arc
          .core_state
          .read()
          .endpoints
          .get(&uri)
          .and_then(|ep_info| ep_info.pipe_ids.map(|pids| pids.1)); // pids.1 is the read_id

        if let Some(s_read_id) = synthetic_read_id_opt {
          let cmd_for_isocket = Command::PipeMessageReceived {
            pipe_id: s_read_id,
            msg,
          };
          if let Err(e) = socket_logic_strong.handle_pipe_event(s_read_id, cmd_for_isocket).await {
            tracing::error!(handle=core_handle, %fd, "Error from ISocket::handle_pipe_event for UringFdMessage: {}", e);
            // This error might require closing the UringFdConnection.
            // The error should ideally propagate from ISocket::handle_pipe_event if it's fatal for the "pipe".
          }
        } else {
          tracing::warn!(handle=core_handle, %fd, %uri, "No synthetic_read_id found for UringFdMessage. Inconsistent state?");
        }
      } else {
        tracing::warn!(handle=core_handle, %fd, "Received UringFdMessage for unknown FD. Message dropped.");
      }
    }
    #[cfg(feature = "io-uring")]
    Command::UringFdError { fd, error } => {
      let endpoint_uri_opt = core_arc.core_state.read().uring_fd_to_endpoint_uri.get(&fd).cloned();

      if let Some(uri) = endpoint_uri_opt {
        let conn_iface_opt;
        let synthetic_read_id_opt;
        {
          let cs = core_arc.core_state.read();
          let ep_info_opt = cs.endpoints.get(&uri);
          conn_iface_opt = ep_info_opt.map(|ep| ep.connection_iface.clone());
          synthetic_read_id_opt = ep_info_opt.and_then(|ep| ep.pipe_ids.map(|pids| pids.1));
        }
        tracing::warn!(handle=core_handle, %fd, %uri, %error, "Processing UringFdError. Initiating close and cleanup.");

        // 1. Initiate close of the underlying connection via ISocketConnection
        if let Some(iface) = conn_iface_opt {
          if let Err(close_err) = iface.close_connection().await {
            tracing::warn!(handle=core_handle, %fd, "Error calling close_connection() for UringFdError: {}", close_err);
          }
        } else {
          tracing::warn!(handle=core_handle, %fd, "No ISocketConnection found to close for UringFdError on URI {}.", uri);
        }

        // 2. Notify ISocket logic that its "pipe" is detached
        if let Some(s_read_id) = synthetic_read_id_opt {
          socket_logic_strong.pipe_detached(s_read_id).await;
        }

        // 3. Clean up SocketCore's state for this endpoint/FD
        //    This will remove EndpointInfo, pipe_read_id_to_uri, uring_fd_to_uri,
        //    and unregister from global_uring_state.
        //    The 'stopped_child_actor_id' is fd as usize.
        //    'actor_type' is conceptually Session.
        //    'error_opt' is Some(&error).
        //    'is_full_core_shutdown' depends on SocketCore's current state.
        pipe_manager::cleanup_stopped_child_resources(
          core_arc.clone(),
          socket_logic_strong,
          fd as usize,        // The "child_id" is the FD
          ActorType::Session, // Treat as a session for cleanup
          Some(&uri),
          Some(&error),
          current_shutdown_phase != ShutdownPhase::Running,
        )
        .await;
      } else {
        tracing::warn!(handle=core_handle, %fd, %error, "Received UringFdError for unknown FD (URI not found in uring_fd_to_endpoint_uri map).");
      }
    }
    #[cfg(feature = "io-uring")]
    Command::UringFdHandshakeComplete { fd, peer_identity } => {
      let endpoint_uri_opt = core_arc.core_state.read().uring_fd_to_endpoint_uri.get(&fd).cloned();
      if let Some(uri) = endpoint_uri_opt {
        let synthetic_read_id_opt = core_arc
          .core_state
          .read()
          .endpoints
          .get(&uri)
          .and_then(|ep_info| ep_info.pipe_ids.map(|pids| pids.1));

        if let Some(s_read_id) = synthetic_read_id_opt {
          socket_logic_strong.update_peer_identity(s_read_id, peer_identity).await;
        } else {
          tracing::warn!(handle=core_handle, %fd, %uri, "No synthetic_read_id for UringFdHandshakeComplete.");
        }
      } else {
        tracing::warn!(handle=core_handle, %fd, "UringFdHandshakeComplete for unknown FD.");
      }
    }

    // Commands NOT expected by SocketCore's main mailbox:
    // Attach, SessionPushCmd, EnginePushCmd, EngineReady, EngineError, EngineStopped,
    // RequestZapAuth, ProcessZapReply, AttachPipe, PipeMessageReceived, PipeClosedByPeer (these last two are now events to ISocket)
    _ => {
      tracing::error!(handle = core_handle, cmd_name = %command_name_str, "SocketCore received UNEXPECTED command type on its mailbox!");
      // This could be a ZmqError::Internal if it indicates a logic flaw.
    }
  }
  Ok(())
}

async fn handle_user_bind(
  core_arc: Arc<SocketCore>,
  endpoint: String,
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, %endpoint, "Processing UserBind command");

  let parse_result = parse_endpoint(&endpoint);
  let context_clone = core_arc.context.clone(); // For spawning actors
  let parent_socket_id = core_arc.handle; // For ActorStarted event

  let mut actual_uri_for_state_update: Option<String> = None;
  let bind_result: Result<(), ZmqError>;

  match parse_result {
    Ok(Endpoint::Tcp(_addr, ref uri_from_parse)) => {
      // Use parsed URI as key
      let core_s_read = core_arc.core_state.read();
      if core_s_read.endpoints.contains_key(uri_from_parse) {
        bind_result = Err(ZmqError::AddrInUse(uri_from_parse.clone()));
      } else {
        let monitor_tx_clone = core_s_read.get_monitor_sender_clone();
        let options_clone = core_s_read.options.clone();
        drop(core_s_read); // Release read lock

        let child_actor_handle = context_clone.inner().next_handle();
        // Spawn TcpListener
        match TcpListener::create_and_spawn(
          child_actor_handle,
          endpoint.clone(), // User provided endpoint for listener creation
          options_clone,
          context_clone.inner().next_handle.clone(), // For unique ID generation within listener
          monitor_tx_clone,
          context_clone.clone(),
          parent_socket_id,
        ) {
          Ok((listener_mailbox, listener_task_handle, resolved_uri)) => {
            let mut core_s_write = core_arc.core_state.write();
            // Use resolved_uri as the key in endpoints map
            if core_s_write.endpoints.contains_key(&resolved_uri) {
              listener_task_handle.abort(); // Abort newly created listener
              bind_result = Err(ZmqError::AddrInUse(resolved_uri));
            } else {
              core_s_write.endpoints.insert(
                resolved_uri.clone(),
                EndpointInfo {
                  mailbox: listener_mailbox,
                  task_handle: Some(listener_task_handle),
                  endpoint_type: EndpointType::Listener,
                  endpoint_uri: resolved_uri.clone(),
                  pipe_ids: None,
                  handle_id: child_actor_handle,
                  target_endpoint_uri: None,
                  is_outbound_connection: false,
                  // Listeners don't have a single ISocketConnection; they manage multiple.
                  // We need a dummy or specialized ISocketConnection here if the field is mandatory.
                  connection_iface: Arc::new(crate::socket::connection_iface::DummyConnection),
                },
              );
              actual_uri_for_state_update = Some(resolved_uri);
              bind_result = Ok(());
            }
          }
          Err(e) => bind_result = Err(e),
        }
      }
    }
    #[cfg(feature = "ipc")]
    Ok(Endpoint::Ipc(ref path_buf, ref uri_from_parse)) => {
      let core_s_read = core_arc.core_state.read();
      if core_s_read.endpoints.contains_key(uri_from_parse) {
        bind_result = Err(ZmqError::AddrInUse(uri_from_parse.clone()));
      } else {
        let monitor_tx_clone = core_s_read.get_monitor_sender_clone();
        let options_clone = core_s_read.options.clone();
        drop(core_s_read);

        let child_actor_handle = context_clone.inner().next_handle();
        match IpcListener::create_and_spawn(
          child_actor_handle,
          endpoint.clone(), // User provided
          path_buf.clone(),
          options_clone,
          context_clone.inner().next_handle.clone(),
          monitor_tx_clone,
          context_clone.clone(),
          parent_socket_id,
        ) {
          Ok((listener_mailbox, listener_task_handle, resolved_uri)) => {
            let mut core_s_write = core_arc.core_state.write();
            if core_s_write.endpoints.contains_key(&resolved_uri) {
              listener_task_handle.abort();
              bind_result = Err(ZmqError::AddrInUse(resolved_uri));
            } else {
              core_s_write.endpoints.insert(
                resolved_uri.clone(),
                EndpointInfo {
                  mailbox: listener_mailbox,
                  task_handle: Some(listener_task_handle),
                  endpoint_type: EndpointType::Listener,
                  endpoint_uri: resolved_uri.clone(),
                  pipe_ids: None,
                  handle_id: child_actor_handle,
                  target_endpoint_uri: None,
                  is_outbound_connection: false,
                  connection_iface: Arc::new(crate::socket::connection_iface::DummyConnection),
                },
              );
              actual_uri_for_state_update = Some(resolved_uri);
              bind_result = Ok(());
            }
          }
          Err(e) => bind_result = Err(e),
        }
      }
    }
    #[cfg(feature = "inproc")]
    Ok(Endpoint::Inproc(ref name)) => {
      let is_already_bound_by_this_socket = core_arc.core_state.read().bound_inproc_names.contains(name);
      if is_already_bound_by_this_socket {
        bind_result = Err(ZmqError::AddrInUse(format!("inproc://{}", name)));
      } else {
        // Check global registry
        if core_arc.context.inner().lookup_inproc(name).is_some() {
          bind_result = Err(ZmqError::AddrInUse(format!("inproc://{} (globally)", name)));
        } else {
          match inproc::bind_inproc(name.clone(), core_arc.clone()).await {
            Ok(()) => {
              actual_uri_for_state_update = Some(format!("inproc://{}", name));
              bind_result = Ok(());
            }
            Err(e) => bind_result = Err(e),
          }
        }
      }
    }
    Err(e) => bind_result = Err(e), // Error from parse_endpoint
    _ => bind_result = Err(ZmqError::UnsupportedTransport(endpoint.to_string())),
  };

  // Post-bind actions: update last_bound_endpoint, send monitor event
  if bind_result.is_ok() {
    if let Some(ref actual_uri) = actual_uri_for_state_update {
      let mut core_s_write = core_arc.core_state.write();
      core_s_write.last_bound_endpoint = Some(actual_uri.clone());
      core_s_write.send_monitor_event(SocketEvent::Listening {
        endpoint: actual_uri.clone(),
      });
    } else {
      // Should not happen if bind_result is Ok
      tracing::error!(handle=core_handle, %endpoint, "Bind OK but no actual_uri_for_state_update. Internal logic error.");
    }
  } else if let Err(ref e) = bind_result {
    core_arc.core_state.read().send_monitor_event(SocketEvent::BindFailed {
      endpoint: endpoint.clone(),
      error_msg: format!("{}", e),
    });
  }

  let _ = reply_tx.send(bind_result);
}

async fn handle_user_connect(
  core_arc: Arc<SocketCore>,
  endpoint_uri: String, // User-provided URI
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, uri = %endpoint_uri, "Processing UserConnect command");

  let parse_result = parse_endpoint(&endpoint_uri);
  match parse_result {
    Ok(Endpoint::Tcp(_, ref parsed_uri_for_connecter)) | Ok(Endpoint::Ipc(_, ref parsed_uri_for_connecter)) => {
      // For TCP/IPC, spawn a Connecter actor.
      // The URI passed to respawn_connecter_actor should be the one used for connection attempts.
      respawn_connecter_actor(core_arc.clone(), parsed_uri_for_connecter.clone()).await;
      let _ = reply_tx.send(Ok(())); // UserConnect is async, Ok(()) means attempt initiated.
    }
    #[cfg(feature = "inproc")]
    Ok(Endpoint::Inproc(ref name)) => {
      // For inproc, connection is handled more directly via events.
      // Spawn a task to handle the inproc connect logic to avoid blocking command_loop.
      let core_arc_clone_for_task = core_arc.clone();
      let name_clone_for_task = name.clone();
      tokio::spawn(async move {
        inproc::connect_inproc(name_clone_for_task, core_arc_clone_for_task, reply_tx).await;
      });
    }
    Err(e) => {
      let _ = reply_tx.send(Err(e));
    }
    _ => {
      // Other endpoint types not connectable or unsupported
      let _ = reply_tx.send(Err(ZmqError::UnsupportedTransport(endpoint_uri)));
    }
  }
}

/// Spawns a new connecter actor for the given target URI.
/// This is called by `handle_user_connect` and potentially by `cleanup_stopped_child_resources` for reconnects.
pub(crate) async fn respawn_connecter_actor(core_arc: Arc<SocketCore>, target_uri: String) {
  let core_handle = core_arc.handle;
  let parent_socket_id = core_handle; // SocketCore is the parent
  let context_clone = core_arc.context.clone();
  tracing::debug!(handle = core_handle, target_uri = %target_uri, "Spawning/Respawning connecter task");

  let parse_res = parse_endpoint(&target_uri);
  match parse_res {
    Ok(Endpoint::Tcp(_, _)) => {
      // Don't need parsed addr here, TcpConnecter re-parses from URI
      let core_s_read = core_arc.core_state.read();
      let options_clone = core_s_read.options.clone();
      let monitor_tx_clone = core_s_read.get_monitor_sender_clone();
      let handle_source_clone = context_clone.inner().next_handle.clone();
      drop(core_s_read);

      let connecter_actor_handle = context_clone.inner().next_handle();
      // TcpConnecter::create_and_spawn itself publishes ActorStarted.
      let _task_handle = TcpConnecter::create_and_spawn(
        connecter_actor_handle,
        target_uri.clone(), // Pass the target_uri
        options_clone,
        handle_source_clone,
        monitor_tx_clone,
        context_clone,
        parent_socket_id,
      );
      // No need to store EndpointInfo for Connecter here; it's short-lived and reports via events.
    }
    #[cfg(feature = "ipc")]
    Ok(Endpoint::Ipc(path_buf, _)) => {
      let core_s_read = core_arc.core_state.read();
      let options_clone = core_s_read.options.clone();
      let monitor_tx_clone = core_s_read.get_monitor_sender_clone();
      let handle_source_clone = context_clone.inner().next_handle.clone();
      drop(core_s_read);

      let connecter_actor_handle = context_clone.inner().next_handle();
      let _task_handle = IpcConnecter::create_and_spawn(
        connecter_actor_handle,
        target_uri.clone(),
        path_buf,
        options_clone,
        handle_source_clone,
        monitor_tx_clone,
        context_clone,
        parent_socket_id,
      );
    }
    Ok(Endpoint::Inproc(_)) => {
      tracing::warn!(handle = core_handle, %target_uri, "Inproc connections are not respawned via Connecter actor mechanism.");
    }
    Err(e) => {
      tracing::error!(handle = core_handle, %target_uri, error = %e, "Failed to parse endpoint for respawning connecter.");
      // Optionally publish ConnectionAttemptFailed here if this was a reconnect attempt.
      let _ = core_arc
        .context
        .event_bus()
        .publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: core_handle,
          target_endpoint_uri: target_uri,
          error_msg: e.to_string(),
        });
    }
    _ => {
      tracing::warn!(handle = core_handle, %target_uri, "Unsupported transport for respawning connecter.");
    }
  }
}

/// Handles the ConnectionAttemptFailed system event.
pub(crate) async fn handle_connect_failed_event(core_arc: Arc<SocketCore>, target_uri: String, error: ZmqError) {
  let core_handle = core_arc.handle;
  tracing::warn!(handle = core_handle, uri = %target_uri, error = %error, "ConnectionAttemptFailed event received.");

  // Emit monitor event
  let monitor_event = SocketEvent::ConnectFailed {
    endpoint: target_uri.clone(),
    error_msg: format!("{}", error),
  };
  core_arc.core_state.read().send_monitor_event(monitor_event);

  // Check if reconnect is configured and error is not fatal
  let should_reconnect = {
    let core_s_read = core_arc.core_state.read();
    core_s_read.options.reconnect_ivl.map_or(false, |d| d != Duration::ZERO) && // reconnect_ivl > 0
        !crate::transport::tcp::is_fatal_connect_error(&error) // Use specific helper
  };

  if should_reconnect {
    tracing::info!(handle = core_handle, uri = %target_uri, "Connection failed, will attempt to respawn connecter (reconnect).");
    // The Connecter actor itself manages the delays (ConnectDelayed, ConnectRetried events).
    // Here, we just ensure a new Connecter task is spawned if the previous one failed definitively.
    // The Connecter's own loop implements the retry delays.
    // If ConnectionAttemptFailed means the Connecter actor *itself* has stopped, then we respawn.
    // This implies ConnectionAttemptFailed is usually published by a Connecter just before it stops.
    respawn_connecter_actor(core_arc, target_uri).await;
  } else {
    tracing::info!(handle = core_handle, uri = %target_uri, "Connection failed, reconnect not enabled or error is fatal.");
  }
}

async fn handle_user_disconnect(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  endpoint: String,
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, %endpoint, "Processing UserDisconnect command");
  let mut disconnect_result = Err(ZmqError::InvalidArgument(format!(
    "Endpoint not found for disconnect: {}",
    endpoint
  )));

  let mut endpoint_info_to_close: Option<(String, Arc<dyn ISocketConnection>)> = None;

  // Find the endpoint by exact URI or by target_endpoint_uri
  if let Some(ep_info) = core_arc.core_state.read().endpoints.get(&endpoint) {
    if ep_info.endpoint_type == EndpointType::Session {
      endpoint_info_to_close = Some((endpoint.clone(), ep_info.connection_iface.clone()));
    }
  } else {
    for (resolved_uri, ep_info) in core_arc.core_state.read().endpoints.iter() {
      if ep_info.endpoint_type == EndpointType::Session && ep_info.target_endpoint_uri.as_deref() == Some(&endpoint) {
        endpoint_info_to_close = Some((resolved_uri.clone(), ep_info.connection_iface.clone()));
        break;
      }
    }
  }

  if let Some((uri_to_close, conn_iface)) = endpoint_info_to_close {
    tracing::info!(handle = core_handle, uri = %uri_to_close, "UserDisconnect: Initiating close for connection.");
    // Closing the connection will trigger ActorStopping from Session/Engine or UringFdError,
    // which will then lead to cleanup_stopped_child_resources.
    match conn_iface.close_connection().await {
      Ok(()) => {
        // The actual removal from endpoints map will happen when ActorStopping is processed.
        // For now, we've initiated the close.
        disconnect_result = Ok(());
      }
      Err(e) => {
        tracing::warn!(handle = core_handle, uri = %uri_to_close, "Error initiating close on disconnect: {}", e);
        disconnect_result = Err(e);
      }
    }
  } else {
    #[cfg(feature = "inproc")]
    if endpoint.starts_with("inproc://") {
      disconnect_result = inproc::disconnect_inproc(&endpoint, core_arc.clone()).await;
    }
    // If not found and not inproc, disconnect_result remains Err(InvalidArgument)
  }

  let _ = reply_tx.send(disconnect_result);
}

async fn handle_user_unbind(
  core_arc: Arc<SocketCore>,
  _socket_logic_strong: &Arc<dyn ISocket>, // Not directly used here yet
  endpoint: String,
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, %endpoint, "Processing UserUnbind command");
  let mut unbind_result = Err(ZmqError::InvalidArgument(format!(
    "Listener endpoint not found for unbind: {}",
    endpoint
  )));

  let mut listener_to_stop: Option<(String, MailboxSender)> = None;
  {
    let core_s_read = core_arc.core_state.read();
    if let Some(ep_info) = core_s_read.endpoints.get(&endpoint) {
      if ep_info.endpoint_type == EndpointType::Listener {
        listener_to_stop = Some((endpoint.clone(), ep_info.mailbox.clone()));
      } else {
        unbind_result = Err(ZmqError::InvalidArgument(
          "Cannot unbind a non-listener endpoint.".into(),
        ));
      }
    }
  } // core_state read lock dropped

  if let Some((uri, listener_mailbox)) = listener_to_stop {
    tracing::info!(handle = core_handle, uri = %uri, "UserUnbind: Sending Stop to Listener actor.");
    // Sending Stop will cause Listener to shutdown, publish ActorStopping.
    // The ActorStopping handler will then remove it from endpoints map.
    if listener_mailbox.send(Command::Stop).await.is_err() {
      tracing::warn!(handle = core_handle, uri = %uri, "Failed to send Stop to Listener on unbind (already stopped?).");
      // If send fails, listener might be gone. We might still need to remove from map if it wasn't cleaned yet.
      // However, ActorStopping should handle the map removal.
      unbind_result = Ok(()); // Consider unbind successful if listener is already gone.
    } else {
      unbind_result = Ok(()); // Stop command sent successfully.
    }
  } else {
    #[cfg(feature = "inproc")]
    if endpoint.starts_with("inproc://") {
      let name_part = endpoint.strip_prefix("inproc://").unwrap_or("");
      let mut was_removed_locally = false;
      if !name_part.is_empty() {
        was_removed_locally = core_arc.core_state.write().bound_inproc_names.remove(name_part);
      }
      if was_removed_locally {
        inproc::unbind_inproc(name_part, &core_arc.context).await; // Global unregister
        core_arc.core_state.read().send_monitor_event(SocketEvent::Closed {
          endpoint: endpoint.clone(),
        });
        unbind_result = Ok(());
      } else if !was_removed_locally && !name_part.is_empty() {
        unbind_result = Err(ZmqError::InvalidArgument(format!(
          "Inproc name '{}' not bound by this socket",
          name_part
        )));
      } else {
        unbind_result = Err(ZmqError::InvalidEndpoint(endpoint));
      }
    }
    // If not found and not inproc, unbind_result remains Err(InvalidArgument)
  }
  let _ = reply_tx.send(unbind_result);
}

async fn handle_set_option(
  core_arc: Arc<SocketCore>,
  socket_logic: &Arc<dyn ISocket>,
  option: i32,
  value: &[u8],
) -> Result<(), ZmqError> {
  tracing::debug!(
    handle = core_arc.handle,
    option = option,
    value_len = value.len(),
    "Setting option"
  );

  // First, try to set as a pattern-specific option
  let handled = socket_logic.set_pattern_option(option, value).await;
  match handled {
    Ok(()) => return Ok(()), // Pattern handled it
    Err(ZmqError::UnsupportedOption(_)) => {
      // Not a pattern option, try as a core option
    }
    Err(e) => return Err(e), // Pattern returned a different error
  }

  update_core_option(&core_arc, |opts| {
    options::apply_core_option_value(opts, option, value)
  })
}

pub(crate) fn update_core_option<F>(core_arc: &SocketCore, applier: F) -> Result<(), ZmqError>
where
  F: FnOnce(&mut SocketOptions) -> Result<(), ZmqError>,
{
  let mut core_s_write = core_arc.core_state.write();

  // Clone the current options
  let mut new_options_instance = (*core_s_write.options).clone();

  // Apply the changes using the passed-in callback
  let apply_result = applier(&mut new_options_instance);

  if apply_result.is_ok() {
    core_s_write.options = Arc::new(new_options_instance);
  }

  apply_result
}

async fn handle_get_option(
  core_arc: Arc<SocketCore>,
  socket_logic: &Arc<dyn ISocket>,
  option: i32,
) -> Result<Vec<u8>, ZmqError> {
  tracing::debug!(handle = core_arc.handle, option = option, "Getting option");

  match socket_logic.get_pattern_option(option).await {
    Ok(v_val) => return Ok(v_val),
    Err(ZmqError::UnsupportedOption(_)) => {}
    Err(e_val) => return Err(e_val),
  }

  let core_s_read = core_arc.core_state.read();
  // Delegate to the new helper in options.rs
  options::retrieve_core_option_value(&core_s_read.options, &core_s_read, option)
}

async fn handle_user_monitor(
  core_arc: Arc<SocketCore>,
  monitor_tx: MonitorSender,
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  core_arc.core_state.write().monitor_tx = Some(monitor_tx);
  let _ = reply_tx.send(Ok(()));
}
