use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
#[cfg(any(feature = "io-uring", feature = "inproc"))]
use crate::runtime::ActorType;
use crate::runtime::system_events::ConnectionInteractionModel;
use crate::runtime::{Command, MailboxSender, SystemEvent};
use crate::sessionx::ScaConnectionIface;
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;

#[cfg(feature = "io-uring")]
use crate::socket::core::pipe_manager;

use crate::socket::core::state::{EndpointInfo, EndpointType, ShutdownPhase};
use crate::socket::core::{SocketCore, shutdown};
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{self, *};
use crate::transport::endpoint::{Endpoint, parse_endpoint};
#[cfg(feature = "inproc")]
use crate::transport::inproc;
#[cfg(feature = "inproc")]
use crate::transport::inproc::DirectInprocConnection;
#[cfg(feature = "ipc")]
use crate::transport::ipc::{IpcConnecter, IpcListener};
use crate::transport::tcp::{TcpConnecter, TcpListener};

use fibre::oneshot;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::Id as TaskId;

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
        tracing::info!(
          handle = core_handle,
          "Processing UserClose command during shutdown."
        );
        shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
        let _ = reply_tx.send(Ok(()));
      }
      Command::Stop => {
        tracing::info!(
          handle = core_handle,
          "Processing Stop command during shutdown."
        );
        shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
      }
      // These events can arrive from the UringWorker after we've already initiated a shutdown.
      // It's safe to ignore them as the shutdown process will clean up the corresponding FD.
      #[cfg(feature = "io-uring")]
      Command::UringFdError { endpoint_uri, error } => {
        tracing::debug!(handle=core_handle, %endpoint_uri, %error, "UringFdError during shutdown — treating as ActorStopping.");
        let handle_id_opt = core_arc
          .core_state
          .read()
          .endpoints
          .get(&endpoint_uri)
          .map(|ep| ep.handle_id);
        shutdown::handle_actor_stopping_event(
          core_arc.clone(),
          socket_logic_strong,
          handle_id_opt.unwrap_or(0),
          ActorType::Session,
          Some(&endpoint_uri),
          Some(&error),
        )
        .await;
      }
      #[cfg(feature = "io-uring")]
      Command::UringConnectionEstablished { connection_iface, endpoint_uri, .. } => {
        tracing::warn!(handle = core_handle, %endpoint_uri, "UringConnectionEstablished during shutdown — closing orphaned connection.");
        if let Err(e) = connection_iface.close_connection().await {
          tracing::error!(handle = core_handle, %endpoint_uri, "Error closing orphaned UringFd connection: {}", e);
        }
      }
      // Connection arrived while shutting down — clean up the orphaned connection immediately.
      Command::NewConnectionEstablished {
        endpoint_uri,
        connection_iface,
        interaction_model,
        ..
      } => {
        tracing::warn!(
          handle = core_handle,
          new_conn_uri = %endpoint_uri,
          "SocketCore ignoring NewConnectionEstablished during shutdown. Closing orphaned connection."
        );
        if let Some(iface) = connection_iface {
          if let Err(e) = iface.close_connection().await {
            tracing::error!(handle = core_handle, uri = %endpoint_uri, "Error closing orphaned connection during shutdown: {}", e);
          }
        }
        if let ConnectionInteractionModel::ViaSca { sca_mailbox, .. } = interaction_model {
          let _ = sca_mailbox.try_send(Command::Stop);
        }
      }
      // For other commands, if shutting down, reply with an error or ignore.
      Command::UserBind { reply_tx, .. }
      | Command::UserConnect { reply_tx, .. }
      | Command::UserDisconnect { reply_tx, .. }
      | Command::UserUnbind { reply_tx, .. }
      | Command::UserSetOpt { reply_tx, .. }
      | Command::UserMonitor { reply_tx, .. } => {
        tracing::warn!(handle = core_handle, cmd_name = %command_name_str, "Command ignored: SocketCore is shutting down.");
        let _ = reply_tx.send(Err(ZmqError::InvalidState(
          "Socket is shutting down".into(),
        )));
      }
      Command::UserRecv { reply_tx } => {
        let _ = reply_tx.send(Err(ZmqError::InvalidState(
          "Socket is shutting down".into(),
        )));
      }
      Command::UserGetOpt { reply_tx, .. } => {
        let _ = reply_tx.send(Err(ZmqError::InvalidState(
          "Socket is shutting down".into(),
        )));
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
      handle_user_bind(core_arc, socket_logic_strong.clone(), endpoint, reply_tx).await;
    }
    Command::UserConnect { endpoint, reply_tx } => {
      handle_user_connect(core_arc, socket_logic_strong.clone(), endpoint, reply_tx).await;
    }
    Command::UserDisconnect { endpoint, reply_tx } => {
      handle_user_disconnect(core_arc, endpoint, reply_tx).await;
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
      let _ = reply_tx
        .send(handle_set_option(core_arc.clone(), socket_logic_strong, option, &value).await);
    }
    Command::UserGetOpt { option, reply_tx } => {
      let _ = reply_tx.send(handle_get_option(core_arc.clone(), socket_logic_strong, option).await);
    }
    Command::UserMonitor {
      monitor_tx,
      reply_tx,
    } => {
      handle_user_monitor(core_arc.clone(), monitor_tx, reply_tx).await;
    }
    Command::UserClose { reply_tx } => {
      tracing::info!(
        handle = core_handle,
        "SocketCore received UserClose command."
      );
      // Publish event first, then initiate shutdown
      shutdown::publish_socket_closing_event(&core_arc.context, core_handle).await;
      shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
      let _ = reply_tx.send(Ok(())); // Acknowledge close initiation
    }
    Command::Stop => {
      // Direct stop command to SocketCore
      tracing::info!(
        handle = core_handle,
        "SocketCore received direct Stop command."
      );
      // Publish event first, then initiate shutdown
      shutdown::publish_socket_closing_event(&core_arc.context, core_handle).await;
      shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
    }

    // --- io_uring Specific Commands (URI-keyed, FD-agnostic) ---

    /// Emitted by the UringWorker when the ZMTP handshake completes. Atomically registers
    /// the connection and attaches the pipes — no prior NewConnectionEstablished needed.
    #[cfg(feature = "io-uring")]
    Command::UringConnectionEstablished {
      endpoint_uri,
      target_endpoint_uri,
      connection_iface,
      peer_identity,
      fd,
    } => {
      let is_outbound = {
        let cs = core_arc.core_state.read();
        !cs.endpoints.values().any(|ep| {
          ep.endpoint_type == EndpointType::Listener && ep.endpoint_uri == target_endpoint_uri
        })
      };

      let synthetic_read_id = core_arc.context.inner().next_handle();
      let synthetic_write_id = core_arc.context.inner().next_handle();

      let endpoint_info = EndpointInfo {
        mailbox: core_arc.command_sender(),
        task_handle: None,
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint_uri.clone(),
        pipe_ids: Some((synthetic_write_id, synthetic_read_id)),
        handle_id: synthetic_read_id,
        target_endpoint_uri: Some(target_endpoint_uri),
        is_outbound_connection: is_outbound,
        connection_iface,
        peer_socket_type: None,
      };

      {
        let mut cs = core_arc.core_state.write();
        if let Some(old) = cs.endpoints.insert(endpoint_uri.clone(), endpoint_info) {
          tracing::warn!(handle=core_handle, %endpoint_uri, "Overwrote existing EndpointInfo on UringConnectionEstablished.");
          if let Some(h) = old.task_handle { h.abort(); }
        }
        cs.pipe_read_id_to_endpoint_uri.insert(synthetic_read_id, endpoint_uri.clone());
      }

      tracing::info!(
        handle=core_handle, %endpoint_uri, synth_r=synthetic_read_id, synth_w=synthetic_write_id,
        ?peer_identity, "UringConnectionEstablished: pipes attached, handshake complete."
      );

      socket_logic_strong
        .pipe_attached(synthetic_read_id, synthetic_write_id, peer_identity.as_ref().map(|b| b.as_ref()))
        .await;
      socket_logic_strong.update_peer_identity(synthetic_read_id, peer_identity).await;

      // Direct Ingress Handoff: extract the PipeMessageSender from the socket pattern and
      // deliver it to the worker handler so it can push decoded messages straight to the
      // ReadyPipeQueue — no intermediate UringPipeReader task needed.
      if let Some(ingress_sender) = socket_logic_strong.get_incoming_pipe_sender(synthetic_read_id) {
        match crate::uring::global_state::get_global_uring_worker_op_tx() {
          Ok(worker_tx) => {
            let ud = core_arc.context.inner().next_handle() as u64;
            let (reply_tx, reply_rx) = oneshot::oneshot::<Result<crate::io_uring_backend::ops::UringOpCompletion, crate::ZmqError>>();
            let req = crate::io_uring_backend::ops::UringOpRequest::AttachIngressSender {
              user_data: ud,
              fd,
              ingress_sender,
              reply_tx,
            };
            if let Err(e) = worker_tx.send(req).await {
              tracing::warn!(handle=core_handle, %endpoint_uri, "AttachIngressSender send failed: {:?}", e);
            } else if let Ok(reply) = reply_rx.recv().await {
              if let Ok(crate::io_uring_backend::ops::UringOpCompletion::AttachIngressSenderSuccess { .. }) = reply {
                tracing::debug!(handle=core_handle, %endpoint_uri, "AttachIngressSender: success");
              } else {
                tracing::warn!(handle=core_handle, %endpoint_uri, "AttachIngressSender: unexpected reply: {:?}", reply);
              }
            }
          }
          Err(e) => {
            tracing::warn!(handle=core_handle, %endpoint_uri, "AttachIngressSender: worker tx unavailable: {}", e);
          }
        }
      }

      core_arc.core_state.read().send_monitor_event(SocketEvent::HandshakeSucceeded {
        endpoint: endpoint_uri,
      });
    }

    #[cfg(feature = "io-uring")]
    Command::UringFdError { endpoint_uri, error } => {
      let (conn_iface_opt, synthetic_read_id_opt, handle_id_opt) = {
        let cs = core_arc.core_state.read();
        let ep = cs.endpoints.get(&endpoint_uri);
        (
          ep.map(|e| e.connection_iface.clone()),
          ep.and_then(|e| e.pipe_ids.map(|pids| pids.1)),
          ep.map(|e| e.handle_id),
        )
      };
      tracing::warn!(handle=core_handle, %endpoint_uri, %error, "UringFdError — closing connection.");

      if let Some(iface) = conn_iface_opt {
        if let Err(e) = iface.close_connection().await {
          tracing::warn!(handle=core_handle, %endpoint_uri, "close_connection error: {}", e);
        }
      }
      if let Some(s_read_id) = synthetic_read_id_opt {
        socket_logic_strong.pipe_detached(s_read_id).await;
      }
      pipe_manager::cleanup_stopped_child_resources(
        core_arc.clone(),
        socket_logic_strong,
        handle_id_opt.unwrap_or(0),
        ActorType::Session,
        Some(&endpoint_uri),
        Some(&error),
        current_shutdown_phase != ShutdownPhase::Running,
      )
      .await;
    }

    Command::NewConnectionEstablished {
      endpoint_uri,
      target_endpoint_uri,
      connection_iface,
      interaction_model,
      managing_actor_task_id,
    } => {
      handle_new_connection_established(
        core_arc,
        socket_logic_strong,
        endpoint_uri,
        target_endpoint_uri,
        connection_iface,
        interaction_model,
        managing_actor_task_id,
      )
      .await?;
    }

    // Commands NOT expected by SocketCore's main mailbox:
    _ => {
      tracing::error!(handle = core_handle, cmd_name = %command_name_str, "SocketCore received UNEXPECTED command type on its mailbox!");
    }
  }
  Ok(())
}

async fn handle_user_bind(
  core_arc: Arc<SocketCore>,
  socket_logic: Arc<dyn ISocket>,
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
          socket_logic,
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
                  peer_socket_type: None,
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
          socket_logic,
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
                  peer_socket_type: None,
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
      let is_already_bound_by_this_socket =
        core_arc.core_state.read().bound_inproc_names.contains(name);
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
    core_arc
      .core_state
      .read()
      .send_monitor_event(SocketEvent::BindFailed {
        endpoint: endpoint.clone(),
        error_msg: format!("{}", e),
      });
  }

  let _ = reply_tx.send(bind_result);
}

async fn handle_user_connect(
  core_arc: Arc<SocketCore>,
  socket_logic: Arc<dyn ISocket>,
  endpoint_uri: String, // User-provided URI
  reply_tx: oneshot::Sender<Result<(), ZmqError>>,
) {
  let core_handle = core_arc.handle;
  tracing::debug!(handle = core_handle, uri = %endpoint_uri, "Processing UserConnect command");

  let parse_result = parse_endpoint(&endpoint_uri);
  match parse_result {
    Ok(Endpoint::Tcp(_, ref parsed_uri_for_connecter))
    | Ok(Endpoint::Ipc(_, ref parsed_uri_for_connecter)) => {
      // For TCP/IPC, spawn a Connecter actor.
      // The URI passed to respawn_connecter_actor should be the one used for connection attempts.
      respawn_connecter_actor(
        core_arc.clone(),
        socket_logic,
        parsed_uri_for_connecter.clone(),
      )
      .await;
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
pub(crate) async fn respawn_connecter_actor(
  core_arc: Arc<SocketCore>,
  socket_logic: Arc<dyn ISocket>,
  target_uri: String,
) {
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

      let current_attempts = core_s_read
        .reconnect_states
        .get(&target_uri)
        .map(|s| s.current_attempts)
        .unwrap_or(0);

      let handle_source_clone = context_clone.inner().next_handle.clone();
      drop(core_s_read);

      let connecter_actor_handle = context_clone.inner().next_handle();
      // TcpConnecter::create_and_spawn itself publishes ActorStarted.
      let _task_handle = TcpConnecter::create_and_spawn(
        connecter_actor_handle,
        target_uri.clone(), // Pass the target_uri
        options_clone,
        socket_logic,
        handle_source_clone,
        monitor_tx_clone,
        context_clone,
        parent_socket_id,
        current_attempts,
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
        socket_logic,
        handle_source_clone,
        monitor_tx_clone,
        context_clone,
        parent_socket_id,
      );
    }
    Ok(Endpoint::Inproc(_)) => {
      tracing::warn!(handle = core_handle, %target_uri, "Inproc connections are not respawned via Connecter actor mechanism.");
    }
    Err(err) => {
      tracing::error!(handle = core_handle, %target_uri, error = %err, "Failed to parse endpoint for respawning connecter.");
      // Optionally publish ConnectionAttemptFailed here if this was a reconnect attempt.
      let _ = core_arc
        .context
        .event_bus()
        .publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: core_handle,
          target_endpoint_uri: target_uri,
          error: err,
        });
    }
    _ => {
      tracing::warn!(handle = core_handle, %target_uri, "Unsupported transport for respawning connecter.");
    }
  }
}

/// Handles the ConnectionAttemptFailed system event.
pub(crate) async fn handle_connect_failed_event(
  core_arc: Arc<SocketCore>,
  _socket_logic: Arc<dyn ISocket + 'static>, // Logic argument no longer used directly here
  target_uri: String,
  error: ZmqError,
) {
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
    core_s_read
      .options
      .reconnect_ivl
      .map_or(false, |d| d != Duration::ZERO)
      && !crate::transport::tcp::is_fatal_connect_error(&error)
  };

  if should_reconnect {
    let mut state = core_arc.core_state.write();
    let options = state.options.clone();

    // Default to 100ms if not set, consistent with ZMQ defaults
    let base = options
      .reconnect_ivl
      .unwrap_or(std::time::Duration::from_millis(100));
    let max = options
      .reconnect_ivl_max
      .unwrap_or(std::time::Duration::from_secs(60));

    let recon_state = state
      .reconnect_states
      .entry(target_uri.clone())
      .or_default();
    let delay = recon_state.on_connection_failure(base, max);

    tracing::info!(
      handle = core_handle,
      uri = %target_uri,
      attempt = recon_state.current_attempts,
      next_attempt_in = ?delay,
      "Connection failed. Scheduling reconnect via passive backoff."
    );
  } else {
    tracing::info!(handle = core_handle, uri = %target_uri, "Connection failed, reconnect not enabled or error is fatal.");
  }
}

async fn handle_user_disconnect(
  core_arc: Arc<SocketCore>,
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
      if ep_info.endpoint_type == EndpointType::Session
        && ep_info.target_endpoint_uri.as_deref() == Some(&endpoint)
      {
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
        was_removed_locally = core_arc
          .core_state
          .write()
          .bound_inproc_names
          .remove(name_part);
      }
      if was_removed_locally {
        inproc::unbind_inproc(name_part, &core_arc.context).await; // Global unregister
        core_arc
          .core_state
          .read()
          .send_monitor_event(SocketEvent::Closed {
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

async fn handle_new_connection_established(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  endpoint_uri_from_event: String,
  target_endpoint_uri_from_event: String,
  connection_iface_from_event_opt: Option<Arc<dyn ISocketConnection>>,
  interaction_model_from_event: ConnectionInteractionModel,
  _managing_actor_task_id: Option<TaskId>,
) -> Result<(), ZmqError> {
  let core_handle = core_arc.handle;

  let is_outbound_this_core_initiated = {
    let core_s_guard = core_arc.core_state.read();
    !core_s_guard.endpoints.values().any(|ep_info| {
      ep_info.endpoint_type == EndpointType::Listener
        && ep_info.endpoint_uri == target_endpoint_uri_from_event
    })
  };

  match interaction_model_from_event {
    ConnectionInteractionModel::ViaSca {
      sca_mailbox,
      sca_handle_id,
    } => {
      tracing::debug!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        sca_actor_id = sca_handle_id,
        "NewConnectionEstablished: SessionConnectionActorX path."
      );

      if connection_iface_from_event_opt.is_some() {
        tracing::warn!(handle = core_handle, conn_uri = %endpoint_uri_from_event, "ViaSca received unexpected pre-existing ISocketConnection. Ignoring.");
      }

      let (tx_core_to_sca, rx_sca_from_core) =
        fibre::mpmc::bounded_async::<FrameBatch>(core_arc.core_state.read().options.sndhwm.max(1));

      let core_write_id = core_arc.context.inner().next_handle();
      let core_read_id = core_arc.context.inner().next_handle();

      let sndtimeo_snapshot = core_arc.core_state.read().options.sndtimeo;

      let sca_iface = Arc::new(ScaConnectionIface::new(
        sca_mailbox.clone(),
        sca_handle_id,
        tx_core_to_sca.clone(),
        core_write_id,
        sndtimeo_snapshot,
      ));

      let endpoint_info = EndpointInfo {
        mailbox: sca_mailbox.clone(),
        task_handle: None,
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint_uri_from_event.clone(),
        pipe_ids: Some((core_write_id, core_read_id)),
        handle_id: sca_handle_id,
        target_endpoint_uri: Some(target_endpoint_uri_from_event),
        is_outbound_connection: is_outbound_this_core_initiated,
        peer_socket_type: None,
        connection_iface: sca_iface,
      };

      {
        core_arc
          .core_state
          .write()
          .pipes_tx
          .insert(core_write_id, tx_core_to_sca);

        let old_info_result = core_arc
          .core_state
          .write()
          .endpoints
          .insert(endpoint_uri_from_event.clone(), endpoint_info);

        if let Some(old_info) = old_info_result {
          tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnection (SCA).");
          tokio::spawn(async move {
            let _ = old_info.connection_iface.close_connection().await;
            if let Some(h) = old_info.task_handle {
              h.abort();
            }
          });
        }
        core_arc
          .core_state
          .write()
          .pipe_read_id_to_endpoint_uri
          .insert(core_read_id, endpoint_uri_from_event.clone());
      }

      // pipe_attached FIRST so the socket pattern registers the pipe and creates the sender.
      tracing::debug!(handle=core_handle, sca_id=sca_handle_id, conn_uri=%endpoint_uri_from_event, "Notifying ISocket of pipe attachment for new SCA connection.");
      socket_logic_strong
        .pipe_attached(core_read_id, core_write_id, None)
        .await;

      // Retrieve the ingress sender the socket created inside pipe_attached.
      let incoming_pipe_sender = socket_logic_strong.get_incoming_pipe_sender(core_read_id);

      let attach_cmd = Command::ScaInitializePipes {
        sca_handle_id,
        rx_from_core: rx_sca_from_core,
        core_pipe_read_id_for_incoming_routing: core_read_id,
        incoming_pipe_sender,
      };

      if sca_mailbox.send(attach_cmd).await.is_err() {
        return Err(ZmqError::Internal(format!(
          "Failed to send ScaInitializePipes to SCA {}",
          sca_handle_id
        )));
      }

      tracing::debug!(
        handle = core_handle,
        sca_id = sca_handle_id,
        core_w_id = core_write_id,
        core_r_id = core_read_id,
        "Sent ScaInitializePipes to SessionConnectionActorX."
      );
    }

    #[cfg(feature = "inproc")]
    ConnectionInteractionModel::ViaDirectInproc { local_rx, peer_identity } => {
      tracing::debug!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        "NewConnectionEstablished: ViaDirectInproc path."
      );

      let rx = match local_rx.lock().unwrap().take() {
        Some(r) => r,
        None => {
          return Err(ZmqError::Internal("ViaDirectInproc: local_rx already consumed".into()));
        }
      };

      let connection_iface = match connection_iface_from_event_opt {
        Some(iface) => iface,
        None => {
          return Err(ZmqError::Internal("ViaDirectInproc requires connection_iface".into()));
        }
      };

      // Extract the peer-facing sender from the DirectInprocConnection so we can also
      // put it in pipes_tx — the socket pattern's outbound routing goes through pipes_tx.
      let tx_to_peer = match connection_iface.as_any().downcast_ref::<DirectInprocConnection>() {
        Some(c) => c.peer_queue_sender.clone(),
        None => {
          return Err(ZmqError::Internal(
            "ViaDirectInproc: connection_iface is not a DirectInprocConnection".into(),
          ));
        }
      };

      let core_write_id = core_arc.context.inner().next_handle();
      let core_read_id = core_arc.context.inner().next_handle();
      let reader_task_id = core_arc.context.inner().next_handle();

      // Dummy closed mailbox — no SCAX actor to command for direct inproc.
      let (dummy_mailbox_tx, _dropped_rx) =
        fibre::mpmc::bounded_async::<Command>(1);

      let endpoint_info = EndpointInfo {
        mailbox: dummy_mailbox_tx,
        task_handle: None,
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint_uri_from_event.clone(),
        pipe_ids: Some((core_write_id, core_read_id)),
        handle_id: reader_task_id,
        target_endpoint_uri: Some(target_endpoint_uri_from_event),
        is_outbound_connection: is_outbound_this_core_initiated,
        peer_socket_type: None,
        connection_iface,
      };

      {
        let mut cs = core_arc.core_state.write();
        cs.pipes_tx.insert(core_write_id, tx_to_peer);
        if let Some(old_info) = cs.endpoints.insert(endpoint_uri_from_event.clone(), endpoint_info) {
          tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnection (DirectInproc).");
          tokio::spawn(async move {
            let _ = old_info.connection_iface.close_connection().await;
          });
        }
        cs.pipe_read_id_to_endpoint_uri
          .insert(core_read_id, endpoint_uri_from_event.clone());
      }

      tracing::debug!(handle=core_handle, conn_uri=%endpoint_uri_from_event, "Notifying ISocket of pipe attachment for direct inproc connection.");
      socket_logic_strong
        .pipe_attached(core_read_id, core_write_id, peer_identity.as_deref())
        .await;

      // None is valid for send-only socket types (e.g. Push). The reader task still runs
      // to drain the channel and handle the connection lifecycle — it just discards frames.
      let pipe_sender_opt = socket_logic_strong.get_incoming_pipe_sender(core_read_id);

      let event_bus = core_arc.context.event_bus().clone();
      let endpoint_uri_clone = endpoint_uri_from_event.clone();
      let task_handle = tokio::spawn(async move {
        let mut accumulator = FrameBatch::new();
        loop {
          match rx.recv().await {
            Ok(batch) => {
              accumulator.extend(batch);
              if let Some(last_msg) = accumulator.last_mut() {
                if !last_msg.is_more() {
                  let complete_batch = std::mem::replace(&mut accumulator, FrameBatch::new());
                  if let Some(ref sender) = pipe_sender_opt {
                    if sender.send(complete_batch).await.is_err() {
                      break;
                    }
                  }
                }
              }
            }
            Err(_) => break,
          }
        }
        tracing::debug!(reader_task_id, uri = %endpoint_uri_clone, "DirectInproc reader task exiting.");
        let _ = event_bus.publish(SystemEvent::ActorStopping {
          handle_id: reader_task_id,
          actor_type: ActorType::PipeReader,
          endpoint_uri: Some(endpoint_uri_clone),
          parent_id: Some(core_handle),
          error: None,
        });
      });

      core_arc
        .core_state
        .write()
        .pipe_reader_task_handles
        .insert(core_read_id, task_handle);

      tracing::debug!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        core_w_id = core_write_id,
        core_r_id = core_read_id,
        reader_id = reader_task_id,
        "ViaDirectInproc pipes wired."
      );
    }

    // ViaUringFd connections are now handled atomically via Command::UringConnectionEstablished,
    // emitted by the worker only after the ZMTP handshake completes. NewConnectionEstablished
    // is only dispatched for ViaSca connections by the transport layer.
    #[cfg(feature = "io-uring")]
    ConnectionInteractionModel::ViaUringFd { .. } => {
      tracing::warn!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        "Unexpected ViaUringFd in NewConnectionEstablished — ignored (UringConnectionEstablished handles this path)."
      );
    }
    #[cfg(not(feature = "io-uring"))]
    ConnectionInteractionModel::ViaUringFd { _fd_placeholder } => {
      tracing::error!(handle = core_handle, "FATAL: ViaUringFd model when io-uring is disabled.");
      return Err(ZmqError::Internal("Invalid connection model for build configuration".into()));
    }
  }
  Ok(())
}

