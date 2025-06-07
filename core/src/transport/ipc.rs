// core/src/transport/ipc.rs

#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::context::Context;
// Use the specific helper for IPC engines, which uses ZmtpEngineCoreStd<UnixStream>
use crate::error::ZmqError;
use crate::runtime::{
  mailbox, system_events::ConnectionInteractionModel, ActorDropGuard, ActorType, Command,
  MailboxReceiver as GenericMailboxReceiver, MailboxSender as GenericMailboxSender, SystemEvent,
};
use crate::sessionx::actor::SessionConnectionActorX;
use crate::sessionx::states::ActorConfigX;
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::SocketOptions;
use crate::socket::{ISocket, ZmtpEngineConfig};

use core::fmt;
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
use tokio::sync::{broadcast, Semaphore};
use tokio::task::{Id as TaskId, JoinHandle};
use tokio::time::sleep; // timeout for connect attempt

// --- IpcListener Actor ---
pub(crate) struct IpcListener {
  handle: usize,
  endpoint: String,
  path: PathBuf,
  mailbox_receiver: GenericMailboxReceiver,
  listener_handle: Option<JoinHandle<()>>, // Option to allow taking it in run_command_loop
  context: Context,
  parent_socket_id: usize,
  socket_logic: Arc<dyn ISocket>,
}

impl fmt::Debug for IpcListener {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("IpcListener")
      .field("handle", &self.handle)
      .field("endpoint", &self.endpoint)
      .field("path", &self.path)
      .field("mailbox_receiver_is_closed", &self.mailbox_receiver.is_closed())
      .field("listener_handle_is_some", &self.listener_handle.is_some())
      .field("context_present", &true) // Avoid printing full context
      .field("parent_socket_id", &self.parent_socket_id)
      .field("socket_logic_present", &true) // Placeholder for Arc<dyn ISocket>
      .finish()
  }
}

impl IpcListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String, // User-provided URI
    path: PathBuf,        // Parsed path from URI
    options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> Result<(GenericMailboxSender, JoinHandle<()>, String), ZmqError> {
    let actor_type = ActorType::Listener;
    let capacity = context.inner().get_actor_mailbox_capacity();
    let (tx, rx) = mailbox(capacity);

    match std::fs::remove_file(&path) {
      Ok(_) => tracing::debug!(listener_handle = handle, path = ?path, "Removed existing IPC socket file."),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      Err(e) => {
        tracing::warn!(listener_handle = handle, path = ?path, error = %e, "Failed to remove existing IPC socket file.")
      }
    }

    let listener = TokioUnixListener::bind(&path).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri))?;
    let resolved_uri = endpoint_uri.clone();
    tracing::info!(listener_handle = handle, path = ?path, uri = %resolved_uri, "IPC Listener bound successfully");

    let max_conns = options.max_connections.unwrap_or(std::usize::MAX);
    let conn_limiter = Arc::new(Semaphore::new(max_conns.max(1)));

    let accept_loop_parent_hdl = handle;
    let accept_loop_hdl_id = context.inner().next_handle();

    let accept_loop_task_jh = tokio::spawn(IpcListener::run_accept_loop(
      accept_loop_hdl_id,
      accept_loop_parent_hdl,
      resolved_uri.clone(),
      Arc::new(listener),
      options.clone(),
      socket_logic.clone(),
      context_handle_source.clone(),
      monitor_tx.clone(),
      context.clone(),
      parent_socket_id,
      conn_limiter.clone(),
    ));

    let listener_actor = IpcListener {
      handle,
      endpoint: resolved_uri.clone(),
      path,
      mailbox_receiver: rx,
      listener_handle: Some(accept_loop_task_jh),
      context: context.clone(),
      parent_socket_id,
      socket_logic,
    };

    let cmd_loop_jh = tokio::spawn(listener_actor.run_command_loop(parent_socket_id));

    Ok((tx, cmd_loop_jh, resolved_uri))
  }

  async fn run_command_loop(mut self, parent_handle_id: usize) {
    let listener_cmd_loop_handle = self.handle;
    let endpoint_uri_clone_log = self.endpoint.clone();
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();
    let listener_abort_handle = self.listener_handle.take();

    let mut actor_drop_guard = ActorDropGuard::new(
      self.context.clone(),
      listener_cmd_loop_handle,
      ActorType::Listener,
      Some(endpoint_uri_clone_log.clone()),
      Some(parent_handle_id),
    );

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command loop started");
    let mut final_error_for_actor_stopping: Option<ZmqError> = None;

    let _loop_result: Result<(), ()> = async {
      loop {
        tokio::select! {
          biased;
          event_result = system_event_rx.recv() => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received ContextTerminating, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); } break;
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, parent_id = self.parent_socket_id, "IPC Listener received SocketClosing for parent, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); } break;
              }
              Ok(_) => {}
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, skipped = n, "System event bus lagged for IPC Listener command loop!");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus lagged (IPC)".into())); break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "System event bus closed unexpectedly for IPC Listener command loop!");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus closed (IPC)".into())); break;
              }
            }
          }
          cmd_result = self.mailbox_receiver.recv() => {
            match cmd_result {
              Ok(Command::Stop) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received Stop command");
                if let Some(h) = &listener_abort_handle { h.abort(); } break;
              }
              Ok(other_cmd) => tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received unhandled command: {:?}", other_cmd.variant_name()),
              Err(_) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command mailbox closed, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                if final_error_for_actor_stopping.is_none() { final_error_for_actor_stopping = Some(ZmqError::Internal("Listener command mailbox closed by peer (IPC)".into())); }
                break;
              }
            }
          }
        }
      }
      Ok(())
    }.await;

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command loop finished, awaiting accept loop task.");
    if let Some(listener_join_handle) = listener_abort_handle {
      if let Err(e) = listener_join_handle.await {
        if !e.is_cancelled() {
          tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task panicked: {:?}", e);
          if final_error_for_actor_stopping.is_none() {
            final_error_for_actor_stopping = Some(ZmqError::Internal(format!(
              "Listener accept loop panicked (IPC): {:?}",
              e
            )));
          }
        } else {
          tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task cancelled.");
        }
      } else {
        tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task joined cleanly.");
      }
    }

    if let Some(err) = final_error_for_actor_stopping {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }

    tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command loop actor fully stopped.");
  }

  async fn run_accept_loop(
    accept_loop_handle: usize,
    _listener_cmd_loop_handle: usize,
    endpoint_uri: String, // Resolved listener URI
    listener: Arc<TokioUnixListener>,
    socket_options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
    connection_limiter: Arc<Semaphore>,
  ) {
    let mut actor_drop_guard = ActorDropGuard::new(
      context.clone(),
      accept_loop_handle,
      ActorType::AcceptLoop,
      Some(endpoint_uri.clone()),
      Some(parent_socket_core_id),
    );
    tracing::debug!(handle = accept_loop_handle, uri = %endpoint_uri, "IPC Accept loop started.");
    let mut loop_error_to_report: Option<ZmqError> = None;

    loop {
      let permit = match connection_limiter.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => {
          loop_error_to_report = Some(ZmqError::Internal("IPC Connection limiter closed".into()));
          break;
        }
      };

      match listener.accept().await {
        Ok((unix_stream, _ipc_peer_addr_os_specific)) => {
          let _permit_guard = permit;
          let peer_addr_str = format!("ipc-peer-fd-{}", unix_stream.as_raw_fd());
          tracing::info!(
            "Accepted new IPC connection (peer: {}) for listener {}",
            peer_addr_str,
            endpoint_uri
          );

          if let Some(ref tx) = monitor_tx {
            let _ = tx.try_send(SocketEvent::Accepted {
              endpoint: endpoint_uri.clone(),
              peer_addr: peer_addr_str.clone(),
            });
          }

          // IPC always uses the standard SessionBase + ZmtpEngineCoreStd<UnixStream> path.
          // The io_uring.session_enabled option from SocketOptions does not apply to IPC transport.
          let connection_specific_uri = format!("ipc://{}", peer_addr_str);

          tokio::spawn({
            let context_clone = context.clone();
            let socket_options_clone = socket_options.clone();
            let monitor_tx_clone = monitor_tx.clone();
            let handle_source_clone = handle_source.clone();
            let actual_connected_uri = connection_specific_uri.clone();
            let logical_uri = endpoint_uri.clone();
            let socket_logic = socket_logic.clone();

            async move {
              let _permit_scoped_for_task = _permit_guard;
              // Variables for the common event publishing logic
              let mut interaction_model_for_event: Option<ConnectionInteractionModel> = None;
              let mut managing_actor_task_id_for_event: Option<TaskId> = None;
              let mut setup_successful = true;

              // IPC doesn't have an io_uring path like TCP. It always uses an actor.
              // <<< MODIFIED START [Replace old SessionBase/Engine with SCA] >>>
              let sca_handle_id = handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let actor_conf = ActorConfigX {
                context: context_clone.clone(),
                monitor_tx: monitor_tx_clone,
                logical_target_endpoint_uri: logical_uri.clone(),
                connected_endpoint_uri: actual_connected_uri.clone(),
                is_server_role: true, // Accepted connections are server role
              };
              // Derive ZmtpEngineConfig from the full SocketOptions
              let engine_conf = Arc::new(ZmtpEngineConfig::from(&*socket_options_clone));

              let (command_sender_for_sca, command_receiver_for_sca) =
                mailbox(context_clone.inner().get_actor_mailbox_capacity());

              // unix_stream is moved into SessionConnectionActorX
              let sca_task_handle = SessionConnectionActorX::create_and_spawn(
                sca_handle_id,
                parent_socket_core_id,
                unix_stream,
                actor_conf,
                engine_conf,
                command_receiver_for_sca,
                socket_logic.clone(),
              );

              interaction_model_for_event = Some(ConnectionInteractionModel::ViaSca {
                sca_mailbox: command_sender_for_sca,
                sca_handle_id,
              });
              managing_actor_task_id_for_event = Some(sca_task_handle.id());
              // connection_iface_for_event is None, SocketCore creates it
              // <<< MODIFIED END >>>

              // Common event publishing logic (same as in TcpListener)
              if setup_successful {
                // setup_successful is true unless an error occurred above
                if let Some(inter_model) = interaction_model_for_event {
                  let event = SystemEvent::NewConnectionEstablished {
                    parent_core_id: parent_socket_core_id,
                    endpoint_uri: actual_connected_uri.clone(),
                    target_endpoint_uri: logical_uri.clone(),
                    connection_iface: None, // SocketCore creates the iface
                    interaction_model: inter_model,
                    managing_actor_task_id: managing_actor_task_id_for_event,
                  };
                  if context_clone.event_bus().publish(event).is_err() {
                    tracing::error!("Failed to publish NewConnection(SCA/IPC) for {}", actual_connected_uri);
                    // Abort spawned SCA if publish fails
                    // Managing_actor_task_id_for_event holds the TaskId of SCA
                    // Direct abort via TaskId isn't straightforward, SCA should handle lack of AttachPipes.
                    if let Some(task_id) = managing_actor_task_id_for_event {
                      tracing::warn!(
                        "Pub NewConn failed for IPC. SCA task {:?} might self-terminate.",
                        task_id
                      );
                    }
                  }
                } else {
                  // Should not happen if SCA path always sets the model
                  tracing::error!(
                    "IPC: Inconsistent state - setup_successful true but no interaction model for {}",
                    actual_connected_uri
                  );
                }
              } else {
                tracing::warn!(
                  "IPC Connection setup failed for {}, NewConnectionEstablished not published.",
                  actual_connected_uri
                );
              }
            }
          });
        }
        Err(e) => {
          drop(permit);
          tracing::error!("Error accepting IPC connection (listener {}): {}", endpoint_uri, e);
          if let Some(ref tx) = monitor_tx {
            let _ = tx.try_send(SocketEvent::AcceptFailed {
              endpoint: endpoint_uri.clone(),
              error_msg: e.to_string(),
            });
          }
          if is_fatal_ipc_accept_error(&e) {
            loop_error_to_report = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break;
          }
          sleep(Duration::from_millis(100)).await;
        }
      }
    }

    if let Some(err) = loop_error_to_report {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }

    tracing::info!("IPC Accept loop {} fully stopped.", accept_loop_handle);
  }
}

impl Drop for IpcListener {
  // ... (Drop implementation remains the same)
  fn drop(&mut self) {
    tracing::debug!(listener_handle = self.handle, path = ?self.path, "Dropping IpcListener, cleaning up IPC socket file");
    if let Some(listener_join_handle) = self.listener_handle.take() {
      if !listener_join_handle.is_finished() {
        listener_join_handle.abort();
        tracing::debug!(listener_handle = self.handle, path = ?self.path, "Aborted accept loop task in IpcListener Drop.");
      }
    }
    match std::fs::remove_file(&self.path) {
      Ok(_) => tracing::debug!(listener_handle = self.handle, path = ?self.path, "Removed IPC socket file."),
      Err(e) if e.kind() == io::ErrorKind::NotFound => {
        tracing::trace!(listener_handle = self.handle, path = ?self.path, "IPC socket file not found during drop.")
      }
      Err(e) => {
        tracing::warn!(listener_handle = self.handle, path = ?self.path, error = %e, "Failed to remove IPC socket file during drop.")
      }
    }
  }
}

// --- IpcConnecter Actor ---
pub(crate) struct IpcConnecter {
  handle: usize,
  endpoint_uri: String,
  path: PathBuf,
  context_options: Arc<SocketOptions>,
  socket_logic: Arc<dyn ISocket>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  context: Context,
  parent_socket_id: usize,
}

impl fmt::Debug for IpcConnecter {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("IpcConnecter")
      .field("handle", &self.handle)
      .field("endpoint_uri", &self.endpoint_uri)
      .field("path", &self.path)
      .field("context_options", &self.context_options) // SocketOptions is Debug
      .field("socket_logic_present", &true) // Placeholder
      .field("context_present", &true)
      .field("parent_socket_id", &self.parent_socket_id)
      .finish()
  }
}

impl IpcConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> JoinHandle<()> {
    let connecter_actor = IpcConnecter {
      handle,
      endpoint_uri: endpoint_uri.clone(),
      path,
      context_options: options,
      context_handle_source,
      context: context.clone(),
      parent_socket_id,
      socket_logic,
    };
    let task_join_handle = tokio::spawn(connecter_actor.run_connect_attempt(monitor_tx, parent_socket_id));
    task_join_handle
  }

  async fn run_connect_attempt(self, monitor_tx: Option<MonitorSender>, _parent_socket_id_unused: usize) {
    let connecter_handle = self.handle;
    let endpoint_uri_original = self.endpoint_uri.clone(); // User's target URI

    let mut actor_drop_guard = ActorDropGuard::new(
      self.context.clone(),
      connecter_handle,
      ActorType::Connecter,
      Some(endpoint_uri_original.clone()),
      Some(self.parent_socket_id),
    );
    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_original, path = ?self.path, "IPC Connecter actor started.");

    // Variables to hold the outcome of the connection attempt
    let mut connection_outcome: Result<
      (ConnectionInteractionModel, Option<TaskId>, String /* actual_uri */),
      ZmqError,
    >;

    // For IPC, we typically attempt to connect once with a timeout.
    // More complex retry logic like TCP's isn't standard for local IPC paths.
    // Use handshake_ivl as a connect timeout, or a default.
    let connect_timeout = self
      .context_options
      .handshake_ivl
      .unwrap_or_else(|| Duration::from_secs(5));
    let mut system_event_rx = self.context.event_bus().subscribe(); // For early abort

    let connect_future = UnixStream::connect(&self.path);

    tokio::select! {
        biased;
        _ = async { // Early abort if context/socket is closing
            loop {
                match system_event_rx.recv().await {
                    Ok(SystemEvent::ContextTerminating) => break,
                    Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => break,
                    Ok(_) => continue,
                    Err(_) => break, // Channel closed or lagged
                }
            }
        } => {
            connection_outcome = Err(ZmqError::Internal("IPC Connect aborted by system event.".into()));
        }
        connect_result = tokio::time::timeout(connect_timeout, connect_future) => {
            match connect_result {
                Ok(Ok(unix_stream)) => { // Successfully connected within timeout
                    #[cfg(unix)]
                    let peer_id_str = format!("ipc-fd-{}", unix_stream.as_raw_fd());
                    #[cfg(not(unix))] // Fallback if AsRawFd is not available/not unix
                    let peer_id_str = format!("ipc-conn-{}", self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed));

                    let actual_connected_uri = format!("ipc://{}", peer_id_str);
                    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_original, actual = %actual_connected_uri, "IPC Connect successful");

                    if let Some(ref tx) = monitor_tx {
                    let _ = tx.try_send(SocketEvent::Connected {
                        endpoint: endpoint_uri_original.clone(),
                        peer_addr: actual_connected_uri.clone(),
                    });
                    }

                    // Spawn SessionConnectionActorX
                    let sca_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let actor_conf = ActorConfigX {
                        context: self.context.clone(),
                        monitor_tx: monitor_tx.clone(), // Pass along the monitor
                        logical_target_endpoint_uri: endpoint_uri_original.clone(),
                        connected_endpoint_uri: actual_connected_uri.clone(),
                        is_server_role: false, // Outgoing connection is client role
                    };
                    let engine_conf = Arc::new(ZmtpEngineConfig::from(&*self.context_options));

                    let (command_sender_for_sca, command_receiver_for_sca) =
                        mailbox(self.context.inner().get_actor_mailbox_capacity());

                    let sca_task_handle = SessionConnectionActorX::create_and_spawn(
                        sca_handle_id,
                        self.parent_socket_id,
                        unix_stream, // Stream moved here
                        actor_conf,
                        engine_conf,
                        command_receiver_for_sca,
                        self.socket_logic.clone(),
                    );

                    let interaction_model = ConnectionInteractionModel::ViaSca {
                        sca_mailbox: command_sender_for_sca,
                        sca_handle_id,
                    };
                    connection_outcome = Ok((interaction_model, Some(sca_task_handle.id()), actual_connected_uri));
                }
                Ok(Err(e)) => { // Connect failed (not timeout related to tokio::time::timeout itself)
                    connection_outcome = Err(ZmqError::from_io_endpoint(e, &endpoint_uri_original));
                }
                Err(_timeout_elapsed) => { // Timeout from tokio::time::timeout
                    connection_outcome = Err(ZmqError::Timeout);
                }
            }
        }
    }

    // Process the connection_outcome
    match connection_outcome {
      Ok((interaction_model, managing_actor_task_id, actual_uri)) => {
        let event = SystemEvent::NewConnectionEstablished {
          parent_core_id: self.parent_socket_id,
          endpoint_uri: actual_uri,
          target_endpoint_uri: endpoint_uri_original.clone(),
          connection_iface: None, // SocketCore creates the iface
          interaction_model,
          managing_actor_task_id,
        };
        if self.context.event_bus().publish(event).is_err() {
          tracing::error!(
            "IPC Connecter: Failed to publish NewConnectionEstablished for {}.",
            endpoint_uri_original
          );
          actor_drop_guard.set_error(ZmqError::Internal("IPC Failed to publish NewConnection".into()));
          // TODO: Abort SCA if spawned and publish failed. SCA should self-terminate if AttachPipes not received.
        } else {
          actor_drop_guard.waive(); // Successful connection and event publish
        }
      }
      Err(err) => {
        tracing::error!(handle = connecter_handle, uri = %endpoint_uri_original, error = %err, "IPC Connect final failure");
        if let Some(ref tx) = monitor_tx {
          let _ = tx.try_send(SocketEvent::ConnectFailed {
            endpoint: endpoint_uri_original.clone(),
            error_msg: err.to_string(),
          });
        }
        // Only publish ConnectionAttemptFailed if it wasn't an internal abort
        if !matches!(&err, ZmqError::Internal(s) if s.contains("aborted by system event")) {
          let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed {
            parent_core_id: self.parent_socket_id,
            target_endpoint_uri: endpoint_uri_original.clone(),
            error_msg: err.to_string(),
          });
        }
        actor_drop_guard.set_error(err);
      }
    }

    tracing::info!(handle = connecter_handle, uri = %self.endpoint_uri, "IPC Connecter actor fully stopped.");
  }
}

fn is_fatal_ipc_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}