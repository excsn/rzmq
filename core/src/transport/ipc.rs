// core/src/transport/ipc.rs

#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::context::Context;
// Use the specific helper for IPC engines, which uses ZmtpEngineCoreStd<UnixStream>
use crate::error::ZmqError;
use crate::runtime::{
  mailbox, system_events::ConnectionInteractionModel, ActorDropGuard, ActorType, Command,
  MailboxReceiver as GenericMailboxReceiver, MailboxSender as GenericMailboxSender, SystemEvent,
};
use crate::socket::connection_iface::ISocketConnection;
use crate::transport::ipc::create_and_spawn_ipc_engine_wrapper as create_and_spawn_ipc_engine;
// EngineConnectionType for Command::Attach for standard path
use crate::runtime::command::EngineConnectionType as CommandEngineConnectionType;
use crate::session::SessionBase;
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::SocketOptions;

use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

// use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
use tokio::sync::{broadcast, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout}; // timeout for connect attempt

// --- IpcListener Actor ---
#[derive(Debug)]
pub(crate) struct IpcListener {
  handle: usize,
  endpoint: String,
  path: PathBuf,
  mailbox_receiver: GenericMailboxReceiver,
  listener_handle: Option<JoinHandle<()>>, // Option to allow taking it in run_command_loop
  context: Context,
  parent_socket_id: usize,
}

impl IpcListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String, // User-provided URI
    path: PathBuf,        // Parsed path from URI
    options: Arc<SocketOptions>,
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
    };

    let cmd_loop_jh = tokio::spawn(listener_actor.run_command_loop(parent_socket_id));

    Ok((tx, cmd_loop_jh, resolved_uri))
  }

  // ... (run_command_loop remains the same)
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
            let actual_connected_uri_ipc = connection_specific_uri.clone();
            let logical_uri = endpoint_uri.clone();

            async move {
              let _permit_scoped_for_task = _permit_guard;

              let session_hdl_id = handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_hdl_id = handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

              let (session_cmd_mailbox, session_task_hdl) = SessionBase::create_and_spawn(
                session_hdl_id,
                actual_connected_uri_ipc.clone(),
                logical_uri.clone(),
                monitor_tx_clone.clone(),
                context_clone.clone(),
                parent_socket_core_id,
              );
              let managing_actor_task_id_val = Some(session_task_hdl.id());

              // Attach Engine to Session
              let mut setup_successful = true; // Assume success initially
              let (engine_mb, engine_task_hdl) = create_and_spawn_ipc_engine(
                engine_hdl_id,
                session_cmd_mailbox.clone(),
                unix_stream, // unix_stream moved here
                socket_options_clone.clone(),
                true, // is_server = true for accepted connections
                &context_clone,
                session_hdl_id,
              );
              let attach_engine_cmd = Command::Attach {
                connection: CommandEngineConnectionType::Standard {
                  engine_mailbox: engine_mb,
                },
                engine_handle: Some(engine_hdl_id),
                engine_task_handle: Some(engine_task_hdl),
              };
              if session_cmd_mailbox.send(attach_engine_cmd).await.is_err() {
                tracing::error!("AttachEngine to IPC Session {} failed.", session_hdl_id);
                session_task_hdl.abort();
                setup_successful = false;
              }

              if setup_successful {
                // <<< MODIFIED [connection_iface is None; interaction_model provides session_actor_handle_id] >>>
                let interaction_model = ConnectionInteractionModel::ViaSessionActor {
                  session_actor_mailbox: session_cmd_mailbox,
                  session_actor_handle_id: session_hdl_id,
                };

                let event = SystemEvent::NewConnectionEstablished {
                  parent_core_id: parent_socket_core_id,
                  endpoint_uri: actual_connected_uri_ipc.clone(),
                  target_endpoint_uri: logical_uri.clone(),
                  connection_iface: None, // SocketCore will create the SessionConnection
                  interaction_model,
                  managing_actor_task_id: managing_actor_task_id_val,
                };
                if context_clone.event_bus().publish(event).is_err() {
                  tracing::error!(
                    "Failed to publish NewConnectionEstablished for IPC {}",
                    actual_connected_uri_ipc
                  );
                  session_task_hdl.abort();
                }
              } else {
                tracing::warn!(
                  "IPC Connection setup failed for {}, NewConnectionEstablished not published.",
                  actual_connected_uri_ipc
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
// ... (IpcConnecter struct definition remains the same)
#[derive(Debug)]
pub(crate) struct IpcConnecter {
  handle: usize,
  endpoint_uri: String,
  path: PathBuf,
  context_options: Arc<SocketOptions>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  context: Context,
  parent_socket_id: usize,
}

impl IpcConnecter {
  // ... (create_and_spawn method remains the same)
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
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
    };
    let task_join_handle = tokio::spawn(connecter_actor.run_connect_attempt(monitor_tx, parent_socket_id));
    task_join_handle
  }

  async fn run_connect_attempt(self, monitor_tx: Option<MonitorSender>, parent_handle_id: usize) {
    let connecter_handle = self.handle;
    let endpoint_uri_clone = self.endpoint_uri.clone();
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    let mut actor_drop_guard = ActorDropGuard::new(
      self.context.clone(),
      connecter_handle,
      ActorType::Connecter,
      Some(endpoint_uri_clone.clone()),
      Some(parent_handle_id),
    );

    tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, path = ?self.path, "IPC Connecter actor started.");

    let mut system_event_rx = self.context.event_bus().subscribe();
    let mut should_abort_attempt_due_to_event = false;

    // IPC connect attempt logic with timeout
    let connect_with_timeout = timeout(Duration::from_secs(5), UnixStream::connect(&self.path));

    tokio::select! {
      biased;
      _ = async {
        loop {
          match system_event_rx.recv().await {
            Ok(SystemEvent::ContextTerminating) | Ok(SystemEvent::SocketClosing { .. }) => {
              should_abort_attempt_due_to_event = true; break;
            }
            Ok(_) => {}
            Err(_) => { should_abort_attempt_due_to_event = true; break; }
          }
        }
      } => {
        if should_abort_attempt_due_to_event {
           tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC connect attempt aborted by system event during connect window.");
           final_error_for_actor_stop = Some(ZmqError::Internal("IPC Connecter aborted by system event".into()));
        }
      }
      connect_result = connect_with_timeout => {
        if should_abort_attempt_due_to_event {
             tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC connection completed but abort was flagged. Ignoring result.");
             final_error_for_actor_stop = Some(ZmqError::Internal("IPC Connecter aborted by system event (completed during abort)".into()));
        } else {
          match connect_result {
            Ok(Ok(unix_stream)) => { // Successfully connected within timeout
              let peer_addr_str = format!("ipc-peer-fd-{}", unix_stream.as_raw_fd());
              tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, path = ?self.path, peer = %peer_addr_str, "IPC Connect successful");
              if let Some(ref tx) = monitor_tx { let _ = tx.try_send(SocketEvent::Connected { endpoint: endpoint_uri_clone.clone(), peer_addr: peer_addr_str.clone() }); }

              let session_hdl_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_hdl_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let actual_connected_uri_ipc = format!("ipc://{}", peer_addr_str);
              let logical_uri = endpoint_uri_clone.clone();

              let (session_cmd_mailbox, session_task_hdl) = SessionBase::create_and_spawn(
                session_hdl_id, actual_connected_uri_ipc.clone(), logical_uri.clone(), monitor_tx.clone(),
                self.context.clone(), self.parent_socket_id,
              );
              let managing_actor_task_id_val = Some(session_task_hdl.id());

              let mut setup_ok = true;
              let (engine_mb, engine_task_hdl) = create_and_spawn_ipc_engine(
                engine_hdl_id, session_cmd_mailbox.clone(), unix_stream, self.context_options.clone(),
                false, &self.context, session_hdl_id,
              );
              let attach_engine_cmd = Command::Attach {
                connection: CommandEngineConnectionType::Standard { engine_mailbox: engine_mb },
                engine_handle: Some(engine_hdl_id), engine_task_handle: Some(engine_task_hdl),
              };
              if session_cmd_mailbox.send(attach_engine_cmd).await.is_err() {
                session_task_hdl.abort(); setup_ok = false;
                final_error_for_actor_stop = Some(ZmqError::Internal("Failed AttachEngine to IPC Session".into()));
              }

              if setup_ok {
                // <<< MODIFIED [connection_iface is None; interaction_model provides session_actor_handle_id] >>>
                let interaction_model = ConnectionInteractionModel::ViaSessionActor {
                  session_actor_mailbox: session_cmd_mailbox,
                  session_actor_handle_id: session_hdl_id,
                };
                let event = SystemEvent::NewConnectionEstablished {
                  parent_core_id: self.parent_socket_id, endpoint_uri: actual_connected_uri_ipc.clone(),
                  target_endpoint_uri: endpoint_uri_clone.clone(),
                  connection_iface: None, // SocketCore will construct SessionConnection
                  interaction_model,
                  managing_actor_task_id: managing_actor_task_id_val,
                };
                if self.context.event_bus().publish(event).is_err() {
                  final_error_for_actor_stop = Some(ZmqError::Internal("Failed to publish NewConnectionEstablished (IPC)".into()));
                  session_task_hdl.abort();
                }
              }
            }
            Ok(Err(e)) => { // Connect failed (not timeout)
              let current_error = ZmqError::from_io_endpoint(e, &endpoint_uri_clone);
              final_error_for_actor_stop = Some(current_error.clone());
              tracing::error!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, error = %current_error, "IPC Connect failed");
              if let Some(ref tx) = monitor_tx { let _ = tx.try_send(SocketEvent::ConnectFailed { endpoint: endpoint_uri_clone.clone(), error_msg: current_error.to_string() }); }
              let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed { parent_core_id: self.parent_socket_id, target_endpoint_uri: endpoint_uri_clone.clone(), error_msg: current_error.to_string() });
            }
            Err(_timeout_err) => { // Timeout from connect_with_timeout
              final_error_for_actor_stop = Some(ZmqError::Timeout);
              tracing::warn!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC Connect timed out");
              if let Some(ref tx) = monitor_tx { let _ = tx.try_send(SocketEvent::ConnectFailed { endpoint: endpoint_uri_clone.clone(), error_msg: "Timeout".to_string() }); }
              let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed { parent_core_id: self.parent_socket_id, target_endpoint_uri: endpoint_uri_clone.clone(), error_msg: "Timeout".to_string() });
            }
          }
        }
      }
    }

    tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC Connecter actor finished.");
    if let Some(err) = final_error_for_actor_stop {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }
    tracing::info!(connecter_handle = connecter_handle, uri = %self.endpoint_uri, "IPC Connecter actor fully stopped.");
  }
}

fn is_fatal_ipc_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

// ... (create_and_spawn_ipc_engine_wrapper remains the same)
pub(crate) fn create_and_spawn_ipc_engine_wrapper(
  engine_handle_id: usize,
  session_cmd_mailbox: GenericMailboxSender,
  unix_stream: UnixStream,
  socket_options: Arc<SocketOptions>,
  is_server_role: bool,
  context: &Context,
  session_handle_id: usize,
) -> (GenericMailboxSender, JoinHandle<()>) {
  let (engine_command_mailbox, engine_task_join_handle) = crate::engine::zmtp_ipc::create_and_spawn_ipc_engine(
    engine_handle_id,
    session_cmd_mailbox,
    unix_stream,
    socket_options,
    is_server_role,
    context,
    session_handle_id,
  );
  (engine_command_mailbox, engine_task_join_handle)
}
