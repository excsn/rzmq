// src/transport/ipc.rs

#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::context::Context;
use crate::engine; // For engine::zmtp_ipc
use crate::error::ZmqError;
use crate::runtime::{
  self,
  mailbox,
  ActorDropGuard,
  ActorType,
  Command,
  EventBus,
  MailboxReceiver,
  MailboxSender,
  SystemEvent, // System events for lifecycle management
};
use crate::session::SessionBase; // For spawning Session actors
use crate::socket::events::{MonitorSender, SocketEvent}; // For emitting monitor events
use crate::socket::options::SocketOptions; // Configuration types
use std::io;
use std::os::fd::AsRawFd; // For creating a synthetic peer address from file descriptor
use std::path::PathBuf; // For IPC path manipulation
use std::sync::Arc;
use std::time::Duration;
use tokio::fs; // For asynchronous file system operations (removing socket file) - though Drop uses std::fs
use tokio::net::{UnixListener as TokioUnixListener, UnixStream}; // Tokio's Unix domain socket types
use tokio::sync::broadcast; // For EventBus subscription
use tokio::task::JoinHandle; // For managing spawned tasks
use tokio::time::{sleep, timeout}; // For delays and timeouts

// --- IpcListener Actor ---
/// Manages a listening Unix Domain Socket, accepts incoming connections,
/// and spawns Session/Engine pairs for them.
#[derive(Debug)]
pub(crate) struct IpcListener {
  handle: usize,                           // Handle of the Listener's command loop actor.
  endpoint: String,                        // The URI this listener is bound to.
  path: PathBuf,                           // The file system path of the socket, for cleanup in Drop.
  mailbox_receiver: MailboxReceiver,       // For receiving Stop commands.
  listener_handle: Option<JoinHandle<()>>, // JoinHandle for the accept loop task.
  context: Context,                        // The rzmq Context.
  parent_socket_id: usize,                 // Handle ID of the parent SocketCore.
}

impl IpcListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let actor_type = ActorType::Listener;
    let (tx, rx) = mailbox();

    // Attempt to remove an existing socket file before binding.
    match std::fs::remove_file(&path) {
      Ok(_) => {
        tracing::debug!(listener_handle = handle, path = ?path, "Removed existing IPC socket file before binding.")
      }
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      Err(e) => {
        tracing::warn!(listener_handle = handle, path = ?path, error = %e, "Failed to remove existing IPC socket file before binding. Bind may fail.");
      }
    }

    let listener = TokioUnixListener::bind(&path).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri))?;
    tracing::info!(listener_handle = handle, path = ?path, uri = %endpoint_uri, "IPC Listener bound successfully");
    let listener_arc = Arc::new(listener);

    // Prepare arguments for the accept loop task.
    let accept_listener_arc = listener_arc.clone();
    let accept_handle_source_clone = context_handle_source.clone();
    let accept_options_clone = options.clone();
    let accept_monitor_tx_clone = monitor_tx.clone();
    let endpoint_uri_for_accept_loop = endpoint_uri.clone();
    let path_for_accept_loop = path.clone();
    let context_for_accept_loop = context.clone();
    let parent_socket_id_for_accept_loop = parent_socket_id;
    let accept_loop_parent_handle = handle;

    let accept_loop_handle_id = context_for_accept_loop.inner().next_handle();
    let accept_loop_actor_type = ActorType::AcceptLoop;

    let accept_loop_task_join_handle = tokio::spawn(IpcListener::run_accept_loop(
      accept_loop_handle_id,
      accept_loop_parent_handle,
      endpoint_uri_for_accept_loop,
      path_for_accept_loop,
      accept_listener_arc,
      accept_options_clone,
      accept_handle_source_clone,
      accept_monitor_tx_clone,
      context_for_accept_loop.clone(),
      parent_socket_id_for_accept_loop,
    ));
    context.publish_actor_started(
      accept_loop_handle_id,
      accept_loop_actor_type,
      Some(accept_loop_parent_handle),
    );

    let listener_actor = IpcListener {
      handle,
      endpoint: endpoint_uri,
      path, // Store path for Drop.
      mailbox_receiver: rx,
      listener_handle: Some(accept_loop_task_join_handle),
      context: context.clone(),
      parent_socket_id,
    };

    let command_loop_join_handle = tokio::spawn(listener_actor.run_command_loop());
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));

    Ok((tx, command_loop_join_handle))
  }

  async fn run_command_loop(mut self) {
    let listener_cmd_loop_handle = self.handle;
    let listener_cmd_loop_actor_type = ActorType::Listener;
    let endpoint_uri_clone_log = self.endpoint.clone();
    let path_clone_log = self.path.clone();
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();
    let mut listener_abort_handle = self.listener_handle.take();

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, path = ?path_clone_log, "IPC Listener command loop started");

    let mut final_error_for_actor_stopping: Option<ZmqError> = None;

    let _loop_result: Result<(), ()> = async {
      loop {
        tokio::select! {
          biased;
          event_result = system_event_rx.recv() => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received ContextTerminating, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                break;
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, parent_id = self.parent_socket_id, "IPC Listener received SocketClosing for parent, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                break;
              }
              Ok(_) => { /* Ignore other events. */ }
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, skipped = n, "System event bus lagged for IPC Listener command loop!");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus lagged (IPC)".into()));
                break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "System event bus closed unexpectedly for IPC Listener command loop!");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus closed (IPC)".into()));
                break;
              }
            }
          }
          cmd_result = self.mailbox_receiver.recv() => {
            match cmd_result {
              Ok(Command::Stop) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received Stop command");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                break;
              }
              Ok(other_cmd) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener received unhandled command: {:?}", other_cmd.variant_name());
              }
              Err(_) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command mailbox closed, stopping accept loop.");
                if let Some(h) = &listener_abort_handle { h.abort(); }
                if final_error_for_actor_stopping.is_none() {
                    final_error_for_actor_stopping = Some(ZmqError::Internal("Listener command mailbox closed by peer (IPC)".into()));
                }
                break;
              }
            }
          }
        }
      }
      Ok(())
    }.await;

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command loop finished, awaiting accept loop task.");
    if let Some(listener_handle) = self.listener_handle.take() {
      if let Err(e) = listener_handle.await {
        if !e.is_cancelled() {
          tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task panicked: {:?}", e);
          if final_error_for_actor_stopping.is_none() {
            final_error_for_actor_stopping = Some(ZmqError::Internal(format!(
              "Listener accept loop panicked (IPC): {:?}",
              e
            )));
          }
        } else {
          tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task was cancelled as expected.");
        }
      } else {
        tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task joined cleanly.");
      }
    }

    self.context.publish_actor_stopping(
      listener_cmd_loop_handle,
      listener_cmd_loop_actor_type,
      Some(self.endpoint.clone()), // Pass owned endpoint URI from struct.
      final_error_for_actor_stopping,
    );
    tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener command loop actor fully stopped.");
    // Drop impl for IpcListener handles socket file removal.
  }

  async fn run_accept_loop(
    accept_loop_handle: usize,
    listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    path: PathBuf, // Path for logging.
    listener: Arc<TokioUnixListener>,
    socket_options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
  ) {
    let accept_loop_actor_type = ActorType::AcceptLoop;
    let actor_drop_guard = ActorDropGuard::new(
      context.clone(),
      accept_loop_handle,
      accept_loop_actor_type,
      Some(endpoint_uri.clone()),
    );

    tracing::debug!(handle = accept_loop_handle, parent_handle = listener_cmd_loop_handle, uri = %endpoint_uri, path = ?path, "IPC Listener accept loop started");
    let mut loop_error_to_report: Option<ZmqError> = None;

    loop {
      tokio::select! {
        biased;
        accept_result = listener.accept() => {
          match accept_result {
            Ok((unix_stream, _peer_addr_os_specific)) => {
              let peer_addr_str = format!("ipc-peer-fd-{}", unix_stream.as_raw_fd());
              tracing::info!(listener_accept_loop_handle = accept_loop_handle, path = ?path, peer = %peer_addr_str, "Accepted new IPC connection");

              if let Some(ref tx) = monitor_tx {
                let event = SocketEvent::Accepted { endpoint: endpoint_uri.clone(), peer_addr: peer_addr_str.clone() };
                let tx_clone = tx.clone();
                tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
              }

              // UnixStream does not have options like NODELAY or KEEPALIVE.

              let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let connection_specific_uri = format!("ipc://{}", peer_addr_str);

              let (session_cmd_mailbox, session_task_join_handle) = SessionBase::create_and_spawn(
                session_handle_id,
                connection_specific_uri.clone(),
                monitor_tx.clone(),
                context.clone(),
                parent_socket_core_id,
              );

              let (engine_mailbox, engine_task_join_handle_inner) = create_and_spawn_ipc_engine(
                engine_handle_id,
                session_cmd_mailbox.clone(),
                unix_stream,
                socket_options.clone(),
                true, // Server-side engine.
                &context,
                session_handle_id, // Engine's parent is Session.
              );

              let attach_cmd = Command::Attach {
                engine_mailbox,
                engine_handle: Some(engine_handle_id),
                engine_task_handle: Some(engine_task_join_handle_inner),
              };
              if session_cmd_mailbox.send(attach_cmd).await.is_err() {
                tracing::error!(session_handle = session_handle_id, uri = %connection_specific_uri, "Failed to send Attach to new IPC Session.");
                // If attach fails, Session will stop, Engine might too. Accept loop continues.
                continue;
              }

              let event = SystemEvent::NewConnectionEstablished {
                parent_core_id: parent_socket_core_id,
                endpoint_uri: connection_specific_uri.clone(),
                target_endpoint_uri: endpoint_uri.clone(),
                session_mailbox: session_cmd_mailbox,
                session_handle_id,
                session_task_id: session_task_join_handle.id(),
              };
              if context.event_bus().publish(event).is_err() {
                tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, "Failed to publish NewConnectionEstablished for IPC.");
                loop_error_to_report = Some(ZmqError::Internal("Event bus publish failed for IPC NewConnectionEstablished".into()));
                break;
              }
            }
            Err(e) => {
              tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, error = %e, "Error accepting new IPC connection");
              if let Some(ref tx) = monitor_tx {
                let event = SocketEvent::AcceptFailed { endpoint: endpoint_uri.clone(), error_msg: format!("{}", e) };
                let tx_clone = tx.clone();
                tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
              }
              if is_fatal_ipc_accept_error(&e) {
                tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, error = %e, "Fatal error in IPC accept loop, stopping.");
                loop_error_to_report = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
                break;
              }
              sleep(Duration::from_millis(100)).await;
            }
          }
        }
        _ = tokio::time::sleep(Duration::from_secs(60 * 5)) => {
          tracing::warn!(handle = accept_loop_handle, uri = %endpoint_uri, "IPC Listener accept loop timed out (safety break).");
          loop_error_to_report = Some(ZmqError::Internal("IPC Accept loop safety timeout".into()));
          break;
        }
      } // end select!
    } // end loop

    tracing::debug!(handle = accept_loop_handle, parent_handle = listener_cmd_loop_handle, uri = %endpoint_uri, "IPC Listener accept loop finished");
    actor_drop_guard.waive();
    context.publish_actor_stopping(
      accept_loop_handle,
      accept_loop_actor_type,
      Some(endpoint_uri),
      loop_error_to_report,
    );
  }
}

impl Drop for IpcListener {
  fn drop(&mut self) {
    tracing::debug!(listener_handle = self.handle, path = ?self.path, "Dropping IpcListener, attempting to clean up IPC socket file");
    match std::fs::remove_file(&self.path) {
      Ok(_) => {
        tracing::debug!(listener_handle = self.handle, path = ?self.path, "Removed IPC socket file successfully.")
      }
      Err(e) if e.kind() == io::ErrorKind::NotFound => {
        tracing::trace!(listener_handle = self.handle, path = ?self.path, "IPC socket file not found during drop.");
      }
      Err(e) => {
        tracing::warn!(listener_handle = self.handle, path = ?self.path, error = %e, "Failed to remove IPC socket file during drop.");
      }
    }

    if let Some(listener_handle) = self.listener_handle.take() {
      if !listener_handle.is_finished() {
        listener_handle.abort();
        tracing::debug!(listener_handle = self.handle, path = ?self.path, "Aborted accept loop task in IpcListener Drop.");
      }
    }
  }
}
// --- End IpcListener ---

// --- IpcConnecter Actor ---
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
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> (MailboxSender, JoinHandle<()>) {
    let actor_type = ActorType::Connecter;
    let (tx, _rx) = mailbox(); // Mailbox for connecter (receiver currently unused).

    let connecter_actor = IpcConnecter {
      handle,
      endpoint_uri: endpoint_uri.clone(),
      path,
      context_options: options,
      context_handle_source,
      context: context.clone(),
      parent_socket_id,
    };

    let task_join_handle = tokio::spawn(connecter_actor.run_connect_attempt(monitor_tx));
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));

    (tx, task_join_handle)
  }

  async fn run_connect_attempt(mut self, monitor_tx: Option<MonitorSender>) {
    let connecter_handle = self.handle;
    let connecter_actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint_uri.clone();
    let path_clone_log = self.path.clone();
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, path = ?path_clone_log, "IPC Connecter actor started connection attempt");

    let mut system_event_rx = self.context.event_bus().subscribe();
    let mut should_abort_attempt_due_to_event = false;

    tokio::select! {
      biased;
      _ = async {
        loop {
          match system_event_rx.recv().await {
            Ok(SystemEvent::ContextTerminating) => {
              tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC Connecter received ContextTerminating, flagging for abort.");
              should_abort_attempt_due_to_event = true; break;
            }
            Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
              tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, parent_id = self.parent_socket_id, "IPC Connecter received SocketClosing for parent, flagging for abort.");
              should_abort_attempt_due_to_event = true; break;
            }
            Ok(_) => { /* Ignore other events. */ }
            Err(_) => { // Lagged or Closed
              tracing::warn!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC Connecter: Error receiving from system event bus. Flagging for abort.");
              should_abort_attempt_due_to_event = true; break;
            }
          }
        }
      } => { /* Event monitoring loop completed or broke. */ }

      connect_result_fut = UnixStream::connect(&self.path) => {
        if should_abort_attempt_due_to_event {
          tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC connection attempt completed but abort was flagged by event. Ignoring result.");
          final_error_for_actor_stop = Some(ZmqError::Internal("IPC Connecter aborted by system event".into()));
        } else {
          match connect_result_fut {
            Ok(unix_stream) => {
              let peer_addr_str = format!("ipc-peer-fd-{}", unix_stream.as_raw_fd());
              tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, path = ?path_clone_log, peer = %peer_addr_str, "IPC Connect successful");

              if let Some(ref tx) = monitor_tx { /* ... emit Connected ... */ }

              let session_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let connection_specific_uri = format!("ipc://{}", peer_addr_str);

              let (session_cmd_mailbox, session_task_join_handle) = SessionBase::create_and_spawn(
                session_handle_id, connection_specific_uri.clone(), monitor_tx.clone(),
                self.context.clone(), self.parent_socket_id,
              );
              let (engine_mailbox, engine_task_join_handle_inner) = create_and_spawn_ipc_engine(
                engine_handle_id, session_cmd_mailbox.clone(), unix_stream, self.context_options.clone(),
                false, &self.context, session_handle_id,
              );
              let attach_cmd = Command::Attach {
                engine_mailbox, engine_handle: Some(engine_handle_id), engine_task_handle: Some(engine_task_join_handle_inner)
              };

              if session_cmd_mailbox.send(attach_cmd).await.is_err() {
                final_error_for_actor_stop = Some(ZmqError::Internal("Failed to attach engine to session (IPC)".into()));
                if !session_task_join_handle.is_finished() { session_task_join_handle.abort(); }
              } else {
                let event = SystemEvent::NewConnectionEstablished {
                  parent_core_id: self.parent_socket_id, endpoint_uri: connection_specific_uri.clone(),
                  target_endpoint_uri: endpoint_uri_clone.clone(), session_mailbox: session_cmd_mailbox,
                  session_handle_id, session_task_id: session_task_join_handle.id(),
                };
                if self.context.event_bus().publish(event).is_err() {
                  final_error_for_actor_stop = Some(ZmqError::Internal("Failed to publish NewConnectionEstablished (IPC)".into()));
                  if !session_task_join_handle.is_finished() { session_task_join_handle.abort(); }
                }
              }
            }
            Err(e) => {
              let current_error = ZmqError::from_io_endpoint(e, &endpoint_uri_clone);
              final_error_for_actor_stop = Some(current_error.clone());
              tracing::error!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, error = %current_error, "IPC Connect failed");
              if let Some(ref tx) = monitor_tx { /* ... emit ConnectFailed ... */ }
              let event = SystemEvent::ConnectionAttemptFailed {
                parent_core_id: self.parent_socket_id, target_endpoint_uri: endpoint_uri_clone.clone(),
                error_msg: current_error.to_string(), // Pass ZmqError
              };
              let _ = self.context.event_bus().publish(event);
            }
          }
        }
      } // end connect_result_fut arm
    } // end select!

    tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC Connecter actor finished connection attempt");
    self.context.publish_actor_stopping(
      connecter_handle,
      connecter_actor_type,
      Some(endpoint_uri_clone),
      final_error_for_actor_stop, // This is Option<ZmqError>
    );
    tracing::info!(connecter_handle = connecter_handle, uri = %self.endpoint_uri, "IPC Connecter actor fully stopped.");
  }
}
// --- End IpcConnecter ---

fn is_fatal_ipc_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

/// Helper to create and spawn a ZMTP Engine task for an IPC stream.
/// This function also publishes the `ActorStarted` event for the spawned engine.
pub(crate) fn create_and_spawn_ipc_engine(
  engine_handle_id: usize,
  session_cmd_mailbox: MailboxSender,
  unix_stream: UnixStream,
  socket_options: Arc<SocketOptions>,
  is_server_role: bool,
  context: &Context,
  session_handle_id: usize,
) -> (MailboxSender, JoinHandle<()>) {
  let engine_actor_type = ActorType::Engine;
  let (engine_command_mailbox, engine_task_join_handle) = engine::zmtp_ipc::create_and_spawn_ipc_engine(
    engine_handle_id,
    session_cmd_mailbox,
    unix_stream,
    socket_options,
    is_server_role,
    context,
    session_handle_id,
  );
  context.publish_actor_started(engine_handle_id, engine_actor_type, Some(session_handle_id));
  (engine_command_mailbox, engine_task_join_handle)
}
