// src/transport/ipc.rs

#![cfg(feature = "ipc")] // Only compile this file if ipc feature is enabled

use crate::context::Context;
// Use the specific helper for IPC engines, which uses ZmtpEngineCoreStd<UnixStream>
use crate::transport::ipc::create_and_spawn_ipc_engine_wrapper as create_and_spawn_ipc_engine;
use crate::error::ZmqError;
use crate::runtime::{
  self, mailbox, ActorDropGuard, ActorType, Command, SystemEvent, 
  MailboxSender as GenericMailboxSender, MailboxReceiver as GenericMailboxReceiver,
  system_events::ConnectionInteractionModel,
};
// ISocketConnection trait and its SessionConnection implementation
use crate::socket::connection_iface::{ISocketConnection, SessionConnection};
// EngineConnectionType for Command::Attach for standard path
use crate::runtime::command::EngineConnectionType as CommandEngineConnectionType; 

use crate::session::{self, SessionBase}; 
use crate::socket::events::{MonitorSender, SocketEvent}; 
use crate::socket::options::SocketOptions; 
use std::io;
use std::os::fd::AsRawFd; 
use std::path::PathBuf; 
use std::sync::Arc;
use std::time::Duration;
use tokio::fs as tokio_fs; // Tokio fs for async operations if needed, though Drop uses std::fs
use tokio::net::{UnixListener as TokioUnixListener, UnixStream}; 
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore}; 
use tokio::task::{JoinHandle, Id as TaskId}; 
use tokio::time::{sleep, timeout}; 

// --- IpcListener Actor ---
#[derive(Debug)]
pub(crate) struct IpcListener {
  handle: usize,                           
  endpoint: String,                        
  path: PathBuf,                           
  mailbox_receiver: GenericMailboxReceiver,       
  listener_handle: Option<JoinHandle<()>>, 
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

    // Attempt to remove an existing socket file before binding.
    match std::fs::remove_file(&path) {
      Ok(_) => tracing::debug!(listener_handle = handle, path = ?path, "Removed existing IPC socket file."),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}, // Good, no file to remove
      Err(e) => tracing::warn!(listener_handle = handle, path = ?path, error = %e, "Failed to remove existing IPC socket file."),
    }

    let listener = TokioUnixListener::bind(&path).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri))?;
    // For IPC, the bound endpoint_uri is the same as the user-provided one, as there's no port resolution.
    let resolved_uri = endpoint_uri.clone(); 
    tracing::info!(listener_handle = handle, path = ?path, uri = %resolved_uri, "IPC Listener bound successfully");
    
    let max_conns = options.max_connections.unwrap_or(std::usize::MAX);
    let conn_limiter = Arc::new(Semaphore::new(max_conns.max(1)));

    let accept_loop_parent_hdl = handle;
    let accept_loop_hdl_id = context.inner().next_handle();
    let accept_loop_act_type = ActorType::AcceptLoop;

    let accept_loop_task_jh = tokio::spawn(IpcListener::run_accept_loop(
        accept_loop_hdl_id, accept_loop_parent_hdl, resolved_uri.clone(), 
        Arc::new(listener), options.clone(), 
        context_handle_source.clone(), monitor_tx.clone(), context.clone(),
        parent_socket_id, conn_limiter.clone(),
    ));
    context.publish_actor_started(accept_loop_hdl_id, accept_loop_act_type, Some(accept_loop_parent_hdl));

    let listener_actor = IpcListener {
      handle, endpoint: resolved_uri.clone(), path, mailbox_receiver: rx,
      listener_handle: Some(accept_loop_task_jh), context: context.clone(), parent_socket_id,
    };

    let cmd_loop_jh = tokio::spawn(listener_actor.run_command_loop());
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));

    Ok((tx, cmd_loop_jh, resolved_uri))
  }

  async fn run_command_loop(mut self) { // `mut self` to take listener_handle
    let listener_cmd_loop_handle = self.handle;
    let listener_cmd_loop_actor_type = ActorType::Listener;
    let endpoint_uri_clone_log = self.endpoint.clone();
    // path_clone_log removed as it's not used for logging here, self.path is available for Drop
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();
    // Take listener_handle for aborting.
    let listener_abort_handle = self.listener_handle.take(); 

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
    if let Some(listener_join_handle) = listener_abort_handle { // Use the taken handle
      if let Err(e) = listener_join_handle.await {
        if !e.is_cancelled() {
          tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task panicked: {:?}", e);
          if final_error_for_actor_stopping.is_none() { final_error_for_actor_stopping = Some(ZmqError::Internal(format!("Listener accept loop panicked (IPC): {:?}", e))); }
        } else { tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task cancelled."); }
      } else { tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "IPC Listener accept loop task joined cleanly."); }
    }

    self.context.publish_actor_stopping(
      listener_cmd_loop_handle, listener_cmd_loop_actor_type,
      Some(self.endpoint.clone()), final_error_for_actor_stopping,
    );
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
    let accept_loop_actor_type = ActorType::AcceptLoop;
    let actor_drop_guard = ActorDropGuard::new(
      context.clone(), accept_loop_handle, accept_loop_actor_type, Some(endpoint_uri.clone()),
    );
    tracing::debug!(handle = accept_loop_handle, uri = %endpoint_uri, "IPC Accept loop started.");
    let mut loop_error_to_report: Option<ZmqError> = None;

    loop {
      let permit = match connection_limiter.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => { loop_error_to_report = Some(ZmqError::Internal("IPC Connection limiter closed".into())); break; }
      };

      match listener.accept().await {
        Ok((unix_stream, _ipc_peer_addr_os_specific)) => {
          let _permit_guard = permit; 
          // For IPC, peer_addr is often unnamed or less meaningful than for TCP.
          // We can create a synthetic peer address for uniqueness if needed.
          let peer_addr_str = format!("ipc-peer-fd-{}", unix_stream.as_raw_fd());
          tracing::info!("Accepted new IPC connection (peer: {}) for listener {}", peer_addr_str, endpoint_uri);
          
          if let Some(ref tx) = monitor_tx { 
            let _ = tx.try_send(SocketEvent::Accepted { endpoint: endpoint_uri.clone(), peer_addr: peer_addr_str.clone() });
          }
          
          // IPC always uses the standard SessionBase + ZmtpEngineCoreStd<UnixStream> path.
          // io_uring.session_enabled option does not apply to IPC.
          let connection_specific_uri = format!("ipc://{}", peer_addr_str); // Unique URI for this connection

          tokio::spawn({
            let context_clone = context.clone();
            let socket_options_clone = socket_options.clone();
            let monitor_tx_clone = monitor_tx.clone();
            let endpoint_uri_listener_clone = endpoint_uri.clone(); 
            let handle_source_clone = handle_source.clone();
            // Move unix_stream into the task
            // connection_specific_uri_for_task is now the cloned var
            let connection_specific_uri_for_task_clone = connection_specific_uri.clone();


            async move {
              let _permit_scoped_for_task = _permit_guard; 

              let session_hdl_id = handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_hdl_id = handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

              let pipe_hwm = socket_options_clone.rcvhwm.max(socket_options_clone.sndhwm).max(1);
              let (tx_core_to_session, rx_session_from_core) = async_channel::bounded(pipe_hwm);
              let (tx_session_to_core, _rx_core_from_session) = async_channel::bounded(pipe_hwm);

              let (session_cmd_mailbox, session_task_hdl) = SessionBase::create_and_spawn(
                  session_hdl_id, connection_specific_uri_for_task_clone.clone(), 
                  monitor_tx_clone.clone(), context_clone.clone(), parent_socket_core_id,
              );
              let managing_actor_task_id_val = Some(session_task_hdl.id());

              let pipe_read_id_sess = context_clone.inner().next_handle();
              let pipe_write_id_sess = context_clone.inner().next_handle();
              let attach_pipe_cmd = Command::AttachPipe {
                  rx_from_core: rx_session_from_core, tx_to_core: tx_session_to_core.clone(),
                  pipe_read_id: pipe_read_id_sess, pipe_write_id: pipe_write_id_sess,
              };
              
              let mut setup_successful = true;
              if session_cmd_mailbox.send(attach_pipe_cmd).await.is_err() {
                tracing::error!("AttachPipe to IPC Session {} failed.", session_hdl_id); 
                session_task_hdl.abort(); 
                setup_successful = false;
              } else {
                let (engine_mb, engine_task_hdl) = create_and_spawn_ipc_engine(
                    engine_hdl_id, session_cmd_mailbox.clone(), unix_stream, // unix_stream moved here
                    socket_options_clone.clone(), true, &context_clone, session_hdl_id,
                );
                let attach_engine_cmd = Command::Attach {
                    connection: CommandEngineConnectionType::Standard { engine_mailbox: engine_mb },
                    engine_handle: Some(engine_hdl_id), engine_task_handle: Some(engine_task_hdl),
                };
                if session_cmd_mailbox.send(attach_engine_cmd).await.is_err() {
                  tracing::error!("AttachEngine to IPC Session {} failed.", session_hdl_id); 
                  session_task_hdl.abort(); 
                  setup_successful = false;
                }
              }

              if setup_successful {
                let connection_iface = Arc::new(SessionConnection::new(session_cmd_mailbox.clone(), session_hdl_id, tx_core_to_session));
                let interaction_model = ConnectionInteractionModel::ViaSessionActor { session_actor_mailbox: session_cmd_mailbox };
                
                let event = SystemEvent::NewConnectionEstablished {
                    parent_core_id: parent_socket_core_id, 
                    endpoint_uri: connection_specific_uri_for_task_clone.clone(), 
                    target_endpoint_uri: endpoint_uri_listener_clone.clone(), 
                    connection_iface,
                    interaction_model,
                    managing_actor_task_id: managing_actor_task_id_val,
                };
                if context_clone.event_bus().publish(event).is_err() {
                  tracing::error!("Failed to publish NewConnectionEstablished for IPC {}", connection_specific_uri_for_task_clone);
                  session_task_hdl.abort(); // Abort session if event publish fails
                }
              } else {
                  tracing::warn!("IPC Connection setup failed for {}, NewConnectionEstablished not published.", connection_specific_uri_for_task_clone);
              }
            } 
          }); 
        } 
        Err(e) => { 
          drop(permit);
          tracing::error!("Error accepting IPC connection (listener {}): {}", endpoint_uri, e);
          if let Some(ref tx) = monitor_tx {
            let _ = tx.try_send(SocketEvent::AcceptFailed { endpoint: endpoint_uri.clone(), error_msg: e.to_string() });
          }
          if is_fatal_ipc_accept_error(&e) { // Use specific helper for IPC if needed
            loop_error_to_report = Some(ZmqError::from_io_endpoint(e, &endpoint_uri)); break;
          }
          sleep(Duration::from_millis(100)).await;
        }
      }
    } 
    actor_drop_guard.waive();
    context.publish_actor_stopping(accept_loop_handle, accept_loop_actor_type, Some(endpoint_uri), loop_error_to_report);
    tracing::info!("IPC Accept loop {} fully stopped.", accept_loop_handle);
  }
} 

impl Drop for IpcListener {
  fn drop(&mut self) {
    tracing::debug!(listener_handle = self.handle, path = ?self.path, "Dropping IpcListener, cleaning up IPC socket file");
    // listener_handle is already Option, take it to ensure it's handled once.
    if let Some(listener_join_handle) = self.listener_handle.take() {
      if !listener_join_handle.is_finished() {
        listener_join_handle.abort();
        tracing::debug!(listener_handle = self.handle, path = ?self.path, "Aborted accept loop task in IpcListener Drop.");
      }
    }
    // Synchronous remove_file in Drop is okay.
    match std::fs::remove_file(&self.path) {
      Ok(_) => tracing::debug!(listener_handle = self.handle, path = ?self.path, "Removed IPC socket file."),
      Err(e)if e.kind() == io::ErrorKind::NotFound => tracing::trace!(listener_handle = self.handle, path = ?self.path, "IPC socket file not found during drop."),
      Err(e) => tracing::warn!(listener_handle = self.handle, path = ?self.path, error = %e, "Failed to remove IPC socket file during drop."),
    }
  }
}


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
  ) -> JoinHandle<()> { // Returns JoinHandle directly as Connecter is fire-and-forget from SocketCore's mailbox perspective
    let actor_type = ActorType::Connecter;
    let connecter_actor = IpcConnecter {
      handle, endpoint_uri: endpoint_uri.clone(), path,
      context_options: options, context_handle_source,
      context: context.clone(), parent_socket_id,
    };
    let task_join_handle = tokio::spawn(connecter_actor.run_connect_attempt(monitor_tx));
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));
    task_join_handle
  }

  async fn run_connect_attempt(mut self, monitor_tx: Option<MonitorSender>) {
    let connecter_handle = self.handle;
    let actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint_uri.clone();
    // path_clone_log removed, self.path can be used if needed before move/drop
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, path = ?self.path, "IPC Connecter actor started.");

    let mut system_event_rx = self.context.event_bus().subscribe();
    let mut should_abort_attempt_due_to_event = false;

    // IPC connections typically don't have the same elaborate retry logic as TCP (reconnect_ivl).
    // They usually succeed or fail quickly. We attempt once.
    tokio::select! {
      biased;
      _ = async { // Event monitoring arm
        loop {
          match system_event_rx.recv().await {
            Ok(SystemEvent::ContextTerminating) | Ok(SystemEvent::SocketClosing { .. }) => {
              should_abort_attempt_due_to_event = true; break;
            }
            Ok(_) => {}
            Err(_) => { should_abort_attempt_due_to_event = true; break; } // Lagged or Closed
          }
        }
      } => { /* Event monitoring determined outcome (or completed if connect won first) */ }

      connect_result_fut = UnixStream::connect(&self.path) => {
        // This arm executes if connect_result_fut completes (Ok or Err)
        // before the event monitoring arm breaks due to a critical event.
      }
    }

    if should_abort_attempt_due_to_event {
      tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC connect attempt aborted by system event.");
      final_error_for_actor_stop = Some(ZmqError::Internal("IPC Connecter aborted by system event".into()));
    } else {
      // Need to re-evaluate connect_result_fut as select! doesn't return its value directly when another branch wins.
      // For simplicity, we'll re-issue the connect attempt here if not aborted,
      // or structure the select differently. A better way is to use a timeout on the connect itself.
      // Let's assume the select! above means if connect_result_fut didn't win, we treat it as if it hasn't happened yet for this path.
      // This is tricky. A cleaner way:
      let connect_with_timeout = timeout(Duration::from_secs(5), UnixStream::connect(&self.path)); // Example timeout

      tokio::select! {
        biased;
         _ = async { /* same event monitoring as above */ 
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
                tracing::info!(connecter_handle = connecter_handle, uri = %endpoint_uri_clone, "IPC connect attempt aborted by system event during timeout window.");
                final_error_for_actor_stop = Some(ZmqError::Internal("IPC Connecter aborted by system event (timeout window)".into()));
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
                        let connection_specific_uri = format!("ipc://{}", peer_addr_str);

                        let pipe_hwm = self.context_options.rcvhwm.max(self.context_options.sndhwm).max(1);
                        let (tx_core_to_session, rx_session_from_core) = async_channel::bounded(pipe_hwm);
                        let (tx_session_to_core, _rx_core_from_session) = async_channel::bounded(pipe_hwm);

                        let (session_cmd_mailbox, session_task_hdl) = SessionBase::create_and_spawn(
                            session_hdl_id, connection_specific_uri.clone(), monitor_tx.clone(),
                            self.context.clone(), self.parent_socket_id,
                        );
                        
                        let pipe_read_id_sess = self.context.inner().next_handle();
                        let pipe_write_id_sess = self.context.inner().next_handle();
                        let attach_pipe_cmd = Command::AttachPipe {
                            rx_from_core: rx_session_from_core, tx_to_core: tx_session_to_core.clone(),
                            pipe_read_id: pipe_read_id_sess, pipe_write_id: pipe_write_id_sess,
                        };
                        
                        let mut setup_ok = true;
                        if session_cmd_mailbox.send(attach_pipe_cmd).await.is_err() {
                            session_task_hdl.abort(); setup_ok = false;
                            final_error_for_actor_stop = Some(ZmqError::Internal("Failed AttachPipe to IPC Session".into()));
                        } else {
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
                        }

                        if setup_ok {
                            let connection_iface = Arc::new(SessionConnection::new(session_cmd_mailbox.clone(), session_hdl_id, tx_core_to_session));
                            let interaction_model = ConnectionInteractionModel::ViaSessionActor { session_actor_mailbox: session_cmd_mailbox };
                            let event = SystemEvent::NewConnectionEstablished {
                                parent_core_id: self.parent_socket_id, endpoint_uri: connection_specific_uri.clone(),
                                target_endpoint_uri: endpoint_uri_clone.clone(), 
                                connection_iface, interaction_model,
                                managing_actor_task_id: Some(session_task_hdl.id()),
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
    self.context.publish_actor_stopping(
      connecter_handle, actor_type,
      Some(endpoint_uri_clone), final_error_for_actor_stop,
    );
    tracing::info!(connecter_handle = connecter_handle, uri = %self.endpoint_uri, "IPC Connecter actor fully stopped.");
  }
}
}

fn is_fatal_ipc_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

/// Wrapper to call engine::zmtp_ipc::create_and_spawn_ipc_engine and publish ActorStarted.
/// Renamed to avoid conflict if engine::zmtp_ipc is brought into scope directly.
pub(crate) fn create_and_spawn_ipc_engine_wrapper(
  engine_handle_id: usize,
  session_cmd_mailbox: GenericMailboxSender, // This is MailboxSender<Command>
  unix_stream: UnixStream,
  socket_options: Arc<SocketOptions>,
  is_server_role: bool,
  context: &Context,
  session_handle_id: usize, // Parent session's handle
) -> (GenericMailboxSender, JoinHandle<()>) {
  let engine_actor_type = ActorType::Engine;
  // The zmtp_ipc module should contain the actual engine spawning logic.
  // We assume it returns the engine's command mailbox and its join handle.
  let (engine_command_mailbox, engine_task_join_handle) = 
    crate::engine::zmtp_ipc::create_and_spawn_ipc_engine(
      engine_handle_id, session_cmd_mailbox, unix_stream,
      socket_options, is_server_role, context, session_handle_id,
  );
  context.publish_actor_started(engine_handle_id, engine_actor_type, Some(session_handle_id));
  (engine_command_mailbox, engine_task_join_handle)
}