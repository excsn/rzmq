// src/transport/tcp.rs

use crate::context::Context;
use crate::engine::zmtp_tcp::create_and_spawn_tcp_engine as create_engine_task_internal;
use crate::error::ZmqError;
use crate::runtime::{
  self, mailbox, ActorDropGuard, ActorType, Command, EventBus, MailboxReceiver, MailboxSender, SystemEvent
};
use crate::session::{self, SessionBase};
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{SocketOptions, TcpTransportConfig};
use socket2::{SockRef, TcpKeepalive}; // For setting TCP options like NODELAY, KEEPALIVE
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::broadcast; // For receiving system events
use tokio::task::JoinHandle;
use tokio::time::sleep;

// --- TcpListener Actor ---
/// Manages a listening TCP socket, accepts incoming connections, and spawns Session/Engine pairs.
#[derive(Debug)]
pub(crate) struct TcpListener {
  handle: usize,                     // Handle of the Listener's command loop actor.
  endpoint: String,                  // The URI this listener is bound to.
  mailbox_receiver: MailboxReceiver, // For receiving Stop commands.
  listener_handle: JoinHandle<()>,   // JoinHandle for the accept loop task.
  context: Context,                  // The rzmq Context.
  parent_socket_id: usize,           // Handle ID of the parent SocketCore.
}

impl TcpListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let actor_type = ActorType::Listener;
    let (tx, rx) = mailbox(); // Mailbox for this Listener's command loop.

    // Bind the OS TCP listener.
    let bind_addr_str = endpoint
      .strip_prefix("tcp://")
      .ok_or_else(|| ZmqError::InvalidEndpoint(endpoint.clone()))?;
    let std_listener =
      std::net::TcpListener::bind(bind_addr_str).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
    std_listener.set_nonblocking(true).map_err(ZmqError::from)?;
    SockRef::from(&std_listener)
      .set_reuse_address(true)
      .map_err(ZmqError::from)?;

    let tokio_listener = TokioTcpListener::from_std(std_listener).map_err(ZmqError::from)?;
    let local_addr = tokio_listener.local_addr().map_err(ZmqError::from)?;
    tracing::info!(listener_handle = handle, ?local_addr, uri = %endpoint, "TCP Listener bound successfully");
    let listener_arc = Arc::new(tokio_listener);

    // Prepare TCP configuration for accepted streams.
    let transport_config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    // Spawn the accept loop task.
    let accept_listener_arc = listener_arc.clone();
    let accept_handle_source_clone = context_handle_source.clone();
    let accept_options_clone = options.clone();
    let accept_config_clone = transport_config.clone();
    let accept_monitor_tx_clone = monitor_tx.clone();
    let endpoint_for_accept_loop = endpoint.clone();
    let context_for_accept_loop = context.clone();
    let parent_socket_id_for_accept_loop = parent_socket_id;
    let accept_loop_parent_handle = handle; // Accept loop's parent for eventing is this Listener.

    let accept_loop_handle_id = context_for_accept_loop.inner().next_handle();
    let accept_loop_actor_type = ActorType::AcceptLoop;

    let accept_loop_task_join_handle = tokio::spawn(TcpListener::run_accept_loop(
      accept_loop_handle_id,
      accept_loop_parent_handle,
      endpoint_for_accept_loop,
      accept_listener_arc,
      accept_config_clone,
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

    let listener_actor = TcpListener {
      handle,
      endpoint,
      mailbox_receiver: rx,
      listener_handle: accept_loop_task_join_handle,
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
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop started");

    let mut final_error_for_actor_stopping: Option<ZmqError> = None;

    let _loop_result: Result<(), ()> = async {
      loop {
        tokio::select! {
          biased;
          event_result = system_event_rx.recv() => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener received ContextTerminating, stopping accept loop.");
                self.listener_handle.abort();
                break;
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, parent_id = self.parent_socket_id, "TCP Listener received SocketClosing for parent, stopping accept loop.");
                self.listener_handle.abort();
                break;
              }
              Ok(_) => { /* Ignore other events. */ }
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, skipped = n, "System event bus lagged for TCP Listener command loop!");
                self.listener_handle.abort();
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus lagged".into()));
                break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "System event bus closed unexpectedly for TCP Listener command loop!");
                self.listener_handle.abort();
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus closed".into()));
                break;
              }
            }
          }
          cmd_result = self.mailbox_receiver.recv() => {
            match cmd_result {
              Ok(Command::Stop) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener received Stop command");
                self.listener_handle.abort();
                break;
              }
              Ok(other_cmd) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener received unhandled command: {:?}", other_cmd.variant_name());
              }
              Err(_) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command mailbox closed, stopping accept loop.");
                self.listener_handle.abort();
                if final_error_for_actor_stopping.is_none() {
                    final_error_for_actor_stopping = Some(ZmqError::Internal("Listener command mailbox closed by peer".into()));
                }
                break;
              }
            }
          }
        }
      }
      Ok(())
    }.await;

    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop finished, awaiting accept loop task.");
    if let Err(e) = self.listener_handle.await {
      if !e.is_cancelled() {
        tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener accept loop task panicked: {:?}", e);
        if final_error_for_actor_stopping.is_none() {
          final_error_for_actor_stopping = Some(ZmqError::Internal(format!("Listener accept loop panicked: {:?}", e)));
        }
      } else {
        tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener accept loop task was cancelled as expected.");
      }
    } else {
      tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener accept loop task joined cleanly.");
    }

    self.context.publish_actor_stopping(
      listener_cmd_loop_handle,
      listener_cmd_loop_actor_type,
      Some(self.endpoint),
      final_error_for_actor_stopping, // This is Option<ZmqError>
    );
    tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop actor fully stopped.");
  }

  async fn run_accept_loop(
    accept_loop_handle: usize,
    listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    listener: Arc<TokioTcpListener>,
    transport_config: TcpTransportConfig,
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
    
    tracing::debug!(handle = accept_loop_handle, parent_handle = listener_cmd_loop_handle, uri = %endpoint_uri, "TCP Listener accept loop started");
    let mut loop_error_to_report: Option<ZmqError> = None; // Store ZmqError

    loop {
      tokio::select! {
        biased;
        accept_result = listener.accept() => {
          match accept_result {
            Ok((tcp_stream, peer_addr)) => {
              let peer_addr_str = peer_addr.to_string();
              tracing::info!(listener_accept_loop_handle = accept_loop_handle, peer = %peer_addr_str, "Accepted new TCP connection");

              if let Some(ref tx) = monitor_tx {
                let event = SocketEvent::Accepted { endpoint: endpoint_uri.clone(), peer_addr: peer_addr_str.clone() };
                let tx_clone = tx.clone();
                tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
              }

              if let Err(e) = apply_tcp_socket_options(&tcp_stream, &transport_config) {
                tracing::error!(accept_loop_handle = accept_loop_handle, peer = %peer_addr_str, error = %e, "Failed to apply TCP options. Dropping.");
                if let Some(ref tx) = monitor_tx {
                  let event = SocketEvent::AcceptFailed { endpoint: endpoint_uri.clone(), error_msg: format!("Apply options failed: {}", e) };
                  let tx_clone = tx.clone();
                  tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
                }
                drop(tcp_stream);
                continue;
              }

              let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let connection_specific_uri = format!("tcp://{}", peer_addr_str);

              let (session_cmd_mailbox, session_task_join_handle) = SessionBase::create_and_spawn(
                session_handle_id,
                connection_specific_uri.clone(),
                monitor_tx.clone(),
                context.clone(),
                parent_socket_core_id,
              );

              let (engine_mailbox, engine_task_join_handle_inner) = create_and_spawn_tcp_engine(
                engine_handle_id,
                session_cmd_mailbox.clone(),
                tcp_stream,
                socket_options.clone(),
                true, // Server-side engine
                &context,
                session_handle_id, // Engine's parent is Session
              );

              let attach_cmd = Command::Attach {
                engine_mailbox,
                engine_handle: Some(engine_handle_id),
                engine_task_handle: Some(engine_task_join_handle_inner),
              };
              if session_cmd_mailbox.send(attach_cmd).await.is_err() {
                tracing::error!(session_handle = session_handle_id, uri = %connection_specific_uri, "Failed to send Attach to Session.");
                // If attach fails, Session will likely stop, Engine might too.
                // Accept loop continues to accept other connections.
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
                tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, "Failed to publish NewConnectionEstablished event.");
                loop_error_to_report = Some(ZmqError::Internal("Event bus publish failed for NewConnectionEstablished".into()));
                break;
              }
            }
            Err(e) => {
              tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, error = %e, "Error accepting TCP connection");
              if let Some(ref tx) = monitor_tx {
                let event = SocketEvent::AcceptFailed { endpoint: endpoint_uri.clone(), error_msg: format!("{}", e) };
                let tx_clone = tx.clone();
                tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
              }
              if is_fatal_accept_error(&e) {
                tracing::error!(accept_loop_handle = accept_loop_handle, uri = %endpoint_uri, error = %e, "Fatal error in TCP accept loop, stopping.");
                loop_error_to_report = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
                break;
              }
              sleep(Duration::from_millis(100)).await;
            }
          }
        }
        _ = tokio::time::sleep(Duration::from_secs(60 * 5)) => {
          tracing::warn!(handle = accept_loop_handle, uri = %endpoint_uri, "TCP Listener accept loop timed out (safety break).");
          loop_error_to_report = Some(ZmqError::Internal("TCP Accept loop safety timeout".into()));
          break;
        }
      }
    }
    tracing::debug!(handle = accept_loop_handle, parent_handle = listener_cmd_loop_handle, uri = %endpoint_uri, "TCP Listener accept loop finished");
    actor_drop_guard.waive();
    context.publish_actor_stopping(
      accept_loop_handle,
      accept_loop_actor_type,
      Some(endpoint_uri),
      loop_error_to_report,
    );
  }
}

// --- TcpConnecter Actor ---
#[derive(Debug)]
pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String,
  config: TcpTransportConfig,
  context_options: Arc<SocketOptions>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  context: Context,
  parent_socket_id: usize,
}

impl TcpConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> JoinHandle<()> {
    let actor_type = ActorType::Connecter;
    let transport_config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };
    let connecter_actor = TcpConnecter {
      handle,
      endpoint: endpoint.clone(),
      config: transport_config,
      context_options: options,
      context_handle_source,
      context: context.clone(),
      parent_socket_id,
    };

    let task_join_handle = tokio::spawn(connecter_actor.run_connect_loop(monitor_tx));
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));
    task_join_handle
  }

  async fn run_connect_loop(mut self, monitor_tx: Option<MonitorSender>) {
    let connecter_handle = self.handle;
    let connecter_actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint.clone();

    let target_addr_str = match endpoint_uri_clone.strip_prefix("tcp://") {
      Some(addr) => addr.to_string(),
      None => {
        let err = ZmqError::InvalidEndpoint(endpoint_uri_clone.clone());
        tracing::error!(handle = connecter_handle, uri = %endpoint_uri_clone, "{}", err);
        if let Some(ref tx) = monitor_tx {
          let event = SocketEvent::ConnectFailed {
            endpoint: endpoint_uri_clone.clone(),
            error_msg: format!("{}", err),
          };
          let tx_clone = tx.clone();
          tokio::spawn(async move {
            if tx_clone.send(event).await.is_err() { /* Warn */ }
          });
        }
        // Publish ConnectionAttemptFailed event
        let fail_event = SystemEvent::ConnectionAttemptFailed {
          parent_core_id: self.parent_socket_id,
          target_endpoint_uri: endpoint_uri_clone.clone(),
          error_msg: err.to_string(),
        };
        let _ = self.context.event_bus().publish(fail_event);
        self.context.publish_actor_stopping(
          connecter_handle,
          connecter_actor_type,
          Some(endpoint_uri_clone),
          Some(err),
        );
        return;
      }
    };

    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "TCP Connecter actor started connection attempts.");
    let mut current_delay = self.context_options.reconnect_ivl.unwrap_or(Duration::ZERO);
    let max_delay = self.context_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    let mut system_event_rx = self.context.event_bus().subscribe();

    loop {
      tokio::select! {
        biased;
        event_result = system_event_rx.recv() => {
          match event_result {
            Ok(SystemEvent::ContextTerminating) => {
              tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "TCP Connecter received ContextTerminating, stopping attempts.");
              final_error_for_actor_stop = Some(ZmqError::Internal("Connecter shutdown by ContextTerminating".into()));
              break; // Exit the connection loop
            }
            Ok(SystemEvent::SocketClosing{ socket_id }) => {
              if socket_id == self.parent_socket_id {
                tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, parent_id = self.parent_socket_id, "TCP Connecter received SocketClosing for parent, stopping attempts.");
                final_error_for_actor_stop = Some(ZmqError::Internal("Connecter shutdown by parent SocketClosing".into()));
                break; // Exit the connection loop
              }
              // If SocketClosing is for a different socket_id, ignore it.
            }
            Err(_) => { // Lagged or Closed
              final_error_for_actor_stop = Some(ZmqError::Internal("Connecter event bus error".into()));
              break;
            }
            Ok(_) => { /* Ignore other events. */ }
          }
        }
        should_break_outer_loop = async {
          if attempt_count > 0 {
            if current_delay.is_zero() && self.context_options.reconnect_ivl != Some(Duration::ZERO) {
                final_error_for_actor_stop = Some(ZmqError::Internal("Reconnect disabled after first attempt".into()));
                let event = SystemEvent::ConnectionAttemptFailed {
                    parent_core_id: self.parent_socket_id,
                    target_endpoint_uri: endpoint_uri_clone.clone(),
                    error_msg: final_error_for_actor_stop.clone().unwrap_or_else(|| ZmqError::Internal("Unknown reconnect disable error".into())).to_string(),
                };
                let _ = self.context.event_bus().publish(event);
                return true;
            }
             if current_delay.is_zero() { // Initial reconnect_ivl was zero.
                // Error will be set by the failed connect attempt below.
                // This return true ensures we don't sleep and just try once.
                return true; // Will attempt once and then break if it fails and reconnect_ivl is zero.
            }
            tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, delay = ?current_delay, "Waiting before reconnect attempt #{}", attempt_count + 1);
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::ConnectRetried { endpoint: endpoint_uri_clone.clone(), interval: current_delay };
              let tx_clone = tx.clone();
              tokio::spawn(async move { if tx_clone.send(event).await.is_err() { /* Warn */ } });
            }
            sleep(current_delay).await;
          }
          attempt_count += 1;

          tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, "Attempting TCP connect #{}", attempt_count);
          let connect_result = TcpStream::connect(&target_addr_str).await;

          match connect_result {
            Ok(tcp_stream) => {
              let peer_addr_str = tcp_stream.peer_addr().map(|a|a.to_string()).unwrap_or_else(|_| "unknown_peer".to_string());
              tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, peer = %peer_addr_str, "TCP Connect successful (attempt #{})", attempt_count);
              if let Some(ref tx) = monitor_tx { /* ... emit Connected ... */ }

              if let Err(e) = apply_tcp_socket_options(&tcp_stream, &self.config) {
                tracing::error!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %e, "Failed to apply options, will retry.");
                drop(tcp_stream);
                if let Some(max_ivl_val) = max_delay { if !max_ivl_val.is_zero() { current_delay = (current_delay * 2).min(max_ivl_val); } }
                else if self.context_options.reconnect_ivl == Some(Duration::ZERO) {
                    final_error_for_actor_stop = Some(ZmqError::Internal(format!("Failed to apply options and reconnect disabled: {}", e)));
                    let event = SystemEvent::ConnectionAttemptFailed {
                        parent_core_id: self.parent_socket_id, target_endpoint_uri: endpoint_uri_clone.clone(),
                        error_msg: final_error_for_actor_stop.clone().unwrap_or_else(|| ZmqError::Internal("Unknown option apply error".into())).to_string(),
                    };
                    let _ = self.context.event_bus().publish(event);
                    return true;
                }
                return false; // Retry
              }

              let session_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let engine_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
              let connection_specific_uri = format!("tcp://{}", peer_addr_str);

              let (session_cmd_mailbox, session_task_join_handle) = SessionBase::create_and_spawn(
                session_handle_id, connection_specific_uri.clone(), monitor_tx.clone(),
                self.context.clone(), self.parent_socket_id,
              );
              let (engine_mailbox, engine_task_join_handle_inner) = create_and_spawn_tcp_engine(
                engine_handle_id, session_cmd_mailbox.clone(), tcp_stream, self.context_options.clone(),
                false, &self.context, session_handle_id,
              );
              let attach_cmd = Command::Attach { engine_mailbox, engine_handle: Some(engine_handle_id), engine_task_handle: Some(engine_task_join_handle_inner) };

              if session_cmd_mailbox.send(attach_cmd).await.is_err() {
                final_error_for_actor_stop = Some(ZmqError::Internal("Failed to attach engine to session".into()));
                if !session_task_join_handle.is_finished() { session_task_join_handle.abort(); }
                return true; // Fatal
              } else {
                let event = SystemEvent::NewConnectionEstablished {
                  parent_core_id: self.parent_socket_id, endpoint_uri: connection_specific_uri.clone(),
                  target_endpoint_uri: endpoint_uri_clone.clone(), session_mailbox: session_cmd_mailbox,
                  session_handle_id, session_task_id: session_task_join_handle.id(),
                };
                if self.context.event_bus().publish(event).is_err() {
                  final_error_for_actor_stop = Some(ZmqError::Internal("Failed to publish NewConnectionEstablished".into()));
                  if !session_task_join_handle.is_finished() { session_task_join_handle.abort(); }
                }
                return true; // Success or event publish failure, break outer loop
              }
            }
            Err(e) => { // TCP connect failed
              let current_error = ZmqError::from_io_endpoint(e, &endpoint_uri_clone); // Store ZmqError
              tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %current_error, "TCP Connect attempt #{} failed", attempt_count);
              if attempt_count == 1 && self.context_options.reconnect_ivl != Some(Duration::ZERO) && !is_fatal_connect_error(&current_error) {
                if let Some(ref tx) = monitor_tx { /* emit ConnectDelayed */ }
              }
              if let Some(max_ivl_val) = max_delay { if !max_ivl_val.is_zero() { current_delay = (current_delay * 2).min(max_ivl_val); } }
              else if current_delay > Duration::ZERO { current_delay *= 2; }

              if self.context_options.reconnect_ivl == Some(Duration::ZERO) || is_fatal_connect_error(&current_error) {
                final_error_for_actor_stop = Some(current_error.clone());
                let event = SystemEvent::ConnectionAttemptFailed {
                    parent_core_id: self.parent_socket_id, target_endpoint_uri: endpoint_uri_clone.clone(),
                    error_msg: current_error.to_string(), // Pass ZmqError
                };
                let _ = self.context.event_bus().publish(event);
                return true; // Break outer loop
              }
              return false; // Continue retrying
            }
          }
        } => { if should_break_outer_loop { break; } }
      } // end select!
    } // end loop

    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "TCP Connecter actor finished connection loop.");
    self.context.publish_actor_stopping(
      connecter_handle,
      connecter_actor_type,
      Some(endpoint_uri_clone),
      final_error_for_actor_stop, // This is Option<ZmqError>
    );
    tracing::info!(handle = connecter_handle, uri = %self.endpoint, "TCP Connecter actor fully stopped.");
  }
}

fn apply_tcp_socket_options(stream: &TcpStream, config: &TcpTransportConfig) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  tracing::trace!(nodelay = config.tcp_nodelay, "Applied TCP_NODELAY");

  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    #[cfg(any(unix, target_os = "windows"))]
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval);
    }
    #[cfg(not(any(unix, target_os = "windows")))]
    if config.keepalive_interval.is_some() {
      tracing::warn!("TCP Keepalive Interval not supported on this platform.");
    }
    #[cfg(unix)]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count);
    }
    #[cfg(not(unix))]
    if config.keepalive_count.is_some() {
      tracing::warn!("TCP Keepalive Count not supported on this platform.");
    }
    socket_ref.set_tcp_keepalive(&keepalive)?;
    tracing::debug!("Applied TCP Keepalive settings: {:?}", keepalive);
  } else {
    tracing::trace!("TCP Keepalive settings not configured, using system defaults.");
  }
  Ok(())
}

fn is_fatal_accept_error(e: &io::Error) -> bool {
  // Renamed from is_fatal
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

fn is_fatal_connect_error(e: &ZmqError) -> bool {
  // Takes ZmqError
  match e {
    ZmqError::IoError { kind, .. } => {
      matches!(
        kind,
        io::ErrorKind::AddrNotAvailable |
                io::ErrorKind::AddrInUse | // Though less common for connect
                io::ErrorKind::InvalidInput |
                io::ErrorKind::PermissionDenied
      )
    }
    // Other ZmqError variants that might be considered fatal for connect attempts
    ZmqError::InvalidEndpoint(_) => true,
    ZmqError::UnsupportedTransport(_) => true,
    _ => false, // Default to not fatal for other ZmqError types
  }
}

pub(crate) fn create_and_spawn_tcp_engine(
  engine_handle_id: usize,
  session_cmd_mailbox: MailboxSender,
  tcp_stream: TcpStream,
  socket_options: Arc<SocketOptions>,
  is_server_role: bool,
  context: &Context,
  session_handle_id: usize,
) -> (MailboxSender, JoinHandle<()>) {
  let engine_actor_type = ActorType::Engine;
  let (engine_command_mailbox, engine_task_join_handle) = create_engine_task_internal(
    engine_handle_id,
    session_cmd_mailbox,
    tcp_stream,
    socket_options,
    is_server_role,
    context,
    session_handle_id,
  );
  context.publish_actor_started(engine_handle_id, engine_actor_type, Some(session_handle_id));
  (engine_command_mailbox, engine_task_join_handle)
}
