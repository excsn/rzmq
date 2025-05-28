// src/transport/tcp.rs

use crate::context::Context;
use crate::engine::zmtp_tcp::create_and_spawn_tcp_engine; // For ZmtpEngineCoreStd
use crate::error::ZmqError;
use crate::runtime::{
  self, mailbox, ActorDropGuard, ActorType, Command, EngineConnectionType, EventBus, MailboxReceiver, MailboxSender,
  SystemEvent,
};
#[cfg(feature = "io-uring")]
use crate::runtime::{uring_runtime, AppToUringEngineCmd, UringLaunchInformation};
use crate::session::{self, SessionBase};
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{SocketOptions, TcpTransportConfig, ZmtpEngineConfig}; // Added ZmtpEngineConfig

use std::io;
use std::net::SocketAddr as StdSocketAddr;
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::sleep;

#[cfg(feature = "io-uring")]
use std::os::unix::io::{IntoRawFd, OwnedFd};

// Standard Tokio net types are used when 'io-uring' feature is NOT enabled,
// or when 'io-uring' is enabled but a specific session is chosen NOT to use uring.
mod underlying_std_net {
  pub use tokio::net::TcpListener;
  pub use tokio::net::TcpStream;
}

// Tokio-uring net types are used when 'io-uring' feature IS enabled AND
// a specific session is chosen TO use uring.
#[cfg(feature = "io-uring")]
mod underlying_uring_net {
  pub use tokio_uring::net::TcpListener as UringTcpListener;
  pub use tokio_uring::net::TcpStream as UringTcpStream;
}

// --- TcpListener Actor ---
#[derive(Debug)]
pub(crate) struct TcpListener {
  handle: usize,
  endpoint: String,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // JoinHandle for the accept loop task.
  context: Context,
  parent_socket_id: usize,
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
  ) -> Result<(MailboxSender, JoinHandle<()>, String), ZmqError> {
    let actor_type = ActorType::Listener;
    let capacity = context.inner().get_actor_mailbox_capacity();
    let (tx, rx) = mailbox(capacity);

    let bind_addr_str = endpoint
      .strip_prefix("tcp://")
      .ok_or_else(|| ZmqError::InvalidEndpoint(endpoint.clone()))?;

    // Determine if this listener should use io_uring for its accept socket based on options.
    // This is a high-level switch for the listener itself. Individual connections might differ.
    let listener_uses_io_uring = options.io_uring.session_enabled && cfg!(feature = "io-uring");
    tracing::debug!(listener_handle=handle, %endpoint, listener_uses_io_uring, "TCP Listener type decision.");

    let (std_listener_socket, actual_bind_addr) = {
      // Handle wildcard port by attempting to bind to a specific interface first if 0.0.0.0:0 or [::]:0
      // This allows socket2 to pick a port, which we can then retrieve.
      let addr_for_socket2_parse = if bind_addr_str == "0.0.0.0:0" || bind_addr_str.starts_with("[::]:0") {
        if bind_addr_str.starts_with("[::]") {
          "::1:0"
        } else {
          "127.0.0.1:0"
        }
      } else {
        bind_addr_str
      };
      let parsed_socket_addr: StdSocketAddr = addr_for_socket2_parse.parse().map_err(|e| {
        ZmqError::InvalidEndpoint(format!(
          "Failed to parse bind address '{}': {}",
          addr_for_socket2_parse, e
        ))
      })?;

      let domain = if parsed_socket_addr.is_ipv4() {
        socket2::Domain::IPV4
      } else {
        socket2::Domain::IPV6
      };
      let s = socket2::Socket::new(domain, socket2::Type::STREAM, None).map_err(ZmqError::from)?;
      s.set_reuse_address(true).map_err(ZmqError::from)?;

      // Use the original bind_addr_str if it contains wildcards for IP, but with port 0 resolved by OS
      let addr_for_bind_call = bind_addr_str.parse::<StdSocketAddr>().map_err(|e| {
        ZmqError::InvalidEndpoint(format!(
          "Failed to parse bind address for actual bind call'{}': {}",
          bind_addr_str, e
        ))
      })?;

      s.bind(&addr_for_bind_call.into())
        .map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
      s.listen(128).map_err(ZmqError::from)?;
      let final_bound_addr = s.local_addr().map_err(ZmqError::from)?.as_socket().unwrap(); // Must be Some for TCP
      (s, final_bound_addr)
    };

    let std_listener: std::net::TcpListener = std_listener_socket.into();
    std_listener.set_nonblocking(true).map_err(ZmqError::from)?;

    let resolved_uri = format!("tcp://{}", actual_bind_addr);
    tracing::info!(listener_handle = handle, local_addr = %resolved_uri, user_provided_uri = %endpoint, "TCP Listener bound successfully");

    let max_conns_for_listener = options.max_connections.unwrap_or(std::usize::MAX);
    let connection_limiter = Arc::new(Semaphore::new(max_conns_for_listener.max(1)));

    let transport_config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    let accept_handle_source_clone = context_handle_source.clone();
    let accept_options_clone = options.clone();
    let accept_config_clone = transport_config.clone();
    let accept_monitor_tx_clone = monitor_tx.clone();
    let endpoint_for_accept_loop = resolved_uri.clone();
    let context_for_accept_loop = context.clone();
    let parent_socket_id_for_accept_loop = parent_socket_id;
    let accept_loop_parent_handle = handle;
    let accept_loop_handle_id = context_for_accept_loop.inner().next_handle();
    let accept_loop_actor_type = ActorType::AcceptLoop;

    let accept_loop_task_join_handle = if listener_uses_io_uring {
      #[cfg(feature = "io-uring")]
      {
        let uring_listener =
          underlying_uring_net::UringTcpListener::from_std(std_listener).map_err(|e| ZmqError::IoError {
            kind: e.kind(),
            message: e.to_string(),
          })?;
        tokio::spawn(TcpListener::run_accept_loop_uring(
          accept_loop_handle_id,
          accept_loop_parent_handle,
          endpoint_for_accept_loop,
          Arc::new(uring_listener),
          accept_config_clone,
          accept_options_clone,
          accept_handle_source_clone,
          accept_monitor_tx_clone,
          context_for_accept_loop.clone(),
          parent_socket_id_for_accept_loop,
          connection_limiter.clone(),
        ))
      }
      #[cfg(not(feature = "io-uring"))]
      {
        unreachable!("listener_uses_io_uring is true but 'io-uring' feature not compiled");
      }
    } else {
      let std_tokio_listener = underlying_std_net::TcpListener::from_std(std_listener).map_err(ZmqError::from)?;
      tokio::spawn(TcpListener::run_accept_loop_std(
        accept_loop_handle_id,
        accept_loop_parent_handle,
        endpoint_for_accept_loop,
        Arc::new(std_tokio_listener),
        accept_config_clone,
        accept_options_clone,
        accept_handle_source_clone,
        accept_monitor_tx_clone,
        context_for_accept_loop.clone(),
        parent_socket_id_for_accept_loop,
        connection_limiter.clone(),
      ))
    };
    context.publish_actor_started(
      accept_loop_handle_id,
      accept_loop_actor_type,
      Some(accept_loop_parent_handle),
    );

    let listener_actor = TcpListener {
      handle,
      endpoint: resolved_uri.clone(),
      mailbox_receiver: rx,
      listener_handle: accept_loop_task_join_handle,
      context: context.clone(),
      parent_socket_id,
    };

    let command_loop_join_handle = tokio::spawn(listener_actor.run_command_loop());
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));

    Ok((tx, command_loop_join_handle, resolved_uri))
  }

  async fn run_command_loop(self) {
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
                self.listener_handle.abort(); break;
              }
              Ok(SystemEvent::SocketClosing{ socket_id }) if socket_id == self.parent_socket_id => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, parent_id = self.parent_socket_id, "TCP Listener received SocketClosing for parent, stopping accept loop.");
                self.listener_handle.abort(); break;
              }
              Ok(_) => {}
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, skipped = n, "System event bus lagged!");
                self.listener_handle.abort();
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus lagged".into())); break;
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "System event bus closed unexpectedly!");
                self.listener_handle.abort();
                final_error_for_actor_stopping = Some(ZmqError::Internal("Listener event bus closed".into())); break;
              }
            }
          }
          cmd_result = self.mailbox_receiver.recv() => {
            match cmd_result {
              Ok(Command::Stop) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener received Stop command");
                self.listener_handle.abort(); break;
              }
              Ok(other_cmd) => tracing::warn!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener received unhandled command: {:?}", other_cmd.variant_name()),
              Err(_) => {
                tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command mailbox closed, stopping accept loop.");
                self.listener_handle.abort();
                if final_error_for_actor_stopping.is_none() { final_error_for_actor_stopping = Some(ZmqError::Internal("Listener command mailbox closed by peer".into()));}
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
      final_error_for_actor_stopping,
    );
    tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop actor fully stopped.");
  }

  // Generic accept loop function, parameterized by listener and stream types
  async fn run_accept_loop_internal<L, S>(
    accept_loop_handle: usize,
    _listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    listener: Arc<L>,
    transport_config: TcpTransportConfig,
    socket_options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
    connection_limiter: Arc<Semaphore>,
    is_uring_listener: bool,
  ) where
    L: AcceptStream<Stream = S> + Send + Sync + 'static,
    S: AcceptedStream + Send + Sync + 'static,
  {
    let accept_loop_actor_type = ActorType::AcceptLoop;
    let actor_drop_guard = ActorDropGuard::new(
      context.clone(),
      accept_loop_handle,
      accept_loop_actor_type,
      Some(endpoint_uri.clone()),
    );
    tracing::debug!(handle = accept_loop_handle, uri = %endpoint_uri, is_uring_listener, "TCP Accept loop (generic) started.");
    let mut loop_error_to_report: Option<ZmqError> = None;

    loop {
      let permit = match connection_limiter.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => {
          loop_error_to_report = Some(ZmqError::Internal("Connection limiter closed".into()));
          break;
        }
      };

      let accept_result = listener.accept_stream().await;

      match accept_result {
        Ok((accepted_stream, peer_addr)) => {
          let peer_addr_str = peer_addr.to_string();
          tracing::info!(
            "Accepted new TCP connection from {} (for listener {})",
            peer_addr_str,
            endpoint_uri
          );
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::Accepted {
              endpoint: endpoint_uri.clone(),
              peer_addr: peer_addr_str.clone(),
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              let _ = tx_clone.send(event).await;
            });
          }

          let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let connection_specific_uri = format!("tcp://{}", peer_addr_str);
          let conn_context = context.clone();
          let conn_socket_options = socket_options.clone();
          let conn_monitor_tx = monitor_tx.clone();
          let listener_uri_for_event = endpoint_uri.clone();

          tokio::spawn({
            let transport_config = transport_config.clone();
            async move {
              let _permit_guard = permit; // Permit is owned by this task

              let (session_cmd_tx, _session_cmd_rx) = SessionBase::create_and_spawn(
                session_handle_id,
                connection_specific_uri.clone(),
                conn_monitor_tx.clone(),
                conn_context.clone(),
                parent_socket_core_id,
              );

              let engine_connection_type: EngineConnectionType;
              let mut engine_task_handle_for_attach: Option<JoinHandle<()>> = None;
              let use_io_uring_for_session = conn_socket_options.io_uring.session_enabled && cfg!(feature = "io-uring");

              if use_io_uring_for_session {
                #[cfg(feature = "io-uring")]
                {
                  match accepted_stream.into_std_tcp_stream() {
                    Ok(std_stream) => {
                      if let Err(e) = apply_tcp_socket_options_to_std(&std_stream, &transport_config) {
                        tracing::error!(
                          "Failed to apply TCP options to std_stream for {}: {}. Dropping.",
                          peer_addr_str,
                          e
                        );
                        return; // Permit dropped
                      }
                      let owned_fd: OwnedFd = std_stream.into();
                      let (app_to_engine_tx, app_to_engine_rx) = async_channel::bounded::<AppToUringEngineCmd>(
                        conn_context.inner().get_actor_mailbox_capacity(),
                      );
                      let launch_info = UringLaunchInformation {
                        engine_handle_id,
                        owned_fd,
                        config: ZmtpEngineConfig::from(&*conn_socket_options),
                        is_server: true,
                        context_clone: conn_context.clone(),
                        session_base_mailbox: session_cmd_tx.clone(),
                        app_to_engine_cmd_rx: app_to_engine_rx,
                        parent_session_handle_id: session_handle_id,
                      };
                      match uring_runtime::submit_uring_engine_launch(launch_info) {
                        Ok(true) => engine_connection_type = EngineConnectionType::Uring { app_to_engine_cmd_tx },
                        _ => {
                          tracing::error!("Failed to launch/submit uring engine for {}", connection_specific_uri);
                          let (dummy_tx, _) = async_channel::bounded(1);
                          dummy_tx.close();
                          engine_connection_type = EngineConnectionType::Uring {
                            app_to_engine_cmd_tx: dummy_tx,
                          };
                        }
                      }
                    }
                    Err(e) => {
                      tracing::error!(
                        "Failed to convert accepted stream to std for uring: {}. Session for {} will fail.",
                        e,
                        connection_specific_uri
                      );
                      let (dummy_tx, _) = async_channel::bounded(1);
                      dummy_tx.close();
                      engine_connection_type = EngineConnectionType::Uring {
                        app_to_engine_cmd_tx: dummy_tx,
                      };
                    }
                  }
                }
                #[cfg(not(feature = "io-uring"))]
                {
                  unreachable!();
                }
              } else {
                // Standard engine path
                match accepted_stream.into_tokio_tcp_stream() {
                  Ok(tokio_stream) => {
                    if let Err(e) = apply_tcp_socket_options_to_tokio(&tokio_stream, &transport_config) {
                      tracing::error!(
                        "Failed to apply TCP options to tokio_stream for {}: {}. Dropping.",
                        peer_addr_str,
                        e
                      );
                      return; // Permit dropped
                    }
                    let (std_engine_mailbox, std_engine_task_handle) = create_and_spawn_tcp_engine(
                      engine_handle_id,
                      session_cmd_tx.clone(),
                      tokio_stream,
                      conn_socket_options.clone(),
                      true,
                      &conn_context,
                      session_handle_id,
                    );
                    engine_connection_type = EngineConnectionType::Standard {
                      engine_mailbox: std_engine_mailbox,
                    };
                    engine_task_handle_for_attach = Some(std_engine_task_handle);
                  }
                  Err(e) => {
                    tracing::error!(
                      "Failed to convert accepted stream to tokio for std engine: {}. Session for {} will fail.",
                      e,
                      connection_specific_uri
                    );
                    let (dummy_tx, _) = mailbox(1); // Create a dummy standard engine mailbox
                    dummy_tx.close();
                    engine_connection_type = EngineConnectionType::Standard {
                      engine_mailbox: dummy_tx,
                    };
                  }
                }
              }

              let attach_cmd = Command::Attach {
                connection: engine_connection_type,
                engine_handle: Some(engine_handle_id),
                engine_task_handle: engine_task_handle_for_attach,
              };
              let mut session_setup_ok = true;
              if session_cmd_tx.send(attach_cmd).await.is_err() {
                tracing::error!("Failed to Attach engine for {}", connection_specific_uri);
                session_setup_ok = false;
              } else {
                let event = SystemEvent::NewConnectionEstablished {
                  parent_core_id: parent_socket_core_id,
                  endpoint_uri: connection_specific_uri.clone(),
                  target_endpoint_uri: listener_uri_for_event,
                  session_mailbox: session_cmd_tx,
                  session_handle_id,
                  session_task_id: tokio::task::spawn(async {}).id(), // Dummy task ID
                };
                if conn_context.event_bus().publish(event).is_err() {
                  tracing::error!(
                    "Failed to publish NewConnectionEstablished for {}",
                    connection_specific_uri
                  );
                  session_setup_ok = false;
                }
              }
              if !session_setup_ok { /* TODO: Abort SessionBase task */ }
            }
          });
        }
        Err(e) => {
          drop(permit);
          tracing::error!("Error accepting TCP connection (listener {}): {}", endpoint_uri, e);
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::AcceptFailed {
              endpoint: endpoint_uri.clone(),
              error_msg: e.to_string(),
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              let _ = tx_clone.send(event).await;
            });
          }
          if is_fatal_accept_error(&e) {
            loop_error_to_report = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break;
          }
          sleep(Duration::from_millis(100)).await;
        }
      }
    }
    actor_drop_guard.waive();
    context.publish_actor_stopping(
      accept_loop_handle,
      accept_loop_actor_type,
      Some(endpoint_uri),
      loop_error_to_report,
    );
    tracing::info!("TCP Accept loop {} fully stopped.", accept_loop_handle);
  }

  #[cfg(feature = "io-uring")]
  async fn run_accept_loop_uring(
    accept_loop_handle: usize,
    listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    listener: Arc<underlying_uring_net::UringTcpListener>,
    transport_config: TcpTransportConfig,
    socket_options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
    connection_limiter: Arc<Semaphore>,
  ) {
    Self::run_accept_loop_internal(
      accept_loop_handle,
      listener_cmd_loop_handle,
      endpoint_uri,
      listener,
      transport_config,
      socket_options,
      handle_source,
      monitor_tx,
      context,
      parent_socket_core_id,
      connection_limiter,
      true,
    )
    .await;
  }

  async fn run_accept_loop_std(
    accept_loop_handle: usize,
    listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    listener: Arc<underlying_std_net::TcpListener>,
    transport_config: TcpTransportConfig,
    socket_options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
    connection_limiter: Arc<Semaphore>,
  ) {
    Self::run_accept_loop_internal(
      accept_loop_handle,
      listener_cmd_loop_handle,
      endpoint_uri,
      listener,
      transport_config,
      socket_options,
      handle_source,
      monitor_tx,
      context,
      parent_socket_core_id,
      connection_limiter,
      false,
    )
    .await;
  }
}

// --- TcpConnecter Actor ---
#[derive(Debug)]
pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String,
  config: TcpTransportConfig,          // Pre-connect options
  context_options: Arc<SocketOptions>, // For ZmtpEngineConfig and io_uring.session_enabled
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
    // TcpConnecter is fire-and-forget from SocketCore's perspective
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
    // Added mut self
    let connecter_handle = self.handle;
    let connecter_actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint.clone();

    // This will store the final error that causes the Connecter actor to stop *permanently*.
    // It remains None if the connecter eventually succeeds or is shut down cleanly by a system event
    // that is handled within a helper.
    let mut final_actor_shutdown_reason: Option<ZmqError> = None;

    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "TCP Connecter actor started connection attempts.");

    // --- Start: Initial parameter parsing (target_addr_str, target_socket_addr) ---
    let target_addr_str = match endpoint_uri_clone.strip_prefix("tcp://") {
      Some(addr) => addr.to_string(),
      None => {
        final_actor_shutdown_reason = Some(ZmqError::InvalidEndpoint(endpoint_uri_clone.clone()));
        // No retries possible if endpoint is invalid from the start.
        // Publish stopping event and return.
        self.context.publish_actor_stopping(
          connecter_handle,
          connecter_actor_type,
          Some(endpoint_uri_clone),
          final_actor_shutdown_reason.clone(),
        );
        tracing::error!(handle = connecter_handle, uri = %self.endpoint, "Invalid TCP endpoint format. Stopping.");
        return;
      }
    };
    let target_socket_addr: StdSocketAddr = match target_addr_str.parse() {
      Ok(addr) => addr,
      Err(e) => {
        final_actor_shutdown_reason = Some(ZmqError::InvalidEndpoint(format!(
          "Failed to parse target address '{}': {}",
          target_addr_str, e
        )));
        self.context.publish_actor_stopping(
          connecter_handle,
          connecter_actor_type,
          Some(endpoint_uri_clone),
          final_actor_shutdown_reason.clone(),
        );
        tracing::error!(handle = connecter_handle, uri = %self.endpoint, "Failed to parse target TCP address. Stopping.");
        return;
      }
    };
    // --- End: Initial parameter parsing ---

    let initial_reconnect_ivl_opt = self.context_options.reconnect_ivl;
    let mut current_retry_delay_base = initial_reconnect_ivl_opt.unwrap_or(Duration::from_millis(5000));
    // Handle ZMQ_RECONNECT_IVL=0 correctly: first attempt occurs, then loop breaks if it failed.
    // This is managed by the conditions at the end of the 'retry_loop'.

    let max_delay_opt = self.context_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut system_event_rx = self.context.event_bus().subscribe();

    'retry_loop: loop {
      // --- 1. Handle Retry Delay (if not the first attempt) ---
      if attempt_count > 0 {
        // If initial_reconnect_ivl_opt was Some(ZERO), this block is correctly skipped after the first failure
        // because the break condition at the end of the loop would have been met.
        match self
          .wait_for_retry_delay(
            current_retry_delay_base,
            &mut system_event_rx,
            &monitor_tx,
            attempt_count,
            &endpoint_uri_clone,
          )
          .await
        {
          Ok(()) => { /* Delay completed or non-fatally interrupted */ }
          Err(fatal_event_err) => {
            // Fatal system event occurred during delay
            final_actor_shutdown_reason = Some(fatal_event_err);
            break 'retry_loop;
          }
        }
        // Update delay for next potential retry (if this one fails and is retriable)
        if let Some(md) = max_delay_opt {
          if md > Duration::ZERO {
            current_retry_delay_base = (current_retry_delay_base * 2).min(md);
          }
        } else {
          if current_retry_delay_base > Duration::ZERO {
            current_retry_delay_base *= 2;
          } else if initial_reconnect_ivl_opt.is_none() {
            current_retry_delay_base = Duration::from_millis(100) * 2;
          }
        }
      }
      attempt_count += 1;

      // --- 2. Pre-emptive check for fatal system events BEFORE this attempt ---
      // This check is important if a fatal event arrived while not in an await point of the connecter.
      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) => {
          final_actor_shutdown_reason = Some(ZmqError::Internal(
            "Connecter shutdown by ContextTerminating (pre-attempt)".into(),
          ));
        }
        Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => {
          final_actor_shutdown_reason = Some(ZmqError::Internal(
            "Connecter shutdown by parent SocketClosing (pre-attempt)".into(),
          ));
        }
        Err(broadcast::error::TryRecvError::Closed) => {
          final_actor_shutdown_reason = Some(ZmqError::Internal("Connecter event bus closed (pre-attempt)".into()));
        }
        _ => {} // No fatal event, or event queue empty, or non-fatal event.
      }
      if final_actor_shutdown_reason.is_some() {
        break 'retry_loop;
      }

      // --- 3. Perform a Single Connection Attempt ---
      tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, "Attempting TCP connect #{}", attempt_count);

      // `try_connect_once` itself handles fatal system events during the connection attempt.
      let attempt_result = self
        .try_connect_once(
          &target_socket_addr,
          &target_addr_str,
          &endpoint_uri_clone,
          &mut system_event_rx,
        )
        .await;

      // This variable holds the error specific to *this* connection attempt, if it failed.
      let mut error_from_this_specific_attempt: Option<ZmqError> = None;

      match attempt_result {
        Ok(connect_output) => {
          // TCP Connection was successful, now set up session and engine
          match self
            .setup_session_and_engine(connect_output, &monitor_tx, &endpoint_uri_clone)
            .await
          {
            Ok(()) => {
              // Full setup successful
              final_actor_shutdown_reason = None; // Mark overall success for connecter
            }
            Err(setup_err) => {
              // Setup after connect failed (e.g., attaching engine)
              final_actor_shutdown_reason = Some(setup_err);
            }
          }
          break 'retry_loop; // Break whether setup succeeded or failed; setup failures are not retried by this loop.
        }
        Err(attempt_err) => {
          // Single connection attempt failed
          error_from_this_specific_attempt = Some(attempt_err.clone()); // Note: clone `attempt_err`
          tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_err, "TCP Connect attempt #{} failed", attempt_count);

          // Emit ConnectDelayed only on first actual connection failure if retries are enabled
          // and the error isn't one that inherently stops the connecter (like a shutdown event).
          if attempt_count == 1 &&
                 initial_reconnect_ivl_opt != Some(Duration::ZERO) && // Retries are enabled
                 !is_fatal_connect_error(&attempt_err) && // Error is retriable
                 !matches!(&attempt_err, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error"))
          // Not a fatal system event
          {
            if let Some(ref tx_mon) = monitor_tx {
              let event = SocketEvent::ConnectDelayed {
                endpoint: endpoint_uri_clone.clone(),
                error_msg: format!("{}", attempt_err),
              };
              let tx_clone = tx_mon.clone();
              tokio::spawn(async move {
                let _ = tx_clone.send(event).await;
              });
            }
          }
        }
      } // End match attempt_result

      // --- Decide if the 'retry_loop' should break based on the outcome of this attempt ---
      // If `final_actor_shutdown_reason` was set by a fatal system event *during this attempt* (inside try_connect_once),
      // or by a setup failure, the loop will break here.
      if final_actor_shutdown_reason.is_some() {
        break 'retry_loop;
      }

      // If we reached here, it means the `attempt_result` was `Err(attempt_err)`,
      // and that `attempt_err` was not a fatal system event caught by `try_connect_once` directly.
      // Now, check `error_from_this_specific_attempt`.
      if let Some(ref current_attempt_err_ref) = error_from_this_specific_attempt {
        // Check if this attempt's error means we should stop all retries for the connecter.
        if initial_reconnect_ivl_opt == Some(Duration::ZERO) || // Configured to not retry after first failure
             is_fatal_connect_error(current_attempt_err_ref) ||   // This attempt's error is fatal for further retries
             matches!(current_attempt_err_ref, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error"))
        // Should have been caught by try_connect_once, but check again
        {
          final_actor_shutdown_reason = error_from_this_specific_attempt; // Store this as the final reason
          break 'retry_loop;
        }
        // Otherwise (error is retriable, and retries are enabled), the loop continues.
        // `final_actor_shutdown_reason` remains None, allowing next iteration.
      } else {
        // `error_from_this_specific_attempt` is None. This means `attempt_result` was `Ok`.
        // The `Ok` arm of `match attempt_result` should have set `final_actor_shutdown_reason` (to None or setup_err)
        // and then `break 'retry_loop'`. So, this path should ideally not be reached.
        tracing::error!(
          handle = connecter_handle,
          "Connecter retry loop in inconsistent state (attempt succeeded but didn't break and no error recorded)."
        );
        final_actor_shutdown_reason = Some(ZmqError::Internal(
          "Connecter retry logic state error (success without break)".into(),
        ));
        break 'retry_loop;
      }
    } // End of 'retry_loop

    // --- Post-Loop: Publish final status ---
    if let Some(ref err) = final_actor_shutdown_reason {
      // Only publish ConnectionAttemptFailed if the error was a genuine connection failure,
      // not an internal shutdown signal for the connecter itself.
      if !matches!(err, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error") || s.contains("state error") || s.contains("launch submit failed"))
      {
        tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %err, "All TCP connect attempts failed or connecter stopped due to error.");
        let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: self.parent_socket_id,
          target_endpoint_uri: endpoint_uri_clone.clone(),
          error_msg: err.to_string(),
        });
        if let Some(ref tx_mon) = monitor_tx {
          let event_monitor = SocketEvent::ConnectFailed {
            endpoint: endpoint_uri_clone.clone(),
            error_msg: err.to_string(),
          };
          // Use try_send as connecter is stopping, don't want to block on monitor if its channel is full.
          let _ = tx_mon.try_send(event_monitor);
        }
      }
    }

    // Publish ActorStopping for the connecter itself.
    // `final_actor_shutdown_reason` correctly reflects the reason for stopping (or None if it was a setup success that broke the loop).
    self.context.publish_actor_stopping(
      connecter_handle,
      connecter_actor_type,
      Some(endpoint_uri_clone),
      final_actor_shutdown_reason,
    );
    tracing::info!(handle = connecter_handle, uri = %self.endpoint, "TCP Connecter actor fully stopped.");
  }

  /// Helper function to handle the delay between retry attempts.
  /// Returns Ok(()) if the delay completed, or Err(ZmqError) if a fatal system event occurred.
  async fn wait_for_retry_delay(
    &self, // Taking &self for access to handle, parent_socket_id etc. for logging/context
    delay: Duration,
    system_event_rx: &mut broadcast::Receiver<SystemEvent>, // Mutable ref to consume events
    monitor_tx: &Option<MonitorSender>,                     // For ConnectRetried
    attempt_count: usize,                                   // For logging
    endpoint_uri_clone: &str,                               // For logging
  ) -> Result<(), ZmqError> {
    if delay.is_zero() && attempt_count > 1 {
      // No delay if duration is zero, unless it's after the first attempt (RECONNECT_IVL=0 case)
      return Ok(());
    }

    tracing::debug!(handle = self.handle, uri = %endpoint_uri_clone, ?delay, "Waiting before reconnect attempt #{}", attempt_count + 1);
    if let Some(ref tx) = monitor_tx {
      let event = SocketEvent::ConnectRetried {
        endpoint: endpoint_uri_clone.to_string(),
        interval: delay,
      };
      let tx_clone = tx.clone();
      tokio::spawn(async move {
        let _ = tx_clone.send(event).await;
      });
    }

    tokio::select! {
        biased;
        _ = sleep(delay) => Ok(()),
        event_result = system_event_rx.recv() => {
             match event_result {
                Ok(SystemEvent::ContextTerminating) => {
                    tracing::info!(handle = self.handle, uri = %endpoint_uri_clone, "Connecter shutdown during retry delay by ContextTerminating.");
                    Err(ZmqError::Internal("Connecter shutdown by ContextTerminating".into()))
                }
                Ok(SystemEvent::SocketClosing{ socket_id: sid }) if sid == self.parent_socket_id => {
                    tracing::info!(handle = self.handle, uri = %endpoint_uri_clone, "Connecter shutdown during retry delay by parent SocketClosing.");
                    Err(ZmqError::Internal("Connecter shutdown by parent SocketClosing".into()))
                }
                Err(e) => { // Lagged or Closed
                    tracing::warn!(handle = self.handle, uri = %endpoint_uri_clone, "Connecter: Error receiving from system event bus during retry delay: {:?}. Aborting.", e);
                    Err(ZmqError::Internal(format!("Connecter event bus error during retry delay: {:?}", e)))
                 }
                Ok(_) => {
                    tracing::trace!(handle = self.handle, uri = %endpoint_uri_clone, "Non-fatal system event during retry delay. Proceeding with retry.");
                    Ok(()) // Non-fatal event, delay is effectively over or can be considered completed.
                }
            }
        }
    }
  }

  /// Helper function to perform a single TCP connection attempt.
  /// Monitors for fatal system events during the attempt.
  /// Returns Ok(ConnectLogicOutputType) on successful connection,
  /// or Err(ZmqError) if connection fails or a fatal event occurs.
  async fn try_connect_once(
    &self, // For access to self.config, self.context_options, etc.
    target_socket_addr: &StdSocketAddr,
    target_addr_str: &str,    // String version of target for uring connect
    endpoint_uri_clone: &str, // Original user endpoint for error context
    system_event_rx: &mut broadcast::Receiver<SystemEvent>,
  ) -> Result<ConnectLogicOutputType, ZmqError> {
    let use_io_uring = self.context_options.io_uring.session_enabled && cfg!(feature = "io-uring");

    let mut connect_future = Box::pin(async {
      if use_io_uring {
        #[cfg(feature = "io-uring")]
        {
          let domain = if target_socket_addr.is_ipv4() {
            socket2::Domain::IPV4
          } else {
            socket2::Domain::IPV6
          };
          let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
          apply_socket2_options_pre_connect(&socket, &self.config)?;
          let std_stream = tokio::task::spawn_blocking(move || {
            socket.connect(&target_socket_addr.into())?; // Use already parsed target_socket_addr
            socket.into_tcp_stream()
          })
          .await
          .map_err(|je| ZmqError::Internal(format!("Blocking connect task join error: {}", je)))??;
          std_stream.set_nonblocking(true)?;
          let owned_fd: OwnedFd = unsafe { OwnedFd::from_raw_fd(std_stream.into_raw_fd()) };
          let (app_to_engine_tx, app_to_engine_rx) =
            async_channel::bounded::<AppToUringEngineCmd>(self.context.inner().get_actor_mailbox_capacity());
          let session_handle_id = self
            .context_handle_source
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = self
            .context_handle_source
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          Ok((
            session_handle_id,
            engine_handle_id,
            EngineConnectionType::Uring { app_to_engine_cmd_tx },
            Some(owned_fd),
            Box::new(app_to_engine_rx) as Box<dyn std::any::Any + Send>,
            format!("tcp://{}", target_addr_str), // Using target_addr_str for consistency
          ))
        }
        #[cfg(not(feature = "io-uring"))]
        {
          unreachable!();
        }
      } else {
        // Standard Tokio path
        let std_tokio_stream = underlying_std_net::TcpStream::connect(target_socket_addr)
          .await // Use parsed target_socket_addr
          .map_err(|e| ZmqError::from_io_endpoint(e, endpoint_uri_clone))?;
        apply_tcp_socket_options_to_tokio(&std_tokio_stream, &self.config)?;
        let peer_addr_actual = std_tokio_stream
          .peer_addr()
          .map_err(|e| ZmqError::Internal(format!("Failed to get peer_addr post-connect: {}", e)))?;
        let session_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let engine_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (dummy_engine_tx, _) = async_channel::bounded::<Command>(1);
        Ok((
          session_handle_id,
          engine_handle_id,
          EngineConnectionType::Standard {
            engine_mailbox: dummy_engine_tx,
          },
          None,
          Box::new(std_tokio_stream) as Box<dyn std::any::Any + Send>,
          format!("tcp://{}", peer_addr_actual),
        ))
      }
    });

    // Loop to poll connect_future and system_event_rx for this single attempt
    loop {
      tokio::select! {
          biased;
          event_res = system_event_rx.recv() => {
              match event_res {
                  Ok(SystemEvent::ContextTerminating) => return Err(ZmqError::Internal("Connect attempt aborted by ContextTerminating".into())),
                  Ok(SystemEvent::SocketClosing{ socket_id: sid }) if sid == self.parent_socket_id => return Err(ZmqError::Internal("Connect attempt aborted by parent SocketClosing".into())),
                  Ok(_) => { /* Non-fatal event, continue polling connect_future */ }
                  Err(e) => return Err(ZmqError::Internal(format!("Event bus error during connect attempt: {:?}", e))),
              }
          }
          connect_logic_output = &mut connect_future => {
              return connect_logic_output; // Return the result of the connect_future
          }
      }
    }
  }

  /// Helper function to set up Session and Engine after a successful TCP connection.
  /// Returns Ok(()) if setup is successful, or Err(ZmqError) if setup fails (e.g., attaching engine).
  async fn setup_session_and_engine(
    &self, // For access to self.context, self.parent_socket_id, etc.
    connect_output: ConnectLogicOutputType,
    monitor_tx: &Option<MonitorSender>,
    endpoint_uri_clone: &str, // Original user-provided endpoint URI for target_endpoint_uri in event
  ) -> Result<(), ZmqError> {
    let (
      session_handle_id,
      engine_handle_id,
      engine_conn_type_template,
      opt_owned_fd,
      stream_or_app_rx_boxed,
      connection_specific_uri, // This is the actual peer URI
    ) = connect_output;

    if let Some(ref tx_mon) = monitor_tx {
      let event = SocketEvent::Connected {
        endpoint: endpoint_uri_clone.to_string(),   // User's target
        peer_addr: connection_specific_uri.clone(), // Actual connected peer
      };
      let tx_clone = tx_mon.clone();
      tokio::spawn(async move {
        let _ = tx_clone.send(event).await;
      });
    }

    let (session_cmd_tx, session_base_task_handle) = SessionBase::create_and_spawn(
      session_handle_id,
      connection_specific_uri.clone(),
      monitor_tx.clone(),
      self.context.clone(),
      self.parent_socket_id,
    );

    let final_engine_conn_type: EngineConnectionType;
    let mut final_engine_task_handle: Option<JoinHandle<()>> = None;

    match engine_conn_type_template {
      #[cfg(feature = "io-uring")]
      EngineConnectionType::Uring { app_to_engine_cmd_tx } => {
        let app_to_engine_cmd_rx = stream_or_app_rx_boxed
          .downcast_arc_receiver()
          .ok_or_else(|| ZmqError::Internal("Logic error: Failed to downcast app_cmd_rx for uring setup".into()))?;
        let launch_info = UringLaunchInformation {
          engine_handle_id,
          owned_fd: opt_owned_fd.expect("OwnedFd must be Some for Uring path in setup"),
          config: ZmtpEngineConfig::from(&*self.context_options),
          is_server: false,
          context_clone: self.context.clone(),
          session_base_mailbox: session_cmd_tx.clone(),
          app_to_engine_cmd_rx,
          parent_session_handle_id: session_handle_id,
        };
        uring_runtime::submit_uring_engine_launch(launch_info)
          .map_err(|e| ZmqError::Internal(format!("Uring engine launch submit failed during setup: {:?}", e)))?
          .then_some(())
          .ok_or_else(|| ZmqError::Internal("Uring runtime did not accept launch during setup".into()))?;
        final_engine_conn_type = EngineConnectionType::Uring { app_to_engine_cmd_tx };
      }
      EngineConnectionType::Standard { .. } => {
        let std_tokio_stream = stream_or_app_rx_boxed
          .downcast_arc_stream()
          .ok_or_else(|| ZmqError::Internal("Logic error: Failed to downcast TcpStream for std engine setup".into()))?;
        let (std_engine_mailbox, std_engine_task_handle) = create_and_spawn_tcp_engine(
          engine_handle_id,
          session_cmd_tx.clone(),
          std_tokio_stream,
          self.context_options.clone(),
          false,
          &self.context,
          session_handle_id,
        );
        final_engine_conn_type = EngineConnectionType::Standard {
          engine_mailbox: std_engine_mailbox,
        };
        final_engine_task_handle = Some(std_engine_task_handle);
      }
      #[cfg(not(feature = "io-uring"))]
      _ => unreachable!("Invalid EngineConnectionType template when io-uring is not enabled during setup"),
    }

    let attach_cmd = Command::Attach {
      connection: final_engine_conn_type,
      engine_handle: Some(engine_handle_id),
      engine_task_handle: final_engine_task_handle,
    };

    if session_cmd_tx.send(attach_cmd).await.is_err() {
      session_base_task_handle.abort(); // Abort session if attach fails
      return Err(ZmqError::Internal(
        "Failed to attach engine to session post-connect".into(),
      ));
    } else {
      publish_new_conn_event(
        &self.context,
        self.parent_socket_id,
        &connection_specific_uri,
        endpoint_uri_clone,
        session_cmd_tx,
        session_handle_id,
        // session_base_task_handle.id() // Get task ID if needed by event
      );
      Ok(())
    }
  }
}

// Placeholder functions for IDs, replace with actual ID generation
fn engine_handle_id_placeholder() -> usize {
  0
}
fn session_handle_id_placeholder() -> usize {
  0
}

fn publish_new_conn_event(
  context: &Context,
  parent_core_id: usize,
  endpoint_uri: &str,
  target_endpoint_uri: &str,
  session_mailbox: MailboxSender,
  session_handle_id: usize,
) {
  let event = SystemEvent::NewConnectionEstablished {
    parent_core_id,
    endpoint_uri: endpoint_uri.to_string(),
    target_endpoint_uri: target_endpoint_uri.to_string(),
    session_mailbox,
    session_handle_id,
    session_task_id: tokio::task::spawn(async {}).id(), // Dummy
  };
  if context.event_bus().publish(event).is_err() {
    tracing::error!("Failed to publish NewConnectionEstablished for {}", endpoint_uri);
  }
}

// Helper traits for generic accept loop
#[async_trait::async_trait]
trait AcceptStream {
  type Stream: AcceptedStream + Send + Sync + 'static;
  async fn accept_stream(&self) -> io::Result<(Self::Stream, StdSocketAddr)>;
}

#[cfg(feature = "io-uring")]
#[async_trait::async_trait]
impl AcceptStream for underlying_uring_net::UringTcpListener {
  type Stream = underlying_uring_net::UringTcpStream;
  async fn accept_stream(&self) -> io::Result<(Self::Stream, StdSocketAddr)> {
    self.accept().await // UringTcpListener::accept()
  }
}

#[async_trait::async_trait]
impl AcceptStream for underlying_std_net::TcpListener {
  type Stream = underlying_std_net::TcpStream;
  async fn accept_stream(&self) -> io::Result<(Self::Stream, StdSocketAddr)> {
    self.accept().await // tokio::net::TcpListener::accept()
  }
}

// Helper trait for converting accepted stream to std or tokio variants
trait AcceptedStream {
  fn into_std_tcp_stream(self) -> io::Result<std::net::TcpStream>;
  fn into_tokio_tcp_stream(self) -> io::Result<tokio::net::TcpStream>;
}

#[cfg(feature = "io-uring")]
impl AcceptedStream for underlying_uring_net::UringTcpStream {
  fn into_std_tcp_stream(self) -> io::Result<std::net::TcpStream> {
    self.into_std()
  }
  fn into_tokio_tcp_stream(self) -> io::Result<tokio::net::TcpStream> {
    self.into_std().and_then(tokio::net::TcpStream::from_std)
  }
}

impl AcceptedStream for underlying_std_net::TcpStream {
  fn into_std_tcp_stream(self) -> io::Result<std::net::TcpStream> {
    self.into_std()
  }
  fn into_tokio_tcp_stream(self) -> io::Result<tokio::net::TcpStream> {
    Ok(self) // Already a tokio::net::TcpStream
  }
}

fn apply_socket2_options_pre_connect(socket: &socket2::Socket, config: &TcpTransportConfig) -> Result<(), ZmqError> {
  socket.set_nodelay(config.tcp_nodelay).map_err(ZmqError::from)?;
  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    #[cfg(any(unix, target_os = "windows"))]
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval);
    }
    #[cfg(unix)]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count);
    }
    socket.set_tcp_keepalive(&keepalive).map_err(ZmqError::from)?;
  }
  Ok(())
}

fn apply_tcp_socket_options_to_tokio(
  stream: &tokio::net::TcpStream,
  config: &TcpTransportConfig,
) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream); // Safe for tokio::net::TcpStream
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    #[cfg(any(unix, target_os = "windows"))] // Tokio TcpStream has these capabilities
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval);
    }
    #[cfg(unix)]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count);
    }
    socket_ref.set_tcp_keepalive(&keepalive)?;
  }
  Ok(())
}

fn apply_tcp_socket_options_to_std(stream: &std::net::TcpStream, config: &TcpTransportConfig) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream); // Safe for std::net::TcpStream
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    #[cfg(any(unix, target_os = "windows"))]
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval);
    }
    #[cfg(unix)]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count);
    }
    socket_ref.set_tcp_keepalive(&keepalive)?;
  }
  Ok(())
}

// Helper Downcast Arc Any for stream_or_app_rx variants
// This is a bit of a hack. A better way would be an enum for stream_or_app_rx.
trait DynamicDowncast {
  #[cfg(feature = "io-uring")]
  fn downcast_arc_receiver(self) -> Option<async_channel::Receiver<AppToUringEngineCmd>>;
  fn downcast_arc_stream(self) -> Option<underlying_std_net::TcpStream>;
}

impl DynamicDowncast for Box<dyn std::any::Any + Send> {
  #[cfg(feature = "io-uring")]
  fn downcast_arc_receiver(self) -> Option<async_channel::Receiver<AppToUringEngineCmd>> {
    self
      .downcast::<async_channel::Receiver<AppToUringEngineCmd>>()
      .ok()
      .map(|boxed_val| *boxed_val)
  }
  fn downcast_arc_stream(self) -> Option<underlying_std_net::TcpStream> {
    self
      .downcast::<underlying_std_net::TcpStream>()
      .ok()
      .map(|boxed_val| *boxed_val)
  }
}

fn is_fatal_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

fn is_fatal_connect_error(e: &ZmqError) -> bool {
  match e {
    ZmqError::IoError { kind, .. } => {
      matches!(
        kind,
        io::ErrorKind::AddrNotAvailable
          | io::ErrorKind::AddrInUse
          | io::ErrorKind::InvalidInput
          | io::ErrorKind::PermissionDenied
      )
    }
    ZmqError::InvalidEndpoint(_) => true,
    ZmqError::UnsupportedTransport(_) => true,
    _ => false,
  }
}

impl From<&SocketOptions> for ZmtpEngineConfig {
  fn from(options: &SocketOptions) -> Self {
    ZmtpEngineConfig {
      routing_id: options.routing_id.clone(),
      socket_type_name: options.socket_type_name.clone(),
      heartbeat_ivl: options.heartbeat_ivl,
      heartbeat_timeout: options.heartbeat_timeout,
      use_send_zerocopy: options.io_uring.send_zerocopy,
      use_recv_multishot: options.io_uring.recv_multishot,
      use_cork: options.tcp_cork,
      #[cfg(feature = "io-uring")]
      recv_multishot_buffer_count: options.iouring.recv_buffer_count,
      #[cfg(feature = "io-uring")]
      recv_multishot_buffer_capacity: options.iouring.recv_buffer_size,

      #[cfg(feature = "noise_xx")]
      use_noise_xx: options.noise_xx_options.enabled,
      #[cfg(feature = "noise_xx")]
      noise_xx_local_sk_bytes_for_engine: options.noise_xx_options.static_secret_key_bytes,
      #[cfg(feature = "noise_xx")]
      noise_xx_remote_pk_bytes_for_engine: options.noise_xx_options.remote_static_public_key_bytes,
    }
  }
}

// Placeholder type for the `connect_logic_result` in `TcpConnecter::run_connect_loop`
// This helps make the Ok variant consistent before downcasting or specific handling.
type ConnectLogicOutputType = (
  usize,                         // session_handle_id
  usize,                         // engine_handle_id
  EngineConnectionType,          // engine_conn_type_template
  Option<OwnedFd>,               // opt_owned_fd (for uring)
  Box<dyn std::any::Any + Send>, // stream_or_app_rx (boxed tokio::net::TcpStream or async_channel::Receiver<AppToUringEngineCmd>)
  String,                        // connection_specific_uri
);
