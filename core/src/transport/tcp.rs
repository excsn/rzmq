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
use crate::socket::options::{SocketOptions, TcpTransportConfig, ZmtpEngineConfig};
use crate::socket::DEFAULT_RECONNECT_IVL_MS; // Added ZmtpEngineConfig

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
// Define the output type for a successful connection attempt by try_connect_once
// This tuple will contain everything needed by setup_session_and_engine
type ConnectLogicOutputType = (
  usize,                         // session_handle_id
  usize,                         // engine_handle_id
  EngineConnectionType,          // engine_conn_type_template (Standard or Uring placeholder)
  Option<OwnedFd>,               // opt_owned_fd (for uring path)
  Box<dyn std::any::Any + Send>, // stream_or_app_rx_boxed (actual TcpStream or Uring's AppToEngineCmd Receiver)
  String,                        // connection_specific_uri (e.g., tcp://<actual_peer_ip>:<port>)
);

#[derive(Debug)]
pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String,                                           // Original user-provided endpoint
  config: TcpTransportConfig,                                 // Pre-connect options for socket2
  context_options: Arc<SocketOptions>,                        // Full socket options for engine, retry logic
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // For new actor IDs
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
    // Connecter is fire-and-forget from SocketCore's perspective
    let actor_type = ActorType::Connecter;

    let transport_config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    let connecter_actor = TcpConnecter {
      handle,
      endpoint: endpoint.clone(), // Store original endpoint
      config: transport_config,
      context_options: options, // Arc clone
      context_handle_source,    // Arc clone
      context: context.clone(),
      parent_socket_id,
    };

    let task_join_handle = tokio::spawn(connecter_actor.run_connect_loop(monitor_tx));

    // Publish ActorStarted for the Connecter actor itself.
    context.publish_actor_started(handle, actor_type, Some(parent_socket_id));
    task_join_handle
  }

  /// Main lifecycle loop for the TcpConnecter actor.
  /// It attempts to connect to the target endpoint, handling retries internally
  /// based on socket options, until a connection is established, a fatal error occurs,
  /// or a shutdown signal is received.
  async fn run_connect_loop(mut self, monitor_tx: Option<MonitorSender>) {
    let connecter_handle = self.handle;
    let actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint.clone(); // For logging and events

    tracing::info!(
        handle = connecter_handle,
        uri = %endpoint_uri_clone,
        "Long-Lived TCP Connecter actor started. Will manage its own retries."
    );

    // --- Parameter Parsing (once at the start) ---
    let target_addr_str: String;
    let target_socket_addr: StdSocketAddr;
    match endpoint_uri_clone.strip_prefix("tcp://") {
      Some(addr) => {
        target_addr_str = addr.to_string();
        match addr.parse::<StdSocketAddr>() {
          Ok(parsed_addr) => target_socket_addr = parsed_addr,
          Err(e) => {
            let err = ZmqError::InvalidEndpoint(format!("Failed to parse target address '{}': {}", addr, e));
            tracing::error!(handle = connecter_handle, uri = %self.endpoint, error = %err, "Invalid TCP endpoint address. Connecter stopping.");
            let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed {
              parent_core_id: self.parent_socket_id,
              target_endpoint_uri: self.endpoint.clone(),
              error_msg: err.to_string(),
            });
            self
              .context
              .publish_actor_stopping(connecter_handle, actor_type, Some(endpoint_uri_clone), Some(err));
            return;
          }
        }
      }
      None => {
        let err = ZmqError::InvalidEndpoint(endpoint_uri_clone.clone());
        tracing::error!(handle = connecter_handle, uri = %self.endpoint, error = %err, "Invalid TCP endpoint scheme. Connecter stopping.");
        let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: self.parent_socket_id,
          target_endpoint_uri: self.endpoint.clone(),
          error_msg: err.to_string(),
        });
        self
          .context
          .publish_actor_stopping(connecter_handle, actor_type, Some(endpoint_uri_clone), Some(err));
        return;
      }
    };

    // --- Retry Logic Parameters ---
    let initial_reconnect_ivl = self
      .context_options
      .reconnect_ivl
      .unwrap_or(Duration::from_millis(DEFAULT_RECONNECT_IVL_MS));
    // ZMQ_RECONNECT_IVL=0 means try once, then if ZMQ_RECONNECT_IVL_MAX is also 0, give up.
    // If ZMQ_RECONNECT_IVL_MAX > 0, then use ZMQ_RECONNECT_IVL_MAX as fixed delay.
    // The current ZmqOption parsing maps ZMQ_RECONNECT_IVL=0 to None, so we default to 100ms.
    // Let's adjust to better match ZMQ:
    // if ZMQ_RECONNECT_IVL=0, no retries unless MAX is also 0 (which means give up) or MAX > 0 (use MAX as delay).
    let no_retry_after_first_connect_fail = self.context_options.reconnect_ivl == Some(Duration::ZERO)
      && (self.context_options.reconnect_ivl_max == Some(Duration::ZERO)
        || self.context_options.reconnect_ivl_max.is_none());

    let mut current_retry_delay = if self.context_options.reconnect_ivl == Some(Duration::ZERO)
      && self
        .context_options
        .reconnect_ivl_max
        .map_or(false, |d| d > Duration::ZERO)
    {
      self.context_options.reconnect_ivl_max.unwrap() // Use MAX as fixed delay if IVL=0 and MAX>0
    } else {
      initial_reconnect_ivl
    };
    let reconnect_ivl_max_opt = self.context_options.reconnect_ivl_max;

    let mut attempt_count = 0;
    let mut system_event_rx = self.context.event_bus().subscribe();
    let mut last_connect_attempt_error: Option<ZmqError> = None;

    'connecter_life_loop: loop {
      // --- 1. Apply Delay if this is a retry ---
      if attempt_count > 0 {
        // Check if retries are disabled for this specific configuration
        if no_retry_after_first_connect_fail {
          // This was already checked for attempt_count=1, but defensive
          tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: Retries disabled (ZMQ_RECONNECT_IVL=0 and no effective ZMQ_RECONNECT_IVL_MAX). Stopping after first failure.");
          break 'connecter_life_loop;
        }

        match self
          .wait_for_retry_delay_internal(
            &mut system_event_rx,
            current_retry_delay,
            &monitor_tx,
            attempt_count + 1,
          )
          .await
        {
          Ok(true) => { /* Delay completed successfully */ }
          Ok(false) => {
            // Shutdown signaled during delay
            last_connect_attempt_error = Some(ZmqError::Internal(
              "Connecter shutdown by event during retry delay.".into(),
            ));
            break 'connecter_life_loop;
          }
          Err(()) => {
            // Should map to specific error if wait_for_retry_delay_internal is changed
            last_connect_attempt_error = Some(ZmqError::Internal(
              "Connecter shutdown error during retry delay.".into(),
            ));
            break 'connecter_life_loop;
          }
        }

        // Update delay for the *next* potential retry (if ZMQ_RECONNECT_IVL_MAX is not 0)
        if current_retry_delay > Duration::ZERO && reconnect_ivl_max_opt.map_or(true, |max_d| max_d > Duration::ZERO) {
          if let Some(max_d) = reconnect_ivl_max_opt {
            // Max is > 0, apply exponential backoff up to max
            current_retry_delay = (current_retry_delay * 2).min(max_d);
          } else {
            // Max is None (default) -> use fixed interval
            current_retry_delay = initial_reconnect_ivl;
          }
        } else if reconnect_ivl_max_opt.map_or(false, |max_d| max_d > Duration::ZERO) {
          // Current delay is 0 (e.g. initial_reconnect_ivl was 0), but MAX is set, so use MAX for retries
          current_retry_delay = reconnect_ivl_max_opt.unwrap();
        }
        // If current_retry_delay is 0 and MAX is 0 or None, it means ZMQ_RECONNECT_IVL=0, so retries should stop (handled by no_retry_after_first_connect_fail)
      }
      attempt_count += 1;

      // --- 2. Pre-emptive check for shutdown signals ---
      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) => {
          last_connect_attempt_error = Some(ZmqError::Internal(
            "Connecter shutdown by ContextTerminating (pre-connect).".into(),
          ));
          break 'connecter_life_loop;
        }
        Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => {
          last_connect_attempt_error = Some(ZmqError::Internal(
            "Connecter shutdown by parent SocketClosing (pre-connect).".into(),
          ));
          break 'connecter_life_loop;
        }
        Err(broadcast::error::TryRecvError::Closed) => {
          last_connect_attempt_error = Some(ZmqError::Internal("Connecter event bus closed (pre-connect).".into()));
          break 'connecter_life_loop;
        }
        Err(broadcast::error::TryRecvError::Lagged(n)) => {
          tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, skipped = n, "Connecter event bus lagged (pre-connect). Stopping.");
          last_connect_attempt_error = Some(ZmqError::Internal("Connecter event bus lagged (pre-connect).".into()));
          break 'connecter_life_loop;
        }
        _ => {} // No fatal event, or queue empty
      }

      tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: Attempting TCP connect #{}", attempt_count);

      // --- 3. Perform a Single Connection Attempt ---
      let single_attempt_outcome = self
        .try_connect_once(&target_socket_addr, &endpoint_uri_clone, &mut system_event_rx)
        .await;

      match single_attempt_outcome {
        Ok(connect_output_tuple) => {
          match self
            .setup_session_and_engine(connect_output_tuple, &monitor_tx, &endpoint_uri_clone)
            .await
          {
            Ok(()) => {
              tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: Connection and full setup successful.");
              last_connect_attempt_error = None; // Success
            }
            Err(setup_err) => {
              tracing::error!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %setup_err, "Connecter: Session/Engine setup FAILED after TCP connect.");
              last_connect_attempt_error = Some(setup_err);
            }
          }
          break 'connecter_life_loop; // Exit loop on success or setup failure.
        }
        Err(attempt_failure_error) => {
          last_connect_attempt_error = Some(attempt_failure_error.clone());
          tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_failure_error, "Connecter: TCP connect attempt #{} failed.", attempt_count);

          if is_fatal_connect_error(&attempt_failure_error)
            || matches!(&attempt_failure_error, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error"))
          {
            tracing::error!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_failure_error, "Connecter: Fatal error during connect attempt. Stopping retry loop.");
            break 'connecter_life_loop;
          }

          if no_retry_after_first_connect_fail && attempt_count == 1 {
            tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_failure_error, "Connecter: ZMQ_RECONNECT_IVL=0 & no MAX, stopping after first failed attempt.");
            break 'connecter_life_loop;
          }

          // Emit ConnectDelayed on the first failure that leads to a retry
          if attempt_count == 1 {
            if let Some(ref mon_tx) = monitor_tx {
              let _ = mon_tx.try_send(SocketEvent::ConnectDelayed {
                endpoint: endpoint_uri_clone.clone(),
                error_msg: attempt_failure_error.to_string(),
              });
            }
          }
          // Loop continues for another retry
        }
      }
    } // End of 'connecter_life_loop

    // --- Post-Loop: Connecter is stopping ---
    if let Some(ref final_err) = last_connect_attempt_error {
      if !matches!(final_err, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error")) {
        tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %final_err, "Connecter: All attempts failed or stopped by fatal error. Publishing ConnectionAttemptFailed.");
        let _ = self.context.event_bus().publish(SystemEvent::ConnectionAttemptFailed {
          parent_core_id: self.parent_socket_id,
          target_endpoint_uri: endpoint_uri_clone.clone(),
          error_msg: final_err.to_string(),
        });
        if let Some(ref mon_tx_final) = monitor_tx {
          let _ = mon_tx_final.try_send(SocketEvent::ConnectFailed {
            endpoint: endpoint_uri_clone.clone(),
            error_msg: final_err.to_string(),
          });
        }
      }
    }
    // Else (if last_connect_attempt_error is None), NewConnectionEstablished was already published by setup_session_and_engine.

    self.context.publish_actor_stopping(
      connecter_handle,
      actor_type,
      Some(endpoint_uri_clone),
      last_connect_attempt_error,
    );
    tracing::info!(handle = connecter_handle, uri = %self.endpoint, "TCP Connecter actor (long-lived for retries) task fully stopped.");
  }

  /// Helper to perform a single TCP connection attempt, monitoring for shutdown.
  async fn try_connect_once(
    &self,
    target_socket_addr: &StdSocketAddr,
    endpoint_uri_original: &str, // Original user endpoint for error context
    system_event_rx: &mut broadcast::Receiver<SystemEvent>,
  ) -> Result<ConnectLogicOutputType, ZmqError> {
    let use_io_uring = self.context_options.io_uring.session_enabled && cfg!(feature = "io-uring");

    let connect_future = async {
      // Pinning happens implicitly with tokio::select!
      if use_io_uring {
        #[cfg(feature = "io-uring")]
        {
          // Uring path (using socket2 for pre-connect options, then tokio_uring)
          let domain = if target_socket_addr.is_ipv4() {
            socket2::Domain::IPV4
          } else {
            socket2::Domain::IPV6
          };
          let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None).map_err(ZmqError::from)?;
          apply_socket2_options_pre_connect(&socket, &self.config)?;

          // Blocking connect call in a spawn_blocking task
          let std_stream = tokio::task::spawn_blocking({
            let addr_clone = *target_socket_addr; // Clone for move
            move || {
              socket.connect(&addr_clone.into())?;
              socket.into_tcp_stream()
            }
          })
          .await
          .map_err(|je| ZmqError::Internal(format!("Blocking connect task join error: {}", je)))??;

          std_stream.set_nonblocking(true)?; // Ensure non-blocking for tokio_uring conversion

          // Convert std::net::TcpStream to OwnedFd for UringLaunchInformation
          let owned_fd: OwnedFd = std_stream.into(); // This consumes std_stream

          // Prepare for UringLaunchInformation
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
            EngineConnectionType::Uring { app_to_engine_cmd_tx }, // Placeholder, real tx comes from launch info
            Some(owned_fd),
            Box::new(app_to_engine_rx) as Box<dyn std::any::Any + Send>, // Box the receiver
            format!("tcp://{}", target_socket_addr),                     // Actual connected addr
          ))
        }
        #[cfg(not(feature = "io-uring"))]
        {
          unreachable!("io_uring path selected but feature not compiled");
        }
      } else {
        // Standard Tokio path
        let std_tokio_stream = underlying_std_net::TcpStream::connect(target_socket_addr)
          .await
          .map_err(|e| ZmqError::from_io_endpoint(e, endpoint_uri_original))?;
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

        // For standard engine, it will create its own mailbox. We pass a template.
        // The actual stream is passed in the Box<dyn Any>.
        let (dummy_engine_tx, _) = mailbox(1); // Dummy for template

        Ok((
          session_handle_id,
          engine_handle_id,
          EngineConnectionType::Standard {
            engine_mailbox: dummy_engine_tx,
          },
          None,                                                        // No OwnedFd for standard path
          Box::new(std_tokio_stream) as Box<dyn std::any::Any + Send>, // Box the stream
          format!("tcp://{}", peer_addr_actual),
        ))
      }
    };

    // Select on the connection attempt and system events
    tokio::select! {
      biased; // Prioritize checking for critical shutdown events.

      // This branch runs a loop to consume system events.
      // It only breaks out (thus selecting this branch of the outer select!)
      // if a CRITICAL event occurs or the event bus closes.
      _ = async {
        loop {
            match system_event_rx.recv().await {
                Ok(SystemEvent::ContextTerminating) => {
                    tracing::info!(handle = self.handle, uri = %self.endpoint, "Connect attempt aborted: ContextTerminating event received.");
                    break; // Break inner loop, this select arm is chosen
                }
                Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => {
                    tracing::info!(handle = self.handle, uri = %self.endpoint, "Connect attempt aborted: Parent SocketClosing event received.");
                    break; // Break inner loop, this select arm is chosen
                }
                Ok(event) => {
                    // Non-critical event, log it if necessary but continue allowing connect_future to poll.
                    tracing::trace!(handle = self.handle, uri = %self.endpoint, "Connecter received non-critical system event during connect attempt: {:?}", event);
                    continue; // Continue inner loop, effectively re-polling system_event_rx
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(handle = self.handle, uri = %self.endpoint, "Connect attempt aborted: Event bus lagged (skipped {} events).", n);
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!(handle = self.handle, uri = %self.endpoint, "Connect attempt aborted: Event bus closed.");
                    break; // Break inner loop, this select arm is chosen
                }
            }
        }
      } => {
        // If this select arm is chosen, it's due to a break from the inner loop (critical event or bus error).
        Err(ZmqError::Internal("Connect attempt aborted by critical system event or bus error.".into()))
      }

      // This branch polls the actual connection future.
      connect_logic_output_result = connect_future => {
          connect_logic_output_result // This is already Result<ConnectLogicOutputType, ZmqError>
      }
    }
  }

  /// Helper function to set up Session and Engine after a successful TCP connection.
  async fn setup_session_and_engine(
    &self,
    connect_output: ConnectLogicOutputType,
    monitor_tx: &Option<MonitorSender>,
    original_target_uri: &str, // Original user-provided endpoint URI
  ) -> Result<(), ZmqError> {
    let (
      session_handle_id,
      engine_handle_id,
      engine_conn_type_template, // This is a template; for Uring, actual app_tx is from launch_info
      opt_owned_fd,
      stream_or_app_rx_boxed,  // Box<dyn Any + Send> containing TcpStream or Receiver
      connection_specific_uri, // Actual peer URI, e.g., tcp://1.2.3.4:5678
    ) = connect_output;

    tracing::info!(
        connecter_handle = self.handle,
        session_id = session_handle_id,
        engine_id = engine_handle_id,
        target_uri = %original_target_uri,
        actual_peer_uri = %connection_specific_uri,
        "TCP connect successful, setting up session/engine."
    );

    if let Some(ref tx_mon) = monitor_tx {
      let event = SocketEvent::Connected {
        endpoint: original_target_uri.to_string(),
        peer_addr: connection_specific_uri.clone(),
      };
      let tx_clone = tx_mon.clone();
      // Spawn to avoid blocking setup if monitor channel is slow/full
      tokio::spawn(async move {
        let _ = tx_clone.send(event).await;
      });
    }

    // Create and spawn SessionBase
    let (session_cmd_tx, session_base_task_handle) = SessionBase::create_and_spawn(
      session_handle_id,
      connection_specific_uri.clone(), // Session identified by actual peer URI
      monitor_tx.clone(),
      self.context.clone(),
      self.parent_socket_id,
    );

    // Prepare final EngineConnectionType and spawn engine
    let final_engine_conn_type: EngineConnectionType;
    let mut final_engine_task_handle_for_attach: Option<JoinHandle<()>> = None;

    match engine_conn_type_template {
      #[cfg(feature = "io-uring")]
      EngineConnectionType::Uring {
        app_to_engine_cmd_tx: _template_tx,
      } => {
        // The _template_tx was just a placeholder. The real one comes from app_to_engine_rx_from_box.
        let app_to_engine_rx_from_box = stream_or_app_rx_boxed
          .downcast_arc_receiver() // Consumes Box<dyn Any>
          .ok_or_else(|| ZmqError::Internal("Logic error: Failed to downcast app_cmd_rx for uring".into()))?;

        // We need a new app_to_engine_cmd_tx for the UringLaunchInformation
        let (new_app_tx, new_app_rx_for_launch_info) =
          async_channel::bounded::<AppToUringEngineCmd>(self.context.inner().get_actor_mailbox_capacity());

        let launch_info = UringLaunchInformation {
          engine_handle_id,
          owned_fd: opt_owned_fd.expect("OwnedFd must be Some for Uring path"),
          config: ZmtpEngineConfig::from(&*self.context_options), // Create from options
          is_server: false,                                       // Connector is client role
          context_clone: self.context.clone(),
          session_base_mailbox: session_cmd_tx.clone(), // Session's mailbox
          app_to_engine_cmd_rx: new_app_rx_for_launch_info, // The RX end for the new channel
          parent_session_handle_id: session_handle_id,
        };

        // Submit launch to uring runtime
        match uring_runtime::submit_uring_engine_launch(launch_info) {
          Ok(true) => {
            final_engine_conn_type = EngineConnectionType::Uring {
              app_to_engine_cmd_tx: new_app_tx,
            };
            // No JoinHandle for uring engine task from here
          }
          Ok(false) => {
            return Err(ZmqError::Internal(
              "Uring runtime not available or submit failed".into(),
            ))
          }
          Err(e) => {
            return Err(ZmqError::Internal(format!(
              "Uring engine launch submit channel error: {:?}",
              e
            )))
          }
        }
      }
      EngineConnectionType::Standard { .. } => {
        let std_tokio_stream = stream_or_app_rx_boxed
          .downcast_arc_stream() // Consumes Box<dyn Any>
          .ok_or_else(|| ZmqError::Internal("Logic error: Failed to downcast TcpStream for std engine".into()))?;

        let (std_engine_mailbox, std_engine_task_handle) = create_and_spawn_tcp_engine(
          engine_handle_id,
          session_cmd_tx.clone(), // Session's mailbox
          std_tokio_stream,
          self.context_options.clone(),
          false, // Connector is client role
          &self.context,
          session_handle_id, // Parent of engine is session
        );
        final_engine_conn_type = EngineConnectionType::Standard {
          engine_mailbox: std_engine_mailbox,
        };
        final_engine_task_handle_for_attach = Some(std_engine_task_handle);
      }
      #[cfg(not(feature = "io-uring"))]
      _ => unreachable!("Invalid EngineConnectionType template when io-uring is not enabled"),
    }

    // Attach engine to session
    let attach_cmd = Command::Attach {
      connection: final_engine_conn_type,
      engine_handle: Some(engine_handle_id),
      engine_task_handle: final_engine_task_handle_for_attach,
    };

    if session_cmd_tx.send(attach_cmd).await.is_err() {
      session_base_task_handle.abort(); // Abort session if attach fails
                                        // If engine_task_handle_for_attach was Some, its associated engine will be stopped by session's drop/stop.
      return Err(ZmqError::Internal(
        "Failed to attach engine to session post-connect".into(),
      ));
    }

    // Publish NewConnectionEstablished event
    let event = SystemEvent::NewConnectionEstablished {
      parent_core_id: self.parent_socket_id,
      endpoint_uri: connection_specific_uri,                // Actual peer URI
      target_endpoint_uri: original_target_uri.to_string(), // User's target URI
      session_mailbox: session_cmd_tx,                      // Mailbox for the new session
      session_handle_id,
      session_task_id: session_base_task_handle.id(), // ID of the SessionBase task
    };
    if self.context.event_bus().publish(event).is_err() {
      session_base_task_handle.abort();
      return Err(ZmqError::Internal(
        "Failed to publish NewConnectionEstablished post-connect".into(),
      ));
    }

    Ok(())
  }

  /// Helper for retry delay, returns Ok(false) if shutdown signaled during delay.
  async fn wait_for_retry_delay_internal(
    &self,
    system_event_rx: &mut broadcast::Receiver<SystemEvent>,
    delay: Duration,
    monitor_tx: &Option<MonitorSender>,
    next_attempt_num: usize, // For logging ConnectRetried
  ) -> Result<bool, ()> {
    // Ok(true) = delay completed, Ok(false) = shutdown
    if delay.is_zero() {
      // No delay, but still check for immediate shutdown signals
      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) | Ok(SystemEvent::SocketClosing { .. }) => return Ok(false),
        _ => return Ok(true),
      }
    }

    tracing::debug!(handle = self.handle, uri = %self.endpoint, ?delay, "Connecter waiting before attempt #{}", next_attempt_num);
    if let Some(ref tx) = monitor_tx {
      let _ = tx.try_send(SocketEvent::ConnectRetried {
        // Use try_send for monitor
        endpoint: self.endpoint.clone(),
        interval: delay,
      });
    }

    tokio::select! {
        biased; // Prioritize shutdown events
        event_res = system_event_rx.recv() => {
            match event_res {
                Ok(SystemEvent::ContextTerminating) => {
                    tracing::info!(handle = self.handle, uri = %self.endpoint, "Connecter shutdown during retry delay by ContextTerminating.");
                    Ok(false) // Shutdown signaled
                }
                Ok(SystemEvent::SocketClosing { socket_id: s_id }) => {
                    if s_id == self.parent_socket_id {
                        tracing::info!(handle = self.handle, uri = %self.endpoint, "Connecter shutdown during retry delay by parent SocketClosing.");
                        Ok(false) // Shutdown signaled
                    } else {
                        // SocketClosing for a different socket, delay can continue if sleep hasn't finished.
                        // This select arm completed, so effectively the delay is over or needs to re-evaluate.
                        // We must ensure the original delay time is respected.
                        // This requires a more complex select loop or re-calculating remaining sleep.
                        // For simplicity now, let's assume if an event comes, delay is "done" for this tick.
                        Ok(true) // Effectively, an event occurred, so the "wait" for *this specific event* is over.
                                 // The outer loop will re-evaluate if it needs to sleep more or connect.
                                 // This could be refined to ensure full delay unless it's a *fatal* event.
                                 // Better: The select should be sleep OR fatal_event. Non-fatal events don't interrupt sleep.
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                     tracing::warn!(handle = self.handle, uri = %self.endpoint, "Connecter: Event bus closed during retry delay. Assuming shutdown.");
                     Ok(false)
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                     tracing::warn!(handle = self.handle, uri = %self.endpoint, "Connecter: Event bus lagged during retry delay. Assuming shutdown.");
                     Ok(false)
                }
                Ok(_) => Ok(true), // Other non-fatal event, delay effectively completed for this tick.
            }
        }
        _ = tokio::time::sleep(delay) => Ok(true), // Delay completed
    }
  }
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

pub(crate) fn is_fatal_connect_error(e: &ZmqError) -> bool {
  // <<< ADDED [Debug Log inside is_fatal_connect_error] >>>
  tracing::info!("[is_fatal_connect_error] Received error: {:?}", e);
  // <<< ADDED END >>>
  match e {
    ZmqError::IoError { kind, .. } => {
      let fatal_kind = matches!(
        kind,
        io::ErrorKind::AddrNotAvailable
          | io::ErrorKind::AddrInUse
          | io::ErrorKind::InvalidInput
          | io::ErrorKind::PermissionDenied
      );
      // <<< ADDED [Debug Log inside is_fatal_connect_error] >>>
      tracing::info!(
        "[is_fatal_connect_error] IoError kind: {:?}, fatal_kind: {}",
        kind,
        fatal_kind
      );
      // <<< ADDED END >>>
      fatal_kind
    }
    ZmqError::InvalidEndpoint(_) => {
      tracing::info!("[is_fatal_connect_error] Matched InvalidEndpoint (fatal)");
      true
    }
    ZmqError::UnsupportedTransport(_) => {
      tracing::info!("[is_fatal_connect_error] Matched UnsupportedTransport (fatal)");
      true
    }
    ZmqError::SecurityError(_) => {
      tracing::info!("[is_fatal_connect_error] Matched SecurityError (fatal)");
      true
    }
    ZmqError::AuthenticationFailure(_) => {
      tracing::info!("[is_fatal_connect_error] Matched AuthenticationFailure (fatal)");
      true
    }
    e_other => {
      tracing::info!(
        "[is_fatal_connect_error] Matched other error (non-fatal): {:?}",
        e_other
      );
      false
    }
  }
}

impl From<&SocketOptions> for ZmtpEngineConfig {
  fn from(options: &SocketOptions) -> Self {
    ZmtpEngineConfig {
      routing_id: options.routing_id.clone(),
      socket_type_name: options.socket_type_name.clone(),
      heartbeat_ivl: options.heartbeat_ivl,
      heartbeat_timeout: options.heartbeat_timeout,
      handshake_timeout: options.handshake_ivl,
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
      use_plain: options.plain_options.enabled,
      plain_username_for_engine: options.plain_options.username.clone(),
      plain_password_for_engine: options.plain_options.password.clone(),
    }
  }
}
