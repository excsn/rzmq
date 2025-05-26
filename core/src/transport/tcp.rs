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
    let listener_uses_io_uring = options.io_uring_session_enabled && cfg!(feature = "io-uring");
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
              let use_io_uring_for_session = conn_socket_options.io_uring_session_enabled && cfg!(feature = "io-uring");

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
  context_options: Arc<SocketOptions>, // For ZmtpEngineConfig and io_uring_session_enabled
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

  async fn run_connect_loop(self, monitor_tx: Option<MonitorSender>) {
    let connecter_handle = self.handle;
    let connecter_actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint.clone();
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "TCP Connecter actor started connection attempts.");

    let target_addr_str = match endpoint_uri_clone.strip_prefix("tcp://") {
      Some(addr) => addr.to_string(),
      None => {
        final_error_for_actor_stop = Some(ZmqError::InvalidEndpoint(endpoint_uri_clone.clone()));
        self.context.publish_actor_stopping(
          connecter_handle,
          connecter_actor_type,
          Some(endpoint_uri_clone),
          final_error_for_actor_stop,
        );
        tracing::error!(handle = connecter_handle, uri = %self.endpoint, "Invalid TCP endpoint format for connecter. Stopping.");
        return;
      }
    };

    let target_socket_addr: StdSocketAddr = match target_addr_str.parse() {
      Ok(addr) => addr,
      Err(e) => {
        final_error_for_actor_stop = Some(ZmqError::InvalidEndpoint(format!(
          "Failed to parse target address '{}': {}",
          target_addr_str, e
        )));
        self.context.publish_actor_stopping(
          connecter_handle,
          connecter_actor_type,
          Some(endpoint_uri_clone),
          final_error_for_actor_stop,
        );
        tracing::error!(handle = connecter_handle, uri = %self.endpoint, "Failed to parse target TCP address for connecter. Stopping.");
        return;
      }
    };

    let initial_reconnect_ivl_opt = self.context_options.reconnect_ivl;
    let mut current_retry_delay_base = initial_reconnect_ivl_opt.unwrap_or(Duration::from_millis(100));
    if initial_reconnect_ivl_opt == Some(Duration::ZERO) {
      current_retry_delay_base = Duration::ZERO;
    }
    let max_delay_opt = self.context_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut system_event_rx = self.context.event_bus().subscribe();

    'retry_loop: loop {
      if attempt_count > 0 {
        if initial_reconnect_ivl_opt == Some(Duration::ZERO) {
          tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Reconnect disabled (RECONNECT_IVL=0) and first attempt failed. Stopping.");
          break 'retry_loop;
        }
        let delay_for_this_retry = current_retry_delay_base;
        tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, delay = ?delay_for_this_retry, "Waiting before reconnect attempt #{}", attempt_count + 1);
        if let Some(ref tx) = monitor_tx {
          let event = SocketEvent::ConnectRetried {
            endpoint: endpoint_uri_clone.clone(),
            interval: delay_for_this_retry,
          };
          let tx_clone = tx.clone();
          tokio::spawn(async move {
            let _ = tx_clone.send(event).await;
          });
        }
        tokio::select! {
            biased;
            _ = sleep(delay_for_this_retry) => {},
            event_result = system_event_rx.recv() => {
                 match event_result {
                    Ok(SystemEvent::ContextTerminating) => {
                        final_error_for_actor_stop = Some(ZmqError::Internal("Connecter shutdown by ContextTerminating".into())); break 'retry_loop;
                    }
                    Ok(SystemEvent::SocketClosing{ socket_id: sid }) if sid == self.parent_socket_id => {
                        final_error_for_actor_stop = Some(ZmqError::Internal("Connecter shutdown by parent SocketClosing".into())); break 'retry_loop;
                    }
                    Err(_) => { final_error_for_actor_stop = Some(ZmqError::Internal("Connecter event bus error".into())); break 'retry_loop; }
                    _ => {}
                }
            }
        }
        if final_error_for_actor_stop.is_some() {
          break 'retry_loop;
        }
        if let Some(md) = max_delay_opt {
          if md > Duration::ZERO {
            current_retry_delay_base = (current_retry_delay_base * 2).min(md);
          }
        } else if current_retry_delay_base > Duration::ZERO {
          current_retry_delay_base *= 2;
        } else if initial_reconnect_ivl_opt.is_none() {
          current_retry_delay_base = Duration::from_millis(100) * 2;
        }
      }
      attempt_count += 1;

      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) => {
          final_error_for_actor_stop = Some(ZmqError::Internal("Connecter shutdown by event pre-attempt".into()));
          break 'retry_loop;
        }
        Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => {
          final_error_for_actor_stop = Some(ZmqError::Internal(
            "Connecter shutdown by parent event pre-attempt".into(),
          ));
          break 'retry_loop;
        }
        Err(broadcast::error::TryRecvError::Closed) => {
          final_error_for_actor_stop = Some(ZmqError::Internal("Connecter event bus closed pre-attempt".into()));
          break 'retry_loop;
        }
        _ => {}
      }
      if final_error_for_actor_stop.is_some() {
        break 'retry_loop;
      }

      tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, "Attempting TCP connect #{}", attempt_count);
      let use_io_uring = self.context_options.io_uring_session_enabled && cfg!(feature = "io-uring");

      let connect_attempt_result: Result<ConnectLogicOutputType, ZmqError> = tokio::select! {
          biased;
          event_result = system_event_rx.recv() => {
              match event_result {
                  Ok(SystemEvent::ContextTerminating) => Err(ZmqError::Internal("Connecter shutdown during connect by ContextTerminating".into())),
                  Ok(SystemEvent::SocketClosing{ socket_id: sid }) if sid == self.parent_socket_id => Err(ZmqError::Internal("Connecter shutdown during connect by parent SocketClosing".into())),
                  Err(_) => Err(ZmqError::Internal("Connecter event bus error during connect".into())),
                  _ => Err(ZmqError::Internal("Connect attempt preempted by non-critical event, retrying".into())),
              }
          }

          connect_logic_result = async {
              if use_io_uring {
                  #[cfg(feature = "io-uring")] {
                      let domain = if target_socket_addr.is_ipv4() { socket2::Domain::IPV4 } else { socket2::Domain::IPV6 };
                      let socket = socket2::Socket::new(domain, socket2::Type::STREAM, None)?;
                      apply_socket2_options_pre_connect(&socket, &self.config)?;
                      let std_stream = tokio::task::spawn_blocking(move || {
                          socket.connect(&target_socket_addr.into())?;
                          socket.into_tcp_stream()
                      }).await.map_err(|je| ZmqError::Internal(format!("Blocking connect task join error: {}", je)))??;
                      std_stream.set_nonblocking(true)?;
                      let owned_fd: OwnedFd = std_stream.into();
                      let (app_to_engine_tx, app_to_engine_rx) = async_channel::bounded(self.context.inner().get_actor_mailbox_capacity());
                      let session_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                      let engine_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                      Ok((
                          session_handle_id,
                          engine_handle_id,
                          EngineConnectionType::Uring { app_to_engine_cmd_tx },
                          Some(owned_fd),
                          Box::new(app_to_engine_rx) as Box<dyn std::any::Any + Send>,
                          format!("tcp://{}", target_addr_str)
                      ))
                  }
                  #[cfg(not(feature = "io-uring"))] { unreachable!(); }
              } else { // Standard path
                  let std_tokio_stream = underlying_std_net::TcpStream::connect(&target_addr_str).await
                      .map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri_clone))?;
                  apply_tcp_socket_options_to_tokio(&std_tokio_stream, &self.config)?;
                  let peer_addr_actual = std_tokio_stream.peer_addr()
                      .map_err(|e| ZmqError::Internal(format!("Failed to get peer_addr post-connect: {}",e)))?;

                  let session_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                  let engine_handle_id = self.context_handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                  // <<< MODIFIED [Correct placeholder type] >>>
                  let (dummy_engine_tx, _) = async_channel::bounded::<Command>(1);
                  Ok((
                      session_handle_id,
                      engine_handle_id,
                      EngineConnectionType::Standard { engine_mailbox: dummy_engine_tx }, // Correct type
                      None,
                      // <<< MODIFIED [Box the stream for DynamicDowncast] >>>
                      Box::new(std_tokio_stream) as Box<dyn std::any::Any + Send>,
                      format!("tcp://{}", peer_addr_actual)
                  ))
                  // <<< MODIFIED END >>>
              }
          } => connect_logic_result,
      };

      match connect_attempt_result {
        Ok((
          session_handle_id,
          engine_handle_id,
          engine_conn_type_template,
          opt_owned_fd,
          stream_or_app_rx_boxed,
          connection_specific_uri,
        )) => {
          // ... (Monitor event: Connected) ...
          if let Some(ref tx_mon) = monitor_tx {
            let event = SocketEvent::Connected {
              endpoint: endpoint_uri_clone.clone(),
              peer_addr: connection_specific_uri.clone(),
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
              let app_to_engine_cmd_rx = match stream_or_app_rx_boxed.downcast_arc_receiver() {
                // downcast_arc_receiver is on Box<dyn Any>
                Some(rx) => rx,
                None => {
                  final_error_for_actor_stop =
                    Some(ZmqError::Internal("Failed to downcast app_cmd_rx for uring".into()));
                  session_base_task_handle.abort();
                  break 'retry_loop;
                }
              };
              let launch_info = UringLaunchInformation {
                engine_handle_id,
                owned_fd: opt_owned_fd.expect("OwnedFd missing for uring path"),
                config: ZmtpEngineConfig::from(&*self.context_options),
                is_server: false,
                context_clone: self.context.clone(),
                session_base_mailbox: session_cmd_tx.clone(),
                app_to_engine_cmd_rx,
                parent_session_handle_id: session_handle_id,
              };
              match uring_runtime::submit_uring_engine_launch(launch_info) {
                Ok(true) => {
                  final_engine_conn_type = EngineConnectionType::Uring { app_to_engine_cmd_tx };
                }
                _ => {
                  final_error_for_actor_stop = Some(ZmqError::Internal("Uring launch submit fail in connecter".into()));
                  session_base_task_handle.abort();
                  break 'retry_loop;
                }
              }
            }
            EngineConnectionType::Standard { .. } => {
              // <<< MODIFIED [Call downcast_arc_stream on the Boxed value] >>>
              let std_tokio_stream = match stream_or_app_rx_boxed.downcast_arc_stream() {
                Some(s) => s,
                None => {
                  final_error_for_actor_stop =
                    Some(ZmqError::Internal("Failed to downcast TcpStream for std engine".into()));
                  session_base_task_handle.abort();
                  break 'retry_loop;
                }
              };
              // <<< MODIFIED END >>>
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
            _ => {
              unreachable!("Should be Standard if io-uring is not featured");
            }
          }

          let attach_cmd = Command::Attach {
            connection: final_engine_conn_type,
            engine_handle: Some(engine_handle_id),
            engine_task_handle: final_engine_task_handle,
          };

          if session_cmd_tx.send(attach_cmd).await.is_err() {
            final_error_for_actor_stop = Some(ZmqError::Internal(
              "Failed to attach engine to session (TCP Connecter)".into(),
            ));
            session_base_task_handle.abort();
          } else {
            publish_new_conn_event(
              &self.context,
              self.parent_socket_id,
              &connection_specific_uri,
              &endpoint_uri_clone,
              session_cmd_tx,
              session_handle_id,
            );
            final_error_for_actor_stop = None;
          }
          break 'retry_loop;
        }
        Err(e) => {
          final_error_for_actor_stop = Some(e);
          tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %final_error_for_actor_stop.as_ref().unwrap(), "TCP Connect attempt #{} logic failed or preempted", attempt_count);
          if attempt_count == 1
            && initial_reconnect_ivl_opt.map_or(true, |ivl| ivl != Duration::ZERO)
            && final_error_for_actor_stop
              .as_ref()
              .map_or(false, |err| !is_fatal_connect_error(err))
          {
            if let Some(ref tx_mon) = monitor_tx {
              let event = SocketEvent::ConnectDelayed {
                endpoint: endpoint_uri_clone.clone(),
                error_msg: format!("{}", final_error_for_actor_stop.as_ref().unwrap()),
              };
              let tx_clone = tx_mon.clone();
              tokio::spawn(async move {
                let _ = tx_clone.send(event).await;
              });
            }
          }
        }
      }

      if let Some(ref err) = final_error_for_actor_stop {
        if matches!(err, ZmqError::Internal(s) if s.contains("shutdown") || s.contains("preempted")) {
          break 'retry_loop;
        }
        if initial_reconnect_ivl_opt == Some(Duration::ZERO) {
          break 'retry_loop;
        }
        if is_fatal_connect_error(err) {
          break 'retry_loop;
        }
      }
    } // End of 'retry_loop

    // ... (final error publishing and actor stopping logic as before) ...
    if let Some(ref err) = final_error_for_actor_stop {
      if !matches!(err, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus closed") || s.contains("preempted"))
      {
        // Added preempted
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
          let _ = tx_mon.try_send(event_monitor);
        }
      }
    }
    self.context.publish_actor_stopping(
      connecter_handle,
      connecter_actor_type,
      Some(endpoint_uri_clone),
      final_error_for_actor_stop,
    );
    tracing::info!(handle = connecter_handle, uri = %self.endpoint, "TCP Connecter actor fully stopped.");
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

// ... (is_fatal_accept_error, is_fatal_connect_error, ZmtpEngineConfig From<&SocketOptions> remain) ...
fn is_fatal_accept_error(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

fn is_fatal_connect_error(e: &ZmqError) -> bool {
  match e {
    ZmqError::IoError { kind, .. } => {
      matches!(
        kind,
        io::ErrorKind::AddrNotAvailable |
        io::ErrorKind::AddrInUse |
        io::ErrorKind::InvalidInput |
        io::ErrorKind::PermissionDenied |
        io::ErrorKind::HostUnreachable | // Added based on common fatal connect errors
        io::ErrorKind::NetworkUnreachable // Added
      )
    }
    ZmqError::InvalidEndpoint(_) => true,
    ZmqError::UnsupportedTransport(_) => true,
    ZmqError::ConnectionRefused(_) => true, // Explicitly fatal
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
      use_send_zerocopy: options.io_uring_send_zerocopy,
      use_recv_multishot: options.io_uring_recv_multishot,
      use_cork: options.tcp_cork,
      #[cfg(feature = "io-uring")]
      recv_multishot_buffer_count: options.io_uring_recv_buffer_count,
      #[cfg(feature = "io-uring")]
      recv_multishot_buffer_capacity: options.io_uring_recv_buffer_size,
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
