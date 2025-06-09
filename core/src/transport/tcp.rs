use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{
  mailbox, system_events::ConnectionInteractionModel, ActorDropGuard, ActorType, Command,
  MailboxReceiver as GenericMailboxReceiver, MailboxSender as GenericMailboxSender, SystemEvent,
};
use crate::sessionx::actor::SessionConnectionActorX;
use crate::sessionx::states::ActorConfigX;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{SocketOptions, TcpTransportConfig, ZmtpEngineConfig};
use crate::socket::{ISocket, DEFAULT_RECONNECT_IVL_MS};

#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::{
  ProtocolConfig as WorkerProtocolConfig, UringOpCompletion as WorkerUringOpCompletion,
  UringOpRequest as WorkerUringOpRequest,
};
#[cfg(feature = "io-uring")]
use crate::socket::connection_iface::UringFdConnection;
#[cfg(feature = "io-uring")]
use crate::uring;
#[cfg(feature = "io-uring")]
use fibre::mpsc;
#[cfg(feature = "io-uring")]
use fibre::oneshot::oneshot;

#[cfg(feature = "io-uring")]
use std::os::unix::io::{AsRawFd, IntoRawFd};

use core::fmt;
use std::io;
use std::net::SocketAddr as StdSocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{SockRef, TcpKeepalive};
use tokio::sync::{broadcast, Semaphore};
use tokio::task::{Id as TaskId, JoinHandle};
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

mod underlying_std_net {
  pub use tokio::net::TcpListener;
  pub use tokio::net::TcpStream;
}

// --- TcpListener Actor ---
pub(crate) struct TcpListener {
  handle: usize,
  endpoint: String,
  mailbox_receiver: GenericMailboxReceiver,
  listener_handle: JoinHandle<()>,
  context: Context,
  parent_socket_id: usize,
  socket_logic: Arc<dyn ISocket>,
}

impl fmt::Debug for TcpListener {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("TcpListener")
      .field("handle", &self.handle)
      .field("endpoint", &self.endpoint)
      .field(
        "mailbox_receiver_is_closed",
        &self.mailbox_receiver.is_closed(),
      )
      .field(
        "listener_handle_is_finished",
        &self.listener_handle.is_finished(),
      )
      .field("context_present", &true)
      .field("parent_socket_id", &self.parent_socket_id)
      .field("socket_logic_present", &true)
      .finish()
  }
}

impl TcpListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> Result<(GenericMailboxSender, JoinHandle<()>, String), ZmqError> {
    let capacity = context.inner().get_actor_mailbox_capacity();
    let (tx, rx) = mailbox(capacity);

    let bind_addr_str_for_parse = endpoint
      .strip_prefix("tcp://")
      .ok_or_else(|| ZmqError::InvalidEndpoint(endpoint.clone()))?;

    let addr_for_socket2_parse =
      if bind_addr_str_for_parse == "0.0.0.0:0" || bind_addr_str_for_parse.starts_with("[::]:0") {
        if bind_addr_str_for_parse.starts_with("[::]") {
          "::1:0"
        } else {
          "127.0.0.1:0"
        }
      } else {
        bind_addr_str_for_parse
      };
    let parsed_socket_addr: StdSocketAddr = addr_for_socket2_parse.parse().map_err(|e| {
      ZmqError::InvalidEndpoint(format!(
        "Parse bind address '{}': {}",
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

    let addr_for_bind_call = bind_addr_str_for_parse
      .parse::<StdSocketAddr>()
      .map_err(|e| {
        ZmqError::InvalidEndpoint(format!(
          "Parse bind address for bind call'{}': {}",
          bind_addr_str_for_parse, e
        ))
      })?;
    s.bind(&addr_for_bind_call.into())
      .map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
    s.listen(options.backlog.unwrap_or(128) as i32)
      .map_err(ZmqError::from)?;
    let actual_bind_addr = s.local_addr().map_err(ZmqError::from)?.as_socket().unwrap();

    let std_listener_os: std::net::TcpListener = s.into();
    std_listener_os
      .set_nonblocking(true)
      .map_err(ZmqError::from)?;
    let tokio_listener =
      underlying_std_net::TcpListener::from_std(std_listener_os).map_err(ZmqError::from)?;

    let resolved_uri = format!("tcp://{}", actual_bind_addr);
    tracing::info!(listener_handle = handle, local_addr = %resolved_uri, user_uri = %endpoint, "TCP Listener bound");

    let max_conns = options.max_connections.unwrap_or(std::usize::MAX);
    let conn_limiter = Arc::new(Semaphore::new(max_conns.max(1)));

    let transport_cfg = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    let accept_loop_parent_hdl = handle;
    let accept_loop_hdl_id = context.inner().next_handle();

    let accept_loop_task_jh = tokio::spawn(TcpListener::run_accept_loop(
      accept_loop_hdl_id,
      accept_loop_parent_hdl,
      resolved_uri.clone(),
      Arc::new(tokio_listener),
      transport_cfg.clone(),
      options.clone(),
      socket_logic.clone(),
      context_handle_source.clone(),
      monitor_tx.clone(),
      context.clone(),
      parent_socket_id,
      conn_limiter.clone(),
    ));

    let listener_actor = TcpListener {
      handle,
      endpoint: resolved_uri.clone(),
      mailbox_receiver: rx,
      listener_handle: accept_loop_task_jh,
      context: context.clone(),
      parent_socket_id,
      socket_logic,
    };

    let cmd_loop_jh = tokio::spawn(listener_actor.run_command_loop(parent_socket_id));

    Ok((tx, cmd_loop_jh, resolved_uri))
  }

  async fn run_command_loop(mut self, parent_socket_id: usize) {
    let listener_cmd_loop_handle = self.handle;
    let endpoint_uri_clone_log = self.endpoint.clone();
    let event_bus = self.context.event_bus();
    let mut system_event_rx = event_bus.subscribe();
    tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop started");
    let mut final_error_for_actor_stopping: Option<ZmqError> = None;

    let mut actor_drop_guard = ActorDropGuard::new(
      self.context,
      listener_cmd_loop_handle,
      ActorType::Listener,
      Some(endpoint_uri_clone_log.clone()),
      Some(parent_socket_id),
    );

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
          final_error_for_actor_stopping = Some(ZmqError::Internal(format!(
            "Listener accept loop panicked: {:?}",
            e
          )));
        }
      } else {
        tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener accept loop task was cancelled as expected.");
      }
    } else {
      tracing::debug!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener accept loop task joined cleanly.");
    }

    if let Some(err) = final_error_for_actor_stopping.take() {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }

    tracing::info!(handle = listener_cmd_loop_handle, uri = %endpoint_uri_clone_log, "TCP Listener command loop actor fully stopped.");
  }

  async fn run_accept_loop(
    accept_loop_handle: usize,
    _listener_cmd_loop_handle: usize,
    endpoint_uri: String,
    listener: Arc<underlying_std_net::TcpListener>,
    transport_config: TcpTransportConfig,
    socket_options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_core_id: usize,
    connection_limiter: Arc<Semaphore>,
  ) {
    #[cfg(feature = "io-uring")]
    if socket_options.io_uring.session_enabled {
      uring::global_state::get_global_uring_worker_op_tx()
        .expect("URING HAS NOT BEEN INITIALIZED!");
    }

    let mut actor_drop_guard = ActorDropGuard::new(
      context.clone(),
      accept_loop_handle,
      ActorType::AcceptLoop,
      Some(endpoint_uri.clone()),
      Some(parent_socket_core_id),
    );
    tracing::debug!(handle = accept_loop_handle, uri = %endpoint_uri, "TCP Accept loop (unified) started.");
    let mut loop_error_to_report: Option<ZmqError> = None;

    loop {
      let permit = match connection_limiter.clone().acquire_owned().await {
        Ok(p) => p,
        Err(_) => {
          loop_error_to_report = Some(ZmqError::Internal("Connection limiter closed".into()));
          break;
        }
      };

      match listener.accept().await {
        Ok((tokio_tcp_stream, peer_addr)) => {
          let _permit_guard = permit;
          let peer_addr_str = peer_addr.to_string();
          tracing::info!(
            "Accepted TCP connection from {} (for listener {})",
            peer_addr_str,
            endpoint_uri
          );

          if let Some(ref tx) = monitor_tx {
            let _ = tx.try_send(SocketEvent::Accepted {
              endpoint: endpoint_uri.clone(),
              peer_addr: peer_addr_str.clone(),
            });
          }

          let use_io_uring_for_session =
            socket_options.io_uring.session_enabled && cfg!(feature = "io-uring");
          let connection_specific_uri = format!("tcp://{}", peer_addr_str);

          tokio::spawn({
            let context_clone = context.clone();
            let socket_options_clone = socket_options.clone();
            let transport_config_clone = transport_config.clone();
            let monitor_tx_clone = monitor_tx.clone();
            let endpoint_uri_listener = endpoint_uri.clone();
            let handle_source_clone = handle_source.clone();
            let actual_connected_uri = connection_specific_uri.clone();
            let logical_uri = endpoint_uri_listener.to_string(); // e.g., "tcp://127.0.0.1:5558"
            let socket_logic = socket_logic.clone();

            async move {
              let _permit_scoped_for_task = _permit_guard;

              let mut connection_iface_for_event: Option<Arc<dyn ISocketConnection>> = None;
              let mut interaction_model_for_event: Option<ConnectionInteractionModel> = None;
              let mut managing_actor_task_id_for_event: Option<TaskId> = None;
              let mut setup_successful = true;
              let mut sca_or_session_join_handle_opt: Option<JoinHandle<()>> = None;

              if use_io_uring_for_session {
                #[cfg(feature = "io-uring")]
                {
                  match tokio_tcp_stream.into_std() {
                    Ok(std_stream) => {
                      if socket_options_clone.tcp_cork {
                        tracing::debug!(
                          handle = accept_loop_handle,
                          fd = std_stream.as_raw_fd(),
                          "TcpListener: Applying TCP_CORK to accepted connection FD for IO URing."
                        );
                        let sock_ref = socket2::SockRef::from(&std_stream);
                        if let Err(e) = sock_ref.set_cork(true) {
                          tracing::error!(handle = accept_loop_handle, fd = std_stream.as_raw_fd(), error = %e, "TcpListener: Failed to set TCP_CORK for IO URing FD. Proceeding without.");
                          // Not making this fatal for the connection, it will proceed without CORK.
                        }
                      }

                      if let Err(e) =
                        apply_tcp_socket_options_to_std(&std_stream, &transport_config_clone)
                      {
                        tracing::error!(
                          "Opt apply failed (std stream) for {}: {}. Dropping.",
                          peer_addr_str,
                          e
                        );
                        setup_successful = false;
                      } else {

                        let raw_fd = std_stream.into_raw_fd();

                        let worker_op_tx =
                          uring::global_state::get_global_uring_worker_op_tx().unwrap();
                        let protocol_config = WorkerProtocolConfig::Zmtp(Arc::new(
                          ZmtpEngineConfig::from(&*socket_options_clone),
                        ));
                        let user_data_for_op = context_clone.inner().next_handle() as u64;
                        let (reply_tx_for_op, reply_rx_for_op) = oneshot();

                        let hwm = socket_options_clone.sndhwm.max(1); // TODO Needs bounded mpsc fibre to respect this
                        let (mpsc_tx_for_conn, mpsc_rx_for_worker) = mpsc::unbounded();

                        let new_conn_iface = Arc::new(UringFdConnection::new(
                          raw_fd,
                          mpsc_tx_for_conn.to_async(),
                          context_clone.clone(),
                        ));

                        let register_fd_req = WorkerUringOpRequest::RegisterExternalFd {
                          user_data: user_data_for_op,
                          fd: raw_fd,
                          protocol_handler_factory_id: "zmtp-uring/3.1".to_string(),
                          protocol_config,
                          is_server_role: true,
                          reply_tx: reply_tx_for_op,
                          mpsc_rx_for_worker: Arc::new(mpsc_rx_for_worker),
                        };

                        if let Err(e) = worker_op_tx.send(register_fd_req).await {
                          tracing::error!(
                            "Send RegisterExternalFd to UringWorker for fd {}: {}",
                            raw_fd,
                            e
                          );
                          unsafe {
                            let _ = libc::close(raw_fd);
                          }
                          setup_successful = false;
                        } else {
                          match reply_rx_for_op.recv().await {
                            Ok(Ok(WorkerUringOpCompletion::RegisterExternalFdSuccess {
                              fd: returned_fd,
                              ..
                            }))
                              if returned_fd == raw_fd =>
                            {
                              info!("Registered accepted FD {} with UringWorker.", raw_fd);
                              connection_iface_for_event = Some(new_conn_iface);
                              interaction_model_for_event =
                                Some(ConnectionInteractionModel::ViaUringFd { fd: raw_fd });
                              // managing_actor_task_id_for_event remains None for UringFd path
                            }
                            Ok(Ok(other_completion)) => {
                              tracing::error!(
                                "UringWorker bad success for RegisterExternalFd (fd {}): {:?}",
                                raw_fd,
                                other_completion
                              );
                              unsafe {
                                let _ = libc::close(raw_fd);
                              }
                              setup_successful = false;
                            }
                            Ok(Err(worker_err)) => {
                              tracing::error!(
                                "Register accepted FD {} with UringWorker failed (worker error): {:?}",
                                raw_fd,
                                worker_err
                              );
                              unsafe {
                                let _ = libc::close(raw_fd);
                              }
                              setup_successful = false;
                            }
                            Err(oneshot_recv_err) => {
                              tracing::error!(
                                "Register accepted FD {} with UringWorker failed (reply channel error): {:?}",
                                raw_fd,
                                oneshot_recv_err
                              );
                              unsafe {
                                let _ = libc::close(raw_fd);
                              }
                              setup_successful = false;
                            }
                          }
                        }
                      }
                    }
                    Err(e) => {
                      tracing::error!(
                        "tokio_tcp_stream to std failed for accepted conn: {}. Dropping.",
                        e
                      );
                      setup_successful = false;
                    }
                  }
                }
                #[cfg(not(feature = "io-uring"))]
                {
                  setup_successful = false;
                  unreachable!("io_uring feature not enabled but use_io_uring_for_session is true");
                }
              } else {
                // Standard path: SessionBase + ZmtpEngineCoreStd
                if let Err(e) =
                  apply_tcp_socket_options_to_tokio(&tokio_tcp_stream, &transport_config_clone)
                {
                  tracing::error!(
                    "Opt apply failed (tokio stream) for {}: {}. Dropping.",
                    peer_addr_str,
                    e
                  );
                  setup_successful = false;
                } else {
                  let sca_handle_id =
                    handle_source_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                  let actor_conf = ActorConfigX {
                    context: context_clone.clone(),
                    monitor_tx: monitor_tx_clone,
                    logical_target_endpoint_uri: logical_uri.clone(), // Was `logical_uri` in your original
                    connected_endpoint_uri: actual_connected_uri.clone(),
                    is_server_role: true,
                  };
                  let engine_conf = Arc::new(ZmtpEngineConfig::from(&*socket_options_clone));

                  let (command_sender_for_sca, command_receiver_for_sca) =
                    mailbox(context_clone.inner().get_actor_mailbox_capacity());

                  // tokio_tcp_stream is moved into SessionConnectionActorX
                  let sca_task_handle = SessionConnectionActorX::create_and_spawn(
                    sca_handle_id,
                    parent_socket_core_id,
                    tokio_tcp_stream,
                    actor_conf,
                    engine_conf,
                    command_receiver_for_sca,
                    socket_logic,
                  );

                  interaction_model_for_event = Some(ConnectionInteractionModel::ViaSca {
                    sca_mailbox: command_sender_for_sca,
                    sca_handle_id,
                  });
                  managing_actor_task_id_for_event = Some(sca_task_handle.id());
                  connection_iface_for_event = None; // SocketCore creates ScaConnectionIface
                }
              }

              if setup_successful {
                if let Some(inter_model) = interaction_model_for_event {
                  let event = SystemEvent::NewConnectionEstablished {
                    parent_core_id: parent_socket_core_id,
                    endpoint_uri: actual_connected_uri.clone(),
                    target_endpoint_uri: logical_uri.clone(),
                    connection_iface: connection_iface_for_event,
                    interaction_model: inter_model,
                    managing_actor_task_id: managing_actor_task_id_for_event,
                  };
                  if context_clone.event_bus().publish(event).is_err() {
                    tracing::error!(
                      "Failed to publish NewConnectionEstablished for {}",
                      actual_connected_uri
                    );
                    // If session was spawned, it needs to be aborted
                    if let Some(task_id) = managing_actor_task_id_for_event {
                      // This is tricky, we don't have the JoinHandle here directly if it was session path.
                      // The SessionBase will stop itself if its parent (SocketCore) doesn't attach pipes.
                      // For UringFd, the FD might need explicit close if `connection_iface_for_event` was set.
                      tracing::warn!("NewConnectionEstablished publish failed, related session/FD for task_id {:?} might need manual cleanup if not handled by its own lifecycle.", task_id);
                    }
                  }
                } else {
                  tracing::error!(
                    "Inconsistent state: setup_successful true but interaction model missing for {}",
                    actual_connected_uri
                  );
                }
              } else {
                tracing::warn!(
                  "Connection setup failed for {}, NewConnectionEstablished not published.",
                  actual_connected_uri
                );
              }
            }
          });
        }
        Err(e) => {
          drop(permit);
          tracing::error!(
            "Error accepting TCP connection (listener {}): {}",
            endpoint_uri,
            e
          );
          if let Some(ref tx) = monitor_tx {
            let _ = tx.try_send(SocketEvent::AcceptFailed {
              endpoint: endpoint_uri.clone(),
              error_msg: e.to_string(),
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
    if let Some(err) = loop_error_to_report.take() {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }
    tracing::info!("TCP Accept loop {} fully stopped.", accept_loop_handle);
  }
}

// --- TcpConnecter Actor ---
type UnifiedConnectOutcome = (
  Option<Arc<dyn ISocketConnection>>,
  ConnectionInteractionModel,
  Option<TaskId>, // managing_actor_task_id
  String,         // actual_peer_uri
);

pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String,
  config: TcpTransportConfig,
  socket_options: Arc<SocketOptions>,
  socket_logic: Arc<dyn ISocket>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  context: Context,
  parent_socket_id: usize,
}

impl fmt::Debug for TcpConnecter {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("TcpConnecter")
      .field("handle", &self.handle)
      .field("endpoint", &self.endpoint)
      .field("config", &self.config)
      .field("context_options", &self.socket_options)
      .field("socket_logic_present", &true)
      .field("context_present", &true)
      .field("parent_socket_id", &self.parent_socket_id)
      .finish()
  }
}

impl TcpConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    socket_logic: Arc<dyn ISocket>,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>,
    context: Context,
    parent_socket_id: usize,
  ) -> JoinHandle<()> {
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
      socket_options: options,
      socket_logic,
      context_handle_source,
      context: context.clone(),
      parent_socket_id,
    };
    let task_join_handle =
      tokio::spawn(connecter_actor.run_connect_loop(monitor_tx, parent_socket_id));

    task_join_handle
  }

  async fn run_connect_loop(self, monitor_tx: Option<MonitorSender>, parent_socket_id: usize) {
    let connecter_handle = self.handle;
    let actor_type = ActorType::Connecter;
    let endpoint_uri_clone = self.endpoint.clone();
    let mut actor_drop_guard = ActorDropGuard::new(
      self.context.clone(),
      connecter_handle,
      actor_type,
      Some(endpoint_uri_clone.clone()),
      Some(parent_socket_id),
    );

    tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Long-Lived TCP Connecter actor started.");

    let target_socket_addr: StdSocketAddr = match endpoint_uri_clone.strip_prefix("tcp://") {
      Some(addr_str) => match addr_str.parse() {
        Ok(addr) => addr,
        Err(e) => {
          let err =
            ZmqError::InvalidEndpoint(format!("Parse target address '{}': {}", addr_str, e));
          let _ = self
            .context
            .event_bus()
            .publish(SystemEvent::ConnectionAttemptFailed {
              parent_core_id: self.parent_socket_id,
              target_endpoint_uri: self.endpoint.clone(),
              error_msg: err.to_string(),
            });
          actor_drop_guard.set_error(err);
          return;
        }
      },
      None => {
        let err = ZmqError::InvalidEndpoint(endpoint_uri_clone.clone());
        let _ = self
          .context
          .event_bus()
          .publish(SystemEvent::ConnectionAttemptFailed {
            parent_core_id: self.parent_socket_id,
            target_endpoint_uri: self.endpoint.clone(),
            error_msg: err.to_string(),
          });
        actor_drop_guard.set_error(err);
        return;
      }
    };

    let initial_reconnect_ivl = self
      .socket_options
      .reconnect_ivl
      .unwrap_or(Duration::from_millis(DEFAULT_RECONNECT_IVL_MS));
    let no_retry_after_first_connect_fail = self.socket_options.reconnect_ivl
      == Some(Duration::ZERO)
      && (self.socket_options.reconnect_ivl_max == Some(Duration::ZERO)
        || self.socket_options.reconnect_ivl_max.is_none());
    let mut current_retry_delay = if self.socket_options.reconnect_ivl == Some(Duration::ZERO)
      && self
        .socket_options
        .reconnect_ivl_max
        .map_or(false, |d| d > Duration::ZERO)
    {
      self.socket_options.reconnect_ivl_max.unwrap()
    } else {
      initial_reconnect_ivl
    };
    let reconnect_ivl_max_opt = self.socket_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut system_event_rx = self.context.event_bus().subscribe();
    let mut last_connect_attempt_error: Option<ZmqError> = None;

    'connecter_life_loop: loop {
      if attempt_count > 0 {
        if no_retry_after_first_connect_fail {
          tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: Retries disabled. Stopping after first failure.");
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
          Ok(true) => {}
          Ok(false) => {
            last_connect_attempt_error = Some(ZmqError::Internal(
              "Connecter shutdown by event retry delay.".into(),
            ));
            break 'connecter_life_loop;
          }
          Err(()) => {
            last_connect_attempt_error = Some(ZmqError::Internal(
              "Connecter shutdown error retry delay.".into(),
            ));
            break 'connecter_life_loop;
          }
        }
        if current_retry_delay > Duration::ZERO
          && reconnect_ivl_max_opt.map_or(true, |max_d| max_d > Duration::ZERO)
        {
          if let Some(max_d) = reconnect_ivl_max_opt {
            current_retry_delay = (current_retry_delay * 2).min(max_d);
          } else {
            current_retry_delay = initial_reconnect_ivl;
          }
        } else if reconnect_ivl_max_opt.map_or(false, |max_d| max_d > Duration::ZERO) {
          current_retry_delay = reconnect_ivl_max_opt.unwrap();
        }
      }
      attempt_count += 1;

      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) => {
          last_connect_attempt_error = Some(ZmqError::Internal(
            "Shutdown by ContextTerminating (pre-connect).".into(),
          ));
          break 'connecter_life_loop;
        }
        Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => {
          last_connect_attempt_error = Some(ZmqError::Internal(
            "Shutdown by parent SocketClosing (pre-connect).".into(),
          ));
          break 'connecter_life_loop;
        }
        Err(broadcast::error::TryRecvError::Closed) => {
          last_connect_attempt_error =
            Some(ZmqError::Internal("Event bus closed (pre-connect).".into()));
          break 'connecter_life_loop;
        }
        Err(broadcast::error::TryRecvError::Lagged(_)) => {
          last_connect_attempt_error =
            Some(ZmqError::Internal("Event bus lagged (pre-connect).".into()));
          break 'connecter_life_loop;
        }
        _ => {}
      }

      tracing::debug!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: TCP connect attempt #{}", attempt_count);

      let single_attempt_outcome: Result<UnifiedConnectOutcome, ZmqError> = self
        .try_connect_once(
          &target_socket_addr,
          &endpoint_uri_clone,
          &mut system_event_rx,
          &monitor_tx,
        )
        .await;

      match single_attempt_outcome {
        Ok((connection_iface_opt, interaction_model, managing_actor_task_id, actual_peer_uri)) => {
          tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, actual_peer = %actual_peer_uri, "Connecter: TCP connect successful.");
          let event = SystemEvent::NewConnectionEstablished {
            parent_core_id: self.parent_socket_id,
            endpoint_uri: actual_peer_uri,
            target_endpoint_uri: endpoint_uri_clone.clone(),
            connection_iface: connection_iface_opt, // Pass the Option
            interaction_model,
            managing_actor_task_id,
          };
          if self.context.event_bus().publish(event).is_err() {
            tracing::error!(
              "Connecter: Failed to publish NewConnectionEstablished for {}.",
              endpoint_uri_clone
            );
            last_connect_attempt_error = Some(ZmqError::Internal(
              "Failed to publish NewConnectionEstablished".into(),
            ));
          } else {
            last_connect_attempt_error = None;
          }
          break 'connecter_life_loop;
        }
        Err(attempt_failure_error) => {
          last_connect_attempt_error = Some(attempt_failure_error.clone());
          tracing::warn!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_failure_error, "Connecter: TCP connect attempt #{} failed.", attempt_count);
          if is_fatal_connect_error(&attempt_failure_error)
            || matches!(&attempt_failure_error, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error"))
          {
            tracing::error!(handle = connecter_handle, uri = %endpoint_uri_clone, error = %attempt_failure_error, "Connecter: Fatal error. Stopping.");
            break 'connecter_life_loop;
          }
          if no_retry_after_first_connect_fail && attempt_count == 1 {
            tracing::info!(handle = connecter_handle, uri = %endpoint_uri_clone, "Connecter: No retry, stopping after first fail.");
            break 'connecter_life_loop;
          }
          if attempt_count == 1 {
            if let Some(ref mon_tx) = monitor_tx {
              let _ = mon_tx.try_send(SocketEvent::ConnectDelayed {
                endpoint: endpoint_uri_clone.clone(),
                error_msg: attempt_failure_error.to_string(),
              });
            }
          }
        }
      }
    }

    if let Some(ref final_err) = last_connect_attempt_error {
      if !matches!(final_err, ZmqError::Internal(s) if s.contains("shutdown by") || s.contains("event bus error"))
      {
        let _ = self
          .context
          .event_bus()
          .publish(SystemEvent::ConnectionAttemptFailed {
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

    if let Some(err) = last_connect_attempt_error.take() {
      actor_drop_guard.set_error(err);
    } else {
      actor_drop_guard.waive();
    }
    tracing::info!(handle = connecter_handle, uri = %self.endpoint, "TCP Connecter actor task fully stopped.");
  }

  async fn try_connect_once(
    &self,
    target_socket_addr: &StdSocketAddr,
    endpoint_uri_original: &str,
    system_event_rx: &mut broadcast::Receiver<SystemEvent>,
    monitor_tx: &Option<MonitorSender>,
  ) -> Result<UnifiedConnectOutcome, ZmqError> {
    let use_io_uring = self.socket_options.io_uring.session_enabled && cfg!(feature = "io-uring");

    let connect_future = async {
      if use_io_uring {
        #[cfg(feature = "io-uring")]
        {
          let domain = if target_socket_addr.is_ipv4() {
            socket2::Domain::IPV4
          } else {
            socket2::Domain::IPV6
          };
          let socket =
            socket2::Socket::new(domain, socket2::Type::STREAM, None).map_err(ZmqError::from)?;
          apply_socket2_options_pre_connect(&socket, &self.config)?;

          // Apply TCP_CORK before connect if enabled in context_options
          if self.socket_options.tcp_cork {
            tracing::debug!(
              handle = self.handle,
              "TcpConnecter: Applying TCP_CORK to outgoing connection FD before connect for IO URing."
            );
            // Apply to socket2::Socket before connect()
            if let Err(e) = socket.set_cork(true) {
              // socket2::Socket has set_cork
              tracing::error!(handle = self.handle, error = %e, "TcpConnecter: Failed to set TCP_CORK (socket2) for IO URing FD. Proceeding without.");
              // Optionally, make this fatal:
              // return Err(ZmqError::IoError { kind: e.kind(), message: format!("Failed to set TCP_CORK: {}", e) });
            }
          }

          let std_stream: std::net::TcpStream = tokio::task::spawn_blocking({
            let addr_clone = *target_socket_addr;
            move || {
              let _ = socket.connect(&addr_clone.into());
              socket.into()
            }
          })
          .await
          .map_err(|je| ZmqError::Internal(format!("Blocking connect join error: {}", je)))?;

          let peer_addr_actual = std_stream.peer_addr().map_err(ZmqError::from)?;

          if let Some(ref mon_tx) = monitor_tx {
            let _ = mon_tx.try_send(SocketEvent::Connected {
              endpoint: endpoint_uri_original.to_string(),
              peer_addr: format!("tcp://{}", peer_addr_actual),
            });
          }
          apply_tcp_socket_options_to_std(&std_stream, &self.config)?;
          let raw_fd = std_stream.into_raw_fd();

          let worker_op_tx = uring::global_state::get_global_uring_worker_op_tx()?;
          let protocol_config =
            WorkerProtocolConfig::Zmtp(Arc::new(ZmtpEngineConfig::from(&*self.socket_options)));
          let user_data_for_op = self.context.inner().next_handle() as u64;
          let (reply_tx_for_op, reply_rx_for_op) = oneshot();


          let hwm = self.socket_options.sndhwm.max(1); // TODO Needs bounded mpsc fibre to respect this
          let (mpsc_tx_for_conn, mpsc_rx_for_worker) = mpsc::unbounded();
          let new_conn_iface = Arc::new(UringFdConnection::new(
              raw_fd,
              mpsc_tx_for_conn.to_async(),
              self.context.clone(),
          ));
          let register_fd_req = WorkerUringOpRequest::RegisterExternalFd {
            user_data: user_data_for_op,
            fd: raw_fd,
            protocol_handler_factory_id: "zmtp-uring/3.1".to_string(),
            protocol_config,
            is_server_role: false,
            reply_tx: reply_tx_for_op,
            mpsc_rx_for_worker: Arc::new(mpsc_rx_for_worker),
          };

          worker_op_tx.send(register_fd_req).await.map_err(|e| {
            ZmqError::Internal(format!("Send RegisterExternalFd to UringWorker: {}", e))
          })?;

          match reply_rx_for_op.recv().await {
            Ok(Ok(WorkerUringOpCompletion::RegisterExternalFdSuccess {
              fd: returned_fd, ..
            }))
              if returned_fd == raw_fd =>
            {
              info!(
                "Successfully registered connected FD {} with UringWorker.",
                raw_fd
              );

              let connection_iface: Option<Arc<dyn ISocketConnection>> = Some(new_conn_iface);
              let interaction_model = ConnectionInteractionModel::ViaUringFd { fd: raw_fd };
              Ok((
                connection_iface,
                interaction_model,
                None,
                format!("tcp://{}", peer_addr_actual),
              ))
            }
            Ok(Ok(other_completion)) => {
              tracing::error!(
                "UringWorker bad success for RegisterExternalFd (fd {}): {:?}",
                raw_fd,
                other_completion
              );
              unsafe {
                let _ = libc::close(raw_fd);
              }
              Err(ZmqError::Internal(format!(
                "UringWorker unexpected success: {:?}",
                other_completion
              )))
            }
            Ok(Err(worker_err)) => {
              tracing::error!(
                "Register FD {} with UringWorker failed (worker error): {:?}",
                raw_fd,
                worker_err
              );
              unsafe {
                let _ = libc::close(raw_fd);
              }
              Err(worker_err)
            }
            Err(oneshot_recv_err) => {
              tracing::error!(
                "Register FD {} with UringWorker failed (reply channel error): {:?}",
                raw_fd,
                oneshot_recv_err
              );
              unsafe {
                let _ = libc::close(raw_fd);
              }
              Err(ZmqError::Internal(format!(
                "Register FD with UringWorker reply error: {:?}",
                oneshot_recv_err
              )))
            }
          }
        }
        #[cfg(not(feature = "io-uring"))]
        {
          unreachable!();
        }
      } else {
        // Standard path for SessionBase
        let std_tokio_stream = underlying_std_net::TcpStream::connect(target_socket_addr)
          .await
          .map_err(|e| ZmqError::from_io_endpoint(e, endpoint_uri_original))?;

        let peer_addr_actual = std_tokio_stream.peer_addr().map_err(ZmqError::from)?;

        if let Some(ref mon_tx) = monitor_tx {
          let _ = mon_tx.try_send(SocketEvent::Connected {
            endpoint: endpoint_uri_original.to_string(),
            peer_addr: format!("tcp://{}", peer_addr_actual),
          });
        }

        apply_tcp_socket_options_to_tokio(&std_tokio_stream, &self.config)?;

        let actual_connected_uri = format!("tcp://{}", peer_addr_actual);

        let sca_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let actor_conf = ActorConfigX {
          context: self.context.clone(),
          monitor_tx: monitor_tx.clone(), // Pass the Connecter's monitor_tx
          logical_target_endpoint_uri: endpoint_uri_original.to_string(),
          connected_endpoint_uri: actual_connected_uri.clone(),
          is_server_role: false, // Outgoing connection is client role
        };
        let engine_conf = Arc::new(ZmtpEngineConfig::from(&*self.socket_options));

        // Create a standard Command mailbox for the SCA
        let (command_sender_for_sca, command_receiver_for_sca) =
          mailbox(self.context.inner().get_actor_mailbox_capacity());

        // tokio_tcp_stream is moved into SessionConnectionActorX here
        let sca_task_handle = SessionConnectionActorX::create_and_spawn(
          sca_handle_id,
          self.parent_socket_id,
          std_tokio_stream,
          actor_conf,
          engine_conf,
          command_receiver_for_sca,
          self.socket_logic.clone(),
        );

        // Populate variables for the common return structure
        let interaction_model = ConnectionInteractionModel::ViaSca {
          sca_mailbox: command_sender_for_sca, // SocketCore uses this to send Command::ScaInitializePipes
          sca_handle_id,
        };
        let connection_iface = None;

        Ok((
          connection_iface,
          interaction_model,
          Some(sca_task_handle.id()),
          format!("tcp://{}", peer_addr_actual),
        ))
      }
    };

    tokio::select! {
      biased;
      _ = async {
        loop {
          match system_event_rx.recv().await {
            Ok(SystemEvent::ContextTerminating) => break,
            Ok(SystemEvent::SocketClosing { socket_id: sid }) if sid == self.parent_socket_id => break,
            Ok(_) => continue,
            Err(_) => break,
          }
        }
      } => {
        Err(ZmqError::Internal("Connect aborted by system event.".into()))
      }
      connect_outcome_result = connect_future => {
        connect_outcome_result
      }
    }
  }

  // ... (wait_for_retry_delay_internal method remains the same)
  async fn wait_for_retry_delay_internal(
    &self,
    system_event_rx: &mut broadcast::Receiver<SystemEvent>,
    delay: Duration,
    monitor_tx: &Option<MonitorSender>,
    next_attempt_num: usize,
  ) -> Result<bool, ()> {
    if delay.is_zero() {
      match system_event_rx.try_recv() {
        Ok(SystemEvent::ContextTerminating) | Ok(SystemEvent::SocketClosing { .. }) => {
          return Ok(false)
        }
        _ => return Ok(true),
      }
    }
    tracing::debug!(handle = self.handle, uri = %self.endpoint, ?delay, "Connecter waiting before attempt #{}", next_attempt_num);
    if let Some(ref tx) = monitor_tx {
      let _ = tx.try_send(SocketEvent::ConnectRetried {
        endpoint: self.endpoint.clone(),
        interval: delay,
      });
    }
    tokio::select! {
      biased;
      event_res = system_event_rx.recv() => {
        match event_res {
          Ok(SystemEvent::ContextTerminating) => Ok(false),
          Ok(SystemEvent::SocketClosing { socket_id: s_id }) if s_id == self.parent_socket_id => Ok(false),
          Err(_) => Ok(false),
          Ok(_) => Ok(true),
        }
      }
      _ = tokio::time::sleep(delay) => Ok(true),
    }
  }
}

// --- Helper Functions ---
// ... (is_fatal_accept_error, is_fatal_connect_error, apply_socket2_options_pre_connect, apply_tcp_socket_options_to_tokio, apply_tcp_socket_options_to_std, ZmtpEngineConfig::from remain the same)
pub(crate) fn is_fatal_accept_error(e: &io::Error) -> bool {
  matches!(
    e.kind(),
    io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe
  )
}

pub(crate) fn is_fatal_connect_error(e: &ZmqError) -> bool {
  match e {
    ZmqError::IoError { kind, .. } => {
      matches!(
        kind,
        io::ErrorKind::AddrNotAvailable
          | io::ErrorKind::AddrInUse
          | io::ErrorKind::InvalidInput
          | io::ErrorKind::PermissionDenied
          | io::ErrorKind::ConnectionRefused
      )
    }
    ZmqError::InvalidEndpoint(_)
    | ZmqError::UnsupportedTransport(_)
    | ZmqError::SecurityError(_)
    | ZmqError::AuthenticationFailure(_) => true,
    _ => false,
  }
}

fn apply_socket2_options_pre_connect(
  socket: &socket2::Socket,
  config: &TcpTransportConfig,
) -> Result<(), ZmqError> {
  socket
    .set_nodelay(config.tcp_nodelay)
    .map_err(ZmqError::from)?;
  if config.keepalive_time.is_some()
    || config.keepalive_interval.is_some()
    || config.keepalive_count.is_some()
  {
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
    socket
      .set_tcp_keepalive(&keepalive)
      .map_err(ZmqError::from)?;
  }
  Ok(())
}

fn apply_tcp_socket_options_to_tokio(
  stream: &tokio::net::TcpStream,
  config: &TcpTransportConfig,
) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  if config.keepalive_time.is_some()
    || config.keepalive_interval.is_some()
    || config.keepalive_count.is_some()
  {
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

fn apply_tcp_socket_options_to_std(
  stream: &std::net::TcpStream,
  config: &TcpTransportConfig,
) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  if config.keepalive_time.is_some()
    || config.keepalive_interval.is_some()
    || config.keepalive_count.is_some()
  {
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
