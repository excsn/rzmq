// src/transport/tcp.rs

use crate::engine::zmtp_tcp::create_and_spawn_tcp_engine;
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::{self, SessionBase};
use crate::socket::events::{MonitorSender, SocketEvent}; // Import monitor types
use crate::socket::options::{SocketOptions, TcpTransportConfig};
use socket2::{SockRef, TcpKeepalive};
use std::io; // For error kinds
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::sleep;

// --- TcpListener ---
#[derive(Debug)]
pub(crate) struct TcpListener {
  handle: usize,
  endpoint: String, // The original requested endpoint URI
  // local_addr: std::net::SocketAddr, // Store if needed, not strictly required by actor
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // Handle to the accept loop task
                                   // No need to store listener Arc, options Arc, or handle source Arc here
}

impl TcpListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let (tx, rx) = mailbox();

    // --- Bind the actual listener ---
    let bind_addr_str = endpoint
      .strip_prefix("tcp://")
      .ok_or_else(|| ZmqError::InvalidEndpoint(endpoint.clone()))?;
    let std_listener =
      std::net::TcpListener::bind(bind_addr_str).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
    std_listener.set_nonblocking(true).map_err(ZmqError::Io)?;
    SockRef::from(&std_listener)
      .set_reuse_address(true)
      .map_err(ZmqError::Io)?;

    let tokio_listener = TokioTcpListener::from_std(std_listener).map_err(ZmqError::Io)?;
    let local_addr = tokio_listener.local_addr().map_err(ZmqError::Io)?;
    tracing::info!(handle = handle, ?local_addr, uri = %endpoint, "TCP Listener bound successfully");
    let listener_arc = Arc::new(tokio_listener);
    // --- End Bind ---

    let config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    // Spawn the accept loop task separately
    let accept_core_mailbox = core_mailbox.clone();
    let accept_listener = listener_arc.clone();
    let accept_handle_source = context_handle_source.clone();
    let accept_options = options.clone();
    let accept_config = config.clone();
    let accept_monitor_tx = monitor_tx.clone();
    let endpoint_clone = endpoint.clone();

    let accept_loop_handle = tokio::spawn(TcpListener::run_accept_loop(
      handle,
      endpoint_clone, // Pass original endpoint URI
      accept_listener,
      accept_core_mailbox,
      accept_config,
      accept_options,
      accept_handle_source,
      accept_monitor_tx, // Pass monitor sender
    ));

    let actor = TcpListener {
      handle,
      endpoint, // Store original endpoint
      // local_addr, // Not needed after bind log
      core_mailbox,
      mailbox_receiver: rx,
      listener_handle: accept_loop_handle, // Store handle to accept loop
    };

    let command_loop_handle = tokio::spawn(actor.run_command_loop());
    Ok((tx, command_loop_handle)) // Return mailbox and handle for command loop
  }

  /// Main loop to handle commands (like Stop) for the Listener actor itself.
  async fn run_command_loop(mut self) {
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone();
    tracing::debug!(handle = handle, uri=%endpoint_uri, "Listener command loop started");

    loop {
      tokio::select! {
          biased;
          cmd = self.mailbox_receiver.recv() => {
            match cmd {
              Ok(Command::Stop) => {
                tracing::info!(handle = handle, uri=%endpoint_uri, "Listener received Stop command");
                self.listener_handle.abort(); // Stop accept loop first
                break; // Exit command loop
              },
              Ok(other) => tracing::warn!(handle=handle, uri=%endpoint_uri, "Listener received unhandled command: {:?}", other),
              Err(_) => {
                tracing::info!(handle=handle, uri=%endpoint_uri, "Listener command mailbox closed, stopping accept loop.");
                self.listener_handle.abort();
                break;
              }
            }
          }
      }
    }
    tracing::debug!(handle = handle, uri=%endpoint_uri, "Listener command loop finished");

    // Send CleanupComplete notification AFTER command loop exits
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle: self.handle,
        endpoint_uri: Some(self.endpoint.clone()),
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle=self.handle, uri=%self.endpoint, "Failed to send CleanupComplete to Core: {:?}", e);
      } else {
        tracing::debug!(handle=self.handle, uri=%self.endpoint, "Listener sent CleanupComplete to Core.");
      }
    }
    // No Drop impl needed here, socket closure handled by std/tokio
  }

  /// Separate task that runs the blocking `accept()` loop.
  async fn run_accept_loop(
    listener_handle_id: usize,
    endpoint_uri: String, // Original listener URI
    listener: Arc<TokioTcpListener>,
    core_mailbox: MailboxSender,
    config: TcpTransportConfig,
    options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) {
    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Listener accept loop started");
    let mut loop_error: Option<ZmqError> = None;

    loop {
      match listener.accept().await {
        Ok((stream, peer_addr)) => {
          let peer_addr_str = peer_addr.to_string();
          tracing::info!(parent_handle = listener_handle_id, %peer_addr, "Accepted new TCP connection");

          // Emit Accepted event
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::Accepted {
              endpoint: endpoint_uri.clone(), // Event relates to the listener endpoint
              peer_addr: peer_addr_str.clone(),
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              if tx_clone.send(event).await.is_err() {
                tracing::warn!(
                  socket_handle = listener_handle_id,
                  "Monitor channel closed while sending Accepted event"
                );
              }
            });
          }

          // Apply socket options
          if let Err(e) = apply_tcp_socket_options(&stream, &config) {
            tracing::error!(handle=listener_handle_id, peer=%peer_addr_str, error=%e, "Failed to apply socket options");
            // Emit AcceptFailed event
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::AcceptFailed {
                endpoint: endpoint_uri.clone(),
                error_msg: format!("Failed to apply socket options: {}", e), // Include reason
              };
              let tx_clone = tx.clone();
              tokio::spawn(async move {
                if tx_clone.send(event).await.is_err() { /* Warn */ }
              });
            }
            drop(stream); // Close the stream
            continue; // Skip this connection
          }

          // --- Spawn Session & Engine ---
          let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          // Session endpoint URI identifies the *specific* connection
          let conn_endpoint_uri = format!("tcp://{}", peer_addr_str);

          let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
            session_handle_id,
            conn_endpoint_uri.clone(),
            core_mailbox.clone(),
            monitor_tx.clone(), // Pass monitor sender to Session
          );
          let (engine_mailbox, engine_task_handle) = create_and_spawn_tcp_engine(
            engine_handle_id,
            session_cmd_mailbox.clone(),
            stream,
            options.clone(),
            true,
          );
          let attach_cmd = Command::Attach {
            engine_mailbox,
            engine_handle: Some(engine_handle_id),
            engine_task_handle: Some(engine_task_handle),
          };
          if session_cmd_mailbox.send(attach_cmd).await.is_err() {
            tracing::error!(session_handle=session_handle_id, uri=%conn_endpoint_uri, "Failed to send Attach command to session, dropping connection.");
            // TODO: Should we emit an AcceptFailed event here too?
            continue;
          }

          // Report ConnSuccess to Core
          let success_cmd = Command::ConnSuccess {
            endpoint: conn_endpoint_uri.clone(), // Resolved peer URI
            target_endpoint_uri: endpoint_uri.clone(), // Original target URI
            session_mailbox: session_cmd_mailbox,
            session_handle: Some(session_handle_id),
            session_task_handle: Some(session_task_handle),
          };
          if core_mailbox.send(success_cmd).await.is_err() {
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Failed to send ConnSuccess to core mailbox. Core actor likely stopped.");
            loop_error = Some(ZmqError::Internal("Core mailbox closed".into()));
            break;
          }
        }
        Err(e) => {
          tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Error accepting TCP connection");
          // Emit AcceptFailed event
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::AcceptFailed {
              endpoint: endpoint_uri.clone(),
              error_msg: format!("{}", e),
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              if tx_clone.send(event).await.is_err() { /* Warn */ }
            });
          }
          // Check for fatal errors
          if is_fatal(&e) {
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Fatal error in accept loop, stopping.");
            loop_error = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break;
          }
          tokio::time::sleep(Duration::from_millis(100)).await;
        }
      } // end match accept_result
    } // end loop

    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Listener accept loop finished");

    // Send ReportError if loop exited due to error
    if let Some(error) = loop_error {
      if !core_mailbox.is_closed() {
        let report_cmd = Command::ReportError {
          handle: listener_handle_id, // Report error against listener handle
          endpoint_uri: endpoint_uri.clone(),
          error,
        };
        if let Err(e) = core_mailbox.send(report_cmd).await { /* Warn */
        } else {
          tracing::debug!(handle=listener_handle_id, uri=%endpoint_uri, "Listener accept loop sent ReportError to Core.");
        }
      }
    }
  } // end run_accept_loop
} // end impl TcpListener

// --- TcpConnecter ---
#[derive(Debug)]
pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String, // Target endpoint URI
  core_mailbox: MailboxSender,
  config: TcpTransportConfig,
  context_options: Arc<SocketOptions>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  // No need to store monitor_tx here, passed directly to run_connect_loop
}

impl TcpConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) -> JoinHandle<()> {
    // Returns JoinHandle for the connect loop task
    let config = TcpTransportConfig {
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };
    let connecter = TcpConnecter {
      handle,
      endpoint,
      core_mailbox,
      config,
      context_options: options,
      context_handle_source,
    };
    // Spawn the main task that attempts connection, passing monitor_tx
    let task_handle = tokio::spawn(connecter.run_connect_loop(monitor_tx));
    task_handle
  }

  /// Main task loop that attempts to connect. Consumes self.
  async fn run_connect_loop(self, monitor_tx: Option<MonitorSender>) {
    // Accept monitor_tx
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone();
    let target_addr_str = match endpoint_uri.strip_prefix("tcp://") {
      Some(addr) => addr.to_string(),
      None => {
        tracing::error!(handle = handle, uri = %endpoint_uri, "Invalid TCP endpoint format");
        let error = ZmqError::InvalidEndpoint(endpoint_uri.clone());
        // Emit ConnectFailed immediately
        if let Some(ref tx) = monitor_tx {
          let event = SocketEvent::ConnectFailed {
            endpoint: endpoint_uri.clone(),
            error_msg: format!("{}", error),
          };
          let tx_clone = tx.clone();
          tokio::spawn(async move {
            if tx_clone.send(event).await.is_err() { /* Warn */ }
          });
        }
        // Report ConnFailed to Core
        let fail_cmd = Command::ConnFailed {
          endpoint: endpoint_uri.clone(),
          error,
        };
        let _ = self.core_mailbox.send(fail_cmd).await;
        // Send CleanupComplete because this task finished
        if !self.core_mailbox.is_closed() {
          let cleanup_cmd = Command::CleanupComplete {
            handle,
            endpoint_uri: Some(endpoint_uri),
          };
          if let Err(e) = self.core_mailbox.send(cleanup_cmd).await { /* Warn */ }
        }
        return;
      }
    };

    tracing::info!(handle = handle, uri = %endpoint_uri, "TcpConnecter actor started.");
    let mut current_delay = self.context_options.reconnect_ivl.unwrap_or(Duration::ZERO);
    let max_delay = self.context_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut connection_established = false; // Flag to track if ConnSuccess was sent

    loop {
      if attempt_count > 0 {
        if current_delay.is_zero() {
          tracing::info!(handle = handle, uri = %endpoint_uri, "Reconnect disabled (interval is 0), stopping connecter task.");
          // Final failure if connection wasn't established before
          if !connection_established {
            // Emit final ConnectFailed if needed (maybe last error was recoverable?)
            // We need the last error here. Let's assume loop only breaks on fatal or disabled reconnect.
            // The ConnectFailed event would have been sent inside the Err block below.
          }
          break;
        }
        tracing::debug!(handle = handle, uri = %endpoint_uri, delay = ?current_delay, "Waiting before reconnect attempt {}", attempt_count + 1);
        // Emit ConnectRetried event
        if let Some(ref tx) = monitor_tx {
          let event = SocketEvent::ConnectRetried {
            endpoint: endpoint_uri.clone(),
            interval: current_delay,
          };
          let tx_clone = tx.clone();
          tokio::spawn(async move {
            if tx_clone.send(event).await.is_err() { /* Warn */ }
          });
        }
        sleep(current_delay).await;
      }
      attempt_count += 1;

      tracing::debug!(handle = handle, uri = %endpoint_uri, "Attempting TCP connect #{}", attempt_count);
      let connect_result = TcpStream::connect(&target_addr_str).await;

      match connect_result {
        Ok(stream) => {
          let peer_addr_str = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
          tracing::info!(handle = handle, uri = %endpoint_uri, peer = %peer_addr_str, "TCP Connect successful (attempt {})", attempt_count);

          // Emit Connected event (Transport level)
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::Connected {
              endpoint: endpoint_uri.clone(),
              peer_addr: peer_addr_str.clone(),
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              if tx_clone.send(event).await.is_err() { /* Warn */ }
            });
          }

          // Apply socket options
          if let Err(e) = apply_tcp_socket_options(&stream, &self.config) {
            tracing::error!(handle = handle, uri = %endpoint_uri, peer = %peer_addr_str, error = %e, "Failed to apply socket options after connect, will retry connection.");
            // Emit ConnectFailed for this attempt? Or just retry? Let's retry.
            drop(stream);
            if let Some(max_ivl) = max_delay {
              if !max_ivl.is_zero() {
                current_delay = (current_delay * 2).min(max_ivl);
              }
            }
            continue; // Loop to retry connection
          }

          // --- Spawn Session & Engine ---
          let session_handle_id = self
            .context_handle_source
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = self
            .context_handle_source
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let conn_endpoint_uri = format!("tcp://{}", peer_addr_str);

          let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
            session_handle_id,
            conn_endpoint_uri.clone(),
            self.core_mailbox.clone(),
            monitor_tx.clone(), // Pass monitor sender to Session
          );
          let (engine_mailbox, engine_task_handle) = create_and_spawn_tcp_engine(
            engine_handle_id,
            session_cmd_mailbox.clone(),
            stream,
            self.context_options.clone(),
            false,
          );
          let attach_cmd = Command::Attach {
            engine_mailbox,
            engine_handle: Some(engine_handle_id),
            engine_task_handle: Some(engine_task_handle),
          };

          // --- Attach Engine & Report Success ---
          if session_cmd_mailbox.send(attach_cmd).await.is_err() {
            tracing::error!(handle = handle, uri = %endpoint_uri, session_handle = session_handle_id, "Failed to send Attach command to session. Reporting internal error.");
            let error = ZmqError::Internal("Failed to attach engine".into());
            // Emit ConnectFailed because session setup failed
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::ConnectFailed {
                endpoint: endpoint_uri.clone(),
                error_msg: format!("{}", error),
              };
              let tx_clone = tx.clone();
              tokio::spawn(async move {
                if tx_clone.send(event).await.is_err() { /* Warn */ }
              });
            }
            // Report ReportError to Core (so Core cleans up session state if it was added)
            let report_cmd = Command::ReportError {
              handle,
              endpoint_uri: conn_endpoint_uri.clone(),
              error,
            }; // Use conn_endpoint_uri
            let _ = self.core_mailbox.send(report_cmd).await;
            session_task_handle.abort();
            break; // Exit connecter loop after reporting error
          } else {
            // Report ConnSuccess to Core
            let success_cmd = Command::ConnSuccess {
              endpoint: conn_endpoint_uri.clone(), // Resolved peer URI
              target_endpoint_uri: endpoint_uri.clone(), // Original target URI
              session_mailbox: session_cmd_mailbox,
              session_handle: Some(session_handle_id),
              session_task_handle: Some(session_task_handle),
            };
            if self.core_mailbox.send(success_cmd).await.is_err() {
              tracing::error!(handle=handle, uri=%endpoint_uri, "Failed to send ConnSuccess to core mailbox.");

              // If sending fails, the session handle moved into success_cmd is dropped,
              // but the session task is still running. We can't easily abort it here anymore.
              // SocketCore won't know about the session, so it won't clean it up.
              // This might lead to an orphaned session task if the core died.
              // This is an acceptable limitation for now maybe? Or we could clone the handle *before* moving?
              // Let's keep it simple: if send fails, we can't abort the session from here.
            } else {
              connection_established = true; // Mark success
            }
            break; // Exit loop on success
          }
        } // End Ok(stream)
        Err(e) => {
          tracing::warn!(handle = handle, uri = %endpoint_uri, error = %e, "TCP Connect attempt #{} failed", attempt_count);
          // Emit ConnectDelayed event (on first failure)
          if attempt_count == 1 {
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::ConnectDelayed {
                endpoint: endpoint_uri.clone(),
                error_msg: format!("{}", e),
              };
              let tx_clone = tx.clone();
              tokio::spawn(async move {
                if tx_clone.send(event).await.is_err() { /* Warn */ }
              });
            }
          }
          // Calculate next delay
          if let Some(max_ivl) = max_delay {
            if !max_ivl.is_zero() {
              current_delay = (current_delay * 2).min(max_ivl);
            }
          }
          // Check if reconnect is disabled OR if error is fatal
          if current_delay.is_zero() || is_fatal_connect_error(&e) {
            tracing::error!(handle = handle, uri = %endpoint_uri, "Stopping connection attempts.");
            // Emit final ConnectFailed
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::ConnectFailed {
                endpoint: endpoint_uri.clone(),
                error_msg: format!("{}", e),
              };
              let tx_clone = tx.clone();
              tokio::spawn(async move {
                if tx_clone.send(event).await.is_err() { /* Warn */ }
              });
            }
            // Report ConnFailed to Core
            let fail_cmd = Command::ConnFailed {
              endpoint: endpoint_uri.clone(),
              error: ZmqError::from_io_endpoint(e, &endpoint_uri),
            };
            let _ = self.core_mailbox.send(fail_cmd).await;
            break; // Exit loop
          }
          // Otherwise, loop continues to wait and retry
        }
      } // end match connect_result
    } // end loop

    // --- Task Cleanup ---
    tracing::info!(handle = handle, uri = %endpoint_uri, "TcpConnecter actor finished.");
    // Send CleanupComplete - Connecter *always* sends this when its task ends naturally
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle,
        endpoint_uri: Some(endpoint_uri),
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle = handle, "Failed to send CleanupComplete: {:?}", e);
      } else {
        tracing::debug!(handle = handle, "TcpConnecter sent CleanupComplete to Core.");
      }
    }
  } // end run_connect_loop
} // end impl TcpConnecter

// --- Helper Functions ---
fn apply_tcp_socket_options(stream: &TcpStream, config: &TcpTransportConfig) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);
  socket_ref.set_nodelay(config.tcp_nodelay)?;
  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    #[cfg(all(
      feature = "all",
      any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_vendor = "apple",
        target_os = "windows"
      )
    ))]
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval);
    }
    #[cfg(all(
      feature = "all",
      any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "linux",
        target_os = "netbsd",
        target_vendor = "apple"
      )
    ))]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count);
    }
    socket_ref.set_tcp_keepalive(&keepalive)?;
    tracing::debug!("Applied TCP Keepalive settings: {:?}", keepalive);
  }
  Ok(())
}

/// Checks for fatal accept loop errors
fn is_fatal(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}

/// Checks for connect errors that should immediately stop retry attempts
fn is_fatal_connect_error(e: &io::Error) -> bool {
  matches!(
    e.kind(),
        io::ErrorKind::AddrNotAvailable |
        io::ErrorKind::AddrInUse | // If somehow trying to connect from an address already in use
        io::ErrorKind::InvalidInput | // E.g., invalid address format parsed earlier
        io::ErrorKind::PermissionDenied // Consider adding HostUnreachable, NetworkUnreachable?
                                        // io::ErrorKind::HostUnreachable |
                                        // io::ErrorKind::NetworkUnreachable
  )
}
