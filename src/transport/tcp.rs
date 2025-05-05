// src/transport/tcp.rs

use crate::engine::zmtp_tcp::create_and_spawn_tcp_engine;
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::{self, SessionBase}; // Import placeholder Session
use crate::socket::options::{SocketOptions, TcpTransportConfig, ZmtpEngineConfig};
use crate::Msg; // Import config struct
use socket2::{SockRef, TcpKeepalive};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep; // For socket options

pub(crate) struct TcpListener {
  handle: usize,
  endpoint: String,                 // The original requested endpoint URI
  local_addr: std::net::SocketAddr, // The actual bound address
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // Handle to the accept loop task
  listener: Arc<TokioTcpListener>, // Listener needs to be shared for accept loop
  config: TcpTransportConfig,
  context_options: Arc<SocketOptions>, // Store full options to pass to Engine factory
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // Source for unique IDs
}

impl TcpListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String, // e.g., "tcp://*:5555" or "tcp://127.0.0.1:5555"
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // Pass down handle source
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let (tx, rx) = mailbox();

    // --- Bind the actual listener ---
    // TODO: Extract address from endpoint string properly
    let bind_addr_str = endpoint.strip_prefix("tcp://").unwrap_or(&endpoint); // Simplistic parsing
    let std_listener =
      std::net::TcpListener::bind(bind_addr_str).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
    std_listener.set_nonblocking(true).map_err(|e| ZmqError::Io(e))?; // Essential for Tokio
                                                                      // Apply SO_REUSEADDR? Often needed for servers.
    SockRef::from(&std_listener)
      .set_reuse_address(true)
      .map_err(|e| ZmqError::Io(e))?;

    let tokio_listener = TokioTcpListener::from_std(std_listener).map_err(|e| ZmqError::Io(e))?;
    let local_addr = tokio_listener.local_addr().map_err(|e| ZmqError::Io(e))?;
    tracing::info!(handle = handle, ?local_addr, "TCP Listener bound successfully");
    let listener_arc = Arc::new(tokio_listener);
    // --- End Bind ---

    let config = TcpTransportConfig {
      // Create transport config
      tcp_nodelay: options.tcp_nodelay,
      keepalive_time: options.tcp_keepalive_idle,
      keepalive_interval: options.tcp_keepalive_interval,
      keepalive_count: options.tcp_keepalive_count,
    };

    // Spawn the accept loop task separately
    let accept_core_mailbox = core_mailbox.clone();
    let accept_listener = listener_arc.clone();
    let accept_handle_source = context_handle_source.clone();
    let accept_loop_handle = tokio::spawn(TcpListener::run_accept_loop(
      handle,
      accept_listener,
      accept_core_mailbox,
      config.clone(),
      options.clone(),
      accept_handle_source,
    ));

    let actor = TcpListener {
      handle,
      endpoint,
      local_addr,
      core_mailbox,
      mailbox_receiver: rx,
      listener_handle: accept_loop_handle,
      listener: listener_arc,   // Keep Arc reference
      config,                   // Store transport config
      context_options: options, // Store full options Arc
      context_handle_source,
    };

    // Spawn the command handling loop for this Listener actor
    let command_loop_handle = tokio::spawn(actor.run_command_loop());

    Ok((tx, command_loop_handle)) // Return mailbox to command this actor
  }

  /// Main loop to handle commands (like Stop) for the Listener actor itself.
  async fn run_command_loop(mut self) {
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone(); // Clone for cleanup message
    tracing::debug!(handle = handle, uri=%endpoint_uri, "Listener command loop started");

    loop {
      tokio::select! {
          biased;
          cmd = self.mailbox_receiver.recv() => {
            match cmd {
              Ok(Command::Stop) => {
                tracing::info!(handle = self.handle, endpoint=%self.endpoint, "Listener received Stop command");
                // Stop the accept loop by aborting its task handle
                self.listener_handle.abort();
                // We should ideally wait briefly for the accept loop to exit
                // after aborting it, before sending CleanupComplete.
                // However, awaiting the aborted handle directly can panic if the task panicked.
                // A simple sleep might suffice, or a more complex signal mechanism.
                // For now, just break and send cleanup immediately.
                // let _ = self.listener_handle.await; // This might panic
                break; // Exit command loop
              },
              Ok(other) => tracing::warn!(handle=self.handle, uri=%endpoint_uri, "Listener received unhandled command: {:?}", other),
              Err(_) => {
                tracing::info!(handle=self.handle, uri=%endpoint_uri, "Listener command mailbox closed, stopping accept loop.");
                self.listener_handle.abort(); // Ensure accept loop stops
                break;
              }
            }
          }
      }
    }
    tracing::debug!(handle = self.handle, uri=%endpoint_uri, "Listener command loop finished");

    // Send cleanup notification AFTER command loop exits (implies accept loop was aborted)
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle: self.handle,
        endpoint_uri: Some(self.endpoint.clone()), // Use original URI
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle=self.handle, uri=%self.endpoint, "Failed to send CleanupComplete to Core: {:?}", e);
      } else {
        tracing::debug!(handle=self.handle, uri=%self.endpoint, "Listener sent CleanupComplete to Core.");
      }
    }
  }

  /// Separate task that runs the blocking `accept()` loop.
  async fn run_accept_loop(
    listener_handle_id: usize, // Use the main listener actor handle for logging context
    listener: Arc<TokioTcpListener>,
    core_mailbox: MailboxSender,
    config: TcpTransportConfig,
    options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) {
    let local_addr_str = listener.local_addr().map(|a| a.to_string()).unwrap_or_default();
    // TODO: Need the original endpoint URI passed down for accurate reporting, local_addr might be wildcard.
    // Let's assume for now we pass the original URI down to this function.
    // This requires changing create_and_spawn and the function signature.
    // Alternative: Use listener_handle_id and have Core map it back? Less direct.
    // Placeholder - Assume original_endpoint_uri is passed:
    let endpoint_uri = format!("tcp://{}", local_addr_str); // Placeholder
    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Listener accept loop started");

    let mut loop_error: Option<ZmqError> = None; // Track fatal error

    loop {
      // Accept new connections
      let accept_result = listener.accept().await;

      match accept_result {
        Ok((stream, peer_addr)) => {
          tracing::info!(parent_handle = listener_handle_id, %peer_addr, "Accepted new TCP connection");

          // Apply socket options (Nagle, Keepalive) using socket2
          if let Err(e) = apply_tcp_socket_options(&stream, &config) {
            tracing::error!("Failed to apply socket options: {}", e);
            // Optionally close the stream here?
            continue; // Skip this connection
          }

          // --- Spawn Session & Engine for the new connection ---
          let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

          // Determine Endpoint URI - Use peer address? Listener address? TBD by SocketCore needs.
          // Let's use peer address for now as it's unique per connection.
          let endpoint_uri = format!("tcp://{}", peer_addr);

          // 1. Create Session actor
          let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
            session_handle_id,
            endpoint_uri.clone(),
            core_mailbox.clone(), // Pass Core's command mailbox only
                                  // No pipe ends passed here anymore
          );

          // 2. Create Engine actor, passing the connected stream & config
          let (engine_mailbox, engine_task_handle) = crate::engine::zmtp_tcp::create_and_spawn_tcp_engine(
            engine_handle_id,
            session_cmd_mailbox.clone(), // Session's command mailbox
            stream,                      // The TcpStream
            options.clone(),
            true, // true for listener, false for connecter
          );

          // 3. Attach Engine to Session
          let attach_cmd = Command::Attach {
            engine_mailbox,
            engine_handle: Some(engine_handle_id),
            engine_task_handle: Some(engine_task_handle),
          };
          // Send Attach command TO the Session actor's command mailbox
          if session_cmd_mailbox.send(attach_cmd).await.is_err() {
            tracing::error!(
              session_handle = session_handle_id,
              "Failed to send Attach command to session, dropping connection."
            );
            continue;
          }

          // 4. Report success back to SocketCore
          let success_cmd = Command::ConnSuccess {
            endpoint: endpoint_uri.clone(),               // Identify connection
            session_mailbox: session_cmd_mailbox.clone(), // Provide Session command mailbox
            session_handle: Some(session_handle_id),
            session_task_handle: Some(session_task_handle),
          };
          tracing::warn!("TODO: ConnSuccess command needs to include pipe channel ends for SocketCore!");
          if core_mailbox.send(success_cmd).await.is_err() {
            tracing::error!(
              parent_handle = listener_handle_id,
              "Failed to send ConnSuccess to core mailbox. Core actor might be stopped."
            );
            loop_error = Some(ZmqError::Internal("Core mailbox closed".into()));
            // If core is gone, maybe stop accepting?
            break;
          }
        } // end Ok((stream, peer_addr))
        Err(e) => {
          // Accept errors are often recoverable (e.g., EMFILE)
          // But could also be fatal if listener is closed
          tracing::error!(parent_handle = listener_handle_id, "Error accepting connection: {}", e);
          use std::io;
          if matches!(
            e.kind(),
            io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe /* Add other fatal kinds */
          ) {
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Fatal error in accept loop, stopping.");
            loop_error = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break; // Exit loop on fatal error
          }

          // Consider delay before retrying accept?
          tokio::time::sleep(Duration::from_millis(100)).await;
          // TODO: Should check if error is fatal and break loop?
        }
      } // end match accept_result
    } // end loop

    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Listener accept loop finished");

    // Send ReportError if loop exited due to error, otherwise nothing (command loop sends CleanupComplete)
    if let Some(error) = loop_error {
      if !core_mailbox.is_closed() {
        let report_cmd = Command::ReportError {
          handle: listener_handle_id,
          endpoint_uri: endpoint_uri.clone(),
          error,
        };
        if let Err(e) = core_mailbox.send(report_cmd).await {
          tracing::warn!(handle=listener_handle_id, uri=%endpoint_uri, "Failed to send ReportError from accept loop: {:?}", e);
        } else {
          tracing::debug!(handle=listener_handle_id, uri=%endpoint_uri, "Listener accept loop sent ReportError to Core.");
        }
      }
    }
    // Note: CleanupComplete is now sent by the run_command_loop owning the listener actor.
  } // end run_accept_loop
}

pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String, // Target endpoint URI
  core_mailbox: MailboxSender,
  config: TcpTransportConfig,
  context_options: Arc<SocketOptions>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
}

impl TcpConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) -> JoinHandle<()> {
    let config = TcpTransportConfig {
      // Create transport config
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
    // Spawn the main task that attempts connection
    let task_handle = tokio::spawn(connecter.run_connect_loop());

    task_handle
  }

  /// Main task loop that attempts to connect.
  async fn run_connect_loop(self) {
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone();
    let target_addr_str = match endpoint_uri.strip_prefix("tcp://") {
      Some(addr) => addr.to_string(),
      None => {
        tracing::error!(handle = handle, uri = %endpoint_uri, "Invalid TCP endpoint format");
        let fail_cmd = Command::ConnFailed {
          endpoint: endpoint_uri.clone(),
          error: ZmqError::InvalidEndpoint(endpoint_uri.clone()),
        };
        let _ = self.core_mailbox.send(fail_cmd).await;
        // Don't send CleanupComplete here, the actor task finishing is the signal
        return;
      }
    };

    tracing::info!(handle = handle, uri = %endpoint_uri, "TcpConnecter actor started.");

    let mut current_delay = self.context_options.reconnect_ivl.unwrap_or(Duration::ZERO);
    let max_delay = self.context_options.reconnect_ivl_max;
    let mut attempt_count = 0;
    let mut stopped_by_command = false;

    loop {
      // --- Waiting Phase (skip delay for first attempt) ---
      if attempt_count > 0 {
        if current_delay.is_zero() {
          tracing::info!(handle = handle, uri = %endpoint_uri, "Reconnect disabled (interval is 0), stopping connecter task.");
          // No final ConnFailed needed, core just won't get ConnSuccess
          break; // Exit loop normally
        }
        tracing::debug!(handle = handle, uri = %endpoint_uri, delay = ?current_delay, "Waiting before reconnect attempt {}", attempt_count + 1);
        // Just sleep, no select needed
        sleep(current_delay).await;
      }
      attempt_count += 1;

      // --- Connection Attempt Phase ---
      tracing::debug!(handle = handle, uri = %endpoint_uri, "Attempting TCP connect #{}", attempt_count);
      let connect_result = TcpStream::connect(&target_addr_str).await;

      match connect_result {
        Ok(stream) => {
          let peer_addr = stream
            .peer_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
          tracing::info!(handle = handle, uri = %endpoint_uri, peer = %peer_addr, "TCP Connect successful (attempt {})", attempt_count);

          // Apply socket options
          if let Err(e) = apply_tcp_socket_options(&stream, &self.config) {
            tracing::error!(handle = handle, uri = %endpoint_uri, peer = %peer_addr, error = %e, "Failed to apply socket options after connect, will retry connection.");
            drop(stream); // Close the faulty stream
                          // Calculate next delay before continuing
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

          let (session_cmd_mailbox, session_task_handle) =
            SessionBase::create_and_spawn(session_handle_id, endpoint_uri.clone(), self.core_mailbox.clone());
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
            let report_cmd = Command::ReportError {
              handle,
              endpoint_uri: endpoint_uri.clone(),
              error: ZmqError::Internal("Failed to attach engine".into()),
            };
            let _ = self.core_mailbox.send(report_cmd).await;
            session_task_handle.abort();
            // Even though attach failed, loop should exit as connection *was* made, but setup failed.
            // Core handles the error report.
            break;
          } else {
            let success_cmd = Command::ConnSuccess {
              endpoint: endpoint_uri.clone(),
              session_mailbox: session_cmd_mailbox,
              session_handle: Some(session_handle_id),
              session_task_handle: Some(session_task_handle),
            };
            if self.core_mailbox.send(success_cmd).await.is_err() {
              tracing::error!(handle=handle, uri=%endpoint_uri, "Failed to send ConnSuccess to core mailbox.");
              // Core is gone, nothing more we can do. Session/Engine will likely time out.
            }
            // Connection successful and reported, exit the loop.
            break;
          }
        }
        Err(e) => {
          // --- Connection Attempt Failed ---
          tracing::warn!(handle = handle, uri = %endpoint_uri, error = %e, "TCP Connect attempt #{} failed", attempt_count);
          // Calculate next delay before continuing the loop
          if let Some(max_ivl) = max_delay {
            if !max_ivl.is_zero() {
              // max=0 disables backoff
              current_delay = (current_delay * 2).min(max_ivl);
            }
          }
          // Continue loop to wait and retry
        }
      } // end match connect_result
    } // end loop

    // --- Task Cleanup ---
    // Send CleanupComplete *only* if we were explicitly stopped by command
    // before achieving a successful connection and reporting ConnSuccess.
    // If the loop exited due to success or reconnect_ivl=0, we don't send CleanupComplete.
    if stopped_by_command {
      tracing::debug!(handle = handle, uri=%endpoint_uri, "Connecter task was stopped by command.");
      if !self.core_mailbox.is_closed() {
        let cleanup_cmd = Command::CleanupComplete {
          handle,
          endpoint_uri: Some(endpoint_uri.clone()),
        };
        if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
          tracing::warn!(handle=handle, uri=%endpoint_uri, "Failed to send CleanupComplete after stop: {:?}", e);
        } else {
          tracing::debug!(handle=handle, uri=%endpoint_uri, "Connecter sent CleanupComplete after stop.");
        }
      }
    } else {
      tracing::debug!(handle = handle, uri=%endpoint_uri, "Connecter task finished naturally (connected or stopped retrying).");
    }

    tracing::info!(handle = handle, uri = %endpoint_uri, "TcpConnecter actor finished.");
  }
}

/// Helper to apply common TCP socket options
fn apply_tcp_socket_options(stream: &TcpStream, config: &TcpTransportConfig) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);

  socket_ref.set_nodelay(config.tcp_nodelay)?;

  // Apply Keepalive settings
  // Check if *any* keepalive parameter is set in the config
  if config.keepalive_time.is_some() || config.keepalive_interval.is_some() || config.keepalive_count.is_some() {
    let mut keepalive = TcpKeepalive::new();
    if let Some(time) = config.keepalive_time {
      keepalive = keepalive.with_time(time);
    }
    // --- Apply interval if supported ---
    #[cfg(all( // Match cfg from socket2 source
          feature = "all", // Assuming 'all' feature of socket2 is enabled or using defaults
          any(
              target_os = "android", target_os = "dragonfly", target_os = "freebsd",
              target_os = "fuchsia", target_os = "illumos", target_os = "linux",
              target_os = "netbsd", target_vendor = "apple", target_os = "windows", // Added windows here based on source
          )
      ))]
    if let Some(interval) = config.keepalive_interval {
      keepalive = keepalive.with_interval(interval); // Use with_interval
    }

    // --- Apply retries/count if supported ---
    #[cfg(all( // Match cfg from socket2 source
          feature = "all",
          any(
              target_os = "android", target_os = "dragonfly", target_os = "freebsd",
              target_os = "fuchsia", target_os = "illumos", target_os = "linux",
              target_os = "netbsd", target_vendor = "apple",
              // Windows does NOT support setting retries via this struct/API
          )
      ))]
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count); // Use with_retries
    }

    // Enable keepalive using the configured parameters
    socket_ref.set_tcp_keepalive(&keepalive)?;
    tracing::debug!("Applied TCP Keepalive settings: {:?}", keepalive);
  }
  // Note: We don't explicitly handle disabling keepalive (ZMQ_TCP_KEEPALIVE=-1) here yet.
  // `set_tcp_keepalive` likely enables SO_KEEPALIVE implicitly. Disabling might need `socket_ref.set_keepalive(Some(false))`.

  Ok(())
}
