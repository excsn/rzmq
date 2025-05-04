// src/transport/tcp.rs

use crate::engine::ZmtpTcpEngine; // Import placeholder Engine
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase; // Import placeholder Session
use crate::socket::options::ZmtpTcpConfig;
use crate::Msg; // Import config struct
use socket2::{SockRef, TcpKeepalive};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
use tokio::task::JoinHandle; // For socket options

// --- Real TcpListener ---
// <<< MODIFIED TcpListener >>>
pub(crate) struct TcpListener {
  handle: usize,
  endpoint: String,                 // The original requested endpoint URI
  local_addr: std::net::SocketAddr, // The actual bound address
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // Handle to the accept loop task
  listener: Arc<TokioTcpListener>, // Listener needs to be shared for accept loop
  config: ZmtpTcpConfig,           // Store config passed down from SocketCore/Options
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // Source for unique IDs
}

impl TcpListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,      // e.g., "tcp://*:5555" or "tcp://127.0.0.1:5555"
    config: ZmtpTcpConfig, // Pass down config
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // Pass down handle source
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let (tx, rx) = mailbox();

    // --- Bind the actual listener ---
    // TODO: Extract address from endpoint string properly
    let bind_addr_str = endpoint.strip_prefix("tcp://").unwrap_or(&endpoint); // Simplistic parsing
    let std_listener = std::net::TcpListener::bind(bind_addr_str)
      .map_err(|e| ZmqError::from_io_endpoint(e, &endpoint))?;
    std_listener
      .set_nonblocking(true)
      .map_err(|e| ZmqError::Io(e))?; // Essential for Tokio
                                      // Apply SO_REUSEADDR? Often needed for servers.
    SockRef::from(&std_listener)
      .set_reuse_address(true)
      .map_err(|e| ZmqError::Io(e))?;

    let tokio_listener = TokioTcpListener::from_std(std_listener).map_err(|e| ZmqError::Io(e))?;
    let local_addr = tokio_listener.local_addr().map_err(|e| ZmqError::Io(e))?;
    tracing::info!(
      handle = handle,
      ?local_addr,
      "TCP Listener bound successfully"
    );
    let listener_arc = Arc::new(tokio_listener);
    // --- End Bind ---

    // Spawn the accept loop task separately
    let accept_core_mailbox = core_mailbox.clone();
    let accept_listener = listener_arc.clone();
    let accept_config = config.clone();
    let accept_handle_source = context_handle_source.clone();
    let accept_loop_handle = tokio::spawn(TcpListener::run_accept_loop(
      handle,
      accept_listener,
      accept_core_mailbox,
      accept_config,
      accept_handle_source,
    ));

    let actor = TcpListener {
      handle,
      endpoint,
      local_addr,
      core_mailbox,
      mailbox_receiver: rx,
      listener_handle: accept_loop_handle,
      listener: listener_arc, // Keep Arc reference
      config,
      context_handle_source,
    };

    // Spawn the command handling loop for this Listener actor
    let command_loop_handle = tokio::spawn(actor.run_command_loop());

    Ok((tx, command_loop_handle)) // Return mailbox to command this actor
  }

  /// Main loop to handle commands (like Stop) for the Listener actor itself.
  async fn run_command_loop(mut self) {
    tracing::debug!(handle = self.handle, "Listener command loop started");
    loop {
      tokio::select! {
          biased;
          cmd = self.mailbox_receiver.recv() => {
               match cmd {
                   Ok(Command::Stop) => {
                       tracing::info!(handle = self.handle, endpoint=%self.endpoint, "Listener received Stop command");
                       // Stop the accept loop by aborting its task handle
                       self.listener_handle.abort();
                       break; // Exit command loop
                   },
                   Ok(other) => tracing::warn!(handle=self.handle, "Listener received unhandled command: {:?}", other),
                   Err(_) => {
                        tracing::info!(handle=self.handle, "Listener command mailbox closed, stopping accept loop.");
                        self.listener_handle.abort(); // Ensure accept loop stops
                        break;
                   }
               }
          }
      }
    }
    tracing::debug!(handle = self.handle, "Listener command loop finished");
    // Notify core AFTER accept loop has fully stopped (implicitly via Drop or explicit signal?)
    // The run_accept_loop sends ListenerStopped on its exit.
  }

  /// Separate task that runs the blocking `accept()` loop.
  async fn run_accept_loop(
    listener_handle_id: usize, // Use the main listener actor handle for logging context
    listener: Arc<TokioTcpListener>,
    core_mailbox: MailboxSender,
    config: ZmtpTcpConfig,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) {
    tracing::debug!(
      parent_handle = listener_handle_id,
      "Listener accept loop started"
    );
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
          let (engine_mailbox, engine_task_handle) = ZmtpTcpEngine::create_and_spawn(
            engine_handle_id,
            session_cmd_mailbox.clone(), // Engine talks TO Session's command mailbox
            stream,
            config.clone(),
            true,
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
          tracing::warn!(
            "TODO: ConnSuccess command needs to include pipe channel ends for SocketCore!"
          );
          if core_mailbox.send(success_cmd).await.is_err() {
            tracing::error!(
              parent_handle = listener_handle_id,
              "Failed to send ConnSuccess to core mailbox. Core actor might be stopped."
            );
            // If core is gone, maybe stop accepting?
            break;
          }
        } // end Ok((stream, peer_addr))
        Err(e) => {
          // Accept errors are often recoverable (e.g., EMFILE)
          // But could also be fatal if listener is closed
          tracing::error!(
            parent_handle = listener_handle_id,
            "Error accepting connection: {}",
            e
          );
          // Consider delay before retrying accept?
          tokio::time::sleep(Duration::from_millis(100)).await;
          // TODO: Should check if error is fatal and break loop?
        }
      } // end match accept_result
    } // end loop

    tracing::debug!(
      parent_handle = listener_handle_id,
      "Listener accept loop finished"
    );
    // Notify SocketCore that this listener has stopped
    // Need the original endpoint URI string here
    // let _ = core_mailbox.send(Command::ListenerStopped { handle: listener_handle_id, endpoint_uri: ??? }).await;
  } // end run_accept_loop

  /// Separate command loop task is needed for the listener actor itself to handle Stop
  async fn run_command_loop_removed(self) {
    unimplemented!()
  } // Placeholder for removed function
}
// <<< MODIFIED TcpListener END >>>

// --- Real TcpConnecter ---
// <<< MODIFIED TcpConnecter >>>
pub(crate) struct TcpConnecter {
  handle: usize,
  endpoint: String, // Target endpoint URI
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  config: ZmtpTcpConfig,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
}

impl TcpConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint: String,
    config: ZmtpTcpConfig, // Pass config
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) -> (MailboxSender, JoinHandle<()>) {
    let (tx, rx) = mailbox();
    let connecter = TcpConnecter {
      handle,
      endpoint,
      core_mailbox,
      mailbox_receiver: rx,
      config,
      context_handle_source,
    };
    // Spawn the main task that attempts connection
    let task_handle = tokio::spawn(connecter.run_connect_attempt());
    (tx, task_handle) // Return mailbox sender for controlling this connecter (e.g., Stop)
  }

  /// Main task loop that attempts to connect.
  async fn run_connect_attempt(mut self) {
    tracing::info!(handle = self.handle, endpoint = %self.endpoint, "TcpConnecter actor started");

    // TODO: Implement reconnect logic based on options (RECONNECT_IVL etc.)
    // For now, just attempt once.

    // TODO: Extract address from endpoint string properly
    let target_addr_str = self
      .endpoint
      .strip_prefix("tcp://")
      .unwrap_or(&self.endpoint);

    let connect_result = TcpStream::connect(target_addr_str).await;

    match connect_result {
      Ok(stream) => {
        let peer_addr = stream
          .peer_addr()
          .map_or_else(|_| "unknown".to_string(), |a| a.to_string());
        tracing::info!(handle = self.handle, %peer_addr, "TCP Connect successful");

        // Apply socket options
        if let Err(e) = apply_tcp_socket_options(&stream, &self.config) {
          tracing::error!(
            handle = self.handle,
            "Failed to apply socket options after connect: {}",
            e
          );

          // Report failure back to core? Close stream?
          let fail_cmd = Command::ConnFailed {
            endpoint: self.endpoint.clone(),
            error: ZmqError::Internal(format!("Failed to set socket options: {}", e)),
          };

          let _ = self.core_mailbox.send(fail_cmd).await;

          // Exit connecter task after failure
          // Notify core that connecter *task* stopped
          let _ = self
            .core_mailbox
            .send(Command::ConnecterStopped {
              handle: self.handle,
              endpoint_uri: self.endpoint.clone(),
            })
            .await;
          return;
        }

        // --- Spawn Session & Engine ---
        let session_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let engine_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Create pipes
        let endpoint_uri = self.endpoint.clone(); // Use original endpoint URI
        let pipe_hwm = 1000; // TODO: Get from config
        let (tx_core_to_sess, rx_core_to_sess) = async_channel::bounded::<Msg>(pipe_hwm);
        let (tx_sess_to_core, rx_sess_to_core) = async_channel::bounded::<Msg>(pipe_hwm);
        // TODO: Store core's ends (tx_core_to_sess, rx_sess_to_core)

        let pipe_id_core_write = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let pipe_id_core_read = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // 1. Create Session
        let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
          session_handle_id,
          endpoint_uri.clone(),
          self.core_mailbox.clone(),
        );

        // 2. Create Engine
        let (engine_mailbox, engine_task_handle) = ZmtpTcpEngine::create_and_spawn(
          engine_handle_id,
          session_cmd_mailbox.clone(),
          stream,
          self.config.clone(),
          false,
        );

        // 3. Attach Engine to Session
        let attach_cmd = Command::Attach {
          engine_mailbox,
          engine_handle: Some(engine_handle_id),
          engine_task_handle: Some(engine_task_handle),
        };
        if session_cmd_mailbox.send(attach_cmd).await.is_err() {
          tracing::error!(
            session_handle = session_handle_id,
            "Failed to send Attach command to session."
          );
          // Report ConnFailed because Session is needed
          let fail_cmd = Command::ConnFailed {
            endpoint: self.endpoint.clone(),
            error: ZmqError::Internal("Failed to attach engine to session".into()),
          };
          let _ = self.core_mailbox.send(fail_cmd).await;
          // Exit connecter task after failure
          // Notify core that connecter *task* stopped
          let _ = self
            .core_mailbox
            .send(Command::ConnecterStopped {
              handle: self.handle,
              endpoint_uri: self.endpoint.clone(),
            })
            .await;
          return;
        }

        // 4. Report success back to SocketCore
        let success_cmd = Command::ConnSuccess {
          endpoint: self.endpoint.clone(), // Report the original target endpoint
          session_mailbox: session_cmd_mailbox,
          session_handle: Some(session_handle_id),
          session_task_handle: Some(session_task_handle),
        };
        if self.core_mailbox.send(success_cmd).await.is_err() {
          tracing::error!(
            handle = self.handle,
            "Failed to send ConnSuccess to core mailbox."
          );
          // Core might be gone, can't do much more here
        }
      } // end Ok(stream)
      Err(e) => {
        // Connection failed
        tracing::error!(handle = self.handle, endpoint = %self.endpoint, error = %e, "TCP Connect failed");
        let fail_cmd = Command::ConnFailed {
          endpoint: self.endpoint.clone(),
          error: ZmqError::from_io_endpoint(e, &self.endpoint), // Wrap the IO error
        };
        let _ = self.core_mailbox.send(fail_cmd).await; // Report failure to core
      }
    } // end match connect_result

    // Connecter task typically finishes after the first attempt (success or fail)
    // unless implementing complex retry logic internally.
    tracing::info!(handle = self.handle, endpoint = %self.endpoint, "TcpConnecter actor finished attempt");
    // Notify SocketCore that this connecter *task* has stopped
    let _ = self
      .core_mailbox
      .send(Command::ConnecterStopped {
        handle: self.handle,
        endpoint_uri: self.endpoint.clone(),
      })
      .await;
  } // end run_connect_attempt

  // Command loop might be needed if Stop needs to interrupt connect attempt/retries
  async fn run_command_loop_removed(self) {
    unimplemented!()
  }
}

/// Helper to apply common TCP socket options
fn apply_tcp_socket_options(stream: &TcpStream, config: &ZmtpTcpConfig) -> Result<(), ZmqError> {
  let socket_ref = SockRef::from(stream);

  socket_ref.set_nodelay(config.tcp_nodelay)?;

  // Apply Keepalive settings
  // Check if *any* keepalive parameter is set in the config
  if config.keepalive_time.is_some()
     || config.keepalive_interval.is_some()
     // <<< MODIFIED: Use keepalive_count field name >>>
     || config.keepalive_count.is_some()
  // <<< MODIFIED END >>>
  {
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
    // <<< MODIFIED: Use keepalive_count field and with_retries method >>>
    if let Some(count) = config.keepalive_count {
      keepalive = keepalive.with_retries(count); // Use with_retries
    }
    // <<< MODIFIED END >>>

    // Enable keepalive using the configured parameters
    socket_ref.set_tcp_keepalive(&keepalive)?;
    tracing::debug!("Applied TCP Keepalive settings: {:?}", keepalive);
  }
  // Note: We don't explicitly handle disabling keepalive (ZMQ_TCP_KEEPALIVE=-1) here yet.
  // `set_tcp_keepalive` likely enables SO_KEEPALIVE implicitly. Disabling might need `socket_ref.set_keepalive(Some(false))`.

  Ok(())
}
