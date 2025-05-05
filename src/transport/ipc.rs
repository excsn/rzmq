// src/transport/ipc.rs

#![cfg(feature = "ipc")]

use crate::engine; // Engine factory functions
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase;
use crate::socket::events::{MonitorSender, SocketEvent}; // Import monitor types
use crate::socket::options::SocketOptions;
use std::io;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs; // For removing socket file
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
use tokio::task::JoinHandle;
use tokio::time::sleep; // For delay

// --- IpcListener ---
#[derive(Debug)]
pub(crate) struct IpcListener {
  handle: usize,
  endpoint: String, // Original URI (e.g., "ipc:///tmp/rzmq.sock")
  path: PathBuf,    // Parsed Path - needed for Drop cleanup
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // Accept loop handle - needed for Stop/Drop
}

impl IpcListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,               // Pass parsed path
    options: Arc<SocketOptions>, // Options needed for accept loop
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>, // Handle source needed for accept loop
    monitor_tx: Option<MonitorSender>,                          // Accept monitor sender
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let (tx, rx) = mailbox();

    // --- Bind the actual listener ---
    match std::fs::remove_file(&path) {
      Ok(_) => tracing::debug!(handle=handle, path=?path, "Removed existing IPC socket file before binding."),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
      Err(e) => {
        tracing::warn!(handle=handle, path=?path, error=%e, "Failed to remove existing IPC socket file before binding.")
      }
    }

    let listener = TokioUnixListener::bind(&path).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri))?;
    tracing::info!(handle=handle, path=?path, uri=%endpoint_uri, "IPC Listener bound successfully");
    let listener_arc = Arc::new(listener);
    // --- End Bind ---

    // Spawn the accept loop task separately
    let accept_core_mailbox = core_mailbox.clone();
    let accept_listener = listener_arc.clone();
    let accept_handle_source = context_handle_source.clone();
    let accept_options = options.clone();
    let accept_monitor_tx = monitor_tx.clone();
    let endpoint_clone = endpoint_uri.clone();
    let path_clone = path.clone(); // For logging inside accept loop

    let accept_loop_handle = tokio::spawn(IpcListener::run_accept_loop(
      handle,
      endpoint_clone, // Pass original endpoint URI
      path_clone,     // Pass path for logging
      accept_listener,
      accept_core_mailbox,
      accept_options,
      accept_handle_source,
      accept_monitor_tx, // Pass monitor sender
    ));

    let actor = IpcListener {
      handle,
      endpoint: endpoint_uri,
      path, // Keep path for Drop impl
      core_mailbox,
      mailbox_receiver: rx,
      listener_handle: accept_loop_handle, // Store handle to abort loop
    };

    let command_loop_handle = tokio::spawn(actor.run_command_loop());
    Ok((tx, command_loop_handle)) // Return mailbox and handle for command loop
  }

  /// Main loop to handle commands (like Stop) for the Listener actor itself.
  async fn run_command_loop(mut self) {
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone();
    tracing::debug!(handle = handle, uri=%endpoint_uri, "IPC Listener command loop started");

    loop {
      tokio::select! {
          biased;
          cmd = self.mailbox_receiver.recv() => {
            match cmd {
              Ok(Command::Stop) => {
                tracing::info!(handle = handle, uri=%endpoint_uri, "IPC Listener received Stop command");
                self.listener_handle.abort();
                break;
              },
              Ok(other) => tracing::warn!(handle=handle, uri=%endpoint_uri, "IPC Listener received unhandled command: {:?}", other),
              Err(_) => {
                tracing::info!(handle=handle, uri=%endpoint_uri, "IPC Listener command mailbox closed, stopping accept loop.");
                self.listener_handle.abort();
                break;
              }
            }
          }
      }
    }
    tracing::debug!(handle = handle, uri=%endpoint_uri, "IPC Listener command loop finished");

    // Send CleanupComplete notification AFTER command loop exits
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle: self.handle,
        endpoint_uri: Some(self.endpoint.clone()),
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle=self.handle, uri=%self.endpoint, "Failed to send CleanupComplete to Core: {:?}", e);
      } else {
        tracing::debug!(handle=self.handle, uri=%self.endpoint, "IPC Listener sent CleanupComplete to Core.");
      }
    }
    // Drop impl handles socket file removal
  }

  /// Separate task that runs the blocking `accept()` loop.
  async fn run_accept_loop(
    listener_handle_id: usize,
    endpoint_uri: String, // Original listener URI
    path: PathBuf,        // Listener path for logging
    listener: Arc<TokioUnixListener>,
    core_mailbox: MailboxSender,
    options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) {
    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, path=?path, "IPC Listener accept loop started");
    let mut loop_error: Option<ZmqError> = None;

    loop {
      match listener.accept().await {
        Ok((stream, _peer_addr)) => {
          // Peer addr is often meaningless for UnixStream
          let peer_addr_str = format!("ipc-peer-fd-{}", stream.as_raw_fd()); // Use FD as identifier?
          tracing::info!(parent_handle = listener_handle_id, path=?path, peer=?peer_addr_str, "Accepted new IPC connection");

          // Emit Accepted event
          if let Some(ref tx) = monitor_tx {
            let event = SocketEvent::Accepted {
              endpoint: endpoint_uri.clone(),
              peer_addr: peer_addr_str.clone(), // Use synthetic peer addr
            };
            let tx_clone = tx.clone();
            tokio::spawn(async move {
              if tx_clone.send(event).await.is_err() { /* Warn */ }
            });
          }

          // --- Spawn Session & Engine ---
          let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          // Session endpoint URI identifies the specific connection
          let conn_endpoint_uri = format!("ipc://{}", peer_addr_str); // Use unique identifier

          let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
            session_handle_id,
            conn_endpoint_uri.clone(),
            core_mailbox.clone(),
            monitor_tx.clone(), // Pass monitor sender to Session
          );
          let (engine_mailbox, engine_task_handle) = engine::zmtp_ipc::create_and_spawn_ipc_engine(
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
            // Emit AcceptFailed because session setup failed?
            if let Some(ref tx) = monitor_tx {
              let event = SocketEvent::AcceptFailed {
                endpoint: endpoint_uri.clone(),
                error_msg: "Failed to attach engine to session".to_string(),
              };
              let tx_clone = tx.clone();
              tokio::spawn(async move {
                if tx_clone.send(event).await.is_err() { /* Warn */ }
              });
            }
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
          tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Error accepting IPC connection");
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
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Fatal error in IPC accept loop, stopping.");
            loop_error = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break;
          }
          sleep(Duration::from_millis(100)).await;
        }
      } // end match accept_result
    } // end loop

    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "IPC Listener accept loop finished");

    // Send ReportError if loop exited due to error
    if let Some(error) = loop_error {
      if !core_mailbox.is_closed() {
        let report_cmd = Command::ReportError {
          handle: listener_handle_id,
          endpoint_uri: endpoint_uri.clone(),
          error,
        };
        if let Err(e) = core_mailbox.send(report_cmd).await { /* Warn */
        } else {
          tracing::debug!(handle=listener_handle_id, uri=%endpoint_uri, "IPC Listener accept loop sent ReportError to Core.");
        }
      }
    }
  } // end run_accept_loop
} // end impl IpcListener

// Implement Drop to clean up the socket file path
impl Drop for IpcListener {
  fn drop(&mut self) {
    tracing::debug!(handle = self.handle, path=?self.path, "Dropping IpcListener, cleaning up socket file");
    match std::fs::remove_file(&self.path) {
      Ok(_) => tracing::debug!(path=?self.path, "Removed IPC socket file."),
      Err(e) if e.kind() == io::ErrorKind::NotFound => {}
      Err(e) => tracing::warn!(path=?self.path, error=%e, "Failed to remove IPC socket file during drop."),
    }
    self.listener_handle.abort();
  }
}
// --- End IpcListener ---

// --- IpcConnecter ---
#[derive(Debug)]
pub(crate) struct IpcConnecter {
  handle: usize,
  endpoint_uri: String, // Original URI
  path: PathBuf,        // Parsed path
  core_mailbox: MailboxSender,
  // No need for receiver mailbox if it only runs once
  context_options: Arc<SocketOptions>,
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  // No need to store monitor_tx, passed directly to run_connect_attempt
}

impl IpcConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
    monitor_tx: Option<MonitorSender>, // Accept monitor sender
  ) -> (MailboxSender, JoinHandle<()>) {
    // Return mailbox for consistency, even if unused
    let (tx, _rx) = mailbox(); // Create mailbox pair
    let connecter = IpcConnecter {
      handle,
      endpoint_uri,
      path,
      core_mailbox,
      context_options: options,
      context_handle_source,
    };
    // Spawn the main task that attempts connection
    let task_handle = tokio::spawn(connecter.run_connect_attempt(monitor_tx)); // Pass monitor_tx
    (tx, task_handle) // Return mailbox and handle
  }

  /// Main task loop that attempts to connect. Consumes self.
  async fn run_connect_attempt(self, monitor_tx: Option<MonitorSender>) {
    // Accept monitor_tx
    let handle = self.handle;
    let endpoint_uri = self.endpoint_uri.clone();
    let path = self.path.clone(); // Clone path for logging/use

    tracing::info!(handle = handle, uri = %endpoint_uri, path=?path, "IpcConnecter actor started connection attempt");

    // Simplified: Attempt connect once for IPC (reconnect logic less common)
    // Could add retry logic similar to TCP if desired.
    let connect_result = UnixStream::connect(&path).await;

    match connect_result {
      Ok(stream) => {
        let peer_addr_str = format!("ipc-peer-fd-{}", stream.as_raw_fd()); // Synthetic peer addr
        tracing::info!(handle = handle, uri = %endpoint_uri, path=?path, peer=?peer_addr_str, "IPC Connect successful");

        // Emit Connected event
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

        // --- Spawn Session & Engine ---
        let session_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let engine_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let conn_endpoint_uri = format!("ipc://{}", peer_addr_str); // Unique connection identifier

        let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
          session_handle_id,
          conn_endpoint_uri.clone(),
          self.core_mailbox.clone(),
          monitor_tx.clone(), // Pass monitor sender to Session
        );
        let (engine_mailbox, engine_task_handle) = engine::zmtp_ipc::create_and_spawn_ipc_engine(
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
          // Emit ConnectFailed
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
          // Report ReportError to Core
          let report_cmd = Command::ReportError {
            handle,
            endpoint_uri: conn_endpoint_uri.clone(),
            error,
          };
          let _ = self.core_mailbox.send(report_cmd).await;
          session_task_handle.abort();
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
          }
        }
      } // end Ok(stream)

      Err(e) => {
        tracing::error!(handle = handle, uri = %endpoint_uri, path=?path, error = %e, "IPC Connect failed");
        let error = ZmqError::from_io_endpoint(e, &endpoint_uri);
        // Emit ConnectFailed immediately (no retry logic for IPC currently)
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
      }
    } // end match connect_result

    // --- Task Cleanup ---
    tracing::info!(handle = handle, uri = %endpoint_uri, "IpcConnecter actor finished connection attempt");
    // Send CleanupComplete - Connecter always sends this when its task ends
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle,
        endpoint_uri: Some(endpoint_uri),
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await { /* Warn */
      } else {
        tracing::debug!(handle = handle, "IPC Connecter sent CleanupComplete to Core.");
      }
    }
  } // end run_connect_attempt
}
// --- End IpcConnecter ---

// Helper function (same as TCP version)
fn is_fatal(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe)
}
