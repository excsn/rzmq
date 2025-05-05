// src/transport/ipc.rs

#![cfg(feature = "ipc")]

use crate::engine; // Engine factory functions
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase;
use crate::socket::options::SocketOptions; // Use full options now
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs; // For removing socket file
use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
use tokio::task::JoinHandle; // For error kinds

// --- IpcListener ---
#[derive(Debug)]
pub(crate) struct IpcListener {
  handle: usize,
  endpoint: String, // Original URI (e.g., "ipc:///tmp/rzmq.sock")
  path: PathBuf,    // Parsed Path
  core_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  listener_handle: JoinHandle<()>, // Accept loop handle
  listener: Arc<TokioUnixListener>,
  context_options: Arc<SocketOptions>, // Store full options to pass to Engine factory
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
}

impl IpcListener {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,               // Pass parsed path
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) -> Result<(MailboxSender, JoinHandle<()>), ZmqError> {
    let (tx, rx) = mailbox();

    // --- Bind the actual listener ---
    // Attempt to remove existing file first for robustness (optional)
    match std::fs::remove_file(&path) {
      Ok(_) => tracing::debug!(handle=handle, path=?path, "Removed existing IPC socket file before binding."),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // Fine if not found
      Err(e) => {
        tracing::warn!(handle=handle, path=?path, error=%e, "Failed to remove existing IPC socket file before binding.")
      }
    }

    let listener = TokioUnixListener::bind(&path).map_err(|e| ZmqError::from_io_endpoint(e, &endpoint_uri))?;
    tracing::info!(handle=handle, path=?path, "IPC Listener bound successfully");
    let listener_arc = Arc::new(listener);
    // --- End Bind ---

    // Spawn the accept loop task separately
    let accept_core_mailbox = core_mailbox.clone();
    let accept_listener = listener_arc.clone();
    let accept_handle_source = context_handle_source.clone();
    let accept_loop_handle = tokio::spawn(IpcListener::run_accept_loop(
      handle,
      endpoint_uri.clone(), // Pass original URI for reporting
      path.clone(),         // Pass path for logging
      accept_listener,
      accept_core_mailbox,
      options.clone(), // Pass options Arc
      accept_handle_source,
    ));

    let actor = IpcListener {
      handle,
      endpoint: endpoint_uri,
      path,
      core_mailbox,
      mailbox_receiver: rx,
      listener_handle: accept_loop_handle,
      listener: listener_arc,
      context_options: options, // Store options Arc
      context_handle_source,
    };
    let command_loop_handle = tokio::spawn(actor.run_command_loop());
    Ok((tx, command_loop_handle))
  }

  /// Main loop to handle commands (Stop) for the Listener actor.
  async fn run_command_loop(mut self) {
    let handle = self.handle;
    let endpoint_uri = self.endpoint.clone(); // Clone for cleanup message
    tracing::debug!(handle = handle, uri=%endpoint_uri, "IPC Listener command loop started");
    loop {
      tokio::select! {
        biased;
        cmd = self.mailbox_receiver.recv() => {
          match cmd {
            Ok(Command::Stop) => {
              tracing::info!(handle = handle, uri=%endpoint_uri, "IPC Listener received Stop command");
              self.listener_handle.abort(); // Stop accept loop first
              // Consider brief await/yield here before sending cleanup?
              break; // Exit command loop
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
        endpoint_uri: Some(self.endpoint.clone()), // Use original URI
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle=self.handle, uri=%self.endpoint, "Failed to send CleanupComplete to Core: {:?}", e);
      } else {
        tracing::debug!(handle=self.handle, uri=%self.endpoint, "IPC Listener sent CleanupComplete to Core.");
      }
    }
    // Drop impl will handle socket file removal
  } // end run_command_loop

  /// Separate task that runs the blocking `accept()` loop.
  async fn run_accept_loop(
    listener_handle_id: usize, // Use the main listener actor handle for logging context
    endpoint_uri: String,      // Original URI for reporting
    path: PathBuf,             // Pass path for logging
    listener: Arc<TokioUnixListener>,
    core_mailbox: MailboxSender,
    options: Arc<SocketOptions>,
    handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) {
    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "IPC Listener accept loop started");

    let mut loop_error: Option<ZmqError> = None; // Track fatal error

    loop {
      match listener.accept().await {
        Ok((stream, _peer_addr)) => {
          tracing::info!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Accepted new IPC connection");
          // Note: No common socket options to apply to UnixStream

          // --- Spawn Session & Engine ---
          let session_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          let engine_handle_id = handle_source.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
          // Use the listener's original endpoint URI for reporting connection success.
          // The session needs a unique ID, but core associates via original listener URI.
          let conn_endpoint_uri = endpoint_uri.clone(); // Use listener's URI

          // 1. Create Session
          let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
            session_handle_id,
            conn_endpoint_uri.clone(), // Pass listener URI for identification
            core_mailbox.clone(),
          );

          // 2. Create IPC Engine using the correct factory function
          let (engine_mailbox, engine_task_handle) = engine::zmtp_ipc::create_and_spawn_ipc_engine(
            engine_handle_id,
            session_cmd_mailbox.clone(), // Session's command mailbox
            stream,                      // The accepted UnixStream
            options.clone(),             // Pass options Arc
            true,                        // is_server = true
          );

          // 3. Attach Engine to Session
          let attach_cmd = Command::Attach {
            engine_mailbox,
            engine_handle: Some(engine_handle_id),
            engine_task_handle: Some(engine_task_handle),
          };
          if session_cmd_mailbox.send(attach_cmd).await.is_err() {
            tracing::error!(session_handle=session_handle_id, uri=%conn_endpoint_uri, "Failed to send Attach command to session, dropping connection.");
            // Abort engine task? No handle here. Session will likely fail later.
            continue; // Don't report ConnSuccess
          }

          // 4. Report success back to SocketCore
          let success_cmd = Command::ConnSuccess {
            endpoint: conn_endpoint_uri.clone(), // Report based on listener URI
            session_mailbox: session_cmd_mailbox,
            session_handle: Some(session_handle_id),
            session_task_handle: Some(session_task_handle),
          };
          if core_mailbox.send(success_cmd).await.is_err() {
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, "Failed to send ConnSuccess to core mailbox. Core actor likely stopped.");
            loop_error = Some(ZmqError::Internal("Core mailbox closed".into()));
            break; // Exit accept loop
          }
        } // end Ok((stream, _peer_addr))

        Err(e) => {
          tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Error accepting IPC connection");
          // Check for fatal errors indicating listener closed
          if matches!(
            e.kind(),
            io::ErrorKind::InvalidInput | io::ErrorKind::BrokenPipe /* Add others? */
          ) {
            tracing::error!(parent_handle = listener_handle_id, uri=%endpoint_uri, error=%e, "Fatal error in IPC accept loop, stopping.");
            loop_error = Some(ZmqError::from_io_endpoint(e, &endpoint_uri));
            break; // Exit loop on fatal error
          }
          // Sleep briefly for potentially recoverable errors
          tokio::time::sleep(Duration::from_millis(100)).await;
        }
      } // end match accept_result
    } // end loop

    tracing::debug!(parent_handle = listener_handle_id, uri=%endpoint_uri, "IPC Listener accept loop finished");

    // Send ReportError if loop exited due to error
    if let Some(error) = loop_error {
      if !core_mailbox.is_closed() {
        let report_cmd = Command::ReportError {
          handle: listener_handle_id, // Report error associated with the listener actor handle
          endpoint_uri: endpoint_uri.clone(),
          error,
        };
        if let Err(e) = core_mailbox.send(report_cmd).await {
          tracing::warn!(handle=listener_handle_id, uri=%endpoint_uri, "Failed to send ReportError from accept loop: {:?}", e);
        } else {
          tracing::debug!(handle=listener_handle_id, uri=%endpoint_uri, "IPC Listener accept loop sent ReportError to Core.");
        }
      }
    }
    // CleanupComplete is sent by the command loop that owns the actor
  } // end run_accept_loop
} // end impl IpcListener

// Implement Drop to clean up the socket file path
impl Drop for IpcListener {
  fn drop(&mut self) {
    tracing::debug!(handle = self.handle, path=?self.path, "Dropping IpcListener, cleaning up socket file");
    // Best effort removal.
    match std::fs::remove_file(&self.path) {
      Ok(_) => tracing::debug!(path=?self.path, "Removed IPC socket file."),
      Err(e) if e.kind() == std::io::ErrorKind::NotFound => {} // Already gone
      Err(e) => tracing::warn!(path=?self.path, error=%e, "Failed to remove IPC socket file during drop."),
    }
    // Ensure accept loop is stopped if not already done
    // abort() is idempotent if already finished/aborted
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
  _mailbox_receiver: MailboxReceiver,  // For potential future Stop command
  context_options: Arc<SocketOptions>, // Use full options Arc
  context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
}

impl IpcConnecter {
  pub(crate) fn create_and_spawn(
    handle: usize,
    endpoint_uri: String,
    path: PathBuf,
    options: Arc<SocketOptions>,
    core_mailbox: MailboxSender,
    context_handle_source: Arc<std::sync::atomic::AtomicUsize>,
  ) -> (MailboxSender, JoinHandle<()>) {
    let (tx, rx) = mailbox();
    let connecter = IpcConnecter {
      handle,
      endpoint_uri,
      path,
      core_mailbox,
      _mailbox_receiver: rx,
      context_options: options, // Store options Arc
      context_handle_source,
    };
    let task_handle = tokio::spawn(connecter.run_connect_attempt());
    (tx, task_handle)
  }

  /// Main task loop that attempts to connect. Consumes self.
  async fn run_connect_attempt(self) {
    let handle = self.handle; // Capture for reporting
    let endpoint_uri = self.endpoint_uri.clone(); // Capture for reporting
    tracing::info!(handle = handle, uri = %endpoint_uri, "IpcConnecter actor started connection attempt");

    // TODO: Implement reconnect logic/delay? Attempt once for now.
    let connect_result = UnixStream::connect(&self.path).await;

    match connect_result {
      Ok(stream) => {
        // --- Connection Successful ---
        tracing::info!(handle = handle, uri = %endpoint_uri, path=?self.path, "IPC Connect successful");
        // Note: No socket options like Nagle/Keepalive apply to UnixStream

        // --- Spawn Session & Engine ---
        let session_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let engine_handle_id = self
          .context_handle_source
          .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // 1. Create Session
        let (session_cmd_mailbox, session_task_handle) = SessionBase::create_and_spawn(
          session_handle_id,
          endpoint_uri.clone(), // Use original endpoint URI
          self.core_mailbox.clone(),
        );

        // 2. Create IPC Engine using the correct factory
        let (engine_mailbox, engine_task_handle) = engine::zmtp_ipc::create_and_spawn_ipc_engine(
          engine_handle_id,
          session_cmd_mailbox.clone(),
          stream,                       // Pass the connected stream
          self.context_options.clone(), // Pass options Arc
          false,                        // is_server = false for connecter
        );

        // 3. Attach Engine to Session
        let attach_cmd = Command::Attach {
          engine_mailbox,
          engine_handle: Some(engine_handle_id),
          engine_task_handle: Some(engine_task_handle),
        };
        if session_cmd_mailbox.send(attach_cmd).await.is_err() {
          tracing::error!(handle = handle, uri = %endpoint_uri, session_handle = session_handle_id, "Failed to send Attach command to session. Reporting internal error.");
          // Report internal error because connection succeeded but setup failed
          let report_cmd = Command::ReportError {
            handle, // Report error associated with this connecter actor
            endpoint_uri: endpoint_uri.clone(),
            error: ZmqError::Internal("Failed to attach engine to session after connect".into()),
          };
          let _ = self.core_mailbox.send(report_cmd).await;
          session_task_handle.abort(); // Abort session as it's unusable
                                       // Fall through to send CleanupComplete below
        } else {
          // Attached successfully, report ConnSuccess to SocketCore
          let success_cmd = Command::ConnSuccess {
            endpoint: endpoint_uri.clone(), // Report original target endpoint
            session_mailbox: session_cmd_mailbox,
            session_handle: Some(session_handle_id),
            session_task_handle: Some(session_task_handle),
          };
          if self.core_mailbox.send(success_cmd).await.is_err() {
            tracing::error!(handle=handle, uri=%endpoint_uri, "Failed to send ConnSuccess to core mailbox.");
            // Core is likely gone.
          }
          // ConnSuccess sent, connecter's primary job for this attempt is done.
        }
      } // end Ok(stream)

      Err(e) => {
        // --- Connection Attempt Failed ---
        tracing::error!(handle = handle, uri = %endpoint_uri, path=?self.path, error = %e, "IPC Connect failed");
        let fail_cmd = Command::ConnFailed {
          endpoint: endpoint_uri.clone(),
          error: ZmqError::from_io_endpoint(e, &endpoint_uri), // Wrap the IO error
        };
        let _ = self.core_mailbox.send(fail_cmd).await; // Report failure to core
                                                        // Fall through to send CleanupComplete below
      }
    } // end match connect_result

    // --- Task Cleanup ---
    tracing::info!(handle = handle, uri = %endpoint_uri, "IpcConnecter actor finished connection attempt");

    // Send CleanupComplete to SocketCore to signal this actor task is finished.
    if !self.core_mailbox.is_closed() {
      let cleanup_cmd = Command::CleanupComplete {
        handle,                           // Use own handle
        endpoint_uri: Some(endpoint_uri), // Provide context
      };
      if let Err(e) = self.core_mailbox.send(cleanup_cmd).await {
        tracing::warn!(handle = handle, "Failed to send CleanupComplete to Core: {:?}", e);
      } else {
        tracing::debug!(handle = handle, "IPC Connecter sent CleanupComplete to Core.");
      }
    }
  } // end run_connect_attempt
}
// --- End IpcConnecter ---
