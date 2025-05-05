// src/socket/core.rs

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase;
// Use our types
use crate::socket::options::{
  self, parse_blob_option, parse_bool_option, parse_duration_ms_option, parse_heartbeat_option, parse_i32_option,
  parse_keepalive_mode_option, parse_linger_option, parse_secs_duration_option, parse_timeout_option, parse_u32_option,
  SocketOptions, ZmtpEngineConfig, ROUTER_MANDATORY,
};

#[cfg(feature = "curve")]
use crate::socket::options::{parse_key_option, CURVE_KEY_LEN};
use crate::socket::patterns;
use crate::socket::types::SocketType;
use crate::socket::ISocket;
use crate::transport::endpoint::{parse_endpoint, Endpoint};
use crate::transport::inproc::bind_inproc;
#[cfg(feature = "inproc")]
use crate::transport::inproc::connect_inproc;
#[cfg(feature = "ipc")]
use crate::transport::ipc::{IpcConnecter, IpcListener};
use crate::transport::tcp::{TcpConnecter, TcpListener};
use crate::Msg;
// use crate::socket::{
//   dealer_socket::DealerSocket, pub_socket::PubSocket, pull_socket::PullSocket,
//   push_socket::PushSocket, rep_socket::RepSocket, req_socket::ReqSocket,
//   router_socket::RouterSocket, sub_socket::SubSocket,
// };

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use async_channel::{Receiver as AsyncReceiver, SendError, Sender as AsyncSender, TrySendError};
use tokio::sync::{oneshot, Mutex}; // Use Tokio's Mutex for CoreState
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::{DealerSocket, PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, RouterSocket, SubSocket};

// Information stored for each active endpoint (listener or connection)
#[derive(Debug)]
pub(crate) struct EndpointInfo {
  pub mailbox: MailboxSender,      // Mailbox of the child actor (Listener/Connecter/Session)
  pub task_handle: JoinHandle<()>, // Task handle of the child actor
  pub endpoint_type: EndpointType, // Listener or Session (Connecter transitions to Session)
  pub endpoint_uri: String,        // The normalized endpoint string
  pub pipe_ids: Option<(usize, usize)>, // (ID for core->sess chan, ID for sess->core chan)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointType {
  Listener,
  // Connecter, // Connecter is transient, transitions into Session
  Session, // Represents an active connection/session
}

/// Holds the mutable state managed exclusively by the SocketCore actor task.
#[derive(Debug)] // Basic Debug
pub(crate) struct CoreState {
  /// Options configured for this socket.
  pub options: SocketOptions, // Define this struct in options.rs

  /// Type of this socket (Pub, Sub, etc.).
  pub socket_type: SocketType,

  // Map pipe ID -> Sender for messages Core sends TO Sessions/Inproc
  pub pipes_tx: HashMap<usize, AsyncSender<Msg>>,

  // Map pipe READ ID -> Info about the task reading from that pipe
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,

  /// Map of active listeners or connected sessions.
  /// Key is the normalized endpoint URI string.
  pub endpoints: HashMap<String, EndpointInfo>,
  // --- State relevant for pattern logic / message handling ---
  // pub rcvhwm_reached: bool, // Example state flag
  // pub sndhwm_reached: bool, // Example state flag
  // Add other state as needed by ISocket impls or core logic
  #[cfg(feature = "inproc")]
  pub(crate) bound_inproc_names: HashSet<String>, // Track bound names
}

impl CoreState {
  fn new(socket_type: SocketType, options: SocketOptions) -> Self {
    Self {
      options,
      socket_type,
      pipes_tx: HashMap::new(),
      pipe_reader_task_handles: HashMap::new(),
      endpoints: HashMap::new(),
      #[cfg(feature = "inproc")]
      bound_inproc_names: HashSet::new(),
    }
  }

  /// Gets a clone of the sender for a specific write pipe ID.
  /// Used by ISocket implementations (e.g., PushSocket::send).
  pub(crate) fn get_pipe_sender(&self, pipe_write_id: usize) -> Option<AsyncSender<Msg>> {
    self.pipes_tx.get(&pipe_write_id).cloned()
  }

  /// Gets the JoinHandle for a specific pipe reader task ID.
  /// Used during cleanup.
  pub(crate) fn get_reader_task_handle(&self, pipe_read_id: usize) -> Option<&JoinHandle<()>> {
    self.pipe_reader_task_handles.get(&pipe_read_id)
  }

  /// Removes all state associated with a pipe pair.
  /// Aborts the reader task. Returns true if elements were removed.
  pub(crate) fn remove_pipe_state(&mut self, pipe_write_id: usize, pipe_read_id: usize) -> bool {
    let tx_removed = self.pipes_tx.remove(&pipe_write_id).is_some();
    if tx_removed {
      tracing::trace!(pipe_id = pipe_write_id, "Removed pipe sender");
    }

    // Remove reader task handle and abort task
    let reader_removed = if let Some(handle) = self.pipe_reader_task_handles.remove(&pipe_read_id) {
      tracing::trace!(pipe_id = pipe_read_id, "Removing and aborting pipe reader task");
      handle.abort(); // Forcefully stop the reader task
      true
    } else {
      false
    };
    if reader_removed {
      tracing::trace!(pipe_id = pipe_read_id, "Removed pipe reader task handle");
    }

    tx_removed || reader_removed
  }
}

/// The core actor managing the state and lifecycle of a single rzmq socket.
#[derive(Debug)]
pub(crate) struct SocketCore {
  pub(crate) handle: usize,
  pub(crate) context: Context,
  pub(crate) mailbox_sender: MailboxSender,
  mailbox_receiver: Mutex<MailboxReceiver>,
  pub(crate) core_state: Mutex<CoreState>,
  socket_logic: Mutex<Option<Weak<dyn ISocket>>>,
  command_task_handle: Mutex<Option<JoinHandle<()>>>,
}

impl SocketCore {
  pub(crate) fn mailbox_sender(&self) -> &MailboxSender {
    &self.mailbox_sender
  }

  /// Gets a strong reference to the ISocket logic if it's still alive.
  /// Marked pub(crate) for use by helpers like transport::inproc.
  pub(crate) async fn get_socket_logic(&self) -> Option<Arc<dyn ISocket>> {
    let guard = self.socket_logic.lock().await; // Acquire lock
    guard.as_ref().and_then(|weak_ref| weak_ref.upgrade())
  }
  
  /// Creates the SocketCore actor, the specific ISocket pattern implementation,
  /// spawns the core command loop task, and returns the public ISocket handle
  /// and the mailbox sender for the core actor.
  pub(crate) fn create_and_spawn(
    handle: usize,
    context: Context,
    socket_type: SocketType,
    mut initial_options: SocketOptions,
  ) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
    let (mailbox_sender, mailbox_receiver) = mailbox();

    initial_options.socket_type_name = format!("{:?}", socket_type).to_uppercase();
    let initial_options_clone = initial_options.clone();

    // 1. Create SocketCore instance with an initial empty Weak reference
    let core_state = CoreState::new(socket_type, initial_options);
    let core_arc = Arc::new(SocketCore {
      handle,
      context,
      mailbox_sender: mailbox_sender.clone(),
      mailbox_receiver: Mutex::new(mailbox_receiver),
      core_state: Mutex::new(core_state),
      socket_logic: Mutex::new(None),
      command_task_handle: Mutex::new(None),
    });

    // 2. Create the Concrete ISocket implementation, giving it the Arc<SocketCore>
    let socket_logic_arc: Arc<dyn ISocket> = match socket_type {
      SocketType::Pub => Arc::new(PubSocket::new(core_arc.clone())),
      SocketType::Sub => Arc::new(SubSocket::new(core_arc.clone(), initial_options_clone)),
      SocketType::Req => Arc::new(ReqSocket::new(core_arc.clone(), initial_options_clone)),
      SocketType::Rep => Arc::new(RepSocket::new(core_arc.clone(), initial_options_clone)),
      SocketType::Dealer => Arc::new(DealerSocket::new(core_arc.clone(), initial_options_clone)),
      SocketType::Router => Arc::new(RouterSocket::new(core_arc.clone(), initial_options_clone)),
      SocketType::Push => Arc::new(PushSocket::new(core_arc.clone())),
      SocketType::Pull => Arc::new(PullSocket::new(core_arc.clone(), initial_options_clone)),
    };

    // 3. Create and store the Weak reference back into SocketCore
    let weak_isocket = Arc::downgrade(&socket_logic_arc);
    // Use try_lock: should be uncontended here
    if let Ok(mut socket_logic_guard) = core_arc.socket_logic.try_lock() {
      *socket_logic_guard = Some(weak_isocket);
    } else {
      tracing::error!(handle = handle, "Failed to lock socket_logic mutex during init");
      return Err(ZmqError::Internal("Failed to initialize socket weak reference".into()));
    }

    // 4. Spawn the main command loop task
    let task_core_ref = core_arc.clone(); // Clone Arc for the task
    let task_socket_logic_ref = socket_logic_arc.clone();
    let task_handle = tokio::spawn(SocketCore::run_command_loop(task_core_ref, task_socket_logic_ref));

    // 5. Store the task handle back into SocketCore
    if let Ok(mut handle_guard) = core_arc.command_task_handle.try_lock() {
      *handle_guard = Some(task_handle);
    } else {
      // If this fails, we should probably abort the task we just spawned
      task_handle.abort();
      tracing::error!(
        handle = handle,
        "Failed to lock task handle mutex during init, task aborted"
      );
      return Err(ZmqError::Internal("Failed to store socket task handle".into()));
    }

    // 6. Return the public handle and mailbox sender
    Ok((socket_logic_arc, mailbox_sender))
  }

  /// Task function executed by tokio::spawn for each incoming pipe receiver.
  /// Reads messages from the session and sends them to the SocketCore's mailbox.
  pub(crate) async fn run_pipe_reader_task(
    core_handle: usize,          // Handle of the parent SocketCore for logging
    core_mailbox: MailboxSender, // Mailbox of the parent SocketCore
    pipe_read_id: usize,         // ID of the pipe this task is reading from
    pipe_receiver: AsyncReceiver<Msg>, // The actual channel receiver end
                                 // Optional: stop_receiver: oneshot::Receiver<()> // To allow explicit stop signal
  ) {
    tracing::debug!(
      core_handle = core_handle,
      pipe_id = pipe_read_id,
      "Pipe reader task started"
    );

    loop {
      // Select between receiving a message or being stopped
      tokio::select! {
          // biased; // Prioritize stop signal if implemented?

          // Read from the session pipe
          msg_result = pipe_receiver.recv() => {
              match msg_result {
                  Ok(msg) => {
                      // Forward the message AS A COMMAND to SocketCore's main loop
                      // This centralizes message processing logic in SocketCore::run_command_loop
                      // We need a new Command variant for this.
                      let cmd = Command::PipeMessageReceived { pipe_id: pipe_read_id, msg };
                      if core_mailbox.send(cmd).await.is_err() {
                          tracing::warn!(core_handle=core_handle, pipe_id=pipe_read_id, "Core mailbox closed while forwarding pipe message. Stopping reader.");
                          break; // Core actor is gone
                      }
                  }
                  Err(_) => {
                      // Channel closed by the sender (Session actor)
                      tracing::debug!(core_handle=core_handle, pipe_id=pipe_read_id, "Session pipe sender closed. Stopping reader.");
                      // Notify SocketCore that this specific pipe closed
                      let cmd = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
                       if core_mailbox.send(cmd).await.is_err() {
                           tracing::warn!(core_handle=core_handle, pipe_id=pipe_read_id, "Core mailbox closed while sending PipeClosedByPeer. Stopping reader.");
                       }
                      break; // Exit loop
                  }
              }
          }

          // Placeholder for explicit stop signal if needed
          // _ = stop_receiver => {
          //    tracing::debug!(core_handle=core_handle, pipe_id=pipe_read_id, "Pipe reader received stop signal.");
          //    break;
          // }
      }
    } // end loop

    tracing::debug!(
      core_handle = core_handle,
      pipe_id = pipe_read_id,
      "Pipe reader task finished"
    );
    // No need to send PipeClosedCmd here, that's for when the *other* end closes.
    // SocketCore removes this task's info when SessionStopped or PipeClosedByPeer is handled.
  }

  async fn run_command_loop(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: Arc<dyn ISocket>, // Keep strong ref alive for loop duration
  ) {
    let handle = core_arc.handle;
    tracing::info!(handle = handle, "SocketCore run_command_loop started");

    loop {
      // <--- START loop
      let command_result = {
        let mut receiver_guard = core_arc.mailbox_receiver.lock().await;
        tokio::select! {
            biased;
            // TODO: Add pipe/timer select arms later
            cmd = receiver_guard.recv() => cmd
        }
      };

      let mut should_break_loop = false;

      match command_result {
        // <--- START match command_result
        Ok(command) => {
          // <--- START Ok(command) arm
          tracing::debug!(handle = handle, "Received command: {:?}", command);

          // If socket_logic is invalid, skip command processing
          if !should_break_loop {
            // socket_logic_strong is valid, proceed
            // --- Command Handling Logic ---
            match command {
              // <--- START match command
              // --- Stop / Close ---
              Command::Stop => {
                tracing::info!(handle = handle, "Stop command received, initiating shutdown.");
                Self::shutdown_logic(core_arc.clone(), &socket_logic_strong).await;
                should_break_loop = true;
              }
              Command::UserClose { reply_tx } => {
                tracing::info!(handle = handle, "UserClose received, initiating shutdown.");
                Self::shutdown_logic(core_arc.clone(), &socket_logic_strong).await;
                let _ = reply_tx.send(Ok(()));
                should_break_loop = true;
              }

              // --- User Commands (Core Handling / Spawning) ---
              Command::UserBind { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserBind");

                let bind_result = match parse_endpoint(&endpoint) {
                  Ok(Endpoint::Tcp(_addr, uri)) => {
                    let mut state = core_arc.core_state.lock().await;
                    let options_arc = Arc::new(state.options.clone());
                    if state.endpoints.contains_key(&uri) {
                      Err(ZmqError::AddrInUse(uri))
                    } else {
                      let handle_source = core_arc.context.inner().next_handle.clone();
                      let child_handle = core_arc.context.inner().next_handle();
                      match TcpListener::create_and_spawn(
                        child_handle,
                        uri.clone(),
                        options_arc,
                        core_arc.mailbox_sender().clone(),
                        handle_source,
                      ) {
                        Ok((mailbox, task)) => {
                          let info = EndpointInfo {
                            mailbox,
                            task_handle: task,
                            endpoint_type: EndpointType::Listener,
                            endpoint_uri: uri.clone(),
                            pipe_ids: None,
                          };
                          state.endpoints.insert(uri, info);
                          Ok(())
                        }
                        Err(e) => Err(e),
                      }
                    }
                  }
                  #[cfg(feature = "ipc")]
                  Ok(Endpoint::Ipc(path, uri)) => {
                    // Ensure feature enabled in function signature if needed? No, cfg on module.
                    let mut state = core_arc.core_state.lock().await;
                    let options_arc = Arc::new(state.options.clone());
                    if state.endpoints.contains_key(&uri) {
                      Err(ZmqError::AddrInUse(uri))
                    } else {
                      let child_handle = core_arc.context.inner().next_handle();
                      let handle_source = core_arc.context.inner().next_handle.clone();
                      match IpcListener::create_and_spawn(
                        child_handle,
                        uri.clone(),
                        path,
                        options_arc,
                        core_arc.mailbox_sender().clone(),
                        handle_source,
                      ) {
                        Ok((mailbox, task)) => {
                          let info = EndpointInfo {
                            mailbox,
                            task_handle: task,
                            endpoint_type: EndpointType::Listener,
                            endpoint_uri: uri.clone(),
                            pipe_ids: None,
                          };
                          state.endpoints.insert(uri, info);
                          Ok(())
                        }
                        Err(e) => Err(e),
                      }
                    }
                  }
                  #[cfg(feature = "inproc")]
                  Ok(Endpoint::Inproc(name)) => {
                    let core_clone = core_arc.clone();
                    let name_clone = name.clone();
                    // Use tokio::spawn for the async call
                    let bind_future = async move {
                      match crate::transport::inproc::bind_inproc(name_clone.clone(), core_clone.clone()).await {
                        // Clone name_clone
                        Ok(()) => {
                          // Add name to state on success
                          let mut state = core_clone.core_state.lock().await;
                          state.bound_inproc_names.insert(name_clone); // Use the cloned name
                          Ok(())
                        }
                        Err(e) => Err(e),
                      }
                    };
                    // Await the result of the bind operation
                    match bind_future.await {
                      Ok(()) => Ok(()),
                      Err(e) => Err(e),
                    }
                  }
                  Err(e) => Err(e), // Error from parse_endpoint
                  // Catch non-enabled features (parse_endpoint should handle this via cfg)
                  _ => Err(ZmqError::UnsupportedTransport(endpoint.to_string())),
                };

                // 7. Send Reply back to user
                let _ = reply_tx.send(bind_result); // Ignore error if user task already gone
              } // End UserBind arm

              Command::UserConnect { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserConnect");

                // 1. Parse Endpoint (directly)
                match parse_endpoint(&endpoint) {
                  Ok(Endpoint::Tcp(_addr, uri)) => {
                    // 2. Get Options (needs lock)
                    let state = core_arc.core_state.lock().await;
                    let options_arc = Arc::new(state.options.clone());
                    let handle_source = core_arc.context.inner().next_handle.clone();
                    drop(state); // Release lock

                    // 3. Generate Handle
                    let child_handle = core_arc.context.inner().next_handle();

                    // 4. Spawn TCP Connecter Actor
                    // create_and_spawn returns immediately after spawning the task
                    let _task = TcpConnecter::create_and_spawn(
                      child_handle,
                      uri,         // Pass endpoint URI
                      options_arc, // Pass Arc<SocketOptions>
                      core_arc.mailbox_sender().clone(),
                      handle_source,
                    );
                    let _ = reply_tx.send(Ok(())); 
                  }
                  #[cfg(feature = "ipc")]
                  Ok(Endpoint::Ipc(path, uri)) => {
                    // 2. Get Options
                    let state = core_arc.core_state.lock().await;
                    let options_arc = Arc::new(state.options.clone());
                    let handle_source = core_arc.context.inner().next_handle.clone();
                    drop(state);
                    // 3. Generate Handle
                    let child_handle = core_arc.context.inner().next_handle();
                    // 4. Spawn IPC Connecter Actor
                    let (_mailbox, _task) = IpcConnecter::create_and_spawn(
                      child_handle,
                      uri,         // Pass endpoint URI
                      path,        // Pass PathBuf
                      options_arc, // Pass Arc<SocketOptions>
                      core_arc.mailbox_sender().clone(),
                      handle_source,
                    );
                    let _ = reply_tx.send(Ok(())); 
                  }

                  #[cfg(feature = "inproc")]
                  Ok(Endpoint::Inproc(name)) => {
                    // Inproc connect happens more synchronously via context lookup
                    // Spawn task only for the async lookup/request part
                    let core_clone = core_arc.clone();
                    let name_clone = name.clone();
                    let reply_tx_clone = reply_tx; // Move reply_tx into task
                    tokio::spawn(async move {
                      // Call helper which handles sending reply internally
                      connect_inproc(name_clone, core_clone, reply_tx_clone).await;
                    });
                  }
                  Err(e) => {
                    // Error from parse_endpoint
                    // Send error back immediately
                    let _ = reply_tx.send(Err(e));
                  }
                  // Catch non-enabled features
                  _ => {
                    let endpoint_str = endpoint.clone(); // Clone for error message
                    let _ = reply_tx.send(Err(ZmqError::UnsupportedTransport(endpoint_str)));
                  }
                }; // End match parse_endpoint
              } // End UserConnect arm
              Command::UserDisconnect { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserDisconnect");
                // Call ISocket's disconnect method
                let result = socket_logic_strong.disconnect(&endpoint).await;
                let _ = reply_tx.send(result);
              }
              Command::UserUnbind { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserUnbind");
                // Call ISocket's unbind method
                let result = socket_logic_strong.unbind(&endpoint).await;
                let _ = reply_tx.send(result);
              }

              // --- User Commands (Direct Delegation to ISocket) ---
              Command::UserSend { msg } => {
                // Delegate directly to the pattern's send logic
                if let Err(e) = socket_logic_strong.send(msg).await {
                  tracing::error!(handle = handle, "Error during send: {}", e);
                  // TODO: How to report async send errors back? Maybe send needs a reply_tx?
                }
              }
              Command::UserRecv { reply_tx } => {
                // Delegate directly to the pattern's recv logic
                let result = socket_logic_strong.recv().await;
                let _ = reply_tx.send(result);
              }
              Command::UserSetOpt {
                option,
                value,
                reply_tx,
              } => {
                let result = Self::handle_set_option(core_arc.clone(), &socket_logic_strong, option, &value).await;
                let _ = reply_tx.send(result);
              }
              Command::UserGetOpt { option, reply_tx } => {
                let result = Self::handle_get_option(core_arc.clone(), &socket_logic_strong, option).await;
                let _ = reply_tx.send(result);
              }

              // --- Child Lifecycle Commands (Handled by Core) ---
              Command::ConnSuccess {
                endpoint,
                session_mailbox,
                session_task_handle,
                session_handle,
              } => {
                tracing::info!(handle = handle, %endpoint, "Connection successful reported");

                let session_handle_id = session_handle.unwrap_or_else(|| core_arc.context.inner().next_handle());

                // 1. Create Pipe Channels
                let pipe_id_core_write = core_arc.context.inner().next_handle();
                let pipe_id_core_read = core_arc.context.inner().next_handle();

                let mut state = core_arc.core_state.lock().await;

                // Determine capacity based on relevant HWM for each direction.
                // Core writes TO Session (influenced by peer's RCVHWM, effectively our SNDHWM setting)
                let core_to_session_capacity = state.options.sndhwm.max(1); // Ensure > 0
                                                                            // Core reads FROM Session (influenced by peer's SNDHWM, effectively our RCVHWM setting)
                let session_to_core_capacity = state.options.rcvhwm.max(1); // Ensure > 0
                tracing::debug!(
                  handle = handle,
                  write_id = pipe_id_core_write,
                  write_cap = core_to_session_capacity,
                  read_id = pipe_id_core_read,
                  read_cap = session_to_core_capacity,
                  "Creating pipes with HWM capacities"
                );

                let (tx_core_to_sess, rx_core_to_sess) = async_channel::bounded::<Msg>(core_to_session_capacity);
                let (tx_sess_to_core, rx_sess_to_core) = async_channel::bounded::<Msg>(session_to_core_capacity);

                // 2. Store Core's Ends & Spawn Reader Task
                state.pipes_tx.insert(pipe_id_core_write, tx_core_to_sess.clone()); // Store Core's Sender

                let reader_task_handle = tokio::spawn(Self::run_pipe_reader_task(
                  core_arc.handle,
                  core_arc.mailbox_sender().clone(),
                  pipe_id_core_read, // This task reads pipe 'read_id'
                  rx_sess_to_core,   // The actual receiver end Session writes to
                ));
                state
                  .pipe_reader_task_handles
                  .insert(pipe_id_core_read, reader_task_handle);

                // 3. Store Endpoint Info (associating session with pipe IDs)
                let info = EndpointInfo {
                  mailbox: session_mailbox.clone(), // Session's command mailbox
                  task_handle: session_task_handle.expect("ConnSuccess must provide Session task handle"),
                  endpoint_type: EndpointType::Session,
                  endpoint_uri: endpoint.clone(),
                  pipe_ids: Some((pipe_id_core_write, pipe_id_core_read)), // Store BOTH IDs
                };
                state.endpoints.insert(endpoint.clone(), info);
                drop(state); // Release lock

                // 4. Send AttachPipe command to Session's command mailbox
                let attach_pipe_cmd = Command::AttachPipe {
                  rx_from_core: rx_core_to_sess,    // Give Session the end it reads from
                  tx_to_core: tx_sess_to_core,      // Give Session the end it writes to
                  pipe_read_id: pipe_id_core_write, // Let Session know ID Core writes to
                  pipe_write_id: pipe_id_core_read, // Let Session know ID Core reads from
                };

                if session_mailbox.send(attach_pipe_cmd).await.is_err() {
                  tracing::error!(
                    handle = handle,
                    session_handle = session_handle_id,
                    "Failed to send AttachPipe to session. Cleaning up."
                  );

                  // Use the cleanup helper! Pass IDs directly.
                  Self::cleanup_session_state_by_pipe_ids(
                    core_arc.clone(),
                    pipe_id_core_write,
                    pipe_id_core_read,
                    &socket_logic_strong,
                  )
                  .await;
                } else {
                  // 5. Notify ISocket logic (only after pipe successfully attached to Session)
                  let peer_identity: Option<&[u8]> = None; // TODO
                  socket_logic_strong
                    .pipe_attached(
                      pipe_id_core_read,  // Read ID
                      pipe_id_core_write, // Write ID
                      peer_identity,
                    )
                    .await;
                }
              }
              Command::ConnFailed { endpoint, error } => {
                tracing::error!(handle = handle, %endpoint, error = %error, "Connection failed reported");
                // TODO: Cleanup pending state if any
              }
              Command::ReportError {
                handle: child_handle,
                endpoint_uri,
                error,
              } => {
                tracing::error!(handle = handle, child = child_handle, error = %error, "Child actor reported fatal error");

                // Use the cleanup helper which handles removing endpoint, pipes, and reader task
                // based on the URI, and also notifies ISocket.
                Self::cleanup_session_state_by_uri(core_arc.clone(), &endpoint_uri, &socket_logic_strong).await;

                // TODO: Decide if a child error should cause the whole SocketCore to shut down.
                // For now, we just clean up the failed endpoint.
                // if should_shutdown_on_child_error {
                //    Self::shutdown_logic(core_arc.clone()).await;
                //    should_break_loop = true;
                // }
              }
              Command::CleanupComplete {
                handle: child_handle,
                endpoint_uri,
              } => {
                tracing::debug!(
                  handle = handle,
                  child = child_handle,
                  uri = ?endpoint_uri, // Log URI if present
                  "Child actor cleanup complete reported"
                );
                // Usually no action needed if state removed on *Stopped/ReportError
              }
              #[cfg(feature = "inproc")]
              Command::InprocConnectRequest {
                connector_uri,
                connector_pipe_tx,       // Binder WRITES here (Connector reads from its paired rx)
                connector_pipe_rx,       // Binder READS here (Connector writes to its paired tx)
                connector_pipe_write_id, // ID Binder uses to write (Corresponds to connector's rx_binder_to_connector)
                connector_pipe_read_id,  // ID Binder uses to read (Corresponds to connector's tx_connector_to_binder)
                reply_tx,
              } => {
                tracing::debug!(binder_handle = handle, %connector_uri, "Handling InprocConnectRequest");
                let mut binder_state = core_arc.core_state.lock().await;

                // Store the writing end (binder writes to connector)
                binder_state.pipes_tx.insert(connector_pipe_write_id, connector_pipe_tx);

                // Spawn reader task for the reading end (binder reads from connector)
                let reader_task = tokio::spawn(Self::run_pipe_reader_task(
                  handle, // Binder's handle
                  core_arc.mailbox_sender().clone(),
                  connector_pipe_read_id, // ID binder reads from
                  connector_pipe_rx,      // Channel binder reads from
                ));
                binder_state
                  .pipe_reader_task_handles
                  .insert(connector_pipe_read_id, reader_task);

                // Store EndpointInfo for the connection in the *binder*
                // Mailbox: Maybe store connector's core mailbox if available? For now, use binder's own?
                // TaskHandle: No direct task handle for the connector's core. This field is problematic for inproc. Use dummy?
                let dummy_task_handle = tokio::spawn(async {}); // Placeholder needed for EndpointInfo struct
                let info = EndpointInfo {
                  mailbox: core_arc.mailbox_sender().clone(), // Using binder's mailbox as placeholder
                  task_handle: dummy_task_handle,             // Dummy handle
                  endpoint_type: EndpointType::Session,       // Treat inproc connection like a session
                  endpoint_uri: connector_uri.clone(),
                  pipe_ids: Some((connector_pipe_write_id, connector_pipe_read_id)), // Store binder's pipe IDs
                };
                binder_state.endpoints.insert(connector_uri.clone(), info);
                drop(binder_state); // Release lock

                // Notify ISocket logic about the new "pipe"
                socket_logic_strong
                  .pipe_attached(
                    connector_pipe_read_id,  // Binder reads this ID
                    connector_pipe_write_id, // Binder writes this ID
                    None,                    // Inproc has no ZMTP identity exchange
                  )
                  .await;

                tracing::info!(binder_handle = handle, %connector_uri, "Inproc connection accepted");
                // Send success reply back to the connector
                let _ = reply_tx.send(Ok(()));
              }

              #[cfg(feature = "inproc")]
              Command::InprocPipeClosed { pipe_read_id } => {
                // This command is sent BY the connector TO the binder when the connector disconnects.
                // `pipe_read_id` here is the ID the *BINDER* reads from.
                tracing::debug!(
                  binder_handle = handle,
                  pipe_id = pipe_read_id,
                  "Handling InprocPipeClosed"
                );
                // Use the standard cleanup helper, which uses the read ID
                Self::cleanup_session_state_by_pipe(core_arc.clone(), pipe_read_id, &socket_logic_strong).await;
              }
              Command::PipeMessageReceived { pipe_id, msg } => {
                tracing::trace!(
                  handle = handle,
                  pipe_id = pipe_id,
                  msg_size = msg.size(),
                  "Received message from pipe reader task"
                );
                let core_handle = core_arc.handle; // Use separate name for clarity if needed
                tracing::debug!(handle=core_handle, pipe_id=pipe_id, "Attempting to delegate PipeMessageReceived to ISocket::handle_pipe_event");
                // Delegate message to ISocket pattern logic via handle_pipe_event
                // Note: We need the *original* command enum structure here maybe?
                // Or a dedicated ISocket method `handle_pipe_message(pipe_id, msg)`?
                // Using handle_pipe_event for now requires reconstructing a Command variant... awkward.
                // Let's assume ISocket::handle_pipe_event handles PipeMessageReceived directly.
                let result = socket_logic_strong
                .handle_pipe_event(pipe_id, Command::PipeMessageReceived { pipe_id, msg })
                .await;
              
                tracing::debug!(handle=core_handle, pipe_id=pipe_id, "ISocket::handle_pipe_event returned: {:?}", result);
                if let Err(e) = result
                {
                  tracing::error!(
                    handle = handle,
                    pipe_id = pipe_id,
                    "Error handling PipeMessageReceived: {}",
                    e
                  );
                }
              }
              Command::PipeClosedByPeer {
                pipe_id: closed_pipe_read_id,
              } => {
                tracing::debug!(
                  handle = handle,
                  pipe_id = closed_pipe_read_id,
                  "Pipe closed by session peer"
                );
                // Use helper to clean up state based on the READ pipe ID
                Self::cleanup_session_state_by_pipe(core_arc.clone(), closed_pipe_read_id, &socket_logic_strong).await;
              }

              // --- Unhandled ---
              _ => {
                tracing::warn!(handle = handle, "Unhandled command in SocketCore loop: {:?}", command);
              }
            } // <--- END match command
          } // <--- END else block (socket_logic_strong is valid)
        } // <--- END Ok(command) arm
        Err(_) => {
          // <--- START Err arm (mailbox closed)
          // Mailbox closed
          tracing::info!(handle = handle, "Mailbox closed, initiating shutdown.");
          Self::shutdown_logic(core_arc.clone(), &socket_logic_strong).await;
          should_break_loop = true;
        } // <--- END Err arm
      } // <--- END match command_result

      if should_break_loop {
        break; // Exit loop
      }
    } // <--- END loop

    #[cfg(feature = "inproc")]
    {
      let bound_names = {
        // Scope for lock
        let mut state = core_arc.core_state.lock().await;
        std::mem::take(&mut state.bound_inproc_names) // Take ownership
      };
      if !bound_names.is_empty() {
        tracing::debug!(handle = handle, "Unregistering bound inproc names: {:?}", bound_names);
        let unbind_futures = bound_names.into_iter().map(|name| {
          let context_inner = core_arc.context.inner().clone(); // Clone Arc for async block
          async move {
            // Call directly to context for unregistration
            context_inner.unregister_inproc(&name).await;
          }
        });
        futures::future::join_all(unbind_futures).await;
      }
    }

    // --- Final Cleanup outside loop ---
    tracing::info!(handle = handle, "SocketCore run_command_loop finishing.");
    // Unregister from context *after* shutdown logic and loop exit
    let ctx_inner = core_arc.context.inner().clone();
    ctx_inner.unregister_socket(handle).await;
    tracing::info!(handle = handle, "SocketCore task fully stopped.");
  }

  /// Helper to clean up state associated with a session endpoint URI.
  async fn cleanup_session_state_by_uri(
    core_arc: Arc<SocketCore>,
    endpoint_uri: &str,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    let handle = core_arc.handle; // For logging
    tracing::debug!(handle=handle, uri=%endpoint_uri, "Attempting cleanup by URI");
    let mut state = core_arc.core_state.lock().await;

    // 1. Remove the endpoint info
    if let Some(removed_info) = state.endpoints.remove(endpoint_uri) {
      tracing::debug!(handle=handle, uri=%endpoint_uri, type=?removed_info.endpoint_type, "Removed endpoint state by URI");

      // 2. Abort the primary task handle for the endpoint (Listener, Session, Connecter?)
      // Avoid aborting self if called during shutdown? No, shutdown handles separately.
      removed_info.task_handle.abort(); // Ensure child task is stopped

      // 3. Clean up associated pipes and reader task if it was a session/inproc
      let mut detached_pipe_read_id: Option<usize> = None; // Store ID for ISocket notification
      if let Some((write_pipe_id, read_pipe_id)) = removed_info.pipe_ids {
        detached_pipe_read_id = Some(read_pipe_id); // Remember the read ID
        if state.remove_pipe_state(write_pipe_id, read_pipe_id) {
          tracing::debug!(handle=handle, uri=%endpoint_uri, read_pipe=read_pipe_id, write_pipe=write_pipe_id, "Removed associated pipe state");
        } else {
          // This might happen if cleanup called twice or state inconsistent
          tracing::warn!(handle=handle, uri=%endpoint_uri, read_pipe=read_pipe_id, write_pipe=write_pipe_id, "Cleanup by URI: Associated pipe state already removed?");
        }
      } else {
        // If no pipe IDs, it was likely a Listener that stopped/errored
        tracing::debug!(handle=handle, uri=%endpoint_uri, "No pipe state associated with removed endpoint (likely Listener)");
      }

      // 4. Release lock *before* notifying ISocket
      drop(state);

      // 5. Notify ISocket logic about pipe detachment *if* pipes were present
      if let Some(read_id) = detached_pipe_read_id {
        socket_logic.pipe_detached(read_id).await;
        tracing::debug!(handle=handle, uri=%endpoint_uri, pipe_read_id=read_id, "Notified ISocket of pipe detachment");
      }
    } else {
      // Endpoint info was already gone. Maybe cleanup called twice?
      tracing::warn!(handle=handle, uri=%endpoint_uri, "Cleanup by URI attempted, but endpoint state was not found (already cleaned up?)");
      // No need to drop lock here, it's already out of scope if 'if let' fails
    }
  }

  /// Helper to clean up state associated with a session pipe read ID.
  async fn cleanup_session_state_by_pipe(
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    let mut state = core_arc.core_state.lock().await;
    let mut endpoint_to_remove: Option<String> = None; // Store URI
    let mut write_pipe_id_to_remove = None;

    // Find endpoint URI and write pipe ID associated with the read pipe ID
    for (uri, info) in state.endpoints.iter() {
      if let Some((write_id, read_id)) = info.pipe_ids {
        if read_id == pipe_read_id {
          endpoint_to_remove = Some(uri.clone());
          write_pipe_id_to_remove = Some(write_id);
          break;
        }
      }
    }

    let write_pipe_id = match write_pipe_id_to_remove {
      Some(id) => id,
      None => {
        tracing::warn!(
          handle = core_arc.handle,
          pipe_id = pipe_read_id,
          "Cleanup by pipe: Couldn't find associated endpoint/write pipe ID"
        );
        // Still try to remove reader task if possible, as it's keyed by read_id
        if state.pipe_reader_task_handles.remove(&pipe_read_id).is_some() {
          // handle.abort(); // Abort handled in remove_pipe_state called below implicitly
          tracing::debug!(
            handle = core_arc.handle,
            pipe_id = pipe_read_id,
            "Removed dangling pipe reader task handle"
          );
        }
        drop(state);
        socket_logic.pipe_detached(pipe_read_id).await; // Still notify detached
        return;
      }
    };

    // Remove endpoint entry first if found
    if let Some(uri) = endpoint_to_remove {
      if state.endpoints.remove(&uri).is_some() {
        tracing::debug!(handle=core_arc.handle, pipe=pipe_read_id, uri=%uri, "Removed endpoint for closed pipe");
      }
    }

    // Remove pipe state using helper (uses write_id and read_id)
    let removed = state.remove_pipe_state(write_pipe_id, pipe_read_id);
    drop(state); // Release lock

    if removed {
      tracing::debug!(handle = core_arc.handle, pipe_id = pipe_read_id, "Removed pipe state");
    } else {
      tracing::warn!(
        handle = core_arc.handle,
        pipe_id = pipe_read_id,
        "Cleanup by pipe attempted, but no pipe state found"
      );
    }

    // Notify ISocket
    socket_logic.pipe_detached(pipe_read_id).await;
  }

  async fn cleanup_session_state_by_pipe_ids(
    core_arc: Arc<SocketCore>,
    pipe_write_id: usize,
    pipe_read_id: usize,
    _socket_logic: &Arc<dyn ISocket>,
  ) {
    let mut state = core_arc.core_state.lock().await;
    if state.remove_pipe_state(pipe_write_id, pipe_read_id) {
      tracing::debug!(
        "Cleaned up pipe state after AttachPipe failure for write_id={}, read_id={}",
        pipe_write_id,
        pipe_read_id
      );
      // Don't call pipe_detached here as pipe was never fully attached to ISocket logic
    }
  }

  /// Helper to contain the shutdown sequence logic.
  async fn shutdown_logic(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>, // Use passed-in strong ref
  ) {
    let handle = core_arc.handle; // Get handle early for logging if needed outside loop
    tracing::debug!(handle = handle, "Executing shutdown logic...");

    // 1. Stop child endpoint actors (Listeners, Sessions) & identify inproc connections
    let mut inproc_pipe_ids_to_cleanup: Vec<(usize, usize)> = Vec::new(); // (write_id, read_id)
    let mut endpoints_to_stop_tasks: Vec<(EndpointInfo, String)> = Vec::new(); // Store URI too

    {
      // Scope for state lock
      let mut state = core_arc.core_state.lock().await;
      let endpoints = std::mem::take(&mut state.endpoints); // Take ownership

      for (uri, info) in endpoints {
        // Check if it's an inproc endpoint based on URI scheme
        if uri.starts_with("inproc://") {
          if let Some(ids) = info.pipe_ids {
            tracing::debug!(handle=handle, uri=%uri, "Identified inproc endpoint for specific cleanup");
            // Don't send Stop, don't await dummy handle. Just record pipes.
            inproc_pipe_ids_to_cleanup.push(ids);
          } else {
            tracing::warn!(handle=handle, uri=%uri, "Inproc endpoint missing pipe IDs during shutdown");
          }
        } else {
          // Regular TCP/IPC endpoint
          endpoints_to_stop_tasks.push((info, uri));
        }
      }
    } // state lock released

    // Stop TCP/IPC child actors
    if !endpoints_to_stop_tasks.is_empty() {
      let stop_futures = endpoints_to_stop_tasks.into_iter().map(|(info, uri)| { // Use URI from tuple
            let core_arc_clone = core_arc.clone();
            async move {
                tracing::debug!(parent_handle=core_arc_clone.handle, child_uri=%uri, child_type=?info.endpoint_type, "Sending Stop to child actor");
                let _ = info.mailbox.send(Command::Stop).await;
                // Await child task completion - Use a timeout? Shutdown shouldn't block indefinitely.
                match tokio::time::timeout(Duration::from_secs(2), info.task_handle).await { // Example 2s timeout
                     Ok(Ok(())) => tracing::debug!(parent_handle=core_arc_clone.handle, child_uri=%uri, "Child task joined successfully"),
                     Ok(Err(e)) => tracing::error!(parent_handle=core_arc_clone.handle, child_uri=%uri, "Child task join error (panic?): {:?}", e),
                     Err(_) => tracing::warn!(parent_handle=core_arc_clone.handle, child_uri=%uri, "Child task join timed out"),
                }
            }
        });
      futures::future::join_all(stop_futures).await;
      tracing::debug!(handle = handle, "Non-inproc endpoint actors stopped/timed out.");
    } else {
      tracing::debug!(handle = handle, "No non-inproc endpoint actors to stop.");
    }

    // 2. Apply Linger
    // Check linger option *before* forcefully closing/aborting pipes/readers
    let mut linger_duration_opt = { core_arc.core_state.lock().await.options.linger };
    let linger_start_time = Instant::now();
    let mut linger_timeout: Option<Duration> = None; // Timeout only for positive linger

    match linger_duration_opt {
      None => {
        // Linger = -1 (Infinite)
        tracing::debug!(
          handle = handle,
          "Starting infinite linger (waiting for queues to drain)..."
        );
        // Timeout is None, loop potentially forever
      }
      Some(duration) if duration.is_zero() => {
        // Linger = 0 (Immediate close)
        tracing::debug!(handle = handle, "Linger is zero, skipping queue drain wait.");
        // Don't loop, proceed directly to closing pipes
        linger_duration_opt = None; // Signal to skip the loop below
      }
      Some(duration) => {
        // Linger > 0 (Timed wait)
        tracing::debug!(
          handle = handle,
          ?duration,
          "Starting timed linger (waiting for queues to drain)..."
        );
        linger_timeout = Some(duration); // Set the timeout for the loop
      }
    }

    // --- Loop to wait for queue draining (only if linger != 0) ---
    if linger_duration_opt.map_or(true, |d| !d.is_zero()) {
      // Execute loop if linger is None (-1) or Some(>0)
      loop {
        // Check timeout if applicable
        if let Some(timeout_duration) = linger_timeout {
          if linger_start_time.elapsed() >= timeout_duration {
            tracing::warn!(
              handle = handle,
              ?timeout_duration,
              "Linger timeout reached. Proceeding with shutdown."
            );
            break; // Exit loop due to timeout
          }
        }

        // Check if all outbound pipes are empty
        let all_empty = {
          // Scope for lock
          let state = core_arc.core_state.lock().await;
          if state.pipes_tx.is_empty() {
            true // No pipes left to drain
          } else {
            // Check length of each sender channel
            state.pipes_tx.values().all(|sender| sender.len() == 0)
          }
        };

        if all_empty {
          tracing::debug!(handle = handle, "All outbound pipe queues drained. Linger complete.");
          break; // Exit loop, queues are empty
        }

        // Queues not empty, yield/sleep briefly before checking again
        tokio::time::sleep(Duration::from_millis(10)).await; // Adjust sleep duration as needed
      } // end loop
    } // end if linger != 0

    // 3. Close owned pipe senders (core -> session) & Abort pipe reader tasks
    tracing::debug!(handle = handle, "Proceeding to close pipes and abort readers.");
    let pipes_tx_to_close;
    let mut reader_tasks_to_abort; // Make mutable
    {
      // Scope for state lock
      let mut state = core_arc.core_state.lock().await;
      pipes_tx_to_close = std::mem::take(&mut state.pipes_tx);
      reader_tasks_to_abort = std::mem::take(&mut state.pipe_reader_task_handles);

      // Also remove the pipe state we identified for inproc earlier,
      // ensuring it doesn't get processed generically if somehow still present.
      // This might be redundant if the main pipe closing handles it, but safer.
      for (write_id, read_id) in &inproc_pipe_ids_to_cleanup {
        state.pipes_tx.remove(write_id);
        state.pipe_reader_task_handles.remove(read_id);
      }
    } // state lock released

    // Close senders (signals EOF to corresponding Session/Peer reader loops)
    for (id, sender) in pipes_tx_to_close {
      sender.close(); // Close channel sender end
      tracing::trace!(handle = handle, pipe_id = id, "Closed pipe sender during shutdown");
    }
    tracing::debug!(handle = handle, "All pipe senders closed.");

    // Abort ALL reader tasks forcefully (including those associated with inproc)
    if !reader_tasks_to_abort.is_empty() {
      let abort_futures = reader_tasks_to_abort.into_iter().map(|(id, handle)| {
        let core_arc_clone = core_arc.clone(); // Use clone for task
        async move {
          tracing::debug!(
            core_handle = core_arc_clone.handle, // Use cloned handle
            pipe_id = id,
            "Aborting pipe reader task during shutdown"
          );
          handle.abort();
          let _ = handle.await; // Await aborted task to ensure it cleans up? Optional.
        }
      });
      futures::future::join_all(abort_futures).await;
      tracing::debug!(handle = handle, "All pipe reader tasks aborted/joined.");
    } else {
      tracing::debug!(handle = handle, "No pipe reader tasks to abort.");
    }

    // *** Notify ISocket for detached inproc pipes ***
    // <<< MODIFIED START: Use passed-in socket_logic reference directly >>>
    if !inproc_pipe_ids_to_cleanup.is_empty() {
      for (_write_id, read_id) in inproc_pipe_ids_to_cleanup {
        tracing::debug!(
          handle = handle,
          pipe_read_id = read_id,
          "Notifying ISocket of inproc pipe detachment during shutdown"
        );
        socket_logic.pipe_detached(read_id).await;
      }
    }
    // <<< MODIFIED END >>>

    tracing::info!(handle = handle, "Shutdown logic complete.");
  }

  /// Handles setting socket options. Modifies CoreState or delegates.
  async fn handle_set_option(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>, // Pass strong ref
    option: i32,
    value: &[u8],
  ) -> Result<(), ZmqError> {
    tracing::debug!(
      handle = core_arc.handle,
      option = option,
      value_len = value.len(),
      "Setting option"
    );

    match option {
      options::SUBSCRIBE | options::UNSUBSCRIBE | options::ROUTING_ID => {
        // Add other pattern-specific options here
        // Attempt delegation first
        match socket_logic.set_pattern_option(option, value).await {
          Ok(()) => return Ok(()), // Pattern handled it
          Err(ZmqError::UnsupportedOption(_)) => { /* Fall through to core handling */ }
          Err(e) => return Err(e), // Pattern encountered another error
        }
      }
      _ => { /* Option not typically handled by pattern, proceed to core */ }
    }

    let mut state = core_arc.core_state.lock().await; // Lock state for modification

    match option {
      // --- Core Options ---
      options::SNDHWM => {
        let hwm = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        if hwm < 0 {
          return Err(ZmqError::InvalidOptionValue(option));
        }
        state.options.sndhwm = hwm as usize;
        // NOTE: No change applied to existing pipes_tx channel capacities.
        tracing::info!(
          handle = core_arc.handle,
          hwm = state.options.sndhwm,
          "Set SNDHWM (affects new connections)"
        );
      }
      options::RCVHWM => {
        let hwm = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        if hwm < 0 {
          return Err(ZmqError::InvalidOptionValue(option));
        }
        state.options.rcvhwm = hwm as usize;
        // NOTE: No change applied to existing FairQueues or pipe reader channel capacities.
        // TODO: Resizing FairQueue in ISocket impls would be complex.
        tracing::info!(
          handle = core_arc.handle,
          hwm = state.options.rcvhwm,
          "Set RCVHWM (affects new connections)"
        );
      }
      options::LINGER => {
        state.options.linger = parse_linger_option(value)?; // Use Option<Duration> parser
        tracing::info!(handle=core_arc.handle, linger=?state.options.linger, "Set LINGER");
      }

      options::HEARTBEAT_IVL => {
        state.options.heartbeat_ivl = parse_heartbeat_option(value, option)?;
        tracing::info!(handle=core_arc.handle, ivl=?state.options.heartbeat_ivl, "Set HEARTBEAT_IVL");
      }
      options::HEARTBEAT_TIMEOUT => {
        state.options.heartbeat_timeout = parse_heartbeat_option(value, option)?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.heartbeat_timeout, "Set HEARTBEAT_TIMEOUT");
      }
      options::ZAP_DOMAIN => {
        // TODO: Validate domain string? ZMQ limits?
        state.options.zap_domain =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle=core_arc.handle, domain=?state.options.zap_domain, "Set ZAP_DOMAIN");
      }
      options::PLAIN_SERVER => {
        state.options.plain_server = Some(parse_bool_option(value)?);
        tracing::info!(handle=core_arc.handle, server=?state.options.plain_server, "Set PLAIN_SERVER");
      }
      options::PLAIN_USERNAME => {
        // TODO: Validate username? UTF8? Length?
        state.options.plain_username =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set PLAIN_USERNAME (length {})", value.len());
      }
      options::PLAIN_PASSWORD => {
        // TODO: Secure handling! This stores in memory.
        state.options.plain_password =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set PLAIN_PASSWORD (length {})", value.len());
      }
      #[cfg(feature = "curve")]
      options::CURVE_SERVER => {
        state.options.curve_server = Some(parse_bool_option(value)?);
        tracing::info!(handle=core_arc.handle, server=?state.options.curve_server, "Set CURVE_SERVER");
      }
      #[cfg(feature = "curve")]
      options::CURVE_PUBLICKEY => {
        state.options.curve_public_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_PUBLICKEY");
      }
      #[cfg(feature = "curve")]
      options::CURVE_SECRETKEY => {
        // TODO: Secure handling!
        state.options.curve_secret_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_SECRETKEY");
      }
      #[cfg(feature = "curve")]
      options::CURVE_SERVERKEY => {
        state.options.curve_server_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_SERVERKEY");
      }
      options::ROUTING_ID => {
        // Core might still store it even if pattern uses it
        state.options.routing_id = Some(parse_blob_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set ROUTING_ID (core state)");
      }
      options::RCVTIMEO => {
        state.options.rcvtimeo =
          parse_timeout_option(value, option).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.rcvtimeo, "Set RCVTIMEO");
      }
      options::SNDTIMEO => {
        state.options.sndtimeo =
          parse_timeout_option(value, option).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.sndtimeo, "Set SNDTIMEO");
      }

      // --- TCP Options (Store, applied by Engine on connection) ---
      options::TCP_KEEPALIVE => {
        state.options.tcp_keepalive_enabled =
          parse_keepalive_mode_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(
          handle = core_arc.handle,
          mode = state.options.tcp_keepalive_enabled,
          "Set TCP_KEEPALIVE"
        );
      }
      options::TCP_KEEPALIVE_IDLE => {
        state.options.tcp_keepalive_idle =
          parse_secs_duration_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, duration=?state.options.tcp_keepalive_idle, "Set TCP_KEEPALIVE_IDLE");
      }
      options::TCP_KEEPALIVE_CNT => {
        state.options.tcp_keepalive_count =
          parse_u32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, count=?state.options.tcp_keepalive_count, "Set TCP_KEEPALIVE_CNT");
      }
      options::TCP_KEEPALIVE_INTVL => {
        state.options.tcp_keepalive_interval =
          parse_secs_duration_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, duration=?state.options.tcp_keepalive_interval, "Set TCP_KEEPALIVE_INTVL");
      }
      options::ROUTER_MANDATORY => {
        // This option applies specifically to ROUTER behavior
        // We store it here, ROUTER ISocket impl will read it.
        state.options.router_mandatory = parse_bool_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, mandatory=?state.options.router_mandatory, "Set ROUTER_MANDATORY");
      }

      // --- Options Delegated to ISocket Pattern Logic ---
      options::SUBSCRIBE | options::UNSUBSCRIBE => {
        // Release lock before calling await on pattern logic
        drop(state);
        // Use a dummy command structure for delegation if process_command handles it,
        // or call a dedicated method on ISocket. Let's assume dedicated method is cleaner.
        // socket_logic.set_subscription(option, value).await?;
        // For now, use process_command as defined in trait:
        let cmd = Command::UserSetOpt {
          option,
          value: value.to_vec(),          // Clone value for command
          reply_tx: oneshot::channel().0, // Dummy reply needed for command structure
        };
        match socket_logic.process_command(cmd).await {
          Ok(true) => { /* Handled by pattern */ }
          Ok(false) => return Err(ZmqError::UnsupportedOption(option)), // Core doesn't handle it
          Err(e) => return Err(e),
        }
      }

      // --- Other Options (Security, etc.) ---
      // TODO: Handle security options (store keys, flags)
      // TODO: Handle transport optimization flags (TCP_CORK etc.)
      _ => {
        tracing::warn!(
          handle = core_arc.handle,
          option = option,
          "Attempted to set unsupported option"
        );
        return Err(ZmqError::UnsupportedOption(option));
      }
    }

    Ok(())
  }

  /// Handles getting socket option values. Reads from CoreState or delegates.
  async fn handle_get_option(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    option: i32,
  ) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(handle = core_arc.handle, option = option, "Getting option");

    match option {
      options::ROUTING_ID => {
        // Example: ROUTING_ID might be primarily managed by pattern
        match socket_logic.get_pattern_option(option).await {
          Ok(v) => return Ok(v), // Pattern handled it
          Err(ZmqError::UnsupportedOption(_)) => { /* Fall through */ }
          Err(e) => return Err(e),
        }
      }
      // Add other options primarily handled by patterns here
      _ => {}
    }

    let state = core_arc.core_state.lock().await; // Lock state for reading

    match option {
      options::SNDHWM => Ok((state.options.sndhwm as i32).to_ne_bytes().to_vec()),
      options::RCVHWM => Ok((state.options.rcvhwm as i32).to_ne_bytes().to_vec()),
      options::LINGER => {
        let linger_ms_i32 = match state.options.linger {
          None => -1,                                              // Infinite linger
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX), // Convert ms to i32, saturate on overflow
        };
        Ok(linger_ms_i32.to_ne_bytes().to_vec())
      }
      options::HEARTBEAT_IVL => {
        let ivl_ms_i32 = state.options.heartbeat_ivl.map_or(0, |d| d.as_millis() as i32);
        Ok(ivl_ms_i32.to_ne_bytes().to_vec())
      }
      options::HEARTBEAT_TIMEOUT => {
        let timeout_ms_i32 = state.options.heartbeat_timeout.map_or(0, |d| d.as_millis() as i32);
        Ok(timeout_ms_i32.to_ne_bytes().to_vec())
      }
      // Note: We generally DON'T allow reading back sensitive options like passwords or secret keys.
      options::ZAP_DOMAIN => state
        .options
        .zap_domain
        .as_ref()
        .map(|s| s.as_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("ZAP_DOMAIN not set".into())),
      options::PLAIN_SERVER => state
        .options
        .plain_server
        .map(|b| (b as i32).to_ne_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("PLAIN_SERVER not set".into())), // Or return default?
      options::PLAIN_USERNAME => state
        .options
        .plain_username
        .as_ref()
        .map(|s| s.as_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("PLAIN_USERNAME not set".into())),
      // PLAIN_PASSWORD - Not readable for security
      #[cfg(feature = "curve")]
      options::CURVE_SERVER => state
        .options
        .curve_server
        .map(|b| (b as i32).to_ne_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_SERVER not set".into())), // Or return default?
      #[cfg(feature = "curve")]
      options::CURVE_PUBLICKEY => state
        .options
        .curve_public_key
        .map(|k| k.to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_PUBLICKEY not set".into())),
      #[cfg(feature = "curve")]
      // CURVE_SECRETKEY - Not readable for security
      options::CURVE_SERVERKEY => state
        .options
        .curve_server_key
        .map(|k| k.to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_SERVERKEY not set".into())),
      options::ROUTING_ID => state
        .options
        .routing_id
        .as_ref() // Core fallback if pattern didn't handle
        .map(|blob| blob.to_vec())
        .ok_or(ZmqError::InvalidState("ROUTING_ID not set".into())), // Return error if not set?
      options::RCVTIMEO => {
        let timeo_ms_i32 = match state.options.rcvtimeo {
          None => -1,
          Some(d) if d.is_zero() => 0,
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX), // Saturate
        };
        Ok(timeo_ms_i32.to_ne_bytes().to_vec())
      }
      options::SNDTIMEO => {
        let timeo_ms_i32 = match state.options.sndtimeo {
          None => -1,
          Some(d) if d.is_zero() => 0,
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX),
        };
        Ok(timeo_ms_i32.to_ne_bytes().to_vec())
      }
      options::TCP_KEEPALIVE => Ok(state.options.tcp_keepalive_enabled.to_ne_bytes().to_vec()),
      options::TCP_KEEPALIVE_IDLE => Ok(
        state
          .options
          .tcp_keepalive_idle
          .map_or(0, |d| d.as_secs() as i32)
          .to_ne_bytes()
          .to_vec(),
      ), // 0 if None? Check ZMQ default
      options::TCP_KEEPALIVE_CNT => Ok(
        state
          .options
          .tcp_keepalive_count
          .map_or(0, |c| c as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      options::TCP_KEEPALIVE_INTVL => Ok(
        state
          .options
          .tcp_keepalive_interval
          .map_or(0, |d| d.as_secs() as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      options::ROUTER_MANDATORY => Ok((state.options.router_mandatory as i32).to_ne_bytes().to_vec()),
      // --- Options Delegated to ISocket Pattern Logic ---
      // Example: Getting current subscriptions? ZMQ doesn't really support this.
      // Options like TYPE are handled here though.
      // ZMQ_TYPE = 16
      16 => Ok((state.socket_type as i32).to_ne_bytes().to_vec()), // Assuming SocketType mirrors ZMQ int values

      _ => {
        // Try delegating to pattern logic? Some options might be readable there.
        drop(state); // Release lock
                     // let cmd = Command::UserGetOpt { option, reply_tx: oneshot::channel().0 }; // Dummy tx
                     // match socket_logic.process_command(cmd).await { ... }
                     // Or call dedicated get method:
                     // socket_logic.get_pattern_option(option).await
        tracing::warn!(
          handle = core_arc.handle,
          option = option,
          "Attempted to get unsupported option"
        );
        Err(ZmqError::UnsupportedOption(option))
      }
    }
  }
}

/// Helper to send a message on a pipe channel respecting SNDTIMEO.
pub(crate) async fn send_msg_with_timeout(
  pipe_tx: &AsyncSender<Msg>,    // Borrow sender
  msg: Msg,                      // Take ownership of message
  timeout_opt: Option<Duration>, // SNDTIMEO value
  socket_handle: usize,          // For logging
  pipe_id: usize,                // For logging
) -> Result<(), ZmqError> {
  match timeout_opt {
    None => {
      // Infinite timeout (-1), block indefinitely
      tracing::trace!(handle = socket_handle, pipe_id = pipe_id, "Sending message (blocking)");
      pipe_tx
        .send(msg)
        .await
        .map_err(|SendError(_)| ZmqError::ConnectionClosed) // Map closed error
    }
    Some(duration) if duration.is_zero() => {
      // Zero timeout (0), non-blocking attempt
      tracing::trace!(
        handle = socket_handle,
        pipe_id = pipe_id,
        "Attempting non-blocking send"
      );
      match pipe_tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached), // EAGAIN
        Err(TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
      }
    }
    Some(duration) => {
      // Positive timeout (> 0)
      tracing::trace!(
        handle = socket_handle,
        pipe_id = pipe_id,
        ?duration,
        "Attempting timed send"
      );
      match timeout(duration, pipe_tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),                                     // Sent within timeout
        Ok(Err(SendError(_))) => Err(ZmqError::ConnectionClosed), // Closed during await
        Err(_) => Err(ZmqError::Timeout),                         // Timeout elapsed
      }
    }
  }
}
