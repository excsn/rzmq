// src/socket/core.rs

use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase;
// Use our types
use crate::socket::core::dummy_sockets::*;
use crate::socket::options::{
  self, parse_blob_option, parse_duration_ms_option, parse_i32_option, parse_keepalive_mode_option,
  parse_linger_option, parse_secs_duration_option, parse_u32_option, SocketOptions, ZmtpTcpConfig,
}; // Will define SocketOptions later
use crate::socket::patterns;
use crate::socket::types::SocketType;
use crate::socket::ISocket;
use crate::transport::tcp::{TcpConnecter, TcpListener};
use crate::Msg;
// use crate::socket::{
//   dealer_socket::DealerSocket, pub_socket::PubSocket, pull_socket::PullSocket,
//   push_socket::PushSocket, rep_socket::RepSocket, req_socket::ReqSocket,
//   router_socket::RouterSocket, sub_socket::SubSocket,
// };

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_channel::{Receiver as AsyncReceiver, Sender as AsyncSender};
use tokio::sync::{oneshot, Mutex}; // Use Tokio's Mutex for CoreState
use tokio::task::JoinHandle;

use super::pull_socket::PullSocket;
use super::push_socket::PushSocket;

// Information stored for each active endpoint (listener or connection)
#[derive(Debug)]
pub(crate) struct EndpointInfo {
  pub mailbox: MailboxSender, // Mailbox of the child actor (Listener/Connecter/Session)
  pub task_handle: JoinHandle<()>, // Task handle of the child actor
  pub endpoint_type: EndpointType, // Listener or Session (Connecter transitions to Session)
  pub endpoint_uri: String,   // The normalized endpoint string
  pub pipe_ids: Option<(usize, usize)>, // (ID for core->sess chan, ID for sess->core chan)
                              // <<< ADDED END >>>
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
  // Map pipe ID -> Receiver for messages Core receives FROM Sessions/Inproc
  // Note: Receivers usually need to be polled concurrently. Storing them directly
  // in a HashMap makes polling complex in the core loop.
  // Alternative: Spawn a dedicated task per receiver that sends received messages
  // as Commands back to the core mailbox? Or use a StreamMux?
  // Let's keep the receivers for now, but acknowledge polling complexity.
  pub pipes_rx: HashMap<usize, AsyncReceiver<Msg>>,

  // Map pipe READ ID -> Info about the task reading from that pipe
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,

  /// Map of active listeners or connected sessions.
  /// Key is the normalized endpoint URI string.
  pub endpoints: HashMap<String, EndpointInfo>,
  // --- State relevant for pattern logic / message handling ---
  // pub rcvhwm_reached: bool, // Example state flag
  // pub sndhwm_reached: bool, // Example state flag
  // Add other state as needed by ISocket impls or core logic
}

impl CoreState {
  fn new(socket_type: SocketType, options: SocketOptions) -> Self {
    Self {
      options,
      socket_type,
      pipes_tx: HashMap::new(),
      pipes_rx: HashMap::new(),
      pipe_reader_task_handles: HashMap::new(),
      endpoints: HashMap::new(),
    }
  }
}
// <<< ADDED CoreState STRUCT END >>>

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
  /// Creates the SocketCore actor, the specific ISocket pattern implementation,
  /// spawns the core command loop task, and returns the public ISocket handle
  /// and the mailbox sender for the core actor.
  pub(crate) fn create_and_spawn(
    handle: usize,
    context: Context,
    socket_type: SocketType,
    initial_options: SocketOptions,
  ) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
    let (mailbox_sender, mailbox_receiver) = mailbox();

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
      SocketType::Sub => Arc::new(SubSocket::new(core_arc.clone())),
      SocketType::Req => Arc::new(ReqSocket::new(core_arc.clone())),
      SocketType::Rep => Arc::new(RepSocket::new(core_arc.clone())),
      SocketType::Dealer => Arc::new(DealerSocket::new(core_arc.clone())),
      SocketType::Router => Arc::new(RouterSocket::new(core_arc.clone())),
      SocketType::Push => Arc::new(PushSocket::new(core_arc.clone())),
      SocketType::Pull => Arc::new(PullSocket::new(core_arc.clone())),
    };

    // 3. Create and store the Weak reference back into SocketCore
    let weak_isocket = Arc::downgrade(&socket_logic_arc);
    // Use try_lock: should be uncontended here
    if let Ok(mut socket_logic_guard) = core_arc.socket_logic.try_lock() {
      *socket_logic_guard = Some(weak_isocket);
    } else {
      tracing::error!(
        handle = handle,
        "Failed to lock socket_logic mutex during init"
      );
      return Err(ZmqError::Internal(
        "Failed to initialize socket weak reference".into(),
      ));
    }

    // 4. Spawn the main command loop task
    let task_core_ref = core_arc.clone(); // Clone Arc for the task
    let task_handle = tokio::spawn(SocketCore::run_command_loop(task_core_ref));

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
      return Err(ZmqError::Internal(
        "Failed to store socket task handle".into(),
      ));
    }

    // 6. Return the public handle and mailbox sender
    Ok((socket_logic_arc, mailbox_sender))
  }

  /// Task function executed by tokio::spawn for each incoming pipe receiver.
  /// Reads messages from the session and sends them to the SocketCore's mailbox.
  async fn run_pipe_reader_task(
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

  async fn run_command_loop(core_arc: Arc<SocketCore>) {
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

          let socket_logic_strong = {
            // Define scope for guard
            let guard = core_arc.socket_logic.lock().await;
            match guard.as_ref().and_then(|weak_ref| weak_ref.upgrade()) {
              Some(s) => s, // Got the strong reference
              None => {
                // Weak ref invalid - log and signal shutdown
                let is_none = guard.is_none(); // Check why it failed within the lock
                if is_none {
                  tracing::error!(
                    handle = handle,
                    "ISocket logic weak reference was never set! Shutting down."
                  );
                } else {
                  tracing::warn!(
                    handle = handle,
                    "ISocket logic weak reference expired. Shutting down."
                  );
                }
                drop(guard); // Release lock before calling shutdown_logic
                Self::shutdown_logic(core_arc.clone()).await;
                should_break_loop = true;
                // Use continue to skip the rest of the Ok(command) arm for this iteration
                continue;
              }
            }
            // guard is dropped here
          };

          // If socket_logic is invalid, skip command processing
          if !should_break_loop {
            // socket_logic_strong is valid, proceed
            // --- Command Handling Logic ---
            match command {
              // <--- START match command
              // --- Stop / Close ---
              Command::Stop => {
                tracing::info!(
                  handle = handle,
                  "Stop command received, initiating shutdown."
                );
                Self::shutdown_logic(core_arc.clone()).await;
                should_break_loop = true;
              }
              Command::UserClose { reply_tx } => {
                tracing::info!(handle = handle, "UserClose received, initiating shutdown.");
                Self::shutdown_logic(core_arc.clone()).await;
                let _ = reply_tx.send(Ok(()));
                should_break_loop = true;
              }

              // --- User Commands (Core Handling / Spawning) ---
              Command::UserBind { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserBind");

                // 1. Parse and Validate Endpoint (Placeholder for now)
                // let parsed_endpoint = crate::transport::endpoint::parse_endpoint(&endpoint)?;
                // TODO: Replace basic check with proper parsing and scheme check
                let result = if endpoint.starts_with("tcp://")
                  || endpoint.starts_with("ipc://")
                  || endpoint.starts_with("inproc://")
                {
                  // 2. Check if already bound to this endpoint
                  let mut state = core_arc.core_state.lock().await;
                  if state.endpoints.contains_key(&endpoint) {
                    Err(ZmqError::AddrInUse(endpoint.clone()))
                  } else {
                    // 3. Prepare Config (Derive from current options)
                    // TODO: Create helper function to derive ZmtpTcpConfig/ZmtpIpcConfig from SocketOptions
                    let config = ZmtpTcpConfig::default(); // Placeholder

                    // 4. Generate Handle for child actor
                    let child_handle = core_arc.context.inner().next_handle();
                    let handle_source = core_arc.context.inner().next_handle.clone(); // Clone Arc<AtomicUsize>

                    // 5. Spawn Appropriate Listener Actor based on scheme
                    let spawn_result = if endpoint.starts_with("tcp://") {
                      // --- Spawn TCP Listener ---
                      TcpListener::create_and_spawn(
                        child_handle,
                        endpoint.clone(),
                        config,                            // Pass TCP specific config
                        core_arc.mailbox_sender().clone(), // Clone mailbox sender
                        handle_source,
                      )
                      // map Result<(Sender, JoinHandle), Error> -> Result<(Sender, JoinHandle), Error>
                    } else if endpoint.starts_with("ipc://") {
                      // --- Spawn IPC Listener ---
                      // Need #[cfg(feature = "ipc")]
                      // IpcListener::create_and_spawn(...)
                      Err(ZmqError::UnsupportedTransport(
                        "IPC not implemented yet".into(),
                      )) // Placeholder
                    } else if endpoint.starts_with("inproc://") {
                      // --- Handle Inproc Bind ---
                      // Inproc bind registers directly in ContextInner, no separate actor needed usually
                      // Needs async block to call async register_inproc
                      // TODO: Implement inproc logic
                      Err(ZmqError::UnsupportedTransport(
                        "Inproc not implemented yet".into(),
                      )) // Placeholder
                    } else {
                      // Should be caught by initial check, but defensively handle
                      unreachable!("Invalid scheme passed initial check")
                    }; // End spawn_result determination

                    // 6. Store EndpointInfo if spawning succeeded
                    match spawn_result {
                      Ok((listener_mailbox, listener_task)) => {
                        let info = EndpointInfo {
                          mailbox: listener_mailbox,
                          task_handle: listener_task,
                          endpoint_type: EndpointType::Listener, // Correct type
                          endpoint_uri: endpoint.clone(),
                          pipe_ids: None, // Listeners don't have direct pipes
                        };
                        state.endpoints.insert(endpoint.clone(), info);
                        tracing::info!(handle=handle, endpoint=%endpoint, child_handle=child_handle, "Spawned Listener actor");
                        Ok(()) // Signal success of *initiating* bind
                      }
                      Err(e) => {
                        tracing::error!(handle=handle, endpoint=%endpoint, "Failed to spawn Listener actor: {}", e);
                        Err(e) // Propagate spawn error
                      }
                    } // End match spawn_result
                  } // End else block (not already bound)
                } else {
                  // End if valid scheme
                  Err(ZmqError::UnsupportedTransport(endpoint.clone()))
                }; // End result determination

                // 7. Send Reply back to user
                let _ = reply_tx.send(result); // Ignore error if user task already gone
              } // End UserBind arm

              Command::UserConnect { endpoint, reply_tx } => {
                tracing::debug!(handle = handle, %endpoint, "Handling UserConnect");

                // 1. Parse and Validate Endpoint
                // TODO: Replace basic check with proper parsing
                let result = if endpoint.starts_with("tcp://")
                  || endpoint.starts_with("ipc://")
                  || endpoint.starts_with("inproc://")
                {
                  // 2. Check if already connected? Depends on socket type.
                  // Some types allow multiple connects (DEALER, PUSH), others replace (REQ).
                  // For now, allow multiple connect attempts, let connecter handle details.
                  let state = core_arc.core_state.lock().await; // Read lock might suffice?

                  // 3. Prepare Config
                  // TODO: Derive config from state.options
                  let config = ZmtpTcpConfig::default(); // Placeholder

                  // 4. Generate Handle for child actor
                  let child_handle = core_arc.context.inner().next_handle();
                  let handle_source = core_arc.context.inner().next_handle.clone();

                  // 5. Spawn Appropriate Connecter Actor based on scheme
                  let spawn_result = if endpoint.starts_with("tcp://") {
                    // --- Spawn TCP Connecter ---
                    Ok(TcpConnecter::create_and_spawn(
                      child_handle,
                      endpoint.clone(),
                      config, // Pass TCP specific config
                      core_arc.mailbox_sender().clone(),
                      handle_source,
                    ))
                  } else if endpoint.starts_with("ipc://") {
                    // --- Spawn IPC Connecter ---
                    // #[cfg(feature = "ipc")]
                    // IpcConnecter::create_and_spawn(...)
                    Err(ZmqError::UnsupportedTransport(
                      "IPC not implemented yet".into(),
                    ))
                  } else if endpoint.starts_with("inproc://") {
                    // --- Handle Inproc Connect ---
                    // Inproc connect finds binder in registry and creates pipes directly.
                    // Needs async block to call async lookup_inproc / pipepair logic.
                    // TODO: Implement inproc logic
                    Err(ZmqError::UnsupportedTransport(
                      "Inproc not implemented yet".into(),
                    )) // Placeholder
                  } else {
                    unreachable!("Invalid scheme passed initial check")
                  }; // End spawn_result determination

                  // 6. Handle Spawn Result
                  match spawn_result {
                    // Note: We don't store connecter state directly in endpoints map usually.
                    // We wait for ConnSuccess/ConnFailed report from the spawned connecter task.
                    Ok((_connecter_mailbox, _connecter_task)) => {
                      tracing::info!(handle=handle, endpoint=%endpoint, child_handle=child_handle, "Spawned Connecter actor");
                      // Don't store _connecter_mailbox/_connecter_task here unless needed
                      // to explicitly stop pending connect attempts via UserDisconnect. TBD.
                      Ok(()) // Signal success of *initiating* connect
                    }
                    Err(e) => {
                      tracing::error!(handle=handle, endpoint=%endpoint, "Failed to spawn Connecter actor: {}", e);
                      Err(e)
                    }
                  } // End match spawn_result
                } else {
                  // End if valid scheme
                  Err(ZmqError::UnsupportedTransport(endpoint.clone()))
                }; // End result determination

                // 7. Send Reply back to user
                let _ = reply_tx.send(result);
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
                let result =
                  Self::handle_set_option(core_arc.clone(), &socket_logic_strong, option, &value)
                    .await;
                let _ = reply_tx.send(result);
              }
              Command::UserGetOpt { option, reply_tx } => {
                let result =
                  Self::handle_get_option(core_arc.clone(), &socket_logic_strong, option).await;
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
                let session_handle_id =
                  session_handle.unwrap_or_else(|| core_arc.context.inner().next_handle());

                // 1. Create Pipe Channels
                let mut state = core_arc.core_state.lock().await;
                let pipe_id_core_write = core_arc.context.inner().next_handle();
                let pipe_id_core_read = pipe_id_core_write + 1;
                let hwm = state.options.rcvhwm.max(state.options.sndhwm).max(1000);
                let (tx_core_to_sess, rx_core_to_sess) = async_channel::bounded::<Msg>(hwm);
                let (tx_sess_to_core, rx_sess_to_core) = async_channel::bounded::<Msg>(hwm);

                let mut state = core_arc.core_state.lock().await;

                // 2. Store Core's Ends & Spawn Reader Task
                state
                  .pipes_tx
                  .insert(pipe_id_core_write, tx_core_to_sess.clone()); // Clone Sender for AttachPipe
                let reader_task = tokio::spawn(Self::run_pipe_reader_task(
                  core_arc.handle,
                  core_arc.mailbox_sender().clone(),
                  pipe_id_core_read,
                  rx_sess_to_core, // Pass receiver to task
                ));

                state
                  .pipe_reader_task_handles
                  .insert(pipe_id_core_read, reader_task);

                // 3. Store Endpoint Info (associating session with pipe IDs)
                let info = EndpointInfo {
                  mailbox: session_mailbox.clone(), // Session's command mailbox
                  task_handle: session_task_handle.expect("ConnSuccess must provide task handle"),
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
                  
                  // If we can't attach pipe, session is useless. Clean up.
                  // Clean up based on the endpoint URI we have
                  Self::cleanup_session_state_by_uri(
                    core_arc.clone(),     // Pass Arc<SocketCore>
                    &endpoint,            // Pass endpoint URI
                    &socket_logic_strong, // Pass Arc<dyn ISocket>
                  )
                  .await;
                  // Abort session task?
                  // session_task_handle.unwrap().abort();
                } else {
                  // 5. Notify ISocket logic (only after pipe successfully attached to Session)
                  let peer_identity: Option<&[u8]> = None;
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
              Command::ListenerStopped {
                handle: _child_handle,
                endpoint_uri,
              } => {
                tracing::debug!(handle = handle, listener_uri = %endpoint_uri, "Listener stopped reported");
                // <<< MODIFIED: Remove by URI >>>
                let mut state = core_arc.core_state.lock().await;
                if let Some(removed_info) = state.endpoints.remove(&endpoint_uri) {
                  tracing::debug!(handle=handle, listener_uri=%endpoint_uri, type=?removed_info.endpoint_type, "Removed listener endpoint state");
                  // Optional: Abort task handle just in case?
                  // removed_info.task_handle.abort();
                } else {
                  tracing::warn!(handle=handle, listener_uri=%endpoint_uri, "Listener stopped, but no corresponding endpoint state found");
                }
              }
              Command::ConnecterStopped {
                handle: _child_handle,
                endpoint_uri,
              } => {
                tracing::debug!(handle = handle, connecter_uri = %endpoint_uri, "Connecter stopped reported");
                // Usually no state to clean here for connecter itself,
                // unless we stored pending state before ConnSuccess/Failed.
                // If we did, remove it here using endpoint_uri.
              }
              Command::SessionStopped {
                handle: _child_handle,
                endpoint_uri,
              } => {
                tracing::debug!(handle = handle, session_uri = %endpoint_uri, "Session stopped reported");
                let mut state = core_arc.core_state.lock().await;

                // Remove endpoint state using the URI
                let removed_endpoint_info = state.endpoints.remove(&endpoint_uri);

                if removed_endpoint_info.is_none() {
                  tracing::warn!(handle=handle, session_uri=%endpoint_uri, "Session stopped, but no corresponding endpoint state found");
                }

                // --- Pipe Cleanup ---
                // TODO: How to find the correct pipe channels to remove/close?
                // We need to associate pipe IDs (e.g., pipe_id_core_write, pipe_id_core_read)
                // with the endpoint/session when ConnSuccess happens and store them,
                // perhaps in EndpointInfo or a separate map `HashMap<String(uri), (usize, usize)>`.
                let pipe_id_to_detach = {
                  // Placeholder ID
                  tracing::warn!(handle=handle, session_uri=%endpoint_uri, "TODO: Need correct pipe ID for stopped session cleanup!");
                  0 // Need the actual pipe ID associated with this session!
                };
                // state.pipes_tx.remove(&pipe_id_core_write?); // Remove sender if stored
                // state.pipes_rx.remove(&pipe_id_core_read?); // Remove receiver if stored
                drop(state); // Release lock

                // Notify ISocket logic that the pipe is gone
                // Still need the correct pipe ID here
                socket_logic_strong.pipe_detached(pipe_id_to_detach).await;
              }
              Command::ReportError {
                handle: child_handle,
                error,
              } => {
                tracing::error!(handle = handle, child = child_handle, error = %error, "Child actor reported fatal error");
                // TODO: Need child URI/ID here too for cleanup
                // let mut state = core_arc.core_state.lock().await;
                // Find and remove child state using handle/URI from ReportError variant
                // state.endpoints.remove(...);
                // state.pipes.remove(...);
                // Potentially initiate self shutdown:
                // Self::shutdown_logic(core_arc.clone()).await;
                // should_break_loop = true;
              }
              Command::CleanupComplete {
                handle: child_handle,
              } => {
                tracing::debug!(
                  handle = handle,
                  child = child_handle,
                  "Child actor cleanup complete"
                );
                // Usually no action needed if state removed on *Stopped/ReportError
              }
              Command::PipeMessageReceived { pipe_id, msg } => {
                tracing::trace!(
                  handle = handle,
                  pipe_id = pipe_id,
                  msg_size = msg.size(),
                  "Received message from pipe reader task"
                );
                // Delegate message to ISocket pattern logic via handle_pipe_event
                // Note: We need the *original* command enum structure here maybe?
                // Or a dedicated ISocket method `handle_pipe_message(pipe_id, msg)`?
                // Using handle_pipe_event for now requires reconstructing a Command variant... awkward.
                // Let's assume ISocket::handle_pipe_event handles PipeMessageReceived directly.
                if let Err(e) = socket_logic_strong
                  .handle_pipe_event(pipe_id, Command::PipeMessageReceived { pipe_id, msg })
                  .await
                {
                  tracing::error!(
                    handle = handle,
                    pipe_id = pipe_id,
                    "Error handling PipeMessageReceived: {}",
                    e
                  );
                }
              }
              Command::PipeClosedByPeer { pipe_id } => {
                // This means the Session closed its *sending* end (tx_sess_to_core)
                // The corresponding PipeReaderTask will have exited.
                tracing::debug!(
                  handle = handle,
                  pipe_id = pipe_id,
                  "Pipe closed by session peer (reader task should stop)"
                );
                let mut state = core_arc.core_state.lock().await;
                // Remove the reader task info
                if let Some(reader_info) = state.pipe_reader_task_handles.remove(&pipe_id) {
                  // Optionally abort just in case?
                  // reader_info.task_handle.abort();
                } else {
                  tracing::warn!(
                    handle = handle,
                    pipe_id = pipe_id,
                    "PipeClosedByPeer received, but reader task info not found"
                  );
                }
                // Find associated endpoint and clean it up too? Yes, if session didn't send SessionStopped.
                let mut endpoint_to_remove = None;
                for (uri, info) in state.endpoints.iter() {
                  if let Some((_, read_id)) = info.pipe_ids {
                    if read_id == pipe_id {
                      endpoint_to_remove = Some(uri.clone());
                      break;
                    }
                  }
                }
                if let Some(uri) = endpoint_to_remove {
                  state.endpoints.remove(&uri);
                  tracing::debug!(handle=handle, pipe=pipe_id, uri=%uri, "Removed endpoint for closed pipe");
                }

                drop(state); // Release lock
                             // Notify ISocket logic
                socket_logic_strong.pipe_detached(pipe_id).await;
              }

              // --- Unhandled ---
              _ => {
                tracing::warn!(
                  handle = handle,
                  "Unhandled command in SocketCore loop: {:?}",
                  command
                );
              }
            } // <--- END match command
          } // <--- END else block (socket_logic_strong is valid)
        } // <--- END Ok(command) arm
        Err(_) => {
          // <--- START Err arm (mailbox closed)
          // Mailbox closed
          tracing::info!(handle = handle, "Mailbox closed, initiating shutdown.");
          Self::shutdown_logic(core_arc.clone()).await;
          should_break_loop = true;
        } // <--- END Err arm
      } // <--- END match command_result

      if should_break_loop {
        break; // Exit loop
      }
    } // <--- END loop

    // --- Final Cleanup outside loop ---
    tracing::info!(handle = handle, "SocketCore run_command_loop finishing.");
    let ctx_inner = core_arc.context.inner().clone();
    ctx_inner.unregister_socket(handle).await;
    tracing::info!(handle = handle, "SocketCore task fully stopped.");
  }

  // ... (shutdown_logic helper - needs refinement based on how child handles are stored/identified) ...
  async fn shutdown_logic(core_arc: Arc<SocketCore>) {
    // ... (Implementation needs refinement for identifying children) ...
    tracing::debug!("Executing shutdown logic (Refinement needed)...");
    // Stop children... Await... Close pipes... Linger...
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
        // TODO: Apply change to existing pipes? Or only new ones?
        tracing::info!(
          handle = core_arc.handle,
          hwm = state.options.sndhwm,
          "Set SNDHWM"
        );
      }
      options::RCVHWM => {
        let hwm = parse_i32_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        if hwm < 0 {
          return Err(ZmqError::InvalidOptionValue(option));
        }
        state.options.rcvhwm = hwm as usize;
        // TODO: Apply change to existing pipes?
        tracing::info!(
          handle = core_arc.handle,
          hwm = state.options.rcvhwm,
          "Set RCVHWM"
        );
      }
      options::LINGER => {
        state.options.linger = parse_linger_option(value)?; // Use Option<Duration> parser
        tracing::info!(handle=core_arc.handle, linger=?state.options.linger, "Set LINGER");
      }

      options::ROUTING_ID => {
        // Core might still store it even if pattern uses it
        state.options.routing_id =
          Some(parse_blob_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set ROUTING_ID (core state)");
      }
      options::RCVTIMEO => {
        state.options.rcvtimeo =
          parse_duration_ms_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.rcvtimeo, "Set RCVTIMEO");
      }
      options::SNDTIMEO => {
        state.options.sndtimeo =
          parse_duration_ms_option(value).map_err(|_| ZmqError::InvalidOptionValue(option))?;
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
      options::ROUTING_ID => state
        .options
        .routing_id
        .as_ref() // Core fallback if pattern didn't handle
        .map(|blob| blob.to_vec())
        .ok_or(ZmqError::InvalidState("ROUTING_ID not set".into())), // Return error if not set?
      options::RCVTIMEO => Ok(
        state
          .options
          .rcvtimeo
          .map_or(-1, |d| d.as_millis() as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      options::SNDTIMEO => Ok(
        state
          .options
          .sndtimeo
          .map_or(-1, |d| d.as_millis() as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
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

  /// Helper to clean up state associated with a session endpoint URI.
  async fn cleanup_session_state_by_uri(
    core_arc: Arc<SocketCore>,
    endpoint_uri: &str,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    let mut state = core_arc.core_state.lock().await;
    if let Some(removed_info) = state.endpoints.remove(endpoint_uri) {
      tracing::debug!("Removed endpoint state for: {}", endpoint_uri);
      if let Some((write_pipe_id, read_pipe_id)) = removed_info.pipe_ids {
        state.pipes_tx.remove(&write_pipe_id);
        if let Some(task_handle) = state.pipe_reader_task_handles.remove(&read_pipe_id) {
          task_handle.abort(); // Stop the reader task
          tracing::debug!(
            "Removed & aborted pipe reader task for pipe_id {}",
            read_pipe_id
          );
        }
        drop(state); // Release lock before await
        socket_logic.pipe_detached(read_pipe_id).await;
      } else {
        drop(state);
      }
    } else {
      tracing::warn!(handle=core_arc.handle, uri=%endpoint_uri, "Cleanup attempted, but no endpoint state found");
    }
  }

  /// Helper to clean up state associated with a session pipe read ID.
  async fn cleanup_session_state_by_pipe(
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    let mut state = core_arc.core_state.lock().await;
    let mut endpoint_to_remove = None;
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

    // Remove reader task
    if let Some(task_handle) = state.pipe_reader_task_handles.remove(&pipe_read_id) {
      task_handle.abort(); // Stop the reader task
      tracing::debug!(
        "Removed & aborted pipe reader task for pipe_id {}",
        pipe_read_id
      );
    } else {
      tracing::warn!(
        handle = core_arc.handle,
        pipe_id = pipe_read_id,
        "PipeClosedByPeer cleanup, but reader task info not found"
      );
    }
    // Remove sender pipe
    if let Some(write_id) = write_pipe_id_to_remove {
      state.pipes_tx.remove(&write_id);
    }
    // Remove endpoint entry
    if let Some(uri) = endpoint_to_remove {
      state.endpoints.remove(&uri);
      tracing::debug!(handle=core_arc.handle, pipe=pipe_read_id, uri=%uri, "Removed endpoint for closed pipe");
    }
    drop(state); // Release lock

    // Notify ISocket (even if state was inconsistent)
    socket_logic.pipe_detached(pipe_read_id).await;
  }
}

// These need to be defined in their respective files (pub_socket.rs etc.)
// Add placeholders here just to satisfy SocketCore::create_and_spawn compilation. REMOVE LATER.
mod dummy_sockets {
  use super::*;
  macro_rules! dummy_socket_impl {
    ($name:ident) => {
      #[derive(Debug)]
      pub(crate) struct $name {
        pub(crate) core: Arc<SocketCore>,
      }
      impl $name {
        pub fn new(core: Arc<SocketCore>) -> Self {
          Self { core }
        }
      }
      #[async_trait::async_trait]
      impl ISocket for $name {
        fn mailbox(&self) -> &MailboxSender {
          self.core.mailbox_sender()
        }
        // Basic User methods (delegate to core or implement pattern specific)
        async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
          // Typically core handles spawning transport actors
          self
            .core
            .mailbox_sender()
            .send(Command::UserBind {
              endpoint: endpoint.to_string(),
              reply_tx: oneshot::channel().0, // Dummy reply, core handles real one
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          Ok(()) // This OK doesn't reflect bind success, just command send
        }
        async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
          self
            .core
            .mailbox_sender()
            .send(Command::UserConnect {
              endpoint: endpoint.to_string(),
              reply_tx: oneshot::channel().0,
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          Ok(())
        }
        async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
          self
            .core
            .mailbox_sender()
            .send(Command::UserDisconnect {
              endpoint: endpoint.to_string(),
              reply_tx: oneshot::channel().0,
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          Ok(())
        }
        async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
          self
            .core
            .mailbox_sender()
            .send(Command::UserUnbind {
              endpoint: endpoint.to_string(),
              reply_tx: oneshot::channel().0,
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          Ok(())
        }
        async fn send(&self, _msg: Msg) -> Result<(), ZmqError> {
          unimplemented!("{} send", stringify!($name))
        }
        async fn recv(&self) -> Result<Msg, ZmqError> {
          unimplemented!("{} recv", stringify!($name))
        }
        async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
          // Core handles general options, calls set_pattern_option for specifics
          let (tx, rx) = oneshot::channel();
          self
            .core
            .mailbox_sender()
            .send(Command::UserSetOpt {
              option,
              value: value.to_vec(),
              reply_tx: tx,
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          rx.await
            .map_err(|_| ZmqError::Internal("Reply channel error".into()))? // Wait for core result
        }
        async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
          // Core handles general options, calls get_pattern_option for specifics
          let (tx, rx) = oneshot::channel();
          self
            .core
            .mailbox_sender()
            .send(Command::UserGetOpt {
              option,
              reply_tx: tx,
            })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          rx.await
            .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
        }
        async fn close(&self) -> Result<(), ZmqError> {
          let (tx, rx) = oneshot::channel();
          self
            .core
            .mailbox_sender()
            .send(Command::UserClose { reply_tx: tx })
            .await
            .map_err(|_| ZmqError::Internal("Mailbox send error".into()))?;
          rx.await
            .map_err(|_| ZmqError::Internal("Reply channel error".into()))?
        }

        // <<< ADDED PLACEHOLDER for set_pattern_option >>>
        async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
          // Default: This pattern doesn't handle this option
          Err(ZmqError::UnsupportedOption(option))
        }
        // <<< ADDED PLACEHOLDER for get_pattern_option >>>
        async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
          // Default: This pattern doesn't handle this option
          Err(ZmqError::UnsupportedOption(option))
        }

        // Internal methods
        async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
          Ok(false) // Default: Don't handle commands via this path
        }
        async fn handle_pipe_event(&self, pipe_id: usize, event: Command) -> Result<(), ZmqError> {
          match event {
            Command::PipeMessageReceived { msg, .. } => {
              tracing::trace!(
                socket_type = stringify!($name),
                pipe_id = pipe_id,
                msg_size = msg.size(),
                "DUMMY handle_pipe_event: PipeMessageReceived"
              );
              // TODO: Concrete impls queue this message for user recv()
            }
            _ => {
              tracing::warn!(
                socket_type = stringify!($name),
                pipe_id = pipe_id,
                "DUMMY handle_pipe_event: Unhandled event: {:?}",
                event
              );
            }
          }
          Ok(())
        }
        async fn pipe_attached(
          &self,
          pipe_read_id: usize,
          pipe_write_id: usize,
          _peer_identity: Option<&[u8]>,
        ) {
          tracing::trace!(
            socket_type = stringify!($name),
            pipe_read_id = pipe_read_id,
            pipe_write_id = pipe_write_id,
            "DUMMY pipe_attached"
          );
        }
        async fn pipe_detached(&self, pipe_read_id: usize) {
          tracing::trace!(
            socket_type = stringify!($name),
            pipe_id = pipe_read_id,
            "DUMMY pipe_detached"
          );
        }
      }
    };
  }
  // Instantiate dummies
  dummy_socket_impl!(PubSocket);
  dummy_socket_impl!(SubSocket);
  dummy_socket_impl!(ReqSocket);
  dummy_socket_impl!(RepSocket);
  dummy_socket_impl!(DealerSocket);
  dummy_socket_impl!(RouterSocket);
}
