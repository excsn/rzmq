// src/socket/core.rs

// --- Imports ---
use super::{DealerSocket, PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, RouterSocket, SubSocket};
use crate::context::Context;
use crate::error::ZmqError;
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::session::SessionBase;
use crate::socket::events::{MonitorSender, SocketEvent};
use crate::socket::options::{
  self, parse_blob_option, parse_bool_option, parse_duration_ms_option, parse_heartbeat_option, parse_i32_option,
  parse_keepalive_mode_option, parse_linger_option, parse_reconnect_ivl_max_option, parse_reconnect_ivl_option,
  parse_secs_duration_option, parse_timeout_option, parse_u32_option, SocketOptions, ZmtpEngineConfig, HEARTBEAT_IVL,
  HEARTBEAT_TIMEOUT, LINGER, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME, RCVHWM, RCVTIMEO, RECONNECT_IVL,
  RECONNECT_IVL_MAX, ROUTER_MANDATORY, ROUTING_ID, SNDHWM, SNDTIMEO, SUBSCRIBE, TCP_KEEPALIVE, TCP_KEEPALIVE_CNT,
  TCP_KEEPALIVE_IDLE, TCP_KEEPALIVE_INTVL, UNSUBSCRIBE, ZAP_DOMAIN,
};
#[cfg(feature = "curve")]
use crate::socket::options::{
  parse_key_option, CURVE_KEY_LEN, CURVE_PUBLICKEY, CURVE_SECRETKEY, CURVE_SERVER, CURVE_SERVERKEY,
};
use crate::socket::patterns;
use crate::socket::types::SocketType;
use crate::socket::ISocket;
use crate::transport::endpoint::{parse_endpoint, Endpoint};
use crate::transport::inproc::bind_inproc;
#[cfg(feature = "inproc")]
use crate::transport::inproc::{connect_inproc, disconnect_inproc, unbind_inproc};
#[cfg(feature = "ipc")]
use crate::transport::ipc::IpcConnecter;
use crate::transport::ipc::IpcListener;
use crate::transport::tcp::{TcpConnecter, TcpListener};
use crate::{Blob, Msg};
use async_channel::{bounded, Receiver as AsyncReceiver, SendError, Sender as AsyncSender, TrySendError};
use futures::future::join_all;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant}; // Use std::time::Instant
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::timeout;

// --- Structs: EndpointInfo, EndpointType, CoreState ---
#[derive(Debug)]
pub(crate) struct EndpointInfo {
  pub mailbox: MailboxSender,
  pub task_handle: JoinHandle<()>,
  pub endpoint_type: EndpointType,
  pub endpoint_uri: String, // This is the RESOLVED URI (e.g., peer IP/port or listener URI)
  pub pipe_ids: Option<(usize, usize)>,
  pub handle_id: usize, // Child actor handle ID
  // <<< ADDED FIELD >>>
  /// The original endpoint URI requested by the user for connect() calls.
  /// Needed for automatic reconnection. None for listener endpoints.
  pub target_endpoint_uri: Option<String>,
  // <<< ADDED FIELD END >>>
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointType {
  Listener,
  Session,
  Connecter, // Used for connect tasks before session established
}
#[derive(Debug)]
pub(crate) struct CoreState {
  pub(crate) handle: usize,
  pub options: SocketOptions,
  pub socket_type: SocketType,
  pub pipes_tx: HashMap<usize, AsyncSender<Msg>>,
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,
  pub endpoints: HashMap<String, EndpointInfo>, // Keyed by RESOLVED URI
  #[cfg(feature = "inproc")]
  pub(crate) bound_inproc_names: HashSet<String>,
  pub(crate) monitor_tx: Option<MonitorSender>,
}

impl CoreState {
  fn new(handle: usize, socket_type: SocketType, options: SocketOptions) -> Self {
    Self {
      handle,
      options,
      socket_type,
      pipes_tx: HashMap::new(),
      pipe_reader_task_handles: HashMap::new(),
      endpoints: HashMap::new(),
      #[cfg(feature = "inproc")]
      bound_inproc_names: HashSet::new(),
      monitor_tx: None,
    }
  }
  pub(crate) fn get_pipe_sender(&self, pipe_write_id: usize) -> Option<AsyncSender<Msg>> {
    self.pipes_tx.get(&pipe_write_id).cloned()
  }
  #[allow(dead_code)] // May not be used if get_reader_task_handle isn't needed elsewhere
  pub(crate) fn get_reader_task_handle(&self, pipe_read_id: usize) -> Option<&JoinHandle<()>> {
    self.pipe_reader_task_handles.get(&pipe_read_id)
  }
  pub(crate) fn remove_pipe_state(&mut self, pipe_write_id: usize, pipe_read_id: usize) -> bool {
    let tx_removed = self.pipes_tx.remove(&pipe_write_id).is_some();
    if tx_removed {
      tracing::trace!(handle = self.handle, pipe_id = pipe_write_id, "Removed pipe sender");
    }
    let reader_removed = if let Some(handle) = self.pipe_reader_task_handles.remove(&pipe_read_id) {
      tracing::trace!(
        handle = self.handle,
        pipe_id = pipe_read_id,
        "Removing and aborting pipe reader task"
      );
      handle.abort();
      true
    } else {
      false
    };
    if reader_removed {
      tracing::trace!(
        handle = self.handle,
        pipe_id = pipe_read_id,
        "Removed pipe reader task handle"
      );
    }
    tx_removed || reader_removed
  }
  pub(crate) async fn send_monitor_event(&self, event: SocketEvent) {
    if let Some(ref tx) = self.monitor_tx {
      if let Err(e) = tx.send(event).await {
        tracing::warn!(socket_handle = self.handle, error = ?e, "Failed to send event to monitor channel (full or closed)");
      }
    }
  }
  pub(crate) fn get_monitor_sender_clone(&self) -> Option<MonitorSender> {
    self.monitor_tx.clone()
  }
}

// --- Shutdown Coordinator ---
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownPhase {
  Running,
  StoppingChildren,
  Lingering,
  CleaningPipes,
  Finished,
}
#[derive(Debug)]
struct ShutdownCoordinator {
  state: ShutdownPhase,
  pending_children: HashMap<usize, String>, // child_handle -> endpoint_uri
  #[cfg(feature = "inproc")]
  // Store write_id, read_id, and the known URI
  pending_inproc_pipes: Vec<(usize, usize, String)>,
  linger_deadline: Option<Instant>,
}
impl Default for ShutdownCoordinator {
  fn default() -> Self {
    Self {
      state: ShutdownPhase::Running,
      pending_children: HashMap::new(),
      #[cfg(feature = "inproc")]
      pending_inproc_pipes: Vec::new(),
      linger_deadline: None,
    }
  }
}
impl ShutdownCoordinator {
  fn current_phase(&self) -> ShutdownPhase {
    self.state
  }
  fn initiate(&mut self, core_state: &mut CoreState, core_handle: usize) -> bool {
    if self.state != ShutdownPhase::Running {
      return false;
    }
    tracing::debug!(handle = core_handle, "ShutdownCoordinator: Initiating state.");
    self.pending_children.clear();
    #[cfg(feature = "inproc")]
    {
      self.pending_inproc_pipes.clear();
    }
    let endpoints_to_process = std::mem::take(&mut core_state.endpoints);

    for (uri, info) in endpoints_to_process {
      tracing::trace!(handle = core_handle, uri=%uri, type=?info.endpoint_type, child_handle=info.handle_id, "ShutdownCoordinator: Processing endpoint");
      if uri.starts_with("inproc://") {
        #[cfg(feature = "inproc")]
        {
          if let Some(ids) = info.pipe_ids {
            // Store pipe IDs AND the URI
            self.pending_inproc_pipes.push((ids.0, ids.1, uri.clone()));
          }
        }
        info.task_handle.abort(); // Abort dummy/placeholder handle
      } else {
        let mailbox = info.mailbox.clone();
        let child_actor_handle = info.handle_id;
        tokio::spawn(async move {
          let _ = mailbox.send(Command::Stop).await;
        });
        self.pending_children.insert(child_actor_handle, uri);
        // Don't abort task_handle here, wait for child to report completion
      }
    }
    if self.pending_children.is_empty() {
      tracing::debug!(
        handle = core_handle,
        "ShutdownCoordinator: No children to wait for, moving to Lingering state."
      );
      self.state = ShutdownPhase::Lingering;
    } else {
      tracing::debug!(
        handle = core_handle,
        num_children = self.pending_children.len(),
        "ShutdownCoordinator: Waiting for children, moving to StoppingChildren state."
      );
      self.state = ShutdownPhase::StoppingChildren;
    }
    true
  }
  fn record_child_stopped(&mut self, child_handle: usize, core_handle: usize) -> bool {
    if self.state != ShutdownPhase::StoppingChildren {
      tracing::warn!(handle = core_handle, child=child_handle, state=?self.state, "Received child stopped notification in unexpected state.");
      return false;
    }
    if let Some(uri) = self.pending_children.remove(&child_handle) {
      tracing::debug!(handle = core_handle, child_handle=child_handle, uri=%uri, remaining = self.pending_children.len(), "ShutdownCoordinator: Recorded child stopped.");
      if self.pending_children.is_empty() {
        tracing::debug!(
          handle = core_handle,
          "ShutdownCoordinator: All children stopped, moving to Lingering state."
        );
        self.state = ShutdownPhase::Lingering;
        return true;
      }
    } else {
      tracing::warn!(
        handle = core_handle,
        child = child_handle,
        "Received stop confirmation from unknown/already stopped child handle during shutdown."
      );
    }
    false
  }
  fn start_linger(&mut self, linger_opt: Option<Duration>, core_handle: usize) {
    if self.state != ShutdownPhase::Lingering {
      tracing::error!(handle=core_handle, state=?self.state, "Attempted to start linger in incorrect state");
      return;
    }
    match linger_opt {
      None => {
        self.linger_deadline = None;
        tracing::debug!(handle = core_handle, "ShutdownCoordinator: Starting infinite linger.");
      }
      Some(d) if d.is_zero() => {
        self.linger_deadline = Some(Instant::now());
        tracing::debug!(handle = core_handle, "ShutdownCoordinator: Linger is zero.");
      }
      Some(d) => {
        self.linger_deadline = Some(Instant::now() + d);
        tracing::debug!(handle = core_handle, linger_duration=?d, "ShutdownCoordinator: Starting timed linger.");
      }
    }
  }
  fn is_linger_expired(&self, core_handle: usize) -> bool {
    if self.state != ShutdownPhase::Lingering {
      return false;
    }
    match self.linger_deadline {
      None => false,
      Some(deadline) => {
        let expired = Instant::now() >= deadline;
        if expired {
          tracing::debug!(handle = core_handle, "ShutdownCoordinator: Linger deadline expired.");
        }
        expired
      }
    }
  }
  fn are_queues_empty(&self, core_state: &CoreState) -> bool {
    core_state.pipes_tx.is_empty() || core_state.pipes_tx.values().all(|s| s.len() == 0)
  }
  fn advance_to_cleaning(&mut self, core_handle: usize) {
    if self.state == ShutdownPhase::Lingering {
      tracing::debug!(
        handle = core_handle,
        "ShutdownCoordinator: Linger finished/skipped, moving to CleaningPipes state."
      );
      self.state = ShutdownPhase::CleaningPipes;
    } else {
      tracing::warn!(handle=core_handle, state=?self.state, "Tried to advance to cleaning from unexpected state");
    }
  }
  fn finish(&mut self, core_handle: usize) {
    tracing::debug!(handle = core_handle, "ShutdownCoordinator: Moving to Finished state.");
    self.state = ShutdownPhase::Finished;
  }
}

// --- Struct: SocketCore ---
#[derive(Debug)]
pub(crate) struct SocketCore {
  pub(crate) handle: usize,
  pub(crate) context: Context,
  pub(crate) mailbox_sender: MailboxSender,
  mailbox_receiver: Mutex<MailboxReceiver>,
  pub(crate) core_state: Mutex<CoreState>,
  socket_logic: Mutex<Option<Weak<dyn ISocket>>>,
  command_task_handle: Mutex<Option<JoinHandle<()>>>,
  shutdown_coordinator: Mutex<ShutdownCoordinator>,
}

impl SocketCore {
  pub(crate) fn create_and_spawn(
    handle: usize,
    context: Context,
    socket_type: SocketType,
    mut initial_options: SocketOptions,
  ) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
    let (mailbox_sender, mailbox_receiver) = mailbox();
    initial_options.socket_type_name = format!("{:?}", socket_type).to_uppercase();
    let initial_options_clone = initial_options.clone();
    let core_state = CoreState::new(handle, socket_type, initial_options);
    let core_arc = Arc::new(SocketCore {
      handle,
      context,
      mailbox_sender: mailbox_sender.clone(),
      mailbox_receiver: Mutex::new(mailbox_receiver),
      core_state: Mutex::new(core_state),
      socket_logic: Mutex::new(None),
      command_task_handle: Mutex::new(None),
      shutdown_coordinator: Mutex::new(ShutdownCoordinator::default()),
    });
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
    let weak_isocket = Arc::downgrade(&socket_logic_arc);
    if let Ok(mut socket_logic_guard) = core_arc.socket_logic.try_lock() {
      *socket_logic_guard = Some(weak_isocket);
    } else {
      return Err(ZmqError::Internal("Mutex lock failure during socket init".into()));
    }
    let task_core_ref = core_arc.clone();
    let task_socket_logic_ref = socket_logic_arc.clone();
    let task_handle = tokio::spawn(SocketCore::run_command_loop(task_core_ref, task_socket_logic_ref));
    if let Ok(mut handle_guard) = core_arc.command_task_handle.try_lock() {
      *handle_guard = Some(task_handle);
    } else {
      task_handle.abort();
      return Err(ZmqError::Internal("Mutex lock failure storing task handle".into()));
    }
    Ok((socket_logic_arc, mailbox_sender))
  }

  pub(crate) fn mailbox_sender(&self) -> &MailboxSender {
    &self.mailbox_sender
  }
  pub(crate) async fn get_socket_logic(&self) -> Option<Arc<dyn ISocket>> {
    self.socket_logic.lock().await.as_ref().and_then(|w| w.upgrade())
  }
  pub(crate) async fn run_pipe_reader_task(
    core_handle: usize,
    core_mailbox: MailboxSender,
    pipe_read_id: usize,
    pipe_receiver: AsyncReceiver<Msg>,
  ) {
    tracing::debug!(
      core_handle = core_handle,
      pipe_id = pipe_read_id,
      "Pipe reader task started"
    );
    loop {
      tokio::select! {
          // Prioritize receiving messages
          msg_result = pipe_receiver.recv() => {
            match msg_result {
              Ok(msg) => {
                let cmd = Command::PipeMessageReceived { pipe_id: pipe_read_id, msg };
                if core_mailbox.send(cmd).await.is_err() {
                  tracing::warn!(core_handle=core_handle, pipe_id=pipe_read_id, "Core mailbox closed while forwarding pipe message.");
                  break; // Exit loop if core mailbox is closed
                }
              }
              Err(_) => { // Assuming RecvError means closed channel
                tracing::debug!(core_handle=core_handle, pipe_id=pipe_read_id, "Session pipe sender closed.");
                let cmd = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
                tracing::debug!(core_handle=core_handle, pipe_id=pipe_read_id, "PipeReaderTask sending PipeClosedByPeer command.");
                if core_mailbox.send(cmd).await.is_err() {
                  tracing::warn!(core_handle=core_handle, pipe_id=pipe_read_id, "Core mailbox closed sending PipeClosedByPeer.");
                }
                break; // Exit loop as pipe is closed
              }
            }
          }
      }
    }
    tracing::debug!(
      core_handle = core_handle,
      pipe_id = pipe_read_id,
      "Pipe reader task finished"
    );
  }

  async fn run_command_loop(core_arc: Arc<SocketCore>, socket_logic_strong: Arc<dyn ISocket>) {
    let handle = core_arc.handle;
    let socket_type_debug = core_arc.core_state.try_lock().map(|s| s.socket_type);
    tracing::info!(handle = handle, socket_type = ?socket_type_debug, "SocketCore run_command_loop starting task.");

    let mut linger_check_interval = tokio::time::interval(Duration::from_millis(10));
    linger_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
      let current_phase = core_arc.shutdown_coordinator.lock().await.current_phase();
      if current_phase == ShutdownPhase::Finished {
        tracing::debug!(
          handle = handle,
          "Shutdown sequence finished (checked before select), breaking command loop."
        );
        break;
      }

      let mut receiver_guard = core_arc.mailbox_receiver.lock().await;

      tokio::select! {
          biased;

          // --- Arm 1: Receive Commands ---
          cmd_result = receiver_guard.recv(), if current_phase != ShutdownPhase::Finished => {
            drop(receiver_guard); // Release lock ASAP
            match cmd_result {
              Ok(command) => {
                let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                let current_phase_inner = coordinator_guard.current_phase(); // Check phase *after* receiving command

                match command {
                    // Handle Coordination Commands during Shutdown
                    Command::SessionStopped { handle: child_handle_id, ref endpoint_uri } |
                    Command::ReportError { handle: child_handle_id, ref endpoint_uri, .. } |
                    Command::CleanupComplete { handle: child_handle_id, endpoint_uri: Some(ref endpoint_uri) }
                        if matches!(current_phase_inner, ShutdownPhase::StoppingChildren | ShutdownPhase::Lingering | ShutdownPhase::CleaningPipes) =>
                    {
                        tracing::info!(handle=handle, child_handle=child_handle_id, uri=%endpoint_uri, command_type=?command.variant_name(), "SocketCore processing child stop/cleanup during shutdown");

                        //Handle potential reconnect trigger
                        let maybe_target_uri_for_reconnect: Option<String>;
                        let mut endpoint_type = None; // <<< ADDED: Store endpoint type >>>
                        { // Scope for state lock to get target URI before dropping guard
                            let core_state_guard = core_arc.core_state.lock().await;
                            let info_opt = core_state_guard.endpoints.get(endpoint_uri);

                            maybe_target_uri_for_reconnect = info_opt
                                .filter(|info| info.endpoint_type == EndpointType::Session)
                                .and_then(|info| info.target_endpoint_uri.clone());
                              
                            endpoint_type = info_opt.map(|info| info.endpoint_type);
                        } // core_state_guard lock released

                        // Log ReportError if necessary
                        if let Command::ReportError { error, .. } = &command {
                            tracing::error!(handle=handle, child_handle=child_handle_id, uri=%endpoint_uri, %error, "Received ReportError from child");
                        }

                        let mut perform_endpoint_specific_cleanup = false; // Flag to indicate if specific cleanup is needed after coordination
                        let mut needs_full_pipe_cleanup = false; // Flag to trigger perform_pipe_cleanup
                        let mut pipes_to_clean = Vec::new(); // Initialize empty

                        let was_last_child = coordinator_guard.record_child_stopped(child_handle_id, handle);
                        if was_last_child {
                            let linger_opt = { core_arc.core_state.lock().await.options.linger };
                            coordinator_guard.start_linger(linger_opt, handle);
                            let queues_empty = coordinator_guard.are_queues_empty(&*core_arc.core_state.lock().await);
                            if coordinator_guard.is_linger_expired(handle) || queues_empty {
                                coordinator_guard.advance_to_cleaning(handle);
                                #[cfg(feature="inproc")]
                                { pipes_to_clean = coordinator_guard.pending_inproc_pipes.clone(); } // Get pipes before dropping guard
                                needs_full_pipe_cleanup = true; // Trigger full cleanup
                                perform_endpoint_specific_cleanup = false; // Full cleanup handles everything
                            } else {
                                // Linger continues
                                perform_endpoint_specific_cleanup = true; // Need to cleanup this specific child now
                            }
                        } else {
                            // Not the last child
                            perform_endpoint_specific_cleanup = true; // Need to cleanup this specific child now
                        }

                        // Drop coordinator guard *before* any async operations (including potential respawn)
                        drop(coordinator_guard);

                        // --- Perform full pipe cleanup if needed ---
                        if needs_full_pipe_cleanup {
                            Self::perform_pipe_cleanup(core_arc.clone(), &socket_logic_strong, pipes_to_clean).await;
                            // NOTE: perform_pipe_cleanup now advances state to Finished
                            // No need for further endpoint-specific cleanup if full cleanup happened
                            perform_endpoint_specific_cleanup = false;
                        }

                        // --- Perform endpoint-specific cleanup (modified to potentially respawn) ---
                        if perform_endpoint_specific_cleanup {
                            let cleanup_uri = endpoint_uri.clone(); // Clone URI for async block/match
                            // <<< CHECK IF CORE IS SHUTTING DOWN BEFORE RESPAWN CHECK >>>
                            let core_is_running = current_phase_inner == ShutdownPhase::Running;
                            let should_respawn = matches!(command, Command::SessionStopped { .. } | Command::ReportError { .. })
                                             && endpoint_type == Some(EndpointType::Session) // Must be a session
                                             && maybe_target_uri_for_reconnect.is_some()
                                             && core_is_running;

                            match command {
                                Command::SessionStopped { .. } | Command::ReportError { .. } => {
                                    // Perform the regular cleanup first
                                    Self::cleanup_session_state_by_uri(core_arc.clone(), &cleanup_uri, &socket_logic_strong).await;

                                    // Then, potentially respawn the connecter if appropriate
                                    if should_respawn {
                                        let target_uri = maybe_target_uri_for_reconnect.unwrap(); // Safe due to is_some() check
                                        let reconnect_enabled = {
                                            let state = core_arc.core_state.lock().await;
                                            // Check if reconnect_ivl is set and not zero
                                            state.options.reconnect_ivl.map_or(false, |d| !d.is_zero())
                                        };
                                        if reconnect_enabled {
                                            tracing::info!(handle = handle, target_uri=%target_uri, "Session terminated, initiating reconnect...");
                                            Self::respawn_connecter(core_arc.clone(), target_uri).await;
                                        } else {
                                            tracing::debug!(handle = handle, target_uri=%target_uri, "Session terminated, reconnect disabled by options.");
                                        }
                                    }
                                }
                                Command::CleanupComplete { .. } => {
                                    // Regular cleanup for Listeners/Connecters that finished
                                    let event = SocketEvent::Closed { endpoint: cleanup_uri };
                                    core_arc.core_state.lock().await.send_monitor_event(event).await;
                                }
                                _ => { /* Should not happen based on outer match */ }
                            }
                        }
                        // <<< MODIFIED END >>>
                    }

                    // Handle PipeClosedByPeer (Can happen anytime)
                    Command::PipeClosedByPeer { pipe_id: closed_pipe_read_id } => {
                       // Handle potential reconnect trigger
                       let (maybe_target_uri_for_reconnect, endpoint_type) = { // Don't need cleanup URI here
                         let state_guard = core_arc.core_state.lock().await;
                         let mut target_uri = None;
                         let mut ep_type = None;
                         // Find the endpoint associated with this pipe read ID
                         for (_uri, info) in state_guard.endpoints.iter() {
                           if info.pipe_ids.map_or(false, |(_, read_id)| read_id == closed_pipe_read_id) {
                            ep_type = Some(info.endpoint_type);
                            if info.endpoint_type == EndpointType::Session {
                              target_uri = info.target_endpoint_uri.clone();
                            }
                             break; // Found the pipe, no need to store resolved URI here
                           }
                         }
                         (target_uri, ep_type) // Return None for cleanup URI
                       };

                       // Drop coordinator guard *before* potential await in cleanup/respawn
                       drop(coordinator_guard);

                       // Perform cleanup regardless
                       Self::cleanup_session_state_by_pipe(core_arc.clone(), closed_pipe_read_id, &socket_logic_strong).await;

                       // Check if we should respawn (only if not shutting down Core)
                       // <<< CHECK IF CORE IS SHUTTING DOWN BEFORE RESPAWN CHECK >>>
                       let core_is_running = current_phase_inner == ShutdownPhase::Running;
                       let should_respawn = endpoint_type == Some(EndpointType::Session)
                                            && maybe_target_uri_for_reconnect.is_some()
                                            && core_is_running;

                       if should_respawn {
                         let target_uri = maybe_target_uri_for_reconnect.unwrap();
                         let reconnect_enabled = {
                           let state = core_arc.core_state.lock().await;
                           state.options.reconnect_ivl.map_or(false, |d| !d.is_zero())
                         };
                         if reconnect_enabled {
                           tracing::info!(handle = handle, target_uri=%target_uri, "Pipe closed by peer, initiating reconnect...");
                           Self::respawn_connecter(core_arc.clone(), target_uri).await;
                         } else {
                            tracing::debug!(handle = handle, target_uri=%target_uri, "Pipe closed by peer, reconnect disabled by options.");
                         }
                       }
                    }

                    // Handle commands that INITIATE shutdown
                    cmd @ (Command::Stop | Command::UserClose { .. }) => {
                        if current_phase_inner == ShutdownPhase::Running {
                             drop(coordinator_guard); // Release lock before initiating shutdown
                            if Self::initiate_shutdown_phase(core_arc.clone()).await {
                                // If UserClose, reply after successfully initiating
                                if let Command::UserClose { reply_tx } = cmd { let _ = reply_tx.send(Ok(())); }
                            } else {
                                // Initiate failed (already shutting down), still reply if UserClose
                                if let Command::UserClose { reply_tx } = cmd { let _ = reply_tx.send(Ok(())); }
                            }
                        } else {
                            // Already shutting down, just reply if UserClose
                             drop(coordinator_guard); // Release lock
                             if let Command::UserClose { reply_tx } = cmd { let _ = reply_tx.send(Ok(())); }
                        }
                    }

                    // Handle normal operational commands ONLY if Running
                    other_command => {
                      if current_phase_inner == ShutdownPhase::Running {
                        // Release coordinator lock *before* processing normal commands
                        drop(coordinator_guard);
                        // Delegate to specific handlers
                        match other_command {
                            Command::UserMonitor { monitor_tx, reply_tx } => { Self::handle_user_monitor(core_arc.clone(), monitor_tx, reply_tx).await; }
                            Command::UserBind { endpoint, reply_tx } => { Self::handle_user_bind(core_arc.clone(), endpoint, reply_tx).await; }
                            Command::UserConnect { endpoint, reply_tx } => { Self::handle_user_connect(core_arc.clone(), endpoint, reply_tx).await; }
                            Command::UserDisconnect { endpoint, reply_tx } => { Self::handle_user_disconnect(core_arc.clone(), endpoint, reply_tx).await; }
                            Command::UserUnbind { endpoint, reply_tx } => { Self::handle_user_unbind(core_arc.clone(), endpoint, reply_tx).await; }
                            Command::UserSend { msg } => { if let Err(e) = socket_logic_strong.send(msg).await { tracing::error!(handle = handle, "Error during send: {}", e); } }
                            Command::UserRecv { reply_tx } => { let _ = reply_tx.send(socket_logic_strong.recv().await); }
                            Command::UserSetOpt { option, value, reply_tx } => { let _ = reply_tx.send(Self::handle_set_option(core_arc.clone(), &socket_logic_strong, option, &value).await); }
                            Command::UserGetOpt { option, reply_tx } => { let _ = reply_tx.send(Self::handle_get_option(core_arc.clone(), &socket_logic_strong, option).await); }
                            // <<< MODIFIED: handle_conn_success signature changed >>>
                            Command::ConnSuccess { endpoint, target_endpoint_uri, session_mailbox, session_handle, session_task_handle } => {
                              Self::handle_conn_success(core_arc.clone(), &socket_logic_strong, endpoint, target_endpoint_uri, session_mailbox, session_handle, session_task_handle).await;
                            }
                            // <<< MODIFIED END >>>
                            Command::ConnFailed { endpoint, error } => { Self::handle_conn_failed(core_arc.clone(), endpoint, error).await; }
                            Command::PipeMessageReceived { pipe_id, msg } => { Self::handle_pipe_message(core_arc.clone(), &socket_logic_strong, pipe_id, msg).await; }
                            #[cfg(feature = "inproc")]
                            Command::InprocConnectRequest { .. } => { Self::handle_inproc_connect(core_arc.clone(), &socket_logic_strong, other_command).await; }
                            #[cfg(feature = "inproc")]
                            Command::InprocPipeClosed { pipe_read_id } => {
                                tracing::debug!(handle = handle, pipe_id = pipe_read_id, "Processing InprocPipeClosed command.");
                                // Could potentially trigger reconnect if needed, similar to PipeClosedByPeer
                                Self::cleanup_session_state_by_pipe(core_arc.clone(), pipe_read_id, &socket_logic_strong).await;
                            }
                            // Commands handled explicitly above are ignored here
                            Command::Stop | Command::UserClose{..} | Command::CleanupComplete{..} |
                            Command::SessionStopped{..} | Command::PipeClosedByPeer{..} | Command::ReportError{..} => {
                                tracing::error!(handle = handle, command = ?other_command, "Command reached default arm unexpectedly");
                            }
                             _ => { tracing::warn!(handle = handle, "Unhandled command in SocketCore loop: {:?}", other_command); }
                        } // end match other_command
                      } else {
                        // Socket is shutting down, ignore other commands
                        tracing::debug!(handle = handle, phase=?current_phase_inner, "Socket is shutting down, ignoring command: {:?}", other_command);
                         drop(coordinator_guard); // Release coordinator lock
                      }
                    } // end other_command catch-all case
                } // end match command (inner)
              } // end Ok(command) case
              Err(_) => { // Mailbox closed by sender (Context likely terminated)
                  tracing::info!(handle = handle, "Mailbox closed, ensuring final shutdown phases.");
                  // Acquire lock to check/update state before final cleanup
                  let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                  if coordinator_guard.current_phase() != ShutdownPhase::Finished {
                      // Get pipes_to_clean correctly with cfg
                      #[cfg(feature="inproc")]
                      let pipes_to_clean = coordinator_guard.pending_inproc_pipes.clone();
                      #[cfg(not(feature="inproc"))]
                      let pipes_to_clean = Vec::new(); // Pass empty vec if feature disabled
                      // Force state to CleaningPipes if not already there or finished
                      if coordinator_guard.current_phase() != ShutdownPhase::CleaningPipes {
                          coordinator_guard.state = ShutdownPhase::CleaningPipes;
                          tracing::debug!(handle=handle, "Mailbox closed: Forcing state to CleaningPipes");
                      }
                      // Release lock *before* calling potentially async cleanup
                      drop(coordinator_guard);
                      Self::perform_pipe_cleanup(core_arc.clone(), &socket_logic_strong, pipes_to_clean).await; // Pass correct struct
                  } else {
                      drop(coordinator_guard); // Already finished, just release lock
                  }
                  break; // Stop the command loop now
              }
            } // end match cmd_result
          }, // end command processing arm

          // --- Arm 2: Periodic Linger Check ---
          _ = linger_check_interval.tick(), if current_phase == ShutdownPhase::Lingering => {
              // Release mailbox lock before checking linger
              drop(receiver_guard);
              let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
              // Double-check phase as state might change between loop start and here
              if coordinator_guard.current_phase() == ShutdownPhase::Lingering {
                   if coordinator_guard.linger_deadline.is_none() { // Start timer only on first check in this state
                        let linger_opt = { core_arc.core_state.lock().await.options.linger };
                        coordinator_guard.start_linger(linger_opt, handle);
                   }
                   let queues_empty = coordinator_guard.are_queues_empty(&*core_arc.core_state.lock().await);
                   if coordinator_guard.is_linger_expired(handle) || queues_empty {
                       if queues_empty { tracing::debug!(handle = handle, "Linger check: Queues empty."); }
                       coordinator_guard.advance_to_cleaning(handle);
                       // Get pipes_to_clean correctly with cfg
                       #[cfg(feature="inproc")]
                       let pipes_to_clean = coordinator_guard.pending_inproc_pipes.clone();
                       #[cfg(not(feature="inproc"))]
                       let pipes_to_clean = Vec::new(); // Pass empty vec if feature disabled
                       // Release lock *before* calling cleanup
                       drop(coordinator_guard);
                       Self::perform_pipe_cleanup(core_arc.clone(), &socket_logic_strong, pipes_to_clean).await; // Pass correct struct
                   } else {
                       drop(coordinator_guard); // Release lock if linger continues
                   }
              } else {
                  drop(coordinator_guard); // State changed, release lock
              }
          }, // end linger check arm

      } // end select!

      // Loop continues until break condition at the top is met
    } // <--- END loop

    // --- Close the core's own sender end of the mailbox ---
    tracing::debug!(handle = handle, "SocketCore closing its mailbox sender...");
    core_arc.mailbox_sender.close(); // Close the sender end
    tracing::debug!(handle = handle, "SocketCore mailbox sender closed.");

    tracing::info!(handle = handle, socket_type = ?socket_type_debug, "SocketCore run_command_loop loop finished.");
    tracing::info!(handle = handle, socket_type = ?socket_type_debug, "SocketCore run_command_loop performing final unregister...");

    // Perform final context unregistration and inproc cleanup outside the loop
    #[cfg(feature = "inproc")]
    {
      let bound_names = {
        let mut state = core_arc.core_state.lock().await;
        std::mem::take(&mut state.bound_inproc_names)
      };
      if !bound_names.is_empty() {
        tracing::debug!(handle = handle, "Unregistering bound inproc names: {:?}", bound_names);
        let unbind_futures = bound_names.into_iter().map(|name| {
          let ctx_inner = core_arc.context.inner().clone(); // Clone Arc for async block
          async move {
            ctx_inner.unregister_inproc(&name).await;
          }
        });
        join_all(unbind_futures).await;
      }
    }

    // Final unregister from context
    let ctx_inner = core_arc.context.inner().clone();
    ctx_inner.unregister_socket(handle).await;

    tracing::info!(handle = handle, socket_type = ?socket_type_debug, "SocketCore task FULLY STOPPED (after loop and cleanup).");
  }

  // --- Refactored Shutdown Phase Methods ---
  async fn initiate_shutdown_phase(core_arc: Arc<SocketCore>) -> bool {
    let mut coordinator = core_arc.shutdown_coordinator.lock().await;
    let core_handle = core_arc.handle; // Get handle directly from core_arc
    let mut core_state = core_arc.core_state.lock().await; // Now lock mutably
                                                           // Pass the separately obtained handle
    coordinator.initiate(&mut core_state, core_handle)
  }

  // This function remains mainly for shutdown coordination, not reconnect triggering
  async fn handle_child_stopped(core_arc: Arc<SocketCore>, child_handle: usize, socket_logic: &Arc<dyn ISocket>) {
    let handle = core_arc.handle;
    let mut coordinator = core_arc.shutdown_coordinator.lock().await;
    // Use the URI stored in pending_children, looked up by handle
    let endpoint_uri = coordinator.pending_children.get(&child_handle).cloned();

    if let Some(_uri) = endpoint_uri {
      // Proceed only if handle was found
      if coordinator.record_child_stopped(child_handle, handle) {
        // was_last_child
        let linger_opt = { core_arc.core_state.lock().await.options.linger };
        coordinator.start_linger(linger_opt, handle);
        let queues_empty = coordinator.are_queues_empty(&*core_arc.core_state.lock().await);
        if coordinator.is_linger_expired(handle) || queues_empty {
          coordinator.advance_to_cleaning(handle);
          // Get pipes_to_clean correctly with cfg
          #[cfg(feature = "inproc")]
          let pipes_to_clean = coordinator.pending_inproc_pipes.clone();
          #[cfg(not(feature = "inproc"))]
          let pipes_to_clean = Vec::new(); // Pass empty vec if feature disabled
          drop(coordinator);
          Self::perform_pipe_cleanup(core_arc.clone(), socket_logic, pipes_to_clean).await;
        } else {
          drop(coordinator); // Release lock if linger continues
        }
      } else {
        drop(coordinator); // Release lock if not last child
      }
    } else {
      drop(coordinator); // Release lock if child not found
      tracing::warn!(
        handle = handle,
        child = child_handle,
        "Received stop for untracked child handle during shutdown."
      );
    }
  }

  async fn perform_pipe_cleanup(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    #[cfg(feature = "inproc")] pending_inproc_pipes: Vec<(usize, usize, String)>, // Receive pipes with URIs
    #[cfg(not(feature = "inproc"))] _pending_inproc_pipes: Vec<(usize, usize, String)>, // Use placeholder if feature disabled
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, "Performing final pipe cleanup phase...");
    let pipes_tx_to_close;
    let reader_tasks_to_abort;
    let monitor_tx_clone; // Need monitor sender
    #[cfg(feature = "inproc")]
    let mut disconnected_inproc_endpoints = Vec::new(); // Store URIs to notify about

    let mut coordinator = core_arc.shutdown_coordinator.lock().await;

    if coordinator.state != ShutdownPhase::CleaningPipes {
      tracing::warn!(handle=handle, state=?coordinator.state, "perform_pipe_cleanup called in unexpected state");
      drop(coordinator);
      return;
    }

    {
      // Scope for core_state lock
      let mut state = core_arc.core_state.lock().await;
      monitor_tx_clone = state.get_monitor_sender_clone(); // Get monitor sender clone
                                                           // Take these regardless of inproc/tcp/ipc for general pipe cleanup
      pipes_tx_to_close = std::mem::take(&mut state.pipes_tx);
      reader_tasks_to_abort = std::mem::take(&mut state.pipe_reader_task_handles);

      #[cfg(feature = "inproc")]
      {
        // Iterate through pending inproc pipes identified during shutdown initiation
        for (_write_id, _read_id, uri) in &pending_inproc_pipes {
          disconnected_inproc_endpoints.push(uri.clone());
          tracing::debug!(handle=handle, %uri, "Marking inproc connection for Disconnected event during cleanup.");
        }
      }
    } // core_state lock released

    // Release coordinator lock *before* potential awaits in loops/joins
    drop(coordinator);

    // --- Close Senders and Abort Readers (Existing Logic - now handles all pipes) ---
    for (id, sender) in pipes_tx_to_close {
      sender.close();
      tracing::trace!(handle = handle, pipe_id = id, "Closed pipe sender during final cleanup");
    }
    tracing::debug!(handle = handle, "All pipe senders closed during final cleanup.");
    if !reader_tasks_to_abort.is_empty() {
      let abort_handles: Vec<_> = reader_tasks_to_abort.into_values().collect();
      for task in &abort_handles {
        task.abort(); // Abort first
      }
      tracing::debug!(handle = handle, "All pipe reader tasks aborted during final cleanup.");
    } else {
      tracing::debug!(handle = handle, "No pipe reader tasks to abort during final cleanup.");
    }

    // --- Notify ISocket about Detached Pipes (Inproc) ---
    #[cfg(feature = "inproc")]
    {
      if !pending_inproc_pipes.is_empty() {
        let mut notify_futures = vec![];
        for (_write_id, read_id, uri) in &pending_inproc_pipes {
          tracing::debug!(
            handle = handle,
            pipe_read_id = read_id,
            uri = %uri, // Log URI for context
            "Notifying ISocket of inproc pipe detachment during final cleanup"
          );
          // Clone necessary data for the async block
          let socket_logic_clone = socket_logic.clone();
          let current_read_id = *read_id;
          notify_futures.push(async move {
            socket_logic_clone.pipe_detached(current_read_id).await;
          });
        }
        futures::future::join_all(notify_futures).await;
      }
    }

    // --- Send Disconnected Events for Inproc ---
    #[cfg(feature = "inproc")]
    if let Some(monitor_tx) = monitor_tx_clone {
      let mut event_futures = vec![];
      for endpoint_uri in disconnected_inproc_endpoints {
        let event = SocketEvent::Disconnected { endpoint: endpoint_uri };
        let tx_clone = monitor_tx.clone();
        event_futures.push(async move {
          if tx_clone.send(event).await.is_err() {
            tracing::warn!(socket_handle = handle, "Failed to send Disconnected event for inproc");
          }
        });
      }
      futures::future::join_all(event_futures).await; // Send events concurrently
    }

    // Advance to final state AFTER cleanup awaits complete
    let mut final_coordinator_guard = core_arc.shutdown_coordinator.lock().await;
    final_coordinator_guard.finish(handle);
    tracing::info!(handle = handle, "Pipe cleanup phase complete. Shutdown finished.");
  }

  // --- Command Handler Helpers ---
  async fn handle_user_monitor(
    core_arc: Arc<SocketCore>,
    monitor_tx: MonitorSender,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle; // Get handle for logging
    tracing::debug!(handle = handle, "Handling UserMonitor (helper)");
    let mut state_guard = core_arc.core_state.lock().await;
    if state_guard.monitor_tx.is_some() {
      tracing::warn!(handle = handle, "Monitor channel already set, replacing.");
    }
    state_guard.monitor_tx = Some(monitor_tx);
    drop(state_guard); // Release lock
    let _ = reply_tx.send(Ok(())); // Confirm setup
  }

  async fn handle_user_bind(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, %endpoint, "Handling UserBind (helper)");
    let parse_result = parse_endpoint(&endpoint);
    let bind_result = match parse_result {
      Ok(Endpoint::Tcp(_addr, ref uri)) => {
        let mut state = core_arc.core_state.lock().await;
        let monitor_tx_clone = state.get_monitor_sender_clone();
        let options_arc = Arc::new(state.options.clone());
        if state.endpoints.contains_key(uri) {
          // <<< ADDED CHECK: If already bound, return AddrInUse >>>
          Err(ZmqError::AddrInUse(uri.clone()))
          // <<< ADDED CHECK END >>>
        } else {
          let child_handle = core_arc.context.inner().next_handle();
          let handle_source = core_arc.context.inner().next_handle.clone();
          let listener_result = TcpListener::create_and_spawn(
            child_handle,
            uri.clone(),
            options_arc,
            core_arc.mailbox_sender().clone(),
            handle_source,
            monitor_tx_clone,
          );
          match listener_result {
            Ok((mailbox, task)) => {
              let info = EndpointInfo {
                mailbox,
                task_handle: task,
                endpoint_type: EndpointType::Listener,
                endpoint_uri: uri.clone(),
                pipe_ids: None,
                handle_id: child_handle,
                target_endpoint_uri: None, // Listeners don't have a target
              };
              state.endpoints.insert(uri.clone(), info);
              let event = SocketEvent::Listening { endpoint: uri.clone() };
              state.send_monitor_event(event).await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
      }
      #[cfg(feature = "ipc")]
      Ok(Endpoint::Ipc(ref path, ref uri)) => {
        let mut state = core_arc.core_state.lock().await;
        let monitor_tx_clone = state.get_monitor_sender_clone();
        let options_arc = Arc::new(state.options.clone());
        if state.endpoints.contains_key(uri) {
          // <<< ADDED CHECK: If already bound, return AddrInUse >>>
          Err(ZmqError::AddrInUse(uri.clone()))
          // <<< ADDED CHECK END >>>
        } else {
          let child_handle = core_arc.context.inner().next_handle();
          let handle_source = core_arc.context.inner().next_handle.clone();
          let listener_result = IpcListener::create_and_spawn(
            child_handle,
            uri.clone(),
            path.clone(),
            options_arc,
            core_arc.mailbox_sender().clone(),
            handle_source,
            monitor_tx_clone,
          );
          match listener_result {
            Ok((mailbox, task)) => {
              let info = EndpointInfo {
                mailbox,
                task_handle: task,
                endpoint_type: EndpointType::Listener,
                endpoint_uri: uri.clone(),
                pipe_ids: None,
                handle_id: child_handle,
                target_endpoint_uri: None, // Listeners don't have a target
              };
              state.endpoints.insert(uri.clone(), info);
              let event = SocketEvent::Listening { endpoint: uri.clone() };
              state.send_monitor_event(event).await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
      }
      #[cfg(feature = "inproc")]
      Ok(Endpoint::Inproc(ref name)) => {
        // <<< ADDED CHECK: Avoid duplicate bind for inproc >>>
        let core_clone = core_arc.clone(); // Clone for async block
        let name_clone = name.clone();
        // Check if already bound *before* attempting registration
        let is_bound = core_clone
          .core_state
          .lock()
          .await
          .bound_inproc_names
          .contains(&name_clone);
        if is_bound {
          Err(ZmqError::AddrInUse(format!("inproc://{}", name)))
        } else {
          match crate::transport::inproc::bind_inproc(name_clone.clone(), core_clone.clone()).await {
            Ok(()) => {
              // No need to add to endpoints map for inproc bind
              let mut state = core_clone.core_state.lock().await; // Lock again to modify set
              state.bound_inproc_names.insert(name_clone); // Track locally in CoreState
              let event = SocketEvent::Listening {
                endpoint: format!("inproc://{}", name),
              };
              state.send_monitor_event(event).await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
        // <<< ADDED CHECK END >>>
      }
      Err(e) => Err(e),
      _ => Err(ZmqError::UnsupportedTransport(endpoint.to_string())),
    };
    if let Err(e) = &bind_result {
      let event = SocketEvent::BindFailed {
        endpoint: endpoint.clone(),
        error_msg: format!("{}", e),
      };
      core_arc.core_state.lock().await.send_monitor_event(event).await;
    }
    let _ = reply_tx.send(bind_result);
  }

  async fn handle_user_connect(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, %endpoint, "Handling UserConnect (helper)");
    let parse_result = parse_endpoint(&endpoint);
    match parse_result {
      Ok(Endpoint::Tcp(_, uri)) => {
        // Spawn a connecter task - no need to store it in endpoints map directly
        Self::respawn_connecter(core_arc, uri).await;
        let _ = reply_tx.send(Ok(())); // Connect call returns immediately
      }
      #[cfg(feature = "ipc")]
      Ok(Endpoint::Ipc(_path, uri)) => {
        // Spawn a connecter task
        Self::respawn_connecter(core_arc, uri).await;
        let _ = reply_tx.send(Ok(()));
      }
      #[cfg(feature = "inproc")]
      Ok(Endpoint::Inproc(name)) => {
        // Inproc connect is synchronous within the context, handled differently
        let core_clone = core_arc.clone();
        tokio::spawn(async move {
          connect_inproc(name, core_clone, reply_tx).await;
        });
      }
      Err(e) => {
        let _ = reply_tx.send(Err(e));
      }
      _ => {
        let _ = reply_tx.send(Err(ZmqError::UnsupportedTransport(endpoint)));
      }
    }
  }

  async fn handle_user_disconnect(
    core_arc: Arc<SocketCore>,
    endpoint: String, // This is the RESOLVED URI (peer addr) or TARGET URI
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, %endpoint, "Handling UserDisconnect (helper)");
    let mut disconnect_result = Ok(());
    let mut state = core_arc.core_state.lock().await;
    let mut endpoint_to_stop: Option<(String, MailboxSender)> = None;

    // Find the session based on the provided endpoint (could be resolved or target)
    if let Some(info) = state.endpoints.get(&endpoint) {
      if info.endpoint_type == EndpointType::Session {
        endpoint_to_stop = Some((endpoint.clone(), info.mailbox.clone()));
      }
    } else {
      // If not found by resolved URI, check if it matches a target URI
      for (resolved_uri, info) in state.endpoints.iter() {
        if info.endpoint_type == EndpointType::Session && info.target_endpoint_uri.as_deref() == Some(&endpoint) {
          endpoint_to_stop = Some((resolved_uri.clone(), info.mailbox.clone()));
          break;
        }
      }
    }

    if let Some((resolved_uri, mailbox)) = endpoint_to_stop {
      // Remove info before dropping lock and sending stop
      if let Some(removed_info) = state.endpoints.remove(&resolved_uri) {
        drop(state); // Drop lock before sending command
        tracing::debug!(handle = handle, uri = %resolved_uri, "Sending Stop to session for disconnect");
        if mailbox.send(Command::Stop).await.is_err() {
          tracing::warn!(handle = handle, uri = %resolved_uri, "Failed to send Stop command to session (already gone?)");
          // Should still clean up pipe state associated with removed_info
          // But the normal SessionStopped/ReportError path won't run.
          // Need manual cleanup here? Or rely on eventual core shutdown?
          // Let's call cleanup manually if send fails.
          if let Some((w_id, r_id)) = removed_info.pipe_ids {
            let _ = core_arc.core_state.lock().await.remove_pipe_state(w_id, r_id);
            if let Some(sl) = core_arc.get_socket_logic().await {
              sl.pipe_detached(r_id).await;
            }
          }
        }
      } else {
        // Should not happen if we just found it
        drop(state);
      }
    } else if endpoint.starts_with("inproc://") {
      drop(state); // Drop lock before potentially async inproc disconnect
      #[cfg(feature = "inproc")]
      {
        disconnect_result = disconnect_inproc(&endpoint, core_arc.clone()).await;
      }
      #[cfg(not(feature = "inproc"))]
      {
        disconnect_result = Err(ZmqError::UnsupportedTransport(endpoint.clone()));
      }
    } else {
      drop(state);
      tracing::warn!(handle=handle, uri=%endpoint, "Disconnect requested for unknown endpoint/target");
      // Return Ok even if not found, mimicking libzmq? Or error? Let's return Ok.
    }

    let _ = reply_tx.send(disconnect_result);
  }

  async fn handle_user_unbind(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, %endpoint, "Handling UserUnbind (helper)");
    let mut unbind_result = Ok(());
    let mut state = core_arc.core_state.lock().await;
    let mut listener_to_stop: Option<(String, MailboxSender)> = None;

    if let Some(info) = state.endpoints.get(&endpoint) {
      if info.endpoint_type == EndpointType::Listener {
        listener_to_stop = Some((endpoint.clone(), info.mailbox.clone()));
      } else {
        tracing::warn!(handle=handle, uri=%endpoint, "Unbind requested for non-listener type: {:?}", info.endpoint_type);
        unbind_result = Err(ZmqError::InvalidArgument(
          "Cannot unbind a non-listener endpoint".into(),
        ));
      }
    } else if endpoint.starts_with("inproc://") {
      // Handle inproc unbind separately
    } else {
      tracing::warn!(handle=handle, uri=%endpoint, "Unbind requested for unknown endpoint");
      // libzmq returns ENOENT - Address not available? Or just Ok? Let's return Ok.
    }

    if let Some((uri, mailbox)) = listener_to_stop {
      if let Some(removed_info) = state.endpoints.remove(&uri) {
        drop(state); // Drop lock before sending
        tracing::debug!(handle = handle, uri = %uri, "Sending Stop to listener for unbind");
        if mailbox.send(Command::Stop).await.is_err() {
          tracing::warn!(handle = handle, uri = %uri, "Failed to send Stop command to listener (already gone?)");
        }
        // Let CleanupComplete handle final state removal
      } else {
        drop(state); // Should not happen
      }
    } else if endpoint.starts_with("inproc://") {
      #[cfg(feature = "inproc")]
      {
        let name = endpoint.strip_prefix("inproc://").unwrap_or("");
        if !name.is_empty() {
          if state.bound_inproc_names.remove(name) {
            tracing::debug!(handle=handle, name=%name, "Removed inproc name from bound set");
            // Need to drop state lock before calling async unbind_inproc
            drop(state);
            unbind_inproc(name, &core_arc.context).await;
          } else {
            drop(state);
            tracing::warn!(handle=handle, name=%name, "Inproc unbind requested for name not bound by this socket");
            // Return Ok even if not bound by us? Consistent with TCP/IPC.
          }
        } else {
          drop(state);
          unbind_result = Err(ZmqError::InvalidEndpoint(endpoint));
        }
      }
      #[cfg(not(feature = "inproc"))]
      {
        drop(state);
        unbind_result = Err(ZmqError::UnsupportedTransport(endpoint.clone()));
      }
    } else {
      drop(state); // If no listener found and not inproc
    }

    let _ = reply_tx.send(unbind_result);
  }

  // --- Connection Handlers ---
  async fn handle_conn_success(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    endpoint: String,            // Resolved peer URI
    target_endpoint_uri: String, // Original target URI
    session_mailbox: MailboxSender,
    session_handle: Option<usize>,
    session_task_handle: Option<JoinHandle<()>>,
  ) {
    let handle = core_arc.handle;
    tracing::info!(handle = handle, %endpoint, target_uri=%target_endpoint_uri, "Handling ConnSuccess (helper)");
    let pipe_id_core_write = core_arc.context.inner().next_handle();
    let pipe_id_core_read = pipe_id_core_write + 1;
    let mut state = core_arc.core_state.lock().await;
    let core_to_session_capacity = state.options.sndhwm.max(1);
    let session_to_core_capacity = state.options.rcvhwm.max(1);
    let (tx_core_to_sess, rx_core_to_sess) = bounded::<Msg>(core_to_session_capacity);
    let (tx_sess_to_core, rx_sess_to_core) = bounded::<Msg>(session_to_core_capacity);
    state.pipes_tx.insert(pipe_id_core_write, tx_core_to_sess.clone());
    let reader_task_handle = tokio::spawn(Self::run_pipe_reader_task(
      handle,
      core_arc.mailbox_sender().clone(),
      pipe_id_core_read,
      rx_sess_to_core,
    ));
    state
      .pipe_reader_task_handles
      .insert(pipe_id_core_read, reader_task_handle);
    let child_actor_handle = session_handle.unwrap_or_else(|| core_arc.context.inner().next_handle());
    let owned_task_handle = session_task_handle.expect("ConnSuccess command must provide Session task handle");
    let info = EndpointInfo {
      mailbox: session_mailbox.clone(),
      task_handle: owned_task_handle,
      endpoint_type: EndpointType::Session,
      endpoint_uri: endpoint.clone(), // Store resolved URI
      pipe_ids: Some((pipe_id_core_write, pipe_id_core_read)),
      handle_id: child_actor_handle,
      target_endpoint_uri: Some(target_endpoint_uri), // Store original target URI
    };
    state.endpoints.insert(endpoint.clone(), info);
    drop(state);
    let attach_pipe_cmd = Command::AttachPipe {
      rx_from_core: rx_core_to_sess,
      tx_to_core: tx_sess_to_core,
      pipe_read_id: pipe_id_core_write,
      pipe_write_id: pipe_id_core_read,
    };
    if session_mailbox.send(attach_pipe_cmd).await.is_err() {
      tracing::error!(
        handle = handle,
        session_handle = child_actor_handle,
        "Failed to send AttachPipe to session. Cleaning up."
      );
      Self::cleanup_session_state_by_pipe_ids(core_arc.clone(), pipe_id_core_write, pipe_id_core_read, socket_logic)
        .await;
    } else {
      socket_logic
        .pipe_attached(pipe_id_core_read, pipe_id_core_write, None) // TODO: Pass identity from ConnSuccess if available
        .await;
    }
  }

  async fn handle_conn_failed(core_arc: Arc<SocketCore>, endpoint: String, error: ZmqError) {
    let handle = core_arc.handle;
    tracing::error!(handle = handle, %endpoint, error = %error, "Handling ConnFailed (helper)");
    // ConnFailed implies the Connecter task is stopping after final failure.
    // No explicit reconnect attempt needed here; the Connecter handles retries.
    let event = SocketEvent::ConnectFailed {
      endpoint: endpoint.clone(),
      error_msg: format!("{}", error),
    };
    core_arc.core_state.lock().await.send_monitor_event(event).await;
  }

  async fn handle_pipe_message(core_arc: Arc<SocketCore>, socket_logic: &Arc<dyn ISocket>, pipe_id: usize, msg: Msg) {
    let handle = core_arc.handle;
    let msg_size_log = msg.size();
    tracing::trace!(
      handle = handle,
      pipe_id = pipe_id,
      msg_size = msg_size_log,
      "Handling PipeMessageReceived (helper)"
    );
    tracing::debug!(
      handle = handle,
      pipe_id = pipe_id,
      "Attempting to delegate PipeMessageReceived to ISocket::handle_pipe_event"
    );
    let event_cmd = Command::PipeMessageReceived { pipe_id, msg };
    let result = socket_logic.handle_pipe_event(pipe_id, event_cmd).await;
    tracing::debug!(
      handle = handle,
      pipe_id = pipe_id,
      "ISocket::handle_pipe_event returned: {:?}",
      result
    );
    if let Err(e) = result {
      tracing::error!(
        handle = handle,
        pipe_id = pipe_id,
        "Error handling PipeMessageReceived: {}",
        e
      );
    }
  }

  #[cfg(feature = "inproc")]
  async fn handle_inproc_connect(core_arc: Arc<SocketCore>, socket_logic: &Arc<dyn ISocket>, command: Command) {
    let handle = core_arc.handle;
    if let Command::InprocConnectRequest {
      connector_uri,
      reply_tx,
      connector_pipe_tx,
      connector_pipe_rx,
      connector_pipe_write_id,
      connector_pipe_read_id,
    } = command
    {
      tracing::debug!(handle = handle, %connector_uri, "Handling InprocConnectRequest (helper)");
      let accept_result: Result<(), ZmqError> = Ok(()); // Simple accept
      if accept_result.is_ok() {
        let child_actor_handle = core_arc.context.inner().next_handle(); // Generate handle ID for endpoint info
        let mut binder_state = core_arc.core_state.lock().await;
        binder_state.pipes_tx.insert(connector_pipe_write_id, connector_pipe_tx);
        let reader_task = tokio::spawn(Self::run_pipe_reader_task(
          handle,
          core_arc.mailbox_sender().clone(),
          connector_pipe_read_id,
          connector_pipe_rx,
        ));
        binder_state
          .pipe_reader_task_handles
          .insert(connector_pipe_read_id, reader_task);

        let info = EndpointInfo {
          mailbox: core_arc.mailbox_sender().clone(), // TODO: Maybe store connector mailbox if available?
          task_handle: tokio::spawn(async {}),        // Dummy task handle for inproc
          endpoint_type: EndpointType::Session,
          endpoint_uri: connector_uri.clone(),
          pipe_ids: Some((connector_pipe_write_id, connector_pipe_read_id)),
          handle_id: child_actor_handle,                    // Store handle ID
          target_endpoint_uri: Some(connector_uri.clone()), // Inproc target is same as resolved
        };
        binder_state.endpoints.insert(connector_uri.clone(), info);

        let monitor_tx_clone = binder_state.get_monitor_sender_clone();
        drop(binder_state);
        if let Some(tx) = monitor_tx_clone {
          let event = SocketEvent::Connected {
            endpoint: connector_uri.clone(),
            peer_addr: format!("inproc-binder-handle-{}", handle),
          };
          if tx.send(event).await.is_err() { /* warn */ }
        }
        socket_logic
          .pipe_attached(connector_pipe_read_id, connector_pipe_write_id, None)
          .await;
        tracing::info!(binder_handle = handle, %connector_uri, "Inproc connection accepted");
      } else {
        let event = SocketEvent::ConnectFailed {
          endpoint: connector_uri.clone(),
          error_msg: accept_result
            .as_ref()
            .err()
            .map_or("Binder rejected".to_string(), |e| format!("{}", e)),
        };
        core_arc.core_state.lock().await.send_monitor_event(event).await;
        tracing::warn!(binder_handle = handle, %connector_uri, "Inproc connection rejected");
      }
      let _ = reply_tx.send(accept_result);
    } else {
      tracing::error!(handle = handle, "Mismatched command type in handle_inproc_connect");
    }
  }

  // --- Cleanup Helpers ---
  async fn cleanup_session_state_by_uri(
    core_arc: Arc<SocketCore>,
    endpoint_uri: &str, // Resolved URI
    socket_logic: &Arc<dyn ISocket>,
  ) {
    // This function ONLY cleans up. Reconnect trigger is handled in the command loop.
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, uri = %endpoint_uri, "Attempting cleanup by URI");
    let mut state = core_arc.core_state.lock().await;
    let mut endpoint_removed = false;
    let mut detached_pipe_read_id: Option<usize> = None;
    let monitor_tx_clone = state.get_monitor_sender_clone();

    if let Some(removed_info) = state.endpoints.remove(endpoint_uri) {
      endpoint_removed = true;
      tracing::info!(handle = handle, uri = %endpoint_uri, type = ?removed_info.endpoint_type, child_handle=removed_info.handle_id, "Removed endpoint state by URI during cleanup");
      removed_info.task_handle.abort(); // Abort the session/listener task
      if let Some((write_pipe_id, read_pipe_id)) = removed_info.pipe_ids {
        detached_pipe_read_id = Some(read_pipe_id);
        if state.remove_pipe_state(write_pipe_id, read_pipe_id) {
          tracing::debug!(handle=handle, uri=%endpoint_uri, read_pipe=read_pipe_id, write_pipe=write_pipe_id, "Removed associated pipe state by URI");
        } else {
          tracing::warn!(handle=handle, uri=%endpoint_uri, read_pipe=read_pipe_id, write_pipe=write_pipe_id, "Cleanup by URI: Associated pipe state already removed?");
        }
      } else {
        tracing::debug!(handle = handle, uri=%endpoint_uri, "No pipe state associated with removed endpoint");
      }
    } else {
      tracing::warn!(handle = handle, uri=%endpoint_uri, "Cleanup by URI attempted, but endpoint state was not found");
    }
    drop(state);

    if endpoint_removed {
      if let Some(tx) = monitor_tx_clone {
        let event = SocketEvent::Disconnected {
          endpoint: endpoint_uri.to_string(),
        };
        if tx.send(event).await.is_err() {
          tracing::warn!(socket_handle = handle, uri=%endpoint_uri, "Failed to send Disconnected event");
        }
      }
    }
    if let Some(read_id) = detached_pipe_read_id {
      socket_logic.pipe_detached(read_id).await;
      tracing::debug!(handle = handle, uri = %endpoint_uri, pipe_read_id = read_id, "Notified ISocket of pipe detachment by URI");
    }
  }

  async fn cleanup_session_state_by_pipe(
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    // This function ONLY cleans up. Reconnect trigger is handled in the command loop.
    let handle = core_arc.handle;
    tracing::debug!(
      handle = handle,
      pipe_read_id = pipe_read_id,
      "Attempting cleanup by Pipe Read ID"
    );
    let mut state = core_arc.core_state.lock().await;
    let mut endpoint_uri_to_remove: Option<String> = None;
    let mut write_pipe_id_to_remove: Option<usize> = None;
    let mut task_handle_to_abort: Option<JoinHandle<()>> = None;
    let mut child_actor_handle_id: Option<usize> = None;
    let monitor_tx_clone = state.get_monitor_sender_clone();

    for (uri, info) in state.endpoints.iter() {
      if let Some((write_id, read_id)) = info.pipe_ids {
        if read_id == pipe_read_id {
          endpoint_uri_to_remove = Some(uri.clone());
          write_pipe_id_to_remove = Some(write_id);
          child_actor_handle_id = Some(info.handle_id);
          break;
        }
      }
    }

    let mut pipes_removed = false;
    if let (Some(ref uri), Some(write_pipe_id)) = (&endpoint_uri_to_remove, write_pipe_id_to_remove) {
      if let Some(removed_info) = state.endpoints.remove(uri) {
        tracing::info!(handle=handle, pipe_read_id=pipe_read_id, uri=%uri, type=?removed_info.endpoint_type, child_handle=removed_info.handle_id, "Removed endpoint state by Pipe during cleanup");
        task_handle_to_abort = Some(removed_info.task_handle);
      } else {
        tracing::warn!(handle=handle, pipe_read_id=pipe_read_id, uri=%uri, "Endpoint disappeared after finding it in iterator?");
      }
      pipes_removed = state.remove_pipe_state(write_pipe_id, pipe_read_id);
      if pipes_removed {
        tracing::debug!(
          handle = handle,
          pipe_read_id = pipe_read_id,
          "Removed pipe state by Pipe ID"
        );
      } else {
        tracing::warn!(
          handle = handle,
          pipe_id = pipe_read_id,
          "Cleanup by pipe attempted, but no pipe state found"
        );
      }
    } else {
      tracing::warn!(
        handle = handle,
        pipe_id = pipe_read_id,
        "Cleanup by pipe: Couldn't find associated endpoint/write pipe ID"
      );
      // Handle dangling reader task if endpoint info was already gone
      if let Some(reader_handle) = state.pipe_reader_task_handles.remove(&pipe_read_id) {
        reader_handle.abort();
        tracing::debug!(
          handle = handle,
          pipe_id = pipe_read_id,
          "Removed dangling pipe reader task handle by Pipe ID"
        );
        pipes_removed = true; // Mark pipes as removed if reader was cleaned up
      }
    }
    drop(state);

    if let Some(task_handle) = task_handle_to_abort {
      task_handle.abort(); // Abort the session task
    }

    if let (Some(uri), true) = (endpoint_uri_to_remove, pipes_removed) {
      if let Some(tx) = monitor_tx_clone {
        let event = SocketEvent::Disconnected { endpoint: uri };
        if tx.send(event).await.is_err() {
          tracing::warn!(
            socket_handle = handle,
            pipe_read_id = pipe_read_id,
            "Failed to send Disconnected event"
          );
        }
      }
    }
    if pipes_removed {
      socket_logic.pipe_detached(pipe_read_id).await;
      tracing::debug!(
        handle = handle,
        pipe_read_id = pipe_read_id,
        "Notified ISocket of pipe detachment by Pipe ID"
      );
    }
  }

  async fn cleanup_session_state_by_pipe_ids(
    core_arc: Arc<SocketCore>,
    pipe_write_id: usize,
    pipe_read_id: usize,
    _socket_logic: &Arc<dyn ISocket>, // Socket logic not needed for just cleanup
  ) {
    let handle = core_arc.handle;
    tracing::debug!(
      handle = handle,
      pipe_write_id = pipe_write_id,
      pipe_read_id = pipe_read_id,
      "Cleaning up pipe state after AttachPipe failure"
    );
    let mut state = core_arc.core_state.lock().await;
    if state.remove_pipe_state(pipe_write_id, pipe_read_id) {
      tracing::debug!(
        handle = handle,
        pipe_write_id = pipe_write_id,
        pipe_read_id = pipe_read_id,
        "Cleaned up pipe state"
      );
    } else {
      tracing::warn!(
        handle = handle,
        pipe_write_id = pipe_write_id,
        pipe_read_id = pipe_read_id,
        "Pipe state already gone during AttachPipe failure cleanup?"
      );
    }
    // Also remove endpoint info if it exists for these pipes
    let mut endpoint_to_remove: Option<String> = None;
    for (uri, info) in state.endpoints.iter() {
      if info.pipe_ids == Some((pipe_write_id, pipe_read_id)) {
        endpoint_to_remove = Some(uri.clone());
        break;
      }
    }
    if let Some(uri) = endpoint_to_remove {
      if let Some(removed_info) = state.endpoints.remove(&uri) {
        tracing::debug!(handle=handle, uri=%uri, "Removed endpoint state during AttachPipe failure cleanup");
        removed_info.task_handle.abort(); // Abort associated session task
      }
    }
  }

  // <<< ADDED HELPER FUNCTION >>>
  /// Spawns a new Connecter task for the given target URI.
  async fn respawn_connecter(core_arc: Arc<SocketCore>, target_uri: String) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, target_uri=%target_uri, "Respawning connecter task");

    let parse_result = parse_endpoint(&target_uri);
    match parse_result {
      Ok(Endpoint::Tcp(_, _)) => {
        let state = core_arc.core_state.lock().await;
        let options_arc = Arc::new(state.options.clone());
        let monitor_tx_clone = state.get_monitor_sender_clone();
        let handle_source = core_arc.context.inner().next_handle.clone();
        // Generate a *new* handle ID for this connecter task itself
        let connecter_handle_id = core_arc.context.inner().next_handle();
        drop(state); // Release lock before spawning

        // Spawn the connecter task, but we don't store its EndpointInfo here.
        // It will report back via ConnSuccess or ConnFailed normally.
        let _task = TcpConnecter::create_and_spawn(
          connecter_handle_id, // Use new handle for the connecter actor itself
          target_uri.clone(),  // Use the original target URI
          options_arc,
          core_arc.mailbox_sender().clone(),
          handle_source,
          monitor_tx_clone,
        );
        tracing::info!(handle=handle, connecter_handle=connecter_handle_id, target_uri=%target_uri, "Spawned new TcpConnecter for automatic reconnect");
      }
      #[cfg(feature = "ipc")]
      Ok(Endpoint::Ipc(path, _)) => {
        let state = core_arc.core_state.lock().await;
        let options_arc = Arc::new(state.options.clone());
        let monitor_tx_clone = state.get_monitor_sender_clone();
        let handle_source = core_arc.context.inner().next_handle.clone();
        let connecter_handle_id = core_arc.context.inner().next_handle();
        drop(state);

        let (_mailbox, _task) = IpcConnecter::create_and_spawn(
          connecter_handle_id,
          target_uri.clone(), // Pass original URI
          path,               // Pass parsed path
          options_arc,
          core_arc.mailbox_sender().clone(),
          handle_source,
          monitor_tx_clone,
        );
        tracing::info!(handle=handle, connecter_handle=connecter_handle_id, target_uri=%target_uri, "Spawned new IpcConnecter for automatic reconnect");
      }
      Ok(Endpoint::Inproc(_)) => {
        tracing::warn!(handle=handle, target_uri=%target_uri, "Automatic reconnect for inproc not implemented/meaningful.");
      }
      Err(e) => {
        tracing::error!(handle=handle, target_uri=%target_uri, error=%e, "Failed to parse endpoint for respawning connecter");
      }
    }
  }
  // <<< ADDED HELPER FUNCTION END >>>

  // --- Option Handling ---
  async fn handle_set_option(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
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
      SUBSCRIBE | UNSUBSCRIBE | ROUTING_ID => match socket_logic.set_pattern_option(option, value).await {
        Ok(()) => return Ok(()),
        Err(ZmqError::UnsupportedOption(_)) => {}
        Err(e) => return Err(e),
      },
      _ => {}
    }
    let mut state = core_arc.core_state.lock().await;
    match option {
      SNDHWM => {
        let hwm = parse_i32_option(value)?;
        if hwm < 0 {
          return Err(ZmqError::InvalidOptionValue(option));
        }
        state.options.sndhwm = hwm as usize;
        tracing::info!(handle = core_arc.handle, hwm = state.options.sndhwm, "Set SNDHWM");
      }
      RCVHWM => {
        let hwm = parse_i32_option(value)?;
        if hwm < 0 {
          return Err(ZmqError::InvalidOptionValue(option));
        }
        state.options.rcvhwm = hwm as usize;
        tracing::info!(handle = core_arc.handle, hwm = state.options.rcvhwm, "Set RCVHWM");
      }
      LINGER => {
        state.options.linger = options::parse_linger_option(value)?;
        tracing::info!(handle=core_arc.handle, linger=?state.options.linger, "Set LINGER");
      }
      RECONNECT_IVL => {
        state.options.reconnect_ivl = options::parse_reconnect_ivl_option(value)?;
        tracing::info!(handle=core_arc.handle, ivl=?state.options.reconnect_ivl, "Set RECONNECT_IVL");
      }
      RECONNECT_IVL_MAX => {
        state.options.reconnect_ivl_max = options::parse_reconnect_ivl_max_option(value)?;
        tracing::info!(handle=core_arc.handle, max_ivl=?state.options.reconnect_ivl_max, "Set RECONNECT_IVL_MAX");
      }
      HEARTBEAT_IVL => {
        state.options.heartbeat_ivl = parse_heartbeat_option(value, option)?;
        tracing::info!(handle=core_arc.handle, ivl=?state.options.heartbeat_ivl, "Set HEARTBEAT_IVL");
      }
      HEARTBEAT_TIMEOUT => {
        state.options.heartbeat_timeout = parse_heartbeat_option(value, option)?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.heartbeat_timeout, "Set HEARTBEAT_TIMEOUT");
      }
      ZAP_DOMAIN => {
        state.options.zap_domain =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle=core_arc.handle, domain=?state.options.zap_domain, "Set ZAP_DOMAIN");
      }
      PLAIN_SERVER => {
        state.options.plain_server = Some(parse_bool_option(value)?);
        tracing::info!(handle=core_arc.handle, server=?state.options.plain_server, "Set PLAIN_SERVER");
      }
      PLAIN_USERNAME => {
        state.options.plain_username =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set PLAIN_USERNAME (len {})", value.len());
      }
      PLAIN_PASSWORD => {
        state.options.plain_password =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
        tracing::info!(handle = core_arc.handle, "Set PLAIN_PASSWORD (len {})", value.len());
      }
      #[cfg(feature = "curve")]
      CURVE_SERVER => {
        state.options.curve_server = Some(parse_bool_option(value)?);
        tracing::info!(handle=core_arc.handle, server=?state.options.curve_server, "Set CURVE_SERVER");
      }
      #[cfg(feature = "curve")]
      CURVE_PUBLICKEY => {
        state.options.curve_public_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_PUBLICKEY");
      }
      #[cfg(feature = "curve")]
      CURVE_SECRETKEY => {
        state.options.curve_secret_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_SECRETKEY");
      }
      #[cfg(feature = "curve")]
      CURVE_SERVERKEY => {
        state.options.curve_server_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
        tracing::info!(handle = core_arc.handle, "Set CURVE_SERVERKEY");
      }
      ROUTING_ID => {
        state.options.routing_id = Some(parse_blob_option(value)?);
        tracing::info!(handle = core_arc.handle, "Set ROUTING_ID (core state)");
      }
      RCVTIMEO => {
        state.options.rcvtimeo = parse_timeout_option(value, option)?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.rcvtimeo, "Set RCVTIMEO");
      }
      SNDTIMEO => {
        state.options.sndtimeo = parse_timeout_option(value, option)?;
        tracing::info!(handle=core_arc.handle, timeout=?state.options.sndtimeo, "Set SNDTIMEO");
      }
      TCP_KEEPALIVE => {
        state.options.tcp_keepalive_enabled = parse_keepalive_mode_option(value)?;
        tracing::info!(
          handle = core_arc.handle,
          mode = state.options.tcp_keepalive_enabled,
          "Set TCP_KEEPALIVE"
        );
      }
      TCP_KEEPALIVE_IDLE => {
        state.options.tcp_keepalive_idle = parse_secs_duration_option(value)?;
        tracing::info!(handle=core_arc.handle, duration=?state.options.tcp_keepalive_idle, "Set TCP_KEEPALIVE_IDLE");
      }
      TCP_KEEPALIVE_CNT => {
        state.options.tcp_keepalive_count = parse_u32_option(value)?;
        tracing::info!(handle=core_arc.handle, count=?state.options.tcp_keepalive_count, "Set TCP_KEEPALIVE_CNT");
      }
      TCP_KEEPALIVE_INTVL => {
        state.options.tcp_keepalive_interval = parse_secs_duration_option(value)?;
        tracing::info!(handle=core_arc.handle, duration=?state.options.tcp_keepalive_interval, "Set TCP_KEEPALIVE_INTVL");
      }
      ROUTER_MANDATORY => {
        state.options.router_mandatory = parse_bool_option(value)?;
        tracing::info!(handle=core_arc.handle, mandatory=?state.options.router_mandatory, "Set ROUTER_MANDATORY");
      }
      SUBSCRIBE | UNSUBSCRIBE => {
        tracing::warn!(
          handle = core_arc.handle,
          option = option,
          "Pattern does not support this option"
        );
        return Err(ZmqError::UnsupportedOption(option));
      }
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

  async fn handle_get_option(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    option: i32,
  ) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(handle = core_arc.handle, option = option, "Getting option");
    match option {
      ROUTING_ID => match socket_logic.get_pattern_option(option).await {
        Ok(v) => return Ok(v),
        Err(ZmqError::UnsupportedOption(_)) => {}
        Err(e) => return Err(e),
      },
      _ => {}
    }
    let state = core_arc.core_state.lock().await;
    match option {
      SNDHWM => Ok((state.options.sndhwm as i32).to_ne_bytes().to_vec()),
      RCVHWM => Ok((state.options.rcvhwm as i32).to_ne_bytes().to_vec()),
      LINGER => {
        let v = match state.options.linger {
          None => -1,
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX),
        };
        Ok(v.to_ne_bytes().to_vec())
      }
      RECONNECT_IVL => {
        let v = state.options.reconnect_ivl.map_or(0, |d| d.as_millis() as i32);
        Ok(v.to_ne_bytes().to_vec())
      }
      RECONNECT_IVL_MAX => {
        let v = state.options.reconnect_ivl_max.map_or(0, |d| d.as_millis() as i32);
        Ok(v.to_ne_bytes().to_vec())
      }
      HEARTBEAT_IVL => {
        let v = state.options.heartbeat_ivl.map_or(0, |d| d.as_millis() as i32);
        Ok(v.to_ne_bytes().to_vec())
      }
      HEARTBEAT_TIMEOUT => {
        let v = state.options.heartbeat_timeout.map_or(0, |d| d.as_millis() as i32);
        Ok(v.to_ne_bytes().to_vec())
      }
      ZAP_DOMAIN => state
        .options
        .zap_domain
        .as_ref()
        .map(|s| s.as_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("ZAP_DOMAIN not set".into())),
      PLAIN_SERVER => state
        .options
        .plain_server
        .map(|b| (b as i32).to_ne_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("PLAIN_SERVER not set".into())),
      PLAIN_USERNAME => state
        .options
        .plain_username
        .as_ref()
        .map(|s| s.as_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("PLAIN_USERNAME not set".into())),
      #[cfg(feature = "curve")]
      CURVE_SERVER => state
        .options
        .curve_server
        .map(|b| (b as i32).to_ne_bytes().to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_SERVER not set".into())),
      #[cfg(feature = "curve")]
      CURVE_PUBLICKEY => state
        .options
        .curve_public_key
        .map(|k| k.to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_PUBLICKEY not set".into())),
      #[cfg(feature = "curve")]
      CURVE_SERVERKEY => state
        .options
        .curve_server_key
        .map(|k| k.to_vec())
        .ok_or(ZmqError::InvalidState("CURVE_SERVERKEY not set".into())),
      ROUTING_ID => state
        .options
        .routing_id
        .as_ref()
        .map(|b| b.to_vec())
        .ok_or(ZmqError::InvalidState("ROUTING_ID not set".into())),
      RCVTIMEO => {
        let v = match state.options.rcvtimeo {
          None => -1,
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX),
        };
        Ok(v.to_ne_bytes().to_vec())
      }
      SNDTIMEO => {
        let v = match state.options.sndtimeo {
          None => -1,
          Some(d) => d.as_millis().try_into().unwrap_or(i32::MAX),
        };
        Ok(v.to_ne_bytes().to_vec())
      }
      TCP_KEEPALIVE => Ok(state.options.tcp_keepalive_enabled.to_ne_bytes().to_vec()),
      TCP_KEEPALIVE_IDLE => Ok(
        state
          .options
          .tcp_keepalive_idle
          .map_or(0, |d| d.as_secs() as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      TCP_KEEPALIVE_CNT => Ok(
        state
          .options
          .tcp_keepalive_count
          .map_or(0, |c| c as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      TCP_KEEPALIVE_INTVL => Ok(
        state
          .options
          .tcp_keepalive_interval
          .map_or(0, |d| d.as_secs() as i32)
          .to_ne_bytes()
          .to_vec(),
      ),
      ROUTER_MANDATORY => Ok((state.options.router_mandatory as i32).to_ne_bytes().to_vec()),
      16 => Ok((state.socket_type as i32).to_ne_bytes().to_vec()), // ZMQ_TYPE
      SUBSCRIBE | UNSUBSCRIBE => Err(ZmqError::UnsupportedOption(option)),
      _ => {
        tracing::warn!(
          handle = core_arc.handle,
          option = option,
          "Attempted to get unsupported option"
        );
        Err(ZmqError::UnsupportedOption(option))
      }
    }
  }
} // end impl SocketCore

// --- Helper: send_msg_with_timeout ---
pub(crate) async fn send_msg_with_timeout(
  pipe_tx: &AsyncSender<Msg>,
  msg: Msg,
  timeout_opt: Option<Duration>,
  socket_handle: usize,
  pipe_id: usize,
) -> Result<(), ZmqError> {
  match timeout_opt {
    None => {
      tracing::trace!(handle = socket_handle, pipe_id = pipe_id, "Sending message (blocking)");
      pipe_tx
        .send(msg)
        .await
        .map_err(|SendError(_)| ZmqError::ConnectionClosed)
    }
    Some(d) if d.is_zero() => {
      tracing::trace!(
        handle = socket_handle,
        pipe_id = pipe_id,
        "Attempting non-blocking send"
      );
      match pipe_tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
        Err(TrySendError::Closed(_)) => Err(ZmqError::ConnectionClosed),
      }
    }
    Some(duration) => {
      tracing::trace!(
        handle = socket_handle,
        pipe_id = pipe_id,
        ?duration,
        "Attempting timed send"
      );
      match timeout(duration, pipe_tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(SendError(_))) => Err(ZmqError::ConnectionClosed),
        Err(_) => Err(ZmqError::Timeout),
      }
    }
  }
}
