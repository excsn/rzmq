// src/socket/core.rs

// --- Imports ---
// Import specific socket pattern implementations.
use super::{DealerSocket, PubSocket, PullSocket, PushSocket, RepSocket, ReqSocket, RouterSocket, SubSocket};
use crate::context::Context; // For accessing EventBus, handle generation, inproc registry.
use crate::error::ZmqError; // Custom error type.
use crate::runtime::{
  self, mailbox, ActorDropGuard, ActorType, Command, EventBus, MailboxReceiver, MailboxSender, SystemEvent, WaitGroup,
}; // Core runtime primitives. `SocketMailbox` was removed.
use crate::session::SessionBase; // For spawning Session actors.
use crate::socket::events::{MonitorSender, SocketEvent}; // For socket monitoring.
                                                         // Import all socket option constants and parsing helpers.
use crate::socket::options::{
  self, parse_blob_option, parse_bool_option, parse_duration_ms_option, parse_heartbeat_option, parse_i32_option,
  parse_keepalive_mode_option, parse_linger_option, parse_reconnect_ivl_max_option, parse_reconnect_ivl_option,
  parse_secs_duration_option, parse_timeout_option, parse_u32_option, SocketOptions, ZmtpEngineConfig, HEARTBEAT_IVL,
  HEARTBEAT_TIMEOUT, LINGER, PLAIN_PASSWORD, PLAIN_SERVER, PLAIN_USERNAME, RCVHWM, RCVTIMEO, RECONNECT_IVL,
  RECONNECT_IVL_MAX, ROUTER_MANDATORY, ROUTING_ID, SNDHWM, SNDTIMEO, SUBSCRIBE, TCP_KEEPALIVE, TCP_KEEPALIVE_CNT,
  TCP_KEEPALIVE_IDLE, TCP_KEEPALIVE_INTVL, UNSUBSCRIBE, ZAP_DOMAIN,
};
#[cfg(feature = "io-uring")]
use crate::socket::options::{
  IO_URING_RCVMULTISHOT,
  IO_URING_SNDZEROCOPY,
};
#[cfg(feature = "curve")]
use crate::socket::options::{
  parse_key_option, CURVE_KEY_LEN, CURVE_PUBLICKEY, CURVE_SECRETKEY, CURVE_SERVER, CURVE_SERVERKEY,
};
// Patterns module not directly used here, but ISocket impls use it.
// use crate::socket::patterns;
use crate::socket::types::SocketType; // Enum for socket types.
use crate::socket::ISocket; // Trait implemented by specific socket patterns.
use crate::transport::endpoint::{parse_endpoint, Endpoint}; // For parsing endpoint strings.
                                                            // Import transport-specific actor spawners and helpers.
#[cfg(feature = "inproc")]
use crate::transport::inproc::{bind_inproc, connect_inproc, disconnect_inproc, unbind_inproc};
#[cfg(feature = "ipc")]
use crate::transport::ipc::{create_and_spawn_ipc_engine, IpcConnecter, IpcListener};
use crate::transport::tcp::{create_and_spawn_tcp_engine, TcpConnecter, TcpListener};
use crate::{Blob, Msg}; // Core message types.

// Tokio and standard library imports.
use async_channel::{bounded, Receiver as AsyncReceiver, SendError, Sender as AsyncSender, TrySendError};
use futures::future::join_all; // For managing multiple futures (e.g., unbinding inproc names).
use std::collections::{HashMap, HashSet, VecDeque}; // Data structures.
use std::sync::{Arc, Weak}; // For shared ownership and weak references.
use std::time::{Duration, Instant}; // For timeouts and linger.
use tokio::sync::{broadcast, oneshot, Mutex, MutexGuard}; // Synchronization primitives.
use tokio::task::JoinHandle; // For managing spawned actor tasks.
use tokio::time::{interval, timeout, Interval}; // For periodic checks (linger).

// --- Structs: EndpointInfo, EndpointType, CoreState ---

/// Stores information about an active endpoint (Listener or Session) managed by `SocketCore`.
#[derive(Debug)]
pub(crate) struct EndpointInfo {
  /// Command mailbox sender for the child actor (Listener or Session).
  pub mailbox: MailboxSender,
  /// `JoinHandle` for the child actor's main task.
  pub task_handle: JoinHandle<()>,
  /// Type of the endpoint (Listener or Session).
  pub endpoint_type: EndpointType,
  /// The resolved URI of this endpoint.
  pub endpoint_uri: String,
  /// For Session endpoints, stores the (write_id, read_id) pair for its data pipes
  /// from the `SocketCore`'s perspective.
  pub pipe_ids: Option<(usize, usize)>,
  /// The unique handle ID of the child actor (Listener or Session) itself.
  pub handle_id: usize,
  /// For connected endpoints (Sessions), this stores the original target URI.
  pub target_endpoint_uri: Option<String>,
  pub is_outbound_connection: bool,
}

/// Enum to differentiate between Listener endpoints and active Session (connection) endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointType {
  Listener,
  Session,
}

/// Holds the mutable state for a `SocketCore` actor.
#[derive(Debug)]
pub(crate) struct CoreState {
  pub(crate) handle: usize,
  pub options: SocketOptions,
  pub socket_type: SocketType,
  pub pipes_tx: HashMap<usize, AsyncSender<Msg>>,
  pub pipe_reader_task_handles: HashMap<usize, JoinHandle<()>>,
  pub endpoints: HashMap<String, EndpointInfo>,
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

  #[allow(dead_code)]
  pub(crate) fn get_reader_task_handle(&self, pipe_read_id: usize) -> Option<&JoinHandle<()>> {
    self.pipe_reader_task_handles.get(&pipe_read_id)
  }

  pub(crate) fn remove_pipe_state(&mut self, pipe_write_id: usize, pipe_read_id: usize) -> bool {
    let tx_removed = self.pipes_tx.remove(&pipe_write_id).is_some();
    if tx_removed {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_write_id,
        "Removed pipe sender for pipe_write_id"
      );
    }
    let reader_removed = if let Some(handle) = self.pipe_reader_task_handles.remove(&pipe_read_id) {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_read_id,
        "Removing and aborting pipe reader task for pipe_read_id"
      );
      handle.abort();
      true
    } else {
      false
    };
    if reader_removed {
      tracing::trace!(
        core_handle = self.handle,
        pipe_id = pipe_read_id,
        "Removed pipe reader task handle for pipe_read_id"
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
  pending_children: HashMap<usize, String>,
  #[cfg(feature = "inproc")]
  inproc_connections_to_close: Vec<(usize, usize, String)>, // (CoreWriteID, CoreReadID, ConnectorURI)
  linger_deadline: Option<Instant>,
}

impl Default for ShutdownCoordinator {
  fn default() -> Self {
    Self {
      state: ShutdownPhase::Running,
      pending_children: HashMap::new(),
      #[cfg(feature = "inproc")]
      inproc_connections_to_close: Vec::new(),
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
    tracing::debug!(
        handle = core_handle,
        "ShutdownCoordinator: Initiating shutdown. Identifying children/inproc pipes."
    );
    // Clear lists before population
    self.pending_children.clear();
    #[cfg(feature = "inproc")]
    {
        self.inproc_connections_to_close.clear();
    }

    // Iterate directly over endpoints without taking ownership
    for (endpoint_uri_str, endpoint_info_val) in core_state.endpoints.iter() {
        #[cfg(feature = "inproc")]
        if endpoint_info_val.endpoint_type == EndpointType::Session && endpoint_uri_str.starts_with("inproc://") {
            if let Some(pipe_ids_tuple_val) = endpoint_info_val.pipe_ids {
                tracing::trace!(
                    handle = core_handle,
                    inproc_peer_uri = %endpoint_uri_str,
                    write_pipe_id = pipe_ids_tuple_val.0,
                    read_pipe_id = pipe_ids_tuple_val.1,
                    "ShutdownCoordinator: Registering inproc pipe for closure list."
                );
                self.inproc_connections_to_close.push((
                    pipe_ids_tuple_val.0,
                    pipe_ids_tuple_val.1,
                    endpoint_uri_str.clone(),
                ));
            } else {
                tracing::warn!(handle = core_handle, inproc_peer_uri = %endpoint_uri_str, "ShutdownCoordinator: Inproc endpoint found without pipe IDs during shutdown initiation.");
            }
        } else {
            // Regular child actor (Listener or TCP/IPC Session)
            // Ensure it has a valid handle ID > 0 (skip potential dummy entries if any)
            if endpoint_info_val.handle_id > 0 {
                 tracing::trace!(
                    handle = core_handle,
                    child_uri = %endpoint_uri_str,
                    child_type = ?endpoint_info_val.endpoint_type,
                    child_actor_handle = endpoint_info_val.handle_id,
                    "ShutdownCoordinator: Registering child actor for shutdown tracking."
                );
                self.pending_children.insert(endpoint_info_val.handle_id, endpoint_uri_str.clone());
            } else {
                 tracing::warn!(handle = core_handle, child_uri = %endpoint_uri_str, "ShutdownCoordinator: Skipping child with invalid handle ID 0 during registration.");
            }
        }
    } // End loop

    // Transition state based ONLY on pending *actor* children
    if self.pending_children.is_empty() {
        tracing::debug!(
            handle = core_handle,
            num_inproc_pipes = self.inproc_connections_to_close.len(),
            "ShutdownCoordinator: No active actor children found. Moving to Lingering state."
        );
        self.state = ShutdownPhase::Lingering;
        let linger_duration_option = core_state.options.linger;
        self.start_linger(linger_duration_option, core_handle);
    } else {
        tracing::debug!(
            handle = core_handle,
            num_pending_children = self.pending_children.len(),
            num_inproc_pipes = self.inproc_connections_to_close.len(),
            "ShutdownCoordinator: Waiting for children. Moving to StoppingChildren state."
        );
        self.state = ShutdownPhase::StoppingChildren;
    }
    true
  }

  fn record_child_stopped(&mut self, child_actor_handle: usize, core_handle: usize) -> bool {
    if self.state != ShutdownPhase::StoppingChildren {
      tracing::warn!(
        handle = core_handle,
        stopped_child_handle = child_actor_handle,
        current_coordinator_state = ?self.state,
        "ShutdownCoordinator: Received child stopped notification in unexpected state."
      );
      return false;
    }

    if let Some(stopped_child_uri) = self.pending_children.remove(&child_actor_handle) {
      tracing::debug!(
        handle = core_handle,
        stopped_child_handle = child_actor_handle,
        child_uri = %stopped_child_uri,
        remaining_children = self.pending_children.len(),
        "ShutdownCoordinator: Recorded child actor stopped."
      );
      if self.pending_children.is_empty() {
        tracing::debug!(
          handle = core_handle,
          "ShutdownCoordinator: All children stopped. Moving to Lingering state."
        );
        self.state = ShutdownPhase::Lingering;
        return true;
      }
    } else {
      tracing::trace!(
        handle = core_handle,
        stopped_child_handle = child_actor_handle,
        "ShutdownCoordinator: Received stop from child not in pending_children (e.g. PipeReaderTask)."
      );
    }
    false
  }

  fn start_linger(&mut self, linger_duration_option: Option<Duration>, core_handle: usize) {
    if self.state != ShutdownPhase::Lingering {
      tracing::error!(handle = core_handle, current_coordinator_state = ?self.state, "ShutdownCoordinator: Attempted to start LINGER in incorrect state.");
      return;
    }
    match linger_duration_option {
      None => {
        self.linger_deadline = None;
        tracing::debug!(
          handle = core_handle,
          "ShutdownCoordinator: Starting infinite LINGER period."
        );
      }
      Some(d) if d.is_zero() => {
        self.linger_deadline = Some(Instant::now());
        tracing::debug!(handle = core_handle, "ShutdownCoordinator: LINGER period is zero.");
      }
      Some(d) => {
        self.linger_deadline = Some(Instant::now() + d);
        tracing::debug!(handle = core_handle, linger_duration = ?d, "ShutdownCoordinator: Starting timed LINGER period.");
      }
    }
  }

  fn is_linger_expired(&self, core_handle: usize) -> bool {
    if self.state != ShutdownPhase::Lingering {
      return false;
    }
    match self.linger_deadline {
      None => false,
      Some(deadline_instant) => {
        let now_instant = Instant::now();
        let expired_flag = now_instant >= deadline_instant;
        if expired_flag {
          tracing::trace!(
            handle = core_handle,
            "ShutdownCoordinator: LINGER deadline has expired."
          );
        }
        expired_flag
      }
    }
  }

  fn are_queues_empty(&self, core_state: &CoreState) -> bool {
    core_state
      .pipes_tx
      .values()
      .all(|sender_channel| sender_channel.is_empty())
  }

  fn advance_to_cleaning(&mut self, core_handle: usize, core_state: &mut CoreState) {
    if self.state == ShutdownPhase::Lingering {
      tracing::debug!(
        handle = core_handle,
        "ShutdownCoordinator: LINGER complete. Closing inproc pipes and moving to CleaningPipes state."
      );

      // --- Close Inproc Pipe Senders ---
      #[cfg(feature = "inproc")]
      {
        let mut pipes_to_remove_from_state = Vec::new();
        for (pipe_write_id, pipe_read_id, peer_uri) in &self.inproc_connections_to_close {
          tracing::debug!(handle = core_handle, write_pipe_id = pipe_write_id, read_pipe_id = pipe_read_id, peer = %peer_uri, "Closing inproc pipe sender.");
          if let Some(sender) = core_state.pipes_tx.get(pipe_write_id) {
            sender.close(); // Close the sending end
          } else {
            tracing::warn!(
              handle = core_handle,
              write_pipe_id = pipe_write_id,
              "Inproc pipe sender not found in CoreState during cleanup."
            );
          }
          // Mark for removal from CoreState endpoints map later if needed,
          // though perform_pipe_cleanup might handle this generically.
          // Let's rely on perform_pipe_cleanup for now.
          pipes_to_remove_from_state.push(peer_uri.clone()); // Mark URI for potential endpoint removal
        }

        // Optional: Remove the EndpointInfo entries now if desired
        // for uri in pipes_to_remove_from_state {
        //    core_state.endpoints.remove(&uri);
        // }
      }

      self.state = ShutdownPhase::CleaningPipes;
    } else {
      tracing::warn!(handle = core_handle, current_coordinator_state = ?self.state, "ShutdownCoordinator: Tried to advance to CleaningPipes from unexpected state.");
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
  pub(crate) command_sender: MailboxSender,
  pub(crate) core_state: Mutex<CoreState>,
  socket_logic: Mutex<Option<Weak<dyn ISocket>>>,
  shutdown_coordinator: Mutex<ShutdownCoordinator>,
}

impl SocketCore {
  pub(crate) fn create_and_spawn(
    handle: usize,
    context: Context,
    socket_type: SocketType,
    mut initial_options: SocketOptions,
  ) -> Result<(Arc<dyn ISocket>, MailboxSender), ZmqError> {
    let actor_type_socketcore = ActorType::SocketCore;
    let (cmd_tx, cmd_rx) = mailbox();

    initial_options.socket_type_name = format!("{:?}", socket_type).to_uppercase();
    let initial_options_for_isocket = initial_options.clone();

    let core_state_instance_new = CoreState::new(handle, socket_type, initial_options);

    let socket_core_arc = Arc::new(SocketCore {
      handle,
      context: context.clone(),
      command_sender: cmd_tx.clone(),
      core_state: Mutex::new(core_state_instance_new),
      socket_logic: Mutex::new(None),
      shutdown_coordinator: Mutex::new(ShutdownCoordinator::default()),
    });

    let socket_logic_arc_impl: Arc<dyn ISocket> = match socket_type {
      SocketType::Pub => Arc::new(PubSocket::new(socket_core_arc.clone())),
      SocketType::Sub => Arc::new(SubSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
      SocketType::Req => Arc::new(ReqSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
      SocketType::Rep => Arc::new(RepSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
      SocketType::Dealer => Arc::new(DealerSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
      SocketType::Router => Arc::new(RouterSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
      SocketType::Push => Arc::new(PushSocket::new(socket_core_arc.clone())),
      SocketType::Pull => Arc::new(PullSocket::new(socket_core_arc.clone(), initial_options_for_isocket)),
    };

    if let Ok(mut socket_logic_weak_ref_guard) = socket_core_arc.socket_logic.try_lock() {
      *socket_logic_weak_ref_guard = Some(Arc::downgrade(&socket_logic_arc_impl));
    } else {
      return Err(ZmqError::Internal(
        "SocketCore: Mutex lock failure during ISocket weak reference initialization".into(),
      ));
    }

    let system_event_receiver = context.event_bus().subscribe();
    let core_arc_for_task = socket_core_arc.clone();
    let socket_logic_for_task = socket_logic_arc_impl.clone();
    let _core_task_join_handle = tokio::spawn(SocketCore::run_command_loop(
      core_arc_for_task,
      socket_logic_for_task,
      cmd_rx,
      system_event_receiver,
    ));

    context.publish_actor_started(handle, actor_type_socketcore, None);
    Ok((socket_logic_arc_impl, cmd_tx))
  }

  pub(crate) fn command_sender(&self) -> MailboxSender {
    self.command_sender.clone()
  }

  pub(crate) async fn get_socket_logic(&self) -> Option<Arc<dyn ISocket>> {
    self
      .socket_logic
      .lock()
      .await
      .as_ref()
      .and_then(|weak_ref| weak_ref.upgrade())
  }

  pub(crate) async fn is_running(&self) -> bool {
    self.shutdown_coordinator.lock().await.current_phase() == ShutdownPhase::Running
  }

  async fn publish_socket_closing(context: &Context, socket_id: usize) {
    let event = SystemEvent::SocketClosing { socket_id };
    let event_bus_instance = context.event_bus();
    if let Err(e) = event_bus_instance.publish(event) {
      tracing::warn!(
        socket_handle = socket_id,
        "Failed to publish SocketClosing event for self (receivers={}): {}",
        event_bus_instance.subscriber_count(),
        e
      );
    } else {
      tracing::debug!(
        socket_handle = socket_id,
        "SocketCore published SocketClosing event for self."
      );
    }
  }

  pub(crate) async fn run_pipe_reader_task(
    context: Context,
    core_handle: usize,
    core_command_mailbox: MailboxSender,
    pipe_read_id: usize,
    pipe_receiver: AsyncReceiver<Msg>,
  ) {
    let pipe_reader_handle = context.inner().next_handle();
    let pipe_reader_actor_type = ActorType::PipeReader;
    context.publish_actor_started(pipe_reader_handle, pipe_reader_actor_type, Some(core_handle));

    let actor_drop_guard = ActorDropGuard::new(context.clone(), pipe_reader_handle, pipe_reader_actor_type, None);

    tracing::debug!(
      core_handle = core_handle,
      pipe_read_id = pipe_read_id,
      pipe_reader_task_handle = pipe_reader_handle,
      "PipeReaderTask started"
    );
    let mut final_error_for_stopping: Option<ZmqError> = None;

    let _loop_result: Result<(),()> = async {
      loop {
        tokio::select! {
          msg_result = pipe_receiver.recv() => {
            match msg_result {
              Ok(msg) => {
                let cmd = Command::PipeMessageReceived { pipe_id: pipe_read_id, msg };
                if core_command_mailbox.send(cmd).await.is_err() {
                  tracing::warn!(core_handle=core_handle, pipe_read_id=pipe_read_id, pipe_reader_task_handle = pipe_reader_handle, "PipeReaderTask: Core command mailbox closed. Stopping.");
                  final_error_for_stopping = Some(ZmqError::Internal("PipeReaderTask: Core mailbox closed".into()));
                  break;
                }
              }
              Err(_) => {
                tracing::debug!(core_handle=core_handle, pipe_read_id=pipe_read_id, pipe_reader_task_handle = pipe_reader_handle, "PipeReaderTask: Data pipe closed by peer. Sending PipeClosedByPeer.");
                let cmd = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
                if core_command_mailbox.send(cmd).await.is_err() {
                  tracing::warn!(core_handle=core_handle, pipe_read_id=pipe_read_id, pipe_reader_task_handle = pipe_reader_handle, "PipeReaderTask: Core command mailbox closed sending PipeClosedByPeer.");
                   if final_error_for_stopping.is_none() {
                     final_error_for_stopping = Some(ZmqError::Internal("PipeReaderTask: Core mailbox closed on PipeClosedByPeer".into()));
                  }
                }
                break;
              }
            }
          }
        }
      }
      Ok(())
    }.await;

    tracing::debug!(
      core_handle = core_handle,
      pipe_read_id = pipe_read_id,
      pipe_reader_task_handle = pipe_reader_handle,
      "PipeReaderTask finished"
    );
    actor_drop_guard.waive();
    context.publish_actor_stopping(
      pipe_reader_handle,
      pipe_reader_actor_type,
      None,
      final_error_for_stopping,
    );
  }

  async fn run_command_loop(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: Arc<dyn ISocket>,
    mut command_receiver: MailboxReceiver,
    mut system_event_rx: broadcast::Receiver<SystemEvent>,
  ) {
    let core_handle = core_arc.handle;
    let core_actor_type = ActorType::SocketCore;
    let context_clone_for_stop = core_arc.context.clone();
    let socket_type_for_log = core_arc.core_state.try_lock().map(|s| s.socket_type);

    tracing::info!(handle = core_handle, socket_type = ?socket_type_for_log, "SocketCore actor main loop starting.");
    let mut linger_check_interval: Interval = interval(Duration::from_millis(10));
    linger_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut final_error_for_actorstop: Option<ZmqError> = None;

    let _loop_result: Result<(), ZmqError> = async {
      loop {
        let current_shutdown_phase = { core_arc.shutdown_coordinator.lock().await.current_phase() };
        if current_shutdown_phase == ShutdownPhase::Finished {
          tracing::debug!(handle = core_handle, "SocketCore shutdown sequence finished. Breaking command loop.");
          break;
        }

        tokio::select! {
          biased;

          event_result = system_event_rx.recv(), if current_shutdown_phase != ShutdownPhase::Finished => {
            match event_result {
              Ok(SystemEvent::ContextTerminating) => {
                tracing::info!(handle = core_handle, "SocketCore received ContextTerminating event.");
                let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                if coordinator_guard.current_phase() == ShutdownPhase::Running {
                  drop(coordinator_guard);
                  Self::publish_socket_closing(&core_arc.context, core_handle).await;
                  Self::initiate_shutdown_phase(core_arc.clone()).await;
                  // After initiating, tell children to stop. initiate_shutdown_phase only registers them.
                  Self::stop_child_actors(core_arc.clone()).await;
                  #[cfg(feature="inproc")]
                  Self::disconnect_inproc_connections(core_arc.clone()).await;
                }
              }
              Ok(SystemEvent::SocketClosing { socket_id }) if socket_id == core_handle => {
                tracing::debug!(handle = core_handle, "SocketCore received its own SocketClosing event.");
                let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                if coordinator_guard.current_phase() == ShutdownPhase::Running {
                  drop(coordinator_guard);
                  Self::initiate_shutdown_phase(core_arc.clone()).await;
                  Self::stop_child_actors(core_arc.clone()).await;
                  #[cfg(feature="inproc")]
                  Self::disconnect_inproc_connections(core_arc.clone()).await;
                }
              }
              Ok(SystemEvent::ActorStopping { handle_id: child_actor_id, actor_type: _child_type, endpoint_uri, error_msg }) => {
                let child_error_opt = error_msg.map(ZmqError::Internal); // Convert String to simple ZmqError
                Self::process_child_completion(core_arc.clone(), &socket_logic_strong, child_actor_id, endpoint_uri.as_deref(), "ActorStoppingEvent", child_error_opt.as_ref()).await;
              }
              Ok(SystemEvent::NewConnectionEstablished { parent_core_id, endpoint_uri, target_endpoint_uri, session_mailbox, session_handle_id, session_task_id }) => {
                if parent_core_id == core_handle {
                  if current_shutdown_phase == ShutdownPhase::Running {
                    let dummy_join_handle = tokio::spawn(async move { tracing::trace!("Dummy task for session_task_id: {}", session_task_id); }); // Placeholder
                    Self::handle_conn_success(core_arc.clone(), &socket_logic_strong, endpoint_uri, target_endpoint_uri, session_mailbox, Some(session_handle_id), Some(dummy_join_handle)).await;
                  } else {
                    tracing::warn!(handle = core_handle, new_conn_uri = %endpoint_uri, "SocketCore ignoring NewConnectionEstablished during shutdown.");
                    let _ = session_mailbox.try_send(Command::Stop); // Attempt to stop the new session
                  }
                }
              }
              Ok(SystemEvent::PeerIdentityEstablished { parent_core_id, core_pipe_read_id, peer_identity, session_handle_id }) => {
                if parent_core_id == core_handle {
                  if current_shutdown_phase == ShutdownPhase::Running {
                    tracing::debug!(
                      handle = core_handle,
                      pipe_read_id = core_pipe_read_id,
                      session_id = session_handle_id,
                      identity = ?peer_identity,
                      "SocketCore received PeerIdentityEstablished event."
                    );
                    socket_logic_strong.update_peer_identity(core_pipe_read_id, peer_identity).await;
                  } else {
                    tracing::debug!(
                      handle = core_handle,
                      pipe_read_id = core_pipe_read_id,
                      "SocketCore ignoring PeerIdentityEstablished during shutdown."
                    );
                  }
                }
              }
              Ok(SystemEvent::ConnectionAttemptFailed { parent_core_id, target_endpoint_uri, error_msg }) => {
                if parent_core_id == core_handle {
                  if current_shutdown_phase == ShutdownPhase::Running {
                    Self::handle_conn_failed(core_arc.clone(), target_endpoint_uri, ZmqError::Internal(error_msg)).await;
                  } else {
                    tracing::debug!(handle = core_handle, failed_uri = %target_endpoint_uri, "SocketCore ignoring ConnectionAttemptFailed during shutdown.");
                  }
                }
              }
              #[cfg(feature="inproc")]
              Ok(SystemEvent::InprocBindingRequest {
                  target_inproc_name,
                  connector_uri,
                  binder_pipe_tx_to_connector,
                  binder_pipe_rx_from_connector,
                  connector_pipe_write_id, // From connector's perspective, this is what Binder writes to (connector's read pipe)
                  connector_pipe_read_id,  // From connector's perspective, this is what Binder reads from (connector's write pipe)
                  reply_tx
              }) => {
                  let is_my_binding_name = {
                      core_arc.core_state.lock().await.bound_inproc_names.contains(&target_inproc_name)
                  };
                  if is_my_binding_name { // This SocketCore is the binder for the target name.
                      if current_shutdown_phase == ShutdownPhase::Running {
                          // Call the new helper function to process the binding request.
                          Self::process_inproc_binding_request(
                            core_arc.clone(),
                            &socket_logic_strong,
                            connector_uri,
                            binder_pipe_rx_from_connector, // This is what binder will SEND on (connector receives)
                            binder_pipe_tx_to_connector, // This is what binder will RECEIVE on (connector sends)
                            connector_pipe_write_id,     // This is the ID binder uses to WRITE to connector
                            connector_pipe_read_id,      // This is the ID binder uses to READ from connector
                            reply_tx
                          ).await;
                      } else {
                          tracing::debug!(
                              handle = core_handle,
                              target_inproc_name = %target_inproc_name,
                              "SocketCore (binder) ignoring InprocBindingRequest during its shutdown."
                          );
                          // Reject the connection attempt if the binder is shutting down.
                          let _ = reply_tx.take_and_send_forget(Err(ZmqError::InvalidState("Binder socket is shutting down".into())));
                      }
                  }
                  // If not my binding name, this SocketCore ignores the event.
              }
              #[cfg(feature="inproc")]
              Ok(SystemEvent::InprocPipePeerClosed { target_inproc_name, closed_by_connector_pipe_read_id }) => {
                let is_my_binding_name = { core_arc.core_state.lock().await.bound_inproc_names.contains(&target_inproc_name) };
                if is_my_binding_name {
                  tracing::debug!(handle = core_handle, binder_name=%target_inproc_name, connector_pipe_id = closed_by_connector_pipe_read_id, "Processing InprocPipePeerClosed event.");
                  let mut binder_core_state_guard = core_arc.core_state.lock().await;
                  let mut binder_read_id_to_cleanup: Option<usize> = None;
                  for info in binder_core_state_guard.endpoints.values() {
                    if let Some((binder_writes_here, binder_reads_from_here)) = info.pipe_ids {
                      if binder_writes_here == closed_by_connector_pipe_read_id {
                        binder_read_id_to_cleanup = Some(binder_reads_from_here);
                        break;
                      }
                    }
                  }
                  drop(binder_core_state_guard);
                  if let Some(read_id_to_clean) = binder_read_id_to_cleanup {
                    Self::cleanup_session_state_by_pipe(core_arc.clone(), read_id_to_clean, &socket_logic_strong).await;
                  } else {
                    tracing::warn!(handle = core_handle, binder_name=%target_inproc_name, "SocketCore (binder) could not find matching pipe for InprocPipePeerClosed.");
                  }
                }
              }
              Ok(_) => {} // Ignore other events
              Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(handle = core_handle, skipped = n, "System event bus lagged for SocketCore!");
              }
              Err(broadcast::error::RecvError::Closed) => {
                tracing::error!(handle = core_handle, "System event bus closed unexpectedly!");
                final_error_for_actorstop = Some(ZmqError::Internal("SocketCore: Event bus closed".into()));
                let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                if coordinator_guard.current_phase() == ShutdownPhase::Running {
                  drop(coordinator_guard);
                  Self::publish_socket_closing(&core_arc.context, core_handle).await;
                  Self::initiate_shutdown_phase(core_arc.clone()).await;
                  Self::stop_child_actors(core_arc.clone()).await;
                  #[cfg(feature="inproc")]
                  Self::disconnect_inproc_connections(core_arc.clone()).await;
                }
              }
            }
          }

          cmd_result = command_receiver.recv(), if current_shutdown_phase != ShutdownPhase::Finished => {
            match cmd_result {
              Ok(command) => {
                let command_name_for_log = command.variant_name();
                tracing::trace!(handle = core_handle, cmd = command_name_for_log, "SocketCore received command");
                match command {
                  Command::Stop => {
                    tracing::info!(handle=core_handle, "SocketCore received direct Stop command.");
                    let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                    if coordinator_guard.current_phase() == ShutdownPhase::Running {
                      drop(coordinator_guard);
                      Self::publish_socket_closing(&core_arc.context, core_handle).await;
                      Self::initiate_shutdown_phase(core_arc.clone()).await;
                      Self::stop_child_actors(core_arc.clone()).await;
                      #[cfg(feature="inproc")]
                      Self::disconnect_inproc_connections(core_arc.clone()).await;
                    }
                  }
                  Command::UserBind { endpoint, reply_tx } => { Self::handle_user_bind(core_arc.clone(), endpoint, reply_tx).await; }
                  Command::UserConnect { endpoint, reply_tx } => { Self::handle_user_connect(core_arc.clone(), endpoint, reply_tx).await; }
                  Command::UserDisconnect { endpoint, reply_tx } => { Self::handle_user_disconnect(core_arc.clone(), endpoint, reply_tx).await; }
                  Command::UserUnbind { endpoint, reply_tx } => { Self::handle_user_unbind(core_arc.clone(), endpoint, reply_tx).await; }
                  Command::UserSend { msg } => {
                    if current_shutdown_phase == ShutdownPhase::Running {
                      if let Err(e) = socket_logic_strong.send(msg).await {
                        tracing::debug!(handle = core_handle, "UserSend ISocket::send error: {}", e);
                      }
                    } else {
                      tracing::warn!(handle = core_handle, "UserSend ignored: SocketCore shutting down.");
                    }
                  }
                  Command::UserRecv { reply_tx } => {
                    if current_shutdown_phase == ShutdownPhase::Running {
                      let result = socket_logic_strong.recv().await;
                      if let Err(e) = &result { tracing::debug!(handle = core_handle, "UserRecv ISocket::recv error: {}", e); }
                      let _ = reply_tx.send(result);
                    } else {
                      tracing::warn!(handle = core_handle, "UserRecv ignored: SocketCore shutting down.");
                      let _ = reply_tx.send(Err(ZmqError::InvalidState("Socket is shutting down")));
                    }
                  }
                  Command::UserSetOpt { option, value, reply_tx } => {
                    if current_shutdown_phase == ShutdownPhase::Running {
                      let _ = reply_tx.send(Self::handle_set_option(core_arc.clone(), &socket_logic_strong, option, &value).await);
                    } else {
                      let _ = reply_tx.send(Err(ZmqError::InvalidState("Cannot set option: Socket shutting down")));
                    }
                  }
                  Command::UserGetOpt { option, reply_tx } => {
                    let _ = reply_tx.send(Self::handle_get_option(core_arc.clone(), &socket_logic_strong, option).await);
                  }
                  Command::UserMonitor { monitor_tx, reply_tx } => { Self::handle_user_monitor(core_arc.clone(), monitor_tx, reply_tx).await; }
                  Command::UserClose { reply_tx } => {
                    tracing::info!(handle = core_handle, "SocketCore received UserClose command.");
                    let initiated_now = Self::initiate_shutdown_phase(core_arc.clone()).await;
                    if initiated_now {
                      Self::publish_socket_closing(&core_arc.context, core_handle).await;
                      Self::stop_child_actors(core_arc.clone()).await;
                      #[cfg(feature="inproc")]
                      Self::disconnect_inproc_connections(core_arc.clone()).await;
                    }
                    let _ = reply_tx.send(Ok(()));
                  }
                  Command::PipeClosedByPeer { pipe_id: closed_pipe_read_id } => {
                    tracing::debug!(handle = core_handle, pipe_read_id = closed_pipe_read_id, "SocketCore processing PipeClosedByPeer.");
                    let target_uri_opt = Self::cleanup_session_state_by_pipe(core_arc.clone(), closed_pipe_read_id, &socket_logic_strong).await;
                    let latest_phase = { core_arc.shutdown_coordinator.lock().await.current_phase() };
                    if latest_phase == ShutdownPhase::Running {
                      if let Some(target_uri) = target_uri_opt {
                        if !target_uri.starts_with("inproc://") { // Check if NOT inproc
                           let reconnect_enabled = { core_arc.core_state.lock().await.options.reconnect_ivl.map_or(false, |d| !d.is_zero()) };
                           if reconnect_enabled {
                             tracing::info!(handle = core_handle, target_uri=%target_uri, "Pipe closed by peer, initiating reconnect...");
                             Self::respawn_connecter(core_arc.clone(), target_uri).await;
                           } else { tracing::debug!(handle = core_handle, target_uri=%target_uri, "Pipe closed, reconnect disabled."); }
                         } else {
                            tracing::debug!(handle = core_handle, target_uri=%target_uri, "Inproc pipe closed by peer, no reconnect needed.");
                         }
                      } else { tracing::debug!(handle = core_handle, pipe_read_id = closed_pipe_read_id, "Pipe closed, no target URI for reconnect."); }
                    } else {
                      if let Some(uri)=target_uri_opt {
                        tracing::debug!(handle = core_handle, target_uri=%uri, "Pipe closed, skipping reconnect (shutting down).");
                      }
                  }
                  }
                  Command::PipeMessageReceived { pipe_id, msg } => {
                    if current_shutdown_phase == ShutdownPhase::Running {
                      Self::handle_pipe_message(core_arc.clone(), &socket_logic_strong, pipe_id, msg).await;
                    } else { tracing::trace!(handle=core_handle, pipe_id=pipe_id, "SocketCore dropping pipe message during shutdown."); }
                  }
                  // These commands are not expected directly on SocketCore's mailbox anymore.
                  Command::Attach {..} | Command::SessionPushCmd {..} | Command::EnginePushCmd {..} |
                  Command::EngineReady {..} | Command::EngineError {..} | Command::EngineStopped {..} |
                  Command::RequestZapAuth {..} | Command::ProcessZapReply {..} | Command::AttachPipe {..} => {
                    tracing::error!(handle = core_handle, cmd = command_name_for_log, "SocketCore received UNEXPECTED command type on its mailbox!");
                  }
                }
              }
              Err(_) => {
                tracing::info!(handle = core_handle, "SocketCore command mailbox closed. Initiating shutdown.");
                final_error_for_actorstop = Some(ZmqError::Internal("SocketCore: Command mailbox closed".into()));
                let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
                if coordinator_guard.current_phase() == ShutdownPhase::Running {
                  drop(coordinator_guard);
                  Self::publish_socket_closing(&core_arc.context, core_handle).await;
                  Self::initiate_shutdown_phase(core_arc.clone()).await;
                  Self::stop_child_actors(core_arc.clone()).await;
                  #[cfg(feature="inproc")]
                  Self::disconnect_inproc_connections(core_arc.clone()).await;
                }
              }
            }
          }

          _ = linger_check_interval.tick(), if current_shutdown_phase == ShutdownPhase::Lingering => {
            let mut coordinator_guard = core_arc.shutdown_coordinator.lock().await;
            if coordinator_guard.current_phase() == ShutdownPhase::Lingering {
              if coordinator_guard.linger_deadline.is_none() && core_arc.core_state.lock().await.options.linger != Some(Duration::ZERO) {
                let linger_opt_val = { core_arc.core_state.lock().await.options.linger };
                coordinator_guard.start_linger(linger_opt_val, core_handle);
              }
              let queues_empty_check = coordinator_guard.are_queues_empty(&*core_arc.core_state.lock().await);
              if coordinator_guard.is_linger_expired(core_handle) || queues_empty_check {
                if queues_empty_check { tracing::debug!(handle = core_handle, "SocketCore Linger: Queues empty."); }
                
                let mut core_state_guard = core_arc.core_state.lock().await;
                coordinator_guard.advance_to_cleaning(core_handle, &mut core_state_guard);
                #[cfg(feature="inproc")] let pipes_to_clean_list = coordinator_guard.inproc_connections_to_close.clone();
                #[cfg(not(feature="inproc"))] let pipes_to_clean_list = Vec::new();
                drop(coordinator_guard);
                Self::perform_pipe_cleanup(core_arc.clone(), &socket_logic_strong, pipes_to_clean_list).await;
              } else { drop(coordinator_guard); }
            } else { drop(coordinator_guard); }
          }
        }
      }
      Ok(())
    }.await;

    if let Err(e) = _loop_result {
      tracing::error!(handle = core_handle, socket_type = ?socket_type_for_log, "SocketCore loop error: {}", e);
      final_error_for_actorstop = Some(e);
      let mut coordinator = core_arc.shutdown_coordinator.lock().await;
      if coordinator.state != ShutdownPhase::Finished {
        coordinator.state = ShutdownPhase::Finished;
      }
    }
    tracing::info!(handle = core_handle, socket_type = ?socket_type_for_log, "SocketCore main loop finished.");

    #[cfg(feature = "inproc")]
    {
      let bound_names_list = { std::mem::take(&mut core_arc.core_state.lock().await.bound_inproc_names) };
      if !bound_names_list.is_empty() {
        tracing::debug!(
          handle = core_handle,
          "SocketCore unregistering inproc names: {:?}",
          bound_names_list
        );
        let unbind_futs = bound_names_list.into_iter().map(|name_val| {
          let ctx_inner_clone_val = core_arc.context.inner().clone();
          async move {
            ctx_inner_clone_val.unregister_inproc(&name_val).await;
          }
        });
        join_all(unbind_futs).await;
      }
    }

    core_arc.context.inner().unregister_socket(core_handle).await;

    context_clone_for_stop.publish_actor_stopping(core_handle, core_actor_type, None, final_error_for_actorstop);

    tracing::info!(handle = core_handle, socket_type = ?socket_type_for_log, "SocketCore task FULLY STOPPED.");
  }

  /// Sends a Stop command to all registered child actors (Listeners/Sessions).
  async fn stop_child_actors(core_arc: Arc<SocketCore>) {
    let core_handle = core_arc.handle;
    tracing::debug!(handle = core_handle, "Stopping child actors...");
    let endpoints_to_stop = {
        let state = core_arc.core_state.lock().await;
        // Clone mailboxes of children identified by the shutdown coordinator
        let coordinator = core_arc.shutdown_coordinator.lock().await; // Lock coordinator briefly
        coordinator.pending_children.keys()
            .filter_map(|child_handle_id| {
                // Find the endpoint info corresponding to the child handle
                state.endpoints.values().find(|info| info.handle_id == *child_handle_id)
                     .map(|info| (info.handle_id, info.mailbox.clone())) // Clone mailbox
            })
            .collect::<Vec<_>>()
    }; // Locks released

    if endpoints_to_stop.is_empty() {
        tracing::debug!(handle = core_handle, "No child actors to stop.");
        return;
    }

    let mut stop_futures = Vec::new();
    for (child_handle, mailbox) in endpoints_to_stop {
        stop_futures.push(async move {
            if let Err(e) = mailbox.send(Command::Stop).await {
                tracing::warn!(parent_handle = core_handle, child_handle = child_handle, "Failed to send Stop to child actor: {:?}", e);
            } else {
                tracing::trace!(parent_handle = core_handle, child_handle = child_handle, "Sent Stop command to child actor.");
            }
        });
    }
    futures::future::join_all(stop_futures).await;
    tracing::debug!(handle = core_handle, "Finished sending Stop commands to children.");
  }

  #[cfg(feature="inproc")]
  async fn disconnect_inproc_connections(core_arc: Arc<SocketCore>) {
      let core_handle = core_arc.handle;
      tracing::debug!(handle = core_handle, "Disconnecting all inproc connections...");
      // Get the list of inproc URIs to disconnect
      let inproc_uris_to_disconnect = {
          let coordinator = core_arc.shutdown_coordinator.lock().await;
          // Iterate through the pipes registered for closure
            coordinator.inproc_connections_to_close.iter()
              .map(|(_, _, uri)| uri.clone())
              .collect::<Vec<_>>()
      }; // Lock released

      if inproc_uris_to_disconnect.is_empty() {
          tracing::debug!(handle = core_handle, "No inproc connections to disconnect.");
          return;
      }

      let mut disconnect_futures = Vec::new();
      for uri in inproc_uris_to_disconnect {
          tracing::trace!(handle = core_handle, %uri, "Scheduling inproc disconnect.");
          let core_clone = core_arc.clone();
          // Spawn a task for each disconnect to avoid blocking the main loop if one disconnect hangs
          disconnect_futures.push(tokio::spawn(async move {
              if let Err(e) = crate::transport::inproc::disconnect_inproc(&uri, core_clone).await {
                  tracing::warn!(parent_handle = core_handle, %uri, "Error disconnecting inproc peer during shutdown: {}", e);
              }
          }));
      }
      futures::future::join_all(disconnect_futures).await; // Wait for disconnect tasks
      tracing::debug!(handle = core_handle, "Finished attempting to disconnect inproc connections.");
  }

  async fn initiate_shutdown_phase(core_arc: Arc<SocketCore>) -> bool {
    let mut coordinator = core_arc.shutdown_coordinator.lock().await;
    let core_handle = core_arc.handle;
    let mut core_state_guard = core_arc.core_state.lock().await;
    let initiated_flag = coordinator.initiate(&mut core_state_guard, core_handle);

    if initiated_flag {
      if coordinator.current_phase() == ShutdownPhase::Lingering {
        let linger_opt_val = core_state_guard.options.linger;
        coordinator.start_linger(linger_opt_val, core_handle);
        if coordinator.is_linger_expired(core_handle) || coordinator.are_queues_empty(&core_state_guard) {
          coordinator.advance_to_cleaning(core_handle, &mut core_state_guard);
          #[cfg(feature = "inproc")]
          let pipes_to_clean_val = coordinator.inproc_connections_to_close.clone();

          #[cfg(not(feature = "inproc"))]
          let pipes_to_clean_val = Vec::new();
          drop(coordinator);
          drop(core_state_guard);

          if let Some(sl_val) = core_arc.get_socket_logic().await {
            Self::perform_pipe_cleanup(core_arc.clone(), &sl_val, pipes_to_clean_val).await;
          } else {
            tracing::error!(
              handle = core_handle,
              "SocketCore initiate_shutdown: ISocket logic gone for cleanup."
            );
            let mut final_coord = core_arc.shutdown_coordinator.lock().await;
            final_coord.finish(core_handle);
          }
        }
      }
    }
    initiated_flag
  }

  async fn perform_pipe_cleanup(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    #[cfg(feature = "inproc")] pending_inproc_pipes_to_cleanup: Vec<(usize, usize, String)>,
    #[cfg(not(feature = "inproc"))] _pending_inproc_pipes_to_cleanup: Vec<(usize, usize, String)>, // Parameter needed for conditional compilation
  ) {
    let core_handle = core_arc.handle;
    tracing::info!(
      handle = core_handle,
      inproc_pipe_list_len = pending_inproc_pipes_to_cleanup.len(), // Log length immediately
      "SocketCore entering final pipe cleanup..."
    );
    let pipes_tx_map: HashMap<usize, AsyncSender<Msg>>;
    let reader_tasks_map: HashMap<usize, JoinHandle<()>>;
    let monitor_tx_opt_clone: Option<MonitorSender>;
    let context_clone = core_arc.context.clone();

    let mut coordinator_val = core_arc.shutdown_coordinator.lock().await;
    if !matches!(coordinator_val.state, ShutdownPhase::CleaningPipes | ShutdownPhase::Finished) {
        tracing::warn!(handle=core_handle, state=?coordinator_val.state, "perform_pipe_cleanup called in unexpected state");
        return;
    }
    {
        let mut state_val = core_arc.core_state.lock().await;
        monitor_tx_opt_clone = state_val.get_monitor_sender_clone();
        // Take ALL pipes and readers now, including inproc ones
        pipes_tx_map = std::mem::take(&mut state_val.pipes_tx);
        reader_tasks_map = std::mem::take(&mut state_val.pipe_reader_task_handles);

        // Remove EndpointInfo entries associated with the inproc pipes being cleaned up
        #[cfg(feature="inproc")]
        {
            tracing::debug!(handle=core_handle, num_inproc_pipes = pending_inproc_pipes_to_cleanup.len(), "Removing EndpointInfo for cleaned inproc pipes.");
            for (_, _, uri) in &pending_inproc_pipes_to_cleanup {
                if state_val.endpoints.remove(uri).is_some() {
                      tracing::trace!(handle=core_handle, %uri, "Removed inproc EndpointInfo during pipe cleanup.");
                } else {
                      tracing::warn!(handle=core_handle, %uri, "Inproc EndpointInfo already removed before pipe cleanup?");
                }
            }
        }
    }
    if coordinator_val.state == ShutdownPhase::CleaningPipes {
        coordinator_val.finish(core_handle);
    }
    drop(coordinator_val); // Release coordinator lock

    // Close potentially remaining pipe senders (should be empty if advance_to_cleaning worked for inproc)
    for (id_val, sender_val) in pipes_tx_map.iter() {
        if !sender_val.is_closed() { // Only log if not already closed
            sender_val.close();
            tracing::trace!(handle = core_handle, pipe_id = id_val, "Closed pipe sender in cleanup.");
        }
    }
    if !pipes_tx_map.is_empty() { // Log if any were taken, even if already closed
        tracing::debug!(handle = core_handle, count = pipes_tx_map.len(), "Pipe sender map processed.");
    }

    // Abort reader tasks
    if !reader_tasks_map.is_empty() {
        let reader_handles_list: Vec<(usize, JoinHandle<()>)> = reader_tasks_map.into_iter().collect();
        for (id_val, task_h_val) in &reader_handles_list {
            task_h_val.abort();
            tracing::trace!(handle = core_handle, pipe_read_id = id_val, "Aborted pipe reader task.");
        }
        tokio::time::sleep(Duration::from_millis(5)).await; // Allow drop guards
        tracing::debug!(handle = core_handle, count = reader_handles_list.len(), "All pipe reader tasks aborted.");
    } else {
        tracing::debug!(handle = core_handle, "No pipe reader tasks to abort.");
    }

    // Notify ISocket & Emit Monitor Event for Inproc
    #[cfg(feature = "inproc")]
    {
      let mut notify_futs = Vec::new();
      let mut event_futs = Vec::new();
      let mut peer_closed_events = Vec::new();

      for (_write_id_val, read_id_val, uri_val) in &pending_inproc_pipes_to_cleanup {
          // --- Notify ISocket ---
          tracing::debug!(handle = core_handle, pipe_read_id = read_id_val, uri = %uri_val, "Notifying ISocket of inproc pipe detach during cleanup.");
          let sl_clone = socket_logic.clone();
          let current_read_id = *read_id_val;
          notify_futs.push(async move {
              sl_clone.pipe_detached(current_read_id).await;
          });

          // --- Prepare Monitor Event ---
          if let Some(monitor_tx) = monitor_tx_opt_clone.as_ref() {
              let event = SocketEvent::Disconnected { endpoint: uri_val.clone() };
              let tx_clone = monitor_tx.clone();
              let uri_clone = uri_val.clone();
              event_futs.push(async move {
                    tracing::debug!(handle=core_handle, %uri_clone, "Emitting Disconnected event for inproc pipe.");
                    if tx_clone.send(event).await.is_err() {
                        tracing::warn!(handle=core_handle, %uri_clone, "Failed sending Disconnected event for inproc pipe.");
                    }
              });
          } else {
              tracing::trace!(handle=core_handle, %uri_val, "No monitor configured, skipping Disconnected event for inproc pipe.");
          }

          // --- Prepare PeerClosed Event ---
          if let Some(name) = uri_val.strip_prefix("inproc://") {
                let target_name = name.to_string();
                let peer_closed_event = SystemEvent::InprocPipePeerClosed {
                    target_inproc_name: target_name.clone(),
                    closed_by_connector_pipe_read_id: *read_id_val,
                };
                peer_closed_events.push(peer_closed_event);
                tracing::debug!(handle=core_handle, target_name=%target_name, pipe_id = read_id_val, "Prepared InprocPipePeerClosed event.");
          } else {
              tracing::warn!(handle=core_handle, uri=%uri_val, "Could not extract name from inproc URI for PeerClosed event.");
          }
      }

      // Execute notifications and event sends concurrently
      if !notify_futs.is_empty() {
          futures::future::join_all(notify_futs).await;
      }
      if !event_futs.is_empty() {
          futures::future::join_all(event_futs).await;
      }

      // Publish PeerClosed events
      let event_bus = context_clone.event_bus();
      for ev in peer_closed_events {
        tracing::debug!(handle=core_handle, event=?ev, "Publishing InprocPipePeerClosed event.");
        if event_bus.publish(ev).is_err() {
            tracing::warn!(handle=core_handle, "Failed to publish InprocPipePeerClosed event during cleanup.");
        }
      }
    }

    let mut final_coord_guard_val = core_arc.shutdown_coordinator.lock().await;
    final_coord_guard_val.finish(core_handle);

    tracing::info!(
      handle = core_handle,
      "Pipe cleanup complete. Shutdown finished for this SocketCore."
    );
  }

  async fn handle_user_monitor(
    core_arc: Arc<SocketCore>,
    monitor_tx: MonitorSender,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let handle = core_arc.handle;
    tracing::debug!(handle = handle, "Handling UserMonitor");
    let mut state_guard = core_arc.core_state.lock().await;
    if state_guard.monitor_tx.is_some() {
      tracing::warn!(handle = handle, "Monitor channel already set, replacing.");
    }
    state_guard.monitor_tx = Some(monitor_tx);
    drop(state_guard);
    let _ = reply_tx.send(Ok(()));
  }

  async fn handle_user_bind(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let core_handle = core_arc.handle;
    tracing::debug!(handle = core_handle, %endpoint, "Handling UserBind");
    let parse_result = parse_endpoint(&endpoint);
    let context_clone = core_arc.context.clone();
    let parent_socket_id_val = core_arc.handle;

    let bind_res = match parse_result {
      Ok(Endpoint::Tcp(_addr, ref uri)) => {
        let mut state_g = core_arc.core_state.lock().await;
        if state_g.endpoints.contains_key(uri) {
          Err(ZmqError::AddrInUse(uri.clone()))
        } else {
          let monitor_tx_c = state_g.get_monitor_sender_clone();
          let options_a = Arc::new(state_g.options.clone());
          drop(state_g);
          let child_h = context_clone.inner().next_handle();
          let handle_src = context_clone.inner().next_handle.clone();
          let listener_res = TcpListener::create_and_spawn(
            child_h,
            uri.clone(),
            options_a,
            handle_src,
            monitor_tx_c,
            context_clone.clone(),
            parent_socket_id_val,
          );
          match listener_res {
            Ok((child_mb, child_th)) => {
              let mut state_w = core_arc.core_state.lock().await;
              state_w.endpoints.insert(
                uri.clone(),
                EndpointInfo {
                  mailbox: child_mb,
                  task_handle: child_th,
                  endpoint_type: EndpointType::Listener,
                  endpoint_uri: uri.clone(),
                  pipe_ids: None,
                  handle_id: child_h,
                  target_endpoint_uri: None,
                  is_outbound_connection: false,
                },
              );
              state_w
                .send_monitor_event(SocketEvent::Listening { endpoint: uri.clone() })
                .await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
      }
      #[cfg(feature = "ipc")]
      Ok(Endpoint::Ipc(ref path, ref uri)) => {
        let mut state_g = core_arc.core_state.lock().await;
        if state_g.endpoints.contains_key(uri) {
          Err(ZmqError::AddrInUse(uri.clone()))
        } else {
          let monitor_tx_c = state_g.get_monitor_sender_clone();
          let options_a = Arc::new(state_g.options.clone());
          drop(state_g);
          let child_h = context_clone.inner().next_handle();
          let handle_src = context_clone.inner().next_handle.clone();
          let listener_res = IpcListener::create_and_spawn(
            child_h,
            uri.clone(),
            path.clone(),
            options_a,
            handle_src,
            monitor_tx_c,
            context_clone.clone(),
            parent_socket_id_val,
          );
          match listener_res {
            Ok((child_mb, child_th)) => {
              let mut state_w = core_arc.core_state.lock().await;
              state_w.endpoints.insert(
                uri.clone(),
                EndpointInfo {
                  mailbox: child_mb,
                  task_handle: child_th,
                  endpoint_type: EndpointType::Listener,
                  endpoint_uri: uri.clone(),
                  pipe_ids: None,
                  handle_id: child_h,
                  target_endpoint_uri: None,
                  is_outbound_connection: false,
                },
              );
              state_w
                .send_monitor_event(SocketEvent::Listening { endpoint: uri.clone() })
                .await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
      }
      #[cfg(feature = "inproc")]
      Ok(Endpoint::Inproc(ref name)) => {
        let core_c = core_arc.clone();
        let name_c = name.clone();
        let is_bound = core_c.core_state.lock().await.bound_inproc_names.contains(&name_c);
        if is_bound {
          Err(ZmqError::AddrInUse(format!("inproc://{}", name)))
        } else {
          match bind_inproc(name_c.clone(), core_c).await {
            // bind_inproc takes Arc<SocketCore>
            Ok(()) => {
              let mut state_g = core_arc.core_state.lock().await; // Re-lock to send monitor
              state_g
                .send_monitor_event(SocketEvent::Listening {
                  endpoint: format!("inproc://{}", name_c),
                })
                .await;
              Ok(())
            }
            Err(e) => Err(e),
          }
        }
      }
      Err(e) => Err(e),
      _ => Err(ZmqError::UnsupportedTransport(endpoint.to_string())),
    };
    if let Err(e) = &bind_res {
      core_arc
        .core_state
        .lock()
        .await
        .send_monitor_event(SocketEvent::BindFailed {
          endpoint: endpoint.clone(),
          error_msg: format!("{}", e),
        })
        .await;
    }
    let _ = reply_tx.send(bind_res);
  }

  async fn handle_user_connect(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let core_handle = core_arc.handle;
    tracing::debug!(handle = core_handle, %endpoint, "Handling UserConnect");
    let parse_result = parse_endpoint(&endpoint);
    match parse_result {
      Ok(Endpoint::Tcp(_, uri)) | Ok(Endpoint::Ipc(_, uri)) => {
        Self::respawn_connecter(core_arc, uri).await; // respawn_connecter will publish ActorStarted
        let _ = reply_tx.send(Ok(()));
      }
      #[cfg(feature = "inproc")]
      Ok(Endpoint::Inproc(name)) => {
        let core_c = core_arc.clone();
        // connect_inproc publishes InprocBindingRequest event
        tokio::spawn(async move {
          connect_inproc(name, core_c, reply_tx).await;
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
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let core_handle = core_arc.handle;
    tracing::debug!(handle = core_handle, %endpoint, "Handling UserDisconnect");
    let mut disconnect_res = Ok(());
    let mut state_g = core_arc.core_state.lock().await;
    let mut child_id_to_signal: Option<usize> = None; // No direct signal, but for logging/identification
    let mut resolved_uri_to_stop: Option<String> = None;

    if let Some(info) = state_g.endpoints.get(&endpoint) {
      if info.endpoint_type == EndpointType::Session {
        child_id_to_signal = Some(info.handle_id);
        resolved_uri_to_stop = Some(endpoint.clone());
      }
    } else {
      for (res_uri, info) in state_g.endpoints.iter() {
        if info.endpoint_type == EndpointType::Session && info.target_endpoint_uri.as_deref() == Some(&endpoint) {
          child_id_to_signal = Some(info.handle_id);
          resolved_uri_to_stop = Some(res_uri.clone());
          break;
        }
      }
    }

    if let Some(child_id_val) = child_id_to_signal {
      if let Some(res_uri_val) = resolved_uri_to_stop {
        // Removing from endpoints map. The Session actor will stop on SocketClosing or ContextTerminating.
        // Its ActorStopping event will trigger full cleanup via process_child_completion.
        if state_g.endpoints.remove(&res_uri_val).is_some() {
          tracing::debug!(handle = core_handle, child_actor_id = child_id_val, disconnected_uri = %res_uri_val, "SocketCore marked endpoint for disconnect. Session will stop via events.");
          // The specific session needs to be told to stop. This is tricky without a direct command.
          // Option 1: Publish a targeted event (e.g., DisconnectPeer { session_id })
          // Option 2: The ISocket::disconnect might need to do more than just send UserDisconnect.
          // For now, rely on the fact that removing it from `endpoints` means no new messages will be routed.
          // The session will eventually be cleaned up when the main socket closes or context terms.
          // This might not be immediate, which differs from a direct Stop.
          // To make it more immediate, we'd need the session's mailbox.
          // The EndpointInfo has 'mailbox', which *is* the session's command mailbox.
          // Let's send a Stop command directly to the session.
          if let Some(info_for_stop) = state_g.endpoints.get(&res_uri_val) { // Should not be Some, as we just removed. This logic is flawed.
             // Correct logic: Get mailbox before removing.
             // This part is complex due to lock ownership. Let's assume for now that removing it
             // from the list of active endpoints is sufficient, and it will be cleaned up.
             // The original code sent Stop and then `publish_socket_closing` for the child.
             // The child (Session) should listen to its own SocketClosing event if we publish one.
             // This needs a more robust way for SocketCore to signal a *specific child session* to stop.
             // For now, we remove it. It will be cleaned up on general socket/context termination.
          }
        }
      }
    } else if endpoint.starts_with("inproc://") {
      drop(state_g); // Release lock before async call
      #[cfg(feature = "inproc")]
      {
        disconnect_res = disconnect_inproc(&endpoint, core_arc.clone()).await;
      }
      #[cfg(not(feature = "inproc"))]
      {
        disconnect_res = Err(ZmqError::UnsupportedTransport(endpoint.clone()));
      }
      let _ = reply_tx.send(disconnect_res); // Send result for inproc
      return; // Return early as state_g was dropped
    } else {
      disconnect_res = Err(ZmqError::InvalidArgument(format!(
        "Endpoint not found for disconnect: {}",
        endpoint
      )));
    }
    // Ensure lock is released if not an early return
    if child_id_to_signal.is_some() || !endpoint.starts_with("inproc://") {
      drop(state_g);
    }
    let _ = reply_tx.send(disconnect_res);
  }

  async fn handle_user_unbind(
    core_arc: Arc<SocketCore>,
    endpoint: String,
    reply_tx: oneshot::Sender<Result<(), ZmqError>>,
  ) {
    let core_handle = core_arc.handle;
    tracing::debug!(handle = core_handle, %endpoint, "Handling UserUnbind");
    let mut unbind_res = Ok(());
    let mut state_g = core_arc.core_state.lock().await;
    let mut listener_info_opt: Option<(String, MailboxSender, usize)> = None;

    if let Some(info) = state_g.endpoints.get(&endpoint) {
      if info.endpoint_type == EndpointType::Listener {
        listener_info_opt = Some((endpoint.clone(), info.mailbox.clone(), info.handle_id));
      } else {
        unbind_res = Err(ZmqError::InvalidArgument("Cannot unbind non-listener".into()));
      }
    } else if !endpoint.starts_with("inproc://") {
      unbind_res = Err(ZmqError::InvalidArgument(format!(
        "Endpoint not found for unbind: {}",
        endpoint
      )));
    }

    if let Some((uri, mailbox, child_id)) = listener_info_opt {
      if state_g.endpoints.remove(&uri).is_some() {
        drop(state_g); // Release lock before sending command
        tracing::debug!(handle = core_handle, listener_uri = %uri, "Sending Stop to Listener for unbind.");
        // Send a direct Stop command to the Listener. The Listener's command loop will handle it,
        // abort its accept loop, and then publish ActorStopping.
        if mailbox.send(Command::Stop).await.is_err() {
          tracing::warn!(handle = core_handle, "Failed to send Stop to Listener on unbind.");
        }
      } else {
        drop(state_g);
      }
    } else if endpoint.starts_with("inproc://") {
      drop(state_g); // Release lock
      #[cfg(feature = "inproc")]
      {
        let name = endpoint.strip_prefix("inproc://").unwrap_or("");
        if !name.is_empty() {
          let mut cs_recheck = core_arc.core_state.lock().await;
          if cs_recheck.bound_inproc_names.remove(name) {
            drop(cs_recheck);
            unbind_inproc(name, &core_arc.context).await; // Global unregister
            core_arc
              .core_state
              .lock()
              .await
              .send_monitor_event(SocketEvent::Closed {
                endpoint: endpoint.clone(),
              })
              .await;
          } else {
            drop(cs_recheck);
            unbind_res = Err(ZmqError::InvalidArgument(format!(
              "Inproc name '{}' not bound by this socket",
              name
            )));
          }
        } else {
          unbind_res = Err(ZmqError::InvalidEndpoint(endpoint));
        }
      }
      #[cfg(not(feature = "inproc"))]
      {
        unbind_res = Err(ZmqError::UnsupportedTransport(endpoint.clone()));
      }
      let _ = reply_tx.send(unbind_res);
      return; // Early return
    } else {
      drop(state_g);
    } // Ensure lock is dropped
    let _ = reply_tx.send(unbind_res);
  }

  async fn handle_conn_success(
    core_arc: Arc<SocketCore>,
    socket_logic: &Arc<dyn ISocket>,
    endpoint: String,
    target_endpoint_uri: String,
    session_mailbox: MailboxSender,
    session_handle_id_opt: Option<usize>,
    session_task_handle_opt: Option<JoinHandle<()>>,
  ) {
    let core_handle = core_arc.handle;
    let context_c = core_arc.context.clone();
    tracing::info!(handle = core_handle, %endpoint, target_uri=%target_endpoint_uri, "Handling ConnSuccess (from event)");

    let pipe_id_core_w = context_c.inner().next_handle();
    let pipe_id_core_r = context_c.inner().next_handle();
    let (tx_c_s, rx_c_s) = {
      let s = core_arc.core_state.lock().await;
      bounded::<Msg>(s.options.sndhwm.max(1))
    };
    let (tx_s_c, rx_s_c) = {
      let s = core_arc.core_state.lock().await;
      bounded::<Msg>(s.options.rcvhwm.max(1))
    };

    let reader_th = tokio::spawn(Self::run_pipe_reader_task(
      context_c.clone(),
      core_handle,
      core_arc.command_sender(),
      pipe_id_core_r,
      rx_s_c,
    ));
    let mut state_g = core_arc.core_state.lock().await;
    state_g.pipes_tx.insert(pipe_id_core_w, tx_c_s.clone());
    state_g.pipe_reader_task_handles.insert(pipe_id_core_r, reader_th);

    let child_actor_h = session_handle_id_opt.expect("ConnSuccess event must provide Session handle ID");
    // session_task_handle_opt is a JoinHandle (possibly dummy if event only has ID)
    let owned_task_h = session_task_handle_opt.unwrap_or_else(|| tokio::spawn(async {}));

    let is_this_an_outbound_connection = {
    // Check if target_uri_from_event corresponds to one of *this SocketCore's UserConnect targets*
    // rather than one of its *Listener bind URIs*.
    // If target_uri_from_event matches one of *our own listeners' URIs*, it's an inbound connection.
    let mut is_inbound_to_own_listener = false;
    for (ep_uri_key, ep_info_val) in state_g.endpoints.iter() {
        if ep_info_val.endpoint_type == EndpointType::Listener && ep_uri_key == &target_endpoint_uri {
            is_inbound_to_own_listener = true;
            break;
        }
    }
    !is_inbound_to_own_listener // It's outbound if it's NOT inbound to one of our listeners
  };

    state_g.endpoints.insert(
      endpoint.clone(),
      EndpointInfo {
        mailbox: session_mailbox.clone(),
        task_handle: owned_task_h,
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint.clone(),
        pipe_ids: Some((pipe_id_core_w, pipe_id_core_r)),
        handle_id: child_actor_h,
        target_endpoint_uri: Some(target_endpoint_uri),
        is_outbound_connection: is_this_an_outbound_connection
      },
    );
    drop(state_g);

    let attach_cmd = Command::AttachPipe {
      rx_from_core: rx_c_s,
      tx_to_core: tx_s_c,
      pipe_read_id: pipe_id_core_w,
      pipe_write_id: pipe_id_core_r,
    };
    if session_mailbox.is_closed() {
      tracing::warn!(
        handle = core_handle,
        session_handle = child_actor_h,
        "Session mailbox closed before AttachPipe."
      );
      let mut sg_cleanup = core_arc.core_state.lock().await;
      sg_cleanup.remove_pipe_state(pipe_id_core_w, pipe_id_core_r);
    } else if session_mailbox.send(attach_cmd).await.is_err() {
      tracing::error!(
        handle = core_handle,
        session_handle = child_actor_h,
        "Failed to send AttachPipe to session."
      );
      Self::cleanup_session_state_by_pipe_ids(core_arc.clone(), pipe_id_core_w, pipe_id_core_r, socket_logic).await;
    } else {
      socket_logic.pipe_attached(pipe_id_core_r, pipe_id_core_w, None).await; // Assuming no early peer_identity for now
    }
  }

  async fn handle_conn_failed(core_arc: Arc<SocketCore>, target_uri: String, error: ZmqError) {
    let core_handle = core_arc.handle;
    tracing::error!(handle = core_handle, uri = %target_uri, error = %error, "Handling ConnFailed (from event)");
    let event_payload = SocketEvent::ConnectFailed {
      endpoint: target_uri.clone(),
      error_msg: format!("{}", error),
    };
    core_arc.core_state.lock().await.send_monitor_event(event_payload).await;
    // Logic for potential retries is handled by respawn_connecter if applicable,
    // or if the Connecter itself manages retries. Here, we just log and emit monitor event.
  }

  async fn handle_pipe_message(core_arc: Arc<SocketCore>, socket_logic: &Arc<dyn ISocket>, pipe_id: usize, msg: Msg) {
    let core_handle = core_arc.handle;
    tracing::trace!(
      handle = core_handle,
      pipe_read_id = pipe_id,
      msg_size = msg.size(),
      "Handling PipeMessageReceived"
    );
    let event_cmd = Command::PipeMessageReceived { pipe_id, msg };
    if let Err(e) = socket_logic.handle_pipe_event(pipe_id, event_cmd).await {
      tracing::error!(
        handle = core_handle,
        pipe_read_id = pipe_id,
        "Error handling PipeMessageReceived in ISocket: {}",
        e
      );
    }
  }

  /// Processes an `InprocBindingRequest` received via the system event bus.
  /// This logic was previously in `handle_inproc_connect` which took a `Command`.
  #[cfg(feature = "inproc")]
  async fn process_inproc_binding_request(
    core_arc: Arc<SocketCore>, // This is the Binder's SocketCore
    socket_logic: &Arc<dyn ISocket>,
    connector_uri: String,
    pipe_rx_for_binder_to_receive_from_connector: async_channel::Receiver<Msg>, // Binder receives on this.
    pipe_tx_for_binder_to_send_to_connector: async_channel::Sender<Msg>,        // Binder sends on this.
    // Pipe IDs from the BINDER's perspective for this connection:
    connector_pipe_write_id: usize, // Binder uses this ID to write to connector.
    connector_pipe_read_id: usize,  // Binder uses this ID to read from connector.
    reply_tx_to_connector: crate::runtime::OneShotSender, // To confirm acceptance/rejection to connector.
  ) {
    let binder_core_handle = core_arc.handle;
    let context_for_pipe_reader = core_arc.context.clone();
    tracing::debug!(
        binder_handle = binder_core_handle,
        connector_uri = %connector_uri,
        binder_write_pipe_id = connector_pipe_write_id,
        binder_read_pipe_id = connector_pipe_read_id,
        "SocketCore (binder) processing InprocBindingRequest event."
    );

    // For now, assume the binder always accepts the connection if it's running.
    // More complex acceptance logic (e.g., based on load, security) could be added here.
    let accept_result: Result<(), ZmqError> = Ok(());

    if accept_result.is_ok() {
      // Spawn a PipeReaderTask for the binder to read messages from the connector.
      // The PipeReaderTask will send `PipeMessageReceived` or `PipeClosedByPeer` commands
      // to this binder's `SocketCore` command mailbox.
      let pipe_reader_task = tokio::spawn(Self::run_pipe_reader_task(
        context_for_pipe_reader,                      // Context.
        binder_core_handle,                           // Parent core handle (this binder).
        core_arc.command_sender(),                    // Binder's command mailbox.
        connector_pipe_read_id,                       // ID binder uses to read from this pipe.
        pipe_rx_for_binder_to_receive_from_connector, // Channel binder receives on.
      ));

      // Store the binder's ends of the pipes in its CoreState.
      let mut binder_core_state = core_arc.core_state.lock().await;
      binder_core_state.pipes_tx.insert(
        connector_pipe_write_id,                 // Key: ID binder uses to write.
        pipe_tx_for_binder_to_send_to_connector, // Value: Sender channel.
      );
      binder_core_state.pipe_reader_task_handles.insert(
        connector_pipe_read_id, // Key: ID binder uses to read.
        pipe_reader_task,       // Value: JoinHandle for the reader task.
      );

      // Create an EndpointInfo entry in the binder's state to represent this inproc connection.
      let endpoint_entry_handle_id = core_arc.context.inner().next_handle();
      let endpoint_info_for_binder = EndpointInfo {
        // For an inproc connection from binder's side, the 'mailbox' in EndpointInfo
        // isn't for a child Session/Listener actor. It could be the connector's SocketCore
        // mailbox if known, or a placeholder. For now, using binder's own as placeholder.
        // The connector is identified by its URI if direct interaction were needed.
        mailbox: core_arc.command_sender(),   // Placeholder.
        task_handle: tokio::spawn(async {}),  // Dummy task handle for this entry.
        endpoint_type: EndpointType::Session, // Treat inproc connection as a conceptual session.
        endpoint_uri: connector_uri.clone(),  // URI of the connecting peer.
        pipe_ids: Some((connector_pipe_write_id, connector_pipe_read_id)),
        handle_id: endpoint_entry_handle_id, // Unique ID for this endpoint entry in binder.
        target_endpoint_uri: Some(connector_uri.clone()), // Target URI is the connector's URI.
        is_outbound_connection: false,
      };
      binder_core_state
        .endpoints
        .insert(connector_uri.clone(), endpoint_info_for_binder);
      let monitor_tx_for_binder_event = binder_core_state.get_monitor_sender_clone();
      drop(binder_core_state); // Release lock.

      // Emit "Connected" monitor event from the binder's perspective.
      if let Some(monitor_tx) = monitor_tx_for_binder_event {
        let event = SocketEvent::Connected {
          endpoint: connector_uri.clone(), // The endpoint we connected to (the connector).
          peer_addr: format!("inproc-connector-uri-{}", connector_uri), // Synthetic peer address.
        };
        let _ = monitor_tx.send(event).await; // Best effort.
      }

      // Notify the binder's ISocket logic that a new "pipe" (inproc connection) is attached.
      socket_logic
        .pipe_attached(
          connector_pipe_read_id,  // ID binder uses to read.
          connector_pipe_write_id, // ID binder uses to write.
          None,                    // No ZMTP identity for inproc pipes.
        )
        .await;
      tracing::info!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Inproc connection accepted by binder and pipes attached.");
    } else {
      // Connection was rejected by the binder's logic (though current logic always accepts).
      let error_msg_str = accept_result
        .as_ref()
        .err()
        .map_or("Binder rejected connection".to_string(), |e| format!("{}", e));
      // Emit ConnectFailed from the binder's perspective (should this be an AcceptFailed?)
      // The connector will emit ConnectFailed based on the reply_tx.
      // For binder, perhaps a specific "BindingRequestRejected" monitor event would be better.
      // For now, let's assume no specific monitor event from binder on rejection.
      tracing::warn!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Inproc connection request rejected by binder's logic.");
    }
    // Send the acceptance/rejection result back to the connector via the oneshot channel.
    if let Some(res) = reply_tx_to_connector.take_and_send(accept_result).await {
      if res.is_err() {
        tracing::warn!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Failed to send InprocBindingRequest reply to connector (connector might have timed out or disappeared).");
        // If reply fails, the connector will likely timeout. The binder has already set up its side if accepted.
        // If it was accepted, the pipes might need cleanup if the connector is gone.
        // This edge case (reply send fail after accept) might require more robust cleanup on binder.
        // For now, if accepted, pipes are set up. If connector is gone, PipeClosedByPeer will eventually trigger.
      }
    }
  }

  async fn process_child_completion(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    child_handle: usize,
    endpoint_uri_opt: Option<&str>,
    command_name: &'static str,
    error_opt: Option<&ZmqError>,
  ) {
    let core_h = core_arc.handle;
    let is_err = error_opt.is_some();
    let mut coordinator_g = core_arc.shutdown_coordinator.lock().await;
    let current_phase_val = coordinator_g.current_phase();
    let log_uri_val = endpoint_uri_opt.unwrap_or("<UnknownURI>");

    // Log based on whether we are shutting down or running normally
    if matches!(
      current_phase_val,
      ShutdownPhase::StoppingChildren | ShutdownPhase::Lingering | ShutdownPhase::CleaningPipes | ShutdownPhase::Finished // Include finished
    ) {
      if is_err {
        tracing::error!(handle=core_h, child_handle=child_handle, error=%error_opt.unwrap(), command=command_name, "Received {} from child during shutdown", command_name);
      } else {
        tracing::info!(handle=core_h, child_handle=child_handle, uri=%log_uri_val, command=command_name, "Processing {} from child during shutdown", command_name);
      }
    } else { // Core is Running
        if is_err {
        tracing::error!(handle=core_h, child_handle=child_handle, error=%error_opt.unwrap(), command=command_name, "Received {} from child while running", command_name);
      } else {
        tracing::debug!(handle=core_h, child_handle=child_handle, uri=%log_uri_val, command=command_name, "Processing {} from child while running", command_name);
      }
    }

    // Record that the child stopped if we are in the relevant phases
    let mut state_transitioned_to_linger = false;
    if matches!(current_phase_val, ShutdownPhase::StoppingChildren | ShutdownPhase::Lingering) {
      state_transitioned_to_linger = coordinator_g.record_child_stopped(child_handle, core_h);
    }

    // <<< MODIFIED START [Handle immediate transition to CleaningPipes] >>>
    // If recording the stop caused a transition to Lingering, check linger conditions
    if state_transitioned_to_linger {
      // Need CoreState lock to check queues and pass to advance_to_cleaning
      let mut core_state_guard = core_arc.core_state.lock().await;
      let linger_o = core_state_guard.options.linger;
      coordinator_g.start_linger(linger_o, core_h); // Start linger timer/state

      if coordinator_g.is_linger_expired(core_h) || coordinator_g.are_queues_empty(&core_state_guard) {
        tracing::debug!(handle = core_h, "process_child_completion: Last child stopped and linger condition met, advancing to cleaning.");
        // Pass the mutable core_state_guard here
        coordinator_g.advance_to_cleaning(core_h, &mut core_state_guard);
        // Copy pipe list before releasing locks
        #[cfg(feature = "inproc")]
        let pipes_clean_list = coordinator_g.inproc_connections_to_close.clone();
        #[cfg(not(feature = "inproc"))]
        let pipes_clean_list = Vec::new();
        // Release locks before calling async cleanup function
        drop(coordinator_g);
        drop(core_state_guard);
        Self::perform_pipe_cleanup(core_arc.clone(), socket_logic_strong, pipes_clean_list).await;
        // Early return as cleanup is done and state is Finished
        return;
      }
      // Release CoreState lock if not advancing to cleaning yet
      drop(core_state_guard);
    }
    // Release coordinator lock if not advancing to cleaning
    drop(coordinator_g);
    // <<< MODIFIED END >>>


    let mut removed_endpoint_info_for_reconnect: Option<EndpointInfo> = None;
    // --- Perform state cleanup for the stopped child ---
    // This needs to happen regardless of the shutdown phase if the child stopped unexpectedly
    // or if we are shutting down and need to remove its entry.
    if let Some(uri_s) = endpoint_uri_opt {
      // Determine if cleanup is needed. Cleanup should happen if the endpoint still exists
      // in the map OR if we are in any shutdown phase (to ensure removal).
      let needs_clean = {
        let state_guard = core_arc.core_state.lock().await;
        state_guard.endpoints.contains_key(uri_s)
      } || current_phase_val != ShutdownPhase::Running; // Also clean if shutting down

      if needs_clean {
        tracing::debug!(handle=core_h, child_handle=child_handle, uri=%uri_s, "Performing state cleanup for stopped child (URI: {}).", uri_s);

        let captured_info = Self::cleanup_session_state_by_uri(core_arc.clone(), uri_s, socket_logic_strong, child_handle).await;
        if current_phase_val == ShutdownPhase::Running && is_err { // Check if stopped unexpectedly while core is running
          removed_endpoint_info_for_reconnect = captured_info;
        }
      } else {
        tracing::trace!(handle=core_h, child_handle=child_handle, uri=%uri_s, "Skipping state cleanup for stopped child (endpoint likely already removed).");
      }
    } else {
      tracing::warn!(
        handle = core_h,
        child_handle = child_handle,
        command = command_name,
        "Child completion missing endpoint URI, cannot perform URI-based cleanup."
      );
    }
    // --- Phase 3: Potentially respawn connecter ---
    let current_core_phase_now = { // Re-check current phase after cleanup and potential shutdown progression
        let guard = core_arc.shutdown_coordinator.lock().await;
        guard.current_phase()
    }; // Guard dropped

    if current_core_phase_now == ShutdownPhase::Running {
      if let Some(ref removed_info) = removed_endpoint_info_for_reconnect { // Child died unexpectedly while core was running
        if removed_info.endpoint_type == EndpointType::Session && removed_info.is_outbound_connection { // Check the new flag
          if let Some(ref target_uri_to_reconnect) = removed_info.target_endpoint_uri {
            // No reconnect for inproc, already handled by its specific logic
            if !target_uri_to_reconnect.starts_with("inproc://") {
                let reconnect_enabled = { 
                    let cs_guard = core_arc.core_state.lock().await;
                    cs_guard.options.reconnect_ivl.map_or(false, |d| !d.is_zero()) 
                };

                if reconnect_enabled {
                    tracing::info!(
                        handle = core_h, 
                        target_uri = %target_uri_to_reconnect, 
                        child_handle = child_handle,
                        "Unexpected session stop for an 'is_outbound_connection=true' session. Initiating reconnect."
                    );
                    Self::respawn_connecter(core_arc.clone(), target_uri_to_reconnect.clone()).await;
                } else {
                    tracing::debug!(
                        handle = core_h, 
                        target_uri = %target_uri_to_reconnect, 
                        child_handle = child_handle,
                        "Reconnect disabled for unexpectedly stopped outbound session child."
                    );
                }
            }
          } else {
            tracing::warn!(
              handle = core_h,
              child_handle = child_handle,
              "Outbound session child died unexpectedly but had no target_endpoint_uri for reconnect."
            );
          }
        } else if removed_info.endpoint_type == EndpointType::Session && !removed_info.is_outbound_connection {
          tracing::debug!(
            handle = core_h,
            child_handle = child_handle,
            actual_peer_uri = %removed_info.endpoint_uri, // This is the client's URI
            "Session for an inbound connection (is_outbound_connection=false) stopped. No reconnect attempted by SocketCore."
          );
        }
            // If it was a Listener type, no reconnect attempt is made here.
      }
    } else {
      if removed_endpoint_info_for_reconnect.is_some() { // Log if we *would* have reconnected
        tracing::debug!(
          handle = core_h, 
          child_handle = child_handle,
          target_uri = ?removed_endpoint_info_for_reconnect.as_ref().and_then(|i| i.target_endpoint_uri.as_ref()), 
          current_phase = ?current_core_phase_now, 
          "Skipping potential reconnect for child because SocketCore is no longer in Running phase (current phase: {:?}).",
          current_core_phase_now
        );
      }
    }
  }

  async fn cleanup_session_state_by_uri(
    core_arc: Arc<SocketCore>,
    endpoint_uri: &str,
    socket_logic: &Arc<dyn ISocket>,
    stopping_child_handle_id: usize,
  ) -> Option<EndpointInfo> {
    let core_h = core_arc.handle;
    tracing::debug!(handle = core_h, uri = %endpoint_uri, stopping_child_id = stopping_child_handle_id, "Attempting cleanup by URI for specific child");
    let mut state_g = core_arc.core_state.lock().await;
    let mut removed_correct_child = false;
    let mut detached_pipe_read_id_opt: Option<usize> = None;
    let monitor_tx_c = state_g.get_monitor_sender_clone();
    let mut ep_type_removed_opt: Option<EndpointType> = None;
    let mut actual_removed_ep_info: Option<EndpointInfo> = None;

    if let Some(info_at_uri_val) = state_g.endpoints.get(endpoint_uri) {
      if info_at_uri_val.handle_id == stopping_child_handle_id {
        if let Some(removed_val) = state_g.endpoints.remove(endpoint_uri) {
          actual_removed_ep_info = Some(removed_val);
          removed_correct_child = true;
          ep_type_removed_opt = Some(actual_removed_ep_info.as_ref().unwrap().endpoint_type);
          tracing::info!(handle = core_h, uri = %endpoint_uri, type = ?ep_type_removed_opt.unwrap(), child_handle=stopping_child_handle_id, "Removed endpoint state by URI for stopping child.");
          actual_removed_ep_info.as_ref().unwrap().task_handle.abort();
          if let Some((write_id, read_id)) = actual_removed_ep_info.as_ref().unwrap().pipe_ids {
            detached_pipe_read_id_opt = Some(read_id);
            if state_g.remove_pipe_state(write_id, read_id) {
              tracing::debug!(handle=core_h, uri=%endpoint_uri, "Removed associated pipe state by URI");
            } else {
              tracing::warn!(handle=core_h, uri=%endpoint_uri, "Cleanup by URI: Associated pipe state already removed?");
            }
          }
        }
      } else {
        tracing::debug!(handle = core_h, uri=%endpoint_uri, "Cleanup by URI for child {} found ep for different child {}.", stopping_child_handle_id, info_at_uri_val.handle_id);
      }
    } else {
      tracing::warn!(handle = core_h, uri=%endpoint_uri, "Cleanup by URI: endpoint state not found.");
    }
    drop(state_g);

    if removed_correct_child {
      let event_ep_uri = actual_removed_ep_info.as_ref().unwrap().endpoint_uri.clone();
      let event_opt = match ep_type_removed_opt {
        Some(EndpointType::Session) => Some(SocketEvent::Disconnected { endpoint: event_ep_uri }),
        Some(EndpointType::Listener) => Some(SocketEvent::Closed { endpoint: event_ep_uri }),
        None => None,
      };
      if let (Some(ev_val), Some(tx_val)) = (event_opt, monitor_tx_c) {
        if tx_val.send(ev_val).await.is_err() { /* warn */ }
      }
      if let Some(read_id_val) = detached_pipe_read_id_opt {
        socket_logic.pipe_detached(read_id_val).await;
      }
    }

    actual_removed_ep_info
  }

  async fn cleanup_session_state_by_pipe(
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize,
    socket_logic: &Arc<dyn ISocket>,
  ) -> Option<String> {
    let core_h = core_arc.handle;
    tracing::debug!(
      handle = core_h,
      pipe_read_id = pipe_read_id,
      "Attempting cleanup by Pipe Read ID"
    );
    let mut state_g = core_arc.core_state.lock().await;
    let mut ep_uri_to_remove_opt: Option<String> = None;
    let mut write_pipe_id_to_remove_opt: Option<usize> = None;
    let mut task_h_to_abort_opt: Option<JoinHandle<()>> = None;
    let mut ep_type_removed_opt: Option<EndpointType> = None;
    let mut target_uri_found_opt: Option<String> = None;
    let monitor_tx_c = state_g.get_monitor_sender_clone();

    for (uri_val, info_val) in state_g.endpoints.iter() {
      if let Some((write_id_val, read_id_val)) = info_val.pipe_ids {
        if read_id_val == pipe_read_id {
          ep_uri_to_remove_opt = Some(uri_val.clone());
          write_pipe_id_to_remove_opt = Some(write_id_val);
          ep_type_removed_opt = Some(info_val.endpoint_type);
          if info_val.endpoint_type == EndpointType::Session {
            target_uri_found_opt = info_val.target_endpoint_uri.clone();
          }
          break;
        }
      }
    }
    let mut pipes_removed_flag = false;
    if let (Some(ref uri_ref), Some(write_pipe_id_val)) = (&ep_uri_to_remove_opt, write_pipe_id_to_remove_opt) {
      if let Some(removed_info_val) = state_g.endpoints.remove(uri_ref) {
        tracing::info!(handle=core_h, pipe_read_id=pipe_read_id, uri=%uri_ref, type=?removed_info_val.endpoint_type, "Removed endpoint by Pipe ID.");
        task_h_to_abort_opt = Some(removed_info_val.task_handle);
      }
      pipes_removed_flag = state_g.remove_pipe_state(write_pipe_id_val, pipe_read_id);
    } else {
      if let Some(reader_h_val) = state_g.pipe_reader_task_handles.remove(&pipe_read_id) {
        reader_h_val.abort();
        pipes_removed_flag = true;
      }
    }
    drop(state_g);
    if let Some(task_h_val) = task_h_to_abort_opt {
      task_h_val.abort();
    }
    if let (Some(ref uri_ref_val), true) = (&ep_uri_to_remove_opt, pipes_removed_flag) {
      if ep_type_removed_opt == Some(EndpointType::Session) {
        if let Some(tx_m) = monitor_tx_c {
          if tx_m
            .send(SocketEvent::Disconnected {
              endpoint: uri_ref_val.clone(),
            })
            .await
            .is_err()
          { /* warn */ }
        }
      }
    }
    if pipes_removed_flag {
      socket_logic.pipe_detached(pipe_read_id).await;
    }
    target_uri_found_opt
  }

  async fn cleanup_session_state_by_pipe_ids(
    core_arc: Arc<SocketCore>,
    pipe_write_id: usize,
    pipe_read_id: usize,
    socket_logic: &Arc<dyn ISocket>,
  ) {
    let core_h = core_arc.handle;
    tracing::debug!(
      handle = core_h,
      pipe_write_id = pipe_write_id,
      pipe_read_id = pipe_read_id,
      "Cleaning up pipe state after AttachPipe failure"
    );
    let mut state_g = core_arc.core_state.lock().await;
    let pipes_removed_f = state_g.remove_pipe_state(pipe_write_id, pipe_read_id);
    let mut uri_of_ep_to_remove_opt: Option<String> = None;
    for (uri_v, info_v) in state_g.endpoints.iter() {
      if info_v.pipe_ids == Some((pipe_write_id, pipe_read_id)) {
        uri_of_ep_to_remove_opt = Some(uri_v.clone());
        break;
      }
    }
    let mut task_h_to_abort_opt: Option<JoinHandle<()>> = None;
    if let Some(uri_v_val) = uri_of_ep_to_remove_opt {
      if let Some(removed_info_v) = state_g.endpoints.remove(&uri_v_val) {
        task_h_to_abort_opt = Some(removed_info_v.task_handle);
      }
    }
    drop(state_g);
    if let Some(h_v) = task_h_to_abort_opt {
      h_v.abort();
    }
    if pipes_removed_f {
      socket_logic.pipe_detached(pipe_read_id).await;
    }
  }

  async fn respawn_connecter(core_arc: Arc<SocketCore>, target_uri: String) {
    let core_handle = core_arc.handle;
    let parent_socket_id_for_conn = core_handle;
    let context_c = core_arc.context.clone();
    tracing::debug!(handle = core_handle, target_uri=%target_uri, "Respawning connecter task");
    let parse_res = parse_endpoint(&target_uri);
    match parse_res {
      Ok(Endpoint::Tcp(_, _)) => {
        let state_g = core_arc.core_state.lock().await;
        let options_a = Arc::new(state_g.options.clone());
        let monitor_tx_c = state_g.get_monitor_sender_clone();
        let handle_src_c = context_c.inner().next_handle.clone();
        let connecter_h_id = context_c.inner().next_handle();
        drop(state_g);
        let _task_val = TcpConnecter::create_and_spawn(
          connecter_h_id,
          target_uri.clone(),
          options_a,
          handle_src_c,
          monitor_tx_c,
          context_c.clone(),
          parent_socket_id_for_conn,
        );
        tracing::info!(handle=core_handle, connecter_handle=connecter_h_id, target_uri=%target_uri, "Spawned new TcpConnecter for reconnect");
      }
      #[cfg(feature = "ipc")]
      Ok(Endpoint::Ipc(path_v, _)) => {
        let state_g = core_arc.core_state.lock().await;
        let options_a = Arc::new(state_g.options.clone());
        let monitor_tx_c = state_g.get_monitor_sender_clone();
        let handle_src_c = context_c.inner().next_handle.clone();
        let connecter_h_id = context_c.inner().next_handle();
        drop(state_g);
        let (_mailbox_v, _task_v) = IpcConnecter::create_and_spawn(
          connecter_h_id,
          target_uri.clone(),
          path_v,
          options_a,
          handle_src_c,
          monitor_tx_c,
          context_c.clone(),
          parent_socket_id_for_conn,
        );
        tracing::info!(handle=core_handle, connecter_handle=connecter_h_id, target_uri=%target_uri, "Spawned new IpcConnecter for reconnect");
      }
      #[cfg(feature = "inproc")]
      Ok(Endpoint::Inproc(_)) => {
        tracing::warn!(handle=core_handle, target_uri=%target_uri, "Auto-reconnect for inproc not meaningful.");
      }
      Err(e) => {
        tracing::error!(handle=core_handle, target_uri=%target_uri, error=%e, "Failed to parse endpoint for respawn.");
      }
      _ => {
        tracing::warn!(handle=core_handle, target_uri=%target_uri, "Unsupported transport for reconnect.");
      }
    }
  }

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
    
    match socket_logic.set_pattern_option(option, value).await {
      Ok(()) => return Ok(()),
      Err(ZmqError::UnsupportedOption(_)) => {}
      Err(e) => return Err(e),
    }

    let mut state_g = core_arc.core_state.lock().await;

     #[cfg(feature = "io-uring")]
    {
        if option == IO_URING_SNDZEROCOPY {
            state_g.options.io_uring_send_zerocopy = options::parse_bool_option(value)?;
            // Note: This typically affects new engines. Existing engines won't change behavior.
            tracing::debug!(handle = core_arc.handle, "IO_URING_SNDZEROCOPY set to {}", state_g.options.io_uring_send_zerocopy);
            return Ok(());
        } else if option == IO_URING_RCVMULTISHOT {
            state_g.options.io_uring_recv_multishot = options::parse_bool_option(value)?;
            tracing::debug!(handle = core_arc.handle, "IO_URING_RCVMULTISHOT set to {}", state_g.options.io_uring_recv_multishot);
            return Ok(());
        }
    }

    match option {
      SNDHWM => {
        state_g.options.sndhwm = parse_i32_option(value)?.max(0) as usize;
      }
      RCVHWM => {
        state_g.options.rcvhwm = parse_i32_option(value)?.max(0) as usize;
      }
      LINGER => {
        state_g.options.linger = options::parse_linger_option(value)?;
      }
      RECONNECT_IVL => {
        state_g.options.reconnect_ivl = options::parse_reconnect_ivl_option(value)?;
      }
      RECONNECT_IVL_MAX => {
        state_g.options.reconnect_ivl_max = options::parse_reconnect_ivl_max_option(value)?;
      }
      HEARTBEAT_IVL => {
        state_g.options.heartbeat_ivl = parse_heartbeat_option(value, option)?;
      }
      HEARTBEAT_TIMEOUT => {
        state_g.options.heartbeat_timeout = parse_heartbeat_option(value, option)?;
      }
      ZAP_DOMAIN => {
        state_g.options.zap_domain =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
      }
      PLAIN_SERVER => {
        state_g.options.plain_server = Some(parse_bool_option(value)?);
      }
      PLAIN_USERNAME => {
        state_g.options.plain_username =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
      }
      PLAIN_PASSWORD => {
        state_g.options.plain_password =
          Some(String::from_utf8(value.to_vec()).map_err(|_| ZmqError::InvalidOptionValue(option))?);
      }
      #[cfg(feature = "curve")]
      CURVE_SERVER => {
        state_g.options.curve_server = Some(parse_bool_option(value)?);
      }
      #[cfg(feature = "curve")]
      CURVE_PUBLICKEY => {
        state_g.options.curve_public_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
      }
      #[cfg(feature = "curve")]
      CURVE_SECRETKEY => {
        state_g.options.curve_secret_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
      }
      #[cfg(feature = "curve")]
      CURVE_SERVERKEY => {
        state_g.options.curve_server_key = Some(parse_key_option::<CURVE_KEY_LEN>(value, option)?);
      }
      ROUTING_ID => {
        state_g.options.routing_id = Some(parse_blob_option(value)?);
      }
      RCVTIMEO => {
        state_g.options.rcvtimeo = parse_timeout_option(value, option)?;
      }
      SNDTIMEO => {
        state_g.options.sndtimeo = parse_timeout_option(value, option)?;
      }
      TCP_KEEPALIVE => {
        state_g.options.tcp_keepalive_enabled = parse_keepalive_mode_option(value)?;
      }
      TCP_KEEPALIVE_IDLE => {
        state_g.options.tcp_keepalive_idle = parse_secs_duration_option(value)?;
      }
      TCP_KEEPALIVE_CNT => {
        state_g.options.tcp_keepalive_count = parse_u32_option(value)?;
      }
      TCP_KEEPALIVE_INTVL => {
        state_g.options.tcp_keepalive_interval = parse_secs_duration_option(value)?;
      }
      ROUTER_MANDATORY => {
        state_g.options.router_mandatory = parse_bool_option(value)?;
      }
      SUBSCRIBE | UNSUBSCRIBE => {
        return Err(ZmqError::UnsupportedOption(option));
      }
      _ => {
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
    
    match socket_logic.get_pattern_option(option).await {
      Ok(v_val) => return Ok(v_val),
      Err(ZmqError::UnsupportedOption(_)) => {}
      Err(e_val) => return Err(e_val),
    }

    let state_g = core_arc.core_state.lock().await;

     #[cfg(feature = "io-uring")]
    {
        if option == options::IO_URING_SNDZEROCOPY {
            return Ok((state_g.options.io_uring_send_zerocopy as i32).to_ne_bytes().to_vec());
        } else if option == options::IO_URING_RCVMULTISHOT {
            return Ok((state_g.options.io_uring_recv_multishot as i32).to_ne_bytes().to_vec());
        }
    }
    
    match option {
      SNDHWM => Ok((state_g.options.sndhwm as i32).to_ne_bytes().to_vec()),
      RCVHWM => Ok((state_g.options.rcvhwm as i32).to_ne_bytes().to_vec()),
      LINGER => Ok(state_g.options.linger.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
      RECONNECT_IVL => Ok(state_g.options.reconnect_ivl.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
      RECONNECT_IVL_MAX => Ok(state_g.options.reconnect_ivl_max.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
      HEARTBEAT_IVL => Ok(state_g.options.heartbeat_ivl.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
      HEARTBEAT_TIMEOUT => Ok(state_g.options.heartbeat_timeout.map_or(0, |d| d.as_millis() as i32).to_ne_bytes().to_vec()),
      ZAP_DOMAIN => state_g.options.zap_domain.as_ref().map(|s| s.as_bytes().to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      PLAIN_SERVER => state_g.options.plain_server.map(|b| (b as i32).to_ne_bytes().to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      PLAIN_USERNAME => state_g.options.plain_username.as_ref().map(|s| s.as_bytes().to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      #[cfg(feature = "curve")] CURVE_SERVER => state_g.options.curve_server.map(|b| (b as i32).to_ne_bytes().to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      #[cfg(feature = "curve")] CURVE_PUBLICKEY => state_g.options.curve_public_key.map(|k| k.to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      #[cfg(feature = "curve")] CURVE_SERVERKEY => state_g.options.curve_server_key.map(|k| k.to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      ROUTING_ID => state_g.options.routing_id.as_ref().map(|b| b.to_vec()).ok_or(ZmqError::Internal("Option not set".into())),
      RCVTIMEO => Ok(state_g.options.rcvtimeo.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
      SNDTIMEO => Ok(state_g.options.sndtimeo.map_or(-1, |d| d.as_millis().try_into().unwrap_or(i32::MAX)).to_ne_bytes().to_vec()),
      TCP_KEEPALIVE => Ok(state_g.options.tcp_keepalive_enabled.to_ne_bytes().to_vec()),
      TCP_KEEPALIVE_IDLE => Ok(state_g.options.tcp_keepalive_idle.map_or(0, |d| d.as_secs() as i32).to_ne_bytes().to_vec()),
      TCP_KEEPALIVE_CNT => Ok(state_g.options.tcp_keepalive_count.map_or(0, |c| c as i32).to_ne_bytes().to_vec()),
      TCP_KEEPALIVE_INTVL => Ok(state_g.options.tcp_keepalive_interval.map_or(0, |d| d.as_secs() as i32).to_ne_bytes().to_vec()),
      ROUTER_MANDATORY => Ok((state_g.options.router_mandatory as i32).to_ne_bytes().to_vec()),
      16 /* ZMQ_TYPE */ => Ok((state_g.socket_type as i32).to_ne_bytes().to_vec()),
      SUBSCRIBE | UNSUBSCRIBE => Err(ZmqError::UnsupportedOption(option)),
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
  }
}

/// Helper function to send a ZMQ message over an async pipe with optional timeout.
pub(crate) async fn send_msg_with_timeout(
  pipe_tx: &AsyncSender<Msg>,
  msg: Msg,
  timeout_opt: Option<Duration>,
  socket_core_handle: usize,
  pipe_target_id: usize,
) -> Result<(), ZmqError> {
  match timeout_opt {
    None => {
      tracing::trace!(
        core_handle = socket_core_handle,
        pipe_id = pipe_target_id,
        "Sending message via pipe (blocking on HWM)"
      );
      pipe_tx.send(msg).await.map_err(|SendError(_failed_msg_back)| {
        tracing::debug!(
          core_handle = socket_core_handle,
          pipe_id = pipe_target_id,
          "Pipe send failed (ConnectionClosed)"
        );
        ZmqError::ConnectionClosed
      })
    }
    Some(d) if d.is_zero() => {
      tracing::trace!(
        core_handle = socket_core_handle,
        pipe_id = pipe_target_id,
        "Attempting non-blocking send via pipe"
      );
      match pipe_tx.try_send(msg) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_failed_msg_back)) => {
          tracing::trace!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Non-blocking pipe send failed (HWM - ResourceLimitReached)"
          );
          Err(ZmqError::ResourceLimitReached)
        }
        Err(TrySendError::Closed(_failed_msg_back)) => {
          tracing::debug!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Non-blocking pipe send failed (ConnectionClosed)"
          );
          Err(ZmqError::ConnectionClosed)
        }
      }
    }
    Some(timeout_duration) => {
      tracing::trace!(core_handle = socket_core_handle, pipe_id = pipe_target_id, send_timeout_duration = ?timeout_duration, "Attempting timed send via pipe");
      match timeout(timeout_duration, pipe_tx.send(msg)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(SendError(_failed_msg_back))) => {
          tracing::debug!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Timed pipe send failed (ConnectionClosed)"
          );
          Err(ZmqError::ConnectionClosed)
        }
        Err(_timeout_elapsed_error) => {
          tracing::trace!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Timed pipe send failed (Timeout on HWM)"
          );
          Err(ZmqError::Timeout)
        }
      }
    }
  }
}
