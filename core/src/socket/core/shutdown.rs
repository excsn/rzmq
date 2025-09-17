// core/src/socket/core/shutdown.rs

use crate::error::ZmqError;
use crate::runtime::{ActorType, Command, SystemEvent};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::{CoreState, EndpointType, ShutdownCoordinator, ShutdownPhase}; // Import from state.rs
use crate::socket::core::{command_processor, pipe_manager, SocketCore}; // pipe_manager for cleanup_stopped_child_resources
use crate::socket::ISocket;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
// JoinHandle not directly used here, but by callers that store it in EndpointInfo for Listeners

// --- ShutdownCoordinator Methods ---
impl ShutdownCoordinator {
  pub(crate) fn current_phase(&self) -> ShutdownPhase {
    self.state
  }

  pub(crate) fn begin_shutdown_sequence(
    &mut self,
    core_handle: usize,
    core_s_reader: &CoreState, // Pass read reference to CoreState
  ) -> bool {
    if self.state != ShutdownPhase::Running {
      return false;
    }
    tracing::debug!(
      handle = core_handle,
      "ShutdownCoordinator: Initiating shutdown. Populating pending lists."
    );

    self.pending_child_actors.clear();
    self.pending_connections_to_close.clear();
    #[cfg(feature = "inproc")]
    self.inproc_connections_to_cleanup.clear();

    for (ep_uri, ep_info) in core_s_reader.endpoints.iter() {
      match ep_info.endpoint_type {
        EndpointType::Listener => {
          // Only track listeners if SocketCore actually has a task_handle for them.
          if ep_info.task_handle.is_some() {
            self
              .pending_child_actors
              .insert(ep_info.handle_id, ep_uri.clone());
            tracing::trace!(handle=core_handle, child_id=ep_info.handle_id, uri=%ep_uri, "Registered Listener for shutdown tracking.");
          }
        }
        EndpointType::Session => {
          // For all active sessions (SessionActor or UringFd), record them for ISocketConnection::close_connection()
          self.pending_connections_to_close.insert(
            ep_info.handle_id, // Keyed by Session actor handle or RawFd (as usize)
            (ep_uri.clone(), ep_info.connection_iface.clone()),
          );
          tracing::trace!(handle=core_handle, conn_id=ep_info.handle_id, uri=%ep_uri, "Registered active Connection for ISocketConnection.close().");

          #[cfg(feature = "inproc")]
          if ep_uri.starts_with("inproc://") && !ep_info.is_outbound_connection {
            if let Some(pipe_ids) = ep_info.pipe_ids {
              // These are actual pipe IDs for inproc binder
              self
                .inproc_connections_to_cleanup
                .push((pipe_ids.0, pipe_ids.1, ep_uri.clone()));
              tracing::trace!(handle=core_handle, uri=%ep_uri, "Registered inproc binder connection for final pipe cleanup.");
            }
          }
        }
      }
    }
    true // Shutdown newly initiated
  }

  /// Records that a child actor (Listener) has stopped.
  /// Returns true if this was the last pending entity (actor or connection),
  /// triggering a potential move to Lingering.
  fn record_child_actor_stopped(&mut self, child_actor_handle: usize, core_handle: usize) -> bool {
    if self.state == ShutdownPhase::Finished {
      return false;
    }

    if self
      .pending_child_actors
      .remove(&child_actor_handle)
      .is_some()
    {
      tracing::debug!(
        handle = core_handle,
        child_id = child_actor_handle,
        "Tracked child actor stopped."
      );
      return self.pending_child_actors.is_empty() && self.pending_connections_to_close.is_empty();
    }
    false
  }

  /// Records that an active connection (Session/UringFd) has been closed/stopped.
  /// Returns true if this was the last pending entity (actor or connection),
  /// triggering a potential move to Lingering.
  fn record_connection_closed(&mut self, connection_id: usize, core_handle: usize) -> bool {
    if self.state == ShutdownPhase::Finished {
      return false;
    }

    if self
      .pending_connections_to_close
      .remove(&connection_id)
      .is_some()
    {
      tracing::debug!(
        handle = core_handle,
        conn_id = connection_id,
        "Tracked active connection closed/stopped."
      );
      return self.pending_connections_to_close.is_empty() && self.pending_child_actors.is_empty();
    }
    false
  }

  pub(crate) fn start_linger_if_needed(
    &mut self,
    linger_duration_option: Option<Duration>,
    core_handle: usize,
  ) {
    if self.state != ShutdownPhase::Lingering {
      tracing::warn!(handle=core_handle, current_phase = ?self.state, "Attempted to start LINGER in incorrect state.");
      return;
    }
    if self.linger_deadline.is_some() && linger_duration_option != Some(Duration::ZERO) {
      tracing::trace!(
        handle = core_handle,
        "Linger deadline already set or effectively active."
      );
      return;
    }
    match linger_duration_option {
      None => {
        self.linger_deadline = None;
        tracing::debug!(handle = core_handle, "Starting infinite LINGER.");
      }
      Some(d) if d.is_zero() => {
        self.linger_deadline = Some(Instant::now());
        tracing::debug!(handle = core_handle, "LINGER is zero.");
      }
      Some(d) => {
        self.linger_deadline = Some(Instant::now() + d);
        tracing::debug!(handle = core_handle, ?d, "Starting timed LINGER.");
      }
    }
  }

  pub(crate) fn is_linger_expired_or_queues_empty(
    &self,
    core_s_reader: &CoreState,
    core_handle: usize,
  ) -> bool {
    if self.state != ShutdownPhase::Lingering {
      return false;
    }
    let core_pipes_empty = core_s_reader
      .pipes_tx
      .values()
      .all(|sender| sender.is_empty());

    if core_pipes_empty {
      tracing::debug!(
        handle = core_handle,
        "Linger check: All SocketCore pipes_tx empty. Linger can complete."
      );
      return true;
    }
    if let Some(deadline) = self.linger_deadline {
      if Instant::now() >= deadline {
        tracing::debug!(
          handle = core_handle,
          "Linger deadline expired. Core pipes_tx empty: {}.",
          core_pipes_empty
        );
        return true;
      }
    }
    false
  }
}

// --- High-Level Shutdown Orchestration Functions ---

pub(crate) async fn publish_socket_closing_event(
  context: &crate::context::Context,
  socket_id: usize,
) {
  let event = SystemEvent::SocketClosing { socket_id };
  if context.event_bus().publish(event).is_err() {
    // Borrow error from SendError is not Clone.
    tracing::warn!(
      socket_handle = socket_id,
      "Failed to publish SocketClosing event for self."
    );
  } else {
    tracing::debug!(
      socket_handle = socket_id,
      "Published SocketClosing event for self."
    );
  }
}

pub(crate) async fn initiate_core_shutdown(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  was_due_to_error: bool,
) {
  let core_handle = core_arc.handle;
  let mut coordinator = core_arc.shutdown_coordinator.lock().await;

  if coordinator.state != ShutdownPhase::Running {
    return;
  }
  
  tracing::info!(
    handle = core_handle,
    was_due_to_error,
    "Initiating SocketCore shutdown steps."
  );

  // Event publishing is now done by the caller (command_loop or event_processor)
  // publish_socket_closing_event(&core_arc.context, core_handle).await;

  {
    let core_s_reader = core_arc.core_state.read(); // Read lock for populating coordinator
    coordinator.begin_shutdown_sequence(core_handle, &core_s_reader);
  }

  let child_actors_to_stop = coordinator.pending_child_actors.clone();
  let connections_to_close = coordinator.pending_connections_to_close.clone();
  // `inproc_connections_to_cleanup` is used later by `perform_final_pipe_cleanup`.
  // No need to clone it here just for stopping, as inproc "connections" are stopped
  // by closing their ISocketConnection if they appear in `connections_to_close`.

  drop(coordinator); // Release coordinator lock before async calls

  stop_child_listener_actors(core_arc.clone(), child_actors_to_stop).await;
  close_active_connections(core_arc.clone(), connections_to_close).await;

  let mut coordinator = core_arc.shutdown_coordinator.lock().await; // Re-acquire lock
  if coordinator.pending_child_actors.is_empty()
    && coordinator.pending_connections_to_close.is_empty()
  {
    tracing::debug!(
      handle = core_handle,
      "No pending children/connections after initial stop signals. Moving to Lingering."
    );
    coordinator.state = ShutdownPhase::Lingering;
    let linger_opt = core_arc.core_state.read().options.linger;
    coordinator.start_linger_if_needed(linger_opt, core_handle);

    if coordinator.is_linger_expired_or_queues_empty(&core_arc.core_state.read(), core_handle) {
      {
        let mut core_s_write = core_arc.core_state.write();
        advance_to_cleaning_phase(&mut coordinator, core_handle, &mut core_s_write);
      }
      #[cfg(feature = "inproc")]
      let pipes_to_clean = coordinator.inproc_connections_to_cleanup.clone();
      #[cfg(not(feature = "inproc"))]
      let pipes_to_clean = Vec::new();
      drop(coordinator);
      perform_final_pipe_cleanup(core_arc.clone(), socket_logic_strong, pipes_to_clean).await;
    }
  } else {
    tracing::debug!(
      handle = core_handle,
      "State: StoppingChildren. Waiting for child actors and active connections to stop."
    );
    coordinator.state = ShutdownPhase::StoppingChildren;
  }
}

async fn stop_child_listener_actors(
  core_arc: Arc<SocketCore>,
  child_actors_to_stop: HashMap<usize, String>, // (handle_id, uri)
) {
  let core_handle = core_arc.handle;
  if child_actors_to_stop.is_empty() {
    return;
  }

  tracing::debug!(
    handle = core_handle,
    count = child_actors_to_stop.len(),
    "Stopping child Listener actors..."
  );
  let mut stop_futs = Vec::new();
  for (child_actor_handle_id, child_uri) in child_actors_to_stop.iter() {
    let mailbox_opt = core_arc
      .core_state
      .read()
      .endpoints
      .get(child_uri)
      .filter(|ei| ei.endpoint_type == EndpointType::Listener) // Ensure it's a listener
      .map(|ei| ei.mailbox.clone());

    if let Some(mailbox) = mailbox_opt {
      let child_id_clone = *child_actor_handle_id;
      stop_futs.push(async move {
        if mailbox.send(Command::Stop).await.is_err() {
          tracing::warn!(
            parent_handle = core_handle,
            child_handle = child_id_clone,
            "Failed to send Stop to Listener actor {}.",
            child_id_clone
          );
        }
      });
    } else {
      // This implies the child_actors_to_stop list from coordinator was stale or incorrect.
      tracing::warn!(parent_handle = core_handle, child_handle = *child_actor_handle_id, uri = %child_uri, "Could not find mailbox for Listener actor during shutdown. It may have already stopped.");
    }
  }
  if !stop_futs.is_empty() {
    futures::future::join_all(stop_futs).await;
  }
}

async fn close_active_connections(
  core_arc: Arc<SocketCore>,
  // connections_to_close is HashMap<connection_instance_id (EndpointInfo.handle_id), (uri, iface)>
  connections_to_close: HashMap<usize, (String, Arc<dyn ISocketConnection>)>,
) {
  let core_handle = core_arc.handle;
  if connections_to_close.is_empty() {
    return;
  }

  tracing::debug!(
    handle = core_handle,
    count = connections_to_close.len(),
    "Closing active connections via ISocketConnection..."
  );
  let mut close_futs = Vec::new();

  // Need to iterate carefully due to async and coordinator lock
  // Collect IDs to modify coordinator after futures.
  let mut inproc_connections_processed_ids = Vec::new();

  for (conn_id, (conn_uri, conn_iface)) in connections_to_close.iter() {
    let iface_clone = conn_iface.clone();
    let id_clone = *conn_id; // This is the EndpointInfo.handle_id
    let uri_clone = conn_uri.clone();

    close_futs.push(async move {
      let close_result = iface_clone.close_connection().await;
      if let Err(e) = close_result {
        tracing::warn!(parent_handle = core_handle, conn_id = id_clone, uri = %uri_clone, "Error from ISocketConnection.close_connection(): {}", e);
      }
      // Return the conn_id and uri if it was inproc, to process with coordinator later
      if uri_clone.starts_with("inproc://") {
        Some(id_clone)
      } else {
        None
      }
    });
  }

  let results = futures::future::join_all(close_futs).await;
  for res_opt in results {
    if let Some(inproc_conn_id) = res_opt {
      inproc_connections_processed_ids.push(inproc_conn_id);
    }
  }

  // Now, update the coordinator for all processed inproc connections
  if !inproc_connections_processed_ids.is_empty() {
    let mut coordinator = core_arc.shutdown_coordinator.lock().await;
    let mut made_progress_to_linger = false;
    for inproc_conn_id_val in inproc_connections_processed_ids {
      // If record_connection_closed returns true, it means this was the last pending entity
      // and the coordinator might have transitioned.
      if coordinator.record_connection_closed(inproc_conn_id_val, core_handle) {
        made_progress_to_linger = true;
      }
    }

    // If coordinator state advanced to Lingering due to these inproc connections closing
    if made_progress_to_linger && coordinator.state == ShutdownPhase::Lingering {
      tracing::debug!(
        handle = core_handle,
        "close_active_connections: Inproc connections closed, last pending. Advancing linger/cleaning."
      );
      let linger_opt_val = core_arc.core_state.read().options.linger;
      coordinator.start_linger_if_needed(linger_opt_val, core_handle); // Call the method on coordinator
                                                                       // The check for linger expiry and advancing to cleaning will happen in the main command_loop's linger_check_interval.
                                                                       // Or, we can replicate that check here if we want to be more proactive.
                                                                       // For now, let linger_check_interval handle the next step.
    }
  }
}

pub(crate) async fn handle_actor_stopping_event(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  stopped_actor_id: usize,
  stopped_actor_type: ActorType,
  endpoint_uri_opt: Option<&str>,
  error_opt: Option<&ZmqError>,
) {
  let core_handle = core_arc.handle;
  
  // First, perform the resource cleanup regardless of the shutdown phase.
  // This removes the endpoint from the main map.
  // This function returns true if the cleanup might warrant a reconnect.
  let should_consider_reconnect = pipe_manager::cleanup_stopped_child_resources(
      core_arc.clone(),
      socket_logic_strong,
      stopped_actor_id,
      stopped_actor_type,
      endpoint_uri_opt,
      error_opt,
      false, // Assume not a full shutdown initially, we check phase below.
  ).await;

  // Now, acquire the coordinator lock to update the shutdown state.
  let mut coordinator = core_arc.shutdown_coordinator.lock().await;

  // We no longer need to check if the state is Running at the top. We handle all cases.
  match coordinator.state {
    ShutdownPhase::Running => {
      // Child stopped unexpectedly.
      tracing::warn!(
          handle = core_handle,
          child_id = stopped_actor_id,
          ?stopped_actor_type,
          uri = ?endpoint_uri_opt,
          "Child stopped unexpectedly while Core Running."
      );
      // Only reconnect if the cleanup indicated it was an outbound session that failed.
      if should_consider_reconnect {
          if let Some(uri) = endpoint_uri_opt {
              // We need to drop the lock before calling the async reconnect function
              let target_uri_to_reconnect = uri.to_string();
              drop(coordinator); // Release lock
              command_processor::respawn_connecter_actor(
                  core_arc.clone(),
                  socket_logic_strong.clone(),
                  target_uri_to_reconnect,
              )
              .await;
          }
      }
    }
    ShutdownPhase::StoppingChildren => {
      // This is the expected path during a normal shutdown.
      let mut was_last_pending = false;
      match stopped_actor_type {
        ActorType::Listener => {
          if coordinator.record_child_actor_stopped(stopped_actor_id, core_handle) {
            was_last_pending = true;
          }
        }
        ActorType::Session => {
          if coordinator.record_connection_closed(stopped_actor_id, core_handle) {
            was_last_pending = true;
          }
        }
        _ => { /* Other types aren't tracked by the coordinator's lists. */ }
      }

      // If this was the last pending entity, advance the state machine.
      if was_last_pending {
        tracing::debug!(
          handle = core_handle,
          "All children/connections now stopped. Moving to Lingering."
        );
        coordinator.state = ShutdownPhase::Lingering;
        let linger_opt = core_arc.core_state.read().options.linger;
        coordinator.start_linger_if_needed(linger_opt, core_handle);
        // The main loop's linger check will handle the rest.
      }
    }
    // If the event arrives while Lingering or later, it's a late arrival.
    // The resource cleanup was still important, but we don't need to touch the counter.
    ShutdownPhase::Lingering | ShutdownPhase::CleaningPipes | ShutdownPhase::Finished => {
      tracing::debug!(
          handle = core_handle,
          child_id = stopped_actor_id,
          "Received late ActorStopping event during phase {:?}. Cleanup already done.",
          coordinator.state
      );
    }
  }
}

pub(crate) async fn check_and_advance_linger(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
) -> Result<(), ZmqError> {
  let core_handle = core_arc.handle;
  let mut coordinator = core_arc.shutdown_coordinator.lock().await;

  if coordinator.state != ShutdownPhase::Lingering {
    return Ok(());
  }

  if coordinator.linger_deadline.is_none()
    && core_arc.core_state.read().options.linger != Some(Duration::ZERO)
  {
    let linger_opt = core_arc.core_state.read().options.linger;
    coordinator.start_linger_if_needed(linger_opt, core_handle);
  }

  if coordinator.is_linger_expired_or_queues_empty(&core_arc.core_state.read(), core_handle) {
    tracing::debug!(
      handle = core_handle,
      "Linger complete/queues empty. Advancing to CleaningPipes."
    );
    {
      let mut core_s_write = core_arc.core_state.write();
      advance_to_cleaning_phase(&mut coordinator, core_handle, &mut core_s_write);
    }
    #[cfg(feature = "inproc")]
    let pipes_to_clean = coordinator.inproc_connections_to_cleanup.clone();
    #[cfg(not(feature = "inproc"))]
    let pipes_to_clean = Vec::new();
    drop(coordinator);
    perform_final_pipe_cleanup(core_arc.clone(), socket_logic_strong, pipes_to_clean).await;
  }
  Ok(())
}

pub(crate) fn advance_to_cleaning_phase(
  coordinator: &mut ShutdownCoordinator,
  core_handle: usize,
  _core_s_write: &mut CoreState, // Mutable borrow, though not directly used for modification here
) {
  if coordinator.state == ShutdownPhase::Lingering {
    tracing::debug!(handle = core_handle, "Advancing to CleaningPipes state.");
    coordinator.state = ShutdownPhase::CleaningPipes;
  }
}

pub(crate) async fn perform_final_pipe_cleanup(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  #[cfg(feature = "inproc")] mut inproc_pipes_to_cleanup: Vec<(usize, usize, String)>,
  #[cfg(not(feature = "inproc"))] _inproc_pipes_to_cleanup: Vec<(usize, usize, String)>,
) {
  let core_handle = core_arc.handle;
  tracing::info!(
    handle = core_handle,
    "SocketCore performing final pipe and resource cleanup."
  );

  let (mut pipes_tx_map, mut reader_tasks_map) = {
    let mut core_s = core_arc.core_state.write();
    (
      std::mem::take(&mut core_s.pipes_tx),
      std::mem::take(&mut core_s.pipe_reader_task_handles),
    )
  };

  let _ = pipes_tx_map.drain();

  for (id, handle) in reader_tasks_map.drain() {
    handle.abort();
    tracing::trace!(handle = core_handle, pipe_id = id, "Aborted pipe reader.");
  }

  #[cfg(feature = "inproc")]
  {
    let mut detach_futs = Vec::new();
    for (_write_id, read_id, ref uri) in inproc_pipes_to_cleanup.drain(..) {
      tracing::debug!(handle=core_handle, pipe_read_id=read_id, %uri, "Notifying ISocket of inproc pipe detach.");
      let sl_clone = socket_logic_strong.clone();
      let r_id_clone = read_id;
      detach_futs.push(async move {
        sl_clone.pipe_detached(r_id_clone).await;
      });
    }
    if !detach_futs.is_empty() {
      futures::future::join_all(detach_futs).await;
    }
  }

  {
    core_arc
      .core_state
      .write()
      .pipe_read_id_to_endpoint_uri
      .clear();
    #[cfg(feature = "io-uring")]
    {
      for fd_to_unreg in core_arc.core_state.read().uring_fd_to_endpoint_uri.keys() {
        crate::uring::global_state::unregister_uring_fd_socket_core_mailbox(*fd_to_unreg);
      }
      core_arc.core_state.write().uring_fd_to_endpoint_uri.clear();
    }
    if !core_arc.core_state.read().endpoints.is_empty() {
      tracing::warn!(
        handle = core_handle,
        "Endpoints map not empty. Forcing clear. Rem: {}",
        core_arc.core_state.read().endpoints.len()
      );
      // Before clearing, ensure any remaining ISocketConnections are closed (best effort)
      let endpoints_to_force_close: Vec<Arc<dyn ISocketConnection>> = core_arc
        .core_state
        .read()
        .endpoints
        .values()
        .map(|ei| ei.connection_iface.clone())
        .collect();

      for iface in endpoints_to_force_close {
        let _ = iface.close_connection().await;
      }
      core_arc.core_state.write().endpoints.clear(); // Re-acquire and clear
    }
  }

  let mut coordinator = core_arc.shutdown_coordinator.lock().await;
  coordinator.state = ShutdownPhase::Finished;
  tracing::info!(
    handle = core_handle,
    "SocketCore final cleanup complete. Shutdown finished."
  );
}