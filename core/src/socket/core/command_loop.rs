// core/src/socket/core/command_loop.rs

use crate::error::ZmqError;
use crate::runtime::{actor_drop_guard, ActorDropGuard, ActorType, Command, MailboxReceiver, SystemEvent};
use crate::socket::core::state::ShutdownPhase; // To access ShutdownPhase directly
use crate::socket::core::{command_processor, event_processor, shutdown, SocketCore}; // For calling helpers
use crate::socket::ISocket;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::{interval, Interval};

/// The main actor loop for `SocketCore`.
/// It listens for user commands on its mailbox and system events on the event bus.
pub(crate) async fn run_command_loop(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: Arc<dyn ISocket>, // Strong Arc to the ISocket pattern logic
  command_receiver: MailboxReceiver,     // Mailbox for user commands, now mutable
  mut system_event_rx: broadcast::Receiver<SystemEvent>, // Event bus receiver
) {
  let core_handle = core_arc.handle;
  let context_clone_for_stop = core_arc.context.clone(); // For publishing ActorStopping
  let socket_type_for_log = core_arc.core_state.read().socket_type; // Get once for logging

  tracing::info!(
      handle = core_handle,
      socket_type = ?socket_type_for_log,
      "SocketCore actor main loop starting."
  );

  // Publish ActorStarted event after spawning
  let mut actor_drop_guard = ActorDropGuard::new(context_clone_for_stop.clone(), core_handle, ActorType::SocketCore, None, None);


  // Linger check interval
  let mut linger_check_interval: Interval = interval(Duration::from_millis(100)); // Adjusted interval
  linger_check_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

  let mut final_error_for_actorstop: Option<ZmqError> = None;

  // The main loop
  // The Result<(), ZmqError> is conceptual for the loop's success/failure.
  // If an unrecoverable error occurs, we'll set final_error_for_actorstop and break.
  let _loop_result: Result<(), ()> = async { // Changed to Result<(), ()> for loop structure
    loop {
      let current_shutdown_phase = {
        // Lock acquired and dropped quickly
        core_arc.shutdown_coordinator.lock().await.state // Access state directly
      };

      if current_shutdown_phase == ShutdownPhase::Finished {
        tracing::debug!(
          handle = core_handle,
          "SocketCore shutdown sequence fully finished. Breaking command loop."
        );
        break; // Exit the main loop
      }

      tokio::select! {
        biased; // Prioritize shutdown-related events or commands

        // --- 1. System Event Processing ---
        // Only listen to system events if not fully finished shutting down.
        event_result = system_event_rx.recv(), if current_shutdown_phase != ShutdownPhase::Finished => {
            match event_result {
                Ok(event) => {
                    // Delegate to event_processor module
                    if let Err(e) = event_processor::process_system_event(
                        core_arc.clone(),
                        &socket_logic_strong,
                        event,
                    ).await {
                        tracing::error!(handle = core_handle, "Fatal error processing system event: {}. Initiating shutdown.", e);
                        final_error_for_actorstop = Some(e);
                        // Ensure shutdown is initiated if not already
                        let coord = core_arc.shutdown_coordinator.lock().await;
                        if coord.state == ShutdownPhase::Running {
                            drop(coord); // Release before async call
                            shutdown::initiate_core_shutdown(core_arc.clone(), &socket_logic_strong, true).await;
                        }

                        // Loop will break due to ShutdownPhase::Finished or next error check
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(handle = core_handle, skipped = n, "System event bus lagged for SocketCore!");
                    // Potentially treat as critical error and initiate shutdown if not already underway
                    if current_shutdown_phase == ShutdownPhase::Running {
                        final_error_for_actorstop = Some(ZmqError::Internal("SocketCore: Event bus lagged".into()));
                        shutdown::initiate_core_shutdown(core_arc.clone(), &socket_logic_strong, true).await;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    tracing::error!(handle = core_handle, "System event bus closed unexpectedly!");
                    final_error_for_actorstop = Some(ZmqError::Internal("SocketCore: Event bus closed".into()));
                    if current_shutdown_phase == ShutdownPhase::Running {
                        shutdown::initiate_core_shutdown(core_arc.clone(), &socket_logic_strong, true).await;
                    }
                    // Loop will break due to ShutdownPhase::Finished or next error check
                }
            }
        }

        // --- 2. User Command Processing ---
        // Only process commands if not fully finished.
        cmd_result = command_receiver.recv(), if current_shutdown_phase != ShutdownPhase::Finished => {
            match cmd_result {
                Ok(command) => {
                    // Delegate to command_processor module
                    if let Err(e) = command_processor::process_socket_command(
                        core_arc.clone(),
                        &socket_logic_strong,
                        command,
                    ).await {
                        tracing::error!(handle = core_handle, "Fatal error processing command: {}. Initiating shutdown.", e);
                        final_error_for_actorstop = Some(e);
                        let coord = core_arc.shutdown_coordinator.lock().await;
                        if coord.state == ShutdownPhase::Running {
                            drop(coord);
                            shutdown::initiate_core_shutdown(core_arc.clone(), &socket_logic_strong, true).await;
                        }
                    }
                }
                Err(_) => { // Command mailbox closed
                    tracing::info!(handle = core_handle, "SocketCore command mailbox closed. Initiating shutdown.");
                    final_error_for_actorstop = Some(ZmqError::Internal("SocketCore: Command mailbox closed".into()));
                    let coord = core_arc.shutdown_coordinator.lock().await;
                    if coord.state == ShutdownPhase::Running {
                        drop(coord);
                        shutdown::initiate_core_shutdown(core_arc.clone(), &socket_logic_strong, true).await;
                    }
                    // Loop will break due to ShutdownPhase::Finished or next error check
                }
            }
        }

        // --- 3. Linger Check (Timer) ---
        // Only tick if in Lingering phase.
        _ = linger_check_interval.tick(), if current_shutdown_phase == ShutdownPhase::Lingering => {
            // Delegate to shutdown module
            if let Err(e) = shutdown::check_and_advance_linger(
                core_arc.clone(),
                &socket_logic_strong,
            ).await {
                tracing::error!(handle = core_handle, "Error during linger check: {}. Forcing cleanup.", e);
                final_error_for_actorstop = final_error_for_actorstop.clone().or(Some(e));
                // Force advance to cleaning phase if linger check itself errors badly
                let mut coord = core_arc.shutdown_coordinator.lock().await;
                if coord.state == ShutdownPhase::Lingering { // Re-check state
                    {
                        // Pass a mutable CoreState guard
                        let mut core_state_write_guard = core_arc.core_state.write();
                        shutdown::advance_to_cleaning_phase(&mut coord, core_handle, &mut core_state_write_guard);
                    }
                    // Drop guards before async call
                    #[cfg(feature = "inproc")]
                    let pipes_to_clean_val = coord.inproc_connections_to_cleanup.clone();
                    #[cfg(not(feature = "inproc"))]
                    let pipes_to_clean_val = Vec::new(); // Dummy for non-inproc
                    drop(coord);

                    shutdown::perform_final_pipe_cleanup(core_arc.clone(), &socket_logic_strong, pipes_to_clean_val).await;

                } else {
                    drop(coord); // Still need to drop if not advancing
                }
            }
        }
      } // end tokio::select!
    } // end loop
    Ok(()) // Loop finished (normally implies shutdown complete)
  }.await;

  // --- Post-Loop Cleanup & ActorStopping Event ---
  // This section runs after the main loop breaks (due to shutdown or fatal error).
  tracing::info!(
      handle = core_handle,
      socket_type = ?socket_type_for_log,
      "SocketCore main loop finished execution."
  );

  // If loop exited due to an error that wasn't already part of a graceful shutdown,
  // ensure shutdown is initiated and as much cleanup as possible happens.
  // This is more of a failsafe.
  let mut final_coord_guard = core_arc.shutdown_coordinator.lock().await;
  if final_coord_guard.state != ShutdownPhase::Finished {
    tracing::warn!(
        handle = core_handle,
        current_phase = ?final_coord_guard.state,
        "SocketCore loop exited prematurely or shutdown not fully completed. Attempting final cleanup."
    );
    // Attempt to run the final cleanup stages if not already done.
    // This is tricky because we are outside the select loop.
    // For simplicity, we'll assume that if the loop broke, final_coord_guard.state
    // should ideally be ShutdownPhase::Finished. If not, it might indicate an
    // unhandled error path within the loop.
    // A robust approach here might involve re-running parts of the shutdown sequence
    // if they weren't completed, but that adds complexity.
    // For now, we ensure we move to Finished state.
    final_coord_guard.state = ShutdownPhase::Finished;
  }
  drop(final_coord_guard);

  // Unregister this socket from the context (if it was registered)
  // This needs to be done carefully if context itself is shutting down.
  // ContextInner::unregister_socket should be robust.
  core_arc.context.inner().unregister_socket(core_handle);

  // Unregister any bound inproc names.
  #[cfg(feature = "inproc")]
  {
    // Take the names to avoid holding lock during async operations
    let bound_names_to_unregister = {
      let mut core_s_guard = core_arc.core_state.write();
      std::mem::take(&mut core_s_guard.bound_inproc_names)
    };
    if !bound_names_to_unregister.is_empty() {
      tracing::debug!(
        handle = core_handle,
        "SocketCore unregistering inproc names: {:?}",
        bound_names_to_unregister
      );
      for name_val in bound_names_to_unregister {
        // ContextInner::unregister_inproc is synchronous.
        core_arc.context.inner().unregister_inproc(&name_val);
      }
    }
  }

  // Publish the final ActorStopping event for this SocketCore.
  if let Some(err) = final_error_for_actorstop {
    actor_drop_guard.set_error(err);
  } else {
    actor_drop_guard.waive();
  }

  tracing::info!(
      handle = core_handle,
      socket_type = ?socket_type_for_log,
      "SocketCore actor task FULLY STOPPED."
  );
}
