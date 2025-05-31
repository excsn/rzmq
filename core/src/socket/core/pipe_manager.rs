// core/src/socket/core/pipe_manager.rs

use crate::context::Context as RzmqContext; // Alias to avoid clash if needed
use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::{ActorDropGuard, ActorType, Command, MailboxSender};
use crate::runtime::OneShotSender as RuntimeOneShotSender;
use crate::socket::core::state::{EndpointInfo, EndpointType};
use crate::socket::core::{command_processor, SocketCore};
use crate::socket::{ISocket, SocketEvent};

use async_channel::{bounded, Receiver as AsyncReceiver, SendError, Sender as AsyncSender, TrySendError};
use tokio::time::timeout;
use std::os::fd::RawFd;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Sets up the data pipes between SocketCore and a newly established SessionBase actor.
///
/// This function:
/// 1. Creates two bounded async_channels (one for Core->Session, one for Session->Core).
/// 2. Stores SocketCore's sending end (`tx_core_to_session`) in `core_state.pipes_tx`.
/// 3. Spawns a `run_pipe_reader_task` to consume messages from Session on `rx_core_from_session`
///    and stores its `JoinHandle` in `core_state.pipe_reader_task_handles`.
/// 4. Sends a `Command::AttachPipe` to the SessionBase actor, providing its ends of the channels.
///
/// Returns `Ok((core_write_id, core_read_id))` on success, or `Err(ZmqError)` if `AttachPipe` fails.
pub(crate) async fn setup_pipe_with_session_actor(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: Arc<dyn ISocket>, // For the PipeReaderTask
    session_actor_mailbox: &MailboxSender, // Mailbox of the SessionBase actor
    session_actor_handle_id: usize,      // Handle ID of the SessionBase actor
    endpoint_uri_for_logging: &str,      // For logging context
) -> Result<(usize, usize), ZmqError> {
    let core_handle = core_arc.handle;
    tracing::debug!(
        core_handle = core_handle,
        session_actor_id = session_actor_handle_id,
        conn_uri = %endpoint_uri_for_logging,
        "Setting up data pipes with SessionBase actor."
    );

    // Determine HWM for the pipes from SocketCore's options
    let pipe_hwm = {
        let core_s_guard = core_arc.core_state.read();
        core_s_guard.options.sndhwm.max(core_s_guard.options.rcvhwm).max(1)
    };

    // Create unique pipe IDs from SocketCore's perspective
    let core_write_id = core_arc.context.inner().next_handle(); // ID Core uses to write to session
    let core_read_id = core_arc.context.inner().next_handle();  // ID Core uses to read from session

    // Create pipes:
    // Pipe 1: SocketCore -> SessionBase (Core writes, Session reads)
    let (tx_core_to_session, rx_session_from_core) = bounded::<Msg>(pipe_hwm);
    // Pipe 2: SessionBase -> SocketCore (Session writes, Core reads)
    let (tx_session_to_core, rx_core_from_session) = bounded::<Msg>(pipe_hwm);

    // --- Store SocketCore's ends of the pipes and spawn its reader task ---
    {
        // Scope for write lock on core_state
        let mut core_s_guard = core_arc.core_state.write();

        // Store the sender for Core -> Session data
        core_s_guard.pipes_tx.insert(core_write_id, tx_core_to_session.clone()); // Clone for storage

        // Spawn the PipeReaderTask for Session -> Core data
        let pipe_reader_task_join_handle = tokio::spawn(run_pipe_reader_task(
            core_arc.context.clone(),
            core_handle,
            core_arc.command_sender(), // SocketCore's command mailbox
            socket_logic_strong.clone(), // ISocket logic for handling incoming messages
            core_read_id,              // The ID this reader task is responsible for
            rx_core_from_session,      // The receiving end of the Session -> Core pipe
        ));
        core_s_guard.pipe_reader_task_handles.insert(core_read_id, pipe_reader_task_join_handle);
    } // CoreState write lock released

    // --- Send AttachPipe command to the SessionBase actor ---
    // The SessionBase actor will use these IDs to identify its communication channels with SocketCore.
    // - Session reads from `rx_session_from_core` (which corresponds to `core_write_id` from Core's view).
    // - Session writes to `tx_session_to_core` (which corresponds to `core_read_id` from Core's view).
    let attach_pipe_cmd = Command::AttachPipe {
        rx_from_core: rx_session_from_core, // Session will read on this
        tx_to_core: tx_session_to_core,     // Session will send on this
        pipe_read_id: core_write_id,        // Session's "read ID" is Core's "write ID"
        pipe_write_id: core_read_id,        // Session's "write ID" is Core's "read ID"
    };

    if session_actor_mailbox.send(attach_pipe_cmd).await.is_err() {
        tracing::error!(
            core_handle = core_handle,
            session_actor_id = session_actor_handle_id,
            conn_uri = %endpoint_uri_for_logging,
            "Failed to send AttachPipe command to SessionBase actor. Cleaning up SocketCore pipe state."
        );
        // If AttachPipe fails, the session won't use these pipes. Clean them up on SocketCore's side.
        {
            let mut core_s_guard = core_arc.core_state.write();
            core_s_guard.remove_pipe_state(core_write_id, core_read_id);
            // The PipeReaderTask was spawned; its JoinHandle was in pipe_reader_task_handles
            // and remove_pipe_state would have aborted it.
        }
        return Err(ZmqError::Internal(format!(
            "Failed to send AttachPipe to Session actor {}",
            session_actor_handle_id
        )));
    }

    tracing::debug!(
        core_handle = core_handle,
        session_actor_id = session_actor_handle_id,
        conn_uri = %endpoint_uri_for_logging,
        core_write_pipe_id = core_write_id,
        core_read_pipe_id = core_read_id,
        "Successfully sent AttachPipe to SessionBase. Pipes established."
    );

    Ok((core_write_id, core_read_id))
}


/// Task that reads messages from a single pipe (e.g., from a Session) and forwards
/// them to the SocketCore's command mailbox as `Command::PipeMessageReceived` or
/// signals `Command::PipeClosedByPeer`.
pub(crate) async fn run_pipe_reader_task(
    context: RzmqContext, // The rzmq Context for publishing ActorStarted/Stopping
    core_handle: usize, // Handle of the parent SocketCore
    core_command_mailbox: MailboxSender, // Mailbox of the parent SocketCore
    socket_logic_strong: Arc<dyn ISocket>, // ISocket logic to call handle_pipe_event
    pipe_read_id: usize, // The ID SocketCore uses to identify this pipe for reading
    pipe_receiver: AsyncReceiver<Msg>, // The actual channel receiver for this pipe
) {
    let pipe_reader_task_handle_id = context.inner().next_handle();
    let pipe_reader_actor_type = ActorType::PipeReader;

    // Publish ActorStarted for this PipeReaderTask
    context.publish_actor_started(
        pipe_reader_task_handle_id,
        pipe_reader_actor_type,
        Some(core_handle), // Parent is the SocketCore
    );

    // Setup drop guard for abnormal termination
    let drop_guard = ActorDropGuard::new(
        context.clone(),
        pipe_reader_task_handle_id,
        pipe_reader_actor_type,
        None, // PipeReaderTasks don't have a specific endpoint URI
    );

    tracing::debug!(
        core_handle = core_handle,
        pipe_read_id = pipe_read_id,
        pipe_reader_task_id = pipe_reader_task_handle_id,
        "PipeReaderTask started."
    );

    let mut final_error_for_stopping: Option<ZmqError> = None;

    loop {
        // Before attempting to receive, check if the parent SocketCore is still alive.
        // If its command mailbox is closed, it means SocketCore has terminated.
        if core_command_mailbox.is_closed() {
            tracing::info!(
                core_handle = core_handle,
                pipe_reader_task_id = pipe_reader_task_handle_id,
                pipe_read_id = pipe_read_id,
                "PipeReaderTask: Parent SocketCore's command mailbox closed. Stopping."
            );
            if final_error_for_stopping.is_none() {
                final_error_for_stopping = Some(ZmqError::Internal(
                    "PipeReaderTask: Parent SocketCore terminated".into(),
                ));
            }
            break;
        }

        // For PipeReaderTask, the primary "event" is a message on its pipe_receiver.
        // We don't need a complex select! here unless we add more explicit shutdown signals
        // directly to PipeReaderTasks (beyond checking core_command_mailbox.is_closed()).
        match pipe_receiver.recv().await {
            Ok(msg) => {
                // A message was received from the pipe (e.g., from a Session).
                // Instead of sending a Command to SocketCore's mailbox,
                // directly call the ISocket::handle_pipe_event method.
                let cmd_for_isocket = Command::PipeMessageReceived { pipe_id: pipe_read_id, msg };
                if let Err(e) = socket_logic_strong.handle_pipe_event(pipe_read_id, cmd_for_isocket).await {
                    tracing::error!(
                        core_handle = core_handle,
                        pipe_reader_task_id = pipe_reader_task_handle_id,
                        pipe_read_id = pipe_read_id,
                        "PipeReaderTask: Error from ISocket::handle_pipe_event: {}. Stopping this reader task.", e
                    );
                    if final_error_for_stopping.is_none() {
                        final_error_for_stopping = Some(e);
                    }
                    break; // Stop this PipeReaderTask on ISocket error
                }
                // Message successfully processed by ISocket logic via handle_pipe_event.
            }
            Err(_) => {
                // Pipe closed by the sender (e.g., SessionBase terminated).
                tracing::debug!(
                    core_handle = core_handle,
                    pipe_reader_task_id = pipe_reader_task_handle_id,
                    pipe_read_id = pipe_read_id,
                    "PipeReaderTask: Data pipe closed by peer. Notifying SocketCore."
                );
                // Construct the PipeClosedByPeer command for ISocket
                let cmd_closed_for_isocket = Command::PipeClosedByPeer { pipe_id: pipe_read_id };
                // Call ISocket::handle_pipe_event with this command
                if let Err(e) = socket_logic_strong.handle_pipe_event(pipe_read_id, cmd_closed_for_isocket).await {
                     tracing::warn!(
                        core_handle = core_handle,
                        pipe_reader_task_id = pipe_reader_task_handle_id,
                        pipe_read_id = pipe_read_id,
                        "PipeReaderTask: Error from ISocket::handle_pipe_event for PipeClosedByPeer: {}.", e
                    );
                    if final_error_for_stopping.is_none() {
                        final_error_for_stopping = Some(e);
                    }
                }
                // Even if handle_pipe_event errors, the pipe is closed, so this task must stop.
                break;
            }
        }
    }

    tracing::debug!(
        core_handle = core_handle,
        pipe_read_id = pipe_read_id,
        pipe_reader_task_id = pipe_reader_task_handle_id,
        "PipeReaderTask finished."
    );

    drop_guard.waive(); // Signal normal termination
    context.publish_actor_stopping(
        pipe_reader_task_handle_id,
        pipe_reader_actor_type,
        None, // No specific endpoint URI for a pipe reader
        final_error_for_stopping,
    );
}

/// Cleans up resources associated with a child actor (Session, Listener, Connecter) that has stopped.
/// This is typically called when an ActorStopping event is received for a child,
/// or if a child stops unexpectedly.
///
/// If `is_full_core_shutdown` is true, it implies SocketCore itself is shutting down,
/// so some actions like trying to respawn a Connecter might be skipped.
pub(crate) async fn cleanup_stopped_child_resources(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    stopped_child_actor_id: usize, // The handle_id of the actor that stopped
    stopped_child_actor_type: ActorType,
    endpoint_uri_opt: Option<&str>, // URI associated with the stopped actor
    error_opt: Option<&ZmqError>,   // Error if the child stopped due to one
    is_full_core_shutdown: bool,    // Is SocketCore itself shutting down?
) {
    let core_handle = core_arc.handle;
    tracing::debug!(
        parent_core_handle = core_handle,
        stopped_child_id = stopped_child_actor_id,
        ?stopped_child_actor_type,
        uri = ?endpoint_uri_opt,
        error = ?error_opt,
        full_core_shutdown = is_full_core_shutdown,
        "Cleaning up resources for stopped child actor."
    );

    let mut removed_endpoint_info: Option<EndpointInfo> = None;
    let mut detached_pipe_read_id: Option<usize> = None;

    // 1. Find and remove the EndpointInfo for the stopped child.
    //    We need to iterate because endpoint_uri_opt might be None or ambiguous,
    //    and stopped_child_actor_id is the most reliable key for actors SocketCore tracks.
    if let Some(uri_key_to_remove) = endpoint_uri_opt {
        // Attempt removal by URI first if provided and matches handle_id
        let mut core_s_write = core_arc.core_state.write();
        if let Some(ep_info) = core_s_write.endpoints.get(uri_key_to_remove) {
            if ep_info.handle_id == stopped_child_actor_id {
                removed_endpoint_info = core_s_write.endpoints.remove(uri_key_to_remove);
                tracing::debug!(handle=core_handle, child_id=stopped_child_actor_id, uri=%uri_key_to_remove, "Removed EndpointInfo by URI for stopped child.");
            } else {
                // URI found, but handle_id mismatch. Fallback to iterating.
                tracing::warn!(handle=core_handle, child_id=stopped_child_actor_id, uri=%uri_key_to_remove, "URI found but handle_id mismatch. Will iterate.");
            }
        }
    }

    if removed_endpoint_info.is_none() {
        // Fallback: Iterate to find by handle_id if URI-based removal failed or URI wasn't provided.
        let mut core_s_write = core_arc.core_state.write();
        let mut key_of_endpoint_to_remove: Option<String> = None;
        for (uri, info) in core_s_write.endpoints.iter() {
            if info.handle_id == stopped_child_actor_id {
                key_of_endpoint_to_remove = Some(uri.clone());
                break;
            }
        }
        if let Some(key) = key_of_endpoint_to_remove {
            removed_endpoint_info = core_s_write.endpoints.remove(&key);
            tracing::debug!(handle=core_handle, child_id=stopped_child_actor_id, uri=%key, "Removed EndpointInfo by iteration for stopped child.");
        }
    }


    // 2. If EndpointInfo was found and removed:
    if let Some(ep_info) = &removed_endpoint_info {
        // a. Abort its task_handle (if any - Listeners/Connecters)
        //    Session task handles are not managed directly by SocketCore's EndpointInfo.
        if let Some(task_handle) = &ep_info.task_handle { // task_handle is Option
            if !task_handle.is_finished() {
                task_handle.abort();
                tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Aborted task_handle for stopped child.");
            }
        }

        // b. Clean up pipe state (if it was a session-like endpoint with pipes)
        if let Some((core_write_id, core_read_id)) = ep_info.pipe_ids {
            core_arc.core_state.write().remove_pipe_state(core_write_id, core_read_id);
            detached_pipe_read_id = Some(core_read_id); // For ISocket::pipe_detached
            tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, uri=%ep_info.endpoint_uri, "Removed pipe state for stopped child.");

            // If it was an io_uring FD, unregister it from global state
            #[cfg(feature = "io-uring")]
            if ep_info.connection_iface.as_any().is::<crate::socket::connection_iface::UringFdConnection>() {
                let fd = ep_info.handle_id as std::os::unix::io::RawFd; // handle_id is RawFd for Uring
                core_arc.core_state.write().uring_fd_to_endpoint_uri.remove(&fd);
                crate::runtime::global_uring_state::unregister_uring_fd_socket_core_mailbox(fd);
                tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, %fd, "Unregistered UringFD state for stopped child.");
            }
        }

        // c. Emit monitor event (Disconnected for Session, Closed for Listener)
        let monitor_event = match ep_info.endpoint_type {
            EndpointType::Session => SocketEvent::Disconnected { endpoint: ep_info.endpoint_uri.clone() },
            EndpointType::Listener => SocketEvent::Closed { endpoint: ep_info.endpoint_uri.clone() },
        };
        core_arc.core_state.read().send_monitor_event(monitor_event);

    } else {
        tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, ?stopped_child_actor_type, "No EndpointInfo found to remove for stopped child (might be PipeReader or already cleaned up).");
    }

    // 3. Notify ISocket logic about pipe detachment if applicable
    if let Some(read_id) = detached_pipe_read_id {
        socket_logic_strong.pipe_detached(read_id).await;
        tracing::debug!(handle = core_handle, child_id = stopped_child_actor_id, pipe_read_id = read_id, "Notified ISocket of pipe detachment.");
    }

    // 4. Potentially respawn Connecter if child was an outbound session and died unexpectedly
    if !is_full_core_shutdown && // Only if SocketCore itself is not shutting down
       error_opt.is_some() && // Only if the child stopped due to an error
       matches!(stopped_child_actor_type, ActorType::Session | ActorType::Engine) // Engine error implies Session failure
    {
        if let Some(ep_info) = removed_endpoint_info { // Use the info we removed
            if ep_info.endpoint_type == EndpointType::Session && ep_info.is_outbound_connection {
                if let Some(target_uri_to_reconnect) = ep_info.target_endpoint_uri {
                    if !target_uri_to_reconnect.starts_with("inproc://") {
                        let reconnect_enabled = core_arc.core_state.read().options.reconnect_ivl.map_or(false, |d| !d.is_zero());
                        let is_fatal_for_reconnect = error_opt.map_or(false, crate::transport::tcp::is_fatal_connect_error);

                        if reconnect_enabled && !is_fatal_for_reconnect {
                            tracing::info!(
                                handle = core_handle,
                                target_uri = %target_uri_to_reconnect,
                                "Unexpected session/engine stop for outbound connection. Initiating reconnect..."
                            );
                            // Delegate respawning to command_processor or a common helper
                            command_processor::respawn_connecter_actor(core_arc.clone(), target_uri_to_reconnect).await;
                        } else {
                             tracing::warn!(
                                handle = core_handle,
                                target_uri = %target_uri_to_reconnect,
                                reconnect_enabled,
                                is_fatal_for_reconnect,
                                "Reconnect SKIPPED for stopped outbound session."
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Cleans up resources for a specific connection identified by its endpoint URI.
/// This is typically called when a UserDisconnect command is processed for a non-inproc session.
/// It initiates closure of the connection and removes state from SocketCore.
///
/// The `_expected_handle_id_if_known` can be used as an assertion if the caller has it,
/// to ensure we're cleaning up the intended connection if multiple endpoints somehow share a URI (unlikely).
/// For now, we'll primarily rely on the URI.
pub(crate) async fn cleanup_session_state_by_uri(
    core_arc: Arc<SocketCore>,
    endpoint_uri: &str,
    socket_logic_strong: &Arc<dyn ISocket>,
    _expected_handle_id_if_known: Option<usize>, // Optional: for assertion/confirmation
) -> Option<EndpointInfo> { // Returns the removed EndpointInfo if found
    let core_handle = core_arc.handle;
    tracing::debug!(
        parent_core_handle = core_handle,
        uri_to_cleanup = %endpoint_uri,
        "Attempting to cleanup session state by URI."
    );

    let mut removed_endpoint_info: Option<EndpointInfo> = None;
    let mut detached_pipe_read_id: Option<usize> = None;

    // 1. Find and remove the EndpointInfo by URI.
    // This must be done under a write lock to prevent races.
    {
        let mut core_s_write = core_arc.core_state.write();
        if let Some(ep_info_to_remove) = core_s_write.endpoints.remove(endpoint_uri) {
            // Assert that we are removing the correct type of endpoint (Session)
            if ep_info_to_remove.endpoint_type != EndpointType::Session {
                tracing::error!(
                    handle = core_handle,
                    uri = %endpoint_uri,
                    actual_type = ?ep_info_to_remove.endpoint_type,
                    "Cleanup by URI: Expected Session type, found {:?}. Reinserting and erroring.",
                    ep_info_to_remove.endpoint_type
                );
                // Put it back if it's not a session, this call was misused.
                core_s_write.endpoints.insert(endpoint_uri.to_string(), ep_info_to_remove);
                // This case should ideally not happen if UserDisconnect logic is correct.
                return None; // Or return an error
            }

            tracing::info!(
                handle = core_handle,
                uri = %endpoint_uri,
                child_id = ep_info_to_remove.handle_id, // Session actor handle or RawFd
                "Removed EndpointInfo by URI during proactive cleanup."
            );

            // a. Clean up pipe state (if it was a session-like endpoint with pipes)
            if let Some((core_write_id, core_read_id)) = ep_info_to_remove.pipe_ids {
                // remove_pipe_state also removes from pipe_read_id_to_endpoint_uri
                core_s_write.remove_pipe_state(core_write_id, core_read_id);
                detached_pipe_read_id = Some(core_read_id);
                tracing::debug!(handle = core_handle, uri=%endpoint_uri, "Removed pipe state for proactively cleaned up session.");
            }

            // b. If it was an io_uring FD, unregister it from global state
            #[cfg(feature = "io-uring")]
            if ep_info_to_remove.connection_iface.as_any().is::<crate::socket::connection_iface::UringFdConnection>() {
                let fd = ep_info_to_remove.handle_id as RawFd;
                core_s_write.uring_fd_to_endpoint_uri.remove(&fd);
                crate::runtime::global_uring_state::unregister_uring_fd_socket_core_mailbox(fd);
                tracing::debug!(handle = core_handle, uri=%endpoint_uri, %fd, "Unregistered UringFD state for proactively cleaned up session.");
            }

            // c. Emit Disconnected monitor event
            core_s_write.send_monitor_event(SocketEvent::Disconnected { endpoint: endpoint_uri.to_string() });

            removed_endpoint_info = Some(ep_info_to_remove); // Store the removed info
        } else {
            tracing::warn!(handle = core_handle, uri=%endpoint_uri, "Cleanup by URI: EndpointInfo not found (already removed or connect failed?).");
            return None; // Nothing to do if not found
        }
    } // Write lock on core_state released

    // 2. If EndpointInfo was successfully found and removed:
    if let Some(ref ep_info) = removed_endpoint_info {
        // a. Initiate close on the connection interface.
        //    This will tell SessionBase to stop or UringWorker to close the FD.
        tracing::debug!(
            handle = core_handle,
            uri = %ep_info.endpoint_uri,
            conn_id = ep_info.handle_id,
            "Cleanup by URI: Calling close_connection() on ISocketConnection."
        );
        if let Err(e) = ep_info.connection_iface.close_connection().await {
            tracing::warn!(
                handle = core_handle,
                uri = %ep_info.endpoint_uri,
                conn_id = ep_info.handle_id,
                "Error calling close_connection() during cleanup by URI: {}", e
            );
        }

        // b. Abort its task_handle (this is None for Sessions, but cleanup is generic)
        if let Some(task_handle) = &ep_info.task_handle {
            if !task_handle.is_finished() { // Check if it's Some and not finished
                task_handle.abort();
                tracing::debug!(handle = core_handle, uri=%ep_info.endpoint_uri, "Aborted task_handle during cleanup by URI (if applicable).");
            }
        }

        // c. Notify ISocket logic about pipe detachment if applicable
        if let Some(read_id) = detached_pipe_read_id {
            socket_logic_strong.pipe_detached(read_id).await;
            tracing::debug!(handle = core_handle, uri=%ep_info.endpoint_uri, pipe_read_id = read_id, "Notified ISocket of pipe detachment during cleanup by URI.");
        }
    }

    removed_endpoint_info // Return the removed info, caller might use it (e.g., for logging)
}

pub(crate) async fn cleanup_session_state_by_pipe(
    core_arc: Arc<SocketCore>,
    pipe_read_id: usize, // This is Core's read_id (actual or synthetic)
    socket_logic_strong: &Arc<dyn ISocket>,
) -> Option<String> { // Returns Option<target_uri> for potential reconnect
    let core_handle = core_arc.handle;
    tracing::debug!(handle=core_handle, pipe_read_id, "Cleaning up state by pipe_read_id.");
    
    let mut endpoint_uri_to_remove: Option<String> = None;
    // Store the fields we need from EndpointInfo before it's removed
    let mut target_uri_for_reconnect: Option<String> = None;
    let mut removed_ep_type: Option<EndpointType> = None;
    let mut removed_ep_uri_for_event: Option<String> = None;
    let mut task_handle_to_abort: Option<JoinHandle<()>> = None;
    let mut pipe_ids_of_removed: Option<(usize, usize)> = None;
    #[cfg(feature = "io-uring")]
    let mut fd_to_unregister_opt: Option<RawFd> = None;

    // 1. Find the endpoint_uri associated with this pipe_read_id
    {
        let core_s_read = core_arc.core_state.read();
        if let Some(uri) = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id) {
            endpoint_uri_to_remove = Some(uri.clone());
            // If we found the URI, also try to get details from the main endpoints map
            if let Some(ep_info) = core_s_read.endpoints.get(uri) {
                target_uri_for_reconnect = ep_info.target_endpoint_uri.clone();
                removed_ep_type = Some(ep_info.endpoint_type);
                removed_ep_uri_for_event = Some(ep_info.endpoint_uri.clone());
                // task_handle is Option<JoinHandle>, JoinHandle is not Clone.
                // We can't clone it here. We'll take it when removing the EndpointInfo.
                pipe_ids_of_removed = ep_info.pipe_ids;

                #[cfg(feature = "io-uring")]
                if ep_info.connection_iface.as_any().is::<crate::socket::connection_iface::UringFdConnection>() {
                    fd_to_unregister_opt = Some(ep_info.handle_id as RawFd);
                }
            }
        }
    }

    // 2. If URI found, remove the EndpointInfo and associated pipe state
    if let Some(ref uri_key) = endpoint_uri_to_remove {
        let mut core_s_write = core_arc.core_state.write();
        if let Some(removed_ep_info_struct) = core_s_write.endpoints.remove(uri_key) {
            tracing::info!(handle=core_handle, uri=%uri_key, pipe_read_id, "Removed EndpointInfo during cleanup_session_state_by_pipe.");
            
            // Now that we have ownership of removed_ep_info_struct, we can take its JoinHandle
            task_handle_to_abort = removed_ep_info_struct.task_handle; 
            // If we hadn't captured these above, we could get them from removed_ep_info_struct here
            // target_uri_for_reconnect = removed_ep_info_struct.target_endpoint_uri;
            // removed_ep_type = Some(removed_ep_info_struct.endpoint_type);
            // etc.

            // Remove pipe state (writer and reader task handle for this pipe_read_id)
            // This also removes from pipe_read_id_to_endpoint_uri map.
            if let Some((write_id, _read_id_from_epinfo)) = pipe_ids_of_removed {
                // _read_id_from_epinfo should match `pipe_read_id`
                core_s_write.remove_pipe_state(write_id, pipe_read_id);
            }

            // If it was an io_uring FD, unregister from global state and local map
            #[cfg(feature = "io-uring")]
            if let Some(fd_to_unregister) = fd_to_unregister_opt { // Use the captured FD
                core_s_write.uring_fd_to_endpoint_uri.remove(&fd_to_unregister);
                crate::runtime::global_uring_state::unregister_uring_fd_socket_core_mailbox(fd_to_unregister);
            }

            // Emit monitor event (using captured URI and type)
            if let (Some(ep_type), Some(ep_uri_event)) = (removed_ep_type, removed_ep_uri_for_event) {
                let event = match ep_type {
                    EndpointType::Session => SocketEvent::Disconnected { endpoint: ep_uri_event },
                    EndpointType::Listener => SocketEvent::Closed { endpoint: ep_uri_event },
                };
                core_s_write.send_monitor_event(event);
            }

        } else {
            tracing::warn!(handle=core_handle, pipe_read_id, "cleanup_session_state_by_pipe: EndpointInfo for URI '{}' not found during write, though pipe_read_id mapping existed.", uri_key);
        }
    } else {
        tracing::warn!(handle=core_handle, pipe_read_id, "cleanup_session_state_by_pipe: No endpoint_uri found for pipe_read_id. Might have been cleaned up already.");
        // Still try to remove from pipe_reader_task_handles if it was just a reader task without full endpoint.
        core_arc.core_state.write().pipe_reader_task_handles.remove(&pipe_read_id).map(|h| h.abort());
    }

    // Abort associated task_handle if it was captured
    if let Some(th) = task_handle_to_abort {
        if removed_ep_type == Some(EndpointType::Listener) { // Only abort if it was for a Listener we manage
            th.abort();
        }
    }

    // 3. Notify ISocket logic
    socket_logic_strong.pipe_detached(pipe_read_id).await;

    // 4. Return target URI for potential reconnect
    target_uri_for_reconnect
}


#[cfg(feature = "inproc")]
pub(crate) async fn process_inproc_binding_request_event(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    connector_uri: String,
    pipe_rx_for_binder_to_receive_from_connector: AsyncReceiver<Msg>,
    pipe_tx_for_binder_to_send_to_connector: AsyncSender<Msg>,
    binder_write_id_for_this_connection: usize, // Binder uses this ID to write to connector
    binder_read_id_for_this_connection: usize,  // Binder uses this ID to read from connector
    reply_tx_to_connector: RuntimeOneShotSender, // Renamed for clarity
) -> Result<(), ZmqError> {
    use crate::socket::{core::state::{EndpointInfo, EndpointType}, SocketEvent};

    let binder_core_handle = core_arc.handle;
    tracing::debug!(
        binder_handle = binder_core_handle,
        connector_uri = %connector_uri,
        binder_write_pipe_id = binder_write_id_for_this_connection,
        binder_read_pipe_id = binder_read_id_for_this_connection,
        "SocketCore (binder) processing InprocBindingRequest event."
    );

    // For now, binder always accepts.
    let accept_result: Result<(), ZmqError> = Ok(());

    if accept_result.is_ok() {
        // Spawn PipeReaderTask for the binder to read messages from the connector.
        let pipe_reader_task = tokio::spawn(run_pipe_reader_task(
            core_arc.context.clone(),
            binder_core_handle,
            core_arc.command_sender(),
            socket_logic_strong.clone(),
            binder_read_id_for_this_connection,
            pipe_rx_for_binder_to_receive_from_connector,
        ));

        let inproc_endpoint_entry_handle_id = core_arc.context.inner().next_handle();
        let endpoint_info_for_binder = EndpointInfo {
            mailbox: core_arc.command_sender(), // Placeholder, not a child actor
            task_handle: None, // No separate task for SocketCore to manage for this inproc connection itself
            endpoint_type: EndpointType::Session, // Treat as a session
            endpoint_uri: connector_uri.clone(),
            pipe_ids: Some((binder_write_id_for_this_connection, binder_read_id_for_this_connection)),
            handle_id: inproc_endpoint_entry_handle_id, // Unique ID for this binder's view of the connection
            target_endpoint_uri: Some(connector_uri.clone()), // Target is the connector
            is_outbound_connection: false, // From binder's perspective, it's an inbound connection
            connection_iface: Arc::new(
                // Use the newly defined struct
                crate::socket::connection_iface::InprocBinderSideConnection {
                    _connection_id: inproc_endpoint_entry_handle_id,
                }
            ),
        };

        {
            let mut binder_core_state = core_arc.core_state.write();
            binder_core_state.pipes_tx.insert(
                binder_write_id_for_this_connection,
                pipe_tx_for_binder_to_send_to_connector,
            );
            binder_core_state.pipe_reader_task_handles.insert(
                binder_read_id_for_this_connection,
                pipe_reader_task,
            );
            binder_core_state.endpoints.insert(connector_uri.clone(), endpoint_info_for_binder);
            binder_core_state.pipe_read_id_to_endpoint_uri.insert(binder_read_id_for_this_connection, connector_uri.clone());
        }

        if let Some(monitor_tx) = core_arc.core_state.read().get_monitor_sender_clone() {
            let _ = monitor_tx.try_send(SocketEvent::Accepted { // Or Connected from binder's side?
                endpoint: connector_uri.clone(), // The "peer" is the connector
                peer_addr: format!("inproc-connector-{}", connector_uri), // Synthetic
            });
        }

        socket_logic_strong.pipe_attached(
            binder_read_id_for_this_connection,
            binder_write_id_for_this_connection,
            None,
        ).await;
        tracing::info!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Inproc connection accepted by binder.");
    }

    // Send reply to connector
    if reply_tx_to_connector.take_and_send_forget(accept_result.clone()).await == false {
         tracing::warn!(binder_handle = binder_core_handle, connector_uri = %connector_uri, "Failed to send InprocBindingRequest reply to connector (already taken/dropped).");
         if accept_result.is_ok() {
            // If we accepted but couldn't reply, connector might timeout.
            // We've set up our side, so it should eventually be cleaned by PipeClosedByPeer from connector
            // or by SocketCore shutdown.
         }
    }
    accept_result // Return the result for potential error propagation
}

#[cfg(feature = "inproc")]
pub(crate) async fn handle_inproc_pipe_peer_closed_event(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    // This is the ID the *binder* uses to WRITE to the (now closed) connector.
    // It corresponds to the *connector's* `pipe_read_id`.
    closed_binder_write_id: usize,
) {
    use crate::socket::core::state::EndpointType;

    let binder_core_handle = core_arc.handle;
    tracing::debug!(
        binder_handle = binder_core_handle,
        binder_write_id_to_closed_connector = closed_binder_write_id,
        "SocketCore (binder) handling InprocPipePeerClosed event."
    );

    // The binder needs to find which of its *read pipes* corresponds to this connection.
    // The event gives us `closed_by_connector_pipe_read_id`, which is what the *binder writes to*.
    // We need to find the `EndpointInfo` where `pipe_ids.0` (core_writes_here) matches `closed_binder_write_id`.
    // Then, from that `EndpointInfo`, get `pipe_ids.1` (core_reads_from_here), which is the binder's read_id.
    let mut binder_read_id_to_cleanup: Option<usize> = None;
    let mut endpoint_uri_of_closed_conn: Option<String> = None;

    {
        let core_s_read = core_arc.core_state.read();
        for (uri, ep_info) in core_s_read.endpoints.iter() {
            if ep_info.endpoint_type == EndpointType::Session { // Inproc conns are Session type
                if let Some((write_id, read_id)) = ep_info.pipe_ids {
                    if write_id == closed_binder_write_id {
                        binder_read_id_to_cleanup = Some(read_id);
                        endpoint_uri_of_closed_conn = Some(uri.clone());
                        break;
                    }
                }
            }
        }
    } // core_state read lock dropped

    if let Some(read_id_to_clean) = binder_read_id_to_cleanup {
        tracing::debug!(
            binder_handle = binder_core_handle,
            binder_read_pipe_id_to_clean = read_id_to_clean,
            uri = %endpoint_uri_of_closed_conn.as_deref().unwrap_or("N/A"),
            "Found binder's read pipe to clean up for closed inproc connection."
        );
        // Use the generic cleanup function. It will also call ISocket::pipe_detached.
        let _ = cleanup_session_state_by_pipe(core_arc.clone(), read_id_to_clean, socket_logic_strong).await;
    } else {
        tracing::warn!(
            binder_handle = binder_core_handle,
            binder_write_id_to_closed_connector = closed_binder_write_id,
            "SocketCore (binder) could not find its corresponding read pipe for InprocPipePeerClosed event. Cleanup may be incomplete."
        );
    }
}

/// Helper function to send a ZMQ message over an async pipe with optional timeout.
/// This function is now pub(crate) within the pipe_manager module.
pub(crate) async fn send_msg_with_timeout(
  pipe_tx: &AsyncSender<Msg>, // Stays async-channel for now
  msg: Msg,
  timeout_opt: Option<Duration>,
  socket_core_handle: usize, // For logging context
  pipe_target_id: usize,     // For logging context
) -> Result<(), ZmqError> {
  match timeout_opt {
    None => { // Infinite timeout (block until HWM allows or pipe closes)
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
    Some(d) if d.is_zero() => { // Non-blocking (SNDTIMEO = 0)
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
    Some(timeout_duration) => { // Timed send (SNDTIMEO > 0)
      tracing::trace!(
          core_handle = socket_core_handle,
          pipe_id = pipe_target_id,
          send_timeout_duration = ?timeout_duration,
          "Attempting timed send via pipe"
      );
      match timeout(timeout_duration, pipe_tx.send(msg)).await {
        Ok(Ok(())) => Ok(()), // Sent within timeout
        Ok(Err(SendError(_failed_msg_back))) => { // Pipe closed during timed send
          tracing::debug!(
            core_handle = socket_core_handle,
            pipe_id = pipe_target_id,
            "Timed pipe send failed (ConnectionClosed)"
          );
          Err(ZmqError::ConnectionClosed)
        }
        Err(_timeout_elapsed_error) => { // tokio::time::Timeout error (timeout elapsed)
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