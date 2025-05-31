// core/src/socket/core/event_processor.rs

use crate::error::ZmqError;
use crate::runtime::{Command, SystemEvent}; // Command needed for ViaSessionActor stop
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::{EndpointInfo, EndpointType, ShutdownPhase};
use crate::socket::core::{SocketCore, shutdown, pipe_manager, command_processor};
use crate::socket::ISocket;
use crate::runtime::system_events::ConnectionInteractionModel;
#[cfg(feature = "io-uring")]
use crate::runtime::global_uring_state;

use std::sync::Arc;
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use tokio::task::Id as TaskId;


/// Processes a system event received by the SocketCore.
/// Returns Ok(()) if processed successfully, or Err(ZmqError) for fatal errors
/// that should cause SocketCore to shut down.
pub(crate) async fn process_system_event(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    event: SystemEvent,
) -> Result<(), ZmqError> {
    let core_handle = core_arc.handle;
    tracing::trace!(handle = core_handle, event = ?event, "SocketCore processing system event");

    let current_shutdown_phase = core_arc.shutdown_coordinator.lock().await.state;

    match event {
        SystemEvent::ContextTerminating => {
            tracing::info!(handle = core_handle, "SocketCore received ContextTerminating event.");
            shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
        }

        SystemEvent::SocketClosing { socket_id } if socket_id == core_handle => {
            tracing::debug!(handle = core_handle, "SocketCore received its own SocketClosing event.");
            shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
        }

        SystemEvent::ActorStopping {
            handle_id: child_actor_id,
            actor_type,
            endpoint_uri,
            error,
        } => {
            shutdown::handle_actor_stopping_event(
                core_arc.clone(),
                socket_logic_strong,
                child_actor_id,
                actor_type,
                endpoint_uri.as_deref(),
                error.as_ref(),
            ).await;
        }

        SystemEvent::NewConnectionEstablished {
            parent_core_id,
            endpoint_uri,
            target_endpoint_uri,
            connection_iface,
            interaction_model,
            managing_actor_task_id,
        } => {
            if parent_core_id == core_handle {
                if current_shutdown_phase == ShutdownPhase::Running {
                    // Call the fully implemented helper
                    handle_new_connection_established(
                        core_arc,
                        socket_logic_strong,
                        endpoint_uri,
                        target_endpoint_uri,
                        connection_iface,
                        interaction_model,
                        managing_actor_task_id,
                    ).await?; // Propagate error if connection setup fails fatally
                } else {
                    tracing::warn!(
                        handle = core_handle,
                        new_conn_uri = %endpoint_uri,
                        "SocketCore ignoring NewConnectionEstablished during its shutdown. Attempting to close new connection."
                    );
                    if let Err(e) = connection_iface.close_connection().await {
                        tracing::error!(handle = core_handle, new_conn_uri = %endpoint_uri, "Error closing orphaned new connection: {}", e);
                    }
                    if let ConnectionInteractionModel::ViaSessionActor { session_actor_mailbox } = interaction_model {
                        // Attempt to stop the session actor directly
                        let _ = session_actor_mailbox.try_send(Command::Stop);
                    }
                }
            }
        }

        SystemEvent::PeerIdentityEstablished {
            parent_core_id,
            connection_identifier, // This is pipe_read_id (actual or synthetic)
            peer_identity,
        } => {
            if parent_core_id == core_handle {
                if current_shutdown_phase == ShutdownPhase::Running {
                    tracing::debug!(
                        handle = core_handle,
                        conn_id = connection_identifier,
                        identity = ?peer_identity,
                        "SocketCore processing PeerIdentityEstablished event."
                    );
                    socket_logic_strong.update_peer_identity(connection_identifier, peer_identity).await;
                } else {
                    tracing::debug!(handle = core_handle, conn_id = connection_identifier, "SocketCore ignoring PeerIdentityEstablished during shutdown.");
                }
            }
        }

        SystemEvent::ConnectionAttemptFailed {
            parent_core_id,
            target_endpoint_uri,
            error_msg,
        } => {
            if parent_core_id == core_handle {
                // ConnectionAttemptFailed can occur when running or shutting down (if a pending connect fails)
                // The handler should be robust to the current phase.
                command_processor::handle_connect_failed_event( // Assuming this helper moved
                    core_arc,
                    target_endpoint_uri,
                    ZmqError::Internal(error_msg), // Convert String to ZmqError
                ).await;
            }
        }

        #[cfg(feature = "inproc")]
        SystemEvent::InprocBindingRequest {
            target_inproc_name,
            connector_uri,
            binder_pipe_tx_to_connector,
            binder_pipe_rx_from_connector,
            connector_pipe_write_id,
            connector_pipe_read_id,
            reply_tx,
        } => {
            let is_my_binding_name = core_arc.core_state.read().bound_inproc_names.contains(&target_inproc_name);
            if is_my_binding_name {
                if current_shutdown_phase == ShutdownPhase::Running {
                    pipe_manager::process_inproc_binding_request_event(
                        core_arc,
                        socket_logic_strong,
                        connector_uri,
                        binder_pipe_rx_from_connector,
                        binder_pipe_tx_to_connector,
                        connector_pipe_write_id,
                        connector_pipe_read_id,
                        reply_tx,
                    ).await?;
                } else {
                    tracing::debug!(handle = core_handle, target_inproc_name = %target_inproc_name, "SocketCore (binder) ignoring InprocBindingRequest during shutdown.");
                    let _ = reply_tx.take_and_send_forget(Err(ZmqError::InvalidState("Binder socket is shutting down".into()))).await;
                }
            }
        }

        #[cfg(feature = "inproc")]
        SystemEvent::InprocPipePeerClosed {
            target_inproc_name,
            closed_by_connector_pipe_read_id,
        } => {
            let is_my_binding_name = core_arc.core_state.read().bound_inproc_names.contains(&target_inproc_name);
            if is_my_binding_name {
                tracing::debug!(handle = core_handle, binder_name=%target_inproc_name, id_closed = closed_by_connector_pipe_read_id, "SocketCore (binder) processing InprocPipePeerClosed.");
                pipe_manager::handle_inproc_pipe_peer_closed_event(
                    core_arc,
                    socket_logic_strong,
                    closed_by_connector_pipe_read_id,
                ).await;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Handles the NewConnectionEstablished system event.
async fn handle_new_connection_established(
    core_arc: Arc<SocketCore>,
    socket_logic_strong: &Arc<dyn ISocket>,
    endpoint_uri_from_event: String, // Actual resolved URI of the connection
    target_endpoint_uri_from_event: String, // Original user target URI
    connection_iface_from_event: Arc<dyn ISocketConnection>,
    interaction_model_from_event: ConnectionInteractionModel,
    _managing_actor_task_id_from_event: Option<TaskId>, // TaskId of SessionBase, informational
) -> Result<(), ZmqError> {
    let core_handle = core_arc.handle;

    let connection_instance_id = connection_iface_from_event.get_connection_id();

    let is_outbound_this_core_initiated = {
        let core_s_guard = core_arc.core_state.read();
        !core_s_guard.endpoints.values().any(|ep_info|
            ep_info.endpoint_type == EndpointType::Listener && ep_info.endpoint_uri == target_endpoint_uri_from_event
        )
    };

    match interaction_model_from_event {
        ConnectionInteractionModel::ViaSessionActor { ref session_actor_mailbox } => {
            tracing::debug!(
                handle = core_handle,
                conn_uri = %endpoint_uri_from_event,
                session_actor_id = connection_instance_id,
                "NewConnectionEstablished: Standard SessionActor path."
            );

            let (actual_core_write_id, actual_core_read_id) = pipe_manager::setup_pipe_with_session_actor(
                core_arc.clone(),
                socket_logic_strong.clone(),
                session_actor_mailbox,
                connection_instance_id, // This is SessionBase actor's handle_id
                &endpoint_uri_from_event,
            ).await?; // Propagate error: if this fails, connection setup is aborted.

            let endpoint_info = EndpointInfo {
                mailbox: session_actor_mailbox.clone(),
                task_handle: None, // SocketCore doesn't manage SessionBase task_handle directly
                endpoint_type: EndpointType::Session,
                endpoint_uri: endpoint_uri_from_event.clone(),
                pipe_ids: Some((actual_core_write_id, actual_core_read_id)),
                handle_id: connection_instance_id, // Session actor's handle
                target_endpoint_uri: Some(target_endpoint_uri_from_event),
                is_outbound_connection: is_outbound_this_core_initiated,
                connection_iface: connection_iface_from_event.clone(),
            };

            {
                let mut core_s = core_arc.core_state.write();
                if let Some(old_info) = core_s.endpoints.insert(endpoint_uri_from_event.clone(), endpoint_info) {
                    tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnectionEstablished.");
                    // If old_info had a task_handle (e.g. for a Listener on same URI, though unlikely for session path), abort it.
                    if let Some(old_task_handle) = old_info.task_handle { old_task_handle.abort(); }
                    // If old_info had pipe_ids, ensure they are cleaned up.
                    if let Some((old_w, old_r)) = old_info.pipe_ids {
                        core_s.remove_pipe_state(old_w, old_r);
                        // ISocket::pipe_detached should also be called for the old pipe_ids.
                        // This scenario (URI collision leading to overwrite) needs careful handling to prevent resource leaks.
                        // For now, just log and basic cleanup. A more robust solution might involve
                        // ensuring URIs for sessions are always unique (e.g., by including peer address).
                        let socket_logic_clone = socket_logic_strong.clone(); // Clone for async move block
                        tokio::spawn(async move { socket_logic_clone.pipe_detached(old_r).await; });
                    }
                }
                core_s.pipe_read_id_to_endpoint_uri.insert(actual_core_read_id, endpoint_uri_from_event.clone());
            }
            socket_logic_strong.pipe_attached(actual_core_read_id, actual_core_write_id, None).await;
            tracing::info!(handle=core_handle, session_id=connection_instance_id, conn_uri=%endpoint_uri_from_event, "Session-based connection fully attached.");
        }

        #[cfg(feature = "io-uring")]
        ConnectionInteractionModel::ViaUringFd { fd } => {
            tracing::debug!(
                handle = core_handle,
                conn_uri = %endpoint_uri_from_event,
                raw_fd = fd,
                "NewConnectionEstablished: io_uring FD path."
            );

            let uring_fd_as_endpoint_handle_id = fd as usize;

            let synthetic_read_id = core_arc.context.inner().next_handle();
            let synthetic_write_id = core_arc.context.inner().next_handle();

            let endpoint_info = EndpointInfo {
                mailbox: core_arc.command_sender(), // Placeholder
                task_handle: None,
                endpoint_type: EndpointType::Session,
                endpoint_uri: endpoint_uri_from_event.clone(),
                pipe_ids: Some((synthetic_write_id, synthetic_read_id)),
                handle_id: uring_fd_as_endpoint_handle_id,
                target_endpoint_uri: Some(target_endpoint_uri_from_event),
                is_outbound_connection: is_outbound_this_core_initiated,
                connection_iface: connection_iface_from_event.clone(),
            };

            global_uring_state::register_uring_fd_socket_core_mailbox(fd, core_arc.command_sender());

            {
                let mut core_s = core_arc.core_state.write();
                if let Some(old_info) = core_s.endpoints.insert(endpoint_uri_from_event.clone(), endpoint_info) {
                     tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnectionEstablished (io_uring).");
                     if let Some(old_task_handle) = old_info.task_handle { old_task_handle.abort(); }
                     // Similar cleanup logic as above if old_info was a session or uring fd.
                }
                core_s.pipe_read_id_to_endpoint_uri.insert(synthetic_read_id, endpoint_uri_from_event.clone());
                core_s.uring_fd_to_endpoint_uri.insert(fd, endpoint_uri_from_event.clone());
            }
            socket_logic_strong.pipe_attached(synthetic_read_id, synthetic_write_id, None).await;
            tracing::info!(handle=core_handle, raw_fd=fd, conn_uri=%endpoint_uri_from_event, "UringFd-based connection fully attached.");
        }
        #[cfg(not(feature = "io-uring"))]
        ConnectionInteractionModel::ViaUringFd { _fd_placeholder } => {
            tracing::error!(handle = core_handle, "FATAL: Received ViaUringFd model when io-uring feature is disabled.");
            return Err(ZmqError::Internal("Invalid connection model for build configuration".into()));
        }
    }
    Ok(())
}