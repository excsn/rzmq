use crate::error::ZmqError;
use crate::runtime::{Command, SystemEvent};
use crate::socket::ISocket;
#[cfg(feature = "io-uring")]
use crate::socket::connection_iface::UringFdConnection;
use crate::socket::core::state::ShutdownPhase;
use crate::socket::core::{SocketCore, command_processor, pipe_manager, shutdown};
#[cfg(feature = "io-uring")]
use crate::uring;

use std::sync::Arc;

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
      tracing::info!(
        handle = core_handle,
        "SocketCore received ContextTerminating event."
      );
      shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
    }

    SystemEvent::SocketClosing { socket_id } => {
      if socket_id == core_handle {
        tracing::debug!(
          handle = core_handle,
          "SocketCore received its own SocketClosing event."
        );
        shutdown::initiate_core_shutdown(core_arc.clone(), socket_logic_strong, false).await;
      } else {
        // This event is for another socket, SocketCore ignores it.
        tracing::trace!(
          handle = core_handle,
          other_socket_id = socket_id,
          "SocketCore observed SocketClosing event for another socket."
        );
      }
    }
    SystemEvent::ActorStopping {
      handle_id: child_actor_id,
      actor_type,
      endpoint_uri,
      parent_id,
      error,
    } => {
      if Some(core_handle) == parent_id {
        shutdown::handle_actor_stopping_event(
          core_arc.clone(),
          socket_logic_strong,
          child_actor_id,
          actor_type,
          endpoint_uri.as_deref(),
          error.as_ref(),
        )
        .await;
      }
    }

    SystemEvent::PeerIdentityEstablished {
      parent_core_id,
      connection_identifier,
      peer_identity,
      peer_socket_type,
    } => {
      if parent_core_id == core_handle {
        if current_shutdown_phase == ShutdownPhase::Running {
          tracing::debug!(
            handle = core_handle,
            conn_id = connection_identifier,
            identity = ?peer_identity,
            "SocketCore processing PeerIdentityEstablished event."
          );

          {
            // Scoped write lock
            let mut core_s_write = core_arc.core_state.write();
            // First, clone the URI from the mapping. This releases any borrow on core_s_write related to the lookup.
            let uri_opt = core_s_write
              .pipe_read_id_to_endpoint_uri
              .get(&connection_identifier)
              .cloned();

            if let Some(uri) = uri_opt {
              // Now, perform the mutable lookup using the cloned URI.
              if let Some(ep_info) = core_s_write.endpoints.get_mut(&uri) {
                tracing::debug!(
                    handle = core_handle,
                    pipe_id = connection_identifier,
                    peer_type = ?peer_socket_type,
                    "Updating peer socket type in EndpointInfo."
                );
                ep_info.peer_socket_type = peer_socket_type;
              }

              // 2. Reset reconnect backoff state on successful handshake
              if let Some(recon_state) = core_s_write.reconnect_states.get_mut(&uri) {
                recon_state.on_connection_success();
                tracing::trace!(handle = core_handle, uri = %uri, "Reset reconnect backoff state after success.");
              }
            }
          }

          let core_write_id_opt = {
            let core_s = core_arc.core_state.read();
            core_s
              .pipe_read_id_to_endpoint_uri
              .get(&connection_identifier)
              .and_then(|uri| core_s.endpoints.get(uri))
              .and_then(|ep_info| ep_info.pipe_ids.map(|pids| pids.0))
          };

          if let Some(core_write_id) = core_write_id_opt {
            socket_logic_strong
              .pipe_attached(
                connection_identifier,
                core_write_id,
                peer_identity.as_ref().map(|b| b.as_ref()),
              )
              .await;
          }

          socket_logic_strong
            .update_peer_identity(connection_identifier, peer_identity)
            .await;
        } else {
          tracing::debug!(
            handle = core_handle,
            conn_id = connection_identifier,
            "SocketCore ignoring PeerIdentityEstablished during shutdown."
          );
        }
      }
    }

    SystemEvent::ConnectionAttemptFailed {
      parent_core_id,
      target_endpoint_uri,
      error,
    } => {
      if parent_core_id == core_handle {
        command_processor::handle_connect_failed_event(
          core_arc,
          socket_logic_strong.clone(),
          target_endpoint_uri,
          error,
        )
        .await;
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
      let is_my_binding_name = core_arc
        .core_state
        .read()
        .bound_inproc_names
        .contains(&target_inproc_name);
      if is_my_binding_name {
        if current_shutdown_phase == ShutdownPhase::Running {
          pipe_manager::process_inproc_binding_request_event(
            core_arc,
            socket_logic_strong,
            connector_uri,
            binder_pipe_rx_from_connector,
            binder_pipe_tx_to_connector,
            connector_pipe_read_id,
            connector_pipe_write_id,
            reply_tx,
          )
          .await?;
        } else {
          tracing::debug!(handle = core_handle, target_inproc_name = %target_inproc_name, "SocketCore (binder) ignoring InprocBindingRequest during shutdown.");
          let _ = reply_tx.send(Err(ZmqError::InvalidState(
            "Binder socket is shutting down".into(),
          )));
        }
      }
    }

    #[cfg(feature = "inproc")]
    SystemEvent::InprocPipePeerClosed {
      target_inproc_name,
      closed_by_connector_pipe_read_id,
    } => {
      let is_my_binding_name = core_arc
        .core_state
        .read()
        .bound_inproc_names
        .contains(&target_inproc_name);
      if is_my_binding_name {
        tracing::debug!(handle = core_handle, binder_name=%target_inproc_name, id_closed = closed_by_connector_pipe_read_id, "SocketCore (binder) processing InprocPipePeerClosed.");
        pipe_manager::handle_inproc_pipe_peer_closed_event(
          core_arc,
          socket_logic_strong,
          closed_by_connector_pipe_read_id,
        )
        .await;
      }
    }

    SystemEvent::ActorStarted {
      handle_id: _started_actor_id,
      actor_type: _actor_type,
      parent_id: _parent_id_opt,
    } => {
      // SocketCore primarily cares about ActorStopping events for its direct children
      // to manage their lifecycle (e.g., remove from endpoints map, call pipe_detached).
      // ActorStarted is mainly for the Context's WaitGroup.
      // No specific action needed by SocketCore for ActorStarted events *of other actors* in general.
      // It publishes ActorStarted for its own children (Listeners, Connecters, PipeReaders).
      tracing::trace!(handle = core_handle, event = ?event, "SocketCore observed ActorStarted event (typically no action needed by SocketCore for this).");
    }
  }
  Ok(())
}

// handle_new_connection_established has been moved to command_processor.rs.
// Connection registration now flows through Command::NewConnectionEstablished (direct mailbox).
