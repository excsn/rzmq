use crate::error::ZmqError;
use crate::runtime::system_events::ConnectionInteractionModel;
use crate::runtime::{Command, SystemEvent};
use crate::sessionx::ScaConnectionIface;
use crate::socket::connection_iface::ISocketConnection;
#[cfg(feature = "io-uring")]
use crate::socket::connection_iface::UringFdConnection;
use crate::socket::core::state::{EndpointInfo, EndpointType, ShutdownPhase};
use crate::socket::core::{command_processor, pipe_manager, shutdown, SocketCore};
use crate::socket::ISocket;
#[cfg(feature = "io-uring")]
use crate::uring;
use crate::Msg;

use std::sync::Arc;
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

    SystemEvent::NewConnectionEstablished {
      parent_core_id,
      endpoint_uri,
      target_endpoint_uri,
      connection_iface: cif_opt,
      interaction_model,
      managing_actor_task_id,
    } => {
      if parent_core_id == core_handle {
        if current_shutdown_phase == ShutdownPhase::Running {
          handle_new_connection_established(
            core_arc,
            socket_logic_strong,
            endpoint_uri,
            target_endpoint_uri,
            cif_opt,
            interaction_model,
            managing_actor_task_id,
          )
          .await?;
        } else {
          tracing::warn!(
            handle = core_handle,
            new_conn_uri = %endpoint_uri,
            "SocketCore ignoring NewConnectionEstablished during its shutdown. Attempting to close new connection."
          );

          if let Some(iface) = cif_opt {
            if let Err(e) = iface.close_connection().await {
              tracing::error!(handle = core_handle, new_conn_uri = %endpoint_uri, "Error closing orphaned new connection: {}", e);
            }
          }

          if let ConnectionInteractionModel::ViaSca { sca_mailbox, .. } = interaction_model {
            let _ = sca_mailbox.try_send(Command::Stop);
          }
        }
      }
    }

    SystemEvent::PeerIdentityEstablished {
      parent_core_id,
      connection_identifier,
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
      error_msg,
    } => {
      if parent_core_id == core_handle {
        command_processor::handle_connect_failed_event(
          core_arc,
          socket_logic_strong.clone(),
          target_endpoint_uri,
          ZmqError::Internal(error_msg),
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

/// Handles the NewConnectionEstablished system event.
async fn handle_new_connection_established(
  core_arc: Arc<SocketCore>,
  socket_logic_strong: &Arc<dyn ISocket>,
  endpoint_uri_from_event: String,
  target_endpoint_uri_from_event: String,
  connection_iface_from_event_opt: Option<Arc<dyn ISocketConnection>>,
  interaction_model_from_event: ConnectionInteractionModel,
  _managing_actor_task_id: Option<TaskId>,
) -> Result<(), ZmqError> {
  let core_handle = core_arc.handle;

  let connection_instance_id = match &interaction_model_from_event {
    ConnectionInteractionModel::ViaSca { sca_handle_id, .. } => *sca_handle_id,
    #[cfg(feature = "io-uring")]
    ConnectionInteractionModel::ViaUringFd { fd } => *fd as usize,
    #[cfg(not(feature = "io-uring"))]
    ConnectionInteractionModel::ViaUringFd { .. } => {
      unreachable!("ViaUringFd model when io-uring feature is disabled")
    }
  };

  let is_outbound_this_core_initiated = {
    let core_s_guard = core_arc.core_state.read();
    !core_s_guard.endpoints.values().any(|ep_info| {
      ep_info.endpoint_type == EndpointType::Listener
        && ep_info.endpoint_uri == target_endpoint_uri_from_event
    })
  };

  // Centralized construction of final_connection_iface
  let final_connection_iface: Arc<dyn ISocketConnection>;

  match interaction_model_from_event {
    ConnectionInteractionModel::ViaSca {
      sca_mailbox,
      sca_handle_id,
    } => {
      tracing::debug!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        sca_actor_id = sca_handle_id,
        "NewConnectionEstablished: SessionConnectionActorX path."
      );

      // connection_iface_from_event_opt should be None for SCA, as SocketCore creates the iface.
      if connection_iface_from_event_opt.is_some() {
        tracing::warn!(handle = core_handle, conn_uri = %endpoint_uri_from_event, "ViaSca received unexpected pre-existing ISocketConnection. Ignoring.");
      }

      // 1. Create data pipe for SocketCore -> SCA
      let (tx_core_to_sca, rx_sca_from_core) =
        fibre::mpmc::bounded_async::<Vec<Msg>>(core_arc.core_state.read().options.sndhwm.max(1));

      // 2. Generate pipe IDs from SocketCore's perspective
      let core_write_id = core_arc.context.inner().next_handle(); // SocketCore writes here
      let core_read_id = core_arc.context.inner().next_handle(); // SocketCore reads from SCA via commands associated with this ID

      // 4. Send AttachPipesAndRoutingInfo to the SessionConnectionActorX
      let attach_cmd = Command::ScaInitializePipes {
        sca_handle_id,
        rx_from_core: rx_sca_from_core,
        core_pipe_read_id_for_incoming_routing: core_read_id,
      };

      if sca_mailbox.send(attach_cmd).await.is_err() {
        return Err(ZmqError::Internal(format!(
          "Failed to send ScaInitializePipes to SCA {}",
          sca_handle_id
        )));
      }

      tracing::debug!(
        handle = core_handle,
        sca_id = sca_handle_id,
        core_w_id = core_write_id,
        core_r_id = core_read_id,
        "Sent AttachPipesAndRoutingInfo to SessionConnectionActorX."
      );

      // 5. Create the connection interface for SocketCore
      let sca_iface = Arc::new(ScaConnectionIface::new(
        sca_mailbox.clone(),
        sca_handle_id,
        core_arc.clone(),
        core_write_id,
      ));

      // 6. Create and store EndpointInfo
      let endpoint_info = EndpointInfo {
        mailbox: sca_mailbox, // Mailbox to the SCA (for Stop commands mainly)
        task_handle: None,    // SocketCore doesn't hold JoinHandle for SCA
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint_uri_from_event.clone(),
        pipe_ids: Some((core_write_id, core_read_id)),
        handle_id: sca_handle_id, // This is the SCA's own handle
        target_endpoint_uri: Some(target_endpoint_uri_from_event),
        is_outbound_connection: is_outbound_this_core_initiated,
        connection_iface: sca_iface,
      };

      {
        core_arc
          .core_state
          .write()
          .pipes_tx
          .insert(core_write_id, tx_core_to_sca); // Store sender end of data pipe

        let old_info_result = core_arc
          .core_state
          .write()
          .endpoints
          .insert(endpoint_uri_from_event.clone(), endpoint_info);

        if let Some(old_info) = old_info_result {
          tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnection (SCA).");
          // Defer async cleanup outside lock
          tokio::spawn(async move {
            let _ = old_info.connection_iface.close_connection().await;
            if let Some(h) = old_info.task_handle {
              h.abort();
            }
          });
        }
        core_arc
          .core_state
          .write()
          .pipe_read_id_to_endpoint_uri
          .insert(core_read_id, endpoint_uri_from_event.clone());
      }

      // 7. Notify ISocket logic that pipes are attached
      // The ISocket pattern logic uses these IDs to manage its internal state for the connection.
      socket_logic_strong
        .pipe_attached(
          core_read_id,
          core_write_id,
          None, /* peer_identity comes later */
        )
        .await;
      tracing::info!(handle=core_handle, sca_id=sca_handle_id, conn_uri=%endpoint_uri_from_event, "SessionConnectionActorX connection fully attached to SocketCore.");
    }

    #[cfg(feature = "io-uring")]
    ConnectionInteractionModel::ViaUringFd { fd } => {
      tracing::debug!(
        handle = core_handle,
        conn_uri = %endpoint_uri_from_event,
        raw_fd = fd,
        "NewConnectionEstablished: io_uring FD path."
      );

      final_connection_iface = connection_iface_from_event_opt.ok_or_else(|| {
        ZmqError::Internal("UringFdConnection missing from NewConnectionEstablished event".into())
      })?;

      let uring_fd_as_endpoint_handle_id = fd as usize;

      let synthetic_read_id = core_arc.context.inner().next_handle();
      let synthetic_write_id = core_arc.context.inner().next_handle();

      let endpoint_info = EndpointInfo {
        mailbox: core_arc.command_sender(),
        task_handle: None,
        endpoint_type: EndpointType::Session,
        endpoint_uri: endpoint_uri_from_event.clone(),
        pipe_ids: Some((synthetic_write_id, synthetic_read_id)),
        handle_id: uring_fd_as_endpoint_handle_id,
        target_endpoint_uri: Some(target_endpoint_uri_from_event),
        is_outbound_connection: is_outbound_this_core_initiated,
        connection_iface: final_connection_iface.clone(),
      };

      uring::global_state::register_uring_fd_socket_core_mailbox(fd, core_arc.command_sender());

      {
        let mut core_s = core_arc.core_state.write();
        if let Some(old_info) = core_s
          .endpoints
          .insert(endpoint_uri_from_event.clone(), endpoint_info)
        {
          tracing::warn!(handle=core_handle, uri=%endpoint_uri_from_event, "Overwrote existing EndpointInfo for NewConnectionEstablished (io_uring).");
          if let Some(old_task_handle) = old_info.task_handle {
            old_task_handle.abort();
          }
        }
        core_s
          .pipe_read_id_to_endpoint_uri
          .insert(synthetic_read_id, endpoint_uri_from_event.clone());
        core_s
          .uring_fd_to_endpoint_uri
          .insert(fd, endpoint_uri_from_event.clone());
      }
      socket_logic_strong
        .pipe_attached(synthetic_read_id, synthetic_write_id, None)
        .await;
      tracing::info!(handle=core_handle, raw_fd=fd, conn_uri=%endpoint_uri_from_event, "UringFd-based connection fully attached.");
    }
    #[cfg(not(feature = "io-uring"))]
    ConnectionInteractionModel::ViaUringFd { _fd_placeholder } => {
      tracing::error!(
        handle = core_handle,
        "FATAL: Received ViaUringFd model when io-uring feature is disabled."
      );
      return Err(ZmqError::Internal(
        "Invalid connection model for build configuration".into(),
      ));
    }
  }
  Ok(())
}
