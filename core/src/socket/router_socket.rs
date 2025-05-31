// core/src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::ROUTER_MANDATORY;
use crate::socket::patterns::{incoming_orchestrator::IncomingMessageOrchestrator, RouterMap, WritePipeCoordinator};
use crate::socket::{ISocket, SourcePipeReadId};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{RwLock as ParkingLotRwLock, RwLockReadGuard as ParkingLotRwLockReadGuard};
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit};

use super::core::command_processor::update_core_option;
use super::parse_bool_option;

/// Structure to hold state for an ongoing fragmented send.
#[derive(Debug)]
struct ActiveFragmentedSend {
  target_endpoint_uri: String,
  _permit: OwnedSemaphorePermit,
}

/// Context for Router's static message processing function.
struct RouterMessageProcessCtx<'a> {
  identity_map_guard: ParkingLotRwLockReadGuard<'a, HashMap<usize, Blob>>,
}

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map_for_send: RouterMap,
  incoming_orchestrator: IncomingMessageOrchestrator,
  pipe_to_identity_shared_map: Arc<ParkingLotRwLock<HashMap<usize, Blob>>>,
  current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let shared_pipe_to_identity_map = Arc::new(ParkingLotRwLock::new(HashMap::new()));
    let orchestrator = IncomingMessageOrchestrator::new(&core);

    Self {
      core,
      router_map_for_send: RouterMap::new(),
      incoming_orchestrator: orchestrator,
      pipe_to_identity_shared_map: shared_pipe_to_identity_map,
      current_send_target: TokioMutex::new(None),
      pipe_send_coordinator: Arc::new(WritePipeCoordinator::new()),
    }
  }

  // core_state_read() remains, assuming its usage elsewhere follows the pattern.
  fn core_state_read(&self) -> ParkingLotRwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from_bytes(Bytes::from(format!("pipe:{}", pipe_read_id)))
  }

  fn process_raw_message_for_router(
    pipe_read_id: usize,
    mut raw_zmtp_message: Vec<Msg>,
    ctx: RouterMessageProcessCtx<'_>,
  ) -> Result<Vec<Msg>, ZmqError> {
    let identity_blob = ctx.identity_map_guard.get(&pipe_read_id).cloned().unwrap_or_else(|| {
      tracing::warn!(
        "Router transform: Identity for pipe {} not found in shared map, using placeholder.",
        pipe_read_id
      );
      Self::pipe_id_to_placeholder_identity(pipe_read_id)
    });

    let mut id_msg = Msg::from_vec(identity_blob.to_vec());
    id_msg.set_flags(MsgFlags::MORE);

    if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
      tracing::trace!(
        "Router transform: Stripped empty ZMTP delimiter from pipe {}.",
        pipe_read_id
      );
      raw_zmtp_message.remove(0);
    }

    let mut application_message = Vec::with_capacity(1 + raw_zmtp_message.len());
    application_message.push(id_msg);
    application_message.extend(raw_zmtp_message);

    Ok(application_message)
  }
}

#[async_trait]
impl ISocket for RouterSocket {
  fn core(&self) -> &Arc<SocketCore> {
    &self.core
  }
  fn mailbox(&self) -> MailboxSender {
    self.core.command_sender()
  }

  async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserBind, endpoint: endpoint.to_string())
  }
  async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserConnect, endpoint: endpoint.to_string())
  }
  async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserDisconnect, endpoint: endpoint.to_string())
  }
  async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserUnbind, endpoint: endpoint.to_string())
  }
  async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
  }
  async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    delegate_to_core!(self, UserGetOpt, option: option)
  }
  async fn close(&self) -> Result<(), ZmqError> {
    delegate_to_core!(self, UserClose,)
  }

  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    // Extract options once. The Arc<SocketOptions> itself is cheap to clone.
    let core_opts_arc = self.core.core_state.read().options.clone();
    let timeout_opt: Option<Duration> = core_opts_arc.sndtimeo;
    let router_mandatory_opt: bool = core_opts_arc.router_mandatory;

    // Check if we are continuing a fragmented send.
    // Lock current_send_target very briefly to get the URI if it exists.
    let active_target_uri_if_fragmenting: Option<String> = {
      let guard = self.current_send_target.lock().await;
      guard
        .as_ref()
        .map(|active_info| active_info.target_endpoint_uri.clone())
      // guard for current_send_target is dropped here
    };

    if let Some(target_uri_for_payload) = active_target_uri_if_fragmenting {
      // --- Subsequent Frame(s): Payload for an existing fragmented send ---
      // The permit is held by _permit in ActiveFragmentedSend which is inside current_send_target.

      // Get ISocketConnection for the target URI.
      // Lock core_state very briefly.
      let conn_iface_for_payload: Option<Arc<dyn ISocketConnection>> = self
        .core
        .core_state
        .read()
        .endpoints
        .get(&target_uri_for_payload)
        .map(|ep_info| ep_info.connection_iface.clone());

      let conn_iface = match conn_iface_for_payload {
        Some(iface) => iface,
        None => {
          // Target disappeared mid-fragmented send. Clear the state.
          // Lock current_send_target briefly to clear it.
          let mut clear_target_guard = self.current_send_target.lock().await;
          *clear_target_guard = None; // Drops the permit implicitly
                                      // clear_target_guard dropped.
          return if router_mandatory_opt {
            Err(ZmqError::HostUnreachable("Peer for fragmented send disappeared".into()))
          } else {
            Ok(()) // Silently drop
          };
        }
      };

      let is_last_user_part = !msg.is_more();
      // Perform the send operation (await point).
      let send_result = conn_iface.send_message(msg).await;

      if is_last_user_part {
        // Lock current_send_target briefly to clear the state.
        let mut clear_target_guard = self.current_send_target.lock().await;
        *clear_target_guard = None; // Release permit by dropping ActiveFragmentedSend
                                    // clear_target_guard dropped.
      }

      return match send_result {
        Ok(()) => Ok(()),
        Err(e) => {
          if router_mandatory_opt {
            Err(if matches!(e, ZmqError::ConnectionClosed) {
              ZmqError::HostUnreachable("Peer disconnected during payload send".into())
            } else {
              e
            })
          } else {
            Ok(())
          }
        }
      };
    } else {
      // --- First Frame: Must be the Destination Identity ---
      if !msg.is_more() {
        return Err(ZmqError::InvalidMessage(
          "ROUTER send: First frame (identity) must have MORE flag set".into(),
        ));
      }
      let destination_id = Blob::from_bytes(msg.data_bytes().unwrap_or_default());
      if destination_id.is_empty() {
        return Err(ZmqError::InvalidMessage(
          "ROUTER send: Identity frame cannot be empty".into(),
        ));
      }

      // Find target endpoint URI (await point).
      let target_endpoint_uri = match self.router_map_for_send.get_uri_for_identity(&destination_id).await {
        Some(uri) => uri,
        None => {
          return if router_mandatory_opt {
            Err(ZmqError::HostUnreachable(format!(
              "Peer {:?} not found (ROUTER_MANDATORY)",
              destination_id
            )))
          } else {
            Ok(()) // Silently drop
          };
        }
      };

      // Get ISocketConnection and connection_id for the target.
      // Lock core_state briefly.
      let (conn_iface_for_identity, conn_id_for_identity): (Option<Arc<dyn ISocketConnection>>, Option<usize>) = self
        .core
        .core_state
        .read()
        .endpoints
        .get(&target_endpoint_uri)
        .map_or((None, None), |ep_info| {
          (Some(ep_info.connection_iface.clone()), Some(ep_info.handle_id))
        });

      let (conn_iface, conn_id) = match (conn_iface_for_identity, conn_id_for_identity) {
        (Some(iface), Some(id)) => (iface, id),
        _ => {
          self.router_map_for_send.remove_peer_by_identity(&destination_id).await; // Clean up RouterMap
          return if router_mandatory_opt {
            Err(ZmqError::HostUnreachable(
              "Peer connection for identity disappeared".into(),
            ))
          } else {
            Ok(()) // Silently drop
          };
        }
      };

      // Acquire permit (await point).
      let permit = self
        .pipe_send_coordinator
        .acquire_send_permit(conn_id, timeout_opt)
        .await?;
      msg.set_flags(msg.flags() | MsgFlags::MORE); // Ensure identity has MORE

      // Send identity (await point).
      match conn_iface.send_message(msg).await {
        Ok(()) => {
          let mut delimiter_frame = Msg::new();
          delimiter_frame.set_flags(MsgFlags::MORE);
          // Send delimiter (await point).
          match conn_iface.send_message(delimiter_frame).await {
            Ok(()) => {
              // Successfully sent identity and delimiter. Lock current_send_target to store state.
              let mut set_target_guard = self.current_send_target.lock().await;
              *set_target_guard = Some(ActiveFragmentedSend {
                target_endpoint_uri,
                _permit: permit,
              });
              // set_target_guard dropped.
              Ok(())
            }
            Err(e) => {
              // Permit is dropped here as it's not moved.
              if router_mandatory_opt {
                Err(if matches!(e, ZmqError::ConnectionClosed) {
                  ZmqError::HostUnreachable("Peer disconnected during delimiter send".into())
                } else {
                  e
                })
              } else {
                Ok(())
              }
            }
          }
        }
        Err(e) => {
          // Permit is dropped here as it's not moved.
          if router_mandatory_opt {
            Err(if matches!(e, ZmqError::ConnectionClosed) {
              ZmqError::HostUnreachable("Peer disconnected during identity send".into())
            } else {
              e
            })
          } else {
            Ok(())
          }
        }
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
    self.incoming_orchestrator.recv_message(rcvtimeo_opt).await
  }

  async fn send_multipart(&self, mut frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if frames.len() < 2 {
      return Err(ZmqError::InvalidMessage(
        "ROUTER send_multipart requires at least identity and one payload frame".into(),
      ));
    }

    let mut destination_identity_frame = frames.remove(0);
    let destination_id_blob = Blob::from_bytes(destination_identity_frame.data_bytes().unwrap_or_default());
    if destination_id_blob.is_empty() {
      return Err(ZmqError::InvalidMessage(
        "First frame (destination identity) cannot be empty.".into(),
      ));
    }

    let core_opts_arc = self.core.core_state.read().options.clone(); // Guard dropped
    let timeout_opt: Option<Duration> = core_opts_arc.sndtimeo;
    let router_mandatory_opt: bool = core_opts_arc.router_mandatory;

    let target_endpoint_uri = match self
      .router_map_for_send
      .get_uri_for_identity(&destination_id_blob)
      .await
    {
      Some(uri) => uri,
      None => {
        return if router_mandatory_opt {
          Err(ZmqError::HostUnreachable(format!(
            "Peer {:?} not found (ROUTER_MANDATORY)",
            destination_id_blob
          )))
        } else {
          Ok(())
        };
      }
    };

    // Corrected lock scope for core_state_read
    let conn_iface_and_id_opt: Option<(Arc<dyn ISocketConnection>, usize)> = self
      .core
      .core_state
      .read() // Guard dropped
      .endpoints
      .get(&target_endpoint_uri)
      .map(|ep_info| (ep_info.connection_iface.clone(), ep_info.handle_id));

    let (conn_iface, conn_id) = match conn_iface_and_id_opt {
      Some((iface, id)) => (iface, id),
      None => {
        self
          .router_map_for_send
          .remove_peer_by_identity(&destination_id_blob)
          .await;
        return if router_mandatory_opt {
          Err(ZmqError::HostUnreachable(
            "Peer connection for identity disappeared".into(),
          ))
        } else {
          Ok(())
        };
      }
    };

    {
      // Short scope for current_send_target read lock
      let guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*guard {
        if active_info.target_endpoint_uri == target_endpoint_uri {
          tracing::debug!("ROUTER send_multipart for {} waiting for active fragmented send to same target to complete (permit system will handle).", target_endpoint_uri);
        }
      }
      // guard dropped
    }

    let _permit = self
      .pipe_send_coordinator
      .acquire_send_permit(conn_id, timeout_opt)
      .await
      .map_err(|e| {
        if router_mandatory_opt {
          e
        } else {
          ZmqError::Internal("Send silently dropped but permit error".into())
        }
      })?;

    destination_identity_frame.set_flags(MsgFlags::MORE);
    match conn_iface.send_message(destination_identity_frame).await {
      Ok(_) => {}
      Err(e) => return if router_mandatory_opt { Err(e) } else { Ok(()) },
    }

    let mut delimiter_frame = Msg::new();
    delimiter_frame.set_flags(MsgFlags::MORE);
    match conn_iface.send_message(delimiter_frame).await {
      Ok(_) => {}
      Err(e) => return if router_mandatory_opt { Err(e) } else { Ok(()) },
    }

    let num_payload_frames = frames.len();
    if num_payload_frames == 0 {
      let last_empty_frame = Msg::new();
      match conn_iface.send_message(last_empty_frame).await {
        Ok(_) => {}
        Err(e) => return if router_mandatory_opt { Err(e) } else { Ok(()) },
      }
    } else {
      for (i, mut frame) in frames.into_iter().enumerate() {
        if i < num_payload_frames - 1 {
          frame.set_flags(frame.flags() | MsgFlags::MORE);
        } else {
          frame.set_flags(frame.flags() & !MsgFlags::MORE);
        }
        match conn_iface.send_message(frame).await {
          Ok(_) => {}
          Err(e) => return if router_mandatory_opt { Err(e) } else { Ok(()) },
        }
      }
    }
    Ok(())
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
    self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await
  }

  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    if option == ROUTER_MANDATORY {
      update_core_option(&self.core, |options| {
        options.router_mandatory = parse_bool_option(value)?;
        Ok(())
      })?;
      Ok(())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = self.core_state_read().options.router_mandatory;
      Ok((val as i32).to_ne_bytes().to_vec())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { mut msg, .. } => {
        msg.metadata_mut().insert_typed(SourcePipeReadId(pipe_read_id)).await;
        if let Some(raw_zmtp_message_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          let app_logical_message = {
            let identity_map_guard = self.pipe_to_identity_shared_map.read();
            let router_ctx = RouterMessageProcessCtx { identity_map_guard };
            Self::process_raw_message_for_router(pipe_read_id, raw_zmtp_message_vec, router_ctx)?
          };
          self
            .incoming_orchestrator
            .queue_application_message_frames(pipe_read_id, app_logical_message)
            .await?;
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, peer_identity_opt: Option<&[u8]>) {
    let (endpoint_uri_opt, connection_id_opt) = {
      let core_s_read = self.core_state_read();
      let uri_opt = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
      let conn_id_opt = uri_opt
        .as_ref()
        .and_then(|uri| core_s_read.endpoints.get(uri).map(|ep_info| ep_info.handle_id));
      (uri_opt, conn_id_opt)
    };

    if let (Some(endpoint_uri), Some(connection_id)) = (endpoint_uri_opt, connection_id_opt) {
      let identity_to_use = match peer_identity_opt {
        Some(id_bytes) if !id_bytes.is_empty() => Blob::from_bytes(Bytes::copy_from_slice(id_bytes)),
        _ => Self::pipe_id_to_placeholder_identity(pipe_read_id),
      };

      tracing::debug!(handle = self.core.handle, pipe_read_id, %endpoint_uri, ?identity_to_use, "ROUTER attaching connection");
      self
        .router_map_for_send
        .add_peer(identity_to_use.clone(), pipe_read_id, endpoint_uri)
        .await;
      self
        .pipe_to_identity_shared_map
        .write()
        .insert(pipe_read_id, identity_to_use);
      self.pipe_send_coordinator.add_pipe(connection_id).await;
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER pipe_attached: Endpoint URI or Connection ID not found"
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, new_identity_opt: Option<Blob>) {
    let endpoint_uri_opt = self
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_opt {
      let new_identity = match new_identity_opt {
        Some(id) if !id.is_empty() => id,
        _ => Self::pipe_id_to_placeholder_identity(pipe_read_id),
      };
      tracing::debug!(handle = self.core.handle, pipe_read_id, new_identity = ?new_identity, "ROUTER updating peer identity");

      self
        .router_map_for_send
        .update_peer_identity(pipe_read_id, new_identity.clone(), &endpoint_uri)
        .await;
      self
        .pipe_to_identity_shared_map
        .write()
        .insert(pipe_read_id, new_identity);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER update_peer_identity: Endpoint URI not found"
      );
    }
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "ROUTER detaching pipe");

    let (endpoint_uri_opt, connection_id_opt) = {
      let core_s_read = self.core_state_read();
      let uri_opt = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
      let conn_id_opt = uri_opt
        .as_ref()
        .and_then(|uri| core_s_read.endpoints.get(uri).map(|ep_info| ep_info.handle_id));
      (uri_opt, conn_id_opt)
    };

    self.router_map_for_send.remove_peer_by_read_pipe(pipe_read_id).await;
    self.pipe_to_identity_shared_map.write().remove(&pipe_read_id);

    if let Some(conn_id) = connection_id_opt {
      if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(conn_id).await {
        semaphore.close();
      }

      // Lock current_send_target briefly to check and clear if necessary
      let mut active_frag_guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*active_frag_guard {
        if endpoint_uri_opt.as_deref() == Some(&active_info.target_endpoint_uri) {
          *active_frag_guard = None;
        }
      }
      // active_frag_guard dropped
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }
}
