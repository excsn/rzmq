use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::ROUTER_MANDATORY;
use crate::socket::patterns::incoming_orchestrator::IncomingMessageOrchestrator;
use crate::socket::patterns::{RouterMap, WritePipeCoordinator};
use crate::socket::ISocket;

use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::RwLockReadGuard as ParkingLotRwLockReadGuard;
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit};

use super::core::command_processor::update_core_option;
use super::parse_bool_option;

/// Structure to hold state for an ongoing fragmented send (identity part already sent).
#[derive(Debug)]
struct ActiveFragmentedSend {
  target_endpoint_uri: String,
  _permit: OwnedSemaphorePermit,
}

#[derive(Debug, Default)]
struct RouterRecvBuffer {
  identity: Option<Blob>,
  payload_frames: VecDeque<Msg>,
}

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map_for_send: RouterMap,
  incoming_orchestrator: IncomingMessageOrchestrator<(Blob, Vec<Msg>)>,
  pipe_to_identity_shared_map: Arc<DashMap<usize, Blob>>,
  current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
  current_recv_buffer: TokioMutex<RouterRecvBuffer>,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let rcvhwm = { core.core_state.read().options.rcvhwm };
    let orchestrator = IncomingMessageOrchestrator::new(core.handle, rcvhwm);

    Self {
      core,
      router_map_for_send: RouterMap::new(),
      incoming_orchestrator: orchestrator,
      pipe_to_identity_shared_map: Arc::new(DashMap::new()),
      current_send_target: TokioMutex::new(None),
      pipe_send_coordinator: Arc::new(WritePipeCoordinator::new()),
      current_recv_buffer: TokioMutex::new(RouterRecvBuffer::default()),
    }
  }

  // Removed core_state_read() helper, direct use core.core_state.read() in scopes

  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from_bytes(Bytes::from(format!("pipe:{}", pipe_read_id)))
  }

  fn process_incoming_zmtp_message(
    &self,
    pipe_read_id: usize,
    mut raw_zmtp_message: Vec<Msg>,
  ) -> Result<(Blob, Vec<Msg>), ZmqError> {
    let identity_blob = self
      .pipe_to_identity_shared_map
      .get(&pipe_read_id)
      .map(|entry| entry.value().clone())
      .unwrap_or_else(|| {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "Router: Identity for pipe {} not found in shared map, using placeholder for incoming message processing.",
          pipe_read_id
        );
        Self::pipe_id_to_placeholder_identity(pipe_read_id)
      });

    if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Router: Stripped empty ZMTP delimiter from incoming message."
      );
      raw_zmtp_message.remove(0);
    } else {
      tracing::debug!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "Router: Incoming ZMTP message from pipe did not start with an empty delimiter."
      );
    }
    Ok((identity_blob, raw_zmtp_message))
  }

  fn transform_qitem_to_app_frames(identity_blob: Blob, payload_frames_vec: Vec<Msg>) -> Vec<Msg> {
    let mut result_frames = Vec::with_capacity(1 + payload_frames_vec.len());
    let id_bytes = Bytes::copy_from_slice(identity_blob.as_ref());
    let mut id_msg = Msg::from_bytes(id_bytes);

    if !payload_frames_vec.is_empty() {
      id_msg.set_flags(MsgFlags::MORE);
    } else {
      id_msg.set_flags(id_msg.flags() & !MsgFlags::MORE);
    }
    result_frames.push(id_msg);
    result_frames.extend(payload_frames_vec);

    if let Some(last_frame) = result_frames.last_mut() {
      last_frame.set_flags(last_frame.flags() & !MsgFlags::MORE);
    } else if result_frames.is_empty() && !identity_blob.is_empty() {
      // This case implies payload_frames_vec was empty.
      // id_msg is already in result_frames with NOMORE flag set.
    }
    result_frames
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
    // First, delegate the close command to the core to start its shutdown.
    let core_close_result = delegate_to_core!(self, UserClose,);

    // Now, close the internal orchestrator to unblock any waiting recv() calls.
    // This is the crucial step that was missing.
    self.incoming_orchestrator.close().await;

    // Return the result from the core shutdown initiation.
    core_close_result
  }

  async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let (timeout_opt, router_mandatory_opt) = {
      let core_s_read = self.core.core_state.read();
      (
        core_s_read.options.sndtimeo,
        core_s_read.options.router_mandatory,
      )
    };

    let mut current_send_target_guard = self.current_send_target.lock().await;

    if let Some(active_info) = &*current_send_target_guard {
      let target_uri_for_payload = active_info.target_endpoint_uri.clone();
      let permit_exists = true;
      drop(current_send_target_guard);

      let conn_iface_for_payload: Option<Arc<dyn ISocketConnection>> = {
        let core_s_read = self.core.core_state.read();
        core_s_read
          .endpoints
          .get(&target_uri_for_payload)
          .map(|ep_info| ep_info.connection_iface.clone())
      };

      let conn_iface = match conn_iface_for_payload {
        Some(iface) => iface,
        None => {
          if permit_exists {
            let mut clear_target_guard_on_err = self.current_send_target.lock().await;
            *clear_target_guard_on_err = None;
          }
          return if router_mandatory_opt {
            Err(ZmqError::HostUnreachable(
              "Peer for fragmented send disappeared".into(),
            ))
          } else {
            Ok(())
          };
        }
      };

      let is_last_user_part = !msg.is_more();
      let send_result = conn_iface.send_message(msg).await;

      if is_last_user_part && permit_exists {
        let mut clear_target_guard_on_done = self.current_send_target.lock().await;
        *clear_target_guard_on_done = None;
      }

      return match send_result {
        Ok(()) => Ok(()),
        Err(e) => {
          if !is_last_user_part && permit_exists {
            let mut clear_target_guard_on_err_payload = self.current_send_target.lock().await;
            *clear_target_guard_on_err_payload = None;
          }
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
      if !msg.is_more() {
        drop(current_send_target_guard);
        return Err(ZmqError::InvalidMessage(
          "ROUTER send: First frame (identity) must have MORE flag set by application".into(),
        ));
      }
      let destination_id = Blob::from_bytes(msg.data_bytes().unwrap_or_default());
      if destination_id.is_empty() {
        drop(current_send_target_guard);
        return Err(ZmqError::InvalidMessage(
          "ROUTER send: Identity frame cannot be empty".into(),
        ));
      }
      drop(current_send_target_guard);

      let target_endpoint_uri = match self
        .router_map_for_send
        .get_uri_for_identity(&destination_id)
        .await
      {
        Some(uri) => uri,
        None => {
          return if router_mandatory_opt {
            Err(ZmqError::HostUnreachable(format!(
              "Peer {:?} not found (ROUTER_MANDATORY)",
              destination_id
            )))
          } else {
            Ok(())
          };
        }
      };

      let (conn_iface_opt, conn_id_opt) = {
        let core_s_read_guard = self.core.core_state.read(); // Guard active
        let result = core_s_read_guard
          .endpoints
          .get(&target_endpoint_uri)
          .map_or((None, None), |ep_info| {
            (
              Some(ep_info.connection_iface.clone()),
              Some(ep_info.handle_id),
            )
          });
        // Guard dropped when core_s_read_guard goes out of scope here
        result
      };

      let (conn_iface, conn_id) = match (conn_iface_opt, conn_id_opt) {
        (Some(iface), Some(id)) => (iface, id),
        _ => {
          self
            .router_map_for_send
            .remove_peer_by_identity(&destination_id)
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

      let permit = self
        .pipe_send_coordinator
        .acquire_send_permit(conn_id, timeout_opt)
        .await?;

      let mut set_target_guard = self.current_send_target.lock().await;

      match conn_iface.send_message(msg).await {
        Ok(()) => {
          let mut delimiter_frame = Msg::new();
          delimiter_frame.set_flags(MsgFlags::MORE);
          match conn_iface.send_message(delimiter_frame).await {
            Ok(()) => {
              *set_target_guard = Some(ActiveFragmentedSend {
                target_endpoint_uri,
                _permit: permit,
              });
              Ok(())
            }
            Err(e) => {
              drop(set_target_guard);
              drop(permit);
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
          drop(set_target_guard);
          drop(permit);
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
    let rcvtimeo_opt = { self.core.core_state.read().options.rcvtimeo };
    self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, |(identity_blob, payload_frames_vec)| {
        Self::transform_qitem_to_app_frames(identity_blob, payload_frames_vec)
      })
      .await
  }

  async fn send_multipart(&self, mut frames: Vec<Msg>) -> Result<(), ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if frames.is_empty() {
      return Err(ZmqError::InvalidMessage(
        "ROUTER send_multipart requires at least an identity frame.".into(),
      ));
    }
    if frames.len() < 2 && !(frames.len() == 1 && frames[0].data().map_or(false, |d| d.is_empty()))
    {
      if frames.len() == 1 {
        if frames[0].is_more() {
          return Err(ZmqError::InvalidMessage(
            "ROUTER send_multipart: Single identity frame must not have MORE flag if no payload."
              .into(),
          ));
        }
      } else {
        return Err(ZmqError::InvalidMessage("ROUTER send_multipart requires at least identity and one payload frame, or just an identity frame for an empty message.".into()));
      }
    }

    let mut destination_identity_msg = frames.remove(0);
    let destination_id_blob =
      Blob::from_bytes(destination_identity_msg.data_bytes().unwrap_or_default());

    if destination_id_blob.is_empty() {
      return Err(ZmqError::InvalidMessage(
        "ROUTER send_multipart: Identity frame cannot be empty".into(),
      ));
    }

    let (timeout_opt, router_mandatory_opt) = {
      let core_s_read = self.core.core_state.read();
      (
        core_s_read.options.sndtimeo,
        core_s_read.options.router_mandatory,
      )
    };

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

    let conn_info = {
      let core_s_read = self.core.core_state.read(); // Guard active
      let result = core_s_read
        .endpoints
        .get(&target_endpoint_uri)
        .map(|ep_info| (ep_info.connection_iface.clone(), ep_info.handle_id));
      // Guard dropped when core_s_read goes out of scope here
      result
    };

    let (conn_iface, conn_id) = match conn_info {
      // Use the extracted values
      Some((iface, id)) => (iface, id),
      None => {
        // Peer connection disappeared after URI lookup but before getting EndpointInfo
        // No RwLockReadGuard held here for this async call
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
      let guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*guard {
        if active_info.target_endpoint_uri == target_endpoint_uri {
          tracing::debug!(handle = self.core.handle, uri = %target_endpoint_uri, "ROUTER send_multipart to same target as active fragmented send. Permit system will serialize.");
        }
      }
    }

    let _permit = self
      .pipe_send_coordinator
      .acquire_send_permit(conn_id, timeout_opt)
      .await
      .map_err(|e| {
        if router_mandatory_opt {
          e
        } else {
          ZmqError::Internal("Send dropped due to permit error (non-mandatory)".into())
        }
      })?;

    let mut zmtp_wire_frames: Vec<Msg> = Vec::with_capacity(frames.len() + 2);
    destination_identity_msg.set_flags(destination_identity_msg.flags() | MsgFlags::MORE);
    zmtp_wire_frames.push(destination_identity_msg);

    let mut delimiter_frame = Msg::new();
    if !frames.is_empty() {
      delimiter_frame.set_flags(MsgFlags::MORE);
    } else {
      delimiter_frame.set_flags(delimiter_frame.flags() & !MsgFlags::MORE);
    }
    zmtp_wire_frames.push(delimiter_frame);

    let num_payload_frames = frames.len();
    for (i, mut frame) in frames.into_iter().enumerate() {
      if i < num_payload_frames - 1 {
        frame.set_flags(frame.flags() | MsgFlags::MORE);
      } else {
        frame.set_flags(frame.flags() & !MsgFlags::MORE);
      }
      zmtp_wire_frames.push(frame);
    }

    match conn_iface.send_multipart(zmtp_wire_frames).await {
      Ok(()) => Ok(()),
      Err(e) => {
        if router_mandatory_opt {
          Err(if matches!(e, ZmqError::ConnectionClosed) {
            ZmqError::HostUnreachable("Peer disconnected during multipart send".into())
          } else {
            e
          })
        } else {
          Ok(())
        }
      }
    }
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt = { self.core.core_state.read().options.rcvtimeo };
    self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, |(identity_blob, payload_frames_vec)| {
        Self::transform_qitem_to_app_frames(identity_blob, payload_frames_vec)
      })
      .await
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
      let val = { self.core.core_state.read().options.router_mandatory };
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
      Command::PipeMessageReceived { msg, .. } => {
        if let Some(raw_zmtp_message_vec) = self
          .incoming_orchestrator
          .accumulate_pipe_frame(pipe_read_id, msg)?
        {
          match self.process_incoming_zmtp_message(pipe_read_id, raw_zmtp_message_vec) {
            Ok((identity_blob, payload_only_vec)) => {
              self
                .incoming_orchestrator
                .queue_item(pipe_read_id, (identity_blob, payload_only_vec))
                .await?;
            }
            Err(e) => {
              tracing::error!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "Router: Error processing incoming ZMTP message: {}. Dropped.",
                e
              );
            }
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    peer_identity_opt: Option<&[u8]>,
  ) {
    let (endpoint_uri_opt, connection_id_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri_opt = core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();
      let conn_id_opt = uri_opt.as_ref().and_then(|uri| {
        core_s_read
          .endpoints
          .get(uri)
          .map(|ep_info| ep_info.handle_id)
      });
      (uri_opt, conn_id_opt)
    };

    if let (Some(endpoint_uri), Some(connection_id)) = (endpoint_uri_opt, connection_id_opt) {
      let identity_to_use = match peer_identity_opt {
        Some(id_bytes) if !id_bytes.is_empty() => {
          Blob::from_bytes(Bytes::copy_from_slice(id_bytes))
        }
        _ => Self::pipe_id_to_placeholder_identity(pipe_read_id),
      };

      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, ?identity_to_use, conn_id = connection_id, "ROUTER attaching connection");
      self
        .router_map_for_send
        .add_peer(identity_to_use.clone(), pipe_read_id, endpoint_uri)
        .await;
      self
        .pipe_to_identity_shared_map
        .insert(pipe_read_id, identity_to_use);
      self.pipe_send_coordinator.add_pipe(connection_id).await;
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER pipe_attached: Endpoint URI or Connection ID not found. Maps not fully updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, new_identity_opt: Option<Blob>) {
    let endpoint_uri_opt = {
      self
        .core
        .core_state
        .read()
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned()
    };

    if let Some(endpoint_uri) = endpoint_uri_opt {
      let new_identity = match new_identity_opt {
        Some(id) if !id.is_empty() => id,
        _ => {
          tracing::warn!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "ROUTER update_peer_identity: No valid new ZMTP identity provided, using placeholder.");
          Self::pipe_id_to_placeholder_identity(pipe_read_id)
        }
      };
      tracing::debug!(handle = self.core.handle, pipe_read_id, new_identity = ?new_identity, "ROUTER updating peer identity");

      self
        .router_map_for_send
        .update_peer_identity(pipe_read_id, new_identity.clone(), &endpoint_uri)
        .await;
      self
        .pipe_to_identity_shared_map
        .insert(pipe_read_id, new_identity);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER update_peer_identity: Endpoint URI not found for pipe. Cannot update identity maps."
      );
    }
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "ROUTER detaching pipe"
    );

    let (endpoint_uri_opt, connection_id_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri_opt = core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();
      let conn_id_opt = uri_opt
        .as_ref()
        .and_then(|uri| core_s_read.endpoints.get(uri))
        .map(|ep_info| ep_info.handle_id);
      (uri_opt, conn_id_opt)
    };

    self
      .router_map_for_send
      .remove_peer_by_read_pipe(pipe_read_id)
      .await;
    self.pipe_to_identity_shared_map.remove(&pipe_read_id);

    if let Some(conn_id) = connection_id_opt {
      if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(conn_id).await {
        semaphore.close();
      }

      let mut active_frag_guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*active_frag_guard {
        if endpoint_uri_opt.as_deref() == Some(&active_info.target_endpoint_uri) {
          *active_frag_guard = None;
        }
      }
    }

    self
      .incoming_orchestrator
      .clear_pipe_state(pipe_read_id)
      .await;
  }
}
