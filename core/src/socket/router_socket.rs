use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::SocketCore;
use crate::socket::options::{AUTO_DELIMITER, ROUTER_MANDATORY};
use crate::socket::patterns::AddressedIngressEngine;
use crate::socket::patterns::ready_pipe_queue::{PeerTag, PipeMessageSender};
use crate::socket::patterns::{FramingLatch, RouterMap, WritePipeCoordinator, router_auto_encode};

use parking_lot::{Mutex as ParkingMutex, RwLock as ParkingRwLock};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit};

use super::core::command_processor::update_core_option;
use super::parse_bool_option;

/// Structure to hold state for an ongoing fragmented send (identity part already sent).
#[derive(Debug)]
struct ActiveFragmentedSend {
  target_endpoint_uri: String,
  _permit: OwnedSemaphorePermit,
}

/// A pipe's routing identity and the socket type its peer announced.
#[derive(Debug)]
struct PeerIdentityState {
  identity: Blob,
  peer_socket_type: Option<String>,
}

const PEER_KIND_OTHER: u8 = 0;
const PEER_KIND_REQ_OR_DEALER: u8 = 1;
const PEER_KIND_ROUTER: u8 = 2;

fn peer_kind_of(socket_type: Option<&str>) -> u8 {
  match socket_type {
    Some("REQ") | Some("DEALER") => PEER_KIND_REQ_OR_DEALER,
    Some("ROUTER") => PEER_KIND_ROUTER,
    _ => PEER_KIND_OTHER,
  }
}

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map_for_send: RouterMap,
  ingress_engine: AddressedIngressEngine,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
  frame_recv_buffer: ParkingMutex<Option<VecDeque<Msg>>>,
  current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
  framing: FramingLatch,
  /// Per-pipe identity, keyed by `pipe_read_id`. A pipe's ingress slot stays
  /// gated until its identity is final, then carries a copy of it with every
  /// message, so receiving never reads this map.
  peer_states: ParkingRwLock<HashMap<usize, PeerIdentityState>>,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let (max_conn, rcvbatch_count) = {
      let opts = &core.core_state.read().options;
      (opts.max_connections.unwrap_or(1024), opts.rcvbatch_count)
    };
    Self {
      core,
      router_map_for_send: RouterMap::new(),
      ingress_engine: AddressedIngressEngine::new(max_conn, rcvbatch_count),
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
      frame_recv_buffer: ParkingMutex::new(None),
      current_send_target: TokioMutex::new(None),
      pipe_send_coordinator: Arc::new(WritePipeCoordinator::new()),
      framing: FramingLatch::new(router_auto_encode),
      peer_states: ParkingRwLock::new(HashMap::new()),
    }
  }

  /// Opens a pipe's ingress slot with its identity. Idempotent: `pipe_attached`
  /// (io-uring, inproc) and `update_peer_identity` may both call it, and a pipe
  /// with no identity entry gets the placeholder.
  fn finalize_pipe(&self, pipe_read_id: usize) {
    let tag = {
      let mut states = self.peer_states.write();
      let state = states.entry(pipe_read_id).or_insert_with(|| PeerIdentityState {
        identity: Self::pipe_id_to_placeholder_identity(pipe_read_id),
        peer_socket_type: None,
      });
      PeerTag {
        identity: state.identity.clone(),
        kind: peer_kind_of(state.peer_socket_type.as_deref()),
      }
    };
    if let Some(sender) = self.pending_pipe_senders.lock().get(&pipe_read_id) {
      sender.open_gate(tag);
    }
  }

  /// Next message with the identity its pipe had when the message was queued.
  async fn recv_tagged(
    &self,
    rcvtimeo_opt: Option<std::time::Duration>,
  ) -> Result<(usize, FrameBatch, Option<PeerTag>), ZmqError> {
    match rcvtimeo_opt {
      Some(d) if d.is_zero() => self
        .ingress_engine
        .try_pop_tagged()
        .ok_or(ZmqError::ResourceLimitReached),
      Some(d) => tokio::time::timeout(d, self.ingress_engine.pop_tagged())
        .await
        .map_err(|_| ZmqError::Timeout)?,
      None => self.ingress_engine.pop_tagged().await,
    }
  }

  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from_bytes(Bytes::from(format!("pipe:{}", pipe_read_id)))
  }

  fn process_incoming_zmtp_message(
    &self,
    pipe_read_id: usize,
    mut raw_zmtp_message: FrameBatch,
    tag: Option<PeerTag>,
  ) -> Result<(Blob, FrameBatch), ZmqError> {
    // The application should receive [sender_identity, payload...].
    // The 'payload' is the entire message received from the peer, after stripping
    // a single leading delimiter IF the peer is a REQ or DEALER.

    let (identity_blob, kind) = match tag {
      Some(tag) => (tag.identity, tag.kind),
      None => (Self::pipe_id_to_placeholder_identity(pipe_read_id), PEER_KIND_OTHER),
    };

    if self.framing.is_manual() {
      return Ok((identity_blob, raw_zmtp_message));
    }

    match kind {
      PEER_KIND_REQ_OR_DEALER => {
        if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
          raw_zmtp_message.remove(0);
        } else {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "ROUTER: Expected empty delimiter from REQ/DEALER peer, but not found."
          );
        }
      }
      PEER_KIND_ROUTER => {
        // Do nothing. The entire message is the payload from a ROUTER peer.
      }
      _ => {
        // Peer didn't announce a type: strip a leading delimiter if present.
        if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
          raw_zmtp_message.remove(0);
        }
      }
    }

    Ok((identity_blob, raw_zmtp_message))
  }

  fn transform_qitem_to_app_frames(identity_blob: Blob, payload_frames_vec: FrameBatch) -> FrameBatch {
    let mut result_frames = FrameBatch::with_capacity(1 + payload_frames_vec.len());
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
    }
    result_frames
  }
}

#[async_trait]
impl ISocket for RouterSocket {
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

  async fn send(&self, msg: Msg) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let (timeout_opt, router_mandatory_opt) = {
      let core_s_read = self.core.core_state.read();
      (
        core_s_read.options.sndtimeo,
        core_s_read.options.router_mandatory,
      )
    };

    let current_send_target_guard = self.current_send_target.lock().await;

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
        .get_peer_info_for_identity(&destination_id)
        .await
      {
        Some(info) => info.uri,
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

      let (conn_iface_opt, pipe_read_id_opt) = {
        let core_s_read_guard = self.core.core_state.read();
        let result = core_s_read_guard
          .endpoints
          .get(&target_endpoint_uri)
          .map_or((None, None), |ep_info| {
            (
              Some(ep_info.connection_iface.clone()),
              ep_info.pipe_ids.map(|(_, read_id)| read_id),
            )
          });
        result
      };

      let (conn_iface, pipe_read_id) = match (conn_iface_opt, pipe_read_id_opt) {
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
        .acquire_send_permit(pipe_read_id, timeout_opt)
        .await?;

      let mut set_target_guard = self.current_send_target.lock().await;

      match conn_iface.send_message(msg).await {
        Ok(()) => {
          let delimiter_result = if !self.framing.is_manual() {
            let mut delimiter_frame = Msg::new();
            delimiter_frame.set_flags(MsgFlags::MORE);
            conn_iface.send_message(delimiter_frame).await
          } else {
            Ok(())
          };
          match delimiter_result {
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    // Fast path: serve from frame buffer (mid-multipart delivery).
    {
      let mut guard = self.frame_recv_buffer.lock();
      if let Some(ref mut frames) = *guard {
        if let Some(frame) = frames.pop_front() {
          if frames.is_empty() { *guard = None; }
          return Ok(frame);
        }
        *guard = None;
      }
    }
    // Slow path: fetch next complete raw batch, process, return first frame.
    let rcvtimeo_opt = self.core.core_state.read().options.rcvtimeo;
    let (pipe_read_id, raw_batch, tag) = self.recv_tagged(rcvtimeo_opt).await?;
    let (identity_blob, payload) = self.process_incoming_zmtp_message(pipe_read_id, raw_batch, tag)?;
    let mut app_frames = Self::transform_qitem_to_app_frames(identity_blob, payload);
    if app_frames.is_empty() {
      return Ok(Msg::new());
    }
    if app_frames.len() == 1 {
      return Ok(app_frames.remove(0));
    }
    let mut deque: VecDeque<Msg> = app_frames.into_iter().collect();
    let first = deque.pop_front().unwrap();
    *self.frame_recv_buffer.lock() = Some(deque);
    Ok(first)
  }

  async fn send_multipart(&self, mut frames: FrameBatch) -> Result<(), ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if frames.is_empty() {
      return Err(ZmqError::InvalidMessage(
        "ROUTER send_multipart requires at least an identity frame.".into(),
      ));
    }

    // The first frame is the destination identity.
    let destination_identity_msg = frames.remove(0);
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

    // 1. Find the peer's info (URI and strategy) using the identity.
    // This is an async call but uses internal, non-Tokio locks.
    let peer_info = match self
      .router_map_for_send
      .get_peer_info_for_identity(&destination_id_blob)
      .await
    {
      Some(info) => info,
      None => {
        return if router_mandatory_opt {
          Err(ZmqError::HostUnreachable(format!(
            "Peer with identity {:?} not found (ROUTER_MANDATORY)",
            destination_id_blob
          )))
        } else {
          // Silently drop the message.
          Ok(())
        };
      }
    };

    // 2. Look up the connection interface and pipe_read_id using the URI from PeerInfo.
    let (conn_iface_opt, pipe_read_id_opt) = {
      let core_s_read = self.core.core_state.read();
      core_s_read
        .endpoints
        .get(&peer_info.uri)
        .map_or((None, None), |ep_info| {
          (
            Some(ep_info.connection_iface.clone()),
            ep_info.pipe_ids.map(|(_, read_id)| read_id),
          )
        })
    };

    let (conn_iface, pipe_read_id) = match (conn_iface_opt, pipe_read_id_opt) {
      (Some(iface), Some(id)) => (iface, id),
      _ => {
        // Stale entry: peer is in RouterMap but its EndpointInfo is gone.
        self
          .router_map_for_send
          .remove_peer_by_identity(&destination_id_blob)
          .await;
        return if router_mandatory_opt {
          Err(ZmqError::HostUnreachable(
            "Peer connection for identity disappeared before send".into(),
          ))
        } else {
          Ok(())
        };
      }
    };

    // All locks are released before we hit the first .await below.

    // 3. Acquire a send permit for this specific pipe.
    let _permit = self
      .pipe_send_coordinator
      .acquire_send_permit(pipe_read_id, timeout_opt)
      .await
      .map_err(|e| {
        if router_mandatory_opt {
          e
        } else {
          ZmqError::Internal("Send dropped due to permit error (non-mandatory)".into())
        }
      })?;

    // 4. Prepare the final wire frames using the peer-specific strategy.
    let mut zmtp_wire_frames =
      peer_info
        .strategy
        .prepare_wire_frames(destination_identity_msg, frames, &self.framing);

    if let Some(last_frame) = zmtp_wire_frames.last_mut() {
      last_frame.set_flags(last_frame.flags() & !MsgFlags::MORE);
    }

    // 5. Send the message.
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
          // Silently drop on error if not mandatory.
          Ok(())
        }
      }
    }
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt = self.core.core_state.read().options.rcvtimeo;
    let (pipe_read_id, raw_batch, tag) = self.recv_tagged(rcvtimeo_opt).await?;
    let (identity_blob, payload) = self.process_incoming_zmtp_message(pipe_read_id, raw_batch, tag)?;
    Ok(Self::transform_qitem_to_app_frames(identity_blob, payload))
  }

  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    if option == ROUTER_MANDATORY {
      update_core_option(&self.core, |options| {
        options.router_mandatory = parse_bool_option(value)?;
        Ok(())
      })?;
      Ok(())
    } else if option == AUTO_DELIMITER {
      if !parse_bool_option(value)? {
        self.framing.set_manual();
      }
      Ok(())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    if option == ROUTER_MANDATORY {
      let val = { self.core.core_state.read().options.router_mandatory };
      Ok((val as i32).to_ne_bytes().to_vec())
    } else if option == AUTO_DELIMITER {
      Ok((!self.framing.is_manual() as i32).to_ne_bytes().to_vec())
    } else {
      Err(ZmqError::UnsupportedOption(option))
    }
  }

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {
    match command {
      Command::Stop => {
        self.ingress_engine.close();
      }
      _ => return Ok(false),
    }

    Ok(true)
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().get(&pipe_read_id).cloned()
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
      let is_inproc = endpoint_uri.starts_with("inproc://");
      self
        .router_map_for_send
        .add_peer(identity_to_use.clone(), pipe_read_id, endpoint_uri)
        .await;
      self.peer_states.write().insert(
        pipe_read_id,
        PeerIdentityState {
          identity: identity_to_use,
          peer_socket_type: None,
        },
      );
      self.pipe_send_coordinator.add_pipe(pipe_read_id).await;

      // Register per-pipe ingress channel.
      let (rcvhwm, rcvbatch_count) = {
        let opts = self.core.core_state.read();
        (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
      };
      let sender = self.ingress_engine.register_pipe_gated(pipe_read_id, rcvhwm, rcvbatch_count, true, None);
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);

      // If a real identity is known at attach time (io-uring's atomic
      // post-handshake attach), the pipe is finalized immediately. A
      // placeholder/`None` attach (standard pre-handshake path) leaves the pipe
      // pending until `update_peer_identity` finalizes it.
      // Direct inproc connections have no handshake, so they are finalized immediately.
      if matches!(peer_identity_opt, Some(id) if !id.is_empty()) || is_inproc {
        self.finalize_pipe(pipe_read_id);
      }
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER pipe_attached: Endpoint URI or Connection ID not found. Maps not fully updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, new_identity_opt: Option<Blob>) {
    let (endpoint_uri_opt, peer_socket_type_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri_opt = core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();

      let type_opt = if let Some(ref uri) = uri_opt {
        core_s_read
          .endpoints
          .get(uri)
          .and_then(|ep_info| ep_info.peer_socket_type.clone())
      } else {
        None
      };
      (uri_opt, type_opt)
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
        .update_peer_identity(
          pipe_read_id,
          new_identity.clone(),
          &endpoint_uri,
          peer_socket_type_opt.as_deref(),
        )
        .await;
      self.peer_states.write().insert(
        pipe_read_id,
        PeerIdentityState {
          identity: new_identity,
          peer_socket_type: peer_socket_type_opt,
        },
      );
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "ROUTER update_peer_identity: Endpoint URI not found for pipe. Cannot update identity maps."
      );
    }

    // Universal post-handshake signal; also fires on the warn path above so the
    // gate cannot stick if the maps are transiently inconsistent.
    self.finalize_pipe(pipe_read_id);
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

    self.pipe_send_coordinator.remove_pipe(pipe_read_id).await;

    if connection_id_opt.is_some() {
      let mut active_frag_guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*active_frag_guard {
        if endpoint_uri_opt.as_deref() == Some(&active_info.target_endpoint_uri) {
          *active_frag_guard = None;
        }
      }
    }

    self.pending_pipe_senders.lock().remove(&pipe_read_id);

    // A finalized pipe's slot carries its identity, so batches it already
    // queued are still delivered after the peer disconnects, as libzmq does. A
    // slot still gated has no ready token and drops with its batches: they can
    // never be routed with a real identity (ZMQ permits loss on disconnect).
    self.ingress_engine.detach_pipe(pipe_read_id);
    self.peer_states.write().remove(&pipe_read_id);
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::context::Context;
  use crate::runtime::mailbox;
  use crate::socket::core::CoreState;
  use crate::socket::core::state::ShutdownCoordinator;
  use crate::socket::options::SocketOptions;
  use crate::socket::types::SocketType;
  use std::sync::atomic::{AtomicBool, Ordering};
  use std::time::Duration;
  use tokio::sync::RwLock as TokioRwLock;
  use tokio_util::sync::CancellationToken;

  /// A ROUTER over a SocketCore whose command loop is never spawned, with
  /// RCVTIMEO 0 so an empty recv returns at once.
  fn router() -> Arc<RouterSocket> {
    let mut options = SocketOptions::default();
    options.rcvtimeo = Some(Duration::ZERO);
    let (command_sender, _command_receiver) = mailbox(16);
    let core = Arc::new(SocketCore {
      handle: 1,
      context: Context::new().expect("context"),
      command_sender,
      core_state: ParkingRwLock::new(CoreState::new(1, SocketType::Router, options)),
      socket_logic: TokioRwLock::new(None),
      shutdown_coordinator: TokioMutex::new(ShutdownCoordinator::default()),
      is_running_flag: AtomicBool::new(true),
      closing: CancellationToken::new(),
    });
    Arc::new(RouterSocket::new(core))
  }

  /// Registers `pipe_id` the way `pipe_attached` does, finalized or still
  /// waiting for the handshake identity.
  fn attach(router: &RouterSocket, pipe_id: usize, finalized: bool) -> PipeMessageSender {
    let sender = router.ingress_engine.register_pipe_gated(pipe_id, 16, 16, true, None);
    router.pending_pipe_senders.lock().insert(pipe_id, sender.clone());
    router.peer_states.write().insert(
      pipe_id,
      PeerIdentityState {
        identity: RouterSocket::pipe_id_to_placeholder_identity(pipe_id),
        peer_socket_type: None,
      },
    );
    if finalized {
      router.finalize_pipe(pipe_id);
    }
    sender
  }

  /// `attach` with a real identity in place of the placeholder.
  fn attach_as(router: &RouterSocket, pipe_id: usize, finalized: bool, identity: &'static [u8]) -> PipeMessageSender {
    let sender = attach(router, pipe_id, false);
    if finalized {
      finalize_as(router, pipe_id, identity);
    }
    sender
  }

  /// Finalizes a pending pipe with `identity`, as `update_peer_identity` does.
  fn finalize_as(router: &RouterSocket, pipe_id: usize, identity: &'static [u8]) {
    router.peer_states.write().get_mut(&pipe_id).expect("state").identity = Blob::from_bytes(Bytes::from_static(identity));
    router.finalize_pipe(pipe_id);
  }

  /// Batches the ROUTER still holds for `pipe_id` outside its ingress queue.
  fn retained_batches(router: &RouterSocket, pipe_id: usize) -> usize {
    router.ingress_engine.cached_for(pipe_id)
  }

  async fn push(sender: &PipeMessageSender, payload: &'static [u8]) {
    let mut batch = FrameBatch::new();
    batch.push(Msg::from_static(payload));
    sender.send(batch).await.expect("pipe send");
  }

  async fn drain(router: &RouterSocket) -> Vec<Vec<u8>> {
    let mut got = Vec::new();
    while let Ok(frames) = router.recv_multipart().await {
      got.push(frames.last().and_then(|f| f.data()).unwrap_or_default().to_vec());
    }
    got
  }

  #[tokio::test]
  async fn finalized_pipe_batches_are_delivered_in_order_after_detach() {
    let router = router();
    let sender = attach(&router, 7, true);
    for payload in [b"1", b"2", b"3"] {
      push(&sender, payload).await;
    }
    drop(sender);
    router.pipe_detached(7).await;
    assert_eq!(drain(&router).await, [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    assert!(!router.peer_states.read().contains_key(&7));
  }

  #[tokio::test]
  async fn finalized_pipe_with_nothing_queued_is_released_at_detach() {
    let router = router();
    drop(attach(&router, 7, true));
    router.pipe_detached(7).await;
    assert!(!router.peer_states.read().contains_key(&7));
  }

  #[tokio::test]
  async fn unfinalized_pipe_batches_are_dropped_at_detach() {
    let router = router();
    let sender = attach(&router, 7, false);
    push(&sender, b"1").await;
    push(&sender, b"2").await;
    drop(sender);
    router.pipe_detached(7).await;

    assert!(drain(&router).await.is_empty());
    assert_eq!(retained_batches(&router, 7), 0);
    assert!(!router.peer_states.read().contains_key(&7));
  }

  #[tokio::test]
  async fn unfinalized_pipe_batches_wait_unseen_and_a_late_finalize_does_not_release_them() {
    let router = router();
    let sender = attach(&router, 7, false);
    push(&sender, b"1").await;
    drop(sender);
    assert!(drain(&router).await.is_empty(), "a pending pipe's batch was delivered");

    router.pipe_detached(7).await;
    router.finalize_pipe(7);
    assert!(drain(&router).await.is_empty(), "a detached pending pipe's batch was delivered");
    assert_eq!(retained_batches(&router, 7), 0);
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn concurrent_recv_on_detached_pipe_delivers_each_batch_once_and_releases_state() {
    const PAYLOADS: [&[u8]; 16] = [
      b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"10", b"11", b"12", b"13", b"14", b"15",
    ];
    for _ in 0..50 {
      let router = router();
      let sender = attach(&router, 7, true);
      for payload in PAYLOADS {
        push(&sender, payload).await;
      }
      drop(sender);
      router.pipe_detached(7).await;

      let tasks: Vec<_> = (0..4)
        .map(|_| {
          let router = router.clone();
          tokio::spawn(async move { drain(&router).await })
        })
        .collect();
      let mut got = Vec::new();
      for task in tasks {
        got.extend(task.await.expect("recv task"));
      }
      got.sort();
      let mut want: Vec<Vec<u8>> = PAYLOADS.iter().map(|p| p.to_vec()).collect();
      want.sort();
      assert_eq!(got, want);
      assert!(!router.peer_states.read().contains_key(&7));
    }
  }
  const SIXTEEN: [&[u8]; 16] = [
    b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8", b"9", b"10", b"11", b"12", b"13", b"14", b"15",
  ];

  /// Four RCVTIMEO 0 consumers drain `router` until `detached` is set and the
  /// queue has stayed empty, returning (identity frame, payload) per message.
  async fn drain_concurrently(router: &Arc<RouterSocket>, detached: &Arc<AtomicBool>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let consumers: Vec<_> = (0..4)
      .map(|_| {
        let router = router.clone();
        let detached = detached.clone();
        tokio::spawn(async move {
          let mut got = Vec::new();
          let mut empty_after_detach = 0;
          loop {
            match router.recv_multipart().await {
              Ok(frames) => got.push((
                frames[0].data().unwrap_or_default().to_vec(),
                frames.last().and_then(|f| f.data()).unwrap_or_default().to_vec(),
              )),
              Err(_) => {
                if detached.load(Ordering::SeqCst) {
                  empty_after_detach += 1;
                  if empty_after_detach > 3 {
                    break;
                  }
                }
                tokio::task::yield_now().await;
              }
            }
          }
          got
        })
      })
      .collect();
    let mut got = Vec::new();
    for consumer in consumers {
      got.extend(consumer.await.expect("consumer"));
    }
    got
  }

  fn spin(iteration: usize) {
    for _ in 0..(iteration * 7919) % 2000 {
      std::hint::spin_loop();
    }
  }

  /// Batches popped while their pipe detaches are each delivered once, with the
  /// pipe's identity.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn batches_popped_while_their_pipe_detaches_keep_its_identity() {
    let (mut lost, mut misrouted) = (0, 0);
    for iteration in 0..500 {
      let router = router();
      let sender = attach_as(&router, 7, true, b"peer7");
      for payload in SIXTEEN {
        push(&sender, payload).await;
      }
      drop(sender);
      let detached = Arc::new(AtomicBool::new(false));
      let draining = tokio::spawn({
        let router = router.clone();
        let detached = detached.clone();
        async move { drain_concurrently(&router, &detached).await }
      });
      spin(iteration);
      router.pipe_detached(7).await;
      detached.store(true, Ordering::SeqCst);
      let mut got = draining.await.expect("drain");
      got.extend(drain(&router).await.into_iter().map(|p| (b"peer7".to_vec(), p)));
      lost += SIXTEEN.len().saturating_sub(got.len());
      misrouted += got.iter().filter(|(id, _)| id.as_slice() != b"peer7").count();
    }
    assert_eq!((lost, misrouted), (0, 0), "(lost, misrouted) over 500 detaches");
  }

  /// A pending pipe that detaches while consumers pop it delivers nothing and
  /// leaves no batch behind.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn a_pending_pipe_detached_during_recv_retains_nothing() {
    let (mut delivered, mut retained) = (0, 0);
    for iteration in 0..500 {
      let router = router();
      let sender = attach(&router, 7, false);
      for payload in SIXTEEN {
        push(&sender, payload).await;
      }
      drop(sender);
      let detached = Arc::new(AtomicBool::new(false));
      let draining = tokio::spawn({
        let router = router.clone();
        let detached = detached.clone();
        async move { drain_concurrently(&router, &detached).await }
      });
      spin(iteration);
      router.pipe_detached(7).await;
      detached.store(true, Ordering::SeqCst);
      delivered += draining.await.expect("drain").len();
      retained += retained_batches(&router, 7);
    }
    assert_eq!((delivered, retained), (0, 0), "(delivered, retained) over 500 detaches");
  }

  /// Batches that waited for the pipe's identity keep it when the pipe detaches
  /// before they are read.
  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn batches_held_before_finalize_keep_their_identity_after_detach() {
    let (mut lost, mut misrouted) = (0, 0);
    for _ in 0..2000 {
      let router = router();
      let sender = attach(&router, 7, false);
      for payload in SIXTEEN {
        push(&sender, payload).await;
      }
      drop(sender);
      assert!(drain(&router).await.is_empty(), "a pending pipe's batch was delivered");
      finalize_as(&router, 7, b"peer7");
      router.pipe_detached(7).await;
      let detached = Arc::new(AtomicBool::new(true));
      let mut got = drain_concurrently(&router, &detached).await;
      got.extend(drain(&router).await.into_iter().map(|p| (b"peer7".to_vec(), p)));
      lost += SIXTEEN.len().saturating_sub(got.len());
      misrouted += got.iter().filter(|(id, _)| id.as_slice() != b"peer7").count();
    }
    assert_eq!((lost, misrouted), (0, 0), "(lost, misrouted) over 2000 detaches");
  }

}
