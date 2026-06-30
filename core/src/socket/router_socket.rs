use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::SocketCore;
use crate::socket::options::{AUTO_DELIMITER, ROUTER_MANDATORY};
use crate::socket::patterns::AddressedIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::socket::patterns::{FramingLatch, RouterMap, WritePipeCoordinator, router_auto_decode, router_auto_encode};

use dashmap::DashMap;
use parking_lot::Mutex as ParkingMutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::{Mutex as TokioMutex, Notify, OwnedSemaphorePermit};

use super::core::command_processor::update_core_option;
use super::parse_bool_option;

/// Structure to hold state for an ongoing fragmented send (identity part already sent).
#[derive(Debug)]
struct ActiveFragmentedSend {
  target_endpoint_uri: String,
  _permit: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(crate) struct RouterSocket {
  core: Arc<SocketCore>,
  router_map_for_send: RouterMap,
  ingress_engine: AddressedIngressEngine,
  pending_pipe_senders: ParkingMutex<HashMap<usize, PipeMessageSender>>,
  frame_recv_buffer: ParkingMutex<Option<VecDeque<Msg>>>,
  pipe_to_identity_shared_map: Arc<DashMap<usize, Blob>>,
  current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
  pipe_send_coordinator: Arc<WritePipeCoordinator>,
  framing: FramingLatch,
  // --- Identity-finalization gate (Option 1: gate ingress on finalized identity) ---
  // A pipe is "finalized" once its real ZMTP identity (or the placeholder for an
  // anonymous peer) has been applied. The standard TCP/IPC path attaches the pipe
  // with a placeholder *before* the handshake, so the first inbound message can race
  // ahead of the identity event and be delivered with `pipe:N` instead of the peer's
  // announced identity. We hold a pending pipe's batches until it finalizes, then
  // release them in arrival order — mirroring the io-uring path's atomic attach.
  /// Presence of a `pipe_read_id` key means that pipe's identity is finalized.
  pipe_finalized: Arc<DashMap<usize, ()>>,
  /// Per-pipe FIFO of batches received while the pipe was still pending.
  held_ingress: ParkingMutex<HashMap<usize, VecDeque<FrameBatch>>>,
  /// Total batches across `held_ingress`; lets the recv hot path skip the lock.
  held_count: AtomicUsize,
  /// Signalled whenever a pipe finalizes, releasing any held batches.
  identity_finalized_notify: Arc<Notify>,
}

impl RouterSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let max_conn = core.core_state.read().options.max_connections.unwrap_or(1024);
    Self {
      core,
      router_map_for_send: RouterMap::new(),
      ingress_engine: AddressedIngressEngine::new(max_conn),
      pending_pipe_senders: ParkingMutex::new(HashMap::new()),
      frame_recv_buffer: ParkingMutex::new(None),
      pipe_to_identity_shared_map: Arc::new(DashMap::new()),
      current_send_target: TokioMutex::new(None),
      pipe_send_coordinator: Arc::new(WritePipeCoordinator::new()),
      framing: FramingLatch::new(router_auto_encode, router_auto_decode),
      pipe_finalized: Arc::new(DashMap::new()),
      held_ingress: ParkingMutex::new(HashMap::new()),
      held_count: AtomicUsize::new(0),
      identity_finalized_notify: Arc::new(Notify::new()),
    }
  }

  /// Marks a pipe's identity as finalized and wakes any recv waiting to release
  /// held batches. Idempotent — both `pipe_attached(Some(id))` (io-uring atomic
  /// attach) and `update_peer_identity` (every successful handshake) may call it.
  fn finalize_pipe(&self, pipe_read_id: usize) {
    self.pipe_finalized.insert(pipe_read_id, ());
    self.identity_finalized_notify.notify_waiters();
  }

  /// Buffers a batch that arrived before its pipe finalized.
  fn hold_pending_batch(&self, pipe_read_id: usize, batch: FrameBatch) {
    self
      .held_ingress
      .lock()
      .entry(pipe_read_id)
      .or_default()
      .push_back(batch);
    self.held_count.fetch_add(1, Ordering::AcqRel);
  }

  /// Pops the next held batch belonging to a now-finalized pipe (FIFO per pipe).
  /// Returns `None` when no held batch is releasable yet.
  fn take_finalized_held(&self) -> Option<(usize, FrameBatch)> {
    if self.held_count.load(Ordering::Acquire) == 0 {
      return None;
    }
    let mut held = self.held_ingress.lock();
    let target = held
      .keys()
      .find(|pid| self.pipe_finalized.contains_key(*pid))
      .copied()?;
    let queue = held.get_mut(&target).expect("key just found");
    let batch = queue.pop_front();
    if queue.is_empty() {
      held.remove(&target);
    }
    if batch.is_some() {
      self.held_count.fetch_sub(1, Ordering::AcqRel);
    }
    batch.map(|b| (target, b))
  }

  /// Like `AddressedIngressEngine::recv_logical_message`, but never delivers a
  /// batch from a pipe whose identity is not yet finalized. Pending batches are
  /// held and released, in arrival order, once the pipe finalizes.
  async fn recv_logical_finalized(
    &self,
    rcvtimeo_opt: Option<std::time::Duration>,
  ) -> Result<(usize, FrameBatch), ZmqError> {
    use std::time::Duration;

    let non_blocking = matches!(rcvtimeo_opt, Some(d) if d.is_zero());
    let deadline = match rcvtimeo_opt {
      Some(d) if !d.is_zero() => Some(tokio::time::Instant::now() + d),
      _ => None,
    };

    loop {
      // 1. Release held data from a finalized pipe before pulling anything new,
      //    so a peer's first (held) message precedes its later (queued) ones.
      if let Some(item) = self.take_finalized_held() {
        return Ok(item);
      }

      // 2. Non-blocking mode: try once, never wait on finalization.
      if non_blocking {
        let (pid, batch) = self
          .ingress_engine
          .recv_logical_message(Some(Duration::ZERO))
          .await?;
        if self.pipe_finalized.contains_key(&pid) {
          return Ok((pid, batch));
        }
        // Pending: buffer it and re-loop; if nothing else is ready the next
        // iteration surfaces `ResourceLimitReached` (would-block).
        self.hold_pending_batch(pid, batch);
        continue;
      }

      // 3. Blocking / timed mode: race a fresh queue item against a finalize
      //    signal (which may release held data) and the deadline.
      let notified = self.identity_finalized_notify.notified();
      tokio::pin!(notified);
      // Register as a waiter *before* re-checking so a finalize firing between
      // the check and the await cannot be lost.
      notified.as_mut().enable();
      if let Some(item) = self.take_finalized_held() {
        return Ok(item);
      }

      tokio::select! {
        biased;
        _ = &mut notified => {
          // A pipe finalized; loop to drain its held data.
          continue;
        }
        popped = self.ingress_engine.pop() => {
          let (pid, batch) = popped?;
          if self.pipe_finalized.contains_key(&pid) {
            return Ok((pid, batch));
          }
          self.hold_pending_batch(pid, batch);
          continue;
        }
        _ = async {
          match deadline {
            Some(dl) => tokio::time::sleep_until(dl).await,
            None => std::future::pending::<()>().await,
          }
        } => {
          return Err(ZmqError::Timeout);
        }
      }
    }
  }

  fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
    Blob::from_bytes(Bytes::from(format!("pipe:{}", pipe_read_id)))
  }

  fn process_incoming_zmtp_message(
    &self,
    pipe_read_id: usize,
    mut raw_zmtp_message: FrameBatch,
  ) -> Result<(Blob, FrameBatch), ZmqError> {
    // The application should receive [sender_identity, payload...].
    // The 'payload' is the entire message received from the peer, after stripping
    // a single leading delimiter IF the peer is a REQ or DEALER.

    // First, determine the sender's identity from our internal map.
    let identity_blob = self
      .pipe_to_identity_shared_map
      .get(&pipe_read_id)
      .map(|entry| entry.value().clone())
      .unwrap_or_else(|| {
        tracing::warn!(
          handle = self.core.handle,
          pipe_id = pipe_read_id,
          "Router: Identity for pipe not found, using placeholder for incoming message."
        );
        Self::pipe_id_to_placeholder_identity(pipe_read_id)
      });

    // Now, determine if we need to strip a delimiter based on the peer's socket type.
    let peer_socket_type = {
      let core_s = self.core.core_state.read();
      core_s
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .and_then(|uri| core_s.endpoints.get(uri))
        .and_then(|ep_info| ep_info.peer_socket_type.clone())
    };

    if self.framing.is_manual() {
      return Ok((identity_blob, raw_zmtp_message));
    }

    match peer_socket_type.as_deref() {
      Some("REQ") | Some("DEALER") => {
        if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
          // Correctly strip the delimiter from REQ/DEALER peers.
          raw_zmtp_message.remove(0);
        } else {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "ROUTER: Expected empty delimiter from REQ/DEALER peer, but not found."
          );
        }
      }
      Some("ROUTER") => {
        // Do nothing. The entire message is the payload from a ROUTER peer.
      }
      _ => {
        // Default/Unknown: For backward compatibility or peers that don't announce type,
        // we can try to guess. The old behavior was to strip a delimiter if present.
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
    let (pipe_read_id, raw_batch) = self.recv_logical_finalized(rcvtimeo_opt).await?;
    let (identity_blob, payload) = self.process_incoming_zmtp_message(pipe_read_id, raw_batch)?;
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
        // Peer not found.
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
    // This is done in a tightly scoped lock.
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
        // The peer was in RouterMap but its EndpointInfo is gone. This is a stale entry.
        // We should clean up the RouterMap and then decide what to do.
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
        // If acquiring the permit fails, decide whether to error or drop.
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

    // Final flag setting on the last frame
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
    let (pipe_read_id, raw_batch) = self.recv_logical_finalized(rcvtimeo_opt).await?;
    let (identity_blob, payload) = self.process_incoming_zmtp_message(pipe_read_id, raw_batch)?;
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

  async fn handle_pipe_event(&self, _pipe_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      // Data frames are pushed directly by the actor via PipeMessageSender; no action needed.
      Command::PipeMessageReceived { .. } | Command::PipeMessageBatchReceived { .. } => {}
      _ => {}
    }
    Ok(())
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
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
      self
        .pipe_to_identity_shared_map
        .insert(pipe_read_id, identity_to_use);
      self.pipe_send_coordinator.add_pipe(pipe_read_id).await;

      // Register per-pipe ingress channel.
      let (rcvhwm, rcvbatch_count) = {
        let opts = self.core.core_state.read();
        (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
      };
      let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm, rcvbatch_count);
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

    // Finalize the identity gate regardless: this is the universal post-handshake
    // signal (fired for every peer type, anonymous or not, on both transports).
    // Releasing here even on the warn-path above avoids a stuck gate if the maps
    // are transiently inconsistent.
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
    self.pipe_to_identity_shared_map.remove(&pipe_read_id);

    self.pipe_send_coordinator.remove_pipe(pipe_read_id).await;

    if connection_id_opt.is_some() {
      let mut active_frag_guard = self.current_send_target.lock().await;
      if let Some(active_info) = &*active_frag_guard {
        if endpoint_uri_opt.as_deref() == Some(&active_info.target_endpoint_uri) {
          *active_frag_guard = None;
        }
      }
    }

    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);

    // Tear down the identity gate for this pipe. Any batches still held for a
    // pipe that detached before finalizing are dropped: the connection died
    // before its identity was resolved, so the messages can never be routed
    // with a real identity (ZMQ permits message loss on disconnect). Wake any
    // recv waiting on a finalize so it re-evaluates without this pipe.
    self.pipe_finalized.remove(&pipe_read_id);
    if let Some(dropped) = self.held_ingress.lock().remove(&pipe_read_id) {
      if !dropped.is_empty() {
        self.held_count.fetch_sub(dropped.len(), Ordering::AcqRel);
      }
    }
    self.identity_finalized_notify.notify_waiters();
  }
}
