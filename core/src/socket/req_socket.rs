use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::ISocket;
use crate::socket::core::SocketCore;
use crate::socket::options::{parse_bool_option, REQ_CORRELATE, REQ_RELAXED};
use crate::socket::patterns::LoadBalancer;
use crate::socket::patterns::AddressedIngressEngine;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::{Blob, delegate_to_core};

use async_trait::async_trait;
use parking_lot::Mutex as ParkingLotMutex;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::time::timeout as tokio_timeout;

/// Driven by send and recv only. A pipe going away clears `reply_pipe` and nothing else.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,
  /// `reply_pipe` is the pipe the request went out on. `None` once that pipe has
  /// detached, after which a reply is taken from any pipe.
  ExpectingReply { reply_pipe: Option<usize> },
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer,
  ingress_engine: AddressedIngressEngine,
  pending_pipe_senders: ParkingLotMutex<HashMap<usize, PipeMessageSender>>,
  state: ParkingLotMutex<ReqState>,
  reply_available_notifier: Arc<Notify>,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
  /// REQ_RELAXED: send while a reply is outstanding abandons that reply.
  relaxed: AtomicBool,
  /// REQ_CORRELATE: requests carry a request-id frame ahead of the delimiter.
  correlate: AtomicBool,
  request_id: AtomicU32,
}

impl ReqSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let (max_conn, rcvbatch_count) = {
      let opts = &core.core_state.read().options;
      (opts.max_connections.unwrap_or(1024), opts.rcvbatch_count)
    };
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      ingress_engine: AddressedIngressEngine::new(max_conn, rcvbatch_count),
      pending_pipe_senders: ParkingLotMutex::new(HashMap::new()),
      state: ParkingLotMutex::new(ReqState::ReadyToSend),
      reply_available_notifier: Arc::new(Notify::new()),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
      relaxed: AtomicBool::new(false),
      correlate: AtomicBool::new(false),
      request_id: AtomicU32::new(rand::random()),
    }
  }

  fn expecting_reply(&self) -> bool {
    matches!(*self.state.lock(), ReqState::ExpectingReply { .. })
  }

  /// Strips the reply envelope. `None` means the message is not the reply to the
  /// outstanding request and is discarded: it came from a pipe other than the one
  /// the request went out on, its request id differs or it has no delimiter.
  fn accept_reply(&self, pipe_read_id: usize, mut batch: FrameBatch) -> Option<FrameBatch> {
    if let ReqState::ExpectingReply { reply_pipe: Some(expected) } = *self.state.lock() {
      if expected != pipe_read_id {
        tracing::debug!(handle = self.core.handle, pipe_read_id, "REQ: reply from another pipe discarded.");
        return None;
      }
    }
    if self.correlate.load(Ordering::Relaxed) {
      let expected = self.request_id.load(Ordering::Relaxed).to_ne_bytes();
      let id_matches = batch.len() >= 2 && batch[0].is_more() && batch[0].data() == Some(&expected[..]);
      if !id_matches {
        tracing::debug!(handle = self.core.handle, pipe_read_id, "REQ: reply with another request id discarded.");
        return None;
      }
      batch.remove(0);
    }
    if batch.is_empty() || batch[0].size() != 0 {
      tracing::warn!(handle = self.core.handle, pipe_read_id, "REQ: reply without an empty delimiter discarded.");
      return None;
    }
    batch.remove(0);
    Some(batch)
  }

  /// Waits for the reply to the outstanding request. Only a delivered reply moves
  /// the state back to `ReadyToSend`; an error leaves the request outstanding.
  async fn wait_for_reply(&self, rcvtimeo: Option<Duration>) -> Result<FrameBatch, ZmqError> {
    let deadline = rcvtimeo.filter(|d| !d.is_zero()).map(|d| Instant::now() + d);
    let payload = loop {
      if !self.expecting_reply() {
        return Err(ZmqError::InvalidState("Socket state changed while waiting for reply"));
      }
      let remaining = match deadline {
        Some(deadline) => {
          let left = deadline.saturating_duration_since(Instant::now());
          if left.is_zero() {
            return Err(ZmqError::Timeout);
          }
          Some(left)
        }
        None => rcvtimeo,
      };
      let notified = self.reply_available_notifier.notified();
      tokio::select! {
        biased;
        _ = notified => {
          if !self.core.is_running() {
            return Err(ZmqError::ConnectionClosed);
          }
        }
        res = self.ingress_engine.recv_logical_message(remaining) => {
          let (pipe_read_id, batch) = res?;
          if let Some(payload) = self.accept_reply(pipe_read_id, batch) {
            break payload;
          }
        }
      }
    };
    {
      let mut state = self.state.lock();
      if matches!(*state, ReqState::ExpectingReply { .. }) {
        *state = ReqState::ReadyToSend;
      }
    }
    self.reply_available_notifier.notify_waiters();
    Ok(payload)
  }
}

#[async_trait]
impl ISocket for ReqSocket {
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
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    if msg.is_more() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
    }

    // The slot is taken under the lock so concurrent senders see ExpectingReply at
    // once; it is given back if the send fails.
    let abandoned_reply = {
      let mut state = self.state.lock();
      let abandoned = matches!(*state, ReqState::ExpectingReply { .. });
      if abandoned && !self.relaxed.load(Ordering::Relaxed) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
      *state = ReqState::ExpectingReply { reply_pipe: None };
      abandoned
    };
    if abandoned_reply {
      self.reply_available_notifier.notify_waiters();
    }

    // Replies still queued belong to earlier requests.
    while self.ingress_engine.recv_logical_message(Some(Duration::ZERO)).await.is_ok() {}

    let timeout_opt: Option<Duration> = { self.core.core_state.read().options.sndtimeo };

    let peer = loop {
      if let Some(p) = self.load_balancer.get_next_connection() {
        break p;
      }
      if self.core.command_sender().is_closed() {
        *self.state.lock() = ReqState::ReadyToSend;
        return Err(ZmqError::InvalidState("Socket terminated".into()));
      }
      let waited = match timeout_opt {
        Some(duration) if duration.is_zero() => Err(ZmqError::ResourceLimitReached),
        None => self.load_balancer.wait_for_connection().await,
        Some(duration) => match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
          Ok(res) => res,
          Err(_) => Err(ZmqError::Timeout),
        },
      };
      if let Err(e) = waited {
        *self.state.lock() = ReqState::ReadyToSend;
        return Err(e);
      }
    };

    let reply_pipe = self
      .pipe_read_to_endpoint_uri
      .read()
      .iter()
      .find(|(_, uri)| **uri == peer.uri)
      .map(|(id, _)| *id);

    let mut zmtp_frames_to_send = FrameBatch::new();
    if self.correlate.load(Ordering::Relaxed) {
      let id = self.request_id.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
      let mut id_frame = Msg::from_vec(id.to_ne_bytes().to_vec());
      id_frame.set_flags(MsgFlags::MORE);
      zmtp_frames_to_send.push(id_frame);
    }
    let mut empty_delimiter = Msg::new();
    empty_delimiter.set_flags(MsgFlags::MORE);
    zmtp_frames_to_send.push(empty_delimiter);
    zmtp_frames_to_send.push(msg);

    match peer.iface.send_multipart(zmtp_frames_to_send).await {
      Ok(()) => {
        *self.state.lock() = ReqState::ExpectingReply { reply_pipe };
        Ok(())
      }
      Err(e) => {
        *self.state.lock() = ReqState::ReadyToSend;
        match e {
          ZmqError::ConnectionClosed => {
            self.load_balancer.remove_connection(&peer.uri);
            Err(ZmqError::HostUnreachable(format!(
              "REQ: Connection to {} closed during send",
              peer.uri
            )))
          }
          other => Err(other),
        }
      }
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if !self.expecting_reply() {
      return Err(ZmqError::InvalidState(
        "REQ socket must call send() before receiving",
      ));
    }
    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;
    let mut payload = self.wait_for_reply(rcvtimeo_opt).await?;
    Ok(if payload.is_empty() { Msg::new() } else { payload.remove(0) })
  }

  async fn send_multipart(&self, _frames: FrameBatch) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      "REQ socket: send_multipart() called. REQ sockets should use send() for single-part requests."
    );
    Err(ZmqError::UnsupportedFeature(
      "REQ sockets use send() for single-part requests. Use DEALER for general multipart messaging.".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<FrameBatch, ZmqError> {
    if !self.core.is_running() {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    if !self.expecting_reply() {
      return Err(ZmqError::InvalidState(
        "REQ socket must call send() before receiving reply",
      ));
    }
    let rcvtimeo_opt: Option<Duration> = self.core.core_state.read().options.rcvtimeo;
    self.wait_for_reply(rcvtimeo_opt).await
  }

  async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
    match option {
      REQ_RELAXED => {
        self.relaxed.store(parse_bool_option(value)?, Ordering::Relaxed);
        Ok(())
      }
      REQ_CORRELATE => {
        self.correlate.store(parse_bool_option(value)?, Ordering::Relaxed);
        Ok(())
      }
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    match option {
      REQ_RELAXED => Ok((self.relaxed.load(Ordering::Relaxed) as i32).to_ne_bytes().to_vec()),
      REQ_CORRELATE => Ok((self.correlate.load(Ordering::Relaxed) as i32).to_ne_bytes().to_vec()),
      _ => Err(ZmqError::UnsupportedOption(option)),
    }
  }

  async fn process_command(&self, command: Command) -> Result<bool, ZmqError> {
    match command {
      Command::Stop => {
        self.ingress_engine.close();
        self.reply_available_notifier.notify_waiters();
      }
      _ => return Ok(false),
    }

    Ok(true)
  }

  fn get_incoming_pipe_sender(&self, pipe_read_id: usize) -> Option<PipeMessageSender> {
    self.pending_pipe_senders.lock().remove(&pipe_read_id)
  }

  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize,
    _peer_identity: Option<&[u8]>,
  ) {
    let (endpoint_uri_opt, connection_iface_opt) = {
      let core_s_read = self.core.core_state.read();
      let uri = core_s_read
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned();
      let iface = uri.as_ref().and_then(|u| {
        core_s_read
          .endpoints
          .get(u)
          .map(|ep| ep.connection_iface.clone())
      });
      (uri, iface)
    };

    if let (Some(endpoint_uri), Some(iface)) = (endpoint_uri_opt, connection_iface_opt) {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "REQ attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());

      self.load_balancer.add_connection(endpoint_uri, iface);

      let (rcvhwm, rcvbatch_count) = {
        let opts = self.core.core_state.read();
        (opts.options.rcvhwm.max(1), opts.options.rcvbatch_count)
      };
      let sender = self.ingress_engine.register_pipe(pipe_read_id, rcvhwm, rcvbatch_count);
      self.pending_pipe_senders.lock().insert(pipe_read_id, sender);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "REQ pipe_attached: Endpoint URI or connection interface not found. Maps not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "REQ",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, REQ socket ignores it."
    );
  }

  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(
      handle = self.core.handle,
      pipe_read_id,
      "REQ detaching pipe"
    );

    if let Some(detached_uri) = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id) {
      self.load_balancer.remove_connection(&detached_uri);
    }
    {
      let mut state = self.state.lock();
      if *state == (ReqState::ExpectingReply { reply_pipe: Some(pipe_read_id) }) {
        *state = ReqState::ExpectingReply { reply_pipe: None };
      }
    }

    self.ingress_engine.deregister_pipe(pipe_read_id);
    self.pending_pipe_senders.lock().remove(&pipe_read_id);
  }
}
