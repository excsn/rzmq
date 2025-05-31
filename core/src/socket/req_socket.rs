// core/src/socket/req_socket.rs

use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::{FairQueue, LoadBalancer};
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob};

use async_trait::async_trait;
use futures::future::Either; // Kept as it was in your original provided code for recv_multipart
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex as TokioMutex, MutexGuard as TokioMutexGuard, Notify};
use tokio::time::timeout as tokio_timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,
  ExpectingReply { target_endpoint_uri: String }, // MODIFIED
}

#[derive(Debug)]
struct ReqRecvStateInternal {
  // Renamed from ReqRecvState
  state: ReqState,
  reply_or_error_notifier: Arc<Notify>,
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer,
  incoming_reply_queue: FairQueue,
  state: TokioMutex<ReqRecvStateInternal>, // Mutex guards ReqStateInternal
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>, // MODIFIED
}

impl ReqSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let reply_queue_capacity = 1;
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_reply_queue: FairQueue::new(reply_queue_capacity),
      state: TokioMutex::new(ReqRecvStateInternal {
        state: ReqState::ReadyToSend,
        reply_or_error_notifier: Arc::new(Notify::new()),
      }),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }
}

#[async_trait]
impl ISocket for ReqSocket {
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

    if msg.is_more() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
      tracing::trace!(
        handle = self.core.handle,
        "REQ send: Cleared MORE flag from outgoing message."
      );
    }

    {
      let current_state_internal_guard = self.state.lock().await;
      if !matches!(current_state_internal_guard.state, ReqState::ReadyToSend) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
    }

    let timeout_opt: Option<Duration> = self.core_state_read().options.sndtimeo;

    // Peer selection loop - RETAINED FROM YOUR ORIGINAL PROVIDED CODE
    let endpoint_uri_to_send_to = loop {
      if let Some(uri) = self.load_balancer.get_next_connection_uri() {
        if self.core_state_read().endpoints.contains_key(&uri) {
          break uri;
        } else {
          self.load_balancer.remove_connection(&uri);
        }
      } else {
        if !self.core.is_running().await {
          return Err(ZmqError::InvalidState(
            "Socket terminated while waiting for peer".into(),
          ));
        }
        match timeout_opt {
          Some(duration) if duration.is_zero() => return Err(ZmqError::ResourceLimitReached),
          None => {
            self.load_balancer.wait_for_connection().await;
            continue;
          }
          Some(duration) => match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
            Ok(()) => continue,
            Err(_) => return Err(ZmqError::Timeout),
          },
        }
      }
    };

    let conn_iface: Arc<dyn ISocketConnection> = {
      let core_s_read = self.core_state_read();
      match core_s_read.endpoints.get(&endpoint_uri_to_send_to) {
        Some(ep_info) => ep_info.connection_iface.clone(),
        None => {
          self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
          return Err(ZmqError::HostUnreachable(format!(
            "Peer at {} disappeared before send",
            endpoint_uri_to_send_to
          )));
        }
      }
    };

    // Standard ZMTP REQ->REP: send [<empty_frame_MORE>, <data_frame_NOMORE>]
    let mut empty_delimiter = Msg::new();
    empty_delimiter.set_flags(MsgFlags::MORE);

    let send_delimiter_result = conn_iface.send_message(empty_delimiter).await;

    let final_send_result = if send_delimiter_result.is_ok() {
      msg.set_flags(msg.flags() & !MsgFlags::MORE);
      conn_iface.send_message(msg).await
    } else {
      send_delimiter_result
    };

    match final_send_result {
      Ok(()) => {
        let mut current_state_internal_guard = self.state.lock().await;
        match current_state_internal_guard.state {
          ReqState::ReadyToSend => {
            current_state_internal_guard.state = ReqState::ExpectingReply {
              target_endpoint_uri: endpoint_uri_to_send_to.clone(),
            };
          }
          ReqState::ExpectingReply { .. } => {
            tracing::error!(handle = self.core.handle, state = ?current_state_internal_guard.state, "REQ state was not ReadyToSend after successful send. Protocol violation likely.");
            current_state_internal_guard.state = ReqState::ExpectingReply {
              target_endpoint_uri: endpoint_uri_to_send_to.clone(),
            };
          }
        }
        Ok(())
      }
      Err(e @ ZmqError::ConnectionClosed) => {
        self.load_balancer.remove_connection(&endpoint_uri_to_send_to);
        Err(ZmqError::HostUnreachable(format!(
          "Connection to {} closed during send",
          endpoint_uri_to_send_to
        )))
      }
      Err(e) => Err(e),
    }
  }

  // recv() method - RETAINED FROM YOUR ORIGINAL PROVIDED CODE
  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };

    let (_current_target_uri_ignored, notifier_clone) = {
      let op_state_guard = self.state.lock().await;
      match &op_state_guard.state {
        ReqState::ExpectingReply { target_endpoint_uri: _ } => ((), op_state_guard.reply_or_error_notifier.clone()),
        ReqState::ReadyToSend => {
          return Err(ZmqError::InvalidState("REQ socket must call send() before receiving"));
        }
      }
    };

    loop {
      let pop_future = self.incoming_reply_queue.pop_message();
      let notified_future = notifier_clone.notified();

      let selected_future = match rcvtimeo_opt {
        Some(duration) if !duration.is_zero() => {
          tokio::select! {
              biased;
              pop_res = pop_future => Either::Left(pop_res),
              _ = tokio::time::sleep(Duration::from_micros(10)) => {
                  tokio::select! {
                     _ = notified_future => Either::Right(Ok(())),
                     _ = tokio_timeout(duration, futures::future::pending::<()>()) => Either::Right(Err(ZmqError::Timeout)),
                  }
              }
          }
        }
        _ => {
          tokio::select! {
              biased;
              pop_res = pop_future => Either::Left(pop_res),
               _ = tokio::time::sleep(Duration::from_micros(10)) => {
                  tokio::select!{
                      _ = notified_future => Either::Right(Ok(())),
                  }
               }
          }
        }
      };

      match selected_future {
        Either::Left(Ok(Some(mut msg))) => {
          if msg.is_more() {
            msg.set_flags(msg.flags() & !MsgFlags::MORE);
          }
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Ok(msg);
        }
        Either::Left(Ok(None)) => {
          if rcvtimeo_opt == Some(Duration::ZERO) {
            return Err(ZmqError::ResourceLimitReached);
          }
        }
        Either::Left(Err(e)) => {
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Err(e);
        }
        Either::Right(Ok(())) => {
          if let Ok(Some(mut msg)) = self.incoming_reply_queue.try_pop_message() {
            if msg.is_more() {
              msg.set_flags(msg.flags() & !MsgFlags::MORE);
            }
            let mut op_state_guard = self.state.lock().await;
            op_state_guard.state = ReqState::ReadyToSend;
            op_state_guard.reply_or_error_notifier.notify_waiters();
            return Ok(msg);
          }
          let op_state_guard = self.state.lock().await;
          if matches!(op_state_guard.state, ReqState::ReadyToSend) {
            return Err(ZmqError::Internal("Request cancelled due to peer detachment".into()));
          }
          tracing::trace!("REQ recv: Notified, but queue empty and still ExpectingReply. Looping.");
        }
        Either::Right(Err(timeout_err @ ZmqError::Timeout)) => {
          return Err(timeout_err);
        }
        Either::Right(Err(other_err)) => {
          return Err(other_err);
        }
      }
    }
  }

  // <<< REVERTED send_multipart to its original form from your provided code >>>
  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      "REQ socket: send_multipart() called. REQ sockets should use send() for single-part requests. Use DEALER for general multipart messaging."
    );
    Err(ZmqError::UnsupportedFeature(
      "REQ sockets use send() for single-part requests. Use DEALER for general multipart messaging.".into(),
    ))
  }

  // <<< REVERTED recv_multipart to its original form from your provided code >>>
  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
    let overall_deadline = rcvtimeo_opt.map(|d| Instant::now() + d);

    let initial_notifier_clone = {
      let op_state_guard = self.state.lock().await;
      match op_state_guard.state {
        ReqState::ExpectingReply { .. } => op_state_guard.reply_or_error_notifier.clone(),
        ReqState::ReadyToSend => {
          return Err(ZmqError::InvalidState(
            "REQ socket must call send() before receiving reply",
          ));
        }
      }
    };

    let mut reply_frames: Vec<Msg> = Vec::new();

    loop {
      let current_frame_timeout = if let Some(deadline) = overall_deadline {
        deadline.checked_duration_since(Instant::now())
      } else {
        None
      };

      if let Some(timeout_val) = current_frame_timeout {
        if timeout_val.is_zero() {
          if reply_frames.is_empty() {
            return Err(ZmqError::Timeout);
          } else {
            tracing::warn!(
              handle = self.core.handle,
              "REQ recv_multipart: Timeout receiving subsequent part. Discarding partial reply."
            );
            let mut op_state_guard = self.state.lock().await;
            op_state_guard.state = ReqState::ReadyToSend;
            op_state_guard.reply_or_error_notifier.notify_waiters();
            return Err(ZmqError::ProtocolViolation("Timeout during multi-part reply".into()));
          }
        }
      } else if rcvtimeo_opt.is_some() && current_frame_timeout.is_none() {
        if reply_frames.is_empty() {
          return Err(ZmqError::Timeout);
        } else {
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Err(ZmqError::ProtocolViolation(
            "Timeout during multi-part reply (deadline passed)".into(),
          ));
        }
      }

      let pop_future = self.incoming_reply_queue.pop_message();
      let notified_future = initial_notifier_clone.notified();

      enum FrameRecvOutcome {
        Frame(Msg),
        QueueClosedOrError(ZmqError),
        NotifierFired,
        TimeoutWaitingForNotifier,
      }

      let frame_outcome_result = match current_frame_timeout {
        Some(duration_for_this_frame) if !duration_for_this_frame.is_zero() => {
          tokio::select! {
              biased;
              pop_res = pop_future => {
                  match pop_res {
                      Ok(Some(msg)) => FrameRecvOutcome::Frame(msg),
                      Ok(None) => FrameRecvOutcome::QueueClosedOrError(ZmqError::Internal("REQ: Reply queue closed".into())),
                      Err(e) => FrameRecvOutcome::QueueClosedOrError(e),
                  }
              },
              notify_res = tokio_timeout(duration_for_this_frame, notified_future) => {
                  match notify_res {
                      Ok(()) => FrameRecvOutcome::NotifierFired,
                      Err(_) => FrameRecvOutcome::TimeoutWaitingForNotifier,
                  }
              }
          }
        }
        _ => {
          if rcvtimeo_opt == Some(Duration::ZERO) && reply_frames.is_empty() {
            match self.incoming_reply_queue.try_pop_message() {
              Ok(Some(msg)) => FrameRecvOutcome::Frame(msg),
              Ok(None) => return Err(ZmqError::ResourceLimitReached),
              Err(e) => FrameRecvOutcome::QueueClosedOrError(e),
            }
          } else {
            tokio::select! {
                biased;
                pop_res = pop_future => {
                    match pop_res {
                        Ok(Some(msg)) => FrameRecvOutcome::Frame(msg),
                        Ok(None) => FrameRecvOutcome::QueueClosedOrError(ZmqError::Internal("REQ: Reply queue closed".into())),
                        Err(e) => FrameRecvOutcome::QueueClosedOrError(e),
                    }
                },
                _ = notified_future => FrameRecvOutcome::NotifierFired,
            }
          }
        }
      };

      match frame_outcome_result {
        FrameRecvOutcome::Frame(current_frame) => {
          let is_last_part = !current_frame.is_more();
          reply_frames.push(current_frame);
          if is_last_part {
            let mut op_state_guard = self.state.lock().await;
            if matches!(op_state_guard.state, ReqState::ExpectingReply { .. }) {
              op_state_guard.state = ReqState::ReadyToSend;
              op_state_guard.reply_or_error_notifier.notify_waiters();
              return Ok(reply_frames);
            } else {
              return Err(ZmqError::InvalidState(
                "REQ state changed mid-multipart receive, request likely cancelled.".into(),
              ));
            }
          }
        }
        FrameRecvOutcome::NotifierFired => {
          let mut op_state_guard = self.state.lock().await;
          if !matches!(op_state_guard.state, ReqState::ExpectingReply { .. }) {
            if !reply_frames.is_empty() {
              op_state_guard.state = ReqState::ReadyToSend;
              op_state_guard.reply_or_error_notifier.notify_waiters();
              return Err(ZmqError::ProtocolViolation(
                "Request cancelled mid-multipart reply due to state change.".into(),
              ));
            }
          }
          tracing::trace!("REQ recv_multipart: Notifier fired. Re-evaluating.");
          continue;
        }
        FrameRecvOutcome::TimeoutWaitingForNotifier => {
          if reply_frames.is_empty() {
            return Err(ZmqError::Timeout);
          } else {
            let mut op_state_guard = self.state.lock().await;
            op_state_guard.state = ReqState::ReadyToSend;
            op_state_guard.reply_or_error_notifier.notify_waiters();
            return Err(ZmqError::ProtocolViolation(
              "Timeout during multi-part reply (notifier path)".into(),
            ));
          }
        }
        FrameRecvOutcome::QueueClosedOrError(e) => {
          let mut op_state_guard = self.state.lock().await;
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_waiters();
          return Err(e);
        }
      }
    }
  }

  async fn set_pattern_option(&self, option: i32, _value: &[u8]) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }
  async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
    Err(ZmqError::UnsupportedOption(option))
  }

  async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
    Ok(false)
  }

  // <<< MODIFIED: handle_pipe_event checks against target_endpoint_uri >>>
  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        let source_uri_opt = self.pipe_read_to_endpoint_uri.read().get(&pipe_read_id).cloned();

        if source_uri_opt.is_none() {
          tracing::warn!(
            handle = self.core.handle,
            pipe_id = pipe_read_id,
            "REQ received reply from unknown pipe (no URI mapping). Dropping."
          );
          return Ok(());
        }
        let source_uri = source_uri_opt.unwrap();

        let mut op_state_guard = self.state.lock().await;
        match &op_state_guard.state {
          ReqState::ExpectingReply { target_endpoint_uri } => {
            if *target_endpoint_uri == source_uri {
              let mut actual_payload = msg;
              if actual_payload.size() == 0 && actual_payload.is_more() {
                tracing::trace!(
                  handle = self.core.handle,
                  pipe_id = pipe_read_id,
                  "REQ handle_pipe_event: Received ZMTP empty delimiter, waiting for actual payload frame."
                );
                return Ok(());
              }

              if actual_payload.is_more() {
                actual_payload.set_flags(actual_payload.flags() & !MsgFlags::MORE);
              }

              let notifier_to_signal = op_state_guard.reply_or_error_notifier.clone();
              drop(op_state_guard);

              if self.incoming_reply_queue.push_message(actual_payload).await.is_err() {
                tracing::error!(
                  handle = self.core.handle,
                  pipe_id = pipe_read_id,
                  "REQ: Failed to push reply to FairQueue."
                );
                let mut op_state_guard_err = self.state.lock().await;
                op_state_guard_err.state = ReqState::ReadyToSend;
                op_state_guard_err.reply_or_error_notifier.notify_waiters();
              } else {
                notifier_to_signal.notify_one();
              }
            } else {
              tracing::warn!(handle = self.core.handle, source_pipe_id = pipe_read_id, source_uri = %source_uri, expected_target_uri = %target_endpoint_uri, "REQ received reply from unexpected peer. Dropping.");
            }
          }
          ReqState::ReadyToSend => {
            tracing::warn!(handle = self.core.handle, source_pipe_id = pipe_read_id, source_uri = %source_uri, "REQ received unexpected reply (state ReadyToSend). Dropping.");
          }
        }
      }
      _ => {}
    }
    Ok(())
  }

  // <<< MODIFIED: pipe_attached updates pipe_read_to_endpoint_uri and LoadBalancer with URI >>>
  async fn pipe_attached(
    &self,
    pipe_read_id: usize,
    _pipe_write_id: usize, // No longer used by ReqSocket as LoadBalancer stores URI
    _peer_identity: Option<&[u8]>,
  ) {
    let endpoint_uri_option = self
      .core_state_read()
      .pipe_read_id_to_endpoint_uri
      .get(&pipe_read_id)
      .cloned();

    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "REQ attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.load_balancer.add_connection(endpoint_uri);
      self.incoming_reply_queue.pipe_attached(pipe_read_id);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "REQ pipe_attached: Endpoint URI not found for pipe in CoreState. Maps not updated."
      );
    }
  }

  async fn update_peer_identity(&self, pipe_read_id: usize, identity: Option<Blob>) {
    tracing::trace!(
      handle = self.core.handle,
      socket_type = "REQ",
      pipe_read_id,
      ?identity,
      "update_peer_identity called, but REQ socket does not use peer identities. Ignoring."
    );
  }

  // <<< MODIFIED: pipe_detached uses pipe_read_to_endpoint_uri and updates state correctly >>>
  async fn pipe_detached(&self, pipe_read_id: usize) {
    tracing::debug!(handle = self.core.handle, pipe_read_id, "REQ detaching pipe");

    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);

    if let Some(detached_uri) = maybe_endpoint_uri {
      self.load_balancer.remove_connection(&detached_uri);

      let mut op_state_guard = self.state.lock().await;
      if let ReqState::ExpectingReply {
        ref target_endpoint_uri,
      } = op_state_guard.state
      {
        if *target_endpoint_uri == detached_uri {
          tracing::warn!(handle = self.core.handle, pipe_id = pipe_read_id, uri = %detached_uri, "Target REP peer detached while REQ was expecting reply. Resetting state and notifying recv.");
          op_state_guard.state = ReqState::ReadyToSend;
          op_state_guard.reply_or_error_notifier.notify_one();
        }
      }
    }
    self.incoming_reply_queue.pipe_detached(pipe_read_id);
  }
}
