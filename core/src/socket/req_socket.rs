use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::patterns::incoming_orchestrator::IncomingMessageOrchestrator;
use crate::socket::patterns::LoadBalancer;
use crate::socket::ISocket;
use crate::{delegate_to_core, Blob};

use async_trait::async_trait;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex as TokioMutex, Notify};
use tokio::time::timeout as tokio_timeout;

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReqState {
  ReadyToSend,
  ExpectingReply { target_endpoint_uri: String },
}

#[derive(Debug)]
pub(crate) struct ReqSocket {
  core: Arc<SocketCore>,
  load_balancer: LoadBalancer,
  incoming_orchestrator: IncomingMessageOrchestrator<Vec<Msg>>,
  state: TokioMutex<ReqState>,
  reply_available_notifier: Arc<Notify>,
  pipe_read_to_endpoint_uri: RwLock<HashMap<usize, String>>,
}

impl ReqSocket {
  pub fn new(core: Arc<SocketCore>) -> Self {
    let orchestrator = IncomingMessageOrchestrator::new(core.handle, 1);
    Self {
      core,
      load_balancer: LoadBalancer::new(),
      incoming_orchestrator: orchestrator,
      state: TokioMutex::new(ReqState::ReadyToSend),
      reply_available_notifier: Arc::new(Notify::new()),
      pipe_read_to_endpoint_uri: RwLock::new(HashMap::new()),
    }
  }

  fn core_state_read(&self) -> RwLockReadGuard<'_, CoreState> {
    self.core.core_state.read()
  }

  fn process_incoming_zmtp_message_for_req(
    &self,
    pipe_read_id: usize,
    mut raw_zmtp_message: Vec<Msg>,
  ) -> Result<Vec<Msg>, ZmqError> {
    if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
      tracing::trace!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "REQ: Stripped empty ZMTP delimiter from incoming reply."
      );
      raw_zmtp_message.remove(0);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_id = pipe_read_id,
        "REQ: Incoming ZMTP reply from pipe did not start with an empty delimiter as expected from REP."
      );
    }
    Ok(raw_zmtp_message)
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
    self.reply_available_notifier.notify_waiters();
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
        "REQ send: Cleared MORE flag from user-provided message."
      );
    }

    let target_endpoint_uri_for_state_update: String;
    {
      let mut current_state_guard = self.state.lock().await;
      if !matches!(*current_state_guard, ReqState::ReadyToSend) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call recv() before sending again",
        ));
      }
      let timeout_opt: Option<Duration> = { self.core.core_state.read().options.sndtimeo };

      target_endpoint_uri_for_state_update = loop {
        if let Some(uri) = self.load_balancer.get_next_connection_uri() {
          let exists = { self.core.core_state.read().endpoints.contains_key(&uri) };
          if exists {
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
            None => self.load_balancer.wait_for_connection().await,
            Some(duration) => match tokio_timeout(duration, self.load_balancer.wait_for_connection()).await {
              Ok(()) => {}
              Err(_) => return Err(ZmqError::Timeout),
            },
          }
        }
      };
    }

    let conn_iface: Arc<dyn ISocketConnection> = {
      let core_s_read = self.core_state_read();
      match core_s_read.endpoints.get(&target_endpoint_uri_for_state_update) {
        Some(ep_info) => ep_info.connection_iface.clone(),
        None => {
          self
            .load_balancer
            .remove_connection(&target_endpoint_uri_for_state_update);
          return Err(ZmqError::HostUnreachable(format!(
            "REQ: Peer at {} disappeared before send",
            target_endpoint_uri_for_state_update
          )));
        }
      }
    };

    let mut empty_delimiter = Msg::new();
    empty_delimiter.set_flags(MsgFlags::MORE);
    let zmtp_frames_to_send = vec![empty_delimiter, msg];
    let send_result = conn_iface.send_multipart(zmtp_frames_to_send).await;

    match send_result {
      Ok(()) => {
        let mut current_state_guard = self.state.lock().await;
        *current_state_guard = ReqState::ExpectingReply {
          target_endpoint_uri: target_endpoint_uri_for_state_update.clone(),
        };
        // <<< MODIFIED [Call orchestrator's method to reset its internal buffer] >>>
        self.incoming_orchestrator.reset_recv_message_buffer().await;
        Ok(())
      }
      Err(e @ ZmqError::ConnectionClosed) => {
        self
          .load_balancer
          .remove_connection(&target_endpoint_uri_for_state_update);
        Err(ZmqError::HostUnreachable(format!(
          "REQ: Connection to {} closed during send",
          target_endpoint_uri_for_state_update
        )))
      }
      Err(e) => Err(e),
    }
  }

  async fn recv(&self) -> Result<Msg, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }

    let rcvtimeo_opt: Option<Duration> = { self.core.core_state.read().options.rcvtimeo };

    // Check current state before calling orchestrator
    {
      let op_state_guard = self.state.lock().await;
      if !matches!(*op_state_guard, ReqState::ExpectingReply { .. }) {
        return Err(ZmqError::InvalidState("REQ socket must call send() before receiving"));
      }
    } // Lock released

    // For REQ, QItem is Vec<Msg> (the reply payload).
    // The transform_fn tells orchestrator.recv_message how to convert this QItem (Vec<Msg>)
    // into the sequence of app_frames it should buffer and serve one-by-one.
    // For REQ, the QItem (reply payload Vec<Msg>) IS the app_frames for the orchestrator's buffer.
    let transform_fn = |q_item: Vec<Msg>| q_item;

    let result = self
      .incoming_orchestrator
      .recv_message(rcvtimeo_opt, transform_fn)
      .await;

    // If recv_message successfully returned a frame and that frame was the last part
    // of the logical message (MORE=false), then transition REQ state to ReadyToSend.
    // The orchestrator's recv_message sets the MORE flag on the returned Msg correctly
    // based on its internal buffer.
    if result.is_ok() && result.as_ref().map_or(true, |m| !m.is_more()) {
      let mut state_guard = self.state.lock().await;
      // Only transition if still ExpectingReply; could have been changed by pipe_detached or error.
      if matches!(*state_guard, ReqState::ExpectingReply { .. }) {
        *state_guard = ReqState::ReadyToSend;
        // Notify any other tasks that might be waiting on state change, though less common for REQ.
        self.reply_available_notifier.notify_waiters();
      }
    } else if result.is_err() {
      // On any error from recv_message (Timeout, ResourceLimitReached, QueueClosed),
      // reset state to allow a new request.
      let mut state_guard = self.state.lock().await;
      *state_guard = ReqState::ReadyToSend;
      self.reply_available_notifier.notify_waiters();
    }
    result
  }

  async fn send_multipart(&self, _frames: Vec<Msg>) -> Result<(), ZmqError> {
    tracing::warn!(
      handle = self.core.handle,
      "REQ socket: send_multipart() called. REQ sockets should use send() for single-part requests."
    );
    Err(ZmqError::UnsupportedFeature(
      "REQ sockets use send() for single-part requests. Use DEALER for general multipart messaging.".into(),
    ))
  }

  async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
    if !self.core.is_running().await {
      return Err(ZmqError::InvalidState("Socket is closing".into()));
    }
    // Check REQ state
    {
      let op_state_guard = self.state.lock().await;
      if !matches!(*op_state_guard, ReqState::ExpectingReply { .. }) {
        return Err(ZmqError::InvalidState(
          "REQ socket must call send() before receiving reply",
        ));
      }
    }

    let rcvtimeo_opt: Option<Duration> = { self.core.core_state.read().options.rcvtimeo };
    // For REQ, QItem is Vec<Msg> (the reply payload parts). The transform closure is identity.
    let transform_fn = |q_item: Vec<Msg>| q_item;
    let result = self
      .incoming_orchestrator
      .recv_logical_message(rcvtimeo_opt, transform_fn)
      .await;

    // After receiving the full logical message (or erroring), transition state.
    let mut state_guard = self.state.lock().await;
    *state_guard = ReqState::ReadyToSend;
    self.reply_available_notifier.notify_waiters();

    result
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

  async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
    match event {
      Command::PipeMessageReceived { msg, .. } => {
        let source_uri = { self.pipe_read_to_endpoint_uri.read().get(&pipe_read_id).cloned() };

        let source_uri = match source_uri {
          Some(s_uri) => s_uri,
          None => {
            tracing::warn!(
              handle = self.core.handle,
              pipe_id = pipe_read_id,
              "REQ received reply from unknown pipe (no URI mapping). Dropping frame."
            );
            return Ok(());
          }
        };

        let (is_expected_reply, notifier_clone_opt) = {
          let op_state_guard = self.state.lock().await;
          match &*op_state_guard {
            ReqState::ExpectingReply { target_endpoint_uri } => (
              *target_endpoint_uri == source_uri,
              Some(self.reply_available_notifier.clone()),
            ),
            ReqState::ReadyToSend => (false, None),
          }
        };

        if !is_expected_reply {
          tracing::warn!(handle = self.core.handle, source_pipe_id = pipe_read_id, source_uri = %source_uri, "REQ received reply from unexpected peer or in wrong state. Dropping frame.");
          return Ok(());
        }

        if let Some(raw_zmtp_reply_vec) = self.incoming_orchestrator.accumulate_pipe_frame(pipe_read_id, msg)? {
          match self.process_incoming_zmtp_message_for_req(pipe_read_id, raw_zmtp_reply_vec) {
            Ok(reply_payload_parts) => {
              // Release lock before await on queue_item
              // The notifier_clone_opt needs to be handled carefully if op_state_guard is re-acquired
              // For simplicity, just use self.reply_available_notifier after queue_item completes or errors.
              if self
                .incoming_orchestrator
                .queue_item(pipe_read_id, reply_payload_parts)
                .await
                .is_err()
              {
                tracing::error!(
                  handle = self.core.handle,
                  pipe_id = pipe_read_id,
                  "REQ: Failed to push reply to orchestrator queue."
                );
                let mut state_guard_err = self.state.lock().await;
                *state_guard_err = ReqState::ReadyToSend;
                self.reply_available_notifier.notify_waiters();
              } else {
                self.reply_available_notifier.notify_one();
              }
            }
            Err(e) => {
              tracing::error!(
                handle = self.core.handle,
                pipe_id = pipe_read_id,
                "REQ: Error processing ZMTP reply: {}. Dropping.",
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

  async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, _peer_identity: Option<&[u8]>) {
    let endpoint_uri_option = {
      self
        .core
        .core_state
        .read()
        .pipe_read_id_to_endpoint_uri
        .get(&pipe_read_id)
        .cloned()
    };
    if let Some(endpoint_uri) = endpoint_uri_option {
      tracing::debug!(handle = self.core.handle, pipe_read_id, uri = %endpoint_uri, "REQ attaching connection");
      self
        .pipe_read_to_endpoint_uri
        .write()
        .insert(pipe_read_id, endpoint_uri.clone());
      self.load_balancer.add_connection(endpoint_uri);
    } else {
      tracing::warn!(
        handle = self.core.handle,
        pipe_read_id,
        "REQ pipe_attached: Endpoint URI not found. Maps not updated."
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
    tracing::debug!(handle = self.core.handle, pipe_read_id, "REQ detaching pipe");
    let maybe_endpoint_uri = self.pipe_read_to_endpoint_uri.write().remove(&pipe_read_id);

    if let Some(detached_uri) = maybe_endpoint_uri {
      self.load_balancer.remove_connection(&detached_uri);
      let mut op_state_guard = self.state.lock().await;
      if let ReqState::ExpectingReply {
        ref target_endpoint_uri,
      } = *op_state_guard
      {
        if *target_endpoint_uri == detached_uri {
          tracing::warn!(handle = self.core.handle, pipe_id = pipe_read_id, uri = %detached_uri, "Target REP peer detached while REQ was expecting reply. Resetting state and notifying recv.");
          *op_state_guard = ReqState::ReadyToSend;
          self.reply_available_notifier.notify_one();
        }
      }
    }
    self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
  }
}
