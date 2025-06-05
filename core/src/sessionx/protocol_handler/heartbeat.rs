// core/src/sessionx/protocol_handler/heartbeat.rs

#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::engine::ZmtpStdStream;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::ZmtpCommand;

use std::time::{Duration, Instant};

/// State specific to the ZMTP handshake process.
#[derive(Debug)]
pub(crate) struct ZmtpHandshakeStateX {
  pub sub_phase: super::HandshakeSubPhaseX,
  // Potential future additions:
  // pub greeting_sent: bool,
  // pub peer_greeting_received: bool,
  // pub own_ready_sent: bool,
  // pub peer_ready_received: bool,
}

impl ZmtpHandshakeStateX {
  pub(crate) fn new() -> Self {
    Self {
      sub_phase: super::HandshakeSubPhaseX::GreetingExchange,
    }
  }
}

/// State specific to ZMTP heartbeat (PING/PONG) management.
#[derive(Debug)]
pub(crate) struct ZmtpHeartbeatStateX {
  pub last_activity_time: Instant,
  pub last_ping_sent_time: Option<Instant>,
  pub waiting_for_pong: bool,
  pub ivl: Option<Duration>,
  pub timeout: Duration,
}

impl ZmtpHeartbeatStateX {
  pub(crate) fn new(ivl: Option<Duration>, timeout: Duration) -> Self {
    Self {
      last_activity_time: Instant::now(),
      last_ping_sent_time: None,
      waiting_for_pong: false,
      ivl,
      timeout,
    }
  }

  pub(crate) fn record_activity(&mut self) {
    self.last_activity_time = Instant::now();
  }

  pub(crate) fn ping_sent(&mut self) {
    self.last_ping_sent_time = Some(Instant::now());
    self.waiting_for_pong = true;
    self.last_activity_time = Instant::now(); // Sending a PING is activity
  }

  pub(crate) fn pong_received(&mut self) {
    self.waiting_for_pong = false;
    self.last_ping_sent_time = None;
    // Receiving a PONG is activity, record_activity() will be called by the frame processing logic.
  }

  pub(crate) fn should_send_ping(&self, now: Instant) -> bool {
    if self.waiting_for_pong {
      return false;
    }
    match self.ivl {
      Some(interval) => now.duration_since(self.last_activity_time) >= interval,
      None => false,
    }
  }

  pub(crate) fn has_pong_timed_out(&self, now: Instant) -> bool {
    if !self.waiting_for_pong {
      return false;
    }
    match self.last_ping_sent_time {
      Some(ping_sent_at) => now.duration_since(ping_sent_at) >= self.timeout,
      None => false, // Should not happen if waiting_for_pong is true
    }
  }

  /// Returns the Instant when a PONG is expected by, if a PING has been sent.
  pub(crate) fn get_pong_deadline(&self) -> Option<Instant> {
    if self.waiting_for_pong {
      self.last_ping_sent_time.map(|sent_at| sent_at + self.timeout)
    } else {
      None
    }
  }
}

pub(crate) fn process_heartbeat_command_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  cmd_msg: &Msg,
) -> Result<Option<Msg>, ZmqError> {
  tracing::trace!(
    sca_handle = handler.actor_handle,
    "Processing incoming command frame in data phase for heartbeat."
  );
  match ZmtpCommand::parse(cmd_msg) {
    Some(ZmtpCommand::Ping(ping_context_payload)) => {
      // This payload is just the context part
      tracing::debug!(
        sca_handle = handler.actor_handle,
        ping_payload_len = ping_context_payload.len(),
        "Received PING, preparing PONG."
      );
      // The ping_context_payload from ZmtpCommand::Ping is already just the <Context> part
      // because ZmtpCommand::parse extracts it as &body[5+2..]
      let pong_reply_msg = ZmtpCommand::create_pong(&ping_context_payload);
      Ok(Some(pong_reply_msg))
    }
    Some(ZmtpCommand::Pong(_pong_context_payload)) => {
      tracing::debug!(sca_handle = handler.actor_handle, "Received PONG.");
      handler.heartbeat_state.pong_received();
      Ok(None)
    }
    Some(ZmtpCommand::Error) => Err(ZmqError::ProtocolViolation("Received ZMTP ERROR from peer".into())),
    Some(other) => {
      Ok(None) // Ignore other commands
    }
    None => Err(ZmqError::ProtocolViolation(
      "Unparseable command received in data phase".into(),
    )),
  }
}

pub(crate) async fn try_send_ping_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<(), ZmqError> {
  if handler.heartbeat_state.should_send_ping(Instant::now()) {
    tracing::debug!(
      sca_handle = handler.actor_handle,
      "Heartbeat: Sending PING due to inactivity."
    );
    // ZMTP PING: Default TTL 0 (hops), empty context b""
    let ping_msg = ZmtpCommand::create_ping(0, b"");

    // Use the data_io module's write function
    let _was_last_part = super::data_io::write_data_msg_impl(handler, ping_msg, true).await?;

    handler.heartbeat_state.ping_sent();
  }
  Ok(())
}

pub(crate) fn check_pong_timeout_impl(heartbeat_state: &ZmtpHeartbeatStateX, now: Instant) -> bool {
  heartbeat_state.has_pong_timed_out(now)
}
