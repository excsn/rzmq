#![allow(dead_code, unused_variables, unused_mut)]

mod data_io;
mod handshake;
mod heartbeat;

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::protocol::zmtp::greeting::ZmtpGreeting;
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::security::framer::{ISecureFramer, NullFramer};
use crate::security::{Mechanism, NullMechanism};
use crate::socket::options::ZmtpEngineConfig;
use crate::transport::{ZmtpReadHalf, ZmtpStdStream};

use bytes::BytesMut;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;

use self::heartbeat::ZmtpHeartbeatStateX;
#[cfg(target_os = "linux")]
use super::cork::{TcpCorkInfoX, try_create_cork_info};
use super::types::{ConnectionPhaseX, HandshakeSubPhaseX, ZmtpHandshakeProgressX};
use heartbeat::ZmtpHandshakeStateX;

pub(crate) enum NetworkActionX {
  HandshakeProgress(ZmtpHandshakeProgressX),
  DataBatch(FrameBatch),
}

pub(crate) struct ZmtpProtocolHandlerX<S: ZmtpStdStream> {
  pub(crate) config: Arc<ZmtpEngineConfig>,
  pub(crate) is_server: bool,

  /// The owned read half — `None` once the actor extracts it into a local variable
  /// for use in the concurrent ingress `select!` arm.
  pub(crate) read_half: Option<S::ReadHalf>,
  /// The owned write half — `None` once the actor extracts it for the egress arm.
  pub(crate) write_half: Option<S::WriteHalf>,

  pub(crate) network_read_buffer: BytesMut,

  pub(crate) handshake_state: ZmtpHandshakeStateX,
  pub(crate) security_mechanism: Box<dyn Mechanism>,
  pub(crate) pending_peer_greeting: Option<ZmtpGreeting>,
  pub(crate) zmtp_manual_parser: ZmtpManualParser,
  pub(crate) framer: Box<dyn ISecureFramer>,

  pub(crate) heartbeat_state: ZmtpHeartbeatStateX,

  #[cfg(target_os = "linux")]
  pub(crate) cork_info: Option<TcpCorkInfoX>,
  #[cfg(not(target_os = "linux"))]
  pub(crate) cork_info: Option<()>,

  /// Partially-consumed inbound chunk held between `read_data_frames_batch_impl` calls.
  /// Only active on io-uring connections with a NULL/PLAIN framer (no encryption).
  #[cfg(feature = "io-uring")]
  pub(crate) active_lease: Option<bytes::Bytes>,

  pub(crate) actor_handle: usize,
}

impl<S: ZmtpStdStream + fmt::Debug> fmt::Debug for ZmtpProtocolHandlerX<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ZmtpProtocolHandlerX")
      .field("actor_handle", &self.actor_handle)
      .field("config", &self.config)
      .field("is_server", &self.is_server)
      .field("read_half_is_some", &self.read_half.is_some())
      .field("write_half_is_some", &self.write_half.is_some())
      .field("network_read_buffer_len", &self.network_read_buffer.len())
      .field("framer_active", &"true")
      .field("handshake_state", &self.handshake_state)
      .field("security_mechanism_name", &self.security_mechanism.name())
      .field("pending_peer_greeting", &self.pending_peer_greeting)
      .field("heartbeat_state", &self.heartbeat_state)
      .finish_non_exhaustive()
  }
}

impl<S: ZmtpStdStream> ZmtpProtocolHandlerX<S> {
  pub(crate) fn new(
    stream: S,
    config: Arc<ZmtpEngineConfig>,
    is_server: bool,
    actor_handle: usize,
  ) -> Self {
    let heartbeat_ivl_from_config = config.heartbeat_ivl;
    let effective_timeout_corrected = config.heartbeat_timeout.unwrap_or_else(|| {
      heartbeat_ivl_from_config.map_or(Duration::from_secs(30), |ivl| {
        ivl.max(Duration::from_millis(100))
      })
    });
    let max_msg_size = config.max_msg_size;
    let rcvbuf = config.rcvbuf.unwrap_or(65536);

    // Capture the raw fd for cork setup BEFORE consuming the stream via into_split.
    #[cfg(target_os = "linux")]
    let cork_info_val = {
      let stream_ref_for_cork: Option<&S> = if config.use_cork { Some(&stream) } else { None };
      try_create_cork_info(stream_ref_for_cork, config.use_cork)
    };
    #[cfg(not(target_os = "linux"))]
    let cork_info_val: Option<()> = None;

    // Split the stream into independent owned halves.
    let (read_half, write_half) = stream.into_split();

    Self {
      actor_handle,
      config,
      is_server,
      read_half: Some(read_half),
      write_half: Some(write_half),
      network_read_buffer: BytesMut::with_capacity(rcvbuf),
      handshake_state: ZmtpHandshakeStateX::new(),
      security_mechanism: Box::new(NullMechanism),
      pending_peer_greeting: None,
      zmtp_manual_parser: ZmtpManualParser::new(max_msg_size),
      framer: Box::new(NullFramer::new(max_msg_size)),
      heartbeat_state: ZmtpHeartbeatStateX::new(
        heartbeat_ivl_from_config,
        effective_timeout_corrected,
      ),
      cork_info: cork_info_val,
      #[cfg(feature = "io-uring")]
      active_lease: None,
    }
  }

  pub(crate) fn is_handshake_complete(&self) -> bool {
    self.handshake_state.sub_phase == HandshakeSubPhaseX::Done
  }

  // --- Main Public Methods ---

  pub(crate) async fn advance_handshake(&mut self) -> Result<ZmtpHandshakeProgressX, ZmqError> {
    handshake::advance_handshake_step_impl(self).await
  }

  /// Reads from the read half, decrypts (if applicable), and parses one full ZMTP message.
  /// Used by legacy callers; the actor uses `read_and_parse_data_frames_batch` instead.
  pub(crate) async fn read_and_parse_data_frame(&mut self) -> Result<Option<Msg>, ZmqError> {
    data_io::read_data_frame_impl(self).await
  }

  /// Greedy inbound read: one async kernel read + synchronous drain of all complete frames.
  ///
  /// `reader` is the extracted local read half from the actor's `run_loop`. The handler's
  /// own `read_half` field will be `None` at this point (taken out by the actor).
  pub(crate) async fn read_and_parse_data_frames_batch(
    &mut self,
    reader: &mut S::ReadHalf,
  ) -> Result<FrameBatch, ZmqError> {
    data_io::read_data_frames_batch_impl(self, reader).await
  }

  pub(crate) async fn write_data_msg(
    &mut self,
    msg: Msg,
    is_first_part_of_logical_zmq_msg: bool,
  ) -> Result<bool, ZmqError> {
    data_io::write_data_msg_impl(self, msg, is_first_part_of_logical_zmq_msg).await
  }

  pub(crate) async fn write_data_msgs(&mut self, msgs: FrameBatch) -> Result<(), ZmqError> {
    data_io::write_data_msgs_impl(self, msgs).await
  }

  pub(crate) async fn write_data_batch(&mut self, batch: &[FrameBatch]) -> Result<(), ZmqError> {
    data_io::write_data_batch_impl(self, batch).await
  }

  pub(crate) fn process_incoming_data_command_frame(
    &mut self,
    cmd_msg: &Msg,
  ) -> Result<Option<Msg>, ZmqError> {
    heartbeat::process_heartbeat_command_impl(self, cmd_msg)
  }

  pub(crate) async fn maybe_send_ping(&mut self) -> Result<(), ZmqError> {
    heartbeat::try_send_ping_impl(self).await
  }

  pub(crate) fn frame_outgoing_batch(&mut self, batch: &[FrameBatch]) -> Result<bytes::Bytes, ZmqError> {
    self.framer.write_msg_batch(batch)
  }

  pub(crate) fn frame_outgoing_batch_vectored(
    &mut self,
    batch: &[FrameBatch],
  ) -> Result<Vec<bytes::Bytes>, ZmqError> {
    let total_payload: usize = batch.iter().flat_map(|g| g.iter().map(|m| m.size())).sum();
    if total_payload < self.config.sndbatch_bytes {
      Ok(vec![self.framer.write_msg_batch(batch)?])
    } else {
      self.framer.frame_vectored(batch)
    }
  }

  pub(crate) fn frame_outgoing_msgs(&mut self, msgs: FrameBatch) -> Result<bytes::Bytes, ZmqError> {
    self.framer.write_msg_multipart(msgs)
  }

  pub(crate) fn maybe_build_ping(&mut self) -> Result<Option<bytes::Bytes>, ZmqError> {
    heartbeat::try_build_ping_impl(self)
  }

  /// Thin delegation used by `ZmqMessageProcessor` to parse/decrypt one frame
  /// from an externally managed byte buffer without yielding to the runtime.
  /// The framer stays in the handler so its cipher state (nonces) remains unified
  /// across the read and write directions — no cipher split required.
  pub(crate) fn try_read_msg_from_buffer(
    &mut self,
    buf: &mut bytes::BytesMut,
  ) -> Result<Option<Msg>, ZmqError> {
    self.framer.try_read_msg(buf)
  }

  pub(crate) fn has_pong_timed_out(&self) -> bool {
    heartbeat::check_pong_timeout_impl(&self.heartbeat_state, Instant::now())
  }

  /// Graceful shutdown: uncork if needed, then shut down the write half.
  /// The actor restores `write_half` before calling this if it had taken it out.
  pub(crate) async fn initiate_stream_shutdown(&mut self) -> Result<(), ZmqError> {
    tracing::debug!(
      sca_handle = self.actor_handle,
      "ZmtpProtocolHandlerX: Initiating stream shutdown."
    );

    self.clear_handshake_state();

    if let Some(mut w) = self.write_half.take() {
      #[cfg(target_os = "linux")]
      {
        if let Some(cork_info_ref) = self.cork_info.as_mut() {
          if cork_info_ref.is_corked() {
            cork_info_ref.apply_cork_state(false, self.actor_handle).await;
          }
        }
      }
      match w.shutdown().await {
        Ok(()) => tracing::debug!(sca_handle = self.actor_handle, "Write half shutdown() successful."),
        Err(e) => tracing::warn!(sca_handle = self.actor_handle, error = %e, "Error during write half shutdown()."),
      }
    }
    self.read_half = None;
    Ok(())
  }

  fn clear_handshake_state(&mut self) {
    self.pending_peer_greeting = None;
    self.network_read_buffer.clear();
    if self.security_mechanism.name() != NullMechanism::NAME {
      self.security_mechanism = Box::new(NullMechanism);
    }
    self.framer = Box::new(NullFramer::new(self.config.max_msg_size));
  }
}
