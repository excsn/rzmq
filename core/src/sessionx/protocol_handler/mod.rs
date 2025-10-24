#![allow(dead_code, unused_variables, unused_mut)]

mod data_io;
mod handshake;
mod heartbeat;

use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::greeting::ZmtpGreeting;
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::security::{IDataCipher, Mechanism, NullMechanism};
use crate::socket::options::ZmtpEngineConfig;
use crate::transport::ZmtpStdStream;

use bytes::BytesMut;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::AsyncWriteExt;


use self::heartbeat::ZmtpHeartbeatStateX;
#[cfg(target_os = "linux")]
use super::cork::{try_create_cork_info, TcpCorkInfoX};
use super::types::{HandshakeSubPhaseX, ZmtpHandshakeProgressX};
use heartbeat::ZmtpHandshakeStateX;

pub(crate) struct ZmtpProtocolHandlerX<S: ZmtpStdStream> {
  // --- Shared State accessible by sub-module functions via &self or &mut self ---
  pub(crate) config: Arc<ZmtpEngineConfig>,
  pub(crate) is_server: bool,
  pub(crate) stream: Option<S>,

  pub(crate) network_read_buffer: BytesMut,
  pub(crate) plaintext_feed_buffer: BytesMut,

  pub(crate) handshake_state: ZmtpHandshakeStateX,
  pub(crate) security_mechanism: Box<dyn Mechanism>,
  pub(crate) pending_peer_greeting: Option<ZmtpGreeting>,

  pub(crate) data_cipher: Option<Box<dyn IDataCipher>>,
  pub(crate) zmtp_manual_parser: ZmtpManualParser,

  pub(crate) heartbeat_state: ZmtpHeartbeatStateX,

  #[cfg(target_os = "linux")]
  pub(crate) cork_info: Option<TcpCorkInfoX>,
  #[cfg(not(target_os = "linux"))]
  pub(crate) cork_info: Option<()>,

  pub(crate) actor_handle: usize,
}

impl<S: ZmtpStdStream + fmt::Debug> fmt::Debug for ZmtpProtocolHandlerX<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut debug_struct = f.debug_struct("ZmtpProtocolHandlerX");
    debug_struct
      .field("actor_handle", &self.actor_handle)
      .field("config", &self.config)
      .field("is_server", &self.is_server)
      .field("stream_is_some", &self.stream.is_some());
    debug_struct
      .field("network_read_buffer_len", &self.network_read_buffer.len())
      .field(
        "plaintext_feed_buffer_len",
        &self.plaintext_feed_buffer.len(),
      );
    debug_struct
      .field("handshake_state", &self.handshake_state)
      .field("security_mechanism_name", &self.security_mechanism.name())
      .field("pending_peer_greeting", &self.pending_peer_greeting);
    debug_struct.field("data_cipher_is_some", &self.data_cipher.is_some());
    debug_struct
      .field("zmtp_manual_parser", &self.zmtp_manual_parser)
      .field("heartbeat_state", &self.heartbeat_state);
    #[cfg(target_os = "linux")]
    debug_struct.field("cork_info", &self.cork_info);
    #[cfg(not(target_os = "linux"))]
    debug_struct.field("cork_info", &"None (Non-Linux)"); // Or just self.cork_info if it's Option<()>
    debug_struct.finish_non_exhaustive()
  }
}

impl<S: ZmtpStdStream> ZmtpProtocolHandlerX<S> {
  pub(crate) fn new(
    stream: S, // Take ownership of the stream directly
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

    #[cfg(target_os = "linux")]
    let cork_info_val = {
      // Assuming S: AsRawFd is handled by the caller passing an appropriate stream
      // when config.use_cork is true.
      let stream_ref_for_cork: Option<&S> = if config.use_cork { Some(&stream) } else { None };
      try_create_cork_info(stream_ref_for_cork, config.use_cork)
    };
    #[cfg(not(target_os = "linux"))]
    let cork_info_val: Option<()> = None;

    Self {
      actor_handle,
      config,
      is_server,
      stream: Some(stream),
      network_read_buffer: BytesMut::with_capacity(8192 * 2),
      plaintext_feed_buffer: BytesMut::with_capacity(8192 * 2),
      handshake_state: ZmtpHandshakeStateX::new(),
      security_mechanism: Box::new(NullMechanism),
      pending_peer_greeting: None,
      data_cipher: None,
      zmtp_manual_parser: ZmtpManualParser::new(),
      heartbeat_state: ZmtpHeartbeatStateX::new(
        heartbeat_ivl_from_config,
        effective_timeout_corrected,
      ),
      cork_info: cork_info_val,
    }
  }

  /// Indicates if the ZMTP handshake (Greeting, Security, Ready) is fully completed.
  pub(crate) fn is_handshake_complete(&self) -> bool {
    self.handshake_state.sub_phase == HandshakeSubPhaseX::Done
  }

  // --- Main Public Methods Delegating to Sub-Modules ---

  /// Advances the ZMTP handshake state machine by one step.
  /// Called repeatedly by SessionConnectionActorX until handshake completes or fails.
  pub(crate) async fn advance_handshake(&mut self) -> Result<ZmtpHandshakeProgressX, ZmqError> {
    handshake::advance_handshake_step_impl(self).await
  }

  /// Reads from the stream, decrypts (if applicable), and parses one full ZMTP message
  /// (data or command) during the operational data phase.
  pub(crate) async fn read_and_parse_data_frame(&mut self) -> Result<Option<Msg>, ZmqError> {
    data_io::read_data_frame_impl(self).await
  }

  /// Frames, encrypts (if applicable), manages TCP_CORK, and writes a ZMTP message
  /// to the stream during the operational data phase.
  ///
  /// `is_first_part_of_logical_zmq_msg`: True if this ZMTP frame is the first frame
  /// of a (potentially multi-frame) logical ZMQ message. Used for TCP_CORK.
  ///
  /// Returns `Ok(true)` if this was the last ZMTP frame of the logical ZMQ message,
  /// `Ok(false)` otherwise. This return value helps the caller manage its own
  /// state for `is_first_part_of_logical_zmq_msg` for subsequent calls.
  pub(crate) async fn write_data_msg(
    &mut self,
    msg: Msg,
    is_first_part_of_logical_zmq_msg: bool,
  ) -> Result<bool /* was_last_part_of_logical_zmq_msg */, ZmqError> {
    data_io::write_data_msg_impl(self, msg, is_first_part_of_logical_zmq_msg).await
  }

  /// Frames, encrypts, and writes a full logical ZMQ message (one or more parts)
  /// to the stream. This is the new, preferred method for sending data.
  pub(crate) async fn write_data_msgs(&mut self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    data_io::write_data_msgs_impl(self, msgs).await
  }
  
  /// Processes an incoming ZMTP command frame received during the data phase,
  /// primarily for handling PING/PONG.
  ///
  /// Returns `Ok(Some(Msg))` if a PONG reply needs to be sent.
  pub(crate) fn process_incoming_data_command_frame(
    &mut self,
    cmd_msg: &Msg,
  ) -> Result<Option<Msg>, ZmqError> {
    heartbeat::process_heartbeat_command_impl(self, cmd_msg)
  }

  /// Checks if a ZMTP PING should be sent due to inactivity and sends it if needed.
  pub(crate) async fn maybe_send_ping(&mut self) -> Result<(), ZmqError> {
    heartbeat::try_send_ping_impl(self).await
  }

  /// Checks if the timeout for an expected PONG reply has been exceeded.
  pub(crate) fn has_pong_timed_out(&self) -> bool {
    heartbeat::check_pong_timeout_impl(&self.heartbeat_state, Instant::now())
  }

  /// Initiates a graceful shutdown of the underlying stream.
  /// This includes uncorking if TCP_CORK was active.
  pub(crate) async fn initiate_stream_shutdown(&mut self) -> Result<(), ZmqError> {
    tracing::debug!(
      sca_handle = self.actor_handle,
      "ZmtpProtocolHandlerX: Initiating stream shutdown."
    );
    if let Some(stream_ref) = self.stream.as_mut() {
      #[cfg(target_os = "linux")]
      {
        if let Some(cork_info_ref) = self.cork_info.as_mut() {
          if cork_info_ref.is_corked() {
            cork_info_ref
              .apply_cork_state(false, self.actor_handle)
              .await;
          }
        }
      }
      // Attempt graceful shutdown of the write side of the stream.
      match stream_ref.shutdown().await {
        Ok(()) => tracing::debug!(
          sca_handle = self.actor_handle,
          "Stream shutdown() successful."
        ),
        Err(e) => {
          tracing::warn!(sca_handle = self.actor_handle, error = %e, "Error during stream shutdown().")
        }
      }
    }
    self.stream = None; // Drop the stream, which should close it.
    Ok(())
  }
}
