// src/engine/core.rs

use crate::engine::ZmtpStdStream;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::protocol::zmtp::ZmtpReady;
use crate::protocol::zmtp::{
  codec::ZmtpCodec,
  command::ZmtpCommand,
  greeting::{ZmtpGreeting, GREETING_LENGTH},
};
use crate::runtime::{ActorType, Command, MailboxReceiver, MailboxSender};
#[cfg(feature = "noise_xx")]
use crate::security::NoiseXxMechanism;
use crate::security::{negotiate_security_mechanism, IDataCipher};
use crate::security::{null::NullMechanism, plain::PlainMechanism, Mechanism, MechanismStatus};
use crate::socket::options::ZmtpEngineConfig;
use crate::{Blob, Context, MsgFlags};

use std::collections::HashMap;
use std::os::fd::AsRawFd;
use std::time::{Duration, Instant};
use std::{fmt, marker::PhantomData};

use bytes::{BufMut, BytesMut};
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::interval;
use tokio_util::codec::{Encoder, Framed};

#[cfg(target_os = "linux")]
use std::os::fd::RawFd;

/// Core ZMTP engine logic, generic over the underlying stream type `S`.
/// This struct encapsulates the state and logic for handling the ZMTP protocol,
/// including handshakes (greeting, security, ready commands) and message exchange.
/// It is designed to run as an independent actor task.
pub(crate) struct ZmtpEngineCoreStd<S: ZmtpStdStream> {
  /// Unique handle ID for this Engine instance, primarily for logging and debugging.
  handle: usize,
  /// Mailbox sender to communicate commands *to* the associated Session actor.
  session_mailbox: MailboxSender,
  /// Mailbox receiver for commands *from* the associated Session actor.
  mailbox_receiver: MailboxReceiver,
  stream: Option<S>,
  /// Configuration specific to the ZMTP engine's behavior.
  config: ZmtpEngineConfig,
  /// The active security mechanism.
  mechanism: Box<dyn Mechanism>,
  data_cipher: Option<Box<dyn IDataCipher>>,
  /// Indicates if this engine instance is operating in a server role.
  is_server: bool,
  /// Marker for the generic stream type `S`.
  _marker: PhantomData<S>,
  // --- Heartbeat and Keepalive State ---
  last_activity_time: Instant,
  last_ping_sent_time: Option<Instant>,
  waiting_for_pong: bool,
  heartbeat_ivl: Option<Duration>,
  heartbeat_timeout: Duration,
  /// Clone of the rzmq `Context`, needed for publishing `ActorStopping` events.
  context: Context,
  #[cfg(target_os = "linux")]
  is_corked: bool,
  #[cfg(target_os = "linux")]
  stream_fd_for_cork: Option<RawFd>,
  // Tracks if the *next* send is the first frame of a new logical ZMQ message
  expecting_first_frame_of_msg: bool,
}

impl<S: ZmtpStdStream> fmt::Debug for ZmtpEngineCoreStd<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut debug_struct = f.debug_struct("ZmtpEngineCoreStd");
    debug_struct
      .field("handle", &self.handle)
      .field("session_mailbox_closed", &self.session_mailbox.is_closed())
      .field("mailbox_receiver_closed", &self.mailbox_receiver.is_closed())
      .field("config", &self.config)
      .field("mechanism_status", &self.mechanism.status())
      .field("is_server", &self.is_server)
      .field("last_activity_time", &self.last_activity_time)
      .field("last_ping_sent_time", &self.last_ping_sent_time)
      .field("waiting_for_pong", &self.waiting_for_pong)
      .field("heartbeat_ivl", &self.heartbeat_ivl)
      .field("heartbeat_timeout", &self.heartbeat_timeout)
      .field("context_present", &true);

    #[cfg(target_os = "linux")]
    {
      debug_struct.field("is_corked", &self.is_corked);
      debug_struct.field("stream_fd_for_cork", &self.stream_fd_for_cork);
    }

    debug_struct.field("expecting_first_frame_of_msg", &self.expecting_first_frame_of_msg);
    debug_struct.finish_non_exhaustive()
  }
}

impl<S: ZmtpStdStream + AsRawFd> ZmtpEngineCoreStd<S> {
  pub fn new(
    handle: usize,
    session_mailbox: MailboxSender,
    mailbox_receiver: MailboxReceiver,
    stream: S,
    config: ZmtpEngineConfig,
    is_server: bool,
    context: Context,
  ) -> Self {
    let heartbeat_ivl_from_config = config.heartbeat_ivl;
    let effective_timeout_corrected = config
      .heartbeat_timeout
      .unwrap_or_else(|| heartbeat_ivl_from_config.map_or(Duration::from_secs(30), |ivl| ivl));

    #[cfg(target_os = "linux")]
    let initial_fd_for_cork = {
      if config.use_cork {
        Some(stream.as_raw_fd())
      } else {
        None
      }
    };

    Self {
      handle,
      session_mailbox,
      mailbox_receiver,
      stream: Some(stream),
      config,
      mechanism: Box::new(NullMechanism),
      data_cipher: None,
      is_server,
      _marker: PhantomData,
      last_activity_time: Instant::now(),
      last_ping_sent_time: None,
      waiting_for_pong: false,
      heartbeat_ivl: heartbeat_ivl_from_config,
      heartbeat_timeout: effective_timeout_corrected,
      context,
      #[cfg(target_os = "linux")]
      is_corked: false,
      #[cfg(target_os = "linux")]
      stream_fd_for_cork: initial_fd_for_cork,
      expecting_first_frame_of_msg: true,
    }
  }

  async fn exchange_greetings(&mut self, stream: &mut S) -> Result<ZmtpGreeting, ZmqError> {
    let mut greeting_buffer_to_send = BytesMut::with_capacity(GREETING_LENGTH);

    let own_greeting_mechanism_bytes: &'static [u8; 20] = {
      #[cfg(feature = "noise_xx")]
      if self.config.use_noise_xx {
        // If Noise is enabled in config, propose NOISE_XX.
        // Also, ensure necessary keys are present for this role.
        // Client needs remote_pk, Server just needs its own sk.
        let can_propose_noise = if self.is_server {
          self.config.noise_xx_local_sk_bytes_for_engine.is_some()
        } else {
          // Client
          self.config.noise_xx_local_sk_bytes_for_engine.is_some()
            && self.config.noise_xx_remote_pk_bytes_for_engine.is_some()
        };

        if can_propose_noise {
          tracing::debug!(engine_handle = self.handle, "Proposing NOISE_XX in own greeting.");
          NoiseXxMechanism::NAME_BYTES // This should be the 20-byte padded version
        } else {
          tracing::warn!(
            engine_handle = self.handle,
            "Noise_XX configured but required keys missing for this role. Falling back.."
          );
          if self.config.use_plain {
            tracing::debug!(
              engine_handle = self.handle,
              "Proposing PLAIN in own greeting (Noise fallback)."
            );
            PlainMechanism::NAME_BYTES
          } else {
            tracing::debug!(
              engine_handle = self.handle,
              "Proposing NULL in own greeting (Noise fallback, PLAIN disabled)."
            );
            NullMechanism::NAME_BYTES
          }
        }
      } else if self.config.use_plain {
        // NOISE_XX not enabled, but PLAIN is.
        tracing::debug!(engine_handle = self.handle, "Proposing PLAIN in own greeting.");
        PlainMechanism::NAME_BYTES
      } else {
        // Neither NOISE_XX nor PLAIN enabled, propose NULL.
        tracing::debug!(engine_handle = self.handle, "Proposing NULL in own greeting.");
        NullMechanism::NAME_BYTES
      }
      #[cfg(not(feature = "noise_xx"))] // If noise_xx feature is NOT compiled
      {
        // This block will execute if noise_xx is not a feature, overriding the above if block.
        if self.config.use_plain {
          tracing::debug!(
            engine_handle = self.handle,
            "Proposing PLAIN in own greeting (NOISE_XX feature disabled)."
          );
          PlainMechanism::NAME_BYTES
        } else {
          tracing::debug!(
            engine_handle = self.handle,
            "Proposing NULL in own greeting (NOISE_XX feature disabled, PLAIN disabled)."
          );
          NullMechanism::NAME_BYTES
        }
      }
    };

    ZmtpGreeting::encode(
      &own_greeting_mechanism_bytes,
      self.is_server,
      &mut greeting_buffer_to_send,
    );

    stream.write_all(&greeting_buffer_to_send).await?;
    stream.flush().await?;
    tracing::debug!(
      engine_handle = self.handle,
      role = if self.is_server { "Server" } else { "Client" },
      "Sent own ZMTP greeting (proposing: {}).",
      std::str::from_utf8(own_greeting_mechanism_bytes)
        .unwrap_or("")
        .trim_end_matches('\0')
    );
    self.last_activity_time = Instant::now();

    let mut received_greeting_bytes = BytesMut::with_capacity(GREETING_LENGTH);
    while received_greeting_bytes.len() < GREETING_LENGTH {
      let bytes_read = stream.read_buf(&mut received_greeting_bytes).await?;
      if bytes_read == 0 {
        tracing::error!(
          engine_handle = self.handle,
          "Connection closed by peer during raw greeting read (EOF)."
        );
        return Err(ZmqError::ConnectionClosed);
      }
      tracing::trace!(
        engine_handle = self.handle,
        bytes_read,
        current_buffer_len = received_greeting_bytes.len(),
        "Read raw greeting bytes from peer."
      );
      self.last_activity_time = Instant::now();
    }

    match ZmtpGreeting::decode(&mut received_greeting_bytes)? {
      Some(peer_greeting) => {
        tracing::debug!(
          engine_handle = self.handle,
          ?peer_greeting,
          "Received and parsed peer's ZMTP greeting (raw)."
        );
        Ok(peer_greeting)
      }
      None => {
        tracing::error!(
          engine_handle = self.handle,
          "Failed to decode full greeting buffer, this is unexpected."
        );
        Err(ZmqError::ProtocolViolation(
          "Greeting decode failed unexpectedly".into(),
        ))
      }
    }
  }

  async fn perform_security_and_ready_handshake(
    &mut self,
    framed_handshake_stream: &mut Framed<S, ZmtpCodec>, // S is the ZmtpStdStream
    peer_greeting: ZmtpGreeting,
  ) -> Result<(Box<dyn Mechanism>, Option<Blob>), ZmqError> {
    // --- Validate Peer's Greeting & Select Security Mechanism ---
    if peer_greeting.version.0 < 3 {
      let version_error_msg = format!(
        "Unsupported ZMTP version {}.{} received from peer.",
        peer_greeting.version.0, peer_greeting.version.1
      );
      tracing::error!(engine_handle = self.handle, error = %version_error_msg);
      return Err(ZmqError::ProtocolViolation(version_error_msg));
    }

    let mut negotiated_mechanism =
      negotiate_security_mechanism(self.is_server, &self.config, &peer_greeting, self.handle)?;

    // --- Security Handshake (Token Exchange using Framed stream) ---
    tracing::debug!(
      engine_handle = self.handle,
      mechanism = negotiated_mechanism.name(),
      "Starting security token exchange."
    );
    let handshake_timeout = self.config.handshake_timeout.unwrap_or(Duration::from_secs(30));

    // Refined loop for security token exchange
    loop {
      // 1. Produce and send any token our mechanism needs to send.
      if let Some(token_to_send_vec) = negotiated_mechanism.produce_token()? {
        let mut command_msg = Msg::from_vec(token_to_send_vec);
        command_msg.set_flags(MsgFlags::COMMAND);
        tracing::trace!(
          engine_handle = self.handle,
          mechanism = negotiated_mechanism.name(),
          token_size = command_msg.size(),
          "Sending security token."
        );
        match tokio::time::timeout(handshake_timeout, framed_handshake_stream.send(command_msg)).await {
          Ok(Ok(())) => { /* Sent successfully */ }
          Ok(Err(e)) => {
            negotiated_mechanism.set_error(format!("Stream error sending token: {}", e));
            tracing::error!(engine_handle = self.handle, error = %e, "Failed to send security token.");
            // No need to break here, error state will be caught by next check
          }
          Err(_) => {
            let err_msg = "Timeout sending security token".to_string();
            negotiated_mechanism.set_error(err_msg.clone());
            tracing::error!(engine_handle = self.handle, error = %err_msg);
            // No need to break here, error state will be caught by next check
          }
        }
        self.last_activity_time = Instant::now();
      }

      // 2. Check if the handshake is complete or an error occurred.
      //    This check is important after our send attempt or if we had nothing to send.
      if negotiated_mechanism.is_complete() || negotiated_mechanism.is_error() {
        tracing::debug!(engine_handle=self.handle, mechanism=negotiated_mechanism.name(), status=?negotiated_mechanism.status(), "Security token loop: Breaking due to completion or error.");
        break;
      }

      // 3. If handshake is not done, we must be waiting for the peer's next message.
      tracing::trace!(
        engine_handle = self.handle,
        mechanism = negotiated_mechanism.name(),
        "Waiting for security token from peer."
      );
      match tokio::time::timeout(handshake_timeout, framed_handshake_stream.next()).await {
        Ok(Some(Ok(received_msg))) => {
          self.last_activity_time = Instant::now();
          if !received_msg.is_command() {
            let protocol_err_msg = "Expected COMMAND frame during security handshake, got data frame.";
            negotiated_mechanism.set_error(protocol_err_msg.to_string());
            tracing::error!(engine_handle = self.handle, error = %protocol_err_msg);
            // Error state set, next loop iteration will break.
          } else {
            let token_data = received_msg.data().unwrap_or(&[]);
            tracing::trace!(
              engine_handle = self.handle,
              mechanism = negotiated_mechanism.name(),
              token_size = token_data.len(),
              "Received security token."
            );
            // Process token. This might generate a response to be sent in the next iteration's produce_token().
            // Or it might complete the handshake (e.g., server processing client's last message).
            // Or it might result in an error.
            if let Err(e) = negotiated_mechanism.process_token(token_data) {
              negotiated_mechanism.set_error(format!("Error processing token: {}", e)); // Ensure error state is set in mechanism
              tracing::error!(engine_handle = self.handle, error=%e, "Error processing received security token.");
              // Error state set, next loop iteration will break.
            }
          }
        }
        Ok(Some(Err(e))) => {
          negotiated_mechanism.set_error(format!("Stream error receiving token: {}", e));
          tracing::error!(engine_handle = self.handle, error = %e, "Stream error during security handshake.");
        }
        Ok(None) => {
          let err_msg = "Connection closed by peer during security handshake".to_string();
          negotiated_mechanism.set_error(err_msg.clone());
          tracing::error!(engine_handle = self.handle, error = %err_msg);
        }
        Err(_) => {
          let err_msg = "Timeout waiting for security token from peer".to_string();
          negotiated_mechanism.set_error(err_msg.clone());
          tracing::error!(engine_handle = self.handle, error = %err_msg);
        }
      }

      // ZAP check, if it's still relevant after processing a token
      if !negotiated_mechanism.is_complete() && !negotiated_mechanism.is_error() {
        if let Some(_zap_req_frames) = negotiated_mechanism.zap_request_needed() {
          let zap_err_msg = "ZAP authentication required but not implemented in this engine core".to_string();
          negotiated_mechanism.set_error(zap_err_msg.clone()); // This will cause loop to break on next iteration
          tracing::warn!(engine_handle = self.handle, %zap_err_msg);
        }
      }
      // Loop again. The is_complete() || is_error() check at the top will handle termination.
    }

    if negotiated_mechanism.is_error() {
      let reason = negotiated_mechanism
        .error_reason()
        .unwrap_or("Unknown mechanism handshake failure");
      tracing::error!(
        engine_handle = self.handle,
        mechanism = negotiated_mechanism.name(),
        reason = reason,
        "Security handshake failed."
      );
      return Err(ZmqError::SecurityError(reason.to_string()));
    }

    // If we break the loop because is_complete() is true, but not is_error(), then handshake is successful.
    tracing::info!(
      engine_handle = self.handle,
      mechanism = negotiated_mechanism.name(),
      "Security handshake successful."
    );

    // --- ZMTP READY Command Exchange ---
    let mut identity_from_ready_command: Option<Blob> = None;
    let local_socket_type_name = self.config.socket_type_name.clone();
    let local_routing_id = self.config.routing_id.clone();

    if !self.is_server {
      let mut client_ready_props = HashMap::new();
      client_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());
      if let Some(id_blob) = local_routing_id {
        if !id_blob.is_empty() && id_blob.len() <= 255 {
          tracing::info!(
            engine_handle = self.handle,
            role = "Client",
            socket_type = %local_socket_type_name,
            ready_identity_payload = ?String::from_utf8_lossy(id_blob.as_ref()),
            "Client engine constructing READY: Adding 'Identity' property."
          );
          client_ready_props.insert("Identity".to_string(), id_blob.to_vec());
        } else {
          tracing::warn!(
            engine_handle = self.handle,
            id_len = id_blob.len(),
            "Client local routing ID invalid (empty or too long), not sending in READY."
          );
        }
      } else {
        tracing::info!(
            engine_handle = self.handle,
            role = "Client",
            socket_type = %local_socket_type_name,
            "Client engine constructing READY: No local ROUTING_ID set, 'Identity' property will not be sent."
        );
      }
      let client_ready_msg = ZmtpReady::create_msg(client_ready_props);
      tracing::debug!(engine_handle = self.handle, "Client sending ZMTP READY command.");
      if let Err(e) = framed_handshake_stream.send(client_ready_msg).await {
        tracing::error!(engine_handle=self.handle, error=%e, "Failed to send client READY command.");
        return Err(e);
      }
      self.last_activity_time = Instant::now();
    }

    tracing::debug!(engine_handle = self.handle, "Waiting for peer's ZMTP READY command.");
    let peer_ready_command_parsed = loop {
      match tokio::time::timeout(handshake_timeout, framed_handshake_stream.next()).await {
        // Added timeout
        Ok(Some(Ok(received_msg))) => {
          self.last_activity_time = Instant::now();
          if !received_msg.is_command() {
            let err_msg = "Expected ZMTP READY command, but received a data frame.";
            tracing::error!(engine_handle = self.handle, %err_msg);
            return Err(ZmqError::ProtocolViolation(err_msg.to_string()));
          }
          match ZmtpCommand::parse(&received_msg) {
            Some(ZmtpCommand::Ready(ready_cmd_data)) => {
              break ready_cmd_data;
            }
            Some(other_cmd) => {
              let err_msg = format!(
                "Expected ZMTP READY command, but received other command: {:?}",
                other_cmd
              );
              tracing::error!(engine_handle = self.handle, %err_msg);
              return Err(ZmqError::ProtocolViolation(err_msg));
            }
            None => {
              let err_msg = "Received unparseable ZMTP command when expecting READY.";
              tracing::error!(engine_handle = self.handle, %err_msg);
              return Err(ZmqError::ProtocolViolation(err_msg.to_string()));
            }
          }
        }
        Ok(Some(Err(e))) => {
          tracing::error!(engine_handle = self.handle, error=%e, "Stream error while waiting for peer's READY command.");
          return Err(e);
        }
        Ok(None) => {
          let err_msg = "Connection closed by peer while waiting for READY command.";
          tracing::error!(engine_handle = self.handle, %err_msg);
          return Err(ZmqError::ConnectionClosed);
        }
        Err(_) => {
          // Timeout for READY
          tracing::error!(
            engine_handle = self.handle,
            "Timeout waiting for peer's ZMTP READY command."
          );
          return Err(ZmqError::Timeout);
        }
      }
    };
    tracing::debug!(engine_handle = self.handle, peer_ready_cmd = ?peer_ready_command_parsed, "Received peer's ZMTP READY command.");

    if let Some(id_bytes_vec) = peer_ready_command_parsed.properties.get("Identity") {
      if !id_bytes_vec.is_empty() && id_bytes_vec.len() <= 255 {
        let id_blob = Blob::from(id_bytes_vec.clone());
        let ready_identity_payload = String::from_utf8_lossy(id_blob.as_ref()).to_string();
        identity_from_ready_command = Some(id_blob);
        tracing::debug!(engine_handle = self.handle, peer_identity_from_ready = ?ready_identity_payload, "Extracted Identity from peer's READY command.");
      } else {
        tracing::warn!(
          engine_handle = self.handle,
          id_len = id_bytes_vec.len(),
          "Peer sent invalid Identity in READY (empty or too long)."
        );
      }
    }

    if self.is_server {
      let mut server_ready_props = HashMap::new();
      server_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());
      let server_ready_msg = ZmtpReady::create_msg(server_ready_props);
      tracing::debug!(engine_handle = self.handle, "Server sending ZMTP READY command.");

      // Corrected timeout handling for server's READY send
      match tokio::time::timeout(handshake_timeout, framed_handshake_stream.send(server_ready_msg)).await {
        Ok(Ok(())) => { /* Successfully sent server's READY */ }
        Ok(Err(send_err)) => {
          // Inner future (send) resulted in an error
          tracing::error!(engine_handle=self.handle, error=%send_err, "Failed to send server READY command.");
          return Err(send_err); // Propagate the ZmqError from send
        }
        Err(_timeout_elapsed) => {
          // Outer timeout occurred
          tracing::error!(engine_handle = self.handle, "Timeout sending server READY command.");
          return Err(ZmqError::Timeout);
        }
      }
      self.last_activity_time = Instant::now();
    }

    let identity_from_mechanism = negotiated_mechanism.peer_identity().map(Blob::from);
    let final_peer_identity = identity_from_mechanism.or(identity_from_ready_command);

    tracing::info!(
        engine_handle = self.handle,
        mechanism = negotiated_mechanism.name(),
        final_peer_id = ?final_peer_identity,
        "ZMTP Security & Ready Handshake fully completed."
    );
    Ok((negotiated_mechanism, final_peer_identity))
  }

  // Helper to set TCP_CORK
  #[cfg(target_os = "linux")]
  async fn set_tcp_cork_rawfd(fd: RawFd, enable: bool, engine_handle: usize) {
    use std::os::fd::FromRawFd;

    tracing::trace!(engine_handle, fd, cork_enable = enable, "Attempting to set TCP_CORK");
    // Using a blocking task for setsockopt as socket2 is blocking.
    // This is okay for infrequent operations like corking.
    let res = tokio::task::spawn_blocking(move || {
      let socket = unsafe { socket2::Socket::from_raw_fd(fd) };
      let result = socket.set_cork(enable);
      std::mem::forget(socket); // Crucial: prevent SockRef from closing the FD
      result
    })
    .await;

    match res {
      Ok(Ok(())) => tracing::debug!(
        engine_handle,
        fd,
        cork_enable = enable,
        "TCP_CORK successfully {}set",
        if enable { "" } else { "un" }
      ),
      Ok(Err(e)) => {
        tracing::warn!(engine_handle, fd, cork_enable = enable, error = %e, "Failed to set TCP_CORK socket option")
      }
      Err(join_err) => {
        tracing::error!(engine_handle, fd, cork_enable = enable, error = %join_err, "Task for setting TCP_CORK panicked")
      }
    }
  }

  pub async fn run_loop(mut self) {
    tokio::task::yield_now().await;

    let engine_actor_handle = self.handle;
    let engine_actor_type = ActorType::Engine;
    let engine_context_clone = self.context.clone();
    let role_str = if self.is_server { "Server" } else { "Client" };

    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCoreStd actor main loop starting."
    );

    // Buffer for raw network reads, used when a data_cipher is active.
    let mut network_read_buffer = BytesMut::with_capacity(8192 * 2);
    // Buffer to feed the ZmtpManualParser with plaintext ZMTP frames.
    let mut zmtp_plaintext_feed_buffer = BytesMut::with_capacity(8192 * 2);
    let mut zmtp_manual_parser = ZmtpManualParser::new(); // For parsing ZMTP frames in data phase

    let mut initial_raw_stream = match self.stream.take() {
      Some(s) => s,
      None => {
        let error = ZmqError::Internal("EngineCore started without stream".into());
        let _ = self
          .session_mailbox
          .send(Command::EngineError { error: error.clone() })
          .await;
        engine_context_clone.publish_actor_stopping(engine_actor_handle, engine_actor_type, None, Some(error));
        return;
      }
    };

    #[cfg(target_os = "linux")]
    if self.config.use_cork && self.stream_fd_for_cork.is_none() {
      self.stream_fd_for_cork = Some(initial_raw_stream.as_raw_fd());
      tracing::debug!(engine_handle = self.handle, fd = ?self.stream_fd_for_cork, "TCP_CORK enabled, cached FD from initial_raw_stream.");
    }

    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    // 1. ZMTP Greeting Exchange
    let peer_greeting = match self.exchange_greetings(&mut initial_raw_stream).await {
      Ok(pg) => pg,
      Err(greeting_err) => {
        final_error_for_actor_stop = Some(greeting_err.clone());
        let _ = self
          .session_mailbox
          .send(Command::EngineError { error: greeting_err })
          .await;
        engine_context_clone.publish_actor_stopping(
          engine_actor_handle,
          engine_actor_type,
          None,
          final_error_for_actor_stop.clone(),
        );
        return;
      }
    };

    // 2. Security Handshake & ZMTP READY Command Exchange (uses a temporary Framed stream)
    let mut framed_for_handshake_and_ready = Framed::new(initial_raw_stream, ZmtpCodec::new());

    let (negotiated_mechanism_box, final_peer_identity_for_session) = match self
      .perform_security_and_ready_handshake(&mut framed_for_handshake_and_ready, peer_greeting)
      .await
    {
      Ok((mech, peer_id_opt)) => (mech, peer_id_opt),
      Err(handshake_err) => {
        final_error_for_actor_stop = Some(handshake_err.clone());
        let _ = self
          .session_mailbox
          .send(Command::EngineError { error: handshake_err })
          .await;
        let _ = framed_for_handshake_and_ready.close().await;
        engine_context_clone.publish_actor_stopping(
          engine_actor_handle,
          engine_actor_type,
          None,
          final_error_for_actor_stop.clone(),
        );
        return;
      }
    };

    self.mechanism = negotiated_mechanism_box; // Store the final chosen mechanism

    // 3. Set up `self.data_cipher` based on the successfully negotiated mechanism
    if self.mechanism.status() == MechanismStatus::Ready {
      let mechanism_to_convert = std::mem::replace(&mut self.mechanism, Box::new(NullMechanism)); // Consume self.mechanism
      match mechanism_to_convert.into_data_cipher_parts() {
        Ok((cipher, _mechanism_peer_id)) => {
          // _mechanism_peer_id already factored into final_peer_identity_for_session
          tracing::info!(engine_handle = self.handle, "Data cipher obtained for data phase.");
          self.data_cipher = Some(cipher);
        }
        Err(e) => {
          tracing::error!(
            engine_handle = self.handle,
            "Failed to get data cipher from mechanism: {}",
            e
          );
          final_error_for_actor_stop = Some(e);
          let _ = self
            .session_mailbox
            .send(Command::EngineError {
              error: final_error_for_actor_stop.clone().unwrap(),
            })
            .await;
          let _ = framed_for_handshake_and_ready.close().await;
          engine_context_clone.publish_actor_stopping(
            engine_actor_handle,
            engine_actor_type,
            None,
            final_error_for_actor_stop.clone(),
          );
          return;
        }
      }
    } else {
      let err_msg = format!(
        "Mechanism not Ready (status: {:?}) after handshake phase.",
        self.mechanism.status()
      );
      tracing::error!(engine_handle = self.handle, "{}", err_msg);
      final_error_for_actor_stop = Some(ZmqError::Internal(err_msg));
      let _ = self
        .session_mailbox
        .send(Command::EngineError {
          error: final_error_for_actor_stop.clone().unwrap(),
        })
        .await;
      let _ = framed_for_handshake_and_ready.close().await;
      engine_context_clone.publish_actor_stopping(
        engine_actor_handle,
        engine_actor_type,
        None,
        final_error_for_actor_stop.clone(),
      );
      return;
    }

    // 4. Send EngineReady to Session
    if self
      .session_mailbox
      .send(Command::EngineReady {
        peer_identity: final_peer_identity_for_session,
      })
      .await
      .is_err()
    {
      tracing::warn!(
        engine_handle = engine_actor_handle,
        "Session mailbox closed before EngineReady. Stopping engine."
      );
      final_error_for_actor_stop = Some(ZmqError::Internal("Session closed before EngineReady".into()));
      let _ = framed_for_handshake_and_ready.close().await;
      engine_context_clone.publish_actor_stopping(
        engine_actor_handle,
        engine_actor_type,
        None,
        final_error_for_actor_stop.clone(),
      );
      return;
    }
    self.last_activity_time = Instant::now();

    // 5. Get the raw underlying stream `S` back for the main data loop.
    let mut current_underlying_stream: S = framed_for_handshake_and_ready.into_inner();

    tracing::debug!(
      engine_handle = engine_actor_handle,
      "ZmtpEngineCoreStd entering main message loop."
    );

    let keepalive_ping_enabled = self.heartbeat_ivl.map_or(false, |d| !d.is_zero());
    let mut ping_check_timer = if keepalive_ping_enabled {
      let configured_ivl = self.heartbeat_ivl.unwrap();
      let check_frequency = configured_ivl
        .checked_sub(Duration::from_millis(50))
        .unwrap_or(configured_ivl);
      let mut timer = interval(check_frequency.max(Duration::from_millis(100)));
      timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
      Some(timer)
    } else {
      None
    };
    let mut pong_timeout_timer = interval(self.heartbeat_timeout);
    pong_timeout_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut should_break_loop = false;

    while !should_break_loop {
      tokio::select! {
        biased;

        // --- ARM 1: Receive Command from Session ---
        session_command_result = self.mailbox_receiver.recv() => {
          let command_from_session = match session_command_result {
            Ok(cmd) => cmd,
            Err(_) => {
              tracing::info!(engine_handle = engine_actor_handle, "Engine command mailbox (from Session) closed. Stopping.");
              final_error_for_actor_stop = Some(ZmqError::Internal("Session mailbox closed".into()));
              should_break_loop = true;
              continue;
            }
          };

          match command_from_session {
            Command::SessionPushCmd { msg } => {
              let original_msg_is_last_part = !msg.is_more(); // For TCP_CORK logic

              // 1. ZMTP-frame the Msg
              let mut temp_zmtp_encoder = ZmtpCodec::new();
              let mut plaintext_zmtp_frame_buffer = BytesMut::new();
              if let Err(e) = temp_zmtp_encoder.encode(msg, &mut plaintext_zmtp_frame_buffer) {
                tracing::error!(engine_handle = self.handle, error = %e, "Failed to ZMTP-encode outgoing message.");
                final_error_for_actor_stop = Some(e); should_break_loop = true; continue;
              }
              let plaintext_zmtp_frame_bytes = plaintext_zmtp_frame_buffer.freeze();

              // 2. Encrypt if cipher is active
              let wire_bytes_to_send = if let Some(cipher) = &mut self.data_cipher {
                match cipher.encrypt_zmtp_frame(plaintext_zmtp_frame_bytes) {
                  Ok(eb) => eb,
                  Err(e) => {
                    tracing::error!(engine_handle = self.handle, error = %e, "Failed to encrypt outgoing ZMTP frame.");
                    final_error_for_actor_stop = Some(e); should_break_loop = true; continue;
                  }
                }
              } else {
                plaintext_zmtp_frame_bytes
              };

              // 3. TCP_CORK logic
              #[cfg(target_os = "linux")]
              if self.config.use_cork {
                if let Some(fd) = self.stream_fd_for_cork {
                  if self.expecting_first_frame_of_msg && !self.is_corked {
                    Self::set_tcp_cork_rawfd(fd, true, self.handle).await;
                    self.is_corked = true;
                  }
                } else if self.expecting_first_frame_of_msg {
                   tracing::warn!(engine_handle = self.handle, "TCP_CORK is enabled but stream FD is not available for setting cork.");
                }
              }

              // 4. Send the bytes
              if let Err(e) = current_underlying_stream.write_all(&wire_bytes_to_send).await {
                final_error_for_actor_stop = Some(ZmqError::from(e));
                should_break_loop = true;
                #[cfg(target_os = "linux")]
                if self.is_corked {
                  if let Some(fd) = self.stream_fd_for_cork { Self::set_tcp_cork_rawfd(fd, false, self.handle).await; }
                  self.is_corked = false;
                  self.expecting_first_frame_of_msg = true;
                }
                continue;
              }

              self.last_activity_time = Instant::now();
              self.expecting_first_frame_of_msg = original_msg_is_last_part;

              #[cfg(target_os = "linux")]
              if self.is_corked && self.expecting_first_frame_of_msg {
                if let Some(fd) = self.stream_fd_for_cork {
                  Self::set_tcp_cork_rawfd(fd, false, self.handle).await;
                }
                self.is_corked = false;
              }
            }
            Command::Stop => {
              tracing::info!(engine_handle = engine_actor_handle, "Engine received Stop command.");
              should_break_loop = true;
              continue;
            }
            _ => { /* Unhandled command */ }
          }
        }

        // --- ARM 2: Read from Network Stream ---
        // `read_buf` reads into `network_read_buffer`.
        // Data is then decrypted (if cipher active) into `zmtp_plaintext_feed_buffer`.
        // Then `zmtp_manual_parser` decodes ZMTP messages from `zmtp_plaintext_feed_buffer`.
        read_result = current_underlying_stream.read_buf(&mut network_read_buffer) => {
          match read_result {
            Ok(0) => { // EOF
              tracing::info!(engine_handle = engine_actor_handle, "Connection closed by peer (EOF).");
              final_error_for_actor_stop = Some(ZmqError::ConnectionClosed);
              should_break_loop = true;
              continue;
            }
            Ok(_) => { // Some bytes read into network_read_buffer
              self.last_activity_time = Instant::now();
              self.expecting_first_frame_of_msg = true; // Activity from peer resets send cork expectation
              #[cfg(target_os = "linux")]
              if self.is_corked {
                if let Some(fd) = self.stream_fd_for_cork { Self::set_tcp_cork_rawfd(fd, false, self.handle).await; }
                self.is_corked = false;
              }

              if let Some(cipher) = &mut self.data_cipher {
                // Decrypt loop: process all complete secure messages in network_read_buffer
                'decrypt_loop: loop {
                  match cipher.decrypt_wire_data_to_zmtp_frame(&mut network_read_buffer) {
                    Ok(Some(plaintext_zmtp_bytes)) => {
                      zmtp_plaintext_feed_buffer.put(plaintext_zmtp_bytes);
                    }
                    Ok(None) => break 'decrypt_loop, // Need more data in network_read_buffer
                    Err(e) => {
                      tracing::error!(engine_handle = self.handle, error = %e, "Decryption failed.");
                      final_error_for_actor_stop = Some(e);
                      should_break_loop = true;
                      break 'decrypt_loop;
                    }
                  }
                }
                if should_break_loop { continue; }
              } else {
                // No cipher, move data directly from network_read_buffer to zmtp_plaintext_feed_buffer
                zmtp_plaintext_feed_buffer.extend_from_slice(&network_read_buffer);
                network_read_buffer.clear(); // All data moved
              }

              // ZMTP Parse loop: process all complete ZMTP frames in zmtp_plaintext_feed_buffer
              'zmtp_parse_loop: loop {
                match zmtp_manual_parser.decode_from_buffer(&mut zmtp_plaintext_feed_buffer) {
                  Ok(Some(decoded_msg)) => {
                    if decoded_msg.is_command() {
                      if let Some(zmtp_cmd) = ZmtpCommand::parse(&decoded_msg) {
                        match zmtp_cmd {
                          ZmtpCommand::Ping(ping_context) => {
                            let pong_reply_msg = ZmtpCommand::create_pong(&ping_context);
                            let mut temp_zmtp_enc = ZmtpCodec::new();
                            let mut ptxt_buf = BytesMut::new();
                            if let Err(e) = temp_zmtp_enc.encode(pong_reply_msg, &mut ptxt_buf) {
                              final_error_for_actor_stop = Some(e); should_break_loop = true; break 'zmtp_parse_loop;
                            }
                            let ptxt_bytes = ptxt_buf.freeze();
                            let wire_bytes = if let Some(c) = &mut self.data_cipher {
                              match c.encrypt_zmtp_frame(ptxt_bytes) {
                                Ok(eb) => eb,
                                Err(e) => { final_error_for_actor_stop = Some(e); should_break_loop = true; break 'zmtp_parse_loop; }
                              }
                            } else { ptxt_bytes };
                            if let Err(e) = current_underlying_stream.write_all(&wire_bytes).await {
                              final_error_for_actor_stop = Some(ZmqError::from(e)); should_break_loop = true; break 'zmtp_parse_loop;
                            }
                            self.last_activity_time = Instant::now();
                          }
                          ZmtpCommand::Pong(_) => {
                            self.waiting_for_pong = false;
                            self.last_ping_sent_time = None;
                          }
                          ZmtpCommand::Error => {
                            final_error_for_actor_stop = Some(ZmqError::ProtocolViolation("Received ZMTP ERROR from peer".into()));
                            should_break_loop = true; break 'zmtp_parse_loop;
                          }
                          _ => { /* Ignore other commands like Ready (already handled), Unknown */ }
                        }
                      }
                    } else { // Data message
                      if self.session_mailbox.send(Command::EnginePushCmd { msg: decoded_msg }).await.is_err() {
                        final_error_for_actor_stop = Some(ZmqError::Internal("Session mailbox closed on incoming data".into()));
                        should_break_loop = true; break 'zmtp_parse_loop;
                      }
                    }
                  }
                  Ok(None) => break 'zmtp_parse_loop, // Need more data in zmtp_plaintext_feed_buffer
                  Err(e) => {
                    final_error_for_actor_stop = Some(e);
                    should_break_loop = true; break 'zmtp_parse_loop;
                  }
                }
              } // End ZMTP parse loop
              if should_break_loop { continue; }
            }
            Err(e) => { // IO error reading from stream
              final_error_for_actor_stop = Some(ZmqError::from(e));
              should_break_loop = true;
              continue;
            }
          }
        }

        // --- ARM 3: Heartbeat PING Timer ---
        _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong && ping_check_timer.is_some() => {
          let now = Instant::now();
          if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
            tracing::trace!(engine_handle = self.handle, "Heartbeat interval elapsed. Sending PING.");
            let ping_to_send_msg = ZmtpCommand::create_ping(0, b""); // TTL and Context for PING

            let mut temp_zmtp_encoder = ZmtpCodec::new();
            let mut plaintext_zmtp_frame_buffer = BytesMut::new();
            if let Err(e) = temp_zmtp_encoder.encode(ping_to_send_msg, &mut plaintext_zmtp_frame_buffer) {
              final_error_for_actor_stop = Some(e); should_break_loop = true; continue;
            }
            let plaintext_zmtp_frame_bytes = plaintext_zmtp_frame_buffer.freeze();

            let wire_bytes_to_send = if let Some(cipher) = &mut self.data_cipher {
              match cipher.encrypt_zmtp_frame(plaintext_zmtp_frame_bytes) {
                Ok(eb) => eb,
                Err(e) => { final_error_for_actor_stop = Some(e); should_break_loop = true; continue; }
              }
            } else {
              plaintext_zmtp_frame_bytes
            };

            #[cfg(target_os = "linux")]
            if self.config.use_cork {
              if let Some(fd) = self.stream_fd_for_cork {
                if !self.is_corked {
                  Self::set_tcp_cork_rawfd(fd, true, self.handle).await;
                  self.is_corked = true;
                }
              }
            }

            match current_underlying_stream.write_all(&wire_bytes_to_send).await {
              Ok(()) => {
                self.last_activity_time = now;
                self.last_ping_sent_time = Some(now);
                self.waiting_for_pong = true;
                pong_timeout_timer.reset();
                tracing::debug!(engine_handle = self.handle, "PING sent. Waiting for PONG.");
              }
              Err(e) => {
                final_error_for_actor_stop = Some(ZmqError::from(e));
                should_break_loop = true;
                #[cfg(target_os = "linux")] if self.is_corked { if let Some(fd) = self.stream_fd_for_cork { Self::set_tcp_cork_rawfd(fd, false, self.handle).await; } self.is_corked = false; }
                continue;
              }
            }

            #[cfg(target_os = "linux")]
            if self.is_corked { // PING is a self-contained logical message
              if let Some(fd) = self.stream_fd_for_cork {
                Self::set_tcp_cork_rawfd(fd, false, self.handle).await;
              }
              self.is_corked = false;
            }
            self.expecting_first_frame_of_msg = true;
          }
        }

        // --- ARM 4: PONG Timeout ---
        _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
          tracing::warn!(engine_handle = self.handle, timeout = ?self.heartbeat_timeout, "Timed out waiting for PONG.");
          final_error_for_actor_stop = Some(ZmqError::Timeout);
          should_break_loop = true;
          #[cfg(target_os = "linux")]
          if self.is_corked {
            if let Some(fd) = self.stream_fd_for_cork { Self::set_tcp_cork_rawfd(fd, false, self.handle).await; }
            self.is_corked = false;
          }
          self.expecting_first_frame_of_msg = true;
          continue;
        }
      } // end select!
    } // end while !should_break_loop

    // --- Cleanup ---
    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCoreStd loop finished. Cleaning up."
    );

    #[cfg(target_os = "linux")]
    if self.is_corked {
      if let Some(fd) = self.stream_fd_for_cork {
        if fd > 0 {
          // Basic check
          Self::set_tcp_cork_rawfd(fd, false, self.handle).await;
        }
      }
      self.is_corked = false;
    }

    // Attempt to shutdown the stream gracefully.
    // `current_underlying_stream` is the raw `S`.
    if let Err(e) = current_underlying_stream.shutdown().await {
      tracing::warn!(engine_handle = engine_actor_handle, error = %e, "Error during stream shutdown.");
    }
    drop(current_underlying_stream); // Ensure it's dropped.

    // Notify Session that engine has stopped.
    if !self.session_mailbox.is_closed() {
      if let Err(e) = self.session_mailbox.send(Command::EngineStopped).await {
        tracing::warn!(engine_handle = engine_actor_handle, error = ?e, "Failed to send EngineStopped to Session.");
      }
    }

    // Publish ActorStopping event.
    engine_context_clone.publish_actor_stopping(
      engine_actor_handle,
      engine_actor_type,
      None, // TODO: Could add peer URI from config if it's a client engine.
      final_error_for_actor_stop,
    );
    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCoreStd actor task fully stopped."
    );
  }
}
