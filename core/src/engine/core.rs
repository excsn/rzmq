// src/engine/core.rs

use crate::engine::ZmtpStream;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpReady;
use crate::protocol::zmtp::{
  codec::ZmtpCodec,
  command::ZmtpCommand,
  greeting::{ZmtpGreeting, GREETING_LENGTH},
};
use crate::runtime::{ActorType, Command, MailboxReceiver, MailboxSender};
#[cfg(feature = "curve")]
use crate::security::CurveMechanism;
use crate::security::{Mechanism, NullMechanism, PlainMechanism};
use crate::socket::options::ZmtpEngineConfig;
use crate::{Blob, Context, MsgFlags};

use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant};
use std::{fmt, marker::PhantomData};

use bytes::BytesMut;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
#[cfg(target_os = "linux")]
use socket2::SockRef; // For setting TCP_CORK
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::interval;
use tokio_util::codec::Framed;

/// Core ZMTP engine logic, generic over the underlying stream type `S`.
/// This struct encapsulates the state and logic for handling the ZMTP protocol,
/// including handshakes (greeting, security, ready commands) and message exchange.
/// It is designed to run as an independent actor task.
pub(crate) struct ZmtpEngineCore<S: ZmtpStream> {
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

impl<S: ZmtpStream> fmt::Debug for ZmtpEngineCore<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut debug_struct = f.debug_struct("ZmtpEngineCore");
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

    debug_struct.field(
      "expecting_first_frame_of_msg",
      &self.expecting_first_frame_of_msg,
    );
    debug_struct.finish_non_exhaustive()
  }
}

impl<S: ZmtpStream + AsRawFd> ZmtpEngineCore<S> {
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
    // In a real scenario, the mechanism chosen by socket options might influence the greeting.
    // For ZMTP 3.1, the greeting itself proposes a mechanism (often NULL or PLAIN/CURVE directly).
    // Libzmq sends the configured socket mechanism in the greeting if it's PLAIN or CURVE.
    // For now, sending NULL and relying on negotiation.
    let own_greeting_mechanism_bytes = {
      let mut mechanism_name_to_send = NullMechanism::NAME_BYTES; // Default to NULL

      #[cfg(feature = "curve")]
      if self.config.socket_type_name == "CURVECLIENTTODO" {
        // Example placeholder
        mechanism_name_to_send = CurveMechanism::NAME_BYTES;
      } else if self.config.socket_type_name == "PLAINCLIENTTODO" {
        // Example placeholder
        mechanism_name_to_send = PlainMechanism::NAME_BYTES;
      }
      // If no specific mechanism is forced by config for the initial greeting, NullMechanism::NAME_BYTES is used.
      mechanism_name_to_send
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
      "Sent own ZMTP greeting (raw)."
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
    framed_stream: &mut Framed<S, ZmtpCodec>,
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
    if self.is_server == peer_greeting.as_server {
      let role_error_msg = format!(
        "Role mismatch: we_are_server={}, peer_is_server={}.",
        self.is_server, peer_greeting.as_server
      );
      tracing::error!(engine_handle = self.handle, error = %role_error_msg);
      return Err(ZmqError::ProtocolViolation(role_error_msg));
    }

    let peer_proposed_mechanism_name = peer_greeting.mechanism_name();
    tracing::info!(
      engine_handle = self.handle,
      peer_mechanism_proposal = peer_proposed_mechanism_name,
      "Selecting security mechanism based on peer greeting."
    );

    // TODO: Actual mechanism selection should consider socket options (e.g., if this socket is PLAIN server,
    // it might reject a NULL proposal or only accept PLAIN. If it's CURVE, it only accepts CURVE).
    // For now, this is a simple negotiation based on what peer proposed.
    let mut negotiated_mechanism: Box<dyn Mechanism> = match peer_proposed_mechanism_name {
      NullMechanism::NAME => Box::new(NullMechanism),
      PlainMechanism::NAME => {
        // If we are server, and peer proposes PLAIN, we expect username/password.
        // If we are client, and peer proposes PLAIN, this is unusual (server usually dictates PLAIN).
        // For now, just instantiate based on our role.
        // TODO: Pass PLAIN options (username/password if client) from self.config or socket options.
        Box::new(PlainMechanism::new(self.is_server))
      }
      #[cfg(feature = "curve")]
      CurveMechanism::NAME => {
        // TODO: Pass CURVE keys (server key for client, client key list for server) from self.config or socket options.
        Box::new(CurveMechanism::new(self.is_server))
      }
      unsupported_name => {
        let security_error_msg = format!("Unsupported security mechanism '{}' from peer.", unsupported_name);
        tracing::error!(engine_handle = self.handle, error = %security_error_msg);
        // TODO: Send ZMTP ERROR command back to peer with reason?
        return Err(ZmqError::SecurityError(security_error_msg));
      }
    };
    // The `self.mechanism` field in ZmtpEngineCore will be updated by run_loop after this function returns.

    // --- Security Handshake (Token Exchange using Framed stream) ---
    tracing::debug!(
      engine_handle = self.handle,
      mechanism = negotiated_mechanism.name(),
      "Starting security token exchange (using framed stream)."
    );

    while !negotiated_mechanism.is_complete() && !negotiated_mechanism.is_error() {
      // Produce a token if the mechanism has one to send.
      if let Some(token_to_send_vec) = negotiated_mechanism.produce_token()? {
        let mut command_msg = Msg::from_vec(token_to_send_vec);
        command_msg.set_flags(MsgFlags::COMMAND); // Security tokens are ZMTP commands.
        tracing::trace!(
          engine_handle = self.handle,
          mechanism = negotiated_mechanism.name(),
          token_size = command_msg.size(),
          "Sending security token."
        );
        if let Err(e) = framed_stream.send(command_msg).await {
          negotiated_mechanism.set_error(format!("Stream error sending token: {}", e));
          tracing::error!(engine_handle = self.handle, error = %e, "Failed to send security token.");
          return Err(e);
        }
        self.last_activity_time = Instant::now();
      }

      // If mechanism is still not complete or in error, expect a token from peer.
      if !negotiated_mechanism.is_complete() && !negotiated_mechanism.is_error() {
        match framed_stream.next().await {
          Some(Ok(received_msg)) => {
            self.last_activity_time = Instant::now();
            if !received_msg.is_command() {
              let protocol_err_msg = "Expected COMMAND frame during security handshake, got data frame.";
              negotiated_mechanism.set_error(protocol_err_msg.to_string());
              tracing::error!(engine_handle = self.handle, error = %protocol_err_msg);
              return Err(ZmqError::SecurityError(protocol_err_msg.to_string()));
            }
            let token_data = received_msg.data().unwrap_or(&[]); // COMMAND body is the token
            tracing::trace!(
              engine_handle = self.handle,
              mechanism = negotiated_mechanism.name(),
              token_size = token_data.len(),
              "Received security token."
            );
            if let Err(e) = negotiated_mechanism.process_token(token_data) {
              // process_token itself might set_error internally, or return an error
              tracing::error!(engine_handle = self.handle, error = %e, "Failed to process security token.");
              return Err(e);
            }
          }
          Some(Err(e)) => {
            // Error decoding from framed stream
            negotiated_mechanism.set_error(format!("Stream error receiving token: {}", e));
            tracing::error!(engine_handle = self.handle, error = %e, "Stream error during security handshake.");
            return Err(e);
          }
          None => {
            // Stream closed by peer
            let err_msg = "Connection closed by peer during security handshake".to_string();
            negotiated_mechanism.set_error(err_msg.clone());
            tracing::error!(engine_handle = self.handle, error = %err_msg);
            return Err(ZmqError::ConnectionClosed);
          }
        }
      }

      // Placeholder for ZAP (ZeroMQ Authentication Protocol) interaction
      // If mechanism requires ZAP, it will indicate so. Session would then interact with ZAP server.
      if let Some(_zap_req_frames) = negotiated_mechanism.zap_request_needed() {
        // In a full implementation, this would signal back to Session to perform ZAP.
        // For now, we'll assume ZAP is not implemented or not required by the chosen mechanism.
        let zap_err_msg = "ZAP authentication required but not implemented in this engine core".to_string();
        negotiated_mechanism.set_error(zap_err_msg.clone());
        tracing::warn!(engine_handle = self.handle, %zap_err_msg);
        // This typically means the handshake fails if ZAP was mandatory.
      }
    } // End while loop for token exchange

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
      // TODO: Send ZMTP ERROR command back to peer?
      return Err(ZmqError::SecurityError(reason.to_string()));
    }
    tracing::info!(
      engine_handle = self.handle,
      mechanism = negotiated_mechanism.name(),
      "Security handshake successful."
    );

    // --- ZMTP READY Command Exchange ---
    let mut identity_from_ready_command: Option<Blob> = None;
    let local_socket_type_name = self.config.socket_type_name.clone();
    let local_routing_id = self.config.routing_id.clone(); // From ZmtpEngineConfig

    // Client sends READY first
    if !self.is_server {
      let mut client_ready_props = HashMap::new();
      client_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());
      if let Some(id_blob) = local_routing_id {
        if !id_blob.is_empty() && id_blob.len() <= 255 {
          client_ready_props.insert("Identity".to_string(), id_blob.to_vec());
        } else {
          tracing::warn!(
            engine_handle = self.handle,
            id_len = id_blob.len(),
            "Client local routing ID invalid (empty or too long), not sending in READY."
          );
        }
      }
      let client_ready_msg = ZmtpReady::create_msg(client_ready_props);
      tracing::debug!(engine_handle = self.handle, "Client sending ZMTP READY command.");
      if let Err(e) = framed_stream.send(client_ready_msg).await {
        tracing::error!(engine_handle=self.handle, error=%e, "Failed to send client READY command.");
        return Err(e);
      }
      self.last_activity_time = Instant::now();
    }

    // Both client and server expect a READY command from the peer.
    tracing::debug!(engine_handle = self.handle, "Waiting for peer's ZMTP READY command.");
    let peer_ready_command_parsed = loop {
      match framed_stream.next().await {
        Some(Ok(received_msg)) => {
          self.last_activity_time = Instant::now();
          if !received_msg.is_command() {
            let err_msg = "Expected ZMTP READY command, but received a data frame.";
            tracing::error!(engine_handle = self.handle, %err_msg);
            return Err(ZmqError::ProtocolViolation(err_msg.to_string()));
          }
          // Try to parse as ZmtpCommand, then check if it's Ready
          match ZmtpCommand::parse(&received_msg) {
            Some(ZmtpCommand::Ready(ready_cmd_data)) => {
              break ready_cmd_data; // Successfully got and parsed READY
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
              // Could not parse as any known ZMTP command
              let err_msg = "Received unparseable ZMTP command when expecting READY.";
              tracing::error!(engine_handle = self.handle, %err_msg);
              return Err(ZmqError::ProtocolViolation(err_msg.to_string()));
            }
          }
        }
        Some(Err(e)) => {
          // Error decoding from framed stream
          tracing::error!(engine_handle = self.handle, error=%e, "Stream error while waiting for peer's READY command.");
          return Err(e);
        }
        None => {
          // Stream closed by peer
          let err_msg = "Connection closed by peer while waiting for READY command.";
          tracing::error!(engine_handle = self.handle, %err_msg);
          return Err(ZmqError::ConnectionClosed);
        }
      }
    };
    tracing::debug!(engine_handle = self.handle, peer_ready_cmd = ?peer_ready_command_parsed, "Received peer's ZMTP READY command.");

    // Extract Identity from peer's READY command if present
    if let Some(id_bytes_vec) = peer_ready_command_parsed.properties.get("Identity") {
      if !id_bytes_vec.is_empty() && id_bytes_vec.len() <= 255 {
        identity_from_ready_command = Some(Blob::from(id_bytes_vec.clone()));
        tracing::debug!(engine_handle = self.handle, peer_identity_from_ready = ?identity_from_ready_command, "Extracted Identity from peer's READY command.");
      } else {
        tracing::warn!(
          engine_handle = self.handle,
          id_len = id_bytes_vec.len(),
          "Peer sent invalid Identity in READY (empty or too long)."
        );
      }
    }

    // Server sends READY second (after receiving client's READY)
    if self.is_server {
      let mut server_ready_props = HashMap::new();
      server_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());
      // Server does not send Identity in its READY typically, unless ZMQ_ROUTING_ID is set on it and it's a client-facing socket?
      // Libzmq server sockets (REP, ROUTER, PUB, PULL) do not send an Identity in their READY.
      let server_ready_msg = ZmtpReady::create_msg(server_ready_props);
      tracing::debug!(engine_handle = self.handle, "Server sending ZMTP READY command.");
      if let Err(e) = framed_stream.send(server_ready_msg).await {
        tracing::error!(engine_handle=self.handle, error=%e, "Failed to send server READY command.");
        return Err(e);
      }
      self.last_activity_time = Instant::now();
    }

    // Determine final peer identity: mechanism's identity takes precedence over READY's identity.
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
    tracing::trace!(engine_handle, fd, cork_enable = enable, "Attempting to set TCP_CORK");
    // Using a blocking task for setsockopt as socket2 is blocking.
    // This is okay for infrequent operations like corking.
    let res = tokio::task::spawn_blocking(move || {
      let socket = unsafe { SockRef::from_fd(fd) };
      let result = socket.set_tcp_cork(enable);
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
      "ZmtpEngineCore actor main loop starting."
    );

    let mut raw_stream = match self.stream.take() {
      Some(s) => s,
      None => {
        tracing::error!(
          engine_handle = engine_actor_handle,
          "ZmtpEngineCore started without a valid stream!"
        );
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
      // Get the FD from the raw_stream before it's moved into Framed.
      // This assumes S implements AsRawFd.
      self.stream_fd_for_cork = Some(raw_stream.as_raw_fd());
      tracing::debug!(engine_handle = self.handle, fd = ?self.stream_fd_for_cork, "TCP_CORK enabled, cached FD.");
    }

    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    let peer_greeting = match self.exchange_greetings(&mut raw_stream).await {
      Ok(pg) => pg,
      Err(greeting_err) => {
        tracing::error!(engine_handle = engine_actor_handle, error = %greeting_err, "ZMTP Greeting exchange failed.");
        let _ = self
          .session_mailbox
          .send(Command::EngineError {
            error: greeting_err.clone(),
          })
          .await;
        final_error_for_actor_stop = Some(greeting_err);
        engine_context_clone.publish_actor_stopping(
          engine_actor_handle,
          engine_actor_type,
          None,
          final_error_for_actor_stop,
        );
        return;
      }
    };

    let mut framed_transport_stream = Framed::new(raw_stream, ZmtpCodec::new());

    // This assumes the FD obtained from the stream wrapped by Framed is the same
    // as the initial raw stream. This should be true.
    #[cfg(target_os = "linux")]
    if self.config.use_cork && self.stream_fd_for_cork.is_none() {
      if let Some(fs) = self.framed_stream.as_ref() {
        self.stream_fd_for_cork = Some(fs.get_ref().as_raw_fd());
        tracing::debug!(engine_handle = self.handle, fd = ?self.stream_fd_for_cork, "TCP_CORK enabled, cached FD from framed_stream in run_loop.");
      } else {
        tracing::error!(
          engine_handle = self.handle,
          "Cannot get FD for corking: framed_stream is None post-handshake attempt."
        );
        // This would be a critical internal error.
      }
    }

    match self
      .perform_security_and_ready_handshake(&mut framed_transport_stream, peer_greeting)
      .await
    {
      Ok((negotiated_mech, peer_id_opt)) => {
        self.mechanism = negotiated_mech;
        if self
          .session_mailbox
          .send(Command::EngineReady {
            peer_identity: peer_id_opt,
          })
          .await
          .is_err()
        {
          tracing::warn!(
            engine_handle = engine_actor_handle,
            "Session mailbox closed after successful handshake. Stopping engine."
          );
          let _ = framed_transport_stream.close().await;
          final_error_for_actor_stop = Some(ZmqError::Internal("Session closed post-handshake".into()));
          engine_context_clone.publish_actor_stopping(
            engine_actor_handle,
            engine_actor_type,
            None,
            final_error_for_actor_stop.clone(),
          );
          return;
        }
      }
      Err(handshake_err) => {
        tracing::error!(engine_handle = engine_actor_handle, error = %handshake_err, "ZMTP Security/Ready Handshake failed.");
        let _ = self
          .session_mailbox
          .send(Command::EngineError {
            error: handshake_err.clone(),
          })
          .await;
        let _ = framed_transport_stream.close().await;
        final_error_for_actor_stop = Some(handshake_err.clone());
        engine_context_clone.publish_actor_stopping(engine_actor_handle, engine_actor_type, None, Some(handshake_err));
        return;
      }
    }
    self.last_activity_time = Instant::now();

    tracing::debug!(
      engine_handle = engine_actor_handle,
      "ZmtpEngineCore entering main message loop."
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

        session_command_result = self.mailbox_receiver.recv() => {
          let command_from_session = match session_command_result {
            Ok(cmd) => cmd,
            Err(_) => {
              tracing::info!(engine_handle = engine_actor_handle, "Engine command mailbox (from Session) closed. Stopping.");
              should_break_loop = true;
              if final_error_for_actor_stop.is_none() { final_error_for_actor_stop = Some(ZmqError::Internal("Session mailbox closed".into())); }
              continue;
            }
          };
          match command_from_session {
            Command::SessionPushCmd { msg } => {
              let is_last_frame_of_user_message = !msg.is_more();
              let mut current_send_error: Option<ZmqError> = None;
              let mut send_occurred_successfully = false;

              #[cfg(target_os = "linux")]
              if self.config.use_cork {
                  if let Some(fd) = self.stream_fd_for_cork {
                      if self.expecting_first_frame_of_msg && !self.is_corked {
                          // This is the first frame of a new logical ZMQ message, and we are not yet corked. Cork it.
                          Self::set_tcp_cork_rawfd(fd, true, self.handle).await;
                          self.is_corked = true;
                      }
                  } else if self.expecting_first_frame_of_msg { // Log if cork enabled but FD missing
                      tracing::warn!(engine_handle = self.handle, "TCP_CORK is enabled but stream FD is not available for setting cork.");
                  }
              }

              // --- Determine Send Path (Zerocopy or Standard) ---
              let use_zc_path = cfg!(feature = "io-uring") && self.config.use_send_zerocopy;

              if use_zc_path {
                #[cfg(feature = "io-uring")] // Ensures this block only active if io-uring feature is on
                {
                  let uring_stream_ref = framed_transport_stream.get_ref(); // S should be tokio_uring::net::TcpStream

                  let zmtp_header_bytes: Bytes = {
                    let mut temp_buf = bytes::BytesMut::with_capacity(9);
                    let codec = ZmtpCodec::new();
                    match codec.encode_header_only(&msg, &mut temp_buf) {
                        Ok(()) => temp_buf.freeze(),
                        Err(e) => {
                            tracing::error!(engine_handle = self.handle, "ZC Send: Failed to encode header: {}", e);
                            current_send_error = Some(e);
                            Bytes::new() // Dummy to satisfy type, error will be handled
                        }
                    }
                  };

                  if current_send_error.is_none() {
                    let payload_bytes: Bytes = msg.data_bytes().unwrap_or_else(Bytes::new);
                    let bufs = [zmtp_header_bytes.slice(..), payload_bytes.slice(..)];
                    let total_bytes_to_send = bufs[0].bytes_init() + bufs[1].bytes_init();

                    tracing::trace!(engine_handle = self.handle, header_len = bufs[0].bytes_init(), payload_len = bufs[1].bytes_init(), "Attempting ZC vectored send.");
                    match uring_stream_ref.send_vectored_zc(&bufs).await {
                      Ok(bytes_sent) => {
                        if bytes_sent != total_bytes_to_send {
                          tracing::error!(engine_handle = self.handle, expected = total_bytes_to_send, actual = bytes_sent, "ZC send_vectored_zc resulted in a partial send.");
                          current_send_error = Some(ZmqError::from(io::Error::new(io::ErrorKind::WriteZero, "Partial zerocopy send")));
                        } else {
                          send_occurred_successfully = true;
                          tracing::trace!(engine_handle = self.handle, bytes_sent, "ZC vectored send successful.");
                        }
                      }
                      Err(e) => {
                        tracing::error!(engine_handle = self.handle, error = %e, "Engine ZC send_vectored_zc failed.");
                        current_send_error = Some(ZmqError::from(e));
                      }
                    }
                  }
                }
              } else { // Standard send path
                if let Err(e) = framed_transport_stream.send(msg).await {
                  current_send_error = Some(e);
                } else {
                  send_occurred_successfully = true;
                }
              }

              // Current plan: All sends go through framed_stream. Zerocopy path is deferred.
              if let Some(e) = current_send_error {
                tracing::error!(engine_handle = self.handle, error = %e, "Engine failed to send message.");

                if !self.session_mailbox.is_closed() {
                  let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await;
                }

                final_error_for_actor_stop = Some(e);
                should_break_loop = true;

                // If send fails, ensure cork is released if it was set for this message attempt
                #[cfg(target_os = "linux")]
                if self.is_corked {
                  if let Some(fd) = self.stream_fd_for_cork {
                    Self::set_tcp_cork_rawfd(fd, false, self.handle).await; // Uncork on error
                  }
                  self.is_corked = false;
                  self.expecting_first_frame_of_msg = true; // Reset for next attempt if any
                }
              } else if send_occurred_successfully {
                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = is_last_frame_of_user_message;

                #[cfg(target_os = "linux")]
                if self.is_corked && self.expecting_first_frame_of_msg && send_occurred_successfully {
                  // This means the frame just sent was the last part of a logical ZMQ message,
                  // and the cork was active. Uncork now.
                  if let Some(fd) = self.stream_fd_for_cork {
                      Self::set_tcp_cork_rawfd(fd, false, self.handle).await; // Uncork
                  }
                  self.is_corked = false;
                }
              }
            }
            Command::Stop => {
              tracing::info!(engine_handle = engine_actor_handle, "Engine received Stop command.");
              should_break_loop = true;
            }
            _ => { /* Unhandled command */ }
          }
        }

        frame_decode_result = framed_transport_stream.next() => {
          match frame_decode_result {
            Some(Ok(decoded_msg)) => {
              self.last_activity_time = Instant::now();

              // Any valid frame received from peer means peer is active.
              // If we were corked waiting to send more, this implies the other side might expect a turn.
              // Also, TCP ACKs might have flushed our send buffer.
              // It's safer to reset cork state.
              if self.expecting_first_frame_of_msg == false { // We were in the middle of sending a logical message
                tracing::trace!(engine_handle = self.handle, "Data received from peer mid-send; ZMQ message send interrupted by receive.");
              }
              self.expecting_first_frame_of_msg = true;
              #[cfg(target_os = "linux")]
              if self.is_corked {
                if let Some(fd) = self.stream_fd_for_cork {
                  tracing::trace!(engine_handle = self.handle, "Data received from peer, ensuring TCP_CORK is off if it was on.");
                  Self::set_tcp_cork_rawfd(fd, false, self.handle).await; // Uncork on receiving data
                }
                self.is_corked = false;
              }

              if decoded_msg.is_command() {
                if let Some(zmtp_cmd) = ZmtpCommand::parse(&decoded_msg) {
                  match zmtp_cmd {
                    ZmtpCommand::Ping(ping_context) => {
                      let pong_reply = ZmtpCommand::create_pong(&ping_context);
                      if let Err(e) = framed_transport_stream.send(pong_reply).await {
                        if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await; }
                        should_break_loop = true; final_error_for_actor_stop = Some(e);
                      } else { self.last_activity_time = Instant::now(); }
                    }
                    ZmtpCommand::Pong(_) => {
                      if self.waiting_for_pong { self.waiting_for_pong = false; self.last_ping_sent_time = None; }
                      else { tracing::warn!(engine_handle = engine_actor_handle, "Received unexpected PONG."); }
                    }
                    ZmtpCommand::Error => {
                      if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::ProtocolViolation("Received ZMTP ERROR".into()) }).await; }
                      should_break_loop = true; final_error_for_actor_stop = Some(ZmqError::ProtocolViolation("Received ZMTP ERROR from peer".into()));
                    }
                    _ => { /* Ignore other commands like Ready, Unknown */ }
                  }
                }
              } else {
                if self.session_mailbox.send(Command::EnginePushCmd { msg: decoded_msg }).await.is_err() {
                  should_break_loop = true;
                  if final_error_for_actor_stop.is_none() { final_error_for_actor_stop = Some(ZmqError::Internal("Session mailbox closed on incoming data".into())); }
                }
              }
            }
            Some(Err(e)) => {
              if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await; }
              should_break_loop = true; final_error_for_actor_stop = Some(e);
            }
            None => {
              should_break_loop = true;
              if final_error_for_actor_stop.is_none() { final_error_for_actor_stop = Some(ZmqError::ConnectionClosed); }
            }
          }
        }

        // --- Heartbeat PING Timer Arm ---
        // This arm fires if keepalive is enabled, we are not already waiting for a PONG,
        // and the ping_check_timer ticks.
        _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong && ping_check_timer.is_some() => {
          // Check if it's time to send a PING based on last_activity_time and heartbeat_ivl
          let now = Instant::now();
          if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
            tracing::trace!(engine_handle = self.handle, "Heartbeat interval elapsed. Sending PING.");
            let ping_to_send = ZmtpCommand::create_ping(0, b""); // TTL and Context for PING
            let mut ping_sent_successfully = false;

            #[cfg(target_os = "linux")]
            if self.config.use_cork {
              if let Some(fd) = self.cached_fd_for_cork {
                // PING is a new logical message. If not already corked (e.g., from a previous send
                // that didn't complete with its last frame), cork now.
                if !self.is_corked {
                  Self::set_tcp_cork_on_fd(fd, true, self.handle).await;
                  self.is_corked = true;
                }
                // If it *was* already corked (e.g. user sent frame1, then PING timer fired),
                // we keep it corked, send the PING, then uncork because PING is a complete message.
              } else {
                tracing::warn!(engine_handle = self.handle, "TCP_CORK enabled for PING but stream FD is not available.");
              }
            }

            match framed_transport_stream.send(ping_to_send).await {
              Ok(()) => {
                self.last_activity_time = now;      // Update activity time after successful PING send
                self.last_ping_sent_time = Some(now);
                self.waiting_for_pong = true;
                pong_timeout_timer.reset(); // Reset the PONG timeout timer
                ping_sent_successfully = true;
                tracing::debug!(engine_handle = self.handle, "PING sent. Waiting for PONG.");
              }
              Err(e) => {
                tracing::error!(engine_handle = self.handle, error = %e, "Engine failed to send PING.");
                if !self.session_mailbox.is_closed() {
                  let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await;
                }
                final_error_for_actor_stop = Some(e);
                should_break_loop = true;
                // If PING send fails, ensure cork is released if it was set for this attempt
                #[cfg(target_os = "linux")]
                if self.is_corked {
                  if let Some(fd) = self.cached_fd_for_cork {
                    Self::set_tcp_cork_on_fd(fd, false, self.handle).await; // Uncork on error
                  }
                  self.is_corked = false;
                }
              }
            }

            // PING is a self-contained, single-frame logical ZMTP message.
            // So, if cork was active, it should be released after sending it.
            #[cfg(target_os = "linux")]
            if self.is_corked && ping_sent_successfully {
              if let Some(fd) = self.cached_fd_for_cork {
                Self::set_tcp_cork_on_fd(fd, false, self.handle).await; // Uncork after PING
              }
              self.is_corked = false;
            }
            // After sending a PING, the next user data send will be the start of a new logical message.
            self.expecting_first_frame_of_msg = true;
          }
        }

        // --- PONG Timeout Arm ---
        _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
          tracing::warn!(engine_handle = self.handle, timeout = ?self.heartbeat_timeout, "Timed out waiting for PONG.");
          if !self.session_mailbox.is_closed() {
            let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::Timeout }).await;
          }
          final_error_for_actor_stop = Some(ZmqError::Timeout);
          should_break_loop = true;

          // If we timed out waiting for PONG, and we were corked (perhaps waiting to send more data
          // that never happened because the PING failed to elicit a PONG), ensure we uncork.
          #[cfg(target_os = "linux")]
          if self.is_corked {
            if let Some(fd) = self.cached_fd_for_cork {
              Self::set_tcp_cork_on_fd(fd, false, self.handle).await;
            }
            self.is_corked = false;
          }
          self.expecting_first_frame_of_msg = true; // Reset for any future communication
        }
      }
    }

    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCore loop finished. Cleaning up."
    );

    #[cfg(target_os = "linux")]
    if self.is_corked {
      if let Some(fd) = self.stream_fd_for_cork {
        // Check if fd is likely valid, e.g. > 0, though this isn't foolproof.
        // The primary safety is that SockRef::from_fd is unsafe and relies on fd being valid.
        // If the stream was closed, fd might be -1 or reused.
        // This cleanup is best-effort.
        if fd > 0 {
          // Simple check, not a guarantee of validity
          tracing::debug!(
            engine_handle = self.handle,
            fd,
            "Ensuring TCP_CORK is unset during cleanup."
          );
          Self::set_tcp_cork_rawfd(fd, false, self.handle).await;
        }
      }
      self.is_corked = false; // Mark as uncorked anyway
    }

    let _ = framed_transport_stream.close().await;

    if !self.session_mailbox.is_closed() {
      if let Err(e) = self.session_mailbox.send(Command::EngineStopped).await {
        tracing::warn!(engine_handle = engine_actor_handle, error = ?e, "Failed to send EngineStopped to Session.");
      }
    }

    engine_context_clone.publish_actor_stopping(
      engine_actor_handle,
      engine_actor_type,
      None,
      final_error_for_actor_stop,
    );
    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCore actor task fully stopped."
    );
  }
}
