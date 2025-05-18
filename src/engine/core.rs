// src/engine/core.rs

use crate::engine::{IEngine, ZmtpStream}; // Use trait alias
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpReady;
use crate::protocol::zmtp::{
  codec::ZmtpCodec,
  command::{ZmtpCommand, ZMTP_CMD_PONG_NAME}, // Import command related types
  greeting::{ZmtpGreeting, GREETING_LENGTH},  // Import greeting related types
};
use crate::runtime::{ActorType, Command, MailboxReceiver, MailboxSender}; // ActorType for events, Context for events
#[cfg(feature = "curve")]
use crate::security::CurveMechanism; // For CURVE security mechanism if feature enabled
use crate::security::{Mechanism, MechanismStatus, NullMechanism, PlainMechanism}; // Core security traits and mechanisms
use crate::socket::options::ZmtpEngineConfig; // Configuration for ZMTP engine behavior
use crate::{Blob, Context, MsgFlags}; // Blob for identities, Context for publishing events

use bytes::BytesMut; // For efficient byte buffer manipulation
use futures::sink::SinkExt; // For Framed write operations (send)
use futures::stream::StreamExt; // For Framed read operations (next)
use std::collections::HashMap; // For ZmtpReady properties
use std::time::{Duration, Instant}; // For heartbeat timing
use std::{fmt, marker::PhantomData}; // PhantomData for unused generic type S
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // For raw stream read/write during handshake
                                              // JoinHandle not directly used in ZmtpEngineCore struct, but often for actor tasks
use tokio::time::interval; // For periodic heartbeat pings
use tokio_util::codec::Framed; // For ZMTP message framing/deframing over a stream

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
  /// The ZMTP framed stream, using `ZmtpCodec` for message encoding/decoding.
  framed_stream: Option<Framed<S, ZmtpCodec>>,
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
}

impl<S: ZmtpStream> fmt::Debug for ZmtpEngineCore<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ZmtpEngineCore")
      .field("handle", &self.handle)
      .field("session_mailbox_closed", &self.session_mailbox.is_closed())
      .field("mailbox_receiver_closed", &self.mailbox_receiver.is_closed())
      .field("framed_stream_is_some", &self.framed_stream.is_some())
      .field("config", &self.config)
      .field("mechanism_status", &self.mechanism.status())
      .field("is_server", &self.is_server)
      .field("last_activity_time", &self.last_activity_time)
      .field("last_ping_sent_time", &self.last_ping_sent_time)
      .field("waiting_for_pong", &self.waiting_for_pong)
      .field("heartbeat_ivl", &self.heartbeat_ivl)
      .field("heartbeat_timeout", &self.heartbeat_timeout)
      .field("context_present", &true)
      .finish_non_exhaustive()
  }
}

impl<S: ZmtpStream> ZmtpEngineCore<S> {
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

    Self {
      handle,
      session_mailbox,
      mailbox_receiver,
      stream: Some(stream),
      framed_stream: None,
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
        if self.config.socket_type_name == "CURVECLIENTTODO" { // Example placeholder
            mechanism_name_to_send = CurveMechanism::NAME_BYTES;
        } else if self.config.socket_type_name == "PLAINCLIENTTODO" { // Example placeholder
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
              if let Err(e) = framed_transport_stream.send(msg).await {
                tracing::error!(engine_handle = engine_actor_handle, error = %e, "Engine failed to send message.");
                if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await; }
                should_break_loop = true;
                final_error_for_actor_stop = Some(e);
              } else { self.last_activity_time = Instant::now(); }
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

        _ = async { ping_check_timer.as_mut().expect("PING timer must exist if enabled").tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong => {
          let now = Instant::now();
          if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
            let ping_to_send = ZmtpCommand::create_ping(0, b"");
            match framed_transport_stream.send(ping_to_send).await {
              Ok(()) => {
                self.last_activity_time = now; self.last_ping_sent_time = Some(now);
                self.waiting_for_pong = true; pong_timeout_timer.reset();
              }
              Err(e) => {
                if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: e.clone() }).await; }
                should_break_loop = true; final_error_for_actor_stop = Some(e);
              }
            }
          }
        }

        _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
          if !self.session_mailbox.is_closed() { let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::Timeout }).await; }
          should_break_loop = true; final_error_for_actor_stop = Some(ZmqError::Timeout);
        }
      }
    }

    tracing::info!(
      engine_handle = engine_actor_handle,
      role = role_str,
      "ZmtpEngineCore loop finished. Cleaning up."
    );
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
