// src/engine/core.rs (NEW FILE)

use crate::engine::{IEngine, ZmtpStream}; // Use trait alias
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpReady;
use crate::protocol::zmtp::{
  codec::ZmtpCodec,
  command::{ZmtpCommand, ZMTP_CMD_PONG_NAME}, // Import command stuff
  greeting::{ZmtpGreeting, GREETING_LENGTH},
};
use crate::runtime::{Command, MailboxReceiver, MailboxSender};
use crate::security::{Mechanism, MechanismStatus, NullMechanism, PlainMechanism};
#[cfg(feature = "curve")]
use crate::security::CurveMechanism;
use crate::socket::options::ZmtpEngineConfig;
use crate::{Blob, MsgFlags}; // Reuse config for now
use async_trait::async_trait;
use bytes::BytesMut;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use std::{fmt, marker::PhantomData};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // For raw read/write during handshake
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::codec::Framed;

/// Core ZMTP engine logic, generic over the underlying stream type.
pub(crate) struct ZmtpEngineCore<S: ZmtpStream> {
  handle: usize,
  session_mailbox: MailboxSender,
  mailbox_receiver: MailboxReceiver,
  framed_stream: Option<Framed<S, ZmtpCodec>>, // Generic stream
  config: ZmtpEngineConfig,                    // Keep config type for now
  mechanism: Box<dyn Mechanism>,
  is_server: bool,
  _marker: PhantomData<S>,     // Marker for unused generic type if needed
  last_activity_time: Instant, // Tracks reads OR writes
  last_ping_sent_time: Option<Instant>,
  waiting_for_pong: bool,
  heartbeat_ivl: Option<Duration>,
  heartbeat_timeout: Duration, // Calculated effective timeout
}

impl<S: ZmtpStream> fmt::Debug for ZmtpEngineCore<S> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ZmtpEngineCore")
      .field("handle", &self.handle)
      .field("session_mailbox", &self.session_mailbox) // Sender might be Debug
      .field("framed_stream_is_some", &self.framed_stream.is_some())
      .field("config", &self.config) // Assuming config is Debug
      .field("mechanism_status", &self.mechanism.status()) // Use status method
      .field("is_server", &self.is_server)
      .field("last_activity_time", &self.last_activity_time)
      .field("waiting_for_pong", &self.waiting_for_pong)
      .field("heartbeat_ivl", &self.heartbeat_ivl)
      .field("heartbeat_timeout", &self.heartbeat_timeout)
      .finish_non_exhaustive()
  }
}

impl<S: ZmtpStream> ZmtpEngineCore<S> {
  /// Creates the internal state for the generic engine.
  pub fn new(
    handle: usize,
    session_mailbox: MailboxSender,
    mailbox_receiver: MailboxReceiver, // Receive the receiver
    stream: S,                         // Takes generic stream
    config: ZmtpEngineConfig,
    is_server: bool,
  ) -> Self {
    let codec = ZmtpCodec::new();
    let framed = Framed::new(stream, codec);

    let heartbeat_ivl = config.heartbeat_ivl;
    // Default timeout is typically the interval itself if not specified
    let heartbeat_timeout = config
      .heartbeat_timeout
      .or(heartbeat_ivl)
      .unwrap_or_else(|| Duration::from_secs(60)); // Fallback if IVL also None? ZMQ has defaults...
                                                   // Let's default to IVL if TIMEOUT is None, assume IVL is set if keepalive enabled.
    let effective_timeout = config.heartbeat_timeout.unwrap_or_else(|| {
      heartbeat_ivl.map_or(Duration::from_secs(30), |ivl| ivl) // Default TTL = IVL, or 30s if IVL also None
    });

    Self {
      handle,
      session_mailbox,
      mailbox_receiver,
      framed_stream: Some(framed),
      config,
      mechanism: Box::new(NullMechanism), // Default placeholder
      is_server,
      _marker: PhantomData,
      last_activity_time: Instant::now(),
      last_ping_sent_time: None,
      waiting_for_pong: false,
      heartbeat_ivl: heartbeat_ivl,         // Store configured interval
      heartbeat_timeout: effective_timeout, // Store calculated timeout
    }
  }

  /// Executes the ZMTP greeting exchange and security handshake (Generic version).
  pub async fn perform_handshake(
    &mut self,                         // Needs &mut self to store final mechanism
    framed: &mut Framed<S, ZmtpCodec>, // Pass mutable reference to Framed stream
  ) -> Result<(Box<dyn Mechanism>, Option<Blob>), ZmqError> {
    // --- 1. Greeting Exchange ---
    let mut greeting_buf = BytesMut::with_capacity(GREETING_LENGTH);

    // Determine best mechanism to offer based on local config
    let own_mechanism_bytes = NullMechanism::NAME_BYTES; // Default to NULL
    // if config allows CURVE: own_mechanism_bytes = CurveMechanism::NAME_BYTES;
    // else if config allows PLAIN: own_mechanism_bytes = PlainMechanism::NAME_BYTES;

    ZmtpGreeting::encode(&own_mechanism_bytes, self.is_server, &mut greeting_buf);

    // Write own greeting (using raw stream access)
    framed.get_mut().write_all(&greeting_buf).await?;
    framed.get_mut().flush().await?; // Flush recommended after raw write
    tracing::debug!(handle = self.handle, "Sent own greeting");

    // Receive peer greeting (using raw stream access)
    let mut read_buf = BytesMut::with_capacity(GREETING_LENGTH * 2);
    let peer_greeting = loop {
      if let Some(greeting) = ZmtpGreeting::decode(&mut read_buf)? {
        break greeting; // Successfully decoded
      }
      // Read more data if needed
      let bytes_read = framed.get_mut().read_buf(&mut read_buf).await?;
      if bytes_read == 0 {
        tracing::error!(handle = self.handle, "Connection closed during greeting exchange");
        return Err(ZmqError::ConnectionClosed); // Peer closed connection
      }
      tracing::trace!(handle = self.handle, bytes_read, "Read greeting bytes");
    };
    tracing::debug!(handle = self.handle, "Received peer greeting: {:?}", peer_greeting);

    // --- 2. Validate Greeting & Select Mechanism ---
    if peer_greeting.version.0 < 3 {
      return Err(ZmqError::ProtocolViolation(format!(
        "Unsupported ZMTP version {}.{}",
        peer_greeting.version.0, peer_greeting.version.1
      )));
    }
    if self.is_server == peer_greeting.as_server {
      let msg = format!(
        "Role mismatch: we_are_server={}, peer_is_server={}",
        self.is_server, peer_greeting.as_server
      );
      tracing::error!(handle = self.handle, error = %msg, "Handshake role mismatch");
      return Err(ZmqError::ProtocolViolation(msg));
    }

    let peer_mech_name = peer_greeting.mechanism_name();
    tracing::info!(
      handle = self.handle,
      peer_mechanism = peer_mech_name,
      "Selecting mechanism"
    );

    // TODO: Implement real mechanism negotiation based on self.config.security_mechanism and peer_mech_name
    let mut mechanism: Box<dyn Mechanism> = match peer_mech_name {
      NullMechanism::NAME => Box::new(NullMechanism),
      
      PlainMechanism::NAME => {
        // TODO: Check local config allows PLAIN
        Box::new(PlainMechanism::new(self.is_server /* Add options */))
      }
      #[cfg(feature = "curve")]
      CurveMechanism::NAME => {
        // TODO: Check local config allows CURVE
        // TODO: Pass necessary keys from config
        Box::new(CurveMechanism::new(self.is_server /* Add options */))
     }
      _ => {
        let msg = format!("Unsupported security mechanism requested by peer: {}", peer_mech_name);
        tracing::error!(handle = self.handle, %msg);
        // TODO: Optionally send ZMTP ERROR frame back to peer?
        return Err(ZmqError::SecurityError(msg));
      }
    };

    // --- 3. Security Handshake (using Framed for commands) ---
    tracing::debug!(
      handle = self.handle,
      "Starting security handshake ({})",
      mechanism.name()
    );
    while !mechanism.is_complete() && !mechanism.is_error() {
      // a. Produce and send token
      if let Some(token_to_send) = mechanism.produce_token()? {
        tracing::trace!(
          handle = self.handle,
          token_len = token_to_send.len(),
          "Mechanism produced token to send"
        );
        let mut cmd_msg = Msg::from_vec(token_to_send);
        cmd_msg.set_flags(MsgFlags::COMMAND); // Security messages are commands
        framed.send(cmd_msg).await?;

        // Update activity timer? Handshake is activity.
        self.last_activity_time = Instant::now();
      }

      // b. Read and process peer token if needed
      if !mechanism.is_complete() && !mechanism.is_error() {
        // Re-check state
        tracing::trace!(handle = self.handle, "Mechanism waiting for peer token");
        match framed.next().await {
          // Read next ZMTP frame via codec
          Some(Ok(msg)) => {
            self.last_activity_time = Instant::now();

            if !msg.is_command() {
              mechanism.set_error("Expected COMMAND frame during handshake".to_string());
              return Err(ZmqError::SecurityError(
                "Expected COMMAND frame during security handshake, got data".into(),
              ));
            }
            mechanism.process_token(msg.data().unwrap_or(&[]))?; // Process token
            tracing::trace!(
              handle = self.handle,
              token_len = msg.size(),
              "Mechanism processed token"
            );
          }
          Some(Err(e)) => {
            tracing::error!(handle=self.handle, error=%e, "Error reading security frame");
            // mechanism.set_error(format!("Transport read error: {}", e)); // Optional
            return Err(e); // Propagate codec/IO error
          }
          None => {
            tracing::error!(handle = self.handle, "Stream closed during security handshake");
            // mechanism.set_error("Connection closed during handshake".to_string()); // Optional
            return Err(ZmqError::ConnectionClosed);
          }
        }
      }

      if let Some(_zap_req_frames) = mechanism.zap_request_needed() {
        tracing::warn!("ZAP required but not yet implemented in Engine Core. Failing handshake.");
        // Send RequestZapAuth command up to Session mailbox
        // Wait for ProcessZapReply command from Session mailbox (needs select! or dedicated state)
        // mechanism.process_zap_reply(...)?;

        // Placeholder: Fail if ZAP needed but not implemented
        mechanism.set_error("ZAP authentication required but not implemented".to_string());
      }
    } // end while handshake not complete/error

    if mechanism.is_error() {
       let reason = mechanism.error_reason().unwrap_or("Mechanism handshake failed");
       tracing::error!(handle = self.handle, mechanism = mechanism.name(), reason, "Security handshake failed");
       // TODO: Send ZMTP ERROR frame?
       return Err(ZmqError::SecurityError(reason.to_string()));
    }
    tracing::info!(
      handle = self.handle,
      "Security handshake successful ({})",
      mechanism.name()
    );

    // --- 4. READY Command Exchange ---
    let mut identity_from_ready: Option<Blob> = None;

    // Get socket type name and local identity from engine's stored config
    let socket_type_name = self.config.socket_type_name.clone();
    let local_identity = self.config.routing_id.clone();

    if !self.is_server {
      // Client sends READY first
      let mut props = HashMap::new();
      props.insert("Socket-Type".to_string(), socket_type_name.as_bytes().to_vec());
      if let Some(id) = local_identity {
        if !id.is_empty() && id.len() <= 255 {
          props.insert("Identity".to_string(), id.to_vec());
        } else {
          tracing::warn!(
            handle = self.handle,
            "Local ROUTING_ID is invalid, not sending in READY."
          );
        }
      }
      let ready_msg = ZmtpReady::create_msg(props);
      tracing::debug!(handle = self.handle, "Client sending READY");
      framed.send(ready_msg).await?;
      // No explicit flush here needed generally for commands unless corking aggressively
    }

    // Both sides wait for peer's READY command
    tracing::debug!(handle = self.handle, "Waiting for peer READY command");
    let peer_ready_cmd = loop {
      match framed.next().await {
        Some(Ok(msg)) => {
          if !msg.is_command() {
            return Err(ZmqError::ProtocolViolation("Expected READY command, got data".into()));
          }
          if let Some(ZmtpCommand::Ready(ready)) = ZmtpCommand::parse(&msg) {
            break ready; // Got peer READY
          } else {
            // Ignore other valid commands? Or error? Let's error for now.
            tracing::warn!(
              handle = self.handle,
              "Expected READY command, got different/invalid command frame"
            );
            return Err(ZmqError::ProtocolViolation(
              "Expected READY command, got other command".into(),
            ));
          }
        }
        Some(Err(e)) => return Err(e),                  // Propagate error
        None => return Err(ZmqError::ConnectionClosed), // EOF
      }
    };
    tracing::debug!(handle = self.handle, "Received peer READY");

    // Extract peer identity if present in metadata
    if let Some(id_bytes) = peer_ready_cmd.properties.get("Identity") {
      if !id_bytes.is_empty() && id_bytes.len() <= 255 {
         // <<< MODIFIED: Assign to correct variable >>>
         identity_from_ready = Some(Blob::from(id_bytes.clone()));
         // <<< MODIFIED END >>>
         tracing::info!(handle = self.handle, "Received peer identity via READY");
      } else {
         tracing::warn!(handle = self.handle, "Received invalid peer identity in READY");
      }
   }
    // TODO: Validate received peer Socket-Type against ours for compatibility?

    if self.is_server {
      // Server sends READY second
      let mut props = HashMap::new();
      props.insert("Socket-Type".to_string(), socket_type_name.as_bytes().to_vec());
      let ready_msg = ZmtpReady::create_msg(props);
      tracing::debug!(handle = self.handle, "Server sending READY");
      framed.send(ready_msg).await?;
      // No explicit flush needed generally
    }

    let identity_from_mechanism: Option<Blob> = mechanism.peer_identity().map(Blob::from);

    // Return the successfully negotiated mechanism and the extracted peer identity
    Ok((mechanism, identity_from_ready.or(identity_from_mechanism)))
  } // end perform_handshake

  /// Main message processing loop (Generic version).
  pub async fn run_loop(mut self) {
    let handle = self.handle; // Capture handle for logging consistency
    tracing::info!(handle = handle, server = self.is_server, "ZmtpEngineCore actor started");

    // Take ownership of Framed stream from Option, handle case where it's already None
    let mut framed: Framed<S, ZmtpCodec> = match self.framed_stream.take() {
      Some(f) => f,
      None => {
        tracing::error!(handle = handle, "Engine started without a stream!");
        // Attempt to notify session, but ignore error as mailbox might also be invalid
        let _ = self
          .session_mailbox
          .send(Command::EngineError {
            error: ZmqError::Internal("Engine started without stream".into()),
          })
          .await;
        return; // Cannot proceed
      }
    };

    // Phase 1: Handshake
    // Perform handshake, get negotiated mechanism and potentially peer identity
    match self.perform_handshake(&mut framed).await {
      Ok((mechanism, identity_opt)) => {
        tracing::info!(
          handle = handle,
          mechanism = mechanism.name(),
          "ZMTP Handshake successful"
        );
        self.mechanism = mechanism; // Store the final mechanism
                                    // Send EngineReady *with* the extracted identity back to Session
        if self
          .session_mailbox
          .send(Command::EngineReady {
            peer_identity: identity_opt,
          })
          .await
          .is_err()
        {
          tracing::warn!(
            handle = handle,
            "Session mailbox closed after handshake, stopping engine."
          );
          let _ = framed.close().await; // Attempt to close stream
          return; // Session already gone
        }
      }
      Err(e) => {
        tracing::error!(handle = handle, error = %e, "ZMTP Handshake failed");
        // Send error to session
        let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
        let _ = framed.close().await; // Attempt to close stream
        return; // Stop engine task
      }
    }
    // --- Handshake Complete ---

    // Reset activity timer *after* successful handshake
    self.last_activity_time = Instant::now();

    // Phase 2: Message Loop
    tracing::debug!(handle = handle, "Entering message loop");
    let keepalive_enabled = self.heartbeat_ivl.is_some();
    let mut ping_check_timer = if let Some(ivl) = self.heartbeat_ivl {
      // Check slightly more frequently than the interval to avoid drift issues
      let check_interval = ivl.checked_sub(Duration::from_millis(50)).unwrap_or(ivl);
      let mut timer = interval(check_interval.max(Duration::from_millis(100))); // Ensure >0 interval
      timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay); // Or Skip? Delay seems safer.
      Some(timer)
    } else {
      None
    };
    // Pong timeout timer - only active when waiting_for_pong is true
    let mut pong_timeout_timer = interval(self.heartbeat_timeout); // Reuse interval timer
    pong_timeout_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip); // Skip if busy
    pong_timeout_timer.reset(); // Start paused initially

    let mut should_break = false; // Flag to signal loop termination
    while !should_break {
      tokio::select! {
          biased; // Prioritize commands slightly over reading from network

          // --- Arm 1: Receive Commands from Session ---
          cmd_result = self.mailbox_receiver.recv() => {
              let command = match cmd_result {
                  Ok(cmd) => cmd,
                  Err(_) => {
                      // Mailbox closed, usually means Session initiated a clean stop.
                      tracing::info!(handle = handle, "Engine command mailbox closed, stopping.");
                      should_break = true;
                      continue; // Skip to end of loop to break
                  }
              };

              // Process command received from Session
              match command {
                  Command::SessionPushCmd { msg } => {
                      // Received data message from Session to send over transport
                      tracing::trace!(handle=handle, msg_size=msg.size(), "Engine sending message via Framed");
                      if let Err(e) = framed.send(msg).await {
                        tracing::error!(handle=handle, error=%e, "Engine failed to send message");
                        let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                        should_break = true; // Treat send error as fatal
                      } else {
                          // Successfully sent, update activity timer
                        self.last_activity_time = Instant::now();
                      }
                      // No explicit flush needed here for ZMTP messages typically,
                      // unless TCP_CORK is manually managed at a lower level.
                  }
                  Command::Stop => {
                      // Received explicit stop command from Session
                      tracing::info!(handle = handle, "Engine received Stop command");
                      should_break = true;
                  }
                  // TODO: Handle ProcessZapReply if implementing ZAP
                  // Command::ProcessZapReply { reply_frames } => { /* ... */ }
                  _ => tracing::warn!(handle = handle, "Engine received unhandled command: {:?}", command),
              } // end match command
          } // end cmd_result arm


          // --- Arm 2: Receive Messages from Transport (via Framed) ---
          frame_result = framed.next() => {
              match frame_result {
                  Some(Ok(msg)) => {
                      // Successfully decoded a frame using ZmtpCodec::decode
                      self.last_activity_time = Instant::now(); // Reset timer on ANY frame received
                      tracing::trace!(handle=handle, msg_size=msg.size(), flags=?msg.flags(), "Engine decoded frame");

                      if msg.is_command() {
                          // Handle ZMTP Command Frame
                          if let Some(command) = ZmtpCommand::parse(&msg) {
                              match command {
                                  ZmtpCommand::Ping(context) => {
                                    tracing::debug!(handle=handle, "Received PING, sending PONG");
                                    let pong_msg = ZmtpCommand::create_pong(&context);
                                    if let Err(e) = framed.send(pong_msg).await {
                                      tracing::error!(handle=handle, error=%e, "Failed to send PONG");
                                      let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                                      should_break = true; // Treat as fatal
                                    } else {
                                      self.last_activity_time = Instant::now(); // Update on successful PONG send
                                    }
                                    // No flush generally needed after command
                                  }
                                  ZmtpCommand::Pong(_context) => {
                                    tracing::debug!(handle=handle, "Received PONG");
                                    if self.waiting_for_pong {
                                      self.waiting_for_pong = false;
                                      self.last_ping_sent_time = None;
                                      // Reset pong timer (effectively pausing it until next PING)
                                      pong_timeout_timer.reset();
                                      tracing::trace!(handle=handle, "Keepalive PONG received, timer reset.");
                                    } else {
                                      tracing::warn!(handle=handle, "Received unexpected PONG (was not waiting).");
                                    }
                                  }
                                  ZmtpCommand::Ready(_) => {
                                      tracing::warn!(handle=handle, "Received unexpected READY command after handshake");
                                      // Optionally treat as protocol violation
                                  }
                                  ZmtpCommand::Error => {
                                      tracing::error!(handle=handle, "Received ERROR command from peer");
                                      // TODO: Parse reason string
                                      let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::ProtocolViolation("Received ERROR command from peer".into()) }).await;
                                      should_break = true; // Treat ERROR as fatal
                                  }
                                  ZmtpCommand::Unknown(body) => {
                                      tracing::warn!(handle=handle, cmd_body=?body, "Received unhandled ZMTP command frame");
                                      // Ignore unknown commands for forward compatibility?
                                  }
                              } // end match ZmtpCommand
                          } else { // else ZmtpCommand::parse failed
                              tracing::warn!(handle=self.handle, "Failed to parse received ZMTP command frame body");
                              // Treat as protocol violation?
                              // let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::ProtocolViolation("Failed to parse command body".into()) }).await;
                              // should_break = true;
                          } // end if let Some command
                      } else {
                          // Handle Data Message Frame -> Send EnginePushCmd up to Session
                          if self.session_mailbox.send(Command::EnginePushCmd { msg }).await.is_err() {
                               // Session mailbox closed
                               tracing::warn!(handle=self.handle, "Session mailbox closed while sending EnginePushCmd. Stopping engine.");
                               should_break = true; // Session is gone
                          }
                      } // end if msg.is_command / else
                  } // end Some(Ok(msg))
                  Some(Err(e)) => {
                      // Decoding error (e.g., ProtocolViolation from codec) or Stream I/O error
                      tracing::error!(handle=self.handle, error=%e, "Error reading/decoding frame from stream");
                      let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                      should_break = true; // Exit loop on decode/read error
                  }
                  None => {
                      // Stream closed by peer (EOF)
                      tracing::info!(handle=self.handle, "Transport stream closed by peer (EOF)");
                      should_break = true; // Exit loop
                  }
              } // end match frame_result
          } // end frame_result arm

          // Check if we need to send a PING
          _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_enabled && ping_check_timer.is_some() && !self.waiting_for_pong => {
            let now = Instant::now();
            if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
                tracing::debug!(handle=handle, interval=?self.heartbeat_ivl.unwrap(), "Keepalive interval expired, sending PING.");
                // Send PING command (use empty context for now)
                let ping_msg = ZmtpCommand::create_ping(0, b""); // TTL 0 for ZMTP 3.x PING
                 match framed.send(ping_msg).await {
                    Ok(()) => {
                       self.last_activity_time = now; // Update activity time after successful PING send
                       self.last_ping_sent_time = Some(now);
                       self.waiting_for_pong = true;
                       // Reset the pong timeout timer to start timing NOW
                       pong_timeout_timer.reset(); // Starts the timer for heartbeat_timeout duration
                       tracing::trace!(handle=handle, timeout=?self.heartbeat_timeout, "PING sent, waiting for PONG.");
                    }
                    Err(e) => {
                        tracing::error!(handle=handle, error=%e, "Failed to send keepalive PING");
                         let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                        should_break = true; // Treat as fatal if we can't send PING
                    }
                 }
            }
        }

        // Check if PONG timeout expired
        _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
          // Timer only ticks if waiting_for_pong is true (reset() called after PING sent)
          // No need to check Instant again, the tick itself signifies timeout
          tracing::error!(handle=handle, timeout=?self.heartbeat_timeout, "Keepalive PONG timed out.");
          let _ = self.session_mailbox.send(Command::EngineError { error: ZmqError::Timeout }).await;
          should_break = true;
      }

      } // end select!
    } // end while !should_break

    // --- Cleanup ---
    tracing::info!(handle = self.handle, "ZmtpEngineCore actor stopping");
    // Close underlying stream via Framed wrapper before sending EngineStopped
    let _ = framed.close().await;
    // Notify Session of clean stop (unless loop broken by error where EngineError was already sent)
    // Send EngineStopped regardless for simplicity. Session can track if error already received.
    let _ = self.session_mailbox.send(Command::EngineStopped).await; // Ignore error if session already gone
  }
} // end impl ZmtpEngineCore
