// src/engine/zmtp_tcp.rs

use crate::engine::IEngine; // Import the trait
use crate::error::ZmqError;
use crate::protocol::zmtp::{
  codec::ZmtpCodec, // Import the codec
  greeting::{ZmtpGreeting, GREETING_LENGTH}, // Import greeting logic
                    // Import mechanism constants/types later when needed
};
use crate::runtime::{mailbox, Command, MailboxReceiver, MailboxSender};
use crate::security::{Mechanism, MechanismStatus, NullMechanism};
use crate::socket::options::ZmtpTcpConfig;
use crate::{Msg, MsgFlags};

use std::fmt;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::sink::SinkExt; // For Framed::send
use futures::stream::StreamExt; use tokio::io::{AsyncReadExt, AsyncWriteExt};
// For Framed::next
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::codec::{Decoder, Encoder, Framed};

pub(crate) struct ZmtpTcpEngine {
  handle: usize,
  session_mailbox: MailboxSender,    // Mailbox TO Session
  mailbox_receiver: MailboxReceiver, // Mailbox FROM Session
  // Wrap stream + codec in Framed for convenient message handling
  framed_stream: Option<Framed<TcpStream, ZmtpCodec>>,
  config: ZmtpTcpConfig,
  mechanism: Box<dyn Mechanism>, // Store the chosen mechanism
  is_server: bool,               // Track if we initiated as server (listener) or client (connecter)
}

impl fmt::Debug for ZmtpTcpEngine {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ZmtpTcpEngine")
      .field("handle", &self.handle)
      .field("session_mailbox", &self.session_mailbox)
      .field("framed_stream_is_some", &self.framed_stream.is_some())
      .field("config", &self.config)
      .field("mechanism_status", &self.mechanism.status())
      .field("is_server", &self.is_server)
      .finish_non_exhaustive()
  }
}

impl ZmtpTcpEngine {
  /// Creates and spawns the ZmtpTcpEngine actor task.
  pub(crate) fn create_and_spawn(
    handle: usize,
    session_mailbox: MailboxSender,
    stream: TcpStream, // Takes the connected stream
    config: ZmtpTcpConfig,
    is_server: bool,
  ) -> (MailboxSender, JoinHandle<()>) {
    let (tx, rx) = mailbox();

    // Assume NullMechanism for now, actual mechanism selected after greeting
    let codec = ZmtpCodec::new();
    let framed = Framed::new(stream, codec); // Wrap stream and codec

    let engine = ZmtpTcpEngine {
      handle,
      session_mailbox,             // Corrected: Pass the one given
      mailbox_receiver: rx,        // Use the receiver paired with tx
      framed_stream: Some(framed), // Store Framed stream
      config,
      mechanism: Box::new(NullMechanism), // Placeholder, select after greeting
      is_server,
    };
    let task_handle = tokio::spawn(engine.run_loop());
    (tx, task_handle)
  }

  async fn run_loop(mut self) {
    tracing::info!(handle = self.handle, "ZmtpTcpEngine actor started");

    let mut framed = match self.framed_stream.take() {
      Some(f) => f,
      None => {
        tracing::error!(
          handle = self.handle,
          "Engine started without a framed stream!"
        );
        // Notify session? Should not happen with current create_and_spawn.
        let _ = self
          .session_mailbox
          .send(Command::EngineError {
            error: ZmqError::Internal("Engine started without stream".into()),
          })
          .await;
        // Don't send EngineStopped here, as we didn't really start properly.
        return;
      }
    };

    // --- Phase 1: Handshake ---
    tracing::debug!(handle = self.handle, "Starting ZMTP handshake");
    match self.perform_handshake(&mut framed).await {
      Ok(mechanism) => {
        tracing::info!(
          handle = self.handle,
          mechanism = mechanism.name(),
          "ZMTP Handshake successful"
        );
        self.mechanism = mechanism; // Store the negotiated mechanism
                                    // Notify Session that engine is ready for messages
        if self
          .session_mailbox
          .send(Command::EngineReady)
          .await
          .is_err()
        {
          tracing::warn!(
            handle = self.handle,
            "Session mailbox closed after handshake, stopping engine."
          );
          return; // Session already gone
        }
      }
      Err(e) => {
        tracing::error!(handle = self.handle, error = %e, "ZMTP Handshake failed");
        let _ = self
          .session_mailbox
          .send(Command::EngineError { error: e })
          .await;
        // Don't send EngineStopped, EngineError implies failure
        return; // Stop engine task
      }
    }
    // --- Handshake Complete ---

    tracing::debug!(handle = self.handle, "Entering message loop");
    let mut message_counter: u64 = 0; // For dummy messages (keep for now)

    loop {
      tokio::select! {
          // ... (select arms: recv command, read stream placeholder, sleep) ...
          biased;
          cmd_result = self.mailbox_receiver.recv() => {

            let command = match cmd_result { /* ... handle Err (break loop) ... */
              Ok(cmd) => cmd,
              Err(_) => {
                  tracing::info!(handle = self.handle, "Engine mailbox closed, stopping.");
                  // Close framed stream gracefully before breaking
                  let _ = framed.close().await;
                  self.framed_stream = None; // Mark as gone
                  break;
              }
          };
              match command {
                  Command::SessionPushCmd { msg } => {

                    tracing::trace!(handle=self.handle, msg_size=msg.size(), "Engine received push from Session");
                    if let Err(e) = framed.send(msg).await {
                      tracing::error!(handle=self.handle, error=%e, "Engine failed to send message via Framed stream");
                      // Treat stream write error as fatal
                      let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                      break; // Exit loop
                    }
                  }
                  Command::Stop => {
                    let _ = framed.close().await; // Close stream gracefully
                    self.framed_stream = None; // Mark as gone
                      break; // Exit loop
                  }
                  _ => tracing::warn!(handle = self.handle, "Engine received unhandled command: {:?}", command),
              }
          }
          // --- Receive Messages from TCP Stream (via Framed) ---
          frame_result = framed.next() => {
            match frame_result {
              Some(Ok(msg)) => {
                  // Successfully decoded a message/command frame
                  tracing::trace!(handle=self.handle, msg_size=msg.size(), flags=?msg.flags(), "Engine decoded frame");
                  if msg.is_command() {
                      // --- Handle ZMTP Command Frame ---
                      // TODO: Implement command handling (PING/PONG, ERROR?, etc.)
                      tracing::warn!(handle=self.handle, "Received unhandled ZMTP command frame");
                  } else {
                      // --- Handle Data Message Frame ---
                      // Send EnginePushCmd up to Session
                      if self.session_mailbox.send(Command::EnginePushCmd { msg }).await.is_err() {
                            tracing::warn!(handle=self.handle, "Session mailbox closed while sending EnginePushCmd. Stopping engine.");
                            break; // Session is gone
                      }
                  }
              }
              Some(Err(e)) => {
                  // Decoding error (e.g., ProtocolViolation from codec) or Stream I/O error
                  tracing::error!(handle=self.handle, error=%e, "Error reading/decoding frame from stream");
                  let _ = self.session_mailbox.send(Command::EngineError { error: e }).await;
                  break; // Exit loop on decode/read error
              }
              None => {
                  // Stream closed by peer (EOF)
                  tracing::info!(handle=self.handle, "TCP stream closed by peer (EOF)");
                  // Session should be notified via EngineStopped eventually
                  break; // Exit loop
              }
            } // end match frame_result
          } // end frame_result = framed.next() arm
          _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
            
            // Check framed_stream again just before sending simulation
            if self.framed_stream.is_none() { continue; }

            message_counter += 1;
            let dummy_payload = format!("Engine Message {} from handle {}", message_counter, self.handle);
            let dummy_msg = Msg::from_vec(dummy_payload.into_bytes());
            tracing::trace!(handle=self.handle, msg_size=dummy_msg.size(), "Engine simulating received TCP message");

            // <<< ADDED: Send EnginePushCmd Up >>>
            if self.session_mailbox.send(Command::EnginePushCmd { msg: dummy_msg }).await.is_err() {
                  tracing::warn!(handle=self.handle, "Session mailbox closed while sending EnginePushCmd. Stopping engine.");
                  break; // Session is gone
            }
          }
      }
    }
    tracing::info!(handle = self.handle, "ZmtpTcpEngine actor stopped");

    // Close stream if still open (Framed drop might do this?)
    if let Some(mut framed) = Some(framed) {
      // Re-take ownership to close
      let _ = framed.close().await;
    }
    // Notify Session cleanly
    let _ = self.session_mailbox.send(Command::EngineStopped).await;
  } // end run_loop

  /// Executes the ZMTP greeting exchange and security handshake.
  async fn perform_handshake(
    &mut self,
    framed: &mut Framed<TcpStream, ZmtpCodec>,
  ) -> Result<Box<dyn Mechanism>, ZmqError> {
    // 1. Send own greeting
    let mut greeting_buf = BytesMut::with_capacity(GREETING_LENGTH);
    // TODO: Choose mechanism based on socket options later (e.g., CURVE keys)
    let own_mechanism = NullMechanism::NAME_BYTES; // Use NULL for now
    ZmtpGreeting::encode(&own_mechanism, self.is_server, &mut greeting_buf);
    framed.get_mut().write_all(&greeting_buf).await?; // Write raw bytes directly
    framed.get_mut().flush().await?; // Ensure it's sent
    tracing::debug!(handle = self.handle, "Sent own greeting");

    // 2. Receive peer greeting
    let mut read_buf = BytesMut::with_capacity(GREETING_LENGTH * 2); // Allow extra space
    let peer_greeting = loop {
      // Read until we have enough data or get an error/EOF
      if let Some(greeting) = ZmtpGreeting::decode(&mut read_buf)? {
        break greeting; // Successfully decoded
      }
      // Read more data from the raw stream (bypass Framed for greeting)
      let bytes_read = framed.get_mut().read_buf(&mut read_buf).await?;
      if bytes_read == 0 {
        tracing::error!(
          handle = self.handle,
          "Connection closed during greeting exchange"
        );
        return Err(ZmqError::ConnectionClosed); // Or ProtocolViolation?
      }
      tracing::trace!(
        handle = self.handle,
        bytes_read = bytes_read,
        "Read greeting bytes"
      );
    };
    tracing::debug!(
      handle = self.handle,
      "Received peer greeting: {:?}",
      peer_greeting
    );

    // 3. Validate peer greeting (Version checked in decode)
    // Check AsServer flag consistency? Server expects Client, Client expects Server.
    if self.is_server == peer_greeting.as_server {
      let msg = format!(
        "Role mismatch: we_are_server={}, peer_is_server={}",
        self.is_server, peer_greeting.as_server
      );
      tracing::error!(handle = self.handle, error = %msg, "Handshake role mismatch");
      return Err(ZmqError::ProtocolViolation(msg));
    }

    // 4. Select and Initialize Mechanism based on peer greeting
    // TODO: Implement mechanism negotiation (e.g., based on socket options and peer mechanism)
    let peer_mech_name = peer_greeting.mechanism_name();
    tracing::info!(
      handle = self.handle,
      peer_mechanism = peer_mech_name,
      "Selecting mechanism"
    );
    let mut mechanism: Box<dyn Mechanism> = match peer_mech_name {
      "NULL" => Box::new(NullMechanism),
      // "PLAIN" => Box::new(PlainMechanism::new(...)), // Requires config
      // "CURVE" => Box::new(CurveMechanism::new(...)), // Requires keys
      _ => {
        let msg = format!(
          "Unsupported security mechanism requested by peer: {}",
          peer_mech_name
        );
        tracing::error!(handle = self.handle, error = %msg);
        // TODO: Send ZMTP ERROR command frame back?
        return Err(ZmqError::SecurityError(msg));
      }
    };

    // 5. Drive Security Handshake (using Framed for command frames)
    // For NULL, this is trivial. For PLAIN/CURVE it's a loop.
    tracing::debug!(
      handle = self.handle,
      "Starting security handshake (NULL is trivial)"
    );
    while !mechanism.is_complete() && !mechanism.is_error() {
      // a. Produce outgoing token (if any)
      if let Some(token_to_send) = mechanism.produce_token()? {
        tracing::trace!(
          handle = self.handle,
          token_len = token_to_send.len(),
          "Mechanism produced token to send"
        );
        // Wrap token in a COMMAND Msg
        let mut cmd_msg = Msg::from_vec(token_to_send);
        cmd_msg.set_flags(MsgFlags::COMMAND); // Mark as command
        framed.send(cmd_msg).await?; // Send via Framed codec
      }

      // b. If waiting for peer, read next frame
      if !mechanism.is_complete() && !mechanism.is_error() {
        // Re-check state
        tracing::trace!(handle = self.handle, "Mechanism waiting for peer token");
        match framed.next().await {
          Some(Ok(msg)) => {
            if !msg.is_command() {
              return Err(ZmqError::SecurityError(
                "Expected COMMAND frame during handshake, got data".into(),
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
            return Err(e); // Propagate codec/IO error
          }
          None => {
            tracing::error!(
              handle = self.handle,
              "Stream closed during security handshake"
            );
            return Err(ZmqError::ConnectionClosed);
          }
        }
      }
      // TODO: Add ZAP interaction logic here if mechanism.zap_request_needed()
    } // end while !complete && !error

    if mechanism.is_error() {
      return Err(ZmqError::SecurityError("Mechanism handshake failed".into()));
    }

    // Handshake successful
    Ok(mechanism) // Return the initialized mechanism
  }
}

// Dummy IEngine impl
#[async_trait]
impl IEngine for ZmtpTcpEngine {
  async fn send_message(&self, _msg: crate::message::Msg) -> Result<(), ZmqError> {
    unimplemented!()
  }
  async fn start_handshake(&self) -> Result<(), ZmqError> {
    unimplemented!()
  } // Handshake simulation is now in run_loop start
  async fn process_zap_reply(&self, _reply_frames: Vec<Vec<u8>>) -> Result<(), ZmqError> {
    unimplemented!()
  }
  fn get_security_status(&self) -> crate::security::MechanismStatus {
    self.mechanism.status() // Get status from the actual mechanism
  }
}
