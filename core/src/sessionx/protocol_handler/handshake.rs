use super::ZmtpProtocolHandlerX;
use crate::Blob;
use crate::MsgFlags;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpCodec;
use crate::protocol::zmtp::command::{ZmtpCommand, ZmtpReady};
use crate::protocol::zmtp::greeting::{GREETING_LENGTH, ZmtpGreeting};
#[cfg(feature = "noise_xx")]
use crate::security::NoiseXxMechanism;
#[cfg(feature = "plain")]
use crate::security::PlainMechanism;
use crate::security::mechanism::ProcessTokenAction;
use crate::security::{NullMechanism, negotiate_security_mechanism};
use crate::transport::ZmtpStdStream;

use bytes::BytesMut;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Encoder;

use crate::sessionx::types::{HandshakeSubPhaseX, ZmtpHandshakeProgressX};

pub(crate) async fn advance_handshake_step_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  // The overall handshake timeout (handler.config.handshake_timeout)
  // will be enforced by SessionConnectionActorX.
  let operation_timeout = Duration::from_secs(15); // Default for individual send/recv during handshake

  match handler.handshake_state.sub_phase {
    HandshakeSubPhaseX::GreetingExchange => {
      exchange_greetings_impl(handler, operation_timeout).await
    }
    HandshakeSubPhaseX::SecurityHandshake => {
      perform_security_handshake_step_impl(handler, operation_timeout).await
    }
    HandshakeSubPhaseX::ReadyExchange => {
      perform_ready_exchange_step_impl(handler, operation_timeout).await
    }
    HandshakeSubPhaseX::Done => Ok(ZmtpHandshakeProgressX::HandshakeComplete),
  }
}

/// Reads from the stream until one complete ZMTP frame is available, then parses it.
/// This function is specifically for the handshake phase where messages are unencrypted.
/// It ensures that the returned message is a command, returning a ProtocolViolation error otherwise.
async fn read_handshake_command_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  timeout_duration: Duration,
) -> Result<Msg, ZmqError> {
  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for reading handshake command".into()))?;

  // Use an overall deadline for the entire operation of getting one command.
  let deadline = tokio::time::Instant::now() + timeout_duration;

  loop {
    // 1. Attempt to parse a message from any data already in the buffer.
    if !handler.network_read_buffer.is_empty() {
      match handler
        .zmtp_manual_parser
        .decode_from_buffer(&mut handler.network_read_buffer)
      {
        Ok(Some(msg)) => {
          // Successfully parsed a message.
          handler.heartbeat_state.record_activity();

          // During the handshake, all messages MUST be command frames.
          if !msg.is_command() {
            tracing::error!(
              sca_handle = handler.actor_handle,
              "Expected COMMAND frame during handshake, but received a data frame."
            );
            return Err(ZmqError::ProtocolViolation(
              "Expected COMMAND frame in handshake".into(),
            ));
          }
          return Ok(msg);
        }
        Ok(None) => {
          // Not enough data in the buffer for a full frame. Continue to read from network.
        }
        Err(e) => return Err(e), // A parsing error occurred.
      }
    }

    // 2. If no message was parsed, read more data from the network.
    let remaining_time = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining_time.is_zero() {
      return Err(ZmqError::Timeout);
    }

    let bytes_read = tokio::time::timeout(
      remaining_time,
      stream.read_buf(&mut handler.network_read_buffer),
    )
    .await
    .map_err(|_| ZmqError::Timeout)? // Map tokio's timeout to our ZmqError::Timeout
    .map_err(|e| ZmqError::from_io_endpoint(e, "handshake command read"))?;

    if bytes_read == 0 {
      // The stream was closed by the peer.
      return Err(ZmqError::ConnectionClosed);
    }

    handler.heartbeat_state.record_activity();
    // Loop will now repeat, attempting to parse again with the new data.
  }
}

async fn send_handshake_command_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  mut command_msg: Msg,
  timeout_duration: Duration,
) -> Result<(), ZmqError> {
  command_msg.set_flags(MsgFlags::COMMAND);
  let mut temp_zmtp_encoder = ZmtpCodec::new();
  let mut encoded_command_buffer = BytesMut::new();
  temp_zmtp_encoder.encode(command_msg, &mut encoded_command_buffer)?;
  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for send".into()))?;
  tokio::time::timeout(timeout_duration, stream.write_all(&encoded_command_buffer))
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "handshake command send"))?;
  handler.heartbeat_state.record_activity();
  Ok(())
}

async fn exchange_greetings_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  operation_timeout: Duration,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  let own_greeting_mechanism_bytes = determine_own_greeting_mechanism_impl(handler);
  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable".into()))?;
  let mut greeting_buffer_to_send = BytesMut::with_capacity(GREETING_LENGTH);
  ZmtpGreeting::encode(
    own_greeting_mechanism_bytes,
    handler.is_server,
    &mut greeting_buffer_to_send,
  );
  tokio::time::timeout(
    operation_timeout,
    stream.write_all(&greeting_buffer_to_send),
  )
  .await
  .map_err(|_| ZmqError::Timeout)?
  .map_err(|e| ZmqError::from_io_endpoint(e, "g send"))?;
  tokio::time::timeout(operation_timeout, stream.flush())
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "g flush"))?;
  handler.heartbeat_state.record_activity();
  tracing::debug!(
    sca_handle = handler.actor_handle,
    role = if handler.is_server { "S" } else { "C" },
    "Sent ZMTP greeting."
  );

  handler.network_read_buffer.clear();
  if handler.network_read_buffer.capacity() < GREETING_LENGTH {
    handler.network_read_buffer.reserve(GREETING_LENGTH);
  }
  let deadline = Instant::now() + operation_timeout;
  while handler.network_read_buffer.len() < GREETING_LENGTH {
    let remaining_time = deadline.saturating_duration_since(Instant::now());
    if remaining_time.is_zero() && handler.network_read_buffer.len() < GREETING_LENGTH {
      return Err(ZmqError::Timeout);
    }
    let br = tokio::time::timeout(
      remaining_time,
      stream.read_buf(&mut handler.network_read_buffer),
    )
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "g read"))?;
    if br == 0 {
      return Err(ZmqError::ConnectionClosed);
    }
    handler.heartbeat_state.record_activity();
  }
  match ZmtpGreeting::decode(&mut handler.network_read_buffer) {
    Ok(Some(pg)) => {
      if pg.version.0 < 3 {
        return Err(ZmqError::ProtocolViolation(format!(
          "V {}.{}",
          pg.version.0, pg.version.1
        )));
      }
      handler.pending_peer_greeting = Some(pg);
      handler.handshake_state.sub_phase = HandshakeSubPhaseX::SecurityHandshake;
      Ok(ZmtpHandshakeProgressX::InProgress)
    }
    Ok(None) => Err(ZmqError::ProtocolViolation("g decode".into())),
    Err(e) => Err(e),
  }
}

fn determine_own_greeting_mechanism_impl<S: ZmtpStdStream>(
  handler: &ZmtpProtocolHandlerX<S>,
) -> &'static [u8; 20] {
  #[cfg(feature = "noise_xx")]
  if handler.config.use_noise_xx {
    let can_propose_noise = if handler.is_server {
      handler.config.noise_xx_local_sk_bytes_for_engine.is_some()
    } else {
      handler.config.noise_xx_local_sk_bytes_for_engine.is_some()
        && handler.config.noise_xx_remote_pk_bytes_for_engine.is_some()
    };
    if can_propose_noise {
      return NoiseXxMechanism::NAME_BYTES;
    }
  }

  #[cfg(feature = "curve")]
  if handler.config.use_curve {
    return crate::security::CurveMechanism::NAME_BYTES;
  }

  #[cfg(feature = "plain")]
  if handler.config.use_plain {
    return PlainMechanism::NAME_BYTES;
  }

  return NullMechanism::NAME_BYTES;
}

async fn perform_security_handshake_step_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  operation_timeout: Duration,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  // 1. One-time setup: On the first entry into this phase, negotiate and
  //    initialize the actual security mechanism based on the peer's greeting.
  if handler.security_mechanism.name() == "NULL" {
    let peer_greeting = handler.pending_peer_greeting.as_ref().ok_or_else(|| {
      ZmqError::Internal("Handshake entered security phase without a peer greeting".to_string())
    })?;

    // This replaces the initial NullMechanism with the negotiated one (e.g., CurveMechanism).
    handler.security_mechanism = negotiate_security_mechanism(
      handler.is_server,
      &handler.config,
      peer_greeting,
      handler.actor_handle,
    )?;

    tracing::debug!(
      sca_handle = handler.actor_handle,
      mechanism = handler.security_mechanism.name(),
      "Security mechanism negotiated and initialized."
    );
  }

  // 2. Main handshake loop: Drive the mechanism's state machine.
  loop {
    // Check for terminal states first.
    if handler.security_mechanism.is_complete() {
      // The handshake is done. Finalize it by creating the framer.
      let mechanism_to_finalize =
        std::mem::replace(&mut handler.security_mechanism, Box::new(NullMechanism));

      let (new_framer, id_opt) = mechanism_to_finalize.into_framer()?;
      handler.framer = new_framer;

      // Transition the main handshake state machine to the next phase.
      handler.handshake_state.sub_phase = HandshakeSubPhaseX::ReadyExchange;

      // Report progress, including the identity if the mechanism provided one.
      return if let Some(identity) = id_opt {
        Ok(ZmtpHandshakeProgressX::IdentityReady(identity.into()))
      } else {
        Ok(ZmtpHandshakeProgressX::InProgress)
      };
    }

    if handler.security_mechanism.is_error() {
      return Err(ZmqError::SecurityError(
        handler
          .security_mechanism
          .error_reason()
          .unwrap_or("Unknown security error")
          .to_string(),
      ));
    }

    // A. Ask the mechanism if it has a token to send.
    if let Some(token_to_send) = handler.security_mechanism.produce_token()? {
      tracing::debug!(
        sca_handle = handler.actor_handle,
        token_len = token_to_send.len(),
        mechanism = handler.security_mechanism.name(),
        "Handshake: Producing and sending security token."
      );
      let command_msg = Msg::from_vec(token_to_send);
      send_handshake_command_frame_impl(handler, command_msg, operation_timeout).await?;
      // After sending, loop immediately to check the new state. The mechanism might
      // be complete now (e.g., client after sending INITIATE).
      continue;
    }

    // B. If no token to send, we must be waiting for the peer.
    tracing::trace!(
      sca_handle = handler.actor_handle,
      mechanism = handler.security_mechanism.name(),
      "Handshake: Waiting to read security token from peer."
    );
    let received_msg = read_handshake_command_frame_impl(handler, operation_timeout).await?;
    let token_data = received_msg.data().unwrap_or(&[]);
    tracing::debug!(
      sca_handle = handler.actor_handle,
      token_len = token_data.len(),
      mechanism = handler.security_mechanism.name(),
      "Handshake: Read and processing peer's security token."
    );

    // C. Process the received token and determine the next action.
    let action = handler.security_mechanism.process_token(token_data)?;

    match action {
      ProcessTokenAction::ContinueWaiting => {
        // The mechanism consumed the token and is still waiting for more from the peer.
        // The loop will repeat, which will lead back to reading from the network.
      }
      ProcessTokenAction::ProduceAndSend => {
        // The mechanism has a reply ready. Send it immediately.
        if let Some(token_to_send) = handler.security_mechanism.produce_token()? {
          tracing::debug!(
            sca_handle = handler.actor_handle,
            token_len = token_to_send.len(),
            "Handshake: Immediately producing and sending reply token."
          );
          let command_msg = Msg::from_vec(token_to_send);
          send_handshake_command_frame_impl(handler, command_msg, operation_timeout).await?;
          // After sending the reply, loop again to check the new state.
        } else {
          // This indicates a logic error within the mechanism's implementation.
          return Err(ZmqError::Internal(
            "Mechanism requested ProduceAndSend but then produced no token.".to_string(),
          ));
        }
      }
      ProcessTokenAction::HandshakeComplete => {
        // The mechanism is now complete after processing the peer's token.
        // The loop will repeat and the `is_complete()` check at the top will catch this
        // and trigger the transition to the next phase.
      }
    }
    // After any action, we loop to re-evaluate the state machine.
  }
}

async fn perform_ready_exchange_step_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  operation_timeout: Duration,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  let mut final_peer_id_from_ready: Option<Blob> = None;
  if !handler.is_server {
    let mut props = HashMap::new();
    props.insert(
      "Socket-Type".to_string(),
      handler.config.socket_type_name.as_bytes().to_vec(),
    );
    if let Some(id) = &handler.config.routing_id {
      if !id.is_empty() && id.len() <= 255 {
        props.insert("Identity".to_string(), id.to_vec());
      }
    }
    send_handshake_command_frame_impl(handler, ZmtpReady::create_msg(props), operation_timeout)
      .await?;
    tracing::debug!(sca_handle = handler.actor_handle, "Client sent READY.");
  }
  let recv_ready_msg = read_handshake_command_frame_impl(handler, operation_timeout).await?;
  let peer_ready_data = match ZmtpCommand::parse(&recv_ready_msg) {
    Some(ZmtpCommand::Ready(data)) => data,
    _ => return Err(ZmqError::ProtocolViolation("Expected READY".into())),
  };
  tracing::debug!(sca_handle = handler.actor_handle, "Recvd peer READY.");
  if let Some(id_v) = peer_ready_data.properties.get("Identity") {
    if !id_v.is_empty() && id_v.len() <= 255 {
      final_peer_id_from_ready = Some(id_v.clone().into());
    }
  }

  if handler.is_server {
    let mut props = HashMap::new();
    props.insert(
      "Socket-Type".to_string(),
      handler.config.socket_type_name.as_bytes().to_vec(),
    );
    send_handshake_command_frame_impl(handler, ZmtpReady::create_msg(props), operation_timeout)
      .await?;
    tracing::debug!(sca_handle = handler.actor_handle, "Server sent READY.");
  }
  handler.handshake_state.sub_phase = HandshakeSubPhaseX::Done;
  Ok(final_peer_id_from_ready.map_or(
    ZmtpHandshakeProgressX::HandshakeComplete,
    ZmtpHandshakeProgressX::IdentityReady,
  ))
}
