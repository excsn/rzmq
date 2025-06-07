// core/src/sessionx/protocol_handler/handshake.rs

#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::transport::ZmtpStdStream;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::command::{ZmtpCommand, ZmtpReady};
use crate::protocol::zmtp::greeting::{ZmtpGreeting, GREETING_LENGTH};
use crate::protocol::zmtp::ZmtpCodec;
#[cfg(feature = "noise_xx")]
use crate::security::NoiseXxMechanism;
#[cfg(feature = "plain")]
use crate::security::PlainMechanism;
use crate::security::{negotiate_security_mechanism, NullMechanism, PlainMechanism};
use crate::Blob;
use crate::MsgFlags;

use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // Added Instant

use crate::sessionx::types::{HandshakeSubPhaseX, ZmtpHandshakeProgressX};

pub(crate) async fn advance_handshake_step_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  // <<< MODIFIED [Use a fixed per-operation timeout for handshake steps] >>>
  // The overall handshake timeout (handler.config.handshake_timeout)
  // will be enforced by SessionConnectionActorX.
  let operation_timeout = Duration::from_secs(15); // Default for individual send/recv during handshake
                                                   // <<< MODIFIED END >>>

  match handler.handshake_state.sub_phase {
    HandshakeSubPhaseX::GreetingExchange => exchange_greetings_impl(handler, operation_timeout).await,
    HandshakeSubPhaseX::SecurityHandshake => perform_security_handshake_step_impl(handler, operation_timeout).await,
    HandshakeSubPhaseX::ReadyExchange => perform_ready_exchange_step_impl(handler, operation_timeout).await,
    HandshakeSubPhaseX::Done => Ok(ZmtpHandshakeProgressX::HandshakeComplete),
  }
}

async fn read_handshake_command_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  timeout_duration: Duration,
) -> Result<Msg, ZmqError> {
  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for reading handshake command".into()))?;
  loop {
    if !handler.network_read_buffer.is_empty() {
      match handler
        .zmtp_manual_parser
        .decode_from_buffer(&mut handler.network_read_buffer)
      {
        Ok(Some(msg)) => {
          handler.heartbeat_state.record_activity();
          if !msg.is_command() {
            return Err(ZmqError::ProtocolViolation(
              "Expected COMMAND frame in handshake".into(),
            ));
          }
          return Ok(msg);
        }
        Ok(None) => {}
        Err(e) => return Err(e),
      }
    }
    let bytes_read = tokio::time::timeout(timeout_duration, stream.read_buf(&mut handler.network_read_buffer))
      .await
      .map_err(|_| ZmqError::Timeout)?
      .map_err(|e| ZmqError::from_io_endpoint(e, "handshake command read"))?;
    if bytes_read == 0 {
      return Err(ZmqError::ConnectionClosed);
    }
    handler.heartbeat_state.record_activity();
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
  tokio::time::timeout(operation_timeout, stream.write_all(&greeting_buffer_to_send))
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
    let br = tokio::time::timeout(remaining_time, stream.read_buf(&mut handler.network_read_buffer))
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

fn determine_own_greeting_mechanism_impl<S: ZmtpStdStream>(handler: &ZmtpProtocolHandlerX<S>) -> &'static [u8; 20] {
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
  if handler.config.use_plain {
    PlainMechanism::NAME_BYTES
  } else {
    NullMechanism::NAME_BYTES
  }
}

async fn perform_security_handshake_step_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  operation_timeout: Duration,
) -> Result<ZmtpHandshakeProgressX, ZmqError> {
  let peer_greeting = handler
    .pending_peer_greeting
    .as_ref()
    .ok_or_else(|| ZmqError::Internal("No peer g".into()))?;
  if handler.security_mechanism.name() == NullMechanism::NAME {
    handler.security_mechanism =
      negotiate_security_mechanism(handler.is_server, &handler.config, peer_greeting, handler.actor_handle)?;
    tracing::debug!(
      sca_handle = handler.actor_handle,
      mechanism = handler.security_mechanism.name(),
      "Sec mech negotiated."
    );
  }

  if let Some(token_to_send_vec) = handler.security_mechanism.produce_token()? {
    // <<< MODIFIED [Get length before move] >>>
    let token_len = token_to_send_vec.len();
    let command_msg = Msg::from_vec(token_to_send_vec);
    // <<< MODIFIED END >>>
    send_handshake_command_frame_impl(handler, command_msg, operation_timeout).await?;
    tracing::trace!(
      sca_handle = handler.actor_handle,
      mechanism = handler.security_mechanism.name(),
      token_size = token_len,
      "Sent security token."
    );
  }

  if !handler.security_mechanism.is_complete() && !handler.security_mechanism.is_error() {
    let received_msg = read_handshake_command_frame_impl(handler, operation_timeout).await?;
    let token_data = received_msg.data().unwrap_or(&[]);
    tracing::trace!(
      sca_handle = handler.actor_handle,
      mechanism = handler.security_mechanism.name(),
      token_size = token_data.len(),
      "Recvd security token."
    );
    if let Err(e) = handler.security_mechanism.process_token(token_data) {
      handler
        .security_mechanism
        .set_error(format!("Token process err: {}", e));
    }
  }

  if handler.security_mechanism.is_complete() {
    let mech_box = std::mem::replace(&mut handler.security_mechanism, Box::new(NullMechanism));
    let (cipher, id_opt) = mech_box.into_data_cipher_parts()?;
    handler.data_cipher = Some(cipher);
    handler.handshake_state.sub_phase = HandshakeSubPhaseX::ReadyExchange;
    return Ok(id_opt.map_or(ZmtpHandshakeProgressX::InProgress, |v| {
      ZmtpHandshakeProgressX::IdentityReady(v.into())
    }));
  } else if handler.security_mechanism.is_error() {
    return Err(ZmqError::SecurityError(
      handler
        .security_mechanism
        .error_reason()
        .unwrap_or("Unknown sec err")
        .to_string(),
    ));
  }
  Ok(ZmtpHandshakeProgressX::InProgress)
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
    send_handshake_command_frame_impl(handler, ZmtpReady::create_msg(props), operation_timeout).await?;
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
    send_handshake_command_frame_impl(handler, ZmtpReady::create_msg(props), operation_timeout).await?;
    tracing::debug!(sca_handle = handler.actor_handle, "Server sent READY.");
  }
  handler.handshake_state.sub_phase = HandshakeSubPhaseX::Done;
  Ok(final_peer_id_from_ready.map_or(
    ZmtpHandshakeProgressX::HandshakeComplete,
    ZmtpHandshakeProgressX::IdentityReady,
  ))
}
