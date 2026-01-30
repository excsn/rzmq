#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::transport::ZmtpStdStream;

use std::time::Duration;
use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn read_data_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<Option<Msg>, ZmqError> {
  const MAX_BUFFER_SIZE: usize = 16 * 1024 * 1024; // 16MB limit to prevent unbounded growth

  let stream = handler.stream.as_mut().ok_or_else(|| {
    tracing::error!(
      sca_handle = handler.actor_handle,
      "Stream is None during read_data_frame_impl."
    );
    ZmqError::Internal("Stream unavailable for data frame reading".into())
  })?;

  // Do NOT use RCVTIMEO or arbitrary timeouts for the low-level stream read.
  // RCVTIMEO is a user-facing API timeout for socket.recv() operations, not for the underlying TCP stream.
  // Connection liveness is already managed by the actor's select! loop via:
  //   - ZMTP Heartbeats (PING/PONG mechanism)
  //   - Overall handshake timeout (for handshake phase)
  //   - System shutdown events
  // The stream read can safely block indefinitely. The actor's select! will cancel it if needed.

  loop {
    // Protect against buffer bloat from malicious partial frames or protocol violations
    if handler.network_read_buffer.len() > MAX_BUFFER_SIZE {
      tracing::error!(
        sca_handle = handler.actor_handle,
        buffer_size = handler.network_read_buffer.len(),
        "Network buffer exceeded maximum size. Possible attack or protocol violation."
      );
      return Err(ZmqError::ResourceLimitReached);
    }

    // 1. Attempt to frame and decrypt a message from the existing network buffer.
    // The framer encapsulates all logic for finding message boundaries.
    match handler
      .framer
      .try_read_msg(&mut handler.network_read_buffer)
    {
      Ok(Some(msg)) => {
        // Success: a complete message was framed and parsed.
        handler.heartbeat_state.record_activity();
        
        // If buffer has grown large and is mostly empty, shrink it
        if handler.network_read_buffer.capacity() > 65536
          && handler.network_read_buffer.len() < 8192
        {
          handler.network_read_buffer = BytesMut::with_capacity(16384);
        }

        return Ok(Some(msg));
      }
      Ok(None) => {
        // Not enough data in the buffer to form a complete message.
        // Proceed to read more from the network.
      }
      Err(e) => {
        // A fatal error occurred during framing or decryption.
        return Err(e);
      }
    }

    // 2. If no message could be framed, read more data from the network.
    // `read_buf` appends to the existing buffer.
    // We await directly on read_buf. The actor's select! loop handles
    // cancellation if Heartbeats fail or shutdown is requested.
    let bytes_read = stream
      .read_buf(&mut handler.network_read_buffer)
      .await
      .map_err(|e| ZmqError::from_io_endpoint(e, "data read"))?;

    if bytes_read == 0 {
      tracing::debug!(
        sca_handle = handler.actor_handle,
        "Connection closed by peer (EOF in data phase)."
      );
      return Err(ZmqError::ConnectionClosed);
    }

    handler.heartbeat_state.record_activity();
    tracing::trace!(
      sca_handle = handler.actor_handle,
      bytes_read,
      "Read data from network."
    );

    // After reading, the loop will repeat, giving the framer another chance
    // to process the (now larger) network_read_buffer.
  }
}

/// Batches, frames, encrypts, and writes a full logical ZMQ message (one or more `Msg` parts).
/// This is the optimal path for multipart messages.
/// Batches, frames, encrypts, and writes a full logical ZMQ message (one or more `Msg` parts).
pub(crate) async fn write_data_msgs_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msgs: Vec<Msg>, // Takes ownership of the Vec
) -> Result<(), ZmqError> {
  if msgs.is_empty() {
    return Ok(());
  }

  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for writing data message".into()))?;

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  // 1. Delegate framing and encryption entirely to the framer.
  let wire_bytes_to_send = handler.framer.write_msg_multipart(msgs)?;

  // 2. Apply CORK, write the entire buffer, and then uncork.
  #[cfg(target_os = "linux")]
  {
    if let Some(ci) = handler.cork_info.as_mut() {
      ci.apply_cork_state(true, handler.actor_handle).await;
    }
  }

  let write_result = tokio::time::timeout(operation_timeout, stream.write_all(&wire_bytes_to_send))
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "data write"));

  handler.heartbeat_state.record_activity();

  // CRUCIAL: Uncork after the write operation, regardless of success or failure.
  #[cfg(target_os = "linux")]
  {
    if let Some(ci) = handler.cork_info.as_mut() {
      if ci.is_corked() {
        ci.apply_cork_state(false, handler.actor_handle).await;
      }
      ci.set_expecting_first_frame(true);
    }
  }

  // Finally, return the result of the write operation.
  write_result
}

/// A wrapper for sending a single message part, which is now just a special case of sending a multipart message.
pub(crate) async fn write_data_msg_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msg: Msg, // Takes ownership of the Msg
  _is_first_part_of_logical_zmq_msg: bool,
) -> Result<bool /* was_last_part_of_logical_zmq_msg */, ZmqError> {
  let was_last_part = !msg.is_more();

  // Simply call the multipart version with a single-element Vec.
  write_data_msgs_impl(handler, vec![msg]).await?;

  Ok(was_last_part)
}
