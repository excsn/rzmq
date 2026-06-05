#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::transport::ZmtpStdStream;

use bytes::BytesMut;
use std::time::Duration;
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
        if handler.network_read_buffer.is_empty() && handler.network_read_buffer.capacity() > 65536
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

/// Reads from the network and greedily extracts all complete ZMTP messages from the buffer,
/// up to `rcvbatch_count` messages or `rcvbatch_bytes` total payload bytes.
/// Returns an empty `Vec` when a read succeeded but no complete frame arrived yet.
/// Returns `Err(ConnectionClosed)` on EOF.
pub(crate) async fn read_data_frames_batch_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<Vec<Msg>, ZmqError> {
  let max_count = handler.config.rcvbatch_count;
  let max_bytes = handler.config.rcvbatch_bytes;
  let mut batch: Vec<Msg> = Vec::new();
  let mut total_bytes: usize = 0;

  // Fast path: drain frames already in the buffer (leftover from a prior read)
  loop {
    if batch.len() >= max_count || total_bytes >= max_bytes {
      break;
    }
    match handler.framer.try_read_msg(&mut handler.network_read_buffer)? {
      Some(msg) => { total_bytes += msg.size(); batch.push(msg); }
      None => break,
    }
  }
  if !batch.is_empty() {
    return Ok(batch);
  }

  // Slow path: buffer empty — do one async kernel read
  if handler.network_read_buffer.len() > 16 * 1024 * 1024 {
    return Err(ZmqError::ResourceLimitReached);
  }
  let stream = handler.stream.as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for batch data read".into()))?;
  let bytes_read = stream.read_buf(&mut handler.network_read_buffer).await
    .map_err(|e| ZmqError::from_io_endpoint(e, "data batch read"))?;
  if bytes_read == 0 {
    tracing::debug!(sca_handle = handler.actor_handle, "Connection closed by peer (EOF in batch read).");
    return Err(ZmqError::ConnectionClosed);
  }
  handler.heartbeat_state.record_activity();

  // Synchronously drain all complete frames from what just arrived
  loop {
    if batch.len() >= max_count || total_bytes >= max_bytes {
      break;
    }
    match handler.framer.try_read_msg(&mut handler.network_read_buffer)? {
      Some(msg) => { total_bytes += msg.size(); batch.push(msg); }
      None => break,
    }
  }

  if handler.network_read_buffer.is_empty() && handler.network_read_buffer.capacity() > 65536 {
    handler.network_read_buffer = BytesMut::with_capacity(16384);
  }

  Ok(batch)
}

/// Batches, frames, encrypts, and writes a full logical ZMQ message (one or more `Msg` parts).
/// This is the optimal path for multipart messages.
pub(crate) async fn write_data_msgs_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msgs: Vec<Msg>,
) -> Result<(), ZmqError> {
  if msgs.is_empty() {
    return Ok(());
  }

  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for writing data message".into()))?;

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  // Determine if this is a multipart message on a latency socket requiring dynamic corking
  let socket_type = handler.config.socket_type_name.as_str();
  let is_latency_pattern = matches!(socket_type, "REQ" | "REP" | "DEALER" | "ROUTER");
  let should_dynamic_cork = is_latency_pattern && msgs.len() > 1;

  // 1. Delegate framing and encryption entirely to the framer.
  let wire_bytes_to_send = handler.framer.write_msg_multipart(msgs)?;

  // 2. Only dynamic-cork if it is a multipart message on a latency socket
  #[cfg(target_os = "linux")]
  {
    if should_dynamic_cork {
      if let Some(ci) = handler.cork_info.as_mut() {
        ci.apply_cork_state(true, handler.actor_handle).await;
      }
    }
  }

  let write_result = tokio::time::timeout(operation_timeout, stream.write_all(&wire_bytes_to_send))
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "data write"));

  handler.heartbeat_state.record_activity();

  // 3. Only uncork/flush if we dynamically applied the cork
  #[cfg(target_os = "linux")]
  {
    if should_dynamic_cork {
      if let Some(ci) = handler.cork_info.as_mut() {
        if ci.is_corked() {
          ci.apply_cork_state(false, handler.actor_handle).await;
        }
      }
    }
  }

  write_result
}

/// Frames, coalesces, and writes an entire batch of logical ZMQ messages in a single
/// network operation. Applies dynamic TCP corking around the write when appropriate.
pub(crate) async fn write_data_batch_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  batch: &[Vec<Msg>],
) -> Result<(), ZmqError> {
  if batch.is_empty() {
    return Ok(());
  }

  let stream = handler
    .stream
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Stream unavailable for batch write".into()))?;

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  let socket_type = handler.config.socket_type_name.as_str();
  let is_latency_pattern = matches!(socket_type, "REQ" | "REP" | "DEALER" | "ROUTER");
  let should_dynamic_cork =
    is_latency_pattern && (batch.len() > 1 || batch.iter().any(|msgs| msgs.len() > 1));

  let wire_bytes = handler.framer.write_msg_batch(batch)?;

  #[cfg(target_os = "linux")]
  {
    if should_dynamic_cork {
      if let Some(ci) = handler.cork_info.as_mut() {
        ci.apply_cork_state(true, handler.actor_handle).await;
      }
    }
  }

  let write_result = tokio::time::timeout(operation_timeout, stream.write_all(&wire_bytes))
    .await
    .map_err(|_| ZmqError::Timeout)?
    .map_err(|e| ZmqError::from_io_endpoint(e, "data batch write"));

  handler.heartbeat_state.record_activity();

  #[cfg(target_os = "linux")]
  {
    if should_dynamic_cork {
      if let Some(ci) = handler.cork_info.as_mut() {
        if ci.is_corked() {
          ci.apply_cork_state(false, handler.actor_handle).await;
        }
      }
    }
  }

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
