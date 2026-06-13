#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::transport::{ZmtpReadHalf, ZmtpStdStream, ZmtpWriteHalf};

use bytes::BytesMut;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) async fn read_data_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<Option<Msg>, ZmqError> {
  const MAX_BUFFER_SIZE: usize = 16 * 1024 * 1024;

  let reader = handler.read_half.as_mut().ok_or_else(|| {
    tracing::error!(
      sca_handle = handler.actor_handle,
      "Read half is None during read_data_frame_impl."
    );
    ZmqError::Internal("Read half unavailable for data frame reading".into())
  })?;

  loop {
    if handler.network_read_buffer.len() > MAX_BUFFER_SIZE {
      return Err(ZmqError::ResourceLimitReached);
    }

    match handler.framer.try_read_msg(&mut handler.network_read_buffer) {
      Ok(Some(msg)) => {
        handler.heartbeat_state.record_activity();
        if handler.network_read_buffer.is_empty() && handler.network_read_buffer.capacity() > 65536
        {
          handler.network_read_buffer = BytesMut::with_capacity(16384);
        }
        return Ok(Some(msg));
      }
      Ok(None) => {}
      Err(e) => return Err(e),
    }

    let bytes_read = reader
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
  }
}

/// Greedy inbound read: one async kernel read + synchronous drain of all complete frames.
///
/// `reader` is the split read half passed in by the actor — the handler's own `read_half`
/// field is `None` while the actor holds the local variable.
pub(crate) async fn read_data_frames_batch_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  reader: &mut S::ReadHalf,
) -> Result<FrameBatch, ZmqError> {
  let max_count = handler.config.rcvbatch_count;
  let max_bytes = handler.config.rcvbatch_bytes;
  let mut batch: FrameBatch = FrameBatch::new();
  let mut total_bytes: usize = 0;

  // Drain any frames already assembled in the buffer from a prior partial read.
  loop {
    if batch.len() >= max_count || total_bytes >= max_bytes {
      break;
    }
    match handler.framer.try_read_msg(&mut handler.network_read_buffer)? {
      Some(msg) => {
        total_bytes += msg.size();
        batch.push(msg);
      }
      None => break,
    }
  }
  if !batch.is_empty() {
    return Ok(batch);
  }

  // Zero-copy fast path: parse directly from kernel ring-buffer chunks.
  // Active on io-uring connections with NULL/PLAIN framer (no encryption).
  // Not gated on network_read_buffer.is_empty() — partial bytes are prepended inline.
  #[cfg(feature = "io-uring")]
  if handler.framer.is_passthrough() {
    if handler.active_lease.is_none() {
      if let Some(stolen) = reader.steal_current_bytes() {
        handler.active_lease = Some(stolen);
      }
    }

    loop {
      if handler.active_lease.is_none() {
        match reader.try_recv_bytes() {
          None => break,
          Some(Ok(bytes)) if bytes.is_empty() => return Err(ZmqError::ConnectionClosed),
          Some(Ok(bytes)) => handler.active_lease = Some(bytes),
          Some(Err(_)) => return Err(ZmqError::ConnectionClosed),
        }
      }

      let mut bytes = handler.active_lease.take().unwrap();

      // If a partial frame is trapped in network_read_buffer from a prior straddle,
      // complete it by copying only the frame's missing remainder out of the new
      // chunk. The rest of the chunk stays a zero-copy slice (active_lease) — the
      // old approach copied the entire chunk into the buffer on every straddle.
      if !handler.network_read_buffer.is_empty() {
        // Top up the header first (≤ 9 bytes total) so the frame length is known.
        let total_len = loop {
          if let Some(total) = handler
            .zmtp_manual_parser
            .peek_frame_len(&handler.network_read_buffer)?
          {
            break Some(total);
          }
          if bytes.is_empty() {
            break None;
          }
          handler.network_read_buffer.extend_from_slice(&bytes[..1]);
          bytes = bytes.slice(1..);
        };

        let Some(total_len) = total_len else {
          // Chunk exhausted while completing the header; wait for more data.
          break;
        };

        let needed = total_len.saturating_sub(handler.network_read_buffer.len());
        if needed > bytes.len() {
          // The whole chunk belongs to this frame: the frame's bytes must end up
          // contiguous, so this copy is unavoidable (and copies frame bytes only).
          handler.network_read_buffer.extend_from_slice(&bytes);
          break;
        }

        handler.network_read_buffer.extend_from_slice(&bytes[..needed]);
        let rest = bytes.slice(needed..);
        if !rest.is_empty() {
          handler.active_lease = Some(rest);
        }

        // split() hands off the completed frame without copying.
        let frame_bytes = handler.network_read_buffer.split().freeze();
        match handler.zmtp_manual_parser.decode_frame_from_bytes(&frame_bytes)? {
          Some((msg, consumed)) => {
            debug_assert_eq!(consumed, frame_bytes.len(), "straddle frame length mismatch");
            handler.heartbeat_state.record_activity();
            total_bytes += msg.size();
            batch.push(msg);
            if batch.len() >= max_count || total_bytes >= max_bytes {
              return Ok(batch);
            }
            continue;
          }
          None => {
            // Unreachable: frame_bytes holds exactly total_len bytes. Restore the
            // partial frame defensively rather than lose data.
            handler.network_read_buffer.extend_from_slice(&frame_bytes);
            break;
          }
        }
      }

      // Standard zero-copy parse from the raw lease.
      match handler.zmtp_manual_parser.decode_frame_from_bytes(&bytes)? {
        Some((msg, consumed)) => {
          bytes = bytes.slice(consumed..);
          if !bytes.is_empty() {
            handler.active_lease = Some(bytes);
          }
          handler.heartbeat_state.record_activity();
          total_bytes += msg.size();
          batch.push(msg);
          if batch.len() >= max_count || total_bytes >= max_bytes {
            return Ok(batch);
          }
        }
        None => {
          handler.network_read_buffer.extend_from_slice(&bytes);
          break;
        }
      }
    }

    if !batch.is_empty() {
      return Ok(batch);
    }
  }

  // Slow path: one async yield to wait for data, then greedy synchronous drain.
  if handler.network_read_buffer.len() > 16 * 1024 * 1024 {
    return Err(ZmqError::ResourceLimitReached);
  }
  handler.network_read_buffer.reserve(65536);
  let bytes_read = reader
    .read_buf(&mut handler.network_read_buffer)
    .await
    .map_err(|e| ZmqError::from_io_endpoint(e, "data batch read"))?;
  if bytes_read == 0 {
    tracing::debug!(
      sca_handle = handler.actor_handle,
      "Connection closed by peer (EOF in batch read)."
    );
    return Err(ZmqError::ConnectionClosed);
  }
  handler.heartbeat_state.record_activity();

  // Greedy drain: pull more bytes synchronously without yielding, capped at max_bytes.
  // TCP/IPC call try_read(); InprocStream returns WouldBlock immediately (no regression).
  let mut greedy_buf = [0u8; 65536];
  while handler.network_read_buffer.len() < max_bytes {
    match reader.try_read_chunk(&mut greedy_buf) {
      Ok(0) => {
        tracing::debug!(sca_handle = handler.actor_handle, "Connection closed by peer (EOF in greedy read).");
        return Err(ZmqError::ConnectionClosed);
      }
      Ok(n) => handler.network_read_buffer.extend_from_slice(&greedy_buf[..n]),
      Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
      Err(e) => return Err(ZmqError::from_io_endpoint(e, "greedy batch read")),
    }
  }

  // Drain all complete frames from the accumulated bytes.
  loop {
    if batch.len() >= max_count || total_bytes >= max_bytes {
      break;
    }
    match handler.framer.try_read_msg(&mut handler.network_read_buffer)? {
      Some(msg) => {
        total_bytes += msg.size();
        batch.push(msg);
      }
      None => break,
    }
  }

  if handler.network_read_buffer.is_empty() && handler.network_read_buffer.capacity() > 65536 {
    handler.network_read_buffer = BytesMut::with_capacity(16384);
  }

  Ok(batch)
}

pub(crate) fn frame_single_msg_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msg: Msg,
) -> Result<bytes::Bytes, crate::ZmqError> {
  let mut fb = FrameBatch::new();
  fb.push(msg);
  handler.framer.write_msg_multipart(fb)
}

pub(crate) async fn write_data_msgs_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msgs: FrameBatch,
) -> Result<(), ZmqError> {
  if msgs.is_empty() {
    return Ok(());
  }

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  // Extract capability flag without holding a mutable borrow into the match arms.
  let use_owned = handler
    .write_half
    .as_ref()
    .map(|w| w.supports_owned_write())
    .ok_or_else(|| ZmqError::Internal("Write half unavailable for writing data message".into()))?;

  if use_owned {
    let batch = [msgs];
    let bufs = handler.framer.frame_vectored(&batch)?;
    let writer = handler.write_half.as_mut().unwrap();
    tokio::time::timeout(operation_timeout, writer.write_owned(bufs))
      .await
      .map_err(|_| ZmqError::Timeout)?
      .map_err(|e| ZmqError::from_io_endpoint(e, "data write"))?;
    handler.heartbeat_state.record_activity();
    return Ok(());
  }

  // TCP/IPC/inproc: cork + write_all path (cancel-safe via EgressBuffer in the hot path;
  // here it's called for control messages where cancellation is not a concern).
  let socket_type = handler.config.socket_type_name.as_str();
  let is_latency_pattern = matches!(socket_type, "REQ" | "REP" | "DEALER" | "ROUTER");
  let should_dynamic_cork = is_latency_pattern && msgs.len() > 1;

  let wire_bytes_to_send = handler.framer.write_msg_multipart(msgs)?;

  let writer = handler
    .write_half
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Write half unavailable for writing data message".into()))?;

  #[cfg(target_os = "linux")]
  {
    if should_dynamic_cork {
      if let Some(ci) = handler.cork_info.as_mut() {
        ci.apply_cork_state(true, handler.actor_handle).await;
      }
    }
  }

  let write_result =
    tokio::time::timeout(operation_timeout, writer.write_all(&wire_bytes_to_send))
      .await
      .map_err(|_| ZmqError::Timeout)?
      .map_err(|e| ZmqError::from_io_endpoint(e, "data write"));

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

pub(crate) async fn write_data_batch_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  batch: &[FrameBatch],
) -> Result<(), ZmqError> {
  if batch.is_empty() {
    return Ok(());
  }

  let writer = handler
    .write_half
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Write half unavailable for batch write".into()))?;

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

  let write_result = tokio::time::timeout(operation_timeout, writer.write_all(&wire_bytes))
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

pub(crate) async fn write_data_msg_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msg: Msg,
  _is_first_part_of_logical_zmq_msg: bool,
) -> Result<bool, ZmqError> {
  let was_last_part = !msg.is_more();
  let mut fb = FrameBatch::new();
  fb.push(msg);
  write_data_msgs_impl(handler, fb).await?;
  Ok(was_last_part)
}
