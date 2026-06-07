#![allow(dead_code, unused_variables)]

use super::ZmtpProtocolHandlerX;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::transport::{ZmtpReadHalf, ZmtpStdStream};

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

  // Zero-copy fast path: consume kernel ring buffer leases directly.
  // Active only on io-uring connections with no encryption (NULL/PLAIN framer).
  #[cfg(feature = "io-uring")]
  if handler.network_read_buffer.is_empty()
    && handler.config.use_recv_multishot
    && handler.framer.is_passthrough()
  {
    // Steal any partially-consumed lease from the handshake→data transition (first call only).
    if handler.active_lease.is_none() {
      if let Some(stolen) = reader.steal_current_lease() {
        handler.active_lease = Some(stolen);
      }
    }

    loop {
      if handler.active_lease.is_none() {
        let maybe = reader.try_recv_lease();
        match maybe {
          None => break,
          Some(Ok(lease)) if lease.len == 0 => return Err(ZmqError::ConnectionClosed),
          Some(Ok(lease)) => {
            handler.active_lease = Some((lease, 0));
          }
          Some(Err(_)) => return Err(ZmqError::ConnectionClosed),
        }
      }

      // SAFETY: ptr is stable kernel ring memory, exclusively owned by this lease.
      let (lease, mut offset) = handler.active_lease.take().unwrap();
      let remaining =
        unsafe { std::slice::from_raw_parts(lease.ptr.add(offset), lease.len - offset) };
      match handler.zmtp_manual_parser.decode_frame_from_slice(remaining)? {
        Some((msg, consumed)) => {
          offset += consumed;
          if offset < lease.len {
            handler.active_lease = Some((lease, offset));
          } else {
            // lease drops here → RecycleRecvBuffer sent
            reader.notify_lease_consumed();
          }
          handler.heartbeat_state.record_activity();
          let msg_size = msg.size();
          batch.push(msg);
          total_bytes += msg_size;
          if batch.len() >= max_count || total_bytes >= max_bytes {
            return Ok(batch);
          }
        }
        None => {
          // Frame straddles this lease boundary — copy remainder to network_read_buffer.
          handler.network_read_buffer.extend_from_slice(remaining);
          // lease drops here → RecycleRecvBuffer sent
          reader.notify_lease_consumed();
          break;
        }
      }
    }

    if !batch.is_empty() {
      return Ok(batch);
    }
    // Fall through to standard path
  }

  // Slow path: buffer empty — do one async kernel read
  if handler.network_read_buffer.len() > 16 * 1024 * 1024 {
    return Err(ZmqError::ResourceLimitReached);
  }
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

  // Synchronously drain all complete frames from what just arrived
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
  handler.framer.write_msg_multipart(vec![msg])
}

pub(crate) async fn write_data_msgs_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msgs: Vec<Msg>,
) -> Result<(), ZmqError> {
  if msgs.is_empty() {
    return Ok(());
  }

  let writer = handler
    .write_half
    .as_mut()
    .ok_or_else(|| ZmqError::Internal("Write half unavailable for writing data message".into()))?;

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  let socket_type = handler.config.socket_type_name.as_str();
  let is_latency_pattern = matches!(socket_type, "REQ" | "REP" | "DEALER" | "ROUTER");
  let should_dynamic_cork = is_latency_pattern && msgs.len() > 1;

  let wire_bytes_to_send = handler.framer.write_msg_multipart(msgs)?;

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
  batch: &[Vec<Msg>],
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
  write_data_msgs_impl(handler, vec![msg]).await?;
  Ok(was_last_part)
}
