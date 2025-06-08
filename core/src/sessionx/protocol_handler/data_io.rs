// core/src/sessionx/protocol_handler/data_io.rs

#![allow(dead_code, unused_variables)] // Keep for now

use super::ZmtpProtocolHandlerX; // To access fields of the main struct
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpCodec;
use crate::transport::ZmtpStdStream; // For encoding messages

use bytes::{BufMut, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Encoder; // Added BufMut // For timeouts

pub(crate) async fn read_data_frame_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
) -> Result<Option<Msg>, ZmqError> {
  let stream = handler.stream.as_mut().ok_or_else(|| {
    tracing::error!(
      sca_handle = handler.actor_handle,
      "Stream is None during read_data_frame_impl."
    );
    ZmqError::Internal("Stream unavailable for data frame reading".into())
  })?;

  // Timeout for a data read operation. Can be based on RCVTIMEO or a long default.
  // SessionConnectionActorX's select loop might have its own overall idle timeouts.
  let operation_timeout = handler.config.rcvtimeo.unwrap_or(Duration::from_secs(300)); // 5 minutes default

  loop {
    // Determine the buffer to parse from based on whether decryption is active.
    let (mut parse_attempt_occurred, parse_result) = {
      if handler.data_cipher.is_some() {
        // If decrypting, plaintext_feed_buffer is the target for the parser.
        // Only attempt parse if it has data.
        if !handler.plaintext_feed_buffer.is_empty() {
          (
            true,
            handler
              .zmtp_manual_parser
              .decode_from_buffer(&mut handler.plaintext_feed_buffer),
          )
        } else {
          (false, Ok(None)) // No data in plaintext buffer to parse yet
        }
      } else {
        // No decryption, parse directly from network_read_buffer.
        // Only attempt parse if it has data.
        if !handler.network_read_buffer.is_empty() {
          (
            true,
            handler
              .zmtp_manual_parser
              .decode_from_buffer(&mut handler.network_read_buffer),
          )
        } else {
          (false, Ok(None)) // No data in network buffer to parse yet
        }
      }
    };

    if parse_attempt_occurred {
      match parse_result {
        Ok(Some(msg)) => {
          handler.heartbeat_state.record_activity();
          return Ok(Some(msg));
        }
        Ok(None) => { /* Need more data in the respective buffer */ }
        Err(e) => return Err(e), // Parsing error
      }
    }

    // If here, either no parse attempt was made (buffers were empty) or parse attempt needed more data.
    // Read more data from the network into network_read_buffer.
    // Note: read_buf appends to network_read_buffer. If it's not empty from a previous partial
    // encrypted frame, new data will be added.
    let bytes_read = tokio::time::timeout(
      operation_timeout,
      stream.read_buf(&mut handler.network_read_buffer),
    )
    .await
    .map_err(|_| {
      tracing::warn!(
        sca_handle = handler.actor_handle,
        "Timeout reading data frame (single op)."
      );
      ZmqError::Timeout
    })?
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

    // Process newly read data: decrypt if necessary.
    if let Some(cipher) = &mut handler.data_cipher {
      // Decrypt loop for potentially multiple secure messages in one read.
      // This moves decrypted data from network_read_buffer to plaintext_feed_buffer.
      let mut made_progress_decrypting = false;
      while !handler.network_read_buffer.is_empty() {
        match cipher.decrypt_wire_data_to_zmtp_frame(&mut handler.network_read_buffer) {
          Ok(Some(plaintext_zmtp_bytes)) => {
            handler.plaintext_feed_buffer.put(plaintext_zmtp_bytes);
            made_progress_decrypting = true;
          }
          Ok(None) => break, // Need more data in network_read_buffer for current secure frame
          Err(e) => {
            tracing::error!(sca_handle = handler.actor_handle, error = %e, "Decryption failed.");
            return Err(e);
          }
        }
      }
      // If we decrypted something, loop again to try parsing from plaintext_feed_buffer.
      // If not, and network_read_buffer is now empty (meaning partial encrypted frame),
      // the outer loop will try to read more network data.
    } else {
      // No cipher: data in network_read_buffer is ready for parsing.
      // The loop will retry parsing directly from network_read_buffer.
    }
    // Loop again to attempt parsing (either from plaintext_feed_buffer or network_read_buffer)
  }
}

/// Batches, frames, encrypts, and writes a full logical ZMQ message (one or more `Msg` parts).
/// This is the optimal path for multipart messages.
pub(crate) async fn write_data_msgs_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  mut msgs: Vec<Msg>, // Takes ownership of the Vec
) -> Result<(), ZmqError> {
  // Pass a mutable slice of the Vec to the core implementation.
  write_message_slice_impl(handler, &mut msgs).await
}

/// A wrapper for sending a single message part, avoiding heap allocation for the `Vec`.
pub(crate) async fn write_data_msg_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  mut msg: Msg, // Takes ownership of the Msg
  _is_first_part_of_logical_zmq_msg: bool,
) -> Result<bool /* is_last_part_of_logical_zmq_msg */, ZmqError> {
  let msg_is_more = msg.is_more();

  // Create a temporary array on the stack and pass a mutable slice to the core implementation.
  // This avoids allocating a Vec on the heap.
  write_message_slice_impl(handler, &mut [msg]).await?;

  Ok(!msg_is_more)
}

/// Private helper that contains the core logic for batching, framing, and writing.
/// It takes a mutable slice `&mut [Msg]` to avoid allocations for the single-message case.
async fn write_message_slice_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msgs: &mut [Msg],
) -> Result<(), ZmqError> {
  if msgs.is_empty() {
    return Ok(());
  }

  let stream = handler.stream.as_mut().ok_or_else(|| {
    tracing::error!(
      sca_handle = handler.actor_handle,
      "Stream is None during write_message_slice_impl."
    );
    ZmqError::Internal("Stream unavailable for writing data message".into())
  })?;

  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  // 1. Synchronously encode all message parts from the slice into a single buffer.
  let mut plaintext_zmtp_frame_buffer = BytesMut::new();
  let mut temp_zmtp_encoder = ZmtpCodec::new();
  for msg in msgs {
    temp_zmtp_encoder.encode(msg.clone(), &mut plaintext_zmtp_frame_buffer)?;
  }

  // 2. Encrypt the entire batch of ZMTP frames if a cipher is active.
  let wire_bytes_to_send = if let Some(cipher) = &mut handler.data_cipher {
    cipher.encrypt_zmtp_frame(plaintext_zmtp_frame_buffer.freeze())?
  } else {
    plaintext_zmtp_frame_buffer.freeze()
  };

  // 3. Apply CORK, write the entire buffer, and then uncork.
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
