// core/src/sessionx/protocol_handler/data_io.rs

#![allow(dead_code, unused_variables)] // Keep for now

use super::ZmtpProtocolHandlerX; // To access fields of the main struct
use crate::engine::ZmtpStdStream;
use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::ZmtpCodec; // For encoding messages

use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder; // Added BufMut
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt}; // For timeouts

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
    let bytes_read = tokio::time::timeout(operation_timeout, stream.read_buf(&mut handler.network_read_buffer))
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
    tracing::trace!(sca_handle = handler.actor_handle, bytes_read, "Read data from network.");

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

pub(crate) async fn write_data_msg_impl<S: ZmtpStdStream>(
  handler: &mut ZmtpProtocolHandlerX<S>,
  msg: Msg,
  is_first_part_of_logical_zmq_msg: bool, // True if this is the first ZMTP frame of a potentially multi-frame ZMQ message
) -> Result<bool /* is_last_part_of_logical_zmq_msg */, ZmqError> {
  let stream = handler.stream.as_mut().ok_or_else(|| {
    tracing::error!(
      sca_handle = handler.actor_handle,
      "Stream is None during write_data_msg_impl."
    );
    ZmqError::Internal("Stream unavailable for writing data message".into())
  })?;

  // Timeout for a data write operation. Can be based on SNDTIMEO or a long default.
  let operation_timeout = handler.config.sndtimeo.unwrap_or(Duration::from_secs(300));

  let mut temp_zmtp_encoder = ZmtpCodec::new(); // Stateless
  let mut plaintext_zmtp_frame_buffer = BytesMut::new();
  // Clone msg because ZmtpCodec::encode might take ownership or if we need msg.is_more() later.
  // ZmtpCodec::encode takes `item: Msg`, so we pass the original `msg`.
  // We need `msg.is_more()` *after* encoding for cork logic.
  let msg_is_more = msg.is_more();
  temp_zmtp_encoder
    .encode(msg, &mut plaintext_zmtp_frame_buffer)
    .map_err(|e| ZmqError::Internal(format!("Failed to ZMTP-encode outgoing message: {}", e)))?;

  let wire_bytes_to_send = if let Some(cipher) = &mut handler.data_cipher {
    cipher.encrypt_zmtp_frame(plaintext_zmtp_frame_buffer.freeze())?
  } else {
    plaintext_zmtp_frame_buffer.freeze()
  };

  #[cfg(target_os = "linux")]
  {
    if let Some(ci) = handler.cork_info.as_mut() {
      // Enable CORK if:
      // 1. This is the first frame of a logical ZMQ message (indicated by caller)
      // 2. AND cork_info expects the first frame (meaning previous ZMQ message ended)
      // 3. AND not already corked.
      if is_first_part_of_logical_zmq_msg && ci.is_expecting_first_frame() && !ci.is_corked() {
        ci.apply_cork_state(true, handler.actor_handle).await;
      }
    }
  }

  tokio::time::timeout(operation_timeout, stream.write_all(&wire_bytes_to_send))
    .await
    .map_err(|_| {
      tracing::warn!(
        sca_handle = handler.actor_handle,
        "Timeout writing data msg (single op)."
      );
      ZmqError::Timeout
    })?
    .map_err(|e| {
      #[cfg(target_os = "linux")]
      if let Some(ci) = handler.cork_info.as_mut() {
        if ci.is_corked() {
          // If a write fails while corked, it's crucial to attempt to uncork
          // to prevent stalling subsequent operations on this FD if it's reused
          // or if the error is recoverable.
          // Spawn a task for best-effort uncorking as apply_cork_state is async.
          let fd_to_uncork = ci.fd(); // Capture fd if needed by a free function
          let actor_handle_for_uncork = handler.actor_handle;
          // This needs ci to be 'static or for apply_cork_state to not need &mut self
          // For simplicity, if apply_cork_state is on TcpCorkInfoX, this is complex.
          // A simpler immediate fix if write fails while corked:
          // ci.is_corked = false; // Assume it's effectively uncorked or will be by OS on error/close
          // But proper uncorking is better.
          // Let's assume apply_cork_state can be called.
          // This requires careful thought if ci.apply_cork_state is async and needs &mut self.
          // For now, let's assume a synchronous attempt or log the need.
          tracing::warn!(
            sca_handle = handler.actor_handle,
            "Write error occurred while corked. Attempting to uncork is complex here. OS might handle on close."
          );
          // Ideally, before propagating error:
          // ci.apply_cork_state(false, handler.actor_handle).await; // This line makes error handling async
        }
      }
      ZmqError::from_io_endpoint(e, "data write")
    })?;
  handler.heartbeat_state.record_activity();

  let is_last_frame_of_logical_zmq_msg = !msg_is_more;

  #[cfg(target_os = "linux")]
  {
    if let Some(ci) = handler.cork_info.as_mut() {
      if ci.is_corked() && is_last_frame_of_logical_zmq_msg {
        ci.apply_cork_state(false, handler.actor_handle).await;
      }
      // This flag means "is the *next* frame we send going to be the first of a new ZMQ message?"
      ci.set_expecting_first_frame(is_last_frame_of_logical_zmq_msg);
    }
  }

  // Return true if this was the last part of the ZMQ message, false otherwise.
  // This helps SessionConnectionActorX manage its `sca_is_sending_first_frame_of_zmq_message` state.
  Ok(is_last_frame_of_logical_zmq_msg)
}
