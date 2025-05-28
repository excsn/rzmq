// core/src/security/noise_stream.rs

#![cfg(feature = "noise_xx")] // Only compile if noise_xx feature is enabled

use bytes::{Buf, BufMut, BytesMut}; // BytesMut for internal buffering if needed
use snow::{Error as SnowError, TransportState};
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::engine::ZmtpStdStream;
// Import your ZmqError and its From<SnowError> conversion
use crate::error::ZmqError;

// Max typical Noise message size (payload + tag + potentially length, though snow handles framing)
// Noise protocol messages have a 2-byte length prefix then ciphertext+tag. Max ciphertext is 65535.
// So, a full noise message could be 2 + 65535 + 16 (tag) = 65553 bytes.
// For our internal buffers, a smaller practical size might be okay if we handle partial reads/writes.
const NOISE_MESSAGE_MAX_LEN: usize = 65535 + 18; // Max payload + 2 len bytes + 16 tag bytes
const INTERNAL_BUFFER_CAPACITY: usize = 8192 * 2; // Example capacity for read/write buffering
const AEAD_TAG_LENGTH: usize = 16; // For ChaChaPoly and AES-GCM (common in Noise)

// Temporary buffer size for reading from inner_stream before appending to staging_buffer
const TEMP_READ_BUF_SIZE: usize = 4096;

// Custom Debug for NoiseStream because TransportState might not be Debug
pub struct NoiseStream {
  /// The underlying (e.g., TCP) stream.
  inner_stream: Box<dyn ZmtpStdStream>,
  /// The snow TransportState for encrypting/decrypting.
  transport_state: TransportState,

  /// Buffer for plaintext data that has been decrypted but not yet read by the application.
  decrypted_read_buffer: BytesMut,

  /// Buffer for encrypted data read from inner_stream but not yet fully processed/decrypted
  /// (e.g., if we read only part of a Noise message).
  encrypted_read_staging_buffer: BytesMut,

  /// Buffer for encrypted data that has been prepared but not yet fully written to inner_stream.
  pending_write_buffer: BytesMut,
}

impl std::fmt::Debug for NoiseStream {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("NoiseStream")
      .field("inner_stream", &"Box<dyn ZmtpStdStream>") // Cannot debug dyn Trait easily
      .field("transport_state_is_valid", &true)
      .field("decrypted_read_buffer_len", &self.decrypted_read_buffer.len())
      .field(
        "encrypted_read_staging_buffer_len",
        &self.encrypted_read_staging_buffer.len(),
      )
      .field("pending_write_buffer_len", &self.pending_write_buffer.len())
      .finish()
  }
}

impl NoiseStream {
  pub fn new(inner: Box<dyn ZmtpStdStream>, transport_state: TransportState) -> Self {
    Self {
      inner_stream: inner,
      transport_state,
      decrypted_read_buffer: BytesMut::with_capacity(INTERNAL_BUFFER_CAPACITY),
      encrypted_read_staging_buffer: BytesMut::with_capacity(NOISE_MESSAGE_MAX_LEN),
      pending_write_buffer: BytesMut::new(), // Initialize empty
    }
  }

  // Helper to convert SnowError to IoError for AsyncRead/AsyncWrite traits
  fn snow_to_io_error(e: SnowError) -> IoError {
    tracing::warn!("NoiseStream: Snow error occurred: {:?}", e);
    // You might want more specific IoErrorKind mappings
    let zmq_err: ZmqError = e.into(); // Use your existing From<SnowError> for ZmqError
    IoError::new(IoErrorKind::Other, zmq_err.to_string()) // Wrap ZmqError string
  }
}

impl AsyncWrite for NoiseStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8], // Plaintext from ZMTP layer
  ) -> Poll<IoResult<usize>> {
    let mut this = self.as_mut().get_mut(); // Get mutable reference to self's fields

    // 1. Try to flush any previously buffered encrypted data first.
    while !this.pending_write_buffer.is_empty() {
      tracing::trace!(
        "NoiseStream: Attempting to flush pending_write_buffer (len: {})",
        this.pending_write_buffer.len()
      );
      match Pin::new(&mut this.inner_stream).poll_write(cx, &this.pending_write_buffer) {
        Poll::Ready(Ok(written_count)) => {
          tracing::trace!("NoiseStream: Wrote {} bytes from pending_write_buffer.", written_count);
          this.pending_write_buffer.advance(written_count);
          if this.pending_write_buffer.is_empty() {
            tracing::trace!("NoiseStream: pending_write_buffer fully flushed.");
            break; // Buffer is empty, proceed to encrypt new data
          }
          // Still data in pending_write_buffer, but poll_write returned Ok.
          // This implies the underlying stream might accept more now or in next poll.
          // We stay in this loop to try and flush more. If it was a full write of what was pending, loop breaks.
          // If it was partial, we loop and this call to poll_write will likely return Poll::Pending from inner_stream
          // or write more.
        }
        Poll::Ready(Err(e)) => {
          tracing::warn!(
            "NoiseStream: Error writing from pending_write_buffer to inner_stream: {}",
            e
          );
          return Poll::Ready(Err(e));
        }
        Poll::Pending => {
          tracing::trace!("NoiseStream: Writing from pending_write_buffer to inner_stream would block.");
          // Cannot write more from pending_write_buffer now.
          // If buf (new plaintext) is empty, we return Pending.
          // If buf has new data, we can't process it yet because pending must clear.
          return Poll::Pending;
        }
      }
    }

    // 2. If pending_write_buffer is now empty and input `buf` is empty, we're done.
    if buf.is_empty() {
      return Poll::Ready(Ok(0));
    }

    const MAX_PLAINTEXT_PER_NOISE_MESSAGE: usize = 65535 - AEAD_TAG_LENGTH;
    let plaintext_to_encrypt = if buf.len() > MAX_PLAINTEXT_PER_NOISE_MESSAGE {
      &buf[..MAX_PLAINTEXT_PER_NOISE_MESSAGE]
    } else {
      buf
    };

    // The output buffer for snow::TransportState::write_message needs to be
    // large enough for the ciphertext (same size as plaintext) + the AEAD tag.
    // snow itself will then prepend a 2-byte length to this, so the actual
    // buffer for `write_message` needs to be `plaintext_to_encrypt.len() + AEAD_TAG_LENGTH`.
    // The `vec!` we create for `encrypted_buf_for_wire` needs to be large enough for snow's *output*,
    // which *includes* the 2-byte length prefix.
    // So, if `write_message` expects a buffer for `ciphertext + tag`, and it writes `len_prefix + ciphertext + tag`,
    // then our `encrypted_buf_for_wire` should be sized for that full output.
    // The `write_message` API: `write_message(self, payload: &[u8], message_buf: &mut [u8]) -> Result<usize>`
    // `message_buf` is the output buffer for the full Noise message (len_prefix, ciphertext, tag).
    // It should be at least `payload.len() + TAGLEN + 2`.
    let required_buf_len = plaintext_to_encrypt.len() + AEAD_TAG_LENGTH + 2;
    let mut encrypted_buf_for_wire = vec![0u8; required_buf_len];

    match this
      .transport_state
      .write_message(plaintext_to_encrypt, &mut encrypted_buf_for_wire)
    {
      Ok(encrypted_len_on_wire) => {
        // `encrypted_len_on_wire` is the total length written by snow, including its 2-byte prefix.
        encrypted_buf_for_wire.truncate(encrypted_len_on_wire);
        tracing::trace!(
          "NoiseStream: Encrypted {} plaintext bytes into {} Noise message bytes for wire.",
          plaintext_to_encrypt.len(),
          encrypted_len_on_wire
        );

        // Now write the `encrypted_buf_for_wire` (which is a complete Noise message)
        // to the inner stream. This part still needs robust partial write handling.
        match Pin::new(&mut this.inner_stream).poll_write(cx, &encrypted_buf_for_wire) {
          Poll::Ready(Ok(written_on_wire)) => {
            if written_on_wire < encrypted_len_on_wire {
              // Partial write: buffer the remainder.
              this
                .pending_write_buffer
                .put_slice(&encrypted_buf_for_wire[written_on_wire..]);
              tracing::trace!(
                "NoiseStream: Partially wrote {} of {} encrypted bytes. {} pending.",
                written_on_wire,
                encrypted_len_on_wire,
                this.pending_write_buffer.len()
              );
            } else {
              tracing::trace!(
                "NoiseStream: Successfully wrote all {} newly encrypted bytes.",
                written_on_wire
              );
            }
            // Report how much of the *original plaintext `buf`* was processed and handed off for writing.
            Poll::Ready(Ok(plaintext_to_encrypt.len()))
          }
          Poll::Ready(Err(e)) => {
            tracing::warn!("NoiseStream: Error writing encrypted data to inner stream: {}", e);
            Poll::Ready(Err(e))
          }
          Poll::Pending => {
            tracing::trace!("NoiseStream: Writing newly encrypted data to inner_stream would block. Buffering all.");
            // Buffer the entire newly encrypted chunk.
            this.pending_write_buffer.put_slice(&encrypted_buf_for_wire);
            Poll::Pending
            // Inform caller that no input `buf` was consumed yet due to backpressure.
            // The caller should call poll_write again (maybe with same buf, or after poll_flush).
            // Returning Ok(0) would also be valid if we buffered but couldn't write anything.
            // But Pending is clearer that the operation is not complete for *this* input.
          }
        }
      }
      Err(snow_err) => Poll::Ready(Err(Self::snow_to_io_error(snow_err))),
    }
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
    let mut this = self.as_mut().get_mut();
    tracing::trace!(
      "NoiseStream: poll_flush called. Pending write buffer len: {}",
      this.pending_write_buffer.len()
    );

    // Try to flush our internal encrypted buffer
    while !this.pending_write_buffer.is_empty() {
      match Pin::new(&mut this.inner_stream).poll_write(cx, &this.pending_write_buffer) {
        Poll::Ready(Ok(written_count)) => {
          this.pending_write_buffer.advance(written_count);
          tracing::trace!(
            "NoiseStream: Flushed {} bytes from pending_write_buffer.",
            written_count
          );
          // If still not empty, loop and try to write more.
          // If the write didn't complete, the underlying stream might return Pending on next attempt.
        }
        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        Poll::Pending => {
          // Can't write more from our buffer now, underlying stream is pending.
          // But we are also pending because our buffer isn't empty.
          return Poll::Pending;
        }
      }
    }

    // If our buffer is empty, flush the inner stream
    Pin::new(&mut this.inner_stream).poll_flush(cx)
  }

  fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
    // Attempt to flush any remaining buffered data before shutting down.
    // This might loop if poll_flush needs multiple calls.
    match self.as_mut().poll_flush(cx) {
      Poll::Ready(Ok(())) => {
        // Buffered data flushed, now shutdown inner stream
        tracing::trace!("NoiseStream: Pending writes flushed, shutting down inner stream.");
        Pin::new(&mut self.get_mut().inner_stream).poll_shutdown(cx)
      }
      Poll::Ready(Err(e)) => Poll::Ready(Err(e)), // Error during flush
      Poll::Pending => {
        // Still flushing our internal buffer, not ready to shutdown inner stream.
        tracing::trace!("NoiseStream: Pending writes still flushing, shutdown is Pending.");
        Poll::Pending
      }
    }
  }
}

impl AsyncRead for NoiseStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>, // Output buffer for decrypted plaintext
  ) -> Poll<IoResult<()>> {
    // 1. If there's already decrypted plaintext available, serve it first.
    if !self.decrypted_read_buffer.is_empty() {
      let len_to_copy = std::cmp::min(buf.remaining(), self.decrypted_read_buffer.len());
      buf.put_slice(&self.decrypted_read_buffer[..len_to_copy]);
      self.decrypted_read_buffer.advance(len_to_copy);
      tracing::trace!("NoiseStream: Served {} bytes from decrypted_read_buffer.", len_to_copy);
      return Poll::Ready(Ok(()));
    }

    // 2. Try to read and decrypt a new Noise message from the inner stream.
    loop {
      // 2a. Do we have enough in staging to determine the length of the next Noise message?
      if self.encrypted_read_staging_buffer.len() < 2 {
        // Not enough to read the 2-byte length prefix. Try to read more from inner_stream.
        // Read into a temporary buffer then extend staging_buffer.
        let mut temp_buf_for_read = [0u8; TEMP_READ_BUF_SIZE];
        let mut read_buf_adapter = ReadBuf::new(&mut temp_buf_for_read);

        match Pin::new(&mut self.inner_stream).poll_read(cx, &mut read_buf_adapter) {
          Poll::Ready(Ok(())) => {
            let bytes_read = read_buf_adapter.filled().len();
            if bytes_read == 0 {
              // EOF from inner_stream
              tracing::debug!("NoiseStream: inner_stream reported EOF.");
              if !self.encrypted_read_staging_buffer.is_empty() {
                tracing::warn!("NoiseStream: EOF on inner_stream with partial Noise message in staging buffer ({} bytes). Reporting as unexpected EOF.", self.encrypted_read_staging_buffer.len());
                return Poll::Ready(Err(IoError::new(
                  IoErrorKind::UnexpectedEof,
                  "Partial Noise message before EOF",
                )));
              }
              return Poll::Ready(Ok(())); // Clean EOF
            }
            self.encrypted_read_staging_buffer.put_slice(read_buf_adapter.filled());
            tracing::trace!(
              "NoiseStream: Read {} bytes into staging_buffer (total now {}).",
              bytes_read,
              self.encrypted_read_staging_buffer.len()
            );
            // Loop again to check if we now have the length.
            continue;
          }
          Poll::Ready(Err(e)) => {
            tracing::warn!("NoiseStream: Error reading from inner_stream: {}", e);
            return Poll::Ready(Err(e));
          }
          Poll::Pending => {
            tracing::trace!("NoiseStream: Reading from inner_stream would block (Poll::Pending).");
            return Poll::Pending;
          }
        }
      }

      // 2b. We have at least 2 bytes, parse the length of the encrypted Noise payload (ciphertext + tag).
      // Snow prepends a 2-byte big-endian length for the (ciphertext + tag).
      let length_bytes: [u8; 2] = self.encrypted_read_staging_buffer[0..2]
        .try_into()
        .expect("Staging buffer has < 2 bytes after check; this is a logic error."); // Should not panic
      let encrypted_payload_len = u16::from_be_bytes(length_bytes) as usize;

      if encrypted_payload_len == 0 {
        tracing::warn!("NoiseStream: Read Noise message length prefix of 0. Consuming and retrying.");
        self.encrypted_read_staging_buffer.advance(2); // Consume the length
        continue; // Try to read the next message if any
      }

      if encrypted_payload_len > (NOISE_MESSAGE_MAX_LEN - 2) {
        tracing::error!(
          "NoiseStream: Decoded Noise message length (ciphertext + tag) {} exceeds maximum {}.",
          encrypted_payload_len,
          NOISE_MESSAGE_MAX_LEN - 2
        );
        return Poll::Ready(Err(IoError::new(
          IoErrorKind::InvalidData,
          "Decoded Noise message too large",
        )));
      }

      let total_noise_message_len_on_wire = 2 + encrypted_payload_len;

      if self.encrypted_read_staging_buffer.len() < total_noise_message_len_on_wire {
        let needed_more = total_noise_message_len_on_wire - self.encrypted_read_staging_buffer.len();
        let mut temp_buf_for_read = [0u8; TEMP_READ_BUF_SIZE];
        let mut read_buf_adapter = ReadBuf::new(&mut temp_buf_for_read);

        match Pin::new(&mut self.inner_stream).poll_read(cx, &mut read_buf_adapter) {
          Poll::Ready(Ok(())) => {
            let bytes_read = read_buf_adapter.filled().len();
            if bytes_read == 0 {
              tracing::warn!("NoiseStream: EOF on inner_stream while expecting {} more bytes for current Noise message (total expected {}).", needed_more, total_noise_message_len_on_wire);
              return Poll::Ready(Err(IoError::new(
                IoErrorKind::UnexpectedEof,
                "Partial Noise message before EOF",
              )));
            }
            self.encrypted_read_staging_buffer.put_slice(read_buf_adapter.filled());
            tracing::trace!(
              "NoiseStream: Read {} more bytes into staging_buffer for current Noise message (total now {}).",
              bytes_read,
              self.encrypted_read_staging_buffer.len()
            );
            continue;
          }
          Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
          Poll::Pending => return Poll::Pending,
        }
      }

      let full_noise_message_bytes = self
        .encrypted_read_staging_buffer
        .split_to(total_noise_message_len_on_wire);
      let encrypted_payload_with_tag_slice = &full_noise_message_bytes[2..];

      let max_plaintext_len = if encrypted_payload_len >= AEAD_TAG_LENGTH {
        encrypted_payload_len - AEAD_TAG_LENGTH
      } else {
        tracing::error!(
          "NoiseStream: Encrypted payload length ({}) is less than AEAD tag length ({}). Invalid message.",
          encrypted_payload_len,
          AEAD_TAG_LENGTH
        );
        return Poll::Ready(Err(IoError::new(
          IoErrorKind::InvalidData,
          "Encrypted payload too short for tag",
        )));
      };

      let mut temp_plaintext_buf_for_decrypt = vec![0u8; max_plaintext_len];

      match self
        .transport_state
        .read_message(encrypted_payload_with_tag_slice, &mut temp_plaintext_buf_for_decrypt)
      {
        Ok(actual_plaintext_len) => {
          self
            .decrypted_read_buffer
            .put_slice(&temp_plaintext_buf_for_decrypt[..actual_plaintext_len]);
          let len_to_copy = std::cmp::min(buf.remaining(), self.decrypted_read_buffer.len());
          buf.put_slice(&self.decrypted_read_buffer[..len_to_copy]);
          self.decrypted_read_buffer.advance(len_to_copy);
          tracing::trace!("NoiseStream: Served {} plaintext bytes to application.", len_to_copy);
          return Poll::Ready(Ok(()));
        }
        Err(snow_err) => {
          return Poll::Ready(Err(Self::snow_to_io_error(snow_err)));
        }
      }
    } // end loop
  }
}
