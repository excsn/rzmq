use bytes::BytesMut;

use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::transport::{ZmtpReadHalf, ZmtpStdStream};

use super::protocol_handler::ZmtpProtocolHandlerX;

/// Size of each async read request and the greedy-drain stack buffer.
/// Matches the typical OS socket receive-buffer size; one chunk drains
/// a full TCP window without requiring a second async yield.
const INGRESS_READ_CHUNK: usize = 65536;

/// Drop the network read buffer back to this capacity when it empties after
/// a large burst, so per-connection overhead returns to baseline.
const INGRESS_BUFFER_SHRINK_THRESHOLD: usize = INGRESS_READ_CHUNK;

/// Per-connection ingress processor.
///
/// Accumulates raw network bytes, delegates parse/decrypt to the protocol handler's
/// framer (keeping cipher state unified for both directions), and assembles complete
/// logical ZMQ messages from ZMTP frames before forwarding them downstream.
pub(crate) struct ZmqMessageProcessor {
  network_read_buffer: BytesMut,
  partial_accumulator: FrameBatch,
  assembling_multipart: bool,
}

impl ZmqMessageProcessor {
  pub(crate) fn new() -> Self {
    Self {
      network_read_buffer: BytesMut::with_capacity(8192),
      partial_accumulator: FrameBatch::new(),
      assembling_multipart: false,
    }
  }

  /// Async read path: reads directly from the transport, then parses all complete
  /// logical messages from the accumulated buffer.
  ///
  /// Returns `Err(ZmqError::ConnectionClosed)` on EOF. Uses a greedy synchronous
  /// drain after the initial async yield to reduce wakeup overhead.
  pub(crate) async fn read_and_process<S: ZmtpStdStream>(
    &mut self,
    reader: &mut S::ReadHalf,
    handler: &mut ZmtpProtocolHandlerX<S>,
  ) -> Result<Vec<FrameBatch>, ZmqError> {
    use tokio::io::AsyncReadExt;

    if self.network_read_buffer.len() > 16 * 1024 * 1024 {
      return Err(ZmqError::ResourceLimitReached);
    }

    self.network_read_buffer.reserve(INGRESS_READ_CHUNK);
    let n = reader
      .read_buf(&mut self.network_read_buffer)
      .await
      .map_err(|e| ZmqError::from_io_endpoint(e, "ingress read"))?;
    if n == 0 {
      return Err(ZmqError::ConnectionClosed);
    }

    // Greedy synchronous drain: avoid extra async yields when more data is ready.
    let mut greedy_buf = [0u8; INGRESS_READ_CHUNK];
    loop {
      match reader.try_read_chunk(&mut greedy_buf) {
        Ok(0) => return Err(ZmqError::ConnectionClosed),
        Ok(n) => self.network_read_buffer.extend_from_slice(&greedy_buf[..n]),
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
        Err(e) => return Err(ZmqError::from_io_endpoint(e, "ingress greedy read")),
      }
    }

    self.drain_buffer(handler)
  }

  fn drain_buffer<S: ZmtpStdStream>(
    &mut self,
    handler: &mut ZmtpProtocolHandlerX<S>,
  ) -> Result<Vec<FrameBatch>, ZmqError> {
    let mut complete: Vec<FrameBatch> = Vec::new();

    loop {
      match handler.try_read_msg_from_buffer(&mut self.network_read_buffer)? {
        None => break,
        Some(msg) => {
          let is_last = !msg.is_more();
          self.partial_accumulator.push(msg);
          if is_last {
            let assembled = std::mem::replace(&mut self.partial_accumulator, FrameBatch::new());
            self.assembling_multipart = false;
            complete.push(assembled);
          } else {
            self.assembling_multipart = true;
          }
        }
      }
    }

    // Shrink if the buffer grew large but is now empty.
    if self.network_read_buffer.is_empty()
      && self.network_read_buffer.capacity() > INGRESS_BUFFER_SHRINK_THRESHOLD
    {
      self.network_read_buffer = BytesMut::with_capacity(INGRESS_READ_CHUNK / 8);
    }

    Ok(complete)
  }

  /// True while a partial multipart message is being assembled.
  pub(crate) fn has_partial_message(&self) -> bool {
    self.assembling_multipart || !self.partial_accumulator.is_empty()
  }

  /// Discard any buffered state (e.g., on connection shutdown).
  pub(crate) fn reset(&mut self) {
    self.network_read_buffer.clear();
    self.partial_accumulator.clear();
    self.assembling_multipart = false;
  }
}
