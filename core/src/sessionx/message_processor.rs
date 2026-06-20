use bytes::BytesMut;

use crate::error::ZmqError;
use crate::protocol::zmtp::actions::EngineOutput;
use crate::protocol::zmtp::engine::ZmtpEngine;
use crate::transport::ZmtpReadHalf;

use super::INGRESS_GREEDY_CHUNK;

/// Per-connection ingress I/O helper.
///
/// Performs the async network read (including a greedy synchronous drain to
/// avoid extra wakeups) and feeds the raw bytes into `ZmtpEngine::on_network_bytes`.
/// All parse/decrypt state lives in the engine; this struct holds no per-call state.
pub(crate) struct ZmqMessageProcessor;

impl ZmqMessageProcessor {
  pub(crate) fn new() -> Self {
    Self
  }

  /// Async read path: reads directly from the transport, greedily drains any
  /// immediately-available data up to a strict batch limit, then feeds all bytes
  /// to the engine.
  ///
  /// Returns `Err(ZmqError::ConnectionClosed)` on EOF. The `EngineOutput`
  /// contains both net actions (e.g. PONG frames to send) and app actions
  /// (e.g. `DeliverMessage` for complete logical messages).
  pub(crate) async fn read_and_process<RH: ZmtpReadHalf>(
    &mut self,
    reader: &mut RH,
    engine: &mut ZmtpEngine,
  ) -> Result<EngineOutput, ZmqError> {
    use tokio::io::AsyncReadExt;

    // Hard cap to prevent memory exhaustion if the engine isn't consuming bytes fast enough
    if engine.buffer_len() > 16 * 1024 * 1024 {
      return Err(ZmqError::ResourceLimitReached);
    }

    let rcvbuf = engine.config().rcvbuf.unwrap_or(INGRESS_GREEDY_CHUNK);
    let mut buf = BytesMut::with_capacity(INGRESS_GREEDY_CHUNK);

    // 1. Initial async read (yields to Tokio if no data is available)
    let n = reader
      .read_buf(&mut buf)
      .await
      .map_err(|e| ZmqError::from_io_endpoint(e, "ingress read"))?;

    if n == 0 {
      return Err(ZmqError::ConnectionClosed);
    }

    let mut total_read = n;

    // CRITICAL FIX: Synchronous Starvation Prevention
    // On extremely fast links (like localhost), the OS network buffer refills
    // as fast as we can drain it. If we loop infinitely until `WouldBlock`,
    // this task will hijack the OS thread, starve the Tokio executor, and
    // prevent the parsed messages from ever being delivered to the application.
    // We enforce a strict byte ceiling per async cycle to guarantee cooperative yielding.
    let max_greedy_read = engine.config().rcvbatch_bytes.max(INGRESS_GREEDY_CHUNK);

    // 2. Greedy synchronous drain up to the configured batch limit
    let mut greedy_buf = [0u8; INGRESS_GREEDY_CHUNK];
    while total_read < max_greedy_read {
      match reader.try_read_chunk(&mut greedy_buf) {
        Ok(0) => return Err(ZmqError::ConnectionClosed),
        Ok(n) => {
          buf.extend_from_slice(&greedy_buf[..n]);
          total_read += n;
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
        Err(e) => return Err(ZmqError::from_io_endpoint(e, "ingress greedy read")),
      }
    }

    // 3. Hand off the gathered bytes to the protocol engine for framing/decryption
    Ok(engine.on_network_bytes(buf.freeze()))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::protocol::zmtp::engine::ZmtpEngine;
  use crate::socket::options::ZmtpEngineConfig;
  use std::io;
  use std::pin::Pin;
  use std::sync::Arc;
  use std::task::{Context, Poll};
  use tokio::io::{AsyncRead, ReadBuf};

  // A mock reader that simulates an infinitely fast, never-blocking network stream.
  #[derive(Debug)]
  struct InfiniteMockReader;

  impl AsyncRead for InfiniteMockReader {
    fn poll_read(
      self: Pin<&mut Self>,
      _cx: &mut Context<'_>,
      buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
      // First async read succeeds immediately with dummy data
      let dummy = vec![0u8; buf.remaining()];
      buf.put_slice(&dummy);
      Poll::Ready(Ok(()))
    }
  }

  impl crate::transport::ZmtpReadHalf for InfiniteMockReader {
    fn try_read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      // The trap: always return data, NEVER return WouldBlock.
      // On a broken implementation, this causes an infinite loop.
      for b in buf.iter_mut() {
        *b = 0;
      }
      Ok(buf.len())
    }
  }

  #[tokio::test]
  async fn test_mre_ingress_starvation_deadlock() {
    let mut processor = ZmqMessageProcessor::new();
    let config = Arc::new(ZmtpEngineConfig::default());
    let mut engine = ZmtpEngine::new(true, config);
    let mut mock_reader = InfiniteMockReader;

    // Wrap the call in a strict timeout.
    // If the greedy loop is un-bounded, this will time out and fail the test.
    // If it is bounded, it will process the cap and return almost instantly.
    let result = tokio::time::timeout(
      std::time::Duration::from_millis(50),
      processor.read_and_process(&mut mock_reader, &mut engine),
    )
    .await;

    assert!(
      result.is_ok(),
      "REGRESSION: ZmaqMessageProcessor is trapped in an infinite greedy-read loop!"
    );
  }
}
