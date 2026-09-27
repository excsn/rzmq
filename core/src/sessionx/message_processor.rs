use bytes::BytesMut;

use crate::error::ZmqError;
use crate::protocol::zmtp::actions::EngineOutput;
use crate::protocol::zmtp::engine::ZmtpEngine;
use crate::transport::ZmtpReadHalf;

use super::INGRESS_GREEDY_CHUNK;

/// Spare capacity granted to a fresh read buffer per reserve on the anonymous
/// (accumulate) path. Kept modest relative to the greedy ceiling
/// (`INGRESS_GREEDY_CHUNK`: 512 KB non-macOS, 64 KB macOS).
#[cfg(not(target_os = "macos"))]
const READ_CHUNK: usize = 65536 * 2;
#[cfg(target_os = "macos")]
const READ_CHUNK: usize = 65536;

/// Initial size / reserve increment for the fresh frame-in-place buffer on the
/// addressed path (REP/DEALER/ROUTER). Deliberately small: these sockets are
/// request-response with small, low-pipelining messages, so a right-sized buffer
/// keeps payload-pinned allocations tiny and cache-resident. It still `reserve`s
/// upward on the rare large burst.
const FRAME_READ_CHUNK: usize = 16 * 1024;

/// Per-connection ingress I/O helper.
///
/// Each call allocates a *fresh* read buffer, seeds it with the engine's carried
/// partial-frame remainder, reads socket bytes into it (initial + greedy drain),
/// then frames directly from it via `ZmtpEngine::drive_buf`. Framing from a fresh,
/// unshared buffer avoids the redundant per-read copy into the accumulator and the
/// realloc-on-shared penalty. This struct holds no per-call state.
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
  ///
  /// `frame_in_place` selects the ingress strategy (resolved once per connection
  /// from the socket's ingress kind): `false` for anonymous ingress (PULL, SUB)
  /// — read into a throwaway buffer and copy once into the accumulator, which
  /// benchmarks best for high-pipelining streaming; `true` for addressed ingress
  /// (REP, DEALER, ROUTER) — frame directly from a fresh buffer with no
  /// accumulator copy. The branch is one predicted check per read batch.
  pub(crate) async fn read_and_process<RH: ZmtpReadHalf>(
    &mut self,
    reader: &mut RH,
    engine: &mut ZmtpEngine,
    frame_in_place: bool,
  ) -> Result<EngineOutput, ZmqError> {
    // Hard cap to prevent memory exhaustion if the engine isn't consuming bytes fast enough
    if engine.buffer_len() > 16 * 1024 * 1024 {
      return Err(ZmqError::ResourceLimitReached);
    }

    if frame_in_place {
      self.read_frame_in_place(reader, engine).await
    } else {
      self.read_accumulate(reader, engine).await
    }
  }

  /// Anonymous-ingress path (PULL, SUB): read into a fresh buffer and hand it to
  /// the engine, which copies it once into the accumulator.
  async fn read_accumulate<RH: ZmtpReadHalf>(
    &mut self,
    reader: &mut RH,
    engine: &mut ZmtpEngine,
  ) -> Result<EngineOutput, ZmqError> {
    use tokio::io::AsyncReadExt;

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

    // Enforce a strict byte ceiling per async cycle so a fast link (localhost)
    // can't hijack the OS thread and starve the Tokio executor.
    let max_greedy_read = engine.config().rcvbatch_bytes.max(INGRESS_GREEDY_CHUNK);

    // 2. Greedy synchronous drain up to the configured batch limit, straight into
    // `buf`. A full first read means a busy stream, so the buffer grows once to
    // the limit instead of doubling as reads arrive.
    if buf.len() == buf.capacity() && total_read < max_greedy_read {
      buf.reserve(max_greedy_read - total_read);
    }
    while total_read < max_greedy_read && buf.len() < buf.capacity() {
      match reader.try_read_buf(&mut buf) {
        Ok(0) => break,
        Ok(k) => {
          total_read += k;
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
        Err(e) => return Err(ZmqError::from_io_endpoint(e, "ingress greedy read")),
      }
    }

    // 3. Hand off the gathered bytes to the protocol engine for framing.
    Ok(engine.on_network_bytes(buf.freeze()))
  }

  /// Addressed-ingress path (REP, DEALER, ROUTER): frame directly from a fresh
  /// buffer seeded with the carried partial-frame remainder — no accumulator
  /// copy, and reads never hit BytesMut's realloc-on-shared path (the buffer is
  /// unshared until framing slices payloads from it).
  async fn read_frame_in_place<RH: ZmtpReadHalf>(
    &mut self,
    reader: &mut RH,
    engine: &mut ZmtpEngine,
  ) -> Result<EngineOutput, ZmqError> {
    use tokio::io::AsyncReadExt;

    let max_greedy_read = engine.config().rcvbatch_bytes.max(INGRESS_GREEDY_CHUNK);

    let carry = engine.carry();
    let mut buf = BytesMut::with_capacity(carry.len() + FRAME_READ_CHUNK);
    buf.extend_from_slice(carry);

    // 1. Initial async read (yields to Tokio if no data is available)
    let n = reader
      .read_buf(&mut buf)
      .await
      .map_err(|e| ZmqError::from_io_endpoint(e, "ingress read"))?;

    if n == 0 {
      return Err(ZmqError::ConnectionClosed);
    }

    let mut total_read = n;

    // 2. Greedy synchronous drain into the fresh buffer's spare (bounded increments).
    while total_read < max_greedy_read {
      if buf.capacity() - buf.len() < 4096 {
        buf.reserve(FRAME_READ_CHUNK);
      }
      match reader.try_read_buf(&mut buf) {
        Ok(0) => break,
        Ok(k) => {
          total_read += k;
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
        Err(e) => return Err(ZmqError::from_io_endpoint(e, "ingress greedy read")),
      }
    }

    // 3. Frame directly from the fresh buffer; the engine keeps the remainder.
    Ok(engine.drive_buf(buf))
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

    fn try_read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
      // Same trap for the fresh-buffer drain path: always provide data, never
      // WouldBlock. A bounded greedy loop terminates; an unbounded one hangs.
      let spare = buf.capacity() - buf.len();
      if spare == 0 {
        return Ok(0);
      }
      buf.extend_from_slice(&vec![0u8; spare]);
      Ok(spare)
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
      processor.read_and_process(&mut mock_reader, &mut engine, false),
    )
    .await;

    assert!(
      result.is_ok(),
      "REGRESSION: ZmaqMessageProcessor is trapped in an infinite greedy-read loop!"
    );
  }
}
