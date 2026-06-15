use bytes::BytesMut;

use crate::error::ZmqError;
use crate::protocol::zmtp::actions::EngineOutput;
use crate::protocol::zmtp::engine::ZmtpEngine;
use crate::transport::ZmtpReadHalf;

/// Size of each async read request and the greedy-drain stack buffer.
/// Matches the typical OS socket receive-buffer size; one chunk drains
/// a full TCP window without requiring a second async yield.
const INGRESS_READ_CHUNK: usize = 65536;

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
    /// immediately-available data, then feeds all bytes to the engine.
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

        if engine.buffer_len() > 16 * 1024 * 1024 {
            return Err(ZmqError::ResourceLimitReached);
        }

        let mut buf = BytesMut::with_capacity(INGRESS_READ_CHUNK);
        let n = reader
            .read_buf(&mut buf)
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
                Ok(n) => buf.extend_from_slice(&greedy_buf[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(ZmqError::from_io_endpoint(e, "ingress greedy read")),
            }
        }

        Ok(engine.on_network_bytes(buf.freeze()))
    }
}
