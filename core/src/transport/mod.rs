pub mod endpoint;
#[cfg(feature = "inproc")]
pub mod inproc;
#[cfg(feature = "inproc")]
pub mod inproc_stream;
#[cfg(feature = "ipc")]
pub mod ipc;
pub mod tcp;
#[cfg(feature = "io-uring")]
pub mod uring_stream;

use std::os::fd::AsRawFd;

use tokio::io::{AsyncRead, AsyncWrite};

/// Trait for the read half of a split ZMTP stream.
///
/// Carries the io-uring fast-path methods as default no-ops, allowing standard
/// transports (TCP, IPC, inproc) to satisfy the bound without any io-uring
/// awareness. Only `UringReadHalf` overrides these methods.
pub(crate) trait ZmtpReadHalf: AsyncRead + Unpin + Send + std::fmt::Debug + 'static {
  #[cfg(feature = "io-uring")]
  fn try_recv_bytes(&mut self) -> Option<std::io::Result<bytes::Bytes>> {
    None
  }

  #[cfg(feature = "io-uring")]
  fn steal_current_bytes(&mut self) -> Option<bytes::Bytes> {
    None
  }

  #[cfg(feature = "io-uring")]
  fn poll_recv_bytes(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::io::Result<bytes::Bytes>> {
    std::task::Poll::Ready(Err(std::io::Error::new(
      std::io::ErrorKind::Unsupported,
      "poll_recv_bytes not supported for this stream type",
    )))
  }
}

/// Extension trait for write halves that support ownership-transfer sends.
///
/// Transports that implement a Proactor I/O model (io_uring) can accept a
/// `Vec<Bytes>` and hold the references alive in the kernel until the write
/// completes — eliminating the memcpy that `AsyncWrite::poll_write` requires.
///
/// Standard transports (TCP, IPC, inproc) use the default `supports_owned_write`
/// returning `false` and leave `write_owned` unimplemented; the session actor
/// routes them through the cancel-safe `EgressBuffer` + `AsyncWrite` path instead.
pub(crate) trait ZmtpWriteHalf: AsyncWrite + Unpin + Send + std::fmt::Debug + 'static {
  /// Returns `true` only for transports that provide a cancel-safe zero-copy
  /// `write_owned` implementation. TCP/IPC default to `false`.
  fn supports_owned_write(&self) -> bool {
    false
  }

  /// Transfers ownership of `bufs` to the transport without copying bytes.
  /// Only called when `supports_owned_write()` is `true`.
  ///
  /// Cancel-safety: callers hold the original `Vec<Bytes>` outside this future
  /// (passing a `.clone()`) so that if the future is dropped mid-await the data
  /// is not lost — the caller retries on the next loop iteration.
  fn write_owned(
    &mut self,
    _bufs: Vec<bytes::Bytes>,
  ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
    async {
      Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "write_owned not supported for this transport",
      ))
    }
  }

  /// Toggle TCP_CORK on the underlying socket. No-op for transports that do
  /// not support it (TCP standard path, IPC, inproc).
  fn set_cork(&self, _enable: bool) {}
}

/// Trait alias for full-duplex streams usable by ZMTP connection actors.
///
/// The stream is used directly during the handshake phase (reads and writes
/// are sequential, so no split is needed). Once the handshake completes the
/// actor calls `into_split` to obtain independent owned halves for concurrent
/// I/O in separate `select!` arms.
pub(crate) trait ZmtpStdStream:
  AsyncRead + AsyncWrite + AsRawFd + Unpin + Send + std::fmt::Debug + 'static
{
  /// The owned read half returned by `into_split`.
  type ReadHalf: ZmtpReadHalf;
  /// The owned write half returned by `into_split`. Must also satisfy `ZmtpWriteHalf`
  /// for capability-gated zero-copy egress on io_uring connections.
  type WriteHalf: ZmtpWriteHalf;

  /// Consume the stream and produce independent, owned read and write halves.
  fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

// --- TCP ---

impl ZmtpReadHalf for tokio::net::tcp::OwnedReadHalf {}

// Empty impl — TCP uses EgressBuffer + AsyncWrite path (supports_owned_write = false).
impl ZmtpWriteHalf for tokio::net::tcp::OwnedWriteHalf {}

impl ZmtpStdStream for tokio::net::TcpStream {
  type ReadHalf = tokio::net::tcp::OwnedReadHalf;
  type WriteHalf = tokio::net::tcp::OwnedWriteHalf;

  fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
    tokio::net::TcpStream::into_split(self)
  }
}

// --- IPC (UnixStream) ---

#[cfg(feature = "ipc")]
impl ZmtpReadHalf for tokio::net::unix::OwnedReadHalf {}

// Empty impl — IPC uses EgressBuffer + AsyncWrite path (supports_owned_write = false).
#[cfg(feature = "ipc")]
impl ZmtpWriteHalf for tokio::net::unix::OwnedWriteHalf {}

#[cfg(feature = "ipc")]
impl ZmtpStdStream for tokio::net::UnixStream {
  type ReadHalf = tokio::net::unix::OwnedReadHalf;
  type WriteHalf = tokio::net::unix::OwnedWriteHalf;

  fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
    tokio::net::UnixStream::into_split(self)
  }
}

// UringStream impl is in uring_stream.rs
// InprocStream impl is in inproc_stream.rs
