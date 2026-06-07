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
/// Carries the io-uring zero-copy lease methods as default no-ops, allowing
/// standard transports (TCP, IPC, inproc) to satisfy the bound without any
/// io-uring awareness. Only `UringReadHalf` overrides these methods.
pub(crate) trait ZmtpReadHalf: AsyncRead + Unpin + Send + std::fmt::Debug + 'static {
  #[cfg(feature = "io-uring")]
  fn try_recv_lease(
    &mut self,
  ) -> Option<std::io::Result<crate::io_uring_backend::byte_handler::UringInboundLease>> {
    None
  }

  #[cfg(feature = "io-uring")]
  fn steal_current_lease(
    &mut self,
  ) -> Option<(crate::io_uring_backend::byte_handler::UringInboundLease, usize)> {
    None
  }

  #[cfg(feature = "io-uring")]
  fn poll_recv_lease(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::io::Result<crate::io_uring_backend::byte_handler::UringInboundLease>>
  {
    std::task::Poll::Ready(Err(std::io::Error::new(
      std::io::ErrorKind::Unsupported,
      "poll_recv_lease not supported for this stream type",
    )))
  }

  #[cfg(feature = "io-uring")]
  fn notify_lease_consumed(&self) {}
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
  /// The owned write half returned by `into_split`.
  type WriteHalf: AsyncWrite + Unpin + Send + std::fmt::Debug + 'static;

  /// Consume the stream and produce independent, owned read and write halves.
  fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf);
}

// --- TCP ---

impl ZmtpReadHalf for tokio::net::tcp::OwnedReadHalf {}

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
