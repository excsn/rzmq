#![cfg(feature = "inproc")]

use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// In-memory duplex stream for inproc:// connections.
///
/// Wraps `tokio::io::DuplexStream` and provides a virtual file descriptor of `-1`
/// to satisfy the `AsRawFd` bound without any OS resources. TCP_CORK and io-uring
/// zero-copy are automatically disabled because `fd < 0` is checked in those paths.
pub(crate) struct InprocStream {
  inner: tokio::io::DuplexStream,
}

impl InprocStream {
  pub(crate) fn new(inner: tokio::io::DuplexStream) -> Self {
    Self { inner }
  }
}

impl std::fmt::Debug for InprocStream {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("InprocStream").finish_non_exhaustive()
  }
}

impl AsRawFd for InprocStream {
  fn as_raw_fd(&self) -> RawFd {
    -1 // Virtual descriptor — no kernel resource
  }
}

impl Unpin for InprocStream {}

impl AsyncRead for InprocStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<std::io::Result<()>> {
    Pin::new(&mut self.inner).poll_read(cx, buf)
  }
}

impl AsyncWrite for InprocStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<std::io::Result<usize>> {
    Pin::new(&mut self.inner).poll_write(cx, buf)
  }

  fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
    Pin::new(&mut self.inner).poll_flush(cx)
  }

  fn poll_shutdown(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<std::io::Result<()>> {
    Pin::new(&mut self.inner).poll_shutdown(cx)
  }
}

impl crate::transport::ZmtpReadHalf for tokio::io::ReadHalf<InprocStream> {}

impl crate::transport::ZmtpStdStream for InprocStream {
  type ReadHalf = tokio::io::ReadHalf<InprocStream>;
  type WriteHalf = tokio::io::WriteHalf<InprocStream>;

  fn into_split(self) -> (Self::ReadHalf, Self::WriteHalf) {
    tokio::io::split(self)
  }
}
