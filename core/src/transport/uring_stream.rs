#![cfg(feature = "io-uring")]

use std::future::Future;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::{
  Arc,
  atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};

use bytes::Bytes;
use eventfd::EventFD;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::io_uring_backend::byte_handler::EgressChunk;
use crate::io_uring_backend::send_buffer_pool::SendBufferPool;
use crate::transport::{ZmtpReadHalf, ZmtpStdStream};

// ─── Read Half ───────────────────────────────────────────────────────────────

/// Owned read half of a split `UringStream`.
///
/// Holds the inbound bytes channel and any partially-consumed active chunk.
pub(crate) struct UringReadHalf {
  fd: RawFd,
  rx_from_worker: fibre::mpsc::BoundedAsyncReceiver<Bytes>,
  /// Active chunk with remaining bytes for partial drains.
  current_lease: Option<Bytes>,
  worker_asleep: Arc<AtomicBool>,
  event_fd: EventFD,
  /// Wakeup threshold: write EventFD when channel occupancy drops to or below this value.
  channel_lwm: usize,
}

impl std::fmt::Debug for UringReadHalf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringReadHalf").field("fd", &self.fd).finish_non_exhaustive()
  }
}

impl Unpin for UringReadHalf {}

impl AsyncRead for UringReadHalf {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<io::Result<()>> {
    // Fast path: drain the active chunk (partial read from a prior call).
    if let Some(ref mut bytes) = self.current_lease {
      let to_copy = bytes.len().min(buf.remaining());
      buf.put_slice(&bytes[..to_copy]);
      if to_copy < bytes.len() {
        *bytes = bytes.slice(to_copy..); // zero-alloc slice advance
      } else {
        self.current_lease = None;
      }
      return Poll::Ready(Ok(()));
    }

    // Wait for the next chunk from the worker.
    match Pin::new(&mut self.rx_from_worker).poll_next(cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringReadHalf: worker channel closed",
      ))),
      Poll::Ready(Some(bytes)) => {
        if bytes.is_empty() {
          return Poll::Ready(Ok(())); // EOF sentinel
        }
        let to_copy = bytes.len().min(buf.remaining());
        buf.put_slice(&bytes[..to_copy]);
        if to_copy < bytes.len() {
          self.current_lease = Some(bytes.slice(to_copy..));
        }
        // Nudge the worker awake if the channel has drained to or below LWM so
        // it runs prepare_sqes and drains any spillover promptly.
        if self.rx_from_worker.len() <= self.channel_lwm
          && self.worker_asleep.load(Ordering::Relaxed)
        {
          let _ = self.event_fd.write(1);
        }
        Poll::Ready(Ok(()))
      }
    }
  }
}

impl UringReadHalf {
  /// Poll for the next raw inbound chunk without copying into a `ReadBuf`.
  pub(crate) fn poll_recv_bytes(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
  ) -> Poll<io::Result<Bytes>> {
    if self.current_lease.is_some() {
      return Poll::Ready(Err(io::Error::new(
        io::ErrorKind::Other,
        "UringReadHalf: partial chunk active; drain before polling raw bytes",
      )));
    }
    match Pin::new(&mut self.rx_from_worker).poll_next(cx) {
      Poll::Ready(Some(bytes)) => {
        if self.rx_from_worker.len() <= self.channel_lwm
          && self.worker_asleep.load(Ordering::Relaxed)
        {
          let _ = self.event_fd.write(1);
        }
        Poll::Ready(Ok(bytes))
      }
      Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringReadHalf: worker channel closed",
      ))),
      Poll::Pending => Poll::Pending,
    }
  }

  /// Try to receive the next chunk synchronously (noop waker — always returns immediately).
  pub(crate) fn try_recv_bytes(&mut self) -> Option<io::Result<Bytes>> {
    if self.current_lease.is_some() {
      return None;
    }
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    match Pin::new(&mut self.rx_from_worker).poll_next(&mut cx) {
      Poll::Ready(Some(bytes)) => {
        if self.rx_from_worker.len() <= self.channel_lwm
          && self.worker_asleep.load(Ordering::Relaxed)
        {
          let _ = self.event_fd.write(1);
        }
        Some(Ok(bytes))
      }
      Poll::Ready(None) => Some(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringReadHalf: worker channel closed",
      ))),
      Poll::Pending => None,
    }
  }

  /// Take ownership of any partially-consumed chunk (handshake→data phase transition).
  pub(crate) fn steal_current_bytes(&mut self) -> Option<Bytes> {
    self.current_lease.take()
  }
}

impl ZmtpReadHalf for UringReadHalf {
  fn try_recv_bytes(&mut self) -> Option<io::Result<Bytes>> {
    UringReadHalf::try_recv_bytes(self)
  }

  fn steal_current_bytes(&mut self) -> Option<Bytes> {
    UringReadHalf::steal_current_bytes(self)
  }

  fn poll_recv_bytes(
    self: Pin<&mut Self>,
    cx: &mut std::task::Context<'_>,
  ) -> Poll<io::Result<Bytes>> {
    UringReadHalf::poll_recv_bytes(self, cx)
  }

}

// ─── Write Half ──────────────────────────────────────────────────────────────

/// Owned write half of a split `UringStream`.
///
/// Forwards bytes to the io_uring worker via MPSC channels, with a zero-copy
/// fast path through pre-registered kernel send buffers.
pub(crate) struct UringWriteHalf {
  fd: RawFd,
  tx_to_worker_sync: fibre::mpsc::BoundedSender<EgressChunk>,
  tx_to_worker_async: fibre::mpsc::BoundedAsyncSender<EgressChunk>,
  /// Stored only when the egress channel was full on a prior `poll_write` call.
  pending_write:
    Option<(usize, Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>>)>,
  worker_asleep: Arc<AtomicBool>,
  event_fd: EventFD,
  pool: Option<Arc<SendBufferPool>>,
}

impl std::fmt::Debug for UringWriteHalf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringWriteHalf").field("fd", &self.fd).finish_non_exhaustive()
  }
}

impl Unpin for UringWriteHalf {}

impl UringWriteHalf {
  #[inline]
  fn wake_worker_if_asleep(&self) {
    if self.worker_asleep.load(Ordering::Acquire) {
      let _ = self.event_fd.write(1);
    }
  }

  fn poll_pending_write(&mut self, cx: &mut Context<'_>) -> Option<Poll<io::Result<usize>>> {
    if let Some((pending_n, ref mut fut)) = self.pending_write {
      match fut.as_mut().poll(cx) {
        Poll::Ready(Ok(())) => {
          let n = pending_n;
          self.pending_write = None;
          self.wake_worker_if_asleep();
          Some(Poll::Ready(Ok(n)))
        }
        Poll::Ready(Err(_)) => {
          self.pending_write = None;
          Some(Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "UringWriteHalf: worker channel closed",
          ))))
        }
        Poll::Pending => Some(Poll::Pending),
      }
    } else {
      None
    }
  }

  fn send_chunk(&mut self, cx: &mut Context<'_>, chunk: EgressChunk, n: usize) -> Poll<io::Result<usize>> {
    match self.tx_to_worker_sync.try_send(chunk) {
      Ok(()) => {
        self.wake_worker_if_asleep();
        Poll::Ready(Ok(n))
      }
      Err(fibre::TrySendError::Full(returned_chunk)) => {
        let tx = self.tx_to_worker_async.clone();
        let mut fut: Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>> =
          Box::pin(async move { tx.send(returned_chunk).await });
        match fut.as_mut().poll(cx) {
          Poll::Ready(Ok(())) => {
            self.wake_worker_if_asleep();
            Poll::Ready(Ok(n))
          }
          Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "UringWriteHalf: worker channel closed",
          ))),
          Poll::Pending => {
            self.pending_write = Some((n, fut));
            Poll::Pending
          }
        }
      }
      Err(_) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringWriteHalf: worker channel closed",
      ))),
    }
  }

  /// Fire-and-forget TCP_CORK toggle.
  pub(crate) fn set_cork(&self, enable: bool) {
    let _ = self.tx_to_worker_sync.try_send(EgressChunk::SetCork(enable));
    self.wake_worker_if_asleep();
  }
}

impl AsyncWrite for UringWriteHalf {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    let n = buf.len();

    if let Some(result) = self.poll_pending_write(cx) {
      return result;
    }

    if n > 0 {
      if let Some(ref pool) = self.pool {
        if let Some(lease) = pool.acquire_lease() {
          if n <= lease.capacity {
            // SAFETY: lease.ptr is exclusively held by this lease, capacity verified above.
            unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), lease.ptr, n) };
            return self.send_chunk(cx, EgressChunk::LeasedZeroCopy { lease, actual_len: n }, n);
          }
        }
      }
    }

    let chunk = EgressChunk::Contiguous(Bytes::copy_from_slice(buf));
    self.send_chunk(cx, chunk, n)
  }

  fn is_write_vectored(&self) -> bool {
    true
  }

  fn poll_write_vectored(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    bufs: &[io::IoSlice<'_>],
  ) -> Poll<io::Result<usize>> {
    let total_len: usize = bufs.iter().map(|b| b.len()).sum();
    if total_len == 0 {
      return Poll::Ready(Ok(0));
    }

    if let Some(result) = self.poll_pending_write(cx) {
      return result;
    }

    let bytes_vec: Vec<Bytes> = bufs
      .iter()
      .filter(|b| !b.is_empty())
      .map(|b| Bytes::copy_from_slice(&**b))
      .collect();
    let chunk = EgressChunk::Vectored(bytes_vec);
    self.send_chunk(cx, chunk, total_len)
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

// ─── Unified UringStream (used during handshake before split) ─────────────────

/// Tokio-side proxy stream that bridges `SessionConnectionActorX<S>` to the
/// `UringByteHandler` running on the io_uring worker OS thread.
///
/// During the handshake phase this struct is used directly (reads and writes
/// are sequential). Once the handshake completes, `into_split` is called to
/// produce independent `UringReadHalf` and `UringWriteHalf` for concurrent I/O.
pub(crate) struct UringStream {
  fd: RawFd,
  rx_from_worker: fibre::mpsc::BoundedAsyncReceiver<Bytes>,
  current_lease: Option<Bytes>,
  tx_to_worker_sync: fibre::mpsc::BoundedSender<EgressChunk>,
  tx_to_worker_async: fibre::mpsc::BoundedAsyncSender<EgressChunk>,
  pending_write: Option<(usize, Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>>)>,
  worker_asleep: Arc<AtomicBool>,
  event_fd: EventFD,
  pool: Option<Arc<SendBufferPool>>,
  channel_lwm: usize,
}

impl UringStream {
  pub(crate) fn new(
    fd: RawFd,
    rx_from_worker: fibre::mpsc::BoundedAsyncReceiver<Bytes>,
    tx_to_worker_sync: fibre::mpsc::BoundedSender<EgressChunk>,
    worker_asleep: Arc<AtomicBool>,
    event_fd: EventFD,
    pool: Option<Arc<SendBufferPool>>,
    channel_hwm: usize,
  ) -> Self {
    let tx_to_worker_async = tx_to_worker_sync.clone().to_async();
    Self {
      fd,
      rx_from_worker,
      current_lease: None,
      tx_to_worker_sync,
      tx_to_worker_async,
      pending_write: None,
      worker_asleep,
      event_fd,
      pool,
      channel_lwm: channel_hwm / 2,
    }
  }

  #[inline]
  fn wake_worker_if_asleep(&self) {
    if self.worker_asleep.load(Ordering::Acquire) {
      let _ = self.event_fd.write(1);
    }
  }

  fn poll_pending_write(&mut self, cx: &mut Context<'_>) -> Option<Poll<io::Result<usize>>> {
    if let Some((pending_n, ref mut fut)) = self.pending_write {
      match fut.as_mut().poll(cx) {
        Poll::Ready(Ok(())) => {
          let n = pending_n;
          self.pending_write = None;
          self.wake_worker_if_asleep();
          Some(Poll::Ready(Ok(n)))
        }
        Poll::Ready(Err(_)) => {
          self.pending_write = None;
          Some(Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "UringStream: worker channel closed",
          ))))
        }
        Poll::Pending => Some(Poll::Pending),
      }
    } else {
      None
    }
  }

  fn send_chunk(&mut self, cx: &mut Context<'_>, chunk: EgressChunk, n: usize) -> Poll<io::Result<usize>> {
    match self.tx_to_worker_sync.try_send(chunk) {
      Ok(()) => {
        self.wake_worker_if_asleep();
        Poll::Ready(Ok(n))
      }
      Err(fibre::TrySendError::Full(returned_chunk)) => {
        let tx = self.tx_to_worker_async.clone();
        let mut fut: Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>> =
          Box::pin(async move { tx.send(returned_chunk).await });
        match fut.as_mut().poll(cx) {
          Poll::Ready(Ok(())) => {
            self.wake_worker_if_asleep();
            Poll::Ready(Ok(n))
          }
          Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
            io::ErrorKind::ConnectionAborted,
            "UringStream: worker channel closed",
          ))),
          Poll::Pending => {
            self.pending_write = Some((n, fut));
            Poll::Pending
          }
        }
      }
      Err(_) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringStream: worker channel closed",
      ))),
    }
  }

  pub(crate) fn set_cork(&self, enable: bool) {
    let _ = self.tx_to_worker_sync.try_send(EgressChunk::SetCork(enable));
    self.wake_worker_if_asleep();
  }

}

impl std::fmt::Debug for UringStream {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringStream").field("fd", &self.fd).finish_non_exhaustive()
  }
}

impl AsRawFd for UringStream {
  fn as_raw_fd(&self) -> RawFd {
    self.fd
  }
}

impl Unpin for UringStream {}

impl AsyncRead for UringStream {
  fn poll_read(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &mut ReadBuf<'_>,
  ) -> Poll<io::Result<()>> {
    if let Some(ref mut bytes) = self.current_lease {
      let to_copy = bytes.len().min(buf.remaining());
      buf.put_slice(&bytes[..to_copy]);
      if to_copy < bytes.len() {
        *bytes = bytes.slice(to_copy..);
      } else {
        self.current_lease = None;
      }
      return Poll::Ready(Ok(()));
    }

    match Pin::new(&mut self.rx_from_worker).poll_next(cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringStream: worker channel closed",
      ))),
      Poll::Ready(Some(bytes)) => {
        if bytes.is_empty() {
          return Poll::Ready(Ok(())); // EOF sentinel
        }
        let to_copy = bytes.len().min(buf.remaining());
        buf.put_slice(&bytes[..to_copy]);
        if to_copy < bytes.len() {
          self.current_lease = Some(bytes.slice(to_copy..));
        }
        if self.rx_from_worker.len() <= self.channel_lwm
          && self.worker_asleep.load(Ordering::Relaxed)
        {
          let _ = self.event_fd.write(1);
        }
        Poll::Ready(Ok(()))
      }
    }
  }
}

impl AsyncWrite for UringStream {
  fn poll_write(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    buf: &[u8],
  ) -> Poll<io::Result<usize>> {
    let n = buf.len();

    if let Some(result) = self.poll_pending_write(cx) {
      return result;
    }

    if n > 0 {
      if let Some(ref pool) = self.pool {
        if let Some(lease) = pool.acquire_lease() {
          if n <= lease.capacity {
            // SAFETY: lease.ptr is exclusively held, capacity verified above.
            unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), lease.ptr, n) };
            return self.send_chunk(cx, EgressChunk::LeasedZeroCopy { lease, actual_len: n }, n);
          }
        }
      }
    }

    let chunk = EgressChunk::Contiguous(Bytes::copy_from_slice(buf));
    self.send_chunk(cx, chunk, n)
  }

  fn is_write_vectored(&self) -> bool {
    true
  }

  fn poll_write_vectored(
    mut self: Pin<&mut Self>,
    cx: &mut Context<'_>,
    bufs: &[io::IoSlice<'_>],
  ) -> Poll<io::Result<usize>> {
    let total_len: usize = bufs.iter().map(|b| b.len()).sum();
    if total_len == 0 {
      return Poll::Ready(Ok(0));
    }

    if let Some(result) = self.poll_pending_write(cx) {
      return result;
    }

    let bytes_vec: Vec<Bytes> = bufs
      .iter()
      .filter(|b| !b.is_empty())
      .map(|b| Bytes::copy_from_slice(&**b))
      .collect();
    let chunk = EgressChunk::Vectored(bytes_vec);
    self.send_chunk(cx, chunk, total_len)
  }

  fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    Poll::Ready(Ok(()))
  }
}

impl ZmtpStdStream for UringStream {
  type ReadHalf = UringReadHalf;
  type WriteHalf = UringWriteHalf;

  fn into_split(self) -> (UringReadHalf, UringWriteHalf) {
    let read_half = UringReadHalf {
      fd: self.fd,
      rx_from_worker: self.rx_from_worker,
      current_lease: self.current_lease,
      worker_asleep: Arc::clone(&self.worker_asleep),
      event_fd: self.event_fd.clone(),
      channel_lwm: self.channel_lwm,
    };
    let write_half = UringWriteHalf {
      fd: self.fd,
      tx_to_worker_sync: self.tx_to_worker_sync,
      tx_to_worker_async: self.tx_to_worker_async,
      pending_write: self.pending_write,
      worker_asleep: self.worker_asleep,
      event_fd: self.event_fd,
      pool: self.pool,
    };
    (read_half, write_half)
  }
}
