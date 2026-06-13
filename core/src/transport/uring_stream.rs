#![cfg(feature = "io-uring")]

use std::future::Future;
use std::io;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::{
  Arc,
  atomic::{AtomicU8, Ordering},
};
use std::task::{Context, Poll};

use bytes::Bytes;
use eventfd::EventFD;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::io_uring_backend::byte_handler::EgressChunk;
use crate::io_uring_backend::ops::{WAKEUP_STATE_SIGNALED, WAKEUP_STATE_SLEEPING};
use crate::io_uring_backend::send_buffer_pool::SendBufferPool;
use crate::transport::{ZmtpReadHalf, ZmtpStdStream, ZmtpWriteHalf};

// ─── Read Half ───────────────────────────────────────────────────────────────

/// Max chunks pulled from the worker channel per batch refill in `try_recv_bytes`.
const CHUNK_REFILL_MAX: usize = 16;

/// Owned read half of a split `UringStream`.
///
/// Holds the inbound bytes channel and any partially-consumed active chunk.
pub(crate) struct UringReadHalf {
  fd: RawFd,
  rx_from_worker: fibre::mpsc::BoundedAsyncReceiver<Bytes>,
  /// Active chunk with remaining bytes for partial drains.
  current_lease: Option<Bytes>,
  /// Chunks pulled ahead from the worker channel by a batch refill, served in FIFO
  /// order before the channel is polled again. May contain the empty EOF sentinel.
  pending_chunks: std::collections::VecDeque<Bytes>,
  worker_asleep: Arc<AtomicU8>,
  event_fd: EventFD,
  /// Wakeup threshold: write EventFD when channel occupancy drops to or below this value.
  channel_lwm: usize,
}

impl std::fmt::Debug for UringReadHalf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringReadHalf")
      .field("fd", &self.fd)
      .finish_non_exhaustive()
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

    // Serve any chunk pulled ahead by a batch refill before polling the channel.
    if let Some(bytes) = self.pending_chunks.pop_front() {
      if bytes.is_empty() {
        return Poll::Ready(Ok(())); // EOF sentinel
      }
      let to_copy = bytes.len().min(buf.remaining());
      buf.put_slice(&bytes[..to_copy]);
      if to_copy < bytes.len() {
        self.current_lease = Some(bytes.slice(to_copy..));
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
        self.wake_worker_if_asleep();
        Poll::Ready(Ok(()))
      }
    }
  }
}

impl UringReadHalf {
  /// Nudge the worker awake if it confirmed itself asleep. During active
  /// streaming `worker_asleep` is false, so this write is skipped entirely.
  #[inline]
  fn wake_worker_if_asleep(&self) {
    if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
      if self.worker_asleep.compare_exchange(
        WAKEUP_STATE_SLEEPING,
        WAKEUP_STATE_SIGNALED,
        Ordering::AcqRel,
        Ordering::Acquire,
      ).is_ok() {
        let _ = self.event_fd.write(1);
      }
    }
  }

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
    if let Some(bytes) = self.pending_chunks.pop_front() {
      return Poll::Ready(Ok(bytes));
    }
    match Pin::new(&mut self.rx_from_worker).poll_next(cx) {
      Poll::Ready(Some(bytes)) => {
        self.wake_worker_if_asleep();
        Poll::Ready(Ok(bytes))
      }
      Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringReadHalf: worker channel closed",
      ))),
      Poll::Pending => Poll::Pending,
    }
  }

  /// Try to receive the next chunk synchronously (always returns immediately).
  ///
  /// Refills from the worker channel in batches of up to `CHUNK_REFILL_MAX`:
  /// one channel pass and at most one worker wakeup per refill instead of per chunk.
  pub(crate) fn try_recv_bytes(&mut self) -> Option<io::Result<Bytes>> {
    if let Some(lease) = self.current_lease.take() {
      return Some(Ok(lease));
    }
    if let Some(chunk) = self.pending_chunks.pop_front() {
      return Some(Ok(chunk));
    }
    match self.rx_from_worker.try_recv_batch(CHUNK_REFILL_MAX) {
      Ok(chunks) => {
        let mut iter = chunks.into_iter();
        let first = iter.next()?; // batch is guaranteed non-empty
        self.pending_chunks.extend(iter);
        self.wake_worker_if_asleep();
        Some(Ok(first))
      }
      Err(fibre::TryRecvError::Empty) => None,
      Err(fibre::TryRecvError::Disconnected) => Some(Err(io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "UringReadHalf: worker channel closed",
      ))),
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

  fn try_read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    match UringReadHalf::try_recv_bytes(self) {
      None => Err(io::Error::from(io::ErrorKind::WouldBlock)),
      Some(Err(e)) => Err(e),
      Some(Ok(bytes)) if bytes.is_empty() => Ok(0), // EOF sentinel
      Some(Ok(bytes)) => {
        let n = bytes.len().min(buf.len());
        buf[..n].copy_from_slice(&bytes[..n]);
        if n < bytes.len() {
          self.pending_chunks.push_front(bytes.slice(n..));
        }
        Ok(n)
      }
    }
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
  pending_write: Option<(
    usize,
    Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>>,
  )>,
  worker_asleep: Arc<AtomicU8>,
  event_fd: EventFD,
  pool: Option<Arc<SendBufferPool>>,
}

impl std::fmt::Debug for UringWriteHalf {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringWriteHalf")
      .field("fd", &self.fd)
      .finish_non_exhaustive()
  }
}

impl Unpin for UringWriteHalf {}

impl UringWriteHalf {
  #[inline]
  fn wake_worker_if_asleep(&self) {
    if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
      if self.worker_asleep.compare_exchange(
        WAKEUP_STATE_SLEEPING,
        WAKEUP_STATE_SIGNALED,
        Ordering::AcqRel,
        Ordering::Acquire,
      ).is_ok() {
        let _ = self.event_fd.write(1);
      }
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

  fn send_chunk(
    &mut self,
    cx: &mut Context<'_>,
    chunk: EgressChunk,
    n: usize,
  ) -> Poll<io::Result<usize>> {
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
    let _ = self
      .tx_to_worker_sync
      .try_send(EgressChunk::SetCork(enable));
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

    // VERY IMPORTANT: If we already failed to enqueue this exact buffer last time
    // due to channel backpressure, we MUST resolve that pending future before doing
    // anything else. `AsyncWrite` dictates the caller provides the *same* buffer on retry.
    if let Some(result) = self.poll_pending_write(cx) {
      return result;
    }

    if n > 0 {
      if let Some(ref pool) = self.pool {
        if let Some(lease) = pool.acquire_lease() {
          if n <= lease.capacity {
            // SAFETY: lease.ptr is exclusively held by this lease, capacity verified above.
            unsafe { std::ptr::copy_nonoverlapping(buf.as_ptr(), lease.ptr, n) };
            return self.send_chunk(
              cx,
              EgressChunk::LeasedZeroCopy {
                lease,
                actual_len: n,
              },
              n,
            );
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
  pending_write: Option<(
    usize,
    Pin<Box<dyn Future<Output = Result<(), fibre::SendError>> + Send>>,
  )>,
  worker_asleep: Arc<AtomicU8>,
  event_fd: EventFD,
  pool: Option<Arc<SendBufferPool>>,
  channel_lwm: usize,
}

impl UringStream {
  pub(crate) fn new(
    fd: RawFd,
    rx_from_worker: fibre::mpsc::BoundedAsyncReceiver<Bytes>,
    tx_to_worker_sync: fibre::mpsc::BoundedSender<EgressChunk>,
    worker_asleep: Arc<AtomicU8>,
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
    if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
      if self.worker_asleep.compare_exchange(
        WAKEUP_STATE_SLEEPING,
        WAKEUP_STATE_SIGNALED,
        Ordering::AcqRel,
        Ordering::Acquire,
      ).is_ok() {
        let _ = self.event_fd.write(1);
      }
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

  fn send_chunk(
    &mut self,
    cx: &mut Context<'_>,
    chunk: EgressChunk,
    n: usize,
  ) -> Poll<io::Result<usize>> {
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
    let _ = self
      .tx_to_worker_sync
      .try_send(EgressChunk::SetCork(enable));
    self.wake_worker_if_asleep();
  }
}

impl std::fmt::Debug for UringStream {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("UringStream")
      .field("fd", &self.fd)
      .finish_non_exhaustive()
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
        if self.worker_asleep.load(Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
          if self.worker_asleep.compare_exchange(
            WAKEUP_STATE_SLEEPING,
            WAKEUP_STATE_SIGNALED,
            Ordering::AcqRel,
            Ordering::Acquire,
          ).is_ok() {
            let _ = self.event_fd.write(1);
          }
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
            return self.send_chunk(
              cx,
              EgressChunk::LeasedZeroCopy {
                lease,
                actual_len: n,
              },
              n,
            );
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

impl ZmtpWriteHalf for UringWriteHalf {
  fn supports_owned_write(&self) -> bool {
    true
  }

  fn set_cork(&self, enable: bool) {
    let _ = self.tx_to_worker_sync.try_send(EgressChunk::SetCork(enable));
    self.wake_worker_if_asleep();
  }

  fn write_owned(
    &mut self,
    bufs: Vec<bytes::Bytes>,
  ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
    let non_empty: Vec<_> = bufs.into_iter().filter(|b| !b.is_empty()).collect();
    let tx_sync = self.tx_to_worker_sync.clone();
    let tx_async = self.tx_to_worker_async.clone();
    let worker_asleep = self.worker_asleep.clone();
    let event_fd = self.event_fd.clone();

    async move {
      if non_empty.is_empty() {
        return Ok(());
      }
      let chunk = EgressChunk::Vectored(non_empty);
      match tx_sync.try_send(chunk) {
        Ok(()) => {
          if worker_asleep.load(std::sync::atomic::Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
            if worker_asleep.compare_exchange(
              WAKEUP_STATE_SLEEPING,
              WAKEUP_STATE_SIGNALED,
              std::sync::atomic::Ordering::AcqRel,
              std::sync::atomic::Ordering::Acquire,
            ).is_ok() {
              let _ = event_fd.write(1);
            }
          }
          Ok(())
        }
        Err(fibre::TrySendError::Full(c)) => {
          tx_async.send(c).await.map_err(|_| {
            std::io::Error::new(
              std::io::ErrorKind::ConnectionAborted,
              "UringWriteHalf: worker channel closed",
            )
          })?;
          if worker_asleep.load(std::sync::atomic::Ordering::Relaxed) == WAKEUP_STATE_SLEEPING {
            if worker_asleep.compare_exchange(
              WAKEUP_STATE_SLEEPING,
              WAKEUP_STATE_SIGNALED,
              std::sync::atomic::Ordering::AcqRel,
              std::sync::atomic::Ordering::Acquire,
            ).is_ok() {
              let _ = event_fd.write(1);
            }
          }
          Ok(())
        }
        Err(_) => Err(std::io::Error::new(
          std::io::ErrorKind::ConnectionAborted,
          "UringWriteHalf: worker channel closed",
        )),
      }
    }
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
      pending_chunks: std::collections::VecDeque::new(),
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
