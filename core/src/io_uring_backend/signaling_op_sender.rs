#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UringOpRequest;
use crate::io_uring_backend::send_buffer_pool::SendBufferPool;
use fibre::{mpmc::AsyncSender, SendError, TrySendError};
use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{os::fd::AsRawFd, usize};

#[derive(Clone)] // EventFD is Cloneable
pub struct SignalingOpSender {
  op_tx: AsyncSender<UringOpRequest>,
  event_fd: eventfd::EventFD,
  worker_asleep: Arc<AtomicU8>,
  /// Monotonic work-enqueued generation counter shared with the worker loop.
  /// Bumped on every successful op enqueue so a spinning worker detects the new
  /// op via a single relaxed load instead of locking the mpmc op channel.
  work_signal_gen: Arc<AtomicUsize>,
  /// Shared slot populated by the worker thread once the `SendBufferPool` is initialized.
  /// `OnceCell` is lock-free for reads after the one-time write.
  pool_slot: Arc<OnceCell<Arc<SendBufferPool>>>,
}

impl SignalingOpSender {
  pub fn new(
    op_tx: AsyncSender<UringOpRequest>,
    event_fd: eventfd::EventFD,
    worker_asleep: Arc<AtomicU8>,
    work_signal_gen: Arc<AtomicUsize>,
    pool_slot: Arc<OnceCell<Arc<SendBufferPool>>>,
  ) -> Self {
    Self { op_tx, event_fd, worker_asleep, work_signal_gen, pool_slot }
  }

  /// Clones the worker-asleep flag to share with data connections.
  pub fn clone_worker_asleep(&self) -> Arc<AtomicU8> {
    Arc::clone(&self.worker_asleep)
  }

  /// Returns a clone of the send-buffer pool, if one is initialized.
  pub fn clone_send_buffer_pool(&self) -> Option<Arc<SendBufferPool>> {
    self.pool_slot.get().cloned()
  }

  /// Asynchronously sends an operation request and signals the eventfd on success.
  pub async fn send(&self, req: UringOpRequest) -> Result<(), SendError> {
    // Send to the underlying channel first.
    let send_result = self.op_tx.send(req).await;

    if send_result.is_ok() {
      // Monotonic work-signal bump (Release): pairs with the worker's Acquire load
      // in the spin loop so the enqueued op is visible without locking op_tx.
      self.work_signal_gen.fetch_add(1, Ordering::Release);
      // If send was successful, signal the eventfd.
      let val_to_write: u64 = 1;
      if let Err(e) = self.event_fd.write(val_to_write) {
        // Log the error. The UringOpRequest is already sent, so we can't easily roll that back.
        // The UringWorker might miss the immediate wakeup for this op if eventfd write fails.
        tracing::error!(
          "[SignalingOpSender] Failed to write to eventfd {} after op send: {}. UringWorker might not wake for new op.",
          self.event_fd.as_raw_fd(), // Using AsRawFd for logging
          e
        );
        // Depending on severity, could panic or return a custom error,
        // but for now, the primary send_result is what's returned.
      } else {
        tracing::trace!(
          "[SignalingOpSender] Signaled eventfd {} with value {} after op send.",
          self.event_fd.as_raw_fd(),
          val_to_write
        );
      }
    }
    // Return the original result of the send operation.
    send_result
  }

  /// Attempts to send an operation request without blocking and signals eventfd on success.
  pub fn try_send(&self, req: UringOpRequest) -> Result<(), TrySendError<UringOpRequest>> {
    let send_result = self.op_tx.try_send(req);

    if send_result.is_ok() {
      // Monotonic work-signal bump (Release): see `send` above.
      self.work_signal_gen.fetch_add(1, Ordering::Release);
      let val_to_write: u64 = 1;
      if let Err(e) = self.event_fd.write(val_to_write) {
        tracing::error!(
          "[SignalingOpSender] Failed to write to eventfd {} after op try_send: {}. Worker might not wake.",
          self.event_fd.as_raw_fd(),
          e
        );
      } else {
        tracing::trace!(
          "[SignalingOpSender] Signaled eventfd {} with value {} after op try_send.",
          self.event_fd.as_raw_fd(),
          val_to_write
        );
      }
    }
    send_result
  }

  // --- Delegated methods to AsyncSender ---

  pub fn is_closed(&self) -> bool {
    self.op_tx.is_closed()
  }

  pub fn is_full(&self) -> bool {
    self.op_tx.is_full()
  }

  pub fn capacity(&self) -> usize {
    self.op_tx.capacity().unwrap_or(usize::MAX)
  }

  pub fn len(&self) -> usize {
    self.op_tx.len()
  }

  /// Clones the underlying EventFD to be shared across data connections.
  pub fn clone_event_fd(&self) -> eventfd::EventFD {
    self.event_fd.clone()
  }
}

// Optional: Implement Debug manually if EventFD's Debug is not available or desired.
impl std::fmt::Debug for SignalingOpSender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SignalingOpSender")
      .field(
        "op_tx_details",
        &format_args!(
          "AsyncSender(len:{}, cap:{:?}, closed:{})",
          self.op_tx.len(),
          self.op_tx.capacity(),
          self.op_tx.is_closed()
        ),
      )
      .field("event_fd_raw", &self.event_fd.as_raw_fd())
      .finish()
  }
}
