// core/src/io_uring_backend/signaling_op_sender.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UringOpRequest;
use kanal::{AsyncSender as KanalAsyncSender, SendError as KanalSendError};
use std::os::fd::AsRawFd;

#[derive(Clone)] // EventFD is Cloneable
pub struct SignalingOpSender {
  op_tx: KanalAsyncSender<UringOpRequest>, // Store the async sender directly
  event_fd: eventfd::EventFD,            // Clone of the UringWorker's EventFD
}

impl SignalingOpSender {
  pub fn new(op_tx: KanalAsyncSender<UringOpRequest>, event_fd: eventfd::EventFD) -> Self {
    Self { op_tx, event_fd }
  }

  /// Asynchronously sends an operation request and signals the eventfd on success.
  pub async fn send(&self, req: UringOpRequest) -> Result<(), KanalSendError> {
    // Send to the underlying Kanal channel first.
    // Kanal's async sender's send method is itself async.
    let send_result = self.op_tx.send(req).await;

    if send_result.is_ok() {
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
    // Return the original result of the Kanal send operation.
    send_result
  }

  /// Attempts to send an operation request without blocking and signals eventfd on success.
  pub fn try_send(&self, req: UringOpRequest) -> Result<bool, KanalSendError> {
    let send_result = self.op_tx.try_send(req);

    if send_result.is_ok() {
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

  // --- Delegated methods to KanalAsyncSender ---

  pub fn is_closed(&self) -> bool {
    self.op_tx.is_closed()
  }

  pub fn is_full(&self) -> bool {
    self.op_tx.is_full()
  }
  
  pub fn capacity(&self) -> usize {
    self.op_tx.capacity()
  }

  pub fn len(&self) -> usize {
    self.op_tx.len()
  }

  // Note: Other methods like `send_blocking` or `send_timeout` from Kanal's Sender
  // would need to be implemented here if they are intended to be used through SignalingOpSender,
  // each also signaling the event_fd on success. For now, focusing on `send` (async) and `try_send`.
}

// Optional: Implement Debug manually if EventFD's Debug is not available or desired.
impl std::fmt::Debug for SignalingOpSender {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("SignalingOpSender")
      .field("op_tx_details", &format_args!("KanalAsyncSender(len:{}, cap:{:?}, closed:{})", self.op_tx.len(), self.op_tx.capacity(), self.op_tx.is_closed()))
      .field("event_fd_raw", &self.event_fd.as_raw_fd())
      .finish()
  }
}
// <<< MODIFIED END >>>