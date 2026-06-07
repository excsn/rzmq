#![cfg(feature = "io-uring")]

use crate::ZmqError;
use bytes::{Bytes, BytesMut};
use io_uring::IoUring;
use io_uring_buf_ring::{BorrowedBuffer, IoUringBufRing};
use std::fmt;

/// Manages a ring of buffers registered with io_uring for efficient receives.
pub struct BufferRingManager {
  buf_ring_instance: IoUringBufRing<BytesMut>,
}

impl fmt::Debug for BufferRingManager {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("BufferRingManager")
      .field("bgid", &self.buf_ring_instance.buffer_group())
      .finish_non_exhaustive()
  }
}

impl BufferRingManager {
  pub fn new(
    ring: &IoUring,
    ring_entries: u16,
    bgid: u16,
    buffer_capacity: usize,
  ) -> Result<Self, ZmqError> {
    tracing::info!(
      "Initializing BufferRingManager with bgid: {}, requested_entries: {}, capacity_per_buffer: {}",
      bgid,
      ring_entries,
      buffer_capacity
    );

    // Create an iterator that yields `ring_entries` number of BytesMut buffers.
    let bufs_iter = (0..ring_entries).map(|_| BytesMut::with_capacity(buffer_capacity));

    match IoUringBufRing::new_with_buffers_and_flags(ring, bufs_iter, bgid, 0) {
      Ok(buf_ring_instance) => {
        tracing::info!(
          "BufferRingManager: IoUringBufRing built and registered successfully for bgid: {}",
          bgid
        );
        Ok(Self { buf_ring_instance })
      }
      Err(e) => {
        tracing::error!("Failed to build and register IoUringBufRing: {:?}", e);
        Err(ZmqError::Internal(format!(
          "BufferRingManager build failed: {:?}",
          e
        )))
      }
    }
  }

  pub fn group_id(&self) -> u16 {
    self.buf_ring_instance.buffer_group()
  }

  pub unsafe fn borrow_kernel_filled_buffer(
    &self,
    buffer_id: u16,
    available_len: usize,
  ) -> Result<BorrowedBuffer<BytesMut>, ZmqError> {
    unsafe {
      self
        .buf_ring_instance
        .get_buf(buffer_id, available_len)
        .ok_or_else(|| {
          ZmqError::Internal(format!(
            "Failed to borrow buffer ID {} from ring",
            buffer_id
          ))
        })
    }
  }

  /// Re-registers a buffer slot with the kernel ring after the Tokio side has finished reading
  /// a `UringInboundLease`. Uses the `get_buf(id, 0)` + immediate drop trick to call
  /// `release_borrowed_buffer` without touching any data.
  ///
  /// # Safety
  /// No other `BorrowedBuffer` guard for `buffer_id` may be live when this is called.
  pub unsafe fn reprovide_buffer(&self, buffer_id: u16) -> Result<(), ZmqError> {
    let borrowed = unsafe {
      self
        .buf_ring_instance
        .get_buf(buffer_id, 0)
        .ok_or_else(|| ZmqError::Internal(format!("reprovide_buffer: invalid id {}", buffer_id)))?
    };
    drop(borrowed); // BorrowedBuffer::drop → release_borrowed_buffer → advances ring tail
    Ok(())
  }

  /// Take the kernel-filled buffer at `buffer_id`, freeze it into an owned `Bytes`, and
  /// automatically replenish the ring slot when the `BorrowedBuffer` guard drops.
  ///
  /// # Architecture note
  /// The `io_uring_buf_ring` crate (v0.2) uses borrow semantics — it does not expose
  /// `take_buf`. This implementation therefore copies the received data into a new allocation
  /// (`Bytes::copy_from_slice`). When the crate is extended with a `take_buf` API, this
  /// function can be updated to return the kernel buffer directly (zero copies).
  ///
  /// The returned `Bytes` supports cheap sub-slicing (`Bytes::slice`), so frame parsing
  /// in the callers is already zero-copy within the returned allocation.
  ///
  /// # Safety
  /// Same contract as `borrow_kernel_filled_buffer`.
  pub unsafe fn take_and_replenish_buffer(
    &self,
    buffer_id: u16,
    available_len: usize,
  ) -> Result<Bytes, ZmqError> {
    // Borrow the kernel-filled slot.
    let borrowed = unsafe {
      self
        .buf_ring_instance
        .get_buf(buffer_id, available_len)
        .ok_or_else(|| {
          ZmqError::Internal(format!(
            "Failed to borrow buffer ID {} from ring",
            buffer_id
          ))
        })?
    };
    // Copy the received bytes into an owned allocation.
    // `BorrowedBuffer::drop` automatically re-registers the original slot with the kernel.
    let data = Bytes::copy_from_slice(&*borrowed);
    // `borrowed` drops here → slot re-offered to kernel for new incoming data.
    Ok(data)
  }
}
