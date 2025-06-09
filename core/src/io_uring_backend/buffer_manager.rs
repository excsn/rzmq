// core/src/io_uring_backend/buffer_manager.rs

#![cfg(feature = "io-uring")]

use crate::ZmqError;
use bytes::BytesMut;
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
      // If `ring_entries` was stored in BufferRingManager upon creation:
      // .field("ring_entries", &self.stored_ring_entries_count)
      .finish_non_exhaustive()
  }
}

impl BufferRingManager {
  pub fn new(ring: &IoUring, ring_entries: u16, bgid: u16, buffer_capacity: usize) -> Result<Self, ZmqError> {
    tracing::info!(
      "Initializing BufferRingManager with bgid: {}, requested_entries: {}, capacity_per_buffer: {}",
      bgid,
      ring_entries,
      buffer_capacity
    );

    // Create an iterator that yields `ring_entries` number of BytesMut buffers.
    let bufs_iter = (0..ring_entries).map(|_| BytesMut::with_capacity(buffer_capacity));

    // `IoUringBufRing::new_with_buffers` takes ownership of the iterator.
    // The actual number of entries in the ring will be `bufs_iter.count().next_power_of_two()`.
    // If `ring_entries` is already a power of two, it will be that.
    match IoUringBufRing::new_with_buffers(ring, bufs_iter, bgid) {
      Ok(buf_ring_instance) => {
        tracing::info!(
          "BufferRingManager: IoUringBufRing built and registered successfully for bgid: {}",
          bgid
        );
        Ok(Self { buf_ring_instance })
      }
      Err(e) => {
        tracing::error!("Failed to build and register IoUringBufRing: {:?}", e);
        Err(ZmqError::Internal(format!("BufferRingManager build failed: {:?}", e)))
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
    self
      .buf_ring_instance
      .get_buf(buffer_id, available_len)
      .ok_or_else(|| ZmqError::Internal(format!("Failed to borrow buffer ID {} from ring", buffer_id)))
  }
}
