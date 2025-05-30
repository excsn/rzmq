// core/src/io_uring_backend/buffer_manager.rs

#![cfg(feature = "io-uring")]

use crate::ZmqError;
use io_uring::IoUring;
use io_uring_buf_ring::{IoUringBufRing, BorrowedBuffer, Buffer as IoUringBufferTrait};
use std::fmt;
use std::sync::Arc;
use bytes::BytesMut;

/// Manages a ring of buffers registered with io_uring for efficient receives.
pub struct BufferRingManager {
  buf_ring_instance: IoUringBufRing<BytesMut>, 
}

impl fmt::Debug for BufferRingManager {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    // <<< MODIFIED START: Get ring_entries from buf_ring_mmap.len() via IoUringBufRing internals if accessible,
    // otherwise just show bgid or a placeholder.
    // Accessing buf_ring_mmap directly is not possible as it's ManuallyDrop<UnsafeCell<...>>.
    // The public API of IoUringBufRing doesn't seem to expose the number of entries directly.
    // The `mask()` was private. Let's stick to what's clearly public or inferable.
    // We know `ring_entries` was passed to `new_with_buffers`, but it's not stored directly on `Self`.
    // We can print the buffer_group id, which is public.
    // For the number of entries, we might have to omit it or store it separately during construction if needed for debug.
    // For now, let's just print the `bgid`.
    // >>>
    f.debug_struct("BufferRingManager")
      .field("bgid", &self.buf_ring_instance.buffer_group())
      // If `ring_entries` was stored in BufferRingManager upon creation:
      // .field("ring_entries", &self.stored_ring_entries_count) 
      .finish_non_exhaustive()
  }
}
// <<< MODIFIED END >>>

impl BufferRingManager {
  // <<< MODIFIED START: Consider storing ring_entries if needed for Debug or other logic >>>
  // If we want to debug the number of entries, we might need to store it.
  // For now, the constructor takes `ring_entries` but `IoUringBufRing` might adjust it
  // to the next power of two internally if `new_with_buffers` is used and the iterator length
  // isn't a power of two. The true number of slots is `mask + 1`.
  // The IoUringBufRing::new (not new_with_buffers) takes `ring_entries` and ensures it's a power of two.
  // If using `new_with_buffers`, the effective number of entries is `bufs.len().next_power_of_two()`.

  // Let's assume for now that the `ring_entries` passed to `new` is what we care about conceptually,
  // even if `IoUringBufRing` might slightly adjust the internal mmap size.
  // The `IoUringBufRing` itself doesn't seem to publicly expose its effective entry count easily
  // without accessing private members or the mask.
  // So, for the Debug impl, we'll stick to publicly accessible info like `bgid`.
  // >>>
  pub fn new(
    ring: &IoUring,
    ring_entries: u16, 
    bgid: u16,
    buffer_capacity: usize,
  ) -> Result<Self, ZmqError> {
    tracing::info!(
      "Initializing BufferRingManager with bgid: {}, requested_entries: {}, capacity_per_buffer: {}",
      bgid, ring_entries, buffer_capacity
    );

    // Create an iterator that yields `ring_entries` number of BytesMut buffers.
    let bufs_iter = (0..ring_entries).map(|_| BytesMut::with_capacity(buffer_capacity));
    
    // `IoUringBufRing::new_with_buffers` takes ownership of the iterator.
    // The actual number of entries in the ring will be `bufs_iter.count().next_power_of_two()`.
    // If `ring_entries` is already a power of two, it will be that.
    match IoUringBufRing::new_with_buffers(ring, bufs_iter, bgid) {
      Ok(buf_ring_instance) => {
        tracing::info!("BufferRingManager: IoUringBufRing built and registered successfully for bgid: {}", bgid);
        Ok(Self {
          buf_ring_instance,
        })
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
    self.buf_ring_instance
      .get_buf(buffer_id, available_len) 
      .ok_or_else(|| ZmqError::Internal(format!("Failed to borrow buffer ID {} from ring", buffer_id)))
  }
}