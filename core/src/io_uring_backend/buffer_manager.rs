#![cfg(feature = "io-uring")]

use crate::io_uring_backend::provided_buffer_ring::ProvidedBufferRing;
use crate::ZmqError;
use bytes::Bytes;
use io_uring::IoUring;
use std::fmt;

/// Manages a ring of buffers registered with io_uring for efficient receives.
///
/// Thin facade over [`ProvidedBufferRing`], which owns the kernel registration and the
/// recycling pool. Unlike the previous `io_uring_buf_ring`-based implementation, taking
/// a kernel-filled buffer transfers ownership (zero copy) instead of copying it.
pub struct BufferRingManager {
  ring: ProvidedBufferRing,
}

impl fmt::Debug for BufferRingManager {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("BufferRingManager")
      .field("ring", &self.ring)
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
    let ring = ProvidedBufferRing::new(ring, ring_entries, bgid, buffer_capacity)?;
    tracing::info!(
      "BufferRingManager: provided-buffer ring registered successfully for bgid: {}",
      bgid
    );
    Ok(Self { ring })
  }

  pub fn group_id(&self) -> u16 {
    self.ring.group_id()
  }

  /// Unregisters the ring from the kernel. Must be called before dropping the manager
  /// while the `IoUring` instance is still alive (worker CleaningUp phase).
  pub fn unregister(&self, ring: &IoUring) {
    self.ring.unregister(ring);
  }

  /// Re-registers a buffer slot with the kernel ring after a CQE selected the buffer
  /// but delivered no data (EOF sentinel).
  pub fn reprovide_buffer(&self, buffer_id: u16) -> Result<(), ZmqError> {
    self.ring.reprovide(buffer_id)
  }

  /// Takes ownership of the kernel-filled buffer at `buffer_id` — **zero copy** — and
  /// immediately replenishes the ring slot with a recycled buffer.
  ///
  /// The returned `Bytes` exposes the `available_len` kernel-filled bytes; when its
  /// last reference (including `Bytes::slice` children held by parsed `Msg`s) drops,
  /// the underlying buffer returns to the pool for reuse.
  pub fn take_and_replenish_buffer(
    &self,
    buffer_id: u16,
    available_len: usize,
  ) -> Result<Bytes, ZmqError> {
    self.ring.take(buffer_id, available_len)
  }
}
