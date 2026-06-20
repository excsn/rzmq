#![cfg(feature = "io-uring")]

//! In-house management of an io_uring provided-buffer ring (`IORING_REGISTER_PBUF_RING`),
//! replacing the `io_uring_buf_ring` crate.
//!
//! The crate's API only *borrowed* kernel-filled buffers, which forced
//! `BufferRingManager` to memcpy every received byte into a fresh allocation on the
//! worker thread. This module transfers **ownership** of the kernel-filled buffer to the
//! caller instead (`Bytes::from_owner`, zero copy) and swaps a recycled buffer into the
//! ring slot. Consumed buffers return to a thread-safe pool when the last `Bytes`
//! reference drops, so the steady state performs no copies and no allocations.
//!
//! Threading model: the ring (`provide`/`take`/`reprovide`) is driven exclusively by the
//! `UringWorker` OS thread — interior mutability is `Cell`/`RefCell` and the type is
//! deliberately `!Sync`. Only the [`BufferPool`] is shared across threads, because the
//! `Bytes` handed to consumers (and thus the pool-returning drop) can travel anywhere.

use std::alloc::{alloc_zeroed, dealloc, Layout};
use std::cell::{Cell, RefCell};
use std::mem::size_of;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use io_uring::types::BufRingEntry;
use io_uring::IoUring;
use parking_lot::Mutex;

use crate::ZmqError;

/// Thread-safe recycling pool for receive buffers.
///
/// Buffers are full-capacity `Vec<u8>` with `len == capacity` (initialized once at
/// allocation; the kernel overwrites contents on each receive).
pub(crate) struct BufferPool {
  free: Mutex<Vec<Vec<u8>>>,
  buffer_capacity: usize,
  max_pooled: usize,
}

impl BufferPool {
  fn new(buffer_capacity: usize, max_pooled: usize) -> Self {
    Self {
      free: Mutex::new(Vec::with_capacity(max_pooled)),
      buffer_capacity,
      max_pooled,
    }
  }

  fn acquire(&self) -> Vec<u8> {
    if let Some(buf) = self.free.lock().pop() {
      buf
    } else {
      vec![0u8; self.buffer_capacity]
    }
  }

  fn release(&self, buf: Vec<u8>) {
    debug_assert_eq!(buf.len(), self.buffer_capacity);
    let mut free = self.free.lock();
    if free.len() < self.max_pooled {
      free.push(buf);
    }
    // Over max_pooled: let the buffer deallocate.
  }
}

/// Owner type for `Bytes::from_owner`: exposes the kernel-filled prefix of a pooled
/// buffer and returns the buffer to the pool when the last `Bytes` reference drops.
struct PooledChunk {
  buf: Vec<u8>,
  filled: usize,
  pool: Arc<BufferPool>,
}

impl AsRef<[u8]> for PooledChunk {
  fn as_ref(&self) -> &[u8] {
    &self.buf[..self.filled]
  }
}

impl Drop for PooledChunk {
  fn drop(&mut self) {
    self.pool.release(std::mem::take(&mut self.buf));
  }
}

/// A registered provided-buffer ring plus the buffers currently lent to the kernel.
pub(crate) struct ProvidedBufferRing {
  bgid: u16,
  entry_count: u16,
  mask: u16,
  buffer_capacity: usize,
  /// Page-aligned array of `entry_count` ring entries, registered with the kernel.
  ring_ptr: *mut BufRingEntry,
  ring_layout: Layout,
  /// `bid -> buffer` currently lent to the kernel. `None` while the kernel has handed
  /// the buffer back via a CQE and it has not been re-provided yet (a transient state
  /// within `take`/`reprovide`).
  slots: RefCell<Vec<Option<Vec<u8>>>>,
  local_tail: Cell<u16>,
  pool: Arc<BufferPool>,
}

// SAFETY: `ring_ptr` refers to a heap allocation with no thread affinity, and all
// mutation goes through `&self` with single-thread interior mutability (`Cell`/`RefCell`
// keep the type `!Sync`, which is what actually matters). Moving the struct to another
// thread is sound; it is only ever owned and driven by the UringWorker thread.
unsafe impl Send for ProvidedBufferRing {}

impl ProvidedBufferRing {
  /// Allocates the ring memory, registers it with the kernel under `bgid`, and provides
  /// `entry_count` (requested rounded up to a power of two) freshly pooled buffers.
  pub(crate) fn new(
    ring: &IoUring,
    requested_entries: u16,
    bgid: u16,
    buffer_capacity: usize,
  ) -> Result<Self, ZmqError> {
    if requested_entries == 0 || buffer_capacity == 0 {
      return Err(ZmqError::Internal(
        "ProvidedBufferRing: entries and buffer capacity must be non-zero".into(),
      ));
    }
    let entry_count = requested_entries
      .checked_next_power_of_two()
      .ok_or_else(|| {
        ZmqError::Internal(format!(
          "ProvidedBufferRing: entry count {} not representable as a power of two",
          requested_entries
        ))
      })?;

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
    let ring_layout =
      Layout::from_size_align(entry_count as usize * size_of::<BufRingEntry>(), page_size)
        .map_err(|e| ZmqError::Internal(format!("ProvidedBufferRing: ring layout: {}", e)))?;
    // Zeroed allocation also initializes the ring tail (entry 0's resv field) to 0,
    // as the kernel expects at registration time.
    let ring_ptr = unsafe { alloc_zeroed(ring_layout) } as *mut BufRingEntry;
    if ring_ptr.is_null() {
      return Err(ZmqError::Internal(
        "ProvidedBufferRing: ring memory allocation failed".into(),
      ));
    }

    // SAFETY: ring_ptr/entry_count describe a valid, page-aligned allocation that stays
    // alive until `Drop`, which runs after the io_uring fd is closed (see Drop notes).
    if let Err(e) = unsafe {
      ring
        .submitter()
        .register_buf_ring_with_flags(ring_ptr as u64, entry_count, bgid, 0)
    } {
      unsafe { dealloc(ring_ptr as *mut u8, ring_layout) };
      return Err(ZmqError::Internal(format!(
        "ProvidedBufferRing: register_buf_ring (bgid {}, entries {}) failed: {}",
        bgid, entry_count, e
      )));
    }

    // Pool headroom above entry_count covers buffers in flight to consumers while the
    // ring is fully re-provided.
    let pool = Arc::new(BufferPool::new(buffer_capacity, entry_count as usize * 2));

    let this = Self {
      bgid,
      entry_count,
      mask: entry_count - 1,
      buffer_capacity,
      ring_ptr,
      ring_layout,
      slots: RefCell::new((0..entry_count).map(|_| None).collect()),
      local_tail: Cell::new(0),
      pool,
    };

    for bid in 0..entry_count {
      let buf = this.pool.acquire();
      this.provide(bid, buf);
    }

    Ok(this)
  }

  pub(crate) fn group_id(&self) -> u16 {
    self.bgid
  }

  /// Lends `buf` to the kernel under `bid`: writes the next ring entry and publishes it
  /// with a release store on the shared tail.
  fn provide(&self, bid: u16, mut buf: Vec<u8>) {
    debug_assert_eq!(buf.len(), self.buffer_capacity);
    let tail = self.local_tail.get();
    let idx = (tail & self.mask) as usize;
    // SAFETY: idx < entry_count, so the entry pointer is within our allocation. The
    // setters do not touch entry 0's resv field (the shared tail).
    unsafe {
      let entry = &mut *self.ring_ptr.add(idx);
      entry.set_addr(buf.as_mut_ptr() as u64);
      entry.set_len(self.buffer_capacity as u32);
      entry.set_bid(bid);
    }
    self.slots.borrow_mut()[bid as usize] = Some(buf);
    let new_tail = tail.wrapping_add(1);
    self.local_tail.set(new_tail);
    // SAFETY: ring_ptr is the valid first entry of the registered ring; the tail field
    // is shared with the kernel and must be published with release ordering.
    unsafe {
      let tail_ptr = BufRingEntry::tail(self.ring_ptr as *const BufRingEntry) as *const AtomicU16;
      (*tail_ptr).store(new_tail, Ordering::Release);
    }
  }

  /// Takes ownership of the kernel-filled buffer `bid` (zero copy) and immediately
  /// re-provides the slot with a recycled buffer. The returned `Bytes` exposes the
  /// first `filled` bytes and returns the buffer to the pool on final drop.
  pub(crate) fn take(&self, bid: u16, filled: usize) -> Result<Bytes, ZmqError> {
    if filled > self.buffer_capacity {
      return Err(ZmqError::Internal(format!(
        "ProvidedBufferRing: CQE length {} exceeds buffer capacity {}",
        filled, self.buffer_capacity
      )));
    }
    let buf = self
      .slots
      .borrow_mut()
      .get_mut(bid as usize)
      .and_then(|slot| slot.take())
      .ok_or_else(|| {
        ZmqError::Internal(format!(
          "ProvidedBufferRing: buffer id {} is not currently kernel-owned",
          bid
        ))
      })?;
    self.provide(bid, self.pool.acquire());
    let chunk = PooledChunk {
      buf,
      filled,
      pool: Arc::clone(&self.pool),
    };
    Ok(Bytes::from_owner(chunk))
  }

  /// Unregisters the buffer ring from the kernel. **Must** be called before this
  /// struct is dropped while the `IoUring` is still alive — `Drop` frees the ring
  /// memory unconditionally and the kernel must no longer reference it by then.
  pub(crate) fn unregister(&self, ring: &IoUring) {
    if let Err(e) = ring.submitter().unregister_buf_ring(self.bgid) {
      tracing::warn!(
        bgid = self.bgid,
        "ProvidedBufferRing: unregister_buf_ring failed: {}",
        e
      );
    }
  }

  /// Returns the (unconsumed) buffer `bid` straight back to the kernel — used when a
  /// CQE selected a buffer but carried no data (EOF).
  pub(crate) fn reprovide(&self, bid: u16) -> Result<(), ZmqError> {
    let buf = self
      .slots
      .borrow_mut()
      .get_mut(bid as usize)
      .and_then(|slot| slot.take())
      .ok_or_else(|| {
        ZmqError::Internal(format!(
          "ProvidedBufferRing: buffer id {} is not currently kernel-owned",
          bid
        ))
      })?;
    self.provide(bid, buf);
    Ok(())
  }
}

impl Drop for ProvidedBufferRing {
  fn drop(&mut self) {
    // Safety contract: the kernel must no longer reference this memory — either
    // `unregister` was called (worker CleaningUp phase) or the io_uring fd is
    // already closed. Both teardown paths satisfy this before the drop runs.
    unsafe { dealloc(self.ring_ptr as *mut u8, self.ring_layout) };
  }
}

impl std::fmt::Debug for ProvidedBufferRing {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("ProvidedBufferRing")
      .field("bgid", &self.bgid)
      .field("entry_count", &self.entry_count)
      .field("buffer_capacity", &self.buffer_capacity)
      .finish_non_exhaustive()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use io_uring::{cqueue, opcode, squeue, types};
  use std::os::unix::io::RawFd;

  // ── BufferPool ──────────────────────────────────────────────────────────────

  #[test]
  fn pool_allocates_full_capacity_buffers() {
    let pool = BufferPool::new(1024, 2);
    let buf = pool.acquire();
    assert_eq!(buf.len(), 1024);
    assert!(buf.iter().all(|&b| b == 0));
  }

  #[test]
  fn pool_recycles_and_respects_cap() {
    let pool = BufferPool::new(64, 2);
    let a = pool.acquire();
    let b = pool.acquire();
    let c = pool.acquire();
    pool.release(a);
    pool.release(b);
    pool.release(c); // beyond max_pooled — must be dropped, not pooled
    assert_eq!(pool.free.lock().len(), 2);
    let _ = pool.acquire();
    assert_eq!(pool.free.lock().len(), 1);
  }

  // ── PooledChunk / Bytes::from_owner ────────────────────────────────────────

  #[test]
  fn pooled_chunk_returns_buffer_only_after_last_bytes_ref_drops() {
    let pool = Arc::new(BufferPool::new(64, 4));
    let mut buf = pool.acquire();
    buf[..5].copy_from_slice(b"hello");
    let bytes = Bytes::from_owner(PooledChunk {
      buf,
      filled: 5,
      pool: Arc::clone(&pool),
    });
    assert_eq!(&bytes[..], b"hello");

    // A sub-slice (as held by a parsed Msg) keeps the buffer alive.
    let slice = bytes.slice(1..4);
    drop(bytes);
    assert_eq!(
      pool.free.lock().len(),
      0,
      "buffer must not return while a slice lives"
    );
    assert_eq!(&slice[..], b"ell");
    drop(slice);
    assert_eq!(
      pool.free.lock().len(),
      1,
      "buffer must return to the pool on final drop"
    );
  }

  #[test]
  fn pooled_chunk_exposes_only_filled_prefix() {
    let pool = Arc::new(BufferPool::new(64, 4));
    let bytes = Bytes::from_owner(PooledChunk {
      buf: pool.acquire(),
      filled: 7,
      pool: Arc::clone(&pool),
    });
    assert_eq!(bytes.len(), 7);
  }

  // ── ProvidedBufferRing bookkeeping (requires a real io_uring instance) ─────

  fn test_ring() -> IoUring {
    IoUring::new(8).expect("io_uring creation failed (requires Linux >= 5.19)")
  }

  #[test]
  fn ring_registers_and_provides_all_buffers() {
    let ring = test_ring();
    let pbr = ProvidedBufferRing::new(&ring, 4, 7, 4096).expect("ring init");
    assert_eq!(pbr.group_id(), 7);
    assert_eq!(pbr.local_tail.get(), 4);
    assert!(pbr.slots.borrow().iter().all(|s| s.is_some()));
    pbr.unregister(&ring);
  }

  #[test]
  fn ring_rounds_entries_up_to_power_of_two() {
    let ring = test_ring();
    let pbr = ProvidedBufferRing::new(&ring, 3, 1, 1024).expect("ring init");
    assert_eq!(pbr.entry_count, 4);
    assert_eq!(pbr.slots.borrow().len(), 4);
    pbr.unregister(&ring);
  }

  // Note: the bookkeeping tests below call take/reprovide without the kernel having
  // actually consumed a buffer (no ops are submitted), which over-provides the ring.
  // That is harmless here — with no in-flight ops the kernel never reads the entries —
  // and lets the slot/tail accounting be tested in isolation. The end-to-end test
  // below exercises the real consume-then-replenish protocol.

  #[test]
  fn take_replenishes_slot_and_rejects_bad_args() {
    let ring = test_ring();
    let pbr = ProvidedBufferRing::new(&ring, 2, 2, 256).expect("ring init");

    let tail_before = pbr.local_tail.get();
    let bytes = pbr.take(1, 100).expect("take");
    assert_eq!(bytes.len(), 100);
    // Slot was immediately re-provided with a fresh buffer and the tail advanced.
    assert!(pbr.slots.borrow()[1].is_some());
    assert_eq!(pbr.local_tail.get(), tail_before.wrapping_add(1));

    // Out-of-range bid and oversized CQE length must error, not panic.
    assert!(pbr.take(9, 1).is_err());
    assert!(pbr.take(0, 257).is_err());
    pbr.unregister(&ring);
  }

  #[test]
  fn reprovide_requires_owned_slot() {
    let ring = test_ring();
    let pbr = ProvidedBufferRing::new(&ring, 2, 3, 256).expect("ring init");
    pbr.reprovide(0).expect("reprovide of provided slot");
    assert!(pbr.reprovide(9).is_err());
    pbr.unregister(&ring);
  }

  // ── End-to-end: kernel fills provided buffers through a socketpair ─────────

  fn socketpair() -> (RawFd, RawFd) {
    let mut fds = [0 as RawFd; 2];
    let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
    assert_eq!(rc, 0, "socketpair failed");
    (fds[0], fds[1])
  }

  /// Drives enough buffer-select reads through a real kernel to cycle the ring
  /// several times, exercising wraparound indexing, replenishment, and recycling.
  #[test]
  fn kernel_read_cycles_through_ring_with_wraparound() {
    const BGID: u16 = 11;
    const ENTRIES: u16 = 2;
    const CAPACITY: usize = 256;

    let mut ring = test_ring();
    let pbr = ProvidedBufferRing::new(&ring, ENTRIES, BGID, CAPACITY).expect("ring init");
    let (write_fd, read_fd) = socketpair();

    // 3 full cycles of the ring.
    for round in 0u8..(ENTRIES as u8 * 3) {
      let payload = [round; 64];
      let written = unsafe {
        libc::write(
          write_fd,
          payload.as_ptr() as *const libc::c_void,
          payload.len(),
        )
      };
      assert_eq!(written, payload.len() as isize);

      let read_sqe = opcode::Recv::new(types::Fd(read_fd), std::ptr::null_mut(), CAPACITY as u32)
        .buf_group(BGID)
        .build()
        .flags(squeue::Flags::BUFFER_SELECT)
        .user_data(round as u64);
      unsafe { ring.submission().push(&read_sqe).expect("push sqe") };
      ring.submit_and_wait(1).expect("submit_and_wait");

      let cqe = ring.completion().next().expect("cqe");
      assert_eq!(cqe.user_data(), round as u64);
      assert!(cqe.result() > 0, "read failed: {}", cqe.result());
      let bid = cqueue::buffer_select(cqe.flags()).expect("CQE must carry a buffer id");
      let bytes = pbr.take(bid, cqe.result() as usize).expect("take");
      assert_eq!(bytes.len(), payload.len());
      assert!(
        bytes.iter().all(|&b| b == round),
        "payload mismatch in round {}",
        round
      );
      // bytes drops here → buffer returns to the pool for the next rounds.
    }

    // After full cycles with prompt drops, the pool should be recycling rather
    // than growing: at most entry_count buffers were in flight at once.
    assert!(pbr.pool.free.lock().len() <= ENTRIES as usize * 2);

    unsafe {
      libc::close(write_fd);
      libc::close(read_fd);
    }
    pbr.unregister(&ring);
  }
}
