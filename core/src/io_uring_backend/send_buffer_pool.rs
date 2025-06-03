#![cfg(feature = "io-uring")]

use crate::ZmqError;
use bytes::Bytes;
use io_uring::IoUring;
use libc;
use parking_lot::Mutex;
use std::collections::VecDeque;
use tracing::{error, info, trace, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct RegisteredSendBufferId(u16);

#[derive(Debug)]
struct SendBufferSlot {
  id: RegisteredSendBufferId,
  buffer: Vec<u8>,     // Owns the memory for the buffer
  in_kernel_use: bool, // True if SEND_ZC submitted, awaiting F_NOTIFY
}

impl SendBufferSlot {
  fn ptr(&self) -> *const u8 {
    self.buffer.as_ptr()
  }
  fn capacity(&self) -> usize {
    self.buffer.capacity()
  }
  fn as_mut_slice(&mut self) -> &mut [u8] {
    &mut self.buffer
  }
  fn iovec(&self) -> libc::iovec {
    libc::iovec {
      iov_base: self.buffer.as_ptr() as *mut libc::c_void,
      iov_len: self.buffer.len(), // For registration, use the full allocated length
    }
  }
}

#[derive(Debug)]
struct SendBufferPoolInner {
  pool: Vec<SendBufferSlot>,
  free_ids: VecDeque<RegisteredSendBufferId>,
}

#[derive(Debug)]
pub(crate) struct SendBufferPool {
  inner: Mutex<SendBufferPoolInner>,
}

impl SendBufferPool {
  pub fn new(ring: &IoUring, count: usize, capacity_per_buffer: usize) -> Result<Self, ZmqError> {
    if count == 0 || capacity_per_buffer == 0 {
      warn!("SendBufferPool initialized with zero count or capacity. Zero-copy send will be effectively disabled.");
      return Ok(Self {
        inner: Mutex::new(SendBufferPoolInner {
          pool: Vec::new(),
          free_ids: VecDeque::new(),
        }),
      });
    }

    // TODO: Consider integrating RLIMIT_MEMLOCK check here or ensuring UringWorker does it.
    // If count * capacity_per_buffer > memlock_limit, adjust or error.
    // For now, assume parameters are valid.

    let mut slots = Vec::with_capacity(count);
    let mut free_ids = VecDeque::with_capacity(count);

    for i in 0..count {
      slots.push(SendBufferSlot {
        id: RegisteredSendBufferId(i as u16),
        buffer: vec![0u8; capacity_per_buffer],
        in_kernel_use: false,
      });
      free_ids.push_back(RegisteredSendBufferId(i as u16));
    }

    let iovecs_to_register: Vec<libc::iovec> = slots.iter().map(|slot| slot.iovec()).collect();

    unsafe {
      ring.submitter().register_buffers(&iovecs_to_register).map_err(|e| {
        error!("SendBufferPool: Failed to register_buffers: {}", e);
        ZmqError::Internal(format!("SendBufferPool: Failed to register_buffers: {}", e))
      })?;
    }

    info!(
      "SendBufferPool: Registered {} send buffers ({} bytes each).",
      count, capacity_per_buffer
    );
    Ok(Self {
      inner: Mutex::new(SendBufferPoolInner { pool: slots, free_ids }),
    })
  }

  /// Attempts to acquire a free buffer, copies `data_to_copy` into it,
  /// and marks it as in kernel use.
  /// Returns the buffer's ID, a pointer to its data, and the length of data copied.
  pub fn acquire_and_prep_buffer(&self, data_to_copy: &Bytes) -> Option<(RegisteredSendBufferId, *const u8, u32)> {
    if data_to_copy.is_empty() {
      trace!("SendBufferPool: acquire_and_prep_buffer called with empty data, skipping.");
      return None; // Cannot SEND_ZC empty data
    }

    let mut inner_guard = self.inner.lock();
    if inner_guard.pool.is_empty() {
      trace!("SendBufferPool: No buffers configured in the pool.");
      return None;
    }

    if let Some(buffer_id) = inner_guard.free_ids.pop_front() {
      let slot = &mut inner_guard.pool[buffer_id.0 as usize];

      if data_to_copy.len() > slot.capacity() {
        warn!(
          "Data ({} bytes) too large for send buffer slot {:?} (capacity {} bytes). Cannot use zero-copy for this send.",
          data_to_copy.len(),
          buffer_id,
          slot.capacity()
        );
        inner_guard.free_ids.push_front(buffer_id); // Put it back, it's still free
        return None;
      }

      // Copy data into the registered buffer
      slot.as_mut_slice()[..data_to_copy.len()].copy_from_slice(data_to_copy);
      slot.in_kernel_use = true; // Mark as given to kernel

      trace!(
        "SendBufferPool: Acquired buffer {:?} for {} bytes.",
        buffer_id,
        data_to_copy.len()
      );
      Some((buffer_id, slot.ptr(), data_to_copy.len() as u32))
    } else {
      trace!("SendBufferPool: No free send buffers available.");
      None // No free buffers
    }
  }

  /// Releases a buffer, making it available for reuse.
  /// Called after the kernel signals it's done (e.g., via SEND_ZC F_NOTIFY CQE).
  pub fn release_buffer(&self, id: RegisteredSendBufferId) {
    let mut inner_guard = self.inner.lock();
    if let Some(slot_index) = inner_guard.pool.iter().position(|s| s.id == id) {
      let slot = &mut inner_guard.pool[slot_index];
      if slot.in_kernel_use {
        slot.in_kernel_use = false;
        // Check if it's already in free_ids to prevent duplicates, though ideally it shouldn't be.
        if !inner_guard.free_ids.contains(&id) {
          inner_guard.free_ids.push_back(id);
        } else {
          warn!("SendBufferPool: Buffer ID {:?} was already in free_ids when trying to release (in_kernel_use was true). State may be inconsistent.", id);
        }
        trace!("SendBufferPool: Released buffer {:?}.", id);
      } else {
        warn!(
          "SendBufferPool: Attempted to release buffer {:?} which was not marked as in_kernel_use.",
          id
        );
        // If it wasn't marked in_kernel_use, it should ideally already be in free_ids or never taken.
        // Adding it defensively if not present.
        if !inner_guard.free_ids.contains(&id) {
          inner_guard.free_ids.push_back(id);
        }
      }
    } else {
      error!("SendBufferPool: Attempted to release an unknown buffer ID: {:?}", id);
    }
  }

  /// Unregisters all buffers from io_uring. Called on UringWorker shutdown.
  pub unsafe fn unregister_all(&self, ring: &IoUring) -> Result<(), ZmqError> {
    let inner_guard = self.inner.lock(); // Ensure no modifications during unregister
    if inner_guard.pool.is_empty() {
      return Ok(());
    }
    // It's important that no operations are pending on these buffers when unregistering.
    // The UringWorker should ensure all SEND_ZC ops are complete (notifications received).
    drop(inner_guard); // Release lock

    info!("SendBufferPool: Unregistering all send buffers.");
    ring.submitter().unregister_buffers().map_err(|e| {
      error!("SendBufferPool: Failed to unregister_buffers: {}", e);
      ZmqError::Internal(format!("SendBufferPool: Failed to unregister_buffers: {}", e))
    })
  }
}
