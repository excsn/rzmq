#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::send_buffer_pool::RegisteredSendBufferId;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::os::unix::io::RawFd;

/// Simple, protocol-agnostic scatter-gather batch for raw vectored writes.
///
/// Unlike `PinnedBatchWrite`, this has no ZMTP-specific fields (`header_slab`, `app_op_name`).
///
/// Memory safety: `iovecs` is a heap-allocated Box whose address is stable through any
/// HashMap resize. `iov_base` pointers inside each iovec reference `payloads` entries,
/// which are kept alive for the entire SQE→CQE window by this struct's ownership.
/// Only the worker thread accesses this during the SQE→CQE window.
pub(crate) struct PinnedEgressBatch {
  pub iovecs: Box<[libc::iovec]>,
  pub payloads: Vec<Bytes>,
  pub total_len: usize,
  pub send_op_flags: i32,
}

// SAFETY: iovec contains raw pointers into payloads (owned by this struct).
// Worker thread is the sole accessor during SQE→CQE window.
unsafe impl Send for PinnedEgressBatch {}

impl std::fmt::Debug for PinnedEgressBatch {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("PinnedEgressBatch")
      .field("iovec_count", &self.iovecs.len())
      .field("total_len", &self.total_len)
      .finish()
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum InternalOpType {
  RingRead,
  Send,
  SendZeroCopy,
  CloseFd,
  GenericHandlerOp,
  EventFdPoll,
  RingReadMultishot,
  AsyncCancel,
  /// Protocol-agnostic writev for raw vectored egress.
  SendRawVectored,
  /// `SEND_ZC` from a pre-registered, pre-written buffer slot (Phase 3 zero-copy lease path).
  SendZeroCopyLeased,
  /// One-shot poll for the peer closing, armed by handlers that keep no read posted.
  PeerClosePoll,
}

/// Payload associated with an internal operation, e.g., buffer for send.
#[derive(Debug)]
pub(crate) enum InternalOpPayload {
  None,
  SendBuffer {
    buffer: Bytes,
    send_op_flags: i32,
    app_op_ud: Option<UserData>,
    app_op_name: Option<String>,
  },
  CancelTarget,
  SendZeroCopy {
    send_buf_id: RegisteredSendBufferId,
    original_data: Bytes,
    send_op_flags: i32,
    app_op_ud: UserData,
    app_op_name: String,
  },
  /// Protocol-agnostic scatter-gather batch for raw vectored egress.
  RawVectored(PinnedEgressBatch),
  /// Leased pre-registered buffer slot — only the buffer ID is needed for release on completion.
  SendZeroCopyLeased {
    send_buf_id: RegisteredSendBufferId,
  },
}

impl Default for InternalOpPayload {
  fn default() -> Self {
    InternalOpPayload::None
  }
}

/// Details stored for an in-flight internal operation.
#[derive(Debug)]
pub(crate) struct InternalOpDetails {
  pub fd: RawFd,
  pub op_type: InternalOpType,
  pub payload: InternalOpPayload,
}

/// Internal op user_data IDs are offset by this constant so they never overlap with
/// external (app-visible) op IDs, which are allocated from the range 1..INTERNAL_OP_BASE.
const INTERNAL_OP_BASE: u64 = 1_000_000_000;
const SLOT_BITS: u32 = 32;
const SLOT_MASK: u64 = (1 << SLOT_BITS) - 1;

/// A tracked op and the generation its `user_data` carries. A slab slot is
/// reused once freed, so the generation is what tells a late CQE of an earlier
/// op in the same slot from the op that holds it now.
#[derive(Debug)]
pub(crate) struct TrackedOp {
  generation: u32,
  pub(crate) details: InternalOpDetails,
}

#[derive(Debug)]
pub(crate) struct InternalOpTracker {
  /// Hot path: O(1) direct-index lookup via the slot bits of `user_data`.
  pub(crate) op_to_details: slab::Slab<TrackedOp>,
  /// Cold path: holds entries for in-flight SEND_ZC operations awaiting a second
  /// notification CQE with the same `user_data`. Typically empty; bounded by active ZC sends.
  pending_notifications: HashMap<UserData, InternalOpDetails>,
  /// Ops still in the kernel whose fd has been closed. Their CQEs only release
  /// what the op holds; the fd number may already belong to a new connection.
  orphaned: HashSet<UserData>,
  next_generation: u32,
}

impl InternalOpTracker {
  pub fn new() -> Self {
    Self {
      op_to_details: slab::Slab::new(),
      pending_notifications: HashMap::new(),
      orphaned: HashSet::new(),
      next_generation: 0,
    }
  }

  fn encode(slot: usize, generation: u32) -> UserData {
    INTERNAL_OP_BASE + (((generation as u64) << SLOT_BITS) | slot as u64)
  }

  /// The slab slot of `user_data` if that slot still holds the op it names.
  fn live_slot(&self, user_data: UserData) -> Option<usize> {
    if user_data < INTERNAL_OP_BASE {
      return None;
    }
    let raw = user_data - INTERNAL_OP_BASE;
    let slot = (raw & SLOT_MASK) as usize;
    let generation = (raw >> SLOT_BITS) as u32;
    self
      .op_to_details
      .get(slot)
      .filter(|op| op.generation == generation)
      .map(|_| slot)
  }

  /// Allocates a new `UserData` for an internal operation, stores its details, and returns the ID.
  pub fn new_op_id(
    &mut self,
    fd: RawFd,
    op_type: InternalOpType,
    payload: InternalOpPayload,
  ) -> UserData {
    let generation = self.next_generation;
    self.next_generation = self.next_generation.wrapping_add(1);
    let slot = self.op_to_details.insert(TrackedOp {
      generation,
      details: InternalOpDetails {
        fd,
        op_type,
        payload,
      },
    });
    Self::encode(slot, generation)
  }

  pub fn take_op_details(&mut self, user_data: UserData) -> Option<InternalOpDetails> {
    if !self.orphaned.is_empty() {
      self.orphaned.remove(&user_data);
    }
    match self.live_slot(user_data) {
      Some(slot) => Some(self.op_to_details.remove(slot).details),
      None => self.pending_notifications.remove(&user_data),
    }
  }

  #[allow(dead_code)]
  pub fn get_op_details(&self, user_data: UserData) -> Option<&InternalOpDetails> {
    match self.live_slot(user_data) {
      Some(slot) => Some(&self.op_to_details[slot].details),
      None => self.pending_notifications.get(&user_data),
    }
  }

  /// Re-registers details for a SEND_ZC op that expects a second notification CQE with the
  /// same `user_data`. The entry is moved to the secondary map so `take_op_details` can find
  /// it when the kernel delivers the `IORING_CQE_F_NOTIF` completion.
  pub fn reinsert_for_notification(&mut self, user_data: UserData, details: InternalOpDetails) {
    self.pending_notifications.insert(user_data, details);
  }

  pub fn is_empty(&self) -> bool {
    self.op_to_details.is_empty() && self.pending_notifications.is_empty()
  }

  /// Returns all tracked `UserData` IDs (slab + notification map). Used for bulk cancellation
  /// during shutdown.
  pub fn all_op_ids(&self) -> Vec<UserData> {
    self
      .op_to_details
      .iter()
      .map(|(slot, op)| Self::encode(slot, op.generation))
      .chain(self.pending_notifications.keys().copied())
      .collect()
  }

  pub fn is_orphaned(&self, user_data: UserData) -> bool {
    !self.orphaned.is_empty() && self.orphaned.contains(&user_data)
  }

  /// Marks every op still tracked for `fd` as orphaned and returns their types.
  /// Each keeps its `user_data` and payload until its final CQE.
  pub fn orphan_ops_for_fd(&mut self, fd: RawFd) -> Vec<InternalOpType> {
    let mut orphaned_types = Vec::new();
    for (slot, op) in self.op_to_details.iter() {
      if op.details.fd == fd {
        let user_data = Self::encode(slot, op.generation);
        if self.orphaned.insert(user_data) {
          orphaned_types.push(op.details.op_type);
        }
      }
    }
    for (user_data, details) in self.pending_notifications.iter() {
      if details.fd == fd && self.orphaned.insert(*user_data) {
        orphaned_types.push(details.op_type);
      }
    }
    orphaned_types
  }

  pub(crate) fn has_pending_read_op(&self, fd_to_check: RawFd) -> bool {
    // pending_notifications holds ZC send continuations, never reads
    self.op_to_details.iter().any(|(slot, op)| {
      op.details.fd == fd_to_check
        && matches!(
          op.details.op_type,
          InternalOpType::RingRead | InternalOpType::RingReadMultishot
        )
        && !self.is_orphaned(Self::encode(slot, op.generation))
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn a_late_cqe_of_a_freed_op_does_not_reach_the_op_reusing_its_slot() {
    let mut tracker = InternalOpTracker::new();
    let first = tracker.new_op_id(7, InternalOpType::SendRawVectored, InternalOpPayload::None);
    assert!(tracker.take_op_details(first).is_some());

    let second = tracker.new_op_id(9, InternalOpType::EventFdPoll, InternalOpPayload::None);
    assert_ne!(first, second);
    assert!(tracker.get_op_details(first).is_none());
    assert!(tracker.take_op_details(first).is_none());
    assert_eq!(tracker.take_op_details(second).map(|d| d.fd), Some(9));
  }

  #[test]
  fn an_orphaned_op_keeps_its_user_data_until_its_final_cqe() {
    let mut tracker = InternalOpTracker::new();
    let write = tracker.new_op_id(7, InternalOpType::SendRawVectored, InternalOpPayload::None);
    let read = tracker.new_op_id(7, InternalOpType::RingReadMultishot, InternalOpPayload::None);
    let other = tracker.new_op_id(8, InternalOpType::RingRead, InternalOpPayload::None);

    assert_eq!(
      tracker.orphan_ops_for_fd(7),
      vec![InternalOpType::SendRawVectored, InternalOpType::RingReadMultishot]
    );
    assert!(tracker.is_orphaned(write) && tracker.is_orphaned(read));
    assert!(!tracker.is_orphaned(other));
    assert!(!tracker.has_pending_read_op(7), "an orphaned read does not count for a new connection on fd 7");

    let reused = tracker.new_op_id(7, InternalOpType::RingRead, InternalOpPayload::None);
    assert!(!tracker.is_orphaned(reused));
    assert_ne!(reused, write);
    assert_ne!(reused, read);

    assert_eq!(tracker.take_op_details(write).map(|d| d.op_type), Some(InternalOpType::SendRawVectored));
    assert!(!tracker.is_orphaned(write));
    assert!(tracker.is_orphaned(read));
  }
}
