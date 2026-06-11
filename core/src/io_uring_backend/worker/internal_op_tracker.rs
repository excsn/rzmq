#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::send_buffer_pool::RegisteredSendBufferId;
use bytes::Bytes;
use std::collections::HashMap;
use std::os::unix::io::RawFd;

/// Simple, protocol-agnostic scatter-gather batch for raw vectored writes.
///
/// Unlike `PinnedBatchWrite`, this has no ZMTP-specific fields (`header_slab`, `app_op_name`).
/// Used exclusively by `UringByteHandler` for `EgressChunk::Vectored` writes.
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
  Accept,
  RingRead,
  Send,
  SendZeroCopy,
  CloseFd,
  GenericHandlerOp,
  EventFdPoll,
  RingReadMultishot,
  AsyncCancel,
  /// Protocol-agnostic writev for `UringByteHandler` vectored egress.
  SendRawVectored,
  /// `SEND_ZC` from a pre-registered, pre-written buffer slot (Phase 3 zero-copy lease path).
  SendZeroCopyLeased,
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
  CancelTarget {
    target_user_data: UserData,
  },
  SendZeroCopy {
    send_buf_id: RegisteredSendBufferId,
    original_data: Bytes,
    send_op_flags: i32,
    app_op_ud: UserData,
    app_op_name: String,
  },
  /// Protocol-agnostic scatter-gather batch for `UringByteHandler` vectored egress.
  RawVectored(PinnedEgressBatch),
  /// Leased pre-registered buffer slot — only the buffer ID is needed for release on completion.
  SendZeroCopyLeased { send_buf_id: RegisteredSendBufferId },
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

#[derive(Debug)]
pub(crate) struct InternalOpTracker {
  /// Hot path: O(1) direct-index lookup via `user_data - INTERNAL_OP_BASE`.
  pub(crate) op_to_details: slab::Slab<InternalOpDetails>,
  /// Cold path: holds entries for in-flight SEND_ZC operations awaiting a second
  /// notification CQE with the same `user_data`. Typically empty; bounded by active ZC sends.
  pending_notifications: HashMap<UserData, InternalOpDetails>,
}

impl InternalOpTracker {
  pub fn new() -> Self {
    Self {
      op_to_details: slab::Slab::new(),
      pending_notifications: HashMap::new(),
    }
  }

  /// Allocates a new `UserData` for an internal operation, stores its details, and returns the ID.
  pub fn new_op_id(
    &mut self,
    fd: RawFd,
    op_type: InternalOpType,
    payload: InternalOpPayload,
  ) -> UserData {
    let key = self.op_to_details.insert(InternalOpDetails { fd, op_type, payload });
    key as u64 + INTERNAL_OP_BASE
  }

  pub fn take_op_details(&mut self, user_data: UserData) -> Option<InternalOpDetails> {
    if user_data < INTERNAL_OP_BASE {
      return None;
    }
    let key = (user_data - INTERNAL_OP_BASE) as usize;
    if self.op_to_details.contains(key) {
      Some(self.op_to_details.remove(key))
    } else {
      self.pending_notifications.remove(&user_data)
    }
  }

  #[allow(dead_code)]
  pub fn get_op_details(&self, user_data: UserData) -> Option<&InternalOpDetails> {
    if user_data < INTERNAL_OP_BASE {
      return None;
    }
    let key = (user_data - INTERNAL_OP_BASE) as usize;
    self.op_to_details.get(key)
      .or_else(|| self.pending_notifications.get(&user_data))
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
    self.op_to_details
      .iter()
      .map(|(k, _)| k as u64 + INTERNAL_OP_BASE)
      .chain(self.pending_notifications.keys().copied())
      .collect()
  }

  pub fn remove_ops_for_fd(&mut self, fd_to_remove: RawFd) -> Vec<InternalOpDetails> {
    let slab_keys: Vec<usize> = self
      .op_to_details
      .iter()
      .filter(|(_, v)| v.fd == fd_to_remove)
      .map(|(k, _)| k)
      .collect();
    let mut removed: Vec<InternalOpDetails> = slab_keys
      .into_iter()
      .map(|k| self.op_to_details.remove(k))
      .collect();

    let notif_keys: Vec<UserData> = self
      .pending_notifications
      .iter()
      .filter(|(_, d)| d.fd == fd_to_remove)
      .map(|(k, _)| *k)
      .collect();
    for k in notif_keys {
      if let Some(d) = self.pending_notifications.remove(&k) {
        removed.push(d);
      }
    }
    removed
  }

  /// Finds all `UserData` for a given FD that match a predicate on the op_type.
  pub fn find_ops_for_fd(
    &self,
    fd_to_find: RawFd,
    predicate: impl Fn(InternalOpType) -> bool,
  ) -> Vec<UserData> {
    let mut result: Vec<UserData> = self
      .op_to_details
      .iter()
      .filter(|(_, d)| d.fd == fd_to_find && predicate(d.op_type))
      .map(|(k, _)| k as u64 + INTERNAL_OP_BASE)
      .collect();
    let from_notifs: Vec<UserData> = self
      .pending_notifications
      .iter()
      .filter(|(_, d)| d.fd == fd_to_find && predicate(d.op_type))
      .map(|(k, _)| *k)
      .collect();
    result.extend(from_notifs);
    result
  }

  pub(crate) fn has_pending_read_op(&self, fd_to_check: RawFd) -> bool {
    // pending_notifications holds ZC send continuations, never reads
    self.op_to_details.values().any(|d| {
      d.fd == fd_to_check
        && matches!(d.op_type, InternalOpType::RingRead | InternalOpType::RingReadMultishot)
    })
  }
}
