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

#[derive(Debug)]
pub(crate) struct InternalOpTracker {
  pub(crate) op_to_details: HashMap<UserData, InternalOpDetails>,
  pub(crate) next_id: UserData,
}

impl InternalOpTracker {
  pub fn new() -> Self {
    Self {
      op_to_details: HashMap::new(),
      next_id: 1_000_000_000,
    }
  }

  /// Generates a new UserData for an internal operation and maps it to its details,
  /// including an optional payload.
  pub fn new_op_id(
    &mut self,
    fd: RawFd,
    op_type: InternalOpType,
    payload: InternalOpPayload, // Accept payload
  ) -> UserData {
    let id = self.next_id;
    self.next_id = self.next_id.wrapping_add(1);
    if self.next_id == 0 {
      self.next_id = 1_000_000_000;
    }
    self.op_to_details.insert(
      id,
      InternalOpDetails {
        fd,
        op_type,
        payload,
      },
    );
    id
  }

  pub fn take_op_details(&mut self, user_data: UserData) -> Option<InternalOpDetails> {
    self.op_to_details.remove(&user_data)
  }

  #[allow(dead_code)]
  pub fn get_op_details(&self, user_data: UserData) -> Option<&InternalOpDetails> {
    self.op_to_details.get(&user_data)
  }

  pub fn is_empty(&self) -> bool {
    self.op_to_details.is_empty()
  }

  pub fn remove_ops_for_fd(&mut self, fd_to_remove: RawFd) -> Vec<InternalOpDetails> {
    let keys: Vec<UserData> = self
      .op_to_details
      .iter()
      .filter(|(_, v)| v.fd == fd_to_remove)
      .map(|(k, _)| *k)
      .collect();
    keys.into_iter()
      .filter_map(|k| self.op_to_details.remove(&k))
      .collect()
  }

  /// Finds all UserData for a given FD that match a predicate on the op_type.
  pub fn find_ops_for_fd(
    &self,
    fd_to_find: RawFd,
    predicate: impl Fn(InternalOpType) -> bool,
  ) -> Vec<UserData> {
    self
      .op_to_details
      .iter()
      .filter(|(_, details)| details.fd == fd_to_find && predicate(details.op_type))
      .map(|(user_data, _)| *user_data)
      .collect()
  }

  pub(crate) fn has_pending_read_op(&self, fd_to_check: RawFd) -> bool {
    self.op_to_details.values().any(|details| {
      details.fd == fd_to_check
        && matches!(
          details.op_type,
          InternalOpType::RingRead | InternalOpType::RingReadMultishot
        )
    })
  }
}
