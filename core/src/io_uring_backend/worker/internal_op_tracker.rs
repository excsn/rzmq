// core/src/io_uring_backend/worker/internal_op_tracker.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::send_buffer_pool::RegisteredSendBufferId;
use bytes::Bytes; // For storing send buffer
use std::collections::HashMap;
use std::os::unix::io::RawFd;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum InternalOpType {
  Accept,
  RingRead,
  Send,
  CloseFd,
  GenericHandlerOp,
  EventFdPoll,
  RingReadMultishot,
  AsyncCancel,
  SendZeroCopy,
  UringCmdSetCorkEnable,
  UringCmdSetCorkDisable,
}

/// Payload associated with an internal operation, e.g., buffer for send.
#[derive(Debug, Clone)] // Clone is needed if InternalOpDetails is Clone. Bytes is cheap to clone.
pub(crate) enum InternalOpPayload {
  None,
  SendBuffer {
    buffer: Bytes,               // Data being sent (if not ZC)
    app_op_ud: Option<UserData>, // UserData of the originating UringOpRequest (e.g., SendDataViaHandler)
    app_op_name: Option<String>, // Name of the app-level operation
  },
  CancelTarget {
    target_user_data: UserData,
  },
  SendZeroCopy {
    send_buf_id: RegisteredSendBufferId, // ID from SendBufferPool
    app_op_ud: UserData,                 // UserData of the originating UringOpRequest
    app_op_name: String,                 // Name of the app-level operation
  },
}

impl Default for InternalOpPayload {
  fn default() -> Self {
    InternalOpPayload::None
  }
}

/// Details stored for an in-flight internal operation.
#[derive(Debug, Clone)] // Must be Clone if InternalOpPayload is Clone
pub(crate) struct InternalOpDetails {
  pub fd: RawFd,
  pub op_type: InternalOpType,
  pub payload: InternalOpPayload, // Added to store data like send buffers
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

  pub fn remove_ops_for_fd(&mut self, fd_to_remove: RawFd) {
    // Payloads (like Bytes for Send) associated with removed ops will be dropped here.
    self
      .op_to_details
      .retain(|_user_data, details| details.fd != fd_to_remove);
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
}
