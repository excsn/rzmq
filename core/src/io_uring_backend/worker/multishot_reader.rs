// core/src/io_uring_backend/worker/multishot_reader.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::worker::internal_op_tracker::{InternalOpTracker, InternalOpType, InternalOpPayload};
use crate::io_uring_backend::buffer_manager::BufferRingManager;
use crate::io_uring_backend::connection_handler::{
    UringConnectionHandler, UringWorkerInterface, HandlerIoOps, HandlerSqeBlueprint,
};
use crate::ZmqError;
use io_uring::cqueue;
use io_uring::{squeue, types, cqueue::Entry as CqeResult};
use std::fmt;
use std::os::unix::io::RawFd;

pub const IORING_CQE_F_MORE: u32 = 1 << 1;

#[derive(Debug)]
pub(crate) struct MultishotReader {
  fd: RawFd,
  buffer_group_id: u16,

  active_op_user_data: Option<UserData>, 
  is_active: bool,                     

  cancel_op_user_data: Option<UserData>, 
}

impl MultishotReader {
  pub fn new(fd: RawFd, buffer_group_id: u16) -> Self {
    Self {
      fd,
      buffer_group_id,
      active_op_user_data: None,
      is_active: false,
      cancel_op_user_data: None,
    }
  }

  pub fn buffer_group_id(&self) -> u16 {
    self.buffer_group_id
  }

  /// Called by handler to signal intent to start a multishot read.
  pub fn prepare_recv_multi_intent(&self) -> Option<HandlerSqeBlueprint> {
    if self.is_active || self.cancel_op_user_data.is_some() {
      return None; // Already active or being cancelled
    }
    Some(HandlerSqeBlueprint::RequestNewRingReadMultishot {
      fd: self.fd,
      bgid: self.buffer_group_id,
    })
  }

  /// Called by worker after it successfully submits the RECV_MULTISHOT SQE.
  pub fn mark_operation_submitted(&mut self, op_user_data: UserData) {
    self.active_op_user_data = Some(op_user_data);
    self.is_active = true;
    self.cancel_op_user_data = None; // Clear any prior cancellation attempt for this new op
    tracing::debug!("[MultishotReader FD={}] Marked as active in kernel with UserData {}.", self.fd, op_user_data);
  }

  /// Called by handler to signal intent to cancel the active multishot read.
  pub fn prepare_cancel_intent(&self) -> Option<HandlerSqeBlueprint> {
    if !self.is_active || self.active_op_user_data.is_none() || self.cancel_op_user_data.is_some() {
      return None; // Not active, or no UserData to target, or already cancelling
    }
    Some(HandlerSqeBlueprint::RequestNewAsyncCancel {
      fd: self.fd,
      target_user_data: self.active_op_user_data.unwrap(),
    })
  }

  /// Called by worker after it successfully submits the ASYNC_CANCEL SQE.
  pub fn mark_cancellation_submitted(&mut self, cancel_sqe_user_data: UserData, _target_op_user_data: UserData) {
    // _target_op_user_data should match self.active_op_user_data if logic is correct
    self.cancel_op_user_data = Some(cancel_sqe_user_data);
    // is_active remains true until cancel CQE or original op CQE without MORE.
    tracing::debug!("[MultishotReader FD={}] Cancellation submitted with UserData {}.", self.fd, cancel_sqe_user_data);
  }
  
  pub fn process_cqe(
    &mut self,
    cqe: &CqeResult,
    buffer_manager: &BufferRingManager,
    owner_handler: &mut dyn UringConnectionHandler,
    worker_interface: &UringWorkerInterface<'_>,
    internal_op_tracker_ref: &mut InternalOpTracker, 
  ) -> Result<(HandlerIoOps, bool), ZmqError> {
    let cqe_ud = cqe.user_data();
    let cqe_res = cqe.result();
    let cqe_flags = cqe.flags();
    let mut ops_to_return = HandlerIoOps::new();
    let mut cleanup_this_active_op_ud = false;

    if Some(cqe_ud) == self.active_op_user_data {
      // This CQE is for our active multishot read operation.
      // is_active should be true here if logic is correct.
      if !self.is_active {
          tracing::warn!("[MultishotReader FD={}] CQE (ud {}) for active_op_user_data, but reader not marked active_in_kernel. State inconsistency?", self.fd, cqe_ud);
      }

      if cqe_res < 0 {
        let errno = -cqe_res;
        tracing::error!("[MultishotReader FD={}] Error on active multishot read (ud {}): errno {}. Terminating multishot.", self.fd, cqe_ud, errno);
        // Worker needs to take details for self.active_op_user_data.
        self.active_op_user_data = None;
        self.is_active = false;
        cleanup_this_active_op_ud = true;
        return Ok((ops_to_return.set_error_close(),cleanup_this_active_op_ud));
      }

      // Check for IORING_CQE_F_BUFFER using the public helper
      let buffer_id_opt = cqueue::buffer_select(cqe_flags);
      if buffer_id_opt.is_none() {
          tracing::error!("[MultishotReader FD={}] Multishot CQE (ud {}) missing F_BUFFER flag or invalid BID! Flags: {:x}", self.fd, cqe_ud, cqe_flags);
          self.active_op_user_data = None;
          self.is_active = false;
          cleanup_this_active_op_ud = true;
        return Ok((ops_to_return.set_error_close(),cleanup_this_active_op_ud));
      }
      let buffer_id = buffer_id_opt.unwrap(); // Safe due to check above
      let bytes_read = cqe_res as usize;

      if bytes_read > 0 {
        match unsafe { buffer_manager.borrow_kernel_filled_buffer(buffer_id, bytes_read) } {
          Ok(borrowed_buffer) => {
            ops_to_return = owner_handler.process_ring_read_data(&borrowed_buffer, buffer_id, worker_interface);
            // BorrowedBuffer is dropped here, returning it to the pool.
          }
          Err(e) => {
            tracing::error!("[MultishotReader FD={}] Failed to borrow buffer ID {} ({} bytes): {:?}. Terminating multishot.", self.fd, buffer_id, bytes_read, e);
            self.active_op_user_data = None;
            self.is_active = false;
        return Ok((ops_to_return.set_error_close(),cleanup_this_active_op_ud));
          }
        }
      } else { // bytes_read == 0 (EOF)
         tracing::info!("[MultishotReader FD={}] EOF on multishot read (ud {}). Terminating multishot.", self.fd, cqe_ud);
         ops_to_return = owner_handler.process_ring_read_data(&[], buffer_id, worker_interface);
         // Fall through to MORE flag check; EOF means the multishot op should terminate.
      }
      
      // Check IORING_CQE_F_MORE flag
      if (cqe_flags & IORING_CQE_F_MORE) == 0 || bytes_read == 0 { // Terminate on EOF too
        tracing::debug!("[MultishotReader FD={}] Multishot read (ud {}) finished (no MORE flag or EOF). Bytes read: {}", self.fd, cqe_ud, bytes_read);
        // This specific multishot op instance is done.
        // The cqe_processor will call internal_op_tracker.take_op_details(self.active_op_user_data.unwrap()).
        self.active_op_user_data = None; 
        self.is_active = false;
        cleanup_this_active_op_ud = true;
        // The handler, via its `prepare_sqes`, can decide to initiate a new multishot read.
      } else {
        tracing::trace!("[MultishotReader FD={}] Multishot read (ud {}) has MORE flag. Op remains active.", self.fd, cqe_ud);
        cleanup_this_active_op_ud = false; 
        // is_active remains true, active_op_user_data remains set.
        // internal_op_tracker entry for cqe_ud is NOT taken by cqe_processor.
      }
      
    } else if Some(cqe_ud) == self.cancel_op_user_data {
      // This CQE is for our cancellation request for the multishot op.
      tracing::debug!("[MultishotReader FD={}] AsyncCancel CQE (ud {}) received for multishot op (target_ud: {:?}). Res: {}", self.fd, cqe_ud, self.active_op_user_data, cqe_res);
      if cqe_res < 0 && cqe_res != -libc::ECANCELED && cqe_res != -libc::ENOENT { // ENOENT means op already completed
          tracing::warn!("[MultishotReader FD={}] AsyncCancel for multishot op failed with error: {}", self.fd, cqe_res);
      }
      
      // The worker/cqe_processor will take internal_op_details for cqe_ud (the cancel op).
      // It also needs to take internal_op_details for self.active_op_user_data (the original multishot op)
      // because the cancellation means the original op is now definitely finished.
      if let Some(original_multishot_ud) = self.active_op_user_data.take() {
          tracing::trace!("[MultishotReader FD={}] Original multishot op (ud {}) is now considered terminated due to cancel CQE.", self.fd, original_multishot_ud);
          // The cqe_processor, when it sees an AsyncCancel CQE, will look at its payload
          // (CancelTarget { target_user_data }) and also remove that target_user_data from internal_op_tracker.
      }
      self.cancel_op_user_data = None;
      self.is_active = false;
      cleanup_this_active_op_ud = true;
    } else {
      // This CQE was not for this MultishotReader. This should ideally not be reached
      // if cqe_processor calls delegate_cqe_to_multishot_reader only after handler.matches_cqe_user_data().
      tracing::error!("[MultishotReader FD={}] process_cqe called with non-matching UserData (ud {}). This indicates a logic error in cqe_processor's delegation.", self.fd, cqe_ud);
      return Err(ZmqError::Internal("MultishotReader::process_cqe called with non-matching UserData".into()));
    }
    Ok((ops_to_return, cleanup_this_active_op_ud))
  }

  pub fn is_active(&self) -> bool {
    self.is_active && self.cancel_op_user_data.is_none()
  }

  pub(crate) fn set_active(&mut self, user_data: UserData) {
      if self.active_op_user_data == Some(user_data) {
          self.is_active = true;
          tracing::debug!("[MultishotReader FD={}] Marked as active with UserData {}.", self.fd, user_data);
      } else {
          // This can happen if a new multishot op is prepared (generating new active_op_user_data)
          // but the worker calls set_active for the *old* UserData if a submit attempt failed and retried.
          // Or if an old blueprint was processed.
          tracing::warn!("[MultishotReader FD={}] set_active called with UserData {}, but current expected is {:?}. State unchanged unless matching.", self.fd, user_data, self.active_op_user_data);
          // Only set active if the UserData matches the one we are currently tracking for submission.
      }
  }

  /// Checks if the given CQE UserData matches any operation this reader is expecting.
  pub(crate) fn matches_cqe_user_data(&self, cqe_user_data: UserData) -> bool {
    self.active_op_user_data == Some(cqe_user_data) || self.cancel_op_user_data == Some(cqe_user_data)
  }

  /// Called when the owning handler's FD is closed, ensuring the reader is marked inactive.
  pub(crate) fn set_inactive_due_to_close(&mut self) {
    tracing::debug!("[MultishotReader FD={}] Marked as inactive due to FD closure.", self.fd);
    self.is_active = false;
    self.active_op_user_data = None;
    self.cancel_op_user_data = None; // Clear any pending cancellation state too
  }
}