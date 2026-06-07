#![cfg(feature = "io-uring")]

use crate::ZmqError;
use crate::io_uring_backend::buffer_manager::BufferRingManager;

use crate::io_uring_backend::connection_handler::{
  HandlerIoOps, HandlerSqeBlueprint, UringConnectionHandler, UringWorkerInterface,
};
use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::worker::internal_op_tracker::InternalOpTracker;
use io_uring::cqueue;
use io_uring::cqueue::Entry as CqeResult;
use std::os::unix::io::RawFd;

pub const IOURING_CQE_F_MORE: u32 = 1 << 1;

/// Kernel-level backpressure state for a multishot recv operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MultishotFlowState {
  /// RECV_MULTISHOT is active in the kernel — CQEs are being delivered.
  Reading,
  /// ASYNC_CANCEL has been submitted; in-flight data CQEs may still arrive.
  Pausing,
  /// Kernel-side read is confirmed stopped. Waiting for ResumeConnection signal.
  Paused,
}

#[derive(Debug)]
pub(crate) struct MultishotReader {
  fd: RawFd,
  buffer_group_id: u16,

  active_op_user_data: Option<UserData>,
  is_active: bool,

  cancel_op_user_data: Option<UserData>,
  /// Flow control state for kernel-level backpressure.
  flow_state: MultishotFlowState,
}

impl MultishotReader {
  pub fn new(fd: RawFd, buffer_group_id: u16) -> Self {
    Self {
      fd,
      buffer_group_id,
      active_op_user_data: None,
      is_active: false,
      cancel_op_user_data: None,
      flow_state: MultishotFlowState::Paused,
    }
  }

  /// True only when a RECV_MULTISHOT is live in the kernel (not mid-cancel).
  pub fn is_reading(&self) -> bool {
    matches!(self.flow_state, MultishotFlowState::Reading) && self.is_active
  }

  /// True while an ASYNC_CANCEL is in-flight (headroom absorption window).
  pub fn is_pausing(&self) -> bool {
    matches!(self.flow_state, MultishotFlowState::Pausing)
  }

  /// True when the kernel-side read is confirmed stopped.
  pub fn is_paused(&self) -> bool {
    matches!(self.flow_state, MultishotFlowState::Paused)
  }

  /// Transition to `Paused` state. Called when the cancel CQE (or -ECANCELED on the
  /// original multishot CQE) is reaped.
  fn acknowledge_pause(&mut self) {
    tracing::debug!(
      "[MultishotReader FD={}] Kernel-side read confirmed stopped — flow state: Paused.",
      self.fd
    );
    self.flow_state = MultishotFlowState::Paused;
    self.is_active = false;
    self.active_op_user_data = None;
    self.cancel_op_user_data = None;
  }

  pub fn buffer_group_id(&self) -> u16 {
    self.buffer_group_id
  }

  /// Called by handler to signal intent to start a multishot read.
  pub fn prepare_recv_multi_intent(&self) -> Option<HandlerSqeBlueprint> {
    if self.is_active || self.cancel_op_user_data.is_some() {
      return None;
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
    self.cancel_op_user_data = None;
    self.flow_state = MultishotFlowState::Reading;
    tracing::debug!(
      "[MultishotReader FD={}] Marked as active in kernel with UserData {} — flow state: Reading.",
      self.fd,
      op_user_data
    );
  }

  /// Transitions to `Pausing` and returns an `ASYNC_CANCEL` blueprint targeting the active
  /// multishot operation. Returns `None` if already pausing/paused or not active.
  pub fn prepare_cancel_intent(&mut self) -> Option<HandlerSqeBlueprint> {
    if !self.is_reading() || self.active_op_user_data.is_none() {
      return None;
    }
    self.flow_state = MultishotFlowState::Pausing;
    tracing::debug!(
      "[MultishotReader FD={}] Transitioning to Pausing — submitting ASYNC_CANCEL for UserData {:?}.",
      self.fd,
      self.active_op_user_data
    );
    Some(HandlerSqeBlueprint::RequestNewAsyncCancel {
      fd: self.fd,
      target_user_data: self.active_op_user_data.unwrap(),
    })
  }

  /// Called by worker after it successfully submits the ASYNC_CANCEL SQE.
  pub fn mark_cancellation_submitted(
    &mut self,
    cancel_sqe_user_data: UserData,
    _target_op_user_data: UserData,
  ) {
    // _target_op_user_data should match self.active_op_user_data if logic is correct
    self.cancel_op_user_data = Some(cancel_sqe_user_data);
    // is_active remains true until cancel CQE or original op CQE without MORE.
    tracing::debug!(
      "[MultishotReader FD={}] Cancellation submitted with UserData {}.",
      self.fd,
      cancel_sqe_user_data
    );
  }

  pub fn process_cqe(
    &mut self,
    cqe: &CqeResult,
    buffer_manager: &BufferRingManager,
    owner_handler: &mut dyn UringConnectionHandler,
    worker_interface: &UringWorkerInterface<'_>,
    _internal_op_tracker_ref: &mut InternalOpTracker,
  ) -> Result<(HandlerIoOps, bool), ZmqError> {
    let cqe_ud = cqe.user_data();
    let cqe_res = cqe.result();
    let cqe_flags = cqe.flags();
    let mut ops_to_return = HandlerIoOps::new();

    if Some(cqe_ud) == self.active_op_user_data {
      // This CQE is for our active multishot read operation.
      // is_active should be true here if logic is correct.
      if !self.is_active {
        tracing::warn!(
          "[MultishotReader FD={}] CQE (ud {}) for active_op_user_data, but reader not marked active_in_kernel. State inconsistency?",
          self.fd,
          cqe_ud
        );
        self.is_active = true;
      }

      if cqe_res < 0 {
        let errno = -cqe_res;
        if errno == libc::ECANCELED as i32 {
          // The original multishot CQE arrived with -ECANCELED (from our ASYNC_CANCEL).
          // Transition to Paused and clean up — this is not an error.
          tracing::debug!(
            "[MultishotReader FD={}] Original multishot (ud {}) received -ECANCELED; confirming Paused.",
            self.fd,
            cqe_ud
          );
          self.acknowledge_pause();
          return Ok((ops_to_return, true));
        }
        if errno == libc::ENOBUFS {
          // Non-fatal: kernel buffer ring temporarily exhausted. Notify the handler to
          // set its throttle flag so `prepare_sqes` won't immediately resubmit and spin.
          // The handler also sends a `ResumeConnection` to the worker op channel so the
          // read is re-armed after the worker sleeps and Tokio drains the receive queues.
          tracing::debug!(
            "[MultishotReader FD={}] Buffer ring exhausted (ENOBUFS). Notifying handler.",
            self.fd
          );
          self.active_op_user_data = None;
          self.is_active = false;
          owner_handler.on_buffer_ring_exhausted();
          return Ok((ops_to_return, true));
        }

        tracing::error!(
          "[MultishotReader FD={}] Error on active multishot read (ud {}): errno {}. Terminating multishot.",
          self.fd,
          cqe_ud,
          errno
        );
        self.active_op_user_data = None;
        self.is_active = false;
        return Ok((ops_to_return.set_error_close(), true));
      }

      // Check for clean EOF before attempting buffer_select — the kernel does NOT set
      // IORING_CQE_F_BUFFER on EOF (result == 0), so checking for the buffer first would
      // produce a false "missing F_BUFFER" error and incorrectly treat EOF as fatal.
      if cqe_res == 0 {
        tracing::debug!(
          "[MultishotReader FD={}] Clean EOF on multishot read (ud {}). Notifying handler.",
          self.fd,
          cqe_ud
        );
        self.active_op_user_data = None;
        self.is_active = false;
        ops_to_return = owner_handler.process_ring_read_bytes(bytes::Bytes::new(), worker_interface);
        return Ok((ops_to_return, true));
      }

      // Check for IORING_CQE_F_BUFFER using the public helper
      let buffer_id_opt = cqueue::buffer_select(cqe_flags);
      if buffer_id_opt.is_none() {
        tracing::error!(
          "[MultishotReader FD={}] Multishot CQE (ud {}) missing F_BUFFER flag or invalid BID! Flags: {:x}",
          self.fd,
          cqe_ud,
          cqe_flags
        );
        self.active_op_user_data = None;
        self.is_active = false;

        return Ok((ops_to_return.set_error_close(), true));
      }
      let buffer_id = buffer_id_opt.unwrap(); // Safe due to check above
      let bytes_read = cqe_res as usize;

      if bytes_read > 0 {
        // Copy data into owned Bytes and immediately replenish the ring slot.
        match unsafe { buffer_manager.take_and_replenish_buffer(buffer_id, bytes_read) } {
          Ok(owned_bytes) => {
            ops_to_return = owner_handler.process_ring_read_bytes(owned_bytes, worker_interface);
          }
          Err(e) => {
            tracing::error!(
              "[MultishotReader FD={}] Failed to copy buffer ID {} ({} bytes): {:?}. Terminating multishot.",
              self.fd,
              buffer_id,
              bytes_read,
              e
            );
            self.active_op_user_data = None;
            self.is_active = false;

            return Ok((ops_to_return.set_error_close(), true));
          }
        }
      } else {
        // bytes_read == 0 (EOF with buffer consumed — reprovide slot inline)
        tracing::info!(
          "[MultishotReader FD={}] EOF on multishot read (ud {}). Terminating multishot.",
          self.fd,
          cqe_ud
        );
        if let Err(e) = unsafe { buffer_manager.reprovide_buffer(buffer_id) } {
          tracing::warn!("[MultishotReader FD={}] reprovide_buffer({}) on EOF failed: {:?}", self.fd, buffer_id, e);
        }
        ops_to_return = owner_handler.process_ring_read_bytes(bytes::Bytes::new(), worker_interface);
        // Fall through to MORE flag check; EOF means the multishot op should terminate.
      }

      // Check IORING_CQE_F_MORE flag
      if (cqe_flags & IOURING_CQE_F_MORE) == 0 || bytes_read == 0 {
        // Terminate on EOF too
        tracing::debug!(
          "[MultishotReader FD={}] Multishot read (ud {}) finished (no MORE flag or EOF). Bytes read: {}",
          self.fd,
          cqe_ud,
          bytes_read
        );
        // This specific multishot op instance is done.
        // The cqe_processor will call internal_op_tracker.take_op_details(self.active_op_user_data.unwrap()).
        self.active_op_user_data = None;
        self.is_active = false;
        return Ok((ops_to_return, true));
        // The handler, via its `prepare_sqes`, can decide to initiate a new multishot read.
      } else {
        tracing::trace!(
          "[MultishotReader FD={}] Multishot read (ud {}) has MORE flag. Op remains active.",
          self.fd,
          cqe_ud
        );
        return Ok((ops_to_return, false));
        // is_active remains true, active_op_user_data remains set.
        // internal_op_tracker entry for cqe_ud is NOT taken by cqe_processor.
      }
    } else if Some(cqe_ud) == self.cancel_op_user_data {
      // This CQE is for our ASYNC_CANCEL request.
      // -ENOENT means the target op had already completed by the time the cancel was processed —
      // treat it identically to success (0): the operation is gone, proceed to Paused.
      let is_cancel_success = cqe_res == 0 || cqe_res == -libc::ENOENT;
      if !is_cancel_success {
        tracing::warn!(
          "[MultishotReader FD={}] ASYNC_CANCEL (ud {}) returned unexpected result {}; proceeding to Paused anyway.",
          self.fd,
          cqe_ud,
          cqe_res
        );
      } else {
        tracing::debug!(
          "[MultishotReader FD={}] ASYNC_CANCEL (ud {}) confirmed (res {}). Transitioning to Paused.",
          self.fd,
          cqe_ud,
          cqe_res
        );
      }
      self.acknowledge_pause();
      return Ok((ops_to_return, true));
    } else {
      // This CQE was not for this MultishotReader. This should ideally not be reached
      // if cqe_processor calls delegate_cqe_to_multishot_reader only after handler.matches_cqe_user_data().
      tracing::error!(
        "[MultishotReader FD={}] process_cqe called with non-matching UserData (ud {}). This indicates a logic error in cqe_processor's delegation.",
        self.fd,
        cqe_ud
      );
      return Err(ZmqError::Internal(
        "MultishotReader::process_cqe called with non-matching UserData".into(),
      ));
    }
  }

  pub fn is_active(&self) -> bool {
    self.is_active && self.cancel_op_user_data.is_none()
  }

  pub(crate) fn set_active(&mut self, user_data: UserData) {
    if self.active_op_user_data == Some(user_data) {
      self.is_active = true;
      tracing::debug!(
        "[MultishotReader FD={}] Marked as active with UserData {}.",
        self.fd,
        user_data
      );
    } else {
      // Tracking ID mismatch — indicates out-of-order state transitions or a stale blueprint
      // being re-submitted. Log at error level so regressions surface immediately.
      tracing::error!(
        fd = self.fd,
        expected = ?self.active_op_user_data,
        got = user_data,
        "MultishotReader set_active out-of-order: tracking ID mismatch"
      );
    }
  }

  /// Checks if the given CQE UserData matches any operation this reader is expecting.
  pub(crate) fn matches_cqe_user_data(&self, cqe_user_data: UserData) -> bool {
    self.active_op_user_data == Some(cqe_user_data)
      || self.cancel_op_user_data == Some(cqe_user_data)
  }

  /// Called when the owning handler's FD is closed, ensuring the reader is marked inactive.
  pub(crate) fn set_inactive_due_to_close(&mut self) {
    tracing::debug!(
      "[MultishotReader FD={}] Marked as inactive due to FD closure.",
      self.fd
    );
    self.is_active = false;
    self.active_op_user_data = None;
    self.cancel_op_user_data = None;
    self.flow_state = MultishotFlowState::Paused;
  }
}
