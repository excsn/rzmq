#![cfg(feature = "io-uring")]

use crate::io_uring_backend::ops::UserData;
use crate::io_uring_backend::worker::internal_op_tracker::{InternalOpPayload, InternalOpTracker, InternalOpType};

use io_uring::{opcode, squeue, types};
use std::os::fd::AsRawFd;
use std::os::unix::io::RawFd;

pub(crate) struct EventFdPoller {
  event_fd: eventfd::EventFD,
  pub(crate) current_poll_user_data: UserData,
  pub(crate) is_poll_submitted: bool,
}

impl std::fmt::Debug for EventFdPoller {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("EventFdPoller")
      .field("event_fd_raw", &self.event_fd.as_raw_fd()) // Use AsRawFd
      .field("current_poll_user_data", &self.current_poll_user_data)
      .field("is_poll_submitted", &self.is_poll_submitted)
      .finish()
  }
}

impl EventFdPoller {
  pub fn new(
    initial_event_fd_val: u32,
    flags: eventfd::EfdFlags,
    internal_op_tracker: &mut InternalOpTracker,
  ) -> Result<Self, std::io::Error> {
    let event_fd_instance = eventfd::EventFD::new(initial_event_fd_val, flags)?;
    let initial_user_data = internal_op_tracker.new_op_id(
      event_fd_instance.as_raw_fd(),
      InternalOpType::EventFdPoll,
      InternalOpPayload::None,
    );
    Ok(Self {
      event_fd: event_fd_instance,
      current_poll_user_data: initial_user_data,
      is_poll_submitted: false,
    })
  }

  pub fn new_with_fd(
    event_fd_instance: eventfd::EventFD, // Takes ownership
    internal_op_tracker: &mut InternalOpTracker,
  ) -> Self {
    let initial_user_data = internal_op_tracker.new_op_id(
      event_fd_instance.as_raw_fd(),
      InternalOpType::EventFdPoll,
      InternalOpPayload::None,
    );
    Self {
      event_fd: event_fd_instance,
      current_poll_user_data: initial_user_data,
      is_poll_submitted: false,
    }
  }

  pub fn event_fd_raw(&self) -> RawFd {
    self.event_fd.as_raw_fd()
  }

  pub fn clone_event_fd(&self) -> eventfd::EventFD {
    self.event_fd.clone()
  }

  /// Builds and attempts to push an IORING_OP_POLL_ADD SQE for the eventfd.
  /// Returns true if an SQE was successfully pushed, false if SQ was full or already submitted.
  pub fn try_submit_initial_poll_sqe(
    &mut self,
    sq: &mut squeue::SubmissionQueue<'_>,
    // internal_op_tracker is not needed here because current_poll_user_data
    // is managed on new() and after CQE handling.
  ) -> bool {
    if self.is_poll_submitted {
      // Already submitted, no action needed from this call.
      // Consider if this should return true (already done) or false (no new SQE pushed *now*).
      // For the main_loop, if it's already submitted, the loop doesn't need to retry this specific call.
      // So, returning true as "the desired state of being submitted is met".
      return true;
    }
    if unsafe { sq.is_full() } {
      tracing::warn!("[EventFdPoller] SQ full, cannot submit eventfd poll SQE now.");
      return false; // Caller (main_loop) might retry later.
    }

    // Build the POLL_ADD SQE.
    // The `current_poll_user_data` field holds the UserData generated in `new()`
    // or by `handle_cqe_if_matches()` after a previous poll completed.
    let poll_entry = opcode::PollAdd::new(types::Fd(self.event_fd_raw()), libc::POLLIN as u32)
      .build()
      .user_data(self.current_poll_user_data);

    match unsafe { sq.push(&poll_entry) } {
      Ok(_) => {
        tracing::debug!(
          "[EventFdPoller] Submitted initial eventfd poll SQE with UserData: {}",
          self.current_poll_user_data
        );
        self.is_poll_submitted = true;
        true // SQE pushed successfully
      }
      Err(_) => {
        tracing::warn!("[EventFdPoller] Failed to push eventfd poll SQE (SQ full race?). Will retry.");
        // is_poll_submitted remains false, so it will be retried.
        false // SQE not pushed
      }
    }
  }

  /// Handles a CQE if it matches this poller's current_poll_user_data.
  /// If matched, it reads from the eventfd and updates state for the next poll.
  /// Returns true if the CQE was handled by this poller, false otherwise.
  pub fn handle_cqe_if_matches(
    &mut self,
    cqe_user_data: UserData,
    cqe_result: i32,
    internal_op_tracker: &mut InternalOpTracker,
    is_shutting_down: bool,
  ) -> bool {
    if cqe_user_data == self.current_poll_user_data {
      tracing::debug!(
        "[EventFdPoller] CQE matched (ud: {}, res: {}). Handling eventfd.",
        cqe_user_data,
        cqe_result
      );

      // Current poll is now considered complete, regardless of outcome.
      self.is_poll_submitted = false;

      // Important: The InternalOpTracker entry for `self.current_poll_user_data`
      // should be consumed by the caller (`cqe_processor`) by calling
      // `internal_op_tracker.take_op_details(cqe_user_data)`.
      // This is because the `UserData` has served its purpose for this specific poll instance.

      if cqe_result >= 0 {
        // Poll successful (event occurred or timeout if poll had one, though our poll doesn't timeout by itself)
        // Or, if IORING_CQE_F_NOTIFY is set, this means the poll operation was cancelled, not that the event fired.
        // For a simple POLLIN, a non-negative result usually means POLLIN is ready.
        // (Note: IORING_CQE_F_MORE is for multi-shot polls, not directly relevant here yet)
        if (cqe_result as u32 & libc::POLLERR as u32) != 0 {
          tracing::error!(
            "[EventFdPoller] Eventfd poll (ud: {}) completed with POLLERR. FD: {}",
            cqe_user_data,
            self.event_fd_raw()
          );
          // Consider this a critical error for the eventfd itself. The poller might not be recoverable.
          // For now, we will still attempt to re-poll, but this needs monitoring.
        } else if (cqe_result as u32 & libc::POLLHUP as u32) != 0 {
          tracing::error!(
            "[EventFdPoller] Eventfd poll (ud: {}) completed with POLLHUP. FD: {}",
            cqe_user_data,
            self.event_fd_raw()
          );
        } else {
          // Assume POLLIN if no error flags and non-negative result.
          match self.event_fd.read() {
            Ok(val) => {
              tracing::trace!(
                "[EventFdPoller] Read value {} from eventfd {}.",
                val,
                self.event_fd_raw()
              );
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
              tracing::trace!(
                "[EventFdPoller] Read from eventfd {} would block (non-blocking read after poll signal).",
                self.event_fd_raw()
              );
            }
            Err(e) => {
              tracing::error!(
                "[EventFdPoller] Error reading from eventfd {}: {}. Worker might not wake for new ops.",
                self.event_fd_raw(),
                e
              );
              // This is problematic. The eventfd might be in a bad state.
            }
          }
        }
      } else {
        // cqe_result < 0
        let errno = -cqe_result;
        tracing::error!(
          "[EventFdPoller] Eventfd poll SQE (ud: {}) failed with errno: {}. FD: {}",
          cqe_user_data,
          errno,
          self.event_fd_raw()
        );
        // If the poll operation itself fails (e.g., EBADF), re-polling might not be useful.
        // For now, we will still set up for a re-poll.
      }


      if !is_shutting_down {
        // Regardless of the outcome of the previous poll (success or failure),
        // we need to generate a new UserData for the *next* poll submission attempt.
        // The old one (`self.current_poll_user_data`) has been "used up" by this CQE.
        self.current_poll_user_data = internal_op_tracker.new_op_id(
          self.event_fd_raw(),
          InternalOpType::EventFdPoll,
          InternalOpPayload::None,
        );
        tracing::debug!(
          "[EventFdPoller] Prepared for next poll submission with new UserData: {}",
          self.current_poll_user_data
        );
      } else {
        // If we are shutting down, we do NOT create a new poll operation.
        // We just handled the completion of the last one, and now the poller is idle.
        tracing::debug!("[EventFdPoller] Worker is shutting down. Not preparing new poll operation.");
      }

      return true; // CQE was handled by this poller
    }
    false // CQE did not match this poller's UserData
  }
}
