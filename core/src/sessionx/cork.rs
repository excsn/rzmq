#![cfg(target_os = "linux")]

use std::os::fd::{AsRawFd, FromRawFd, RawFd};

/// State specific to TCP_CORK management on Linux.
/// This struct will only be actively used if the underlying stream `S`
/// implements `AsRawFd` and the `use_cork` option is enabled.
#[derive(Debug)]
pub(crate) struct TcpCorkInfoX {
  fd: RawFd,
  is_corked: bool,
  /// Tracks if the *next* send operation is for the first frame of a new logical ZMQ message.
  /// This is important for deciding when to enable TCP_CORK.
  expecting_first_frame_of_new_zmq_message: bool,
}

impl TcpCorkInfoX {
  /// Creates a new `TcpCorkInfoX` if the stream provides a raw file descriptor.
  /// Returns `None` if `fd` cannot be obtained (which shouldn't happen if `S: AsRawFd`).
  pub(crate) fn new<S: AsRawFd>(stream: &S) -> Option<Self> {
    let fd = stream.as_raw_fd();
    if fd < 0 {
      // Basic sanity check for a valid FD
      tracing::warn!(
        "TcpCorkInfoX: Received invalid RawFd (<0) from stream, corking will be disabled."
      );
      return None;
    }
    Some(Self {
      fd,
      is_corked: false,
      expecting_first_frame_of_new_zmq_message: true,
    })
  }

  /// Returns the raw file descriptor.
  pub(crate) fn fd(&self) -> RawFd {
    self.fd
  }

  /// Returns the current corked state.
  pub(crate) fn is_corked(&self) -> bool {
    self.is_corked
  }

  /// Returns whether the next frame is expected to be the first of a new ZMQ message.
  pub(crate) fn is_expecting_first_frame(&self) -> bool {
    self.expecting_first_frame_of_new_zmq_message
  }

  /// Sets the expectation for the next frame.
  /// Call with `true` after a full ZMQ message (all parts) has been sent.
  /// Call with `false` after the first part of a ZMQ message has been sent if more parts are coming.
  pub(crate) fn set_expecting_first_frame(&mut self, expecting: bool) {
    self.expecting_first_frame_of_new_zmq_message = expecting;
  }

  /// Attempts to set the `TCP_CORK` socket option on the stored file descriptor.
  ///
  /// # Arguments
  /// * `enable`: `true` to enable `TCP_CORK`, `false` to disable it.
  /// * `actor_handle`: The handle of the calling actor, for logging purposes.
  ///
  /// This operation is performed in a blocking Tokio task because `socket2`'s
  /// `set tcp cork` is a blocking syscall.
  pub(crate) async fn apply_cork_state(&mut self, enable: bool, actor_handle: usize) {
    if self.is_corked == enable {
      // Already in the desired state
      return;
    }

    let fd_to_cork = self.fd; // Copy fd for use in the blocking task
    tracing::trace!(
      sca_handle = actor_handle,
      fd = fd_to_cork,
      cork_enable = enable,
      "Attempting to set TCP_CORK"
    );

    let dup_fd = unsafe { libc::dup(fd_to_cork) };
    if dup_fd < 0 {
      // Log the OS error and return early, instead of trying to return an Err.
      let e = std::io::Error::last_os_error();
      tracing::error!(
        sca_handle = actor_handle,
        fd = fd_to_cork,
        error = %e,
        "Failed to duplicate file descriptor for TCP_CORK operation. Aborting cork change."
      );
      return; // Abort the operation.
    }

    let res = tokio::task::spawn_blocking(move || {
      let socket = unsafe { socket2::Socket::from_raw_fd(dup_fd) };

      let result = socket.set_tcp_cork(enable);
      result
    })
    .await;

    match res {
      Ok(Ok(())) => {
        tracing::debug!(
          sca_handle = actor_handle,
          fd = fd_to_cork,
          cork_enable = enable,
          "TCP_CORK successfully {}set",
          if enable { "" } else { "un" }
        );
        self.is_corked = enable;
      }
      Ok(Err(e)) => {
        tracing::warn!(
          sca_handle = actor_handle,
          fd = fd_to_cork,
          cork_enable = enable,
          error = %e, "Failed to set TCP_CORK socket option"
        );
        // If setting cork failed, we should probably assume it's not in the desired state.
        // If enable=true failed, is_corked remains false.
        // If enable=false failed, is_corked might still be true (undesirable).
        // For robustness, if an attempt to change state fails, reflect that accurately.
        // However, setting cork is unlikely to fail when disabling if it was previously enabled,
        // unless the FD became invalid.
        // If we failed to enable, self.is_corked is already false.
        // If we failed to disable, it means self.is_corked was true, and we couldn't change it.
        // This state might be problematic. For now, we just log.
      }
      Err(join_err) => {
        // This is a Tokio JoinError, meaning the spawn_blocking task panicked.
        tracing::error!(
          sca_handle = actor_handle,
          fd = fd_to_cork,
          cork_enable = enable,
          error = %join_err, "Task for setting TCP_CORK panicked"
        );
      }
    }
  }
}

// Helper function that might be used by ZmtpProtocolHandlerX to initialize its cork_info field.
// This is defined outside the struct so it can be called conditionally.
pub(crate) fn try_create_cork_info<S: AsRawFd>(
  stream_option: Option<&S>,
  use_cork_config: bool,
) -> Option<TcpCorkInfoX> {
  if !use_cork_config {
    return None;
  }
  stream_option.and_then(TcpCorkInfoX::new)
}
