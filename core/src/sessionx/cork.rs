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
  /// This operation is performed in a blocking Tokio task because `socket2`s
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

    let optval: libc::c_int = if enable { 1 } else { 0 };

    // Perform the setsockopt synchronously directly on the raw file descriptor.
    // This executes in the nanosecond range and eliminates thread spawning overhead.
    let res = unsafe {
      libc::setsockopt(
        fd_to_cork,
        libc::IPPROTO_TCP,
        libc::TCP_CORK,
        &optval as *const _ as *const libc::c_void,
        std::mem::size_of_val(&optval) as libc::socklen_t,
      )
    };

    if res == 0 {
      tracing::debug!(
        sca_handle = actor_handle,
        fd = fd_to_cork,
        cork_enable = enable,
        "TCP_CORK successfully {}set",
        if enable { "" } else { "un" }
      );
      self.is_corked = enable;
    } else {
      let e = std::io::Error::last_os_error();
      tracing::warn!(
        sca_handle = actor_handle,
        fd = fd_to_cork,
        cork_enable = enable,
        error = %e, "Failed to set TCP_CORK socket option"
      );
    }
  }
}

pub(crate) fn try_create_cork_info<S: AsRawFd>(
  stream_option: Option<&S>,
  use_cork_config: bool,
) -> Option<TcpCorkInfoX> {
  if !use_cork_config {
    return None;
  }
  stream_option.and_then(TcpCorkInfoX::new)
}