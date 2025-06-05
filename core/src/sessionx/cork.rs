// core/src/sessionx/cork.rs

#![allow(dead_code)] // Allow dead code for now

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, RawFd};

/// State specific to TCP_CORK management on Linux.
/// This struct will only be actively used if the underlying stream `S`
/// implements `AsRawFd` and the `use_cork` option is enabled.
#[derive(Debug)]
pub(crate) struct TcpCorkInfoX {
  #[cfg(target_os = "linux")]
  fd: RawFd,
  #[cfg(target_os = "linux")]
  is_corked: bool,
  /// Tracks if the *next* send operation is for the first frame of a new logical ZMQ message.
  /// This is important for deciding when to enable TCP_CORK.
  #[cfg(target_os = "linux")]
  expecting_first_frame_of_new_zmq_message: bool,
}

#[cfg(target_os = "linux")]
impl TcpCorkInfoX {
  /// Creates a new `TcpCorkInfoX` if the stream provides a raw file descriptor.
  /// Returns `None` if `fd` cannot be obtained (which shouldn't happen if `S: AsRawFd`).
  pub(crate) fn new<S: AsRawFd>(stream: &S) -> Option<Self> {
    // In a real scenario, we might not always have the stream at this exact point
    // if TcpCorkInfoX is part of ZmtpProtocolHandlerX constructed before stream is Some.
    // For now, assume stream is available to get the FD.
    // Alternatively, fd could be passed in if obtained earlier.
    let fd = stream.as_raw_fd();
    if fd < 0 {
      // Basic sanity check for a valid FD
      tracing::warn!("TcpCorkInfoX: Received invalid RawFd (<0) from stream, corking will be disabled.");
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
  /// `set_cork` is a blocking syscall.
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

    let res = tokio::task::spawn_blocking(move || {
      // SAFETY: We are assuming fd_to_cork is a valid socket FD.
      // The FromRawFd trait upholds safety if the FD is valid and owned correctly.
      // Here, we create a socket2::Socket temporarily.
      let socket = unsafe { socket2::Socket::from_raw_fd(fd_to_cork) };
      let result = socket.set_cork(enable);
      // Crucial: Prevent socket2::Socket from closing the FD when it's dropped.
      // The original stream still owns the FD.
      std::mem::forget(socket);
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
        // However, set_cork is unlikely to fail when disabling if it was previously enabled,
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

/// Fallback for non-Linux platforms or when `S` is not `AsRawFd`.
/// The `ZmtpProtocolHandlerX` can hold an `Option<TcpCorkInfoX>`,
/// which will be `None` in these cases.
/// Alternatively, to always have the field, `TcpCorkInfoX` could be an enum:
/// enum TcpCorkInfoX {
///   Linux { fd: RawFd, ... },
///   Unsupported,
/// }
/// For simplicity, we'll rely on `Option<TcpCorkInfoX>` in `ZmtpProtocolHandlerX`.
/// This file then only really defines the Linux-specific struct and its methods.

// Helper function that might be used by ZmtpProtocolHandlerX to initialize its cork_info field.
// This is defined outside the struct so it can be called conditionally.
#[cfg(target_os = "linux")]
pub(crate) fn try_create_cork_info<S: AsRawFd>(
  stream_option: Option<&S>,
  use_cork_config: bool,
) -> Option<TcpCorkInfoX> {
  if !use_cork_config {
    return None;
  }
  stream_option.and_then(TcpCorkInfoX::new)
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn try_create_cork_info<S>(_stream_option: Option<&S>, _use_cork_config: bool) -> Option<TcpCorkInfoX> {
  // On non-Linux, TcpCorkInfoX itself is not defined with fields,
  // or we simply always return None for the Option<TcpCorkInfoX> field.
  // To make this compile, we need a definition for TcpCorkInfoX on non-Linux.
  None
}

// If TcpCorkInfoX is intended to be a field in ZmtpProtocolHandlerX regardless of platform,
// we need a non-Linux definition.
#[cfg(not(target_os = "linux"))]
impl TcpCorkInfoX {
  // Provide minimal methods or make them no-ops for non-Linux if the struct exists.
  // If ZmtpProtocolHandlerX has Option<TcpCorkInfoX>, these aren't strictly needed
  // as it would be None.

  // This new constructor is for the non-Linux case IF TcpCorkInfoX is not an Option.
  // However, with Option<TcpCorkInfoX>, this `new` isn't needed for the non-Linux path.
  // pub(crate) fn new<S>(_stream: &S) -> Option<Self> { None }

  pub(crate) fn is_corked(&self) -> bool {
    false
  }
  pub(crate) fn is_expecting_first_frame(&self) -> bool {
    true
  } // Default that doesn't affect logic
  pub(crate) fn set_expecting_first_frame(&mut self, _expecting: bool) {}
  pub(crate) async fn apply_cork_state(&mut self, _enable: bool, _actor_handle: usize) {}
}
