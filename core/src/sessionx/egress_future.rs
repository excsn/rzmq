use super::egress_buffer::EgressBuffer;
use crate::error::ZmqError;
use crate::transport::ZmtpWriteHalf;
use crate::{counter, log_egress_diagnostics};
use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Cancel-safe egress write future.
///
/// Borrows the durable `EgressBuffer` and the transport write half. If dropped
/// mid-write (e.g., a select! arm fires a different branch), the `EgressBuffer`
/// retains its `write_offset` and pending bytes so the next instance resumes
/// from the exact byte position — no data is lost or duplicated.
pub(crate) struct EgressDriver<'a, W: ZmtpWriteHalf> {
  write_half: &'a mut W,
  egress_buffer: &'a mut EgressBuffer,
  max_iovecs: usize,
  actor_handle: usize,
}

impl<'a, W: ZmtpWriteHalf> EgressDriver<'a, W> {
  pub(crate) fn new(
    write_half: &'a mut W,
    egress_buffer: &'a mut EgressBuffer,
    max_iovecs: usize,
    actor_handle: usize,
  ) -> Self {
    Self {
      write_half,
      egress_buffer,
      max_iovecs,
      actor_handle,
    }
  }
}

impl<'a, W: ZmtpWriteHalf> Unpin for EgressDriver<'a, W> {}

impl<'a, W: ZmtpWriteHalf> Future for EgressDriver<'a, W> {
  type Output = Result<(), ZmqError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    loop {
      if this.egress_buffer.is_empty() {
        return match Pin::new(&mut *this.write_half).poll_flush(cx) {
          Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
          Poll::Ready(Err(e)) => Poll::Ready(Err(ZmqError::from_io_endpoint(e, "egress write"))),
          Poll::Pending => Poll::Pending,
        };
      }

      let n = {
        // Create a 64-element stack array of IoSlices initialized with dummy data
        let dummy = &[];
        let mut slices = [std::io::IoSlice::new(dummy); 64];

        // Fill the stack array directly from the buffer without heap allocation
        let max_slices = this.max_iovecs.min(64);
        let count = this.egress_buffer.fill_slices(&mut slices[..max_slices]);

        match Pin::new(&mut *this.write_half).poll_write_vectored(cx, &slices[..count]) {
          Poll::Ready(Ok(0)) => return Poll::Ready(Err(ZmqError::ConnectionClosed)),
          Poll::Ready(Ok(n)) => n,
          Poll::Pending => return Poll::Pending,
          Poll::Ready(Err(e)) => {
            return Poll::Ready(Err(ZmqError::from_io_endpoint(e, "egress write")));
          }
        }
      };

      // 1. Advance buffer and retrieve how many logical messages were popped
      let popped_msgs = this.egress_buffer.advance(n);

      // 2. Accumulate pulled counters
      counter!(global, global_msgs_pulled, add, popped_msgs as u64);
      counter!(global, global_bytes_pulled, add, n as u64);

      log_egress_diagnostics!(
        this.actor_handle,
        this.egress_buffer.pending_messages(),
        this.egress_buffer.peak_messages(),
        this.egress_buffer.peak_bytes()
      );
    }
  }
}
