use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::error::ZmqError;
use crate::transport::ZmtpWriteHalf;
use super::egress_buffer::EgressBuffer;

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
}

impl<'a, W: ZmtpWriteHalf> EgressDriver<'a, W> {
  pub(crate) fn new(write_half: &'a mut W, egress_buffer: &'a mut EgressBuffer, max_iovecs: usize) -> Self {
    Self { write_half, egress_buffer, max_iovecs }
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
          Poll::Ready(Err(e)) => Poll::Ready(Err(ZmqError::from_io_endpoint(e, "egress flush"))),
          Poll::Pending => Poll::Pending,
        };
      }

      let n = {
        let mut slices: Vec<IoSlice<'_>> = Vec::new();
        this.egress_buffer.current_slices(this.max_iovecs, &mut slices);
        match Pin::new(&mut *this.write_half).poll_write_vectored(cx, &slices) {
          Poll::Ready(Ok(0)) => return Poll::Ready(Err(ZmqError::ConnectionClosed)),
          Poll::Ready(Ok(n)) => n,
          Poll::Pending => return Poll::Pending,
          Poll::Ready(Err(e)) => return Poll::Ready(Err(ZmqError::from_io_endpoint(e, "egress write"))),
        }
        // `slices` dropped here — immutable borrow of egress_buffer released.
      };
      this.egress_buffer.advance(n);
    }
  }
}
