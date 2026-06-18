use super::egress_buffer::EgressBuffer;
use crate::error::ZmqError;
use crate::transport::ZmtpWriteHalf;
use std::future::Future;
use std::io::IoSlice;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

// Unified global counters accumulated across all 8 independent connection buffers
pub(crate) static GLOBAL_MSGS_PULLED: AtomicU64 = AtomicU64::new(0);
pub(crate) static GLOBAL_BYTES_PULLED: AtomicU64 = AtomicU64::new(0);

static LAST_BYTES_SENT: AtomicU64 = AtomicU64::new(0);
static LAST_PRINT_TIME: std::sync::OnceLock<std::sync::Mutex<Instant>> = std::sync::OnceLock::new();

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
      GLOBAL_MSGS_PULLED.fetch_add(popped_msgs as u64, Ordering::Relaxed);
      let total_bytes_written =
        GLOBAL_BYTES_PULLED.fetch_add(n as u64, Ordering::Relaxed) + n as u64;

      let now = Instant::now();
      let print_time_mutex = LAST_PRINT_TIME.get_or_init(|| std::sync::Mutex::new(now));

      if let Ok(mut last_print_time) = print_time_mutex.try_lock() {
        let elapsed = now.duration_since(*last_print_time);
        if elapsed >= Duration::from_secs(1) {
          let last_bytes = LAST_BYTES_SENT.load(Ordering::Relaxed);
          let delta_bytes = total_bytes_written.saturating_sub(last_bytes);

          let mb_sec = (delta_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();
          let total_mb = total_bytes_written as f64 / 1_048_576.0;

          println!(
            "[EgressDriver ID: {}] Total: {:.2} MB | Speed: {:.2} MB/s | Buffer Msgs: {} | Peak Msgs: {} | Peak Bytes: {} | Global Drained Msgs: {} | Global Pulled Msgs: {}",
            this.actor_handle, // Output the unique actor ID
            total_mb,
            mb_sec,
            this.egress_buffer.pending_messages(),
            this.egress_buffer.peak_messages(),
            this.egress_buffer.peak_bytes(),
            crate::sessionx::actor::GLOBAL_DRAINED_MSGS.load(Ordering::Relaxed),
            GLOBAL_MSGS_PULLED.load(Ordering::Relaxed)
          );

          LAST_BYTES_SENT.store(total_bytes_written, Ordering::Relaxed);
          *last_print_time = now;
        }
      }
    }
  }
}
