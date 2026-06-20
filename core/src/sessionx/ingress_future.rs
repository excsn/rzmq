use crate::message::FrameBatch;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::ZmqError;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(crate) struct IngressDriver<'a> {
  sender_opt: Option<&'a PipeMessageSender>,
  ingress_buffer: &'a mut VecDeque<FrameBatch>,
  // Set only when try_send_batch hits backpressure; cleared after the async send resolves.
  fut: Option<Pin<Box<dyn Future<Output = Result<(), ZmqError>> + Send + 'a>>>,
}

impl<'a> IngressDriver<'a> {
  /// Safe to call even when `ingress_buffer` is empty — no buffer access occurs here.
  /// The select! guard (`if !ingress_buffer.is_empty()`) prevents poll() from being
  /// called in that case, so the lazy init in poll() is always safe.
  pub fn new(
    sender_opt: Option<&'a PipeMessageSender>,
    ingress_buffer: &'a mut VecDeque<FrameBatch>,
  ) -> Self {
    Self {
      sender_opt,
      ingress_buffer,
      fut: None,
    }
  }
}

impl<'a> Future for IngressDriver<'a> {
  type Output = Result<usize, ZmqError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();
    let mut total_sent = 0;

    // Resolve any in-flight async send (from a prior backpressure stall) first.
    if let Some(fut) = this.fut.as_mut() {
      match fut.as_mut().poll(cx) {
        Poll::Ready(Ok(())) => {
          this.fut = None;
          let batch = this.ingress_buffer.pop_front().unwrap();
          total_sent += batch.len();
          // Fall through to bulk-drain whatever's left.
        }
        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        Poll::Pending => return Poll::Pending,
      }
    }

    match this.sender_opt {
      None => {
        // No pipe sender (PUB/PUSH send-only) — silently discard.
        for batch in this.ingress_buffer.drain(..) {
          total_sent += batch.len();
        }
      }
      Some(sender) => {
        // Coalesced batch push: O(1) atomics and one wakeup for the whole buffer.
        total_sent += sender.try_send_batch(this.ingress_buffer);

        // If items remain, the front hit backpressure. Fall back to async to
        // register the waker so this future is re-polled when the pipe drains.
        if let Some(blocked) = this.ingress_buffer.front() {
          let fut = sender.send(blocked.clone());
          this.fut = Some(Box::pin(fut));
          match this.fut.as_mut().unwrap().as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
              this.fut = None;
              let sent = this.ingress_buffer.pop_front().unwrap();
              total_sent += sent.len();
            }
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => {
              return if total_sent > 0 {
                Poll::Ready(Ok(total_sent))
              } else {
                Poll::Pending
              };
            }
          }
        }
      }
    }

    if total_sent > 0 {
      Poll::Ready(Ok(total_sent))
    } else {
      Poll::Pending
    }
  }
}
