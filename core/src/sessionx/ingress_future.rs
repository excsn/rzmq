use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use crate::message::FrameBatch;
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;
use crate::ZmqError;

pub(crate) struct IngressDriver<'a> {
  sender_opt: Option<&'a PipeMessageSender>,
  ingress_buffer: &'a mut VecDeque<FrameBatch>,
  // Lazily initialized on first poll() so new() never touches the buffer.
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
    Self { sender_opt, ingress_buffer, fut: None }
  }
}

impl<'a> Future for IngressDriver<'a> {
  type Output = Result<usize, ZmqError>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let this = self.get_mut();

    // Lazy init: only runs on the first poll(), which only fires when the guard is true.
    if this.fut.is_none() {
      if this.ingress_buffer.is_empty() {
        return Poll::Pending;
      }
      match this.sender_opt {
        None => {
          // No pipe sender (PUSH/PUB) — silently drain.
          let batch = this.ingress_buffer.pop_front().unwrap();
          return Poll::Ready(Ok(batch.len()));
        }
        Some(sender) => {
          let batch = this.ingress_buffer.front().unwrap().clone();
          this.fut = Some(Box::pin(sender.send(batch)));
        }
      }
    }

    match this.fut.as_mut().unwrap().as_mut().poll(cx) {
      Poll::Ready(Ok(())) => {
        let batch = this.ingress_buffer.pop_front().unwrap();
        Poll::Ready(Ok(batch.len()))
      }
      Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
      Poll::Pending => Poll::Pending,
    }
  }
}
