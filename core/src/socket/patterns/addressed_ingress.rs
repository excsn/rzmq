use crate::message::FrameBatch;
use crate::ZmqError;
use crate::socket::patterns::ready_pipe_queue::{PipeMessageSender, ReadyPipeQueue};

pub(crate) struct AddressedIngressEngine {
  queue: ReadyPipeQueue<FrameBatch>,
}

impl AddressedIngressEngine {
  pub fn new(activation_capacity: usize) -> Self {
    Self {
      queue: ReadyPipeQueue::new(activation_capacity),
    }
  }

  pub fn register_pipe(&self, pipe_id: usize, capacity: usize, drain_delta: usize) -> PipeMessageSender {
    let sender = self.queue.register_pipe(pipe_id, capacity, drain_delta);
    PipeMessageSender::DirectAddressed { sender }
  }

  pub fn deregister_pipe(&self, pipe_id: usize) {
    self.queue.deregister_pipe(pipe_id);
  }

  pub fn close(&self) {
    self.queue.close();
  }

  pub async fn recv_logical_message(
    &self,
    rcvtimeo_opt: Option<std::time::Duration>,
  ) -> Result<(usize, FrameBatch), ZmqError> {
    match rcvtimeo_opt {
      Some(d) if d.is_zero() => self.queue.try_pop().ok_or(ZmqError::ResourceLimitReached),
      Some(d) => tokio::time::timeout(d, self.queue.pop())
        .await
        .map_err(|_| ZmqError::Timeout)?,
      None => self.queue.pop().await,
    }
  }
}

impl std::fmt::Debug for AddressedIngressEngine {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("AddressedIngressEngine").finish_non_exhaustive()
  }
}
