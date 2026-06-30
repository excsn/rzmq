use crate::message::FrameBatch;
use crate::runtime::{ActorType, EventBus, SystemEvent};
use crate::socket::patterns::ready_pipe_queue::PipeMessageSender;

use fibre::mpmc::AsyncReceiver;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub(crate) fn spawn(
  rx: AsyncReceiver<FrameBatch>,
  pipe_sender_opt: Option<PipeMessageSender>,
  event_bus: Arc<EventBus>,
  endpoint_uri: String,
  reader_task_id: usize,
  core_handle: usize,
  rcvbatch_count: usize,
) -> JoinHandle<()> {
  tokio::spawn(async move {
    let mut accumulator = FrameBatch::new();
    let mut drain_buf: Vec<FrameBatch> = Vec::with_capacity(rcvbatch_count);
    let mut out: VecDeque<FrameBatch> = VecDeque::new();

    'outer: loop {
      match rx.recv().await {
        Ok(batch) => drain_buf.push(batch),
        Err(_) => break,
      }
      rx.try_recv_batch_mut(&mut drain_buf, rcvbatch_count - 1);

      for batch in drain_buf.drain(..) {
        accumulator.extend(batch);
        if accumulator.last_mut().map(|m| !m.is_more()).unwrap_or(false) {
          out.push_back(std::mem::replace(&mut accumulator, FrameBatch::new()));
        }
      }

      if let Some(ref sender) = pipe_sender_opt {
        sender.try_send_batch(&mut out);
        while let Some(front) = out.pop_front() {
          if sender.send(front).await.is_err() {
            break 'outer;
          }
          sender.try_send_batch(&mut out);
        }
      } else {
        out.clear();
      }
    }

    tracing::debug!(reader_task_id, uri = %endpoint_uri, "DirectInproc reader task exiting.");
    let _ = event_bus.publish(SystemEvent::ActorStopping {
      handle_id: reader_task_id,
      actor_type: ActorType::PipeReader,
      endpoint_uri: Some(endpoint_uri),
      parent_id: Some(core_handle),
      error: None,
    });
  })
}
