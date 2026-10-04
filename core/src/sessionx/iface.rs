#![allow(dead_code)]

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::ConservationCounters;
use crate::socket::patterns::PipeCredits;

use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fibre::TrySendError;
use fibre::mpmc::AsyncSender as BoundedAsyncSender;
use tokio::time::timeout;

/// The session actor currently draining a pipe. A connect-side pipe outlives
/// its sessions, so this is rebound on every reconnect and empty in between.
#[derive(Debug, Clone)]
struct BoundSession {
  stop_mailbox: MailboxSender,
  handle_id: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ScaConnectionIface {
  session: Arc<RwLock<Option<BoundSession>>>,
  pipe_sender: BoundedAsyncSender<FrameBatch>,
  pipe_write_id_to_sca: usize,
  sndtimeo: Option<Duration>,
  credits: Option<Arc<PipeCredits>>,
  counters: Arc<ConservationCounters>,
}

impl ScaConnectionIface {
  pub(crate) fn new(
    sca_stop_mailbox: MailboxSender,
    sca_handle_id: usize,
    pipe_sender: BoundedAsyncSender<FrameBatch>,
    pipe_write_id_to_sca: usize,
    sndtimeo: Option<Duration>,
    credits: Option<Arc<PipeCredits>>,
    counters: Arc<ConservationCounters>,
  ) -> Self {
    let iface = Self::unbound(pipe_sender, pipe_write_id_to_sca, sndtimeo, credits, counters);
    iface.bind_session(sca_stop_mailbox, sca_handle_id);
    iface
  }

  /// A pipe with no session yet: sends queue in the pipe up to its capacity.
  pub(crate) fn unbound(
    pipe_sender: BoundedAsyncSender<FrameBatch>,
    pipe_write_id_to_sca: usize,
    sndtimeo: Option<Duration>,
    credits: Option<Arc<PipeCredits>>,
    counters: Arc<ConservationCounters>,
  ) -> Self {
    Self {
      session: Arc::new(RwLock::new(None)),
      pipe_sender,
      pipe_write_id_to_sca,
      sndtimeo,
      credits,
      counters,
    }
  }

  fn count_sent(&self) {
    ConservationCounters::add(&self.counters.sent, 1);
  }

  pub(crate) fn bind_session(&self, stop_mailbox: MailboxSender, handle_id: usize) {
    *self.session.write() = Some(BoundSession { stop_mailbox, handle_id });
  }

  pub(crate) fn unbind_session(&self) {
    *self.session.write() = None;
  }

  pub(crate) fn session_handle_id(&self) -> Option<usize> {
    self.session.read().as_ref().map(|s| s.handle_id)
  }

  fn sca_handle_id(&self) -> usize {
    self.session_handle_id().unwrap_or(0)
  }
}

#[async_trait]
impl ISocketConnection for ScaConnectionIface {
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    tracing::trace!(
      sca_handle = self.sca_handle_id(),
      pipe_id = self.pipe_write_id_to_sca,
      "ScaConnectionIface sending single message via data pipe."
    );

    let mut fb = FrameBatch::new();
    fb.push(msg);
    match self.pipe_sender.try_send(fb) {
      Ok(()) => {
        self.count_sent();
        return Ok(());
      }
      Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
      Err(TrySendError::Full(_)) if self.sndtimeo == Some(Duration::ZERO) => {
        return Err(ZmqError::ResourceLimitReached);
      }
      Err(TrySendError::Full(returned_fb)) => {
        let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
        return match timeout(timeout_duration, self.pipe_sender.send(returned_fb)).await {
          Ok(Ok(())) => {
            self.count_sent();
            Ok(())
          }
          Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
          Err(_) => Err(ZmqError::ResourceLimitReached),
        };
      }
      Err(TrySendError::Sent(_)) => unreachable!(),
    }
  }

  async fn send_multipart(&self, msgs: FrameBatch) -> Result<(), ZmqError> {
    tracing::trace!(
      sca_handle = self.sca_handle_id(),
      pipe_id = self.pipe_write_id_to_sca,
      num_msgs = msgs.len(),
      "ScaConnectionIface sending multipart via data pipe."
    );

    match self.pipe_sender.try_send(msgs) {
      Ok(()) => {
        self.count_sent();
        return Ok(());
      }
      Err(TrySendError::Closed(_)) => return Err(ZmqError::ConnectionClosed),
      Err(TrySendError::Full(_)) if self.sndtimeo == Some(Duration::ZERO) => {
        return Err(ZmqError::ResourceLimitReached);
      }
      Err(TrySendError::Full(returned_msgs)) => {
        let timeout_duration = self.sndtimeo.unwrap_or(Duration::from_secs(30));
        return match timeout(timeout_duration, self.pipe_sender.send(returned_msgs)).await {
          Ok(Ok(())) => {
            self.count_sent();
            Ok(())
          }
          Ok(Err(_)) => Err(ZmqError::ConnectionClosed),
          Err(_) => Err(ZmqError::ResourceLimitReached),
        };
      }
      Err(TrySendError::Sent(_)) => unreachable!(),
    }
  }

  async fn send_multipart_owned(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    match self.pipe_sender.try_send(msgs) {
      Ok(()) => {
        self.count_sent();
        Ok(())
      }
      Err(TrySendError::Closed(returned)) => Err((returned, ZmqError::ConnectionClosed)),
      Err(TrySendError::Full(returned)) if self.sndtimeo == Some(Duration::ZERO) => {
        Err((returned, ZmqError::ResourceLimitReached))
      }
      Err(TrySendError::Full(returned)) => match self.sndtimeo {
        None => match self.pipe_sender.send(returned).await {
          Ok(()) => {
            self.count_sent();
            Ok(())
          }
          Err(_) => Err((FrameBatch::new(), ZmqError::ConnectionClosed)),
        },
        Some(duration) => match timeout(duration, self.pipe_sender.send(returned)).await {
          Ok(Ok(())) => {
            self.count_sent();
            Ok(())
          }
          Ok(Err(_)) => Err((FrameBatch::new(), ZmqError::ConnectionClosed)),
          Err(_) => Err((FrameBatch::new(), ZmqError::Timeout)),
        },
      },
      Err(TrySendError::Sent(_)) => unreachable!(),
    }
  }

  fn try_send_multipart_owned_sync(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    match self.pipe_sender.try_send(msgs) {
      Ok(()) => {
        self.count_sent();
        Ok(())
      }
      Err(TrySendError::Full(returned)) => Err((returned, ZmqError::ResourceLimitReached)),
      Err(TrySendError::Closed(returned)) => Err((returned, ZmqError::ConnectionClosed)),
      Err(TrySendError::Sent(_)) => unreachable!(),
    }
  }

  fn pipe_credits(&self) -> Option<&Arc<PipeCredits>> {
    self.credits.as_ref()
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    let Some(session) = self.session.read().clone() else {
      return Ok(());
    };
    tracing::debug!(
      "ScaConnectionIface: Sending Stop command to SCA {}.",
      session.handle_id
    );
    session.stop_mailbox.send(Command::Stop).await.map_err(|e| {
      ZmqError::Internal(format!(
        "Failed to send Stop to SCA {}: {}",
        session.handle_id, e
      ))
    })
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
