use crate::error::ZmqError;
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::UringOpRequest;
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::signaling_op_sender::SignalingOpSender;
use crate::message::Msg;
use crate::runtime::SystemEvent;
use crate::runtime::{command::Command, mailbox::MailboxSender as SessionMailboxSender};
use crate::socket::events::MonitorSender;
use crate::socket::options::SocketOptions;
use crate::socket::SocketEvent;
#[cfg(feature = "io-uring")]
use crate::uring;
use crate::Context;

use std::any::Any;
use std::fmt;
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fibre::mpmc::AsyncSender;
#[cfg(feature = "io-uring")]
use fibre::mpsc;
#[cfg(feature = "io-uring")]
use fibre::oneshot::oneshot;
use fibre::{SendError, TrySendError};
use tokio::time::timeout as tokio_timeout;

#[async_trait]
pub(crate) trait ISocketConnection: Send + Sync + fmt::Debug {
  /// Sends a single message as a convenience wrapper around `send_multipart`.
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    // Default implementation wraps the single message in a Vec and calls the primary method.
    self.send_multipart(vec![msg]).await
  }

  /// Sends a complete logical message, which may consist of one or more parts.
  /// This is the primary method for sending data over the connection.
  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError>;

  async fn close_connection(&self) -> Result<(), ZmqError>;
  fn get_connection_id(&self) -> usize;
  fn as_any(&self) -> &dyn Any;
}

#[derive(Debug, Clone)]
pub(crate) struct DummyConnection;

#[async_trait]
impl ISocketConnection for DummyConnection {
  async fn send_message(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedFeature(
      "DummyConnection cannot send".into(),
    ))
  }

  async fn send_multipart(&self, _msgs: Vec<Msg>) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedFeature(
      "DummyConnection cannot send multipart".into(),
    ))
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    Ok(())
  }

  fn get_connection_id(&self) -> usize {
    0
  }

  fn as_any(&self) -> &dyn Any {
    self
  }
}

#[derive(Clone)]
pub(crate) struct SessionConnection {
  session_mailbox: SessionMailboxSender,
  connection_id: usize,
  pipe_to_session_tx: AsyncSender<Vec<Msg>>,
  socket_options: Arc<SocketOptions>,
  // context field to generate unique UserData for operations.
  // This is necessary if UringFdConnection needs a similar capability and we want to keep new() signatures consistent
  // or if SessionConnection itself might later interact with systems needing unique IDs.
  // For now, primarily for UringFdConnection pattern.
  #[allow(dead_code)]
  // May not be used if SessionConnection doesn't directly create ops for ExternalOpTracker
  context: Context,
}

impl fmt::Debug for SessionConnection {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("SessionConnection")
      .field("session_mailbox_closed", &self.session_mailbox.is_closed())
      .field("connection_id", &self.connection_id)
      .field(
        "pipe_to_session_tx_closed",
        &self.pipe_to_session_tx.is_closed(),
      )
      .field("socket_options", &self.socket_options)
      // Context doesn't have a simple Debug, so just indicate its presence
      .field("context_present", &true)
      .finish()
  }
}

impl SessionConnection {
  pub(crate) fn new(
    session_mailbox: SessionMailboxSender,
    connection_id: usize,
    pipe_to_session_tx: AsyncSender<Vec<Msg>>,
    socket_options: Arc<SocketOptions>,
    context: Context,
  ) -> Self {
    Self {
      session_mailbox,
      connection_id,
      pipe_to_session_tx,
      socket_options,
      context,
    }
  }
}

#[async_trait]
impl ISocketConnection for SessionConnection {
  // For SessionConnection, send_multipart sends each part individually.
  // ZMTP framing for the logical message happens at a higher level (e.g., in specific ISocket impls).
  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    let timeout_opt = self.socket_options.sndtimeo; // Get SNDTIMEO from stored options

    match timeout_opt {
      None => {
        // Infinite timeout (block until HWM allows or pipe closes)
        tracing::trace!(
          conn_id = self.connection_id,
          "SessionConnection: Sending multipart (blocking on HWM)"
        );

        self.pipe_to_session_tx.send(msgs).await.map_err(|_| {
          tracing::warn!(
            conn_id = self.connection_id,
            "SessionConnection: Pipe send failed (ConnectionClosed)"
          );
          ZmqError::ConnectionClosed
        })
      }
      Some(d) if d.is_zero() => {
        // Non-blocking (SNDTIMEO = 0)
        tracing::trace!(
          conn_id = self.connection_id,
          "SessionConnection: Attempting non-blocking multipart send via pipe"
        );
        match self.pipe_to_session_tx.try_send(msgs) {
          Ok(()) => Ok(()),
          Err(TrySendError::Full(_failed_msg_back)) => {
            tracing::trace!(
              conn_id = self.connection_id,
              "SessionConnection: Non-blocking pipe send failed (HWM - ResourceLimitReached)"
            );
            Err(ZmqError::ResourceLimitReached)
          }
          Err(TrySendError::Closed(_failed_msg_back)) => {
            tracing::warn!(
              conn_id = self.connection_id,
              "SessionConnection: Non-blocking pipe send failed (ConnectionClosed)"
            );
            Err(ZmqError::ConnectionClosed)
          }
          _ => unreachable!(),
        }
      }
      Some(timeout_duration) => {
        // Timed send (SNDTIMEO > 0)
        tracing::trace!(
            conn_id = self.connection_id,
            send_timeout_duration = ?timeout_duration,
            "SessionConnection: Attempting timed multipart send via pipe"
        );
        match tokio_timeout(timeout_duration, self.pipe_to_session_tx.send(msgs)).await {
          Ok(Ok(())) => Ok(()), // Sent within timeout
          Ok(Err(SendError::Closed)) => {
            // Pipe closed during timed send
            tracing::warn!(
              conn_id = self.connection_id,
              "SessionConnection: Timed pipe send failed (ConnectionClosed)"
            );
            Err(ZmqError::ConnectionClosed)
          }
          Err(_timeout_elapsed_error) => {
            // tokio::time::Timeout error (timeout elapsed)
            tracing::trace!(
              conn_id = self.connection_id,
              "SessionConnection: Timed pipe send failed (Timeout on HWM)"
            );
            Err(ZmqError::Timeout)
          }
          _ => unreachable!(),
        }
      }
    }
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    if self.session_mailbox.send(Command::Stop).await.is_err() {
      tracing::warn!(conn_id = self.connection_id, "Failed to send Stop command to Session mailbox (already closed?). Connection might not clean up fully via this path.");
    }
    Ok(())
  }
  fn get_connection_id(&self) -> usize {
    self.connection_id
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}

#[cfg(feature = "io-uring")]
pub(crate) struct UringFdConnection {
  fd: RawFd,
  // The application pushes messages here.
  mpsc_tx: mpsc::BoundedAsyncSender<Arc<Vec<Msg>>>,
  context: Context,
}

#[cfg(feature = "io-uring")]
impl fmt::Debug for UringFdConnection {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("UringFdConnection")
      .field("fd", &self.fd)
      .field("mpsc_tx_is_closed", &self.mpsc_tx.is_closed())
      .field("context_present", &true)
      .finish()
  }
}

#[cfg(feature = "io-uring")]
impl UringFdConnection {
  pub(crate) fn new(
    fd: RawFd,
    mpsc_tx: mpsc::BoundedAsyncSender<Arc<Vec<Msg>>>,
    context: Context,
  ) -> Self {
    Self {
      fd,
      mpsc_tx,
      context,
    }
  }

  /// Synchronously and non-blockingly attempts to send a multipart message.
  /// This is the primary method for sending data on this connection type.
  pub fn send_multipart_sync(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    match self.mpsc_tx.try_send(Arc::new(msgs)) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(_)) => {
        // This is the expected backpressure signal when the worker can't keep up.
        Err(ZmqError::ResourceLimitReached)
      }
      Err(TrySendError::Closed(_)) => {
        // The worker has dropped the receiver, meaning the connection is dead.
        Err(ZmqError::ConnectionClosed)
      }
      _ => unreachable!(),
    }
  }
}

#[cfg(feature = "io-uring")]
#[async_trait]
impl ISocketConnection for UringFdConnection {
  
  /// This method is now effectively synchronous, wrapping the non-blocking `try_send`.
  /// The `async` keyword is only here to satisfy the trait definition.
  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    self.send_multipart_sync(msgs)
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    // Closing is an async request to the worker.
    let (reply_tx, reply_rx) = oneshot();
    let unique_user_data = self.context.inner().next_handle() as u64;
    let req = UringOpRequest::ShutdownConnectionHandler {
      user_data: unique_user_data,
      fd: self.fd,
      reply_tx,
    };
    
    let worker_op_tx = uring::global_state::get_global_uring_worker_op_tx()?;
    worker_op_tx.send(req).await.map_err(|e| {
      ZmqError::Internal(format!("UringWorker op channel error for close: {}", e))
    })?;

    match tokio::time::timeout(Duration::from_secs(5), reply_rx.recv()).await {
      Ok(Ok(Ok(_))) => Ok(()),
      Ok(Ok(Err(e))) => Err(e),
      Ok(Err(_)) => Err(ZmqError::Internal("UringWorker reply channel error for close".into())),
      Err(_) => Err(ZmqError::Timeout),
    }
  }

  fn get_connection_id(&self) -> usize {
    self.fd as usize
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}

#[derive(Clone)]
pub(crate) struct InprocConnection {
  connection_id: usize,
  local_pipe_write_id_to_peer: usize,
  local_pipe_read_id_from_peer: usize,
  peer_inproc_name_or_uri: String,
  context: Context,
  data_tx_to_peer: AsyncSender<Vec<Msg>>,
  monitor_tx: Option<MonitorSender>,
  socket_options: Arc<SocketOptions>,
}

impl fmt::Debug for InprocConnection {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("InprocConnection")
      .field("connection_id", &self.connection_id)
      .field(
        "local_pipe_write_id_to_peer",
        &self.local_pipe_write_id_to_peer,
      )
      .field(
        "local_pipe_read_id_from_peer",
        &self.local_pipe_read_id_from_peer,
      )
      .field("peer_inproc_name_or_uri", &self.peer_inproc_name_or_uri)
      .field("context_present", &true) // Context doesn't have a simple Debug
      .field("data_tx_to_peer_closed", &self.data_tx_to_peer.is_closed())
      .field("monitor_tx_is_some", &self.monitor_tx.is_some())
      .field("socket_options", &self.socket_options)
      .finish()
  }
}

impl InprocConnection {
  pub(crate) fn new(
    connection_id: usize,
    local_pipe_write_id_to_peer: usize,
    local_pipe_read_id_from_peer: usize,
    peer_inproc_name_or_uri: String,
    context: Context,
    data_tx_to_peer: AsyncSender<Vec<Msg>>,
    monitor_tx: Option<MonitorSender>,
    socket_options: Arc<SocketOptions>,
  ) -> Self {
    Self {
      connection_id,
      local_pipe_write_id_to_peer,
      local_pipe_read_id_from_peer,
      peer_inproc_name_or_uri,
      context,
      data_tx_to_peer,
      monitor_tx,
      socket_options,
    }
  }
}

#[async_trait]
impl ISocketConnection for InprocConnection {
  async fn send_multipart(&self, msgs: Vec<Msg>) -> Result<(), ZmqError> {
    let timeout_opt = self.socket_options.sndtimeo;

    match timeout_opt {
      None => {
        self.data_tx_to_peer.send(msgs).await.map_err(|_| {
          tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection send failed (ConnectionClosed)");
          ZmqError::ConnectionClosed
        })
      }
      Some(d) if d.is_zero() => {
        match self.data_tx_to_peer.try_send(msgs) {
          Ok(()) => Ok(()),
          Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
          Err(TrySendError::Closed(_)) => {
            tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection non-blocking send failed (ConnectionClosed)");
            Err(ZmqError::ConnectionClosed)
          }
          _ => unreachable!(),
        }
      }
      Some(duration) => {
        match tokio_timeout(duration, self.data_tx_to_peer.send(msgs)).await {
          Ok(Ok(())) => Ok(()),
          Ok(Err(SendError::Closed)) => {
            tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection timed send failed (ConnectionClosed)");
            Err(ZmqError::ConnectionClosed)
          }
          Err(_) => Err(ZmqError::Timeout),
          _ => unreachable!(),
        }
      }
    }
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    tracing::debug!(
      conn_id = self.connection_id,
      peer = %self.peer_inproc_name_or_uri,
      local_read_pipe_id_being_closed = self.local_pipe_read_id_from_peer,
      "InprocConnection::close_connection called."
    );

    if let Some(ref monitor) = self.monitor_tx {
      let event = SocketEvent::Disconnected {
        endpoint: self.peer_inproc_name_or_uri.clone(),
      };
      if monitor.try_send(event).is_err() {
        tracing::warn!(
          conn_id = self.connection_id,
          peer = %self.peer_inproc_name_or_uri,
          "Failed to send Disconnected monitor event for inproc connection (channel full/closed)."
        );
      } else {
        tracing::debug!(
          conn_id = self.connection_id,
          peer = %self.peer_inproc_name_or_uri,
          "Sent Disconnected monitor event for inproc connection."
        );
      }
    }

    let target_name_for_event = self
      .peer_inproc_name_or_uri
      .strip_prefix("inproc://")
      .unwrap_or(&self.peer_inproc_name_or_uri)
      .to_string();

    let event = SystemEvent::InprocPipePeerClosed {
      target_inproc_name: target_name_for_event,
      closed_by_connector_pipe_read_id: self.local_pipe_read_id_from_peer,
    };

    if self.context.event_bus().publish(event).is_err() {
      tracing::warn!(
        conn_id = self.connection_id,
        peer = %self.peer_inproc_name_or_uri,
        "Failed to publish InprocPipePeerClosed event."
      );
    }
    Ok(())
  }
  fn get_connection_id(&self) -> usize {
    self.connection_id
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}
