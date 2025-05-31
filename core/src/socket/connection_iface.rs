// core/src/socket/connection_iface.rs

use crate::error::ZmqError;
use crate::message::Msg;
use crate::runtime::SystemEvent;
use crate::runtime::{command::Command, mailbox::MailboxSender as SessionMailboxSender};
// <<< ADDED [SocketOptions needed for SNDTIMEO] >>>
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::one_shot_sender::OneShotSender as WorkerOneShotSender;
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::UringOpRequest;
#[cfg(feature = "io-uring")]
use crate::runtime::global_uring_state;
use crate::socket::events::MonitorSender;
use crate::socket::options::SocketOptions;
use crate::socket::SocketEvent;
use crate::Context;
use std::any::Any;
#[cfg(feature = "io-uring")]
use tokio::sync::oneshot as tokio_oneshot;

use async_trait::async_trait;
use std::fmt;
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;
// <<< ADDED [For send_message timeout logic] >>>
use async_channel::{SendError, TrySendError};
use tokio::time::timeout as tokio_timeout;

#[async_trait]
pub(crate) trait ISocketConnection: Send + Sync + fmt::Debug {
  // <<< MODIFIED [send_message no longer needs explicit timeout, it will use stored options] >>>
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError>;
  async fn close_connection(&self) -> Result<(), ZmqError>;
  fn get_connection_id(&self) -> usize;
  fn as_any(&self) -> &dyn Any;
}

// ... (DummyConnection remains the same)
#[derive(Debug, Clone)]
pub(crate) struct DummyConnection;

#[async_trait]
impl ISocketConnection for DummyConnection {
  async fn send_message(&self, _msg: Msg) -> Result<(), ZmqError> {
    Err(ZmqError::UnsupportedFeature("DummyConnection cannot send".into()))
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

// <<< MODIFIED START [SessionConnection struct and new method] >>>
#[derive(Debug)]
// Not Clone anymore due to SocketOptions potentially being mutated by SocketCore elsewhere if not careful
// However, SocketOptions is Arc'd from SocketCore's state which is under RwLock, so cloning Arc<SocketOptions> is fine.
// Let's make it Clone if SocketOptions is Arc<SocketOptions>.
// If SocketOptions is directly held, then not Clone.
// Current SocketOptions in CoreState is not Arc'd, so SessionConnection cannot be Clone if it holds SocketOptions directly.
// Let's make it hold Arc<SocketOptions> for clonability and safety.
#[derive(Clone)] // Now it can be Clone again
pub(crate) struct SessionConnection {
  session_mailbox: SessionMailboxSender,
  connection_id: usize,
  pipe_to_session_tx: async_channel::Sender<Msg>,
  // <<< ADDED [Field to store socket options, primarily for SNDTIMEO] >>>
  socket_options: Arc<SocketOptions>,
}

impl SessionConnection {
  pub(crate) fn new(
    session_mailbox: SessionMailboxSender,
    connection_id: usize,
    pipe_to_session_tx: async_channel::Sender<Msg>,
    // <<< ADDED [socket_options parameter] >>>
    socket_options: Arc<SocketOptions>,
  ) -> Self {
    Self {
      session_mailbox,
      connection_id,
      pipe_to_session_tx,
      socket_options, // Store it
    }
  }
}
// <<< MODIFIED END >>>

#[async_trait]
impl ISocketConnection for SessionConnection {
  // <<< MODIFIED START [send_message now uses stored socket_options for SNDTIMEO] >>>
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    let timeout_opt = self.socket_options.sndtimeo; // Get SNDTIMEO from stored options

    match timeout_opt {
      None => {
        // Infinite timeout (block until HWM allows or pipe closes)
        tracing::trace!(
          conn_id = self.connection_id,
          // pipe_id = self.pipe_to_session_tx.id_somehow(), // async_channel Sender doesn't expose an ID easily
          "SessionConnection: Sending message (blocking on HWM)"
        );
        self
          .pipe_to_session_tx
          .send(msg)
          .await
          .map_err(|SendError(_failed_msg_back)| {
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
          "SessionConnection: Attempting non-blocking send via pipe"
        );
        match self.pipe_to_session_tx.try_send(msg) {
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
        }
      }
      Some(timeout_duration) => {
        // Timed send (SNDTIMEO > 0)
        tracing::trace!(
            conn_id = self.connection_id,
            send_timeout_duration = ?timeout_duration,
            "SessionConnection: Attempting timed send via pipe"
        );
        match tokio_timeout(timeout_duration, self.pipe_to_session_tx.send(msg)).await {
          Ok(Ok(())) => Ok(()), // Sent within timeout
          Ok(Err(SendError(_failed_msg_back))) => {
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
        }
      }
    }
  }
  // <<< MODIFIED END >>>

  async fn close_connection(&self) -> Result<(), ZmqError> {
    // ... (close_connection remains the same)
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

// UringFdConnection::send_message already has its own timeout logic for worker reply,
// which could be tied to SNDTIMEO if passed or accessed.
#[cfg(feature = "io-uring")]
#[derive(Debug, Clone)]
pub(crate) struct UringFdConnection {
  fd: RawFd,
  worker_op_tx: kanal::Sender<UringOpRequest>,
  // <<< ADDED [Field to store socket options, primarily for SNDTIMEO] >>>
  socket_options: Arc<SocketOptions>,
}

#[cfg(feature = "io-uring")]
impl UringFdConnection {
  // <<< MODIFIED [new takes Arc<SocketOptions>] >>>
  pub(crate) fn new(fd: RawFd, socket_options: Arc<SocketOptions>) -> Self {
    Self {
      fd,
      worker_op_tx: global_uring_state::get_global_uring_worker_op_tx(),
      socket_options,
    }
  }
}

#[cfg(feature = "io-uring")]
#[async_trait]
impl ISocketConnection for UringFdConnection {
  // <<< MODIFIED START [send_message uses stored socket_options.sndtimeo for its internal timeout] >>>
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = tokio_oneshot::channel();
    let req = UringOpRequest::SendDataViaHandler {
      user_data: 0,
      fd: self.fd,
      app_data: Arc::new(msg),
      reply_tx: WorkerOneShotSender::new(reply_tx),
    };

    self.worker_op_tx.as_async().send(req).await.map_err(|e| {
      tracing::error!(
        fd = self.fd,
        "Failed to send SendDataViaHandler request to UringWorker: {}",
        e
      );
      ZmqError::Internal(format!("UringWorker op channel error for send: {}", e))
    })?;

    // Use SNDTIMEO for the timeout waiting for UringWorker's ACK.
    // If SNDTIMEO is None (infinite), wait indefinitely.
    // If SNDTIMEO is Some(ZERO), it's tricky. SendDataViaHandler is async to worker.
    // A true non-blocking check isn't simple here. For now, use a very small timeout or default if ZERO.
    let ack_timeout = self.socket_options.sndtimeo.unwrap_or(Duration::from_secs(5)); // Default 5s if infinite SNDTIMEO
    let effective_ack_timeout = if ack_timeout.is_zero() {
      Duration::from_millis(1)
    } else {
      ack_timeout
    };

    match tokio_timeout(effective_ack_timeout, reply_rx).await {
      Ok(Ok(Ok(_completion))) => Ok(()),
      Ok(Ok(Err(e))) => {
        tracing::warn!(fd = self.fd, "UringWorker reported error for SendDataViaHandler: {}", e);
        Err(e)
      }
      Ok(Err(oneshot_err)) => {
        tracing::error!(
          fd = self.fd,
          "OneShot channel error waiting for SendDataViaHandler ack: {}",
          oneshot_err
        );
        Err(ZmqError::Internal("UringWorker reply channel error for send".into()))
      }
      Err(_timeout_elapsed) => {
        tracing::error!(
          fd = self.fd,
          "Timeout waiting for SendDataViaHandler ack from UringWorker (timeout used: {:?}).",
          effective_ack_timeout
        );
        // If original SNDTIMEO was zero, ResourceLimitReached might be more appropriate if the intent was "don't wait for worker ack"
        if self.socket_options.sndtimeo == Some(Duration::ZERO) {
          Err(ZmqError::ResourceLimitReached)
        } else {
          Err(ZmqError::Timeout)
        }
      }
    }
  }
  // <<< MODIFIED END >>>
  // ... (close_connection, get_connection_id, as_any for UringFdConnection remain the same)
  async fn close_connection(&self) -> Result<(), ZmqError> {
    let (reply_tx, reply_rx) = tokio_oneshot::channel();
    let req = UringOpRequest::ShutdownConnectionHandler {
      user_data: 0,
      fd: self.fd,
      reply_tx: WorkerOneShotSender::new(reply_tx),
    };
    self.worker_op_tx.as_async().send(req).await.map_err(|e| {
      tracing::error!(
        fd = self.fd,
        "Failed to send ShutdownConnectionHandler request to UringWorker: {}",
        e
      );
      ZmqError::Internal(format!("UringWorker op channel error for close: {}", e))
    })?;
    match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
      Ok(Ok(Ok(_completion))) => Ok(()),
      Ok(Ok(Err(e))) => {
        tracing::warn!(
          fd = self.fd,
          "UringWorker reported error for ShutdownConnectionHandler: {}",
          e
        );
        Err(e)
      }
      Ok(Err(oneshot_err)) => {
        tracing::error!(
          fd = self.fd,
          "OneShot channel error waiting for ShutdownConnectionHandler ack: {}",
          oneshot_err
        );
        Err(ZmqError::Internal("UringWorker reply channel error for close".into()))
      }
      Err(_timeout_elapsed) => {
        tracing::error!(
          fd = self.fd,
          "Timeout waiting for ShutdownConnectionHandler ack from UringWorker."
        );
        Err(ZmqError::Timeout)
      }
    }
  }
  fn get_connection_id(&self) -> usize {
    self.fd as usize
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}

#[derive(Debug)]
pub(crate) struct InprocConnection {
  connection_id: usize,
  local_pipe_write_id_to_peer: usize,
  local_pipe_read_id_from_peer: usize,
  peer_inproc_name_or_uri: String,
  context: Context,
  data_tx_to_peer: async_channel::Sender<Msg>,
  monitor_tx: Option<MonitorSender>,
  // <<< ADDED [Field to store socket options, primarily for SNDTIMEO] >>>
  socket_options: Arc<SocketOptions>,
}

impl InprocConnection {
  // <<< MODIFIED START [Added socket_options parameter to new] >>>
  pub(crate) fn new(
    connection_id: usize,
    local_pipe_write_id_to_peer: usize,
    local_pipe_read_id_from_peer: usize,
    peer_inproc_name_or_uri: String,
    context: Context,
    data_tx_to_peer: async_channel::Sender<Msg>,
    monitor_tx: Option<MonitorSender>,
    socket_options: Arc<SocketOptions>, // Added parameter
  ) -> Self {
    Self {
      connection_id,
      local_pipe_write_id_to_peer,
      local_pipe_read_id_from_peer,
      peer_inproc_name_or_uri,
      context,
      data_tx_to_peer,
      monitor_tx,
      socket_options, // Store it
    }
  }
  // <<< MODIFIED END >>>
}

#[async_trait]
impl ISocketConnection for InprocConnection {
  // <<< MODIFIED START [send_message now uses stored socket_options for SNDTIMEO] >>>
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    let timeout_opt = self.socket_options.sndtimeo; // Get SNDTIMEO from stored options

    match timeout_opt {
      None => {
        self.data_tx_to_peer.send(msg).await.map_err(|SendError(_)| {
          tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection send failed (ConnectionClosed)");
          ZmqError::ConnectionClosed
        })
      }
      Some(d) if d.is_zero() => {
        match self.data_tx_to_peer.try_send(msg) {
          Ok(()) => Ok(()),
          Err(TrySendError::Full(_)) => Err(ZmqError::ResourceLimitReached),
          Err(TrySendError::Closed(_)) => {
            tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection non-blocking send failed (ConnectionClosed)");
            Err(ZmqError::ConnectionClosed)
          }
        }
      }
      Some(duration) => {
        match tokio_timeout(duration, self.data_tx_to_peer.send(msg)).await {
          Ok(Ok(())) => Ok(()),
          Ok(Err(SendError(_))) => {
            tracing::warn!(conn_id = self.connection_id, peer = %self.peer_inproc_name_or_uri, "InprocConnection timed send failed (ConnectionClosed)");
            Err(ZmqError::ConnectionClosed)
          }
          Err(_) => Err(ZmqError::Timeout),
        }
      }
    }
  }
  // <<< MODIFIED END >>>

  // ... (close_connection, get_connection_id, as_any for InprocConnection remain the same)
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

    self.data_tx_to_peer.close();

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
