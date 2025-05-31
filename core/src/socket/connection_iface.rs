// core/src/socket/connection_iface.rs

use crate::error::ZmqError;
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::one_shot_sender::OneShotSender as WorkerOneShotSender;
#[cfg(feature = "io-uring")]
use crate::io_uring_backend::ops::UringOpRequest;
use crate::message::Msg;
#[cfg(feature = "io-uring")]
use crate::runtime::global_uring_state;
use crate::runtime::SystemEvent;
use crate::runtime::{command::Command, mailbox::MailboxSender as SessionMailboxSender};
// <<< MODIFIED [SocketEvent needed for InprocConnection] >>>
use crate::socket::SocketEvent;
use crate::Context;
use std::any::Any;
#[cfg(feature = "io-uring")]
use tokio::sync::oneshot as tokio_oneshot;
// <<< MODIFIED [MonitorSender needed for InprocConnection] >>>
use crate::socket::events::MonitorSender;

use async_trait::async_trait;
use std::fmt;
#[cfg(feature = "io-uring")]
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Duration;

// ... (ISocketConnection trait, DummyConnection, SessionConnection, UringFdConnection remain the same)
#[async_trait]
pub(crate) trait ISocketConnection: Send + Sync + fmt::Debug {
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError>;
  async fn close_connection(&self) -> Result<(), ZmqError>;
  fn get_connection_id(&self) -> usize;
  fn as_any(&self) -> &dyn Any;
}

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

#[derive(Debug, Clone)]
pub(crate) struct SessionConnection {
  session_mailbox: SessionMailboxSender,
  connection_id: usize,
  pipe_to_session_tx: async_channel::Sender<Msg>,
}

impl SessionConnection {
  pub(crate) fn new(
    session_mailbox: SessionMailboxSender,
    connection_id: usize,
    pipe_to_session_tx: async_channel::Sender<Msg>,
  ) -> Self {
    Self {
      session_mailbox,
      connection_id,
      pipe_to_session_tx,
    }
  }
}

#[async_trait]
impl ISocketConnection for SessionConnection {
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    self.pipe_to_session_tx.send(msg).await.map_err(|e| {
      tracing::warn!(
        conn_id = self.connection_id,
        "Failed to send message to Session pipe: {}",
        e
      );
      ZmqError::Internal("Session data pipe closed or send error".into())
    })
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
#[derive(Debug, Clone)]
pub(crate) struct UringFdConnection {
  fd: RawFd,
  worker_op_tx: kanal::Sender<UringOpRequest>,
}

#[cfg(feature = "io-uring")]
impl UringFdConnection {
  pub(crate) fn new(fd: RawFd) -> Self {
    Self {
      fd,
      worker_op_tx: global_uring_state::get_global_uring_worker_op_tx(),
    }
  }
}

#[cfg(feature = "io-uring")]
#[async_trait]
impl ISocketConnection for UringFdConnection {
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
    match tokio::time::timeout(Duration::from_secs(5), reply_rx).await {
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
          "Timeout waiting for SendDataViaHandler ack from UringWorker."
        );
        Err(ZmqError::Timeout)
      }
    }
  }
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
  // <<< ADDED [monitor_tx field] >>>
  monitor_tx: Option<MonitorSender>,
  // <<< ADDED END >>>
}

impl InprocConnection {
  // <<< MODIFIED START [Added monitor_tx parameter to new] >>>
  pub(crate) fn new(
    connection_id: usize,
    local_pipe_write_id_to_peer: usize,
    local_pipe_read_id_from_peer: usize,
    peer_inproc_name_or_uri: String,
    context: Context,
    data_tx_to_peer: async_channel::Sender<Msg>,
    monitor_tx: Option<MonitorSender>, // Added parameter
  ) -> Self {
    Self {
      connection_id,
      local_pipe_write_id_to_peer,
      local_pipe_read_id_from_peer,
      peer_inproc_name_or_uri,
      context,
      data_tx_to_peer,
      monitor_tx, // Store it
    }
  }
  // <<< MODIFIED END >>>
}

#[async_trait]
impl ISocketConnection for InprocConnection {
  async fn send_message(&self, msg: Msg) -> Result<(), ZmqError> {
    self.data_tx_to_peer.send(msg).await.map_err(|e| {
      tracing::warn!(
        conn_id = self.connection_id,
        peer = %self.peer_inproc_name_or_uri,
        "InprocConnection send_message failed: {}", e
      );
      ZmqError::ConnectionClosed
    })
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    tracing::debug!(
      conn_id = self.connection_id,
      peer = %self.peer_inproc_name_or_uri,
      local_read_pipe_id_being_closed = self.local_pipe_read_id_from_peer,
      "InprocConnection::close_connection called."
    );

    // <<< ADDED START [Emit Disconnected event via stored monitor_tx] >>>
    if let Some(ref monitor) = self.monitor_tx {
      let event = SocketEvent::Disconnected {
        // Use self.peer_inproc_name_or_uri as the endpoint this connector was connected to.
        // Or, if this InprocConnection instance has its own URI (e.g. from EndpointInfo.endpoint_uri),
        // that might be more accurate if it's different from peer_inproc_name_or_uri.
        // For a connector, its EndpointInfo.endpoint_uri is "inproc://service_name", which is peer_inproc_name_or_uri.
        endpoint: self.peer_inproc_name_or_uri.clone(),
      };
      // Use try_send for monitor events as they are best-effort.
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
    // <<< ADDED END >>>

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

// ... (InprocBinderSideConnection remains the same)
#[derive(Debug, Clone)]
pub(crate) struct InprocBinderSideConnection {
  pub(crate) _connection_id: usize,
}

#[async_trait]
impl ISocketConnection for InprocBinderSideConnection {
  async fn send_message(&self, _msg: Msg) -> Result<(), ZmqError> {
    tracing::warn!("InprocBinderSideConnection::send_message called, should not happen for data path.");
    Err(ZmqError::UnsupportedFeature(
      "send_message not applicable for InprocBinderSideConnection direct use".into(),
    ))
  }
  async fn close_connection(&self) -> Result<(), ZmqError> {
    tracing::debug!(
      conn_id = self._connection_id,
      "InprocBinderSideConnection::close_connection called (NOP)."
    );
    Ok(())
  }
  fn get_connection_id(&self) -> usize {
    self._connection_id
  }
  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
