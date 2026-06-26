use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::connection_iface::ISocketConnection;
use async_trait::async_trait;
use fibre::mpmc::AsyncSender;
use fibre::TrySendError;
use std::any::Any;

#[derive(Debug, Clone)]
pub(crate) struct DirectInprocConnection {
  pub connection_id: usize,
  pub target_endpoint_uri: String,
  pub peer_queue_sender: AsyncSender<FrameBatch>,
}

#[async_trait]
impl ISocketConnection for DirectInprocConnection {
  async fn send_multipart(&self, msgs: FrameBatch) -> Result<(), ZmqError> {
    self.peer_queue_sender.send(msgs).await.map_err(|_| ZmqError::ConnectionClosed)
  }

  fn try_send_multipart_owned_sync(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    match self.peer_queue_sender.try_send(msgs) {
      Ok(()) => Ok(()),
      Err(TrySendError::Full(returned)) => Err((returned, ZmqError::ResourceLimitReached)),
      Err(TrySendError::Closed(returned)) => Err((returned, ZmqError::ConnectionClosed)),
      _ => unreachable!(),
    }
  }

  async fn close_connection(&self) -> Result<(), ZmqError> {
    self.peer_queue_sender.close();
    Ok(())
  }

  fn as_any(&self) -> &dyn Any {
    self
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::Msg;

  #[tokio::test]
  async fn test_successful_direct_send() {
    let (tx, rx) = fibre::mpmc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 100,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
    };

    let batch = FrameBatch::from(vec![Msg::from_static(b"hello-inproc")]);
    assert!(connection.send_multipart(batch).await.is_ok());

    let received = rx.recv().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].data().unwrap(), b"hello-inproc");
  }

  #[tokio::test]
  async fn test_synchronous_hwm_boundary() {
    let (tx, _rx) = fibre::mpmc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 101,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
    };

    let batch1 = FrameBatch::from(vec![Msg::from_static(b"frame-1")]);
    let batch2 = FrameBatch::from(vec![Msg::from_static(b"frame-2")]);

    let res1 = connection.try_send_multipart_owned_sync(batch1);
    assert!(res1.is_ok());

    let res2 = connection.try_send_multipart_owned_sync(batch2);
    assert!(res2.is_err());
    let (returned_batch, err) = res2.unwrap_err();
    assert!(matches!(err, ZmqError::ResourceLimitReached));
    assert_eq!(returned_batch[0].data().unwrap(), b"frame-2");
  }

  #[tokio::test]
  async fn test_peer_disconnected_on_send() {
    let (tx, rx) = fibre::mpmc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 102,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
    };

    drop(rx);

    let batch = FrameBatch::from(vec![Msg::from_static(b"lost-frame")]);
    let res = connection.send_multipart(batch).await;
    assert!(matches!(res, Err(ZmqError::ConnectionClosed)));
  }
}
