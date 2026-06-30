use crate::error::ZmqError;
use crate::message::FrameBatch;
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::events::{MonitorSender, SocketEvent, clean_endpoint_uri};
use async_trait::async_trait;
use fibre::mpsc::BoundedAsyncSender;
use fibre::TrySendError;
use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub(crate) struct DirectInprocConnection {
  pub connection_id: usize,
  pub target_endpoint_uri: String,
  pub peer_queue_sender: BoundedAsyncSender<FrameBatch>,
  pub monitor_tx: Option<MonitorSender>,
  pub is_congested: Arc<AtomicBool>,
  pub sndtimeo: Option<Duration>,
}

#[async_trait]
impl ISocketConnection for DirectInprocConnection {
  async fn send_multipart(&self, msgs: FrameBatch) -> Result<(), ZmqError> {
    match self.send_multipart_owned(msgs).await {
      Ok(()) => Ok(()),
      Err((_, e)) => Err(e),
    }
  }

  async fn send_multipart_owned(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    let clean_endpoint = clean_endpoint_uri(&self.target_endpoint_uri).to_owned();

    match self.peer_queue_sender.try_send(msgs) {
      Ok(()) => {
        if self.peer_queue_sender.is_full() {
          if !self.is_congested.swap(true, Ordering::AcqRel) {
            if let Some(ref tx) = self.monitor_tx {
              let _ = tx.try_send(SocketEvent::ConnectionCongested {
                endpoint: clean_endpoint,
              });
            }
          }
        }
        Ok(())
      }
      Err(TrySendError::Closed(returned)) => Err((returned, ZmqError::ConnectionClosed)),
      Err(TrySendError::Full(returned)) => {
        if self.sndtimeo == Some(Duration::ZERO) {
          if !self.is_congested.swap(true, Ordering::AcqRel) {
            if let Some(ref tx) = self.monitor_tx {
              let _ = tx.try_send(SocketEvent::ConnectionCongested {
                endpoint: clean_endpoint,
              });
            }
          }
          return Err((returned, ZmqError::ResourceLimitReached));
        }

        if !self.is_congested.swap(true, Ordering::AcqRel) {
          if let Some(ref tx) = self.monitor_tx {
            let _ = tx.try_send(SocketEvent::ConnectionCongested {
              endpoint: clean_endpoint.clone(),
            });
          }
        }

        let timeout_dur = self.sndtimeo.unwrap_or(Duration::from_secs(300));
        match tokio::time::timeout(timeout_dur, self.peer_queue_sender.send(returned)).await {
          Ok(Ok(())) => {
            if !self.peer_queue_sender.is_full() {
              if self.is_congested.swap(false, Ordering::AcqRel) {
                if let Some(ref tx) = self.monitor_tx {
                  let _ = tx.try_send(SocketEvent::ConnectionUncongested {
                    endpoint: clean_endpoint,
                  });
                }
              }
            }
            Ok(())
          }
          Ok(Err(_)) => Err((FrameBatch::new(), ZmqError::ConnectionClosed)),
          Err(_) => Err((FrameBatch::new(), ZmqError::Timeout)),
        }
      }
      _ => unreachable!(),
    }
  }

  fn try_send_multipart_owned_sync(&self, msgs: FrameBatch) -> Result<(), (FrameBatch, ZmqError)> {
    let clean_endpoint = clean_endpoint_uri(&self.target_endpoint_uri).to_owned();
    match self.peer_queue_sender.try_send(msgs) {
      Ok(()) => {
        if self.peer_queue_sender.is_full() {
          if !self.is_congested.swap(true, Ordering::AcqRel) {
            if let Some(ref tx) = self.monitor_tx {
              let _ = tx.try_send(SocketEvent::ConnectionCongested {
                endpoint: clean_endpoint,
              });
            }
          }
        } else {
          if self.is_congested.swap(false, Ordering::AcqRel) {
            if let Some(ref tx) = self.monitor_tx {
              let _ = tx.try_send(SocketEvent::ConnectionUncongested {
                endpoint: clean_endpoint,
              });
            }
          }
        }
        Ok(())
      }
      Err(TrySendError::Full(returned)) => {
        if !self.is_congested.swap(true, Ordering::AcqRel) {
          if let Some(ref tx) = self.monitor_tx {
            let _ = tx.try_send(SocketEvent::ConnectionCongested {
              endpoint: clean_endpoint,
            });
          }
        }
        Err((returned, ZmqError::ResourceLimitReached))
      }
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
    let (tx, rx) = fibre::mpsc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 100,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
      monitor_tx: None,
      is_congested: Arc::new(AtomicBool::new(false)),
      sndtimeo: None,
    };

    let batch = FrameBatch::from(vec![Msg::from_static(b"hello-inproc")]);
    assert!(connection.send_multipart(batch).await.is_ok());

    let received = rx.recv().await.unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].data().unwrap(), b"hello-inproc");
  }

  #[tokio::test]
  async fn test_synchronous_hwm_boundary() {
    let (tx, _rx) = fibre::mpsc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 101,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
      monitor_tx: None,
      is_congested: Arc::new(AtomicBool::new(false)),
      sndtimeo: None,
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
    let (tx, rx) = fibre::mpsc::bounded_async::<FrameBatch>(1);
    let connection = DirectInprocConnection {
      connection_id: 102,
      target_endpoint_uri: "inproc://test-uri".to_string(),
      peer_queue_sender: tx,
      monitor_tx: None,
      is_congested: Arc::new(AtomicBool::new(false)),
      sndtimeo: None,
    };

    drop(rx);

    let batch = FrameBatch::from(vec![Msg::from_static(b"lost-frame")]);
    let res = connection.send_multipart(batch).await;
    assert!(matches!(res, Err(ZmqError::ConnectionClosed)));
  }
}
