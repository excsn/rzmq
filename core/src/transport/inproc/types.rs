use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch};
use crate::socket::types::SocketType;
use fibre::mpsc::BoundedAsyncSender;
use fibre::oneshot;

pub(crate) struct InprocHandshakeRequest {
  pub connector_id: usize,
  pub connector_socket_type: SocketType,
  pub connector_identity: Option<Blob>,
  pub connector_rx_sender: BoundedAsyncSender<FrameBatch>,
  pub reply_tx: oneshot::Sender<Result<InprocHandshakeResponse, ZmqError>>,
}

#[derive(Clone)]
pub(crate) struct InprocHandshakeResponse {
  pub binder_id: usize,
  pub binder_socket_type: SocketType,
  pub binder_identity: Option<Blob>,
  pub binder_rx_sender: BoundedAsyncSender<FrameBatch>,
}

impl InprocHandshakeRequest {
  pub fn verify_response(&self, response: &InprocHandshakeResponse) -> Result<(), ZmqError> {
    super::handshake::validate_socket_compatibility(
      self.connector_socket_type,
      response.binder_socket_type,
    )
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::socket::types::SocketType;
  use fibre::oneshot;

  fn dummy_handshake_request(
    socket_type: SocketType,
  ) -> (
    InprocHandshakeRequest,
    oneshot::Receiver<Result<InprocHandshakeResponse, ZmqError>>,
  ) {
    let (tx_to_connector, _) = fibre::mpsc::bounded_async::<FrameBatch>(1);
    let (reply_tx, reply_rx) = oneshot::oneshot();

    let request = InprocHandshakeRequest {
      connector_id: 1,
      connector_socket_type: socket_type,
      connector_identity: None,
      connector_rx_sender: tx_to_connector,
      reply_tx,
    };
    (request, reply_rx)
  }

  #[test]
  fn test_handshake_contract_matching_types() {
    let (request, _rx) = dummy_handshake_request(SocketType::Push);
    let (tx_to_binder, _) = fibre::mpsc::bounded_async::<FrameBatch>(1);

    let valid_response = InprocHandshakeResponse {
      binder_id: 2,
      binder_socket_type: SocketType::Pull,
      binder_identity: None,
      binder_rx_sender: tx_to_binder,
    };

    assert!(request.verify_response(&valid_response).is_ok());
  }

  #[test]
  fn test_handshake_contract_mismatched_types() {
    let (request, _rx) = dummy_handshake_request(SocketType::Push);
    let (tx_to_binder, _) = fibre::mpsc::bounded_async::<FrameBatch>(1);

    let invalid_response = InprocHandshakeResponse {
      binder_id: 2,
      binder_socket_type: SocketType::Pub,
      binder_identity: None,
      binder_rx_sender: tx_to_binder,
    };

    assert!(matches!(
      request.verify_response(&invalid_response),
      Err(ZmqError::InvalidSocketType(_))
    ));
  }
}
