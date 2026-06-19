use crate::error::ZmqError;
use crate::socket::types::SocketType;

/// Validates whether two socket types are allowed to connect over `inproc`.
/// Returns `Ok(())` if compatible, or `Err(ZmqError::InvalidSocketType)` if incompatible.
pub(crate) fn validate_socket_compatibility(
  connector: SocketType,
  binder: SocketType,
) -> Result<(), ZmqError> {
  let compatible = match (connector, binder) {
    (SocketType::Push, SocketType::Pull) | (SocketType::Pull, SocketType::Push) => true,
    (SocketType::Pub, SocketType::Sub) | (SocketType::Sub, SocketType::Pub) => true,
    (SocketType::Req, SocketType::Rep) | (SocketType::Rep, SocketType::Req) => true,
    (SocketType::Dealer, SocketType::Router) | (SocketType::Router, SocketType::Dealer) => true,
    _ => false,
  };

  if compatible {
    Ok(())
  } else {
    Err(ZmqError::InvalidSocketType("Incompatible inproc socket patterns"))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_valid_compatibility_pairs() {
    assert!(validate_socket_compatibility(SocketType::Push, SocketType::Pull).is_ok());
    assert!(validate_socket_compatibility(SocketType::Pull, SocketType::Push).is_ok());
    assert!(validate_socket_compatibility(SocketType::Pub, SocketType::Sub).is_ok());
    assert!(validate_socket_compatibility(SocketType::Sub, SocketType::Pub).is_ok());
    assert!(validate_socket_compatibility(SocketType::Req, SocketType::Rep).is_ok());
    assert!(validate_socket_compatibility(SocketType::Rep, SocketType::Req).is_ok());
    assert!(validate_socket_compatibility(SocketType::Dealer, SocketType::Router).is_ok());
    assert!(validate_socket_compatibility(SocketType::Router, SocketType::Dealer).is_ok());
  }

  #[test]
  fn test_invalid_compatibility_pairs() {
    assert!(matches!(
      validate_socket_compatibility(SocketType::Push, SocketType::Pub),
      Err(ZmqError::InvalidSocketType(_))
    ));
    assert!(matches!(
      validate_socket_compatibility(SocketType::Req, SocketType::Req),
      Err(ZmqError::InvalidSocketType(_))
    ));
    assert!(matches!(
      validate_socket_compatibility(SocketType::Push, SocketType::Push),
      Err(ZmqError::InvalidSocketType(_))
    ));
    assert!(matches!(
      validate_socket_compatibility(SocketType::Dealer, SocketType::Dealer),
      Err(ZmqError::InvalidSocketType(_))
    ));
    assert!(matches!(
      validate_socket_compatibility(SocketType::Pub, SocketType::Req),
      Err(ZmqError::InvalidSocketType(_))
    ));
  }
}
