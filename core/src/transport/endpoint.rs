use crate::error::ZmqError;
use std::path::PathBuf;

/// Represents a parsed and validated endpoint address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Endpoint {
  Tcp(String, String), // Store address part (e.g., "my-host:5555") and original URI
  #[cfg(feature = "ipc")]
  Ipc(PathBuf, String), // Store validated PathBuf and original string
  #[cfg(feature = "inproc")]
  Inproc(String), // Store name
}

/// Parses an endpoint string into a structured Endpoint enum.
pub(crate) fn parse_endpoint(endpoint_str: &str) -> Result<Endpoint, ZmqError> {
  let invalid_endpoint_err = || ZmqError::InvalidEndpoint(endpoint_str.to_string());

  // Find the scheme separator "://"
  if let Some(separator_pos) = endpoint_str.find("://") {
    let scheme = &endpoint_str[..separator_pos];
    let address_part = &endpoint_str[separator_pos + 3..]; // Skip "://"

    // Match on the scheme
    match scheme {
      "tcp" => {
        if address_part.is_empty() {
          Err(invalid_endpoint_err())
        } else {
          Ok(Endpoint::Tcp(
            address_part.to_string(),
            endpoint_str.to_string(),
          ))
        }
      }

      #[cfg(feature = "ipc")]
      "ipc" => {
        // Parse IPC path
        if address_part.is_empty() || address_part.contains('\0') {
          Err(invalid_endpoint_err())
        } else {
          let path = PathBuf::from(address_part);
          Ok(Endpoint::Ipc(path, endpoint_str.to_string()))
        }
      }

      #[cfg(feature = "inproc")]
      "inproc" => {
        // Parse Inproc name
        if address_part.is_empty() || address_part.contains('\0') {
          Err(invalid_endpoint_err())
        } else {
          Ok(Endpoint::Inproc(address_part.to_string()))
        }
      }

      // Handle schemes disabled by features or unknown schemes
      _ => Err(ZmqError::UnsupportedTransport(endpoint_str.to_string())),
    } // End match scheme
  } else {
    // No "://" found, invalid format
    Err(invalid_endpoint_err())
  }
}

// Optional: Add methods to Endpoint enum?
// impl Endpoint { pub fn scheme(&self) -> &'static str { ... } }

#[cfg(test)]
mod additional_endpoint_tests {
  use super::*;

  #[test]
  fn test_parse_valid_tcp_endpoint() {
    let uri = "tcp://127.0.0.1:5555";
    let parsed = parse_endpoint(uri).expect("Should parse valid TCP URI");

    match parsed {
      Endpoint::Tcp(addr, original) => {
        assert_eq!(addr, "127.0.0.1:5555");
        assert_eq!(original, uri);
      }
      #[allow(unreachable_patterns)]
      _ => panic!("Expected Endpoint::Tcp"),
    }
  }

  #[test]
  fn test_parse_invalid_format() {
    assert!(matches!(
      parse_endpoint("tcp:/127.0.0.1"),
      Err(ZmqError::InvalidEndpoint(_))
    ));
    assert!(matches!(
      parse_endpoint("tcp://"),
      Err(ZmqError::InvalidEndpoint(_))
    ));
  }

  #[test]
  fn test_parse_unsupported_scheme() {
    let result = parse_endpoint("udp://127.0.0.1:5555");
    assert!(matches!(result, Err(ZmqError::UnsupportedTransport(_))));
  }

  #[test]
  #[cfg(feature = "ipc")]
  fn test_parse_valid_ipc_endpoint() {
    let uri = "ipc:///tmp/rzmq.sock";
    let parsed = parse_endpoint(uri).expect("Should parse valid IPC URI");

    match parsed {
      Endpoint::Ipc(path, original) => {
        assert_eq!(path.to_str().unwrap(), "/tmp/rzmq.sock");
        assert_eq!(original, uri);
      }
      _ => panic!("Expected Endpoint::Ipc"),
    }
  }

  #[test]
  #[cfg(feature = "inproc")]
  fn test_parse_valid_inproc_endpoint() {
    let uri = "inproc://my-service";
    let parsed = parse_endpoint(uri).expect("Should parse valid inproc URI");

    match parsed {
      Endpoint::Inproc(name) => {
        assert_eq!(name, "my-service");
      }
      _ => panic!("Expected Endpoint::Inproc"),
    }
  }
}
