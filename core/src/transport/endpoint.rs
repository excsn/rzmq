// src/transport/endpoint.rs

use crate::error::ZmqError;
use std::{
  net::SocketAddr,
  path::{Path, PathBuf},
};

/// Represents a parsed and validated endpoint address.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Endpoint {
  Tcp(std::net::SocketAddr, String), // Store original string for maps/disconnect
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
        // Parse TCP address
        address_part
          .parse::<SocketAddr>()
          .map(|addr| Endpoint::Tcp(addr, endpoint_str.to_string()))
          .map_err(|_| {
            tracing::debug!("Failed to parse TCP address: {}", address_part);
            invalid_endpoint_err()
          })
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
