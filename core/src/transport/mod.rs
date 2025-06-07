pub mod endpoint;
#[cfg(feature = "inproc")]
pub mod inproc;
#[cfg(feature = "ipc")]
pub mod ipc;
pub mod tcp;

use tokio::io::{AsyncRead, AsyncWrite};

/// Trait alias for streams usable by ZMTP connection actors.
pub(crate) trait ZmtpStdStream: AsyncRead + AsyncWrite + Unpin + Send + std::fmt::Debug + 'static {}

// Implement the marker trait for Tokio's streams
impl ZmtpStdStream for tokio::net::TcpStream {}
#[cfg(feature = "ipc")]
impl ZmtpStdStream for tokio::net::UnixStream {}