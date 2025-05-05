// src/engine/zmtp_tcp.rs

use crate::runtime::{mailbox, MailboxSender};
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCore;

/// Creates and spawns the ZMTP Engine actor task for a TCP stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
pub(crate) fn create_and_spawn_tcp_engine(
  handle: usize,
  session_mailbox: MailboxSender,
  stream: TcpStream,
  options: Arc<SocketOptions>,
  is_server: bool,
) -> (MailboxSender, JoinHandle<()>) {
  let (tx, rx) = mailbox();

  
  let config = ZmtpEngineConfig {
    // Copy relevant fields
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    // Add heartbeat options
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    // io_uring options need checking based on feature flag
    #[cfg(feature = "io-uring")]
    use_send_zerocopy: true, // TODO: Get from options
    #[cfg(feature = "io-uring")]
    use_recv_multishot: true, // TODO: Get from options
  };
  
  let core = ZmtpEngineCore::<TcpStream>::new(handle, session_mailbox, rx, stream, config, is_server);
  let task_handle = tokio::spawn(core.run_loop());
  (tx, task_handle)
}
