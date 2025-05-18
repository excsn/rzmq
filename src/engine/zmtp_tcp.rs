// src/engine/zmtp_tcp.rs

use crate::runtime::{mailbox, ActorType, Command, MailboxSender}; // Added Context, ActorType (though ActorType not directly used here)
use crate::socket::options::{SocketOptions, ZmtpEngineConfig};
use crate::Context;

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use super::core::ZmtpEngineCore;

/// Creates and spawns the ZMTP Engine actor task for a TCP stream.
/// Returns the MailboxSender for the spawned engine task and its JoinHandle.
// <<< MODIFIED START [Signature accepts Context and parent_id] >>>
pub(crate) fn create_and_spawn_tcp_engine(
  handle: usize, // Engine's own handle ID
  session_mailbox: MailboxSender,
  stream: TcpStream,
  options: Arc<SocketOptions>,
  is_server: bool,
  context: &Context, // Accept Context reference
  parent_id: usize,  // ID of the parent Session actor
) -> (MailboxSender, JoinHandle<()>) {
  // <<< MODIFIED END >>>

  let (tx, rx) = mailbox(); // Mailbox for the engine actor

  let config = ZmtpEngineConfig {
    // Copy relevant fields from socket options
    routing_id: options.routing_id.clone(),
    socket_type_name: options.socket_type_name.clone(),
    heartbeat_ivl: options.heartbeat_ivl,
    heartbeat_timeout: options.heartbeat_timeout,
    // io_uring options need checking based on feature flag
    #[cfg(feature = "io-uring")]
    use_send_zerocopy: true, // TODO: Get from options dynamically if needed
    #[cfg(feature = "io-uring")]
    use_recv_multishot: true, // TODO: Get from options dynamically if needed
  };

  // <<< MODIFIED START [Pass Context to ZmtpEngineCore::new] >>>
  // Create the generic core state with TcpStream as the type
  let core = ZmtpEngineCore::<TcpStream>::new(
    handle,
    session_mailbox,
    rx, // Pass the receiving end of the engine's mailbox
    stream,
    config,
    is_server,
    context.clone(), // Pass context clone to the engine core
  );
  // <<< MODIFIED END >>>

  // Spawn the generic core loop task
  let task_handle = tokio::spawn(core.run_loop()); // run_loop consumes core

  // Note: ActorStarted event is published by the caller (e.g., Listener/Connecter)
  // immediately after this function returns successfully and the task is known to be spawned.

  // Return the mailbox sender *for the engine task* and the task handle
  (tx, task_handle)
}
