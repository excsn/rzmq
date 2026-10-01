#![cfg(feature = "io-uring")]

use fibre::oneshot;

use crate::io_uring_backend::ops::{ProtocolConfig, UringOpCompletion, UserData};
use crate::runtime::MailboxSyncSender;
use crate::ZmqError;
use std::collections::HashMap;
use std::os::unix::io::RawFd;

#[derive(Debug)]
pub(crate) struct ExternalOpContext {
  pub reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  pub op_name: String,
  pub protocol_handler_factory_id: Option<String>,
  pub protocol_config: Option<ProtocolConfig>,
  pub socket_mailbox: Option<MailboxSyncSender>, // For Listen/Connect/RegisterExternalFd
  pub fd_created_for_connect_op: Option<RawFd>,  // For Connect, FD before CQE
  pub target_fd_for_shutdown: Option<RawFd>,
}

#[derive(Debug)]
pub(crate) struct ExternalOpTracker {
  pub(crate) in_flight: HashMap<UserData, ExternalOpContext>,
}

impl ExternalOpTracker {
  pub fn new() -> Self {
    Self {
      in_flight: HashMap::new(),
    }
  }

  pub fn add_op(&mut self, user_data: UserData, context: ExternalOpContext) {
    if self.in_flight.contains_key(&user_data) {
      // This should ideally not happen if UserData is generated uniquely
      tracing::warn!(
        "ExternalOpTracker: Overwriting existing in-flight operation for UserData {}",
        user_data
      );
    }
    self.in_flight.insert(user_data, context);
  }

  pub fn take_op(&mut self, user_data: UserData) -> Option<ExternalOpContext> {
    self.in_flight.remove(&user_data)
  }

  /// Takes an operation if it's a ShutdownConnectionHandler targeting the specified FD.
  pub fn take_op_if_shutdown_for_fd(
    &mut self,
    fd_to_check: RawFd,
  ) -> Option<(UserData, ExternalOpContext)> {
    let mut found_ud: Option<UserData> = None;
    for (ud, ctx) in self.in_flight.iter() {
      if ctx.op_name == "ShutdownConnectionHandler"
        && ctx.target_fd_for_shutdown == Some(fd_to_check)
      {
        found_ud = Some(*ud);
        break;
      }
    }
    if let Some(ud_to_remove) = found_ud {
      self
        .in_flight
        .remove(&ud_to_remove)
        .map(|ctx| (ud_to_remove, ctx))
    } else {
      None
    }
  }

  pub fn is_empty(&self) -> bool {
    self.in_flight.is_empty()
  }

  #[allow(dead_code)] // May be useful for graceful shutdown logic
  pub fn drain_all(&mut self) -> Vec<(UserData, ExternalOpContext)> {
    self.in_flight.drain().collect()
  }

  pub(crate) fn get_op_context_ref(&self, user_data: UserData) -> Option<&ExternalOpContext> {
    self.in_flight.get(&user_data)
  }
}
