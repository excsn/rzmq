// core/src/io_uring_backend/worker/external_op_tracker.rs

#![cfg(feature = "io-uring")]

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use crate::io_uring_backend::ops::{UserData, UringOpCompletion};
use crate::io_uring_backend::one_shot_sender::OneShotSender;
use crate::ZmqError; // For the Result in OneShotSender

#[derive(Debug)]
pub(crate) struct ExternalOpContext {
  pub reply_tx: OneShotSender<Result<UringOpCompletion, ZmqError>>,
  pub op_name: String,
  pub protocol_handler_factory_id: Option<String>, // For Listen/Connect
  pub fd_created_for_connect_op: Option<RawFd>,    // For Connect, FD before CQE
  pub listener_fd: Option<RawFd>,                  // For Listen, the FD of the successfully created listener
  pub target_fd_for_shutdown: Option<RawFd>,       // For ShutdownConnectionHandler
}

#[derive(Debug)]
pub(crate) struct ExternalOpTracker {
  pub(crate) in_flight: HashMap<UserData, ExternalOpContext>,
  pub(crate) next_id: UserData,
}

impl ExternalOpTracker {
  pub fn new() -> Self {
    Self {
      in_flight: HashMap::new(),
      next_id: 1, // Start from 1, 0 might be special for io-uring or reserved
    }
  }

  /// Generates a new unique UserData for an external operation.
  /// Ensures the ID stays within a range distinct from internal operations.
  pub fn new_op_id(&mut self) -> UserData {
    let id = self.next_id;
    self.next_id = self.next_id.wrapping_add(1);
    // Reserve 0 and ensure it doesn't wrap into the typical range for internal ops (e.g., >= 1_000_000_000)
    // Adjust max value as needed. 999_999_999 gives plenty of IDs before internal range.
    if self.next_id == 0 || self.next_id >= 1_000_000_000 {
        self.next_id = 1;
    }
    id
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
  pub fn take_op_if_shutdown_for_fd(&mut self, fd_to_check: RawFd) -> Option<(UserData, ExternalOpContext)> {
    let mut found_ud: Option<UserData> = None;
    for (ud, ctx) in self.in_flight.iter() {
        if ctx.op_name == "ShutdownConnectionHandler" && ctx.target_fd_for_shutdown == Some(fd_to_check) {
            found_ud = Some(*ud);
            break;
        }
    }
    if let Some(ud_to_remove) = found_ud {
        self.in_flight.remove(&ud_to_remove).map(|ctx| (ud_to_remove, ctx))
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
}