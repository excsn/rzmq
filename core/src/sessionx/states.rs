// core/src/sessionx/states.rs

#![allow(dead_code)] // Allow dead code for now

use crate::context::Context; // For ActorConfig
use crate::runtime::MailboxSender as GenericMailboxSender; // For CorePipeManagerX
use crate::socket::events::MonitorSender;
use crate::{Blob, Msg}; // For CorePipeManagerX

use async_channel::Receiver as AsyncReceiver;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// General configuration and context for the SessionConnectionActorX.
#[derive(Debug, Clone)] // Clone if it needs to be passed around easily
pub(crate) struct ActorConfigX {
  pub context: Context,
  pub monitor_tx: Option<MonitorSender>,
  pub logical_target_endpoint_uri: String,
  pub connected_endpoint_uri: String,
  pub is_server_role: bool,
}

// Shell for CorePipeManagerX state, to be fleshed out later.
// Located here as it's primarily a state holder for pipe-related fields.
#[derive(Debug)]
pub(crate) struct CorePipeManagerXState {
  pub rx_from_core: Option<AsyncReceiver<Msg>>,
  pub parent_socket_core_mailbox: Option<GenericMailboxSender>,
  pub core_pipe_read_id_for_incoming_routing: Option<usize>,
  pub is_attached: bool,
  // Potential future additions for advanced backpressure:
  // pub incoming_to_core_buffer: std::collections::VecDeque<Msg>,
  // pub max_core_buffer_size: usize,
}

impl CorePipeManagerXState {
  pub(crate) fn new() -> Self {
    Self {
      rx_from_core: None,
      parent_socket_core_mailbox: None,
      core_pipe_read_id_for_incoming_routing: None,
      is_attached: false,
      // incoming_to_core_buffer: Default::default(),
      // max_core_buffer_size: 0, // or some default
    }
  }
}
