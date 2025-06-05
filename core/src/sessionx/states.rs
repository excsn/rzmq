// core/src/sessionx/states.rs

#![allow(dead_code)] // Allow dead code for now

use crate::context::Context; // For ActorConfig
use crate::socket::events::MonitorSender;
use crate::Msg; // For CorePipeManagerX

use async_channel::Receiver as AsyncReceiver;

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
  pub core_pipe_read_id_for_incoming_routing: Option<usize>,
  pub is_attached: bool,
}

impl CorePipeManagerXState {
  pub(crate) fn new() -> Self {
    Self {
      rx_from_core: None,
      core_pipe_read_id_for_incoming_routing: None,
      is_attached: false,
    }
  }
}
