// core/src/sessionx/mod.rs

#![allow(dead_code)] // Allow dead code for now

// Sub-modules to be created
pub(crate) mod actor;
pub(crate) mod cork;
pub(crate) mod iface;
pub(crate) mod pipe_manager;
pub(crate) mod protocol_handler;
pub(crate) mod states;
pub(crate) mod types;

pub(crate) use iface::ScaConnectionIface; 
// Re-export key types for easier access within the sessionx module
pub(crate) use types::{
  ConnectionPhaseX, HandshakeSubPhaseX, ZmtpHandshakeProgressX,
};

// Re-export the main actor when it's defined
// pub(crate) use actor::SessionConnectionActorX;