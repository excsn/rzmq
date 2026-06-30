pub(crate) mod actor;
pub(crate) mod egress_buffer;
pub(crate) mod egress_driver;
pub(crate) mod message_processor;
#[cfg(target_os = "linux")]
pub(crate) mod cork;
pub(crate) mod iface;
pub(crate) mod ingress_future;
pub(crate) mod pipe_manager;
pub(crate) mod regulator;
pub(crate) mod states;
pub(crate) mod types;

pub(crate) use iface::ScaConnectionIface;

/// Per-read iteration chunk size for the synchronous greedy drain stack buffer.
/// Must be a compile-time constant because it sizes a stack array.
#[cfg(target_os = "macos")]
pub(crate) const INGRESS_GREEDY_CHUNK: usize = 65536;
#[cfg(not(target_os = "macos"))]
pub(crate) const INGRESS_GREEDY_CHUNK: usize = 65536 * 8;