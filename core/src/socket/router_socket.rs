// core/src/socket/router_socket.rs

use crate::delegate_to_core;
use crate::error::ZmqError;
use crate::message::{Blob, Msg, MsgFlags};
use crate::runtime::{Command, MailboxSender};
use crate::socket::connection_iface::ISocketConnection;
use crate::socket::core::state::EndpointInfo;
use crate::socket::core::{CoreState, SocketCore};
use crate::socket::options::ROUTER_MANDATORY;
use crate::socket::patterns::{incoming_orchestrator::IncomingMessageOrchestrator, RouterMap, WritePipeCoordinator};
use crate::socket::{ISocket, SourcePipeReadId};

// Removed: use std::collections::HashMap; // If only used for pipe_to_identity_shared_map
use dashmap::DashMap; // Added
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
// Removed: use parking_lot::{RwLock as ParkingLotRwLock, RwLockReadGuard as ParkingLotRwLockReadGuard}; // If no longer needed
use parking_lot::RwLockReadGuard as ParkingLotRwLockReadGuard; // Keep if core_state uses it
use tokio::sync::{Mutex as TokioMutex, OwnedSemaphorePermit};

use super::core::command_processor::update_core_option;
use super::parse_bool_option;

/// Structure to hold state for an ongoing fragmented send.
#[derive(Debug)]
struct ActiveFragmentedSend {
    target_endpoint_uri: String,
    _permit: OwnedSemaphorePermit,
}

// Removed RouterMessageProcessCtx as it's no longer needed with DashMap approach
// struct RouterMessageProcessCtx<'a> {
//   identity_map_guard: ParkingLotRwLockReadGuard<'a, HashMap<usize, Blob>>,
// }

#[derive(Debug)]
pub(crate) struct RouterSocket {
    core: Arc<SocketCore>,
    router_map_for_send: RouterMap,
    incoming_orchestrator: IncomingMessageOrchestrator,
    pipe_to_identity_shared_map: Arc<DashMap<usize, Blob>>, // Changed
    current_send_target: TokioMutex<Option<ActiveFragmentedSend>>,
    pipe_send_coordinator: Arc<WritePipeCoordinator>,
    counter: AtomicU64,
}

impl RouterSocket {
    pub fn new(core: Arc<SocketCore>) -> Self {
        let orchestrator = IncomingMessageOrchestrator::new(&core);

        Self {
            core,
            router_map_for_send: RouterMap::new(),
            incoming_orchestrator: orchestrator,
            pipe_to_identity_shared_map: Arc::new(DashMap::new()), // Changed
            current_send_target: TokioMutex::new(None),
            pipe_send_coordinator: Arc::new(WritePipeCoordinator::new()),
            counter: AtomicU64::new(0),
        }
    }

    fn core_state_read(&self) -> ParkingLotRwLockReadGuard<'_, CoreState> {
        self.core.core_state.read()
    }

    fn pipe_id_to_placeholder_identity(pipe_read_id: usize) -> Blob {
        Blob::from_bytes(Bytes::from(format!("pipe:{}", pipe_read_id)))
    }

    // Changed signature: takes identity_blob directly
    fn process_raw_message_for_router(
        pipe_read_id: usize, // For logging
        mut raw_zmtp_message: Vec<Msg>,
        identity_blob: Blob, // Identity is now passed directly
    ) -> Result<Vec<Msg>, ZmqError> {
        // identity_blob is already resolved and passed in

        let mut id_msg = Msg::from_vec(identity_blob.to_vec()); // .to_vec() on Blob clones its Bytes
        id_msg.set_flags(MsgFlags::MORE);

        if !raw_zmtp_message.is_empty() && raw_zmtp_message[0].size() == 0 {
            tracing::trace!(
                "Router transform: Stripped empty ZMTP delimiter from pipe {}.",
                pipe_read_id
            );
            raw_zmtp_message.remove(0);
        }

        let mut application_message = Vec::with_capacity(1 + raw_zmtp_message.len());
        application_message.push(id_msg);
        application_message.extend(raw_zmtp_message);

        Ok(application_message)
    }
}

#[async_trait]
impl ISocket for RouterSocket {
    fn core(&self) -> &Arc<SocketCore> {
        &self.core
    }
    fn mailbox(&self) -> MailboxSender {
        self.core.command_sender()
    }

    async fn bind(&self, endpoint: &str) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserBind, endpoint: endpoint.to_string())
    }
    async fn connect(&self, endpoint: &str) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserConnect, endpoint: endpoint.to_string())
    }
    async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserDisconnect, endpoint: endpoint.to_string())
    }
    async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserUnbind, endpoint: endpoint.to_string())
    }
    async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserSetOpt, option: option, value: value.to_vec())
    }
    async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
        delegate_to_core!(self, UserGetOpt, option: option)
    }
    async fn close(&self) -> Result<(), ZmqError> {
        delegate_to_core!(self, UserClose,)
    }

    async fn send(&self, mut msg: Msg) -> Result<(), ZmqError> {
        if !self.core.is_running().await {
            return Err(ZmqError::InvalidState("Socket is closing".into()));
        }

        let core_opts_arc = self.core.core_state.read().options.clone();
        let timeout_opt: Option<Duration> = core_opts_arc.sndtimeo;
        let router_mandatory_opt: bool = core_opts_arc.router_mandatory;

        let active_target_uri_if_fragmenting: Option<String> = {
            let guard = self.current_send_target.lock().await;
            guard.as_ref().map(|active_info| active_info.target_endpoint_uri.clone())
        };

        if let Some(target_uri_for_payload) = active_target_uri_if_fragmenting {
            let conn_iface_for_payload: Option<Arc<dyn ISocketConnection>> = self
                .core
                .core_state
                .read()
                .endpoints
                .get(&target_uri_for_payload)
                .map(|ep_info| ep_info.connection_iface.clone());

            let conn_iface = match conn_iface_for_payload {
                Some(iface) => iface,
                None => {
                    let mut clear_target_guard = self.current_send_target.lock().await;
                    *clear_target_guard = None;
                    return if router_mandatory_opt {
                        Err(ZmqError::HostUnreachable("Peer for fragmented send disappeared".into()))
                    } else {
                        Ok(())
                    };
                }
            };

            let is_last_user_part = !msg.is_more();
            let send_result = conn_iface.send_message(msg).await;

            if is_last_user_part {
                let mut clear_target_guard = self.current_send_target.lock().await;
                *clear_target_guard = None;
            }

            return match send_result {
                Ok(()) => Ok(()),
                Err(e) => {
                    if router_mandatory_opt {
                        Err(if matches!(e, ZmqError::ConnectionClosed) {
                            ZmqError::HostUnreachable("Peer disconnected during payload send".into())
                        } else {
                            e
                        })
                    } else {
                        Ok(())
                    }
                }
            };
        } else {
            if !msg.is_more() {
                return Err(ZmqError::InvalidMessage(
                    "ROUTER send: First frame (identity) must have MORE flag set".into(),
                ));
            }
            let destination_id = Blob::from_bytes(msg.data_bytes().unwrap_or_default());
            if destination_id.is_empty() {
                return Err(ZmqError::InvalidMessage(
                    "ROUTER send: Identity frame cannot be empty".into(),
                ));
            }

            let target_endpoint_uri = match self.router_map_for_send.get_uri_for_identity(&destination_id).await {
                Some(uri) => uri,
                None => {
                    return if router_mandatory_opt {
                        Err(ZmqError::HostUnreachable(format!(
                            "Peer {:?} not found (ROUTER_MANDATORY)",
                            destination_id
                        )))
                    } else {
                        Ok(())
                    };
                }
            };

            let (conn_iface_for_identity, conn_id_for_identity): (Option<Arc<dyn ISocketConnection>>, Option<usize>) =
                self.core.core_state.read().endpoints.get(&target_endpoint_uri).map_or(
                    (None, None),
                    |ep_info| (Some(ep_info.connection_iface.clone()), Some(ep_info.handle_id)),
                );

            let (conn_iface, conn_id) = match (conn_iface_for_identity, conn_id_for_identity) {
                (Some(iface), Some(id)) => (iface, id),
                _ => {
                    self.router_map_for_send.remove_peer_by_identity(&destination_id).await;
                    return if router_mandatory_opt {
                        Err(ZmqError::HostUnreachable("Peer connection for identity disappeared".into()))
                    } else {
                        Ok(())
                    };
                }
            };

            let permit = self
                .pipe_send_coordinator
                .acquire_send_permit(conn_id, timeout_opt)
                .await?;
            msg.set_flags(msg.flags() | MsgFlags::MORE);

            match conn_iface.send_message(msg).await {
                Ok(()) => {
                    let mut delimiter_frame = Msg::new();
                    delimiter_frame.set_flags(MsgFlags::MORE);
                    match conn_iface.send_message(delimiter_frame).await {
                        Ok(()) => {
                            let mut set_target_guard = self.current_send_target.lock().await;
                            *set_target_guard = Some(ActiveFragmentedSend {
                                target_endpoint_uri,
                                _permit: permit,
                            });
                            Ok(())
                        }
                        Err(e) => {
                            if router_mandatory_opt {
                                Err(if matches!(e, ZmqError::ConnectionClosed) {
                                    ZmqError::HostUnreachable("Peer disconnected during delimiter send".into())
                                } else {
                                    e
                                })
                            } else {
                                Ok(())
                            }
                        }
                    }
                }
                Err(e) => {
                    if router_mandatory_opt {
                        Err(if matches!(e, ZmqError::ConnectionClosed) {
                            ZmqError::HostUnreachable("Peer disconnected during identity send".into())
                        } else {
                            e
                        })
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }

    async fn recv(&self) -> Result<Msg, ZmqError> {
        if !self.core.is_running().await {
            return Err(ZmqError::InvalidState("Socket is closing".into()));
        }
        let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
        self.incoming_orchestrator.recv_message(rcvtimeo_opt).await
    }

    async fn send_multipart(&self, mut frames: Vec<Msg>) -> Result<(), ZmqError> {
        if !self.core.is_running().await {
            return Err(ZmqError::InvalidState("Socket is closing".into()));
        }
        if frames.len() < 2 {
            return Err(ZmqError::InvalidMessage(
                "ROUTER send_multipart requires at least identity and one payload frame".into(),
            ));
        }

        let mut destination_identity_frame = frames.remove(0);
        let destination_id_blob = Blob::from_bytes(destination_identity_frame.data_bytes().unwrap_or_default());
        if destination_id_blob.is_empty() {
            return Err(ZmqError::InvalidMessage(
                "First frame (destination identity) cannot be empty.".into(),
            ));
        }

        let core_opts_arc = self.core.core_state.read().options.clone();
        let timeout_opt: Option<Duration> = core_opts_arc.sndtimeo;
        let router_mandatory_opt: bool = core_opts_arc.router_mandatory;

        let target_endpoint_uri = match self
            .router_map_for_send
            .get_uri_for_identity(&destination_id_blob)
            .await
        {
            Some(uri) => uri,
            None => {
                return if router_mandatory_opt {
                    Err(ZmqError::HostUnreachable(format!(
                        "Peer {:?} not found (ROUTER_MANDATORY)",
                        destination_id_blob
                    )))
                } else {
                    Ok(())
                };
            }
        };

        let conn_iface_and_id_opt: Option<(Arc<dyn ISocketConnection>, usize)> = self
            .core
            .core_state
            .read()
            .endpoints
            .get(&target_endpoint_uri)
            .map(|ep_info| (ep_info.connection_iface.clone(), ep_info.handle_id));

        let (conn_iface, conn_id) = match conn_iface_and_id_opt {
            Some((iface, id)) => (iface, id),
            None => {
                self.router_map_for_send.remove_peer_by_identity(&destination_id_blob).await;
                return if router_mandatory_opt {
                    Err(ZmqError::HostUnreachable("Peer connection for identity disappeared".into()))
                } else {
                    Ok(())
                };
            }
        };

        {
            let guard = self.current_send_target.lock().await;
            if let Some(active_info) = &*guard {
                if active_info.target_endpoint_uri == target_endpoint_uri {
                    tracing::debug!("ROUTER send_multipart for {} waiting for active fragmented send to same target to complete (permit system will handle).", target_endpoint_uri);
                }
            }
        }

        let _permit = self
            .pipe_send_coordinator
            .acquire_send_permit(conn_id, timeout_opt)
            .await
            .map_err(|e| {
                if router_mandatory_opt {
                    e
                } else {
                    // This case implies a logic error if non-mandatory sends still get here after permit error
                    tracing::error!("ROUTER_MANDATORY=false but permit acquisition failed: {:?}", e);
                    ZmqError::Internal("Send silently dropped due to permit error, but this is unexpected for non-mandatory.".into())
                }
            })?;


        destination_identity_frame.set_flags(MsgFlags::MORE);
        if let Err(e) = conn_iface.send_message(destination_identity_frame).await {
            return if router_mandatory_opt { Err(e) } else { Ok(()) };
        }

        let mut delimiter_frame = Msg::new();
        delimiter_frame.set_flags(MsgFlags::MORE);
        if let Err(e) = conn_iface.send_message(delimiter_frame).await {
            return if router_mandatory_opt { Err(e) } else { Ok(()) };
        }

        let num_payload_frames = frames.len();
        if num_payload_frames == 0 {
            // Send a single empty frame if the original payload was empty after identity
            let last_empty_frame = Msg::new(); // Already has MORE=false by default
            if let Err(e) = conn_iface.send_message(last_empty_frame).await {
                return if router_mandatory_opt { Err(e) } else { Ok(()) };
            }
        } else {
            for (i, mut frame) in frames.into_iter().enumerate() {
                if i < num_payload_frames - 1 {
                    frame.set_flags(frame.flags() | MsgFlags::MORE);
                } else {
                    frame.set_flags(frame.flags() & !MsgFlags::MORE);
                }
                if let Err(e) = conn_iface.send_message(frame).await {
                    return if router_mandatory_opt { Err(e) } else { Ok(()) };
                }
            }
        }
        Ok(())
    }

    async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError> {
        if !self.core.is_running().await {
            return Err(ZmqError::InvalidState("Socket is closing".into()));
        }
        let rcvtimeo_opt: Option<Duration> = { self.core_state_read().options.rcvtimeo };
        self.incoming_orchestrator.recv_logical_message(rcvtimeo_opt).await
    }

    async fn set_pattern_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError> {
        if option == ROUTER_MANDATORY {
            update_core_option(&self.core, |options| {
                options.router_mandatory = parse_bool_option(value)?;
                Ok(())
            })?;
            Ok(())
        } else {
            Err(ZmqError::UnsupportedOption(option))
        }
    }
    async fn get_pattern_option(&self, option: i32) -> Result<Vec<u8>, ZmqError> {
        if option == ROUTER_MANDATORY {
            let val = self.core_state_read().options.router_mandatory;
            Ok((val as i32).to_ne_bytes().to_vec())
        } else {
            Err(ZmqError::UnsupportedOption(option))
        }
    }

    async fn process_command(&self, _command: Command) -> Result<bool, ZmqError> {
        Ok(false)
    }

    async fn handle_pipe_event(&self, pipe_read_id: usize, event: Command) -> Result<(), ZmqError> {
        match event {
            Command::PipeMessageReceived { mut msg, .. } => {
                msg.metadata_mut().insert_typed(SourcePipeReadId(pipe_read_id)).await;
                if let Some(raw_zmtp_message_vec) = self
                    .incoming_orchestrator
                    .accumulate_pipe_frame(pipe_read_id, msg)?
                {
                    // Retrieve identity using DashMap
                    let identity_blob = self
                        .pipe_to_identity_shared_map
                        .get(&pipe_read_id) // DashMap::get returns a Ref<K, V>
                        .map(|entry| entry.value().clone()) // entry.value() is &Blob, so clone it
                        .unwrap_or_else(|| {
                            tracing::warn!(
                                "Router transform: Identity for pipe {} not found in shared map, using placeholder.",
                                pipe_read_id
                            );
                            Self::pipe_id_to_placeholder_identity(pipe_read_id)
                        });

                    let app_logical_message = Self::process_raw_message_for_router(
                        pipe_read_id,
                        raw_zmtp_message_vec,
                        identity_blob, // Pass the resolved Blob
                    )?;

                    self.incoming_orchestrator
                        .queue_application_message_frames(pipe_read_id, app_logical_message)
                        .await?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    // The timed version of handle_pipe_event is omitted for brevity but would follow the same logic changes.

    async fn pipe_attached(&self, pipe_read_id: usize, _pipe_write_id: usize, peer_identity_opt: Option<&[u8]>) {
        let (endpoint_uri_opt, connection_id_opt) = {
            let core_s_read = self.core_state_read();
            let uri_opt = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
            let conn_id_opt = uri_opt
                .as_ref()
                .and_then(|uri| core_s_read.endpoints.get(uri).map(|ep_info| ep_info.handle_id));
            (uri_opt, conn_id_opt)
        };

        if let (Some(endpoint_uri), Some(connection_id)) = (endpoint_uri_opt, connection_id_opt) {
            let identity_to_use = match peer_identity_opt {
                Some(id_bytes) if !id_bytes.is_empty() => Blob::from_bytes(Bytes::copy_from_slice(id_bytes)),
                _ => Self::pipe_id_to_placeholder_identity(pipe_read_id),
            };

            tracing::debug!(handle = self.core.handle, pipe_read_id, %endpoint_uri, ?identity_to_use, "ROUTER attaching connection");
            self.router_map_for_send
                .add_peer(identity_to_use.clone(), pipe_read_id, endpoint_uri)
                .await;
            // Use DashMap API
            self.pipe_to_identity_shared_map.insert(pipe_read_id, identity_to_use);
            self.pipe_send_coordinator.add_pipe(connection_id).await;
        } else {
            tracing::warn!(
                handle = self.core.handle,
                pipe_read_id,
                "ROUTER pipe_attached: Endpoint URI or Connection ID not found"
            );
        }
    }

    async fn update_peer_identity(&self, pipe_read_id: usize, new_identity_opt: Option<Blob>) {
        let endpoint_uri_opt = self
            .core_state_read()
            .pipe_read_id_to_endpoint_uri
            .get(&pipe_read_id)
            .cloned();

        if let Some(endpoint_uri) = endpoint_uri_opt {
            let new_identity = match new_identity_opt {
                Some(id) if !id.is_empty() => id,
                _ => Self::pipe_id_to_placeholder_identity(pipe_read_id),
            };
            tracing::debug!(handle = self.core.handle, pipe_read_id, new_identity = ?new_identity, "ROUTER updating peer identity");

            self.router_map_for_send
                .update_peer_identity(pipe_read_id, new_identity.clone(), &endpoint_uri)
                .await;
            // Use DashMap API
            self.pipe_to_identity_shared_map.insert(pipe_read_id, new_identity);
        } else {
            tracing::warn!(
                handle = self.core.handle,
                pipe_read_id,
                "ROUTER update_peer_identity: Endpoint URI not found"
            );
        }
    }

    async fn pipe_detached(&self, pipe_read_id: usize) {
        tracing::debug!(handle = self.core.handle, pipe_read_id, "ROUTER detaching pipe");

        let (endpoint_uri_opt, connection_id_opt) = {
            let core_s_read = self.core_state_read();
            let uri_opt = core_s_read.pipe_read_id_to_endpoint_uri.get(&pipe_read_id).cloned();
            let conn_id_opt = uri_opt
                .as_ref()
                .and_then(|uri| core_s_read.endpoints.get(uri).map(|ep_info| ep_info.handle_id));
            (uri_opt, conn_id_opt)
        };

        self.router_map_for_send.remove_peer_by_read_pipe(pipe_read_id).await;
        // Use DashMap API
        self.pipe_to_identity_shared_map.remove(&pipe_read_id);

        if let Some(conn_id) = connection_id_opt {
            if let Some(semaphore) = self.pipe_send_coordinator.remove_pipe(conn_id).await {
                semaphore.close();
            }

            let mut active_frag_guard = self.current_send_target.lock().await;
            if let Some(active_info) = &*active_frag_guard {
                if endpoint_uri_opt.as_deref() == Some(&active_info.target_endpoint_uri) {
                    *active_frag_guard = None;
                }
            }
        }
        self.incoming_orchestrator.clear_pipe_state(pipe_read_id).await;
    }
}