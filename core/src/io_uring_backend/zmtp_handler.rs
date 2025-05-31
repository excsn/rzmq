#![cfg(feature = "io-uring")]

use crate::io_uring_backend::connection_handler::{
    UringConnectionHandler, UringWorkerInterface, HandlerIoOps, HandlerSqeBlueprint, UserData,
    WorkerIoConfig, ProtocolHandlerFactory,
};
use crate::io_uring_backend::ops::ProtocolConfig;

use crate::protocol::zmtp::{
    greeting::{ZmtpGreeting, GREETING_LENGTH, MECHANISM_LENGTH},
    command::{ZmtpCommand, ZmtpReady},
    manual_parser::ZmtpManualParser,
};
use crate::security::{
    self, negotiate_security_mechanism, IDataCipher, Mechanism, MechanismStatus, NullMechanism,
    PlainMechanism, NoiseXxMechanism, // Assuming these are available if features enabled
};
use crate::socket::options::ZmtpEngineConfig;
use crate::message::{Msg, MsgFlags, Metadata};
use crate::{Blob, ZmqError};

use bytes::{BytesMut, Bytes, BufMut};
use tokio_util::codec::Encoder;
use std::sync::Arc;
use std::os::unix::io::RawFd;
use std::time::{Instant, Duration};
use std::collections::{VecDeque, HashMap};
use std::any::Any;
use tracing::{debug, error, info, trace, warn};


#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ZmtpHandlerPhase {
    Initial,
    ClientSendGreeting, ClientWaitServerGreeting,
    ServerWaitClientGreeting, ServerSendGreeting,
    SecurityExchange,
    ReadyClientSend, ReadyClientWaitServer,
    ReadyServerWaitClient, ReadyServerSend,
    DataPhase,
    Error, Closed,
}

pub struct ZmtpUringHandler {
    fd: RawFd,
    zmtp_config: Arc<ZmtpEngineConfig>,
    is_server: bool,
    phase: ZmtpHandlerPhase,

    greeting_buffer: BytesMut, 
    network_read_accumulator: BytesMut, 
    
    plaintext_zmtp_frame_accumulator: BytesMut,

    security_mechanism: Option<Box<dyn Mechanism>>,
    data_cipher: Option<Box<dyn IDataCipher>>,
    zmtp_parser: ZmtpManualParser, 

    last_activity_time: Instant,
    last_ping_sent_time: Option<Instant>,
    waiting_for_pong: bool,
    heartbeat_ivl: Option<Duration>,
    heartbeat_timeout_duration: Duration,

    outgoing_app_messages: VecDeque<Msg>, 
    
    handshake_timeout: Duration,
    handshake_timeout_deadline: Instant,
    read_is_pending: bool, 

    peer_identity_from_security: Option<Blob>, 
    peer_identity_from_ready: Option<Blob>,    
    final_peer_identity: Option<Blob>,         
    
    last_sent_was_ping: bool, 
}

impl ZmtpUringHandler {
    pub fn new(
        fd: RawFd,
        zmtp_config_arg: Arc<ZmtpEngineConfig>,
        is_server: bool,
    ) -> Self {
        let handshake_timeout_duration = zmtp_config_arg.handshake_timeout.unwrap_or(Duration::from_secs(30));
        let heartbeat_timeout_val = zmtp_config_arg.heartbeat_timeout.unwrap_or_else(||
            zmtp_config_arg.heartbeat_ivl.map_or(Duration::from_secs(30), |ivl| ivl.saturating_mul(2))
        );
        let heartbeat_ivl_val = zmtp_config_arg.heartbeat_ivl;

        Self {
            fd,
            zmtp_config: zmtp_config_arg,
            is_server,
            phase: ZmtpHandlerPhase::Initial,
            greeting_buffer: BytesMut::with_capacity(GREETING_LENGTH),
            network_read_accumulator: BytesMut::with_capacity(8192 * 2),
            plaintext_zmtp_frame_accumulator: BytesMut::with_capacity(8192 * 2),
            security_mechanism: None,
            data_cipher: None,
            zmtp_parser: ZmtpManualParser::new(),
            last_activity_time: Instant::now(),
            last_ping_sent_time: None,
            waiting_for_pong: false,
            heartbeat_ivl: heartbeat_ivl_val,
            heartbeat_timeout_duration: heartbeat_timeout_val,
            outgoing_app_messages: VecDeque::new(),
            handshake_timeout: handshake_timeout_duration,
            handshake_timeout_deadline: Instant::now() + handshake_timeout_duration,
            read_is_pending: false,
            peer_identity_from_security: None,
            peer_identity_from_ready: None,
            final_peer_identity: None,
            last_sent_was_ping: false,
        }
    }

    fn zmtp_encode_msg_to_bytes(msg: &Msg) -> Result<Bytes, ZmqError> {
        let mut temp_codec = crate::protocol::zmtp::ZmtpCodec::new();
        let mut dst_buffer = BytesMut::new();
        temp_codec.encode(msg.clone(), &mut dst_buffer)?;
        Ok(dst_buffer.freeze())
    }
    
    fn apply_encryption_if_needed(
        data_cipher_opt: Option<&mut Box<dyn IDataCipher>>,
        zmtp_frame_bytes: Bytes 
    ) -> Result<Bytes, ZmqError> {
        if let Some(cipher) = data_cipher_opt {
            cipher.encrypt_zmtp_frame(zmtp_frame_bytes)
        } else {
            Ok(zmtp_frame_bytes) 
        }
    }

    fn ensure_read_is_pending(&mut self, ops: &mut HandlerIoOps, interface: &UringWorkerInterface<'_>) {
        if !self.read_is_pending && !matches!(self.phase, ZmtpHandlerPhase::Closed | ZmtpHandlerPhase::Error) {
            if interface.default_buffer_group_id().is_some() {
                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestRingRead);
                self.read_is_pending = true;
                trace!(fd = self.fd, "ZmtpUringHandler: Requested RingRead.");
            } else {
                error!(fd = self.fd, "ZmtpUringHandler: Critical - Cannot request read, no default_bgid configured for worker. Handler requires ring reads.");
                let err = ZmqError::Internal("Ring read required by handler but not configured in worker (no default_bgid).".into());
                let mut temp_ops = std::mem::take(ops);
                self.transition_to_error(&mut temp_ops, err, interface); 
                *ops = temp_ops;
            }
        }
    }
    
    fn transition_to_error(&mut self, ops: &mut HandlerIoOps, error: ZmqError, _interface: &UringWorkerInterface<'_>) {
        if self.phase == ZmtpHandlerPhase::Error || self.phase == ZmtpHandlerPhase::Closed {
            return;
        }
        let previous_phase = self.phase;
        error!(fd = self.fd, error_msg = %error, ?previous_phase, "ZmtpUringHandler: Transitioning to error state.");
        self.phase = ZmtpHandlerPhase::Error;
        
        ops.initiate_close_due_to_error = true;
        if !ops.sqe_blueprints.iter().any(|bp| matches!(bp, HandlerSqeBlueprint::RequestClose)) {
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
        }

        if !matches!(previous_phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
             warn!(fd=self.fd, "Signaling handshake failure upstream due to error: {}", error);
             let _ = _interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(error)));
        }
    }

    fn build_ready_properties(&self) -> HashMap<String, Vec<u8>> {
        let mut props = HashMap::new();
        props.insert("Socket-Type".to_string(), self.zmtp_config.socket_type_name.as_bytes().to_vec());
        if let Some(id_blob) = &self.zmtp_config.routing_id { 
            if !id_blob.is_empty() && id_blob.len() <= 255 {
                 props.insert("Identity".to_string(), id_blob.to_vec());
            } else if id_blob.is_empty() {
                trace!(fd=self.fd, "Local routing_id is empty, not sending in READY.");
            } else {
                warn!(fd=self.fd, id_len=id_blob.len(), "Local routing_id too long (max 255), not sending in READY.");
            }
        }
        props
    }

    fn signal_upstream_handshake_complete(&mut self, interface: &UringWorkerInterface<'_>) -> Result<(), ZmqError> {
        self.final_peer_identity = self.peer_identity_from_ready.clone()
            .or_else(|| self.peer_identity_from_security.clone());

        info!(fd=self.fd, final_peer_id=?self.final_peer_identity, "ZmtpUringHandler: Signaling ZMTP handshake completion upstream.");
        
        let signal_content = format!(
            "ZMTP_HANDSHAKE_COMPLETE_SIGNAL_FD_{}_PEER_ID_{:?}", 
            self.fd,
            self.final_peer_identity.as_ref().map(|id_blob| String::from_utf8_lossy(id_blob.as_ref()))
        );

        interface.worker_io_config.parsed_msg_tx_zmtp.try_send(
            (self.fd, Err(ZmqError::Internal(signal_content))) 
        ).map_err(|e| {
            error!(fd=self.fd, "Failed to send ZMTP_HANDSHAKE_COMPLETE signal upstream: {:?}", e);
            ZmqError::Internal("Failed to signal handshake completion".into())
        }).map(|_| ())
    }

    fn process_buffered_reads(&mut self, interface: &UringWorkerInterface<'_>, ops: &mut HandlerIoOps) -> Result<bool, ZmqError> {
        let mut made_progress_this_call = false;

        loop {
            let initial_greeting_len = self.greeting_buffer.len();
            let initial_network_acc_len = self.network_read_accumulator.len(); 
            let initial_plaintext_acc_len = self.plaintext_zmtp_frame_accumulator.len();

            if Instant::now() > self.handshake_timeout_deadline &&
               !matches!(self.phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
                warn!(fd=self.fd, current_phase=?self.phase, "Overall handshake timeout occurred in process_buffered_reads.");
                let err = ZmqError::Timeout;
                let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops;
                return Err(err); 
            }
            
            match self.phase {
                ZmtpHandlerPhase::Initial => {
                    error!(fd=self.fd, "ZmtpHandler in Initial phase during process_buffered_reads. This is a bug.");
                    return Err(ZmqError::InvalidState("ZmtpHandler in Initial phase during data processing".into()));
                }
                ZmtpHandlerPhase::ClientSendGreeting | ZmtpHandlerPhase::ServerSendGreeting |
                ZmtpHandlerPhase::ReadyClientSend | ZmtpHandlerPhase::ReadyServerSend => {
                    return Ok(made_progress_this_call); 
                }

                ZmtpHandlerPhase::ServerWaitClientGreeting | ZmtpHandlerPhase::ClientWaitServerGreeting => {
                    let needed_for_greeting = GREETING_LENGTH.saturating_sub(self.greeting_buffer.len());
                    if needed_for_greeting > 0 {
                        let source_buf = &mut self.network_read_accumulator; 
                        let can_take = std::cmp::min(needed_for_greeting, source_buf.len());
                        if can_take > 0 {
                            self.greeting_buffer.put(source_buf.split_to(can_take));
                            made_progress_this_call = true;
                        }
                        if self.greeting_buffer.len() < GREETING_LENGTH { return Ok(made_progress_this_call); }
                    }

                    match ZmtpGreeting::decode(&mut self.greeting_buffer) { 
                        Ok(Some(peer_greeting)) => {
                            made_progress_this_call = true;
                            debug!(fd = self.fd, role = if self.is_server {"S"} else {"C"}, ?peer_greeting, "Received and decoded peer greeting");
                            if self.is_server == peer_greeting.as_server { 
                                let err = ZmqError::SecurityError("Role mismatch in greeting".into());
                                let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                            }
                            self.security_mechanism = Some(negotiate_security_mechanism(self.is_server, &self.zmtp_config, &peer_greeting, self.fd as usize)?);
                            info!(fd=self.fd, mechanism=?self.security_mechanism.as_ref().unwrap().name(), "Negotiated security mechanism");

                            if self.is_server {
                                let mut greeting_to_send_buf = BytesMut::with_capacity(GREETING_LENGTH);
                                ZmtpGreeting::encode(self.zmtp_config.security_mechanism_bytes_to_propose(self.is_server), true, &mut greeting_to_send_buf);
                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: greeting_to_send_buf.freeze() });
                                self.phase = ZmtpHandlerPhase::ServerSendGreeting; 
                            } else { 
                                self.phase = ZmtpHandlerPhase::SecurityExchange;
                                if let Some(token_vec) = self.security_mechanism.as_mut().unwrap().produce_token()? {
                                    let token_msg = Msg::from_vec(token_vec).with_flags(MsgFlags::COMMAND);
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&token_msg)? });
                                }
                            }
                        }
                        Ok(None) => { return Ok(made_progress_this_call); }
                        Err(e) => { let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, e.clone(), interface); *ops = temp_ops; return Err(e); }
                    }
                }

                ZmtpHandlerPhase::SecurityExchange => {
                    let sec_mech = self.security_mechanism.as_mut().ok_or_else(|| ZmqError::InvalidState("Security mechanism None in SecurityExchange phase".into()))?;
                    let mut action_this_pass_for_sec = false; 

                    // <<< MODIFIED START [Removed is_my_turn(), rely on produce_token()] >>>
                    // Try to produce and send our token if the mechanism has one.
                    if let Some(token_to_send_vec) = sec_mech.produce_token()? {
                        let token_msg = Msg::from_vec(token_to_send_vec).with_flags(MsgFlags::COMMAND);
                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&token_msg)? });
                        made_progress_this_call = true; action_this_pass_for_sec = true;
                    }
                    // <<< MODIFIED END >>>

                    if !self.network_read_accumulator.is_empty() {
                         match self.zmtp_parser.decode_from_buffer(&mut self.network_read_accumulator) { 
                            Ok(Some(token_msg_from_peer)) => {
                                made_progress_this_call = true; action_this_pass_for_sec = true;
                                if !token_msg_from_peer.is_command() {
                                    let err = ZmqError::SecurityError("Expected ZMTP COMMAND for security token".into());
                                    let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                                }
                                sec_mech.process_token(token_msg_from_peer.data().unwrap_or_default())?;
                                // <<< MODIFIED START [Removed is_my_turn(), rely on produce_token()] >>>
                                // After processing, it might be our turn again. produce_token() will handle this.
                                if let Some(response_token_vec) = sec_mech.produce_token()? {
                                    let response_token_msg = Msg::from_vec(response_token_vec).with_flags(MsgFlags::COMMAND);
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&response_token_msg)? });
                                }
                                // <<< MODIFIED END >>>
                            }
                            Ok(None) => { }
                            Err(e) => { let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, e.clone(), interface); *ops = temp_ops; return Err(e); }
                        }
                    }
                    
                    if sec_mech.is_complete() {
                        info!(fd = self.fd, "Security handshake complete. Mechanism: {}", sec_mech.name());
                        self.peer_identity_from_security = sec_mech.peer_identity().map(Blob::from); 
                        let taken_mechanism = self.security_mechanism.take().unwrap(); 
                        let (cipher, _sec_peer_id_already_got) = taken_mechanism.into_data_cipher_parts()?;
                        self.data_cipher = Some(cipher); 
                        
                        self.phase = if self.is_server { ZmtpHandlerPhase::ReadyServerWaitClient } else { ZmtpHandlerPhase::ReadyClientSend };
                        if !self.is_server { 
                            let client_ready_msg = ZmtpReady::create_msg(self.build_ready_properties());
                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{ data: Self::zmtp_encode_msg_to_bytes(&client_ready_msg)? });
                        }
                        made_progress_this_call = true; 
                    } else if sec_mech.is_error() {
                        let err_reason = sec_mech.error_reason().unwrap_or("Unknown security error").to_string();
                        let err = ZmqError::SecurityError(err_reason);
                        let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                    } else if !action_this_pass_for_sec && self.network_read_accumulator.is_empty() {
                        return Ok(made_progress_this_call);
                    }
                }

                ZmtpHandlerPhase::ReadyServerWaitClient | ZmtpHandlerPhase::ReadyClientWaitServer => {
                    if self.network_read_accumulator.is_empty() { return Ok(made_progress_this_call); }
                    match self.zmtp_parser.decode_from_buffer(&mut self.network_read_accumulator) { 
                        Ok(Some(ready_msg_from_peer)) => {
                            made_progress_this_call = true;
                            match ZmtpCommand::parse(&ready_msg_from_peer) { 
                                Some(ZmtpCommand::Ready(ready_data)) => {
                                    debug!(fd=self.fd, "Received READY from peer. Properties: {:?}", ready_data.properties);
                                    if let Some(id_bytes_vec) = ready_data.properties.get("Identity") {
                                        if !id_bytes_vec.is_empty() && id_bytes_vec.len() <= 255 {
                                            self.peer_identity_from_ready = Some(Blob::from(id_bytes_vec.clone()));
                                        } else {
                                            warn!(fd=self.fd, id_len=id_bytes_vec.len(), "Peer sent invalid Identity in READY (empty or too long).");
                                        }
                                    }
                                    
                                    if self.is_server { 
                                        let server_ready_msg = ZmtpReady::create_msg(self.build_ready_properties());
                                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&server_ready_msg)? });
                                        self.phase = ZmtpHandlerPhase::ReadyServerSend; 
                                    } else { 
                                        self.phase = ZmtpHandlerPhase::DataPhase;
                                        info!(fd=self.fd, "ZmtpUringHandler: Client handshake fully complete. Transitioning to DataPhase.");
                                        self.signal_upstream_handshake_complete(interface)?; 
                                    }
                                }
                                _ => { 
                                    let err = ZmqError::ProtocolViolation("Expected READY command, got other/unparseable".into());
                                    let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                                }
                            }
                        }
                        Ok(None) => return Ok(made_progress_this_call), 
                        Err(e) => { let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, e.clone(), interface); *ops = temp_ops; return Err(e); }
                    }
                }

                ZmtpHandlerPhase::DataPhase => {
                    let mut source_for_zmtp_parser: &mut BytesMut;
                    
                    if self.data_cipher.is_some() {
                        while !self.network_read_accumulator.is_empty() {
                             match self.data_cipher.as_mut().unwrap().decrypt_wire_data_to_zmtp_frame(&mut self.network_read_accumulator) {
                                Ok(Some(plaintext_zmtp_frame_bytes)) => {
                                    made_progress_this_call = true;
                                    self.plaintext_zmtp_frame_accumulator.put(plaintext_zmtp_frame_bytes);
                                }
                                Ok(None) => break, 
                                Err(e) => { let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, e.clone(), interface); *ops = temp_ops; return Err(e); }
                            }
                        }
                        source_for_zmtp_parser = &mut self.plaintext_zmtp_frame_accumulator;
                    } else {
                        source_for_zmtp_parser = &mut self.network_read_accumulator;
                    };
                    
                    if source_for_zmtp_parser.is_empty() { return Ok(made_progress_this_call); } 

                    while !source_for_zmtp_parser.is_empty() {
                        match self.zmtp_parser.decode_from_buffer(source_for_zmtp_parser) {
                            Ok(Some(msg)) => {
                                made_progress_this_call = true;
                                self.last_activity_time = Instant::now(); 
                                if msg.is_command() {
                                    match ZmtpCommand::parse(&msg) {
                                        Some(ZmtpCommand::Ping(ping_context_payload)) => {
                                            let pong_reply_msg = ZmtpCommand::create_pong(&ping_context_payload);
                                            let pong_plaintext_bytes = Self::zmtp_encode_msg_to_bytes(&pong_reply_msg)?;
                                            let pong_wire_bytes = Self::apply_encryption_if_needed(self.data_cipher.as_mut(), pong_plaintext_bytes)?;
                                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: pong_wire_bytes });
                                            debug!(fd=self.fd, "Prepared PONG in response to PING");
                                        }
                                        Some(ZmtpCommand::Pong(_pong_context_payload)) => {
                                            self.waiting_for_pong = false; self.last_ping_sent_time = None;
                                            debug!(fd=self.fd, "Received PONG");
                                        }
                                        Some(ZmtpCommand::Error) => { 
                                            warn!(fd = self.fd, "Peer sent ZMTP ERROR command. Transitioning to error state.");
                                            let err = ZmqError::ProtocolViolation("Peer sent ZMTP ERROR command".into());
                                            let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                                        }
                                        _ => { warn!(fd = self.fd, "Received unhandled ZMTP command in DataPhase: {:?}", msg.data()); }
                                    }
                                } else { 
                                    if let Err(send_err) = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Ok(msg))) {
                                        error!(fd = self.fd, "Failed to send ZMTP data msg upstream: {:?}", send_err);
                                        let err = ZmqError::Internal("Upstream channel error for ZMTP data".into());
                                        let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); *ops = temp_ops; return Err(err);
                                    }
                                }
                            }
                            Ok(None) => break, 
                            Err(e) => { let mut temp_ops = std::mem::take(ops); self.transition_to_error(&mut temp_ops, e.clone(), interface); *ops = temp_ops; return Err(e); }
                        }
                    }
                }
                ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed => return Ok(made_progress_this_call),
            } 

            let no_greeting_change = self.greeting_buffer.len() == initial_greeting_len;
            let no_network_acc_change = self.network_read_accumulator.len() == initial_network_acc_len;
            let no_plaintext_acc_change = self.plaintext_zmtp_frame_accumulator.len() == initial_plaintext_acc_len;

            if no_greeting_change && no_network_acc_change && no_plaintext_acc_change && !made_progress_this_call {
                break; 
            }
            made_progress_this_call = false; 
        } 
        Ok(true) 
    }
}


impl UringConnectionHandler for ZmtpUringHandler {
    fn fd(&self) -> RawFd { self.fd }

    fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        info!(fd = self.fd, role = if self.is_server {"S"} else {"C"}, "ZmtpUringHandler: connection_ready.");
        self.last_activity_time = Instant::now();
        self.handshake_timeout_deadline = Instant::now() + self.handshake_timeout; 
        let mut ops = HandlerIoOps::new();

        if self.is_server {
            self.phase = ZmtpHandlerPhase::ServerWaitClientGreeting;
        } else { 
            let mut greeting_to_send_buf = BytesMut::with_capacity(GREETING_LENGTH);
            let proposed_mechanism_bytes = self.zmtp_config.security_mechanism_bytes_to_propose(self.is_server);
            ZmtpGreeting::encode(proposed_mechanism_bytes, false, &mut greeting_to_send_buf);
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: greeting_to_send_buf.freeze() });
            self.phase = ZmtpHandlerPhase::ClientSendGreeting;
        }
        self.ensure_read_is_pending(&mut ops, interface);
        ops
    }

    fn process_ring_read_data(&mut self, buffer_slice: &[u8], _buffer_id: u16, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        trace!(fd = self.fd, len = buffer_slice.len(), phase = ?self.phase, "ZmtpUringHandler: process_ring_read_data");
        self.last_activity_time = Instant::now();
        self.read_is_pending = false; 
        let mut ops = HandlerIoOps::new();

        if buffer_slice.is_empty() && !matches!(self.phase, ZmtpHandlerPhase::Closed | ZmtpHandlerPhase::Error) {
            let original_phase_eof = self.phase;
            info!(fd = self.fd, ?original_phase_eof, "Peer closed connection (EOF received on read).");
            let eof_err = ZmqError::ConnectionClosed;
            let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, eof_err, interface); ops = temp_ops;
            return ops; 
        }
        
        if !buffer_slice.is_empty() {
            self.network_read_accumulator.put_slice(buffer_slice);
        }
        
        if let Err(e) = self.process_buffered_reads(interface, &mut ops) {
            if self.phase != ZmtpHandlerPhase::Error && self.phase != ZmtpHandlerPhase::Closed {
                 error!(fd = self.fd, error = %e, "process_buffered_reads returned error but phase not Error/Closed. Forcing error state.");
                 let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, e, interface); ops = temp_ops;
            }
        }
        self.ensure_read_is_pending(&mut ops, interface);
        ops
    }

    fn handle_internal_sqe_completion(&mut self, _sqe_user_data: UserData, cqe_result: i32, _cqe_flags: u32, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        trace!(fd = self.fd, cqe_res = cqe_result, phase = ?self.phase, "ZmtpUringHandler: handle_internal_sqe_completion (likely Send ACK)");
        self.last_activity_time = Instant::now(); 
        let mut ops = HandlerIoOps::new();

        if cqe_result < 0 { 
            let io_err = std::io::Error::from_raw_os_error(-cqe_result);
            let zmq_err = ZmqError::from(io_err);
            error!(fd = self.fd, error = %zmq_err, "Kernel error on send operation.");
            let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, zmq_err, interface); ops = temp_ops;
            return ops;
        }

        let previous_phase = self.phase;
        match self.phase {
            ZmtpHandlerPhase::ClientSendGreeting => { self.phase = ZmtpHandlerPhase::ClientWaitServerGreeting; }
            ZmtpHandlerPhase::ServerSendGreeting => {
                self.phase = ZmtpHandlerPhase::SecurityExchange;
                if let Some(sec_mech) = self.security_mechanism.as_mut() {
                    // <<< MODIFIED START [Removed is_my_turn(), rely on produce_token()] >>>
                    if let Ok(Some(token_vec)) = sec_mech.produce_token() { // produce_token() decides if it's turn.
                         let token_msg = Msg::from_vec(token_vec).with_flags(MsgFlags::COMMAND);
                         if let Ok(bytes) = Self::zmtp_encode_msg_to_bytes(&token_msg) {
                             ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{ data: bytes });
                         } else {
                             let err = ZmqError::Internal("Failed to encode initial server security token".into());
                             let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, err, interface); ops = temp_ops; return ops;
                         }
                    }
                    // <<< MODIFIED END >>>
                }
            }
            ZmtpHandlerPhase::SecurityExchange => { }
            ZmtpHandlerPhase::ReadyClientSend => { self.phase = ZmtpHandlerPhase::ReadyClientWaitServer; }
            ZmtpHandlerPhase::ReadyServerSend => {
                self.phase = ZmtpHandlerPhase::DataPhase;
                info!(fd=self.fd, "ZmtpUringHandler: Server handshake fully complete. Transitioning to DataPhase.");
                if let Err(e) = self.signal_upstream_handshake_complete(interface) {
                    let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, e, interface); ops = temp_ops; return ops;
                }
            }
            ZmtpHandlerPhase::DataPhase => {
                if self.last_sent_was_ping { 
                    self.waiting_for_pong = true;
                    self.last_ping_sent_time = Some(self.last_activity_time); 
                    debug!(fd=self.fd, "PING send acknowledged by kernel. Now waiting for PONG reply.");
                    self.last_sent_was_ping = false; 
                }
                if let Some(next_msg) = self.outgoing_app_messages.pop_front() {
                    match Self::zmtp_encode_msg_to_bytes(&next_msg) {
                        Ok(zmtp_frame_bytes) => { 
                            match Self::apply_encryption_if_needed(self.data_cipher.as_mut(), zmtp_frame_bytes) {
                                Ok(final_bytes_to_send) => {
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{data: final_bytes_to_send });
                                }
                                Err(enc_err) => { 
                                    error!(fd=self.fd, "Encryption failed for queued app message: {}", enc_err);
                                    self.outgoing_app_messages.push_front(next_msg); 
                                    let mut temp_ops = std::mem::take(&mut ops); 
                                    self.transition_to_error(&mut temp_ops, enc_err, interface); 
                                    ops = temp_ops; return ops; 
                                }
                            }
                        }
                        Err(encode_err) => {
                             error!(fd=self.fd, "Failed to ZMTP encode next outgoing msg from queue: {}", encode_err);
                             self.outgoing_app_messages.push_front(next_msg); 
                             let mut temp_ops = std::mem::take(&mut ops);
                             self.transition_to_error(&mut temp_ops, encode_err, interface);
                             ops = temp_ops; return ops;
                        }
                    }
                }
            }
            _ => { warn!(fd = self.fd, phase = ?previous_phase, "Send completion (ack) received in unexpected phase."); }
        }
        
        if self.phase != previous_phase && 
           (!self.greeting_buffer.is_empty() || !self.network_read_accumulator.is_empty() || !self.plaintext_zmtp_frame_accumulator.is_empty()) {
            if let Err(e) = self.process_buffered_reads(interface, &mut ops) {
                if self.phase != ZmtpHandlerPhase::Error && self.phase != ZmtpHandlerPhase::Closed {
                    let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, e, interface); ops = temp_ops;
                }
            }
        }
        self.ensure_read_is_pending(&mut ops, interface);
        ops
    }

    fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        self.ensure_read_is_pending(&mut ops, interface);

        if Instant::now() > self.handshake_timeout_deadline &&
           !matches!(self.phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
            warn!(fd = self.fd, current_phase=?self.phase, "Overall handshake timeout occurred in prepare_sqes.");
            let err = ZmqError::Timeout;
            let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); ops = temp_ops;
            return ops; 
        }
        
        if self.phase == ZmtpHandlerPhase::DataPhase {
            if ops.sqe_blueprints.is_empty() { 
                if let Some(msg_to_send) = self.outgoing_app_messages.pop_front() {
                    match Self::zmtp_encode_msg_to_bytes(&msg_to_send) {
                        Ok(zmtp_frame_bytes) => {
                            match Self::apply_encryption_if_needed(self.data_cipher.as_mut(), zmtp_frame_bytes) {
                                Ok(final_bytes_to_send) => {
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: final_bytes_to_send });
                                }
                                Err(enc_err) => {
                                    error!(fd=self.fd, "Encryption failed for outgoing app message in prepare_sqes: {}", enc_err);
                                    self.outgoing_app_messages.push_front(msg_to_send); 
                                    let mut temp_ops = std::mem::take(&mut ops);
                                    self.transition_to_error(&mut temp_ops, enc_err, interface);
                                    ops = temp_ops; 
                                }
                            }
                        }
                        Err(encode_err) => {
                             error!(fd=self.fd, "Failed to ZMTP encode outgoing app message in prepare_sqes: {}", encode_err);
                             self.outgoing_app_messages.push_front(msg_to_send); 
                             let mut temp_ops = std::mem::take(&mut ops);
                             self.transition_to_error(&mut temp_ops, encode_err, interface);
                             ops = temp_ops; 
                        }
                    }
                }
                else if ops.sqe_blueprints.is_empty() { 
                    let now = Instant::now();
                    if self.waiting_for_pong {
                        if let Some(ping_sent_at) = self.last_ping_sent_time {
                            if now.duration_since(ping_sent_at) > self.heartbeat_timeout_duration {
                                warn!(fd = self.fd, "PONG timeout in prepare_sqes. Transitioning to error.");
                                let err = ZmqError::Timeout;
                                let mut temp_ops = std::mem::take(&mut ops);
                                self.transition_to_error(&mut temp_ops, err.clone(), interface);
                                ops = temp_ops; 
                            }
                        }
                    } else if let Some(ivl) = self.heartbeat_ivl { 
                        if now.duration_since(self.last_activity_time) >= ivl {
                            debug!(fd = self.fd, "Heartbeat interval elapsed. Preparing PING.");
                            let ping_msg = ZmtpCommand::create_ping(0, b"hb_ping"); 
                            match Self::zmtp_encode_msg_to_bytes(&ping_msg) {
                                Ok(ping_plaintext_bytes) => {
                                    match Self::apply_encryption_if_needed(self.data_cipher.as_mut(), ping_plaintext_bytes) {
                                        Ok(ping_wire_bytes) => {
                                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: ping_wire_bytes });
                                            self.last_sent_was_ping = true; 
                                        }
                                        Err(enc_err) => {
                                             error!(fd=self.fd, "Failed to encrypt PING: {}", enc_err);
                                             let mut temp_ops = std::mem::take(&mut ops);
                                             self.transition_to_error(&mut temp_ops, enc_err, interface);
                                             ops = temp_ops; 
                                        }
                                    }
                                }
                                Err(encode_err) => {
                                    error!(fd=self.fd, "Failed to ZMTP encode PING: {}", encode_err);
                                    let mut temp_ops = std::mem::take(&mut ops);
                                    self.transition_to_error(&mut temp_ops, encode_err, interface);
                                    ops = temp_ops; 
                                }
                            }
                        }
                    }
                }
            }
        }
        ops
    }

    fn handle_outgoing_app_data(&mut self, data: Arc<dyn Any + Send + Sync>, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        match DowncastArcAny::downcast_arc::<Msg>(data) {
            Ok(msg_arc) => {
                let msg_to_send = (*msg_arc).clone(); 
                if self.phase == ZmtpHandlerPhase::DataPhase && self.outgoing_app_messages.is_empty() {
                    match Self::zmtp_encode_msg_to_bytes(&msg_to_send) {
                        Ok(zmtp_frame_bytes) => { 
                            match Self::apply_encryption_if_needed(self.data_cipher.as_mut(), zmtp_frame_bytes) {
                                Ok(final_bytes_to_send) => {
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{ data: final_bytes_to_send });
                                }
                                Err(enc_err) => {
                                    warn!(fd=self.fd, "Encryption failed for direct send of app data: {}. Queuing message.", enc_err);
                                    self.outgoing_app_messages.push_back(msg_to_send);
                                }
                            }
                        }
                        Err(encode_err) => {
                            error!(fd=self.fd, "Failed to ZMTP encode outgoing app message for immediate send: {}. Queuing.", encode_err);
                            self.outgoing_app_messages.push_back(msg_to_send);
                        }
                    }
                } else {
                    trace!(fd = self.fd, phase = ?self.phase, queue_len=self.outgoing_app_messages.len(), "Queuing outgoing app data.");
                    self.outgoing_app_messages.push_back(msg_to_send);
                }
            }
            Err(_original_arc_any) => {
                error!(fd = self.fd, "ZmtpUringHandler received non-Msg app data via handle_outgoing_app_data. Ignoring.");
            }
        }
        self.ensure_read_is_pending(&mut ops, interface);
        ops
    }

    fn close_initiated(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        info!(fd = self.fd, "ZmtpUringHandler: close_initiated called.");
        let mut ops = HandlerIoOps::new(); 
        let previous_phase_on_close = self.phase; 
        
        self.phase = ZmtpHandlerPhase::Closed;
        self.outgoing_app_messages.clear(); 
        
        let close_error_signal = if !matches!(previous_phase_on_close, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
            ZmqError::Internal("Connection closed during handshake by local request".into())
        } else {
            ZmqError::ConnectionClosed 
        };

        self.transition_to_error(&mut ops, close_error_signal, interface);
        
        self.phase = ZmtpHandlerPhase::Closed; 
        ops
    }

    fn fd_has_been_closed(&mut self) {
        info!(fd = self.fd, "ZmtpUringHandler: fd_has_been_closed notification received.");
        self.phase = ZmtpHandlerPhase::Closed;
    }
}

pub struct ZmtpHandlerFactory {}

impl ProtocolHandlerFactory for ZmtpHandlerFactory {
    fn id(&self) -> &'static str { "zmtp-uring/3.1" } 

    fn create_handler(
        &self,
        fd: RawFd,
        _worker_io_config: Arc<WorkerIoConfig>, 
        protocol_config: &ProtocolConfig, 
        is_server_role: bool,
    ) -> Result<Box<dyn UringConnectionHandler + Send>, String> {
        match protocol_config {
            ProtocolConfig::Zmtp(engine_config_arc) => {
                Ok(Box::new(ZmtpUringHandler::new(
                    fd,
                    engine_config_arc.clone(), 
                    is_server_role,
                )))
            }
            #[allow(unreachable_patterns)] 
            _ => Err(format!(
                "ZmtpHandlerFactory (id: '{}') received an incompatible ProtocolConfig variant: {:?}",
                self.id(),
                protocol_config 
            )),
        }
    }
}

trait DowncastArcAny {
    fn downcast_arc<T: Any + Send + Sync>(self) -> Result<Arc<T>, Self> where Self: Sized;
}
impl DowncastArcAny for Arc<dyn Any + Send + Sync> {
    fn downcast_arc<T: Any + Send + Sync>(self) -> Result<Arc<T>, Self> {
        if self.is::<T>() {
            unsafe { Ok(Arc::from_raw(Arc::into_raw(self).cast::<T>())) }
        } else {
            Err(self)
        }
    }
}

trait MsgWithFlags { fn with_flags(self, flags: MsgFlags) -> Self; }
impl MsgWithFlags for Msg { fn with_flags(mut self, flags: MsgFlags) -> Self { self.set_flags(flags); self } }

trait ZmtpConfigSecurityExt {
    fn security_mechanism_bytes_to_propose(&self, is_handler_server_role: bool) -> &'static [u8; MECHANISM_LENGTH];
}
impl ZmtpConfigSecurityExt for ZmtpEngineConfig {
    fn security_mechanism_bytes_to_propose(&self, is_handler_server_role: bool) -> &'static [u8; MECHANISM_LENGTH] {
        #[cfg(feature = "noise_xx")]
        if self.use_noise_xx {
            let can_propose_noise = if is_handler_server_role { 
                self.noise_xx_local_sk_bytes_for_engine.is_some()
            } else { 
                self.noise_xx_local_sk_bytes_for_engine.is_some() && self.noise_xx_remote_pk_bytes_for_engine.is_some()
            };
            if can_propose_noise { return NoiseXxMechanism::NAME_BYTES; }
            else { warn!("NoiseXX configured (use_noise_xx=true) but required keys missing for current role ('{}') to propose; falling back.", if is_handler_server_role {"server"} else {"client"}); }
        }
        if self.use_plain {
            return PlainMechanism::NAME_BYTES;
        }
        NullMechanism::NAME_BYTES
    }
}