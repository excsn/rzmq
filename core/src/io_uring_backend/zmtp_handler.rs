#![cfg(feature = "io-uring")]

use crate::io_uring_backend::connection_handler::{
    UringConnectionHandler, UringWorkerInterface, HandlerIoOps, HandlerSqeBlueprint, UserData,
    WorkerIoConfig, ProtocolHandlerFactory, HandlerUpstreamEvent,
};
use crate::io_uring_backend::ops::{ProtocolConfig, HANDLER_INTERNAL_SEND_OP_UD};
use crate::io_uring_backend::worker::MultishotReader;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::{
    greeting::{ZmtpGreeting, GREETING_LENGTH, MECHANISM_LENGTH},
    command::{ZmtpCommand, ZmtpReady},
    manual_parser::ZmtpManualParser,
};
use crate::security::{
    negotiate_security_mechanism, IDataCipher, Mechanism, NullMechanism,
    PlainMechanism,
};
#[cfg(feature = "noise_xx")]
use crate::security::NoiseXxMechanism;
use crate::socket::options::ZmtpEngineConfig;
use crate::{Blob, ZmqError};
use super::buffer_manager::BufferRingManager;
use super::worker::InternalOpTracker;

use std::sync::Arc;
use std::os::unix::io::RawFd;
use std::time::{Instant, Duration};
use std::collections::{VecDeque, HashMap};
use std::any::Any;

use bytes::{BytesMut, Bytes, BufMut};
use tokio_util::codec::Encoder;
use tracing::{debug, error, info, trace, warn};

const ZC_SEND_THRESHOLD: usize = 4096;

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

    outgoing_app_messages: VecDeque<(Msg, UserData)>, 
    outgoing_multipart_app_messages: VecDeque<(Vec<Msg>, UserData)>,
    
    handshake_timeout: Duration,
    handshake_timeout_deadline: Instant,
    read_is_pending: bool, 

    peer_identity_from_security: Option<Blob>, 
    peer_identity_from_ready: Option<Blob>,    
    final_peer_identity: Option<Blob>,         
    
    last_sent_was_ping: bool,
    multishot_reader: Option<MultishotReader>,
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
            outgoing_multipart_app_messages: VecDeque::new(),
            handshake_timeout: handshake_timeout_duration,
            handshake_timeout_deadline: Instant::now() + handshake_timeout_duration,
            read_is_pending: false,
            peer_identity_from_security: None,
            peer_identity_from_ready: None,
            final_peer_identity: None,
            last_sent_was_ping: false,
            multishot_reader: None, 
        }
    }

    fn zmtp_encode_msg_to_bytes(msg: &Msg) -> Result<Bytes, ZmqError> {
        let mut temp_codec = crate::protocol::zmtp::ZmtpCodec::new();
        let mut dst_buffer = BytesMut::new();
        temp_codec.encode(msg.clone(), &mut dst_buffer)?;
        Ok(dst_buffer.freeze())
    }

    /// Encodes a Vec<Msg> (representing ZMTP frames) into a Vec<Bytes> (wire frames).
    /// Applies encryption if a data_cipher is active.
    fn zmtp_encode_and_encrypt_frames(&mut self, zmtp_frames: Vec<Msg>) -> Result<Vec<Bytes>, ZmqError> {
      let mut wire_frames = Vec::with_capacity(zmtp_frames.len());
      for frame in zmtp_frames {
        // ZMTP encode individual frame (already has its flags)
        let zmtp_encoded_bytes = Self::zmtp_encode_msg_to_bytes(&frame)?;
        // Encrypt if needed
        let wire_frame_bytes = Self::apply_encryption_if_needed(self.data_cipher.as_mut(), zmtp_encoded_bytes)?;
        wire_frames.push(wire_frame_bytes);
      }
      Ok(wire_frames)
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

    /// Ensures a standard (single-shot) ring read is requested if conditions are met.
    fn ensure_standard_read_is_pending(&mut self, ops: &mut HandlerIoOps, interface: &UringWorkerInterface<'_>) {
        if self.multishot_reader.is_some() && self.multishot_reader.as_ref().unwrap().is_active() {
            // Multishot is active, don't submit a standard read.
            return;
        }
        if !self.read_is_pending && !matches!(self.phase, ZmtpHandlerPhase::Closed | ZmtpHandlerPhase::Error) {
            if interface.default_buffer_group_id().is_some() {
                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestRingRead);
                self.read_is_pending = true;
                trace!(fd = self.fd, "ZmtpUringHandler: Requested standard RingRead.");
            } else {
                error!(fd = self.fd, "ZmtpUringHandler: Critical - Cannot request standard read, no default_bgid configured for worker.");
                let err = ZmqError::Internal("Standard read required but not configured in worker (no default_bgid).".into());

                let mut temp_ops = std::mem::take(ops);
                self.transition_to_error(&mut temp_ops, err.clone(), interface);
                *ops = temp_ops;
            }
        }
    }

    fn transition_to_error(&mut self, ops: &mut HandlerIoOps, error: ZmqError, interface: &UringWorkerInterface<'_>) {
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

        // Signal error upstream using the new HandlerUpstreamEvent
        if !matches!(previous_phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
             warn!(fd=self.fd, "Signaling handshake failure upstream due to error: {}", error);
             let _ = interface.worker_io_config.upstream_event_tx.try_send(
                 (self.fd, HandlerUpstreamEvent::Error(error))
             );
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
        
        // Use the new HandlerUpstreamEvent to signal completion
        let event = HandlerUpstreamEvent::HandshakeComplete {
            peer_identity: self.final_peer_identity.clone(),
        };

        interface.worker_io_config.upstream_event_tx.try_send(
            (self.fd, event)
        ).map_err(|e| {
            error!(fd=self.fd, "Failed to send HandshakeComplete signal upstream: {:?}", e);
            ZmqError::Internal("Failed to signal handshake completion".into())
        }).map(|_| ())
    }

    fn process_buffered_reads(&mut self, interface: &UringWorkerInterface<'_>, ops: &mut HandlerIoOps) -> Result<bool, ZmqError> {
        let mut made_progress_this_call = false;

        // Outer loop: keep processing as long as progress is made or phases change
        // and buffers might have data relevant to the new phase.
        'phase_processing_loop: loop {
            // Store initial buffer lengths to detect if any data was consumed in this iteration of the outer loop.
            // This helps decide if we should loop again or if we're stuck.
            let initial_greeting_len_outer = self.greeting_buffer.len();
            let initial_network_acc_len_outer = self.network_read_accumulator.len();
            let initial_plaintext_acc_len_outer = self.plaintext_zmtp_frame_accumulator.len();
            let mut progress_this_iteration = false;


            // Handshake timeout check
            if Instant::now() > self.handshake_timeout_deadline &&
               !matches!(self.phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
                warn!(fd=self.fd, current_phase=?self.phase, "Overall handshake timeout occurred in process_buffered_reads.");
                let err = ZmqError::Timeout;
                // transition_to_error will modify ops and self.phase
                self.transition_to_error(ops, err.clone(), interface);
                return Err(err); 
            }
            
            trace!(fd=self.fd, phase=?self.phase, greeting_buf_len=self.greeting_buffer.len(), net_acc_len=self.network_read_accumulator.len(), "ProcessBufferedReads: Top of loop");

            match self.phase {
                ZmtpHandlerPhase::Initial => {
                    error!(fd=self.fd, "ZmtpHandler in Initial phase during process_buffered_reads. This is a bug.");
                    let err = ZmqError::InvalidState("ZmtpHandler in Initial phase during data processing".into());
                    self.transition_to_error(ops, err.clone(), interface);
                    return Err(err);
                }

                // Phases where this function primarily waits for send completions, not for processing read data.
                ZmtpHandlerPhase::ClientSendGreeting | ZmtpHandlerPhase::ServerSendGreeting |
                ZmtpHandlerPhase::ReadyClientSend | ZmtpHandlerPhase::ReadyServerSend => {
                    trace!(fd=self.fd, phase=?self.phase, "ProcessBufferedReads: In a 'Send' phase, primarily waiting for send ACK. No read processing.");
                    break 'phase_processing_loop; // No read processing in these states from this function
                }

                // Greeting Exchange (Server waiting for Client's Greeting)
                ZmtpHandlerPhase::ServerWaitClientGreeting => {
                    let needed_for_greeting = GREETING_LENGTH.saturating_sub(self.greeting_buffer.len());
                    if needed_for_greeting > 0 {
                        let source_buf = &mut self.network_read_accumulator; 
                        let can_take = std::cmp::min(needed_for_greeting, source_buf.len());
                        if can_take > 0 {
                            self.greeting_buffer.put(source_buf.split_to(can_take));
                            progress_this_iteration = true;
                        }
                        if self.greeting_buffer.len() < GREETING_LENGTH { break 'phase_processing_loop; /* Need more data for greeting */ }
                    }

                    match ZmtpGreeting::decode(&mut self.greeting_buffer) { 
                        Ok(Some(peer_greeting)) => {
                            progress_this_iteration = true;
                            debug!(fd = self.fd, role = "S", ?peer_greeting, "Received and decoded client greeting");
                            if self.is_server == peer_greeting.as_server { 
                                let err = ZmqError::SecurityError("Role mismatch in greeting".into());
                                self.transition_to_error(ops, err.clone(), interface); return Err(err);
                            }
                            self.security_mechanism = Some(negotiate_security_mechanism(self.is_server, &self.zmtp_config, &peer_greeting, self.fd as usize)?);
                            info!(fd=self.fd, mechanism=?self.security_mechanism.as_ref().unwrap().name(), "Negotiated security mechanism");

                            // Server sends its greeting in response
                            let mut greeting_to_send_buf = BytesMut::with_capacity(GREETING_LENGTH);
                            ZmtpGreeting::encode(self.zmtp_config.security_mechanism_bytes_to_propose(self.is_server), true, &mut greeting_to_send_buf);
                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                                data: greeting_to_send_buf.freeze(),
                                send_op_flags: 0, // The ZMTP greeting is a single, fixed-size "frame".
                                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                            });
                            self.phase = ZmtpHandlerPhase::ServerSendGreeting; // Expect ACK for this send
                        }
                        Ok(None) => { /* Should not happen if greeting_buffer.len() == GREETING_LENGTH */ }
                        Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                    }
                }
                
                // Greeting Exchange (Client waiting for Server's Greeting) - similar to above
                ZmtpHandlerPhase::ClientWaitServerGreeting => {
                    let needed_for_greeting = GREETING_LENGTH.saturating_sub(self.greeting_buffer.len());
                    if needed_for_greeting > 0 {
                        let source_buf = &mut self.network_read_accumulator; 
                        let can_take = std::cmp::min(needed_for_greeting, source_buf.len());
                        if can_take > 0 {
                            self.greeting_buffer.put(source_buf.split_to(can_take));
                            progress_this_iteration = true;
                        }
                        if self.greeting_buffer.len() < GREETING_LENGTH { break 'phase_processing_loop; }
                    }

                    match ZmtpGreeting::decode(&mut self.greeting_buffer) { 
                        Ok(Some(peer_greeting)) => {
                            progress_this_iteration = true;
                            debug!(fd = self.fd, role = "C", ?peer_greeting, "Received and decoded server greeting");
                             if self.is_server == peer_greeting.as_server { 
                                let err = ZmqError::SecurityError("Role mismatch in greeting".into());
                                self.transition_to_error(ops, err.clone(), interface); return Err(err);
                            }
                            self.security_mechanism = Some(negotiate_security_mechanism(self.is_server, &self.zmtp_config, &peer_greeting, self.fd as usize)?);
                            info!(fd=self.fd, mechanism=?self.security_mechanism.as_ref().unwrap().name(), "Negotiated security mechanism");
                            self.phase = ZmtpHandlerPhase::SecurityExchange; // Now proceed to security token exchange
                        }
                        Ok(None) => {}
                        Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                    }
                }

                ZmtpHandlerPhase::SecurityExchange => {
                    trace!(fd = self.fd, phase = ?self.phase, "ProcessBufferedReads: Entering SecurityExchange arm.");

                    let mut should_transition_out_of_security_exchange = false;
                    let mut mechanism_name_for_log_on_completion = ""; 
                    let mut peer_id_from_sec_mech_on_completion: Option<Blob> = None;
                    let mut mechanism_had_error = false;
                    let mut error_reason_from_mechanism = String::new();

                    if let Some(sec_mech_ref) = self.security_mechanism.as_mut() {
                        if sec_mech_ref.is_complete() {
                            info!(fd = self.fd, "SecurityExchange: Mechanism ({}) already complete. Preparing transition.", sec_mech_ref.name());
                            mechanism_name_for_log_on_completion = sec_mech_ref.name();
                            peer_id_from_sec_mech_on_completion = sec_mech_ref.peer_identity().map(Blob::from);
                            should_transition_out_of_security_exchange = true;
                        } else {
                            let mut token_action_this_iteration = false;

                            // Try to produce our token. produce_token() itself should handle "whose turn".
                            if let Some(token_to_send_vec) = sec_mech_ref.produce_token()? {
                                debug!(fd = self.fd, "SecurityExchange: Producing token (len {}).", token_to_send_vec.len());
                                let token_msg = Msg::from_vec(token_to_send_vec).with_flags(MsgFlags::COMMAND);
                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&token_msg)?,
                                    send_op_flags: 0, // Security tokens are ZMTP command frames, usually single.
                                    originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                                });
                                progress_this_iteration = true; token_action_this_iteration = true;
                            }

                            // If there's data from the peer, try to process it.
                            if !self.network_read_accumulator.is_empty() {
                                match self.zmtp_parser.decode_from_buffer(&mut self.network_read_accumulator) {
                                    Ok(Some(token_msg_from_peer)) => {
                                        debug!(fd = self.fd, "SecurityExchange: Decoded peer token (len {}).", token_msg_from_peer.size());
                                        progress_this_iteration = true; token_action_this_iteration = true;
                                        if !token_msg_from_peer.is_command() {
                                            let err_msg = "Expected ZMTP COMMAND for security token".to_string();
                                            mechanism_had_error = true; 
                                            error_reason_from_mechanism = err_msg;
                                        } else {
                                            sec_mech_ref.process_token(token_msg_from_peer.data().unwrap_or_default())?;
                                        }
                                        
                                        // After processing peer's token, it might be our turn to send a response token.
                                        // Call produce_token() again.
                                        if !mechanism_had_error { // Only if no error so far
                                            if let Some(response_token_vec) = sec_mech_ref.produce_token()? {
                                                debug!(fd = self.fd, "SecurityExchange: Producing response token (len {}).", response_token_vec.len());
                                                let response_token_msg = Msg::from_vec(response_token_vec).with_flags(MsgFlags::COMMAND);
                                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { 
                                                    data: Self::zmtp_encode_msg_to_bytes(&response_token_msg)?,
                                                    send_op_flags: 0, // Security tokens are ZMTP command frames, usually single.
                                                    originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,

                                                });
                                                // progress_this_iteration and token_action_this_iteration are likely already true
                                            }
                                        }
                                    }
                                    Ok(None) => {
                                        trace!(fd = self.fd, "SecurityExchange: Accumulator has data, but not a full ZMTP frame for security token yet.");
                                    }
                                    Err(e) => {
                                        mechanism_had_error = true;
                                        error_reason_from_mechanism = format!("Failed to parse ZMTP frame for security token: {}", e);
                                    }
                                }
                            }
                            
                            // Check mechanism status after attempting to produce/process
                            // This must happen *after* any produce_token or process_token calls in this iteration.
                            if !mechanism_had_error { // Only check these if no parsing error occurred
                                if sec_mech_ref.is_complete() {
                                    info!(fd=self.fd, "SecurityExchange: Mechanism ({}) became complete after token produce/process.", sec_mech_ref.name());
                                    mechanism_name_for_log_on_completion = sec_mech_ref.name();
                                    peer_id_from_sec_mech_on_completion = sec_mech_ref.peer_identity().map(Blob::from);
                                    should_transition_out_of_security_exchange = true;
                                } else if sec_mech_ref.is_error() {
                                    mechanism_had_error = true; // Mark that the mechanism itself reported an error
                                    error_reason_from_mechanism = sec_mech_ref.error_reason().unwrap_or("Unknown security error from mechanism").to_string();
                                }
                            }

                            // If an error occurred (either parsing or from mechanism), transition to error.
                            if mechanism_had_error {
                                let err = ZmqError::SecurityError(error_reason_from_mechanism.clone());
                                self.transition_to_error(ops, err.clone(), interface); 
                                return Err(err); // Fatal error in security exchange
                            }
                            
                            // If no token action was taken in this sub-iteration AND the accumulator is empty AND not yet ready to transition,
                            // then we are waiting.
                            if !token_action_this_iteration && self.network_read_accumulator.is_empty() && !should_transition_out_of_security_exchange {
                                trace!(fd = self.fd, "SecurityExchange: No token action, buffer empty, not complete. Waiting for peer/ACK.");
                                break 'phase_processing_loop; 
                            }
                        }
                    } else {
                        let err = ZmqError::InvalidState("CRITICAL: Security mechanism is None while in SecurityExchange phase.".into());
                        self.transition_to_error(ops, err.clone(), interface); 
                        return Err(err);
                    }

                    if should_transition_out_of_security_exchange {
                        trace!(fd = self.fd, "SecurityExchange: Executing transition post-completion.");
                        self.peer_identity_from_security = peer_id_from_sec_mech_on_completion;
                        
                        let taken_mechanism = self.security_mechanism.take()
                            .expect("INTERNAL ERROR: security_mechanism was Some but now None before take for transition"); 
                        
                        let (cipher, _final_peer_id_from_sec_mech) = taken_mechanism.into_data_cipher_parts()?;
                        self.data_cipher = Some(cipher); 
                        
                        let old_phase_before_transition = self.phase; 
                        self.phase = if self.is_server { 
                            ZmtpHandlerPhase::ReadyServerWaitClient
                        } else { 
                            ZmtpHandlerPhase::ReadyClientSend       
                        };
                        info!(fd=self.fd, old_phase=?old_phase_before_transition, new_phase=?self.phase, mech_completed=mechanism_name_for_log_on_completion, "Transitioned out of SecurityExchange.");
                        
                        if !self.is_server { 
                            let client_ready_msg = ZmtpReady::create_msg(self.build_ready_properties());
                            debug!("[ZmtpHandler FD={}] Client adding its ZMTP READY Send blueprint (from SecurityExchange transition).", self.fd);
                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{
                                data: Self::zmtp_encode_msg_to_bytes(&client_ready_msg)?,
                                send_op_flags: 0, // ZMTP READY is a single ZMTP command frame.
                                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                            });
                        } else {
                            debug!("[ZmtpHandler FD={}] Server finished security ({}), now in {:?} phase (from SecurityExchange transition). Waiting for client's ZMTP READY.", self.fd, mechanism_name_for_log_on_completion, self.phase);
                        }
                        progress_this_iteration = true; 
                    }
                }

                // ZMTP READY Command Exchange (Server waiting for Client's READY)
                ZmtpHandlerPhase::ReadyServerWaitClient => {
                    if self.network_read_accumulator.is_empty() { break 'phase_processing_loop; /* Need data */ }
                    match self.zmtp_parser.decode_from_buffer(&mut self.network_read_accumulator) { 
                        Ok(Some(ready_msg_from_client)) => {
                            progress_this_iteration = true;
                            match ZmtpCommand::parse(&ready_msg_from_client) { 
                                Some(ZmtpCommand::Ready(ready_data)) => {
                                    debug!(fd=self.fd, "S: Received Client's READY. Properties: {:?}", ready_data.properties);
                                    if let Some(id_bytes_vec) = ready_data.properties.get("Identity") {
                                        self.peer_identity_from_ready = Some(Blob::from(id_bytes_vec.clone()));
                                    }
                                    
                                    // Server now sends its own READY
                                    let server_ready_msg = ZmtpReady::create_msg(self.build_ready_properties());
                                    debug!("[ZmtpHandler FD={}] S: Adding its ZMTP READY Send blueprint.", self.fd);
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                                        data: Self::zmtp_encode_msg_to_bytes(&server_ready_msg)?,
                                        send_op_flags: 0, // ZMTP READY is a single ZMTP command frame.
                                        originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                                    });
                                    self.phase = ZmtpHandlerPhase::ReadyServerSend; 
                                }
                                _ => { 
                                    let err = ZmqError::ProtocolViolation("S: Expected READY from client, got other/unparseable".into());
                                    self.transition_to_error(ops, err.clone(), interface); return Err(err);
                                }
                            }
                        }
                        Ok(None) => { break 'phase_processing_loop; /* Need more data for client's READY */ }
                        Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                    }
                }
                
                // ZMTP READY Command Exchange (Client waiting for Server's READY)
                ZmtpHandlerPhase::ReadyClientWaitServer => {
                    if self.network_read_accumulator.is_empty() { break 'phase_processing_loop; /* Need data */ }
                     match self.zmtp_parser.decode_from_buffer(&mut self.network_read_accumulator) { 
                        Ok(Some(ready_msg_from_server)) => {
                            progress_this_iteration = true;
                            match ZmtpCommand::parse(&ready_msg_from_server) { 
                                Some(ZmtpCommand::Ready(ready_data)) => {
                                    debug!(fd=self.fd, "C: Received Server's READY. Properties: {:?}", ready_data.properties);
                                    if let Some(id_bytes_vec) = ready_data.properties.get("Identity") {
                                         self.peer_identity_from_ready = Some(Blob::from(id_bytes_vec.clone()));
                                    }
                                    // Client handshake fully complete
                                    self.phase = ZmtpHandlerPhase::DataPhase;
                                    info!(fd=self.fd, "ZmtpUringHandler: Client handshake fully complete. Transitioning to DataPhase.");
                                    self.signal_upstream_handshake_complete(interface)?; 
                                }
                                _ => { 
                                    let err = ZmqError::ProtocolViolation("C: Expected READY from server, got other/unparseable".into());
                                    self.transition_to_error(ops, err.clone(), interface); return Err(err);
                                }
                            }
                        }
                        Ok(None) => { break 'phase_processing_loop; /* Need more data for server's READY */ }
                        Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                    }
                }

                ZmtpHandlerPhase::DataPhase => {
                    // Decrypt network data into plaintext ZMTP frame accumulator first
                    if self.data_cipher.is_some() {
                        while !self.network_read_accumulator.is_empty() {
                             match self.data_cipher.as_mut().unwrap().decrypt_wire_data_to_zmtp_frame(&mut self.network_read_accumulator) {
                                Ok(Some(plaintext_zmtp_frame_bytes)) => {
                                    progress_this_iteration = true;
                                    self.plaintext_zmtp_frame_accumulator.put(plaintext_zmtp_frame_bytes);
                                }
                                Ok(None) => break, // Need more encrypted data
                                Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                            }
                        }
                    } else { // No cipher, move directly
                        if !self.network_read_accumulator.is_empty() {
                            progress_this_iteration = true;
                            self.plaintext_zmtp_frame_accumulator.extend_from_slice(&self.network_read_accumulator);
                            self.network_read_accumulator.clear();
                        }
                    };
                    
                    if self.plaintext_zmtp_frame_accumulator.is_empty() { break 'phase_processing_loop; /* Need plaintext data */ }

                    // Process all complete ZMTP frames from plaintext_zmtp_frame_accumulator
                    while !self.plaintext_zmtp_frame_accumulator.is_empty() {
                        match self.zmtp_parser.decode_from_buffer(&mut self.plaintext_zmtp_frame_accumulator) {
                            Ok(Some(msg)) => {
                                progress_this_iteration = true;
                                self.last_activity_time = Instant::now(); 
                                if msg.is_command() {
                                    match ZmtpCommand::parse(&msg) {
                                        Some(ZmtpCommand::Ping(ping_context_payload)) => {
                                            let pong_reply_msg = ZmtpCommand::create_pong(&ping_context_payload);
                                            let pong_plaintext_bytes = Self::zmtp_encode_msg_to_bytes(&pong_reply_msg)?;
                                            let pong_wire_bytes = Self::apply_encryption_if_needed(self.data_cipher.as_mut(), pong_plaintext_bytes)?;
                                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                                                data: pong_wire_bytes,
                                                send_op_flags: 0, // PONG is a single ZMTP frame, no MSG_MORE.
                                                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                                            });
                                            debug!(fd=self.fd, "DataPhase: Prepared PONG in response to PING");
                                        }
                                        Some(ZmtpCommand::Pong(_pong_context_payload)) => {
                                            self.waiting_for_pong = false; self.last_ping_sent_time = None;
                                            debug!(fd=self.fd, "DataPhase: Received PONG");
                                        }
                                        Some(ZmtpCommand::Error) => { 
                                            warn!(fd = self.fd, "DataPhase: Peer sent ZMTP ERROR command.");
                                            let err = ZmqError::ProtocolViolation("Peer sent ZMTP ERROR command".into());
                                            self.transition_to_error(ops, err.clone(), interface); return Err(err);
                                        }
                                        _ => { warn!(fd = self.fd, "DataPhase: Received unhandled ZMTP command: {:?}", msg.data()); }
                                    }
                                } else { // Data message
                                    let upstream_event = HandlerUpstreamEvent::Data(msg);
                                    if let Err(send_err) = interface.worker_io_config.upstream_event_tx.try_send((self.fd, upstream_event)) {
                                        error!(fd = self.fd, "DataPhase: Failed to send ZMTP data msg upstream: {:?}", send_err);
                                        let err = ZmqError::Internal("Upstream channel error for ZMTP data".into());
                                        self.transition_to_error(ops, err.clone(), interface); return Err(err);
                                    }
                                    // println!("[ZmtpHandler FD={}] Sent to UpstreamProcQ (Channel B); Global ParsedMsgQ len now: {}", self.fd, interface.worker_io_config.parsed_msg_tx_zmtp.len()); //TODO Remove Logging?
                                }
                            }
                            Ok(None) => break, // Need more data in plaintext_zmtp_frame_accumulator
                            Err(e) => { self.transition_to_error(ops, e.clone(), interface); return Err(e); }
                        }
                    }
                }
                ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed => {
                    break 'phase_processing_loop; // Final states, no more processing
                }
            } // End match self.phase

            made_progress_this_call |= progress_this_iteration;

            // Check if loop should continue:
            // If no data was consumed from any buffer, and no other progress (like phase change) was made in *this iteration*, break.
            let no_greeting_change_outer = self.greeting_buffer.len() == initial_greeting_len_outer;
            let no_network_acc_change_outer = self.network_read_accumulator.len() == initial_network_acc_len_outer;
            let no_plaintext_acc_change_outer = self.plaintext_zmtp_frame_accumulator.len() == initial_plaintext_acc_len_outer;

            if no_greeting_change_outer && no_network_acc_change_outer && no_plaintext_acc_change_outer && !progress_this_iteration {
                trace!(fd=self.fd, phase=?self.phase, "ProcessBufferedReads: No data consumed or progress in this iteration. Breaking inner loop.");
                break 'phase_processing_loop; 
            }
            // If progress was made (data consumed or phase changed), allow loop to continue to re-evaluate with new state/buffers.
            // Reset for next iteration of outer loop.
            // made_progress_this_call is accumulated across iterations of this outer loop
        } // End 'phase_processing_loop
        
        Ok(made_progress_this_call) // Return overall progress
    }

    // Helper method that needs to be implemented within ZmtpUringHandler
    // This is a placeholder for your actual logic to convert a single app Msg
    // into all necessary ZMTP wire frames (e.g., delimiter + payload for REQ/DEALER).
    // Each Bytes in the Vec should be a complete, ZMTP-encoded, and encrypted wire frame.
    fn prepare_zmtp_wire_frames_for_app_msg(&mut self, app_msg: Msg) -> Result<Vec<Bytes>, ZmqError> {
        let mut wire_frames = Vec::new();

        // Example for REQ/DEALER like sockets that prepend an empty delimiter
        if self.zmtp_config.socket_type_name == "REQ" || self.zmtp_config.socket_type_name == "DEALER" {
            let delimiter_zmtp = Msg::new().with_flags(MsgFlags::MORE); // Delimiter always has MORE
            let d_bytes = Self::zmtp_encode_msg_to_bytes(&delimiter_zmtp)?;
            let enc_d_bytes = Self::apply_encryption_if_needed(self.data_cipher.as_mut(), d_bytes)?;
            wire_frames.push(enc_d_bytes);
        }

        // Prepare the main payload part
        let mut payload_part_for_wire = app_msg;
        // The payload is the last part of this logical ZMQ message, so it should not have MORE
        // (unless this `app_msg` itself was marked by the user as part of a larger sequence,
        // which is less common for REQ/DEALER app-level messages).
        // For safety, ensure the app-level payload, when it becomes the last ZMTP frame, has NOMORE.
        payload_part_for_wire.set_flags(payload_part_for_wire.flags() & !MsgFlags::MORE);

        let p_bytes = Self::zmtp_encode_msg_to_bytes(&payload_part_for_wire)?;
        let enc_p_bytes = Self::apply_encryption_if_needed(self.data_cipher.as_mut(), p_bytes)?;
        wire_frames.push(enc_p_bytes);

        Ok(wire_frames)
    }

    /// Helper to take final ZMTP wire frames, decide on ZC/normal send,
    /// and add appropriate blueprints to HandlerIoOps.
    fn add_send_blueprints_for_wire_frames(
        &self, // Needs &self to access zmtp_config and ZC_SEND_THRESHOLD
        final_wire_frames: Vec<Bytes>, // Already ZMTP encoded & encrypted
        originating_op_ud_for_blueprints: UserData, // Actual app UD or sentinel
        ops: &mut HandlerIoOps,
    ) {
        let num_final_wire_frames = final_wire_frames.len();
        if num_final_wire_frames == 0 {
            // This case should ideally be handled by the caller (e.g., prepare_zmtp_wire_frames_for_app_msg
            // should not return an empty Vec unless it's a valid ZMTP way to send "nothing").
            // For PUSH, an empty app message might mean nothing is sent.
            trace!(fd = self.fd, op_ud = originating_op_ud_for_blueprints, "add_send_blueprints_for_wire_frames called with empty wire frames. No blueprints added.");
            return;
        }

        for (idx, final_wire_bytes_for_part) in final_wire_frames.into_iter().enumerate() {
            let is_last_logical_part = idx == num_final_wire_frames - 1;
            let send_op_flags: i32 = if is_last_logical_part { 0 } else { libc::MSG_MORE };

            if self.zmtp_config.use_send_zerocopy 
            && final_wire_bytes_for_part.len() > ZC_SEND_THRESHOLD 
            {
                trace!(fd = self.fd, len = final_wire_bytes_for_part.len(), part_idx = idx, app_op_ud = originating_op_ud_for_blueprints, "ZmtpHandler (helper): Attempting ZC send.");
                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSendZeroCopy {
                    data_to_send: final_wire_bytes_for_part,
                    send_op_flags,
                    originating_app_op_ud: originating_op_ud_for_blueprints,
                });
            } else {
                trace!(fd = self.fd, len = final_wire_bytes_for_part.len(), part_idx = idx, app_op_ud = originating_op_ud_for_blueprints, zc_enabled = self.zmtp_config.use_send_zerocopy, "ZmtpHandler (helper): Using normal send.");
                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                    data: final_wire_bytes_for_part,
                    send_op_flags,
                    originating_app_op_ud: originating_op_ud_for_blueprints,
                });
            }
        }
    }
}


impl UringConnectionHandler for ZmtpUringHandler {
    fn fd(&self) -> RawFd { self.fd }

    fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        info!(fd = self.fd, role = if self.is_server {"S"} else {"C"}, "ZmtpUringHandler: connection_ready.");
        self.last_activity_time = Instant::now();
        self.handshake_timeout_deadline = Instant::now() + self.handshake_timeout; 
        let mut ops = HandlerIoOps::new();

         if self.zmtp_config.use_recv_multishot { // Assuming ZmtpEngineConfig has this field
            if let Some(bgid) = interface.default_buffer_group_id() {
                self.multishot_reader = Some(MultishotReader::new(self.fd, bgid));
                tracing::debug!("[ZmtpUringHandler FD={}] MultishotReader initialized. Initial read will be requested via prepare_sqes.", self.fd);
                // The actual RequestRingReadMultishot blueprint will be added by prepare_sqes.
            } else {
                tracing::error!("[ZmtpUringHandler FD={}] Multishot configured (use_recv_multishot=true) but no default_bgid available from worker interface! Falling back to standard reads.", self.fd);
                // Fallback: ensure standard read is requested if multishot setup fails
                self.ensure_standard_read_is_pending(&mut ops, interface);
            }
        } else {
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }

        if self.is_server {
            self.phase = ZmtpHandlerPhase::ServerWaitClientGreeting;
        } else { 
            let mut greeting_to_send_buf = BytesMut::with_capacity(GREETING_LENGTH);
            let proposed_mechanism_bytes = self.zmtp_config.security_mechanism_bytes_to_propose(self.is_server);
            ZmtpGreeting::encode(proposed_mechanism_bytes, false, &mut greeting_to_send_buf);
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                data: greeting_to_send_buf.freeze(),
                send_op_flags: 0, // Greeting is a single "frame".
                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD, 
            });
            self.phase = ZmtpHandlerPhase::ClientSendGreeting;
        }
        self.ensure_standard_read_is_pending(&mut ops, interface);
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

            if let Some(reader) = &mut self.multishot_reader {
                if reader.is_active() {
                    // The `interface` doesn't easily provide `InternalOpTracker` here.
                    // `prepare_cancel_blueprint` in MultishotReader needs it.
                    // This suggests that either `process_ring_read_data` needs the tracker,
                    // or cancellation due to EOF is handled differently (e.g. by `cqe_processor`
                    // which *can* call `reader.prepare_cancel_blueprint` with the tracker).
                    // For now, we'll just transition to error and let `close_initiated` handle cancel.
                    tracing::info!("[ZmtpUringHandler FD={}] EOF received, multishot was active. Will be cancelled during close_initiated.", self.fd);
                }
            }

            let mut temp_ops = std::mem::take(&mut ops);
            self.transition_to_error(&mut temp_ops, eof_err.clone(), interface); // Pass clone
            ops = temp_ops;
            // Also ensure the error is sent upstream if transition_to_error didn't send it (e.g., if already in DataPhase)
            if matches!(original_phase_eof, ZmtpHandlerPhase::DataPhase) {
                 let _ = interface.worker_io_config.upstream_event_tx.try_send(
                     (self.fd, HandlerUpstreamEvent::Error(eof_err))
                 );
            }
            
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
        
        
        if self.multishot_reader.as_ref().map_or(true, |r| !r.is_active()) {
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }

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
                    if let Ok(Some(token_vec)) = sec_mech.produce_token() { // produce_token() decides if it's turn.
                         let token_msg = Msg::from_vec(token_vec).with_flags(MsgFlags::COMMAND);
                         if let Ok(bytes) = Self::zmtp_encode_msg_to_bytes(&token_msg) {
                             ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{
                                data: bytes,
                                send_op_flags: 0, // Security tokens are ZMTP command frames, usually single.
                                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD, // Internal protocol message.
                            });
                         } else {
                             let err = ZmqError::Internal("Failed to encode initial server security token".into());
                             let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, err, interface); ops = temp_ops; return ops;
                         }
                    }
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

                if let Some((multipart_msg_parts, queued_originating_app_op_ud)) = self.outgoing_multipart_app_messages.pop_front() {
                  match self.zmtp_encode_and_encrypt_frames(multipart_msg_parts.clone()) {
                    Ok(wire_frames_to_send) => {
                        self.add_send_blueprints_for_wire_frames(
                            wire_frames_to_send,
                            queued_originating_app_op_ud,
                            &mut ops,
                        );
                    }
                    Err(e) => { /* error handling, potentially re-queue with UD or handle error */
                        error!(fd = self.fd, "Failed to encode/encrypt queued multipart message: {}. Message dropped from queue.", e);
                        // Re-queue the original parts along with their UserData on failure
                        self.outgoing_multipart_app_messages.push_front((multipart_msg_parts, queued_originating_app_op_ud));
                        // Potentially transition to error or handle differently based on error type
                        self.transition_to_error(&mut ops, e, interface); // Assuming interface is available
                        return ops; // Or continue if error is not fatal for other operations
                    }
                }
                } else if let Some((next_app_msg, queued_originating_app_op_ud)) = self.outgoing_app_messages.pop_front() {
                    // This app_msg needs to be converted into one or more ZMTP wire frames.
                    // For example, a REQ socket sends [empty_delimiter_MORE, payload_NOMORE].
                    // A PUSH socket sends [payload_NOMORE].
                    // The prepare_zmtp_wire_frames_for_app_msg helper handles this.
                    match self.prepare_zmtp_wire_frames_for_app_msg(next_app_msg.clone()) { // Clone if re-queue on error
                        Ok(wire_frames_to_send) => { // This returns Vec<Bytes>
                            
                            self.add_send_blueprints_for_wire_frames(
                                wire_frames_to_send,
                                queued_originating_app_op_ud,
                                &mut ops,
                            );
                        }
                        Err(e) => {
                            error!(fd = self.fd, "Failed to prepare_zmtp_wire_frames for queued single message: {}. Re-queuing.", e);
                            // Re-queue the original app message and its UserData on failure
                            self.outgoing_app_messages.push_front((next_app_msg, queued_originating_app_op_ud));
                            self.transition_to_error(&mut ops, e, interface); // Assuming interface is available
                            return ops; // Or continue
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
        
        if self.multishot_reader.as_ref().map_or(true, |r| !r.is_active()) {
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }

        ops
    }

    fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        self.ensure_standard_read_is_pending(&mut ops, interface);

        if Instant::now() > self.handshake_timeout_deadline &&
           !matches!(self.phase, ZmtpHandlerPhase::DataPhase | ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed) {
            warn!(fd = self.fd, current_phase=?self.phase, "Overall handshake timeout occurred in prepare_sqes.");
            let err = ZmqError::Timeout;
            let mut temp_ops = std::mem::take(&mut ops); self.transition_to_error(&mut temp_ops, err.clone(), interface); ops = temp_ops;
            return ops;
        }
        
        if let Some(reader) = &mut self.multishot_reader {
            if !reader.is_active() { // Use the reader's state
                if let Some(blueprint) = reader.prepare_recv_multi_intent() {
                    ops.sqe_blueprints.push(blueprint);
                }
            }
        } else if !self.read_is_pending {
            // Fallback to standard read - interface is needed for default_buffer_group_id
            // Let's pass interface to ensure_standard_read_is_pending
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }

        if self.phase == ZmtpHandlerPhase::DataPhase {
            if ops.sqe_blueprints.is_empty() { 
                if let Some((multipart_app_parts, queued_originating_app_op_ud)) = self.outgoing_multipart_app_messages.pop_front() {
                    match self.zmtp_encode_and_encrypt_frames(multipart_app_parts.clone()) { // Clone for potential re-queue
                        Ok(wire_frames_to_send) => { // wire_frames_to_send is Vec<Bytes>
                            self.add_send_blueprints_for_wire_frames(
                                wire_frames_to_send,
                                queued_originating_app_op_ud,
                                &mut ops, // ops is the HandlerIoOps being built
                            );
                        }
                        Err(e) => {
                            error!(fd = self.fd, "Failed to encode/encrypt multipart message from queue in prepare_sqes (or similar): {}. Re-queuing.", e);
                            self.outgoing_multipart_app_messages.push_front((multipart_app_parts, queued_originating_app_op_ud));
                            // Ensure 'interface' is available if this code is in a place where it's not a direct argument
                            // For 'prepare_sqes', 'interface' IS an argument.
                            let mut temp_ops_taken = std::mem::take(&mut ops); // To pass &mut ops to transition_to_error
                            self.transition_to_error(&mut temp_ops_taken, e, interface);
                            ops = temp_ops_taken;
                            // Depending on function's return type, may need to `return ops;` or handle error propagation
                        }
                    }
                } else if let Some((app_msg_to_send, queued_originating_app_op_ud)) = self.outgoing_app_messages.pop_front() {
                    match self.prepare_zmtp_wire_frames_for_app_msg(app_msg_to_send.clone()) {
                        Ok(final_wire_frames_for_single_msg) => {
                            self.add_send_blueprints_for_wire_frames(
                                final_wire_frames_for_single_msg,
                                queued_originating_app_op_ud,
                                &mut ops,
                            );
                        }
                        Err(e) => {
                            error!(fd = self.fd, "Failed to prepare_zmtp_wire_frames for queued single message: {}. Re-queuing.", e);
                            self.outgoing_app_messages.push_front((app_msg_to_send, queued_originating_app_op_ud));
                            let mut temp_ops_taken = std::mem::take(&mut ops);
                            self.transition_to_error(&mut temp_ops_taken, e, interface);
                            ops = temp_ops_taken;
                            // return ops; or handle error
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
                                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend {
                                                data: ping_wire_bytes,
                                                send_op_flags: 0,
                                                originating_app_op_ud: HANDLER_INTERNAL_SEND_OP_UD,
                                            });
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

    fn handle_outgoing_app_data(
        &mut self,
        data: Arc<dyn Any + Send + Sync>,
        interface: &UringWorkerInterface<'_>,
    ) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        let originating_app_op_ud = interface.current_external_op_ud;

        match DowncastArcAny::downcast_arc::<Vec<Msg>>(data.clone()) {
            Ok(app_data_parts_arc) => { // Multipart message
                let app_data_parts_vec = (*app_data_parts_arc).clone();
                if self.phase == ZmtpHandlerPhase::DataPhase
                    && self.outgoing_app_messages.is_empty()
                    && self.outgoing_multipart_app_messages.is_empty()
                {
                    match self.zmtp_encode_and_encrypt_frames(app_data_parts_vec.clone()) {
                        Ok(wire_frames_to_send) => { // wire_frames_to_send is Vec<Bytes>
                            self.add_send_blueprints_for_wire_frames(
                                wire_frames_to_send,
                                originating_app_op_ud,
                                &mut ops,
                            );
                        }
                        Err(e) => {
                            error!(fd = self.fd, "Failed to encode/encrypt outgoing multipart app data: {}. Queuing.", e);
                            self.outgoing_multipart_app_messages.push_back((app_data_parts_vec, originating_app_op_ud));
                        }
                    }
                } else {
                    trace!(fd = self.fd, phase = ?self.phase, "Queuing outgoing multipart app data ({} parts).", app_data_parts_vec.len());
                    self.outgoing_multipart_app_messages.push_back((app_data_parts_vec, originating_app_op_ud));
                }
            }
            Err(original_arc_any) => { // Not Arc<Vec<Msg>>, try Arc<Msg> (single part)
                match DowncastArcAny::downcast_arc::<Msg>(original_arc_any) {
                    Ok(msg_arc) => {
                        let msg_to_send_app_level = (*msg_arc).clone(); // This is the single app-level Msg
                        if self.phase == ZmtpHandlerPhase::DataPhase
                            && self.outgoing_app_messages.is_empty()
                            && self.outgoing_multipart_app_messages.is_empty()
                        {
                            // This is the part that needs to correctly prepare the *full sequence*
                            // of ZMTP wire frames for a single application-level message.
                            // For PUSH, this might be one frame. For REQ/DEALER, it's [delimiter, payload].
                            // Let's assume a helper method `prepare_wire_frames_for_app_msg` exists
                            // that takes the app `Msg` and returns `Result<Vec<Bytes>, ZmqError>`,
                            // where each `Bytes` is a fully ZMTP-encoded and encrypted wire frame.
                            match self.prepare_zmtp_wire_frames_for_app_msg(msg_to_send_app_level.clone()) {
                                Ok(final_wire_frames_for_single_msg) => {
                                    self.add_send_blueprints_for_wire_frames(
                                        final_wire_frames_for_single_msg,
                                        originating_app_op_ud,
                                        &mut ops,
                                    );
                                }
                                Err(e) => {
                                    error!(fd = self.fd, "Failed to prepare wire frames for single app message: {}. Queuing.", e);
                                    self.outgoing_app_messages.push_back((msg_to_send_app_level, originating_app_op_ud));
                                }
                            }
                        } else {
                            trace!(fd = self.fd, phase = ?self.phase, "Queuing outgoing single-part app data.");
                            self.outgoing_app_messages.push_back((msg_to_send_app_level, originating_app_op_ud));
                        }
                    }
                    Err(_unhandled_arc_any) => {
                        error!(fd = self.fd, "ZmtpUringHandler received unknown app data type. Ignoring.");
                    }
                }
            }
        }

        if self.multishot_reader.as_ref().map_or(true, |r| !r.is_active()) {
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }
        ops
    }

    fn close_initiated(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        info!(fd = self.fd, "ZmtpUringHandler: close_initiated called.");
        let mut ops = HandlerIoOps::new(); 
        let previous_phase_on_close = self.phase; 

        if self.zmtp_config.use_recv_multishot { 
            if let Some(bgid) = interface.default_buffer_group_id() {
                self.multishot_reader = Some(MultishotReader::new(self.fd, bgid));
                tracing::debug!("[ZmtpUringHandler FD={}] MultishotReader initialized. Initial read will be requested via prepare_sqes.", self.fd);
            } else {
                tracing::error!("[ZmtpUringHandler FD={}] Multishot configured (use_recv_multishot=true) but no default_bgid available from worker interface! Falling back to standard reads.", self.fd);
                self.ensure_standard_read_is_pending(&mut ops, interface);
            }
        } else {
            self.ensure_standard_read_is_pending(&mut ops, interface);
        }
        
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

     fn delegate_cqe_to_multishot_reader(
        &mut self,
        cqe: &io_uring::cqueue::Entry, // Pass the full CQE
        buffer_manager: &BufferRingManager, 
        worker_interface: &UringWorkerInterface<'_>, 
        internal_op_tracker: &mut InternalOpTracker, 
    ) -> Option<Result<(HandlerIoOps, bool), ZmqError>> { // bool is should_cleanup_active_op_ud
        let cqe_user_data = cqe.user_data();
        
        // Immutable check first to see if the reader exists and if the UserData might match.
        let reader_might_handle = self.multishot_reader
            .as_ref()
            .map_or(false, |r| r.matches_cqe_user_data(cqe_user_data));

        if reader_might_handle {
            // If it might handle, take the reader mutably to process the CQE.
            // This pattern (take, process, put_back) is crucial to avoid borrow checker issues
            // when `MultishotReader::process_cqe` calls `self.process_ring_read_data`.
            if let Some(mut reader) = self.multishot_reader.take() {
                let result_tuple = reader.process_cqe(
                    cqe,
                    buffer_manager,
                    self, // `self` (ZmtpUringHandler) is passed as `owner_handler`
                    worker_interface,
                    internal_op_tracker,
                );
                // Put the reader back after processing
                self.multishot_reader = Some(reader);
                return Some(result_tuple);
            } else {
                // This case should ideally not be reached if `reader_might_handle` was true
                // and `self.multishot_reader` was Some. It implies a logic error or race if
                // another part of the code could also `take()` the reader.
                tracing::error!(
                    "[ZmtpUringHandler FD={}] Inconsistent state in delegate_cqe_to_multishot_reader: \
                    reader_might_handle was true, but multishot_reader was None on take(). CQE UserData: {}",
                    self.fd, cqe_user_data
                );
                // Fall through to return None, indicating CQE was not handled by multishot logic here.
            }
        }
        None // CQE not for an active multishot reader of this handler, or no reader.
    }

    fn inform_multishot_reader_op_submitted(
        &mut self, 
        op_user_data: UserData, 
        is_cancel_op: bool, 
        target_op_data_if_cancel: Option<UserData>
    ) {
        if let Some(reader) = &mut self.multishot_reader {
            if is_cancel_op {
                if let Some(target_ud) = target_op_data_if_cancel {
                    reader.mark_cancellation_submitted(op_user_data, target_ud);
                } else {
                    tracing::warn!("[ZmtpHandler FD={}] inform_multishot_reader_op_submitted called for cancel_op but target_op_data_if_cancel is None.", self.fd);
                }
            } else {
                reader.mark_operation_submitted(op_user_data);
            }
        } else {
            tracing::warn!("[ZmtpHandler FD={}] inform_multishot_reader_op_submitted called but no multishot_reader exists.", self.fd);
        }
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