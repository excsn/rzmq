// core/src/io_uring_backend/zmtp_handler.rs

#![cfg(feature = "io-uring")]

use crate::io_uring_backend::connection_handler::{
    UringConnectionHandler, UringWorkerInterface, HandlerIoOps, HandlerSqeBlueprint, UserData,
    // WorkerIoConfig is not directly stored here, but accessed via UringWorkerInterface
};
// BufferRingManager might not be directly used if all reads are through RequestRingRead blueprint
// use crate::io_uring_backend::buffer_manager::BufferRingManager;

use crate::protocol::zmtp::{
    greeting::{ZmtpGreeting, GREETING_LENGTH, MECHANISM_LENGTH},
    command::{ZmtpCommand, ZmtpReady},
    manual_parser::ZmtpManualParser,
};
use crate::security::{
    self, negotiate_security_mechanism, IDataCipher, Mechanism, MechanismStatus, NullMechanism // For direct use or examples
};
use crate::socket::options::ZmtpEngineConfig;
use crate::message::{Msg, MsgFlags};
use crate::{Blob, ZmqError};

use bytes::{BytesMut, Bytes, BufMut, Buf};
use std::sync::Arc;
use std::os::unix::io::RawFd;
use std::time::{Instant, Duration};
use std::collections::{VecDeque, HashMap}; // For Ready properties
use std::any::Any;

use super::ops::ProtocolConfig;
use super::{ProtocolHandlerFactory, WorkerIoConfig}; // For downcasting app_data

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ZmtpHandlerPhase {
    // Initial states (before any I/O or after connection_ready)
    Initial, // Set in new()

    // Greeting Exchange
    ClientSendGreeting,    // Client: Preparing to send its greeting
    ServerWaitClientGreeting, // Server: Waiting for client's greeting
    ServerSendGreeting,    // Server: Received client's, preparing to send own
    ClientWaitServerGreeting, // Client: Sent own, waiting for server's

    // Security Handshake (after both have exchanged greetings)
    SecurityInitiate,   // Deciding who speaks first in security based on role
    SecurityExchange,   // Actively exchanging security tokens

    // Ready Command Exchange (after security is Ready)
    ReadyClientSend,    // Client: Preparing to send its READY command
    ReadyServerWaitClient, // Server: Waiting for client's READY
    ReadyServerSend,    // Server: Received client's, preparing own READY
    ReadyClientWaitServer, // Client: Sent own, waiting for server's READY

    // Data Transfer
    DataPhase,

    // Terminal States
    Error,
    Closed, // Explicitly closed by close_initiated
}

pub struct ZmtpUringHandler {
    fd: RawFd,
    // worker_io_config: Arc<WorkerIoConfig>, // Not stored directly, accessed via interface
    zmtp_config: Arc<ZmtpEngineConfig>,
    is_server: bool,

    phase: ZmtpHandlerPhase,

    // Buffers
    greeting_buffer: BytesMut,   // For accumulating received greeting bytes
    read_accumulator: BytesMut,  // For all other incoming data (security tokens, ready, data)
    // write_buffer: BytesMut, // For constructing frames to send. Or build Bytes directly.

    // Protocol State
    security_mechanism: Option<Box<dyn Mechanism>>,
    data_cipher: Option<Box<dyn IDataCipher>>,
    zmtp_parser: ZmtpManualParser, // For data phase ZMTP message parsing

    // Heartbeat State
    last_activity_time: Instant,
    last_ping_sent_time: Option<Instant>,
    waiting_for_pong: bool,
    heartbeat_ivl: Option<Duration>,
    heartbeat_timeout_duration: Duration, // Renamed to avoid conflict with Option

    // Outgoing message queue for application data
    outgoing_app_messages: VecDeque<Msg>,
    // Internal command queue (e.g., for PINGs, PONGs, handshake frames)
    // These often need to be prioritized or sent immediately.
    // For simplicity, we can try to send immediately and only queue app data if SQ is full.
    // Or, have a small priority queue for commands.
    // For now, let's try to send commands directly via blueprints.

    // Handshake specific state
    // peer_greeting_info: Option<ZmtpGreeting>, // May not need to store if processed immediately
    pending_outgoing_token: Option<Bytes>, // For security mechanism
    handshake_timeout: Duration, // From ZmtpEngineConfig

    // To ensure reads are always pending if connection is active
    read_is_pending: bool,
}

impl ZmtpUringHandler {
    pub fn new(
        fd: RawFd,
        // worker_io_config is not passed here, UringWorkerInterface provides it.
        zmtp_config: Arc<ZmtpEngineConfig>,
        is_server: bool,
    ) -> Self {
        let handshake_timeout_val = zmtp_config.handshake_timeout.unwrap_or(Duration::from_secs(30));
        let heartbeat_timeout_val = zmtp_config.heartbeat_timeout.unwrap_or_else(||
            zmtp_config.heartbeat_ivl.map_or(Duration::from_secs(30), |ivl| ivl.saturating_mul(2))
        );
        let heartbeat_ivl = zmtp_config.heartbeat_ivl;

        Self {
            fd,
            zmtp_config,
            is_server,
            phase: ZmtpHandlerPhase::Initial, // Will transition in connection_ready
            greeting_buffer: BytesMut::with_capacity(GREETING_LENGTH), // Only for peer's greeting
            read_accumulator: BytesMut::with_capacity(8192 * 2),
            security_mechanism: None,
            data_cipher: None,
            zmtp_parser: ZmtpManualParser::new(),
            last_activity_time: Instant::now(),
            last_ping_sent_time: None,
            waiting_for_pong: false,
            heartbeat_ivl, // This is already Option<Duration>
            heartbeat_timeout_duration: heartbeat_timeout_val,
            outgoing_app_messages: VecDeque::new(),
            pending_outgoing_token: None,
            handshake_timeout: handshake_timeout_val,
            read_is_pending: false,
        }
    }

    // --- Helper: Frame Encoding (simplified, real one is in ZmtpCodec) ---
    fn zmtp_encode_msg_to_bytes(msg: &Msg) -> Result<Bytes, ZmqError> {
        // This is a simplified version. Ideally, use something like ZmtpCodec.
        // For now, let's assume a simple framing.
        let data = msg.data().unwrap_or(&[]);
        let size = data.len();
        let mut zmtp_flags_byte = 0u8;
        if msg.is_more() { zmtp_flags_byte |= 0b01; }
        if msg.is_command() { zmtp_flags_byte |= 0b100; } // ZMTP_FLAG_COMMAND is 0b100

        let mut frame_buf = BytesMut::new();
        if size <= 255 {
            frame_buf.reserve(1 + 1 + size);
            frame_buf.put_u8(zmtp_flags_byte);
            frame_buf.put_u8(size as u8);
        } else {
            zmtp_flags_byte |= 0b10; // ZMTP_FLAG_LONG
            frame_buf.reserve(1 + 8 + size);
            frame_buf.put_u8(zmtp_flags_byte);
            frame_buf.put_u64(size as u64);
        }
        frame_buf.put_slice(data);
        Ok(frame_buf.freeze())
    }

    // --- Helper: Request a read if one isn't already pending ---
    fn ensure_read_is_pending(&mut self, ops: &mut HandlerIoOps, interface: &UringWorkerInterface<'_>) {
        if !self.read_is_pending && matches!(self.phase, ZmtpHandlerPhase::Closed | ZmtpHandlerPhase::Error) == false {
            if interface.default_buffer_group_id().is_some() && self.zmtp_config.use_recv_multishot /* a config flag */ {
                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestRingRead);
            } else {
                // Fallback or if non-ring read is preferred for handshake/small data
                // For now, always assume RequestRingRead if available, or error if not.
                // A real implementation might have a `RequestNormalRead` blueprint.
                // For simplicity, if no ring read, this handler might struggle.
                // Let's assume RequestRingRead is the primary mechanism for this handler.
                if interface.default_buffer_group_id().is_some() {
                     ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestRingRead);
                } else {
                    // This is a config error: handler expects ring reads but no bgid.
                    // We should probably error out earlier if this is the case.
                    // For now, just log. The worker won't be able to submit this.
                    tracing::error!(fd = self.fd, "ZmtpUringHandler: Cannot request read, no default_bgid and no fallback read mechanism defined.");
                }
            }
            self.read_is_pending = true; // Mark that we've requested a read
        }
    }

    // --- Helper: Processing incoming data from read_accumulator ---
    // Returns Ok(true) if it made progress and might be able to process more from accumulator.
    // Returns Ok(false) if it needs more data from the network.
    // Returns Err(_) on fatal error.
    fn process_accumulated_data(&mut self, interface: &UringWorkerInterface<'_>, ops: &mut HandlerIoOps) -> Result<bool, ZmqError> {
        let mut made_progress_this_call = false;
        loop { // Loop to process as much as possible from read_accumulator
            let initial_accumulator_len = self.read_accumulator.len();
            match self.phase {
                ZmtpHandlerPhase::Initial => {
                    // Should have been transitioned by connection_ready
                    return Err(ZmqError::InvalidState("ZmtpHandler in Initial phase during data processing".into()));
                }
                ZmtpHandlerPhase::ClientSendGreeting | ZmtpHandlerPhase::ServerSendGreeting => {
                    // These phases are about *sending*. Receiving data here is unexpected until after send completes
                    // and we transition to a "WaitPeer" state.
                    // However, TCP is full duplex. Peer might send early.
                    // Let's assume for now that we strictly wait for our send to complete via
                    // `handle_internal_sqe_completion` before changing phase to expect data.
                    // If data arrives early, it stays in `read_accumulator`.
                    return Ok(made_progress_this_call); // No progress on read here, waiting for send ACK
                }

                ZmtpHandlerPhase::ServerWaitClientGreeting | ZmtpHandlerPhase::ClientWaitServerGreeting => {
                    if self.read_accumulator.len() < GREETING_LENGTH {
                        return Ok(made_progress_this_call); // Need more data for greeting
                    }
                    let peer_greeting_bytes = self.read_accumulator.split_to(GREETING_LENGTH);
                    match ZmtpGreeting::decode(&mut peer_greeting_bytes.clone()) { // Clone because decode takes &mut
                        Ok(Some(peer_greeting)) => {
                            made_progress_this_call = true;
                            tracing::debug!(fd = self.fd, role = if self.is_server {"S"} else {"C"}, ?peer_greeting, "Received peer greeting");
                            // Validate greeting roles
                            if self.is_server == peer_greeting.as_server {
                                tracing::error!(fd = self.fd, "Role mismatch in greeting exchange.");
                                self.phase = ZmtpHandlerPhase::Error;
                                ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                                return Err(ZmqError::SecurityError("Role mismatch".into()));
                            }

                            match self.security_mechanism.as_mut() {
                                Some(_) => { /* Already initialized, this is unexpected */ }
                                None => {
                                    self.security_mechanism = Some(
                                        negotiate_security_mechanism(
                                            self.is_server,
                                            &self.zmtp_config,
                                            &peer_greeting,
                                            self.fd as usize, // Using fd as a pseudo-handle for logging in negotiate
                                        )?
                                    );
                                    tracing::info!(fd=self.fd, mechanism=?self.security_mechanism.as_ref().unwrap().name(), "Negotiated security mechanism");
                                }
                            }


                            if self.is_server { // Currently ServerWaitClientGreeting
                                // Server has received client's greeting. Now server sends its own.
                                let mut greeting_to_send_buf = BytesMut::with_capacity(GREETING_LENGTH);
                                ZmtpGreeting::encode(
                                    NullMechanism::NAME_BYTES, // Placeholder, should use own proposed mechanism
                                    true,
                                    &mut greeting_to_send_buf,
                                );
                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: greeting_to_send_buf.freeze() });
                                self.phase = ZmtpHandlerPhase::ServerSendGreeting; // Phase indicating we've sent our greeting and will move to security.
                                                                                 // Actual transition to SecurityInitiate happens on Send completion.
                            } else { // Currently ClientWaitServerGreeting
                                // Client has received server's greeting. Move to security.
                                self.phase = ZmtpHandlerPhase::SecurityInitiate;
                                // `process_accumulated_data` will loop and hit SecurityInitiate case.
                            }
                        }
                        Ok(None) => { /* Should not happen if len >= GREETING_LENGTH */ return Ok(made_progress_this_call); }
                        Err(e) => {
                            tracing::error!(fd = self.fd, "Failed to decode peer greeting: {}", e);
                            self.phase = ZmtpHandlerPhase::Error;
                            ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                            return Err(e);
                        }
                    }
                }

                ZmtpHandlerPhase::SecurityInitiate => {
                    // This phase is a trigger to start the security exchange.
                    // The actual token production/processing happens in SecurityExchange.
                    made_progress_this_call = true; // Progress in terms of state change
                    self.phase = ZmtpHandlerPhase::SecurityExchange;

                    if let Some(sec_mech) = self.security_mechanism.as_mut() {
                        if let Some(initial_token) = sec_mech.produce_token()? {
                            self.pending_outgoing_token = Some(Bytes::from(initial_token));
                            // `prepare_sqes` will pick this up if `RequestSend` is not added here.
                            // Or add directly:
                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: self.pending_outgoing_token.take().unwrap() });
                        }
                    } else {
                        return Err(ZmqError::InvalidState("Security mechanism not initialized at SecurityInitiate".into()));
                    }
                    // Loop to potentially process buffered data if peer sent token fast
                }

                ZmtpHandlerPhase::SecurityExchange => {
                    if self.read_accumulator.is_empty() {
                        return Ok(made_progress_this_call); // Need more data for security token
                    }
                    // Assume security tokens are full ZMTP command messages.
                    // Use ZmtpManualParser to get one command message.
                    match self.zmtp_parser.decode_from_buffer(&mut self.read_accumulator) {
                        Ok(Some(token_msg)) => {
                            made_progress_this_call = true;
                            if !token_msg.is_command() {
                                tracing::error!(fd = self.fd, "Expected ZMTP COMMAND for security token, got data frame.");
                                self.phase = ZmtpHandlerPhase::Error;
                                ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                                return Err(ZmqError::SecurityError("Expected security command".into()));
                            }
                            let token_data = token_msg.data().unwrap_or_default();

                            if let Some(sec_mech) = self.security_mechanism.as_mut() {
                                sec_mech.process_token(token_data)?;
                                if let Some(response_token_vec) = sec_mech.produce_token()? {
                                    // Frame this token as a ZMTP command message
                                    let mut cmd_msg = Msg::from_vec(response_token_vec);
                                    cmd_msg.set_flags(MsgFlags::COMMAND);
                                    ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: Self::zmtp_encode_msg_to_bytes(&cmd_msg)? });
                                }

                                if sec_mech.is_complete() {
                                    tracing::info!(fd = self.fd, "Security handshake complete. Mechanism: {}", sec_mech.name());
                                    let (cipher, peer_id_from_mech) = sec_mech.as_any().downcast_ref::<NullMechanism>() // Example, need generic way or consume
                                        .ok_or(ZmqError::Internal("Failed to downcast security mechanism".into()))? // This is bad.
                                        // A better way: consume self.security_mechanism.take().unwrap().into_data_cipher_parts()
                                        // This requires `security_mechanism` to be `Option<Box<dyn Mechanism>>`.
                                        // Let's assume this happens AFTER loop, before transitioning phase.
                                        // For now, placeholder:
                                        .into_data_cipher_parts()?; // This is placeholder, need to consume
                                    self.data_cipher = Some(cipher);
                                    // self.peer_identity = peer_id_from_mech; // Store it

                                    self.phase = if self.is_server { ZmtpHandlerPhase::ReadyServerWaitClient } else { ZmtpHandlerPhase::ReadyClientSend };
                                    // If client, send READY now
                                    if !self.is_server {
                                        let client_ready_msg = ZmtpReady::create_msg(HashMap::new()); // Add Socket-Type, Identity
                                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{ data: Self::zmtp_encode_msg_to_bytes(&client_ready_msg)? });
                                    }
                                } else if sec_mech.is_error() {
                                    self.phase = ZmtpHandlerPhase::Error;
                                    ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                                    return Err(ZmqError::SecurityError(sec_mech.error_reason().unwrap_or("Unknown security error").into()));
                                }
                            } else {
                                return Err(ZmqError::InvalidState("Security mechanism not available".into()));
                            }
                        }
                        Ok(None) => return Ok(made_progress_this_call), // Need more data for full token message
                        Err(e) => {
                            self.phase = ZmtpHandlerPhase::Error;
                            ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                            return Err(e);
                        }
                    }
                }
                // TODO: Implement ReadyCommandExchange, DataPhase
                ZmtpHandlerPhase::ReadyClientSend | ZmtpHandlerPhase::ReadyServerWaitClient | ZmtpHandlerPhase::ReadyServerSend | ZmtpHandlerPhase::ReadyClientWaitServer => {
                    // Similar to SecurityExchange, parse READY command, transition phase, send own READY.
                    // On final READY received/sent, transition to DataPhase.
                    // This part is complex due to ZMTP properties in READY.
                    tracing::warn!(fd=self.fd, phase=?self.phase, "READY command exchange not fully implemented in process_accumulated_data");
                    // For now, just consume data to avoid infinite loop if parser can't handle it
                    if !self.read_accumulator.is_empty() { self.read_accumulator.clear(); made_progress_this_call=true; }

                    // Placeholder: Assume READY exchange is instant for now
                    if self.phase != ZmtpHandlerPhase::DataPhase { // Avoid re-triggering if already done
                        tracing::info!(fd=self.fd, "ZmtpUringHandler: Placeholder - Transitioning to DataPhase from READY exchange.");
                        self.phase = ZmtpHandlerPhase::DataPhase;
                        // Send ZMTP_READY to application logic (SocketCore)
                        if let Err(e) = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(ZmqError::Internal("ZMTP_HANDSHAKE_COMPLETE_SIGNAL".into())))) { // Special signal
                             tracing::error!(fd=self.fd, "Failed to send ZMTP_HANDSHAKE_COMPLETE signal upstream: {:?}", e);
                             // This could be a critical error.
                        }
                    }
                }

                ZmtpHandlerPhase::DataPhase => {
                    if self.read_accumulator.is_empty() { return Ok(made_progress_this_call); }

                    let mut data_to_parse = self.read_accumulator.split(); // Take all data

                    if let Some(cipher) = self.data_cipher.as_mut() {
                        match cipher.decrypt_wire_data_to_zmtp_frame(&mut data_to_parse) { // data_to_parse becomes ciphertext source
                            Ok(Some(plaintext_frame)) => {
                                // ZmtpManualParser expects to be fed raw ZMTP frames one by one.
                                // The IDataCipher for Noise already returns one full ZMTP frame.
                                let mut single_frame_buf = BytesMut::from(plaintext_frame.as_ref());
                                match self.zmtp_parser.decode_from_buffer(&mut single_frame_buf) {
                                    Ok(Some(msg)) => {
                                        made_progress_this_call = true;
                                        // Handle msg (PING/PONG/Data)
                                        if msg.is_command() { /* handle PING/PONG */ }
                                        else if let Err(e) = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Ok(msg))) {
                                            tracing::error!(fd=self.fd, "Failed to send ZMTP data msg upstream: {:?}", e);
                                        }
                                    }
                                    Ok(None) => { /* Incomplete frame from cipher? Should not happen if cipher returns full frames */ }
                                    Err(e) => return Err(e),
                                }
                                // If data_to_parse (from decrypt) had remaining undecrypted data, put it back.
                                if !data_to_parse.is_empty() { self.read_accumulator.put_front(data_to_parse); }

                            }
                            Ok(None) => { // Not enough data for full encrypted message
                                self.read_accumulator.put_front(data_to_parse); // Put back unconsumed
                                return Ok(made_progress_this_call);
                            }
                            Err(e) => return Err(e),
                        }
                    } else { // No cipher (e.g., NULL/PLAIN)
                        match self.zmtp_parser.decode_from_buffer(&mut data_to_parse) {
                            Ok(Some(msg)) => {
                                made_progress_this_call = true;
                                if msg.is_command() { /* handle PING/PONG */ }
                                else if let Err(e) = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Ok(msg))) {
                                    tracing::error!(fd=self.fd, "Failed to send ZMTP data msg upstream: {:?}", e);
                                }
                            }
                            Ok(None) => { /* Need more data for ZMTP frame */ }
                            Err(e) => return Err(e),
                        }
                        // Put back any remaining unparsed data from data_to_parse
                        if !data_to_parse.is_empty() { self.read_accumulator.put_front(data_to_parse); }
                    }
                }

                ZmtpHandlerPhase::Error | ZmtpHandlerPhase::Closed => return Ok(made_progress_this_call), // No more processing
            }
            // If accumulator was modified, loop again. Otherwise, break.
            if self.read_accumulator.len() == initial_accumulator_len && !made_progress_this_call {
                break; // No change in accumulator, no progress made, must need more network data
            }
            // If progress was made, reset for next iteration of loop
            made_progress_this_call = false;
        }
        Ok(true) // Reached here means we looped and potentially made progress or consumed all.
    }
}


impl UringConnectionHandler for ZmtpUringHandler {
    fn fd(&self) -> RawFd { self.fd }

    fn connection_ready(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        tracing::info!(fd = self.fd, role = if self.is_server {"S"} else {"C"}, "ZmtpUringHandler: connection_ready called.");
        self.last_activity_time = Instant::now();
        let mut ops = HandlerIoOps::new();

        if self.is_server {
            self.phase = ZmtpHandlerPhase::ServerWaitClientGreeting;
            // Server waits for client's greeting first.
            self.ensure_read_is_pending(&mut ops, interface);
        } else {
            // Client sends its greeting first.
            let mut greeting_buf = BytesMut::with_capacity(GREETING_LENGTH);
            // TODO: Propose correct mechanism based on self.zmtp_config
            let proposed_mechanism_bytes =
                if self.zmtp_config.use_noise_xx && cfg!(feature="noise_xx") {
                    security::NoiseXxMechanism::NAME_BYTES
                } else if self.zmtp_config.use_plain {
                    security::PlainMechanism::NAME_BYTES
                } else {
                    NullMechanism::NAME_BYTES
                };

            ZmtpGreeting::encode(
                proposed_mechanism_bytes,
                false, // as_server = false for client
                &mut greeting_buf,
            );
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: greeting_buf.freeze() });
            self.phase = ZmtpHandlerPhase::ClientSendGreeting; // We are in process of sending
            // We'll request a read after this send completes, in handle_internal_sqe_completion.
        }
        ops
    }

    fn process_ring_read_data(
        &mut self,
        buffer_slice: &[u8],
        _buffer_id: u16, // We don't use buffer_id directly here, just the data
        interface: &UringWorkerInterface<'_>,
    ) -> HandlerIoOps {
        tracing::trace!(fd = self.fd, len = buffer_slice.len(), phase = ?self.phase, "ZmtpUringHandler: process_ring_read_data");
        self.last_activity_time = Instant::now();
        self.read_is_pending = false; // Current read completed
        let mut ops = HandlerIoOps::new();

        if buffer_slice.is_empty() && self.phase != ZmtpHandlerPhase::Closed && self.phase != ZmtpHandlerPhase::Error {
            // EOF from peer
            tracing::info!(fd = self.fd, "Peer closed connection (EOF). Current phase: {:?}", self.phase);
            self.phase = ZmtpHandlerPhase::Error; // Or a specific EOF/ClosedByPeer phase
            ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
            // Send error upstream if handshake wasn't complete
            if !matches!(self.phase, ZmtpHandlerPhase::DataPhase) {
                 let _ = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(ZmqError::ConnectionClosed)));
            }
            return ops;
        }

        self.read_accumulator.put_slice(buffer_slice);

        match self.process_accumulated_data(interface, &mut ops) {
            Ok(_) => { /* Potentially made progress, ops updated */ }
            Err(e) => {
                tracing::error!(fd = self.fd, error = %e, "Error processing accumulated data. Closing connection.");
                self.phase = ZmtpHandlerPhase::Error;
                ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                 let _ = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(e)));
            }
        }

        self.ensure_read_is_pending(&mut ops, interface);
        ops
    }

    fn handle_internal_sqe_completion(
        &mut self,
        _sqe_user_data: UserData, // UserData of the SQE that completed (e.g. for a specific Send)
        cqe_result: i32,
        _cqe_flags: u32,
        interface: &UringWorkerInterface<'_>,
    ) -> HandlerIoOps {
        tracing::trace!(fd = self.fd, cqe_res = cqe_result, phase = ?self.phase, "ZmtpUringHandler: handle_internal_sqe_completion");
        self.last_activity_time = Instant::now();
        let mut ops = HandlerIoOps::new();

        if cqe_result < 0 {
            let io_err = std::io::Error::from_raw_os_error(-cqe_result);
            tracing::error!(fd = self.fd, error = %io_err, "SQE completion error (e.g. Send failed). Closing.");
            self.phase = ZmtpHandlerPhase::Error;
            ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
            let _ = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(ZmqError::from(io_err))));
            return ops;
        }

        // Successful completion (typically a Send)
        match self.phase {
            ZmtpHandlerPhase::ClientSendGreeting => {
                // Client's greeting successfully sent. Now wait for server's greeting.
                self.phase = ZmtpHandlerPhase::ClientWaitServerGreeting;
                self.ensure_read_is_pending(&mut ops, interface);
            }
            ZmtpHandlerPhase::ServerSendGreeting => {
                // Server's greeting successfully sent. Move to security exchange.
                self.phase = ZmtpHandlerPhase::SecurityInitiate;
                // Try to process any buffered data which might be client's first security token
                match self.process_accumulated_data(interface, &mut ops) {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(fd=self.fd, "Error processing post ServerSendGreeting: {}", e);
                        self.phase = ZmtpHandlerPhase::Error;
                        ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                        let _ = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(e)));
                    }
                }
                self.ensure_read_is_pending(&mut ops, interface);
            }
            ZmtpHandlerPhase::SecurityExchange => {
                // A security token was successfully sent. Peer's response will come via read.
                // If we had a pending outgoing token that was just sent, clear it.
                self.pending_outgoing_token = None;
                // Ensure we are reading for the next token from peer.
                self.ensure_read_is_pending(&mut ops, interface);
            }
            ZmtpHandlerPhase::ReadyClientSend => {
                // Client's READY command sent. Now wait for server's READY.
                self.phase = ZmtpHandlerPhase::ReadyClientWaitServer;
                self.ensure_read_is_pending(&mut ops, interface);
            }
            ZmtpHandlerPhase::ReadyServerSend => {
                // Server's READY command sent. Handshake is fully complete.
                self.phase = ZmtpHandlerPhase::DataPhase;
                tracing::info!(fd=self.fd, "ZmtpUringHandler: Server completed READY send. Transitioning to DataPhase.");
                // Signal upstream that ZMTP handshake is fully complete
                // This special "Internal" error is a signal.
                if let Err(e) = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(ZmqError::Internal("ZMTP_HANDSHAKE_COMPLETE_SIGNAL".into())))) {
                     tracing::error!(fd=self.fd, "Failed to send ZMTP_HANDSHAKE_COMPLETE signal upstream: {:?}", e);
                }
                self.ensure_read_is_pending(&mut ops, interface);
            }
            ZmtpHandlerPhase::DataPhase => {
                // A data message (or PING/PONG) was successfully sent.
                // If outgoing_app_messages has more, prepare_sqes will try to send them.
                // Or, if this send was from outgoing_app_messages, try to send next one now.
                if !self.outgoing_app_messages.is_empty() {
                    if let Some(next_msg) = self.outgoing_app_messages.pop_front() {
                         match Self::zmtp_encode_msg_to_bytes(&next_msg) {
                             Ok(bytes_to_send) => {
                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{data: bytes_to_send });
                             }
                             Err(e) => {
                                 tracing::error!(fd=self.fd, "Failed to encode next outgoing msg: {}", e);
                                 // Potentially close or signal error for this message
                             }
                         }
                    }
                }
                self.ensure_read_is_pending(&mut ops, interface);
            }
            _ => {
                tracing::warn!(fd = self.fd, phase = ?self.phase, "Send completion in unexpected phase.");
                self.ensure_read_is_pending(&mut ops, interface);
            }
        }
        ops
    }

    fn prepare_sqes(&mut self, interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        self.ensure_read_is_pending(&mut ops, interface); // Always ensure read is active if possible

        // Try to send any pending outgoing security token (if process_token prepared one)
        if let Some(token_bytes) = self.pending_outgoing_token.take() {
            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: token_bytes });
            // Don't do heartbeats or app messages if handshake token is pending
            return ops;
        }

        // Try to send from app message queue if in DataPhase
        if self.phase == ZmtpHandlerPhase::DataPhase {
            if let Some(msg_to_send) = self.outgoing_app_messages.pop_front() {
                match Self::zmtp_encode_msg_to_bytes(&msg_to_send) { // Assuming direct framing here
                    Ok(bytes_to_send) => {
                        // TODO: Add encryption step if self.data_cipher is Some
                        ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: bytes_to_send });
                        // If this queue is drained, a completion will trigger sending the next.
                        // If SQ is full, this blueprint might be dropped by worker.
                        // `handle_outgoing_app_data` should handle this by re-queuing.
                    }
                    Err(e) => {
                        tracing::error!(fd=self.fd, "Failed to ZMTP encode outgoing app message: {}", e);
                        // TODO: What to do with this message? Error upstream? For now, it's dropped.
                    }
                }
            }
        }

        // Heartbeat logic (only if in DataPhase and no other critical sends pending)
        if self.phase == ZmtpHandlerPhase::DataPhase && ops.sqe_blueprints.is_empty() {
            let now = Instant::now();
            if self.waiting_for_pong {
                if let Some(ping_sent_at) = self.last_ping_sent_time {
                    if now.duration_since(ping_sent_at) > self.heartbeat_timeout_duration {
                        tracing::warn!(fd = self.fd, "PONG timeout. Closing connection.");
                        self.phase = ZmtpHandlerPhase::Error;
                        ops.set_error_close().sqe_blueprints.push(HandlerSqeBlueprint::RequestClose);
                        let _ = interface.worker_io_config.parsed_msg_tx_zmtp.try_send((self.fd, Err(ZmqError::Timeout)));
                        return ops; // Error, no further ops
                    }
                }
            } else if let Some(ivl) = self.heartbeat_ivl {
                if now.duration_since(self.last_activity_time) >= ivl {
                    tracing::debug!(fd = self.fd, "Heartbeat interval. Sending PING.");
                    let ping_msg = ZmtpCommand::create_ping(0, b"heartbeat"); // TTL and Context
                    match Self::zmtp_encode_msg_to_bytes(&ping_msg) {
                        Ok(ping_bytes) => {
                             // TODO: Add encryption step if self.data_cipher is Some
                            ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend { data: ping_bytes });
                            // State update (waiting_for_pong, last_ping_sent_time) should happen
                            // in handle_internal_sqe_completion for this PING send.
                        }
                        Err(e) => tracing::error!(fd=self.fd, "Failed to encode PING: {}", e),
                    }
                }
            }
        }
        ops
    }

    fn handle_outgoing_app_data(
        &mut self,
        data: Arc<dyn Any + Send + Sync>,
        _interface: &UringWorkerInterface<'_>
    ) -> HandlerIoOps {
        let mut ops = HandlerIoOps::new();
        match data.downcast_arc::<Msg>() {
            Ok(msg_arc) => {
                let msg = (*msg_arc).clone(); // Get owned Msg
                if self.phase == ZmtpHandlerPhase::DataPhase {
                    // Try to send immediately if queue is empty
                    if self.outgoing_app_messages.is_empty() {
                         match Self::zmtp_encode_msg_to_bytes(&msg) { // Assuming direct framing
                            Ok(bytes_to_send) => {
                                // TODO: Encryption step
                                ops.sqe_blueprints.push(HandlerSqeBlueprint::RequestSend{ data: bytes_to_send });
                            }
                            Err(e) => {
                                tracing::error!(fd=self.fd, "Failed to ZMTP encode outgoing app message for immediate send: {}", e);
                                self.outgoing_app_messages.push_back(msg); // Queue if encoding failed for direct send
                            }
                        }
                    } else {
                        self.outgoing_app_messages.push_back(msg);
                    }
                } else {
                    // TODO: Handle REQ/REP patterns where sending might not be allowed in current phase
                    tracing::warn!(fd = self.fd, phase = ?self.phase, "Received app data in non-DataPhase. Queuing.");
                    self.outgoing_app_messages.push_back(msg);
                }
            }
            Err(_) => {
                tracing::error!(fd = self.fd, "ZmtpUringHandler received non-Msg app data. Ignoring.");
            }
        }
        ops
    }

    fn close_initiated(&mut self, _interface: &UringWorkerInterface<'_>) -> HandlerIoOps {
        tracing::info!(fd = self.fd, "ZmtpUringHandler: close_initiated called.");
        self.phase = ZmtpHandlerPhase::Closed;
        self.outgoing_app_messages.clear();
        // If there was a pending read, the worker might complete it with an error if pipe is closed.
        // The handler itself doesn't need to cancel reads.
        HandlerIoOps::new().add_blueprint(HandlerSqeBlueprint::RequestClose)
    }

    fn fd_has_been_closed(&mut self) {
        tracing::info!(fd = self.fd, "ZmtpUringHandler: fd_has_been_closed notification received.");
        self.phase = ZmtpHandlerPhase::Closed; // Ensure terminal state
    }
}

// Factory for ZmtpUringHandler
pub struct ZmtpHandlerFactory {
}

impl ProtocolHandlerFactory for ZmtpHandlerFactory {
    fn id(&self) -> &'static str { "zmtp-uring/3.1" } // Or whatever ID you use

    fn create_handler(
        &self,
        fd: RawFd,
        _worker_io_config: Arc<WorkerIoConfig>, // Not directly used by ZmtpUringHandler::new
        protocol_config: &ProtocolConfig,     // Expects a reference to the enum
        is_server_role: bool,
    ) -> Result<Box<dyn UringConnectionHandler + Send>, String> {
        match protocol_config {
            ProtocolConfig::Zmtp(engine_config_arc) => {
                // engine_config_arc is Arc<ZmtpEngineConfig>
                Ok(Box::new(ZmtpUringHandler::new(
                    fd,
                    engine_config_arc.clone(), // Clone the Arc for the handler
                    is_server_role,
                )))
            }
            // Add matches for other config types if ZmtpHandlerFactory could somehow handle them,
            // or more likely, this factory would only be called if the ID matches ZMTP.
            // So, a mismatch here would be an internal logic error in HandlerManager.
            #[allow(unreachable_patterns)] // If only Zmtp variant exists for now
            _ => Err(format!(
                "ZmtpHandlerFactory (id: '{}') received an incompatible ProtocolConfig variant: {:?}",
                self.id(),
                protocol_config
            )),
        }
    }
}