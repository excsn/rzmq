use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch};
use crate::security::framer::{ISecureFramer, NullFramer};
use crate::security::mechanism::Mechanism;
use crate::security::null::NullMechanism;
use crate::socket::options::ZmtpEngineConfig;
use super::actions::{AppAction, EngineOutput, NetAction};
use super::command::{ZmtpCommand, ZmtpReady};
use super::greeting::{ZmtpGreeting, GREETING_LENGTH, MECHANISM_LENGTH};
use super::ZmtpCodec;

use bytes::{Bytes, BytesMut};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::codec::Encoder;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZmtpPhase {
    Greeting,
    Security,
    Ready,
    Data,
    Closed,
}

pub struct ZmtpEngine {
    config: Arc<ZmtpEngineConfig>,
    is_server: bool,
    pub phase: ZmtpPhase,

    network_read_accumulator: BytesMut,

    security_mechanism: Box<dyn Mechanism>,
    framer: Box<dyn ISecureFramer>,
    // Framer derived from the completed security mechanism, held here until the
    // READY exchange finishes (READY is sent/received plain, not encrypted).
    pending_framer: Option<Box<dyn ISecureFramer>>,

    last_activity_time: Instant,
    last_ping_sent_time: Option<Instant>,
    waiting_for_pong: bool,

    // Holds frames of an in-progress multipart message across on_network_bytes() calls.
    partial_batch: FrameBatch,
}

impl ZmtpEngine {
    pub fn new(is_server: bool, config: Arc<ZmtpEngineConfig>) -> Self {
        let max_msg_size = config.max_msg_size;
        Self {
            config,
            is_server,
            phase: ZmtpPhase::Greeting,
            network_read_accumulator: BytesMut::with_capacity(8192),
            security_mechanism: Box::new(NullMechanism),
            framer: Box::new(NullFramer::new(max_msg_size)),
            pending_framer: None,
            last_activity_time: Instant::now(),
            last_ping_sent_time: None,
            waiting_for_pong: false,
            partial_batch: FrameBatch::new(),
        }
    }

    // --- Public accessor API for the Tokio actor ---

    pub fn is_waiting_for_pong(&self) -> bool { self.waiting_for_pong }

    pub fn get_pong_deadline(&self) -> Option<Instant> {
        if self.waiting_for_pong {
            self.last_ping_sent_time.map(|t| {
                t + self.config.heartbeat_timeout.unwrap_or(Duration::from_secs(30))
            })
        } else {
            None
        }
    }

    pub fn record_activity(&mut self) { self.last_activity_time = Instant::now(); }

    pub fn buffer_len(&self) -> usize { self.network_read_accumulator.len() }

    pub fn config(&self) -> &Arc<ZmtpEngineConfig> { &self.config }

    /// Encode a batch of outgoing logical messages to a single wire `Bytes` (standard path).
    pub fn frame_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
        self.framer.write_msg_batch(batch)
    }

    /// Encode a batch for io-uring vectored write, returning multiple `Bytes` slices.
    pub fn frame_batch_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
        let total: usize = batch.iter().flat_map(|g| g.iter().map(|m| m.size())).sum();
        if total < self.config.sndbatch_bytes {
            Ok(vec![self.framer.write_msg_batch(batch)?])
        } else {
            self.framer.frame_vectored(batch)
        }
    }

    /// Encode a single logical multipart message (FrameBatch) to wire bytes.
    pub fn frame_msgs(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError> {
        self.framer.write_msg_multipart(msgs)
    }

    pub fn start(&mut self) -> EngineOutput {
        let mut out = EngineOutput::new();
        let is_server = self.is_server;
        let greeting_bytes = encode_greeting(local_mechanism_name_bytes(&self.config), is_server);
        out.net_actions.push(NetAction::Send { data: greeting_bytes, zc_eligible: false });
        out
    }

    pub fn on_network_bytes(&mut self, data: Bytes) -> EngineOutput {
        self.network_read_accumulator.extend_from_slice(&data);
        let mut out = EngineOutput::new();

        match self.phase {
            ZmtpPhase::Greeting => self.process_greeting(&mut out),
            ZmtpPhase::Security => self.process_security(&mut out),
            ZmtpPhase::Ready => self.process_ready(&mut out),
            ZmtpPhase::Data => self.process_data(&mut out),
            ZmtpPhase::Closed => {}
        }

        out
    }

    pub fn on_app_message(&mut self, msgs: FrameBatch) -> EngineOutput {
        if self.phase != ZmtpPhase::Data {
            return EngineOutput::new();
        }
        let mut out = EngineOutput::new();
        match self.framer.write_msg_multipart(msgs) {
            Ok(data) => {
                let zc = self.config.use_send_zerocopy;
                out.net_actions.push(NetAction::Send { data, zc_eligible: zc });
            }
            Err(e) => {
                out.app_actions.push(AppAction::PeerError(e));
            }
        }
        out
    }

    pub fn on_tick(&mut self, now: Instant) -> EngineOutput {
        let mut out = EngineOutput::new();
        if self.phase != ZmtpPhase::Data {
            return out;
        }

        if let Some(timeout) = self.config.heartbeat_timeout {
            if self.waiting_for_pong {
                if let Some(ping_time) = self.last_ping_sent_time {
                    if now.duration_since(ping_time) >= timeout {
                        self.phase = ZmtpPhase::Closed;
                        out.app_actions.push(AppAction::PeerError(ZmqError::Timeout));
                        return out;
                    }
                }
            }
        }

        if let Some(ivl) = self.config.heartbeat_ivl {
            if !self.waiting_for_pong && now.duration_since(self.last_activity_time) >= ivl {
                let ttl_ms = self.config.heartbeat_timeout
                    .map(|d| d.as_millis().min(u16::MAX as u128) as u16)
                    .unwrap_or(0);
                let ping_msg = ZmtpCommand::create_ping(ttl_ms, &[]);
                match encode_msg(ping_msg) {
                    Ok(data) => {
                        out.net_actions.push(NetAction::Send { data, zc_eligible: false });
                        self.waiting_for_pong = true;
                        self.last_ping_sent_time = Some(now);
                    }
                    Err(e) => out.app_actions.push(AppAction::PeerError(e)),
                }
            }
        }

        out
    }

    pub fn close(&mut self) -> EngineOutput {
        self.phase = ZmtpPhase::Closed;
        let delay = if self.config.use_cork {
            Some(Duration::from_millis(500))
        } else {
            None
        };
        EngineOutput::new()
            .with_net(NetAction::SetCork(false))
            .with_net(NetAction::ScheduleClose(delay))
    }

    // --- Private phase handlers ---

    fn process_greeting(&mut self, out: &mut EngineOutput) {
        if self.network_read_accumulator.len() < GREETING_LENGTH {
            return;
        }

        let peer_greeting = match ZmtpGreeting::decode(&mut self.network_read_accumulator) {
            Ok(Some(g)) => g,
            Ok(None) => return,
            Err(e) => {
                self.phase = ZmtpPhase::Closed;
                out.app_actions.push(AppAction::PeerError(e));
                return;
            }
        };

        let mechanism = match crate::security::negotiate_security_mechanism(
            self.is_server,
            &self.config,
            &peer_greeting,
            0,
        ) {
            Ok(m) => m,
            Err(e) => {
                self.phase = ZmtpPhase::Closed;
                out.app_actions.push(AppAction::PeerError(e));
                return;
            }
        };
        self.security_mechanism = mechanism;

        if self.security_mechanism.is_complete() {
            match self.derive_pending_framer() {
                Ok(f) => self.pending_framer = Some(f),
                Err(e) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(e));
                    return;
                }
            }
            self.phase = ZmtpPhase::Ready;
            // Client sends its READY immediately upon entering the Ready phase.
            if !self.is_server {
                self.emit_local_ready(out);
            }
            // Drain any bytes that arrived in the same read as the greeting (e.g. server READY).
            if !self.network_read_accumulator.is_empty() {
                self.process_ready(out);
            }
        } else {
            self.phase = ZmtpPhase::Security;
            self.process_security(out);
        }
    }

    fn process_security(&mut self, out: &mut EngineOutput) {
        // Produce any initial token the mechanism wants to send before receiving
        // anything from the peer. Client-initiated mechanisms (PLAIN, CURVE,
        // Noise_XX) send their first message here; server-side mechanisms return
        // None and fall straight through to the read loop below.
        if !self.emit_security_token(out) {
            return;
        }
        if self.check_security_complete(out) {
            return;
        }

        loop {
            let msg = match self.framer.try_read_msg(&mut self.network_read_accumulator) {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(e));
                    return;
                }
            };

            let token = msg.data().unwrap_or(&[]);
            match self.security_mechanism.process_token(token) {
                Ok(_) => {}
                Err(e) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(e));
                    return;
                }
            }

            if self.security_mechanism.is_error() {
                let reason = self.security_mechanism.error_reason().unwrap_or("unknown").to_owned();
                self.phase = ZmtpPhase::Closed;
                out.app_actions.push(AppAction::PeerError(ZmqError::SecurityError(reason)));
                return;
            }

            if !self.emit_security_token(out) {
                return;
            }
            if self.check_security_complete(out) {
                return;
            }
        }
    }

    /// Calls `produce_token()` and, if a token is ready, encodes and enqueues it.
    /// Returns `false` if a fatal error was emitted (caller should return early).
    fn emit_security_token(&mut self, out: &mut EngineOutput) -> bool {
        match self.security_mechanism.produce_token() {
            Ok(Some(token_bytes)) => {
                use crate::{Msg, MsgFlags};
                let mut token_msg = Msg::from_vec(token_bytes);
                token_msg.set_flags(MsgFlags::COMMAND);
                match encode_msg(token_msg) {
                    Ok(data) => out.net_actions.push(NetAction::Send { data, zc_eligible: false }),
                    Err(e) => {
                        self.phase = ZmtpPhase::Closed;
                        out.app_actions.push(AppAction::PeerError(e));
                        return false;
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                self.phase = ZmtpPhase::Closed;
                out.app_actions.push(AppAction::PeerError(e));
                return false;
            }
        }
        true
    }

    /// Checks whether the mechanism is complete and, if so, transitions to the
    /// Ready phase. Returns `true` if the security exchange is done (caller
    /// should return early from the security loop).
    fn check_security_complete(&mut self, out: &mut EngineOutput) -> bool {
        if !self.security_mechanism.is_complete() {
            return false;
        }
        // Derive the data-phase framer now but hold it in pending_framer.
        // READY frames are exchanged in plain (same as security tokens),
        // so we only activate the encrypted framer after READY completes.
        match self.derive_pending_framer() {
            Ok(f) => self.pending_framer = Some(f),
            Err(e) => {
                self.phase = ZmtpPhase::Closed;
                out.app_actions.push(AppAction::PeerError(e));
                return true;
            }
        }
        self.phase = ZmtpPhase::Ready;
        if !self.is_server {
            self.emit_local_ready(out);
        }
        if !self.network_read_accumulator.is_empty() {
            self.process_ready(out);
        }
        true
    }

    fn process_ready(&mut self, out: &mut EngineOutput) {
        loop {
            // READY is exchanged in plain (self.framer is still NullFramer here;
            // the encrypted framer lives in pending_framer until we transition to Data).
            let msg = match self.framer.try_read_msg(&mut self.network_read_accumulator) {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(e));
                    return;
                }
            };

            let ready_cmd = match ZmtpCommand::parse(&msg) {
                Some(ZmtpCommand::Ready(r)) => r,
                Some(ZmtpCommand::Error) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(
                        ZmqError::ProtocolViolation("Peer sent ERROR during Ready exchange".into()),
                    ));
                    return;
                }
                _ => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(
                        ZmqError::ProtocolViolation("Expected READY command".into()),
                    ));
                    return;
                }
            };

            let peer_socket_type = ready_cmd.properties.get("Socket-Type")
                .map(|v| String::from_utf8_lossy(v).into_owned());
            let peer_identity = ready_cmd.properties.get("Identity")
                .map(|v| Blob::from(v.clone()));

            if self.is_server {
                // Server received client READY → send server READY then complete.
                self.emit_local_ready(out);
            }

            // Activate the encrypted framer now that the plain READY exchange is done.
            self.activate_pending_framer();
            self.phase = ZmtpPhase::Data;
            self.last_activity_time = Instant::now();

            // Enable TCP_CORK for throughput-optimised socket types on transition to Data.
            if self.config.use_cork
                && matches!(
                    self.config.socket_type_name.as_str(),
                    "PUSH" | "PULL" | "PUB" | "SUB"
                )
            {
                out.net_actions.push(NetAction::SetCork(true));
            }

            out.app_actions.push(AppAction::HandshakeComplete { peer_identity, peer_socket_type });

            // Drain any remaining accumulated bytes into the Data phase handler.
            if !self.network_read_accumulator.is_empty() {
                self.process_data(out);
            }
            return;
        }
    }

    fn process_data(&mut self, out: &mut EngineOutput) {
        loop {
            let msg = match self.framer.try_read_msg(&mut self.network_read_accumulator) {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    self.phase = ZmtpPhase::Closed;
                    out.app_actions.push(AppAction::PeerError(e));
                    return;
                }
            };

            self.last_activity_time = Instant::now();

            if msg.is_command() {
                match ZmtpCommand::parse(&msg) {
                    Some(ZmtpCommand::Ping(ctx)) => {
                        let pong = ZmtpCommand::create_pong(&ctx);
                        match encode_msg(pong) {
                            Ok(data) => out.net_actions.push(NetAction::Send { data, zc_eligible: false }),
                            Err(e) => out.app_actions.push(AppAction::PeerError(e)),
                        }
                    }
                    Some(ZmtpCommand::Pong(_)) => {
                        self.waiting_for_pong = false;
                    }
                    Some(ZmtpCommand::Error) => {
                        self.phase = ZmtpPhase::Closed;
                        out.app_actions.push(AppAction::PeerError(
                            ZmqError::ProtocolViolation("Peer sent ERROR command".into()),
                        ));
                        return;
                    }
                    _ => {}
                }
                continue;
            }

            let is_more = msg.is_more();
            self.partial_batch.push(msg);
            if !is_more {
                let batch = std::mem::replace(&mut self.partial_batch, SmallVec::new());
                out.app_actions.push(AppAction::DeliverMessage(batch));
            }
        }
    }

    // --- Helpers ---

    fn derive_pending_framer(&mut self) -> Result<Box<dyn ISecureFramer>, ZmqError> {
        let old = std::mem::replace(&mut self.security_mechanism, Box::new(NullMechanism));
        let (new_framer, _peer_id_bytes) = old.into_framer(self.config.max_msg_size)?;
        Ok(new_framer)
    }

    fn activate_pending_framer(&mut self) {
        if let Some(f) = self.pending_framer.take() {
            self.framer = f;
        }
    }

    fn emit_local_ready(&self, out: &mut EngineOutput) {
        let props = build_local_ready_props(&self.config);
        let ready_msg = ZmtpReady::create_msg(props);
        match encode_msg(ready_msg) {
            Ok(data) => out.net_actions.push(NetAction::Send { data, zc_eligible: false }),
            Err(e) => out.app_actions.push(AppAction::PeerError(e)),
        }
    }
}

// --- Module-level helpers ---

fn local_mechanism_name_bytes(config: &ZmtpEngineConfig) -> &'static [u8; MECHANISM_LENGTH] {
    #[cfg(feature = "plain")]
    if config.use_plain {
        return crate::security::PlainMechanism::NAME_BYTES;
    }
    #[cfg(feature = "curve")]
    if config.use_curve {
        return crate::security::CurveMechanism::NAME_BYTES;
    }
    #[cfg(feature = "noise_xx")]
    if config.use_noise_xx {
        return crate::security::NoiseXxMechanism::NAME_BYTES;
    }
    NullMechanism::NAME_BYTES
}

fn encode_greeting(mechanism: &[u8; MECHANISM_LENGTH], as_server: bool) -> Bytes {
    let mut buf = BytesMut::with_capacity(GREETING_LENGTH);
    ZmtpGreeting::encode(mechanism, as_server, &mut buf);
    buf.freeze()
}

fn encode_msg(msg: crate::Msg) -> Result<Bytes, ZmqError> {
    let mut codec = ZmtpCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf)
        .map_err(|e| ZmqError::Internal(e.to_string()))?;
    Ok(buf.freeze())
}

fn build_local_ready_props(config: &ZmtpEngineConfig) -> HashMap<String, Vec<u8>> {
    let mut props = HashMap::new();
    props.insert("Socket-Type".to_string(), config.socket_type_name.as_bytes().to_vec());
    if let Some(ref rid) = config.routing_id {
        if !rid.is_empty() {
            props.insert("Identity".to_string(), rid.as_ref().to_vec());
        }
    }
    props
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::zmtp::greeting::ZmtpGreeting;
    use crate::protocol::zmtp::command::ZmtpReady;
    use crate::protocol::zmtp::GREETING_LENGTH;

    /// Collect all `NetAction::Send` bytes from an `EngineOutput` into a single `Bytes`.
    fn collect_sends(out: &EngineOutput) -> Bytes {
        let mut buf = BytesMut::new();
        for action in &out.net_actions {
            if let NetAction::Send { data, .. } = action {
                buf.extend_from_slice(data);
            }
        }
        buf.freeze()
    }

    fn has_handshake_complete(out: &EngineOutput) -> bool {
        out.app_actions.iter().any(|a| matches!(a, AppAction::HandshakeComplete { .. }))
    }

    /// Drive two engines against each other until both reach Data phase.
    /// Asserts the exact message sequence for the NULL mechanism.
    #[test]
    fn test_null_handshake_full_exchange() {
        let cfg = Arc::new(ZmtpEngineConfig::default());
        let mut client = ZmtpEngine::new(false, cfg.clone());
        let mut server = ZmtpEngine::new(true, cfg.clone());

        // Step 1: both send greetings simultaneously.
        let client_start = client.start();
        let server_start = server.start();

        let client_greeting = collect_sends(&client_start);
        let server_greeting = collect_sends(&server_start);
        assert_eq!(client_greeting.len(), GREETING_LENGTH, "client greeting must be 64 bytes");
        assert_eq!(server_greeting.len(), GREETING_LENGTH, "server greeting must be 64 bytes");
        assert_eq!(client.phase, ZmtpPhase::Greeting);
        assert_eq!(server.phase, ZmtpPhase::Greeting);

        // Step 2: each side processes the other's greeting.
        let client_out = client.on_network_bytes(server_greeting);
        let server_out = server.on_network_bytes(client_greeting);

        // Client (NULL, is_server=false) sends its READY immediately.
        let client_ready = collect_sends(&client_out);
        assert!(!client_ready.is_empty(), "client must send READY after receiving server greeting");
        assert_eq!(client.phase, ZmtpPhase::Ready);
        assert!(!has_handshake_complete(&client_out), "client HandshakeComplete must not fire yet");

        // Server transitions to Ready but does not send anything yet.
        assert!(collect_sends(&server_out).is_empty(), "server must NOT send anything after client greeting");
        assert_eq!(server.phase, ZmtpPhase::Ready);
        assert!(!has_handshake_complete(&server_out), "server HandshakeComplete must not fire yet");

        // Step 3: server receives client READY → sends server READY + HandshakeComplete.
        let server_out2 = server.on_network_bytes(client_ready);
        let server_ready = collect_sends(&server_out2);
        assert!(!server_ready.is_empty(), "server must send READY after receiving client READY");
        assert!(has_handshake_complete(&server_out2), "server must emit HandshakeComplete");
        assert_eq!(server.phase, ZmtpPhase::Data);

        // Step 4: client receives server READY → HandshakeComplete.
        let client_out2 = client.on_network_bytes(server_ready);
        assert!(collect_sends(&client_out2).is_empty(), "client must not send anything after server READY");
        assert!(has_handshake_complete(&client_out2), "client must emit HandshakeComplete");
        assert_eq!(client.phase, ZmtpPhase::Data);
    }

    #[test]
    fn test_null_handshake_flow_server_side() {
        let config = Arc::new(ZmtpEngineConfig::default());
        let mut engine = ZmtpEngine::new(true, config);

        // 1. Start: server sends its greeting immediately (simultaneous exchange).
        let start_out = engine.start();
        assert_eq!(start_out.net_actions.len(), 1, "expected server greeting from start()");
        match &start_out.net_actions[0] {
            NetAction::Send { data, .. } => assert_eq!(data.len(), GREETING_LENGTH),
            _ => panic!("expected NetAction::Send for server greeting"),
        }
        assert_eq!(engine.phase, ZmtpPhase::Greeting);

        // 2. Feed client greeting (64 bytes).
        let mut client_greeting_buf = BytesMut::new();
        ZmtpGreeting::encode(NullMechanism::NAME_BYTES, false, &mut client_greeting_buf);
        assert_eq!(client_greeting_buf.len(), GREETING_LENGTH);

        let out = engine.on_network_bytes(client_greeting_buf.freeze());

        // NULL security completes immediately; no tokens or extra sends at this point.
        // Server does not send READY until it receives the client's READY.
        assert!(out.net_actions.is_empty(), "no sends expected after client greeting (server waits for client READY)");
        assert_eq!(engine.phase, ZmtpPhase::Ready);

        // 3. Feed client READY.
        let mut props = HashMap::new();
        props.insert("Socket-Type".to_string(), b"DEALER".to_vec());
        let client_ready_msg = ZmtpReady::create_msg(props);

        let mut codec = ZmtpCodec::new();
        let mut client_ready_bytes = BytesMut::new();
        Encoder::encode(&mut codec, client_ready_msg, &mut client_ready_bytes)
            .expect("encode client READY");

        let out = engine.on_network_bytes(client_ready_bytes.freeze());

        // Server must reply with its own READY and signal handshake completion.
        assert_eq!(out.net_actions.len(), 1, "expected server READY");
        assert_eq!(out.app_actions.len(), 1, "expected HandshakeComplete");

        match &out.app_actions[0] {
            AppAction::HandshakeComplete { peer_socket_type, .. } => {
                assert_eq!(peer_socket_type.as_deref(), Some("DEALER"));
            }
            _ => panic!("expected AppAction::HandshakeComplete"),
        }
        assert_eq!(engine.phase, ZmtpPhase::Data);
    }
}
