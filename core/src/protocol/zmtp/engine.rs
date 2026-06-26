use super::actions::{AppAction, EngineOutput, NetAction};
use super::command::{ZmtpCommand, ZmtpReady};
use super::greeting::{
  encode_signature, encode_v3_tail, socket_type_code, socket_type_name_from_code, ZmtpGreeting,
  GREETING_LENGTH, MECHANISM_LENGTH, SIGNATURE_LENGTH, V2_REVISION, V3_REVISION,
};
use super::ZmtpCodec;
use crate::error::ZmqError;
use crate::message::{Blob, FrameBatch};
use crate::security::framer::{ISecureFramer, NullFramer};
use crate::security::mechanism::Mechanism;
use crate::security::null::NullMechanism;
use crate::socket::options::ZmtpEngineConfig;

use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::codec::Encoder;

/// Greeting byte index of the protocol revision (immediately after the signature).
const REVISION_OFFSET: usize = SIGNATURE_LENGTH; // 10
/// Greeting byte index of the ZMTP/2.0 socket-type code.
const V2_SOCKET_TYPE_OFFSET: usize = SIGNATURE_LENGTH + 1; // 11
/// Total length of a ZMTP/2.0 greeting header: signature + revision + socket-type.
const V2_GREETING_LENGTH: usize = SIGNATURE_LENGTH + 2; // 12

/// Negotiated ZMTP wire-protocol version for a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZmtpVersion {
  /// ZMTP/2.0 (legacy): no security negotiation, no READY command, no heartbeats.
  V2,
  /// ZMTP/3.x: full greeting with mechanism + READY exchange.
  V3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZmtpPhase {
  Greeting,
  Security,
  Ready,
  /// ZMTP/2.0-only: exchanging bare identity frames in place of the READY command.
  V2Identity,
  Data,
  Closed,
}

pub struct ZmtpEngine {
  config: Arc<ZmtpEngineConfig>,
  is_server: bool,
  pub phase: ZmtpPhase,

  network_read_accumulator: BytesMut,

  /// Negotiated protocol version. `None` until the peer's revision byte is read
  /// during the staged greeting exchange.
  version: Option<ZmtpVersion>,
  /// Whether we have already emitted our revision byte (greeting Stage B).
  revision_sent: bool,
  /// Whether we have already emitted our ZMTP/2.0 identity frame.
  v2_identity_sent: bool,
  /// Peer's socket-type name parsed from the ZMTP/2.0 greeting.
  v2_peer_socket_type: Option<String>,

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
    let sndbatch_count = config.sndbatch_count;
    let sndbatch_bytes_physical = config.sndbatch_bytes_physical;
    Self {
      config,
      is_server,
      phase: ZmtpPhase::Greeting,
      network_read_accumulator: BytesMut::with_capacity(8192),
      version: None,
      revision_sent: false,
      v2_identity_sent: false,
      v2_peer_socket_type: None,
      security_mechanism: Box::new(NullMechanism),
      framer: Box::new(NullFramer::new(max_msg_size, sndbatch_count, sndbatch_bytes_physical)),
      pending_framer: None,
      last_activity_time: Instant::now(),
      last_ping_sent_time: None,
      waiting_for_pong: false,
      partial_batch: FrameBatch::new(),
    }
  }

  // --- Public accessor API for the Tokio actor ---

  pub fn is_waiting_for_pong(&self) -> bool {
    self.waiting_for_pong
  }

  pub fn get_pong_deadline(&self) -> Option<Instant> {
    if self.waiting_for_pong {
      self.last_ping_sent_time.map(|t| {
        t + self
          .config
          .heartbeat_timeout
          .unwrap_or(Duration::from_secs(30))
      })
    } else {
      None
    }
  }

  pub fn record_activity(&mut self) {
    self.last_activity_time = Instant::now();
  }

  pub fn buffer_len(&self) -> usize {
    self.network_read_accumulator.len()
  }

  pub fn config(&self) -> &Arc<ZmtpEngineConfig> {
    &self.config
  }

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
    // Staged greeting (Stage A): send only the 10-byte version-agnostic signature.
    // The revision byte (Stage B) and version-specific tail (Stage C) are emitted
    // from `process_greeting` once we have seen the peer's signature/revision, so
    // that we can downgrade to ZMTP/2.0 without deadlocking.
    let mut out = EngineOutput::new();
    let mut buf = BytesMut::with_capacity(SIGNATURE_LENGTH);
    encode_signature(&mut buf);
    out.net_actions.push(NetAction::Send {
      data: buf.freeze(),
      zc_eligible: false,
    });
    out
  }

  pub fn on_network_bytes(&mut self, data: Bytes) -> EngineOutput {
    self.network_read_accumulator.extend_from_slice(&data);
    let mut out = EngineOutput::new();

    match self.phase {
      ZmtpPhase::Greeting => self.process_greeting(&mut out),
      ZmtpPhase::Security => self.process_security(&mut out),
      ZmtpPhase::Ready => self.process_ready(&mut out),
      ZmtpPhase::V2Identity => self.process_v2_identity(&mut out),
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
        out.net_actions.push(NetAction::Send {
          data,
          zc_eligible: zc,
        });
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
    // ZMTP/2.0 has no PING/PONG heartbeat (no COMMAND frames).
    if self.version == Some(ZmtpVersion::V2) {
      return out;
    }

    if let Some(timeout) = self.config.heartbeat_timeout {
      if self.waiting_for_pong {
        if let Some(ping_time) = self.last_ping_sent_time {
          if now.duration_since(ping_time) >= timeout {
            self.phase = ZmtpPhase::Closed;
            out
              .app_actions
              .push(AppAction::PeerError(ZmqError::Timeout));
            return out;
          }
        }
      }
    }

    if let Some(ivl) = self.config.heartbeat_ivl {
      if !self.waiting_for_pong && now.duration_since(self.last_activity_time) >= ivl {
        let ttl_ms = self
          .config
          .heartbeat_timeout
          .map(|d| d.as_millis().min(u16::MAX as u128) as u16)
          .unwrap_or(0);
        let ping_msg = ZmtpCommand::create_ping(ttl_ms, &[]);
        match encode_msg(ping_msg) {
          Ok(data) => {
            out.net_actions.push(NetAction::Send {
              data,
              zc_eligible: false,
            });
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
    // ── Stage B: emit our revision byte once we have seen the peer's signature. ──
    // We only need the peer's 10-byte signature here (not its revision), which is
    // what avoids the mutual-wait deadlock.
    if !self.revision_sent {
      if self.network_read_accumulator.len() < SIGNATURE_LENGTH {
        return;
      }
      let sig = &self.network_read_accumulator[..SIGNATURE_LENGTH];
      if sig[0] != 0xFF || sig[SIGNATURE_LENGTH - 1] != 0x7F {
        self.fail(out, ZmqError::ProtocolViolation("Invalid ZMTP signature".into()));
        return;
      }
      out.net_actions.push(NetAction::Send {
        data: Bytes::from_static(&[V3_REVISION]),
        zc_eligible: false,
      });
      self.revision_sent = true;
    }

    // ── Stage C: commit to a version once we have the peer's revision byte. ──
    if self.version.is_none() {
      if self.network_read_accumulator.len() < REVISION_OFFSET + 1 {
        return;
      }
      let peer_revision = self.network_read_accumulator[REVISION_OFFSET];

      if peer_revision >= V3_REVISION {
        self.version = Some(ZmtpVersion::V3);
        let mut tail = BytesMut::new();
        encode_v3_tail(local_mechanism_name_bytes(&self.config), self.is_server, &mut tail);
        out.net_actions.push(NetAction::Send {
          data: tail.freeze(),
          zc_eligible: false,
        });
        // Fall through to the v3 greeting decode below.
      } else if peer_revision == V2_REVISION {
        if !self.config.allow_zmtp2 {
          self.fail(
            out,
            ZmqError::ProtocolViolation("ZMTP/2.0 downgrade disallowed by config".into()),
          );
          return;
        }
        // The v2 socket-type lives at byte 11; wait for the full 12-byte header.
        if self.network_read_accumulator.len() < V2_GREETING_LENGTH {
          return;
        }
        let peer_stype_byte = self.network_read_accumulator[V2_SOCKET_TYPE_OFFSET];
        if let Err(e) = self.validate_v2_compatibility(peer_stype_byte) {
          self.fail(out, e);
          return;
        }
        self.v2_peer_socket_type = socket_type_name_from_code(peer_stype_byte).map(String::from);
        self.version = Some(ZmtpVersion::V2);

        // Emit our v2 tail: a single socket-type byte at position 11.
        let local_stype = match socket_type_code(&self.config.socket_type_name) {
          Some(code) => code,
          None => {
            self.fail(
              out,
              ZmqError::ProtocolViolation(format!(
                "Socket type {} has no ZMTP/2.0 code",
                self.config.socket_type_name
              )),
            );
            return;
          }
        };
        out.net_actions.push(NetAction::Send {
          data: Bytes::copy_from_slice(&[local_stype]),
          zc_eligible: false,
        });

        // Consume the 12-byte v2 greeting header; the rest is identity/data frames.
        self.network_read_accumulator.advance(V2_GREETING_LENGTH);
        self.phase = ZmtpPhase::V2Identity;
        self.process_v2_identity(out);
        return;
      } else {
        self.fail(
          out,
          ZmqError::ProtocolViolation(format!("Unsupported ZMTP revision {}", peer_revision)),
        );
        return;
      }
    }

    // ── ZMTP/3.x path: wait for the full 64-byte greeting, then proceed. ──
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
        let reason = self
          .security_mechanism
          .error_reason()
          .unwrap_or("unknown")
          .to_owned();
        self.phase = ZmtpPhase::Closed;
        out
          .app_actions
          .push(AppAction::PeerError(ZmqError::SecurityError(reason)));
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
          Ok(data) => out.net_actions.push(NetAction::Send {
            data,
            zc_eligible: false,
          }),
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
          out
            .app_actions
            .push(AppAction::PeerError(ZmqError::ProtocolViolation(
              "Peer sent ERROR during Ready exchange".into(),
            )));
          return;
        }
        _ => {
          self.phase = ZmtpPhase::Closed;
          out
            .app_actions
            .push(AppAction::PeerError(ZmqError::ProtocolViolation(
              "Expected READY command".into(),
            )));
          return;
        }
      };

      let peer_socket_type = ready_cmd
        .properties
        .get("Socket-Type")
        .map(|v| String::from_utf8_lossy(v).into_owned());
      let peer_identity = ready_cmd
        .properties
        .get("Identity")
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

      out.app_actions.push(AppAction::HandshakeComplete {
        peer_identity,
        peer_socket_type,
      });

      // Drain any remaining accumulated bytes into the Data phase handler.
      if !self.network_read_accumulator.is_empty() {
        self.process_data(out);
      }
      return;
    }
  }

  /// ZMTP/2.0 post-greeting handshake: each side sends one bare identity frame
  /// (the routing id, or empty for anonymous) and reads the peer's. There is no
  /// security exchange and no READY command. Completing it emits
  /// `HandshakeComplete` and transitions to the Data phase.
  fn process_v2_identity(&mut self, out: &mut EngineOutput) {
    // Send our identity frame exactly once.
    if !self.v2_identity_sent {
      let identity_bytes = self
        .config
        .routing_id
        .as_ref()
        .map_or_else(Vec::new, |id| id.as_ref().to_vec());
      let identity_msg = crate::Msg::from_vec(identity_bytes);
      match encode_msg(identity_msg) {
        Ok(data) => out.net_actions.push(NetAction::Send {
          data,
          zc_eligible: false,
        }),
        Err(e) => {
          self.fail(out, e);
          return;
        }
      }
      self.v2_identity_sent = true;
    }

    // Read the peer's identity frame (a plain data frame; NullFramer is active).
    let identity_msg = match self.framer.try_read_msg(&mut self.network_read_accumulator) {
      Ok(Some(m)) => m,
      Ok(None) => return,
      Err(e) => {
        self.fail(out, e);
        return;
      }
    };

    if identity_msg.is_command() || identity_msg.is_more() {
      self.fail(
        out,
        ZmqError::ProtocolViolation("Invalid ZMTP/2.0 identity frame".into()),
      );
      return;
    }
    let id_bytes = identity_msg.data().unwrap_or(&[]);
    if id_bytes.len() > 255 {
      self.fail(
        out,
        ZmqError::ProtocolViolation("ZMTP/2.0 identity exceeded 255 bytes".into()),
      );
      return;
    }
    let peer_identity = if id_bytes.is_empty() {
      None
    } else {
      Some(Blob::from(id_bytes.to_vec()))
    };

    self.phase = ZmtpPhase::Data;
    self.last_activity_time = Instant::now();

    if self.config.use_cork
      && matches!(
        self.config.socket_type_name.as_str(),
        "PUSH" | "PULL" | "PUB" | "SUB"
      )
    {
      out.net_actions.push(NetAction::SetCork(true));
    }

    out.app_actions.push(AppAction::HandshakeComplete {
      peer_identity,
      peer_socket_type: self.v2_peer_socket_type.clone(),
    });

    // Drain any data frames that arrived in the same read as the identity frame.
    if !self.network_read_accumulator.is_empty() {
      self.process_data(out);
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
        // ZMTP/2.0 has no COMMAND frames; receiving one is a protocol violation.
        if self.version == Some(ZmtpVersion::V2) {
          self.phase = ZmtpPhase::Closed;
          out
            .app_actions
            .push(AppAction::PeerError(ZmqError::ProtocolViolation(
              "Received COMMAND-flagged frame on a ZMTP/2.0 session".into(),
            )));
          return;
        }
        match ZmtpCommand::parse(&msg) {
          Some(ZmtpCommand::Ping(ctx)) => {
            let pong = ZmtpCommand::create_pong(&ctx);
            match encode_msg(pong) {
              Ok(data) => out.net_actions.push(NetAction::Send {
                data,
                zc_eligible: false,
              }),
              Err(e) => out.app_actions.push(AppAction::PeerError(e)),
            }
          }
          Some(ZmtpCommand::Pong(_)) => {
            self.waiting_for_pong = false;
          }
          Some(ZmtpCommand::Error) => {
            self.phase = ZmtpPhase::Closed;
            out
              .app_actions
              .push(AppAction::PeerError(ZmqError::ProtocolViolation(
                "Peer sent ERROR command".into(),
              )));
            return;
          }
          _ => {}
        }
        continue;
      }

      let is_more = msg.is_more();
      self.partial_batch.push(msg);
      if !is_more {
        let batch = std::mem::replace(&mut self.partial_batch, FrameBatch::new());
        out.app_actions.push(AppAction::DeliverMessage(batch));
      }
    }
  }

  // --- Helpers ---

  /// Marks the engine closed and emits a fatal `PeerError`.
  fn fail(&mut self, out: &mut EngineOutput, err: ZmqError) {
    self.phase = ZmtpPhase::Closed;
    out.app_actions.push(AppAction::PeerError(err));
  }

  /// Validates that our local socket type is compatible with the peer's ZMTP/2.0
  /// socket-type byte (e.g. PUSH↔PULL, REQ↔REP/ROUTER).
  fn validate_v2_compatibility(&self, peer_byte: u8) -> Result<(), ZmqError> {
    use super::greeting::*;
    let peer_name = socket_type_name_from_code(peer_byte).ok_or_else(|| {
      ZmqError::ProtocolViolation(format!("v2 peer used unknown socket-type byte {:#04x}", peer_byte))
    })?;
    let own = self.config.socket_type_name.as_str();
    let ok = matches!(
      (own, peer_byte),
      ("PULL", V2_SOCKET_TYPE_PUSH)
        | ("PUSH", V2_SOCKET_TYPE_PULL)
        | ("PUB", V2_SOCKET_TYPE_SUB)
        | ("SUB", V2_SOCKET_TYPE_PUB)
        | ("PUB", V2_SOCKET_TYPE_XSUB)
        | ("XSUB", V2_SOCKET_TYPE_PUB)
        | ("XPUB", V2_SOCKET_TYPE_SUB)
        | ("SUB", V2_SOCKET_TYPE_XPUB)
        | ("XPUB", V2_SOCKET_TYPE_XSUB)
        | ("XSUB", V2_SOCKET_TYPE_XPUB)
        | ("REQ", V2_SOCKET_TYPE_REP)
        | ("REP", V2_SOCKET_TYPE_REQ)
        | ("REQ", V2_SOCKET_TYPE_ROUTER)
        | ("ROUTER", V2_SOCKET_TYPE_REQ)
        | ("REP", V2_SOCKET_TYPE_DEALER)
        | ("DEALER", V2_SOCKET_TYPE_REP)
        | ("DEALER", V2_SOCKET_TYPE_ROUTER)
        | ("ROUTER", V2_SOCKET_TYPE_DEALER)
        | ("DEALER", V2_SOCKET_TYPE_DEALER)
        | ("ROUTER", V2_SOCKET_TYPE_ROUTER)
        | ("PAIR", V2_SOCKET_TYPE_PAIR)
    );
    if !ok {
      return Err(ZmqError::ProtocolViolation(format!(
        "Incompatible ZMTP/2.0 sockets: local {} <-> peer {}",
        own, peer_name
      )));
    }
    Ok(())
  }

  fn derive_pending_framer(&mut self) -> Result<Box<dyn ISecureFramer>, ZmqError> {
    let old = std::mem::replace(&mut self.security_mechanism, Box::new(NullMechanism));
    let (new_framer, _peer_id_bytes) = old.into_framer(
      self.config.max_msg_size,
      self.config.sndbatch_count,
      self.config.sndbatch_bytes_physical,
    )?;
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
      Ok(data) => out.net_actions.push(NetAction::Send {
        data,
        zc_eligible: false,
      }),
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

fn encode_msg(msg: crate::Msg) -> Result<Bytes, ZmqError> {
  let mut codec = ZmtpCodec::new();
  let mut buf = BytesMut::new();
  codec
    .encode(msg, &mut buf)
    .map_err(|e| ZmqError::Internal(e.to_string()))?;
  Ok(buf.freeze())
}

fn build_local_ready_props(config: &ZmtpEngineConfig) -> HashMap<String, Vec<u8>> {
  let mut props = HashMap::new();
  props.insert(
    "Socket-Type".to_string(),
    config.socket_type_name.as_bytes().to_vec(),
  );
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
  use crate::protocol::zmtp::greeting::{
    socket_type_code, V2_SOCKET_TYPE_PUB, V2_SOCKET_TYPE_PULL, V2_SOCKET_TYPE_PUSH,
    V2_SOCKET_TYPE_REP, V2_SOCKET_TYPE_SUB,
  };
  use bytes::BufMut;

  // --- Helpers ---

  fn cfg(socket_type: &str, allow_zmtp2: bool) -> Arc<ZmtpEngineConfig> {
    Arc::new(ZmtpEngineConfig {
      socket_type_name: socket_type.to_string(),
      allow_zmtp2,
      ..Default::default()
    })
  }

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
    out
      .app_actions
      .iter()
      .any(|a| matches!(a, AppAction::HandshakeComplete { .. }))
  }

  fn peer_error(out: &EngineOutput) -> Option<&ZmqError> {
    out.app_actions.iter().find_map(|a| match a {
      AppAction::PeerError(e) => Some(e),
      _ => None,
    })
  }

  fn signature_bytes() -> Bytes {
    let mut b = BytesMut::new();
    encode_signature(&mut b);
    b.freeze()
  }

  /// Encode a plain (non-command) ZMTP data frame, as a real peer would put it on
  /// the wire — used to simulate v2 identity frames and data.
  fn plain_frame(payload: &[u8]) -> Bytes {
    let mut codec = ZmtpCodec::new();
    let mut buf = BytesMut::new();
    Encoder::encode(&mut codec, crate::Msg::from_vec(payload.to_vec()), &mut buf).unwrap();
    buf.freeze()
  }

  /// Drive two rzmq engines against each other until both reach Data (or one
  /// closes). Returns the `AppAction`s each side accumulated. Both rzmq peers
  /// advertise revision 3, so this always negotiates ZMTP/3.x.
  fn drive(client: &mut ZmtpEngine, server: &mut ZmtpEngine) -> (Vec<AppAction>, Vec<AppAction>) {
    let mut to_server = collect_sends(&client.start());
    let mut to_client = collect_sends(&server.start());
    let mut client_apps = Vec::new();
    let mut server_apps = Vec::new();

    for _ in 0..64 {
      let mut progressed = false;

      if !to_client.is_empty() {
        let data = std::mem::replace(&mut to_client, Bytes::new());
        let out = client.on_network_bytes(data);
        let sends = collect_sends(&out);
        if !sends.is_empty() {
          let mut buf = BytesMut::from(&to_server[..]);
          buf.extend_from_slice(&sends);
          to_server = buf.freeze();
        }
        client_apps.extend(out.app_actions);
        progressed = true;
      }

      if !to_server.is_empty() {
        let data = std::mem::replace(&mut to_server, Bytes::new());
        let out = server.on_network_bytes(data);
        let sends = collect_sends(&out);
        if !sends.is_empty() {
          let mut buf = BytesMut::from(&to_client[..]);
          buf.extend_from_slice(&sends);
          to_client = buf.freeze();
        }
        server_apps.extend(out.app_actions);
        progressed = true;
      }

      if client.phase == ZmtpPhase::Closed || server.phase == ZmtpPhase::Closed {
        break;
      }
      if client.phase == ZmtpPhase::Data
        && server.phase == ZmtpPhase::Data
        && to_client.is_empty()
        && to_server.is_empty()
      {
        break;
      }
      if !progressed {
        break;
      }
    }
    (client_apps, server_apps)
  }

  /// Feed a single rzmq engine the exact wire bytes a real ZMTP/2.0 peer emits:
  /// signature, then revision(0x01) + socket-type, then a bare identity frame.
  /// Returns the engine's outputs for each fed chunk.
  fn drive_against_v2_peer(
    engine: &mut ZmtpEngine,
    peer_stype: u8,
    peer_identity: &[u8],
  ) -> Vec<EngineOutput> {
    let _ = engine.start();
    let mut outs = Vec::new();
    outs.push(engine.on_network_bytes(signature_bytes()));
    outs.push(engine.on_network_bytes(Bytes::copy_from_slice(&[V2_REVISION, peer_stype])));
    outs.push(engine.on_network_bytes(plain_frame(peer_identity)));
    outs
  }

  // --- I. Staged greeting / negotiation ---

  #[test]
  fn test_start_emits_signature_only() {
    let mut eng = ZmtpEngine::new(false, cfg("DEALER", true));
    let out = eng.start();
    assert_eq!(collect_sends(&out).len(), SIGNATURE_LENGTH, "start() must emit only the 10-byte signature");
  }

  #[test]
  fn test_revision_emitted_after_peer_signature_no_deadlock() {
    // Both peers send only their signature first. Each must then emit its
    // revision byte from *only* the peer's signature — never blocking on the
    // peer's revision (which would deadlock).
    let mut client = ZmtpEngine::new(false, cfg("DEALER", true));
    let mut server = ZmtpEngine::new(true, cfg("ROUTER", true));
    let _ = client.start();
    let _ = server.start();

    let c_out = client.on_network_bytes(signature_bytes());
    let s_out = server.on_network_bytes(signature_bytes());

    assert_eq!(collect_sends(&c_out).as_ref(), &[V3_REVISION], "client emits revision after peer signature");
    assert_eq!(collect_sends(&s_out).as_ref(), &[V3_REVISION], "server emits revision after peer signature");
    assert!(client.revision_sent && server.revision_sent);
  }

  #[test]
  fn test_v3_handshake_completes_both_sides() {
    let mut client = ZmtpEngine::new(false, cfg("DEALER", true));
    let mut server = ZmtpEngine::new(true, cfg("ROUTER", true));
    let (c_apps, s_apps) = drive(&mut client, &mut server);

    assert_eq!(client.phase, ZmtpPhase::Data);
    assert_eq!(server.phase, ZmtpPhase::Data);
    assert_eq!(client.version, Some(ZmtpVersion::V3));
    assert_eq!(server.version, Some(ZmtpVersion::V3));
    assert!(c_apps.iter().any(|a| matches!(a, AppAction::HandshakeComplete { .. })));

    match s_apps.iter().find(|a| matches!(a, AppAction::HandshakeComplete { .. })) {
      Some(AppAction::HandshakeComplete { peer_socket_type, .. }) => {
        assert_eq!(peer_socket_type.as_deref(), Some("DEALER"));
      }
      _ => panic!("server expected HandshakeComplete with peer_socket_type"),
    }
  }

  #[test]
  fn test_v3_greeting_fragmented_one_byte_at_a_time() {
    // A v3 server engine fed the peer greeting one byte at a time must still
    // negotiate v3 at the correct boundaries.
    let mut eng = ZmtpEngine::new(true, cfg("ROUTER", true));
    let _ = eng.start();

    let mut peer = BytesMut::new();
    encode_signature(&mut peer);
    peer.put_u8(V3_REVISION);
    encode_v3_tail(NullMechanism::NAME_BYTES, false, &mut peer);
    assert_eq!(peer.len(), GREETING_LENGTH);

    for b in peer.freeze() {
      eng.on_network_bytes(Bytes::copy_from_slice(&[b]));
    }
    assert_eq!(eng.version, Some(ZmtpVersion::V3));
    // NULL completes immediately; server waits in Ready for the client READY.
    assert_eq!(eng.phase, ZmtpPhase::Ready);
  }

  // --- II. ZMTP/2.0 downgrade (exact-wire-bytes simulation of a libzmq v2 peer) ---

  #[test]
  fn test_v2_downgrade_anonymous() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true)); // peer is PUSH
    let outs = drive_against_v2_peer(&mut eng, V2_SOCKET_TYPE_PUSH, &[]);

    // After the revision+socket-type chunk, the engine commits to v2, sends its
    // socket-type tail + identity frame, and is awaiting the peer identity.
    let stype_chunk = collect_sends(&outs[1]);
    assert_eq!(stype_chunk[0], socket_type_code("PULL").unwrap(), "v2 tail starts with our socket-type byte");
    assert_eq!(eng.version, Some(ZmtpVersion::V2));

    assert_eq!(eng.phase, ZmtpPhase::Data, "engine reaches Data after peer identity");
    match outs[2].app_actions.iter().find(|a| matches!(a, AppAction::HandshakeComplete { .. })) {
      Some(AppAction::HandshakeComplete { peer_identity, peer_socket_type }) => {
        assert_eq!(peer_socket_type.as_deref(), Some("PUSH"));
        assert!(peer_identity.is_none(), "anonymous peer has no identity");
      }
      _ => panic!("expected HandshakeComplete"),
    }
  }

  #[test]
  fn test_v2_downgrade_with_identity() {
    let mut eng = ZmtpEngine::new(false, cfg("PUSH", true)); // peer is PULL
    let outs = drive_against_v2_peer(&mut eng, V2_SOCKET_TYPE_PULL, b"peer-id");

    assert_eq!(eng.version, Some(ZmtpVersion::V2));
    assert_eq!(eng.phase, ZmtpPhase::Data);
    match outs[2].app_actions.iter().find(|a| matches!(a, AppAction::HandshakeComplete { .. })) {
      Some(AppAction::HandshakeComplete { peer_identity, .. }) => {
        assert_eq!(peer_identity.as_ref().map(|b| b.as_ref()), Some(&b"peer-id"[..]));
      }
      _ => panic!("expected HandshakeComplete with identity"),
    }
  }

  #[test]
  fn test_v2_downgrade_all_bytes_in_one_read() {
    // Signature + revision + socket-type + identity arriving in a single read
    // must drain straight through to Data.
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true));
    let _ = eng.start();
    let mut wire = BytesMut::new();
    wire.extend_from_slice(&signature_bytes());
    wire.put_u8(V2_REVISION);
    wire.put_u8(V2_SOCKET_TYPE_PUSH);
    wire.extend_from_slice(&plain_frame(&[]));
    let out = eng.on_network_bytes(wire.freeze());
    assert_eq!(eng.phase, ZmtpPhase::Data);
    assert!(has_handshake_complete(&out));
  }

  #[test]
  fn test_v2_disallowed_when_config_off() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", false));
    let _ = eng.start();
    eng.on_network_bytes(signature_bytes());
    let out = eng.on_network_bytes(Bytes::copy_from_slice(&[V2_REVISION, V2_SOCKET_TYPE_PUSH]));
    assert_eq!(eng.phase, ZmtpPhase::Closed);
    assert!(matches!(peer_error(&out), Some(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_v2_incompatible_socket_type() {
    let mut eng = ZmtpEngine::new(true, cfg("REQ", true)); // REQ vs SUB is invalid
    let _ = eng.start();
    eng.on_network_bytes(signature_bytes());
    let out = eng.on_network_bytes(Bytes::copy_from_slice(&[V2_REVISION, V2_SOCKET_TYPE_SUB]));
    assert_eq!(eng.phase, ZmtpPhase::Closed);
    match peer_error(&out) {
      Some(ZmqError::ProtocolViolation(s)) => assert!(s.contains("Incompatible")),
      other => panic!("expected Incompatible ProtocolViolation, got {:?}", other),
    }
  }

  #[test]
  fn test_v2_unknown_socket_type_byte() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true));
    let _ = eng.start();
    eng.on_network_bytes(signature_bytes());
    let out = eng.on_network_bytes(Bytes::copy_from_slice(&[V2_REVISION, 200u8]));
    assert_eq!(eng.phase, ZmtpPhase::Closed);
    assert!(matches!(peer_error(&out), Some(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_v2_identity_too_long() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true));
    let _ = eng.start();
    eng.on_network_bytes(signature_bytes());
    eng.on_network_bytes(Bytes::copy_from_slice(&[V2_REVISION, V2_SOCKET_TYPE_PUSH]));
    let out = eng.on_network_bytes(plain_frame(&vec![0u8; 256]));
    assert_eq!(eng.phase, ZmtpPhase::Closed);
    assert!(matches!(peer_error(&out), Some(ZmqError::ProtocolViolation(_))));
  }

  // --- III. v2 compatibility matrix ---

  #[test]
  fn test_v2_compatibility_matrix() {
    // (local, peer_byte, expected_ok)
    let cases: &[(&str, u8, bool)] = &[
      ("PULL", V2_SOCKET_TYPE_PUSH, true),
      ("PUSH", V2_SOCKET_TYPE_PUB, false),
      ("PUB", V2_SOCKET_TYPE_SUB, true),
      ("SUB", V2_SOCKET_TYPE_PUB, true),
      ("REQ", V2_SOCKET_TYPE_REP, true),
      ("REQ", V2_SOCKET_TYPE_SUB, false),
      ("ROUTER", V2_SOCKET_TYPE_REP, false),
    ];
    for (local, peer, expect_ok) in cases {
      let eng = ZmtpEngine::new(true, cfg(local, true));
      assert_eq!(
        eng.validate_v2_compatibility(*peer).is_ok(),
        *expect_ok,
        "local={} peer={:#04x}",
        local,
        peer
      );
    }
  }

  // --- IV. v2 data-phase + heartbeat constraints ---

  #[test]
  fn test_v2_data_phase_rejects_command_frame() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true));
    drive_against_v2_peer(&mut eng, V2_SOCKET_TYPE_PUSH, &[]);
    assert_eq!(eng.phase, ZmtpPhase::Data);

    // A COMMAND-flagged frame (a PING) is illegal on a v2 session.
    let ping = ZmtpCommand::create_ping(0, &[]);
    let out = eng.on_network_bytes(encode_msg(ping).unwrap());
    assert_eq!(eng.phase, ZmtpPhase::Closed);
    assert!(matches!(peer_error(&out), Some(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn test_v2_data_frame_round_trips() {
    let mut eng = ZmtpEngine::new(true, cfg("PULL", true));
    drive_against_v2_peer(&mut eng, V2_SOCKET_TYPE_PUSH, &[]);
    let out = eng.on_network_bytes(plain_frame(b"hello"));
    let delivered = out.app_actions.iter().any(|a| matches!(a, AppAction::DeliverMessage(_)));
    assert!(delivered, "v2 data frame must be delivered");
  }

  #[test]
  fn test_v2_on_tick_never_pings() {
    let config = Arc::new(ZmtpEngineConfig {
      socket_type_name: "PULL".to_string(),
      allow_zmtp2: true,
      heartbeat_ivl: Some(Duration::from_millis(1)),
      heartbeat_timeout: Some(Duration::from_secs(1)),
      ..Default::default()
    });
    let mut eng = ZmtpEngine::new(true, config);
    drive_against_v2_peer(&mut eng, V2_SOCKET_TYPE_PUSH, &[]);
    assert_eq!(eng.phase, ZmtpPhase::Data);
    let out = eng.on_tick(Instant::now() + Duration::from_secs(10));
    assert!(out.net_actions.is_empty(), "v2 sessions must never emit a PING");
  }

  #[test]
  fn test_v3_on_tick_still_pings() {
    let config = Arc::new(ZmtpEngineConfig {
      socket_type_name: "DEALER".to_string(),
      allow_zmtp2: true,
      heartbeat_ivl: Some(Duration::from_millis(1)),
      heartbeat_timeout: Some(Duration::from_secs(1)),
      ..Default::default()
    });
    let mut client = ZmtpEngine::new(false, config.clone());
    let mut server = ZmtpEngine::new(true, config);
    drive(&mut client, &mut server);
    assert_eq!(client.phase, ZmtpPhase::Data);
    let out = client.on_tick(Instant::now() + Duration::from_secs(10));
    assert!(!out.net_actions.is_empty(), "v3 session should emit a PING after the interval");
  }
}
