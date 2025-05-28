// core/src/engine/uring_core.rs

#![cfg(feature = "io-uring")]

use crate::context::Context;
use crate::error::ZmqError;
use crate::message::{Msg, MsgFlags};
use crate::protocol::zmtp::command::{
  ZmtpCommand, ZMTP_CMD_PONG_NAME, ZMTP_CMD_READY_NAME, ZMTP_FLAG_COMMAND, ZMTP_FLAG_LONG, ZMTP_FLAG_MORE,
};
use crate::protocol::zmtp::greeting::{ZmtpGreeting, GREETING_LENGTH, GREETING_VERSION_MAJOR, MECHANISM_LENGTH};
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::runtime::{ActorType, Command as SessionBaseCommand, MailboxReceiver, MailboxSender};
use crate::security::{Mechanism, MechanismStatus, NullMechanism, PlainMechanism};
use crate::socket::options::ZmtpEngineConfig;
use crate::Blob;

use super::uring_recv::UringMultishotReceiver;
use tokio_uring::buf::BufResult;

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::io::OwnedFd;
use std::pin::Pin;
use std::time::{Duration, Instant};
use std::{fmt, io};

use tokio::io::{AsyncReadExt, AsyncWriteExt as TokioAsyncWriteExt}; // Use alias for std AsyncWriteExt
use tokio::time::interval as tokio_interval;
use tokio_uring::net::TcpStream as UringTcpStream;

#[derive(Debug)]
pub(crate) enum AppToUringEngineCmd {
  SendMsg(Msg),
  Stop,
}

#[derive(Debug)]
pub(crate) enum EncodedMsgParts {
  ForWriteAll(Bytes),
  ForSendZc(Bytes, Option<Bytes>),
}

#[derive(Debug)]
pub(crate) enum ActiveReceiveFutureUring {
  UringMultishot(Pin<Box<dyn Future<Output = BufResult<(usize, Vec<BytesMut>)>> + Send>>),
  NotActive,
}

impl Default for ActiveReceiveFutureUring {
  fn default() -> Self {
    ActiveReceiveFutureUring::NotActive
  }
}

pub(crate) struct ZmtpEngineCoreUring {
  handle: usize,
  session_base_mailbox: MailboxSender,
  app_cmd_rx: MailboxReceiver<AppToUringEngineCmd>,
  stream: UringTcpStream,
  config: ZmtpEngineConfig,
  is_server: bool,
  mechanism: Box<dyn Mechanism>,
  data_cipher: Option<Box<dyn IDataCipher>>,
  // New buffer for raw network reads, used when a data_cipher is active.
  // `self.read_buffer` will then be used for zmtp_manual_parser input after decryption.
  network_bytes_buffer: BytesMut, // For accumulating bytes from socket before decryption/parsing secure messages
  read_buffer: BytesMut,
  zmtp_manual_parser: ZmtpManualParser,
  multishot_receiver_manager: Option<UringMultishotReceiver>,
  active_recv_future_holder: ActiveReceiveFutureUring,
  context: Context,
  last_activity_time: Instant,
  last_ping_sent_time: Option<Instant>,
  waiting_for_pong: bool,
  heartbeat_ivl: Option<Duration>,
  heartbeat_timeout: Duration,
  #[cfg(target_os = "linux")]
  expecting_first_frame_of_msg: bool,
  #[cfg(target_os = "linux")]
  is_corked: bool,
  #[cfg(target_os = "linux")]
  cork_fd: Option<RawFd>,
  _phantom: PhantomData<*const ()>,
}

impl fmt::Debug for ZmtpEngineCoreUring {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("ZmtpEngineCoreUring")
      .field("handle", &self.handle)
      .field("is_server", &self.is_server)
      .field("config", &self.config)
      .field("mechanism_status", &self.mechanism.status())
      .field("last_activity_time", &self.last_activity_time)
      .field("heartbeat_ivl", &self.heartbeat_ivl)
      .field("heartbeat_timeout", &self.heartbeat_timeout)
      .field(
        "multishot_active",
        &matches!(
          self.active_recv_future_holder,
          ActiveReceiveFutureUring::UringMultishot(_)
        ),
      )
      .finish_non_exhaustive()
  }
}

impl ZmtpEngineCoreUring {
  pub fn new(
    handle: usize,
    session_base_mailbox: MailboxSender,
    app_cmd_rx: MailboxReceiver<AppToUringEngineCmd>,
    stream: UringTcpStream,
    config: ZmtpEngineConfig,
    is_server: bool,
    context: Context,
  ) -> Self {
    #[cfg(target_os = "linux")]
    let initial_cork_fd = if config.use_cork {
      Some(stream.as_raw_fd())
    } else {
      None
    };

    let ms_receiver_init = if config.use_recv_multishot {
      tracing::debug!(
        engine_handle = handle,
        num_bufs = config.recv_multishot_buffer_count,
        buf_cap = config.recv_multishot_buffer_capacity,
        "UringEngine: Initializing UringMultishotReceiver"
      );
      Some(UringMultishotReceiver::new(
        config.recv_multishot_buffer_count.max(1),
        config.recv_multishot_buffer_capacity.max(1024),
      ))
    } else {
      None
    };

    Self {
      handle,
      session_base_mailbox,
      app_cmd_rx,
      stream,
      is_server,
      mechanism: Box::new(NullMechanism),
      data_cipher: None,
      network_bytes_buffer: BytesMut::with_capacity(8192 * 2), // Initialize new buffer
      read_buffer: BytesMut::with_capacity(8192 * 2),
      zmtp_manual_parser: ZmtpManualParser::new(),
      multishot_receiver_manager: ms_receiver_init,
      active_recv_future_holder: ActiveReceiveFutureUring::NotActive,
      context,
      last_activity_time: Instant::now(),
      last_ping_sent_time: None,
      waiting_for_pong: false,
      heartbeat_ivl: config.heartbeat_ivl,
      heartbeat_timeout: config.heartbeat_timeout.unwrap_or_else(|| {
        config
          .heartbeat_ivl
          .map_or(Duration::from_secs(30), |ivl| ivl.saturating_mul(2))
      }),
      #[cfg(target_os = "linux")]
      expecting_first_frame_of_msg: true,
      #[cfg(target_os = "linux")]
      is_corked: false,
      #[cfg(target_os = "linux")]
      cork_fd: initial_cork_fd,
      config,
      _phantom: PhantomData,
    }
  }

  fn encode_msg_for_send(&self, msg: &Msg) -> Result<EncodedMsgParts, ZmqError> {
    let data = msg.data().unwrap_or(&[]);
    let size = data.len();
    let msg_flags = msg.flags();
    let mut zmtp_flags_byte = 0u8;
    if msg_flags.contains(MsgFlags::MORE) {
      zmtp_flags_byte |= ZMTP_FLAG_MORE;
    }
    if msg_flags.contains(MsgFlags::COMMAND) {
      zmtp_flags_byte |= ZMTP_FLAG_COMMAND;
    }

    let mut header_buf = BytesMut::new();
    if size <= 255 {
      header_buf.reserve(1 + 1);
      header_buf.put_u8(zmtp_flags_byte);
      header_buf.put_u8(size as u8);
    } else {
      zmtp_flags_byte |= ZMTP_FLAG_LONG;
      header_buf.reserve(1 + 8);
      header_buf.put_u8(zmtp_flags_byte);
      header_buf.put_u64(size as u64);
    }

    if self.config.use_send_zerocopy {
      let payload_bytes = msg.data_bytes();
      Ok(EncodedMsgParts::ForSendZc(header_buf.freeze(), payload_bytes))
    } else {
      let mut full_frame_buf = header_buf;
      full_frame_buf.extend_from_slice(data);
      Ok(EncodedMsgParts::ForWriteAll(full_frame_buf.freeze()))
    }
  }

  async fn send_greeting_uring(&mut self) -> Result<(), ZmqError> {
    let mut greeting_buf = BytesMut::with_capacity(GREETING_LENGTH);
    let own_greeting_mechanism_bytes = NullMechanism::NAME_BYTES;
    ZmtpGreeting::encode(&own_greeting_mechanism_bytes, self.is_server, &mut greeting_buf);
    self.stream.write_all(&greeting_buf).await?;
    // No explicit flush needed for uring after write_all if not corked.
    // If corked, uncorking is the "flush".
    tracing::debug!(engine_handle = self.handle, "Sent own ZMTP greeting (uring).");
    self.last_activity_time = Instant::now();
    Ok(())
  }

  async fn receive_greeting_uring(&mut self) -> Result<ZmtpGreeting, ZmqError> {
    let mut received_greeting_bytes = BytesMut::with_capacity(GREETING_LENGTH);
    while received_greeting_bytes.len() < GREETING_LENGTH {
      let bytes_read = self.stream.read_buf(&mut received_greeting_bytes).await?;
      if bytes_read == 0 {
        return Err(ZmqError::ConnectionClosed);
      }
      self.last_activity_time = Instant::now();
    }
    ZmtpGreeting::decode(&mut received_greeting_bytes)?
      .ok_or_else(|| ZmqError::ProtocolViolation("Incomplete greeting".into()))
  }

  async fn perform_security_handshake_uring(&mut self, peer_greeting: ZmtpGreeting) -> Result<(), ZmqError> {
    if peer_greeting.version.0 < GREETING_VERSION_MAJOR {
      return Err(ZmqError::ProtocolViolation(format!(
        "Unsupported ZMTP version: {}.{}",
        peer_greeting.version.0, peer_greeting.version.1
      )));
    }
    if self.is_server == peer_greeting.as_server {
      return Err(ZmqError::ProtocolViolation("Role mismatch".into()));
    }

    let peer_mech_name = peer_greeting.mechanism_name();
    let new_mechanism: Box<dyn Mechanism> = match peer_mech_name {
      NullMechanism::NAME => Box::new(NullMechanism),
      PlainMechanism::NAME => Box::new(PlainMechanism::new(self.is_server)),
      #[cfg(feature = "noise_xx")]
      NoiseXxMechanism::NAME if self.config.use_noise_xx => {
        // Placeholder for brevity
        let local_sk = self
          .config
          .noise_xx_local_sk_bytes_for_engine
          .ok_or(ZmqError::Internal("todo".into()))?;
        let remote_pk = self.config.noise_xx_remote_pk_bytes_for_engine;
        Box::new(NoiseXxMechanism::new(self.is_server, &local_sk, remote_pk)?)
      }
      unsupported => {
        return Err(ZmqError::SecurityError(format!(
          "Unsupported mechanism: {}",
          unsupported
        )))
      }
    };
    self.mechanism = new_mechanism;

    tracing::info!(
      engine_handle = self.handle,
      mechanism = self.mechanism.name(),
      "Selected security mechanism (uring)"
    );

    while !self.mechanism.is_complete() && !self.mechanism.is_error() {
      if let Some(token_vec) = self.mechanism.produce_token()? {
        let mut cmd_msg = Msg::from_vec(token_vec);
        cmd_msg.set_flags(MsgFlags::COMMAND);
        let encoded_token = self.encode_msg_for_send(&cmd_msg)?;
        match encoded_token {
          EncodedMsgParts::ForWriteAll(bytes) => {
            self.stream.write_all(&bytes).await?;
          }
          EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
            self.stream.write_all(&hdr).await?;
            if let Some(body) = payload_opt {
              if !body.is_empty() {
                let (res, _) = self.stream.send_zc(&body).await;
                res?;
              }
            }
          }
        }
        self.last_activity_time = Instant::now();
      }

      if !self.mechanism.is_complete() && !self.mechanism.is_error() {
        let received_msg = loop {
          match self.zmtp_manual_parser.decode_from_buffer(&mut self.read_buffer) {
            Ok(Some(msg)) => break msg,
            Ok(None) => {
              let bytes_read = self.stream.read_buf(&mut self.read_buffer).await?;
              if bytes_read == 0 {
                return Err(ZmqError::ConnectionClosed);
              }
              self.last_activity_time = Instant::now();
              continue;
            }
            Err(e) => return Err(e),
          }
        };
        if !received_msg.is_command() {
          let reason = "Expected COMMAND frame during security handshake".to_string();
          self.mechanism.set_error(reason.clone());
          return Err(ZmqError::SecurityError(reason));
        }
        self.mechanism.process_token(received_msg.data().unwrap_or_default())?;
      }
      // TODO: Implement ZAP interaction if self.mechanism.zap_request_needed()
    }
    if self.mechanism.is_error() {
      return Err(ZmqError::SecurityError(
        self.mechanism.error_reason().unwrap_or("Unknown").to_string(),
      ));
    }
    Ok(())
  }

  async fn exchange_ready_commands_uring(&mut self) -> Result<Option<Blob>, ZmqError> {
    // Operates on self.stream, self.read_buffer, self.zmtp_manual_parser
    // Similar logic to the READY part of ZmtpEngineCoreStd::perform_security_and_ready_handshake
    // Send local READY
    // Receive peer's READY (looping with self.stream.read_buf into self.read_buffer, then parse with self.zmtp_manual_parser)
    // Extract identity from peer's READY
    // Return peer's identity from READY command
    // Placeholder for brevity, but it's a direct adaptation
    let mut peer_identity_from_ready: Option<Blob> = None;
    let local_socket_type_name = self.config.socket_type_name.clone();
    let local_routing_id = self.config.routing_id.clone();

    if !self.is_server {
      let mut client_ready_props = HashMap::new();
      client_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());

      if let Some(id_blob) = local_routing_id {
        if !id_blob.is_empty() && id_blob.len() <= 255 {
          client_ready_props.insert("Identity".to_string(), id_blob.to_vec());
        }
      }

      let client_ready_cmd_msg = crate::protocol::zmtp::command::ZmtpReady::create_msg(client_ready_props);
      let encoded_ready = self.encode_msg_for_send(&client_ready_cmd_msg)?;

      match encoded_ready {
        EncodedMsgParts::ForWriteAll(bytes) => {
          self.stream.write_all(&bytes).await?;
        }
        EncodedMsgParts::ForSendZc(hdr, opt_body) => {
          self.stream.write_all(&hdr).await?;
          if let Some(b) = opt_body {
            if !b.is_empty() {
              let (res, _) = self.stream.send_zc(&b).await;
              res?;
            }
          }
        }
      }
      self.last_activity_time = Instant::now();
    }

    let peer_ready_data = loop {
      match self.zmtp_manual_parser.decode_from_buffer(&mut self.read_buffer)? {
        Some(received_msg) => {
          if !received_msg.is_command() {
            return Err(ZmqError::ProtocolViolation("Expected READY (command)".into()));
          }
          match ZmtpCommand::parse(&received_msg) {
            Some(ZmtpCommand::Ready(ready_data)) => break ready_data,
            _ => return Err(ZmqError::ProtocolViolation("Expected READY command".into())),
          }
        }
        None => {
          let n = self.stream.read_buf(&mut self.read_buffer).await?;
          if n == 0 {
            return Err(ZmqError::ConnectionClosed);
          }
          self.last_activity_time = Instant::now();
        }
      }
    };

    if let Some(id_bytes) = peer_ready_data.properties.get("Identity") {
      if !id_bytes.is_empty() && id_bytes.len() <= 255 {
        peer_identity_from_ready = Some(Blob::from(id_bytes.clone()));
      }
    }

    if self.is_server {
      let mut server_ready_props = HashMap::new();
      server_ready_props.insert("Socket-Type".to_string(), local_socket_type_name.as_bytes().to_vec());
      if let Some(id_blob) = local_routing_id {
        if !id_blob.is_empty() && id_blob.len() <= 255 {
          server_ready_props.insert("Identity".to_string(), id_blob.to_vec());
        }
      }
      let server_ready_cmd_msg = crate::protocol::zmtp::command::ZmtpReady::create_msg(server_ready_props);
      let encoded_ready = self.encode_msg_for_send(&server_ready_cmd_msg)?;
      match encoded_ready {
        EncodedMsgParts::ForWriteAll(bytes) => {
          self.stream.write_all(&bytes).await?;
        }
        EncodedMsgParts::ForSendZc(hdr, opt_body) => {
          self.stream.write_all(&hdr).await?;
          if let Some(b) = opt_body {
            if !b.is_empty() {
              let (res, _) = self.stream.send_zc(&b).await;
              res?;
            }
          }
        }
      }
      self.last_activity_time = Instant::now();
    }
    // let final_peer_id = self
    //   .mechanism
    //   .peer_identity()
    //   .map(Blob::from)
    //   .or(peer_identity_from_ready);

    // Ok(final_peer_id)
    Ok(peer_identity_from_ready)
  }

  #[cfg(target_os = "linux")]
  async fn set_tcp_cork_uring(&mut self, enable: bool) -> Result<(), io::Error> {
    if let Some(fd) = self.cork_fd {
      use std::os::fd::FromRawFd;
      let res = tokio::task::spawn_blocking(move || {
        let socket = unsafe { socket2::Socket::from_raw_fd(fd) };
        let result = socket.set_cork(enable);
        std::mem::forget(socket);
        result
      })
      .await;
      match res {
        Ok(Ok(())) => {
          self.is_corked = enable;
          tracing::debug!(
            engine_handle = self.handle,
            fd,
            cork_enable = enable,
            "TCP_CORK successfully set (uring)"
          );
          Ok(())
        }
        Ok(Err(e)) => {
          tracing::warn!(engine_handle = self.handle, fd, cork_enable = enable, error = %e, "Failed to set TCP_CORK (uring)");
          Err(e)
        }
        Err(join_err) => {
          tracing::error!(engine_handle = self.handle, fd, cork_enable = enable, error = %join_err, "Task for TCP_CORK panicked (uring)");
          Err(io::Error::new(io::ErrorKind::Other, join_err))
        }
      }
    } else {
      Ok(())
    }
  }

  fn try_arm_multishot_recv_future_uring(&mut self) {
    if !self.config.use_recv_multishot || !matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive)
    {
      return;
    }
    if let Some(ms_manager) = self.multishot_receiver_manager.as_mut() {
      if let Some(buffers_to_submit) = ms_manager.take_buffers_for_submission() {
        if !buffers_to_submit.is_empty() {
          let future = Box::pin(self.stream.recv_multishot(buffers_to_submit));
          self.active_recv_future_holder = ActiveReceiveFutureUring::UringMultishot(future);
          tracing::trace!(engine_handle = self.handle, "UringEngine: Armed recv_multishot.");
        }
      }
    }
  }

  pub async fn run_loop(mut self) {
    tokio_uring::assert_uring_available();
    let engine_actor_handle = self.handle;
    let engine_actor_type = ActorType::Engine;
    let engine_context_clone = self.context.clone();
    tracing::info!("ZmtpEngineCoreUring (handle {}) run_loop starting.", self.handle);
    let mut final_error_for_actor_stop: Option<ZmqError> = None;

    // --- Handshake Phase (operates on self.stream: UringTcpStream) ---
    // --- Handshake Phase (operates on self.stream: UringTcpStream) ---
    let handshake_and_ready_result: Result<Option<Blob> /* final peer id */, ZmqError> = async {
      // 1. Greetings (uses self.stream for read_buf/write_all)
      self.send_greeting_uring().await?; // Uses self.stream
      let peer_greeting = self.receive_greeting_uring().await?; // Uses self.stream & self.read_buffer
      self.read_buffer.clear(); // Clear after greeting, before security handshake ZMTP commands

      // 2. Security Token Exchange (uses self.stream for ZMTP command frames)
      // This sets self.mechanism
      self.perform_security_handshake_uring(peer_greeting).await?;
      self.read_buffer.clear(); // Clear after security tokens, before ZMTP READY commands

      // 3. ZMTP READY Command Exchange (uses self.stream for ZMTP command frames)
      let identity_from_ready_cmd = self.exchange_ready_commands_uring().await?;
      self.read_buffer.clear(); // Clear after READY commands, before data phase

      // 4. Determine final peer identity and set up data_cipher
      let mechanism_peer_id = self.mechanism.peer_identity(); // Get before consuming mechanism

      if self.mechanism.status() == MechanismStatus::Ready {
        let current_mechanism_box = std::mem::replace(&mut self.mechanism, Box::new(NullMechanism));
        match current_mechanism_box.into_data_cipher_parts() {
          Ok((cipher, _id_from_cipher_already_got_above)) => {
            self.data_cipher = Some(cipher);
            tracing::info!(
              engine_handle = self.handle,
              "Data cipher (uring) activated for data phase."
            );
          }
          Err(e) => {
            tracing::error!(engine_handle = self.handle, "Failed to get data cipher (uring): {}", e);
            return Err(e); // Propagate error to handshake_and_ready_result
          }
        }
      } else {
        let status = self.mechanism.status();
        let reason = self.mechanism.error_reason().unwrap_or("not specified");
        let err_msg = format!(
          "Mechanism not Ready (status: {:?}, reason: {}) after handshake.",
          status, reason
        );
        tracing::error!(engine_handle = self.handle, "{}", err_msg);
        return Err(ZmqError::SecurityError(err_msg));
      }

      Ok(mechanism_peer_id.map(Blob::from).or(identity_from_ready_cmd))
    }
    .await;

    match handshake_and_ready_result {
      Ok(final_peer_identity) => {
        if self
          .session_base_mailbox
          .send(SessionBaseCommand::EngineReady {
            peer_identity: final_peer_identity,
          })
          .await
          .is_err()
        {
          final_error_for_actor_stop = Some(ZmqError::Internal(
            "Session mailbox closed post-handshake (uring)".into(),
          ));
        } else {
          tracing::info!(
            engine_handle = self.handle,
            "ZMTP handshake & READY successful (uring). Entering main loop."
          );
          // If using multishot AND no encryption, arm it now.
          if self.config.use_recv_multishot && self.data_cipher.is_none() {
            self.try_arm_multishot_recv_future_uring();
          }
        }
      }
      Err(e) => {
        final_error_for_actor_stop = Some(e.clone());
        let _ = self
          .session_base_mailbox
          .send(SessionBaseCommand::EngineError { error: e })
          .await;
      }
    }
    self.last_activity_time = Instant::now();

    if final_error_for_actor_stop.is_none() {
      let keepalive_ping_enabled = self.heartbeat_ivl.map_or(false, |d| !d.is_zero());
      let mut ping_check_timer = if keepalive_ping_enabled {
        let ivl = self.heartbeat_ivl.unwrap();
        let freq = ivl.checked_sub(Duration::from_millis(50)).unwrap_or(ivl);
        let mut timer = tokio_interval(freq.max(Duration::from_millis(100)));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Some(timer)
      } else {
        None
      };

      let mut pong_timeout_timer = tokio_interval(self.heartbeat_timeout);
      pong_timeout_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

      'main_loop: loop {
        // Determine if specialized uring I/O can be used
        let is_encrypted_session = self.data_cipher.is_some();

        // Conditions for enabling specialized uring receive paths
        let can_poll_multishot_recv = !is_encrypted_session
          && self.config.use_recv_multishot
          && matches!(
            self.active_recv_future_holder,
            ActiveReceiveFutureUring::UringMultishot(_)
          );

        let can_use_standard_read_buf = matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive)
          && (is_encrypted_session || !self.config.use_recv_multishot)
          && self.network_bytes_buffer.capacity() > self.network_bytes_buffer.len();

        tokio::select! {
          biased;

          // --- SEND PATH (AppToUringEngineCmd::SendMsg) ---
          app_command_res = self.app_cmd_rx.recv() => {
            match app_command_res {
              Ok(AppToUringEngineCmd::SendMsg(rzmq_msg)) => {
                let original_msg_is_last_part = !rzmq_msg.is_more();

                // 1. ZMTP-frame the rzmq_msg
                //    encode_msg_for_send uses self.config.use_send_zerocopy internally to decide variant.
                let encoded_zmtp_parts = match self.encode_msg_for_send(&rzmq_msg) {
                  Ok(parts) => parts,
                  Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                };

                // 2. Encrypt if cipher active, otherwise prepare for direct send
                let wire_send_op_result: Result<(), io::Error> = async {
                  if let Some(cipher) = &mut self.data_cipher {
                    // Encrypted path: combine ZMTP parts, then encrypt
                    let plaintext_zmtp_frame_bytes = match encoded_zmtp_parts {
                      EncodedMsgParts::ForWriteAll(bytes) => bytes,
                      EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
                        let mut temp_buf = BytesMut::with_capacity(hdr.len() + payload_opt.as_ref().map_or(0, |p| p.len()));
                        temp_buf.put(hdr);
                        if let Some(p) = payload_opt { temp_buf.put(p); }
                        temp_buf.freeze()
                      }
                    };
                    let encrypted_wire_bytes = cipher.encrypt_zmtp_frame(plaintext_zmtp_frame_bytes)
                                                   .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

                    // Send encrypted blob (Noise message: [len][ciphertext+tag])
                    // Try to use send_zc for the main part of the encrypted blob.
                    if self.config.use_send_zerocopy && encrypted_wire_bytes.len() > 2 {
                      let (len_prefix, main_blob) = encrypted_wire_bytes.split_at(2);
                      self.stream.write_all(len_prefix).await?;
                      if !main_blob.is_empty() { self.stream.send_zc(&Bytes::copy_from_slice(main_blob)).await.0?; }
                    } else {
                      self.stream.write_all(&encrypted_wire_bytes).await?;
                    }
                  } else {
                    // Raw ZMTP path (no encryption)
                    match encoded_zmtp_parts {
                      EncodedMsgParts::ForWriteAll(bytes) => self.stream.write_all(&bytes).await?,
                      EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
                        self.stream.write_all(&hdr).await?;
                        if let Some(body) = payload_opt { if !body.is_empty() { self.stream.send_zc(&body).await.0?; }}
                      }
                    }
                  }
                  Ok(())
                }.await;

                // 3. TCP_CORK logic (applied before send, released after)
                #[cfg(target_os="linux")]
                if self.config.use_cork {
                  if let Some(fd) = self.cork_fd {
                    if self.expecting_first_frame_of_msg && !self.is_corked {
                      if self.set_tcp_cork_uring(true).await.is_err() {
                        final_error_for_actor_stop = Some(ZmqError::Internal("Cork set failed (send)".into()));
                        break 'main_loop;
                      }
                    }
                  }
                }

                if let Err(e) = wire_send_op_result {
                  final_error_for_actor_stop = Some(ZmqError::from(e));
                  #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() {} self.expecting_first_frame_of_msg = true; }
                  break 'main_loop;
                }

                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = original_msg_is_last_part;
                #[cfg(target_os="linux")]
                if self.is_corked && self.expecting_first_frame_of_msg {
                  if self.set_tcp_cork_uring(false).await.is_err() {
                    final_error_for_actor_stop = Some(ZmqError::Internal("Cork unset failed (send)".into()));
                    break 'main_loop;
                  }
                }
              }
              Ok(AppToUringEngineCmd::Stop) => { final_error_for_actor_stop = None; break 'main_loop; } // Clean stop
              Err(_) => { final_error_for_actor_stop = Some(ZmqError::Internal("AppCmdRx closed for Uring Engine".into())); break 'main_loop; }
            }
          }

          // --- RECEIVE PATH: Multishot (only if NO cipher and configured) ---
          multishot_res = async { /* poll self.active_recv_future_holder ... */ }, if can_poll_multishot_recv => {
            self.active_recv_future_holder = ActiveReceiveFutureUring::NotActive; // Reset before processing
            match multishot_res { // This is the BufResult from the future
              Ok(Ok((num_filled, returned_buffers))) => {
                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = true; // Received data, reset send expectation
                #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() { final_error_for_actor_stop = Some(ZmqError::Internal("Cork unset failed (ms_recv)".into())); break 'main_loop; }}

                if let Some(ms_mgr) = self.multishot_receiver_manager.as_mut() {
                  // `process_completed_submission` uses ZmtpCodec to parse Msgs from raw ZMTP frames
                  match ms_mgr.process_completed_submission(num_filled, returned_buffers) {
                    Ok(decoded_zmtp_msgs) => {
                      for msg in decoded_zmtp_msgs {
                        if self.process_decoded_message(msg, &mut final_error_for_actor_stop).await.is_err() {
                          break 'main_loop;
                        }
                      }
                    }
                    Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                  }
                } else {
                  final_error_for_actor_stop = Some(ZmqError::Internal("Multishot manager missing".into()));
                  break 'main_loop;
                }
              }
              Ok(Err(e)) => { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; } // IO error from recv_multishot
              Err(_) => { /* Future dropped/cancelled, ignore */ }
            }
            // Re-arm multishot if still no error and conditions met
            if final_error_for_actor_stop.is_none() && !is_encrypted_session && self.config.use_recv_multishot {
               self.try_arm_multishot_recv_future_uring();
            }
          }

          // --- RECEIVE PATH: Standard read_buf (used if cipher active OR multishot not configured/armed) ---
          // Reads raw bytes into `self.network_bytes_buffer`.
          // If cipher active, decrypts from `network_bytes_buffer` into `self.read_buffer`.
          // If no cipher (and multishot wasn't used for this read), moves data from `network_bytes_buffer` to `self.read_buffer`.
          // Then, `self.zmtp_manual_parser` decodes ZMTP messages from `self.read_buffer`.
          read_buf_res = self.stream.read_buf(&mut self.network_bytes_buffer), if can_use_standard_read_buf => {
            match read_buf_res {
              Ok(0) => { // EOF
                final_error_for_actor_stop = Some(ZmqError::ConnectionClosed);
                break 'main_loop;
              }
              Ok(_) => { // Some bytes read into self.network_bytes_buffer
                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = true;
                #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() { final_error_for_actor_stop = Some(ZmqError::Internal("Cork unset failed (rb_recv)".into())); break 'main_loop; }}

                if let Some(cipher) = &mut self.data_cipher {
                  // Decrypt from network_bytes_buffer, append plaintext ZMTP to self.read_buffer
                  'decrypt_loop_uring: loop {
                    match cipher.decrypt_wire_data_to_zmtp_frame(&mut self.network_bytes_buffer) {
                      Ok(Some(plaintext_zmtp_bytes)) => {
                        self.read_buffer.put(plaintext_zmtp_bytes);
                      }
                      Ok(None) => break 'decrypt_loop_uring, // Need more in network_bytes_buffer
                      Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                    }
                  }
                } else {
                  // No cipher, move data directly to ZMTP parser's input buffer (self.read_buffer)
                  self.read_buffer.extend_from_slice(&self.network_bytes_buffer);
                  self.network_bytes_buffer.clear();
                }
                if final_error_for_actor_stop.is_some() { break 'main_loop; }

                // Parse ZMTP from self.read_buffer
                'parse_loop_uring: loop {
                  match self.zmtp_manual_parser.decode_from_buffer(&mut self.read_buffer) {
                    Ok(Some(msg)) => {
                      if self.process_decoded_message(msg, &mut final_error_for_actor_stop).await.is_err() {
                        break 'main_loop;
                      }
                    }
                    Ok(None) => break 'parse_loop_uring, // Need more in self.read_buffer
                    Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                  }
                }
                if final_error_for_actor_stop.is_some() { break 'main_loop; }

                // Optional buffer compaction for network_bytes_buffer and read_buffer
                // if self.network_bytes_buffer.capacity() > ... { self.network_bytes_buffer = BytesMut::with_capacity(...); }

                // If multishot is configured but wasn't used (e.g. because cipher was active,
                // but now isn't, or if it just wasn't armed), try to arm it for next time.
                if !is_encrypted_session && self.config.use_recv_multishot && matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive) {
                  self.try_arm_multishot_recv_future_uring();
                }
              }
              Err(e) => { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; }
            }
          }

          // --- Heartbeat PING Timer ---
          _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong && ping_check_timer.is_some() => {
            let now = Instant::now();
            if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
              let ping_to_send_msg = ZmtpCommand::create_ping(0, b"");
              // ZMTP-frame (encode_msg_for_send will produce ForWriteAll for commands)
              let encoded_ping_parts = match self.encode_msg_for_send(&ping_to_send_msg) {
                  Ok(parts) => parts, Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
              };
              let plaintext_ping_bytes = match encoded_ping_parts {
                  EncodedMsgParts::ForWriteAll(b) => b,
                  EncodedMsgParts::ForSendZc(h, p_opt) => { // Should not happen for commands, but handle
                      let mut temp = BytesMut::new(); temp.put(h); if let Some(p) = p_opt { temp.put(p); } temp.freeze()
                  }
              };

              // Encrypt if cipher active
              let wire_bytes_ping = if let Some(cipher) = &mut self.data_cipher {
                match cipher.encrypt_zmtp_frame(plaintext_ping_bytes) {
                  Ok(eb) => eb, Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                }
              } else { plaintext_ping_bytes };

              // Corking logic
              #[cfg(target_os="linux")]
              if self.config.use_cork {
                if let Some(fd) = self.cork_fd { if !self.is_corked { if self.set_tcp_cork_uring(true).await.is_err() { final_error_for_actor_stop=Some(ZmqError::Internal("cork err".into())); break 'main_loop; }}}}

              // Send PING (potentially ZC for encrypted blob)
              let ping_send_op_result: Result<(), io::Error> = async {
                  if self.data_cipher.is_some() && self.config.use_send_zerocopy && wire_bytes_ping.len() > 2 {
                      let (len_prefix, main_blob) = wire_bytes_ping.split_at(2);
                      self.stream.write_all(len_prefix).await?;
                      if !main_blob.is_empty() { self.stream.send_zc(&Bytes::copy_from_slice(main_blob)).await.0?; }
                  } else {
                      self.stream.write_all(&wire_bytes_ping).await?;
                  }
                  Ok(())
              }.await;

              if let Err(e) = ping_send_op_result {
                final_error_for_actor_stop = Some(ZmqError::from(e));
                #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() {}}
                break 'main_loop;
              }

              self.last_activity_time = now; self.last_ping_sent_time = Some(now);
              self.waiting_for_pong = true; pong_timeout_timer.reset();

              #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() { final_error_for_actor_stop=Some(ZmqError::Internal("cork err".into())); break 'main_loop;} }
              self.expecting_first_frame_of_msg = true; // PING is a complete logical message
            }
          }

          // --- PONG Timeout ---
          _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
            final_error_for_actor_stop = Some(ZmqError::Timeout);
            #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() {} self.expecting_first_frame_of_msg = true;}
            break 'main_loop;
          }
        } // end select!
      }
    }

    tracing::info!(
      "ZmtpEngineCoreUring (handle {}) loop finished. Cleaning up.",
      self.handle
    );

    #[cfg(target_os = "linux")]
    if self.is_corked {
      let _ = self.set_tcp_cork_uring(false).await;
    }

    if final_error_for_actor_stop.is_none() {
      let _ = self.session_base_mailbox.send(SessionBaseCommand::EngineStopped).await;
    } else if let Some(err) = &final_error_for_actor_stop {
      let _ = self
        .session_base_mailbox
        .send(SessionBaseCommand::EngineError { error: err.clone() })
        .await;
    }

    // Engine owns self.stream (UringTcpStream) directly. Close it.
    // If active_recv_future_holder holds a future, it needs to be dropped/cancelled before stream close.
    self.active_recv_future_holder = ActiveReceiveFutureUring::NotActive; // Drop any active future

    let _ = self.stream.shutdown(std::net::Shutdown::Both); // More graceful than just close sometimes
    let close_res = self.stream.close().await; // Close the uring stream
    if let Err(e) = close_res {
      tracing::warn!(engine_handle = self.handle, error = %e, "Error closing UringTcpStream.");
    }

    engine_context_clone.publish_actor_stopping(
      engine_actor_handle,
      engine_actor_type,
      None,
      final_error_for_actor_stop,
    );
    tracing::info!(
      "ZmtpEngineCoreUring (handle {}) run_loop task fully stopped.",
      self.handle
    );
  }

  async fn process_decoded_message(&mut self, msg: Msg, final_error: &mut Option<ZmqError>) -> Result<(), ()> {
    if msg.is_command() {
      if let Some(cmd) = ZmtpCommand::parse(&msg) {
        match cmd {
          ZmtpCommand::Ping(ping_context) => {
            let pong_reply = ZmtpCommand::create_pong(&ping_context);
            // <<< MODIFIED [Corrected PONG send logic] >>>
            let send_pong_op_result: Result<(), ZmqError> = async {
              let encoded = self.encode_msg_for_send(&pong_reply)?;
              match encoded {
                EncodedMsgParts::ForWriteAll(bytes) => self.stream.write_all(&bytes).await.map_err(ZmqError::from),
                EncodedMsgParts::ForSendZc(hdr, opt_body) => {
                  self.stream.write_all(&hdr).await.map_err(ZmqError::from)?;
                  if let Some(body) = opt_body {
                    if !body.is_empty() {
                      let (res, _) = self.stream.send_zc(&body).await;
                      res.map_err(ZmqError::from)?;
                    }
                  }
                  Ok(())
                }
              }
            }
            .await;
            // <<< MODIFIED END >>>
            if let Err(e) = send_pong_op_result {
              tracing::error!(engine_handle = self.handle, error = %e, "Failed to send PONG (uring).");
              *final_error = Some(e);
              return Err(());
            }
            self.last_activity_time = Instant::now();
          }
          ZmtpCommand::Pong(_) => {
            if self.waiting_for_pong {
              self.waiting_for_pong = false;
              self.last_ping_sent_time = None;
            } else {
              tracing::warn!(engine_handle = self.handle, "Unexpected PONG (uring).");
            }
            self.last_activity_time = Instant::now();
          }
          ZmtpCommand::Error => {
            tracing::warn!(engine_handle = self.handle, "Peer sent ZMTP ERROR (uring).");
            *final_error = Some(ZmqError::ProtocolViolation("Peer sent ZMTP ERROR".into()));
            return Err(());
          }
          _ => { /* Ignore other commands */ }
        }
      }
    } else {
      if self
        .session_base_mailbox
        .send(SessionBaseCommand::EnginePushCmd { msg })
        .await
        .is_err()
      {
        tracing::error!(engine_handle = self.handle, "Session mailbox closed (uring data).");
        *final_error = Some(ZmqError::Internal("Session mailbox closed for uring data".into()));
        return Err(());
      }
    }
    Ok(())
  }
}

#[derive(Debug)]
pub struct UringLaunchInformation {
  pub engine_handle_id: usize,
  pub owned_fd: OwnedFd,
  pub config: ZmtpEngineConfig,
  pub is_server: bool,
  pub context_clone: Context,
  pub session_base_mailbox: MailboxSender,
  pub app_to_engine_cmd_rx: MailboxReceiver<AppToUringEngineCmd>,
  pub parent_session_handle_id: usize,
}
