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
use crate::runtime::mailbox::URingMailboxReceiver;
use crate::runtime::{ActorType, Command as SessionBaseCommand, MailboxReceiver, MailboxSender};
#[cfg(feature = "noise_xx")]
use crate::security::NoiseXxMechanism;
use crate::security::{null::NullMechanism, plain::PlainMechanism, IDataCipher, Mechanism, MechanismStatus, negotiate_security_mechanism};
use crate::socket::options::ZmtpEngineConfig;
use crate::Blob;

use super::uring_recv::UringMultishotReceiver;
use tokio_uring::BufResult;

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::io::OwnedFd;
use std::pin::Pin;
use std::time::{Duration, Instant};
use std::{fmt, io};

use tokio::io::{AsyncWriteExt as TokioAsyncWriteExt}; 
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

pub(crate) enum ActiveReceiveFutureUring {
  UringMultishot(Pin<Box<dyn Future<Output = BufResult<usize, Vec<BytesMut>>> + Send>>),
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
  app_cmd_rx: URingMailboxReceiver,
  stream: UringTcpStream,
  config: ZmtpEngineConfig,
  is_server: bool,
  mechanism: Box<dyn Mechanism>,
  data_cipher: Option<Box<dyn IDataCipher>>,
  network_bytes_buffer: BytesMut, 
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
    app_cmd_rx: URingMailboxReceiver,
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
      network_bytes_buffer: BytesMut::with_capacity(8192 * 2), 
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
    let mut greeting_buf_bytes_mut = BytesMut::with_capacity(GREETING_LENGTH);

    let own_greeting_mechanism_bytes = {
      if self.config.use_noise_xx {
        let can_propose_noise = if self.is_server {
          self.config.noise_xx_local_sk_bytes_for_engine.is_some()
        } else {
          // Client
          self.config.noise_xx_local_sk_bytes_for_engine.is_some()
            && self.config.noise_xx_remote_pk_bytes_for_engine.is_some()
        };

        if can_propose_noise {
          tracing::debug!(
            engine_handle = self.handle,
            "Uring: Proposing NOISE_XX in own greeting."
          );
          NoiseXxMechanism::NAME_BYTES
        } else {
          tracing::warn!(
            engine_handle = self.handle,
            "Uring: Noise_XX configured but required keys missing. Falling back."
          );
          if self.config.use_plain {
            tracing::debug!(
              engine_handle = self.handle,
              "Uring: Proposing PLAIN in own greeting (Noise fallback)."
            );
            PlainMechanism::NAME_BYTES
          } else {
            tracing::debug!(
              engine_handle = self.handle,
              "Uring: Proposing NULL in own greeting (Noise fallback, PLAIN disabled)."
            );
            NullMechanism::NAME_BYTES
          }
        }
      } else if self.config.use_plain {
        tracing::debug!(engine_handle = self.handle, "Uring: Proposing PLAIN in own greeting.");
        PlainMechanism::NAME_BYTES
      } else {
        tracing::debug!(engine_handle = self.handle, "Uring: Proposing NULL in own greeting.");
        NullMechanism::NAME_BYTES
      }
      #[cfg(not(feature = "noise_xx"))] 
      {
        if self.config.use_plain {
          tracing::debug!(
            engine_handle = self.handle,
            "Uring: Proposing PLAIN in own greeting (NOISE_XX feature disabled)."
          );
          PlainMechanism::NAME_BYTES
        } else {
          tracing::debug!(
            engine_handle = self.handle,
            "Uring: Proposing NULL in own greeting (NOISE_XX feature disabled, PLAIN disabled)."
          );
          NullMechanism::NAME_BYTES
        }
      }
    };

    ZmtpGreeting::encode(&own_greeting_mechanism_bytes, self.is_server, &mut greeting_buf_bytes_mut);
    // <<< MODIFIED START [Convert BytesMut to Vec<u8> for write_all] >>>
    let (res, _) = self.stream.write_all(greeting_buf_bytes_mut.to_vec()).await;
    res?;
    // <<< MODIFIED END >>>
    tracing::debug!(engine_handle = self.handle, "Sent own ZMTP greeting (uring).");
    self.last_activity_time = Instant::now();
    Ok(())
  }

  async fn receive_greeting_uring(&mut self) -> Result<ZmtpGreeting, ZmqError> {
    let mut received_greeting_bytes = BytesMut::with_capacity(GREETING_LENGTH);
    while received_greeting_bytes.len() < GREETING_LENGTH {
      let remaining_to_read = GREETING_LENGTH - received_greeting_bytes.len();
      let mut temp_buf = vec![0u8; remaining_to_read.min(1024)]; 

      let (res, temp_buf_after_read) = self.stream.read(temp_buf).await;
      let bytes_read = res?;
      
      if bytes_read == 0 {
        return Err(ZmqError::ConnectionClosed);
      }
      received_greeting_bytes.put_slice(&temp_buf_after_read[..bytes_read]);
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

    self.mechanism = negotiate_security_mechanism(
      self.is_server,
      &self.config,
      &peer_greeting,
      self.handle,
    )?;

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
            // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
            let (res, _) = self.stream.write_all(bytes.to_vec()).await;
            res?;
            // <<< MODIFIED END >>>
          }
          EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
            // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
            let (res_hdr, _) = self.stream.write_all(hdr.to_vec()).await;
            res_hdr?;
            // <<< MODIFIED END >>>
            if let Some(body) = payload_opt {
              if !body.is_empty() {
                let (res_body, _) = self.stream.send_zc(&body).await;
                res_body?;
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
              let mut temp_read_buf = vec![0u8; 4096]; 
              let (res, temp_read_buf_after_read) = self.stream.read(temp_read_buf).await;
              let bytes_read = res?;
              if bytes_read == 0 {
                return Err(ZmqError::ConnectionClosed);
              }
              self.read_buffer.put_slice(&temp_read_buf_after_read[..bytes_read]); 
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
    }
    if self.mechanism.is_error() {
      return Err(ZmqError::SecurityError(
        self.mechanism.error_reason().unwrap_or("Unknown").to_string(),
      ));
    }
    Ok(())
  }

  async fn exchange_ready_commands_uring(&mut self) -> Result<Option<Blob>, ZmqError> {
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
          // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
          let (res, _) = self.stream.write_all(bytes.to_vec()).await;
          res?;
          // <<< MODIFIED END >>>
        }
        EncodedMsgParts::ForSendZc(hdr, opt_body) => {
          // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
          let (res_hdr, _) = self.stream.write_all(hdr.to_vec()).await;
          res_hdr?;
          // <<< MODIFIED END >>>
          if let Some(b) = opt_body {
            if !b.is_empty() {
              let (res_body, _) = self.stream.send_zc(&b).await;
              res_body?;
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
          let mut temp_read_buf = vec![0u8; 4096];
          let (res, temp_read_buf_after_read) = self.stream.read(temp_read_buf).await;
          let n = res?;
          if n == 0 {
            return Err(ZmqError::ConnectionClosed);
          }
          self.read_buffer.put_slice(&temp_read_buf_after_read[..n]);
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
      if let Some(id_blob) = self.config.routing_id.clone() { 
        if !id_blob.is_empty() && id_blob.len() <= 255 {
          server_ready_props.insert("Identity".to_string(), id_blob.to_vec());
        }
      }
      let server_ready_cmd_msg = crate::protocol::zmtp::command::ZmtpReady::create_msg(server_ready_props);
      let encoded_ready = self.encode_msg_for_send(&server_ready_cmd_msg)?;
      match encoded_ready {
        EncodedMsgParts::ForWriteAll(bytes) => {
          // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
          let (res, _) = self.stream.write_all(bytes.to_vec()).await;
          res?;
          // <<< MODIFIED END >>>
        }
        EncodedMsgParts::ForSendZc(hdr, opt_body) => {
          // <<< MODIFIED START [Convert Bytes to Vec<u8> for write_all] >>>
          let (res_hdr, _) = self.stream.write_all(hdr.to_vec()).await;
          res_hdr?;
          // <<< MODIFIED END >>>
          if let Some(b) = opt_body {
            if !b.is_empty() {
              let (res_body, _) = self.stream.send_zc(&b).await;
              res_body?;
            }
          }
        }
      }
      self.last_activity_time = Instant::now();
    }
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

    let handshake_and_ready_result: Result<Option<Blob>, ZmqError> = async {
      self.send_greeting_uring().await?; 
      let peer_greeting = self.receive_greeting_uring().await?; 
      self.read_buffer.clear(); 

      self.perform_security_handshake_uring(peer_greeting).await?;
      self.read_buffer.clear(); 

      let identity_from_ready_cmd = self.exchange_ready_commands_uring().await?;
      self.read_buffer.clear(); 

      let mechanism_peer_id = self.mechanism.peer_identity(); 

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
            return Err(e); 
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
        let is_encrypted_session = self.data_cipher.is_some();
        let can_poll_multishot_recv = !is_encrypted_session
          && self.config.use_recv_multishot
          && matches!(
            self.active_recv_future_holder,
            ActiveReceiveFutureUring::UringMultishot(_)
          );

        let can_use_standard_read = matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive)
          && (is_encrypted_session || !self.config.use_recv_multishot)
          && self.network_bytes_buffer.capacity() > self.network_bytes_buffer.len();

        tokio::select! {
          biased;
          app_command_res = self.app_cmd_rx.recv() => {
            match app_command_res {
              Ok(AppToUringEngineCmd::SendMsg(rzmq_msg)) => {
                let original_msg_is_last_part = !rzmq_msg.is_more();
                let encoded_zmtp_parts = match self.encode_msg_for_send(&rzmq_msg) {
                  Ok(parts) => parts,
                  Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                };

                let wire_send_op_result: Result<(), io::Error> = async {
                  if let Some(cipher) = &mut self.data_cipher {
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

                    if self.config.use_send_zerocopy && encrypted_wire_bytes.len() > 2 {
                      let (len_prefix, main_blob) = encrypted_wire_bytes.split_at(2);
                      // <<< MODIFIED START [Convert slice to Vec for write_all] >>>
                      self.stream.write_all(len_prefix.to_vec()).await.0?;
                      // <<< MODIFIED END >>>
                      if !main_blob.is_empty() { self.stream.send_zc(&Bytes::copy_from_slice(main_blob)).await.0?; }
                    } else {
                      // <<< MODIFIED START [Convert Bytes to Vec for write_all] >>>
                      self.stream.write_all(encrypted_wire_bytes.to_vec()).await.0?;
                      // <<< MODIFIED END >>>
                    }
                  } else {
                    match encoded_zmtp_parts {
                      // <<< MODIFIED START [Convert Bytes to Vec for write_all] >>>
                      EncodedMsgParts::ForWriteAll(bytes) => self.stream.write_all(bytes.to_vec()).await.0?,
                      EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
                        self.stream.write_all(hdr.to_vec()).await.0?;
                        if let Some(body) = payload_opt { if !body.is_empty() { self.stream.send_zc(&body).await.0?; }}
                      }
                      // <<< MODIFIED END >>>
                    }
                  }
                  Ok(())
                }.await;

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
              Ok(AppToUringEngineCmd::Stop) => { final_error_for_actor_stop = None; break 'main_loop; } 
              Err(_) => { final_error_for_actor_stop = Some(ZmqError::Internal("AppCmdRx closed for Uring Engine".into())); break 'main_loop; }
            }
          }

          multishot_res = async { 
            let future_to_poll = match std::mem::replace(&mut self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive) {
                ActiveReceiveFutureUring::UringMultishot(fut) => fut,
                ActiveReceiveFutureUring::NotActive => {
                    tracing::error!(engine_handle = self.handle, "UringEngine: Multishot future was NotActive unexpectedly.");
                    // This return will be caught by the match below
                    return Err(io::Error::new(io::ErrorKind::Other, "Multishot future vanished"));
                }
            };
            future_to_poll.await // This is BufResult<usize, Vec<BytesMut>>
          }, if can_poll_multishot_recv => {
            match multishot_res { 
              Ok((num_filled, returned_buffers)) => { 
                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = true; 
                #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() { final_error_for_actor_stop = Some(ZmqError::Internal("Cork unset failed (ms_recv)".into())); break 'main_loop; }}

                if let Some(ms_mgr) = self.multishot_receiver_manager.as_mut() {
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
              Err(e) => { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; } 
            }
            if final_error_for_actor_stop.is_none() && !is_encrypted_session && self.config.use_recv_multishot {
               self.try_arm_multishot_recv_future_uring();
            }
          }
          
          read_res_tuple = async {
            let mut temp_read_buf = vec![0u8; self.network_bytes_buffer.capacity().saturating_sub(self.network_bytes_buffer.len()).max(4096)]; 
            self.stream.read(temp_read_buf).await 
          }, if can_use_standard_read => { 
            let (read_result_io, temp_buf_after_read) = read_res_tuple; 
            match read_result_io {
              Ok(0) => { 
                final_error_for_actor_stop = Some(ZmqError::ConnectionClosed);
                break 'main_loop;
              }
              Ok(bytes_read) => { 
                self.network_bytes_buffer.put_slice(&temp_buf_after_read[..bytes_read]); 
                self.last_activity_time = Instant::now();
                self.expecting_first_frame_of_msg = true;
                #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() { final_error_for_actor_stop = Some(ZmqError::Internal("Cork unset failed (rb_recv)".into())); break 'main_loop; }}

                if let Some(cipher) = &mut self.data_cipher {
                  'decrypt_loop_uring: loop {
                    match cipher.decrypt_wire_data_to_zmtp_frame(&mut self.network_bytes_buffer) {
                      Ok(Some(plaintext_zmtp_bytes)) => {
                        self.read_buffer.put(plaintext_zmtp_bytes);
                      }
                      Ok(None) => break 'decrypt_loop_uring, 
                      Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                    }
                  }
                } else {
                  self.read_buffer.extend_from_slice(&self.network_bytes_buffer);
                  self.network_bytes_buffer.clear();
                }
                if final_error_for_actor_stop.is_some() { break 'main_loop; }

                'parse_loop_uring: loop {
                  match self.zmtp_manual_parser.decode_from_buffer(&mut self.read_buffer) {
                    Ok(Some(msg)) => {
                      if self.process_decoded_message(msg, &mut final_error_for_actor_stop).await.is_err() {
                        break 'main_loop;
                      }
                    }
                    Ok(None) => break 'parse_loop_uring, 
                    Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                  }
                }
                if final_error_for_actor_stop.is_some() { break 'main_loop; }
                if !is_encrypted_session && self.config.use_recv_multishot && matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive) {
                  self.try_arm_multishot_recv_future_uring();
                }
              }
              Err(e) => { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; }
            }
          }

          _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong && ping_check_timer.is_some() => {
            let now = Instant::now();
            if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
              let ping_to_send_msg = ZmtpCommand::create_ping(0, b"");
              let encoded_ping_parts = match self.encode_msg_for_send(&ping_to_send_msg) {
                  Ok(parts) => parts, Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
              };
              let plaintext_ping_bytes = match encoded_ping_parts {
                  EncodedMsgParts::ForWriteAll(b) => b,
                  EncodedMsgParts::ForSendZc(h, p_opt) => { 
                      let mut temp = BytesMut::new(); temp.put(h); if let Some(p) = p_opt { temp.put(p); } temp.freeze()
                  }
              };

              let wire_bytes_ping = if let Some(cipher) = &mut self.data_cipher {
                match cipher.encrypt_zmtp_frame(plaintext_ping_bytes) {
                  Ok(eb) => eb, Err(e) => { final_error_for_actor_stop = Some(e); break 'main_loop; }
                }
              } else { plaintext_ping_bytes };

              #[cfg(target_os="linux")]
              if self.config.use_cork {
                if let Some(fd) = self.cork_fd { if !self.is_corked { if self.set_tcp_cork_uring(true).await.is_err() { final_error_for_actor_stop=Some(ZmqError::Internal("cork err".into())); break 'main_loop; }}}}

              let ping_send_op_result: Result<(), io::Error> = async {
                  if self.data_cipher.is_some() && self.config.use_send_zerocopy && wire_bytes_ping.len() > 2 {
                      let (len_prefix, main_blob) = wire_bytes_ping.split_at(2);
                      // <<< MODIFIED START [Convert slice to Vec for write_all] >>>
                      self.stream.write_all(len_prefix.to_vec()).await.0?;
                      // <<< MODIFIED END >>>
                      if !main_blob.is_empty() { self.stream.send_zc(&Bytes::copy_from_slice(main_blob)).await.0?; }
                  } else {
                      // <<< MODIFIED START [Convert Bytes to Vec for write_all] >>>
                      self.stream.write_all(wire_bytes_ping.to_vec()).await.0?;
                      // <<< MODIFIED END >>>
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
              self.expecting_first_frame_of_msg = true; 
            }
          }

          _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
            final_error_for_actor_stop = Some(ZmqError::Timeout);
            #[cfg(target_os="linux")] if self.is_corked { if self.set_tcp_cork_uring(false).await.is_err() {} self.expecting_first_frame_of_msg = true;}
            break 'main_loop;
          }
        } 
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
    self.active_recv_future_holder = ActiveReceiveFutureUring::NotActive; 

    let _ = self.stream.shutdown(std::net::Shutdown::Both); 
    let close_res = self.stream.close().await; 
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
            let send_pong_op_result: Result<(), ZmqError> = async {
              let encoded = self.encode_msg_for_send(&pong_reply)?;
              let plaintext_pong_bytes = match encoded {
                EncodedMsgParts::ForWriteAll(b) => b,
                EncodedMsgParts::ForSendZc(h, p_opt) => {
                  let mut temp = BytesMut::new();
                  temp.put(h);
                  if let Some(p) = p_opt {
                    temp.put(p);
                  }
                  temp.freeze()
                }
              };

              let wire_bytes_pong = if let Some(cipher) = &mut self.data_cipher {
                cipher
                  .encrypt_zmtp_frame(plaintext_pong_bytes)
                  .map_err(|e| ZmqError::SecurityError(format!("PONG encrypt failed: {}", e)))?
              } else {
                plaintext_pong_bytes
              };

              if self.data_cipher.is_some() && self.config.use_send_zerocopy && wire_bytes_pong.len() > 2 {
                let (len_prefix, main_blob) = wire_bytes_pong.split_at(2);
                // <<< MODIFIED START [Convert slice to Vec for write_all] >>>
                self.stream.write_all(len_prefix.to_vec()).await.0.map_err(ZmqError::from)?;
                // <<< MODIFIED END >>>
                if !main_blob.is_empty() {
                  self
                    .stream
                    .send_zc(&Bytes::copy_from_slice(main_blob))
                    .await
                    .0
                    .map_err(ZmqError::from)?;
                }
              } else {
                // <<< MODIFIED START [Convert Bytes to Vec for write_all] >>>
                self.stream.write_all(wire_bytes_pong.to_vec()).await.0.map_err(ZmqError::from)?;
                // <<< MODIFIED END >>>
              }
            }
            .await;
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
  pub app_to_engine_cmd_rx: URingMailboxReceiver,
  pub parent_session_handle_id: usize,
}