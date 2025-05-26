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
#[cfg(feature = "curve")]
use crate::security::CurveMechanism;
use crate::security::{CurveMechanism, Mechanism, MechanismStatus, NullMechanism, PlainMechanism};
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
  mechanism: Box<dyn Mechanism>,
  is_server: bool,
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
      mechanism: Box::new(NullMechanism),
      is_server,
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
      #[cfg(feature = "curve")]
      CurveMechanism::NAME => Box::new(CurveMechanism::new(self.is_server)),
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
        }
      }
      self.last_activity_time = Instant::now();
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
    let final_peer_id = self
      .mechanism
      .peer_identity()
      .map(Blob::from)
      .or(peer_identity_from_ready);
    Ok(final_peer_id)
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

    let handshake_res: Result<Option<Blob>, ZmqError> = async {
      self.send_greeting_uring().await?;
      let peer_greeting = self.receive_greeting_uring().await?;
      self.active_recv_future_holder = ActiveReceiveFutureUring::NotActive;
      self.perform_security_handshake_uring(peer_greeting).await?;
      self.exchange_ready_commands_uring().await
    }
    .await;

    match handshake_res {
      Ok(peer_identity) => {
        if self
          .session_base_mailbox
          .send(SessionBaseCommand::EngineReady { peer_identity })
          .await
          .is_err()
        {
          final_error_for_actor_stop = Some(ZmqError::Internal(
            "Session mailbox closed post-handshake (uring)".into(),
          ));
        } else {
          tracing::info!(engine_handle = self.handle, "ZMTP handshake successful (uring).");
          if self.config.use_recv_multishot {
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
        let can_read_buf = !self.config.use_recv_multishot
          || matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive);
        let can_poll_multishot = self.config.use_recv_multishot
          && matches!(
            self.active_recv_future_holder,
            ActiveReceiveFutureUring::UringMultishot(_)
          );

        tokio::select! {
            biased;

            app_command_res = self.app_cmd_rx.recv() => {
                match app_command_res {
                    Ok(AppToUringEngineCmd::SendMsg(rzmq_msg)) => {
                        let is_last = !rzmq_msg.is_more();
                        #[cfg(target_os="linux")]
                        if self.config.use_cork && self.expecting_first_frame_of_msg && !self.is_corked {
                            if let Err(e) = self.set_tcp_cork_uring(true).await {
                                final_error_for_actor_stop = Some(ZmqError::from(e));
                                break 'main_loop;
                            }
                        }
                        // <<< MODIFIED [Corrected send logic placement] >>>
                        let send_op_result: Result<(), ZmqError> = async {
                            match self.encode_msg_for_send(&rzmq_msg)? {
                                EncodedMsgParts::ForWriteAll(bytes) => {
                                    self.stream.write_all(&bytes).await?;
                                    // No explicit flush for uring unless TCP_CORK is managed.
                                }
                                EncodedMsgParts::ForSendZc(hdr, payload_opt) => {
                                    self.stream.write_all(&hdr).await?;
                                    if let Some(body) = payload_opt {
                                        if !body.is_empty() {
                                            let (zc_res, _) = self.stream.send_zc(&body).await;
                                            zc_res?;
                                        }
                                    }
                                    // No explicit flush for uring unless TCP_CORK is managed.
                                }
                            }
                            Ok(())
                        }.await;

                        if let Err(e) = send_op_result {
                            final_error_for_actor_stop = Some(e); // ZmqError from conversion or direct io::Error wrapped
                            break 'main_loop;
                        }
                        // <<< MODIFIED END >>>

                        self.last_activity_time = Instant::now();
                        self.expecting_first_frame_of_msg = is_last;
                        #[cfg(target_os="linux")]
                        if self.is_corked && self.expecting_first_frame_of_msg {
                            if let Err(e) = self.set_tcp_cork_uring(false).await {
                                final_error_for_actor_stop = Some(ZmqError::from(e));
                                break 'main_loop;
                            }
                        }
                    }
                    Ok(AppToUringEngineCmd::Stop) => { break 'main_loop; }
                    Err(_) => {
                        if final_error_for_actor_stop.is_none() { final_error_for_actor_stop = Some(ZmqError::Internal("AppChanClosed".into()));}
                        break 'main_loop;
                    }
                }
            }

            read_res = self.stream.read_buf(&mut self.read_buffer), if can_read_buf && self.read_buffer.capacity() > self.read_buffer.len() => {
                match read_res {
                    Ok(0) => { if final_error_for_actor_stop.is_none() {final_error_for_actor_stop=Some(ZmqError::ConnectionClosed);} break 'main_loop; }
                    Ok(_) => {
                        self.last_activity_time = Instant::now();
                        self.expecting_first_frame_of_msg = true;
                        #[cfg(target_os="linux")] if self.is_corked { if let Err(e) = self.set_tcp_cork_uring(false).await { final_error_for_actor_stop=Some(ZmqError::from(e)); break 'main_loop;}}
                        'parse_loop_std: loop {
                            match self.zmtp_manual_parser.decode_from_buffer(&mut self.read_buffer) {
                                Ok(Some(msg)) => { if self.process_decoded_message(msg, &mut final_error_for_actor_stop).await.is_err() { break 'main_loop; } }
                                Ok(None) => break 'parse_loop_std,
                                Err(e) => { final_error_for_actor_stop=Some(e); break 'main_loop; }
                            }
                        }
                        if self.read_buffer.capacity() > 16384 && self.read_buffer.len() < self.read_buffer.capacity()/4 { self.read_buffer.shrink_to_fit(); } // Or shrink_to to a reasonable size
                        if self.config.use_recv_multishot && matches!(self.active_recv_future_holder, ActiveReceiveFutureUring::NotActive) { self.try_arm_multishot_recv_future_uring(); }
                    }
                    Err(e) => { final_error_for_actor_stop=Some(ZmqError::from(e)); break 'main_loop; }
                }
            }

            multishot_completion_res = async {
                match &mut self.active_recv_future_holder {
                    ActiveReceiveFutureUring::UringMultishot(fut) => Ok(fut.await),
                    ActiveReceiveFutureUring::NotActive => Err(()),
                }
            }, if can_poll_multishot => {
                self.active_recv_future_holder = ActiveReceiveFutureUring::NotActive;
                match multishot_completion_res {
                    Ok(Ok((num_filled, returned_buffers))) => {
                        self.last_activity_time = Instant::now();
                        self.expecting_first_frame_of_msg = true;
                        #[cfg(target_os="linux")] if self.is_corked { if let Err(e) = self.set_tcp_cork_uring(false).await { final_error_for_actor_stop=Some(ZmqError::from(e)); break 'main_loop;}}
                        if let Some(ms_mgr) = self.multishot_receiver_manager.as_mut() {
                            match ms_mgr.process_completed_submission(num_filled, returned_buffers) {
                                Ok(msgs) => { for msg in msgs { if self.process_decoded_message(msg, &mut final_error_for_actor_stop).await.is_err() { break 'main_loop; }}}
                                Err(e) => { final_error_for_actor_stop=Some(e); break 'main_loop; }
                            }
                        } else { final_error_for_actor_stop=Some(ZmqError::Internal("ms_mgr None".into())); break 'main_loop; }
                    }
                    Ok(Err(e)) => { final_error_for_actor_stop=Some(ZmqError::from(e)); break 'main_loop; }
                    Err(_) => {}
                }
                if final_error_for_actor_stop.is_none() { self.try_arm_multishot_recv_future_uring(); }
            }

            _ = async { ping_check_timer.as_mut().unwrap().tick().await }, if keepalive_ping_enabled && !self.waiting_for_pong && ping_check_timer.is_some() => {
                let now = Instant::now();
                if now.duration_since(self.last_activity_time) >= self.heartbeat_ivl.unwrap() {
                     let ping_to_send = ZmtpCommand::create_ping(0, b"");
                     #[cfg(target_os = "linux")] if self.config.use_cork && !self.is_corked { if let Err(e) = self.set_tcp_cork_uring(true).await { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; } }
                     // <<< MODIFIED [Corrected PING send logic] >>>
                     let send_ping_op_result: Result<(), ZmqError> = async {
                         match self.encode_msg_for_send(&ping_to_send)? {
                            EncodedMsgParts::ForWriteAll(bytes) => self.stream.write_all(&bytes).await.map_err(ZmqError::from),
                            EncodedMsgParts::ForSendZc(hdr, opt_body) => {
                                self.stream.write_all(&hdr).await.map_err(ZmqError::from)?;
                                if let Some(body) = opt_body { if !body.is_empty() { let (res,_) = self.stream.send_zc(&body).await; res.map_err(ZmqError::from)?; }}
                                Ok(())
                            }
                         }
                     }.await;
                     // <<< MODIFIED END >>>
                     if let Err(e) = send_ping_op_result {
                        final_error_for_actor_stop=Some(e); break 'main_loop;
                     }
                     self.last_activity_time = now; self.last_ping_sent_time = Some(now); self.waiting_for_pong = true; pong_timeout_timer.reset();
                     #[cfg(target_os = "linux")] if self.is_corked { if let Err(e) = self.set_tcp_cork_uring(false).await { final_error_for_actor_stop = Some(ZmqError::from(e)); break 'main_loop; } }
                     self.expecting_first_frame_of_msg = true;
                }
            }
            _ = pong_timeout_timer.tick(), if self.waiting_for_pong => {
                final_error_for_actor_stop = Some(ZmqError::Timeout); break 'main_loop;
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
    let _ = self.stream.shutdown(std::net::Shutdown::Both); // More graceful than just close sometimes
    let _ = self.stream.close().await; // Close the uring stream
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
