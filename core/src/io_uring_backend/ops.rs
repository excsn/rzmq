// core/src/io_uring_backend/ops.rs

#![cfg(feature = "io-uring")]

use crate::message::Msg;
use crate::socket::ZmtpEngineConfig;
use crate::ZmqError;

use std::fmt;
use std::net::SocketAddr;
use std::os::unix::io::RawFd;
use std::sync::Arc;

use fibre::{mpsc, oneshot};

pub const HANDLER_INTERNAL_SEND_OP_UD: UserData = 0;

#[derive(Clone, Debug)]
pub(crate) enum ProtocolConfig {
  Zmtp(Arc<ZmtpEngineConfig>),
  // Example: Http(Arc<HttpConfig>),
}

pub type UserData = u64;

#[derive(Clone)]
pub enum UringOpRequest {
  Nop {
    user_data: UserData,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  InitializeBufferRing {
    user_data: UserData,
    bgid: u16,
    num_buffers: u16,
    buffer_capacity: usize,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  RegisterRawBuffers {
    user_data: UserData,
    buffers: Vec<Vec<u8>>,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  Listen {
    user_data: UserData,
    addr: SocketAddr,
    protocol_handler_factory_id: String,
    protocol_config: ProtocolConfig,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  Connect {
    user_data: UserData,
    target_addr: SocketAddr,
    protocol_handler_factory_id: String,
    protocol_config: ProtocolConfig,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  RegisterExternalFd {
    user_data: UserData,
    fd: RawFd, // The externally established FD
    protocol_handler_factory_id: String,
    protocol_config: ProtocolConfig,
    is_server_role: bool, // True if this FD is from an accepted connection on server-side
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
    mpsc_rx_for_worker: Arc<mpsc::Receiver<Arc<Vec<Msg>>>>,
  },
  StartFdReadLoop {
    user_data: UserData,
    fd: RawFd,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
  ShutdownConnectionHandler {
    user_data: UserData,
    fd: RawFd,
    reply_tx: oneshot::Sender<Result<UringOpCompletion, ZmqError>>,
  },
}

impl UringOpRequest {
  
  pub(crate) fn get_user_data_ref(&self) -> UserData {
    match self {
      Self::Nop { user_data, .. }
      | Self::InitializeBufferRing { user_data, .. }
      | Self::RegisterRawBuffers { user_data, .. }
      | Self::Listen { user_data, .. }
      | Self::Connect { user_data, .. }
      | Self::RegisterExternalFd { user_data, .. }
      | Self::StartFdReadLoop { user_data, .. }
      | Self::ShutdownConnectionHandler { user_data, .. } => *user_data,
    }
  }

  pub(crate) fn op_name_str(&self) -> String {
    match self {
      Self::Nop { .. } => "Nop".to_string(),
      Self::InitializeBufferRing { .. } => "InitializeBufferRing".to_string(),
      Self::RegisterRawBuffers { .. } => "RegisterRawBuffers".to_string(),
      Self::Listen { .. } => "Listen".to_string(),
      Self::Connect { .. } => "Connect".to_string(),
      Self::RegisterExternalFd { .. } => "RegisterExternalFd".to_string(),
      Self::StartFdReadLoop { .. } => "StartFdReadLoop".to_string(),
      Self::ShutdownConnectionHandler { .. } => "ShutdownConnectionHandler".to_string(),
    }
  }

  pub(crate) fn get_reply_tx_ref(&self) -> &oneshot::Sender<Result<UringOpCompletion, ZmqError>> {
    match self {
      Self::Nop { reply_tx, .. }
      | Self::InitializeBufferRing { reply_tx, .. }
      | Self::RegisterRawBuffers { reply_tx, .. }
      | Self::Listen { reply_tx, .. }
      | Self::RegisterExternalFd { reply_tx, .. }
      | Self::Connect { reply_tx, .. }
      | Self::StartFdReadLoop { reply_tx, .. }
      | Self::ShutdownConnectionHandler { reply_tx, .. } => reply_tx,
    }
  }
}

impl fmt::Debug for UringOpRequest {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      UringOpRequest::Nop { user_data, .. } => f
        .debug_struct("Nop")
        .field("user_data", user_data)
        .finish_non_exhaustive(),
      UringOpRequest::InitializeBufferRing {
        user_data,
        bgid,
        num_buffers,
        buffer_capacity,
        ..
      } => f
        .debug_struct("InitializeBufferRing")
        .field("user_data", user_data)
        .field("bgid", bgid)
        .field("num_buffers", num_buffers)
        .field("buffer_capacity", buffer_capacity)
        .finish_non_exhaustive(),
      UringOpRequest::RegisterRawBuffers { user_data, buffers, .. } => f
        .debug_struct("RegisterRawBuffers")
        .field("user_data", user_data)
        .field("buffers_count", &buffers.len())
        .finish_non_exhaustive(),
      UringOpRequest::Listen {
        user_data,
        addr,
        protocol_handler_factory_id,
        ..
      } => f
        .debug_struct("Listen")
        .field("user_data", user_data)
        .field("addr", addr)
        .field("protocol_handler_factory_id", protocol_handler_factory_id)
        .finish_non_exhaustive(),
      UringOpRequest::Connect {
        user_data,
        target_addr,
        protocol_handler_factory_id,
        ..
      } => f
        .debug_struct("Connect")
        .field("user_data", user_data)
        .field("target_addr", target_addr)
        .field("protocol_handler_factory_id", protocol_handler_factory_id)
        .finish_non_exhaustive(),
      UringOpRequest::RegisterExternalFd {
        user_data,
        fd,
        protocol_handler_factory_id,
        is_server_role,
        ..
      } => f
        .debug_struct("RegisterExternalFd")
        .field("user_data", user_data)
        .field("fd", fd)
        .field("protocol_handler_factory_id", protocol_handler_factory_id)
        .field("is_server_role", is_server_role)
        .finish_non_exhaustive(),
      UringOpRequest::StartFdReadLoop { user_data, fd, .. } => f
        .debug_struct("StartFdReadLoop")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish_non_exhaustive(),
      UringOpRequest::ShutdownConnectionHandler { user_data, fd, .. } => f
        .debug_struct("ShutdownConnectionHandler")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish_non_exhaustive(),
    }
  }
}

pub enum UringOpCompletion {
  NopSuccess {
    user_data: UserData,
  },
  InitializeBufferRingSuccess {
    user_data: UserData,
    bgid: u16,
  },
  RegisterRawBuffersSuccess {
    user_data: UserData,
  },
  ListenSuccess {
    user_data: UserData,
    listener_fd: RawFd,
    actual_addr: SocketAddr,
  },
  ConnectSuccess {
    user_data: UserData,
    connected_fd: RawFd,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
  },
  RegisterExternalFdSuccess {
    user_data: UserData,
    fd: RawFd,
  },
  StartFdReadLoopAck {
    user_data: UserData,
    fd: RawFd,
  },
  SendDataViaHandlerAck {
    user_data: UserData,
    fd: RawFd,
  },
  ShutdownConnectionHandlerComplete {
    user_data: UserData,
    fd: RawFd,
  },
  OpError {
    user_data: UserData,
    op_name: String,
    error: ZmqError,
  },
}

impl fmt::Debug for UringOpCompletion {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      UringOpCompletion::NopSuccess { user_data } => {
        f.debug_struct("NopSuccess").field("user_data", user_data).finish()
      }
      UringOpCompletion::InitializeBufferRingSuccess { user_data, bgid } => f
        .debug_struct("InitializeBufferRingSuccess")
        .field("user_data", user_data)
        .field("bgid", bgid)
        .finish(),
      UringOpCompletion::RegisterRawBuffersSuccess { user_data } => f
        .debug_struct("RegisterRawBuffersSuccess")
        .field("user_data", user_data)
        .finish(),
      UringOpCompletion::ListenSuccess {
        user_data,
        listener_fd,
        actual_addr,
      } => f
        .debug_struct("ListenSuccess")
        .field("user_data", user_data)
        .field("listener_fd", listener_fd)
        .field("actual_addr", actual_addr)
        .finish(),
      UringOpCompletion::ConnectSuccess {
        user_data,
        connected_fd,
        peer_addr,
        local_addr,
      } => f
        .debug_struct("ConnectSuccess")
        .field("user_data", user_data)
        .field("connected_fd", connected_fd)
        .field("peer_addr", peer_addr)
        .field("local_addr", local_addr)
        .finish(),
      UringOpCompletion::RegisterExternalFdSuccess { user_data, fd } => f
        .debug_struct("RegisterExternalFdSuccess")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish(),
      UringOpCompletion::StartFdReadLoopAck { user_data, fd } => f
        .debug_struct("StartFdReadLoopAck")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish(),
      UringOpCompletion::SendDataViaHandlerAck { user_data, fd } => f
        .debug_struct("SendDataViaHandlerAck")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish(),
      UringOpCompletion::ShutdownConnectionHandlerComplete { user_data, fd } => f
        .debug_struct("ShutdownConnectionHandlerComplete")
        .field("user_data", user_data)
        .field("fd", fd)
        .finish(),
      UringOpCompletion::OpError {
        user_data,
        op_name,
        error,
      } => f
        .debug_struct("OpError")
        .field("user_data", user_data)
        .field("op_name", op_name)
        .field("error", error)
        .finish(),
    }
  }
}
