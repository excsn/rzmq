use crate::{
  message::Blob,
  socket::patterns::router::strategies::{
    DealerPeerStrategy, DefaultRouterStrategy, ReqPeerStrategy, RouterPeerStrategy,
    RouterSendStrategy,
  },
};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tracing;

#[derive(Debug, Clone)]
pub(crate) struct PeerInfo {
  pub uri: String,
  pub strategy: Arc<dyn RouterSendStrategy>,
}

#[derive(Debug, Default)]
pub(crate) struct RouterMap {
  pub(crate) identity_to_peer_info: RwLock<HashMap<Blob, PeerInfo>>,
  pub(crate) read_pipe_to_identity: RwLock<HashMap<usize, Blob>>,
}

impl RouterMap {
  pub fn new() -> Self {
    Self::default()
  }

  // Methods remain async fn for API consistency if called from async contexts
  pub async fn add_peer(&self, identity: Blob, pipe_read_id: usize, endpoint_uri: String) {
    let mut id_to_info_guard = self.identity_to_peer_info.write();
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write();

    let peer_info = PeerInfo {
      uri: endpoint_uri.clone(),
      strategy: Arc::new(DefaultRouterStrategy), // Use default strategy initially
    };

    if let Some(old_info) = id_to_info_guard.insert(identity.clone(), peer_info) {
      if old_info.uri != endpoint_uri {
        tracing::warn!(
            new_uri = %endpoint_uri,
            old_uri = %old_info.uri,
            ?identity,
            "RouterMap: Identity collision or re-route. Identity now maps to new URI."
        );
      }
    }

    if let Some(old_identity_for_this_pipe) =
      pipe_to_id_guard.insert(pipe_read_id, identity.clone())
    {
      if old_identity_for_this_pipe != identity {
        id_to_info_guard.remove(&old_identity_for_this_pipe);
        tracing::warn!(
            pipe_read_id,
            new_identity = ?identity,
            old_identity = ?old_identity_for_this_pipe,
            "RouterMap: Pipe read ID re-assigned to a new identity. Old identity's forward mapping removed."
        );
      }
    }
    tracing::trace!(?identity, pipe_read_id, uri = %endpoint_uri, "RouterMap added/updated peer with default strategy");
  }

  pub async fn remove_peer_by_read_pipe(&self, pipe_read_id: usize) {
    let identity_to_remove: Option<Blob>;
    {
      let mut pipe_to_id_guard = self.read_pipe_to_identity.write();
      identity_to_remove = pipe_to_id_guard.remove(&pipe_read_id);
    }

    if let Some(identity) = identity_to_remove {
      let mut id_to_info_guard = self.identity_to_peer_info.write();
      if let Some(removed_info) = id_to_info_guard.remove(&identity) {
        tracing::trace!(
            ?identity,
            pipe_read_id,
            removed_uri = %removed_info.uri,
            "RouterMap removed peer by read pipe"
        );
      } else {
        tracing::warn!(
          ?identity,
          pipe_read_id,
          "RouterMap: Identity found for read_pipe_id, but no corresponding entry in identity_to_peer_info map during removal."
        );
      }
    } else {
      tracing::trace!(
        pipe_read_id,
        "RouterMap remove peer by read pipe: ID not found in reverse map"
      );
    }
  }

  pub async fn get_identity_by_read_pipe(&self, pipe_read_id: usize) -> Option<Blob> {
    self
      .read_pipe_to_identity
      .read()
      .get(&pipe_read_id)
      .cloned() // Sync read lock
  }

  pub async fn remove_peer_by_identity(&self, identity: &Blob) {
    let mut id_to_info_guard = self.identity_to_peer_info.write();
    if let Some(removed_info) = id_to_info_guard.remove(identity) {
      tracing::trace!(
          ?identity,
          removed_uri = %removed_info.uri,
          "RouterMap removed peer by identity (forward map)."
      );

      // Now, find and remove the reverse mapping.
      let mut pipe_to_id_guard = self.read_pipe_to_identity.write();
      let mut key_to_remove: Option<usize> = None;
      for (key, val) in pipe_to_id_guard.iter() {
        if val == identity {
          key_to_remove = Some(*key);
          break;
        }
      }
      if let Some(key) = key_to_remove {
        pipe_to_id_guard.remove(&key);
        tracing::trace!(
          ?identity,
          pipe_read_id = key,
          "RouterMap removed peer by identity (reverse map)."
        );
      }
    }
  }

  pub async fn update_peer_identity(
    &self,
    pipe_read_id: usize,
    new_identity: Blob,
    endpoint_uri: &str,
    peer_socket_type: Option<&str>,
  ) {
    let strategy: Arc<dyn RouterSendStrategy> = match peer_socket_type {
      Some("REQ") => Arc::new(ReqPeerStrategy),
      Some("DEALER") => Arc::new(DealerPeerStrategy),
      Some("ROUTER") => Arc::new(RouterPeerStrategy),
      _ => Arc::new(DefaultRouterStrategy),
    };

    let peer_info = PeerInfo {
      uri: endpoint_uri.to_string(),
      strategy,
    };

    let mut id_to_info_guard = self.identity_to_peer_info.write();
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write();

    // Remove old identity if the pipe is being re-identified
    if let Some(old_identity) = pipe_to_id_guard.get(&pipe_read_id) {
      if *old_identity != new_identity {
        id_to_info_guard.remove(old_identity);
      }
    }

    pipe_to_id_guard.insert(pipe_read_id, new_identity.clone());
    id_to_info_guard.insert(new_identity, peer_info);
  }

  pub async fn get_peer_info_for_identity(&self, identity: &Blob) -> Option<PeerInfo> {
    self.identity_to_peer_info.read().get(identity).cloned()
  }
}

pub(crate) mod strategies {
  use crate::message::{Msg, MsgFlags};
  use crate::socket::patterns::framing::FramingLatch;
  use std::fmt::Debug;

  /// Defines a strategy for how a ROUTER socket should prepare outgoing frames
  /// based on the peer's socket type.
  pub(crate) trait RouterSendStrategy: Debug + Send + Sync + 'static {
    /// Prepares the final Vec<Msg> to be sent over the wire.
    ///
    /// # Arguments
    /// * `destination_identity_msg`: The first frame from the user, containing the routing ID.
    /// * `payload_frames`: The rest of the frames from the user.
    fn prepare_wire_frames(
      &self,
      destination_identity_msg: Msg,
      payload_frames: Vec<Msg>,
      framing: &FramingLatch,
    ) -> Vec<Msg>;
  }

  // Strategy for talking to REQ peers.
  #[derive(Debug)]
  pub(crate) struct ReqPeerStrategy;

  impl RouterSendStrategy for ReqPeerStrategy {
    fn prepare_wire_frames(
      &self,
      _destination_identity_msg: Msg,
      payload_frames: Vec<Msg>,
      _framing: &FramingLatch,
    ) -> Vec<Msg> {
      // REQ peers expect: [empty_delimiter, payload...]
      // The destination_identity_msg is discarded.
      let mut zmtp_wire_frames = Vec::with_capacity(1 + payload_frames.len());
      let mut delimiter = Msg::new();
      if !payload_frames.is_empty() {
        delimiter.set_flags(MsgFlags::MORE);
      }
      zmtp_wire_frames.push(delimiter);
      zmtp_wire_frames.extend(payload_frames);
      zmtp_wire_frames
    }
  }

  // Strategy for talking to DEALER peers.
  #[derive(Debug)]
  pub(crate) struct DealerPeerStrategy;

  impl RouterSendStrategy for DealerPeerStrategy {
    fn prepare_wire_frames(
      &self,
      mut destination_identity_msg: Msg,
      mut payload_frames: Vec<Msg>,
      framing: &FramingLatch,
    ) -> Vec<Msg> {
      // A ROUTER must send [identity, delimiter, payload...] to a DEALER.
      
      // 1. Prepare Identity (Must have MORE flag if payload follows)
      if !payload_frames.is_empty() {
        destination_identity_msg.set_flags(destination_identity_msg.flags() | MsgFlags::MORE);
      }
      
      // 2. Insert Identity at the front.
      payload_frames.insert(0, destination_identity_msg);

      // 3. Apply framing (Auto inserts delimiter at index 1, Manual does nothing)
      framing.encode(&mut payload_frames);
      
      payload_frames
    }
  }

  #[derive(Debug)]
  pub(crate) struct RouterPeerStrategy;

  impl RouterSendStrategy for RouterPeerStrategy {
    fn prepare_wire_frames(
      &self,
      _destination_identity_msg: Msg,
      payload_frames: Vec<Msg>,
      _framing: &FramingLatch,
    ) -> Vec<Msg> {
      // When a ROUTER sends to another ROUTER peer, it strips the peer's
      // identity (the destination_identity_msg) for routing and sends
      // only the remaining payload frames over the wire.
      payload_frames
    }
  }

  // Default strategy for talking to ROUTER, REP, or unknown peers.
  #[derive(Debug)]
  pub(crate) struct DefaultRouterStrategy;

  impl RouterSendStrategy for DefaultRouterStrategy {
    fn prepare_wire_frames(
      &self,
      mut destination_identity_msg: Msg,
      mut payload_frames: Vec<Msg>,
      framing: &FramingLatch,
    ) -> Vec<Msg> {
      // Default behavior: [identity, empty_delimiter, payload...]
      
      if !payload_frames.is_empty() {
        destination_identity_msg.set_flags(destination_identity_msg.flags() | MsgFlags::MORE);
      }
      
      payload_frames.insert(0, destination_identity_msg);
      framing.encode(&mut payload_frames);
      
      payload_frames
    }
  }
}
