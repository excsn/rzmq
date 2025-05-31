// core/src/socket/patterns/router.rs

use crate::message::Blob;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing;

/// Maps peer identities (Blobs) to their endpoint URIs.
/// Used by ROUTER sockets to send messages to specific peers.
#[derive(Debug, Default)]
pub(crate) struct RouterMap {
  /// Map: Peer Identity -> Endpoint URI (String)
  pub(crate) identity_to_uri: RwLock<HashMap<Blob, String>>,
  /// Map: Pipe Read ID (from SocketCore's perspective) -> Peer Identity
  /// Needed to find the Identity when a pipe is detached, so the main
  /// identity_to_uri map can be cleaned.
  pub(crate) read_pipe_to_identity: RwLock<HashMap<usize, Blob>>,
}

impl RouterMap {
  pub fn new() -> Self {
    Self::default()
  }

  /// Adds or updates the mapping for a peer.
  ///
  /// # Arguments
  /// * `identity`: The Blob identity of the peer.
  /// * `pipe_read_id`: The ID SocketCore uses to read messages from this peer's connection.
  /// * `endpoint_uri`: The endpoint URI string associated with this peer's connection.
  pub async fn add_peer(
    &self,
    identity: Blob,
    pipe_read_id: usize,
    endpoint_uri: String,
  ) {
    let mut id_to_uri_guard = self.identity_to_uri.write().await;
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write().await;

    if let Some(old_uri) = id_to_uri_guard.insert(identity.clone(), endpoint_uri.clone()) {
      if old_uri != endpoint_uri {
        tracing::warn!(
          new_uri = %endpoint_uri,
          old_uri = %old_uri,
          ?identity,
          "RouterMap: Identity collision or re-route. Identity now maps to new URI."
        );
      }
    }

    if let Some(old_identity_for_this_pipe) = pipe_to_id_guard.insert(pipe_read_id, identity.clone()) {
      if old_identity_for_this_pipe != identity {
        id_to_uri_guard.remove(&old_identity_for_this_pipe);
        tracing::warn!(
            pipe_read_id,
            new_identity = ?identity,
            old_identity = ?old_identity_for_this_pipe,
            "RouterMap: Pipe read ID re-assigned to a new identity. Old identity's forward mapping removed."
        );
      }
    }
    tracing::trace!(?identity, pipe_read_id, uri = %endpoint_uri, "RouterMap added/updated peer");
  }

  /// Removes a peer mapping using the pipe READ ID (typically provided on detachment).
  pub async fn remove_peer_by_read_pipe(&self, pipe_read_id: usize) {
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write().await;
    if let Some(identity) = pipe_to_id_guard.remove(&pipe_read_id) {
      drop(pipe_to_id_guard);
      let mut id_to_uri_guard = self.identity_to_uri.write().await;
      if let Some(removed_uri) = id_to_uri_guard.remove(&identity) {
        tracing::trace!(?identity, pipe_read_id, removed_uri = %removed_uri, "RouterMap removed peer by read pipe");
      } else {
        tracing::warn!(?identity, pipe_read_id, "RouterMap: Identity found for read_pipe_id, but no corresponding entry in identity_to_uri map during removal.");
      }
    } else {
      tracing::trace!(pipe_read_id, "RouterMap remove peer by read pipe: ID not found in reverse map");
    }
  }
  
  /// Gets the endpoint URI for a given peer identity.
  pub async fn get_uri_for_identity(&self, identity: &Blob) -> Option<String> {
    self.identity_to_uri.read().await.get(identity).cloned()
  }

  /// Gets the identity associated with a given pipe read ID.
  pub async fn get_identity_by_read_pipe(&self, pipe_read_id: usize) -> Option<Blob> {
    self.read_pipe_to_identity.read().await.get(&pipe_read_id).cloned()
  }

  /// Removes a peer mapping using the identity.
  /// Note: This primarily cleans the forward map (Identity -> URI).
  /// The reverse map (Read Pipe ID -> Identity) is best cleaned via `remove_peer_by_read_pipe`.
  pub async fn remove_peer_by_identity(&self, identity: &Blob) {
    let mut id_to_uri_guard = self.identity_to_uri.write().await;
    if let Some(removed_uri) = id_to_uri_guard.remove(identity) {
      tracing::trace!(?identity, removed_uri = %removed_uri, "RouterMap removed peer by identity (forward map only)");
    }
  }

  /// Updates the identity associated with a pipe_read_id and its endpoint_uri.
  /// This is crucial when the true ZMTP identity is established after initial connection.
  pub async fn update_peer_identity(
    &self,
    pipe_read_id: usize,
    new_identity: Blob,
    endpoint_uri: &str,
  ) {
    let mut id_to_uri_guard = self.identity_to_uri.write().await;
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write().await;

    if let Some(old_identity) = pipe_to_id_guard.get(&pipe_read_id) {
      if *old_identity != new_identity {
        id_to_uri_guard.remove(old_identity);
        tracing::debug!(pipe_read_id, old_id = ?old_identity, new_id = ?new_identity, "RouterMap: Old identity removed for pipe during identity update.");
      }
    }
    
    pipe_to_id_guard.insert(pipe_read_id, new_identity.clone());
    id_to_uri_guard.insert(new_identity.clone(), endpoint_uri.to_string());
    
    tracing::debug!(pipe_read_id, new_id = ?new_identity, uri = %endpoint_uri, "RouterMap: Peer identity updated.");
  }
}