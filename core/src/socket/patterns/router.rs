use crate::message::Blob;
use std::collections::HashMap;
// use tokio::sync::RwLock; // Remove
use parking_lot::RwLock; // Add
use tracing;

#[derive(Debug, Default)]
pub(crate) struct RouterMap {
  pub(crate) identity_to_uri: RwLock<HashMap<Blob, String>>,
  pub(crate) read_pipe_to_identity: RwLock<HashMap<usize, Blob>>,
}

impl RouterMap {
  pub fn new() -> Self {
    Self::default()
  }

  // Methods remain async fn for API consistency if called from async contexts
  pub async fn add_peer(&self, identity: Blob, pipe_read_id: usize, endpoint_uri: String) {
    let mut id_to_uri_guard = self.identity_to_uri.write(); // Sync write lock
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write(); // Sync write lock

    // Logic remains the same, guards are dropped at the end of the scope
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
        // Important: id_to_uri_guard is already held
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

  pub async fn remove_peer_by_read_pipe(&self, pipe_read_id: usize) {
    let identity_to_remove: Option<Blob>;
    {
      // Scope for the first write lock
      let mut pipe_to_id_guard = self.read_pipe_to_identity.write();
      identity_to_remove = pipe_to_id_guard.remove(&pipe_read_id);
    } // pipe_to_id_guard is dropped here

    if let Some(identity) = identity_to_remove {
      let mut id_to_uri_guard = self.identity_to_uri.write();
      if let Some(removed_uri) = id_to_uri_guard.remove(&identity) {
        tracing::trace!(?identity, pipe_read_id, removed_uri = %removed_uri, "RouterMap removed peer by read pipe");
      } else {
        tracing::warn!(?identity, pipe_read_id, "RouterMap: Identity found for read_pipe_id, but no corresponding entry in identity_to_uri map during removal.");
      }
    } else {
      tracing::trace!(
        pipe_read_id,
        "RouterMap remove peer by read pipe: ID not found in reverse map"
      );
    }
  }

  pub async fn get_uri_for_identity(&self, identity: &Blob) -> Option<String> {
    self.identity_to_uri.read().get(identity).cloned() // Sync read lock
  }

  pub async fn get_identity_by_read_pipe(&self, pipe_read_id: usize) -> Option<Blob> {
    self.read_pipe_to_identity.read().get(&pipe_read_id).cloned() // Sync read lock
  }

  pub async fn remove_peer_by_identity(&self, identity: &Blob) {
    let mut id_to_uri_guard = self.identity_to_uri.write(); // Sync write lock
    if let Some(removed_uri) = id_to_uri_guard.remove(identity) {
      tracing::trace!(?identity, removed_uri = %removed_uri, "RouterMap removed peer by identity (forward map only)");
    }
  }

  pub async fn update_peer_identity(&self, pipe_read_id: usize, new_identity: Blob, endpoint_uri: &str) {
    let mut id_to_uri_guard = self.identity_to_uri.write(); // Sync write lock
    let mut pipe_to_id_guard = self.read_pipe_to_identity.write(); // Sync write lock

    if let Some(old_identity) = pipe_to_id_guard.get(&pipe_read_id) {
      if *old_identity != new_identity {
        // id_to_uri_guard is already held
        id_to_uri_guard.remove(old_identity);
        tracing::debug!(pipe_read_id, old_id = ?old_identity, new_id = ?new_identity, "RouterMap: Old identity removed for pipe during identity update.");
      }
    }

    pipe_to_id_guard.insert(pipe_read_id, new_identity.clone());
    id_to_uri_guard.insert(new_identity.clone(), endpoint_uri.to_string());

    tracing::debug!(pipe_read_id, new_id = ?new_identity, uri = %endpoint_uri, "RouterMap: Peer identity updated.");
  }
}
