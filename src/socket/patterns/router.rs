// src/socket/patterns/router.rs

use crate::message::Blob; // Use Blob for identities
use std::collections::HashMap;
use tokio::sync::Mutex; // Protect the maps

/// Maps peer identities (Blobs) to outgoing pipe write IDs.
/// Used by ROUTER sockets to send messages to specific peers.
#[derive(Debug, Default)]
pub(crate) struct RouterMap {
  // Map: Peer Identity -> Pipe Write ID (Core -> Session channel)
  identity_to_pipe: Mutex<HashMap<Blob, usize>>,
  // Map: Pipe Read ID -> Peer Identity (Needed for cleanup on detach)
  // We only learn the Read ID on detach, so need this reverse lookup.
  read_pipe_to_identity: Mutex<HashMap<usize, Blob>>,
}

impl RouterMap {
  pub fn new() -> Self {
    Self::default()
  }
  
  /// Gets the identity associated with a given pipe read ID.
  pub async fn get_identity_by_read_pipe(&self, pipe_read_id: usize) -> Option<Blob> {
      self.read_pipe_to_identity.lock().await.get(&pipe_read_id).cloned()
  }

  /// Adds or updates the mapping for a peer.
  pub async fn add_peer(
    &self,
    identity: Blob,
    pipe_read_id: usize,  // ID Core reads from this peer
    pipe_write_id: usize, // ID Core writes to this peer
  ) {
    let mut id_to_pipe_guard = self.identity_to_pipe.lock().await;
    let mut pipe_to_id_guard = self.read_pipe_to_identity.lock().await;

    // Check if another pipe was using this identity
    if let Some(old_pipe_write_id) = id_to_pipe_guard.insert(identity.clone(), pipe_write_id) {
      if old_pipe_write_id != pipe_write_id {
        let mut _old_read_id: Option<usize> = None; // Provide type usize
                                                    // <<< MODIFIED END >>>
                                                    // Find the old read ID associated with the old write ID to clean up reverse map
                                                    // Still inefficient without better map structure, logging warning only.
        tracing::warn!(
               new_pipe_write_id = pipe_write_id,
               old_pipe_write_id = old_pipe_write_id,
               ?identity,
               "RouterMap: Identity collision detected, overwriting. Cannot efficiently cleanup old reverse mapping."
           );
        // Example of finding (but requires another lock or different structure):
        // for (r_id, id_blob) in pipe_to_id_guard.iter() {
        //    if id_blob == &identity && *some_map.get(r_id)? == old_pipe_write_id {
        //        old_read_id = Some(*r_id); break;
        //    }
        // }
        // if let Some(r_id) = old_read_id { pipe_to_id_guard.remove(&r_id); }
      }
    }

    // Add reverse mapping
    pipe_to_id_guard.insert(pipe_read_id, identity.clone());

    tracing::trace!(
      ?identity,
      pipe_read_id,
      pipe_write_id,
      "RouterMap added peer"
    );
  }

  /// Removes a peer mapping using the pipe READ ID (typically provided on detachment).
  pub async fn remove_peer_by_read_pipe(&self, pipe_read_id: usize) {
    let mut pipe_to_id_guard = self.read_pipe_to_identity.lock().await;
    if let Some(identity) = pipe_to_id_guard.remove(&pipe_read_id) {
      // Also remove the forward mapping
      drop(pipe_to_id_guard); // Release lock before acquiring next
      let mut id_to_pipe_guard = self.identity_to_pipe.lock().await;
      id_to_pipe_guard.remove(&identity);
      tracing::trace!(
        ?identity,
        pipe_read_id,
        "RouterMap removed peer by read pipe"
      );
    } else {
      tracing::trace!(
        pipe_read_id,
        "RouterMap remove peer by read pipe: ID not found"
      );
    }
  }

  /// Gets the pipe write ID for a given peer identity.
  pub async fn get_pipe(&self, identity: &Blob) -> Option<usize> {
    self.identity_to_pipe.lock().await.get(identity).copied()
  }
}
