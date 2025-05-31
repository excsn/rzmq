// core/src/io_uring_backend/one_shot_sender.rs

#![cfg(feature = "io-uring")]

use std::sync::Arc;
use tokio::sync::oneshot; // Still use tokio's oneshot channel itself
use parking_lot::Mutex;   // Use parking_lot::Mutex for sync internal locking
use std::fmt; 

// OneShotSender<T> where T is Result<UringOpCompletion, ZmqError> or Result<Vec<u8>, ZmqError> etc.
pub struct OneShotSender<T> {
  inner: Arc<Mutex<Option<oneshot::Sender<T>>>>,
}

impl<T> fmt::Debug for OneShotSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.inner.lock(); 
        f.debug_struct("OneShotSender")
            .field("inner_is_some", &guard.is_some())
            .finish_non_exhaustive()
    }
}

impl<T> Clone for OneShotSender<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T: Send> OneShotSender<T> { 
  pub fn new(sender: oneshot::Sender<T>) -> Self { 
    Self {
      inner: Arc::new(Mutex::new(Some(sender))),
    }
  }

  pub fn take_and_send_forget_sync(&self, result: T) -> bool {
    let mut guard = self.inner.lock(); 
    if let Some(sender) = guard.take() {
      let _ = sender.send(result); 
      true
    } else {
      tracing::warn!("OneShotSender: take_and_send_forget_sync called on already taken sender");
      false
    }
  }

  pub fn take_and_send_sync(&self, result: T) -> Option<Result<(), T>> {
    let mut guard = self.inner.lock(); 
    if let Some(sender) = guard.take() {
      Some(sender.send(result)) 
    } else {
      tracing::warn!("OneShotSender: take_and_send_sync called on already taken sender");
      None
    }
  }
}