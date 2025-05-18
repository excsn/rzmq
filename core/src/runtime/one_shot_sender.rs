use crate::error::ZmqError;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex}; // For the Result type in the oneshot channel

/// A cloneable wrapper around a `tokio::sync::oneshot::Sender` that ensures
/// the underlying sender can only be used once.
///
/// This allows a oneshot reply mechanism to be part of a `Clone`-able structure,
/// like a `SystemEvent`, which might be broadcast. The first entity to successfully
/// `take_and_send` will consume the sender.
#[derive(Debug, Clone)]
pub struct OneShotSender {
  // Arc allows multiple clones of the wrapper.
  // Mutex provides interior mutability to access the Option.
  // Option allows the Sender to be `take()`-n, ensuring it's used only once.
  inner: Arc<Mutex<Option<oneshot::Sender<Result<(), ZmqError>>>>>,
}

impl OneShotSender {
  /// Creates a new `CloneableOneShotSender` wrapping a given `oneshot::Sender`.
  pub fn new(sender: oneshot::Sender<Result<(), ZmqError>>) -> Self {
    Self {
      inner: Arc::new(Mutex::new(Some(sender))),
    }
  }

  /// Attempts to take the underlying `oneshot::Sender` and send a result on it.
  /// This method consumes the sender to ensure the "one-shot" guarantee.
  ///
  /// # Arguments
  /// * `result` - The `Result<(), ZmqError>` to send back.
  ///
  /// # Returns
  /// * `true` if the sender was present and the send was attempted (regardless of whether the receiver was still alive).
  /// * `false` if the sender had already been taken and used.
  pub async fn take_and_send_forget(&self, result: Result<(), ZmqError>) -> bool {
    let mut guard = self.inner.lock().await;
    if let Some(sender) = guard.take() {
      // `take()` removes the Sender from the Option
      // We don't care if the receiver was dropped; that's a normal outcome for oneshot.
      // The important part is that we tried to send.
      let _ = sender.send(result);
      true
    } else {
      // Sender was already taken by someone else or was never there.
      false
    }
  }

  pub async fn take_and_send(&self, result: Result<(), ZmqError>) -> Option<Result<(), Result<(), ZmqError>>> {
    let mut guard = self.inner.lock().await;
    if let Some(sender) = guard.take() {
      Some(sender.send(result)) // Return the Result from the actual send
    } else {
      None
    }
  }
}
