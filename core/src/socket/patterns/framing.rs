use crate::message::{Msg, MsgFlags};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Function signature for framing operations (encode/decode).
type FramingOp = fn(&mut Vec<Msg>);

/// No-op function for Manual mode (Raw).
fn noop(_: &mut Vec<Msg>) {}

/// A specialized latch that manages framing strategy.
/// Starts as 'Auto' (0) and can be switched to 'Manual' (1) exactly once.
///
/// This structure is designed to be lock-free and branch-free on the hot path.
#[derive(Debug)]
pub(crate) struct FramingLatch {
  // 0 = Auto, 1 = Manual
  mode: AtomicUsize,
  // Lookup tables for behavior.
  // Index 0 = Auto Logic, Index 1 = No-op (Manual)
  encoders: [FramingOp; 2],
  decoders: [FramingOp; 2],
}

impl FramingLatch {
  pub fn new(auto_encode: FramingOp, auto_decode: FramingOp) -> Self {
    Self {
      mode: AtomicUsize::new(0),
      encoders: [auto_encode, noop],
      decoders: [auto_decode, noop],
    }
  }

  /// Hot Path: Encode outgoing frames.
  /// Uses branchless array lookup to execute the correct strategy.
  #[inline(always)]
  pub fn encode(&self, frames: &mut Vec<Msg>) {
    let idx = self.mode.load(Ordering::Acquire);
    // Safety: idx & 1 ensures we never go out of bounds even if memory is corrupted,
    // allowing the compiler to elide bounds checks.
    (self.encoders[idx & 1])(frames);
  }

  /// Hot Path: Decode incoming frames.
  #[inline(always)]
  pub fn decode(&self, frames: &mut Vec<Msg>) {
    let idx = self.mode.load(Ordering::Acquire);
    (self.decoders[idx & 1])(frames);
  }

  /// Configuration: Switch to Manual mode.
  /// This is a one-way operation. Returns true if changed, false if already manual.
  pub fn set_manual(&self) -> bool {
    self
      .mode
      .compare_exchange(0, 1, Ordering::Release, Ordering::Relaxed)
      .is_ok()
  }

  pub fn is_manual(&self) -> bool {
    self.mode.load(Ordering::Acquire) == 1
  }
}

// --- Optimized Auto Implementations ---

pub(crate) fn router_auto_encode(frames: &mut Vec<Msg>) {
  // Assumes frames is [Identity, Payload...]
  // We want to insert a delimiter at index 1 -> [Identity, Delimiter, Payload...]

  let mut delimiter = Msg::new();
  if frames.len() > 1 {
    // If there is payload after the delimiter, the delimiter needs the MORE flag.
    delimiter.set_flags(MsgFlags::MORE);
  }
  // The Identity (frames[0]) always needs the MORE flag because we are inserting a delimiter after it.
  if !frames.is_empty() {
    let current_flags = frames[0].flags();
    frames[0].set_flags(current_flags | MsgFlags::MORE);
  }

  // insert(1) shifts elements 1..end to the right.
  // This is more efficient than allocating a new vector structure.
  if !frames.is_empty() {
    frames.insert(1, delimiter);
  }
}

pub(crate) fn router_auto_decode(frames: &mut Vec<Msg>) {
  // Router receives [Identity, Delimiter, Payload...]. We remove index 1.
  if frames.len() > 1 {
    frames.remove(1);
  }
}

pub(crate) fn dealer_auto_encode(frames: &mut Vec<Msg>) {
  // Dealer sends [Payload...]. We insert Delimiter at index 0 -> [Delimiter, Payload...]
  let mut delimiter = Msg::new();
  if !frames.is_empty() {
    delimiter.set_flags(MsgFlags::MORE);
  }
  frames.insert(0, delimiter);
}

pub(crate) fn dealer_auto_decode(frames: &mut Vec<Msg>) {
  // Dealer receives [Delimiter, Payload...]. We remove index 0.
  if !frames.is_empty() {
    frames.remove(0);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_router_auto_encode() {
    // Case 1: Identity + Payload
    let mut frames = vec![Msg::from_static(b"Identity"), Msg::from_static(b"Payload")];
    router_auto_encode(&mut frames);

    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].data().unwrap(), b"Identity");
    assert!(frames[0].is_more()); // Identity should have MORE

    assert_eq!(frames[1].size(), 0); // Delimiter
    assert!(frames[1].is_more()); // Delimiter should have MORE because payload follows

    assert_eq!(frames[2].data().unwrap(), b"Payload");
    assert!(!frames[2].is_more()); // Payload didn't have MORE

    // Case 2: Identity only (no payload)
    let mut frames = vec![Msg::from_static(b"Identity")];
    router_auto_encode(&mut frames);

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].data().unwrap(), b"Identity");
    assert!(frames[0].is_more());

    assert_eq!(frames[1].size(), 0); // Delimiter
    assert!(!frames[1].is_more()); // Delimiter is last, no MORE
  }

  #[test]
  fn test_dealer_auto_encode() {
    // Case 1: Payload
    let mut frames = vec![Msg::from_static(b"Payload")];
    dealer_auto_encode(&mut frames);

    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].size(), 0); // Delimiter
    assert!(frames[0].is_more()); // Delimiter has MORE

    assert_eq!(frames[1].data().unwrap(), b"Payload");
    assert!(!frames[1].is_more());

    // Case 2: Empty payload list
    let mut frames = vec![];
    dealer_auto_encode(&mut frames);

    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].size(), 0);
    assert!(!frames[0].is_more());
  }
}
