use std::collections::VecDeque;
use bytes::Bytes;

pub(crate) struct EgressBuffer {
  chunks: VecDeque<Bytes>,
  write_offset: usize,
  total_bytes: usize,
}

impl EgressBuffer {
  pub(crate) fn new() -> Self {
    Self { chunks: VecDeque::new(), write_offset: 0, total_bytes: 0 }
  }

  pub(crate) fn push(&mut self, data: Bytes) {
    if data.is_empty() { return; }
    self.total_bytes += data.len();
    self.chunks.push_back(data);
  }

  pub(crate) fn push_priority(&mut self, data: Bytes) {
    if data.is_empty() { return; }
    self.total_bytes += data.len();
    // Inject in front but after the current write_offset position of the head chunk.
    // The simplest correct approach: if there is a partially-written head chunk, leave it
    // at index 0 and insert at index 1; otherwise push_front.
    if self.write_offset > 0 {
      self.chunks.insert(1, data);
    } else {
      self.chunks.push_front(data);
    }
  }

  /// Returns a slice into the un-written portion of the front chunk, or `None` if empty.
  pub(crate) fn current_slice(&self) -> Option<&[u8]> {
    self.chunks.front().map(|c| &c[self.write_offset..])
  }

  /// Advances the write cursor by `n` bytes. Pops the front chunk when fully consumed.
  pub(crate) fn advance(&mut self, n: usize) {
    self.total_bytes = self.total_bytes.saturating_sub(n);
    self.write_offset += n;
    if let Some(front) = self.chunks.front() {
      if self.write_offset >= front.len() {
        self.chunks.pop_front();
        self.write_offset = 0;
      }
    }
  }

  pub(crate) fn total_pending_bytes(&self) -> usize {
    self.total_bytes
  }

  pub(crate) fn is_empty(&self) -> bool {
    self.chunks.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn partial_writes_advance_and_pop() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"0123456789"));
    assert_eq!(buf.current_slice().unwrap(), b"0123456789");
    buf.advance(6);
    assert_eq!(buf.current_slice().unwrap(), b"6789");
    assert_eq!(buf.total_pending_bytes(), 4);
    buf.advance(4);
    assert!(buf.is_empty());
    assert_eq!(buf.total_pending_bytes(), 0);
  }

  #[test]
  fn total_pending_bytes_tracks_advances() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"aaa"));
    buf.push(Bytes::from_static(b"bb"));
    buf.push(Bytes::from_static(b"c"));
    assert_eq!(buf.total_pending_bytes(), 6);
    buf.advance(3);
    assert_eq!(buf.total_pending_bytes(), 3);
    buf.advance(2);
    assert_eq!(buf.total_pending_bytes(), 1);
    buf.advance(1);
    assert!(buf.is_empty());
  }

  #[test]
  fn push_priority_front_of_non_empty() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"data"));
    buf.push_priority(Bytes::from_static(b"PONG"));
    assert_eq!(buf.current_slice().unwrap(), b"PONG");
    buf.advance(4);
    assert_eq!(buf.current_slice().unwrap(), b"data");
  }

  #[test]
  fn push_priority_after_partial_write() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"data"));
    buf.advance(2); // partially written
    buf.push_priority(Bytes::from_static(b"PONG"));
    // Head is still "ta" (remaining of "data")
    assert_eq!(buf.current_slice().unwrap(), b"ta");
    buf.advance(2);
    // PONG is next
    assert_eq!(buf.current_slice().unwrap(), b"PONG");
  }
}
