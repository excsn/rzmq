use bytes::Bytes;
use std::collections::VecDeque;

pub(crate) struct EgressBuffer {
  chunks: VecDeque<(Bytes, usize)>, // (data, logical_message_count)
  write_offset: usize,
  total_bytes: usize,
  message_count: usize,
  // Track the absolute peak values seen during the lifetime of this buffer
  pub(crate) peak_messages: usize,
  pub(crate) peak_bytes: usize,
}

impl EgressBuffer {
  pub(crate) fn new() -> Self {
    Self {
      chunks: VecDeque::new(),
      write_offset: 0,
      total_bytes: 0,
      message_count: 0,
      peak_messages: 0,
      peak_bytes: 0,
    }
  }

  pub(crate) fn push(&mut self, data: Bytes, msg_count: usize) {
    if data.is_empty() {
      return;
    }
    self.total_bytes += data.len();
    self.message_count += msg_count;
    self.chunks.push_back((data, msg_count));

    // Update peak values
    self.peak_messages = self.peak_messages.max(self.message_count);
    self.peak_bytes = self.peak_bytes.max(self.total_bytes);
  }

  pub(crate) fn peak_messages(&self) -> usize {
    self.peak_messages
  }

  pub(crate) fn peak_bytes(&self) -> usize {
    self.peak_bytes
  }

  pub(crate) fn push_priority(&mut self, data: Bytes) {
    if data.is_empty() {
      return;
    }
    self.total_bytes += data.len();
    // Control frames (PING/PONG) are outside HWM accounting — msg_count = 0.
    // Inject in front but after the current write_offset position of the head chunk.
    if self.write_offset > 0 {
      self.chunks.insert(1, (data, 0));
    } else {
      self.chunks.push_front((data, 0));
    }
  }

  /// Returns a slice into the un-written portion of the front chunk, or `None` if empty.
  pub(crate) fn current_slice(&self) -> Option<&[u8]> {
    self.chunks.front().map(|(c, _)| &c[self.write_offset..])
  }

  /// Gathers pending slices into the provided stack-allocated array.
  /// Returns the number of slices populated.
  pub(crate) fn fill_slices<'a>(&'a self, out: &mut [std::io::IoSlice<'a>]) -> usize {
    if self.chunks.is_empty() || out.is_empty() {
      return 0;
    }

    let mut iter = self.chunks.iter();
    let mut count = 0;

    if let Some((front, _)) = iter.next() {
      out[count] = std::io::IoSlice::new(&front[self.write_offset..]);
      count += 1;
    }

    for (chunk, _) in iter {
      if count >= out.len() {
        break;
      }
      out[count] = std::io::IoSlice::new(chunk);
      count += 1;
    }
    count
  }

  /// Advances the write cursor by `n` bytes, popping fully-consumed chunks and
  /// decrementing `message_count` for each popped chunk's logical message tally.
  /// Advances the write cursor by `n` bytes, popping fully-consumed chunks and
  /// decrementing `message_count` for each popped chunk's logical message tally.
  /// Returns the number of logical messages popped during this advancement.
  pub(crate) fn advance(&mut self, mut n: usize) -> usize {
    self.total_bytes = self.total_bytes.saturating_sub(n);
    let mut popped_messages = 0;
    while n > 0 {
      if let Some((front, _)) = self.chunks.front() {
        let remaining = front.len() - self.write_offset;
        if n >= remaining {
          n -= remaining;
          let (_, mc) = self.chunks.pop_front().unwrap();
          self.message_count = self.message_count.saturating_sub(mc);
          popped_messages += mc; // Accumulate popped messages
          self.write_offset = 0;
        } else {
          self.write_offset += n;
          break;
        }
      } else {
        break;
      }
    }
    popped_messages
  }

  pub(crate) fn pending_messages(&self) -> usize {
    self.message_count
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
    buf.push(Bytes::from_static(b"0123456789"), 1);
    assert_eq!(buf.current_slice().unwrap(), b"0123456789");
    buf.advance(6);
    assert_eq!(buf.current_slice().unwrap(), b"6789");
    assert_eq!(buf.total_pending_bytes(), 4);
    buf.advance(4);
    assert!(buf.is_empty());
    assert_eq!(buf.total_pending_bytes(), 0);
    assert_eq!(buf.pending_messages(), 0);
  }

  #[test]
  fn total_pending_bytes_tracks_advances() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"aaa"), 1);
    buf.push(Bytes::from_static(b"bb"), 1);
    buf.push(Bytes::from_static(b"c"), 1);
    assert_eq!(buf.total_pending_bytes(), 6);
    assert_eq!(buf.pending_messages(), 3);
    buf.advance(3);
    assert_eq!(buf.total_pending_bytes(), 3);
    assert_eq!(buf.pending_messages(), 2);
    buf.advance(2);
    assert_eq!(buf.total_pending_bytes(), 1);
    assert_eq!(buf.pending_messages(), 1);
    buf.advance(1);
    assert!(buf.is_empty());
    assert_eq!(buf.pending_messages(), 0);
  }

  #[test]
  fn push_priority_front_of_non_empty() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"data"), 1);
    buf.push_priority(Bytes::from_static(b"PONG"));
    assert_eq!(buf.current_slice().unwrap(), b"PONG");
    assert_eq!(buf.pending_messages(), 1); // PONG doesn't count
    buf.advance(4);
    assert_eq!(buf.current_slice().unwrap(), b"data");
    buf.advance(4);
    assert_eq!(buf.pending_messages(), 0);
  }

  #[test]
  fn advance_spans_multiple_chunks() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"0123456789"), 1); // 10 bytes
    buf.push(Bytes::from_static(b"abcde"), 1); //  5 bytes
    buf.push(Bytes::from_static(b"XY"), 1); //  2 bytes
    assert_eq!(buf.pending_messages(), 3);
    // advance 12: consumes all of chunk[0] + 2 bytes of chunk[1]
    buf.advance(12);
    assert_eq!(buf.current_slice().unwrap(), b"cde");
    assert_eq!(buf.total_pending_bytes(), 5);
    assert_eq!(buf.pending_messages(), 2);
    // advance 5: consumes rest of chunk[1] + all of chunk[2]
    buf.advance(5);
    assert!(buf.is_empty());
    assert_eq!(buf.pending_messages(), 0);
  }

  #[test]
  fn push_priority_after_partial_write() {
    let mut buf = EgressBuffer::new();
    buf.push(Bytes::from_static(b"data"), 1);
    buf.advance(2); // partially written
    buf.push_priority(Bytes::from_static(b"PONG"));
    // Head is still "ta" (remaining of "data")
    assert_eq!(buf.current_slice().unwrap(), b"ta");
    assert_eq!(buf.pending_messages(), 1); // only the "data" chunk counts
    buf.advance(2);
    // PONG is next
    assert_eq!(buf.current_slice().unwrap(), b"PONG");
    buf.advance(4);
    assert!(buf.is_empty());
    assert_eq!(buf.pending_messages(), 0);
  }
}
