use crate::error::ZmqError;
use crate::message::{FrameBatch, MsgFlags};
use bytes::{BufMut, Bytes, BytesMut};

/// Payloads of at least this many bytes are written as their own chunk by
/// `frame_mixed` instead of being copied into the coalesce buffer.
pub(crate) const COPY_THRESHOLD: usize = 8 * 1024;

/// Writes a ZMTP frame header: flags byte, then a 1-byte or 8-byte length.
#[inline]
fn put_header(buf: &mut BytesMut, flags: MsgFlags, len: usize) {
  let mut zmtp_flags = 0u8;
  if flags.contains(MsgFlags::MORE) {
    zmtp_flags |= 0x01;
  }
  if flags.contains(MsgFlags::COMMAND) {
    zmtp_flags |= 0x04;
  }
  if len <= 255 {
    buf.put_u8(zmtp_flags);
    buf.put_u8(len as u8);
  } else {
    buf.put_u8(zmtp_flags | 0x02);
    buf.put_u64(len as u64);
  }
}

/// Write-side ZMTP frame serialization engine.
/// Manages reusable header and coalesce buffers to eliminate per-message allocation
/// on the egress hot path. Does not handle reading or parsing.
pub(crate) struct ZmtpFrameEncoder {
  header_slab: BytesMut,
  coalesce_buffer: BytesMut,
}

impl ZmtpFrameEncoder {
  pub fn new(initial_header_cap: usize, initial_coalesce_cap: usize) -> Self {
    Self {
      header_slab: BytesMut::with_capacity(initial_header_cap),
      coalesce_buffer: BytesMut::with_capacity(initial_coalesce_cap),
    }
  }

  /// Serializes multiple FrameBatches into a single contiguous Bytes buffer.
  pub fn frame_contiguous(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    self.coalesce_buffer.clear();

    let mut required_size = 0;
    for group in batch {
      for msg in group {
        let len = msg.size();
        required_size += if len <= 255 { 2 + len } else { 9 + len };
      }
    }
    self.coalesce_buffer.reserve(required_size);

    for group in batch {
      for msg in group {
        let data = msg.data().unwrap_or(&[]);
        let len = data.len();
        let flags = msg.flags();

        let mut zmtp_flags = 0u8;
        if flags.contains(MsgFlags::MORE) {
          zmtp_flags |= 0x01;
        }
        if flags.contains(MsgFlags::COMMAND) {
          zmtp_flags |= 0x04;
        }

        if len <= 255 {
          self.coalesce_buffer.put_u8(zmtp_flags);
          self.coalesce_buffer.put_u8(len as u8);
        } else {
          zmtp_flags |= 0x02;
          self.coalesce_buffer.put_u8(zmtp_flags);
          self.coalesce_buffer.put_u64(len as u64);
        }
        self.coalesce_buffer.put_slice(data);
      }
    }

    Ok(self.coalesce_buffer.split().freeze())
  }

  /// Serializes a batch into chunks in wire order, each paired with the number
  /// of logical messages whose last frame it ends. Headers and payloads below
  /// `copy_threshold` are copied into runs of the coalesce buffer; larger
  /// payloads are emitted as their own `Bytes` without a copy. A batch with no
  /// large payload yields a single chunk identical to `frame_contiguous`.
  pub fn frame_mixed(&mut self, batch: &[FrameBatch], copy_threshold: usize, out: &mut Vec<(Bytes, usize)>) {
    let mut run_bytes = 0;
    for group in batch {
      for msg in group {
        let len = msg.size();
        run_bytes += if len <= 255 { 2 } else { 9 };
        if len < copy_threshold {
          run_bytes += len;
        }
      }
    }
    self.coalesce_buffer.reserve(run_bytes);

    let mut completed = 0;
    for group in batch {
      let last = group.len().saturating_sub(1);
      for (i, msg) in group.iter().enumerate() {
        let ends_message = i == last;
        let len = msg.size();
        put_header(&mut self.coalesce_buffer, msg.flags(), len);
        if len < copy_threshold {
          self.coalesce_buffer.put_slice(msg.data().unwrap_or(&[]));
          completed += usize::from(ends_message);
        } else {
          out.push((self.coalesce_buffer.split().freeze(), completed));
          completed = 0;
          out.push((msg.data_bytes().unwrap_or_default(), usize::from(ends_message)));
        }
      }
    }
    if !self.coalesce_buffer.is_empty() {
      out.push((self.coalesce_buffer.split().freeze(), completed));
    }
  }

  /// Carves headers out of the reusable header slab, pairing them with the
  /// original message payloads. No copy of the message payload occurs.
  pub fn frame_vectored(&mut self, batch: &[FrameBatch]) -> Result<Vec<Bytes>, ZmqError> {
    let total_frames: usize = batch.iter().map(|g| g.len()).sum();
    let mut out = Vec::with_capacity(total_frames * 2);

    let required_header_space = total_frames * 9;
    if self.header_slab.remaining_mut() < required_header_space {
      self.header_slab = BytesMut::with_capacity(required_header_space.max(4096));
    }

    for group in batch {
      for msg in group {
        let payload = msg.data_bytes().unwrap_or_default();
        let len = payload.len();
        let is_more = msg.flags().contains(MsgFlags::MORE);

        if len <= 255 {
          self.header_slab.put_u8(if is_more { 0x01 } else { 0x00 });
          self.header_slab.put_u8(len as u8);
        } else {
          self.header_slab.put_u8(if is_more { 0x03 } else { 0x02 });
          self.header_slab.put_u64(len as u64);
        }

        out.push(self.header_slab.split().freeze());

        if !payload.is_empty() {
          out.push(payload);
        }
      }
    }

    Ok(out)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::Msg;

  #[test]
  fn test_contiguous_framer_correctness() {
    let mut enc = ZmtpFrameEncoder::new(1024, 1024);
    let batch: Vec<FrameBatch> = vec![
      FrameBatch::from(vec![Msg::from_static(b"hello")]),
      FrameBatch::from(vec![Msg::from_static(b"world")]),
    ];

    let result = enc.frame_contiguous(&batch).unwrap();

    assert_eq!(result.len(), 14);
    assert_eq!(&result[..2], &[0x00, 5]);
    assert_eq!(&result[2..7], b"hello");
    assert_eq!(&result[7..9], &[0x00, 5]);
    assert_eq!(&result[9..14], b"world");
  }

  #[test]
  fn test_zero_reallocation_under_steady_state() {
    let mut enc = ZmtpFrameEncoder::new(4096, 4096);
    let batch: Vec<FrameBatch> = vec![FrameBatch::from(vec![Msg::from_static(b"static-size")])];

    let _ = enc.frame_contiguous(&batch).unwrap();
    let initial_cap = enc.coalesce_buffer.capacity();

    for _ in 0..100 {
      let _ = enc.frame_contiguous(&batch).unwrap();
      // split() gives away the head of the allocation so capacity shrinks slightly,
      // but must never jump up — that would indicate a new heap allocation.
      assert!(
        enc.coalesce_buffer.capacity() <= initial_cap,
        "capacity grew, indicating an unexpected re-allocation"
      );
    }
  }

  #[test]
  fn test_vectored_header_carving() {
    let mut enc = ZmtpFrameEncoder::new(1024, 1024);
    let batch: Vec<FrameBatch> = vec![FrameBatch::from(vec![Msg::from_static(b"payload")])];

    let slices = enc.frame_vectored(&batch).unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].len(), 2);
    assert_eq!(&slices[0][..], &[0x00, 7]);

    let original_ptr = batch[0][0].data().unwrap().as_ptr();
    let vectored_ptr = slices[1].as_ref().as_ptr();
    assert_eq!(
      original_ptr, vectored_ptr,
      "Zero-copy pointer matching failed!"
    );
  }

  fn mixed_batch(seed: u64, messages: usize) -> Vec<FrameBatch> {
    let mut state = seed | 1;
    let mut next = move || {
      state ^= state << 13;
      state ^= state >> 7;
      state ^= state << 17;
      state
    };
    let sizes = [0usize, 1, 200, 255, 256, 4000, 8191, 8192, 20000, 70000];
    (0..messages)
      .map(|_| {
        let frames = 1 + (next() % 3) as usize;
        let msgs: Vec<Msg> = (0..frames)
          .map(|f| {
            let len = sizes[(next() % sizes.len() as u64) as usize];
            let mut msg = Msg::from_vec((0..len).map(|b| (b % 251) as u8).collect());
            let mut flags = MsgFlags::empty();
            if f + 1 < frames {
              flags |= MsgFlags::MORE;
            }
            if next() % 7 == 0 {
              flags |= MsgFlags::COMMAND;
            }
            msg.set_flags(flags);
            msg
          })
          .collect();
        FrameBatch::from(msgs)
      })
      .collect()
  }

  #[test]
  fn mixed_framing_is_byte_identical_to_contiguous() {
    for seed in 1..200u64 {
      for threshold in [0usize, 1, 256, 8192, usize::MAX] {
        let batch = mixed_batch(seed, 1 + (seed % 12) as usize);
        let expected = ZmtpFrameEncoder::new(64, 64).frame_contiguous(&batch).unwrap();

        let mut chunks = Vec::new();
        ZmtpFrameEncoder::new(64, 64).frame_mixed(&batch, threshold, &mut chunks);
        let joined: Vec<u8> = chunks.iter().flat_map(|(b, _)| b.iter().copied()).collect();
        assert_eq!(joined, &expected[..], "seed {} threshold {}", seed, threshold);

        let counted: usize = chunks.iter().map(|(_, n)| n).sum();
        assert_eq!(counted, batch.len(), "seed {} threshold {}", seed, threshold);
      }
    }
  }

  #[test]
  fn mixed_framing_counts_each_message_on_the_chunk_that_ends_it() {
    let small = Msg::from_static(b"head");
    let mut large = Msg::from_vec(vec![7u8; COPY_THRESHOLD]);
    large.set_flags(MsgFlags::empty());
    let mut first = small.clone();
    first.set_flags(MsgFlags::MORE);
    let batch = vec![FrameBatch::from(vec![first, large]), FrameBatch::from(vec![small])];

    let mut chunks = Vec::new();
    ZmtpFrameEncoder::new(64, 64).frame_mixed(&batch, COPY_THRESHOLD, &mut chunks);
    let counts: Vec<usize> = chunks.iter().map(|(_, n)| *n).collect();
    assert_eq!(counts, vec![0, 1, 1], "run with header, large payload ending message 1, run with message 2");
  }

  #[test]
  fn mixed_framing_passes_large_payloads_without_copying() {
    let payload = Msg::from_vec(vec![3u8; 20000]);
    let original = payload.data().unwrap().as_ptr();
    let batch = vec![FrameBatch::from(vec![payload])];

    let mut chunks = Vec::new();
    ZmtpFrameEncoder::new(64, 64).frame_mixed(&batch, COPY_THRESHOLD, &mut chunks);
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[1].0.as_ptr(), original);
  }

  #[test]
  fn mixed_framing_of_small_messages_is_one_chunk() {
    let batch: Vec<FrameBatch> = (0..50).map(|_| FrameBatch::from(vec![Msg::from_vec(vec![1u8; 1000])])).collect();
    let mut chunks = Vec::new();
    ZmtpFrameEncoder::new(64, 64).frame_mixed(&batch, COPY_THRESHOLD, &mut chunks);
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].1, 50);
  }
}
