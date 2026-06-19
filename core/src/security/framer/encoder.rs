use crate::error::ZmqError;
use crate::message::{FrameBatch, MsgFlags};
use bytes::{BufMut, Bytes, BytesMut};

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
  use smallvec::smallvec;

  #[test]
  fn test_contiguous_framer_correctness() {
    let mut enc = ZmtpFrameEncoder::new(1024, 1024);
    let batch: Vec<FrameBatch> = vec![
      smallvec![Msg::from_static(b"hello")],
      smallvec![Msg::from_static(b"world")],
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
    let batch: Vec<FrameBatch> = vec![smallvec![Msg::from_static(b"static-size")]];

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
    let batch: Vec<FrameBatch> = vec![smallvec![Msg::from_static(b"payload")]];

    let slices = enc.frame_vectored(&batch).unwrap();
    assert_eq!(slices.len(), 2);
    assert_eq!(slices[0].len(), 2);
    assert_eq!(&slices[0][..], &[0x00, 7]);

    let original_ptr = batch[0][0].data().unwrap().as_ptr();
    let vectored_ptr = slices[1].as_ref().as_ptr();
    assert_eq!(original_ptr, vectored_ptr, "Zero-copy pointer matching failed!");
  }
}
