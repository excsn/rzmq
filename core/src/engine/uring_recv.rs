// src/engine/uring_recv.rs

#![cfg(feature = "io-uring")] // This whole module is io-uring specific

use crate::error::ZmqError;
use crate::message::Msg;
use crate::protocol::zmtp::codec::ZmtpCodec;

use std::collections::VecDeque;
use std::{future::Future, pin::Pin};

use bytes::{BufMut, BytesMut}; // BufMut for extend_from_slice
use tokio_uring::{BufResult, net::TcpStream as UringTcpStream};
use tokio_util::codec::Decoder;

const MIN_BUFFER_CAPACITY_FOR_REUSE: usize = 1024; // If buffer becomes too small after partial consume, reallocate

#[derive(Debug)]
pub(crate) struct UringMultishotReceiver {
  /// Buffers currently available to be submitted to an io_uring recv_multishot operation.
  /// These buffers are "owned" by the pool and are lent out.
  available_buffers: Vec<BytesMut>,
  /// The ZMTP codec, stateful for handling partial frames across buffer completions.
  codec: ZmtpCodec,
  /// Configured number of buffers in the pool.
  config_num_buffers: usize,
  /// Configured capacity of each individual buffer when (re)allocated.
  config_buffer_capacity: usize,
}

impl UringMultishotReceiver {
  pub fn new(num_buffers: usize, capacity_per_buffer: usize) -> Self {
    assert!(num_buffers > 0, "num_buffers must be positive");
    assert!(capacity_per_buffer > 0, "capacity_per_buffer must be positive");

    let mut available_buffers = Vec::with_capacity(num_buffers);
    for _ in 0..num_buffers {
      // Initialize with capacity. Length is 0.
      available_buffers.push(BytesMut::with_capacity(capacity_per_buffer));
    }

    Self {
      available_buffers,
      codec: ZmtpCodec::new(), // Each receiver manages its own codec state
      config_num_buffers: num_buffers,
      config_buffer_capacity: capacity_per_buffer,
    }
  }
  /// Takes all currently available buffers from the pool for submission to `recv_multishot`.
  ///
  /// The caller (`ZmtpEngineCore`) is responsible for passing these to the `io_uring` operation
  /// and then returning them via `process_completed_submission`.
  ///
  /// Returns `Some(Vec<BytesMut>)` if buffers were available, `None` otherwise.
  /// The returned `Vec<BytesMut>` will be empty if no buffers were available.
  pub fn take_buffers_for_submission(&mut self) -> Option<Vec<BytesMut>> {
    if self.available_buffers.is_empty() {
      None
    } else {
      // Drain all available buffers to pass to io_uring
      let buffers_to_submit = std::mem::take(&mut self.available_buffers);
      // Ensure they are ready for new data (they should be if recycled correctly)
      // for buf in &mut buffers_to_submit {
      //     buf.clear(); // Should already be clear from recycling
      // }
      Some(buffers_to_submit)
    }
  }

  /// Processes buffers returned from a completed `recv_multishot` operation.
  ///
  /// Decodes ZMTP messages from the filled buffers and recycles all buffers (filled or not)
  /// back into the available pool.
  ///
  /// # Arguments
  /// * `num_buffers_filled`: The number of buffers (from the start of `buffers_returned_from_uring`)
  ///   that were actually filled with data by `io_uring`.
  /// * `buffers_returned_from_uring`: The `Vec<BytesMut>` that was originally taken by
  ///   `take_buffers_for_submission` and is now returned by `recv_multishot`.
  ///   The `len()` of each `BytesMut` up to `num_buffers_filled` should reflect the
  ///   amount of data written into it by the kernel.
  ///
  /// # Returns
  /// `Ok(Vec<Msg>)` containing all fully decoded messages, or an `Err(ZmqError)` if
  /// a decoding error occurs.
  pub fn process_completed_submission(
    &mut self,
    num_buffers_filled: usize,
    mut buffers_returned_from_uring: Vec<BytesMut>, // Takes ownership
  ) -> Result<Vec<Msg>, ZmqError> {
    let mut all_decoded_msgs: Vec<Msg> = Vec::new();

    // Buffer to accumulate data for the codec, including any prefix from previous partial decodes.
    // The codec's internal prefix_bytes will be used.
    // We feed one filled uring buffer at a time (plus codec's prefix) to the codec.
    let mut current_chunk_for_codec = self.codec.take_prefix_bytes().take().unwrap_or_else(BytesMut::new);

    for i in 0..buffers_returned_from_uring.len() {
      let mut processed_buffer = buffers_returned_from_uring.remove(0); // Take ownership of first buffer

      if i < num_buffers_filled {
        // This buffer was reported as filled by io_uring. Its `len()` should be > 0.
        // The `recv_multishot` API that takes `Vec<BytesMut>` should have updated
        // the length of the BytesMut to indicate how many bytes were read.
        if processed_buffer.is_empty() {
          // This case should ideally not happen if num_buffers_filled is accurate
          // and indicates this buffer slot was used. Could be a 0-byte read though.
          tracing::trace!("UringMultishotReceiver: Buffer {} reported as filled but is empty.", i);
        }
        current_chunk_for_codec.put_slice(&processed_buffer[..]); // Append new data
      }

      // Regardless of whether it was filled or not in *this* uring op,
      // try to decode from current_chunk_for_codec (which includes new data + previous prefix).
      // This loop is needed because one filled buffer might contain multiple ZMTP messages,
      // or contribute to completing a message started in a previous buffer.
      loop {
        match self.codec.decode(&mut current_chunk_for_codec) {
          Ok(Some(msg)) => {
            all_decoded_msgs.push(msg);
          }
          Ok(None) => {
            // Codec needs more data, current_chunk_for_codec contains the partial frame.
            break;
          }
          Err(e) => {
            // Decoding error. Store remaining data as prefix and return error.
            self.codec.set_prefix_bytes(Some(current_chunk_for_codec));
            // Recycle the original processed_buffer (and any others remaining in buffers_returned_from_uring)
            processed_buffer.clear();
            self.available_buffers.push(processed_buffer);
            for mut b in buffers_returned_from_uring {
              // Recycle remaining
              b.clear();
              self.available_buffers.push(b);
            }
            return Err(e);
          }
        }
      }
      // After attempting to decode, recycle the processed_buffer shell.
      // The actual data (or its remainder) is now in current_chunk_for_codec.
      processed_buffer.clear();
      // Ensure capacity is still good, reallocate if it shrunk too much (less likely with BytesMut)
      if processed_buffer.capacity() < MIN_BUFFER_CAPACITY_FOR_REUSE.min(self.config_buffer_capacity) {
        self
          .available_buffers
          .push(BytesMut::with_capacity(self.config_buffer_capacity));
      } else {
        self.available_buffers.push(processed_buffer);
      }
    }

    // After processing all buffers from this uring completion,
    // any remaining data in current_chunk_for_codec is the new prefix.
    if !current_chunk_for_codec.is_empty() {
      self.codec.set_prefix_bytes(Some(current_chunk_for_codec));
    } else {
      self.codec.set_prefix_bytes(None);
    }

    Ok(all_decoded_msgs)
  }

  pub fn has_available_buffers(&self) -> bool {
    !self.available_buffers.is_empty()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::message::{Msg, MsgFlags}; // For creating test messages
  use bytes::BufMut;
use tokio_util::codec::Encoder; // For BytesMut::put_slice

  // Helper to create a ZMTP-framed message in a BytesMut
  // This bypasses needing a full engine to send.
  fn create_zmtp_frame(payload: &[u8], is_more: bool) -> BytesMut {
    let mut codec = ZmtpCodec::new();
    let mut msg = Msg::from_vec(payload.to_vec());
    if is_more {
      msg.set_flags(MsgFlags::MORE);
    }
    let mut buf = BytesMut::new();
    codec.encode(msg, &mut buf).unwrap(); // Assuming encode doesn't fail for this
    buf
  }

  #[test]
  fn test_uring_receiver_new() {
    let receiver = UringMultishotReceiver::new(5, 1024);
    assert_eq!(receiver.config_num_buffers, 5);
    assert_eq!(receiver.config_buffer_capacity, 1024);
    assert_eq!(receiver.available_buffers.len(), 5);
    for buf in &receiver.available_buffers {
      assert_eq!(buf.capacity(), 1024);
      assert_eq!(buf.len(), 0);
    }
    assert!(receiver.codec.prefix_bytes().is_none());
  }

  #[test]
  #[should_panic]
  fn test_uring_receiver_new_zero_buffers() {
    UringMultishotReceiver::new(0, 1024);
  }

  #[test]
  #[should_panic]
  fn test_uring_receiver_new_zero_capacity() {
    UringMultishotReceiver::new(5, 0);
  }

  #[test]
  fn test_take_buffers_for_submission() {
    let mut receiver = UringMultishotReceiver::new(3, 128);
    assert!(receiver.has_available_buffers());

    let taken_option = receiver.take_buffers_for_submission();
    assert!(taken_option.is_some());
    let taken = taken_option.unwrap();
    assert_eq!(taken.len(), 3);
    for buf in &taken {
      assert_eq!(buf.capacity(), 128);
    }
    assert!(receiver.available_buffers.is_empty());
    assert!(!receiver.has_available_buffers());

    // Simulate returning them (though process_completed_submission does this)
    receiver.available_buffers = taken; // Simplified for this test of 'take'

    let taken_again = receiver.take_buffers_for_submission().unwrap();
    assert_eq!(taken_again.len(), 3);
    assert!(receiver.available_buffers.is_empty());

    let no_buffers_taken = receiver.take_buffers_for_submission();
    assert!(no_buffers_taken.is_none()); // Or Some(empty_vec) depending on impl. Current impl is None.
  }

  #[test]
  fn test_process_submission_single_full_frame() {
    let mut receiver = UringMultishotReceiver::new(1, 1024);
    let payload = b"Hello";
    let zmtp_frame = create_zmtp_frame(payload, false);

    let mut buffer_from_uring = BytesMut::with_capacity(1024);
    buffer_from_uring.put_slice(&zmtp_frame); // Simulate kernel filling the buffer

    let mut returned_vec = Vec::new();
    returned_vec.push(buffer_from_uring);

    let result = receiver.process_completed_submission(1, returned_vec);
    assert!(result.is_ok());
    let decoded_msgs = result.unwrap();
    assert_eq!(decoded_msgs.len(), 1);
    assert_eq!(decoded_msgs[0].data().unwrap(), payload);
    assert!(!decoded_msgs[0].is_more());
    assert!(receiver.codec.prefix_bytes().is_none());
    assert_eq!(receiver.available_buffers.len(), 1); // Buffer recycled
  }

  #[test]
  fn test_process_submission_multiple_frames_in_one_buffer() {
    let mut receiver = UringMultishotReceiver::new(1, 1024);
    let payload1 = b"Frame1";
    let payload2 = b"Frame2Next";
    let mut combined_zmtp_frames = create_zmtp_frame(payload1, true); // MORE = true
    combined_zmtp_frames.extend_from_slice(&create_zmtp_frame(payload2, false));

    let mut buffer_from_uring = BytesMut::with_capacity(1024);
    buffer_from_uring.put_slice(&combined_zmtp_frames);

    let result = receiver.process_completed_submission(1, vec![buffer_from_uring]);
    assert!(result.is_ok());
    let decoded_msgs = result.unwrap();
    assert_eq!(decoded_msgs.len(), 2);
    assert_eq!(decoded_msgs[0].data().unwrap(), payload1);
    assert!(decoded_msgs[0].is_more());
    assert_eq!(decoded_msgs[1].data().unwrap(), payload2);
    assert!(!decoded_msgs[1].is_more());
    assert!(receiver.codec.prefix_bytes().is_none() || receiver.codec.prefix_bytes().unwrap().is_empty());
    assert_eq!(receiver.available_buffers.len(), 1);
  }

  #[test]
  fn test_process_submission_frame_spanning_two_buffers() {
    let mut receiver = UringMultishotReceiver::new(2, 128); // Need at least 2 buffers
    let payload = b"ThisIsAVeryLongPayloadThatWillDefinitelySpanAcrossBuffersWhenSmallBuffersAreUsed";
    let zmtp_frame_full = create_zmtp_frame(payload, false);

    let split_point = zmtp_frame_full.len() / 2;
    let mut part1_buf = BytesMut::with_capacity(128);
    part1_buf.put_slice(&zmtp_frame_full[..split_point]);
    let mut part2_buf = BytesMut::with_capacity(128);
    part2_buf.put_slice(&zmtp_frame_full[split_point..]);

    // Simulate first completion
    let result1 = receiver.process_completed_submission(1, vec![part1_buf]);
    assert!(result1.is_ok());
    let decoded_msgs1 = result1.unwrap();
    assert!(decoded_msgs1.is_empty()); // No full message yet
    assert!(receiver.codec.prefix_bytes().is_some());
    assert!(!receiver.codec.prefix_bytes().unwrap().is_empty());
    assert_eq!(receiver.available_buffers.len(), 1); // Buffer1 recycled

    // Simulate second completion
    let result2 = receiver.process_completed_submission(1, vec![part2_buf]);
    assert!(result2.is_ok());
    let decoded_msgs2 = result2.unwrap();
    assert_eq!(decoded_msgs2.len(), 1);
    assert_eq!(decoded_msgs2[0].data().unwrap(), payload);
    assert!(!decoded_msgs2[0].is_more());
    assert!(receiver.codec.prefix_bytes().is_none() || receiver.codec.prefix_bytes().as_ref().unwrap().is_empty());
    assert_eq!(receiver.available_buffers.len(), 2); // Buffer2 recycled
  }

  #[test]
  fn test_process_submission_full_frame_and_partial_next() {
    let mut receiver = UringMultishotReceiver::new(1, 1024);
    let payload1 = b"CompleteFrame";
    let payload2_partial_data = b"StartOfNext"; // Incomplete ZMTP frame

    let mut zmtp_frame1 = create_zmtp_frame(payload1, false);
    let mut combined_data = BytesMut::new();
    combined_data.put_slice(&zmtp_frame1);
    combined_data.put_slice(payload2_partial_data); // This is not a valid ZMTP frame start on its own

    let mut buffer_from_uring = BytesMut::with_capacity(1024);
    buffer_from_uring.put_slice(&combined_data);

    let result = receiver.process_completed_submission(1, vec![buffer_from_uring]);
    assert!(result.is_ok());
    let decoded_msgs = result.unwrap();
    assert_eq!(decoded_msgs.len(), 1);
    assert_eq!(decoded_msgs[0].data().unwrap(), payload1);
    assert!(receiver.codec.prefix_bytes().is_some());
    assert_eq!(
      receiver.codec.prefix_bytes().unwrap().as_ref(),
      payload2_partial_data
    );
    assert_eq!(receiver.available_buffers.len(), 1);
  }

  #[test]
  fn test_process_submission_no_filled_buffers() {
    let mut receiver = UringMultishotReceiver::new(3, 128);
    let mut bufs = Vec::new();
    for _ in 0..3 {
      bufs.push(BytesMut::with_capacity(128));
    }

    let result = receiver.process_completed_submission(0, bufs); // num_filled = 0
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
    assert!(receiver.codec.prefix_bytes().is_none());
    assert_eq!(receiver.available_buffers.len(), 3); // All buffers recycled
  }

  #[test]
  fn test_process_submission_some_filled_some_not() {
    let mut receiver = UringMultishotReceiver::new(3, 1024);
    let payload = b"TestData";
    let zmtp_frame = create_zmtp_frame(payload, false);

    let mut buf1_filled = BytesMut::with_capacity(1024);
    buf1_filled.put_slice(&zmtp_frame);
    let buf2_empty = BytesMut::with_capacity(1024); // Not filled by uring
    let buf3_filled_also = create_zmtp_frame(b"Another", false); // Another frame
    let mut buf3_filled_final = BytesMut::with_capacity(1024);
    buf3_filled_final.put_slice(&buf3_filled_also);

    let mut bufs_from_uring = vec![buf1_filled, buf2_empty, buf3_filled_final];

    // Simulate uring filled buffer 0 and 2, but not 1.
    // Our process_completed_submission iterates 0..len(), but checks i < num_filled.
    // This setup is a bit artificial for the current `process_completed_submission`
    // which expects the first `num_filled` buffers in `bufs_from_uring` to be the ones with data.
    // Let's adjust the test to match how `recv_multishot` likely returns buffers.
    // `recv_multishot` typically returns the *original vec*, with the first `num_filled`
    // elements modified (their len updated).

    let mut buffer_from_uring1 = BytesMut::with_capacity(1024);
    buffer_from_uring1.put_slice(&zmtp_frame);
    let mut buffer_from_uring2_unused = BytesMut::with_capacity(1024); // Will be "unused" by this call

    // For this test, let's say only 1 buffer was filled in this io_uring op
    let result = receiver.process_completed_submission(1, vec![buffer_from_uring1, buffer_from_uring2_unused]);
    assert!(result.is_ok());
    let msgs = result.unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].data().unwrap(), payload);
    assert!(receiver.codec.prefix_bytes().is_none() || receiver.codec.prefix_bytes().unwrap().is_empty());
    assert_eq!(receiver.available_buffers.len(), 2); // Both original buffers should be recycled.
  }
}
