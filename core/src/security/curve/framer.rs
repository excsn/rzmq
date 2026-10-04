//! CurveZMQ data phase: every ZMTP frame travels as a MESSAGE command,
//! `\x07MESSAGE` nonce(8) Box[flags(1) + body], as libzmq's curve_encoding_t does.

use crate::error::ZmqError;
use crate::message::{FrameBatch, Msg, MsgFlags};
use crate::protocol::zmtp::manual_parser::ZmtpManualParser;
use crate::security::curve::handshake::{open_afternm, seal_afternm, Key};
use crate::security::framer::encoder::ZmtpFrameEncoder;
use crate::security::framer::ISecureFramer;

use bytes::{Bytes, BytesMut};
use zeroize::Zeroize;

const PREFIX: &[u8] = b"\x07MESSAGE";
const HEADER: usize = PREFIX.len() + 8;
const MAC: usize = 16;

pub(crate) struct CurveMessageFramer {
  parser: ZmtpManualParser,
  encoder: ZmtpFrameEncoder,
  precom: Key,
  encode_prefix: &'static [u8; 16],
  decode_prefix: &'static [u8; 16],
  cn_nonce: u64,
  cn_peer_nonce: u64,
}

impl Drop for CurveMessageFramer {
  fn drop(&mut self) {
    self.precom.zeroize();
  }
}

impl CurveMessageFramer {
  pub(crate) fn new(
    is_server: bool,
    precom: Key,
    cn_nonce: u64,
    cn_peer_nonce: u64,
    max_msg_size: i64,
    sndbatch_count: usize,
    sndbatch_bytes_physical: usize,
  ) -> Self {
    let (encode_prefix, decode_prefix) = if is_server {
      (b"CurveZMQMESSAGES", b"CurveZMQMESSAGEC")
    } else {
      (b"CurveZMQMESSAGEC", b"CurveZMQMESSAGES")
    };
    Self {
      parser: ZmtpManualParser::new(max_msg_size),
      encoder: ZmtpFrameEncoder::new(sndbatch_count * 9, sndbatch_bytes_physical),
      precom,
      encode_prefix,
      decode_prefix,
      cn_nonce,
      cn_peer_nonce,
    }
  }

  fn nonce(prefix: &[u8; 16], counter: u64) -> [u8; 24] {
    let mut n = [0u8; 24];
    n[..16].copy_from_slice(prefix);
    n[16..].copy_from_slice(&counter.to_be_bytes());
    n
  }

  fn wrap(&mut self, msg: &Msg) -> Msg {
    let body = msg.data().unwrap_or(&[]);
    let mut plain = Vec::with_capacity(1 + body.len());
    plain.push((msg.flags() & (MsgFlags::MORE | MsgFlags::COMMAND)).bits());
    plain.extend_from_slice(body);
    let counter = self.cn_nonce;
    self.cn_nonce += 1;
    let boxed = seal_afternm(&plain, &Self::nonce(self.encode_prefix, counter), &self.precom);
    let mut wire = Vec::with_capacity(HEADER + boxed.len());
    wire.extend_from_slice(PREFIX);
    wire.extend_from_slice(&counter.to_be_bytes());
    wire.extend_from_slice(&boxed);
    // libzmq sends MESSAGE as a data frame (no command flag) and accepts either
    // form, so the flag is left off and not required on receipt.
    Msg::from_vec(wire)
  }

  fn unwrap(&mut self, frame: &Msg) -> Result<Msg, ZmqError> {
    let body = frame.data().unwrap_or(&[]);
    if !body.starts_with(PREFIX) {
      return Err(ZmqError::ProtocolViolation(format!(
        "Expected CURVE MESSAGE, got {:02x?}",
        &body[..body.len().min(16)]
      )));
    }
    if body.len() < HEADER + MAC + 1 {
      return Err(ZmqError::ProtocolViolation("CURVE MESSAGE is too short".into()));
    }
    let counter = u64::from_be_bytes(body[PREFIX.len()..HEADER].try_into().unwrap());
    if counter <= self.cn_peer_nonce {
      return Err(ZmqError::ProtocolViolation("CURVE MESSAGE nonce out of sequence".into()));
    }
    let plain = open_afternm(&body[HEADER..], &Self::nonce(self.decode_prefix, counter), &self.precom)
      .ok_or_else(|| ZmqError::AuthenticationFailure("CURVE message authentication failed".into()))?;
    self.cn_peer_nonce = counter;
    let mut msg = Msg::from_vec(plain[1..].to_vec());
    msg.set_flags(MsgFlags::from_bits_truncate(plain[0]) & (MsgFlags::MORE | MsgFlags::COMMAND));
    Ok(msg)
  }

  fn wrap_batch(&mut self, batch: &[FrameBatch]) -> Vec<FrameBatch> {
    batch
      .iter()
      .map(|frames| {
        let mut wrapped = FrameBatch::with_capacity(frames.len());
        for m in frames.iter() {
          wrapped.push(self.wrap(m));
        }
        wrapped
      })
      .collect()
  }
}

impl ISecureFramer for CurveMessageFramer {
  fn try_read_msg(&mut self, network_buffer: &mut BytesMut) -> Result<Option<Msg>, ZmqError> {
    match self.parser.decode_from_buffer(network_buffer)? {
      Some(frame) => Ok(Some(self.unwrap(&frame)?)),
      None => Ok(None),
    }
  }

  fn write_msg_multipart(&mut self, msgs: FrameBatch) -> Result<Bytes, ZmqError> {
    let wrapped = self.wrap_batch(&[msgs]);
    self.encoder.frame_contiguous(&wrapped)
  }

  fn write_msg_batch(&mut self, batch: &[FrameBatch]) -> Result<Bytes, ZmqError> {
    let wrapped = self.wrap_batch(batch);
    self.encoder.frame_contiguous(&wrapped)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::security::curve::handshake::tests::completed_pair;

  fn framer_pair() -> (CurveMessageFramer, CurveMessageFramer) {
    let (client, server) = completed_pair();
    let (ck, cn, cp) = client.into_session().unwrap();
    let (sk, sn, sp) = server.into_session().unwrap();
    (
      CurveMessageFramer::new(false, ck, cn, cp, -1, 128, 65536),
      CurveMessageFramer::new(true, sk, sn, sp, -1, 128, 65536),
    )
  }

  fn read_all(framer: &mut CurveMessageFramer, wire: Bytes) -> Vec<Msg> {
    let mut buf = BytesMut::from(&wire[..]);
    let mut out = Vec::new();
    while let Some(m) = framer.try_read_msg(&mut buf).unwrap() {
      out.push(m);
    }
    assert!(buf.is_empty());
    out
  }

  #[test]
  fn multipart_and_flags_survive_the_round_trip() {
    let (mut client, mut server) = framer_pair();
    let mut first = Msg::from_static(b"first");
    first.set_flags(MsgFlags::MORE);
    let mut ping = Msg::from_static(b"\x04PING\x00\x10ctx");
    ping.set_flags(MsgFlags::COMMAND);
    let batch = FrameBatch::from(vec![first, Msg::from_vec(vec![7u8; 1000]), ping]);
    let wire = client.write_msg_multipart(batch).unwrap();
    let got = read_all(&mut server, wire);
    assert_eq!(got.len(), 3);
    assert_eq!(got[0].data().unwrap(), b"first");
    assert!(got[0].is_more());
    assert_eq!(got[1].size(), 1000);
    assert!(!got[1].is_more());
    assert!(got[2].is_command());
    assert_eq!(got[2].data().unwrap(), b"\x04PING\x00\x10ctx");
  }

  #[test]
  fn wire_is_a_message_frame_with_sequential_nonces() {
    let (mut client, _) = framer_pair();
    let wire = client.write_msg_multipart(FrameBatch::from(vec![Msg::from_static(b"a"), Msg::from_static(b"b")])).unwrap();
    let mut parser = ZmtpManualParser::new(-1);
    let mut buf = BytesMut::from(&wire[..]);
    let one = parser.decode_from_buffer(&mut buf).unwrap().unwrap();
    let two = parser.decode_from_buffer(&mut buf).unwrap().unwrap();
    assert!(!one.is_command() && !one.is_more());
    assert!(one.data().unwrap().starts_with(b"\x07MESSAGE"));
    assert_eq!(&one.data().unwrap()[8..16], &3u64.to_be_bytes());
    assert_eq!(&two.data().unwrap()[8..16], &4u64.to_be_bytes());
  }

  #[test]
  fn replayed_message_is_rejected() {
    let (mut client, mut server) = framer_pair();
    let wire = client.write_msg_multipart(FrameBatch::from(vec![Msg::from_static(b"once")])).unwrap();
    read_all(&mut server, wire.clone());
    let mut buf = BytesMut::from(&wire[..]);
    assert!(matches!(server.try_read_msg(&mut buf), Err(ZmqError::ProtocolViolation(_))));
  }

  #[test]
  fn tampered_message_fails_authentication_with_fixed_message() {
    let (mut client, mut server) = framer_pair();
    let wire = client.write_msg_multipart(FrameBatch::from(vec![Msg::from_static(b"intact")])).unwrap();
    let mut bytes = wire.to_vec();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    let mut buf = BytesMut::from(&bytes[..]);
    match server.try_read_msg(&mut buf) {
      Err(ZmqError::AuthenticationFailure(m)) => assert_eq!(m, "CURVE message authentication failed"),
      other => panic!("got {:?}", other),
    }
  }
}
