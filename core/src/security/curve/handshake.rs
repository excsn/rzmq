//! CurveZMQ handshake (RFC 26), byte-compatible with libzmq's curve_client and
//! curve_server. Short-term nonces are a 16-byte prefix and a big-endian u64.

use crate::error::ZmqError;
use crate::protocol::zmtp::ZmtpReady;
use crate::socket::options::ZmtpEngineConfig;

use std::collections::HashMap;

use bytes::BytesMut;
use dryoc::classic::crypto_box::{
  crypto_box_beforenm, crypto_box_detached, crypto_box_detached_afternm, crypto_box_keypair,
  crypto_box_open_detached, crypto_box_open_detached_afternm,
};
use dryoc::classic::crypto_secretbox::{crypto_secretbox_detached, crypto_secretbox_open_detached};
use dryoc::keypair::StackKeyPair;
use dryoc::types::ByteArray;
use zeroize::Zeroize;

pub(crate) type Key = [u8; 32];
type Nonce = [u8; 24];
const MAC: usize = 16;
const COOKIE_LEN: usize = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurveHandshakePhase {
  ClientSendHello,
  ClientExpectWelcome,
  ClientSendInitiate,
  ClientExpectReady,
  ServerExpectHello,
  ServerSendWelcome,
  ServerExpectInitiate,
  ServerSendReady,
  Complete,
  Error,
}

pub(crate) struct CurveHandshake {
  pub(crate) phase: CurveHandshakePhase,
  pub(crate) is_server: bool,
  static_pk: Key,
  static_sk: Key,
  /// The server's long-term public key: configured on a client, our own on a server.
  server_pk: Key,
  cn_pk: Key,
  cn_sk: Key,
  /// The peer's short-term public key: C' on the server, S' on the client.
  cn_peer: Option<Key>,
  cookie_key: Option<Key>,
  cn_cookie: Option<[u8; COOKIE_LEN]>,
  precom: Option<Key>,
  cn_nonce: u64,
  cn_peer_nonce: u64,
  local_metadata: HashMap<String, Vec<u8>>,
  pub(crate) peer_metadata: Option<HashMap<String, Vec<u8>>>,
}

impl std::fmt::Debug for CurveHandshake {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("CurveHandshake")
      .field("phase", &self.phase)
      .field("is_server", &self.is_server)
      .finish_non_exhaustive()
  }
}

impl Drop for CurveHandshake {
  fn drop(&mut self) {
    self.static_sk.zeroize();
    self.cn_sk.zeroize();
    if let Some(k) = self.cookie_key.as_mut() {
      k.zeroize();
    }
    if let Some(k) = self.precom.as_mut() {
      k.zeroize();
    }
  }
}

fn nonce(prefix: &[u8], suffix: &[u8]) -> Nonce {
  let mut n = [0u8; 24];
  n[..prefix.len()].copy_from_slice(prefix);
  n[prefix.len()..prefix.len() + suffix.len()].copy_from_slice(suffix);
  n
}

/// MAC followed by ciphertext, as libzmq lays out every box after its BOXZEROBYTES.
fn seal(plain: &[u8], n: &Nonce, pk: &Key, sk: &Key) -> Vec<u8> {
  let mut out = vec![0u8; MAC + plain.len()];
  let (mac, cipher) = out.split_at_mut(MAC);
  crypto_box_detached(cipher, mac.try_into().unwrap(), plain, n, pk, sk);
  out
}

fn open(sealed: &[u8], n: &Nonce, pk: &Key, sk: &Key) -> Option<Vec<u8>> {
  if sealed.len() < MAC {
    return None;
  }
  let (mac, cipher) = sealed.split_at(MAC);
  let mut plain = vec![0u8; cipher.len()];
  crypto_box_open_detached(&mut plain, mac.try_into().unwrap(), cipher, n, pk, sk).ok()?;
  Some(plain)
}

pub(crate) fn seal_afternm(plain: &[u8], n: &Nonce, key: &Key) -> Vec<u8> {
  let mut out = vec![0u8; MAC + plain.len()];
  let (mac, cipher) = out.split_at_mut(MAC);
  crypto_box_detached_afternm(cipher, mac.try_into().unwrap(), plain, n, key);
  out
}

pub(crate) fn open_afternm(sealed: &[u8], n: &Nonce, key: &Key) -> Option<Vec<u8>> {
  if sealed.len() < MAC {
    return None;
  }
  let (mac, cipher) = sealed.split_at(MAC);
  let mut plain = vec![0u8; cipher.len()];
  crypto_box_open_detached_afternm(&mut plain, mac.try_into().unwrap(), cipher, n, key).ok()?;
  Some(plain)
}

fn secret_seal(plain: &[u8], n: &Nonce, key: &Key) -> Vec<u8> {
  let mut out = vec![0u8; MAC + plain.len()];
  let (mac, cipher) = out.split_at_mut(MAC);
  crypto_secretbox_detached(cipher, mac.try_into().unwrap(), plain, n, key);
  out
}

fn secret_open(sealed: &[u8], n: &Nonce, key: &Key) -> Option<Vec<u8>> {
  if sealed.len() < MAC {
    return None;
  }
  let (mac, cipher) = sealed.split_at(MAC);
  let mut plain = vec![0u8; cipher.len()];
  crypto_secretbox_open_detached(&mut plain, mac.try_into().unwrap(), cipher, n, key).ok()?;
  Some(plain)
}

fn random<const N: usize>() -> [u8; N] {
  let mut out = [0u8; N];
  rand::fill(&mut out[..]);
  out
}

impl CurveHandshake {
  pub(crate) const HELLO_LEN: usize = 200;
  pub(crate) const WELCOME_LEN: usize = 168;
  const INITIATE_MIN_LEN: usize = 257;
  const READY_MIN_LEN: usize = 30;

  pub(crate) fn new(
    is_server: bool,
    config: &ZmtpEngineConfig,
    local_metadata: HashMap<String, Vec<u8>>,
  ) -> Result<Self, ZmqError> {
    let static_sk = config.curve_local_secret_key.ok_or(ZmqError::InvalidCurveKey)?;
    let static_pk = *StackKeyPair::from_secret_key(static_sk.into()).public_key.as_array();
    let server_pk = if is_server {
      static_pk
    } else {
      config.curve_remote_public_key.ok_or(ZmqError::InvalidCurveKey)?
    };
    let (cn_pk, cn_sk) = crypto_box_keypair();
    Ok(Self {
      phase: if is_server {
        CurveHandshakePhase::ServerExpectHello
      } else {
        CurveHandshakePhase::ClientSendHello
      },
      is_server,
      static_pk,
      static_sk,
      server_pk,
      cn_pk,
      cn_sk,
      cn_peer: None,
      cookie_key: None,
      cn_cookie: None,
      precom: None,
      cn_nonce: 1,
      cn_peer_nonce: 1,
      local_metadata,
      peer_metadata: None,
    })
  }

  fn next_nonce(&mut self) -> [u8; 8] {
    let n = self.cn_nonce;
    self.cn_nonce += 1;
    n.to_be_bytes()
  }

  fn fail(&mut self, message: &str) -> ZmqError {
    self.phase = CurveHandshakePhase::Error;
    ZmqError::SecurityError(message.into())
  }

  fn metadata_bytes(&self) -> Vec<u8> {
    let mut buf = BytesMut::new();
    ZmtpReady {
      properties: self.local_metadata.clone(),
    }
    .encode_properties(&mut buf);
    buf.to_vec()
  }

  fn take_metadata(&mut self, bytes: &[u8]) -> Result<(), ZmqError> {
    match ZmtpReady::parse_properties(bytes) {
      Ok(ready) => {
        self.peer_metadata = Some(ready.properties);
        Ok(())
      }
      Err(_) => Err(self.fail("CURVE metadata is malformed")),
    }
  }

  /// HELLO: prefix(6) version(2) padding(72) C'(32) nonce(8) Box[64 zeros](C'->S)(80).
  pub(crate) fn build_client_hello(&mut self) -> Result<Vec<u8>, ZmqError> {
    let short_nonce = self.next_nonce();
    let n = nonce(b"CurveZMQHELLO---", &short_nonce);
    let signature = seal(&[0u8; 64], &n, &self.server_pk, &self.cn_sk);
    let mut hello = Vec::with_capacity(Self::HELLO_LEN);
    hello.extend_from_slice(b"\x05HELLO");
    hello.extend_from_slice(&[1, 0]);
    hello.extend_from_slice(&[0u8; 72]);
    hello.extend_from_slice(&self.cn_pk);
    hello.extend_from_slice(&short_nonce);
    hello.extend_from_slice(&signature);
    debug_assert_eq!(hello.len(), Self::HELLO_LEN);
    self.phase = CurveHandshakePhase::ClientExpectWelcome;
    Ok(hello)
  }

  pub(crate) fn process_client_hello(&mut self, hello: &[u8]) -> Result<(), ZmqError> {
    if !hello.starts_with(b"\x05HELLO") {
      return Err(self.fail("Expected CURVE HELLO"));
    }
    if hello.len() != Self::HELLO_LEN {
      return Err(self.fail("CURVE HELLO has the wrong size"));
    }
    if hello[6..8] != [1, 0] {
      return Err(self.fail("CURVE HELLO has an unknown version"));
    }
    let cn_client: Key = hello[80..112].try_into().unwrap();
    let n = nonce(b"CurveZMQHELLO---", &hello[112..120]);
    self.cn_peer_nonce = u64::from_be_bytes(hello[112..120].try_into().unwrap());
    if open(&hello[120..200], &n, &cn_client, &self.static_sk).is_none() {
      return Err(self.fail("CURVE HELLO could not be decrypted (client has the wrong server key?)"));
    }
    self.cn_peer = Some(cn_client);
    self.phase = CurveHandshakePhase::ServerSendWelcome;
    Ok(())
  }

  /// WELCOME: prefix(8) nonce(16) Box[S' + cookie](S->C')(144).
  /// cookie = nonce(16) + SecretBox[C' + s'](cookie key)(80).
  pub(crate) fn build_server_welcome(&mut self) -> Result<Vec<u8>, ZmqError> {
    let cn_client = self.cn_peer.ok_or_else(|| self.fail("CURVE WELCOME before HELLO"))?;
    let cookie_key: Key = random();
    let cookie_nonce: [u8; 16] = random();
    let mut cookie_plain = [0u8; 64];
    cookie_plain[..32].copy_from_slice(&cn_client);
    cookie_plain[32..].copy_from_slice(&self.cn_sk);
    let cookie_box = secret_seal(&cookie_plain, &nonce(b"COOKIE--", &cookie_nonce), &cookie_key);
    cookie_plain.zeroize();
    self.cookie_key = Some(cookie_key);

    let welcome_nonce: [u8; 16] = random();
    let mut welcome_plain = Vec::with_capacity(128);
    welcome_plain.extend_from_slice(&self.cn_pk);
    welcome_plain.extend_from_slice(&cookie_nonce);
    welcome_plain.extend_from_slice(&cookie_box);
    let welcome_box = seal(&welcome_plain, &nonce(b"WELCOME-", &welcome_nonce), &cn_client, &self.static_sk);

    let mut welcome = Vec::with_capacity(Self::WELCOME_LEN);
    welcome.extend_from_slice(b"\x07WELCOME");
    welcome.extend_from_slice(&welcome_nonce);
    welcome.extend_from_slice(&welcome_box);
    debug_assert_eq!(welcome.len(), Self::WELCOME_LEN);
    self.phase = CurveHandshakePhase::ServerExpectInitiate;
    Ok(welcome)
  }

  pub(crate) fn process_server_welcome(&mut self, welcome: &[u8]) -> Result<(), ZmqError> {
    if !welcome.starts_with(b"\x07WELCOME") {
      return Err(self.fail("Expected CURVE WELCOME"));
    }
    if welcome.len() != Self::WELCOME_LEN {
      return Err(self.fail("CURVE WELCOME has the wrong size"));
    }
    let n = nonce(b"WELCOME-", &welcome[8..24]);
    let plain = match open(&welcome[24..168], &n, &self.server_pk, &self.cn_sk) {
      Some(p) => p,
      None => return Err(self.fail("CURVE WELCOME could not be decrypted (wrong server public key?)")),
    };
    let cn_server: Key = plain[..32].try_into().unwrap();
    self.cn_cookie = Some(plain[32..128].try_into().unwrap());
    self.precom = Some(crypto_box_beforenm(&cn_server, &self.cn_sk));
    self.cn_peer = Some(cn_server);
    self.phase = CurveHandshakePhase::ClientSendInitiate;
    Ok(())
  }

  /// INITIATE: prefix(9) cookie(96) nonce(8) Box[C + vouch nonce(16) + vouch(80) + metadata](C'->S').
  /// vouch = Box[C' + S](C->S').
  pub(crate) fn build_client_initiate(&mut self) -> Result<Vec<u8>, ZmqError> {
    let (cn_server, cookie, precom) = match (self.cn_peer, self.cn_cookie, self.precom) {
      (Some(s), Some(c), Some(p)) => (s, c, p),
      _ => return Err(self.fail("CURVE INITIATE before WELCOME")),
    };
    let vouch_nonce: [u8; 16] = random();
    let mut vouch_plain = [0u8; 64];
    vouch_plain[..32].copy_from_slice(&self.cn_pk);
    vouch_plain[32..].copy_from_slice(&self.server_pk);
    let vouch = seal(&vouch_plain, &nonce(b"VOUCH---", &vouch_nonce), &cn_server, &self.static_sk);

    let metadata = self.metadata_bytes();
    let mut plain = Vec::with_capacity(128 + metadata.len());
    plain.extend_from_slice(&self.static_pk);
    plain.extend_from_slice(&vouch_nonce);
    plain.extend_from_slice(&vouch);
    plain.extend_from_slice(&metadata);

    let short_nonce = self.next_nonce();
    let boxed = seal_afternm(&plain, &nonce(b"CurveZMQINITIATE", &short_nonce), &precom);

    let mut initiate = Vec::with_capacity(113 + boxed.len());
    initiate.extend_from_slice(b"\x08INITIATE");
    initiate.extend_from_slice(&cookie);
    initiate.extend_from_slice(&short_nonce);
    initiate.extend_from_slice(&boxed);
    self.phase = CurveHandshakePhase::ClientExpectReady;
    Ok(initiate)
  }

  pub(crate) fn process_client_initiate(&mut self, initiate: &[u8]) -> Result<(), ZmqError> {
    if !initiate.starts_with(b"\x08INITIATE") {
      return Err(self.fail("Expected CURVE INITIATE"));
    }
    if initiate.len() < Self::INITIATE_MIN_LEN {
      return Err(self.fail("CURVE INITIATE is too short"));
    }
    let (cn_client, cookie_key) = match (self.cn_peer, self.cookie_key) {
      (Some(c), Some(k)) => (c, k),
      _ => return Err(self.fail("CURVE INITIATE before WELCOME")),
    };
    let cookie_plain = match secret_open(&initiate[25..105], &nonce(b"COOKIE--", &initiate[9..25]), &cookie_key) {
      Some(p) => p,
      None => return Err(self.fail("CURVE INITIATE cookie could not be decrypted")),
    };
    if cookie_plain[..32] != cn_client || cookie_plain[32..64] != self.cn_sk {
      return Err(self.fail("CURVE INITIATE cookie is not valid"));
    }

    let precom = crypto_box_beforenm(&cn_client, &self.cn_sk);
    self.cn_peer_nonce = u64::from_be_bytes(initiate[105..113].try_into().unwrap());
    let plain = match open_afternm(&initiate[113..], &nonce(b"CurveZMQINITIATE", &initiate[105..113]), &precom) {
      Some(p) if p.len() >= 128 => p,
      _ => return Err(self.fail("CURVE INITIATE could not be decrypted")),
    };
    let client_key: Key = plain[..32].try_into().unwrap();
    let vouch = match open(&plain[48..128], &nonce(b"VOUCH---", &plain[32..48]), &client_key, &self.cn_sk) {
      Some(v) => v,
      None => return Err(self.fail("CURVE INITIATE vouch could not be decrypted")),
    };
    if vouch[..32] != cn_client {
      return Err(self.fail("CURVE INITIATE vouch is not valid"));
    }
    self.take_metadata(&plain[128..])?;
    self.precom = Some(precom);
    self.phase = CurveHandshakePhase::ServerSendReady;
    Ok(())
  }

  /// READY: prefix(6) nonce(8) Box[metadata](S'->C').
  pub(crate) fn build_server_ready(&mut self) -> Result<Vec<u8>, ZmqError> {
    let precom = self.precom.ok_or_else(|| self.fail("CURVE READY before INITIATE"))?;
    let metadata = self.metadata_bytes();
    let short_nonce = self.next_nonce();
    let boxed = seal_afternm(&metadata, &nonce(b"CurveZMQREADY---", &short_nonce), &precom);
    let mut ready = Vec::with_capacity(14 + boxed.len());
    ready.extend_from_slice(b"\x05READY");
    ready.extend_from_slice(&short_nonce);
    ready.extend_from_slice(&boxed);
    self.phase = CurveHandshakePhase::Complete;
    Ok(ready)
  }

  pub(crate) fn process_server_ready(&mut self, ready: &[u8]) -> Result<(), ZmqError> {
    if !ready.starts_with(b"\x05READY") {
      return Err(self.fail("Expected CURVE READY"));
    }
    if ready.len() < Self::READY_MIN_LEN {
      return Err(self.fail("CURVE READY is too short"));
    }
    let precom = self.precom.ok_or_else(|| self.fail("CURVE READY before WELCOME"))?;
    self.cn_peer_nonce = u64::from_be_bytes(ready[6..14].try_into().unwrap());
    let plain = match open_afternm(&ready[14..], &nonce(b"CurveZMQREADY---", &ready[6..14]), &precom) {
      Some(p) => p,
      None => return Err(self.fail("CURVE READY could not be decrypted")),
    };
    self.take_metadata(&plain)?;
    self.phase = CurveHandshakePhase::Complete;
    Ok(())
  }

  /// ERROR: prefix(6) reason-len(1) reason. libzmq puts the ZAP status code there.
  pub(crate) fn process_error(&mut self, error: &[u8]) -> ZmqError {
    self.phase = CurveHandshakePhase::Error;
    let reason = match error.get(6) {
      Some(&len) if error.len() >= 7 + len as usize => {
        String::from_utf8_lossy(&error[7..7 + len as usize]).into_owned()
      }
      _ => String::new(),
    };
    ZmqError::AuthenticationFailure(format!("Received ERROR from server: {}", reason))
  }

  /// The data-phase key and nonce state once the handshake is complete.
  pub(crate) fn into_session(mut self) -> Result<(Key, u64, u64), ZmqError> {
    if self.phase != CurveHandshakePhase::Complete {
      return Err(ZmqError::InvalidState("Handshake not complete"));
    }
    let precom = self.precom.take().ok_or(ZmqError::InvalidState("Handshake has no session key"))?;
    Ok((precom, self.cn_nonce, self.cn_peer_nonce))
  }
}

#[cfg(test)]
pub(crate) mod tests {
  use super::*;

  pub(crate) fn handshake_pair() -> (CurveHandshake, CurveHandshake) {
    let (server_pk, server_sk) = crypto_box_keypair();
    let (_, client_sk) = crypto_box_keypair();
    let server_config = ZmtpEngineConfig {
      curve_local_secret_key: Some(server_sk),
      ..Default::default()
    };
    let client_config = ZmtpEngineConfig {
      curve_local_secret_key: Some(client_sk),
      curve_remote_public_key: Some(server_pk),
      ..Default::default()
    };
    let meta = |t: &[u8]| HashMap::from([("Socket-Type".to_string(), t.to_vec())]);
    (
      CurveHandshake::new(false, &client_config, meta(b"REQ")).unwrap(),
      CurveHandshake::new(true, &server_config, meta(b"REP")).unwrap(),
    )
  }

  pub(crate) fn completed_pair() -> (CurveHandshake, CurveHandshake) {
    let (mut client, mut server) = handshake_pair();
    server.process_client_hello(&client.build_client_hello().unwrap()).unwrap();
    client.process_server_welcome(&server.build_server_welcome().unwrap()).unwrap();
    server.process_client_initiate(&client.build_client_initiate().unwrap()).unwrap();
    client.process_server_ready(&server.build_server_ready().unwrap()).unwrap();
    (client, server)
  }

  #[test]
  fn commands_have_libzmq_sizes() {
    let (mut client, mut server) = handshake_pair();
    let hello = client.build_client_hello().unwrap();
    assert_eq!(hello.len(), 200);
    assert_eq!(&hello[..8], b"\x05HELLO\x01\x00");
    assert!(hello[8..80].iter().all(|&b| b == 0));
    assert_eq!(&hello[112..120], &1u64.to_be_bytes());
    server.process_client_hello(&hello).unwrap();
    let welcome = server.build_server_welcome().unwrap();
    assert_eq!(welcome.len(), 168);
    client.process_server_welcome(&welcome).unwrap();
    // Metadata is one property: 1 + "Socket-Type" + 4 + 3 = 19 bytes.
    let initiate = client.build_client_initiate().unwrap();
    assert_eq!(initiate.len(), 113 + 16 + 128 + 19);
    assert_eq!(&initiate[105..113], &2u64.to_be_bytes());
    server.process_client_initiate(&initiate).unwrap();
    let ready = server.build_server_ready().unwrap();
    assert_eq!(ready.len(), 14 + 16 + 19);
    assert_eq!(&ready[6..14], &1u64.to_be_bytes());
    client.process_server_ready(&ready).unwrap();
    assert_eq!(client.peer_metadata.as_ref().unwrap()["Socket-Type"], b"REP");
    assert_eq!(server.peer_metadata.as_ref().unwrap()["Socket-Type"], b"REQ");
  }

  #[test]
  fn both_sides_derive_the_same_session_key() {
    let (client, server) = completed_pair();
    let (ck, c_nonce, c_peer) = client.into_session().unwrap();
    let (sk, s_nonce, s_peer) = server.into_session().unwrap();
    assert_eq!(ck, sk);
    assert_eq!((c_nonce, c_peer), (3, 1));
    assert_eq!((s_nonce, s_peer), (2, 2));
  }

  #[test]
  fn wrong_server_key_fails_at_welcome_with_fixed_message() {
    let (mut client, _) = handshake_pair();
    let (_, mut other_server) = handshake_pair();
    other_server.process_client_hello(&client.build_client_hello().unwrap()).unwrap_err();
    let (_, mut server) = handshake_pair();
    server.cn_peer = Some(client.cn_pk);
    let welcome = server.build_server_welcome().unwrap();
    match client.process_server_welcome(&welcome) {
      Err(ZmqError::SecurityError(m)) => assert_eq!(m, "CURVE WELCOME could not be decrypted (wrong server public key?)"),
      other => panic!("got {:?}", other),
    }
  }

  #[test]
  fn tampered_initiate_fails_with_fixed_message() {
    let (mut client, mut server) = handshake_pair();
    server.process_client_hello(&client.build_client_hello().unwrap()).unwrap();
    client.process_server_welcome(&server.build_server_welcome().unwrap()).unwrap();
    let mut initiate = client.build_client_initiate().unwrap();
    initiate[120] ^= 0x01;
    match server.process_client_initiate(&initiate) {
      Err(ZmqError::SecurityError(m)) => assert_eq!(m, "CURVE INITIATE could not be decrypted"),
      other => panic!("got {:?}", other),
    }
  }

  #[test]
  fn replayed_cookie_from_another_welcome_is_rejected() {
    let (mut client, mut server) = handshake_pair();
    server.process_client_hello(&client.build_client_hello().unwrap()).unwrap();
    client.process_server_welcome(&server.build_server_welcome().unwrap()).unwrap();
    server.build_server_welcome().unwrap();
    match server.process_client_initiate(&client.build_client_initiate().unwrap()) {
      Err(ZmqError::SecurityError(m)) => assert_eq!(m, "CURVE INITIATE cookie could not be decrypted"),
      other => panic!("got {:?}", other),
    }
  }

  #[test]
  fn error_from_server_carries_zap_status() {
    let (mut client, _) = handshake_pair();
    match client.process_error(b"\x05ERROR\x03400") {
      ZmqError::AuthenticationFailure(m) => assert_eq!(m, "Received ERROR from server: 400"),
      other => panic!("got {:?}", other),
    }
  }
}
