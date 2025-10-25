use crate::error::ZmqError;
use crate::security::IDataCipher;
use crate::security::curve::cipher::CurveDataCipher;
use crate::socket::options::ZmtpEngineConfig;

use std::collections::HashMap;

use dryoc::classic::crypto_box::{
  Mac, Nonce, crypto_box_beforenm, crypto_box_detached_afternm, crypto_box_keypair,
  crypto_box_open_detached_afternm,
};
use dryoc::keypair::{PublicKey, SecretKey, StackKeyPair as Keypair};
use dryoc::types::{ByteArray, Bytes, MutByteArray, MutBytes, NewByteArray, StackByteArray};
use zeroize::Zeroize;

/// Represents the internal state of the CurveZMQ handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurveHandshakePhase {
  // Client states
  ClientStart,
  ClientSentHello,              // Waiting for WELCOME
  ClientReadyToProduceInitiate, // Received WELCOME, ready to send INITIATE

  // Server states
  ServerStart,                 // Waiting for HELLO
  ServerReadyToProduceWelcome, // Received HELLO, ready to send WELCOME
  ServerAwaitingInitiate,      // Sent WELCOME, waiting for INITIATE

  // Terminal states
  Complete,
  Error,
}

/// A state machine that encapsulates the logic and state for a ZMTP CurveZMQ handshake.
#[derive(Debug)]
pub(crate) struct CurveHandshake {
  pub(crate) phase: CurveHandshakePhase,
  pub(crate) is_server: bool,

  // Cryptographic materials
  pub(crate) local_static_keypair: Keypair,
  pub(crate) remote_static_public_key: Option<PublicKey>, // Client knows, server learns

  pub(crate) local_ephemeral_keypair: Keypair,
  pub(crate) remote_ephemeral_public_key: Option<PublicKey>, // Learned during handshake

  // The result of `crypto_box::beforenm`. This is the key material used for encryption.
  pub(crate) precomputed_key: Option<[u8; 32]>,

  // Nonces for encrypting/decrypting handshake commands (WELCOME, INITIATE).
  send_nonce: u64,
  recv_nonce: u64,
}

impl CurveHandshake {
  const HELLO_PREFIX: &'static [u8] = b"\x05HELLO";
  const WELCOME_PREFIX: &'static [u8] = b"\x07WELCOME";
  const INITIATE_PREFIX: &'static [u8] = b"\x08INITIATE";
  const COOKIE_NONCE_PREFIX: &'static [u8] = b"COOKIE--";

  /// Creates a new Curve handshake state machine.
  pub(crate) fn new(is_server: bool, config: &ZmtpEngineConfig) -> Result<Self, ZmqError> {
    let local_static_keypair = Keypair::from_secret_key(
      config
        .curve_local_secret_key
        .ok_or(ZmqError::InvalidCurveKey)?
        .into(),
    );

    let remote_static_public_key = if is_server {
      None
    } else {
      Some(
        config
          .curve_remote_public_key
          .ok_or(ZmqError::InvalidCurveKey)?
          .into(),
      )
    };

    let (epk, esk) = crypto_box_keypair();

    Ok(Self {
      phase: if is_server {
        CurveHandshakePhase::ServerStart
      } else {
        CurveHandshakePhase::ClientStart
      },
      is_server,
      local_static_keypair,
      remote_static_public_key,
      local_ephemeral_keypair: Keypair {
        public_key: epk.into(),
        secret_key: esk.into(),
      },
      remote_ephemeral_public_key: None,
      precomputed_key: None,
      send_nonce: 1,
      recv_nonce: 1,
    })
  }

  /// Helper to log key prefixes for debugging.
  fn key_prefix(key: &[u8]) -> String {
    hex::encode(&key[..4])
  }
  
  /// Consumes the completed handshake state to produce the data-phase cipher.
  pub(crate) fn into_data_cipher(self) -> Result<Box<dyn IDataCipher>, ZmqError> {
    if self.phase != CurveHandshakePhase::Complete {
      return Err(ZmqError::InvalidState("Handshake not complete"));
    }
    let secret = self
      .precomputed_key
      .ok_or(ZmqError::InvalidState("Shared secret not derived"))?;
    tracing::debug!(precomputed_key_prefix = %Self::key_prefix(&secret), "Finalizing handshake, creating data cipher.");
    Ok(Box::new(CurveDataCipher::new(secret)))
  }

  /// Consumes the completed handshake state to produce the directional session keys.
  pub(crate) fn into_session_keys(self) -> Result<([u8; 32], [u8; 32]), ZmqError> {
    if self.phase != CurveHandshakePhase::Complete {
      return Err(ZmqError::InvalidState("Handshake not complete"));
    }

    let client_pk = self
      .remote_static_public_key
      .as_ref()
      .ok_or_else(|| ZmqError::InvalidState("Handshake complete but client PK missing"))?;
    let server_pk = self.local_static_keypair.public_key.as_array();

    let (mut rx, mut tx) = ([0u8; 32], [0u8; 32]);

    if self.is_server {
      // We are the server, so we calculate the server's session keys.
      // Our RX key is for messages from the client.
      // Our TX key is for messages to the client.
      dryoc::classic::crypto_kx::crypto_kx_server_session_keys(
        &mut rx,
        &mut tx,
        server_pk,
        self.local_static_keypair.secret_key.as_array(),
        client_pk.as_array(),
      )?;
    } else {
      // We are the client.
      dryoc::classic::crypto_kx::crypto_kx_client_session_keys(
        &mut rx,
        &mut tx,
        self.local_static_keypair.public_key.as_array(),
        self.local_static_keypair.secret_key.as_array(),
        server_pk,
      )?;
    }

    Ok((rx, tx))
  }
  //
  // All build_* and process_* methods from here down are corrected.
  //

  pub(crate) fn build_client_hello(&self) -> Result<Vec<u8>, ZmqError> {
    tracing::debug!(
        side = "client",
        state = "build_hello",
        client_eph_pk = %Self::key_prefix(self.local_ephemeral_keypair.public_key.as_slice()),
        "Building HELLO command"
    );
    let mut command = Vec::with_capacity(256);
    command.extend_from_slice(Self::HELLO_PREFIX);
    let mut metadata = HashMap::new();
    metadata.insert(
      "Public-Key-Client".to_string(),
      self.local_ephemeral_keypair.public_key.as_slice().to_vec(),
    );
    command.extend(encode_metadata(&metadata));
    command.resize(192 + 6, 0);
    Ok(command)
  }

  pub(crate) fn process_client_hello(&mut self, token: &[u8]) -> Result<(), ZmqError> {
    if !token.starts_with(Self::HELLO_PREFIX) {
      return Err(ZmqError::ProtocolViolation("Expected HELLO command".into()));
    }
    let metadata = decode_metadata(&token[Self::HELLO_PREFIX.len()..])?;
    let client_ephemeral_pk_slice =
      metadata
        .get("Public-Key-Client")
        .ok_or(ZmqError::ProtocolViolation(
          "HELLO missing Public-Key-Client".into(),
        ))?;
    self.remote_ephemeral_public_key =
      Some(PublicKey::try_from(client_ephemeral_pk_slice.as_slice())?);
    tracing::debug!(
        side = "server",
        state = "process_hello",
        client_eph_pk = %Self::key_prefix(client_ephemeral_pk_slice),
        "Processed HELLO command"
    );
    Ok(())
  }

  pub(crate) fn build_server_welcome(&mut self) -> Result<Vec<u8>, ZmqError> {
    let client_eph_pk = self.remote_ephemeral_public_key.as_ref().unwrap();
    let k = crypto_box_beforenm(
      client_eph_pk.as_array(),
      self.local_static_keypair.secret_key.as_array(),
    );
    tracing::debug!(side = "server", state = "build_welcome", cookie_key_prefix = %Self::key_prefix(&k), "Pre-computed key for WELCOME cookie.");

    let mut cookie_ciphertext = vec![0u8; 32];
    let mut cookie_mac = Mac::new_byte_array();
    let mut nonce = Nonce::new_byte_array();
    nonce.as_mut_slice()[..8].copy_from_slice(Self::COOKIE_NONCE_PREFIX);

    // **FIXED**: Using crypto_box_detached_afternm, which is in your imports
    crypto_box_detached_afternm(
      &mut cookie_ciphertext,
      cookie_mac.as_mut_array(),
      self.local_ephemeral_keypair.public_key.as_slice(),
      nonce.as_array(),
      &k,
    );
    let cookie_full = [cookie_mac.as_slice(), cookie_ciphertext.as_slice()].concat();

    let mut command = Vec::with_capacity(256);
    command.extend_from_slice(Self::WELCOME_PREFIX);
    let mut metadata = HashMap::new();
    metadata.insert("Cookie".to_string(), cookie_full);
    command.extend(encode_metadata(&metadata));
    command.resize(128 + 8, 0);
    Ok(command)
  }

  pub(crate) fn process_server_welcome(&mut self, token: &[u8]) -> Result<(), ZmqError> {
    if !token.starts_with(Self::WELCOME_PREFIX) {
      return Err(ZmqError::ProtocolViolation(
        "Expected WELCOME command".into(),
      ));
    }
    let metadata = decode_metadata(&token[Self::WELCOME_PREFIX.len()..])?;
    let cookie = metadata
      .get("Cookie")
      .ok_or(ZmqError::ProtocolViolation("WELCOME missing Cookie".into()))?;

    // Key for cookie is derived from Server's STATIC Public Key and Client's EPHEMERAL Secret Key
    let server_static_pk = self.remote_static_public_key.as_ref().unwrap();
    let k = crypto_box_beforenm(
      server_static_pk.as_array(),
      self.local_ephemeral_keypair.secret_key.as_array(),
    );

    tracing::debug!(
        side = "client",
        state = "process_welcome",
        cookie_key_prefix = %Self::key_prefix(&k),
        "Pre-computed key for opening WELCOME cookie."
    );

    let mut server_ephemeral_pk_bytes = vec![0u8; 32];
    let mut nonce = Nonce::new_byte_array();
    nonce.as_mut_slice()[..8].copy_from_slice(Self::COOKIE_NONCE_PREFIX);
    let mac = Mac::try_from(&cookie[..16])?;
    let cookie_ciphertext = &cookie[16..];

    crypto_box_open_detached_afternm(
      &mut server_ephemeral_pk_bytes,
      mac.as_array(),
      cookie_ciphertext,
      nonce.as_array(),
      &k,
    )?;

    self.remote_ephemeral_public_key =
      Some(PublicKey::try_from(server_ephemeral_pk_bytes.as_slice())?);

    // Final key is derived from Server's EPHEMERAL Public Key and Client's STATIC Secret Key
    let server_eph_pk = self.remote_ephemeral_public_key.as_ref().unwrap();
    let final_k = crypto_box_beforenm(
      server_eph_pk.as_array(),
      self.local_static_keypair.secret_key.as_array(),
    );
    self.precomputed_key = Some(final_k);

    tracing::debug!(
        side = "client",
        state = "process_welcome",
        final_key_prefix = %Self::key_prefix(&final_k),
        "Derived FINAL pre-computed key."
    );

    Ok(())
  }

  pub(crate) fn build_client_initiate(&mut self) -> Result<Vec<u8>, ZmqError> {
    let server_static_pk = self.remote_static_public_key.as_ref().unwrap();
    let mut initiate_key = crypto_box_beforenm(
      server_static_pk.as_array(),
      self.local_ephemeral_keypair.secret_key.as_array(),
    );

    let nonce = self.next_send_nonce();
    tracing::debug!(side = "client", state = "build_initiate", initiate_key_prefix = %Self::key_prefix(&initiate_key), "Building INITIATE command with temporary key.");

    let mut metadata = HashMap::new();
    metadata.insert(
      "Public-Key-Client".to_string(),
      self.local_static_keypair.public_key.as_slice().to_vec(),
    );
    let metadata_bytes = encode_metadata(&metadata);

    let mut mac = Mac::new_byte_array();
    let mut encrypted_metadata = vec![0u8; metadata_bytes.len()];

    // PROPOSED FIX: Use the correct temporary 'initiate_key' for encryption.
    crypto_box_detached_afternm(
      &mut encrypted_metadata,
      mac.as_mut_array(),
      &metadata_bytes,
      nonce.as_array(),
      &initiate_key,
    );

    // It's good practice to zeroize the temporary key after use.
    initiate_key.zeroize();

    let ciphertext_with_mac = [mac.as_slice(), encrypted_metadata.as_slice()].concat();

    let mut command = Vec::with_capacity(256);
    command.extend_from_slice(Self::INITIATE_PREFIX);
    let mut final_meta = HashMap::new();
    final_meta.insert("Ciphertext".to_string(), ciphertext_with_mac);
    command.extend(encode_metadata(&final_meta));
    command.resize(128 + 9, 0);
    Ok(command)
  }

  pub(crate) fn process_client_initiate(&mut self, token: &[u8]) -> Result<(), ZmqError> {
    if !token.starts_with(Self::INITIATE_PREFIX) {
      return Err(ZmqError::ProtocolViolation(
        "Expected INITIATE command".into(),
      ));
    }
    let metadata = decode_metadata(&token[Self::INITIATE_PREFIX.len()..])?;
    let ciphertext_with_mac = metadata
      .get("Ciphertext")
      .ok_or(ZmqError::ProtocolViolation(
        "INITIATE missing Ciphertext".into(),
      ))?;

    // The key for INITIATE is derived from Client's EPHEMERAL Public key and Server's STATIC secret key
    let client_eph_pk = self.remote_ephemeral_public_key.as_ref().unwrap();
    let initial_k = crypto_box_beforenm(
      client_eph_pk.as_array(),
      self.local_static_keypair.secret_key.as_array(),
    );

    tracing::debug!(
        side = "server",
        state = "process_initiate",
        initial_key_prefix = %Self::key_prefix(&initial_k),
        "Opening INITIATE with initial key."
    );

    let mac = Mac::try_from(&ciphertext_with_mac[..16])?;
    let ciphertext = &ciphertext_with_mac[16..];
    let nonce = self.next_recv_nonce();

    let mut decrypted_metadata_bytes = vec![0u8; ciphertext.len()];
    crypto_box_open_detached_afternm(
      &mut decrypted_metadata_bytes,
      mac.as_array(),
      ciphertext,
      nonce.as_array(),
      &initial_k,
    )?;

    let decrypted_metadata = decode_metadata(&decrypted_metadata_bytes)?;
    let client_static_pk_slice =
      decrypted_metadata
        .get("Public-Key-Client")
        .ok_or(ZmqError::ProtocolViolation(
          "INITIATE missing Public-Key-Client".into(),
        ))?;

    self.remote_static_public_key = Some(PublicKey::try_from(client_static_pk_slice.as_slice())?);

    // NOW that we have the client's static public key, we can derive the FINAL key
    let client_static_pk = self.remote_static_public_key.as_ref().unwrap();
    let final_k = crypto_box_beforenm(
      client_static_pk.as_array(),
      self.local_ephemeral_keypair.secret_key.as_array(),
    );
    self.precomputed_key = Some(final_k);

    tracing::debug!(
        side = "server",
        state = "process_initiate",
        final_key_prefix = %Self::key_prefix(&final_k),
        "Derived FINAL pre-computed key after opening INITIATE."
    );

    Ok(())
  }

  // --- Nonce helpers ---
  fn next_send_nonce(&mut self) -> StackByteArray<24> {
    let mut nonce = StackByteArray::<24>::new();
    nonce.as_mut_slice()[16..].copy_from_slice(&self.send_nonce.to_le_bytes());
    self.send_nonce += 1;
    nonce
  }

  fn next_recv_nonce(&mut self) -> StackByteArray<24> {
    let mut nonce = StackByteArray::<24>::new();
    nonce.as_mut_slice()[16..].copy_from_slice(&self.recv_nonce.to_le_bytes());
    self.recv_nonce += 1;
    nonce
  }
}

// --- ZMTP Metadata Helpers ---

fn encode_metadata(metadata: &HashMap<String, Vec<u8>>) -> Vec<u8> {
  let mut bytes = Vec::new();
  for (key, value) in metadata {
    bytes.push(key.len() as u8);
    bytes.extend_from_slice(key.as_bytes());
    bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
    bytes.extend_from_slice(value);
  }
  bytes
}

fn decode_metadata(data: &[u8]) -> Result<HashMap<String, Vec<u8>>, ZmqError> {
  let mut metadata = HashMap::new();
  let mut current = 0;
  while current < data.len() {
    if data[current] == 0 {
      break;
    } // Padding starts
    let key_len = data[current] as usize;
    current += 1;
    if current + key_len > data.len() {
      return Err(ZmqError::ProtocolViolation("Invalid metadata".into()));
    }
    let key = String::from_utf8(data[current..current + key_len].to_vec()).unwrap();
    current += key_len;

    if current + 4 > data.len() {
      return Err(ZmqError::ProtocolViolation("Invalid metadata".into()));
    }
    let value_len = u32::from_be_bytes(data[current..current + 4].try_into().unwrap()) as usize;
    current += 4;
    if current + value_len > data.len() {
      return Err(ZmqError::ProtocolViolation("Invalid metadata".into()));
    }
    let value = data[current..current + value_len].to_vec();
    current += value_len;

    metadata.insert(key, value);
  }
  Ok(metadata)
}
