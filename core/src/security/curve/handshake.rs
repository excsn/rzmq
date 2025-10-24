use crate::error::ZmqError;
use dryoc::keypair::{PublicKey, SecretKey, StackKeyPair as Keypair};

/// Represents the internal state of the CurveZMQ handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CurveHandshakePhase {
  // Client states
  ClientStart,
  ClientSentHello, // Waiting for WELCOME
  
  // Server states
  ServerStart,     // Waiting for HELLO
  ServerSentWelcome, // Waiting for INITIATE

  // Terminal states
  Complete,
  // We cannot embed ZmqError directly if we want to derive Clone, Copy.
  // We will store the error separately in the CurveMechanism.
  Error,
}

/// A state machine that encapsulates the logic and state for a ZMTP CurveZMQ handshake.
#[derive(Debug)]
pub(crate) struct CurveHandshake {
  pub(crate) phase: CurveHandshakePhase,
  pub(crate) is_server: bool,
  
  // Cryptographic materials
  pub(crate) local_static_keypair: Keypair,
  pub(crate) remote_static_public_key: Option<PublicKey>, // Client knows this, server learns it
  
  pub(crate) local_ephemeral_keypair: Keypair,
  pub(crate) remote_ephemeral_public_key: Option<PublicKey>, // Learned during handshake

  // The result of `crypto_box::beforenm`. This is the key material used for encryption.
  pub(crate) precomputed_key: Option<[u8; 32]>,

  // Nonces for encrypting/decrypting handshake commands (WELCOME, INITIATE).
  // These are separate from the data-phase nonces.
  // We'll use a simple counter for now.
  pub(crate) send_nonce: u64,
  pub(crate) recv_nonce: u64,
}