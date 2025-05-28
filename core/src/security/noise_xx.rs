#![cfg(feature = "noise_xx")]

use crate::error::ZmqError;
use crate::security::{Mechanism, MechanismStatus, Metadata};
use bytes::{Buf, Bytes, BytesMut};
use snow::error::{Prerequisite, StateProblem};
// Use your existing types
use snow::params::NoiseParams;
use snow::{Error as SnowError, TransportState};

use super::cipher::IDataCipher;

// Helper to convert SnowError to ZmqError
impl From<SnowError> for ZmqError {
  fn from(e: SnowError) -> Self {
    tracing::warn!("Snow protocol error occurred: {}", e); // Log the Display form of SnowError
    match e {
      SnowError::Pattern(problem) => {
        // Example: Map specific pattern problems if needed, or use a generic message
        ZmqError::SecurityError(format!("Noise pattern configuration error: {:?}", problem))
      }
      SnowError::Init(stage) => ZmqError::SecurityError(format!("Noise initialization error at stage: {:?}", stage)),
      SnowError::Prereq(prereq) => match prereq {
        Prerequisite::LocalPrivateKey => {
          ZmqError::SecurityError("Noise prerequisite error: Local private key missing or invalid.".into())
        }
        Prerequisite::RemotePublicKey => ZmqError::SecurityError(
          "Noise prerequisite error: Remote public key missing or invalid for current operation.".into(),
        ),
      },
      SnowError::State(problem) => match problem {
        StateProblem::MissingKeyMaterial => {
          ZmqError::SecurityError("Noise state error: Missing required key material for operation.".into())
        }
        StateProblem::HandshakeNotFinished => {
          ZmqError::InvalidState("Noise state error: Handshake is not yet finished for requested operation.".into())
        }
        StateProblem::HandshakeAlreadyFinished => {
          ZmqError::InvalidState("Noise state error: Handshake is already finished.".into())
        }
        // Map other StateProblem variants as needed to more specific ZmqErrors or generic SecurityError
        _ => ZmqError::SecurityError(format!("Noise state machine error: {:?}", problem)),
      },
      SnowError::Input => {
        // This often relates to message sizes or invalid input to read/write_message
        ZmqError::SecurityError("Noise input error: Invalid message format or size for current state.".into())
      }
      SnowError::Dh => ZmqError::SecurityError("Noise Diffie-Hellman operation failed.".into()),
      SnowError::Decrypt => {
        // This is critical, means message authentication (tag verification) or decryption failed.
        ZmqError::SecurityError("Noise decrypt/authentication failed (e.g., bad MAC or ciphertext).".into())
      }
      #[cfg(feature = "hfs")] // If you enable hfs feature in snow
      SnowError::Kem => ZmqError::SecurityError("Noise Key Encapsulation Mechanism (KEM) failed.".into()),
      _ => {
        // This arm catches any future variants added to snow::Error
        // or any existing variants not explicitly matched above (if any).
        tracing::error!("Unhandled snow::Error variant: {}", e);
        ZmqError::SecurityError(format!("Unhandled or new Noise protocol error: {}", e))
      }
    }
  }
}

// Custom Debug for NoiseXxMechanism because snow::HandshakeState is not Debug
pub struct NoiseXxMechanism {
  is_server_role: bool,
  state: Option<snow::HandshakeState>, // Store the HandshakeState directly

  // This will be populated once the handshake is complete and state transitions.
  // The NoiseStream will use this TransportState.
  pub(crate) transport_state: Option<TransportState>,

  // Only used by client during init and verification
  configured_remote_static_pk_bytes: Option<[u8; 32]>,
  // Stores the peer's static public key once learned and verified from the handshake
  verified_peer_static_pk: Option<Vec<u8>>,

  current_status: MechanismStatus,
  error_reason_str: Option<String>,
  // Buffer for outgoing handshake messages, if produce_token is called before process_token has a chance to consume it
  pending_outgoing_handshake_msg: Option<Vec<u8>>,
}

// Manual Debug implementation
impl std::fmt::Debug for NoiseXxMechanism {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("NoiseXxMechanism")
      .field("is_server_role", &self.is_server_role)
      .field(
        "state_is_handshake_finished",
        &self.state.as_ref().map(|s| s.is_handshake_finished()).unwrap_or(false),
      )
      .field("transport_state_is_some", &self.transport_state.is_some())
      .field(
        "verified_peer_static_pk_len",
        &self.verified_peer_static_pk.as_ref().map(|v| v.len()),
      )
      .field("current_status", &self.current_status)
      .field("error_reason_str", &self.error_reason_str)
      .field(
        "pending_outgoing_handshake_msg_len",
        &self.pending_outgoing_handshake_msg.as_ref().map(|v| v.len()),
      )
      .finish()
  }
}

impl NoiseXxMechanism {
  pub const NAME: &'static str = "NOISE_XX";
  const NOISE_PARAMS_STR: &'static str = "Noise_XX_25519_ChaChaPoly_BLAKE2s"; // Standard Noise pattern

  pub fn new(
    is_server: bool,
    local_static_sk_bytes: &[u8; 32], // Expects the snow::Keypair struct directly
    // For XX client, this is the server's static public key it expects.
    // For XX server, this is None initially.
    initial_remote_static_pk_bytes: Option<[u8; 32]>,
  ) -> Result<Self, ZmqError> {
    let params: NoiseParams = Self::NOISE_PARAMS_STR.parse().map_err(|e| {
      ZmqError::Internal(format!(
        "Failed to parse Noise params string '{}': {:?}",
        Self::NOISE_PARAMS_STR,
        e
      ))
    })?;

    let mut builder = snow::Builder::new(params);
    builder = builder.local_private_key(local_static_sk_bytes);

    if !is_server {
      // Client (initiator)
      if let Some(ref pk_bytes) = initial_remote_static_pk_bytes {
        builder = builder.remote_public_key(pk_bytes);
      } else {
        // For XX initiator, remote static key is required for authentication.
        return Err(ZmqError::SecurityError(
          "NOISE_XX Client: Server's static public key is required for configuration.".into(),
        ));
      }
    }
    // For XX server (responder), remote_public_key is not set on the builder initially.
    // It's received from the client and verified during the handshake.

    let noise_handshake_state = if is_server {
      builder.build_responder()?
    } else {
      builder.build_initiator()?
    };

    Ok(Self {
      is_server_role: is_server,
      state: Some(noise_handshake_state),
      transport_state: None,
      configured_remote_static_pk_bytes: initial_remote_static_pk_bytes,
      verified_peer_static_pk: None,
      current_status: MechanismStatus::Initializing, // Will move to Handshaking
      error_reason_str: None,
      pending_outgoing_handshake_msg: None,
    })
  }

  /// Internal helper called when handshake is finished to transition to transport mode
  /// and perform final verifications.
  fn transition_to_final_state(&mut self) -> Result<(), SnowError> {
    let current_handshake_state = self
      .state
      .as_ref()
      .ok_or(SnowError::State(StateProblem::HandshakeAlreadyFinished))?; // Or some other state error

    if !current_handshake_state.is_handshake_finished() {
      tracing::warn!("NOISE_XX: transition_to_final_state called but handshake not finished.");
      return Err(SnowError::State(StateProblem::HandshakeNotFinished));
    }

    let handshake_derived_peer_pk = match current_handshake_state.get_remote_static() {
      Some(pk_bytes_slice) => pk_bytes_slice.to_vec(),
      None => {
        let err_msg =
          "NOISE_XX handshake purportedly finished but no remote static key material was obtained.".to_string();
        tracing::error!("{}", err_msg);
        self.current_status = MechanismStatus::Error;
        self.error_reason_str = Some(err_msg);
        return Err(SnowError::State(StateProblem::MissingKeyMaterial));
      }
    };

    // Extract the peer's static public key that was authenticated during the handshake
    self.verified_peer_static_pk = Some(handshake_derived_peer_pk.clone());

    if !self.is_server_role {
      // Client
      if let Some(expected_server_pk_array) = self.configured_remote_static_pk_bytes {
        if handshake_derived_peer_pk.as_slice() != expected_server_pk_array.as_slice() {
          tracing::error!(
            mechanism = Self::NAME,
            role = "Client",
            "Server public key validation FAILED. Expected PK: {:?}, Actual PK from handshake: {:?}",
            expected_server_pk_array.as_slice(),
            handshake_derived_peer_pk.as_slice()
          );
          self.current_status = MechanismStatus::Error;
          self.error_reason_str = Some("NOISE_XX: Server public key mismatch. Connection rejected.".into());
          return Err(SnowError::Decrypt);
        }
        tracing::debug!(
          mechanism = Self::NAME,
          role = "Client",
          "Server static public key successfully verified."
        );
      } else {
        let err_msg =
          "NOISE_XX Client: Configuration error - missing expected remote server public key for verification."
            .to_string();
        tracing::error!("{}", err_msg);
        self.current_status = MechanismStatus::Error;
        self.error_reason_str = Some(err_msg);
        return Err(SnowError::Prereq(Prerequisite::RemotePublicKey));
      }
    } else {
      // Server
      tracing::debug!(
        mechanism = Self::NAME,
        role = "Server",
        "Learned and cryptographically verified client's static public key: {:?}",
        handshake_derived_peer_pk.as_slice()
      );
    }

    // Perform role-specific verifications or actions:
    if !self.is_server_role {
      // We are the Client
      if let Some(expected_server_pk_array) = self.configured_remote_static_pk_bytes {
        // Compare the public key derived from the handshake with the one configured for the server.
        if handshake_derived_peer_pk.as_slice() != expected_server_pk_array.as_slice() {
          // --- EXPANDED SECTION ---
          tracing::error!(
            mechanism = Self::NAME,
            role = "Client",
            "Server public key validation FAILED during NOISE_XX handshake.
                        Expected PK (configured via socket option): {:?}, 
                        Actual PK received from peer during handshake: {:?}",
            expected_server_pk_array.as_slice(), // Log as slice for consistent formatting
            handshake_derived_peer_pk.as_slice()
          );
          // --- END EXPANDED SECTION ---
          self.current_status = MechanismStatus::Error;
          self.error_reason_str = Some("NOISE_XX: Server public key mismatch. Connection rejected.".into());
          // SnowError::Decrypt is often used to signal an authentication/integrity failure.
          // SnowError::Identity from snow crate is not a variant.
          // Another option could be a custom error or mapping to a more specific ZmqError later.
          return Err(SnowError::Decrypt);
        }
        tracing::debug!(
          mechanism = Self::NAME,
          role = "Client",
          "Server static public key successfully verified against configuration."
        );
      } else {
        // This case should ideally be caught by NoiseXxMechanism::new() if a client doesn't have
        // the server's PK configured for the XX pattern. This is a defensive check.
        let err_msg = "NOISE_XX Client: Configuration error - missing expected remote server public key for verification post-handshake.".to_string();
        tracing::error!("{}", err_msg);
        self.current_status = MechanismStatus::Error;
        self.error_reason_str = Some(err_msg);
        return Err(SnowError::Prereq(Prerequisite::RemotePublicKey)); // Or State(MissingKeyMaterial)
      }
    } else {
      // We are the Server
      // The server has now learned and authenticated the client's static public key.
      // It's stored in self.verified_peer_static_pk.
      // If the server had a list of allowed client PKs (e.g., from a socket option
      // like NOISE_XX_ALLOWED_PEERS), this is the point to check against that list.
      // For now, we assume any client that successfully completes the XX handshake is allowed
      // (or ZAP would have handled pre-authentication).
      tracing::debug!(
        mechanism = Self::NAME,
        role = "Server",
        "Learned and cryptographically verified client's static public key: {:?}",
        handshake_derived_peer_pk.as_slice()
      );
      // Potentially store this learned client PK in self.configured_remote_static_pk_bytes if that field
      // is meant to hold the *actual* peer's key after handshake for servers.
      // self.configured_remote_static_pk_bytes = Some(handshake_derived_peer_pk.try_into().unwrap_or_else(|_| panic!("PK len mismatch")));
    }

    // If all checks passed, transition to transport mode and set status to Ready.
    // Must clone noise_state before calling into_transport_mode as it consumes HandshakeState.

    // Consume HandshakeState to get TransportState
    // Take the HandshakeState out of the Option to call into_transport_mode()
    let handshake_state_to_consume = self
      .state
      .take()
      .expect("HandshakeState was None unexpectedly during transition");
    match handshake_state_to_consume.into_transport_mode() {
      Ok(ts) => {
        self.transport_state = Some(ts); // Store the TransportState
        self.current_status = MechanismStatus::Ready;
        tracing::info!(
          mechanism = Self::NAME,
          "Handshake successful. Transitioned to transport mode. Status: Ready."
        );
        Ok(())
      }
      Err(e) => {
        let err_msg = format!(
          "NOISE_XX: Failed to transition to transport mode after handshake: {:?}",
          e
        );
        tracing::error!("{}", err_msg);
        self.current_status = MechanismStatus::Error;
        self.error_reason_str = Some(err_msg);
        // Put the HandshakeState back if transition failed, though its internal state might be bad
        // self.state = Some(handshake_state_to_consume); // This is tricky, snow might have invalidated it.
        // It's safer to consider it an error state.
        Err(e)
      }
    }
  }

  /// Consumes the NoiseXxMechanism (if handshake is complete and successful)
  /// and returns an IDataCipher for the data phase, along with the peer's identity.
  pub fn into_data_cipher_parts(mut self) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError> {
    if self.current_status != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "Noise handshake not complete, cannot create data cipher.".into(),
      ));
    }
    let ts = self
      .transport_state
      .take()
      .ok_or_else(|| ZmqError::Internal("NoiseXxMechanism is Ready but TransportState is missing.".into()))?;

    let peer_id = self.verified_peer_static_pk.clone(); // Clone before self is fully consumed by Box::new

    Ok((Box::new(NoiseDataCipher { transport_state: ts }), peer_id))
  }
}

#[async_trait::async_trait] // If any methods become async, though these are CPU-bound
impl Mechanism for NoiseXxMechanism {
  fn name(&self) -> &'static str {
    Self::NAME
  }

  fn produce_token(&mut self) -> Result<Option<Vec<u8>>, ZmqError> {
    if self.current_status == MechanismStatus::Error || self.current_status == MechanismStatus::Ready {
      return Ok(None);
    }
    if self.current_status == MechanismStatus::Initializing && self.is_server_role {
      self.current_status = MechanismStatus::Handshaking;
      return Ok(None);
    }
    self.current_status = MechanismStatus::Handshaking;

    if let Some(msg) = self.pending_outgoing_handshake_msg.take() {
      tracing::debug!("NOISE_XX produce_token: Sending pending msg (len {})", msg.len());
      return Ok(Some(msg));
    }

    let handshake_state = self
      .state
      .as_mut()
      .ok_or_else(|| ZmqError::InvalidState("Noise HandshakeState missing before produce_token".into()))?;

    if handshake_state.is_my_turn() {
      let mut msg_buf = vec![0u8; 1024];
      let len = handshake_state.write_message(&[], &mut msg_buf)?;
      msg_buf.truncate(len);
      tracing::debug!(
        "NOISE_XX Client (Initiator) produce_token: Generated first handshake message (len {})",
        len
      );
      Ok(Some(msg_buf))
    } else {
      tracing::trace!(
        "NOISE_XX produce_token: No message to produce now (is_server={}, is_initiator={}, sending_next_message={})",
        self.is_server_role,
        handshake_state.is_initiator(),
        handshake_state.is_my_turn()
      );
      Ok(None)
    }
  }

  fn process_token(&mut self, token: &[u8]) -> Result<(), ZmqError> {
    if self.current_status == MechanismStatus::Error || self.current_status == MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "NOISE_XX: Processing token in Error/Ready state".into(),
      ));
    }
    if self.current_status == MechanismStatus::Initializing {
      self.current_status = MechanismStatus::Handshaking;
    }
    tracing::debug!(
      "NOISE_XX process_token: Received handshake message (len {})",
      token.len()
    );

    let handshake_state = self
      .state
      .as_mut()
      .ok_or_else(|| ZmqError::InvalidState("Noise HandshakeState missing before process_token".into()))?;

    let mut read_payload_buf = vec![0u8; 1024];
    let _payload_len = handshake_state.read_message(token, &mut read_payload_buf)?;

    if handshake_state.is_handshake_finished() {
      tracing::debug!("NOISE_XX process_token: Handshake finished after processing token.");
      self.transition_to_final_state()?;
      self.pending_outgoing_handshake_msg = None;
    } else if handshake_state.is_my_turn() {
      let mut msg_buf = vec![0u8; 1024];
      let len = handshake_state.write_message(&[], &mut msg_buf)?;
      msg_buf.truncate(len);

      tracing::debug!(
        "NOISE_XX process_token: Generated next handshake message (len {}) for pending send.",
        len
      );
      self.pending_outgoing_handshake_msg = Some(msg_buf);
    } else {
      tracing::trace!("NOISE_XX process_token: Processed token, awaiting peer's next or handshake done by peer.");
      self.pending_outgoing_handshake_msg = None;
    }
    Ok(())
  }

  fn status(&self) -> MechanismStatus {
    self.current_status
  }

  fn peer_identity(&self) -> Option<Vec<u8>> {
    self.verified_peer_static_pk.clone()
  }

  fn metadata(&self) -> Option<Metadata> {
    None
  }

  // set_error should also invalidate the Option<HandshakeState>
  fn set_error(&mut self, reason: String) {
    tracing::error!("NOISE_XX Mechanism error set: {}", reason);
    self.current_status = MechanismStatus::Error;
    self.error_reason_str = Some(reason);
    self.state = None; // Invalidate/clear handshake state on error
    self.transport_state = None; // Clear transport state too
    self.pending_outgoing_handshake_msg = None;
  }

  fn error_reason(&self) -> Option<&str> {
    self.error_reason_str.as_deref()
  }

  fn zap_request_needed(&mut self) -> Option<Vec<Vec<u8>>> {
    None
  }
  fn process_zap_reply(&mut self, _reply_frames: &[Vec<u8>]) -> Result<(), ZmqError> {
    Ok(())
  }

  // Required by the trait, for downcasting.
  fn as_any(&self) -> &dyn std::any::Any {
    self
  }

  fn into_data_cipher_parts(mut self: Box<Self>) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError> {
    if self.current_status != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "Noise handshake not complete, cannot create data cipher.".into(),
      ));
    }
    let ts = self
      .transport_state
      .take()
      .ok_or_else(|| ZmqError::Internal("NoiseXxMechanism is Ready but TransportState is missing.".into()))?;

    // Peer identity should have been verified and stored in self.verified_peer_static_pk
    // by transition_to_final_state() before current_status became Ready.
    let peer_id = self.verified_peer_static_pk.clone();

    Ok((Box::new(NoiseDataCipher { transport_state: ts }), peer_id))
  }
}

// New struct specific for Noise data phase crypto
#[derive(Debug)] // TransportState isn't Debug, so manual or selective Debug
struct NoiseDataCipher {
  transport_state: TransportState,
}

impl IDataCipher for NoiseDataCipher {
  fn encrypt_zmtp_frame(&mut self, plaintext_zmtp_frame: Bytes) -> Result<Bytes, ZmqError> {
    const MAX_NOISE_PAYLOAD_LEN: usize = 65535 - 16; // Max data before tag
    const NOISE_TAG_LEN: usize = 16;
    const NOISE_LEN_PREFIX_LEN: usize = 2;

    if plaintext_zmtp_frame.len() > MAX_NOISE_PAYLOAD_LEN {
      tracing::error!(
        plaintext_len = plaintext_zmtp_frame.len(),
        max_len = MAX_NOISE_PAYLOAD_LEN,
        "ZMTP frame too large for a single Noise message. Chunking not yet implemented."
      );
      return Err(ZmqError::InvalidMessage(
        "ZMTP frame too large for a single Noise message.".into(),
      ));
    }

    // Buffer for [2-byte len][encrypted_zmtp_frame][16-byte tag]
    let required_buf_len = NOISE_LEN_PREFIX_LEN + plaintext_zmtp_frame.len() + NOISE_TAG_LEN;
    let mut noise_message_buf = vec![0u8; required_buf_len];

    // `write_message` encrypts `plaintext_zmtp_frame`, adds the tag, prepends the 2-byte length
    // of (encrypted_zmtp_frame + tag), and writes it all into `noise_message_buf`.
    // The buffer passed to `write_message` must be large enough for all of this.
    // `snow` expects the output buffer to be `payload.len() + TAGLEN + 2`
    let len_written_to_noise_buf = self.transport_state.write_message(
      &plaintext_zmtp_frame, // This is the "payload" for the Noise message
      &mut noise_message_buf,
    )?; // SnowError converted to ZmqError by From impl

    noise_message_buf.truncate(len_written_to_noise_buf);
    Ok(Bytes::from(noise_message_buf))
  }

  fn decrypt_wire_data_to_zmtp_frame(&mut self, encrypted_wire_data: &mut BytesMut) -> Result<Option<Bytes>, ZmqError> {
    const NOISE_LEN_PREFIX_LEN: usize = 2;
    const MIN_NOISE_MSG_PAYLOAD_WITH_TAG: usize = 16; // Smallest possible encrypted payload is just the tag

    if encrypted_wire_data.len() < NOISE_LEN_PREFIX_LEN {
      return Ok(None); // Not enough data for length prefix
    }

    // Peek at the 2-byte big-endian length prefix.
    let length_bytes: [u8; 2] = encrypted_wire_data[0..NOISE_LEN_PREFIX_LEN].try_into().unwrap();
    let encrypted_payload_len_with_tag = u16::from_be_bytes(length_bytes) as usize;

    if encrypted_payload_len_with_tag == 0 {
      // A zero-length noise message is valid (e.g. for rekeying or empty transport data)
      // but doesn't contain a ZMTP frame. We should consume it and signal no ZMTP frame.
      tracing::trace!("Decrypted zero-length noise message, consuming.");
      encrypted_wire_data.advance(NOISE_LEN_PREFIX_LEN); // Consume the length prefix
      return Ok(None); // No ZMTP frame produced from an empty Noise payload
    }

    let total_noise_message_len = NOISE_LEN_PREFIX_LEN + encrypted_payload_len_with_tag;

    if encrypted_wire_data.len() < total_noise_message_len {
      return Ok(None); // Not enough data for the full Noise message
    }

    // We have a full Noise message. Consume it.
    // `split_to` consumes from `encrypted_wire_data`.
    let noise_message_bytes_frozen = encrypted_wire_data.split_to(total_noise_message_len).freeze();
    let encrypted_payload_with_tag_slice = &noise_message_bytes_frozen[NOISE_LEN_PREFIX_LEN..];

    if encrypted_payload_len_with_tag < MIN_NOISE_MSG_PAYLOAD_WITH_TAG {
      tracing::error!(
        actual_len = encrypted_payload_len_with_tag,
        min_len = MIN_NOISE_MSG_PAYLOAD_WITH_TAG,
        "Noise message payload too short to contain a tag."
      );
      return Err(ZmqError::ProtocolViolation("Noise message too short for tag.".into()));
    }
    let max_plaintext_len = encrypted_payload_len_with_tag - MIN_NOISE_MSG_PAYLOAD_WITH_TAG;
    // If encrypted_payload_len_with_tag IS 16, max_plaintext_len is 0. A 0-len plaintext is valid.
    let mut decrypted_zmtp_frame_buf = vec![0u8; max_plaintext_len];

    let len_decrypted = self.transport_state.read_message(
      encrypted_payload_with_tag_slice, // This is ciphertext + tag
      &mut decrypted_zmtp_frame_buf,
    )?; // SnowError converted to ZmqError

    decrypted_zmtp_frame_buf.truncate(len_decrypted);
    Ok(Some(Bytes::from(decrypted_zmtp_frame_buf)))
  }
}
