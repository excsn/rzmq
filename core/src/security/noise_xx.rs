#![cfg(feature = "noise_xx")]

use crate::error::ZmqError;
use crate::security::{Mechanism, MechanismStatus, Metadata};
use bytes::{Buf, BufMut, Bytes, BytesMut};
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
  pub const NAME_BYTES: &'static [u8; 20] = b"NOISE_XX\0\0\0\0\0\0\0\0\0\0\0\0";
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
    // If there's a message prepared by a previous process_token() call, prioritize sending it.
    if let Some(msg_to_send) = self.pending_outgoing_handshake_msg.take() {
      tracing::debug!(
        "NOISE_XX produce_token: Sending pending msg (len {}) from previous process_token.",
        msg_to_send.len()
      );

      // If this pending message is the initiator's final one (s,ss for XX):
      // The snow::HandshakeState became "finished" when this msg was *prepared* in process_token.
      // Now that we are *producing* it for the engine to send, perform final verification and transition.
      if !self.is_server_role {
        // Client/Initiator
        if let Some(state) = self.state.as_ref() {
          // Check without consuming
          if state.is_handshake_finished() {
            tracing::debug!(
              "NOISE_XX produce_token (Initiator): Final message produced. Transitioning state (verifies peer PK)."
            );
            // transition_to_final_state will set self.current_status to Ready or Error.
            // If it errors (e.g. PK mismatch), the error is propagated from this produce_token call.
            self.transition_to_final_state()?;
          }
        }
      }
      return Ok(Some(msg_to_send));
    }

    // If no pending message, check if we should generate a new one.
    // Do not proceed if already Ready or in Error.
    if self.current_status == MechanismStatus::Ready || self.current_status == MechanismStatus::Error {
      return Ok(None);
    }

    // Ensure we are in Handshaking state if we are about to produce a token.
    if self.current_status == MechanismStatus::Initializing {
      self.current_status = MechanismStatus::Handshaking;
      // For server/responder, it waits for client's first message, so produce_token is None initially.
      if self.is_server_role {
        return Ok(None);
      }
    }

    let handshake_state = self
      .state
      .as_mut()
      .ok_or_else(|| ZmqError::InvalidState("Noise HandshakeState missing when trying to produce new token.".into()))?;

    if handshake_state.is_my_turn() {
      let mut msg_buf = vec![0u8; 1024];
      let len = handshake_state.write_message(&[], &mut msg_buf)?;
      msg_buf.truncate(len);
      tracing::debug!(
        "NOISE_XX produce_token (is_server={}): Generated NEW handshake message (len {}).",
        self.is_server_role,
        len
      );

      // If *this current write* finished the snow::HandshakeState (e.g. for responder, or other patterns).
      // For XX Initiator, its first write (->e) does NOT finish snow::HandshakeState.
      // For XX Responder, its first write (->e,es) does NOT finish snow::HandshakeState.
      // This block is thus more for the responder completing its part if its write is final for the pattern.
      if handshake_state.is_handshake_finished() {
        tracing::debug!(
          "NOISE_XX produce_token: snow::HandshakeState became finished *after this write*. Transitioning state."
        );
        if self.is_server_role {
          // Only responder should transition to Ready based on its own write completing the pattern.
          self.transition_to_final_state()?;
        }
        // If initiator, its transition to Ready happens when its *final pending* message is produced (handled above).
      }
      Ok(Some(msg_buf))
    } else {
      tracing::trace!("NOISE_XX produce_token: Not my turn and no pending message. Waiting for peer.");
      Ok(None) // Not our turn to send.
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
      "NOISE_XX process_token (is_server={}): Received handshake message (len {})",
      self.is_server_role,
      token.len()
    );

    let handshake_state = self
      .state
      .as_mut()
      .ok_or_else(|| ZmqError::InvalidState("Noise HandshakeState missing before process_token".into()))?;

    let mut read_payload_buf = vec![0u8; 1024];
    let _payload_len = handshake_state.read_message(token, &mut read_payload_buf)?;

    if handshake_state.is_handshake_finished() {
      // This is true if peer's message was the last one in the pattern (e.g. server processing client's s,ss).
      tracing::debug!(
        "NOISE_XX process_token (is_server={}): snow::HandshakeState finished after read_message. Transitioning state.",
        self.is_server_role
      );
      self.transition_to_final_state()?; // Sets self.current_status to Ready or Error
      self.pending_outgoing_handshake_msg = None;
    } else if handshake_state.is_my_turn() {
      // It's now our turn to write. Prepare the message.
      let mut msg_buf = vec![0u8; 1024];
      let len = handshake_state.write_message(&[], &mut msg_buf)?;
      msg_buf.truncate(len);
      tracing::debug!(
        "NOISE_XX process_token (is_server={}): Generated next handshake message (len {}) for pending send.",
        self.is_server_role,
        len
      );
      self.pending_outgoing_handshake_msg = Some(msg_buf);

      // Client (Initiator) specific:
      // If this write_message *would have been* the initiator's last (s,ss),
      // its snow::HandshakeState is now finished. However, we DO NOT transition to Ready here.
      // The transition to Ready (including PK verification) will happen when this pending message
      // is actually *produced* by produce_token().
      if !self.is_server_role && handshake_state.is_handshake_finished() {
        tracing::debug!("NOISE_XX process_token (Initiator): snow::HandshakeState finished after preparing final message. Mechanism status remains Handshaking. Verification will happen in produce_token().");
      }
    } else {
      tracing::trace!(
        "NOISE_XX process_token (is_server={}): Processed token, awaiting peer's next. No pending message generated.",
        self.is_server_role
      );
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

    if plaintext_zmtp_frame.len() > (u16::MAX as usize - NOISE_TAG_LEN) {
      // Max ZMTP frame size
      return Err(ZmqError::InvalidMessage(
        "ZMTP frame too large for Noise message.".into(),
      ));
    }

    // Buffer for [2-byte len_prefix][ciphertext][16-byte tag]
    // The ciphertext will be the same length as plaintext_zmtp_frame for ChaChaPoly.
    let ciphertext_len = plaintext_zmtp_frame.len();
    let payload_with_tag_len = ciphertext_len + NOISE_TAG_LEN; // This is the length that goes into the prefix

    let mut output_buffer = BytesMut::with_capacity(NOISE_LEN_PREFIX_LEN + payload_with_tag_len);
    output_buffer.put_u16(payload_with_tag_len as u16); // Write length prefix (BIG ENDIAN default for put_u16)

    // Prepare a buffer for snow to write ciphertext + tag into
    // This buffer does not include the prefix space.
    let mut snow_output_buf = vec![0u8; payload_with_tag_len];

    tracing::trace!(
      plaintext_len = plaintext_zmtp_frame.len(),
      snow_output_buffer_capacity = snow_output_buf.len(),
      "NoiseDataCipher::encrypt: Calling snow's write_message."
    );

    // snow.write_message writes ciphertext + tag into snow_output_buf
    let bytes_written_by_snow = self.transport_state.write_message(
      &plaintext_zmtp_frame, // This is the "payload" for the Noise message
      &mut snow_output_buf,  // snow writes ciphertext+tag here
    )?;

    // bytes_written_by_snow should be equal to payload_with_tag_len
    if bytes_written_by_snow != payload_with_tag_len {
      tracing::error!(
        expected_snow_write = payload_with_tag_len,
        actual_snow_write = bytes_written_by_snow,
        "Mismatch in expected bytes written by snow."
      );
      return Err(ZmqError::EncryptionError("Snow write_message length mismatch".into()));
    }

    output_buffer.put_slice(&snow_output_buf[..bytes_written_by_snow]); // Append ciphertext+tag

    tracing::trace!(
        total_noise_msg_len = output_buffer.len(), // Should be 2 + payload_with_tag_len
        prefix_val = payload_with_tag_len,
        actual_len_prefix_bytes = ?output_buffer.get(..2),
        "NoiseDataCipher::encrypt: Encryption successful."
    );

    Ok(output_buffer.freeze())
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
