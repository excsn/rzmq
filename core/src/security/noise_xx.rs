#![cfg(feature = "noise_xx")]

use super::cipher::IDataCipher;
use crate::error::ZmqError;
use crate::security::framer::{ISecureFramer, LengthPrefixedFramer};
use crate::security::mechanism::ProcessTokenAction;
use crate::security::{Mechanism, MechanismStatus, Metadata};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use snow::error::{Prerequisite, StateProblem};
use snow::params::NoiseParams;
use snow::{Error as SnowError, TransportState};

pub struct NoiseXxMechanism {
  is_server_role: bool,
  state: Option<snow::HandshakeState>,

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
        &self
          .state
          .as_ref()
          .map(|s| s.is_handshake_finished())
          .unwrap_or(false),
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
        &self
          .pending_outgoing_handshake_msg
          .as_ref()
          .map(|v| v.len()),
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
          "NOISE_XX handshake purportedly finished but no remote static key material was obtained."
            .to_string();
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
          self.error_reason_str =
            Some("NOISE_XX: Server public key mismatch. Connection rejected.".into());
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
          tracing::error!(
            mechanism = Self::NAME,
            role = "Client",
            "Server public key validation FAILED during NOISE_XX handshake.
                        Expected PK (configured via socket option): {:?}, 
                        Actual PK received from peer during handshake: {:?}",
            expected_server_pk_array.as_slice(),
            handshake_derived_peer_pk.as_slice()
          );
          self.current_status = MechanismStatus::Error;
          self.error_reason_str =
            Some("NOISE_XX: Server public key mismatch. Connection rejected.".into());

          // SnowError::Decrypt is often used to signal an authentication/integrity failure.
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
  pub fn into_data_cipher(mut self) -> Result<(Box<dyn IDataCipher>, Option<Vec<u8>>), ZmqError> {
    if self.current_status != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "Noise handshake not complete, cannot create data cipher.".into(),
      ));
    }
    let ts = self.transport_state.take().ok_or_else(|| {
      ZmqError::Internal("NoiseXxMechanism is Ready but TransportState is missing.".into())
    })?;

    let peer_id = self.verified_peer_static_pk.clone(); // Clone before self is fully consumed by Box::new

    Ok((
      Box::new(NoiseDataCipher {
        transport_state: ts,
      }),
      peer_id,
    ))
  }
}

#[async_trait::async_trait]
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
    if self.current_status == MechanismStatus::Ready
      || self.current_status == MechanismStatus::Error
    {
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

    let handshake_state = self.state.as_mut().ok_or_else(|| {
      ZmqError::InvalidState(
        "Noise HandshakeState missing when trying to produce new token.".into(),
      )
    })?;

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
      tracing::trace!(
        "NOISE_XX produce_token: Not my turn and no pending message. Waiting for peer."
      );
      Ok(None) // Not our turn to send.
    }
  }

  fn process_token(&mut self, token: &[u8]) -> Result<ProcessTokenAction, ZmqError> {
    if self.current_status == MechanismStatus::Error
      || self.current_status == MechanismStatus::Ready
    {
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

    let handshake_state = self.state.as_mut().ok_or_else(|| {
      ZmqError::InvalidState("Noise HandshakeState missing before process_token".into())
    })?;

    let mut read_payload_buf = vec![0u8; 1024];
    let _payload_len = handshake_state.read_message(token, &mut read_payload_buf)?;

    if handshake_state.is_handshake_finished() {
      // This is true if peer's message was the last one in the pattern (e.g. server processing client's s,ss).
      tracing::debug!(
        "NOISE_XX process_token (is_server={}): snow::HandshakeState finished after read_message. Transitioning state.",
        self.is_server_role
      );
      self.transition_to_final_state()?; // Sets self.current_status to Ready or Error
      Ok(ProcessTokenAction::HandshakeComplete)
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
      Ok(ProcessTokenAction::ProduceAndSend)
    } else {
      tracing::trace!(
        "NOISE_XX process_token (is_server={}): Processed token, awaiting peer's next. No pending message generated.",
        self.is_server_role
      );
      self.pending_outgoing_handshake_msg = None;
      Ok(ProcessTokenAction::ContinueWaiting)
    }
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

  fn into_framer(
    mut self: Box<Self>,
  ) -> Result<(Box<dyn ISecureFramer>, Option<Vec<u8>>), ZmqError> {
    if self.current_status != MechanismStatus::Ready {
      return Err(ZmqError::InvalidState(
        "Noise handshake not complete, cannot create framer.".into(),
      ));
    }

    // The into_data_cipher method consumes the mechanism to produce the cipher.
    // We can call it here to get the parts.
    let (cipher, peer_id) = self.into_data_cipher()?;

    // Wrap the pure cipher in the framer.
    let framer = Box::new(LengthPrefixedFramer::new(cipher));

    Ok((framer, peer_id))
  }
}

#[derive(Debug)] // TransportState isn't Debug, so manual or selective Debug
struct NoiseDataCipher {
  transport_state: TransportState,
}

impl IDataCipher for NoiseDataCipher {
  fn encrypt(&mut self, plaintext: &[u8]) -> Result<Vec<u8>, ZmqError> {
    const NOISE_TAG_LEN: usize = 16;
    if plaintext.len() > (u16::MAX as usize - NOISE_TAG_LEN) {
      return Err(ZmqError::InvalidMessage(
        "Payload too large for Noise frame.".into(),
      ));
    }

    let mut output_buffer = vec![0u8; plaintext.len() + NOISE_TAG_LEN];

    let bytes_written = self
      .transport_state
      .write_message(plaintext, &mut output_buffer)?;

    output_buffer.truncate(bytes_written);
    Ok(output_buffer)
  }

  fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, ZmqError> {
    const MIN_NOISE_MSG_LEN: usize = 16; // Smallest possible is just the tag
    if ciphertext.len() < MIN_NOISE_MSG_LEN {
      return Err(ZmqError::ProtocolViolation(
        "Noise message too short for tag.".into(),
      ));
    }

    let max_plaintext_len = ciphertext.len() - MIN_NOISE_MSG_LEN;
    let mut decrypted_buffer = vec![0u8; max_plaintext_len];

    let len_decrypted = self
      .transport_state
      .read_message(ciphertext, &mut decrypted_buffer)?;

    decrypted_buffer.truncate(len_decrypted);
    Ok(decrypted_buffer)
  }
}

// Helper to convert SnowError to ZmqError
impl From<SnowError> for ZmqError {
  fn from(e: SnowError) -> Self {
    tracing::warn!("Snow protocol error occurred: {}", e); // Log the Display form of SnowError
    match e {
      SnowError::Pattern(problem) => {
        // Example: Map specific pattern problems if needed, or use a generic message
        ZmqError::SecurityError(format!("Noise pattern configuration error: {:?}", problem))
      }
      SnowError::Init(stage) => {
        ZmqError::SecurityError(format!("Noise initialization error at stage: {:?}", stage))
      }
      SnowError::Prereq(prereq) => match prereq {
        Prerequisite::LocalPrivateKey => ZmqError::SecurityError(
          "Noise prerequisite error: Local private key missing or invalid.".into(),
        ),
        Prerequisite::RemotePublicKey => ZmqError::SecurityError(
          "Noise prerequisite error: Remote public key missing or invalid for current operation."
            .into(),
        ),
      },
      SnowError::State(problem) => match problem {
        StateProblem::MissingKeyMaterial => ZmqError::SecurityError(
          "Noise state error: Missing required key material for operation.".into(),
        ),
        StateProblem::HandshakeNotFinished => ZmqError::InvalidState(
          "Noise state error: Handshake is not yet finished for requested operation.".into(),
        ),
        StateProblem::HandshakeAlreadyFinished => {
          ZmqError::InvalidState("Noise state error: Handshake is already finished.".into())
        }
        // Map other StateProblem variants as needed to more specific ZmqErrors or generic SecurityError
        _ => ZmqError::SecurityError(format!("Noise state machine error: {:?}", problem)),
      },
      SnowError::Input => {
        // This often relates to message sizes or invalid input to read/write_message
        ZmqError::SecurityError(
          "Noise input error: Invalid message format or size for current state.".into(),
        )
      }
      SnowError::Dh => ZmqError::SecurityError("Noise Diffie-Hellman operation failed.".into()),
      SnowError::Decrypt => {
        // This is critical, means message authentication (tag verification) or decryption failed.
        ZmqError::SecurityError(
          "Noise decrypt/authentication failed (e.g., bad MAC or ciphertext).".into(),
        )
      }
      #[cfg(feature = "hfs")] // If you enable hfs feature in snow
      SnowError::Kem => {
        ZmqError::SecurityError("Noise Key Encapsulation Mechanism (KEM) failed.".into())
      }
      _ => {
        // This arm catches any future variants added to snow::Error
        // or any existing variants not explicitly matched above (if any).
        tracing::error!("Unhandled snow::Error variant: {}", e);
        ZmqError::SecurityError(format!("Unhandled or new Noise protocol error: {}", e))
      }
    }
  }
}
