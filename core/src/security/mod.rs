mod cipher;
pub(crate) mod framer;
pub mod mechanism;
pub(crate) mod null;
pub(crate) mod plain;
pub mod zap;

pub(crate) use cipher::IDataCipher;
pub(crate) use mechanism::{Mechanism, MechanismStatus};
pub(crate) use {null::NullMechanism, plain::PlainMechanism};

use crate::message::Metadata;
use crate::{error::ZmqError, protocol::zmtp::ZmtpGreeting, socket::ZmtpEngineConfig};

#[cfg(feature = "curve")]
mod curve;

#[cfg(feature = "noise_xx")]
mod noise_xx;

#[cfg(feature = "curve")]
pub(crate) use curve::CurveMechanism;

#[cfg(feature = "noise_xx")]
pub(crate) use noise_xx::NoiseXxMechanism;

// Mechanism Initializer Type and specific initializers
type MechanismInitializerFn = fn(
  is_server: bool,
  local_config: &ZmtpEngineConfig,
  engine_handle_for_log: usize, // Added for consistency in logging
) -> Result<Box<dyn Mechanism>, ZmqError>;

struct KnownMechanismDescriptor {
  name_static_bytes: &'static [u8; 20], // For matching greeting
  name_str: &'static str,               // For logging and matching
  initializer: MechanismInitializerFn,
  is_locally_enabled: fn(local_config: &ZmtpEngineConfig) -> bool,
}

fn initialize_null(
  _is_server: bool,
  _local_config: &ZmtpEngineConfig,
  _engine_handle_for_log: usize,
) -> Result<Box<dyn Mechanism>, ZmqError> {
  Ok(Box::new(NullMechanism))
}

fn initialize_plain(
  is_server: bool,
  local_config: &ZmtpEngineConfig,
  _engine_handle_for_log: usize, // May use for logging inside if needed
) -> Result<Box<dyn Mechanism>, ZmqError> {
  let mut plain_mech = PlainMechanism::new(is_server);
  if !is_server {
    // Client role
    // Credentials might be None, PlainMechanism::set_client_credentials handles Option
    let user_bytes = local_config
      .plain_username_for_engine
      .as_ref()
      .map(|s| s.as_bytes().to_vec());
    let pass_bytes = local_config
      .plain_password_for_engine
      .as_ref()
      .map(|s| s.as_bytes().to_vec());
    plain_mech.set_client_credentials(user_bytes, pass_bytes);
  }
  Ok(Box::new(plain_mech))
}

#[cfg(feature = "curve")]
fn initialize_curve(
  is_server: bool,
  local_config: &ZmtpEngineConfig,
  _engine_handle_for_log: usize,
) -> Result<Box<dyn Mechanism>, ZmqError> {
  use crate::security::curve::handshake::CurveHandshake;
  let handshake = CurveHandshake::new(is_server, local_config)?;
  Ok(Box::new(CurveMechanism {
    handshake,
    status: MechanismStatus::Handshaking,
    error_reason: None,
  }))
}

#[cfg(feature = "noise_xx")]
fn initialize_noise_xx(
  is_server: bool,
  local_config: &ZmtpEngineConfig,
  _engine_handle_for_log: usize,
) -> Result<Box<dyn Mechanism>, ZmqError> {
  let local_sk_bytes_arr: [u8; 32] =
    local_config
      .noise_xx_local_sk_bytes_for_engine
      .ok_or_else(|| {
        ZmqError::SecurityError("NOISE_XX: Local static secret key not configured.".into())
      })?;
  let remote_pk_config_opt_ref: Option<[u8; 32]> = local_config
    .noise_xx_remote_pk_bytes_for_engine
    .as_ref()
    .copied();

  Ok(Box::from(NoiseXxMechanism::new(
    is_server,
    &local_sk_bytes_arr,
    remote_pk_config_opt_ref,
  )?))
}

const KNOWN_MECHANISMS: &[KnownMechanismDescriptor] = &[
  KnownMechanismDescriptor {
    name_static_bytes: NullMechanism::NAME_BYTES,
    name_str: NullMechanism::NAME,
    initializer: initialize_null,
    is_locally_enabled: |cfg| {
      // NULL is enabled if no other security mechanisms are.
      !cfg.security_enabled
    },
  },
  KnownMechanismDescriptor {
    name_static_bytes: PlainMechanism::NAME_BYTES,
    name_str: PlainMechanism::NAME,
    initializer: initialize_plain,
    is_locally_enabled: |cfg| cfg.use_plain,
  },
  #[cfg(feature = "curve")]
  KnownMechanismDescriptor {
    name_static_bytes: CurveMechanism::NAME_BYTES,
    name_str: CurveMechanism::NAME,
    initializer: initialize_curve,
    is_locally_enabled: |cfg| cfg.use_curve,
  },
  #[cfg(feature = "noise_xx")]
  KnownMechanismDescriptor {
    name_static_bytes: NoiseXxMechanism::NAME_BYTES,
    name_str: NoiseXxMechanism::NAME,
    initializer: initialize_noise_xx,
    is_locally_enabled: |cfg| cfg.use_noise_xx,
  },
];

pub(crate) fn negotiate_security_mechanism(
  is_server: bool,
  local_config: &ZmtpEngineConfig,
  peer_greeting: &ZmtpGreeting,
  engine_handle_for_log: usize,
) -> Result<Box<dyn Mechanism>, ZmqError> {
  let peer_proposed_mechanism_name_bytes = &peer_greeting.mechanism; // This is [u8; 20]

  let noise_config_log_val = {
    #[cfg(feature = "noise_xx")]
    {
      format!("{}", local_config.use_noise_xx)
    }
    #[cfg(not(feature = "noise_xx"))]
    {
      "N/A (feature disabled)".to_string()
    }
  };
  tracing::info!(
    engine_handle = engine_handle_for_log,
    peer_mechanism_proposal_bytes = ?std::str::from_utf8(peer_proposed_mechanism_name_bytes).unwrap_or("").trim_end_matches('\0'),
    config_use_plain = local_config.use_plain,
    config_use_noise = %noise_config_log_val,
    "Negotiating security mechanism."
  );

  for descriptor in KNOWN_MECHANISMS {
    // Compare the full 20-byte mechanism field from the greeting
    if descriptor.name_static_bytes == peer_proposed_mechanism_name_bytes {
      // Peer proposed a mechanism we know by its byte signature.
      // Now check if we have it enabled locally.
      if (descriptor.is_locally_enabled)(local_config) {
        tracing::info!(
          engine_handle = engine_handle_for_log,
          mechanism_name = descriptor.name_str,
          "Peer proposed '{}', which is locally enabled. Attempting initialization.",
          descriptor.name_str
        );
        return (descriptor.initializer)(is_server, local_config, engine_handle_for_log);
      } else {
        // We know this mechanism, but it's not enabled in our config.
        let err_msg = format!(
          "Peer proposed mechanism '{}', but it is not enabled in the local configuration.",
          descriptor.name_str
        );
        tracing::error!(engine_handle = engine_handle_for_log, "{}", err_msg);
        return Err(ZmqError::SecurityError(err_msg));
      }
    }
  }

  // If loop completes, peer proposed something not in KNOWN_MECHANISMS at all.
  let peer_name_str = std::str::from_utf8(peer_proposed_mechanism_name_bytes)
    .unwrap_or("<invalid_utf8>")
    .trim_end_matches('\0');
  let security_error_msg = format!(
    "Unsupported or unknown security mechanism '{}' proposed by peer.",
    peer_name_str
  );
  tracing::error!(engine_handle = engine_handle_for_log, error = %security_error_msg);
  Err(ZmqError::SecurityError(security_error_msg))
}
