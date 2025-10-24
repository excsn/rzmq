use crate::error::ZmqError;
use crate::security::curve::handshake::CurveHandshake;
use crate::security::{Mechanism, MechanismStatus, Metadata};
use crate::transport::ZmtpStdStream;

/// The public-facing wrapper for the CurveZMQ security mechanism.
/// This struct implements the `Mechanism` trait and holds the underlying handshake state machine.
#[derive(Debug)]
pub(crate) struct CurveMechanism {
  pub(crate) handshake: CurveHandshake,
  pub(crate) status: MechanismStatus,
  pub(crate) error_reason: Option<String>,
}

impl CurveMechanism {
  pub const NAME: &'static str = "CURVE";
  // ZMTP mechanism names are 20 bytes, padded with nulls.
  pub const NAME_BYTES: &'static [u8; 20] = b"CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0";
}

