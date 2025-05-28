mod cipher;
pub(crate) mod null;
pub(crate) mod plain;
pub mod mechanism;
pub mod zap;

pub(crate) use cipher::{IDataCipher, PassThroughDataCipher};
pub(crate) use mechanism::{Mechanism, MechanismStatus};
pub(crate) use {null::NullMechanism, plain::PlainMechanism};
pub(crate) use zap::ZapClient;

use crate::error::ZmqError;
use crate::message::Metadata;

#[cfg(feature = "noise_xx")]
mod noise_xx;
#[cfg(feature = "noise_xx")]
mod noise_stream;

#[cfg(feature = "noise_xx")]
pub use noise_xx::NoiseXxMechanism;