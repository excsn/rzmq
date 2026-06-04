use bitflags::bitflags;

bitflags! {
    /// Flags associated with a `Msg` indicating its role or attributes.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct MsgFlags: u8 {
        /// More message parts follow this one.
        const MORE = 0b01;
        /// Internal: Indicates a ZMTP command frame.
        const COMMAND = 0b10;
        // Add other flags later if needed
    }
}

#[cfg(test)]
mod additional_flags_tests {
  use super::*;

  #[test]
  fn test_flags_bitwise_operations() {
    let more_flag = MsgFlags::MORE;
    let cmd_flag = MsgFlags::COMMAND;

    let combined = more_flag | cmd_flag;
    assert!(combined.contains(MsgFlags::MORE));
    assert!(combined.contains(MsgFlags::COMMAND));

    let intersection = combined & MsgFlags::MORE;
    assert!(intersection.contains(MsgFlags::MORE));
    assert!(!intersection.contains(MsgFlags::COMMAND));

    let difference = combined - MsgFlags::MORE;
    assert!(!difference.contains(MsgFlags::MORE));
    assert!(difference.contains(MsgFlags::COMMAND));
  }
}
