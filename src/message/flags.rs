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
