// src/protocol/zmtp/command.rs

// <<< ADDED ZMTP FRAMING CONSTANTS >>>

// --- ZMTP Frame Flags ---
// Located in the first byte of the length field for messages/commands.
pub const ZMTP_FLAG_LONG: u8 = 0b0000_0010; // Indicates 8-byte length instead of 1-byte
pub const ZMTP_FLAG_MORE: u8 = 0b0000_0001; // Indicates more frames follow (like rzmq::MsgFlags::MORE)
pub const ZMTP_FLAG_COMMAND: u8 = 0b0000_0100; // Indicates this is a ZMTP command frame

// --- ZMTP Command Names ---
// Used as the first part of a COMMAND frame's body.
pub const ZMTP_CMD_READY: &[u8] = b"\x05READY";
pub const ZMTP_CMD_ERROR: &[u8] = b"\x05ERROR";
pub const ZMTP_CMD_SUBSCRIBE: &[u8] = b"\x09SUBSCRIBE";
pub const ZMTP_CMD_CANCEL: &[u8] = b"\x06CANCEL";
pub const ZMTP_CMD_PING: &[u8] = b"\x04PING";
pub const ZMTP_CMD_PONG: &[u8] = b"\x04PONG";
// Security mechanisms define others (HELLO, INITIATE, etc.)

// <<< ADDED ZMTP FRAMING CONSTANTS END >>>

// TODO: Define ZmtpCommand enum/structs for parsing command bodies later
