# rzmq API Reference

**Version:** 1.0

This document provides a quick reference to the public API of the `rzmq` library. `rzmq` offers asynchronous ZeroMQ patterns using Tokio.

## Core Concepts

*   **Context:** Manages resources and creates sockets. Obtain via `rzmq::context()`. Must live as long as its sockets. Cloneable.
*   **Socket:** Represents a ZMQ socket (PUB, SUB, REQ, etc.). Created via `Context::socket()`. Use `bind()` or `connect()` to establish communication. Handles are cloneable. Use `async`/`.await` for all operations.
*   **Msg:** Represents a message frame. Holds data (`bytes::Bytes`). Use `MsgFlags::MORE` for multi-part messages. Cloneable (cheaply).
*   **Endpoint:** String specifying the address (e.g., `"tcp://127.0.0.1:5555"`, `"ipc:///tmp/mysock"`, `"inproc://my_service"`).
*   **Error:** All fallible functions return `Result<T, rzmq::ZmqError>`.

## Top-Level Functions

```rust
/// Creates a new library context.
pub fn context() -> Result<Context, ZmqError>;

/// Returns the library version (major, minor, patch).
pub fn version() -> (i32, i32, i32);

/// Returns the major version number.
pub fn version_major() -> i32;

/// Returns the minor version number.
pub fn version_minor() -> i32;

/// Returns the patch version number.
pub fn version_patch() -> i32;
```

## `rzmq::Context`

Manages sockets and shared resources.

```rust
impl Context {
    /// Creates a new, independent context.
    pub fn new() -> Result<Self, ZmqError>;

    /// Creates a socket of the specified type associated with this context.
    ///
    /// # Arguments
    /// * `socket_type`: A `rzmq::SocketType` enum variant (e.g., `SocketType::Pub`).
    pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>;

    /// Initiates background shutdown of all sockets created by this context.
    /// Returns immediately. Sockets will close gracefully.
    pub fn shutdown(&self) -> Result<(), ZmqError>;

    /// Shuts down all sockets and waits for their clean termination.
    /// Consumes the Context handle. Use for final cleanup.
    /// (Note: Awaiting behavior might depend on calling context - async/sync)
    pub fn term(self) -> Result<(), ZmqError>;

    // Context is Cloneable (cheaply, uses Arc internally)
}
```

## `rzmq::Socket`

The main handle for interacting with a socket.

```rust
// Socket is Cloneable (cheaply, uses Arc internally)

impl Socket {
    /// Binds the socket to listen on a local endpoint.
    /// Example: `socket.bind("tcp://*:5555").await?`
    pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>;

    /// Connects the socket to a remote endpoint.
    /// Example: `socket.connect("tcp://127.0.0.1:5555").await?`
    pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>;

    /// Disconnects from a specific endpoint previously connected to via `connect`.
    pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>;

    /// Stops listening on a specific endpoint previously bound to via `bind`.
    pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>;

    /// Sends a message asynchronously. Respects socket pattern and HWM.
    /// For multi-part, set `MsgFlags::MORE` on message parts before the last.
    pub async fn send(&self, msg: Msg) -> Result<(), ZmqError>;

    /// Receives a message asynchronously. Awaits until a message arrives.
    pub async fn recv(&self) -> Result<Msg, ZmqError>;

    /// Sets a socket option (e.g., `rzmq::SNDHWM`, `rzmq::SUBSCRIBE`).
    /// `value` is interpreted based on the option type.
    pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>;

    /// Gets the current value of a socket option.
    /// Returns the value as raw bytes.
    pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>;

    /// Initiates graceful shutdown of the socket.
    /// Dropping the last Socket clone also triggers this implicitly.
    pub async fn close(&self) -> Result<(), ZmqError>;
}
```

## `rzmq::Msg`

Represents a message frame.

```rust
// Msg is Cloneable (cheaply, shares underlying Bytes)

impl Msg {
    /// Creates an empty message.
    pub fn new() -> Self;

    /// Creates a message from a Vec<u8> (takes ownership).
    pub fn from_vec(data: Vec<u8>) -> Self;

    /// Creates a message from bytes::Bytes (efficient).
    pub fn from_bytes(data: bytes::Bytes) -> Self;

    /// Creates a message from a static byte slice (zero-copy).
    pub fn from_static(data: &'static [u8]) -> Self;

    /// Returns a reference to the message payload, if any.
    pub fn data(&self) -> Option<&[u8]>;

    /// Returns the size of the message payload in bytes.
    pub fn size(&self) -> usize;

    /// Returns the flags associated with the message.
    pub fn flags(&self) -> MsgFlags;

    /// Sets the flags for the message (e.g., `MsgFlags::MORE`).
    pub fn set_flags(&mut self, flags: MsgFlags);

    /// Checks if the `MORE` flag is set.
    pub fn is_more(&self) -> bool;

    /// Returns an immutable reference to the message metadata map.
    pub fn metadata(&self) -> &Metadata;

    /// Returns a mutable reference to the message metadata map.
    pub fn metadata_mut(&mut self) -> &mut Metadata;
}
```

## `rzmq::MsgFlags`

Bitflags for messages.

```rust
bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct MsgFlags: u8 {
        /// More message parts follow this one.
        const MORE = 0b01;
        /// Internal: Indicates a ZMTP command frame.
        const COMMAND = 0b10;
        // Other flags TBD
    }
}
```

## `rzmq::Metadata`

Type map for out-of-band message data.

```rust
impl Metadata {
    /// Creates an empty metadata map.
    pub fn new() -> Self;

    /// Inserts a typed value into the map.
    pub fn insert_typed<T: Any + Send + Sync>(&mut self, value: T);

    /// Gets an immutable reference to a typed value if present.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<&T>;

    /// Checks if a value of type T is present.
    pub fn contains<T: Any + Send + Sync>(&self) -> bool;

    /// Removes a value of type T, returning it.
    pub fn remove<T: Any + Send + Sync>(&mut self) -> Option<Arc<dyn Any + Send + Sync>>;

    // Metadata is Cloneable, Debug, Default
}

// Example Key Types (Define these or use your own marker types)
// pub struct PeerAddress(pub std::net::SocketAddr);
// pub struct ZapUserId(pub String);
```

## Enums & Constants

*   **`rzmq::SocketType`**: Enum variants `Pub`, `Sub`, `Req`, `Rep`, `Dealer`, `Router`, `Push`, `Pull`.
*   **Integer Constants**: `rzmq::PUB`, `rzmq::SUB`, etc. for `Context::socket`.
*   **Option Constants**: `rzmq::SNDHWM`, `rzmq::RCVHWM`, `rzmq::LINGER`, `rzmq::SUBSCRIBE`, `rzmq::ROUTING_ID`, `rzmq::TCP_KEEPALIVE`, `rzmq::CURVE_SERVER`, etc. for `Socket::set/get_option`.

## Error Type

*   **`rzmq::ZmqError`**: Enum implementing `std::error::Error`. Returned by all fallible functions. Check its variants for specific failure reasons (I/O, Timeout, Protocol, Security, State, etc.).