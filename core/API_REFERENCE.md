# rzmq API Reference

This document provides a reference to the public API components of the `rzmq` library, designed for developers integrating `rzmq` into their applications.

## Core Concepts

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ messaging patterns, built upon the Tokio asynchronous runtime. It uses an actor-based internal architecture for managing socket state, protocol handling, and I/O operations.

*   **`Context`**: The primary entry point for using `rzmq`. A `Context` manages shared resources and is the factory for creating `Socket` instances. It's lightweight and cloneable.
*   **`Socket`**: Represents a ZeroMQ-style socket. It provides methods for network operations like binding, connecting, sending, and receiving messages. Sockets are also lightweight, cloneable handles to underlying actor machinery.
*   **`SocketType`**: An enum defining the messaging pattern of a `Socket` (e.g., PUB, SUB, REQ, REP, PUSH, PULL, DEALER, ROUTER). This choice dictates the socket's communication behavior.
*   **`Msg`**: Represents a single message part or frame. `rzmq` supports multi-part messages, where a logical message can consist of several `Msg` instances.
*   **`MsgFlags`**: Flags associated with a `Msg`, primarily `MsgFlags::MORE` to indicate that more parts of a multi-part message will follow.
*   **Asynchronous Operations**: All network operations (`bind`, `connect`, `send`, `recv`, `close`, `term`) are asynchronous and return `Future`s that must be `.await`ed.
*   **Socket Options**: Behavior of sockets can be customized using options (e.g., high-water marks, timeouts) set via `Socket::set_option()` and retrieved with `Socket::get_option()`. Constants for these options are typically found in the `rzmq::socket::options` module.
*   **Error Handling**: Most operations return `Result<T, ZmqError>`, where `ZmqError` is the library's primary error enum.
*   **Monitoring**: Sockets can be monitored for lifecycle events (e.g., connection, disconnection) using `Socket::monitor()`, which returns a `MonitorReceiver`.
*   **Graceful Shutdown**: The `Context::term()` method should be called for a graceful shutdown of all resources. Individual sockets can be closed with `Socket::close()`.

## Main Entry Points

*   **`rzmq::Context::new()`**: Creates a new communication context.
*   **`rzmq::Context::socket(socket_type: SocketType)`**: Creates a socket of a specific pattern.
*   **`#[rzmq::main]` (Attribute Macro, conditional on `io-uring` feature)**:
    ```rust
    #[cfg(feature = "io-uring")]
    pub use rzmq_macros::main;
    ```
    When the `io-uring` feature is enabled for `rzmq` (and on Linux), this attribute should be used on your `async main` function instead of `#[tokio::main]`. It automatically configures the Tokio runtime to use `tokio-uring`. If the feature is not enabled or not on Linux, it defaults to `#[tokio::main]`.

    *Usage:*
    ```rust
    // #[rzmq::main] // if io-uring feature is enabled
    // #[tokio::main] // otherwise
    // async fn main() { /* ... */ }
    ```

## Common Types and Patterns

*   **`rzmq::ZmqError`**: The primary enum for errors throughout the library. (See [Error Handling](#error-handling))
*   **`Result<T, ZmqError>`**: The standard result type for most fallible operations. Often aliased as `rzmq::ZmqResult<T>`.
*   **`rzmq::Msg`**: The type for message frames.
*   **`rzmq::Blob`**: An immutable, cheaply cloneable byte sequence, often used for identities or subscription topics.

## Configuration

Configuration is primarily done through socket options.

### Socket Options (Constants)

These constants are typically used with `Socket::set_option()` and `Socket::get_option()`. They are usually located in the `rzmq::socket::options` module.

*   **`rzmq::socket::options::SNDHWM`**: `i32` - Send High-Water Mark (max number of outgoing messages queued).
*   **`rzmq::socket::options::RCVHWM`**: `i32` - Receive High-Water Mark (max number of incoming messages queued).
*   **`rzmq::socket::options::LINGER`**: `i32` - Linger period in milliseconds for pending messages on close (-1: infinite, 0: immediate, >0: timeout).
*   **`rzmq::socket::options::SNDTIMEO`**: `i32` - Send timeout in milliseconds (-1: infinite block, 0: non-blocking, >0: timeout).
*   **`rzmq::socket::options::RCVTIMEO`**: `i32` - Receive timeout in milliseconds (-1: infinite block, 0: non-blocking (though actual behavior might depend on pattern), >0: timeout).
*   **`rzmq::socket::options::RECONNECT_IVL`**: `i32` - Initial reconnect interval in milliseconds (0: no reconnect after first failure, -1: use default, >0: interval).
*   **`rzmq::socket::options::RECONNECT_IVL_MAX`**: `i32` - Maximum reconnect interval in milliseconds (0: disable exponential backoff, >0: max interval).
*   **`rzmq::socket::options::ROUTING_ID`**: `&[u8]` (passed as value) - Socket identity (e.g., for `DEALER`, `ROUTER`). Max 255 bytes.
*   **`rzmq::socket::options::SUBSCRIBE`**: `&[u8]` (passed as value) - For `SUB` sockets, subscribes to a topic prefix.
*   **`rzmq::socket::options::UNSUBSCRIBE`**: `&[u8]` (passed as value) - For `SUB` sockets, unsubscribes from a topic prefix.
*   **`rzmq::socket::options::TCP_KEEPALIVE`**: `i32` - TCP keepalive mode (-1: disable, 0: use system default, 1: enable).
*   **`rzmq::socket::options::TCP_KEEPALIVE_IDLE`**: `i32` - TCP keepalive idle time in seconds before probes are sent.
*   **`rzmq::socket::options::TCP_KEEPALIVE_CNT`**: `i32` - Number of TCP keepalive probes before dropping connection.
*   **`rzmq::socket::options::TCP_KEEPALIVE_INTVL`**: `i32` - Interval in seconds between TCP keepalive probes.
*   **`rzmq::socket::options::HEARTBEAT_IVL`**: `i32` - ZMTP heartbeat interval in milliseconds (0: disable).
*   **`rzmq::socket::options::HEARTBEAT_TIMEOUT`**: `i32` - ZMTP heartbeat timeout in milliseconds.
*   **`rzmq::socket::options::ROUTER_MANDATORY`**: `i32` (0 or 1) - For `ROUTER` sockets, controls behavior when routing to an unknown identity.
*   **`rzmq::socket::options::PLAIN_SERVER`**: `i32` (0 or 1) - Configures PLAIN security mechanism as server.
*   **`rzmq::socket::options::PLAIN_USERNAME`**: `&[u8]` - Username for PLAIN security.
*   **`rzmq::socket::options::PLAIN_PASSWORD`**: `&[u8]` - Password for PLAIN security.
*   **`rzmq::socket::options::ZAP_DOMAIN`**: `&[u8]` - ZAP authentication domain.
*   **`rzmq::socket::options::CURVE_SERVER`**: `i32` (0 or 1) - (Requires `curve` feature) Configures CURVE security as server.
*   **`rzmq::socket::options::CURVE_PUBLICKEY`**: `&[u8]` (32 bytes) - (Requires `curve` feature) Socket's public key.
*   **`rzmq::socket::options::CURVE_SECRETKEY`**: `&[u8]` (32 bytes) - (Requires `curve` feature) Socket's secret key.
*   **`rzmq::socket::options::CURVE_SERVERKEY`**: `&[u8]` (32 bytes) - (Requires `curve` feature) Server's public key (for client sockets).
*   **`rzmq::socket::options::IO_URING_SNDZEROCOPY`**: `i32` (0 or 1) - (Requires `io-uring` feature) Hint to use zerocopy for sends if possible.
*   **`rzmq::socket::options::IO_URING_RCVMULTISHOT`**: `i32` (0 or 1) - (Requires `io-uring` feature) Hint to use multishot receive operations if possible.

*Note: Integer option values are typically passed as `&i32_value.to_ne_bytes()` to `set_option`.*

---

## Main Types and Their Public Methods

### `rzmq::Context`

The `Context` is the entry point for creating and managing `rzmq` sockets. It encapsulates shared resources for all sockets created from it.

*   **Constructors:**
    *   `pub fn new() -> Result<Context, ZmqError>`
        *   Creates a new, independent `rzmq` context. This is the starting point for any `rzmq` application.

*   **Socket Creation:**
    *   `pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>`
        *   Creates a new `Socket` of the specified `SocketType` (e.g., `SocketType::Push`, `SocketType::Sub`) associated with this context.

*   **Termination:**
    *   `pub async fn shutdown(&self) -> Result<(), ZmqError>`
        *   Initiates a graceful background shutdown of all sockets and actors created by this context. This method returns quickly. For a blocking wait until termination is complete, use `term()`.
    *   `pub async fn term(&self) -> Result<(), ZmqError>`
        *   Initiates a graceful shutdown of the context and all associated sockets and waits for their clean termination. This is the recommended way to ensure all resources are released before the application exits.

### `rzmq::Socket`

The `Socket` struct is the public handle for interacting with a specific ZeroMQ-style socket. It is cloneable and thread-safe (`Arc`-based).

*   **Connection Management:**
    *   `pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Binds the socket to a local endpoint string (e.g., `"tcp://127.0.0.1:5555"`, `"ipc:///tmp/rzmq.sock"`, `"inproc://service_a"`). For connection-oriented transports, this allows incoming connections.
    *   `pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Connects the socket to a remote or local endpoint. This operation typically happens asynchronously in the background.
    *   `pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Disconnects from a specific endpoint that was previously established using `connect()`.
    *   `pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Stops listening on a specific endpoint that was previously bound using `bind()`.

*   **Message Sending/Receiving:**
    *   `pub async fn send(&self, msg: Msg) -> Result<(), ZmqError>`
        *   Sends a message (`Msg`) according to the socket's specific messaging pattern. For multi-part messages, ensure `MsgFlags::MORE` is set on all but the last part.
    *   `pub async fn recv(&self) -> Result<Msg, ZmqError>`
        *   Receives a message (`Msg`) according to the socket's pattern. This method will block (asynchronously) until a message is available or a timeout occurs (if `RCVTIMEO` is set). Check `msg.is_more()` for multi-part messages.

*   **Socket Options:**
    *   `pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>`
        *   Sets a socket option using an integer option ID (see [Socket Options](#socket-options-constants)) and a byte slice representing the value.
    *   `pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>`
        *   Retrieves the current value of a socket option. The returned `Vec<u8>` must be interpreted according to the option type.

*   **Lifecycle & Monitoring:**
    *   `pub async fn close(&self) -> Result<(), ZmqError>`
        *   Initiates a graceful shutdown of this specific socket, closing all its connections and releasing associated resources.
    *   `pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError>`
        *   Creates a channel for receiving `SocketEvent` notifications about this socket's lifecycle and network activity. `capacity` defines the event buffer size.
    *   `pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError>`
        *   A convenience method that calls `monitor()` with a default capacity (`rzmq::socket::events::DEFAULT_MONITOR_CAPACITY`).

---

## Public Enums (Non-Config)

### `rzmq::SocketType`

Defines the ZeroMQ messaging pattern for a `Socket`.

*   **Variants:**
    *   `Pub`: Publisher for Pub-Sub pattern.
    *   `Sub`: Subscriber for Pub-Sub pattern.
    *   `Req`: Request client for Req-Rep pattern.
    *   `Rep`: Reply server for Req-Rep pattern.
    *   `Dealer`: Asynchronous request/reply client (often used with `Router`).
    *   `Router`: Asynchronous request/reply server, routes messages based on identity.
    *   `Push`: Fan-out message distributor for pipeline pattern.
    *   `Pull`: Fan-in message collector for pipeline pattern.

### `rzmq::MsgFlags` (Bitflags)

Flags associated with a `Msg` that modify its behavior or indicate its role. Implemented using the `bitflags` crate.

*   **Flags:**
    *   `MORE`: `0b01` - Indicates that more message parts follow this one in a multi-part message.
    *   *(Internal: `COMMAND` = `0b10` - Indicates a ZMTP command frame, not typically set by users.)*

### `rzmq::socket::events::SocketEvent`

Events emitted by a socket's monitor channel, detailing lifecycle and connection state changes.

*   **Variants (non-exhaustive):**
    *   `Listening { endpoint: String }`: Socket has started listening.
    *   `BindFailed { endpoint: String, error_msg: String }`: Socket failed to bind.
    *   `Accepted { endpoint: String, peer_addr: String }`: Accepted a new connection.
    *   `AcceptFailed { endpoint: String, error_msg: String }`: Failed to accept a connection.
    *   `Connected { endpoint: String, peer_addr: String }`: Connection established (transport layer).
    *   `ConnectDelayed { endpoint: String, error_msg: String }`: Initial connection failed, retrying will start.
    *   `ConnectRetried { endpoint: String, interval: Duration }`: Retrying connection after delay.
    *   `ConnectFailed { endpoint: String, error_msg: String }`: Connection attempt failed definitively.
    *   `Closed { endpoint: String }`: Connection or listener closed.
    *   `Disconnected { endpoint: String }`: Peer disconnected or connection terminated.
    *   `HandshakeFailed { endpoint: String, error_msg: String }`: ZMTP handshake (including security) failed.
    *   `HandshakeSucceeded { endpoint: String }`: ZMTP handshake succeeded.

---

## Public Structs (Data Carrying)

### `rzmq::Msg`

Represents a single message part (frame) in ZeroMQ communication. It is designed for efficient handling of byte data.

*   **Key Public Methods:**
    *   `pub fn new() -> Self`
        *   Creates a new, empty message with no data.
    *   `pub fn from_vec(data: Vec<u8>) -> Self`
        *   Creates a message from a `Vec<u8>`, taking ownership of the data.
    *   `pub fn from_bytes(data: bytes::Bytes) -> Self`
        *   Creates a message from `bytes::Bytes`.
    *   `pub fn from_static(data: &'static [u8]) -> Self`
        *   Creates a message from a static byte slice (zero-copy).
    *   `pub fn data(&self) -> Option<&[u8]>`
        *   Returns an optional slice `&[u8]` representing the message payload.
    *   `pub fn size(&self) -> usize`
        *   Returns the size of the message payload in bytes.
    *   `pub fn flags(&self) -> MsgFlags`
        *   Returns the `MsgFlags` associated with the message.
    *   `pub fn set_flags(&mut self, flags: MsgFlags)`
        *   Sets the `MsgFlags` for the message (e.g., to indicate `MsgFlags::MORE`).
    *   `pub fn is_more(&self) -> bool`
        *   Checks if the `MsgFlags::MORE` flag is set.
    *   `pub fn metadata(&self) -> &Metadata`
        *   (Advanced) Returns an immutable reference to the message's `Metadata` map.
    *   `pub fn metadata_mut(&mut self) -> &mut Metadata`
        *   (Advanced) Returns a mutable reference to the message's `Metadata` map.

### `rzmq::Blob`

An immutable, cheaply cloneable byte sequence, typically used for identities in `ROUTER`/`DEALER` sockets or for subscription topics in `PUB`/`SUB`. Internally backed by `bytes::Bytes`.

*   **Key Public Methods & Conversions:**
    *   `pub fn new() -> Self` (Creates an empty blob)
    *   `pub fn from_bytes(bytes: bytes::Bytes) -> Self`
    *   `pub fn from_static(data: &'static [u8]) -> Self`
    *   `pub fn size(&self) -> usize`
    *   `pub fn is_empty(&self) -> bool`
    *   Implements `Deref<Target = [u8]>`
    *   Implements `AsRef<[u8]>`
    *   Implements `From<Vec<u8>>`
    *   Implements `From<&'static [u8]>`

### `rzmq::message::Metadata`

A type map for associating arbitrary typed data with a `Msg`. Rarely directly used by application code unless interacting with advanced ZMTP properties or custom message metadata.

*   **Key Public Methods (async):**
    *   `pub async fn insert_typed<T: Any + Send + Sync>(&self, value: T) -> Option<Arc<dyn Any + Send + Sync>>`
    *   `pub async fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>>`
    *   `pub async fn contains<T: Any + Send + Sync>(&self) -> bool`
    *   `pub async fn remove<T: Any + Send + Sync>(&self) -> Option<Arc<dyn Any + Send + Sync>>`
    *   *(Other methods: `new`, `is_empty`, `len`)*

---

## Public Functions (Free-standing)

Located in the `rzmq` crate root.

*   `pub fn version() -> (i32, i32, i32)`
    *   Returns the library version as a tuple: `(major, minor, patch)`.
*   `pub fn version_major() -> i32`
*   `pub fn version_minor() -> i32`
*   `pub fn version_patch() -> i32`

---

## Public Type Aliases

*   **`rzmq::socket::events::MonitorReceiver`**:
    ```rust
    pub type MonitorReceiver = async_channel::Receiver<SocketEvent>;
    ```
    The receiving end of a channel used for socket monitoring events.

---

## Error Handling

### `rzmq::ZmqError` (Enum)

The primary error type used by `rzmq`. It is `Clone`-able.

*   **Variants (non-exhaustive):**
    *   `IoError { kind: std::io::ErrorKind, message: String }`: Wraps an underlying I/O error.
    *   `InvalidArgument(String)`: Invalid argument provided (not socket option related).
    *   `Timeout`: Operation timed out.
    *   `AddrInUse(String)`: Address already in use (e.g., for `bind`).
    *   `AddrNotAvailable(String)`: Address not available.
    *   `ConnectionRefused(String)`: Connection refused by peer.
    *   `HostUnreachable(String)`: Destination host is unreachable.
    *   `NetworkUnreachable(String)`: Network is unreachable.
    *   `ConnectionClosed`: Connection closed by peer or transport.
    *   `PermissionDenied(String)`: Permission denied for an endpoint operation.
    *   `InvalidEndpoint(String)`: Endpoint string has an invalid format.
    *   `EndpointResolutionFailed(String)`: Failed to resolve endpoint (e.g., DNS).
    *   `InvalidOption(i32)`: Invalid socket option ID.
    *   `InvalidOptionValue(i32)`: Invalid value for a socket option.
    *   `InvalidSocketType(&'static str)`: Operation invalid for socket type.
    *   `InvalidState(&'static str)`: Operation invalid for current socket state (e.g., REQ send twice).
    *   `ProtocolViolation(String)`: ZMTP protocol violation detected.
    *   `InvalidMessage(String)`: Invalid message format for the operation.
    *   `SecurityError(String)`: Generic security mechanism error.
    *   `AuthenticationFailure(String)`: Authentication with peer failed.
    *   `EncryptionError(String)`: Error during message encryption/decryption.
    *   `ResourceLimitReached`: Resource limit hit (e.g., HWM, equivalent to EAGAIN).
    *   `UnsupportedTransport(String)`: Transport scheme not supported.
    *   `UnsupportedOption(i32)`: Socket option not supported.
    *   `UnsupportedFeature(&'static str)`: Feature not supported.
    *   `Internal(String)`: Internal library error.
    *   `#[cfg(feature = "io-uring")] UringError(String)`: Error specific to `io_uring` operations.

### `rzmq::ZmqResult<T, E = ZmqError>` (Type Alias)

The standard `Result` type returned by most fallible operations in `rzmq`.

```rust
pub type ZmqResult<T, E = ZmqError> = std::result::Result<T, E>;
```
*(This alias is typically defined in `rzmq::error` but used throughout as `rzmq::ZmqResult` or just `Result<_, ZmqError>`).*