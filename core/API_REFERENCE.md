# rzmq API Reference

This document provides an API reference for the `rzmq` (core) library.

## 1. Introduction / Core Concepts

The `rzmq` library provides an asynchronous, pure-Rust implementation of ZeroMQ messaging patterns. Key concepts and types include:

*   **`Context`**: The central object for managing sockets and shared resources within an `rzmq` application. All sockets are created from a `Context`. It also manages the lifecycle of internal actors and the `io_uring` backend if enabled.
*   **`Socket`**: The public handle for interacting with a ZeroMQ socket. It provides methods for binding, connecting, sending/receiving messages, and configuring options. `Socket` instances are cloneable.
*   **`SocketType`**: An enum defining the ZeroMQ messaging pattern for a `Socket` (e.g., `REQ`, `REP`, `PUB`, `SUB`, `DEALER`, `ROUTER`, `PUSH`, `PULL`).
*   **`Msg`**: Represents a single message part (or frame) in ZeroMQ. It can hold arbitrary byte data and has associated flags (e.g., `MsgFlags::MORE`).
*   **`ZmqError`**: The primary error type used throughout the library for operations that can fail.
*   **`ZmqResult<T>`**: A type alias for `std::result::Result<T, ZmqError>`, used as the return type for most fallible operations.
*   **Asynchronous Operations**: All network I/O and potentially blocking operations are asynchronous, designed to be used within a Tokio runtime.
*   **Actor Model**: Internally, `rzmq` uses an actor-based architecture. A `SocketCore` actor manages the state and logic for each `Socket`. Users interact with the `Socket` handle, which communicates with its `SocketCore` actor.
*   **`io_uring` Backend (Linux-specific, optional feature)**: An advanced I/O backend that can be enabled for TCP transport to potentially achieve higher performance by leveraging Linux's `io_uring` interface. Its initialization and configuration are managed via the `rzmq::uring` module.

**Main Entry Points:**

*   Create a `rzmq::Context` using `Context::new()` or `Context::with_capacity()`.
*   Create `rzmq::Socket` instances from a `Context` using `Context::socket(SocketType)`.

**Pervasive Types:**

*   `rzmq::ZmqError`: The error type returned by most operations.
*   `rzmq::ZmqResult<T>`: The standard result type alias.
*   `rzmq::Msg`: The type used for sending and receiving message data.
*   `rzmq::SocketType`: Enum used to specify the messaging pattern when creating a socket.

### 1.1. Security Model

`rzmq` provides a pluggable security layer that operates transparently over TCP connections. When security is enabled on a socket, the library automatically performs a cryptographic handshake upon connection establishment before any application messages are sent or received.

*   **Mechanisms**: The library supports several standard ZeroMQ security mechanisms:
    *   **`NULL`**: The default behavior. No security is applied, and messages are sent in plaintext.
    *   **`PLAIN`**: A simple username/password authentication scheme. (Requires the `plain` feature).
    *   **`CURVE`**: A robust public-key encryption mechanism providing strong security. (Requires the `curve` feature).
    *   **`NOISE_XX`**: A modern and secure handshake protocol based on the Noise Protocol Framework. (Requires the `noise_xx` feature).

*   **Configuration**: Security is configured on a per-socket basis using `Socket::set_option`.
    *   To act as a server (e.g., a `REP`, `ROUTER`, or `PULL` socket that binds), you must enable the server role for the chosen mechanism (e.g., by setting `PLAIN_SERVER` or `CURVE_SERVER` to `true`) and provide its secret key.
    *   To act as a client, you provide the client's own keys and, for authentication, the public key of the server you intend to connect to (e.g., `CURVE_SERVER_KEY`).

*   **Lifecycle and Monitoring**: The success or failure of the security handshake can be observed using the socket monitoring feature.
    *   Upon successful completion of the handshake, a `SocketEvent::HandshakeSucceeded` event is emitted. Only after this event can application data be securely exchanged.
    *   If the handshake fails for any reason (e.g., key mismatch, authentication failure), a `SocketEvent::HandshakeFailed` event is emitted, and the connection is terminated.
## 2. Configuration

### `rzmq::uring::UringConfig` Struct

Configuration for the global `io_uring` backend. This is used when calling `rzmq::uring::initialize_uring_backend()`. This struct is only available when the `io-uring` feature is enabled.

*   **Public Fields**:
    *   `ring_entries: u32`
        *   The number of submission queue entries for the `io_uring` instance.
    *   `default_send_zerocopy: bool`
        *   Global flag to enable or disable the *attempt* to use zero-copy send operations by the `UringWorker`. A send buffer pool is allocated if this is true.
    *   `default_recv_multishot: bool`
        *   Global flag to enable or disable the creation of the default `io_uring` provided buffer ring (group ID 0) at worker startup, primarily for multishot receive operations.
    *   `default_recv_buffer_count: usize`
        *   The number of buffers to provision in the default receive buffer ring.
    *   `default_recv_buffer_size: usize`
        *   The size (in bytes) of each buffer in the default receive buffer ring.
    *   `default_send_buffer_count: usize`
        *   The number of buffers to provision in the global send buffer pool.
    *   `default_send_buffer_size: usize`
        *   The size (in bytes) of each buffer in the global send buffer pool.

*   **Default Values** (via `UringConfig::default()`):
    *   `ring_entries: 256`
    *   `default_send_zerocopy: false`
    *   `default_recv_multishot: true`
    *   `default_recv_buffer_count: rzmq::uring::DEFAULT_IO_URING_RECV_BUFFER_COUNT` (16)
    *   `default_recv_buffer_size: rzmq::uring::DEFAULT_IO_URING_RECV_BUFFER_SIZE` (65536)
    *   `default_send_buffer_count: rzmq::uring::DEFAULT_IO_URING_SND_BUFFER_COUNT` (16)
    *   `default_send_buffer_size: rzmq::uring::DEFAULT_IO_URING_SND_BUFFER_SIZE` (65536)

## 3. Main Types and Their Public Methods

### `rzmq::Context` Struct

The `Context` is the entry point for creating `rzmq` sockets. It manages shared resources and the lifecycle of socket actors. `Context` handles are cloneable.

*   **Constructors**:
    *   `pub fn new() -> Result<Self, ZmqError>`
        *   Creates a new, independent context with default internal actor mailbox capacities.
    *   `pub fn with_capacity(actor_mailbox_capacity: Option<usize>) -> Result<Self, ZmqError>`
        *   Creates a new, independent context, allowing specification of the bounded capacity for internal actor command mailboxes. If `None`, `rzmq::runtime::mailbox::DEFAULT_MAILBOX_CAPACITY` is used. Minimum capacity is 1.

*   **Methods**:
    *   `pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>`
        *   Creates a socket of the specified `SocketType` associated with this context.
    *   `pub async fn shutdown(&self) -> Result<(), ZmqError>`
        *   Initiates background shutdown of all sockets and actors created by this context. This is a non-blocking initiation; actual termination happens asynchronously.
    *   `pub async fn term(&self) -> Result<(), ZmqError>`
        *   Shuts down all sockets and waits for their clean termination. This is the recommended way to ensure all resources are released gracefully before an application exits.

### `rzmq::Socket` Struct

The public handle for an `rzmq` socket. Provides methods for network operations, option management, and monitoring. `Socket` handles are cloneable.

*   **Methods**:
    *   `pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Binds the socket to listen on a local endpoint (e.g., "tcp://127.0.0.1:5555", "ipc:///tmp/mysock").
    *   `pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Connects the socket to a remote endpoint.
    *   `pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Disconnects from a specific endpoint previously connected via `connect()`.
    *   `pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>`
        *   Stops listening on a specific endpoint previously bound via `bind()`.
    *   `pub async fn send(&self, msg: Msg) -> Result<(), ZmqError>`
        *   Sends a single message part (`Msg`) according to the socket's pattern.
    *   `pub async fn recv(&self) -> Result<Msg, ZmqError>`
        *   Receives a single message part (`Msg`) according to the socket's pattern.
    *   `pub async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError>`
        *   Sends a sequence of message frames atomically as one logical message. The implementation will automatically set `MsgFlags::MORE` on all but the last frame.
    *   `pub async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError>`
        *   Receives all frames of a complete logical ZMQ message.
    *   `pub async fn set_option<T: ToBytes>(&self, option: i32, value: T) -> Result<(), ZmqError>`
        *   Sets a socket option. The `value` type must implement the `rzmq::socket::ToBytes` trait.
    *   `pub async fn set_option_raw(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>`
        *   Sets a socket option using a raw byte slice for the value.
    *   `pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>`
        *   Gets a socket option value as a `Vec<u8>`.
    *   `pub async fn close(&self) -> Result<(), ZmqError>`
        *   Initiates a graceful shutdown of the socket.
    *   `pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError>`
        *   Creates a monitoring channel for this socket with the specified event capacity.
    *   `pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError>`
        *   Creates a monitoring channel with default capacity (`rzmq::socket::DEFAULT_MONITOR_CAPACITY`).

### `rzmq::Msg` Struct

Represents a single message part (frame) in ZeroMQ.

*   **Constructors**:
    *   `pub fn new() -> Self`
        *   Creates an empty message.
    *   `pub fn from_vec(data: Vec<u8>) -> Self`
        *   Creates a message from a `Vec<u8>`, taking ownership.
    *   `pub fn from_bytes(data: bytes::Bytes) -> Self`
        *   Creates a message from `bytes::Bytes`.
    *   `pub fn from_static(data: &'static [u8]) -> Self`
        *   Creates a message from a static byte slice (zero-copy).

*   **Methods**:
    *   `pub fn data(&self) -> Option<&[u8]>`
        *   Returns a reference to the message payload bytes, if any.
    *   `pub fn size(&self) -> usize`
        *   Returns the size of the message payload in bytes.
    *   `pub fn flags(&self) -> MsgFlags`
        *   Returns the flags associated with the message.
    *   `pub fn set_flags(&mut self, flags: MsgFlags)`
        *   Sets the flags for the message.
    *   `pub fn metadata(&self) -> &Metadata`
        *   Returns an immutable reference to the message metadata map.
    *   `pub fn metadata_mut(&mut self) -> &mut Metadata`
        *   Returns a mutable reference to the message metadata map. (Operations on `Metadata` are async).
    *   `pub fn is_more(&self) -> bool`
        *   Checks if the `MsgFlags::MORE` flag is set.
    *   `pub fn is_command(&self) -> bool`
        *   Checks if the `MsgFlags::COMMAND` flag is set (internal ZMTP command).
    *   `pub fn data_bytes(&self) -> Option<bytes::Bytes>`
        *   Returns the internal `bytes::Bytes` object if data is present, useful for cheap cloning.

### `rzmq::Blob` Struct

An immutable, cheaply cloneable byte sequence, often used for identities or subscription topics. Implements `Deref<Target=[u8]>` and `AsRef<[u8]>`.

*   **Constructors**:
    *   `pub fn new() -> Self`
    *   `pub fn from_bytes(bytes: bytes::Bytes) -> Self`
    *   `pub fn from_static(data: &'static [u8]) -> Self`

*   **Methods**:
    *   `pub fn size(&self) -> usize`
    *   `pub fn is_empty(&self) -> bool`

*   **Conversions**:
    *   `impl From<Vec<u8>> for Blob`
    *   `impl From<&'static [u8]> for Blob`

### `rzmq::Metadata` Struct

A type map for associating arbitrary typed data with a `Msg`. Operations are asynchronous due to internal locking.

*   **Constructors**:
    *   `pub fn new() -> Self`

*   **Methods**:
    *   `pub async fn insert_typed<T: Any + Send + Sync>(&self, value: T) -> Option<Arc<dyn Any + Send + Sync>>`
    *   `pub async fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>>`
    *   `pub async fn contains<T: Any + Send + Sync>(&self) -> bool`
    *   `pub async fn remove<T: Any + Send + Sync>(&self) -> Option<Arc<dyn Any + Send + Sync>>`
    *   `pub async fn is_empty(&self) -> bool`
    *   `pub async fn len(&self) -> usize`

## 4. Public Traits and Their Methods

### `rzmq::socket::ToBytes` Trait

A utility trait for converting various types into a `Vec<u8>`, used by `Socket::set_option`.

*   **Methods**:
    *   `fn to_bytes(&self) -> Vec<u8>`
        *   Converts the instance into a byte vector.

*   **Implementors (within `rzmq`)**:
    *   `Vec<u8>`
    *   `&[u8]`
    *   `&[u8; N]` (for any const `N`)
    *   `i32`
    *   `u32`
    *   `bool` (converted to `1i32` or `0i32` then to bytes)
    *   `String`
    *   `&str`

## 5. Public Enums (Non-Config)

### `rzmq::SocketType` Enum

Defines the ZeroMQ messaging pattern for a `Socket`.

*   **Variants**:
    *   `Pub`
    *   `Sub`
    *   `Req`
    *   `Rep`
    *   `Dealer`
    *   `Router`
    *   `Push`
    *   `Pull`

### `rzmq::MsgFlags` Bitflags Enum

Flags associated with an `rzmq::Msg`.

*   **Flags**:
    *   `MORE = 0b01`: More message parts follow this one.
    *   `COMMAND = 0b10`: Internal: Indicates a ZMTP command frame.

### `rzmq::socket::SocketEvent` Enum

Represents significant events occurring within a socket or its connections, used for socket monitoring.

*   **Variants**:
    *   `Listening { endpoint: String }`
    *   `BindFailed { endpoint: String, error_msg: String }`
    *   `Accepted { endpoint: String, peer_addr: String }`
    *   `AcceptFailed { endpoint: String, error_msg: String }`
    *   `Connected { endpoint: String, peer_addr: String }`
    *   `ConnectDelayed { endpoint: String, error_msg: String }`
    *   `ConnectRetried { endpoint: String, interval: Duration }`
    *   `ConnectFailed { endpoint: String, error_msg: String }`
    *   `Closed { endpoint: String }`
    *   `Disconnected { endpoint: String }`
    *   `HandshakeFailed { endpoint: String, error_msg: String }`
    *   `HandshakeSucceeded { endpoint: String }`

## 6. Public Functions (Free-standing)

### In `rzmq::uring` (only if `io-uring` feature is enabled):

*   `pub fn initialize_uring_backend(config: UringConfig) -> Result<(), ZmqError>`
    *   Initializes the global `io_uring` backend with the provided configuration. Must be called once before any `io_uring`-based socket operations if custom configuration is desired, or it will be auto-initialized with defaults.
*   `pub async fn shutdown_uring_backend() -> Result<(), ZmqError>`
    *   Shuts down the global `io_uring` backend, joining worker threads and cleaning up resources.

## 7. Public Type Aliases

### In `rzmq::error`:

*   `pub type ZmqResult<T, E = ZmqError> = std::result::Result<T, E>`
    *   The standard result type used for operations that can fail within the `rzmq` library.

### In `rzmq::socket::events`:

*   `pub type MonitorSender = fibre::mpmc::AsyncSender<SocketEvent>`
    *   The sending end of the channel used for socket monitor events.
*   `pub type MonitorReceiver = fibre::mpmc::AsyncReceiver<SocketEvent>`
    *   The receiving end of the channel used for socket monitor events.

## 8. Public Constants

### In `rzmq::socket::options`:

Constants for socket option integer IDs.
*   `pub const SNDHWM: i32 = 23`
*   `pub const RCVHWM: i32 = 24`
*   `pub const LINGER: i32 = 17`
*   `pub const SUBSCRIBE: i32 = 6`
*   `pub const UNSUBSCRIBE: i32 = 7`
*   `pub const ROUTING_ID: i32 = 5`
*   `pub const RECONNECT_IVL: i32 = 18`
*   `pub const RECONNECT_IVL_MAX: i32 = 21`
*   `pub const RCVTIMEO: i32 = 27`
*   `pub const SNDTIMEO: i32 = 28`
*   `pub const LAST_ENDPOINT: i32 = 32`
*   `pub const TCP_KEEPALIVE: i32 = 34`
*   `pub const TCP_KEEPALIVE_IDLE: i32 = 35`
*   `pub const TCP_KEEPALIVE_CNT: i32 = 36`
*   `pub const TCP_KEEPALIVE_INTVL: i32 = 37`
*   `pub const HEARTBEAT_IVL: i32 = 38`
*   `pub const HEARTBEAT_TIMEOUT: i32 = 39`
*   `pub const HEARTBEAT_TTL: i32 = 40`
*   `pub const HANDSHAKE_IVL: i32 = 41`
*   `pub const ROUTER_MANDATORY: i32 = 33`
*   `pub const ZAP_DOMAIN: i32 = 55`
*   `pub const PLAIN_SERVER: i32 = 44` (Requires `plain` feature)
*   `pub const PLAIN_USERNAME: i32 = 45` (Requires `plain` feature)
*   `pub const PLAIN_PASSWORD: i32 = 46` (Requires `plain` feature)
*   `pub const CURVE_SERVER: i32 = 47` (Requires `curve` feature)
*   `pub const CURVE_SECRET_KEY: i32 = 49` (Requires `curve` feature)
*   `pub const CURVE_SERVER_KEY: i32 = 48` (Requires `curve` feature)
*   `pub const NOISE_XX_ENABLED: i32 = 1202` (Requires `noise_xx` feature)
*   `pub const NOISE_XX_STATIC_SECRET_KEY: i32 = 1200` (Requires `noise_xx` feature)
*   `pub const NOISE_XX_REMOTE_STATIC_PUBLIC_KEY: i32 = 1201` (Requires `noise_xx` feature)
*   `pub const MAX_CONNECTIONS: i32 = 1000`
*   `pub const IO_URING_SNDZEROCOPY: i32 = 1170` (Requires `io-uring` feature)
*   `pub const IO_URING_RCVMULTISHOT: i32 = 1171` (Requires `io-uring` feature)
*   `pub const TCP_CORK: i32 = 1172` (Requires `io-uring` feature, Linux only)
*   `pub const IO_URING_SESSION_ENABLED: i32 = 1175` (Requires `io-uring` feature)

### In `rzmq::socket::events`:

*   `pub const DEFAULT_MONITOR_CAPACITY: usize = 100`

### In `rzmq::uring` (only if `io-uring` feature is enabled):

*   `pub const DEFAULT_IO_URING_SND_BUFFER_COUNT: usize = 16`
*   `pub const DEFAULT_IO_URING_SND_BUFFER_SIZE: usize = 65536`
*   `pub const DEFAULT_IO_URING_RECV_BUFFER_COUNT: usize = 16`
*   `pub const DEFAULT_IO_URING_RECV_BUFFER_SIZE: usize = 65536`
*   `pub static URING_BACKEND_INITIALIZED: AtomicBool` (A static atomic boolean, not a `const`)

## 9. Error Handling

### `rzmq::ZmqError` Enum

The primary error type for the `rzmq` library. It is `Clone`-able.

*   **Variants**:
    *   `IoError { kind: std::io::ErrorKind, message: String }`: Wraps an underlying I/O error.
    *   `InvalidArgument(String)`: Invalid argument provided.
    *   `Timeout`: Operation timed out.
    *   `AddrInUse(String)`: Address already in use.
    *   `AddrNotAvailable(String)`: Address not available.
    *   `ConnectionRefused(String)`: Connection refused by peer.
    *   `HostUnreachable(String)`: Host is unreachable.
    *   `NetworkUnreachable(String)`: Network is unreachable.
    *   `ConnectionClosed`: Connection closed by peer or transport.
    *   `PermissionDenied(String)`: Permission denied for endpoint operation.
    *   `InvalidEndpoint(String)`: Invalid endpoint format.
    *   `EndpointResolutionFailed(String)`: Endpoint resolution failed.
    *   `InvalidOption(i32)`: Invalid socket option ID.
    *   `InvalidOptionValue(i32)`: Invalid value for a socket option.
    *   `InvalidSocketType(&'static str)`: Operation invalid for socket type.
    *   `InvalidState(&'static str)`: Operation invalid for current socket state.
    *   `ProtocolViolation(String)`: ZMTP protocol violation.
    *   `InvalidMessage(String)`: Invalid message format for operation.
    *   `SecurityError(String)`: Generic security mechanism error.
    *   `AuthenticationFailure(String)`: Authentication failed.
    *   `EncryptionError(String)`: Encryption/decryption error.
    *   `ResourceLimitReached`: Resource limit reached (e.g., HWM).
    *   `UnsupportedTransport(String)`: Transport scheme not supported.
    *   `UnsupportedOption(i32)`: Socket option not supported.
    *   `UnsupportedFeature(&'static str)`: Feature not supported.
    *   `Internal(String)`: Generic internal library error.

### `rzmq::ZmqResult<T>` Type Alias

*   `pub type ZmqResult<T, E = ZmqError> = std::result::Result<T, E>;`
    *   Standard result type used across the library.