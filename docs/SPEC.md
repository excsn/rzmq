**rzmq Library Specification**

**Version:** 1.0

**Status:** Draft

**1. Overview**

`rzmq` is a pure-Rust, asynchronous messaging library providing implementations of core ZeroMQ messaging patterns and socket types. It leverages the Tokio runtime and is built upon the Actor Model to achieve high performance, concurrency, and safety. The library aims to provide a familiar API experience for ZeroMQ users while fully embracing Rust's asynchronous ecosystem and offering opportunities for high-performance transport optimizations.

**1.1. Goals**

*   Provide a high-quality, asynchronous implementation of common ZeroMQ socket types and patterns (PUB/SUB, REQ/REP, PUSH/PULL, DEALER/ROUTER).
*   Offer a safe and idiomatic Rust API leveraging `async`/`.await` and `Result`.
*   Achieve high performance suitable for demanding messaging applications, with specific optimizations for TCP transport on supported platforms (e.g., via `io_uring`).
*   Support standard ZeroMQ transports (TCP, IPC, inproc) using the ZMTP/3.1 protocol.
*   Implement standard ZeroMQ security mechanisms (NULL, PLAIN, CURVE) via ZAP.
*   Maintain a modular architecture allowing for maintainability and potential future extensions.
*   Prioritize correctness, robustness, and clear error handling.

**1.2. Core Principles**

*   **Actor Model:** Core components (`Context`, `Socket`, `Session`, `Engine`, `Listener`, `Connecter`) are implemented as independent asynchronous Tokio tasks (actors).
*   **Message Passing:** Actors communicate primarily via type-safe, bounded asynchronous message queues (`tokio::sync::mpsc`, referred to as Mailboxes) using a well-defined `Command` enum. This ensures decoupling and manages backpressure.
*   **Tokio Integration:** Deeply integrated with the Tokio runtime for task scheduling, asynchronous I/O (TCP, IPC, potentially `io_uring`), timers, and synchronization primitives.
*   **Asynchronous Everywhere:** All potentially blocking operations (I/O, waiting for messages, handling options) are exposed via an `async` API. Internal operations use non-blocking I/O and await points.
*   **Layered Architecture:** Clear separation of concerns:
    *   **API Layer (`Socket`):** User-facing handle.
    *   **Socket Core Layer (`SocketCore`):** Central actor logic, state management, connection lifecycle.
    *   **Pattern Layer (`ISocket` Impls):** Socket-type specific routing/distribution logic.
    *   **Session Layer (`SessionBase`):** Mediation between Socket and a single connection's Engine. Manages ZAP.
    *   **Engine Layer (`IEngine` Impls):** Protocol (ZMTP/3.1) handling and transport I/O management. TCP Engine may leverage advanced features like `io_uring` when available and enabled.
    *   **Transport Layer (`Listener`/`Connecter`):** Connection establishment logic.
*   **Lifecycle Management:** Actor lifecycles are explicitly managed via `Stop` commands propagated down the hierarchy and `CleanupComplete`/`ReportError` messages propagated up. `JoinHandle`s are used for task tracking.
*   **Resource Management:** Careful handling of sockets, file descriptors, and buffer pools. Graceful shutdown is prioritized.
*   **Performance Optimization:** While maintaining correctness, the implementation should strive for performance, including minimizing data copies (`bytes::Bytes`) and optionally leveraging advanced OS features (`io_uring` via feature flag) for TCP transport.

**2. Public API**

This section defines the stable interface exposed to users of the `rzmq` library.

**2.1. Top-Level Functions**

*   `rzmq::context() -> Result<Context, ZmqError>`
    *   Creates and returns a new `Context`. This is the standard entry point for using the library. Each `Context` represents an independent container for sockets.
*   `rzmq::version() -> (i32, i32, i32)`
    *   Returns the library version tuple (major, minor, patch).
*   `rzmq::version_major() -> i32`
    *   Returns the major version number.
*   `rzmq::version_minor() -> i32`
    *   Returns the minor version number.
*   `rzmq::version_patch() -> i32`
    *   Returns the patch version number.

**2.2. Primary Types**

*   **`rzmq::Context`**
    *   **Role:** Manages shared resources within its scope (e.g., the inproc transport registry), tracks active sockets created from it, and acts as a factory for `Socket` instances.
    *   **Lifecycle:** Created via `Context::new()`. Shutdown of associated sockets is initiated via `shutdown()` or `term()`.
    *   **Cloning:** Cheaply cloneable (`Arc<ContextInner>` internally). Clones share the same underlying context state. Essential for sharing context across tasks/threads.
    *   **Methods:**
        *   `Context::new() -> Result<Self, ZmqError>`: Creates a new, independent context.
        *   `socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>`: Creates a new `Socket` actor of the specified type, associates it with this context, and returns the public `Socket` handle.
        *   `shutdown(&self) -> Result<(), ZmqError>`: Asynchronously sends a `Stop` command to all sockets currently managed by this context. Returns immediately.
        *   `term(self) -> Result<(), ZmqError>`: Initiates shutdown by calling `shutdown()`, then waits (asynchronously if in an async context) for all associated sockets to confirm clean termination before returning. Consumes the `Context` handle.

*   **`rzmq::Socket`** (Opaque struct wrapping `Arc<dyn ISocket>`)
    *   **Role:** The primary user handle for interacting with a specific ZeroMQ socket. Provides the public asynchronous API. Represents a handle to an underlying actor.
    *   **Lifecycle:** Created via `Context::socket()`. Communication begins after `bind()` or `connect()`. Graceful closure via `close()` or dropping the last clone.
    *   **Cloning:** Cheaply cloneable (`Arc` internally).
    *   **Methods (all `async`):**
        *   `bind(&self, endpoint: &str) -> Result<(), ZmqError>`: Request the socket to listen on the `endpoint`.
        *   `connect(&self, endpoint: &str) -> Result<(), ZmqError>`: Request the socket to connect to the `endpoint`.
        *   `disconnect(&self, endpoint: &str) -> Result<(), ZmqError>`: Request disconnection from a connected `endpoint`.
        *   `unbind(&self, endpoint: &str) -> Result<(), ZmqError>`: Request cessation of listening on a bound `endpoint`.
        *   `send(&self, msg: Msg) -> Result<(), ZmqError>`: Send a message, respecting pattern and HWM flow control. Use `MsgFlags::MORE` for multi-part messages.
        *   `recv(&self) -> Result<Msg, ZmqError>`: Receive a message, respecting pattern. Awaits message or error/timeout.
        *   `set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>`: Set a socket option asynchronously.
        *   `get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>`: Get a socket option value asynchronously.
        *   `close(&self) -> Result<(), ZmqError>`: Initiate graceful socket shutdown asynchronously.

*   **`rzmq::Msg`**
    *   **Role:** Represents a single message frame.
    *   **Data:** Holds payload using `bytes::Bytes`.
    *   **Lifecycle:** Created via constructors, consumed by `send`, produced by `recv`.
    *   **Cloning:** Cheap clone (shared buffer).
    *   **Methods:** Constructors (`new`, `from_vec`, `from_bytes`, `from_static`), Accessors (`data`, `size`, `flags`, `metadata`), Modifiers (`set_flags`, `metadata_mut`), Helpers (`is_more`).

*   **`rzmq::MsgFlags`** (Bitflags struct)
    *   **Role:** Flags associated with a `Msg`.
    *   **Values:** `MORE`, `COMMAND` (internal ZMTP use).

*   **`rzmq::Metadata`** (Type Map: `TypeId -> Arc<dyn Any + Send + Sync>`)
    *   **Role:** Container for arbitrary typed out-of-band data with a `Msg`.
    *   **Methods:** `insert_typed`, `get`, `contains`, `remove`.

**2.3. Constants**

*   **Socket Types (`rzmq::SocketType`, `rzmq::*`)**: Enum `SocketType` (`Pub`, `Sub`, etc.) and integer constants (`rzmq::PUB`, `rzmq::SUB`, etc.).
*   **Socket Options (`rzmq::*`)**: Integer constants mirroring ZMQ C API (HWM, LINGER, RCVTIMEO, SUBSCRIBE, ROUTING_ID, TCP_KEEPALIVE, ZAP_DOMAIN, CURVE_*, PLAIN_*, etc.). May include constants for enabling/disabling transport optimizations (e.g., `R ZMQ_TCP_CORK`, `R ZMQ_TCP_ZEROCOPY_SEND`).

**2.4. Error Type**

*   **`rzmq::ZmqError`** (Enum implementing `std::error::Error`, using `thiserror`)
    *   **Role:** Unified error type for public API and internal propagation.
    *   **Variants:** Comprehensive coverage including: `Io`, `Timeout`, `AddrInUse`, `ConnectionRefused`, `HostUnreachable`, `ConnectionClosed`, `PermissionDenied`, `InvalidEndpoint`, `EndpointNotAvailable`, `InvalidOption`, `InvalidOptionValue`, `InvalidState`, `ProtocolViolation`, `AuthenticationFailure`, `EncryptionError`, `ResourceLimitReached` (HWM/EAGAIN), `UnsupportedTransport`, `UnsupportedOption`, `Internal`, `MailboxSendError`, `TaskJoinError`, `UringError` (if `io-uring` feature enabled).

**3. Internal Architecture**

**3.1. Actor Model Overview**

*   Components (`SocketCore`, `SessionBase`, `Listener`, `Connecter`, `Engine`) are Tokio tasks.
*   Communication via `Command` enum over bounded MPSC mailboxes.
*   Actors use `tokio::select!` for concurrent handling of commands and I/O.

**3.2. `Command` Enum (Illustrative Key Variants)**

*   As specified previously: User requests with `reply_tx`, Lifecycle signals, Connection management results, Session/Engine interactions, Pipe notifications.

**3.3. Component Roles & Interactions**

*   **`ContextInner`:** Holds shared state (`inproc_registry`, `sockets` map), manages socket lifecycle tracking for `term()`.
*   **`SocketCore` (Actor):** Central coordinator. Manages `CoreState` (pipes, connection map `String -> EndpointInfo`). Handles `User*` commands, spawns Listeners/Connecters, manages Sessions, handles child lifecycle events, delegates pattern logic to `ISocket`. Passes configuration down.
*   **`ISocket` (Trait):** Implemented by `PubSocket`, etc. Embeds `SocketCore`. Holds pattern state/helpers. Implements `process_command` for pattern logic and `handle_pipe_event`.
*   **`Listener`/`Connecter` (Actors):** Transport-specific (TCP, IPC) connection setup using `tokio::net`. On success, create `SessionBase` and `IEngine` (passing connected stream and config), `Attach` Engine to Session, report `ConnSuccess` to `SocketCore`.
*   **`SessionBase` (Actor):** Manages one connection. Owns `Pipe` pair to `SocketCore`. Relays messages between Pipe and Engine mailbox. Manages ZAP client interactions. Forwards `Stop` to Engine. Propagates Engine status/errors to `SocketCore`.
*   **`IEngine` (Trait / Actor):** Implementations (`ZmtpTcpEngine`, `ZmtpIpcEngine`). Owns transport stream (`TcpStream`/`UnixStream` or potentially `tokio_uring::net::TcpStream`). Handles ZMTP/3.1 framing/codec. Drives security `Mechanism`. *Optionally* uses `io_uring` for TCP ops if feature enabled and kernel supports. Handles `Stop`. Reports status/errors to Session.
*   **`Pipe`:** Bounded async `Msg` queue between Session and SocketCore. Notifies consumer actor of state changes.

**3.4. Lifecycle Management Details**

*   **Startup Sequence:** `Context::socket` -> `SocketCore` -> `bind/connect` -> `Listener/Connecter` -> `Session` -> `Engine` (with stream) -> Handshake -> `EngineReady` -> `ConnSuccess`.
*   **Shutdown Sequence:** `Stop` command cascade (Socket -> Children -> Engine). Resource cleanup, task joining, `CleanupComplete`/`ReportError` propagation upwards. Linger handled by `SocketCore`.

**4. Socket Types & Patterns**

*   Implement standard PUB/SUB, REQ/REP, DEALER/ROUTER, PUSH/PULL patterns with correct message handling semantics.
*   Use internal helper modules (`distributor`, `fair_queue`, `load_balancer`, `router`, `trie`) for pattern logic.

**5. Transport Protocols**

*   **Core Transports:**
    *   `tcp://<host>:<port>`: Uses `tokio::net::TcpStream` potentially enhanced with `tokio-uring` if `io-uring` feature is enabled.
    *   `ipc://<path>`: Uses `tokio::net::UnixStream`. Requires `ipc` feature.
    *   `inproc://<name>`: Uses internal async queues/pipes. Requires `inproc` feature.
*   **Protocol:** ZMTP/3.1 over TCP and IPC.
*   **Optimization (`io-uring` Feature):** When the `io-uring` feature is enabled, the `ZmtpTcpEngine` SHOULD attempt to use `tokio-uring` internally. It MUST perform runtime checks for kernel support for specific optimizations (e.g., zero-copy send, multishot receive) and utilize them only if configured, enabled via feature, and supported by the kernel. It MUST gracefully fall back to standard `tokio::net::TcpStream` operations if the feature is disabled, `tokio-uring` is unavailable/fails init, or specific kernel features are unsupported.

**6. Security Mechanisms**

*   Handled during `IEngine` handshake phase.
*   **NULL:** Default, no security.
*   **PLAIN:** ZAP-based username/password authentication.
*   **CURVE:** Curve25519 encryption and authentication, potentially using ZAP for server key verification. Requires `curve` feature and crypto backend.
*   **ZAP:** Session acts as ZAP client, communicating with user-provided authenticator service.

**7. Error Handling Policies**

*   Fatal errors propagate upwards via `ReportError`.
*   Actors receiving fatal errors initiate their own shutdown.
*   `ZmqError` distinguishes recoverable vs. fatal errors. Use `tracing` for logging.

**8. Configuration & Options**

*   Options set via `Socket::set_option`. Routed to appropriate component (SocketCore, Session, Engine, ISocket). Applied asynchronously. Includes standard ZMQ options and potentially flags to control transport optimizations (e.g., enable/disable corking, zerocopy attempts if `io-uring` feature is active).

**9. Concurrency Model**

*   Based on Tokio multi-threaded scheduler. Non-blocking actor tasks.
*   Use `tokio::task::spawn_blocking` for any unavoidable synchronous blocking code.
*   If `io-uring` feature is used, ensure tasks interacting with `tokio-uring` primitives run in the appropriate `tokio-uring` context if required by the library.

**10. Example Usage (REQ/REP)**

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    // Create a context
    let ctx = Context::new()?;

    // Spawn the REP server task
    let server_handle = {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let responder = ctx.socket(SocketType::Rep)?;
            // Example: Enable TCP optimizations if available
            // responder.set_option(rzmq::TCP_CORK, b"1").await?; // Fictional option constant
            responder.bind("tcp://127.0.0.1:5555").await?;
            println!("REP: Bound");

            let request_msg = responder.recv().await?;
            println!("REP: Received: {}", String::from_utf8_lossy(request_msg.data().unwrap_or(b"")));
            sleep(Duration::from_millis(50)).await;
            responder.send(Msg::from_static(b"World")).await?;
            println!("REP: Sent reply");

            responder.close().await?;
            Ok::<(), ZmqError>(())
        })
    };

    sleep(Duration::from_millis(100)).await; // Allow server bind

    // Create and run the REQ client
    let requester = ctx.socket(SocketType::Req)?;
    requester.connect("tcp://127.0.0.1:5555").await?;
    println!("REQ: Connected");

    requester.send(Msg::from_static(b"Hello")).await?;
    println!("REQ: Sent request");
    let reply_msg = requester.recv().await?;
    println!("REQ: Received: {}", String::from_utf8_lossy(reply_msg.data().unwrap_or(b"")));

    requester.close().await?;
    println!("REQ: Closed");

    server_handle.await.map_err(|e| ZmqError::Internal(format!("Server task error: {}", e)))??;
    println!("Server task finished.");

    // Optional: Explicit context termination
    // ctx.term()?;

    Ok(())
}
```