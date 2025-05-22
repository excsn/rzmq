# Usage Guide: rzmq

This guide provides a detailed overview of how to use the `rzmq` library, covering core concepts, API usage, configuration, and error handling for building asynchronous messaging applications in Rust.

## Table of Contents

*   [Introduction](#introduction)
*   [Core Concepts](#core-concepts)
*   [Quick Start](#quick-start)
    *   [Push/Pull Example](#pushpull-example)
    *   [Request/Reply Example](#requestreply-example)
*   [Main API Components](#main-api-components)
    *   [`rzmq::Context`](#rzmqcontext)
    *   [`rzmq::Socket`](#rzmqsocket)
    *   [`rzmq::Msg` and `rzmq::MsgFlags`](#rzmqmsg-and-rzmqmsgflags)
    *   [`rzmq::Blob`](#rzmqblob)
*   [Working with Sockets](#working-with-sockets)
    *   [Creating Sockets](#creating-sockets)
    *   [Binding and Connecting](#binding-and-connecting)
    *   [Sending and Receiving Messages](#sending-and-receiving-messages)
    *   [Multi-Part Messages](#multi-part-messages)
    *   [Socket Options](#socket-options)
    *   [Monitoring Socket Events](#monitoring-socket-events)
    *   [Closing Sockets and Context Termination](#closing-sockets-and-context-termination)
*   [Supported Socket Types](#supported-socket-types)
*   [Using `io_uring` (Linux Specific)](#using-io_uring-linux-specific)
*   [Error Handling](#error-handling)

## Introduction

This guide will help you understand the fundamental building blocks of `rzmq` and demonstrate how to use its API to construct various messaging patterns. `rzmq` leverages Tokio for its asynchronous operations, allowing for efficient, non-blocking communication.

## Core Concepts

Understanding these core concepts is key to effectively using `rzmq`:

*   **`Context`**: The starting point for all `rzmq` operations. It acts as a container for sockets and manages shared resources like thread pools (implicitly via Tokio) and global state (e.g., for in-process communication). A single `Context` can manage multiple sockets. `Context` handles are lightweight (`Arc`-based) and can be cloned.
*   **`Socket`**: Represents a ZeroMQ-style communication endpoint. Each socket has a specific `SocketType` that defines its messaging behavior (e.g., PUB/SUB, REQ/REP). Like `Context`, `Socket` handles are cloneable. All I/O operations on a `Socket` are asynchronous.
*   **`SocketType`**: An enum that determines the pattern of a socket. This is specified when creating a socket (e.g., `SocketType::Pub`, `SocketType::Req`). The chosen type dictates how the socket sends and receives messages.
*   **`Msg`**: The fundamental unit for data exchange. Messages can be single-part or multi-part. `rzmq` provides the `Msg` struct for creating and manipulating message frames.
*   **`MsgFlags`**: Associated with `Msg` instances, these flags control aspects of message handling. The most common is `MsgFlags::MORE`, used to indicate that more frames are part of the current logical message.
*   **Endpoints**: String identifiers for network addresses or communication channels. `rzmq` supports `tcp://host:port`, `ipc:///path/to/socket` (on Unix-like systems with the `ipc` feature), and `inproc://name` (for intra-process communication with the `inproc` feature).
*   **Asynchronous Model**: All potentially blocking operations (network I/O, waiting for messages) are `async` and need to be `.await`ed within a Tokio runtime.
*   **Socket Options**: Sockets can be configured with various options (e.g., high-water marks, timeouts, identity) using `Socket::set_option()`.
*   **Monitoring**: `rzmq` provides a mechanism to monitor socket events (like connection establishment, disconnections, etc.) through a channel obtained via `Socket::monitor()`.
*   **Graceful Shutdown**: It's crucial to call `Context::term().await` to ensure all sockets are properly closed, pending messages are handled according to `LINGER` options, and background tasks are terminated cleanly.

## Quick Start

These examples demonstrate basic usage. Ensure you have `rzmq` and `tokio` in your `Cargo.toml`.

### Push/Pull Example

Pushes messages to a load-balanced set of pullers.

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

// On Linux with "io-uring" feature for rzmq:
// #[cfg(all(target_os = "linux", feature = "io-uring"))]
// #[rzmq::main]
// async fn main() -> Result<(), ZmqError> { ... }

// Otherwise (or if io-uring feature is not used):
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?; // Use default capacities

    let push_socket = ctx.socket(SocketType::Push)?;
    let pull_socket = ctx.socket(SocketType::Pull)?;

    // For this example, use the "inproc" feature.
    // Ensure rzmq is added with `features = ["inproc"]` in Cargo.toml
    let endpoint = "inproc://qstart-pushpull";

    pull_socket.bind(endpoint).await?;
    push_socket.connect(endpoint).await?;

    // Allow time for connection
    tokio::time::sleep(Duration::from_millis(50)).await;

    let message_text = "Hello from PUSH socket!";
    push_socket.send(Msg::from_static(message_text.as_bytes())).await?;
    println!("PUSH: Sent message.");

    let received_msg = pull_socket.recv().await?;
    println!("PULL: Received: {}", String::from_utf8_lossy(received_msg.data().unwrap_or_default()));

    assert_eq!(received_msg.data().unwrap_or_default(), message_text.as_bytes());

    ctx.term().await?;
    Ok(())
}
```

### Request/Reply Example

A client sends a request and waits for a reply from a server.

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

// #[rzmq::main] // if io-uring
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::with_capacity(Some(128))?; // Example: specify mailbox capacity

    let req_socket = ctx.socket(SocketType::Req)?;
    let rep_socket = ctx.socket(SocketType::Rep)?;

    let endpoint = "tcp://127.0.0.1:5558"; // Use a unique port

    rep_socket.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow bind

    req_socket.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(100)).await; // Allow connect & handshake

    // REQ sends
    println!("REQ: Sending 'PING'");
    req_socket.send(Msg::from_static(b"PING")).await?;

    // REP receives
    let request = rep_socket.recv().await?;
    println!("REP: Received '{}'", String::from_utf8_lossy(request.data().unwrap_or_default()));
    assert_eq!(request.data().unwrap_or_default(), b"PING");

    // REP sends reply
    println!("REP: Sending 'PONG'");
    rep_socket.send(Msg::from_static(b"PONG")).await?;

    // REQ receives reply
    let reply = req_socket.recv().await?;
    println!("REQ: Received '{}'", String::from_utf8_lossy(reply.data().unwrap_or_default()));
    assert_eq!(reply.data().unwrap_or_default(), b"PONG");

    ctx.term().await?;
    Ok(())
}
```

## Main API Components

### `rzmq::Context`

Manages sockets and shared resources.

*   **`pub fn new() -> Result<Context, ZmqError>`**
    Creates a new, independent `rzmq` context with default internal capacities.
*   **`pub fn with_capacity(actor_mailbox_capacity: Option<usize>) -> Result<Context, ZmqError>`**
    Creates a new, independent `rzmq` context. `actor_mailbox_capacity` optionally specifies the bounded capacity for internal actor command mailboxes. If `None`, a default capacity is used. The minimum capacity used will be 1.
*   **`pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>`**
    Creates a new `Socket` of the specified `SocketType` associated with this context.
*   **`pub async fn shutdown(&self) -> Result<(), ZmqError>`**
    Initiates background shutdown of the context. Returns quickly.
*   **`pub async fn term(&self) -> Result<(), ZmqError>`**
    Initiates a graceful shutdown and waits for all background tasks to complete.

### `rzmq::Socket`

Handle for interacting with a socket.

*   **`pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>`**
    Binds the socket to a local endpoint (e.g., `"tcp://*:5555"`, `"ipc:///tmp/socket"`).
*   **`pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>`**
    Connects the socket to a remote endpoint.
*   **`pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>`**
    Disconnects from a specific connected endpoint.
*   **`pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>`**
    Stops listening on a bound endpoint.
*   **`pub async fn send(&self, msg: Msg) -> Result<(), ZmqError>`**
    Sends a message. For multi-part messages, use `MsgFlags::MORE` on all but the last part.
*   **`pub async fn recv(&self) -> Result<Msg, ZmqError>`**
    Receives a message. Check `msg.is_more()` for multi-part messages.
*   **`pub async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError>`**
    Sends a sequence of `Msg` frames as a single logical message. The library handles necessary delimiters (e.g., for DEALER/ROUTER). The caller should set `MsgFlags::MORE` appropriately on the input `frames` if the *application payload itself* is multi-part; the final frame of the application payload should not have `MORE`.
*   **`pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>`**
    Sets a socket option. `value` is typically `&some_i32.to_ne_bytes()` for integer options or `b"string_value"` for byte slice options.
*   **`pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>`**
    Gets a socket option value as raw bytes.
*   **`pub async fn close(&self) -> Result<(), ZmqError>`**
    Gracefully closes this specific socket.
*   **`pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError>`**
    Creates a channel for receiving `SocketEvent` notifications.
*   **`pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError>`**
    Creates a monitor channel with default capacity (`rzmq::socket::DEFAULT_MONITOR_CAPACITY`).

### `rzmq::Msg` and `rzmq::MsgFlags`

`Msg` represents a message frame. `MsgFlags` (especially `MsgFlags::MORE`) are used for multi-part messages.

*   **Creating `Msg` instances:**
    *   `Msg::new()`: Empty message.
    *   `Msg::from_vec(data: Vec<u8>)`
    *   `Msg::from_bytes(data: bytes::Bytes)`
    *   `Msg::from_static(data: &'static [u8])` (zero-copy for static data)
*   **Accessing data and flags:**
    *   `msg.data() -> Option<&[u8]>`
    *   `msg.data_bytes() -> Option<bytes::Bytes>` (cheaply cloneable internal `Bytes`)
    *   `msg.size() -> usize`
    *   `msg.flags() -> MsgFlags`
    *   `msg.set_flags(flags: MsgFlags)`
    *   `msg.is_more() -> bool`

### `rzmq::Blob`
An immutable, reference-counted byte sequence, often used for identities or subscription topics.
*   Can be created from `Vec<u8>`, `&'static [u8]`, or `bytes::Bytes`.
*   Provides `Deref` to `[u8]` and `AsRef<[u8]>`.

## Working with Sockets

### Creating Sockets
Sockets are created from a `Context` instance, specifying a `SocketType`:
```rust
use rzmq::{Context, SocketType, ZmqError}; // Add ZmqError for the Result type

async fn setup_sockets() -> Result<(), ZmqError> {
    let ctx = Context::new()?;
    let publisher = ctx.socket(SocketType::Pub)?;
    let subscriber = ctx.socket(SocketType::Sub)?;
    // ... use publisher and subscriber ...
    ctx.term().await?; // Example of context termination
    Ok(())
}
```

### Binding and Connecting
*   **Bind (Server-side):** Sockets that accept incoming connections use `bind`.
    ```rust
    // rep_socket is a Socket of SocketType::Rep
    // async fn bind_example(rep_socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     rep_socket.bind("tcp://*:5555").await?; // Listen on all interfaces, port 5555
    //     rep_socket.bind("inproc://my-service").await?; // Requires "inproc" feature
    //     Ok(())
    // }
    ```
*   **Connect (Client-side):** Sockets that initiate connections use `connect`.
    ```rust
    // req_socket is a Socket of SocketType::Req
    // async fn connect_example(req_socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     req_socket.connect("tcp://server-address:5555").await?;
    //     req_socket.connect("inproc://my-service").await?; // Requires "inproc" feature
    //     Ok(())
    // }
    ```
Connections are typically asynchronous. `rzmq` handles retries for TCP connections if `RECONNECT_IVL` is set appropriately.

### Sending and Receiving Messages
*   **Sending:**
    ```rust
    use rzmq::Msg;
    // async fn send_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     let message = Msg::from_static(b"Data to send");
    //     socket.send(message).await?;
    //     Ok(())
    // }
    ```
*   **Receiving:**
    ```rust
    // async fn recv_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     let received_message = socket.recv().await?;
    //     if let Some(data) = received_message.data() {
    //         println!("Received: {}", String::from_utf8_lossy(data));
    //     }
    //     Ok(())
    // }
    ```

### Multi-Part Messages
To send or receive a message composed of multiple frames:
*   **Sending with `send()`:** Set `MsgFlags::MORE` on all frames except the last one.
    ```rust
    use rzmq::{Msg, MsgFlags};
    // async fn send_multipart_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     let mut frame1 = Msg::from_static(b"Identity");
    //     frame1.set_flags(MsgFlags::MORE);
    //     let mut frame2 = Msg::from_static(b""); // Empty delimiter
    //     frame2.set_flags(MsgFlags::MORE);
    //     let frame3 = Msg::from_static(b"Actual payload"); // Last frame, no MORE flag

    //     // Assuming 'socket' is a ROUTER or DEALER that handles these frames appropriately
    //     socket.send(frame1).await?;
    //     socket.send(frame2).await?;
    //     socket.send(frame3).await?;
    //     Ok(())
    // }
    ```
*   **Sending with `send_multipart()`:** (Recommended for ROUTER/DEALER)
    ```rust
    use rzmq::{Msg, MsgFlags};
    // async fn send_multipart_atomic_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    //     // For ROUTER: first frame is identity, subsequent are payload.
    //     // For DEALER: all frames are payload, delimiter is prepended.
    //     // Caller sets MORE flags for application payload parts.
    //     let identity_frame = Msg::from_static(b"destination_peer_id"); // No MORE flag if single payload frame follows
    //     let payload_frame = Msg::from_static(b"Hello to peer");

    //     // If application payload itself is multi-part:
    //     // let mut app_payload_part1 = Msg::from_static(b"Part 1");
    //     // app_payload_part1.set_flags(MsgFlags::MORE);
    //     // let app_payload_part2 = Msg::from_static(b"Part 2");
    //     // socket.send_multipart(vec![identity_frame, app_payload_part1, app_payload_part2]).await?;

    //     socket.send_multipart(vec![identity_frame, payload_frame]).await?;
    //     Ok(())
    // }
    ```

*   **Receiving:** After `socket.recv().await?`, check `msg.is_more()`. If `true`, call `recv()` again for the next part.
    ```rust
    // async fn recv_multipart_example(socket: &rzmq::Socket) -> Result<Vec<Msg>, ZmqError> {
    //     let mut all_frames = Vec::new();
    //     loop {
    //         let frame = socket.recv().await?;
    //         let is_last_part = !frame.is_more();
    //         all_frames.push(frame);
    //         if is_last_part {
    //             break;
    //         }
    //     }
    //     // Now 'all_frames' contains all parts of the logical message.
    //     Ok(all_frames)
    // }
    ```

### Socket Options
Fine-tune socket behavior using `set_option` and `get_option`.
Refer to the "Socket Options (Constants)" section in the main [API Reference](#socket-options-constants) for a list of common options and their types.

Example: Setting `SNDTIMEO` (send timeout)
```rust
use rzmq::socket::SNDTIMEO; // Assuming options are re-exported under rzmq::socket
// async fn set_sndtimeo_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
//     // Set send timeout to 500ms
//     socket.set_option(SNDTIMEO, &(500i32).to_ne_bytes()).await?;
//     Ok(())
// }
```
Example: Subscribing a `SUB` socket
```rust
use rzmq::socket::SUBSCRIBE; // Assuming options are re-exported under rzmq::socket
// async fn subscribe_example(sub_socket: &rzmq::Socket) -> Result<(), ZmqError> {
//     // sub_socket is a Socket of SocketType::Sub
//     sub_socket.set_option(SUBSCRIBE, b"NASDAQ:").await?; // Subscribe to messages starting with "NASDAQ:"
//     sub_socket.set_option(SUBSCRIBE, b"").await?;      // Subscribe to all messages
//     Ok(())
// }
```
Example: Getting `LAST_ENDPOINT` after binding to an ephemeral TCP port
```rust
use rzmq::socket::LAST_ENDPOINT; // Assuming options are re-exported under rzmq::socket
// async fn get_last_endpoint_example(pull_socket: &rzmq::Socket) -> Result<(), ZmqError> {
//     // pull_socket is a Socket, e.g., SocketType::Pull
//     pull_socket.bind("tcp://127.0.0.1:0").await?; // Bind to OS-assigned port
//     let endpoint_bytes = pull_socket.get_option(LAST_ENDPOINT).await?;
//     let resolved_endpoint = String::from_utf8(endpoint_bytes).unwrap_or_default();
//     println!("Socket bound to: {}", resolved_endpoint); // e.g., "tcp://127.0.0.1:54321"
//     Ok(())
// }
```

### Monitoring Socket Events
Track socket lifecycle events:
```rust
use rzmq::socket::SocketEvent; // Assuming events are re-exported under rzmq::socket
// async fn monitor_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
//     let mut monitor_rx = socket.monitor_default().await?;
//     tokio::spawn(async move {
//         while let Ok(event) = monitor_rx.recv().await {
//             match event {
//                 SocketEvent::Connected { endpoint, peer_addr } => {
//                     println!("Socket connected to {} (peer: {})", endpoint, peer_addr);
//                 }
//                 SocketEvent::Disconnected { endpoint } => {
//                     println!("Socket disconnected from {}", endpoint);
//                 }
//                 // Handle other events like Listening, Accepted, BindFailed, etc.
//                 _ => println!("Monitor: {:?}", event),
//             }
//         }
//         println!("Monitor channel closed.");
//     });
//     Ok(())
// }
```

### Closing Sockets and Context Termination
*   **`socket.close().await?`**: Initiates a graceful shutdown of a specific socket.
*   **`ctx.term().await?`**: Shuts down the entire context, including all its sockets and background tasks. This is essential for a clean application exit.

It is important to ensure that `Socket` handles are dropped or explicitly closed, and that `Context::term()` is awaited to allow background actors to terminate and release resources. The `Drop` implementation for `Socket` will attempt to initiate a close if not done explicitly, but relying on `Context::term()` is more robust for application-wide cleanup.

## Supported Socket Types

`rzmq` provides implementations for several common ZeroMQ patterns:

*   **`Pub` (Publish) / `Sub` (Subscribe)**: For broadcasting messages to multiple subscribers based on topic filters.
*   **`Req` (Request) / `Rep` (Reply)**: For synchronous request-response communication.
*   **`Push` / `Pull`**: For distributing messages in a pipeline or work queue.
*   **`Dealer` / `Router`**: For advanced, asynchronous request-response and message routing, often used to build brokers or load balancers. `ROUTER` sockets prefix incoming messages with the sender's identity.

## Using `io_uring` (Linux Specific)

`rzmq` offers an optional `io_uring` backend for TCP communication on Linux systems, which can offer performance benefits in high-I/O scenarios.

1.  **Enable Feature**: Add the `io-uring` feature for `rzmq` in your `Cargo.toml`:
    ```toml
    [dependencies]
    rzmq = { version = "...", features = ["io-uring"] } # Or git source
    ```
2.  **Use `#[rzmq::main]`**: Modify your application's `main` function to use the `#[rzmq::main]` attribute instead of `#[tokio::main]`.
    ```rust
    // In your main.rs or lib.rs if re-exporting from a library
    // pub use rzmq::main as rzmq_main_attr; // Optional rename if `main` is ambiguous

    #[rzmq::main] // Or #[rzmq_main_attr]
    async fn main() -> Result<(), rzmq::ZmqError> {
        // Your rzmq application code...
        let ctx = rzmq::Context::new()?;
        // Sockets created from this context will use io_uring for TCP
        // if the feature is enabled and the platform is Linux.
        // ...
        ctx.term().await?;
        Ok(())
    }
    ```
    The `#[rzmq::main]` macro automatically configures the Tokio runtime to use `tokio-uring` when the conditions are met. If not on Linux or the `io-uring` feature is disabled, it defaults to `#[tokio::main]`.

3.  **`io_uring` Specific Options**: (Optional)
    You can hint at `io_uring`-specific behaviors using socket options if the underlying transport uses them:
    *   `rzmq::socket::IO_URING_SNDZEROCOPY`: (Boolean) Hint to use zerocopy for sends.
    *   `rzmq::socket::IO_URING_RCVMULTISHOT`: (Boolean) Hint to use multishot receive.
    *   `rzmq::socket::IO_URING_RECV_BUFFER_COUNT`: (Integer) Number of buffers in the multishot receive pool (default: 16, min: 1).
    *   `rzmq::socket::IO_URING_RECV_BUFFER_SIZE`: (Integer) Size of each buffer (in bytes) in the multishot receive pool (default: 65536, min: 1024).

    These are set using `socket.set_option(OPTION_CONST, &value.to_ne_bytes()).await?`. Boolean options typically use `1i32` for true and `0i32` for false.

## Error Handling

The primary error type is `rzmq::ZmqError`. It's an enum covering various error conditions.

*   **Common Variants**:
    *   `IoError { kind, message }`: Wraps `std::io::Error`.
    *   `Timeout`: Operation exceeded specified timeout (`SNDTIMEO` or `RCVTIMEO`).
    *   `AddrInUse(String)`: Address already in use during `bind`.
    *   `ConnectionRefused(String)`: Peer actively refused connection.
    *   `InvalidState(&'static str)`: Operation attempted in an invalid socket state (e.g., REQ sending twice before a recv).
    *   `ResourceLimitReached`: Send/receive buffer high-water mark reached (acts like EAGAIN).
    *   `HostUnreachable(String)`: For `ROUTER` with `ROUTER_MANDATORY`, if peer identity is unknown.
    *   `InvalidEndpoint(String)`: Malformed endpoint string.
    *   *(See the full list in the API Reference or `rzmq::ZmqError` documentation for more variants.)*

*   **Result Type Alias**:
    `rzmq::error::ZmqResult<T>` is an alias for `Result<T, rzmq::ZmqError>`.
    Most API functions return this type (or `Result<(), rzmq::ZmqError>`).
    ```rust
    // Example of handling errors
    // async fn my_operation(socket: &rzmq::Socket) {
    //     match socket.send(Msg::from_static(b"test")).await {
    //         Ok(()) => println!("Message sent!"),
    //         Err(rzmq::ZmqError::Timeout) => eprintln!("Send timed out"),
    //         Err(e) => eprintln!("An error occurred: {}", e),
    //     }
    // }
    ```