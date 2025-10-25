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
*   [Security Mechanisms](#security-mechanisms)
*   [Using `io_uring` (Linux Specific)](#using-io_uring-linux-specific)
*   [Error Handling](#error-handling)

## Introduction

This guide will help you understand the fundamental building blocks of `rzmq` and demonstrate how to use its API to construct various messaging patterns. `rzmq` leverages Tokio for its asynchronous operations, allowing for efficient, non-blocking communication. It aims for ZMTP 3.1 wire-level interoperability with `libzmq` for core patterns using NULL or PLAIN security.

## Core Concepts

Understanding these core concepts is key to effectively using `rzmq`:

*   **`Context`**: The starting point for all `rzmq` operations. It acts as a container for sockets and manages shared resources. A single `Context` can manage multiple sockets. `Context` handles are lightweight (`Arc`-based) and can be cloned.
*   **`Socket`**: Represents a ZeroMQ-style communication endpoint. Each socket has a specific `SocketType` that defines its messaging behavior (e.g., PUB/SUB, REQ/REP). `Socket` handles are cloneable. All I/O operations on a `Socket` are asynchronous.
*   **`SocketType`**: An enum that determines the pattern of a socket. This is specified when creating a socket (e.g., `SocketType::Pub`, `SocketType::Req`). The chosen type dictates how the socket sends and receives messages.
*   **`Msg`**: The fundamental unit for data exchange. Messages can be single-part or multi-part. `rzmq` provides the `Msg` struct for creating and manipulating message frames.
*   **`MsgFlags`**: Associated with `Msg` instances, these flags control aspects of message handling. The most common is `MsgFlags::MORE`, used to indicate that more frames are part of the current logical message.
*   **Endpoints**: String identifiers for network addresses or communication channels. `rzmq` supports `tcp://host:port`, `ipc:///path/to/socket` (on Unix-like systems with the `ipc` feature), and `inproc://name` (for intra-process communication with the `inproc` feature).
*   **Asynchronous Model**: All potentially blocking operations (network I/O, waiting for messages) are `async` and need to be `.await`ed within a Tokio runtime.
*   **Socket Options**: Sockets can be configured with various options (e.g., high-water marks, timeouts, identity) using `Socket::set_option_raw()` or the convenience `Socket::set_option()` for types implementing `ToBytes`.
*   **Monitoring**: `rzmq` provides a mechanism to monitor socket events (like connection establishment, disconnections, etc.) through a channel obtained via `Socket::monitor()`.
*   **Graceful Shutdown**: It's crucial to call `Context::term().await` to ensure all sockets are properly closed, pending messages are handled according to `LINGER` options, and background tasks are terminated cleanly.
*   **Security**: `rzmq` supports multiple security mechanisms, including NULL (no security), PLAIN (username/password), CURVE (for interoperable encryption with libzmq), and a custom Noise_XX implementation for `rzmq`-to-`rzmq` communication. ZAP is not yet supported.

## Quick Start

These examples demonstrate basic usage. Ensure you have `rzmq` and `tokio` in your `Cargo.toml`.

### Push/Pull Example

Pushes messages to a load-balanced set of pullers.

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?; // Use default capacities

    let push_socket = ctx.socket(SocketType::Push)?;
    let pull_socket = ctx.socket(SocketType::Pull)?;

    // For this example, use the "inproc" feature.
    // Ensure rzmq is added with `features = ["inproc"]` in Cargo.toml
    let endpoint = "inproc://qstart-pushpull";

    pull_socket.bind(endpoint).await?;
    // Give a moment for bind to complete, especially important for inproc
    tokio::time::sleep(Duration::from_millis(10)).await;
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
    Initiates a graceful shutdown and waits for all background tasks to complete. **This is the recommended way to shut down an `rzmq` application.**

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
    Sends a single message part (`Msg`). For multi-part messages where `send` is used iteratively, set `MsgFlags::MORE` on all but the last part.
*   **`pub async fn recv(&self) -> Result<Msg, ZmqError>`**
    Receives a single message part (`Msg`). Check `msg.is_more()` for multi-part messages and call `recv()` again if true.
*   **`pub async fn send_multipart(&self, frames: Vec<Msg>) -> Result<(), ZmqError>`**
    Sends a sequence of `Msg` frames as a single logical message. The library automatically handles setting the `MORE` flag on the ZMTP frames that are sent over the wire.
*   **`pub async fn recv_multipart(&self) -> Result<Vec<Msg>, ZmqError>`**
    Receives all frames of a complete logical ZeroMQ message.
*   **`pub async fn set_option<T: ToBytes>(&self, option: i32, value: T) -> Result<(), ZmqError>`**
    Sets a socket option using a value that implements the `rzmq::socket::ToBytes` trait (e.g., `i32`, `bool`, `String`, `&[u8]`).
*   **`pub async fn set_option_raw(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>`**
    Sets a socket option using a raw byte slice for the value.
*   **`pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>`**
    Gets a socket option value as raw bytes.
*   **`pub async fn close(&self) -> Result<(), ZmqError>`**
    Gracefully closes this specific socket.
*   **`pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError>`**
    Creates a channel for receiving `SocketEvent` notifications with a specific capacity.
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
use rzmq::{Context, SocketType, ZmqError};

async fn setup_sockets() -> Result<(), ZmqError> {
    let ctx = Context::new()?;
    let publisher = ctx.socket(SocketType::Pub)?;
    let subscriber = ctx.socket(SocketType::Sub)?;
    // ... use publisher and subscriber ...
    ctx.term().await?;
    Ok(())
}
```

### Binding and Connecting
*   **Bind (Server-side):** Sockets that accept incoming connections use `bind`.
    ```rust
    async fn bind_example(rep_socket: &rzmq::Socket) -> Result<(), ZmqError> {
        rep_socket.bind("tcp://*:5555").await?; // Listen on all interfaces, port 5555
        // For inproc, ensure "inproc" feature is enabled for rzmq
        // rep_socket.bind("inproc://my-service").await?;
        Ok(())
    }
    ```
*   **Connect (Client-side):** Sockets that initiate connections use `connect`.
    ```rust
    async fn connect_example(req_socket: &rzmq::Socket) -> Result<(), ZmqError> {
        req_socket.connect("tcp://server-address:5555").await?;
        // For inproc, ensure "inproc" feature is enabled for rzmq
        // req_socket.connect("inproc://my-service").await?;
        Ok(())
    }
    ```
Connections are asynchronous. `rzmq` handles retries for TCP connections if `RECONNECT_IVL` is set appropriately.

### Sending and Receiving Messages
*   **Sending (single part):**
    ```rust
    use rzmq::Msg;
    async fn send_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
        let message = Msg::from_static(b"Data to send");
        socket.send(message).await?;
        Ok(())
    }
    ```
*   **Receiving (single part):**
    ```rust
    async fn recv_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
        let received_message = socket.recv().await?;
        if let Some(data) = received_message.data() {
            println!("Received: {}", String::from_utf8_lossy(data));
        }
        Ok(())
    }
    ```

### Multi-Part Messages
*   **Sending with iterative `send()`:** Set `MsgFlags::MORE` on all frames except the last one.
    ```rust
    use rzmq::{Msg, MsgFlags};
    async fn send_multipart_iterative_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
        let mut frame1 = Msg::from_static(b"Frame1");
        frame1.set_flags(MsgFlags::MORE);
        let frame2 = Msg::from_static(b"Frame2_Last"); // No MORE flag

        socket.send(frame1).await?;
        socket.send(frame2).await?;
        Ok(())
    }
    ```
*   **Sending with `send_multipart()`:** This is the preferred method for sending a complete logical message. The library handles setting the `MORE` flag on the wire.
    ```rust
    use rzmq::{Msg, MsgFlags};
    async fn send_multipart_atomic_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
        let frames = vec![
            Msg::from_static(b"Identity"), // For ROUTER
            Msg::from_static(b"Payload Part 1"),
            Msg::from_static(b"Payload Part 2"),
        ];
        socket.send_multipart(frames).await?;
        Ok(())
    }
    ```

*   **Receiving with iterative `recv()`:** After `socket.recv().await?`, check `msg.is_more()`. If `true`, call `recv()` again.
    ```rust
    async fn recv_multipart_iterative_example(socket: &rzmq::Socket) -> Result<Vec<Msg>, ZmqError> {
        let mut all_frames = Vec::new();
        loop {
            let frame = socket.recv().await?;
            let is_last_part = !frame.is_more();
            all_frames.push(frame);
            if is_last_part {
                break;
            }
        }
        Ok(all_frames)
    }
    ```
*   **Receiving with `recv_multipart()`:**
    ```rust
    async fn recv_multipart_atomic_example(socket: &rzmq::Socket) -> Result<Vec<Msg>, ZmqError> {
        let all_frames = socket.recv_multipart().await?;
        // 'all_frames' contains all parts of the logical ZMQ message (e.g., for ROUTER, this includes identity).
        Ok(all_frames)
    }
    ```

### Socket Options
Fine-tune socket behavior using `socket.set_option_raw(OPTION_ID, byte_slice_value)` or the convenience `socket.set_option(OPTION_ID, typed_value)`.
Refer to the [API Reference](#8-public-constants) for a list of common option constants (e.g., `rzmq::socket::SNDHWM`).

Example: Setting `SNDTIMEO` (send timeout)
```rust
use rzmq::socket::SNDTIMEO;
use std::time::Duration;
async fn set_sndtimeo_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    let timeout_ms = 500i32; // 500 ms
    socket.set_option(SNDTIMEO, timeout_ms).await?; // Using convenience set_option
    // Or with set_option_raw:
    // socket.set_option_raw(SNDTIMEO, &timeout_ms.to_ne_bytes()).await?;
    Ok(())
}
```
Example: Subscribing a `SUB` socket
```rust
use rzmq::socket::SUBSCRIBE;
async fn subscribe_example(sub_socket: &rzmq::Socket) -> Result<(), ZmqError> {
    sub_socket.set_option(SUBSCRIBE, "NASDAQ:").await?; // Subscribe to messages starting with "NASDAQ:"
    // To subscribe to all messages:
    // sub_socket.set_option(SUBSCRIBE, "").await?;
    Ok(())
}
```

### Monitoring Socket Events
Track socket lifecycle events:
```rust
use rzmq::socket::SocketEvent;
async fn monitor_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    let mut monitor_rx = socket.monitor_default().await?;
    tokio::spawn(async move {
        while let Ok(event) = monitor_rx.recv().await {
            match event {
                SocketEvent::Connected { endpoint, peer_addr } => {
                    println!("Socket connected to {} (peer: {})", endpoint, peer_addr);
                }
                SocketEvent::Disconnected { endpoint } => {
                    println!("Socket disconnected from {}", endpoint);
                }
                SocketEvent::HandshakeSucceeded { endpoint } => {
                    println!("Handshake succeeded with {}", endpoint);
                }
                // Handle other events like Listening, Accepted, BindFailed, HandshakeFailed etc.
                _ => println!("Monitor: {:?}", event),
            }
        }
        println!("Monitor channel closed.");
    });
    Ok(())
}
```

### Closing Sockets and Context Termination
*   **`socket.close().await?`**: Initiates a graceful shutdown of a specific socket.
*   **`ctx.term().await?`**: Shuts down the entire context, including all its sockets and background tasks. This is essential for a clean application exit.

It is important to ensure that `Socket` handles are dropped or explicitly closed, and that `Context::term()` is awaited to allow background actors to terminate and release resources.

## Supported Socket Types

`rzmq` provides implementations for several common ZeroMQ patterns:

*   **`Pub` (Publish) / `Sub` (Subscribe)**: For broadcasting messages to multiple subscribers based on topic filters.
*   **`Req` (Request) / `Rep` (Reply)**: For synchronous request-response communication.
*   **`Push` / `Pull`**: For distributing messages in a pipeline or work queue.
*   **`Dealer` / `Router`**: For advanced, asynchronous request-response and message routing.

## Security Mechanisms

`rzmq` supports the following security mechanisms:

*   **NULL**: No security (default). Interoperable with other ZeroMQ implementations using NULL.
*   **PLAIN** (requires `plain` feature): Username/password based authentication. Interoperable with other ZeroMQ implementations using PLAIN.
    *   Server: Set `rzmq::socket::PLAIN_SERVER` option to `true` (as `1i32`).
    *   Client: Set `rzmq::socket::PLAIN_USERNAME` and `rzmq::socket::PLAIN_PASSWORD` options.
*   **CURVE** (requires `curve` feature): Encrypted and authenticated sessions based on the official CurveZMQ security protocol. This mechanism provides strong security and is designed to be **interoperable** with `libzmq`'s CURVE implementation.
    *   Server: Set `rzmq::socket::CURVE_SERVER` option to `true` (as `1i32`) and provide its secret key via `rzmq::socket::CURVE_SECRET_KEY`.
    *   Client: Provide its own secret key via `rzmq::socket::CURVE_SECRET_KEY` and the server's public key via `rzmq::socket::CURVE_SERVER_KEY`.
*   **Noise_XX** (requires `noise_xx` feature): Encrypted and authenticated sessions using the Noise Protocol Framework (XX handshake pattern). This is a modern security mechanism specific to `rzmq` and **will not interoperate** with `libzmq`'s `CURVE` security. Use it for `rzmq`-to-`rzmq` communication.
    *   Enable with `rzmq::socket::NOISE_XX_ENABLED` option (`true` as `1i32`).
    *   Set `rzmq::socket::NOISE_XX_STATIC_SECRET_KEY` (32-byte array).
    *   Client must set `rzmq::socket::NOISE_XX_REMOTE_STATIC_PUBLIC_KEY` (server's 32-byte public key).

**Note:** ZAP (ZeroMQ Authentication Protocol) is **not yet fully supported**.

## Using `io_uring` (Linux Specific)

`rzmq` offers an optional `io_uring` backend for TCP communication on Linux systems, which can offer significant performance benefits.

1.  **Enable Cargo Feature**: Add the `io-uring` feature for `rzmq` in your `Cargo.toml`:
    ```toml
    [dependencies]
    rzmq = { version = "...", features = ["io-uring"] } # Or git source
    ```
2.  **Use `#[tokio::main]`**: Your application's `main` function should use the standard `#[tokio::main]` attribute. `rzmq`'s `io_uring` backend integrates with the Tokio runtime it finds.
3.  **Initialize the `io_uring` Backend**: Before creating any `rzmq::Context`, you must initialize the `io_uring` backend. This is a global, one-time setup.
    ```rust
    use rzmq::uring::{initialize_uring_backend, shutdown_uring_backend, UringConfig};

    #[tokio::main]
    async fn main() -> Result<(), rzmq::ZmqError> {
        // Initialize with default configuration. This can only be done once.
        initialize_uring_backend(UringConfig::default())?;
        
        // Or with custom configuration:
        // let uring_cfg = UringConfig {
        //     ring_entries: 512,
        //     default_send_zerocopy: true,
        //     /* ... other fields ... */
        //     ..Default::default()
        // };
        // initialize_uring_backend(uring_cfg)?;

        let ctx = rzmq::Context::new()?;
        // Sockets created from this context can now opt-in to use io_uring.
        // ... your application logic ...

        ctx.term().await?;

        // Shutdown the io_uring backend (optional but good practice).
        shutdown_uring_backend().await?;
        Ok(())
    }
    ```
4.  **Enable `io_uring` for Socket Sessions**: For each `Socket` where you want TCP connections to use `io_uring`, you must set the `IO_URING_SESSION_ENABLED` socket option to `true` (as `1i32`).
    ```rust
    // req_socket is an rzmq::Socket
    // req_socket.set_option(rzmq::socket::IO_URING_SESSION_ENABLED, 1i32).await?;
    // Now, TCP connections made by/to this req_socket will attempt to use the io_uring backend.
    ```
5.  **`io_uring` Specific Socket Options**: (Optional)
    Once `IO_URING_SESSION_ENABLED` is set, you can further influence behavior with:
    *   `rzmq::socket::TCP_CORK`: (Boolean `1i32` or `0i32`) Enable/disable TCP corking for better batching.
    *   `rzmq::socket::IO_URING_SNDZEROCOPY`: (Boolean) Request zero-copy sends. Fulfillment depends on global `UringConfig.default_send_zerocopy` and pool setup.
    *   `rzmq::socket::IO_URING_RCVMULTISHOT`: (Boolean) Request multishot receives. Fulfillment depends on global `UringConfig.default_recv_multishot` and default ring setup.

    Global parameters for zero-copy send pools and the default multishot receive ring are set via `UringConfig` during `initialize_uring_backend()`.

## Error Handling

The primary error type is `rzmq::ZmqError`. It's an enum covering various error conditions.

*   **Common Variants**:
    *   `IoError { kind, message }`: Wraps `std::io::Error`.
    *   `Timeout`: Operation exceeded specified timeout (`SNDTIMEO` or `RCVTIMEO`).
    *   `AddrInUse(String)`: Address already in use during `bind`.
    *   `ConnectionRefused(String)`: Peer actively refused connection.
    *   `InvalidState(&'static str)`: Operation attempted in an invalid socket state (e.g., REQ sending twice).
    *   `ResourceLimitReached`: Send/receive buffer high-water mark reached (acts like EAGAIN).
    *   `HostUnreachable(String)`: E.g., for `ROUTER` with `ROUTER_MANDATORY`, if peer identity is unknown.
    *   `InvalidEndpoint(String)`: Malformed endpoint string.
    *   *(See the full list in the API Reference or `rzmq::ZmqError` documentation for more variants.)*

*   **Result Type Alias**:
    `rzmq::ZmqResult<T>` is an alias for `Result<T, rzmq::ZmqError>`.
    Most API functions return this type.
    ```rust
    async fn my_operation(socket: &rzmq::Socket) {
        match socket.send(Msg::from_static(b"test")).await {
            Ok(()) => println!("Message sent!"),
            Err(rzmq::ZmqError::Timeout) => eprintln!("Send timed out"),
            Err(e) => eprintln!("An error occurred: {}", e),
        }
    }
    ```