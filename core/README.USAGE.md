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
    *   [Sharing a TCP Port (`SO_REUSEPORT`)](#sharing-a-tcp-port-so_reuseport)
    *   [Monitoring Socket Events](#monitoring-socket-events)
    *   [Closing Sockets and Context Termination](#closing-sockets-and-context-termination)
*   [Supported Socket Types](#supported-socket-types)
*   [Security Mechanisms](#security-mechanisms)
*   [Using `io_uring` (Linux Specific)](#using-io_uring-linux-specific)
*   [Adaptive I/O Throttling](#adaptive-io-throttling)
*   [Publishing to Slow Subscribers](#publishing-to-slow-subscribers)
    *   [Waiting for Every Subscriber (`XPUB_NODROP`)](#waiting-for-every-subscriber-xpub_nodrop)
    *   [Bounding the Wait](#bounding-the-wait)
    *   [Reading the Setting](#reading-the-setting)
*   [Retrying a Request](#retrying-a-request)
    *   [Sending Again on the Same Socket (`REQ_RELAXED` and `REQ_CORRELATE`)](#sending-again-on-the-same-socket-req_relaxed-and-req_correlate)
    *   [Replying When the Requester Has Left](#replying-when-the-requester-has-left)
    *   [Correlating Through a ROUTER](#correlating-through-a-router)
    *   [Reading the Settings](#reading-the-settings)
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
*   **Graceful Shutdown**: It's crucial to call `Context::term().await` to ensure all sockets are properly closed, pending messages are delivered according to `LINGER` (default -1, deliver everything queued) and background tasks are terminated cleanly.
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
*   **`pub fn with_blocky(self, blocky: bool) -> Self`**
    libzmq's `ZMQ_BLOCKY`. With `false`, sockets this context creates start with `LINGER` 0, so `close` and `term` drop queued messages instead of delivering them. The default is `true`.
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
Connections are asynchronous. `connect()` returns before the connection exists but the socket already has a pipe for that peer. Sends made before the connection is up queue in that pipe up to `SNDHWM` and are delivered in order once the handshake completes. The pipe survives a disconnect, so messages sent while a peer is down are delivered when it reconnects, with `RECONNECT_IVL` governing the retries. A peer that is down therefore keeps taking its share of a `PUSH` or `DEALER` round-robin until its pipe is full. Set `IMMEDIATE` to `true` to attach a peer only while it is connected and skip it otherwise.

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

Example: Setting `AUTO_DELIMITER` (Removes auto delimiting handling from router/dealer sockets, for interoperation purposes)
```rust
use rzmq::socket::AUTO_DELIMITER;
use std::time::Duration;
async fn set_auto_delimiter_example(socket: &rzmq::Socket) -> Result<(), ZmqError> {
    socket.set_option(AUTO_DELIMITER, false).await?;
    Ok(())
}
```
Example: Setting `MAXMSGSIZE` (maximum inbound frame size)
```rust
use rzmq::socket::MAXMSGSIZE;
async fn set_maxmsgsize_example(socket: &rzmq::Socket) -> Result<(), rzmq::ZmqError> {
    // Reject any inbound frame larger than 1 MiB
    let limit: i64 = 1024 * 1024;
    socket.set_option_raw(MAXMSGSIZE, &limit.to_ne_bytes()).await?;

    // Restore the default (unlimited)
    socket.set_option_raw(MAXMSGSIZE, &(-1i64).to_ne_bytes()).await?;
    Ok(())
}
```
The value is an `i64` (8 bytes, native-endian). `-1` means unlimited and is the default. A peer that
sends a frame exceeding the limit will have its connection dropped with a protocol violation.

Example: Setting `SNDBUF`/`RCVBUF` (OS socket buffer sizes)
```rust
use rzmq::socket::{SNDBUF, RCVBUF};
async fn set_socket_buffers_example(socket: &rzmq::Socket) -> Result<(), rzmq::ZmqError> {
    // Request 256 KiB send and receive buffers from the OS.
    // Note: Linux silently doubles the requested value and may cap it at
    // /proc/sys/net/core/wmem_max and rmem_max. Set 0 to use the OS default.
    let buf_size: i32 = 256 * 1024;
    socket.set_option(SNDBUF, buf_size).await?;
    socket.set_option(RCVBUF, buf_size).await?;
    Ok(())
}
```
`SNDBUF` and `RCVBUF` configure the **OS kernel** send/receive socket buffers, which are distinct
from the application-level high-water marks (`SNDHWM`/`RCVHWM`). Increasing these values can
reduce kernel context-switching overhead on high-throughput TCP and IPC connections. The setting
is applied when a connection is accepted or established; changing it after that has no effect on
existing connections.

Example: Setting `ALLOW_ZMTP2` (legacy ZMTP/2.0 wire-protocol compatibility)
```rust
use rzmq::socket::ALLOW_ZMTP2;
async fn configure_zmtp2(socket: &rzmq::Socket) -> Result<(), rzmq::ZmqError> {
    // Enabled by default: rzmq automatically downgrades the handshake to ZMTP/2.0
    // when a legacy peer (older libzmq) announces it, in either direction.
    // Disable it to enforce ZMTP/3.x only and reject v2 peers:
    socket.set_option(ALLOW_ZMTP2, false).await?;
    Ok(())
}
```
ZMTP/2.0 is the wire protocol used by older `libzmq` (3.0–3.2). When enabled (the default),
the handshake transparently negotiates the highest common version. A ZMTP/2.0 session uses
**NULL security only** (CURVE/PLAIN/Noise are not available over v2), exchanges peer identities
as bare frames instead of the `READY` command, and has **no PING/PONG heartbeats**. Set this
before `bind`/`connect`.

### Outbound Write Batching (`SNDBATCH_COUNT` / `SNDBATCH_BYTES`)

By default, each session actor issues one `write_all` syscall per logical ZMQ message. Under high message rates, telemetry pipelines, fan-out `PUB`/`SUB`, high-frequency `PUSH` senders - this means thousands of short syscalls per second and the corresponding context-switch overhead.

**How it works**: after the actor wakes on the first available message, it synchronously drains additional queued messages from the pipe (using a non-blocking channel `try_recv`) until either the message count or the cumulative payload size hits the configured ceiling. All drained messages are encoded into a single coalesced buffer and sent with one `write_all` call. The adaptive throttle is debited for the full batch weight at once.

**When to tune these**:

| Workload | Recommendation |
|---|---|
| High-throughput fan-out (`PUB`/`PUSH`) with many small messages | Increase `SNDBATCH_COUNT` (e.g. 256–512) and `SNDBATCH_BYTES` to match your typical burst size |
| Latency-sensitive request/reply (`REQ`/`REP`, `DEALER`/`ROUTER`) | Keep defaults or reduce `SNDBATCH_COUNT` to 1–4; each request is usually a single message and batching adds unnecessary latency |
| Large message streams (video frames, bulk data) | Reduce `SNDBATCH_COUNT` to 1–2 but keep `SNDBATCH_BYTES` high; each message is already large enough to fill a TCP segment |

```rust
use rzmq::socket::{SNDBATCH_COUNT, SNDBATCH_BYTES};

async fn configure_batching(socket: &rzmq::Socket) -> Result<(), rzmq::ZmqError> {
    // Coalesce up to 256 messages or 1 MiB per write, whichever comes first.
    socket.set_option_raw(SNDBATCH_COUNT, &(256i32).to_ne_bytes()).await?;   // option id 1215
    socket.set_option_raw(SNDBATCH_BYTES, &(1024 * 1024i32).to_ne_bytes()).await?; // option id 1216
    Ok(())
}
```

Both options must be set **before** `bind`/`connect`. Defaults (`128` messages, `512 KB`) are tuned for general use and typically require no adjustment unless profiling shows syscall overhead in `write_all`.

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

### Sharing a TCP Port (`SO_REUSEPORT`)

By default one TCP address:port carries exactly one listener with one accept loop. Setting `REUSE_PORT` before `bind` applies `SO_REUSEPORT` to the listening socket, so several listeners can share the same port and accept work spreads across them.

```rust
use rzmq::socket::REUSE_PORT;

async fn shard_a_port(ctx: &rzmq::Context) -> Result<(), rzmq::ZmqError> {
    let endpoint = "tcp://0.0.0.0:5555";
    let mut shards = Vec::new();
    for _ in 0..4 {
        let pull = ctx.socket(rzmq::SocketType::Pull)?;
        pull.set_option(REUSE_PORT, 1i32).await?;
        pull.bind(endpoint).await?;
        shards.push(pull);
    }
    Ok(())
}
```

The same option also lets a *single* socket `bind` one endpoint repeatedly, giving that socket several accept loops behind one message stream. `unbind` names the endpoint, not an individual listener, so it stops every listener sharing that URI.

Constraints worth knowing before relying on it:

*   **Unix only.** On Windows, Solaris and illumos the option is accepted and stored but never applied, so code stays portable without `cfg` branches.
*   **Linux distributes, macOS does not.** Linux load-balances incoming connections across every listener on the port. On macOS the binds all succeed but a single listener receives every connection and the others accept nothing, so a sharded setup looks healthy while most of its consumers sit idle. FreeBSD needs `SO_REUSEPORT_LB` for distribution; macOS has no equivalent. Sharding is therefore a Linux deployment strategy, even though the binds succeed elsewhere.
*   **Every binder must set it, including the first.** If the first listener on a port did not set `REUSE_PORT`, later binds still fail with `AddrInUse`.
*   **Set it before `bind`.** Options are snapshotted when the bind is processed; setting it afterwards affects only subsequent binds.
*   **Use an explicit port.** `tcp://host:0` asks the OS for an ephemeral port and each bind gets a different one. Bind the first socket to `:0`, read `LAST_ENDPOINT`, then bind the rest to that resolved URI.

### Monitoring Socket Events
Track socket lifecycle events. The `SocketEvent` enum (non-exhaustive) currently emits:

*   **Listener**: `Listening`, `BindFailed`, `Accepted`, `AcceptFailed`
*   **Connecter**: `Connected`, `ConnectDelayed`, `ConnectRetried`, `ConnectFailed`
*   **Connection/session**: `Closed`, `Disconnected`
*   **Handshake/security**: `HandshakeSucceeded`, `HandshakeFailed`
*   **Backpressure/congestion**: `ConnectionCongested` (egress pipe hit send HWM; peer skipped via write-ready skip), `ConnectionUncongested` (egress pipe drained below HWM; peer re-enters rotation)

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
                // Handle other events like Listening, Accepted, BindFailed,
                // HandshakeFailed, ConnectionCongested, ConnectionUncongested, etc.
                _ => println!("Monitor: {:?}", event),
            }
        }
        println!("Monitor channel closed.");
    });
    Ok(())
}
```

### Closing Sockets and Context Termination
*   **`socket.close().await?`**: Initiates a graceful shutdown of a specific socket. It returns at once; the socket keeps delivering queued messages in the background for its `LINGER`.
*   **`ctx.term().await?`**: Shuts down the entire context, including all its sockets and background tasks. This is essential for a clean application exit.

`LINGER` defaults to -1, as in libzmq: a message accepted by `send` before `close` is delivered and `term` waits for it. A socket whose peer never comes back would hold `term` for ever, so `term` forces any socket still lingering after 10 seconds to close and drop what it holds. To drop queued messages at once instead, set `LINGER` to 0 on the socket or create the context with `Context::new()?.with_blocky(false)`.

```rust
use rzmq::socket::LINGER;
async fn close_without_waiting(socket: &rzmq::Socket) -> Result<(), rzmq::ZmqError> {
    socket.set_option(LINGER, 0i32).await?;
    socket.close().await
}
```

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
    *   Server: Set `rzmq::socket::PLAIN_SERVER` to `true` (as `1i32`) and the one username and password it accepts with `rzmq::socket::PLAIN_USERNAME` and `rzmq::socket::PLAIN_PASSWORD`. A client presenting anything else is rejected with `HandshakeFailed`. A server with no username and password set rejects every client. rzmq has no ZAP; the pair on the server socket takes the place of the authenticator a `libzmq` PLAIN server requires.
    *   Client: Set `rzmq::socket::PLAIN_USERNAME` and `rzmq::socket::PLAIN_PASSWORD` options.
*   **CURVE** (requires `curve` feature): Encrypted and authenticated sessions using CurveZMQ (RFC 26), on the wire as `libzmq` sends it, so an rzmq client talks to a `libzmq` server and the other way round. Keys are given as 32 raw bytes or as the 40-character Z85 text that `zmq_curve_keypair` and pyzmq's `zmq.curve_keypair()` produce.
    *   Server: Set `rzmq::socket::CURVE_SERVER` to `true` (as `1i32`) and its secret key with `rzmq::socket::CURVE_SECRET_KEY`. Every client holding the server's public key is accepted; rzmq has no ZAP, so there is no per-client allow list.
    *   Client: Set its own secret key with `rzmq::socket::CURVE_SECRET_KEY` and the server's public key with `rzmq::socket::CURVE_SERVER_KEY`. `rzmq::socket::CURVE_PUBLIC_KEY` is optional, since the public key is derived from the secret key.
    *   `get_option` returns a key as 32 raw bytes; `rzmq::security::z85::encode` turns it into the 40-character text, and `rzmq::security::z85::decode` reads one.
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
    *   `rzmq::socket::IO_URING_ZC_SEND_THRESHOLD`: (`i32`) Minimum payload bytes before switching from copy-based to `SEND_ZC`. Default `16384`; only relevant when `IO_URING_SNDZEROCOPY` is also enabled.

    Global parameters for zero-copy send pools and the default multishot receive ring are set via `UringConfig` during `initialize_uring_backend()`.

6.  **Choosing the Worker Count**: The backend runs `num_workers` worker threads, each with its own ring and buffer pools. Each connection is assigned to a worker round-robin when it registers and stays there. The default is `ceil(cores / 2) - 2`, clamped to 1–8.
    ```rust
    use rzmq::uring::{initialize_uring_backend, UringConfig};

    let uring_cfg = UringConfig {
        num_workers: 4, // default_uring_num_workers() gives the cores-based value
        ..Default::default()
    };
    initialize_uring_backend(uring_cfg)?;
    ```
    One connection always runs on one worker, so extra workers only help when the process has several `io_uring` connections. Set `num_workers: 1` to put every connection on a single ring.

    Each worker registers its own receive buffer ring, so pinned receive memory is `num_workers × default_recv_buffer_count × default_recv_buffer_size` and counts against `RLIMIT_MEMLOCK`. The zero-copy send pool is a total that is split across workers, so it does not grow. If the first worker fails to start, `initialize_uring_backend` returns the error. If a later one fails, the backend continues with fewer workers and logs a warning. With `sqpoll_enabled` each worker also gets its own kernel poll thread.

## Adaptive I/O Throttling

`rzmq` includes a built-in, probabilistic I/O fairness engine that runs transparently inside each connection's async loop. It prevents one direction of traffic (ingress or egress) from completely starving the other when the workload is asymmetric. This feature is **unique to `rzmq`** and has no equivalent in `libzmq`.

### How It Works

Each connection tracks a real-time **debt/credit balance**:

*   Every ingress operation adds `credit_per_message` to the balance (creating "debt").
*   Every egress operation subtracts `credit_per_message` from the balance (paying it off).

An exponential moving average (EMA) continuously learns the natural operating balance of the workload. When the real-time balance strays too far from the learned average, the throttle probabilistically calls `tokio::task::yield_now()`, handing CPU time back to the scheduler so the under-served direction can run. A hard consecutive-op cap ensures eventual fairness regardless of probability.

The throttle uses a static function pointer (`should_throttle_fn`) on the guard struct, so **a disabled throttle adds zero branches to the hot path** and `begin_work` returns immediately, touching no atomics.

### Default Behavior

The throttle is **enabled by default** on every socket. `AdaptiveThrottleSocketConfig::default()` holds the settings it runs with:

| Field | Default |
|---|---|
| `enabled` | `true` |
| `credit_per_message` | 5 |
| `healthy_balance_width` | 1 024 000 |
| `max_imbalance` | 6 553 600 |
| `yield_after_n_consecutive` | 256 |
| `nudge_interval_ops` | 100 |
| `adaptive_learning_rate` | 0.05 |
| `strategy` | `ThrottleStrategy::PowerCurve { exponent: 2.0 }` |
| `priority` | `ThrottlePriority::Auto` (Egress for accepted connections, Ingress for outbound ones) |
| `priority_boost_factor` | 5.0 |

### Configuring the Throttle

`ADAPTIVE_THROTTLE` takes an `AdaptiveThrottleSocketConfig`. Set it before `bind` or `connect`; afterwards the call returns `ZmqError::InvalidState`.

```rust
use rzmq::socket::{AdaptiveThrottleSocketConfig, ThrottlePriority, ThrottleStrategy, ADAPTIVE_THROTTLE};
use rzmq::{Context, SocketType, ZmqError};

async fn example(ctx: &Context) -> Result<(), ZmqError> {
    let socket = ctx.socket(SocketType::Dealer)?;
    let config = AdaptiveThrottleSocketConfig {
        yield_after_n_consecutive: 64,
        strategy: ThrottleStrategy::Linear,
        priority: ThrottlePriority::Egress,
        ..Default::default()
    };
    socket.set_option(ADAPTIVE_THROTTLE, &config).await?;
    socket.connect("tcp://127.0.0.1:5555").await?;
    Ok(())
}
```

`Socket::with_throttle_config` does the same while building a socket:

```rust
let socket = ctx
    .socket(SocketType::Dealer)?
    .with_throttle_config(AdaptiveThrottleSocketConfig::default())
    .await?;
```

### Turning the Throttle On or Off

`ADAPTIVE_THROTTLE` also takes `1i32` or `0i32`, which changes `enabled` and leaves the other fields as they are. A workload that is already balanced can turn it off to skip the overhead:

```rust
use rzmq::socket::ADAPTIVE_THROTTLE;
use rzmq::{Context, SocketType, ZmqError};

async fn example(ctx: &Context) -> Result<(), ZmqError> {
    let socket = ctx.socket(SocketType::Push)?;
    socket.set_option(ADAPTIVE_THROTTLE, 0i32).await?;
    socket.connect("tcp://127.0.0.1:5555").await?;
    Ok(())
}
```

### Reading the Configuration

`get_option(ADAPTIVE_THROTTLE)` returns the encoded config. Decode it with `FromBytes`:

```rust
use rzmq::socket::types::FromBytes;
use rzmq::socket::{AdaptiveThrottleSocketConfig, ADAPTIVE_THROTTLE};

let config = AdaptiveThrottleSocketConfig::from_bytes(&socket.get_option(ADAPTIVE_THROTTLE).await?)?;
println!("Throttle enabled: {}", config.enabled);
```

A config set with `ThrottlePriority::Auto` reads back as `Auto`. The priority each connection resolved to is in the per-connection state below.

### Reading Per-Connection State

`ADAPTIVE_THROTTLE_STATS` is read-only and returns one `ThrottleStats` per connection that has a throttle. io_uring connections have no throttle and are not listed.

```rust
use rzmq::socket::types::FromBytes;
use rzmq::socket::{ThrottleStats, ADAPTIVE_THROTTLE_STATS};

let stats = Vec::<ThrottleStats>::from_bytes(&socket.get_option(ADAPTIVE_THROTTLE_STATS).await?)?;
for s in &stats {
    println!(
        "{} priority={:?} balance={} learned={:.1} streak in/out={}/{}",
        s.endpoint_uri, s.priority, s.current_balance, s.learned_balance,
        s.consecutive_ingress, s.consecutive_egress,
    );
}
```

Each entry's fields are read separately while the connection runs, so they are not a single consistent instant.

## Publishing to Slow Subscribers

A `PUB` never waits for a subscriber by default. Each subscriber has its own queue of up to `SNDHWM` messages. When that queue is full the subscriber misses the message and the other subscribers still get it:

```rust
use rzmq::socket::SNDHWM;
use rzmq::{Context, Msg, SocketType, ZmqError};

async fn example(ctx: &Context) -> Result<(), ZmqError> {
    let publisher = ctx.socket(SocketType::Pub)?;
    publisher.set_option(SNDHWM, 1000i32).await?;
    publisher.bind("tcp://127.0.0.1:5556").await?;
    // Returns once the message is queued for every subscriber that has room.
    publisher
        .send_multipart(vec![Msg::from_static(b"prices"), Msg::from_static(b"42")])
        .await?;
    Ok(())
}
```

A [monitor](#monitoring-socket-events) reports `ConnectionCongested` when a subscriber's queue fills and `ConnectionUncongested` when it drains.

### Waiting for Every Subscriber (`XPUB_NODROP`)

With `XPUB_NODROP` set, each send reaches every matched subscriber or none of them. A send that finds a full subscriber waits for room instead of dropping:

```rust
use rzmq::socket::XPUB_NODROP;

publisher.set_option(XPUB_NODROP, 1i32).await?;
```

A subscriber that disconnects stops holding sends back. Sends from cloned handles that have to wait complete in the order they started waiting.

### Bounding the Wait

`SNDTIMEO` decides how long a send waits for room. A send that gives up delivers to no subscriber. A send whose future is dropped delivers to none either:

```rust
use rzmq::socket::{SNDTIMEO, XPUB_NODROP};
use rzmq::{Msg, Socket, ZmqError};

async fn publish(publisher: &Socket, update: Vec<Msg>) -> Result<bool, ZmqError> {
    publisher.set_option(XPUB_NODROP, 1i32).await?;
    publisher.set_option(SNDTIMEO, 500i32).await?;
    match publisher.send_multipart(update).await {
        Ok(()) => Ok(true),
        // A subscriber stayed full for 500 ms; no subscriber received the update.
        Err(ZmqError::Timeout) => Ok(false),
        Err(e) => Err(e),
    }
}
```

| `SNDTIMEO` | A send that finds a full subscriber |
|---|---|
| `-1` (default) | waits until there is room or the subscriber disconnects |
| `0` | returns `ZmqError::ResourceLimitReached` at once |
| positive (ms) | returns `ZmqError::Timeout` when the time runs out |

### Reading the Setting

```rust
use rzmq::socket::XPUB_NODROP;

let raw = publisher.get_option(XPUB_NODROP).await?;
let nodrop = i32::from_ne_bytes(raw.try_into().unwrap()) != 0;
```

## Retrying a Request

A `REQ` alternates strictly between `send` and `recv`. A request that has not been answered stays outstanding: `recv` returns `ZmqError::Timeout` when `RCVTIMEO` expires, another `recv` waits for the same reply and `send` returns `ZmqError::InvalidState` until a reply has arrived. The `REP` going away changes none of this. Giving up on a request means closing the socket and asking again on a fresh one:

```rust
use rzmq::socket::RCVTIMEO;
use rzmq::{Context, Msg, SocketType, ZmqError};

async fn ask(ctx: &Context, endpoint: &str, question: Msg) -> Result<Option<Msg>, ZmqError> {
    let req = ctx.socket(SocketType::Req)?;
    req.set_option(RCVTIMEO, 250i32).await?;
    req.connect(endpoint).await?;
    req.send(question).await?;
    match req.recv().await {
        Ok(reply) => Ok(Some(reply)),
        Err(ZmqError::Timeout) => {
            // The request is still outstanding on this socket; the retry needs a new one.
            req.close().await?;
            Ok(None)
        }
        Err(e) => Err(e),
    }
}
```

### Sending Again on the Same Socket (`REQ_RELAXED` and `REQ_CORRELATE`)

With `REQ_RELAXED` set, a `send` while a reply is outstanding abandons that reply, discards any replies already queued and sends to the next peer. A reply to the abandoned request that arrives after the new send looks like any other reply, so set `REQ_CORRELATE` with it. Each request then carries a request-id frame that the `REP` echoes, and a reply with another id is discarded:

```rust
use rzmq::socket::{RCVTIMEO, REQ_CORRELATE, REQ_RELAXED};
use rzmq::{Msg, Socket, ZmqError};

async fn ask_with_retry(req: &Socket, question: Msg, attempts: usize) -> Result<Option<Msg>, ZmqError> {
    req.set_option(RCVTIMEO, 250i32).await?;
    req.set_option(REQ_RELAXED, 1i32).await?;
    req.set_option(REQ_CORRELATE, 1i32).await?;
    for _ in 0..attempts {
        req.send(question.clone()).await?;
        match req.recv().await {
            Ok(reply) => return Ok(Some(reply)),
            Err(ZmqError::Timeout) => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(None)
}
```

| Options set | A late reply to an abandoned request |
|---|---|
| none (default) | cannot happen: `send` returns `ZmqError::InvalidState` while a reply is outstanding |
| `REQ_RELAXED` | is returned as the reply to the new request if it arrives after the new send |
| `REQ_RELAXED` and `REQ_CORRELATE` | is discarded |

### Replying When the Requester Has Left

A `REP` holding a request keeps its turn when the requester disconnects. `recv` returns `ZmqError::InvalidState` until the reply is sent, and that `send` drops the reply and returns `Ok(())`. A server loop needs no special case:

```rust
use rzmq::{Msg, Socket, ZmqError};

async fn serve(rep: &Socket) -> Result<(), ZmqError> {
    loop {
        let request = rep.recv().await?;
        let reply = handle(request).await;
        // Succeeds whether or not the requester is still there.
        rep.send(reply).await?;
    }
}
```

### Correlating Through a ROUTER

A `REP` echoes the request-id frame on its own. A `ROUTER` with its default framing strips a leading empty delimiter and inserts one on reply, which does not fit an envelope of `[request id, delimiter]`. Set `AUTO_DELIMITER` to `false` on the `ROUTER` and echo every frame ahead of the body:

```rust
use rzmq::socket::AUTO_DELIMITER;
use rzmq::{Msg, Socket, ZmqError};

async fn serve_correlated(router: &Socket) -> Result<(), ZmqError> {
    router.set_option(AUTO_DELIMITER, false).await?;
    loop {
        // [identity, request id, empty delimiter, body]
        let mut frames = router.recv_multipart().await?;
        let body = frames.pop().unwrap();
        frames.push(handle(body).await);
        router.send_multipart(frames).await?;
    }
}
```

### Reading the Settings

```rust
use rzmq::socket::{REQ_CORRELATE, REQ_RELAXED};

let raw = req.get_option(REQ_RELAXED).await?;
let relaxed = i32::from_ne_bytes(raw.try_into().unwrap()) != 0;
let raw = req.get_option(REQ_CORRELATE).await?;
let correlate = i32::from_ne_bytes(raw.try_into().unwrap()) != 0;
```

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