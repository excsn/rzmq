# rzmq API Reference

This document provides a quick reference to the main public API components of the `rzmq` library.

## Note

You must call close() on any socket to release resources associated.

## Overview

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ patterns built on Tokio. It uses an actor model internally.

## Getting Started

1.  **Create a Context:** All sockets are created from a `Context`.
2.  **Create Sockets:** Use `context.socket()` to create sockets of specific types.
3.  **Use Socket Methods:** Interact with sockets using async methods like `bind`, `connect`, `send`, `recv`.
4.  **Terminate Context:** Call `context.term().await` for graceful shutdown when done.

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    // 1. Create Context
    let ctx = Context::new()?;

    // 2. Create Sockets
    let push = ctx.socket(SocketType::Push)?;
    let pull = ctx.socket(SocketType::Pull)?;

    // 3. Use Socket Methods
    pull.bind("inproc://example").await?;
    push.connect("inproc://example").await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow connection

    push.send(Msg::from_static(b"Hello")).await?;
    let received = pull.recv().await?;
    println!("Received: {:?}", received.data());

    // 4. Terminate Context
    ctx.term().await?;

    Ok(())
}
```

## `Context`

Manages sockets and shared resources. It's cheap to clone (`Arc`-based).

*   **`Context::new() -> Result<Self, ZmqError>`**
    ```rust
    pub fn new() -> Result<Self, ZmqError>
    ```
    Creates a new, independent rzmq context.

*   **`context.socket(socket_type: SocketType) -> Result<Socket, ZmqError>`**
    ```rust
    pub fn socket(&self, socket_type: SocketType) -> Result<Socket, ZmqError>
    ```
    Creates a new `Socket` of the specified `SocketType` associated with this context.

*   **`context.term().await -> Result<(), ZmqError>`**
    ```rust
    pub async fn term(self) -> Result<(), ZmqError>
    ```
    Initiates a graceful shutdown of the context and all associated sockets. Waits for all background tasks to complete. Consumes the `Context` handle. Must be awaited.

## `Socket`

The public handle for interacting with a specific socket instance. It's cheap to clone (`Arc`-based). All operations are asynchronous.

*   **`socket.bind(endpoint: &str).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn bind(&self, endpoint: &str) -> Result<(), ZmqError>
    ```
    Binds the socket to listen on a local endpoint (e.g., `"tcp://127.0.0.1:5555"`, `"ipc:///tmp/zmq.sock"`, `"inproc://my_service"`).

*   **`socket.connect(endpoint: &str).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn connect(&self, endpoint: &str) -> Result<(), ZmqError>
    ```
    Connects the socket to a remote or local endpoint. Handles connection asynchronously in the background.

*   **`socket.disconnect(endpoint: &str).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn disconnect(&self, endpoint: &str) -> Result<(), ZmqError>
    ```
    Disconnects from a previously connected endpoint.

*   **`socket.unbind(endpoint: &str).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn unbind(&self, endpoint: &str) -> Result<(), ZmqError>
    ```
    Stops listening on a previously bound endpoint.

*   **`socket.send(msg: Msg).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn send(&self, msg: Msg) -> Result<(), ZmqError>
    ```
    Sends a message according to the socket's pattern. For multi-part messages, set `MsgFlags::MORE` on all but the last part using `msg.set_flags()`. May return `ZmqError::ResourceLimitReached` (EAGAIN equivalent) if HWM is hit and `SNDTIMEO` is 0, or `ZmqError::Timeout` if `SNDTIMEO` expires.

*   **`socket.recv().await -> Result<Msg, ZmqError>`**
    ```rust
    pub async fn recv(&self) -> Result<Msg, ZmqError>
    ```
    Receives a message according to the socket's pattern. Blocks asynchronously until a message is available or an error occurs. May return `ZmqError::Timeout` if `RCVTIMEO` expires. Check `msg.is_more()` for multi-part messages.

*   **`socket.set_option(option: i32, value: &[u8]).await -> Result<(), ZmqError>`**
    ```rust
    pub async fn set_option(&self, option: i32, value: &[u8]) -> Result<(), ZmqError>
    ```
    Sets a socket option. Use constants from `rzmq::socket::options`. Value format depends on the option (e.g., `i32.to_ne_bytes()` for integer options).

*   **`socket.get_option(option: i32).await -> Result<Vec<u8>, ZmqError>`**
    ```rust
    pub async fn get_option(&self, option: i32) -> Result<Vec<u8>, ZmqError>
    ```
    Gets a socket option value. Returns bytes which need to be interpreted based on the option.

*   **`socket.close().await -> Result<(), ZmqError>`**
    ```rust
    pub async fn close(&self) -> Result<(), ZmqError>
    ```
    Initiates graceful shutdown of *this specific socket*. Waits for associated background tasks to complete.

*   **`socket.monitor(capacity: usize).await -> Result<MonitorReceiver, ZmqError>`**
    ```rust
    pub async fn monitor(&self, capacity: usize) -> Result<MonitorReceiver, ZmqError>
    ```
    Creates a channel to receive `SocketEvent` notifications about this socket's lifecycle (connections, disconnections, errors, etc.). `capacity` sets the event buffer size.

*   **`socket.monitor_default().await -> Result<MonitorReceiver, ZmqError>`**
    ```rust
    pub async fn monitor_default(&self) -> Result<MonitorReceiver, ZmqError>
    ```
    Creates a monitoring channel with a default capacity.

## `SocketType` (Enum)

Specifies the messaging pattern when creating a socket with `context.socket()`.

*   `Pub`: Publisher (Pub-Sub)
*   `Sub`: Subscriber (Pub-Sub)
*   `Req`: Request (Req-Rep)
*   `Rep`: Reply (Req-Rep)
*   `Dealer`: Asynchronous Req-Rep (Dealer-Router)
*   `Router`: Asynchronous Req-Rep (Dealer-Router)
*   `Push`: Fan-out distributor (Push-Pull)
*   `Pull`: Fan-in collector (Push-Pull)

## `Msg`

Represents a single message part (frame). Uses `bytes::Bytes` internally for efficiency.

*   `Msg::new()`: Creates an empty message.
*   `Msg::from_vec(data: Vec<u8>)`: Creates a message from a `Vec`.
*   `Msg::from_static(data: &'static [u8])`: Creates a message from a static slice (zero-copy).
*   `msg.data() -> Option<&[u8]>`: Gets a reference to the message payload.
*   `msg.size() -> usize`: Gets the payload size.
*   `msg.flags() -> MsgFlags`: Gets the current flags.
*   `msg.set_flags(flags: MsgFlags)`: Sets the flags (e.g., `MsgFlags::MORE`).
*   `msg.is_more() -> bool`: Checks if the `MORE` flag is set.
*   `msg.metadata() -> &Metadata`: Access message metadata (rarely needed by end-users).

## `MsgFlags` (Bitflags)

Flags associated with a `Msg`.

*   `MsgFlags::MORE`: Indicates more message parts follow this one in a multi-part message.

## `ZmqError` (Enum)

Error type returned by most API calls. Variants correspond to common network and ZeroMQ errors (e.g., `Io`, `Timeout`, `AddrInUse`, `ConnectionRefused`, `InvalidState`, `ResourceLimitReached`, `ProtocolViolation`).

## Socket Options

Set/get using `socket.set_option()` / `socket.get_option()` with integer constants found in `rzmq::socket::options`. Common examples:

*   `SNDHWM`, `RCVHWM`: High-water marks (queue limits).
*   `LINGER`: Time to wait for pending messages on close.
*   `SNDTIMEO`, `RCVTIMEO`: Send/receive timeouts in milliseconds (-1 = infinite, 0 = immediate, >0 = timeout).
*   `RECONNECT_IVL`, `RECONNECT_IVL_MAX`: Reconnection intervals.
*   `SUBSCRIBE`, `UNSUBSCRIBE`: For `Sub` sockets.
*   `ROUTING_ID`: Socket identity (e.g., for `Dealer`, `Router`).
*   `TCP_KEEPALIVE`, `TCP_KEEPALIVE_IDLE`, etc.: TCP keepalive settings.
*   `HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`: ZMTP keepalive settings.

*Note: Option values are typically passed as `&i32.to_ne_bytes()`.*

## Monitoring

Use `socket.monitor()` to get a `MonitorReceiver` channel. Receive `SocketEvent` enum variants from this channel to track socket activity asynchronously.

*   **`MonitorReceiver`**: An `async_channel::Receiver<SocketEvent>`. Use `receiver.recv().await`.
*   **`SocketEvent`**: Enum detailing events like `Listening`, `Connected`, `Disconnected`, `Accepted`, `BindFailed`, `HandshakeSucceeded`, `HandshakeFailed`, etc. (See `rzmq::socket::events` for variants).

## Version

*   **`rzmq::version() -> (i32, i32, i32)`**: Returns the library version (major, minor, patch).

This reference covers the primary user-facing parts of the `rzmq` library based on the provided code.