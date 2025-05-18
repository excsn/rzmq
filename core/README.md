# rzmq - Async, Pure Rust ZeroMQ

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ (ØMQ) messaging patterns, built on top of the [Tokio](https://tokio.rs/) runtime. It aims to provide a familiar ZeroMQ-style API within the Rust async ecosystem, allowing developers to build distributed and concurrent applications without a dependency on the C `libzmq` library.

## Project Status: Experimental ⚠️

**Please Note:** This project is currently **experimental** and under active development.

*   It is **NOT** yet production-ready.
*   It does **NOT** have full feature parity with `libzmq`.
*   The API may still change.
*   While core patterns are implemented, robustness, performance, and error handling might not match `libzmq` in all edge cases.

Use with caution and expect potential bugs or missing features. Contributions are welcome!

## Key Features

### Asynchronous & Pure Rust
Built entirely on Tokio for non-blocking I/O, with no dependency on the C `libzmq` library.

### Core API
Provides a `Context` for managing sockets and a `Socket` handle with async methods (`bind`, `connect`, `send`, `recv`, `set_option`, `get_option`, `close`).

### Supported Socket Types
*   Request-Reply: `REQ`, `REP`
*   Publish-Subscribe: `PUB`, `SUB`
*   Pipeline: `PUSH`, `PULL`
*   Asynchronous Req-Rep: `DEALER`, `ROUTER`

### Multiple Transports
*   **`tcp`**: Reliable TCP transport for network communication.
*   **`ipc`**: Inter-Process Communication via Unix Domain Sockets (requires `ipc` feature, Unix-like systems only).
*   **`inproc`**: In-process communication between threads within the same application (requires `inproc` feature).
*   **`io-uring` (Optional, Linux-only)**: Enables an `io_uring`-based backend for TCP transport via `tokio-uring` for potentially higher performance. Requires using the `#[rzmq::main]` attribute. (Requires `io-uring` feature).

### ZMTP 3.1 Protocol Basics
Implements core aspects of the ZeroMQ Message Transport Protocol version 3.1, including Greeting, Framing, READY command, and PING/PONG keepalives.

### Common Socket Options
Supports a range of common socket options for fine-tuning behavior, such as:
*   High-Water Marks: `SNDHWM`, `RCVHWM`
*   Timeouts: `SNDTIMEO`, `RCVTIMEO`, `LINGER`
*   Connection: `RECONNECT_IVL`, `RECONNECT_IVL_MAX`, `TCP_KEEPALIVE`
*   Pattern-specific: `SUBSCRIBE`, `UNSUBSCRIBE` (for SUB), `ROUTING_ID` (for DEALER/ROUTER), `ROUTER_MANDATORY`
*   Keepalives: ZMTP heartbeats (`HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`)

### Socket Monitoring
Offers an event channel via `Socket::monitor()` to observe socket lifecycle events (e.g., connected, disconnected, bind failed), similar to `zmq_socket_monitor`.

### Graceful Shutdown
Facilitates coordinated shutdown of the context and all associated sockets using `Context::term()`.

### Basic Security Placeholders
Includes infrastructure for NULL, PLAIN, and CURVE security mechanisms, though full implementations (especially for CURVE and ZAP) are still under development. (CURVE support requires `curve` feature).

## Installation

Add `rzmq` to your `Cargo.toml` dependencies. You will also need `tokio`.

```toml
[dependencies]
# Replace "..." with the desired version or Git source
# rzmq = "0.1.0" # Example for a published version
# rzmq = { git = "https://github.com/your-username/rzmq.git", branch = "main" } # Example for Git

# To enable specific features:
rzmq = { version = "...", features = ["ipc", "inproc", "curve", "io-uring"] }

tokio = { version = "1", features = ["full"] } # "full" feature recommended
```

**Available Cargo Features:**

*   `ipc`: Enables the `ipc://` transport (Unix-like systems only).
*   `inproc`: Enables the `inproc://` transport.
*   `curve`: Enables basic infrastructure for CURVE security (requires `libsodium-rs`). The implementation is experimental.
*   `io-uring`: (Linux-only) Enables the `io_uring` backend for TCP transport. Requires using `#[rzmq::main]` (see [Usage Guide](README.USAGE.md#using-io_uring-linux)).

**Prerequisites:**

*   **Rust & Cargo**: A recent stable version of Rust.
*   **Tokio**: `rzmq` is built on Tokio and expects a Tokio runtime.
*   **Libsodium** (for `curve` feature): If using the `curve` feature, the libsodium development library must be installed on your system.
*   **Modern Linux Kernel** (for `io-uring` feature): A Linux kernel supporting `io_uring` (typically 5.1+).

## Getting Started / Documentation

For a detailed guide on using `rzmq`, including core concepts, examples, API overviews, and how to use features like `io_uring`, please see the **[Usage Guide (README.USAGE.md)](README.USAGE.md)**.

The library may include an `examples/` directory in its repository showcasing various usage patterns.

The full API reference documentation is available on [docs.rs/rzmq](https://docs.rs/rzmq) (link will be active once published).

A brief example (Push/Pull):
```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

// Use #[rzmq::main] if io-uring feature is enabled and on Linux
// #[cfg(all(target_os = "linux", feature = "io-uring"))]
// #[rzmq::main]
// async fn main() -> Result<(), ZmqError> { ... }

// Otherwise, use #[tokio::main]
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?;

    let push = ctx.socket(SocketType::Push)?;
    let pull = ctx.socket(SocketType::Pull)?;

    let endpoint = "inproc://example";
    pull.bind(endpoint).await?;
    push.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;

    push.send(Msg::from_static(b"Hello rzmq!")).await?;
    let received = pull.recv().await?;
    assert_eq!(received.data().unwrap_or_default(), b"Hello rzmq!");
    println!("Received: {}", String::from_utf8_lossy(received.data().unwrap_or_default()));

    ctx.term().await?;
    Ok(())
}
```

## Missing Features / Limitations (Known)

*   **Full ZMQ Option Parity:** Many `libzmq` options are not yet implemented (e.g., `ZMQ_LAST_ENDPOINT`, various buffer size controls, `ZMQ_IMMEDIATE`).
*   **Complete Security:** CURVE cryptography and full ZAP (ZeroMQ Authentication Protocol) are not fully implemented or robust.
*   **`zmq_poll` Equivalent:** No direct high-level equivalent for polling multiple sockets. Users can manage multiple socket futures using Tokio's `select!`.
*   **`zmq_proxy` Equivalent:** No built-in high-level proxy function.
*   **Advanced Pattern Options:** Behavior for some advanced options (e.g., certain `ZMQ_ROUTER_*` flags) needs full verification and implementation.
*   **Performance:** While built on Tokio, `rzmq` has not yet undergone heavy performance optimization or benchmarking against `libzmq`. Advanced `io_uring` features like explicit zerocopy are not yet deeply integrated.
*   **Error Handling Parity:** The mapping of internal errors to specific `ZmqError` variants corresponding to all `zmq_errno()` values may not be exhaustive.
*   **Robustness:** Edge cases, high-concurrency scenarios, and complex network failure conditions require more extensive testing.

## Running Tests

```bash
# Run default tests
cargo test

# Run tests enabling specific features
cargo test --features "ipc,inproc"
cargo test --features "io-uring" # On Linux
```

## License

This project is licensed under the **Mozilla Public License Version 2.0 (MPL-2.0)**. See the `LICENSE` file in the repository for the full license text.

## Contributing

Contributions are welcome! If you find bugs, have feature requests, or want to improve the implementation, please:

1.  **Open an Issue:** Discuss the change or report the bug on the project's issue tracker.
2.  **Fork the Repository.**
3.  **Create a Feature Branch.**
4.  **Implement Changes & Add Tests.**
5.  **Run `cargo test --all-features` (or relevant feature combinations) and `cargo fmt`.**
6.  **Submit a Pull Request** with a clear description of your changes.

Thank you for your interest in `rzmq`!