# rzmq - Async, Pure Rust ZeroMQ

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ (ØMQ) messaging patterns, built on top of the [Tokio](https://tokio.rs/) runtime. It aims to provide a familiar ZeroMQ-style API within the Rust async ecosystem, allowing developers to build distributed and concurrent applications without a dependency on the C `libzmq` library.

## Project Status: Experimental ⚠️

**Please Note:** This project is currently **experimental** and under active development.

*   It is **NOT** yet production-ready.
*   It does **NOT** have full feature parity with `libzmq` and may not be interoperable yet.
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

### Performance Enhancements (Optional)
*   **`io_uring` Backend (Linux-only)**: (Requires `io-uring` feature) Enables an `io_uring`-based backend for TCP transport via `tokio-uring`. This can offer higher performance and lower latency by leveraging Linux's advanced asynchronous I/O interface. Requires using the `#[tokio::main]` attribute on the application's main function.
*   **TCP Corking (Linux-only)**: When enabled via the `TCP_CORK_OPT` socket option, attempts to batch smaller ZMTP frames by setting the `TCP_CORK` socket option on the underlying TCP stream. This can reduce the number of TCP segments sent, potentially improving efficiency for multi-part messages or frequent small messages.
*   **Zerocopy Send (Experimental, with `io_uring`)**: (Requires `io-uring` feature and `IO_URING_SNDZEROCOPY` option, Linux-only) Aims to reduce CPU usage and improve throughput for message sending by using `sendmsg` with `MSG_ZEROCOPY` or `send_vectored_zc` (via `tokio-uring`), minimizing data copies from userspace to the kernel for network transmission.
*   **Multishot Receive (Experimental, with `io_uring`)**: (Requires `io-uring` feature and `IO_URING_RCVMULTISHOT` option, Linux-only) Leverages `io_uring`'s multishot receive operations (`IORING_OP_RECVMSG_MULTISHOT` or `IORING_OP_RECV_MULTISHOT`) to submit multiple receive buffers to the kernel at once, potentially reducing syscall overhead for high-message-rate scenarios. Buffer pool size and individual buffer capacity are configurable via `IO_URING_RECV_BUFFER_COUNT` and `IO_URING_RECV_BUFFER_SIZE` options.

### ZMTP 3.1 Protocol Basics
Implements core aspects of the ZeroMQ Message Transport Protocol version 3.1, including Greeting, Framing, READY command, and PING/PONG keepalives.

### Common Socket Options
Supports a range of common socket options for fine-tuning behavior, including:
*   High-Water Marks: `SNDHWM`, `RCVHWM`
*   Timeouts: `SNDTIMEO`, `RCVTIMEO`, `LINGER`
*   Connection: `RECONNECT_IVL`, `RECONNECT_IVL_MAX`, `TCP_KEEPALIVE` options
*   Binding: `LAST_ENDPOINT` (read-only, to get actual bound endpoint, e.g., after binding to port 0)
*   Pattern-specific: `SUBSCRIBE`, `UNSUBSCRIBE` (for SUB), `ROUTING_ID` (for DEALER/ROUTER), `ROUTER_MANDATORY`
*   Keepalives: ZMTP heartbeats (`HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`)
*   Performance/Platform-Specific:
    *   `TCP_CORK_OPT` (Linux)
    *   `IO_URING_SNDZEROCOPY` (Linux, `io-uring` feature)
    *   `IO_URING_RCVMULTISHOT` (Linux, `io-uring` feature)
    *   `IO_URING_RECV_BUFFER_COUNT` (Linux, `io-uring` feature)
    *   `IO_URING_RECV_BUFFER_SIZE` (Linux, `io-uring` feature)

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
rzmq = { git = "https://github.com/your-username/rzmq.git", branch = "main" } # Or your specific version/fork

# Enable desired features:
# rzmq = { version = "...", features = ["ipc", "inproc", "curve", "io-uring"] }

tokio = { version = "1", features = ["full"] } # "full" feature recommended for general use
# For io-uring, rzmq's io-uring feature will pull in a compatible tokio-uring.
```

**Available Cargo Features:**

*   `ipc`: Enables the `ipc://` transport (Unix-like systems only).
*   `inproc`: Enables the `inproc://` transport.
*   `curve`: Enables basic infrastructure for CURVE security (requires `libsodium-rs`). The implementation is experimental.
*   `io-uring`: (Linux-only) Enables the `io_uring` backend for TCP transport and related optimizations like Zerocopy Send and Multishot Receive. Requires using `#[tokio::main]` (see [Usage Guide (README.USAGE.md)](README.USAGE.md#using-io_uring-linux-specific)). This feature also makes `TCP_CORK_OPT`, `IO_URING_SNDZEROCOPY`, `IO_URING_RCVMULTISHOT`, `IO_URING_RECV_BUFFER_COUNT`, and `IO_URING_RECV_BUFFER_SIZE` options available.

**Prerequisites:**

*   **Rust & Cargo**: A recent stable version of Rust (e.g., 1.70+ recommended).
*   **Tokio**: `rzmq` is built on Tokio and expects a Tokio runtime.
*   **Libsodium** (for `curve` feature): If using the `curve` feature, the libsodium development library must be installed on your system. See [libsodium-rs documentation](https://docs.rs/libsodium-rs/) for details.
*   **Modern Linux Kernel** (for `io-uring` feature, `TCP_CORK_OPT`):
    *   For `io_uring`: A Linux kernel version that supports `io_uring` (typically 5.1+ for basic features, 5.6+ for more stable/performant operations, newer for advanced features like multishot receive - e.g., 5.19+ for `IORING_OP_RECVMSG_MULTISHOT`, 6.0+ for `IORING_OP_RECV_MULTISHOT`).
    *   For `TCP_CORK`: This is a standard Linux TCP socket option available on most modern kernels.

## Getting Started / Documentation

For a detailed guide on using `rzmq`, including core concepts, examples, API overviews, and how to use features like `io_uring`, please see the **[Usage Guide (README.USAGE.md)](README.USAGE.md)** (you'll need to create this file).

The library may include an `examples/` directory in its repository showcasing various usage patterns.

The full API reference documentation can be generated locally using `cargo doc --open`. (A link to [docs.rs/rzmq](https://docs.rs/rzmq) will be active once published).

A brief example (Push/Pull):
```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

// On Linux with "io-uring" feature for rzmq:
// #[cfg(all(target_os = "linux", feature = "io-uring"))]
// #[tokio::main]
// async fn main() -> Result<(), ZmqError> { /* ... */ }

// Otherwise (or if io-uring feature is not used):
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?;

    let push = ctx.socket(SocketType::Push)?;
    let pull = ctx.socket(SocketType::Pull)?;

    let endpoint = "inproc://example"; // "inproc" requires the "inproc" feature
    pull.bind(endpoint).await?;
    push.connect(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(50)).await; // Allow connections to establish

    push.send(Msg::from_static(b"Hello rzmq!")).await?;
    let received = pull.recv().await?;
    assert_eq!(received.data().unwrap_or_default(), b"Hello rzmq!");
    println!("Received: {}", String::from_utf8_lossy(received.data().unwrap_or_default()));

    // Cleanly shut down the context and all associated sockets
    ctx.term().await?;
    Ok(())
}
```
*(Note: The example uses `inproc` which requires the `inproc` feature enabled for `rzmq`.)*

## Missing Features / Limitations (Known)

*   **Full ZMQ Option Parity:** Many `libzmq` options are not yet implemented (e.g., various buffer size controls, `ZMQ_IMMEDIATE`, detailed multicast options).
*   **Complete Security:** CURVE cryptography and full ZAP (ZeroMQ Authentication Protocol) are not yet production-ready.
*   **`zmq_poll` Equivalent:** No direct high-level equivalent. Tokio's `select!` macro or task management should be used for concurrent operations on multiple sockets.
*   **`zmq_proxy` Equivalent:** No built-in high-level proxy function.
*   **Advanced Pattern Options:** Behavior for some advanced options (e.g., certain `ZMQ_ROUTER_*` flags, `SUB` forwarding) needs full verification and implementation.
*   **Performance:**
    *   While `io_uring` support is added for potential gains, `rzmq` has not undergone extensive performance optimization or direct benchmarking against `libzmq` across all scenarios.
    *   Advanced `io_uring` buffer management (like registered buffers for multishot receive) is not yet implemented.
*   **Error Handling Parity:** The mapping of internal errors to specific `ZmqError` variants corresponding to all `zmq_errno()` values may not be exhaustive.
*   **Robustness:** Edge cases, high-concurrency stress, diverse network failure modes, and very long-running stability require more extensive testing.

## Running Tests

```bash
# Run default tests (standard Tokio backend, no optional features)
cargo test

# Run tests enabling specific transport features (e.g., IPC and Inproc)
cargo test --features "ipc,inproc"

# Run tests with the io_uring backend (on Linux)
# This will also enable tests that might be specific to io_uring behavior.
cargo test --features "io-uring"

# Run all tests with all available features
cargo test --all-features
```

## Benchmarks

Benchmarks are located in the `core/benches` directory and can be run using Criterion:

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark (e.g., PUSH/PULL throughput)
cargo bench --bench pull_throughput

# Run benchmarks with specific features (e.g., io_uring for multishot_recv_bench)
cargo bench --features "io-uring" --bench multishot_recv_bench
```
Some benchmarks, like `generic_client_benchmark`, may require environment variables for configuration (e.g., target server address, socket type).

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