# rzmq - Async, Pure Rust ZeroMQ with Leading Performance on Linux

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ (ØMQ) messaging patterns, built on top of the [Tokio](https://tokio.rs/) runtime. It aims to provide a familiar ZeroMQ-style API within the Rust async ecosystem, **striving for wire-level interoperability with `libzmq` and other ZeroMQ implementations for core patterns and ZMTP 3.1 using NULL or PLAIN security.**

**A key design goal and demonstrated capability of `rzmq` is achieving exceptional performance on Linux.** Leveraging its advanced `io_uring` backend, **`rzmq` has shown superior throughput and lower latency compared to other ZeroMQ implementations, including the C-based `libzmq`, in high-throughput benchmark scenarios.** This makes `rzmq` a compelling choice for performance-critical distributed applications on Linux.

## Project Status: Beta ⚠️

**Please Note:** `rzmq` is currently in **Beta**. While core functionality and significant performance advantages (on Linux with `io_uring`) are in place, users should be aware of the following:

*   **API Stability:** The public API is stabilizing but may still see minor refinements before a 1.0 release.
*   **Feature Scope:** While major ZeroMQ patterns and options are supported, **full feature parity with all of `libzmq`'s extensive options and advanced behaviors (such as ZAP or CURVE security) is a non-goal.** The focus is on core ZMTP 3.1 compliance, popular patterns, and specific security mechanisms (NULL, PLAIN, and its own Noise_XX offering).
*   **Interoperability:** `rzmq` aims for wire-level interoperability with `libzmq` and other standard ZMTP 3.1 implementations for supported socket patterns using the **NULL or PLAIN** security mechanisms. The **Noise_XX** mechanism implemented in `rzmq` is specific to this library and will not interoperate with `libzmq`'s CURVE or other security layers.
*   **Testing Environment:**
    *   Core functionality has primarily been tested on **macOS (ARM & x86)** and **Linux (Kernel version 6.x)**.
    *   The high-performance `io_uring` backend is Linux-specific and has been developed and tested primarily against **Linux Kernel 6.x**. Functionality on older kernels supporting `io_uring` (e.g., 5.6+) may vary, especially for advanced features.
    *   Windows and other operating systems are **not currently supported or tested**.
*   **Performance Generalization:** While leading performance is demonstrated in specific benchmarks, comprehensive benchmarking across all diverse workloads and hardware configurations is ongoing.
*   **Robustness & Edge Cases:** The library has been tested for common use cases on the aforementioned platforms, but some edge cases or extreme conditions might not be as hardened as the mature `libzmq`.
*   **Security Mechanisms:** NULL and PLAIN security mechanisms are functional. **Noise_XX is also provided as a modern, robust alternative specific to `rzmq`. CURVE security and the ZAP (ZeroMQ Authentication Protocol) are not supported and are not planned for implementation.**

We encourage testing, feedback, and contributions to help mature the library towards a stable 1.0 release.

## Key Features

### Leading Performance on Linux (with `io_uring`)
*   **`io_uring` Backend**: On supported Linux systems, `rzmq`'s `io_uring` backend has demonstrated superior throughput and lower latency compared to other ZeroMQ implementations, including `libzmq`, in high-throughput benchmark scenarios. This is achieved by optimized syscall patterns, reduced data copying (especially with zerocopy send enabled), and efficient kernel-level I/O batching.
    *   Activated per-socket session using the `IO_URING_SESSION_ENABLED` socket option.
    *   Global `io_uring` parameters (ring size, default buffer pool parameters) are configured via `UringConfig` when calling `rzmq::uring::initialize_uring_backend()`.
*   **TCP Corking (Linux-only)**: Enabled via the `TCP_CORK` socket option, contributing to performance gains by batching smaller ZMTP frames for a single network write.

### Asynchronous & Pure Rust
Built entirely on Tokio for non-blocking I/O, with no dependency on the C `libzmq` library.

### Core API
Provides a `Context` for managing sockets and a `Socket` handle with async methods (`bind`, `connect`, `send`, `recv`, `set_option_raw`, `get_option`, `close`). A convenience `set_option` method is also available for types implementing the `ToBytes` trait.

### Supported Socket Types
*   Request-Reply: `REQ`, `REP`
*   Publish-Subscribe: `PUB`, `SUB`
*   Pipeline: `PUSH`, `PULL`
*   Asynchronous Req-Rep: `DEALER`, `ROUTER`

### Multiple Transports
*   **`tcp`**: Reliable TCP transport for network communication.
*   **`ipc`**: Inter-Process Communication via Unix Domain Sockets (requires `ipc` feature, Unix-like systems only).
*   **`inproc`**: In-process communication between threads within the same application (requires `inproc` feature).

### Advanced `io_uring` Optimizations (Experimental, Linux-only)
*   **Zerocopy Send**: (Requires `io-uring` feature)
    *   Individual socket sessions can request zero-copy sends by enabling the `IO_URING_SNDZEROCOPY` socket option.
    *   The `UringWorker` will only attempt to perform actual zero-copy operations if `UringConfig.default_send_zerocopy` was set to `true` *and* a send buffer pool was successfully configured (via `UringConfig.default_send_buffer_count` and `UringConfig.default_send_buffer_size`) during backend initialization. Otherwise, it falls back to standard sends.
    *   Aims to reduce CPU usage for message sending by using `send_zc` via `io_uring`, minimizing data copies.
*   **Multishot Receive**: (Requires `io-uring` feature)
    *   Individual socket sessions can request multishot receives by enabling the `IO_URING_RCVMULTISHOT` socket option.
    *   The `UringWorker` uses a global default receive buffer ring (for group ID 0) if buffer parameters (`default_recv_buffer_count`, `default_recv_buffer_size`) were valid during backend initialization. If this default ring isn't available, multishot requests might not be fulfilled as intended.
    *   Leverages `io_uring`'s multishot receive operations to submit multiple receive buffers to the kernel at once, potentially reducing syscall overhead.

### ZMTP 3.1 Protocol Basics
Implements core aspects of the ZeroMQ Message Transport Protocol version 3.1, including Greeting, Framing, READY command, and PING/PONG keepalives.

### Common Socket Options
Supports a range of common socket options for fine-tuning behavior, including:
*   High-Water Marks: `SNDHWM`, `RCVHWM`
*   Timeouts: `SNDTIMEO`, `RCVTIMEO`, `LINGER`
*   Connection: `RECONNECT_IVL`, `RECONNECT_IVL_MAX`, `HANDSHAKE_IVL`
*   TCP Keepalives: `TCP_KEEPALIVE`, `TCP_KEEPALIVE_IDLE`, `TCP_KEEPALIVE_CNT`, `TCP_KEEPALIVE_INTVL`
*   Binding: `LAST_ENDPOINT` (read-only, to get actual bound endpoint, e.g., after binding to port 0)
*   Pattern-specific: `SUBSCRIBE`, `UNSUBSCRIBE` (for SUB), `ROUTING_ID` (for DEALER/ROUTER identity), `ROUTER_MANDATORY`
*   Keepalives: ZMTP heartbeats (`HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`)
*   Security:
    *   `PLAIN_SERVER`, `PLAIN_USERNAME`, `PLAIN_PASSWORD`
    *   `NOISE_XX_ENABLED`, `NOISE_XX_STATIC_SECRET_KEY`, `NOISE_XX_REMOTE_STATIC_PUBLIC_KEY` (requires `noise_xx` feature)
*   Performance/Platform-Specific (`io-uring` feature, Linux-only):
    *   `IO_URING_SESSION_ENABLED` (to enable io_uring for a socket's connections)
    *   `TCP_CORK`
    *   `IO_URING_SNDZEROCOPY` (requests zero-copy for the socket session)
    *   `IO_URING_RCVMULTISHOT` (requests multishot receive for the socket session)

### Socket Monitoring
Offers an event channel via `Socket::monitor()` (or `monitor_default()`) to observe socket lifecycle events (e.g., connected, disconnected, bind failed, handshake events), similar to `zmq_socket_monitor`.

### Graceful Shutdown
Facilitates coordinated shutdown of the context and all associated sockets using `Context::term()`.

### Supported Security Mechanisms
*   **NULL**: No security (default). Interoperable with other ZeroMQ implementations using NULL.
*   **PLAIN**: Username/password based authentication. Interoperable with other ZeroMQ implementations using PLAIN.
*   **Noise_XX** (Experimental, requires `noise_xx` feature): Encrypted and authenticated sessions using the Noise Protocol Framework (XX handshake pattern). **This is a modern security mechanism specific to `rzmq` and is not part of the standard ZMTP security mechanisms found in `libzmq` (like CURVE). Therefore, Noise_XX in `rzmq` will only interoperate with other `rzmq` instances also configured for Noise_XX.**

## Installation

Add `rzmq` to your `Cargo.toml` dependencies. You will also need `tokio`.

```toml
[dependencies]
# Replace "..." with the desired version or Git source
rzmq = { git = "https://github.com/zeromq/rzmq.git", branch = "main" }

# Enable desired features:
# rzmq = { git = "...", features = ["ipc", "inproc", "noise_xx", "io-uring"] }

tokio = { version = "1", features = ["full"] } # "full" feature recommended for general use
```

**Available Cargo Features:**

*   `ipc`: Enables the `ipc://` transport (Unix-like systems only).
*   `inproc`: Enables the `inproc://` transport.
*   `noise_xx`: (Experimental) Enables the Noise_XX security mechanism (specific to `rzmq`).
*   `io-uring`: (Linux-only) Enables the `io_uring` backend for TCP transport and related optimizations.

**Prerequisites:**

*   **Rust & Cargo**: A recent stable version of Rust (e.g., 1.70+ recommended).
*   **Tokio**: `rzmq` is built on Tokio and expects a Tokio runtime.
*   **Operating System**:
    *   Core functionality: Tested on **macOS (ARM & x86)** and **Linux (Kernel 6.x recommended)**.
    *   `ipc` feature: Unix-like systems only.
    *   `io_uring` feature & `TCP_CORK` option: **Linux-only**.
*   **Modern Linux Kernel** (for `io_uring` feature):
    *   For basic `io_uring` functionality: Linux kernel 5.6+ is generally required.
    *   For advanced `io_uring` features used by `rzmq`:
        *   **Multishot Receive**: Kernel **6.0+** is recommended for reliable operation.
        *   **Send Zerocopy** (`IORING_OP_SEND_ZC`): Kernel 5.19+ for the opcode, but kernel **6.0+** is required for reliable completion notifications (`IORING_CQE_F_NOTIFY`).
    *   For `TCP_CORK`: This is a standard Linux TCP socket option available on most modern kernels.

## Getting Started / Documentation

For a detailed guide on using `rzmq`, including core concepts, examples, API overviews, and how to use features like `io_uring`, please see the **[Usage Guide (README.USAGE.md)](./README.USAGE.md)**.

The library includes an `examples/` directory in its repository showcasing various usage patterns.

The full API reference documentation can be generated locally using `cargo doc --open`.

A brief example (Push/Pull):
```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

// On Linux with "io-uring" feature for rzmq:
// #[cfg(all(target_os = "linux", feature = "io-uring"))]
// #[tokio::main]
// async fn main() -> Result<(), ZmqError> {
//    // For io_uring, initialize the backend first
//    rzmq::uring::initialize_uring_backend(Default::default())?;
//    // /* ... rest of the example ... */
//    rzmq::uring::shutdown_uring_backend().await?;
//    Ok(())
// }

// Otherwise (or if io-uring feature is not used):
#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    let ctx = Context::new()?;

    let push = ctx.socket(SocketType::Push)?;
    let pull = ctx.socket(SocketType::Pull)?;

    let endpoint = "inproc://example"; // "inproc" requires the "inproc" feature
    pull.bind(endpoint).await?;
    tokio::time::sleep(Duration::from_millis(10)).await;
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
*(Note: The example uses `inproc` which requires the `inproc` feature enabled for `rzmq`.)*

## Missing Features / Limitations (Known)

*   **Limited ZMQ Option Parity:** Many `libzmq` options are not implemented (e.g., various buffer size controls, `ZMQ_IMMEDIATE`, detailed multicast options). **Full parity with all `libzmq` options is a non-goal.**
*   **Unsupported Standard Security Mechanisms:**
    *   **CURVE encryption (from `libzmq`) is not supported and not planned for implementation.** `rzmq` offers Noise_XX as its primary modern, robust authenticated encryption mechanism.
    *   **ZAP (ZeroMQ Authentication Protocol) is not supported and not planned for implementation.** Authentication needs are addressed by the supported mechanisms (PLAIN, Noise_XX).
*   **`zmq_poll` Equivalent:** No direct high-level equivalent. Tokio's `select!` macro or task management should be used for concurrent operations on multiple sockets.
*   **`zmq_proxy` Equivalent:** No built-in high-level proxy function.
*   **Advanced Pattern Options:** Behavior for some advanced options (e.g., certain `ZMQ_ROUTER_*` flags, `SUB` forwarding) needs full verification and implementation if deemed in scope.
*   **Performance:**
    *   While `io_uring` support shows leading performance in specific benchmarks, `rzmq` has not undergone exhaustive performance optimization or direct benchmarking against `libzmq` across *all* scenarios and socket types.
*   **Error Handling Parity:** The mapping of internal errors to specific `ZmqError` variants corresponding to all `zmq_errno()` values may not be exhaustive.
*   **Robustness:** Edge cases, high-concurrency stress, diverse network failure modes, and very long-running stability require more extensive testing.

## Running Tests

```bash
# Run default tests (standard Tokio backend, no optional features)
cargo test

# Run tests enabling specific transport features (e.g., IPC and Inproc)
cargo test --features "ipc,inproc"

# Run tests with the io_uring backend (on Linux)
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
```
Some benchmarks, like `generic_client_benchmark`, may require specific configurations or peer processes to be running. Refer to the benchmark source or examples for setup instructions.

## License

This project is licensed under the **Mozilla Public License Version 2.0 (MPL-2.0)**. See the `LICENSE` file in the repository for the full license text.

Thank you for your interest in `rzmq`