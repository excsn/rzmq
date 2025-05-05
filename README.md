# RZMQ - Async, Pure Rust ZeroMQ

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
<!-- Add other badges as appropriate: CI status, crates.io version, etc. -->
<!-- [![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq) -->
<!-- [![Build Status](...)]() -->

`rzmq` is an asynchronous, pure-Rust implementation of ZeroMQ (ØMQ) messaging patterns, built on top of the [Tokio](https://tokio.rs/) runtime. It aims to provide a familiar ZeroMQ-style API within the Rust async ecosystem.

## Project Status: Experimental ⚠️

**Please Note:** This project is currently **experimental** and under active development.

*   It is **NOT** yet production-ready.
*   It does **NOT** have full feature parity with `libzmq`.
*   The API may still change.
*   While core patterns are implemented, robustness, performance, and error handling might not match `libzmq` in all edge cases.

Use with caution and expect potential bugs or missing features. Contributions are welcome!

## Features

*   **Asynchronous:** Built entirely on Tokio for non-blocking I/O.
*   **Pure Rust:** No dependency on the C `libzmq` library.
*   **Core API:**
    *   `Context` for managing sockets.
    *   `Socket` handle with async methods (`bind`, `connect`, `send`, `recv`, `set_option`, `get_option`, `close`).
*   **Socket Types:**
    *   Request-Reply: `REQ`, `REP`
    *   Publish-Subscribe: `PUB`, `SUB`
    *   Pipeline: `PUSH`, `PULL`
    *   Asynchronous Req-Rep: `DEALER`, `ROUTER`
*   **Transports:**
    *   `tcp`: TCP transport.
    *   `ipc`: Inter-Process Communication via Unix Domain Sockets (requires `ipc` feature).
    *   `inproc`: In-process communication between threads (requires `inproc` feature).
*   **Basic ZMTP 3.1:** Implements core protocol aspects (Greeting, Framing, READY, PING/PONG).
*   **Basic Socket Options:** Supports common options like `SNDHWM`, `RCVHWM`, `LINGER`, `SNDTIMEO`, `RCVTIMEO`, `SUBSCRIBE`, `UNSUBSCRIBE`, `ROUTING_ID`, reconnection intervals, TCP keepalives, ZMTP heartbeats.
*   **Socket Monitoring:** Provides an event channel similar to `zmq_socket_monitor`.
*   **Graceful Shutdown:** `Context::term()` allows for coordinated shutdown.
*   **Basic Security:** Infrastructure for NULL, PLAIN, and CURVE security mechanisms (implementations are placeholders or basic).

## Missing Features / Limitations (Known)

*   **Full ZMQ Option Parity:** Many `libzmq` options are not yet implemented (e.g., `ZMQ_LAST_ENDPOINT`, buffer sizes, `ZMQ_IMMEDIATE`).
*   **Complete Security:** CURVE crypto and ZAP authentication are not fully implemented.
*   **`zmq_poll` Equivalent:** No built-in mechanism for polling multiple sockets simultaneously.
*   **`zmq_proxy` Equivalent:** No high-level proxy function.
*   **Advanced Pattern Options:** Options like `ZMQ_ROUTER_MANDATORY`, `ZMQ_ROUTER_HANDOVER` behavior needs full verification/implementation.
*   **Performance:** Performance has not been heavily optimized or benchmarked against `libzmq`. Zero-copy operations are limited.
*   **Error Handling Parity:** Mapping of internal errors to specific `ZmqError` variants corresponding to `zmq_errno()` may not be exhaustive.
*   **Robustness:** Edge cases, high-concurrency scenarios, and complex failure conditions need more testing.

## Installation

Add `rzmq` to your `Cargo.toml`. You likely also need `tokio`.

```toml
[dependencies]
# If using git:
# rzmq = { git = "repository-url", branch = "main" }
# Once published:
# rzmq = "0.1.0" # Replace with actual version

# Enable transport features as needed:
# rzmq = { version = "...", features = ["ipc", "inproc"] }

tokio = { version = "1", features = ["full"] } # "full" recommended for ease
tracing = "0.1" # Optional: for logging
tracing-subscriber = { version = "0.3", features = ["env-filter"] } # Optional: for logging
```

**Features:**

*   `ipc`: Enables the `ipc://` transport (Unix only).
*   `inproc`: Enables the `inproc://` transport.
*   `curve`: Enables placeholders for CURVE security (requires `libsodium-rs`).

## Usage Example (Push/Pull)

```rust
use rzmq::{Context, SocketType, Msg, ZmqError};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), ZmqError> {
    // Initialize logging (optional)
    // tracing_subscriber::fmt::init();

    println!("Creating context...");
    let ctx = Context::new()?;

    println!("Creating PUSH and PULL sockets...");
    let push = ctx.socket(SocketType::Push)?;
    let pull = ctx.socket(SocketType::Pull)?;

    let endpoint = "inproc://push-pull-example"; // Use inproc for simple example

    println!("Binding PULL to {}...", endpoint);
    pull.bind(endpoint).await?;

    println!("Connecting PUSH to {}...", endpoint);
    push.connect(endpoint).await?;

    // Allow time for in-process connection (usually very fast)
    tokio::time::sleep(Duration::from_millis(50)).await;

    const NUM_MESSAGES: usize = 5;
    println!("PUSH: Sending {} messages...", NUM_MESSAGES);
    for i in 0..NUM_MESSAGES {
        let msg_str = format!("Message {}", i);
        println!("  Sending: {}", msg_str);
        push.send(Msg::from_vec(msg_str.into_bytes())).await?;
    }
    println!("PUSH: Finished sending.");

    println!("PULL: Receiving messages...");
    for i in 0..NUM_MESSAGES {
        match pull.recv().await {
            Ok(msg) => {
                let received_str = String::from_utf8_lossy(msg.data().unwrap_or(b""));
                println!("  Received: {}", received_str);
                assert_eq!(received_str, format!("Message {}", i));
            }
            Err(e) => {
                eprintln!("PULL: Error receiving message {}: {}", i, e);
                return Err(e);
            }
        }
    }
    println!("PULL: Finished receiving.");

    // Terminate context gracefully
    println!("Terminating context...");
    ctx.term().await?;
    println!("Context terminated.");

    Ok(())
}
```

## API Overview

*   **`rzmq::Context`**: Manages sockets. Create using `Context::new()`. Terminate with `context.term().await`.
*   **`rzmq::Socket`**: Handle to a socket. Created via `context.socket(SocketType::...)`. Offers async methods like `.bind()`, `.connect()`, `.send()`, `.recv()`, `.set_option()`, `.get_option()`, `.close()`, `.monitor()`.
*   **`rzmq::SocketType`**: Enum defining the socket pattern (`Pub`, `Sub`, `Req`, `Rep`, `Push`, `Pull`, `Dealer`, `Router`).
*   **`rzmq::Msg`**: Represents a message frame. Use `Msg::from_vec()`, `Msg::from_static()`, etc. Check/set flags with `.flags()`, `.set_flags()`, `.is_more()`. Access data with `.data()`.
*   **`rzmq::ZmqError`**: Error enum returned by API calls.
*   **`rzmq::socket::options`**: Contains constants for socket options.
*   **`rzmq::socket::SocketEvent`**: Enum received from the monitor channel.

(See the code or generated documentation for detailed function signatures).

## Running Tests

```bash
# Run all tests (some may be skipped if features are disabled)
cargo test

# Run tests enabling specific features
cargo test --features ipc,inproc
```

## License

This project is licensed under the **Mozilla Public License Version 2.0 (MPL-2.0)**.

See the [LICENSE](LICENSE) file for details. (You will need to create a `LICENSE` file containing the MPL 2.0 text). MPL 2.0 is a copyleft license that applies at the file level.

## Contributing

Contributions are welcome! If you find bugs, have feature requests, or want to improve the implementation, please:

1.  **Open an Issue:** Discuss the change or report the bug.
2.  **Fork the Repository:** Create your own fork.
3.  **Create a Branch:** Make your changes on a separate branch.
4.  **Run Tests:** Ensure `cargo test` (with relevant features) passes.
5.  **Format Code:** Run `cargo fmt`.
6.  **Submit a Pull Request:** Describe your changes clearly.

Thank you for your interest in `rzmq`!