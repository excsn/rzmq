# rzmq: An Asynchronous Pure Rust ZeroMQ Implementation

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)

**rzmq** is an ongoing effort to build a high-performance, asynchronous, and pure Rust implementation of the ZeroMQ (ØMQ) messaging library. It leverages the [Tokio](https://tokio.rs/) runtime for its asynchronous capabilities and aims to provide a familiar ZeroMQ API within the modern Rust ecosystem.

## Project Status: Experimental ⚠️

**This project is currently experimental and under active development.** It is **not yet production-ready** and should be used with the understanding that APIs may change, features may be incomplete, and stability may not match that of the official `libzmq`. We are actively working towards greater stability and feature parity.

## Motivation

The ZeroMQ library (`libzmq`) is a robust and widely adopted messaging solution. `rzmq` aims to offer a modern alternative within the Rust ecosystem, driven by several key motivations:

1.  **Pure Rust Implementation:**
    *   **Memory Safety & Concurrency:** Capitalize on Rust's strong safety guarantees to build a reliable messaging library.
    *   **Seamless Async Integration:** Provide a native asynchronous experience using Tokio and `async/await`, avoiding FFI complexities.
    *   **Simplified Build Process:** Eliminate the external dependency on the C `libzmq` library for Rust projects.

2.  **Asynchronous-First Design:**
    *   Architected from the ground up for non-blocking I/O and high concurrency, tailored for async Rust applications.

3.  **Exploring Modern I/O for High Performance (Linux):**
    *   A primary goal is to harness advanced Linux kernel I/O interfaces. `rzmq` features an optional **`io_uring` backend** (via `tokio-uring`) for its TCP transport. This backend aims to reduce syscall overhead and improve data transfer efficiency.
    *   Further performance enhancements include:
        *   **TCP Corking:** An option to batch ZMTP frames into fewer TCP segments, potentially reducing network overhead for certain workloads on Linux.
        *   **Experimental Zerocopy Send:** When using the `io_uring` backend, an experimental option enables a zerocopy send path, aiming to minimize CPU data copies for outgoing messages.

4.  **Learning and Innovation:**
    *   Reimplementing ZeroMQ offers a platform to explore protocol design and asynchronous patterns within Rust, fostering learning and potential innovation in messaging library architecture.

## Current Features & Capabilities

`rzmq` currently supports:

*   **Core ZeroMQ API:** A `Context` for managing sockets and `Socket` handles providing asynchronous methods for `bind`, `connect`, `send`, `recv`, option management (`set_option`/`get_option`), and lifecycle control (`close`/`term`).
*   **Standard Socket Types:**
    *   Request-Reply: `REQ`, `REP`
    *   Publish-Subscribe: `PUB`, `SUB` (with basic topic prefix filtering)
    *   Pipeline: `PUSH`, `PULL`
    *   Asynchronous Request-Reply: `DEALER`, `ROUTER` (with ZMTP identity handling)
*   **Transports:**
    *   `tcp://` (IPv4 and IPv6) with an optional `io_uring` backend on Linux.
    *   `ipc://` (Inter-Process Communication on Unix-like systems, via `ipc` feature flag).
    *   `inproc://` (In-Process communication between threads, via `inproc` feature flag).
*   **ZMTP 3.1 Protocol:** Implementation of core protocol elements including Greeting, message Framing, and essential commands like READY, PING/PONG for keepalives.
*   **Common Socket Options:** Support for key options such as:
    *   High-Water Marks (`SNDHWM`, `RCVHWM`)
    *   Timeouts (`SNDTIMEO`, `RCVTIMEO`, `LINGER`)
    *   Reconnection behavior (`RECONNECT_IVL`, `RECONNECT_IVL_MAX`)
    *   TCP-specifics (`TCP_KEEPALIVE` settings)
    *   ZMTP heartbeats (`HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`)
    *   Pattern-specific (`SUBSCRIBE`, `UNSUBSCRIBE`, `ROUTING_ID`, `ROUTER_MANDATORY`)
    *   Performance-related (Linux): `TCP_CORK_OPT`, `IO_URING_SNDZEROCOPY` (when `io-uring` feature is active).
*   **Socket Monitoring:** An event system (`Socket::monitor()`) for observing socket lifecycle events (connections, disconnections, errors, etc.).
*   **Basic Security Placeholders:** Infrastructure for NULL, PLAIN, and (feature-gated) CURVE security mechanisms. Full implementation and hardening are ongoing.

## Goals and Roadmap

While still experimental, the long-term goals for `rzmq` include:

*   **Stability and Robustness:** Mature into a reliable library suitable for a widening range of applications, eventually targeting production-grade stability.
*   **Feature Parity:** Systematically implement more features and socket options found in `libzmq` to provide a comprehensive ZeroMQ experience.
*   **Performance Optimization:**
    *   Continuously improve performance through code optimization and benchmarking.
    *   Deepen `io_uring` integration: Fully leverage features like zerocopy send/receive, registered buffers, and multishot operations where beneficial.
    *   Refine TCP Corking and other transport-level optimizations.
*   **Comprehensive Security:** Complete and rigorously test CURVE security and ZAP (ZeroMQ Authentication Protocol) integration.
*   **Community and Documentation:** Grow an active user and contributor community, supported by thorough documentation and examples.

## When to Consider `rzmq` (Currently)

*   **Learning & Exploration:** For those interested in ZeroMQ internals, asynchronous Rust, or modern I/O interfaces like `io_uring`.
*   **Prototyping & Non-Critical Use Cases:** When a pure Rust, `async`-native ZeroMQ-like library is appealing, and the experimental nature is acceptable.
*   **Early Adoption of `io_uring`:** For developers specifically looking to experiment with or contribute to an `io_uring`-backed messaging layer in Rust.
*   **Contributing:** If you are passionate about messaging systems and want to help shape a new ZeroMQ implementation in Rust.

For production-critical systems demanding the proven stability and full feature set of ZeroMQ, the official C `libzmq` (typically accessed via Rust bindings like `zmq-rs`) remains the established choice at this time.

## Getting Involved

*   **For Rust Developers:** To integrate `rzmq` into your Rust project, please refer to the Rust-specific `README.md` (often found in the crate's root or a `rust/` subdirectory if this is a multi-language project overview) and the detailed `README.USAGE.md`. These documents cover installation, features, and API usage from a Rust perspective.
*   **Source Code:** [https://github.com/excsn/rzmq](https://github.com/excsn/rzmq) (Replace with your actual repository URL)
*   **Issue Tracker:** (Link to your GitHub issues for bug reports and feature requests)
*   **Contributing:** Contributions are highly encouraged! Whether it's bug reports, feature suggestions, documentation improvements, or code contributions, please feel free to get involved. Check for a `CONTRIBUTING.md` file in the repository for guidelines.

## License

`rzmq` is licensed under the Mozilla Public License Version 2.0 (MPL-2.0). This means you are free to use, modify, and distribute it under the terms of the MPL-2.0, which requires that modifications to MPL-licensed files be made available under the same license.