# rzmq: An Asynchronous Pure Rust ZeroMQ Implementation

[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)
[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)

**rzmq** is an ongoing effort to build a high-performance, asynchronous, and pure Rust implementation of the ZeroMQ (ØMQ) messaging library. It leverages the [Tokio](https://tokio.rs/) runtime for its asynchronous capabilities and aims to provide a familiar ZeroMQ API within the modern Rust ecosystem.

## Project Status: Experimental ⚠️

**This project is currently experimental and under active development.** It is **not yet production-ready** and should be used with the understanding that APIs may change, features may be incomplete, and stability may not match that of the official `libzmq`. We are actively working towards greater stability and feature parity.

## Motivation

The ZeroMQ library (`libzmq`) is a fantastic piece of engineering that has powered countless distributed systems. However, several motivations drive the development of `rzmq`:

1.  **Pure Rust Implementation:**
    *   **Safety:** Leverage Rust's memory safety and concurrency guarantees to reduce common bugs found in C/C++ codebases.
    *   **Ecosystem Integration:** Provide a native Rust library that integrates seamlessly with the burgeoning Rust async ecosystem (Tokio, async/await) without FFI overhead or `unsafe` block proliferation.
    *   **Build Simplicity:** Eliminate the need for users to have the C `libzmq` library and its development headers installed on their system, simplifying the build process for Rust projects.

2.  **Asynchronous Core:**
    *   Design from the ground up for asynchronous operation using Tokio, aiming for high concurrency and efficient resource utilization in async Rust applications.

3.  **Exploring Modern I/O: `io_uring` Acceleration (Linux):**
    *   A key goal is to leverage modern Linux kernel I/O interfaces like `io_uring` for potentially significant performance improvements in high-throughput scenarios. `rzmq` includes an optional `io_uring` backend (via `tokio-uring`) for its TCP transport, aiming to reduce syscall overhead and enable more efficient data transfer.

4.  **Learning and Innovation:**
    *   Reimplementing a complex system like ZeroMQ in a different language and paradigm offers valuable learning opportunities and a chance to explore alternative architectural choices within the Rust ecosystem.

## Current Features & Capabilities

`rzmq` currently supports:

*   **Core ZeroMQ API:** `Context` and `Socket` abstractions with asynchronous methods for `bind`, `connect`, `send`, `recv`, setting/getting options, and graceful `close`/`term`.
*   **Standard Socket Types:**
    *   Request-Reply: `REQ`, `REP`
    *   Publish-Subscribe: `PUB`, `SUB` (with basic topic filtering)
    *   Pipeline: `PUSH`, `PULL`
    *   Asynchronous Request-Reply: `DEALER`, `ROUTER` (with identity handling)
*   **Transports:**
    *   `tcp://` (IPv4 and IPv6)
    *   `ipc://` (Inter-Process Communication on Unix-like systems, via feature flag)
    *   `inproc://` (In-Process communication between threads, via feature flag)
    *   Optional `io_uring` backend for TCP on Linux (via feature flag and `tokio-uring`).
*   **ZMTP 3.1 Basics:** Implementation of the core ZeroMQ Message Transport Protocol, including greeting, message framing, and basic commands (READY, PING/PONG).
*   **Common Socket Options:** Support for essential options like `SNDHWM`, `RCVHWM`, `LINGER`, send/receive timeouts, reconnection intervals, TCP keepalives, and ZMTP heartbeats.
*   **Socket Monitoring:** An event system to monitor socket lifecycle events (connects, disconnects, errors).
*   **Security Placeholders:** Basic infrastructure for NULL, PLAIN, and CURVE security mechanisms is present, but full, robust implementations (especially CURVE and ZAP) are still under development.

## Goals and Roadmap

While still experimental, the long-term goals for `rzmq` include:

*   **Stability and Robustness:** Achieve a level of stability suitable for consideration in less critical production environments, eventually targeting full production readiness.
*   **Feature Parity:** Gradually implement more features and socket options found in `libzmq`.
*   **Performance Optimization:** Continuously improve performance, including deeper integration of `io_uring` features (e.g., zerocopy operations) and general code optimization.
*   **Comprehensive Security:** Fully implement and test CURVE security and ZAP authentication.
*   **Community Building:** Foster a community around the project for contributions, feedback, and usage.

## When to Consider `rzmq` (Currently)

*   **Learning & Exploration:** If you're interested in the internals of ZeroMQ or asynchronous Rust networking.
*   **Prototyping:** For projects where a pure Rust, async ZeroMQ-like library is desired, and the experimental nature is acceptable.
*   **Specific `io_uring` Interest:** If you are keen on experimenting with an `io_uring`-backed messaging layer in Rust.
*   **Contributing:** If you'd like to help build a modern ZeroMQ alternative in Rust.

For production-critical applications requiring the full stability and feature set of ZeroMQ, the official `libzmq` (often via Rust bindings like `zmq-rs`) remains the recommended choice at this time.

## Getting Involved

*   **Rust Developers:** To use `rzmq` in your Rust project, see the [Installation](#installation) section in the Rust-specific [README.md](README.md) and the [Usage Guide](README.USAGE.md).
*   **Source Code:** (Link to your GitHub repository)
*   **Issue Tracker:** (Link to your GitHub issues)
*   **Contributing:** Contributions are highly encouraged! Whether it's bug reports, feature suggestions, documentation improvements, or code contributions, please feel free to get involved. See `CONTRIBUTING.md` (if available) for guidelines.

## License

`rzmq` is licensed under the Mozilla Public License Version 2.0 (MPL-2.0). This means you are free to use, modify, and distribute it under the terms of the MPL-2.0, which requires that modifications to MPL-licensed files be made available under the same license.