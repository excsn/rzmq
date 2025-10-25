# rzmq: An Asynchronous Pure Rust ZeroMQ Implementation

[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

**rzmq** is an ongoing effort to build a high-performance, asynchronous pure Rust implementation of the ZeroMQ (ØMQ) messaging library. It leverages the [Tokio](https://tokio.rs/) runtime for its asynchronous capabilities and aims to provide a familiar ZeroMQ API within the modern Rust ecosystem.

**A primary focus of `rzmq` is to deliver leading performance on Linux.** By integrating an advanced `io_uring` backend with TCP Corking, `rzmq` **has demonstrated superior throughput and lower latency compared to other ZeroMQ implementations, including the C-based `libzmq`, in high-throughput benchmark scenarios.**

## Project Status: Beta ⚠️

**`rzmq` is currently in Beta.** While core functionality and significant performance advantages (on Linux with `io_uring`) are in place, users should be aware of the following:

*   **API Stability:** The public API is stabilizing but may still see minor refinements before a 1.0 release.
*   **Feature Scope:** The focus is on core ZMTP 3.1 compliance and popular patterns. **Full feature parity with all of `libzmq`'s extensive options and advanced behaviors is a non-goal.** Notably, **ZAP (ZeroMQ Authentication Protocol) is not supported and is not planned.**
*   **Interoperability:** `rzmq` aims for wire-level interoperability with `libzmq` for supported socket patterns using **NULL, PLAIN, and CURVE** security. The **Noise_XX** mechanism is specific to `rzmq` and will only interoperate with other `rzmq` instances.
*   **Testing Environment:** Primarily tested on **macOS (ARM & x86)** and **Linux (Kernel 6.x)**. The `io_uring` backend is Linux-specific and best tested on Kernel 6.x+. Windows is not currently supported.
*   **Performance:** While leading performance is a key achievement, comprehensive benchmarking across all diverse workloads and hardware is ongoing.
*   **Robustness:** Tested for common use cases; edge case hardening is continuous.

## Motivation

1.  **Pure Rust & Async Native:** Memory safety, seamless `async/await` integration with Tokio, and no C `libzmq` dependency.
2.  **High Performance on Linux:** Specifically designed to leverage `io_uring` for superior throughput and low latency, as demonstrated in benchmarks.
3.  **Flexible & Modern Security:** Provides interoperable security with `libzmq`'s traditional **CURVE** mechanism, while also offering the modern **Noise_XX** protocol as a high-performance alternative for `rzmq`-to-`rzmq` communication.
4.  **Learning & Innovation:** A platform to explore messaging system architecture in Rust.

## Current Features & Capabilities

`rzmq` (`core` crate) currently supports:

*   **Core ZeroMQ API:** `Context`, `Socket` handle with async `bind`, `connect`, `send`, `recv`, `set_option_raw`, `get_option`, `close`, `term`.
*   **Standard Socket Types:** `REQ`, `REP`, `PUB`, `SUB`, `PUSH`, `PULL`, `DEALER`, `ROUTER`.
*   **Transports:**
    *   `tcp://` (IPv4/IPv6), with an optional high-performance `io_uring` backend on Linux.
    *   `ipc://` (Unix Domain Sockets, `ipc` feature, Unix-like systems).
    *   `inproc://` (In-process, `inproc` feature).
*   **ZMTP 3.1 Protocol:** Core elements including Greeting, Framing, READY, PING/PONG.
*   **Common Socket Options:**
    *   Watermarks (`SNDHWM`, `RCVHWM`), Timeouts (`SNDTIMEO`, `RCVTIMEO`, `LINGER`), Reconnection (`RECONNECT_IVL`, `RECONNECT_IVL_MAX`), TCP Keepalives, `LAST_ENDPOINT`.
    *   Pattern-specific: `SUBSCRIBE`, `UNSUBSCRIBE`, `ROUTING_ID`, `ROUTER_MANDATORY`.
    *   ZMTP Heartbeats (`HEARTBEAT_IVL`, `HEARTBEAT_TIMEOUT`), `HANDSHAKE_IVL`.
    *   Security:
        *   `PLAIN_SERVER/USERNAME/PASSWORD` (`plain` feature)
        *   `CURVE_SERVER/SECRET_KEY/SERVER_KEY` (`curve` feature)
        *   `NOISE_XX_ENABLED/STATIC_SECRET_KEY/REMOTE_STATIC_PUBLIC_KEY` (`noise_xx` feature)
        *   Linux Performance (`io-uring` feature): `IO_URING_SESSION_ENABLED`, `TCP_CORK`, `IO_URING_SNDZEROCOPY`, `IO_URING_RCVMULTISHOT`.
*   **Socket Monitoring:** Event system (`Socket::monitor()`) for lifecycle events.
*   **Supported Security Mechanisms:**
    *   **NULL**: No security (default, interoperable).
    *   **PLAIN** (`plain` feature): Username/password authentication (interoperable).
    *   **CURVE** (`curve` feature): Public-key encryption, providing strong security and interoperability with `libzmq`.
    *   **Noise_XX** (`noise_xx` feature): A modern, high-performance security protocol for `rzmq`-to-`rzmq` communication (not interoperable with `libzmq`).

## Goals and Non-Goals

**Goals:**

*   **Stability and Robustness:** Achieve production-grade stability.
*   **Leading Performance:** Continue to optimize, especially the `io_uring` path on Linux.
*   **Ease of Use:** Provide a Rust-idiomatic and intuitive API.
*   **Modern Security:** Offer strong, modern security options like Noise_XX.
*   **Community and Documentation:** Foster an active community with clear documentation.

**Non-Goals:**

*   **Full `libzmq` Feature Parity:** Replicating every single feature and option of `libzmq` is not intended.
*   **Support for ZAP:** The ZAP (ZeroMQ Authentication Protocol) is not planned for implementation. Authentication is handled directly by the supported security mechanisms.

## When to Consider `rzmq` (Currently)

*   **Performance-Critical Linux Applications:** When seeking the highest possible messaging throughput and lowest latency, leveraging the `io_uring` backend.
*   **Pure Rust Environments:** To avoid C dependencies and benefit from Rust's safety and async ecosystem.
*   **Modern Security Needs:** If Noise_XX is a desired security protocol for `rzmq`-to-`rzmq` communication.
*   **Learning & Contribution:** For those interested in ZeroMQ internals, asynchronous Rust, `io_uring`, or contributing to a modern messaging library.

For applications requiring the broadest `libzmq` feature set (e.g., ZAP), or support for platforms beyond macOS/Linux, the official C `libzmq` (typically via Rust bindings like `zmq-rs`) remains the established choice.

## Structure

This repository may contain multiple crates:

*   `core/`: The main `rzmq` library implementation. (See `core/README.md` for detailed information about the library itself).
*   `cli/`: Command Line Utility to help generate NoiseXX keys. (See `cli/README.md` for detailed information about the cli itself).

## Getting Started

Please refer to the **[`core/README.md`](core/README.md)** for detailed installation instructions, prerequisites, API usage, and examples.

## License

`rzmq` is licensed under the Mozilla Public License Version 2.0 (MPL-2.0). This means you are free to use, modify, and distribute it under the terms of the MPL-2.0, which requires that modifications to MPL-licensed files be made available under the same license.