# rzmq: An Asynchronous Pure Rust ZeroMQ Implementation

[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

**rzmq** is a high-performance, asynchronous pure Rust implementation of the ZeroMQ (ØMQ) messaging library. It leverages the [Tokio](https://tokio.rs/) runtime for its asynchronous capabilities alongside an optional io_uring worker and aims to provide a familiar ZeroMQ API.

**The primary focus of `rzmq` is to deliver leading performance on Linux.**

## Performance Highlights

TCP Loopback (`tcp://127.0.0.1`), 10-second window, release build on an AMD Ryzen 5 7640U Balanced Power Profile with Adaptive Throttling disabled.

- **3.2 M msg/s** - PushPull · 64 B · Linux · 4 workers
- **8.7 GB/s** - PushPull · 32 KB · Linux · 4 workers

- **3.2 M msg/s** - PushPull · 64 B · Linux · io\_uring + cork · 4 workers
- **7.3 GB/s** - PushPull · 32 KB · Linux · io\_uring + cork + multishot + zerocopy · 8 workers

 By integrating `io_uring` with TCP Cork, `rzmq` **has demonstrated stunningly superior throughput and lower latency compared to every other ZeroMQ implementation, including the C-based `libzmq`, in high-throughput [benchmarks](#benchmarks) included in this repository.**

## Project Status: Beta ⚠️

**`rzmq` is currently in Beta.** While core functionality and significant performance advantages (on Linux with `io_uring`) are in place, users should be aware of the following:

*   **API Stability:** The public API is stabilizing but may still see minor refinements before a 1.0 release.
*   **Feature Scope:** The focus is on core ZMTP 3.1 compliance and popular patterns. **Full feature parity with all of `libzmq`'s extensive options and advanced behaviors is a non-goal.** Notably, **ZAP (ZeroMQ Authentication Protocol) is not supported and is not planned.**
*   **Interoperability:** `rzmq` aims for wire-level interoperability with `libzmq` for supported socket patterns using **NULL, PLAIN, and CURVE** security. The **Noise_XX** mechanism is specific to `rzmq` and will only interoperate with other `rzmq` instances.
*   **Testing Environment:** Primarily tested on **macOS (ARM & x86)** and **Linux (Kernel 6.x)**. The `io_uring` backend is Linux-specific and best tested on Kernel 6.x+. Windows is not currently supported.
*   **Performance:** While leading performance is a key achievement, comprehensive benchmarking across all diverse workloads and hardware is desired.
*   **Robustness:** Tested for common use cases; edge case hardening is a continuous effort.

## Notable Users

[Hi Stakes Markets Game](https://www.histakesgame.com) -  The world's most advanced financial simulator, available on iPhone and Android.

## Motivation

1.  **Pure Rust & Async Native:** Memory safety, seamless `async/await` integration with Tokio, and no C `libzmq` dependency.
2.  **High Performance on Linux:** Specifically designed to provide `io_uring` and TCP Cork for superior throughput and low latency, as demonstrated in benchmarks.
3.  **Flexible & Modern Security:** Provides interoperable security with `libzmq`'s traditional **CURVE** mechanism, while also offering the modern **Noise_XX** protocol as a high-performance alternative for `rzmq`-to-`rzmq` communication.

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
*   **Adaptive I/O Throttling:** A unique built-in fairness engine (not present in `libzmq`) that probabilistically balances ingress vs. egress work per connection, preventing starvation under asymmetric load. Uses static function-pointer dispatch for a zero-overhead bypass when disabled. Fully configurable or toggled via the `ADAPTIVE_THROTTLE` socket option / `Socket::with_throttle_config()`.
*   **Socket Monitoring:** Event system (`Socket::monitor()`) for lifecycle events.
*   **Supported Security Mechanisms:**
    *   **NULL**: No security (default, interoperable).
    *   **PLAIN** (`plain` feature): Username/password authentication (interoperable).
    *   **CURVE** (`curve` feature): Public-key encryption, providing strong security and interoperability with `libzmq`.
    *   **Noise_XX** (`noise_xx` feature): A modern, high-performance security protocol for `rzmq`-to-`rzmq` communication (not interoperable with `libzmq`).

## When to Consider `rzmq` (Currently)

*   **Performance-Critical Linux Applications:** When seeking the highest possible messaging throughput and lowest latency, leveraging `io_uring` and TCP Cork.
*   **Pure Rust Environments:** To avoid C dependencies and benefit from Rust's safety and async ecosystem.
*   **Modern Security Needs:** If Noise_XX is a desired security protocol for `rzmq`-to-`rzmq` communication.
*   **Learning & Contribution:** For those interested in ZeroMQ internals, asynchronous Rust, `io_uring`, or contributing to a modern messaging library.

For applications requiring the broadest `libzmq` feature set (e.g., ZAP), or support for platforms beyond macOS/Linux, the official C `libzmq` (typically via Rust bindings like `zmq-rs`) remains the established choice.

## Structure

This repository may contain multiple crates:

*   `core/`: The main `rzmq` library implementation. (See `core/README.md` for detailed information about the library itself).
*   `cli/`: Command Line Utility to help generate NoiseXX keys. (See `cli/README.md` for detailed information about the cli itself).
*   `bench/`: Standalone benchmarking tool for measuring throughput and latency.

## Getting Started

Please refer to the **[`core/README.md`](core/README.md)** for detailed installation instructions, prerequisites, API usage, and examples.

## Benchmarks

Full results across all patterns and configurations are in [`bench/docs/`](bench/docs/):

| Platform | Results |
|---|---|
| Linux (AMD Ryzen 5 7640U) | [`bench/docs/linux_bench.md`](bench/docs/linux_bench.md) |
| macOS (Apple M4) | [`bench/docs/mac_bench.md`](bench/docs/mac_bench.md) |

See the [`bench/`](bench/) crate for instructions on running benchmarks yourself.

## License

`rzmq` is licensed under the Mozilla Public License Version 2.0 (MPL-2.0). This means you are free to use, modify, and distribute it under the terms of the MPL-2.0, which requires that modifications to MPL-licensed files be made available under the same license.