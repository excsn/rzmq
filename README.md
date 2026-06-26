# rzmq: Asynchronous Pure Rust ZeroMQ with io-uring and TCP Cork Acceleration

[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

**rzmq** is a high-performance, asynchronous pure Rust implementation of ZeroMQ (ØMQ) built on [Tokio](https://tokio.rs/). It implements the ZMTP 3.1 wire protocol with familiar ZeroMQ socket patterns and an opt-in `io_uring` worker for ultra low latency, high throughput on Linux.

## Performance Highlights

TCP Loopback (`tcp://127.0.0.1`), PUSH/PULL Sockets, 10-second window, Linux release build on an AMD Ryzen 5 7640U Balanced Power Profile with Adaptive Throttling disabled.

- **3.5 M msg/s** - 64 B · 4 workers
- **16.2 GB/s** - 32 KB · 4 workers

- **5.3 M msg/s** - 64 B · io\_uring + cork · 4 workers
- **7.8 GB/s** - 32 KB · io\_uring + cork + multishot + zerocopy · 4 workers

`rzmq` delivers stunningly superior throughput and lower latency compared to every other ZeroMQ implementation, including the C-based `libzmq`, in high-throughput [benchmarks](#benchmarks) included.

## Project Status: Beta ⚠️

`rzmq` is currently in Beta. See [`core/README.md`](core/README.md#project-status-beta-️) for full details.

## Notable Users

[Hi Stakes Markets Game](https://www.histakesgame.com) - The world's most advanced financial simulator, available on iPhone and Android.

## Structure

*   `core/`: The main `rzmq` library. See [`core/README.md`](core/README.md) for full documentation, installation, API usage, and examples.
*   `cli/`: Command-line utility for generating Noise_XX keys. See [`cli/README.md`](cli/README.md).
*   `bench/`: Standalone benchmarking tools. See [`bench/`](bench/).

## Getting Started

Please refer to **[`core/README.md`](core/README.md)** for installation instructions, prerequisites, API usage, and examples.

## Benchmarks

Full results across all patterns and configurations are in [`bench/docs/`](bench/docs/):

| Platform | Results |
|---|---|
| Linux (AMD Ryzen 5 7640U) | [`bench/docs/linux_bench.md`](bench/docs/linux_bench.md) |
| macOS (Apple M4) | [`bench/docs/mac_bench.md`](bench/docs/mac_bench.md) |

See the [`bench/`](bench/) crate for instructions on running benchmarks yourself.

## License

`rzmq` is licensed under the Mozilla Public License Version 2.0 (MPL-2.0). You are free to use, modify, and distribute it under the terms of the MPL-2.0, which requires that modifications to MPL-licensed files be made available under the same license.
