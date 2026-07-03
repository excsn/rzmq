# rzmq: Asynchronous Pure Rust ZeroMQ with io-uring and TCP Cork Acceleration

[![crates.io](https://img.shields.io/crates/v/rzmq.svg)](https://crates.io/crates/rzmq)
[![License: MPL-2.0](https://img.shields.io/badge/License-MPL%202.0-brightgreen.svg)](https://opensource.org/licenses/MPL-2.0)

**rzmq** is a high-performance, asynchronous pure Rust implementation of ZeroMQ (ØMQ) built on [Tokio](https://tokio.rs/).

Implements the ZMTP 2/3.1 wire protocol with familiar ZeroMQ socket patterns.

Supports an optional per-socket **dedicated** `io_uring` worker on Linux, allowing individual sockets to trade CPU for lower latency and higher throughput.

Delivers stunningly superior throughput and lower latency compared to every other ZeroMQ implementation as shown in high-throughput [benchmarks](#benchmarks) included.

***Fast, boring and correct.***

## Performance Highlights

TCP Loopback (`tcp://127.0.0.1`), PUSH/PULL Sockets, 10-second window, Linux release build on an AMD Ryzen 5 7640U Balanced Power Profile with Adaptive Throttling disabled.

Standard · 4 workers

- **3.5 M msg/s** - 64 B
- **~17 GB/s** - 32 KB · cork

io_uring + cork · 4 workers

- **6.5 M msg/s** - 64 B
- **7.9 GB/s** - 32 KB · multishot + zerocopy (600 second sustained)

**WARNING**: Always do your own testing for production use. Benchmarks tell a narrative against one environment and library configuration at a snapshot of time. Never trust any benchmarks especially library comparison benchmarks done over a short duration. Benchmarks are *always* out of date and these numbers are provided as tongue in cheek numbers: No universal guarantees ;).

## Project Status: Beta ⚠️

`rzmq` is currently in Beta, used in **long term Production** software.

See [`core/README.md`](core/README.md#project-status-beta-️) for full details.

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
