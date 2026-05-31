# `rzmq_bench` Benchmarking Tool: Architecture and Operations Manual

This document provides a technical overview and operating instructions for the `rzmq_bench` tool. The tool is designed to measure the latency and throughput characteristics of the `rzmq` library across various transports, socket patterns, and I/O backends.

---

## 1. Architectural Overview

`rzmq_bench` provides a highly isolated testing environment by supporting both orchestrated multi-process runs on a single host and manual distributed runs across separate physical hosts. It separates execution profiles into two distinct architectural streams:

1.  **Standard Native Transport (Cross-Platform):** Built on top of the default Tokio asynchronous runtime, leveraging the host operating system's native event-polling interface.
2.  **`io_uring` Transport (Linux-Specific):** An optional, high-performance transport path designed to minimize system call overhead and memory copying using Linux kernel asynchronous I/O queues.

---

## 2. Transport Models

### A. Standard Native Transport (Cross-Platform)

This transport path is available on all supported operating systems. It delegates socket management to the standard Tokio runtime, which internally leverages the platform’s native, highly optimized event-driven notification reactor:

*   **Linux:** Resolves internally to `epoll`.
*   **macOS / BSD:** Resolves internally to `kqueue`.
*   **Windows:** Resolves internally to Input/Output Completion Ports (`IOCP`).

Within this native stream, `rzmq_bench` supports three transport schemes:

*   **TCP (`tcp://`):** Uses standard OS network sockets. On Linux, this path can optionally be configured with `TCP_CORK` to group small adjacent packets before flushing to the wire.
*   **In-Process (`inproc://`):** Employs lock-free memory channels to transfer messages between sockets residing in the same workspace. This bypasses the OS network stack entirely, eliminating context switches and network serialization overhead.
*   **Inter-Process Communication (`ipc://`):** Available on Unix-like operating systems (Linux, macOS, BSD). It utilizes Unix Domain Sockets for local inter-process communication, avoiding the overhead of the loopback TCP network layer.

### B. High-Performance `io_uring` Transport (Linux-Specific)

On modern Linux kernels, the `io_uring` backend bypasses the standard reactor-poll-syscall cycle. Instead, it submits I/O requests directly to the kernel submission queue (SQ) and reaps completions from the completion queue (CQ). It features two advanced performance options:

*   **Zero-Copy Send (`--uring-zerocopy`):** Maps pages from a pre-registered send buffer pool directly into the network device's DMA engine, avoiding memory copies between user space and kernel space during transmit operations.
*   **Multishot Receive (`--uring-multishot`):** Issues a single, persistent read request to the kernel. As data arrives, the kernel automatically assigns a buffer from a pre-registered receive buffer ring to hold the payload and pushes a completion entry, eliminating the need to re-submit read requests after every receive event.

 ### ⚠️ Operational Constraint: TCP Corking vs. Latency Workloads

> The `--cork` flag utilizes the Linux-specific `TCP_CORK` socket option to aggregate multiple small frames into a single physical network packet.

*   **Recommended Use Cases:** Only use `--cork` with unidirectional, high-throughput streaming workloads (such as **`push-pull`** or **`pub-sub`**). Under these patterns, the high message volume continuously saturates the TCP buffer, triggering highly efficient, consolidated kernel flushes.
*   **Do Not Use With:** Any synchronous, latency-bound ping-pong workloads (such as **`req-rep`** or **`dealer-router`**). Because these patterns restrict the pipeline to a single message in-flight, the socket buffer cannot be saturated. The kernel will hold your single 64-byte request in memory until its delayed-flush timer expires, causing round-trip latencies to spike to **~400ms** and dropping throughput to **~2 msg/s**.

---

## 3. Socket Patterns & Workloads

`rzmq_bench` implements specific workload engines mapped to standard ZeroMQ socket patterns to test distinct performance bottlenecks:

| Pattern | Sockets Used | Primary Focus | Workload Behavior |
| :--- | :--- | :--- | :--- |
| **`ReqRep`** | `REQ` (Client) / `REP` (Server) | Latency (RTT) | **Ping-Pong:** Client sends a message, records a timestamp, and blocks until the server echoes the message back. |
| **`PushPull`** | `PUSH` (Client) / `PULL` (Server) | Throughput (Streaming) | **Unidirectional Stream:** Client streams messages at maximum speed to saturate the queue. Server drains and discards. |
| **`DealerRouter`**| `DEALER` (Client) / `ROUTER` (Server) | Multipart/Asynchronous | **Vectored Ping-Pong:** Client sends a message, server echoes it back. Tests handling of multipart frames and routing identities. |
| **`PubSub`** | `PUB` (Client) / `SUB` (Server) | Fan-out/Filtering | **Filtered Stream:** Client publishes messages. Server subscribes with an empty prefix (`""`) and drains all matching traffic. |

---

## 4. Command Execution Matrix

All commands assume execution in release mode from the workspace root. If running in a workspace, append the binary flag: `--bin rzmq_bench`.

### A. Cross-Platform Transport

This transport leverages the default Tokio event loop (which resolves to `epoll` on Linux, `kqueue` on macOS/BSD, and `IOCP` on Windows).

#### 1. TCP Transport (`tcp://`)

*   **PUSH / PULL (Throughput Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --duration 10
    ```
*   **REQ / REP (Latency Ping-Pong):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --duration 10
    ```
*   **DEALER / ROUTER (Multipart RTT):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64 --duration 10
    ```
*   **PUB / SUB (Fan-out Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --duration 10
    ```

#### 2. In-Process Transport (`inproc://`)

*   **PUSH / PULL (Throughput Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint inproc://bench_stream --pattern push-pull --msg-size 64 --duration 10
    ```
*   **REQ / REP (Latency Ping-Pong):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint inproc://bench_latency --pattern req-rep --msg-size 64 --duration 10
    ```
*   **DEALER / ROUTER (Multipart RTT):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint inproc://bench_async --pattern dealer-router --msg-size 64 --duration 10
    ```
*   **PUB / SUB (Fan-out Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint inproc://bench_pubsub --pattern pub-sub --msg-size 64 --duration 10
    ```

#### 3. Inter-Process Communication (IPC - `ipc://` - Unix-only)

*   **PUSH / PULL (Throughput Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint ipc:///tmp/rzmq_push_pull --pattern push-pull --msg-size 64 --duration 10
    ```
*   **REQ / REP (Latency Ping-Pong):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint ipc:///tmp/rzmq_req_rep --pattern req-rep --msg-size 64 --duration 10
    ```
*   **DEALER / ROUTER (Multipart RTT):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint ipc:///tmp/rzmq_dealer_router --pattern dealer-router --msg-size 64 --duration 10
    ```
*   **PUB / SUB (Fan-out Stream):**
    ```bash
    cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint ipc:///tmp/rzmq_pub_sub --pattern pub-sub --msg-size 64 --duration 10
    ```

---

### B. Linux-only Transport

#### 1. Standard TCP with TCP_CORK Enabled
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --duration 10 --cork
```

#### 2. CPU Pinning (`--pin-cpus`)

By default, child processes are allowed to float across any available CPU core. Passing `--pin-cpus` instructs the orchestrator to pin the server process to core 0 and the client process to core 1 before they start.

This reduces context-switch noise and improves measurement stability, particularly for tail-latency percentiles, but requires that at least two physical cores be available.

> **Note:** `--pin-cpus` is Linux-only and is **disabled by default**. Enabling it on a heavily loaded single-core machine will hurt performance rather than help.

```bash
# PUSH/PULL throughput with CPU pinning
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --duration 10 --pin-cpus

# REQ/REP latency with CPU pinning
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --duration 10 --pin-cpus
```

### 3. High-Performance `io_uring` Transport

This transport requires compiling the binary with the `io-uring` feature flag enabled:
```bash
cargo build --release --features io-uring --bin rzmq_bench
```

The custom `io_uring` backend is currently implemented only for **TCP network sockets**.

*   **REQ / REP (Basic Latency Ping-Pong):**
    ```bash
    cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --use-io-uring
    ```
*   **PUSH / PULL (Optimized Throughput with Zero-Copy Send & Multishot Receive):**
    ```bash
    cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 4096 --use-io-uring --uring-zerocopy --uring-multishot
    ```
*   **DEALER / ROUTER (Optimized Multipart with Zero-Copy Send):**
    ```bash
    cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 1024 --use-io-uring --uring-zerocopy
    ```
*   **PUB / SUB (Optimized Fan-out with Multishot Receive on Subscribers):**
    ```bash
    cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 512 --use-io-uring --uring-multishot
    ```

---

### C. Distributed Multi-Host Execution

To completely eliminate local CPU and memory bus contention, run the server and client as separate processes on different physical machines over a network.

#### 1. On Host A (Server/Receiver, e.g., IP: `192.168.1.10`)
```bash
cargo run --release --bin rzmq_bench -- --role server --endpoint tcp://0.0.0.0:19876 --pattern push-pull
```

#### 2. On Host B (Client/Sender)
```bash
cargo run --release --bin rzmq_bench -- --role client --endpoint tcp://192.168.1.10:19876 --pattern push-pull --duration 30
```

---

## 5. Concurrency and Pipelining

Two flags control parallel execution within the client process:

| Flag | Default | Applies To | Description |
| :--- | :--- | :--- | :--- |
| `--concurrency N` | `1` | All patterns | Spawn N independent async worker tasks in the client |
| `--pipeline N` | `1` | `dealer-router` only | In-flight message window depth per worker |

### How They Work

**`--concurrency`** spawns N tokio tasks that each run their own send/receive loop on a cloned socket handle. Because `rzmq::Socket` is `Arc`-backed and cheaply cloneable, this adds no socket-layer overhead. All tasks share a single `BenchmarkCollector` via `Arc`, so the final report reflects the combined throughput across all workers.

**`--pipeline`** applies only to the `dealer-router` pattern. It uses a `tokio::sync::Semaphore` to cap in-flight requests. A sender task acquires a permit before each send; a paired receiver task releases a permit for each reply received. The total window is `concurrency × pipeline`. Latency tracking (RTT histogram) is automatically disabled when `pipeline > 1` because individual message RTTs cannot be measured when sends and receives are decoupled.

### Constraints

> **`req-rep` + `--concurrency`:** The ZeroMQ `REQ` socket enforces a strict `send → recv → send → recv` alternation. Running multiple concurrent tasks on the same socket would violate this state machine. `rzmq_bench` logs a warning and clamps to 1 worker for `req-rep` regardless of the `--concurrency` value.

> **`--pipeline` without `dealer-router`:** The `--pipeline` flag is silently ignored for non-`dealer-router` patterns.

### Example Commands

```bash
# PUSH/PULL: 4 concurrent workers (throughput scaling)
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern push-pull --concurrency 4 --duration 10

# PUSH/PULL: 8 concurrent workers, message-count termination
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern push-pull --concurrency 8 --messages 1000000

# DEALER/ROUTER: 16 in-flight requests per worker (pipelining)
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --pipeline 16 --duration 10

# DEALER/ROUTER: 2 workers × 16 pipeline depth = 32 total in-flight requests
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 2 --pipeline 16 --duration 10
```

---

## 6. Output Metrics & Interpretation

`rzmq_bench` supports three output formats:

*   **`--output text` (Default):** Human-readable terminal output summarizing operational parameters, overall throughput (msg/s and MB/s), and RTT latency percentiles (min, p50, p90, p95, p99, p99.9, p99.99, max).
*   **`--output json`:** A structured JSON object containing all measured fields. When using orchestrated mode with JSON, the parent process automatically isolates server stderr logging to output only valid JSON to stdout.
*   **`--output csv`:** Comma-separated values, useful for appending historical test runs to a spreadsheet or importing directly into visualization tools.

### Example Text Report Analysis

```text
========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : ReqRep
Role                      : Client
Message Size              : 1024 bytes
Elapsed Time              : 10.0001 seconds
Total Messages            : 154320
Total Data                : 150.70 MB
--------------------------------------------------------
Throughput                : 15431.84 msg/s
Throughput Rate           : 15.07 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min                     :         35 us
  p50 (Median)            :         62 us
  p90                     :         88 us
  p95                     :        112 us
  p99                     :        240 us
  p99.9                   :        412 us
  Max                     :       1215 us
========================================================
```

*   **Throughput Metrics:** Ensure that your network interface is not saturating. For TCP throughput, compare these numbers against the theoretical bandwidth of your link.
*   **Tail Latency (p99 to Max):** In latency-focused patterns, monitor the delta between the median (p50) and the high percentiles (p99+). High p99+ latency indicates jitter, which can point to CPU context switching overhead, garbage collection/allocation pauses, or packet retransmission in the network layer. Pass `--pin-cpus` on Linux to keep tail latencies stable by pinning server and client to dedicated cores.