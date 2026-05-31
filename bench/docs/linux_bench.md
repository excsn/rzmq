# Benchmark Runs

* **System:** AMD Ryzen 5 7640U
* **Message Size:** 64 bytes
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 192,244 | 19,224.37 | 1.17 | 51 us | 62 us |
| **DealerRouter** | Standard | 247,153 | 24,715.10 | 1.51 | 37 us | 66 us |
| **PushPull** | Standard | 1,658,460 | 165,845.95 | 10.12 | — | — |
| **PubSub** | Standard | 1,438,614 | 143,816.81 | 8.78 | — | — |
| **PushPull** | `--cork` | 5,811,066 | 581,068.20 | 35.47 | — | — |
| **PushPull** | `io-uring` | 13,644,883 | 1,364,488.11 | 83.28 | — | — |
| **PushPull** | `io-uring` + `--cork` | 22,470,235 | 2,247,023.22 | 137.15 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 14,990,443 | 1,499,044.12 | 91.49 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 192,244
* **Total Data:** 11.73 MB
* **Throughput:** 19,224.37 msg/s
* **Throughput Rate:** 1.17 MB/s

**Latency Distribution:**
* **Min:** 40 us
* **p50 (Median):** 51 us
* **p90:** 54 us
* **p95:** 56 us
* **p99:** 62 us
* **p99.9:** 121 us
* **Max:** 799 us

---

### 2. DealerRouter (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 247,153
* **Total Data:** 15.09 MB
* **Throughput:** 24,715.10 msg/s
* **Throughput Rate:** 1.51 MB/s

**Latency Distribution:**
* **Min:** 33 us
* **p50 (Median):** 37 us
* **p90:** 44 us
* **p95:** 53 us
* **p99:** 66 us
* **p99.9:** 117 us
* **Max:** 1,557 us

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 1,658,460
* **Total Data:** 101.22 MB
* **Throughput:** 165,845.95 msg/s
* **Throughput Rate:** 10.12 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0031 seconds
* **Total Messages:** 1,438,614
* **Total Data:** 87.81 MB
* **Throughput:** 143,816.81 msg/s
* **Throughput Rate:** 8.78 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 5,811,066
* **Total Data:** 354.68 MB
* **Throughput:** 581,068.20 msg/s
* **Throughput Rate:** 35.47 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 13,644,883
* **Total Data:** 832.82 MB
* **Throughput:** 1,364,488.11 msg/s
* **Throughput Rate:** 83.28 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 22,470,235
* **Total Data:** 1,371.47 MB
* **Throughput:** 2,247,023.22 msg/s
* **Throughput Rate:** 137.15 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 14,990,443
* **Total Data:** 914.94 MB
* **Throughput:** 1,499,044.12 msg/s
* **Throughput Rate:** 91.49 MB/s