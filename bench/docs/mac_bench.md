# Benchmark Runs

* **System:** Macbook Pro M4
* **Message Size:** 64 bytes
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876` (unless otherwise specified)

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 291,667 | 29,168.90 | 1.78 | 33.28 µs | 51.07 µs |
| **ReqRep** | Concurrency 4 | 824,370 | 82,434.88 | 5.03 | 47.42 µs | 78.40 µs |
| **ReqRep** | Concurrency 8 | 1,146,607 | 114,662.88 | 7.00 | 68.22 µs | 110.14 µs |
| **DealerRouter**| Standard | 279,851 | 27,953.15 | 1.71 | 34.59 µs | 51.84 µs |
| **DealerRouter**| Concurrency 2, Pipeline 4 | 862,548 | 86,246.83 | 5.26 | 43.26 µs | 66.43 µs |
| **DealerRouter**| Concurrency 4, Pipeline 8 | 863,394 | 86,248.34 | 5.26 | 43.26 µs | 63.49 µs |
| **PushPull** | Standard | 26,507,219 | 2,650,818.38 | 161.79 | — | — |
| **PushPull** | Concurrency 2 | 26,549,866 | 2,654,677.67 | 162.03 | — | — |
| **PushPull** | Concurrency 4, Msg Size 32KB | 2,065,300 | 206,526.24 | 6,453.95 | — | — |
| **PubSub** | Standard | 20,557,796 | 2,055,867.02 | 125.48 | — | — |
| **PubSub** | Concurrency 2 | 19,223,737 | 1,922,517.69 | 117.34 | — | — |
| **PubSub** | Concurrency 4 | 37,547,928 | 3,754,404.22 | 229.15 | — | — |
| **PubSub** | Concurrency 4, Msg Size 32KB | 2,009,380 | 200,941.70 | 6,279.43 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 291,667
* **Total Data:** 17.80 MB
* **Throughput:** 29,168.90 msg/s
* **Throughput Rate:** 1.78 MB/s

**Latency Distribution:**
* **Min:** 21.488 µs
* **p50 (Median):** 33.279 µs
* **p90:** 37.343 µs
* **p95:** 44.319 µs
* **p99:** 51.071 µs
* **p99.9:** 72.767 µs
* **Max:** 227.839 µs

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 824,370
* **Total Data:** 50.32 MB
* **Throughput:** 82,434.88 msg/s
* **Throughput Rate:** 5.03 MB/s

**Latency Distribution:**
* **Min:** 15.792 µs
* **p50 (Median):** 47.423 µs
* **p90:** 62.975 µs
* **p95:** 67.967 µs
* **p99:** 78.399 µs
* **p99.9:** 93.311 µs
* **Max:** 214.655 µs

#### Concurrency 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --concurrency 8
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 1,146,607
* **Total Data:** 69.98 MB
* **Throughput:** 114,662.88 msg/s
* **Throughput Rate:** 7.00 MB/s

**Latency Distribution:**
* **Min:** 20.112 µs
* **p50 (Median):** 68.223 µs
* **p90:** 89.087 µs
* **p95:** 95.807 µs
* **p99:** 110.143 µs
* **p99.9:** 133.375 µs
* **Max:** 449.791 µs

---

### 2. DealerRouter

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0114 seconds
* **Total Messages:** 279,851
* **Total Data:** 17.08 MB
* **Throughput:** 27,953.15 msg/s
* **Throughput Rate:** 1.71 MB/s

**Latency Distribution:**
* **Min:** 22.368 µs
* **p50 (Median):** 34.591 µs
* **p90:** 38.687 µs
* **p95:** 45.439 µs
* **p99:** 51.839 µs
* **p99.9:** 78.079 µs
* **Max:** 278.783 µs

#### Concurrency 2, Pipeline 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 2 --pipeline 4 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0009 seconds
* **Total Messages:** 862,548
* **Total Data:** 52.65 MB
* **Throughput:** 86,246.83 msg/s
* **Throughput Rate:** 5.26 MB/s

**Latency Distribution:**
* **Min:** 22.704 µs
* **p50 (Median):** 43.263 µs
* **p90:** 49.727 µs
* **p95:** 55.647 µs
* **p99:** 66.431 µs
* **p99.9:** 119.167 µs
* **Max:** 268.031 µs

#### Concurrency 4, Pipeline 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 4 --pipeline 8 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0106 seconds
* **Total Messages:** 863,394
* **Total Data:** 52.70 MB
* **Throughput:** 86,248.34 msg/s
* **Throughput Rate:** 5.26 MB/s

**Latency Distribution:**
* **Min:** 21.152 µs
* **p50 (Median):** 43.263 µs
* **p90:** 49.471 µs
* **p95:** 55.167 µs
* **p99:** 63.487 µs
* **p99.9:** 109.311 µs
* **Max:** 211.327 µs

---

### 3. PushPull

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 26,507,219
* **Total Data:** 1,617.87 MB
* **Throughput:** 2,650,818.38 msg/s
* **Throughput Rate:** 161.79 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0012 seconds
* **Total Messages:** 26,549,866
* **Total Data:** 1,620.48 MB
* **Throughput:** 2,654,677.67 msg/s
* **Throughput Rate:** 162.03 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0002 seconds
* **Total Messages:** 2,065,300
* **Total Data:** 64,540.62 MB
* **Throughput:** 206,526.24 msg/s
* **Throughput Rate:** 6,453.95 MB/s

---

### 4. PubSub

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 20,557,796
* **Total Data:** 1,254.75 MB
* **Throughput:** 2,055,867.02 msg/s
* **Throughput Rate:** 125.48 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 19,223,737
* **Total Data:** 1,173.32 MB
* **Throughput:** 1,922,517.69 msg/s
* **Throughput Rate:** 117.34 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0010 seconds
* **Total Messages:** 37,547,928
* **Total Data:** 12,291.74 MB
* **Throughput:** 3,754,404.22 msg/s
* **Throughput Rate:** 229.15 MB/s

#### Concurrency 4, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 2,009,380
* **Total Data:** 62,793.12 MB
* **Throughput:** 200,941.70 msg/s
* **Throughput Rate:** 6,279.43 MB/s