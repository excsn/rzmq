# Benchmark Runs

* **System:** Macbook Pro M4
* **Message Size:** 64 bytes (unless otherwise specified)
* **Role:** Client/Server
* **Target Endpoint:** `tcp://127.0.0.1:19876` (unless otherwise specified)

---

## Summary of Results

| Pattern | Features / Flags | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **ReqRep** | Standard | 291,667 | 29,168.90 | 1.78 | 33.28 µs | 51.07 µs |
| **ReqRep** | Concurrency 4 | 824,370 | 82,434.88 | 5.03 | 47.42 µs | 78.40 µs |
| **ReqRep** | Concurrency 8 | 1,146,607 | 114,662.88 | 7.00 | 68.22 µs | 110.14 µs |
| **DealerRouter** | Standard | 279,851 | 27,953.15 | 1.71 | 34.59 µs | 51.84 µs |
| **DealerRouter** | Concurrency 2, Pipeline 4 | 1,681,546 | 167,991.58 | 10.25 | 44.61 µs | 83.52 µs |
| **DealerRouter** | Concurrency 4, Pipeline 8 | 2,405,347 | 240,263.61 | 14.66 | 63.90 µs | 104.58 µs |
| **PushPull** | Standard | 39,917,123 | 3,991,308.88 | 243.61 | — | — |
| **PushPull** | Concurrency 2 | 35,209,030 | 3,521,088.08 | 214.91 | — | — |
| **PushPull** | Concurrency 4, Msg Size 32KB | 4,524,344 | 452,415.77 | 14,137.99 | — | — |
| **PubSub** | Standard | 33,527,057 | 3,352,853.88 | 204.64 | — | — |
| **PubSub** | Concurrency 2 | 23,849,970 | 2,385,035.28 | 145.57 | — | — |
| **PubSub** | Concurrency 4 | 37,547,928 | 3,754,404.22 | 229.15 | — | — |
| **PubSub** | Concurrency 1, Msg Size 16KB | 6,532,096 | 653,200.69 | 10,206.26 | — | — |
| **PubSub** | Concurrency 4, Msg Size 16KB | 9,406,909 | 940,692.03 | 14,698.31 | — | — |
| **PubSub** | Concurrency 1, Msg Size 32KB | 3,433,100 | 343,316.29 | 10,728.63 | — | — |
| **PubSub** | Concurrency 4, Msg Size 32KB | 4,578,940 | 457,861.91 | 14,308.18 | — | — |

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
* **Elapsed Time:** 10.0097 seconds
* **Total Messages:** 1,681,546
* **Total Data:** 102.63 MB
* **Throughput:** 167,991.58 msg/s
* **Throughput Rate:** 10.25 MB/s

**Latency Distribution:**
* **Min:** 16.736 µs
* **p50 (Median):** 44.607 µs
* **p90:** 58.943 µs
* **p95:** 65.375 µs
* **p99:** 83.519µs
* **p99.9:** 120.171 µs
* **Max:** 462.114 µs

#### Concurrency 4, Pipeline 8

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --pattern dealer-router --concurrency 4 --pipeline 8 --duration 10
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0113 seconds
* **Total Messages:** 2,405,347
* **Total Data:** 146.81 MB
* **Throughput:** 240,263.61 msg/s
* **Throughput Rate:** 14.66 MB/s

**Latency Distribution:**
* **Min:** 19.776 µs
* **p50 (Median):** 63.903 µs
* **p90:** 84.223 µs
* **p95:** 90.879 µs
* **p99:** 104.575 µs
* **p99.9:** 124.991 µs
* **Max:** 1.615 ms

---

### 3. PushPull

#### Standard

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0010 seconds
* **Total Messages:** 39,917,123
* **Total Data:** 2,436.35 MB
* **Throughput:** 3,991,308.88 msg/s
* **Throughput Rate:** 243.61 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9995 seconds
* **Total Messages:** 35,209,030
* **Total Data:** 2,148.99 MB
* **Throughput:** 3,521,088.08 msg/s
* **Throughput Rate:** 214.91 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 4,524,344
* **Total Data:** 141,385.75 MB
* **Throughput:** 452,415.77 msg/s
* **Throughput Rate:** 14,137.99 MB/s

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
* **Total Messages:** 33,527,057
* **Total Data:** 2,046.33 MB
* **Throughput:** 3,352,853.88 msg/s
* **Throughput Rate:** 204.64 MB/s

#### Concurrency 2

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --concurrency 2
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 23,849,970
* **Total Data:** 1,455.69 MB
* **Throughput:** 2,385,035.28 msg/s
* **Throughput Rate:** 145.57 MB/s

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

#### Concurrency 1, Msg size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 16384
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 6,532,096
* **Total Data:** 102,064.00 MB
* **Throughput:** 653,200.69 msg/s
* **Throughput Rate:** 10,206.26 MB/s

#### Concurrency 4, Msg size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 16384 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 9,406,909
* **Total Data:** 146,982.95
* **Throughput:** 940,692.03 msg/s
* **Throughput Rate:** 14,698.31 MB/s

#### Concurrency 1, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 3,433,100
* **Total Data:** 107,284.38 MB
* **Throughput:** 343,316.29 msg/s
* **Throughput Rate:** 10,728.63 MB/s

#### Concurrency 4, Msg size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 4,578,940
* **Total Data:** 143,091.88 MB
* **Throughput:** 457,861.91 msg/s
* **Throughput Rate:** 14,308.18 MB/s