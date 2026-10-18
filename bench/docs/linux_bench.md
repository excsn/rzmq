# Benchmark Runs

* **System:** AMD Ryzen 5 7640U, Balanced Power Profile
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client/Server
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 333,963 | 33,395.90 | 2.04 | 28.6 us | 48.4 us |
| **ReqRep** | `io-uring` + `--uring-workers 1` | 1 | 355,739 | 35,577.99 | 2.17 | 27.2 us | 39.3 us |
| **DealerRouter** | Standard | 1 | 318,621 | 31,861.98 | 2.13 | 29.3 us | 53.4 us |
| **PushPull** | Standard | 1 | 36,612,839 | 3,660,131.41 | 223.40 | — | — |
| **PushPull** | Standard | 4 | 53,670,061 | 5,367,105.03 | 327.58 | — | — |
| **PushPull** | Standard (16 KB msg) | 1 | 3,118,145 | 311,817.53 | 4,872.15 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,631,115 | 163,112.16 | 5,097.25 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 5,255,044 | 525,502.92 | 16,421.97 | — | — |
| **PubSub** | Standard | 1 | 38,217,729 | 3,821,726.09 | 233.26 | — | — |
| **PubSub** | `--cork` | 1 | 42,270,369 | 4,227,393.20 | 258.02 | — | — |
| **PushPull** | `--cork` | 1 | 37,095,115 | 3,708,290.50 | 226.34 | — | — |
| **PushPull** | `--cork` | 4 | 58,078,766 | 5,807,977.10 | 354.49 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 5,595,599 | 559,561.07 | 17,486.28 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` | 1 | 68,944,511 | 6,895,703.39 | 420.88 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` | 4 | 62,530,878 | 6,253,667.06 | 381.69 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--cork` | 1 | 51,681,075 | 5,168,844.68 | 315.48 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--cork` | 4 | 68,005,901 | 6,801,245.82 | 415.12 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` | 1 | 47,109,998 | 4,710,429.71 | 287.50 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` | 4 | 48,301,496 | 4,830,708.07 | 294.84 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` (32 KB msg) | 1 | 2,277,794 | 227,817.78 | 7,119.31 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` (32 KB msg) | 1 | 2,196,284 | 219,634.65 | 6,863.58 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` (32 KB msg) | 4 | 2,475,391 | 247,578.21 | 7,736.82 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 1 | 2,080,823 | 208,107.20 | 6,503.35 | — | — |
| **PushPull** | `io-uring` + `--uring-workers 1` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 4 | 2,186,193 | 218,646.74 | 6,832.71 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 4` (4 KB msg) | 8 | 34,878,227 | 3,488,170.23 | 13,625.66 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 2` (32 KB msg) | 8 | 4,411,810 | 441,223.18 | 13,788.22 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-workers 4` (32 KB msg) | 8 | 4,884,773 | 488,535.40 | 15,266.73 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--cork` + `--uring-workers 4` (32 KB msg) | 8 | 5,207,226 | 520,800.43 | 16,275.01 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 333,963
* **Total Data:** 20.38 MB
* **Throughput:** 33,395.90 msg/s
* **Throughput Rate:** 2.04 MB/s

**Latency Distribution:**
* **Min:** 24.912 us
* **p50 (Median):** 28.623 us
* **p90:** 32.719 us
* **p95:** 35.007 us
* **p99:** 48.447 us
* **p99.9:** 252.671 us
* **Max:** 1.390 ms


#### io-uring

**Command:**
```bash
cargo run --release --bin rzmq_bench --features io-uring -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --use-io-uring --uring-workers 1
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9989 seconds
* **Total Messages:** 355,739
* **Total Data:** 21.71 MB
* **Throughput:** 35,577.99 msg/s
* **Throughput Rate:** 2.17 MB/s

**Latency Distribution:**
* **Min:** 20.896 us
* **p50 (Median):** 27.183 us
* **p90:** 29.311 us
* **p95:** 30.751 us
* **p99:** 39.263 us
* **p99.9:** 244.735 us
* **Max:** 2.111 ms

---

### 2. DealerRouter (Standard/io-uring)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 318,621
* **Total Data:** 21.27 MB
* **Throughput:** 31,861.98 msg/s
* **Throughput Rate:** 2.13 MB/s

**Latency Distribution:**
* **Min:** 25.568 us
* **p50 (Median):** 29.295 us
* **p90:** 34.431 us
* **p95:** 36.639 us
* **p99:** 53.439 us
* **p99.9:** 284.415 us
* **Max:** 2.736 ms

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0031 seconds
* **Total Messages:** 36,612,839
* **Total Data:** 2,234.67 MB
* **Throughput:** 3,660,131.41 msg/s
* **Throughput Rate:** 223.40 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 53,670,061
* **Total Data:** 3,275.76 MB
* **Throughput:** 5,367,105.03 msg/s
* **Throughput Rate:** 327.58 MB/s

#### Concurrency 1, Msg Size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 16384
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 3,118,145
* **Total Data:** 48,721.02 MB
* **Throughput:** 311,817.53 msg/s
* **Throughput Rate:** 4,872.15 MB/s

#### Concurrency 1, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 1,631,115
* **Total Data:** 50,972.34 MB
* **Throughput:** 163,112.16 msg/s
* **Throughput Rate:** 5,097.25 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 5,255,044
* **Total Data:** 164,220.12 MB
* **Throughput:** 525,502.92 msg/s
* **Throughput Rate:** 16,421.97 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 38,217,729
* **Total Data:** 2,332.63 MB
* **Throughput:** 3,821,726.09 msg/s
* **Throughput Rate:** 233.26 MB/s

#### PubSub with Cork

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 42,270,369
* **Total Data:** 2,579.98 MB
* **Throughput:** 4,227,393.20 msg/s
* **Throughput Rate:** 258.02 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0033 seconds
* **Total Messages:** 37,095,115
* **Total Data:** 2,264.11 MB
* **Throughput:** 3,708,290.50 msg/s
* **Throughput Rate:** 226.34 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 58,078,766
* **Total Data:** 3,544.85 MB
* **Throughput:** 5,807,977.10 msg/s
* **Throughput Rate:** 354.49 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0000 seconds
* **Total Messages:** 5,595,599
* **Total Data:** 174,862.47 MB
* **Throughput:** 559,561.07 msg/s
* **Throughput Rate:** 17,486.28 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9982 seconds
* **Total Messages:** 68,944,511
* **Total Data:** 4,208.04 MB
* **Throughput:** 6,895,703.39 msg/s
* **Throughput Rate:** 420.88 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9991 seconds
* **Total Messages:** 62,530,878
* **Total Data:** 3,816.58 MB
* **Throughput:** 6,253,667.06 msg/s
* **Throughput Rate:** 381.69 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9986 seconds
* **Total Messages:** 51,681,075
* **Total Data:** 3,154.36 MB
* **Throughput:** 5,168,844.68 msg/s
* **Throughput Rate:** 315.48 MB/s


#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 68,005,901
* **Total Data:** 4,150.75 MB
* **Throughput:** 6,801,245.82 msg/s
* **Throughput Rate:** 415.12 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0012 seconds
* **Total Messages:** 47,109,998
* **Total Data:** 2,875.37 MB
* **Throughput:** 4,710,429.71 msg/s
* **Throughput Rate:** 287.50 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 48,301,496
* **Total Data:** 2,948.09 MB
* **Throughput:** 4,830,708.07 msg/s
* **Throughput Rate:** 294.84 MB/s

---

### 9. PushPull, io-uring, Msg Size 32KB

#### Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9983 seconds
* **Total Messages:** 2,277,794
* **Total Data:** 71,181.06 MB
* **Throughput:** 227,817.78 msg/s
* **Throughput Rate:** 7,119.31 MB/s

#### with Multishot, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 2,196,284
* **Total Data:** 68,633.88 MB
* **Throughput:** 219,634.65 msg/s
* **Throughput Rate:** 6,863.58 MB/s


#### with Multishot, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9984 seconds
* **Total Messages:** 2,475,391
* **Total Data:** 77,355.97 MB
* **Throughput:** 247,578.21 msg/s
* **Throughput Rate:** 7,736.82 MB/s

#### with Multishot and ZeroCopy, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --uring-zerocopy --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 2,080,823
* **Total Data:** 65,025.72 MB
* **Throughput:** 208,107.20 msg/s
* **Throughput Rate:** 6,503.35 MB/s

#### with Multishot and ZeroCopy, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-workers 1 --uring-multishot --uring-zerocopy --concurrency 4 --duration 600
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9987 seconds
* **Total Messages:** 2,186,193
* **Total Data:** 68,318.53 MB
* **Throughput:** 218,646.74 msg/s
* **Throughput Rate:** 6,832.71 MB/s

---

### 10. PushPull, io-uring Multishot, Sharded Workers (`--uring-workers`)

Connections are sharded round-robin across independent `UringWorker` rings. With
`--concurrency 8` there are 8 connections to distribute; the 2→4 worker sweep at 32 KB shows
the sharding scaling (433 K → 517 K msg/s), and cork on top adds another ~7%.

#### 4 Workers, Msg Size 4KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 4096 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 34,878,227
* **Total Data:** 136,243.07 MB
* **Throughput:** 3,488,170.23 msg/s
* **Throughput Rate:** 13,625.66 MB/s

#### 2 Workers, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 2
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 4,411,810
* **Total Data:** 137,869.06 MB
* **Throughput:** 441,223.18 msg/s
* **Throughput Rate:** 13,788.22 MB/s

#### 4 Workers, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 4,884,773
* **Total Data:** 152,649.16 MB
* **Throughput:** 488,535.40 msg/s
* **Throughput Rate:** 15,266.73 MB/s

#### 4 Workers with Cork, Msg Size 32KB, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8 --use-io-uring --uring-multishot --cork --uring-workers 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9985 seconds
* **Total Messages:** 5,207,226
* **Total Data:** 162,725.81 MB
* **Throughput:** 520,800.43 msg/s
* **Throughput Rate:** 16,275.01 MB/s