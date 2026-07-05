# Benchmark Runs

* **System:** AMD Ryzen 5 7640U, Balanced Power Profile
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client/Server
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 317,038 | 31,704.15 | 1.94 | 29.3 us | 61.8 us |
| **ReqRep** | `io-uring` | 1 | 404,892 | 40,492.43 | 2.47 | 22.9 us | 37.0 us |
| **DealerRouter** | Standard | 1 | 308,810 | 30,847.09 | 1.88 | 30.4 us | 56.9 us |
| **PushPull** | Standard | 1 | 36,601,649 | 3,660,368.74 | 223.41 | — | — |
| **PushPull** | Standard | 4 | 61,338,016 | 6,133,238.86 | 374.34 | — | — |
| **PushPull** | Standard (16 KB msg) | 1 | 3,589,120 | 358,856.23 | 5,607.13 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,815,560 | 181,584.88 | 5,674.53 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 5,029,391 | 502,924.14 | 15,716.38 | — | — |
| **PubSub** | Standard | 1 | 32,700,791 | 3,270,458.98 | 199.61 | — | — |
| **PubSub** | `--cork` | 1 | 35,354,522 | 3,535,691.42 | 215.80 | — | — |
| **PushPull** | `--cork` | 1 | 31,791,104 | 3,026,835.40 | 184.74 | — | — |
| **PushPull** | `--cork` | 4 | 32,103,173 | 3,210,380.06 | 195.95 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 5,422,517 | 542,206.15 | 16,943.94 | — | — |
| **PushPull** | `io-uring` | 1 | 63,357,957 | 6,335,992.08 | 386.72 | — | — |
| **PushPull** | `io-uring` | 4 | 63,645,623 | 6,363,663.20 | 388.41 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 57,916,459 | 5,792,037.78 | 353.52 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 65,276,114 | 6,526,385.38 | 398.34 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 44,693,530 | 4,469,718.31 | 272.81 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 50,435,732 | 5,035,601.18 | 307.35 | — | — |
| **PushPull** | `io-uring` (32 KB msg) | 1 | 2,291,371 | 229,142.24 | 7,160.70 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 1 | 2,552,365 | 255,244.75 | 7,976.40 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 4 | 2,549,252 | 254,907.35 | 7,965.85 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 1 | 2,557,517 | 255,773.78 | 7,992.93 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 4 | 150,823,172 | 251,372.05 | 7,855.38 | — | — |

---

## Detailed Benchmark Reports

### 1. ReqRep (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 317,038
* **Total Data:** 19.35 MB
* **Throughput:** 31,704.15 msg/s
* **Throughput Rate:** 1.94 MB/s

**Latency Distribution:**
* **Min:** 25.664 us
* **p50 (Median):** 29.327 us
* **p90:** 34.975 us
* **p95:** 43.583 us
* **p99:** 61.791 us
* **p99.9:** 122.559 us
* **Max:** 800.008 us


#### io-uring

**Command:**
```bash
cargo run --release --bin rzmq_bench --features io-uring -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** ReqRep
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 404,892
* **Total Data:** 24.71 MB
* **Throughput:** 40,492.43 msg/s
* **Throughput Rate:** 2.47 MB/s

**Latency Distribution:**
* **Min:** 18.224 us
* **p50 (Median):** 22.911 us
* **p90:** 28.783 us
* **p95:** 30.735 us
* **p99:** 36.959 us
* **p99.9:** 76.415 us
* **Max:** 1.165 ms

---

### 2. DealerRouter (Standard/io-uring)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64
```

**Metrics:**
* **Pattern:** DealerRouter
* **Elapsed Time:** 10.0110 seconds
* **Total Messages:** 308,810
* **Total Data:** 18.85 MB
* **Throughput:** 30,847.09 msg/s
* **Throughput Rate:** 1.88 MB/s

**Latency Distribution:**
* **Min:** 27.552 us
* **p50 (Median):** 30.351 us
* **p90:** 35.135 us
* **p95:** 40.191 us
* **p99:** 56.927 us
* **p99.9:** 124.031 us
* **Max:** 872.447 us

---

### 3. PushPull (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 36,601,649
* **Total Data:** 2,233.99 MB
* **Throughput:** 3,660,368.74 msg/s
* **Throughput Rate:** 223.41 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0009 seconds
* **Total Messages:** 61,338,016
* **Total Data:** 3,743.78 MB
* **Throughput:** 6,133,238.86 msg/s
* **Throughput Rate:** 374.34 MB/s

#### Concurrency 1, Msg Size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 16384
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0016 seconds
* **Total Messages:** 3,589,120
* **Total Data:** 56,080.00 MB
* **Throughput:** 358,856.23 msg/s
* **Throughput Rate:** 5,607.13 MB/s

#### Concurrency 1, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9984 seconds
* **Total Messages:** 1,815,560
* **Total Data:** 56,736.25 MB
* **Throughput:** 181,584.88 msg/s
* **Throughput Rate:** 5,674.53 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 5,029,391
* **Total Data:** 157,168.47 MB
* **Throughput:** 502,924.14 msg/s
* **Throughput Rate:** 15,716.38 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 32,700,791
* **Total Data:** 1,995.90 MB
* **Throughput:** 3,270,458.98 msg/s
* **Throughput Rate:** 199.61 MB/s

#### PubSub with Cork

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 35,354,522
* **Total Data:** 2,157.87 MB
* **Throughput:** 3,535,691.42 msg/s
* **Throughput Rate:** 215.80 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 31,791,104
* **Total Data:** 1,940.38 MB
* **Throughput:** 3,026,835.40 msg/s
* **Throughput Rate:** 184.74 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 32,103,173
* **Total Data:** 1,959.42 MB
* **Throughput:** 3,210,380.06 msg/s
* **Throughput Rate:** 195.95 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0008 seconds
* **Total Messages:** 5,422,517
* **Total Data:** 169,453.66 MB
* **Throughput:** 542,206.15 msg/s
* **Throughput Rate:** 16,943.94 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 63,357,957
* **Total Data:** 3,867.06 MB
* **Throughput:** 6,335,992.08 msg/s
* **Throughput Rate:** 386.72 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0014 seconds
* **Total Messages:** 63,645,623
* **Total Data:** 3,884.62 MB
* **Throughput:** 6,363,663.20 msg/s
* **Throughput Rate:** 388.41 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 57,916,459
* **Total Data:** 3,534.94 MB
* **Throughput:** 5,792,037.78 msg/s
* **Throughput Rate:** 353.52 MB/s


#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0019 seconds
* **Total Messages:** 65,276,114
* **Total Data:** 3,984.14 MB
* **Throughput:** 6,526,385.38 msg/s
* **Throughput Rate:** 398.34 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9992 seconds
* **Total Messages:** 44,693,530
* **Total Data:** 2,727.88 MB
* **Throughput:** 4,469,718.31 msg/s
* **Throughput Rate:** 272.81 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0158 seconds
* **Total Messages:** 50,435,732
* **Total Data:** 3,078.35 MB
* **Throughput:** 5,035,601.18 msg/s
* **Throughput Rate:** 307.35 MB/s

---

### 9. PushPull, io-uring, Msg Size 32KB

#### Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 2,291,371
* **Total Data:** 71,605.34 MB
* **Throughput:** 229,142.24 msg/s
* **Throughput Rate:** 7,160.70 MB/s

#### with Multishot, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9997 seconds
* **Total Messages:** 2,552,365
* **Total Data:** 79,761.41 MB
* **Throughput:** 255,244.75 msg/s
* **Throughput Rate:** 7,976.40 MB/s


#### with Multishot, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0007 seconds
* **Total Messages:** 2,549,252
* **Total Data:** 79,664.12 MB
* **Throughput:** 254,907.35 msg/s
* **Throughput Rate:** 7,965.85 MB/s

#### with Multishot and ZeroCopy, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --uring-zerocopy --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9991 seconds
* **Total Messages:** 2,557,517
* **Total Data:** 79,922.41 MB
* **Throughput:** 255,773.78 msg/s
* **Throughput Rate:** 7,992.93 MB/s

#### with Multishot and ZeroCopy, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --uring-zerocopy --concurrency 4 --duration 600
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 599.9998 seconds
* **Total Messages:** 150,823,172
* **Total Data:** 4,713,224.12 MB
* **Throughput:** 251,372.05 msg/s
* **Throughput Rate:** 7,855.38 MB/s