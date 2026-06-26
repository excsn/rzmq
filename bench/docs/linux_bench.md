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
| **PushPull** | Standard | 1 | 31,416,960 | 3,141,591.30 | 191.75 | — | — |
| **PushPull** | Standard | 4 | 33,532,814 | 3,353,464.13 | 204.68 | — | — |
| **PushPull** | Standard (16 KB msg) | 1 | 3,589,120 | 358,856.23 | 5,607.13 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,815,560 | 181,584.88 | 5,674.53 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 5,029,391 | 502,924.14 | 15,716.38 | — | — |
| **PubSub** | Standard | 1 | 32,700,791 | 3,270,458.98 | 199.61 | — | — |
| **PubSub** | `--cork` | 1 | 35,354,522 | 3,535,691.42 | 215.80 | — | — |
| **PushPull** | `--cork` | 1 | 31,791,104 | 3,026,835.40 | 184.74 | — | — |
| **PushPull** | `--cork` | 4 | 32,103,173 | 3,210,380.06 | 195.95 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 5,422,517 | 542,206.15 | 16,943.94 | — | — |
| **PushPull** | `io-uring` | 1 | 40,466,303 | 4,046,773.88 | 247.00 | — | — |
| **PushPull** | `io-uring` | 4 | 49,317,766 | 4,923,121.95 | 300.48 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 41,914,266 | 4,191,825.75 | 255.85 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 53,224,944 | 5,319,865.40 | 324.70 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 35,943,858 | 3,592,001.06 | 219.24 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 40,904,075 | 4,090,856.30 | 249.69 | — | — |
| **PushPull** | `io-uring` (32 KB msg) | 1 | 2,291,371 | 229,142.24 | 7,160.70 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 1 | 2,489,001 | 248,909.66 | 7,778.43 | — | — |
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
* **Elapsed Time:** 10.0003 seconds
* **Total Messages:** 31,416,960
* **Total Data:** 1,917.54 MB
* **Throughput:** 3,141,591.30 msg/s
* **Throughput Rate:** 191.75 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9995 seconds
* **Total Messages:** 33,532,814
* **Total Data:** 2,046.68 MB
* **Throughput:** 3,353,464.13 msg/s
* **Throughput Rate:** 204.68 MB/s

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
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 40,466,303
* **Total Data:** 2,469.87 MB
* **Throughput:** 4,046,773.88 msg/s
* **Throughput Rate:** 247.00 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0176 seconds
* **Total Messages:** 49,317,766
* **Total Data:** 3,010.12 MB
* **Throughput:** 4,923,121.95 msg/s
* **Throughput Rate:** 300.48 MB/s

---

### 7. PushPull (io-uring with Cork)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9990 seconds
* **Total Messages:** 41,914,266
* **Total Data:** 2,558.24 MB
* **Throughput:** 4,191,825.75 msg/s
* **Throughput Rate:** 255.85 MB/s


#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0049 seconds
* **Total Messages:** 53,224,944
* **Total Data:** 3,248.59 MB
* **Throughput:** 5,319,865.40 msg/s
* **Throughput Rate:** 324.70 MB/s

---

### 8. PushPull (io-uring with Multishot)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0066 seconds
* **Total Messages:** 35,943,858
* **Total Data:** 2,193.84 MB
* **Throughput:** 3,592,001.06 msg/s
* **Throughput Rate:** 219.24 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9989 seconds
* **Total Messages:** 40,904,075
* **Total Data:** 2,496.59 MB
* **Throughput:** 4,090,856.30 msg/s
* **Throughput Rate:** 249.69 MB/s

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
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 2,489,001
* **Total Data:** 77,781.28 MB
* **Throughput:** 248,909.66 msg/s
* **Throughput Rate:** 7,778.43 MB/s


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