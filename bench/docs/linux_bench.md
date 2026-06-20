# Benchmark Runs

* **System:** AMD Ryzen 5 7640U, Balanced Power Profile
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 317,038 | 31,704.15 | 1.94 | 29.3 us | 61.8 us |
| **ReqRep** | `io-uring` | 1 | 404,892 | 40,492.43 | 2.47 | 22.9 us | 37.0 us |
| **DealerRouter** | Standard | 1 | 308,810 | 30,847.09 | 1.88 | 30.4 us | 56.9 us |
| **PushPull** | Standard | 1 | 28,179,700 | 2,818,072.87 | 172.00 | — | — |
| **PushPull** | Standard | 4 | 50,809,050 | 5,080,981.41 | 310.12 | — | — |
| **PushPull** | Standard (16 KB msg) | 1 | 3,345,408 | 334,559.46 | 5,227.49 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,542,970 | 154,316.05 | 4,822.38 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 3,764,240 | 376,255.74 | 11,757.99 | — | — |
| **PubSub** | Standard | 1 | 25,855,210 | 2,585,706.53 | 157.82 | — | — |
| **PubSub** | `--cork` | 1 | 26,223,062 | 2,622,501.09 | 160.06 | — | — |
| **PushPull** | `--cork` | 1 | 29,882,684 | 2,988,397.11 | 182.40 | — | — |
| **PushPull** | `--cork` | 4 | 50,332,551 | 5,032,172.68 | 307.14 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 3,791,153 | 379,098.41 | 11,846.83 | — | — |
| **PushPull** | `io-uring` | 1 | 37,154,606 | 3,715,715.44 | 226.79 | — | — |
| **PushPull** | `io-uring` | 4 | 44,389,655 | 4,439,437.51 | 270.96 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 41,914,266 | 4,191,825.75 | 255.85 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 53,224,944 | 5,319,865.40 | 324.70 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 35,943,858 | 3,592,001.06 | 219.24 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 40,904,075 | 4,090,856.30 | 249.69 | — | — |
| **PushPull** | Standard (32 KB msg) | 1 | 1,524,745 | 152,468.00 | 4,764.63 | — | — |
| **PushPull** | Standard (32 KB msg) | 8 | 2,347,191 | 234,390.02 | 7,324.69 | — | — |
| **PushPull** | `io-uring` (32 KB msg) | 1 | 2,191,242 | 219,137.34 | 6,848.04 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 1 | 2,489,001 | 248,909.66 | 7,778.43 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 4 | 2,439,876 | 243,992.26 | 7,624.76 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 1 | 2,252,529 | 225,265.11 | 7,039.53 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 4 | 2,467,931 | 246,802.56 | 7,712.58 | — | — |

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

### 2. DealerRouter (Standard)

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
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 28,179,700
* **Total Data:** 1,719.95 MB
* **Throughput:** 2,818,072.87 msg/s
* **Throughput Rate:** 172.00 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 50,809,050
* **Total Data:** 3,101.14 MB
* **Throughput:** 5,080,981.41 msg/s
* **Throughput Rate:** 310.12 MB/s

#### Concurrency 1, Msg Size 16KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 16384
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 3,345,408
* **Total Data:** 52,272.00 MB
* **Throughput:** 334,559.46 msg/s
* **Throughput Rate:** 5,227.49 MB/s

#### Concurrency 1, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9988 seconds
* **Total Messages:** 1,542,970
* **Total Data:** 48,217.81 MB
* **Throughput:** 154,316.05 msg/s
* **Throughput Rate:** 4,822.38 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0045 seconds
* **Total Messages:** 3,764,240
* **Total Data:** 117,632.50 MB
* **Throughput:** 376,255.74 msg/s
* **Throughput Rate:** 11,757.99 MB/s

---

### 4. PubSub (Standard)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 25,855,210
* **Total Data:** 1,578.08 MB
* **Throughput:** 2,585,706.53 msg/s
* **Throughput Rate:** 157.82 MB/s

#### PubSub with Cork

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PubSub
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 26,223,062
* **Total Data:** 1,600.53 MB
* **Throughput:** 2,622,501.09 msg/s
* **Throughput Rate:** 160.06 MB/s

---

### 5. PushPull (with Cork)

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 29,882,684
* **Total Data:** 1,823.89 MB
* **Throughput:** 2,988,397.11 msg/s
* **Throughput Rate:** 182.40 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0022 seconds
* **Total Messages:** 50,332,551
* **Total Data:** 3,072.06 MB
* **Throughput:** 5,032,172.68 msg/s
* **Throughput Rate:** 307.14 MB/s

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 3,791,153
* **Total Data:** 118,473.53 MB
* **Throughput:** 379,098.41 msg/s
* **Throughput Rate:** 11,846.83 MB/s

---

### 6. PushPull (io-uring)

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9993 seconds
* **Total Messages:** 37,154,606
* **Total Data:** 2,267.74 MB
* **Throughput:** 3,715,715.44 msg/s
* **Throughput Rate:** 226.79 MB/s

#### Concurrency 4

**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9989 seconds
* **Total Messages:** 44,389,655
* **Total Data:** 2,709.33 MB
* **Throughput:** 4,439,437.51 msg/s
* **Throughput Rate:** 270.96 MB/s

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

### 9. PushPull, Msg Size 32KB

#### Standard, Concurrency 1
**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 1,524,745
* **Total Data:** 47,648.28 MB
* **Throughput:** 152,468.00 msg/s
* **Throughput Rate:** 4,764.63 MB/s

#### Standard, Concurrency 8
**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0140 seconds
* **Total Messages:** 2,347,191
* **Total Data:** 73,349.72 MB
* **Throughput:** 234,390.02 msg/s
* **Throughput Rate:** 7,324.69 MB/s

#### io-uring, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9994 seconds
* **Total Messages:** 2,191,242
* **Total Data:** 68,476.31 MB
* **Throughput:** 219,137.34 msg/s
* **Throughput Rate:** 6,848.04 MB/s

#### io-uring with Multishot, Concurrency 1
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


#### io-uring with Multishot, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9998 seconds
* **Total Messages:** 2,439,876
* **Total Data:** 76,246.12 MB
* **Throughput:** 243,992.26 msg/s
* **Throughput Rate:** 7,624.76 MB/s

#### io-uring with Multishot and ZeroCopy, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --uring-zerocopy --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9995 seconds
* **Total Messages:** 2,252,529
* **Total Data:** 70,391.53 MB
* **Throughput:** 225,265.11 msg/s
* **Throughput Rate:** 7,039.53 MB/s

#### io-uring with Multishot and ZeroCopy, Concurrency 4
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --uring-zerocopy --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9996 seconds
* **Total Messages:** 2,467,931
* **Total Data:** 77,122.84 MB
* **Throughput:** 246,802.56 msg/s
* **Throughput Rate:** 7,712.58 MB/s