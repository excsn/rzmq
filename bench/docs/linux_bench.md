# Benchmark Runs

* **System:** AMD Ryzen 5 7640U, Balanced Power Profile
* **Message Size:** 64 bytes (unless specified otherwise)
* **Role:** Client
* **Target Endpoint:** `tcp://127.0.0.1:19876`

---

## Results Overview

| Pattern | Features / Flags | Concurrency | Total Messages | Throughput (msg/s) | Throughput Rate (MB/s) | p50 Latency | p99 Latency |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- | :--- |
| **ReqRep** | Standard | 1 | 192,244 | 19,224.37 | 1.17 | 51 us | 62 us |
| **ReqRep** | `io-uring` | 1 | 404,892 | 40,492.43 | 2.47 | 22.9 us | 37.0 us |
| **DealerRouter** | Standard | 1 | 308,810 | 30,847.09 | 1.88 | 30.4 us | 56.9 us |
| **PushPull** | Standard | 1 | 28,179,700 | 2,818,072.87 | 172.00 | — | — |
| **PushPull** | Standard | 4 | 50,809,050 | 5,080,981.41 | 310.12 | — | — |
| **PushPull** | Standard (32 KB msg) | 4 | 3,124,097 | 312,393.53 | 9,762.30 | — | — |
| **PubSub** | Standard | 1 | 25,855,210 | 2,585,706.53 | 157.82 | — | — |
| **PubSub** | `--cork` | 1 | 26,223,062 | 2,622,501.09 | 160.06 | — | — |
| **PushPull** | `--cork` | 1 | 29,882,684 | 2,988,397.11 | 182.40 | — | — |
| **PushPull** | `--cork` | 4 | 50,332,551 | 5,032,172.68 | 307.14 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 4 | 2,878,977 | 287,887.15 | 8,996.47 | — | — |
| **PushPull** | `io-uring` | 1 | 37,154,606 | 3,715,715.44 | 226.79 | — | — |
| **PushPull** | `io-uring` | 4 | 44,389,655 | 4,439,437.51 | 270.96 | — | — |
| **PushPull** | `io-uring` + `--cork` | 1 | 41,914,266 | 4,191,825.75 | 255.85 | — | — |
| **PushPull** | `io-uring` + `--cork` | 4 | 53,224,944 | 5,319,865.40 | 324.70 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 1 | 35,943,858 | 3,592,001.06 | 219.24 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` | 4 | 40,904,075 | 4,090,856.30 | 249.69 | — | — |
| **PushPull** | `--cork` (32 KB msg) | 8 | 2,286,920 | 228,688.71 | 7,146.52 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` (32 KB msg) | 8 | 2,438,344 | 243,825.80 | 7,619.56 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 1 | 2,090,241 | 209,026.80 | 6,532.09 | — | — |
| **PushPull** | `io-uring` + `--uring-multishot` + `--uring-zerocopy` (32 KB msg) | 8 | 2,449,825 | 244,985.04 | 7,655.78 | — | — |

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

#### Concurrency 4, Msg Size 32KB

**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --concurrency 4
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0005 seconds
* **Total Messages:** 3,124,097
* **Total Data:** 97,628.03 MB
* **Throughput:** 312,393.53 msg/s
* **Throughput Rate:** 9,762.30 MB/s

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
* **Total Messages:** 2,878,977
* **Total Data:** 89,968.03 MB
* **Throughput:** 287,887.15 msg/s
* **Throughput Rate:** 8,996.47 MB/s

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

#### Standard, Concurrency 8
**Command:**
```bash
cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --cork --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0001 seconds
* **Total Messages:** 2,286,920
* **Total Data:** 71,466.25 MB
* **Throughput:** 228,688.71 msg/s
* **Throughput Rate:** 7,146.52 MB/s


#### io-uring with Multishot, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 10.0004 seconds
* **Total Messages:** 2,438,344
* **Total Data:** 76,198.25 MB
* **Throughput:** 243,825.80 msg/s
* **Throughput Rate:** 7,619.56 MB/s

#### io-uring with Multishot and ZeroCopy, Concurrency 1
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --concurrency 1
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 2,090,241
* **Total Data:** 65,320.03 MB
* **Throughput:** 209,026.80 msg/s
* **Throughput Rate:** 6,532.09 MB/s

#### io-uring with Multishot and ZeroCopy, Concurrency 8
**Command:**
```bash
cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 32768 --use-io-uring --uring-multishot --uring-zerocopy --concurrency 8
```

**Metrics:**
* **Pattern:** PushPull
* **Elapsed Time:** 9.9999 seconds
* **Total Messages:** 2,449,825
* **Total Data:** 76,557.03 MB
* **Throughput:** 244,985.04 msg/s
* **Throughput Rate:** 7,655.78 MB/s