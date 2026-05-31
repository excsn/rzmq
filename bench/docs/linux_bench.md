# Benchmark runs

System: AMD Ryzen 5 7640U

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern req-rep --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : ReqRep
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 192244
Total Data                : 11.73 MB
--------------------------------------------------------
Throughput                : 19224.37 msg/s
Throughput Rate           : 1.17 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         40 us
  p50 (Median)    :         51 us
  p90             :         54 us
  p95             :         56 us
  p99             :         62 us
  p99.9           :        121 us
  Max             :        799 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern dealer-router --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : DealerRouter
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0001 seconds
Total Messages            : 247153
Total Data                : 15.09 MB
--------------------------------------------------------
Throughput                : 24715.10 msg/s
Throughput Rate           : 1.51 MB/s
--------------------------------------------------------
Latency Distribution (Microseconds):
  Min             :         33 us
  p50 (Median)    :         37 us
  p90             :         44 us
  p95             :         53 us
  p99             :         66 us
  p99.9           :        117 us
  Max             :       1557 us
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 1658460
Total Data                : 101.22 MB
--------------------------------------------------------
Throughput                : 165845.95 msg/s
Throughput Rate           : 10.12 MB/s
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern pub-sub --msg-size 64

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PubSub
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0031 seconds
Total Messages            : 1438614
Total Data                : 87.81 MB
--------------------------------------------------------
Throughput                : 143816.81 msg/s
Throughput Rate           : 8.78 MB/s
========================================================

Command: cargo run --release --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --cork

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0007 seconds
Total Messages            : 5811066
Total Data                : 354.68 MB
--------------------------------------------------------
Throughput                : 581068.20 msg/s
Throughput Rate           : 35.47 MB/s
========================================================

Command: cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 13644883
Total Data                : 832.82 MB
--------------------------------------------------------
Throughput                : 1364488.11 msg/s
Throughput Rate           : 83.28 MB/s
========================================================

Command: cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --cork

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 22470235
Total Data                : 1371.47 MB
--------------------------------------------------------
Throughput                : 2247023.22 msg/s
Throughput Rate           : 137.15 MB/s
========================================================

Command: cargo run --release --features io-uring --bin rzmq_bench -- --role orchestrate --endpoint tcp://127.0.0.1:19876 --pattern push-pull --msg-size 64 --use-io-uring --uring-multishot

========================================================
               rzmq BENCHMARK FINAL REPORT              
========================================================
Pattern                   : PushPull
Role                      : Client
Message Size              : 64 bytes
Elapsed Time              : 10.0000 seconds
Total Messages            : 14990443
Total Data                : 914.94 MB
--------------------------------------------------------
Throughput                : 1499044.12 msg/s
Throughput Rate           : 91.49 MB/s
========================================================