# xStore

eXtreme performance in distributed tv-Store for time-series

# Testbed
- Dual socket Intel® Xeon® Silver 4108 Processor @ 1.80GHz (16-cores, 32-hardware threads)
- 8x 8GB (64 GB) of DIMM DDR4 ECC Registered RAM
- 1x Micron 5100 MTFDDAK240TCB SATA SSD (223.5GB)
- Networking:
  - Mellanox Technologies MT27800 Family
  - Intel Corporation Ethernet Controller 10G X550T
- Ubuntu 18.04.4 LTS 
- g++ 8.4.0
- C++17

# Benchmark
<details>
<summary>XStore</summary>
- Dataset: 1-second granularity

| N Elements | Size in GB | Bytes per row |
| ----------:| ----------:| -------------:|
| 31,535,999 | 3.1        |            100|

- **Benchmark I: Retaining caches**
  - Throughout 1,000 queries, caches are retained from start to finish
  - Single-threaded
  - Open/Close file on each query

| Query Execution Time |          Second [s] |        Millisecond [ms] | Microsecond [µs] | Nanosecond [ns] |
|:--------------------:| -------------------:| -----------------------:| ----------------:| ---------------:|
| Min                  |         0.000020403 |                0.020403 |           20.403 |          20,403 |
| Max                  |         0.000104566 |                0.104566 |          104.566 |         104,566 |
| Mean                 |        0.0000229269 |               0.0229269 |          22.9269 |        22,926.9 |
| Stdev                |       0.00000599272 |              0.00599272 |          5.99272 |        5,992.72 |

- **Benchmark II: Caches cleared**
  - Throughout 1,000 queries, caches are cleared prior to each query
  - Single-threaded
  - Open/Close file on each query

| Query Execution Time |          Second [s] |        Millisecond [ms] | Microsecond [µs] | Nanosecond [ns] |
|:--------------------:| -------------------:| -----------------------:| ----------------:| ---------------:|
| Min                  |         0.000674733 |                0.674733 |          674.733 |         674,733 |
| Max                  |           0.0258391 |                25.8391  |         25,839.1 |      25,839,100 |
| Mean                 |           0.0146491 |                 14.6491 |         14,649.1 |      14,649,100 |
| Stdev                |          0.00220903 |                 2.20903 |         2,209.03 |       2,209,030 |

**Note:** 
- Caches cleared using:
```c++
system("free > /dev/null");
system("sync > /dev/null");
system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
system("free > /dev/null");
```
</details>