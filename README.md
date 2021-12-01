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

- Dataset: 1-second granularity

| N Elements | Size in GB | Bytes per row |
| ----------:| ----------:| -------------:|
| 31,535,999 | 3.1        |            100|

<details>
<summary><b>XStore</b></summary>

**Benchmark I: Retaining caches**
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

**Benchmark III: Retaining caches**
- Throughout 1,000 queries, caches are retained from start to finish
- Single-threaded
- Single Open/Close

| Query Execution Time |          Second [s] |        Millisecond [ms] | Microsecond [µs] | Nanosecond [ns] |
|:--------------------:| -------------------:| -----------------------:| ----------------:| ---------------:|
| Min                  |         0.000011016 |                0.011016 |           11.016 |          11,016 |
| Max                  |         0.000077135 |                0.077135 |           77.135 |          77,135 |
| Mean                 |        0.0000133988 |               0.0133988 |          13.3988 |        13,398.8 |
| Stdev                |       0.00000432255 |              0.00432255 |         4.432255 |        4,322.55 |

- **Benchmark IV: Caches cleared**
  - Throughout 1,000 queries, caches are cleared prior to each query
  - Single-threaded
  - Single Open/Close

| Query Execution Time |          Second [s] |        Millisecond [ms] | Microsecond [µs] | Nanosecond [ns] |
|:--------------------:| -------------------:| -----------------------:| ----------------:| ---------------:|
| Min                  |          0.00035584 |                 0.35584 |           355.84 |         355,840 |
| Max                  |           0.0195211 |                 19.5211 |         19,521.1 |      19,521,100 |
| Mean                 |           0.0114994 |                 11.4994 |         11,499.4 |      11,499,400 |
| Stdev                |          0.00179292 |                 1.79292 |         1,792.92 |       1,792,920 |

**Note:** 
- Caches cleared using:

```c++
system("free > /dev/null");
system("sync > /dev/null");
system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
system("free > /dev/null");
```
</details>