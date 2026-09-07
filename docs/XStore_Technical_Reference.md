# XStore: A High-Performance Time-Series Storage System — Technical Reference

**Purpose of this document:** a complete technical description of the XStore codebase (architecture, data model, storage engine, concurrency design, networking layer, optimization history, testing/benchmark methodology, and known constraints), written for use as background material when writing a scientific/technical paper about the system.

---

## 1. Provenance and Context

XStore is a research time-series storage system developed at the **Illinois Institute of Technology, Data-Intensive Distributed Systems Laboratory**.

- **Author:** Lan Nguyen (Lnguyen18@hawk.iit.edu)
- **Advisor:** Ioan Raicu (iraicu@iit.edu)
- **Publication:** Lan Nguyen, Ioan Raicu. *"Accelerating CRUD with Chrono Dilation for Time-Series Storage Systems"*, IEEE/ACM Supercomputing (SC) 2023 — poster + extended abstract + presentation.

The repository contains the server (C++17), a C++ client library, a Python client library, a protobuf wire schema, an integration/unit test suite (GoogleTest), an in-repo micro-benchmark suite, and a large empirical benchmark artifact corpus (`10Y_128-Cols_Experiments/`) produced by a **separate sibling project, XBench** (same lab/author, `/home/ubuntu/XBench` in this environment; not itself part of the XStore repository) — a distributed, multi-database benchmarking framework that drives XStore (and several competing TSDB systems) over the network from multiple client nodes. XBench is described in detail in §10.3.

The codebase is under active, iterative performance engineering: the git history (see §9) shows a sequence of profile-driven changes, each with a specific measured or reasoned motivation, several of which were tried, reverted, and re-applied after correction. This history is itself a useful narrative of the design trade-offs made.

---

## 2. System Overview

XStore is a **single-writer-per-file, multi-threaded, RPC-accessed, file-backed time-series row store**. Each logical database ("DB") is a named collection of fixed-width binary rows, physically partitioned into **one file per calendar year** ("year-file"), addressed by direct offset arithmetic from a UNIX timestamp — there is no index, B-tree, or LSM-tree. The row's timestamp *is* its address. `UnaryInsert` durability is backed by a real write-ahead log (§4.12); `BatchInsert`/`RangeInsert` durability is backed by a simpler async data-file fsync coalescer (§4.13) — see those sections for the distinction and what each does and doesn't guarantee.

High-level components:

```
                         ┌─────────────────────────────┐
   Client (C++/Python)   │        ZMQ_Server           │
   XStore_Client  ─req──►│  ROUTER (clients)           │
                         │     ⇅ inproc queue          │
                         │  DEALER → N worker threads  │
                         │     each: REP socket        │
                         │           + XStore instance │
                         └──────────┬──────────────────┘
                                    │ pread/pwrite/mmap/fallocate/pwritev
                                    ▼
                    DataStore/<DBNAME>/<DBNAME>.CFG
                    DataStore/<DBNAME>/<YEAR>.XSTORE  (fixed-width row slots)
                    DataStore/WAL/shard_<N>.WAL        (UnaryInsert durability log)
```

- **Wire protocol:** Google Protocol Buffers v3, over **ZeroMQ** (ROUTER/DEALER broker pattern, `tcp://` transport).
- **Concurrency model:** one OS thread per configured worker; each thread owns its **own `XStore` instance** (stateless request logic) while several caches (mmap cache, fd cache, config cache) are **static/shared across all instances**, sharded to reduce cross-thread lock contention.
- **Storage:** POSIX files, one directory per DB, one file per (DB, calendar year). Row slots are fixed-width and pre-sized (`ftruncate`) to the whole year's capacity at first touch.
- **Row format:** each row is a serialized `RowData` protobuf message (a `repeated FData` — effectively a variable-typed column list) padded/truncated to the DB's configured `rowLength`.
- **No index/log for addressing:** point/range addressing is arithmetic (`offset = (timestamp − yearFirstEpoch) × rowLength`) — durability, not addressing, is what the WAL and fsync coalescer provide (§4.12-4.13).

---

## 3. Data Model

### 3.1 Column value type — `FData`

```protobuf
message FData {
  oneof oneOfData {
    double doubleData = 1;      float floatData = 2;
    int32 int32Data = 3;        int64 int64Data = 4;
    uint32 uint32Data = 5;      uint64 uint64Data = 6;
    sint32 sint32Data = 7;      sint64 sint64Data = 8;
    fixed32 fixed32Data = 9;    fixed64 fixed64Data = 10;
    sfixed32 sfixed32Data = 11; sfixed64 sfixed64Data = 12;
    bool boolData = 13;         string stringData = 14;
    bytes bytesData = 15;
  }
}
```

Each column is a **protobuf `oneof`**, so a column's wire type is self-describing per-row (no fixed schema for column *types*, only for row *count/order*, and rows within a DB are not required to populate every column). This gives per-cell dynamic typing at the cost of oneof tag overhead per column and a runtime type-dispatch switch (`XStore::GetFDataValue`) whenever a column's value must be coerced to a scalar (used by all four aggregate kernels, which normalize every numeric variant to `long double`/`double` for comparison/summation).

### 3.2 Row — `RowData`

```protobuf
message RowData { repeated FData fieldData = 1; }
```

A row is just an ordered list of `FData`. Column identity is **positional** (the Nth `fieldData` entry is column N) — there is no named-column schema stored per row. `RowData` is serialized with `ByteSizeLong()`/`SerializeToArray()` into a **fixed-size slot** (`rowLength`, set once at `CreateDB` time and stored in the DB's `.CFG` file); any row whose serialized size exceeds `rowLength` is **rejected outright** (not silently truncated — this was a historical bug, see §9) and the unused tail of the slot is zero-padded.

A **fully-zeroed row slot is XStore's hole representation** — a byte-0 check (`RowPtr[0] == 0`) on the first serialized byte is used everywhere (scan kernels, `BatchQuery`) as an O(1) "is this row present" test, since a real serialized protobuf message's first byte is always a non-zero varint tag.

### 3.3 Config — `ConfigStruct`

```protobuf
message ConfigStruct {
  string dbName = 1;
  TimeUnit timeResolution = 2;   // SECOND | MILLISECOND | MICROSECOND (client-creatable)
  sfixed32 rowLength = 3;
}
```

Persisted once per DB as `<DataStoreDir>/<DBNAME>/<DBNAME>.CFG` (a single serialized `ConfigStruct`, no framing needed since the file contains exactly one message). `CreateDB` only *accepts* `SECOND`/`MILLISECOND`/`MICROSECOND` (checked against `XStore::PermittedTimeResolution`), but `TimeStruct`/the resolution enum also defines `NANOSECOND`, `MINUTE`, `HOUR`, `DAY` — supported by the time-math layer but not exposed as a creatable resolution at the API boundary.

### 3.4 Wire envelope — `Request`

All 17 RPC kinds (`INFO`, `CREATEDB`, `REMOVEDB`, `REMOVEDBFILES`, `UNARYINSERT/QUERY/DELETE`, `BATCHINSERT/QUERY/DELETE`, `RANGEINSERT/QUERY/DELETE`, `MIN`, `MAX`, `SUM`, `AVG`, `PING`) are carried as one `oneof payload` inside a single `Request` message, so the server parses the wire bytes exactly once and dispatches on `payload_case()` — no separate command-tag pre-parse. This replaced an earlier `RequestType RType` enum field on each message (still present on request messages for backward-compat/logging but no longer the dispatch key).

---

## 4. Storage Engine

### 4.1 Directory / file layout

```
<BaseDir>/DataStore/<DBNAME>/<DBNAME>.CFG        -- one ConfigStruct, written once at CreateDB
<BaseDir>/DataStore/<DBNAME>/<YEAR>.XSTORE        -- one file per calendar year, fixed-width row slots
```

`BaseDir` = the server process's current working directory at startup (assumption: the binary is run from the directory it lives in — see §11, Constraints). All directory-path members are stored **without** a trailing `/` by convention; joining is always done through a single `JoinPath()` helper, never by string concatenation assuming a trailing slash.

### 4.2 Row addressing

For a DB with resolution `R` and row length `L`, and a timestamp `t` (in units of `R`) whose calendar year (in UTC) is `Y`:

```
FirstEpoch(Y) = start-of-year Y expressed in units of R
offset(t)     = (t − FirstEpoch(Y)) × L
```

The year-file is `ftruncate`'d up front to `(LastEpoch(Y) − FirstEpoch(Y) + 1) × L` bytes (i.e. sized for the year's full capacity in slots) the first time it's opened, so every valid timestamp in that year has a pre-existing slot — inserts and deletes never grow/shrink the file (deletes use `fallocate64(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)`, a sparse-file hole-punch that frees the underlying blocks but keeps the file's logical size and slot addressing intact).

### 4.3 Year-bucketing math — `TimeStruct`

Every insert/query/delete must first determine which year-file a timestamp belongs to and that year's `[FirstEpoch, LastEpoch]` bounds. This is implemented as **closed-form proleptic-Gregorian civil-calendar arithmetic** (Howard Hinnant's `days_from_civil`/`civil_from_days` algorithms), *not* `gmtime_r()`/`mktime()`/`timegm()`:

- `DaysFromCivil(y, m, d)` — days-since-epoch for a given (Y, M, D), pure integer math, no table lookups, no libc TZ database access.
- `CivilFromDays(z)` — inverse: (Y, M, D) for a given day-count.
- `FloorDiv` — floor (not truncating) division, needed so pre-1970 (negative) timestamps bucket to the correct day rather than off-by-one.
- `WeekdayFromDays` — 0=Sunday..6=Saturday directly from the day count (matches `tm_wday`), derived from the fact that day 0 (1970-01-01) was a Thursday.

**Why this matters / design rationale (from in-code commentary and commit `9da4d5f`, "Rewrite TimeStruct on civil-date math"):**
1. **Determinism** — `gmtime_r` results are affected by process TZ/locale state; on-disk row addressing must be identical regardless of host configuration. Closed-form math has no such dependency.
2. **Resolution-awareness** — `ToSeconds`/`FromSeconds` explicitly convert `MILLISECOND`/`MICROSECOND`/`NANOSECOND`/`MINUTE`/`HOUR`/`DAY` timestamps to/from whole seconds before calendar decomposition. A prior implementation path (`gmtime_r` called directly on the raw integer) implicitly assumed "raw value = whole seconds", silently mis-bucketing MILLISECOND/MICROSECOND databases into the wrong year's file — a correctness bug fixed by routing every year-bucketing decision through `TimeStruct`.
3. **Performance** — no syscall / libc calendar-table lookup per row. `BatchInsert`/`BatchQuery`/`RangeInsert` additionally exploit that input is (or is sorted to be) timestamp-ascending: a `TimeStruct` is only reconstructed on an actual year crossing (comparing against the previous row's cached `[FirstEpoch, LastEpoch]` window), not per row.

### 4.4 Config cache — sharded `CfgMap`

```cpp
struct CfgShard { std::unordered_map<std::string, ConfigStruct> Map; std::shared_mutex Mutex; };
static constexpr size_t CfgShardCount = 16;
static std::array<CfgShard, CfgShardCount> CfgShards;
```

`GetOrLoadCfg` is the **first thing every single RPC calls** (every insert, query, delete, and all four aggregates) — the hottest piece of shared state in the system, even though the fast path only takes a *shared* (read) lock. It is sharded by `hash(dbName) % 16` specifically to spread that per-call shared-lock acquire/release across 16 independent cache lines instead of one process-wide cache line hit by every worker thread on every call. Fast path: shared-lock lookup; miss path: exclusive lock + double-checked re-lookup + `fopen`/`ParseFromFileDescriptor` from the `.CFG` file, then insert. No eviction (the config set is small and static per DB — bounded by DB count, not row count).

### 4.5 Mmap cache — sharded MRU, used by read paths that scan sequentially

```cpp
struct MappedFile { void *Base; size_t Len; };
struct MmapShard {
    std::list<std::pair<std::string, MappedFile>> Lru;
    std::unordered_map<std::string, ...::iterator> Index;
    std::mutex Mutex;
};
static constexpr size_t MmapShardCount = 64, MmapCacheCap = 256;   // 4 slots/shard
```

Used by `RangeQuery` and all four aggregate scans (`WalkYearSpan` → `GetOrMapFile`). Keyed by full file path, sharded by `hash(path) % 64`. Fast path (already-mapped): lock only the owning shard, look up, MRU-promote (`list::splice`), return the base pointer. Miss path: `open()`/`fstat()`/`ftruncate()`/`mmap(PROT_READ|PROT_WRITE, MAP_SHARED)` are all done **without** holding the shard lock (these are syscalls whose cost scales with file size/disk state — holding the mutex across them would serialize every other thread hashing to the same shard, for any DB, for the syscall's duration); the lock is re-acquired only to install the result, with a double-checked race: if another thread mapped the same file first, the redundant mapping is `munmap`'d and the winner's is used. Bounded to 256 total resident mappings (4 per shard) via LRU eviction on insert.

**Why sharded (not one global mutex + MRU list):** a cold-started server hit by many concurrent first-touches (N threads each first-mapping a different year file at once) previously serialized entirely on one mutex for the lookup+promote step even though the actual open/mmap work was already unlocked. Two threads touching *different* files now usually land in different shards and share no lock at all; only same-file (or same-shard-hash-collision) races still serialize.

**Eviction race fix (commit `d6b9ffe`):** each shard entry is a `shared_ptr<MappedFile>`, not a bare `MappedFile`, and `GetOrMapFile` returns a `shared_ptr<void>` aliased onto that control block rather than a raw `void*`. Previously, LRU eviction (or an explicit `EraseMappedFile`/`EraseMappedFilesUnderPrefix`) called `munmap()` unconditionally as soon as an entry aged out, with no coordination against a thread still mid-access on a raw pointer it had read out of the cache earlier — a use-after-unmap race, reproduced under concurrent RND inserts once the working set exceeded `MmapCacheCap`. The `shared_ptr`'s refcount now tracks every outstanding `GetOrMapFile()` caller in addition to the cache's own slot: eviction/erase only drops the *cache's* reference, and the custom deleter's `munmap()` doesn't run until the last holder — cache or in-flight caller — releases theirs. Callers must keep the returned `shared_ptr` alive for as long as they touch the mapping; that's what defers the `munmap()` past a concurrent eviction.

### 4.6 File-descriptor cache — sharded MRU, used by write paths and point reads

```cpp
struct FdShard { std::list<std::pair<std::string,int>> Lru; ...Index; std::mutex Mutex; };
static constexpr size_t FdShardCount = 4, FdCacheCap = 8;   // 2 slots/shard
```

Used by `UnaryInsert`/`UnaryQuery`/`BatchInsert`/`BatchQuery`/`RangeInsert` via `pwrite()`/`pread()`. Structurally identical MRU-shard design to the mmap cache, but deliberately **much smaller** (8 total vs. 256): an fd-open miss is cheap (microseconds, no page-fault-time block allocation to hide behind an unlocked window), so the cache only needs to cover the handful of year-files actively being written/read at once, not amortize an expensive miss. Fewer shards (4, not 64) for the same reason — with a cap this small, 64 shards would leave under one slot each.

`IsFdCached()` (peek-only, no open/creation) is used by `UnaryQuery` (and, since commit `8c6c4fc`, `BatchQuery`) to test file existence without a `stat()` syscall on a warm cache: only files opened via `GetOrOpenFile` are ever cached, and both removal paths (`EraseOpenFile`/`EraseOpenFilesUnderPrefix`) drop the cache entry in the same call that deletes the file, so a cache hit is itself proof the file exists.

**Eviction race fix (commit `d6b9ffe`), same defect class as the mmap cache above:** entries are `shared_ptr<int>`, and `GetOrOpenFile` returns a `shared_ptr<int>` rather than a bare `int`. Eviction/`EraseOpenFile`/`EraseOpenFilesUnderPrefix` previously `close()`'d the fd unconditionally, racing a thread still mid-`pwrite()`/`pread()` on that exact fd via an earlier `GetOrOpenFile()` return value — either a clean `EBADF` or, worse, a silent write into the wrong file if the closed fd number got reused by an unrelated concurrent `open()` before the racing `pwrite`/`pread` ran. `FdCacheCap`'s small size (8, process-wide) made this easy to hit once more than a handful of year files were concurrently hot. As with the mmap cache, the deleter's `close()` is deferred until every holder — cache and in-flight callers — has released its copy; `BatchDelete` and `RangeDelete` (previously `fopen`/`fclose`-per-call for their `fallocate64()` punches) were also switched onto this cached-fd path in commit `8c6c4fc`, both holding the `shared_ptr` through the `fallocate64()` call for the same reason.

### 4.7 Insert path: `pwrite()` vs. mmap write-fault

**Design decision, reverted once and re-applied (commits `2713d92` → `c33a4cf` (revert) → `9e12711` (reapply)):** `UnaryInsert`/`BatchInsert`/`RangeInsert` write via `pwrite()` into a cached fd, *not* a `memcpy` into a cached `PROT_WRITE` mmap region. Rationale: a `pwrite()` into a still-sparse region is a buffered write whose actual block allocation is deferred to async writeback — same cost whether the target offset is sequential or random. An mmap write-fault into an untouched page **cannot** defer that: it must synchronously allocate a real block before the fault returns, making random-offset (RAND) inserts far more expensive than sequential (SEQ) ones under the mmap-write design. `pwrite()` removes that asymmetry.

**Read path is asymmetric on purpose:** `RangeQuery` and the aggregate scans (`WalkYearSpan`) *do* use the mmap cache (`GetOrMapFile`) because they walk a file in sequential offset order, where mmap's page-fault-on-touch model plus kernel readahead is a net win, and each mapping is reused across many rows in one call. `UnaryQuery` and `BatchQuery`, by contrast, use `pread()` on a cached fd:
- `UnaryQuery`: a single random point read — mmap would pay a synchronous major page fault on nearly every call with no reuse to amortize it.
- `BatchQuery`: row order is *request* order (client-controlled, potentially scattered across the year's full offset span), so mmap's readahead prefetches pages the loop will never touch, and a per-call `madvise(MADV_RANDOM)` to suppress that isn't free once part of the mapping is already resident. A plain `pread()` per row sidesteps all of this.

### 4.8 Delete path

`UnaryDelete`/`BatchDelete`/`RangeDelete` all use `fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, length)` under a write lock spanning the punched range — this deallocates the underlying disk blocks (returning them to the filesystem) while keeping the file's logical size and slot addressing unchanged, so the hole reads back as all-zero bytes (XStore's hole sentinel, §3.2) without any special-case "row deleted" flag.

`BatchDelete` cannot coalesce its `fallocate64` calls the way `WriteRowGroupToFile` coalesces writes (§4.9): the requested timestamps are scattered, not necessarily contiguous, and punching one big range spanning the group's min-to-max offset would also deallocate *other* rows sitting in the gaps between the requested points. The **lock**, however, is still taken once over the whole min-to-max span (harmless to over-cover a lock, unlike a hole-punch).

### 4.9 Write coalescing — `WriteRowGroupToFile`

Shared by `BatchInsert` and `RangeInsert` (`include/XStore/XStore_Internal.h`), templated on an `EntryAccessor` lambda so it works over either message type without materializing an intermediate row vector. Both feed it a `YearBucketMap` (`std::vector<std::pair<year, vector<rowIndex>>>` built by `AppendToYearBucket`, a linear scan over existing buckets before appending a new one) — replacing an earlier `std::map<year, vector<rowIndex>>`, since commit `8c6c4fc`: a single request realistically spans only a handful of distinct years, so a flat vector (no per-key tree-node allocation, no pointer-chasing, whole bucket list fits in a couple cache lines) beats a tree map for that small K. `BatchDelete`/`RangeDelete` (which don't route through `WriteRowGroupToFile`, since deletes can't always coalesce, §4.8) were switched to the same `YearBucketMap`/`AppendToYearBucket` in the same commit. `BatchQuery`'s bucket map is explicitly re-sorted by year after building (unlike `Batch`/`RangeInsert`, whose responses are just a status string) to keep its response chronological across years — matching the old `std::map`'s always-sorted iteration order, which mattered specifically when `SortTime=false` lets buckets get created in request order rather than year order.

1. Take one `FileRangeLockGuard` (WRITE) spanning the whole year-group's min-to-max byte offset — **not** one lock per row (see §5.1 for why whole-span won over per-row after being tried at both extremes).
2. Filter to `Valid` rows whose serialized size fits `rowLength` (oversized rows are rejected, not truncated — a correctness fix, see §9).
3. Scan `Valid` (offset-sorted) for **maximal contiguous runs** — adjacent rows whose offsets differ by exactly `rowLength` with no gap.
4. Serialize each run into one thread-local scratch buffer (grown, never freed, reused across calls) and issue **one `pwrite()` per run**, not one per row.

A fully sequential batch collapses to a single run → a single `pwrite()` for the entire group (mirroring the syscall-coalescing a buffered `fwrite()` stream gets for free). A fully scattered (RAND) batch degrades to one run per row — same syscall count as the naive per-row approach, never worse.

### 4.10 Directory scan / `InfoQuery`

`InfoQuery` enumerates every DB directory and every file within it (`std::filesystem::directory_iterator` + `stat()` per file for `st_blocks × 512` allocated-size accounting), fetching each DB's config through the same shared `CfgMap` cache every other operation uses rather than a fresh `fopen`+parse per DB — relevant because this is a status/monitoring endpoint likely to be polled repeatedly.

### 4.11 Path caching — `CachedDbBasePath` / `CachedYearFilePath`

Two `thread_local` hashmap memos (`include/XStore/XStore_Management.cpp`), introduced in commit `62ef571` to replace an earlier **single-entry** memo:

```cpp
const std::string& CachedDbBasePath(const std::string &dbName) const;                  // dbName -> "<DataStoreDir>/<dbName>"
const std::string& CachedYearFilePath(const std::string &dbBasePath, unsigned short year) const;  // (dbBasePath, year) -> ".../<year>.XSTORE"
```

`DbBasePath()`/the `fmt::format`-built year-file path are both pure (no I/O), but every single op function (Unary/Batch/Range Insert/Query/Delete, all four aggregates via `WalkYearSpan`) calls them unconditionally on every call — a heap allocation paid even on an otherwise-fully-cached fast path. The original design was a **single-entry** `thread_local` memo (last dbName/year wins, invalidated on any change) — fine for a worker thread that stays on one DB/year across consecutive requests, but it thrashes (misses on nearly every call) under a RND-pattern workload where a thread bounces between a handful of distinct years in no particular order (see `UnaryInsertRandomThroughput` in `XStorePerfTest.cpp`). The hashmap version stays warm regardless of access order, since a real dataset's year range is small enough to fit entirely. `CachedYearFilePath` is keyed by `dbBasePath` (not `dbName`) since callers that only have `dbBasePath` in hand (e.g. `WalkYearSpan`, shared by all four aggregates) don't need to separately thread `dbName` through just for this.

**Cross-directory aliasing bug (fixed in commit `8c6c4fc`):** `CachedDbBasePath`'s outer key was `dbName` alone. In production this is safe — `DataStoreDir` is fixed for the life of a server process — but the test suite `chdir()`s into a fresh scratch directory per test case while reusing the same `dbName` (e.g. `"TESTDB"`) across nearly every test; a `dbName`-only memo doesn't expire when a test's directory is torn down, so a later test's lookup for `"TESTDB"` could silently return an earlier, already-deleted test's path. Fixed by keying on `(DataStoreDir, dbName)` — an outer map keyed by `DataStoreDir`, inner map keyed by `dbName` — which collapses back to the single-outer-entry case in production (one `DataStoreDir` per process) while giving each distinct directory its own inner map in tests.

### 4.12 `UnaryInsert` durability — the write-ahead log

`UnaryInsert` is the only operation routed through a true WAL. Each `WalDurabilityMode` maps to a distinct processing model:

- **How it processes the request:** the row is framed into a WAL record (`[u32 recordLen][u32 crc32][u16 dbNameLen][u16 rowLength][i32 timeResolution][u32 rowCount][dbName][epochTime + row bytes]...`) and handed to `EnqueueWalRecord`, which hashes the DB's base path onto one of `WAL_SHARD_COUNT` (8) shards so one DB's writes funnel through one shard's group-commit window. A dedicated writer thread per active shard (`WalWriterLoop`) waits up to `WAL_GROUP_COMMIT_WINDOW_US` (10ms) for the first record, then lingers over an **adaptive** gather window (`WAL_GATHER_WINDOW_MIN_US`=50 to `WAL_GATHER_WINDOW_MAX_US`=250, driven by a per-shard EMA of recent batch sizes) up to `WAL_BATCH_MAX_RECORDS`=256 — a Kafka-style `linger.ms`+`batch.size` dual trigger. The batch is written via `pwritev()` (vectored I/O, zero-copy of row bytes) into a WAL file preallocated to `WAL_FILE_PREALLOC_BYTES`=40MB (so a commit's `fdatasync()` never has to extend the file), then applied row-by-row to the real `.XSTORE` data file, and each record's completion semaphore is resolved with the true durability result.
  - **SYNC:** the caller blocks until both the WAL `fdatasync()` and the data-file apply succeed. Guarantee: return only after the write is durable on stable storage. Cost: one `fdatasync()` per group-commit window, amortized across every record that landed in that window.
  - **ASYNC** (default): the caller's enqueue returns immediately after a lock-free hand-off; the writer thread's group-commit loop above still runs, just off the request's critical path. Guarantee: eventual durability within one gather-window's worth of latency, not immediate. A crash inside that window loses the record — the same trade every ASYNC durability mode makes.
  - **BYPASS:** `ApplyWalRecord` runs inline on the calling ZMQ worker thread, skipping the WAL entirely — no queue, no CRC, no second write. Durability bound is OS writeback timing (`dirty_expire_centisecs`, ~30s default), nothing more.
- **Crash recovery:** `ReplayWal()` runs at `InitDB()`, before any DB is trusted. It scans every `shard_<N>.WAL` file, CRC-validates each record (catching a torn/partial write from a crash mid-`pwritev`), replays valid records via `ApplyWalRecord`, and stops at the first invalid record. `InvalidateWalFileHeader` zeroes the first 4 bytes and `fdatasync()`s after replay so already-applied records don't replay again on a later restart — without ever truncating the preallocated file.
- **Pros:** SYNC gives a real, provable durability guarantee at group-commit-amortized cost, not per-record cost. ASYNC gives near-zero-latency inserts with a bounded, known loss window. Crash recovery is `O(records since the WAL was last invalidated)`, not `O(database size)`, and needs no index reconstruction.
- **Cons:** current implementation has no WAL segmentation/rotation (one fixed-size file per shard, overwritten at offset 0 each time it's invalidated), no LSN tracking, and applies to the data file synchronously in the same writer-thread loop rather than via a decoupled background materializer — see `TODO.md` for this tracked, still-open work.
- **What's novel:** the WAL is **record-oriented, not page-oriented**, exploiting XStore's own O(1) timestamp→offset addressing — recovery never needs to reconstruct or traverse an index, unlike a conventional database's WAL, which typically must also log page/tuple/index metadata. This was the founding design thesis for XStore's WAL (see git history around commits `be5d8d41`..`c748f3ee`) and is largely realized: recovery is genuinely just "verify CRC, compute offset, rewrite."

### 4.13 `BatchInsert`/`RangeInsert` durability — the async data-fsync coalescer

Batch and Range inserts do **not** go through the WAL above. Rows are already written sequentially via the coalesced `pwrite()`s described in §4.9, so instead of a separate log, durability is handled per touched file descriptor:

- **How it processes the request:** after `WriteRowGroupToFile` returns, the same `WalDurabilityMode` enum (reused, with a narrower meaning here) picks the action per touched year-file: **SYNC** calls `fdatasync(fd)` inline, before the reply is built — verified to genuinely block the reply on completion, with a failed fsync surfaced as an error rather than masked as success. **ASYNC** calls `ScheduleAsyncDataFsync(fd)`, which only inserts the fd into a pending map and returns immediately; a single background coalescer thread wakes every `DATA_FSYNC_COALESCE_WINDOW_US` (1000µs), swaps out the pending map, and issues one `fdatasync()` per distinct fd. **BYPASS** does nothing beyond the `pwrite()`s already done, relying on OS writeback timing alone.
- **Pros:** SYNC's guarantee is genuinely equivalent to the WAL's SYNC guarantee for this write pattern — no correctness gap. ASYNC deduplicates fsyncs across many touched files into one coalescer tick, cheaply.
- **Cons (the real, narrower gap — verified, not assumed):** ASYNC here has the same bounded (~1ms nominal) crash-loss window UnaryInsert's own ASYNC WAL path has, but **lacks the WAL's CRC-framed record and replay-on-restart** — if a crash lands inside that window, or the data-file write itself was torn, there is no independently-durable log to recover from. This is a real, if narrow, asymmetry with UnaryInsert's WAL and is tracked in `TODO.md` ("give ASYNC Batch/Range Insert a real log-then-apply safety net").
- **What's novel:** deferring and deduplicating fsyncs by fd, rather than by record, is a much simpler mechanism than a full WAL and is sufficient for SYNC's guarantee — the design deliberately doesn't build a second WAL implementation for a write pattern (large, already-sequential/coalesced writes) where a full log-then-apply model buys comparatively little over `fdatasync` at the OS-file granularity.

---

## 5. Concurrency Control

### 5.1 In-process byte-range locking — `FileRangeLock` / `FileLockRegistry`

Replaces POSIX `fcntl()` record locks, which are scoped to the **process** (not thread or fd) and therefore provide **no** mutual exclusion between XStore's worker threads (each of which independently opens/maps the same files) — `fcntl()` locks would have been silently ineffective for this concurrency model.

```cpp
class FileRangeLock {                  // one instance per file path
  bool Conflicts(start, end, type) const;   // overlap AND (either side WRITE)
  void LockRange(start, length, type);      // blocks (condvar) until no conflict, then registers
  void UnlockRange(start, length, type);    // O(1) swap-and-pop removal, notify_all()
  std::vector<ActiveLock> ActiveLocks;      // linear scan — bounded by worker thread count
};
class FileLockRegistry {               // process-wide, sharded: 16x {shared_mutex, path->FileRangeLock map}
  static FileRangeLock& Get(path);     // hash(path) % 16 picks the shard; shared lock on hit, exclusive+double-check on first-touch miss
};
class FileRangeLockGuard { /* RAII: guarantees UnlockRange runs even on exception/early return */ };
```

- **Semantics:** two ranges conflict only if they **overlap** *and* **at least one side is WRITE**. Disjoint ranges of the same file never block each other regardless of type — unrelated rows/years can be written concurrently by different worker threads. Both-READ overlapping ranges are compatible (shared).
- **`length == 0`** is a sentinel for "rest of file" (kept for compatibility with the old `fcntl` convention) and conflicts with every other range.
- **Conflict check is a linear scan** over `ActiveLocks` (a flat `std::vector`, not a node-based container) — deliberately, since concurrently-held ranges per file are bounded by the worker thread count, making a flat vector cheaper (no per-lock heap allocation, better cache locality) than a tree/map.
- **RAII guard is a correctness fix, not just convenience:** a missed unlock under the old `fcntl` code was a harmless no-op (released automatically on `fclose()`); under this in-process model a missed unlock is a **permanent hang** for any other thread waiting on that range, hence the guard guarantees `UnlockRange` runs on every exit path including exceptions.
- **Registry sharded 16-ways (commit `d6b9ffe`):** `FileLockRegistry::Get()` is called on *every* op (Unary/Batch/Range Insert/Query/Delete, every call, not just on lock contention), so a single global `shared_mutex` guarding the whole `path → FileRangeLock` map was a serialization point shared by all files, not just contending writers of the same file — threads touching completely unrelated files/DBs still bounced the same mutex's cache line. Sharded by `hash(filePath) % 16` (same pattern as `CfgShards`/`FdShards`/`MmapShards`), unrelated files' lookups now usually land in different shards and share no lock at all; only a first-touch on a still-unregistered path (or a same-shard hash collision) briefly takes that shard's exclusive lock.

**Locking granularity trade-off (explicitly noted as tried at both extremes):** per-row locking and whole-span locking were both implemented and benchmarked; whole-span (one lock covering a batch/range group's full min-to-max offset) won and is what `WriteRowGroupToFile`, `RangeQuery`, `WalkYearSpan`, and `BatchDelete` all use. The write *coalescing* (fewer `pwrite`/`pread` calls) is a separate, independently-tunable axis from the *lock* granularity — a whole-span lock does not require a whole-span write; `WriteRowGroupToFile` derives its run-splitting from actual offset contiguity, not from the lock span.

### 5.2 Threading model

One OS thread per configured worker (`ZMQ_Server::NumThreads`, set from a CLI argument), each running an infinite `recv → process → send` loop (`WorkerRoutine`) and owning:
- **Its own `XStore` instance** — stateless per-call logic; the constructor is idempotent per distinct `DataStoreDir` (guarded by a `static` mutex + `unordered_set<string>` of already-initialized dirs), so directory creation + config-cache warm-up (`InitDB()`, a full scan of existing DBs) happens once per process, not once per thread, even though many `XStore` instances are constructed (this also lets tests construct many `XStore` instances against many distinct scratch directories within one process without re-triggering the scan).
- **Its own `google::protobuf::Arena`**, reused via `Arena.Reset()` every iteration rather than reconstructed — keeps the arena's already-allocated blocks around for the next request instead of returning them to the system allocator, so steady-state traffic pays only bump-pointer allocation cost, not block-list malloc/free churn (§7.2).
- **Its own reusable `std::vector<zmq::message_t>`** receive buffer (`.clear()`, not reconstructed, each iteration).

The `XStore` class's **shared state** (mmap/fd/config caches, `FileLockRegistry`) is `static` and therefore genuinely process-wide/cross-thread — this is intentional: the caches and locks must be shared across worker threads, since concurrent workers may touch the same file, while the request-processing logic (`XStore`'s non-static members are all path/config helpers with no per-call mutable state beyond what's passed in) is safely duplicated per thread to avoid any need for per-request synchronization on the "business logic" itself.

---

## 6. Query & Aggregate Execution

### 6.1 `WalkYearSpan` — shared scan driver

```cpp
template <typename Kernel>
void XStore::WalkYearSpan(dbBasePath, cfgStruct, startEpochT, endEpochT, Kernel &&kernel);
```

Common to `MinAggregate`/`MaxAggregate`/`SumAggregate`/`AvgAggregate` (and `RangeQuery`/`RangeDelete` follow the same *shape*, though they aren't literally routed through this template): walks `[start, end]` one year-file at a time, and for each existing year:
1. `GetOrMapFile` — cached mmap (§4.5).
2. Take one READ `FileRangeLockGuard` over that year-segment's byte span.
3. `madvise(MADV_WILLNEED)` over the (page-aligned) span — a **bulk prefetch hint** issued once per segment instead of relying on lazy per-page faulting as the kernel scans forward. This matters more for aggregates than for `RangeQuery` because aggregate kernels always walk the **full span with no early exit** (every row must be visited to compute min/max/sum), so the entire region is guaranteed to be touched.
4. Invoke `kernel(rowsBase, rowsToProcess)` — a pointer directly into the cached mapping, no per-call `mmap()`/`munmap()`.

A missing year contributes nothing (the kernel simply isn't called for it) — consistent with how `RangeQuery`/`RangeDelete` treat a missing year (zero entries / zero deletes), as opposed to a *hole within an existing year*, which is a real (zeroed) row that participates as "no valid data for this timestamp" rather than being silently skipped from iteration.

Since commit `8c6c4fc`, `WalkYearSpan` is invoked two ways: directly (narrow spans, and internally by `WalkYearSpanParallel` below for the sequential fallback), or once per segment from inside a parallel worker thread (wide spans) — see §6.4. The per-call logic (steps 1–4 above) is identical either way; only *how many times* and *from which thread* it's invoked changes.

### 6.2 Skip-parse column extraction — `ExtractColumnByIndexSkipParse`

The aggregate kernels only ever need **one column** (`columnIdx`) out of a row. The original implementation fully parsed every row into a `RowData` (`MergePartialFromCodedStream`), decoding and heap-allocating an `FData` for every column, then discarded all but one. `ExtractColumnByIndexSkipParse` instead walks the row's raw protobuf wire bytes directly:

```cpp
while ((Tag = CodedIn.ReadTag()) != 0) {
    if (WireFormatLite::GetTagFieldNumber(Tag) != RowData::kFieldDataFieldNumber) {
        if (!WireFormatLite::SkipField(&CodedIn, Tag)) return false;
        continue;
    }
    if (Occurrence == columnIdx) return WireFormatLite::ReadMessage(&CodedIn, &outData);
    if (!WireFormatLite::SkipField(&CodedIn, Tag)) return false;
    ++Occurrence;
}
return false;  // ran out of columns before reaching columnIdx
```

Every non-target column is skipped by **cursor advance only** (`WireFormatLite::SkipField`) — no `FData` object is constructed for it. `ReadTag() == 0` is the same end-of-row/short-row signal the old full-parse path relied on via `fielddata_size()` bound checking. One deliberate semantic difference from full-row parsing: because this returns as soon as the target column is read, corruption in a *later* column no longer fails the read of an earlier, valid target column the way a full-row parse would (a relaxation, not a regression, for this use case).

**Measured impact:** benchmarked ~40–54% faster than the old full-parse kernel across N = 100/1k/10k rows (commit `764999e`; the A/B perf-test harness that produced this number was removed after the skip-parse kernel was promoted to the only implementation, since there was no longer a second kernel to compare against).

### 6.3 Aggregate kernels

```cpp
template <typename Cmp>
void ProcessFileRangeSelect(rowsBase, totalRows, rowLen, columnIdx, ValueStruct& best, FData& bestData);
//   Cmp = std::less<long double>    -> MIN
//   Cmp = std::greater<long double> -> MAX

void ProcessFileRangeAccumulate(rowsBase, totalRows, rowLen, columnIdx, double& sum, uint64& validCount);
//   backs SUM directly; AVG = sum / validCount after the same accumulate pass
```

Both kernels: for each row, skip if the first byte is `0` (hole, §3.2); else `ExtractColumnByIndexSkipParse`; if the extracted `FData`'s `oneOfData_case()` is one of the 12 **numeric** variants (string/bytes/bool are excluded — the `case() > 0 && case() <= 12` check), coerce it to a scalar via `GetFDataValue`'s type-switch and fold it into the running best/sum. `ProcessFileRangeSelect` is **explicitly instantiated** for exactly the two comparators ever used (`std::less`/`std::greater<long double>`), so the compiler generates two fully-inlined, branch-free (on `Cmp`) versions with no `std::function`/vtable indirection.

`ValueStruct` is a minimal `{unsigned int Index; long double Value; bool IsInitialized;}` — `long double` is used as the common comparison type across all 12 numeric protobuf wire types so `ProcessFileRangeSelect` doesn't need a template per source type.

### 6.4 Multi-year parallel aggregate scanning — `WalkYearSpanParallel`

```cpp
template <typename PartialState, typename SegmentKernel, typename MergeFn>
PartialState XStore::WalkYearSpanParallel(dbBasePath, cfgStruct, startEpochT, endEpochT,
                                            PartialState initialState, SegmentKernel &&segmentKernel, MergeFn &&mergeFn);
```

Added in commit `8c6c4fc`. All four aggregates (`Min`/`Max`/`Sum`/`AvgAggregate`) now route through this instead of calling `WalkYearSpan` directly:

1. **Enumerate segments first** — one `[Start, End]` pair per calendar year in `[startEpochT, endEpochT]`, computed via `TimeStruct` the same way `WalkYearSpan`'s internal loop does. This is O(#years), not O(#rows), so it's cheap even for a wide span, and lets the function decide up front whether parallel dispatch is worth it before committing to any thread spawning.
2. **Narrow span (`< kMinSegmentsForParallelWalk` = 4 distinct years):** delegates straight to a single `WalkYearSpan` call over the whole `[startEpochT, endEpochT]` — byte-identical code path to before parallelism existed, no threads, no merge step. This is the common case: perf-suite defaults and most real queries never span more than a handful of years, and thread creation (tens of microseconds each) plus the final merge isn't worth paying for a span the sequential walk finishes just as fast anyway.
3. **Wide span (≥ 4 years):** spawns `min(#segments, hardware_concurrency())` `std::thread`s, **striped** across segments (thread *t* takes segments *t, t+NumThreads, t+2·NumThreads, …*, not a contiguous chunk) so a handful of unusually large years don't all land on one thread. Each thread accumulates into its **own** `PartialState` (never a value shared across threads — protobuf/`ValueStruct` objects aren't safe for concurrent mutation even on logically-disjoint fields per protobuf's own thread-safety docs), calling `WalkYearSpan` once per segment it owns (each segment falls entirely within one year, so `WalkYearSpan`'s internal loop runs exactly once per call — same `GetOrMapFile`/lock/`madvise`/kernel logic as the sequential path, just invoked per-segment).
4. After `join()`, `mergeFn(finalState, partialState)` folds each thread's partial result into the running total **sequentially** — O(#threads), not O(#rows).

`MinAggregate`/`MaxAggregate` use a `MinMaxPartial{ValueStruct Value; FData Data;}` partial state, merging by strict improvement only (never on a tie — same rule `ProcessFileRangeSelect` itself uses); ties **across** different threads' segments resolve by thread/segment processing order rather than strict chronological order in that specific case, a relaxation not exercised by any current test and considered acceptable since real sensor/metric data essentially never produces an exact cross-year tie on an extremum. `SumAggregate`/`AvgAggregate` use a `SumPartial{double Sum; uint64 Count;}`, merged by plain addition — narrow spans are untouched (byte-identical to the pre-parallel sequential sum, including the exact-value check in `SumAggregateMatchesIndependentlyComputedSumFromRawDiskBytes`), but a wide-span parallel sum **reassociates** the floating-point addition relative to a strict single-pass left-to-right sum (per-year partial sums combine in a different order), which can differ by a few ULP from the sequential result for the same data — an inherent property of any correct year-parallel sum, not an avoidable implementation choice.

**Cross-request thread-pool scope (verified, corrected):** `WalkYearSpanParallel`'s `hardware_concurrency()` cap bounds only *this request's own* thread count — it has no visibility into other concurrent requests' thread spawns. There is a real, if incidental, partial bound worth crediting: each aggregate request runs synchronously on one ZMQ worker thread for its entire duration (a `REP` socket enforces strict recv-then-send ordering), so the number of *concurrently in-flight aggregate requests* is capped at the ZMQ worker count (physical core count by default). But that cap doesn't bound *threads spawned per request* — each in-flight request can still independently spawn up to `hardware_concurrency()` more threads, so the two caps multiply rather than nest (`workers × hardware_concurrency()`; worst case several hundred threads on a modest multi-core box). This has not been exercised by any perf or integration test at the concurrency level that would expose it — the in-repo perf suite's aggregate benchmarks are single-year (never reach the parallel path at all), and the "benchmarked under 4-concurrent wide-span load, no clear regression" data point referenced in earlier revisions of this document was an ad hoc manual run, not a checked-in regression test. A pool-based fix already exists elsewhere in this codebase and could be applied here: `XStorePreadPool::ParallelFor` (§6.2's sibling infrastructure, used by `BatchQuery`) already caps total concurrent work items pool-wide via a ticket model — routing `WalkYearSpanParallel`'s segment work through it instead of raw `std::thread` spawn would give this exactly the cross-request coordination it currently lacks. Tracked in `TODO.md`.

**Measured impact:** 5.08x speedup on wide multi-year aggregate queries; no measurable regression on narrow/single-year throughput.

**Removed in the same commit:** the previous §6.4, `ProcessFileRangeSelectTypedDouble`/`MinAggregateTypedDouble` — a profiling-only MIN kernel that skipped `ExtractColumnByIndexSkipParse` for a full `RowData` parse and compared `doubleData` natively (no `GetFDataValue` switch, no `long double` promotion), used only to measure the delta a fully type-specialized kernel would buy over the generic skip-parse path. Deleted along with its perf-suite A/B tests once that comparison had served its purpose — it was never part of the four-aggregate production API surface.

---

## 7. Networking Layer

### 7.1 ZeroMQ broker topology

```cpp
zmq::socket_t ClientSocket(Ctx, zmq::socket_type::router);  // faces clients, tcp://<addr>:<port>
zmq::socket_t WorkerSocket(Ctx, zmq::socket_type::dealer);  // faces workers, inproc://workers
zmq::proxy(ClientSocket, WorkerSocket, nullptr);            // broker: load-balances client reqs across workers
```

Each of `NumThreads` worker threads connects a `REP` socket to `inproc://workers` and runs an independent `recv → dispatch → send` loop. This is the classic ZeroMQ **ROUTER-DEALER broker / REP worker pool** pattern: the broker transparently multiplexes many concurrent client `REQ` connections onto a fixed worker pool, and `zmq::proxy` handles the envelope routing so individual workers never see client identity framing directly — they just `recv`/`send` one request/response per REP-socket round trip.

### 7.2 Per-request allocation strategy — Arena

```cpp
google::protobuf::Arena Arena;      // one per worker thread, constructed once
...
while (true) {
    Arena.Reset();                  // frees everything from the previous request in one shot
    auto *Req = google::protobuf::Arena::Create<Request>(&Arena);
    Req->ParseFromArray(...);
    ...
    auto *Rep = google::protobuf::Arena::Create<BatchQuery_REP>(&Arena);  // for wide/nested response types
    MyStore.BatchQuery(Req, Rep);
    SendProto(Socket, *Rep);
}
```

Applied selectively: **incoming requests whose payload nests repeated messages** (`BatchInsert_REQ`, `RangeInsert_REQ` — each row is a `RowData` → `repeated FData`) are parsed straight into arena memory automatically as part of parsing the top-level `Request`, no extra code needed at the call site. **Response types with nested repeated fields** (`UnaryQuery_REP`, `BatchQuery_REP`, `RangeQuery_REP` — each response row owns its own `RowData` → `repeated FData`) are explicitly `Arena::Create`'d, since under the ordinary allocator a wide/long response means one heap allocation per row plus one per column inside it. Response types that are just a flat status string (`CreateDB_REP`, `RemoveDB_REP`, `UnaryInsert_REP`, `BatchInsert_REP`, `RangeInsert_REP`, delete-family `_REP`s) are **not** arena-allocated — there's nothing nested to save an allocation on, so a plain stack-local suffices.

`UnaryQuery` is called out in-code as likely the **highest-QPS RPC** (single-row point queries, not scaled by span like `RangeQuery`) — its response is arena-allocated even though it's not "wide," because the per-call cost (one allocation, paid on *every* call with zero reuse across calls) matters even at a small constant size once QPS is high enough.

### 7.3 Request dispatch

`Request::payload_case()` (the `oneof` tag) is the single dispatch key — a `switch` over 18 cases (17 RPC kinds + `PAYLOAD_NOT_SET`) in `WorkerRoutine`. An unrecognized/empty payload still gets an (empty) reply sent, so a client's `REQ` socket (which enforces strict send-then-recv ordering) is never left hanging on a request the server silently drops.

### 7.4 Response serialization

Most RPCs — everything except `BatchQuery`/`RangeQuery` — reply via a single-frame helper:

```cpp
template <typename T> void SendProto(zmq::socket_t &socket, const T &msg) {
    size_t Size = msg.ByteSizeLong();
    zmq::message_t Reply(Size);
    msg.SerializeWithCachedSizesToArray(Reply.data());  // reuses ByteSizeLong()'s result, not a second size pass
    socket.send(Reply, zmq::send_flags::dontwait);
}
```

`dontwait` is kept for parity with the original fire-and-forget send (no result check) — on a `REP` socket, ZeroMQ's own recv-then-send state-machine enforcement means the send can't actually block here regardless of the flag.

**`BatchQuery`/`RangeQuery`: streamed, skip-copy multipart replies.** These two RPCs never build a full response object tree. `include/XStore/XStore_WireEncode.h` walks each row's on-disk bytes with a `CodedInputStream`/`WireFormatLite` cursor (`RealRowDataLength()`) to find the row's true serialized length (skipping the zero-padding), then hand-writes the wire tags for one `UnaryQuery_REP`-shaped entry (`queryresponse` field, inner `timestamp`+`data`) directly into an outbound chunk buffer — copying the on-disk row bytes verbatim rather than constructing a `RowData`/`FData` object graph and re-serializing it. Chunks are capped at `XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES` (64MB) and sent as successive ZMQ frames (`sndmore` on all but the last); the client's `SendRecv` reassembles all frames into one buffer before a single `ParseFromArray` call (protobuf doesn't care where the bytes were split, only that they're concatenated in order).

- **Pros:** server-side peak memory for a query never exceeds one chunk, regardless of how large the eligible result is — this is what let a prior full-object-tree-per-response design's permanent RSS floor under Arena `Reset()` (see §9) go away entirely for these two RPCs.
- **Cons:** this only bounds server memory. It does not lift protobuf's ~2GB message ceiling, since the client still reassembles everything into one buffer before parsing — a request whose *total* result would exceed 2GB is rejected up front, before any chunk is built (§11).
- **What's novel:** skip-copying raw, already-serialized row bytes straight into the outbound wire format — reusing the same wire-tag-walk technique the aggregate kernels use for skip-parse (§6.2), just applied to encoding instead of decoding — avoids the allocate-decode-then-reallocate-reencode round trip a naive streaming implementation would still pay per row.

---

## 8. Client Libraries

### 8.1 C++ (`client_src/Cpp/XStore_Client.h`) — header-only

- All 17 RPCs funnel through one templated `SendRecv<RepT>(const Request&)`: serialize → blocking `send` → blocking `recv` (`zmq::recv_multipart` into a reused member buffer) → parse. Centralizing this means the 17 call sites can't drift out of sync the way hand-copied send/recv blocks would.
- **Timeout semantics:** constructor takes `timeoutMs` (default −1 = block forever, matching legacy behavior). A `REQ` socket that hits a send/recv timeout is left in an **undefined state machine position** per ZeroMQ's own documentation — the documented fix is a fresh socket, not a retried send, hence `Reconnect()` exists as an explicit required step after catching a timeout, rather than being automatic/hidden.
- **Warmup:** `Ping()` (RPC #18, added specifically for this — an empty round trip with no server-side `XStore` work) forces the lazy `connect()`'s TCP + ZMTP handshake at construction time (`warmupOnInit`, default `true`) rather than on the first real RPC, so the first *business* call doesn't eat handshake latency. `InfoRequest()` was previously (ab)used for this and does strictly more server-side work (directory scan) than necessary for a pure liveness/warmup check.
- Batch/Range insert helpers offer both a convenience overload (`vector<pair<timestamp, RowData>>`, internally Arena-built to amortize per-row submessage allocation for large batches) and an advanced overload that accepts a pre-built request message directly (move-friendly, for callers who want to reuse/construct the message themselves).
- `XStore_FData::Double()/Int32()/String()/...` factory helpers exist because proto3 gives `sint32`/`fixed32`/`sfixed32` (etc.) the *same* C++ parameter type (`int32_t`) despite different wire encodings — overloading on parameter type alone can't disambiguate them, so intent is named explicitly instead.
- **`SuggestRowLength<Alignment=8>`** — a static helper that estimates a `rowLength` to pass to `CreateDB`, given one sample `RowData`: switches on each `FData::oneOfData_case()` (including the `NOT_SET`/hole case, handled as zero-cost), sums `sizeof(type) + PROTOBUF_FIELD_OVERHEAD` per field, rounds up to `Alignment`. This is a **pre-`CreateDB` sizing estimate** from a hypothetical sample row — a forward-looking convenience, not an audit of an already-configured DB's real usage (that's a separate, already-solved problem — see the `xstore-viewer` tooling below).

### 8.2 Python (`client_src/Python/XStore.py` + generated `XStore_pb2.py`)

Generated from the same `XStore.proto` (kept in sync with the C++ server's `Request`-envelope/oneof protocol — commit `115db5e` specifically fixed the Python client to wrap requests in the `Request` envelope and resync its generated `_pb2.py` after the server-side protocol changed, which is a concrete example of the wire-schema/client drift risk inherent to hand-maintained multi-language clients against one `.proto`). Kept in feature parity with the C++ client, including `WalDurabilityMode` exposure and a Python equivalent of `SuggestRowLength`.

### 8.3 Tooling — `xstore-viewer`

A separate desktop/web-server tool (`tools/xstore-viewer/`, Rust backend + JS frontend) for browsing DBs, running ad hoc queries, and surfacing operational diagnostics that don't belong in the wire protocol itself. Notably, it implements the **row-size sizing guide** — sampling real query results, computing each row's true serialized size (`convert.rs`, via `encoded_len()`), and comparing that against the DB's configured `rowLength` (from `DbStats`) to flag wasted padding or truncation risk (`app.js`'s `slotBadge()`, rendered in the DB list and status bar as "too small" / "N% waste" / OK). This is the functional answer to "is `rowLength` sized well for this DB" — a diagnostic that lives in the viewer rather than the client library, since it needs a live query result to sample against, not just a hypothetical row.

---

## 9. Optimization History (Design Evolution)

The git history is a sequence of individually-justified, often individually-*measured* performance changes. In rough chronological/causal order (not strictly commit order):

| Change | From → To | Motivation |
|---|---|---|
| Locking primitive | `fcntl()` process-scoped record locks → in-process `FileRangeLock`/`FileLockRegistry` byte-range locks | `fcntl()` locks don't provide mutual exclusion *between threads* of the same process — ineffective for a thread-pool concurrency model. |
| Insert I/O path | `PROT_WRITE` mmap + memcpy → cached fd + `pwrite()` | mmap write-faults into untouched pages synchronously allocate a block before returning; `pwrite()` defers to async writeback regardless of offset pattern — removes SEQ/RAND asymmetry. Tried, reverted once when it apparently regressed something, root-caused, and reapplied (`2713d92`→`c33a4cf`→`9e12711`). |
| Per-row lock/write granularity | one lock + one `pwrite()`/`pread()` per row → one lock per year-group + coalesced multi-row `pwrite()` runs | Both extremes (per-row, whole-span) were implemented and compared; whole-span won. Write coalescing detects contiguous runs so a fully sequential batch collapses to one syscall. |
| Aggregate scan I/O | fresh `fopen`+`mmap`+`munmap` per aggregate call → shared MRU-cached `GetOrMapFile` mapping (same cache every other read path uses) + `madvise(WILLNEED)` prefetch | Removes per-call mmap setup/teardown cost; prefetch avoids one page fault per touched page during a full-span scan. |
| Aggregate column extraction | full `RowData` parse (all columns decoded/allocated) → wire-level skip-parse of only the target column | Benchmarked ~40–54% faster (N=100/1k/10k); the old kernel and its A/B test harness were deleted after promotion. |
| Year-bucketing | `gmtime_r()` per row → closed-form civil-calendar math (`TimeStruct`) | Determinism (no TZ/locale dependency for on-disk addressing), resolution-awareness (fixes a MILLISECOND/MICROSECOND mis-bucketing correctness bug), and removes a libc calendar call from the hot path; further amortized by reusing the previous row's year-window across sorted-ascending batches. |
| Shared-state locking granularity | one global mutex per cache (mmap cache, fd cache, config cache) → sharded mutex arrays (64/4/16 shards respectively) | Cuts cross-DB/cross-file lock contention on cold-start bursts and steady-state hot paths; shard counts are individually tuned to each cache's miss cost and cap size, not uniform. |
| Request/response allocation | default heap allocator (per-submessage `new`) → per-worker-thread `google::protobuf::Arena`, `Reset()` each iteration | Converts per-row/per-column heap churn in wide batch/range responses into bump-pointer allocation off reused blocks; applied selectively (only to message trees with real nesting). |
| Per-call transient allocation | fresh `std::string`/`std::vector` per call (path formatting, serialize buffers, receive buffers) → `thread_local`/reused member buffers, grow-only | Removes a heap allocation from `CachedYearFilePath` (path string), `RowBuf`/`RunBuf`/`BufferIn` (serialize/read scratch), and the ZMQ receive-message vector — all reused across calls within a thread instead of reconstructed. |
| Wire dispatch | separate lightweight command-tag parse + full payload parse → single `Request` envelope `oneof`, `payload_case()` as the dispatch tag | One parse of the wire bytes total, not two. |
| Row-size validation | unchecked `SerializeToArray` (oversized rows silently truncated, corrupting the following row's slot) → `ByteSizeLong()` bound check, reject-and-report if oversized | Correctness fix; `ByteSizeLong()`'s result is also *reused* (not recomputed) for the actual serialize call, so the fix didn't add a second full-size computation. |
| Client warmup | `InfoRequest()` (directory scan) used as a connectivity probe → dedicated `Ping` RPC (empty round trip, no server-side `XStore` work) | Removes unnecessary server-side I/O from a pure liveness/handshake-forcing check. |
| Mmap/fd cache eviction | bare pointer/fd in cache, unconditional `munmap()`/`close()` on evict → `shared_ptr<MappedFile>`/`shared_ptr<int>` entries, deleter-deferred `munmap()`/`close()` | Fixed a use-after-unmap / use-after-close race: eviction could run while another thread was still mid-access on a pointer/fd read from the cache earlier, causing silent concurrent-insert data loss (reproduced under concurrent RND inserts once the working set exceeded cache capacity). Commit `d6b9ffe`. |
| Byte-range lock registry | one process-wide `shared_mutex` guarding the whole `path → FileRangeLock` map → 16-way sharded (`hash(path) % 16`) | `FileLockRegistry::Get()` runs on every single op call; one global mutex serialized lookups across *all* files, not just contending writers of the same file. Commit `d6b9ffe`. |
| DB-base-path / year-file-path formatting | fresh `DbBasePath()`/`fmt::format()` call per op, then a **single-entry** `thread_local` memo → per-thread hashmap memo (`CachedDbBasePath`/`CachedYearFilePath`) | The single-entry memo thrashed (missed nearly every call) under RND-pattern workloads bouncing between a handful of years in no particular order; a hashmap stays warm regardless of access order since a real year range is small. Commit `62ef571`. |
| `CachedDbBasePath` cache key | keyed by `dbName` alone → keyed by `(DataStoreDir, dbName)` | `dbName`-only keying let a later test's lookup for a reused `dbName` (e.g. `"TESTDB"`) silently return an earlier, already-torn-down test's path when the test suite `chdir()`s between scratch directories reusing the same `dbName`; harmless in production (one fixed `DataStoreDir` per process) but a real cross-directory aliasing bug in the test/embedding case. Commit `8c6c4fc`. |
| Aggregate scan dispatch | single-threaded `WalkYearSpan` over the whole span, always → `WalkYearSpanParallel`: narrow spans (< 4 years) unchanged, wide spans (≥ 4 years) striped across `min(#years, hardware_concurrency())` threads with per-thread partial state + sequential merge | Measured 5.08x speedup on wide multi-year aggregate queries with zero regression on narrow/single-year throughput. Reassociates Sum/Avg's floating-point addition (ULP-level difference from the sequential result) as an inherent, accepted tradeoff. Commit `8c6c4fc`. |
| `Batch`/`RangeInsert`/`Query`/`Delete` year-bucketing container | `std::map<year, vector<rowIndex>>` → flat `YearBucketMap` (`vector<pair<year, vector<rowIndex>>>`) with linear-scan `AppendToYearBucket` | A single request realistically touches only a handful of distinct years — a flat vector beats a tree map's per-key allocation/pointer-chasing at that small K. Commit `8c6c4fc`. |
| `BatchInsert`/`BatchQuery` DB-existence check | explicit `std::filesystem::is_directory()` stat before `GetOrLoadCfg` → dropped, relies on `GetOrLoadCfg`'s own stat-on-miss | `GetOrLoadCfg` already stats the `.CFG` file on a cache miss; the upfront check was a redundant `stat()` on every call regardless of cache state. Same fix already applied to the Unary ops earlier. Commit `8c6c4fc`. |
| `dbName` parameter passing | `std::string dbName` (by-value copy) on every public `XStore` method → `std::string_view dbName`, uppercased once into a reused `thread_local` buffer (`UpperCaseInto`) | Removes a `std::string` construction/copy at every call boundary plus the old copy-then-transform-in-place's second pass; one read-uppercase-write pass instead. Commit `8c6c4fc`. |
| `UnaryInsert` durability | OS page cache only, no explicit fsync barrier → real write-ahead log (CRC-framed records, per-shard adaptive group-commit writer threads, `ReplayWal` crash recovery) | Gives `UnaryInsert` a provable durability guarantee (SYNC) with group-commit-amortized cost, and a bounded-loss ASYNC mode, without needing an index for recovery (§4.12). |
| `BatchInsert`/`RangeInsert` durability | none beyond OS writeback → async data-fsync coalescer, deduplicated per touched fd | Simpler than a full WAL, sufficient for these operations' already-sequential/coalesced write pattern (§4.13); SYNC mode's guarantee is verified equivalent to the WAL's. |
| `BatchQuery`/`RangeQuery` response construction | build full `RowData`/`FData` response object tree, then serialize | streamed, skip-copy multipart encoding straight from on-disk row bytes (§7.4) | Removed a permanent RSS floor under Arena `Reset()` for wide responses; server-side peak memory per query now bounded by one chunk (64MB), not the full result size. |

Several earlier cache-sharding rows above (mmap/fd/config/lock-registry) share one underlying rationale — a single global mutex per shared cache became a cross-DB/cross-file serialization point once every op called into it — with shard counts individually tuned per cache's own miss cost and cap size; see §4.4-§4.6 and §5.1 for the per-cache specifics rather than repeating them here.

This table is a synthesis of commit messages and in-code rationale comments; the two agree closely, since the code comments in this repository are written to explain *why* a decision was made (including alternatives tried and rejected), not merely what the code does.

---

## 10. Testing & Benchmark Methodology

### 10.1 Test suite (GoogleTest, fetched via CMake `FetchContent` at v1.17.0)

| File | Approx. size | Focus |
|---|---|---|
| `tests/integration/XStoreTest.cpp` | 2921 lines, 102 `TEST_F` cases | Correctness of every `XStore` method directly (no network): CRUD, batch/range ops, all four aggregates including edge cases (holes, missing years, mixed/wrong-typed columns, multi-year spans, empty ranges), plus (as of commits `d6b9ffe`/`8c6c4fc`) concurrent fd/mmap-cache eviction-race regression tests (e.g. `ConcurrentUnaryInsertAcrossManyYearFilesDoesNotLoseDataToFdCacheEviction`) and parallel-aggregate-walk correctness tests (`AggregateOverWideMultiYearSpanUsesParallelWalkCorrectly`, `MinAggregateTieAcrossYearsUnderParallelWalkPicksCorrectValue`). |
| `tests/integration/XStoreZmqNetworkTest.cpp` | 543 lines, 11 cases | Same operations driven **over the wire** (real ZMQ client/server), including concurrency (`ConcurrentClientsUnaryInsertAndQueryOverWireDoNotCorruptData`) and error-path behavior (nonexistent DB, duplicate `CreateDB`). |
| `tests/integration/XStoreClientLiveTest.cpp` | 279 lines, 5 cases | `XStore_Client` warmup/timeout/reconnect behavior against a live server. |
| `tests/integration/XStoreClientTest.cpp` | 38 lines, 3 cases | Client-side unit behavior not requiring a live server. |
| `tests/perf/XStorePerfTest.cpp` | 586 lines, 12 `TEST_F` cases | Throughput/latency micro-benchmarks (below); the typed-vs-generic MIN comparison cases (§6.4) were removed in commit `8c6c4fc` along with the prototype kernel they benchmarked, and two parallel-aggregate-path cases were added in its place. |

Total: **121 test cases** across correctness + client + network layers, independent of the perf suite.

### 10.2 In-repo performance micro-benchmarks

`tests/perf/XStorePerfTest.cpp` is deliberately **not** built on a benchmark library (no `google/benchmark` dependency) — each case is a plain timed loop (`std::chrono::steady_clock`) reported via `std::cout`, with only soft correctness assertions (no crash / no data loss), **no hard throughput/latency thresholds asserted** (explicitly documented: absolute numbers vary too much by machine to make a good CI gate; this suite exists to be read by a human doing perf work — e.g. comparing before/after a change — not to fail a build).

- Default op count: 200,000, overridable via `XSTORE_PERF_N` environment variable.
- Logging is forced to `spdlog::level::warn` in `SetUp()` — at 200k+ calls, per-call `[INFO]` stdout logging would dominate the timed loop and measure logging cost instead of `XStore`'s.
- Covers: Unary Insert/Query throughput (one row per call — "the shape of a single live write/read in production"), Batch Insert/Query throughput, and comparative kernels (`MinAggregateTypedVsGeneric_{Small,Medium,Large}` — the typed-double prototype vs. the generic skip-parse kernel, §6.4).
- Reported metrics per case: `rows`, `elapsed` (s), `throughput` (rows/sec), `avg_latency` (µs/row).

### 10.3 XBench — the external, multi-database distributed benchmark harness

**XBench** ("A distributed benchmarking framework for TSDB") is a sibling project from the same lab/author (found at `/home/ubuntu/XBench` in this environment; ships its own `README.md`/`instructions.md` and is *not* part of the XStore repository — it depends on XStore as a **git submodule**, `external/XStore` → `https://gitlab.com/lvn2007/XStore.git`, so it links against XStore's real client library rather than reimplementing wire logic). Its purpose is to drive **head-to-head comparative benchmarks** of XStore against other time-series-capable datastores under an identical workload generator and driver harness.

**Systems under test (per-DB adapter classes, uniform static-method interface):**

| Adapter | Wired into `main.cpp` dispatch tables? | Notes |
|---|---|---|
| `XStore_Adapter` | Yes | Wraps `XStore_Client` (the same C++ client described in §8.1). |
| `MongoDB_Adapter` | Yes | Only other adapter supporting arbitrary-timestamp-set `BatchQuery`. |
| `InfluxDB_Adapter` | Yes | `AggQuery` aggregate-type strings differ in case/spelling from XStore's (`"MIN"` vs Mongo's `"min"`; `AVG` maps to InfluxDB's `MEAN`). |
| `GraphiteDB_Adapter` | Yes | |
| `TimescaleDB_Adapter` | **No** — source exists (`TimescaleDB/TimescaleDB_Adapter.{h,cpp}`) but is not `#include`'d or wired into `main.cpp`'s dispatch maps; effectively unused/in-progress in this checkout. | |

`main.cpp` dispatches every operation through a `std::map<std::string, DispatchFn>` keyed by DB name, where `DispatchFn` is one function-pointer type shared by **every** adapter's `UnaryQuery`/`RangeQuery`/`BatchQuery`/`UnaryInsert`/`RangeInsert`/`BatchInsert`/`AggMin`/`AggMax`/`AggSum`/`AggAvg` methods — collapsing what would otherwise be a per-call-site 4-way if/else chain into one map lookup. Not every DB supports every operation shape: only XStore and MongoDB expose arbitrary (non-contiguous) timestamp-set `BatchQuery` (InfluxDB/Graphite fall through to an explicit "unsupported" warning); InfluxDB/MongoDB/Graphite have no wire-level distinction between a contiguous-range insert and an arbitrary-batch insert, so their "SEQ insert" path reuses the same `BatchInsert` adapter method XStore's dedicated `RangeInsert` RPC exists specifically to avoid.

**CLI surface** (`argparse`-based, `include/argparse.hpp`), four subcommands — `queryBench`, `aggBench`, `insertBench`, `protoBench` — sharing most flags: `-i/--ip_address`, `-p/--port_number`, `-d/--db_name {XSTORE,MONGODB,INFLUXDB,GRAPHITE}`, `-t/--thread_count`, `-e/--experiment_type {UNARY_SEQ,UNARY_RAND,BATCH_SEQ,BATCH_RAND}`, `-n/--n_iteration`, `-b/--batch_iteration` (batch/range group size, 0 for unary workloads), `-ia/--invoke_at` (a scheduled start time, letting multiple distributed client processes launch their runs synchronized rather than racing to start), `--debug`.

**Synthetic dataset generation (`DataManager`, using Apache Arrow/Parquet):**
- Each row's value cells are **deterministic MD5 hashes** of `(timestamp, columnIndex)` (`HashFromTimestamp` → `GetMD5String`), not sampled/real sensor values — chosen so generated data is exactly reproducible (any node can regenerate/verify the same dataset from the same time range/column count without shipping the raw values around) and so correctness can be checked by re-deriving the expected hash rather than storing an oracle copy.
- Parquet schema: column 0 is always `timestamp` (`INT64`/`UINT_64`); columns 1..N are `BYTE_ARRAY`/`UTF8` (the hash strings) — i.e. the benchmark's payload is string-typed, not numeric, for all non-timestamp columns (relevant caveat: XStore's own numeric aggregate kernels, §6.3, would find zero valid numeric columns against this exact synthetic schema — the aggregate workloads necessarily target a different, numeric-typed dataset variant, or exercise the aggregate RPCs' *code path/latency* rather than meaningful reduction results).
- `ChunkSize = 1000` rows is documented as the "optimal" Parquet row-group size used when writing.
- A separate **`ProtoBench`** module/CLI subcommand exists purely to measure raw protobuf serialize/deserialize cost (`entries × byteSize`, N iterations) in isolation from any database or network I/O — isolating wire-format marshaling overhead as its own variable from the end-to-end DB benchmarks.

**Workload parameter space** (from XBench's `README.md`, "To generate workload datasets"): thread counts **{1, 2, 4, 8, 16, 32}** per client node, across **8 client nodes**, for each of `UNARY_SEQ`/`UNARY_RAND`/`BATCH_SEQ`/`BATCH_RAND` query and insert workloads plus `AGG_MIN`/`AGG_MAX`/`AGG_SUM`/`AGG_AVG`. The sample commands use `-st 946684800 -et 1262303999` — **2000-01-01T00:00:00Z to 2009-12-31T23:59:59Z, a 10-year span** — and `-c 128` columns for data generation, which is precisely the `10Y_128-Cols_Experiments` corpus naming (§10.4): 10 years × 128 columns. Per-node workload files are offset-sharded via `-o` (row offset into the generated dataset) so each of the 8 nodes/32 threads works a disjoint slice.

**Multi-node orchestration (`run.sh`):** a `tmux` + `ssh`-based controller script that declares a `targetNodes` array (bash), opens one tmux window per target node, and broadcasts setup/build/data-generation/load/read/write commands to all nodes in parallel (`sendCMD` → `tmux send-keys` to every node's window), then collects each node's result files back to the controller (`scpRecvBenchResults`, one `Node-<idx>/` subdirectory per node) — the same node-indexed layout (`*_Node-0` .. `*_Node-7`) visible in XStore's `10Y_128-Cols_Experiments/` artifact directories. The script also documents the target install profile (Apache Arrow APT repo + `libparquet-dev` + `cmake g++ protobuf-compiler libprotobuf-dev libzmq3-dev` + `pandas`/`fastparquet` for the Python-side tooling).

### 10.4 Large-scale empirical benchmark corpus — `10Y_128-Cols_Experiments/`

The artifact output of an XBench run (§10.3) against the XStore server, checked into the XStore repository itself. Covers **12 workload types**, each run across **8 nodes** (`*_Node-0` through `*_Node-7`), with multiple concurrent clients per node (client-sharded output files):

```
AGG_AVG_QUERY   AGG_MAX_QUERY   AGG_MIN_QUERY   AGG_SUM_QUERY
BATCH_INSERT_RAND   BATCH_INSERT_SEQ   BATCH_QUERY_RAND   BATCH_QUERY_SEQ
UNARY_INSERT_RAND   UNARY_INSERT_SEQ   UNARY_QUERY_RAND   UNARY_QUERY_SEQ
```

Naming convention observed in the artifacts (e.g. `BASIC-INSERT-BATCH-SEQ_Client-26_10-1000_900000-910000.parquet`): `<WORKLOAD-TAG>_Client-<clientID>_<param>-<param>_<rangeStart>-<rangeEnd>.parquet` — each file is one client's timed results for one contiguous timestamp sub-range of the 10-year (2000–2009), 128-column synthetic dataset described in §10.3, stored in **Parquet** for downstream analysis. This corpus is the empirical basis for whatever throughput/latency figures the associated paper reports at scale (as opposed to the single-machine, single-process numbers the in-repo `tests/perf` suite produces) — it exercises **SEQ vs. RAND** access patterns and **UNARY vs. BATCH** call granularity orthogonally across all four CRUD-adjacent operation families plus all four aggregate types, driven by XBench across a distributed multi-node client fleet against one XStore server.

`benchmark_artifact/` exists in the XStore repo but is currently empty (a designated output location, not yet populated in this checkout).

---

## 11. Constraints, Assumptions, and Known Limitations

These are worth stating explicitly for a paper's "limitations/threats to validity" discussion:

- **`BaseDir` = process CWD.** `GetBaseDir()` returns `std::filesystem::current_path()` — the server assumes it is launched with its working directory set to the directory it lives in, with `DataStore/` as a direct child. No configuration flag overrides this.
- **2 GB protobuf response ceiling — a real, separate limitation, not something the streaming encoder solves.** `BatchQuery` and `RangeQuery` both explicitly reject requests whose estimated response size (`rowCount × rowLength`) exceeds 2,000,000,000 bytes — protobuf's own hard message-size ceiling — and this check runs *before* any chunk is built. The streamed multipart reply mechanism (§7.4) only bounds server-side peak memory for results *under* that ceiling; it cannot lift the ceiling itself, since the client still reassembles every chunk into one buffer before a single `ParseFromArray` call. There is no pagination/cursor mechanism across multiple request/response round trips today. Confirmed intentional (documented in-code), not an oversight.
- **Fixed row length per DB, set at `CreateDB` time, minimum 4 bytes.** A row cannot be resized later; a row whose serialized form exceeds the configured `rowLength` is rejected (not truncated, not auto-widened). The `xstore-viewer` tool's sizing-guide diagnostic (§8.3) helps catch a poorly-chosen `rowLength` after the fact.
- **No authentication/authorization/encryption on the ZeroMQ transport.** `tcp://` sockets bind and accept any client that can reach them; the only request-side safety check found is `RemoveDBFiles`' rejection of filenames containing `/` (defends against path traversal, e.g. `"../other.CFG"`, escaping the DB directory) and refusal to delete the DB's own `.CFG` file via that path. ZeroMQ's CURVE mechanism could add real encryption/auth at the cost of key management/distribution — worth prioritizing only if XStore ever runs on a shared/untrusted network segment.
- **Durability is WAL-level for `UnaryInsert`, fsync-coalescer-level for `BatchInsert`/`RangeInsert`** (§4.12-4.13) — not OS-page-cache-level as an earlier revision of this document stated (that was accurate as of commit `8c6c4fc`, before the WAL existed). SYNC mode on either path gives a genuine fdatasync-before-reply guarantee. The remaining gap is narrow: ASYNC Batch/Range Insert has the same bounded crash-loss window as ASYNC UnaryInsert, but lacks the WAL's CRC-framed record and replay-on-restart for that window — tracked in `TODO.md`.
- **The WAL itself has no segmentation/rotation, no LSN tracking, and applies to the data file synchronously rather than via a decoupled background materializer.** All three are tracked as open work in `TODO.md`, along with cross-process write coordination (below) and C++23/messaging-layer/storage-compression exploration.
- **Column identity is purely positional**, not named — reordering how a client populates `RowData.fieldData` changes column meaning; there is no schema registry mapping names to indices beyond what the client application itself tracks.
- **`CreateDB`'s time-resolution whitelist (`SECOND`/`MILLISECOND`/`MICROSECOND`) is narrower than what the time-math layer (`TimeStruct`/the `TimeUnit` enum) actually supports** (`NANOSECOND`/`MINUTE`/`HOUR`/`DAY` are implemented but not creatable via the public API) — an intentional (or at least current) restriction of the exposed surface, not a limitation of the underlying arithmetic.
- **`RangeInsert` assumes pre-sorted-ascending input** (documented, unchecked) — unlike `BatchInsert`, which sorts unconditionally. Passing unsorted data to `RangeInsert` is a caller contract violation, not a runtime-detected error.
- **Single-writer-per-slot correctness relies entirely on the in-process `FileRangeLock` registry** — it provides no protection against a second XStore *process* (not thread) writing the same files concurrently; the design assumes exactly one server process owns a given `DataStoreDir`. `FileRangeLock`'s own header comment documents this as an in-process-only design choice, not an oversight. Tracked in `TODO.md`.
- **`WalkYearSpanParallel`'s per-request thread cap has no cross-request coordination** (§6.4) — confirmed real: the ZMQ worker-thread count bounds concurrent *requests*, not *threads spawned per request*, so the two caps multiply rather than nest. Untested at the concurrency level that would expose it. A fix already exists in-repo (`XStorePreadPool::ParallelFor`, used by `BatchQuery` but not yet by the aggregate path) — tracked in `TODO.md`.
- **Parallel `Sum`/`AvgAggregate` results can differ by a few ULP from the sequential (narrow-span) result for the same data** (§6.4) — an inherent floating-point reassociation effect of combining per-year partial sums in a different order than a strict single continuous accumulation, not a bug.
- **Compiler warnings are globally suppressed** (`add_definitions(-w)` in `CMakeLists.txt`) and the build defaults to `-O3` Release with no assertions layer beyond what GoogleTest/normal C++ provides; AddressSanitizer/UBSan flags exist in `CMakeLists.txt` but are commented out by default (opt-in for debugging only). Re-enabling `-Wall -Wextra` is tracked in `TODO.md`.
- **XBench's synthetic non-timestamp columns are string-typed (MD5 hashes)**, chosen for deterministic reproducibility/verification rather than to model a realistic numeric sensor-data distribution — a paper citing throughput numbers from this corpus should note the payload is a synthetic worst/typical-case for serialization size and wire cost, not a claim about numeric-aggregate performance under that exact dataset (see §10.3).
- **XStore is today strictly single-node.** The `hash % N` striping described throughout §4-§5 is in-process lock/cache/WAL-queue striping only, never network partitioning — there is no cluster/membership/replication code, no node/shard/replica identity at any layer. A full distributed architecture (sharding, replication, failover, a competitive analysis against 15 SOTA systems, and estimated operation costs) is designed but not yet implemented — see `docs/XStore_Distributed_Architecture.md` and the corresponding `TODO.md` entry.

See the repository's `TODO.md` for the full, current list of tracked open work (effort-categorized), including items beyond what's called out inline above — C++23 migration, an alternative-messaging-layer research spike, and storage-level compression.

---

## 12. Build & Dependency Surface

- **Language/standard:** C++17 (`CMAKE_CXX_STANDARD 17`), compiled `-O3` in Release (the default build type if unspecified).
- **Required external packages:** Abseil (`absl`, transitively via Protobuf), Google Protocol Buffers (≥3.18.1, `optimize_for = SPEED`, `--experimental_allow_proto3_optional`), ZeroMQ C++ bindings (cppzmq, ≥4.8.1 per README; headers vendored under `include/Networking/`), POSIX Threads.
- **Vendored (bundled in-tree, not fetched):** spdlog (+ bundled fmt) under `third_party/spdlog/` — the large majority of the graphify-derived node/edge count for this repository is this vendored logging/formatting library, not XStore's own logic; ZeroMQ C++ header bindings (`zmq.hpp`, `zmq_addon.hpp`, `zhelpers.hpp`) live under `third_party/` (server copy) and are duplicated under `client_src/Cpp/third_party/` (client copy, kept self-contained within `client_src/Cpp/` so the client library has no dependency outside its own directory and can be copied out standalone).
- **Fetched at configure time:** GoogleTest v1.17.0 via CMake `FetchContent` (only if `XSTORE_BUILD_TESTS`, default `ON`).
- **Build targets:** `XStore` (server executable), optional `client_src/Cpp` subdirectory (`XSTORE_BUILD_CLIENT`, default `ON`), optional `tests` subdirectory (`XSTORE_BUILD_TESTS`, default `ON`).
- **Large-file support** explicitly enabled (`_LARGEFILE_SOURCE`, `_LARGE_FILES`, `_FILE_OFFSET_BITS=64`) — expected given year-files can be large, fixed-width, and pre-`ftruncate`d to full-year capacity up front.
- **CI** (`.gitlab-ci.ymll`, GitLab CI over LXC-restored runners): stages are Initialize (LXC snapshot restore) → OS Configuration (apt update/upgrade, install `build-essential cmake g++ protobuf-compiler libzmq3-dev libzstd-dev`) → Build (server, then client) → Test → Cleanup.
- **XBench's own dependency surface** (separate build, §10.3): additionally requires Apache Arrow + `libparquet-dev` (via Apache's APT source) for its `DataManager`'s Parquet I/O, `argparse.hpp` (vendored header-only CLI parser), and driver-side database clients for whichever comparison systems are under test (MongoDB, InfluxDB, Graphite, and — unwired — TimescaleDB).

---

## 13. Reference: RPC Surface Summary

| RPC | Request → Response | Notes |
|---|---|---|
| `PING` | `ReqCMD` → `Ping_REP{}` | No `XStore` work; pure round-trip for handshake/liveness. |
| `INFO` | `ReqCMD` → `SysInfo_REP` | Full DataStore directory scan; per-DB file listing + allocated-byte accounting. |
| `CREATEDB` | `CreateDB_REQ{dbName,timeResolution,rowLength}` → `CreateDB_REP` | `rowLength ≥ 4`; resolution ∈ {SECOND,MILLISECOND,MICROSECOND}; idempotency: fails if DB dir already exists. |
| `REMOVEDB` | `RemoveDB_REQ{dbName}` → `RemoveDB_REP` | Recursive directory removal + cache eviction (config, mmap, fd) under that prefix. |
| `REMOVEDBFILES` | `RemoveDBFiles_REQ{dbName, [fileName]}` → `RemoveDBFiles_REP{[status]}` | Per-file status; rejects path-separator-containing names and the DB's own `.CFG`. |
| `UNARYINSERT` / `UNARYQUERY` / `UNARYDELETE` | single `(timestamp, [RowData])`; insert takes `durabilityMode` (ASYNC default/SYNC/BYPASS) | Point ops; insert routes through the WAL (§4.12), query via `pread`, delete via `fallocate` hole-punch. |
| `BATCHINSERT` / `BATCHQUERY` / `BATCHDELETE` | `repeated (timestamp[, RowData])`, arbitrary (non-contiguous) timestamp set; insert takes `durabilityMode` | Sorted (parallel above 10k elements), year-bucketed, whole-span-locked, run-coalesced writes; insert durability via the async data-fsync coalescer (§4.13), not the WAL. `BatchQuery` omits holes from the response entirely, replies streamed (§7.4). |
| `RANGEINSERT` / `RANGEQUERY` / `RANGEDELETE` | contiguous `[startTime, endTime]` (insert takes explicit per-row data, assumed pre-sorted); insert takes `durabilityMode` | `RangeQuery` returns **every** timestamp in range including holes (as empty-field entries) — the opposite convention from `BatchQuery`; replies streamed (§7.4). 2 GB total-result ceiling enforced (§11), independent of streaming. Insert durability via the async data-fsync coalescer (§4.13). |
| `MIN` / `MAX` / `SUM` / `AVG` | `(startTime, endTime, dbName, columnIdx)` → single `FData`/`double` + status | Single-column reduction over `[start,end]`, walked year-file-by-year-file via `WalkYearSpan`/`WalkYearSpanParallel` for wide spans (§6.4); non-numeric/holed rows excluded from the reduction, not treated as zero. |

---

*End of technical reference. Compiled from direct inspection of the XStore source tree (`src/`, `include/`, `proto/`, `client_src/`, `tests/`, `tools/xstore-viewer/`), the sibling XBench benchmark-harness source tree (`/home/ubuntu/XBench`), the XStore project's git history, and its benchmark artifact layout. Originally compiled as of commit `8c6c4fc`; revised 2026-09-06 to cover the WAL/durability-mode implementation, the async data-fsync coalescer, streamed Batch/RangeQuery replies, `SuggestRowLength`, and `xstore-viewer` — see `TODO.md` for currently open work.*
