# Zone-Map Secondary Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give XStore a per-block min/max/sum/count secondary index ("zone map") for every numeric column, so a predicate or aggregate over a non-timestamp column can prune whole blocks of rows instead of scanning every row's slot in the queried range — with zero wire-format change and zero risk of an incorrect (false-negative) query result if the server crashes mid-write.

**Architecture:** A new per-(db, year-file) sidecar file (`<YEAR>.zmap`) stores one fixed-size `ZoneMapEntry` (min/max/sum/valid_count/null_count) per (block, tracked-column) pair, where a block is a contiguous run of `rows_per_block` timestamp slots (derived from a target byte size, not a fixed row count — XStore's fixed-width `rowLength` varies too much across DBs for one constant to make sense). Every insert path (`UnaryInsert`/`BatchInsert`/`RangeInsert`) updates the affected block **write-through**: an in-process cache holds a block's entries only to avoid a repeat `pread` on consecutive updates to the same block (the common sequential-append case), but every single update is `pwrite`-ed back to disk before the insert call returns — there is no buffered/"dirty" state that can be lost. This is a deliberate correctness choice, not the first design tried (see the rejected write-back alternative in Task 2): a write-back cache that only flushes on eviction leaves a window where a block is flushed once, accumulates more updates in memory, and is lost on a crash — a later insert into that same block then reloads the stale on-disk copy, keeps accumulating on top of it, and eventually re-flushes it looking complete, silently and permanently under-representing that block with no way to detect it later. Write-through closes that window entirely: any block's on-disk stats are always exactly consistent with every insert that has returned, so the index needs no WAL involvement and no "trust only if flushed" bookkeeping to stay crash-safe. The real cost this buys correctness with — one extra small `pwrite` per row-affecting insert call — is measured in Task 8, not assumed away. Query-side, `MinAggregate`/`MaxAggregate` consult the zone map to skip whole blocks that can't contain a better answer; `SumAggregate`/`AvgAggregate` add a fully-covered block's precomputed sum/count directly, with zero row scan for that block.

**Tech Stack:** C++17, GoogleTest, raw `pread`/`pwrite` (no protobuf — this is a local-only sidecar file, never sent over the wire).

**Spec:** `TODO.md` Medium Effort section, item "Zone-map secondary index for non-timestamp columns" (originally scoped in `~/.claude/plans/speaking-from-database-design-floating-pelican.md`, Gap 3) — refined by this plan, which corrects two gaps in that original brainstorm (see Global Constraints below) discovered while grounding the design against the real codebase.

## Global Constraints

- **No wire/proto change.** The zone map is a server-local file; no `.proto` message changes, no client-library changes, in this plan.
- **Never a false negative.** Every zone-map update is write-through (`pwrite`-ed to disk before the triggering insert call returns, see Architecture above) — there is no buffered state a crash can lose, so a block's on-disk stats are always exactly consistent with every row actually inserted. This is what makes the index crash-safe without adding it to the WAL. A false *positive* (failing to prune a block that could have been skipped, e.g. one never written to) is always acceptable; a false negative (skipping a block that contains a real match) is never acceptable and this design has no path to one.
- **XStore has no declared column schema** — `CreateDB` takes only `rowLength` (bytes), not a column count (confirmed: `proto/XStore.proto:118-123`, `CreateDB_REQ` has no column-count field). Per-row column count is heterogeneous and holes are fully supported (`TODO.md`, "Verified — no action needed" section). This plan tracks at most `XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS` (default 64) columns per DB — column indices at or past that cap get no zone-map acceleration and always fall back to a full scan for that column, which is always correct, just not accelerated. (The original brainstorm's "~16 bytes/column/block, single-digit MB" estimate assumed a small declared column count; deriving a column count from `rowLength` instead — e.g. `rowLength / 2` — was checked and rejected: it inflates zone-map size past "single-digit MB" for wide-row DBs, e.g. ~708MB for a 1536-byte-row, SECOND-resolution year at that derivation. A fixed small cap avoids this blowup and keeps the near-zero-memory promise honest.)
- **Zone-map entries are `double`, not `long double`.** `ValueStruct::value` (`include/DataStructs/ValueStruct.h`) is `long double` for the in-kernel comparison type, but `long double` on this platform is 16 bytes (80-bit extended + padding) and not a portable on-disk width. Persist as `double` (8 bytes) — matches the precision `SumAggregate`/`ProcessFileRangeAccumulate` already commit to (`double& sum_out`, `XStore.h:406`), so this loses no precision versus what a query already returns today.
- Must build clean under `-Wall -Wextra -Wpedantic -Werror` (project already enables `-Wall -Wextra`; treat warnings as errors per `CPP.md`).
- `pread`/`pwrite` return values must be captured into a checked (or `[[maybe_unused]]`) variable, never a bare `(void)`-cast — glibc's `_FORTIFY_SOURCE` doesn't suppress the warning that way (established pattern, see `21d4aa9c`).
- Every existing test in `tests/integration/XStoreTest.cpp` and `tests/integration/XStoreWalTest.cpp` must still pass after every task (`./build/tests/xstore_tests`).
- Out of scope for this plan (each is its own future plan, deliberately not folded in here — see "Deferred" section at the end): approximate sketches (variance, percentile, distinct-count) and the new aggregate RPC ops they'd require; a roaring-bitmap/bloom layer for categorical/string columns; full-text/pattern search.

---

## File Structure

- `include/XStore/XStore_Tunables.h` — add `ZONE_MAP_TARGET_BLOCK_BYTES`, `ZONE_MAP_MAX_TRACKED_COLUMNS`, `ZONE_MAP_HOT_CACHE_BLOCKS`, `ZONE_MAP_REGISTRY_CAP` (Task 1).
- `include/XStore/XStore_ZoneMap.h` — new. `ZoneMapEntry`, `ZoneMapFileHeader`, `RowsPerBlock()`, `BlockIndexForRow()`, `ZoneMapFile` class declaration (Tasks 1-3).
- `src/XStore/XStore_ZoneMap.cpp` — new. `ZoneMapFile` implementation: file creation/open, write-through `UpdateRow`, read-cached `TryGetBlockStats` (Tasks 2-3).
- `include/XStore/XStore.h` — declare `ZoneMapFile& GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, unsigned short row_length)` private helper and its cache (Task 3).
- `src/XStore/XStore_UnaryOps.cpp` — hook the zone-map update into `UnaryInsert` right after a successful row write (Task 4).
- `src/XStore/XStore_BatchOps.cpp`, `src/XStore/XStore_RangeOps.cpp` — same hook, once per row in the year-bucket loop (Task 5).
- `src/XStore/XStore_Aggregates.cpp` — `MinAggregate`/`MaxAggregate` block-skip pruning (Task 6); `SumAggregate`/`AvgAggregate` fully-covered-block pushdown (Task 7).
- `tests/integration/XStoreZoneMapTest.cpp` — new test file (Tasks 1-7).
- `tests/CMakeLists.txt` — register the new test file (Task 1).
- `docs/XStore_Technical_Reference.md` — new subsection documenting the zone map (Task 8).
- `TODO.md` — mark the item done with findings (Task 8).

---

## Task 1: Tunables, pure block-index math, and file header layout

**Status: DONE — committed `3c29062c`.**

**Files:**
- Modify: `include/XStore/XStore_Tunables.h` (add constants, near the other per-feature tunable blocks)
- Create: `include/XStore/XStore_ZoneMap.h`
- Create: `tests/integration/XStoreZoneMapTest.cpp`
- Modify: `tests/CMakeLists.txt:7-10` (add `integration/XStoreZoneMapTest.cpp` next to `XStoreWalTest.cpp`)

**Interfaces:**
- Consumes: nothing (pure math + POD structs, no I/O yet).
- Produces: `RowsPerBlock(uint16_t row_length) -> uint64_t`, `BlockIndexForRow(uint64_t row_index, uint64_t rows_per_block) -> uint64_t`, `struct ZoneMapEntry`, `struct ZoneMapFileHeader` — all consumed by Task 2.

- [x] **Step 1: Write the failing test for `RowsPerBlock`**

```cpp
// tests/integration/XStoreZoneMapTest.cpp
#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_ZoneMap.h"
#include "XStoreTestSupport.h"
#include <filesystem>
#include <type_traits>

// Same anonymous-namespace + using-declaration convention as
// XStoreWalTest.cpp -- keeps this file's helpers/fixture from colliding
// with any same-named symbol in another test translation unit.
namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

TEST(ZoneMapMathTest, RowsPerBlockDerivesFromTargetByteSizeNotFixedRowCount) {
    // Narrow row (8 bytes): target/row_length should give many rows/block.
    EXPECT_EQ(RowsPerBlock(8), XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES / 8);
    // Wide row (larger than the target itself): must still return >= 1, never 0.
    uint16_t huge_row = static_cast<uint16_t>(XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES * 4 > 65535
                                                   ? 65535
                                                   : XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES * 4);
    EXPECT_GE(RowsPerBlock(huge_row), 1u);
}

TEST(ZoneMapMathTest, BlockIndexForRowIsIntegerDivision) {
    EXPECT_EQ(BlockIndexForRow(0, 100), 0u);
    EXPECT_EQ(BlockIndexForRow(99, 100), 0u);
    EXPECT_EQ(BlockIndexForRow(100, 100), 1u);
    EXPECT_EQ(BlockIndexForRow(250, 100), 2u);
}

TEST(ZoneMapEntryLayoutTest, EntryIsFixedWidthPodForRawFileIo) {
    static_assert(std::is_trivially_copyable<ZoneMapEntry>::value,
                  "ZoneMapEntry must be trivially copyable for raw pread/pwrite");
    EXPECT_EQ(sizeof(ZoneMapEntry), 40u);
}

} // namespace
```

Every later task in this plan (2-7) that adds tests to this same file opens and closes its own `namespace { ... }` block at the end of the file — legal C++ (a translation unit may reopen an anonymous namespace any number of times) and it keeps every task's own build-and-test step self-contained instead of depending on a namespace some later task closes.

- [x] **Step 2: Register the test file and confirm it fails to build**

Add `integration/XStoreZoneMapTest.cpp` to `tests/CMakeLists.txt` right after line 9 (`integration/XStoreWalTest.cpp`), in the same `add_executable(xstore_tests ...)` list.

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `XStore_ZoneMap.h` does not exist yet.

- [x] **Step 3: Write the tunables**

```cpp
// include/XStore/XStore_Tunables.h -- add inside namespace XStoreTunables, after WAL block

// Zone-map secondary index (see docs/superpowers/plans/2026-09-14-zone-map-
// secondary-index.md). Target BYTE size per block, not a fixed row count --
// XStore's fixed-width rowLength varies 10-100x across differently-shaped
// DBs, so a constant row count (e.g. ClickHouse's index_granularity=8192)
// would produce wildly different block byte sizes per DB. 2MB lands near a
// single efficient NVMe read chunk; re-sweep on Mystic before trusting this
// default in production (see Task 8's benchmark gate).
inline constexpr uint64_t ZONE_MAP_TARGET_BLOCK_BYTES = 2ull * 1024 * 1024;

// XStore has no declared column schema (CreateDB takes only rowLength, a
// byte budget -- see proto/XStore.proto's CreateDB_REQ). Tracking "one zone-
// map entry per column that could possibly fit in rowLength" was checked
// and rejected: it inflates zone-map file size past the feature's own
// near-zero-memory premise for wide-row DBs (a 1536-byte row would derive
// ~768 phantom tracked columns). This cap bounds tracked columns to a small,
// realistic count; column indices >= this cap always fall back to a full
// scan for that column -- correct, just not accelerated.
inline constexpr uint32_t ZONE_MAP_MAX_TRACKED_COLUMNS = 64;

// How many blocks' stats a ZoneMapFile keeps resident at once, per (db,
// year) file, purely to avoid a repeat pread() when consecutive updates hit
// the same block (the common sequential-append case) -- every entry here is
// always disk-coherent (write-through, see ZoneMapFile::UpdateRow), so this
// is a pure performance cache, not a durability buffer. 4 blocks * 64
// tracked columns * 40 bytes/entry (ZoneMapEntry) = 10,240 bytes resident
// per (db, year) file with this default -- negligible on its own; see
// ZONE_MAP_REGISTRY_CAP below for the bound on how many (db, year) files can
// be resident at once, which is what actually keeps total memory bounded.
inline constexpr size_t ZONE_MAP_HOT_CACHE_BLOCKS = 4;

// How many ZoneMapFile instances (one per (db, year) pair ever touched) the
// registry keeps resident at once (see XStore::GetOrOpenZoneMap). Same
// discipline as MMAP_CACHE_CAP/FD_CACHE_CAP above: every other per-key cache
// in this codebase is capped, and a zone-map registry with no cap would grow
// for the lifetime of a long-running server across every distinct (db,
// year) combination it ever sees -- slow, but genuinely unbounded, which
// directly conflicts with XStore's minimal-memory-footprint goal. Eviction
// here is trivially safe and cheap (unlike the mmap/fd caches): every
// ZoneMapFile update is already write-through, so dropping an instance
// loses nothing -- the next touch just reopens the file and pays one fresh
// pread() for whatever block it needs. 256 instances * ~10KB each (default
// ZONE_MAP_HOT_CACHE_BLOCKS) = ~2.5MB worst-case resident for this cache --
// re-sweep alongside the Task 8 benchmark gate before trusting this default,
// same rule as every other capacity constant in this file.
inline constexpr size_t ZONE_MAP_REGISTRY_CAP = 256;
```

- [x] **Step 4: Write `XStore_ZoneMap.h`**

```cpp
#pragma once
#include <cstdint>
#include <type_traits>
#include "include/XStore/XStore_Tunables.h"

// Zone-map secondary index: per-block min/max/sum/count summary statistics
// for numeric columns, used to prune blocks a range/aggregate query cannot
// possibly need. See docs/superpowers/plans/2026-09-14-zone-map-secondary-
// index.md for the full design and its rejected alternatives.

inline uint64_t RowsPerBlock(uint16_t row_length) {
    if (row_length == 0) return 1;
    uint64_t rows = XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES / row_length;
    return rows < 1 ? 1 : rows;
}

inline uint64_t BlockIndexForRow(uint64_t row_index, uint64_t rows_per_block) {
    return row_index / rows_per_block;
}

// One (block, column) summary. min_value/max_value are only meaningful when
// valid_count > 0 -- a never-observed column in a block has valid_count==0
// and min_value/max_value left at their sentinel (see ZoneMapFile::
// ZeroedEntry). Persisted as raw bytes (pread/pwrite), never protobuf --
// this file never crosses the wire.
struct ZoneMapEntry {
    double   min_value;
    double   max_value;
    double   sum_value;
    uint64_t valid_count;   // rows in this block where the column was numeric and set
    uint64_t null_count;    // rows in this block where the column was a hole or non-numeric
};
static_assert(std::is_trivially_copyable<ZoneMapEntry>::value,
              "ZoneMapEntry must be trivially copyable for raw pread/pwrite");
static_assert(sizeof(ZoneMapEntry) == 40, "update size-dependent capacity-planning docs if this changes");

struct ZoneMapFileHeader {
    uint32_t magic   = 0x5A4D4150; // 'ZMAP'
    uint32_t version = 1;
    uint32_t tracked_column_count = 0;
    uint64_t rows_per_block = 0;
};
```

- [x] **Step 5: Run tests, confirm pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=ZoneMap*`
Expected: 3/3 PASS.

- [x] **Step 6: Commit**

```bash
git add include/XStore/XStore_Tunables.h include/XStore/XStore_ZoneMap.h tests/integration/XStoreZoneMapTest.cpp tests/CMakeLists.txt
git commit -m "feat(zonemap): add block-size math and on-disk entry layout"
```

---

## Task 2: `ZoneMapFile` — sidecar file creation, write-through updates, block reads

**Status: DONE — committed `e098a0bc`, plus unplanned follow-up fix `dd3c4f21` (code review found `Open`/`UpdateRow` weren't checking `pwrite` return values — fixed, re-reviewed, all 140 tests pass).**

**Files:**
- Modify: `include/XStore/XStore_ZoneMap.h` (add `ZoneMapFile` class declaration)
- Create: `src/XStore/XStore_ZoneMap.cpp`
- Modify: `CMakeLists.txt` — add `src/XStore/XStore_ZoneMap.cpp` to the main library's source list (same list `XStore_Wal.cpp` is in)
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `RowsPerBlock`, `BlockIndexForRow`, `ZoneMapEntry`, `ZoneMapFileHeader` (Task 1).
- Produces: `ZoneMapFile` with `Open(path, row_length)`, `UpdateRow(row_index, column values)`, `TryGetBlockStats(block_index) -> std::optional<std::vector<ZoneMapEntry>>` — consumed by Tasks 3, 6, 7.

**Design note — write-through, not write-back (rejected alternative):** the first version of this task cached a block's entries in memory and only flushed to disk on LRU eviction, treating any still-cached ("dirty") block as untrustworthy for pruning. That has a real crash-consistency hole: block B gets flushed once (baseline on disk), more rows land in B and accumulate only in memory, the process is killed before B is evicted/flushed again — the accumulated updates are gone, but B is NOT untouched, it has a stale-but-present on-disk baseline. The next insert into B reloads that stale baseline via `EnsureBlockLoaded`, keeps accumulating on top of it, and eventually flushes it again looking complete — silently and permanently under-representing B (missing whatever rows were lost), with nothing left to signal "this one's suspect." That is exactly the false-negative-pruning risk this whole feature must never produce. Fixing it by routing zone-map writes through the WAL would work but couples this index's correctness to WAL/LSN machinery it doesn't otherwise need. **Instead: every `UpdateRow` call `pwrite`s its block's entries back to disk before returning, always.** The in-memory cache below exists purely to avoid a repeat `pread` when consecutive calls touch the same block (the common sequential-append case) — it holds no state a crash can lose, because nothing is ever buffered past the call that produced it. This trades one extra small `pwrite` per row-affecting insert call for eliminating the failure mode entirely; Task 8 benchmarks that cost rather than assuming it's negligible.

- [x] **Step 1: Write the failing test — sequential updates are visible immediately (no explicit flush needed), and survive a reopen**

```cpp
// ScratchDir has no default constructor and no Path() helper (see
// XStoreTestSupport.h) -- it takes a prefix, chdir()s into a fresh temp
// directory for the test's lifetime, and exposes that directory as
// `test_root`. Build file paths as `scratch.test_root / "name"`, the same
// pattern XStoreWalTest.cpp already uses.
namespace {

using xstore_test::ScratchDir;

TEST(ZoneMapFileTest, SequentialUpdatesToSameBlockAreVisibleImmediately) {
    ScratchDir scratch("zone_map_file_test");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), /*row_length=*/64));

    // Two rows landing in the same block, column 0 values 5.0 and -3.0.
    zmap.UpdateRow(0, {{0, 5.0, true}});
    zmap.UpdateRow(1, {{0, -3.0, true}});

    // No explicit flush call -- write-through means this is already durable.
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, -3.0);
    EXPECT_DOUBLE_EQ((*stats)[0].max_value, 5.0);
    EXPECT_DOUBLE_EQ((*stats)[0].sum_value, 2.0);
    EXPECT_EQ((*stats)[0].valid_count, 2u);
}

TEST(ZoneMapFileTest, StatsSurviveReopenFromDiskWithNoCachedState) {
    ScratchDir scratch("zone_map_file_test_reopen");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    {
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(path.string(), 64));
        zmap.UpdateRow(0, {{0, 10.0, true}});
        // Destructor runs here -- nothing to flush, already on disk.
    }
    ZoneMapFile reopened;
    ASSERT_TRUE(reopened.Open(path.string(), 64));
    auto stats = reopened.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 10.0);
}

TEST(ZoneMapFileTest, UntouchedBlockReportsZeroValidCountNotMissingData) {
    ScratchDir scratch("zone_map_file_test_untouched");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), 64));
    zmap.UpdateRow(0, {{0, 1.0, true}}); // touches block 0 only
    auto stats = zmap.TryGetBlockStats(5); // block 5 never touched
    ASSERT_TRUE(stats.has_value()); // a real, safe answer: "zero rows here," not "unknown"
    EXPECT_EQ((*stats)[0].valid_count, 0u);
}

TEST(ZoneMapFileTest, ColumnIndexAtOrPastCapIsSilentlyIgnored) {
    ScratchDir scratch("zone_map_file_test_column_cap");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), 64));
    // Must not crash/UB -- just has no effect on tracked columns.
    zmap.UpdateRow(0, {{XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS, 1.0, true}});
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->size(), XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);
}

} // namespace
```

Add `#include <filesystem>` to the top of `XStoreZoneMapTest.cpp` (Task 1's include list) alongside the others -- needed here for `std::filesystem::path`.

- [x] **Step 2: Run, confirm failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -30`
Expected: FAIL — `ZoneMapFile` incomplete type.

- [x] **Step 3: Declare `ZoneMapFile`**

```cpp
// append to include/XStore/XStore_ZoneMap.h
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// One numeric observation to fold into a block's running stats for one
// column. `is_numeric=false` (a hole, or a non-numeric FData branch such as
// stringData/bytesData) increments null_count instead of touching min/max/
// sum -- mirrors AggregateSkipsNonNumericColumnValues' existing semantics
// (XStoreTest.cpp) so a zone map never disagrees with what a real scan
// would have concluded.
struct ZoneMapObservation {
    uint32_t column_idx;
    double   value;
    bool     is_numeric;
};

class ZoneMapFile {
public:
    // Creates the file (header only) if it doesn't exist; opens it
    // otherwise. row_length is used only to derive rows_per_block on first
    // creation -- ignored on reopen (rows_per_block is read from the header,
    // so it can never drift from what earlier blocks were written against).
    bool Open(const std::string &path, uint16_t row_length);

    // Folds each observation into the block containing row_index and
    // pwrite()s that block back to disk before returning -- write-through,
    // no buffered state a crash can lose (see the Task 2 design note in the
    // plan for why this isn't a write-back cache). The in-memory cache below
    // only saves a repeat pread() when consecutive calls hit the same block.
    void UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations);

    // Always returns real, safe stats for block_index -- a never-touched
    // block simply has valid_count==0 everywhere, which is the true and
    // correct answer for "no rows landed here yet," not a placeholder for
    // "unknown." nullopt is reserved for an actual I/O failure on the read.
    std::optional<std::vector<ZoneMapEntry>> TryGetBlockStats(uint64_t block_index);

    ~ZoneMapFile();

    uint64_t RowsPerBlockValue() const { return header_.rows_per_block; }

private:
    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BlockFileOffset(uint64_t block_index) const;

    int fd_ = -1;
    ZoneMapFileHeader header_;
    std::unordered_map<uint64_t, std::vector<ZoneMapEntry>> cache_; // pure read-cache, always disk-coherent
    std::vector<uint64_t> lru_order_; // most-recently-touched at the back
};
```

- [x] **Step 4: Implement `XStore_ZoneMap.cpp`**

```cpp
#include "include/XStore/XStore_ZoneMap.h"
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

bool ZoneMapFile::Open(const std::string &path, uint16_t row_length) {
    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) return false;

    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= static_cast<off_t>(sizeof(ZoneMapFileHeader))) {
        ssize_t read_bytes = pread(fd_, &header_, sizeof(header_), 0);
        if (read_bytes != static_cast<ssize_t>(sizeof(header_)) || header_.magic != ZoneMapFileHeader{}.magic) {
            return false; // corrupt/foreign file -- caller falls back to no acceleration
        }
    } else {
        header_ = ZoneMapFileHeader{};
        header_.tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS;
        header_.rows_per_block = RowsPerBlock(row_length);
        [[maybe_unused]] ssize_t written = pwrite(fd_, &header_, sizeof(header_), 0);
    }
    return true;
}

off_t ZoneMapFile::BlockFileOffset(uint64_t block_index) const {
    return static_cast<off_t>(sizeof(ZoneMapFileHeader)) +
           static_cast<off_t>(block_index) * header_.tracked_column_count * sizeof(ZoneMapEntry);
}

void ZoneMapFile::EnsureBlockLoaded(uint64_t block_index) {
    if (cache_.count(block_index)) return;

    std::vector<ZoneMapEntry> entries(header_.tracked_column_count, ZoneMapEntry{0, 0, 0, 0, 0});

    off_t offset = BlockFileOffset(block_index);
    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= offset + static_cast<off_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) {
        // Existing on-disk block -- load its current (always-durable) stats.
        [[maybe_unused]] ssize_t read_bytes =
            pread(fd_, entries.data(), header_.tracked_column_count * sizeof(ZoneMapEntry), offset);
    }
    // Otherwise: never-touched block, zero-initialized entries above are correct.

    cache_.emplace(block_index, std::move(entries));
    lru_order_.push_back(block_index);
    EvictIfOverCapacity();
}

void ZoneMapFile::EvictIfOverCapacity() {
    // Pure memory-bound eviction -- nothing to flush, every cached copy is
    // already disk-coherent by the time UpdateRow returns.
    while (cache_.size() > XStoreTunables::ZONE_MAP_HOT_CACHE_BLOCKS) {
        uint64_t victim = lru_order_.front();
        lru_order_.erase(lru_order_.begin());
        cache_.erase(victim);
    }
}

void ZoneMapFile::UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations) {
    uint64_t block_index = BlockIndexForRow(row_index, header_.rows_per_block);
    EnsureBlockLoaded(block_index);

    lru_order_.erase(std::remove(lru_order_.begin(), lru_order_.end(), block_index), lru_order_.end());
    lru_order_.push_back(block_index);

    std::vector<ZoneMapEntry> &entries = cache_.at(block_index);
    for (const auto &obs : observations) {
        if (obs.column_idx >= header_.tracked_column_count) continue; // beyond cap -- not tracked, always safe
        ZoneMapEntry &e = entries[obs.column_idx];
        if (obs.is_numeric) {
            e.min_value = (e.valid_count == 0) ? obs.value : std::min(e.min_value, obs.value);
            e.max_value = (e.valid_count == 0) ? obs.value : std::max(e.max_value, obs.value);
            e.sum_value += obs.value;
            e.valid_count++;
        } else {
            e.null_count++;
        }
    }

    // Write-through: durable before this call returns, no dirty state left
    // behind for a crash to lose. See the Task 2 design note for why.
    off_t offset = BlockFileOffset(block_index);
    [[maybe_unused]] ssize_t written =
        pwrite(fd_, entries.data(), entries.size() * sizeof(ZoneMapEntry), offset);
}

std::optional<std::vector<ZoneMapEntry>> ZoneMapFile::TryGetBlockStats(uint64_t block_index) {
    auto cached = cache_.find(block_index);
    if (cached != cache_.end()) return cached->second; // always disk-coherent, safe to serve directly

    off_t offset = BlockFileOffset(block_index);
    struct stat st{};
    if (fstat(fd_, &st) != 0) return std::nullopt; // real I/O failure
    if (st.st_size < offset + static_cast<off_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) {
        // Never-touched block -- a real, safe zero'd answer, not an error.
        return std::vector<ZoneMapEntry>(header_.tracked_column_count, ZoneMapEntry{0, 0, 0, 0, 0});
    }
    std::vector<ZoneMapEntry> entries(header_.tracked_column_count);
    ssize_t read_bytes = pread(fd_, entries.data(), header_.tracked_column_count * sizeof(ZoneMapEntry), offset);
    if (read_bytes != static_cast<ssize_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) return std::nullopt;
    return entries;
}

ZoneMapFile::~ZoneMapFile() {
    if (fd_ >= 0) close(fd_);
}
```

- [x] **Step 5: Run tests, confirm pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=ZoneMapFileTest*`
Expected: 4/4 PASS.

- [x] **Step 6: Commit**

```bash
git add include/XStore/XStore_ZoneMap.h src/XStore/XStore_ZoneMap.cpp CMakeLists.txt tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): add ZoneMapFile with crash-safe write-through updates"
```

---

## Task 3: Per-DB `ZoneMapFile` registry, wired into `CreateDB`

**Status: DONE — committed `872dcba4`. OPEN FINDING from controller-side review: `ZoneMapFile::Open()` in `include/XStore/XStore_ZoneMap.h:67` still lacks `[[nodiscard]]` (its return value can be silently dropped) — not yet fixed, next up.**

**Files:**
- Modify: `include/XStore/XStore.h` — declare the registry and `GetOrOpenZoneMap`
- Modify: `src/XStore/XStore_Management.cpp` (wherever `CreateDB` and `InitDB` live — mirrors the existing `CfgMap`/path-cache sharded pattern, `docs/XStore_Technical_Reference.md` §4.4)
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `ZoneMapFile` (Task 2).
- Produces: `XStore::GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length) -> ZoneMapFile&` — consumed by Tasks 4-7.

- [x] **Step 1: Define the shared fixture and write the failing test — two calls for the same (db, year) return the same instance**

`XStore::DbBasePath` is private (`db_name` must already be upper-cased, confirmed at `include/XStore/XStore.h:458`, inside the `private:` section that starts at line 423) — this fixture does not need friend access to it. `ScratchDir`'s constructor already `chdir()`s into a fresh `test_root` and creates `test_root/DataStore`, and `XStore`'s default `data_store_dir` is `"DataStore"` relative to cwd — the exact same assumption `XStoreWalTest.cpp` already relies on for its own `db_base_path` construction (e.g. `scratch->test_root / "DataStore" / "WALCRASH"`). Every test in this file uses an already-upper-case DB name, so no case-folding helper is needed either.

```cpp
namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

// Shared fixture for every zone-map test in this file (Tasks 3-7 all use
// TEST_F(XStoreZoneMapTest, ...)). Same ScratchDir-per-test pattern as
// XStoreWalTest.cpp -- a fresh temp directory per test avoids collisions
// between tests that reuse gtest's heap-allocated fixture address.
class XStoreZoneMapTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_zone_map_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
    }

    void TearDown() override {
        scratch.reset();
    }

    // db_name must already be upper-case.
    std::string DbPath(std::string_view db_name) const {
        return (scratch->test_root / "DataStore" / std::string(db_name)).string();
    }
};

TEST_F(XStoreZoneMapTest, GetOrOpenZoneMapReturnsSameInstanceForSameDbAndYear) {
    XStore store;
    CreateTestDb(store, "ZMAPREG", XStore_Proto::SECOND, 64);
    ZoneMapFile &a = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2024, 64);
    ZoneMapFile &b = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2024, 64);
    EXPECT_EQ(&a, &b);

    ZoneMapFile &different_year = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2025, 64);
    EXPECT_NE(&a, &different_year);
}

} // namespace
```

- [x] **Step 2: Run, confirm failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `GetOrOpenZoneMap` not a member of `XStore`.

- [x] **Step 3a: Write the failing test — the registry never grows past `ZONE_MAP_REGISTRY_CAP`**

```cpp
namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, GetOrOpenZoneMapEvictsOldestPastRegistryCap) {
    XStore store;
    CreateTestDb(store, "ZMAPCAP", XStore_Proto::SECOND, 64);

    // Touch more distinct (db, year) combinations than the cap allows.
    for (unsigned short year = 2000; year < 2000 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 10; ++year) {
        store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), year, 64);
    }
    EXPECT_LE(store.ZoneMapRegistrySize(), XStoreTunables::ZONE_MAP_REGISTRY_CAP);

    // Evicting one instance must not lose durably-written data -- write-
    // through (Task 2) means eviction is just closing a file handle.
    store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), 2000, 64)
        .UpdateRow(0, {{0, 5.0, true}});
    for (unsigned short year = 2001; year < 2000 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
        store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), year, 64); // evicts year 2000 out
    }
    auto stats = store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), 2000, 64).TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 5.0); // survived eviction + reopen
}

} // namespace
```

(`DbPath` is the `XStoreZoneMapTest` fixture helper from Step 1, already committed to the repo by the time this step runs — it is a `protected` fixture member, reachable from any `TEST_F(XStoreZoneMapTest, ...)` in this file.)

- [x] **Step 3b: Run, confirm failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `ZoneMapRegistrySize` not a member of `XStore`.

- [x] **Step 3c: Declare and implement a capped, LRU-evicting registry**

```cpp
// include/XStore/XStore.h -- private section, near the other per-DB caches
#include "include/XStore/XStore_ZoneMap.h"

// Keyed by "<db_base_path>/<year>" -- one ZoneMapFile per (db, year) pair
// ever touched. Capped at ZONE_MAP_REGISTRY_CAP (see its comment in
// XStore_Tunables.h for the memory-footprint reasoning) with plain LRU
// eviction -- unlike the mmap/fd caches this mirrors, eviction here is
// always safe and cheap: every ZoneMapFile update is write-through (Task
// 2), so an evicted instance has lost nothing, and the next touch just
// reopens its .zmap file and pays one fresh pread() for whatever block it
// needs. A plain unordered_map behind one mutex, not sharded -- CreateDB/
// first-insert-of-a-year frequency is nowhere near hot-path enough to need
// striping the way the WAL/mmap caches do.
std::mutex zone_map_registry_mutex_;
std::unordered_map<std::string, std::unique_ptr<ZoneMapFile>> zone_map_registry_;
std::vector<std::string> zone_map_registry_lru_; // most-recently-touched at the back

ZoneMapFile& GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length);

// Test-only introspection -- not part of the public API surface any real
// caller needs, same as this codebase's other test-only accessors.
size_t ZoneMapRegistrySize() const { return zone_map_registry_.size(); }
```

```cpp
// src/XStore/XStore_Management.cpp (or wherever CachedYearFilePath lives --
// mirror its file-path construction exactly so the .zmap sidecar always
// sits next to its .XSTORE file)
ZoneMapFile& XStore::GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length) {
    std::string key = std::string(db_base_path) + "/" + std::to_string(year);
    std::lock_guard<std::mutex> lock(zone_map_registry_mutex_);

    auto it = zone_map_registry_.find(key);
    if (it != zone_map_registry_.end()) {
        zone_map_registry_lru_.erase(
            std::remove(zone_map_registry_lru_.begin(), zone_map_registry_lru_.end(), key),
            zone_map_registry_lru_.end());
        zone_map_registry_lru_.push_back(key);
        return *it->second;
    }

    if (zone_map_registry_.size() >= XStoreTunables::ZONE_MAP_REGISTRY_CAP && !zone_map_registry_lru_.empty()) {
        std::string victim = zone_map_registry_lru_.front();
        zone_map_registry_lru_.erase(zone_map_registry_lru_.begin());
        zone_map_registry_.erase(victim); // ~ZoneMapFile() closes the fd -- no flush needed, already durable
    }

    auto zmap = std::make_unique<ZoneMapFile>();
    std::string path = std::string(db_base_path) + "/" + std::to_string(year) + ".zmap";
    zmap->Open(path, row_length);
    ZoneMapFile &ref = *zmap;
    zone_map_registry_.emplace(key, std::move(zmap));
    zone_map_registry_lru_.push_back(key);
    return ref;
}
```

- [x] **Step 4: Run tests, confirm pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.GetOrOpenZoneMap*`
Expected: PASS, including the cap/eviction test.

- [x] **Step 5: Commit**

```bash
git add include/XStore/XStore.h src/XStore/XStore_Management.cpp tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): add per-(db,year) ZoneMapFile registry"
```

---

## Task 4: Wire `UnaryInsert` into the zone map

**Status: NOT STARTED.**

**Files:**
- Modify: `src/XStore/XStore_UnaryOps.cpp:13-...` (after the successful row write, both write sites at `:161` and `:237` per the SYNC/ASYNC-apply split)
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `GetOrOpenZoneMap` (Task 3), `ZoneMapFile::UpdateRow` (Task 2), `XStore::GetFDataValue(const FData*, ValueStruct*)` (`XStore.h:676`, already exists) for coercing each `RowData.fielddata(i)` to a numeric value the same way aggregate kernels already do.
- Produces: nothing new for later tasks — Task 5 mirrors this exact pattern for Batch/Range.

- [ ] **Step 1: Write the failing test — inserting through `UnaryInsert` populates the zone map immediately**

`DbPath`/`XStoreZoneMapTest` are the fixture from Task 3, already in the repo. `RowsPerBlock`/`BlockIndexForRow` are from Task 1.

```cpp
namespace {

using xstore_test::CreateTestDb;

// 2023-01-01T00:00:00Z, SECOND resolution -- the start-of-year epoch XStore's
// own TimeStruct year-bucketing computes internally for any 2023 timestamp
// (docs/XStore_Technical_Reference.md §4.3). Used here so the test computes
// `diff`/block index the exact same way production code does, rather than
// guessing or hardcoding block 0 (wrong: 1700600000 is ~28 million seconds
// into 2023, nowhere near its first block).
constexpr unsigned long long kYear2023StartEpoch = 1672531200ULL;

TEST_F(XStoreZoneMapTest, UnaryInsertUpdatesZoneMapForNumericColumns) {
    XStore store;
    CreateTestDb(store, "ZMAPUNARY", XStore_Proto::SECOND, 64);

    unsigned long long timestamp = 1700600000ULL; // 2023-11-21 -- within year 2023
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(7.5);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(timestamp, row, "ZMAPUNARY", &rep, XStore_Proto::SYNC);

    // No explicit flush call -- UpdateRow is write-through (Task 2), so this
    // is already durable.
    ZoneMapFile &zmap = store.GetOrOpenZoneMap(DbPath("ZMAPUNARY"), 2023, 64);
    uint64_t diff = timestamp - kYear2023StartEpoch;
    uint64_t block = BlockIndexForRow(diff, zmap.RowsPerBlockValue());
    auto stats = zmap.TryGetBlockStats(block);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 7.5);
    EXPECT_EQ((*stats)[0].valid_count, 1u);
}

TEST_F(XStoreZoneMapTest, UnaryInsertTreatsHoleAsNullNotZero) {
    XStore store;
    CreateTestDb(store, "ZMAPHOLE", XStore_Proto::SECOND, 64);

    unsigned long long timestamp = 1700600000ULL;
    XStore_Proto::RowData row; // fielddata left empty -- a hole at column 0
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(timestamp, row, "ZMAPHOLE", &rep, XStore_Proto::SYNC);

    ZoneMapFile &zmap = store.GetOrOpenZoneMap(DbPath("ZMAPHOLE"), 2023, 64);
    uint64_t diff = timestamp - kYear2023StartEpoch;
    uint64_t block = BlockIndexForRow(diff, zmap.RowsPerBlockValue());
    auto stats = zmap.TryGetBlockStats(block);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 0u);
    EXPECT_EQ((*stats)[0].null_count, 1u);
}

} // namespace
```

- [ ] **Step 2: Run, confirm failure**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.UnaryInsert*`
Expected: FAIL — zone map has no stats (feature not wired yet).

- [ ] **Step 3: Add a shared observation-building helper and call it from both `UnaryInsert` write sites**

```cpp
// include/XStore/XStore_Internal.h -- shared by Unary/Batch/Range (Task 5 reuses this)
#include "include/XStore/XStore_ZoneMap.h"
#include "include/DataStructs/ValueStruct.h"

inline std::vector<ZoneMapObservation> BuildZoneMapObservations(XStore &store, const XStore_Proto::RowData &row) {
    std::vector<ZoneMapObservation> observations;
    int tracked = static_cast<int>(XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);
    observations.reserve(std::min(tracked, row.fielddata_size()));
    for (int col = 0; col < tracked; ++col) {
        if (col >= row.fielddata_size()) break; // no more fields in this row -- not a hole, just absent
        ValueStruct vs;
        store.GetFDataValue(&row.fielddata(col), &vs);
        observations.push_back({static_cast<uint32_t>(col), static_cast<double>(vs.value), vs.is_initialized});
    }
    return observations;
}
```

```cpp
// src/XStore/XStore_UnaryOps.cpp -- immediately after each successful write
// (both the SYNC-path write around :161 and the ASYNC-apply write around :237)
{
    unsigned short row_length = static_cast<unsigned short>(cfg_struct.rowlength());
    ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length);
    zmap.UpdateRow(diff, BuildZoneMapObservations(*this, data));
}
```

- [ ] **Step 4: Run tests, confirm pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.UnaryInsert*`
Expected: 2/2 PASS.

- [ ] **Step 5: Run full suite to confirm zero regressions**

Run: `./build/tests/xstore_tests`
Expected: all existing tests still pass, count increased by the new zone-map tests.

- [ ] **Step 6: Commit**

```bash
git add include/XStore/XStore_Internal.h src/XStore/XStore_UnaryOps.cpp tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): update zone map on every UnaryInsert"
```

---

## Task 5: Wire `BatchInsert`/`RangeInsert` into the zone map

**Status: NOT STARTED.**

**Files:**
- Modify: `src/XStore/XStore_BatchOps.cpp` — per-row, inside the existing year-bucket loop (`YearBucketMap`-driven, same loop `WalRecord` construction already iterates per `TODO.md`'s async-batch-range-wal plan)
- Modify: `src/XStore/XStore_RangeOps.cpp` — mirrors `XStore_BatchOps.cpp` exactly
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `BuildZoneMapObservations` (Task 4), `GetOrOpenZoneMap` (Task 3).
- Produces: nothing new — this is the last insert-path integration point.

- [ ] **Step 1: Write the failing test — a multi-row batch spanning one block updates it once per row**

`DbPath`/`XStoreZoneMapTest` are the fixture from Task 3; `BlockIndexForRow` is from Task 1.

```cpp
namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, BatchInsertUpdatesZoneMapForEveryRow) {
    XStore store;
    CreateTestDb(store, "ZMAPBATCH", XStore_Proto::SECOND, 64);

    unsigned long long base_timestamp = 1700600000ULL; // 2023-11-21
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("ZMAPBATCH");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (int i = 0; i < 5; ++i) {
        auto *entry = req.add_batchinsert_data();
        entry->set_timestamp(base_timestamp + i);
        entry->mutable_data()->add_fielddata()->set_doubledata(static_cast<double>(i));
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);

    ZoneMapFile &zmap = store.GetOrOpenZoneMap(DbPath("ZMAPBATCH"), 2023, 64);
    // 2023-01-01T00:00:00Z -- see kYear2023StartEpoch in Task 4's tests for
    // why this must be computed, not assumed to be block 0.
    uint64_t diff = base_timestamp - 1672531200ULL;
    uint64_t block = BlockIndexForRow(diff, zmap.RowsPerBlockValue());
    auto stats = zmap.TryGetBlockStats(block);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 0.0);
    EXPECT_DOUBLE_EQ((*stats)[0].max_value, 4.0);
    EXPECT_EQ((*stats)[0].valid_count, 5u);
}

} // namespace
```

- [ ] **Step 2: Run, confirm failure**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.BatchInsert*`
Expected: FAIL.

- [ ] **Step 3: Add the same three-line hook to `BatchInsert`'s and `RangeInsert`'s per-row loop**

```cpp
// src/XStore/XStore_BatchOps.cpp -- inside the per-row loop that already
// builds each WalRow for the current year-bucket group
{
    ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length);
    zmap.UpdateRow(diff, BuildZoneMapObservations(*this, entry.data()));
}
```

Apply the identical block to `src/XStore/XStore_RangeOps.cpp`'s equivalent loop.

- [ ] **Step 4: Run tests, confirm pass, then full suite**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.*` then `./build/tests/xstore_tests`
Expected: all PASS, zero regressions.

- [ ] **Step 5: Commit**

```bash
git add src/XStore/XStore_BatchOps.cpp src/XStore/XStore_RangeOps.cpp tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): update zone map on BatchInsert/RangeInsert"
```

---

## Task 6: `MinAggregate`/`MaxAggregate` block-skip pruning

**Status: NOT STARTED.**

**Files:**
- Modify: `src/XStore/XStore_Aggregates.cpp` — `MinAggregate`/`MaxAggregate`, wrapping the existing call into `ProcessFileRangeSelect`
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `ZoneMapFile::TryGetBlockStats` (Task 2), `ProcessFileRangeSelect<Cmp>` (`XStore.h:398-400`, unchanged).
- Produces: nothing new for later tasks — Task 7 follows the same shape for Sum/Avg.

- [ ] **Step 1: Write the failing test — a huge unmatched range still finds the right min despite most blocks being prunable**

`XStoreZoneMapTest`/`RowsPerBlock` are from Tasks 1/3, already in the repo.

```cpp
namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, MinAggregatePrunesBlocksButStillFindsCorrectAnswer) {
    XStore store;
    CreateTestDb(store, "ZMAPMINPRUNE", XStore_Proto::SECOND, 64);

    // Populate enough rows to span several blocks (RowsPerBlock(64) rows/block);
    // plant the true minimum in the LAST block only, everything else higher.
    uint64_t rows_per_block = RowsPerBlock(64);
    unsigned long long base = 1700000000ULL;
    for (uint64_t block = 0; block < 3; ++block) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(block == 2 ? -100.0 : 100.0 + static_cast<double>(block));
        store.UnaryInsert(base + block * rows_per_block, row, "ZMAPMINPRUNE", &rep, XStore_Proto::SYNC);
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(base, base + 3 * rows_per_block, "ZMAPMINPRUNE", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.data().doubledata(), -100.0);
}

TEST_F(XStoreZoneMapTest, MinAggregateCorrectWhenRangeBoundaryFallsMidBlock) {
    // The queried range covers only part of a block -- that block can never
    // be a fully-prunable or fully-pushdownable candidate, so this exercises
    // the real per-row scan fallback path and must still find the true min.
    XStore store;
    CreateTestDb(store, "ZMAPMINBOUNDARY", XStore_Proto::SECOND, 64);
    unsigned long long base = 1700000000ULL;

    XStore_Proto::UnaryInsert_REP rep1, rep2;
    XStore_Proto::RowData row1, row2;
    row1.add_fielddata()->set_doubledata(42.0);
    row2.add_fielddata()->set_doubledata(-7.0); // the true min, one second later
    store.UnaryInsert(base, row1, "ZMAPMINBOUNDARY", &rep1, XStore_Proto::SYNC);
    store.UnaryInsert(base + 1, row2, "ZMAPMINBOUNDARY", &rep2, XStore_Proto::SYNC);

    // Range ends right after the second row -- both rows are in the same
    // block, so this block is neither fully before nor fully after the
    // range boundary in a way that would let a naive implementation skip it.
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(base, base + 2, "ZMAPMINBOUNDARY", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.data().doubledata(), -7.0);
}

} // namespace
```

- [ ] **Step 2: Run, confirm the first test currently passes by luck (no pruning yet) and note the second already passes — pruning is additive, not required for correctness today**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.MinAggregate*`
Expected: both PASS already (pruning is a perf optimization, not a correctness fix — these tests exist to pin correctness before AND after Step 3 changes the code path).

- [ ] **Step 3: Add pruning to the per-block walk inside `MinAggregate`/`MaxAggregate`**

```cpp
// src/XStore/XStore_Aggregates.cpp -- inside MinAggregate, before calling
// ProcessFileRangeSelect over a year's full row span, split the span into
// per-block sub-ranges and skip any block the zone map proves can't help:
ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length);
uint64_t rows_per_block = zmap.RowsPerBlockValue();
uint64_t first_row = /* existing diff-based start-of-range row index */;
uint64_t last_row  = /* existing diff-based end-of-range row index */;

for (uint64_t block = BlockIndexForRow(first_row, rows_per_block);
     block <= BlockIndexForRow(last_row, rows_per_block);
     ++block) {
    auto stats = zmap.TryGetBlockStats(block);
    if (stats.has_value() && column_idx < stats->size()) {
        const ZoneMapEntry &e = (*stats)[column_idx];
        if (e.valid_count == 0) continue; // block has no numeric values for this column at all -- skip
        if (best_value.is_initialized && e.min_value >= best_value.value) continue; // MIN: can't improve
        // (MaxAggregate mirrors this with e.max_value <= best_value.value)
    }
    // Not prunable (this block could contain a better value, or its column
    // index is beyond the tracked cap) -- scan it for real via the existing
    // ProcessFileRangeSelect over just this block's row sub-range.
}
```

Wire the analogous `e.max_value <= best_value.value` guard into `MaxAggregate`.

- [ ] **Step 4: Run tests, confirm still pass, then full suite**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.*` then `./build/tests/xstore_tests`
Expected: all PASS, zero regressions — pruning must never change an answer, only which bytes get touched to reach it.

- [ ] **Step 5: Commit**

```bash
git add src/XStore/XStore_Aggregates.cpp tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): prune blocks in MinAggregate/MaxAggregate via zone map"
```

---

## Task 7: `SumAggregate`/`AvgAggregate` fully-covered-block pushdown

**Status: NOT STARTED.**

**Files:**
- Modify: `src/XStore/XStore_Aggregates.cpp` — `SumAggregate`/`AvgAggregate`
- Test: `tests/integration/XStoreZoneMapTest.cpp`

**Interfaces:**
- Consumes: `ZoneMapFile::TryGetBlockStats` (Task 2), `ProcessFileRangeAccumulate` (`XStore.h:404-406`, unchanged for partially-covered blocks).
- Produces: nothing new — this is the last aggregate integration point in this plan.

- [ ] **Step 1: Write the failing (by-luck-passing, correctness-pinning) test**

```cpp
namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, SumAggregateUsesPrecomputedSumForFullyCoveredBlocks) {
    XStore store;
    CreateTestDb(store, "ZMAPSUM", XStore_Proto::SECOND, 64);
    uint64_t rows_per_block = RowsPerBlock(64);
    unsigned long long base = 1700000000ULL;
    double expected_sum = 0.0;
    for (uint64_t i = 0; i < rows_per_block; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>(i));
        store.UnaryInsert(base + i, row, "ZMAPSUM", &rep, XStore_Proto::SYNC);
        expected_sum += static_cast<double>(i);
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(base, base + rows_per_block, "ZMAPSUM", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.data(), expected_sum);
}

} // namespace
```

- [ ] **Step 2: Run, confirm it already passes (pinning correctness before the optimization)**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.SumAggregate*`
Expected: PASS (unoptimized path is already correct).

- [ ] **Step 3: Add the pushdown — a block fully inside `[start_epoch_t, end_epoch_t)` contributes its precomputed sum/count with zero row scan**

```cpp
// src/XStore/XStore_Aggregates.cpp -- inside SumAggregate's per-block loop
// (same block iteration structure Task 6 introduced for MinAggregate)
auto stats = zmap.TryGetBlockStats(block);
bool block_fully_in_range = (block_first_row >= first_row && block_last_row <= last_row);
if (block_fully_in_range && stats.has_value() && column_idx < stats->size()) {
    const ZoneMapEntry &e = (*stats)[column_idx];
    sum_out += e.sum_value;
    valid_count_out += e.valid_count;
    continue; // zero-scan for this block
}
// Partially-covered or unprunable block: fall back to
// ProcessFileRangeAccumulate over just this block's row sub-range, as today.
```

`AvgAggregate` needs no separate change beyond this — it already divides `SumAggregate`'s `sum_out`/`valid_count_out` (per the existing "AVG divides after" comment at `XStore.h:405`).

- [ ] **Step 4: Add a test proving the pushdown is actually exercised (partial + full block mix)**

```cpp
namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, SumAggregateMixesPushdownAndScanForPartialBoundaryBlocks) {
    XStore store;
    CreateTestDb(store, "ZMAPSUMMIX", XStore_Proto::SECOND, 64);
    uint64_t rows_per_block = RowsPerBlock(64);
    unsigned long long base = 1700000000ULL;
    double expected_sum = 0.0;
    // Fill 2 full blocks plus 3 extra rows into a third (partial) block.
    uint64_t total_rows = rows_per_block * 2 + 3;
    for (uint64_t i = 0; i < total_rows; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(1.0);
        store.UnaryInsert(base + i, row, "ZMAPSUMMIX", &rep, XStore_Proto::SYNC);
        expected_sum += 1.0;
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(base, base + total_rows, "ZMAPSUMMIX", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.data(), expected_sum);
}

} // namespace
```

- [ ] **Step 5: Run tests, confirm pass, then full suite**

Run: `./build/tests/xstore_tests --gtest_filter=XStoreZoneMapTest.*` then `./build/tests/xstore_tests`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/XStore/XStore_Aggregates.cpp tests/integration/XStoreZoneMapTest.cpp
git commit -m "feat(zonemap): push SumAggregate/AvgAggregate down to zone map for fully-covered blocks"
```

---

## Task 8: Benchmark gate, documentation, TODO.md update

**Status: NOT STARTED.**

**Files:**
- Modify: `docs/XStore_Technical_Reference.md` — new subsection under §4 (Storage Engine)
- Modify: `TODO.md` — mark the "Zone-map secondary index" item done, with findings
- No code changes

- [ ] **Step 1: Insert-path overhead benchmark**

Per `docs/Cross_Machine_Access_And_Benchmarking.md` methodology: scope to `INSERT_UNARY_SEQ`/`INSERT_UNARY_RAND`/`INSERT_BATCH_SEQ`/`INSERT_BATCH_RAND` (the workloads whose insert path this plan touches), full thread sweep `[1,2,4,8,16,32,64]×8 clients`, baseline = `main` at the commit this branch forked from. This measures the hot-cache-hit cost (same block repeatedly touched — the common sequential case) added to every insert.

- [ ] **Step 2: Query-side pruning win benchmark**

No existing Mystic workload tag exercises a non-timestamp-column predicate/aggregate specifically (confirm via `grep -rn AGGREGATE benchmark_artifact/` and check whether existing `MIN`/`MAX`/`SUM`/`AVG` tags vary column selectivity) — if none does, this needs a new targeted local benchmark (not necessarily a full Mystic gate) comparing `MinAggregate`/`SumAggregate` wall-clock over a wide multi-block range before vs. after Task 6/7, with a synthetic dataset shaped so most blocks are prunable (matching the realistic case zone maps target: a needle-in-haystack range predicate).

- [ ] **Step 3: Document actual zone-map file size for a couple of realistic shapes**

Compute and record (don't estimate) `tracked_columns(64) * 40 bytes * block_count` for at least: a narrow row (64 bytes, SECOND resolution) and a wide row (1536 bytes, SECOND resolution) — replaces the original brainstorm's now-corrected "single-digit MB" claim with real numbers for this design.

- [ ] **Step 4: Write the technical reference subsection and update TODO.md**

Document: block-size derivation, the tracked-column cap and why, the crash-safety argument (dirty-block-never-trusted), and the benchmark numbers from Steps 1-3. Mark the TODO.md item done, following the same "what was verified" style every other completed item in that file already uses.

- [ ] **Step 5: Commit**

```bash
git add docs/XStore_Technical_Reference.md TODO.md
git commit -m "docs(zonemap): document design, benchmark results, and mark TODO item done"
```

---

## Deferred / Future Phases (explicitly out of scope for this plan)

Each of these is its own subsystem-sized plan, not a task to fold in here — see Scope Check in the writing-plans skill this plan followed.

- **Variance/standard deviation** (Welford's online algorithm: running mean + M2, exactly mergeable across blocks — not approximate, same tier of correctness as this plan's min/max/sum/count). Cheapest near-term follow-on of everything in this section — needs one new `VarianceAggregate` RPC op (proto + both client libraries) and two more `double` fields on `ZoneMapEntry`, but no new *category* of correctness guarantee, unlike the approximate sketches below. Left out of Tasks 1-8 only to keep this plan's own scope to what it actually tests end-to-end; a natural Task 9 for a follow-up plan.
- **Approximate aggregate sketches** (percentiles via a mergeable t-digest/KLL sketch, distinct-count via HyperLogLog, mode/top-K via Count-Min Sketch or the Space-Saving algorithm). These require NEW RPC ops (`PercentileAggregate`, `DistinctCountAggregate`, `TopKAggregate` or similar) added to `proto/XStore.proto`, wired through both the C++ and Python client libraries — real wire/API surface growth, unlike this plan's server-only sidecar file. They also introduce a new correctness category XStore has never had: approximate answers with bounded error, which must never be silently returned where an exact one (min/max/sum/count/variance) is expected — needs its own explicit API distinction (e.g. a separate RPC family or an explicit `approximate=true` flag), not an implicit swap. Grouped together here because all three share that same new-correctness-category problem, not because they share an algorithm.
- **Categorical/high-cardinality acceleration** (roaring bitmap per distinct value, bloom filter for point-equality). Useful where min/max prunes poorly (evenly-spread low-cardinality values); needs its own on-disk structure and its own maintenance-on-insert cost analysis, separate from the fixed-width `ZoneMapEntry` this plan defines.
- **String pattern/full-text search** (`LIKE`, regex). Needs an inverted or n-gram/trigram index — structurally a per-row-mutating index, the exact shape this plan's design (and the original brainstorm) explicitly avoided building via a classic B-tree. Out of scope, and in tension with the throughput/low-memory priority this feature is premised on unless scoped very carefully.
- **First/last/OHLC-style bar aggregates and `EXISTS`/`ANY` short-circuit queries.** Cheap byproducts of the same `ZoneMapEntry` this plan already collects (first/last need one more field each; `EXISTS` is just "does any block's `valid_count/null_count` prove a match is possible") — a natural, small follow-on to this plan once it has shipped and been benchmarked, not included here to keep this plan's scope to what Tasks 1-8 actually deliver and test.
