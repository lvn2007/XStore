# Categorical/High-Cardinality Column Acceleration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new equality-lookup RPC (`FindByValue`, `WHERE columnIdx = value`) accelerated by a two-tier `.catidx` sidecar file — a per-block bloom filter (hot, write-through on insert) gating a per-distinct-value roaring bitmap (cold, built at compaction) — so equality queries on non-timestamp columns don't require a full scan, a capability XStore has never had.

**Architecture:** New `.catidx` file per (db, year), separate from `.zmap`, registered/cached the same way (`CatIdxFile` mirrors `ZoneMapFile`, `XStore::GetOrOpenCatIdx` mirrors `GetOrOpenZoneMap`). Bloom filter write-through hooked at the same `ApplyWalRecord` choke point zone-map uses. Bitmap built by a new background compaction thread (modeled on `StartWalWriters`/`WalWriterLoop`) once a year file goes cold, gated by a per-column cardinality cap so continuous columns never get a bitmap. Query path is a new `ScanSegmentForValueMatch` kernel dispatched through the existing `WalkYearSpanParallel`, tiered: bloom-skip → bitmap-exact-lookup (if built) → real-scan fallback.

**Tech Stack:** C++17, GoogleTest, protobuf/ZMQ (existing XStore stack), reuses `XStore_Crc32c.h` for one of two bloom hash functions (no new external dependency).

**Spec:** [docs/superpowers/specs/2026-09-21-categorical-high-cardinality-acceleration-design.md](../specs/2026-09-21-categorical-high-cardinality-acceleration-design.md)

## Global Constraints

- Scope is **equality only** (`WHERE columnIdx = value`). No range predicates (`>=`/`<=`), no `!=`, no unknown-column/any-column search — all explicitly deferred, see spec's "Deferred follow-ons" section.
- Applies automatically to every tracked column (up to `ZONE_MAP_MAX_TRACKED_COLUMNS`/`ConfigStruct.columncount`, same per-DB high-water mark zone-map already uses) — no per-column opt-in.
- Supports all `FData` oneof types (int64/double/bool/bytes/string/etc.), exact match (double equality is bitwise, not epsilon-tolerant).
- `.catidx` is a wholly separate file from `.zmap` — never touch `.zmap`'s format, version, or registry.
- Every write-path hook must fire only **after** the corresponding data-file `pwrite` succeeds — never claim a row before it's durable (same rule zone-map already follows, see `ApplyWalRecord`).
- Bloom filter must never produce a false negative (only false positives are acceptable).
- Bitmap section must be exact (no false positives or negatives) and only built for columns whose distinct-value count stays under `CATIDX_MAX_BITMAP_DISTINCT_VALUES`.
- Full test suite (`./build/tests/xstore_tests`) must stay green after every task.

---

## File Structure

New files:
- `include/XStore/XStore_CatIdx.h` — `CatIdxFileHeader`, `CatIdxObservation`, `EncodeFDataForCatIdx`, bloom hash/bit helpers, `CatIdxFile` class declaration.
- `src/XStore/XStore_CatIdx.cpp` — `CatIdxFile` method bodies, registry (`XStore::GetOrOpenCatIdx`), `BuildCatIdxObservations`, compaction (`RebuildStaleCatIdxForDb`, `CompactOneYearCatIdx`), background thread (`StartCatIdxCompactionThread`/`CatIdxCompactionLoop`).
- `tests/integration/XStoreCatIdxTest.cpp` — all new local tests.

Modified files:
- `proto/XStore.proto` — new `RequestType.FINDBYVALUE`, `FindByValue_REQ`/`FindByValue_REP`, `Request` oneof entry.
- `include/XStore/XStore_Tunables.h` — new `CATIDX_*` constants.
- `include/XStore/XStore.h` — `WalRow` gets a `catidx_observations` field; new method declarations (`FindByValue`, `GetOrOpenCatIdx`, `BuildCatIdxObservations`, `ScanSegmentForValueMatch`, compaction functions).
- `src/XStore/XStore_UnaryOps.cpp:72`, `src/XStore/XStore_BatchOps.cpp:146`, `src/XStore/XStore_RangeOps.cpp:106` — add `BuildCatIdxObservations` call alongside the existing `BuildZoneMapObservations` call.
- `src/XStore/XStore_Wal.cpp` — `ApplyWalRecord` gets a `catidx.UpdateRow(...)` call alongside `zmap.UpdateRow(...)`; `StartWalWriters`-adjacent startup gets the new compaction thread start call.
- `src/XStore/XStore_Aggregates.cpp` — new `ScanSegmentForValueMatch` kernel, `FindByValue` method.
- `src/XStore/XStore_Management.cpp` — `InitDB()`'s existing rebuild call site gets a sibling `RebuildStaleCatIdxForDb` call.
- `src/XStore_Networking/ZMQ_Server.cpp` — new `case XStore_Proto::Request::kFindbyvalue:`.
- `client_src/Cpp/XStore_Client.h` — new `FindByValue` method.
- `client_src/Python/XStore.py` — new `findByValue` method.
- `tests/integration/XStoreZmqNetworkTest.cpp` — new round-trip test.
- `docs/XStore_Technical_Reference.md`, `TODO.md` — final docs task.

---

### Task 1: Proto schema

**Files:**
- Modify: `proto/XStore.proto`

**Interfaces:**
- Produces: `XStore_Proto::RequestType::FINDBYVALUE`, `XStore_Proto::FindByValue_REQ` (fields: `rtype`, `starttime`, `endtime`, `dbname`, `columnidx`, `value`), `XStore_Proto::FindByValue_REP` (fields: `queryresponse` repeated `UnaryQuery_REP`, `querystatus`), `XStore_Proto::Request::kFindbyvalue`.

- [ ] **Step 1: Add the new RequestType enum value**

In `proto/XStore.proto`, find the `RequestType` enum (currently ends `... TOPK = 24;`) and add:

```protobuf
enum RequestType {
  INFO = 0; CREATEDB = 1; REMOVEDB = 2; REMOVEDBFILES = 3; UNARYINSERT = 4;
  UNARYQUERY = 5; UNARYDELETE = 6; BATCHINSERT = 7; BATCHQUERY = 8; BATCHDELETE = 9;
  RANGEINSERT = 10; RANGEQUERY = 11; RANGEDELETE = 12; MIN = 13; MAX = 14;
  SUM = 15; AVG = 16; PING = 17; VARIANCE = 18; STDDEV = 19;
  EXISTS = 20; OHLC = 21; PERCENTILE = 22; DISTINCTCOUNT = 23; TOPK = 24;
  FINDBYVALUE = 25;
}
```

- [ ] **Step 2: Add the request/response messages**

Add near `Exists_REQ`/`Exists_REP` (find them by searching for `message Exists_REQ`):

```protobuf
message FindByValue_REQ {
  RequestType RType = 1;
  uint64 startTime = 2;
  uint64 endTime = 3;
  string dbName = 4;
  sint32 columnIdx = 5;
  FData value = 6;
}

message FindByValue_REP {
  repeated UnaryQuery_REP queryResponse = 1;
  optional string queryStatus = 3;
}
```

`FindByValue_REP` deliberately reuses `UnaryQuery_REP`'s exact wire shape (`{timestamp, data}` per matching row) — this is what makes it possible to build the reply with the existing `EncodeQueryResponseEntry` helper (`include/XStore/XStore_WireEncode.h:82`) that `RangeQuery_REP`/`BatchQuery`'s streamed responses already use, field-numbered identically to `RangeQuery_REP` (`proto/XStore.proto:542-545`) for consistency.

- [ ] **Step 3: Add the oneof entry**

In the `Request` message's `payload` oneof (find it by searching `message Request`, currently ends `... TopK_REQ topk = 25;`), add:

```protobuf
FindByValue_REQ findbyvalue = 26;
```

- [ ] **Step 4: Regenerate protobuf and build**

Run: `cmake --build build -j$(nproc)`
Expected: clean build. This confirms the proto compiles and the generated `XStore.pb.h`/`XStore.pb.cc` have `XStore_Proto::FindByValue_REQ`, `XStore_Proto::FindByValue_REP`, `XStore_Proto::Request::kFindbyvalue`, `XStore_Proto::FINDBYVALUE`.

- [ ] **Step 5: Commit**

```bash
git add proto/XStore.proto
git commit -m "proto: add FINDBYVALUE request type and messages for categorical index lookup"
```

---

### Task 2: CatIdx tunables

**Files:**
- Modify: `include/XStore/XStore_Tunables.h`

**Interfaces:**
- Produces: `XStoreTunables::CATIDX_VERSION`, `CATIDX_BLOOM_HASH_COUNT`, `CATIDX_BLOOM_BYTES_PER_ENTRY`, `CATIDX_MAX_TRACKED_COLUMNS`, `CATIDX_REGISTRY_CAP`, `CATIDX_HOT_CACHE_BLOCKS`, `CATIDX_MAX_BITMAP_DISTINCT_VALUES`, `CATIDX_COMPACTION_POLL_INTERVAL_SEC`.

- [ ] **Step 1: Add the tunables**

In `include/XStore/XStore_Tunables.h`, near the existing `## Zone Map` block (around the `ZONE_MAP_*` constants), add a new `## CatIdx` block:

```cpp
// ## CatIdx (categorical/high-cardinality equality-lookup acceleration)
//
// Separate sidecar file from .zmap (see docs/superpowers/specs/
// 2026-09-21-categorical-high-cardinality-acceleration-design.md for why:
// independent version churn, no cache-footprint regression to existing
// zone-map-only queries, isolated corruption blast radius).
inline constexpr uint32_t CATIDX_VERSION = 1;

// k=7 hash functions, ~154 bytes/entry at n=128 (RowsPerBlock default),
// 1% target false-positive rate -- rounded to 160B for 8-byte alignment.
inline constexpr size_t   CATIDX_BLOOM_HASH_COUNT = 7;
inline constexpr size_t   CATIDX_BLOOM_BYTES_PER_ENTRY = 160;

// Same per-DB high-water-mark tracked-column-count concept zone-map uses
// (ConfigStruct.columncount) -- reuse the existing cap rather than a second one.
inline constexpr uint32_t CATIDX_MAX_TRACKED_COLUMNS = ZONE_MAP_MAX_TRACKED_COLUMNS;

// Max resident CatIdxFile instances across all (db,year) pairs -- same
// bound/reasoning as ZONE_MAP_REGISTRY_CAP.
inline constexpr size_t   CATIDX_REGISTRY_CAP = 256;

// Per-(db,year) bloom-block LRU cache size -- same reasoning as ZONE_MAP_HOT_CACHE_BLOCKS.
inline constexpr size_t   CATIDX_HOT_CACHE_BLOCKS = 4;

// Cardinality cap for the compaction-built bitmap section: a column whose
// distinct-value count in a given year crosses this threshold never gets a
// bitmap for that year (stays bloom+real-scan forever) -- this is what lets
// "automatic for every column" coexist with continuous (near-unique-value)
// columns without exploding disk usage. Chosen well above realistic
// categorical cardinalities (symbol/venue/status IDs) and well below where
// a per-value roaring bitmap directory stops paying for itself.
inline constexpr uint64_t CATIDX_MAX_BITMAP_DISTINCT_VALUES = 10000;

// How often the compaction background thread checks for newly-cold year
// files (year rolled over from current calendar year). Compaction is a
// rare, low-frequency background job -- no need for WAL-writer-loop-style
// sub-millisecond polling.
inline constexpr uint64_t CATIDX_COMPACTION_POLL_INTERVAL_SEC = 3600;
```

- [ ] **Step 2: Build**

Run: `cmake --build build -j$(nproc)`
Expected: clean build (tunables are unused so far, no code references them yet — this just confirms no syntax errors).

- [ ] **Step 3: Commit**

```bash
git add include/XStore/XStore_Tunables.h
git commit -m "tunables: add CATIDX_* constants for categorical index acceleration"
```

---

### Task 3: Canonical FData encoding helper

**Files:**
- Create: `include/XStore/XStore_CatIdx.h`
- Test: `tests/integration/XStoreCatIdxTest.cpp` (new file)
- Modify: `tests/CMakeLists.txt` (register the new test file, follow the existing pattern for how `XStoreZoneMapTest.cpp` is registered)

**Interfaces:**
- Produces: `EncodeFDataForCatIdx(const XStore_Proto::FData &f_data) -> std::string`

This is the single canonicalization function used by BOTH the insert-time bloom/bitmap write path (Task 6) and the query-time lookup (Task 7) — using the identical function on both sides is what guarantees a bloom bit set at insert time is found at query time. It must distinguish values that are numerically equal but differently-typed (e.g. `doubleData=5.0` vs `int64Data=5`) as different keys, since XStore's `FData` oneof does not itself claim cross-type numeric equivalence anywhere else in the codebase (aggregates always read via `GetFDataValue` into one canonical `double`, but equality lookup here is exact-oneof-case match, matching the spec's "all FData oneof types" scope, not `GetFDataValue`'s aggregate-numeric-coercion scope).

- [ ] **Step 1: Write the failing test**

Create `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
#include <gtest/gtest.h>
#include "XStore/XStore_CatIdx.h"
#include "XStore.pb.h"

TEST(EncodeFDataForCatIdxTest, SameInt64ValueEncodesIdentically) {
    XStore_Proto::FData a;
    a.set_int64data(42);
    XStore_Proto::FData b;
    b.set_int64data(42);
    EXPECT_EQ(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, DifferentInt64ValuesEncodeDifferently) {
    XStore_Proto::FData a;
    a.set_int64data(42);
    XStore_Proto::FData b;
    b.set_int64data(43);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, SameNumericMagnitudeDifferentOneofCaseEncodesDifferently) {
    // int64Data=5 and doubleData=5.0 are numerically equal but must be
    // treated as distinct keys -- this is exact-type equality, not
    // aggregate-style numeric coercion (see GetFDataValue for that instead).
    XStore_Proto::FData a;
    a.set_int64data(5);
    XStore_Proto::FData b;
    b.set_doubledata(5.0);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, StringDataEncodesRawBytes) {
    XStore_Proto::FData a;
    a.set_stringdata("AAPL");
    XStore_Proto::FData b;
    b.set_stringdata("AAPL");
    XStore_Proto::FData c;
    c.set_stringdata("MSFT");
    EXPECT_EQ(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(c));
}

TEST(EncodeFDataForCatIdxTest, BoolDataEncodesDistinctly) {
    XStore_Proto::FData a;
    a.set_booldata(true);
    XStore_Proto::FData b;
    b.set_booldata(false);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}
```

- [ ] **Step 2: Register the test file and run to verify it fails**

Add `XStoreCatIdxTest.cpp` to `tests/CMakeLists.txt` the same way `XStoreZoneMapTest.cpp` is registered (find that line and add a sibling entry).

Run: `cmake --build build -j$(nproc) 2>&1 | tail -30`
Expected: FAIL — `XStore_CatIdx.h` doesn't exist yet, compile error `fatal error: XStore/XStore_CatIdx.h: No such file or directory`.

- [ ] **Step 3: Implement `EncodeFDataForCatIdx`**

Create `include/XStore/XStore_CatIdx.h`:

```cpp
#pragma once
#include <cstdint>
#include <cstring>
#include <string>

#include "XStore.pb.h"

// Canonical byte encoding for an FData value, used identically at insert
// time (BuildCatIdxObservations) and query time (FindByValue) so a bloom
// bit set for a value at insert is found by the same value's lookup at
// query time. Distinguishes oneof case explicitly (int64Data=5 and
// doubleData=5.0 encode differently) -- this is exact-type equality, not
// GetFDataValue's aggregate-style numeric coercion into one double.
inline std::string EncodeFDataForCatIdx(const XStore_Proto::FData &f_data) {
    std::string out;
    auto case_id = f_data.oneOfData_case();
    out.push_back(static_cast<char>(static_cast<uint8_t>(case_id)));

    switch (case_id) {
        case XStore_Proto::FData::kDoubleData: {
            double v = f_data.doubledata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFloatData: {
            float v = f_data.floatdata();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt32Data: {
            int32_t v = f_data.int32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kInt64Data: {
            int64_t v = f_data.int64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint32Data: {
            uint32_t v = f_data.uint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kUint64Data: {
            uint64_t v = f_data.uint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint32Data: {
            int32_t v = f_data.sint32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSint64Data: {
            int64_t v = f_data.sint64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed32Data: {
            uint32_t v = f_data.fixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kFixed64Data: {
            uint64_t v = f_data.fixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed32Data: {
            int32_t v = f_data.sfixed32data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kSfixed64Data: {
            int64_t v = f_data.sfixed64data();
            out.append(reinterpret_cast<const char*>(&v), sizeof(v));
            break;
        }
        case XStore_Proto::FData::kBoolData: {
            char v = f_data.booldata() ? 1 : 0;
            out.push_back(v);
            break;
        }
        case XStore_Proto::FData::kStringData:
            out.append(f_data.stringdata());
            break;
        case XStore_Proto::FData::kBytesData:
            out.append(f_data.bytesdata());
            break;
        default:
            // ONEOFDATA_NOT_SET -- a hole. Caller (BuildCatIdxObservations)
            // is responsible for not emitting an observation for holes at
            // all; this function is never called on an unset FData in
            // practice, but returns the tag-only bytes rather than
            // asserting, matching the codebase's existing "hole is not an
            // error" precedent (see BuildZoneMapObservations).
            break;
    }
    return out;
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=EncodeFDataForCatIdxTest.*`
Expected: PASS, 5/5.

- [ ] **Step 5: Commit**

```bash
git add include/XStore/XStore_CatIdx.h tests/integration/XStoreCatIdxTest.cpp tests/CMakeLists.txt
git commit -m "feat: add canonical FData encoding for categorical index equality keys"
```

---

### Task 4: Bloom filter math + `CatIdxFile` class

**Files:**
- Modify: `include/XStore/XStore_CatIdx.h` (add declarations)
- Create: `src/XStore/XStore_CatIdx.cpp`
- Modify: `tests/integration/XStoreCatIdxTest.cpp` (add tests)
- Modify: `src/CMakeLists.txt` (register the new `.cpp`, follow the pattern for `XStore_ZoneMap.cpp`)

**Interfaces:**
- Consumes: `EncodeFDataForCatIdx` (Task 3), `Crc32c(const void*, size_t)` (`include/XStore/XStore_Crc32c.h`, existing).
- Produces: `CatIdxObservation` struct, `CatIdxFileHeader` struct, `CatIdxFile` class with `Open`, `UpdateRow`, `MightContain`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
#include "XStore/XStore_CatIdx.h"
#include <filesystem>

namespace {
std::string TempCatIdxPath(const std::string &name) {
    auto dir = std::filesystem::temp_directory_path() / "xstore_catidx_test";
    std::filesystem::create_directories(dir);
    return (dir / name).string();
}
} // namespace

TEST(CatIdxFileTest, OpenCreatesFreshFile) {
    std::string path = TempCatIdxPath("open_fresh.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, /*row_length=*/64, /*tracked_column_count=*/4));
}

TEST(CatIdxFileTest, InsertedValueIsFound) {
    std::string path = TempCatIdxPath("insert_found.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 4));

    XStore_Proto::FData val;
    val.set_int64data(137);
    CatIdxObservation obs{/*column_idx=*/2, EncodeFDataForCatIdx(val), /*is_present=*/true};
    f.UpdateRow(/*row_index=*/5, {obs});

    EXPECT_TRUE(f.MightContain(/*block_index=*/0, /*column_idx=*/2, EncodeFDataForCatIdx(val)));
}

TEST(CatIdxFileTest, NeverFalseNegativeAcrossManyValues) {
    std::string path = TempCatIdxPath("no_false_negative.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    std::vector<std::string> inserted;
    for (int64_t i = 0; i < 500; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        std::string enc = EncodeFDataForCatIdx(val);
        f.UpdateRow(static_cast<uint64_t>(i), {CatIdxObservation{0, enc, true}});
        inserted.push_back(enc);
    }
    for (const auto &enc : inserted) {
        // block_index for row i with default RowsPerBlock(64) -- compute
        // the same way BlockIndexForRow does, reusing rows_per_block from
        // the zone-map header math (RowsPerBlock(64) at 2MiB target).
    }
    // Re-check every inserted value against its own block -- no false
    // negatives allowed, ever.
    uint64_t rows_per_block = RowsPerBlock(64);
    for (int64_t i = 0; i < 500; ++i) {
        uint64_t block = BlockIndexForRow(static_cast<uint64_t>(i), rows_per_block);
        EXPECT_TRUE(f.MightContain(block, 0, inserted[static_cast<size_t>(i)]))
            << "false negative for value " << i;
    }
}

TEST(CatIdxFileTest, AbsentValueUsuallyNotFound) {
    // Bloom filters are probabilistic -- this asserts the false-positive
    // rate stays reasonable (well under 100%), not that it's exactly zero.
    std::string path = TempCatIdxPath("absent_mostly_not_found.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    for (int64_t i = 0; i < 100; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        f.UpdateRow(static_cast<uint64_t>(i), {CatIdxObservation{0, EncodeFDataForCatIdx(val), true}});
    }

    int false_positives = 0;
    for (int64_t i = 100000; i < 100200; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        if (f.MightContain(0, 0, EncodeFDataForCatIdx(val))) ++false_positives;
    }
    EXPECT_LT(false_positives, 20); // well under 10% at these fill levels
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -30`
Expected: FAIL — `CatIdxFile`, `CatIdxObservation`, `RowsPerBlock`/`BlockIndexForRow` (reused from zone-map header, need including) not declared.

- [ ] **Step 3: Implement**

Add to `include/XStore/XStore_CatIdx.h` (after `EncodeFDataForCatIdx`):

```cpp
#include <array>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include "XStore/XStore_Crc32c.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore_ZoneMap.h" // reuse RowsPerBlock/BlockIndexForRow -- same block geometry as .zmap

// One tracked-column value observed for one row, ready to fold into that
// row's block's bloom entry. is_present=false means a hole/unset field --
// contributes nothing (mirrors ZoneMapObservation's is_numeric=false case).
struct CatIdxObservation {
    uint32_t    column_idx;
    std::string encoded_value; // from EncodeFDataForCatIdx -- empty/ignored if !is_present
    bool        is_present;
};

struct CatIdxFileHeader {
    uint32_t magic                = 0x43415449; // 'CATI'
    uint32_t version               = XStoreTunables::CATIDX_VERSION;
    uint32_t tracked_column_count  = 0;
    uint32_t block_count           = 0;
    uint64_t bitmap_section_offset = 0; // 0 = not compacted yet
};

// FNV-1a 64-bit -- trivial, no external dependency, used as the second of
// two hash functions for Kirsch-Mitzenmacher double-hashing
// (g_i(x) = h1(x) + i*h2(x)), h1 being the existing hardware-accelerated
// Crc32c(). Repurposing Crc32c outside its original WAL-framing-checksum
// scope is fine functionally (it's just a fast, well-distributed hash here).
inline uint64_t Fnv1a64(const void *data, size_t len) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    const auto *bytes = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < len; ++i) {
        hash ^= bytes[i];
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

class CatIdxFile {
public:
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length,
                             uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

    void UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations);

    bool MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value);

    ~CatIdxFile();

    uint64_t RowsPerBlockValue() const { return rows_per_block_; }

private:
    using BloomEntry = std::array<unsigned char, XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY>;

    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BloomSectionFileOffset(uint64_t block_index) const;
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);
    void FlushBlock(uint64_t block_index);

    int fd_ = -1;
    CatIdxFileHeader header_;
    uint64_t rows_per_block_ = 0;
    std::unordered_map<uint64_t, std::vector<BloomEntry>> cache_; // block_index -> per-column bloom bytes
    std::vector<uint64_t> lru_order_;
    std::mutex mutex_;
};
```

Create `src/XStore/XStore_CatIdx.cpp`:

```cpp
#include "XStore/XStore_CatIdx.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstring>

#include <spdlog/spdlog.h>

namespace {
void BloomSetBit(unsigned char *entry, size_t entry_size, uint64_t h1, uint64_t h2, size_t k) {
    size_t total_bits = entry_size * 8;
    for (size_t i = 0; i < k; ++i) {
        uint64_t combined = h1 + i * h2;
        size_t bit = combined % total_bits;
        entry[bit / 8] |= static_cast<unsigned char>(1u << (bit % 8));
    }
}
bool BloomCheckBit(const unsigned char *entry, size_t entry_size, uint64_t h1, uint64_t h2, size_t k) {
    size_t total_bits = entry_size * 8;
    for (size_t i = 0; i < k; ++i) {
        uint64_t combined = h1 + i * h2;
        size_t bit = combined % total_bits;
        if ((entry[bit / 8] & static_cast<unsigned char>(1u << (bit % 8))) == 0) return false;
    }
    return true;
}
} // namespace

bool CatIdxFile::Open(const std::string &path, uint16_t row_length, uint32_t tracked_column_count) {
    rows_per_block_ = RowsPerBlock(row_length);

    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        spdlog::error("[CATIDX] - Unable to open/create {}", path);
        return false;
    }

    struct stat st{};
    if (fstat(fd_, &st) != 0) return false;

    if (st.st_size >= static_cast<off_t>(sizeof(CatIdxFileHeader))) {
        CatIdxFileHeader on_disk{};
        if (pread(fd_, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk)) &&
            on_disk.magic == CatIdxFileHeader{}.magic &&
            on_disk.version == XStoreTunables::CATIDX_VERSION &&
            on_disk.tracked_column_count > 0) {
            header_ = on_disk;
            return true;
        }
    }

    return WriteFreshHeader(row_length, tracked_column_count);
}

bool CatIdxFile::WriteFreshHeader(uint16_t /*row_length*/, uint32_t tracked_column_count) {
    header_ = CatIdxFileHeader{};
    header_.tracked_column_count = tracked_column_count;
    header_.block_count = 0;
    header_.bitmap_section_offset = 0;

    if (ftruncate(fd_, static_cast<off_t>(sizeof(CatIdxFileHeader))) != 0) return false;
    ssize_t written = pwrite(fd_, &header_, sizeof(header_), 0);
    return written == static_cast<ssize_t>(sizeof(header_));
}

off_t CatIdxFile::BloomSectionFileOffset(uint64_t block_index) const {
    return static_cast<off_t>(sizeof(CatIdxFileHeader)) +
           static_cast<off_t>(block_index * header_.tracked_column_count * sizeof(BloomEntry));
}

void CatIdxFile::EnsureBlockLoaded(uint64_t block_index) {
    if (cache_.count(block_index) > 0) {
        lru_order_.erase(std::remove(lru_order_.begin(), lru_order_.end(), block_index), lru_order_.end());
        lru_order_.push_back(block_index);
        return;
    }

    std::vector<BloomEntry> entries(header_.tracked_column_count);
    off_t offset = BloomSectionFileOffset(block_index);
    size_t bytes_to_read = header_.tracked_column_count * sizeof(BloomEntry);

    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= offset + static_cast<off_t>(bytes_to_read)) {
        pread(fd_, entries.data(), bytes_to_read, offset);
    } else {
        for (auto &e : entries) e.fill(0);
        ftruncate(fd_, offset + static_cast<off_t>(bytes_to_read));
        pwrite(fd_, entries.data(), bytes_to_read, offset);
        if (block_index >= header_.block_count) {
            header_.block_count = static_cast<uint32_t>(block_index + 1);
            pwrite(fd_, &header_, sizeof(header_), 0);
        }
    }

    cache_[block_index] = std::move(entries);
    lru_order_.push_back(block_index);
    EvictIfOverCapacity();
}

void CatIdxFile::EvictIfOverCapacity() {
    while (cache_.size() > XStoreTunables::CATIDX_HOT_CACHE_BLOCKS && !lru_order_.empty()) {
        uint64_t victim = lru_order_.front();
        lru_order_.erase(lru_order_.begin());
        cache_.erase(victim); // read-cache only, no flush needed -- UpdateRow is already write-through
    }
}

void CatIdxFile::FlushBlock(uint64_t block_index) {
    auto it = cache_.find(block_index);
    if (it == cache_.end()) return;
    off_t offset = BloomSectionFileOffset(block_index);
    size_t bytes = header_.tracked_column_count * sizeof(BloomEntry);
    pwrite(fd_, it->second.data(), bytes, offset);
}

void CatIdxFile::UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t block_index = BlockIndexForRow(row_index, rows_per_block_);
    EnsureBlockLoaded(block_index);

    auto &entries = cache_[block_index];
    for (const auto &obs : observations) {
        if (!obs.is_present || obs.column_idx >= header_.tracked_column_count) continue;
        uint64_t h1 = Crc32c(obs.encoded_value.data(), obs.encoded_value.size());
        uint64_t h2 = Fnv1a64(obs.encoded_value.data(), obs.encoded_value.size());
        BloomSetBit(entries[obs.column_idx].data(), entries[obs.column_idx].size(),
                    h1, h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
    }
    FlushBlock(block_index); // write-through, same immediacy as ZoneMapFile::UpdateRow
}

bool CatIdxFile::MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (column_idx >= header_.tracked_column_count) return true; // no bloom for this column -- caller must real-scan
    EnsureBlockLoaded(block_index);
    auto &entries = cache_[block_index];
    uint64_t h1 = Crc32c(encoded_value.data(), encoded_value.size());
    uint64_t h2 = Fnv1a64(encoded_value.data(), encoded_value.size());
    return BloomCheckBit(entries[column_idx].data(), entries[column_idx].size(),
                          h1, h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
}

CatIdxFile::~CatIdxFile() {
    if (fd_ >= 0) close(fd_);
}
```

Register `src/XStore/XStore_CatIdx.cpp` in `src/CMakeLists.txt` next to `XStore_ZoneMap.cpp`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=CatIdxFileTest.*`
Expected: PASS, 4/4.

- [ ] **Step 5: Commit**

```bash
git add include/XStore/XStore_CatIdx.h src/XStore/XStore_CatIdx.cpp tests/integration/XStoreCatIdxTest.cpp src/CMakeLists.txt
git commit -m "feat: add CatIdxFile bloom filter (write-through, per-block-per-column)"
```

---

### Task 5: `XStore::GetOrOpenCatIdx` registry

**Files:**
- Modify: `include/XStore/XStore.h` (declare static registry members + method)
- Modify: `src/XStore/XStore_CatIdx.cpp` (implement)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `CatIdxFile` (Task 4).
- Produces: `XStore::GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS) -> CatIdxFile&`, `XStore::ClearCatIdxRegistryForTests()`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
#include "XStore/XStore.h"

TEST(GetOrOpenCatIdxTest, SameDbYearReturnsSameInstance) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_db");
    std::filesystem::create_directories(base);

    CatIdxFile &a = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    CatIdxFile &b = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    EXPECT_EQ(&a, &b);
}

TEST(GetOrOpenCatIdxTest, DifferentYearReturnsDifferentInstance) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_db2");
    std::filesystem::create_directories(base);

    CatIdxFile &a = XStore::GetOrOpenCatIdx(base, 2023, 64, 4);
    CatIdxFile &b = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    EXPECT_NE(&a, &b);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `GetOrOpenCatIdx`/`ClearCatIdxRegistryForTests` not declared.

- [ ] **Step 3: Implement**

In `include/XStore/XStore.h`, near the existing `GetOrOpenZoneMap` declaration, add:

```cpp
static CatIdxFile& GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                     uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);
static void ClearCatIdxRegistryForTests();

private: // (or alongside the existing zone_map_registry_ members' access level)
static std::mutex catidx_registry_mutex_;
static std::unordered_map<std::string, std::unique_ptr<CatIdxFile>> catidx_registry_;
static std::vector<std::string> catidx_registry_lru_;
```

Also add `#include "XStore/XStore_CatIdx.h"` near the existing `#include "XStore/XStore_ZoneMap.h"` in `XStore.h`.

In `src/XStore/XStore_CatIdx.cpp`, add (mirrors `GetOrOpenZoneMap` at `src/XStore/XStore_Management.cpp:161-231` exactly, minus the clamp-to-on-disk-width guard, which Task 11 adds once schema growth is wired up):

```cpp
std::mutex XStore::catidx_registry_mutex_;
std::unordered_map<std::string, std::unique_ptr<CatIdxFile>> XStore::catidx_registry_;
std::vector<std::string> XStore::catidx_registry_lru_;

CatIdxFile& XStore::GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                     uint32_t tracked_column_count) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);

    auto it = catidx_registry_.find(key);
    if (it != catidx_registry_.end()) {
        catidx_registry_lru_.erase(std::remove(catidx_registry_lru_.begin(), catidx_registry_lru_.end(), key),
                                    catidx_registry_lru_.end());
        catidx_registry_lru_.push_back(key);
        return *it->second;
    }

    if (catidx_registry_.size() >= XStoreTunables::CATIDX_REGISTRY_CAP && !catidx_registry_lru_.empty()) {
        std::string victim = catidx_registry_lru_.front();
        catidx_registry_lru_.erase(catidx_registry_lru_.begin());
        catidx_registry_.erase(victim);
    }

    auto catidx = std::make_unique<CatIdxFile>();
    std::string path = fmt::format("{}/{}.catidx", db_base_path, year);
    if (!catidx->Open(path, row_length, tracked_column_count)) {
        spdlog::error("[CATIDX] - Unable to open {}", path);
        throw std::runtime_error(fmt::format("CatIdxFile::Open failed for {}", path));
    }
    CatIdxFile &ref = *catidx;
    catidx_registry_.emplace(key, std::move(catidx));
    catidx_registry_lru_.push_back(key);
    return ref;
}

void XStore::ClearCatIdxRegistryForTests() {
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    catidx_registry_.clear();
    catidx_registry_lru_.clear();
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=GetOrOpenCatIdxTest.*`
Expected: PASS, 2/2.

- [ ] **Step 5: Commit**

```bash
git add include/XStore/XStore.h src/XStore/XStore_CatIdx.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: add per-(db,year) CatIdxFile registry, mirrors zone-map registry"
```

---

### Task 6: Write-path integration (bloom, hot)

**Files:**
- Modify: `include/XStore/XStore.h` (`WalRow` struct, `BuildCatIdxObservations` declaration)
- Modify: `include/XStore/XStore_Internal.h` or `src/XStore/XStore_CatIdx.cpp` (implement `BuildCatIdxObservations` — place it next to `BuildZoneMapObservations` for discoverability)
- Modify: `src/XStore/XStore_UnaryOps.cpp:72`, `src/XStore/XStore_BatchOps.cpp:146`, `src/XStore/XStore_RangeOps.cpp:106`
- Modify: `src/XStore/XStore_Wal.cpp` (`ApplyWalRecord`)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `EncodeFDataForCatIdx` (Task 3), `CatIdxFile::UpdateRow` (Task 4), `GetOrOpenCatIdx` (Task 5).
- Produces: `XStore::BuildCatIdxObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count) -> std::vector<CatIdxObservation>`, `WalRow::catidx_observations`.

- [ ] **Step 1: Add `catidx_observations` to `WalRow`**

In `include/XStore/XStore.h`, immediately after the existing `zone_map_observations` field in the `WalRow` struct (around line 207):

```cpp
            std::vector<ZoneMapObservation> zone_map_observations;
            // Same "precomputed at enqueue time, applied by ApplyWalRecord
            // only after this row's real pwrite succeeds" contract as
            // zone_map_observations above -- see BuildCatIdxObservations.
            std::vector<CatIdxObservation> catidx_observations;
```

- [ ] **Step 2: Declare and implement `BuildCatIdxObservations`**

In `include/XStore/XStore.h`, near the `BuildZoneMapObservations` declaration (line 799):

```cpp
static std::vector<CatIdxObservation> BuildCatIdxObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count);
```

In `include/XStore/XStore_Internal.h`, immediately after `BuildZoneMapObservations`'s body:

```cpp
inline std::vector<CatIdxObservation> XStore::BuildCatIdxObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count) {
    (void)store; // unused -- unlike BuildZoneMapObservations, no GetFDataValue coercion needed; EncodeFDataForCatIdx reads FData directly
    const int tracked = static_cast<int>(tracked_column_count);
    std::vector<CatIdxObservation> observations;
    observations.reserve(static_cast<size_t>(tracked));
    for (int col = 0; col < tracked; ++col) {
        if (col < row.fielddata_size() && row.fielddata(col).oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
            observations.push_back({static_cast<uint32_t>(col), EncodeFDataForCatIdx(row.fielddata(col)), true});
        } else {
            observations.push_back({static_cast<uint32_t>(col), std::string(), false});
        }
    }
    return observations;
}
```

Note this covers ALL `FData` oneof cases (not just numeric, unlike `BuildZoneMapObservations`'s `is_numeric` gate) — matches the spec's "all FData types" scope.

- [ ] **Step 3: Wire the three insert call sites**

In `src/XStore/XStore_UnaryOps.cpp:71-72`, change:

```cpp
    record.rows.push_back(WalRow{unix_time, std::string(row_buf.data(), cfg_struct.rowlength()),
                                  BuildZoneMapObservations(*this, data, record.tracked_column_count)});
```
to:
```cpp
    record.rows.push_back(WalRow{unix_time, std::string(row_buf.data(), cfg_struct.rowlength()),
                                  BuildZoneMapObservations(*this, data, record.tracked_column_count),
                                  BuildCatIdxObservations(*this, data, record.tracked_column_count)});
```

Apply the identical change (append `BuildCatIdxObservations(*this, entry.data(), record.tracked_column_count)` as a 4th aggregate-init argument) at `src/XStore/XStore_BatchOps.cpp:145-146` and `src/XStore/XStore_RangeOps.cpp:105-106`.

- [ ] **Step 4: Hook `ApplyWalRecord`**

In `src/XStore/XStore_Wal.cpp`, at the zone-map hook point (around line 202-203), add the CatIdx registry fetch alongside the zone-map one:

```cpp
    ZoneMapFile &zmap = GetOrOpenZoneMap(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                                          record_to_apply.tracked_column_count);
    CatIdxFile &catidx = GetOrOpenCatIdx(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                                         record_to_apply.tracked_column_count);
```

And in the per-run success branch (around line 246-249), alongside the existing `zmap.UpdateRow(...)` call:

```cpp
            for (size_t i = run_start; i < run_end; ++i) {
                uint64_t diff = record_to_apply.rows[i].epoch_time - first_ts.first_epoch;
                zmap.UpdateRow(diff, record_to_apply.rows[i].zone_map_observations);
                catidx.UpdateRow(diff, record_to_apply.rows[i].catidx_observations);
            }
```

This mirrors zone-map's own reasoning exactly: the bloom filter is only updated after this row's `pwrite` is confirmed successful, never before — same crash-safety property.

- [ ] **Step 5: Write and run integration test**

Add to `tests/integration/XStoreCatIdxTest.cpp` (uses the existing `xstore_test::CreateTestDb`/`UnaryInsert` helpers from `XStoreTestSupport.h`, same pattern every other `XStoreTest.cpp`-style test file uses):

```cpp
#include "XStoreTestSupport.h"

TEST_F(XStoreCatIdxTestFixture, InsertedValueBloomIsSetAfterApplyWalRecord) {
    // db_ / db_name_ / db_base_path_ set up by fixture, same pattern as
    // other XStore*Test.cpp fixtures (see XStoreTestSupport.h::CreateTestDb).
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(137);
    UnaryInsert(1000, row, db_name_); // helper from XStoreTestSupport.h, SYNC durability by default

    XStore_Proto::FData val;
    val.set_int64data(137);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(db_base_path_, /*year=*/TimeStruct(1000, SECOND).year, 64, 1);
    EXPECT_TRUE(catidx.MightContain(0, 0, EncodeFDataForCatIdx(val)));
}
```

(Implementer: adapt the exact fixture base class / helper names to whatever `XStoreZoneMapTest.cpp` already uses for its own `TEST_F` fixtures — same DB-creation/insert helper pattern, just swap the assertion.)

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=*CatIdx*`
Expected: PASS.

- [ ] **Step 6: Full suite regression check**

Run: `./build/tests/xstore_tests`
Expected: all pre-existing tests still pass (this task touches `WalRow`'s aggregate-init call sites and `ApplyWalRecord` — a missed call site would be a compile error, not a silent bug, since `WalRow` is an aggregate).

- [ ] **Step 7: Commit**

```bash
git add include/XStore/XStore.h include/XStore/XStore_Internal.h src/XStore/XStore_UnaryOps.cpp src/XStore/XStore_BatchOps.cpp src/XStore/XStore_RangeOps.cpp src/XStore/XStore_Wal.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: wire CatIdx bloom write-through into ApplyWalRecord (Unary/Batch/Range insert)"
```

---

### Task 7: Query kernel + `FindByValue` (bloom + real-scan, no bitmap yet)

**Files:**
- Modify: `include/XStore/XStore.h` (declare `FindByValue`, `ScanSegmentForValueMatch`)
- Modify: `src/XStore/XStore_Aggregates.cpp` (implement)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `WalkYearSpanParallel` (existing), `CatIdxFile::MightContain` (Task 4), `GetOrOpenCatIdx` (Task 5), `EncodeFDataForCatIdx` (Task 3), `EncodeQueryResponseEntry` (`include/XStore/XStore_WireEncode.h`, existing), `ProcessFileRangeSelect`-style real-scan decode (existing, used by `MinAggregate`'s fallback path — reuse its skip-parse column read, not a new decoder).
- Produces: `XStore::FindByValue(unsigned long long start_epoch_t, unsigned long long end_epoch_t, std::string_view db_name, int32_t column_idx, const XStore_Proto::FData &value, size_t chunk_size_bytes, const std::function<void(std::string&&, bool)> &emit_chunk)`.

This task deliberately does NOT yet consult a bitmap section (doesn't exist until Task 9/10) — every "maybe" from the bloom filter falls through to a real per-block scan. This is a genuinely complete, correct, testable deliverable on its own (bloom-accelerated equality lookup), matching the "each task ends with an independently testable deliverable" rule — the bitmap in Task 10 is a pure performance layer added on top without changing correctness.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
TEST_F(XStoreCatIdxTestFixture, FindByValueReturnsOnlyMatchingRows) {
    for (int64_t i = 0; i < 300; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 5); // column 0 cycles 0..4
        UnaryInsert(1000 + i, row, db_name_);
    }

    XStore_Proto::FData target;
    target.set_int64data(2);

    std::string reassembled;
    bool saw_last = false;
    db_.FindByValue(1000, 1000 + 299, db_name_, /*column_idx=*/0, target,
                     /*chunk_size_bytes=*/XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                     [&](std::string &&chunk, bool is_last) {
                         reassembled += chunk;
                         saw_last = is_last;
                     });
    EXPECT_TRUE(saw_last);

    XStore_Proto::RangeQuery_REP rep; // wire-compatible: FindByValue_REP's field 1 is the same UnaryQuery_REP shape
    // Implementer note: FindByValue's chunk stream produces the same bytes
    // RangeQuery's chunk stream does (EncodeQueryResponseEntry, field 1 =
    // repeated {timestamp,data}) -- reassemble with whatever the codebase's
    // existing RangeQuery-chunk-reassembly test helper does (see
    // XStoreTestSupport.h's RunStreamedAndReassemble, used by other
    // *Streamed* tests) rather than a bespoke parser here.
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 60); // 300 rows / 5 values = 60 matches for value=2
    for (const auto &entry : rep.queryresponse()) {
        EXPECT_EQ((entry.timestamp() - 1000) % 5, 2);
    }
}

TEST_F(XStoreCatIdxTestFixture, FindByValueColumnPastTrackedCountStillCorrectViaFallback) {
    // columnIdx >= trackedColumnCount: no bloom ever existed for it, must
    // still return correct results via unconditional real-scan fallback,
    // not an error.
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(42);
    UnaryInsert(2000, row, db_name_);

    XStore_Proto::FData target;
    target.set_int64data(42);

    std::string reassembled;
    db_.FindByValue(2000, 2000, db_name_, /*column_idx=*/999, target,
                     XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                     [&](std::string &&chunk, bool) { reassembled += chunk; });

    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 1);
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `FindByValue` not declared.

- [ ] **Step 3: Declare and implement**

In `include/XStore/XStore.h`, near `RangeQuery`'s declaration:

```cpp
void FindByValue(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                  std::string_view db_name, int32_t column_idx, const XStore_Proto::FData &value,
                  size_t chunk_size_bytes, const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk);
```

In `src/XStore/XStore_Aggregates.cpp`, add a partial-state struct in the anonymous namespace (next to `ExistsPartial`/`MinMaxPartial`):

```cpp
namespace {
struct FindByValuePartial {
    std::vector<std::pair<uint64_t, std::string>> matches; // (epoch_time, raw row bytes)
};
} // namespace
```

Add the per-block kernel (models `ScanSegmentForExistence`'s shape):

```cpp
static void ScanSegmentForValueMatch(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                      uint32_t tracked_column_count, const char *rows_base,
                                      unsigned long long rows_to_process, unsigned long long first_row_index,
                                      int32_t column_idx, const std::string &encoded_target,
                                      std::vector<std::pair<uint64_t, std::string>> &matches_out) {
    uint64_t rows_per_block = RowsPerBlock(row_length);
    CatIdxFile *catidx = nullptr;
    if (column_idx >= 0 && static_cast<uint32_t>(column_idx) < tracked_column_count) {
        catidx = &XStore::GetOrOpenCatIdx(db_base_path, year, row_length, tracked_column_count);
    }

    unsigned long long processed = 0;
    while (processed < rows_to_process) {
        uint64_t row_index = first_row_index + processed;
        uint64_t block_index = BlockIndexForRow(row_index, rows_per_block);
        uint64_t block_start_row = block_index * rows_per_block;
        uint64_t block_end_row = std::min(block_start_row + rows_per_block, first_row_index + rows_to_process);
        unsigned long long rows_in_this_block = block_end_row - row_index;

        bool must_scan = true;
        if (catidx != nullptr) {
            must_scan = catidx->MightContain(block_index, static_cast<uint32_t>(column_idx), encoded_target);
            // (Task 10 will insert a bitmap-exact-lookup branch here between
            // the bloom check and this real-scan fallback.)
        }

        if (must_scan) {
            for (unsigned long long r = 0; r < rows_in_this_block; ++r) {
                const char *row_ptr = rows_base + (processed + r) * row_length;
                if (column_idx < 0) continue;
                // Reuses the same wire-tag-walk skip-parse column extraction
                // MinAggregate's real-scan fallback uses (see
                // ExtractColumnByIndexSkipParse, XStore.h:464) -- decode just
                // this one column, not the whole row.
                XStore_Proto::FData f_data;
                if (XStore::ExtractColumnByIndexSkipParse(row_ptr, row_length, column_idx, &f_data)) {
                    if (EncodeFDataForCatIdx(f_data) == encoded_target) {
                        // epoch_time derivation: see this task's "Known open
                        // question" note above Step 3 -- resolve that first,
                        // then compute the real epoch_time for row_index + r
                        // here instead of this row_index placeholder.
                        matches_out.emplace_back(row_index + r, std::string(row_ptr, row_length));
                    }
                }
            }
        }
        processed += rows_in_this_block;
    }
}
```

**Known open question, resolve first thing in this task before writing the rest of Step 3:** this plan's research pass did not confirm how to reconstruct a real per-row `epoch_time` from `(year, row_index)` inside a `WalkYearSpanParallel` kernel. Every existing kernel that uses this traversal (`ScanSegmentForExistence`, `ScanSegmentForOhlc`, `MinAggregate`'s pruning kernel) returns a boolean or an aggregate scalar and never needed to reconstruct a timestamp, so none of them are a usable template for this specific step — don't copy their shape expecting an epoch answer. `RangeQuery` (`XStore.h:409-411`, `src/XStore/...`) DOES emit real per-row `epoch_time` values into its response, but it does so via its own hand-rolled per-year loop (not `WalkYearSpan`/`WalkYearSpanParallel`) where it tracks `iter_epoch` directly as the loop variable. Before writing `ScanSegmentForValueMatch`'s epoch arithmetic: read `RangeQuery`'s implementation to see exactly how it derives each row's `epoch_time`, then either (a) find/add a `TimeStruct` accessor that gives a given year's `first_epoch` independent of already having an epoch inside that year (check `TimeStruct`'s constructors for one keyed on `year` rather than `epoch`), letting `ScanSegmentForValueMatch` compute `epoch_time = year_first_epoch + row_index`, or (b) if no such accessor exists, thread `cfg_struct` (or just `time_resolution` + a per-segment starting epoch) through `WalkYearSpanParallel`'s kernel closure the same way `MinAggregate`'s lambda already captures `cfg_struct` by reference, and derive the year's first epoch from the segment's own start boundary instead. Do not proceed with the rest of Step 3's kernel body until one of these is confirmed working end-to-end against a real multi-row test, since every other part of this task depends on `epoch_time` being correct.

Implement `FindByValue`:

```cpp
void XStore::FindByValue(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                          std::string_view db_name_view, int32_t column_idx, const XStore_Proto::FData &value,
                          size_t chunk_size_bytes, const std::function<void(std::string&&, bool)> &emit_chunk) {
    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct) || start_epoch_t > end_epoch_t) {
        emit_chunk(std::string(), true);
        return;
    }

    std::string encoded_target = EncodeFDataForCatIdx(value);

    FindByValuePartial final = WalkYearSpanParallel<FindByValuePartial>(db_base_path_str, cfg_struct,
        start_epoch_t, end_epoch_t, FindByValuePartial{},
        [&](FindByValuePartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            ScanSegmentForValueMatch(db_base_path_str, year, static_cast<uint16_t>(cfg_struct.rowlength()),
                EffectiveTrackedColumnCount(cfg_struct), rows_base, rows_to_process, first_row_index,
                column_idx, encoded_target, partial.matches);
        },
        [](FindByValuePartial &final_state, FindByValuePartial &partial) {
            final_state.matches.insert(final_state.matches.end(),
                std::make_move_iterator(partial.matches.begin()), std::make_move_iterator(partial.matches.end()));
        });

    // Buffer-then-chunk, not incremental streaming during the parallel walk
    // -- deliberate: WalkYearSpanParallel's kernel/merge shape is built for
    // reduction, not safe concurrent side-effecting emission (RangeQuery's
    // own true incremental streaming instead uses its own dedicated
    // sequential walk, not WalkYearSpanParallel, for exactly this reason).
    // Correctness-safe simplification for equality lookups specifically,
    // since a selective predicate's match set is expected to be small
    // relative to a full RangeQuery -- this is what the design doc's
    // "Merge across blocks/segments/years: simple concatenation" already
    // anticipated.
    std::string buf;
    for (const auto &[epoch, row_bytes] : final.matches) {
        XStoreWireEncode::EncodeQueryResponseEntry(buf, epoch, row_bytes.data(), row_bytes.size());
        if (buf.size() >= chunk_size_bytes) {
            emit_chunk(std::move(buf), false);
            buf.clear();
        }
    }
    emit_chunk(std::move(buf), true);
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=*CatIdx*`
Expected: PASS. (Implementer: fix the epoch-time derivation flagged in Step 3's note before this will actually pass — it's the one deliberately-incomplete arithmetic in this task, called out explicitly rather than silently wrong.)

- [ ] **Step 5: Commit**

```bash
git add include/XStore/XStore.h src/XStore/XStore_Aggregates.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: add FindByValue equality-lookup query (bloom-accelerated, real-scan fallback)"
```

---

### Task 8: RPC dispatch + C++/Python clients + network round-trip

**Files:**
- Modify: `src/XStore_Networking/ZMQ_Server.cpp`
- Modify: `client_src/Cpp/XStore_Client.h`
- Modify: `client_src/Python/XStore.py`
- Modify: `tests/integration/XStoreZmqNetworkTest.cpp`

**Interfaces:**
- Consumes: `XStore::FindByValue` (Task 7), `XStore_Proto::FindByValue_REQ`/`FindByValue_REP` (Task 1).
- Produces: `XStoreClient::FindByValue(...)`, Python `XStore.findByValue(...)`.

- [ ] **Step 1: Add server dispatch case**

In `src/XStore_Networking/ZMQ_Server.cpp`, next to the `kRangequery` case:

```cpp
                // FIND BY VALUE (categorical equality lookup)
                case XStore_Proto::Request::kFindbyvalue: {
                    const auto &req = client_request->findbyvalue();
                    my_store.FindByValue(req.starttime(), req.endtime(), req.dbname(), req.columnidx(), req.value(),
                        XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                        [&](std::string &&chunk, bool is_last) {
                            socket.send(zmq::buffer(chunk),
                                        is_last ? zmq::send_flags::dontwait
                                                : (zmq::send_flags::dontwait | zmq::send_flags::sndmore));
                        });
                    break;
                }
```

- [ ] **Step 2: Add C++ client method**

In `client_src/Cpp/XStore_Client.h`, next to `RangeQuery`:

```cpp
  [[nodiscard]] XStore_Proto::FindByValue_REP FindByValue(uint64_t start_time, uint64_t end_time,
                                                            std::string_view db_name, int32_t column_idx,
                                                            const XStore_Proto::FData &value) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_findbyvalue();
      payload->set_rtype(XStore_Proto::FINDBYVALUE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      *payload->mutable_value() = value;
      return SendRecv<XStore_Proto::FindByValue_REP>(to_server);
  }
```

- [ ] **Step 3: Add Python client method**

In `client_src/Python/XStore.py`, next to `rangeQuery` (hand-rolled, not via `_aggregate`, since `value` is an extra `FData` field — same reasoning the research pass found for `percentile()`):

```python
    def findByValue(self, startUnixTime, endUnixTime, targetDBName, columnIdx, value):
        """
        :param value: an XStore_Proto.FData instance -- the exact value to match
        :return: FindByValue_REP (queryResponse: repeated UnaryQuery_REP, queryStatus)
        """
        envelope = XStore_Proto.Request()
        payload = envelope.findbyvalue
        payload.RType = XStore_Proto.FINDBYVALUE
        payload.startTime = startUnixTime
        payload.endTime = endUnixTime
        payload.dbName = targetDBName
        payload.columnIdx = columnIdx
        payload.value.CopyFrom(value)

        return self._sendRecv(envelope, XStore_Proto.FindByValue_REP)
```

- [ ] **Step 4: Write network round-trip test**

Add to `tests/integration/XStoreZmqNetworkTest.cpp`, following the file's existing pattern (e.g. `OhlcRoundTripOverNetwork`):

```cpp
TEST_F(XStoreZmqNetworkTestFixture, FindByValueRoundTripOverNetwork) {
    for (int64_t i = 0; i < 50; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 3);
        client_.UnaryInsert(1000 + i, row, db_name_);
    }

    XStore_Proto::FData target;
    target.set_int64data(1);
    XStore_Proto::FindByValue_REP rep = client_.FindByValue(1000, 1049, db_name_, 0, target);

    EXPECT_GT(rep.queryresponse_size(), 0);
    for (const auto &entry : rep.queryresponse()) {
        EXPECT_EQ((entry.timestamp() - 1000) % 3, 1);
    }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_network_tests --gtest_filter=*FindByValue*`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/XStore_Networking/ZMQ_Server.cpp client_src/Cpp/XStore_Client.h client_src/Python/XStore.py tests/integration/XStoreZmqNetworkTest.cpp
git commit -m "feat: wire FindByValue through ZMQ dispatch and both clients"
```

---

### Task 9: Compaction background thread (bitmap section, cold)

**Files:**
- Modify: `include/XStore/XStore_CatIdx.h` (bitmap directory structs, function declarations)
- Modify: `src/XStore/XStore_CatIdx.cpp` (compaction implementation)
- Modify: `include/XStore/XStore.h` (compaction thread lifecycle members, mirrors `WalShard`)
- Modify: `main.cpp` (start the compaction thread, same startup sequence as `StartWalWriters`)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `XStorePreadPool::ParallelFor` (existing), `CatIdxFile` (Task 4), `EncodeFDataForCatIdx` (Task 3).
- Produces: `CatIdxFile::AppendBitmapSection(uint32_t column_idx, const std::map<std::string, std::vector<uint64_t>> &value_to_rows)`, `CatIdxFile::HasBitmapSection()`, `XStore::CompactOneYearCatIdx(...)`, `XStore::StartCatIdxCompactionThread(...)`.

- [ ] **Step 1: Write the failing tests**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
TEST(CatIdxFileTest, AppendBitmapSectionThenExactLookupWorks) {
    std::string path = TempCatIdxPath("bitmap_exact.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    XStore_Proto::FData v137; v137.set_int64data(137);
    std::string enc137 = EncodeFDataForCatIdx(v137);
    f.AppendBitmapSection(/*column_idx=*/0, {{enc137, {5, 42, 99}}});

    ASSERT_TRUE(f.HasBitmapSection());
    auto rows = f.LookupExact(/*column_idx=*/0, enc137);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(*rows, (std::vector<uint64_t>{5, 42, 99}));

    XStore_Proto::FData v138; v138.set_int64data(138);
    EXPECT_FALSE(f.LookupExact(0, EncodeFDataForCatIdx(v138)).has_value());
}

TEST(CatIdxFileTest, ColumnOverCardinalityCapHasNoDirectory) {
    std::string path = TempCatIdxPath("bitmap_over_cap.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));
    // Column 0 never gets AppendBitmapSection called (compaction abandoned
    // it mid-scan for exceeding CATIDX_MAX_BITMAP_DISTINCT_VALUES) --
    // LookupExact must report "no directory" (nullopt), distinct from
    // "directory exists but value absent" (also nullopt) -- both mean
    // "caller must fall back to bloom+real-scan", so the same nullopt
    // return is correct for both; HasBitmapSection() still returns false
    // if literally nothing was ever appended for this year.
    EXPECT_FALSE(f.HasBitmapSection());
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: FAIL — `AppendBitmapSection`/`HasBitmapSection`/`LookupExact` not declared.

- [ ] **Step 3: Implement bitmap section on `CatIdxFile`**

Add to `include/XStore/XStore_CatIdx.h`, in the `CatIdxFile` class:

```cpp
    // Appends one column's value->row-indices directory to the bitmap
    // section (commit-safety: writes the directory bytes and fsyncs BEFORE
    // updating+fsyncing header_.bitmap_section_offset -- a crash between the
    // two leaves harmless unreferenced bytes past the last known-good
    // offset, same "uncommitted tail is safely ignorable" shape the WAL
    // itself relies on). Row indices per value need not be pre-sorted --
    // stored as given (compaction naturally produces them in scan order).
    void AppendBitmapSection(uint32_t column_idx, const std::map<std::string, std::vector<uint64_t>> &value_to_rows);

    bool HasBitmapSection() const { return header_.bitmap_section_offset != 0; }

    // Returns the exact row indices for (column_idx, encoded_value) if a
    // directory exists for that column AND the value is present in it.
    // nullopt covers BOTH "no directory for this column" (over cardinality
    // cap, or year not compacted yet) and "directory exists but value
    // absent" -- both mean the same thing to the caller: fall back to
    // bloom+real-scan.
    std::optional<std::vector<uint64_t>> LookupExact(uint32_t column_idx, const std::string &encoded_value);
```

Add to `src/XStore/XStore_CatIdx.cpp`:

```cpp
void CatIdxFile::AppendBitmapSection(uint32_t column_idx, const std::map<std::string, std::vector<uint64_t>> &value_to_rows) {
    std::lock_guard<std::mutex> lock(mutex_);

    struct stat st{};
    fstat(fd_, &st);
    off_t append_offset = st.st_size;

    // Directory layout: [columnIdx u32][entryCount u32]
    //   then per entry, sorted by encoded value bytes for binary search:
    //   [valueLen u32][valueBytes][rowCount u32][rowIndices: u64 * rowCount]
    std::string buf;
    buf.append(reinterpret_cast<const char*>(&column_idx), sizeof(column_idx));
    uint32_t entry_count = static_cast<uint32_t>(value_to_rows.size());
    buf.append(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

    for (const auto &[value, rows] : value_to_rows) { // std::map iterates sorted by key -- binary-searchable on disk
        uint32_t value_len = static_cast<uint32_t>(value.size());
        buf.append(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
        buf.append(value);
        uint32_t row_count = static_cast<uint32_t>(rows.size());
        buf.append(reinterpret_cast<const char*>(&row_count), sizeof(row_count));
        buf.append(reinterpret_cast<const char*>(rows.data()), rows.size() * sizeof(uint64_t));
    }

    pwrite(fd_, buf.data(), buf.size(), append_offset);
    fdatasync(fd_);

    // Commit point: only now does the header admit this section exists.
    if (header_.bitmap_section_offset == 0) {
        header_.bitmap_section_offset = static_cast<uint64_t>(append_offset);
    }
    pwrite(fd_, &header_, sizeof(header_), 0);
    fdatasync(fd_);
}

std::optional<std::vector<uint64_t>> CatIdxFile::LookupExact(uint32_t column_idx, const std::string &encoded_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (header_.bitmap_section_offset == 0) return std::nullopt;

    struct stat st{};
    fstat(fd_, &st);
    off_t pos = static_cast<off_t>(header_.bitmap_section_offset);

    while (pos < st.st_size) {
        uint32_t col; uint32_t entry_count;
        pread(fd_, &col, sizeof(col), pos); pos += sizeof(col);
        pread(fd_, &entry_count, sizeof(entry_count), pos); pos += sizeof(entry_count);

        for (uint32_t i = 0; i < entry_count; ++i) {
            uint32_t value_len;
            pread(fd_, &value_len, sizeof(value_len), pos); pos += sizeof(value_len);
            std::string value(value_len, '\0');
            pread(fd_, value.data(), value_len, pos); pos += value_len;
            uint32_t row_count;
            pread(fd_, &row_count, sizeof(row_count), pos); pos += sizeof(row_count);

            if (col == column_idx && value == encoded_value) {
                std::vector<uint64_t> rows(row_count);
                pread(fd_, rows.data(), row_count * sizeof(uint64_t), pos);
                return rows;
            }
            pos += row_count * sizeof(uint64_t);
        }
    }
    return std::nullopt;
}
```

(Implementer note: this linear per-column-directory scan is O(directories) per lookup, not true binary search across the whole bitmap section — acceptable for a first version since directory count per year is bounded by `tracked_column_count` (≤64), but flag this as a candidate follow-up if query latency profiling shows it matters; the spec's own directory-sort-for-binary-search intent applies WITHIN one column's entries, which this respects by writing them value-sorted, even though this reference implementation doesn't yet exploit that sort with a binary search — a real binary search over the on-disk sorted run is a drop-in optimization on top of this correct-but-linear baseline, not a correctness change.)

- [ ] **Step 4: Implement compaction (year scan + cardinality cap)**

In `include/XStore/XStore.h`, near `RebuildStaleZoneMapsForDb`:

```cpp
static void CompactOneYearCatIdx(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                  uint32_t tracked_column_count);
static void CompactColdCatIdxForAllDbs(const std::string &data_store_dir);
static void StartCatIdxCompactionThread(const std::string &data_store_dir);
```

In `src/XStore/XStore_CatIdx.cpp`:

```cpp
void XStore::CompactOneYearCatIdx(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                   uint32_t tracked_column_count) {
    std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if (!std::filesystem::exists(data_path)) return;

    CatIdxFile &catidx = GetOrOpenCatIdx(db_base_path, year, row_length, tracked_column_count);
    if (catidx.HasBitmapSection()) return; // already compacted, idempotent no-op

    struct stat st{};
    int fd = open(data_path.c_str(), O_RDONLY);
    if (fd < 0) return;
    fstat(fd, &st);
    uint64_t total_rows = static_cast<uint64_t>(st.st_size) / row_length;

    // {column_idx: {encoded_value: [row_index...]}} -- abandoned (erased)
    // for a column the moment its distinct-value count crosses the cap, so
    // an over-cap column never gets AppendBitmapSection called for it and
    // stays bloom+real-scan-only, per the cardinality safety valve.
    std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> accumulators;
    std::unordered_set<uint32_t> abandoned_columns;
    for (uint32_t c = 0; c < tracked_column_count; ++c) accumulators[c];

    std::vector<char> row_buf(row_length);
    for (uint64_t r = 0; r < total_rows; ++r) {
        pread(fd, row_buf.data(), row_length, static_cast<off_t>(r) * row_length);
        for (uint32_t c = 0; c < tracked_column_count; ++c) {
            if (abandoned_columns.count(c) > 0) continue;
            XStore_Proto::FData f_data;
            if (!XStore::ExtractColumnByIndexSkipParse(row_buf.data(), row_length, static_cast<int32_t>(c), &f_data)) continue;
            std::string enc = EncodeFDataForCatIdx(f_data);
            auto &bucket = accumulators[c][enc];
            bucket.push_back(r);
            if (accumulators[c].size() > XStoreTunables::CATIDX_MAX_BITMAP_DISTINCT_VALUES) {
                accumulators[c].clear(); // free immediately, don't keep building a disqualified column
                abandoned_columns.insert(c);
            }
        }
    }
    close(fd);

    for (auto &[col, value_map] : accumulators) {
        if (abandoned_columns.count(col) > 0 || value_map.empty()) continue;
        catidx.AppendBitmapSection(col, value_map);
    }
    if (catidx.HasBitmapSection() == false) {
        // Every column was abandoned or empty -- still mark this year
        // compacted (empty directory list) so it's not re-scanned every
        // restart. AppendBitmapSection with an empty map still commits the
        // header's bitmap_section_offset.
        catidx.AppendBitmapSection(0, {});
    }
}

void XStore::CompactColdCatIdxForAllDbs(const std::string &data_store_dir) {
    unsigned short current_year = TimeStruct(static_cast<unsigned long long>(std::time(nullptr)), SECOND).year;
    for (auto &p : std::filesystem::directory_iterator(data_store_dir)) {
        if (!p.is_directory()) continue;
        std::string db_base_path = p.path().string();
        std::string cfg_path = fmt::format("{}/config.cfg", db_base_path); // same config file name InitDB already reads
        if (!std::filesystem::exists(cfg_path)) continue;

        XStore_Proto::ConfigStruct cfg_struct;
        // (Implementer: reuse InitDB's existing config-read helper here
        // rather than re-deriving the read -- this plan's research pass
        // didn't capture its exact name; grep InitDB's body in
        // XStore_Management.cpp for how it loads ConfigStruct per db.)

        for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
            std::string filename = f.path().filename().string();
            if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;
            unsigned short year;
            try { year = static_cast<unsigned short>(std::stoi(filename.substr(0, filename.size() - 7))); }
            catch (...) { continue; }
            if (year >= current_year) continue; // only cold (closed) years

            CompactOneYearCatIdx(db_base_path, year, static_cast<uint16_t>(cfg_struct.rowlength()),
                                  EffectiveTrackedColumnCount(cfg_struct));
        }
    }
}
```

- [ ] **Step 5: Background thread lifecycle**

In `include/XStore/XStore.h`, near `WalShard`:

```cpp
static std::thread catidx_compaction_thread_;
static std::atomic<bool> catidx_compaction_stop_flag_;
```

In `src/XStore/XStore_CatIdx.cpp`:

```cpp
std::thread XStore::catidx_compaction_thread_;
std::atomic<bool> XStore::catidx_compaction_stop_flag_{false};

void XStore::StartCatIdxCompactionThread(const std::string &data_store_dir) {
    catidx_compaction_thread_ = std::thread([data_store_dir]() {
        while (!XStore::catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) {
            XStore::CompactColdCatIdxForAllDbs(data_store_dir);
            for (uint64_t waited = 0; waited < XStoreTunables::CATIDX_COMPACTION_POLL_INTERVAL_SEC &&
                 !XStore::catidx_compaction_stop_flag_.load(std::memory_order_relaxed); ++waited) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    });
    // Same atexit-at-spawn-time ordering argument as StartWalWriters: register
    // the join here so it runs before any static logger/queue teardown.
    std::atexit([]() {
        XStore::catidx_compaction_stop_flag_.store(true, std::memory_order_relaxed);
        if (XStore::catidx_compaction_thread_.joinable()) {
            XStore::catidx_compaction_thread_.join();
        }
    });
}
```

In `main.cpp`, call `XStore::StartCatIdxCompactionThread(data_store_dir)` right after the existing `StartWalWriters(data_store_dir)` call.

- [ ] **Step 6: Run tests**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=*CatIdx*`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add include/XStore/XStore_CatIdx.h src/XStore/XStore_CatIdx.cpp include/XStore/XStore.h main.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: add CatIdx compaction thread building cardinality-capped bitmap sections"
```

---

### Task 10: Bitmap-aware query path

**Files:**
- Modify: `src/XStore/XStore_Aggregates.cpp` (`ScanSegmentForValueMatch`)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `CatIdxFile::LookupExact`/`HasBitmapSection` (Task 9).

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
TEST_F(XStoreCatIdxTestFixture, FindByValueUsesBitmapAfterCompaction) {
    for (int64_t i = 0; i < 100; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 4);
        UnaryInsert(1000 + i, row, db_name_);
    }
    // Force this year "cold" for the test by calling compaction directly
    // rather than waiting for the background thread's hourly poll.
    XStore::CompactOneYearCatIdx(db_base_path_, TimeStruct(1000, SECOND).year, 64, 1);

    XStore_Proto::FData target;
    target.set_int64data(3);
    std::string reassembled;
    db_.FindByValue(1000, 1099, db_name_, 0, target, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                     [&](std::string &&chunk, bool) { reassembled += chunk; });

    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 25); // 100/4
}
```

- [ ] **Step 2: Run to verify it already passes (correctness), then verify the bitmap path is actually exercised**

Run: `./build/tests/xstore_tests --gtest_filter=*BitmapAfterCompaction*`
Expected: PASS even before Step 3's change, since Task 7's real-scan fallback is already correct — this task is a pure performance addition, not a correctness fix. To confirm the bitmap path is actually being taken (not silently still falling back), temporarily add a `spdlog::debug` log in the bitmap-lookup branch added in Step 3 and grep the test's log output, or (preferred) add a call-counter test hook — implementer's choice, not prescribed here since it's a verification-only concern, not a behavior change.

- [ ] **Step 3: Extend `ScanSegmentForValueMatch` to consult the bitmap**

In `src/XStore/XStore_Aggregates.cpp`, inside `ScanSegmentForValueMatch`, replace the `must_scan` block:

```cpp
        bool must_scan = true;
        if (catidx != nullptr) {
            bool maybe = catidx->MightContain(block_index, static_cast<uint32_t>(column_idx), encoded_target);
            if (!maybe) {
                must_scan = false; // definite absence -- skip block entirely
            } else if (catidx->HasBitmapSection()) {
                auto exact_rows = catidx->LookupExact(static_cast<uint32_t>(column_idx), encoded_target);
                if (exact_rows.has_value()) {
                    // Exact answer -- fetch only these rows, no block scan.
                    int fd = open(fmt::format("{}/{}.XSTORE", db_base_path, year).c_str(), O_RDONLY);
                    if (fd >= 0) {
                        std::vector<char> row_buf(row_length);
                        for (uint64_t row_index : *exact_rows) {
                            if (row_index < block_start_row || row_index >= block_end_row) continue; // only this segment's slice
                            pread(fd, row_buf.data(), row_length, static_cast<off_t>(row_index) * row_length);
                            matches_out.emplace_back(row_index, std::string(row_buf.data(), row_length));
                        }
                        close(fd);
                    }
                    must_scan = false;
                }
                // exact_rows == nullopt with HasBitmapSection() true and
                // maybe == true: bloom said maybe but this column's
                // directory doesn't have this value (either the column was
                // over the cardinality cap, or -- rare -- a bloom false
                // positive). Either way, must_scan stays true: real-scan
                // this block, which is always correct regardless of why.
            }
        }
```

- [ ] **Step 4: Run tests**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=*CatIdx*`
Expected: PASS.

- [ ] **Step 5: Full suite regression check**

Run: `./build/tests/xstore_tests`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add src/XStore/XStore_Aggregates.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: consult CatIdx bitmap section for exact matches when available"
```

---

### Task 11: Schema-growth rebuild integration

**Files:**
- Modify: `src/XStore/XStore_Management.cpp` (`InitDB`'s rebuild call site)
- Modify: `include/XStore/XStore.h` / `src/XStore/XStore_CatIdx.cpp` (`RebuildStaleCatIdxForDb`)
- Modify: `tests/integration/XStoreCatIdxTest.cpp`

**Interfaces:**
- Consumes: `EffectiveTrackedColumnCount` (existing), `CatIdxFile` (Task 4).
- Produces: `XStore::RebuildStaleCatIdxForDb(...)`.

- [ ] **Step 1: Write the failing test**

Add to `tests/integration/XStoreCatIdxTest.cpp`:

```cpp
TEST_F(XStoreCatIdxTestFixture, TrackedColumnCountGrowthTriggersCatIdxRebuildOnInitDb) {
    // Insert with a narrow tracked-column-count first (creates a .catidx
    // with a small tracked_column_count), then simulate growth the same way
    // MaybeGrowColumnCount does, then re-run InitDB and confirm the
    // .catidx's on-disk tracked_column_count converges to the new width --
    // mirrors the zone-map columnCount-growth regression test.
    XStore_Proto::RowData narrow_row;
    narrow_row.add_fielddata()->set_int64data(1);
    UnaryInsert(1000, narrow_row, db_name_);

    XStore_Proto::RowData wide_row;
    for (int i = 0; i < 20; ++i) wide_row.add_fielddata()->set_int64data(i);
    UnaryInsert(1001, wide_row, db_name_); // triggers MaybeGrowColumnCount

    XStore::ClearCatIdxRegistryForTests();
    // Re-run InitDB (implementer: use whatever test helper already
    // re-triggers InitDB for a single test DB directory -- same one the
    // zone-map growth-convergence test already uses).
    store_.InitDB();

    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(db_base_path_, TimeStruct(1000, SECOND).year, 64, 1);
    // Column 19 must now be tracked (bloom entry exists / doesn't throw) --
    // implementer: assert via whatever CatIdxFile exposes for "is this
    // column within tracked_column_count" (e.g. MightContain returning a
    // real answer rather than the >= tracked_column_count "true" escape
    // hatch) once Task 4's header exposes tracked_column_count for tests.
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -20`
Expected: compiles (this test doesn't call anything new yet) but the assertion logic needs `RebuildStaleCatIdxForDb` wired in to actually pass — implementer fills in the exact assertion once the rebuild call site exists.

- [ ] **Step 3: Implement `RebuildStaleCatIdxForDb` and wire it into `InitDB`**

In `include/XStore/XStore.h`:

```cpp
static void RebuildStaleCatIdxForDb(const std::string &db_name, const std::string &db_base_path,
                                     const XStore_Proto::ConfigStruct &cfg_struct);
```

In `src/XStore/XStore_CatIdx.cpp` (mirrors `RebuildStaleZoneMapsForDb`, but "rebuild" here means "discard and let the next insert/compaction repopulate" rather than a synchronous full-data rescan — the bloom section is cheap to rebuild lazily via normal inserts, and the bitmap section already has its own idempotent compaction path from Task 9, so this function's job is narrower than zone-map's: just detect staleness and delete the stale `.catidx` so `GetOrOpenCatIdx`'s next call creates a fresh one at the new width):

```cpp
void XStore::RebuildStaleCatIdxForDb(const std::string &db_name, const std::string &db_base_path,
                                      const XStore_Proto::ConfigStruct &cfg_struct) {
    uint32_t desired_tracked_columns = EffectiveTrackedColumnCount(cfg_struct);

    for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
        std::string filename = f.path().filename().string();
        if (filename.size() < 8 || filename.substr(filename.size() - 8) != ".catidx") continue; // Note: 7 chars ".catidx" -- fix suffix length to match exactly
        std::string year_str = filename.substr(0, filename.size() - 7);
        unsigned short year;
        try { year = static_cast<unsigned short>(std::stoi(year_str)); } catch (...) { continue; }

        std::string path = f.path().string();
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) continue;
        CatIdxFileHeader on_disk{};
        bool readable = pread(fd, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk));
        close(fd);

        bool stale = !readable || on_disk.magic != CatIdxFileHeader{}.magic ||
                     on_disk.version != XStoreTunables::CATIDX_VERSION ||
                     on_disk.tracked_column_count < desired_tracked_columns;
        if (stale) {
            spdlog::info("[CATIDX REBUILD] [DB: {}] - discarding stale {} (tracked_column_count {} < desired {})",
                         db_name, filename, on_disk.tracked_column_count, desired_tracked_columns);
            std::filesystem::remove(path);
            // Next GetOrOpenCatIdx call for this (db,year) creates a fresh
            // file at desired_tracked_columns -- bloom repopulates lazily
            // via ordinary inserts (write-through means it's always
            // eventually consistent with real data, unlike zone-map's
            // min/max/sum which need a synchronous rescan to recover
            // historical aggregate state -- CatIdx's bloom has no such
            // "historical state that can't be recovered incrementally"
            // property, so a lazy rebuild is correct here, not a shortcut).
            // The bitmap section will be rebuilt the next time
            // CompactColdCatIdxForAllDbs runs for this now-missing file.
        }
    }
}
```

Fix the off-by-one in the suffix-strip shown above (`.catidx` is 7 characters, not 8 — implementer: use the same `filename.size() - 7`/`filename.substr(filename.size() - 7) == ".catidx"` pattern `RebuildStaleZoneMapsForDb` uses for `.XSTORE`/`.zmap`, adjusted for `.catidx`'s length).

In `src/XStore/XStore_Management.cpp`, at `InitDB()`'s existing rebuild call site (around line 639-641), add a sibling call:

```cpp
    RebuildStaleZoneMapsForDb(curr_db_name, p.path().string(), cfg_struct,
                               replayed_it == replayed_years.end() ? kNoForcedYears : replayed_it->second);
    RebuildStaleCatIdxForDb(curr_db_name, p.path().string(), cfg_struct);
```

- [ ] **Step 4: Run tests**

Run: `cmake --build build -j$(nproc) && ./build/tests/xstore_tests --gtest_filter=*CatIdx*`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add include/XStore/XStore.h src/XStore/XStore_CatIdx.cpp src/XStore/XStore_Management.cpp tests/integration/XStoreCatIdxTest.cpp
git commit -m "feat: rebuild stale .catidx files on InitDB when tracked column count grows"
```

---

### Task 12: Docs + TODO.md + full suite verification

**Files:**
- Modify: `docs/XStore_Technical_Reference.md` (new section, mirrors §4.14's zone-map writeup)
- Modify: `TODO.md` (mark the "Categorical/high-cardinality acceleration" line item done, per this repo's existing convention of documenting deviations/verification inline)

**Interfaces:** none (docs-only task).

- [ ] **Step 1: Full suite run**

Run: `cmake --build build --target clean && cmake --build build -j$(nproc) && cd build && ctest --output-on-failure`
Expected: 0 failures, including every new `*CatIdx*` test and `FindByValueRoundTripOverNetwork`.

- [ ] **Step 2: Write the technical reference section**

Add a new subsection to `docs/XStore_Technical_Reference.md` (find §4.14, add a §4.15 or nested subsection) covering: `.catidx` file layout (reuse the spec's own diagram), bloom hash construction (Crc32c + FNV-1a double-hashing), compaction trigger and cardinality cap, and the query-path tiering — same level of detail §4.14 gives zone-map.

- [ ] **Step 3: Update TODO.md**

In `TODO.md`, find the "Categorical/high-cardinality acceleration" line under the zone-map follow-on section (around line 134) and mark it done, following this file's own established convention (see the OHLC/EXISTS/variance-stddev entries directly above it for the expected level of detail: what shipped, deviations from the spec/plan found during implementation, what was verified, commit hashes). List explicitly what's still deferred (range-predicate row-retrieval RPC, unknown-column search, `!=` acceleration — per the spec's own "Deferred follow-ons" section) so a future reader doesn't have to re-derive that scoping decision.

- [ ] **Step 4: Commit**

```bash
git add docs/XStore_Technical_Reference.md TODO.md
git commit -m "docs: document categorical/high-cardinality acceleration (.catidx, FindByValue)"
```

**Mystic benchmark gate:** deferred, same situation `SUM`/`AVG`/wide-span aggregates were in before their own gates — no existing workload tag exercises equality-lookup queries. Note this in the TODO.md entry as follow-up work, not part of this plan's completion criteria.
