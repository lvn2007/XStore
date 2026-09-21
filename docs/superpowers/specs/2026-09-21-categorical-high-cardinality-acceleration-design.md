# Categorical / High-Cardinality Column Acceleration — Design

Status: draft, pending spec review
Date: 2026-09-21
Origin: zone-map follow-on work (`TODO.md`, "Categorical/high-cardinality acceleration" line item), surfaced during a Q&A walkthrough of the zone-map follow-on backlog.

## Problem

XStore has no query capability for row-level equality lookup on a non-timestamp column (`WHERE column = value`) at all today. Every existing op (`UnaryQuery`, `BatchQuery`, `RangeQuery`, and all aggregates: Min/Max/Sum/Avg/Variance/Stddev/Exists/Ohlc/Percentile/DistinctCount/TopK) filters only by time range + column index, never by column value.

The existing zone-map (`.zmap`, min/max/sum/etc. per block) cannot accelerate equality lookups on unsorted, high-cardinality columns: a block's `min`/`max` range almost always contains the queried value even when the value is absent, since XStore's row order is timestamp-based, not value-based. This gives zero pruning power for equality predicates.

## Scope

**In scope:** exact equality lookup (`WHERE columnIdx = value`) on a single, caller-specified column, time-bounded, returning full matching row data. All `FData` oneof types supported (int64, double, bool, bytes, string) — double equality is exact/bitwise, not epsilon-tolerant. Acceleration applied automatically to every tracked column (no per-column opt-in/schema declaration required).

**Explicitly out of scope, deferred separately:**
- **"Unknown column" / any-column search** (`WHERE <any column> = value`) — multiplies index lookups by tracked-column count (up to 64), forces per-candidate-row decode of every column since the matching column isn't known in advance, and degenerates on continuous columns (no declared column types exist in XStore to distinguish categorical from continuous ahead of time). Structurally a different, harder feature — closer to the deferred `LIKE`/full-text item than to this one. Not designed here.
- **Range predicates** (`>=`, `<=`, `>`, `<`) — already addressable via the *existing, shipped* zone-map `min`/`max` fields using the same block-skip mechanism `MinAggregate`/`MaxAggregate` pushdown already uses. A row-retrieval RPC (returns matching rows, not an aggregate scalar) that prunes via existing `.zmap` then real-scan-filters survivors is a smaller, separate, already-half-built follow-on — no new file format needed. Not designed here.
- **Not-equal (`!=`)** — neither bloom filter nor bitmap meaningfully accelerates this (bloom answers presence, not absence; bitmap's "everything minus one bucket" rarely prunes anything and only applies to the low-cardinality columns that get a bitmap at all). Stays a real-scan. Not designed here.

## RPC

New op, `RequestType.FINDBYVALUE = 25` (next free slot after `TOPK=24`), same shape as existing ops (`Exists`, `Ohlc`):

```protobuf
message FindByValue_REQ {
  RequestType RType = 1;
  int64 startTime = 2;
  int64 endTime = 3;
  string dbName = 4;
  int32 columnIdx = 5;
  FData value = 6;
}

message FindByValue_REP {
  OpStatus opStatus = 1;
  repeated RowData rows = 2;   // streamed via existing 64MB chunk framing
}
```

Time-bounded per the existing RPC pattern (`WalkYearSpanParallel` segment enumeration reused, not a new year-scan). Full row data reused via the same streaming chunk machinery `BatchQuery_REP`/`RangeQuery_REP` already use.

## Storage: `.catidx` (new sidecar file, per (db, year))

A separate file from `.zmap`, not an extension of it. Rationale (full comparison in the design discussion, summarized):

- Every existing zone-map-only query (Min/Max/Sum/etc.) stays untouched — never opens `.catidx`. Extending `.zmap` in place would grow every entry from 80B to ~230B (2.9x), regressing the cache footprint of already-shipped, Mystic-perf-gated queries that never use categorical lookup.
- Independent version field. `ZONE_MAP_VERSION` has already bumped 3 times in 4 days during this project's own history (v1→v2 variance/stddev, v2→v3 OHLC). Sharing a version field would force every future bloom-only or aggregate-only format change to trigger a full rebuild of both concerns.
- Preserves `.zmap`'s explicit fixed-width-POD design invariant instead of turning it into a mixed fixed+variable-trailer layout.
- Isolates corruption blast radius between the two files.
- Cost: one extra fd/mmap/registry slot per (db, year) — small, fixed.

### Layout

```
Header (fixed):
  magic               u32   0x43415449 ("CATI")
  version             u32   1
  trackedColumnCount  u32   (mirrors ConfigStruct.columncount, same
                             per-DB high-water-mark convergence zone-map uses)
  blockCount          u32
  bitmapSectionOffset u64   (0 = not yet compacted / hot year)

BloomSection (fixed-width, write-through on insert):
  entry[blockIdx * trackedColumnCount + colIdx] = CATIDX_BLOOM_BYTES_PER_ENTRY bytes
  sized for RowsPerBlock=128, 1% false-positive rate → ~154 bytes/entry,
  rounded to 160B (8-byte aligned). k=7 hash functions via Kirsch-Mitzenmacher
  double-hashing (g_i(x) = h1(x) + i*h2(x)): h1 = the existing hardware-accelerated
  CRC32C (include/XStore/XStore_Crc32c.h, already in the codebase for WAL framing),
  h2 = FNV-1a (new, trivial, no dependency) — no new external hash library.

BitmapSection (append-only, built at compaction, absent until then):
  per tracked column that stayed under the cardinality cap:
    ColumnDirectory {
      columnIdx, distinctValueCount,
      entries[]: { value bytes (8B fixed for numeric/bool, length-prefixed
                   for bytes/string), bitmapOffset u64, bitmapLength u32 }
    }
    sorted by value bytes → binary search, exact match, no confirm-scan needed
  columns over CATIDX_MAX_BITMAP_DISTINCT_VALUES: no directory entry —
  bloom+real-scan is the permanent fallback for that column in that year.
```

Row identity in bitmaps: the same `uint64_t` row-within-year numbering OHLC's `first_row_index`/`last_row_index` already introduced — no new row-addressing scheme.

Registry/lifecycle: new `CatIdxFile` class, mirrors `ZoneMapFile` exactly (per-(db,year) static registry, own mutex, mmap'd, lazy-opened).

Sizing: bloom-only hot year ≈ 2x `.zmap`'s footprint (160B vs 80B/entry) ≈ ~0.24% of data file size (extrapolated from existing §4.14 zone-map numbers). Bitmap section cost is proportional to actual distinct-value × match-count for columns that stay under the cap, not fixed.

## Write path (bloom, hot)

Hooked at `ApplyWalRecord` (`XStore_Wal.cpp`) — the same choke point zone-map uses, for the same reason (Task 4/5's documented deviation): a row only touches the bloom filter once actually durable, avoiding the ASYNC-mode phantom-value risk zone-map had to avoid.

New `BuildCatIdxObservations` walks the same rows `BuildZoneMapObservations` already walks (single pass, not two). For each tracked column with a real (non-hole) value, hashes it into that `(block, column)`'s bloom entry — fixed-cost bit-OR into an already-open mmap region. `CatIdxFile` owns its own mutex guarding the bloom write (same shape, same concurrency precedent as `ZoneMapFile`'s `ConcurrentUpdateRowFromMultipleThreadsDoesNotLoseUpdates` fix).

## Compaction (bitmap, cold)

Background thread, sibling to the WAL writer/fsync-coalescer threads, triggered by the same hot→cold boundary (year ≠ current calendar year) already established for the deferred cold-storage-compression item — reuses that detector rather than building a second one.

Per (db, year) once cold: one pass over raw rows (`XStorePreadPool::ParallelFor`, same fan-out `RebuildStaleZoneMapsForDb` already uses) accumulating `{column: {value: bitmap}}` in memory. Cardinality cap enforced mid-scan: the moment a column's distinct-value count crosses `CATIDX_MAX_BITMAP_DISTINCT_VALUES`, abandon and free that column's accumulator immediately.

**Commit safety:** bitmap section is written to its append region and `fsync`'d *before* the header's `bitmapSectionOffset` is updated and `fsync`'d — the header write is the commit point. A crash between the two leaves harmless unreferenced bytes past the last known-good offset (same "uncommitted tail is safely ignorable" shape the WAL already relies on). No atomic-rename-whole-file, since the bloom section is live/hot and can't be paused for that.

**Idempotency:** `bitmapSectionOffset == 0` means "not compacted" — safe to detect on restart. A year where every column exceeds the cardinality cap is still marked compacted (empty bitmap section) so it isn't re-attempted every restart.

**Schema growth:** if `trackedColumnCount` grows after compaction already ran (same scenario `MaybeGrowColumnCount` created for zone-map), `CatIdxFile`'s staleness check (magic/version/trackedColumnCount mismatch) triggers discard+rebuild, wired into the same `InitDB`-integrated rebuild call site zone-map already uses.

## Query engine

New `XStore::ScanSegmentForValueMatch(year, block, colIdx, value)` kernel, called from `WalkYearSpanParallel`'s existing per-segment dispatch — no new traversal/threading logic. Per block:

```
1. bloom.MightContain(colIdx, value)?
   no    → skip block entirely (zero I/O beyond resident bloom bytes)
   maybe → 2
2. catidx has bitmap section && colIdx has a directory (under cardinality cap)?
   yes → binary-search directory for value → exact row indices → pread just those rows
   no  → real-scan this block only (reuse ProcessFileRangeSelect's skip-parse decode)
3. append matches to response, stream via existing 64MB chunk framing
```

Merge across blocks/segments/years: simple concatenation (no ordering requirement stronger than `RangeQuery`'s existing streamed response already provides).

## Error handling

- Missing DB / `start > end`: same rejection pattern every other RPC uses.
- `columnIdx >= trackedColumnCount`: no bloom/bitmap ever existed for it — falls back to real-scan-every-block for the queried range. Not an error (same "absence of optimization isn't a failure" precedent `EXISTS`'s partial-coverage fallback set).
- Empty result (no matches): `SUCCESS` with zero rows, same precedent as `DistinctCount`/`TopK`'s empty-range handling.

## Testing

New `tests/integration/XStoreCatIdxTest.cpp`, same shape as `XStoreZoneMapTest.cpp`:
- Bloom write-through + false-positive-safe (never false-negative) lookup.
- Bitmap exact-match correctness.
- Cardinality-cap abandonment (column crossing the threshold mid-compaction falls back correctly, never returns a partial/wrong bitmap).
- Compaction idempotency (re-run after interrupted commit doesn't duplicate/corrupt).
- Concurrent insert into same block (mirrors `ConcurrentUpdateRowFromMultipleThreadsDoesNotLoseUpdates`).
- Schema-growth rebuild convergence (mirrors the zone-map columnCount-growth regression test).
- All `FData` types (int64/double/bool/bytes/string) round-tripping through equality match.

One `XStoreZmqNetworkTest.cpp` round-trip test (`FindByValueRoundTripOverNetwork`).

Mystic benchmark gate deferred to the implementation plan itself — no existing workload tag exercises this shape yet (same situation `SUM`/`AVG`/wide-span aggregates were in before their own gates).

## Deferred follow-ons (not this spec)

- Range-predicate row-retrieval RPC (reuses existing `.zmap` min/max, no new file format) — smaller, separate.
- "Unknown column" / any-column value search — own future design cycle, closer in shape to the deferred `LIKE`/full-text item.
- `!=` acceleration — not meaningfully solvable by either structure; not pursued.
