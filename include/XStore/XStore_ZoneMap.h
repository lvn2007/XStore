#pragma once
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include "include/XStore/XStore_Tunables.h"

// Zone-map secondary index: per-block min/max/sum/count summary statistics
// for numeric columns, used to prune blocks a range/aggregate query cannot
// possibly need. See docs/XStore_Technical_Reference.md section 4.14 for the
// full design and its rejected alternatives.

inline uint64_t RowsPerBlock(uint16_t row_length) {
    if (row_length == 0) return 1;
    uint64_t rows = XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES / row_length;
    return rows < 1 ? 1 : rows;
}

inline uint64_t BlockIndexForRow(uint64_t row_index, uint64_t rows_per_block) {
    return row_index / rows_per_block;
}

// One (block, column) summary. min_value/max_value/sum_sq_value/first_value/
// last_value are only meaningful when valid_count > 0 -- a never-observed
// column in a block has valid_count==0 and these left at their sentinel (see
// ZoneMapFile::ZeroedEntry). Persisted as raw bytes (pread/pwrite), never
// protobuf -- this file never crosses the wire.
struct ZoneMapEntry {
    double   min_value;
    double   max_value;
    double   sum_value;
    double   sum_sq_value;    // sum of squares, for VARIANCE/STDDEV pushdown -- see 2026-09-15 design doc
    double   first_value;     // chronologically-FIRST numeric value in this block, for OHLC's Open -- see 2026-09-19 design doc
    double   last_value;      // chronologically-LAST numeric value in this block, for OHLC's Close
    uint64_t valid_count;     // rows in this block where the column was numeric and set
    uint64_t null_count;      // rows in this block where the column was a hole or non-numeric
    uint64_t first_row_index; // row_index first_value was folded from -- lets an out-of-order (backfill) insert correctly tell whether it's earlier than what's already recorded, not just first-observed-wins
    uint64_t last_row_index;  // row_index last_value was folded from -- same reasoning, for "later than what's recorded"
};
static_assert(std::is_trivially_copyable<ZoneMapEntry>::value,
              "ZoneMapEntry must be trivially copyable for raw pread/pwrite");
static_assert(sizeof(ZoneMapEntry) == 80, "update size-dependent capacity-planning docs (docs/XStore_Technical_Reference.md §4.14) if this changes");

// Bumped 2 -> 3 when ZoneMapEntry grew first_value/last_value/first_row_index/
// last_row_index (48 -> 80 bytes) for OHLC's Open/Close pushdown. A file
// written under an older version is never migrated in place -- ZoneMapFile::
// Open() truncates and reinitializes it instead (see Open's comment). Named
// out so a future format change is a one-line bump + rebuild, not a magic-
// number hunt across the codebase.
inline constexpr uint32_t ZONE_MAP_VERSION = 4;

struct ZoneMapFileHeader {
    uint32_t magic   = 0x5A4D4150; // 'ZMAP'
    uint32_t version = ZONE_MAP_VERSION;
    uint32_t tracked_column_count = 0;
    // 1 = may hold writes not yet synced alongside the data file (set before
    // the first write of a session, cleared only after data + index are
    // fsynced); 1 at startup => rebuild. See Durability A plan.
    uint32_t dirty = 0;
    uint64_t rows_per_block = 0;
};
static_assert(sizeof(ZoneMapFileHeader) == 24);
static_assert(offsetof(ZoneMapFileHeader, dirty) == 12);

#include <mutex>
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
    // tracked_column_count is used on first creation, when reinitializing a
    // stale-version file, AND (Task 6) when it's strictly greater than what
    // an already-current-version file has on disk -- that last case is what
    // lets a rebuild actually widen a zone map whose DB's columnCount grew
    // after the file was first created, instead of leaving it permanently
    // narrow and making ZoneMapNeedsRebuild re-flag it as stale forever.
    // Strictly-greater-only means this can never shrink an existing file:
    // defaulted to the global ZONE_MAP_MAX_TRACKED_COLUMNS so every
    // pre-existing call site (live insert/query paths, which don't yet
    // thread a per-DB columnCount this far) keeps its old behavior
    // unchanged; XStore::RebuildOneYearZoneMap (Task 5) is the one caller
    // that passes the real per-DB EffectiveTrackedColumnCount value, so a
    // rebuilt header converges on that count instead of being reset back to
    // the global default and re-triggering ZoneMapNeedsRebuild on every
    // subsequent InitDB() run.
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length, uint32_t tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);

    // Folds each observation into the block containing row_index and
    // pwrite()s that block back to disk before returning -- write-through,
    // no buffered state a crash can lose (see the Task 2 design note in the
    // plan for why this isn't a write-back cache). The in-memory cache below
    // only saves a repeat pread() when consecutive calls hit the same block.
    // pad_to is REQUIRED (no default) so every caller is found at compile
    // time (Task 2: observations now only cover present columns, so this
    // tells UpdateRow how far -- e.g. the row's tracked_column_count -- to
    // bump null_count for the columns past observations.size(), clamped to
    // header_.tracked_column_count). Pass observations.size() itself for a
    // caller that built its own already-complete observation vector and
    // wants no additional padding.
    void UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to);

    // Always returns real, safe stats for block_index -- a never-touched
    // block simply has valid_count==0 everywhere, which is the true and
    // correct answer for "no rows landed here yet," not a placeholder for
    // "unknown." nullopt is reserved for an actual I/O failure on the read.
    std::optional<std::vector<ZoneMapEntry>> TryGetBlockStats(uint64_t block_index);

    ~ZoneMapFile();

    uint64_t RowsPerBlockValue() const { return header_.rows_per_block; }

    [[nodiscard]] uint32_t TrackedColumnCount() const { return header_.tracked_column_count; }

    // Clears the on-disk dirty flag: fdatasync(index content), then header
    // dirty=0 + fdatasync. Returns false on any failure, leaving the file
    // marked dirty. The caller must have fdatasync'd the .XSTORE data file
    // FIRST -- this only makes the index itself durable.
    [[nodiscard]] bool MarkClean();

    // Marks the index dirty on disk now. A rebuild calls this before it touches
    // anything, so an early exit that folded no row cannot leave an empty index
    // marked clean. A failure is logged; the next UpdateRow retries.
    void MarkDirty();

    // Number of successful header dirty-mark writes (test-visible).
    [[nodiscard]] uint64_t DirtyMarkWritesForTests() const;

private:
    // Every method that writes index content calls this first (mutex_ held):
    // one predictable branch once the flag is on disk.
    void EnsureDirtyOnDisk() { if (!dirty_on_disk_) MarkDirtyOnDisk(); }
    void MarkDirtyOnDisk();

    void EnsureBlockLoaded(uint64_t block_index);
    void EvictIfOverCapacity();
    off_t BlockFileOffset(uint64_t block_index) const;
    // Resets header_ to a fresh, empty v2 header and writes it to offset 0
    // -- shared by first-time file creation and by Open()'s stale-version
    // discard path, so there's exactly one place that constructs a valid
    // fresh header.
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);

    int fd_ = -1;
    ZoneMapFileHeader header_;
    bool dirty_on_disk_ = false; // header_.dirty == 1 is durable on disk
    uint64_t dirty_mark_writes_ = 0;
    std::unordered_map<uint64_t, std::vector<ZoneMapEntry>> cache_; // pure read-cache, always disk-coherent
    std::vector<uint64_t> lru_order_; // most-recently-touched at the back
    // GetOrOpenZoneMap hands out a ZoneMapFile& to one shared, process-wide
    // instance per (db,year) -- every server worker thread touching the
    // same DB/year concurrently calls UpdateRow/TryGetBlockStats on this
    // SAME object. Guards cache_/lru_order_/fd_ against exactly that.
    mutable std::mutex mutex_;
};
