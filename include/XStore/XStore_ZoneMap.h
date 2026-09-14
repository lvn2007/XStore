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
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length);

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
