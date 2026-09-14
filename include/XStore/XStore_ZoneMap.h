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
