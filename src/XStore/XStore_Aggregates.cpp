#include <limits>
#include "include/XStore/XStore_ReplyLimit.h"
#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_ChunkPool.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/XStore/XStore_ExactAggregates.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_ScanLock.h"
#include "include/XStore/XStore_ZoneMap.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <atomic>
#include <cmath>
#include <filesystem>
#include <functional>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_set>

// Shared by Min/Max/Sum/AvgAggregate: walks [startEpochT, endEpochT] one
// year-file at a time, read-locking each existing segment and handing it to
// kernel(rowsBase, rowsToProcess) -- the four aggregates differ only in which
// ProcessFileRange* kernel runs and what it accumulates into. A missing year
// contributes nothing (kernel just isn't called for it), same as Range
// Query/Delete's handling of missing years.
//
// Goes through GetOrMapFile -- the same MRU-cached, sharded mapping every
// other read path (Range Query, Unary/Batch/RangeInsert) already reuses --
// instead of a fresh fopen()+mmap()+munmap() per call like this used to do.
template <typename Kernel>
void XStore::WalkYearSpan(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                           unsigned long long start_epoch_t, unsigned long long end_epoch_t, Kernel &&kernel) {
    unsigned long long iter_epoch = start_epoch_t;

    while (iter_epoch <= end_epoch_t) {
        TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
        const std::string &target_file_path = CachedYearFilePath(db_base_path, curr_ts.year);

        unsigned long long end_of_current_file = std::min(end_epoch_t, static_cast<unsigned long long>(curr_ts.last_epoch));
        unsigned long long rows_to_process = (end_of_current_file - iter_epoch) + 1;

        if (std::filesystem::exists(target_file_path)) {
            // Held through the kernel() call below so a concurrent eviction
            // on another thread can't munmap() this mapping mid-scan -- see
            // MmapShard's comment in XStore.h.
            std::shared_ptr<void> mapped_handle = GetOrMapFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
            void *mapped_base = mapped_handle.get();
            if (mapped_base != nullptr) {
                unsigned long long first_offset = (iter_epoch - curr_ts.first_epoch) * cfg_struct.rowlength();
                unsigned long long region_length = rows_to_process * cfg_struct.rowlength();

                // The READ lock (whole range or per chunk, see XStore_ScanLock.h) and the bulk prefetch hint before the kernel's
                // row-by-row walk: aggregates walk the full span with no early exit, so the whole region is always touched.
                xstore::scanlock::ScanUnderReadLock(target_file_path, mapped_base, first_offset, region_length, cfg_struct.rowlength(),
                                  iter_epoch - curr_ts.first_epoch,
                                  [&](const char *rows_base, unsigned long long rows, unsigned long long row_offset) {
                                      kernel(rows_base, rows, curr_ts.year, iter_epoch - curr_ts.first_epoch + row_offset);
                                      return true;
                                  },
                                  [] {});
            }
        }

        iter_epoch = end_of_current_file + 1;
    }
}

// Segments below this count run sequentially via a single WalkYearSpan call
// -- the common case (perf-suite defaults, most real queries) never spans
// more than a handful of years, and thread creation (tens of microseconds
// each) plus the final merge pass isn't worth paying for a span the
// sequential walk would finish just as fast. Only spans wide enough to
// actually benefit (verified in this session's own benchmark before this
// number was chosen) take the parallel path.
constexpr size_t K_MIN_SEGMENTS_FOR_PARALLEL_WALK = 4;

template <typename PartialState, typename SegmentKernel, typename MergeFn, typename ReserveFn>
PartialState XStore::WalkYearSpanParallel(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                                            unsigned long long start_epoch_t, unsigned long long end_epoch_t,
                                            PartialState initial_state, SegmentKernel &&segment_kernel, MergeFn &&merge_fn,
                                            ReserveFn &&reserve_fn) {
    // Enumerate segments first -- O(#years), not O(#rows), so this is cheap
    // even for a wide span -- to know upfront whether parallel dispatch is
    // worth it before committing to any thread spawning.
    struct Segment { unsigned long long start; unsigned long long end; };
    std::vector<Segment> segments;
    const xstore::scanlock::Config scan_config = xstore::scanlock::Get();
    const bool split_years = scan_config.parallel_threads != 1;
    // A year segment is cut into pieces of whole zone-map blocks (on the year's row numbering) when the scan may run parallel.
    const unsigned long long rows_per_block = RowsPerBlock(static_cast<uint16_t>(cfg_struct.rowlength()));
    const unsigned long long piece_rows = std::max<unsigned long long>(1, (std::max<unsigned long long>(1, scan_config.piece_bytes / cfg_struct.rowlength()) + rows_per_block - 1) / rows_per_block) * rows_per_block;
    {
        unsigned long long iter_epoch = start_epoch_t;
        while (iter_epoch <= end_epoch_t) {
            TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
            unsigned long long end_of_current_file = std::min(end_epoch_t, static_cast<unsigned long long>(curr_ts.last_epoch));
            if (!split_years) {
                segments.push_back({iter_epoch, end_of_current_file});
            } else {
                for (unsigned long long piece_start = iter_epoch; piece_start <= end_of_current_file;) {
                    const unsigned long long row = piece_start - curr_ts.first_epoch;
                    const unsigned long long piece_end = std::min(end_of_current_file, curr_ts.first_epoch + (row / piece_rows + 1) * piece_rows - 1);
                    segments.push_back({piece_start, piece_end});
                    piece_start = piece_end + 1;
                }
            }
            iter_epoch = end_of_current_file + 1;
        }
    }

    if (segments.size() < (split_years ? 2 : K_MIN_SEGMENTS_FOR_PARALLEL_WALK)) {
        // Narrow span -- delegate straight to the existing sequential walk,
        // no threads, no merge step, byte-for-byte the same code path this
        // function used before parallelism existed.
        PartialState result = initial_state;
        reserve_fn(result, end_epoch_t - start_epoch_t + 1);
        WalkYearSpan(db_base_path, cfg_struct, start_epoch_t, end_epoch_t,
            [&](const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
                segment_kernel(result, rows_base, rows_to_process, year, first_row_index);
            });
        return result;
    }

    // Wide span -- fan segments out across the shared XStorePreadPool
    // instead of spawning raw std::threads. Same pool BatchQuery already
    // uses (XStore_BatchOps.cpp) -- a process-wide, fixed-size worker set
    // sized as "cores left over after the ZMQ server's own worker threads"
    // (XStorePreadPool::Init, called once from main.cpp). Routing through
    // it means this request's threads are capped by the SAME budget every
    // other concurrent request (aggregate or BatchQuery) draws from, so
    // total threads in flight across the whole process can't exceed the
    // pool's fixed size -- unlike the old per-request hardware_concurrency()
    // cap, which had no visibility into other concurrent requests and could
    // multiply past hardware_concurrency() when several landed at once. One
    // partial-state slot per segment (not per worker thread) since
    // ParallelFor doesn't expose which pool worker runs which index.
    std::vector<PartialState> partial_results(segments.size(), initial_state);
    // At most parallel_threads pieces at a time (waves; a year-wide scan of several years with parallel_threads 1 is one wave).
    // The number of threads is chosen again for every wave (adaptive: from the machine's load right now).
    for (size_t wave_start = 0, wave = 0; wave_start < segments.size(); wave_start += wave) {
    wave = split_years ? std::min<size_t>(xstore::scanlock::EffectiveThreads(scan_config), segments.size() - wave_start) : segments.size();
    XStorePreadPool::ParallelFor(wave, [&](size_t in_wave) {
        const size_t s = wave_start + in_wave;
        const Segment &seg = segments[s];
        reserve_fn(partial_results[s], seg.end - seg.start + 1);
        // Each Segment already falls entirely within one year (built that
        // way above), so this WalkYearSpan call's internal loop runs
        // exactly once -- reuses the exact same GetOrMapFile/lock/madvise/
        // kernel-call logic the sequential path uses, just invoked
        // per-segment instead of once for the whole span.
        WalkYearSpan(db_base_path, cfg_struct, seg.start, seg.end,
            [&](const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
                segment_kernel(partial_results[s], rows_base, rows_to_process, year, first_row_index);
            });
    });
    }

    PartialState result = initial_state;
    for (PartialState &partial : partial_results) {
        merge_fn(result, partial);
    }
    return result;
}

namespace {
// Per-thread partial state for Min/Max's parallel merge. Value/Data are
// exactly what ProcessFileRangeSelect already tracks -- separated into its
// own struct only so WalkYearSpanParallel has one object per thread to
// accumulate into (never a value shared across threads: protobuf/
// ValueStruct objects aren't safe for concurrent mutation from multiple
// threads even on logically-disjoint fields).
struct alignas(64) MinMaxPartial {
    ValueStruct value;
    XStore_Proto::FData data;
};

// Per-thread partial state for Sum/Avg's parallel merge.
struct alignas(64) SumPartial {
    double sum = 0.0;
    unsigned long long count = 0;
};

// Per-thread partial state for Exists's parallel merge. Can't use a bare
// bool for PartialState -- WalkYearSpanParallel stores one slot per segment
// in a std::vector<PartialState>, and std::vector<bool> is the infamous
// bitset specialization whose operator[] returns a proxy reference, not a
// real bool&, which the segment_kernel's bool& parameter can't bind to.
struct alignas(64) ExistsPartial {
    bool found = false;
};

// Per-thread partial state for Ohlc's parallel merge. open/close_set track
// whether this segment saw any valid data at all (both flip true together --
// see ScanSegmentForOhlc); high_low_set is folded into the same open/close
// life-cycle since a valid open/close always implies a valid high/low from
// the same rows, kept as its own bool only because ScanSegmentForOhlc's
// per-block merge needs to distinguish "haven't seen a block yet" from
// "seen one, running min/max are meaningful" independent of open (open, once
// set, never changes within a segment, while high/low keep updating).
struct alignas(64) OhlcPartial {
    double open = 0.0, high = 0.0, low = 0.0, close = 0.0;
    bool open_set = false, high_low_set = false, close_set = false;
};

// Per-thread partial state for Variance/Stddev's parallel merge -- a
// sibling of SumPartial rather than widening it, since Sum/Avg never need
// sum_sq and a shared struct would carry dead weight into their hot path.
struct alignas(64) VariancePartial {
    double sum = 0.0;
    double sum_sq = 0.0;
    unsigned long long count = 0;
};

// Per-piece partial state for Percentile/DistinctCount/TopK: every real value of the piece, in one vector (order is not load-bearing --
// see ProcessFileRangeCollectValues's comment). The merge keeps the pieces' vectors as they are (nothing is concatenated or copied) and
// each of the three ops reduces them where they lie (XStore_ExactAggregates.h).
// One cache line each: the pieces' partial states sit side by side in one vector and every push_back writes its vector's end pointer, so
// states sharing a line made all the threads fight over it (a piece took ~150 ms for 1M rows, ten times a single thread's rate).
struct alignas(64) CollectPartial {
    std::vector<double> values;                    // this piece (or, on the sequential path, the whole span)
    std::vector<std::vector<double>> pieces;       // after the merge: the pieces' vectors
};
static_assert(alignof(CollectPartial) == 64, "neighbouring pieces' states must not share a cache line");
// The other partial states above are one cache line each for the same reason (their kernels write them through references per row; they only
// scan where the zone map does not cover, so the gain is not measured).
static_assert(alignof(MinMaxPartial) == 64 && alignof(SumPartial) == 64 && alignof(ExistsPartial) == 64 && alignof(OhlcPartial) == 64 &&
              alignof(VariancePartial) == 64, "neighbouring pieces' states must not share a cache line");

// A piece's values vector is sized once for the most values the piece can hold (untouched pages of it are not resident), not regrown
// chunk by chunk as the scan lock hands the kernel 4 MiB at a time: that regrowth copied the vector at every chunk, ~90 % of a 12M-row call.
struct ReserveCollected {
    void operator()(CollectPartial &partial, unsigned long long rows) const { partial.values.reserve(rows); }
};

// Hands the piece's vector over to the merged state: no copy, and no second copy of the values alive at any moment.
void MergeCollected(CollectPartial &acc, CollectPartial &partial) {
    if (!partial.values.empty()) acc.pieces.push_back(std::move(partial.values));
    std::vector<double>().swap(partial.values);
}

// The merged state's vectors as spans for the reductions.
std::vector<xstore::ValueSpan> CollectedSpans(CollectPartial &state) {
    std::vector<xstore::ValueSpan> spans;
    if (!state.values.empty()) spans.push_back({state.values.data(), state.values.size()});
    for (std::vector<double> &piece : state.pieces) spans.push_back({piece.data(), piece.size()});
    return spans;
}

size_t CountValues(const std::vector<xstore::ValueSpan> &spans) {
    size_t total = 0;
    for (const xstore::ValueSpan &span : spans) total += span.size;
    return total;
}

// Runs the reductions' tasks on the shared scan pool: as many workers as the scan's adaptive team size (idle cores less headroom, the
// same figure the collect waves use, re-read for every call), each pulling the next task from one counter, so no worker waits for a
// slow task of a "wave" and a busy machine gets one worker, i.e. the plain sequential run.
void RunOnScanTeam(size_t tasks, const std::function<void(size_t)> &body) {
    const size_t team = std::min<size_t>(tasks, xstore::scanlock::EffectiveThreads(xstore::scanlock::Get()));
    if (team <= 1) {
        xstore::RunInline(tasks, body);
        return;
    }
    std::atomic<size_t> next{0};
    XStorePreadPool::ParallelFor(team, [&](size_t) {
        for (size_t task; (task = next.fetch_add(1, std::memory_order_relaxed)) < tasks;) body(task);
    });
}

// Bytes held per value of the range by an exact aggregate (the range length is the upper-bound count of values; see
// XStore_ExactAggregates.h for the reductions). Measured on Mystic over 12M values (unique, 7 distinct, sorted, random order), growth of
// the process's anonymous memory during one call: 11-13 bytes per value for all three (8 for the values, the rest allocator slack and
// the reductions' histograms and heaps), 100 before the vectors were sized once per piece and the pieces' copy was dropped.
// 16 covers it; the reductions work in place on the pieces' vectors, so DistinctCount needs no more than the others. TopK adds 16 bytes
// per entry kept.
constexpr uint64_t PERCENTILE_BYTES_PER_VALUE = 16;
constexpr uint64_t DISTINCTCOUNT_BYTES_PER_VALUE = 16;
constexpr uint64_t TOPK_BYTES_PER_VALUE = 16;
constexpr uint64_t TOPK_BYTES_PER_ENTRY = 16;

uint64_t AggregateWorkingSetEstimate(unsigned long long start_epoch_t, unsigned long long end_epoch_t, uint64_t bytes_per_value) {
    const uint64_t values = end_epoch_t - start_epoch_t + 1;
    return values > UINT64_MAX / bytes_per_value ? UINT64_MAX : values * bytes_per_value;   // saturate: refused as too large
}
}  // namespace

template <typename Cmp, typename PruneCheck>
void XStore::ScanSegmentWithZoneMapPruning(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                            uint32_t tracked_column_count,
                                            const char *rows_base, unsigned long long rows_to_process,
                                            unsigned long long first_row_index, unsigned short column_idx,
                                            ValueStruct &best_value, XStore_Proto::FData &best_data,
                                            PruneCheck &&should_skip_block) {
    if (rows_to_process == 0) return;

    const ZoneMapFile::Pin zmap_pin = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
    ZoneMapFile &zmap = *zmap_pin;
    uint64_t rows_per_block = zmap.RowsPerBlockValue();
    uint64_t first_row = first_row_index;
    uint64_t last_row = first_row_index + rows_to_process - 1;

    for (uint64_t block = BlockIndexForRow(first_row, rows_per_block);
         block <= BlockIndexForRow(last_row, rows_per_block);
         ++block) {
        uint64_t block_first_row = std::max(first_row, block * rows_per_block);
        uint64_t block_last_row = std::min(last_row, block * rows_per_block + rows_per_block - 1);
        uint64_t block_row_count = block_last_row - block_first_row + 1;
        const char *block_rows_base = rows_base + (block_first_row - first_row) * row_length;

        auto stats = zmap.TryGetBlockStats(block);
        if (stats.has_value() && column_idx < stats->size()) {
            const ZoneMapEntry &e = (*stats)[column_idx];
            if (e.valid_count == 0) continue; // block has no numeric values for this column at all -- skip
            if (should_skip_block(e)) continue; // zone map proves this block can't improve on best_value
        }
        // Not prunable (no zone-map coverage yet, column past the tracked
        // cap, or the zone map says this block could improve the answer)
        // -- scan it for real.
        ProcessFileRangeSelect<Cmp>(block_rows_base, block_row_count, row_length, column_idx, best_value, best_data);
    }
}

void XStore::AccumulateSegmentWithZoneMapPushdown(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                                   uint32_t tracked_column_count,
                                                   const char *rows_base, unsigned long long rows_to_process,
                                                   unsigned long long first_row_index, unsigned short column_idx,
                                                   double &sum_out, unsigned long long &valid_count_out,
                                                   double *sum_sq_out) {
    if (rows_to_process == 0) return;

    const ZoneMapFile::Pin zmap_pin = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
    ZoneMapFile &zmap = *zmap_pin;
    uint64_t rows_per_block = zmap.RowsPerBlockValue();
    uint64_t first_row = first_row_index;
    uint64_t last_row = first_row_index + rows_to_process - 1;

    for (uint64_t block = BlockIndexForRow(first_row, rows_per_block);
         block <= BlockIndexForRow(last_row, rows_per_block);
         ++block) {
        uint64_t block_own_first_row = block * rows_per_block;
        uint64_t block_own_last_row = block_own_first_row + rows_per_block - 1;

        // Fully covered means the block's OWN full extent lies inside the
        // query range, not just the already-query-clipped sub-range this
        // segment happens to hand us -- a block only partially inside the
        // query still needs a real scan, since its zone-map sum covers the
        // whole block including rows outside what was actually asked for.
        bool block_fully_in_range = (block_own_first_row >= first_row && block_own_last_row <= last_row);

        // An inexact block (a folded row was overwritten or deleted) still counts the old row: scan it.
        auto stats = zmap.TryGetBlockStats(block);
        if (block_fully_in_range && stats.has_value() && column_idx < stats->size() && !ZoneMapEntryInexact((*stats)[column_idx])) {
            const ZoneMapEntry &e = (*stats)[column_idx];
            sum_out += e.sum_value;
            valid_count_out += e.valid_count;
            if (sum_sq_out) *sum_sq_out += e.sum_sq_value;
            continue; // zero-scan pushdown
        }

        uint64_t block_first_row = std::max(first_row, block_own_first_row);
        uint64_t block_last_row = std::min(last_row, block_own_last_row);
        uint64_t block_row_count = block_last_row - block_first_row + 1;
        const char *block_rows_base = rows_base + (block_first_row - first_row) * row_length;
        ProcessFileRangeAccumulate(block_rows_base, block_row_count, row_length, column_idx, sum_out, valid_count_out, sum_sq_out);
    }
}

void XStore::ScanSegmentForExistence(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                      uint32_t tracked_column_count,
                                      const char *rows_base, unsigned long long rows_to_process,
                                      unsigned long long first_row_index, unsigned short column_idx,
                                      bool &found_out) {
    if (rows_to_process == 0 || found_out) return;

    const ZoneMapFile::Pin zmap_pin = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
    ZoneMapFile &zmap = *zmap_pin;
    uint64_t rows_per_block = zmap.RowsPerBlockValue();
    uint64_t first_row = first_row_index;
    uint64_t last_row = first_row_index + rows_to_process - 1;

    for (uint64_t block = BlockIndexForRow(first_row, rows_per_block);
         block <= BlockIndexForRow(last_row, rows_per_block);
         ++block) {
        uint64_t block_own_first_row = block * rows_per_block;
        uint64_t block_own_last_row = block_own_first_row + rows_per_block - 1;
        uint64_t block_first_row = std::max(first_row, block_own_first_row);
        uint64_t block_last_row = std::min(last_row, block_own_last_row);
        uint64_t block_row_count = block_last_row - block_first_row + 1;
        const char *block_rows_base = rows_base + (block_first_row - first_row) * row_length;

        auto stats = zmap.TryGetBlockStats(block);
        if (stats.has_value() && column_idx < stats->size()) {
            const ZoneMapEntry &e = (*stats)[column_idx];
            if (e.valid_count == 0) {
                // valid_count==0 covers the block's WHOLE extent, so it's
                // safe to skip regardless of how much of the block the
                // query actually covers -- every row in it (including any
                // outside [firstRow,lastRow]) is null.
                continue;
            }
            // valid_count > 0 only proves a non-null value exists SOMEWHERE
            // in the block's full extent -- trustworthy as zero-scan proof
            // of existence only when the query covers that whole extent.
            // A block only partially covered by the query (this is the
            // narrower, already-query-clipped sub-range) could have its one
            // real value sitting outside [firstRow,lastRow] -- same
            // "fully covered" requirement AccumulateSegmentWithZoneMapPushdown
            // uses for Sum/Avg, caught here the same way by a test that
            // queried a sub-range of a block containing an out-of-range value.
            // An inexact block's valid_count may still count an overwritten or deleted value: no proof.
            bool block_fully_in_range = (block_own_first_row >= first_row && block_own_last_row <= last_row);
            if (block_fully_in_range && !ZoneMapEntryInexact(e)) {
                found_out = true;
                return;
            }
            // Falls through to a real scan of just the queried sub-range below.
        }

        // Not prunable (no zone-map coverage yet, column past the
        // tracked cap) -- fall back to a real scan of this block, same
        // kernel MinAggregate uses. best_value/best_data are throwaway
        // here: only whether a valid value was found at all matters.
        ValueStruct best_value;
        XStore_Proto::FData best_data;
        ProcessFileRangeSelect<std::less<long double>>(block_rows_base, block_row_count, row_length, column_idx, best_value, best_data);
        if (best_value.is_initialized) {
            found_out = true;
            return;
        }
    }
}

void XStore::ScanSegmentForOhlc(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                 uint32_t tracked_column_count,
                                 const char *rows_base, unsigned long long rows_to_process,
                                 unsigned long long first_row_index, unsigned short column_idx,
                                 double &open_out, bool &open_set_out,
                                 double &high_out, double &low_out, bool &high_low_set_out,
                                 double &close_out, bool &close_set_out) {
    if (rows_to_process == 0) return;

    const ZoneMapFile::Pin zmap_pin = PinZoneMap(db_base_path, year, row_length, tracked_column_count);
    ZoneMapFile &zmap = *zmap_pin;
    uint64_t rows_per_block = zmap.RowsPerBlockValue();
    uint64_t first_row = first_row_index;
    uint64_t last_row = first_row_index + rows_to_process - 1;

    for (uint64_t block = BlockIndexForRow(first_row, rows_per_block);
         block <= BlockIndexForRow(last_row, rows_per_block);
         ++block) {
        uint64_t block_own_first_row = block * rows_per_block;
        uint64_t block_own_last_row = block_own_first_row + rows_per_block - 1;

        // Same "block's OWN full extent inside the query range" requirement
        // AccumulateSegmentWithZoneMapPushdown/ScanSegmentForExistence use --
        // a block's first_value/last_value describe its whole extent, so a
        // block only partially covered by the query still needs a real scan
        // of just the queried sub-range.
        bool block_fully_in_range = (block_own_first_row >= first_row && block_own_last_row <= last_row);

        // An inexact block's first/last/min/max may describe overwritten or deleted rows: scan it.
        auto stats = zmap.TryGetBlockStats(block);
        if (block_fully_in_range && stats.has_value() && column_idx < stats->size() && !ZoneMapEntryInexact((*stats)[column_idx])) {
            const ZoneMapEntry &e = (*stats)[column_idx];
            if (e.valid_count == 0) continue; // no numeric data anywhere in this block -- skip

            if (!open_set_out) {
                open_out = e.first_value;
                open_set_out = true;
            }
            high_out = high_low_set_out ? std::max(high_out, e.max_value) : e.max_value;
            low_out = high_low_set_out ? std::min(low_out, e.min_value) : e.min_value;
            high_low_set_out = true;
            close_out = e.last_value; // blocks visited in ascending chronological order -- always overwrite, last one wins
            close_set_out = true;
            continue; // zero-scan pushdown
        }

        uint64_t block_first_row = std::max(first_row, block_own_first_row);
        uint64_t block_last_row = std::min(last_row, block_own_last_row);
        uint64_t block_row_count = block_last_row - block_first_row + 1;
        const char *block_rows_base = rows_base + (block_first_row - first_row) * row_length;

        double local_open = 0.0, local_high = 0.0, local_low = 0.0, local_close = 0.0;
        bool local_open_set = false, local_high_low_set = false, local_close_set = false;
        ProcessFileRangeOhlc(block_rows_base, block_row_count, row_length, column_idx,
                              local_open, local_open_set, local_high, local_low, local_high_low_set,
                              local_close, local_close_set);
        if (!local_close_set) continue; // nothing valid in this sub-range

        if (!open_set_out) {
            open_out = local_open;
            open_set_out = true;
        }
        high_out = high_low_set_out ? std::max(high_out, local_high) : local_high;
        low_out = high_low_set_out ? std::min(low_out, local_low) : local_low;
        high_low_set_out = true;
        close_out = local_close;
        close_set_out = true;
    }
}

void XStore::MinAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Min_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[MIN AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[MIN AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("MIN AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // Narrow spans (< kMinSegmentsForParallelWalk years) delegate straight
    // to WalkYearSpan internally -- identical to the pre-parallel code path,
    // same tie-break guarantee (first-encountered-row-wins, verified by
    // MinMaxAggregateTieBreakReturnsFirstEncounteredRow). Wide spans merge
    // per-thread winners by strict improvement only (never on a tie, same
    // rule ProcessFileRangeSelect itself uses) -- ties ACROSS different
    // threads' segments resolve by thread/segment processing order rather
    // than strict chronological order in that case specifically; not
    // exercised by any current test and considered acceptable for the same
    // reason real sensor/metric data essentially never produces an exact
    // cross-year tie on an aggregate extremum.
    MinMaxPartial final = WalkYearSpanParallel<MinMaxPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        MinMaxPartial{},
        [&](MinMaxPartial &partial, const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
            ScanSegmentWithZoneMapPruning<std::less<long double>>(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.value, partial.data,
                [&](const ZoneMapEntry &e) {
                    // MIN: a block whose smallest value is already >= our
                    // current best can't improve it -- skip.
                    return partial.value.is_initialized && e.min_value >= partial.value.value;
                });
        },
        [](MinMaxPartial &final_state, MinMaxPartial &partial) {
            if (partial.value.is_initialized && (!final_state.value.is_initialized || partial.value.value < final_state.value.value)) {
                final_state.value = partial.value;
                final_state.data = partial.data;
            }
        });

    if (final.value.is_initialized) {
        return_data->set_opstatus("SUCCESS");
        *return_data->mutable_opresult() = final.data;
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MIN AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::MaxAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Max_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[MAX AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[MAX AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("MAX AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    MinMaxPartial final = WalkYearSpanParallel<MinMaxPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        MinMaxPartial{},
        [&](MinMaxPartial &partial, const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
            ScanSegmentWithZoneMapPruning<std::greater<long double>>(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.value, partial.data,
                [&](const ZoneMapEntry &e) {
                    // MAX: a block whose largest value is already <= our
                    // current best can't improve it -- skip.
                    return partial.value.is_initialized && e.max_value <= partial.value.value;
                });
        },
        [](MinMaxPartial &final_state, MinMaxPartial &partial) {
            if (partial.value.is_initialized && (!final_state.value.is_initialized || partial.value.value > final_state.value.value)) {
                final_state.value = partial.value;
                final_state.data = partial.data;
            }
        });

    if (final.value.is_initialized) {
        return_data->set_opstatus("SUCCESS");
        *return_data->mutable_opresult() = final.data;
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MAX AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::SumAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Sum_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[SUM AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[SUM AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("SUM AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // Wide spans (>= kMinSegmentsForParallelWalk years) sum per-thread
    // partial sums together sequentially in the merge step -- this
    // reassociates the floating-point addition relative to a strict
    // single-pass left-to-right sum (different terms combine in a different
    // order), which can differ by a few ULP from the narrow-span/sequential
    // result for the same data. This isn't an avoidable implementation
    // choice: any correct year-parallel sum must combine per-year partial
    // sums in some order, which is inherently a different order than one
    // continuous accumulation. Narrow spans are untouched -- byte-identical
    // to before, including SumAggregateMatchesIndependentlyComputedSumFromRawDiskBytes's
    // exact-value check, which stays on the sequential path (single-year span).
    SumPartial final = WalkYearSpanParallel<SumPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        SumPartial{},
        [&](SumPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.sum, partial.count);
        },
        [](SumPartial &final_state, SumPartial &partial) {
            final_state.sum += partial.sum;
            final_state.count += partial.count;
        });

    if (final.count > 0) {
        return_data->set_opstatus("SUCCESS");
        return_data->set_opresult(final.sum);
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[SUM AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::AvgAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Avg_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[AVG AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[AVG AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("AVG AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // See SumAggregate for the parallel-merge floating-point reassociation
    // note -- same tradeoff applies here since Avg is Sum's kernel plus one
    // divide after.
    SumPartial final = WalkYearSpanParallel<SumPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        SumPartial{},
        [&](SumPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.sum, partial.count);
        },
        [](SumPartial &final_state, SumPartial &partial) {
            final_state.sum += partial.sum;
            final_state.count += partial.count;
        });
    double sum_result = final.sum;
    unsigned long long valid_count = final.count;

    if (valid_count > 0) {
        return_data->set_opstatus("SUCCESS");
        return_data->set_opresult(sum_result / (double)valid_count);
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[AVG AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::VarianceAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                                std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Variance_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[VARIANCE AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[VARIANCE AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("VARIANCE AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // Sample variance (N-1), naive sum/sum-of-squares -- never Welford's/
    // Chan's algorithm (plan requirement). See AccumulateSegmentWithZoneMapPushdown
    // for how sum_sq folds through both the pushdown and real-scan paths.
    VariancePartial final = WalkYearSpanParallel<VariancePartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        VariancePartial{},
        [&](VariancePartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.sum, partial.count, &partial.sum_sq);
        },
        [](VariancePartial &final_state, VariancePartial &partial) {
            final_state.sum += partial.sum;
            final_state.sum_sq += partial.sum_sq;
            final_state.count += partial.count;
        });

    if (final.count > 1) {
        double n = static_cast<double>(final.count);
        double variance = (final.sum_sq - (final.sum * final.sum) / n) / (n - 1.0);
        if (variance < 0.0) variance = 0.0; // floating-point cancellation guard
        return_data->set_opstatus("SUCCESS");
        return_data->set_opresult(variance);
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[VARIANCE AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::StddevAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                              std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Stddev_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[STDDEV AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[STDDEV AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("STDDEV AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // Same naive sum/sum-of-squares sample-variance computation as
    // VarianceAggregate -- Stddev is just its sqrt.
    VariancePartial final = WalkYearSpanParallel<VariancePartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        VariancePartial{},
        [&](VariancePartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.sum, partial.count, &partial.sum_sq);
        },
        [](VariancePartial &final_state, VariancePartial &partial) {
            final_state.sum += partial.sum;
            final_state.sum_sq += partial.sum_sq;
            final_state.count += partial.count;
        });

    if (final.count > 1) {
        double n = static_cast<double>(final.count);
        double variance = (final.sum_sq - (final.sum * final.sum) / n) / (n - 1.0);
        if (variance < 0.0) variance = 0.0; // floating-point cancellation guard
        return_data->set_opstatus("SUCCESS");
        return_data->set_opresult(std::sqrt(variance));
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[STDDEV AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::ExistsAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                              std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Exists_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[EXISTS AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[EXISTS AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("EXISTS AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // false is a legitimate answer, not an error -- unlike Min/Max/Sum/Avg's
    // "ERROR: No valid data found", a range with no matching value is a
    // real, successful EXISTS=false result.
    ExistsPartial final = WalkYearSpanParallel<ExistsPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        ExistsPartial{},
        [&](ExistsPartial &partial, const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
            ScanSegmentForExistence(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx, partial.found);
        },
        [](ExistsPartial &final_state, ExistsPartial &partial) {
            final_state.found = final_state.found || partial.found;
        });

    return_data->set_opstatus("SUCCESS");
    return_data->set_exists(final.found);
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::OhlcAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, XStore_Proto::Ohlc_REP *return_data) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[OHLC AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[OHLC AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const std::string unindexed_notice = UnindexedColumnNotice("OHLC AGGREGATE", db_name, db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t, column_idx);

    // Narrow-span sequential path calls ScanSegmentForOhlc repeatedly on the
    // SAME partial across consecutive years -- correct by construction, see
    // ScanSegmentForOhlc's own comment in XStore.h. Wide-span parallel path
    // gives each segment its own fresh OhlcPartial then merges them in the
    // vector's chronological order below -- same open-wins-first/close-wins-
    // last/running-min-max reduction, just applied across segments instead
    // of within one.
    OhlcPartial final = WalkYearSpanParallel<OhlcPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        OhlcPartial{},
        [&](OhlcPartial &partial, const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
            ScanSegmentForOhlc(db_base_path_str, year, cfg_struct.rowlength(), EffectiveTrackedColumnCount(cfg_struct),
                rows_base, rows_to_process, first_row_index, column_idx,
                partial.open, partial.open_set, partial.high, partial.low, partial.high_low_set,
                partial.close, partial.close_set);
        },
        [](OhlcPartial &final_state, OhlcPartial &partial) {
            if (!partial.close_set) return; // this segment/year had no data at all
            if (!final_state.open_set) {
                final_state.open = partial.open;
                final_state.open_set = true;
            }
            final_state.high = final_state.high_low_set ? std::max(final_state.high, partial.high) : partial.high;
            final_state.low = final_state.high_low_set ? std::min(final_state.low, partial.low) : partial.low;
            final_state.high_low_set = true;
            final_state.close = partial.close; // segments merged in chronological (vector) order -- always overwrite, last one wins
            final_state.close_set = true;
        });

    if (final.close_set) {
        return_data->set_opstatus("SUCCESS");
        return_data->set_open(final.open);
        return_data->set_high(final.high);
        return_data->set_low(final.low);
        return_data->set_close(final.close);
    } else {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[OHLC AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
    }
    if (!unindexed_notice.empty()) {
        return_data->set_opstatus(return_data->opstatus() + " | " + unindexed_notice);
    }
}

void XStore::PercentileAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                                  std::string_view db_name_view, unsigned short int column_idx, double percentile,
                                  XStore_Proto::Percentile_REP *return_data, xstore::MemoryPool::Clock::time_point deadline) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[PERCENTILE AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[PERCENTILE AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (percentile < 0.0 || percentile > 1.0) {
        std::string err_msg = fmt::format("[PERCENTILE AGGREGATE] [DB: {}] - percentile must be in [0.0, 1.0]", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const uint64_t working_set = AggregateWorkingSetEstimate(start_epoch_t, end_epoch_t, PERCENTILE_BYTES_PER_VALUE);
    std::shared_ptr<xstore::MemoryLease> working_set_lease;
    if (const xstore::MemoryPool::Result leased = xstore::AcquireOperationLease(working_set, deadline, working_set_lease);
        leased != xstore::MemoryPool::Result::kGranted) {
        const std::string err_msg = xstore::OperationLeaseRejection("PERCENTILE AGGREGATE", db_name, leased, working_set);
        SetTaggedStatus(return_data->mutable_opstatus(), "[ERROR] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    CollectPartial final_state = WalkYearSpanParallel<CollectPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        CollectPartial{},
        [&](CollectPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short /*year*/, unsigned long long /*first_row_index*/) {
            ProcessFileRangeCollectValues(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.values);
        },
        MergeCollected, ReserveCollected{});
    const std::vector<xstore::ValueSpan> spans = CollectedSpans(final_state);
    const size_t value_count = CountValues(spans);

    if (value_count == 0) {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[PERCENTILE AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", db_name, column_idx, start_epoch_t, end_epoch_t);
        return;
    }

    // Exact, PERCENTILE_CONT (linear-interpolation) semantics -- same
    // interpolation numpy.percentile/most SQL engines default to.
    // NaNs are left out of the ranking (ExactPercentile); the status stays exactly "SUCCESS" (callers compare it), so the count goes to the log.
    uint64_t nan_skipped = 0;
    const double value = xstore::ExactPercentile(spans, percentile, RunOnScanTeam, &nan_skipped);
    if (nan_skipped == value_count) {
        return_data->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[PERCENTILE AGGREGATE] [DB: {}] - Every value of column {} over [{}, {}] is NaN", db_name, column_idx, start_epoch_t, end_epoch_t);
        return;
    }
    if (nan_skipped > 0) {
        spdlog::warn("[PERCENTILE AGGREGATE] [DB: {}] - {} NaN value(s) of column {} over [{}, {}] left out of the percentile", db_name, nan_skipped,
                     column_idx, start_epoch_t, end_epoch_t);
    }

    return_data->set_opstatus("SUCCESS");
    return_data->set_value(value);
}

void XStore::DistinctCountAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                                     std::string_view db_name_view, unsigned short int column_idx,
                                     XStore_Proto::DistinctCount_REP *return_data, xstore::MemoryPool::Clock::time_point deadline) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[DISTINCTCOUNT AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[DISTINCTCOUNT AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    const uint64_t working_set = AggregateWorkingSetEstimate(start_epoch_t, end_epoch_t, DISTINCTCOUNT_BYTES_PER_VALUE);
    std::shared_ptr<xstore::MemoryLease> working_set_lease;
    if (const xstore::MemoryPool::Result leased = xstore::AcquireOperationLease(working_set, deadline, working_set_lease);
        leased != xstore::MemoryPool::Result::kGranted) {
        const std::string err_msg = xstore::OperationLeaseRejection("DISTINCTCOUNT AGGREGATE", db_name, leased, working_set);
        SetTaggedStatus(return_data->mutable_opstatus(), "[ERROR] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    CollectPartial final_state = WalkYearSpanParallel<CollectPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        CollectPartial{},
        [&](CollectPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short /*year*/, unsigned long long /*first_row_index*/) {
            ProcessFileRangeCollectValues(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.values);
        },
        MergeCollected, ReserveCollected{});
    const std::vector<xstore::ValueSpan> spans = CollectedSpans(final_state);

    // An empty range is a legitimate count of zero, not an error -- same
    // "absence is a real answer" precedent ExistsAggregate uses for false.
    return_data->set_opstatus("SUCCESS");
    return_data->set_count(xstore::ExactDistinctCount(spans, RunOnScanTeam));
}

void XStore::TopKAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                            std::string_view db_name_view, unsigned short int column_idx, uint32_t k,
                            XStore_Proto::TopK_REP *return_data, xstore::MemoryPool::Clock::time_point deadline) {

    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[TOPK AGGREGATE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[TOPK AGGREGATE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(return_data->mutable_opstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    uint64_t working_set = AggregateWorkingSetEstimate(start_epoch_t, end_epoch_t, TOPK_BYTES_PER_VALUE);
    const uint64_t kept = std::min<uint64_t>(k, end_epoch_t - start_epoch_t + 1) * TOPK_BYTES_PER_ENTRY;
    working_set = working_set > UINT64_MAX - kept ? UINT64_MAX : working_set + kept;
    std::shared_ptr<xstore::MemoryLease> working_set_lease;
    if (const xstore::MemoryPool::Result leased = xstore::AcquireOperationLease(working_set, deadline, working_set_lease);
        leased != xstore::MemoryPool::Result::kGranted) {
        const std::string err_msg = xstore::OperationLeaseRejection("TOPK AGGREGATE", db_name, leased, working_set);
        SetTaggedStatus(return_data->mutable_opstatus(), "[ERROR] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    CollectPartial final_state = WalkYearSpanParallel<CollectPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        CollectPartial{},
        [&](CollectPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short /*year*/, unsigned long long /*first_row_index*/) {
            ProcessFileRangeCollectValues(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.values);
        },
        MergeCollected, ReserveCollected{});
    const std::vector<xstore::ValueSpan> spans = CollectedSpans(final_state);

    // Fewer than k distinct values in range returns however many exist --
    // same as SQL's GROUP BY ... ORDER BY COUNT(*) DESC LIMIT k. An empty
    // range is SUCCESS with zero entries, same precedent as DistinctCount.
    // Most frequent first, ties by ascending value, for determinism.
    const std::vector<std::pair<double, uint64_t>> ranked = xstore::ExactTopK(spans, k, RunOnScanTeam);
    return_data->set_opstatus("SUCCESS");
    for (const auto &[value, count] : ranked) {
        XStore_Proto::TopKEntry *entry = return_data->add_entries();
        entry->set_value(value);
        entry->set_count(count);
    }
}

// FIND BY VALUE (streamed, wire-format encode -- see XStore.h's declaration
// comment for the general contract). Deliberately NOT built on
// WalkYearSpan/WalkYearSpanParallel: every kernel that traversal drives
// (Min/Max/Sum/Avg/Exists/Ohlc/Variance/Stddev/Percentile/DistinctCount/
// TopK) only ever needs a year-relative row index, never a real epoch_time,
// so that shared kernel signature has nothing that reconstructs one. This
// mirrors RangeQuery's own hand-rolled per-year loop instead (see
// XStore_RangeOps.cpp), where iter_epoch is the literal loop variable and
// epoch_time is therefore always trivially correct -- adapted here so the
// innermost loop walks BLOCKS (RowsPerBlock/BlockIndexForRow, same block
// geometry the .zmap/.catidx files already share) rather than individual
// rows: a CatIdxFile bloom filter (Task 4/5) can rule out an entire block at
// once, skipping straight to the block's end+1, before falling back to a
// real per-row decode+compare scan for any block it can't rule out. Matches
// emit incrementally as they're found -- safe here (unlike a
// WalkYearSpanParallel kernel) because this loop is single-threaded and
// sequential, same as RangeQuery's own incremental emit_chunk calls.
void XStore::FindByValue(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                          std::string_view db_name_view, int32_t column_idx, const XStore_Proto::FData &value,
                          size_t chunk_size_bytes, const std::function<void(std::string&&, bool)> &emit_chunk, bool streamed_reply) {
    FindByValueJob job;
    job.start_epoch = start_epoch_t;
    job.end_epoch = end_epoch_t;
    job.db_name = std::string(db_name_view);
    job.column_idx = column_idx;
    job.value = value;
    job.chunk_size_bytes = chunk_size_bytes;
    job.streamed_reply = streamed_reply;
    FindByValueRun(job, emit_chunk, [] { return false; });
}

XStore::RunState XStore::FindByValueRun(FindByValueJob &job, const std::function<void(std::string&&, bool)> &emit_chunk,
                                        const std::function<bool()> &should_park) {
    const std::string &db_name = UpperCaseInto(job.db_name);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    auto set_warning = [&](const char *tag, const std::string &body) {
        // Append rather than overwrite: a later warning (e.g. year-missing)
        // must not silently drop an earlier one (e.g. the full-scan notice).
        if (job.has_warning) {
            job.last_warning += " | ";
        }
        job.has_warning = true;
        job.last_warning += tag;
        job.last_warning += body;
    };
    auto finish = [&](std::string &&chunk_buf) {
        if (job.has_warning) {
            XStoreWireEncode::AppendQueryStatus(chunk_buf, job.last_warning);
        }
        emit_chunk(std::move(chunk_buf), true);
    };

    if (!job.started) {
        job.started = true;
        job.next_epoch = job.start_epoch;
        // Loaded BEFORE the cfg is read, as the deletes do: CfgEpochCurrent tells a resumed run whether the DB was removed meanwhile.
        job.cfg_epoch = CfgEpoch();
        if (!GetOrLoadCfg(db_name, db_base_path_str, job.cfg_struct)) {
            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - {}.CFG does not exist", db_name, db_name));
            finish(std::string());
            return RunState::kDone;
        }
        if (job.start_epoch > job.end_epoch) {
            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - Target end time must be after start time", db_name));
            finish(std::string());
            return RunState::kDone;
        }
        if (std::filesystem::is_directory(db_base_path_str) == 0) {
            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - DB does not exist", db_name));
            finish(std::string());
            return RunState::kDone;
        }

        // A null (unset) target is refused on every path: the bloom never holds unset columns (live inserts and rebuilds
        // both skip them), so an indexed column would answer "no rows" while the real-scan fallback would match the
        // explicitly-unset cells. One rule instead of an answer that depends on indexing.
        if (job.value.oneOfData_case() == XStore_Proto::FData::ONEOFDATA_NOT_SET) {
            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - Searching for a null (unset) value is not supported", db_name));
            finish(std::string());
            return RunState::kDone;
        }

        job.encoded_target = EncodeFDataForCatIdx(job.value);
        job.tracked_column_count = EffectiveTrackedColumnCount(job.cfg_struct);
        job.chunk_buf = xstore::ChunkPool::Instance().Take(job.chunk_size_bytes + 4096);
    } else if (!CfgEpochCurrent(db_name, job.cfg_epoch)) {
        // Parked, and the DB was removed (maybe re-created) since: the rest of the request is stale. The rows already read stay in the reply.
        set_warning("[ERROR] ", fmt::format("[FINDBYVALUE] [DB: {}] - The DB was removed while the request ran", db_name));
        finish(std::move(job.chunk_buf));
        return RunState::kDone;
    }

    const XStore_Proto::ConfigStruct &cfg_struct = job.cfg_struct;
    const int32_t column_idx = job.column_idx;
    const size_t chunk_size_bytes = job.chunk_size_bytes;
    const std::string &encoded_target = job.encoded_target;
    const uint32_t tracked_column_count = job.tracked_column_count;
    // Notice-only: the scan is always correct (real-scan fallback), the caller just deserves to know it paid for
    // one. Decided below from the width of the .catidx this query actually consults, once per query.
    bool &unindexed_notice_emitted = job.unindexed_notice_emitted;
    std::string &chunk_buf = job.chunk_buf;

    // Unlike RangeQuery, an equality predicate's result size can't be known
    // ahead of a scan (it depends on how many rows actually match, not the
    // queried time range), so RangeQuery's pre-flight check-before-scanning
    // guard (XStore_RangeOps.cpp) doesn't apply here -- but the same
    // underlying risk does: a client that reassembles every streamed chunk
    // into one buffer before ParseFromArray/ParseFromString cannot parse a
    // >2GB result. Tracked as a running total across the whole query (not
    // just the current chunk_buf) and checked after every match, so an
    // unselective predicate over a wide range stops scanning instead of
    // streaming past the limit. A streamed_reply (Request.streamOk) goes to
    // a client that parses chunk by chunk, so it has no such limit.
    const unsigned long long reply_limit = job.streamed_reply ? std::numeric_limits<unsigned long long>::max() : xstore::ReplyLimitBytes().load();
    unsigned long long int &total_emitted_bytes = job.total_emitted_bytes;
    bool &size_limit_exceeded = job.size_limit_exceeded;
    bool consumed_any = false;   // parking is only asked after one scan chunk or piece of this run: every run makes progress

    unsigned long long int iter_epoch = job.next_epoch;
    const unsigned long long int end_epoch_t = job.end_epoch;
    while (iter_epoch <= end_epoch_t) {
        TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
        const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);
        const unsigned long long int segment_end = std::min(end_epoch_t, static_cast<unsigned long long int>(curr_ts.last_epoch));

        if (std::filesystem::exists(target_file_path) == 0) {
            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - {}.XSTORE does not exist", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }

        std::shared_ptr<void> mapped_handle = GetOrMapFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
        if (mapped_handle == nullptr) {
            set_warning("[ERROR] ", fmt::format("[FINDBYVALUE] [DB: {}] - Unable to open/map {}.XSTORE", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }
        void *mapped_base = mapped_handle.get();

        uint64_t rows_per_block = RowsPerBlock(static_cast<uint16_t>(cfg_struct.rowlength()));

        // Everything that needs the CatIdxFile is pulled out HERE, in one
        // scoped, pinned acquisition per year segment, and the pin is dropped
        // before the per-block scan below ever runs (a pin held across the
        // row scan would keep the entry out of eviction for the whole query).
        // Pre-fetching also makes LookupExact run ONCE per segment instead of
        // once per "maybe" block, which is where it used to re-scan the whole
        // year's directory.
        const uint64_t first_block = BlockIndexForRow(iter_epoch - curr_ts.first_epoch, rows_per_block);
        const uint64_t last_block = BlockIndexForRow(segment_end - curr_ts.first_epoch, rows_per_block);
        std::vector<char> block_might_contain;       // empty == no bloom for this column, every block must scan
        std::vector<uint64_t> bitmap_candidate_rows; // sorted year-relative row indices, only if bitmapped
        bool have_bitmap_candidates = false;
        if (column_idx >= 0 && static_cast<uint32_t>(column_idx) < tracked_column_count) {
            const CatIdxFile::Pin catidx_pin = PinCatIdx(db_base_path_str, curr_ts.year,
                                                         static_cast<uint16_t>(cfg_struct.rowlength()), tracked_column_count);
            CatIdxFile &catidx = *catidx_pin;
            const bool high_column = static_cast<uint32_t>(column_idx) >= XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS;
            LiveRebuildProgress progress;
            // No live widen anywhere (the common case): one relaxed load, no Rebuilding() for a low column.
            const bool live = YearsRebuilding() != 0 &&
                              FindRebuildProgress(db_base_path_str, curr_ts.year, /*catidx=*/true, progress);
            if (live) {
                // One notice per year whose part of the range reaches the catidx's contiguous watermark (any
                // column >= the always-indexed width: anywhere in the year). See LiveRebuildNotices. None for a
                // column a keep-old rebuild's old index still covers.
                if (static_cast<uint32_t>(column_idx) >= progress.old_width &&
                    (high_column || segment_end - static_cast<unsigned long long>(curr_ts.first_epoch) >= progress.done_rows)) {
                    set_warning("", RebuildingColumnNoticeText("FINDBYVALUE", db_name, column_idx, curr_ts.year, &progress));
                    unindexed_notice_emitted = true;
                }
            } else if (!unindexed_notice_emitted && high_column && catidx.Rebuilding()) {
                set_warning("", RebuildingColumnNoticeText("FINDBYVALUE", db_name, column_idx, curr_ts.year));
                unindexed_notice_emitted = true;
            } else if (!unindexed_notice_emitted && static_cast<uint32_t>(column_idx) >= catidx.TrackedColumnCount()) {
                set_warning("", UnindexedColumnNoticeText("FINDBYVALUE", db_name, column_idx, curr_ts.year,
                                                          catidx.TrackedColumnCount()));
                unindexed_notice_emitted = true;
            }
            block_might_contain.assign(static_cast<size_t>(last_block - first_block + 1), 0);
            for (uint64_t b = first_block; b <= last_block; ++b) {
                block_might_contain[static_cast<size_t>(b - first_block)] =
                    catidx.MightContain(b, static_cast<uint32_t>(column_idx), encoded_target) ? 1 : 0;
            }
            if (catidx.HasBitmapSection()) {
                auto exact_rows = catidx.LookupExact(static_cast<uint32_t>(column_idx), encoded_target);
                if (exact_rows.has_value()) {
                    bitmap_candidate_rows = std::move(*exact_rows);
                    std::sort(bitmap_candidate_rows.begin(), bitmap_candidate_rows.end());
                    have_bitmap_candidates = true;
                }
            }
        }
        // No CatIdxFile reference is live past this point.

        const unsigned long long int first_offset = (iter_epoch - curr_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int region_length = (segment_end - iter_epoch + 1) * cfg_struct.rowlength();

        {
            // The rows are read under a READ lock one scan chunk at a time (xstore::scanlock); a completed output buffer waits
            // in `ready` until that chunk's lock is released, so sending to a slow client never holds off the inserts.
            std::vector<std::string> ready;

            // One shared emitter for both the bitmap-candidate path and the
            // real-scan path -- they only ever differ in WHICH rows they
            // consider, never in how a confirmed match is emitted. Returns
            // false once the 2GB streaming guard trips.
            auto emit_match = [&](unsigned long long int e, const char *row_ptr) {
                size_t size_before = chunk_buf.size();
                XStoreWireEncode::EncodeQueryResponseEntry(chunk_buf, e, row_ptr, cfg_struct.rowlength());
                total_emitted_bytes += static_cast<unsigned long long int>(chunk_buf.size() - size_before);
                if (chunk_buf.size() >= chunk_size_bytes) {
                    ready.push_back(std::move(chunk_buf));
                    chunk_buf = xstore::ChunkPool::Instance().Take(chunk_size_bytes + 4096);
                }
                if (total_emitted_bytes > reply_limit) {
                    set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - Query results exceeded 2GB, results truncated", db_name));
                    size_limit_exceeded = true;
                    return false;
                }
                return true;
            };
            // Confirms a row really still holds the target value right now, by
            // decoding it from the mapped file -- identical to what the
            // real-scan path does per row.
            auto row_still_matches = [&](const char *row_ptr) {
                XStore_Proto::FData f_data;
                return ExtractColumnByIndexSkipParse(row_ptr, cfg_struct.rowlength(),
                                                     static_cast<unsigned short>(column_idx), f_data)
                       && EncodeFDataForCatIdx(f_data) == encoded_target;
            };

            // The block loop over the rows [chunk_lo, chunk_hi] (whole zone-map blocks, except at the ends of the queried range);
            // emit(e, row_ptr) returns false to stop the scan. It only reads shared state, so pieces may run it in parallel.
            auto scan_rows = [&](unsigned long long int chunk_lo, unsigned long long int chunk_hi, auto &&emit) {
            bool stop = false;
            unsigned long long int epoch_time = chunk_lo;
            while (epoch_time <= chunk_hi) {
                uint64_t row_index = epoch_time - curr_ts.first_epoch;
                uint64_t block_index = BlockIndexForRow(row_index, rows_per_block);
                uint64_t block_start_row = block_index * rows_per_block;
                unsigned long long int block_end_epoch = std::min(chunk_hi,
                    static_cast<unsigned long long int>(curr_ts.first_epoch + block_start_row + rows_per_block - 1));

                // Bloom said "definitely absent" for this whole block -- skip
                // it without touching a row. This is the only tier that may
                // skip real data, and it is safe because a bloom filter never
                // produces a false negative.
                bool must_scan = true;
                if (!block_might_contain.empty() &&
                    block_might_contain[static_cast<size_t>(block_index - first_block)] == 0) {
                    must_scan = false;
                } else if (have_bitmap_candidates) {
                    // The bitmap directory is a BITMAP BUILD-TIME SNAPSHOT, i.e.
                    // a candidate list, never an authority: rows it names may
                    // since have been overwritten or deleted. So every
                    // candidate is re-decoded and re-compared against the
                    // CURRENT row bytes before it is emitted, exactly like the
                    // real-scan path below -- still far cheaper than scanning
                    // the block, since only the rows the directory names get
                    // decoded. (The converse staleness -- rows backfilled
                    // AFTER bitmap build that the directory never learned about
                    // -- is handled upstream: any such write invalidates the
                    // whole section in FlushBlock, so have_bitmap_candidates is
                    // false and this block real-scans instead.)
                    auto it = std::lower_bound(bitmap_candidate_rows.begin(), bitmap_candidate_rows.end(), block_start_row);
                    for (; it != bitmap_candidate_rows.end() && *it < block_start_row + rows_per_block; ++it) {
                        unsigned long long int e = curr_ts.first_epoch + *it;
                        if (e < chunk_lo || e > chunk_hi) continue; // clip to this chunk (inside the queried range)
                        const char *row_ptr = static_cast<const char*>(mapped_base)
                                              + (e - curr_ts.first_epoch) * cfg_struct.rowlength();
                        if (!row_still_matches(row_ptr)) continue; // stale directory entry
                        if (!emit(e, row_ptr)) { stop = true; break; }
                    }
                    must_scan = false;
                }
                // Neither tier above applied (bloom said maybe and no usable
                // directory -- column over the cardinality cap at bitmap build,
                // year not bitmapped yet, or directory invalidated since -- or
                // the column is past tracked_column_count): real-scan the
                // block, which is always correct.

                if (must_scan && column_idx >= 0) {
                    for (unsigned long long int e = epoch_time; e <= block_end_epoch; e++) {
                        const char *row_ptr = static_cast<const char*>(mapped_base)
                                              + (e - curr_ts.first_epoch) * cfg_struct.rowlength();
                        if (row_still_matches(row_ptr) && !emit(e, row_ptr)) { stop = true; break; }
                    }
                }
                if (stop) return false;
                epoch_time = block_end_epoch + 1;
            }
            return true;
            };

            // Parallel (xstore::scanlock::Config::parallel_threads > 1 and at least two pieces): the pieces are scanned in parallel
            // and their matches are appended here in order; otherwise the sequential scan.
            bool parked = false;
            unsigned long long int parked_epoch = 0;
            const xstore::scanlock::OrderedScan ordered = xstore::scanlock::ScanRegionOrdered(
                    target_file_path, mapped_base, first_offset, region_length, cfg_struct.rowlength(), iter_epoch - curr_ts.first_epoch,
                    [&](const char *, unsigned long long rows, unsigned long long row_offset, std::string &out) {
                        const unsigned long long int lo = iter_epoch + row_offset;
                        scan_rows(lo, lo + rows - 1, [&](unsigned long long int e, const char *row_ptr) {
                            XStoreWireEncode::EncodeQueryResponseEntry(out, e, row_ptr, cfg_struct.rowlength());
                            return true;
                        });
                    },
                    [&](std::string &piece_out) {
                        total_emitted_bytes += static_cast<unsigned long long int>(piece_out.size());
                        chunk_buf.append(piece_out);
                        if (chunk_buf.size() >= chunk_size_bytes) {
                            emit_chunk(std::move(chunk_buf), false);
                            chunk_buf = xstore::ChunkPool::Instance().Take(chunk_size_bytes + 4096);
                        }
                        if (total_emitted_bytes > reply_limit) {
                            set_warning("[WARNING] ", fmt::format("[FINDBYVALUE] [DB: {}] - Query results exceeded 2GB, results truncated", db_name));
                            size_limit_exceeded = true;
                            return false;
                        }
                        // A whole piece is consumed: the scan can be taken up again after it.
                        consumed_any = true;
                        if (should_park()) {
                            parked = true;
                            return false;
                        }
                        return true;
                    });
            if (ordered.ran) {
                if (parked) {
                    // Parked after the last piece of the segment: nothing is left to resume here, go on to the next segment.
                    parked_epoch = iter_epoch + ordered.rows_consumed;
                    if (parked_epoch > segment_end) parked = false;
                }
            } else {
                xstore::scanlock::ScanUnderReadLock(
                    target_file_path, mapped_base, first_offset, region_length, cfg_struct.rowlength(), iter_epoch - curr_ts.first_epoch,
                    [&](const char *, unsigned long long rows, unsigned long long row_offset) {
                        // Between two scan chunks nothing is in flight: every earlier chunk's output is in chunk_buf or was emitted by after().
                        if (consumed_any && should_park()) {
                            parked = true;
                            parked_epoch = iter_epoch + row_offset;
                            return false;
                        }
                        consumed_any = true;
                        const unsigned long long int lo = iter_epoch + row_offset;
                        return scan_rows(lo, lo + rows - 1, emit_match);
                    },
                    [&] {
                        for (std::string &full : ready) emit_chunk(std::move(full), false);
                        ready.clear();
                    });
            }
            if (parked) {
                job.next_epoch = parked_epoch;
                return RunState::kParked;
            }
        }

        if (size_limit_exceeded) break;
        iter_epoch = segment_end + 1;
    }

    finish(std::move(chunk_buf));
    return RunState::kDone;
}
