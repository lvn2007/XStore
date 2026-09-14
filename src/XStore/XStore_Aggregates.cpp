#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_PreadPool.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <sys/mman.h>
#include <unistd.h>

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

                {
                    FileRangeLockGuard guard(target_file_path, first_offset, region_length, FileRangeLock::LockType::READ);

                    // Bulk prefetch hint before the kernel's row-by-row walk --
                    // same rationale as RangeQuery (XStore_RangeOps.cpp): one
                    // madvise() covering the whole segment instead of one page
                    // fault per first-touched page as the kernel scans forward.
                    // Matters more here than for RangeQuery -- aggregates walk
                    // the full span with no early exit, so the whole region is
                    // always going to be touched.
                    {
                        static const long PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
                        char *span_start = static_cast<char*>(mapped_base) + first_offset;
                        uintptr_t aligned_addr = reinterpret_cast<uintptr_t>(span_start) & ~(static_cast<uintptr_t>(PAGE_SIZE) - 1);
                        size_t align_slop = reinterpret_cast<uintptr_t>(span_start) - aligned_addr;
                        madvise(reinterpret_cast<void*>(aligned_addr), region_length + align_slop, MADV_WILLNEED);
                    }

                    kernel(static_cast<const char*>(mapped_base) + first_offset, rows_to_process,
                           curr_ts.year, iter_epoch - curr_ts.first_epoch);
                }
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

template <typename PartialState, typename SegmentKernel, typename MergeFn>
PartialState XStore::WalkYearSpanParallel(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                                            unsigned long long start_epoch_t, unsigned long long end_epoch_t,
                                            PartialState initial_state, SegmentKernel &&segment_kernel, MergeFn &&merge_fn) {
    // Enumerate segments first -- O(#years), not O(#rows), so this is cheap
    // even for a wide span -- to know upfront whether parallel dispatch is
    // worth it before committing to any thread spawning.
    struct Segment { unsigned long long start; unsigned long long end; };
    std::vector<Segment> segments;
    {
        unsigned long long iter_epoch = start_epoch_t;
        while (iter_epoch <= end_epoch_t) {
            TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
            unsigned long long end_of_current_file = std::min(end_epoch_t, static_cast<unsigned long long>(curr_ts.last_epoch));
            segments.push_back({iter_epoch, end_of_current_file});
            iter_epoch = end_of_current_file + 1;
        }
    }

    if (segments.size() < K_MIN_SEGMENTS_FOR_PARALLEL_WALK) {
        // Narrow span -- delegate straight to the existing sequential walk,
        // no threads, no merge step, byte-for-byte the same code path this
        // function used before parallelism existed.
        PartialState result = initial_state;
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
    XStorePreadPool::ParallelFor(segments.size(), [&](size_t s) {
        const Segment &seg = segments[s];
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
struct MinMaxPartial {
    ValueStruct value;
    XStore_Proto::FData data;
};

// Per-thread partial state for Sum/Avg's parallel merge.
struct SumPartial {
    double sum = 0.0;
    unsigned long long count = 0;
};
}  // namespace

template <typename Cmp, typename PruneCheck>
void XStore::ScanSegmentWithZoneMapPruning(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                            const char *rows_base, unsigned long long rows_to_process,
                                            unsigned long long first_row_index, unsigned short column_idx,
                                            ValueStruct &best_value, XStore_Proto::FData &best_data,
                                            PruneCheck &&should_skip_block) {
    if (rows_to_process == 0) return;

    ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length);
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
                                                   const char *rows_base, unsigned long long rows_to_process,
                                                   unsigned long long first_row_index, unsigned short column_idx,
                                                   double &sum_out, unsigned long long &valid_count_out) {
    if (rows_to_process == 0) return;

    ZoneMapFile &zmap = GetOrOpenZoneMap(db_base_path, year, row_length);
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

        auto stats = zmap.TryGetBlockStats(block);
        if (block_fully_in_range && stats.has_value() && column_idx < stats->size()) {
            const ZoneMapEntry &e = (*stats)[column_idx];
            sum_out += e.sum_value;
            valid_count_out += e.valid_count;
            continue; // zero-scan pushdown
        }

        uint64_t block_first_row = std::max(first_row, block_own_first_row);
        uint64_t block_last_row = std::min(last_row, block_own_last_row);
        uint64_t block_row_count = block_last_row - block_first_row + 1;
        const char *block_rows_base = rows_base + (block_first_row - first_row) * row_length;
        ProcessFileRangeAccumulate(block_rows_base, block_row_count, row_length, column_idx, sum_out, valid_count_out);
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
            ScanSegmentWithZoneMapPruning<std::less<long double>>(db_base_path_str, year, cfg_struct.rowlength(),
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

    MinMaxPartial final = WalkYearSpanParallel<MinMaxPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        MinMaxPartial{},
        [&](MinMaxPartial &partial, const char *rows_base, unsigned long long rows_to_process, unsigned short year, unsigned long long first_row_index) {
            ScanSegmentWithZoneMapPruning<std::greater<long double>>(db_base_path_str, year, cfg_struct.rowlength(),
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
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(),
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

    // See SumAggregate for the parallel-merge floating-point reassociation
    // note -- same tradeoff applies here since Avg is Sum's kernel plus one
    // divide after.
    SumPartial final = WalkYearSpanParallel<SumPartial>(db_base_path_str, cfg_struct, start_epoch_t, end_epoch_t,
        SumPartial{},
        [&](SumPartial &partial, const char *rows_base, unsigned long long rows_to_process,
            unsigned short year, unsigned long long first_row_index) {
            AccumulateSegmentWithZoneMapPushdown(db_base_path_str, year, cfg_struct.rowlength(),
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
}
