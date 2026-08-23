#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <filesystem>
#include <functional>
#include <sys/mman.h>
#include <thread>
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

                    kernel(static_cast<const char*>(mapped_base) + first_offset, rows_to_process);
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
            [&](const char *rows_base, unsigned long long rows_to_process) {
                segment_kernel(result, rows_base, rows_to_process);
            });
        return result;
    }

    // Wide span -- bound THIS request's own thread count to hardware
    // concurrency, so it alone never spawns more threads than there are
    // cores. This cap has no visibility into other concurrent requests,
    // though: if several wide-span requests land at once (e.g. several ZMQ
    // workers each handling one), total threads in flight can still exceed
    // hardware_concurrency -- benchmarked under 4-concurrent load on this
    // 16-core box and didn't show a clear regression (see the session notes
    // that added this), but that's one data point on one machine shape, not
    // a guarantee against oversubscription in general. Segments are striped
    // across workers (t, t+num_threads, t+2*num_threads, ...) rather than
    // chunked, so a handful of unusually large years don't all land on the
    // same thread.
    size_t num_threads = std::min(segments.size(), static_cast<size_t>(std::max(1u, std::thread::hardware_concurrency())));
    std::vector<PartialState> partial_results(num_threads, initial_state);
    std::vector<std::thread> workers;
    workers.reserve(num_threads);

    for (size_t t = 0; t < num_threads; t++) {
        workers.emplace_back([&, t]() {
            for (size_t s = t; s < segments.size(); s += num_threads) {
                const Segment &seg = segments[s];
                // Each Segment already falls entirely within one year (built
                // that way above), so this WalkYearSpan call's internal loop
                // runs exactly once -- reuses the exact same GetOrMapFile/
                // lock/madvise/kernel-call logic the sequential path uses,
                // just invoked per-segment instead of once for the whole span.
                WalkYearSpan(db_base_path, cfg_struct, seg.start, seg.end,
                    [&](const char *rows_base, unsigned long long rows_to_process) {
                        segment_kernel(partial_results[t], rows_base, rows_to_process);
                    });
            }
        });
    }
    for (std::thread &w : workers) w.join();

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
        [&](MinMaxPartial &partial, const char *rows_base, unsigned long long rows_to_process) {
            ProcessFileRangeSelect<std::less<long double>>(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.value, partial.data);
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
        [&](MinMaxPartial &partial, const char *rows_base, unsigned long long rows_to_process) {
            ProcessFileRangeSelect<std::greater<long double>>(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.value, partial.data);
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
        [&](SumPartial &partial, const char *rows_base, unsigned long long rows_to_process) {
            ProcessFileRangeAccumulate(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.sum, partial.count);
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
        [&](SumPartial &partial, const char *rows_base, unsigned long long rows_to_process) {
            ProcessFileRangeAccumulate(rows_base, rows_to_process, cfg_struct.rowlength(), column_idx, partial.sum, partial.count);
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
