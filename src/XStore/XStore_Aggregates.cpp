#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
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
void XStore::WalkYearSpan(const std::string &dbBasePath, const XStore_Proto::ConfigStruct &cfgStruct,
                           unsigned long long startEpochT, unsigned long long endEpochT, Kernel &&kernel) {
    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, cfgStruct.timeresolution());
        const std::string &TargetFilePath = CachedYearFilePath(dbBasePath, CurrTs.Year);

        unsigned long long EndOfCurrentFile = std::min(endEpochT, static_cast<unsigned long long>(CurrTs.LastEpoch));
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            // Held through the kernel() call below so a concurrent eviction
            // on another thread can't munmap() this mapping mid-scan -- see
            // MmapShard's comment in XStore.h.
            std::shared_ptr<void> MappedHandle = GetOrMapFile(TargetFilePath, cfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
            void *MappedBase = MappedHandle.get();
            if (MappedBase != nullptr) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * cfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * cfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

                    // Bulk prefetch hint before the kernel's row-by-row walk --
                    // same rationale as RangeQuery (XStore_RangeOps.cpp): one
                    // madvise() covering the whole segment instead of one page
                    // fault per first-touched page as the kernel scans forward.
                    // Matters more here than for RangeQuery -- aggregates walk
                    // the full span with no early exit, so the whole region is
                    // always going to be touched.
                    {
                        static const long PageSize = sysconf(_SC_PAGE_SIZE);
                        char *SpanStart = static_cast<char*>(MappedBase) + FirstOffset;
                        uintptr_t AlignedAddr = reinterpret_cast<uintptr_t>(SpanStart) & ~(static_cast<uintptr_t>(PageSize) - 1);
                        size_t AlignSlop = reinterpret_cast<uintptr_t>(SpanStart) - AlignedAddr;
                        madvise(reinterpret_cast<void*>(AlignedAddr), RegionLength + AlignSlop, MADV_WILLNEED);
                    }

                    kernel(static_cast<const char*>(MappedBase) + FirstOffset, RowsToProcess);
                }
            }
        }

        IterEpoch = EndOfCurrentFile + 1;
    }
}

// Segments below this count run sequentially via a single WalkYearSpan call
// -- the common case (perf-suite defaults, most real queries) never spans
// more than a handful of years, and thread creation (tens of microseconds
// each) plus the final merge pass isn't worth paying for a span the
// sequential walk would finish just as fast. Only spans wide enough to
// actually benefit (verified in this session's own benchmark before this
// number was chosen) take the parallel path.
constexpr size_t kMinSegmentsForParallelWalk = 4;

template <typename PartialState, typename SegmentKernel, typename MergeFn>
PartialState XStore::WalkYearSpanParallel(const std::string &dbBasePath, const XStore_Proto::ConfigStruct &cfgStruct,
                                            unsigned long long startEpochT, unsigned long long endEpochT,
                                            PartialState initialState, SegmentKernel &&segmentKernel, MergeFn &&mergeFn) {
    // Enumerate segments first -- O(#years), not O(#rows), so this is cheap
    // even for a wide span -- to know upfront whether parallel dispatch is
    // worth it before committing to any thread spawning.
    struct Segment { unsigned long long Start; unsigned long long End; };
    std::vector<Segment> Segments;
    {
        unsigned long long IterEpoch = startEpochT;
        while (IterEpoch <= endEpochT) {
            TimeStruct CurrTs(IterEpoch, cfgStruct.timeresolution());
            unsigned long long EndOfCurrentFile = std::min(endEpochT, static_cast<unsigned long long>(CurrTs.LastEpoch));
            Segments.push_back({IterEpoch, EndOfCurrentFile});
            IterEpoch = EndOfCurrentFile + 1;
        }
    }

    if (Segments.size() < kMinSegmentsForParallelWalk) {
        // Narrow span -- delegate straight to the existing sequential walk,
        // no threads, no merge step, byte-for-byte the same code path this
        // function used before parallelism existed.
        PartialState Result = initialState;
        WalkYearSpan(dbBasePath, cfgStruct, startEpochT, endEpochT,
            [&](const char *rowsBase, unsigned long long rowsToProcess) {
                segmentKernel(Result, rowsBase, rowsToProcess);
            });
        return Result;
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
    // across workers (t, t+NumThreads, t+2*NumThreads, ...) rather than
    // chunked, so a handful of unusually large years don't all land on the
    // same thread.
    size_t NumThreads = std::min(Segments.size(), static_cast<size_t>(std::max(1u, std::thread::hardware_concurrency())));
    std::vector<PartialState> PartialResults(NumThreads, initialState);
    std::vector<std::thread> Workers;
    Workers.reserve(NumThreads);

    for (size_t T = 0; T < NumThreads; T++) {
        Workers.emplace_back([&, T]() {
            for (size_t S = T; S < Segments.size(); S += NumThreads) {
                const Segment &Seg = Segments[S];
                // Each Segment already falls entirely within one year (built
                // that way above), so this WalkYearSpan call's internal loop
                // runs exactly once -- reuses the exact same GetOrMapFile/
                // lock/madvise/kernel-call logic the sequential path uses,
                // just invoked per-segment instead of once for the whole span.
                WalkYearSpan(dbBasePath, cfgStruct, Seg.Start, Seg.End,
                    [&](const char *rowsBase, unsigned long long rowsToProcess) {
                        segmentKernel(PartialResults[T], rowsBase, rowsToProcess);
                    });
            }
        });
    }
    for (std::thread &W : Workers) W.join();

    PartialState Result = initialState;
    for (PartialState &Partial : PartialResults) {
        mergeFn(Result, Partial);
    }
    return Result;
}

namespace {
// Per-thread partial state for Min/Max's parallel merge. Value/Data are
// exactly what ProcessFileRangeSelect already tracks -- separated into its
// own struct only so WalkYearSpanParallel has one object per thread to
// accumulate into (never a value shared across threads: protobuf/
// ValueStruct objects aren't safe for concurrent mutation from multiple
// threads even on logically-disjoint fields).
struct MinMaxPartial {
    ValueStruct Value;
    XStore_Proto::FData Data;
};

// Per-thread partial state for Sum/Avg's parallel merge.
struct SumPartial {
    double Sum = 0.0;
    unsigned long long Count = 0;
};
}  // namespace

void XStore::MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string_view dbNameView, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData) {

    const std::string &dbName = UpperCaseInto(dbNameView);
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[MIN AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[MIN AGGREGATE] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
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
    MinMaxPartial Final = WalkYearSpanParallel<MinMaxPartial>(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        MinMaxPartial{},
        [&](MinMaxPartial &Partial, const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::less<long double>>(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, Partial.Value, Partial.Data);
        },
        [](MinMaxPartial &FinalState, MinMaxPartial &Partial) {
            if (Partial.Value.IsInitialized && (!FinalState.Value.IsInitialized || Partial.Value.Value < FinalState.Value.Value)) {
                FinalState.Value = Partial.Value;
                FinalState.Data = Partial.Data;
            }
        });

    if (Final.Value.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = Final.Data;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MIN AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::MaxAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string_view dbNameView, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData) {

    const std::string &dbName = UpperCaseInto(dbNameView);
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[MAX AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[MAX AGGREGATE] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    MinMaxPartial Final = WalkYearSpanParallel<MinMaxPartial>(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        MinMaxPartial{},
        [&](MinMaxPartial &Partial, const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::greater<long double>>(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, Partial.Value, Partial.Data);
        },
        [](MinMaxPartial &FinalState, MinMaxPartial &Partial) {
            if (Partial.Value.IsInitialized && (!FinalState.Value.IsInitialized || Partial.Value.Value > FinalState.Value.Value)) {
                FinalState.Value = Partial.Value;
                FinalState.Data = Partial.Data;
            }
        });

    if (Final.Value.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = Final.Data;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MAX AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::SumAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string_view dbNameView, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData) {

    const std::string &dbName = UpperCaseInto(dbNameView);
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[SUM AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[SUM AGGREGATE] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
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
    SumPartial Final = WalkYearSpanParallel<SumPartial>(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        SumPartial{},
        [&](SumPartial &Partial, const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, Partial.Sum, Partial.Count);
        },
        [](SumPartial &FinalState, SumPartial &Partial) {
            FinalState.Sum += Partial.Sum;
            FinalState.Count += Partial.Count;
        });

    if (Final.Count > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(Final.Sum);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[SUM AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::AvgAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string_view dbNameView, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData) {

    const std::string &dbName = UpperCaseInto(dbNameView);
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[AVG AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[AVG AGGREGATE] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(returnData->mutable_opstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // See SumAggregate for the parallel-merge floating-point reassociation
    // note -- same tradeoff applies here since Avg is Sum's kernel plus one
    // divide after.
    SumPartial Final = WalkYearSpanParallel<SumPartial>(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        SumPartial{},
        [&](SumPartial &Partial, const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, Partial.Sum, Partial.Count);
        },
        [](SumPartial &FinalState, SumPartial &Partial) {
            FinalState.Sum += Partial.Sum;
            FinalState.Count += Partial.Count;
        });
    double SumResult = Final.Sum;
    unsigned long long ValidCount = Final.Count;

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult / (double)ValidCount);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[AVG AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}
