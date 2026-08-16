#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
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
void XStore::WalkYearSpan(const std::string &dbBasePath, const XStore_Proto::ConfigStruct &cfgStruct,
                           unsigned long long startEpochT, unsigned long long endEpochT, Kernel &&kernel) {
    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, cfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}/{}.XSTORE", dbBasePath, CurrTs.Year);

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

void XStore::MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePathStr = DbBasePath(dbName);

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

    ValueStruct MinValue;
    MinValue.IsInitialized = false;
    XStore_Proto::FData MinData;

    WalkYearSpan(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        [&](const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::less<long double>>(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, MinValue, MinData);
        });

    if (MinValue.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = MinData;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MIN AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::MinAggregateTypedDouble(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePathStr = DbBasePath(dbName);

    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        returnData->set_opstatus(fmt::format("[ERROR] [MIN AGGREGATE] [DB: {}] - {}.CFG does not exist", dbName, dbName));
        return;
    }

    if (startEpochT > endEpochT) {
        returnData->set_opstatus(fmt::format("[ERROR] [MIN AGGREGATE] [DB: {}] - Target end time must be after start time", dbName));
        return;
    }

    bool MinInitialized = false;
    double MinValue = 0.0;
    XStore_Proto::FData MinData;

    WalkYearSpan(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        [&](const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeSelectTypedDouble<std::less<double>>(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, MinInitialized, MinValue, MinData);
        });

    if (MinInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = MinData;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
    }
}

void XStore::MaxAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePathStr = DbBasePath(dbName);

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

    ValueStruct MaxValue;
    MaxValue.IsInitialized = false;
    XStore_Proto::FData MaxData;

    WalkYearSpan(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        [&](const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::greater<long double>>(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, MaxValue, MaxData);
        });

    if (MaxValue.IsInitialized) {
        returnData->set_opstatus("SUCCESS");
        *returnData->mutable_opresult() = MaxData;
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[MAX AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::SumAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePathStr = DbBasePath(dbName);

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

    double SumResult = 0.0;
    unsigned long long ValidCount = 0;

    WalkYearSpan(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        [&](const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
        });

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[SUM AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}

void XStore::AvgAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT,
                            std::string dbName, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData) {

    ToUpperCase(dbName.begin(), dbName.end());
    std::string DbBasePathStr = DbBasePath(dbName);

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

    double SumResult = 0.0;
    unsigned long long ValidCount = 0;

    WalkYearSpan(DbBasePathStr, CfgStruct, startEpochT, endEpochT,
        [&](const char *rowsBase, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(rowsBase, rowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
        });

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult / (double)ValidCount);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[AVG AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}
