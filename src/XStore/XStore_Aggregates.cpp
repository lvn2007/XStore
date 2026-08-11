#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <filesystem>
#include <functional>

namespace {

// Shared by Min/Max/Sum/AvgAggregate: walks [startEpochT, endEpochT] one
// year-file at a time, read-locking each existing segment and handing it to
// kernel(file, firstOffset, rowsToProcess) -- the four aggregates differ only
// in which ProcessFileRange* kernel runs and what it accumulates into. A
// missing year contributes nothing (kernel just isn't called for it), same
// as Range Query/Delete's handling of missing years.
template <typename Kernel>
void WalkYearSpan(const std::string &dbBasePath, const XStore_Proto::ConfigStruct &cfgStruct,
                   unsigned long long startEpochT, unsigned long long endEpochT, Kernel &&kernel) {
    unsigned long long IterEpoch = startEpochT;

    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, cfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}/{}.XSTORE", dbBasePath, CurrTs.Year);

        unsigned long long EndOfCurrentFile = std::min(endEpochT, static_cast<unsigned long long>(CurrTs.LastEpoch));
        unsigned long long RowsToProcess = (EndOfCurrentFile - IterEpoch) + 1;

        if (std::filesystem::exists(TargetFilePath)) {
            FILE *BFile = std::fopen(TargetFilePath.c_str(), "rb");
            if (BFile) {
                unsigned long long FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * cfgStruct.rowlength();
                unsigned long long RegionLength = RowsToProcess * cfgStruct.rowlength();

                {
                    FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);
                    kernel(BFile, FirstOffset, RowsToProcess);
                }
                std::fclose(BFile);
            }
        }

        IterEpoch = EndOfCurrentFile + 1;
    }
}

}  // namespace

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
        [&](FILE *bFile, unsigned long long firstOffset, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::less<long double>>(bFile, firstOffset, rowsToProcess, CfgStruct.rowlength(), columnIdx, MinValue, MinData);
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
        [&](FILE *bFile, unsigned long long firstOffset, unsigned long long rowsToProcess) {
            ProcessFileRangeSelectTypedDouble<std::less<double>>(bFile, firstOffset, rowsToProcess, CfgStruct.rowlength(), columnIdx, MinInitialized, MinValue, MinData);
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
        [&](FILE *bFile, unsigned long long firstOffset, unsigned long long rowsToProcess) {
            ProcessFileRangeSelect<std::greater<long double>>(bFile, firstOffset, rowsToProcess, CfgStruct.rowlength(), columnIdx, MaxValue, MaxData);
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
        [&](FILE *bFile, unsigned long long firstOffset, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(bFile, firstOffset, rowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
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
        [&](FILE *bFile, unsigned long long firstOffset, unsigned long long rowsToProcess) {
            ProcessFileRangeAccumulate(bFile, firstOffset, rowsToProcess, CfgStruct.rowlength(), columnIdx, SumResult, ValidCount);
        });

    if (ValidCount > 0) {
        returnData->set_opstatus("SUCCESS");
        returnData->set_opresult(SumResult / (double)ValidCount);
    } else {
        returnData->set_opstatus("ERROR: No valid data found");
        spdlog::warn("[AVG AGGREGATE] [DB: {}] - No valid data for column {} over [{}, {}]", dbName, columnIdx, startEpochT, endEpochT);
    }
}
