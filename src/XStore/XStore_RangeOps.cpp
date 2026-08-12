#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <ctime>
#include <fcntl.h>
#include <filesystem>

// RANGE INSERT
void XStore::RangeInsert(const XStore_Proto::RangeInsert_REQ *protoFromClient, XStore_Proto::RangeInsert_REP *returnData) {
    // Ensure DB Name string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(DbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[RANGE INSERT] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[RANGE INSERT] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Assuming incoming data is sorted --> Skip sorting
    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->rangeinsert_data_size(); i++) {
        CurrTime = protoFromClient->rangeinsert_data(i).timestamp();
        gmtime_r(&CurrTime, &EpochStruct);
        DataMap[(EpochStruct.tm_year + 1900)].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        const std::vector<unsigned long long int> &Indices = Pair.second;
        std::string TargetFile = fmt::format("{}/{}.XSTORE", DbBasePathStr, YearKey);

        // Grab first inserting element for TimeStruct purposes
        //  Get length of time to truncate
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->rangeinsert_data(Indices.front()).timestamp()), CfgStruct.timeresolution()};

        // Cached fd for this (dbName, year) file -- same fd cache UnaryInsert
        // uses (see GetOrOpenFile). Creates+ftruncates on first use, so the
        // separate open/ftruncate branch this used to have is gone.
        // Timestamps in this year aren't guaranteed contiguous (see
        // RegionLength below), so this still writes row by row rather than
        // as one flat span -- but each row is now a pwrite() rather than a
        // memcpy into a PROT_WRITE mapping -- see WriteRowGroupToFile for why.
        int Fd = GetOrOpenFile(TargetFile, CfgStruct.rowlength(), ThisTs.LastEpoch - ThisTs.FirstEpoch);
        if (Fd < 0) {
            std::string ErrMsg = fmt::format("[RANGE INSERT] [DB: {}] - Unable to open {}.XSTORE", DbName, YearKey);
            SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            continue;
        }

        // RegionLength (computed inside the shared helper below) must cover
        // the actual byte span of the writes, not just count*rowLength --
        // that undersizes the lock (and races unprotected writes past its
        // end) whenever the timestamps in this year aren't contiguous.
        // Relies on the same "incoming data is sorted" precondition the rest
        // of this function already assumes, so the last entry in the group
        // carries the highest offset.
        WriteRowGroupToFile(TargetFile, Fd, ThisTs.FirstEpoch, CfgStruct.rowlength(), Indices,
            [&](unsigned long long int idx) -> const XStore_Proto::RangeInsert_DATA& { return protoFromClient->rangeinsert_data(idx); });
    }

    // Return insert status
    std::string InfoMsg = fmt::format("[RANGE INSERT] [DB: {}] - Inserted {} elements successfully", DbName, protoFromClient->rangeinsert_data_size());
    SetTaggedStatus(returnData->mutable_insertstatus(), "[INFO] ", InfoMsg);
    spdlog::info(InfoMsg);
}

// RANGE QUERY
void XStore::RangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, XStore_Proto::RangeQuery_REP *returnData) {
    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(dbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Must run before the size check below: startEpochT/endEpochT are
    // unsigned, so an inverted range would otherwise underflow (endEpochT -
    // startEpochT) into a huge value and get misreported as ">2GB" instead
    // of the actual "end before start" error.
    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // IF query range > 2GB limit of Protobuf
    if ((endEpochT - startEpochT) * CfgStruct.rowlength() > 2'000'000'000) {
        std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - Query results is larger than 2GB", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Every timestamp in [startEpochT, endEpochT] whose year's file exists
    // gets an entry (holes included, see below), so the total row count --
    // and thus the final size of queryresponse -- is known upfront.
    // Reserving it avoids the repeated field's O(log n) reallocations+copies
    // for large spans; the 2GB check above already bounds this to a sane
    // upfront allocation.
    returnData->mutable_queryresponse()->Reserve(static_cast<int>(endEpochT - startEpochT + 1));

    // Walk the span one year-file at a time. Each iteration clamps its
    // segment to [IterEpoch, min(endEpochT, thisYear.LastEpoch)] and always
    // advances IterEpoch past it before the next iteration -- a single-year
    // span, and every year (including the last) of a multi-year span, are
    // all just this same loop body running once or several times, so there's
    // no separate "is this the final segment" branch to keep in sync (that
    // duplication is what previously let a missing final year's file spin
    // this forever -- see git history -- and let the read-lock region
    // undersize by one row; both are structurally impossible here since
    // IterEpoch always advances unconditionally and RegionLength is always
    // derived from the same inclusive SegmentEnd).
    unsigned long long int IterEpoch = startEpochT;
    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}/{}.XSTORE", DbBasePathStr, CurrTs.Year);
        const unsigned long long int SegmentEnd = std::min(endEpochT, static_cast<unsigned long long int>(CurrTs.LastEpoch));

        // IF target YEAR.XSTORE does NOT exist -- checked explicitly because
        // GetOrMapFile below opens with O_CREAT and would otherwise silently
        // create+map an empty file for a query that should just report no
        // data. A missing year contributes NO entries for its span (unlike a
        // hole within an existing year, which still gets an empty-field
        // entry -- see RangeQueryUninsertedTimestampsReturnEmptyEntriesNotOmitted
        // vs RangeQuerySpanningThreeYearsWithMissingMiddleYearFile).
        if (std::filesystem::exists(TargetFilePath) == 0) {
            std::string WarningMsg = fmt::format("[RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
            SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", WarningMsg);
            spdlog::warn(WarningMsg);
            IterEpoch = SegmentEnd + 1;
            continue;
        }

        // Cached mapping for this (dbName, year) file, same mmap LRU cache
        // Unary/Batch/RangeInsert use -- avoids an fseek+fread (2 syscalls)
        // per row; timestamps here are chronological/contiguous, so this
        // just parses straight out of the mapped page per row with zero
        // syscalls in the loop at all.
        void *MappedBase = GetOrMapFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
        if (MappedBase == nullptr) {
            std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
            SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            IterEpoch = SegmentEnd + 1;
            continue;
        }

        const unsigned long long int FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = (SegmentEnd - IterEpoch + 1) * CfgStruct.rowlength();

        {
            // Lock file prior to reading
            FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

            // Iterate over this segment & add queried data and status. Every
            // timestamp in range gets an entry -- unlike BatchQuery, holes
            // are NOT omitted here, they come back as zero-field RowData.
            for (unsigned long long int EpochTime = IterEpoch; EpochTime <= SegmentEnd; EpochTime++) {
                unsigned long long int Offset = (EpochTime - CurrTs.FirstEpoch) * CfgStruct.rowlength();

                // Parse straight from the mapped row into the response --
                // skips the TempData copy the previous fread()-based path paid.
                XStore_Proto::UnaryQuery_REP *CurrEntry = returnData->add_queryresponse();
                CurrEntry->set_timestamp(EpochTime);
                CurrEntry->mutable_data()->ParseFromArray(static_cast<char*>(MappedBase) + Offset, CfgStruct.rowlength());
            }
        }

        IterEpoch = SegmentEnd + 1;
    }
}

// RANGE DELETE
XStore_Proto::RangeDelete_REP XStore::RangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::RangeDelete_REP ToReturn;
    int DeleteCount = 0;

    // Ensure dbName string is all capitalized
    ToUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(dbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(dbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[RANGE DELETE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    if (startEpochT > endEpochT) {
        std::string ErrMsg = fmt::format("[RANGE DELETE] [DB: {}] - Target end time must be after start time", dbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[RANGE DELETE] [DB: {}] - DB does not exist", dbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // Walk the span one year-file at a time. Each iteration clamps its
    // segment to [IterEpoch, min(endEpochT, thisYear.LastEpoch)] and always
    // advances IterEpoch past it before the next iteration -- same shape as
    // RangeQuery (see there for why this replaced the old three-branch
    // single-year/multi-year-first/multi-year-last duplication and the bugs
    // that duplication let slip through: a missing final year's file used to
    // spin this forever). fallocate64 still needs a real fd (punching holes
    // isn't an mmap operation), so this segment still opens/closes the file
    // via stdio rather than going through GetOrMapFile.
    unsigned long long int IterEpoch = startEpochT;
    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        std::string TargetFilePath = fmt::format("{}/{}.XSTORE", DbBasePathStr, CurrTs.Year);
        const unsigned long long int SegmentEnd = std::min(endEpochT, static_cast<unsigned long long int>(CurrTs.LastEpoch));

        // IF target YEAR.XSTORE does NOT exist -- a missing year contributes
        // nothing to DeleteCount, same as RangeQuery contributes no entries
        // for a missing year.
        if (std::filesystem::exists(TargetFilePath) == 0) {
            std::string WarningMsg = fmt::format("[RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, CurrTs.Year);
            SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", WarningMsg);
            spdlog::warn(WarningMsg);
            IterEpoch = SegmentEnd + 1;
            continue;
        }

        FILE *BFile = std::fopen(TargetFilePath.c_str(), "rb+");

        const unsigned long long int Offset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RangeCount = SegmentEnd - IterEpoch + 1;

        {
            // Lock file prior to deleting
            FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength() * RangeCount, FileRangeLock::LockType::WRITE);

            // Deallocate data according to N bytes from rowLength
            fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength() * RangeCount);
            DeleteCount += RangeCount;
        }
        // Close file after delete
        std::fclose(BFile);

        IterEpoch = SegmentEnd + 1;
    }

    std::string InfoMsg = fmt::format("[RANGE DELETE] [DB: {}] - Deleted {} items", dbName, DeleteCount);
    SetTaggedStatus(ToReturn.mutable_deletestatus(), "[INFO] ", InfoMsg);
    spdlog::info(InfoMsg);
    return ToReturn;
}
