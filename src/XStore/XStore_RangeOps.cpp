#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>

// RANGE INSERT
void XStore::RangeInsert(const XStore_Proto::RangeInsert_REQ *protoFromClient, XStore_Proto::RangeInsert_REP *returnData) {
    // Ensure DB Name string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(DbName);

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
    // Organize incoming data into year buckets -- flat vector + linear scan
    // (see AppendToYearBucket), not std::map: a single request realistically
    // touches only a handful of distinct years, and RangeInsert's response
    // is just a status string, so bucket iteration order doesn't need to be
    // chronological the way BatchQuery's response does.
    YearBucketMap DataMap;

    // Granularity check -- year-bucket via TimeStruct's closed-form calendar
    // calc instead of gmtime_r (see BatchInsert for the same change and why:
    // gmtime_r always treated the raw timestamp as whole seconds regardless
    // of CfgStruct.timeresolution(), silently mis-bucketing MILLISECOND/
    // MICROSECOND DBs). Data here is assumed pre-sorted ascending (see
    // above), so consecutive rows usually share the same year: reuse the
    // last TimeStruct's [FirstEpoch, LastEpoch] window via one comparison
    // instead of rerunning the full calendar decomposition on every row.
    bool HaveWindow = false;
    unsigned short int CurrYear = 0;
    int64_t WindowLastEpoch = 0;
    for (int i = 0; i < protoFromClient->rangeinsert_data_size(); i++) {
        unsigned long long int Ts = protoFromClient->rangeinsert_data(i).timestamp();
        if (!HaveWindow || static_cast<int64_t>(Ts) > WindowLastEpoch) {
            TimeStruct RowTs(static_cast<time_t>(Ts), CfgStruct.timeresolution());
            CurrYear = RowTs.Year;
            WindowLastEpoch = RowTs.LastEpoch;
            HaveWindow = true;
        }
        AppendToYearBucket(DataMap, CurrYear, static_cast<unsigned long long int>(i));
    }

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        const std::vector<unsigned long long int> &Indices = Pair.second;
        const std::string &TargetFile = CachedYearFilePath(DbBasePathStr, YearKey);

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
        // Held through WriteRowGroupToFile below so a concurrent eviction on
        // another thread can't close this fd mid-write -- see FdShard's
        // comment in XStore.h.
        std::shared_ptr<int> FdHandle = GetOrOpenFile(TargetFile, CfgStruct.rowlength(), ThisTs.LastEpoch - ThisTs.FirstEpoch);
        if (!FdHandle) {
            std::string ErrMsg = fmt::format("[RANGE INSERT] [DB: {}] - Unable to open {}.XSTORE", DbName, YearKey);
            SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            continue;
        }
        int Fd = *FdHandle;

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
void XStore::RangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string_view dbNameView, XStore_Proto::RangeQuery_REP *returnData) {
    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

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
        const std::string &TargetFilePath = CachedYearFilePath(DbBasePathStr, CurrTs.Year);
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
        // Held through the read loop below so a concurrent eviction on
        // another thread can't munmap() this mapping mid-read -- see
        // MmapShard's comment in XStore.h.
        std::shared_ptr<void> MappedHandle = GetOrMapFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
        if (MappedHandle == nullptr) {
            std::string ErrMsg = fmt::format("[RANGE QUERY] [DB: {}] - Unable to open/map {}.XSTORE", dbName, CurrTs.Year);
            SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            IterEpoch = SegmentEnd + 1;
            continue;
        }
        void *MappedBase = MappedHandle.get();

        const unsigned long long int FirstOffset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = (SegmentEnd - IterEpoch + 1) * CfgStruct.rowlength();

        {
            // Lock file prior to reading -- READ lock scoped to exactly this
            // segment's byte range, not the whole file, so concurrent reads
            // of disjoint regions (or other years' files entirely) never
            // wait on this one.
            FileRangeLockGuard Guard(TargetFilePath, FirstOffset, RegionLength, FileRangeLock::LockType::READ);

            // Prefetch the queried span in one shot instead of relying on
            // lazy page-faulting (one fault per first-touched page as the
            // loop below walks the range) plus whatever the kernel's default
            // readahead window happens to cover. madvise() requires a
            // page-aligned addr/length, so align down to the containing page
            // and extend the length by that slop.
            {
                static const long PageSize = sysconf(_SC_PAGE_SIZE);
                char *SpanStart = static_cast<char*>(MappedBase) + FirstOffset;
                uintptr_t AlignedAddr = reinterpret_cast<uintptr_t>(SpanStart) & ~(static_cast<uintptr_t>(PageSize) - 1);
                size_t AlignSlop = reinterpret_cast<uintptr_t>(SpanStart) - AlignedAddr;
                madvise(reinterpret_cast<void*>(AlignedAddr), RegionLength + AlignSlop, MADV_WILLNEED);
            }

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
XStore_Proto::RangeDelete_REP XStore::RangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string_view dbNameView) {
    // Protobuf obj to be returned
    XStore_Proto::RangeDelete_REP ToReturn;
    int DeleteCount = 0;

    // Ensure dbName string is all capitalized
    const std::string &dbName = UpperCaseInto(dbNameView);

    // Base directory of a DB
    const std::string &DbBasePathStr = CachedDbBasePath(dbName);

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
    // spin this forever).
    unsigned long long int IterEpoch = startEpochT;
    while (IterEpoch <= endEpochT) {
        TimeStruct CurrTs(IterEpoch, CfgStruct.timeresolution());
        const std::string &TargetFilePath = CachedYearFilePath(DbBasePathStr, CurrTs.Year);
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

        // Cached fd instead of a fresh fopen()/fclose() per year-segment --
        // fallocate64() just needs a plain fd (punching holes isn't an mmap
        // operation, so GetOrMapFile's mmap cache genuinely doesn't apply
        // here, but the separate fd cache does and hands one back with no
        // FILE*/fileno() layer needed). Existence was already confirmed
        // above, so O_CREAT/ftruncate here are no-ops. Held through the
        // fallocate64() call below so a concurrent eviction on another
        // thread can't close this fd mid-delete -- see FdShard's comment in
        // XStore.h.
        std::shared_ptr<int> FdHandle = GetOrOpenFile(TargetFilePath, CfgStruct.rowlength(), CurrTs.LastEpoch - CurrTs.FirstEpoch);
        if (!FdHandle) {
            std::string ErrMsg = fmt::format("[RANGE DELETE] [DB: {}] - Unable to open {}.XSTORE", dbName, CurrTs.Year);
            SetTaggedStatus(ToReturn.mutable_deletestatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            IterEpoch = SegmentEnd + 1;
            continue;
        }
        int Fd = *FdHandle;

        const unsigned long long int Offset = (IterEpoch - CurrTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RangeCount = SegmentEnd - IterEpoch + 1;

        {
            // Lock file prior to deleting
            FileRangeLockGuard Guard(TargetFilePath, Offset, CfgStruct.rowlength() * RangeCount, FileRangeLock::LockType::WRITE);

            // Deallocate data according to N bytes from rowLength
            fallocate64(Fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength() * RangeCount);
            DeleteCount += RangeCount;
        }

        IterEpoch = SegmentEnd + 1;
    }

    std::string InfoMsg = fmt::format("[RANGE DELETE] [DB: {}] - Deleted {} items", dbName, DeleteCount);
    SetTaggedStatus(ToReturn.mutable_deletestatus(), "[INFO] ", InfoMsg);
    spdlog::info(InfoMsg);
    return ToReturn;
}
