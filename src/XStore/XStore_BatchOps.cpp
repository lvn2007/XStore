#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <ctime>
#include <execution>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>

namespace {

// std::execution::par_unseq dispatches work across a thread pool -- worth it
// once a batch is big enough that sorting it in parallel outweighs that
// dispatch/join overhead, but for a small batch (the common case for most
// callers) the overhead alone can cost more than a plain sequential sort
// would take to run the whole thing. Used by Batch Insert/Query/Delete's
// "sort incoming timestamps first" step.
constexpr std::ptrdiff_t ParallelSortThreshold = 10'000;

template <typename Iterator, typename Compare>
void SortBatch(Iterator begin, Iterator end, Compare comp) {
    if (std::distance(begin, end) >= ParallelSortThreshold) {
        std::sort(std::execution::par_unseq, begin, end, comp);
    } else {
        std::sort(begin, end, comp);
    }
}

}  // namespace

// BATCH INSERT
void XStore::BatchInsert(XStore_Proto::BatchInsert_REQ *protoFromClient, XStore_Proto::BatchInsert_REP *returnData) {
    // Ensure DB Name string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(DbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[BATCH INSERT] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[BATCH INSERT] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(returnData->mutable_insertstatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Assuming incoming data is unsorted
    //  -> Ascending sort
    SortBatch(protoFromClient->mutable_batchinsert_data()->begin(), protoFromClient->mutable_batchinsert_data()->end(),
              [](const XStore_Proto::BatchInsert_DATA& a, const XStore_Proto::BatchInsert_DATA &b) {
                    return a.timestamp() < b.timestamp();
    });

    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
        CurrTime = protoFromClient->batchinsert_data(i).timestamp();
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
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->batchinsert_data(Indices.front()).timestamp()), CfgStruct.timeresolution()};

        // Cached PROT_WRITE mapping for this (dbName, year) file -- same mmap
        // LRU cache UnaryInsert uses (see GetOrMapFile). Creates+ftruncates
        // on first use, so the separate open/ftruncate branch this used to
        // have is gone -- GetOrMapFile does it under its own lock. Every row
        // below becomes a direct memcpy into an already-resident page
        // instead of an fseek+fwrite syscall pair.
        void *MappedBase = GetOrMapFile(TargetFile, CfgStruct.rowlength(), ThisTs.LastEpoch - ThisTs.FirstEpoch);
        if (MappedBase == nullptr) {
            std::string ErrMsg = fmt::format("[BATCH INSERT] [DB: {}] - Unable to open/map {}.XSTORE", DbName, YearKey);
            SetTaggedStatus(returnData->mutable_insertstatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            continue;
        }

        // Single lock spanning this year-group's full min-to-max offset span
        // instead of one lock per row -- Indices is sorted ascending (the
        // whole batch was sorted by timestamp above), so front()/back() give
        // the span directly; offset is monotonic in timestamp so this is the
        // same "over-lock the gaps between scattered rows" trick RangeInsert
        // uses too, hence the shared helper.
        WriteRowGroupToMappedFile(TargetFile, MappedBase, ThisTs.FirstEpoch, CfgStruct.rowlength(), Indices,
            [&](unsigned long long int idx) -> const XStore_Proto::BatchInsert_DATA& { return protoFromClient->batchinsert_data(idx); });
    }

    // Return insert status
    std::string SuccessMsg = fmt::format("[BATCH INSERT] [DB: {}] - Inserted {} elements successfully", DbName, protoFromClient->batchinsert_data_size());
    SetTaggedStatus(returnData->mutable_insertstatus(), "[INFO] ", SuccessMsg);
    spdlog::info(SuccessMsg);
}

// BATCH QUERY
void XStore::BatchQuery(XStore_Proto::BatchQuery_REQ *protoFromClient, XStore_Proto::BatchQuery_REP *returnData, bool SortTime) {
    // Ensure dbName string is all capitalized
    std::string DbName = protoFromClient->dbname();
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(DbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[BATCH QUERY] [DB: {}] - {}.CFG does not exist", DbName, DbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // IF query range > 2GB limit of Protobuf or Empty
    if ((protoFromClient->timestamp_size() * CfgStruct.rowlength()) > 2'000'000'000 || protoFromClient->timestamp_size() < 0) {
        std::string ErrMsg = fmt::format("[BATCH QUERY] [DB: {}] - Query results is larger than 2GB or empty", DbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[BATCH QUERY] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    if (SortTime)
    {
        SortBatch(protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
                      [](const unsigned long long int& a, const unsigned long long int& b) {
                        return a < b;
        });
    }

    // Organize incoming batch query into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    unsigned long long int CurrTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        CurrTime = protoFromClient->timestamp(i);
        struct TimeStruct CurrTs(CurrTime, CfgStruct.timeresolution());
        DataMap[CurrTs.Year].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        const std::vector<unsigned long long int> &Indices = Pair.second;
        std::string TargetFile = fmt::format("{}/{}.XSTORE", DbBasePathStr, YearKey);

        // IF target file does NOT exist -- checked explicitly because
        // GetOrMapFile below opens with O_CREAT and would otherwise silently
        // create+map an empty file for a query that should just report no data.
        if (std::filesystem::exists(TargetFile) == 0) {
            // Skip to next iteration
            std::string WarnMsg = fmt::format("[BATCH QUERY] [DB: {}] - {}.XSTORE does not exist", DbName, YearKey);
            SetTaggedStatus(returnData->mutable_querystatus(), "[WARNING] ", WarnMsg);
            spdlog::warn(WarnMsg);
            continue;
        }

        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->timestamp(Indices.front())), CfgStruct.timeresolution()};

        // BatchQuery deliberately does NOT go through GetOrMapFile/mmap here
        // (unlike Unary/Batch/RangeInsert and RangeQuery, which all share
        // that cache). Those all walk a file in offset order -- sequential
        // or nearly so -- where mmap's page-fault-on-touch model plus the
        // kernel's default readahead is a win. BatchQuery's row order is
        // request order, i.e. scattered across the year's full offset span,
        // and mmap random access degrades badly there: each page fault is
        // costlier than a pread() of the same bytes, readahead prefetches
        // pages this loop will never touch, and per-call madvise(MADV_RANDOM)
        // to suppress that isn't free either once a lot of the mapping is
        // resident. A plain pread() per row sidesteps all of it.
        int Fd = open(TargetFile.c_str(), O_RDONLY);
        if (Fd < 0) {
            std::string ErrMsg = fmt::format("[BATCH QUERY] [DB: {}] - Unable to open {}.XSTORE", DbName, YearKey);
            SetTaggedStatus(returnData->mutable_querystatus(), "[ERROR] ", ErrMsg);
            spdlog::error(ErrMsg);
            continue;
        }

        // Single lock spanning this year-group's min-to-max offset span
        // instead of one lock per row. Found via a scan (not just
        // front()/back()) since Indices is only guaranteed sorted when
        // SortTime is true -- BatchInsert always sorts, BatchQuery doesn't
        // have to.
        auto [MinIt, MaxIt] = std::minmax_element(Indices.begin(), Indices.end(),
            [&](unsigned long long int a, unsigned long long int b) {
                return protoFromClient->timestamp(a) < protoFromClient->timestamp(b);
        });
        const unsigned long long int MinOffset = (protoFromClient->timestamp(*MinIt) - ThisTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int MaxOffset = (protoFromClient->timestamp(*MaxIt) - ThisTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = (MaxOffset + CfgStruct.rowlength()) - MinOffset;

        FileRangeLockGuard Guard(TargetFile, MinOffset, RegionLength, FileRangeLock::LockType::READ);

        std::vector<char> BufferIn(CfgStruct.rowlength());
        for (unsigned long long int Idx : Indices) {
            unsigned long long int Timestamp = protoFromClient->timestamp(Idx);
            unsigned long long int Offset = (Timestamp - ThisTs.FirstEpoch) * CfgStruct.rowlength();

            pread(Fd, BufferIn.data(), CfgStruct.rowlength(), static_cast<off_t>(Offset));

            // IF fetched data contains nothing
            //  -> Skip to next iteration. BufferIn is a fixed-width binary
            //  buffer (not a C string), so strlen() would over-read past its
            //  end whenever a real row's bytes fill the whole slot with no
            //  zero byte in it; checking the first byte (a hole is always
            //  fully zeroed) is what the aggregate scan kernels do too.
            if (BufferIn[0] == 0) {
                continue;
            }

            XStore_Proto::UnaryQuery_REP *CurrEntry = returnData->add_queryresponse();
            CurrEntry->set_timestamp(Timestamp);
            CurrEntry->mutable_data()->ParseFromArray(BufferIn.data(), CfgStruct.rowlength());
        }

        close(Fd);
    }
}

// BATCH DELETE
XStore_Proto::BatchDelete_REP XStore::BatchDelete(XStore_Proto::BatchDelete_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::BatchDelete_REP ToReturn;
    std::string DbName = protoFromClient->dbname();
    int DeleteCount = 0;

    // Ensure dbName string is all capitalized
    ToUpperCase(DbName.begin(), DbName.end());

    // Base directory of a DB
    std::string DbBasePathStr = DbBasePath(DbName);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct CfgStruct;
    if (!GetOrLoadCfg(DbName, DbBasePathStr, CfgStruct)) {
        std::string ErrMsg = fmt::format("[BATCH DELETE] [DB: {}] - {}.CFG does not exist", DbName, DbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(DbBasePathStr) == 0) {
        std::string ErrMsg = fmt::format("[BATCH DELETE] [DB: {}] - DB does not exist", DbName);
        SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", ErrMsg);
        spdlog::warn(ErrMsg);
        return ToReturn;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    SortBatch(protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch delete timestamp into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t CurrTime;
    tm EpochStruct;
    std::map<unsigned short int, std::vector<unsigned long long int>> DataMap;

    // Granularity check
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        CurrTime = protoFromClient->timestamp(i);
        gmtime_r(&CurrTime, &EpochStruct);
        DataMap[EpochStruct.tm_year + 1900].push_back(i);
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& Pair : DataMap) {
        unsigned short int YearKey = Pair.first;
        const std::vector<unsigned long long int> &Indices = Pair.second;
        std::string TargetFile = fmt::format("{}/{}.XSTORE", DbBasePathStr, YearKey);

        // IF target file does NOT exist
        if (std::filesystem::exists(TargetFile) == 0) {
            std::string WarningMsg = fmt::format("[BATCH DELETE] [DB: {}] - {}.XSTORE does not exist", DbName, YearKey);
            SetTaggedStatus(ToReturn.mutable_deletestatus(), "[WARNING] ", WarningMsg);
            spdlog::warn(WarningMsg);
            continue;
        }

        FILE *BFile = std::fopen(TargetFile.c_str(), "rb+");
        TimeStruct ThisTs{static_cast<time_t>(protoFromClient->timestamp(Indices.front())), CfgStruct.timeresolution()};

        // Single lock spanning this year-group's min-to-max offset span
        // instead of one lock per row -- Indices is sorted ascending (the
        // whole batch was sorted by timestamp above), so front()/back() give
        // the span directly (same trick as BatchInsert). The fallocate64
        // calls below still can't be coalesced into one, though: timestamps
        // here are randomly distributed, not contiguous, so punching one big
        // range would also deallocate OTHER rows' data sitting in the gaps
        // between the requested points -- unlike a lock, which is harmless
        // to over-cover.
        const unsigned long long int FirstOffset = (protoFromClient->timestamp(Indices.front()) - ThisTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int LastOffset = (protoFromClient->timestamp(Indices.back()) - ThisTs.FirstEpoch) * CfgStruct.rowlength();
        const unsigned long long int RegionLength = (LastOffset + CfgStruct.rowlength()) - FirstOffset;

        {
            // Lock prior to file OP
            FileRangeLockGuard Guard(TargetFile, FirstOffset, RegionLength, FileRangeLock::LockType::WRITE);

            for (unsigned long long int Idx : Indices) {
                unsigned long long int Offset = (protoFromClient->timestamp(Idx) - ThisTs.FirstEpoch) * CfgStruct.rowlength();

                // Deallocate data according to N bytes from rowLength
                fallocate64(fileno(BFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, Offset, CfgStruct.rowlength());
                DeleteCount++;
            }
        }

        // Close file & Release lock after read
        std::fclose(BFile);
    }
    std::string InfoMsg = fmt::format("[BATCH DELETE] [DB: {}] - Deleted {} items", DbName, DeleteCount);
    SetTaggedStatus(ToReturn.mutable_deletestatus(), "[INFO] ", InfoMsg);
    spdlog::info(InfoMsg);
    return ToReturn;
}
