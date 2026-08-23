#include "include/XStore/XStore_Internal.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
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
constexpr std::ptrdiff_t PARALLEL_SORT_THRESHOLD = 10'000;

template <typename Iterator, typename Compare>
void SortBatch(Iterator begin, Iterator end, Compare comp) {
    if (std::distance(begin, end) >= PARALLEL_SORT_THRESHOLD) {
        std::sort(std::execution::par_unseq, begin, end, comp);
    } else {
        std::sort(begin, end, comp);
    }
}

}  // namespace

// BATCH INSERT
void XStore::BatchInsert(XStore_Proto::BatchInsert_REQ *proto_from_client, XStore_Proto::BatchInsert_REP *return_data) {
    // Ensure DB Name string is all capitalized
    std::string db_name = proto_from_client->dbname();
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. This also
    // covers "DB does not exist": GetOrLoadCfg stats the .CFG file itself on
    // a cache miss, so a separate upfront is_directory() check on every call
    // (cache hit or not) would just be a redundant stat -- dropped, same fix
    // already applied to Unary ops.
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[BATCH INSERT] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Assuming incoming data is unsorted -> ascending sort. Physically
    // reorders batchinsert_data() in place (tried sorting a separate index
    // array instead, to avoid mutating the caller's row order -- benchmarked
    // SLOWER: RepeatedPtrField::sort() only swaps pointers, not the
    // underlying BatchInsert_DATA objects, so the physical sort was never
    // the expensive part; the index-array version instead made every
    // downstream getEntry(idx) lookup in WriteRowGroupToFile -- called
    // multiple times per row -- a random-order access into the
    // ~1.6MB/200k-row pointer array instead of a sequential scan, losing
    // hardware prefetch. Reverted after measuring).
    SortBatch(proto_from_client->mutable_batchinsert_data()->begin(), proto_from_client->mutable_batchinsert_data()->end(),
              [](const XStore_Proto::BatchInsert_DATA& a, const XStore_Proto::BatchInsert_DATA &b) {
                    return a.timestamp() < b.timestamp();
    });

    // Organize incoming data into year buckets -- flat vector + linear scan
    // (see AppendToYearBucket), not std::map: a single call realistically
    // touches only a handful of distinct years.
    YearBucketMap data_map;

    // Granularity check -- year-bucket via TimeStruct's closed-form calendar
    // calc instead of gmtime_r. Two reasons: (1) gmtime_r always treated the
    // raw timestamp as whole seconds regardless of CfgStruct.timeresolution(),
    // silently mis-bucketing MILLISECOND/MICROSECOND DBs into the wrong
    // year's file -- TimeStruct is resolution-aware, same as every other use
    // of it in this class. (2) Batch is sorted ascending above, so
    // consecutive rows usually share the same year: reuse the last
    // TimeStruct's [FirstEpoch, LastEpoch] window via one comparison instead
    // of rerunning the full calendar decomposition on every row -- only
    // rebuilt on an actual year crossing.
    bool have_window = false;
    unsigned short int curr_year = 0;
    int64_t window_last_epoch = 0;
    for (int i = 0; i < proto_from_client->batchinsert_data_size(); i++) {
        unsigned long long int ts = proto_from_client->batchinsert_data(i).timestamp();
        if (!have_window || static_cast<int64_t>(ts) > window_last_epoch) {
            TimeStruct row_ts(static_cast<time_t>(ts), cfg_struct.timeresolution());
            curr_year = row_ts.year;
            window_last_epoch = row_ts.last_epoch;
            have_window = true;
        }
        AppendToYearBucket(data_map, curr_year, static_cast<unsigned long long int>(i));
    }

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;
        const std::string &target_file = CachedYearFilePath(db_base_path_str, year_key);

        // Grab first inserting element for TimeStruct purposes
        //  Get length of time to truncate
        TimeStruct this_ts{static_cast<time_t>(proto_from_client->batchinsert_data(indices.front()).timestamp()), cfg_struct.timeresolution()};

        // Cached fd for this (dbName, year) file -- same fd cache UnaryInsert
        // uses (see GetOrOpenFile). Creates+ftruncates on first use, so the
        // separate open/ftruncate branch this used to have is gone. Every
        // row below becomes a pwrite() rather than a memcpy into a
        // PROT_WRITE mapping -- see WriteRowGroupToFile for why.
        // Held through WriteRowGroupToFile below so a concurrent eviction on
        // another thread can't close this fd mid-write -- see FdShard's
        // comment in XStore.h.
        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), this_ts.last_epoch - this_ts.first_epoch);
        if (!fd_handle) {
            std::string err_msg = fmt::format("[BATCH INSERT] [DB: {}] - Unable to open {}.XSTORE", db_name, year_key);
            SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            continue;
        }
        int fd = *fd_handle;

        // Single lock spanning this year-group's full min-to-max offset span
        // instead of one lock per row -- Indices is sorted ascending (the
        // whole batch was sorted by timestamp above), so front()/back() give
        // the span directly; offset is monotonic in timestamp so this is the
        // same "over-lock the gaps between scattered rows" trick RangeInsert
        // uses too, hence the shared helper.
        WriteRowGroupToFile(target_file, fd, this_ts.first_epoch, cfg_struct.rowlength(), indices,
            [&](unsigned long long int idx) -> const XStore_Proto::BatchInsert_DATA& { return proto_from_client->batchinsert_data(idx); });
    }

    // Return insert status
    std::string success_msg = fmt::format("[BATCH INSERT] [DB: {}] - Inserted {} elements successfully", db_name, proto_from_client->batchinsert_data_size());
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
    spdlog::info(success_msg);
}

// BATCH QUERY
void XStore::BatchQuery(XStore_Proto::BatchQuery_REQ *proto_from_client, XStore_Proto::BatchQuery_REP *return_data, bool sort_time) {
    // Ensure dbName string is all capitalized
    std::string db_name = proto_from_client->dbname();
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[BATCH QUERY] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(return_data->mutable_querystatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // IF query range > 2GB limit of Protobuf or Empty
    if ((proto_from_client->timestamp_size() * cfg_struct.rowlength()) > 2'000'000'000 || proto_from_client->timestamp_size() < 0) {
        std::string err_msg = fmt::format("[BATCH QUERY] [DB: {}] - Query results is larger than 2GB or empty", db_name);
        SetTaggedStatus(return_data->mutable_querystatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    if (sort_time)
    {
        SortBatch(proto_from_client->mutable_timestamp()->begin(), proto_from_client->mutable_timestamp()->end(),
                      [](const unsigned long long int& a, const unsigned long long int& b) {
                        return a < b;
        });
    }

    // Organize incoming batch query into year buckets -- flat vector + linear
    // scan (see AppendToYearBucket), not std::map.
    YearBucketMap data_map;

    // Granularity check -- same TimeStruct window-reuse BatchInsert uses
    // (see there for the full rationale), gated on SortTime: the reuse trick
    // only checks an upper bound (has this row crossed into a later year
    // than the current window's LastEpoch), never a lower bound, so it's
    // only valid when consecutive rows are non-decreasing -- guaranteed when
    // SortTime just sorted them, NOT guaranteed otherwise (see
    // BatchQuerySortTimeFalseWithUnsortedTimestampsRoundTrips, which sends
    // out-of-order timestamps with SortTime=false and still expects correct
    // per-row bucketing). "!SortTime ||" makes the rebuild condition always
    // true in that case, so behavior there is unchanged from before.
    bool have_window = false;
    unsigned short int curr_year = 0;
    int64_t window_last_epoch = 0;
    for (int i = 0; i < proto_from_client->timestamp_size(); i++) {
        unsigned long long int curr_time = proto_from_client->timestamp(i);
        if (!sort_time || !have_window || static_cast<int64_t>(curr_time) > window_last_epoch) {
            TimeStruct curr_ts(curr_time, cfg_struct.timeresolution());
            curr_year = curr_ts.year;
            window_last_epoch = curr_ts.last_epoch;
            have_window = true;
        }
        AppendToYearBucket(data_map, curr_year, static_cast<unsigned long long int>(i));
    }

    // Keep the response chronological across years -- matches the old
    // std::map-based version's iteration order, which was always
    // year-ascending regardless of request/bucket-creation order (relevant
    // when SortTime is false, where buckets can otherwise be created in
    // request order instead of year order).
    std::sort(data_map.begin(), data_map.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;
        const std::string &target_file = CachedYearFilePath(db_base_path_str, year_key);

        // IF target file does NOT exist -- checked explicitly because
        // GetOrMapFile below opens with O_CREAT and would otherwise silently
        // create+map an empty file for a query that should just report no
        // data. IsFdCached short-circuits the stat() syscall on a warm
        // cache, same as UnaryQuery.
        if (!IsFdCached(target_file) && std::filesystem::exists(target_file) == 0) {
            // Skip to next iteration
            std::string warn_msg = fmt::format("[BATCH QUERY] [DB: {}] - {}.XSTORE does not exist", db_name, year_key);
            SetTaggedStatus(return_data->mutable_querystatus(), "[WARNING] ", warn_msg);
            spdlog::warn(warn_msg);
            continue;
        }

        TimeStruct this_ts{static_cast<time_t>(proto_from_client->timestamp(indices.front())), cfg_struct.timeresolution()};

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
        //
        // The fd itself, though, goes through the same shared fd cache
        // Unary/BatchInsert use (see GetOrOpenFile) instead of a fresh
        // open()/close() every call -- existence was already confirmed
        // above, so O_CREAT there is a no-op, and any ftruncate is a no-op
        // too since the file is never smaller than the size this computes.
        // A warm cache turns "open syscall + N preads + close syscall" into
        // "one shard-mutex lookup + N preads", and the fd is left open for
        // the next call (or Insert traffic on the same file) to reuse.
        // Held through the pread() loop below so a concurrent eviction on
        // another thread can't close this fd mid-read -- see FdShard's
        // comment in XStore.h.
        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), this_ts.last_epoch - this_ts.first_epoch);
        if (!fd_handle) {
            std::string err_msg = fmt::format("[BATCH QUERY] [DB: {}] - Unable to open {}.XSTORE", db_name, year_key);
            SetTaggedStatus(return_data->mutable_querystatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            continue;
        }
        int fd = *fd_handle;

        // Single lock spanning this year-group's min-to-max offset span
        // instead of one lock per row. Found via a scan (not just
        // front()/back()) since Indices is only guaranteed sorted when
        // SortTime is true -- BatchInsert always sorts, BatchQuery doesn't
        // have to.
        auto [MinIt, MaxIt] = std::minmax_element(indices.begin(), indices.end(),
            [&](unsigned long long int a, unsigned long long int b) {
                return proto_from_client->timestamp(a) < proto_from_client->timestamp(b);
        });
        const unsigned long long int min_offset = (proto_from_client->timestamp(*MinIt) - this_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int max_offset = (proto_from_client->timestamp(*MaxIt) - this_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int region_length = (max_offset + cfg_struct.rowlength()) - min_offset;

        FileRangeLockGuard guard(target_file, min_offset, region_length, FileRangeLock::LockType::READ);

        // thread_local + grow-only: avoids a fresh heap allocation per
        // year-group per call, same pattern UnaryInsert/UnaryQuery's RowBuf
        // and WriteRowGroupToFile's RunBuf already use.
        thread_local std::vector<char> buffer_in;
        if (buffer_in.size() < cfg_struct.rowlength()) {
            buffer_in.resize(cfg_struct.rowlength());
        }
        for (unsigned long long int idx : indices) {
            unsigned long long int timestamp = proto_from_client->timestamp(idx);
            unsigned long long int offset = (timestamp - this_ts.first_epoch) * cfg_struct.rowlength();

            pread(fd, buffer_in.data(), cfg_struct.rowlength(), static_cast<off_t>(offset));

            // IF fetched data contains nothing
            //  -> Skip to next iteration. BufferIn is a fixed-width binary
            //  buffer (not a C string), so strlen() would over-read past its
            //  end whenever a real row's bytes fill the whole slot with no
            //  zero byte in it; checking the first byte (a hole is always
            //  fully zeroed) is what the aggregate scan kernels do too.
            if (buffer_in[0] == 0) {
                continue;
            }

            XStore_Proto::UnaryQuery_REP *curr_entry = return_data->add_queryresponse();
            curr_entry->set_timestamp(timestamp);
            curr_entry->mutable_data()->ParseFromArray(buffer_in.data(), cfg_struct.rowlength());
        }
    }
}

// BATCH DELETE
XStore_Proto::BatchDelete_REP XStore::BatchDelete(XStore_Proto::BatchDelete_REQ *proto_from_client) {
    // Protobuf obj to be returned
    XStore_Proto::BatchDelete_REP to_return;
    std::string db_name = proto_from_client->dbname();
    int delete_count = 0;

    // Ensure dbName string is all capitalized
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[BATCH DELETE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(db_base_path_str) == 0) {
        std::string err_msg = fmt::format("[BATCH DELETE] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    SortBatch(proto_from_client->mutable_timestamp()->begin(), proto_from_client->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch delete timestamps into year buckets -- flat
    // vector + linear scan (see AppendToYearBucket), not std::map.
    YearBucketMap data_map;

    // Granularity check -- TimeStruct instead of gmtime_r (gmtime_r always
    // treated the raw timestamp as whole seconds regardless of
    // CfgStruct.timeresolution(), silently mis-bucketing MILLISECOND/
    // MICROSECOND DBs into the wrong year -- same bug class TimeStruct
    // already fixed for Insert/Query, just never carried over here), plus
    // the same window-reuse BatchInsert uses. Unconditionally safe here
    // (unlike BatchQuery's SortTime-gated version) since this batch is
    // always sorted ascending just above, with no unsorted-input toggle.
    bool have_window = false;
    unsigned short int curr_year = 0;
    int64_t window_last_epoch = 0;
    for (int i = 0; i < proto_from_client->timestamp_size(); i++) {
        unsigned long long int curr_time = proto_from_client->timestamp(i);
        if (!have_window || static_cast<int64_t>(curr_time) > window_last_epoch) {
            TimeStruct row_ts(static_cast<time_t>(curr_time), cfg_struct.timeresolution());
            curr_year = row_ts.year;
            window_last_epoch = row_ts.last_epoch;
            have_window = true;
        }
        AppendToYearBucket(data_map, curr_year, static_cast<unsigned long long int>(i));
    }

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;
        const std::string &target_file = CachedYearFilePath(db_base_path_str, year_key);

        // IF target file does NOT exist
        if (std::filesystem::exists(target_file) == 0) {
            std::string warning_msg = fmt::format("[BATCH DELETE] [DB: {}] - {}.XSTORE does not exist", db_name, year_key);
            SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", warning_msg);
            spdlog::warn(warning_msg);
            continue;
        }

        TimeStruct this_ts{static_cast<time_t>(proto_from_client->timestamp(indices.front())), cfg_struct.timeresolution()};

        // Cached fd instead of a fresh fopen()/fclose() per year-group --
        // fallocate64() just needs a plain fd (punching holes isn't an mmap
        // operation), and GetOrOpenFile already hands one back with no
        // FILE*/fileno() layer needed. Existence was already confirmed
        // above, so O_CREAT/ftruncate here are no-ops, same reasoning as
        // BatchQuery's fd-cache use. Held through the fallocate64() loop
        // below so a concurrent eviction on another thread can't close this
        // fd mid-delete -- see FdShard's comment in XStore.h.
        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), this_ts.last_epoch - this_ts.first_epoch);
        if (!fd_handle) {
            std::string err_msg = fmt::format("[BATCH DELETE] [DB: {}] - Unable to open {}.XSTORE", db_name, year_key);
            SetTaggedStatus(to_return.mutable_deletestatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            continue;
        }
        int fd = *fd_handle;

        // Single lock spanning this year-group's min-to-max offset span
        // instead of one lock per row -- Indices is sorted ascending (the
        // whole batch was sorted by timestamp above), so front()/back() give
        // the span directly (same trick as BatchInsert). The fallocate64
        // calls below still can't be coalesced into one, though: timestamps
        // here are randomly distributed, not contiguous, so punching one big
        // range would also deallocate OTHER rows' data sitting in the gaps
        // between the requested points -- unlike a lock, which is harmless
        // to over-cover.
        const unsigned long long int first_offset = (proto_from_client->timestamp(indices.front()) - this_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int last_offset = (proto_from_client->timestamp(indices.back()) - this_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int region_length = (last_offset + cfg_struct.rowlength()) - first_offset;

        {
            // Lock prior to file OP
            FileRangeLockGuard guard(target_file, first_offset, region_length, FileRangeLock::LockType::WRITE);

            for (unsigned long long int idx : indices) {
                unsigned long long int offset = (proto_from_client->timestamp(idx) - this_ts.first_epoch) * cfg_struct.rowlength();

                // Deallocate data according to N bytes from rowLength
                fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfg_struct.rowlength());
                delete_count++;
            }
        }
    }
    std::string info_msg = fmt::format("[BATCH DELETE] [DB: {}] - Deleted {} items", db_name, delete_count);
    SetTaggedStatus(to_return.mutable_deletestatus(), "[INFO] ", info_msg);
    spdlog::info(info_msg);
    return to_return;
}
