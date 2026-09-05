#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>

using namespace XStoreWireEncode;

// RANGE INSERT
void XStore::RangeInsert(const XStore_Proto::RangeInsert_REQ *proto_from_client, XStore_Proto::RangeInsert_REP *return_data) {
    // Ensure DB Name string is all capitalized
    std::string db_name = proto_from_client->dbname();
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // IF db directory does not exist
    if (std::filesystem::is_directory(db_base_path_str) == 0) {
        std::string err_msg = fmt::format("[RANGE INSERT] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[RANGE INSERT] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(return_data->mutable_insertstatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return;
    }

    // Assuming incoming data is sorted --> Skip sorting
    // Organize incoming data into year buckets -- flat vector + linear scan
    // (see AppendToYearBucket), not std::map: a single request realistically
    // touches only a handful of distinct years, and RangeInsert's response
    // is just a status string, so bucket iteration order doesn't need to be
    // chronological the way BatchQuery's response does.
    YearBucketMap data_map;

    // Granularity check -- year-bucket via TimeStruct's closed-form calendar
    // calc instead of gmtime_r (see BatchInsert for the same change and why:
    // gmtime_r always treated the raw timestamp as whole seconds regardless
    // of CfgStruct.timeresolution(), silently mis-bucketing MILLISECOND/
    // MICROSECOND DBs). Data here is assumed pre-sorted ascending (see
    // above), so consecutive rows usually share the same year: reuse the
    // last TimeStruct's [FirstEpoch, LastEpoch] window via one comparison
    // instead of rerunning the full calendar decomposition on every row.
    bool have_window = false;
    unsigned short int curr_year = 0;
    int64_t window_last_epoch = 0;
    for (int i = 0; i < proto_from_client->rangeinsert_data_size(); i++) {
        unsigned long long int ts = proto_from_client->rangeinsert_data(i).timestamp();
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
        TimeStruct this_ts{static_cast<time_t>(proto_from_client->rangeinsert_data(indices.front()).timestamp()), cfg_struct.timeresolution()};

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
        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), this_ts.last_epoch - this_ts.first_epoch);
        if (!fd_handle) {
            std::string err_msg = fmt::format("[RANGE INSERT] [DB: {}] - Unable to open {}.XSTORE", db_name, year_key);
            SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            continue;
        }
        int fd = *fd_handle;

        // RegionLength (computed inside the shared helper below) must cover
        // the actual byte span of the writes, not just count*rowLength --
        // that undersizes the lock (and races unprotected writes past its
        // end) whenever the timestamps in this year aren't contiguous.
        // Relies on the same "incoming data is sorted" precondition the rest
        // of this function already assumes, so the last entry in the group
        // carries the highest offset.
        WriteRowGroupToFile(target_file, fd, this_ts.first_epoch, cfg_struct.rowlength(), indices,
            [&](unsigned long long int idx) -> const XStore_Proto::RangeInsert_DATA& { return proto_from_client->rangeinsert_data(idx); });
    }

    // Return insert status
    std::string info_msg = fmt::format("[RANGE INSERT] [DB: {}] - Inserted {} elements successfully", db_name, proto_from_client->rangeinsert_data_size());
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", info_msg);
    spdlog::info(info_msg);
}

// RANGE QUERY (streamed) -- see XStore.h's declaration comment for the
// contract. Mirrors RangeQuery's walk/lock/madvise structure exactly (kept
// as its own loop rather than factored out, matching RangeQuery's existing
// independence from the aggregates' WalkYearSpan -- these two already don't
// share a walker, so this isn't new duplication); the only real difference
// is what happens to each row once it's mapped: EncodeQueryResponseEntry
// writes its bytes straight into chunk_buf instead of building a
// UnaryQuery_REP/RowData/FData tree.
void XStore::RangeQuery(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                                 std::string_view db_name_view, size_t chunk_size_bytes,
                                 const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk) {
    const std::string &db_name = UpperCaseInto(db_name_view);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // querystatus (RangeQuery_REP field 3) semantics match RangeQuery
    // exactly: SetTaggedStatus OVERWRITES, so if several segments in one
    // query each hit a warning, only the last one survives -- tracked here
    // the same way, applied once at the very end rather than per-occurrence.
    bool has_warning = false;
    std::string last_warning;
    auto set_warning = [&](const char *tag, const std::string &body) {
        has_warning = true;
        last_warning = tag;
        last_warning += body;
    };

    // Appends querystatus (field 3, only if set) to the last chunk and
    // emits it. The only exit point of this function -- every return path
    // below funnels through here so emit_chunk is always called at least
    // once (a reply must exist even for a zero-row/error query, or the
    // caller's REQ socket would hang forever waiting for one).
    auto finish = [&](std::string &&chunk_buf) {
        if (has_warning) {
            AppendQueryStatus(chunk_buf, last_warning);
        }
        emit_chunk(std::move(chunk_buf), true);
    };

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - {}.CFG does not exist", db_name, db_name));
        finish(std::string());
        return;
    }

    if (start_epoch_t > end_epoch_t) {
        set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - Target end time must be after start time", db_name));
        finish(std::string());
        return;
    }

    // Chunking bounds THIS SERVER's peak memory (never more than
    // ~chunk_size_bytes live at once) -- it does NOT lift protobuf's real
    // ~2GB ceiling on a single message, because the client still
    // concatenates every chunk back into one buffer before ParseFromArray/
    // ParseFromString (see XStore_Client.h's SendRecv) -- a >2GB
    // reassembled result would corrupt on the CLIENT side even though this
    // server never held more than one chunk at a time. So this guard stays,
    // unchanged from RangeQuery, checked before any chunk is sent (an abort
    // partway through a multipart reply has no clean way to signal
    // "actually, error" to a client already receiving frames).
    if ((end_epoch_t - start_epoch_t) * cfg_struct.rowlength() > 2'000'000'000) {
        set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - Query results is larger than 2GB", db_name));
        finish(std::string());
        return;
    }

    if (std::filesystem::is_directory(db_base_path_str) == 0) {
        set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - DB does not exist", db_name));
        finish(std::string());
        return;
    }

    std::string chunk_buf;
    chunk_buf.reserve(chunk_size_bytes + 4096);

    unsigned long long int iter_epoch = start_epoch_t;
    while (iter_epoch <= end_epoch_t) {
        TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
        const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);
        const unsigned long long int segment_end = std::min(end_epoch_t, static_cast<unsigned long long int>(curr_ts.last_epoch));

        if (std::filesystem::exists(target_file_path) == 0) {
            set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }

        std::shared_ptr<void> mapped_handle = GetOrMapFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
        if (mapped_handle == nullptr) {
            set_warning("[ERROR] ", fmt::format("[RANGE QUERY] [DB: {}] - Unable to open/map {}.XSTORE", db_name, curr_ts.year));
            iter_epoch = segment_end + 1;
            continue;
        }
        void *mapped_base = mapped_handle.get();

        const unsigned long long int first_offset = (iter_epoch - curr_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int region_length = (segment_end - iter_epoch + 1) * cfg_struct.rowlength();

        {
            FileRangeLockGuard guard(target_file_path, first_offset, region_length, FileRangeLock::LockType::READ);

            {
                static const long PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
                char *span_start = static_cast<char*>(mapped_base) + first_offset;
                uintptr_t aligned_addr = reinterpret_cast<uintptr_t>(span_start) & ~(static_cast<uintptr_t>(PAGE_SIZE) - 1);
                size_t align_slop = reinterpret_cast<uintptr_t>(span_start) - aligned_addr;
                madvise(reinterpret_cast<void*>(aligned_addr), region_length + align_slop, MADV_WILLNEED);
            }

            for (unsigned long long int epoch_time = iter_epoch; epoch_time <= segment_end; epoch_time++) {
                unsigned long long int offset = (epoch_time - curr_ts.first_epoch) * cfg_struct.rowlength();
                const char *row_ptr = static_cast<const char*>(mapped_base) + offset;
                EncodeQueryResponseEntry(chunk_buf, epoch_time, row_ptr, cfg_struct.rowlength());

                // Flush whenever a completed row has pushed the buffer to
                // (or past) the target -- never mid-row, so every emitted
                // chunk's bytes still concatenate back into a stream of
                // whole, valid queryresponse entries regardless of where
                // the cut lands. Small queries never hit this at all: one
                // buffer, one emit_chunk call at the end (see finish()) --
                // chunking isn't a separate path, just what this becomes
                // once a query is big enough to reach the target.
                if (chunk_buf.size() >= chunk_size_bytes) {
                    emit_chunk(std::move(chunk_buf), false);
                    chunk_buf.clear();
                    chunk_buf.reserve(chunk_size_bytes + 4096);
                }
            }
        }

        iter_epoch = segment_end + 1;
    }

    finish(std::move(chunk_buf));
}

// RANGE DELETE
XStore_Proto::RangeDelete_REP XStore::RangeDelete(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name_view) {
    // Protobuf obj to be returned
    XStore_Proto::RangeDelete_REP to_return;
    int delete_count = 0;

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use
    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        std::string err_msg = fmt::format("[RANGE DELETE] [DB: {}] - {}.CFG does not exist", db_name, db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    if (start_epoch_t > end_epoch_t) {
        std::string err_msg = fmt::format("[RANGE DELETE] [DB: {}] - Target end time must be after start time", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(db_base_path_str) == 0) {
        std::string err_msg = fmt::format("[RANGE DELETE] [DB: {}] - DB does not exist", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", err_msg);
        spdlog::warn(err_msg);
        return to_return;
    }

    // Walk the span one year-file at a time. Each iteration clamps its
    // segment to [IterEpoch, min(endEpochT, thisYear.last_epoch)] and always
    // advances IterEpoch past it before the next iteration -- same shape as
    // RangeQuery (see there for why this replaced the old three-branch
    // single-year/multi-year-first/multi-year-last duplication and the bugs
    // that duplication let slip through: a missing final year's file used to
    // spin this forever).
    unsigned long long int iter_epoch = start_epoch_t;
    while (iter_epoch <= end_epoch_t) {
        TimeStruct curr_ts(iter_epoch, cfg_struct.timeresolution());
        const std::string &target_file_path = CachedYearFilePath(db_base_path_str, curr_ts.year);
        const unsigned long long int segment_end = std::min(end_epoch_t, static_cast<unsigned long long int>(curr_ts.last_epoch));

        // IF target YEAR.XSTORE does NOT exist -- a missing year contributes
        // nothing to DeleteCount, same as RangeQuery contributes no entries
        // for a missing year.
        if (std::filesystem::exists(target_file_path) == 0) {
            std::string warning_msg = fmt::format("[RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", db_name, curr_ts.year);
            SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", warning_msg);
            spdlog::warn(warning_msg);
            iter_epoch = segment_end + 1;
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
        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file_path, cfg_struct.rowlength(), curr_ts.last_epoch - curr_ts.first_epoch);
        if (!fd_handle) {
            std::string err_msg = fmt::format("[RANGE DELETE] [DB: {}] - Unable to open {}.XSTORE", db_name, curr_ts.year);
            SetTaggedStatus(to_return.mutable_deletestatus(), "[ERROR] ", err_msg);
            spdlog::error(err_msg);
            iter_epoch = segment_end + 1;
            continue;
        }
        int fd = *fd_handle;

        const unsigned long long int offset = (iter_epoch - curr_ts.first_epoch) * cfg_struct.rowlength();
        const unsigned long long int range_count = segment_end - iter_epoch + 1;

        {
            // Lock file prior to deleting
            FileRangeLockGuard guard(target_file_path, offset, cfg_struct.rowlength() * range_count, FileRangeLock::LockType::WRITE);

            // Deallocate data according to N bytes from rowLength
            fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfg_struct.rowlength() * range_count);
            delete_count += range_count;
        }

        iter_epoch = segment_end + 1;
    }

    std::string info_msg = fmt::format("[RANGE DELETE] [DB: {}] - Deleted {} items", db_name, delete_count);
    SetTaggedStatus(to_return.mutable_deletestatus(), "[INFO] ", info_msg);
    spdlog::info(info_msg);
    return to_return;
}
