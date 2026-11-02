#include "include/XStore/XStore_ReplyLimit.h"
#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_ScanLock.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <fcntl.h>
#include <filesystem>
#include <sys/mman.h>
#include <unistd.h>

using namespace XStoreWireEncode;

// RANGE INSERT -- shared body with BatchInsert, see MultiRowInsert in XStore_Internal.h.
void XStore::RangeInsert(XStore_Proto::RangeInsert_REQ *proto_from_client, XStore_Proto::RangeInsert_REP *return_data,
                          xstore::MemoryPool::Clock::time_point deadline) {
    MultiRowInsert(proto_from_client, return_data, "RANGE INSERT", deadline);
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
                                 const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk,
                                 xstore::MemoryPool::Clock::time_point deadline, bool streamed_reply) {
    RangeQueryJob job;
    job.start_epoch = start_epoch_t;
    job.end_epoch = end_epoch_t;
    job.db_name = std::string(db_name_view);
    job.chunk_size_bytes = chunk_size_bytes;
    job.deadline = deadline;
    job.streamed_reply = streamed_reply;
    RangeQueryRun(job, emit_chunk, [] { return false; }, 0);
}

XStore::RangeQueryState XStore::RangeQueryRun(RangeQueryJob &job,
                                              const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk,
                                              const std::function<bool()> &should_park, uint64_t slice_lease_bytes) {
    const std::string &db_name = UpperCaseInto(job.db_name);
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // querystatus (RangeQuery_REP field 3) semantics match RangeQuery
    // exactly: SetTaggedStatus OVERWRITES, so if several segments in one
    // query each hit a warning, only the last one survives -- tracked here
    // the same way, applied once at the very end rather than per-occurrence.
    auto set_warning = [&](const char *tag, const std::string &body) {
        job.has_warning = true;
        job.last_warning = tag;
        job.last_warning += body;
    };

    // Appends querystatus (field 3, only if set) to the last chunk and
    // emits it. The only exit point of this function that ends the reply --
    // every return path below that does funnels through here so emit_chunk is
    // always called at least once (a reply must exist even for a zero-row/error
    // query, or the caller's REQ socket would hang forever waiting for one).
    auto finish = [&](std::string &&chunk_buf) {
        if (job.has_warning) {
            AppendQueryStatus(chunk_buf, job.last_warning);
        }
        emit_chunk(std::move(chunk_buf), true);
    };

    const bool resumed = job.started;
    if (!resumed) {
        job.started = true;
        job.next_epoch = job.start_epoch;
        // Loaded BEFORE the cfg is read, as the deletes do: CfgEpochCurrent tells a resumed run whether the DB was removed meanwhile.
        job.cfg_epoch = CfgEpoch();
        if (!GetOrLoadCfg(db_name, db_base_path_str, job.cfg_struct)) {
            set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - {}.CFG does not exist", db_name, db_name));
            finish(std::string());
            return RangeQueryState::kDone;
        }

        if (job.start_epoch > job.end_epoch) {
            set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - Target end time must be after start time", db_name));
            finish(std::string());
            return RangeQueryState::kDone;
        }

        // Chunking bounds THIS SERVER's peak memory (never more than ~chunk_size_bytes live at once) -- it does NOT lift protobuf's real ~2GB
        // ceiling on a single message for a client that concatenates every chunk back into one buffer before ParseFromArray/ParseFromString
        // (older C++ clients, the Python client): a >2GB reassembled result would corrupt on the CLIENT side even though this server never
        // held more than one chunk at a time. So the guard stays for them, checked before any chunk is sent (an abort partway through a reply
        // has no clean way to signal "actually, error" to a client already receiving it). A streamed_reply (Request.streamOk) goes to a client
        // that parses chunk by chunk and never holds one serialized message, so it is not limited here; the memory lease below still is.
        if (!job.streamed_reply && (job.end_epoch - job.start_epoch) * job.cfg_struct.rowlength() > xstore::ReplyLimitBytes().load()) {
            set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - Query results is larger than 2GB", db_name));
            finish(std::string());
            return RangeQueryState::kDone;
        }

        if (std::filesystem::is_directory(db_base_path_str) == 0) {
            set_warning("[WARNING] ", fmt::format("[RANGE QUERY] [DB: {}] - DB does not exist", db_name));
            finish(std::string());
            return RangeQueryState::kDone;
        }
        job.chunk_buf.reserve(job.chunk_size_bytes + 4096);
    } else if (!CfgEpochCurrent(db_name, job.cfg_epoch)) {
        // Parked, and the DB was removed (maybe re-created) since: the rest of the request is stale. The rows already read stay in the reply.
        set_warning("[ERROR] ", fmt::format("[RANGE QUERY] [DB: {}] - The DB was removed while the request ran", db_name));
        finish(std::move(job.chunk_buf));
        return RangeQueryState::kDone;
    }

    // The reply stays in memory until its chunks are written: reserve it from the process-wide pool, an upper bound, as the 2 GB guard above also is
    // (a sparse range encodes smaller). A run that may park reserves only its slice: a parked reply holds no lease.
    const uint64_t remaining_estimate = (job.end_epoch - job.next_epoch + 1) * (static_cast<uint64_t>(job.cfg_struct.rowlength()) + 16);
    const uint64_t reply_estimate = slice_lease_bytes == 0 ? remaining_estimate : std::min(remaining_estimate, slice_lease_bytes);
    std::shared_ptr<xstore::MemoryLease> reply_lease;
    if (const xstore::MemoryPool::Result leased = xstore::AcquireOperationLease(reply_estimate, job.deadline, reply_lease);
        leased != xstore::MemoryPool::Result::kGranted) {
        set_warning("[ERROR] ", xstore::OperationLeaseRejection("RANGE QUERY", db_name, leased, reply_estimate));
        finish(std::move(job.chunk_buf));
        return RangeQueryState::kDone;
    }

    std::string &chunk_buf = job.chunk_buf;
    const unsigned long long int end_epoch_t = job.end_epoch;
    const XStore_Proto::ConfigStruct &cfg_struct = job.cfg_struct;
    bool consumed_any = false;   // parking is only asked after one scan chunk of this run: every run makes progress
    unsigned long long int iter_epoch = job.next_epoch;
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

        // The rows are read under a READ lock one scan chunk at a time (xstore::scanlock), and a completed output buffer is
        // held in `ready` until that chunk's lock is released: sending to a slow client must not hold off the inserts.
        std::vector<std::string> ready;
        bool parked = false;
        unsigned long long int parked_epoch = 0;
        xstore::scanlock::ScanUnderReadLock(
            target_file_path, mapped_base, first_offset, region_length, cfg_struct.rowlength(), iter_epoch - curr_ts.first_epoch,
            [&](const char *rows_base, unsigned long long rows, unsigned long long row_offset) {
                // Between two scan chunks nothing is in flight: every earlier chunk's output is in chunk_buf or was emitted by after().
                if (consumed_any && should_park()) {
                    parked = true;
                    parked_epoch = iter_epoch + row_offset;
                    return false;
                }
                consumed_any = true;
                for (unsigned long long i = 0; i < rows; ++i) {
                    EncodeQueryResponseEntry(chunk_buf, iter_epoch + row_offset + i, rows_base + i * cfg_struct.rowlength(),
                                             cfg_struct.rowlength());

                    // Flush whenever a completed row has pushed the buffer to
                    // (or past) the target -- never mid-row, so every emitted
                    // chunk's bytes still concatenate back into a stream of
                    // whole, valid queryresponse entries regardless of where
                    // the cut lands. Small queries never hit this at all: one
                    // buffer, one emit_chunk call at the end (see finish()) --
                    // chunking isn't a separate path, just what this becomes
                    // once a query is big enough to reach the target.
                    if (chunk_buf.size() >= job.chunk_size_bytes) {
                        ready.push_back(std::move(chunk_buf));
                        chunk_buf.clear();
                        chunk_buf.reserve(job.chunk_size_bytes + 4096);
                    }
                }
                return true;
            },
            [&] {
                for (std::string &full : ready) emit_chunk(std::move(full), false);
                ready.clear();
            });

        if (parked) {
            job.next_epoch = parked_epoch;
            return RangeQueryState::kParked;
        }
        iter_epoch = segment_end + 1;
    }

    finish(std::move(chunk_buf));
    return RangeQueryState::kDone;
}

// RANGE DELETE
XStore_Proto::RangeDelete_REP XStore::RangeDelete(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name_view) {
    // Protobuf obj to be returned
    XStore_Proto::RangeDelete_REP to_return;
    int delete_count = 0;
    unsigned long long int failed_count = 0;
    std::string first_failure; // why the first failed segment was not deleted

    // Ensure dbName string is all capitalized
    const std::string &db_name = UpperCaseInto(db_name_view);

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. The cfg epoch is loaded BEFORE the cfg is read (see
    // UnaryDelete).
    const uint32_t cfg_epoch = CfgEpoch();
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

    // The DB was removed since the cfg read: checked before and after each year-segment's open, as in UnaryDelete.
    // Segments already done went from the removed DB's files; the rest of the request is stale too.
    const auto removed_meanwhile = [&]() {
        std::string err_msg = fmt::format("[RANGE DELETE] [DB: {}] - The DB was removed while the request ran", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return to_return;
    };

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
        if (!CfgEpochCurrent(db_name, cfg_epoch)) return removed_meanwhile();

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

            // The binding check (see UnaryDelete): after the open and the lock, before the zone map or the punch.
            // Per request: one CfgEpoch load; per year-segment: two more (before the open and here), none per row.
            if (!CfgEpochCurrent(db_name, cfg_epoch)) return removed_meanwhile();

            // The zone map still counts these rows. DeleteRows flags the partly covered end blocks, punches, then
            // resets the fully covered blocks to empty without reading them.
            const unsigned long long int first_row = iter_epoch - curr_ts.first_epoch;
            const ZoneMapFile::Pin zmap = OpenZoneMapForDelete(db_base_path_str, curr_ts.year, cfg_struct);
            int punch_errno = 0;
            const bool punched = zmap.get() != nullptr && zmap->DeleteRows(first_row, first_row + range_count - 1, [&] {
                if (fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfg_struct.rowlength() * range_count) == 0) return true;
                punch_errno = errno;
                return false;
            });
            if (punched) {
                // The punch is a metadata change fdatasync must cover (see BatchDelete).
                walring::Dirty().Note(nullptr, target_file_path, offset, offset + cfg_struct.rowlength() * range_count, 0);
                delete_count += range_count;
            } else {
                if (failed_count == 0) {
                    first_failure = zmap.get() == nullptr ? std::string("zone map could not be opened")
                                                    : fmt::format("fallocate failed: {}", strerror(punch_errno));
                }
                failed_count += range_count;
            }
        }

        iter_epoch = segment_end + 1;
    }

    std::string info_msg = fmt::format("[RANGE DELETE] [DB: {}] - Deleted {} items", db_name, delete_count);
    if (failed_count > 0) {
        // SetTaggedStatus overwrites the single status string, so the failure is folded into the one message.
        std::string warn_msg = fmt::format("{}; {} item(s) could not be deleted ({})", info_msg, failed_count,
                                           first_failure);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[WARNING] ", warn_msg);
        spdlog::warn(warn_msg);
        return to_return;
    }
    SetTaggedStatus(to_return.mutable_deletestatus(), "[INFO] ", info_msg);
    spdlog::info(info_msg);
    return to_return;
}
