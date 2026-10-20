#include "include/XStore/XStore_Internal.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_WireEncode.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <ctime>
#include <execution>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <unistd.h>

using namespace XStoreWireEncode;

// BATCH INSERT
void XStore::BatchInsert(XStore_Proto::BatchInsert_REQ *proto_from_client, XStore_Proto::BatchInsert_REP *return_data) {
    if (RejectInsertIfShutDown(return_data->mutable_insertstatus(), "BATCH INSERT")) return;
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
    const uint32_t cfg_epoch = CfgEpoch(); // before the cfg read (see WalRecord::cfg_epoch)
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
    // downstream per-row lookup below -- called multiple times per row --
    // a random-order access into the
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

    // durabilityMode now routes through the WAL -- see the field's comment
    // on BatchInsert_REQ in XStore.proto.
    const XStore_Proto::WalDurabilityMode durability_mode = proto_from_client->durabilitymode();
    bool any_sync_failed = false;
    bool any_overloaded = false;   // a SYNC group failed because the WAL ring was full

    // Grow the index width to this request's widest row BEFORE any
    // WalRecord/zone-map file is created for it -- one pass over the whole
    // request, done once (not per year-bucket group, not per row), so a
    // fresh DB's first batch is born at its true width instead of staying
    // at the floor until a restart picks up the grown column count.
    int widest = 0;
    for (int i = 0; i < proto_from_client->batchinsert_data_size(); i++) {
        // Rejected oversized rows must not grow the persisted width.
        const auto &row_data = proto_from_client->batchinsert_data(i).data();
        if (row_data.fielddata_size() > widest &&
            row_data.ByteSizeLong() <= static_cast<size_t>(cfg_struct.rowlength())) {
            widest = row_data.fielddata_size();
        }
    }
    MaybeGrowColumnCount(db_name, db_base_path_str, widest);

    // Iterate through each key (year) of dataMap
    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;

        // One WalRecord per year-bucket group -- same grouping BatchInsert
        // already used for its direct-write path, now handed to the WAL
        // instead. Rows within one group share this call's cfg_struct, so
        // db_name/db_base_path/time_resolution/row_length/durability_mode
        // are constant per record; only rows differ.
        WalRecord record;
        record.db_name = db_name;
        record.db_base_path = db_base_path_str;
        record.time_resolution = cfg_struct.timeresolution();
        record.row_length = cfg_struct.rowlength();
        record.cfg_epoch = cfg_epoch;
        record.year = year_key;  // routes the record to its (db, year) shard
        record.durability_mode = durability_mode;
        // One width for both the observations below and the zone map
        // ApplyWalRecord will open/create for them -- see
        // WalRecord::tracked_column_count. cfg_struct is this call's own
        // local snapshot, so this stays constant across the whole loop even
        // as MaybeGrowColumnCount moves the persisted value underneath.
        // Folded in with the pre-grown `widest` so a brand-new index file
        // is created at the request's true width on this very first write,
        // not just EffectiveTrackedColumnCount's pre-growth floor.
        record.tracked_column_count = std::max(EffectiveTrackedColumnCount(cfg_struct),
                                                std::min<uint32_t>(static_cast<uint32_t>(widest), XStoreTunables::MAX_TRACKED_COLUMN_COUNT));
        record.rows.reserve(indices.size());

        for (unsigned long long int idx : indices) {
            const XStore_Proto::BatchInsert_DATA &entry = proto_from_client->batchinsert_data(idx);
            size_t serialized_size = entry.data().ByteSizeLong();
            // Reject-not-truncate, same guarantee as UnaryInsert: a row
            // that doesn't fit its slot is dropped rather than silently
            // corrupting whatever follows it.
            if (serialized_size > static_cast<size_t>(cfg_struct.rowlength())) {
                continue;
            }
            std::string row_bytes(cfg_struct.rowlength(), '\0');
            [[maybe_unused]] bool serialize_ok = entry.data().SerializeToArray(row_bytes.data(), static_cast<int>(cfg_struct.rowlength()));
            std::memset(row_bytes.data() + serialized_size, 0, cfg_struct.rowlength() - serialized_size);
            record.rows.push_back(WalRow{entry.timestamp(), std::move(row_bytes),
                                          BuildZoneMapObservations(*this, entry.data(), record.tracked_column_count),
                                          BuildCatIdxObservations(*this, entry.data(), record.tracked_column_count),
                                          record.tracked_column_count});
        }

        if (record.rows.empty()) {
            // Every row in this year-group was rejected as oversized --
            // nothing to write, not a failure of the write path itself.
            continue;
        }

        // BYPASS applies straight to the data file on this thread, same as
        // UnaryInsert's BYPASS path -- no WAL record ever written for it.
        // Everything else (SYNC/ASYNC) hands off to the WAL: EnqueueWalRecord
        // blocks until durable+applied for SYNC, returns immediately after
        // the lock-free enqueue for ASYNC.
        bool ok = (durability_mode == XStore_Proto::BYPASS)
            ? ApplyWalRecord(record)
            : EnqueueWalRecord(std::move(record), &any_overloaded);
        if (!ok) {
            any_sync_failed = true;
            spdlog::error("[BATCH INSERT] [DB: {}] - WAL write failed for {}.XSTORE", db_name, year_key);
        }
    }

    // Return insert status. any_sync_failed is only ever set on a genuine
    // ApplyWalRecord/EnqueueWalRecord failure above -- true durability
    // failures for SYNC/BYPASS, but also a real enqueue failure for ASYNC
    // (queue-full/backpressure timeout, allocation failure, etc.), which is
    // just as real a failure as the other modes' and must not be masked by
    // the unconditional success message below -- no mode condition here.
    if (any_sync_failed) {
        std::string err_msg = fmt::format("[BATCH INSERT] [DB: {}] - Inserted {} elements, but the write failed for at least one group",
                                           db_name, proto_from_client->batchinsert_data_size());
        if (any_overloaded) err_msg += std::string("; ") + kWalOverloadedText;
        SetTaggedStatus(return_data->mutable_insertstatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return;
    }
    std::string success_msg = fmt::format("[BATCH INSERT] [DB: {}] - Inserted {} elements successfully", db_name, proto_from_client->batchinsert_data_size());
    SetTaggedStatus(return_data->mutable_insertstatus(), "[INFO] ", success_msg);
    spdlog::info(success_msg);
}

namespace {
// BatchQuery's thread_local lock list is released above this many entries (512 KB) instead of kept for the next call.
constexpr size_t BATCH_LOCK_OFFSETS_KEEP = 65536;
} // namespace

// BATCH QUERY (streamed) -- see XStore.h's declaration comment for the
// contract. Mirrors BatchQuery's structure exactly (sort/bucket/lock, the
// same pread-not-mmap fd path since BatchQuery's scattered access pattern
// still degrades badly under mmap -- see BatchQuery's comment on that), down
// to holes being OMITTED rather than encoded as empty entries (unlike
// RangeQuery). The only real difference is what happens to each non-hole
// row: EncodeQueryResponseEntry writes its bytes straight into chunk_buf
// instead of building a UnaryQuery_REP/RowData/FData tree.
void XStore::BatchQuery(XStore_Proto::BatchQuery_REQ *proto_from_client, bool sort_time, size_t chunk_size_bytes,
                                 const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk) {
    std::string db_name = proto_from_client->dbname();
    ToUpperCase(db_name.begin(), db_name.end());

    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    bool has_warning = false;
    std::string last_warning;
    auto set_warning = [&](const char *tag, const std::string &body) {
        has_warning = true;
        last_warning = tag;
        last_warning += body;
    };
    auto finish = [&](std::string &&chunk_buf) {
        if (has_warning) {
            AppendQueryStatus(chunk_buf, last_warning);
        }
        emit_chunk(std::move(chunk_buf), true);
    };

    XStore_Proto::ConfigStruct cfg_struct;
    if (!GetOrLoadCfg(db_name, db_base_path_str, cfg_struct)) {
        set_warning("[WARNING] ", fmt::format("[BATCH QUERY] [DB: {}] - {}.CFG does not exist", db_name, db_name));
        finish(std::string());
        return;
    }

    if ((proto_from_client->timestamp_size() * cfg_struct.rowlength()) > 2'000'000'000 || proto_from_client->timestamp_size() < 0) {
        set_warning("[WARNING] ", fmt::format("[BATCH QUERY] [DB: {}] - Query results is larger than 2GB or empty", db_name));
        finish(std::string());
        return;
    }

    if (sort_time) {
        SortBatch(proto_from_client->mutable_timestamp()->begin(), proto_from_client->mutable_timestamp()->end(),
                      [](const unsigned long long int& a, const unsigned long long int& b) {
                        return a < b;
        });
    }

    YearBucketMap data_map;

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

    std::sort(data_map.begin(), data_map.end(),
              [](const auto &a, const auto &b) { return a.first < b.first; });

    std::string chunk_buf;
    chunk_buf.reserve(chunk_size_bytes + 4096);

    for (const std::pair<unsigned short int, std::vector<unsigned long long int>> &pair : data_map) {
        unsigned short int year_key = pair.first;
        const std::vector<unsigned long long int> &indices = pair.second;
        const std::string &target_file = CachedYearFilePath(db_base_path_str, year_key);

        if (!IsFdCached(target_file) && std::filesystem::exists(target_file) == 0) {
            set_warning("[WARNING] ", fmt::format("[BATCH QUERY] [DB: {}] - {}.XSTORE does not exist", db_name, year_key));
            continue;
        }

        TimeStruct this_ts{static_cast<time_t>(proto_from_client->timestamp(indices.front())), cfg_struct.timeresolution()};

        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, cfg_struct.rowlength(), this_ts.last_epoch - this_ts.first_epoch);
        if (!fd_handle) {
            set_warning("[ERROR] ", fmt::format("[BATCH QUERY] [DB: {}] - Unable to open {}.XSTORE", db_name, year_key));
            continue;
        }
        int fd = *fd_handle;

        // BatchQuery's whole reason for pread-not-mmap is a scattered,
        // request-order access pattern (see the comment on that above) --
        // tell the kernel so it stops speculatively reading ahead pages
        // this loop is unlikely to touch next. Advisory only (no I/O, no
        // blocking), so it's cheap to call on every query even though the
        // fd is shared/cached across calls (GetOrOpenFile) -- each call
        // just overwrites the same advice flag on the open file
        // description, not a resource that accumulates or needs undoing.
        posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);

        const size_t row_len = static_cast<size_t>(cfg_struct.rowlength());

        // READ locks on exactly the rows read (indices are in request order: only this lock list is sorted), so a
        // writer's rows in the gaps never wait for this query nor it for them. Past the cap: the min-to-max span.
        // thread_local scratch, reused: the guard is gone before the next year-group refills it.
        thread_local std::vector<unsigned long long int> lock_offsets;
        thread_local std::vector<FileRangeLock::Run> lock_runs;
        lock_offsets.resize(indices.size());
        for (size_t i = 0; i < indices.size(); i++) {
            lock_offsets[i] = (proto_from_client->timestamp(indices[i]) - this_ts.first_epoch) * row_len;
        }
        // O(n) check first: a request sent sorted (or sorted by sort_time) skips the O(n log n) sort.
        if (!std::is_sorted(lock_offsets.begin(), lock_offsets.end())) {
            std::sort(lock_offsets.begin(), lock_offsets.end());   // duplicates join their run in BuildLockRuns
        }
        BuildLockRuns(lock_offsets.data(), lock_offsets.size(), row_len, XStoreTunables::BATCH_QUERY_MAX_RUN_LOCKS,
                      lock_runs);
        // One huge query must not pin 8 bytes per row on every server thread that ever ran one (lock_runs is capped).
        if (lock_offsets.capacity() > BATCH_LOCK_OFFSETS_KEEP) std::vector<unsigned long long int>().swap(lock_offsets);
        FileRangeLockRunsGuard guard(target_file, lock_runs.data(), lock_runs.size(), FileRangeLock::LockType::READ);

        // Below the threshold, plain serial pread inline -- no job-submit +
        // semaphore-wait round trip for a handful of rows where that
        // overhead could plausibly exceed the work itself. The real
        // device-concurrency tuning knob lives on XStorePreadPool itself
        // (XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP), not here -- see
        // its comment in XStore_Tunables.h.
        if (indices.size() < XStoreTunables::BATCH_QUERY_PARALLEL_PREAD_MIN_ROWS) {
            thread_local std::vector<char> buffer_in;
            if (buffer_in.size() < row_len) {
                buffer_in.resize(row_len);
            }
            for (unsigned long long int idx : indices) {
                unsigned long long int timestamp = proto_from_client->timestamp(idx);
                unsigned long long int offset = (timestamp - this_ts.first_epoch) * row_len;

                // (void) doesn't suppress __wur under _FORTIFY_SOURCE; short read is ignored on purpose.
                [[maybe_unused]] ssize_t pread_ret = pread(fd, buffer_in.data(), row_len, static_cast<off_t>(offset));

                // A hole is always fully zeroed, so checking the first byte
                // is enough (buffer_in is a fixed-width binary buffer, not a
                // C string, so strlen() would over-read). Unlike RangeQuery,
                // a hole is OMITTED here, not encoded as an empty entry --
                // matches BatchQuery's existing semantics exactly.
                if (buffer_in[0] == 0) {
                    continue;
                }

                EncodeQueryResponseEntry(chunk_buf, timestamp, buffer_in.data(), row_len);

                if (chunk_buf.size() >= chunk_size_bytes) {
                    emit_chunk(std::move(chunk_buf), false);
                    chunk_buf.clear();
                    chunk_buf.reserve(chunk_size_bytes + 4096);
                }
            }
            continue;
        }

        // Wave-parallel pread: fan indices.size() preads out across
        // BATCH_QUERY_PREAD_WAVE_ROWS-sized waves. Within a wave, every
        // pread is submitted to the shared XStorePreadPool (pread takes its
        // offset as a parameter and doesn't touch shared file-position
        // state, so concurrent calls on the same fd need no locking; the
        // run guard above is a READ lock, so concurrent
        // readers inside it are fine too). Unlike a plain std::execution::
        // par_unseq fan-out, the pool's worker count is fixed and SHARED
        // across every concurrent BatchQuery call server-wide --
        // see XStoreTunables::BATCH_QUERY_PREAD_POOL_SIZE_CAP for why that
        // matters (a real device-concurrency cap, not just a per-call
        // limit two concurrent queries could still multiply past). The
        // encode pass that follows each wave is single-threaded and walks
        // indices in original request order, so the emitted byte stream is
        // identical to the serial path's regardless of which pool worker
        // happened to finish first. Wave size (not the full indices.size())
        // bounds the scratch buffer to a fixed footprint independent of
        // batch size, preserving BatchQuery's server-memory-bounded
        // design -- decoupled from the pool's worker count on purpose, see
        // XStore_Tunables.h.
        // NOT thread_local: this buffer is written by XStorePreadPool's
        // WORKER threads (via the [&]-captured lambda below) and read back
        // on THIS thread once ParallelFor returns. thread_local storage is
        // deliberately exempt from lambda capture (accessed like a global,
        // re-resolved to whichever thread is currently running the lambda
        // body) -- with thread_local here, each pool worker would write
        // into its OWN, never-resized, empty instance instead of this
        // call's actual buffer, corrupting/losing every row. A plain local
        // is genuinely captured by reference, so every worker touches the
        // same one instance this call already owns.
        std::vector<char> wave_buf;
        const size_t wave_rows = XStoreTunables::BATCH_QUERY_PREAD_WAVE_ROWS;
        wave_buf.resize(wave_rows * row_len);
        for (size_t wave_start = 0; wave_start < indices.size(); wave_start += wave_rows) {
            const size_t wave_end = std::min(indices.size(), wave_start + wave_rows);
            const unsigned long long int *wave_base = indices.data() + wave_start;

            XStorePreadPool::ParallelFor(wave_end - wave_start, [&](size_t local_i) {
                const unsigned long long int timestamp = proto_from_client->timestamp(wave_base[local_i]);
                const unsigned long long int offset = (timestamp - this_ts.first_epoch) * row_len;
                // Same __wur note as serial path above.
                [[maybe_unused]] ssize_t pread_ret = pread(fd, wave_buf.data() + local_i * row_len, row_len, static_cast<off_t>(offset));
            });

            for (size_t local_i = 0; local_i < wave_end - wave_start; local_i++) {
                const char *row_ptr = wave_buf.data() + local_i * row_len;

                // Same hole check as the serial path above.
                if (row_ptr[0] == 0) {
                    continue;
                }

                const unsigned long long int timestamp = proto_from_client->timestamp(wave_base[local_i]);
                EncodeQueryResponseEntry(chunk_buf, timestamp, row_ptr, row_len);

                if (chunk_buf.size() >= chunk_size_bytes) {
                    emit_chunk(std::move(chunk_buf), false);
                    chunk_buf.clear();
                    chunk_buf.reserve(chunk_size_bytes + 4096);
                }
            }
        }
    }

    finish(std::move(chunk_buf));
}

// BATCH DELETE
XStore_Proto::BatchDelete_REP XStore::BatchDelete(XStore_Proto::BatchDelete_REQ *proto_from_client) {
    // Protobuf obj to be returned
    XStore_Proto::BatchDelete_REP to_return;
    std::string db_name = proto_from_client->dbname();
    int delete_count = 0;
    int failed_count = 0;
    std::string first_failure; // why the first failed row was not deleted

    // Ensure dbName string is all capitalized
    ToUpperCase(db_name.begin(), db_name.end());

    // Base directory of a DB
    const std::string &db_base_path_str = CachedDbBasePath(db_name);

    // Fetch cfgStruct from cache, loading from disk on first use. The cfg epoch is loaded BEFORE the cfg is read (see
    // UnaryDelete).
    const uint32_t cfg_epoch = CfgEpoch();
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

    // The DB was removed since the cfg read: checked before and after each year-group's open, as in UnaryDelete. Rows
    // of earlier groups went from the removed DB's files; the rest of the request is stale too.
    const auto removed_meanwhile = [&]() {
        std::string err_msg = fmt::format("[BATCH DELETE] [DB: {}] - The DB was removed while the request ran", db_name);
        SetTaggedStatus(to_return.mutable_deletestatus(), "[ERROR] ", err_msg);
        spdlog::error(err_msg);
        return to_return;
    };

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
        if (!CfgEpochCurrent(db_name, cfg_epoch)) return removed_meanwhile();

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

        // WRITE locks on exactly this year-group's runs of rows, taken all at once -- indices is sorted ascending
        // (the whole batch was sorted by timestamp above), so the offsets are too. Rows in the gaps (a scattered
        // writer's, a reader's) do not conflict; past the cap the group locks its min-to-max span. The fallocate64
        // calls below still can't be coalesced into one, though: timestamps here are randomly distributed, not
        // contiguous, so punching one big range would also deallocate OTHER rows' data sitting in the gaps between
        // the requested points. thread_local scratch, reused: the guard is gone before the next year-group refills it.
        thread_local std::vector<unsigned long long int> lock_offsets;
        thread_local std::vector<FileRangeLock::Run> lock_runs;
        lock_offsets.resize(indices.size());
        for (size_t i = 0; i < indices.size(); i++) {
            lock_offsets[i] = (proto_from_client->timestamp(indices[i]) - this_ts.first_epoch) * cfg_struct.rowlength();
        }
        BuildLockRuns(lock_offsets.data(), lock_offsets.size(), cfg_struct.rowlength(),
                      XStoreTunables::BATCH_DELETE_MAX_RUN_LOCKS, lock_runs);
        if (lock_offsets.capacity() > BATCH_LOCK_OFFSETS_KEEP) std::vector<unsigned long long int>().swap(lock_offsets);   // as in BatchQuery

        {
            // Lock prior to file OP
            FileRangeLockRunsGuard guard(target_file, lock_runs.data(), lock_runs.size(), FileRangeLock::LockType::WRITE);

            // The binding check (see UnaryDelete): after the open and the locks, before the zone map or any punch.
            // Per request: one CfgEpoch load; per year-group: two more (before the open and here), none per row.
            if (!CfgEpochCurrent(db_name, cfg_epoch)) return removed_meanwhile();

            // Union of the successful punches: ONE dirty note per year-group (one tracker lock) instead of one per
            // row. Over-marks the unpunched gaps between rows, which only costs the checkpoint a slightly wider
            // fdatasync range.
            unsigned long long int punched_begin = std::numeric_limits<unsigned long long int>::max();
            unsigned long long int punched_end = 0;
            // The zone map still counts these rows: flag each distinct block once, before its rows go. Rows are
            // sorted, so a block's rows are adjacent.
            const ZoneMapFile::Pin zmap = OpenZoneMapForDelete(db_base_path_str, year_key, cfg_struct);
            if (zmap.get() == nullptr) {
                if (failed_count == 0) first_failure = "zone map could not be opened";
                failed_count += static_cast<int>(indices.size());
                continue;
            }
            const uint64_t rows_per_block = zmap->RowsPerBlockValue();
            uint64_t marked_block = std::numeric_limits<uint64_t>::max();
            for (unsigned long long int idx : indices) {
                const unsigned long long int row = proto_from_client->timestamp(idx) - this_ts.first_epoch;
                unsigned long long int offset = row * cfg_struct.rowlength();

                const uint64_t block = BlockIndexForRow(row, rows_per_block);
                if (block != marked_block) {
                    zmap->MarkInexact(row, row);
                    marked_block = block;
                }

                // Deallocate data according to N bytes from rowLength
                if (fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfg_struct.rowlength()) == 0) {
                    punched_begin = std::min(punched_begin, offset);
                    punched_end = std::max(punched_end, offset + cfg_struct.rowlength());
                    delete_count++;
                } else {
                    if (failed_count++ == 0) first_failure = fmt::format("fallocate failed: {}", strerror(errno));
                }
            }
            // The punches are a metadata change fdatasync must cover: mark the file so the next checkpoint cycle
            // (and the final one at a clean stop) syncs it. No ring record carries it.
            if (punched_end != 0) walring::Dirty().Note(nullptr, target_file, punched_begin, punched_end, 0);
        }
    }
    std::string info_msg = fmt::format("[BATCH DELETE] [DB: {}] - Deleted {} items", db_name, delete_count);
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
