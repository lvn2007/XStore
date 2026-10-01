// src/XStore/XStore_Wal.cpp
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/DataStructs/TimeStruct.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <chrono>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

std::array<XStore::WalShard, XStoreTunables::WAL_SHARD_COUNT> XStore::wal_shards;
std::atomic<size_t> XStore::active_wal_shard_count{XStoreTunables::WAL_SHARD_COUNT};
moodycamel::ConcurrentQueue<XStore::WalCompletion*> XStore::completion_pool;
#ifdef XSTORE_WAL_STATS_ENABLED
std::atomic<bool> XStore::wal_stats_stop_flag{false};
std::thread XStore::wal_stats_reporter_thread;
#endif

namespace {

template <typename T>
void AppendPod(std::string &out, T value) {
    out.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

} // namespace

// Wire format (native-endian -- WAL files aren't portable across
// architectures, same assumption XStore's .XSTORE data files already
// make). One record = one write REQUEST, carrying 1..N rows (1 for
// UnaryInsert; BatchInsert/RangeInsert reuse this same framing with N rows
// per year-bucket group instead of a separate format):
//   [u32 recordLen][u32 crc32c][u16 dbNameLen][u16 rowLength]
//   [i32 timeResolution][u32 rowCount][dbName bytes]
//   repeated rowCount times: [u64 epochTime][row bytes]
// recordLen counts every byte AFTER itself. crc32c covers every byte after
// itself (dbNameLen through the last row's bytes) so ReplayWal can
// validate a record before trusting it. Written by WalWriterLoop's
// pwritev path below directly (no dedicated SerializeWalRecord function --
// removed: building one contiguous buffer per record was itself the copy
// this format switch to vectored I/O exists to avoid). Parsed by
// ParseWalRecord, an XStore:: member (not file-local) so ReplayWal's
// crash-recovery path (a different translation unit) uses this exact
// implementation instead of an independently-maintained copy -- that
// duplication is what let a since-fixed bounds-check bug survive uncaught
// in the duplicate.

// Parses one record starting at data[0]. Returns false (consumed left
// unset) if fewer than one full, CRC-valid record is available -- ReplayWal
// treats that as "stop here" (a torn tail record from a crash mid-write),
// not a hard error. XStore:: member for the same sharing reason as
// SerializeWalRecord above.
bool XStore::ParseWalRecord(const char *data, size_t len, size_t &consumed, XStore::WalRecord &out_record) {
    if (len < 8) return false;
    uint32_t record_len;
    std::memcpy(&record_len, data, 4);
    // record_len is widened to size_t before the addition -- adding as
    // 4u + record_len (both 32-bit) can wrap around for a corrupt/torn
    // record_len near UINT32_MAX, defeating this bounds check and letting
    // a huge out-of-bounds Crc32 read through below.
    if (len < static_cast<size_t>(record_len) + 4 || record_len < 4u + 2 + 2 + 4 + 4) return false;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, data + 4, 4);

    const char *payload = data + 8;
    size_t payload_len = record_len - 4;
    if (Crc32c(payload, payload_len) != stored_crc) return false;

    size_t off = 0;
    uint16_t db_name_len; std::memcpy(&db_name_len, payload + off, 2); off += 2;
    uint16_t row_length;  std::memcpy(&row_length, payload + off, 2); off += 2;
    int32_t time_res;     std::memcpy(&time_res, payload + off, 4); off += 4;
    uint32_t row_count;   std::memcpy(&row_count, payload + off, 4); off += 4;

    if (off + db_name_len > payload_len) return false;
    out_record.db_name.assign(payload + off, db_name_len);
    off += db_name_len;

    size_t expected_rows_bytes = static_cast<size_t>(row_count) * (8u + row_length);
    if (off + expected_rows_bytes != payload_len) return false;

    out_record.row_length = row_length;
    out_record.time_resolution = static_cast<XStore_Proto::TimeUnit>(time_res);
    out_record.rows.clear();
    out_record.rows.reserve(row_count);
    for (uint32_t i = 0; i < row_count; ++i) {
        uint64_t epoch_time; std::memcpy(&epoch_time, payload + off, 8); off += 8;
        XStore::WalRow row;
        row.epoch_time = epoch_time;
        row.row_bytes.assign(payload + off, row_length);
        off += row_length;
        out_record.rows.push_back(std::move(row));
    }

    consumed = 4 + record_len;
    return true;
}

namespace {

// Returns "<data_store_dir>/WAL/shard_<shardIdx>.WAL" for the
// data_store_dir that owns dbBasePath (dbBasePath's parent directory).
// Derived per-record, not fixed at thread-start, so one shard's writer
// thread can safely multiplex records from multiple distinct
// data_store_dirs -- true in tests (each constructing XStore against its
// own scratch directory) even though production only ever has one
// data_store_dir per process.
// Pure path computation only -- no filesystem syscalls. The caller
// creates the containing directory only on the fd-cache-miss path (right
// before open()), not once per record in the batch.
std::string WalFilePathFor(const std::string &db_base_path, size_t shard_idx) {
    std::filesystem::path data_store_dir = std::filesystem::path(db_base_path).parent_path();
    std::filesystem::path wal_dir = data_store_dir / XStoreTunables::WAL_DIR_NAME;
    return (wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL")).string();
}

// Same path, computed directly from a data_store_dir instead of reverse-
// engineering one out of a WalRecord's db_base_path -- used by
// StartWalWriters, which has data_store_dir up front and no record yet.
std::string WalFilePathForDataStoreDir(const std::string &data_store_dir, size_t shard_idx) {
    std::filesystem::path wal_dir = std::filesystem::path(data_store_dir) / XStoreTunables::WAL_DIR_NAME;
    return (wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL")).string();
}

// Opens wal_path (creating it if needed) and ensures it has at least
// WAL_FILE_PREALLOC_BYTES of real, block-allocated space (fallocate, not a
// sparse ftruncate) -- called both eagerly (StartWalWriters, the known-
// data_store_dir production case) and lazily (WalWriterLoop's fd-cache-miss
// path, the multi-data_store_dir test case). fallocate only ever grows an
// existing file's allocated range, never truncates/zeroes bytes already
// written, so this is always safe to call on a file that may already hold
// real not-yet-replayed WAL data. Returns -1 on any failure.
int OpenAndPreallocateWalFile(const std::string &wal_path) {
    std::filesystem::create_directories(std::filesystem::path(wal_path).parent_path());
    int fd = open(wal_path.c_str(), O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
        spdlog::error("[WAL] - Unable to open {}", wal_path);
        return -1;
    }
    // posix_fallocate (POSIX-portable) over the Linux-specific fallocate()
    // -- same real-block-allocation guarantee, no _GNU_SOURCE dependency.
    // Returns the errno value directly rather than setting errno.
    if (int err = posix_fallocate(fd, 0, static_cast<off_t>(XStoreTunables::WAL_FILE_PREALLOC_BYTES)); err != 0) {
        spdlog::error("[WAL] - Unable to preallocate {}: {}", wal_path, std::strerror(err));
        close(fd);
        return -1;
    }
    return fd;
}

} // namespace

bool XStore::ApplyWalRecord(const WalRecord &record_to_apply) {
    // Precondition: every row in one WalRecord belongs to the same (db,
    // year) file, and rows are ordered ascending by epoch_time. True for
    // every current producer -- UnaryInsert's 1-row records trivially
    // satisfy this; BatchInsert/RangeInsert build one WalRecord per
    // year-bucket group (see XStore_BatchOps.cpp/XStore_RangeOps.cpp),
    // sorted ascending before being handed off here (undocumented-
    // enforcement, same style as the rest of this precondition).
    if (record_to_apply.rows.empty()) return true;

    TimeStruct first_ts(record_to_apply.rows.front().epoch_time, record_to_apply.time_resolution);
    const std::string &target_file = CachedYearFilePath(record_to_apply.db_base_path, first_ts.year);

    std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, record_to_apply.row_length,
                                                    first_ts.last_epoch - first_ts.first_epoch);
    if (!fd_handle) {
        spdlog::error("[WAL APPLY] [DB: {}] - Unable to open {}.XSTORE", record_to_apply.db_name, first_ts.year);
        return false;
    }
    int fd = *fd_handle;

    // Fetched once per record (every row shares the same (db, year), see
    // this function's precondition comment above) rather than once per
    // row/run -- this is the actual, only place a row's zone-map update
    // happens: at the same moment as its real pwrite below, under the same
    // success/failure check, regardless of which thread or how much later
    // that turns out to be for ASYNC durability mode. Updating eagerly at
    // enqueue time instead (UnaryInsert/BatchInsert/RangeInsert) would let
    // the zone map claim a row that a crash between enqueue and this apply
    // step later proves was never actually written -- exactly the
    // zone-map-disagrees-with-real-data failure mode Task 2's write-through
    // design exists to prevent, just moved to the wrong side of the WAL
    // instead of eliminated.
    //
    // tracked_column_count comes off the RECORD, not the global default:
    // it is the exact width this record's zone_map_observations were built
    // at (captured alongside them at enqueue time, before the .CFG's
    // columnCount could move underneath), so the file this creates can
    // never claim to track more columns than were actually folded into it.
    ZoneMapFile &zmap = GetOrOpenZoneMap(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                                          record_to_apply.tracked_column_count);
    // Same reasoning as zmap above, same (db, year, row_length,
    // tracked_column_count) key -- the bloom filter is only ever updated
    // here, alongside zmap, after a row's real pwrite is confirmed durable.
    CatIdxFile &catidx = GetOrOpenCatIdx(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                                         record_to_apply.tracked_column_count);

    std::vector<unsigned long long> offsets(record_to_apply.rows.size());
    for (size_t i = 0; i < record_to_apply.rows.size(); ++i) {
        offsets[i] = (record_to_apply.rows[i].epoch_time - first_ts.first_epoch) * record_to_apply.row_length;
    }

    // One lock spanning the whole record's min-to-max offset span --
    // cheaper than one lock per run, and correct regardless of how many
    // runs the offsets below turn out to have.
    unsigned long long region_length = (offsets.back() + record_to_apply.row_length) - offsets.front();
    FileRangeLockGuard guard(target_file, offsets.front(), region_length, FileRangeLock::LockType::WRITE);

    // Thread-local scratch buffer, grown to fit the largest run applied so
    // far and reused across calls -- avoids a heap allocation per run.
    thread_local std::vector<char> run_buf;
    bool all_rows_ok = true;

    for (size_t run_start = 0; run_start < offsets.size(); ) {
        size_t run_end = run_start + 1;
        while (run_end < offsets.size() && offsets[run_end] == offsets[run_end - 1] + record_to_apply.row_length) {
            ++run_end;
        }

        size_t run_rows = run_end - run_start;
        size_t run_bytes = run_rows * static_cast<size_t>(record_to_apply.row_length);
        if (run_buf.size() < run_bytes) {
            run_buf.resize(run_bytes);
        }

        for (size_t i = run_start; i < run_end; ++i) {
            const std::string &row_bytes = record_to_apply.rows[i].row_bytes;
            std::memcpy(run_buf.data() + (i - run_start) * record_to_apply.row_length, row_bytes.data(), record_to_apply.row_length);
        }

        ssize_t written = pwrite(fd, run_buf.data(), run_bytes, static_cast<off_t>(offsets[run_start]));
        if (written != static_cast<ssize_t>(run_bytes)) {
            spdlog::error("[WAL APPLY] [DB: {}] - Short/failed write to {}.XSTORE", record_to_apply.db_name, first_ts.year);
            all_rows_ok = false;
        } else {
            // Only reached when this run's rows are confirmed durably
            // written above -- never update the zone map for a row whose
            // real write just failed.
            for (size_t i = run_start; i < run_end; ++i) {
                uint64_t diff = record_to_apply.rows[i].epoch_time - first_ts.first_epoch;
                zmap.UpdateRow(diff, record_to_apply.rows[i].zone_map_observations, record_to_apply.rows[i].zone_map_pad_to);
                catidx.UpdateRow(diff, record_to_apply.rows[i].catidx_observations);
            }
        }
        run_start = run_end;
    }
    return all_rows_ok;
}

void XStore::InvalidateWalFileHeader(int fd, const std::string &wal_path) {
    static constexpr char ZERO_LEN_HEADER[4] = {0, 0, 0, 0};
    // fdatasync, not fsync: this write never changes file size (always inside
    // the preallocated region, see WAL_FILE_PREALLOC_BYTES), so metadata
    // durability isn't needed, only the 4 written bytes.
    if (pwrite(fd, ZERO_LEN_HEADER, sizeof(ZERO_LEN_HEADER), 0) != static_cast<ssize_t>(sizeof(ZERO_LEN_HEADER)) || fdatasync(fd) != 0) {
        // Not fatal to the batch that just durably applied -- those rows
        // are safely on disk in the real data file regardless. Worst case
        // on a crash before this is retried: the stale record replays
        // again next restart, which is only wrong if its DB was deleted
        // and recreated in between -- log it so that's diagnosable instead
        // of silently risking a future "unable to open" surprise.
        spdlog::error("[WAL] - Failed to invalidate checkpoint header for {}", wal_path);
    }
}

void XStore::ReclaimWalFileSize(int fd, const std::string &wal_path, uintmax_t on_disk_bytes) {
    if (on_disk_bytes <= XStoreTunables::WAL_FILE_PREALLOC_BYTES) {
        return;
    }
    // fsync, not fdatasync: shrinking IS a file-size change, the exact
    // class of metadata WalWriterLoop's own oversized_write fallback exists
    // to durably flush -- see the comment on this function in XStore.h.
    if (ftruncate(fd, static_cast<off_t>(XStoreTunables::WAL_FILE_PREALLOC_BYTES)) != 0 || fsync(fd) != 0) {
        // Not fatal: the file just stays at its current (larger) size,
        // same as before this reclaim step existed -- correctness is
        // unaffected, only the disk-space cleanup is deferred to the next
        // restart's attempt.
        spdlog::error("[WAL] - Failed to reclaim {} back to its default preallocated size ({} bytes); "
                      "leaving it at {} bytes.",
                      wal_path, XStoreTunables::WAL_FILE_PREALLOC_BYTES, on_disk_bytes);
        return;
    }
    spdlog::info("[WAL] - Reclaimed {} from {} bytes back down to its default preallocated size ({} bytes).",
                 wal_path, on_disk_bytes, XStoreTunables::WAL_FILE_PREALLOC_BYTES);
}

bool XStore::EnqueueWalRecord(WalRecord record) {
    // Writers are stopped after Shutdown(); a SYNC wait here would never wake.
    if (IsShutDown()) return false;
    // Per-DB hash, not round-robin: tried round-robin first (spreads a
    // single hot DB across every active shard), but on a single physical
    // storage device that backfires -- it splits what could be one shard's
    // large, well-amortized batch into N shards' worth of smaller batches,
    // each paying its own fsync, when the device likely can't actually
    // fsync those N streams in parallel anyway. Per-DB hashing keeps one
    // DB's writers funneled through one shard, which lets the group-commit
    // gather window (see WalWriterLoop) batch them into fewer, bigger
    // fsyncs -- the thing that actually amortizes cost on one device.
    // Multiple DBs still parallelize naturally across shards, same as
    // before either version of this code existed. WAL_SHARDS_PER_DB (see
    // its comment in XStore_Tunables.h) is the escape hatch for actually
    // testing that "device can't fsync N streams in parallel anyway"
    // assumption instead of just asserting it -- now set to 4 (see that
    // tunable's own comment for the benchmarked regression this fixed),
    // so this function no longer behaves bit-for-bit identically to the
    // paragraph above; set it back to 1 to restore that.
    size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
    size_t base_shard_idx = std::hash<std::string>{}(record.db_base_path) % active;
    size_t shard_idx = base_shard_idx;
    if constexpr (XStoreTunables::WAL_SHARDS_PER_DB > 1) {
        // Low-cardinality salt (which calling thread, not full round-robin)
        // spreads one DB's writes across up to WAL_SHARDS_PER_DB of the
        // active shards, capped at `active` itself -- can never exceed the
        // box's real shard count no matter how high the tunable is set.
        // Salting by thread-id (not a per-record random/atomic counter)
        // keeps one thread's own sequential commits landing on the same
        // shard, only spreading DIFFERENT concurrent callers apart.
        size_t db_shard_span = std::min(active, XStoreTunables::WAL_SHARDS_PER_DB);
        size_t salt = std::hash<std::thread::id>{}(std::this_thread::get_id()) % db_shard_span;
        shard_idx = (base_shard_idx + salt) % active;
    }
    WalShard &shard = wal_shards[shard_idx];

    // Backpressure: wait (never reject) for this shard's queue to drain
    // below the high watermark before adding to it. Uniform across every
    // durability mode, including ASYNC -- otherwise a writer that can't
    // keep up would let the queue (and the un-fsync'd, would-be-lost-on-
    // crash backlog) grow without bound while still telling ASYNC callers
    // "success".
    while (shard.queue.size_approx() >= XStoreTunables::WAL_QUEUE_HIGH_WATERMARK) {
        std::this_thread::sleep_for(std::chrono::microseconds(XStoreTunables::WAL_BACKPRESSURE_POLL_US));
    }

    // Not embedded in WalRecord by value: the record gets std::move'd into
    // the queue below, so this caller's own stack frame is a different
    // object than whatever the writer thread eventually sees -- the
    // completion handle needs a stable address reachable by pointer from
    // both sides. Checked out from completion_pool (true one-time
    // allocation, not a fresh malloc per request -- see that member's
    // comment, XStore.h); returned to the pool by this thread after
    // sem.wait() returns instead of being freed.
    WalCompletion *completion = nullptr;
    if (record.durability_mode != XStore_Proto::ASYNC) {
        if (!completion_pool.try_dequeue(completion)) {
            completion = new WalCompletion();
        }
        record.completion = completion;
    }

    if (!shard.queue.enqueue(std::move(record))) {
        if (completion != nullptr) {
            completion_pool.enqueue(completion);
        }
        return false;
    }

    if (completion != nullptr) {
        // Blocks until the writer thread resolves it; result reflects the
        // real durability outcome (false on a WAL fsync failure or a
        // failed apply) instead of unconditionally reporting success --
        // ASYNC skips this and only reports "enqueued", by design.
        completion->sem.wait();
        bool result = completion->result;
        completion_pool.enqueue(completion);
        return result;
    }
    return true;
}

void XStore::StartWalWriters(const std::string &data_store_dir) {
    // Force spdlog's default-logger registry to exist BEFORE the
    // std::atexit callback below is registered. spdlog::details::registry
    // is a function-local static (Meyer's) singleton -- constructed lazily
    // on whichever spdlog:: call happens first, ANYWHERE in the process,
    // not eagerly at static-init time. Per [basic.start.term], termination
    // functions (atexit callbacks and static-object destructors) run in
    // reverse order of "completion", where a static object's construction
    // and an atexit registration are ordered against each other by which
    // one completed first: if this atexit() call below is registered
    // BEFORE the registry singleton is constructed, the registry's
    // destructor is sequenced to run BEFORE this callback executes -- the
    // exact opposite of what the callback's join()s below assume, and
    // exploitable by a still-running WAL/fsync-coalescer thread calling
    // spdlog::error/warn after the logger it's using has already been
    // destroyed (this is the shutdown segfault this comment guards
    // against). Production (src/main.cpp) constructs a Logger before any
    // XStore, so its first spdlog:: call already precedes this one; the
    // test binary never constructs one, so without this line the registry
    // is instead first (and lazily) constructed by a WAL writer thread's
    // OWN first error log call, sometimes well after this point. Calling
    // it here, unconditionally, before spawning any thread or registering
    // the atexit callback, makes the safe ordering hold on every path.
    static_cast<void>(spdlog::default_logger());

    // Writer threads are almost entirely I/O-bound (blocked in fsync() or
    // on the queue's semaphore -- near-zero idle CPU, see the
    // WAL_GROUP_COMMIT_WINDOW_US tunable comment), so the real ceiling on
    // "how many is too many" is contention at the storage device, not CPU
    // headroom -- stays a conservative fraction of core count, not 1:1.
    unsigned hw = std::thread::hardware_concurrency();
    size_t desired = hw == 0 ? XStoreTunables::WAL_SHARD_COUNT
                             : static_cast<size_t>(hw) / XStoreTunables::WAL_ACTIVE_SHARDS_CORE_DIVISOR;
    size_t active = std::clamp(desired, XStoreTunables::WAL_MIN_ACTIVE_SHARDS, XStoreTunables::WAL_SHARD_COUNT);
    active_wal_shard_count.store(active, std::memory_order_relaxed);

    for (size_t i = 0; i < active; ++i) {
        // Eagerly open + fallocate here (not lazily on first write) --
        // production has exactly one data_store_dir, known now, so there's
        // no reason to pay the first commit's latency for it. Safe even if
        // this file already holds real crash-recovery data from an
        // unclean shutdown: OpenAndPreallocateWalFile's fallocate only
        // ever grows, never truncates/zeroes existing bytes, and this runs
        // before ReplayWal (called later, from InitDB) gets a chance to
        // read it.
        std::string wal_path = WalFilePathForDataStoreDir(data_store_dir, i);
        int fd = OpenAndPreallocateWalFile(wal_path);
        wal_shards[i].writer_thread = std::thread(&XStore::WalWriterLoop, i, wal_path, fd);
    }
#ifdef XSTORE_WAL_STATS_ENABLED
    wal_stats_reporter_thread = std::thread(&XStore::WalStatsReporterLoop);
#endif

    // Writer threads must be stopped and joined before other static-storage
    // objects they touch (the moodycamel queue, spdlog's default logger)
    // begin their own destruction at process exit -- a still-running thread
    // racing that teardown corrupts the heap rather than cleanly
    // terminating. std::atexit callbacks run in reverse registration order,
    // before any static destructors registered after this call, so
    // registering here (at the moment the threads are actually started)
    // guarantees this runs first.
    std::atexit([]() { XStore::StopWalWriters(); });
}

std::atomic<bool> XStore::shutdown_flag_{false};

void XStore::StopWalWriters() {
    for (size_t i = 0; i < XStoreTunables::WAL_SHARD_COUNT; ++i) {
        wal_shards[i].stop_flag.store(true, std::memory_order_relaxed);
    }
    for (size_t i = 0; i < XStoreTunables::WAL_SHARD_COUNT; ++i) {
        if (wal_shards[i].writer_thread.joinable()) {
            wal_shards[i].writer_thread.join();
        }
    }
#ifdef XSTORE_WAL_STATS_ENABLED
    wal_stats_stop_flag.store(true, std::memory_order_relaxed);
    if (wal_stats_reporter_thread.joinable()) {
        wal_stats_reporter_thread.join();
    }
#endif
}

void XStore::Shutdown() {
    if (shutdown_flag_.exchange(true, std::memory_order_acq_rel)) return;

    StopWalWriters();

    // Data files first: an index may only be marked clean once the data file
    // it describes is durable. First the fd cache (snapshot shared_ptrs under
    // each shard lock, sync outside it), remembering which paths succeeded.
    std::unordered_set<std::string> synced;
    for (FdShard &shard : fd_shards) {
        std::vector<std::pair<std::string, std::shared_ptr<int>>> snapshot;
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            for (size_t i = 0; i < shard.count; ++i) {
                snapshot.emplace_back(shard.entries[i].key, shard.entries[i].value);
            }
        }
        for (auto &[path, fd] : snapshot) {
            if (fdatasync(*fd) == 0) {
                synced.insert(path);
            } else {
                spdlog::error("[SHUTDOWN] - fdatasync failed for {}: {}", path, std::strerror(errno));
            }
        }
    }

    // Registry key is "<db_base_path>/<year>", i.e. the data file is
    // "<key>.XSTORE" (same string CachedYearFilePath builds). If the fd cache
    // evicted it, sync it through a throwaway read-only fd (fdatasync flushes
    // the inode's dirty pages whichever fd wrote them, mmap included).
    auto data_file_synced = [&](const std::string &key) {
        std::string data_path = key + ".XSTORE";
        if (synced.count(data_path) != 0) return true;
        int fd = open(data_path.c_str(), O_RDONLY);
        if (fd < 0 || fdatasync(fd) != 0) {
            spdlog::error("[SHUTDOWN] - Cannot sync {} ({}); leaving its indexes dirty", data_path, std::strerror(errno));
            if (fd >= 0) close(fd);
            return false;
        }
        close(fd);
        synced.insert(std::move(data_path));
        return true;
    };

    // Registry mutex is held only around the MarkClean() calls (not the data
    // syncs): it also guards entry lifetime against LRU eviction, so a file
    // can't be destroyed mid-MarkClean. Order is registry mutex -> file
    // mutex; no file method takes a registry mutex, so no inversion. A write
    // racing in after MarkClean simply re-marks the file dirty.
    auto mark_registry_clean = [&](std::mutex &registry_mutex, auto &registry, const char *what) {
        std::vector<std::string> keys;
        {
            std::lock_guard<std::mutex> lock(registry_mutex);
            keys.reserve(registry.size());
            for (const auto &entry : registry) keys.push_back(entry.first);
        }
        std::vector<std::string> ready;
        for (std::string &key : keys) {
            if (data_file_synced(key)) ready.push_back(std::move(key));
        }
        std::lock_guard<std::mutex> lock(registry_mutex);
        for (const std::string &key : ready) {
            auto it = registry.find(key);
            if (it != registry.end() && !it->second->MarkClean()) {
                spdlog::error("[SHUTDOWN] - MarkClean failed for {} {}; leaving it dirty", what, key);
            }
        }
    };
    mark_registry_clean(zone_map_registry_mutex_, zone_map_registry_, ".zmap");
    mark_registry_clean(catidx_registry_mutex_, catidx_registry_, ".catidx");

    CheckpointWals();
}

namespace {
std::mutex wal_dirs_mutex;
std::vector<std::string> wal_dirs;
} // namespace

void XStore::RegisterDataStoreDir(const std::string &data_store_dir) {
    std::lock_guard<std::mutex> lock(wal_dirs_mutex);
    wal_dirs.push_back(data_store_dir);
}

// Writers are joined, so each shard file is quiescent and holds exactly what
// ReplayWal would find: its last batch (plus, possibly, a CRC-valid stale
// tail). Parsing it with ParseWalRecord gives the (db, year) data files replay
// would write into; the header is invalidated only once every one of them has
// been fdatasynced by path (a throwaway fd, so an fd-cache eviction cannot
// hide a file, and the file at THAT path is the one flushed). Any failure
// leaves that shard's WAL untouched => the next start replays + rebuilds.
void XStore::CheckpointWals() {
    std::vector<std::string> dirs;
    {
        std::lock_guard<std::mutex> lock(wal_dirs_mutex);
        dirs = wal_dirs;
    }
    std::unordered_set<std::string> synced;
    auto sync_path = [&](const std::string &path) {
        if (synced.count(path) != 0) return true;
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0 || fdatasync(fd) != 0) {
            spdlog::error("[SHUTDOWN] - Cannot sync {} ({}); leaving its WAL records for replay", path, std::strerror(errno));
            if (fd >= 0) close(fd);
            return false;
        }
        close(fd);
        synced.insert(path);
        return true;
    };

    for (const std::string &dir : dirs) {
        const std::filesystem::path wal_dir = std::filesystem::path(dir) / XStoreTunables::WAL_DIR_NAME;
        if (!std::filesystem::is_directory(wal_dir)) continue;
        for (size_t shard_idx = 0; shard_idx < XStoreTunables::WAL_SHARD_COUNT; ++shard_idx) {
            const std::string wal_path = (wal_dir / ("shard_" + std::to_string(shard_idx) + ".WAL")).string();
            int wal_fd = open(wal_path.c_str(), O_RDWR);
            if (wal_fd < 0) continue; // shard never used
            struct stat st{};
            void *map = MAP_FAILED;
            if (fstat(wal_fd, &st) == 0 && st.st_size > 0) {
                map = mmap(nullptr, static_cast<size_t>(st.st_size), PROT_READ, MAP_PRIVATE, wal_fd, 0);
            }
            if (map == MAP_FAILED) {
                spdlog::error("[SHUTDOWN] - Cannot read {}; leaving it for replay", wal_path);
                close(wal_fd);
                continue;
            }
            const char *base = static_cast<const char *>(map);
            const size_t size = static_cast<size_t>(st.st_size);
            bool all_synced = true;
            WalRecord record;
            for (size_t off = 0, consumed = 0; off < size && all_synced; off += consumed) {
                if (!ParseWalRecord(base + off, size - off, consumed, record)) break;
                if (record.rows.empty()) continue;
                TimeStruct ts(static_cast<std::time_t>(record.rows.front().epoch_time), record.time_resolution);
                all_synced = sync_path(fmt::format("{}/{}.XSTORE", JoinPath(dir, record.db_name), ts.year));
            }
            munmap(map, size);
            if (all_synced) InvalidateWalFileHeader(wal_fd, wal_path);
            close(wal_fd);
        }
    }
}

void XStore::ResetShutdownForTests() {
    if (!shutdown_flag_.exchange(false, std::memory_order_acq_rel)) return;
    // Writers resolve their WAL fd lazily from each record's path when
    // handed no preopened fd.
    size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
    for (size_t i = 0; i < active; ++i) {
        wal_shards[i].stop_flag.store(false, std::memory_order_relaxed);
        wal_shards[i].writer_thread = std::thread(&XStore::WalWriterLoop, i, std::string{}, -1);
    }
#ifdef XSTORE_WAL_STATS_ENABLED
    wal_stats_stop_flag.store(false, std::memory_order_relaxed);
    wal_stats_reporter_thread = std::thread(&XStore::WalStatsReporterLoop);
#endif
}

#ifdef XSTORE_WAL_STATS_ENABLED
// Debug/benchmark-only (XSTORE_WAL_STATS CMake option). Wakes every
// WAL_STATS_LOG_INTERVAL_S, logs each active shard's average batch size
// (records/batch) since the LAST report (a delta, not a cumulative
// average), so the number reflects current load rather than being diluted
// by an idle period earlier in the run. No lock needed: reads its own
// prior-snapshot locals plus each shard's relaxed atomics, writer threads
// never read these back.
void XStore::WalStatsReporterLoop() {
    std::vector<uint64_t> prev_batches(XStoreTunables::WAL_SHARD_COUNT, 0);
    std::vector<uint64_t> prev_records(XStoreTunables::WAL_SHARD_COUNT, 0);
    while (!wal_stats_stop_flag.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::seconds(XStoreTunables::WAL_STATS_LOG_INTERVAL_S));
        if (wal_stats_stop_flag.load(std::memory_order_relaxed)) break;

        size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
        for (size_t i = 0; i < active; ++i) {
            uint64_t batches = wal_shards[i].total_batches_processed.load(std::memory_order_relaxed);
            uint64_t records = wal_shards[i].total_records_processed.load(std::memory_order_relaxed);
            uint64_t d_batches = batches - prev_batches[i];
            uint64_t d_records = records - prev_records[i];
            prev_batches[i] = batches;
            prev_records[i] = records;
            if (d_batches == 0) continue; // idle shard this interval -- nothing to report
            double avg_batch_size = static_cast<double>(d_records) / static_cast<double>(d_batches);
            // warn, not info: the server's default log level filters info
            // entirely (see the perf test's own comment on why -- per-request
            // info logging would flood stdout), which would silently drop
            // every one of these diagnostic-only lines otherwise.
            spdlog::warn("[WAL STATS] shard={} batches={} records={} avg_batch_size={:.2f}",
                         i, d_batches, d_records, avg_batch_size);
        }
    }
}
#endif

void XStore::WalWriterLoop(size_t shard_idx, std::string preopened_path, int preopened_fd) {
    WalShard &shard = wal_shards[shard_idx];
    std::vector<WalRecord> batch(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Thread-local to this one writer thread -- no lock needed, only this
    // thread ever touches it. Declared once outside the loop and reused
    // (via .clear()/reset each iteration, or sized once and indexed) rather
    // than re-declared per batch, to avoid a fresh heap allocation on the
    // writer's per-batch hot path.
    //
    // known_size tracks each file's actual on-disk size so the
    // oversized_write check below can compare against reality instead of
    // always against the static WAL_FILE_PREALLOC_BYTES constant. A file
    // only ever grows (fallocate never truncates, and every pass writes
    // from offset 0, never shrinking it -- see WalWriterLoop's own comment
    // on that below), so once a file has organically grown past the
    // constant, a later smaller pass that fits inside its already-grown
    // size needs no metadata-changing write and can safely use
    // fdatasync() again. No fstat() needed to seed this: every file this
    // loop opens (preopened or lazily via OpenAndPreallocateWalFile) is
    // fallocated to at least WAL_FILE_PREALLOC_BYTES before its fd is
    // handed back, so that constant is exactly the correct known starting
    // size.
    std::unordered_map<std::string, size_t> wal_fds_known_size;
    std::unordered_map<std::string, int> wal_fds;
    if (preopened_fd >= 0) {
        wal_fds_known_size.emplace(preopened_path, XStoreTunables::WAL_FILE_PREALLOC_BYTES);
        wal_fds.emplace(std::move(preopened_path), preopened_fd);
    }
    // Per-file list of {pointer, length} pieces for one pwritev() call,
    // built to point DIRECTLY at each record's already-owned memory
    // (db_base_path/rows[].row_bytes, both alive in `batch`, released at
    // the end of this iteration after completions are signalled) instead
    // of copying every record's bytes into one
    // concatenated buffer first -- the thing that used to cost a full
    // extra payload-sized memcpy per record here. record_headers/
    // extra_epoch_bufs below hold the small (tens of bytes) per-record
    // metadata pieces that DO need their own scratch storage (they're
    // computed values, not already-owned memory); everything else is a
    // reference into memory the batch already owns.
    //
    // iovec count per file is no longer assumed to fit in one pwritev call
    // -- BatchInsert/RangeInsert can hand a single WalRecord hundreds of
    // thousands of rows (2 iovecs/row: an epoch-time buffer + the row bytes
    // for every row past the first), which routinely exceeds Linux's
    // IOV_MAX/UIO_MAXIOV of 1024. The write loop below chunks into multiple
    // pwritev calls per file (advancing the file offset between chunks)
    // instead of relying on a single-call bound -- see
    // XStoreTunables::WAL_PWRITEV_MAX_IOVECS.
    std::unordered_map<std::string, std::vector<struct iovec>> iovecs_by_file;
    // Deques, not vectors: push_back on a deque never invalidates
    // references to earlier elements (only vector's does, on reallocation)
    // -- iovecs above hold raw pointers into these, taken before all of
    // this batch's records have been processed, so that stability matters.
    // Each entry: [recordLen(4)][crc32c(4)][dbNameLen(2)][rowLength(2)]
    // [timeResolution(4)][rowCount(4)][dbName][first row's epochTime(8) if
    // any] -- everything on the wire except row bytes themselves (kept as
    // separate iovec entries pointing straight at WalRow::row_bytes) and
    // any 2nd-or-later row's epochTime (in extra_epoch_bufs, rare/unused
    // today -- WAL's only current producer, UnaryInsert, always builds
    // exactly 1 row per record).
    std::deque<std::string> record_headers;
    std::deque<std::array<char, sizeof(uint64_t)>> extra_epoch_bufs;
    // Memoizes WalFilePathFor within one batch: it's pure path computation
    // (filesystem::path construction + string concat, no syscalls) but
    // still real allocation work, and most records in one shard's batch
    // share the same db_base_path (they landed on this shard together
    // precisely because EnqueueWalRecord hashes by db_base_path) -- no
    // reason to recompute the identical path once per record when it's
    // usually the same 1-2 distinct paths for the whole batch.
    std::unordered_map<std::string, std::string> wal_path_cache;
    // Per-file durability outcome for THIS batch -- replaces a single
    // all_durable flag so one file's fsync failure in a multi-DB batch
    // doesn't wrongly withhold the checkpoint (truncate) for a DIFFERENT
    // file that genuinely succeeded, and doesn't wrongly report success for
    // records that share that failed file.
    std::unordered_map<std::string, bool> file_durable;
    // Which wal_path each dequeued record's bytes went into, so the
    // per-record durability check below doesn't need to recompute
    // WalFilePathFor a second time.
    std::vector<std::string> record_wal_path(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Tracks which records in the current batch have already had their
    // completion resolved -- lets the catch block below resolve only the
    // ones an exception left unresolved, instead of risking a second
    // sem.signal() (double-wake, harmless with LightweightSemaphore but
    // still wrong -- the client-side wait() would return early using a
    // stale result/count) on one already resolved by the code that ran
    // before the exception.
    std::vector<bool> resolved(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Epoch (not "now") so the very first iteration is always outside the
    // adaptive window and takes the normal blocking path -- no spin paid on
    // a freshly-started, not-yet-active shard.
    auto last_activity = std::chrono::steady_clock::time_point{};
    // This shard's own exponential moving average of batch size -- see
    // WAL_GATHER_WINDOW_MIN_US/MAX_US/ADAPT_ALPHA in XStore_Tunables.h.
    // Thread-local (no lock/atomic needed -- only this writer thread ever
    // touches it). Starts at 1.0 so the very first gather window is MIN,
    // not an assumed MAX -- a fresh/idle shard shouldn't pay for batching
    // it hasn't demonstrated yet.
    double batch_size_ema = 1.0;

    while (!shard.stop_flag.load(std::memory_order_relaxed)) {
        size_t count = 0;
        if constexpr (XStoreTunables::WAL_ADAPTIVE_POLL_WINDOW_US > 0) {
            auto window = std::chrono::microseconds(XStoreTunables::WAL_ADAPTIVE_POLL_WINDOW_US);
            if (std::chrono::steady_clock::now() - last_activity < window) {
                // Recently active: tight non-blocking spin, bypassing
                // wait_dequeue_bulk_timed's semaphore entirely, so a record
                // arriving during this window is picked up without paying
                // the OS park/wake tax. Self-terminates the moment the
                // window lapses with nothing arriving -- falls through to
                // the blocking path on the next outer-loop iteration.
                while (count == 0) {
                    count = shard.queue.try_dequeue_bulk(batch.data(), XStoreTunables::WAL_BATCH_MAX_RECORDS);
                    if (count == 0 && std::chrono::steady_clock::now() - last_activity >= window) break;
                }
                if (count == 0) continue;
            } else {
                count = shard.queue.wait_dequeue_bulk_timed(
                    batch.data(), XStoreTunables::WAL_BATCH_MAX_RECORDS,
                    std::chrono::microseconds(XStoreTunables::WAL_GROUP_COMMIT_WINDOW_US));
                if (count == 0) continue;
            }
        } else {
            count = shard.queue.wait_dequeue_bulk_timed(
                batch.data(), XStoreTunables::WAL_BATCH_MAX_RECORDS,
                std::chrono::microseconds(XStoreTunables::WAL_GROUP_COMMIT_WINDOW_US));
            if (count == 0) continue;
        }
        last_activity = std::chrono::steady_clock::now();

        // Group-commit gather: the blocking wait above returns the instant
        // the first record arrives, which alone gives near-zero batching
        // under concurrent load (a non-blocking peek only catches what's
        // already sitting in the queue at that exact instant -- tried that
        // first, it didn't move the needle). Instead, actively linger for
        // up to an ADAPTIVE window (derived from batch_size_ema below, see
        // its declaration and WAL_GATHER_WINDOW_MIN_US/MAX_US in
        // XStore_Tunables.h), polling every WAL_GATHER_POLL_US, stopping
        // the moment the batch fills or the window elapses -- same
        // dual-trigger shape as Kafka's linger.ms + batch.size. This DOES
        // cost a lone SYNC writer real added latency (up to the window,
        // every commit) in exchange for real batching once per-DB hashing
        // (see EnqueueWalRecord) funnels concurrent writers of the same DB
        // onto this one shard -- but a shard whose recent batches show no
        // real grouping self-tunes that window down toward MIN instead of
        // assuming MAX, so a lone writer pays close to nothing.
        //
        // No per-record "flush immediately" override here anymore -- an
        // earlier design had a separate SYNC_PER_COMMIT mode that skipped
        // this gather loop entirely, distinct from GROUP_COMMIT which rode
        // it. The two were collapsed into one SYNC mode (see
        // WalDurabilityMode in XStore.proto): the adaptive window above
        // already self-tunes to near-MIN for a lone writer, so a separate
        // caller-visible "never batch me" mode bought nothing the adaptive
        // logic wasn't already doing, and MIN_US itself is the actual knob
        // for how little a lone SYNC writer pays -- see the Mystic
        // benchmark note on that constant before retuning it further.
        if (count < XStoreTunables::WAL_BATCH_MAX_RECORDS) {
            double saturate_t = (batch_size_ema - 1.0)
                / (XStoreTunables::WAL_GATHER_ADAPT_EMA_SATURATE - 1.0);
            saturate_t = std::clamp(saturate_t, 0.0, 1.0);
            auto gather_window_us = static_cast<unsigned>(
                XStoreTunables::WAL_GATHER_WINDOW_MIN_US
                + saturate_t * (XStoreTunables::WAL_GATHER_WINDOW_MAX_US - XStoreTunables::WAL_GATHER_WINDOW_MIN_US));

            auto gather_deadline = std::chrono::steady_clock::now()
                + std::chrono::microseconds(gather_window_us);
            while (count < XStoreTunables::WAL_BATCH_MAX_RECORDS) {
                size_t got = shard.queue.try_dequeue_bulk(batch.data() + count, XStoreTunables::WAL_BATCH_MAX_RECORDS - count);
                count += got;
                if (count >= XStoreTunables::WAL_BATCH_MAX_RECORDS || std::chrono::steady_clock::now() >= gather_deadline) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(XStoreTunables::WAL_GATHER_POLL_US));
            }
        }

        // Feed this batch's real size back into the EMA that decides the
        // NEXT batch's gather window -- updated regardless of whether the
        // gather loop ran its full course or broke early on a full batch.
        batch_size_ema = XStoreTunables::WAL_GATHER_ADAPT_ALPHA * static_cast<double>(count)
            + (1.0 - XStoreTunables::WAL_GATHER_ADAPT_ALPHA) * batch_size_ema;

        std::fill(resolved.begin(), resolved.begin() + static_cast<long>(count), false);

#ifdef XSTORE_WAL_STATS_ENABLED
        shard.total_batches_processed.fetch_add(1, std::memory_order_relaxed);
        shard.total_records_processed.fetch_add(count, std::memory_order_relaxed);
#endif

        // A std::thread's entry function must never let an exception
        // escape -- doing so calls std::terminate() and kills the whole
        // process, silently dropping every in-flight request across all
        // WAL_SHARD_COUNT shards, not just this one. The realistic source
        // here is std::bad_alloc from the string/map allocations below
        // under memory pressure; catching it and failing just this batch
        // (still resolving every caller's future, so nobody hangs forever
        // either) is the graceful-degradation behavior a durability layer
        // is expected to have.
        try {
            // Group this batch by target WAL file -- always exactly one
            // group in production (one data_store_dir per process); only
            // tests running multiple data_store_dirs in one binary ever
            // see more.
            iovecs_by_file.clear();
            record_headers.clear();
            extra_epoch_bufs.clear();
            wal_path_cache.clear();
            for (size_t i = 0; i < count; ++i) {
                const WalRecord &record = batch[i];
                auto [path_it, inserted] = wal_path_cache.try_emplace(record.db_base_path);
                if (inserted) {
                    path_it->second = WalFilePathFor(record.db_base_path, shard_idx);
                }
                record_wal_path[i] = path_it->second;

                // Header piece: everything on the wire except row bytes
                // (kept as separate iovecs below, no copy) -- recordLen and
                // crc32c are patched in after being computed further down,
                // but the iovec pointing at this buffer's final address can
                // be pushed now since pwritev only reads it after the patch.
                record_headers.emplace_back();
                std::string &hdr = record_headers.back();
                hdr.reserve(XStoreTunables::WAL_RECORD_HEADER_BYTES + record.db_name.size() + sizeof(uint64_t));
                hdr.resize(8); // recordLen+crc32c placeholder, patched below
                AppendPod<uint16_t>(hdr, static_cast<uint16_t>(record.db_name.size()));
                AppendPod<uint16_t>(hdr, record.row_length);
                AppendPod<int32_t>(hdr, static_cast<int32_t>(record.time_resolution));
                AppendPod<uint32_t>(hdr, static_cast<uint32_t>(record.rows.size()));
                hdr.append(record.db_name);
                if (!record.rows.empty()) {
                    AppendPod<uint64_t>(hdr, record.rows[0].epoch_time);
                }

                std::vector<struct iovec> &iov_list = iovecs_by_file[record_wal_path[i]];
                iov_list.push_back({hdr.data(), hdr.size()});

                uint32_t crc = Crc32cUpdate(Crc32cInit(), hdr.data() + 8, hdr.size() - 8);
                size_t payload_size_after_header = hdr.size() - 8;
                if (!record.rows.empty()) {
                    const std::string &row_bytes = record.rows[0].row_bytes;
                    iov_list.push_back({const_cast<char *>(row_bytes.data()), row_bytes.size()});
                    crc = Crc32cUpdate(crc, row_bytes.data(), row_bytes.size());
                    payload_size_after_header += row_bytes.size();
                }
                for (size_t r = 1; r < record.rows.size(); ++r) {
                    extra_epoch_bufs.emplace_back();
                    auto &eb = extra_epoch_bufs.back();
                    uint64_t epoch = record.rows[r].epoch_time;
                    std::memcpy(eb.data(), &epoch, sizeof(epoch));
                    iov_list.push_back({eb.data(), eb.size()});
                    crc = Crc32cUpdate(crc, eb.data(), eb.size());

                    const std::string &row_bytes = record.rows[r].row_bytes;
                    iov_list.push_back({const_cast<char *>(row_bytes.data()), row_bytes.size()});
                    crc = Crc32cUpdate(crc, row_bytes.data(), row_bytes.size());
                    payload_size_after_header += eb.size() + row_bytes.size();
                }
                crc = Crc32cFinal(crc);

                uint32_t record_len = static_cast<uint32_t>(4 + payload_size_after_header);
                std::memcpy(hdr.data(), &record_len, sizeof(record_len));
                std::memcpy(hdr.data() + 4, &crc, sizeof(crc));
            }

            file_durable.clear();
            for (auto &entry : iovecs_by_file) {
                const std::string &wal_path = entry.first;
                std::vector<struct iovec> &iov_list = entry.second;
                file_durable[wal_path] = false;

                auto it = wal_fds.find(wal_path);
                int fd;
                if (it == wal_fds.end()) {
                    // Test-only path in production (StartWalWriters already
                    // pre-opened+preallocated the one data_store_dir it
                    // knows about) -- reached for a second/third distinct
                    // data_store_dir in a multi-instance test binary.
                    fd = OpenAndPreallocateWalFile(wal_path);
                    if (fd < 0) {
                        continue;
                    }
                    wal_fds.emplace(wal_path, fd);
                    wal_fds_known_size.emplace(wal_path, XStoreTunables::WAL_FILE_PREALLOC_BYTES);
                } else {
                    fd = it->second;
                }
                size_t &known_size = wal_fds_known_size.at(wal_path);

                // Chunk this file's iovecs into groups of at most
                // WAL_PWRITEV_MAX_IOVECS -- the common case (few
                // records/rows) is exactly one chunk, unchanged from
                // before; only a large multi-row Batch/Range record (or
                // enough concurrent records landing on the same file in one
                // gather window) ever needs more than one pwritev call.
                // Offset advances across chunks within THIS batch pass only
                // -- the WAL file itself is still overwritten from offset 0
                // on every writer-loop pass (see WalWriterLoop's own
                // comment on this), unrelated to chunking.
                //
                // Runtime size check (see the NOTE on WAL_FILE_PREALLOC_BYTES
                // in XStore_Tunables.h): that bound was sized assuming 1 row
                // per WalRecord, true only for UnaryInsert. BatchInsert/
                // RangeInsert can hand this loop a single year-bucket group
                // with hundreds of thousands of rows, whose total on-wire
                // bytes for one file can legitimately exceed the
                // preallocated region. fdatasync() alone is only correct
                // when a write never changes the file's size/metadata --
                // true as long as every byte lands inside the file's actual
                // current on-disk size (known_size), false the instant
                // total_file_bytes below crosses it. Compared against
                // known_size rather than the static WAL_FILE_PREALLOC_BYTES
                // constant so that once a file has organically grown past
                // the constant (fallocate never truncates, and every pass
                // writes from offset 0 -- see the chunking comment above --
                // so a file's real size is monotonically non-decreasing for
                // its whole lifetime), a later smaller pass that fits inside
                // that already-grown size doesn't keep needlessly paying for
                // fsync(). Detected up front (before writing) rather than
                // reacting to a surprising post-write file size, so the
                // fallback to a real fsync() -- which DOES flush the
                // size-changing metadata-journal update -- is decided once,
                // not per chunk.
                size_t total_file_bytes = 0;
                for (const struct iovec &iov : iov_list) total_file_bytes += iov.iov_len;
                bool oversized_write = total_file_bytes > known_size;
                if (oversized_write) {
                    spdlog::warn("[WAL] - Batch for {} totals {} bytes, exceeding its current on-disk size ({}) -- "
                                 "this write grows the file further; falling back to a real fsync() for this "
                                 "commit (one-time metadata-journal cost) instead of fdatasync().",
                                 wal_path, total_file_bytes, known_size);
                    // known_size is only bumped once the write+sync below
                    // actually succeed -- bumping it here, before the write
                    // is even attempted, would let a later smaller pass
                    // wrongly assume the file already covers it (and use
                    // fdatasync()) even though this write never durably
                    // grew it.
                }

                bool file_write_ok = true;
                off_t chunk_offset = 0;
                for (size_t iov_start = 0; iov_start < iov_list.size(); ) {
                    size_t iov_count = std::min(iov_list.size() - iov_start, XStoreTunables::WAL_PWRITEV_MAX_IOVECS);
                    size_t chunk_bytes = 0;
                    for (size_t i = iov_start; i < iov_start + iov_count; ++i) chunk_bytes += iov_list[i].iov_len;

                    ssize_t written = pwritev(fd, iov_list.data() + iov_start, static_cast<int>(iov_count), chunk_offset);
                    if (written != static_cast<ssize_t>(chunk_bytes)) {
                        file_write_ok = false;
                        break;
                    }
                    chunk_offset += static_cast<off_t>(chunk_bytes);
                    iov_start += iov_count;
                }

                bool sync_ok = oversized_write ? (fsync(fd) == 0) : (fdatasync(fd) == 0);
                if (!file_write_ok || !sync_ok) {
                    spdlog::error("[WAL] - Failed to durably write/{} {}", oversized_write ? "fsync" : "fdatasync", wal_path);
                    continue;
                }
                if (oversized_write) known_size = total_file_bytes;
                file_durable[wal_path] = true;
            }

            // Apply every request's rows to their real data files -- same
            // in-memory bytes already held from enqueue, no WAL re-read
            // (WAL is only ever read back during ReplayWal, never
            // mid-batch). A record only counts as durable if BOTH its
            // WAL file's fsync succeeded above AND every one of its rows
            // applied cleanly here -- either failing means the caller must
            // be told, not silently given a success signal.
            for (size_t i = 0; i < count; ++i) {
                bool wal_ok = file_durable[record_wal_path[i]];
                bool apply_ok = ApplyWalRecord(batch[i]);
                bool record_ok = wal_ok && apply_ok;
                if (!record_ok) {
                    spdlog::error("[WAL] [DB: {}] - Record not durably persisted (wal_durable={}, applied={})",
                                  batch[i].db_name, wal_ok, apply_ok);
                }
                if (batch[i].completion) {
                    batch[i].completion->result = record_ok;
                    batch[i].completion->sem.signal();
                }
                resolved[i] = true;
            }

            // No per-commit checkpoint here (used to call
            // InvalidateWalFileHeader on every durable batch -- a second
            // fsync on every single commit). Not needed: ReplayWal already
            // invalidates every shard file unconditionally on every
            // startup, regardless of whether anything was found to replay
            // or whether the apply succeeded. That alone fully closes the
            // "orphaned record replays forever" bug -- the record left
            // sitting here after a live commit gets naturally overwritten
            // by this shard's next real write (offset-0 overwrite, same as
            // always), and if no next write ever comes, the worst case is
            // one redundant (harmless, idempotent) re-apply attempt the
            // next time the process restarts, not a repeat-forever failure.
        } catch (const std::exception &ex) {
            spdlog::error("[WAL] - Writer loop caught exception, failing this batch: {}", ex.what());
            for (size_t i = 0; i < count; ++i) {
                if (!resolved[i] && batch[i].completion) {
                    batch[i].completion->result = false;
                    batch[i].completion->sem.signal();
                }
            }
        } catch (...) {
            spdlog::error("[WAL] - Writer loop caught unknown exception, failing this batch");
            for (size_t i = 0; i < count; ++i) {
                if (!resolved[i] && batch[i].completion) {
                    batch[i].completion->result = false;
                    batch[i].completion->sem.signal();
                }
            }
        }

        // Release applied records now (after every completion above is
        // signalled) instead of when a later dequeue overwrites the slot --
        // same destructor work, earlier, off the SYNC callers' latency path.
        for (size_t i = 0; i < count; ++i) batch[i] = WalRecord{};
    }
}
