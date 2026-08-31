// src/XStore/XStore_Wal.cpp
#include "include/XStore/XStore.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/DataStructs/TimeStruct.h"
#include <spdlog/fmt/fmt.h>
#include "include/Logger/Logger.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <chrono>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>
#include <unordered_map>
#include <vector>

std::array<XStore::WalShard, XStoreTunables::WAL_SHARD_COUNT> XStore::wal_shards;
std::atomic<size_t> XStore::active_wal_shard_count{XStoreTunables::WAL_SHARD_COUNT};

namespace {

// Standard reflected CRC-32 (IEEE 802.3 / zlib polynomial 0xEDB88320),
// table built once via a function-local static (thread-safe init in
// C++11+). Detects a torn/partial record from a crash mid-write during
// ReplayWal -- not hardware-accelerated CRC32C (see docs/TODO.md Gap 1 for
// that as a documented future optimization); correctness first here.
uint32_t Crc32(const void *data, size_t len) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            }
            t[i] = c;
        }
        return t;
    }();

    uint32_t crc = 0xFFFFFFFFu;
    const auto *bytes = static_cast<const unsigned char *>(data);
    for (size_t i = 0; i < len; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xFFu] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFFu;
}

template <typename T>
void AppendPod(std::string &out, T value) {
    out.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

} // namespace

// Wire format (native-endian -- WAL files aren't portable across
// architectures, same assumption XStore's .XSTORE data files already
// make). One record = one write REQUEST, carrying 1..N rows (1 for
// UnaryInsert; a future Batch/Range wiring reuses this same framing with
// N rows instead of a separate format):
//   [u32 recordLen][u32 crc32][u16 dbNameLen][u16 rowLength]
//   [i32 timeResolution][u32 rowCount][dbName bytes]
//   repeated rowCount times: [u64 epochTime][row bytes]
// recordLen counts every byte AFTER itself. crc32 covers every byte after
// itself (dbNameLen through the last row's bytes) so ReplayWal can
// validate a record before trusting it. XStore:: member (not file-local)
// so ReplayWal's crash-recovery path (a different translation unit) uses
// this exact implementation instead of an independently-maintained copy --
// that duplication is what let a since-fixed bounds-check bug survive
// uncaught in the duplicate.
std::string XStore::SerializeWalRecord(const XStore::WalRecord &record) {
    std::string payload;
    size_t row_block_size = record.rows.size() * (8 + static_cast<size_t>(record.row_length));
    payload.reserve(2 + 2 + 4 + 4 + record.db_name.size() + row_block_size);
    AppendPod<uint16_t>(payload, static_cast<uint16_t>(record.db_name.size()));
    AppendPod<uint16_t>(payload, record.row_length);
    AppendPod<int32_t>(payload, static_cast<int32_t>(record.time_resolution));
    AppendPod<uint32_t>(payload, static_cast<uint32_t>(record.rows.size()));
    payload.append(record.db_name);
    for (const auto &row : record.rows) {
        AppendPod<uint64_t>(payload, row.epoch_time);
        payload.append(row.row_bytes);
    }

    uint32_t crc = Crc32(payload.data(), payload.size());

    std::string out;
    out.reserve(4 + 4 + payload.size());
    AppendPod<uint32_t>(out, static_cast<uint32_t>(4 + payload.size()));
    AppendPod<uint32_t>(out, crc);
    out.append(payload);
    return out;
}

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
    if (Crc32(payload, payload_len) != stored_crc) return false;

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
    // One row at a time for v1 -- each row gets its own offset/lock, same
    // shape as UnaryInsert's pre-WAL inline path. Coalescing same-year rows
    // into fewer pwrites (the way WriteRowGroupToFile already does for
    // Batch/Range's direct-write path, XStore_Internal.h) is a natural
    // follow-up once a real multi-row caller exists to benchmark against.
    //
    // Returns true only if every row applied cleanly -- the writer loop's
    // caller uses this (combined with the WAL fsync's own success) to
    // decide whether this record was actually durable, instead of always
    // reporting success regardless of what happened here.
    bool all_rows_ok = true;
    for (const WalRow &row : record_to_apply.rows) {
        TimeStruct curr_ts(row.epoch_time, record_to_apply.time_resolution);
        const std::string &target_file = CachedYearFilePath(record_to_apply.db_base_path, curr_ts.year);

        std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, record_to_apply.row_length,
                                                        curr_ts.last_epoch - curr_ts.first_epoch);
        if (!fd_handle) {
            spdlog::error("[WAL APPLY] [DB: {}] - Unable to open {}.XSTORE", record_to_apply.db_name, curr_ts.year);
            all_rows_ok = false;
            continue;
        }
        int fd = *fd_handle;

        unsigned long long offset = (row.epoch_time - curr_ts.first_epoch) * record_to_apply.row_length;
        ssize_t written;
        {
            FileRangeLockGuard guard(target_file, offset, record_to_apply.row_length, FileRangeLock::LockType::WRITE);
            written = pwrite(fd, row.row_bytes.data(), record_to_apply.row_length, static_cast<off_t>(offset));
        }
        if (written != static_cast<ssize_t>(record_to_apply.row_length)) {
            spdlog::error("[WAL APPLY] [DB: {}] - Short/failed write to {}.XSTORE", record_to_apply.db_name, curr_ts.year);
            all_rows_ok = false;
            continue;
        }
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

bool XStore::EnqueueWalRecord(WalRecord record) {
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
    // before either version of this code existed.
    size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
    size_t shard_idx = std::hash<std::string>{}(record.db_base_path) % active;
    WalShard &shard = wal_shards[shard_idx];

    // Backpressure: wait (never reject) for this shard's queue to drain
    // below the high watermark before adding to it. Uniform across every
    // durability mode, including ASYNC_ONLY -- otherwise a writer that
    // can't keep up would let the queue (and the un-fsync'd, would-be-lost-
    // on-crash backlog) grow without bound while still telling ASYNC_ONLY
    // callers "success".
    while (shard.queue.size_approx() >= XStoreTunables::WAL_QUEUE_HIGH_WATERMARK) {
        std::this_thread::sleep_for(std::chrono::microseconds(XStoreTunables::WAL_BACKPRESSURE_POLL_US));
    }

    std::future<bool> completion_future;
    if (record.durability_mode != XStore_Proto::ASYNC_ONLY) {
        record.completion.emplace();
        completion_future = record.completion->get_future();
    }

    if (!shard.queue.enqueue(std::move(record))) {
        return false;
    }

    if (completion_future.valid()) {
        // Blocks until the writer thread resolves it; get() returns the
        // real durability outcome (false on a WAL fsync failure or a
        // failed apply) instead of unconditionally reporting success --
        // ASYNC_ONLY skips this and only reports "enqueued", by design.
        return completion_future.get();
    }
    return true;
}

void XStore::StartWalWriters(const std::string &data_store_dir) {
    // Writer threads are almost entirely I/O-bound (blocked in fsync() or
    // on the queue's semaphore -- near-zero idle CPU, see the
    // WAL_GROUP_COMMIT_WINDOW_US tunable comment), so the real ceiling on
    // "how many is too many" is contention at the storage device, not CPU
    // headroom -- stays a conservative fraction of core count, not 1:1.
    unsigned hw = std::thread::hardware_concurrency();
    size_t desired = hw == 0 ? XStoreTunables::WAL_SHARD_COUNT : static_cast<size_t>(hw) / 4;
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
    // Writer threads must be stopped and joined before other static-storage
    // objects they touch (the moodycamel queue, spdlog's default logger)
    // begin their own destruction at process exit -- a still-running thread
    // racing that teardown corrupts the heap rather than cleanly
    // terminating. std::atexit callbacks run in reverse registration order,
    // before any static destructors registered after this call, so
    // registering here (at the moment the threads are actually started)
    // guarantees this runs first.
    std::atexit([]() {
        for (size_t i = 0; i < XStoreTunables::WAL_SHARD_COUNT; ++i) {
            wal_shards[i].stop_flag.store(true, std::memory_order_relaxed);
        }
        for (size_t i = 0; i < XStoreTunables::WAL_SHARD_COUNT; ++i) {
            if (wal_shards[i].writer_thread.joinable()) {
                wal_shards[i].writer_thread.join();
            }
        }
    });
}

void XStore::WalWriterLoop(size_t shard_idx, std::string preopened_path, int preopened_fd) {
    WalShard &shard = wal_shards[shard_idx];
    std::vector<WalRecord> batch(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Thread-local to this one writer thread -- no lock needed, only this
    // thread ever touches it. Declared once outside the loop and reused
    // (via .clear()/reset each iteration, or sized once and indexed) rather
    // than re-declared per batch, to avoid a fresh heap allocation on the
    // writer's per-batch hot path.
    std::unordered_map<std::string, int> wal_fds;
    if (preopened_fd >= 0) {
        wal_fds.emplace(std::move(preopened_path), preopened_fd);
    }
    std::unordered_map<std::string, std::string> bytes_by_file;
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
    // completion promise resolved -- lets the catch block below resolve
    // only the ones an exception left unresolved, instead of risking a
    // second set_value() (undefined/throws) on one already resolved by the
    // code that ran before the exception.
    std::vector<bool> resolved(XStoreTunables::WAL_BATCH_MAX_RECORDS);

    while (!shard.stop_flag.load(std::memory_order_relaxed)) {
        size_t count = shard.queue.wait_dequeue_bulk_timed(
            batch.data(), XStoreTunables::WAL_BATCH_MAX_RECORDS,
            std::chrono::microseconds(XStoreTunables::WAL_GROUP_COMMIT_WINDOW_US));
        if (count == 0) continue;

        // SYNC_PER_COMMIT wants the lowest latency possible: fsync as soon
        // as whatever's already queued is grabbed, no added wait. GROUP_
        // COMMIT explicitly opts into lingering for more batching. Scan
        // whatever the blocking dequeue above just grabbed for a
        // SYNC_PER_COMMIT record up front, so a batch that already has one
        // skips the gather loop below entirely instead of paying the gather
        // window for no reason.
        auto has_sync_per_commit = [&](size_t begin, size_t end) {
            for (size_t i = begin; i < end; ++i) {
                if (batch[i].durability_mode == XStore_Proto::SYNC_PER_COMMIT) return true;
            }
            return false;
        };
        bool flush_now = has_sync_per_commit(0, count);

        // Group-commit gather: the blocking wait above returns the instant
        // the first record arrives, which alone gives near-zero batching
        // under concurrent load (a non-blocking peek only catches what's
        // already sitting in the queue at that exact instant -- tried that
        // first, it didn't move the needle). Instead, actively linger for
        // up to WAL_GATHER_WINDOW_US, polling every WAL_GATHER_POLL_US,
        // stopping the moment the batch fills, the window elapses, or a
        // SYNC_PER_COMMIT record joins the batch -- whichever comes first,
        // same dual-trigger shape as Kafka's linger.ms + batch.size, plus
        // the SYNC_PER_COMMIT override. This DOES cost a lone GROUP_COMMIT
        // writer real added latency (up to the full window, every commit)
        // in exchange for real batching once per-DB hashing (see
        // EnqueueWalRecord) funnels concurrent writers of the same DB onto
        // this one shard. A lone SYNC_PER_COMMIT writer pays none of it.
        if (!flush_now && count < XStoreTunables::WAL_BATCH_MAX_RECORDS) {
            auto gather_deadline = std::chrono::steady_clock::now()
                + std::chrono::microseconds(XStoreTunables::WAL_GATHER_WINDOW_US);
            while (count < XStoreTunables::WAL_BATCH_MAX_RECORDS) {
                size_t before = count;
                size_t got = shard.queue.try_dequeue_bulk(batch.data() + count, XStoreTunables::WAL_BATCH_MAX_RECORDS - count);
                count += got;
                if (got > 0 && has_sync_per_commit(before, count)) {
                    break;
                }
                if (count >= XStoreTunables::WAL_BATCH_MAX_RECORDS || std::chrono::steady_clock::now() >= gather_deadline) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::microseconds(XStoreTunables::WAL_GATHER_POLL_US));
            }
        }

        std::fill(resolved.begin(), resolved.begin() + static_cast<long>(count), false);

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
            bytes_by_file.clear();
            for (size_t i = 0; i < count; ++i) {
                record_wal_path[i] = WalFilePathFor(batch[i].db_base_path, shard_idx);
                bytes_by_file[record_wal_path[i]] += SerializeWalRecord(batch[i]);
            }

            file_durable.clear();
            for (auto &entry : bytes_by_file) {
                const std::string &wal_path = entry.first;
                const std::string &bytes = entry.second;
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
                } else {
                    fd = it->second;
                }

                // fdatasync, not fsync: WAL_FILE_PREALLOC_BYTES is sized (see
                // its comment in XStore_Tunables.h, enforced by a
                // static_assert there) to make this write structurally
                // unable to grow the file past its preallocated region, so
                // there's no metadata (st_size) change to durably persist --
                // only the written bytes matter.
                ssize_t written = pwrite(fd, bytes.data(), bytes.size(), 0);
                if (written != static_cast<ssize_t>(bytes.size()) || fdatasync(fd) != 0) {
                    spdlog::error("[WAL] - Failed to durably write/fdatasync {}", wal_path);
                    continue;
                }
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
                    batch[i].completion->set_value(record_ok);
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
                    batch[i].completion->set_value(false);
                }
            }
        } catch (...) {
            spdlog::error("[WAL] - Writer loop caught unknown exception, failing this batch");
            for (size_t i = 0; i < count; ++i) {
                if (!resolved[i] && batch[i].completion) {
                    batch[i].completion->set_value(false);
                }
            }
        }
    }
}
