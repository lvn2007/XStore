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

bool XStore::EnqueueWalRecord(WalRecord record) {
    size_t shard_idx = std::hash<std::string>{}(record.db_base_path) % XStoreTunables::WAL_SHARD_COUNT;
    WalShard &shard = wal_shards[shard_idx];

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

void XStore::StartWalWriters() {
    for (size_t i = 0; i < XStoreTunables::WAL_SHARD_COUNT; ++i) {
        wal_shards[i].writer_thread = std::thread(&XStore::WalWriterLoop, i);
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

void XStore::WalWriterLoop(size_t shard_idx) {
    WalShard &shard = wal_shards[shard_idx];
    std::vector<WalRecord> batch(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Thread-local to this one writer thread -- no lock needed, only this
    // thread ever touches it. Declared once outside the loop and reused
    // (via .clear()/reset each iteration, or sized once and indexed) rather
    // than re-declared per batch, to avoid a fresh heap allocation on the
    // writer's per-batch hot path.
    std::unordered_map<std::string, int> wal_fds;
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
                    // Only pay for create_directories() the first time this
                    // path is actually opened, not once per record in the
                    // batch -- the directory only needs to exist once.
                    std::filesystem::create_directories(std::filesystem::path(wal_path).parent_path());
                    fd = open(wal_path.c_str(), O_WRONLY | O_CREAT, 0644);
                    if (fd < 0) {
                        spdlog::error("[WAL] - Unable to open {}", wal_path);
                        continue;
                    }
                    wal_fds.emplace(wal_path, fd);
                } else {
                    fd = it->second;
                }

                ssize_t written = pwrite(fd, bytes.data(), bytes.size(), 0);
                if (written != static_cast<ssize_t>(bytes.size()) || fsync(fd) != 0) {
                    spdlog::error("[WAL] - Failed to durably write/fsync {}", wal_path);
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

            // Checkpoint: only truncate a file that actually fsync'd
            // successfully above -- truncating a file whose fsync FAILED
            // would discard bytes that were never durable anywhere else,
            // and truncating unconditionally on a shared "all succeeded"
            // flag risked wiping a DIFFERENT file's still-needed bytes
            // whenever a batch touched more than one file. Each file's
            // truncate decision is now independent, matching its own
            // fsync outcome.
            for (auto &entry : bytes_by_file) {
                if (file_durable[entry.first]) {
                    ftruncate(wal_fds[entry.first], 0);
                }
            }
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
