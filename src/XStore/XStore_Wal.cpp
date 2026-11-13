// src/XStore/XStore_Wal.cpp
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_Crc32c.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_PendingMap.h"
#include "include/XStore/XStore_Indexer.h"
#include "include/XStore/XStore_WalCheckpoint.h"
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
#include <set>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

std::array<XStore::WalShard, XStoreTunables::WAL_SHARD_COUNT> XStore::wal_shards;
std::atomic<size_t> XStore::active_wal_shard_count{XStoreTunables::WAL_SHARD_COUNT};
std::atomic<uint64_t> XStore::wal_ring_stalls{0};
std::atomic<uint64_t> XStore::wal_direct_records{0};
std::atomic<uint64_t> XStore::wal_ring_records{0};
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

// Writer found its ring file fenced (failed replay): say so once per path per minute (error path only).
// Heap-allocated and leaked on purpose: the writer threads may still log during static destruction.
void LogFencedWalOncePerMinute(const std::string &wal_path) {
    struct State { std::mutex mu; std::unordered_map<std::string, std::chrono::steady_clock::time_point> last; };
    static State *state = new State;
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        auto [it, inserted] = state->last.try_emplace(wal_path, now);
        if (!inserted) {
            if (now - it->second < std::chrono::minutes(1)) return;
            it->second = now;
        }
    }
    spdlog::error("[WAL] - WAL file {} is fenced (failed replay); inserts to this shard fail until restart", wal_path);
}

// A writer pwritev/fdatasync errno that means the device may be dropping pages (anything but a retried
// EINTR/EAGAIN) and therefore poisons the checkpointer.
constexpr bool IsRealWalIoError(int err) { return err != 0 && err != EINTR && err != EAGAIN; }

std::atomic<uint64_t> g_inline_reread_folds{0};

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
    // The rows section is already WalRecord::wire's layout: one copy, no per-row allocation.
    out_record.wire.assign(payload + off, expected_rows_bytes);
    out_record.row_count = row_count;
    out_record.replayed = true;
    out_record.has_obs = false;   // the record is reused across the replay loop: a replayed record carries no observations

    consumed = 4 + record_len;
    return true;
}

// Record v2 (ring format): [u32 recordLen][u32 crc32c][u64 lsn][u16 dbNameLen]
// [u16 rowLength][i32 timeRes][u32 rowCount][dbName][rows as in v1].
// recordLen counts every byte after itself; crc32c covers lsn..last row byte.
// dbNameLen == walring::PAD_DB_NAME_LEN marks a PAD (ring-end filler, no rows).
XStore::RingParse XStore::ParseWalRecordV2(const char *data, size_t len, uint64_t expected_lsn, size_t &consumed, XStore::WalRecord &out_record,
                                           bool *corrupt) {
    // kEnd is "clean" (corrupt=false) only for a CRC-valid record carrying another LSN: stale lap bytes.
    // Everything else that stops the parse is damage (or a torn tail).
    bool dummy = false;
    bool &damaged = corrupt != nullptr ? *corrupt : dummy;
    damaged = true;
    if (len < walring::RECORD_FIXED_BYTES) return RingParse::kEnd;
    uint32_t record_len;
    std::memcpy(&record_len, data, 4);
    // Widened before the addition, same wraparound reasoning as ParseWalRecord.
    if (record_len < walring::RECORD_FIXED_BYTES - 4 || len < static_cast<size_t>(record_len) + 4) return RingParse::kEnd;

    uint32_t stored_crc;
    std::memcpy(&stored_crc, data + 4, 4);
    const char *payload = data + 8;
    const size_t payload_len = record_len - 4;
    if (Crc32c(payload, payload_len) != stored_crc) return RingParse::kEnd;

    uint64_t lsn; std::memcpy(&lsn, payload, 8);
    if (lsn != expected_lsn) { damaged = false; return RingParse::kEnd; }

    size_t off = 8;
    uint16_t db_name_len; std::memcpy(&db_name_len, payload + off, 2); off += 2;
    uint16_t row_length;  std::memcpy(&row_length, payload + off, 2); off += 2;
    int32_t time_res;     std::memcpy(&time_res, payload + off, 4); off += 4;
    uint32_t row_count;   std::memcpy(&row_count, payload + off, 4); off += 4;

    if (db_name_len == walring::PAD_DB_NAME_LEN) {
        consumed = 4 + static_cast<size_t>(record_len);
        damaged = false;
        return RingParse::kPad;
    }

    if (off + db_name_len > payload_len) return RingParse::kEnd;
    const size_t expected_rows_bytes = static_cast<size_t>(row_count) * (8u + row_length);
    if (off + db_name_len + expected_rows_bytes != payload_len) return RingParse::kEnd;

    out_record.db_name.assign(payload + off, db_name_len);
    off += db_name_len;
    out_record.row_length = row_length;
    out_record.time_resolution = static_cast<XStore_Proto::TimeUnit>(time_res);
    // Same single copy of the rows section as ParseWalRecord.
    out_record.wire.assign(payload + off, expected_rows_bytes);
    out_record.row_count = row_count;
    out_record.replayed = true;
    out_record.has_obs = false;   // as ParseWalRecord
    consumed = 4 + static_cast<size_t>(record_len);
    damaged = false;
    return RingParse::kOk;
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

// One file's slice of a writer sub-batch: the ring it appends to, the
// reserved LSN range, and the iovecs (header+row pieces) to pwritev there.
struct SubBatchFile {
    walring::Ring *ring = nullptr;
    std::vector<struct iovec> iov;
    size_t total = 0;      // ring-bound bytes of this file's records in the sub-batch
    uint64_t start = 0;    // first LSN of the run (after any PAD)
    uint64_t cursor = 0;   // next record's LSN while headers are being built
    size_t pad = 0;        // PAD record bytes to write at start - pad (0 = none)
    bool reserved = false;
    bool durable = false;
    bool overloaded = false;   // Reserve timed out: its SYNC callers are told "overloaded"
};

// kRing: appended to the ring. kLarge: big enough that it MAY go direct -- ChooseRoute decides when the
// writer reaches it (not at gather time: earlier records of the same batch must have been applied first,
// or RingClear would miss them); the decision rewrites it to kRing. kFail: can never be handled (no ring
// could be opened, or a single-row record larger than half the ring); failed without touching any file.
enum class WalRecordKind : uint8_t { kRing, kLarge, kFail, kStale };

// The observation arrays agree with row_count and with each other: no ends (replay) and no observations, or one end
// per row, non-decreasing, the last one exactly at each array's size. Counts only (one pass over the ends, none over
// the values: a CatIdxBloomValue's bit positions are in range by construction, only BloomValueOf creates one), so
// ApplyWalRecord's per-run reads can trust every index and a malformed record fails before any byte is written.
bool ObservationsWellFormed(const XStore::WalRecord &record) {
    if (record.zmap_numeric.size() != record.zmap_values.size()) return false;
    if (record.obs_end.empty()) return record.zmap_values.empty() && record.catidx_vals.empty();
    if (record.obs_end.size() != record.row_count) return false;
    size_t zmap_prev = 0;
    size_t catidx_prev = 0;
    for (const XStore::WalRecord::ObsEnd &end : record.obs_end) {
        if (end.zmap_end < zmap_prev || end.zmap_end > record.zmap_values.size() || end.catidx_end < catidx_prev ||
            end.catidx_end > record.catidx_vals.size()) {
            return false;
        }
        zmap_prev = end.zmap_end;
        catidx_prev = end.catidx_end;
    }
    return zmap_prev == record.zmap_values.size() && catidx_prev == record.catidx_vals.size();
}

// Clears a run's pending mark when the run's loop iteration ends, whichever way: after the inline fold, after a failed
// write (not folded, as before) or on an exception, so no block is left pending forever. Dismiss() keeps the marks: the
// run was handed to the indexer, which clears them after its fold.
struct PendingRunClear {
    xstore::PendingMap *map;
    uint64_t first_row;
    uint64_t row_count;
    PendingRunClear(xstore::PendingMap *m, uint64_t first, uint64_t count) : map(m), first_row(first), row_count(count) {}
    PendingRunClear(const PendingRunClear &) = delete;
    PendingRunClear &operator=(const PendingRunClear &) = delete;
    ~PendingRunClear() { if (map) map->ClearRows(first_row, row_count); }
    void Dismiss() { map = nullptr; }
};

// A backlog reservation not yet handed to the indexer: whatever no hint took is released at scope exit (an exception
// between TryReserve and the last Enqueue).
class ReservationHold {
public:
    ReservationHold(xstore::Indexer &indexer, uint64_t bytes) : indexer_(indexer), bytes_(bytes) {}
    ReservationHold(const ReservationHold &) = delete;
    ReservationHold &operator=(const ReservationHold &) = delete;
    ~ReservationHold() { if (bytes_ != 0) indexer_.Release(bytes_); }
    [[nodiscard]] uint64_t Take(uint64_t bytes) { bytes_ -= bytes; return bytes; }

private:
    xstore::Indexer &indexer_;
    uint64_t bytes_;
};

} // namespace

bool XStore::ApplyWalRecord(const WalRecord &record_to_apply, const std::shared_ptr<walring::Ring> &ring, uint64_t ring_lsn_end) {
    // Precondition: every row in one WalRecord belongs to the same (db,
    // year) file, and rows are ordered ascending by epoch_time. True for
    // every current producer -- UnaryInsert's 1-row records trivially
    // satisfy this; BatchInsert/RangeInsert build one WalRecord per
    // year-bucket group (see XStore_BatchOps.cpp/XStore_RangeOps.cpp),
    // sorted ascending before being handed off here (undocumented-
    // enforcement, same style as the rest of this precondition).
    const size_t row_count = record_to_apply.row_count;
    if (row_count == 0) return true;
    // Every accessor below reads wire (and, for a has_obs record, the observation arrays) by row index and offset: a
    // record whose containers disagree with row_count is a producer bug, refused before anything is read or written.
    // (A ring record never gets here with a mismatched wire: framing checks its size against the planned ring size
    // first.) A has_obs record has exactly one observation end per row (the fold below indexes obs_end by row).
    if (record_to_apply.wire.size() != row_count * record_to_apply.RowStride() ||
        (record_to_apply.has_obs && (record_to_apply.obs_end.size() != row_count || !ObservationsWellFormed(record_to_apply)))) {
        spdlog::error("[WAL APPLY] [DB: {}] - malformed record ({} rows, {} wire bytes, {} observation ends, {} zone-map "
                      "observations, {} catidx values): not applied",
                      record_to_apply.db_name, row_count, record_to_apply.wire.size(), record_to_apply.obs_end.size(),
                      record_to_apply.zmap_values.size(), record_to_apply.catidx_vals.size());
        // A failed apply of a durable ring record still notes its file dirty (see XStore_WalCheckpoint.h), like
        // drop_stale below; the file is derived from the first epoch, which exists whenever the wire holds a row slot.
        if (record_to_apply.wire.size() >= sizeof(uint64_t)) {
            const TimeStruct bad_first_ts(record_to_apply.EpochAt(0), record_to_apply.time_resolution);
            walring::Dirty().NoteRingMarkOnly(ring, CachedYearFilePath(record_to_apply.db_base_path, bad_first_ts.year), ring_lsn_end);
        }
        return false;
    }

    TimeStruct first_ts(record_to_apply.EpochAt(0), record_to_apply.time_resolution);
    const std::string &target_file = CachedYearFilePath(record_to_apply.db_base_path, first_ts.year);

    // The DB was removed (and maybe re-created, with another row length) since this record's cfg was read: nothing of
    // it may be written, like a record of a DB that is gone. Checked twice. Here, before the open: a record already
    // stale does not create (or size, at its old geometry) the re-created DB's year file. Again after the open (below).
    const auto drop_stale = [&]() {
        spdlog::warn("[WAL APPLY] [DB: {}] - The DB was removed after this record was built; its {} row(s) are dropped",
                     record_to_apply.db_name, row_count);
        walring::Dirty().NoteRingMarkOnly(ring, target_file, ring_lsn_end);
        return false;
    };
    if (!CfgEpochCurrent(record_to_apply.db_name, record_to_apply.cfg_epoch)) return drop_stale();

    std::shared_ptr<int> fd_handle = GetOrOpenFile(target_file, record_to_apply.row_length,
                                                    first_ts.last_epoch - first_ts.first_epoch);
    if (!fd_handle) {
        spdlog::error("[WAL APPLY] [DB: {}] - Unable to open {}.XSTORE", record_to_apply.db_name, first_ts.year);
        // A failed apply of a ring record that is already durable must still register its file: RingClear must not
        // call the file clean, or a later direct write could be overwritten by this record's replay after a crash.
        // Nothing was written here, so only the ring mark is recorded and no byte range: the checkpointer must not
        // try to fdatasync a file we could not open (an unreadable file would hold every cycle). The checkpoint is
        // NOT held. Replay and BYPASS pass no ring and keep their old behaviour (NoteRingMarkOnly ignores them).
        walring::Dirty().NoteRingMarkOnly(ring, target_file, ring_lsn_end);
        return false;
    }
    // The binding check, AFTER the open (a record can turn stale between the first check and the open): an open of
    // the re-created DB's file comes after that removal's epoch bump, so it is seen here; a record that passes holds
    // an fd opened before the bump, which the removal waits for (cached or evicted-but-held, see EraseOpenFile*)
    // before a re-create can run (CreateDB refuses while the removal's scope is registered). The index lookups below
    // come after it, so a dropped record never creates index files in a re-created DB. Per record: two atomic loads
    // of one cache line (none per row).
    if (!CfgEpochCurrent(record_to_apply.db_name, record_to_apply.cfg_epoch)) return drop_stale();
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
    // it is the exact width this record's rows are folded at (captured at
    // enqueue time, before the .CFG's columnCount could move underneath), so
    // the file this creates can never claim to track more columns than were
    // actually folded into it.
    //
    // Both files are pinned until this record is applied: the registry may not evict (destroy) a file this apply
    // still writes. The pin is one atomic increment inside the registry lookup and one decrement at return.
    // Same reasoning as zmap for the catidx, same (db, year, row_length, tracked_column_count) key -- the bloom filter
    // is only ever updated here, alongside zmap, after a row's real pwrite is confirmed durable.
    // A cold open that fails (the year is being removed, see RemovalCovers, or the file cannot be opened) fails this
    // record before any byte is written, like the data-file open above, instead of throwing out of the WAL writer's
    // whole batch.
    ZoneMapFile::Pin zmap_pin;
    CatIdxFile::Pin catidx_pin;
    try {
        zmap_pin = PinZoneMap(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                              record_to_apply.tracked_column_count);
        catidx_pin = PinCatIdx(record_to_apply.db_base_path, first_ts.year, record_to_apply.row_length,
                               record_to_apply.tracked_column_count);
    } catch (const std::exception &e) {
        spdlog::error("[WAL APPLY] [DB: {}] - Unable to open the indexes of {}.XSTORE: {}", record_to_apply.db_name,
                      first_ts.year, e.what());
        walring::Dirty().NoteRingMarkOnly(ring, target_file, ring_lsn_end);
        return false;
    }
    ZoneMapFile &zmap = *zmap_pin;
    CatIdxFile &catidx = *catidx_pin;
    // The year's one map, shared by both files; null only for a file opened outside the registry. Owned by the pinned
    // zmap for this whole call (attached before it was shared, never replaced), so no reference count is taken.
    xstore::PendingMap *const pending = zmap.Pending().get();

    std::vector<unsigned long long> offsets(row_count);
    for (size_t i = 0; i < row_count; ++i) {
        offsets[i] = (record_to_apply.EpochAt(i) - first_ts.first_epoch) * record_to_apply.row_length;
    }

    // WRITE locks on exactly the record's runs (contiguous rows; a duplicate row joins its run), taken all at once, so
    // other lock users conflict only on these rows and a reader never sees the record half-applied. Past the cap the
    // record's whole min-to-max span is one lock. While a keep-old rebuild runs on either index file the cap is the
    // keep-old one (see the tunable). A rebuild that starts right after this check only means this one record used
    // the normal cap, which is correct either way (the cap is a performance choice, not a correctness one).
    // OldWidth() is the OLD file's width: non-zero whenever an old index is kept, because production files are created
    // at EffectiveTrackedColumnCount >= 64 (a 0-width old file would only mean the larger cap during that rebuild).
    const uint64_t run_cap = (zmap.OldWidth() != 0 || catidx.OldWidth() != 0)
                                 ? XStoreTunables::WAL_APPLY_MAX_RUN_LOCKS_KEEP_OLD_REBUILD
                                 : XStoreTunables::WAL_APPLY_MAX_RUN_LOCKS;
    thread_local std::vector<FileRangeLock::Run> lock_runs;
    lock_runs.clear();
    const unsigned long long row_length = record_to_apply.row_length;
    lock_runs.push_back({offsets.front(), row_length});
    for (size_t i = 1; i < offsets.size(); ++i) {
        FileRangeLock::Run &last = lock_runs.back();
        if (offsets[i] == last.start + last.length) {
            last.length += row_length;
        } else if (offsets[i] != offsets[i - 1]) {
            if (lock_runs.size() == run_cap) {   // over the cap: stop collecting
                lock_runs.assign(1, {offsets.front(), (offsets.back() + row_length) - offsets.front()});
                break;
            }
            lock_runs.push_back({offsets[i], row_length});
        }
    }
    FileRangeLockRunsGuard guard(target_file, lock_runs.data(), lock_runs.size());

    // Thread-local scratch buffer, grown to fit the largest run applied so
    // far and reused across calls -- avoids a heap allocation per run.
    thread_local std::vector<char> run_buf;
    thread_local std::vector<xstore::IndexRun> fold_runs;   // the one run FoldStoredRows folds inline (no allocation once grown)
    // has_obs records only: one batch per run for each index, folded under one mutex hold per file. Both folds read the
    // record's own observations and values in place through views; only the run's rows and its run-relative ends are
    // built, in these reused vectors (no allocation once grown). The record is const for the whole call and outlives
    // both UpdateRows calls, and the vectors are not touched between building a run's views and those calls.
    thread_local std::vector<uint64_t> run_row_index;      // shared by both views
    thread_local std::vector<size_t> zmap_obs_end;
    thread_local std::vector<size_t> catidx_value_end;
    // Values in columns past the catidx width are never folded (the view's column_limit). Read once, under the range
    // lock: a widen begun after this read only widens the file and starts it at F = 0, and the builder cannot fold
    // these rows (it needs a READ lock on them) before this record is applied, so no folded row needs a dropped value.
    const uint32_t catidx_width = catidx.TrackedColumnCount();
    const std::vector<WalRecord::ObsEnd> &obs_end = record_to_apply.obs_end;   // row_count entries when has_obs (checked above)
    bool all_rows_ok = true;
    xstore::Indexer &indexer = xstore::Indexer::Instance();   // once per record (a function-static's guard load)
    bool sidecars_marked_dirty = false;                        // by this record's first hand-off

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
            std::memcpy(run_buf.data() + (i - run_start) * record_to_apply.row_length, record_to_apply.RowAt(i), record_to_apply.row_length);
        }

        // Overwrite detection: UpdateRow below only ADDS, so a run replacing a row already folded into its zone-map
        // block must mark that block inexact (aggregates then scan it) BEFORE the new bytes land. The range lock
        // above keeps every other writer off these rows. A sorted append into fresh blocks never reads; otherwise
        // read the old bytes (a never-written or deleted row has first byte 0, the same hole test the scans use).
        // An empty RowData also serializes to a zero first byte: it is a hole to every fold, so it was never folded.
        const uint64_t run_first_row = record_to_apply.EpochAt(run_start) - first_ts.first_epoch;
        if (zmap.NeedsOverwriteCheck(run_first_row, run_rows)) {
            thread_local std::vector<char> old_buf;
            if (old_buf.size() < run_bytes) old_buf.resize(run_bytes);
            uint64_t lo = run_rows, hi = 0;
            if (pread(fd, old_buf.data(), run_bytes, static_cast<off_t>(offsets[run_start])) != static_cast<ssize_t>(run_bytes)) {
                lo = 0; hi = run_rows - 1; // unknown old contents: assume all were folded
            } else {
                for (size_t r = 0; r < run_rows; ++r) {
                    if (old_buf[r * record_to_apply.row_length] != 0) { lo = std::min<uint64_t>(lo, r); hi = r; }
                }
            }
            if (lo <= hi) zmap.MarkInexact(run_first_row + lo, run_first_row + hi);
        }

        // Outstanding until this iteration ends (I1: set before the bytes are readable). The clearer is declared first
        // so a MarkRows that throws part way is undone too.
        PendingRunClear pending_run(pending, run_first_row, run_rows);
        if (pending) pending->MarkRows(run_first_row, run_rows);
        ssize_t written = pwrite(fd, run_buf.data(), run_bytes, static_cast<off_t>(offsets[run_start]));
        const bool written_ok = written == static_cast<ssize_t>(run_bytes);
        if (!written_ok) {
            spdlog::error("[WAL APPLY] [DB: {}] - Short/failed write to {}.XSTORE", record_to_apply.db_name, first_ts.year);
            all_rows_ok = false;
        } else {
            // The data file now holds bytes the checkpointer must fdatasync
            // before it may pass this record's ring LSN (spec §3.2).
            walring::Dirty().Note(ring, target_file, offsets[run_start], offsets[run_start] + run_bytes, ring_lsn_end);
        }
        // With the pool off the hand-off costs the one Active() load. A full backlog (TryReserve fails) folds inline.
        // Not while a sidecar of the year is rebuilt (a live widen): its scan would fold the pending rows and the pass
        // again. Checked after MarkRows: a widen that begins later sees these marks and waits for them (LiveWidenOneYear).
        // A has_obs record (indexed by the request thread: pool off, SYNC/BYPASS, sparse or unary; see HandOffToPool) is
        // never handed off, even if the pool is on: its observations are folded below.
        if (written_ok && !record_to_apply.has_obs && pending && indexer.Active() && !zmap.Rebuilding() && !catidx.Rebuilding() &&
            indexer.TryReserve(run_bytes)) {
            // Handed to the indexer: the marks stay until its fold. One hint per block the run touches, each holding
            // the bytes of its own rows, so the hints' reservations add up to run_bytes exactly.
            ReservationHold hold(indexer, run_bytes);
            // The inline path marks both sidecars dirty on disk inside UpdateRows; a handed-off run is folded after the
            // ack, so mark them here, under the WRITE lock and before the ack: an unclean stop must find them dirty
            // (spec section 6). Same call and durability as the inline path (header pwrite + fdatasync, once per file
            // until MarkClean; afterwards a mutex and a flag test).
            if (!sidecars_marked_dirty) {
                zmap.MarkDirty();
                catidx.MarkDirty();
                sidecars_marked_dirty = true;
            }
            pending_run.Dismiss();
            const uint64_t rpb = pending->RowsPerBlock();   // the grouping PendingRunsOfBlock reads the hint's block in
            const uint64_t run_end_row = run_first_row + run_rows;
            for (uint64_t block = run_first_row / rpb; block * rpb < run_end_row; ++block) {
                const uint64_t rows = std::min(run_end_row, (block + 1) * rpb) - std::max(run_first_row, block * rpb);
                xstore::IndexHint hint;
                hint.map = zmap.Pending();
                hint.db_base_path = record_to_apply.db_base_path;
                hint.db_name = record_to_apply.db_name;
                hint.year = first_ts.year;
                hint.row_length = record_to_apply.row_length;
                hint.tracked_column_count = record_to_apply.tracked_column_count;
                hint.block = block;
                hint.reserved_bytes = hold.Take(rows * row_length);
                indexer.Enqueue(std::move(hint));
            }
        } else if (written_ok && record_to_apply.replayed) {
            // Replay folds nothing (see WalRecord::replayed); the sidecars are marked dirty, as the replayed rows are
            // not in them until InitDB's rebuild of the year.
            if (!sidecars_marked_dirty) {
                zmap.MarkDirty();
                catidx.MarkDirty();
                sidecars_marked_dirty = true;
            }
        } else if (written_ok && record_to_apply.has_obs) {
            // The request thread built the observations (pool off, SYNC/BYPASS, sparse or unary): fold them, as the base
            // build does. Same code as the
            // Task 7 tree; the run's rows go to each index as one batch (UpdateRows: one mutex hold, one block write per
            // block touched), still under the WRITE lock and before the next run's overwrite check reads the blocks.
            // Never for a row whose write failed.
            run_row_index.clear();
            zmap_obs_end.clear();
            catidx_value_end.clear();
            // The run's observations and values are contiguous in the record: each view starts at the run's first one
            // and each row's end is rebased to it. Indexes were checked by ObservationsWellFormed.
            const size_t zmap_begin = run_start != 0 ? obs_end[run_start - 1].zmap_end : 0;
            const size_t catidx_begin = run_start != 0 ? obs_end[run_start - 1].catidx_end : 0;
            for (size_t i = run_start; i < run_end; ++i) {
                // An empty RowData (zero first byte, no observations) is a hole, as FoldStoredRows and the rebuild see
                // it: not folded, so pad_to bumps no nulls for it and the bytes equal the rows-only fold's. (Not in the
                // Task 7 tree, which counted its nulls.)
                if (record_to_apply.RowAt(i)[0] == 0) continue;
                run_row_index.push_back(record_to_apply.EpochAt(i) - first_ts.first_epoch);
                zmap_obs_end.push_back(obs_end[i].zmap_end - zmap_begin);
                catidx_value_end.push_back(obs_end[i].catidx_end - catidx_begin);
            }
            const size_t fold_rows = run_row_index.size();
            if (fold_rows != 0) {   // a run of only holes takes no mutex, as FoldStoredRows
                ZoneMapFoldView zmap_view{run_row_index.data(), zmap_obs_end.data(), fold_rows, nullptr,
                                          record_to_apply.zone_map_pad_to};   // one pad_to per record
                zmap_view.values = record_to_apply.zmap_values.data() + zmap_begin;
                zmap_view.numeric = &record_to_apply.zmap_numeric;
                zmap_view.numeric_base = zmap_begin;
                zmap.UpdateRows(zmap_view);
                catidx.UpdateRows(CatIdxFoldView{run_row_index.data(), catidx_value_end.data(), fold_rows,
                                                 record_to_apply.catidx_vals.data() + catidx_begin, catidx_width});
            }
        } else if (written_ok) {
            // Backlog 0, a full backlog or a year being rebuilt (ruling 2): the run's own rows, read back from the page
            // cache the pwrite above just filled, folded under the WRITE lock this thread holds by the indexer's fold
            // function (one fold function, no second code path), at the record's width; values in columns at or past
            // the catidx width read above are not folded. Never for a row whose write failed.
            g_inline_reread_folds.fetch_add(1, std::memory_order_relaxed);
            fold_runs.assign(1, {run_first_row, run_rows});
            if (!FoldStoredRows(fd, record_to_apply.row_length, record_to_apply.tracked_column_count, catidx_width, fold_runs,
                                zmap, catidx)) {
                // The read-back failed: the run is written but not (or only partly) folded. Its marks stay, so no query
                // trusts its blocks (min/max/sum and the bloom are scanned), it is inexact, and the dirty sidecars are
                // rebuilt at the next start (a clean shutdown keeps a year with pending rows dirty).
                pending_run.Dismiss();
                zmap.MarkInexact(run_first_row, run_first_row + run_rows - 1);
                zmap.MarkDirty();
                catidx.MarkDirty();
                spdlog::error("[WAL APPLY] [DB: {}] - Cannot read back {} row(s) of {}.XSTORE to index them; its indexes are "
                              "marked dirty and rebuilt at the next start", record_to_apply.db_name, run_rows, first_ts.year);
            }
        }
        run_start = run_end;
    }
    if (!all_rows_ok && ring && ring_lsn_end != 0) {
        // Some bytes of the failed runs may have landed: note the record's whole row span (the ring mark is what
        // RingClear needs, the range makes the checkpointer sync it). The checkpoint is NOT held.
        walring::Dirty().Note(ring, target_file, offsets.front(), offsets.back() + record_to_apply.row_length, ring_lsn_end);
    }
    // The run vectors keep their capacity for the next record (8 B per row each of the largest run so far), except
    // after a very large run: above 2 MB a vector is freed rather than kept per applying thread.
    constexpr size_t kKeepBatchValues = size_t{1} << 18;
    if (run_row_index.capacity() > kKeepBatchValues) run_row_index = std::vector<uint64_t>{};
    if (zmap_obs_end.capacity() > kKeepBatchValues) zmap_obs_end = std::vector<size_t>{};
    if (catidx_value_end.capacity() > kKeepBatchValues) catidx_value_end = std::vector<size_t>{};
    return all_rows_ok;
}

bool XStore::EnqueueWalRecord(WalRecord record, bool *overloaded) {
    // Writers are stopped after Shutdown(); a SYNC wait here would never wake.
    if (IsShutDown()) return false;
    walring::ParkWriterIfPausedForTests(3);   // tests only: a request thread that built its record before a removal and queues it after
    // Shard = f(db, year) only, never the calling thread: every write to a given row (one (db, year) file) goes
    // through one shard's queue and one writer thread. For SYNC inserts that makes ring order == acknowledgment order
    // (an acknowledged record is already dequeued before a later one starts), so replay (one ring at a time)
    // cannot re-apply an older write over a newer one. ASYNC is acknowledged before the record is queued and the
    // queue is FIFO only per enqueuing thread, so ASYNC writes to one row from two threads have no defined order. Different DBs and different years
    // spread across shards; one hot DB+year funnels through one shard, which is also what lets the group-commit
    // gather window (see WalWriterLoop) amortize fsyncs on a single device.
    const size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
    const size_t shard_idx = WalShardFor(record.db_base_path, record.year, active);
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
        const bool result = completion->result;
        if (completion->overloaded) {   // failure only: the success path pays this one read
            completion->overloaded = false;
            if (overloaded != nullptr) *overloaded = true;
        }
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
        // Eagerly open (and, if the file is new/empty, initialize) this
        // shard's ring here, not lazily on first write -- production has
        // exactly one data_store_dir, known now, so there's no reason to pay
        // the first commit's latency for it. A non-empty file WITHOUT a valid
        // ring header is a legacy WAL that ReplayWal (called later, from
        // InitDB) still has to read: GetOrOpenRing returns nullptr for it
        // rather than wiping it, and the writer opens it lazily on its first
        // append instead.
        std::string wal_path = WalFilePathForDataStoreDir(data_store_dir, i);
        std::shared_ptr<walring::Ring> ring = walring::GetOrOpenRing(wal_path, static_cast<uint32_t>(i));
        wal_shards[i].writer_thread = std::thread(&XStore::WalWriterLoop, i, std::move(ring));
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
    // Started last, stopped last (StopWalWriters, after the writers are joined):
    // it syncs what the writers applied and advances the rings' checkpoints.
    walring::Checkpointer::Instance().Start();
}

std::atomic<bool> XStore::shutdown_flag_{false};

namespace {
std::atomic<int64_t> g_indexer_drain_bound_ms{XStoreTunables::INDEXER_DRAIN_BOUND_MS};
}  // namespace

void XStore::SetIndexerDrainBoundForTests(std::chrono::milliseconds bound) {
    g_indexer_drain_bound_ms.store(bound.count(), std::memory_order_relaxed);
}

uint64_t XStore::InlineRereadFoldsForTests() { return g_inline_reread_folds.load(std::memory_order_relaxed); }

void XStore::StopWalWriters(bool final_checkpoint_cycle) {
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
    // Writers are joined: nothing applies any more. One final cycle syncs every
    // data file they touched and advances each ring's checkpoint to its head,
    // then the checkpointer thread is joined. Also runs from the atexit hook.
    if (final_checkpoint_cycle) {
        walring::Checkpointer::Instance().StopAfterFinalCycle();
    } else {
        walring::Checkpointer::Instance().StopWithoutFinalCycleForTests();
    }
}

void XStore::Shutdown() {
    // Flag first: from here every insert entry point and EnqueueWalRecord rejects, so no
    // write can race the sequence below.
    if (shutdown_flag_.exchange(true, std::memory_order_acq_rel)) return;

    // Joins the writers, then the checkpointer's final cycle fdatasyncs every data file
    // dirtied since the previous cycle (by path, throwaway fd) and advances each ring's
    // checkpoint to its head.
    StopWalWriters();
    // The writers are joined, so no hint arrives any more: let the indexer fold what is queued (bounded). A pool that
    // never started returns at once.
    const std::chrono::milliseconds drain_bound(g_indexer_drain_bound_ms.load(std::memory_order_relaxed));
    xstore::Indexer::Instance().StopAndDrain(drain_bound);

    // An index may only be marked clean once every data file it describes is durable.
    // Earlier cycles synced the files that were clean by the final one, so the gate is
    // the final cycle's outcome (a poisoned checkpointer never completes a cycle, so that
    // also covers "never poisoned": after a real writeback error the kernel may have
    // dropped pages that later syncs report as fine) -- not per-path membership in the
    // last cycle's synced set. Nothing is re-synced here: a fresh fd would not see a
    // prior writeback error.
    if (!walring::Checkpointer::Instance().FinalCycleOk()) {
        spdlog::error("[SHUTDOWN] - indexes left dirty (final checkpoint did not complete / checkpointer poisoned); "
                      "next start will rebuild them");
        return;
    }

    // Registry mutex is held across MarkClean(): it also guards entry lifetime against
    // LRU eviction. Order is registry mutex -> file mutex; no file method takes a
    // registry mutex, so no inversion. A write racing in after MarkClean simply
    // re-marks the file dirty.
    // Rows the indexer did not fold (the drain ran out, a pass failed, a hint was dropped) keep their year's sidecars
    // dirty. Per map, not BacklogBytes(): a failed pass or a dropped hint leaves pending rows with nothing reserved.
    const bool any_pending = xstore::PendingMaps::AnyPending();
    if (any_pending) {
        spdlog::warn("[SHUTDOWN] - rows not indexed yet: their years' indexes stay dirty and are rebuilt at the next start");
    }
    auto mark_registry_clean = [any_pending](std::mutex &registry_mutex, auto &registry, const char *what) {
        std::lock_guard<std::mutex> lock(registry_mutex);
        for (const auto &entry : registry) {
            if (entry.second->Rebuilding()) { // MarkClean refuses it by design: not an error
                spdlog::warn("[SHUTDOWN] - {} {}: an index was still being rebuilt; left dirty, rebuilt at the next start",
                             what, entry.first);
                continue;
            }
            // The file's own map: the one its writers marked through.
            if (any_pending && entry.second->Pending() && entry.second->Pending()->PendingBlockCount() != 0) continue;
            if (!entry.second->MarkClean()) {
                spdlog::error("[SHUTDOWN] - MarkClean failed for {} {}; leaving it dirty", what, entry.first);
            }
        }
    };
    mark_registry_clean(zone_map_registry_mutex_, zone_map_registry_, ".zmap");
    mark_registry_clean(catidx_registry_mutex_, catidx_registry_, ".catidx");
}

void XStore::ResetShutdownForTests() {
    if (!shutdown_flag_.exchange(false, std::memory_order_acq_rel)) return;
    // Writers resolve their ring lazily from each record's path when handed
    // no preopened ring.
    size_t active = active_wal_shard_count.load(std::memory_order_relaxed);
    walring::Checkpointer::Instance().Start();
    for (size_t i = 0; i < active; ++i) {
        wal_shards[i].stop_flag.store(false, std::memory_order_relaxed);
        wal_shards[i].writer_thread = std::thread(&XStore::WalWriterLoop, i, std::shared_ptr<walring::Ring>{});
    }
#ifdef XSTORE_WAL_STATS_ENABLED
    wal_stats_stop_flag.store(false, std::memory_order_relaxed);
    wal_stats_reporter_thread = std::thread(&XStore::WalStatsReporterLoop);
#endif
}

void XStore::WalSimulateRestartForTests() {
    // Already shut down: writers and checkpointer are stopped (and checkpointed
    // gracefully) -- only the in-memory state below needs dropping.
    if (!shutdown_flag_.exchange(true, std::memory_order_acq_rel)) {
        StopWalWriters(/*final_checkpoint_cycle=*/false);
    }
    walring::ClearRingRegistryForTests();
    walring::ClearDirtyForTests();
    ClearZoneMapRegistryForTests();
    ClearCatIdxRegistryForTests();
    ResetShutdownForTests();
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
        spdlog::warn("[WAL STATS] total ring_records={} direct_records={} ring_stalls={}",
                     wal_ring_records.load(std::memory_order_relaxed), wal_direct_records.load(std::memory_order_relaxed),
                     wal_ring_stalls.load(std::memory_order_relaxed));
    }
}
#endif

void XStore::DrainWalWriters() {
    std::atomic_thread_fence(std::memory_order_seq_cst);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(XStoreTunables::BACKGROUND_REMOVE_WAIT_MS);
    const size_t shards = std::min(active_wal_shard_count.load(std::memory_order_relaxed), wal_shards.size());
    for (size_t i = 0; i < shards; ++i) {
        const uint64_t seen = wal_shards[i].batches_done.load(std::memory_order_acquire);
        while (wal_shards[i].busy.load(std::memory_order_acquire) && wal_shards[i].batches_done.load(std::memory_order_acquire) == seen) {
            if (std::chrono::steady_clock::now() >= deadline) {
                spdlog::error("[REMOVE] - WAL writer shard {} did not finish its batch within {} ms; continuing", i,
                              XStoreTunables::BACKGROUND_REMOVE_WAIT_MS);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

void XStore::WalWriterLoop(size_t shard_idx, std::shared_ptr<walring::Ring> preopened_ring) {
    WalShard &shard = wal_shards[shard_idx];
    std::vector<WalRecord> batch(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Everything below is thread-local to this one writer thread -- no lock
    // needed, only this thread ever touches it. Declared once outside the
    // loop and reused (via .clear()/reset each iteration, or sized once and
    // indexed) rather than re-declared per batch, to avoid a fresh heap
    // allocation on the writer's per-batch hot path.
    //
    // Ring cache per WAL file path (one data_store_dir in production; tests
    // run several): the shared_ptr keeps the ring (and its fd) alive even if
    // the process-wide registry is cleared underneath us.
    std::unordered_map<std::string, std::shared_ptr<walring::Ring>> rings;
    uint64_t seen_registry_epoch = walring::RegistryEpoch();
    if (preopened_ring) {
        std::string key = preopened_ring->path();
        rings.emplace(std::move(key), std::move(preopened_ring));
    }
    // Per-file list of {pointer, length} pieces for one pwritev() call,
    // built to point DIRECTLY at each record's already-owned memory
    // (each record's wire rows section, alive in `batch`, released at
    // the end of this iteration after completions are signalled) instead
    // of copying every record's bytes into one
    // concatenated buffer first -- the thing that used to cost a full
    // extra payload-sized memcpy per record here. record_headers below
    // holds the small (tens of bytes) per-record header pieces that DO
    // need their own scratch storage (they're computed values, not
    // already-owned memory); everything else is a reference into memory
    // the batch already owns.
    //
    // Each record is at most 2 iovecs (header + rows section), so a full
    // batch normally fits one pwritev call; the write loop below still
    // chunks into multiple pwritev calls per file (advancing the file
    // offset between chunks) instead of relying on that bound -- see
    // XStoreTunables::WAL_PWRITEV_MAX_IOVECS.
    std::unordered_map<walring::Ring *, SubBatchFile> sub_batch_files;   // keyed by Ring: two path spellings of one file are one entry
    // Deques, not vectors: push_back on a deque never invalidates
    // references to earlier elements (only vector's does, on reallocation)
    // -- iovecs above hold raw pointers into these, taken before all of
    // this sub-batch's records have been processed, so that stability
    // matters. Each entry: [recordLen(4)][crc32c(4)][lsn(8)][dbNameLen(2)]
    // [rowLength(2)][timeResolution(4)][rowCount(4)][dbName] -- everything
    // on the wire before the rows section (a separate iovec entry pointing
    // straight at WalRecord::wire).
    std::deque<std::string> record_headers;
    // Memoizes WalFilePathFor within one batch: it's pure path computation
    // (filesystem::path construction + string concat, no syscalls) but
    // still real allocation work, and most records in one shard's batch
    // share the same db_base_path (they landed on this shard together
    // precisely because EnqueueWalRecord routes by (db, year)) -- no
    // reason to recompute the identical path once per record when it's
    // usually the same 1-2 distinct paths for the whole batch.
    std::unordered_map<std::string, std::string> wal_path_cache;
    // Per dequeued record: which WAL file it targets, the ring serving it
    // (nullptr = could not be opened), its ring-bound size, how it is
    // handled, the sub-batch file slice it belongs to, and its outcome.
    std::vector<std::string> record_wal_path(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    std::vector<walring::Ring *> record_ring(XStoreTunables::WAL_BATCH_MAX_RECORDS, nullptr);
    std::vector<size_t> record_total(XStoreTunables::WAL_BATCH_MAX_RECORDS, 0);
    std::vector<uint64_t> record_end_lsn(XStoreTunables::WAL_BATCH_MAX_RECORDS, 0);   // ring LSN just past each ring record
    std::vector<WalRecordKind> record_kind(XStoreTunables::WAL_BATCH_MAX_RECORDS, WalRecordKind::kRing);
    std::vector<SubBatchFile *> record_file(XStoreTunables::WAL_BATCH_MAX_RECORDS, nullptr);
    // kLarge records only: the data file the record targets (same string ApplyWalRecord uses).
    std::vector<std::string> record_data_path(XStoreTunables::WAL_BATCH_MAX_RECORDS);
    // Direct-path scratch: records of the current direct run that applied and await the data sync, and
    // the distinct data files they touched (one fdatasync + parent-dir fsync per file per run).
    std::vector<size_t> direct_idx;
    std::set<std::string> direct_paths;
    std::vector<bool> record_ok(XStoreTunables::WAL_BATCH_MAX_RECORDS, false);
    // Bytes already placed per ring in the sub-batch being assembled (linear
    // scan: one or two rings in practice).
    std::vector<std::pair<walring::Ring *, size_t>> sub_batch_bytes;
    // PAD record scratch (only wrap-around sub-batches touch it).
    std::vector<char> pad_buf;
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
        // The checkpointer's idle trigger reads this: one relaxed store per gathered batch, nothing per row.
        walring::last_writer_activity_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(last_activity.time_since_epoch()).count(),
            std::memory_order_relaxed);
        // Marks this batch in flight for DrainWalWriters. busy is stored before the fence, the record checks of Pass A come
        // after it; a removal erases its cfg / registers its scope, fences, then reads busy: either it sees the batch (and
        // waits for it) or the batch's checks see the removal.
        struct BatchInFlight {
            WalShard &s;
            explicit BatchInFlight(WalShard &sh) : s(sh) {
                s.busy.store(true, std::memory_order_relaxed);
                std::atomic_thread_fence(std::memory_order_seq_cst);
            }
            ~BatchInFlight() {
                s.batches_done.fetch_add(1, std::memory_order_release);
                s.busy.store(false, std::memory_order_release);
            }
        } batch_in_flight(shard);
        walring::ParkWriterIfPausedForTests(1);   // tests only: one relaxed load

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
        // every commit) in exchange for real batching once (db, year) routing
        // (see EnqueueWalRecord) funnels concurrent writers of the same (db, year)
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
        // Exception path only: a sub-batch whose ring append was committed (head moved) but which never reached
        // step (5) must still publish its applied position, or applied < head stays until some later batch
        // publishes a larger value and the checkpointer (which advances only to applied) spins no-progress
        // cycles once the ring is half full. Its callers are told "failed" below, so nothing acknowledged depends
        // on those rows being applied; rows that did apply were already Note'd for the next checkpoint sync. The
        // records that did NOT apply were never Note'd on a data file, and finding their files could allocate, so the
        // whole ring is marked instead (allocation-free): RingClear is false for every file until the checkpoint
        // passes this sub-batch.
        auto publish_committed = [&]() noexcept {
            for (auto &entry : sub_batch_files) {
                if (!entry.second.durable) continue;
                entry.second.ring->NoteUnattributedThrough(entry.second.start + entry.second.total);
                entry.second.ring->PublishApplied(entry.second.start + entry.second.total);
            }
            sub_batch_files.clear();
        };
        try {
            // ClearRingRegistryForTests() (tests only) bumps the epoch: drop our
            // cached Rings so we reopen through the registry instead of pinning a
            // Ring (and the fd of a possibly deleted file) the registry forgot.
            // The Ring's destructor closes its fd. One relaxed load per batch.
            if (const uint64_t epoch = walring::RegistryEpoch(); epoch != seen_registry_epoch) {
                rings.clear();
                seen_registry_epoch = epoch;
            }
            record_headers.clear();
            wal_path_cache.clear();

            // Resolves record i exactly once: result, then wake the SYNC caller.
            auto complete = [&](size_t i, bool ok) {
                if (batch[i].completion) {
                    batch[i].completion->result = ok;
                    batch[i].completion->sem.signal();
                }
                resolved[i] = true;
            };

            const size_t large_threshold = std::min(walring::DirectMinBytes(), walring::DirectPressureMinBytes());
            // Route of one kLarge record, decided HERE (when the writer reaches it, after every earlier
            // record of this batch has been applied and Note'd) so RingClear sees all earlier ring records.
            //
            // What the ordering rule guarantees: every ring record that was ACKED before this record was
            // submitted has already been Note'd on its file (Note precedes PublishApplied precedes the ack)
            // and RingClear walks every ring mark of the file, whichever shard/ring it came from -- so a
            // direct write is never undone by replaying one of those. NOT covered: CONCURRENT un-acked
            // writes to the same row: every record of one (db, year) file is routed to one shard by
            // EnqueueWalRecord, so ring order == ack order for SEQUENTIAL writes, but two writes in flight at
            // the same time have no defined order. Replay processes rings one shard at a time, which is fine
            // because a file's ring records all live in one ring.
            //
            // A fenced WAL file (replay failed) still holds un-checkpointed records that replay applied
            // with no ring/dirty mark, so RingClear cannot see them: while ANY file is fenced nothing is
            // ring-clear (large records go to the ring; ones over ring/2 fail "overloaded" after the timeout).
            enum class LargeRoute { kRing, kDirect, kFail, kFailOverloaded };
            auto decide_large = [&](size_t i) -> LargeRoute {
                const WalRecord &record = batch[i];
                const size_t ring_bytes = record_ring[i]->ring_bytes();
                const size_t payload = static_cast<size_t>(record.row_count) * record.RowStride();
                const TimeStruct first_ts(record.EpochAt(0), record.time_resolution);
                record_data_path[i] = CachedYearFilePath(record.db_base_path, first_ts.year);
                const std::string &path = record_data_path[i];
                auto file_clear = [&]() { return !walring::AnyFenced() && walring::Dirty().RingClear(path); };

                const bool over_half = payload > ring_bytes / 2;
                // Size first: the fence flag and RingClear (one mutex) only for records at/above the applicable threshold.
                const bool pressure = !over_half && payload < walring::DirectMinBytes() && walring::RingPressure();
                const size_t threshold = pressure ? walring::DirectPressureMinBytes() : walring::DirectMinBytes();
                const bool ring_clear = (over_half || payload >= threshold) && file_clear();
                walring::Route route = walring::ChooseRoute(payload, /*is_bypass=*/false, /*is_unary=*/false, pressure, ring_clear, ring_bytes);

                if (over_half && !ring_clear) {
                    // Too big for the ring and the file is not clear: have the checkpointer thread run cycles
                    // (RunForcedCycle: this thread never takes the cycle mutex, so one long in-flight cycle cannot
                    // push the wait past the deadline) until it is, then go direct. Bounded by the stall timeout;
                    // also leaves on shutdown.
                    walring::Checkpointer &ckpt = walring::Checkpointer::Instance();
                    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(walring::StallTimeoutMs());
                    while (!file_clear()) {
                        if (shard.stop_flag.load(std::memory_order_relaxed)) {
                            spdlog::error("[WAL] [DB: {}] - record of {} bytes exceeds half the ring: shutting down, failing it", record.db_name, payload);
                            return LargeRoute::kFail;
                        }
                        if (ckpt.Poisoned() || std::chrono::steady_clock::now() >= deadline) {
                            wal_ring_stalls.fetch_add(1, std::memory_order_relaxed);
                            spdlog::error("[WAL] [DB: {}] - record of {} bytes exceeds half the ring ({} bytes) and {} could not be "
                                          "checkpointed clear within {} ms: failing it (overloaded){}{}",
                                          record.db_name, payload, ring_bytes, path, walring::StallTimeoutMs(),
                                          ckpt.Poisoned() ? "; checkpointer poisoned by a sync error" : "",
                                          walring::AnyFenced() ? "; a WAL file is fenced (failed replay)" : "");
                            // A poisoned checkpointer needs a restart: not a "retry later" condition for the client.
                            return ckpt.Poisoned() ? LargeRoute::kFail : LargeRoute::kFailOverloaded;
                        }
                        const auto forced = ckpt.RunForcedCycle(deadline, shard.stop_flag);
                        if (forced == walring::Checkpointer::ForcedCycle::kNotRunning) (void)ckpt.RunCycle();
                        if ((forced == walring::Checkpointer::ForcedCycle::kDone || forced == walring::Checkpointer::ForcedCycle::kNotRunning) &&
                            !file_clear()) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(2));
                        }
                    }
                    route = walring::Route::kDirect;
                }
                if (route == walring::Route::kRing) return LargeRoute::kRing;
                if (walring::Checkpointer::Instance().Poisoned()) {
                    spdlog::error("[WAL] [DB: {}] - direct write cannot be made durable: checkpointer poisoned/sync error", record.db_name);
                    return LargeRoute::kFail;
                }
                return LargeRoute::kDirect;
            };

            // Pass A -- no row data touched: per record, its WAL file, the
            // ring serving that file, its exact ring-bound size
            // (RECORD_FIXED_BYTES + dbName + rows * (epoch + row)), and how it
            // is handled.
            for (size_t i = 0; i < count; ++i) {
                const WalRecord &record = batch[i];
                if (i > 0 && record.db_base_path == batch[i - 1].db_base_path) {
                    // Same DB as the previous record (the usual case: they were routed
                    // to this shard together by (db, year)): same file, same ring.
                    record_wal_path[i] = record_wal_path[i - 1];
                    record_ring[i] = record_ring[i - 1];
                } else {
                    auto [path_it, inserted] = wal_path_cache.try_emplace(record.db_base_path);
                    if (inserted) {
                        path_it->second = WalFilePathFor(record.db_base_path, shard_idx);
                    }
                    record_wal_path[i] = path_it->second;

                    auto ring_it = rings.find(record_wal_path[i]);
                    if (ring_it == rings.end()) {
                        std::shared_ptr<walring::Ring> ring = walring::GetOrOpenRing(record_wal_path[i], static_cast<uint32_t>(shard_idx));
                        if (!ring && walring::IsFenced(record_wal_path[i])) {
                            // Failed replay: the fence, not a legacy WAL, is why there is no ring. Never re-init it.
                            LogFencedWalOncePerMinute(record_wal_path[i]);
                        } else if (!ring) {
                            // Non-empty without a valid ring header: a legacy WAL.
                            // If InitDB's ReplayWal had run it would be a ring (or
                            // empty) by now; reaching the first append means no
                            // replay is coming for this file.
                            spdlog::warn("[WAL] - {} has no ring header (legacy WAL never replayed?); initializing it as a ring",
                                         record_wal_path[i]);
                            ring = walring::GetOrOpenRing(record_wal_path[i], static_cast<uint32_t>(shard_idx), /*reinit_legacy=*/true);
                        }
                        if (ring) ring_it = rings.emplace(record_wal_path[i], std::move(ring)).first;
                    }
                    record_ring[i] = ring_it != rings.end() ? ring_it->second.get() : nullptr;
                }
                record_total[i] = walring::RECORD_FIXED_BYTES + record.db_name.size()
                                + static_cast<size_t>(record.row_count) * record.RowStride();
                // Hot path: a small record pays one multiply-compare. Only multi-row records
                // at/above the smaller direct threshold (or over half the ring) are kLarge; the
                // dirty-tracker lookup happens later, in decide_large, for those alone.
                const size_t ring_half = record_ring[i] != nullptr ? record_ring[i]->ring_bytes() / 2 : 0;
                if (!CfgEpochCurrent(record.db_name, record.cfg_epoch)) {   // refused BEFORE the ring append, see DrainWalWriters
                    record_kind[i] = WalRecordKind::kStale;
                } else if (record_ring[i] == nullptr) {
                    record_kind[i] = WalRecordKind::kFail;
                } else if (record.row_count == 1) {
                    // Unary-sized: never direct. Only a (test-sized) tiny ring can make one not fit.
                    // WalRecord has no unary flag, so one row stands in for "unary"; rowLength is a u16, so
                    // a single row is at most 8 + 65535 bytes: below WAL_DIRECT_MIN_BYTES, and kept off the
                    // direct path regardless (it may still exceed WAL_DIRECT_PRESSURE_MIN_BYTES).
                    record_kind[i] = record_total[i] > ring_half ? WalRecordKind::kFail : WalRecordKind::kRing;
                } else {
                    const size_t payload = static_cast<size_t>(record.row_count) * record.RowStride();
                    record_kind[i] = (payload >= large_threshold || payload > ring_half) ? WalRecordKind::kLarge : WalRecordKind::kRing;
                }
            }

            walring::ParkWriterIfPausedForTests(2);   // tests only: the checks above passed, nothing appended yet

            // Records are handled in queue order so same-timestamp writes
            // apply in order: a maximal run of ring records forms one
            // sub-batch (byte-bounded per ring); a non-ring record is its own
            // step.
            for (size_t pos = 0; pos < count; ) {
                if (record_kind[pos] == WalRecordKind::kStale) {
                    spdlog::warn("[WAL] [DB: {}] - The DB was removed after this record was built; its {} row(s) are refused before the ring append",
                                 batch[pos].db_name, batch[pos].row_count);
                    complete(pos, false);
                    ++pos;
                    continue;
                }
                if (record_kind[pos] == WalRecordKind::kFail) {
                    if (record_ring[pos] == nullptr) {
                        spdlog::error("[WAL] [DB: {}] - Unable to open the WAL ring {}", batch[pos].db_name, record_wal_path[pos]);
                    } else {
                        spdlog::error("[WAL] [DB: {}] - Single-row record of {} bytes cannot fit half of the {}-byte ring {}",
                                      batch[pos].db_name, record_total[pos], record_ring[pos]->ring_bytes(), record_wal_path[pos]);
                    }
                    complete(pos, false);
                    ++pos;
                    continue;
                }
                if (record_kind[pos] == WalRecordKind::kLarge) {
                    // Direct run: consecutive kLarge records that route direct are applied in queue
                    // order, then their data files are synced ONCE per distinct file, then (and only
                    // then) the records are acknowledged. A kLarge record that routes to the ring ends
                    // the run and is handled as an ordinary ring record below / next iteration.
                    direct_idx.clear();
                    direct_paths.clear();
                    size_t run_end = pos;
                    while (run_end < count && record_kind[run_end] == WalRecordKind::kLarge) {
                        const LargeRoute decision = decide_large(run_end);
                        if (decision == LargeRoute::kRing) {
                            record_kind[run_end] = WalRecordKind::kRing;   // decided; do not decide again
                            break;
                        }
                        if (decision == LargeRoute::kFail || decision == LargeRoute::kFailOverloaded) {
                            if (decision == LargeRoute::kFailOverloaded && batch[run_end].completion) batch[run_end].completion->overloaded = true;
                            complete(run_end, false);
                            ++run_end;
                            continue;
                        }
                        if (ApplyWalRecord(batch[run_end])) {
                            wal_direct_records.fetch_add(1, std::memory_order_relaxed);
                            direct_paths.insert(record_data_path[run_end]);
                            direct_idx.push_back(run_end);
                        } else {
                            spdlog::error("[WAL] [DB: {}] - Direct-path record failed to apply", batch[run_end].db_name);
                            complete(run_end, false);   // no sync attempted for it
                        }
                        ++run_end;
                    }
                    if (!direct_idx.empty()) {
                        // fdatasync every touched data file + fsync its parent directory (a new year file's
                        // directory entry is not made durable by fdatasync of the file) through the
                        // checkpointer's shared helpers: same fault hooks, same sticky-poison rule.
                        // ApplyWalRecord also noted these ranges dirty (no ring), so the next checkpointer cycle
                        // fdatasyncs them a second time: harmless extra work, kept for simplicity.
                        // On failure the rows are ALREADY in the data file (like BYPASS) -- acceptable
                        // because the caller is told the write failed; the range stays in the dirty set so the
                        // checkpointer syncs it later. Never acknowledge what a fresh fd cannot vouch for.
                        const bool durable = !walring::ConsumeInjectedDirectSyncFailure() &&
                                             walring::Checkpointer::Instance().SyncPathsForReplay(direct_paths);
                        if (!durable) {
                            spdlog::error("[WAL] - direct write cannot be made durable: {} ({} data file(s), first {})",
                                          walring::Checkpointer::Instance().Poisoned() ? "checkpointer poisoned/sync error" : "sync failed",
                                          direct_paths.size(), *direct_paths.begin());
                        }
                        for (size_t idx : direct_idx) complete(idx, durable);
                    }
                    if (run_end != pos) {
                        pos = run_end;
                        continue;
                    }
                    // The first record routed to the ring (kind rewritten above): fall through.
                }

                // Sub-batch = [pos, end): ring records until a non-ring
                // record or a ring would exceed ring_bytes/4 (a lone record
                // larger than that still forms its own sub-batch).
                sub_batch_bytes.clear();
                size_t end = pos;
                for (; end < count && record_kind[end] == WalRecordKind::kRing; ++end) {
                    auto entry = std::find_if(sub_batch_bytes.begin(), sub_batch_bytes.end(),
                                              [&](const auto &e) { return e.first == record_ring[end]; });
                    if (entry == sub_batch_bytes.end()) {
                        sub_batch_bytes.emplace_back(record_ring[end], 0);
                        entry = sub_batch_bytes.end() - 1;
                    }
                    if (entry->second != 0 && entry->second + record_total[end] > record_ring[end]->ring_bytes() / 4) break;
                    entry->second += record_total[end];
                }

                wal_ring_records.fetch_add(end - pos, std::memory_order_relaxed);

                // (1) Group by WAL file and reserve each file's ring space. (sub_batch_files is empty here:
                // cleared after step (5) or by the exception path.)
                for (size_t i = pos; i < end; ++i) {
                    SubBatchFile &file = sub_batch_files[record_ring[i]];
                    file.ring = record_ring[i];
                    file.total += record_total[i];
                    record_file[i] = &file;
                }
                for (auto &entry : sub_batch_files) {
                    SubBatchFile &file = entry.second;
                    uint64_t start = 0;
                    size_t pad = 0;
                    if (file.ring->Reserve(file.total, std::chrono::milliseconds(walring::StallTimeoutMs()), start, pad)) {
                        file.reserved = true;
                        file.start = start;
                        file.cursor = start;
                        file.pad = pad;
                    } else {
                        file.overloaded = !walring::Checkpointer::Instance().Poisoned();
                        wal_ring_stalls.fetch_add(1, std::memory_order_relaxed);
                        spdlog::error("[WAL] - ring full ({}: {} bytes unflushed of {}, needs {} more): no checkpoint progress within {} ms; "
                                      "failing the batch (overloaded){}",
                                      file.ring->path(), file.ring->HeadLsn() - file.ring->CheckpointLsn(), file.ring->ring_bytes(),
                                      file.total, walring::StallTimeoutMs(),
                                      walring::Checkpointer::Instance().Poisoned() ? "; checkpointer poisoned by a sync error" : "");
                    }
                }

                // (2) Build each record's header (LSN assigned in queue order,
                // contiguous per file) and its iovecs.
                for (size_t i = pos; i < end; ++i) {
                    SubBatchFile &file = *record_file[i];
                    if (!file.reserved) continue;
                    const WalRecord &record = batch[i];
                    const uint64_t lsn = file.cursor;
                    file.cursor += record_total[i];
                    record_end_lsn[i] = file.cursor;

                    // Header piece: everything on the wire up to and including
                    // dbName -- recordLen and crc32c are patched in after being
                    // computed further down, but the iovec pointing at this
                    // buffer's final address can be pushed now since pwritev
                    // only reads it after the patch.
                    record_headers.emplace_back();
                    std::string &hdr = record_headers.back();
                    hdr.reserve(XStoreTunables::WAL_RECORD_HEADER_BYTES + sizeof(uint64_t) + record.db_name.size());
                    hdr.resize(16); // recordLen+crc32c placeholder (patched below), then the LSN
                    std::memcpy(hdr.data() + 8, &lsn, sizeof(lsn));
                    AppendPod<uint16_t>(hdr, static_cast<uint16_t>(record.db_name.size()));
                    AppendPod<uint16_t>(hdr, record.row_length);
                    AppendPod<int32_t>(hdr, static_cast<int32_t>(record.time_resolution));
                    AppendPod<uint32_t>(hdr, record.row_count);
                    hdr.append(record.db_name);

                    std::vector<struct iovec> &iov_list = file.iov;
                    iov_list.push_back({hdr.data(), hdr.size()});

                    // CRC covers everything after itself (the LSN included). The rows section
                    // ([u64 epoch][row bytes] x rowCount) is already one contiguous buffer in
                    // record.wire, so it is one iovec (no copy) and one CRC update: CRC32C is
                    // streaming, so the result equals the old per-piece chain over the same bytes.
                    uint32_t crc = Crc32cUpdate(Crc32cInit(), hdr.data() + 8, hdr.size() - 8);
                    size_t payload_size_after_header = hdr.size() - 8;
                    if (!record.wire.empty()) {
                        iov_list.push_back({const_cast<char *>(record.wire.data()), record.wire.size()});
                        crc = Crc32cUpdate(crc, record.wire.data(), record.wire.size());
                        payload_size_after_header += record.wire.size();
                    }
                    crc = Crc32cFinal(crc);

                    uint32_t record_len = static_cast<uint32_t>(4 + payload_size_after_header);
                    // The LSNs handed out above assumed record_total[i]
                    // (row_count rows of row_length); a wire buffer of any
                    // other size would break that contiguity, so fail the
                    // batch instead of writing it.
                    if (static_cast<size_t>(record_len) + 4 != record_total[i]) {
                        throw std::runtime_error("WAL record size differs from its planned ring size");
                    }
                    std::memcpy(hdr.data(), &record_len, sizeof(record_len));
                    std::memcpy(hdr.data() + 4, &crc, sizeof(crc));
                }

                // (3) Write + fdatasync each reserved file, then advance its head.
                for (auto &entry : sub_batch_files) {
                    SubBatchFile &file = entry.second;
                    if (!file.reserved) continue;
                    walring::Ring &ring = *file.ring;
                    const int fd = ring.fd();

                    bool file_write_ok = true;
                    int write_errno = 0;   // set on the failure path only
                    if (file.pad > 0) {
                        // Tail of the lap too small for the run: fill it with a PAD
                        // record so replay can step over it.
                        const uint64_t pad_lsn = file.start - file.pad;
                        pad_buf.resize(file.pad);
                        walring::BuildPad(pad_buf.data(), file.pad, pad_lsn);
                        const ssize_t pad_written = pwrite(fd, pad_buf.data(), file.pad,
                                                           static_cast<off_t>(walring::RingOffset(pad_lsn, ring.ring_bytes())));
                        file_write_ok = pad_written == static_cast<ssize_t>(file.pad);
                        // errno only on a failed call (a short write leaves it stale), captured before anything else runs.
                        if (!file_write_ok) write_errno = pad_written < 0 ? errno : EIO;
                    }

                    // Chunk this file's iovecs into groups of at most
                    // WAL_PWRITEV_MAX_IOVECS -- the common case (few
                    // records/rows) is exactly one chunk; only a large
                    // multi-row Batch/Range record (or enough concurrent
                    // records landing on the same file in one gather window)
                    // ever needs more than one pwritev call. The run never
                    // straddles the ring end (PlanAppend), so one base offset
                    // plus a running chunk offset is enough.
                    off_t chunk_offset = static_cast<off_t>(walring::RingOffset(file.start, ring.ring_bytes()));
                    for (size_t iov_start = 0; file_write_ok && iov_start < file.iov.size(); ) {
                        size_t iov_count = std::min(file.iov.size() - iov_start, XStoreTunables::WAL_PWRITEV_MAX_IOVECS);
                        size_t chunk_bytes = 0;
                        for (size_t k = iov_start; k < iov_start + iov_count; ++k) chunk_bytes += file.iov[k].iov_len;

                        ssize_t written = pwritev(fd, file.iov.data() + iov_start, static_cast<int>(iov_count), chunk_offset);
                        if (written != static_cast<ssize_t>(chunk_bytes)) {
                            file_write_ok = false;
                            write_errno = written < 0 && errno != 0 ? errno : EIO;
                            break;
                        }
                        chunk_offset += static_cast<off_t>(chunk_bytes);
                        iov_start += iov_count;
                    }

                    // Always fdatasync: the ring is fixed-size and pre-zeroed, so
                    // no write ever changes file size/metadata.
                    const bool real_sync_ok = fdatasync(fd) == 0;
                    const bool sync_ok = real_sync_ok && !walring::ConsumeInjectedWalSyncFailure();
                    if (!file_write_ok || !sync_ok) {
                        const int sync_errno = real_sync_ok ? 0 : errno;   // first thing: logging clobbers errno
                        // Test hook: an injected sync failure may stand in for "the pwritev failed too".
                        if (!sync_ok && real_sync_ok && file_write_ok) {
                            if (const int injected_write_errno = walring::InjectedWalWriteErrno(); injected_write_errno != 0) {
                                file_write_ok = false;
                                write_errno = injected_write_errno;
                            }
                        }
                        spdlog::error("[WAL] - Failed to durably write/fdatasync {}", ring.path());
                        // A REAL ring pwritev/fdatasync error (anything but EINTR/EAGAIN, or a sticky injected one)
                        // means the device may be dropping this file's pages and the kernel reports a writeback error
                        // only once: the batch fails to its callers (below), and from now on the checkpointer must
                        // not advance any ring over data files either (fsyncgate). EITHER the write or the sync
                        // being real poisons (a write EINTR followed by a real EIO sync still counts). Error path only.
                        const bool real_write = !file_write_ok && IsRealWalIoError(write_errno);
                        const bool injected_sticky = real_sync_ok && !sync_ok && walring::InjectedWalSyncFailureIsSticky();
                        if (real_write || injected_sticky || IsRealWalIoError(sync_errno)) {
                            walring::Checkpointer::Instance().ReportRealError(
                                real_write ? "WAL ring pwritev" : "WAL ring fdatasync", ring.path(),
                                real_write ? write_errno : (injected_sticky ? EIO : sync_errno));
                        }
                        // head does not advance, so this range is free space again -- but its bytes may
                        // be intact on disk (a failed fdatasync does not undo the write). A later,
                        // shorter record at the same head would leave this record's tail beyond it,
                        // with LSNs that still match: an unacknowledged record that replay could apply
                        // after a later crash. Best effort: overwrite the range with zeros.
                        int zero_write_errno = 0;
                        const bool zeroed = !walring::ConsumeInjectedRingZeroWriteFailure(zero_write_errno) &&
                                            walring::ZeroRingRange(fd, file.start - file.pad, file.start + file.total,
                                                                   ring.ring_bytes(), &zero_write_errno);
                        // A real pwrite failure here is a device write error on the WAL file, same as the writer's own.
                        if (!zeroed && IsRealWalIoError(zero_write_errno)) {
                            walring::Checkpointer::Instance().ReportRealError("WAL ring zero-range pwrite", ring.path(), zero_write_errno);
                        }
                        bool zero_synced = false;
                        if (zeroed) {
                            zero_synced = fdatasync(fd) == 0;
                            int zero_errno = zero_synced ? 0 : errno;
                            if (walring::ConsumeInjectedWalZeroRangeSyncFailure()) { zero_synced = false; zero_errno = EIO; }
                            // A real failure here is another writeback error the kernel reports only once.
                            if (IsRealWalIoError(zero_errno)) {
                                walring::Checkpointer::Instance().ReportRealError("WAL ring zero-range fdatasync", ring.path(), zero_errno);
                            }
                        }
                        if (!zeroed || !zero_synced) {
                            spdlog::error("[WAL] - Could not zero the failed append range in {}", ring.path());
                        }
                        continue;
                    }
                    file.durable = true;
                    ring.CommitAppended(file.start + file.total);
                }

                // (4) Apply every record's rows to their real data files --
                // same in-memory bytes already held from enqueue, no WAL
                // re-read (WAL is only ever read back during ReplayWal, never
                // mid-batch). A record only counts as durable if BOTH its WAL
                // file's fdatasync succeeded above AND every one of its rows
                // applied cleanly here -- either failing means the caller must
                // be told, not silently given a success signal. A record whose
                // ring space could not be reserved, or whose WAL write/sync
                // failed, is not applied at all.
                if (walring::ConsumeInjectedWriterApplyThrow()) throw std::runtime_error("injected writer apply exception");
                for (size_t i = pos; i < end; ++i) {
                    const SubBatchFile &file = *record_file[i];
                    // Invariant: a data-file write only happens for a record whose
                    // ring record is durable (inside [checkpoint, head)). A record
                    // whose space timed out or whose WAL write/fdatasync failed is
                    // neither applied nor acknowledged.
                    if (!file.reserved || !file.durable) {
                        record_ok[i] = false;
                        if (file.overloaded && batch[i].completion) batch[i].completion->overloaded = true;
                        continue;
                    }
                    // record_wal_path[i] is always a key of `rings` for a kRing record.
                    const auto ring_it = rings.find(record_wal_path[i]);
                    const bool apply_ok = ApplyWalRecord(batch[i],
                                                         ring_it != rings.end() ? ring_it->second : std::shared_ptr<walring::Ring>(),
                                                         record_end_lsn[i]);
                    record_ok[i] = apply_ok;
                    if (!apply_ok) {
                        spdlog::error("[WAL] [DB: {}] - Record is in the WAL but its rows failed to apply", batch[i].db_name);
                    }
                }

                // (5) Publish applied LSNs, THEN release the callers, so a
                // returned SYNC insert is already visible in the ring accounting.
                for (auto &entry : sub_batch_files) {
                    if (entry.second.durable) entry.second.ring->PublishApplied(entry.second.start + entry.second.total);
                }
                for (size_t i = pos; i < end; ++i) complete(i, record_ok[i]);
                pos = end;
                sub_batch_files.clear();
            }

            // No per-commit checkpoint here: the checkpoint (Task 4) trails
            // applied_lsn and is persisted in the ring header, so there is
            // nothing to invalidate per batch.
        } catch (const std::exception &ex) {
            spdlog::error("[WAL] - Writer loop caught exception, failing this batch: {}", ex.what());
            publish_committed();
            for (size_t i = 0; i < count; ++i) {
                if (!resolved[i] && batch[i].completion) {
                    batch[i].completion->result = false;
                    batch[i].completion->sem.signal();
                }
            }
        } catch (...) {
            spdlog::error("[WAL] - Writer loop caught unknown exception, failing this batch");
            publish_committed();
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
