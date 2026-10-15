#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

// Everything below is needed by this header's own declarations (member
// types or method signatures). Anything only used inside an XStore_*.cpp
// method body -- filesystem, sys/mman.h, fcntl.h, algorithm, execution,
// TimeStruct, fmt, spdlog, etc. -- is included by that .cpp directly rather
// than leaking into every translation unit that includes this class.
#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <map>
#include <optional>
#include <cstdio>
#include <exception>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <thread>
#include <functional>
#include <blockingconcurrentqueue.h>
#include "XStore.pb.h"
#include "include/DataStructs/ValueStruct.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/XStore/XStore_ZoneMap.h"
#include "include/XStore/XStore_CatIdx.h"
#include "include/XStore/XStore_RebuildProgress.h"

// Length of msg header
#define TRACE_LEN 8
#define DEBUG_LEN 8
#define INFO_LEN 7
#define WARNING_LEN 10
#define ERROR_LEN 8
#define CRITICAL_LEN 11

namespace walring { class Ring; }

class XStore {
    public:
        // Convention: every directory-path member/return value in this class
        // (base_dir, data_store_dir, DbBasePath()'s result) is stored WITHOUT a
        // trailing '/'. Joining is always done explicitly via JoinPath (or an
        // explicit '/' in a format string), never by relying on a variable
        // having one baked in -- that made call sites impossible to read
        // without checking how the value upstream was built.
        std::string base_dir = GetBaseDir();
        std::string data_store_dir = JoinPath(base_dir, "DataStore");

        // Shared across every XStore instance (one per worker thread) so a config
        // read from disk by one thread is visible to all others. Sharded the
        // same way as the mmap/fd caches: GetOrLoadCfg is the first thing
        // every single RPC calls (every insert, query, delete, all four
        // aggregates), so even though the fast path only takes a *shared*
        // lock, that shared_lock's acquire/release is still an atomic RMW on
        // one process-wide cache line hit by every worker thread on every
        // call -- the hottest piece of shared state in the system. No cap or
        // eviction here (unlike Mmap/FdShard) since the config set is small
        // and static per DB, so each shard is just an unordered_map + its
        // own shared_mutex.
        struct CfgShard {
            std::unordered_map<std::string, XStore_Proto::ConfigStruct> map;
            std::shared_mutex mutex;
        };
        static constexpr size_t CFG_SHARD_COUNT = XStoreTunables::CFG_SHARD_COUNT;
        static std::array<CfgShard, CFG_SHARD_COUNT> cfg_shards;

        // Shared cache of read-only (PROT_READ, MAP_SHARED) mmap regions for
        // .XSTORE data files, keyed by full path, used by the read paths
        // (RangeQuery, aggregates, FindByValue/Pattern) to skip
        // open/ftruncate/mmap on repeat queries. Writes NEVER go through the
        // mapping -- they pwrite() on a cached fd (GetOrOpenFile) so the
        // DirtyTracker sees them; a store through this mapping would SIGSEGV
        // by design. MAP_SHARED keeps those pwrite()s visible here (shared
        // page cache). File size is fixed at creation (ftruncate'd
        // up front to the year's full row count), so one mapping covers the
        // whole file for its lifetime -- no remap needed. Bounded
        // (MMAP_CACHE_CAP) to avoid unbounded address-space/FD growth.
        //
        // Per-shard storage is a flat MRU-ordered array (entries[0] = most
        // recently used), not a list+hashmap: MMAP_SHARD_CAP is tiny (4), so
        // a linear scan over one contiguous array is both fewer cache misses
        // and zero per-entry heap allocation compared to a std::list node +
        // std::unordered_map bucket node per entry -- same reasoning as the
        // flat vector + linear scan used for year buckets (see
        // XStore_Internal.h), just applied to this cache instead of a
        // std::map.
        //
        // Sharded across MMAP_SHARD_COUNT independent {array, mutex}
        // instances, keyed by hashing the file path, instead of one
        // process-wide mutex: a cold-started server hit by many concurrent
        // first-touches (e.g. N threads each first-mapping a different year
        // file at once) used to serialize entirely on that single mutex for
        // the lookup+MRU-promote step, even though the actual open/mmap work
        // was already unlocked. Two threads touching different files usually
        // land in different shards now and don't share a lock at all; only
        // threads racing on the same file (or a hash collision) still
        // serialize, same as before. Per-shard cap is MMAP_CACHE_CAP divided
        // across shards so total resident mapping count is unchanged.
        // Entries are shared_ptr<MappedFile>, not a bare MappedFile: eviction
        // (below, and EraseMappedFile/EraseMappedFilesUnderPrefix) used to
        // munmap() unconditionally as soon as a slot aged out or was
        // explicitly dropped, with zero coordination against a thread that
        // had already read this same MappedFile* out of the cache earlier
        // and was still mid-access via the raw pointer GetOrMapFile handed
        // back -- a straight use-after-unmap race (corruption or SIGSEGV,
        // reproduced under concurrent RND inserts once the working set
        // exceeded MMAP_SHARD_CAP). The shared_ptr's refcount now tracks every
        // outstanding GetOrMapFile() caller in addition to the cache's own
        // entry: eviction/erase here only drops the CACHE's reference, and
        // the custom deleter's munmap() doesn't actually run until the last
        // holder -- cache or in-flight caller -- releases theirs.
        static constexpr size_t MMAP_SHARD_COUNT = XStoreTunables::MMAP_SHARD_COUNT;
        static constexpr size_t MMAP_CACHE_CAP = XStoreTunables::MMAP_CACHE_CAP;
        static constexpr size_t MMAP_SHARD_CAP = XStoreTunables::MMAP_SHARD_CAP;

        struct MappedFile { void *base; size_t len; };
        struct MmapShard {
            struct Entry { std::string key; std::shared_ptr<MappedFile> value; };
            std::array<Entry, MMAP_SHARD_CAP> entries{};
            size_t count = 0;
            std::mutex mutex;
        };
        static std::array<MmapShard, MMAP_SHARD_COUNT> mmap_shards;

        // Shared cache of open fds for .XSTORE data files, keyed by full path --
        // used by the pwrite() write paths. Unlike a writable mapping, writing
        // through a plain fd never faults a page into the process's address
        // space: a pwrite() into a still-sparse region goes through the same
        // buffered write()/delayed-allocation path the file gets on every
        // access pattern (SEQ or RND), instead of a synchronous mmap
        // write-fault that must allocate a real block before returning. Same
        // MRU-bounded design as MmapLru (flat array per shard, see MmapShard
        // above), just storing an fd instead of a base pointer.
        //
        // Sized differently than MMAP_CACHE_CAP on purpose: a miss here is just
        // an open() (microseconds, no page-fault-time block allocation), so
        // the cap doesn't need to be large to amortize an expensive miss --
        // it only needs to cover the year files actively being written at
        // once without thrashing. Even at 64 total, resident cost (~256B
        // kernel struct file + one inline std::string + shared_ptr<int> per
        // entry, no per-fd userspace buffer -- pwrite() writes straight to
        // the kernel page cache) stays a couple KB, well under the single
        // ~4KB buffer glibc's fwrite would have allocated for just one open
        // stream -- comparable open-file-cache sizing in RocksDB
        // (max_open_files, commonly left unlimited in production) and
        // LevelDB (table_cache, historically 1000 entries) runs one to
        // several orders of magnitude higher than this for the same reason:
        // an open fd is cheap, a reopen isn't.
        //
        // Sharded the same way as the mmap cache (see MmapShard above), just
        // fewer shards -- FD_CACHE_CAP stays tiny enough that MMAP_SHARD_COUNT
        // shards would leave under one slot each. FD_SHARD_COUNT keeps a
        // usable few-slot cap per shard while still splitting the lock so
        // concurrent first-opens of different files don't all serialize on
        // one mutex. Expect a smaller contention win than the mmap case: an
        // fd-open miss is already cheap (no page-fault-time block allocation
        // to hide behind an unlocked window), so the contention this closes
        // is narrower to start with.
        // shared_ptr<int> entries for the same reason as MmapShard's
        // shared_ptr<MappedFile> above -- eviction/erase used to close()
        // unconditionally with no regard for a thread still mid-pwrite()/
        // pread() on that exact fd via an earlier GetOrOpenFile() return
        // value, risking either a clean EBADF or (worse, if the closed fd
        // number got reused by an unrelated concurrent open() before the
        // racing pwrite/pread ran) a silent write into the wrong file. The
        // cache's original 8-total size made this easy to hit once more than
        // a handful of year files were concurrently hot.
        static constexpr size_t FD_SHARD_COUNT = XStoreTunables::FD_SHARD_COUNT;
        static constexpr size_t FD_CACHE_CAP = XStoreTunables::FD_CACHE_CAP;
        static constexpr size_t FD_SHARD_CAP = XStoreTunables::FD_SHARD_CAP;

        struct FdShard {
            struct Entry { std::string key; std::shared_ptr<int> value; };
            std::array<Entry, FD_SHARD_CAP> entries{};
            size_t count = 0;
            std::mutex mutex;
        };
        static std::array<FdShard, FD_SHARD_COUNT> fd_shards;

        // ---- Write-Ahead Log --------------------------------------------
        // One append-only WAL file per shard (shard = WalShardFor(db_base_path,
        // year, active)), each drained by its own dedicated, process-
        // lifetime writer thread. UnaryInsert/BatchInsert/RangeInsert all
        // enqueue here instead of writing the data file directly -- see
        // src/XStore/XStore_Wal.cpp for the durability/group-commit
        // mechanics.
        //
        // A WalRecord represents one WRITE REQUEST, not one row: UnaryInsert
        // builds a 1-row WalRecord; BatchInsert/RangeInsert build one
        // N-row WalRecord per year-bucket group the same way, reusing this
        // exact type and EnqueueWalRecord/ApplyWalRecord rather than a
        // parallel WAL path. Durability
        // is per-REQUEST: one completion future covers every row in the
        // record, so a batch's caller waits once for the whole batch, not
        // once per row.
        struct WalRow {
            unsigned long long epoch_time = 0;
            std::string row_bytes;   // already-serialized, zero-padded, exactly row_length bytes
            // Precomputed at enqueue time (BuildZoneMapObservations, before
            // row_bytes is even serialized) since ApplyWalRecord only has
            // row_bytes -- already-serialized wire bytes, not the original
            // RowData -- to re-derive column values from. Applied to the
            // zone map by ApplyWalRecord itself, at the same moment (and
            // under the same success/failure check) as this row's real
            // pwrite -- not eagerly at enqueue time -- so the zone map is
            // never ahead of what's actually durable, including under
            // ASYNC durability mode where the real write happens later on
            // the WAL writer thread, not before this call returns.
            std::vector<ZoneMapObservation> zone_map_observations;
            // Same "precomputed at enqueue time, applied by ApplyWalRecord
            // only after this row's real pwrite succeeds" contract as
            // zone_map_observations above -- see BuildCatIdxObservations.
            std::vector<CatIdxObservation> catidx_observations;
            // Observations above now hold only PRESENT columns (Task 2), so
            // ZoneMapFile::UpdateRow needs this to know how far to bump
            // null_count for the columns past the end of that vector, up to
            // the file's own tracked width -- the same total effect the old
            // one-observation-per-tracked-column vector used to have, without
            // building holes for columns the row never touched. 0 for
            // WAL-replayed rows (ParseWalRecord default-constructs WalRow, so
            // this stays 0), which correctly causes NO index update on replay
            // -- see ApplyWalRecord's comment on why replay must be a no-op.
            uint32_t zone_map_pad_to = 0;
        };

        // Cross-thread completion signal for one WalRecord -- replaces
        // std::promise<bool>/std::future<bool> (measured on real hardware:
        // comparable typical-case cost once compared fairly -- a fresh
        // heap-allocated handle per request either way, since WalRecord
        // gets std::move'd into the queue so the enqueuing thread can't
        // wait on its own stack copy -- but promise/future's shared state
        // carries extra machinery (exception storage, type-erased
        // ownership shared between two distinct objects) this doesn't
        // need). LightweightSemaphore is the same primitive the WAL
        // queue's own blocking wait already uses -- reusing a proven
        // mechanism, not introducing a new one. Checked out from/returned
        // to completion_pool below rather than newed/deleted per request
        // -- see that member's comment.
        struct WalCompletion {
            moodycamel::LightweightSemaphore sem;
            bool result = false;
            // Set by the writer (before the signal) when result is false because the ring stayed full for the
            // stall timeout / a record over half the ring could not be checkpointed clear. Read and cleared by
            // EnqueueWalRecord before the completion goes back to the pool.
            bool overloaded = false;
        };

        // Self-sizing, process-lifetime pool of WalCompletion objects --
        // true one-time allocation, not "smaller allocation every request"
        // (which is all a bare new/delete per record would have bought
        // over std::promise/future). Checkout (EnqueueWalRecord): pop one
        // via try_dequeue; on the rare empty case (still growing toward
        // its natural peak, or a burst past whatever that peak has been so
        // far), fall back to `new` for just that one. Return (after
        // sem.wait() returns -- same happens-before safety as the
        // heap-allocated version): always push back into the pool instead
        // of deleting, whether this particular object came from the pool
        // or the `new` fallback -- so the pool organically grows to
        // whatever the real peak concurrent-outstanding-completions count
        // turns out to be, with no capacity guess needed up front and no
        // hard cap to run past. No LightweightSemaphore state to reset
        // between uses: a matched signal()/wait() pair always leaves its
        // count back at 0.
        static moodycamel::ConcurrentQueue<WalCompletion*> completion_pool;

        struct WalRecord {
            std::string db_name;               // already upper-cased
            std::string db_base_path;          // full path -- locates this DB's WAL subdir and, at apply time, its .XSTORE file
            XStore_Proto::TimeUnit time_resolution = XStore_Proto::SECOND;
            unsigned short row_length = 0;
            // The year of this record's rows (TimeStruct(rows.front().epoch_time, time_resolution).year; a record is
            // always one (db, year)). Set by every producer; EnqueueWalRecord routes by (db_base_path, year) only.
            // Sits in what was struct padding after row_length, so sizeof(WalRecord) is unchanged.
            unsigned short year = 0;
            // max(the DB's EffectiveTrackedColumnCount, this record's widest
            // accepted row) as of the moment this record's rows were turned
            // into ZoneMapObservations -- carried
            // here (per RECORD, not per row: every row in a record shares
            // one (db, year) and one cfg snapshot) so ApplyWalRecord opens
            // this DB's zone map at exactly the width its observations were
            // built at. A header wider than the observations is the C1
            // wrong-answer bug: columns in the gap exist in the header but
            // are never folded, so they read back valid_count==0 over real
            // data. Defaults to the global floor so any producer that
            // forgets to set it still gets the pre-columnCount behavior
            // rather than a zero-width zone map.
            uint32_t tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS;
            std::vector<WalRow> rows;          // 1 entry for UnaryInsert; N for a BatchInsert/RangeInsert year-bucket group
            XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::SYNC;
            // One per REQUEST, not per row -- null for ASYNC (nobody
            // waits). Carries whether this record was actually durably
            // fsync'd AND applied (true) or not (false on any failure --
            // WAL write/fsync failure or a failed ApplyWalRecord row), so a
            // caller can tell "queued" from "actually durable" instead of
            // being told success unconditionally. A per-record handle (not
            // a shared batch counter) sidesteps any question of
            // cross-producer enqueue ordering: each caller only ever waits
            // on its own record's flag.
            WalCompletion *completion = nullptr;
        };
#if defined(__GLIBCXX__) && defined(__x86_64__) && (!defined(_GLIBCXX_USE_CXX11_ABI) || _GLIBCXX_USE_CXX11_ABI)
        // std::string is 32 bytes only in libstdc++ with the C++11 ABI; guard so a layout change is noticed. WalRecord::year must keep
        // fitting in the padding after row_length (record stays 120 bytes, two cache lines).
        static_assert(sizeof(WalRecord) == 120, "WalRecord grew: WalRecord::year must fit in existing padding");
#endif

        struct WalShard {
            moodycamel::BlockingConcurrentQueue<WalRecord> queue;
            std::thread writer_thread;
            std::atomic<bool> stop_flag{false};
#ifdef XSTORE_WAL_STATS_ENABLED
            // Batching observability -- compile-time debug/benchmark tool
            // only (XSTORE_WAL_STATS CMake option), not permanent
            // production instrumentation. Incremented once per drained
            // batch in WalWriterLoop; read by StartWalWriters' periodic
            // stats-reporter thread (also gated). Relaxed: single writer
            // (this shard's own writer thread), purely diagnostic reads.
            std::atomic<uint64_t> total_batches_processed{0};
            std::atomic<uint64_t> total_records_processed{0};
#endif
        };
        static std::array<WalShard, XStoreTunables::WAL_SHARD_COUNT> wal_shards;
        // How many of wal_shards' WAL_SHARD_COUNT slots actually have a
        // running writer thread -- set once by StartWalWriters from
        // hardware_concurrency(). Defaults to the full compile-time count
        // so any (theoretical) use before StartWalWriters runs still stays
        // in-bounds rather than modulo-by-zero.
        static std::atomic<size_t> active_wal_shard_count;

        // The WAL shard (0..active-1) every record for (db_base_path, year) goes to, whatever thread enqueues it. One
        // shard = one FIFO queue + one writer thread + one ring. For SYNC inserts that makes the ring order of writes
        // to one row (one (db, year) file) their acknowledgment order (an acknowledged SYNC record is already out of
        // the queue before a later one starts), so replay cannot reorder them. ASYNC is acknowledged when the record
        // is queued, and the queue is FIFO only per enqueuing thread, so ASYNC writes to one row from two threads have
        // no defined order (they still replay in the order they were applied). `active` consecutive years of one DB
        // map to `active` distinct shards. Pure and allocation-free; public so tests can call it.
        [[nodiscard]] static size_t WalShardFor(const std::string &db_base_path, unsigned year, size_t active) {
            return (std::hash<std::string>{}(db_base_path) + year) % active;
        }

        // Times a WAL writer gave up waiting for ring space (walring::Ring::Reserve
        // timed out) and failed a whole sub-batch as "overloaded". Process-wide.
        // Appended to a SYNC insert's error status when the WAL ring stayed full (see EnqueueWalRecord's `overloaded`).
        static constexpr const char *kWalOverloadedText = "server overloaded (WAL ring full), retry later";
        static std::atomic<uint64_t> wal_ring_stalls;
        // Records the WAL writer processed, by route (spec §3.3): applied + synced inline with no ring
        // record (wal_direct_records) vs appended to a ring (wal_ring_records). wal_direct_records counts
        // direct records applied successfully (a record whose apply failed is not counted; a later sync failure still is).
        // Relaxed, process-wide.
        static std::atomic<uint64_t> wal_direct_records;
        static std::atomic<uint64_t> wal_ring_records;

        // Set once by Shutdown(); see IsShutDown().
        static std::atomic<bool> shutdown_flag_;
        // Stops + joins every WAL writer thread (and the stats reporter).
        // Safe to call repeatedly; shared by Shutdown() and the atexit hook.
        // final_checkpoint_cycle=false (WalSimulateRestartForTests only) skips the
        // checkpointer's final cycle: a simulated kill -9.
        static void StopWalWriters(bool final_checkpoint_cycle = true);

        // CatIdx compaction background thread (Task 9): one process-wide
        // thread, started once from main.cpp (never from the XStore
        // constructor -- unlike wal_shards/StartWalWriters, this must NOT
        // fire during tests, since it owns a real background thread that
        // polls the filesystem on an hour-long interval and would race
        // against ScratchDir teardown between tests). Mirrors WalShard's
        // stop_flag/thread shape one level up (one thread total here, not
        // one per shard).
        static std::thread catidx_compaction_thread_;
        static std::atomic<bool> catidx_compaction_stop_flag_;

        std::string permitted_time_resolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int db_count = 0;
        std::vector<std::string> db_names = { };

        // Initialization
        XStore();

        // PROCESSING DbStats/info query
        XStore_Proto::SysInfo_REP InfoQuery(std::string_view server_addr, int thread_count);

        // CREATING new DB instance with row length. column_names is purely
        // persisted into ConfigStruct.columnNames (see that field's proto
        // comment) -- never validated against row_length/column_count_seed,
        // never touches storage layout or zone-map internals.
        [[nodiscard]] XStore_Proto::CreateDB_REP CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length, uint32_t column_count_seed = 0,
                                                            const std::vector<std::string> &column_names = {});

        // The single place that decides "how many columns does this DB's
        // zone map track," consumed by BuildZoneMapObservations, every
        // GetOrOpenZoneMap call site (insert/WAL-apply/aggregate-read) and
        // the startup rebuild scan. Returns
        // max(cfg.columncount(), ZONE_MAP_MAX_TRACKED_COLUMNS).
        //
        // ZONE_MAP_MAX_TRACKED_COLUMNS is a FLOOR here, not merely a
        // fallback for an unset columncount -- that distinction is a
        // correctness requirement, not a tuning choice. columncount is a
        // high-water mark that starts at 0 and is first written by the
        // insert AFTER that same insert already created the year's zone map
        // (see the call order in XStore_UnaryOps/BatchOps/RangeOps), so a
        // plain `columncount > 0 ? columncount : default` would NARROW the
        // effective width from 64 to, say, 1 the moment a DB's first
        // one-column row landed. The zone map's header can only ever widen
        // (ZoneMapFile::Open's deliberately asymmetric `>` guard), so a
        // narrowing observation width leaves every column between the new
        // width and the header's width permanently tracked-but-never-folded
        // -- valid_count==0 on a block that really does hold data, which
        // AccumulateSegmentWithZoneMapPushdown reads as "nothing here" and
        // ScanSegmentWithZoneMapPruning skips outright: a silent wrong
        // answer for MIN/MAX/SUM/AVG/VARIANCE/STDDEV. Flooring at the
        // global default makes this value monotonically non-decreasing for
        // a given DB, which is exactly the invariant that keeps the on-disk
        // header's tracked_column_count from ever exceeding the width the
        // observations were actually built at.
        static uint32_t EffectiveTrackedColumnCount(const XStore_Proto::ConfigStruct &cfg_struct);

        // Cheap on every insert (one integer comparison against the
        // already-cached cfg); only touches disk (a .CFG rewrite) on the
        // rare row that's wider than anything seen before for this DB --
        // see design doc "Column-count growth". Never rejects a wide row;
        // only ever grows the tracked mark.
        void MaybeGrowColumnCount(const std::string &db_name, const std::string &db_base_path, int observed_column_count);

        // REMOVE DB -- Removing an entire DB and its data files
        [[nodiscard]] XStore_Proto::RemoveDB_REP RemoveDB(std::string_view db_name);

        // REMOVE DB FILES -- Removing file(s) based on name in a certain DB
        [[nodiscard]] XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *proto_from_client, std::string_view db_name);

        // UNARY INSERT
        void UnaryInsert(unsigned long long int epoch_time, const XStore_Proto::RowData &data, std::string_view db_name, XStore_Proto::UnaryInsert_REP *return_data,
                          XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::SYNC);

        // UNARY QUERY
        void UnaryQuery(unsigned long long int epoch_time, std::string_view db_name, XStore_Proto::UnaryQuery_REP *return_data);

        // UNARY DELETE
        [[nodiscard]] XStore_Proto::UnaryDelete_REP UnaryDelete(unsigned long long int epoch_time, std::string_view db_name);

        // BATCH INSERT
        void BatchInsert(XStore_Proto::BatchInsert_REQ *proto_from_client, XStore_Proto::BatchInsert_REP *return_data);

        // BATCH QUERY (streamed, wire-format encode -- the only BatchQuery
        // path; the network server is the only production caller and it
        // needs this contract, not a materialized REP -- see
        // RangeQuery's declaration comment below for the general
        // contract (chunking, emit_chunk semantics). Callers that want a
        // synchronous, materialized REP (tests) use the xstore_test::
        // BatchQuery helper in XStoreTestSupport.h, which wraps this. Same
        // query semantics/output either way -- including holes being
        // OMITTED, not included as empty entries, unlike RangeQuery. Takes
        // proto_from_client non-const since sort_time=true sorts the
        // request's timestamps in place.
        void BatchQuery(XStore_Proto::BatchQuery_REQ *proto_from_client, bool sort_time, size_t chunk_size_bytes,
                                 const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk);

        // BATCH DELETE
        [[nodiscard]] XStore_Proto::BatchDelete_REP BatchDelete(XStore_Proto::BatchDelete_REQ *proto_from_client);

        // RANGE INSERT. Takes the request non-const because input that is not
        // ascending is sorted in place (only then; sorted input is never sorted).
        void RangeInsert(XStore_Proto::RangeInsert_REQ *proto_from_client, XStore_Proto::RangeInsert_REP *return_data);

        // RANGE QUERY (streamed, wire-format encode -- the only RangeQuery
        // path; the network server is the only production caller. Never
        // builds a UnaryQuery_REP/RowData/FData object tree: each row's
        // already-serialized on-disk bytes are copied directly into a plain
        // buffer (see XStore_RangeOps.cpp for the wire-encode helpers),
        // which is handed to emit_chunk once it reaches chunk_size_bytes --
        // bounding peak memory to ~chunk_size_bytes regardless of how many
        // rows the query spans, instead of the whole response tree living in
        // the arena at once. emit_chunk's bool argument is true only for the
        // final call. A query whose total output never reaches
        // chunk_size_bytes still calls emit_chunk exactly once
        // (is_last=true) -- chunking is not a separate code path, just what
        // this naturally degenerates to. Callers that want a synchronous,
        // materialized RangeQuery_REP (tests) use the xstore_test::
        // RangeQuery helper in XStoreTestSupport.h, which wraps this
        // (concatenate every emitted chunk, parse once) -- byte-identical
        // result, just reassembled instead of built directly.
        void RangeQuery(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                                 std::string_view db_name, size_t chunk_size_bytes,
                                 const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk);

        // FIND BY VALUE (streamed, wire-format encode -- same chunking
        // contract as RangeQuery above, same RangeQuery_REP wire shape on
        // the response). Equality lookup on one column: [startEpochT,
        // endEpochT] narrows the year/row span the same way RangeQuery
        // does, columnIdx picks which column to compare, and only rows
        // whose column exactly matches value are emitted -- unlike
        // RangeQuery, which emits every row in range unconditionally. Not
        // built on WalkYearSpan/WalkYearSpanParallel: this needs a real
        // per-row epoch_time (those kernels only ever hand back a
        // year-relative row index), so it has its own RangeQuery-shaped
        // manual per-year loop instead (see XStore_RangeOps.cpp -- no,
        // XStore_Aggregates.cpp -- for the implementation and why). Per
        // year, a CatIdxFile bloom filter (Task 4/5) prunes whole blocks
        // that cannot contain the target value before falling back to a
        // real per-row decode+compare scan for any block the bloom can't
        // rule out; columnIdx >= this DB's EffectiveTrackedColumnCount (or
        // negative) has no bloom coverage at all and unconditionally
        // real-scans every block instead, still correct, just without the
        // pruning speedup.
        void FindByValue(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                          std::string_view db_name, int32_t column_idx, const XStore_Proto::FData &value,
                          size_t chunk_size_bytes, const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk);

        // FIND BY PATTERN -- TEMPORARY IMPLEMENTATION (option A). Streams
        // every row in [startEpochT, endEpochT] whose column_idx holds a
        // stringData/bytesData value matching pattern (PATTERN_LIKE: SQL
        // LIKE/ILIKE; PATTERN_REGEX: RE2 partial match), in the same chunked
        // RangeQuery_REP wire shape as FindByValue. Brute-force: every row in
        // range is decoded and tested, no index. The RPC is intended to be
        // permanent; this scan is the baseline for the pending A (scan) vs.
        // B (per-block trigram bloom) vs. C (inverted index) decision -- see
        // TODO.md. Lives alone in XStore_PatternOps.cpp so B/C replace one file.
        void FindByPattern(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t,
                           std::string_view db_name, int32_t column_idx, std::string_view pattern,
                           XStore_Proto::PatternMode mode, bool case_insensitive,
                           size_t chunk_size_bytes, const std::function<void(std::string &&chunk, bool is_last)> &emit_chunk);

        // RANGE DELETE
        [[nodiscard]] XStore_Proto::RangeDelete_REP RangeDelete(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name);

        // Walks [startEpochT, endEpochT] one year-file at a time for
        // Min/Max/Sum/AvgAggregate, read-locking each existing segment and
        // handing kernel(rowsBase, rowsToProcess, year, firstRowIndex) a
        // pointer straight into GetOrMapFile's cached mapping -- a member
        // function (not a free one) purely so it can call GetOrMapFile,
        // which is private. year/firstRowIndex (the row-within-year index
        // of rowsBase's first row, i.e. the same "diff" every insert path
        // computes) let a zone-map-aware kernel open the right (db, year)
        // ZoneMapFile and resolve which blocks this segment covers -- added
        // for Tasks 6-7; a kernel that doesn't need them just ignores them.
        template <typename Kernel>
        void WalkYearSpan(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                          unsigned long long start_epoch_t, unsigned long long end_epoch_t, Kernel &&kernel);

        // Same year-by-year walk as WalkYearSpan, but distributes segments
        // across a bounded thread pool when the span covers enough distinct
        // years to be worth it -- narrow spans (the common case) fall back
        // to calling WalkYearSpan directly, no thread spawned at all.
        // segmentKernel(PartialState&, rowsBase, rowsToProcess, year,
        // firstRowIndex) accumulates into a PER-THREAD-LOCAL PartialState,
        // never a value shared across threads -- protobuf/ValueStruct
        // objects aren't safe for concurrent mutation from multiple
        // threads even on logically-disjoint fields (see protobuf's own
        // thread-safety docs). mergeFn (finalState&, partialState) combines
        // each thread's partial result into the running total sequentially
        // after all threads join -- cheap, O(#threads) not O(#rows).
        // Returns the fully-merged state.
        template <typename PartialState, typename SegmentKernel, typename MergeFn>
        PartialState WalkYearSpanParallel(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                                            unsigned long long start_epoch_t, unsigned long long end_epoch_t,
                                            PartialState initial_state, SegmentKernel &&segment_kernel, MergeFn &&merge_fn);

        // Shared mmap-backed scan kernels used by Min/Max/Sum/AvgAggregate ----------
        // rowsBase points directly into GetOrMapFile's cached, already-resident
        // mapping (offset to the first row this kernel should scan) -- no
        // mmap()/munmap() here anymore. That per-call setup/teardown used to
        // happen fresh on every single aggregate call (WalkYearSpan mapped just
        // the queried sub-range itself, never through the shared cache), unlike
        // every other read path, which all reuse GetOrMapFile's MRU-cached
        // mapping. Now this shares it too (WalkYearSpan also issues one
        // madvise(WILLNEED) per segment before calling the kernel, to prefetch
        // the queried span instead of relying on lazy per-page faulting).
        //
        // Each row is extracted via ExtractColumnByIndexSkipParse (see the .cpp
        // for the tag-walk mechanics) rather than a full RowData parse -- these
        // kernels only ever want ONE column, so decoding every column in the
        // row was pure waste. See the .cpp for how holes/wrong-type/corrupt
        // columns are handled.
        bool ExtractColumnByIndexSkipParse(const char* row_ptr, size_t row_len, unsigned short column_idx, XStore_Proto::FData& out_data);

        // Select kernel: tracks the single winning row under comparator Cmp(candidate, currentBest).
        //  Cmp = std::less<long double>  -> MIN ;  Cmp = std::greater<long double> -> MAX
        template <typename Cmp>
        void ProcessFileRangeSelect(const char* rows_base, unsigned long long total_rows,
                                     size_t row_len, unsigned short column_idx,
                                     ValueStruct& best_value, XStore_Proto::FData& best_data);

        // Accumulate kernel: running sum + valid-row count. Backs SUM directly, AVG divides after.
        // sum_sq_out is an optional running sum-of-squares fold for VARIANCE/STDDEV
        // (Task 2) -- nullptr (the default) skips it entirely, so SUM/AVG's call
        // sites are untouched.
        void ProcessFileRangeAccumulate(const char* rows_base, unsigned long long total_rows,
                                         size_t row_len, unsigned short column_idx,
                                         double& sum_out, unsigned long long& valid_count_out,
                                         double *sum_sq_out = nullptr);

        // OHLC real-scan fallback kernel: one pass over [0, total_rows) that
        // finds the chronologically first (open) and last (close) numeric
        // value plus the value-extremes (high/low), for a sub-range a zone
        // map either doesn't fully cover or has no coverage for yet. Rows
        // are walked in ascending order (i=0..total_rows-1, i.e. ascending
        // time within this sub-range), so "first hit wins" gives open and
        // "always overwrite on a hit" gives close -- no comparison against
        // a row_index needed here, unlike ZoneMapFile::UpdateRow, because a
        // single scan never sees an out-of-order row the way a live
        // out-of-order insert can. The *_set_out flags distinguish "still
        // zero because unset" from "genuinely zero" the same way
        // ValueStruct::is_initialized does for ProcessFileRangeSelect.
        void ProcessFileRangeOhlc(const char* rows_base, unsigned long long total_rows,
                                   size_t row_len, unsigned short column_idx,
                                   double &open_out, bool &open_set_out,
                                   double &high_out, double &low_out, bool &high_low_set_out,
                                   double &close_out, bool &close_set_out);

        // Collect kernel: appends every real numeric value of columnIdx in
        // [0, total_rows) to values_out, in ascending-time order (order is
        // not load-bearing for any of Percentile/DistinctCount/TopK -- all
        // three reduce over the full collected set, not a running scalar --
        // it's just a byproduct of the same forward scan every other kernel
        // here uses). Shared by all three exact-mode aggregates below --
        // deliberately one kernel, not three, since they only differ in
        // what they compute FROM the collected values, not how those values
        // are gathered. No zone-map pushdown exists or is planned for these:
        // unlike min/max/sum/first/last, "the k most frequent values" or
        // "the p-th percentile" cannot be reconstructed from any small
        // per-block summary without becoming the approximate/sketch-based
        // version this deliberately is not (see TODO.md's zone-map
        // follow-on section) -- every call is a real, full scan.
        void ProcessFileRangeCollectValues(const char* rows_base, unsigned long long total_rows,
                                            size_t row_len, unsigned short column_idx,
                                            std::vector<double> &values_out);

        // Shared by MinAggregate/MaxAggregate (Task 6): splits one
        // WalkYearSpan segment into its zone-map blocks and, for each,
        // either skips it (should_skip_block(entry) says this block's
        // column stats prove it can't hold anything better than what
        // best_value already has) or falls through to a real
        // ProcessFileRangeSelect<Cmp> scan over just that block's row
        // sub-range. A block with no on-disk zone-map coverage yet, or
        // whose column is past ZONE_MAP_MAX_TRACKED_COLUMNS, is always
        // scanned for real -- pruning only ever skips a block the zone map
        // can positively rule out, never guesses. should_skip_block is
        // Cmp-specific (MIN checks entry.min_value, MAX checks
        // entry.max_value) so it's passed in rather than hardcoded here.
        //
        // tracked_column_count is this DB's EffectiveTrackedColumnCount,
        // forwarded to GetOrOpenZoneMap so a read that happens to be the
        // first toucher of a (db, year) creates the sidecar at the DB's own
        // width instead of the global default -- the same width every
        // insert/WAL-apply for this DB uses, so no caller can leave a header
        // claiming columns that were never folded.
        template <typename Cmp, typename PruneCheck>
        void ScanSegmentWithZoneMapPruning(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                            uint32_t tracked_column_count,
                                            const char *rows_base, unsigned long long rows_to_process,
                                            unsigned long long first_row_index, unsigned short column_idx,
                                            ValueStruct &best_value, XStore_Proto::FData &best_data,
                                            PruneCheck &&should_skip_block);

        // Shared by SumAggregate/AvgAggregate (Task 7): same per-segment
        // block split as ScanSegmentWithZoneMapPruning above, but for
        // accumulation instead of a single winner. A block contributes its
        // precomputed sum_value/valid_count with zero row scan only when
        // the block's OWN full row span (not the query-clipped sub-range)
        // lies entirely inside [firstRowIndex, firstRowIndex+rowsToProcess)
        // -- a block only partially covered by the query still needs a
        // real ProcessFileRangeAccumulate scan, since the zone map's sum
        // covers the whole block, not just the queried slice of it.
        // sum_sq_out (Task 2, VARIANCE/STDDEV): optional running sum-of-squares
        // fold, mirrored alongside sum_out/valid_count_out -- a block that takes
        // the zero-scan pushdown branch adds e.sum_sq_value, a block that falls
        // back to a real scan forwards the pointer into ProcessFileRangeAccumulate.
        // nullptr (the default) preserves SUM/AVG's call sites byte-for-byte.
        // tracked_column_count: same role as in ScanSegmentWithZoneMapPruning
        // above -- this DB's EffectiveTrackedColumnCount, forwarded to
        // GetOrOpenZoneMap.
        void AccumulateSegmentWithZoneMapPushdown(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                                   uint32_t tracked_column_count,
                                                   const char *rows_base, unsigned long long rows_to_process,
                                                   unsigned long long first_row_index, unsigned short column_idx,
                                                   double &sum_out, unsigned long long &valid_count_out,
                                                   double *sum_sq_out = nullptr);

        // MIN - AGGREGATE
        void MinAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Min_REP *return_data);

        // MAX - AGGREGATE
        void MaxAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Max_REP *return_data);

        // SUM - AGGREGATE
        void SumAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Sum_REP *return_data);

        // AVG - AGGREGATE
        void AvgAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Avg_REP *return_data);

        // VARIANCE - AGGREGATE (sample variance, divisor N-1)
        void VarianceAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Variance_REP *return_data);

        // STDDEV - AGGREGATE (sqrt of sample variance)
        void StddevAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Stddev_REP *return_data);

        // Shared by ExistsAggregate: per-segment block walk that returns as
        // soon as existence is proven, instead of accumulating a value like
        // ScanSegmentWithZoneMapPruning/AccumulateSegmentWithZoneMapPushdown
        // above. A block with zone-map coverage and valid_count > 0 proves
        // existence with zero row scan (no new zone-map field needed --
        // valid_count already means "at least one numeric value here"); a
        // block with valid_count == 0 is skipped outright; a block with no
        // zone-map coverage yet falls back to a real ProcessFileRangeSelect
        // scan (same kernel MinAggregate uses) and checks whether it found
        // anything. found_out is only ever set to true, never cleared --
        // callers pass a fresh false per segment.
        void ScanSegmentForExistence(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                      uint32_t tracked_column_count,
                                      const char *rows_base, unsigned long long rows_to_process,
                                      unsigned long long first_row_index, unsigned short column_idx,
                                      bool &found_out);

        // Shared by OhlcAggregate: same per-segment block split as
        // ScanSegmentWithZoneMapPruning/AccumulateSegmentWithZoneMapPushdown
        // above, but merging four running values (open/high/low/close) into
        // the caller's out-refs instead of one -- individual primitive refs,
        // same convention AccumulateSegmentWithZoneMapPushdown uses (not the
        // OhlcPartial struct itself, which stays local to XStore_Aggregates.cpp's
        // anonymous namespace). A block fully covered by the query with
        // zone-map coverage contributes its own first_value/last_value/
        // min_value/max_value with zero row scan (open only if not already
        // set -- first block wins; close always overwritten -- later block
        // wins, safe because blocks within one segment, and segments across
        // WalkYearSpanParallel's own chronologically-ordered vector, are
        // always visited earliest-to-latest); a block without full zone-map
        // coverage falls back to ProcessFileRangeOhlc over just its queried
        // sub-range and merges that local result the same way. Calling this
        // repeatedly on the SAME out-refs across multiple years
        // (WalkYearSpanParallel's narrow-span sequential path) is exactly as
        // correct as calling it once per segment then merging (its
        // wide-span parallel path) -- both reduce to "first open wins, last
        // close wins, running min/max" over a chronologically-ordered
        // sequence of contributions.
        void ScanSegmentForOhlc(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                 uint32_t tracked_column_count,
                                 const char *rows_base, unsigned long long rows_to_process,
                                 unsigned long long first_row_index, unsigned short column_idx,
                                 double &open_out, bool &open_set_out,
                                 double &high_out, double &low_out, bool &high_low_set_out,
                                 double &close_out, bool &close_set_out);

        // EXISTS - AGGREGATE: does at least one row in [startEpochT, endEpochT]
        // have a non-null/numeric value for columnIdx? No cross-segment/
        // thread short-circuit (same YAGNI precedent as Min/Max, which don't
        // cancel other in-flight segments either) -- the win is per-segment
        // zone-map block-skip plus stopping at the first hit within a
        // segment, not global cancellation.
        void ExistsAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Exists_REP *return_data);

        // OHLC - AGGREGATE (open/high/low/close bar over [startEpochT, endEpochT]):
        // Open/Close are the chronologically first/last numeric values in
        // range (ZoneMapEntry.first_value/last_value, new in v3, pushdown-
        // accelerated the same way Sum/Avg's sum_value already is); High/Low
        // are the existing value-extremes (ZoneMapEntry.max_value/min_value,
        // same fields MIN/MAX already use). All four computed in one block
        // walk rather than as separate ops.
        void OhlcAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Ohlc_REP *return_data);

        // PERCENTILE - AGGREGATE (exact, PERCENTILE_CONT/linear-interpolation
        // semantics; percentile in [0.0, 1.0], 0.5 = median). Real full scan
        // via ProcessFileRangeCollectValues, no zone-map pushdown -- see that
        // kernel's comment for why. No valid data in range is an ERROR (same
        // precedent as Min/Max/Sum/Avg/Variance/Stddev -- a percentile of
        // nothing is undefined, unlike Exists/DistinctCount below where
        // "nothing found" is itself a legitimate answer).
        void PercentileAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, double percentile, XStore_Proto::Percentile_REP *return_data);

        // DISTINCTCOUNT - AGGREGATE (exact, real hash-set over every value in
        // range -- same cost/memory tradeoff as SQL's COUNT(DISTINCT x)).
        // An empty range is SUCCESS with count=0, not an error -- same
        // "absence is a legitimate answer" precedent ExistsAggregate uses.
        void DistinctCountAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::DistinctCount_REP *return_data);

        // TOPK - AGGREGATE (exact, real frequency hash-map over every value
        // in range; k=1 gives the mode -- one op, not a separate MODE op,
        // same "don't split what's one computation" precedent OHLC used).
        // Ties broken by ascending value for determinism. Fewer than k
        // distinct values in range returns however many exist, same as SQL's
        // GROUP BY ... ORDER BY COUNT(*) DESC LIMIT k; an empty range is
        // SUCCESS with zero entries, not an error (same precedent as
        // DistinctCountAggregate above).
        void TopKAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, uint32_t k, XStore_Proto::TopK_REP *return_data);

        // Initialize DB. Loads every DB's ConfigStruct into the shared CfgMap,
        // then (Task 5) rebuilds any stale/missing zone map for that DB
        // before returning -- see RebuildStaleIndexesForDb below. Guarded
        // (at the single call site inside XStore::XStore()) by a function-
        // local std::once-per-directory mechanism so concurrent worker
        // threads' first XStore construction against the same
        // data_store_dir block on this finishing; calling InitDB() again
        // directly (as tests do) always re-runs it, guard or not.
        void InitDB();

        // Graceful shutdown (Durability A DD7 / Durability C). In order: (0) sets
        // the shutdown flag, so Unary/Batch/RangeInsert and EnqueueWalRecord reject
        // writes from here on; (1) StopWalWriters(): joins the WAL writer threads,
        // then the checkpointer runs its FINAL cycle -- every data file dirtied
        // since the previous cycle is fdatasynced (by path, throwaway fd) and each
        // ring's checkpoint_lsn moves to its head, so the next start replays and
        // force-rebuilds nothing; (2) if that final cycle succeeded and the
        // checkpointer is not poisoned, MarkClean()s every registry-open
        // ZoneMapFile / CatIdxFile. Otherwise it logs an ERROR and leaves every
        // index dirty (the next start rebuilds them) -- it still returns normally.
        // The gate is deliberately the final cycle's outcome, not per-path
        // membership in its synced set: files clean since an earlier cycle were
        // synced then. Shutdown re-syncs nothing itself (a fresh fd would not see a
        // prior writeback error). ASYNC records still queued when the writers stop
        // are dropped (unchanged behavior). Idempotent (atomic flag; later calls
        // are no-ops).
        //
        // PRECONDITION: the caller has already stopped serving requests (the
        // ZMQ workers are stopped/drained -- see main.cpp). An insert racing
        // Shutdown could leave an index clean over an unsynced data row, or
        // enqueue a SYNC waiter after the WAL writer exited, which would
        // never be woken.
        static void Shutdown();
        // Single relaxed load -- cheap enough for every insert's entry.
        static bool IsShutDown() { return shutdown_flag_.load(std::memory_order_relaxed); }
        // Test-only: undoes Shutdown() (clears the flag, restarts the WAL
        // writer threads) so later tests in the same process can insert.
        // Public and ungated by design: the "ForTests" suffix is the convention
        // (same as ClearZoneMapRegistryForTests).
        static void ResetShutdownForTests();
        // Test-only: simulates kill -9 + restart inside one process. Stops the WAL
        // writers and the checkpointer WITHOUT a final cycle or
        // MarkClean; drops the ring registry, the dirty tracker and the zone-map /
        // catidx registries; then restarts writers + checkpointer (as
        // ResetShutdownForTests does). Safe whether or not Shutdown() ran. Callers
        // clear cfg_shards themselves and call InitDB() to replay.
        static void WalSimulateRestartForTests();
        // Test-only: close + drop target_file's cached fd (a "new process" does not have it).
        void EvictFdForTests(const std::string &target_file) { EraseOpenFile(target_file); }
        // Test-only: cache an fd for target_file WITHOUT growing it past total_rows (a cold GetOrOpenFile
        // preallocates the whole calendar year, which makes a replay's forced rebuild scan 31.5M rows).
        void WarmFdForTests(const std::string &target_file, size_t row_length, unsigned long long total_rows) {
            static_cast<void>(GetOrOpenFile(target_file, row_length, total_rows));
        }

        // Fetch (creating if needed) the ZoneMapFile for (dbBasePath, year),
        // opening its "<dbBasePath>/<year>.zmap" sidecar on first touch and
        // reusing the same in-registry instance on every later call --
        // consumed by every zone-map-aware insert/aggregate path (Tasks
        // 4-7). row_length is only used on first creation of that (db,
        // year) pair's .zmap file; see ZoneMapFile::Open. Bounded at
        // ZONE_MAP_REGISTRY_CAP with LRU eviction -- see zone_map_registry_
        // below for why eviction here is always safe.
        //
        // tracked_column_count is forwarded to ZoneMapFile::Open (only used
        // on first creation/stale-version reinit, same as row_length) and
        // defaulted to the global ZONE_MAP_MAX_TRACKED_COLUMNS for test
        // call sites that don't care. EVERY production call site --
        // ApplyWalRecord (the one place an insert's rows are actually
        // written), both aggregate-read segment helpers, and
        // RebuildOneYearZoneMap -- passes the DB's real
        // EffectiveTrackedColumnCount, so no two callers can disagree about
        // how wide a given DB's zone map is.
        //
        // This function additionally clamps the request down to a
        // current-version file's existing on-disk width, so only
        // RebuildOneYearZoneMap (which truncates the file to 0 bytes first,
        // then re-folds every row from raw data) can ever widen one -- see
        // the guard's own comment in XStore_Management.cpp for why a
        // request-path widen would be a silent data-loss bug.
        //
        // static, like fd_shards/wal_shards below: ApplyWalRecord (the
        // actual write site every insert path funnels through, on
        // whichever WAL writer thread ends up applying a given record) has
        // no XStore instance to call through -- a per-instance registry
        // (as originally shipped in Task 3) would be unreachable from
        // there. One process-wide registry also matches how ZmqServer
        // actually runs: one XStore instance per worker thread (see
        // ZMQ_Server.cpp), so a per-instance cache would have silently
        // fragmented into one independent registry (and LRU cap) per
        // worker anyway.
        static ZoneMapFile& GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);

        // Same lookup/open as GetOrOpenZoneMap, plus a pin taken under the registry mutex: the registry never evicts
        // or destroys the file while the returned Pin lives. Every production caller that uses the file after the
        // lookup returns takes one (inserts, deletes, aggregates, FindByValue, compaction, the builders); the plain
        // GetOrOpen* above are for tests and single-threaded setup only, where nothing can evict concurrently.
        // Explicit erases (RebuildYearIndexes at startup, Clear*RegistryForTests) ignore pins: never while a pin
        // holder is alive. RemoveDB/RemoveDBFiles retire a pinned entry instead of destroying it (see
        // RetireIndexesUnderPrefix).
        [[nodiscard]] static ZoneMapFile::Pin PinZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                                         uint32_t tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);

        // Delete paths: the (db, year) zone map, which must flag (or, for RangeDelete, reset) the blocks of the rows
        // before they are punched, pinned for the delete. An empty Pin (logged) when it cannot be opened; the caller must
        // then not delete.
        [[nodiscard]] static ZoneMapFile::Pin OpenZoneMapForDelete(const std::string &db_base_path, unsigned short year,
                                                               const XStore_Proto::ConfigStruct &cfg_struct);

        // Lookup-only peek at the same process-wide registry GetOrOpenZoneMap
        // uses (same key, same mutex) -- never opens, creates, or touches LRU
        // order. Returns the warm instance's TrackedColumnCount() if this
        // (db_base_path, year) is already cached, nullopt otherwise (a cold
        // caller falls back to a header pread instead of paying to warm the
        // registry just to answer a peek). Exists so UnindexedColumnNotice
        // can check an already-open zone map for free before ever touching
        // disk -- see its call site for why that matters on the hot path.
        // `rebuilding` (optional): set to the warm instance's Rebuilding() (a live widen running, or a stopped/failed
        // one left untrusted until the next start); false when nothing is cached.
        static std::optional<uint32_t> PeekOpenZoneMapWidth(std::string_view db_base_path, unsigned short year,
                                                            bool *rebuilding = nullptr);

        // Test-only introspection -- not part of the public API surface any
        // real caller needs, same as this codebase's other test-only
        // accessors.
        static size_t ZoneMapRegistrySize() { return zone_map_registry_.size(); }

        // Test-only: drop every warm ZoneMapFile instance, forcing the next
        // GetOrOpenZoneMap for any (db, year) to do a real disk-backed
        // Open(). Only closes fds -- UpdateRow is write-through (Task 2), so
        // nothing durable is lost, exactly like the LRU eviction in
        // GetOrOpenZoneMap itself. Exists so a test can simulate a genuine
        // process restart (cold registry) within one test binary, which is
        // the only way to reproduce the WAL-replay/zone-map gap the
        // ReplayedYearMap plumbing fixes -- a warm registry hands back an
        // instance still holding an fd to the unlinked sidecar and hides it.
        static void ClearZoneMapRegistryForTests();

        // RemoveDB: drop every zone-map and catidx registry entry whose key starts with db_dir_prefix (the DB
        // directory WITH its trailing '/', so DB1 never matches DB10). An unpinned entry is destroyed (its files are
        // being deleted, and nobody uses a registry file without a pin); a pinned one moves to the registry's retired
        // list, which lookups never search, and keeps writing to its unlinked inode until its last pin drops. Retired
        // entries are freed by the next retire or the next cold open, once unpinned. Takes only the registry mutexes
        // and reads only the pin atomics (never a file mutex). Only for files that are being deleted: retiring an entry
        // whose files stay on disk lets a cold open create a second object on the same file.
        static void RetireIndexesUnderPrefix(const std::string &db_dir_prefix);

        // RemoveDBFiles of "<year>.XSTORE" (which also deletes that year's index files): the same, for the one exact
        // key "<db_base_path>/<year>" (year 2020 never matches 20201).
        static void RetireYearIndexes(std::string_view db_base_path, unsigned short year);

        // Test-only: retired zone-map plus catidx entries not yet freed.
        static size_t RetiredIndexCountForTests();

        // Categorical bloom-index registry: one CatIdxFile per (db, year)
        // pair, same process-wide/static shape and LRU-eviction-is-always-
        // safe reasoning as GetOrOpenZoneMap above (CatIdxFile::UpdateRow is
        // also write-through). Unlike GetOrOpenZoneMap, this does NOT clamp
        // tracked_column_count down to an existing file's on-disk width --
        // that guard lands in a later task once schema growth is wired up
        // for .catidx.
        static CatIdxFile& GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

        // GetOrOpenCatIdx plus a registry pin; same contract as PinZoneMap.
        [[nodiscard]] static CatIdxFile::Pin PinCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                                       uint32_t tracked_column_count = XStoreTunables::CATIDX_MAX_TRACKED_COLUMNS);

        // Test-only: mirrors ClearZoneMapRegistryForTests -- drops every
        // warm CatIdxFile instance so the next GetOrOpenCatIdx reopens from
        // disk.
        static void ClearCatIdxRegistryForTests();

        // Test-only: drop every cached mapping whose path falls under
        // db_dir_prefix, without deleting the DB itself (unlike RemoveDB,
        // which calls the private EraseMappedFilesUnderPrefix this wraps for
        // the same reason). Dropping the cache's entry releases this
        // function's own shared_ptr reference; the actual munmap happens
        // when the last shared_ptr holder releases the mapping -- immediately
        // if no query is in flight, or once any in-flight query finishes. A
        // test that wants to simulate a truly cold page cache (e.g. before a
        // perf test's timed "cold" run) must drop this process's own mmap
        // first -- posix_fadvise(DONTNEED) can't evict a page still backing a
        // live mapping in this process, so page-cache-only eviction alone
        // leaves a previously-touched file fully resident.
        void EvictMappedFilesUnderPrefixForTests(const std::string &db_dir_prefix);

        // Spawns the CatIdx compaction background thread (Task 9): once a
        // year file goes cold (year < current calendar year), scans it once
        // and builds an exact per-value bitmap directory, gated by
        // CATIDX_MAX_BITMAP_DISTINCT_VALUES so a high-cardinality/continuous
        // column never gets one (stays bloom+real-scan forever). Called
        // exactly once, from main.cpp only -- deliberately NOT from the
        // XStore constructor (unlike StartWalWriters), since every test in
        // this binary constructs XStore instances against short-lived
        // ScratchDir directories and this thread's own filesystem polling
        // would otherwise race their teardown. The spawned thread owns one
        // local XStore instance for its lifetime (this codebase's "one
        // XStore per worker thread" convention, see the data_store_dir
        // member comment above) so it can reach the non-static
        // ExtractColumnByIndexSkipParse/CompactColdCatIdxForAllDbs path.
        static void StartCatIdxCompactionThread(const std::string &data_store_dir);
        // Stops + joins the compaction thread (no-op if never started or
        // already stopped); the atexit hook runs the same code. main.cpp
        // calls it before Shutdown() so a compaction cannot re-dirty an
        // index Shutdown just marked clean.
        static void StopCatIdxCompactionThread();

        // CatIdx compaction (Task 9): scans one cold <YEAR>.XSTORE file
        // row-by-row and builds a bitmap section (value -> row indices) per
        // tracked column via CatIdxFile::AppendBitmapSection, abandoning any
        // column mid-scan the instant its distinct-value count crosses
        // CATIDX_MAX_BITMAP_DISTINCT_VALUES (freed immediately, never
        // finishes accumulating a disqualified column). Idempotent no-op if
        // this year's .catidx already has a bitmap section.
        //
        // Deliberately a non-static instance method, unlike
        // RebuildOneYearZoneMap below -- it must call the non-static
        // ExtractColumnByIndexSkipParse, which needs an XStore instance. The
        // background thread in StartCatIdxCompactionThread owns one such
        // instance for its lifetime.
        //
        // Public (like GetOrOpenCatIdx/ClearCatIdxRegistryForTests above):
        // it's both the real internal entry point CompactColdCatIdxForAllDbs
        // calls, and the direct test entry point for exercising compaction
        // synchronously without waiting on the hourly background thread.
        void CompactOneYearCatIdx(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                   uint32_t tracked_column_count);

        // Scans every DB directory under data_store_dir and compacts every
        // cold (year < current calendar year) .XSTORE file that doesn't
        // already have a bitmap section. Called once per poll interval by
        // the background thread started from StartCatIdxCompactionThread.
        // Public since M4 stage 3 so tests can run it next to RunLiveWidenPass
        // (it takes part in the RemoveDB handshake, see bg_mutex_).
        void CompactColdCatIdxForAllDbs(const std::string &data_store_dir);

        // One tick of the background index thread (StartCatIdxCompactionThread runs it every LIVE_WIDEN_TICK_SEC with
        // the real clock; tests with a fake one). Due live widening first (RunLiveWidenPass). Compaction yields to
        // widening: while a widen is recorded (live_widen_pending_) compaction is not started, and a running pass
        // abandons its year at its next 64K-row check (nothing appended; retried later). Once no widen is recorded, a
        // compaction after any widen runs LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC later; otherwise
        // compaction runs CATIDX_COMPACTION_POLL_INTERVAL_SEC after the end of its previous run, as before.
        struct BackgroundIndexSchedule {
            std::chrono::steady_clock::time_point next_compaction{}; // the clock's epoch: the first tick compacts
            bool compaction_yielded = false; // a widen was pending: the next compaction is brought forward to after it
        };
        void BackgroundIndexTick(const std::string &data_store_dir, BackgroundIndexSchedule &schedule,
                                 const std::function<std::chrono::steady_clock::time_point()> &now);

        // Rebuilds this year's .catidx bloom from the year file (Task 11): a thin wrapper over RebuildYearIndexes with
        // only the catidx selected, so it takes the same path InitDB does (the .zmap is not touched). A correctness
        // requirement, not an optimization: the bloom must never produce a false negative, and a discarded/narrow file
        // cannot self-heal from future writes alone. Startup/tests only (see RebuildYearIndexes).
        void RebuildOneYearCatIdxBloom(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                        uint32_t tracked_column_count);

        // Live index widening, M4 stage 2 (Tech Reference 4.18 "Live widening"). Called by RunLiveWidenPass (stage 3).
        enum class LiveWidenResult {
            kDone,              // every selected file rebuilt at the new width, durable and clean
            kNothingToDo,       // nothing selected, or the year has no data file
            kAlreadyRebuilding, // nothing began and a selected file is already being rebuilt (busy): the zone map is
                                // busy (then the catidx is not touched either), or the catidx is busy and the zone map
                                // was not selected or failed to begin. Nothing touched. See LiveWidenStats::*_busy.
            kPartial,           // the build completed for the files that began; another selected file could not begin:
                                // BeginRebuild failed (old index intact and trusted) or it was busy (a catidx-only
                                // builder owns it; left alone). See LiveWidenStats::*_began / *_busy.
            kStopped,           // *stop was set: the files stay in rebuild at their frontier, dirty
            kFailed,            // nothing could start, or a read/write/sync failed: see the log; files that began stay
                                // in rebuild (dirty, untrusted past their frontier) until the next start rebuilds them
        };
        struct LiveWidenStats {
            uint64_t chunks = 0;                   // locked ranges processed
            uint64_t hole_chunks = 0;              // ... of which held no data row
            uint64_t rows_folded = 0;
            uint64_t lock_timeouts = 0;
            uint64_t max_consecutive_timeouts = 0;
            uint64_t total_lock_hold_us = 0;
            uint64_t max_lock_hold_us = 0;
            uint64_t writer_yields = 0;            // lock attempts that waited for a waiting writer at least once
            uint64_t writer_yield_us = 0;          // total time those attempts spent yielding
            std::vector<uint32_t> lock_hold_us;    // one entry per chunk
            bool zmap_widened = false;             // finished (FinishRebuild succeeded) at the new width
            bool catidx_widened = false;
            // How each selected file's begin went. began: this call rebuilt (or tried to rebuild) it. busy: another
            // rebuild owned it, so it was not touched: it is in rebuild state and untrusted past its frontier until
            // that rebuild ends or the next start rebuilds it (do not retry in a loop). Neither: BeginRebuild failed,
            // the old (narrower) index is intact and trusted, and a retry with backoff is reasonable.
            bool zmap_began = false;
            bool catidx_began = false;
            bool zmap_busy = false;
            bool catidx_busy = false;
        };
        // Rebuilds this year's .zmap (widen_zmap) and/or .catidx (widen_catidx) IN PLACE at tracked_column_count while
        // requests keep running, with ONE scan of the year file. db_base_path must be the exact string the writers use
        // (CachedDbBasePath): the data-file lock key is CachedYearFilePath(db_base_path, year). The caller decides
        // which files need it (a selected file is rebuilt whatever its width). Protocol per chunk [F, F+C): timed
        // READ range lock on exactly those bytes; under it the hole probe, the reads, the fold into both files and
        // AdvanceFrontier on both; then unlock and pause. Pins are held from before BeginRebuild until after
        // MarkClean; registry entries are never erased. Completion: FinishRebuild, fdatasync of the data file,
        // MarkClean. A BeginRebuild that fails leaves that file's old index as it was (still trusted). Never throws.
        // `stop` (may be nullptr) is checked before every lock attempt and every pause.
        // Workers (Tech Reference 4.18 "Parallel live widen"): with workers > 1 the year is cut into groups of about
        // group_bytes (whole zone-map blocks), claimed in order by `workers` threads (the caller plus dedicated
        // std::threads, joined before return); each chunk keeps the protocol above, MarkFolded replacing
        // AdvanceFrontier, so F is the contiguous watermark and writers also update rows of folded groups above it.
        // workers == 1 is the sequential builder. Stats are summed over the workers (lock_hold_us: worker by worker).
        LiveWidenResult LiveWidenOneYear(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                         uint32_t tracked_column_count, bool widen_zmap, bool widen_catidx,
                                         const std::atomic<bool> *stop, LiveWidenStats *stats = nullptr,
                                         unsigned workers = XStoreTunables::LIVE_WIDEN_WORKERS,
                                         uint64_t group_bytes = XStoreTunables::LIVE_WIDEN_GROUP_BYTES);

        // Live widening trigger, M4 stage 3 (Tech Reference 4.18 "Live widening, stage 3"). MaybeGrowColumnCount's
        // rare path records a DB whose effective tracked width rose; the background index thread
        // (StartCatIdxCompactionThread) calls RunLiveWidenPass once per LIVE_WIDEN_TICK_SEC with the current time.
        // A pass takes every recorded DB whose last growth is at least LIVE_WIDEN_QUIET_PERIOD_SEC before `now` (and
        // whose retry wait, if any, has passed), and widens its year files one at a time, NEWEST year first, to the
        // DB's exact effective width: the zone map when its width differs, the catidx when it is narrower (widths
        // only: a file is normally dirty at runtime). A year whose index is already in rebuild state is skipped (one
        // log line per year file per process). Tests call it directly with a fake `now`. Returns one entry per year
        // looked at, in processing order.
        struct LiveWidenPassEntry {
            std::string db_name;
            unsigned short year = 0;
            LiveWidenResult result = LiveWidenResult::kNothingToDo; // kNothingToDo also when both widths were right
            bool skipped_rebuilding = false;                        // left alone: already in rebuild state
        };
        std::vector<LiveWidenPassEntry> RunLiveWidenPass(std::chrono::steady_clock::time_point now,
                                                         unsigned workers = XStoreTunables::LIVE_WIDEN_WORKERS);

        // Startup index rebuild engine (Tech Reference 4.14): rebuilds the selected index files of every listed year of
        // ONE DB directory. Serially, year by year in list order, it begins each year (registry erase, pins,
        // BeginRebuild, data file open); then `thread_count` threads (the caller plus thread_count - 1 dedicated
        // std::threads, never more than there are groups) fold a flat, group-major round-robin list (group 0 of every year, then group 1 of every year, ...) of groups of consecutive
        // REBUILD_CHUNK_BYTES chunks (`group_bytes` rounded up to whole chunks); then, serially again, each year
        // finishes (FinishRebuild, data fdatasync, MarkClean, log lines). thread_count <= 1 runs everything on the
        // calling thread. Each file's frontier only ever reaches the end of the CONTIGUOUS prefix of rows it has
        // folded, so a group that finishes early never makes a slower, lower group's rows count as folded. Returns one
        // entry per task: the exception that year's rebuild threw (nullptr if none); a throw never stops another year.
        // While the threads run, one reporter thread shows progress (a bar on a terminal, spdlog lines elsewhere; see
        // XStore_RebuildProgress.h): it only reads the years' rows_done counters and is joined before the years finish.
        // Startup/tests only, same rules as RebuildYearIndexes. Public so tests can choose threads and group size.
        struct RebuildYearTask {
            unsigned short year = 0;
            bool zmap = false;
            bool catidx = false;
        };
        [[nodiscard]] std::vector<std::exception_ptr> RebuildYearsIndexes(
            const std::string &db_base_path, const std::vector<RebuildYearTask> &tasks, uint16_t row_length,
            uint32_t tracked_column_count, unsigned thread_count,
            uint64_t group_bytes = XStoreTunables::REBUILD_PARALLEL_GROUP_BYTES,
            const rebuildprogress::Options &progress = rebuildprogress::Options{});

        // Stops the background index thread's current work (the live builder and compaction check it): set by
        // StopCatIdxCompactionThread for good, and by RemoveDB / RemoveDBFiles for the time they wait for the thread
        // to let go of the DB/year they remove. Tests may set it to stop a running pass (and must reset it).
        static std::atomic<bool> bg_cancel_;

        // Test-only views of the trigger state.
        static size_t LiveWidenRequestCountForTests();
        static std::optional<std::chrono::steady_clock::time_point> LiveWidenLastGrowthForTests(const std::string &db_base_path);
        static void ClearLiveWidenRequestsForTests();
        static std::string BackgroundActiveKeyForTests(); // "<db_base_path>/<year>" the thread works on, or ""

        // Ring-format (record v2: ParseWalRecord's layout plus a u64 LSN after
        // the CRC; see XStore_WalRing.h) parser. kOk = real record (out_record
        // filled), kPad = PAD filler to skip (consumed set, no rows), kEnd =
        // torn/CRC-bad/LSN-mismatch (a stale lap's bytes carry a different
        // LSN) -- stop replay, consumed untouched.
        enum class RingParse { kOk, kPad, kEnd };
        // `corrupt` (optional): set for a kEnd caused by a record that is damaged
        // (impossible length, CRC failure, structure mismatch) and cleared for the
        // clean ends (a CRC-valid record whose LSN is not the expected one). Replay
        // zeroes the ring past a damaged record, never past a clean end.
        static RingParse ParseWalRecordV2(const char *data, size_t len, uint64_t expected_lsn, size_t &consumed, WalRecord &out_record,
                                          bool *corrupt = nullptr);

    private:
        // Get current base directory
        std::string GetBaseDir();

        // Thread-local memo of dbName -> DbBasePath(dbName). DbBasePath is
        // pure (data_store_dir + dbName, no I/O) but JoinPath rebuilds it via a
        // fresh heap allocation on every call -- and every single op
        // function (Unary/Batch/Range Insert/Query/Delete, all four
        // aggregates) calls it unconditionally, before even checking
        // whether the cfg cache hit. A hashmap (not a single-entry memo)
        // because a worker thread can flip between a small set of DBs
        // across consecutive requests, not just stay on one.
        const std::string& CachedDbBasePath(const std::string &db_name) const;

        // Thread-local memo of (dbBasePath, year) -> "<dbBasePath>/<year>.XSTORE".
        // Same reasoning as CachedDbBasePath, for the per-(db,year) file
        // path every Unary/Batch/Range op (and WalkYearSpan, for the
        // aggregates) builds via fmt::format on every call (or every
        // year-group/segment, for Batch/Range). A hashmap rather than a
        // single-entry memo specifically because RND-pattern workloads (see
        // UnaryInsertRandomThroughput in XStorePerfTest.cpp) touch a handful
        // of distinct years in no particular order -- a single-entry memo
        // would miss on nearly every call there, while a real dataset's year
        // range is small enough that a full hashmap stays warm regardless of
        // access order. Keyed by dbBasePath (not dbName) since that's
        // already 1:1 with the db and every caller already has it in hand --
        // callers that only know dbBasePath (e.g. WalkYearSpan, shared by
        // all four aggregates) don't need to separately thread dbName
        // through just for this.
        static const std::string& CachedYearFilePath(const std::string &db_base_path, unsigned short year);

        // Base directory for dbName's data/config files under data_store_dir,
        // e.g. "<data_store_dir>/<dbName>" -- no trailing '/' (see convention
        // note above base_dir). Assumes dbName is already upper-cased (call
        // ToUpperCase first, as every public method does).
        std::string DbBasePath(const std::string &db_name) const;

        // Zone-map secondary index registry: one ZoneMapFile per (db, year)
        // pair ever touched, keyed by "<db_base_path>/<year>". Capped at
        // ZONE_MAP_REGISTRY_CAP (see its comment in XStore_Tunables.h for the
        // memory-footprint reasoning) with plain LRU eviction -- unlike the
        // mmap/fd caches above, eviction here is always safe and cheap:
        // every ZoneMapFile update is write-through (see XStore_ZoneMap.h),
        // so an evicted instance has lost nothing, and the next touch just
        // reopens its .zmap file and pays one fresh pread() for whatever
        // block it needs. A plain unordered_map behind one mutex, not
        // sharded -- CreateDB/first-insert-of-a-year frequency is nowhere
        // near hot-path enough to need striping the way the WAL/mmap caches
        // do. static -- see GetOrOpenZoneMap's comment for why (reachable
        // from ApplyWalRecord, which has no XStore instance).
        static std::mutex zone_map_registry_mutex_;
        static std::unordered_map<std::string, std::unique_ptr<ZoneMapFile>> zone_map_registry_;
        static std::vector<std::string> zone_map_registry_lru_; // most-recently-touched at the back

        // Categorical bloom-index registry: one CatIdxFile per (db, year)
        // pair, keyed identically to zone_map_registry_ above
        // ("<db_base_path>/<year>"). Deliberately separate mutex/map/LRU
        // vector -- shares zero state with the zone-map registry -- so a
        // .catidx-only workload's LRU pressure never evicts a warm .zmap
        // instance (or vice versa).
        static std::mutex catidx_registry_mutex_;
        static std::unordered_map<std::string, std::unique_ptr<CatIdxFile>> catidx_registry_;
        static std::vector<std::string> catidx_registry_lru_; // most-recently-touched at the back

        // Entries RetireIndexesUnderPrefix took out of the registries while pinned; guarded by the registry mutex.
        static std::vector<std::unique_ptr<ZoneMapFile>> zone_map_registry_retired_;
        static std::vector<std::unique_ptr<CatIdxFile>> catidx_registry_retired_;

        // Frees the retired entries nobody pins any more. Caller holds that registry's mutex.
        template <typename File>
        static void SweepRetired(std::vector<std::unique_ptr<File>> &retired) {
            retired.erase(std::remove_if(retired.begin(), retired.end(),
                                         [](const std::unique_ptr<File> &f) { return !f->Pinned(); }),
                          retired.end());
        }

        // Retires every entry of one registry whose key `match`es. Caller holds that registry's mutex.
        template <typename File, typename Match>
        static void RetireMatching(std::unordered_map<std::string, std::unique_ptr<File>> &registry,
                                   std::vector<std::string> &lru, std::vector<std::unique_ptr<File>> &retired,
                                   const Match &match) {
            SweepRetired(retired);
            for (auto it = registry.begin(); it != registry.end();) {
                if (!match(it->first)) { ++it; continue; }
                if (it->second->Pinned()) retired.push_back(std::move(it->second));
                it = registry.erase(it);
            }
            lru.erase(std::remove_if(lru.begin(), lru.end(), match), lru.end());
        }

        // Both registries, each under its own mutex (never nested).
        template <typename Match>
        static void RetireIndexesMatching(const Match &match);

        // ---- Live widening trigger (M4 stage 3) and the background index thread handshake ----
        // One recorded DB, keyed by its db_base_path (the CachedDbBasePath spelling: it is the writers' lock key
        // prefix). growth_seq counts growths, so a pass can tell whether one arrived while it ran.
        struct LiveWidenRequest {
            std::string db_name;
            std::chrono::steady_clock::time_point last_growth{};
            std::chrono::steady_clock::time_point retry_after{};
            uint64_t growth_seq = 0;
            uint32_t failed_attempts = 0;
        };
        // Leaf lock: taken briefly (MaybeGrowColumnCount holds a cfg shard mutex then); nothing is called while it is
        // held. Also guards live_widen_skip_logged_.
        static std::mutex live_widen_mutex_;
        static std::map<std::string, LiveWidenRequest> live_widen_requests_;
        static std::set<std::string> live_widen_skip_logged_; // year keys whose "already rebuilding" skip was logged
        static std::atomic<bool> live_widen_pending_;         // live_widen_requests_ is not empty
        static void RecordLiveWidenRequest(const std::string &db_name, const std::string &db_base_path);
        static void DropLiveWidenRequest(const std::string &db_base_path);

        enum class LiveWidenDbOutcome { kDone, kRetry, kStopped, kGone };
        LiveWidenDbOutcome LiveWidenDb(const std::string &db_base_path, const std::string &db_name,
                                       std::vector<LiveWidenPassEntry> &out, unsigned workers);

        // Width of a year's .zmap / .catidx: the warm registry object's when there is one, else the file header's;
        // nullopt for a missing or unreadable file. `rebuilding`: a warm object of the year is in rebuild state.
        struct YearIndexWidths {
            std::optional<uint32_t> zmap;
            std::optional<uint32_t> catidx;
            bool rebuilding = false;
        };
        static YearIndexWidths PeekYearIndexWidths(const std::string &db_base_path, unsigned short year);

        // Background index thread vs RemoveDB / RemoveDBFiles. The thread publishes the year key it works on
        // ("<db_base_path>/<year>", the registry key format) in bg_active_key_ before it starts a year (widen or
        // compaction) and clears it after; it refuses to start a year that a removal scope covers, or any year while
        // bg_cancel_ is set. A removal adds its scope ("<db dir>/" for RemoveDB, "<db dir>/<year>" for RemoveDBFiles),
        // sets bg_cancel_ if the active key is covered, and waits (bounded) until it is not. Lock order: bg_mutex_ is a
        // leaf (nothing else is taken while it is held).
        static std::mutex bg_mutex_;
        static std::condition_variable bg_cv_;
        static std::string bg_active_key_;
        static std::multiset<std::string> bg_removing_scopes_;
        enum class BgEnter { kGo, kRemoving, kCancelled };
        static BgEnter EnterBackgroundYear(const std::string &year_key);
        static void LeaveBackgroundYear();
        [[nodiscard]] static bool StopBackgroundWork(const std::string &scope); // true: it set bg_cancel_
        static void ResumeBackgroundWork(const std::string &scope, bool cancelled);

        // Bodies of GetOrOpenZoneMap/GetOrOpenCatIdx and the Pin* calls; the caller holds the registry mutex.
        static ZoneMapFile& GetOrOpenZoneMapLocked(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                                   uint32_t tracked_column_count);
        static CatIdxFile& GetOrOpenCatIdxLocked(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                                 uint32_t tracked_column_count);

        // Turns one row's proto fields into the zone map's per-column
        // observations -- shared by Unary/Batch/RangeInsert (all three
        // build a WalRow, see XStore_Internal.h for the definition and full
        // rationale). Takes an XStore& purely to reach the private
        // GetFDataValue overload below; touches no instance state of its
        // own.
        static std::vector<ZoneMapObservation> BuildZoneMapObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count);
        // One column's observation as BuildZoneMapObservations builds it; shared with the rebuild's one-pass decoder.
        static ZoneMapObservation ZoneMapObservationOf(XStore &store, const XStore_Proto::FData &f_data, uint32_t column_idx);

        // Same purpose as BuildZoneMapObservations above but for the CatIdx
        // bloom filter -- covers ALL FData oneof cases (no numeric-only
        // gate), since bloom membership works on the encoded byte string
        // regardless of type. Shared by Unary/Batch/RangeInsert, applied by
        // ApplyWalRecord only after the corresponding row's pwrite succeeds
        // (see WalRow::catidx_observations in XStore.h and XStore_Internal.h
        // for the implementation).
        static std::vector<CatIdxObservation> BuildCatIdxObservations(XStore &store, const XStore_Proto::RowData &row, uint32_t tracked_column_count);

        // Clamps a ConfigStruct's columnCount to MAX_TRACKED_COLUMN_COUNT,
        // logging once (spdlog::warn, same shape as CreateDB's own rejection
        // of the identical bound) if it actually had to. Applied to every
        // ConfigStruct that comes back OFF DISK -- CreateDB rejects an
        // out-of-bound seed and MaybeGrowColumnCount clamps growth, but both
        // of those only guard the WRITE side: a .CFG written by an older
        // binary, a restored backup, or a hand-edited/corrupt file still
        // feeds columnCount straight into EffectiveTrackedColumnCount ->
        // RebuildOneYearZoneMap, where BuildZoneMapObservations' reserve()
        // and ZoneMapFile::EnsureBlockLoaded's std::vector<ZoneMapEntry>(n)
        // are both sized from it and an absurd n throws std::length_error.
        // That throw used to escape on an XStorePreadPool worker as a
        // std::terminate (RebuildStaleIndexesForDb now catches and logs it,
        // leaving that year stale) -- and the whole chain hangs off InitDB(),
        // which runs from the XStore constructor, i.e. at ZmqServer worker
        // CONSTRUCTION, outside the dispatch-loop try/catch. Clamped rather than rejected: this is data that
        // already exists on disk, there is no request to fail here, and a
        // clamped width only costs zone-map acceleration for columns past
        // the cap (they fall back to a real scan), never correctness.
        static void ClampPersistedColumnCount(const std::string &db_name, XStore_Proto::ConfigStruct &cfg);

        // Fetch dbName's ConfigStruct from CfgMap, loading it from dbBasePath's
        // .CFG file into the cache on first use. Returns false if the .CFG file
        // does not exist. Thread-safe: shared lock on cache hit, exclusive lock
        // (with re-check) only on cache miss. A freshly-parsed .CFG passes
        // through ClampPersistedColumnCount before it is cached or returned.
        bool GetOrLoadCfg(const std::string &db_name, const std::string &db_base_path, XStore_Proto::ConfigStruct &cfg_out);

        // Insert/overwrite dbName's ConfigStruct in CfgMap under exclusive
        // lock, after ClampPersistedColumnCount (the second chokepoint --
        // the cache must never hold an out-of-bound columnCount regardless
        // of which path populated it).
        void PutCfg(const std::string &db_name, const XStore_Proto::ConfigStruct &cfg);

        // Remove dbName's ConfigStruct from CfgMap under exclusive lock.
        void EraseCfg(const std::string &db_name);

        // Fetch targetFile's cached read-only mapping, creating (and
        // ftruncate'ing to totalRows*rowLength if new) the file and mapping it
        // on first use. Returns nullptr on open/mmap failure. The returned
        // shared_ptr<void>'s .get() is the mapping's base address (an alias
        // into the cache's MappedFile control block, see MmapShard above) --
        // callers must keep it alive for as long as they touch the mapping,
        // since that's what defers the eventual munmap() past a concurrent
        // eviction.
        std::shared_ptr<void> GetOrMapFile(const std::string &target_file, size_t row_length, unsigned long long total_rows);

        // munmap + drop targetFile's cached mapping, if present. Used before a
        // file is deleted out from under the cache (RemoveDBFiles).
        void EraseMappedFile(const std::string &target_file);

        // munmap + drop every cached mapping whose path falls under dbDirPrefix.
        // Used before a DB directory is deleted out from under the cache
        // (RemoveDB), since a single RemoveDB can span many cached year files.
        void EraseMappedFilesUnderPrefix(const std::string &db_dir_prefix);

        // Starts every shard's dedicated writer thread. Called exactly once
        // per process via a std::call_once in XStore::XStore() -- static
        // because wal_shards is process-wide static state, not tied to any
        // one XStore instance's lifetime (instances are constructed one per
        // worker thread; the WAL writer pool must outlive all of them).
        // Takes the first instance's data_store_dir (production: every
        // instance shares one, see the constructor) to eagerly open and
        // fallocate each active shard's WAL file up front, rather than
        // lazily on that shard's first write.
        //
        // Also computes active_wal_shard_count from hardware_concurrency(),
        // clamped to [WAL_MIN_ACTIVE_SHARDS, WAL_SHARD_COUNT] -- only that
        // many of the WAL_SHARD_COUNT array slots get a writer thread /
        // ever get routed traffic; ReplayWal still scans the full
        // WAL_SHARD_COUNT range regardless, so a prior run with a higher
        // active count is still fully recovered.
        static void StartWalWriters(const std::string &data_store_dir);

        // Routes recordToEnqueue by (db_base_path, year) onto one of the
        // active WAL shards (see WalShardFor; tried round-robin first --
        // spreads one hot DB across every shard, but on a single physical
        // device that fragments what could be one shard's large,
        // well-amortized batch into many smaller ones, each paying its own
        // fsync; routing by (db, year) keeps one (db, year)'s writes
        // funneled through one shard so the group-commit gather window can
        // actually batch them). Pushes it onto that
        // shard's queue, and -- unless durability_mode is ASYNC --
        // blocks until that shard's writer thread has fsync'd and applied
        // it, returning whether it was actually durable (false on a WAL
        // write/fsync failure or a failed apply, not just on the queue push
        // itself failing). ASYNC returns true immediately after a
        // successful enqueue -- no durability signal is available yet by
        // design.
        //
        // Before enqueueing, blocks the calling thread (polling, not
        // rejecting) if the target shard's queue is already at
        // WAL_QUEUE_HIGH_WATERMARK -- applies to every durability mode,
        // including ASYNC, so a writer that can't keep up bounds
        // memory growth and the crash-loss window instead of accepting
        // unboundedly while still reporting "success".
        // `overloaded` (optional) is set true, never cleared, when a SYNC record failed because the WAL ring
        // stayed full (kWalOverloadedText for clients); ASYNC records have no completion, so never report it.
        static bool EnqueueWalRecord(WalRecord record, bool *overloaded = nullptr);

        // Parses the on-disk WAL wire format (see XStore_Wal.cpp's
        // WalWriterLoop pwritev path and ParseWalRecord definition for the
        // exact byte layout and CRC framing). ParseWalRecord is an
        // XStore:: member so ReplayWal's crash-recovery path (a different
        // translation unit) uses this exact implementation instead of an
        // independently-maintained copy.
        static bool ParseWalRecord(const char *data, size_t len, size_t &consumed, WalRecord &out_record);

        // Writes every row in recordToApply.rows to its actual .XSTORE data
        // file, at the offset each row's epoch_time (combined with
        // recordToApply's time_resolution/row_length) computes to -- the
        // same logic UnaryInsert used to run inline, now shared between the
        // writer thread's normal path and ReplayWal's crash-recovery path
        // (both call this with a fully self-contained WalRecord, no cfg
        // lookup needed). Returns true only if every row was durably
        // written -- a caller that discards this return value silently
        // loses the failure signal. Coalesces same-year contiguous runs of
        // rows into one pwrite() per run rather than one per row (see the
        // run-detection loop in the .cpp) -- BatchInsert/RangeInsert can
        // hand this a record with hundreds of thousands of rows, so this
        // isn't optional the way it might have been for UnaryInsert's
        // always-1-row records.
        // `ring`/`ring_lsn_end`: the WAL ring record carrying this write (its END lsn),
        // forwarded to walring::Dirty(); the defaults (BYPASS, replay, the WAL
        // writer's direct path) mean a direct write with no ring record.
        [[nodiscard]] static bool ApplyWalRecord(const WalRecord &record_to_apply,
                                                  const std::shared_ptr<walring::Ring> &ring = nullptr,
                                                  uint64_t ring_lsn_end = 0);

        // Body of one shard's dedicated writer thread: waits (bounded by
        // WAL_GROUP_COMMIT_WINDOW_US) for the first record, then actively
        // lingers up to an adaptive window between WAL_GATHER_WINDOW_MIN_US
        // and WAL_GATHER_WINDOW_MAX_US (polling every WAL_GATHER_POLL_US)
        // for more to arrive on the same shard --
        // stopping the moment the batch fills or the window elapses,
        // whichever first -- so genuinely concurrent commits share one
        // fsync instead of each paying for their own. Appends the batch to
        // this shard's WAL ring file (LSN-stamped records, see
        // XStore_WalRing.h), in byte-bounded sub-batches (<= ring_bytes/4
        // per ring each) of: reserve ring space -> pwritev -> fdatasync ->
        // apply every record -> PublishApplied -> release every synchronous
        // caller's completion. A full ring stalls, then fails the sub-batch
        // ("overloaded"), instead of overwriting unflushed records. Large
        // multi-row records (walring::ChooseRoute) on a file with no
        // un-checkpointed ring records take the direct path: apply, then
        // fdatasync the data file(s) + fsync the directory, no ring record.
        //
        // preopened_ring: the ring StartWalWriters already opened for this
        // shard (production, one known data_store_dir); seeds this thread's
        // per-path ring cache. nullptr = open lazily on first use via
        // walring::GetOrOpenRing (shards beyond the constructor's
        // data_store_dir in multi-directory test scenarios, or a legacy WAL
        // file that was still awaiting replay at startup).
        static void WalWriterLoop(size_t shard_idx, std::shared_ptr<walring::Ring> preopened_ring);

#ifdef XSTORE_WAL_STATS_ENABLED
        // Debug/benchmark-only (XSTORE_WAL_STATS): periodically logs each
        // active shard's average batch size (records/batch) since the last
        // report, using WalShard's total_batches_processed/
        // total_records_processed counters. Started/joined the same way as
        // the writer threads themselves -- see StartWalWriters.
        static void WalStatsReporterLoop();
        static std::atomic<bool> wal_stats_stop_flag;
        static std::thread wal_stats_reporter_thread;
#endif

        // "(db, year) pairs WAL replay actually touched during one
        // InitDB() call", keyed by already-upper-cased dbName.
        //
        // Why this has to exist at all: ParseWalRecord rebuilds a WalRow
        // from the WAL's own bytes, and the WAL format deliberately does
        // NOT carry a row's ZoneMapObservations (they are derived state --
        // see WalRow's comment), so every replayed row reaches
        // ApplyWalRecord with an EMPTY observation vector and its
        // zmap.UpdateRow call is a no-op. Replay therefore puts rows back
        // into the data file while folding nothing into the zone map.
        //
        // That gap is older than the zone map's per-DB tracked-column work,
        // but it used to be masked: before EffectiveTrackedColumnCount
        // floored at the global default, essentially every unseeded DB's
        // header width disagreed with its effective width, so
        // ZoneMapNeedsRebuild fired on nearly every restart and the
        // resulting full raw-data rescan repaired the missing rows as a side
        // effect. With the width now monotone, header == effective width in
        // the common case and that accidental repair is gone -- so a
        // replayed (db, year) can be left permanently short, which reads
        // back as valid_count == 0 over real data: MIN/MAX skip the block
        // ("ERROR: No valid data found") and SUM/AVG/VARIANCE/STDDEV push
        // down zeros. Nothing ever heals it.
        //
        // Fix: force RebuildOneYearZoneMap for exactly these pairs. That
        // function truncates the sidecar and re-derives every block from
        // raw data, so it is idempotent and safe to run on a (db, year)
        // that was already correct -- which is why this is a forced
        // *rebuild* rather than an attempt to re-fold the replayed rows
        // inline. Re-folding inline would double-count any record that had
        // already been applied (and folded) before the crash, since
        // UpdateRow accumulates rather than replaces.
        using ReplayedYearMap = std::unordered_map<std::string, std::set<unsigned short>>;

        // Called once from InitDB, before the server starts taking traffic
        // (InitDB must precede all inserts to this data_store_dir): replays
        // every shard WAL file (WAL_SHARD_COUNT of them, regardless of how
        // many were active as writers in this run -- see
        // active_wal_shard_count). A ring-format file (valid header) replays
        // [checkpoint_lsn, end of log) using its own ring size; only after
        // every touched data file (and its directory) is fdatasync'd does the
        // checkpoint move to the log end (the invariant: a record leaves the
        // log only once its data is durable). A pre-ring ("legacy") file is
        // replayed once and, if its data synced, recreated as a ring.
        // Replay is idempotent (direct-offset addressing: re-applying a
        // record rewrites identical bytes), so a failed replay leaves the
        // checkpoint/legacy file untouched and the next start replays it again.
        // Most failures also FENCE the file (walring::FenceWalFile:
        // GetOrOpenRing refuses it, so inserts to that shard fail loudly until
        // the next InitDB/start, which unfences every shard file first):
        //  ring: a pread error, a data-file/directory sync failure (or an
        //    already poisoned checkpointer), a record that cannot be applied
        //    although its DB is not positively gone, a failed zeroing of the
        //    ring past a damaged record, no matching ring, a failed checkpoint
        //    write;
        //  both: a stat/open failure of the shard file;
        //  legacy: a read error (badbit, not EOF), an apply failure, a sync
        //    failure, an unparseable non-blank file (maybe a ring with both
        //    header slots damaged).
        // NOT fenced: success; a ring scan that ends where it started without
        // damage (nothing to do); a legacy conversion failure AFTER the data
        // sync succeeded (the data is durable, the writer may re-init the
        // file). A record whose DB is positively gone (ENOENT/ENOTDIR/
        // not_found) is skipped, not a failure. A torn record of this lap ends
        // the scan and the ring past it is zeroed (length field last), so
        // unacknowledged leftovers cannot come back later.
        //
        // Returns the set of years it actually applied a record into, per
        // (already upper-cased) dbName -- keyed by dbName rather than
        // db_base_path so it can't be defeated by two spellings of the same
        // directory. InitDB() feeds this straight into
        // RebuildStaleIndexesForDb as a FORCED rebuild list; see
        // ReplayedYearMap above for why that is a correctness requirement
        // and not an optimization.
        [[nodiscard]] ReplayedYearMap ReplayWal();
        // ReplayWal's two per-shard-file bodies (see XStore_Management.cpp).
        // Ring: replay [checkpoint_lsn, end) with the file's OWN ring_bytes, fdatasync
        // the touched data files, and only then persist checkpoint_lsn = end.
        // Legacy: the pre-ring parse, then (only if the touched data files synced) the
        // file is recreated as a ring.
        void ReplayRingShard(const std::string &wal_path, size_t shard_idx, int fd, uint64_t ring_bytes,
                             uint64_t checkpoint_lsn, ReplayedYearMap &replayed_years);
        void ReplayLegacyShard(const std::string &wal_path, size_t shard_idx, ReplayedYearMap &replayed_years);

        // Called once per DB from InitDB() (after WAL replay and after the DB's ConfigStruct is loaded), before any
        // worker thread sharing this data_store_dir can serve a request -- see design doc "Startup-time zone-map
        // rebuild" for why this must never run on the request path. Finds every <YEAR>.XSTORE (the data files, not the
        // sidecars: a year with rows but no sidecar must be found), decides per year which index is stale with the two
        // unchanged predicates (ZoneMapNeedsRebuild for the .zmap; CatIdxNeedsRebuild for the .catidx), and rebuilds
        // all those years with ONE RebuildYearsIndexes call on hardware_concurrency threads (one scan per year, split
        // into groups that different threads fold). Not XStorePreadPool: the server reserves its request threads out of
        // that pool, and nothing else runs before InitDB returns.
        //
        // force_rebuild_years is this DB's entry from ReplayWal's ReplayedYearMap: both indexes of those years are
        // rebuilt UNCONDITIONALLY, whatever the header checks say. See ReplayedYearMap for the bug that requires this.
        void RebuildStaleIndexesForDb(const std::string &db_name, const std::string &db_base_path,
                                      const XStore_Proto::ConfigStruct &cfg_struct,
                                      const std::set<unsigned short> &force_rebuild_years);

        // Startup rebuild of one year (Tech Reference 4.19): pins the selected index files (need_zmap / need_catidx; a
        // healthy one is never opened, so a compacted catidx keeps its bitmap section), BeginRebuild()s them at
        // tracked_column_count (dirty on disk before the truncate; a wider or narrower old file is emptied either way),
        // reads total_rows once, scans the year file ONCE in REBUILD_CHUNK_BYTES chunks of whole blocks through
        // FoldChunk, advancing each file's frontier after every chunk. A complete build: FinishRebuild, fdatasync of the
        // data file, MarkClean. Any failure (unreadable row, failed index write, ...) leaves that file dirty and still
        // in rebuild, so blocks it never finished are scanned, not trusted, and the next start rebuilds it. Drops the
        // registry entries it rebuilds first and ignores pins: InitDB/tests only, never while a builder is alive.
        // A one-year, one-thread RebuildYearsIndexes (everything on the calling thread); rethrows that year's exception.
        void RebuildYearIndexes(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                uint32_t tracked_column_count, bool need_zmap, bool need_catidx);

        // Reused across the chunks of one year's rebuild: no allocation per row once warm.
        struct RebuildScratch {
            std::vector<char> scan_buffer;
            ZoneMapFoldBatch zmap;
            CatIdxFoldBatch catidx;
            XStore_Proto::FData column;
            XStore_Proto::RowData row; // only for a row the one-pass decoder declines (see AppendRebuildObservations)
        };
        struct FoldResult {
            bool scan_complete = true;
            bool zmap_written = true;
            bool catidx_written = true;
        };
        // Reads rows [chunk_begin, chunk_end) of an open year file (hole-skipping) and folds them into zmap and/or
        // catidx (nullptr = skip that index) through RebuildFoldRows. If a row cannot be read nothing is folded and
        // scan_complete is false, so a retry of the chunk cannot fold a row twice. Takes no lock: at startup nothing
        // else runs; a live builder (stage 2b) must hold the data-file READ range lock (key: CachedYearFilePath) over
        // [chunk_begin, chunk_end) around this call and both AdvanceFrontier calls.
        FoldResult FoldChunk(int data_fd, uint64_t chunk_begin, uint64_t chunk_end, uint16_t row_length,
                             uint32_t tracked_column_count, ZoneMapFile *zmap, CatIdxFile *catidx, RebuildScratch &scratch);
        // Appends one row's zone-map observations and catidx values to the batches (either may be nullptr), parsing
        // the row once: one tag walk, each column decoded once. The result is the same as the per-row rebuild's
        // (RowData parse + BuildZoneMapObservations for the zone map; ExtractColumnByIndexSkipParse +
        // EncodeFDataForCatIdx per column for the catidx); a row whose bytes are not a plain run of well-formed
        // columns falls back to exactly those calls.
        void AppendRebuildObservations(uint64_t row_index, const char *row, uint16_t row_length,
                                       uint32_t tracked_column_count, ZoneMapFoldBatch *zmap, CatIdxFoldBatch *catidx,
                                       RebuildScratch &scratch);

        // One year of a RebuildYearsIndexes call (defined in XStore_Management.cpp): pins, data fd, group layout,
        // per-file contiguous watermark, shared failure state.
        struct YearRebuild;
        // Serial setup of one year; leaves it inactive (nothing to scan or finish) when there is nothing to do.
        void BeginYearRebuild(const std::string &db_base_path, uint16_t row_length, uint32_t tracked_column_count,
                              uint64_t group_bytes, YearRebuild &y);
        // Folds group g of y chunk by chunk; any thread, one scratch per thread.
        void RunRebuildGroup(YearRebuild &y, size_t g, uint16_t row_length, uint32_t tracked_column_count,
                             RebuildScratch &scratch);
        // Serial teardown of one year: FinishRebuild, data fdatasync, MarkClean, log lines.
        void FinishYearRebuild(const std::string &db_base_path, uint32_t tracked_column_count, YearRebuild &y);

        // Header-only staleness check of a .catidx: missing, unreadable, wrong magic/version, narrower than
        // desired_tracked_columns, or dirty (unclean shutdown). A wider file is kept.
        static bool CatIdxNeedsRebuild(const std::string &catidx_path, uint32_t desired_tracked_columns);

        // Cheap header-only peek at path's .zmap sidecar (no full
        // ZoneMapFile::Open, no registry involvement) -- true if the file
        // is missing, wrong version, or tracked for a different column
        // count than desired_tracked_columns, or whose rows_per_block is not RowsPerBlock(row_length).
        static bool ZoneMapNeedsRebuild(const std::string &zmap_path, uint32_t desired_tracked_columns, uint16_t row_length);

        // Task 4 (insert-path memory plan): a query's column can be inside
        // EffectiveTrackedColumnCount(cfg) (so the caller expects zone-map
        // acceleration) yet still exceed what THIS year's .zmap file was
        // actually built with, if that file predates a later columnCount
        // growth on the same DB (see MaybeGrowColumnCount) -- the result is
        // still correct (every zone-map-pushdown kernel falls back to a
        // real scan for an out-of-range column), but the caller has no way
        // to know it paid for a full scan instead of an indexed read.
        // Returns "" when column_idx is out of [0, EffectiveTrackedColumnCount)
        // or every year's .zmap in [start_epoch, end_epoch] already covers
        // column_idx; otherwise a one-line notice naming the first year
        // whose header falls short. Three tiers, cheapest first: (1) a
        // zero-I/O fast path for column_idx below ZONE_MAP_MAX_TRACKED_COLUMNS
        // (always indexed by construction, so no file access is needed at
        // all); (2) PeekOpenZoneMapWidth, a registry peek that answers from
        // an already-open ZoneMapFile with no I/O; (3) only if neither above
        // resolves it, the same header-only open+pread+magic/version check
        // as ZoneMapNeedsRebuild -- deliberately never calls
        // GetOrOpenZoneMap, which would create/widen the file as a side
        // effect of merely answering a query. FindByValue does NOT use this
        // (it is about .zmap width): it decides from the width of the
        // .catidx file its own scan consults, via UnindexedColumnNoticeText.
        static std::string UnindexedColumnNotice(std::string_view op, const std::string &db_name, const std::string &db_base_path,
                                                  const XStore_Proto::ConfigStruct &cfg, unsigned long long start_epoch,
                                                  unsigned long long end_epoch, int32_t column_idx);

        // The one-line full-scan notice text shared by UnindexedColumnNotice and FindByValue.
        static std::string UnindexedColumnNoticeText(std::string_view op, const std::string &db_name, int32_t column_idx,
                                                      unsigned short year, uint32_t index_width);
        // The same, for a column >= ZONE_MAP_MAX_TRACKED_COLUMNS of a year whose index is being rebuilt (Rebuilding()).
        // Columns below that are also scanned in the blocks the rebuild has not reached yet, with correct results and
        // no notice (they are indexed again as it passes).
        static std::string RebuildingColumnNoticeText(std::string_view op, const std::string &db_name, int32_t column_idx,
                                                       unsigned short year);

        // Rebuilds this year's .zmap from the year file: a thin wrapper over RebuildYearIndexes with only the zone map
        // selected (the .catidx is not touched). Startup/tests only.
        void RebuildOneYearZoneMap(const std::string &db_base_path, unsigned short year, uint16_t row_length, uint32_t tracked_column_count);


        // Fetch targetFile's cached fd, creating (and ftruncate'ing to
        // totalRows*rowLength if new) the file on first use. Returns -1 on
        // open/ftruncate failure. Callers must keep the returned shared_ptr
        // alive for as long as they use the fd (see FdShard above) -- that's
        // what defers the eventual close() past a concurrent eviction.
        static std::shared_ptr<int> GetOrOpenFile(const std::string &target_file, size_t row_length, unsigned long long total_rows);

        // Peek-only: true if targetFile already has a cached fd (no open,
        // no creation). A cache hit here is proof the file exists -- only
        // files opened via GetOrOpenFile are ever cached, and both removal
        // paths (EraseOpenFile/EraseOpenFilesUnderPrefix) drop the cache
        // entry in the same call that deletes the file -- so callers that
        // otherwise need a filesystem::exists() check purely to avoid
        // GetOrOpenFile's O_CREAT can skip that stat() on a cache hit.
        bool IsFdCached(const std::string &target_file);

        // close + drop targetFile's cached fd, if present.
        void EraseOpenFile(const std::string &target_file);

        // close + drop every cached fd whose path falls under dbDirPrefix.
        void EraseOpenFilesUnderPrefix(const std::string &db_dir_prefix);

        // Joins base and child with exactly one '/' between them, regardless
        // of whether base already happens to end with one -- the single
        // place path-join logic lives, so no directory-path member needs to
        // carry a trailing '/' of its own (see the convention note above
        // BaseDir).
        static std::string JoinPath(const std::string &base, const std::string &child) {
            if (!base.empty() && base.back() == '/') {
                return base + child;
            }
            return base + "/" + child;
        }

        void ToUpperCase(std::string::iterator begin, std::string::iterator end) {
            // Only mask the case bit off actual lowercase letters -- doing it
            // unconditionally corrupts every other byte in the 'a'-'z' bit
            // pattern's range (e.g. '0'-'9', '-') into unrelated control chars.
            for (auto it = begin; it != end; ++it) {
                if (*it >= 'a' && *it <= 'z') {
                    *it &= ~0x20;
                }
            }
        };

        void GetFDataValue(const XStore_Proto::FData *f_data_input, unsigned int column_idx, ValueStruct *value_struct);
        void GetFDataValue(const XStore_Proto::FData *f_data_input, ValueStruct *value_struct);
};

#endif //XSTORE_XSTORE_H