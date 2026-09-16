#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

// Everything below is needed by this header's own declarations (member
// types or method signatures). Anything only used inside an XStore_*.cpp
// method body -- filesystem, sys/mman.h, fcntl.h, algorithm, execution,
// TimeStruct, fmt, spdlog, etc. -- is included by that .cpp directly rather
// than leaking into every translation unit that includes this class.
#include <array>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <mutex>
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

// Length of msg header
#define TRACE_LEN 8
#define DEBUG_LEN 8
#define INFO_LEN 7
#define WARNING_LEN 10
#define ERROR_LEN 8
#define CRITICAL_LEN 11

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

        // Shared cache of PROT_WRITE mmap regions for .XSTORE data files, keyed
        // by full path, so repeat UnaryInserts to the same (dbName, year) file
        // skip open/ftruncate/mmap and become a single memcpy into an
        // already-resident page. File size is fixed at creation (ftruncate'd
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
        // Distinct row offsets never overlap in memory so concurrent
        // memcpy's to disjoint slots need no extra locking beyond this
        // (FileRangeLockGuard still serializes same-slot writes).
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
        // used by UnaryInsert's pwrite() path. Unlike GetOrMapFile, writing
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
        // One append-only WAL file per shard (hash(dbBasePath) %
        // WAL_SHARD_COUNT), each drained by its own dedicated, process-
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

        std::string permitted_time_resolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int db_count = 0;
        std::vector<std::string> db_names = { };

        // Initialization
        XStore();

        // PROCESSING DbStats/info query
        XStore_Proto::SysInfo_REP InfoQuery(std::string_view server_addr, int thread_count);

        // CREATING new DB instance with row length
        [[nodiscard]] XStore_Proto::CreateDB_REP CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length);

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

        // RANGE INSERT
        void RangeInsert(const XStore_Proto::RangeInsert_REQ *proto_from_client, XStore_Proto::RangeInsert_REP *return_data);

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
        template <typename Cmp, typename PruneCheck>
        void ScanSegmentWithZoneMapPruning(const std::string &db_base_path, unsigned short year, uint16_t row_length,
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
        void AccumulateSegmentWithZoneMapPushdown(const std::string &db_base_path, unsigned short year, uint16_t row_length,
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

        // Initialize DB
        void InitDB();

        // Fetch (creating if needed) the ZoneMapFile for (dbBasePath, year),
        // opening its "<dbBasePath>/<year>.zmap" sidecar on first touch and
        // reusing the same in-registry instance on every later call --
        // consumed by every zone-map-aware insert/aggregate path (Tasks
        // 4-7). row_length is only used on first creation of that (db,
        // year) pair's .zmap file; see ZoneMapFile::Open. Bounded at
        // ZONE_MAP_REGISTRY_CAP with LRU eviction -- see zone_map_registry_
        // below for why eviction here is always safe.
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
        static ZoneMapFile& GetOrOpenZoneMap(std::string_view db_base_path, unsigned short year, uint16_t row_length);

        // Test-only introspection -- not part of the public API surface any
        // real caller needs, same as this codebase's other test-only
        // accessors.
        static size_t ZoneMapRegistrySize() { return zone_map_registry_.size(); }

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

        // Turns one row's proto fields into the zone map's per-column
        // observations -- shared by Unary/Batch/RangeInsert (all three
        // build a WalRow, see XStore_Internal.h for the definition and full
        // rationale). Takes an XStore& purely to reach the private
        // GetFDataValue overload below; touches no instance state of its
        // own.
        static std::vector<ZoneMapObservation> BuildZoneMapObservations(XStore &store, const XStore_Proto::RowData &row);

        // Fetch dbName's ConfigStruct from CfgMap, loading it from dbBasePath's
        // .CFG file into the cache on first use. Returns false if the .CFG file
        // does not exist. Thread-safe: shared lock on cache hit, exclusive lock
        // (with re-check) only on cache miss.
        bool GetOrLoadCfg(const std::string &db_name, const std::string &db_base_path, XStore_Proto::ConfigStruct &cfg_out);

        // Insert/overwrite dbName's ConfigStruct in CfgMap under exclusive lock.
        void PutCfg(const std::string &db_name, const XStore_Proto::ConfigStruct &cfg);

        // Remove dbName's ConfigStruct from CfgMap under exclusive lock.
        void EraseCfg(const std::string &db_name);

        // Fetch targetFile's cached PROT_WRITE mapping, creating (and
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

        // Hashes recordToEnqueue.db_base_path onto one of the active WAL
        // shards (tried round-robin first -- spreads one hot DB across
        // every shard, but on a single physical device that fragments what
        // could be one shard's large, well-amortized batch into many
        // smaller ones, each paying its own fsync; per-DB hashing keeps one
        // DB's writes funneled through one shard so the group-commit
        // gather window can actually batch them). Pushes it onto that
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
        static bool EnqueueWalRecord(WalRecord record);

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
        [[nodiscard]] static bool ApplyWalRecord(const WalRecord &record_to_apply);

        // Checkpoint step for a preallocated WAL file, called only from
        // ReplayWal (not from every live commit -- see WalWriterLoop):
        // zeroes just the 4-byte record_len header at offset 0 and fsyncs
        // that (tiny, in-place, already-allocated) write -- never
        // ftruncate/resize itself (see ReclaimWalFileSize below for the
        // sibling step that does, called right after this one). record_len=0
        // fails ParseWalRecord's own length check immediately, so a zeroed
        // file replays as "nothing pending". Durability matters here: a DB
        // can be deleted and recreated (RemoveDB, or an external wipe)
        // between a record's apply and the next process restart -- if the
        // invalidation itself weren't fsync'd before a crash, the
        // still-valid-looking old bytes would resurface at the *next*
        // ReplayWal too and get reapplied against whatever now lives at
        // that path, not the DB they actually belonged to. Calling this
        // once per shard file per restart (regardless of what was or
        // wasn't found to replay) is enough to guarantee that -- a live
        // commit's own record doesn't additionally need it: the shard's
        // next real write naturally overwrites it (offset-0 overwrite,
        // same mechanism), and if no next write ever comes, the only cost
        // is one redundant, harmless, idempotent re-apply attempt at the
        // next restart, not a repeat-forever failure -- not worth a second
        // fsync on every single commit to avoid.
        static void InvalidateWalFileHeader(int fd, const std::string &wal_path);

        // Shrinks a shard WAL file back down to WAL_FILE_PREALLOC_BYTES via
        // ftruncate, called only from ReplayWal, right after this file's WAL
        // evidence has already been fully consulted -- ReplayWal's streaming
        // reader always attempts every record (see WAL_REPLAY_MAX_RECORD_
        // BYTES), stopping early only at a genuinely unparseable or
        // implausibly-huge one, same as any torn/corrupt record; either way
        // nothing valid is left unconsulted by the time this runs, so
        // nothing valid is lost by shrinking. No-op if on_disk_bytes is
        // already <= the default. WalWriterLoop
        // itself deliberately never ftruncates (see InvalidateWalFileHeader
        // above / WalWriterLoop's own comment) to avoid a second fsync on
        // every live commit -- but that means a file which ever grows past
        // default (a legitimately oversized BatchInsert/RangeInsert pass,
        // see WAL_FILE_PREALLOC_BYTES) stays that size forever, across every
        // future restart, with no other reclaim point. ReplayWal is the one
        // safe place to pay that ftruncate+fsync cost: startup-only
        // (regardless of the current known_size), an fsync() not
        // fdatasync() since shrinking changes file size same as the
        // oversized_write growth case, and safe from racing WalWriterLoop's
        // already-running writer threads (StartWalWriters starts them in
        // the XStore constructor, before InitDB/ReplayWal) only because
        // ReplayWal is guaranteed to finish before any client can reach
        // CreateDB/EnqueueWalRecord -- see ReplayWal's own comment -- so
        // this shard's writer thread is guaranteed idle (blocked on an
        // empty queue) for the whole window this runs in.
        static void ReclaimWalFileSize(int fd, const std::string &wal_path, uintmax_t on_disk_bytes);

        // Body of one shard's dedicated writer thread: waits (bounded by
        // WAL_GROUP_COMMIT_WINDOW_US) for the first record, then actively
        // lingers up to an adaptive window between WAL_GATHER_WINDOW_MIN_US
        // and WAL_GATHER_WINDOW_MAX_US (polling every WAL_GATHER_POLL_US)
        // for more to arrive on the same shard --
        // stopping the moment the batch fills or the window elapses,
        // whichever first -- so genuinely concurrent commits share one
        // fsync instead of each paying for their own. Fsyncs the batch to
        // WAL, applies every record, then releases every synchronous
        // caller's completion future. Never ftruncates the file -- it's
        // preallocated once (see StartWalWriters/OpenAndPreallocateWalFile)
        // and each batch's self-framed, CRC-checked bytes simply overwrite
        // from offset 0; no per-commit checkpoint either (see
        // InvalidateWalFileHeader for why that's safe to skip here).
        //
        // preopened_path/preopened_fd: when StartWalWriters already
        // opened+fallocated this shard's file up front (the production
        // case, one known data_store_dir), seeds this thread's local fd
        // cache with it so the very first batch skips the lazy-open path.
        // Empty path / fd < 0 means "no such shard file yet" (open lazily
        // on first use instead) -- true for shard indices beyond the
        // constructor's data_store_dir in multi-directory test scenarios.
        static void WalWriterLoop(size_t shard_idx, std::string preopened_path, int preopened_fd);

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

        // Called once from InitDB, before the server starts taking traffic:
        // replays every shard WAL file left over from an unclean shutdown
        // (WAL_SHARD_COUNT of them, regardless of how many were actually
        // active as writers in this run -- see active_wal_shard_count),
        // then checkpoints via InvalidateWalFileHeader (never ftruncate --
        // same reasoning as WalWriterLoop). Idempotent by construction
        // (XStore's direct-offset addressing means re-applying an already-
        // applied record is a harmless overwrite of identical bytes) so
        // this never needs to track which records were already durable in
        // the data file before the crash -- but the checkpoint still must
        // run and must be durable, since the DB a stale record targets can
        // be deleted and recreated by the time of a *later* restart, at
        // which point "idempotent re-apply" no longer holds (see
        // InvalidateWalFileHeader).
        void ReplayWal();

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