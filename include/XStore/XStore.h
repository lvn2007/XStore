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
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include "XStore.pb.h"
#include "include/DataStructs/ValueStruct.h"

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
        static constexpr size_t CFG_SHARD_COUNT = 16;
        static std::array<CfgShard, CFG_SHARD_COUNT> cfg_shards;

        // Shared cache of PROT_WRITE mmap regions for .XSTORE data files, keyed
        // by full path, so repeat UnaryInserts to the same (dbName, year) file
        // skip open/ftruncate/mmap and become a single memcpy into an
        // already-resident page. File size is fixed at creation (ftruncate'd
        // up front to the year's full row count), so one mapping covers the
        // whole file for its lifetime -- no remap needed. MRU-ordered list +
        // index map = O(1) lookup/promote/evict; bounded (MMAP_CACHE_CAP) to
        // avoid unbounded address-space/FD growth.
        //
        // Sharded across MMAP_SHARD_COUNT independent {list, map, mutex}
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
        struct MappedFile { void *base; size_t len; };
        struct MmapShard {
            std::list<std::pair<std::string, std::shared_ptr<MappedFile>>> lru;
            std::unordered_map<std::string, std::list<std::pair<std::string, std::shared_ptr<MappedFile>>>::iterator> index;
            std::mutex mutex;
        };
        static constexpr size_t MMAP_SHARD_COUNT = 64;
        static constexpr size_t MMAP_CACHE_CAP = 256;
        static constexpr size_t MMAP_SHARD_CAP = MMAP_CACHE_CAP / MMAP_SHARD_COUNT;
        static std::array<MmapShard, MMAP_SHARD_COUNT> mmap_shards;

        // Shared cache of open fds for .XSTORE data files, keyed by full path --
        // used by UnaryInsert's pwrite() path. Unlike GetOrMapFile, writing
        // through a plain fd never faults a page into the process's address
        // space: a pwrite() into a still-sparse region goes through the same
        // buffered write()/delayed-allocation path the file gets on every
        // access pattern (SEQ or RND), instead of a synchronous mmap
        // write-fault that must allocate a real block before returning. Same
        // MRU-bounded design as MmapLru, just storing an fd instead of a base
        // pointer.
        //
        // Sized differently than MMAP_CACHE_CAP on purpose: a miss here is just
        // an open() (microseconds, no page-fault-time block allocation), so
        // the cap doesn't need to be large to amortize an expensive miss --
        // it only needs to cover the handful of year files actively being
        // written at once. Kept small so total resident cost (~256B kernel
        // struct file + ~250B userspace list/map node per entry, no
        // per-fd userspace buffer -- pwrite() writes straight to the kernel
        // page cache) stays in the same ballpark as the single ~4KB buffer
        // glibc's fwrite would have allocated for one open stream, rather
        // than inheriting the address-space-driven 256 sizing MmapLru uses.
        //
        // Sharded the same way as the mmap cache (see MmapShard above), just
        // fewer shards -- FD_CACHE_CAP is already tiny (8 total) so 64 shards
        // would leave under one slot each. 4 shards keeps a usable 2-slot
        // cap per shard while still splitting the lock so concurrent
        // first-opens of different files don't all serialize on one mutex.
        // Expect a smaller win than the mmap case: an fd-open miss is
        // already cheap (no page-fault-time block allocation to hide behind
        // an unlocked window), so the contention this closes is narrower to
        // start with.
        // shared_ptr<int> entries for the same reason as MmapShard's
        // shared_ptr<MappedFile> above -- eviction/erase used to close()
        // unconditionally with no regard for a thread still mid-pwrite()/
        // pread() on that exact fd via an earlier GetOrOpenFile() return
        // value, risking either a clean EBADF or (worse, if the closed fd
        // number got reused by an unrelated concurrent open() before the
        // racing pwrite/pread ran) a silent write into the wrong file.
        // FD_CACHE_CAP's small size (8, process-wide) made this easy to hit
        // once more than a handful of year files were concurrently hot.
        struct FdShard {
            std::list<std::pair<std::string, std::shared_ptr<int>>> lru;
            std::unordered_map<std::string, std::list<std::pair<std::string, std::shared_ptr<int>>>::iterator> index;
            std::mutex mutex;
        };
        static constexpr size_t FD_SHARD_COUNT = 4;
        static constexpr size_t FD_CACHE_CAP = 8;
        static constexpr size_t FD_SHARD_CAP = FD_CACHE_CAP / FD_SHARD_COUNT;
        static std::array<FdShard, FD_SHARD_COUNT> fd_shards;
        std::string permitted_time_resolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int db_count = 0;
        std::vector<std::string> db_names = { };
        std::map<std::string, unsigned long long int> resolution_map { {"SECOND", 31'536'000}, {"MILLISECOND", 31'536'000'000}, {"MICROSECOND", 31'536'000'000'000}, {"NANOSECOND", 31'536'000'000'000'000} };

        // Initialization
        XStore();

        // PROCESSING DbStats/info query
        XStore_Proto::SysInfo_REP InfoQuery(std::string_view server_addr, int thread_count);

        // CREATING new DB instance with row length
        XStore_Proto::CreateDB_REP CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length);

        // REMOVE DB -- Removing an entire DB and its data files
        XStore_Proto::RemoveDB_REP RemoveDB(std::string_view db_name);

        // REMOVE DB FILES -- Removing file(s) based on name in a certain DB
        XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *proto_from_client, std::string_view db_name);

        // UNARY INSERT
        void UnaryInsert(unsigned long long int epoch_time, const XStore_Proto::RowData &data, std::string_view db_name, XStore_Proto::UnaryInsert_REP *return_data);

        // UNARY QUERY
        void UnaryQuery(unsigned long long int epoch_time, std::string_view db_name, XStore_Proto::UnaryQuery_REP *return_data);

        // UNARY DELETE
        XStore_Proto::UnaryDelete_REP UnaryDelete(unsigned long long int epoch_time, std::string_view db_name);

        // BATCH INSERT
        void BatchInsert(XStore_Proto::BatchInsert_REQ *proto_from_client, XStore_Proto::BatchInsert_REP *return_data);

        // BATCH QUERY
        void BatchQuery(XStore_Proto::BatchQuery_REQ *proto_from_client, XStore_Proto::BatchQuery_REP *return_data, bool sort_time=true);

        // BATCH DELETE
        XStore_Proto::BatchDelete_REP BatchDelete(XStore_Proto::BatchDelete_REQ *proto_from_client);

        // RANGE INSERT
        void RangeInsert(const XStore_Proto::RangeInsert_REQ *proto_from_client, XStore_Proto::RangeInsert_REP *return_data);

        // RANGE QUERY
        void RangeQuery(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, XStore_Proto::RangeQuery_REP *return_data);

        // RANGE DELETE
        XStore_Proto::RangeDelete_REP RangeDelete(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name);

        // Walks [startEpochT, endEpochT] one year-file at a time for
        // Min/Max/Sum/AvgAggregate, read-locking each existing segment and
        // handing kernel(rowsBase, rowsToProcess) a pointer straight into
        // GetOrMapFile's cached mapping -- a member function (not a free
        // one) purely so it can call GetOrMapFile, which is private.
        template <typename Kernel>
        void WalkYearSpan(const std::string &db_base_path, const XStore_Proto::ConfigStruct &cfg_struct,
                          unsigned long long start_epoch_t, unsigned long long end_epoch_t, Kernel &&kernel);

        // Same year-by-year walk as WalkYearSpan, but distributes segments
        // across a bounded thread pool when the span covers enough distinct
        // years to be worth it -- narrow spans (the common case) fall back
        // to calling WalkYearSpan directly, no thread spawned at all.
        // segmentKernel(PartialState&, rowsBase, rowsToProcess) accumulates
        // into a PER-THREAD-LOCAL PartialState, never a value shared across
        // threads -- protobuf/ValueStruct objects aren't safe for
        // concurrent mutation from multiple threads even on logically-
        // disjoint fields (see protobuf's own thread-safety docs). mergeFn
        // (finalState&, partialState) combines each thread's partial result
        // into the running total sequentially after all threads join --
        // cheap, O(#threads) not O(#rows). Returns the fully-merged state.
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
        void ProcessFileRangeAccumulate(const char* rows_base, unsigned long long total_rows,
                                         size_t row_len, unsigned short column_idx,
                                         double& sum_out, unsigned long long& valid_count_out);

        // MIN - AGGREGATE
        void MinAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Min_REP *return_data);

        // MAX - AGGREGATE
        void MaxAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Max_REP *return_data);

        // SUM - AGGREGATE
        void SumAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Sum_REP *return_data);

        // AVG - AGGREGATE
        void AvgAggregate(unsigned long long int start_epoch_t, unsigned long long int end_epoch_t, std::string_view db_name, unsigned short int column_idx, XStore_Proto::Avg_REP *return_data);

        // Initialize DB
        void InitDB();

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
        const std::string& CachedYearFilePath(const std::string &db_base_path, unsigned short year) const;

        // Base directory for dbName's data/config files under data_store_dir,
        // e.g. "<data_store_dir>/<dbName>" -- no trailing '/' (see convention
        // note above base_dir). Assumes dbName is already upper-cased (call
        // ToUpperCase first, as every public method does).
        std::string DbBasePath(const std::string &db_name) const;

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

        // Fetch targetFile's cached fd, creating (and ftruncate'ing to
        // totalRows*rowLength if new) the file on first use. Returns -1 on
        // open/ftruncate failure. Callers must keep the returned shared_ptr
        // alive for as long as they use the fd (see FdShard above) -- that's
        // what defers the eventual close() past a concurrent eviction.
        std::shared_ptr<int> GetOrOpenFile(const std::string &target_file, size_t row_length, unsigned long long total_rows);

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