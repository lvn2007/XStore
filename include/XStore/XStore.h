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
#include <mutex>
#include <shared_mutex>
#include <string>
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
        // (BaseDir, DataStoreDir, DbBasePath()'s result) is stored WITHOUT a
        // trailing '/'. Joining is always done explicitly via JoinPath (or an
        // explicit '/' in a format string), never by relying on a variable
        // having one baked in -- that made call sites impossible to read
        // without checking how the value upstream was built.
        std::string BaseDir = GetBaseDir();
        std::string DataStoreDir = JoinPath(BaseDir, "DataStore");

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
            std::unordered_map<std::string, XStore_Proto::ConfigStruct> Map;
            std::shared_mutex Mutex;
        };
        static constexpr size_t CfgShardCount = 16;
        static std::array<CfgShard, CfgShardCount> CfgShards;

        // Shared cache of PROT_WRITE mmap regions for .XSTORE data files, keyed
        // by full path, so repeat UnaryInserts to the same (dbName, year) file
        // skip open/ftruncate/mmap and become a single memcpy into an
        // already-resident page. File size is fixed at creation (ftruncate'd
        // up front to the year's full row count), so one mapping covers the
        // whole file for its lifetime -- no remap needed. MRU-ordered list +
        // index map = O(1) lookup/promote/evict; bounded (MmapCacheCap) to
        // avoid unbounded address-space/FD growth.
        //
        // Sharded across MmapShardCount independent {list, map, mutex}
        // instances, keyed by hashing the file path, instead of one
        // process-wide mutex: a cold-started server hit by many concurrent
        // first-touches (e.g. N threads each first-mapping a different year
        // file at once) used to serialize entirely on that single mutex for
        // the lookup+MRU-promote step, even though the actual open/mmap work
        // was already unlocked. Two threads touching different files usually
        // land in different shards now and don't share a lock at all; only
        // threads racing on the same file (or a hash collision) still
        // serialize, same as before. Per-shard cap is MmapCacheCap divided
        // across shards so total resident mapping count is unchanged.
        // Distinct row offsets never overlap in memory so concurrent
        // memcpy's to disjoint slots need no extra locking beyond this
        // (FileRangeLockGuard still serializes same-slot writes).
        struct MappedFile { void *Base; size_t Len; };
        struct MmapShard {
            std::list<std::pair<std::string, MappedFile>> Lru;
            std::unordered_map<std::string, std::list<std::pair<std::string, MappedFile>>::iterator> Index;
            std::mutex Mutex;
        };
        static constexpr size_t MmapShardCount = 64;
        static constexpr size_t MmapCacheCap = 256;
        static constexpr size_t MmapShardCap = MmapCacheCap / MmapShardCount;
        static std::array<MmapShard, MmapShardCount> MmapShards;

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
        // Sized differently than MmapCacheCap on purpose: a miss here is just
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
        // fewer shards -- FdCacheCap is already tiny (8 total) so 64 shards
        // would leave under one slot each. 4 shards keeps a usable 2-slot
        // cap per shard while still splitting the lock so concurrent
        // first-opens of different files don't all serialize on one mutex.
        // Expect a smaller win than the mmap case: an fd-open miss is
        // already cheap (no page-fault-time block allocation to hide behind
        // an unlocked window), so the contention this closes is narrower to
        // start with.
        struct FdShard {
            std::list<std::pair<std::string, int>> Lru;
            std::unordered_map<std::string, std::list<std::pair<std::string, int>>::iterator> Index;
            std::mutex Mutex;
        };
        static constexpr size_t FdShardCount = 4;
        static constexpr size_t FdCacheCap = 8;
        static constexpr size_t FdShardCap = FdCacheCap / FdShardCount;
        static std::array<FdShard, FdShardCount> FdShards;
        std::string PermittedTimeResolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int DbCount = 0;
        std::vector<std::string> DbNames = { };
        std::map<std::string, unsigned long long int> ResolutionMap { {"SECOND", 31'536'000}, {"MILLISECOND", 31'536'000'000}, {"MICROSECOND", 31'536'000'000'000}, {"NANOSECOND", 31'536'000'000'000'000} };

        // Initialization
        XStore();

        // PROCESSING DbStats/info query
        XStore_Proto::SysInfo_REP InfoQuery(std::string serverAddr, int threadCount);

        // CREATING new DB instance with row length
        XStore_Proto::CreateDB_REP CreateDB(std::string dbName, XStore_Proto::TimeUnit timeResolution, short int rowLength);

        // REMOVE DB -- Removing an entire DB and its data files
        XStore_Proto::RemoveDB_REP RemoveDB(std::string dbName);

        // REMOVE DB FILES -- Removing file(s) based on name in a certain DB
        XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(XStore_Proto::RemoveDBFiles_REQ *protoFromClient, std::string dbName);

        // UNARY INSERT
        void UnaryInsert(const unsigned long long int &epochTime, const XStore_Proto::RowData &data, std::string dbName, XStore_Proto::UnaryInsert_REP *returnData);

        // UNARY QUERY
        void UnaryQuery(const unsigned long long int &epochTime, std::string dbName, XStore_Proto::UnaryQuery_REP *returnData);

        // UNARY DELETE
        XStore_Proto::UnaryDelete_REP UnaryDelete(const unsigned long long int &epochTime, std::string dbName);

        // BATCH INSERT
        void BatchInsert(XStore_Proto::BatchInsert_REQ *protoFromClient, XStore_Proto::BatchInsert_REP *returnData);

        // BATCH QUERY
        void BatchQuery(XStore_Proto::BatchQuery_REQ *protoFromClient, XStore_Proto::BatchQuery_REP *returnData, bool SortTime=true);

        // BATCH DELETE
        XStore_Proto::BatchDelete_REP BatchDelete(XStore_Proto::BatchDelete_REQ *protoFromClient);

        // RANGE INSERT
        void RangeInsert(const XStore_Proto::RangeInsert_REQ *protoFromClient, XStore_Proto::RangeInsert_REP *returnData);

        // RANGE QUERY
        void RangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, XStore_Proto::RangeQuery_REP *returnData);

        // RANGE DELETE
        XStore_Proto::RangeDelete_REP RangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName);

        // Walks [startEpochT, endEpochT] one year-file at a time for
        // Min/Max/Sum/AvgAggregate, read-locking each existing segment and
        // handing kernel(rowsBase, rowsToProcess) a pointer straight into
        // GetOrMapFile's cached mapping -- a member function (not a free
        // one) purely so it can call GetOrMapFile, which is private.
        template <typename Kernel>
        void WalkYearSpan(const std::string &dbBasePath, const XStore_Proto::ConfigStruct &cfgStruct,
                          unsigned long long startEpochT, unsigned long long endEpochT, Kernel &&kernel);

        // Shared mmap-backed scan kernels used by Min/Max/Sum/AvgAggregate ----------
        // rowsBase points directly into GetOrMapFile's cached, already-resident
        // mapping (offset to the first row this kernel should scan) -- no
        // mmap()/madvise()/munmap() here anymore. That per-call setup/teardown
        // used to happen fresh on every single aggregate call (WalkYearSpan
        // mapped just the queried sub-range itself, never through the shared
        // cache), unlike every other read path, which all reuse GetOrMapFile's
        // MRU-cached mapping. Now this shares it too.
        //
        // Select kernel: tracks the single winning row under comparator Cmp(candidate, currentBest).
        //  Cmp = std::less<long double>  -> MIN ;  Cmp = std::greater<long double> -> MAX
        template <typename Cmp>
        void ProcessFileRangeSelect(const char* rowsBase, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     ValueStruct& bestValue, XStore_Proto::FData& bestData);

        // Accumulate kernel: running sum + valid-row count. Backs SUM directly, AVG divides after.
        void ProcessFileRangeAccumulate(const char* rowsBase, unsigned long long totalRows,
                                         size_t rowLen, unsigned short columnIdx,
                                         double& sumOut, unsigned long long& validCountOut);

        // MIN - AGGREGATE
        void MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);

        // Profiling-only prototype (see tests/perf/XStorePerfTest.cpp): same scan
        // as ProcessFileRangeSelect/MinAggregate but compares natively-typed
        // doubles directly, skipping GetFDataValue's switch + long double
        // conversion. Hardcoded to doubleData -- exists to measure the delta a
        // type-specialized kernel would buy, not as a drop-in replacement.
        template <typename Cmp>
        void ProcessFileRangeSelectTypedDouble(const char* rowsBase, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     bool& bestInitialized, double& bestValue, XStore_Proto::FData& bestData);
        void MinAggregateTypedDouble(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);

        // MAX - AGGREGATE
        void MaxAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData);

        // SUM - AGGREGATE
        void SumAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData);

        // AVG - AGGREGATE
        void AvgAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData);

        // Initialize DB
        void InitDB();

    private:
        // Get current base directory
        std::string GetBaseDir();

        // Base directory for dbName's data/config files under DataStoreDir,
        // e.g. "<DataStoreDir>/<dbName>" -- no trailing '/' (see convention
        // note above BaseDir). Assumes dbName is already upper-cased (call
        // ToUpperCase first, as every public method does).
        std::string DbBasePath(const std::string &dbName) const;

        // Fetch dbName's ConfigStruct from CfgMap, loading it from dbBasePath's
        // .CFG file into the cache on first use. Returns false if the .CFG file
        // does not exist. Thread-safe: shared lock on cache hit, exclusive lock
        // (with re-check) only on cache miss.
        bool GetOrLoadCfg(const std::string &dbName, const std::string &dbBasePath, XStore_Proto::ConfigStruct &cfgOut);

        // Insert/overwrite dbName's ConfigStruct in CfgMap under exclusive lock.
        void PutCfg(const std::string &dbName, const XStore_Proto::ConfigStruct &cfg);

        // Remove dbName's ConfigStruct from CfgMap under exclusive lock.
        void EraseCfg(const std::string &dbName);

        // Fetch targetFile's cached PROT_WRITE mapping, creating (and
        // ftruncate'ing to totalRows*rowLength if new) the file and mapping it
        // on first use. Returns nullptr on open/mmap failure.
        void* GetOrMapFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows);

        // munmap + drop targetFile's cached mapping, if present. Used before a
        // file is deleted out from under the cache (RemoveDBFiles).
        void EraseMappedFile(const std::string &targetFile);

        // munmap + drop every cached mapping whose path falls under dbDirPrefix.
        // Used before a DB directory is deleted out from under the cache
        // (RemoveDB), since a single RemoveDB can span many cached year files.
        void EraseMappedFilesUnderPrefix(const std::string &dbDirPrefix);

        // Fetch targetFile's cached fd, creating (and ftruncate'ing to
        // totalRows*rowLength if new) the file on first use. Returns -1 on
        // open/ftruncate failure.
        int GetOrOpenFile(const std::string &targetFile, size_t rowLength, unsigned long long totalRows);

        // close + drop targetFile's cached fd, if present.
        void EraseOpenFile(const std::string &targetFile);

        // close + drop every cached fd whose path falls under dbDirPrefix.
        void EraseOpenFilesUnderPrefix(const std::string &dbDirPrefix);

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

        void GetFDataValue(const XStore_Proto::FData *fDataInput, unsigned int columnIdx, ValueStruct *ValueStruct);
        void GetFDataValue(const XStore_Proto::FData *fDataInput, ValueStruct *ValueStruct);
};

#endif //XSTORE_XSTORE_H