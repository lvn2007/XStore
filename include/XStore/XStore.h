#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

#include <string>
#include <cstring>
#include <map>
#include <list>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <iostream>
#include <filesystem>
#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <algorithm>
#include <functional>
#include <sys/types.h>
#include <execution>
#include "XStore.pb.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/DataStruct.h"
#include "include/DataStructs/ConfigStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

// Length of msg header
#define TRACE_LEN 8
#define DEBUG_LEN 8
#define INFO_LEN 7
#define WARNING_LEN 10
#define ERROR_LEN 8
#define CRITICAL_LEN 11

class XStore {
    public:
        std::string BaseDir = GetBaseDir();
        std::string DataStoreDir = BaseDir + "DataStore/";

        // Shared across every XStore instance (one per worker thread) so a config
        // read from disk by one thread is visible to all others, and guarded by
        // CfgMutex since std::map isn't safe for concurrent read/insert.
        static std::map<std::string, XStore_Proto::ConfigStruct> CfgMap;
        static std::shared_mutex CfgMutex;

        // Shared cache of PROT_WRITE mmap regions for .XSTORE data files, keyed
        // by full path, so repeat UnaryInserts to the same (dbName, year) file
        // skip open/ftruncate/mmap and become a single memcpy into an
        // already-resident page. File size is fixed at creation (ftruncate'd
        // up front to the year's full row count), so one mapping covers the
        // whole file for its lifetime -- no remap needed. MRU-ordered list +
        // index map = O(1) lookup/promote/evict; bounded (MmapCacheCap) to
        // avoid unbounded address-space/FD growth. Guarded by MmapMutex since
        // every access (including cache hits, which reorder the MRU list)
        // mutates shared state; distinct row offsets never overlap in memory
        // so concurrent memcpy's to disjoint slots need no extra locking
        // (FileRangeLockGuard still serializes same-slot writes).
        struct MappedFile { void *Base; size_t Len; };
        static std::list<std::pair<std::string, MappedFile>> MmapLru;
        static std::unordered_map<std::string, std::list<std::pair<std::string, MappedFile>>::iterator> MmapIndex;
        static std::mutex MmapMutex;
        static constexpr size_t MmapCacheCap = 256;
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

        // Shared mmap-backed scan kernels used by Min/Max/Sum/AvgAggregate ----------
        // Select kernel: tracks the single winning row under comparator Cmp(candidate, currentBest).
        //  Cmp = std::less<long double>  -> MIN ;  Cmp = std::greater<long double> -> MAX
        template <typename Cmp>
        void ProcessFileRangeSelect(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                     size_t rowLen, unsigned short columnIdx,
                                     ValueStruct& bestValue, XStore_Proto::FData& bestData);

        // Accumulate kernel: running sum + valid-row count. Backs SUM directly, AVG divides after.
        void ProcessFileRangeAccumulate(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                                         size_t rowLen, unsigned short columnIdx,
                                         double& sumOut, unsigned long long& validCountOut);

        // MIN - AGGREGATE
        void MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);

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

        // Check if there's any existing database
        bool HasDB();

        bool HasDBFile(std::string filePath);

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

        void ToUpperCase(std::string::iterator begin, std::string::iterator end) {
            for (auto it = begin; it != end; ++it) {
                *it &= ~0x20;
            }
        };

        void GetFDataValue(const XStore_Proto::FData *fDataInput, unsigned int columnIdx, ValueStruct *ValueStruct);
        void GetFDataValue(const XStore_Proto::FData *fDataInput, ValueStruct *ValueStruct);
};

#endif //XSTORE_XSTORE_H