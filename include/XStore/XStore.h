#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

#include <string>
#include <cstring>
#include <map>
#include <iostream>
#include <filesystem>
#include <stdio.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <algorithm>
#include <sys/types.h>
#include <execution>
#include "XStore.pb.h"
#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/DataStruct.h"
#include "include/DataStructs/FileStruct.h"
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
        std::string baseDir = GetBaseDir();
        std::string dataStoreDir = baseDir + "DataStore/";
        std::map<std::string, FileStruct> dbMap;
        std::map<std::string, XStore_Proto::ConfigStruct> cfgMap;
        std::string permittedTimeResolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int dbCount = 0;
        std::vector<std::string> dbNames = { };
        std::map<std::string, unsigned long long int> resolutionMap { {"SECOND", 31'536'000}, {"MILLISECOND", 31'536'000'000}, {"MICROSECOND", 31'536'000'000'000}, {"NANOSECOND", 31'536'000'000'000'000} };

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

        // MIN - AGGREGATE
        void MinAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);
        void MinAggregateV2(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);
        void ProcessFileRange(FILE* bFile, unsigned long long startOffset, unsigned long long totalRows,
                             size_t rowLen, unsigned short columnIdx,
                             ValueStruct& minValue, XStore_Proto::FData& minData);
        void MinAggregateV3(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Min_REP *returnData);

        // MAX - AGGREGATE
        void MaxAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData);
        void MaxAggregateV2(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Max_REP *returnData);

        // SUM - AGGREGATE
        void SumAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData);
        void SumAggregateV2(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Sum_REP *returnData);

        // AVG - AGGREGATE
        void AvgAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData);
        void AvgAggregateV2(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::Avg_REP *returnData);

        // Initialize DB
        void InitDB();

    private:
        // Get current base directory
        std::string GetBaseDir();

        // Check if there's any existing database
        bool HasDB();

        bool HasDBFile(std::string filePath);

        void ToUpperCase(std::string::iterator begin, std::string::iterator end) {
            for (auto it = begin; it != end; ++it) {
                *it &= ~0x20;
            }
        };

        void GetFDataValue(const XStore_Proto::FData *fDataInput, unsigned int columnIdx, ValueStruct *ValueStruct);
        void GetFDataValue(const XStore_Proto::FData *fDataInput, ValueStruct *ValueStruct);
};

#endif //XSTORE_XSTORE_H