#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

#include <string>
#include <cstring>
#include <map>
#include <iostream>
#include <filesystem>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <algorithm>
#include <sys/types.h>
#include <sys/file.h>
#include <execution>
#include "XStore.pb.h"
#include "include/dataStructs/timeStruct.h"
#include "include/dataStructs/dataStruct.h"
#include "include/dataStructs/fileStruct.h"
#include "include/dataStructs/configStruct.h"
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
        std::string baseDir = getBaseDir();
        std::string dataStoreDir = baseDir + "dataStore/";
        std::map<std::string, fileStruct> dbMap;
        std::map<std::string, XStore_Proto::configStruct> cfgMap;
        std::string permittedTimeResolution[3] = {"SECOND", "MILLISECOND", "MICROSECOND"};

        int dbCount = 0;
        std::vector<std::string> dbNames = { };
        std::map<std::string, unsigned long long int> resolutionMap { {"SECOND", 31'536'000}, {"MILLISECOND", 31'536'000'000}, {"MICROSECOND", 31'536'000'000'000}, {"NANOSECOND", 31'536'000'000'000'000} };

        // Initialization
        XStore();

        // PROCESSING dbStats/info query
        XStore_Proto::sysInfo_REP infoQuery(std::string serverAddr, int threadCount);

        // CREATING new DB instance with row length
        XStore_Proto::createDB_REP createDB(std::string dbName, XStore_Proto::timeUnit timeResolution, short int rowLength);

        // REMOVE DB -- Removing an entire DB and its data files
        XStore_Proto::removeDB_REP removeDB(std::string dbName);

        // REMOVE DB FILES -- Removing file(s) based on name in a certain DB
        XStore_Proto::removeDBFiles_REP removeDBFiles(XStore_Proto::removeDBFiles_REQ *protoFromClient, std::string dbName);

        // UNARY INSERT
        void unaryInsert(const unsigned long long int &epochTime, const XStore_Proto::rowData &data, std::string dbName, XStore_Proto::unaryInsert_REP *returnData);

        // UNARY QUERY
        void unaryQuery(const unsigned long long int &epochTime, std::string dbName, XStore_Proto::unaryQuery_REP *returnData);

        // UNARY DELETE
        XStore_Proto::unaryDelete_REP unaryDelete(const unsigned long long int &epochTime, std::string dbName);

        // BATCH INSERT
        void batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient, XStore_Proto::batchInsert_REP *returnData);

        // BATCH QUERY
        void batchQuery(XStore_Proto::batchQuery_REQ *protoFromClient, XStore_Proto::batchQuery_REP *returnData);

        // BATCH DELETE
        XStore_Proto::batchDelete_REP batchDelete(XStore_Proto::batchDelete_REQ *protoFromClient);

        // RANGE INSERT
        void rangeInsert(const XStore_Proto::rangeInsert_REQ *protoFromClient, XStore_Proto::rangeInsert_REP *returnData);

        // RANGE QUERY
        void rangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, XStore_Proto::rangeQuery_REP *returnData);

        // RANGE DELETE
        XStore_Proto::rangeDelete_REP rangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName);

        // MIN - AGGREGATE
        void minAggregate(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, unsigned short int columnIdx, XStore_Proto::min_REP *returnData);

        // Initialize DB
        void initDB();

    private:
        // Get current base directory
        std::string getBaseDir();

        // Check if there's any existing database
        bool hasDB();

        bool hasDBFile(std::string filePath);

        // File locks
        void wrLockFile(FILE *bFile, unsigned long int startRange, unsigned long int endRange);
        void rdLockFile(FILE *bFile, unsigned long int startRange, unsigned long int endRange);
        void unlockFile(FILE *bFile, unsigned long int startRange, unsigned long int endRange);

        void toUpperCase(std::string::iterator begin, std::string::iterator end) {
            for (auto it = begin; it != end; ++it) {
                *it &= ~0x20;
            }
        };

        void getFDataValue(XStore_Proto::fData *fDataInput, std::vector<long double> *returnData);
        void getFDataValue(XStore_Proto::fData *fDataInput, unsigned int columnIdx, valueStruct *valueStruct);
};

#endif //XSTORE_XSTORE_H