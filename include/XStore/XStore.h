#ifndef XSTORE_XSTORE_H
#define XSTORE_XSTORE_H

#include <string>
#include <map>
#include <iostream>
#include <filesystem>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <algorithm>

#include "include/dataStructs/dataStruct.h"
#include "include/dataStructs/fileStruct.h"
#include "include/dataStructs/configStruct.h"
#include <xStore.grpc.pb.h>

class XStore {
    public:
        std::string baseDir = getBaseDir();
        std::map<std::string, fileStruct> dbMap;

        int dbCount = 0;
        std::vector<std::string> dbNames = { };
        std::map<std::string, long int> resolutionMap { {"SECOND", 31536000}, {"MILLISECOND", 31536000000}, {"MICROSECOND", 31536000000000} };

        // Initialization
        XStore();

        // CREATING new DB instance with row length
        bool createDB(std::string dbName, int rowLength);

        // INSERT time-series oriented values
        void insert(std::time_t epochTime, std::string value, std::string dbName);

        // INSERT BATCH time-series oriented values
        void insertBatch(std::time_t epochTime, std::string value, std::string dbName);

        std::string get(std::time_t epochTime, std::string dbName);

        std::vector<std::string> getBatch(std::time_t startEpochT, std::time_t endEpochT, std::string dbName);

        std::string getSingleOpenClose(std::time_t epochTime, std::string dbName);

        int getRowSize(std::string dbName);

        // Initialize DB
        void initDB();

    private:
        // Get current base directory
        std::string getBaseDir();

        // Check if there's any existing database
        bool hasDB();

        bool hasDBFile(std::string filePath);

        void initMsg();

        void dbStats();

//        bool yearExists(int inputYear, std::string dbName);

        // Returning epoch value of FIRST available datetime of a given year
        long int getStartTimeEpoch(int yearInput, std::string timeResolution);

        // Returning epoch value of LAST available datetime of a given year
        long int getEndTimeEpoch(int yearInput, std::string timeResolution);

        // Time extractor
        std::string getTimeResolution(std::time_t unixTime);

        int toHumanYear(std::time_t unixTime);

        int toHumanMonth(std::time_t unixTime);

        int toHumanDay(std::time_t unixTime);
};

#endif //XSTORE_XSTORE_H