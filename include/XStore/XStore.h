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

class XStore {
    public:
        std::string baseDir = getBaseDir();
        std::string dataStoreDir = baseDir + "dataStore/";
        std::map<std::string, fileStruct> dbMap;
        std::string permittedTimeResolution[2] = {"SECOND", "MILLISECOND"};

        int dbCount = 0;
        std::vector<std::string> dbNames = { };
        std::map<std::string, long int> resolutionMap { {"SECOND", 31536000}, {"MILLISECOND", 31536000000}, {"MICROSECOND", 31536000000000}, {"NANOSECOND", 31536000000000000} };

        // Initialization
        XStore();

        // PROCESSING dbStats/info query
        XStore_Proto::sysInfo_REP infoQuery(std::string serverAddr, int threadCount);

        // CREATING new DB instance with row length
        XStore_Proto::createDB_REP createDB(std::string dbName, std::string timeResolution);
        XStore_Proto::createDB_REP createDB_fTruncate(std::string dbName, std::string timeResolution, short int rowLength);

        // UNARY INSERT time-series oriented values
        XStore_Proto::unaryInsert_REP unaryInsert(std::time_t epochTime, std::string data, std::string dbName);
        XStore_Proto::unaryInsert_REP unaryInsert_fTruncate(std::time_t epochTime, std::string data, std::string dbName);

        // BATCH INSERT time-series oriented values
        XStore_Proto::batchInsert_REP batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient);
        XStore_Proto::batchInsert_REP batchInsert_fTruncate(XStore_Proto::batchInsert_REQ *protoFromClient);

        // UNARY GET time-series oriented values
        XStore_Proto::unaryQuery_REP unaryQuery(std::time_t epochTime, std::string dbName);
        XStore_Proto::unaryQuery_REP unaryQuery_fTruncate(std::time_t epochTime, std::string dbName);

        // BATCH GET time-series oriented values
        XStore_Proto::batchQuery_REP batchQuery(std::time_t startEpochT, std::time_t endEpochT, std::string dbName);

        // REMOVE DB -- Removing an entire DB and its data files
        XStore_Proto::removeDB_REP removeDB(std::string dbName);

        // REMOVE DB FILES -- Removing file(s) based on name in a certain DB
        XStore_Proto::removeDBFiles_REP removeDBFiles(std::string dbName, XStore_Proto::removeDBFiles_REQ *protoFromClient);

        // Initialize DB
        void initDB();

    private:
        // Static array for fast build of file path
        const std::string monthArray[12] {"1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "10/", "11/", "12/"};
        const std::string dayArray[31] {"1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "10/", "11/", "12/", "13/", "14/", "15/", "16/", "17/", "18/", "19/", "20/", "21/", "22/", "23/", "24/", "25/", "26/", "27/", "28/", "29/", "30/", "31/"};
        const std::string hourArray[24] {"0/", "1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "10/", "11/", "12/", "13/", "14/", "15/", "16/", "17/", "18/", "19/", "20/", "21/", "22/", "23/"};
        const std::string minArray[60] {"0/", "1/", "2/", "3/", "4/", "5/", "6/", "7/", "8/", "9/", "10/", "11/", "12/", "13/", "14/", "15/", "16/", "17/", "18/", "19/", "20/", "21/", "22/", "23/", "24/", "25/", "26/", "27/", "28/", "29/", "30/", "31/", "32/", "33/", "34/", "35/", "36/", "37/", "38/", "39/", "40/", "41/", "42/", "43/", "44/", "45/", "46/", "47/", "48/", "49/", "50/", "51/", "52/", "53/", "54/", "55/", "56/", "57/", "58/", "59/"};

        // Get current base directory
        std::string getBaseDir();

        // Check if there's any existing database
        bool hasDB();

        bool hasDBFile(std::string filePath);

        void initMsg();

        void dbStats();

        // Returning epoch value of FIRST available datetime of a given year
        long int getStartTimeEpoch(int yearInput, std::string timeResolution);

        // Returning epoch value of LAST available datetime of a given year
        long int getEndTimeEpoch(int yearInput, std::string timeResolution);

        // Time extractor
        std::string getTimeResolution(std::time_t unixTime);
        unsigned short int toHumanYear(std::time_t &unixTime, std::string &timeResolution);
};

#endif //XSTORE_XSTORE_H