#include "include/XStore/XStore.h"

// Initialization
XStore::XStore() {
    // Initialize DB
    initDB();

    // First, check if there's any existing data exists
//    hasDB();
}

XStore_Proto::sysInfo_REP XStore::infoQuery(std::string serverAddr, int threadCount) {
    // Protobuf obj to be returned
    XStore_Proto::sysInfo_REP toReturn;

    // Overview info
    toReturn.set_serveraddr(serverAddr);
    toReturn.set_threadcount(threadCount);

    // Loop through all db available in dataStore
    for (auto& dir : std::filesystem::directory_iterator(dataStoreDir)) {
        std::string currDBName = dir.path().filename().string();
        std::string currDBDir = dataStoreDir + currDBName;

        // Set DB Name
        XStore_Proto::dbStats *currDB = toReturn.add_dbinfo();
        currDB->set_dbname(currDBName);

        // Fetch config of a target DB
        XStore_Proto::configStruct cfgStruct;
        std::string currCfgFilePath = currDBDir + "/" + currDBName + ".CFG";

        // Check if .CFG file is exist
        if (std::filesystem::exists(currCfgFilePath)) {
            int cfgFile = open(currCfgFilePath.c_str(), O_RDONLY);
            cfgStruct.ParseFromFileDescriptor(cfgFile);

            // Set DB Resolution
            currDB->set_dbresolution(cfgStruct.timeresolution());
        }

        // Size of all data files in current DB
        unsigned long int allSize = 0;
        struct stat statStruct;

        // Loop through all files in currentDBDir
        for (auto& dbDir : std::filesystem::directory_iterator(currDBDir)) {
            // Current file name & path in dbDir
            std::string currFileName = dbDir.path().filename().string();
            stat((currDBDir + "/" + currFileName).c_str(), &statStruct);

            // Fetch current file size & convert to GB
            unsigned int currFileSizeInGB = file_size(dbDir.path());

            // Number of 512B blocks allocated
            allSize += statStruct.st_blocks * 512;
            currDB->add_filename(currFileName);
        }
        currDB->set_totalsizeinbyte(allSize);
    }

    return toReturn;
}

XStore_Proto::createDB_REP XStore::createDB(std::string dbName, XStore_Proto::timeUnit timeResolution, short int rowLength) {
    // Protobuf obj to be returned
    XStore_Proto::createDB_REP toReturn;
    std::string createDBStatus;

    // Ensure dbName & timeResolution string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    if (rowLength < 4) {
        createDBStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Row length must be >= 4", dbName);
        toReturn.set_createdbstatus(createDBStatus);
        spdlog::error(createDBStatus.substr(ERROR_LEN));
        return toReturn;
    }

    // Target directory of a given DB name
    std::string targetPath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(permittedTimeResolution), std::end(permittedTimeResolution), XStore_Proto::timeUnit_Name(timeResolution)) == std::end(permittedTimeResolution)) {
            createDBStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Invalid time resolution", dbName);
            toReturn.set_createdbstatus(createDBStatus);
            spdlog::error(createDBStatus.substr(ERROR_LEN));
            return toReturn;
        }

        // Create DB directory
        if (std::filesystem::create_directory(targetPath) == 1) {
            std::string successMsg = fmt::format("[INFO] [CREATE DB] [DB: {}] - Created DB", dbName);
            toReturn.set_createdbstatus(successMsg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            XStore_Proto::configStruct cfgStruct;
            cfgStruct.set_dbname(dbName);
            cfgStruct.set_timeresolution(timeResolution);
            cfgStruct.set_rowlength(rowLength);

            // Writing cfgStruct to file
            FILE *bFile;
            std::string cfgPath  = fmt::format("{}{}.CFG", targetPath, dbName);
            bFile = std::fopen(cfgPath.c_str(), "wb+");

            // Lock prior to writing
            wrLockFile(bFile, 0, cfgStruct.ByteSizeLong());

            // Writing actual data to disk
            cfgStruct.SerializeToFileDescriptor(fileno(bFile));

            //  Close file & Release lock after write
            unlockFile(bFile, 0, cfgStruct.ByteSizeLong());
            std::fclose(bFile);

            //  Add this cfgStruct to cfgMap
            cfgMap[dbName] = cfgStruct;

            spdlog::info(successMsg.substr(INFO_LEN));
            return toReturn;
        }
    }

    createDBStatus = fmt::format("[ERROR] [CREATE DB] [DB: {}] - Already existed", dbName);
    toReturn.set_createdbstatus(createDBStatus);
    spdlog::error(createDBStatus.substr(ERROR_LEN));
    return toReturn;
}

// UNARY INSERT
void XStore::unaryInsert(const unsigned long long int &unixTime, const XStore_Proto::rowData &data, std::string dbName, XStore_Proto::unaryInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string insertStatus;

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    timeStruct currTS(unixTime);

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [UNARY INSERT] [DB: {}] - DB does not exist", dbName);
        returnData->set_insertstatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
        cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Enforce time granularity
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (currTS.timeResolution != expectedTimeResolution) {
        std::string errMsg = fmt::format("[ERROR] [UNARY INSERT] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
        returnData->set_insertstatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Prepare to write
    FILE *bFile;
    std::string targetFile = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

    // IF target file does NOT exist
    if (std::filesystem::exists(targetFile) == 0) {
        // Truncate
        bFile = std::fopen(targetFile.c_str(), "wb+");

        // Lock prior to file OP
        XStore::wrLockFile(bFile, 0, 0);
        ftruncate(fileno(bFile), (currTS.lastEpoch - currTS.firstEpoch) * cfgStruct.rowlength());
        XStore::unlockFile(bFile, 0, 0);
    }
    else {
        // ELSE - Target file DOES exist
        //  Open in append mode
        bFile = std::fopen(targetFile.c_str(), "rb+");
    }

    //  Computes diff & offset
    unsigned long long int diff = unixTime - currTS.firstEpoch;
    unsigned long long int offset = diff * cfgStruct.rowlength();

    // Lock prior to file OP
    XStore::wrLockFile(bFile, offset, cfgStruct.rowlength());

    //  Seek to target offset prior to write
    fseeko64(bFile, offset, SEEK_SET);

    //  Writing actual data to disk
    fwrite(data.SerializeAsString().data(), cfgStruct.rowlength(), 1, bFile);

    //  Release lock & Close file after write
    XStore::unlockFile(bFile, offset, cfgStruct.rowlength());
    std::fclose(bFile);

    std::string successMsg = fmt::format("[INFO] [UNARY INSERT] [DB: {}] - Inserted {} to {}.XSTORE", dbName, unixTime, currTS.year);
    returnData->set_insertstatus(successMsg);
    spdlog::info(successMsg.substr(INFO_LEN));
    return;
}

// UNARY QUERY
void XStore::unaryQuery(const unsigned long long int &unixTime, std::string dbName, XStore_Proto::unaryQuery_REP *returnData) {
    // Protobuf obj to be returned
    std::string queryStatus;
    returnData->set_timestamp(unixTime);

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    timeStruct currTS(unixTime);

    // Base directory of a DB & Target path of file to READ
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);
    std::string targetFilePath = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

    // IF target year file DOES NOT exist
    if (std::filesystem::exists(targetFilePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [UNARY QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Enforce time resolution
    std::string configPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
    XStore_Proto::configStruct cfgStruct = cfgMap[dbName];

    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (currTS.timeResolution != expectedTimeResolution) {
        std::string errMsg = fmt::format("[ERROR] [UNARY QUERY] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Target file IS EXISTED
    //  Staging area for file waiting to be read
    FILE *bFile;
    bFile = std::fopen(targetFilePath.c_str(), "rb");

    // Obtain file size
    //  Computes diff & offset
    unsigned long long int diff = unixTime - currTS.firstEpoch;
    unsigned long long int offset = diff * cfgStruct.rowlength();

    //  A single line data will be read into here
    char bufferIn[cfgStruct.rowlength()];
    XStore_Proto::rowData tempData;

    //  Lock file prior to reading
    rdLockFile(bFile, offset, cfgStruct.rowlength());

    //  Seek to computed offset
    fseeko64(bFile, offset, SEEK_SET);

    // Extract N bytes of data according to N bytes from .CFG file
    //  .CFG file is set by user when creating DB
    fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

    // Release lock & close file after reading
    unlockFile(bFile, offset, cfgStruct.rowlength());
    fclose(bFile);

    // Set data fetched from XStore to returning Protobuf obj
    tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
    *returnData->mutable_data() = tempData;
}

// UNARY DELETE
XStore_Proto::unaryDelete_REP XStore::unaryDelete(const unsigned long long int &epochTime, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::unaryDelete_REP toReturn;
    std::string deleteStatus;

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    timeStruct currTS(epochTime);

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - DB does not exist", dbName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
        cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Enforce time granularity
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (currTS.timeResolution != expectedTimeResolution) {
        std::string errMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // Prepare to delete (Writing NULL byte)
    FILE *bFile;
    std::string targetFileName = fmt::format("{}.XSTORE", currTS.year);
    std::string targetFilePath = fmt::format("{}{}", dbBasePath, targetFileName);

    // IF target file does NOT exist
    if (std::filesystem::exists(targetFilePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [UNARY DELETE] [DB: {}] - {} does not exist", dbName, targetFileName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // OTHERWISE - Target file DOES exist
    //  Open in append mode
    bFile = std::fopen(targetFilePath.c_str(), "rb+");

    //  Computes diff & offset
    unsigned long long int diff = epochTime - currTS.firstEpoch;
    unsigned long long int offset = diff * cfgStruct.rowlength();

    // Lock prior to file OP
    wrLockFile(bFile, offset, cfgStruct.rowlength());

    // Deallocate data according to N bytes from rowLength
    fallocate64(fileno(bFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfgStruct.rowlength());

    // Close file & Release lock after write
    unlockFile(bFile, offset, cfgStruct.rowlength());
    std::fclose(bFile);

    std::string successMsg = fmt::format("[INFO] [UNARY DELETE] [DB: {}] - Deleted {} in {}", dbName, epochTime, targetFileName);
    toReturn.set_deletestatus(successMsg);
    spdlog::info(successMsg.substr(INFO_LEN));
    return toReturn;
}

// BATCH INSERT
void XStore::batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient, XStore_Proto::batchInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string insertStatus;

    // Ensure DB Name string is all capitalized
    std::string dbName = protoFromClient->dbname();
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [BATCH INSERT] [DB: {}] - DB does not exist", dbName);
        returnData->set_insertstatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & Add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
        cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Assuming incoming data is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_batchinsert_data()->begin(), protoFromClient->mutable_batchinsert_data()->end(),
              [](const XStore_Proto::batchInsert_DATA& a, const XStore_Proto::batchInsert_DATA &b) {
                    return a.timestamp() < b.timestamp();
    });

    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    struct tm* epochStruct;
    std::time_t currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    std::string granularityErrMsg;

    // Granularity check
    for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
        currTime = protoFromClient->batchinsert_data(i).timestamp();
        epochStruct = gmtime(&currTime);
        dataMap[(epochStruct->tm_year + 1900)].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        timeStruct currTS {currTime};
        if (currTS.timeResolution != expectedTimeResolution) {
            granularityErrMsg = fmt::format("[ERROR] [BATCH INSERT] [DB: {}] - Invalid time granularity. Expecting: {}", dbName, expectedTimeResolution);
            returnData->set_insertstatus(granularityErrMsg);
            spdlog::error(granularityErrMsg.substr(ERROR_LEN));
            return;
        }
    }

    // Iterate through each key (year) of dataMap
    XStore_Proto::batchInsert_DATA currInsertData;
    XStore_Proto::rowData data;
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        unsigned short int yearKey = pair.first;
        FILE *bFile;
        std::string targetFile = fmt::format("{}{}.XSTORE", dbBasePath, yearKey);

        // Grab first inserting element for timeStruct purposes
        //  Get length of time to truncate
        timeStruct thisTS{static_cast<time_t>(protoFromClient->batchinsert_data().at(dataMap[yearKey].at(0)).timestamp())};

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Truncate
            bFile = std::fopen(targetFile.c_str(), "wb+");

            // Lock prior to file OP
            wrLockFile(bFile, 0, 0);
            ftruncate(fileno(bFile), (thisTS.lastEpoch - thisTS.firstEpoch) * cfgStruct.rowlength());
            unlockFile(bFile, 0, 0);
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in append mode
            bFile = std::fopen(targetFile.c_str(), "rb+");
        }

        long long int diff, offset;
        // Iterate through each YEAR of inserting data
        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Parse each data in a batch collection
            currInsertData = protoFromClient->batchinsert_data(dataMap[yearKey].at(i));
            std::time_t unixTime = currInsertData.timestamp();
            data = currInsertData.data();

            // Compute diff & offset
            diff = unixTime - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Lock prior to file OP
            wrLockFile(bFile, offset, cfgStruct.rowlength());

            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Writing actual data to disk
            fwrite(data.SerializeAsString().data(), cfgStruct.rowlength(), 1, bFile);

            // Unlock after write
            unlockFile(bFile, offset, cfgStruct.rowlength());
        }

        // Close file after write
        std::fclose(bFile);
    }

    // Return insert status
    std::string successMsg = fmt::format("[INFO] [BATCH INSERT] [DB: {}] - Inserted {} elements successfully", dbName, protoFromClient->batchinsert_data_size());
    returnData->set_insertstatus(successMsg);
    spdlog::info(successMsg.substr(INFO_LEN));
}

// BATCH QUERY
void XStore::batchQuery(XStore_Proto::batchQuery_REQ *protoFromClient, XStore_Proto::batchQuery_REP *returnData) {
    // Protobuf obj to be returned
    XStore_Proto::rowData tempData;

    // Ensure dbName string is all capitalized
    std::string dbName = protoFromClient->dbname();
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);

        if (std::filesystem::exists(cfgPath) == 0) {
            std::string errMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - {}.CFG does not exist", dbName, dbName);
            returnData->set_querystatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // IF query range > 2GB limit of Protobuf or Empty
    if ((protoFromClient->timestamp_size() * cfgStruct.rowlength()) > 2'000'000'000 || protoFromClient->timestamp_size() < 0) {
        std::string errMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - Query results is larger than 2GB or empty", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch query into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    unsigned long long int currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;

    // Granularity check
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        currTime = protoFromClient->timestamp(i);
        struct timeStruct currTS(currTime);
        dataMap[currTS.year].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        if (currTS.timeResolution != expectedTimeResolution) {
            std::string errMsg = fmt::format("[ERROR] [BATCH QUERY] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
            returnData->set_querystatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        FILE *bFile;
        unsigned short int yearKey = pair.first;
        std::string targetFile = fmt::format("{}{}.XSTORE", dbBasePath, yearKey);

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Skip to next iteration
            std::string warnMsg = fmt::format("[WARNING] [BATCH QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, yearKey);
            returnData->set_querystatus(warnMsg);
            spdlog::warn(warnMsg.substr(WARNING_LEN));
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            bFile = std::fopen(targetFile.c_str(), "rb");
        }

        // Time-related & Auxiliary
        unsigned long long int diff, offset;
        char bufferIn[cfgStruct.rowlength()];
        timeStruct thisTS{static_cast<time_t>(protoFromClient->timestamp(dataMap[yearKey].at(0)))};

        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Computes diff & offset
            diff = protoFromClient->timestamp(dataMap[yearKey].at(i)) - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Lock prior to file OP
            rdLockFile(bFile, offset, cfgStruct.rowlength());

            // Seek to target offset prior to read
            fseeko64(bFile, offset, SEEK_SET);

            // Extract N bytes of data according to N bytes from .CFG file
            //  .CFG file is set by user when creating DB
            fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

            // Unlock after read
            unlockFile(bFile, offset, cfgStruct.rowlength());

            // IF fetched data contains nothing
            //  -> Skip to next iteration
            if (strlen(bufferIn) == 0) {
                continue;
            }

            // Add fetched data to returning Protobuf obj
            tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
            XStore_Proto::unaryQuery_REP *currEntry = returnData->add_queryresponse();
            currEntry->set_timestamp(protoFromClient->timestamp(dataMap[yearKey].at(i)));
            *currEntry->mutable_data() = tempData;
        }

        // Close file & Release lock after read
        std::fclose(bFile);
    }
}

// BATCH DELETE
XStore_Proto::batchDelete_REP XStore::batchDelete(XStore_Proto::batchDelete_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchDelete_REP toReturn;
    std::string dbName = protoFromClient->dbname();
    int deleteCount = 0;

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);

        if (std::filesystem::exists(cfgPath) == 0) {
            std::string errMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
            toReturn.set_deletestatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return toReturn;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // IF target delete timestamp(s) is empty OR db directory does not exist
    if (protoFromClient->timestamp_size() < 0) {
        std::string errMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - Empty target timestamp(s)", dbName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }
    else if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - DB does not exist", dbName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch delete timestamp into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    unsigned long long int currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;

    // Granularity check
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        currTime = protoFromClient->timestamp(i);
        struct timeStruct currTS(currTime);
        dataMap[currTS.year].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        if (currTS.timeResolution != expectedTimeResolution) {
            std::string errMsg = fmt::format("[ERROR] [BATCH DELETE] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
            toReturn.set_deletestatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return toReturn;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        FILE *bFile;
        unsigned short int yearKey = pair.first;
        std::string targetFile = fmt::format("{}{}.XSTORE", dbBasePath, yearKey);

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Skip to next iteration
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            bFile = std::fopen(targetFile.c_str(), "rb+");
        }

        // Time-related & Auxiliary
        unsigned long long int diff, offset;
        timeStruct thisTS{static_cast<time_t>(protoFromClient->timestamp(dataMap[yearKey].at(0)))};

        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Computes diff & offset
            diff = protoFromClient->timestamp(dataMap[yearKey].at(i)) - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Lock prior to file OP
            wrLockFile(bFile, offset, cfgStruct.rowlength());

            // Deallocate data according to N bytes from rowLength
            fallocate64(fileno(bFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfgStruct.rowlength());

            // Unlock after file OP
            unlockFile(bFile, offset, cfgStruct.rowlength());
            deleteCount++;
        }

        // Close file & Release lock after read
        std::fclose(bFile);
    }
    std::string infoMsg = fmt::format("[INFO] [BATCH DELETE] [DB: {}] - Deleted {} items", dbName, deleteCount);
    toReturn.set_deletestatus(infoMsg);
    spdlog::info(infoMsg.substr(INFO_LEN));
    return toReturn;
}

// RANGE INSERT
void XStore::rangeInsert(const XStore_Proto::rangeInsert_REQ *protoFromClient, XStore_Proto::rangeInsert_REP *returnData) {
    // Protobuf obj to be returned
    std::string insertStatus;

    // Ensure DB Name string is all capitalized
    std::string dbName = protoFromClient->dbname();
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [RANGE INSERT] [DB: {}] - DB does not exist", dbName);
        returnData->set_insertstatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & Add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);
        cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Assuming incoming data is sorted --> Skip sorting
    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    tm epochStruct;
    std::time_t currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());

    // Granularity check
    for (int i = 0; i < protoFromClient->rangeinsert_data_size(); i++) {
        currTime = protoFromClient->rangeinsert_data(i).timestamp();
        gmtime_r(&currTime, &epochStruct);
        dataMap[(epochStruct.tm_year + 1900)].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        timeStruct currTS(currTime);
        if (currTS.timeResolution != expectedTimeResolution) {
            std::string granularityErrMsg = fmt::format("[ERROR] [RANGE INSERT] [DB: {}] - Invalid time granularity. Expecting: {}", dbName, expectedTimeResolution);
            returnData->set_insertstatus(granularityErrMsg);
            spdlog::error(granularityErrMsg.substr(ERROR_LEN));
            return;
        }
    }

    // Iterate through each key (year) of dataMap
    XStore_Proto::rangeInsert_DATA firstInsertData, currInsertData;
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        unsigned short int yearKey = pair.first;
        FILE *bFile;
        std::string targetFile = fmt::format("{}{}.XSTORE", dbBasePath, yearKey);

        // Grab first inserting element for timeStruct purposes
        //  Get length of time to truncate
        timeStruct thisTS{static_cast<time_t>(protoFromClient->rangeinsert_data().at(dataMap[yearKey].at(0)).timestamp())};

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Truncate
            bFile = std::fopen(targetFile.c_str(), "wb+");

            // Lock prior to file OP
            wrLockFile(bFile, 0, 0);
            ftruncate(fileno(bFile), (thisTS.lastEpoch - thisTS.firstEpoch) * cfgStruct.rowlength());
            unlockFile(bFile, 0, 0);
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in append mode
            bFile = std::fopen(targetFile.c_str(), "rb+");
        }

        // Metadata of first datapoint
        firstInsertData = protoFromClient->rangeinsert_data(dataMap[yearKey].at(0));
        const unsigned long long int &firstUnixTime = firstInsertData.timestamp();
        const unsigned long long int firstOffset = (firstUnixTime - thisTS.firstEpoch) * cfgStruct.rowlength();
        const unsigned long long int regionLength = cfgStruct.rowlength() * dataMap[yearKey].size();

        // Lock prior to file OP
        wrLockFile(bFile, firstOffset, regionLength);

        unsigned long long int diff, offset;
        // Iterate through each YEAR of inserting data
        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Parse each data in a batch collection
            currInsertData = protoFromClient->rangeinsert_data(dataMap[yearKey].at(i));
            const unsigned long long int &unixTime = currInsertData.timestamp();
            const XStore_Proto::rowData &data = currInsertData.data();

            // Compute diff & offset
            diff = unixTime - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Writing actual data to disk
            fwrite(data.SerializeAsString().data(), cfgStruct.rowlength(), 1, bFile);
        }

        // Close file & Release lock after write
        unlockFile(bFile, firstOffset, regionLength);
        std::fclose(bFile);
    }

    // Return insert status
    std::string infoMsg = fmt::format("[INFO] [RANGE INSERT] [DB: {}] - Inserted {} elements successfully", dbName, protoFromClient->rangeinsert_data_size());
    returnData->set_insertstatus(infoMsg);
    spdlog::info(infoMsg.substr(INFO_LEN));
}

// RANGE QUERY
void XStore::rangeQuery(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName, XStore_Proto::rangeQuery_REP *returnData) {
    // Protobuf obj to be returned
    XStore_Proto::rowData tempData;
    XStore_Proto::unaryQuery_REP *currEntry;
    std::string queryStatus;

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = dbBasePath + dbName + ".CFG";

        if (std::filesystem::exists(cfgPath) == 0) {
            std::string errMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - {}.CFG does not exist", dbName, dbName);
            returnData->set_querystatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // IF query range > 2GB limit of Protobuf
    if ((endEpochT - startEpochT) * cfgStruct.rowlength() > 2'000'000'000) {
        std::string errMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - Query results is larger than 2GB", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    if ((endEpochT - startEpochT) < 0) {
        std::string errMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - Target end time must be after start time", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - DB does not exist", dbName);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Time granularity check
    timeStruct startTimeStruct(startEpochT);
    timeStruct endTimeStruct(endEpochT);
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (startTimeStruct.timeResolution != expectedTimeResolution || endTimeStruct.timeResolution != expectedTimeResolution) {
        std::string errMsg = fmt::format("[ERROR] [RANGE QUERY] [DB: {}] - Invalid time resolution. Expecting {}", dbName, expectedTimeResolution);
        returnData->set_querystatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return;
    }

    // Organize into timeStruct
    //  timeStruct provides many auxiliary vars/methods useful for read/write ops
    timeStruct currTS(startEpochT);

    // Placeholder for READ op
    FILE *bFile;
    std::string targetFilePath;
    unsigned long long int diff, offset;
    char bufferIn[cfgStruct.rowlength()];
    std::string successMsg;

    // IF query range is within 1-year
    if (currTS.lastEpoch >= endEpochT) {
        // Open file prior to reading
        targetFilePath = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(targetFilePath) == 0) {
            // Return without error
            std::string warningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, currTS.year);
            returnData->set_querystatus(warningMsg);
            spdlog::warn(warningMsg.substr(WARNING_LEN));
            return;
        }

        bFile = std::fopen(targetFilePath.c_str(), "rb");

        // Metadata of first datapoint
        const unsigned long long int firstOffset = (startEpochT - currTS.firstEpoch) * cfgStruct.rowlength();
        const unsigned long long int regionLength = cfgStruct.rowlength() * (endEpochT - startEpochT);

        // Lock file prior to reading
        rdLockFile(bFile, firstOffset, regionLength);

        // Iterate over range of query data & add queried data and status
        for (long epochTime = startEpochT; epochTime <= endEpochT; epochTime++) {
            // Computes diff & offset
            diff = epochTime - currTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Seek to computed offset
            fseeko64(bFile, offset, SEEK_SET);

            // Extract N bytes of data according to N bytes from .CFG file
            //  .CFG file is set by user when creating DB
            fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

            // IF fetched data contains nothing
            //  -> Skip to next iteration
            if (strlen(bufferIn) == 0) {
                continue;
            }

            // Add fetched data to returning Protobuf obj
            tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
            currEntry = returnData->add_queryresponse();
            currEntry->set_timestamp(epochTime);
            *currEntry->mutable_data() = tempData;
        }

        // Close & Release file lock after reading
        unlockFile(bFile, firstOffset, regionLength);
        fclose(bFile);
    }
    // Otherwise -> Involves multi-year
    else {
        // Make a copy of start epoch time for incrementing
        unsigned long long int startEpoch = startEpochT;

        // Loop through each year individually
        while (startEpoch < endEpochT) {
            targetFilePath = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

            // IF the requested start epoch spans ONLY 1 year
            if (currTS.lastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string warningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, currTS.year);
                    returnData->set_querystatus(warningMsg);
                    spdlog::warn(warningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (currTS.lastEpoch - startEpoch));
                    continue;
                }

                bFile = std::fopen(targetFilePath.c_str(), "rb");

                // Metadata of first datapoint
                const unsigned long long int firstOffset = (startEpoch - currTS.firstEpoch) * cfgStruct.rowlength();
                const unsigned long long int regionLength = cfgStruct.rowlength() * (currTS.lastEpoch - startEpoch);

                // Lock file prior to reading
                rdLockFile(bFile, firstOffset, regionLength);

                // Iterate over range of query data & add queried data and status
                for (unsigned long long int epochTime = startEpoch; epochTime <= currTS.lastEpoch; epochTime++, startEpoch++) {
                    // Computes diff & offset
                    diff = epochTime - currTS.firstEpoch;
                    offset = diff * cfgStruct.rowlength();

                    // Seek to computed offset
                    fseeko64(bFile, offset, SEEK_SET);

                    // Extract N bytes of data according to N bytes from .CFG file
                    //  .CFG file is set by user when creating DB
                    fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

                    // IF fetched data contains nothing
                    //  -> Skip to next iteration
                    if (strlen(bufferIn) == 0) {
                        continue;
                    }

                    // Add fetched data to returning Protobuf obj
                    tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
                    currEntry = returnData->add_queryresponse();
                    currEntry->set_timestamp(epochTime);
                    *currEntry->mutable_data() = tempData;
                }

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Close file & Release lock after write
                unlockFile(bFile, firstOffset, regionLength);
                std::fclose(bFile);
            }
            else {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string warningMsg = fmt::format("[WARNING] [RANGE QUERY] [DB: {}] - {}.XSTORE does not exist", dbName, currTS.year);
                    returnData->set_querystatus(warningMsg);
                    spdlog::warn(warningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (endEpochT - startEpoch));
                    continue;
                }

                bFile = std::fopen(targetFilePath.c_str(), "rb");

                // Metadata of first datapoint
                const unsigned long long int firstOffset = (startEpoch - currTS.firstEpoch) * cfgStruct.rowlength();
                const unsigned long long int regionLength = cfgStruct.rowlength() * (endEpochT - startEpoch);

                // Lock file prior to reading
                rdLockFile(bFile, firstOffset, regionLength);

                // Iterate over range of query data & add queried data and status
                for (unsigned long long int epochTime = startEpoch; epochTime <= endEpochT; epochTime++, startEpoch++) {
                    // Computes diff & offset
                    diff = epochTime - currTS.firstEpoch;
                    offset = diff * cfgStruct.rowlength();

                    // Seek to computed offset
                    fseeko64(bFile, offset, SEEK_SET);

                    // Extract N bytes of data according to N bytes from .CFG file
                    //  .CFG file is set by user when creating DB
                    fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

                    // IF fetched data contains nothing
                    //  -> Skip to next iteration
                    if (strlen(bufferIn) == 0) {
                        continue;
                    }

                    // Add fetched data to returning Protobuf obj
                    tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
                    currEntry = returnData->add_queryresponse();
                    currEntry->set_timestamp(epochTime);
                    *currEntry->mutable_data() = tempData;
                }

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Close file & Release lock after read
                unlockFile(bFile, firstOffset, regionLength);
                std::fclose(bFile);
                break;
            }
        }
    }
}

// RANGE DELETE
XStore_Proto::rangeDelete_REP XStore::rangeDelete(const unsigned long long int &startEpochT, const unsigned long long int &endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::rangeDelete_REP toReturn;
    int deleteCount = 0;

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Base directory of a DB
    std::string dbBasePath = fmt::format("{}dataStore/{}/", baseDir, dbName);

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = fmt::format("{}{}.CFG", dbBasePath, dbName);

        if (std::filesystem::exists(cfgPath) == 0) {
            std::string errMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - {}.CFG does not exist", dbName, dbName);
            toReturn.set_deletestatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
            return toReturn;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    if ((endEpochT - startEpochT) < 0) {
        std::string errMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - Target end time must be after start time", dbName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - DB does not exist", dbName);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // Time granularity check
    timeStruct startTimeStruct(startEpochT);
    timeStruct endTimeStruct(endEpochT);
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (startTimeStruct.timeResolution != expectedTimeResolution || endTimeStruct.timeResolution != expectedTimeResolution) {
        std::string errMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - Invalid time resolution. Expecting: {}", dbName, expectedTimeResolution);
        toReturn.set_deletestatus(errMsg);
        spdlog::error(errMsg.substr(ERROR_LEN));
        return toReturn;
    }

    // Organize into timeStruct
    //  timeStruct provides many auxiliary vars/methods useful for read/write ops
    timeStruct currTS(startEpochT);

    // Placeholder for READ op
    FILE *bFile;
    std::string targetFilePath;
    unsigned long long int diff, offset, rangeCount;

    // IF query range is within 1-year
    if (currTS.lastEpoch >= endEpochT) {
        // Open file prior to reading
        targetFilePath = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(targetFilePath) == 0) {
            // Return without error
            std::string errorMsg = fmt::format("[ERROR] [RANGE DELETE] [DB: {}] - DB does not exist", dbName);
            toReturn.set_deletestatus(errorMsg);
            spdlog::error(errorMsg.substr(ERROR_LEN));
            return toReturn;
        }

        bFile = std::fopen(targetFilePath.c_str(), "rb+");

        // Compute diff & offset
        diff = startEpochT - currTS.firstEpoch;
        offset = diff * cfgStruct.rowlength();
        rangeCount = endEpochT - startEpochT;

        // Lock file prior to deleting
        wrLockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);

        // Deallocate data according to N bytes from rowLength
        fallocate64(fileno(bFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfgStruct.rowlength() * rangeCount);
        deleteCount += rangeCount;

        // Release lock & Close file after delete
        unlockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);
        fclose(bFile);
    }
    // Otherwise -> Involves multi-year
    else {
        // Make a copy of start epoch time for incrementing
        unsigned long long int startEpoch = startEpochT;

        // Loop through each year individually
        while (startEpoch < endEpochT) {
            targetFilePath = fmt::format("{}{}.XSTORE", dbBasePath, currTS.year);

            // IF the requested start epoch spans ONLY 1 year
            if (currTS.lastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string warningMsg = fmt::format("[WARNING] [RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, currTS.year);
                    toReturn.set_deletestatus(warningMsg);
                    spdlog::warn(warningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (currTS.lastEpoch - startEpoch));
                    continue;
                }

                bFile = std::fopen(targetFilePath.c_str(), "rb+");

                // Computes diff & offset
                diff = startEpoch - currTS.firstEpoch;
                offset = diff * cfgStruct.rowlength();
                rangeCount = currTS.lastEpoch - startEpoch;

                // Lock file prior to deleting
                wrLockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);

                // Deallocate data according to N bytes from rowLength
                fallocate64(fileno(bFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfgStruct.rowlength() * rangeCount);
                deleteCount += rangeCount;

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Release lock & Close file after delete
                unlockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);
                std::fclose(bFile);
            }
            else {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string warningMsg = fmt::format("[WARNING] [RANGE DELETE] [DB: {}] - {}.XSTORE does not exist", dbName, currTS.year);
                    toReturn.set_deletestatus(warningMsg);
                    spdlog::warn(warningMsg.substr(WARNING_LEN));

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (endEpochT - startEpoch));
                    continue;
                }

                bFile = std::fopen(targetFilePath.c_str(), "rb+");

                // Computes diff & offset
                diff = startEpoch - currTS.firstEpoch;
                offset = diff * cfgStruct.rowlength();
                rangeCount = endEpochT - startEpoch;

                // Lock file prior to reading
                wrLockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);

                // Deallocate data according to N bytes from rowLength
                fallocate64(fileno(bFile), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, cfgStruct.rowlength() * rangeCount);
                deleteCount += rangeCount;

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Release lock & Close file after delete
                unlockFile(bFile, offset, cfgStruct.rowlength() * rangeCount);
                std::fclose(bFile);
                break;
            }
        }
    }
    std::string infoMsg = fmt::format("[INFO] [RANGE DELETE] [DB: {}] - Deleted {} items", dbName, deleteCount);
    toReturn.set_deletestatus(infoMsg);
    spdlog::info(infoMsg.substr(INFO_LEN));
    return toReturn;
}

XStore_Proto::removeDB_REP XStore::removeDB(std::string dbName) {
    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDB_REP toReturn;

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Location of DB
    std::string dbLoc = fmt::format("{}{}", dataStoreDir, dbName);

    // IF DB does NOT exist
    //  -> Return remove error
    if (!std::filesystem::exists(dbLoc)) {
        removeStatus = fmt::format("[ERROR] [REMOVE DB] [DB: {}] - DB does not exists", dbName);
        toReturn.set_removestatus(removeStatus);
        spdlog::error(removeStatus.substr(ERROR_LEN));
        return toReturn;
    }

    // Recursively remove entire DB with given DB Name
    std::filesystem::remove_all(dbLoc);

    // Remove DB from cfgMap
    cfgMap.erase(dbName);

    removeStatus = fmt::format("[INFO] [REMOVE DB] [DB: {}] - Removed DB with name: {}", dbName, dbName);
    toReturn.set_removestatus(removeStatus);
    spdlog::info(removeStatus.substr(INFO_LEN));
    return toReturn;
}

XStore_Proto::removeDBFiles_REP XStore::removeDBFiles(XStore_Proto::removeDBFiles_REQ *protoFromClient, std::string dbName) {
    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDBFiles_REP toReturn;

    // Ensure dbName string is all capitalized
    toUpperCase(dbName.begin(), dbName.end());

    // Location of DB
    std::string dbLoc = fmt::format("{}{}", dataStoreDir, dbName);

    if (!std::filesystem::exists(dbLoc)) {
        for (int i = 0; i < protoFromClient->filename_size(); i++) {
            removeStatus = fmt::format("[ERROR] [REMOVE DB FILES] [DB: {}] -- Unable to remove {}", dbName, protoFromClient->filename(i));
            toReturn.add_removestatus(removeStatus + protoFromClient->filename(i));
            spdlog::error(removeStatus.substr(ERROR_LEN));
        }
        return toReturn;
    }

    // Loop through each requested file name
    for (int i = 0; i < protoFromClient->filename_size(); i++) {
        std::filesystem::path targetPath = fmt::format("{}{}/{}", dataStoreDir, dbName, protoFromClient->filename(i));
        if (protoFromClient->filename(i) == fmt::format("{}.CFG", dbName)) {
            std::string warnMsg = fmt::format("[WARNING] [REMOVE DB FILES] [DB: {}] - {} is prohibited from remove", dbName, protoFromClient->filename(i));
            toReturn.add_removestatus(warnMsg);
            spdlog::warn(warnMsg.substr(WARNING_LEN));
            continue;
        }
        bool currRemove = std::filesystem::remove(targetPath);
        if (currRemove) {
            std::string infoMsg = fmt::format("[INFO] [REMOVE DB FILES] [DB: {}] - Removed {}", dbName, protoFromClient->filename(i));
            toReturn.add_removestatus(infoMsg);
            spdlog::info(infoMsg.substr(INFO_LEN));
        }
        else {
            std::string errMsg = fmt::format("[ERROR] [REMOVE DB FILES] [DB: {}] - Unable to remove {}", dbName, protoFromClient->filename(i));
            toReturn.add_removestatus(errMsg);
            spdlog::error(errMsg.substr(ERROR_LEN));
        }
    }
    return toReturn;
}

void XStore::initDB() {
    // Loop through each dbName
    for (auto& p : std::filesystem::directory_iterator(dataStoreDir))
    {
        // Full path of each DB
        std::string currDBPath = p.path().string();

        // Extract -- Getting only dbName
        std::string currDBName = currDBPath.erase(0, dataStoreDir.size());

        // Open & Read cfgStruct of all DB currently exists
        std::string cfgFilePath = fmt::format("{}/{}.CFG", p.path().string(), currDBName);

        XStore_Proto::configStruct cfgStruct;
        FILE *cFile = std::fopen(cfgFilePath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
        cfgMap[currDBName] = cfgStruct;
    }
}

std::string XStore::getBaseDir() {
    std::string toReturn;

    // Getting current path of XStore executable
    toReturn = std::filesystem::current_path().parent_path();

    // Extract baseDir for XStore i.e., /home/ubuntu/XStore/
//    toReturn.erase(toReturn.find("XStore") + 7, toReturn.size());
    return toReturn + "/";
}

// Check if there's any existing database
bool XStore::hasDB() {
    // Path to dataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
    const std::filesystem::path tmp_dir{ std::filesystem::path(baseDir + "dataStore")};
    return !std::filesystem::is_empty(tmp_dir);
}

bool XStore::hasDBFile(std::string filePath) {
    struct stat buffer;
    return (stat(filePath.c_str(), &buffer) == 0);
}

void XStore::wrLockFile(FILE *bFile, unsigned long int startRange, unsigned long int length) {
    struct flock64 fl;
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = startRange;
    fl.l_len = (length > 0) ? length + 1 : length;

    if (fcntl64(fileno(bFile), F_SETLKW64, &fl) == -1) {
        if (errno == EACCES || errno == EAGAIN) {
            spdlog::error("[WR LOCK] - Unable to obtain lock");
        }
        else {
            spdlog::error("[WR LOCK] - Unexpected lock error");
        }
    }
}

void XStore::rdLockFile(FILE *bFile, unsigned long int startRange, unsigned long int length) {
    struct flock64 fl;
    fl.l_type = F_RDLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = startRange;
    fl.l_len = (length > 0) ? length + 1 : length;

    if (fcntl64(fileno(bFile), F_SETLKW64, &fl) == -1) {
        if (errno == EACCES || errno == EAGAIN) {
            spdlog::error("[RD LOCK] - Unable to obtain lock");
        }
        else {
            spdlog::error("[RD LOCK] - Unexpected lock error");
        }
    }
}

void XStore::unlockFile(FILE *bFile, unsigned long int startRange, unsigned long int length) {
    struct flock64 fl;
    fl.l_type = F_UNLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = startRange;
    fl.l_len = (length > 0) ? length + 1 : length;

    if (fcntl64(fileno(bFile), F_SETLKW64, &fl) == -1) {
        spdlog::error("[FILE UNLOCK] - Unable to release lock");
    }
}