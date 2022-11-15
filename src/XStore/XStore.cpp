#include "include/XStore/XStore.h"

// Initialization
XStore::XStore() {
    // Initialize message
//    initMsg();

    // Initialize DB
    initDB();

    // First, check if there's any existing data exists
    hasDB();
}

XStore_Proto::sysInfo_REP XStore::infoQuery(std::string serverAddr, int threadCount) {
    // Protobuf obj to be returned
    XStore_Proto::sysInfo_REP toReturn;

    // Overview info
    toReturn.set_serveraddr(serverAddr);
    toReturn.set_threadcount(threadCount);

    // Location of dataStore
    std::string dataStoreLoc = baseDir + "dataStore/";

    // Loop through all db available in dataStore
    for (auto& dir : std::filesystem::directory_iterator(dataStoreLoc)) {
        std::string currDBName = dir.path().filename().string();
        std::string currDBDir = dataStoreLoc + currDBName;

        // Set DB Name
        XStore_Proto::dbStats *currDB = toReturn.add_dbinfo();
        currDB->set_dbname(currDBName);

        // Fetch config of a target DB
//        configStruct cfgStruct;
//        std::string currCfgFilePath = currDBDir + "/" + currDBName + ".config";
//        int cfgFile = open(currCfgFilePath.c_str(), O_RDONLY);
//        pread(cfgFile, &cfgStruct, sizeof(cfgStruct), 0);

        // Set DB Resolution
        currDB->set_dbresolution("SECOND");

        // Size of all data files in current DB
        unsigned long int allSize = 0;

//        for (const std::filesystem::)

        // Loop through sub-dirs and
        for (auto& filePath : std::filesystem::recursive_directory_iterator(currDBDir)) {
            if (!filePath.is_directory()) {
                unsigned long int currFileSizeInGB = std::filesystem::file_size(filePath.path());
                allSize += currFileSizeInGB;
            }
        }
        currDB->set_totalsizeinbyte(allSize);

//        // Loop through all files in currentDBDir
//        for (auto& dbDir : std::filesystem::directory_iterator(currDBDir)) {
//            // Current file name & path in dbDir
//            std::string currFileName = dbDir.path().filename().string();
//
//            // Count only data file -- Ignore .config file(s)
//            //  IF current file name contains ".config"
//            //   -> Skip to next iteration
//            if (currFileName.find(".config") != std::string::npos) {
//                continue;
//            }
//
//            // Fetch current file size & convert to GB
//            double currFileSizeInGB = static_cast<double>(file_size(dbDir.path())) / 1024 / 1024 / 1024;
//            allSize += currFileSizeInGB;
//            currDB->add_filename(currFileName);
//        }
//        currDB->set_totalsizeinbyte(allSize);
    }

    return toReturn;
}

XStore_Proto::createDB_REP XStore::createDB(std::string dbName, std::string timeResolution) {
    // Protobuf obj to be returned
    XStore_Proto::createDB_REP toReturn;
    std::string createDBStatus;

    // Ensure dbName & timeResolution string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    for (int i = 0; i < timeResolution.size(); i++) {
        timeResolution[i] = toupper(timeResolution[i]);
    }

    // Target directory of a given DB name
    std::string targetPath = baseDir + "dataStore/" + dbName + "/";

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(permittedTimeResolution), std::end(permittedTimeResolution), timeResolution) == std::end(permittedTimeResolution)) {
            createDBStatus = "[ERROR] [CREATE DB] [DB: " + dbName + "] - Invalid time resolution";
            toReturn.set_createdbstatus(createDBStatus);
            return toReturn;
        }

        // Create DB directory
        if (std::filesystem::create_directory(targetPath) == 1) {
            std::string successMsg = "[SUCCESS] [CREATE DB] [DB: " + dbName + "] - Created DB";
            toReturn.set_createdbstatus(successMsg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            configStruct cfgStruct;
            strcpy(cfgStruct.dbName, dbName.c_str());
            strcpy(cfgStruct.timeResolution, timeResolution.c_str());

            // Writing cfgStruct to file
            FILE *bFile;
            std::string cfgPath  = targetPath + dbName + ".CFG";
            bFile = std::fopen(cfgPath.c_str(), "wb+");

            // Lock prior to writing
            flock(fileno(bFile), LOCK_EX);

            // Writing actual data to disk
            std::fwrite(&cfgStruct, sizeof(struct configStruct), 1, bFile);

            //  Close file & Release lock after write
            std::fclose(bFile);
            flock(fileno(bFile), LOCK_UN);

            return toReturn;
        }
    }

    createDBStatus = "[ERROR] [CREATE DB] [DB: " + dbName + "] - Already existed";
    toReturn.set_createdbstatus(createDBStatus);
    return toReturn;
}

XStore_Proto::createDB_REP XStore::createDB_fTruncate(std::string dbName, std::string timeResolution, short int rowLength) {
    // Protobuf obj to be returned
    XStore_Proto::createDB_REP toReturn;
    std::string createDBStatus;

    // Ensure dbName & timeResolution string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    for (int i = 0; i < timeResolution.size(); i++) {
        timeResolution[i] = toupper(timeResolution[i]);
    }

    // Target directory of a given DB name
    std::string targetPath = baseDir + "dataStore/" + dbName + "/";

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(permittedTimeResolution), std::end(permittedTimeResolution), timeResolution) == std::end(permittedTimeResolution)) {
            createDBStatus = "[ERROR] [CREATE DB] [DB: " + dbName + "] - Invalid time resolution";
            toReturn.set_createdbstatus(createDBStatus);
            return toReturn;
        }

        // Create DB directory
        if (std::filesystem::create_directory(targetPath) == 1) {
            std::string successMsg = "[SUCCESS] [CREATE DB] [DB: " + dbName + "] - Created DB";
            toReturn.set_createdbstatus(successMsg);

            // Dumps DB configurations info to disk
            //  Organize configs into a struct
            configStruct cfgStruct;
            strcpy(cfgStruct.dbName, dbName.c_str());
            strcpy(cfgStruct.timeResolution, timeResolution.c_str());
            cfgStruct.rowLength = rowLength;

            // Writing cfgStruct to file
            FILE *bFile;
            std::string cfgPath  = targetPath + dbName + ".CFG";
            bFile = std::fopen(cfgPath.c_str(), "wb+");

            // Lock prior to writing
            flock(fileno(bFile), LOCK_EX);

            // Writing actual data to disk
            std::fwrite(&cfgStruct, sizeof(struct configStruct), 1, bFile);

            //  Close file & Release lock after write
            std::fclose(bFile);
            flock(fileno(bFile), LOCK_UN);

            return toReturn;
        }
    }

    createDBStatus = "[ERROR] [CREATE DB] [DB: " + dbName + "] - Already existed";
    toReturn.set_createdbstatus(createDBStatus);
    return toReturn;
}

// INSERT time-series oriented values
XStore_Proto::unaryInsert_REP XStore::unaryInsert(std::time_t unixTime, std::string data, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::unaryInsert_REP toReturn;
    std::string insertStatus;

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    timeStruct currTS{unixTime};

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Enforce time resolution
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";
    std::string configPath = dbBasePath + dbName + ".CFG";
    configStruct cfgStruct;
    FILE *cFile;
    cFile = std::fopen(configPath.c_str(), "rb");
    fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
    std::fclose(cFile);

    if (currTS.timeResolution != cfgStruct.timeResolution) {
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + cfgStruct.timeResolution;
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Target path of file to WRITE
    std::string targetPath = dbBasePath + std::to_string(currTS.year) + "/" + monthArray[currTS.month - 1] + dayArray[currTS.day - 1] + hourArray[currTS.hour] + minArray[currTS.minute];

    // IF hierarchical year folder DOES NOT exist
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Create hierarchical directories
        if (std::filesystem::create_directories(targetPath) == 0) {
            std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Unable to create hierarchical directories";
            toReturn.set_insertstatus(errMsg);
            return toReturn;
        }
    }

    // Staging area for new data file waiting to be inserted
    FILE *bFile;
    targetPath = targetPath + std::to_string(unixTime) + ".XSTORE";
    bFile = std::fopen(targetPath.c_str(), "wb+");

    // Lock prior to writing
    flock(fileno(bFile), LOCK_EX);

    // IF no error
    // -> Organize timestamp & data into an object
    dataStruct currData{unixTime, data};

    // Writing actual data to disk
    std::fwrite((currData.getData()).data(), data.size(), 1, bFile);

    //  Close file & Release lock after write
    std::fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    std::string successMsg = "[SUCCESS] [UNARY INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(unixTime) + ".XSTORE";
    toReturn.set_insertstatus(successMsg);
    return toReturn;
}

XStore_Proto::unaryInsert_REP XStore::unaryInsert_fTruncate(std::time_t unixTime, std::string data, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::unaryInsert_REP toReturn;
    std::string insertStatus;

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    timeStruct currTS{unixTime};

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Enforce time resolution
    std::string configPath = dbBasePath + dbName + ".CFG";
    configStruct cfgStruct;
    FILE *cFile;
    cFile = std::fopen(configPath.c_str(), "rb");
    fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
    std::fclose(cFile);

    if (currTS.timeResolution != cfgStruct.timeResolution) {
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + cfgStruct.timeResolution;
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Prepare to write
    FILE *bFile;
    std::string targetFile = dbBasePath + std::to_string(currTS.year) + ".XSTORE";

    // Lock prior to file OP
    flock(fileno(bFile), LOCK_EX);

    // IF target file does NOT exist
    if (std::filesystem::exists(targetFile) == 0) {
        // Truncate
        bFile = std::fopen(targetFile.c_str(), "wb+");
        ftruncate(fileno(bFile), (currTS.lastEpoch - currTS.firstEpoch) * cfgStruct.rowLength);
    }

    // ELSE - Target file DOES exist
    //  -> Organize timestamp & data into an object
    dataStruct currData{unixTime, data};

    //  Computes diff & offset
    long long int diff = unixTime - currTS.firstEpoch;
    long long int offset = diff * cfgStruct.rowLength;

    //  Seek to target offset prior to write
    bFile = std::fopen(targetFile.c_str(), "rb+");
    fseeko64(bFile, offset, SEEK_SET);

    //  Writing actual data to disk
    std::fwrite((currData.getData()).data(), cfgStruct.rowLength, 1, bFile);

    //  Close file & Release lock after write
    std::fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    std::string successMsg = "[SUCCESS] [UNARY INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(unixTime) + ".XSTORE";
    toReturn.set_insertstatus(successMsg);
    return toReturn;
}

XStore_Proto::batchInsert_REP XStore::batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchInsert_REP toReturn;
    std::string insertStatus;
    configStruct cfgStruct;

    // Iterate over batch of insert data
    for (int idx = 0; idx < protoFromClient->batchinsert_data_size(); idx++) {
        // Parse each data in a batch collection
        XStore_Proto::batchInsert_DATA currentInsertData = protoFromClient->batchinsert_data(idx);
        std::time_t unixTime = currentInsertData.timestamp();
        std::string data = currentInsertData.data();
        std::string dbName = currentInsertData.dbname();

        // Time structure contains all components per UNIXTIME:
        //  Year, month, day, hour, minute, sec, ...
        timeStruct currTS{unixTime};

        // Check whether dbName is already created
        //  Ensure dbName string is all capitalized
        for (int i = 0; i < dbName.size(); i++) {
            dbName[i] = toupper(dbName[i]);
        }

        std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

        // ONLY read config file when necessary
        if (cfgStruct.dbName != dbName) {
            FILE *cFile;
            std::string configPath = dbBasePath + dbName + ".CFG";
            cFile = std::fopen(configPath.c_str(), "rb");
            fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
            std::fclose(cFile);
        }

        // Enforce time resolution
        if (currTS.timeResolution != cfgStruct.timeResolution) {
            std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + cfgStruct.timeResolution;
            toReturn.add_insertstatus(errMsg);
            continue;
        }

        // Target path of file to WRITE
        std::string targetPath = dbBasePath + std::to_string(currTS.year) + "/" + monthArray[currTS.month - 1] + dayArray[currTS.day - 1] + hourArray[currTS.hour] + minArray[currTS.minute];

        // IF hierarchical year folder DOES NOT exist
        if (std::filesystem::is_directory(targetPath) == 0) {
            // Create hierarchical directories
            if (std::filesystem::create_directories(targetPath) == 0) {
                std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Unable to create hierarchical directories";
                toReturn.add_insertstatus(errMsg);
                continue;
            }
        }

        // Staging area for new data file waiting to be inserted
        FILE *bFile;
        targetPath = targetPath + std::to_string(unixTime) + ".XSTORE";
        bFile = std::fopen(targetPath.c_str(), "wb+");

        // Lock prior to writing
        flock(fileno(bFile), LOCK_EX);

        // IF no error
        // -> Organize timestamp & data into an object
        dataStruct currData{unixTime, data};

        // Writing actual data to disk
        std::fwrite((currData.getData()).data(), data.size(), 1, bFile);

        //  Close file & Release lock after write
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);

        std::string successMsg = "[SUCCESS] [UNARY INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(unixTime) + ".XSTORE";
        toReturn.add_insertstatus(successMsg);
    }
    return toReturn;
}

XStore_Proto::unaryQuery_REP XStore::unaryQuery(std::time_t unixTime, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::unaryQuery_REP toReturn;
    toReturn.set_timestamp(unixTime);
    toReturn.set_dataset("NULL");

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    timeStruct currTS{unixTime};

    // Target path of file to READ
    std::string dataFilePath = baseDir + "dataStore/" + dbName + "/" + std::to_string(currTS.year) + "/";
    std::string targetPath = dataFilePath + monthArray[currTS.month - 1] + dayArray[currTS.day - 1] + hourArray[currTS.hour] + minArray[currTS.minute] + std::to_string(unixTime) + ".XSTORE";

    // IF hierarchical year folder DOES NOT exist
    if (std::filesystem::exists(targetPath) == 0) {
        std::string errMsg = "[ERROR] [UNARY QUERY] [DB: " + dbName + "] - " + std::to_string(unixTime) + ".XSTORE does not exist";
        toReturn.set_querystatus(errMsg);
        return toReturn;
    }

    // Target file IS EXISTED
    //  Staging area for file waiting to be read
    FILE *bFile;
    bFile = std::fopen(targetPath.c_str(), "rb");

    //  Lock file prior to reading
    flock(fileno(bFile), LOCK_SH);

    // Obtain file size
    fseek(bFile, 0, SEEK_END);
    int fSize = ftell(bFile);
    rewind(bFile);

    // A single line data will be read into here
    char bufferIn[fSize];

    // Extract line of data from binary file
    fread(&bufferIn[0], 1, fSize, bFile);

    // Close file & release lock after reading
    fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    // Convert to string -- Stripping ending bytes
    std::string returnData(bufferIn, fSize);

    // Set data fetched from XStore to returning Protobuf obj
    std::string successMsg = "[SUCCESS] [UNARY QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(unixTime) + ".XSTORE";
    toReturn.set_querystatus(successMsg);
    toReturn.set_dataset(returnData);
    return toReturn;
}

XStore_Proto::unaryQuery_REP XStore::unaryQuery_fTruncate(std::time_t unixTime, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::unaryQuery_REP toReturn;
    toReturn.set_timestamp(unixTime);
    toReturn.set_dataset("NULL");

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    timeStruct currTS{unixTime};

    // Base directory of a DB & Target path of file to READ
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";
    std::string targetFilePath = dbBasePath + std::to_string(currTS.year) + ".XSTORE";

    // IF hierarchical year folder DOES NOT exist
    if (std::filesystem::exists(targetFilePath) == 0) {
        std::string errMsg = "[ERROR] [UNARY QUERY] [DB: " + dbName + "] - " + std::to_string(currTS.year) + ".XSTORE does not exist";
        toReturn.set_querystatus(errMsg);
        return toReturn;
    }

    // Enforce time resolution
    std::string configPath = dbBasePath + dbName + ".CFG";
    configStruct cfgStruct;
    FILE *cFile;
    cFile = std::fopen(configPath.c_str(), "rb");
    fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
    std::fclose(cFile);

    if (currTS.timeResolution != cfgStruct.timeResolution) {
        std::string errMsg = "[ERROR] [UNARY QUERY] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + cfgStruct.timeResolution;
        toReturn.set_querystatus(errMsg);
        return toReturn;
    }

    // Target file IS EXISTED
    //  Staging area for file waiting to be read
    FILE *bFile;
    bFile = std::fopen(targetFilePath.c_str(), "rb");

    //  Lock file prior to reading
    flock(fileno(bFile), LOCK_SH);

    // Obtain file size
    //  Computes diff & offset
    long long int diff = unixTime - currTS.firstEpoch;
    long long int offset = diff * cfgStruct.rowLength;

    //  A single line data will be read into here
    char bufferIn[cfgStruct.rowLength];

    //  Seek to computed offset
    fseeko64(bFile, offset, SEEK_SET);

    // Extract N bytes of data according to N bytes from .CFG file
    //  .CFG file is set by user when creating db
    int bytesRead = fread(&bufferIn[0], 1, cfgStruct.rowLength, bFile);

    // Close file & release lock after reading
    fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    // Convert to string -- Stripping ending bytes
    std::string returnData(bufferIn);

    // Set data fetched from XStore to returning Protobuf obj
    std::string successMsg = "[SUCCESS] [UNARY QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(unixTime) + ".XSTORE";
    toReturn.set_querystatus(successMsg);
    toReturn.set_dataset(returnData);
    return toReturn;
}

XStore_Proto::batchQuery_REP XStore::batchQuery(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::batchQuery_REP toReturn;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Loop through all query range
    for (int unixTime = startEpochT; unixTime <= endEpochT; unixTime++) {
        // Initialize entry to return
        XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
        currEntry->set_timestamp(unixTime);
        currEntry->set_dataset("NULL");

        //  Time structure contains all components per UNIXTIME:
        //      Year, month, day, hour, minute, sec, ...
        timeStruct currTS{unixTime};

        // Target path of file to READ
        std::string dataFilePath = baseDir + "dataStore/" + dbName + "/" + std::to_string(currTS.year) + "/";
        std::string targetPath = dataFilePath + monthArray[currTS.month - 1] + dayArray[currTS.day - 1] + hourArray[currTS.hour] + minArray[currTS.minute] + std::to_string(unixTime) + ".XSTORE";

        if (std::filesystem::exists(targetPath) == 0) {
            queryStatus = "[ERROR] [BATCH QUERY] [DB: " + dbName + "] - " + std::to_string(unixTime) + ".XSTORE does not exist";
            currEntry->set_querystatus(queryStatus);
            continue;
        }

        // Target file IS EXISTED
        //  Staging area for file waiting to be read
        FILE *bFile;
        bFile = std::fopen(targetPath.c_str(), "rb");

        //  Lock file prior to reading
        flock(fileno(bFile), LOCK_SH);

        // Obtain file size
        fseek(bFile, 0, SEEK_END);
        int fSize = ftell(bFile);
        rewind(bFile);

        // A single line data will be read into here
        char bufferIn[fSize];

        // Extract line of data from binary file
        fread(&bufferIn[0], 1, fSize, bFile);

        // Close file & release lock after reading
        fclose(bFile);
        flock(fileno(bFile), LOCK_UN);

        // Convert to string -- Stripping ending bytes
        std::string returnData(bufferIn, fSize);

        // Set data fetched from XStore to returning Protobuf obj
        queryStatus = "[SUCCESS] [BATCH QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(unixTime) + ".XSTORE";
        currEntry->set_querystatus(queryStatus);
        currEntry->set_dataset(returnData);
    }

    // Set query status msg
    queryStatus = "[SUCCESS] [BATCH QUERY] [DB: " + dbName + "] - Concluded Batch queries";
    toReturn.set_querystatus(queryStatus);
    return toReturn;
}

XStore_Proto::removeDB_REP XStore::removeDB(std::string dbName) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDB_REP toReturn;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    if (dbMap.find(dbName) == dbMap.end()) {
        removeStatus = "[ERROR] -- DB with name: " + dbName + " does not exists";
        toReturn.set_removestatus(removeStatus);
        return toReturn;
    }

    // Recursively remove entire DB with given DB Name
    std::filesystem::path targetPath = dataStoreDir + dbName;
    std::filesystem::remove_all(targetPath);

    toReturn.set_removestatus("[SUCCESS] -- Removed DB with name: " + dbName);
}

XStore_Proto::removeDBFiles_REP XStore::removeDBFiles(std::string dbName, XStore_Proto::removeDBFiles_REQ *protoFromClient) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDBFiles_REP toReturn;

    if (dbMap.find(dbName) == dbMap.end()) {
        std::string errorStatus = "[ERROR] - [DB: " + dbName + "] -- Unable to remove ";
        for (int i = 0; i < protoFromClient->filename_size(); i++) {
            toReturn.add_removestatus(errorStatus + protoFromClient->filename(i));
        }
        return toReturn;
    }

    // Loop through each requested file name
    for (int i = 0; i < protoFromClient->filename_size(); i++) {
        std::filesystem::path targetPath = dataStoreDir + dbName + "/" + protoFromClient->filename(i);
        bool currRemove = std::filesystem::remove(targetPath);
        if (currRemove) {
            toReturn.add_removestatus("[SUCCESS] - [DB: " + dbName + "] -- Removed " + protoFromClient->filename(i));
        }
        else {
            toReturn.add_removestatus("[ERROR] - [DB: " + dbName + "] -- Unable to remove " + protoFromClient->filename(i));
        }
    }
    return toReturn;
}

void XStore::initDB() {
    // Directory contains all data file for a specific DB
    std::filesystem::path dbNames = baseDir + "dataStore/";

    dbMap.clear();

    // Loop through each dbName
    for (auto& p : std::filesystem::directory_iterator(dbNames))
    {
        // Full path of each DB
        std::string currDBName = p.path().string();

        // Extract -- Getting only dbName
        currDBName = currDBName.erase(0, dbNames.string().size());

        // Initialize fileStruct -- Storing file descriptor
        fileStruct fStruct(currDBName, baseDir);

        // Insert fileStruct for each recognized dbName
        dbMap.insert(std::make_pair(currDBName, fStruct));
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

void XStore::initMsg() {
    std::cout << "---- XStore Initialized ----" << std::endl;
    dbStats();
    std::cout << " - DB Count: " <<  dbCount << std::endl;

    std::cout << " - DB Names: [ ";
    for (int i = 0; i < dbNames.size(); i++) {
        if (i + 1 != dbNames.size()) {
            std::cout << dbNames.at(i) << ", ";
            continue;
        }
        std::cout << dbNames.at(i);
    }
    std::cout << " ]" << std::endl;
    std::cout << "----------------------------" << std::endl;
}

void XStore::dbStats() {
    // Path of dataStore
    std::filesystem::path dbDir = baseDir + "dataStore";
    initDB();

    // Reset dbCount & dbNames list
    dbCount = 0;
    dbNames.clear();

    for (auto& p : std::filesystem::directory_iterator(dbDir))
    {
        dbCount++;
        dbNames.push_back(p.path().filename());
    }
}

//bool XStore::yearExists(int inputYear, std::string dbName) {
//    std::string path = baseDir + "dataStore" + dbName;
//
//    for (const auto & entry : std::filesystem::directory_iterator(path)) {
//        std::cout << entry.path();
//    }
//}

// Returning epoch value of FIRST available datetime of a given year
long int XStore::getStartTimeEpoch(int yearInput, std::string timeResolution) {
    std::map<std::string, int> resolutionMap { {"SECOND", 1}, {"MILLISECOND", 1000}, {"MICROSECOND", 1000000} };
    struct tm epochConst = {0};
    epochConst.tm_year = yearInput-1900;
    epochConst.tm_mon = 0;
    epochConst.tm_mday = 0;
    epochConst.tm_hour = 24;
    epochConst.tm_min = 0;
    epochConst.tm_sec = 0;
    time_t startTime = mktime(&epochConst);
    long long int startEpoch = static_cast<long long int> (startTime);
    return startEpoch * resolutionMap[timeResolution];
}

// Returning epoch value of LAST available datetime of a given year
long int XStore::getEndTimeEpoch(int yearInput, std::string timeResolution) {
    std::map<std::string, int> resolutionMap { {"SECOND", 1}, {"MILLISECOND", 1000}, {"MICROSECOND", 1000000} };
    struct tm epochConst = {0};
    epochConst.tm_year = yearInput-1900;
    epochConst.tm_mon = 12-1;
    epochConst.tm_mday = 31;
    epochConst.tm_hour = 23;
    epochConst.tm_min = 59;
    epochConst.tm_sec = 59;
    time_t startTime = mktime(&epochConst);
    long long int startEpoch = static_cast<long long int> (startTime);
    return startEpoch * resolutionMap[timeResolution];
}

// Time extractor
std::string XStore::getTimeResolution(std::time_t unixTime) {
    int count = 0;

    while (unixTime != 0) {
        unixTime /= 10;
        ++count;
    }

    if (count >= 10 && count < 13) {
        return "SECOND";
    }
    else if (count >= 13 && count < 16) {
        return "MILLISECOND";
    }
    else if (count >= 16) {
        return "MICROSECOND";
    }

    return "NULL";
}

unsigned short int XStore::toHumanYear(std::time_t &unixTime, std::string &timeResolution) {
    tm* timePtr;
    if (timeResolution == "SECOND") {
        timePtr = localtime(&unixTime);
    }
    else if (timeResolution == "MILLISECOND") {
        std::time_t choppedTime = unixTime / 1000;
        timePtr = localtime(&choppedTime);
    }
    else if (timeResolution == "MICROSECOND") {
        std::time_t choppedTime = unixTime / 1000000;
        timePtr = localtime(&choppedTime);
    }
    std::cout << timePtr->tm_mon << std::endl;
    return timePtr->tm_year + 1900;
}