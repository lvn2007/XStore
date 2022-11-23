#include "include/XStore/XStore.h"

// Initialization
XStore::XStore() {
    // Initialize message
//    initMsg();

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
        configStruct cfgStruct;
        std::string currCfgFilePath = currDBDir + "/" + currDBName + ".CFG";

        // Check if .CFG file is exist
        if (std::filesystem::exists(currCfgFilePath)) {
            int cfgFile = open(currCfgFilePath.c_str(), O_RDONLY);
            pread(cfgFile, &cfgStruct, sizeof(cfgStruct), 0);
            // Set DB Resolution
            currDB->set_dbresolution(cfgStruct.timeResolution);
        }
        else {
            currDB->set_dbresolution("N/A");
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

            //  Add this cfgStruct to cfgMap
            cfgMap[dbName] = cfgStruct;
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

    // Check if cfgMap NOT contains cfgStruct
    configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = dbBasePath + dbName + ".CFG";
        cFile = std::fopen(cfgPath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Enforce time granularity
    if (currTS.timeResolution != cfgStruct.timeResolution) {
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + cfgStruct.timeResolution;
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Prepare to write
    FILE *bFile;
    std::string targetFile = dbBasePath + std::to_string(currTS.year) + ".XSTORE";

    // IF target file does NOT exist
    if (std::filesystem::exists(targetFile) == 0) {
        // Truncate
        bFile = std::fopen(targetFile.c_str(), "wb+");

        // Lock prior to file OP
        flock(fileno(bFile), LOCK_EX);

        ftruncate(fileno(bFile), (currTS.lastEpoch - currTS.firstEpoch) * cfgStruct.rowLength);
    }
    else {
        // ELSE - Target file DOES exist
        //  Open in append mode
        bFile = std::fopen(targetFile.c_str(), "rb+");

        // Lock prior to file OP
        flock(fileno(bFile), LOCK_EX);
    }

    //  -> Organize timestamp & data into an object
    dataStruct currData{unixTime, data};

    //  Computes diff & offset
    long long int diff = unixTime - currTS.firstEpoch;
    long long int offset = diff * cfgStruct.rowLength;

    //  Seek to target offset prior to write
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

XStore_Proto::batchInsert_REP XStore::batchInsert_fTruncate(XStore_Proto::batchInsert_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchInsert_REP toReturn;
    std::string insertStatus;

    // Ensure DB Name string is all capitalized
    std::string dbName = protoFromClient->dbname();
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - DB does not exist";

        // Iterate over batch of insert data & add insert status
        for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
            toReturn.add_insertstatus(errMsg);
        }
        return toReturn;
    }

    // Check if cfgMap NOT contains cfgStruct
    configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & Add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = dbBasePath + dbName + ".CFG";
        cFile = std::fopen(cfgPath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
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
    std::map<unsigned short int, std::vector<unsigned long int>> dataMap;
    std::string granularityErrMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time granularity. Expecting: " + cfgStruct.timeResolution;
    bool granularityErr = false;
    for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
        currTime = protoFromClient->batchinsert_data(i).timestamp();
        epochStruct = gmtime(&currTime);
        dataMap[(epochStruct->tm_year + 1900)].push_back(i);

        // Granularity check
        timeStruct currTS {currTime};
        if (currTS.timeResolution != cfgStruct.timeResolution) {
            toReturn.add_insertstatus(granularityErrMsg);
            granularityErr = true;
        }
    }

    // Halt execution & Return as long as there's an error in time granularity
    if (granularityErr) {
        return toReturn;
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long int>> const& pair : dataMap) {
        unsigned short int yearKey = pair.first;
        FILE *bFile;
        std::string targetFile = dbBasePath + std::to_string(yearKey) + ".XSTORE";

        // Grab first inserting element for timeStruct purposes
        //  Get length of time to truncate
        timeStruct thisTS{(std::time_t)protoFromClient->batchinsert_data().at(dataMap[yearKey].at(0)).timestamp()};

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Truncate
            bFile = std::fopen(targetFile.c_str(), "wb+");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_EX);
            ftruncate(fileno(bFile), (thisTS.lastEpoch - thisTS.firstEpoch) * cfgStruct.rowLength);
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in append mode
            bFile = std::fopen(targetFile.c_str(), "rb+");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_EX);
        }

        long long int diff, offset;
        // Iterate through each YEAR of inserting data
        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Parse each data in a batch collection
            XStore_Proto::batchInsert_DATA currInsertData = protoFromClient->batchinsert_data(dataMap[yearKey].at(i));
            std::time_t unixTime = currInsertData.timestamp();
            std::string data = currInsertData.data();

            // Organize timestamp & data into an object
            dataStruct currData{unixTime, data};

            // Compute diff & offset
            diff = unixTime - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowLength;

            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Writing actual data to disk
            std::fwrite((currData.getData()).data(), cfgStruct.rowLength, 1, bFile);
            std::string successMsg = "[SUCCESS] [UNARY INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(unixTime) + " to " + std::to_string(yearKey) + ".XSTORE";
            toReturn.add_insertstatus(successMsg);
        }

        // Close file & Release lock after write
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
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
        std::string errMsg = "[ERROR] [UNARY QUERY] [DB: " + dbName + "] - " + std::to_string(currTS.year) + ".XSTORE does not exist";
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
    std::string successMsg = "[SUCCESS] [UNARY QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(currTS.year) + ".XSTORE";
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
    configStruct cfgStruct = cfgMap[dbName];

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
    //  .CFG file is set by user when creating DB
    fread(&bufferIn[0], 1, cfgStruct.rowLength, bFile);

    // Close file & release lock after reading
    fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    // Convert to string -- Stripping ending bytes
    std::string returnData(bufferIn);

    // Set data fetched from XStore to returning Protobuf obj
    std::string successMsg = "[SUCCESS] [UNARY QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(currTS.year) + ".XSTORE";
    toReturn.set_querystatus(successMsg);
    toReturn.set_dataset(returnData);
    return toReturn;
}

XStore_Proto::rangeQuery_REP XStore::rangeQuery(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::rangeQuery_REP toReturn;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [RANGE QUERY] [DB: " + dbName +"] - DB does not exist";

        // Iterate over range of query data & add query status
        for (long epochTime = startEpochT; epochTime <= endEpochT; epochTime++) {
            // Initialize entry to add to toReturn
            XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
            currEntry->set_timestamp(epochTime);
            currEntry->set_dataset("");
            currEntry->set_querystatus(errMsg);
            return toReturn;
        }
        toReturn.set_querystatus(errMsg);
    }

    // Check if cfgMap NOT contains cfgStruct
    configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & Add cfgStruct to cfgMap
        std::string cfgPath = dbBasePath + dbName + ".CFG";
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // Time granularity check
    timeStruct startTimeStruct{startEpochT};
    timeStruct endTimeStruct{endEpochT};
    if (startTimeStruct.timeResolution != cfgStruct.timeResolution || endTimeStruct.timeResolution != cfgStruct.timeResolution) {
        std::string granularityErrMsg = "[ERROR] [RANGE QUERY] [DB: " + dbName + "] - Invalid time granularity. Expecting: " + cfgStruct.timeResolution;
        toReturn.set_querystatus(granularityErrMsg);
        return toReturn;
    }

    // Organize into timeStruct
    //  timeStruct provides many auxiliary vars/methods useful for read/write ops
    timeStruct currTS{startEpochT};

    // Placeholder for READ op
    FILE *bFile;
    std::string targetFilePath;
    long long int diff, offset;
    char bufferIn[cfgStruct.rowLength];
    std::string successMsg;

    // IF query range is within 1-year
    if (currTS.lastEpoch >= endEpochT) {
        // Open file prior to reading
        targetFilePath = dbBasePath + std::to_string(currTS.year) + ".XSTORE";
        bFile = std::fopen(targetFilePath.c_str(), "rb");
        successMsg = "[SUCCESS] [RANGE QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(currTS.year) + ".XSTORE";

        // Lock file prior to reading
        flock(fileno(bFile), LOCK_SH);

        // Iterate over range of query data & add queried data and status
        for (long epochTime = startEpochT; epochTime <= endEpochT; epochTime++) {
            // Computes diff & offset
            diff = epochTime - currTS.firstEpoch;
            offset = diff * cfgStruct.rowLength;

            // Seek to computed offset
            fseeko64(bFile, offset, SEEK_SET);

            // Extract N bytes of data according to N bytes from .CFG file
            //  .CFG file is set by user when creating DB
            fread(&bufferIn[0], 1, cfgStruct.rowLength, bFile);

            // Add fetched data to returning Protobuf obj
            XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
            currEntry->set_timestamp(epochTime);
            currEntry->set_dataset(std::string(bufferIn));
            currEntry->set_querystatus(successMsg);
        }

        // Close & Release file lock after reading
        fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
    }
    // Otherwise -> Involves multi-year
    else {
        // Make a copy of start epoch time for incrementing
        std::time_t startEpoch = startEpochT;

        // Loop through each year individually
        while (startEpoch < endEpochT) {
            targetFilePath = dbBasePath + std::to_string(currTS.year) + ".XSTORE";
            successMsg = "[SUCCESS] [RANGE QUERY] [DB: " + dbName + "] - Acquired " + std::to_string(currTS.year) + ".XSTORE";

            // IF the requested start epoch spans ONLY 1 year
            if (currTS.lastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string errMsg = "[ERROR] [RANGE QUERY] [DB: " + dbName + "] - " + std::to_string(currTS.year) + ".XSTORE does not exist";

                    // Iterate over range of query data & add query status
                    for (long long int epochTime = startEpoch; epochTime <= currTS.lastEpoch; epochTime++, startEpoch++) {
                        // Initialize entry to add to toReturn
                        XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
                        currEntry->set_timestamp(epochTime);
                        currEntry->set_dataset("");
                        currEntry->set_querystatus(errMsg);
                    }

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch);
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_SH);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime <= currTS.lastEpoch; epochTime++, startEpoch++) {
                    // Computes diff & offset
                    diff = epochTime - currTS.firstEpoch;
                    offset = diff * cfgStruct.rowLength;

                    // Seek to computed offset
                    fseeko64(bFile, offset, SEEK_SET);

                    // Extract N bytes of data according to N bytes from .CFG file
                    //  .CFG file is set by user when creating DB
                    fread(&bufferIn[0], 1, cfgStruct.rowLength, bFile);

                    // Add fetched data to returning Protobuf obj
                    XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
                    currEntry->set_timestamp(epochTime);
                    currEntry->set_dataset(std::string(bufferIn));
                    currEntry->set_querystatus(successMsg);
                }

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Close file & Release lock after write
                std::fclose(bFile);
                flock(fileno(bFile), LOCK_UN);
            }
            else {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    std::string errMsg = "[ERROR] [RANGE QUERY] [DB: " + dbName + "] - " + std::to_string(currTS.year) + ".XSTORE does not exist";

                    // Iterate over range of query data & add query status
                    for (long long int epochTime = startEpoch; epochTime < endEpochT; epochTime++, startEpoch++) {
                        // Initialize entry to add to toReturn
                        XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
                        currEntry->set_timestamp(epochTime);
                        currEntry->set_dataset("");
                        currEntry->set_querystatus(errMsg);
                    }

                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch);
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_SH);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime < endEpochT; epochTime++, startEpoch++) {
                    // Computes diff & offset
                    diff = epochTime - currTS.firstEpoch;
                    offset = diff * cfgStruct.rowLength;

                    // Seek to computed offset
                    fseeko64(bFile, offset, SEEK_SET);

                    // Extract N bytes of data according to N bytes from .CFG file
                    //  .CFG file is set by user when creating DB
                    fread(&bufferIn[0], 1, cfgStruct.rowLength, bFile);

                    // Add fetched data to returning Protobuf obj
                    XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
                    currEntry->set_timestamp(epochTime);
                    currEntry->set_dataset(std::string(bufferIn));
                    currEntry->set_querystatus(successMsg);
                }

                // Update currTS with latest startEpoch after incrementing
                currTS = timeStruct(startEpoch);

                // Close file & Release lock after read
                std::fclose(bFile);
                flock(fileno(bFile), LOCK_UN);
                break;
            }
        }
    }

    // Set query status msg
    queryStatus = "[SUCCESS] [BATCH QUERY] [DB: " + dbName + "] - Concluded Batch queries";
    toReturn.set_querystatus(queryStatus);
    return toReturn;
}

XStore_Proto::removeDB_REP XStore::removeDB(std::string dbName) {
    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDB_REP toReturn;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Location of DB
    std::string dbLoc = dataStoreDir + dbName;

    // IF DB does NOT exist
    //  -> Return remove error
    if (!std::filesystem::exists(dbLoc)) {
        removeStatus = "[ERROR] [REMOVE DB] [DB: " + dbName + "] - Does not exists";
        toReturn.set_removestatus(removeStatus);
        return toReturn;
    }

    // Recursively remove entire DB with given DB Name
    std::filesystem::remove_all(dbLoc);

    // Remove DB from cfgMap
    cfgMap.erase(dbName);
    toReturn.set_removestatus("[SUCCESS] [REMOVE DB] [DB: " + dbName + "] - Removed DB with name: " + dbName);
    return toReturn;
}

XStore_Proto::removeDBFiles_REP XStore::removeDBFiles(XStore_Proto::removeDBFiles_REQ *protoFromClient, std::string dbName) {
    // Protobuf obj to be returned
    std::string removeStatus;
    XStore_Proto::removeDBFiles_REP toReturn;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Location of DB
    std::string dbLoc = dataStoreDir + dbName;

    if (!std::filesystem::exists(dbLoc)) {
        removeStatus = "[ERROR] [REMOVE DB FILES] [DB: " + dbName + "] -- Unable to remove ";
        for (int i = 0; i < protoFromClient->filename_size(); i++) {
            toReturn.add_removestatus(removeStatus + protoFromClient->filename(i));
        }
        return toReturn;
    }

    // Loop through each requested file name
    for (int i = 0; i < protoFromClient->filename_size(); i++) {
        std::filesystem::path targetPath = dataStoreDir + dbName + "/" + protoFromClient->filename(i);
        if (protoFromClient->filename(i) == (dbName + ".CFG")) {
            toReturn.add_removestatus("[ERROR] [REMOVE DB FILES] [DB: " + dbName + "] - " + protoFromClient->filename(i) + " is prohibited to remove");
            continue;
        }
        bool currRemove = std::filesystem::remove(targetPath);
        if (currRemove) {
            toReturn.add_removestatus("[SUCCESS] [REMOVE DB FILES] [DB: " + dbName + "] - Removed " + protoFromClient->filename(i));
        }
        else {
            toReturn.add_removestatus("[ERROR] [REMOVE DB FILES] [DB: " + dbName + "] - Unable to remove " + protoFromClient->filename(i));
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
        std::string cfgFilePath = p.path().string() + "/" + currDBName + ".CFG";
        configStruct cfgStruct;
        FILE *cFile = std::fopen(cfgFilePath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
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