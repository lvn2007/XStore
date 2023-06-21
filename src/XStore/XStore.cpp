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
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    if (rowLength < 4) {
        createDBStatus = "[ERROR] [CREATE DB] [DB: " + dbName + "] - Row length must be >= 4";
        toReturn.set_createdbstatus(createDBStatus);
        return toReturn;
    }

    // Target directory of a given DB name
    std::string targetPath = baseDir + "dataStore/" + dbName + "/";

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Check if within permitted time resolution bounds
        if (std::find(std::begin(permittedTimeResolution), std::end(permittedTimeResolution), XStore_Proto::timeUnit_Name(timeResolution)) == std::end(permittedTimeResolution)) {
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
            XStore_Proto::configStruct cfgStruct;
            cfgStruct.set_dbname(dbName);
            cfgStruct.set_timeresolution(timeResolution);
            cfgStruct.set_rowlength(rowLength);

            // Writing cfgStruct to file
            FILE *bFile;
            std::string cfgPath  = targetPath + dbName + ".CFG";
            bFile = std::fopen(cfgPath.c_str(), "wb+");

            // Lock prior to writing
            flock(fileno(bFile), LOCK_EX);

            // Writing actual data to disk
            cfgStruct.SerializeToFileDescriptor(fileno(bFile));

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

// UNARY INSERT
XStore_Proto::unaryInsert_REP XStore::unaryInsert(std::time_t unixTime, XStore_Proto::rowData data, std::string dbName) {
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
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        // Read & add cfgStruct to cfgMap
        FILE *cFile;
        std::string cfgPath = dbBasePath + dbName + ".CFG";
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
        std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + expectedTimeResolution;
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
        ftruncate(fileno(bFile), (currTS.lastEpoch - currTS.firstEpoch) * cfgStruct.rowlength());
    }
    else {
        // ELSE - Target file DOES exist
        //  Open in append mode
        bFile = std::fopen(targetFile.c_str(), "rb+");

        // Lock prior to file OP
        flock(fileno(bFile), LOCK_EX);
    }

    //  Computes diff & offset
    long long int diff = unixTime - currTS.firstEpoch;
    long long int offset = diff * cfgStruct.rowlength();

    //  Seek to target offset prior to write
    fseeko64(bFile, offset, SEEK_SET);

    //  Writing actual data to disk
    fwrite(data.SerializeAsString().data(), cfgStruct.rowlength(), 1, bFile);

    //  Close file & Release lock after write
    std::fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    std::string successMsg = "[SUCCESS] [UNARY INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(unixTime) + " to " + std::to_string(currTS.year) + ".XSTORE";
    toReturn.set_insertstatus(successMsg);
    return toReturn;
}

// UNARY QUERY
XStore_Proto::unaryQuery_REP XStore::unaryQuery(std::time_t unixTime, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::unaryQuery_REP toReturn;
    toReturn.set_timestamp(unixTime);

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

    // IF target year file DOES NOT exist
    if (std::filesystem::exists(targetFilePath) == 0) {
        return toReturn;
    }

    // Enforce time resolution
    std::string configPath = dbBasePath + dbName + ".CFG";
    XStore_Proto::configStruct cfgStruct = cfgMap[dbName];

    if (currTS.timeResolution != XStore_Proto::timeUnit_Name(cfgStruct.timeresolution())) {
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
    long long int offset = diff * cfgStruct.rowlength();

    //  A single line data will be read into here
    char bufferIn[cfgStruct.rowlength()];
    XStore_Proto::rowData tempData;

    //  Seek to computed offset
    fseeko64(bFile, offset, SEEK_SET);

    // Extract N bytes of data according to N bytes from .CFG file
    //  .CFG file is set by user when creating DB
    fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

    // Close file & release lock after reading
    fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    // Set data fetched from XStore to returning Protobuf obj
    tempData.ParseFromArray(&bufferIn, cfgStruct.rowlength());
    *toReturn.mutable_data() = tempData;

    return toReturn;
}

// UNARY DELETE
XStore_Proto::unaryDelete_REP XStore::unaryDelete(std::time_t epochTime, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::unaryDelete_REP toReturn;
    std::string deleteStatus;

    // Time structure contains all components per UNIXTIME:
    //  Year, month, day, hour, minute, sec, ...
    timeStruct currTS{epochTime};

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [UNARY DELETE] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
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
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (currTS.timeResolution != expectedTimeResolution) {
        std::string errMsg = "[ERROR] [UNARY DELETE] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + expectedTimeResolution;
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // Prepare to delete (Writing NULL byte)
    FILE *bFile;
    std::string targetFileName = std::to_string(currTS.year) + ".XSTORE";
    std::string targetFilePath = dbBasePath + targetFileName;

    // IF target file does NOT exist
    if (std::filesystem::exists(targetFilePath) == 0) {
        std::string errMsg = "[ERROR] [UNARY DELETE] [DB: " + dbName + "] - " + targetFileName + " does not exist";
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // OTHERWISE - Target file DOES exist
    //  Open in append mode
    bFile = std::fopen(targetFilePath.c_str(), "rb+");

    // Lock prior to file OP
    flock(fileno(bFile), LOCK_EX);

    //  Computes diff & offset
    long long int diff = epochTime - currTS.firstEpoch;
    long long int offset = diff * cfgStruct.rowlength();

    // Seek to target offset prior to read
    fseeko64(bFile, offset, SEEK_SET);

    // A single line of data will be read into here
    char bufferIn[cfgStruct.rowlength()];

    // Find how many bytes occupied
    fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

    // Seek to target offset prior to write
    fseeko64(bFile, offset, SEEK_SET);

    // Nullify data based on N number of bytes available at that data point
    fwrite(std::string(strlen(bufferIn), '\0').data(), cfgStruct.rowlength(), 1, bFile);

    // Close file & Release lock after write
    std::fclose(bFile);
    flock(fileno(bFile), LOCK_UN);

    std::string successMsg = "[SUCCESS] [UNARY DELETE] [DB: " + dbName + "] - Deleted " + std::to_string(epochTime) + " in " + targetFileName;
    toReturn.set_deletestatus(successMsg);
    return toReturn;
}

// BATCH INSERT
XStore_Proto::batchInsert_REP XStore::batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient) {
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
        std::string errMsg = "[ERROR] [BATCH INSERT] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
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
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    std::string granularityErrMsg = "[ERROR] [BATCH INSERT] [DB: " + dbName + "] - Invalid time granularity. Expecting: " + expectedTimeResolution;

    // Granularity check
    for (int i = 0; i < protoFromClient->batchinsert_data_size(); i++) {
        currTime = protoFromClient->batchinsert_data(i).timestamp();
        epochStruct = gmtime(&currTime);
        dataMap[(epochStruct->tm_year + 1900)].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        timeStruct currTS {currTime};
        if (currTS.timeResolution != expectedTimeResolution) {
            toReturn.set_insertstatus(granularityErrMsg);
            return toReturn;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        unsigned short int yearKey = pair.first;
        FILE *bFile;
        std::string targetFile = dbBasePath + std::to_string(yearKey) + ".XSTORE";

        // Grab first inserting element for timeStruct purposes
        //  Get length of time to truncate
        timeStruct thisTS{static_cast<time_t>(protoFromClient->batchinsert_data().at(dataMap[yearKey].at(0)).timestamp())};

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Truncate
            bFile = std::fopen(targetFile.c_str(), "wb+");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_EX);
            ftruncate(fileno(bFile), (thisTS.lastEpoch - thisTS.firstEpoch) * cfgStruct.rowlength());
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
            XStore_Proto::rowData data = currInsertData.data();

            // Compute diff & offset
            diff = unixTime - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Writing actual data to disk
            fwrite(data.SerializeAsString().data(), cfgStruct.rowlength(), 1, bFile);
        }

        // Close file & Release lock after write
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
    }

    // Return insert status
    std::string successMsg = "[SUCCESS] [BATCH INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(protoFromClient->batchinsert_data_size()) + " elements successfully";
    toReturn.set_insertstatus(successMsg);

    return toReturn;
}

// BATCH QUERY
XStore_Proto::batchQuery_REP XStore::batchQuery(XStore_Proto::batchQuery_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchQuery_REP toReturn;
    XStore_Proto::rowData tempData;
    std::string dbName = protoFromClient->dbname();

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = dbBasePath + dbName + ".CFG";

        if (std::filesystem::exists(cfgPath) == 0) {
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

    // IF query range > 2GB limit of Protobuf or Empty
    if ((protoFromClient->timestamp_size() * cfgStruct.rowlength()) > 2000000000 || protoFromClient->timestamp_size() < 0) {
        return toReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        return toReturn;
    }

    // Assuming incoming batch query is unsorted
    //  -> Ascending sort
    std::sort(std::execution::par_unseq, protoFromClient->mutable_timestamp()->begin(), protoFromClient->mutable_timestamp()->end(),
              [](const unsigned long long int& a, const unsigned long long int& b) {
                return a < b;
    });

    // Organize incoming batch query into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    std::time_t currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;

    // Granularity check
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        currTime = protoFromClient->timestamp(i);
        struct timeStruct currTS{currTime};
        dataMap[currTS.year].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        if (currTS.timeResolution != expectedTimeResolution) {
            return toReturn;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        FILE *bFile;
        unsigned short int yearKey = pair.first;
        std::string targetFile = dbBasePath + std::to_string(yearKey) + ".XSTORE";

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Skip to next iteration
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            bFile = std::fopen(targetFile.c_str(), "rb");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_SH);
        }

        // Time-related & Auxiliary
        long long int diff, offset;
        char bufferIn[cfgStruct.rowlength()];
        timeStruct thisTS{static_cast<time_t>(protoFromClient->timestamp(dataMap[yearKey].at(0)))};

        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Computes diff & offset
            diff = protoFromClient->timestamp(dataMap[yearKey].at(i)) - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Seek to target offset prior to read
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
            XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_queryresponse();
            currEntry->set_timestamp(protoFromClient->timestamp(dataMap[yearKey].at(i)));
            *currEntry->mutable_data() = tempData;
        }

        // Close file & Release lock after read
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
    }
    return toReturn;
}

// BATCH DELETE
XStore_Proto::batchDelete_REP XStore::batchDelete(XStore_Proto::batchDelete_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchDelete_REP toReturn;
    std::string dbName = protoFromClient->dbname();
    int deleteCount = 0;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = dbBasePath + dbName + ".CFG";

        if (std::filesystem::exists(cfgPath) == 0) {
            return toReturn;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    // IF target delete timestamp(s) is empty OR db directory does not exist
    if (protoFromClient->timestamp_size() < 0) {
        std::string errMsg = "[ERROR] [BATCH DELETE] [DB: " + dbName + "] - Empty target timestamp(s)";
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }
    else if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [BATCH DELETE] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_deletestatus(errMsg);
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
    std::time_t currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;

    // Granularity check
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    for (int i = 0; i < protoFromClient->timestamp_size(); i++) {
        currTime = protoFromClient->timestamp(i);
        struct timeStruct currTS{currTime};
        dataMap[currTS.year].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        if (currTS.timeResolution != expectedTimeResolution) {
            std::string errMsg = "[ERROR] [BATCH DELETE] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + expectedTimeResolution;
            toReturn.set_deletestatus(errMsg);
            return toReturn;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        FILE *bFile;
        unsigned short int yearKey = pair.first;
        std::string targetFile = dbBasePath + std::to_string(yearKey) + ".XSTORE";

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Skip to next iteration
            continue;
        }
        else {
            // ELSE - Target file DOES exist
            //  Open in read mode
            bFile = std::fopen(targetFile.c_str(), "rb+");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_EX);
        }

        // Time-related & Auxiliary
        long long int diff, offset;
        char bufferIn[cfgStruct.rowlength()];
        timeStruct thisTS{static_cast<time_t>(protoFromClient->timestamp(dataMap[yearKey].at(0)))};

        for (int i = 0; i < dataMap[yearKey].size(); i++) {
            // Computes diff & offset
            diff = protoFromClient->timestamp(dataMap[yearKey].at(i)) - thisTS.firstEpoch;
            offset = diff * cfgStruct.rowlength();

            // Seek to target offset prior to read
            fseeko64(bFile, offset, SEEK_SET);

            // Extract N bytes of data according to N bytes from .CFG file
            //  .CFG file is set by user when creating DB
            fread(&bufferIn[0], 1, cfgStruct.rowlength(), bFile);

            // IF fetched data contains nothing
            //  -> Skip to next iteration
            if (strlen(bufferIn) == 0) {
                continue;
            }
            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Nullify data based on N number of bytes available at that data point
            fwrite(std::string(strlen(bufferIn), '\0').data(), cfgStruct.rowlength(), 1, bFile);
            deleteCount++;
        }

        // Close file & Release lock after read
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
    }
    std::string successMsg = "[SUCCESS] [BATCH DELETE] [DB: " + dbName + "] - Deleted " + std::to_string(deleteCount) + " items";
    toReturn.set_deletestatus(successMsg);
    return toReturn;
}

// RANGE INSERT
XStore_Proto::rangeInsert_REP XStore::rangeInsert(XStore_Proto::rangeInsert_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::rangeInsert_REP toReturn;
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
        std::string errMsg = "[ERROR] [RANGE INSERT] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_insertstatus(errMsg);
        return toReturn;
    }

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
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

    // Assuming incoming data is sorted --> Skip sorting
    // Organize incoming data into map
    //  dataMap[2016] = Vector {Index 0, Index 1, ...}
    struct tm* epochStruct;
    std::time_t currTime;
    std::map<unsigned short int, std::vector<unsigned long long int>> dataMap;
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    std::string granularityErrMsg = "[ERROR] [RANGE INSERT] [DB: " + dbName + "] - Invalid time granularity. Expecting: " + expectedTimeResolution;

    // Granularity check
    for (int i = 0; i < protoFromClient->rangeinsert_data_size(); i++) {
        currTime = protoFromClient->rangeinsert_data(i).timestamp();
        epochStruct = gmtime(&currTime);
        dataMap[(epochStruct->tm_year + 1900)].push_back(i);

        // Halt execution & Return as long as there's an error in time granularity
        timeStruct currTS {currTime};
        if (currTS.timeResolution != expectedTimeResolution) {
            toReturn.set_insertstatus(granularityErrMsg);
            return toReturn;
        }
    }

    // Iterate through each key (year) of dataMap
    for (std::pair<const unsigned short int, std::vector<unsigned long long int>> const& pair : dataMap) {
        unsigned short int yearKey = pair.first;
        FILE *bFile;
        std::string targetFile = dbBasePath + std::to_string(yearKey) + ".XSTORE";

        // Grab first inserting element for timeStruct purposes
        //  Get length of time to truncate
        timeStruct thisTS{static_cast<time_t>(protoFromClient->rangeinsert_data().at(dataMap[yearKey].at(0)).timestamp())};

        // IF target file does NOT exist
        if (std::filesystem::exists(targetFile) == 0) {
            // Truncate
            bFile = std::fopen(targetFile.c_str(), "wb+");

            // Lock prior to file OP
            flock(fileno(bFile), LOCK_EX);
            ftruncate(fileno(bFile), (thisTS.lastEpoch - thisTS.firstEpoch) * cfgStruct.rowlength());
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
            XStore_Proto::rangeInsert_DATA currInsertData = protoFromClient->rangeinsert_data(dataMap[yearKey].at(i));
            const std::time_t &unixTime = currInsertData.timestamp();
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
        std::fclose(bFile);
        flock(fileno(bFile), LOCK_UN);
    }

    // Return insert status
    std::string successMsg = "[SUCCESS] [RANGE INSERT] [DB: " + dbName + "] - Inserted " + std::to_string(protoFromClient->rangeinsert_data_size()) + " elements successfully";
    toReturn.set_insertstatus(successMsg);

    return toReturn;
}

// RANGE QUERY
XStore_Proto::rangeQuery_REP XStore::rangeQuery(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::rangeQuery_REP toReturn;
    XStore_Proto::rowData tempData;
    std::string queryStatus;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = dbBasePath + dbName + ".CFG";

        if (std::filesystem::exists(cfgPath) == 0) {
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

    // IF query range > 2GB limit of Protobuf
    if ((endEpochT - startEpochT) * cfgStruct.rowlength() > 2000000000) {
        return toReturn;
    }

    if ((endEpochT - startEpochT) < 0) {
        return toReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        return toReturn;
    }

    // Time granularity check
    timeStruct startTimeStruct{startEpochT};
    timeStruct endTimeStruct{endEpochT};
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (startTimeStruct.timeResolution != expectedTimeResolution || endTimeStruct.timeResolution != expectedTimeResolution) {
        return toReturn;
    }

    // Organize into timeStruct
    //  timeStruct provides many auxiliary vars/methods useful for read/write ops
    timeStruct currTS{startEpochT};

    // Placeholder for READ op
    FILE *bFile;
    std::string targetFilePath;
    long long int diff, offset;
    char bufferIn[cfgStruct.rowlength()];
    std::string successMsg;

    // IF query range is within 1-year
    if (currTS.lastEpoch >= endEpochT) {
        // Open file prior to reading
        targetFilePath = dbBasePath + std::to_string(currTS.year) + ".XSTORE";

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(targetFilePath) == 0) {
            // Return without error
            return toReturn;
        }

        bFile = std::fopen(targetFilePath.c_str(), "rb");

        // Lock file prior to reading
        flock(fileno(bFile), LOCK_SH);

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
            XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_queryresponse();
            currEntry->set_timestamp(epochTime);
            *currEntry->mutable_data() = tempData;
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

            // IF the requested start epoch spans ONLY 1 year
            if (currTS.lastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (currTS.lastEpoch - startEpoch));
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_SH);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime <= currTS.lastEpoch; epochTime++, startEpoch++) {
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
                    XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_queryresponse();
                    currEntry->set_timestamp(epochTime);
                    *currEntry->mutable_data() = tempData;
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
                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (endEpochT - startEpoch));
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_SH);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime <= endEpochT; epochTime++, startEpoch++) {
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
                    XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_queryresponse();
                    currEntry->set_timestamp(epochTime);
                    *currEntry->mutable_data() = tempData;
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
    return toReturn;
}

// RANGE DELETE
XStore_Proto::rangeDelete_REP XStore::rangeDelete(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Protobuf obj to be returned
    std::string queryStatus;
    XStore_Proto::rangeDelete_REP toReturn;
    int deleteCount = 0;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Base directory of a DB
    std::string dbBasePath = baseDir + "dataStore/" + dbName + "/";

    // Check if cfgMap NOT contains cfgStruct
    XStore_Proto::configStruct cfgStruct;
    if (cfgMap.find(dbName) == cfgMap.end()) {
        std::string cfgPath = dbBasePath + dbName + ".CFG";

        if (std::filesystem::exists(cfgPath) == 0) {
            return toReturn;
        }

        // Read & Add cfgStruct to cfgMap
        FILE *cFile = std::fopen(cfgPath.c_str(), "rb");
        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
        std::fclose(cFile);
        cfgMap[dbName] = cfgStruct;
    }
    else {
        // Otherwise -> Fetch cfgStruct from cfgMap
        cfgStruct = cfgMap[dbName];
    }

    if ((endEpochT - startEpochT) < 0) {
        std::string errMsg = "[ERROR] [RANGE DELETE] [DB: " + dbName + "] - Invalid range";
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // IF db directory does not exist
    if (std::filesystem::is_directory(dbBasePath) == 0) {
        std::string errMsg = "[ERROR] [RANGE DELETE] [DB: " + dbName + "] - DB does not exist";
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // Time granularity check
    timeStruct startTimeStruct{startEpochT};
    timeStruct endTimeStruct{endEpochT};
    std::string expectedTimeResolution = XStore_Proto::timeUnit_Name(cfgStruct.timeresolution());
    if (startTimeStruct.timeResolution != expectedTimeResolution || endTimeStruct.timeResolution != expectedTimeResolution) {
        std::string errMsg = "[ERROR] [RANGE DELETE] [DB: " + dbName + "] - Invalid time resolution. Expecting: " + expectedTimeResolution;
        toReturn.set_deletestatus(errMsg);
        return toReturn;
    }

    // Organize into timeStruct
    //  timeStruct provides many auxiliary vars/methods useful for read/write ops
    timeStruct currTS{startEpochT};

    // Placeholder for READ op
    FILE *bFile;
    std::string targetFilePath;
    long long int diff, offset;
    char bufferIn[cfgStruct.rowlength()];

    // IF query range is within 1-year
    if (currTS.lastEpoch >= endEpochT) {
        // Open file prior to reading
        targetFilePath = dbBasePath + std::to_string(currTS.year) + ".XSTORE";

        // IF target YEAR.XSTORE does NOT exist
        if (std::filesystem::exists(targetFilePath) == 0) {
            // Return without error
            std::string successMsg = "[SUCCESS] [RANGE DELETE] [DB: " + dbName + "] - Deleted " + std::to_string(deleteCount) + " items";
            toReturn.set_deletestatus(successMsg);
            return toReturn;
        }

        bFile = std::fopen(targetFilePath.c_str(), "rb");

        // Lock file prior to reading
        flock(fileno(bFile), LOCK_EX);

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

            // Seek to target offset prior to write
            fseeko64(bFile, offset, SEEK_SET);

            // Nullify data based on N number of bytes available at that data point
            fwrite(std::string(strlen(bufferIn), '\0').data(), cfgStruct.rowlength(), 1, bFile);
            deleteCount++;
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

            // IF the requested start epoch spans ONLY 1 year
            if (currTS.lastEpoch < endEpochT) {
                // IF target YEAR.XSTORE does NOT exist
                if (std::filesystem::exists(targetFilePath) == 0) {
                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (currTS.lastEpoch - startEpoch));
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_EX);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime <= currTS.lastEpoch; epochTime++, startEpoch++) {
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

                    // Seek to target offset prior to write
                    fseeko64(bFile, offset, SEEK_SET);

                    // Nullify data based on N number of bytes available at that data point
                    fwrite(std::string(strlen(bufferIn), '\0').data(), cfgStruct.rowlength(), 1, bFile);
                    deleteCount++;
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
                    // Update currTS with latest startEpoch after incrementing
                    currTS = timeStruct(startEpoch + (endEpochT - startEpoch));
                    continue;
                }

                // Open & Lock file prior to reading
                bFile = std::fopen(targetFilePath.c_str(), "rb");
                flock(fileno(bFile), LOCK_EX);

                // Iterate over range of query data & add queried data and status
                for (long long int epochTime = startEpoch; epochTime <= endEpochT; epochTime++, startEpoch++) {
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

                    // Seek to target offset prior to write
                    fseeko64(bFile, offset, SEEK_SET);

                    // Nullify data based on N number of bytes available at that data point
                    fwrite(std::string(strlen(bufferIn), '\0').data(), cfgStruct.rowlength(), 1, bFile);
                    deleteCount++;
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
    std::string successMsg = "[SUCCESS] [RANGE DELETE] [DB: " + dbName + "] - Deleted " + std::to_string(deleteCount) + " items";
    toReturn.set_deletestatus(successMsg);
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
        XStore_Proto::configStruct cfgStruct;
        FILE *cFile = std::fopen(cfgFilePath.c_str(), "rb");
        cfgStruct.ParseFromFileDescriptor(fileno(cFile));
//        fread(&cfgStruct, sizeof(struct configStruct), 1, cFile);
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