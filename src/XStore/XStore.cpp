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
                std::cout << filePath << std::endl;
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

XStore_Proto::createDB_REP XStore::createDB(std::string dbName) {
    // Protobuf obj to be returned
    XStore_Proto::createDB_REP toReturn;
    std::string createDBStatus;

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Target directory of a given DB name
    std::string targetPath = baseDir + "dataStore/" + dbName + "/";

    // IF folder for dbName is already created
    //  -> Inform user about the error
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Create DB directory
        if (std::filesystem::create_directory(targetPath) == 1) {
            std::string successMsg = "[SUCCESS] [CREATE DB] [DB: " + dbName + "] - Created DB";
            toReturn.set_createdbstatus(successMsg);
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

    //  Time structure contains all components per UNIXTIME:
    //      Year, month, day, hour, minute, sec, ...
    timeStruct currTS{unixTime};

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Target path of file to WRITE
    std::string targetPath = baseDir + "dataStore/" + dbName + "/" + std::to_string(currTS.year) + "/" + monthArray[currTS.month - 1] + dayArray[currTS.day - 1] + hourArray[currTS.hour] + minArray[currTS.minute];

    // IF hierarchical year folder DOES NOT exist
    if (std::filesystem::is_directory(targetPath) == 0) {
        // Create hierarchical directories
        if (std::filesystem::create_directories(targetPath) == 1) {
            std::string errMsg = "[ERROR] [UNARY INSERT] [DB: " + dbName + "] - Unable to create hierarchical directories";
            toReturn.set_insertstatus(errMsg);
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

XStore_Proto::batchInsert_REP XStore::batchInsert(XStore_Proto::batchInsert_REQ *protoFromClient) {
    // Protobuf obj to be returned
    XStore_Proto::batchInsert_REP toReturn;
    std::string insertStatus;

    // Iterate over batch of insert data
    for (int idx = 0; idx < protoFromClient->batchinsert_data_size(); idx++) {
        // Fetch all dirs in dataStore dir
        dbStats();

        // Parse each data in a batch collection
        XStore_Proto::batchInsert_DATA currentInsertData = protoFromClient->batchinsert_data(idx);
        std::time_t unixTime = currentInsertData.timestamp();
        std::string data = currentInsertData.data();
        std::string dbName = currentInsertData.dbname();

        // IF no DB exists i.e., dbCount == 0
        //  Requiring a db space to be instantiated with "createDB" method prior to this
        if (dbCount == 0) {
            insertStatus = "[ERROR - BATCH INSERT] - [DB: " + dbName + "] -- DB does not exist";
            toReturn.add_insertstatus(insertStatus);
            return toReturn;
        }

        // Check whether dbName is already created
        //  Ensure dbName string is all capitalized
        for (int i = 0; i < dbName.size(); i++) {
            dbName[i] = toupper(dbName[i]);
        }

        //  Check if dbName is already existed in XStore
        bool isFound = std::find(dbNames.begin(), dbNames.end(), dbName) != dbNames.end();

        //  IF dbName is NOT existed
        //   -> Do nothing
        if (!isFound) {
            insertStatus = "[ERROR - BATCH INSERT] - [DB: " + dbName + "] -- DB does not exist";
            toReturn.add_insertstatus(insertStatus);
            return toReturn;
        }

        // Otherwise, create a file with name of inputYear.XSTORE
        //  Read timeValue -- UNIX time
        //  Ensure timeResolution is enforced
        configStruct cfgStruct;
        std::string cfgFilePath = baseDir + "dataStore/" + dbName + "/" + dbName + ".config";
        int cfgFile = open(cfgFilePath.c_str(), O_RDWR);
        pread(cfgFile, &cfgStruct, sizeof(configStruct), 0);

        //  timeResolution staging area
        std::string currResolution = getTimeResolution(unixTime);
        int inputYear = toHumanYear(unixTime, currResolution);

        //  Start & End time of a given year
        long int startEpoch = getStartTimeEpoch(inputYear, currResolution);
        long int endEpoch = getEndTimeEpoch(inputYear, currResolution);

        //  IF timeResolution is not yet initialized
        //   -> First inserting time will determine resolution of this current DB
        if (strcmp(cfgStruct.timeResolution, "NULL") == 0) {
            // Updating configFile and dump to disk
            strcpy(cfgStruct.timeResolution, currResolution.c_str());
            pwrite(cfgFile, &cfgStruct, sizeof(configStruct), 0);
            close(cfgFile);
        }

        //  Staging area for new data file waiting to be inserted
        std::string dataFilePath = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
        FILE *bFile;

        // DB is empty
        if (dbMap.size() == 0 || (dbMap.find(dbName) == dbMap.end())) {
            bFile = std::fopen(dataFilePath.c_str(), "wb+");

            //  Creating dummy file with n Size from cfgStruct
            ftruncate(fileno(bFile), (endEpoch - startEpoch) * cfgStruct.rowLength);
        }

        // DB is not empty
        else {
            // Ensure that the file inputYear.XSTORE is NOT existed
            if (dbMap.find(dbName)->second.hasYear(inputYear) != true) {
                bFile = std::fopen(dataFilePath.c_str(), "wb+");

                //  Creating dummy file with n Size from cfgStruct
                ftruncate(fileno(bFile), (endEpoch - startEpoch) * cfgStruct.rowLength);
            }

            // IF inputYear.XSTORE existed
            //  -> Open in append mode
            else {
                bFile = std::fopen(dataFilePath.c_str(), "rb+");
                rewind(bFile);
            }
        }

        // IF no error
        // -> Organize timestamp & data into an object
        dataStruct currData{unixTime, data};

        // Writing actual data to disk
        //  Epoch time difference between target & startEpoch
        long int diff = unixTime - startEpoch;

        //  Calculate offset -- Exact location of where a particular target row is stored
        long int offSet = diff * cfgStruct.rowLength;

        //  Seek to target offset prior to write
        std::fseek(bFile, offSet, SEEK_SET);

        //  Write as binary file to disk with new line indicates EOL
        std::fwrite((currData.getData()).data(), cfgStruct.rowLength, 1, bFile);

        //  Close file after write
        std::fclose(bFile);

        //  Updating dbStats & dbMap
        dbStats();
        initDB();

        toReturn.add_insertstatus("[SUCCESS - BATCH INSERT] - [DB: " + dbName + "] -- Inserted: " + std::to_string(unixTime));
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

XStore_Proto::batchQuery_REP XStore::batchQuery(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Protobuf obj to be returned
    XStore_Proto::batchQuery_REP toReturn;

    // Ranges of queried time
    //  i.e., N elements of time-series row of data
    long int timeRange = endEpochT - startEpochT;

    // Pre-populating time for all defined ranges
    for (int i = 0; i < timeRange; i++) {
        XStore_Proto::unaryQuery_REP *currEntry = toReturn.add_unaryresponse();
        currEntry->set_timestamp(startEpochT + i);
        currEntry->set_dataset("NULL");
    }

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Make sure dbName exists
    //  IF given dbName is not available
    //  Return empty vector
    if (dbMap.find(dbName) == dbMap.end()) {
        toReturn.set_querystatus("[ERROR] -- DB Name does not exists");
        return toReturn;
    }

    // Read timeValue -- UNIX time
    std::string currResolution = getTimeResolution(startEpochT);
    int startYear = toHumanYear(startEpochT, currResolution);
    int endYear = toHumanYear(endEpochT, currResolution);

    // TEMPORARY -- Waiting for Ioan's inputs/advice on this
    //  Guard against a query that spans over 1 year
    if ((endYear - startYear) != 0) {
        toReturn.set_querystatus("[ERROR] -- Queried range is larger than 1 year");
        return toReturn;
    }

    // Make sure inputYear is available in DB
    //  IF inputYear is not available
    //  Return empty vector
    if ((dbMap.find(dbName)->second.hasYear(startYear)) != true) {
        std::string errMsg = "[ERROR] -- " + std::to_string(startYear) + ".XSTORE does not exists";
        toReturn.set_querystatus(errMsg);
        return toReturn;
    }

    // Fetch config of a target DB
    configStruct cfgStruct;
    std::string cfgFilePath = baseDir + "dataStore/" + dbName + "/" + dbName + ".config";
    int cfgFile = open(cfgFilePath.c_str(), O_RDONLY);
    pread(cfgFile, &cfgStruct, sizeof(configStruct), 0);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(startYear, cfgStruct.timeResolution);

    // Epoch time difference between target & startEpoch
    long int diff = startEpochT - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int startOffset = diff * cfgStruct.rowLength;

    // Look-up file descriptor in dbMap
    int bFile = dbMap.find(dbName)->second.getBfileMap().find(startYear)->second;

    // A single line of data will be read into this buffer
    char buff[cfgStruct.rowLength];

    for (long int i = 0; i < timeRange; i++) {
        // Computes offset of current iteration
        long int currOffset = startOffset + (cfgStruct.rowLength * i);

        // Reads data into buffer
        pread(bFile, &buff[0], cfgStruct.rowLength, currOffset);

        // Remove newline at the end of char[]
        buff[strlen(buff) - 1] = '\0';

        // IF a target time contains no data
        //  -> Set dataset to "NULL" as string
        //  NOTE: We don't actually set dataset to NULL since it was pre-populated prior
        //  Instead, we will skip to next iteration
        if (strlen(buff) == 0) {
            continue;
        }

        // Convert raw line of data to str for further processing
        std::string strBuff = std::string(buff);

        // Strip timestamp from raw line of data
        strBuff.erase(0, strBuff.find_first_of(",") + 1);

        // Set data fetched from XStore to returning Protobuf obj
        toReturn.mutable_unaryresponse(i)->set_dataset(strBuff);
    }

    // Set query status msg
    toReturn.set_querystatus("[SUCCESS] -- Batch queries completed");

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