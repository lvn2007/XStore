#include "include/XStore/XStore.h"

#define lineSize 128

// Initialization
XStore::XStore() {
    // Initialize message
//    initMsg();

    // Initialize DB
    initDB();

    // First, check if there's any existing data exists
    hasDB();
}

bool XStore::createDB(std::string dbName, int rowLength) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Ensure dbName string is all capitalized
    for (int i = 0; i < dbName.size(); i++) {
        dbName[i] = toupper(dbName[i]);
    }

    // Check if dbName is already existed in XStore
    bool isFound = std::find(dbNames.begin(), dbNames.end(), dbName) != dbNames.end();

    // IF folder for dbName is already created
    //  -> Do nothing
    if (isFound) {
        return false;
    }

    // Else -> Create folder
    std::filesystem::create_directories(baseDir + "dataStore/" + dbName);

    // Dumps db configuration info to disk
    //  Organize configs into a struct
    configStruct cfgStruct;
    strcpy(cfgStruct.dbName, dbName.c_str());
    strcpy(cfgStruct.timeResolution, "NULL");
    cfgStruct.rowLength = rowLength;

    //  I/O related items
    std::string configFilePath = baseDir + "dataStore/" + dbName + "/" + dbName + ".config";
    int bFile = open(configFilePath.c_str(), O_WRONLY | O_CREAT, 0666);

    //  Write config to binary file
    pwrite(bFile, &cfgStruct, sizeof(configStruct), 0);
    close(bFile);

//    std::cout << "dbCount: " << dbCount << std::endl;

//    configStruct testData;
//    int bbFile = open(configFilePath.c_str(), O_RDONLY);
//
//    pread(bbFile, &testData, sizeof(configStruct), 0);
//    testData.printData();

    // Re-run dbStats to update new changes
    dbStats();
}

// INSERT time-series oriented values
void XStore::insert(std::time_t unixTime, std::string value, std::string dbName) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // IF no DB exists i.e., dbCount == 0
    //  Requiring a db space to be instantiated with "createDB" method prior to this
    if (dbCount == 0) {
        return;
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
        return;
    }

    // Otherwise, create a file with name of inputYear.XSTORE
    //  Read timeValue -- UNIX time
    //  Ensure timeResolution is enforced
    configStruct cfgStruct;
    std::string cfgFilePath = baseDir + "dataStore/" + dbName + "/" + dbName + ".config";
    int cfgFile = open(cfgFilePath.c_str(), O_RDWR);
    pread(cfgFile, &cfgStruct, sizeof(configStruct), 0);

    //  timeResolution staging area
    int inputYear = toHumanYear(unixTime);
    std::string currResolution = getTimeResolution(unixTime);

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
    if (dbMap.size() == 0) {
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
    dataStruct currData{unixTime, value};

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
}

void XStore::insertBatch(std::time_t epochTime, std::string value, std::string dbName) {

}

std::string XStore::get(std::time_t epochTime, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(epochTime);
    std::string currResolution = getTimeResolution(epochTime);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = epochTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * lineSize;

    FILE *bFile;
    std::string dataDir = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
    bFile = std::fopen(dataDir.c_str(), "rb");

    char *buff = new char[lineSize];

    std::fseek(bFile, offSet, SEEK_SET);
    std::fgets(buff, lineSize, bFile);
//    std::puts(buff);
    std::fclose(bFile);
    std::string toReturn(buff);
    return toReturn;
}

xstoreProto::unaryResponse XStore::getSingleOpenClose(std::time_t epochTime, std::string dbName) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Protobuf obj to be return
    xstoreProto::unaryResponse toReturn;
    toReturn.set_timestamp(epochTime);

    // Make sure dbName exists
    //  IF given dbName is not available
    //  Return "NULL" as string
    if (dbMap.find(dbName) == dbMap.end()) {
        toReturn.set_dataset("NULL");
        return toReturn;
    }

    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(epochTime);

    // Make sure inputYear is available in DB
    //  IF inputYear is not available
    //  Return "NULL" as string
    if ((dbMap.find(dbName)->second.hasYear(inputYear)) != true) {
        toReturn.set_dataset("NULL");
        return toReturn;
    }

    // Fetch config of a target DB
    configStruct cfgStruct;
    std::string cfgFilePath = baseDir + "dataStore/" + dbName + "/" + dbName + ".config";
    int cfgFile = open(cfgFilePath.c_str(), O_RDONLY);
    pread(cfgFile, &cfgStruct, sizeof(configStruct), 0);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, cfgStruct.timeResolution);

    // Epoch time difference between target & startEpoch
    long int diff = epochTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * cfgStruct.rowLength;

    // Look-up file descriptor in dbMap
    int bFile = dbMap.find(dbName)->second.getBfileMap().find(inputYear)->second;

    // A single line data will be read into here
    char buff[cfgStruct.rowLength];

    // Extract line of data from binary file
    pread(bFile, &buff[0], cfgStruct.rowLength , offSet);

    // Remove newline at the end of char[]
    buff[strlen(buff) - 1] = '\0';

    // IF a target time contains no data
    //  -> Return "NULL" as string
    if (strlen(buff) == 0) {
        toReturn.set_dataset("NULL");
        return toReturn;
    }

    // Set data fetched from XStore to returning Protobuf obj
    toReturn.set_dataset(std::string(buff));

    return toReturn;
}

xstoreProto::batchResponse XStore::getBatch(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    // Fetch all dirs in dataStore dir
    dbStats();

    // Protobuf obj to be return
    xstoreProto::batchResponse toReturn;

    // Ranges of queried time
    //  i.e., N elements of time-series row of data
    long int timeRange = endEpochT - startEpochT;

    // Pre-populating time for all defined ranges
    for (int i = 0; i < timeRange; i++) {
        xstoreProto::unaryResponse *currEntry = toReturn.add_unaryresponse();
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
    int startYear = toHumanYear(startEpochT);
    int endYear = toHumanYear(endEpochT);

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

        // Set data fetched from XStore to returning Protobuf obj
        toReturn.mutable_unaryresponse(i)->set_dataset(std::string(buff));
    }

    // Set query status msg
    toReturn.set_querystatus("[SUCCESS] -- Batch queries completed");

    return toReturn;
}

int XStore::getRowSize(std::string dbName) {
    // Make sure dbName exists
    //  IF given dbName is not available
    //  Return -1 to indicate error
    if (dbMap.find(dbName) == dbMap.end()) {
        return -1;
    }

    // Look-up file descriptor in dbMap
//    int bFile = dbMap.find(dbName)->second.getBfileMap().find(inputYear)->second;
    int bFile = dbMap.find(dbName)->second.getBfileMap().begin()->second;

    int tempLineSize = 2;

    // A single line data will be read into here
    char firstBuff[lineSize];
    std::string tempBuff;

    // Extract line of data from binary file
    pread(bFile, &firstBuff[0], tempLineSize, 0);

    while (true) {
        tempLineSize *= 2;
        char secondBuff[tempLineSize];
        pread(bFile, &secondBuff[0], tempLineSize, 0);

        if (std::string(secondBuff).find("\n") < tempLineSize) {
            tempBuff = std::string(secondBuff);
            break;
        }
    }
    char lastChar = firstBuff[strlen(firstBuff) - 1];
    std::cout << tempBuff << std::endl;
    std::cout << tempBuff.find("\n") << std::endl;

    // Remove newline at the end of char[]
//    buff[strlen(buff) - 1] = '\0';


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
    toReturn = std::filesystem::current_path();

    // Extract baseDir for XStore i.e., /home/ubuntu/XStore/
    toReturn.erase(toReturn.find("XStore") + 7, toReturn.size());
    return toReturn;
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
    long int startEpoch = static_cast<long int> (startTime);
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
    long int startEpoch = static_cast<long int> (startTime);
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

int XStore::toHumanYear(std::time_t unixTime) {
    tm* timePtr = localtime(&unixTime);
    return timePtr -> tm_year + 1900;
}

int XStore::toHumanMonth(std::time_t unixTime) {
    tm* timePtr = localtime(&unixTime);
    return timePtr -> tm_mon + 1;
}

int XStore::toHumanDay(std::time_t unixTime) {
    tm* timePtr = localtime(&unixTime);
    return timePtr -> tm_mday;
}