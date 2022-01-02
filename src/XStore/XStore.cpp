#include "include/XStore/XStore.h"

#define lineSize 128

// Initialization
XStore::XStore() {
    // Initialize message
    initMsg();

    // Initialize DB
    initDB();

    //  First, check if there's any existing data exists
    hasDB();

    std::string dataDir = baseDir + "dataStore/" + "tsla" + "/" + std::to_string(2017) + ".XSTORE";
}

// INSERT time-series oriented values
void XStore::insert(std::time_t unixTime, std::string value, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(unixTime);
    std::string currResolution = getTimeResolution(unixTime);
    std::cout << resolutionMap[currResolution] << std::endl;

    FILE *bFile;

    // Start & End time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);
    long int endEpoch = getEndTimeEpoch(inputYear, currResolution);

    // IF no DB exists i.e., dbCount == 0
    if (dbCount == 0) {
        //  -> Create new one
        std::filesystem::create_directories(baseDir + "dataStore/" + dbName);

        // -> Create a file with name of inputYear.XSTORE
        std::string dataDir = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
        bFile = std::fopen(dataDir.c_str(), "wb+");

        // Creating dummy file with n Size
        ftruncate(fileno(bFile), (endEpoch - startEpoch) * lineSize);

        // IF no error
        // -> Organize timestamp & data into an object
        dataStruct currData{unixTime, value};

        // Epoch time difference between target & startEpoch
        long int diff = unixTime - startEpoch;

        // Calculate offset -- Exact location of where a particular target row is stored
        long int offSet = diff * lineSize;

        // Seek to target offset prior to write
        std::fseek(bFile, offSet, SEEK_SET);

        // Write as binary file to disk with new line indicates EOL
        std::fwrite((currData.getData()).data(), lineSize, 1, bFile);

        // Close file after write
        std::fclose(bFile);

        // Updating dbStats
        dbStats();
    }
    else {
        // Making sure a DB's directory is there
        std::filesystem::create_directories(baseDir + "dataStore/" + dbName);

        // -> Create a file with name of inputYear.XSTORE
        std::string dataDir = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";

        // Check if year.XSTORE exists
        //  IF year.XSTORE exists
        if (hasDBFile(dataDir)) {
            // Open in updating mode
            bFile = std::fopen(dataDir.c_str(), "r+b");
        }
        //  IF year.XSTORE exists
        else {
            // Create & write mode
            bFile = std::fopen(dataDir.c_str(), "wb");

            // Creating dummy file with n Size
            ftruncate(fileno(bFile), (endEpoch - startEpoch) * lineSize);
        }

        // IF no error
        // -> Organize timestamp & data into an object
        dataStruct currData{unixTime, value};

        // Epoch time difference between target & startEpoch
        long int diff = unixTime - startEpoch;

        // Calculate offset -- Exact location of where a particular target row is stored
        long int offSet = diff * (lineSize);

        // Seek to target offset prior to write
        std::fseek(bFile, offSet, SEEK_SET);

        // Write as binary file to disk with new line indicates EOL
        std::fwrite((currData.getData()).data(), lineSize, 1, bFile);

        // Close file after write
        std::fclose(bFile);

        // Updating dbStats
        dbStats();
    }
}

void XStore::batchInsert(std::time_t epochTime, std::string value, std::string dbName) {

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

std::string XStore::getp(std::time_t epochTime, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(epochTime);
    std::string currResolution = getTimeResolution(epochTime);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = epochTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * lineSize;

    int bFile;

    // Target file directory
    std::string dataDir = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";

    // Opening binary file
    bFile = open(dataDir.c_str(), O_RDONLY);

    // A single line data will be read into here
    char buff[lineSize];

    // Extract line of data from binary file
    int ret = pread(bFile, &buff[0], lineSize , offSet);

    // Close file
    close(bFile);
    std::string toReturn(buff);
    return toReturn;
}

std::vector<std::string> XStore::getBatch(std::time_t startEpochT, std::time_t endEpochT, std::string dbName) {
    std::vector<std::string> toReturn;

    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(startEpochT);
    std::string currResolution = getTimeResolution(startEpochT);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = startEpochT - startEpoch;
    long int rangeDiff = endEpochT - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * lineSize;

    int bFile, ret;

    // Target file directory
    std::string dataDir = baseDir + "dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";

    // Opening binary file
    bFile = open(dataDir.c_str(), O_RDONLY);

    // A single line data will be read into here
    char buff[lineSize];

    for (int i = offSet; i <= lineSize * rangeDiff; i += lineSize) {
        // Extract line of data from binary file
        ret = pread(bFile, &buff[0], lineSize, i);

        // Cache data to return
        toReturn.push_back(buff);
//        toReturn += buff;
    }

    // Close file
    close(bFile);

    return toReturn;
}

std::string XStore::getSingleOpenClose(std::time_t epochTime, std::string dbName) {
    // Make sure dbName exists
    //  IF given dbName is not available
    //  Return "NULL" as string
    if (dbMap.find(dbName) == dbMap.end()) {
        return "NULL";
    }

    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(epochTime);

    // Make sure inputYear is available in DB
    //  IF inputYear is not available
    //  Return "NULL" as string
    if ((dbMap.find(dbName)->second.hasYear(inputYear)) != true) {
        return "NULL";
    }

    // Otherwise, keep processing
    std::string currResolution = getTimeResolution(epochTime);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = epochTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * lineSize;

    // Look-up file descriptor in dbMap
    int bFile = dbMap.find(dbName)->second.getBfileMap().find(inputYear)->second;

    // A single line data will be read into here
    char buff[lineSize];

    // Extract line of data from binary file
    int ret = pread(bFile, &buff[0], lineSize , offSet);

    std::string toReturn(buff);
    return toReturn;
}

void XStore::initDB() {
    // Directory contains all data file for a specific DB
    std::filesystem::path dbNames = baseDir + "dataStore/";

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

//void XStore::closeFile() {
//    std::fclose(bFile);
//}

//
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
    std::filesystem::path dbDir = baseDir + "dataStore";

    for (auto& p : std::filesystem::directory_iterator(dbDir))
    {
        dbCount++;
        dbNames.push_back(p.path().filename());
    }
}

bool XStore::yearExists(int inputYear, std::string dbName) {
    std::string path = baseDir + "dataStore" + dbName;

    for (const auto & entry : std::filesystem::directory_iterator(path)) {
        std::cout << entry.path();
    }
}


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