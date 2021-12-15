#include "include/XStore/XStore.h"

#define lineSize 100

// Initialization
XStore::XStore() {
    // Initialized message
    initMsg();

    //  First, check if there's any existing data exists
    hasDB();

    std::cout << toHumanYear(1514764800) << std::endl;
    std::cout << toHumanMonth(1514764800) << std::endl;
    std::cout << toHumanDay(1514764800) << std::endl;
    std::cout << getTimeResolution(1514764800) << std::endl;

    std::string dataDir = baseDir + "/XStore/dataStore/" + "tsla" + "/" + std::to_string(2017) + ".XSTORE";
    bFile = std::fopen(dataDir.c_str(), "rb");
}

// INSERT time-series oriented values
void XStore::insert(std::time_t unixTime, std::string value, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(unixTime);
    std::string currResolution = getTimeResolution(unixTime);
    std::cout << resolutionMap[currResolution] << std::endl;

    switch (dbCount) {
        // IF no DB exists i.e., dbCount == 0
        case 0:
            //  -> Create new one
            std::filesystem::create_directories(baseDir + "/XStore/dataStore/" + dbName);

            FILE *bFile;
            // -> Create a file with name of inputYear.XSTORE
            std::string dataDir = baseDir + "/XStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
            bFile = std::fopen(dataDir.c_str(), "wb");

            // Start & End time of a given year
            long int startEpoch = getStartTimeEpoch(inputYear, currResolution);
            long int endEpoch = getEndTimeEpoch(inputYear, currResolution);

            // Staged area for XStore -- Writing dummy data prior to writing real data
            for (long int i = startEpoch; i <= endEpoch; i++) {
                // Preparing line of data with dummy variables based on passed input "value"
                dataStruct dummyData{i, value};

                // Write to binary file to disk with new line indicates EOL
                std::fwrite((dummyData.getData() + "\n").data(), lineSize + 1, 1, bFile);
            }

            // IF no error
            // -> Organize timestamp & data into an object
            dataStruct currData{unixTime, value};

            // DEBUG: Printing data current size
            std::cout << currData.getData().size() << std::endl;

            // Write as binary file to disk with new line indicates EOL
            std::fwrite((currData.getData() + "\n").data(), lineSize + 1, 1, bFile);

            // Close file after write
            std::fclose(bFile);
            break;
    }
}

std::string XStore::get(std::time_t unixTime, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(unixTime);
    std::string currResolution = getTimeResolution(unixTime);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = unixTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * (lineSize + 1);

    FILE *bFile;
    std::string dataDir = baseDir + "/XStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
    bFile = std::fopen(dataDir.c_str(), "rb");

    char *buff = new char[100];
    std::fseek(bFile, offSet, SEEK_SET);
    std::fgets(buff, lineSize + 1, bFile);
//            std::puts(buff);
    std::fclose(bFile);
    std::string toReturn(buff);
    return toReturn;
}

std::string XStore::getSingleOpenClose(std::time_t unixTime, std::string dbName) {
    // Read timeValue -- UNIX time
    int inputYear = toHumanYear(unixTime);
    std::string currResolution = getTimeResolution(unixTime);

    // Start epoch time of a given year
    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

    // Epoch time difference between target & startEpoch
    long int diff = unixTime - startEpoch;

    // Calculate offset -- Exact location of where a particular target row is stored
    long int offSet = diff * (lineSize + 1);

//        FILE *bFile;
//        std::string dataDir = baseDir + "/XStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
    char *buff = new char[100];
    std::fseek(bFile, offSet, SEEK_SET);
    std::fgets(buff, lineSize + 1, bFile);
//        std::puts(buff);
//        std::fclose(bFile);
    std::string toReturn(buff);
    return toReturn;
}

void XStore::closeFile() {
    std::fclose(bFile);
}

// Check if there's any existing database
bool XStore::hasDB() {
    // Path to dataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
    const std::filesystem::path tmp_dir{ std::filesystem::path(baseDir + "/XStore/dataStore")};
    return !std::filesystem::is_empty(tmp_dir);
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
    std::filesystem::path dbDir = baseDir + "/XStore/dataStore";

    for (auto& p : std::filesystem::directory_iterator(dbDir))
    {
        dbCount++;
        dbNames.push_back(p.path().filename());
    }
}

bool XStore::yearExists(int inputYear, std::string dbName) {
    std::string path = baseDir + "/XStore/dataStore" + dbName;

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