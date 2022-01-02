#include "include/dataStruct/fileStruct.h"

// Constructor
fileStruct::fileStruct(std::string dbName, std::string baseDir) {
    this->dbName = dbName;
    this->baseDir = baseDir;
    openAllDBFiles();
}

std::string fileStruct::getDBName() {
    return this->dbName;
}

std::map<int, FILE*> fileStruct::getFfileMap() {
    return this->fFileMap;
}

std::map<int, int> fileStruct::getBfileMap() {
    return this->bFileMap;
}

bool fileStruct::hasYear(int inputYear) {
    return (bFileMap.find(inputYear)->first == inputYear);
}

void fileStruct::printSummary() {

}

void fileStruct::openAllDBFiles() {
    // Placeholder
    FILE *fFile;
    int bFile;
    std::map<FILE*, int> tempMap;
    std::string currDataFilePath;
    int currYear;

    // Directory contains all data file for a specific DB
    std::filesystem::path dataPath = baseDir + "dataStore/" + dbName + "/";

    // Loop through each data file
    for (auto& p : std::filesystem::directory_iterator(dataPath))
    {
        // File path as string
        //  i.e., /home/ubuntu/XStore/dataStore/tsla/2017.XSTORE
        currDataFilePath = p.path().string();

        // Extracted file name from path
        //  i.e., 2017
        currYear = extractYear(currDataFilePath);

        // Open each file
        bFile = open(currDataFilePath.c_str(), O_RDONLY);
        fFile = fopen(currDataFilePath.c_str(), "rb");

        // Insert to appropriate map
        bFileMap[currYear] = bFile;
        fFileMap[currYear] = fFile;
    }
}

int fileStruct::extractYear(std::string inputFileName) {
    // Index of base path up to dbName
    int dbNameIdx = inputFileName.find(dbName);

    // Remove path up to dbName
    inputFileName.erase(0, dbNameIdx + dbName.size() + 1);

    // Index of substr with ".XSTORE"
    int xstoreIdx = inputFileName.find(".XSTORE");

    // Remove .XSTORE
    inputFileName.erase(xstoreIdx, inputFileName.size());

    // Returning year only i.e., 2017
    return atoi(inputFileName.c_str());
}