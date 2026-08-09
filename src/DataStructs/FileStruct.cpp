#include "include/DataStructs/FileStruct.h"

// Constructor
FileStruct::FileStruct(std::string dbName, std::string baseDir) {
    this->dbName = dbName;
    this->baseDir = baseDir;
    OpenAllDBFiles();
}

std::string FileStruct::GetDBName() {
    return this->dbName;
}

std::map<int, FILE*> FileStruct::GetFfileMap() {
    return this->fFileMap;
}

std::map<int, int> FileStruct::GetBfileMap() {
    return this->bFileMap;
}

bool FileStruct::HasYear(int inputYear) {
    return (bFileMap.find(inputYear)->first == inputYear);
}

void FileStruct::PrintSummary() {

}

void FileStruct::OpenAllDBFiles() {
    // Placeholder
    FILE *fFile;
    int bFile;
    std::map<FILE*, int> tempMap;
    std::string currDataFilePath;
    int currYear;

    // Directory contains all data file for a specific DB
    std::filesystem::path dataPath = baseDir + "DataStore/" + dbName + "/";

    // Loop through each data file
    for (auto& p : std::filesystem::directory_iterator(dataPath))
    {
        // File path as string
        //  i.e., /home/ubuntu/XStore/DataStore/tsla/2017.XSTORE
        currDataFilePath = p.path().string();

        // IF meets with any file that's not .XSTORE
        //  -> Skip to next iteration
        if (currDataFilePath.find(".XSTORE") == currDataFilePath.npos) {
            continue;
        }

        // Extracted file name from path
        //  i.e., 2017
        currYear = ExtractYear(currDataFilePath);

        // Open each file
        bFile = open(currDataFilePath.c_str(), O_RDONLY);
        fFile = fopen(currDataFilePath.c_str(), "rb");

        // Insert to appropriate map
        bFileMap[currYear] = bFile;
        fFileMap[currYear] = fFile;
    }
}

int FileStruct::ExtractYear(std::string inputFileName) {
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