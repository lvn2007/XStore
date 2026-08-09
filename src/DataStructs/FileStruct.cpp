#include "include/DataStructs/FileStruct.h"

// Constructor
FileStruct::FileStruct(std::string dbName, std::string baseDir) {
    this->DbName = dbName;
    this->BaseDir = baseDir;
    OpenAllDBFiles();
}

std::string FileStruct::GetDBName() {
    return this->DbName;
}

std::map<int, FILE*> FileStruct::GetFfileMap() {
    return this->FFileMap;
}

std::map<int, int> FileStruct::GetBfileMap() {
    return this->BFileMap;
}

bool FileStruct::HasYear(int inputYear) {
    return (BFileMap.find(inputYear)->first == inputYear);
}

void FileStruct::PrintSummary() {

}

void FileStruct::OpenAllDBFiles() {
    // Placeholder
    FILE *FFile;
    int BFile;
    std::map<FILE*, int> TempMap;
    std::string CurrDataFilePath;
    int CurrYear;

    // Directory contains all data file for a specific DB
    std::filesystem::path DataPath = BaseDir + "DataStore/" + DbName + "/";

    // Loop through each data file
    for (auto& p : std::filesystem::directory_iterator(DataPath))
    {
        // File path as string
        //  i.e., /home/ubuntu/XStore/DataStore/tsla/2017.XSTORE
        CurrDataFilePath = p.path().string();

        // IF meets with any file that's not .XSTORE
        //  -> Skip to next iteration
        if (CurrDataFilePath.find(".XSTORE") == CurrDataFilePath.npos) {
            continue;
        }

        // Extracted file name from path
        //  i.e., 2017
        CurrYear = ExtractYear(CurrDataFilePath);

        // Open each file
        BFile = open(CurrDataFilePath.c_str(), O_RDONLY);
        FFile = fopen(CurrDataFilePath.c_str(), "rb");

        // Insert to appropriate map
        BFileMap[CurrYear] = BFile;
        FFileMap[CurrYear] = FFile;
    }
}

int FileStruct::ExtractYear(std::string inputFileName) {
    // Index of base path up to dbName
    int DbNameIdx = inputFileName.find(DbName);

    // Remove path up to dbName
    inputFileName.erase(0, DbNameIdx + DbName.size() + 1);

    // Index of substr with ".XSTORE"
    int XStoreIdx = inputFileName.find(".XSTORE");

    // Remove .XSTORE
    inputFileName.erase(XStoreIdx, inputFileName.size());

    // Returning year only i.e., 2017
    return atoi(inputFileName.c_str());
}