#ifndef XSTORE_FILESTRUCTS_H
#define XSTORE_FILESTRUCTS_H

#include <iostream>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <map>

// Data structure to hold data for I/O
struct FileStruct {
    std::string DbName;
    std::string BaseDir;

    // Map -- Stores file descriptor
    std::map<int, FILE*> FFileMap;
    std::map<int, int> BFileMap;

    // Constructor
    FileStruct(std::string dbName, std::string baseDir);

    std::string GetDBName();
    std::map<int, FILE*> GetFfileMap();
    std::map<int, int> GetBfileMap();
    bool HasYear(int inputYear);

    // Printing out time-value
    void PrintSummary();

    private:
        void OpenAllDBFiles();
        int ExtractYear(std::string);
};

#endif //XSTORE_FILESTRUCTS_H