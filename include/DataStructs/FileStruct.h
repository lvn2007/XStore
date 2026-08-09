#ifndef XSTORE_FILESTRUCTS_H
#define XSTORE_FILESTRUCTS_H

#include <iostream>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <map>

// Data structure to hold data for I/O
struct FileStruct {
    std::string dbName;
    std::string baseDir;

    // Map -- Stores file descriptor
    std::map<int, FILE*> fFileMap;
    std::map<int, int> bFileMap;

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