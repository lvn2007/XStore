#ifndef XSTORE_FILESTRUCTS_H
#define XSTORE_FILESTRUCTS_H

#include <iostream>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <map>

// Data structure to hold data for I/O
struct fileStruct {
    std::string dbName;
    std::string baseDir;

    // Map -- Stores file descriptor
    std::map<int, FILE*> fFileMap;
    std::map<int, int> bFileMap;

    // Constructor
    fileStruct(std::string dbName, std::string baseDir);

    std::string getDBName();
    std::map<int, FILE*> getFfileMap();
    std::map<int, int> getBfileMap();
    bool hasYear(int inputYear);

    // Printing out time-value
    void printSummary();

    private:
        void openAllDBFiles();
        int extractYear(std::string);
};

#endif //XSTORE_FILESTRUCTS_H