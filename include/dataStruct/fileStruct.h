#ifndef XSTORE_FILESTRUCT_H
#define XSTORE_FILESTRUCT_H

#include <iostream>
#include <filesystem>

// Data structure to hold data for I/O
struct fileStruct {
    std::string ipAddr;
    std::string dbName;
    FILE *fFile;
    int bFile;

    // Constructor
    fileStruct(std::string ipAddr, std::string dbName);

    // Returning a line
    //  i.e., unixTime, values
    std::string getData();

    // Returning an IP Address
    //  This is used as an Identifier (ID)
    std::string getID();

    // Printing out time-value
    void printData(bool humanTime);
    private:
        // Parsing IP addr -- Format: ipv4:xx.xxx.xxx.x:xxxxx
        //  Returning: xx.xxx.xxx.x
        std::string parseIP(std::string ipAddr);
};

#endif //XSTORE_FILESTRUCT_H