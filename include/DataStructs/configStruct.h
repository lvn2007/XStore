#ifndef XSTORE_CONFIGSTRUCTS_H
#define XSTORE_CONFIGSTRUCTS_H

#include <iostream>
#include <string>
#include <vector>

struct configStruct {
    // Limit dbName to 256 chars
    char dbName[256];
    char timeResolution[12];
    short int rowLength;
    std::vector<std::string> dTypeSchema;

    // Printing out configurations
    void printData();
};


#endif //XSTORE_CONFIGSTRUCTS_H
