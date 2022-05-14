#ifndef XSTORE_CONFIGSTRUCTS_H
#define XSTORE_CONFIGSTRUCTS_H

#include <iostream>
#include <string>

struct configStruct {
    // Limit dbName to 256 chars
    char dbName[256];
    char timeResolution[12];
    int rowLength;

    // Printing out configurations
    void printData();
};


#endif //XSTORE_CONFIGSTRUCTS_H
