#ifndef XSTORE_CONFIGSTRUCTS_H
#define XSTORE_CONFIGSTRUCTS_H

#include <iostream>
#include <string>
#include <vector>

struct ConfigStruct {
    // Limit dbName to 256 chars
    char DbName[256];
    char TimeResolution[12];
    short int RowLength;
    std::vector<std::string> DTypeSchema;

    // Printing out configurations
    void PrintData();
};


#endif //XSTORE_CONFIGSTRUCTS_H
