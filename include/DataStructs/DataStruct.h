#ifndef XSTORE_DATASTRUCTS_H
#define XSTORE_DATASTRUCTS_H

#include <iostream>
#include <filesystem>
#include <algorithm>
#include <string>

// Data structure to hold time-value together
struct DataStruct {
    std::time_t UnixTime;
    std::string Data;

    // Constructor
    DataStruct(std::time_t unixTime, std::string data);

    // Returning a line
    //  i.e., unixTime, values
    std::string GetData();

    // Printing out time-value
    void PrintData(bool humanTime);
};

struct ValueStruct {
    unsigned int Index;
    long double Value;
    bool IsInitialized = false;
};

#endif //XSTORE_DATASTRUCTS_H
