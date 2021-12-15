#ifndef XSTORE_DATASTRUCT_H
#define XSTORE_DATASTRUCT_H

#include <iostream>
#include <filesystem>
#include <algorithm>
#include <string>

// Data structure to hold time-value together
struct dataStruct {
    std::time_t unixTime;
    std::string data;

    // Constructor
    dataStruct(std::time_t unixTime, std::string data);

    // Returning a line
    //  i.e., unixTime, values
    std::string getData();

    // Printing out time-value
    void printData(bool humanTime);
};

#endif //XSTORE_DATASTRUCT_H
