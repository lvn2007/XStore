#ifndef XSTORE_CLIENTCMDSTRUCT_H
#define XSTORE_CLIENTCMDSTRUCT_H

#include <iostream>
#include <filesystem>

struct clientCmdStruct {
    std::string requestType;
    std::time_t startTime;
    std::time_t endTime;
    std::string dbName;

    // Constructor
    clientCmdStruct(std::string requestType, std::time_t startTime, std::time_t endTime, std::string dbName);
};

#endif //XSTORE_CLIENTCMDSTRUCT_H
