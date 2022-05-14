#ifndef XSTORE_CLIENTCMDSTRUCTS_H
#define XSTORE_CLIENTCMDSTRUCTS_H

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

#endif //XSTORE_CLIENTCMDSTRUCTS_H
