#include "include/dataStruct/clientCmdStruct.h"

// Constructor
clientCmdStruct::clientCmdStruct(std::string requestType, time_t startTime, time_t endTime, std::string dbName) {
    this->requestType = requestType;
    this->startTime = startTime;
    this->endTime = endTime;
    this->dbName = dbName;
}