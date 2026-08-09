#include "include/DataStructs/ConfigStruct.h"

void ConfigStruct::PrintData() {
    std::cout << "DB name: " << dbName << " | Time resolution: " << timeResolution << " | Row length: " << rowLength << std::endl;
}