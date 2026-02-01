#include "include/DataStructs/configStruct.h"

void configStruct::printData() {
    std::cout << "DB name: " << dbName << " | Time resolution: " << timeResolution << " | Row length: " << rowLength << std::endl;
}