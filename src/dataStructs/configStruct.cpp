#include "include/dataStructs/configStruct.h"

void configStruct::printData() {
    std::cout << "DB name: " << dbName << " | Row length: " << rowLength << " | Time resolution: " << timeResolution << std::endl;
}