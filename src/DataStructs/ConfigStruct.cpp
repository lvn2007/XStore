#include "include/DataStructs/ConfigStruct.h"

void ConfigStruct::PrintData() {
    std::cout << "DB name: " << DbName << " | Time resolution: " << TimeResolution << " | Row length: " << RowLength << std::endl;
}