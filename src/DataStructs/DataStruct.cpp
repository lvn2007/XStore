#include "include/DataStructs/DataStruct.h"

// Constructor
DataStruct::DataStruct(std::time_t unixTime, std::string data) {
    this->UnixTime = unixTime;
    this->Data = data;

    // Ensuring no WHITESPACE in data
    data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
}

// Returning a line
//  i.e., unixTime, values
std::string DataStruct::GetData() {
    return Data;
}

// Printing out time-value
void DataStruct::PrintData(bool humanTime) {
    if (humanTime) {
        std::cout << "Timestamp: " << ctime(&UnixTime) << " | Data: " << Data << std::endl;
        return;
    }
    std::cout << "Timestamp: " << UnixTime << " | Data: " << Data << std::endl;
}