#include "include/DataStructs/DataStruct.h"

// Constructor
DataStruct::DataStruct(std::time_t unixTime, std::string data) {
    this->unixTime = unixTime;
    this->data = data;

    // Ensuring no WHITESPACE in data
    data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
}

// Returning a line
//  i.e., unixTime, values
std::string DataStruct::GetData() {
    return data;
}

// Printing out time-value
void DataStruct::PrintData(bool humanTime) {
    if (humanTime) {
        std::cout << "Timestamp: " << ctime(&unixTime) << " | Data: " << data << std::endl;
        return;
    }
    std::cout << "Timestamp: " << unixTime << " | Data: " << data << std::endl;
}