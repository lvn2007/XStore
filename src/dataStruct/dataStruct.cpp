#include "include/dataStruct/dataStruct.h"

// Constructor
dataStruct::dataStruct(std::time_t unixTime, std::string data) {
    this->unixTime = unixTime;
    this->data = data;

    // Ensuring no WHITESPACE in data
    data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
}

// Returning a line
//  i.e., unixTime, values
std::string dataStruct::getData() {
    return std::to_string(unixTime) + "," + data + "\n";
}

// Printing out time-value
void dataStruct::printData(bool humanTime) {
    if (humanTime) {
        std::cout << "Timestamp: " << ctime(&unixTime) << " | Data: " << data << std::endl;
        return;
    }
    std::cout << "Timestamp: " << unixTime << " | Data: " << data << std::endl;
}