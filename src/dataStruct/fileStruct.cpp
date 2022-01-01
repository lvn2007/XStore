#include "include/dataStruct/fileStruct.h"

// Constructor
fileStruct::fileStruct(std::string ipAddr, std::string dbName) {
    this->ipAddr = parseIP(ipAddr);
    this->dbName = dbName;
//    std::filesystem::current_path(
//    std::cout << std::filesystem::current_path() << std::endl;
}

std::string fileStruct::getID() {
    return ipAddr;
}

std::string fileStruct::parseIP(std::string ipAddr) {
    // Remove first 5 characters i.e., ipv4:
    ipAddr.erase(0, 5);

    // Locate ":" in string
    int colonIdx = ipAddr.find(":");

    // Remove from ":" location to end-of-string
    ipAddr.erase(colonIdx, ipAddr.size());

    return ipAddr;
}

//dataStruct::dataStruct(std::time_t unixTime, std::string data) {
//    this->unixTime = unixTime;
//    this->data = data;
//
//    // Ensuring no WHITESPACE in data
//    data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
//}
//
//// Returning a line
////  i.e., unixTime, values
//std::string dataStruct::getData() {
//    return std::to_string(unixTime) + "," + data + "\n";
//}
//
//// Printing out time-value
//void dataStruct::printData(bool humanTime) {
//    if (humanTime) {
//        std::cout << "Timestamp: " << ctime(&unixTime) << " | Data: " << data << std::endl;
//        return;
//    }
//    std::cout << "Timestamp: " << unixTime << " | Data: " << data << std::endl;
//}