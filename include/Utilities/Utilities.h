#ifndef UTILITIES_H
#define UTILITIES_H
#include <string>
#include "include/JSON/json.hpp"

class Utilities {
public:
    static void ReadJSON(const std::string& filename, nlohmann::json &jsonOutput);
    static void GetLocalIPAddresses(std::vector<std::string> *OutputVector);
    static void InitWrkDir(const std::string& ServerName, const std::string& JsonConfigFile);
    static std::vector<std::string> SplitString(const std::string &input, const std::string &delimiter);
    static float GetLoadAvg();
};

#endif //UTILITIES_H
