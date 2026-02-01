#include "include/Utilities/Utilities.h"
#include <fstream>
#include <iostream>
#include <ifaddrs.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <sys/socket.h>
#include <cstring>
#include <filesystem>
#include <cstdlib>
#include "spdlog/spdlog.h"

void Utilities::ReadJSON(const std::string& filename, nlohmann::json &jsonOutput)
{
    std::ifstream file(filename);
    jsonOutput = nlohmann::json::parse(file);
}

void Utilities::GetLocalIPAddresses(std::vector<std::string> *OutputVector)
{
    ifaddrs *ifa;
    getifaddrs(&ifa);

    for (; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET || (ifa->ifa_flags & IFF_LOOPBACK))
        {
            continue;
        }
        OutputVector->emplace_back(inet_ntoa(reinterpret_cast<sockaddr_in*>(ifa->ifa_addr)->sin_addr));
    }
}

void Utilities::InitWrkDir(const std::string& ServerName, const std::string& JsonConfigFile)
{
    std::string CurrentDir = std::filesystem::current_path().string();

    // Check for JSON config existence
    if (!std::filesystem::exists(JsonConfigFile)) {
        spdlog::error("{} is missing from {}", JsonConfigFile, CurrentDir);
        exit(1);
    }

    // Create MetadataServer DataServer DataStore
    std::vector<std::filesystem::path> DirsToCreate = {"MetadataServer", "DataServer", "DataStore"};

    try {
        for (const std::filesystem::path &dir : DirsToCreate) {
            if (std::filesystem::create_directory(dir)) {
                spdlog::info("Created directory {}", dir.string());
            }
        }
    }
    catch (const std::exception &e) {
        std::cerr << e.what() << std::endl;
        exit(1);
    }
}

std::vector<std::string> Utilities::SplitString(const std::string &input, const std::string &delimiter)
{
    std::vector<std::string> output;

    size_t last, next;
    last = next = 0;

    // Find the delimiter and extract substrings
    while ((next = input.find(delimiter, last)) != std::string::npos) {
        output.push_back(input.substr(last, next - last));
        last = next + delimiter.length();
    }

    // Push the last token after the last delimiter
    output.push_back(input.substr(last));

    return output;
}

float Utilities::GetLoadAvg()
{
    double LoadAvg[3];
    getloadavg(LoadAvg, 3);
    return static_cast<float>(LoadAvg[0]);
}