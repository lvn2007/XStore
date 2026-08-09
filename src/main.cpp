#include <iostream>
#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include <cmath>
#include <variant>
#include <fstream>
#include <bitset>

// Clock init
std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
std::chrono::duration<double> diff;

struct BenchData {
    double usec;
    size_t OGLength;
    size_t compLength;

    BenchData(double usecTimeTaken, size_t originalLength, size_t compressedLength) {
        usec = usecTimeTaken;
        OGLength = originalLength;
        compLength = compressedLength;
    }

    std::string toString() {
        return std::to_string(usec) + "," + std::to_string(OGLength) + "," + std::to_string(compLength) + "\n";
    }
};

// for string delimiter
std::vector<std::string> split(std::string s, std::string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
        token = s.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back (token);
    }

    res.push_back (s.substr (pos_start));
    return res;
}

int main(int argc, char *argv[]) {
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] - Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] - Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

    // ----DEV AREA
    spdlog::level::level_enum CONSOLE_LEVEL = spdlog::level::warn;
    spdlog::level::level_enum FILE_LEVEL = spdlog::level::trace;
    unsigned short int flushInterval = 10;
    Logger logger = Logger(CONSOLE_LEVEL, FILE_LEVEL, flushInterval);

    // Spin ZMQ Server
    ZMQ_Server XStoreZmqServer(THREAD_COUNT);
    XStoreZmqServer.StartServer(BIND_ADDR, BIND_PORT);
}
