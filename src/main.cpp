#include <iostream>
#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include <cmath>
#include <variant>
#include <fstream>
#include <bitset>

// Clock init
std::chrono::time_point<std::chrono::high_resolution_clock> Start, End;
std::chrono::duration<double> Diff;

struct BenchData {
    double Usec;
    size_t OGLength;
    size_t CompLength;

    BenchData(double usecTimeTaken, size_t originalLength, size_t compressedLength) {
        Usec = usecTimeTaken;
        OGLength = originalLength;
        CompLength = compressedLength;
    }

    std::string ToString() {
        return std::to_string(Usec) + "," + std::to_string(OGLength) + "," + std::to_string(CompLength) + "\n";
    }
};

// for string delimiter
std::vector<std::string> Split(std::string s, std::string delimiter) {
    size_t PosStart = 0, PosEnd, DelimLen = delimiter.length();
    std::string Token;
    std::vector<std::string> Res;

    while ((PosEnd = s.find(delimiter, PosStart)) != std::string::npos) {
        Token = s.substr (PosStart, PosEnd - PosStart);
        PosStart = PosEnd + DelimLen;
        Res.push_back (Token);
    }

    Res.push_back (s.substr (PosStart));
    return Res;
}

int main(int argc, char *argv[]) {
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] - Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] - Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BindAddr = argv[1];
    std::string BindPort = argv[2];
    int ThreadCount = atoi(argv[3]);

    // ----DEV AREA
    spdlog::level::level_enum ConsoleLevel = spdlog::level::warn;
    spdlog::level::level_enum FileLevel = spdlog::level::trace;
    unsigned short int FlushInterval = 10;
    Logger LoggerInstance = Logger(ConsoleLevel, FileLevel, FlushInterval);

    // Spin ZMQ Server
    ZMQ_Server XStoreZmqServer(ThreadCount);
    XStoreZmqServer.StartServer(BindAddr, BindPort);
}
