#include <iostream>
#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include <thread>

int main(int argc, char *argv[]) {
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] - Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] - Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string bind_addr = argv[1];
    std::string bind_port = argv[2];
    int thread_count = atoi(argv[3]);
    if (thread_count <= 0) {
        // atoi() returns 0 on a non-numeric/missing argument as well as on a
        // literal "0" -- either way a non-positive thread count would launch
        // a server with no workers to ever answer a request, so fall back to
        // the host's hardware concurrency instead of silently doing that.
        unsigned int fallback = std::thread::hardware_concurrency();
        thread_count = fallback > 0 ? static_cast<int>(fallback) : 1;
        std::cout << "[WARNING] [BOOT] - Invalid thread count '" << argv[3]
                  << "', falling back to " << thread_count << std::endl;
    }

    // ----DEV AREA
    spdlog::level::level_enum console_level = spdlog::level::warn;
    spdlog::level::level_enum file_level = spdlog::level::trace;
    unsigned short int flush_interval = 10;
    Logger logger_instance = Logger(console_level, file_level, flush_interval);

    // Spin ZMQ Server
    ZmqServer x_store_zmq_server(thread_count);
    x_store_zmq_server.StartServer(bind_addr, bind_port);
}