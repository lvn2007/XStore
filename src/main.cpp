#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include <argparse/argparse.hpp>
#include <thread>

int main(int argc, char *argv[]) {
    spdlog::level::level_enum console_level = spdlog::level::warn;
    spdlog::level::level_enum file_level = spdlog::level::trace;
    unsigned short int flush_interval = 10;
    Logger logger_instance = Logger(console_level, file_level, flush_interval);

    argparse::ArgumentParser program("XStore");
    program.add_description(
        "XStore is a key-value/time-series store, served over ZMQ+protobuf");

    program.add_argument("-H", "--host")
        .required()
        .help("host/address to bind the server to");

    program.add_argument("-p", "--port")
        .required()
        .help("port to bind the server to");

    program.add_argument("-t", "--threads")
        .default_value(0)
        .scan<'i', int>()
        .help("worker thread count (<=0 or omitted falls back to hardware_concurrency)");

    try {
        program.parse_args(argc, argv);
    } catch (const std::exception &err) {
        spdlog::error("[BOOT] - {}", err.what());
        std::cerr << program;
        exit(1);
    }

    // Arguments parsing
    std::string host = program.get<std::string>("--host");
    std::string port = program.get<std::string>("--port");
    int thread_count = program.get<int>("--threads");
    if (thread_count <= 0) {
        // A non-positive thread count would launch a server with no workers
        // to ever answer a request, so fall back to the host's hardware
        // concurrency instead of silently doing that.
        int fallback = std::thread::hardware_concurrency();
        thread_count = fallback > 0 ? static_cast<int>(fallback) : 1;
        spdlog::warn("[BOOT] - Invalid thread count, falling back to {}", thread_count);
    }

    // Spin ZMQ Server
    ZmqServer x_store_zmq_server(thread_count);
    x_store_zmq_server.StartServer(host, port);
}
