#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include <argparse/argparse.hpp>

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
        .help("worker thread count (<=0 or omitted defaults to one per physical core)");

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
        // Default to one worker per PHYSICAL core (not this host's
        // logical/SMT-inclusive hardware_concurrency()) -- measured best
        // on real hardware: going past physical core count leaves no free
        // SMT sibling lane for the WAL writer threads to run on relatively
        // uncontended, ~6% worse p50 under real concurrent load in that
        // configuration (see ZmqServer::DefaultThreadCount()'s comment,
        // ZMQ_Server.cpp). Still user-overridable via -t/--threads --
        // don't assume this default is right for every deployment's own
        // hardware/workload without benchmarking it there too.
        thread_count = ZmqServer::DefaultThreadCount();
        spdlog::info("[BOOT] - No/invalid thread count given, defaulting to {} (one per physical core)", thread_count);
    }

    // Spin ZMQ Server
    ZmqServer x_store_zmq_server(thread_count);
    x_store_zmq_server.StartServer(host, port);
}
