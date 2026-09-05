#include "include/Networking/ZMQ_Server.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_PreadPool.h"
#include "include/XStore/XStore_SysStats.h"
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

    program.add_argument("-s", "--stats-port")
        .default_value(0)
        .scan<'i', int>()
        .help("port for the host-stats PUB broadcast (<=0 or omitted defaults to --port minus 1)");

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

    int stats_port = program.get<int>("--stats-port");
    if (stats_port <= 0) {
        // Default convention: one below the main port (e.g. main 9497 ->
        // stats 9496) -- keeps the two sockets adjacent without requiring
        // a second required CLI arg for the common case. Still overridable
        // via -s/--stats-port for deployments where port-1 collides with
        // something else.
        try {
            stats_port = std::stoi(port) - 1;
        } catch (const std::exception &) {
            spdlog::error("[BOOT] - --port ('{}') is not numeric, cannot derive a default --stats-port; pass -s/--stats-port explicitly", port);
            exit(1);
        }
    }

    // Tells XStorePreadPool how many threads the ZMQ server is about to
    // claim, so its own worker count sizes off whatever's left over on
    // this box instead of independently sizing off hardware_concurrency()
    // too -- must run before ZmqServer starts (any query that reaches
    // BatchQuery starts the pread pool's threads on first use, and
    // that count is fixed from then on). See XStore_Tunables.h's
    // BATCH_QUERY_PREAD_POOL_SIZE_CAP comment for the regression this
    // fixes.
    XStorePreadPool::Init(static_cast<size_t>(thread_count));

    // Host-metrics broadcast: independent background thread + its own PUB
    // socket, started before the ZMQ server so it's already publishing by
    // the time any client (desktop viewer or otherwise) connects. Never
    // touches ZmqServer/XStore state -- see XStore_SysStats.h.
    SysStatsCollector::Start(host, stats_port);

    // Spin ZMQ Server
    ZmqServer x_store_zmq_server(thread_count);
    x_store_zmq_server.StartServer(host, port);
}
