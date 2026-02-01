#include <iostream>
#include "include/Networking/XStore_MetadataServer.h"
#include "include/Networking/XStore_DataServer.h"
#include "include/XStore/XStore.h"
#include "include/Argparse/argparse.hpp"
#include "include/Utilities/Utilities.h"
#include <fstream>

// Clock init
std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
std::chrono::duration<double> diff;

int main(int argc, char *argv[]) {
    // Argparse
    argparse::ArgumentParser program("XStore");

    // --- Metadata Server
    argparse::ArgumentParser MetadataServer("MetadataServer");
    MetadataServer.add_description("XStore – Metadata Server manages distributed data nodes and provide unified interface to clients");

    //  Server IP Address
    MetadataServer.add_argument("-i", "--ip_address").required()
            .help("Metadata Server's operating ip address")
            .default_value("0.0.0.0").nargs(1);

    //  Server Port Number
    MetadataServer.add_argument("-p", "--port_number").required()
                .help("Metadata Server's operating port number")
                .default_value("9497").nargs(1);

    //  Number of Threads
    MetadataServer.add_argument("-t", "--thread_count").required()
            .help("Metadata Server's operating thread count")
            .default_value(1).nargs(1)
            .scan<'i', unsigned short int>();

    //  Config file name
    MetadataServer.add_argument("-c", "--config_file").required()
            .help("Metadata Server's operating config file name")
            .default_value("ServerConfigs.json").nargs(1);

    // --- Metadata Server
    argparse::ArgumentParser DataServer("DataServer");
    DataServer.add_description("XStore – Data Server manages amd stores actual data and provide access to clients");

    //  Server IP Address
    DataServer.add_argument("-i", "--ip_address").required()
            .help("Data Server's operating ip address")
            .default_value("0.0.0.0").nargs(1);

    //  Server Port Number
    DataServer.add_argument("-p", "--port_number").required()
                .help("Data Server's operating port number")
                .default_value("9497").nargs(1);

    //  Number of Threads
    DataServer.add_argument("-t", "--thread_count").required()
            .help("Data Server's operating thread count")
            .default_value(1).nargs(1)
            .scan<'i', unsigned short int>();

    //  Config file name
    DataServer.add_argument("-c", "--config_file").required()
            .help("Data Server's operating config file name")
            .default_value("ServerConfigs.json").nargs(1);

    // Arguments parsing
    program.add_subparser(MetadataServer);
    program.add_subparser(DataServer);

    try {
        program.parse_args(argc, argv);
    } catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    // Initialize logging
    spdlog::level::level_enum CONSOLE_LEVEL = spdlog::level::warn;
    spdlog::level::level_enum FILE_LEVEL = spdlog::level::trace;
    unsigned short int flushInterval = 10;

    // Common variables
    std::string SERVER_NAME, SERVER_ADDR, SERVER_PORT, CONFIG_FILENAME;
    unsigned short int THREAD_COUNT;

    if (program.is_subcommand_used(MetadataServer))
    {
        SERVER_NAME = "METADATA SERVER";
        SERVER_ADDR = MetadataServer.get<std::string>("-i");
        SERVER_PORT = MetadataServer.get<std::string>("-p");
        THREAD_COUNT = MetadataServer.get<unsigned short int>("-t");
        CONFIG_FILENAME = MetadataServer.get<std::string>("-c");

        // Logging
        Logger logger = Logger(SERVER_NAME, CONSOLE_LEVEL, FILE_LEVEL, flushInterval);

        // Init working environment
        Utilities::InitWrkDir(SERVER_NAME, CONFIG_FILENAME);

        // Spin ZMQ Server
        XStore_MetadataServer XStore_MetadataServer(THREAD_COUNT);
        XStore_MetadataServer.StartServer(SERVER_ADDR, SERVER_PORT, CONFIG_FILENAME);
    }
    else if (program.is_subcommand_used(DataServer))
    {
        SERVER_NAME = "DATA SERVER";
        SERVER_ADDR = DataServer.get<std::string>("-i");
        SERVER_PORT = DataServer.get<std::string>("-p");
        THREAD_COUNT = DataServer.get<unsigned short int>("-t");
        CONFIG_FILENAME = DataServer.get<std::string>("-c");

        // Logging
        Logger logger = Logger(SERVER_NAME, CONSOLE_LEVEL, FILE_LEVEL, flushInterval);

        // Init working environment
        Utilities::InitWrkDir(SERVER_NAME, CONFIG_FILENAME);

        // Spin ZMQ Server
        XStore_DataServer XStore_DataServer(THREAD_COUNT);
        XStore_DataServer.StartServer(SERVER_ADDR, SERVER_PORT, CONFIG_FILENAME);
    }
}
