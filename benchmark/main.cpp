#include "XStore/XStore_Adapter.h"
#include "MongoDB/MongoDB_Adapter.h"
#include "InfluxDB/InfluxDB_Adapter.h"
#include "ProtoBench/ProtoBench.h"
#include "include/utilities.h"
#include "include/argparse.hpp"
#include <thread>
#include "DataManager/Data_Manager.h"
#include <filesystem>

int main(int argc, char const *argv[])
{
    // Logging
    spdlog::level::level_enum CONSOLE_LEVEL = spdlog::level::info;
    spdlog::level::level_enum FILE_LEVEL = spdlog::level::trace;
    unsigned short int flushInterval = 10;
    Logger logger = Logger(CONSOLE_LEVEL, FILE_LEVEL, flushInterval);

    argparse::ArgumentParser program("X-Bench");

    // ---- Benchmark runner (QUERY)
    argparse::ArgumentParser queryBench("queryBench");
    queryBench.add_description("XBench – CRUD Benchmarking Suites for Time-Series database (QUERY)");

    //  Server IP address
    queryBench.add_argument("-i", "--ip_address").required()
            .help("IP address of target DB server")
            .default_value("127.0.0.1").nargs(1);

    //  Server Port number
    queryBench.add_argument("-p", "--port_number").required()
            .help("Port number of target DB server")
            .default_value("12345").nargs(1);

    //  Target DB
    queryBench.add_argument("-d", "--db_name").required()
            .help("Target DB Name")
            .default_value("XSTORE").nargs(1)
            .choices("XSTORE", "MONGODB", "INFLUXDB");

    //  Number of Threads
    queryBench.add_argument("-t", "--thread_count").required()
            .help("Number of client thread")
            .default_value(1).nargs(1)
            .scan<'i', unsigned short int>();

    //  Type of Experiment
    queryBench.add_argument("-e", "--experiment_type").required()
            .help("Experiment type")
            .default_value("UNARY_SEQ").nargs(1)
            .choices("UNARY_SEQ", "UNARY_RAND", "BATCH_SEQ", "BATCH_RAND");

    //  Number of Iterations
    queryBench.add_argument("-n", "--n_iteration").required()
            .help("Number of iterations")
            .default_value(1).nargs(1)
            .scan<'i', unsigned int>();

    //  Number of Batch Iterations
    queryBench.add_argument("-b", "--batch_iteration")
            .help("Number of batch iterations (Only applicable to RANGE/BATCH workloads)")
            .default_value(10).nargs(1)
            .scan<'i', unsigned int>();

    queryBench.add_argument("-ia", "--invoke_at").required()
            .help("Invoke benchmark at specific time (epoch time)")
            .default_value(0).nargs(1)
            .scan<'i', unsigned long int>();

    queryBench.add_argument("--debug").help("Debug mode")
            .default_value(false).implicit_value(true);

    // ---- Benchmark runner (INSERT)
    argparse::ArgumentParser insertBench("insertBench");
    insertBench.add_description("XBench – CRUD Benchmarking Suites for Time-Series database (INSERT)");

    //  Server IP address
    insertBench.add_argument("-i", "--ip_address").required()
            .help("IP address of target DB server")
            .default_value("127.0.0.1").nargs(1);

    //  Server Port number
    insertBench.add_argument("-p", "--port_number").required()
            .help("Port number of target DB server")
            .default_value("12345").nargs(1);

    //  Target DB
    insertBench.add_argument("-d", "--db_name").required()
            .help("Target DB Name")
            .default_value("XSTORE").nargs(1)
            .choices("XSTORE", "MONGODB", "INFLUXDB");

    //  Number of Threads
    insertBench.add_argument("-t", "--thread_count").required()
            .help("Number of client thread")
            .default_value(1).nargs(1)
            .scan<'i', short unsigned int>().nargs(1);

    //  Type of Experiment
    insertBench.add_argument("-e", "--experiment_type").required()
            .help("Experiment type")
            .default_value("UNARY_SEQ").nargs(1)
            .choices("UNARY_SEQ", "UNARY_RAND", "BATCH_SEQ", "BATCH_RAND");

    //  Number of Iterations
    insertBench.add_argument("-n", "--n_iteration").required()
            .help("Number of iterations")
            .default_value(1).nargs(1)
            .scan<'i', unsigned int>();

    //  Number of Batch Iterations
    insertBench.add_argument("-b", "--batch_iteration")
            .help("Number of batch iterations (Only applicable to RANGE/BATCH workloads)")
            .default_value(10).nargs(1)
            .scan<'i', unsigned int>();

    insertBench.add_argument("-ia", "--invoke_at").required()
            .help("Invoke benchmark at specific time (epoch time)")
            .default_value(0).nargs(1)
            .scan<'i', unsigned long int>();

    insertBench.add_argument("--debug").help("Debug mode")
            .default_value(false).implicit_value(true);

    // ---- Protobuf benchmark
    argparse::ArgumentParser protoBench("protoBench");
    protoBench.add_description("XBench - Benchmarking Google Protocol Buffer");

    //  Number of Entries
    protoBench.add_argument("-e", "--entries").required()
            .help("Number of entries")
            .default_value(1).nargs(1)
            .scan<'i', unsigned int>();

    //  Byte per Entry
    protoBench.add_argument("-s", "--byteSize")
            .help("Byte per entry")
            .default_value(32).nargs(1)
            .scan<'i', unsigned int>();

    //  Number of Iterations
    protoBench.add_argument("-n", "--n_iteration").required()
            .help("Number of iterations")
            .default_value(1).nargs(1)
            .scan<'i', unsigned int>();

    program.add_subparser(queryBench);
    program.add_subparser(insertBench);
    program.add_subparser(protoBench);

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    if (program.is_subcommand_used(queryBench)) {
        std::string SERVER_ADDR = queryBench.get<std::string>("-i");
        std::string SERVER_PORT = queryBench.get<std::string>("-p");
        std::string TARGET_DB = queryBench.get<std::string>("-d");
        toUpperCase(TARGET_DB.begin(), TARGET_DB.end());
        std::string EXPERIMENT_TYPE = queryBench.get<std::string>("-e");
        toUpperCase(EXPERIMENT_TYPE.begin(), EXPERIMENT_TYPE.end());
        short unsigned int NUM_THREAD = queryBench.get<unsigned short int>("-t");
        unsigned int N_ITERATIONS = queryBench.get<unsigned int>("-n");
        unsigned int B_ITERATIONS = queryBench.get<unsigned int>("-b");
        unsigned int INVOKE_AT = queryBench.get<unsigned long int>("-ia");
        bool IS_DEBUG = queryBench.get<bool>("--debug");

        // NUM_THREAD checking
        short unsigned int availableHT = std::thread::hardware_concurrency();
        if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
            spdlog::error("[ARGUMENTS] - Invalid THREAD_COUNT. Expecting: 1 <= THREAD_COUNT <= {}", availableHT);
            exit(1);
        }

        // Spawning Data Manager
        DataManager dataManager;

        // Benchmark data will be stored here
        std::vector<double> timeCost;

        if (EXPERIMENT_TYPE == "UNARY_SEQ") {
            spdlog::info("[UNARY QUERY]  Target DB: {}", TARGET_DB);
            spdlog::info("[UNARY QUERY]    Pattern: SEQUENTIAL");
            spdlog::info("[UNARY QUERY] Iterations: {}", N_ITERATIONS);
            spdlog::info("[UNARY QUERY]     Thread: {}", NUM_THREAD);
            spdlog::info("[UNARY QUERY]  Invoke at: {}", INVOKE_AT);
            spdlog::info("[UNARY QUERY]      Debug: {}", IS_DEBUG);

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-UNARY-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "UNARY_RAND") {
            spdlog::info("[UNARY QUERY]  Target DB: {}", TARGET_DB);
            spdlog::info("[UNARY QUERY]    Pattern: RANDOM");
            spdlog::info("[UNARY QUERY] Iterations: {}", N_ITERATIONS);
            spdlog::info("[UNARY QUERY]     Thread: {}", NUM_THREAD);
            spdlog::info("[UNARY QUERY]  Invoke at: {}", INVOKE_AT);
            spdlog::info("[UNARY QUERY]      Debug: {}", IS_DEBUG);

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-UNARY-RAND_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_SEQ") {
            spdlog::info("[BATCH QUERY]        Target DB: {}", TARGET_DB);
            spdlog::info("[BATCH QUERY]          Pattern: SEQUENTIAL");
            spdlog::info("[BATCH QUERY]       Iterations: {}", N_ITERATIONS);
            spdlog::info("[BATCH QUERY] Batch Iterations: {}", B_ITERATIONS);
            spdlog::info("[BATCH QUERY]           Thread: {}", NUM_THREAD);
            spdlog::info("[BATCH QUERY]        Invoke at: {}", INVOKE_AT);
            spdlog::info("[BATCH QUERY]            Debug: {}", IS_DEBUG);

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-BATCH-SEQ_Client-", NUM_THREAD, N_ITERATIONS, B_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::rangeQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::rangeQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::rangeQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_RAND") {
            spdlog::info("[BATCH QUERY]        Target DB: {}", TARGET_DB);
            spdlog::info("[BATCH QUERY]          Pattern: RANDOM");
            spdlog::info("[BATCH QUERY]       Iterations: {}", N_ITERATIONS);
            spdlog::info("[BATCH QUERY] Batch Iterations: {}", B_ITERATIONS);
            spdlog::info("[BATCH QUERY]           Thread: {}", NUM_THREAD);
            spdlog::info("[BATCH QUERY]        Invoke at: {}", INVOKE_AT);
            spdlog::info("[BATCH QUERY]            Debug: {}", IS_DEBUG);

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-BATCH-RAND_Client-", NUM_THREAD, N_ITERATIONS, B_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::batchQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::batchQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                spdlog::warn("[BATCH QUERY] InfluxQL doesn't support querying a batch of non-chronological timestamps");
                spdlog::warn("[BATCH QUERY] REF 1: https://docs.influxdata.com/influxdb/v1/query_language/explore-data/#syntax-1");
                spdlog::warn("[BATCH QUERY] REF 2: https://community.influxdata.com/t/querying-multiple-time-points/35388");
            }
        }

        // Display benchmark statistics
        if (timeCost.size() > 0) {
            printStatistics(timeCost);
        }
    }
    else if (program.is_subcommand_used(insertBench)) {
        std::string SERVER_ADDR = insertBench.get<std::string>("-i");
        std::string SERVER_PORT = insertBench.get<std::string>("-p");
        std::string TARGET_DB = insertBench.get<std::string>("-d");
        toUpperCase(TARGET_DB.begin(), TARGET_DB.end());
        std::string EXPERIMENT_TYPE = insertBench.get<std::string>("-e");
        toUpperCase(EXPERIMENT_TYPE.begin(), EXPERIMENT_TYPE.end());
        short unsigned int NUM_THREAD = insertBench.get<unsigned short int>("-t");
        unsigned int N_ITERATIONS = insertBench.get<unsigned int>("-n");
        unsigned int B_ITERATIONS = insertBench.get<unsigned int>("-b");
        unsigned int INVOKE_AT = insertBench.get<unsigned long int>("-ia");
        bool IS_DEBUG = insertBench.get<bool>("--debug");

        // NUM_THREAD checking
        short unsigned int availableHT = std::thread::hardware_concurrency();
        if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
            spdlog::error("[ARGUMENTS] - Invalid THREAD_COUNT. Expecting: 1 <= THREAD_COUNT <= {}", availableHT);
            exit(1);
        }

        // Benchmark data will be stored here
        std::vector<double> timeCost;

        // Spawning Data Manager
        DataManager dataManager;

        if (EXPERIMENT_TYPE == "UNARY_SEQ") {
            spdlog::info("[UNARY INSERT]  Target DB: {}", TARGET_DB);
            spdlog::info("[UNARY INSERT]    Pattern: SEQUENTIAL");
            spdlog::info("[UNARY INSERT] Iterations: {}", N_ITERATIONS);
            spdlog::info("[UNARY INSERT]     Thread: {}", NUM_THREAD);
            spdlog::info("[UNARY INSERT]  Invoke at: {}", INVOKE_AT);
            spdlog::info("[UNARY INSERT]      Debug: {}", IS_DEBUG);

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-UNARY-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "UNARY_RAND") {
            spdlog::info("[UNARY INSERT]  Target DB: {}", TARGET_DB);
            spdlog::info("[UNARY INSERT]    Pattern: RANDOM");
            spdlog::info("[UNARY INSERT] Iterations: {}", N_ITERATIONS);
            spdlog::info("[UNARY INSERT]     Thread: {}", NUM_THREAD);
            spdlog::info("[UNARY INSERT]  Invoke at: {}", INVOKE_AT);
            spdlog::info("[UNARY INSERT]      Debug: {}", IS_DEBUG);

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-UNARY-RAND_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_SEQ") {
            spdlog::info("[BATCH INSERT]        Target DB: {}", TARGET_DB);
            spdlog::info("[BATCH INSERT]          Pattern: SEQUENTIAL");
            spdlog::info("[BATCH INSERT]       Iterations: {}", N_ITERATIONS);
            spdlog::info("[BATCH INSERT] Batch Iterations: {}", B_ITERATIONS);
            spdlog::info("[BATCH INSERT]           Thread: {}", NUM_THREAD);
            spdlog::info("[BATCH INSERT]        Invoke at: {}", INVOKE_AT);
            spdlog::info("[BATCH INSERT]            Debug: {}", IS_DEBUG);

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-BATCH-SEQ_Client-", NUM_THREAD, N_ITERATIONS, B_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::rangeInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::batchInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::batchInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_RAND") {
            spdlog::info("[BATCH INSERT]        Target DB: {}", TARGET_DB);
            spdlog::info("[BATCH INSERT]          Pattern: RANDOM");
            spdlog::info("[BATCH INSERT]       Iterations: {}", N_ITERATIONS);
            spdlog::info("[BATCH INSERT] Batch Iterations: {}", B_ITERATIONS);
            spdlog::info("[BATCH INSERT]           Thread: {}", NUM_THREAD);
            spdlog::info("[BATCH INSERT]        Invoke at: {}", INVOKE_AT);
            spdlog::info("[BATCH INSERT]            Debug: {}", IS_DEBUG);

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-BATCH-RAND_Client-", NUM_THREAD, N_ITERATIONS, B_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::batchInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "MONGODB") {
                timeCost = MongoDB_Adapter::batchInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
            else if (TARGET_DB == "INFLUXDB") {
                timeCost = InfluxDB_Adapter::batchInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, B_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE, IS_DEBUG);
            }
        }

        // Display benchmark statistics
        if (timeCost.size() > 0) {
            printStatistics(timeCost);
        }
    }
    else if (program.is_subcommand_used(protoBench)) {
        unsigned int N_ENTRIES = protoBench.get<unsigned int>("-e");
        unsigned int BYTE_SIZE = protoBench.get<unsigned int>("-s");
        unsigned int N_ITERATIONS = protoBench.get<unsigned int>("-n");

        spdlog::info("[PROTO BENCH]     Entries: {}", N_ENTRIES);
        spdlog::info("[PROTO BENCH]   Byte Size: {}", BYTE_SIZE);
        spdlog::info("[PROTO BENCH]  Iterations: {}", N_ITERATIONS);
        spdlog::info("[PROTO BENCH]    Row Size: {}B", 8 + (N_ENTRIES * BYTE_SIZE));

        ProtoBench protoBench;
        std::vector<std::vector<double>> protoCosts = protoBench.run(N_ENTRIES, BYTE_SIZE, N_ITERATIONS);

        // Serialize cost statistics
        if (protoCosts.at(0).size() > 0) {
            spdlog::info("[PROTO BENCH] SERIALIZATION COST");
            printStatistics(protoCosts.at(0));

            // Deserialize cost statistics
            spdlog::info("[PROTO BENCH] DE-SERIALIZATION COST");
            printStatistics(protoCosts.at(1));
        }
    }
}