#include "XStore/XStore_Adapter.h"
#include "include/utilities.h"
#include "include/argparse.hpp"
#include <thread>
#include "DataManager/Data_Manager.h"
#include <filesystem>

int main(int argc, char const *argv[])
{
    argparse::ArgumentParser program("X-Bench");

    // ---- Data LOAD ALL
    argparse::ArgumentParser dataLoader("loader");
    dataLoader.add_description("Data Loader – Loading all data from file to DB");

    //  Server IP address
    dataLoader.add_argument("-i", "--ip_address").required()
            .help("IP address of target DB server")
            .default_value("127.0.0.1").nargs(1);

    //  Server Port number
    dataLoader.add_argument("-p", "--port_number").required()
            .help("Port number of target DB server")
            .default_value("12345").nargs(1);

    //  Target DB
    dataLoader.add_argument("-d", "--db_name").required()
            .help("Target DB Name")
            .default_value("XSTORE").nargs(1);

    //  Number of Threads
    dataLoader.add_argument("-t", "--thread_count").required()
            .help("Number of client thread")
            .default_value(1).nargs(1)
            .scan<'i', short unsigned int>().nargs(1);

    //  Data file name to be read
    dataLoader.add_argument("-f", "--file_name").required()
            .help("Data file name (.parquet format)")
            .default_value("hashData.parquet").nargs(1);

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
            .choices("XSTORE", "MONGODB");

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

    queryBench.add_argument("-ia", "--invoke_at").required()
            .help("Invoke benchmark at specific time (epoch time)")
            .default_value(0).nargs(1)
            .scan<'i', unsigned long int>();

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
            .choices("XSTORE", "MONGODB");

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

    insertBench.add_argument("-ia", "--invoke_at").required()
            .help("Invoke benchmark at specific time (epoch time)")
            .default_value(0).nargs(1)
            .scan<'i', unsigned long int>();

    program.add_subparser(dataLoader);
    program.add_subparser(queryBench);
    program.add_subparser(insertBench);

    try {
        program.parse_args(argc, argv);
    }
    catch (const std::runtime_error& err) {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        return 1;
    }

    if (program.is_subcommand_used(dataLoader)) {
        std::string SERVER_ADDR = dataLoader.get<std::string>("-i");
        std::string SERVER_PORT = dataLoader.get<std::string>("-p");
        std::string TARGET_DB = toUpper(dataLoader.get<std::string>("-d"));
        short unsigned int NUM_THREAD = dataLoader.get<unsigned short int>("-t");
        std::string DATASET = dataLoader.get<std::string>("-f");

        // NUM_THREAD checking
        short unsigned int availableHT = std::thread::hardware_concurrency();
        if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
            std::cerr << "[ERROR - ARGUMENTS] -- Invalid NUM THREAD" << std::endl;
            std::cerr << "Expecting: 1 <= NUM_THREAD <= " << availableHT << std::endl;
            exit(1);
        }

        // Benchmark data will be stored here
        std::vector<double> timeCost;

        std::cout << "----- [DATA LOADING] -----" << std::endl;
        std::cout << "[LOAD] Target DB: " << TARGET_DB << std::endl;
        std::cout << "[LOAD] Performing sequential WRITE" << std::endl;
        std::cout << "[LOAD] Thread: " << NUM_THREAD << std::endl;
        std::cout << "[LOAD] Dataset: " << DATASET << std::endl;

        // Spawning Data Manager
        DataManager dataManager;
        std::vector<std::vector<std::any>> insertData;

        // Read data from parquet file
        arrow::Status readData = dataManager.readParquet(DATASET, &insertData);

        if (TARGET_DB == "XSTORE") {
            timeCost = XStore_Adapter::loadAll(SERVER_ADDR, SERVER_PORT, NUM_THREAD, insertData);
        }
    }
    else if (program.is_subcommand_used(queryBench)) {
        std::string SERVER_ADDR = queryBench.get<std::string>("-i");
        std::string SERVER_PORT = queryBench.get<std::string>("-p");
        std::string TARGET_DB = toUpper(queryBench.get<std::string>("-d"));
        std::string EXPERIMENT_TYPE = toUpper(queryBench.get<std::string>("-e"));
        short unsigned int NUM_THREAD = queryBench.get<unsigned short int>("-t");
        unsigned int N_ITERATIONS = queryBench.get<unsigned int>("-n");
        unsigned int INVOKE_AT = queryBench.get<unsigned long int>("-ia");

        // NUM_THREAD checking
        short unsigned int availableHT = std::thread::hardware_concurrency();
        if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
            std::cerr << "[ERROR - ARGUMENTS] -- Invalid NUM THREAD" << std::endl;
            std::cerr << "Expecting: 1 <= NUM_THREAD <= " << availableHT << std::endl;
            exit(1);
        }

        // Spawning Data Manager
        DataManager dataManager;

        // Benchmark data will be stored here
        std::vector<double> timeCost;

        if (EXPERIMENT_TYPE == "UNARY_SEQ") {
            std::cout << "----- [UNARY QUERY] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: SEQUENTIAL" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-UNARY-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE);
            }
        }
        else if (EXPERIMENT_TYPE == "UNARY_RAND") {
            std::cout << "----- [UNARY QUERY] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: RANDOM" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-UNARY-RAND_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_SEQ") {
            std::cout << "----- [BATCH QUERY] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: SEQUENTIAL" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-BATCH-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::rangeQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_RAND") {
            std::cout << "----- [BATCH QUERY] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: RANDOM" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> queryWorkload = dataManager.readQueryWorkload("BASIC-QUERY-BATCH-RAND_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::batchQuery(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &queryWorkload, EXPERIMENT_TYPE);
            }
        }
        else {
            std::cerr << "[ERROR - BENCHMARK] Error encountered. Workload is not defined" << std::endl;
            exit(1);
        }

        // Display benchmark statistics
        printStatistics(timeCost);
    }
    else if (program.is_subcommand_used(insertBench)) {
        std::string SERVER_ADDR = insertBench.get<std::string>("-i");
        std::string SERVER_PORT = insertBench.get<std::string>("-p");
        std::string TARGET_DB = toUpper(insertBench.get<std::string>("-d"));
        std::string EXPERIMENT_TYPE = toUpper(insertBench.get<std::string>("-e"));
        short unsigned int NUM_THREAD = insertBench.get<unsigned short int>("-t");
        unsigned int N_ITERATIONS = insertBench.get<unsigned int>("-n");
        unsigned int INVOKE_AT = insertBench.get<unsigned long int>("-ia");

        // NUM_THREAD checking
        short unsigned int availableHT = std::thread::hardware_concurrency();
        if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
            std::cerr << "[ERROR - ARGUMENTS] -- Invalid NUM THREAD" << std::endl;
            std::cerr << "Expecting: 1 <= NUM_THREAD <= " << availableHT << std::endl;
            exit(1);
        }

        // Benchmark data will be stored here
        std::vector<double> timeCost;

        // Spawning Data Manager
        DataManager dataManager;

        if (EXPERIMENT_TYPE == "UNARY_SEQ") {
            std::cout << "----- [UNARY INSERT] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: SEQUENTIAL" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-UNARY-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE);
            }
        }
        else if (EXPERIMENT_TYPE == "UNARY_RAND") {
            std::cout << "----- [UNARY INSERT] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: RANDOM" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-UNARY-RAND_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::unaryInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE);
            }
        }
        else if (EXPERIMENT_TYPE == "BATCH_SEQ") {
            std::cout << "----- [BATCH INSERT] -----" << std::endl;
            std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
            std::cout << "[BENCHMARK] Pattern: SEQUENTIAL" << std::endl;
            std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
            std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;
            std::cout << "[BENCHMARK] Invoke at: " << INVOKE_AT << std::endl;

            std::vector<std::vector<std::any>> insertData;

            // Read data from parquet file
            std::vector<std::vector<std::vector<std::any>>> insertWorkload = dataManager.readInsertWorkload("BASIC-INSERT-BATCH-SEQ_Client-", NUM_THREAD, N_ITERATIONS);

            // Poll till
            epochPoller(INVOKE_AT);

            if (TARGET_DB == "XSTORE") {
                timeCost = XStore_Adapter::rangeInsert(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, &insertWorkload, EXPERIMENT_TYPE);
            }
        }
        else {
            std::cerr << "[ERROR - BENCHMARK] Error encountered. Workload is not defined" << std::endl;
            exit(1);
        }

        // Display benchmark statistics
        printStatistics(timeCost);
    }
}