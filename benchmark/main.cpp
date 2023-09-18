#include "XStore/XStore_Adapter.h"
#include "../include/utilities.h"
#include <thread>

int main(int argc, char const *argv[])
{
    // Handle arguments
    if (argc < 6) {
        std::cerr << "[ERROR - Arguments] -- Missing required (4) arguments" << std::endl;
        std::cerr << "Usage: XStore_Client <SERVER ADDR> <SERVER PORT> <EXPERIMENT TYPE> <TARGET DB> <NUM THREAD>" << std::endl;
        std::cerr << "\nAvaiable EXPERIMENT TYPE: \n   - QUERY_A\n   - QUERY_B" << std::endl;
        std::cerr << "\nAvailable TARGET DB: \n   - XSTORE\n   - MONGODB" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string SERVER_ADDR = argv[1];
    std::string SERVER_PORT = argv[2];
    std::string EXPERIMENT_TYPE = argv[3];
    std::string TARGET_DB = argv[4];
    int NUM_THREAD = atoi(argv[5]);

    const std::string ALLOWED_EXPERIMENT[] = {"QUERY_A", "QUERY_B"};
    if (std::find(std::begin(ALLOWED_EXPERIMENT), std::end(ALLOWED_EXPERIMENT), EXPERIMENT_TYPE) == std::end(ALLOWED_EXPERIMENT)) {
        std::cerr << "[ERROR - Arguments] -- Invalid EXPERIMENT TYPE" << std::endl;
        std::cerr << "Available EXPERIMENT TYPE: \n   - QUERY_A\n   - QUERY_B" << std::endl;
        exit(1);
    }

    const std::string ALLOWED_DB[] = {"XSTORE", "MONGODB"};
    if (std::find(std::begin(ALLOWED_DB), std::end(ALLOWED_DB), TARGET_DB) == std::end(ALLOWED_DB)) {
        std::cerr << "[ERROR - Arguments] -- Invalid TARGET DB" << std::endl;
        std::cerr << "Available TARGET DB: \n   - XSTORE\n   - MONGODB" << std::endl;
        exit(1);
    }

    // NUM_THREAD checking
    unsigned int availableHT = std::thread::hardware_concurrency();
    if (NUM_THREAD < 1 || NUM_THREAD > availableHT) {
        std::cerr << "[ERROR - Arguments] -- Invalid NUM THREAD" << std::endl;
        std::cerr << "Expecting: 1 <= NUM_THREAD <= " << availableHT << std::endl;
        exit(1);
    }

    // Benchmark data will be stored here
    std::vector<double> timeCost;

    if (EXPERIMENT_TYPE == "QUERY_A") {
        int N_ITERATION = 1000;
        std::cout << "----- [QUERY_A] -----" << std::endl;
        std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
        std::cout << "[BENCHMARK] Performing sequential READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATION << std::endl;
        std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;

        if (TARGET_DB == "XSTORE") {
            timeCost = XStore_Adapter::queryA(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATION, 1451606400);
        }
    }
    else if (EXPERIMENT_TYPE == "QUERY_B") {
        int N_ITERATIONS = 1000;
        std::cout << "----- [QUERY_B] -----" << std::endl;
        std::cout << "[BENCHMARK] Target DB: " << TARGET_DB << std::endl;
        std::cout << "[BENCHMARK] Performing random READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
        std::cout << "[BENCHMARK] Thread: " << NUM_THREAD << std::endl;

        if (TARGET_DB == "XSTORE") {
            timeCost = XStore_Adapter::queryB(SERVER_ADDR, SERVER_PORT, NUM_THREAD, N_ITERATIONS, 1451606400, 1451606400 + 1000000);
        }
    }

    // Display benchmark statistics
    printStatistics(timeCost);
}