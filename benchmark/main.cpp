#include <algorithm>
#include "XStore/XStore_Adapter.h"
#include "../include/utilities.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
BS::thread_pool pool;

std::vector<double> queryA_XStore(std::string SERVER_ADDR, std::string SERVER_PORT, int nIterations) {
    XStore_Adapter xStoreAdapter = XStore_Adapter(SERVER_ADDR, SERVER_PORT);
    std::vector<double> toReturn = xStoreAdapter.queryA(nIterations);
    return toReturn;
}

std::vector<double> queryB_XStore(std::string SERVER_ADDR, std::string SERVER_PORT, int nIterations, long int startTime, long int endTime) {
    XStore_Adapter xStoreAdapter = XStore_Adapter(SERVER_ADDR, SERVER_PORT);
    std::vector<double> toReturn = xStoreAdapter.queryB(nIterations, startTime, endTime);
    return toReturn;
}

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
    int NUM_THREADS = atoi(argv[5]);

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

    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    if (EXPERIMENT_TYPE == "QUERY_A") {
        int N_ITERATIONS = 1000;
        std::cout << "----- [QUERY_A] -----" << std::endl;
        std::cout << "[BENCHMARK] Performing sequential READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
        std::cout << "[BENCHMARK] Thread: " << NUM_THREADS << std::endl;

        if (TARGET_DB == "XSTORE") {
            for (int i = 0; i < NUM_THREADS; i++) {
                multiFuture.push_back(pool.submit(queryA_XStore, SERVER_ADDR, SERVER_PORT, N_ITERATIONS));
            }
        }
    }
    else if (EXPERIMENT_TYPE == "QUERY_B") {
        int N_ITERATIONS = 1000;
        std::cout << "----- [QUERY_B] -----" << std::endl;
        std::cout << "[BENCHMARK] Performing random READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;
        std::cout << "[BENCHMARK] Thread: " << NUM_THREADS << std::endl;

        if (TARGET_DB == "XSTORE") {
            XStore_Adapter XStoreAdapter = XStore_Adapter(SERVER_ADDR, SERVER_PORT);
//            timeCost = XStoreAdapter.queryB(N_ITERATIONS, 123, 12345);
        }
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    // Display benchmark statistics
    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }

    std::cout << "\n----- [STATISTICS] -----" << std::endl;
    std::cout << "  - Clients: " << NUM_THREADS << std::endl;
    std::cout << "  Total: " << std::reduce(timeCost.begin(), timeCost.end()) << " (ms)" << std::endl;
    std::cout << "  Mean:  " << meanCalc(timeCost) << " (ms)" << std::endl;
    std::cout << "  Min:   " << getMin(timeCost) << " (ms)" << std::endl;
    std::cout << "  Max:   " << getMax(timeCost) << " (ms)" << std::endl;
    std::cout << "  Stdev: " << stdCalc(timeCost) << " (ms)" << std::endl;
}