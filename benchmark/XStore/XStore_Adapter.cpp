#include "XStore_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
BS::thread_pool threadPool;

std::vector<double> XStore_Adapter::queryA(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::queryA_singleThread, SERVER_ADDR, SERVER_PORT, 1, N_ITERATION, startTime));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }

    return timeCost;
}

std::vector<double> XStore_Adapter::queryB(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::queryB_singleThread, SERVER_ADDR, SERVER_PORT, 1, N_ITERATION, startTime, endTime));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }

    return timeCost;
}

std::vector<double> XStore_Adapter::queryA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query response placeholder
    XStore_Proto::unaryQuery_REP unaryQueryRep;
    for (int i = 0; i < N_ITERATION; i++) {
        start = std::chrono::high_resolution_clock::now();

        // Dispatch query
        unaryQueryRep = xstoreClient.unaryQuery(startTime + i, "BENCH_DB");
        end = std::chrono::high_resolution_clock::now();
        timeCost = end - start;
        toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::queryB_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Random number generator
    std::random_device randomDevice;
    std::mt19937 seedGenerator(randomDevice());
    std::uniform_int_distribution<> rangeDistribution(startTime, endTime);

    // Query response placeholder
    XStore_Proto::unaryQuery_REP unaryQueryRep;
    long int targetTime;

    for (int i = 0; i < N_ITERATION; i++) {
        // Generate random within start/end range
        targetTime = rangeDistribution(seedGenerator);
        start = std::chrono::high_resolution_clock::now();

        // Dispatch query
        unaryQueryRep = xstoreClient.unaryQuery(targetTime, "BENCH_DB");
        end = std::chrono::high_resolution_clock::now();
        timeCost = end - start;
        toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
    }
    return toReturn;
}