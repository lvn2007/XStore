#include "XStore_Adapter.h"

XStore_Adapter::XStore_Adapter(std::string SERVER_ADDR, std::string SERVER_PORT) {
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);
}

std::vector<double> XStore_Adapter::queryA(int nIterations) {
    std::vector<double> toReturn;

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query response placeholder
    XStore_Proto::unaryQuery_REP unaryQueryRep;
    for (int i = 0; i < nIterations; i++) {
        start = std::chrono::high_resolution_clock::now();

        // Dispatch query
        unaryQueryRep = xstoreClient.unaryQuery(1451606400 + i, "TSLA");
        end = std::chrono::high_resolution_clock::now();
        timeCost = end - start;
        toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::queryB(int nIterations, long int startTime, long int endTime) {
    std::vector<double> toReturn;

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
    for (int i = 0; i < nIterations; i++) {
        // Generate random within start/end range
        targetTime = rangeDistribution(seedGenerator);
        std::cout << targetTime << std::endl;
        start = std::chrono::high_resolution_clock::now();

        // Dispatch query
        unaryQueryRep = xstoreClient.unaryQuery(targetTime, "TSLA");
        end = std::chrono::high_resolution_clock::now();
        timeCost = end - start;
        toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
    }
    return toReturn;
}