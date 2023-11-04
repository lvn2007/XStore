#ifndef XSTORE_MONGODB_ADAPTER_H
#define XSTORE_MONGODB_ADAPTER_H

#include <vector>
#include <string>
#include "../include/Base_Adapter.h"

class MongoDB_Adapter: public Base_Adapter {
public:
    // Query A -- Unary SEQ query 1k times
    static std::vector<double> queryA(const std::string& SERVER_ADDR, const std::string& SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime);

    // Query B -- Unary RAND query 1k times
    static std::vector<double> queryB(const std::string& SERVER_ADDR, const std::string& SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime);

private:
    static std::vector<double> queryA_singleThread(const std::string& SERVER_ADDR, const std::string& SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime);
    static std::vector<double> queryB_singleThread(const std::string& SERVER_ADDR, const std::string& SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime);
};

#endif //XSTORE_MONGODB_ADAPTER_H
