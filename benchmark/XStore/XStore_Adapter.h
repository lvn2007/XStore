#ifndef XSTORE_XSTORE_ADAPTER_H
#define XSTORE_XSTORE_ADAPTER_H

#include "XStore.pb.h"
#include "XStore_Client.h"
#include "../include/Base_Adapter.h"

class XStore_Adapter: public Base_Adapter {
    public:
        // Query A -- Unary SEQ query 1k times
        static std::vector<double> queryA(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime);

        // Query B -- Unary RAND query 1k times
        static std::vector<double> queryB(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime);

    private:
        static std::vector<double> queryA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime);
        static std::vector<double> queryB_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime);
};

#endif //XSTORE_XSTORE_ADAPTER_H
