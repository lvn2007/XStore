#ifndef XSTORE_XSTORE_ADAPTER_H
#define XSTORE_XSTORE_ADAPTER_H

#include "XStore.pb.h"
#include "../include/XStore_Client.h"
#include "../include/Base_Adapter.h"
#include "../include/utilities.h"

class XStore_Adapter: public Base_Adapter {
    public:
        // Query A -- Unary SEQ query 1k times
        static std::vector<double> queryA(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, long int startTime);

        // Query B -- Unary RAND query 1k times
        static std::vector<double> queryB(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime);

        // Insert A -- Unary SEQ insert 1k times
        static std::vector<double> insertA(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::any>> &insertData);

    private:
        // QUERY
        static std::vector<double> queryA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, long int startTime);
        static std::vector<double> queryB_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, long int startTime, long int endTime);

        // INSERT
        static std::vector<double> insertA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> insertData);
        static void createAndRemoveDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string &dbName, std::vector<std::any> &singleRowData);
        static XStore_Proto::timeUnit getTimeResolution(long int timestamp);
        static short int computeRowLength(std::vector<std::any> &singleRowData);
};

#endif //XSTORE_XSTORE_ADAPTER_H
