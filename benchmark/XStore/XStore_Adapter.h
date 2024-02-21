#ifndef XSTORE_XSTORE_ADAPTER_H
#define XSTORE_XSTORE_ADAPTER_H

#include "XStore.pb.h"
#include "../include/XStore_Client.h"
#include "../include/Base_Adapter.h"
#include "../include/utilities.h"

class XStore_Adapter: public Base_Adapter {
    public:
        // UNARY QUERY
        //  Supports both SEQ & RAND access patterns
        static std::vector<double> unaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern);

        // RANGE QUERY
        //  A batch of queries with SEQ access pattern
        static std::vector<double> rangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern);

        // BATCH QUERY
        //  A batch of queries with RAND access pattern
        static std::vector<double> batchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern);

        // UNARY INSERT
        //  Supports both SEQ & RAND access patterns
        static std::vector<double> unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern);

        // RANGE INSERT
        //  A batch of inserts with SEQ access pattern
        static std::vector<double> rangeInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern);

        // RANGE INSERT
        //  A batch of inserts with SEQ access pattern
        static std::vector<double> batchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern);

        // LOAD ALL
        static std::vector<double> loadAll(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, std::vector<std::vector<std::any>> &insertData);

    private:
        // QUERY
        static std::vector<double> unaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData);
        static std::vector<double> rangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData);
        static std::vector<double> batchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData);

        // INSERT
        static std::vector<double> unaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData);
        static std::vector<double> rangeInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData);
        static std::vector<double> batchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData);

        static void createAndRemoveDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData);
        static XStore_Proto::timeUnit getTimeResolution(long int timestamp);
        static short int computeRowLength(std::vector<std::any> *singleRowData);

        // LOAD ALL
        static std::vector<double> loadAll_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, std::vector<std::vector<std::any>> insertData);
};

#endif //XSTORE_XSTORE_ADAPTER_H
