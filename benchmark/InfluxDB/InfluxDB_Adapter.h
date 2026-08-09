#ifndef XSTORE_INFLUXDB_ADAPTER_H
#define XSTORE_INFLUXDB_ADAPTER_H

#include "../include/Base_Adapter.h"
#include "../include/Utilities.h"
#include <InfluxDB/InfluxDBFactory.h>
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

class InfluxDB_Adapter: public Base_Adapter {
public:
    // UNARY QUERY
    //  Supports both SEQ & RAND access patterns
    static std::vector<double> UnaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // RANGE QUERY
    //  A batch of queries with SEQ access pattern
    static std::vector<double> RangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // BATCH QUERY
    //  A batch of queries with RAND access pattern
    static std::vector<double> BatchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug=false);

    // UNARY INSERT
    //  Supports both SEQ & RAND access patterns
    static std::vector<double> UnaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug=false);

    // BATCH INSERT
    //  A batch of inserts with SEQ/RAND access pattern
    static std::vector<double> BatchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug=false);

private:
    // QUERY
    static std::vector<double> UnaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);
    static std::vector<double> RangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);
    static std::vector<double> BatchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug);

    // INSERT
    static std::vector<double> UnaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);
    static std::vector<double> RangeInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);
    static std::vector<double> BatchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug);

    static void CreateDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData);
    static influxdb::Point MakeSinglePoint(std::vector<std::any> *insertData);
};

#endif //XSTORE_INFLUXDB_ADAPTER_H
