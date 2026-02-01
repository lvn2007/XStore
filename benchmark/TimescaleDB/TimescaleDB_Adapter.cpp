#include "TimescaleDB_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> TimescaleDB_Adapter::unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    TimescaleDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(TimescaleDB_Adapter::unaryInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

void TimescaleDB_Adapter::createDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData) {
    // Init TimescaleDB instances
//    auto db = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db={}", SERVER_ADDR, SERVER_PORT, dbName));
    pqxx::connection cx{fmt::format("postgresql://postgres@{}:{}/{}", SERVER_ADDR, SERVER_PORT, dbName)};

    // Create DB
//    db->createDatabaseIfNotExists();
}