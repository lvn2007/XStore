#include "InfluxDB_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> InfluxDB_Adapter::unaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(InfluxDB_Adapter::unaryQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> InfluxDB_Adapter::rangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(InfluxDB_Adapter::rangeQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> InfluxDB_Adapter::unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    InfluxDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(InfluxDB_Adapter::unaryInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
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

std::vector<double> InfluxDB_Adapter::batchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    InfluxDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(InfluxDB_Adapter::batchInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
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

std::vector<double> InfluxDB_Adapter::unaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;

    // Init InfluxDB instances
    auto influxClient = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db=BENCH_DB", SERVER_ADDR, SERVER_PORT));

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create Point
            influxdb::Point currPoint = makeSinglePoint(&insertData->at(i));

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            influxClient->write(std::move(currPoint));

            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            spdlog::debug("[UNARY INSERT] - Insert Time: {} | Insert Result: OK", std::any_cast<unsigned long int>(insertData->at(i).at(0)));
        }
    }
        // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create Point
            influxdb::Point currPoint = makeSinglePoint(&insertData->at(i));

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            influxClient->write(std::move(currPoint));

            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> InfluxDB_Adapter::unaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;

    // Init InfluxDB instances
    auto influxClient = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db=BENCH_DB", SERVER_ADDR, SERVER_PORT));

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query target time
    unsigned long int queryTargetTime;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            std::vector<influxdb::Point> unaryQueryRep = influxClient->query(fmt::format("SELECT * FROM BENCH_DB WHERE time = {}s", queryTargetTime));
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            if (unaryQueryRep.size() != 0) {
                spdlog::debug("[UNARY QUERY] - Querying: {} | Query Result: {}", queryTargetTime, unaryQueryRep.at(0).getTags());
            }
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            std::vector<influxdb::Point> unaryQueryRep = influxClient->query(fmt::format("SELECT * FROM BENCH_DB WHERE time = {}s", queryTargetTime));
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> InfluxDB_Adapter::rangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;

    // Init InfluxDB instances
    auto influxClient = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db=BENCH_DB", SERVER_ADDR, SERVER_PORT));

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query target time
    unsigned long int queryStartTime, queryEndTime;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            std::vector<influxdb::Point> rangeQueryRep = influxClient->query(fmt::format("SELECT * FROM BENCH_DB WHERE time >= {}s AND time <= {}s", queryStartTime, queryEndTime));

            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::vector<std::string> pointVector;
            
            // Append returned points to str
            for (int j = 0; j < rangeQueryRep.size(); j++) {
                pointVector.push_back(rangeQueryRep.at(j).getTags());
            }

            spdlog::debug("[RANGE QUERY] - From: {} to {} | Query Result: {}", queryStartTime, queryEndTime, fmt::join(pointVector, "\n"));
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            std::vector<influxdb::Point> rangeQueryRep = influxClient->query(fmt::format("SELECT * FROM BENCH_DB WHERE time >= {}s AND time <= {}s", queryStartTime, queryEndTime));

            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> InfluxDB_Adapter::batchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;

    // Init InfluxDB instances
    auto influxClient = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db=BENCH_DB", SERVER_ADDR, SERVER_PORT));
    influxClient->batchOf(insertData->size());

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create Points
            std::vector<influxdb::Point> currPoints;

            for (int j = 0; j < insertData->size(); j++) {
                currPoints.push_back(makeSinglePoint(&insertData->at(j)));
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                influxClient->write(std::move(currPoints));
                influxClient->flushBatch();

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                // Debug
                spdlog::debug("[BATCH INSERT] - Insert Result: OK");
            }
            catch (...) {
                toReturn.push_back(0);
                spdlog::warn("[BATCH INSERT] - There was an exception or operation has timed-out");
            }
        }
    }
    // NOT a debug mode
    else
    {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create Points
            std::vector<influxdb::Point> currPoints;

            for (int j = 0; j < insertData->size(); j++) {
                currPoints.push_back(makeSinglePoint(&insertData->at(j)));
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                influxClient->write(std::move(currPoints));
                influxClient->flushBatch();

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
            }
        }
    }
    return toReturn;
}

void InfluxDB_Adapter::createDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData) {
    // Init InfluxDB instances
    auto db = influxdb::InfluxDBFactory::Get(fmt::format("http://{}:{}?db={}", SERVER_ADDR, SERVER_PORT, dbName));

    // Create DB
    db->createDatabaseIfNotExists();
}

influxdb::Point InfluxDB_Adapter::makeSinglePoint(std::vector<std::any> *insertData) {
    influxdb::Point toReturn{"BENCH_DB"};

    // Timestamp -- Variable as long int since bsoncxx doesn't recognize unsigned long int
    std::chrono::time_point<std::chrono::system_clock> timestamp(std::chrono::milliseconds(std::any_cast<long unsigned int>(insertData->at(0))));
    toReturn.setTimestamp(timestamp);

    std::any currentMetric;
    for (int i = 1; i < insertData->size(); i++) {
        currentMetric = insertData->at(i);
        if (currentMetric.type() == typeid(std::string)) {
            toReturn.addField(fmt::format("col{}", i - 1), std::any_cast<std::string>(insertData->at(i)));
            continue;
        }
        else if (currentMetric.type() == typeid(int)) {
            toReturn.addField(fmt::format("col{}", i - 1), std::any_cast<int>(insertData->at(i)));
            continue;
        }
        else if (currentMetric.type() == typeid(double)) {
            toReturn.addField(fmt::format("col{}", i - 1), std::any_cast<double>(insertData->at(i)));
            continue;
        }
    }
    return toReturn;
}