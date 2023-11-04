#include "XStore_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> XStore_Adapter::queryA(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, long int startTime) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        // Offsetting startTime so that each thread will have unique query target timestamp
        startTime += (i * N_ITERATION);
        multiFuture.push_back(threadPool.submit(XStore_Adapter::queryA_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, startTime));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    std::string fileName = "Query-A_Client-";
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fileName + std::to_string(i) + ".csv", "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::queryB(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, long int startTime, long int endTime) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::queryB_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, startTime, endTime));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    std::string fileName = "Query-B_Client-";
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fileName + std::to_string(i) + ".csv", "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::insertA(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::any>> &insertData) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Pre-emptively delete & create BENCH_DB
    std::string dbName = "BENCH_DB";
    XStore_Adapter::createAndRemoveDB(SERVER_ADDR, SERVER_PORT, dbName, insertData.at(0));

    // Extracting insertData so that each thread will have unique insert data
    std::vector<std::vector<std::vector<std::any>>> threadInsertData;
    for (int i = 0; i < NUM_THREAD; i++) {
        std::vector<std::vector<std::any>> subVector = std::vector<std::vector<std::any>>(insertData.begin() + (i * N_ITERATION), insertData.begin() + (i * N_ITERATION) + N_ITERATION);
        threadInsertData.emplace_back(subVector);
    }

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::insertA_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, threadInsertData.at(i)));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    std::string fileName = "Insert-A_Client-";
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        writeCSV(fileName + std::to_string(i) + ".csv", "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::queryA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, long int startTime) {
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

std::vector<double> XStore_Adapter::queryB_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, long int startTime, long int endTime) {
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

std::vector<double> XStore_Adapter::insertA_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> insertData) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    for (int i = 0; i < N_ITERATION; i++) {
        start = std::chrono::high_resolution_clock::now();

        // Build rowData
        XStore_Proto::rowData rowData;
        std::any currentMetric;

        for (int j = 1; j < insertData.at(i).size(); j++) {
            XStore_Proto::fData *field = rowData.add_fielddata();
            currentMetric = insertData.at(i).at(j);

            if (currentMetric.type() == typeid(std::string)) {
                field->set_stringdata(std::any_cast<std::string>(currentMetric));
                continue;
            }
            else if (currentMetric.type() == typeid(int)) {
                field->set_int32data(std::any_cast<int>(currentMetric));
                continue;
            }
            else if (currentMetric.type() == typeid(double)) {
                field->set_doubledata(std::any_cast<double>(currentMetric));
                continue;
            }
        }

        // Dispatch insert
        XStore_Proto::unaryInsert_REP unaryInsertRep = xstoreClient.unaryInsert(std::any_cast<long unsigned int>(insertData.at(i).at(0)), rowData, "BENCH_DB");
        end = std::chrono::high_resolution_clock::now();
        timeCost = end - start;
        toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
    }
    return toReturn;
}

void XStore_Adapter::createAndRemoveDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string &dbName, std::vector<std::any> &singleRowData) {
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);
    xstoreClient.removeDB(dbName);

    // Analyze first row of data to determine row size
    short int rowLength = XStore_Adapter::computeRowLength(singleRowData);

    // Granularity
    XStore_Proto::timeUnit granularity = XStore_Adapter::getTimeResolution(std::any_cast<long unsigned int>(singleRowData.at(0)));

    // Create BENCH_DB
    xstoreClient.createDB(dbName, granularity, rowLength);
}

XStore_Proto::timeUnit XStore_Adapter::getTimeResolution(long int timestamp) {
    int count = 0;

    while (timestamp != 0) {
        timestamp /= 10;
        ++count;
    }

    if (count < 13) {
        return XStore_Proto::SECOND;
    }
    else if (count >= 13 && count < 16) {
        return XStore_Proto::MILLISECOND;
    }
    else if (count >= 16) {
        return XStore_Proto::MICROSECOND;
    }
}

short int XStore_Adapter::computeRowLength(std::vector<std::any> &singleRowData) {
    short int rowSize = 0;

    std::any currentMetric;
    for (int i = 1; i < singleRowData.size(); i++) {
        currentMetric = singleRowData.at(i);
        if (currentMetric.type() == typeid(std::string)) {
            rowSize += (std::any_cast<std::string>(currentMetric).size());
            continue;
        }
        else if (currentMetric.type() == typeid(int)) {
            rowSize += sizeof(int);
            continue;
        }
        else if (currentMetric.type() == typeid(double)) {
            rowSize += sizeof(double);
            continue;
        }
    }

    short int multipleOfTwo = 0;
    short int multIncrement = 1;
    while (rowSize > multipleOfTwo) {
        multipleOfTwo = pow(2, multIncrement);
        multIncrement++;
    }
    return multipleOfTwo + 32;
}