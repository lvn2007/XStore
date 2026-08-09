#include "XStore_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> XStore_Adapter::UnaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::UnaryQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::RangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::RangeQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::BatchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::BatchQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    std::string fileName = "Client-";
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("QUERY-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::UnaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    XStore_Adapter::CreateDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::UnaryInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::RangeInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    XStore_Adapter::CreateDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::RangeInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::BatchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    XStore_Adapter::CreateDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(XStore_Adapter::BatchInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
    }

    // Wait till all threads finishes
    for (std::vector<double> i : multiFuture.get()) {
        nestedVector.push_back(i);
    }

    std::vector<double> timeCost;

    // Flattening nested vector
    for (int i = 0; i < nestedVector.size(); i++) {
        // Write experiment result to CSV
        WriteCSV(fmt::format("INSERT-{}_Client-{}.csv", pattern, i), "Time taken (ms)", nestedVector.at(i));
        std::move(nestedVector.at(i).begin(), nestedVector.at(i).end(), std::back_inserter(timeCost));
    }
    return timeCost;
}

std::vector<double> XStore_Adapter::UnaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query response placeholder
    XStore_Proto::UnaryQuery_REP unaryQueryRep;
    unsigned long int queryTargetTime;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.UnaryQuery(queryTargetTime, "BENCH_DB", &unaryQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(unaryQueryRep, &debugMsg);
            spdlog::debug("[UNARY QUERY] - Querying: {} | Query Result: {}", queryTargetTime, debugMsg);
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.UnaryQuery(queryTargetTime, "BENCH_DB", &unaryQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::RangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query response placeholder
    XStore_Proto::RangeQuery_REP rangeQueryRep;
    unsigned long int queryStartTime, queryEndTime;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.RangeQuery(queryStartTime, queryEndTime, "BENCH_DB", &rangeQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(rangeQueryRep, &debugMsg);
            spdlog::debug("[RANGE QUERY] - From: {} to {} | Query Result: {}", queryStartTime, queryEndTime, debugMsg);
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.RangeQuery(queryStartTime, queryEndTime, "BENCH_DB", &rangeQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::BatchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query response placeholder
    XStore_Proto::BatchQuery_REP batchQueryRep;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            std::vector<unsigned long int> timeList;

            // Extract target time into timeList - timeList can be seen as a batch of targets
            for (int j = 0; j < queryData->at(i).size(); j++) {
                timeList.emplace_back(std::any_cast<unsigned long int>(queryData->at(i).at(j)));
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.BatchQuery(timeList, "BENCH_DB", &batchQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(batchQueryRep, &debugMsg);
            spdlog::debug("[BATCH QUERY] - Query Result: {}", debugMsg);
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            std::vector<unsigned long int> timeList;

            // Extract target time into timeList - timeList can be seen as a batch of targets
            for (int j = 0; j < queryData->at(i).size(); j++) {
                timeList.emplace_back(std::any_cast<unsigned long int>(queryData->at(i).at(j)));
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.BatchQuery(timeList, "BENCH_DB", &batchQueryRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
        }
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::UnaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Placeholder
    XStore_Proto::UnaryInsert_REQ toServer;
    XStore_Proto::RowData RowData;
    XStore_Proto::UnaryInsert_REP unaryInsertRep;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::UNARYINSERT);
            toServer.set_timestamp(std::any_cast<long unsigned int>(insertData->at(i).at(0)));
            toServer.set_dbname("BENCH_DB");

            // Loop through each item in a row -> Identify type and build RowData
            //  Skipping first element (timestamp) in a row
            XStore_Proto::FData *field;
            for (int j = 1; j < insertData->at(i).size(); j++) {
                field = RowData.add_fielddata();
                xstoreClient.setFieldData(field, &insertData->at(i).at(j));
            }

            // Add row data into Proto UNARY INSERT msg
            *toServer.mutable_data() = RowData;

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.UnaryInsert(&toServer, &unaryInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(unaryInsertRep, &debugMsg);
            spdlog::debug("[UNARY INSERT] - Insert Time: {} | Insert Result: {}", std::any_cast<long unsigned int>(insertData->at(i).at(0)), debugMsg);

            // Clear msg
            toServer.Clear();
            RowData.Clear();
            unaryInsertRep.Clear();
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::UNARYINSERT);
            toServer.set_timestamp(std::any_cast<long unsigned int>(insertData->at(i).at(0)));
            toServer.set_dbname("BENCH_DB");

            // Loop through each item in a row -> Identify type and build RowData
            //  Skipping first element (timestamp) in a row
            XStore_Proto::FData *field;
            for (int j = 1; j < insertData->at(i).size(); j++) {
                field = RowData.add_fielddata();
                xstoreClient.setFieldData(field, &insertData->at(i).at(j));
            }

            // Add row data into Proto UNARY INSERT msg
            *toServer.mutable_data() = RowData;

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.UnaryInsert(&toServer, &unaryInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Clear msg
            toServer.Clear();
            RowData.Clear();
            unaryInsertRep.Clear();
        }
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::RangeInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Placeholder for insert results
    XStore_Proto::RangeInsert_REQ toServer;
    XStore_Proto::RangeInsert_DATA *rangeInsertData;
    XStore_Proto::RowData RowData;
    XStore_Proto::RangeInsert_REP rangeInsertRep;
    XStore_Proto::FData *field;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::RANGEINSERT);
            toServer.set_dbname("BENCH_DB");

            // Loop through each row
            for (int j = 0; j < insertData->size(); j++) {
                rangeInsertData = toServer.add_rangeinsert_data();

                // Build timestamp for current row of data
                rangeInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(j).at(0)));

                // Loop through each "column" in a row
                for (int k = 1; k < insertData->at(j).size(); k++) {
                    field = RowData.add_fielddata();
                    xstoreClient.setFieldData(field, &insertData->at(j).at(k));
                }

                // Merge message
                rangeInsertData->mutable_data()->MergeFrom(RowData);
                field->Clear();
                RowData.Clear();
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.RangeInsert(&toServer, &rangeInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(rangeInsertRep, &debugMsg);
            spdlog::debug("[RANGE INSERT] - Insert Result: {}", debugMsg);

            // Clear msg
            toServer.Clear();
            rangeInsertData->Clear();
            rangeInsertRep.Clear();
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::RANGEINSERT);
            toServer.set_dbname("BENCH_DB");

            // Loop through each row
            for (int j = 0; j < insertData->size(); j++) {
                rangeInsertData = toServer.add_rangeinsert_data();

                // Build timestamp for current row of data
                rangeInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(j).at(0)));

                // Loop through each "column" in a row
                for (int k = 1; k < insertData->at(j).size(); k++) {
                    field = RowData.add_fielddata();
                    xstoreClient.setFieldData(field, &insertData->at(j).at(k));
                }

                // Merge message
                rangeInsertData->mutable_data()->MergeFrom(RowData);
                field->Clear();
                RowData.Clear();
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.RangeInsert(&toServer, &rangeInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Clear msg
            toServer.Clear();
            rangeInsertData->Clear();
            rangeInsertRep.Clear();
        }
    }
    return toReturn;
}

std::vector<double> XStore_Adapter::BatchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;

    // Init client obj
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Placeholder for insert results
    XStore_Proto::BatchInsert_REQ toServer;
    XStore_Proto::BatchInsert_DATA *batchInsertData;
    XStore_Proto::RowData RowData;
    XStore_Proto::BatchInsert_REP batchInsertRep;
    XStore_Proto::FData *field;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::BATCHINSERT);
            toServer.set_dbname("BENCH_DB");

            // Loop through each row
            for (int j = 0; j < insertData->size(); j++) {
                batchInsertData = toServer.add_batchinsert_data();

                // Build timestamp for current row of data
                batchInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(j).at(0)));

                // Loop through each "column" in a row
                for (int k = 1; k < insertData->at(j).size(); k++) {
                    field = RowData.add_fielddata();
                    xstoreClient.setFieldData(field, &insertData->at(j).at(k));
                }

                // Merge message
                batchInsertData->mutable_data()->MergeFrom(RowData);
                field->Clear();
                RowData.Clear();
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.BatchInsert(&toServer, &batchInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Debug
            std::string debugMsg;
            google::protobuf::util::MessageToJsonString(batchInsertRep, &debugMsg);
            spdlog::debug("[BATCH INSERT] - Insert Result: {}", debugMsg);

            // Clear msg
            toServer.Clear();
            batchInsertData->Clear();
            batchInsertRep.Clear();
        }
    }
        // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Build insert msg
            toServer.set_rtype(XStore_Proto::BATCHINSERT);
            toServer.set_dbname("BENCH_DB");

            // Loop through each row
            for (int j = 0; j < insertData->size(); j++) {
                batchInsertData = toServer.add_batchinsert_data();

                // Build timestamp for current row of data
                batchInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(j).at(0)));

                // Loop through each "column" in a row
                for (int k = 1; k < insertData->at(j).size(); k++) {
                    field = RowData.add_fielddata();
                    xstoreClient.setFieldData(field, &insertData->at(j).at(k));
                }

                // Merge message
                batchInsertData->mutable_data()->MergeFrom(RowData);
                field->Clear();
                RowData.Clear();
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            xstoreClient.BatchInsert(&toServer, &batchInsertRep);
            end = std::chrono::high_resolution_clock::now();
            timeCost = end - start;
            toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

            // Clear msg
            toServer.Clear();
            batchInsertData->Clear();
            batchInsertRep.Clear();
        }
    }
    return toReturn;
}

void XStore_Adapter::CreateDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData) {
    XStore_Client xstoreClient;
    xstoreClient.connect(SERVER_ADDR, SERVER_PORT);

    // Analyze first row of data to determine row size
    short int rowLength = XStore_Adapter::ComputeRowLength(singleRowData);

    // Granularity
    XStore_Proto::TimeUnit granularity = XStore_Adapter::GetTimeResolution(std::any_cast<unsigned long int>(singleRowData->at(0)));

    // Create BENCH_DB
    XStore_Proto::CreateDB_REP createDbRep = xstoreClient.CreateDB(dbName, granularity, rowLength);
}

XStore_Proto::TimeUnit XStore_Adapter::GetTimeResolution(unsigned long int timestamp) {
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

short int XStore_Adapter::ComputeRowLength(std::vector<std::any> *singleRowData) {
    short int rowSize = 0;

    std::any currentMetric;
    for (int i = 1; i < singleRowData->size(); i++) {
        currentMetric = singleRowData->at(i);
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