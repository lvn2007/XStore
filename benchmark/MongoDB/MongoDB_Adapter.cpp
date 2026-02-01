#include "MongoDB_Adapter.h"
#include "../include/BS_thread_pool.hpp"

// Threading library
static BS::thread_pool threadPool;

std::vector<double> MongoDB_Adapter::unaryQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(MongoDB_Adapter::unaryQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
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

std::vector<double> MongoDB_Adapter::rangeQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(MongoDB_Adapter::rangeQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
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

std::vector<double> MongoDB_Adapter::batchQuery(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *queryData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(MongoDB_Adapter::batchQuery_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &queryData->at(i), isDebug));
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

std::vector<double> MongoDB_Adapter::unaryInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    MongoDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(MongoDB_Adapter::unaryInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
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

std::vector<double> MongoDB_Adapter::batchInsert(std::string SERVER_ADDR, std::string SERVER_PORT, short unsigned int NUM_THREAD, int N_ITERATION, std::vector<std::vector<std::vector<std::any>>> *insertData, std::string pattern, bool isDebug) {
    BS::multi_future<std::vector<double>> multiFuture;
    std::vector<std::vector<double>> nestedVector;

    // Create BENCH_DB
    std::string dbName = "BENCH_DB";
    MongoDB_Adapter::createDB(SERVER_ADDR, SERVER_PORT, dbName, &insertData->at(0).at(0));

    // Extracting insertData so that each thread will have unique insert data
    for (int i = 0; i < NUM_THREAD; i++) {
        multiFuture.push_back(threadPool.submit(MongoDB_Adapter::batchInsert_singleThread, SERVER_ADDR, SERVER_PORT, N_ITERATION, &insertData->at(i), isDebug));
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

std::vector<double> MongoDB_Adapter::unaryQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init MongoDB instances
    mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
    mongocxx::client mongoClient(uri);

    // Access DB and collection
    const mongocxx::database &mongoDB = mongoClient["BENCH_DB"];
    mongocxx::collection mongoCollection = mongoDB["BENCH_DB"];

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // Query target time
    unsigned long int queryTargetTime;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp", bsoncxx::types::b_date(std::chrono::seconds(queryTargetTime))));

            if (timedOut) {
                spdlog::warn("[UNARY QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<bsoncxx::document::value> &unaryQueryRep = mongoCollection.find_one(currDocument.view());
                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                if (unaryQueryRep) {
                    // Debug
                    spdlog::debug("[UNARY QUERY] - Querying: {} | Query Result: {}", queryTargetTime, bsoncxx::to_json(unaryQueryRep->view()));
                }
            }
            catch (...) {
                spdlog::warn("[UNARY QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryTargetTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp", bsoncxx::types::b_date(std::chrono::seconds(queryTargetTime))));

            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<bsoncxx::document::value> &unaryQueryRep = mongoCollection.find_one(currDocument.view());
                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    return toReturn;
}

std::vector<double> MongoDB_Adapter::rangeQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init MongoDB instances
    mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
    mongocxx::client mongoClient(uri);

    // Access DB and collection
    const mongocxx::database mongoDB = mongoClient["BENCH_DB"];
    mongocxx::collection mongoCollection = mongoDB["BENCH_DB"];

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

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp",
                bsoncxx::builder::basic::make_document(
                    bsoncxx::builder::basic::kvp("$gte", bsoncxx::types::b_date(std::chrono::seconds(queryStartTime))),
                    bsoncxx::builder::basic::kvp("$lte", bsoncxx::types::b_date(std::chrono::seconds(queryEndTime))))));

            if (timedOut) {
                spdlog::warn("[RANGE QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                mongocxx::cursor cursor = mongoCollection.find(currDocument.view());

                // Iterate all elements in cursor without doing anything
                //  This is because of lazy execution of cursor
                std::vector<std::string> docVector;
                for (bsoncxx::document::view currDoc : cursor) {
                    docVector.push_back(bsoncxx::to_json(currDoc));
                }

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                // Debug
                spdlog::debug("[RANGE QUERY] - From: {} to {} | Query Result: {}", queryStartTime, queryEndTime, fmt::join(docVector, "\n"));
            }
            catch (...) {
                spdlog::warn("[RANGE QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            queryStartTime = std::any_cast<unsigned long int>(queryData->at(i).at(0));
            queryEndTime = std::any_cast<unsigned long int>(queryData->at(i).at(1));

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp",
                bsoncxx::builder::basic::make_document(
                bsoncxx::builder::basic::kvp("$gte", bsoncxx::types::b_date(std::chrono::seconds(queryStartTime))),
                bsoncxx::builder::basic::kvp("$lte", bsoncxx::types::b_date(std::chrono::seconds(queryEndTime))))));

            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                mongocxx::cursor cursor = mongoCollection.find(currDocument.view());

                // Iterate all elements in cursor without doing anything
                //  This is because of lazy execution of cursor
                for (bsoncxx::document::view currDoc : cursor) { }
                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    return toReturn;
}

std::vector<double> MongoDB_Adapter::batchQuery_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *queryData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init MongoDB instances
    mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
    mongocxx::client mongoClient(uri);

    // Access DB and collection
    const mongocxx::database mongoDB = mongoClient["BENCH_DB"];
    mongocxx::collection mongoCollection = mongoDB["BENCH_DB"];

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Extract target time into timeList - timeList can be seen as a batch of targets
            bsoncxx::builder::basic::array timeList;
            for (int j = 0; j < queryData->at(i).size(); j++) {
                timeList.append(bsoncxx::types::b_date(std::chrono::seconds(std::any_cast<unsigned long int>(queryData->at(i).at(j)))));
            }

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp",
                bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("$in", timeList))));

            if (timedOut) {
                spdlog::warn("[BATCH QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                mongocxx::cursor cursor = mongoCollection.find(currDocument.view());

                // Iterate all elements in cursor without doing anything
                //  This is because of lazy execution of cursor
                std::vector<std::string> docVector;
                for (bsoncxx::document::view currDoc : cursor) { }
                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                // Debug
                spdlog::debug("[BATCH QUERY] - Query Result: {}", fmt::join(docVector, "\n"));
            }
            catch (...) {
                spdlog::warn("[BATCH QUERY] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Extract target time into timeList - timeList can be seen as a batch of targets
            bsoncxx::builder::basic::array timeList;
            for (int j = 0; j < queryData->at(i).size(); j++) {
                timeList.append(bsoncxx::types::b_date(std::chrono::seconds(std::any_cast<unsigned long int>(queryData->at(i).at(j)))));
            }

            // Create document
            const bsoncxx::document::value &currDocument = bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("timestamp",
                bsoncxx::builder::basic::make_document(bsoncxx::builder::basic::kvp("$in", timeList))));

            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            // Dispatch query
            start = std::chrono::high_resolution_clock::now();
            try {
                mongocxx::cursor cursor = mongoCollection.find(currDocument.view());

                // Iterate all elements in cursor without doing anything
                //  This is because of lazy execution of cursor
                for (bsoncxx::document::view currDoc : cursor) { }
                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    return toReturn;
}

std::vector<double> MongoDB_Adapter::unaryInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init MongoDB instances
    mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
    mongocxx::client mongoClient(uri);
    mongocxx::options::insert insertOptions;
    insertOptions.ordered(false);

    // Access DB and collection
    const mongocxx::database mongoDB = mongoClient["BENCH_DB"];
    mongocxx::collection mongoCollection = mongoDB["BENCH_DB"];

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create document
            bsoncxx::builder::basic::document currDocument = makeSingleDocument(&insertData->at(i));

            if (timedOut) {
                spdlog::warn("[UNARY INSERT] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                continue;
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<mongocxx::result::insert_one> &insertResult = mongoCollection.insert_one(currDocument.view(), insertOptions);

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                // Debug
                spdlog::debug("[UNARY INSERT] - Insert Time: {} | Insert Result: {}", std::any_cast<unsigned long int>(insertData->at(i).at(0)), insertResult->result().inserted_count());
            }
            catch (...) {
                spdlog::warn("[UNARY INSERT] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    // NOT a debug mode
    else {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create document
            bsoncxx::builder::basic::document currDocument = makeSingleDocument(&insertData->at(i));

            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<mongocxx::result::insert_one> &insertResult = mongoCollection.insert_one(currDocument.view(), insertOptions);

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    return toReturn;
}

std::vector<double> MongoDB_Adapter::batchInsert_singleThread(std::string SERVER_ADDR, std::string SERVER_PORT, int N_ITERATION, std::vector<std::vector<std::any>> *insertData, bool isDebug) {
    std::vector<double> toReturn;
    bool timedOut = false;

    // Init MongoDB instances
    mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
    mongocxx::client mongoClient(uri);
    mongocxx::options::insert insertOptions;
    insertOptions.ordered(false);

    // Access DB and collection
    const mongocxx::database mongoDB = mongoClient["BENCH_DB"];
    mongocxx::collection mongoCollection = mongoDB["BENCH_DB"];

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> timeCost;

    // DEBUG mode
    if (isDebug) {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create document
            std::vector<bsoncxx::builder::basic::document> currDocuments;

            for (int j = 0; j < insertData->size(); j++) {
                currDocuments.push_back(makeSingleDocument(&insertData->at(j)));
            }

            if (timedOut) {
                spdlog::warn("[BATCH INSERT] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                continue;
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<mongocxx::result::insert_many> &insertResult = mongoCollection.insert_many(currDocuments, insertOptions);

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());

                // Debug
                spdlog::debug("[BATCH INSERT] - Insert Result: {}", insertResult->inserted_count());
            }
            catch (...) {
                spdlog::warn("[BATCH INSERT] - There was an exception or operation has timed-out");
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    // NOT a debug mode
    else
    {
        for (int i = 0; i < N_ITERATION; i++) {
            // Create document
            std::vector<bsoncxx::builder::basic::document> currDocuments;

            for (int j = 0; j < insertData->size(); j++) {
                currDocuments.push_back(makeSingleDocument(&insertData->at(j)));
            }

            if (timedOut) {
                toReturn.push_back(0);
                continue;
            }

            // Dispatch insert
            start = std::chrono::high_resolution_clock::now();
            try {
                const core::optional<mongocxx::result::insert_many> &insertResult = mongoCollection.insert_many(currDocuments, insertOptions);

                end = std::chrono::high_resolution_clock::now();
                timeCost = end - start;
                toReturn.push_back(std::chrono::duration<double, std::milli> (timeCost).count());
            }
            catch (...) {
                toReturn.push_back(0);
                timedOut = true;        // Once 1 op is timed-out, the rest will also timed-out
            }
        }
    }
    return toReturn;
}

void MongoDB_Adapter::createDB(std::string SERVER_ADDR, std::string SERVER_PORT, std::string dbName, std::vector<std::any> *singleRowData) {
    try {
        // Init MongoDB
        mongocxx::uri uri(fmt::format("mongodb://{}:{}/?socketTimeoutMS=120000", SERVER_ADDR, SERVER_PORT));
        mongocxx::client mongoClient(uri);

        // Access/Create DB and collection
        mongocxx::database mongoDB = mongoClient[dbName];

        // Granularity
        std::string granularity = MongoDB_Adapter::getTimeResolution(std::any_cast<unsigned long int>(singleRowData->at(0)));

        // Setup Time Series collection options.
        bsoncxx::builder::document timeSeriesCollectionOptions =
                {
                        "timeseries",
                        {
                                "timeField", "timestamp",
                                "granularity", granularity
                        }
                };

        // Create time-series collection
        const mongocxx::collection &mongoCollection = mongoDB.has_collection(dbName) ? mongoDB[dbName] : mongoDB.create_collection(dbName, timeSeriesCollectionOptions.view().get_document().value);
    }
    catch (...) {
        spdlog::warn("[CREATE DB] - Failed to create {}", dbName);
    }
}

bsoncxx::builder::basic::document MongoDB_Adapter::makeSingleDocument(std::vector<std::any> *insertData) {
    bsoncxx::builder::basic::document toReturn;

    // Timestamp -- Variable as long int since bsoncxx doesn't recognize unsigned long int
    long int timestamp = std::any_cast<long unsigned int>(insertData->at(0));
    toReturn.append(bsoncxx::builder::basic::kvp("timestamp", bsoncxx::types::b_date(std::chrono::seconds(timestamp))));

    std::any currentMetric;
    for (int i = 1; i < insertData->size(); i++) {
        currentMetric = insertData->at(i);
        if (currentMetric.type() == typeid(std::string)) {
            toReturn.append(bsoncxx::builder::basic::kvp(fmt::format("col{}", i - 1), std::any_cast<std::string>(insertData->at(i))));
            continue;
        }
        else if (currentMetric.type() == typeid(int)) {
            toReturn.append(bsoncxx::builder::basic::kvp(fmt::format("col{}", i - 1), std::any_cast<int>(insertData->at(i))));
            continue;
        }
        else if (currentMetric.type() == typeid(double)) {
            toReturn.append(bsoncxx::builder::basic::kvp(fmt::format("col{}", i - 1), std::any_cast<double>(insertData->at(i))));
            continue;
        }
    }
    return toReturn;
}

std::string MongoDB_Adapter::getTimeResolution(unsigned long int timestamp) {
    int count = 0;

    while (timestamp != 0) {
        timestamp /= 10;
        ++count;
    }

    if (count < 13) {
        return "seconds";
    }
    else if (count >= 13 && count < 16) {
        return "milliseconds";
    }
    else if (count >= 16) {
        return "microseconds";
    }
}