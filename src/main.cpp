#include <iostream>
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"
#include <cmath>
#include <variant>

double meanCalc(std::vector<double> inputVector) {
    double totalSum = 0;

    for (int i = 0; i < inputVector.size(); i++) {
        totalSum += inputVector.at(i);
    }

    return totalSum / inputVector.size();
}

double stdCalc(std::vector<double> inputVector) {
    double totalSum = 0;
    double mean = 0;
    double std = 0;

    for (int i = 0; i < inputVector.size(); i++) {
        totalSum += inputVector.at(i);
    }

    mean = totalSum / inputVector.size();

    for (int i = 0; i < inputVector.size(); i++) {
        std += pow(inputVector.at(i) - mean, 2);
    }

    return sqrt(std / inputVector.size());
}

double getMin(std::vector<double> inputVector) {
    double minVal = inputVector.at(0);

    for (int i = 1; i < inputVector.size(); i++) {
        if (inputVector.at(i) < minVal) {
            minVal = inputVector.at(0);
        }
    }

    return minVal;
}

double getMax(std::vector<double> inputVector) {
    double maxVal = inputVector.at(0);

    for (int i = 1; i < inputVector.size(); i++) {
        if (inputVector.at(i) > maxVal) {
            maxVal = inputVector.at(i);
        }
    }
    return maxVal;
}

int main(int argc, char *argv[]) {
//    FILE *bFile;
//    bFile = fopen("usingFWRITE.ttt", "wb+");
//    oneRow.SerializeToFileDescriptor(fileno(bFile));
//    std::fwrite(oneRow.SerializeAsString().c_str(), 128, 1, bFile);

//    std::cout << "TEST READING ---- " << std::endl;
//    XStore_Proto::data someRow;
//    bFile = fopen("usingFWRITE.ttt", "rb");
//    someRow.ParseFromFileDescriptor(fileno(bFile));
//    someRow.PrintDebugString();

    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] - Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] - Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

    // ----DEV AREA
    // Instantiate XStore obj
//    XStore myStore;

    // Create DB
//    XStore_Proto::createDB_REP create_db_rep = myStore.createDB("TSLA", XStore_Proto::timeUnit::SECOND, 128);
//    create_db_rep.PrintDebugString();

    // UNARY INSERT
//    XStore_Proto::rowData oneRow;
//    XStore_Proto::fieldData *field1 = oneRow.add_fdata();
//    field1->set_int32data(191239);
//    XStore_Proto::fieldData *field2 = oneRow.add_fdata();
//    field2->set_stringdata("191239");
//    XStore_Proto::fieldData *field22 = oneRow.add_fdata();
//    field22->set_doubledata(33.3333333333333);
//
//    XStore_Proto::rowData twoRow;
//    XStore_Proto::fieldData *field3 = twoRow.add_fdata();
//    field3->set_int32data(39123923);
//    XStore_Proto::fieldData *field4 = twoRow.add_fdata();
//    field4->set_stringdata("39123923");
//    XStore_Proto::unaryInsert_REP unary_insert_rep_1 = myStore.unaryInsert(1451606401, oneRow, "TSLA");
//    unary_insert_rep_1.PrintDebugString();

    // UNARY QUERY
//    XStore_Proto::unaryQuery_REP unary_query_rep = myStore.unaryQuery(1451606401, "TSLA");
//    unary_query_rep.PrintDebugString();

//    XStore_Proto::batchInsert_REQ batch_insert_req;
//    batch_insert_req.set_dbname("TSLA");
//
//    for (int i = 1; i < 5; i++) {
//        XStore_Proto::batchInsert_DATA *batch_insert_data = batch_insert_req.add_batchinsert_data();
//        batch_insert_data->set_timestamp(1483228798 + i);
//
//        // Assemble rowData
//        XStore_Proto::rowData oneRow;
//        XStore_Proto::fieldData *field_data = oneRow.add_fdata();
//        field_data->set_stringdata("Test " + std::to_string(1483228798 + i));
//        *batch_insert_data->mutable_data() = oneRow;
//    }
//    batch_insert_req.PrintDebugString();
//
//    XStore_Proto::batchInsert_REP batch_insert_rep = myStore.batchInsert(&batch_insert_req);
//    batch_insert_rep.PrintDebugString();

//    XStore_Proto::batchInsert_REP batch_insert_rep = myStore.batchInsert_fTruncate(&batch_insert_req);
//    batch_insert_req.PrintDebugString();

//    XStore_Proto::batchQuery_REP batch_query_rep = myStore.batchQuery(1482271200, 1482271200 + 62, "TSLA");
//    batch_query_rep.PrintDebugString();

    // Start timer
//    start = std::chrono::high_resolution_clock::now();

    // Unary Insert
//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert(1451606401, "Helo1451606401", "TSLA");
//    unary_insert_rep.PrintDebugString();
//    XStore_Proto::unaryInsert_REP unary_insert_rep2 = myStore.unaryInsert(1451606410, "Helo1451606410", "TSLA");

//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert(1451606400000, "Helo", "TSLA");
//    XStore_Proto::unaryInsert_REP unary_insert_rep1 = myStore.unaryInsert(1451636401233, "Helo123", "TSLA");
//    XStore_Proto::unaryInsert_REP unary_insert_rep2 = myStore.unaryInsert(1451606441897, "Helo456", "TSLA");
//    unary_insert_rep.PrintDebugString();
//
//    XStore_Proto::batchQuery_REQ batch_query_req;
//    batch_query_req.set_dbname("TSLA");
//    batch_query_req.add_timestamp(1451636401233);
//    batch_query_req.add_timestamp(1451606441897);
//    batch_query_req.add_timestamp(1451606400000);
//
//    XStore_Proto::batchQuery_REP batch_query_rep = myStore.batchQuery(&batch_query_req);
//    batch_query_rep.PrintDebugString();

    // RANGE QUERY
//    XStore_Proto::unaryQuery_REP range_query_rep = myStore.unaryQuery(1451606401, "TSLA");
//    range_query_rep.PrintDebugString();
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1451606400000, 1451606400002, "tsla");
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1482271200, 1482271204, "tsla");
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1483228798, 1483928808, "tsla");
//    range_query_rep.PrintDebugString();

    // End timer
//    end = std::chrono::high_resolution_clock::now();
//    diff = end - start;

//    std::cout << "Took: " << std::chrono::duration<double, std::milli> (diff).count() << " (usec)" << std::endl;
//    batch_insert_rep.PrintDebugString();

    // ----DEV AREA
    XStore_zmqServer xStoreZmqServer(THREAD_COUNT);
    xStoreZmqServer.startServer(BIND_ADDR, BIND_PORT);
}
