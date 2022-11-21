#include <iostream>
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"
#include <cmath>

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
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] -- Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] -- Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

    // ----DEV AREA
    // Instantiate XStore obj
    XStore myStore;

    // Create DB
//    XStore_Proto::createDB_REP create_db_rep = myStore.createDB_fTruncate("TSLA", "second", 128);
//    create_db_rep.PrintDebugString();
//
//    XStore_Proto::batchInsert_REQ batch_insert_req;
//    batch_insert_req.set_dbname("TSLA");
//
//    XStore_Proto::batchInsert_DATA *batch_insert_data2 = batch_insert_req.add_batchinsert_data();
//    batch_insert_data2->set_timestamp(1070071200);
//    batch_insert_data2->set_data("Test unordered");
//
//    XStore_Proto::batchInsert_DATA *batch_insert_data1 = batch_insert_req.add_batchinsert_data();
//    batch_insert_data1->set_timestamp(1482271200);
//    batch_insert_data1->set_data("Test unordered");
//
//    for (int i = 1; i < 3; i++) {
//        XStore_Proto::batchInsert_DATA *batch_insert_data = batch_insert_req.add_batchinsert_data();
//        batch_insert_data->set_timestamp(1482271200 + i);
//        batch_insert_data->set_data("Test" + std::to_string(i));
//    }
//
//    XStore_Proto::batchInsert_DATA *batch_insert_data3 = batch_insert_req.add_batchinsert_data();
//    batch_insert_data3->set_timestamp(1070071201);
//    batch_insert_data3->set_data("Test unordered");
//
//    XStore_Proto::batchInsert_REP batch_insert_rep = myStore.batchInsert_fTruncate(&batch_insert_req);
//    batch_insert_req.PrintDebugString();

//    XStore_Proto::batchQuery_REP batch_query_rep = myStore.batchQuery(1482271200, 1482271200 + 62, "TSLA");
//    batch_query_rep.PrintDebugString();

    // Start timer
//    start = std::chrono::high_resolution_clock::now();

    // Unary Insert
//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert_fTruncate(1451606400, "Helo", "TSLA");
//    unary_insert_rep.PrintDebugString();

    // SYS INFO
//    XStore_Proto::sysInfo_REP sys_info_rep = myStore.infoQuery("127.0.0.1:12345", THREAD_COUNT);
//    sys_info_rep.PrintDebugString();

    // REMOVE DB
//    XStore_Proto::removeDB_REP remove_db_rep = myStore.removeDB("TSLA");
//    remove_db_rep.PrintDebugString();

    // RMOVE DB FILES
//    XStore_Proto::removeDBFiles_REQ remove_db_files_req;
//    remove_db_files_req.set_dbname("TSLA");
//    remove_db_files_req.add_filename("2016.XSTORE");
//    remove_db_files_req.add_filename("TSLA.CFG");

//    XStore_Proto::removeDBFiles_REP remove_db_files_rep = myStore.removeDBFiles(&remove_db_files_req, remove_db_files_req.dbname());
//    remove_db_files_rep.PrintDebugString();

//    std::cout << "----" << std::endl;

    // UNARY QUERY
//    XStore_Proto::unaryQuery_REP unary_query_rep = myStore.unaryQuery_fTruncate(1070071200, "tsla");
//    unary_query_rep.PrintDebugString();
//    unary_query_rep = myStore.unaryQuery_fTruncate(1482271200, "tsla");
//    unary_query_rep.PrintDebugString();
//    unary_query_rep = myStore.unaryQuery_fTruncate(1482271201, "tsla");
//    unary_query_rep.PrintDebugString();
//    unary_query_rep = myStore.unaryQuery_fTruncate(1482271202, "tsla");
//    unary_query_rep.PrintDebugString();
//    unary_query_rep = myStore.unaryQuery_fTruncate(1482271203, "tsla");
//    unary_query_rep.PrintDebugString();
//    unary_query_rep = myStore.unaryQuery_fTruncate(1070071201, "tsla");
//    unary_query_rep.PrintDebugString();

    // End timer
//    end = std::chrono::high_resolution_clock::now();
//    diff = end - start;

//    std::cout << "Took: " << std::chrono::duration<double, std::milli> (diff).count() << " (usec)" << std::endl;
//    batch_insert_rep.PrintDebugString();

    // ----DEV AREA
    XStore_zmqServer xStoreZmqServer(THREAD_COUNT);
    xStoreZmqServer.startServer(BIND_ADDR, BIND_PORT);
}
