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
        std::cout << "[ERROR - Arguments] -- Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR - Arguments] -- Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

    // ----DEV AREA
    // Instantiate XStore obj
    XStore myStore;
//    XStore_Proto::sysInfo_REP sys_info_rep = myStore.infoQuery("127.0.0.1:12345", THREAD_COUNT);
//    sys_info_rep.PrintDebugString();

    // Create DB
//    XStore_Proto::createDB_REP create_db_rep = myStore.createDB("TSLA", "second");
//    create_db_rep.PrintDebugString();

//    system("free && sync && sudo sh -c \"echo 3 > /proc/sys/vm/drop_caches\" && free");
//    std::vector<double> us = { };
//    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
//    std::chrono::duration<double> diff;

//    XStore_Proto::batchInsert_REQ batch_insert_req;

//    for (int i = 1; i < 61; i++) {
//        XStore_Proto::batchInsert_DATA *batch_insert_data = batch_insert_req.add_batchinsert_data();
//        batch_insert_data->set_timestamp(1482271200 + i);
//        batch_insert_data->set_dbname("TSLA");
//        batch_insert_data->set_data("Test" + std::to_string(i));
//    }
//    myStore.batchInsert(&batch_insert_req);

//    XStore_Proto::batchQuery_REP batch_query_rep = myStore.batchQuery(1482271200, 1482271200 + 62, "TSLA");
//    batch_query_rep.PrintDebugString();

    // Start timer
//    start = std::chrono::high_resolution_clock::now();

    // Insert some data -- V2
//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert(1482271200, "Test", "TSLA");
//    unary_insert_rep.PrintDebugString();

//    std::cout << "----" << std::endl;

//    XStore_Proto::unaryQuery_REP unary_query_rep = myStore.unaryQuery(1482271200, "tsla");
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