#include <iostream>
#include <filesystem>
#include <algorithm>
#include <map>
#include <cmath>
//#include "include/XStore_Networking/XStore_gRPC.h"
#include "include/XStore_Networking/XStore_tcpServer.h"
#include "include/XStore/XStore.h"
#include "include/dataStruct/clientCmdStruct.h"
#include "xStore.pb.h"

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
    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> diff;

    // Instantiate XStore obj
    XStore myStore;

//    myStore.insert(1483228801, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606400, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606401, "NaN,1,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606402, "NaN,NaN,2,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451676402, "NaN,NaN,7,NaN,NaN,NaN,NaN", "tsla");

    std::vector<double> ms = { };
    std::vector<double> us = { };
    std::vector<double> ns = { };

    XStore_tcpServer tcpServer;
    tcpServer.startServer();

    while (1) {
        // Start accepting client
        tcpServer.acceptClient();

        for (int i = 0; i < 100; i++) {
            // Receiving command from CLIENT
            xStoregRPC::unaryRequestMsg clientCmd = tcpServer.recvCmdFromClientProto();

//            if (clientCmd.requestType == "UNARY") {
                tcpServer.sendDataToClient(clientCmd.timestamp(), clientCmd.dbname());
//            }
        }

        break;
//        xStoregRPC::unaryRequestMsg toClient;
//
//        toClient.set_timestamp(123456678);
//        toClient.set_dbname("TSLA");
//
//        std::cout << toClient.timestamp() << std::endl;
//        std::cout << toClient.dbname() << std::endl;
//
//        std::cout << toClient.ByteSizeLong() << std::endl;
//        char* array = new char[toClient.ByteSizeLong()];
//        toClient.SerializeToArray(array, toClient.ByteSizeLong());
//
//        std::cout << "START" << std::endl;
//        for (int i = 0; i < toClient.ByteSizeLong(); i++) {
//            std::cout << (int) array[i] << std::endl;
//        }
//        std::cout << "END" << std::endl;
//        std::cout << array[0] << std::endl;
    }

//    xStoreServiceImplementation xStoreServiceImplementation;
//    xStoreServiceImplementation.Run();

//    return 0;

//    std::cout << myStore.getSingleOpenClose(1451606400, "tsla") << std::endl;

    // Microbench
//    srand(time(NULL));
//    long int randTime;
//    int nIter = 1000;
//    for (int i = 0; i < nIter; i++) {
//        system("free > /dev/null");
//        system("sync > /dev/null");
//        system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
//        system("free > /dev/null");
//        randTime = rand() % 1000 + 1;
//        long unixTime = 1451606400 + randTime;
//
//        start = std::chrono::high_resolution_clock::now();
//        myStore.getSingleOpenClose(unixTime, "tsla");
////        myStore.getBatch(1451606400, 1451607400, "tsla");
//        end = std::chrono::high_resolution_clock::now();
//        diff = end - start;
//
//        ms.push_back(std::chrono::duration<double, std::milli> (diff).count());
//        us.push_back(std::chrono::duration<double, std::micro> (diff).count());
//        ns.push_back(std::chrono::duration<double, std::nano> (diff).count());
//    }
//
//    std::cout << nIter << " queries experiments" << std::endl;
//    std::cout << "--- EXECUTION TIME in [ms] ---" << std::endl;
//    std::cout << "  - Min: " << *std::min_element(ms.begin(), ms.end()) << std::endl;
//    std::cout << "  - Max: " << *std::max_element(ms.begin(), ms.end()) << std::endl;
//    std::cout << "  - Mean: " << meanCalc(ms) << std::endl;
//    std::cout << "  - Stdev: " << stdCalc(ms) << std::endl;
//
//    std::cout << "--- EXECUTION TIME in [µs] ---" << std::endl;
//    std::cout << "  - Min: " << *std::min_element(us.begin(), us.end()) << std::endl;
//    std::cout << "  - Max: " << *std::max_element(us.begin(), us.end()) << std::endl;
//    std::cout << "  - Mean: " << meanCalc(us) << std::endl;
//    std::cout << "  - Stdev: " << stdCalc(us) << std::endl;
//
//    std::cout << "--- EXECUTION TIME in [ns] ---" << std::endl;
//    std::cout << "  - Min: " << *std::min_element(ns.begin(), ns.end()) << std::endl;
//    std::cout << "  - Max: " << *std::max_element(ns.begin(), ns.end()) << std::endl;
//    std::cout << "  - Mean: " << meanCalc(ns) << std::endl;
//    std::cout << "  - Stdev: " << stdCalc(ns) <<std::endl;
}