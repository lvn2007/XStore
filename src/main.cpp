#include <iostream>
#include <filesystem>
#include <algorithm>
#include <map>
#include <cmath>
//#include "include/XStore_Networking/XStore_gRPC.h"
#include "include/XStore_Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"
#include "include/dataStruct/clientCmdStruct.h"
#include "xStore.pb.h"
//#include "include/XStore_Networking/zmq.hpp"
#include "include/XStore_Networking/zmq_addon.hpp"

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
//    XStore myStore;

//    myStore.insert(1483228801, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606400, "0,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606401, "NaN,1,NaN,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451606402, "NaN,NaN,2,NaN,NaN,NaN,NaN", "tsla");
//    myStore.insert(1451676402, "NaN,NaN,7,NaN,NaN,NaN,NaN", "tsla");

    std::vector<double> ms = { };
    std::vector<double> us = { };
    std::vector<double> ns = { };

    XStore_zmqServer xStoreZmqServer(32);
    xStoreZmqServer.startServer();

//    XStore_tcpServer tcpServer;
//    tcpServer.startServer();
//    zmq::context_t ctx;
//    zmq::socket_t sock(ctx, zmq::socket_type::router);
////    sock.connect("172.20.3.113:12345");
//    sock.bind("tcp://172.20.3.113:12345");
//
//    for (int i = 0; i < 10000; i++) {
//        zmq::message_t zfromClient;
//        std::vector<zmq::message_t> recv_msgs;
//
////        sock.recv(zfromClient, zmq::recv_flags::none);
//        auto res = zmq::recv_multipart(sock, std::back_inserter(recv_msgs));
//
//        xStoregRPC::unaryRequestMsg fromClient;
////        fromClient.ParseFromArray(zfromClient.data(), zfromClient.size());
//        fromClient.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
////        std::cout << "From client: " << fromClient.dbname() << std::endl;
//
//        xStoregRPC::responseData toClient;
//        toClient.set_timestamp(123456789);
////        toClient.set_dataset("FromServer");
//        size_t byteSize = toClient.ByteSizeLong();
//
//        // SENDING
//        zmq::message_t z_out(byteSize);
//        toClient.SerializeToArray(z_out.data(), byteSize);
//
//        // Reply address
//        sock.send(recv_msgs[0], zmq::send_flags::sndmore);
////        std::cout << "Rep addr: " << recv_msgs[0] << std::endl;
//
//        // Deli
//        zmq::message_t zmqMsgDeli(0);
//        sock.send(zmqMsgDeli, zmq::send_flags::sndmore);
//
//        sock.send(z_out, zmq::send_flags::none);

//        std::cout << "Send status: " << sentRes.value() << std::endl;
//        std::cout << "Sent to CLIENT: " << toClient.dataset() << std::endl;
//    }

//    myStore.getRowSize("tsla");

//    xStoregRPC::dbSizeRecord dbSizeRecord;
//    for (int i = 1; i < 4; i++) {
//        xStoregRPC::recordSize* recordSize = dbSizeRecord.add_recordsize();
//        recordSize->set_dbname(std::to_string(i));
//        recordSize->set_rowsize(i);
//    }
//
//    std::cout << "----" << std::endl;
//
//    for (int i = 0; i < dbSizeRecord.recordsize_size(); i++) {
//        std::cout << dbSizeRecord.recordsize(i).DebugString() << std::endl;
//    }
//
//    std::cout << dbSizeRecord.recordsize_size() << std::endl;

//    while (1) {
//        // Start accepting client
//        tcpServer.acceptClient();
//
//        for (int i = 0; i < 10000; i++) {
//            // Receiving command from CLIENT
//            clientCmdStruct clientCmd = tcpServer.recvCmdFromClient();
//
////            if (clientCmd.requestType == "UNARY") {
//                tcpServer.sendDataToClient(clientCmd.startTime, clientCmd.dbName);
////            }
//        }
//
////        xStoregRPC::unaryRequestMsg toClient;
////
////        toClient.set_timestamp(123456678);
////        toClient.set_dbname("TSLA");
////
////        std::cout << toClient.timestamp() << std::endl;
////        std::cout << toClient.dbname() << std::endl;
////
////        std::cout << toClient.ByteSizeLong() << std::endl;
////        char* array = new char[toClient.ByteSizeLong()];
////        toClient.SerializeToArray(array, toClient.ByteSizeLong());
////
////        std::cout << "START" << std::endl;
////        for (int i = 0; i < toClient.ByteSizeLong(); i++) {
////            std::cout << (int) array[i] << std::endl;
////        }
////        std::cout << "END" << std::endl;
////        std::cout << array[0] << std::endl;
//    }

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