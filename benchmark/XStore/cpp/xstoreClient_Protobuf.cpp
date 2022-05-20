#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <cmath>
#include <fstream>
#include "xStore.pb.h"
#include "zmq.hpp"
#include "zmq_addon.hpp"

#define IP_ADDR "172.20.3.113"
#define PORT 12345

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

void writeCSV(std::string fName, std::string colName, std::vector<double> inputs) {
    // Create an output filestream object
    std::ofstream myFile(fName);

    // Send the column name to the stream
    myFile << colName << "\n";

    // Send data to the stream
    for(int i = 0; i < inputs.size(); ++i)
    {
        myFile << inputs.at(i) << "\n";
    }

    // Close the file
    myFile.close();
}

int main(int argc, char const *argv[])
{
    std::string uniqueID;
    uniqueID = argv[1];

    std::vector<double> us = { };

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> diff;

    int nIter = 1;
    srand(time(NULL));
    long int randTime;

    zmq::context_t ctx;
    zmq::socket_t sock(ctx, zmq::socket_type::req);

    sock.connect("tcp://172.20.3.113:12345");

    for (int i = 0; i < nIter; i++) {
        randTime = rand() % 1000 + 1;
        long unixTime = 1451606400 + randTime;

        // Start timer
        start = std::chrono::high_resolution_clock::now();

        // Unary REQ -- Routine
        xstoreProto::unaryQuery_REQ toServer;
        toServer.set_rtype(xstoreProto::UNARYREQUEST);
        toServer.set_timestamp(1483228802);
        toServer.set_dbname("tsla");

        // RANGE REQ -- Routine
//        xstoreProto::rangeQuery_REQ toServer;
//        toServer.set_rtype(xstoreProto::RANGEREQUEST);
//        toServer.set_starttime(1483228800);
//        toServer.set_endtime(1484338805);
//        toServer.set_dbname("tsla");

        // Info REQ -- Routine
//        xstoreProto::reqCMD toServer;
//        toServer.set_rtype(xstoreProto::INFOREQUEST);

        // Unary Insert -- Routine
//        xstoreProto::unaryInsert_REQ toServer;
//        toServer.set_rtype(xstoreProto::UNARYINSERT);
//        toServer.set_timestamp(1483228802);
//        toServer.set_data("Test");
//        toServer.set_dbname("tsla");

        // CreateDB -- Routine
//        xstoreProto::createDB_REQ toServer;
//        toServer.set_rtype(xstoreProto::CREATEDBREQUEST);
//        toServer.set_dbname("abc");
//        toServer.set_rowlength(8);

        // Config msg and send
        size_t byteSize = toServer.ByteSizeLong();
        zmq::message_t z_out(byteSize);
        toServer.SerializeToArray(z_out.data(), byteSize);
        sock.send(z_out, zmq::send_flags::none);

        // RECEIVING
        std::vector<zmq::message_t> recv_msgs;
        zmq::message_t zFromServer;
        zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

        // Received structs
        //  UNARY REP
        xstoreProto::unaryQuery_REP fromServer;

        //  RANGE REP
//        xstoreProto::batchQuery_REP fromServer;

        //  INFO REP
//        xstoreProto::sysInfo_REP fromServer;

        //  UNARY INSERT REP
//        xstoreProto::unaryInsert_REP fromServer;

        //  CREATE DB REP
//        xstoreProto::createDB_REP fromServer;

        fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());

        // End timer
        end = std::chrono::high_resolution_clock::now();
        diff = end - start;

        // OUTPUTS PRINTING
        fromServer.PrintDebugString();

//        std::cout << "[Response] -- Timestamp: " << fromServer.timestamp() << std::endl;
//        std::cout << "[Response] -- Data: " << fromServer.dataset() << std::endl << std::endl;

        us.push_back(std::chrono::duration<double, std::milli> (diff).count());
    }

    std::cout << nIter << " queries experiments" << std::endl;
    std::cout << "--- EXECUTION TIME in [ms] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(us) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(us) << std::endl;

    // Write to CSV
//    writeCSV("zmq_1000_Protobuf-" + uniqueID + ".csv", "Time taken (ms)", us);

//    // Close socket
//    close(sock);
}
