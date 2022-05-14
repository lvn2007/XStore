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

void sendMsgInfoToServer(int serverSocket, int nItems, int totalBytes) {
    // Preparing protobuf obj
    xStoregRPC::messageInfo messageInfo;

    // Configuring messageInfo
    messageInfo.set_nitems(nItems);
    messageInfo.set_msglength(totalBytes);

    // messageInfoSize -- Size about message to be sent
    size_t messageInfoSize = messageInfo.ByteSizeLong();

    // Dispatch messageInfo to SERVER
    char messageInfoBuff[messageInfoSize];
    messageInfo.SerializeToArray(messageInfoBuff, messageInfoSize);
    size_t n = send(serverSocket, messageInfoBuff, 128, 0);

    if (n <= 0) {
        close(serverSocket);
        std::cout << "[ERROR] Could not send data to client!" << std::endl;
    }
}

int main(int argc, char const *argv[])
{
//    int sock = 0, valread;
//    struct sockaddr_in serv_addr;
//    char buffer[4096] = {0};
//    void *sendBuff = malloc(32);
//    int flag = 1;
//
//    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
//    {
//        printf("\n Socket creation error \n");
//        return -1;
//    }
//
////    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, (char *) &flag, sizeof(int));
//    serv_addr.sin_family = AF_INET;
//    serv_addr.sin_port = htons(PORT);
//
//    // Convert IPv4 and IPv6 addresses from text to binary form
//    if(inet_pton(AF_INET, IP_ADDR, &serv_addr.sin_addr)<=0)
//    {
//        printf("\nInvalid address/ Address not supported \n");
//        return -1;
//    }
//
//    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
//    {
//        printf("\nConnection Failed \n");
//        return -1;
//    }

    std::vector<double> us = { };
    std::vector<double> protoMs = { };

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> diff;

    int nIter = 10000;
    srand(time(NULL));
    long int randTime;

    zmq::context_t ctx;
    zmq::socket_t sock(ctx, zmq::socket_type::req);

    sock.connect("tcp://172.20.3.113:12345");

    for (int i = 0; i < nIter; i++) {
        randTime = rand() % 1000 + 1;
        long unixTime = 1451606400 + randTime;

        start = std::chrono::high_resolution_clock::now();
        xStoregRPC::unaryRequestMsg toServer;
        toServer.set_rtype(toServer.UNARY);
        toServer.set_timestamp(unixTime);
        toServer.set_dbname("tsla");

        size_t byteSize = toServer.ByteSizeLong();
        zmq::message_t z_out(byteSize);
        toServer.SerializeToArray(z_out.data(), byteSize);
        sock.send(z_out, zmq::send_flags::none);

        // RECEIVING
        std::vector<zmq::message_t> recv_msgs;

        zmq::message_t zFromServer;

//        auto res = sock.recv(zFromServer);
        zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

//        std::cout << "Received: " << zFromServer.to_string_view() << std::endl;
        xStoregRPC::responseData fromServer;
//        fromServer.ParseFromArray(zFromServer.data(), zFromServer.size());
        fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());

        end = std::chrono::high_resolution_clock::now();
        diff = end - start;

        std::cout << "[Response] -- Timestamp: " << fromServer.timestamp() << std::endl;
        std::cout << "[Response] -- Data: " << fromServer.dataset() << std::endl << std::endl;

        us.push_back(std::chrono::duration<double, std::micro> (diff).count());
    }

//    for (int i = 0; i < nIter; i++) {
//        randTime = rand() % 1000 + 1;
//        long targetTime = 1451606400 + randTime;
//        start = std::chrono::high_resolution_clock::now();
//
//        // Instantiate protobuf obj
//        xStoregRPC::unaryRequestMsg toServer;
//        xStoregRPC::responseData fromServer;
//
//        // Preparing request to SERVER
//        toServer.set_timestamp(targetTime);
//        toServer.set_dbname("tsla");
//        toServer.set_rtype(xStoregRPC::unaryRequestMsg::UNARY);
//        toServer.SerializeToArray(sendBuff, 32);
//
//        // Dispatch request to SERVER
//        send(sock, sendBuff, 32, 0);
//
//        // Read incoming msg from SERVER
//        valread = recv(sock, buffer, 4096, 0);
//        fromServer.ParseFromArray(buffer, 4096);
//
//        end = std::chrono::high_resolution_clock::now();
//        diff = end - start;
//        std::cout << "From server: " << fromServer.dataset() << "\n" << std::endl;
//        us.push_back(std::chrono::duration<double, std::micro> (diff).count());
//    }

    std::cout << nIter << " queries experiments" << std::endl;
    std::cout << "--- EXECUTION TIME in [us] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(us) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(us) << std::endl;

    // Write to CSV
    writeCSV("zmq_1000_Protobuf.csv", "Time taken (us)", us);

//    // Close socket
//    close(sock);
}