#include <stdio.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "message.pb.h"
#include <algorithm>
#include <map>
#include <cmath>
#include <fstream>

#define IP_ADDR "127.0.0.1"
#define PORT 8080

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
    int sock = 0, valread;
    struct sockaddr_in serv_addr;
    char buffer[1024] = {0};
    void *sendBuff = malloc(5);

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("\n Socket creation error \n");
        return -1;
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if(inet_pton(AF_INET, IP_ADDR, &serv_addr.sin_addr)<=0)
    {
        printf("\nInvalid address/ Address not supported \n");
        return -1;
    }

    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    {
        printf("\nConnection Failed \n");
        return -1;
    }

    std::vector<double> ms = { };

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> diff;

    int nIter = 1000;

    for (int i = 0; i < nIter; i++) {
        start = std::chrono::high_resolution_clock::now();
        // Instantiate protobuf obj
        tcpmessage::tcpMessage toServer;
        tcpmessage::tcpMessage fromServer;

        // Prepare protobuf msg
        toServer.set_msgcontent(std::to_string(i));
        toServer.SerializeToArray(sendBuff, 5);

        // Send serialized protobuf to SERVER
        send(sock, sendBuff, 5, 0);

        // Read incoming msg from SERVER
        valread = read(sock, buffer, 5);
        fromServer.ParseFromArray(buffer, 5);

        end = std::chrono::high_resolution_clock::now();
        diff = end - start;
        std::cout << "From server: " << fromServer.msgcontent() << "\n" << std::endl;
        ms.push_back(std::chrono::duration<double, std::milli> (diff).count());
    }

    std::cout << nIter << " queries experiments" << std::endl;
    std::cout << "--- EXECUTION TIME in [ms] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(ms) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(ms) << std::endl;

    // Write to CSV
    writeCSV("ss-sc_1000.csv", "Time taken", ms);
}