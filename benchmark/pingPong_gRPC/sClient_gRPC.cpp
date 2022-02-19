#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include "message.grpc.pb.h"
#include <fstream>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

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

class GreeterClient {
public:
    GreeterClient(std::shared_ptr<Channel> channel) : stub_(tcpmessage::pingPong::NewStub(channel)) {}

    // Assembles the client's payload, sends it and presents the response back
    // from the server.
    std::string SayHello(const std::string& user) {
        // Data we are sending to the server.
        tcpmessage::tcpMessage request;
        request.set_msgcontent(user);

        // Container for the data we expect from the server.
        tcpmessage::tcpMessage reply;

        // Context for the client. It could be used to convey extra information to
        // the server and/or tweak certain RPC behaviors.
        ClientContext context;

        // The actual RPC.
        Status status = stub_->unaryRequest(&context, request, &reply);

        // Act upon its status.
        if (status.ok()) {
            return reply.msgcontent();
        } else {
            std::cout << status.error_code() << ": " << status.error_message() << std::endl;
            return "RPC failed";
        }
    }

private:
    std::unique_ptr<tcpmessage::pingPong::Stub> stub_;
};

int main(int argc, char** argv) {
    std::string target_str = "172.20.3.113:8080";
    GreeterClient greeter(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
//    std::string user("world");
//    std::string reply = greeter.SayHello(user);

    std::vector<double> ms = { };

    // Clock init
    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
    std::chrono::duration<double> diff;

    int nIter = 1000;

    for (int i = 0; i < nIter; i++) {
        start = std::chrono::high_resolution_clock::now();

        // Send msg to SERVER
        std::string reply = greeter.SayHello(std::to_string(i));

//        std::cout << "Server replied: " << reply << std::endl;

        end = std::chrono::high_resolution_clock::now();
        diff = end - start;
        ms.push_back(std::chrono::duration<double, std::milli> (diff).count());
    }

    std::cout << nIter << " queries experiments" << std::endl;
    std::cout << "--- EXECUTION TIME in [ms] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(ms) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(ms) << std::endl;

    // Write to CSV
    writeCSV("ss-sc_1000_gRPC.csv", "Time taken (ms)", ms);
}