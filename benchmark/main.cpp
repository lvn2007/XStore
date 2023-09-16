#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include "XStore/XStore_Adapter.h"
#include <variant>

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
    // Handle arguments
    if (argc < 5) {
        std::cerr << "[ERROR - Arguments] -- Missing required (4) arguments" << std::endl;
        std::cerr << "Usage: XStore_Client <SERVER ADDR> <SERVER PORT> <EXPERIMENT TYPE> <TARGET DB>" << std::endl;
        std::cerr << "\nAvaiable EXPERIMENT TYPE: \n   - QUERY_A\n   - QUERY_B" << std::endl;
        std::cerr << "\nAvailable TARGET DB: \n   - XSTORE\n   - MONGODB" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string SERVER_ADDR = argv[1];
    std::string SERVER_PORT = argv[2];
    std::string EXPERIMENT_TYPE = argv[3];
    std::string TARGET_DB = argv[4];

    const std::string ALLOWED_EXPERIMENT[] = {"QUERY_A", "QUERY_B"};
    if (std::find(std::begin(ALLOWED_EXPERIMENT), std::end(ALLOWED_EXPERIMENT), EXPERIMENT_TYPE) == std::end(ALLOWED_EXPERIMENT)) {
        std::cerr << "[ERROR - Arguments] -- Invalid EXPERIMENT TYPE" << std::endl;
        std::cerr << "Available EXPERIMENT TYPE: \n   - QUERY_A\n   - QUERY_B" << std::endl;
        exit(1);
    }

    const std::string ALLOWED_DB[] = {"XSTORE", "MONGODB"};
    if (std::find(std::begin(ALLOWED_DB), std::end(ALLOWED_DB), TARGET_DB) == std::end(ALLOWED_DB)) {
        std::cerr << "[ERROR - Arguments] -- Invalid TARGET DB" << std::endl;
        std::cerr << "Available TARGET DB: \n   - XSTORE\n   - MONGODB" << std::endl;
        exit(1);
    }

    std::vector<double> timeCost;

    if (EXPERIMENT_TYPE == "QUERY_A") {
        int N_ITERATIONS = 1000;
        std::cout << "----- [QUERY_A] -----" << std::endl;
        std::cout << "[BENCHMARK] Performing sequential READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;

        if (TARGET_DB == "XSTORE") {
            XStore_Adapter XStoreAdapter = XStore_Adapter(SERVER_ADDR, SERVER_PORT);
            timeCost = XStoreAdapter.queryA(N_ITERATIONS);
        }
    }
    else if (EXPERIMENT_TYPE == "QUERY_B") {
        int N_ITERATIONS = 1000;
        std::cout << "----- [QUERY_B] -----" << std::endl;
        std::cout << "[BENCHMARK] Performing random READ" << std::endl;
        std::cout << "[BENCHMARK] Iterations: " << N_ITERATIONS << std::endl;

        if (TARGET_DB == "XSTORE") {
            XStore_Adapter XStoreAdapter = XStore_Adapter(SERVER_ADDR, SERVER_PORT);
            timeCost = XStoreAdapter.queryB(N_ITERATIONS, 123, 12345);
        }
    }

    // Display benchmark statistics
    std::cout << "\n----- [STATISTICS] -----" << std::endl;
    std::cout << "  Total: " << std::reduce(timeCost.begin(), timeCost.end()) << " (ms)" << std::endl;
    std::cout << "  Mean:  " << meanCalc(timeCost) << " (ms)" << std::endl;
    std::cout << "  Min:   " << getMin(timeCost) << " (ms)" << std::endl;
    std::cout << "  Max:   " << getMax(timeCost) << " (ms)" << std::endl;
    std::cout << "  Stdev: " << stdCalc(timeCost) << " (ms)" << std::endl;
}