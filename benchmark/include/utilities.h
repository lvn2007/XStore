#ifndef XSTORE_UTILITIES_H
#define XSTORE_UTILITIES_H

#include <vector>
#include <cmath>
#include <iostream>
#include <fstream>
#include <algorithm>

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

void printStatistics(std::vector<double> &inputVector) {
    std::cout << "\n----- [STATISTICS] -----" << std::endl;
    std::cout << "  Total: " << std::reduce(inputVector.begin(), inputVector.end()) << " (ms)" << std::endl;
    std::cout << "  Mean:  " << meanCalc(inputVector) << " (ms)" << std::endl;
    std::cout << "  Min:   " << getMin(inputVector) << " (ms)" << std::endl;
    std::cout << "  Max:   " << getMax(inputVector) << " (ms)" << std::endl;
    std::cout << "  Stdev: " << stdCalc(inputVector) << " (ms)" << std::endl;
}

#endif //XSTORE_UTILITIES_H
