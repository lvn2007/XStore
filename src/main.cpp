#include <iostream>
#include <filesystem>
#include <algorithm>
#include <map>
#include <cstring>
#include <cmath>
#include "include/XStore/XStore.h"

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

    myStore.getBatch(1451606400, 1451606403, "tsla");
//    myStore.switchDB("tsla");
//    std::cout << myStore.getp(1451606400, "tsla") << std::endl;
//    std::cout << myStore.getp(1451606401, "tsla") << std::endl;
//    std::cout << myStore.getp(1451606402, "tsla") << std::endl;
//    std::cout << myStore.getp(1451606403, "tsla") << std::endl;
//    std::cout << myStore.getp(1451676402, "tsla") << std::endl;
//    std::cout << myStore.getp(1483228801, "tsla") << std::endl;

    // Microbench
//    srand(time(NULL));
//    long int randTime;
//    int nIter = 1000;
//    for (int i = 0; i < nIter; i++) {
////        system("free > /dev/null");
////        system("sync > /dev/null");
////        system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
////        system("free > /dev/null");
//        randTime = rand() % 1000 + 1;
//        long unixTime = 1451606400 + randTime;
//        std::string currVal;
//
//        start = std::chrono::high_resolution_clock::now();
////        currVal = myStore.get(unixTime, "tsla");
////        currVal = myStore.getSingleOpenClose(unixTime, "tsla");
//        currVal = myStore.getp(unixTime, "tsla");
//        end = std::chrono::high_resolution_clock::now();
//        diff = end - start;
//
//        ms.push_back(std::chrono::duration<double, std::milli> (diff).count());
//        us.push_back(std::chrono::duration<double, std::micro> (diff).count());
//        ns.push_back(std::chrono::duration<double, std::nano> (diff).count());
//
////        std::cout << "Execution time: " << std::chrono::duration<double, std::milli> (diff).count() << " [ms]" << std::endl;
////        std::cout << "Execution time: " << std::chrono::duration<double, std::micro> (diff).count() << " [µs]" << std::endl;
////        std::cout << "Execution time: " << std::chrono::duration<double, std::nano> (diff).count() << " [ns]" << std::endl;
//
////        std::cout << "Target: " << unixTime << " | Result: " <<  currVal << std::endl;
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