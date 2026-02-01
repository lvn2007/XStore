#ifndef XSTORE_UTILITIES_H
#define XSTORE_UTILITIES_H

#include <vector>
#include <cmath>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <variant>
#include <ctime>
#include <chrono>
#include <thread>
#include <filesystem>
#include <numeric>
#include <random>
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

static double meanCalc(std::vector<double> &inputVector) {
    double totalSum = 0;

    for (int i = 0; i < inputVector.size(); i++) {
        totalSum += inputVector.at(i);
    }

    return totalSum / inputVector.size();
}

static double stdCalc(std::vector<double> &inputVector) {
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

static void writeCSV(std::string fName, std::string colName, std::vector<double> &inputs) {
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

static void toUpperCase(std::string::iterator begin, std::string::iterator end) {
    for (auto it = begin; it != end; ++it) {
        *it &= ~0x20;
    }
};

static void printStatistics(std::vector<double> &inputVector) {
    spdlog::info("[STATISTICS] Total: {:.4f} (ms)", std::accumulate(inputVector.begin(), inputVector.end(), 0.0));
    spdlog::info("[STATISTICS]  Mean: {:.4f} (ms)", meanCalc(inputVector));
    spdlog::info("[STATISTICS]   Min: {:.4f} (ms)", *std::min_element(inputVector.begin(), inputVector.end()));
    spdlog::info("[STATISTICS]   Max: {:.4f} (ms)", *std::max_element(inputVector.begin(), inputVector.end()));
    spdlog::info("[STATISTICS] STDev: {:.4f} (ms)", stdCalc(inputVector));
}

static void epochPoller(int targetTime) {
    while (true) {
        std::time_t currTime = std::time(nullptr);

        if (currTime >= targetTime) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

static std::vector<std::string> listFiles(std::string beginsWith, std::string path = "") {
    std::vector<std::string> toReturn;

    std::string currDir = (path == "") ? std::filesystem::current_path().string() + "/" + path : std::filesystem::current_path().string() + "/" + path + "/";

    for (auto& file : std::filesystem::directory_iterator(currDir)) {
        std::string currFile = file.path().string().erase(0, currDir.size());

        if (currFile.rfind(beginsWith, 0) == 0) {
            toReturn.emplace_back(currFile);
        }
    }
    return toReturn;
}

static std::vector<std::string> splitByDelim(const std::string& str, const std::string& delim)
{
    std::vector<std::string> vs;
    size_t pos {};

    for (size_t fd = 0; (fd = str.find(delim, pos)) != std::string::npos; pos = fd + delim.size())
        vs.emplace_back(str.data() + pos, str.data() + fd);

    vs.emplace_back(str.data() + pos, str.data() + str.size());
    return vs;
}

// SRC: https://stackoverflow.com/questions/13445688/how-to-generate-a-random-number-in-c
static int randInt(int startRange, int endRange) {
    std::random_device dev;
    std::mt19937 rng(dev());
    std::uniform_int_distribution<std::mt19937::result_type> dist6(startRange, endRange); // distribution in range [1, 6]
    return dist6(rng);
}

// SRC: https://inversepalindrome.com/blog/how-to-create-a-random-string-in-cpp
static std::string randString(int strLen) {
    const std::string CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    std::string random_string;

    for (std::size_t i = 0; i < strLen; ++i)
    {
        random_string += CHARACTERS[distribution(generator)];
    }

    return random_string;
}

static void clearCaches() {
    system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");
}

#endif //XSTORE_UTILITIES_H
