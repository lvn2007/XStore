#include <iostream>
#include <fstream>
#include <filesystem>
#include <algorithm>
#include <cstdio>
#include <map>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cmath>

// Every line of data == 100 bytes in size
#define lineSize 100
#define ALIGNMENT 4096
#define BUFSIZE 1024

// Data structure to hold time-value together
struct dataStruct {
    std::time_t unixTime;
    std::string data;

    // Constructor
    dataStruct(std::time_t unixTime, std::string data) {
            this->unixTime = unixTime;
            this->data = data;

            // Ensuring no WHITESPACE in data
            data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
    }

    // Returning a line
    //  i.e., unixTime, values
    std::string getData() {
        return std::to_string(unixTime) + "," + data;
    }

    // Printing out time-value
    void printData(bool humanTime) {
        if (humanTime) {
            std::cout << "Timestamp: " << ctime(&unixTime) << " | Data: " << data << std::endl;
            return;
        }
        std::cout << "Timestamp: " << unixTime << " | Data: " << data << std::endl;
    }
};

// Time-series oriented database
class xStore {
    public:
        std::string baseDir = "/home/ubuntu";
        int dbCount = 0;
        std::vector<std::string> dbNames = { };
        std::map<std::string, long int> resolutionMap { {"SECOND", 31536000}, {"MILLISECOND", 31536000000}, {"MICROSECOND", 31536000000000} };

        // Initialization
        xStore() {
            // Initialized message
            initMsg();

            //  First, check if there's any existing data exists
            hasDB();

            std::cout << toHumanYear(1514764800) << std::endl;
            std::cout << toHumanMonth(1514764800) << std::endl;
            std::cout << toHumanDay(1514764800) << std::endl;
            std::cout << getTimeResolution(1514764800) << std::endl;
        }

        // INSERT time-series oriented values
        void insert(std::time_t unixTime, std::string value, std::string dbName) {
            // Read timeValue -- UNIX time
            int inputYear = toHumanYear(unixTime);
            std::string currResolution = getTimeResolution(unixTime);
            std::cout << resolutionMap[currResolution] << std::endl;

            switch (dbCount) {
                // IF no DB exists i.e., dbCount == 0
                case 0:
                    //  -> Create new one
                    std::filesystem::create_directories(baseDir + "/xStore/dataStore/" + dbName);

                    FILE *bFile;
                    // -> Create a file with name of inputYear.XSTORE
                    std::string dataDir = baseDir + "/xStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
                    bFile = std::fopen(dataDir.c_str(), "wb");

                    // Start & End time of a given year
                    long int startEpoch = getStartTimeEpoch(inputYear, currResolution);
                    long int endEpoch = getEndTimeEpoch(inputYear, currResolution);

                    // Staged area for xStore -- Writing dummy data prior to writing real data
                    for (long int i = startEpoch; i <= endEpoch; i++) {
                        // Preparing line of data with dummy variables based on passed input "value"
                        dataStruct dummyData{i, value};

                        // Write to binary file to disk with new line indicates EOL
                        std::fwrite((dummyData.getData() + "\n").data(), lineSize + 1, 1, bFile);
                    }

                    // IF no error
                    // -> Organize timestamp & data into an object
                    dataStruct currData{unixTime, value};

                    // DEBUG: Printing data current size
                    std::cout << currData.getData().size() << std::endl;

                    // Write as binary file to disk with new line indicates EOL
                    std::fwrite((currData.getData() + "\n").data(), lineSize + 1, 1, bFile);

                    // Close file after write
                    std::fclose(bFile);
                    break;
            }
        }

        std::string get(std::time_t unixTime, std::string dbName) {
            // Read timeValue -- UNIX time
            int inputYear = toHumanYear(unixTime);
            std::string currResolution = getTimeResolution(unixTime);

            // Start epoch time of a given year
            long int startEpoch = getStartTimeEpoch(inputYear, currResolution);

            // Epoch time difference between target & startEpoch
            long int diff = unixTime - startEpoch;

            // Calculate offset -- Exact location of where a particular target row is stored
            long int offSet = diff * (lineSize + 1);

            FILE *bFile;
            std::string dataDir = baseDir + "/xStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE";
            bFile = std::fopen(dataDir.c_str(), "rb");
            char *buff = new char[100];
            std::fseek(bFile, offSet, SEEK_SET);
            std::fgets(buff, lineSize + 1, bFile);
//            std::puts(buff);
            std::fclose(bFile);
            std::string toReturn(buff);
            return toReturn;
        }

    private:
        // Check if there's any existing database
        bool hasDB() {
            // Path to dataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
            const std::filesystem::path tmp_dir{ std::filesystem::path(baseDir + "/xStore/dataStore")};
            return !std::filesystem::is_empty(tmp_dir);
        }

        void initMsg() {
            std::cout << "---- xStore Initialized ----" << std::endl;
            dbStats();
            std::cout << " - DB Count: " <<  dbCount << std::endl;

            std::cout << " - DB Names: [ ";
            for (int i = 0; i < dbNames.size(); i++) {
                if (i + 1 != dbNames.size()) {
                    std::cout << dbNames.at(i) << ", ";
                    continue;
                }
                std::cout << dbNames.at(i);
            }
            std::cout << " ]" << std::endl;
        }

        void dbStats() {
            std::filesystem::path dbDir = baseDir + "/xStore/dataStore";

            for (auto& p : std::filesystem::directory_iterator(dbDir))
            {
                dbCount++;
                dbNames.push_back(p.path().filename());
            }
        }

        bool yearExists(int inputYear, std::string dbName) {
            std::string path = baseDir + "/xStore/dataStore" + dbName;

            for (const auto & entry : std::filesystem::directory_iterator(path)) {
                std::cout << entry.path();
            }
        }


        // Returning epoch value of FIRST available datetime of a given year
        long int getStartTimeEpoch(int yearInput, std::string timeResolution) {
            std::map<std::string, int> resolutionMap { {"SECOND", 1}, {"MILLISECOND", 1000}, {"MICROSECOND", 1000000} };
            struct tm epochConst = {0};
            epochConst.tm_year = yearInput-1900;
            epochConst.tm_mon = 0;
            epochConst.tm_mday = 0;
            epochConst.tm_hour = 24;
            epochConst.tm_min = 0;
            epochConst.tm_sec = 0;
            time_t startTime = mktime(&epochConst);
            long int startEpoch = static_cast<long int> (startTime);
            return startEpoch * resolutionMap[timeResolution];
        }

        // Returning epoch value of LAST available datetime of a given year
        long int getEndTimeEpoch(int yearInput, std::string timeResolution) {
            std::map<std::string, int> resolutionMap { {"SECOND", 1}, {"MILLISECOND", 1000}, {"MICROSECOND", 1000000} };
            struct tm epochConst = {0};
            epochConst.tm_year = yearInput-1900;
            epochConst.tm_mon = 12-1;
            epochConst.tm_mday = 31;
            epochConst.tm_hour = 23;
            epochConst.tm_min = 59;
            epochConst.tm_sec = 59;
            time_t startTime = mktime(&epochConst);
            long int startEpoch = static_cast<long int> (startTime);
            return startEpoch * resolutionMap[timeResolution];
        }

        // Time extractor
        std::string getTimeResolution(std::time_t unixTime) {
            int count = 0;

            while (unixTime != 0) {
                unixTime /= 10;
                ++count;
            }

            if (count >= 10 && count < 13) {
                return "SECOND";
            }
            else if (count >= 13 && count < 16) {
                return "MILLISECOND";
            }
            else if (count >= 16) {
                return "MICROSECOND";
            }
        }

        int toHumanYear(std::time_t unixTime) {
            tm* timePtr = localtime(&unixTime);
            return timePtr -> tm_year + 1900;
        }

        int toHumanMonth(std::time_t unixTime) {
            tm* timePtr = localtime(&unixTime);
            return timePtr -> tm_mon + 1;
        }

        int toHumanDay(std::time_t unixTime) {
            tm* timePtr = localtime(&unixTime);
            return timePtr -> tm_mday;
        }
};

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

    // Instantiate xStore obj
    xStore myStore;
//    myStore.insert(1504224000, "NaN,NaN,NaN,NaN,NaN,NaN,NaN", "tsla");

    std::vector<double> ms = { };
    std::vector<double> us = { };
    std::vector<double> ns = { };

    // Microbench
    srand(time(NULL));
    long int randTime;
    int nIter = 1000;
    for (int i = 0; i < nIter; i++) {
        system("free > /dev/null");
        system("sync > /dev/null");
        system("sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'");
        system("free > /dev/null");
        randTime = rand() % 1000 + 1;
        long unixTime = 1504224000 + randTime;
        std::string currVal;
        start = std::chrono::high_resolution_clock::now();
        currVal = myStore.get(unixTime, "tsla");
        end = std::chrono::high_resolution_clock::now();
        diff = end - start;

        ms.push_back(std::chrono::duration<double, std::milli> (diff).count());
        us.push_back(std::chrono::duration<double, std::micro> (diff).count());
        ns.push_back(std::chrono::duration<double, std::nano> (diff).count());

//        std::cout << "Execution time: " << std::chrono::duration<double, std::milli> (diff).count() << " [ms]" << std::endl;
//        std::cout << "Execution time: " << std::chrono::duration<double, std::micro> (diff).count() << " [µs]" << std::endl;
//        std::cout << "Execution time: " << std::chrono::duration<double, std::nano> (diff).count() << " [ns]" << std::endl;

//        std::cout << "Target: " << unixTime << " | Result: " <<  currVal << std::endl;
    }

    std::cout << nIter << " queries experiments" << std::endl;
    std::cout << "\n--- EXECUTION TIME in [ms] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(ms.begin(), ms.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(ms) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(ms) << std::endl;

    std::cout << "--- EXECUTION TIME in [µs] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(us.begin(), us.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(us) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(us) << std::endl;

    std::cout << "--- EXECUTION TIME in [ns] ---" << std::endl;
    std::cout << "  - Min: " << *std::min_element(ns.begin(), ns.end()) << std::endl;
    std::cout << "  - Max: " << *std::max_element(ns.begin(), ns.end()) << std::endl;
    std::cout << "  - Mean: " << meanCalc(ns) << std::endl;
    std::cout << "  - Stdev: " << stdCalc(ns) <<std::endl;
}