#include <iostream>
#include <fstream>
#include <filesystem>
#include <iomanip>
#include <time.h>
#include <algorithm>

class dataStruct {
private:
    std::time_t unixTime;
    std::string data;
public:
    // Constructor
    dataStruct(std::time_t unixTime, std::string data) {
        this->unixTime = unixTime;

        // Ensuring no WHITESPACE in data
        data.erase(std::remove(data.begin(), data.end(), ' '), data.end());
        this->data = data;

    }

    // Getter
    std::time_t getUnixtime() {
        return unixTime;
    }

    std::string getData() {
        return data;
    }

    // Simply print out the data it holds in a formatted way
    void printData (bool humanTime) {
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
//            dataStruct currData(1514764800, "111,222,333,444,555,666,777,888");
//            std::cout << currData.getData() << std::endl;
//            std::cout << sizeof(currData.getData()) << std::endl;
        }

        // INSERT time-series oriented values
        void insert(std::time_t unixTime, std::string value, std::string dbName) {
            switch (dbCount) {
                // IF no DB exists i.e., dbCount == 0
                case 0:
                    //  -> Create new one
                    std::filesystem::create_directories(baseDir + "/xStore/dataStore/" + dbName);

                    // Read timeValue -- UNIX time
                    int inputYear = toHumanYear(unixTime);

                    // IF a DB contains no data
                    if (std::filesystem::is_regular_file(std::filesystem::path(baseDir + "/xStore/dataStore/tsla/2018.XSTORE")) == 0) {
                        std::ofstream currFile;
                        // -> Create a file with name of inputYear.XSTORE
                        currFile.open(baseDir + "/xStore/dataStore/" + dbName + "/" + std::to_string(inputYear) + ".XSTORE", std::ios::app|std::ios::binary);

                        // Error handling
                        if (!currFile) {
                            std::cout << "Error creating file: " << std::to_string(inputYear) + ".XSTORE" << std::endl;
                            break;
                        }

                        // IF no error
                        // -> Organize timestamp & data into an object
                        dataStruct currData{unixTime, value};

                        // Write as binary file to disk
                        currFile.write((char *) &currData, sizeof(currData));

                        // Close file after write
                        currFile.close();
                        std::cout << "DONE" << std::endl;
//
//                        currFile.open(std::to_string(inputYear) + ".XSTORE", std::ios::in|std::ios::binary);
//
//                        if (currFile.read((char*) &currData, sizeof(currData))) {
//                            currData.printData(true);
//                        }
                    }
                    break;
            }
        }

        void readData() {
//            std::fstream currFile;
//            dataStruct currData = {};
//
//            currFile.open(baseDir + "/xStore/dataStore/" + "tsla" + "/" + std::to_string(2018) + ".XSTORE", std::ios::in|std::ios::binary);
//
//            currFile.read((char*) &currData, sizeof(dataStruct));
//
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

int main(int argc, char *argv[]) {
    // Instantiate lanDB obj
    xStore myStore;
    myStore.insert(1514764800, "111,222,333,444,555,666,777,888", "tsla");

//    std::time_t unixTime = 1504224002;
//    std::cout << ctime(&unixTime) << std::endl;
//    tm* timePtr = localtime(&unixTime);
//    std::cout << timePtr->tm_year + 1900 << std::endl;
//    std::cout << timePtr->tm_mon + 1 << std::endl;
//    std::cout << std::asctime(std::localtime(&result)) << std::endl;
//    std::tm tm = {};
//    std::stringstream ss("Jan 9 2014 12:35:34");
//    ss >> std::get_time(&tm, "%b %d %Y %H:%M:%S");
//    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));
}