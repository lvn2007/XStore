#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <dirent.h>

// Time-series oriented database
class xStore {
    public:
        // Initialization
        xStore() {
            // Initialized message
            initMsg();

            //  First, check if there's any existing data exists
            hasDB();

            // Second, count number of DB stored
//            dbCount();


        }

        // INSERT time-series oriented values
        void insert(std::string timeValue, std::string value) {

        }

    private:
        // Check if there's any existing database
        bool hasDB() {
            // Path to dataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
            const std::experimental::filesystem::path tmp_dir{ std::experimental::filesystem::path("/home/ubuntu/xStore/dataStore")};
            return !std::experimental::filesystem::is_empty(tmp_dir);
        }

        void initMsg() {
            std::cout << "---- xStore Initialized ----" << std::endl;
            std::cout << " - DB Count: " << dbCount() << std::endl;
        }

        int dbCount() {
            int count = 0;
            std::experimental::filesystem::path dbDir = "/home/ubuntu/xStore/dataStore";

            for (auto& p : std::experimental::filesystem::directory_iterator(dbDir))
            {
                count++;
            }
            return count;
        }
};

int main(int argc, char *argv[]) {
    // Instantiate lanDB obj
    xStore myStore;
}