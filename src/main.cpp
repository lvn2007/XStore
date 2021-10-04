#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <dirent.h>

// Time-series oriented database
class lanDB {
    public:

        // Check if there's any existing database
        bool hasDB() {
//            const char *dataStorePath = "dataStore";
            std::experimental::filesystem::path dataStorePath = "../lanDB/dataStore";
//            std::cout << "Directory: " << dataStorePath << " is " << std::experimental::filesystem::path(dataStorePath).empty() << std::endl;
            std::cout << dataStorePath.parent_path() << std::endl;
        }

        // INSERT time-series oriented values
        void insert(std::string timeValue, std::string value) {

        }
    private:
};

int main(int argc, char *argv[]) {
    std::cout << "Lan DB" << std::endl;

    // Instantiate lanDB obj
    lanDB lanDbObj;
    lanDbObj.hasDB();
}