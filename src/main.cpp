#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <dirent.h>

// Time-series oriented database
class lanDB {
    public:

        // Check if there's any existing database
        bool hasDB() {
            // Path to dataStore -- Currently hard-coded | WILL NEED TO FIX SO IT IS DYNAMIC
            const std::experimental::filesystem::path tmp_dir{ std::experimental::filesystem::path("/Users/user/CLionProjects/lanDB/dataStore")};
            return !std::experimental::filesystem::is_empty(tmp_dir);
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
    std::cout << bool(lanDbObj.hasDB()) << std::endl;
}