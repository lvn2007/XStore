#ifndef XSTORE_DATAMANAGER_H
#define XSTORE_DATAMANAGER_H

#include <vector>
#include <random>
#include <variant>
#include <fstream>
#include <any>
#include <map>
#include "include/md5.h"
#include "include/csv.hpp"
#include <arrow/io/api.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <arrow/api.h>

// Stream
#include <arrow/io/file.h>
#include <parquet/stream_reader.h>

class DataManager {
public:
    //    std::vector<std::vector<std::any>>readCSV(std::string filePath);
    std::vector<std::vector<std::any>> generateHashData(unsigned long int startTime, unsigned long int endTime, int nColumns);
    arrow::Status writeParquet(std::vector<std::vector<std::any>> *inputVector, std::string &outfileName);
    void writeCSV(std::vector<std::vector<std::any>> inputVector, std::string outfileName);

    // Read test dataset
    //  CSV
    std::vector<std::vector<std::any>> readCSV(std::string csvFilename);
    std::vector<std::vector<std::any>> readCSV(std::string csvFilename, int nRows); // This overloads prev. func.

    //  Arrow Parquet
    arrow::Status readParquet(std::string parquetFilename, std::vector<std::vector<std::any>> *outputVector);

private:
    bool isDecimal(long double number) {
        if (std::ceil(number) == std::floor(number)) {
            return false;
        }
        return true;
    }
};

#endif //XSTORE_DATAMANAGER_H