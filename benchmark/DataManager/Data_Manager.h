#ifndef XSTORE_DATAMANAGER_H
#define XSTORE_DATAMANAGER_H

#include <vector>
#include <random>
#include <variant>
#include <fstream>
#include <iostream>
#include <any>
#include <map>
#include "include/md5.h"
#include "include/csv.hpp"
#include "include/utilities.h"
#include <arrow/io/api.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/stream_reader.h>
#include "include/Logger/spdlog/fmt/fmt.h"
#include "include/Logger/Logger.h"

class DataManager {
public:
    unsigned int chunkSize = 1000;    // Optimal chunks = 1000 rows
    arrow::Status writeParquet(unsigned long int startTime, unsigned long int endTime, int nColumns, std::string &outfileName);
    arrow::Status writeParquetNew(unsigned long int startTime, unsigned long int endTime, int nColumns, std::string &outfileName);
    void writeCSV(std::vector<std::vector<std::any>> inputVector, std::string outfileName);

    // Read test dataset
    //  CSV
    std::vector<std::vector<std::any>> readCSV(std::string csvFilename);
    std::vector<std::vector<std::any>> readCSV(std::string csvFilename, int nRows); // This overloads prev. func.

    //  Arrow Parquet
    arrow::Status readParquet(std::string parquetFilename, std::vector<std::vector<std::any>> *outputVector);

    //  Reading QUERY workload
    //      SEQ
    std::vector<std::vector<std::vector<std::any>>> readQueryWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations);
    //      RAND
    std::vector<std::vector<std::vector<std::any>>> readQueryWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations, unsigned int bIterations);

    //  Reading INSERT workload
    //      SEQ
    std::vector<std::vector<std::vector<std::any>>> readInsertWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations);
    //      RAND
    std::vector<std::vector<std::vector<std::any>>> readInsertWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations, unsigned int bIterations);

private:
    std::vector<std::vector<std::any>> generateHashData(unsigned long int startTime, unsigned long int endTime, int nColumns);

    bool isDecimal(long double number) {
        if (std::ceil(number) == std::floor(number)) {
            return false;
        }
        return true;
    }

    std::string hashFromTimestamp(long int timestamp, int columnIdx) {
        return GetMD5String(std::to_string(timestamp) + "-" + std::to_string(columnIdx));
    }

    static std::shared_ptr<parquet::schema::GroupNode> SetupSchema(int nColumns) {
        parquet::schema::NodeVector fields;
        // Create a primitive node named 'boolean_field' with type:BOOLEAN,
        // repetition:REQUIRED

        // First column will always be timestamp
        fields.push_back(parquet::schema::PrimitiveNode::Make("timestamp", parquet::Repetition::REQUIRED, parquet::Type::INT64, parquet::ConvertedType::UINT_64));

        // The rest of columns will be string type
        for (int i = 1; i < nColumns; i++) {
            fields.push_back(parquet::schema::PrimitiveNode::Make("col-" + std::to_string(i), parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));
        }

        // Create a GroupNode named 'schema' using the primitive nodes defined above
        // This GroupNode is the root node of the schema tree
        return std::static_pointer_cast<parquet::schema::GroupNode>(parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
    }
};

#endif //XSTORE_DATAMANAGER_H