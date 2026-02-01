#include "Data_Manager.h"

std::vector<std::vector<std::any>> DataManager::generateHashData(unsigned long int startTime, unsigned long int endTime, int nColumns) {
    // Returning vector contains rows of data
    std::vector<std::vector<std::any>> toReturn;

    // Placeholder for data generation
    long unsigned int currTime;
    std::vector<std::any> rowData;
    std::string hashValue;

    // Loop through & Generate data for each unique timestamp starting from startTime
    for (long unsigned int i = 0; i < (endTime - startTime); i++) {
        currTime = startTime + i;

        // Append timestamp
        rowData.emplace_back(currTime);

        // Loop through each column and create hash value
        for (int numCol = 0; numCol < nColumns; numCol++) {
            hashValue = GetMD5String(std::to_string(currTime) + "-" + std::to_string(numCol + 1));
            rowData.emplace_back(hashValue);
        }

        // Add current row to record;
        toReturn.emplace_back(rowData);

        // Reset rowData
        rowData.clear();
    }
    return toReturn;
}

arrow::Status DataManager::writeParquet(unsigned long int startTime, unsigned long int endTime, int nColumns, std::string &outfileName) {
    // File output stream
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(outfileName + ".parquet"));

    // Parquet schema
    std::shared_ptr<parquet::schema::GroupNode> schema = SetupSchema(nColumns);

    // Writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::GZIP);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    std::shared_ptr<parquet::ParquetFileWriter> file_writer = parquet::ParquetFileWriter::Open(outfile, schema, props);

    unsigned long int totalLoop = (endTime - startTime) / chunkSize;
    unsigned long int currentEndTime = startTime;

    // Gen hash on-demand
    for (unsigned long int idx = 0; idx < totalLoop; idx++) {
        // Append a RowGroup with a specific number of rows.
        parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

        currentEndTime = startTime + (chunkSize * idx) + chunkSize;

        // Writing timestamp column
        parquet::Int64Writer* int64_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
        for (long int timestamp = (startTime + (chunkSize * idx)); timestamp < currentEndTime; timestamp++) {
            int64_writer->WriteBatch(1, nullptr, nullptr, &timestamp);
        }

        // Writing hash columns
        std::string currentStr;
        parquet::ByteArray value;
        for (int colIdx = 1; colIdx < nColumns; colIdx++) {
            parquet::ByteArrayWriter* ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
            for (long int timestamp = (startTime + (chunkSize * idx)); timestamp < currentEndTime; timestamp++) {
                currentStr = this->hashFromTimestamp(timestamp, colIdx);
                value.ptr = reinterpret_cast<const uint8_t*>(currentStr.c_str());
                value.len = currentStr.length();
                ba_writer->WriteBatch(1, nullptr, nullptr, &value);
            }
        }
    }

    if (currentEndTime < endTime) {
        // Append a RowGroup with a specific number of rows.
        parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

        // Writing timestamp column
        parquet::Int64Writer* int64_writer = static_cast<parquet::Int64Writer*>(rg_writer->NextColumn());
        for (long int timestamp = currentEndTime; timestamp < endTime; timestamp++) {
            int64_writer->WriteBatch(1, nullptr, nullptr, &timestamp);
        }

        // Writing hash columns
        std::string currentStr;
        parquet::ByteArray value;
        for (int colIdx = 1; colIdx < nColumns; colIdx++) {
            parquet::ByteArrayWriter* ba_writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->NextColumn());
            for (long int timestamp = currentEndTime; timestamp < endTime; timestamp++) {
                currentStr = this->hashFromTimestamp(timestamp, colIdx);
                value.ptr = reinterpret_cast<const uint8_t*>(currentStr.c_str());
                value.len = currentStr.length();
                ba_writer->WriteBatch(1, nullptr, nullptr, &value);
            }
        }
    }

    file_writer->Close();
    return outfile->Close();

}

void DataManager::writeCSV(std::vector<std::vector<std::any>> inputVector, std::string outfileName) {
    // OFStream object
    std::ofstream outFile(outfileName);

    // Loop through each row of data
    int i, j;
    for (i = 0; i < inputVector.size(); i++) {
        // Push datapoints to stream
        for (j = 0; j < inputVector.at(i).size() - 1; j++) {
            outFile << std::any_cast<std::string>(inputVector.at(i).at(j)) << ",";
        }
        // Send last datapoint to stream then ends with newline char
        outFile << std::any_cast<std::string>(inputVector.at(i).at(j)) << "\n";
    }
    outFile.close();
}

std::vector<std::vector<std::any>> DataManager::readCSV(std::string filePath) {
    // Returning vector contains rows of data
    std::vector<std::vector<std::any>> toReturn;

    // CSV Parser format
    csv::CSVFormat format;
    format.no_header();

    // CSV Parser
    csv::CSVReader reader(filePath, format);

    // Placeholder for each row
    std::vector<std::any> rowData;

    // Loop through each row
    int i;
    for (csv::CSVRow& row : reader) {
        // First field is always timestamp
        rowData.emplace_back(row[0].get<unsigned long int>());

        for (i = 1; i < row.size(); i++) {
            // Metric == null
            if (row[i].is_null()) {
                rowData.emplace_back("");
                continue;
            }
                // Metric == string
            else if (row[i].is_str()) {
                rowData.emplace_back(row[i].get<std::string>());
                continue;
            }
                // Metric == integer
            else if (row[i].is_int()) {
                rowData.emplace_back(row[i].get<int>());
                continue;
            }
                // Metric == floating point
            else if (row[i].is_float()) {
                rowData.emplace_back(row[i].get<double>());
                continue;
            }
        }

        // Append to record
        toReturn.emplace_back(rowData);

        // Clear rowData
        rowData.clear();
    }
    return toReturn;
}

std::vector<std::vector<std::any>> DataManager::readCSV(std::string filePath, int nRows) {
    // Returning vector contains rows of data
    std::vector<std::vector<std::any>> toReturn;

    // CSV Parser format
    csv::CSVFormat format;
    format.no_header();

    // CSV Parser
    csv::CSVReader reader(filePath, format);

    // Placeholder for each row
    std::vector<std::any> rowData;

    // Loop through each row
    int i;
    for (csv::CSVRow& row : reader) {
        // First field is always timestamp
        rowData.emplace_back(row[0].get<unsigned long int>());

        for (i = 1; i < row.size(); i++) {
            // Metric == null
            if (row[i].is_null()) {
                rowData.emplace_back("");
                continue;
            }
                // Metric == string
            else if (row[i].is_str()) {
                rowData.emplace_back(row[i].get<std::string>());
                continue;
            }
                // Metric == integer
            else if (row[i].is_int()) {
                rowData.emplace_back(row[i].get<int>());
                continue;
            }
                // Metric == floating point
            else if (row[i].is_float()) {
                rowData.emplace_back(row[i].get<double>());
                continue;
            }
        }

        // Append to record
        toReturn.emplace_back(rowData);

        // Clear rowData
        rowData.clear();

        // Keep track of items -- Suspend when >= nRows
        if (toReturn.size() >= nRows) {
            break;
        }
    }
    return toReturn;
}

arrow::Status DataManager::readParquet(std::string parquetFilename, std::vector<std::vector<std::any>> *outputVector) {
    // Allowed data-types
    std::string ALLOWED_DTYPES[] = {"utf8", "string", "int", "uint64", "float", "double"};

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(parquetFilename));

    // Reader properties
    parquet::ArrowReaderProperties arrow_reader_properties = parquet::default_arrow_reader_properties();
    arrow_reader_properties.set_pre_buffer(true);
    arrow_reader_properties.set_use_threads(true);
    parquet::ReaderProperties reader_properties = parquet::default_reader_properties();

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    auto reader_builder = parquet::arrow::FileReaderBuilder();
    reader_builder.properties(arrow_reader_properties);
    reader_builder.Open(input, reader_properties);

    ARROW_RETURN_NOT_OK(reader_builder.Build(&arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    // First column will always be timestamp
    // Placeholder for returning data
    int numChunks = table->column(0)->num_chunks();
    for (int i = 0; i < numChunks; i++) {
        auto timestampArray = std::dynamic_pointer_cast<arrow::UInt64Array>(table->column(0)->chunk(i));

        for (unsigned long int rowIdx = 0; rowIdx < timestampArray->length(); rowIdx++) {
            outputVector->emplace_back(std::vector<std::any>{static_cast<unsigned long int>(timestampArray->Value(rowIdx))});
        }
    }

    // Check types of all columns
    for (int i = 1; i < table->num_columns(); i++) {
        std::string currentType = table->column(i)->type()->name();

        // Permitted types: string, int, float, double
        //  Note: Float will be treated as double
        if (std::find(std::begin(ALLOWED_DTYPES), std::end(ALLOWED_DTYPES), currentType) == std::end(ALLOWED_DTYPES)) {
            std::string typeErrorMsg = fmt::format("Type: {} is not supported!", currentType);
            spdlog::error("[READ PARQUET] - {}", typeErrorMsg);
            return arrow::Status::TypeError(typeErrorMsg);
        }

        // Each array specifics
        int localIdx = 0;
        numChunks = table->column(i)->num_chunks();

        if (currentType == "utf8") {
            for (int j = 0; j < numChunks; j++) {
                auto stringArray = std::dynamic_pointer_cast<arrow::StringArray>(table->column(i)->chunk(j));

                for (unsigned long int rowIdx = 0; rowIdx < stringArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<std::string>(stringArray->GetString(rowIdx + localIdx)));
                }
                localIdx += stringArray->length();
            }
        }
        else if (currentType == "uint64") {
            for (int j = 0; j < numChunks; j++) {
                auto uint64Array = std::dynamic_pointer_cast<arrow::UInt64Array>(table->column(i)->chunk(j));

                for (unsigned long int rowIdx = 0; rowIdx < uint64Array->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<unsigned long int>(uint64Array->Value(rowIdx + localIdx)));
                }
                localIdx += uint64Array->length();
            }
        }
        else if (currentType == "float") {
            for (int j = 0; j < numChunks; j++) {
                auto floatArray = std::dynamic_pointer_cast<arrow::FloatArray>(table->column(i)->chunk(j));

                for (unsigned long int rowIdx = 0; rowIdx < floatArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<double>(floatArray->Value(rowIdx + localIdx)));
                }
                localIdx += floatArray->length();
            }
        }
        else if (currentType == "double") {
            for (int j = 0; j < numChunks; j++) {
                auto doubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(table->column(i)->chunk(j));

                for (unsigned long int rowIdx = 0; rowIdx < doubleArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<double>(doubleArray->Value(rowIdx + localIdx)));
                }
                localIdx += doubleArray->length();
            }
        }
    }
    return arrow::Status::OK();
}

std::vector<std::vector<std::vector<std::any>>> DataManager::readQueryWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations) {
    // Filter outputs display
    std::string UNARY_WORKLOADS[] = {"SEQ", "RAND"};

    // Containing all data for all threads
    std::vector<std::vector<std::vector<std::any>>> toReturn;
    std::vector<std::vector<std::any>> tempVector;

    // Get list of files
    std::vector<std::string> targetFileNames = listFiles(nameBeginsWith);

    // Sort list of filename ASC
    std::sort(targetFileNames.begin(), targetFileNames.end(), std::less<>());

    // Check if files match bench criteria
    //  Expecting nThreads == N number of files
    if (targetFileNames.size() < nThreads) {
        spdlog::error("[READ QUERY WORKLOAD] - Insufficient test dataset");
        exit(1);
    }

    //  Expecting correct sample size
    unsigned int parsedSampleSize;
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Parse embedded sample size from filename with filter
        parsedSampleSize = std::stoul(splitByDelim(currFile, "_").at(2));

        // Expecting nIterations == Parsed sample size
        if (parsedSampleSize != nIterations) {
            spdlog::error("[READ QUERY WORKLOAD] - Unexpected sample size");
            exit(1);
        }
    }

    // Reading each parquet workload files
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Read workload into temp vector
        this->readParquet(currFile, &tempVector);

        // Add to master vector
        toReturn.emplace_back(tempVector);

        // Clear temp vector
        tempVector.clear();
    }
    return toReturn;
}

std::vector<std::vector<std::vector<std::any>>> DataManager::readQueryWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations, unsigned int bIterations) {
    // Filter outputs display
    std::string UNARY_WORKLOADS[] = {"SEQ", "RAND"};

    // Containing all data for all threads
    std::vector<std::vector<std::vector<std::any>>> toReturn;
    std::vector<std::vector<std::any>> tempVector;

    // Get list of files
    std::vector<std::string> targetFileNames = listFiles(nameBeginsWith);

    // Sort list of filename ASC
    std::sort(targetFileNames.begin(), targetFileNames.end(), std::less<>());

    // Check if files match bench criteria
    //  Expecting nThreads == N number of files
    if (targetFileNames.size() < nThreads) {
        spdlog::error("[READ QUERY WORKLOAD] - Insufficient test dataset");
        exit(1);
    }

    //  Expecting correct sample size
    unsigned int parsedSampleSize, parsedBatchIter;
    std::string toParse;
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Parse embedded sample size from filename with filter
        toParse = splitByDelim(currFile, "_").at(2);
        parsedSampleSize = std::stoul(splitByDelim(toParse, "-").at(1));
        parsedBatchIter = std::stoul(splitByDelim(toParse, "-").at(0));

        // Expecting nIterations == Parsed sample size
        if (parsedSampleSize != nIterations) {
            spdlog::error("[READ QUERY WORKLOAD] - Unexpected sample size");
            exit(1);
        }

        // Expecting bIterations == Parsed batch size
        if (parsedBatchIter != bIterations) {
            spdlog::error("[READ QUERY WORKLOAD] - Unexpected batch iteration");
            exit(1);
        }
    }

    // Reading each parquet workload files
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Read workload into temp vector
        this->readParquet(currFile, &tempVector);

        // Add to master vector
        toReturn.emplace_back(tempVector);

        // Clear temp vector
        tempVector.clear();
    }
    return toReturn;
}

std::vector<std::vector<std::vector<std::any>>> DataManager::readInsertWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations) {
    // Filter outputs display
    std::string UNARY_WORKLOADS[] = {"SEQ", "RAND"};

    // Containing all data for all threads
    std::vector<std::vector<std::vector<std::any>>> toReturn;
    std::vector<std::vector<std::any>> tempVector;

    // Get list of files
    std::vector<std::string> targetFileNames = listFiles(nameBeginsWith);

    // Sort list of filename ASC
    std::sort(targetFileNames.begin(), targetFileNames.end(), std::less<>());

    // Check if files match bench criteria
    //  Expecting nThreads == N number of files
    if (targetFileNames.size() < nThreads) {
        spdlog::error("[READ INSERT WORKLOAD] - Insufficient test dataset");
        exit(1);
    }

    //  Expecting correct sample size
    unsigned int parsedSampleSize;
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Parse embedded sample size from filename with filter
        parsedSampleSize = std::stoul(splitByDelim(currFile, "_").at(2));

        // Expecting nIterations == Parsed sample size
        if (parsedSampleSize != nIterations) {
            spdlog::error("[READ INSERT WORKLOAD] - Unexpected sample size");
            exit(1);
        }
    }

    // Reading each parquet workload files
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Read workload into temp vector
        this->readParquet(currFile, &tempVector);

        // Add to master vector
        toReturn.emplace_back(tempVector);

        // Clear temp vector
        tempVector.clear();
    }
    return toReturn;
}

std::vector<std::vector<std::vector<std::any>>> DataManager::readInsertWorkload(std::string nameBeginsWith, unsigned int nThreads, unsigned int nIterations, unsigned int bIterations) {
    // Filter outputs display
    std::string UNARY_WORKLOADS[] = {"SEQ", "RAND"};

    // Containing all data for all threads
    std::vector<std::vector<std::vector<std::any>>> toReturn;
    std::vector<std::vector<std::any>> tempVector;

    // Get list of files
    std::vector<std::string> targetFileNames = listFiles(nameBeginsWith);

    // Sort list of filename ASC
    std::sort(targetFileNames.begin(), targetFileNames.end(), std::less<>());

    // Check if files match bench criteria
    //  Expecting nThreads == N number of files
    if (targetFileNames.size() < nThreads) {
        spdlog::error("[READ INSERT WORKLOAD] - Insufficient test dataset");
        exit(1);
    }

    //  Expecting correct sample size
    unsigned int parsedSampleSize, parsedBatchIter;
    std::string toParse;
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Parse embedded sample size from filename with filter
        toParse = splitByDelim(currFile, "_").at(2);
        parsedSampleSize = std::stoul(splitByDelim(toParse, "-").at(1));
        parsedBatchIter = std::stoul(splitByDelim(toParse, "-").at(0));

        // Expecting nIterations == Parsed sample size
        if (parsedSampleSize != nIterations) {
            spdlog::error("[READ INSERT WORKLOAD] - Unexpected sample size");
            exit(1);
        }

        // Expecting bIterations == Parsed batch size
        if (parsedBatchIter != bIterations) {
            spdlog::error("[READ INSERT WORKLOAD] - Unexpected batch iteration");
            exit(1);
        }
    }

    // Reading each parquet workload files
    for (int i = 0; i < nThreads; i++) {
        // File name
        std::string currFile = targetFileNames.at(i);

        // Read workload into temp vector
        this->readParquet(currFile, &tempVector);

        // Add to master vector
        toReturn.emplace_back(tempVector);

        // Clear temp vector
        tempVector.clear();
    }
    return toReturn;
}