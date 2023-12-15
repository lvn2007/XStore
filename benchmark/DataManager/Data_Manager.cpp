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
    // Open Parquet file & Other properties
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    std::unique_ptr<parquet::arrow::FileWriter> writer;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(outfileName + ".parquet"));

    // Parquet compression
    std::shared_ptr<parquet::WriterProperties> props = parquet::WriterProperties::Builder().compression(arrow::Compression::GZIP)->build();
    std::shared_ptr<parquet::ArrowWriterProperties> arrow_props = parquet::ArrowWriterProperties::Builder().store_schema()->build();

    unsigned long int totalLoop = (endTime - startTime) / chunkSize;

    // Create schema for output file
    arrow::FieldVector fields;

    //  First column will always be timestamp
    fields.push_back(arrow::field("timestamp", arrow::uint64()));

    //  Loop through the rest of columns
    for (int i = 0; i < nColumns; i++)
    {
        fields.push_back(arrow::field("col" + std::to_string(i), arrow::utf8()));
    }
    std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);

    ARROW_ASSIGN_OR_RAISE(writer, parquet::arrow::FileWriter::Open(*(schema), arrow::default_memory_pool(), outfile, props, arrow_props));

    unsigned long int currentEndTime = startTime;
    for (unsigned long int idx = 0; idx < totalLoop; idx++) {
        currentEndTime = startTime + (chunkSize * idx) + chunkSize;
        std::vector<std::vector<std::any>> hashData = this->generateHashData(startTime + (chunkSize * idx), currentEndTime, nColumns);

        // Store data into arrow::Table for output
        arrow::ArrayVector array_vector;

        std::shared_ptr<arrow::Array> data_array;
        arrow::UInt64Builder iBuilder;

        for (int i = 0; i < hashData.size(); i++)
        {
            ARROW_RETURN_NOT_OK(iBuilder.Append(std::any_cast<unsigned long int>(hashData.at(i).at(0))));
        }
        ARROW_RETURN_NOT_OK(iBuilder.Finish(&data_array));
        array_vector.push_back(data_array);

        for (int i = 1; i < hashData.at(0).size(); i++) {
            arrow::StringBuilder sBuilder;
            std::shared_ptr<arrow::Array> dataArray;

            for (int j = 0; j < hashData.size(); j++) {
                ARROW_RETURN_NOT_OK(sBuilder.Append(std::any_cast<std::string>(hashData.at(j).at(i))));
            }
            ARROW_RETURN_NOT_OK(sBuilder.Finish(&dataArray));
            array_vector.push_back(dataArray);
        }

        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, array_vector);

        // Write table to output file
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table.get(), chunkSize));
    }

    // Write remainder
    if (currentEndTime < endTime) {
        std::vector<std::vector<std::any>> hashData = this->generateHashData(currentEndTime, endTime, nColumns);

        // Store data into arrow::Table for output
        arrow::ArrayVector array_vector;

        std::shared_ptr<arrow::Array> data_array;
        arrow::UInt64Builder iBuilder;

        for (int i = 0; i < hashData.size(); i++)
        {
            ARROW_RETURN_NOT_OK(iBuilder.Append(std::any_cast<long unsigned int>(hashData.at(i).at(0))));
        }
        ARROW_RETURN_NOT_OK(iBuilder.Finish(&data_array));
        array_vector.push_back(data_array);

        for (int i = 1; i < hashData.at(0).size(); i++) {
            arrow::StringBuilder sBuilder;
            std::shared_ptr<arrow::Array> dataArray;

            for (int j = 0; j < hashData.size(); j++) {
                ARROW_RETURN_NOT_OK(sBuilder.Append(std::any_cast<std::string>(hashData.at(j).at(i))));
            }
            ARROW_RETURN_NOT_OK(sBuilder.Finish(&dataArray));
            array_vector.push_back(dataArray);
        }

        std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, array_vector);

        // Write table to output file
        ARROW_RETURN_NOT_OK(writer->WriteTable(*table.get(), chunkSize));
    }
    return writer->Close();
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
    std::string ALLOWED_DTYPES[] = {"utf8", "string", "int", "float", "double"};

    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(parquetFilename));

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

    // First column will always be timestamp
    // Placeholder for returning data
    int numChunks = table->column(0)->num_chunks();
    for (int i = 0; i < numChunks; i++) {
        auto timestampArray = std::dynamic_pointer_cast<arrow::UInt64Array>(table->column(0)->chunk(i));

        for (int rowIdx = 0; rowIdx < timestampArray->length(); rowIdx++) {
            outputVector->emplace_back(std::vector<std::any>{static_cast<unsigned long int>(timestampArray->Value(rowIdx))});
        }
    }

    // Check types of all columns
    for (int i = 1; i < table->num_columns(); i++) {
        std::string currentType = table->column(i)->type()->name();

        // Permitted types: string, int, float, double
        //  Note: Float will be treated as double
        if (std::find(std::begin(ALLOWED_DTYPES), std::end(ALLOWED_DTYPES), currentType) == std::end(ALLOWED_DTYPES)) {
            std::string typeErrorMsg = "Type: " + currentType + " is not supported!";
            return arrow::Status::TypeError(typeErrorMsg);
        }

        // Each array specifics
        int localIdx = 0;

        if (currentType == "utf8") {
            numChunks = table->column(i)->num_chunks();

            for (int j = 0; j < numChunks; j++) {
                auto stringArray = std::dynamic_pointer_cast<arrow::StringArray>(table->column(i)->chunk(j));

                for (int rowIdx = 0; rowIdx < stringArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<std::string>(stringArray->GetString(rowIdx + localIdx)));
                }
                localIdx += stringArray->length();
            }
        }
        else if (currentType == "float") {
            numChunks = table->column(i)->num_chunks();

            for (int j = 0; j < numChunks; j++) {
                auto floatArray = std::dynamic_pointer_cast<arrow::FloatArray>(table->column(i)->chunk(j));

                for (int rowIdx = 0; rowIdx < floatArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<double>(floatArray->Value(rowIdx + localIdx)));
                }
                localIdx += floatArray->length();
            }
        }
        else if (currentType == "double") {
            numChunks = table->column(i)->num_chunks();

            for (int j = 0; j < numChunks; j++) {
                auto doubleArray = std::dynamic_pointer_cast<arrow::DoubleArray>(table->column(i)->chunk(j));

                for (int rowIdx = 0; rowIdx < doubleArray->length(); rowIdx++) {
                    outputVector->at(rowIdx + localIdx).emplace_back(static_cast<double>(doubleArray->Value(rowIdx + localIdx)));
                }
                localIdx += doubleArray->length();
            }
        }
    }
    return arrow::Status::OK();
}