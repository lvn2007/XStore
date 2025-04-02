#include <iostream>
#include "include/Networking/XStore_zmqServer.h"
#include "include/XStore/XStore.h"
#include <cmath>
#include <variant>
//#include "include/Compression/zstdpp.hpp"
#include "include/Compression/shoco.h"
#include <fstream>
#include <bitset>

// Clock init
std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
std::chrono::duration<double> diff;

struct benchData {
    double usec;
    size_t OGLength;
    size_t compLength;

    benchData(double usecTimeTaken, size_t originalLength, size_t compressedLength) {
        usec = usecTimeTaken;
        OGLength = originalLength;
        compLength = compressedLength;
    }

    std::string toString() {
        return std::to_string(usec) + "," + std::to_string(OGLength) + "," + std::to_string(compLength) + "\n";
    }
};

benchData shoco(std::string &inputData, bool benchComp) {
    char output[inputData.length() + 1];

    if (benchComp) {
        // Clear caches
        system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");

        // Start timer
        start = std::chrono::high_resolution_clock::now();

        // --- COMPRESS
        size_t compressLength = shoco_compress(inputData.data(), 0, output, inputData.length() + 1);

        // End timer
        end = std::chrono::high_resolution_clock::now();
        diff = end - start;
//        std::cout << "Shoco compress: " << std::chrono::duration<double, std::micro> (diff).count() << " (usec)" << std::endl;
//        std::cout << "Original size: " << inputData.length() << std::endl;
//        std::cout << "Shoco compressed size: " << strlen(output) << std::endl;
    }
    else {
        // --- COMPRESS
        size_t compressLength = shoco_compress(inputData.data(), 0, output, inputData.length() + 1);

        // --- DE-COMPRESS
        char decomp[inputData.length() + 1];

        // Clear caches
        system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");

        // Start timer
        start = std::chrono::high_resolution_clock::now();
        size_t decompressLength = shoco_decompress(output, inputData.length() + 1, &decomp[0], inputData.length() + 1);

        // End timer
        end = std::chrono::high_resolution_clock::now();

        diff = end - start;
//        std::cout << "Shoco de-compress: " << std::chrono::duration<double, std::micro> (diff).count() << " (usec)" << std::endl;
//        XStore_Proto::rowData rData;
//        rData.ParseFromArray(decomp, strlen(decomp));
//        rData.PrintDebugString();
//        XStore_Proto::multipleRowData rowData;
//        rowData.ParseFromArray(decomp, strlen(decomp));
//        rowData.PrintDebugString();
    }
    // Assemble benchData
    int trueSize = (sizeof(output) / sizeof(char)) - 1;
    std::string toString = std::string(output);
    std::cout << "String len: " << toString.length() << std::endl;
    std::cout << sizeof(output) << std::endl;
    benchData thisBench(std::chrono::duration<double, std::micro> (diff).count(), inputData.length(), trueSize);
    return thisBench;
}

//benchData zstd(std::string &inputData, bool benchComp) {
//    std::string output;
//
//    if (benchComp) {
//        // Clear caches
//        system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");
//
//        // Start timer
//        start = std::chrono::high_resolution_clock::now();
//
//        // --- COMPRESS
//        zstdpp::buff_compress(inputData, output, 22);
//
//        // End timer
//        end = std::chrono::high_resolution_clock::now();
//
//        diff = end - start;
////        std::cout << "Shoco compress: " << std::chrono::duration<double, std::micro> (diff).count() << " (usec)" << std::endl;
////        std::cout << "Original size: " << inputData.length() << std::endl;
////        std::cout << "ZSTD compressed size: " << output.length() << std::endl;
//    }
//    else {
//        // --- COMPRESS
//        zstdpp::buff_compress(inputData, output, 22);
//
//        // --- DE-COMPRESS
//        std::string decomp;
//
//        // Clear caches
//        system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");
//
//        // Start timer
//        start = std::chrono::high_resolution_clock::now();
//        zstdpp::buff_decompress(output, decomp);
//
//        // End timer
//        end = std::chrono::high_resolution_clock::now();
//
//        diff = end - start;
////        std::cout << "ZSTD de-compress: " << std::chrono::duration<double, std::micro> (diff).count() << " (usec)" << std::endl;
//    }
//    // Assemble benchData
//    benchData thisBench(std::chrono::duration<double, std::micro> (diff).count(), inputData.length(), output.length());
//    return thisBench;
//}

// for string delimiter
std::vector<std::string> split(std::string s, std::string delimiter) {
    size_t pos_start = 0, pos_end, delim_len = delimiter.length();
    std::string token;
    std::vector<std::string> res;

    while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
        token = s.substr (pos_start, pos_end - pos_start);
        pos_start = pos_end + delim_len;
        res.push_back (token);
    }

    res.push_back (s.substr (pos_start));
    return res;
}

int main(int argc, char *argv[]) {
    // Handle arguments
    if (argc < 4) {
        std::cout << "[ERROR] [BOOT] - Missing required (2) arguments" << std::endl;
        std::cout << "[ERROR] [BOOT] - Usages: XStore <BIND ADDR> <BIND PORT> <THREAD COUNT>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string BIND_ADDR = argv[1];
    std::string BIND_PORT = argv[2];
    int THREAD_COUNT = atoi(argv[3]);

    // ----DEV AREA
    // Instantiate XStore obj
//    XStore myStore;
//
//    system("free > /dev/null && sync > /dev/null && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches' && free > /dev/null");
//    // AGG
//    XStore_Proto::avg_REP aggRep;
//    start = std::chrono::high_resolution_clock::now();
//    myStore.avgAggregate(946684800, 946684800 + 4, "TSLA", 2, &aggRep);
//    end  = std::chrono::high_resolution_clock::now();
//    aggRep.PrintDebugString();
//    std::cout << "Took: " << std::chrono::duration<double, std::milli> (end-start).count() << " (msec)" << std::endl;
//    exit(0);

//    XStore_Proto::rowData firstRow;
//    XStore_Proto::fData *field1 = firstRow.add_fielddata();
//    field1->set_floatdata(4689.89);
//    XStore_Proto::fData *field2 = firstRow.add_fielddata();
//    field2->set_doubledata(0.053417);
//    XStore_Proto::fData *field3 = firstRow.add_fielddata();
//    field3->set_int32data(61458);
//    XStore_Proto::fData *field4 = firstRow.add_fielddata();
//    field4->set_int32data(69180);
//    XStore_Proto::fData *field5 = firstRow.add_fielddata();
//    field5->set_int32data(69180);
//    XStore_Proto::fData *field6 = firstRow.add_fielddata();
//    field6->set_booldata(true);

//    std::string input = firstRow.SerializeAsString();

//    std::ifstream file;
//    file.open("/home/ubuntu/candlesticks-S_cleaned.csv");
//    std::vector<std::string> splittedString;
//    std::string serializedString;
//    int count = 0;
//
//    // Write to file
//    std::ofstream outFile;
////    outFile.open("shoco_compress.csv");
//    outFile << "Time Taken (usec),Original Length,Compressed Length\n";
//
//    XStore_Proto::multipleRowData multiRows;
//    for (std::string line; std::getline(file, line); ) {
//        size_t pos;
////        std::cout << line << std::endl;
//        splittedString = split(line, ",");
//
//        XStore_Proto::rowData *oneRow;
//        oneRow = multiRows.add_rdata();
//        XStore_Proto::fData *field1 = oneRow->add_fielddata();
//        field1->set_floatdata(std::stof(splittedString.at(1)));
//        XStore_Proto::fData *field2 = oneRow->add_fielddata();
//        field2->set_floatdata(std::stof(splittedString.at(2)));
//        XStore_Proto::fData *field3 = oneRow->add_fielddata();
//        field3->set_floatdata(std::stof(splittedString.at(3)));
//        XStore_Proto::fData *field4 = oneRow->add_fielddata();
//        field4->set_floatdata(std::stof(splittedString.at(4)));
//        XStore_Proto::fData *field5 = oneRow->add_fielddata();
//        field5->set_doubledata(std::stod(splittedString.at(5)));
//
//        serializedString = oneRow->SerializeAsString();
//
//        // Shoco -- Compress
//        benchData bData = shoco(serializedString, true);
//
//        // ZSTD -- Compress
////        benchData bData = zstd(serializedString, false);
//
////        std::cout << "Time taken: " << bData.usec << std::endl;
////        std::cout << "OG Length: " << bData.OGLength << std::endl;
////        std::cout << "Compressed Length: " << bData.compLength << std::endl;
////        std::cout << "----------------------------" << std::endl;
////        outFile << bData.toString();
//
//        count++;
//        if (count >= 2) {
//            break;
//        }
//    }

//    std::vector<int> iterRange {128000};
//    for (int i = 0; i < iterRange.size(); i++) {
//        for (int j = 0; j < 10; j++) {
//            for (std::string line; std::getline(file, line); ) {
//                splittedString = split(line, ",");
//                XStore_Proto::rowData *oneRow;
//                oneRow = multiRows.add_rdata();
//                XStore_Proto::fData *field1 = oneRow->add_fielddata();
//                field1->set_floatdata(std::stof(splittedString.at(1)));
//                XStore_Proto::fData *field2 = oneRow->add_fielddata();
//                field2->set_floatdata(std::stof(splittedString.at(2)));
//                XStore_Proto::fData *field3 = oneRow->add_fielddata();
//                field3->set_floatdata(std::stof(splittedString.at(3)));
//                XStore_Proto::fData *field4 = oneRow->add_fielddata();
//                field4->set_floatdata(std::stof(splittedString.at(4)));
//                XStore_Proto::fData *field5 = oneRow->add_fielddata();
//                field5->set_doubledata(std::stod(splittedString.at(5)));
//
//                count++;
//                if (count >= iterRange.at(i)) {
//                    break;
//                }
//            }
//
//            serializedString = multiRows.SerializeAsString();
//
//            // Shoco -- Compress
//            benchData bData = shoco(serializedString, false);
//
//            // ZSTD -- Compress
////            benchData bData = zstd(serializedString, false);
//
////        std::cout << "Time taken: " << bData.usec << std::endl;
////        std::cout << "OG Length: " << bData.OGLength << std::endl;
////        std::cout << "Compressed Length: " << bData.compLength << std::endl;
////        std::cout << "----------------------------" << std::endl;
//        outFile << bData.toString();
//        }
//    }

//    multiRows.PrintDebugString();

//    outFile.close();

//    XStore_Proto::rowData firstHash;
//    XStore_Proto::fData *field1 = firstHash.add_fielddata();
//    field1->set_stringdata("aaaabbbb04b73bc0fcdefad9fd3ed7ab");
//
//    firstHash.PrintDebugString();
//
//    std::string input = firstHash.SerializeAsString();
//
//    shoco(input, true);

    // UNARY QUERY
//    XStore_Proto::unaryQuery_REP unary_query_rep = myStore.unaryQuery(1451606401, "TSLA");
//    unary_query_rep.PrintDebugString();

//    XStore_Proto::batchInsert_REQ batch_insert_req;
//    batch_insert_req.set_dbname("TSLA");

//    for (int i = 1; i < 5; i++) {
//        XStore_Proto::batchInsert_DATA *batch_insert_data = batch_insert_req.add_batchinsert_data();
//        batch_insert_data->set_timestamp(1483228798 + i);
//
//        // Assemble rowData
//        XStore_Proto::rowData oneRow;
//        XStore_Proto::fieldData *field_data = oneRow.add_fdata();
//        field_data->set_stringdata("Test " + std::to_string(1483228798 + i));
//        *batch_insert_data->mutable_data() = oneRow;
//    }
//    batch_insert_req.PrintDebugString();

//    XStore_Proto::batchInsert_REP batch_insert_rep = myStore.batchInsert(&batch_insert_req);
//    batch_insert_rep.PrintDebugString();

//    XStore_Proto::batchQuery_REQ batch_query_req;
//    batch_query_req.set_dbname("TSLA");
//    for (int i = 0; i < 5; i++) {
//        batch_query_req.add_timestamp(1483228798 + i);
//    }

//    XStore_Proto::batchQuery_REP batch_query_rep = myStore.batchQuery(&batch_query_req);
//    batch_query_rep.PrintDebugString();

    // Start timer
//    start = std::chrono::high_resolution_clock::now();

    // Unary Insert
//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert(1451606401, "Helo1451606401", "TSLA");
//    unary_insert_rep.PrintDebugString();
//    XStore_Proto::unaryInsert_REP unary_insert_rep2 = myStore.unaryInsert(1451606410, "Helo1451606410", "TSLA");

//    XStore_Proto::unaryInsert_REP unary_insert_rep = myStore.unaryInsert(1451606400000, "Helo", "TSLA");
//    XStore_Proto::unaryInsert_REP unary_insert_rep1 = myStore.unaryInsert(1451636401233, "Helo123", "TSLA");
//    XStore_Proto::unaryInsert_REP unary_insert_rep2 = myStore.unaryInsert(1451606441897, "Helo456", "TSLA");
//    unary_insert_rep.PrintDebugString();
//

    // RANGE QUERY
//    XStore_Proto::unaryQuery_REP range_query_rep = myStore.unaryQuery(1451606401, "TSLA");
//    range_query_rep.PrintDebugString();
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1451606400000, 1451606400002, "tsla");
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1482271200, 1482271204, "tsla");
//    XStore_Proto::rangeQuery_REP range_query_rep = myStore.rangeQuery(1483228798, 1483928808, "tsla");
//    range_query_rep.PrintDebugString();

    // End timer
//    end = std::chrono::high_resolution_clock::now();
//    diff = end - start;

//    std::cout << "Took: " << std::chrono::duration<double, std::milli> (diff).count() << " (usec)" << std::endl;
//    batch_insert_rep.PrintDebugString();

    // ----DEV AREA
    spdlog::level::level_enum CONSOLE_LEVEL = spdlog::level::warn;
    spdlog::level::level_enum FILE_LEVEL = spdlog::level::trace;
    unsigned short int flushInterval = 10;
    Logger logger = Logger(CONSOLE_LEVEL, FILE_LEVEL, flushInterval);

    // Spin ZMQ Server
    XStore_zmqServer XStoreZmqServer(THREAD_COUNT);
    XStoreZmqServer.startServer(BIND_ADDR, BIND_PORT);
}
