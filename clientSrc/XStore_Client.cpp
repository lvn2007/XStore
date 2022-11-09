#include <algorithm>
#include <cmath>
#include <fstream>
#include "XStore.pb.h"
#include "XStore_Client.h"

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

void writeCSV(std::string fName, std::string colName, std::vector<double> inputs) {
    // Create an output filestream object
    std::ofstream myFile(fName);

    // Send the column name to the stream
    myFile << colName << "\n";

    // Send data to the stream
    for(int i = 0; i < inputs.size(); ++i)
    {
        myFile << inputs.at(i) << "\n";
    }

    // Close the file
    myFile.close();
}

int main(int argc, char const *argv[])
{
    // Handle arguments
    if (argc < 5) {
        std::cout << "[ERROR - Arguments] -- Missing required (2) arguments" << std::endl;
        std::cout << " - Usages: XStore_Client <SERVER ADDR> <SERVER PORT> <MODE> <OP_NAME>" << std::endl;
        exit(1);
    }

    // Arguments parsing
    std::string SERVER_ADDR = argv[1];
    std::string SERVER_PORT = argv[2];
    std::string MODE = argv[3];
    std::string OP_NAME = argv[4];

    // Ensuring MODE & OP_NAME is all uppercase
    for (int i = 0; i < MODE.length(); i++) {
        MODE[i] = std::toupper(MODE[i]);
    }
    for (int i = 0; i < OP_NAME.length(); i++) {
        OP_NAME[i] = std::toupper(OP_NAME[i]);
    }

    // Init XStore Client object
    XStore_Client xstoreClient = XStore_Client(SERVER_ADDR, SERVER_PORT);

    if (MODE == "TEST") {
        std::cout << "+------------------------------------------+" << std::endl;
        std::cout << "|                   TEST                   |" << std::endl;
        if (OP_NAME == "BUILD") {
            std::cout << "|                   BUILD                  |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;
            std::cout <<  "[" + xstoreClient.getCurrDT() + "]" + "[INFO] [TEST]" + "Build ok" << std::endl;
            exit(0);
        }
        else if (OP_NAME == "SYS_INFO") {
            std::cout << "|                 SYS_INFO                 |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string dbName = "SYS_INFO";
            std::time_t dummyTime = 1482271200;
            std::string dummyData = "SYS INFO,TEST";

            // FIRST - CREATE_DB (DUMMY)
            std::string createDB_statusMessage = xstoreClient.createDB(dbName);

            // SECOND - UNARY_INSERT (DUMMY)
            std::string insertResult = xstoreClient.unaryInsert(dummyTime, dummyData, dbName);

            // THIRD - SYS_INFO
            sysInfo sysInfo_message = xstoreClient.infoRequest();
            std::cout << "Server address: " << sysInfo_message.serverAddr << std::endl;
            std::cout << "Server thread count: " <<sysInfo_message.numThreads << std::endl;

            std::cout << "DB on server: " << std::endl;
            for (int i = 0; i < sysInfo_message.db_Info.size(); i++) {
                std::cout << "- DB Name: " << sysInfo_message.db_Info.at(i).dbName << std::endl;
                std::cout << "  DB Resolution: " << sysInfo_message.db_Info.at(i).dbResolution << std::endl;
                std::cout << "  DB Size in GB: " << sysInfo_message.db_Info.at(i).dbSizeInGB << std::endl;
                std::cout << "  Filename: " << std::endl;
                for (int j = 0; j < sysInfo_message.db_Info.at(i).fileName.size(); j++) {
                    std::cout << "   - " << sysInfo_message.db_Info.at(i).fileName.at(j) << std::endl;
                }
            }
            std::cout << "Before bool checks" << std::endl;

            bool serverAddress_check = (sysInfo_message.serverAddr == (SERVER_ADDR + ":" +SERVER_PORT));
            bool threadCount_check = (sysInfo_message.numThreads > 0);
            bool dbName_check = (sysInfo_message.db_Info.at(0).dbName == dbName);
            bool dbResolution_check = (sysInfo_message.db_Info.at(0).dbResolution == "SECOND");
            bool dbSize_check = (sysInfo_message.db_Info.at(0).dbSizeInGB > 1);
            bool dbFilename_check = (sysInfo_message.db_Info.at(0).fileName.at(0) == "2016.XSTORE");

            std::cout << "After bool checks" << std::endl;

            // FINALLY - DELETE_DB (CLEANUP DUMMY)
            std::cout << "Before removeDB" << std::endl;
            std::string removeDBStatus = xstoreClient.removeDB(dbName);
            std::cout << "After removeDB" << std::endl;

            // VALIDATE
            if (serverAddress_check && threadCount_check && dbName_check && dbResolution_check && dbSize_check && dbFilename_check) {
                exit(0);
            }
            exit(1);
        }
        else if (OP_NAME == "CREATE_DB") {
            std::cout << "|                CREATE_DB                 |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string dbName = "CREATE_DB";

            // CASE 1: Create ok IF not yet created
            std::string createDB_statusMessage = xstoreClient.createDB(dbName);
            bool createOk = (createDB_statusMessage == ("[SUCCESS] -- DB with name: " + dbName + " is successfully created"));
            std::cout << createDB_statusMessage << std::endl;

            // CASE 2: Create error IF already created
            createDB_statusMessage = xstoreClient.createDB(dbName);
            bool createError = (createDB_statusMessage == ("[ERROR] -- DB with name: " + dbName + " is already existed"));
            std::cout << createDB_statusMessage << std::endl;

            // FINALLY - DELETE_DB (CLEANUP DUMMY)
            std::string removeDBStatus = xstoreClient.removeDB(dbName);

            // VALIDATE
            if (createOk && createError) {
                exit(0);
            }

            exit(1);
        }
        else if (OP_NAME == "UNARY_INSERT") {
            std::cout << "|               UNARY_INSERT               |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string dbName = "UNARY_INSERT";
            std::vector<std::time_t> timeList {1482271200, 1482271200000};
            std::vector<std::string> dataList {"SECOND,GRANULARITY", "MILLISECOND,GRANULARITY"};

            std::vector<bool> insertOk { false, false};

            for (int i = 0; i < timeList.size(); i++) {
                // FIRST - CREATE DB
                std::cout << "[INFO] - [DB: " << dbName << "] Create DB with name: " << dbName << std::endl;
                std::string createDB_message = xstoreClient.createDB(dbName);

                // SECOND - UNARY INSERT
                std::cout << "[INFO] - [DB: " << dbName << "] Insert at: " << timeList.at(i) << " | Data: " << dataList.at(i) << std::endl;
                std::string insertMessage = xstoreClient.unaryInsert(timeList.at(i), dataList.at(i), dbName);

                // THIRD - UNARY QUERY (To validate that insert is valid)
                std::string queryMessage = xstoreClient.unaryQuery(timeList.at(i), dbName);
                std::cout << "[INFO] - [DB: " << dbName << "] Query time: " << timeList.at(i) << " | Result: " << queryMessage << std::endl;

                // FINALLY - DELETE_DB (CLEANUP DUMMY)
                std::string removeDBStatus = xstoreClient.removeDB(dbName);
                std::cout << "[INFO] - [DB: " << dbName << "] Delete DB with name: " << dbName << std::endl;

                if (queryMessage == (std::to_string(timeList.at(i)) + "," + dataList.at(i))) {
                    insertOk[i] = true;
                }
            }

            // Ensuring all insert status is OK
            if (std::find(insertOk.begin(), insertOk.end(), true) != insertOk.end()) {
                exit(0);
            }
            exit(1);
        }

        else if (OP_NAME == "UNARY_QUERY") {
            std::cout << "|               UNARY_QUERY                |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            std::cout << xstoreClient.getCurrDT() << std::endl;

//            std::string queryResult = xstoreClient.unaryQuery(1483228802, "TSLAa");
//            std::cout << queryResult << std::endl;
        }
    }
    else if (MODE == "BENCH") {
        std::cout << "Bench mode" << std::endl;
    }

//    std::string uniqueID;
//    uniqueID = argv[1];
//
//    std::vector<double> us = { };
//
//    // Clock init
//    std::chrono::time_point<std::chrono::high_resolution_clock> start, end;
//    std::chrono::duration<double> diff;
//
//    int nIter = 1;
//    srand(time(NULL));
//    long int randTime;
//
//    zmq::context_t ctx;
//    zmq::socket_t sock(ctx, zmq::socket_type::req);
//
//    sock.connect(IP_ADDR);
//
//    for (int i = 0; i < nIter; i++) {
//        randTime = rand() % 1000 + 1;
//        long unixTime = 1451606400 + randTime;
//
//        // Start timer
//        start = std::chrono::high_resolution_clock::now();
//
//        // Unary REQ -- Routine
//        XStore_Proto::unaryQuery_REQ toServer;
//        toServer.set_rtype(XStore_Proto::UNARYREQUEST);
//        toServer.set_timestamp(1483228802);
//        toServer.set_dbname("tsla");

        // RANGE REQ -- Routine
//        xstoreProto::rangeQuery_REQ toServer;
//        toServer.set_rtype(xstoreProto::RANGEREQUEST);
//        toServer.set_starttime(1483228800);
//        toServer.set_endtime(1484338805);
//        toServer.set_dbname("tsla");

        // Info REQ -- Routine
//        xstoreProto::reqCMD toServer;
//        toServer.set_rtype(xstoreProto::INFOREQUEST);

        // Unary Insert -- Routine
//        xstoreProto::unaryInsert_REQ toServer;
//        toServer.set_rtype(xstoreProto::UNARYINSERT);
//        toServer.set_timestamp(1483228802);
//        toServer.set_data("Test");
//        toServer.set_dbname("tsla");

        // CreateDB -- Routine
//        xstoreProto::createDB_REQ toServer;
//        toServer.set_rtype(xstoreProto::CREATEDBREQUEST);
//        toServer.set_dbname("abc");
//        toServer.set_rowlength(8);

        // Config msg and send
//        size_t byteSize = toServer.ByteSizeLong();
//        zmq::message_t z_out(byteSize);
//        toServer.SerializeToArray(z_out.data(), byteSize);
//        sock.send(z_out, zmq::send_flags::none);

        // RECEIVING
//        std::vector<zmq::message_t> recv_msgs;
//        zmq::message_t zFromServer;
//        zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

        // Received structs
        //  UNARY REP
//        XStore_Proto::unaryQuery_REP fromServer;

        //  RANGE REP
//        xstoreProto::batchQuery_REP fromServer;

        //  INFO REP
//        xstoreProto::sysInfo_REP fromServer;

        //  UNARY INSERT REP
//        xstoreProto::unaryInsert_REP fromServer;

        //  CREATE DB REP
//        xstoreProto::createDB_REP fromServer;

//        fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());

        // End timer
//        end = std::chrono::high_resolution_clock::now();
//        diff = end - start;

        // OUTPUTS PRINTING
//        fromServer.PrintDebugString();

//        std::cout << "[Response] -- Timestamp: " << fromServer.timestamp() << std::endl;
//        std::cout << "[Response] -- Data: " << fromServer.dataset() << std::endl << std::endl;

//        us.push_back(std::chrono::duration<double, std::milli> (diff).count());
//    }

//    std::cout << nIter << " queries experiments" << std::endl;
//    std::cout << "--- EXECUTION TIME in [ms] ---" << std::endl;
//    std::cout << "  - Min: " << *std::min_element(us.begin(), us.end()) << std::endl;
//    std::cout << "  - Max: " << *std::max_element(us.begin(), us.end()) << std::endl;
//    std::cout << "  - Mean: " << meanCalc(us) << std::endl;
//    std::cout << "  - Stdev: " << stdCalc(us) << std::endl;

    // Write to CSV
//    writeCSV("zmq_1000_Protobuf-" + uniqueID + ".csv", "Time taken (ms)", us);

//    // Close socket
//    close(sock);
}
