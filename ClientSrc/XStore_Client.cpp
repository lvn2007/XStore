#include <algorithm>
#include <cmath>
#include <fstream>
#include "XStore.pb.h"
#include "XStore_Client.h"

double MeanCalc(std::vector<double> inputVector) {
    double TotalSum = 0;

    for (int i = 0; i < inputVector.size(); i++) {
        TotalSum += inputVector.at(i);
    }

    return TotalSum / inputVector.size();
}

double StdCalc(std::vector<double> inputVector) {
    double TotalSum = 0;
    double Mean = 0;
    double Std = 0;

    for (int i = 0; i < inputVector.size(); i++) {
        TotalSum += inputVector.at(i);
    }

    Mean = TotalSum / inputVector.size();

    for (int i = 0; i < inputVector.size(); i++) {
        Std += pow(inputVector.at(i) - Mean, 2);
    }

    return sqrt(Std / inputVector.size());
}

double GetMin(std::vector<double> inputVector) {
    double MinVal = inputVector.at(0);

    for (int i = 1; i < inputVector.size(); i++) {
        if (inputVector.at(i) < MinVal) {
            MinVal = inputVector.at(0);
        }
    }

    return MinVal;
}

double GetMax(std::vector<double> inputVector) {
    double MaxVal = inputVector.at(0);

    for (int i = 1; i < inputVector.size(); i++) {
        if (inputVector.at(i) > MaxVal) {
            MaxVal = inputVector.at(i);
        }
    }
    return MaxVal;
}

void WriteCSV(std::string fName, std::string colName, std::vector<double> inputs) {
    // Create an output filestream object
    std::ofstream MyFile(fName);

    // Send the column name to the stream
    MyFile << colName << "\n";

    // Send data to the stream
    for(int i = 0; i < inputs.size(); ++i)
    {
        MyFile << inputs.at(i) << "\n";
    }

    // Close the file
    MyFile.close();
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
    XStore_Client XStoreClient = XStore_Client(SERVER_ADDR, SERVER_PORT);

    if (MODE == "TEST") {
        std::cout << "+------------------------------------------+" << std::endl;
        std::cout << "|                   TEST                   |" << std::endl;

        if (OP_NAME == "BUILD") {
            std::cout << "|                   BUILD                  |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;
            std::cout <<  "[" + XStoreClient.GetCurrDT() + "] [INFO] [TEST]" + " Build ok" << std::endl;
            exit(0);
        }

        else if (OP_NAME == "SYS_INFO") {
            std::cout << "|                 SYS_INFO                 |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string DbName = "SYS_INFO";
            std::time_t DummyTime = 1482271200;
            XStore_Proto::TimeUnit TimeResolution = XStore_Proto::SECOND;
            XStore_Proto::RowData DummyData;
            XStore_Proto::FData *FieldData = DummyData.add_fielddata();
            FieldData->set_stringdata("SYS INFO,TEST");

            // FIRST - CREATE_DB (DUMMY)
            std::string CreateDbStatusMessage = XStoreClient.CreateDB(DbName, TimeResolution, 128);

            // SECOND - UNARY_INSERT (DUMMY)
            std::string InsertResult = XStoreClient.UnaryInsert(DummyTime, DummyData, DbName);

            // THIRD - SYS_INFO
            SysInfo SysInfoMessage = XStoreClient.InfoRequest();
            std::cout << "Server address: " << SysInfoMessage.SERVER_ADDR << std::endl;
            std::cout << "Server thread count: " <<SysInfoMessage.NumThreads << std::endl;

            std::cout << "DB on server: " << std::endl;
            for (int i = 0; i < SysInfoMessage.DbInfoList.size(); i++) {
                std::cout << "- DB Name: " << SysInfoMessage.DbInfoList.at(i).DbName << std::endl;
                std::cout << "  DB Resolution: " << SysInfoMessage.DbInfoList.at(i).DbResolution << std::endl;
                std::cout << "  Total size in Byte: " << SysInfoMessage.DbInfoList.at(i).TotalSizeInByte << std::endl;
            }

            // Validate tests PASSED
            assert(SysInfoMessage.NumThreads > 0);
            assert(SysInfoMessage.DbInfoList.at(0).DbName == DbName);
            assert(SysInfoMessage.DbInfoList.at(0).DbResolution == XStore_Proto::SECOND);
            assert(SysInfoMessage.DbInfoList.at(0).TotalSizeInByte > 1);
            assert(SysInfoMessage.DbInfoList.at(0).FileName.at(0) == "2016.XSTORE");

            // FINALLY - DELETE_DB (CLEANUP DUMMY)
            std::string RemoveDbStatus = XStoreClient.RemoveDB(DbName);
            assert(RemoveDbStatus == "[SUCCESS] [REMOVE DB] [DB: SYS_INFO] - Removed DB with name: SYS_INFO");
        }

        else if (OP_NAME == "CREATE_DB") {
            std::cout << "|                CREATE_DB                 |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string DbName = "CREATE_DB";
            XStore_Proto::TimeUnit TimeResolution = XStore_Proto::SECOND;
            short int RowLength = 128;

            // CASE 1: Create error IF rowLength < 4
            std::string CreateDbStatusMessage = XStoreClient.CreateDB(DbName, TimeResolution, 3);
            assert(CreateDbStatusMessage == ("[ERROR] [CREATE DB] [DB: " + DbName + "] - Row length must be >= 4"));
            std::cout << CreateDbStatusMessage << std::endl;

            // CASE 2: Create ok IF not yet created
            CreateDbStatusMessage = XStoreClient.CreateDB(DbName, TimeResolution, RowLength);
            assert(CreateDbStatusMessage == ("[SUCCESS] [CREATE DB] [DB: " + DbName + "] - Created DB"));
            std::cout << CreateDbStatusMessage << std::endl;

            // CASE 3: Create error IF already created
            CreateDbStatusMessage = XStoreClient.CreateDB(DbName, TimeResolution, RowLength);
            assert(CreateDbStatusMessage == ("[ERROR] [CREATE DB] [DB: " + DbName + "] - Already existed"));
            std::cout << CreateDbStatusMessage << std::endl;

            // FINALLY - DELETE_DB (CLEANUP DUMMY)
            std::string RemoveDbStatus = XStoreClient.RemoveDB(DbName);
            assert(RemoveDbStatus == "[SUCCESS] [REMOVE DB] [DB: CREATE_DB] - Removed DB with name: CREATE_DB");
        }

        else if (OP_NAME == "UNARY_OPS") {
            std::cout << "|                 UNARY_OPS                |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            // Const
            std::string DbName = "UNARY_OPS";
            std::vector<std::time_t> TimeList {1482271200};
            std::vector<XStore_Proto::TimeUnit> TimeResolution {XStore_Proto::SECOND, XStore_Proto::MILLISECOND};

            // FIRST data
            XStore_Proto::RowData FirstRow;
            XStore_Proto::FData *FieldData = FirstRow.add_fielddata();
            FieldData->set_stringdata("SECOND,GRANULARITY");

            // SECOND data
            XStore_Proto::RowData SecondRow;
            XStore_Proto::FData *SData = FirstRow.add_fielddata();
            SData->set_stringdata("MILLISECOND,GRANULARITY");

            std::vector<XStore_Proto::RowData> DataList {FirstRow, SecondRow};

            for (int i = 0; i < TimeList.size(); i++) {
                std::cout << "Granularity: " << XStore_Proto::TimeUnit_Name(TimeResolution.at(i)) << std::endl;
                // FIRST - CREATE DB
                std::string CreateDbMessage = XStoreClient.CreateDB(DbName, TimeResolution.at(i), 128);
                assert(CreateDbMessage == ("[SUCCESS] [CREATE DB] [DB: UNARY_OPS] - Created DB"));
                std::cout << CreateDbMessage << std::endl;

                // SECOND - UNARY INSERT
                std::string InsertMessage = XStoreClient.UnaryInsert(TimeList.at(i), DataList.at(i), DbName);
                assert(InsertMessage == ("[SUCCESS] [UNARY INSERT] [DB: UNARY_OPS] - Inserted " + std::to_string(TimeList.at(i)) + " to 2016.XSTORE"));
                std::cout << InsertMessage << std::endl;

                // THIRD - UNARY QUERY (To validate that insert is valid)
                std::string QueryMessage = XStoreClient.UnaryQuery(TimeList.at(i), DbName);
                assert(QueryMessage == (std::to_string(TimeList.at(i)) + "," + DataList.at(i).fielddata(0).stringdata()));
                std::cout << "[SUCCESS] [UNARY QUERY] [DB: " << DbName << "] - Query time: " << TimeList.at(i) << " | Result: " << QueryMessage << std::endl;

                // FINALLY - DELETE_DB (CLEANUP DUMMY)
                std::string RemoveDbStatus = XStoreClient.RemoveDB(DbName);
                assert(RemoveDbStatus == ("[SUCCESS] [REMOVE DB] [DB: UNARY_OPS] - Removed DB with name: " + DbName));
                std::cout << RemoveDbStatus << std::endl;
            }
        }

        else if (OP_NAME == "BATCH_OPS") {
            std::cout << "|               UNARY_QUERY                |" << std::endl;
            std::cout << "+------------------------------------------+\n" << std::endl;

            std::cout << XStoreClient.GetCurrDT() << std::endl;

//            std::string queryResult = xstoreClient.UnaryQuery(1483228802, "TSLAa");
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
//        XStore_Proto::UnaryQuery_REQ toServer;
//        toServer.set_rtype(XStore_Proto::UNARYREQUEST);
//        toServer.set_timestamp(1483228802);
//        toServer.set_dbname("tsla");

        // RANGE REQ -- Routine
//        xstoreProto::RangeQuery_REQ toServer;
//        toServer.set_rtype(xstoreProto::RANGEREQUEST);
//        toServer.set_starttime(1483228800);
//        toServer.set_endtime(1484338805);
//        toServer.set_dbname("tsla");

        // Info REQ -- Routine
//        xstoreProto::ReqCMD toServer;
//        toServer.set_rtype(xstoreProto::INFOREQUEST);

        // Unary Insert -- Routine
//        xstoreProto::UnaryInsert_REQ toServer;
//        toServer.set_rtype(xstoreProto::UNARYINSERT);
//        toServer.set_timestamp(1483228802);
//        toServer.set_data("Test");
//        toServer.set_dbname("tsla");

        // CreateDB -- Routine
//        xstoreProto::CreateDB_REQ toServer;
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
//        XStore_Proto::UnaryQuery_REP fromServer;

        //  RANGE REP
//        xstoreProto::BatchQuery_REP fromServer;

        //  INFO REP
//        xstoreProto::SysInfo_REP fromServer;

        //  UNARY INSERT REP
//        xstoreProto::UnaryInsert_REP fromServer;

        //  CREATE DB REP
//        xstoreProto::CreateDB_REP fromServer;

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
//    std::cout << "  - Mean: " << MeanCalc(us) << std::endl;
//    std::cout << "  - Stdev: " << StdCalc(us) << std::endl;

    // Write to CSV
//    WriteCSV("zmq_1000_Protobuf-" + uniqueID + ".csv", "Time taken (ms)", us);

//    // Close socket
//    close(sock);
}
