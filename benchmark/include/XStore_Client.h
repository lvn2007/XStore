#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include "zmq.hpp"
#include "zmq_addon.hpp"
#include "XStore.pb.h"
#include <any>

struct dbInfo {
    std::string dbName;
    XStore_Proto::timeUnit dbResolution;
    float totalSizeInByte;
    std::vector<std::string> fileName;
};

struct sysInfo {
    std::string serverAddr;
    int numThreads;
    std::vector<dbInfo> db_Info;
};

class XStore_Client {
    private:
        zmq::context_t ctx;
        zmq::socket_t sock { ctx, zmq::socket_type::req };

    public:
        void connect(std::string SERVER_ADDR, std::string SERVER_PORT) {
            std::string fullAddr = "tcp://" + SERVER_ADDR + ":" + SERVER_PORT;
            sock.connect(fullAddr);
        }

        void setFieldData(XStore_Proto::fData *field, std::any *currentMetric) {
            if (currentMetric->type() == typeid(std::string)) {
                field->set_stringdata(std::any_cast<std::string>(*currentMetric));
                return;
            }
            else if (currentMetric->type() == typeid(int)) {
                field->set_int32data(std::any_cast<int>(*currentMetric));
                return;
            }
            else if (currentMetric->type() == typeid(double)) {
                field->set_doubledata(std::any_cast<double>(*currentMetric));
                return;
            }
        }

//  std::string getCurrDT() {
//      std::stringstream formattedTime;
//      std::time_t timeNow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
//      formattedTime << std::put_time(std::localtime(&timeNow), "%b %d, %Y %r");
//      std::string toReturn = formattedTime.str();
//      return toReturn;
//  }

//  sysInfo infoRequest() {
//      XStore_Proto::reqCMD toServer;
//      toServer.set_rtype(XStore_Proto::INFO);
//
//      // Config msg and send
//      size_t byteSize = toServer.ByteSizeLong();
//      zmq::message_t z_out(byteSize);
//      toServer.SerializeToArray(z_out.data(), byteSize);
//      sock.send(z_out, zmq::send_flags::none);
//
//      // RECEIVING
//      std::vector<zmq::message_t> recv_msgs;
//      zmq::message_t zFromServer;
//      zmq::recv_multipart(sock, std::back_inserter(recv_msgs));
//
//      // Received structs
//      XStore_Proto::sysInfo_REP fromServer;
//      sysInfo toReturn;
//
//      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
//
//      // Put return data from server to struct
//      toReturn.serverAddr = fromServer.serveraddr();
//      toReturn.numThreads = fromServer.threadcount();
//
//      // Loop through info of all DB exists in server
//      for (int i = 0; i < fromServer.dbinfo_size(); i++) {
//          dbInfo currDB;
//          currDB.dbName = fromServer.dbinfo(i).dbname();
//          currDB.dbResolution = fromServer.dbinfo(i).dbresolution();
//          currDB.totalSizeInByte = fromServer.dbinfo(i).totalsizeinbyte();
//
//          // Loop through all file name of a given DB
//          for (int j = 0; j < fromServer.dbinfo(i).filename_size(); j++) {
//              currDB.fileName.push_back(fromServer.dbinfo(i).filename(j));
//          }
//          toReturn.db_Info.push_back(currDB);
//      }
//
//      return toReturn;
//  }

        XStore_Proto::createDB_REP createDB(std::string dbName, XStore_Proto::timeUnit &timeResolution, short int &rowLength) {
            XStore_Proto::createDB_REQ toServer;
            toServer.set_rtype(XStore_Proto::CREATEDB);
            toServer.set_dbname(dbName);
            toServer.set_timeresolution(timeResolution);
            toServer.set_rowlength(rowLength);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::createDB_REP fromServer;

            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        void unaryQuery(long int targetUnixTime, std::string targetDBName, XStore_Proto::unaryQuery_REP *returnData) {
            XStore_Proto::unaryQuery_REQ toServer;
            toServer.set_rtype(XStore_Proto::UNARYQUERY);
            toServer.set_timestamp(targetUnixTime);
            toServer.set_dbname(targetDBName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Query data
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void rangeQuery(unsigned long int startTime, unsigned long int endTime, std::string targetDBName, XStore_Proto::rangeQuery_REP *returnData) {
            XStore_Proto::rangeQuery_REQ toServer;
            toServer.set_rtype(XStore_Proto::RANGEQUERY);
            toServer.set_starttime(startTime);
            toServer.set_endtime(endTime);
            toServer.set_dbname(targetDBName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Query data
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void batchQuery(std::vector<unsigned long int> timeList, std::string targetDBName, XStore_Proto::batchQuery_REP *returnData) {
            XStore_Proto::batchQuery_REQ toServer;
            toServer.set_rtype(XStore_Proto::BATCHQUERY);
            toServer.set_dbname(targetDBName);

            // Add time to Protobuf msg
            for (int i = 0; i < timeList.size(); i++) {
                toServer.add_timestamp(timeList.at(i));
            }

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Query data
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void unaryInsert(long int insertUnixTime, std::vector<std::any> *insertData, std::string dbName, XStore_Proto::unaryInsert_REP *returnData) {
            XStore_Proto::unaryInsert_REQ toServer;
            toServer.set_rtype(XStore_Proto::UNARYINSERT);
            toServer.set_timestamp(insertUnixTime);
            toServer.set_dbname(dbName);

            // Row data
            XStore_Proto::rowData rowData;

            // Loop through each item in a row -> Identify type and build rowData
            //  Skipping first element (timestamp) in a row
            for (int i = 1; i < insertData->size(); i++) {
                XStore_Proto::fData *field = rowData.add_fielddata();
                setFieldData(field, &insertData->at(i));
            }

            // Add row data into Proto UNARY INSERT msg
            *toServer.mutable_data() = rowData;

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert result
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void unaryInsert(XStore_Proto::unaryInsert_REQ *insertData, XStore_Proto::unaryInsert_REP *returnData) {
            // Config msg and send
            size_t byteSize = insertData->ByteSizeLong();
            zmq::message_t z_out(byteSize);
            insertData->SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert result
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void rangeInsert(std::vector<std::vector<std::any>> *insertData, std::string dbName, XStore_Proto::rangeInsert_REP *returnData) {
            XStore_Proto::rangeInsert_REQ toServer;
            toServer.set_rtype(XStore_Proto::RANGEINSERT);
            toServer.set_dbname(dbName);

            // Loop through each row
            for (int i = 0; i < insertData->size(); i++) {
                XStore_Proto::rangeInsert_DATA *rangeInsertData = toServer.add_rangeinsert_data();

                // Build timestamp for current row of data
                rangeInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(i).at(0)));

                // Placeholder for rowData
                XStore_Proto::rowData rowData;

                // Loop through each "column" in a row
                for (int j = 1; j < insertData->at(i).size(); j++) {
                    XStore_Proto::fData *field = rowData.add_fielddata();
                    setFieldData(field, &insertData->at(i).at(j));
                }

                // Merge message
                rangeInsertData->mutable_data()->MergeFrom(rowData);
            }

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert results
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void rangeInsert(XStore_Proto::rangeInsert_REQ *insertData, XStore_Proto::rangeInsert_REP *returnData) {
            // Config msg and send
            size_t byteSize = insertData->ByteSizeLong();
            zmq::message_t z_out(byteSize);
            insertData->SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert results
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void batchInsert(std::vector<std::vector<std::any>> *insertData, std::string dbName, XStore_Proto::batchInsert_REP *returnData) {
            XStore_Proto::batchInsert_REQ toServer;
            toServer.set_rtype(XStore_Proto::BATCHINSERT);
            toServer.set_dbname(dbName);

            // Loop through each row
            for (int i = 0; i < insertData->size(); i++) {
                XStore_Proto::batchInsert_DATA *batchInsertData = toServer.add_batchinsert_data();

                // Build timestamp for current row of data
                batchInsertData->set_timestamp(std::any_cast<unsigned long int>(insertData->at(i).at(0)));

                // Placeholder for rowData
                XStore_Proto::rowData rowData;

                // Loop through each "column" in a row
                for (int j = 1; j < insertData->at(i).size(); j++) {
                    XStore_Proto::fData *field = rowData.add_fielddata();
                    setFieldData(field, &insertData->at(i).at(j));
                }

                // Merge message
                batchInsertData->mutable_data()->MergeFrom(rowData);
            }

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert results
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        void batchInsert(XStore_Proto::batchInsert_REQ *insertData, XStore_Proto::batchInsert_REP *returnData) {
            // Config msg and send
            size_t byteSize = insertData->ByteSizeLong();
            zmq::message_t z_out(byteSize);
            insertData->SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Insert results
            returnData->ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
        }

        XStore_Proto::removeDB_REP removeDB(std::string dbName) {
            XStore_Proto::removeDB_REQ toServer;
            toServer.set_rtype(XStore_Proto::REMOVEDB);
            toServer.set_dbname(dbName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::dontwait);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::removeDB_REP fromServer;

            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_