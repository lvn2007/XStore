#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include "zmq.hpp"
#include "zmq_addon.hpp"
#include "XStore.pb.h"

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
            std::cout << "[INFO] - Connected to " + fullAddr << std::endl;
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
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::createDB_REP fromServer;

            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::unaryQuery_REP unaryQuery(long int targetUnixTime, std::string targetDBName) {
            XStore_Proto::unaryQuery_REQ toServer;
            toServer.set_rtype(XStore_Proto::UNARYQUERY);
            toServer.set_timestamp(targetUnixTime);
            toServer.set_dbname(targetDBName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::unaryQuery_REP fromServer;
            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::rangeQuery_REP rangeQuery(unsigned long int startTime, unsigned long int endTime, std::string targetDBName) {
            XStore_Proto::rangeQuery_REQ toServer;
            toServer.set_rtype(XStore_Proto::RANGEQUERY);
            toServer.set_starttime(startTime);
            toServer.set_endtime(endTime);
            toServer.set_dbname(targetDBName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::rangeQuery_REP fromServer;
            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::batchQuery_REP batchQuery(std::vector<unsigned long int> timeList, std::string targetDBName) {
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
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::batchQuery_REP fromServer;
            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::unaryInsert_REP unaryInsert(long int insertUnixTime, XStore_Proto::rowData data, std::string dbName) {
            XStore_Proto::unaryInsert_REQ toServer;
            toServer.set_rtype(XStore_Proto::UNARYINSERT);
            toServer.set_timestamp(insertUnixTime);
            *toServer.mutable_data() = data;
            toServer.set_dbname(dbName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::unaryInsert_REP fromServer;

            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::rangeInsert_REP rangeInsert(XStore_Proto::rangeInsert_REQ &toServer, std::string dbName) {
            toServer.set_rtype(XStore_Proto::RANGEINSERT);
            toServer.set_dbname(dbName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::none);

            // RECEIVING
            std::vector<zmq::message_t> recv_msgs;
            zmq::message_t zFromServer;
            zmq::recv_multipart(sock, std::back_inserter(recv_msgs));

            // Received structs
            XStore_Proto::rangeInsert_REP fromServer;

            fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
            return fromServer;
        }

        XStore_Proto::removeDB_REP removeDB(std::string dbName) {
            XStore_Proto::removeDB_REQ toServer;
            toServer.set_rtype(XStore_Proto::REMOVEDB);
            toServer.set_dbname(dbName);

            // Config msg and send
            size_t byteSize = toServer.ByteSizeLong();
            zmq::message_t z_out(byteSize);
            toServer.SerializeToArray(z_out.data(), byteSize);
            sock.send(z_out, zmq::send_flags::none);

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