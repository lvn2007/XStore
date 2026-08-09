#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include "zmq.hpp"
#include "zmq_addon.hpp"
#include "XStore.pb.h"

struct DbInfo {
  std::string dbName;
  XStore_Proto::TimeUnit dbResolution;
  float totalSizeInByte;
  std::vector<std::string> fileName;
};

struct SysInfo {
  std::string serverAddr;
  int numThreads;
  std::vector<DbInfo> db_Info;
};

class XStore_Client {
 private:
  zmq::context_t ctx;
  zmq::socket_t sock { ctx, zmq::socket_type::req };
 public:
  XStore_Client(std::string SERVER_ADDR, std::string SERVER_PORT) {
      std::string fullAddr = "tcp://" + SERVER_ADDR + ":" + SERVER_PORT;
      sock.connect(fullAddr);
  }

  std::string GetCurrDT() {
      std::stringstream formattedTime;
      std::time_t timeNow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      formattedTime << std::put_time(std::localtime(&timeNow), "%b %d, %Y %r");
      std::string toReturn = formattedTime.str();
      return toReturn;
  }

  SysInfo InfoRequest() {
      XStore_Proto::ReqCMD toServer;
      toServer.set_rtype(XStore_Proto::INFO);

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
      XStore_Proto::SysInfo_REP fromServer;
      SysInfo toReturn;

      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());

      // Put return data from server to struct
      toReturn.serverAddr = fromServer.serveraddr();
      toReturn.numThreads = fromServer.threadcount();

      // Loop through info of all DB exists in server
      for (int i = 0; i < fromServer.dbinfo_size(); i++) {
          DbInfo currDB;
          currDB.dbName = fromServer.dbinfo(i).dbname();
          currDB.dbResolution = fromServer.dbinfo(i).dbresolution();
          currDB.totalSizeInByte = fromServer.dbinfo(i).totalsizeinbyte();

          // Loop through all file name of a given DB
          for (int j = 0; j < fromServer.dbinfo(i).filename_size(); j++) {
              currDB.fileName.push_back(fromServer.dbinfo(i).filename(j));
          }
          toReturn.db_Info.push_back(currDB);
      }

      return toReturn;
  }

  std::string UnaryQuery(std::time_t &targetUnixTime, std::string &targetDBName) {
      XStore_Proto::UnaryQuery_REQ toServer;
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
      XStore_Proto::UnaryQuery_REP fromServer;
      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());

      return std::to_string(fromServer.timestamp()) + "," + fromServer.data().fielddata(0).stringdata();
  }

  std::string CreateDB(std::string &dbName, XStore_Proto::TimeUnit &timeResolution, short int rowLength) {
      XStore_Proto::CreateDB_REQ toServer;
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
      XStore_Proto::CreateDB_REP fromServer;

      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
      return fromServer.createdbstatus();
  }

  std::string UnaryInsert(std::time_t &insertUnixTime, XStore_Proto::RowData &data, std::string &dbName) {
      XStore_Proto::UnaryInsert_REQ toServer;
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
      XStore_Proto::UnaryInsert_REP fromServer;

      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
      return fromServer.insertstatus();
  }

  std::string RemoveDB(std::string &dbName) {
      XStore_Proto::RemoveDB_REQ toServer;
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
      XStore_Proto::RemoveDB_REP fromServer;

      fromServer.ParseFromArray(recv_msgs.back().data(), recv_msgs.back().size());
      return fromServer.removestatus();
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_