#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include "zmq.hpp"
#include "zmq_addon.hpp"
#include "XStore.pb.h"

struct DbInfo {
  std::string DbName;
  XStore_Proto::TimeUnit DbResolution;
  float TotalSizeInByte;
  std::vector<std::string> FileName;
};

struct SysInfo {
  std::string SERVER_ADDR;
  int NumThreads;
  std::vector<DbInfo> DbInfoList;
};

class XStore_Client {
 private:
  zmq::context_t Ctx;
  zmq::socket_t Sock { Ctx, zmq::socket_type::req };
 public:
  XStore_Client(std::string SERVER_ADDR, std::string SERVER_PORT) {
      std::string FullAddr = "tcp://" + SERVER_ADDR + ":" + SERVER_PORT;
      Sock.connect(FullAddr);
  }

  std::string GetCurrDT() {
      std::stringstream FormattedTime;
      std::time_t TimeNow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      FormattedTime << std::put_time(std::localtime(&TimeNow), "%b %d, %Y %r");
      std::string ToReturn = FormattedTime.str();
      return ToReturn;
  }

  SysInfo InfoRequest() {
      XStore_Proto::ReqCMD ToServer;
      ToServer.set_rtype(XStore_Proto::INFO);

      // Config msg and send
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      Sock.send(ZOut, zmq::send_flags::none);

      // RECEIVING
      std::vector<zmq::message_t> RecvMsgs;
      zmq::message_t ZFromServer;
      zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs));

      // Received structs
      XStore_Proto::SysInfo_REP FromServer;
      SysInfo ToReturn;

      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());

      // Put return data from server to struct
      ToReturn.SERVER_ADDR = FromServer.serveraddr();
      ToReturn.NumThreads = FromServer.threadcount();

      // Loop through info of all DB exists in server
      for (int i = 0; i < FromServer.dbinfo_size(); i++) {
          DbInfo CurrDb;
          CurrDb.DbName = FromServer.dbinfo(i).dbname();
          CurrDb.DbResolution = FromServer.dbinfo(i).dbresolution();
          CurrDb.TotalSizeInByte = FromServer.dbinfo(i).totalsizeinbyte();

          // Loop through all file name of a given DB
          for (int j = 0; j < FromServer.dbinfo(i).filename_size(); j++) {
              CurrDb.FileName.push_back(FromServer.dbinfo(i).filename(j));
          }
          ToReturn.DbInfoList.push_back(CurrDb);
      }

      return ToReturn;
  }

  std::string UnaryQuery(std::time_t &targetUnixTime, std::string &targetDBName) {
      XStore_Proto::UnaryQuery_REQ ToServer;
      ToServer.set_rtype(XStore_Proto::UNARYQUERY);
      ToServer.set_timestamp(targetUnixTime);
      ToServer.set_dbname(targetDBName);

      // Config msg and send
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      Sock.send(ZOut, zmq::send_flags::none);

      // RECEIVING
      std::vector<zmq::message_t> RecvMsgs;
      zmq::message_t ZFromServer;
      zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs));

      // Received structs
      XStore_Proto::UnaryQuery_REP FromServer;
      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());

      return std::to_string(FromServer.timestamp()) + "," + FromServer.data().fielddata(0).stringdata();
  }

  std::string CreateDB(std::string &dbName, XStore_Proto::TimeUnit &timeResolution, short int rowLength) {
      XStore_Proto::CreateDB_REQ ToServer;
      ToServer.set_rtype(XStore_Proto::CREATEDB);
      ToServer.set_dbname(dbName);
      ToServer.set_timeresolution(timeResolution);
      ToServer.set_rowlength(rowLength);

      // Config msg and send
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      Sock.send(ZOut, zmq::send_flags::none);

      // RECEIVING
      std::vector<zmq::message_t> RecvMsgs;
      zmq::message_t ZFromServer;
      zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs));

      // Received structs
      XStore_Proto::CreateDB_REP FromServer;

      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());
      return FromServer.createdbstatus();
  }

  std::string UnaryInsert(std::time_t &insertUnixTime, XStore_Proto::RowData &data, std::string &dbName) {
      XStore_Proto::UnaryInsert_REQ ToServer;
      ToServer.set_rtype(XStore_Proto::UNARYINSERT);
      ToServer.set_timestamp(insertUnixTime);
      *ToServer.mutable_data() = data;
      ToServer.set_dbname(dbName);

      // Config msg and send
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      Sock.send(ZOut, zmq::send_flags::none);

      // RECEIVING
      std::vector<zmq::message_t> RecvMsgs;
      zmq::message_t ZFromServer;
      zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs));

      // Received structs
      XStore_Proto::UnaryInsert_REP FromServer;

      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());
      return FromServer.insertstatus();
  }

  std::string RemoveDB(std::string &dbName) {
      XStore_Proto::RemoveDB_REQ ToServer;
      ToServer.set_rtype(XStore_Proto::REMOVEDB);
      ToServer.set_dbname(dbName);

      // Config msg and send
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      Sock.send(ZOut, zmq::send_flags::none);

      // RECEIVING
      std::vector<zmq::message_t> RecvMsgs;
      zmq::message_t ZFromServer;
      zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs));

      // Received structs
      XStore_Proto::RemoveDB_REP FromServer;

      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());
      return FromServer.removestatus();
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_