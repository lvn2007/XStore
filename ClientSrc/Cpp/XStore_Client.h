#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "zmq.hpp"
#include "zmq_addon.hpp"
#include "XStore.pb.h"

struct DbInfo {
  std::string DbName;
  XStore_Proto::TimeUnit DbResolution;
  // Matches XStore_Proto::DbStats.totalSizeInByte (uint64). A float here
  // silently loses precision past 2^24 bytes (16MB) -- easy to hit for any
  // real time-series file.
  uint64_t TotalSizeInByte;
  std::vector<std::string> FileName;
};

struct SysInfo {
  std::string SERVER_ADDR;
  int NumThreads;
  std::vector<DbInfo> DbInfoList;
};

// Factory helpers for XStore_Proto::FData's oneof. proto3 picks a different
// wire encoding for sint32/fixed32/sfixed32 even though they all take a
// plain int32_t in C++, so overloading on parameter type alone can't tell
// them apart -- name the intent instead. Purely additive: existing callers
// that build FData by hand (field->set_stringdata(...)) still work.
namespace XStore_FData {
  inline XStore_Proto::FData Double(double v)        { XStore_Proto::FData f; f.set_doubledata(v);   return f; }
  inline XStore_Proto::FData Float(float v)           { XStore_Proto::FData f; f.set_floatdata(v);    return f; }
  inline XStore_Proto::FData Int32(int32_t v)         { XStore_Proto::FData f; f.set_int32data(v);    return f; }
  inline XStore_Proto::FData Int64(int64_t v)         { XStore_Proto::FData f; f.set_int64data(v);    return f; }
  inline XStore_Proto::FData UInt32(uint32_t v)       { XStore_Proto::FData f; f.set_uint32data(v);   return f; }
  inline XStore_Proto::FData UInt64(uint64_t v)       { XStore_Proto::FData f; f.set_uint64data(v);   return f; }
  inline XStore_Proto::FData SInt32(int32_t v)        { XStore_Proto::FData f; f.set_sint32data(v);   return f; }
  inline XStore_Proto::FData SInt64(int64_t v)        { XStore_Proto::FData f; f.set_sint64data(v);   return f; }
  inline XStore_Proto::FData Fixed32(uint32_t v)      { XStore_Proto::FData f; f.set_fixed32data(v);  return f; }
  inline XStore_Proto::FData Fixed64(uint64_t v)      { XStore_Proto::FData f; f.set_fixed64data(v);  return f; }
  inline XStore_Proto::FData SFixed32(int32_t v)      { XStore_Proto::FData f; f.set_sfixed32data(v); return f; }
  inline XStore_Proto::FData SFixed64(int64_t v)      { XStore_Proto::FData f; f.set_sfixed64data(v); return f; }
  inline XStore_Proto::FData Bool(bool v)             { XStore_Proto::FData f; f.set_booldata(v);     return f; }
  inline XStore_Proto::FData String(std::string v)    { XStore_Proto::FData f; f.set_stringdata(std::move(v)); return f; }
  inline XStore_Proto::FData Bytes(std::string v)     { XStore_Proto::FData f; f.set_bytesdata(std::move(v));  return f; }
}

class XStore_Client {
 private:
  zmq::context_t Ctx;
  zmq::socket_t Sock { Ctx, zmq::socket_type::req };

  // Every RPC is: serialize request, blocking send, blocking recv, parse
  // reply. Sharing that here means the 17 request types can't drift out of
  // sync with each other the way copy-pasted send/recv blocks do. ToServer
  // is always the Request envelope -- the oneof case tells the server which
  // payload it's holding, so it only has to parse the wire bytes once.
  template <typename RepT>
  RepT SendRecv(const XStore_Proto::Request &ToServer) {
      size_t ByteSize = ToServer.ByteSizeLong();
      zmq::message_t ZOut(ByteSize);
      ToServer.SerializeToArray(ZOut.data(), ByteSize);
      if (!Sock.send(ZOut, zmq::send_flags::none)) {
          throw std::runtime_error("XStore_Client: send failed/timed out");
      }

      std::vector<zmq::message_t> RecvMsgs;
      if (!zmq::recv_multipart(Sock, std::back_inserter(RecvMsgs)) || RecvMsgs.empty()) {
          throw std::runtime_error("XStore_Client: recv failed/timed out");
      }

      RepT FromServer;
      FromServer.ParseFromArray(RecvMsgs.back().data(), RecvMsgs.back().size());
      return FromServer;
  }

 public:
  // timeoutMs < 0 (default) blocks forever, matching the previous behavior.
  // Pass a positive value so an unreachable/dead server raises instead of
  // hanging this REQ socket indefinitely.
  XStore_Client(std::string SERVER_ADDR, std::string SERVER_PORT, int timeoutMs = -1) {
      Sock.set(zmq::sockopt::rcvtimeo, timeoutMs);
      Sock.set(zmq::sockopt::sndtimeo, timeoutMs);
      Sock.set(zmq::sockopt::linger, 0);
      std::string FullAddr = "tcp://" + SERVER_ADDR + ":" + SERVER_PORT;
      Sock.connect(FullAddr);
      Warmup();
  }

  // ZMQ connect() is lazy -- TCP + ZMTP handshake actually fires on first
  // send(). Call this right after construction to eat that cost now instead
  // of on the first real RPC. INFO is the lightest request type (rtype 0),
  // so it's used as the ping. Throws same as any RPC on timeout/unreachable
  // server -- caller must Reconnect() before retrying, same as elsewhere.
  void Warmup() {
      InfoRequest();
  }

  // A REQ socket that hits a send/recv timeout has its strict
  // send-then-recv state machine left in an undefined spot per ZMQ's own
  // docs -- the fix is a fresh socket, not another send. Call this after
  // catching a timeout exception from any RPC below before retrying.
  void Reconnect(std::string SERVER_ADDR, std::string SERVER_PORT, int timeoutMs = -1) {
      Sock = zmq::socket_t(Ctx, zmq::socket_type::req);
      Sock.set(zmq::sockopt::rcvtimeo, timeoutMs);
      Sock.set(zmq::sockopt::sndtimeo, timeoutMs);
      Sock.set(zmq::sockopt::linger, 0);
      Sock.connect("tcp://" + SERVER_ADDR + ":" + SERVER_PORT);
      Warmup();
  }

  std::string GetCurrDT() {
      std::stringstream FormattedTime;
      std::time_t TimeNow = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      FormattedTime << std::put_time(std::localtime(&TimeNow), "%b %d, %Y %r");
      std::string ToReturn = FormattedTime.str();
      return ToReturn;
  }

  // ==== INFO ====
  SysInfo InfoRequest() {
      XStore_Proto::Request ToServer;
      ToServer.mutable_info()->set_rtype(XStore_Proto::INFO);
      XStore_Proto::SysInfo_REP FromServer = SendRecv<XStore_Proto::SysInfo_REP>(ToServer);

      SysInfo ToReturn;
      ToReturn.SERVER_ADDR = FromServer.serveraddr();
      ToReturn.NumThreads = FromServer.threadcount();

      for (int i = 0; i < FromServer.dbinfo_size(); i++) {
          DbInfo CurrDb;
          CurrDb.DbName = FromServer.dbinfo(i).dbname();
          CurrDb.DbResolution = FromServer.dbinfo(i).dbresolution();
          CurrDb.TotalSizeInByte = FromServer.dbinfo(i).totalsizeinbyte();

          for (int j = 0; j < FromServer.dbinfo(i).filename_size(); j++) {
              CurrDb.FileName.push_back(FromServer.dbinfo(i).filename(j));
          }
          ToReturn.DbInfoList.push_back(CurrDb);
      }

      return ToReturn;
  }

  // ==== DB management ====
  std::string CreateDB(std::string &dbName, XStore_Proto::TimeUnit &timeResolution, short int rowLength) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_createdb();
      Payload->set_rtype(XStore_Proto::CREATEDB);
      Payload->set_dbname(dbName);
      Payload->set_timeresolution(timeResolution);
      Payload->set_rowlength(rowLength);
      return SendRecv<XStore_Proto::CreateDB_REP>(ToServer).createdbstatus();
  }

  std::string RemoveDB(std::string &dbName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_removedb();
      Payload->set_rtype(XStore_Proto::REMOVEDB);
      Payload->set_dbname(dbName);
      return SendRecv<XStore_Proto::RemoveDB_REP>(ToServer).removestatus();
  }

  // Per-file status, one entry per requested fileName -- can't be flattened
  // to a single string without losing which file(s) failed.
  XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(std::string &dbName, std::vector<std::string> &fileNames) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_removedbfiles();
      Payload->set_rtype(XStore_Proto::REMOVEDBFILES);
      Payload->set_dbname(dbName);
      for (const std::string &Name : fileNames) {
          Payload->add_filename(Name);
      }
      return SendRecv<XStore_Proto::RemoveDBFiles_REP>(ToServer);
  }

  // ==== Unary ====
  std::string UnaryInsert(std::time_t &insertUnixTime, XStore_Proto::RowData &data, std::string &dbName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_unaryinsert();
      Payload->set_rtype(XStore_Proto::UNARYINSERT);
      Payload->set_timestamp(insertUnixTime);
      *Payload->mutable_data() = data;
      Payload->set_dbname(dbName);
      return SendRecv<XStore_Proto::UnaryInsert_REP>(ToServer).insertstatus();
  }

  XStore_Proto::UnaryQuery_REP UnaryQuery(std::time_t &targetUnixTime, std::string &targetDBName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_unaryquery();
      Payload->set_rtype(XStore_Proto::UNARYQUERY);
      Payload->set_timestamp(targetUnixTime);
      Payload->set_dbname(targetDBName);
      return SendRecv<XStore_Proto::UnaryQuery_REP>(ToServer);
  }

  std::string UnaryDelete(std::time_t &targetUnixTime, std::string &targetDBName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_unarydelete();
      Payload->set_rtype(XStore_Proto::UNARYDELETE);
      Payload->set_timestamp(targetUnixTime);
      Payload->set_dbname(targetDBName);
      return SendRecv<XStore_Proto::UnaryDelete_REP>(ToServer).deletestatus();
  }

  // ==== Batch (arbitrary timestamp set) ====
  std::string BatchInsert(std::string &dbName, std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_batchinsert();
      Payload->set_rtype(XStore_Proto::BATCHINSERT);
      Payload->set_dbname(dbName);
      Payload->mutable_batchinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (auto &Row : rows) {
          XStore_Proto::BatchInsert_DATA *Entry = Payload->add_batchinsert_data();
          Entry->set_timestamp(Row.first);
          *Entry->mutable_data() = Row.second;
      }
      return SendRecv<XStore_Proto::BatchInsert_REP>(ToServer).insertstatus();
  }

  // Advanced/perf overload: build the request directly (e.g. reuse a
  // populated message across calls) instead of via the (timestamp, RowData)
  // vector above. Takes by value so an rvalue caller moves in for free.
  std::string BatchInsert(XStore_Proto::BatchInsert_REQ insertData) {
      XStore_Proto::Request ToServer;
      *ToServer.mutable_batchinsert() = std::move(insertData);
      return SendRecv<XStore_Proto::BatchInsert_REP>(ToServer).insertstatus();
  }

  XStore_Proto::BatchQuery_REP BatchQuery(std::string &dbName, std::vector<uint64_t> &timestamps, bool sortTime = true) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_batchquery();
      Payload->set_rtype(XStore_Proto::BATCHQUERY);
      Payload->set_dbname(dbName);
      Payload->set_sorttime(sortTime);
      for (uint64_t Ts : timestamps) {
          Payload->add_timestamp(Ts);
      }
      return SendRecv<XStore_Proto::BatchQuery_REP>(ToServer);
  }

  std::string BatchDelete(std::string &dbName, std::vector<uint64_t> &timestamps) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_batchdelete();
      Payload->set_rtype(XStore_Proto::BATCHDELETE);
      Payload->set_dbname(dbName);
      for (uint64_t Ts : timestamps) {
          Payload->add_timestamp(Ts);
      }
      return SendRecv<XStore_Proto::BatchDelete_REP>(ToServer).deletestatus();
  }

  // ==== Range (contiguous timestamp span) ====
  std::string RangeInsert(std::string &dbName, std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_rangeinsert();
      Payload->set_rtype(XStore_Proto::RANGEINSERT);
      Payload->set_dbname(dbName);
      Payload->mutable_rangeinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (auto &Row : rows) {
          XStore_Proto::RangeInsert_DATA *Entry = Payload->add_rangeinsert_data();
          Entry->set_timestamp(Row.first);
          *Entry->mutable_data() = Row.second;
      }
      return SendRecv<XStore_Proto::RangeInsert_REP>(ToServer).insertstatus();
  }

  std::string RangeInsert(XStore_Proto::RangeInsert_REQ insertData) {
      XStore_Proto::Request ToServer;
      *ToServer.mutable_rangeinsert() = std::move(insertData);
      return SendRecv<XStore_Proto::RangeInsert_REP>(ToServer).insertstatus();
  }

  XStore_Proto::RangeQuery_REP RangeQuery(uint64_t startTime, uint64_t endTime, std::string &dbName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_rangequery();
      Payload->set_rtype(XStore_Proto::RANGEQUERY);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      return SendRecv<XStore_Proto::RangeQuery_REP>(ToServer);
  }

  std::string RangeDelete(uint64_t startTime, uint64_t endTime, std::string &dbName) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_rangedelete();
      Payload->set_rtype(XStore_Proto::RANGEDELETE);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      return SendRecv<XStore_Proto::RangeDelete_REP>(ToServer).deletestatus();
  }

  // ==== Aggregates ====
  XStore_Proto::Min_REP Min(uint64_t startTime, uint64_t endTime, std::string &dbName, int32_t columnIdx) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_min();
      Payload->set_rtype(XStore_Proto::MIN);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      Payload->set_columnidx(columnIdx);
      return SendRecv<XStore_Proto::Min_REP>(ToServer);
  }

  XStore_Proto::Max_REP Max(uint64_t startTime, uint64_t endTime, std::string &dbName, int32_t columnIdx) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_max();
      Payload->set_rtype(XStore_Proto::MAX);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      Payload->set_columnidx(columnIdx);
      return SendRecv<XStore_Proto::Max_REP>(ToServer);
  }

  XStore_Proto::Sum_REP Sum(uint64_t startTime, uint64_t endTime, std::string &dbName, int32_t columnIdx) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_sum();
      Payload->set_rtype(XStore_Proto::SUM);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      Payload->set_columnidx(columnIdx);
      return SendRecv<XStore_Proto::Sum_REP>(ToServer);
  }

  XStore_Proto::Avg_REP Avg(uint64_t startTime, uint64_t endTime, std::string &dbName, int32_t columnIdx) {
      XStore_Proto::Request ToServer;
      auto *Payload = ToServer.mutable_avg();
      Payload->set_rtype(XStore_Proto::AVG);
      Payload->set_starttime(startTime);
      Payload->set_endtime(endTime);
      Payload->set_dbname(dbName);
      Payload->set_columnidx(columnIdx);
      return SendRecv<XStore_Proto::Avg_REP>(ToServer);
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
