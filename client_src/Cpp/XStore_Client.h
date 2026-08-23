#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef FMT_HEADER_ONLY
#define FMT_HEADER_ONLY
#endif
#include "fmt/format.h"

#include <zmq.hpp>
#include <zmq_addon.hpp>
#include "XStore.pb.h"

struct DbInfo {
  std::string db_name;
  XStore_Proto::TimeUnit db_resolution;
  // Matches XStore_Proto::DbStats.totalSizeInByte (uint64). A float here
  // silently loses precision past 2^24 bytes (16MB) -- easy to hit for any
  // real time-series file.
  uint64_t total_size_in_byte;
  std::vector<std::string> file_name;
};

struct SysInfo {
  std::string server_addr;
  int num_threads;
  std::vector<DbInfo> db_info_list;
};

// Factory helpers for XStore_Proto::FData's oneof. proto3 picks a different
// wire encoding for sint32/fixed32/sfixed32 even though they all take a
// plain int32_t in C++, so overloading on parameter type alone can't tell
// them apart -- name the intent instead. Purely additive: existing callers
// that build FData by hand (field->set_stringdata(...)) still work.
namespace xstore_fdata {
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

class XStoreClient {
 private:
  zmq::context_t ctx;
  zmq::socket_t sock { ctx, zmq::socket_type::req };

  // Reused across calls instead of a fresh std::vector<zmq::message_t>
  // constructed on every SendRecv -- .clear() keeps the already-grown
  // backing storage. Safe as a plain member (not thread_local): a single
  // XStore_Client instance's REQ socket is strict send-then-recv lockstep
  // already, so it was never safe to call concurrently from multiple
  // threads to begin with.
  std::vector<zmq::message_t> recv_buf;

  // Reused across calls for the same reason as recv_buf above: every RPC
  // method below builds a Request, and mutable_X() on a oneof field
  // heap-allocates that submessage when the Request doesn't already hold
  // one -- a fresh Request per call means malloc+free every single RPC.
  // Clear() on a Request that already holds a submessage keeps that
  // submessage's own allocation (and, for repeated fields like
  // batchinsert_data, keeps every already-allocated element too) and just
  // resets its contents, so the alloc/free pair only happens once per
  // Request shape, not once per call. Benchmarked against a per-call
  // google::protobuf::Arena at every scale from a single row up to 20k rows
  // x 128 double columns: reused Request+Clear() was never worse and pulled
  // decisively ahead at the largest sizes, so no Arena dependency here.
  // Same one-instance-per-thread requirement as recv_buf/sock -- confirmed
  // safe against XBench's usage (each of its ThreadPool-submitted
  // *_SingleThread workers constructs its own local XStore_Client).
  XStore_Proto::Request to_server_msg;

  // Every RPC is: serialize request, blocking send, blocking recv, parse
  // reply. Sharing that here means the 17 request types can't drift out of
  // sync with each other the way copy-pasted send/recv blocks do. ToServer
  // is always the Request envelope -- the oneof case tells the server which
  // payload it's holding, so it only has to parse the wire bytes once.
  template <typename RepT>
  RepT SendRecv(const XStore_Proto::Request &to_server) {
      size_t byte_size = to_server.ByteSizeLong();
      zmq::message_t z_out(byte_size);
      to_server.SerializeToArray(z_out.data(), byte_size);
      if (!sock.send(z_out, zmq::send_flags::none)) {
          throw std::runtime_error("XStore_Client: send failed/timed out");
      }

      recv_buf.clear();
      if (!zmq::recv_multipart(sock, std::back_inserter(recv_buf)) || recv_buf.empty()) {
          throw std::runtime_error("XStore_Client: recv failed/timed out");
      }

      RepT from_server;
      from_server.ParseFromArray(recv_buf.back().data(), recv_buf.back().size());
      return from_server;
  }

 public:
  // timeoutMs < 0 (default) blocks forever, matching the previous behavior.
  // Pass a positive value so an unreachable/dead server raises instead of
  // hanging this REQ socket indefinitely. warmupOnInit lets callers who
  // don't care about paying the handshake cost upfront (e.g. short-lived
  // clients that may never send a request) skip it.
  XStoreClient(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1, bool warmup_on_init = true) {
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }

  // ZMQ connect() is lazy -- TCP + ZMTP handshake actually fires on first
  // send(). Call this right after construction to eat that cost now instead
  // of on the first real RPC. PING is a bare round trip with no server-side
  // XStore work (unlike INFO, which scans the data store directory), so it
  // forces the handshake without the extra I/O. Throws same as any RPC on
  // timeout/unreachable server -- caller must Reconnect() before retrying,
  // same as elsewhere.
  void Warmup() {
      Ping();
  }

  // A REQ socket that hits a send/recv timeout has its strict
  // send-then-recv state machine left in an undefined spot per ZMQ's own
  // docs -- the fix is a fresh socket, not another send. Call this after
  // catching a timeout exception from any RPC below before retrying.
  void Reconnect(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1, bool warmup_on_init = true) {
      sock = zmq::socket_t(ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      std::string full_addr = fmt::format("tcp://{}:{}", server_addr, server_port);
      sock.connect(full_addr);
      if (warmup_on_init) {
          Warmup();
      }
  }

  std::string GetCurrDT() {
      std::stringstream formatted_time;
      std::time_t time_now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
      formatted_time << std::put_time(std::localtime(&time_now), "%b %d, %Y %r");
      std::string to_return = formatted_time.str();
      return to_return;
  }

  // ==== PING ====
  // Bare round trip, no server-side XStore work -- cheaper than InfoRequest()
  // for warmup/liveness checks that don't need SysInfo's contents.
  void Ping() {
      to_server_msg.Clear();
      to_server_msg.mutable_ping()->set_rtype(XStore_Proto::PING);
      SendRecv<XStore_Proto::Ping_REP>(to_server_msg);
  }

  // ==== INFO ====
  SysInfo InfoRequest() {
      to_server_msg.Clear();
      to_server_msg.mutable_info()->set_rtype(XStore_Proto::INFO);
      XStore_Proto::SysInfo_REP from_server = SendRecv<XStore_Proto::SysInfo_REP>(to_server_msg);

      SysInfo to_return;
      to_return.server_addr = from_server.serveraddr();
      to_return.num_threads = from_server.threadcount();

      for (int i = 0; i < from_server.dbinfo_size(); i++) {
          DbInfo curr_db;
          curr_db.db_name = from_server.dbinfo(i).dbname();
          curr_db.db_resolution = from_server.dbinfo(i).dbresolution();
          curr_db.total_size_in_byte = from_server.dbinfo(i).totalsizeinbyte();

          for (int j = 0; j < from_server.dbinfo(i).filename_size(); j++) {
              curr_db.file_name.push_back(from_server.dbinfo(i).filename(j));
          }
          to_return.db_info_list.push_back(curr_db);
      }

      return to_return;
  }

  // ==== DB management ====
  [[nodiscard]] std::string CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_createdb();
      payload->set_rtype(XStore_Proto::CREATEDB);
      payload->set_dbname(std::string(db_name));
      payload->set_timeresolution(time_resolution);
      payload->set_rowlength(row_length);
      return SendRecv<XStore_Proto::CreateDB_REP>(to_server_msg).createdbstatus();
  }

  [[nodiscard]] std::string RemoveDB(std::string_view db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_removedb();
      payload->set_rtype(XStore_Proto::REMOVEDB);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RemoveDB_REP>(to_server_msg).removestatus();
  }

  // Per-file status, one entry per requested fileName -- can't be flattened
  // to a single string without losing which file(s) failed.
  [[nodiscard]] XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(std::string_view db_name, const std::vector<std::string> &file_names) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_removedbfiles();
      payload->set_rtype(XStore_Proto::REMOVEDBFILES);
      payload->set_dbname(std::string(db_name));
      for (const std::string &name : file_names) {
          payload->add_filename(name);
      }
      return SendRecv<XStore_Proto::RemoveDBFiles_REP>(to_server_msg);
  }

  // ==== Unary ====
  [[nodiscard]] std::string UnaryInsert(std::time_t insert_unix_time, const XStore_Proto::RowData &data, std::string_view db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_unaryinsert();
      payload->set_rtype(XStore_Proto::UNARYINSERT);
      payload->set_timestamp(insert_unix_time);
      *payload->mutable_data() = data;
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::UnaryInsert_REP>(to_server_msg).insertstatus();
  }

  [[nodiscard]] XStore_Proto::UnaryQuery_REP UnaryQuery(std::time_t target_unix_time, std::string_view target_db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_unaryquery();
      payload->set_rtype(XStore_Proto::UNARYQUERY);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryQuery_REP>(to_server_msg);
  }

  [[nodiscard]] std::string UnaryDelete(std::time_t target_unix_time, std::string_view target_db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_unarydelete();
      payload->set_rtype(XStore_Proto::UNARYDELETE);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryDelete_REP>(to_server_msg).deletestatus();
  }

  // ==== Batch (arbitrary timestamp set) ====
  // Builds into the reused to_server_msg member (see its declaration above)
  // instead of a per-call Arena: benchmarked a reused Request+Clear() against
  // a per-call Arena at row counts from 1 to 20,000 (128 double columns/row)
  // -- reused Request was never worse and won decisively at the largest
  // sizes, since RepeatedPtrField::Clear() already keeps every previously
  // allocated batchinsert_data element resident for reuse, with none of an
  // Arena's own per-reset bookkeeping.
  [[nodiscard]] std::string BatchInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_batchinsert();
      payload->set_rtype(XStore_Proto::BATCHINSERT);
      payload->set_dbname(std::string(db_name));
      payload->mutable_batchinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (const auto &row : rows) {
          XStore_Proto::BatchInsert_DATA *entry = payload->add_batchinsert_data();
          entry->set_timestamp(row.first);
          *entry->mutable_data() = row.second;
      }
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server_msg).insertstatus();
  }

  // Advanced/perf overload: build the request directly (e.g. reuse a
  // populated message across calls) instead of via the (timestamp, RowData)
  // vector above. Takes by value so an rvalue caller moves in for free.
  [[nodiscard]] std::string BatchInsert(XStore_Proto::BatchInsert_REQ insert_data) {
      to_server_msg.Clear();
      *to_server_msg.mutable_batchinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server_msg).insertstatus();
  }

  [[nodiscard]] XStore_Proto::BatchQuery_REP BatchQuery(std::string_view db_name, const std::vector<uint64_t> &timestamps, bool sort_time = true) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_batchquery();
      payload->set_rtype(XStore_Proto::BATCHQUERY);
      payload->set_dbname(std::string(db_name));
      payload->set_sorttime(sort_time);
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecv<XStore_Proto::BatchQuery_REP>(to_server_msg);
  }

  [[nodiscard]] std::string BatchDelete(std::string_view db_name, const std::vector<uint64_t> &timestamps) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_batchdelete();
      payload->set_rtype(XStore_Proto::BATCHDELETE);
      payload->set_dbname(std::string(db_name));
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecv<XStore_Proto::BatchDelete_REP>(to_server_msg).deletestatus();
  }

  // ==== Range (contiguous timestamp span) ====
  // Same reused-Request rationale as the BatchInsert overload above.
  [[nodiscard]] std::string RangeInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_rangeinsert();
      payload->set_rtype(XStore_Proto::RANGEINSERT);
      payload->set_dbname(std::string(db_name));
      payload->mutable_rangeinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (const auto &row : rows) {
          XStore_Proto::RangeInsert_DATA *entry = payload->add_rangeinsert_data();
          entry->set_timestamp(row.first);
          *entry->mutable_data() = row.second;
      }
      return SendRecv<XStore_Proto::RangeInsert_REP>(to_server_msg).insertstatus();
  }

  [[nodiscard]] std::string RangeInsert(XStore_Proto::RangeInsert_REQ insert_data) {
      to_server_msg.Clear();
      *to_server_msg.mutable_rangeinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::RangeInsert_REP>(to_server_msg).insertstatus();
  }

  [[nodiscard]] XStore_Proto::RangeQuery_REP RangeQuery(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_rangequery();
      payload->set_rtype(XStore_Proto::RANGEQUERY);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeQuery_REP>(to_server_msg);
  }

  [[nodiscard]] std::string RangeDelete(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_rangedelete();
      payload->set_rtype(XStore_Proto::RANGEDELETE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeDelete_REP>(to_server_msg).deletestatus();
  }

  // ==== Aggregates ====
  [[nodiscard]] XStore_Proto::Min_REP Min(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_min();
      payload->set_rtype(XStore_Proto::MIN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Min_REP>(to_server_msg);
  }

  [[nodiscard]] XStore_Proto::Max_REP Max(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_max();
      payload->set_rtype(XStore_Proto::MAX);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Max_REP>(to_server_msg);
  }

  [[nodiscard]] XStore_Proto::Sum_REP Sum(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_sum();
      payload->set_rtype(XStore_Proto::SUM);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Sum_REP>(to_server_msg);
  }

  [[nodiscard]] XStore_Proto::Avg_REP Avg(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      to_server_msg.Clear();
      auto *payload = to_server_msg.mutable_avg();
      payload->set_rtype(XStore_Proto::AVG);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Avg_REP>(to_server_msg);
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
