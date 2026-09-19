#ifndef XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
#define XSTORE_CLIENTSRC_XSTORE_CLIENT_H_

#include <chrono>
#include <cstdint>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <optional>
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

// Client-side CURVE identity: this client's own keypair plus the server's
// public key (which the client must already know out-of-band -- see the
// design doc). Deliberately duplicates the tiny amount of Z85-file-reading
// logic XStore_Curve.h has server-side rather than including that header,
// so this file keeps compiling standalone if client_src/Cpp is copied out
// on its own without the rest of the repo.
struct XStoreCurveClientAuth {
  std::string client_public_key_z85;
  std::string client_secret_key_z85;
  std::string server_public_key_z85;

  // client_secret_key_file's sibling ".pub" (same directory/stem) supplies
  // the client's own public key -- the same xstore-keys gen convention
  // used server-side.
  static XStoreCurveClientAuth LoadFromFiles(std::string_view client_secret_key_file,
                                              std::string_view server_public_key_file) {
      auto read_z85 = [](const std::filesystem::path &p) {
          std::ifstream f(p);
          if (!f.is_open()) {
              throw std::runtime_error("XStoreCurveClientAuth: cannot open " + p.string());
          }
          std::string key;
          f >> key;
          if (key.size() != 40) {
              throw std::runtime_error("XStoreCurveClientAuth: " + p.string() +
                                        " does not contain a 40-character Z85 key");
          }
          return key;
      };
      std::filesystem::path secret_path(client_secret_key_file);
      std::filesystem::path public_path = secret_path;
      public_path.replace_extension(".pub");

      XStoreCurveClientAuth auth;
      auth.client_secret_key_z85 = read_z85(secret_path);
      auth.client_public_key_z85 = read_z85(public_path);
      auth.server_public_key_z85 = read_z85(std::filesystem::path(server_public_key_file));
      return auth;
  }
};

struct FileInfo {
  std::string file_name;
  // actual = real bytes on disk (st_blocks*512, sparse-hole-aware);
  // apparent = logical/reported size (st_size). apparent can exceed actual
  // for a preallocated file that still has unwritten holes.
  uint64_t actual_size_bytes;
  uint64_t apparent_size_bytes;
};

struct DbInfo {
  std::string db_name;
  XStore_Proto::TimeUnit db_resolution;
  // Matches XStore_Proto::DbStats.totalSizeInByte (uint64) -- sum of every
  // file's actual_size_bytes, i.e. real disk usage of this DB. A float here
  // silently loses precision past 2^24 bytes (16MB) -- easy to hit for any
  // real time-series file.
  uint64_t total_size_in_byte;
  std::vector<FileInfo> files;
  // Matches XStore_Proto::DbStats.rowLength -- the configured per-row slot
  // size (bytes) every insert's serialized RowData is zero-padded to.
  // Compare against a real row's actual serialized size to tell whether
  // this was sized too large (wasted padding/row) or too small.
  int32_t row_length;
  // Matches XStore_Proto::DbStats.columnNames -- optional per-index column
  // names set at CreateDB time (see ResolveColumnIndex). Empty if the DB
  // was created without names.
  std::vector<std::string> column_names;
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
      // Every existing RPC replies with exactly one frame -- this is the
      // same single ParseFromArray as before, zero behavior change. A
      // streamed reply (currently only RangeQuery, see RangeQuery)
      // arrives as several frames that are pieces of ONE logical message's
      // bytes (protobuf doesn't care where a message's serialized bytes get
      // split, only that they're concatenated back in order before
      // parsing) -- reassembled here so this stays invisible to every
      // caller above SendRecv.
      if (recv_buf.size() == 1) {
          from_server.ParseFromArray(recv_buf.front().data(), recv_buf.front().size());
      } else {
          std::string joined;
          size_t total = 0;
          for (const auto &part : recv_buf) total += part.size();
          joined.reserve(total);
          for (const auto &part : recv_buf) joined.append(static_cast<const char *>(part.data()), part.size());
          from_server.ParseFromArray(joined.data(), joined.size());
      }
      return from_server;
  }

 public:
  // timeoutMs < 0 (default) blocks forever, matching the previous behavior.
  // Pass a positive value so an unreachable/dead server raises instead of
  // hanging this REQ socket indefinitely. warmupOnInit lets callers who
  // don't care about paying the handshake cost upfront (e.g. short-lived
  // clients that may never send a request) skip it.
  XStoreClient(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
               bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
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
  void Reconnect(std::string_view server_addr, std::string_view server_port, int timeout_ms = -1,
                 bool warmup_on_init = true, std::optional<XStoreCurveClientAuth> curve_auth = std::nullopt) {
      sock = zmq::socket_t(ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::sndtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      if (curve_auth) {
          sock.set(zmq::sockopt::curve_serverkey, curve_auth->server_public_key_z85);
          sock.set(zmq::sockopt::curve_publickey, curve_auth->client_public_key_z85);
          sock.set(zmq::sockopt::curve_secretkey, curve_auth->client_secret_key_z85);
      }
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
      XStore_Proto::Request to_server;
      to_server.mutable_ping()->set_rtype(XStore_Proto::PING);
      SendRecv<XStore_Proto::Ping_REP>(to_server);
  }

  // ==== INFO ====
  SysInfo InfoRequest() {
      XStore_Proto::Request to_server;
      to_server.mutable_info()->set_rtype(XStore_Proto::INFO);
      XStore_Proto::SysInfo_REP from_server = SendRecv<XStore_Proto::SysInfo_REP>(to_server);

      SysInfo to_return;
      to_return.server_addr = from_server.serveraddr();
      to_return.num_threads = from_server.threadcount();

      for (int i = 0; i < from_server.dbinfo_size(); i++) {
          DbInfo curr_db;
          curr_db.db_name = from_server.dbinfo(i).dbname();
          curr_db.db_resolution = from_server.dbinfo(i).dbresolution();
          curr_db.total_size_in_byte = from_server.dbinfo(i).totalsizeinbyte();
          curr_db.row_length = from_server.dbinfo(i).rowlength();

          for (int j = 0; j < from_server.dbinfo(i).columnnames_size(); j++) {
              curr_db.column_names.push_back(from_server.dbinfo(i).columnnames(j));
          }

          for (int j = 0; j < from_server.dbinfo(i).files_size(); j++) {
              const XStore_Proto::FileStats &f = from_server.dbinfo(i).files(j);
              curr_db.files.push_back(FileInfo{f.filename(), f.actualsizebytes(), f.apparentsizebytes()});
          }
          to_return.db_info_list.push_back(curr_db);
      }

      return to_return;
  }

  // ==== DB management ====
  [[nodiscard]] std::string CreateDB(std::string_view db_name, XStore_Proto::TimeUnit time_resolution, short int row_length,
                                      const std::vector<std::string> &column_names = {}) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_createdb();
      payload->set_rtype(XStore_Proto::CREATEDB);
      payload->set_dbname(std::string(db_name));
      payload->set_timeresolution(time_resolution);
      payload->set_rowlength(row_length);
      for (const std::string &name : column_names) {
          payload->add_columnnames(name);
      }
      return SendRecv<XStore_Proto::CreateDB_REP>(to_server).createdbstatus();
  }

  // Pure name->index lookup, no network call -- callers fetch column_names
  // once (InfoRequest()'s DbInfo.column_names, or the same list they just
  // passed to CreateDB) and pass it in here before serializing a query/
  // aggregate request with the resolved index. Throws rather than silently
  // guessing on an empty list or an unknown name.
  [[nodiscard]] static int ResolveColumnIndex(const std::vector<std::string> &column_names, std::string_view name) {
      for (size_t i = 0; i < column_names.size(); i++) {
          if (column_names[i] == name) return static_cast<int>(i);
      }
      throw std::invalid_argument(fmt::format("XStoreClient::ResolveColumnIndex: no column named '{}' (DB has {} named column(s))",
                                                name, column_names.size()));
  }

  [[nodiscard]] std::string RemoveDB(std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedb();
      payload->set_rtype(XStore_Proto::REMOVEDB);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RemoveDB_REP>(to_server).removestatus();
  }

  // Per-file status, one entry per requested fileName -- can't be flattened
  // to a single string without losing which file(s) failed.
  [[nodiscard]] XStore_Proto::RemoveDBFiles_REP RemoveDBFiles(std::string_view db_name, const std::vector<std::string> &file_names) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_removedbfiles();
      payload->set_rtype(XStore_Proto::REMOVEDBFILES);
      payload->set_dbname(std::string(db_name));
      for (const std::string &name : file_names) {
          payload->add_filename(name);
      }
      return SendRecv<XStore_Proto::RemoveDBFiles_REP>(to_server);
  }

  // ==== Row sizing ====
  // Suggests a CreateDB rowLength/slot size from one sample RowData, so
  // callers don't have to hand-guess a value (and over/under-provision --
  // see DbInfo::row_length above). Mirrors XBench's
  // XStore_Adapter::ComputeRowLength, ported here so client_src callers get
  // it natively instead of reimplementing it.
  //
  // PROTOBUF_FIELD_OVERHEAD=3 is exact for every fixed-width/varint oneof
  // case (double/int32/int64/etc.): 1B outer repeated-field tag (RowData's
  // fieldData=1) + 1B outer length-varint (assumes the encoded FData stays
  // under 128B, true for any scalar or short string) + 1B inner oneof tag.
  // It undercounts stringData/bytesData by 1B: those are themselves
  // length-delimited, so they need a second inner length-varint the
  // fixed/varint cases don't. That 1B/string-field gap already existed in
  // the XBench original this mirrors -- flagged here, not silently fixed,
  // since this helper is meant to match, not diverge from, the logic it's
  // porting.
  template <uint32_t Alignment = 8>
  [[nodiscard]] static uint32_t SuggestRowLength(const XStore_Proto::RowData &sample_row) {
      static_assert((Alignment & (Alignment - 1)) == 0, "Alignment must be power of 2");
      constexpr uint32_t PROTOBUF_FIELD_OVERHEAD = 3;

      uint32_t row_size = 0;
      for (int i = 0; i < sample_row.fielddata_size(); i++) {
          const XStore_Proto::FData &field = sample_row.fielddata(i);
          switch (field.oneOfData_case()) {
              case XStore_Proto::FData::kStringData:
                  row_size += static_cast<uint32_t>(field.stringdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBytesData:
                  row_size += static_cast<uint32_t>(field.bytesdata().size()) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kDoubleData:
                  row_size += sizeof(double) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kFloatData:
                  row_size += sizeof(float) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kBoolData:
                  row_size += sizeof(bool) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt32Data:
              case XStore_Proto::FData::kUint32Data:
              case XStore_Proto::FData::kSint32Data:
              case XStore_Proto::FData::kFixed32Data:
              case XStore_Proto::FData::kSfixed32Data:
                  row_size += sizeof(int32_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::kInt64Data:
              case XStore_Proto::FData::kUint64Data:
              case XStore_Proto::FData::kSint64Data:
              case XStore_Proto::FData::kFixed64Data:
              case XStore_Proto::FData::kSfixed64Data:
                  row_size += sizeof(int64_t) + PROTOBUF_FIELD_OVERHEAD;
                  break;
              case XStore_Proto::FData::ONEOFDATA_NOT_SET:
                  break;
          }
      }

      return (row_size + Alignment - 1) & ~(Alignment - 1);
  }

  // ==== Unary ====
  // durability_mode defaults to ASYNC -- no fsync wait, lowest latency. A
  // row is only in-memory/queued until the writer thread flushes it, so a
  // crash before that flush loses it. Pass SYNC explicitly for writes that
  // must survive a crash.
  [[nodiscard]] std::string UnaryInsert(std::time_t insert_unix_time, const XStore_Proto::RowData &data, std::string_view db_name,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryinsert();
      payload->set_rtype(XStore_Proto::UNARYINSERT);
      payload->set_timestamp(insert_unix_time);
      *payload->mutable_data() = data;
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      return SendRecv<XStore_Proto::UnaryInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] XStore_Proto::UnaryQuery_REP UnaryQuery(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unaryquery();
      payload->set_rtype(XStore_Proto::UNARYQUERY);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryQuery_REP>(to_server);
  }

  [[nodiscard]] std::string UnaryDelete(std::time_t target_unix_time, std::string_view target_db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_unarydelete();
      payload->set_rtype(XStore_Proto::UNARYDELETE);
      payload->set_timestamp(target_unix_time);
      payload->set_dbname(std::string(target_db_name));
      return SendRecv<XStore_Proto::UnaryDelete_REP>(to_server).deletestatus();
  }

  // ==== Batch (arbitrary timestamp set) ====
  // durability_mode defaults to ASYNC, same WAL-routed semantics as
  // UnaryInsert's: see BatchInsert_REQ's durabilityMode comment in
  // XStore.proto. SYNC blocks until the write is durably fsync'd and
  // applied; ASYNC enqueues and returns immediately -- note that for the
  // default ASYNC mode, both the fsync AND the data-file write itself are
  // deferred to the server's WAL writer thread, so (unlike this client's
  // pre-WAL behavior) a read immediately after an ASYNC BatchInsert is not
  // guaranteed to see it yet.
  [[nodiscard]] std::string BatchInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchinsert();
      payload->set_rtype(XStore_Proto::BATCHINSERT);
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      payload->mutable_batchinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (const auto &row : rows) {
          XStore_Proto::BatchInsert_DATA *entry = payload->add_batchinsert_data();
          entry->set_timestamp(row.first);
          *entry->mutable_data() = row.second;
      }
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server).insertstatus();
  }

  // Advanced/perf overload: build the request directly (e.g. reuse a
  // populated message across calls) instead of via the (timestamp, RowData)
  // vector above. Takes by value so an rvalue caller moves in for free.
  [[nodiscard]] std::string BatchInsert(XStore_Proto::BatchInsert_REQ insert_data) {
      XStore_Proto::Request to_server;
      *to_server.mutable_batchinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::BatchInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] XStore_Proto::BatchQuery_REP BatchQuery(std::string_view db_name, const std::vector<uint64_t> &timestamps, bool sort_time = true) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchquery();
      payload->set_rtype(XStore_Proto::BATCHQUERY);
      payload->set_dbname(std::string(db_name));
      payload->set_sorttime(sort_time);
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecv<XStore_Proto::BatchQuery_REP>(to_server);
  }

  [[nodiscard]] std::string BatchDelete(std::string_view db_name, const std::vector<uint64_t> &timestamps) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_batchdelete();
      payload->set_rtype(XStore_Proto::BATCHDELETE);
      payload->set_dbname(std::string(db_name));
      for (uint64_t ts : timestamps) {
          payload->add_timestamp(ts);
      }
      return SendRecv<XStore_Proto::BatchDelete_REP>(to_server).deletestatus();
  }

  // ==== Range (contiguous timestamp span) ====
  // durability_mode: same WAL-routed semantics as BatchInsert's overload
  // above -- see RangeInsert_REQ's durabilityMode comment in XStore.proto,
  // including the ASYNC default-mode note there.
  [[nodiscard]] std::string RangeInsert(std::string_view db_name, const std::vector<std::pair<uint64_t, XStore_Proto::RowData>> &rows,
                                         XStore_Proto::WalDurabilityMode durability_mode = XStore_Proto::ASYNC) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangeinsert();
      payload->set_rtype(XStore_Proto::RANGEINSERT);
      payload->set_dbname(std::string(db_name));
      payload->set_durabilitymode(durability_mode);
      payload->mutable_rangeinsert_data()->Reserve(static_cast<int>(rows.size()));
      for (const auto &row : rows) {
          XStore_Proto::RangeInsert_DATA *entry = payload->add_rangeinsert_data();
          entry->set_timestamp(row.first);
          *entry->mutable_data() = row.second;
      }
      return SendRecv<XStore_Proto::RangeInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] std::string RangeInsert(XStore_Proto::RangeInsert_REQ insert_data) {
      XStore_Proto::Request to_server;
      *to_server.mutable_rangeinsert() = std::move(insert_data);
      return SendRecv<XStore_Proto::RangeInsert_REP>(to_server).insertstatus();
  }

  [[nodiscard]] XStore_Proto::RangeQuery_REP RangeQuery(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangequery();
      payload->set_rtype(XStore_Proto::RANGEQUERY);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeQuery_REP>(to_server);
  }

  [[nodiscard]] std::string RangeDelete(uint64_t start_time, uint64_t end_time, std::string_view db_name) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_rangedelete();
      payload->set_rtype(XStore_Proto::RANGEDELETE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      return SendRecv<XStore_Proto::RangeDelete_REP>(to_server).deletestatus();
  }

  // ==== Aggregates ====
  [[nodiscard]] XStore_Proto::Min_REP Min(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_min();
      payload->set_rtype(XStore_Proto::MIN);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Min_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Max_REP Max(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_max();
      payload->set_rtype(XStore_Proto::MAX);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Max_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Sum_REP Sum(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_sum();
      payload->set_rtype(XStore_Proto::SUM);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Sum_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Avg_REP Avg(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_avg();
      payload->set_rtype(XStore_Proto::AVG);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Avg_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Variance_REP Variance(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_variance();
      payload->set_rtype(XStore_Proto::VARIANCE);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Variance_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Stddev_REP Stddev(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_stddev();
      payload->set_rtype(XStore_Proto::STDDEV);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Stddev_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Exists_REP Exists(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_exists();
      payload->set_rtype(XStore_Proto::EXISTS);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Exists_REP>(to_server);
  }

  [[nodiscard]] XStore_Proto::Ohlc_REP Ohlc(uint64_t start_time, uint64_t end_time, std::string_view db_name, int32_t column_idx) {
      XStore_Proto::Request to_server;
      auto *payload = to_server.mutable_ohlc();
      payload->set_rtype(XStore_Proto::OHLC);
      payload->set_starttime(start_time);
      payload->set_endtime(end_time);
      payload->set_dbname(std::string(db_name));
      payload->set_columnidx(column_idx);
      return SendRecv<XStore_Proto::Ohlc_REP>(to_server);
  }
};
#endif //XSTORE_CLIENTSRC_XSTORE_CLIENT_H_
