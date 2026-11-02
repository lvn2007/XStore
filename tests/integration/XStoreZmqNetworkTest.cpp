// Transport only; request/response logic is covered in xstore_tests. Drives the real ZMQ wire protocol
// (ROUTER <-> DEALER <-> in-proc REP worker threads, see ZMQ_Server.cpp) to check what only a socket can:
// the oneof dispatch and reply encoding of each RPC family (incl. the zero-object streamed encoder behind
// Batch/Range query), error replies for an empty or unparseable envelope, and multi-client concurrency.
// This is deliberately a *separate* test binary (see tests/CMakeLists.txt) since it links libzmq and spins up
// a real TCP server thread for the whole suite.
//
// The servers of this file bind OS-assigned ports (StartZmqServerOnFreePort: bounded retry on a stolen port, a Ping
// as the positive "this server is up" signal). They are started once in SetUpTestSuite() and deliberately never torn
// down (their workers keep an XStore rooted in the scratch dir) -- the OS reclaims the socket/threads on process
// exit. Only the on-disk scratch directory is cleaned up in TearDownTestSuite().
#include "include/XStore/XStore_ReplyLimit.h"
#include <gtest/gtest.h>
#include <google/protobuf/io/coded_stream.h>
#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <future>
#include <map>
#include <thread>
#include <unistd.h>
#include <vector>

#include "include/Networking/ZMQ_Server.h"
#include "include/Networking/ZMQ_StreamFlow.h"
#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_ScanLock.h"
#include "XStoreTestSupport.h"
#include "XStoreZmqTestSupport.h"

using namespace xstore_test;

namespace {

// Spread across different years (same rationale as XStoreInsertQueryTest):
// avoids clustering all wire-protocol test data into a single year file.
constexpr unsigned long long TS_2005_MID = 1113825600ULL;  // 2005-04-18 12:00:00 UTC
constexpr unsigned long long TS_2012_MID = 1343995200ULL;  // 2012-08-03 12:00:00 UTC
constexpr unsigned long long TS_2020_MID = 1592222400ULL;  // 2020-06-15 12:00:00 UTC
constexpr unsigned long long TS_2028_MID = 1856260800ULL;  // 2028-10-27 12:00:00 UTC

// Wraps a payload into the Request envelope ZMQ_Server now expects on the
// wire (one oneof case per request type, see XStore.proto) -- overload
// resolution picks the right oneof setter from the argument type.
XStore_Proto::Request Wrap(const XStore_Proto::ReqCMD &req) {
    XStore_Proto::Request w;
    *w.mutable_info() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::CreateDB_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_createdb() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::RemoveDB_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_removedb() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::RemoveDBFiles_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_removedbfiles() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::UnaryInsert_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_unaryinsert() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::UnaryQuery_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_unaryquery() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::UnaryDelete_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_unarydelete() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::BatchInsert_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_batchinsert() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::BatchQuery_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_batchquery() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::RangeInsert_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_rangeinsert() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::RangeQuery_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_rangequery() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::BatchDelete_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_batchdelete() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::RangeDelete_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_rangedelete() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Min_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_min() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Max_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_max() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Sum_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_sum() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Avg_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_avg() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Variance_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_variance() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Stddev_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_stddev() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Exists_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_exists() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Ohlc_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_ohlc() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::Percentile_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_percentile() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::DistinctCount_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_distinctcount() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::TopK_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_topk() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::FindByValue_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_findbyvalue() = req;
    return w;
}
XStore_Proto::Request Wrap(const XStore_Proto::FindByPattern_REQ &req) {
    XStore_Proto::Request w;
    *w.mutable_findbypattern() = req;
    return w;
}

// Walks one reply frame of whole RangeQuery_REP.queryresponse entries (ts varint + data bytes) without building
// protobuf objects: a 64 MiB frame of ~6.7M entries would cost hundreds of MB as a parsed message. Calls
// visit(timestamp, data_bytes) per entry; false if the bytes are not a clean run of entries.
template <typename Visit>
bool ScanRangeFrame(const std::string &frame, Visit visit) {
    namespace io = google::protobuf::io;
    io::CodedInputStream in(reinterpret_cast<const uint8_t *>(frame.data()), static_cast<int>(frame.size()));
    in.SetTotalBytesLimit(INT_MAX);
    constexpr uint32_t ENTRY_TAG = 0x0A;      // queryresponse (1), length-delimited
    constexpr uint32_t TIMESTAMP_TAG = 0x08;  // UnaryQuery_REP.timestamp (1), varint
    constexpr uint32_t DATA_TAG = 0x12;       // UnaryQuery_REP.data (2), length-delimited
    while (in.CurrentPosition() < static_cast<int>(frame.size())) {
        uint32_t len = 0;
        uint64_t ts = 0;
        if (in.ReadTag() != ENTRY_TAG || !in.ReadVarint32(&len)) return false;
        const auto limit = in.PushLimit(static_cast<int>(len));
        if (in.ReadTag() != TIMESTAMP_TAG || !in.ReadVarint64(&ts)) return false;
        std::string data;
        if (in.BytesUntilLimit() > 0) {
            uint32_t data_len = 0;
            if (in.ReadTag() != DATA_TAG || !in.ReadVarint32(&data_len) || !in.ReadString(&data, static_cast<int>(data_len))) return false;
        }
        if (in.BytesUntilLimit() != 0) return false;
        in.PopLimit(limit);
        visit(ts, data);
    }
    return true;
}

}  // namespace

namespace {
constexpr size_t TWO_CHUNK_ENTRY_BYTES = 10;   // an empty (hole) entry of a RangeQuery reply
constexpr unsigned long long TWO_CHUNK_T0 = TS_2020_MID;
}  // namespace

// A database whose range [T0, T0 + last] makes a reply of just over one chunk (two frames): one real row at T0, hole entries between, and
// real rows on both sides of the cut that closes frame 0 (`cut`) and at the end.
struct TwoChunkRange {
    unsigned long long cut;
    unsigned long long last;
};

class XStoreZmqNetworkTest : public ::testing::Test {
 protected:
  static ScratchDir *scratch;
  static zmq::context_t *client_ctx;
  static zmq::socket_t *client_socket;
  static std::string server_addr;  // empty if no server came up

  static void SetUpTestSuite() {
      server_addr.clear();   // under --gtest_repeat a failed start must not leave the previous run's address
      // Leaked deliberately (see file header comment): the worker's XStore
      // instance captures this cwd as its baseDir at thread-start time and
      // keeps writing there for the rest of the process's life.
      scratch = new ScratchDir("xstore_zmq_test");

      const int port = StartZmqServerOnFreePort(1);  // single worker: client is a single REQ socket anyway
      if (port < 0) return;
      server_addr = "tcp://127.0.0.1:" + std::to_string(port);

      client_ctx = new zmq::context_t(1);
      client_socket = new zmq::socket_t(*client_ctx, zmq::socket_type::req);
      client_socket->set(zmq::sockopt::rcvtimeo, 5000);
      client_socket->set(zmq::sockopt::linger, 0);
      client_socket->connect(server_addr);
  }

  void SetUp() override { ASSERT_FALSE(server_addr.empty()) << "no ZMQ server came up on any of 5 OS-assigned ports"; }

  static void TearDownTestSuite() {
      delete client_socket;
      delete client_ctx;
      client_socket = nullptr;
      client_ctx = nullptr;
      delete scratch;  // removes the scratch_ DataStore dir; server thread is left dangling, harmless at process exit
  }

  template <typename RepT>
  RepT SendRequest(const XStore_Proto::Request &req) {
      std::string payload = req.SerializeAsString();
      auto send_result = client_socket->send(zmq::buffer(payload), zmq::send_flags::none);
      RepT rep;
      if (!send_result) {
          ADD_FAILURE() << "failed to send request over ZMQ";
          return rep;
      }
      zmq::message_t reply;
      auto recv_result = client_socket->recv(reply, zmq::recv_flags::none);
      if (!recv_result) {
          ADD_FAILURE() << "no reply received from ZMQ server within timeout";
          return rep;
      }
      rep.ParseFromArray(reply.data(), reply.size());
      return rep;
  }

  // Sends raw bytes and returns the reply frame's size, or -1 if no reply arrived within the timeout. Uses a fresh
  // REQ socket: a missed reply must not leave the shared client_socket stuck in its send state for every later case.
  long SendRawExpectReply(const std::string &payload) {
      zmq::socket_t sock(*client_ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, 5000);
      sock.set(zmq::sockopt::linger, 0);
      sock.connect(server_addr);
      if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return -1;
      zmq::message_t reply;
      if (!sock.recv(reply, zmq::recv_flags::none)) return -1;
      return static_cast<long>(reply.size());
  }

  // Sends a request and returns every frame of the (possibly multi-frame) reply, or false if a frame did not arrive in
  // `timeout_ms`. Fresh REQ socket, same reason as SendRawExpectReply.
  bool SendRequestMultipart(const XStore_Proto::Request &req, std::vector<std::string> &frames, int timeout_ms) {
      zmq::socket_t sock(*client_ctx, zmq::socket_type::req);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      sock.connect(server_addr);
      const std::string payload = req.SerializeAsString();
      if (!sock.send(zmq::buffer(payload), zmq::send_flags::none)) return false;
      frames.clear();
      while (true) {
          zmq::message_t frame;
          if (!sock.recv(frame, zmq::recv_flags::none)) return false;
          frames.emplace_back(static_cast<const char *>(frame.data()), frame.size());
          if (!frame.more()) return true;
      }
  }

  // A DEALER client: sends the empty delimiter a REQ would, and receives a reply as MESSAGES of [delimiter][frames...] until the one with
  // an empty delimiter. `request.streamok` is the caller's to set. Returns false if a message did not arrive in `timeout_ms`.
  bool SendRequestStreaming(const XStore_Proto::Request &req, std::vector<std::vector<std::string>> &messages, int timeout_ms) {
      zmq::socket_t sock(*client_ctx, zmq::socket_type::dealer);
      sock.set(zmq::sockopt::rcvtimeo, timeout_ms);
      sock.set(zmq::sockopt::linger, 0);
      sock.connect(server_addr);
      const std::string payload = req.SerializeAsString();
      if (!sock.send(zmq::message_t(), zmq::send_flags::sndmore) || !sock.send(zmq::buffer(payload), zmq::send_flags::none)) return false;
      messages.clear();
      while (true) {
          std::vector<std::string> message;
          while (true) {
              zmq::message_t frame;
              if (!sock.recv(frame, zmq::recv_flags::none)) return false;
              message.emplace_back(static_cast<const char *>(frame.data()), frame.size());
              if (!frame.more()) break;
          }
          messages.push_back(std::move(message));
          if (messages.back().front().empty()) return true;   // the reply's last message
      }
  }

  TwoChunkRange CreateTwoChunkRangeDb(const std::string &db);
  XStore_Proto::Request RangeRequest(const std::string &db, unsigned long long start, unsigned long long end);
};

ScratchDir *XStoreZmqNetworkTest::scratch = nullptr;
zmq::context_t *XStoreZmqNetworkTest::client_ctx = nullptr;
zmq::socket_t *XStoreZmqNetworkTest::client_socket = nullptr;
std::string XStoreZmqNetworkTest::server_addr;

// ---------------------------------------------------------------------
// INFO / CREATEDB
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, InfoQueryOverWireReflectsCreatedDatabase) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_INFO_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    auto create_rep = SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));
    EXPECT_EQ(create_rep.createdbstatus(), "[INFO] [CREATE DB] [DB: ZMQ_INFO_DB] - Created DB");

    XStore_Proto::ReqCMD info_req;
    info_req.set_rtype(XStore_Proto::INFO);
    auto info_rep = SendRequest<XStore_Proto::SysInfo_REP>(Wrap(info_req));

    bool found = false;
    for (int i = 0; i < info_rep.dbinfo_size(); i++) {
        if (info_rep.dbinfo(i).dbname() == "ZMQ_INFO_DB") found = true;
    }
    EXPECT_TRUE(found) << "expected ZMQ_INFO_DB to be reported by INFO over the wire";
}

// PING doesn't share Wrap()'s ReqCMD overload (ambiguous with INFO, same
// C++ type) -- build the envelope directly.
TEST_F(XStoreZmqNetworkTest, PingOverWireReturnsEmptyAckWithoutTouchingDataStore) {
    XStore_Proto::Request ping_req;
    ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
    auto ping_rep = SendRequest<XStore_Proto::Ping_REP>(ping_req);
    // Only assertion that matters: the round trip completed and parsed as
    // Ping_REP (SendRequest would ADD_FAILURE on send/recv timeout above).
    (void)ping_rep;
}

// The server answers a request it cannot act on with one empty frame, so the client's REQ socket never
// hangs (ZMQ_Server.cpp: PAYLOAD_NOT_SET / parse-failure paths).
TEST_F(XStoreZmqNetworkTest, EnvelopeWithNoPayloadGetsAnEmptyReply) {
    EXPECT_EQ(SendRawExpectReply(XStore_Proto::Request().SerializeAsString()), 0);
}

TEST_F(XStoreZmqNetworkTest, UnparseableBytesGetAnEmptyReplyAndServerKeepsServing) {
    EXPECT_EQ(SendRawExpectReply("\xff\xff\xff\xff"), 0);

    // Ping_REP serializes to zero bytes, so use a request with a non-empty reply to prove the worker survived.
    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_NOPE_AFTER_GARBAGE_DB");
    query_req.set_timestamp(TS_2005_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));
    EXPECT_FALSE(query_rep.querystatus().empty());
}

// ---------------------------------------------------------------------
// UNARY INSERT / QUERY / DELETE
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, UnaryInsertThenUnaryQueryRoundTripsDataOverWire) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_UNARY_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::UnaryInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::UNARYINSERT);
    insert_req.set_dbname("ZMQ_UNARY_DB");
    insert_req.set_timestamp(TS_2005_MID);
    // SYNC, not the wire default (ASYNC): this test's whole point is
    // "insert then query sees it immediately" -- ASYNC explicitly does not
    // guarantee that (the row may not be applied yet when the query runs
    // right after), which isn't a bug, just a different durability mode
    // than this test needs to make its own assertion meaningful.
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    *insert_req.mutable_data() = MakeRow({FDouble(2.71828), FString("wire-data"), FBool(true)});
    auto insert_rep = SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_UNARY_DB");
    query_req.set_timestamp(TS_2005_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));

    EXPECT_EQ(query_rep.timestamp(), TS_2005_MID);
    ASSERT_EQ(query_rep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 2.71828);
    EXPECT_EQ(query_rep.data().fielddata(1).stringdata(), "wire-data");
    EXPECT_EQ(query_rep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreZmqNetworkTest, UnaryQueryOverWireOnMissingDBReturnsError) {
    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_NOPE_DB");
    query_req.set_timestamp(TS_2005_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));
    EXPECT_EQ(query_rep.querystatus(), "[WARNING] [UNARY QUERY] [DB: ZMQ_NOPE_DB] - DB does not exist");
}

TEST_F(XStoreZmqNetworkTest, UnaryDeleteOverWirePunchesHoleThenQueryReturnsEmpty) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_DELETE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::UnaryInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::UNARYINSERT);
    insert_req.set_dbname("ZMQ_DELETE_DB");
    insert_req.set_timestamp(TS_2012_MID);
    // SYNC: the row must actually be applied before the delete below runs,
    // same reasoning as UnaryInsertThenUnaryQueryRoundTripsDataOverWire above.
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    *insert_req.mutable_data() = MakeRow({FString("to-be-deleted")});
    SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));

    XStore_Proto::UnaryDelete_REQ delete_req;
    delete_req.set_rtype(XStore_Proto::UNARYDELETE);
    delete_req.set_dbname("ZMQ_DELETE_DB");
    delete_req.set_timestamp(TS_2012_MID);
    auto delete_rep = SendRequest<XStore_Proto::UnaryDelete_REP>(Wrap(delete_req));
    EXPECT_NE(delete_rep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_DELETE_DB");
    query_req.set_timestamp(TS_2012_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));
    EXPECT_EQ(query_rep.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// BATCH INSERT / QUERY -- mixed column counts + holes, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, BatchInsertThenBatchQueryOverWireOmitsHoles) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_BATCH_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const unsigned long long ts1 = TS_2012_MID;      // 1 column
    const unsigned long long ts2 = TS_2020_MID;      // 2 columns, different year
    const unsigned long long ts_hole = TS_2012_MID + 50;  // never inserted

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_BATCH_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);

    XStore_Proto::BatchInsert_DATA *entry1 = insert_req.add_batchinsert_data();
    entry1->set_timestamp(ts1);
    *entry1->mutable_data() = MakeRow({FInt32(-55)});

    XStore_Proto::BatchInsert_DATA *entry2 = insert_req.add_batchinsert_data();
    entry2->set_timestamp(ts2);
    *entry2->mutable_data() = MakeRow({FUInt64(123456789ULL), FBytes("two-cols")});

    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    EXPECT_NE(insert_rep.insertstatus().find("Inserted 2 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::BATCHQUERY);
    query_req.set_dbname("ZMQ_BATCH_DB");
    query_req.add_timestamp(ts1);
    query_req.add_timestamp(ts_hole);
    query_req.add_timestamp(ts2);

    auto query_rep = SendRequest<XStore_Proto::BatchQuery_REP>(Wrap(query_req));

    ASSERT_EQ(query_rep.queryresponse_size(), 2);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> by_ts;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        by_ts[query_rep.queryresponse(i).timestamp()] = query_rep.queryresponse(i);
    }
    ASSERT_EQ(by_ts.count(ts_hole), 0u);
    ASSERT_EQ(by_ts[ts1].data().fielddata_size(), 1);
    EXPECT_EQ(by_ts[ts1].data().fielddata(0).int32data(), -55);
    ASSERT_EQ(by_ts[ts2].data().fielddata_size(), 2);
    EXPECT_EQ(by_ts[ts2].data().fielddata(0).uint64data(), 123456789ULL);
    EXPECT_EQ(by_ts[ts2].data().fielddata(1).bytesdata(), "two-cols");
}

// The worker offers a BatchInsert to the wire path first; a request it declines (here an unknown field, which protobuf accepts and
// keeps) must still be inserted by the normal parsed path, with the same reply and the rows readable.
TEST_F(XStoreZmqNetworkTest, BatchInsertWithAnUnknownFieldIsDeclinedByTheWirePathAndStillInserted) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_BATCH_UNKNOWN_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_BATCH_UNKNOWN_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2012_MID);
    *entry->mutable_data() = MakeRow({FInt32(77)});
    insert_req.GetReflection()->MutableUnknownFields(&insert_req)->AddVarint(100, 1);

    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    EXPECT_NE(insert_rep.insertstatus().find("Inserted 1 elements"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_BATCH_UNKNOWN_DB");
    query_req.set_timestamp(TS_2012_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).int32data(), 77);
}

// A request that starts like a BatchInsert but is cut short: the wire path declines it, the parse fails, the reply is empty and the
// worker keeps serving (the same outcome as any other unparseable bytes).
TEST_F(XStoreZmqNetworkTest, TruncatedBatchInsertGetsAnEmptyReplyAndServerKeepsServing) {
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_NOPE_AFTER_TRUNCATED_DB");
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2012_MID);
    *entry->mutable_data() = MakeRow({FInt32(1), FInt32(2), FInt32(3)});
    const std::string bytes = Wrap(insert_req).SerializeAsString();
    EXPECT_EQ(SendRawExpectReply(bytes.substr(0, bytes.size() - 3)), 0);

    XStore_Proto::UnaryQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::UNARYQUERY);
    query_req.set_dbname("ZMQ_NOPE_AFTER_TRUNCATED_DB");
    query_req.set_timestamp(TS_2005_MID);
    auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(Wrap(query_req));
    EXPECT_FALSE(query_rep.querystatus().empty());
}

// ---------------------------------------------------------------------
// RANGE INSERT / QUERY -- contiguous range with an internal hole, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RangeInsertThenRangeQueryOverWireIncludesHoleAsEmptyEntry) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_RANGE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    // Sparse range insert: endpoints only, leaving a hole at TS_2028_MID + 1.
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_rtype(XStore_Proto::RANGEINSERT);
    insert_req.set_dbname("ZMQ_RANGE_DB");
    XStore_Proto::RangeInsert_DATA *first = insert_req.add_rangeinsert_data();
    first->set_timestamp(TS_2028_MID);
    *first->mutable_data() = MakeRow({FSFixed64(-42)});
    XStore_Proto::RangeInsert_DATA *last = insert_req.add_rangeinsert_data();
    last->set_timestamp(TS_2028_MID + 2);
    *last->mutable_data() = MakeRow({FSFixed64(42)});
    SendRequest<XStore_Proto::RangeInsert_REP>(Wrap(insert_req));

    XStore_Proto::RangeQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::RANGEQUERY);
    query_req.set_dbname("ZMQ_RANGE_DB");
    query_req.set_starttime(TS_2028_MID);
    query_req.set_endtime(TS_2028_MID + 2);

    auto query_rep = SendRequest<XStore_Proto::RangeQuery_REP>(Wrap(query_req));

    ASSERT_EQ(query_rep.queryresponse_size(), 3);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).sfixed64data(), -42);
    EXPECT_EQ(query_rep.queryresponse(1).data().fielddata_size(), 0) << "hole in the middle of the range";
    EXPECT_EQ(query_rep.queryresponse(2).data().fielddata(0).sfixed64data(), 42);
}

// ---------------------------------------------------------------------
// MIN / MAX / SUM / AVG -- dispatch + encoding of the four basic aggregates
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, MinMaxSumAvgRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_BASICAGG_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const std::vector<double> values = {2, 9, 4, 1, 5};  // min 1, max 9, sum 21, avg 4.2
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_BASICAGG_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(TS_2020_MID + i);
        *entry->mutable_data() = MakeRow({FDouble(values[i])});
    }
    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    ASSERT_NE(insert_rep.insertstatus().find("Inserted 5 elements"), std::string::npos) << insert_rep.insertstatus();

    const auto fill = [&](auto &req, XStore_Proto::RequestType type) {
        req.set_rtype(type);
        req.set_dbname("ZMQ_BASICAGG_DB");
        req.set_starttime(TS_2020_MID);
        req.set_endtime(TS_2020_MID + values.size() - 1);
        req.set_columnidx(0);
    };

    XStore_Proto::Min_REQ min_req;
    fill(min_req, XStore_Proto::MIN);
    auto min_rep = SendRequest<XStore_Proto::Min_REP>(Wrap(min_req));
    EXPECT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REQ max_req;
    fill(max_req, XStore_Proto::MAX);
    auto max_rep = SendRequest<XStore_Proto::Max_REP>(Wrap(max_req));
    EXPECT_EQ(max_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 9.0);

    XStore_Proto::Sum_REQ sum_req;
    fill(sum_req, XStore_Proto::SUM);
    auto sum_rep = SendRequest<XStore_Proto::Sum_REP>(Wrap(sum_req));
    EXPECT_EQ(sum_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 21.0);

    XStore_Proto::Avg_REQ avg_req;
    fill(avg_req, XStore_Proto::AVG);
    auto avg_rep = SendRequest<XStore_Proto::Avg_REP>(Wrap(avg_req));
    EXPECT_EQ(avg_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), 4.2);
}

// ---------------------------------------------------------------------
// BATCH DELETE / RANGE DELETE -- dispatch, each followed by a query proving the effect
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, BatchDeleteOverWireRemovesOnlyTheListedRows) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_BATCHDEL_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const unsigned long long ts_a = TS_2012_MID, ts_b = TS_2012_MID + 1, ts_c = TS_2020_MID;  // a, c deleted; two years
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_BATCHDEL_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    int64_t value = 1;
    for (unsigned long long ts : {ts_a, ts_b, ts_c}) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FInt64(value++)});
    }
    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    ASSERT_NE(insert_rep.insertstatus().find("Inserted 3 elements"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::BatchDelete_REQ delete_req;
    delete_req.set_rtype(XStore_Proto::BATCHDELETE);
    delete_req.set_dbname("ZMQ_BATCHDEL_DB");
    delete_req.add_timestamp(ts_a);
    delete_req.add_timestamp(ts_c);
    auto delete_rep = SendRequest<XStore_Proto::BatchDelete_REP>(Wrap(delete_req));
    EXPECT_EQ(delete_rep.deletestatus(), "[INFO] [BATCH DELETE] [DB: ZMQ_BATCHDEL_DB] - Deleted 2 items");

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::BATCHQUERY);
    query_req.set_dbname("ZMQ_BATCHDEL_DB");
    query_req.add_timestamp(ts_a);
    query_req.add_timestamp(ts_b);
    query_req.add_timestamp(ts_c);
    auto query_rep = SendRequest<XStore_Proto::BatchQuery_REP>(Wrap(query_req));
    ASSERT_EQ(query_rep.queryresponse_size(), 1) << "batch query omits holes, so only the undeleted row remains";
    EXPECT_EQ(query_rep.queryresponse(0).timestamp(), ts_b);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).int64data(), 2);
}

TEST_F(XStoreZmqNetworkTest, RangeDeleteOverWireRemovesOnlyTheRange) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_RANGEDEL_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_RANGEDEL_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (int64_t i = 0; i < 4; i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(TS_2028_MID + i);
        *entry->mutable_data() = MakeRow({FInt64(100 + i)});
    }
    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    ASSERT_NE(insert_rep.insertstatus().find("Inserted 4 elements"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::RangeDelete_REQ delete_req;
    delete_req.set_rtype(XStore_Proto::RANGEDELETE);
    delete_req.set_dbname("ZMQ_RANGEDEL_DB");
    delete_req.set_starttime(TS_2028_MID + 1);
    delete_req.set_endtime(TS_2028_MID + 2);
    auto delete_rep = SendRequest<XStore_Proto::RangeDelete_REP>(Wrap(delete_req));
    EXPECT_EQ(delete_rep.deletestatus(), "[INFO] [RANGE DELETE] [DB: ZMQ_RANGEDEL_DB] - Deleted 2 items");

    XStore_Proto::RangeQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::RANGEQUERY);
    query_req.set_dbname("ZMQ_RANGEDEL_DB");
    query_req.set_starttime(TS_2028_MID);
    query_req.set_endtime(TS_2028_MID + 3);
    auto query_rep = SendRequest<XStore_Proto::RangeQuery_REP>(Wrap(query_req));
    ASSERT_EQ(query_rep.queryresponse_size(), 4);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).int64data(), 100);
    EXPECT_EQ(query_rep.queryresponse(1).data().fielddata_size(), 0);
    EXPECT_EQ(query_rep.queryresponse(2).data().fielddata_size(), 0);
    EXPECT_EQ(query_rep.queryresponse(3).data().fielddata(0).int64data(), 103);
}

// ---------------------------------------------------------------------
// REMOVE DB FILES / REMOVE DB -- cleanup path, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RemoveDBFilesThenRemoveDBOverWire) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_REMOVE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::UnaryInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::UNARYINSERT);
    insert_req.set_dbname("ZMQ_REMOVE_DB");
    insert_req.set_timestamp(TS_2005_MID);
    // SYNC: RemoveDBFiles below needs 2005.XSTORE to actually exist on disk
    // yet -- ApplyWalRecord is what creates it, and under the wire default
    // (ASYNC) that may not have happened yet.
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    *insert_req.mutable_data() = MakeRow({FBool(false)});
    SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));

    XStore_Proto::RemoveDBFiles_REQ remove_files_req;
    remove_files_req.set_rtype(XStore_Proto::REMOVEDBFILES);
    remove_files_req.set_dbname("ZMQ_REMOVE_DB");
    remove_files_req.add_filename("2005.XSTORE");
    auto remove_files_rep = SendRequest<XStore_Proto::RemoveDBFiles_REP>(Wrap(remove_files_req));
    ASSERT_EQ(remove_files_rep.removestatus_size(), 3); // 2005.XSTORE plus its .zmap and .catidx
    EXPECT_NE(remove_files_rep.removestatus(0).find("[INFO]"), std::string::npos);

    XStore_Proto::RemoveDB_REQ remove_db_req;
    remove_db_req.set_rtype(XStore_Proto::REMOVEDB);
    remove_db_req.set_dbname("ZMQ_REMOVE_DB");
    auto remove_db_rep = SendRequest<XStore_Proto::RemoveDB_REP>(Wrap(remove_db_req));
    EXPECT_EQ(remove_db_rep.removestatus(), "[INFO] [REMOVE DB] [DB: ZMQ_REMOVE_DB] - Removed DB with name: ZMQ_REMOVE_DB");
}

// ---------------------------------------------------------------------
// VARIANCE / STDDEV AGGREGATES -- over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, VarianceAndStddevRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_VARSTDDEV_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const std::vector<double> values = {2, 4, 4, 4, 5, 5, 7, 9};
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::UnaryInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::UNARYINSERT);
        insert_req.set_dbname("ZMQ_VARSTDDEV_DB");
        insert_req.set_timestamp(TS_2020_MID + i);
        // SYNC: the aggregate queries below need every row actually applied
        // first, same reasoning as the other round-trip tests in this file.
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        *insert_req.mutable_data() = MakeRow({FDouble(values[i])});
        SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    }

    XStore_Proto::Variance_REQ variance_req;
    variance_req.set_rtype(XStore_Proto::VARIANCE);
    variance_req.set_dbname("ZMQ_VARSTDDEV_DB");
    variance_req.set_starttime(TS_2020_MID);
    variance_req.set_endtime(TS_2020_MID + values.size() - 1);
    variance_req.set_columnidx(0);
    auto variance_rep = SendRequest<XStore_Proto::Variance_REP>(Wrap(variance_req));
    EXPECT_EQ(variance_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(variance_rep.opresult(), 32.0 / 7.0, 1e-6);

    XStore_Proto::Stddev_REQ stddev_req;
    stddev_req.set_rtype(XStore_Proto::STDDEV);
    stddev_req.set_dbname("ZMQ_VARSTDDEV_DB");
    stddev_req.set_starttime(TS_2020_MID);
    stddev_req.set_endtime(TS_2020_MID + values.size() - 1);
    stddev_req.set_columnidx(0);
    auto stddev_rep = SendRequest<XStore_Proto::Stddev_REP>(Wrap(stddev_req));
    EXPECT_EQ(stddev_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(stddev_rep.opresult(), std::sqrt(32.0 / 7.0), 1e-6);
}

// ---------------------------------------------------------------------
// EXISTS AGGREGATE -- over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, ExistsRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_EXISTS_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    XStore_Proto::UnaryInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::UNARYINSERT);
    insert_req.set_dbname("ZMQ_EXISTS_DB");
    insert_req.set_timestamp(TS_2020_MID);
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    *insert_req.mutable_data() = MakeRow({FDouble(1.0)});
    SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));

    XStore_Proto::Exists_REQ exists_true_req;
    exists_true_req.set_rtype(XStore_Proto::EXISTS);
    exists_true_req.set_dbname("ZMQ_EXISTS_DB");
    exists_true_req.set_starttime(TS_2020_MID);
    exists_true_req.set_endtime(TS_2020_MID);
    exists_true_req.set_columnidx(0);
    auto exists_true_rep = SendRequest<XStore_Proto::Exists_REP>(Wrap(exists_true_req));
    EXPECT_EQ(exists_true_rep.opstatus(), "SUCCESS");
    EXPECT_TRUE(exists_true_rep.exists());

    XStore_Proto::Exists_REQ exists_false_req;
    exists_false_req.set_rtype(XStore_Proto::EXISTS);
    exists_false_req.set_dbname("ZMQ_EXISTS_DB");
    exists_false_req.set_starttime(TS_2020_MID + 100);
    exists_false_req.set_endtime(TS_2020_MID + 200);
    exists_false_req.set_columnidx(0);
    auto exists_false_rep = SendRequest<XStore_Proto::Exists_REP>(Wrap(exists_false_req));
    EXPECT_EQ(exists_false_rep.opstatus(), "SUCCESS");
    EXPECT_FALSE(exists_false_rep.exists());
}

// ---------------------------------------------------------------------
// OHLC AGGREGATE -- over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, OhlcRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_OHLC_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const std::vector<double> values = {2, 9, 4, 1, 5};
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::UnaryInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::UNARYINSERT);
        insert_req.set_dbname("ZMQ_OHLC_DB");
        insert_req.set_timestamp(TS_2020_MID + i);
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        *insert_req.mutable_data() = MakeRow({FDouble(values[i])});
        SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    }

    XStore_Proto::Ohlc_REQ ohlc_req;
    ohlc_req.set_rtype(XStore_Proto::OHLC);
    ohlc_req.set_dbname("ZMQ_OHLC_DB");
    ohlc_req.set_starttime(TS_2020_MID);
    ohlc_req.set_endtime(TS_2020_MID + values.size() - 1);
    ohlc_req.set_columnidx(0);
    auto ohlc_rep = SendRequest<XStore_Proto::Ohlc_REP>(Wrap(ohlc_req));
    EXPECT_EQ(ohlc_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(ohlc_rep.open(), 2.0);
    EXPECT_DOUBLE_EQ(ohlc_rep.high(), 9.0);
    EXPECT_DOUBLE_EQ(ohlc_rep.low(), 1.0);
    EXPECT_DOUBLE_EQ(ohlc_rep.close(), 5.0);
}

TEST_F(XStoreZmqNetworkTest, PercentileDistinctCountTopKRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_EXACTMODE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    // {1,2,2,3} -- distinct=3, mode=2 (count 2), median(0.5)=2.0.
    const std::vector<double> values = {1, 2, 2, 3};
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::UnaryInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::UNARYINSERT);
        insert_req.set_dbname("ZMQ_EXACTMODE_DB");
        insert_req.set_timestamp(TS_2020_MID + i);
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        *insert_req.mutable_data() = MakeRow({FDouble(values[i])});
        SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    }

    XStore_Proto::Percentile_REQ pct_req;
    pct_req.set_rtype(XStore_Proto::PERCENTILE);
    pct_req.set_dbname("ZMQ_EXACTMODE_DB");
    pct_req.set_starttime(TS_2020_MID);
    pct_req.set_endtime(TS_2020_MID + values.size() - 1);
    pct_req.set_columnidx(0);
    pct_req.set_percentile(0.5);
    auto pct_rep = SendRequest<XStore_Proto::Percentile_REP>(Wrap(pct_req));
    EXPECT_EQ(pct_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(pct_rep.value(), 2.0);

    XStore_Proto::DistinctCount_REQ dc_req;
    dc_req.set_rtype(XStore_Proto::DISTINCTCOUNT);
    dc_req.set_dbname("ZMQ_EXACTMODE_DB");
    dc_req.set_starttime(TS_2020_MID);
    dc_req.set_endtime(TS_2020_MID + values.size() - 1);
    dc_req.set_columnidx(0);
    auto dc_rep = SendRequest<XStore_Proto::DistinctCount_REP>(Wrap(dc_req));
    EXPECT_EQ(dc_rep.opstatus(), "SUCCESS");
    EXPECT_EQ(dc_rep.count(), 3u);

    XStore_Proto::TopK_REQ topk_req;
    topk_req.set_rtype(XStore_Proto::TOPK);
    topk_req.set_dbname("ZMQ_EXACTMODE_DB");
    topk_req.set_starttime(TS_2020_MID);
    topk_req.set_endtime(TS_2020_MID + values.size() - 1);
    topk_req.set_columnidx(0);
    topk_req.set_k(1);
    auto topk_rep = SendRequest<XStore_Proto::TopK_REP>(Wrap(topk_req));
    EXPECT_EQ(topk_rep.opstatus(), "SUCCESS");
    ASSERT_EQ(topk_rep.entries_size(), 1);
    EXPECT_DOUBLE_EQ(topk_rep.entries(0).value(), 2.0);
    EXPECT_EQ(topk_rep.entries(0).count(), 2u);
}

// ---------------------------------------------------------------------
// FIND BY VALUE -- categorical equality lookup, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, FindByValueRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_FINDBYVALUE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    for (int64_t i = 0; i < 50; ++i) {
        XStore_Proto::UnaryInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::UNARYINSERT);
        insert_req.set_dbname("ZMQ_FINDBYVALUE_DB");
        insert_req.set_timestamp(TS_2020_MID + i);
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        *insert_req.mutable_data() = MakeRow({FInt64(i % 3)});
        SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    }

    XStore_Proto::FindByValue_REQ find_req;
    find_req.set_rtype(XStore_Proto::FINDBYVALUE);
    find_req.set_dbname("ZMQ_FINDBYVALUE_DB");
    find_req.set_starttime(TS_2020_MID);
    find_req.set_endtime(TS_2020_MID + 49);
    find_req.set_columnidx(0);
    *find_req.mutable_value() = FInt64(1);

    auto find_rep = SendRequest<XStore_Proto::FindByValue_REP>(Wrap(find_req));
    ASSERT_GT(find_rep.queryresponse_size(), 0);
    for (const auto &entry : find_rep.queryresponse()) {
        EXPECT_EQ(static_cast<int64_t>(entry.timestamp() - TS_2020_MID) % 3, 1);
    }
}

TEST_F(XStoreZmqNetworkTest, FindByPatternRoundTripOverNetwork) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_FINDBYPATTERN_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const char *const symbols[] = {"AAPL", "MSFT", "GOOG"};
    for (int64_t i = 0; i < 30; ++i) {
        XStore_Proto::UnaryInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::UNARYINSERT);
        insert_req.set_dbname("ZMQ_FINDBYPATTERN_DB");
        insert_req.set_timestamp(TS_2020_MID + i);
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        *insert_req.mutable_data() = MakeRow({FString(symbols[i % 3])});
        SendRequest<XStore_Proto::UnaryInsert_REP>(Wrap(insert_req));
    }

    XStore_Proto::FindByPattern_REQ find_req;
    find_req.set_rtype(XStore_Proto::FINDBYPATTERN);
    find_req.set_dbname("ZMQ_FINDBYPATTERN_DB");
    find_req.set_starttime(TS_2020_MID);
    find_req.set_endtime(TS_2020_MID + 29);
    find_req.set_columnidx(0);
    find_req.set_pattern("%sf%");
    find_req.set_mode(XStore_Proto::PATTERN_LIKE);
    find_req.set_caseinsensitive(true);

    auto find_rep = SendRequest<XStore_Proto::FindByPattern_REP>(Wrap(find_req));
    EXPECT_FALSE(find_rep.has_querystatus()) << find_rep.querystatus();
    ASSERT_EQ(find_rep.queryresponse_size(), 10);
    for (const auto &entry : find_rep.queryresponse()) {
        EXPECT_EQ((entry.timestamp() - TS_2020_MID) % 3, 1u);
        EXPECT_EQ(entry.data().fielddata(0).stringdata(), "MSFT");
    }
}

// ---------------------------------------------------------------------
// Streamed reply past STREAMED_QUERY_CHUNK_SIZE_BYTES (64 MiB): one logical reply, several ZMQ frames
// ---------------------------------------------------------------------

// A RangeQuery emits one entry per timestamp, holes included, and a hole entry is exactly 10 bytes (4 tag/length bytes,
// a 5-byte timestamp varint for 2020, a 1-byte empty data length). So a mostly-empty range with 16-byte rows (tiny
// sparse year file, a few KB on disk) reaches the 64 MiB chunk limit after ~6.7M entries, with no big data written.
// The server flushes a chunk right after the first entry that brings the buffer to >= the limit, so the cut index is
// computable: rows are planted on both sides of it, and the test checks the frame split falls exactly there and that
// timestamps run contiguously across it.
TwoChunkRange XStoreZmqNetworkTest::CreateTwoChunkRangeDb(const std::string &db) {
    constexpr size_t CHUNK = XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES;
    constexpr unsigned long long T0 = TWO_CHUNK_T0;

    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname(db);
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(16);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const auto insert_rows = [&](const std::vector<std::pair<unsigned long long, int64_t>> &rows) {
        XStore_Proto::BatchInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::BATCHINSERT);
        insert_req.set_dbname(db);
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[ts, value] : rows) {
            XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
            entry->set_timestamp(ts);
            *entry->mutable_data() = MakeRow({FInt64(value)});
        }
        auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
        EXPECT_NE(insert_rep.insertstatus().find("Inserted"), std::string::npos) << insert_rep.insertstatus();
    };

    // Entry 0 (a real row) is the only one whose size is not TWO_CHUNK_ENTRY_BYTES: measure it with a one-entry query.
    insert_rows({{T0, 7}});
    std::vector<std::string> frames;
    EXPECT_TRUE(SendRequestMultipart(RangeRequest(db, T0, T0), frames, 10000));
    EXPECT_EQ(frames.size(), 1u);
    const size_t first_entry_bytes = frames.empty() ? 0 : frames[0].size();
    // Smallest j with first_entry_bytes + 10 * j >= CHUNK: the entry at offset j closes frame 0.
    const unsigned long long cut = (CHUNK - first_entry_bytes + TWO_CHUNK_ENTRY_BYTES - 1) / TWO_CHUNK_ENTRY_BYTES;
    const unsigned long long last = cut + 1000;
    insert_rows({{T0 + cut, 8}, {T0 + cut + 1, 9}, {T0 + last, 10}});
    return {cut, last};
}

XStore_Proto::Request XStoreZmqNetworkTest::RangeRequest(const std::string &db, unsigned long long start, unsigned long long end) {
    XStore_Proto::RangeQuery_REQ query_req;
    query_req.set_rtype(XStore_Proto::RANGEQUERY);
    query_req.set_dbname(db);
    query_req.set_starttime(start);
    query_req.set_endtime(end);
    return Wrap(query_req);
}

TEST_F(XStoreZmqNetworkTest, RangeQueryReplyLargerThanOneChunkArrivesAsContiguousFrames) {
    constexpr size_t CHUNK = XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES;
    constexpr unsigned long long T0 = TWO_CHUNK_T0;
    const TwoChunkRange range = CreateTwoChunkRangeDb("ZMQ_BIGRANGE_DB");
    const unsigned long long cut = range.cut, last = range.last;
    const auto range_request = [&](unsigned long long start, unsigned long long end) { return RangeRequest("ZMQ_BIGRANGE_DB", start, end); };
    std::vector<std::string> frames;

    const auto started = std::chrono::steady_clock::now();
    ASSERT_TRUE(SendRequestMultipart(range_request(T0, T0 + last), frames, 60000)) << "a frame did not arrive in 60 s";
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started).count();
    RecordProperty("elapsed_ms", static_cast<int>(elapsed_ms));
    ASSERT_EQ(frames.size(), 2u) << "total reply is just over one chunk: exactly two frames";
    EXPECT_GE(frames[0].size(), CHUNK);

    unsigned long long expected_ts = T0;
    size_t entries[2] = {0, 0};
    std::map<unsigned long long, int64_t> rows_seen;
    bool contiguous = true;
    for (int f = 0; f < 2; ++f) {
        ASSERT_TRUE(ScanRangeFrame(frames[f], [&](uint64_t ts, const std::string &data) {
            contiguous = contiguous && ts == expected_ts;
            ++expected_ts;
            ++entries[f];
            if (!data.empty()) {
                XStore_Proto::RowData row;
                ASSERT_TRUE(row.ParseFromString(data));
                rows_seen[ts] = row.fielddata(0).int64data();
            }
        })) << "frame " << f << " is not a clean run of whole entries";
    }
    EXPECT_TRUE(contiguous) << "timestamps must run T0..T0+last without a gap or repeat across the frame boundary";
    EXPECT_EQ(entries[0] + entries[1], last + 1);
    EXPECT_EQ(entries[0], cut + 1) << "frame 0 must end exactly at the entry that reached the chunk limit";
    const std::map<unsigned long long, int64_t> expected_rows = {{T0, 7}, {T0 + cut, 8}, {T0 + cut + 1, 9}, {T0 + last, 10}};
    EXPECT_EQ(rows_seen, expected_rows) << "rows on both sides of the frame boundary (8 closes frame 0, 9 opens frame 1)";
}

// A request with streamOk set may get its reply as separate MESSAGES, one per chunk, each [delimiter][chunk] with a non-empty delimiter
// except the last (empty, like a REQ socket's): a client can use a chunk while the next is still being produced. Same bytes as the
// single multipart message every other request gets.
TEST_F(XStoreZmqNetworkTest, ARangeQueryThatAllowsStreamingArrivesAsOneMessagePerChunk) {
    constexpr unsigned long long T0 = TWO_CHUNK_T0;
    constexpr size_t CHUNK = XStoreTunables::STREAMED_REPLY_CHUNK_SIZE_BYTES;
    const TwoChunkRange range = CreateTwoChunkRangeDb("ZMQ_STREAM_DB");
    XStore_Proto::Request request = RangeRequest("ZMQ_STREAM_DB", T0, T0 + range.last);

    std::vector<std::string> legacy;
    ASSERT_TRUE(SendRequestMultipart(request, legacy, 60000));
    ASSERT_EQ(legacy.size(), 2u);
    std::string legacy_bytes;
    for (const std::string &frame : legacy) legacy_bytes += frame;

    request.set_streamok(true);
    std::vector<std::vector<std::string>> messages;
    ASSERT_TRUE(SendRequestStreaming(request, messages, 60000));
    ASSERT_GT(messages.size(), 2u) << "the reply is cut into chunks of about " << CHUNK << " bytes, not the 64 MiB of a multipart reply";

    std::string streamed_bytes;
    for (size_t i = 0; i < messages.size(); ++i) {
        ASSERT_EQ(messages[i].size(), 2u) << "message " << i << ": [delimiter][one chunk]";
        const bool last = i + 1 == messages.size();
        EXPECT_EQ(messages[i][0].empty(), last) << "only the last message has the empty delimiter (message " << i << ")";
        if (!last) EXPECT_GE(messages[i][1].size(), CHUNK) << "a chunk closes when it has reached the chunk size (message " << i << ")";
        streamed_bytes += messages[i][1];
    }
    EXPECT_TRUE(streamed_bytes == legacy_bytes) << "the same bytes as the multipart reply, in the same order";
}

// The 2 GB guard stays for a request without streamOk (that client joins the frames into one protobuf message), and is skipped for one with it.
TEST_F(XStoreZmqNetworkTest, ARangeQueryOverTwoGBIsRefusedUnlessTheClientAllowsStreaming) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_2GB_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(32000);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));
    XStore_Proto::BatchInsert_REQ insert_req;   // the year's file must exist
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_2GB_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TWO_CHUNK_T0);
    *entry->mutable_data() = MakeRow({FInt64(7)});
    EXPECT_NE(SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req)).insertstatus().find("Inserted"), std::string::npos);

    constexpr unsigned long long T0 = TWO_CHUNK_T0;
    XStore_Proto::Request request = RangeRequest("ZMQ_2GB_DB", T0, T0 + 63000);   // 63000 * 32000 > 2,000,000,000

    std::vector<std::string> frames;
    ASSERT_TRUE(SendRequestMultipart(request, frames, 30000));
    XStore_Proto::RangeQuery_REP refused;
    ASSERT_TRUE(refused.ParseFromString(frames.at(0)));
    EXPECT_NE(refused.querystatus().find("larger than 2GB"), std::string::npos);

    request.set_streamok(true);
    std::vector<std::vector<std::string>> messages;
    ASSERT_TRUE(SendRequestStreaming(request, messages, 30000));
    std::string bytes;
    for (const auto &message : messages) bytes += message.at(1);
    XStore_Proto::RangeQuery_REP served;
    ASSERT_TRUE(served.ParseFromString(bytes));
    EXPECT_FALSE(served.has_querystatus()) << served.querystatus();
    EXPECT_EQ(served.queryresponse_size(), 63001);
}

// FindByValue stops at the reply limit for a client that joins the chunks and not for one that allows streaming (limit lowered to 8 KB).
TEST_F(XStoreZmqNetworkTest, AFindByValueReplyIsCutAtTheLimitUnlessTheClientAllowsStreaming) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_FBV_LIMIT_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(64);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_FBV_LIMIT_DB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (int64_t i = 0; i < 600; ++i) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(TWO_CHUNK_T0 + static_cast<unsigned long long>(i));
        *entry->mutable_data() = MakeRow({FInt64(7), FInt64(i)});
    }
    EXPECT_NE(SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req)).insertstatus().find("Inserted"), std::string::npos);

    XStore_Proto::FindByValue_REQ find_req;
    find_req.set_rtype(XStore_Proto::FINDBYVALUE);
    find_req.set_dbname("ZMQ_FBV_LIMIT_DB");
    find_req.set_starttime(TWO_CHUNK_T0);
    find_req.set_endtime(TWO_CHUNK_T0 + 599);
    find_req.set_columnidx(0);
    *find_req.mutable_value() = FInt64(7);
    XStore_Proto::Request request = Wrap(find_req);

    const unsigned long long old_limit = xstore::ReplyLimitBytes().exchange(8000);   // the server runs in this process
    std::vector<std::string> frames;
    const bool got = SendRequestMultipart(request, frames, 30000);
    request.set_streamok(true);
    std::vector<std::vector<std::string>> messages;
    const bool got_streamed = SendRequestStreaming(request, messages, 30000);
    xstore::ReplyLimitBytes().store(old_limit);
    ASSERT_TRUE(got);
    ASSERT_TRUE(got_streamed);

    std::string joined;
    for (const std::string &frame : frames) joined += frame;
    XStore_Proto::FindByValue_REP truncated;
    ASSERT_TRUE(truncated.ParseFromString(joined));
    EXPECT_LT(truncated.queryresponse_size(), 600);
    EXPECT_NE(truncated.querystatus().find("truncated"), std::string::npos);

    std::string streamed_bytes;
    for (const auto &message : messages) streamed_bytes += message.at(1);
    XStore_Proto::FindByValue_REP whole;
    ASSERT_TRUE(whole.ParseFromString(streamed_bytes));
    EXPECT_EQ(whole.queryresponse_size(), 600);
    EXPECT_FALSE(whole.has_querystatus()) << whole.querystatus();
}

TEST_F(XStoreZmqNetworkTest, RepliesThatAreOneFrameAreUnchangedWhenStreamingIsAllowed) {
    constexpr unsigned long long T0 = TWO_CHUNK_T0;
    (void)CreateTwoChunkRangeDb("ZMQ_STREAM_ONE_DB");
    XStore_Proto::Request ping;
    ping.mutable_ping()->set_rtype(XStore_Proto::PING);
    for (XStore_Proto::Request request : {RangeRequest("ZMQ_STREAM_ONE_DB", T0, T0 + 5), ping}) {
        std::vector<std::string> legacy;
        ASSERT_TRUE(SendRequestMultipart(request, legacy, 10000));
        ASSERT_EQ(legacy.size(), 1u);
        request.set_streamok(true);
        std::vector<std::vector<std::string>> messages;
        ASSERT_TRUE(SendRequestStreaming(request, messages, 10000));
        ASSERT_EQ(messages.size(), 1u);
        ASSERT_EQ(messages[0].size(), 2u);
        EXPECT_TRUE(messages[0][0].empty());
        EXPECT_TRUE(messages[0][1] == legacy[0]);
    }
}

// ---------------------------------------------------------------------
// Multi-worker concurrency -- XStoreZmqNetworkTest above always runs a
// single-worker server against one sequential REQ client, so it never
// exercises ZMQ_Server's actual thread pool (each worker owns its own
// XStore instance sharing XStore's static CfgMap/MmapLru, per
// XStoreTest's Concurrent* tests) or multiple simultaneous clients
// contending on the ROUTER/DEALER proxy. This fixture spins up its own
// multi-worker server on a separate port specifically to cover that gap.
// ---------------------------------------------------------------------

class XStoreZmqConcurrentTest : public ::testing::Test {
 protected:
  static ScratchDir *scratch;
  static std::string server_addr;  // empty if no server came up

  static void SetUpTestSuite() {
      server_addr.clear();
      // Leaked deliberately, same rationale as XStoreZmqNetworkTest.
      scratch = new ScratchDir("xstore_zmq_concurrent_test");

      const int port = StartZmqServerOnFreePort(4);  // multi-worker pool, unlike XStoreZmqNetworkTest's single worker
      if (port >= 0) server_addr = "tcp://127.0.0.1:" + std::to_string(port);
  }

  void SetUp() override { ASSERT_FALSE(server_addr.empty()) << "no ZMQ server came up on any of 5 OS-assigned ports"; }

  static void TearDownTestSuite() {
      delete scratch;  // server thread pool left dangling, harmless at process exit
  }

  // Each client thread below owns its own context/socket -- ZMQ sockets
  // aren't safe to share across threads.
  template <typename RepT>
  static RepT SendRequest(zmq::socket_t &socket, const XStore_Proto::Request &req) {
      std::string payload = req.SerializeAsString();
      auto send_result = socket.send(zmq::buffer(payload), zmq::send_flags::none);
      RepT rep;
      if (!send_result) {
          ADD_FAILURE() << "failed to send request over ZMQ";
          return rep;
      }
      zmq::message_t reply;
      auto recv_result = socket.recv(reply, zmq::recv_flags::none);
      if (!recv_result) {
          ADD_FAILURE() << "no reply received from ZMQ server within timeout";
          return rep;
      }
      rep.ParseFromArray(reply.data(), reply.size());
      return rep;
  }

  static zmq::socket_t MakeClientSocket(zmq::context_t &ctx) {
      zmq::socket_t socket(ctx, zmq::socket_type::req);
      socket.set(zmq::sockopt::rcvtimeo, 5000);
      socket.set(zmq::sockopt::linger, 0);
      socket.connect(server_addr);
      return socket;
  }
};

ScratchDir *XStoreZmqConcurrentTest::scratch = nullptr;
std::string XStoreZmqConcurrentTest::server_addr;

TEST_F(XStoreZmqConcurrentTest, ConcurrentClientsUnaryInsertAndQueryOverWireDoNotCorruptData) {
    zmq::context_t setup_ctx(1);
    zmq::socket_t setup_socket = MakeClientSocket(setup_ctx);

    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_CONCURRENT_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    auto create_rep = SendRequest<XStore_Proto::CreateDB_REP>(setup_socket, Wrap(create_req));
    ASSERT_NE(create_rep.createdbstatus().find("[INFO]"), std::string::npos) << create_rep.createdbstatus();

    constexpr int k_clients = 8;
    constexpr int k_per_client = 50;
    std::vector<std::thread> clients;
    std::atomic<int> failure_count{0};

    for (int c = 0; c < k_clients; c++) {
        clients.emplace_back([c, &failure_count]() {
            zmq::context_t ctx(1);
            zmq::socket_t socket = MakeClientSocket(ctx);

            // Each client writes to its own disjoint offset range, same year
            // -- so a failure can only come from cross-client interference on
            // the server side (shared XStore statics / mmap cache), not from
            // two clients legitimately racing the same row.
            const unsigned long long base = TS_2005_MID + (unsigned long long)c * 10000;
            for (int i = 0; i < k_per_client; i++) {
                unsigned long long ts = base + i;
                int64_t expected = (int64_t)c * 1'000'000LL + i;

                XStore_Proto::UnaryInsert_REQ insert_req;
                insert_req.set_rtype(XStore_Proto::UNARYINSERT);
                insert_req.set_dbname("ZMQ_CONCURRENT_DB");
                insert_req.set_timestamp(ts);
                // SYNC: this test asserts the immediately-following query
                // sees exactly what was just inserted -- needs the row
                // actually applied, not just enqueued (the wire default,
                // ASYNC, doesn't guarantee that by the time the query runs).
                insert_req.set_durabilitymode(XStore_Proto::SYNC);
                *insert_req.mutable_data() = MakeRow({FInt64(expected)});
                auto insert_rep = SendRequest<XStore_Proto::UnaryInsert_REP>(socket, Wrap(insert_req));
                if (insert_rep.insertstatus().find("[INFO]") == std::string::npos) {
                    ADD_FAILURE() << "client " << c << " i=" << i << " insert failed: " << insert_rep.insertstatus();
                    failure_count++;
                    continue;
                }

                XStore_Proto::UnaryQuery_REQ query_req;
                query_req.set_rtype(XStore_Proto::UNARYQUERY);
                query_req.set_dbname("ZMQ_CONCURRENT_DB");
                query_req.set_timestamp(ts);
                auto query_rep = SendRequest<XStore_Proto::UnaryQuery_REP>(socket, Wrap(query_req));
                if (query_rep.data().fielddata_size() != 1 || query_rep.data().fielddata(0).int64data() != expected) {
                    ADD_FAILURE() << "client " << c << " i=" << i << " read back wrong/missing data";
                    failure_count++;
                }
            }
        });
    }
    for (auto &t : clients) t.join();

    EXPECT_EQ(failure_count.load(), 0)
        << "one or more concurrent clients saw a failed/incorrect request through the multi-worker server";
}

// Worker-count defaults. SYNC inserts hold a worker for the whole fsync, so the default is one worker per LOGICAL core
// (measured: more in-flight SYNC calls mean bigger group commits, with and without concurrent CPU-heavy queries). The
// BatchQuery pread pool is sized off cores minus what ZMQ claims: it must be charged the PHYSICAL core count at most,
// not the (larger) default worker count, or the pool collapses to its floor (measured: -60% BatchQuery throughput).
TEST(ZmqServerDefaultsTest, DefaultWorkerCountIsTheLogicalCoreCount) {
    const unsigned logical = std::thread::hardware_concurrency();
    if (logical == 0) GTEST_SKIP() << "hardware_concurrency() is undetermined here";
    EXPECT_EQ(ZmqServer::DefaultThreadCount(), static_cast<int>(logical));
}

TEST(ZmqServerDefaultsTest, PhysicalCoreCountIsPositiveAndNotAboveTheLogicalCount) {
    const int physical = ZmqServer::PhysicalCoreCount();
    EXPECT_GE(physical, 1);
    const unsigned logical = std::thread::hardware_concurrency();
    if (logical != 0) EXPECT_LE(physical, static_cast<int>(logical));
}

TEST(ZmqServerDefaultsTest, HeavyRequestsLeaveAQuarterOfTheWorkersFreeButAlwaysAtLeastOneSlot) {
    EXPECT_EQ(ZmqServer::HeavyRequestCap(32), 24u);
    EXPECT_EQ(ZmqServer::HeavyRequestCap(16), 12u);
    EXPECT_EQ(ZmqServer::HeavyRequestCap(4), 3u);
    EXPECT_EQ(ZmqServer::HeavyRequestCap(3), 3u);    // too few workers to reserve one: no reserve
    EXPECT_EQ(ZmqServer::HeavyRequestCap(1), 1u);    // never 0: heavy requests must be able to run
    EXPECT_EQ(ZmqServer::HeavyRequestCap(0), 1u);
    EXPECT_EQ(ZmqServer::HeavyRequestCap(-5), 1u);
}

TEST(ZmqServerDefaultsTest, PreadPoolIsChargedAtMostThePhysicalCoreCount) {
    EXPECT_EQ(ZmqServer::PreadPoolReserved(/*workers=*/32, /*physical=*/16), 16u);   // more workers than physical cores: capped
    EXPECT_EQ(ZmqServer::PreadPoolReserved(16, 16), 16u);
    EXPECT_EQ(ZmqServer::PreadPoolReserved(8, 16), 8u);                              // fewer workers: the pool keeps growing as before
    EXPECT_EQ(ZmqServer::PreadPoolReserved(1, 16), 1u);
    EXPECT_EQ(ZmqServer::PreadPoolReserved(0, 16), 0u);                              // never negative or wrapped
    EXPECT_EQ(ZmqServer::PreadPoolReserved(-3, 16), 0u);
}

// ---------------------------------------------------------------------
// Flow control of a streamed reply (include/Networking/ZMQ_StreamFlow.h): a client that reads slowly must not make the server queue its whole reply
// ---------------------------------------------------------------------

namespace {
// What a client sees of one streamed RangeQuery read slowly: every payload byte, how many messages, the most bytes the server had queued for all
// replies (xstore::streamflow::QueuedBytes) when a message arrived.
struct SlowRead {
    std::string payload;
    size_t messages = 0;
    uint64_t max_queued = 0;
    bool complete = false;
};
}  // namespace

class StreamFlowServerTest : public XStoreZmqNetworkTest {
protected:
    // These tests drive the flow control where a worker waits for its window; parking (the default) has ParkServerTest. saved_ is the configuration
    // they start from and change: the server's own, with parking off.
    void SetUp() override {
        original_ = xstore::streamflow::Get();
        saved_ = original_;
        saved_.park_replies = false;
        xstore::streamflow::Set(saved_);
    }
    void TearDown() override { xstore::streamflow::Set(original_); }

    // ~40 MB of rows (more than the kernel's socket buffers take on loopback, so a slow reader makes the server queue).
    static constexpr int ROWS = 28000;
    void CreateBigDb(const std::string &db) {
        XStore_Proto::CreateDB_REQ create_req;
        create_req.set_rtype(XStore_Proto::CREATEDB);
        create_req.set_dbname(db);
        create_req.set_timeresolution(XStore_Proto::SECOND);
        create_req.set_rowlength(1500);
        SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));
        const std::string text(1400, 'x');
        for (int begin = 0; begin < ROWS; begin += 1000) {
            XStore_Proto::BatchInsert_REQ insert_req;
            insert_req.set_rtype(XStore_Proto::BATCHINSERT);
            insert_req.set_dbname(db);
            insert_req.set_durabilitymode(XStore_Proto::SYNC);
            for (int i = begin; i < begin + 1000; ++i) {
                XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
                entry->set_timestamp(TWO_CHUNK_T0 + static_cast<unsigned long long>(i));
                *entry->mutable_data() = MakeRow({FString(text)});
            }
            EXPECT_NE(SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req)).insertstatus().find("Inserted"), std::string::npos);
        }
    }

    // A streamed RangeQuery over the whole DB, read at one message per `delay`.
    SlowRead ReadSlowly(const std::string &db, std::chrono::milliseconds delay, std::chrono::milliseconds first_pause = std::chrono::milliseconds(0),
                        const std::string &address = server_addr) {
        return ReadRequestSlowly(RangeRequest(db, TWO_CHUNK_T0, TWO_CHUNK_T0 + ROWS - 1), delay, first_pause, address);
    }
    // Any streamed row call, read at one message per `delay`.
    SlowRead ReadRequestSlowly(XStore_Proto::Request request, std::chrono::milliseconds delay, std::chrono::milliseconds first_pause = std::chrono::milliseconds(0),
                               const std::string &address = server_addr) {
        request.set_streamok(true);
        zmq::socket_t sock(*client_ctx, zmq::socket_type::dealer);
        sock.set(zmq::sockopt::rcvtimeo, 60000);
        sock.set(zmq::sockopt::linger, 0);
        // A client's own libzmq reads the connection into its memory (up to its receive high-water mark, 1000 messages by default) whether or not the
        // application is reading, so a slow application alone backs nothing up on the server. A client that stops taking data off the connection does.
        sock.set(zmq::sockopt::rcvhwm, 2);
        sock.connect(address);
        const std::string payload = request.SerializeAsString();
        SlowRead read;
        if (!sock.send(zmq::message_t(), zmq::send_flags::sndmore) || !sock.send(zmq::buffer(payload), zmq::send_flags::none)) return read;
        while (true) {
            zmq::message_t delimiter;
            if (!sock.recv(delimiter, zmq::recv_flags::none)) return read;
            for (bool more = delimiter.more(); more;) {
                zmq::message_t frame;
                if (!sock.recv(frame, zmq::recv_flags::none)) return read;
                more = frame.more();
                read.payload.append(static_cast<const char *>(frame.data()), frame.size());
            }
            ++read.messages;
            read.max_queued = std::max(read.max_queued, xstore::streamflow::QueuedBytes());
            if (delimiter.size() == 0) {
                read.complete = true;
                return read;
            }
            std::this_thread::sleep_for(read.messages == 1 ? delay + first_pause : delay);
        }
    }

    // The queue is back to nothing once every connection has taken (or dropped) its chunks.
    static bool QueueDrains() {
        for (int i = 0; i < 200; ++i) {
            if (xstore::streamflow::QueuedBytes() == 0) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return false;
    }

    xstore::streamflow::Config saved_;
    xstore::streamflow::Config original_;
};

TEST_F(StreamFlowServerTest, ASlowReaderHoldsAtMostItsWindowAndStillGetsTheSameBytes) {
    CreateBigDb("ZMQ_FLOW_DB");
    xstore::streamflow::Config config = saved_;
    config.window_bytes = 0;
    xstore::streamflow::Set(config);
    ASSERT_TRUE(QueueDrains());
    const SlowRead unlimited = ReadSlowly("ZMQ_FLOW_DB", std::chrono::milliseconds(15));
    ASSERT_TRUE(unlimited.complete);
    ASSERT_TRUE(QueueDrains());

    config.window_bytes = 2u << 20;
    config.max_waiting = 4;
    config.min_chunk_bytes = 64u << 10;
    xstore::streamflow::Set(config);
    const SlowRead windowed = ReadSlowly("ZMQ_FLOW_DB", std::chrono::milliseconds(15));
    ASSERT_TRUE(windowed.complete);

    EXPECT_TRUE(windowed.payload == unlimited.payload) << "the same bytes with and without the window";
    EXPECT_GT(unlimited.max_queued, 8u << 20) << "without the window the server queued most of the reply while the client slept";
    EXPECT_LE(windowed.max_queued, (2u << 20) + XStoreTunables::STREAMED_REPLY_CHUNK_SIZE_BYTES + 65536) << "with it, a window and one chunk";
    EXPECT_TRUE(QueueDrains());
}

// Chunks below the minimum (every chunk of a small reply) skip the accounting: with a minimum above the chunk size, limits that would stop any counted
// reply stop nothing, and nothing is ever counted.
TEST_F(StreamFlowServerTest, ChunksBelowTheMinimumAreNeitherCountedNorLimited) {
    CreateBigDb("ZMQ_FLOW_SMALL_DB");
    xstore::streamflow::Config config = saved_;
    config.window_bytes = 1;
    config.min_chunk_bytes = 4u << 20;   // the chunks are 512 KiB
    xstore::streamflow::Set(config);
    ASSERT_TRUE(QueueDrains());
    const SlowRead read = ReadSlowly("ZMQ_FLOW_SMALL_DB", std::chrono::milliseconds(5));
    ASSERT_TRUE(read.complete);
    EXPECT_EQ(read.max_queued, 0u) << "nothing was counted";
    XStore_Proto::RangeQuery_REP reply;
    ASSERT_TRUE(reply.ParseFromString(read.payload));
    EXPECT_EQ(reply.queryresponse_size(), ROWS);
    EXPECT_FALSE(reply.has_querystatus() && !reply.querystatus().empty()) << reply.querystatus();
}

// ---- Park and resume (docs/superpowers/specs/2026-11-01-park-resume-replies-design.md) ----
// The server of this suite has ONE worker: a reply that waited for its slow client in that worker would block every other request, so a request
// answered while a slow reader is stalled shows the worker was released.
class ParkServerTest : public StreamFlowServerTest {
protected:
    void SetUp() override {
        StreamFlowServerTest::SetUp();
        saved_scan_ = xstore::scanlock::Get();
        xstore::scanlock::Config scan = saved_scan_;
        scan.chunk_bytes = 2u << 20;   // one zone-map block: parking is possible every ~2 MiB of rows
        scan.parallel_threads = 1;
        xstore::scanlock::Set(scan);
    }
    void TearDown() override {
        xstore::scanlock::Set(saved_scan_);
        StreamFlowServerTest::TearDown();
    }
    static constexpr uint64_t WINDOW = 2u << 20;
    xstore::streamflow::Config ParkConfig() const {
        xstore::streamflow::Config config = saved_;
        config.park_replies = true;
        config.window_bytes = WINDOW;
        config.min_chunk_bytes = 64u << 10;
        return config;
    }
    SlowRead Baseline(const std::string &db) {
        xstore::streamflow::Config config = saved_;
        config.park_replies = false;
        config.window_bytes = 0;
        xstore::streamflow::Set(config);
        const SlowRead read = ReadSlowly(db, std::chrono::milliseconds(0));
        EXPECT_TRUE(read.complete);
        EXPECT_TRUE(QueueDrains());
        return read;
    }
    static bool WaitForParks(uint64_t more_than) {
        for (int i = 0; i < 1000; ++i) {
            if (xstore::streamflow::ParkCount() > more_than) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }
    static bool WaitForResumesToCatchUp() {
        for (int i = 0; i < 1000; ++i) {
            if (xstore::streamflow::ResumeCount() >= xstore::streamflow::ParkCount()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }
    bool PingAnswersWithin(std::chrono::milliseconds limit) {
        XStore_Proto::Request ping_req;
        ping_req.mutable_ping()->set_rtype(XStore_Proto::PING);
        const auto start = std::chrono::steady_clock::now();
        (void)SendRequest<XStore_Proto::Ping_REP>(ping_req);
        return std::chrono::steady_clock::now() - start < limit;
    }
    xstore::scanlock::Config saved_scan_;
};

TEST_F(ParkServerTest, AParkedReplyFreesTheOnlyWorkerAndStillDeliversTheSameBytes) {
    CreateBigDb("ZMQ_PARK_DB");
    const SlowRead expected = Baseline("ZMQ_PARK_DB");
    xstore::streamflow::Set(ParkConfig());
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    auto reader = std::async(std::launch::async, [&] { return ReadSlowly("ZMQ_PARK_DB", std::chrono::milliseconds(15)); });
    ASSERT_TRUE(WaitForParks(parks_before)) << "the reply never parked";
    EXPECT_TRUE(PingAnswersWithin(std::chrono::milliseconds(1000))) << "the worker was not released while the reply waits for its slow client";
    EXPECT_EQ(reader.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout) << "the reply was still in progress";
    const SlowRead read = reader.get();
    ASSERT_TRUE(read.complete);
    EXPECT_TRUE(read.payload == expected.payload) << "the same bytes, in the same order, as without parking";
    EXPECT_LE(read.max_queued, WINDOW + (4u << 20)) << "a window plus what one scan chunk adds";
    EXPECT_TRUE(WaitForResumesToCatchUp());
    EXPECT_TRUE(QueueDrains());
}

// The other three row calls park the same way: the single worker is free while the slow reader's reply waits, and the bytes are those of a read
// without parking. Every row of the DB matches (a 1400-byte string repeated), so each reply is the whole 40 MB.
class ParkOtherCallsTest : public ParkServerTest {
protected:
    XStore_Proto::Request FindByValueRequest(const std::string &db) {
        XStore_Proto::FindByValue_REQ req;
        req.set_rtype(XStore_Proto::FINDBYVALUE);
        req.set_dbname(db);
        req.set_starttime(TWO_CHUNK_T0);
        req.set_endtime(TWO_CHUNK_T0 + ROWS - 1);
        req.set_columnidx(0);
        req.mutable_value()->set_stringdata(std::string(1400, 'x'));
        return Wrap(req);
    }
    XStore_Proto::Request FindByPatternRequest(const std::string &db) {
        XStore_Proto::FindByPattern_REQ req;
        req.set_rtype(XStore_Proto::FINDBYPATTERN);
        req.set_dbname(db);
        req.set_starttime(TWO_CHUNK_T0);
        req.set_endtime(TWO_CHUNK_T0 + ROWS - 1);
        req.set_columnidx(0);
        req.set_pattern("xxxx%");
        req.set_mode(XStore_Proto::PATTERN_LIKE);
        return Wrap(req);
    }
    XStore_Proto::Request BatchQueryRequest(const std::string &db) {
        XStore_Proto::BatchQuery_REQ req;
        req.set_rtype(XStore_Proto::BATCHQUERY);
        req.set_dbname(db);
        for (int i = 0; i < ROWS; ++i) req.add_timestamp(TWO_CHUNK_T0 + static_cast<unsigned long long>(i));
        return Wrap(req);
    }

    void ExpectParksFreesTheWorkerAndDeliversTheSameBytes(const XStore_Proto::Request &request) {
        xstore::streamflow::Config off = saved_;
        off.park_replies = false;
        off.window_bytes = 0;
        xstore::streamflow::Set(off);
        const SlowRead expected = ReadRequestSlowly(request, std::chrono::milliseconds(0));
        ASSERT_TRUE(expected.complete);
        ASSERT_TRUE(QueueDrains());
        XStore_Proto::RangeQuery_REP all;
        ASSERT_TRUE(all.ParseFromString(expected.payload));
        ASSERT_EQ(all.queryresponse_size(), ROWS);

        xstore::streamflow::Set(ParkConfig());
        const uint64_t parks_before = xstore::streamflow::ParkCount();
        auto reader = std::async(std::launch::async, [&] { return ReadRequestSlowly(request, std::chrono::milliseconds(15)); });
        ASSERT_TRUE(WaitForParks(parks_before)) << "the reply never parked";
        EXPECT_TRUE(PingAnswersWithin(std::chrono::milliseconds(1000))) << "the worker was not released while the reply waits for its slow client";
        const SlowRead read = reader.get();
        ASSERT_TRUE(read.complete);
        EXPECT_TRUE(read.payload == expected.payload) << "the same bytes, in the same order, as without parking";
        EXPECT_LE(read.max_queued, WINDOW + (8u << 20)) << "a window plus what one boundary adds";
        EXPECT_TRUE(WaitForResumesToCatchUp());
        EXPECT_TRUE(QueueDrains());
    }
};

TEST_F(ParkOtherCallsTest, FindByValueParksAndDeliversTheSameBytes) {
    CreateBigDb("ZMQ_PARK_FV_DB");
    ExpectParksFreesTheWorkerAndDeliversTheSameBytes(FindByValueRequest("ZMQ_PARK_FV_DB"));
}

TEST_F(ParkOtherCallsTest, FindByPatternParksAndDeliversTheSameBytes) {
    CreateBigDb("ZMQ_PARK_FP_DB");
    ExpectParksFreesTheWorkerAndDeliversTheSameBytes(FindByPatternRequest("ZMQ_PARK_FP_DB"));
}

TEST_F(ParkOtherCallsTest, BatchQueryParksAndDeliversTheSameBytes) {
    CreateBigDb("ZMQ_PARK_BQ_DB");
    ExpectParksFreesTheWorkerAndDeliversTheSameBytes(BatchQueryRequest("ZMQ_PARK_BQ_DB"));
}

// One memory budget: a reply parks when the POOL is full, not only when its own window is. The window here is far bigger than the reply, so
// only the pool can make it park; the queue then stays at about one scan chunk, and the reply still arrives whole.
TEST_F(ParkServerTest, AReplyParksWhenThePoolIsFullEvenThoughItsWindowIsNot) {
    CreateBigDb("ZMQ_PARK_POOL_DB");
    const SlowRead expected = Baseline("ZMQ_PARK_POOL_DB");
    xstore::streamflow::Config config = ParkConfig();
    config.window_bytes = 512u << 20;
    xstore::streamflow::Set(config);
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    xstore::streamflow::SetPoolFullForTests(0);
    const SlowRead roomy = ReadSlowly("ZMQ_PARK_POOL_DB", std::chrono::milliseconds(3));
    ASSERT_TRUE(roomy.complete);
    EXPECT_EQ(xstore::streamflow::ParkCount(), parks_before) << "with room in the pool and a window this big, nothing parks";
    ASSERT_TRUE(QueueDrains());

    xstore::streamflow::SetPoolFullForTests(1);
    const SlowRead pressed = ReadSlowly("ZMQ_PARK_POOL_DB", std::chrono::milliseconds(3));
    xstore::streamflow::SetPoolFullForTests(-1);
    ASSERT_TRUE(pressed.complete) << "a reply makes progress whatever the pool says";
    EXPECT_GE(xstore::streamflow::ParkCount(), parks_before + 4) << "a full pool parks the reply at (nearly) every scan chunk";
    EXPECT_TRUE(pressed.payload == expected.payload);
    EXPECT_LE(pressed.max_queued, 8u << 20) << "with the pool full the queue stays at about a scan chunk, not the 512 MiB window";
    EXPECT_TRUE(WaitForResumesToCatchUp());
    EXPECT_TRUE(QueueDrains());
}

TEST_F(ParkServerTest, TheBytesQueuedForASlowClientAreChargedToThePoolAndReturnedWhenTheyAreWritten) {
    CreateBigDb("ZMQ_PARK_CHARGE_DB");
    xstore::streamflow::Set(ParkConfig());
    xstore::MemoryPool &pool = xstore::MemoryPool::Instance();
    const uint64_t base = pool.UsedBytes();
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    auto reader = std::async(std::launch::async, [&] { return ReadSlowly("ZMQ_PARK_CHARGE_DB", std::chrono::milliseconds(15)); });
    ASSERT_TRUE(WaitForParks(parks_before));
    EXPECT_GE(pool.UsedBytes(), base + (1u << 20)) << "a parked reply's queued bytes are in the pool's figure";
    ASSERT_TRUE(reader.get().complete);
    EXPECT_TRUE(QueueDrains());
    EXPECT_EQ(pool.UsedBytes(), base) << "everything written is returned";
}

TEST_F(ParkServerTest, MoreSlowReadersThanWorkersAllCompleteWithTheSameBytesAndTheWorkerStaysFree) {
    CreateBigDb("ZMQ_PARK_MANY_DB");
    const SlowRead expected = Baseline("ZMQ_PARK_MANY_DB");
    xstore::streamflow::Set(ParkConfig());
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    std::vector<std::future<SlowRead>> readers;
    for (int i = 0; i < 5; ++i) readers.push_back(std::async(std::launch::async, [&] { return ReadSlowly("ZMQ_PARK_MANY_DB", std::chrono::milliseconds(15)); }));
    ASSERT_TRUE(WaitForParks(parks_before + 2));
    EXPECT_TRUE(PingAnswersWithin(std::chrono::milliseconds(1000)));
    for (auto &reader : readers) {
        const SlowRead read = reader.get();
        ASSERT_TRUE(read.complete);
        EXPECT_TRUE(read.payload == expected.payload);
        EXPECT_LE(read.max_queued, 5 * (WINDOW + (4u << 20))) << "the total queued over all replies stays bound by their windows";
    }
    EXPECT_TRUE(WaitForResumesToCatchUp());
    EXPECT_TRUE(QueueDrains());
}

TEST_F(ParkServerTest, ADbRemovedWhileTheReplyIsParkedEndsItWithAnErrorStatus) {
    CreateBigDb("ZMQ_PARK_RM_DB");
    xstore::streamflow::Set(ParkConfig());
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    // Takes the first message, then stalls for 2 s (its receive high-water mark of 2 backs the rest up into the server).
    auto reader = std::async(std::launch::async, [&] { return ReadSlowly("ZMQ_PARK_RM_DB", std::chrono::milliseconds(0), std::chrono::milliseconds(2000)); });
    ASSERT_TRUE(WaitForParks(parks_before));
    XStore_Proto::RemoveDB_REQ remove_req;
    remove_req.set_rtype(XStore_Proto::REMOVEDB);
    remove_req.set_dbname("ZMQ_PARK_RM_DB");
    (void)SendRequest<XStore_Proto::RemoveDB_REP>(Wrap(remove_req));
    const SlowRead read = reader.get();
    ASSERT_TRUE(read.complete) << "the reply still ends properly";
    XStore_Proto::RangeQuery_REP reply;
    ASSERT_TRUE(reply.ParseFromString(read.payload));
    EXPECT_NE(reply.querystatus().find("removed while the request ran"), std::string::npos) << reply.querystatus();
    EXPECT_LT(reply.queryresponse_size(), ROWS);
    EXPECT_TRUE(WaitForResumesToCatchUp());
    EXPECT_TRUE(QueueDrains());
}

TEST_F(ParkServerTest, AClientThatDisconnectsWhileItsReplyIsParkedLeavesNothingBehind) {
    CreateBigDb("ZMQ_PARK_GONE_DB");
    xstore::streamflow::Set(ParkConfig());
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    {
        XStore_Proto::Request request = RangeRequest("ZMQ_PARK_GONE_DB", TWO_CHUNK_T0, TWO_CHUNK_T0 + ROWS - 1);
        request.set_streamok(true);
        zmq::socket_t sock(*client_ctx, zmq::socket_type::dealer);
        sock.set(zmq::sockopt::rcvtimeo, 10000);
        sock.set(zmq::sockopt::linger, 0);
        sock.set(zmq::sockopt::rcvhwm, 2);
        sock.connect(server_addr);
        const std::string payload = request.SerializeAsString();
        ASSERT_TRUE(sock.send(zmq::message_t(), zmq::send_flags::sndmore).has_value());
        ASSERT_TRUE(sock.send(zmq::buffer(payload), zmq::send_flags::none).has_value());
        zmq::message_t first;
        ASSERT_TRUE(sock.recv(first, zmq::recv_flags::none).has_value());
        ASSERT_TRUE(WaitForParks(parks_before));
    }   // the socket closes with the reply parked
    EXPECT_TRUE(WaitForResumesToCatchUp()) << "a parked reply whose client left is resumed (and dropped), not kept for ever";
    EXPECT_TRUE(QueueDrains());
    EXPECT_TRUE(PingAnswersWithin(std::chrono::milliseconds(1000)));
}

TEST_F(ParkServerTest, AParkedReplyThatItsClientDoesNotTakeForTheStallTimeIsStoppedWithAnErrorStatus) {
    CreateBigDb("ZMQ_PARK_STALL_DB");
    xstore::streamflow::Config config = ParkConfig();
    config.stall = std::chrono::milliseconds(1000);
    xstore::streamflow::Set(config);
    const int port = StartZmqServerOnFreePort(1);   // its broker reads the stall time at start; the same data directory
    ASSERT_GE(port, 0);
    const std::string address = "tcp://127.0.0.1:" + std::to_string(port);
    const uint64_t parks_before = xstore::streamflow::ParkCount();
    auto reader = std::async(std::launch::async, [&] { return ReadSlowly("ZMQ_PARK_STALL_DB", std::chrono::milliseconds(0), std::chrono::milliseconds(3500), address); });
    ASSERT_TRUE(WaitForParks(parks_before));
    const SlowRead read = reader.get();
    ASSERT_TRUE(read.complete);
    XStore_Proto::RangeQuery_REP reply;
    ASSERT_TRUE(reply.ParseFromString(read.payload));
    EXPECT_NE(reply.querystatus().find("[ERROR]"), std::string::npos) << reply.querystatus();
    EXPECT_NE(reply.querystatus().find("not taking the reply"), std::string::npos) << reply.querystatus();
    EXPECT_LT(reply.queryresponse_size(), ROWS);
    EXPECT_TRUE(WaitForResumesToCatchUp());
    EXPECT_TRUE(QueueDrains());
}
