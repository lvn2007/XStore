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
#include <gtest/gtest.h>
#include <google/protobuf/io/coded_stream.h>
#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdlib>
#include <map>
#include <thread>
#include <unistd.h>
#include <vector>

#include "include/Networking/ZMQ_Server.h"
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
TEST_F(XStoreZmqNetworkTest, RangeQueryReplyLargerThanOneChunkArrivesAsContiguousFrames) {
    constexpr size_t CHUNK = XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES;
    constexpr size_t HOLE_ENTRY_BYTES = 10;
    constexpr unsigned long long T0 = TS_2020_MID;

    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_BIGRANGE_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(16);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    const auto insert_rows = [&](const std::vector<std::pair<unsigned long long, int64_t>> &rows) {
        XStore_Proto::BatchInsert_REQ insert_req;
        insert_req.set_rtype(XStore_Proto::BATCHINSERT);
        insert_req.set_dbname("ZMQ_BIGRANGE_DB");
        insert_req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[ts, value] : rows) {
            XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
            entry->set_timestamp(ts);
            *entry->mutable_data() = MakeRow({FInt64(value)});
        }
        auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
        EXPECT_NE(insert_rep.insertstatus().find("Inserted"), std::string::npos) << insert_rep.insertstatus();
    };
    const auto range_request = [&](unsigned long long start, unsigned long long end) {
        XStore_Proto::RangeQuery_REQ query_req;
        query_req.set_rtype(XStore_Proto::RANGEQUERY);
        query_req.set_dbname("ZMQ_BIGRANGE_DB");
        query_req.set_starttime(start);
        query_req.set_endtime(end);
        return Wrap(query_req);
    };

    // Entry 0 (a real row) is the only one whose size is not HOLE_ENTRY_BYTES: measure it with a one-entry query.
    insert_rows({{T0, 7}});
    std::vector<std::string> frames;
    ASSERT_TRUE(SendRequestMultipart(range_request(T0, T0), frames, 10000));
    ASSERT_EQ(frames.size(), 1u);
    const size_t first_entry_bytes = frames[0].size();
    // Smallest j with first_entry_bytes + 10 * j >= CHUNK: the entry at offset j closes frame 0.
    const unsigned long long cut = (CHUNK - first_entry_bytes + HOLE_ENTRY_BYTES - 1) / HOLE_ENTRY_BYTES;
    const unsigned long long last = cut + 1000;
    insert_rows({{T0 + cut, 8}, {T0 + cut + 1, 9}, {T0 + last, 10}});

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
