// Exercises the XStore API through the real ZMQ wire protocol (ROUTER <->
// DEALER <-> in-proc REP worker threads, see ZMQ_Server.cpp) instead of
// calling into the XStore class directly. This is deliberately a *separate*
// test binary from xstore_core_tests/xstore_insertquery_tests (see
// tests/CMakeLists.txt) since it links libzmq and spins up a real TCP server
// thread for the whole suite.
//
// ZMQ_Server has no shutdown API (StartServer() blocks forever in
// zmq::proxy()), so the server + its worker thread are started once in
// SetUpTestSuite() and deliberately never torn down -- the OS reclaims the
// socket/thread on process exit. Only the on-disk scratch_ directory is
// cleaned up in TearDownTestSuite().
#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <thread>
#include <unistd.h>
#include <vector>

#include "include/Networking/ZMQ_Server.h"
#include "XStoreTestSupport.h"

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

}  // namespace

class XStoreZmqNetworkTest : public ::testing::Test {
 protected:
  static ScratchDir *scratch;
  static zmq::context_t *client_ctx;
  static zmq::socket_t *client_socket;

  static void SetUpTestSuite() {
      // Leaked deliberately (see file header comment): the worker's XStore
      // instance captures this cwd as its baseDir at thread-start time and
      // keeps writing there for the rest of the process's life.
      scratch = new ScratchDir("xstore_zmq_test");

      int port = 25500 + static_cast<int>(getpid() % 4000);
      std::string server_addr = "tcp://127.0.0.1:" + std::to_string(port);

      auto *server = new ZmqServer(1);  // single worker: client is a single REQ socket anyway
      std::thread server_thread([server, port]() {
          server->StartServer("127.0.0.1", std::to_string(port));
      });
      server_thread.detach();

      client_ctx = new zmq::context_t(1);
      client_socket = new zmq::socket_t(*client_ctx, zmq::socket_type::req);
      client_socket->set(zmq::sockopt::rcvtimeo, 5000);
      client_socket->set(zmq::sockopt::linger, 0);
      // ZeroMQ queues connects lazily/retries internally, so connecting
      // before bind() completes on the server thread is safe.
      client_socket->connect(server_addr);

      // Small head start so the first request in the first test doesn't pay
      // for ZeroMQ's initial TCP connect/reconnect backoff.
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  static void TearDownTestSuite() {
      delete client_socket;
      delete client_ctx;
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
};

ScratchDir *XStoreZmqNetworkTest::scratch = nullptr;
zmq::context_t *XStoreZmqNetworkTest::client_ctx = nullptr;
zmq::socket_t *XStoreZmqNetworkTest::client_socket = nullptr;

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

TEST_F(XStoreZmqNetworkTest, CreateDBOverWireRejectsDuplicateName) {
    XStore_Proto::CreateDB_REQ create_req;
    create_req.set_rtype(XStore_Proto::CREATEDB);
    create_req.set_dbname("ZMQ_DUP_DB");
    create_req.set_timeresolution(XStore_Proto::SECOND);
    create_req.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));

    auto second_rep = SendRequest<XStore_Proto::CreateDB_REP>(Wrap(create_req));
    EXPECT_EQ(second_rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: ZMQ_DUP_DB] - Already existed");
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

TEST_F(XStoreZmqNetworkTest, BatchInsertOverWireToNonexistentDBReturnsError) {
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_rtype(XStore_Proto::BATCHINSERT);
    insert_req.set_dbname("ZMQ_NOPE_BATCH_DB");
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2012_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    auto insert_rep = SendRequest<XStore_Proto::BatchInsert_REP>(Wrap(insert_req));
    EXPECT_EQ(insert_rep.insertstatus(), "[WARNING] [BATCH INSERT] [DB: ZMQ_NOPE_BATCH_DB] - DB does not exist");
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
    ASSERT_EQ(remove_files_rep.removestatus_size(), 1);
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
  static std::string server_addr;

  static void SetUpTestSuite() {
      // Leaked deliberately, same rationale as XStoreZmqNetworkTest.
      scratch = new ScratchDir("xstore_zmq_concurrent_test");

      int port = 26500 + static_cast<int>(getpid() % 4000);
      server_addr = "tcp://127.0.0.1:" + std::to_string(port);

      auto *server = new ZmqServer(4);  // multi-worker pool, unlike XStoreZmqNetworkTest's single worker
      std::thread server_thread([server, port]() {
          server->StartServer("127.0.0.1", std::to_string(port));
      });
      server_thread.detach();

      std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

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
