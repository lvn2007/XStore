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
// socket/thread on process exit. Only the on-disk scratch directory is
// cleaned up in TearDownTestSuite().
#include <gtest/gtest.h>
#include <chrono>
#include <cstdlib>
#include <thread>
#include <unistd.h>

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

}  // namespace

class XStoreZmqNetworkTest : public ::testing::Test {
 protected:
  static ScratchDir *scratch;
  static zmq::context_t *clientCtx;
  static zmq::socket_t *clientSocket;

  static void SetUpTestSuite() {
      // Leaked deliberately (see file header comment): the worker's XStore
      // instance captures this cwd as its baseDir at thread-start time and
      // keeps writing there for the rest of the process's life.
      scratch = new ScratchDir("xstore_zmq_test");

      int port = 25500 + static_cast<int>(getpid() % 4000);
      std::string serverAddr = "tcp://127.0.0.1:" + std::to_string(port);

      auto *server = new ZMQ_Server(1);  // single worker: client is a single REQ socket anyway
      std::thread serverThread([server, port]() {
          server->StartServer("127.0.0.1", std::to_string(port));
      });
      serverThread.detach();

      clientCtx = new zmq::context_t(1);
      clientSocket = new zmq::socket_t(*clientCtx, zmq::socket_type::req);
      clientSocket->set(zmq::sockopt::rcvtimeo, 5000);
      clientSocket->set(zmq::sockopt::linger, 0);
      // ZeroMQ queues connects lazily/retries internally, so connecting
      // before bind() completes on the server thread is safe.
      clientSocket->connect(serverAddr);

      // Small head start so the first request in the first test doesn't pay
      // for ZeroMQ's initial TCP connect/reconnect backoff.
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  static void TearDownTestSuite() {
      delete clientSocket;
      delete clientCtx;
      delete scratch;  // removes the scratch DataStore dir; server thread is left dangling, harmless at process exit
  }

  template <typename ReqT, typename RepT>
  RepT SendRequest(const ReqT &req) {
      std::string payload = req.SerializeAsString();
      auto sendResult = clientSocket->send(zmq::buffer(payload), zmq::send_flags::none);
      RepT rep;
      if (!sendResult) {
          ADD_FAILURE() << "failed to send request over ZMQ";
          return rep;
      }
      zmq::message_t reply;
      auto recvResult = clientSocket->recv(reply, zmq::recv_flags::none);
      if (!recvResult) {
          ADD_FAILURE() << "no reply received from ZMQ server within timeout";
          return rep;
      }
      rep.ParseFromArray(reply.data(), reply.size());
      return rep;
  }
};

ScratchDir *XStoreZmqNetworkTest::scratch = nullptr;
zmq::context_t *XStoreZmqNetworkTest::clientCtx = nullptr;
zmq::socket_t *XStoreZmqNetworkTest::clientSocket = nullptr;

// ---------------------------------------------------------------------
// INFO / CREATEDB
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, InfoQueryOverWireReflectsCreatedDatabase) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_INFO_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    auto createRep = SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);
    EXPECT_EQ(createRep.createdbstatus(), "[INFO] [CREATE DB] [DB: ZMQ_INFO_DB] - Created DB");

    XStore_Proto::ReqCMD infoReq;
    infoReq.set_rtype(XStore_Proto::INFO);
    auto infoRep = SendRequest<XStore_Proto::ReqCMD, XStore_Proto::SysInfo_REP>(infoReq);

    bool found = false;
    for (int i = 0; i < infoRep.dbinfo_size(); i++) {
        if (infoRep.dbinfo(i).dbname() == "ZMQ_INFO_DB") found = true;
    }
    EXPECT_TRUE(found) << "expected ZMQ_INFO_DB to be reported by INFO over the wire";
}

TEST_F(XStoreZmqNetworkTest, CreateDBOverWireRejectsDuplicateName) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_DUP_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    auto secondRep = SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);
    EXPECT_EQ(secondRep.createdbstatus(), "[ERROR] [CREATE DB] [DB: ZMQ_DUP_DB] - Already existed");
}

// ---------------------------------------------------------------------
// UNARY INSERT / QUERY / DELETE
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, UnaryInsertThenUnaryQueryRoundTripsDataOverWire) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_UNARY_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    XStore_Proto::UnaryInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::UNARYINSERT);
    insertReq.set_dbname("ZMQ_UNARY_DB");
    insertReq.set_timestamp(TS_2005_MID);
    *insertReq.mutable_data() = MakeRow({FDouble(2.71828), FString("wire-data"), FBool(true)});
    auto insertRep = SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(insertReq);
    EXPECT_NE(insertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ queryReq;
    queryReq.set_rtype(XStore_Proto::UNARYQUERY);
    queryReq.set_dbname("ZMQ_UNARY_DB");
    queryReq.set_timestamp(TS_2005_MID);
    auto queryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(queryReq);

    EXPECT_EQ(queryRep.timestamp(), TS_2005_MID);
    ASSERT_EQ(queryRep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(queryRep.data().fielddata(0).doubledata(), 2.71828);
    EXPECT_EQ(queryRep.data().fielddata(1).stringdata(), "wire-data");
    EXPECT_EQ(queryRep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreZmqNetworkTest, UnaryQueryOverWireOnMissingDBReturnsError) {
    XStore_Proto::UnaryQuery_REQ queryReq;
    queryReq.set_rtype(XStore_Proto::UNARYQUERY);
    queryReq.set_dbname("ZMQ_NOPE_DB");
    queryReq.set_timestamp(TS_2005_MID);
    auto queryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(queryReq);
    EXPECT_EQ(queryRep.querystatus(), "[ERROR] [UNARY QUERY] [DB: ZMQ_NOPE_DB] - DB does not exist");
}

TEST_F(XStoreZmqNetworkTest, UnaryDeleteOverWirePunchesHoleThenQueryReturnsEmpty) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_DELETE_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    XStore_Proto::UnaryInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::UNARYINSERT);
    insertReq.set_dbname("ZMQ_DELETE_DB");
    insertReq.set_timestamp(TS_2012_MID);
    *insertReq.mutable_data() = MakeRow({FString("to-be-deleted")});
    SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(insertReq);

    XStore_Proto::UnaryDelete_REQ deleteReq;
    deleteReq.set_rtype(XStore_Proto::UNARYDELETE);
    deleteReq.set_dbname("ZMQ_DELETE_DB");
    deleteReq.set_timestamp(TS_2012_MID);
    auto deleteRep = SendRequest<XStore_Proto::UnaryDelete_REQ, XStore_Proto::UnaryDelete_REP>(deleteReq);
    EXPECT_NE(deleteRep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ queryReq;
    queryReq.set_rtype(XStore_Proto::UNARYQUERY);
    queryReq.set_dbname("ZMQ_DELETE_DB");
    queryReq.set_timestamp(TS_2012_MID);
    auto queryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(queryReq);
    EXPECT_EQ(queryRep.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// BATCH INSERT / QUERY -- mixed column counts + holes, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, BatchInsertThenBatchQueryOverWireOmitsHoles) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_BATCH_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    const unsigned long long ts1 = TS_2012_MID;      // 1 column
    const unsigned long long ts2 = TS_2020_MID;      // 2 columns, different year
    const unsigned long long tsHole = TS_2012_MID + 50;  // never inserted

    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::BATCHINSERT);
    insertReq.set_dbname("ZMQ_BATCH_DB");

    XStore_Proto::BatchInsert_DATA *entry1 = insertReq.add_batchinsert_data();
    entry1->set_timestamp(ts1);
    *entry1->mutable_data() = MakeRow({FInt32(-55)});

    XStore_Proto::BatchInsert_DATA *entry2 = insertReq.add_batchinsert_data();
    entry2->set_timestamp(ts2);
    *entry2->mutable_data() = MakeRow({FUInt64(123456789ULL), FBytes("two-cols")});

    auto insertRep = SendRequest<XStore_Proto::BatchInsert_REQ, XStore_Proto::BatchInsert_REP>(insertReq);
    EXPECT_NE(insertRep.insertstatus().find("Inserted 2 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_rtype(XStore_Proto::BATCHQUERY);
    queryReq.set_dbname("ZMQ_BATCH_DB");
    queryReq.add_timestamp(ts1);
    queryReq.add_timestamp(tsHole);
    queryReq.add_timestamp(ts2);

    auto queryRep = SendRequest<XStore_Proto::BatchQuery_REQ, XStore_Proto::BatchQuery_REP>(queryReq);

    ASSERT_EQ(queryRep.queryresponse_size(), 2);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> byTs;
    for (int i = 0; i < queryRep.queryresponse_size(); i++) {
        byTs[queryRep.queryresponse(i).timestamp()] = queryRep.queryresponse(i);
    }
    ASSERT_EQ(byTs.count(tsHole), 0u);
    ASSERT_EQ(byTs[ts1].data().fielddata_size(), 1);
    EXPECT_EQ(byTs[ts1].data().fielddata(0).int32data(), -55);
    ASSERT_EQ(byTs[ts2].data().fielddata_size(), 2);
    EXPECT_EQ(byTs[ts2].data().fielddata(0).uint64data(), 123456789ULL);
    EXPECT_EQ(byTs[ts2].data().fielddata(1).bytesdata(), "two-cols");
}

TEST_F(XStoreZmqNetworkTest, BatchInsertOverWireToNonexistentDBReturnsError) {
    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::BATCHINSERT);
    insertReq.set_dbname("ZMQ_NOPE_BATCH_DB");
    XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
    entry->set_timestamp(TS_2012_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    auto insertRep = SendRequest<XStore_Proto::BatchInsert_REQ, XStore_Proto::BatchInsert_REP>(insertReq);
    EXPECT_EQ(insertRep.insertstatus(), "[ERROR] [BATCH INSERT] [DB: ZMQ_NOPE_BATCH_DB] - DB does not exist");
}

// ---------------------------------------------------------------------
// RANGE INSERT / QUERY -- contiguous range with an internal hole, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RangeInsertThenRangeQueryOverWireIncludesHoleAsEmptyEntry) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_RANGE_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    // Sparse range insert: endpoints only, leaving a hole at TS_2028_MID + 1.
    XStore_Proto::RangeInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::RANGEINSERT);
    insertReq.set_dbname("ZMQ_RANGE_DB");
    XStore_Proto::RangeInsert_DATA *first = insertReq.add_rangeinsert_data();
    first->set_timestamp(TS_2028_MID);
    *first->mutable_data() = MakeRow({FSFixed64(-42)});
    XStore_Proto::RangeInsert_DATA *last = insertReq.add_rangeinsert_data();
    last->set_timestamp(TS_2028_MID + 2);
    *last->mutable_data() = MakeRow({FSFixed64(42)});
    SendRequest<XStore_Proto::RangeInsert_REQ, XStore_Proto::RangeInsert_REP>(insertReq);

    XStore_Proto::RangeQuery_REQ queryReq;
    queryReq.set_rtype(XStore_Proto::RANGEQUERY);
    queryReq.set_dbname("ZMQ_RANGE_DB");
    queryReq.set_starttime(TS_2028_MID);
    queryReq.set_endtime(TS_2028_MID + 2);

    auto queryRep = SendRequest<XStore_Proto::RangeQuery_REQ, XStore_Proto::RangeQuery_REP>(queryReq);

    ASSERT_EQ(queryRep.queryresponse_size(), 3);
    EXPECT_EQ(queryRep.queryresponse(0).data().fielddata(0).sfixed64data(), -42);
    EXPECT_EQ(queryRep.queryresponse(1).data().fielddata_size(), 0) << "hole in the middle of the range";
    EXPECT_EQ(queryRep.queryresponse(2).data().fielddata(0).sfixed64data(), 42);
}

// ---------------------------------------------------------------------
// REMOVE DB FILES / REMOVE DB -- cleanup path, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RemoveDBFilesThenRemoveDBOverWire) {
    XStore_Proto::CreateDB_REQ createReq;
    createReq.set_rtype(XStore_Proto::CREATEDB);
    createReq.set_dbname("ZMQ_REMOVE_DB");
    createReq.set_timeresolution(XStore_Proto::SECOND);
    createReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(createReq);

    XStore_Proto::UnaryInsert_REQ insertReq;
    insertReq.set_rtype(XStore_Proto::UNARYINSERT);
    insertReq.set_dbname("ZMQ_REMOVE_DB");
    insertReq.set_timestamp(TS_2005_MID);
    *insertReq.mutable_data() = MakeRow({FBool(false)});
    SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(insertReq);

    XStore_Proto::RemoveDBFiles_REQ removeFilesReq;
    removeFilesReq.set_rtype(XStore_Proto::REMOVEDBFILES);
    removeFilesReq.set_dbname("ZMQ_REMOVE_DB");
    removeFilesReq.add_filename("2005.XSTORE");
    auto removeFilesRep = SendRequest<XStore_Proto::RemoveDBFiles_REQ, XStore_Proto::RemoveDBFiles_REP>(removeFilesReq);
    ASSERT_EQ(removeFilesRep.removestatus_size(), 1);
    EXPECT_NE(removeFilesRep.removestatus(0).find("[INFO]"), std::string::npos);

    XStore_Proto::RemoveDB_REQ removeDbReq;
    removeDbReq.set_rtype(XStore_Proto::REMOVEDB);
    removeDbReq.set_dbname("ZMQ_REMOVE_DB");
    auto removeDbRep = SendRequest<XStore_Proto::RemoveDB_REQ, XStore_Proto::RemoveDB_REP>(removeDbReq);
    EXPECT_EQ(removeDbRep.removestatus(), "[INFO] [REMOVE DB] [DB: ZMQ_REMOVE_DB] - Removed DB with name: ZMQ_REMOVE_DB");
}
