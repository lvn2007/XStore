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
  static ScratchDir *Scratch;
  static zmq::context_t *ClientCtx;
  static zmq::socket_t *ClientSocket;

  static void SetUpTestSuite() {
      // Leaked deliberately (see file header comment): the worker's XStore
      // instance captures this cwd as its baseDir at thread-start time and
      // keeps writing there for the rest of the process's life.
      Scratch = new ScratchDir("xstore_zmq_test");

      int Port = 25500 + static_cast<int>(getpid() % 4000);
      std::string ServerAddr = "tcp://127.0.0.1:" + std::to_string(Port);

      auto *Server = new ZMQ_Server(1);  // single worker: client is a single REQ socket anyway
      std::thread ServerThread([Server, Port]() {
          Server->StartServer("127.0.0.1", std::to_string(Port));
      });
      ServerThread.detach();

      ClientCtx = new zmq::context_t(1);
      ClientSocket = new zmq::socket_t(*ClientCtx, zmq::socket_type::req);
      ClientSocket->set(zmq::sockopt::rcvtimeo, 5000);
      ClientSocket->set(zmq::sockopt::linger, 0);
      // ZeroMQ queues connects lazily/retries internally, so connecting
      // before bind() completes on the server thread is safe.
      ClientSocket->connect(ServerAddr);

      // Small head start so the first request in the first test doesn't pay
      // for ZeroMQ's initial TCP connect/reconnect backoff.
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  static void TearDownTestSuite() {
      delete ClientSocket;
      delete ClientCtx;
      delete Scratch;  // removes the scratch DataStore dir; server thread is left dangling, harmless at process exit
  }

  template <typename ReqT, typename RepT>
  RepT SendRequest(const ReqT &req) {
      std::string Payload = req.SerializeAsString();
      auto SendResult = ClientSocket->send(zmq::buffer(Payload), zmq::send_flags::none);
      RepT Rep;
      if (!SendResult) {
          ADD_FAILURE() << "failed to send request over ZMQ";
          return Rep;
      }
      zmq::message_t Reply;
      auto RecvResult = ClientSocket->recv(Reply, zmq::recv_flags::none);
      if (!RecvResult) {
          ADD_FAILURE() << "no reply received from ZMQ server within timeout";
          return Rep;
      }
      Rep.ParseFromArray(Reply.data(), Reply.size());
      return Rep;
  }
};

ScratchDir *XStoreZmqNetworkTest::Scratch = nullptr;
zmq::context_t *XStoreZmqNetworkTest::ClientCtx = nullptr;
zmq::socket_t *XStoreZmqNetworkTest::ClientSocket = nullptr;

// ---------------------------------------------------------------------
// INFO / CREATEDB
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, InfoQueryOverWireReflectsCreatedDatabase) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_INFO_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    auto CreateRep = SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);
    EXPECT_EQ(CreateRep.createdbstatus(), "[INFO] [CREATE DB] [DB: ZMQ_INFO_DB] - Created DB");

    XStore_Proto::ReqCMD InfoReq;
    InfoReq.set_rtype(XStore_Proto::INFO);
    auto InfoRep = SendRequest<XStore_Proto::ReqCMD, XStore_Proto::SysInfo_REP>(InfoReq);

    bool Found = false;
    for (int i = 0; i < InfoRep.dbinfo_size(); i++) {
        if (InfoRep.dbinfo(i).dbname() == "ZMQ_INFO_DB") Found = true;
    }
    EXPECT_TRUE(Found) << "expected ZMQ_INFO_DB to be reported by INFO over the wire";
}

TEST_F(XStoreZmqNetworkTest, CreateDBOverWireRejectsDuplicateName) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_DUP_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    auto SecondRep = SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);
    EXPECT_EQ(SecondRep.createdbstatus(), "[ERROR] [CREATE DB] [DB: ZMQ_DUP_DB] - Already existed");
}

// ---------------------------------------------------------------------
// UNARY INSERT / QUERY / DELETE
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, UnaryInsertThenUnaryQueryRoundTripsDataOverWire) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_UNARY_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    XStore_Proto::UnaryInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::UNARYINSERT);
    InsertReq.set_dbname("ZMQ_UNARY_DB");
    InsertReq.set_timestamp(TS_2005_MID);
    *InsertReq.mutable_data() = MakeRow({FDouble(2.71828), FString("wire-data"), FBool(true)});
    auto InsertRep = SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(InsertReq);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ QueryReq;
    QueryReq.set_rtype(XStore_Proto::UNARYQUERY);
    QueryReq.set_dbname("ZMQ_UNARY_DB");
    QueryReq.set_timestamp(TS_2005_MID);
    auto QueryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(QueryReq);

    EXPECT_EQ(QueryRep.timestamp(), TS_2005_MID);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(QueryRep.data().fielddata(0).doubledata(), 2.71828);
    EXPECT_EQ(QueryRep.data().fielddata(1).stringdata(), "wire-data");
    EXPECT_EQ(QueryRep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreZmqNetworkTest, UnaryQueryOverWireOnMissingDBReturnsError) {
    XStore_Proto::UnaryQuery_REQ QueryReq;
    QueryReq.set_rtype(XStore_Proto::UNARYQUERY);
    QueryReq.set_dbname("ZMQ_NOPE_DB");
    QueryReq.set_timestamp(TS_2005_MID);
    auto QueryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(QueryReq);
    EXPECT_EQ(QueryRep.querystatus(), "[ERROR] [UNARY QUERY] [DB: ZMQ_NOPE_DB] - DB does not exist");
}

TEST_F(XStoreZmqNetworkTest, UnaryDeleteOverWirePunchesHoleThenQueryReturnsEmpty) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_DELETE_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    XStore_Proto::UnaryInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::UNARYINSERT);
    InsertReq.set_dbname("ZMQ_DELETE_DB");
    InsertReq.set_timestamp(TS_2012_MID);
    *InsertReq.mutable_data() = MakeRow({FString("to-be-deleted")});
    SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(InsertReq);

    XStore_Proto::UnaryDelete_REQ DeleteReq;
    DeleteReq.set_rtype(XStore_Proto::UNARYDELETE);
    DeleteReq.set_dbname("ZMQ_DELETE_DB");
    DeleteReq.set_timestamp(TS_2012_MID);
    auto DeleteRep = SendRequest<XStore_Proto::UnaryDelete_REQ, XStore_Proto::UnaryDelete_REP>(DeleteReq);
    EXPECT_NE(DeleteRep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REQ QueryReq;
    QueryReq.set_rtype(XStore_Proto::UNARYQUERY);
    QueryReq.set_dbname("ZMQ_DELETE_DB");
    QueryReq.set_timestamp(TS_2012_MID);
    auto QueryRep = SendRequest<XStore_Proto::UnaryQuery_REQ, XStore_Proto::UnaryQuery_REP>(QueryReq);
    EXPECT_EQ(QueryRep.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// BATCH INSERT / QUERY -- mixed column counts + holes, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, BatchInsertThenBatchQueryOverWireOmitsHoles) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_BATCH_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    const unsigned long long Ts1 = TS_2012_MID;      // 1 column
    const unsigned long long Ts2 = TS_2020_MID;      // 2 columns, different year
    const unsigned long long TsHole = TS_2012_MID + 50;  // never inserted

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::BATCHINSERT);
    InsertReq.set_dbname("ZMQ_BATCH_DB");

    XStore_Proto::BatchInsert_DATA *Entry1 = InsertReq.add_batchinsert_data();
    Entry1->set_timestamp(Ts1);
    *Entry1->mutable_data() = MakeRow({FInt32(-55)});

    XStore_Proto::BatchInsert_DATA *Entry2 = InsertReq.add_batchinsert_data();
    Entry2->set_timestamp(Ts2);
    *Entry2->mutable_data() = MakeRow({FUInt64(123456789ULL), FBytes("two-cols")});

    auto InsertRep = SendRequest<XStore_Proto::BatchInsert_REQ, XStore_Proto::BatchInsert_REP>(InsertReq);
    EXPECT_NE(InsertRep.insertstatus().find("Inserted 2 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_rtype(XStore_Proto::BATCHQUERY);
    QueryReq.set_dbname("ZMQ_BATCH_DB");
    QueryReq.add_timestamp(Ts1);
    QueryReq.add_timestamp(TsHole);
    QueryReq.add_timestamp(Ts2);

    auto QueryRep = SendRequest<XStore_Proto::BatchQuery_REQ, XStore_Proto::BatchQuery_REP>(QueryReq);

    ASSERT_EQ(QueryRep.queryresponse_size(), 2);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> ByTs;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        ByTs[QueryRep.queryresponse(i).timestamp()] = QueryRep.queryresponse(i);
    }
    ASSERT_EQ(ByTs.count(TsHole), 0u);
    ASSERT_EQ(ByTs[Ts1].data().fielddata_size(), 1);
    EXPECT_EQ(ByTs[Ts1].data().fielddata(0).int32data(), -55);
    ASSERT_EQ(ByTs[Ts2].data().fielddata_size(), 2);
    EXPECT_EQ(ByTs[Ts2].data().fielddata(0).uint64data(), 123456789ULL);
    EXPECT_EQ(ByTs[Ts2].data().fielddata(1).bytesdata(), "two-cols");
}

TEST_F(XStoreZmqNetworkTest, BatchInsertOverWireToNonexistentDBReturnsError) {
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::BATCHINSERT);
    InsertReq.set_dbname("ZMQ_NOPE_BATCH_DB");
    XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
    Entry->set_timestamp(TS_2012_MID);
    *Entry->mutable_data() = MakeRow({FDouble(1.0)});

    auto InsertRep = SendRequest<XStore_Proto::BatchInsert_REQ, XStore_Proto::BatchInsert_REP>(InsertReq);
    EXPECT_EQ(InsertRep.insertstatus(), "[ERROR] [BATCH INSERT] [DB: ZMQ_NOPE_BATCH_DB] - DB does not exist");
}

// ---------------------------------------------------------------------
// RANGE INSERT / QUERY -- contiguous range with an internal hole, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RangeInsertThenRangeQueryOverWireIncludesHoleAsEmptyEntry) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_RANGE_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    // Sparse range insert: endpoints only, leaving a hole at TS_2028_MID + 1.
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::RANGEINSERT);
    InsertReq.set_dbname("ZMQ_RANGE_DB");
    XStore_Proto::RangeInsert_DATA *First = InsertReq.add_rangeinsert_data();
    First->set_timestamp(TS_2028_MID);
    *First->mutable_data() = MakeRow({FSFixed64(-42)});
    XStore_Proto::RangeInsert_DATA *Last = InsertReq.add_rangeinsert_data();
    Last->set_timestamp(TS_2028_MID + 2);
    *Last->mutable_data() = MakeRow({FSFixed64(42)});
    SendRequest<XStore_Proto::RangeInsert_REQ, XStore_Proto::RangeInsert_REP>(InsertReq);

    XStore_Proto::RangeQuery_REQ QueryReq;
    QueryReq.set_rtype(XStore_Proto::RANGEQUERY);
    QueryReq.set_dbname("ZMQ_RANGE_DB");
    QueryReq.set_starttime(TS_2028_MID);
    QueryReq.set_endtime(TS_2028_MID + 2);

    auto QueryRep = SendRequest<XStore_Proto::RangeQuery_REQ, XStore_Proto::RangeQuery_REP>(QueryReq);

    ASSERT_EQ(QueryRep.queryresponse_size(), 3);
    EXPECT_EQ(QueryRep.queryresponse(0).data().fielddata(0).sfixed64data(), -42);
    EXPECT_EQ(QueryRep.queryresponse(1).data().fielddata_size(), 0) << "hole in the middle of the range";
    EXPECT_EQ(QueryRep.queryresponse(2).data().fielddata(0).sfixed64data(), 42);
}

// ---------------------------------------------------------------------
// REMOVE DB FILES / REMOVE DB -- cleanup path, over the wire
// ---------------------------------------------------------------------

TEST_F(XStoreZmqNetworkTest, RemoveDBFilesThenRemoveDBOverWire) {
    XStore_Proto::CreateDB_REQ CreateReq;
    CreateReq.set_rtype(XStore_Proto::CREATEDB);
    CreateReq.set_dbname("ZMQ_REMOVE_DB");
    CreateReq.set_timeresolution(XStore_Proto::SECOND);
    CreateReq.set_rowlength(128);
    SendRequest<XStore_Proto::CreateDB_REQ, XStore_Proto::CreateDB_REP>(CreateReq);

    XStore_Proto::UnaryInsert_REQ InsertReq;
    InsertReq.set_rtype(XStore_Proto::UNARYINSERT);
    InsertReq.set_dbname("ZMQ_REMOVE_DB");
    InsertReq.set_timestamp(TS_2005_MID);
    *InsertReq.mutable_data() = MakeRow({FBool(false)});
    SendRequest<XStore_Proto::UnaryInsert_REQ, XStore_Proto::UnaryInsert_REP>(InsertReq);

    XStore_Proto::RemoveDBFiles_REQ RemoveFilesReq;
    RemoveFilesReq.set_rtype(XStore_Proto::REMOVEDBFILES);
    RemoveFilesReq.set_dbname("ZMQ_REMOVE_DB");
    RemoveFilesReq.add_filename("2005.XSTORE");
    auto RemoveFilesRep = SendRequest<XStore_Proto::RemoveDBFiles_REQ, XStore_Proto::RemoveDBFiles_REP>(RemoveFilesReq);
    ASSERT_EQ(RemoveFilesRep.removestatus_size(), 1);
    EXPECT_NE(RemoveFilesRep.removestatus(0).find("[INFO]"), std::string::npos);

    XStore_Proto::RemoveDB_REQ RemoveDbReq;
    RemoveDbReq.set_rtype(XStore_Proto::REMOVEDB);
    RemoveDbReq.set_dbname("ZMQ_REMOVE_DB");
    auto RemoveDbRep = SendRequest<XStore_Proto::RemoveDB_REQ, XStore_Proto::RemoveDB_REP>(RemoveDbReq);
    EXPECT_EQ(RemoveDbRep.removestatus(), "[INFO] [REMOVE DB] [DB: ZMQ_REMOVE_DB] - Removed DB with name: ZMQ_REMOVE_DB");
}
