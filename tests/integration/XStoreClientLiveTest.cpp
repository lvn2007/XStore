// Exercises XStore_Client against a *real, already-running* XStore server
// reachable over the actual network (not the loopback in-process server used
// by XStoreZmqNetworkTest, and not a direct XStore:: call like XStoreTest).
// This is the one place in the suite that proves the client and server
// binaries -- built and deployed independently, potentially on different
// machines -- actually agree on the wire protocol end to end.
//
// Disabled by default: every TEST_F here is a live network call, so it only
// runs when both XSTORE_LIVE_ADDR and XSTORE_LIVE_PORT are set in the
// environment, e.g.:
//   XSTORE_LIVE_ADDR=64.131.114.155 XSTORE_LIVE_PORT=19497 \
//       ./xstore_client_tests --gtest_filter='XStoreClientLiveTest.*'
// With neither set (the default for a plain build/ctest run) every test
// GTEST_SKIPs immediately, so this file never touches the network or fails a
// build that has no live server to talk to.
//
// Deliberately does NOT include XStoreTestSupport.h -- that header pulls in
// the *server*-side generated XStore.pb.h (compiled as part of the root
// XStore project), while this binary links XStoreClient's own independently
// generated copy of the same bindings. Mixing the two in one translation
// unit would risk an ODR violation (see XStoreClientTest.cpp's file header
// for the same rationale). Row-building helpers are reimplemented locally
// on top of XStore_Client.h's own XStore_FData factory functions instead.
#include <gtest/gtest.h>

#include <cstdlib>
#include <ctime>
#include <map>
#include <memory>
#include <string>
#include <unistd.h>
#include <utility>
#include <vector>

#include "XStore_Client.h"

namespace {

// Read lazily (not via a custom main()) so this file can share gtest_main
// with XStoreClientTest.cpp in the same xstore_client_tests binary instead
// of fighting it over ownership of main().
bool LiveServerEnv(std::string &addr, std::string &port) {
    const char *Addr = std::getenv("XSTORE_LIVE_ADDR");
    const char *Port = std::getenv("XSTORE_LIVE_PORT");
    if (Addr && Port && *Addr && *Port) {
        addr = Addr;
        port = Port;
        return true;
    }
    return false;
}

XStore_Proto::RowData MakeRow(std::vector<XStore_Proto::FData> fields) {
    XStore_Proto::RowData Row;
    for (auto &F : fields) {
        *Row.add_fielddata() = std::move(F);
    }
    return Row;
}

// Unique per test-binary-invocation DB name prefix so concurrent runs from
// several client machines against the same live server never collide on a
// DB/file name.
std::string UniqueDbName(const std::string &suffix) {
    char Host[256] = {0};
    gethostname(Host, sizeof(Host) - 1);
    return "LIVE_" + suffix + "_" + std::string(Host) + "_" + std::to_string(getpid());
}

}  // namespace

class XStoreClientLiveTest : public ::testing::Test {
 protected:
  std::unique_ptr<XStore_Client> Client;

  void SetUp() override {
      std::string Addr, Port;
      if (!LiveServerEnv(Addr, Port)) {
          GTEST_SKIP() << "XSTORE_LIVE_ADDR/XSTORE_LIVE_PORT not set -- skipping live-network client test";
      }
      Client = std::make_unique<XStore_Client>(Addr, Port, /*timeoutMs=*/10000);
  }

  void CreateDb(const std::string &dbName, short rowLength = 128) {
      XStore_Proto::TimeUnit Resolution = XStore_Proto::SECOND;
      std::string DbName = dbName;
      std::string Status = Client->CreateDB(DbName, Resolution, rowLength);
      ASSERT_NE(Status.find("[INFO]"), std::string::npos)
          << "CreateDB(" << dbName << ") over the real network failed: " << Status;
  }

  // Best-effort cleanup -- not asserted, so a failure here doesn't mask the
  // actual test's result.
  void DropDb(const std::string &dbName, const std::vector<std::string> &files) {
      std::string DbName = dbName;
      if (!files.empty()) {
          std::vector<std::string> Files = files;
          Client->RemoveDBFiles(DbName, Files);
      }
      Client->RemoveDB(DbName);
  }
};

// ---------------------------------------------------------------------
// UNARY -- insert several distinct timestamps individually, confirm each is
// independently readable back from the live server with the exact fields.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, UnaryInsertedDataActuallyAppearsOnServer) {
    const std::string DbName = UniqueDbName("UNARY");
    CreateDb(DbName);

    struct Case {
        std::time_t Ts;
        double DoubleVal;
        std::string StringVal;
        bool BoolVal;
    };
    const std::vector<Case> Cases = {
        {1113825600, 2.71828, "unary-real-network-a", true},
        {1343995200, -9.5, "unary-real-network-b", false},
        {1592222400, 0.0, "unary-real-network-c", true},
    };

    for (const auto &C : Cases) {
        std::time_t Ts = C.Ts;
        std::string DbNameArg = DbName;
        XStore_Proto::RowData Row = MakeRow({XStore_FData::Double(C.DoubleVal),
                                              XStore_FData::String(C.StringVal),
                                              XStore_FData::Bool(C.BoolVal)});
        std::string Status = Client->UnaryInsert(Ts, Row, DbNameArg);
        ASSERT_NE(Status.find("[INFO]"), std::string::npos) << "UnaryInsert failed: " << Status;
    }

    // Query each one back over a fresh round trip and verify it matches what
    // was actually sent -- this is the "data actually appears on Server"
    // check, not just "the insert call returned success".
    for (const auto &C : Cases) {
        std::time_t Ts = C.Ts;
        std::string DbNameArg = DbName;
        XStore_Proto::UnaryQuery_REP Rep = Client->UnaryQuery(Ts, DbNameArg);
        EXPECT_EQ(Rep.timestamp(), static_cast<uint64_t>(C.Ts));
        ASSERT_EQ(Rep.data().fielddata_size(), 3) << "timestamp " << C.Ts;
        EXPECT_DOUBLE_EQ(Rep.data().fielddata(0).doubledata(), C.DoubleVal);
        EXPECT_EQ(Rep.data().fielddata(1).stringdata(), C.StringVal);
        EXPECT_EQ(Rep.data().fielddata(2).booldata(), C.BoolVal);
    }

    DropDb(DbName, {"2005.XSTORE", "2012.XSTORE", "2020.XSTORE"});
}

TEST_F(XStoreClientLiveTest, UnaryDeletedDataIsGoneOnServer) {
    const std::string DbName = UniqueDbName("UNARYDEL");
    CreateDb(DbName);

    std::time_t Ts = 1856260800;  // 2028-10-27
    std::string DbNameArg = DbName;
    XStore_Proto::RowData Row = MakeRow({XStore_FData::Int64(424242)});
    std::string InsertStatus = Client->UnaryInsert(Ts, Row, DbNameArg);
    ASSERT_NE(InsertStatus.find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP BeforeDelete = Client->UnaryQuery(Ts, DbNameArg);
    ASSERT_EQ(BeforeDelete.data().fielddata_size(), 1);

    std::string DeleteStatus = Client->UnaryDelete(Ts, DbNameArg);
    EXPECT_NE(DeleteStatus.find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP AfterDelete = Client->UnaryQuery(Ts, DbNameArg);
    EXPECT_EQ(AfterDelete.data().fielddata_size(), 0) << "deleted row should be gone on the server";

    DropDb(DbName, {"2028.XSTORE"});
}

// ---------------------------------------------------------------------
// BATCH -- insert an arbitrary set of timestamps in one call, then query a
// mix of inserted + never-inserted timestamps and confirm the server
// reflects exactly what was written (and nothing else).
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, BatchInsertedDataActuallyAppearsOnServer) {
    const std::string DbName = UniqueDbName("BATCH");
    CreateDb(DbName);

    const uint64_t Ts1 = 1343995200ULL;       // 2012
    const uint64_t Ts2 = 1343995200ULL + 5;   // 2012, same file
    const uint64_t Ts3 = 1592222400ULL;       // 2020, different file
    const uint64_t TsHole = 1343995200ULL + 50;  // never inserted

    std::string DbNameArg = DbName;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> Rows = {
        {Ts1, MakeRow({XStore_FData::Int32(-777)})},
        {Ts2, MakeRow({XStore_FData::Int32(888)})},
        {Ts3, MakeRow({XStore_FData::UInt64(123456789ULL), XStore_FData::Bytes("batch-two-cols")})},
    };
    std::string InsertStatus = Client->BatchInsert(DbNameArg, Rows);
    EXPECT_NE(InsertStatus.find("Inserted 3 elements"), std::string::npos) << InsertStatus;

    std::vector<uint64_t> QueryTimestamps = {Ts1, TsHole, Ts2, Ts3};
    XStore_Proto::BatchQuery_REP Rep = Client->BatchQuery(DbNameArg, QueryTimestamps);

    std::map<uint64_t, XStore_Proto::UnaryQuery_REP> ByTs;
    for (int i = 0; i < Rep.queryresponse_size(); i++) {
        ByTs[Rep.queryresponse(i).timestamp()] = Rep.queryresponse(i);
    }

    ASSERT_EQ(ByTs.count(TsHole), 0u) << "never-inserted timestamp should be omitted, not fabricated";
    ASSERT_EQ(ByTs.count(Ts1), 1u);
    ASSERT_EQ(ByTs[Ts1].data().fielddata_size(), 1);
    EXPECT_EQ(ByTs[Ts1].data().fielddata(0).int32data(), -777);

    ASSERT_EQ(ByTs.count(Ts2), 1u);
    EXPECT_EQ(ByTs[Ts2].data().fielddata(0).int32data(), 888);

    ASSERT_EQ(ByTs.count(Ts3), 1u);
    ASSERT_EQ(ByTs[Ts3].data().fielddata_size(), 2);
    EXPECT_EQ(ByTs[Ts3].data().fielddata(0).uint64data(), 123456789ULL);
    EXPECT_EQ(ByTs[Ts3].data().fielddata(1).bytesdata(), "batch-two-cols");

    DropDb(DbName, {"2012.XSTORE", "2020.XSTORE"});
}

// ---------------------------------------------------------------------
// RANGE -- insert a contiguous span with an internal hole, confirm the
// server returns every offset in range (hole included as an empty row) with
// the endpoints holding the exact values written.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, RangeInsertedDataActuallyAppearsOnServer) {
    const std::string DbName = UniqueDbName("RANGE");
    CreateDb(DbName);

    const uint64_t Start = 1856260800ULL;  // 2028-10-27
    const uint64_t End = Start + 3;

    std::string DbNameArg = DbName;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> Rows = {
        {Start, MakeRow({XStore_FData::SFixed64(-4242)})},
        // Start+1 and Start+2 deliberately left as holes.
        {End, MakeRow({XStore_FData::SFixed64(4242)})},
    };
    std::string InsertStatus = Client->RangeInsert(DbNameArg, Rows);
    ASSERT_NE(InsertStatus.find("[INFO]"), std::string::npos) << InsertStatus;

    XStore_Proto::RangeQuery_REP Rep = Client->RangeQuery(Start, End, DbNameArg);
    ASSERT_EQ(Rep.queryresponse_size(), 4);
    EXPECT_EQ(Rep.queryresponse(0).data().fielddata(0).sfixed64data(), -4242);
    EXPECT_EQ(Rep.queryresponse(1).data().fielddata_size(), 0) << "hole at Start+1";
    EXPECT_EQ(Rep.queryresponse(2).data().fielddata_size(), 0) << "hole at Start+2";
    EXPECT_EQ(Rep.queryresponse(3).data().fielddata(0).sfixed64data(), 4242);

    DropDb(DbName, {"2028.XSTORE"});
}

// ---------------------------------------------------------------------
// AGGREGATES -- Sum/Avg over a range with known values, computed independently
// here and compared against what the live server reports.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, SumAndAvgAggregateOverRealNetworkMatchExpectedValues) {
    const std::string DbName = UniqueDbName("AGG");
    CreateDb(DbName);

    const uint64_t Start = 1592222400ULL;  // 2020
    std::string DbNameArg = DbName;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> Rows;
    int64_t ExpectedSum = 0;
    const int N = 10;
    for (int i = 0; i < N; i++) {
        int64_t Val = i * 7 - 3;
        Rows.push_back({Start + i, MakeRow({XStore_FData::Int64(Val)})});
        ExpectedSum += Val;
    }
    std::string InsertStatus = Client->RangeInsert(DbNameArg, Rows);
    ASSERT_NE(InsertStatus.find("[INFO]"), std::string::npos) << InsertStatus;

    XStore_Proto::Sum_REP SumRep = Client->Sum(Start, Start + N - 1, DbNameArg, /*columnIdx=*/0);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), static_cast<double>(ExpectedSum));

    XStore_Proto::Avg_REP AvgRep = Client->Avg(Start, Start + N - 1, DbNameArg, /*columnIdx=*/0);
    EXPECT_DOUBLE_EQ(AvgRep.opresult(), static_cast<double>(ExpectedSum) / N);

    DropDb(DbName, {"2020.XSTORE"});
}
