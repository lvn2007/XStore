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
    const char *env_addr = std::getenv("XSTORE_LIVE_ADDR");
    const char *env_port = std::getenv("XSTORE_LIVE_PORT");
    if (env_addr && env_port && *env_addr && *env_port) {
        addr = env_addr;
        port = env_port;
        return true;
    }
    return false;
}

XStore_Proto::RowData MakeRow(std::vector<XStore_Proto::FData> fields) {
    XStore_Proto::RowData row;
    for (auto &f : fields) {
        *row.add_fielddata() = std::move(f);
    }
    return row;
}

// Unique per test-binary-invocation DB name prefix so concurrent runs from
// several client machines against the same live server never collide on a
// DB/file name.
std::string UniqueDbName(const std::string &suffix) {
    char host[256] = {0};
    gethostname(host, sizeof(host) - 1);
    return "LIVE_" + suffix + "_" + std::string(host) + "_" + std::to_string(getpid());
}

}  // namespace

class XStoreClientLiveTest : public ::testing::Test {
 protected:
  std::unique_ptr<XStoreClient> client;

  void SetUp() override {
      std::string addr, port;
      if (!LiveServerEnv(addr, port)) {
          GTEST_SKIP() << "XSTORE_LIVE_ADDR/XSTORE_LIVE_PORT not set -- skipping live-network client test";
      }
      client = std::make_unique<XStoreClient>(addr, port, /*timeoutMs=*/10000);
  }

  void CreateDb(const std::string &db_name, short row_length = 128) {
      XStore_Proto::TimeUnit resolution = XStore_Proto::SECOND;
      std::string mutable_db_name = db_name;
      std::string status = client->CreateDB(mutable_db_name, resolution, row_length);
      ASSERT_NE(status.find("[INFO]"), std::string::npos)
          << "CreateDB(" << db_name << ") over the real network failed: " << status;
  }

  // Best-effort cleanup -- not asserted, so a failure here doesn't mask the
  // actual test's result.
  void DropDb(const std::string &db_name, const std::vector<std::string> &files) {
      std::string mutable_db_name = db_name;
      if (!files.empty()) {
          std::vector<std::string> mutable_files = files;
          client->RemoveDBFiles(mutable_db_name, mutable_files);
      }
      client->RemoveDB(mutable_db_name);
  }
};

// ---------------------------------------------------------------------
// UNARY -- insert several distinct timestamps individually, confirm each is
// independently readable back from the live server with the exact fields.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, UnaryInsertedDataActuallyAppearsOnServer) {
    const std::string db_name = UniqueDbName("UNARY");
    CreateDb(db_name);

    struct Case {
        std::time_t ts;
        double double_val;
        std::string string_val;
        bool bool_val;
    };
    const std::vector<Case> cases = {
        {1113825600, 2.71828, "unary-real-network-a", true},
        {1343995200, -9.5, "unary-real-network-b", false},
        {1592222400, 0.0, "unary-real-network-c", true},
    };

    for (const auto &c : cases) {
        std::time_t ts = c.ts;
        std::string db_name_arg = db_name;
        XStore_Proto::RowData row = MakeRow({xstore_fdata::Double(c.double_val),
                                              xstore_fdata::String(c.string_val),
                                              xstore_fdata::Bool(c.bool_val)});
        std::string status = client->UnaryInsert(ts, row, db_name_arg);
        ASSERT_NE(status.find("[INFO]"), std::string::npos) << "UnaryInsert failed: " << status;
    }

    // Query each one back over a fresh round trip and verify it matches what
    // was actually sent -- this is the "data actually appears on Server"
    // check, not just "the insert call returned success".
    for (const auto &c : cases) {
        std::time_t ts = c.ts;
        std::string db_name_arg = db_name;
        XStore_Proto::UnaryQuery_REP rep = client->UnaryQuery(ts, db_name_arg);
        EXPECT_EQ(rep.timestamp(), static_cast<uint64_t>(c.ts));
        ASSERT_EQ(rep.data().fielddata_size(), 3) << "timestamp " << c.ts;
        EXPECT_DOUBLE_EQ(rep.data().fielddata(0).doubledata(), c.double_val);
        EXPECT_EQ(rep.data().fielddata(1).stringdata(), c.string_val);
        EXPECT_EQ(rep.data().fielddata(2).booldata(), c.bool_val);
    }

    DropDb(db_name, {"2005.XSTORE", "2012.XSTORE", "2020.XSTORE"});
}

TEST_F(XStoreClientLiveTest, UnaryDeletedDataIsGoneOnServer) {
    const std::string db_name = UniqueDbName("UNARYDEL");
    CreateDb(db_name);

    std::time_t ts = 1856260800;  // 2028-10-27
    std::string db_name_arg = db_name;
    XStore_Proto::RowData row = MakeRow({xstore_fdata::Int64(424242)});
    std::string insert_status = client->UnaryInsert(ts, row, db_name_arg);
    ASSERT_NE(insert_status.find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP before_delete = client->UnaryQuery(ts, db_name_arg);
    ASSERT_EQ(before_delete.data().fielddata_size(), 1);

    std::string delete_status = client->UnaryDelete(ts, db_name_arg);
    EXPECT_NE(delete_status.find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP after_delete = client->UnaryQuery(ts, db_name_arg);
    EXPECT_EQ(after_delete.data().fielddata_size(), 0) << "deleted row should be gone on the server";

    DropDb(db_name, {"2028.XSTORE"});
}

// ---------------------------------------------------------------------
// BATCH -- insert an arbitrary set of timestamps in one call, then query a
// mix of inserted + never-inserted timestamps and confirm the server
// reflects exactly what was written (and nothing else).
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, BatchInsertedDataActuallyAppearsOnServer) {
    const std::string db_name = UniqueDbName("BATCH");
    CreateDb(db_name);

    const uint64_t ts1 = 1343995200ULL;       // 2012
    const uint64_t ts2 = 1343995200ULL + 5;   // 2012, same file
    const uint64_t ts3 = 1592222400ULL;       // 2020, different file
    const uint64_t ts_hole = 1343995200ULL + 50;  // never inserted

    std::string db_name_arg = db_name;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows = {
        {ts1, MakeRow({xstore_fdata::Int32(-777)})},
        {ts2, MakeRow({xstore_fdata::Int32(888)})},
        {ts3, MakeRow({xstore_fdata::UInt64(123456789ULL), xstore_fdata::Bytes("batch-two-cols")})},
    };
    std::string insert_status = client->BatchInsert(db_name_arg, rows);
    EXPECT_NE(insert_status.find("Inserted 3 elements"), std::string::npos) << insert_status;

    std::vector<uint64_t> query_timestamps = {ts1, ts_hole, ts2, ts3};
    XStore_Proto::BatchQuery_REP rep = client->BatchQuery(db_name_arg, query_timestamps);

    std::map<uint64_t, XStore_Proto::UnaryQuery_REP> by_ts;
    for (int i = 0; i < rep.queryresponse_size(); i++) {
        by_ts[rep.queryresponse(i).timestamp()] = rep.queryresponse(i);
    }

    ASSERT_EQ(by_ts.count(ts_hole), 0u) << "never-inserted timestamp should be omitted, not fabricated";
    ASSERT_EQ(by_ts.count(ts1), 1u);
    ASSERT_EQ(by_ts[ts1].data().fielddata_size(), 1);
    EXPECT_EQ(by_ts[ts1].data().fielddata(0).int32data(), -777);

    ASSERT_EQ(by_ts.count(ts2), 1u);
    EXPECT_EQ(by_ts[ts2].data().fielddata(0).int32data(), 888);

    ASSERT_EQ(by_ts.count(ts3), 1u);
    ASSERT_EQ(by_ts[ts3].data().fielddata_size(), 2);
    EXPECT_EQ(by_ts[ts3].data().fielddata(0).uint64data(), 123456789ULL);
    EXPECT_EQ(by_ts[ts3].data().fielddata(1).bytesdata(), "batch-two-cols");

    DropDb(db_name, {"2012.XSTORE", "2020.XSTORE"});
}

// ---------------------------------------------------------------------
// RANGE -- insert a contiguous span with an internal hole, confirm the
// server returns every offset in range (hole included as an empty row) with
// the endpoints holding the exact values written.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, RangeInsertedDataActuallyAppearsOnServer) {
    const std::string db_name = UniqueDbName("RANGE");
    CreateDb(db_name);

    const uint64_t start = 1856260800ULL;  // 2028-10-27
    const uint64_t end = start + 3;

    std::string db_name_arg = db_name;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows = {
        {start, MakeRow({xstore_fdata::SFixed64(-4242)})},
        // start+1 and start+2 deliberately left as holes.
        {end, MakeRow({xstore_fdata::SFixed64(4242)})},
    };
    std::string insert_status = client->RangeInsert(db_name_arg, rows);
    ASSERT_NE(insert_status.find("[INFO]"), std::string::npos) << insert_status;

    XStore_Proto::RangeQuery_REP rep = client->RangeQuery(start, end, db_name_arg);
    ASSERT_EQ(rep.queryresponse_size(), 4);
    EXPECT_EQ(rep.queryresponse(0).data().fielddata(0).sfixed64data(), -4242);
    EXPECT_EQ(rep.queryresponse(1).data().fielddata_size(), 0) << "hole at Start+1";
    EXPECT_EQ(rep.queryresponse(2).data().fielddata_size(), 0) << "hole at Start+2";
    EXPECT_EQ(rep.queryresponse(3).data().fielddata(0).sfixed64data(), 4242);

    DropDb(db_name, {"2028.XSTORE"});
}

// ---------------------------------------------------------------------
// AGGREGATES -- Sum/Avg over a range with known values, computed independently
// here and compared against what the live server reports.
// ---------------------------------------------------------------------
TEST_F(XStoreClientLiveTest, SumAndAvgAggregateOverRealNetworkMatchExpectedValues) {
    const std::string db_name = UniqueDbName("AGG");
    CreateDb(db_name);

    const uint64_t start = 1592222400ULL;  // 2020
    std::string db_name_arg = db_name;
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    int64_t expected_sum = 0;
    const int n = 10;
    for (int i = 0; i < n; i++) {
        int64_t val = i * 7 - 3;
        rows.push_back({start + i, MakeRow({xstore_fdata::Int64(val)})});
        expected_sum += val;
    }
    std::string insert_status = client->RangeInsert(db_name_arg, rows);
    ASSERT_NE(insert_status.find("[INFO]"), std::string::npos) << insert_status;

    XStore_Proto::Sum_REP sum_rep = client->Sum(start, start + n - 1, db_name_arg, /*columnIdx=*/0);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), static_cast<double>(expected_sum));

    XStore_Proto::Avg_REP avg_rep = client->Avg(start, start + n - 1, db_name_arg, /*columnIdx=*/0);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), static_cast<double>(expected_sum) / n);

    DropDb(db_name, {"2020.XSTORE"});
}
