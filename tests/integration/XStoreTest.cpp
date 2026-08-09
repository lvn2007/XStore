// Direct-API tests: exercise the XStore class by calling its public methods
// directly against the filesystem, bypassing the ZMQ networking layer
// entirely -- that layer is a thin proto (de)serialization wrapper around
// these same methods (see XStoreZmqNetworkTest.cpp, which tests it directly).
//
// Sections follow the method declaration order in include/XStore/XStore.h:
// InfoQuery, CreateDB, RemoveDB, RemoveDBFiles, then each of Unary/Batch/Range
// with its Insert+Query+Delete kept together, then the aggregates, then the
// cross-cutting Concurrency section last.
#include <gtest/gtest.h>
#include <algorithm>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <thread>
#include <vector>

#include "XStoreTestSupport.h"

namespace fs = std::filesystem;
using namespace xstore_test;

namespace {

// Anchors are deliberately spread across many different years (instead of
// clustering everything in one) so the suite exercises year-bucketing/file
// creation logic across the board, not just repeatedly against one file.
// TimeStruct buckets years via localtime_r(), so these are only exact when
// TZ=UTC (true in CI).
constexpr unsigned long long TS_1998_MID = 889876800ULL;    // 1998-03-14 12:00:00 UTC
constexpr unsigned long long TS_2003_MID = 1058875200ULL;   // 2003-07-22 12:00:00 UTC
constexpr unsigned long long TS_2009_MID = 1257422400ULL;   // 2009-11-05 12:00:00 UTC
constexpr unsigned long long TS_2016_MID = 1482271200ULL;   // 2016-12-20 22:00:00 UTC
constexpr unsigned long long TS_2016_LAST = 1483228799ULL;  // 2016-12-31 23:59:59 UTC
constexpr unsigned long long TS_2017_FIRST = 1483228800ULL; // 2017-01-01 00:00:00 UTC
constexpr unsigned long long TS_2024_MID = 1717070400ULL;   // 2024-05-30 12:00:00 UTC
constexpr unsigned long long TS_2031_MID = 1946721600ULL;   // 2031-09-09 12:00:00 UTC

// Wide multi-decade epoch bounds used for the genuinely-random test cases,
// so generated timestamps land across many different years rather than
// clustering in one -- roughly 1975-01-01 .. 2045-01-01.
constexpr unsigned long long RANDOM_RANGE_LOW = 157'766'400ULL;
constexpr unsigned long long RANDOM_RANGE_HIGH = 2'366'841'600ULL;

}  // namespace

class XStoreTest : public ::testing::Test {
 protected:
  std::unique_ptr<ScratchDir> scratch;

  void SetUp() override {
      scratch = std::make_unique<ScratchDir>("xstore_test");
  }

  void TearDown() override {
      scratch.reset();
  }
};

// ---------------------------------------------------------------------
// InfoQuery
// ---------------------------------------------------------------------

TEST_F(XStoreTest, InfoQueryOnEmptyServerReturnsNoDatabases) {
    XStore store;
    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 4);

    EXPECT_EQ(info.serveraddr(), "127.0.0.1");
    EXPECT_EQ(info.threadcount(), 4);
    EXPECT_EQ(info.dbinfo_size(), 0);
}

TEST_F(XStoreTest, InfoQueryReflectsCreatedDatabase) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 2);

    ASSERT_EQ(info.dbinfo_size(), 1);
    EXPECT_EQ(info.dbinfo(0).dbname(), "TESTDB");
    EXPECT_EQ(info.dbinfo(0).dbresolution(), XStore_Proto::SECOND);
}

// ---------------------------------------------------------------------
// CreateDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, CreateDBRejectsRowLengthBelowFour) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 3);

    EXPECT_EQ(rep.createdbstatus(), "[ERROR] [CREATE DB] [DB: TESTDB] - Row length must be >= 4");
}

TEST_F(XStoreTest, CreateDBSucceedsAndWritesConfigFile) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: TESTDB] - Created DB");
    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

TEST_F(XStoreTest, CreateDBRejectsDuplicateName) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(rep.createdbstatus(), "[ERROR] [CREATE DB] [DB: TESTDB] - Already existed");
}

// ---------------------------------------------------------------------
// RemoveDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::RemoveDB_REP rep = store.RemoveDB("TESTDB");

    EXPECT_EQ(rep.removestatus(), "[ERROR] [REMOVE DB] [DB: TESTDB] - DB does not exists");
}

TEST_F(XStoreTest, RemoveDBDeletesDirectoryAndDropsFromInfoQuery) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RemoveDB_REP rep = store.RemoveDB("TESTDB");
    EXPECT_EQ(rep.removestatus(), "[INFO] [REMOVE DB] [DB: TESTDB] - Removed DB with name: TESTDB");
    EXPECT_FALSE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB"));

    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 1);
    EXPECT_EQ(info.dbinfo_size(), 0);
}

// ---------------------------------------------------------------------
// RemoveDBFiles
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBFilesRemovesDataFileButProtectsConfig) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &insertRep);

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("2016.XSTORE");
    req.add_filename("TESTDB.CFG");

    XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, "TESTDB");

    ASSERT_EQ(rep.removestatus_size(), 2);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: TESTDB] - Removed 2016.XSTORE");
    EXPECT_EQ(rep.removestatus(1), "[WARNING] [REMOVE DB FILES] [DB: TESTDB] - TESTDB.CFG is prohibited from remove");

    EXPECT_FALSE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

// ---------------------------------------------------------------------
// UnaryInsert / UnaryQuery / UnaryDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, UnaryInsertOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &rep);

    EXPECT_EQ(rep.insertstatus(), "[ERROR] [UNARY INSERT] [DB: TESTDB] - DB does not exist");
}

TEST_F(XStoreTest, UnaryInsertExactTimestampHasInsertedData) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(3.14), FString("abc"), FBool(true)}), "TESTDB", &insertRep);
    EXPECT_EQ(insertRep.insertstatus(), "[INFO] [UNARY INSERT] [DB: TESTDB] - Inserted 1482271200 to 2016.XSTORE");

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &queryRep);

    EXPECT_EQ(queryRep.timestamp(), TS_2016_MID);
    ASSERT_EQ(queryRep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(queryRep.data().fielddata(0).doubledata(), 3.14);
    EXPECT_EQ(queryRep.data().fielddata(1).stringdata(), "abc");
    EXPECT_EQ(queryRep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreTest, UnaryInsertAllProtoDataTypesRoundTrip) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 256);

    // Exercises every branch of FData's oneof (see ProtoFile/XStore.proto),
    // not just the double/string/bool trio used elsewhere in this file.
    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_MID, MakeRow({
        FDouble(3.1415926535),
        FFloat(2.5f),
        FInt32(-7),
        FInt64(-9000000000LL),
        FUInt32(42u),
        FUInt64(18000000000ULL),
        FSInt32(-13),
        FSInt64(-1300000000LL),
        FFixed32(99u),
        FFixed64(9900000000ULL),
        FSFixed32(-99),
        FSFixed64(-9900000000LL),
        FBool(true),
        FString("hello-world"),
        FBytes(std::string("\x00\x01\x02payload", 11)),
    }), "TESTDB", &insertRep);
    EXPECT_NE(insertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &queryRep);

    ASSERT_EQ(queryRep.data().fielddata_size(), 15);
    const auto &d = queryRep.data();
    EXPECT_DOUBLE_EQ(d.fielddata(0).doubledata(), 3.1415926535);
    EXPECT_FLOAT_EQ(d.fielddata(1).floatdata(), 2.5f);
    EXPECT_EQ(d.fielddata(2).int32data(), -7);
    EXPECT_EQ(d.fielddata(3).int64data(), -9000000000LL);
    EXPECT_EQ(d.fielddata(4).uint32data(), 42u);
    EXPECT_EQ(d.fielddata(5).uint64data(), 18000000000ULL);
    EXPECT_EQ(d.fielddata(6).sint32data(), -13);
    EXPECT_EQ(d.fielddata(7).sint64data(), -1300000000LL);
    EXPECT_EQ(d.fielddata(8).fixed32data(), 99u);
    EXPECT_EQ(d.fielddata(9).fixed64data(), 9900000000ULL);
    EXPECT_EQ(d.fielddata(10).sfixed32data(), -99);
    EXPECT_EQ(d.fielddata(11).sfixed64data(), -9900000000LL);
    EXPECT_EQ(d.fielddata(12).booldata(), true);
    EXPECT_EQ(d.fielddata(13).stringdata(), "hello-world");
    EXPECT_EQ(d.fielddata(14).bytesdata(), std::string("\x00\x01\x02payload", 11));
}

TEST_F(XStoreTest, UnaryQueryNeighboringUninsertedTimestampsReturnEmptyData) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FDouble(1.0)}), "TESTDB", &insertRep);

    // Same year file exists, but nothing was ever written at these offsets --
    // this must come back as a normal (non-error) response with zero fields,
    // not as an error, since the file itself exists.
    for (unsigned long long neighbor : {TS_2009_MID - 5, TS_2009_MID + 5, TS_2009_MID + 3600}) {
        XStore_Proto::UnaryQuery_REP queryRep;
        store.UnaryQuery(neighbor, "TESTDB", &queryRep);
        EXPECT_EQ(queryRep.timestamp(), neighbor);
        EXPECT_EQ(queryRep.data().fielddata_size(), 0) << "neighbor=" << neighbor;
    }

    // Sanity: the originally-inserted timestamp is unaffected by the reads above.
    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2009_MID, "TESTDB", &queryRep);
    ASSERT_EQ(queryRep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(queryRep.data().fielddata(0).doubledata(), 1.0);
}

TEST_F(XStoreTest, UnaryInsertOverwriteSameTimestampKeepsLatestValue) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2003_MID, MakeRow({FString("first")}), "TESTDB", &insertRep);
    store.UnaryInsert(TS_2003_MID, MakeRow({FString("second")}), "TESTDB", &insertRep);

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2003_MID, "TESTDB", &queryRep);

    ASSERT_EQ(queryRep.data().fielddata_size(), 1);
    EXPECT_EQ(queryRep.data().fielddata(0).stringdata(), "second");
}

TEST_F(XStoreTest, UnaryQueryWithDbButNoYearFileReturnsError) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);  // DB exists, but nothing was ever inserted for TS_2016_MID's year

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &queryRep);

    EXPECT_EQ(queryRep.querystatus(), "[ERROR] [UNARY QUERY] [DB: TESTDB] - DB does not exist");
}

// Unlike UnaryInsert/BatchQuery/RangeQuery (which all check the DB directory
// or .CFG path exists before opening it), UnaryQuery goes straight to
// fopen(cfgPath) when the dbName isn't already cached in cfgMap. On a fresh
// XStore instance querying a DB that was never created, that fopen() returns
// NULL and the subsequent fileno(NULL) crashes instead of reporting an error.
TEST_F(XStoreTest, UnaryQueryOnNeverCreatedDBReturnsErrorInsteadOfCrashing) {
    XStore store;  // fresh instance: cfgMap starts empty, so this can't be masked by a warm cache
    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2003_MID, "NEVER_CREATED_DB", &queryRep);
    EXPECT_NE(queryRep.querystatus().find("[ERROR]"), std::string::npos);
}

TEST_F(XStoreTest, UnaryInsertDbNameIsCaseInsensitive) {
    XStore store;
    store.CreateDB("testdb", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_1998_MID, MakeRow({FString("mixed-case")}), "TestDb", &insertRep);
    EXPECT_NE(insertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_1998_MID, "TESTDB", &queryRep);
    ASSERT_EQ(queryRep.data().fielddata_size(), 1);
    EXPECT_EQ(queryRep.data().fielddata(0).stringdata(), "mixed-case");
}

TEST_F(XStoreTest, UnaryInsertDifferentYearsWriteToSeparateFiles) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("year-2016")}), "TESTDB", &insertRep);
    store.UnaryInsert(TS_2031_MID, MakeRow({FString("year-2031")}), "TESTDB", &insertRep);

    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "2031.XSTORE"));

    XStore_Proto::UnaryQuery_REP rep2016, rep2031;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &rep2016);
    store.UnaryQuery(TS_2031_MID, "TESTDB", &rep2031);

    ASSERT_EQ(rep2016.data().fielddata_size(), 1);
    ASSERT_EQ(rep2031.data().fielddata_size(), 1);
    EXPECT_EQ(rep2016.data().fielddata(0).stringdata(), "year-2016");
    EXPECT_EQ(rep2031.data().fielddata(0).stringdata(), "year-2031");
}

TEST_F(XStoreTest, UnaryDeletePunchesHoleThenUnaryQueryReturnsEmptyData) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2024_MID, MakeRow({FString("to-be-deleted")}), "TESTDB", &insertRep);

    XStore_Proto::UnaryDelete_REP deleteRep = store.UnaryDelete(TS_2024_MID, "TESTDB");
    EXPECT_NE(deleteRep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP queryRep;
    store.UnaryQuery(TS_2024_MID, "TESTDB", &queryRep);
    EXPECT_EQ(queryRep.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, UnaryDeleteOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::UnaryDelete_REP deleteRep = store.UnaryDelete(TS_2016_MID, "NOPE");
    EXPECT_EQ(deleteRep.deletestatus(), "[ERROR] [UNARY DELETE] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, UnaryDeleteOnMissingYearFileReturnsError) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    // DB exists but nothing was ever inserted for this year -> no 2016.XSTORE file.
    XStore_Proto::UnaryDelete_REP deleteRep = store.UnaryDelete(TS_2016_MID, "TESTDB");
    EXPECT_EQ(deleteRep.deletestatus(), "[ERROR] [UNARY DELETE] [DB: TESTDB] - 2016.XSTORE does not exist");
}

// ---------------------------------------------------------------------
// BatchInsert / BatchQuery / BatchDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, BatchInsertThenBatchQueryExactTimestampsRoundTrip) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Spread across five different years (rather than one clustered block) so
    // this also exercises BatchInsert/BatchQuery's per-year file bucketing.
    std::map<unsigned long long, std::string> expected = {
        {TS_2031_MID, "e"},
        {TS_1998_MID, "a"},
        {TS_2016_MID, "d"},
        {TS_2009_MID, "c"},
        {TS_2003_MID, "b"},
    };

    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (auto &[ts, val] : expected) {
        XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FString(val)});
    }
    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);
    EXPECT_NE(insertRep.insertstatus().find("Inserted 5 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_dbname("TESTDB");
    // Deliberately shuffled/unsorted input -- BatchQuery must sort internally.
    for (unsigned long long ts : {TS_2016_MID, TS_1998_MID, TS_2031_MID, TS_2003_MID, TS_2009_MID}) {
        queryReq.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP queryRep;
    store.BatchQuery(&queryReq, &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 5);
    for (int i = 0; i < queryRep.queryresponse_size(); i++) {
        const auto &entry = queryRep.queryresponse(i);
        ASSERT_TRUE(expected.count(entry.timestamp())) << "unexpected timestamp " << entry.timestamp();
        ASSERT_EQ(entry.data().fielddata_size(), 1);
        EXPECT_EQ(entry.data().fielddata(0).stringdata(), expected[entry.timestamp()]);
    }
}

TEST_F(XStoreTest, BatchQueryHolesAreOmittedFromResults) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Two unrelated years, each with a small gap between its own points, so
    // hole-detection is verified without clustering all data into one year.
    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 15}) {
        XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FUInt32(1)});
    }
    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);

    // Query the 5 inserted timestamps plus holes interleaved in both years --
    // holes must be silently omitted from queryresponse, not returned as empty rows.
    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 5, TS_2009_MID + 10, TS_2009_MID + 15, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 7, TS_2031_MID + 15}) {
        queryReq.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP queryRep;
    store.BatchQuery(&queryReq, &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 5);
    std::set<unsigned long long> returned;
    for (int i = 0; i < queryRep.queryresponse_size(); i++) {
        returned.insert(queryRep.queryresponse(i).timestamp());
    }
    EXPECT_EQ(returned, std::set<unsigned long long>(
        {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20, TS_2031_MID, TS_2031_MID + 15}));
}

TEST_F(XStoreTest, BatchInsertMixedColumnCountsPerTimestamp) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    const unsigned long long ts1 = TS_1998_MID;              // 1 column
    const unsigned long long ts2 = TS_2003_MID;               // 2 columns
    const unsigned long long ts3 = TS_2009_MID;               // 3 columns
    const unsigned long long ts4 = TS_2016_MID;               // 1 column
    const unsigned long long tsHole = TS_2016_MID + 100000;   // never inserted, same year as ts4

    // Deliberately spread across several different oneof branches (not just
    // double/string/bool) so column-count mixing is also type-diverse.
    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    auto addEntry = [&](unsigned long long ts, const std::vector<XStore_Proto::FData> &vals) {
        XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow(vals);
    };
    addEntry(ts1, {FUInt64(42)});
    addEntry(ts2, {FFixed32(7), FBytes("payload-two")});
    addEntry(ts3, {FSInt64(-123456789), FSFixed32(-42), FBool(true)});
    addEntry(ts4, {FInt64(4)});

    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);

    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {ts1, ts2, ts3, ts4, tsHole}) queryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP queryRep;
    store.BatchQuery(&queryReq, &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 4);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> byTs;
    for (int i = 0; i < queryRep.queryresponse_size(); i++) {
        byTs[queryRep.queryresponse(i).timestamp()] = queryRep.queryresponse(i);
    }

    ASSERT_EQ(byTs.count(tsHole), 0u);
    ASSERT_EQ(byTs[ts1].data().fielddata_size(), 1);
    EXPECT_EQ(byTs[ts1].data().fielddata(0).uint64data(), 42u);

    ASSERT_EQ(byTs[ts2].data().fielddata_size(), 2);
    EXPECT_EQ(byTs[ts2].data().fielddata(0).fixed32data(), 7u);
    EXPECT_EQ(byTs[ts2].data().fielddata(1).bytesdata(), "payload-two");

    ASSERT_EQ(byTs[ts3].data().fielddata_size(), 3);
    EXPECT_EQ(byTs[ts3].data().fielddata(0).sint64data(), -123456789);
    EXPECT_EQ(byTs[ts3].data().fielddata(1).sfixed32data(), -42);
    EXPECT_EQ(byTs[ts3].data().fielddata(2).booldata(), true);

    ASSERT_EQ(byTs[ts4].data().fielddata_size(), 1);
    EXPECT_EQ(byTs[ts4].data().fielddata(0).int64data(), 4);
}

TEST_F(XStoreTest, BatchInsertRandomTimestampsThenRandomBatchQuerySubset) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    std::mt19937 rng(12345);
    std::uniform_int_distribution<unsigned long long> epochDist(RANDOM_RANGE_LOW, RANDOM_RANGE_HIGH);

    // Generate a random, deduplicated set of timestamps spread across ~70
    // years (not clustered in one year) and insert each one carrying its own
    // timestamp value as the payload so round-trip correctness can be
    // checked without a side table.
    std::set<unsigned long long> insertedTimestamps;
    while (insertedTimestamps.size() < 40) {
        insertedTimestamps.insert(epochDist(rng));
    }

    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (unsigned long long ts : insertedTimestamps) {
        XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FInt64((int64_t)ts)});
    }
    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);

    // Sanity check: confirm the generated data really is dispersed across
    // several distinct year-files rather than accidentally clustering.
    std::set<int> distinctYears;
    for (unsigned long long ts : insertedTimestamps) {
        std::time_t t = static_cast<std::time_t>(ts);
        struct tm tmStruct;
        gmtime_r(&t, &tmStruct);
        distinctYears.insert(tmStruct.tm_year + 1900);
    }
    EXPECT_GE(distinctYears.size(), 10u) << "expected timestamps to span many distinct years";

    // Random query set: a mix of timestamps that were and weren't inserted.
    std::vector<unsigned long long> insertedVec(insertedTimestamps.begin(), insertedTimestamps.end());
    std::shuffle(insertedVec.begin(), insertedVec.end(), rng);

    std::set<unsigned long long> queriedSet;
    for (int i = 0; i < 20; i++) queriedSet.insert(insertedVec[i]);   // known hits
    while (queriedSet.size() < 60) {
        queriedSet.insert(epochDist(rng));  // likely (not guaranteed) misses
    }

    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_dbname("TESTDB");
    for (unsigned long long ts : queriedSet) queryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP queryRep;
    store.BatchQuery(&queryReq, &queryRep);

    std::set<unsigned long long> expectedHits;
    for (unsigned long long ts : queriedSet) {
        if (insertedTimestamps.count(ts)) expectedHits.insert(ts);
    }

    ASSERT_EQ(queryRep.queryresponse_size(), (int)expectedHits.size());
    for (int i = 0; i < queryRep.queryresponse_size(); i++) {
        const auto &entry = queryRep.queryresponse(i);
        EXPECT_EQ(expectedHits.count(entry.timestamp()), 1u) << "ts=" << entry.timestamp();
        ASSERT_EQ(entry.data().fielddata_size(), 1);
        EXPECT_EQ(entry.data().fielddata(0).int64data(), (int64_t)entry.timestamp());
    }
}

TEST_F(XStoreTest, BatchInsertToNonexistentDBReturnsError) {
    XStore store;
    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("NOPE");
    XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
    entry->set_timestamp(TS_2024_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);
    EXPECT_EQ(insertRep.insertstatus(), "[ERROR] [BATCH INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, BatchInsertEmptyPayloadOnExistingDBSucceedsWithZeroInserted) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");  // no batchinsert_data entries at all

    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);
    EXPECT_EQ(insertRep.insertstatus(), "[INFO] [BATCH INSERT] [DB: TESTDB] - Inserted 0 elements successfully");
}

TEST_F(XStoreTest, BatchDeleteThenBatchQueryDeletedEntriesOmitted) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Spread across two distinct years; only one entry (in the 2009 file) gets deleted.
    XStore_Proto::BatchInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) {
        XStore_Proto::BatchInsert_DATA *entry = insertReq.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FBool(true)});
    }
    XStore_Proto::BatchInsert_REP insertRep;
    store.BatchInsert(&insertReq, &insertRep);

    XStore_Proto::BatchDelete_REQ deleteReq;
    deleteReq.set_dbname("TESTDB");
    deleteReq.add_timestamp(TS_2009_MID + 10);
    XStore_Proto::BatchDelete_REP deleteRep = store.BatchDelete(&deleteReq);
    EXPECT_NE(deleteRep.deletestatus().find("Deleted 1 items"), std::string::npos);

    XStore_Proto::BatchQuery_REQ queryReq;
    queryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) queryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP queryRep;
    store.BatchQuery(&queryReq, &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 2);
    std::set<unsigned long long> returned;
    for (int i = 0; i < queryRep.queryresponse_size(); i++) returned.insert(queryRep.queryresponse(i).timestamp());
    EXPECT_EQ(returned, std::set<unsigned long long>({TS_2009_MID, TS_2031_MID}));
}

// ---------------------------------------------------------------------
// RangeInsert / RangeQuery / RangeDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RangeInsertThenRangeQueryContiguousRangeAllPresent) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (int i = 0; i < 10; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insertReq.add_rangeinsert_data();
        entry->set_timestamp(TS_2031_MID + i);
        *entry->mutable_data() = MakeRow({FInt64(i)});
    }
    XStore_Proto::RangeInsert_REP insertRep;
    store.RangeInsert(&insertReq, &insertRep);
    EXPECT_NE(insertRep.insertstatus().find("Inserted 10 elements"), std::string::npos);

    XStore_Proto::RangeQuery_REP queryRep;
    store.RangeQuery(TS_2031_MID, TS_2031_MID + 9, "TESTDB", &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 10);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(queryRep.queryresponse(i).timestamp(), TS_2031_MID + i);
        ASSERT_EQ(queryRep.queryresponse(i).data().fielddata_size(), 1);
        EXPECT_EQ(queryRep.queryresponse(i).data().fielddata(0).int64data(), i);
    }
}

TEST_F(XStoreTest, RangeQueryUninsertedTimestampsReturnEmptyEntriesNotOmitted) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Sparse insert: only the two endpoints of a 5-wide window. Positions
    // +1, +2, +3 are holes (unallocated -- never written).
    XStore_Proto::RangeInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *first = insertReq.add_rangeinsert_data();
    first->set_timestamp(TS_2009_MID);
    *first->mutable_data() = MakeRow({FString("start")});
    XStore_Proto::RangeInsert_DATA *last = insertReq.add_rangeinsert_data();
    last->set_timestamp(TS_2009_MID + 4);
    *last->mutable_data() = MakeRow({FString("end")});

    XStore_Proto::RangeInsert_REP insertRep;
    store.RangeInsert(&insertReq, &insertRep);

    XStore_Proto::RangeQuery_REP queryRep;
    store.RangeQuery(TS_2009_MID, TS_2009_MID + 4, "TESTDB", &queryRep);

    // Unlike BatchQuery, RangeQuery does NOT omit holes -- every timestamp in
    // range gets an entry, with holes coming back as zero-field RowData.
    ASSERT_EQ(queryRep.queryresponse_size(), 5);
    EXPECT_EQ(queryRep.queryresponse(0).data().fielddata_size(), 1);
    EXPECT_EQ(queryRep.queryresponse(0).data().fielddata(0).stringdata(), "start");
    for (int i = 1; i <= 3; i++) {
        EXPECT_EQ(queryRep.queryresponse(i).timestamp(), TS_2009_MID + i);
        EXPECT_EQ(queryRep.queryresponse(i).data().fielddata_size(), 0) << "hole at offset " << i;
    }
    EXPECT_EQ(queryRep.queryresponse(4).data().fielddata_size(), 1);
    EXPECT_EQ(queryRep.queryresponse(4).data().fielddata(0).stringdata(), "end");
}

TEST_F(XStoreTest, RangeQuerySpanningTwoYearBoundary) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &insertRep);
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &insertRep);

    XStore_Proto::RangeQuery_REP queryRep;
    store.RangeQuery(TS_2016_LAST, TS_2017_FIRST, "TESTDB", &queryRep);

    ASSERT_EQ(queryRep.queryresponse_size(), 2);
    EXPECT_EQ(queryRep.queryresponse(0).timestamp(), TS_2016_LAST);
    EXPECT_EQ(queryRep.queryresponse(0).data().fielddata(0).stringdata(), "last-of-2016");
    EXPECT_EQ(queryRep.queryresponse(1).timestamp(), TS_2017_FIRST);
    EXPECT_EQ(queryRep.queryresponse(1).data().fielddata(0).stringdata(), "first-of-2017");
}

TEST_F(XStoreTest, RangeInsertToNonexistentDBReturnsError) {
    XStore store;
    XStore_Proto::RangeInsert_REQ insertReq;
    insertReq.set_dbname("NOPE");
    XStore_Proto::RangeInsert_DATA *entry = insertReq.add_rangeinsert_data();
    entry->set_timestamp(TS_2003_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::RangeInsert_REP insertRep;
    store.RangeInsert(&insertReq, &insertRep);
    EXPECT_EQ(insertRep.insertstatus(), "[ERROR] [RANGE INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, RangeDeleteSpanningTwoYearBoundaryDeletesBothEndpoints) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &insertRep);
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &insertRep);

    // Same shape as RangeQuery's year-boundary bug: a range whose end lands
    // exactly on the first second of a year used to leave that last point
    // (endEpochT itself) undeleted.
    XStore_Proto::RangeDelete_REP deleteRep = store.RangeDelete(TS_2016_LAST, TS_2017_FIRST, "TESTDB");
    EXPECT_NE(deleteRep.deletestatus().find("Deleted 2 items"), std::string::npos) << deleteRep.deletestatus();

    XStore_Proto::UnaryQuery_REP rep2016, rep2017;
    store.UnaryQuery(TS_2016_LAST, "TESTDB", &rep2016);
    store.UnaryQuery(TS_2017_FIRST, "TESTDB", &rep2017);
    EXPECT_EQ(rep2016.data().fielddata_size(), 0);
    EXPECT_EQ(rep2017.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// Aggregates -- Min/Max/Sum/Avg, happy path over a contiguous, fully
// populated numeric column
// ---------------------------------------------------------------------

TEST_F(XStoreTest, AggregatesOverContiguousNumericColumn) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    const std::vector<double> values = {10.0, 3.0, 7.0, 20.0, 1.0};
    XStore_Proto::RangeInsert_REQ insertReq;
    insertReq.set_dbname("TESTDB");
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::RangeInsert_DATA *entry = insertReq.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = MakeRow({FDouble(values[i])});
    }
    XStore_Proto::RangeInsert_REP insertRep;
    store.RangeInsert(&insertReq, &insertRep);

    const unsigned long long start = TS_2024_MID;
    const unsigned long long end = TS_2024_MID + values.size() - 1;

    XStore_Proto::Min_REP minRep;
    store.MinAggregateV3(start, end, "TESTDB", 0, &minRep);
    EXPECT_EQ(minRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(minRep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REP maxRep;
    store.MaxAggregateV2(start, end, "TESTDB", 0, &maxRep);
    EXPECT_DOUBLE_EQ(maxRep.opresult().doubledata(), 20.0);

    XStore_Proto::Sum_REP sumRep;
    store.SumAggregateV2(start, end, "TESTDB", 0, &sumRep);
    EXPECT_DOUBLE_EQ(sumRep.opresult(), 41.0);

    XStore_Proto::Avg_REP avgRep;
    store.AvgAggregateV2(start, end, "TESTDB", 0, &avgRep);
    EXPECT_DOUBLE_EQ(avgRep.opresult(), 8.2);
}

TEST_F(XStoreTest, MinAggregateColumnIndexOutOfRangeReturnsNoValidData) {
    XStore store;
    store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insertRep;
    store.UnaryInsert(TS_2031_MID, MakeRow({FDouble(1.0)}), "TESTDB", &insertRep);  // only column 0 exists

    XStore_Proto::Min_REP minRep;
    store.MinAggregateV3(TS_2031_MID, TS_2031_MID, "TESTDB", 5, &minRep);
    EXPECT_EQ(minRep.opstatus(), "ERROR: No valid data found");
}

// ---------------------------------------------------------------------
// Concurrency -- exercises FileRangeLock, the in-process byte-range lock
// that replaced fcntl() record locks. fcntl() locks are scoped to the
// process (not the thread or fd), so two worker threads in one process
// never actually blocked each other; FileRangeLock now provides genuine
// mutual exclusion between them, matching each ZMQ worker owning its own
// XStore instance but sharing the same on-disk files.
// ---------------------------------------------------------------------

TEST_F(XStoreTest, ConcurrentWritesToSameRegionAreSerializedAndDoNotCorruptData) {
    XStore seedStore;
    seedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seedRep;
    seedStore.UnaryInsert(TS_2024_MID, MakeRow({FInt64(-1)}), "TESTDB", &seedRep);  // pre-allocate the year file

    constexpr int kIterations = 500;
    auto writer = [&](int threadId) {
        XStore store;  // each thread gets its own XStore instance, matching ZMQ_Server's one-instance-per-worker design
        for (int i = 0; i < kIterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(threadId * 1'000'000LL + i)}), "TESTDB", &rep);
        }
    };

    std::thread t0(writer, 0);
    std::thread t1(writer, 1);
    t0.join();
    t1.join();

    XStore_Proto::UnaryQuery_REP queryRep;
    seedStore.UnaryQuery(TS_2024_MID, "TESTDB", &queryRep);

    // Every write locked the exact same [offset, offset+rowLength) range, so
    // whichever thread wrote last, the row must be a single, cleanly-parsed
    // value from one of the two threads -- never a torn mix of both.
    ASSERT_EQ(queryRep.data().fielddata_size(), 1) << "row must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t finalValue = queryRep.data().fielddata(0).int64data();
    int finalThread = finalValue / 1'000'000LL;
    int finalIter = finalValue % 1'000'000LL;
    EXPECT_TRUE(finalThread == 0 || finalThread == 1) << "finalValue=" << finalValue;
    EXPECT_GE(finalIter, 0);
    EXPECT_LT(finalIter, kIterations);
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentRegionsOfSameFileDoNotInterfere) {
    XStore seedStore;
    seedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seedRep;
    seedStore.UnaryInsert(TS_2009_MID, MakeRow({FInt64(-1)}), "TESTDB", &seedRep);  // pre-allocate 2009.XSTORE once

    constexpr int kCount = 100;
    auto writer = [&](unsigned long long base) {
        XStore store;
        for (int i = 0; i < kCount; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(base + i, MakeRow({FInt64((int64_t)(base + i))}), "TESTDB", &rep);
        }
    };

    // Both ranges live in the SAME underlying 2009.XSTORE file but never overlap.
    const unsigned long long base0 = TS_2009_MID + 10;       // offsets [10, 110)
    const unsigned long long base1 = TS_2009_MID + 100000;   // offsets [100000, 100100)

    std::thread t0(writer, base0);
    std::thread t1(writer, base1);
    t0.join();
    t1.join();

    for (int i = 0; i < kCount; i++) {
        XStore_Proto::UnaryQuery_REP rep0, rep1;
        seedStore.UnaryQuery(base0 + i, "TESTDB", &rep0);
        seedStore.UnaryQuery(base1 + i, "TESTDB", &rep1);
        ASSERT_EQ(rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(rep0.data().fielddata(0).int64data(), (int64_t)(base0 + i));
        EXPECT_EQ(rep1.data().fielddata(0).int64data(), (int64_t)(base1 + i));
    }
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentYearFilesDoNotInterfere) {
    XStore seedStore;
    seedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seedRep;
    seedStore.UnaryInsert(TS_1998_MID, MakeRow({FInt64(-1)}), "TESTDB", &seedRep);  // pre-allocate 1998.XSTORE
    seedStore.UnaryInsert(TS_2031_MID, MakeRow({FInt64(-1)}), "TESTDB", &seedRep);  // pre-allocate 2031.XSTORE

    constexpr int kCount = 100;
    auto writer = [&](unsigned long long base) {
        XStore store;
        for (int i = 0; i < kCount; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(base + i, MakeRow({FInt64((int64_t)(base + i))}), "TESTDB", &rep);
        }
    };

    std::thread t0(writer, TS_1998_MID);
    std::thread t1(writer, TS_2031_MID);
    t0.join();
    t1.join();

    for (int i = 0; i < kCount; i++) {
        XStore_Proto::UnaryQuery_REP rep0, rep1;
        seedStore.UnaryQuery(TS_1998_MID + i, "TESTDB", &rep0);
        seedStore.UnaryQuery(TS_2031_MID + i, "TESTDB", &rep1);
        ASSERT_EQ(rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(rep0.data().fielddata(0).int64data(), (int64_t)(TS_1998_MID + i));
        EXPECT_EQ(rep1.data().fielddata(0).int64data(), (int64_t)(TS_2031_MID + i));
    }

    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "1998.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->testRoot / "DataStore" / "TESTDB" / "2031.XSTORE"));
}
