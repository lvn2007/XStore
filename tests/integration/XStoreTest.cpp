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
  std::unique_ptr<ScratchDir> Scratch;

  void SetUp() override {
      Scratch = std::make_unique<ScratchDir>("xstore_test");
  }

  void TearDown() override {
      Scratch.reset();
  }
};

// ---------------------------------------------------------------------
// InfoQuery
// ---------------------------------------------------------------------

TEST_F(XStoreTest, InfoQueryOnEmptyServerReturnsNoDatabases) {
    XStore Store;
    XStore_Proto::SysInfo_REP Info = Store.InfoQuery("127.0.0.1", 4);

    EXPECT_EQ(Info.serveraddr(), "127.0.0.1");
    EXPECT_EQ(Info.threadcount(), 4);
    EXPECT_EQ(Info.dbinfo_size(), 0);
}

TEST_F(XStoreTest, InfoQueryReflectsCreatedDatabase) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::SysInfo_REP Info = Store.InfoQuery("127.0.0.1", 2);

    ASSERT_EQ(Info.dbinfo_size(), 1);
    EXPECT_EQ(Info.dbinfo(0).dbname(), "TESTDB");
    EXPECT_EQ(Info.dbinfo(0).dbresolution(), XStore_Proto::SECOND);
}

// ---------------------------------------------------------------------
// CreateDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, CreateDBRejectsRowLengthBelowFour) {
    XStore Store;
    XStore_Proto::CreateDB_REP Rep = Store.CreateDB("TESTDB", XStore_Proto::SECOND, 3);

    EXPECT_EQ(Rep.createdbstatus(), "[ERROR] [CREATE DB] [DB: TESTDB] - Row length must be >= 4");
}

TEST_F(XStoreTest, CreateDBSucceedsAndWritesConfigFile) {
    XStore Store;
    XStore_Proto::CreateDB_REP Rep = Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(Rep.createdbstatus(), "[INFO] [CREATE DB] [DB: TESTDB] - Created DB");
    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

TEST_F(XStoreTest, CreateDBRejectsDuplicateName) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::CreateDB_REP Rep = Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(Rep.createdbstatus(), "[ERROR] [CREATE DB] [DB: TESTDB] - Already existed");
}

// ---------------------------------------------------------------------
// RemoveDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::RemoveDB_REP Rep = Store.RemoveDB("TESTDB");

    EXPECT_EQ(Rep.removestatus(), "[ERROR] [REMOVE DB] [DB: TESTDB] - DB does not exists");
}

TEST_F(XStoreTest, RemoveDBDeletesDirectoryAndDropsFromInfoQuery) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RemoveDB_REP Rep = Store.RemoveDB("TESTDB");
    EXPECT_EQ(Rep.removestatus(), "[INFO] [REMOVE DB] [DB: TESTDB] - Removed DB with name: TESTDB");
    EXPECT_FALSE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB"));

    XStore_Proto::SysInfo_REP Info = Store.InfoQuery("127.0.0.1", 1);
    EXPECT_EQ(Info.dbinfo_size(), 0);
}

// ---------------------------------------------------------------------
// RemoveDBFiles
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBFilesRemovesDataFileButProtectsConfig) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &InsertRep);

    XStore_Proto::RemoveDBFiles_REQ Req;
    Req.add_filename("2016.XSTORE");
    Req.add_filename("TESTDB.CFG");

    XStore_Proto::RemoveDBFiles_REP Rep = Store.RemoveDBFiles(&Req, "TESTDB");

    ASSERT_EQ(Rep.removestatus_size(), 2);
    EXPECT_EQ(Rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: TESTDB] - Removed 2016.XSTORE");
    EXPECT_EQ(Rep.removestatus(1), "[WARNING] [REMOVE DB FILES] [DB: TESTDB] - TESTDB.CFG is prohibited from remove");

    EXPECT_FALSE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

// ---------------------------------------------------------------------
// UnaryInsert / UnaryQuery / UnaryDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, UnaryInsertOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::UnaryInsert_REP Rep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &Rep);

    EXPECT_EQ(Rep.insertstatus(), "[ERROR] [UNARY INSERT] [DB: TESTDB] - DB does not exist");
}

TEST_F(XStoreTest, UnaryInsertExactTimestampHasInsertedData) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(3.14), FString("abc"), FBool(true)}), "TESTDB", &InsertRep);
    EXPECT_EQ(InsertRep.insertstatus(), "[INFO] [UNARY INSERT] [DB: TESTDB] - Inserted 1482271200 to 2016.XSTORE");

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);

    EXPECT_EQ(QueryRep.timestamp(), TS_2016_MID);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(QueryRep.data().fielddata(0).doubledata(), 3.14);
    EXPECT_EQ(QueryRep.data().fielddata(1).stringdata(), "abc");
    EXPECT_EQ(QueryRep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreTest, UnaryInsertAllProtoDataTypesRoundTrip) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 256);

    // Exercises every branch of FData's oneof (see ProtoFile/XStore.proto),
    // not just the double/string/bool trio used elsewhere in this file.
    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({
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
    }), "TESTDB", &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.data().fielddata_size(), 15);
    const auto &d = QueryRep.data();
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
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2009_MID, MakeRow({FDouble(1.0)}), "TESTDB", &InsertRep);

    // Same year file exists, but nothing was ever written at these offsets --
    // this must come back as a normal (non-error) response with zero fields,
    // not as an error, since the file itself exists.
    for (unsigned long long Neighbor : {TS_2009_MID - 5, TS_2009_MID + 5, TS_2009_MID + 3600}) {
        XStore_Proto::UnaryQuery_REP QueryRep;
        Store.UnaryQuery(Neighbor, "TESTDB", &QueryRep);
        EXPECT_EQ(QueryRep.timestamp(), Neighbor);
        EXPECT_EQ(QueryRep.data().fielddata_size(), 0) << "neighbor=" << Neighbor;
    }

    // Sanity: the originally-inserted timestamp is unaffected by the reads above.
    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2009_MID, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(QueryRep.data().fielddata(0).doubledata(), 1.0);
}

TEST_F(XStoreTest, UnaryInsertOverwriteSameTimestampKeepsLatestValue) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2003_MID, MakeRow({FString("first")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2003_MID, MakeRow({FString("second")}), "TESTDB", &InsertRep);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2003_MID, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).stringdata(), "second");
}

TEST_F(XStoreTest, UnaryQueryWithDbButNoYearFileReturnsError) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);  // DB exists, but nothing was ever inserted for TS_2016_MID's year

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);

    EXPECT_EQ(QueryRep.querystatus(), "[ERROR] [UNARY QUERY] [DB: TESTDB] - DB does not exist");
}

// Unlike UnaryInsert/BatchQuery/RangeQuery (which all check the DB directory
// or .CFG path exists before opening it), UnaryQuery goes straight to
// fopen(cfgPath) when the dbName isn't already cached in cfgMap. On a fresh
// XStore instance querying a DB that was never created, that fopen() returns
// NULL and the subsequent fileno(NULL) crashes instead of reporting an error.
TEST_F(XStoreTest, UnaryQueryOnNeverCreatedDBReturnsErrorInsteadOfCrashing) {
    XStore Store;  // fresh instance: cfgMap starts empty, so this can't be masked by a warm cache
    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2003_MID, "NEVER_CREATED_DB", &QueryRep);
    EXPECT_NE(QueryRep.querystatus().find("[ERROR]"), std::string::npos);
}

TEST_F(XStoreTest, UnaryInsertDbNameIsCaseInsensitive) {
    XStore Store;
    Store.CreateDB("testdb", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_MID, MakeRow({FString("mixed-case")}), "TestDb", &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_1998_MID, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).stringdata(), "mixed-case");
}

TEST_F(XStoreTest, UnaryInsertDifferentYearsWriteToSeparateFiles) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("year-2016")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2031_MID, MakeRow({FString("year-2031")}), "TESTDB", &InsertRep);

    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "2031.XSTORE"));

    XStore_Proto::UnaryQuery_REP Rep2016, Rep2031;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &Rep2016);
    Store.UnaryQuery(TS_2031_MID, "TESTDB", &Rep2031);

    ASSERT_EQ(Rep2016.data().fielddata_size(), 1);
    ASSERT_EQ(Rep2031.data().fielddata_size(), 1);
    EXPECT_EQ(Rep2016.data().fielddata(0).stringdata(), "year-2016");
    EXPECT_EQ(Rep2031.data().fielddata(0).stringdata(), "year-2031");
}

TEST_F(XStoreTest, UnaryDeletePunchesHoleThenUnaryQueryReturnsEmptyData) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2024_MID, MakeRow({FString("to-be-deleted")}), "TESTDB", &InsertRep);

    XStore_Proto::UnaryDelete_REP DeleteRep = Store.UnaryDelete(TS_2024_MID, "TESTDB");
    EXPECT_NE(DeleteRep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2024_MID, "TESTDB", &QueryRep);
    EXPECT_EQ(QueryRep.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, UnaryDeleteOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::UnaryDelete_REP DeleteRep = Store.UnaryDelete(TS_2016_MID, "NOPE");
    EXPECT_EQ(DeleteRep.deletestatus(), "[ERROR] [UNARY DELETE] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, UnaryDeleteOnMissingYearFileReturnsError) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    // DB exists but nothing was ever inserted for this year -> no 2016.XSTORE file.
    XStore_Proto::UnaryDelete_REP DeleteRep = Store.UnaryDelete(TS_2016_MID, "TESTDB");
    EXPECT_EQ(DeleteRep.deletestatus(), "[ERROR] [UNARY DELETE] [DB: TESTDB] - 2016.XSTORE does not exist");
}

// ---------------------------------------------------------------------
// BatchInsert / BatchQuery / BatchDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, BatchInsertThenBatchQueryExactTimestampsRoundTrip) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Spread across five different years (rather than one clustered block) so
    // this also exercises BatchInsert/BatchQuery's per-year file bucketing.
    std::map<unsigned long long, std::string> Expected = {
        {TS_2031_MID, "e"},
        {TS_1998_MID, "a"},
        {TS_2016_MID, "d"},
        {TS_2009_MID, "c"},
        {TS_2003_MID, "b"},
    };

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (auto &[ts, val] : Expected) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(ts);
        *Entry->mutable_data() = MakeRow({FString(val)});
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("Inserted 5 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    // Deliberately shuffled/unsorted input -- BatchQuery must sort internally.
    for (unsigned long long ts : {TS_2016_MID, TS_1998_MID, TS_2031_MID, TS_2003_MID, TS_2009_MID}) {
        QueryReq.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 5);
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        const auto &Entry = QueryRep.queryresponse(i);
        ASSERT_TRUE(Expected.count(Entry.timestamp())) << "unexpected timestamp " << Entry.timestamp();
        ASSERT_EQ(Entry.data().fielddata_size(), 1);
        EXPECT_EQ(Entry.data().fielddata(0).stringdata(), Expected[Entry.timestamp()]);
    }
}

TEST_F(XStoreTest, BatchQueryHolesAreOmittedFromResults) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Two unrelated years, each with a small gap between its own points, so
    // hole-detection is verified without clustering all data into one year.
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 15}) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(ts);
        *Entry->mutable_data() = MakeRow({FUInt32(1)});
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    // Query the 5 inserted timestamps plus holes interleaved in both years --
    // holes must be silently omitted from queryresponse, not returned as empty rows.
    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 5, TS_2009_MID + 10, TS_2009_MID + 15, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 7, TS_2031_MID + 15}) {
        QueryReq.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 5);
    std::set<unsigned long long> Returned;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        Returned.insert(QueryRep.queryresponse(i).timestamp());
    }
    EXPECT_EQ(Returned, std::set<unsigned long long>(
        {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20, TS_2031_MID, TS_2031_MID + 15}));
}

TEST_F(XStoreTest, BatchInsertMixedColumnCountsPerTimestamp) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    const unsigned long long Ts1 = TS_1998_MID;              // 1 column
    const unsigned long long Ts2 = TS_2003_MID;               // 2 columns
    const unsigned long long Ts3 = TS_2009_MID;               // 3 columns
    const unsigned long long Ts4 = TS_2016_MID;               // 1 column
    const unsigned long long TsHole = TS_2016_MID + 100000;   // never inserted, same year as ts4

    // Deliberately spread across several different oneof branches (not just
    // double/string/bool) so column-count mixing is also type-diverse.
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    auto AddEntry = [&](unsigned long long ts, const std::vector<XStore_Proto::FData> &vals) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(ts);
        *Entry->mutable_data() = MakeRow(vals);
    };
    AddEntry(Ts1, {FUInt64(42)});
    AddEntry(Ts2, {FFixed32(7), FBytes("payload-two")});
    AddEntry(Ts3, {FSInt64(-123456789), FSFixed32(-42), FBool(true)});
    AddEntry(Ts4, {FInt64(4)});

    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {Ts1, Ts2, Ts3, Ts4, TsHole}) QueryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 4);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> ByTs;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        ByTs[QueryRep.queryresponse(i).timestamp()] = QueryRep.queryresponse(i);
    }

    ASSERT_EQ(ByTs.count(TsHole), 0u);
    ASSERT_EQ(ByTs[Ts1].data().fielddata_size(), 1);
    EXPECT_EQ(ByTs[Ts1].data().fielddata(0).uint64data(), 42u);

    ASSERT_EQ(ByTs[Ts2].data().fielddata_size(), 2);
    EXPECT_EQ(ByTs[Ts2].data().fielddata(0).fixed32data(), 7u);
    EXPECT_EQ(ByTs[Ts2].data().fielddata(1).bytesdata(), "payload-two");

    ASSERT_EQ(ByTs[Ts3].data().fielddata_size(), 3);
    EXPECT_EQ(ByTs[Ts3].data().fielddata(0).sint64data(), -123456789);
    EXPECT_EQ(ByTs[Ts3].data().fielddata(1).sfixed32data(), -42);
    EXPECT_EQ(ByTs[Ts3].data().fielddata(2).booldata(), true);

    ASSERT_EQ(ByTs[Ts4].data().fielddata_size(), 1);
    EXPECT_EQ(ByTs[Ts4].data().fielddata(0).int64data(), 4);
}

TEST_F(XStoreTest, BatchInsertRandomTimestampsThenRandomBatchQuerySubset) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    std::mt19937 Rng(12345);
    std::uniform_int_distribution<unsigned long long> EpochDist(RANDOM_RANGE_LOW, RANDOM_RANGE_HIGH);

    // Generate a random, deduplicated set of timestamps spread across ~70
    // years (not clustered in one year) and insert each one carrying its own
    // timestamp value as the payload so round-trip correctness can be
    // checked without a side table.
    std::set<unsigned long long> InsertedTimestamps;
    while (InsertedTimestamps.size() < 40) {
        InsertedTimestamps.insert(EpochDist(Rng));
    }

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (unsigned long long ts : InsertedTimestamps) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(ts);
        *Entry->mutable_data() = MakeRow({FInt64((int64_t)ts)});
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    // Sanity check: confirm the generated data really is dispersed across
    // several distinct year-files rather than accidentally clustering.
    std::set<int> DistinctYears;
    for (unsigned long long ts : InsertedTimestamps) {
        std::time_t t = static_cast<std::time_t>(ts);
        struct tm TmStruct;
        gmtime_r(&t, &TmStruct);
        DistinctYears.insert(TmStruct.tm_year + 1900);
    }
    EXPECT_GE(DistinctYears.size(), 10u) << "expected timestamps to span many distinct years";

    // Random query set: a mix of timestamps that were and weren't inserted.
    std::vector<unsigned long long> InsertedVec(InsertedTimestamps.begin(), InsertedTimestamps.end());
    std::shuffle(InsertedVec.begin(), InsertedVec.end(), Rng);

    std::set<unsigned long long> QueriedSet;
    for (int i = 0; i < 20; i++) QueriedSet.insert(InsertedVec[i]);   // known hits
    while (QueriedSet.size() < 60) {
        QueriedSet.insert(EpochDist(Rng));  // likely (not guaranteed) misses
    }

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (unsigned long long ts : QueriedSet) QueryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    std::set<unsigned long long> ExpectedHits;
    for (unsigned long long ts : QueriedSet) {
        if (InsertedTimestamps.count(ts)) ExpectedHits.insert(ts);
    }

    ASSERT_EQ(QueryRep.queryresponse_size(), (int)ExpectedHits.size());
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        const auto &Entry = QueryRep.queryresponse(i);
        EXPECT_EQ(ExpectedHits.count(Entry.timestamp()), 1u) << "ts=" << Entry.timestamp();
        ASSERT_EQ(Entry.data().fielddata_size(), 1);
        EXPECT_EQ(Entry.data().fielddata(0).int64data(), (int64_t)Entry.timestamp());
    }
}

TEST_F(XStoreTest, BatchInsertToNonexistentDBReturnsError) {
    XStore Store;
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("NOPE");
    XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
    Entry->set_timestamp(TS_2024_MID);
    *Entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);
    EXPECT_EQ(InsertRep.insertstatus(), "[ERROR] [BATCH INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, BatchInsertEmptyPayloadOnExistingDBSucceedsWithZeroInserted) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");  // no batchinsert_data entries at all

    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);
    EXPECT_EQ(InsertRep.insertstatus(), "[INFO] [BATCH INSERT] [DB: TESTDB] - Inserted 0 elements successfully");
}

TEST_F(XStoreTest, BatchDeleteThenBatchQueryDeletedEntriesOmitted) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Spread across two distinct years; only one entry (in the 2009 file) gets deleted.
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) {
        XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
        Entry->set_timestamp(ts);
        *Entry->mutable_data() = MakeRow({FBool(true)});
    }
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::BatchDelete_REQ DeleteReq;
    DeleteReq.set_dbname("TESTDB");
    DeleteReq.add_timestamp(TS_2009_MID + 10);
    XStore_Proto::BatchDelete_REP DeleteRep = Store.BatchDelete(&DeleteReq);
    EXPECT_NE(DeleteRep.deletestatus().find("Deleted 1 items"), std::string::npos);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) QueryReq.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 2);
    std::set<unsigned long long> Returned;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) Returned.insert(QueryRep.queryresponse(i).timestamp());
    EXPECT_EQ(Returned, std::set<unsigned long long>({TS_2009_MID, TS_2031_MID}));
}

// ---------------------------------------------------------------------
// RangeInsert / RangeQuery / RangeDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RangeInsertThenRangeQueryContiguousRangeAllPresent) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (int i = 0; i < 10; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2031_MID + i);
        *Entry->mutable_data() = MakeRow({FInt64(i)});
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("Inserted 10 elements"), std::string::npos);

    XStore_Proto::RangeQuery_REP QueryRep;
    Store.RangeQuery(TS_2031_MID, TS_2031_MID + 9, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 10);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(QueryRep.queryresponse(i).timestamp(), TS_2031_MID + i);
        ASSERT_EQ(QueryRep.queryresponse(i).data().fielddata_size(), 1);
        EXPECT_EQ(QueryRep.queryresponse(i).data().fielddata(0).int64data(), i);
    }
}

TEST_F(XStoreTest, RangeQueryUninsertedTimestampsReturnEmptyEntriesNotOmitted) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Sparse insert: only the two endpoints of a 5-wide window. Positions
    // +1, +2, +3 are holes (unallocated -- never written).
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *First = InsertReq.add_rangeinsert_data();
    First->set_timestamp(TS_2009_MID);
    *First->mutable_data() = MakeRow({FString("start")});
    XStore_Proto::RangeInsert_DATA *Last = InsertReq.add_rangeinsert_data();
    Last->set_timestamp(TS_2009_MID + 4);
    *Last->mutable_data() = MakeRow({FString("end")});

    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::RangeQuery_REP QueryRep;
    Store.RangeQuery(TS_2009_MID, TS_2009_MID + 4, "TESTDB", &QueryRep);

    // Unlike BatchQuery, RangeQuery does NOT omit holes -- every timestamp in
    // range gets an entry, with holes coming back as zero-field RowData.
    ASSERT_EQ(QueryRep.queryresponse_size(), 5);
    EXPECT_EQ(QueryRep.queryresponse(0).data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.queryresponse(0).data().fielddata(0).stringdata(), "start");
    for (int i = 1; i <= 3; i++) {
        EXPECT_EQ(QueryRep.queryresponse(i).timestamp(), TS_2009_MID + i);
        EXPECT_EQ(QueryRep.queryresponse(i).data().fielddata_size(), 0) << "hole at offset " << i;
    }
    EXPECT_EQ(QueryRep.queryresponse(4).data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.queryresponse(4).data().fielddata(0).stringdata(), "end");
}

TEST_F(XStoreTest, RangeQuerySpanningTwoYearBoundary) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &InsertRep);

    XStore_Proto::RangeQuery_REP QueryRep;
    Store.RangeQuery(TS_2016_LAST, TS_2017_FIRST, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 2);
    EXPECT_EQ(QueryRep.queryresponse(0).timestamp(), TS_2016_LAST);
    EXPECT_EQ(QueryRep.queryresponse(0).data().fielddata(0).stringdata(), "last-of-2016");
    EXPECT_EQ(QueryRep.queryresponse(1).timestamp(), TS_2017_FIRST);
    EXPECT_EQ(QueryRep.queryresponse(1).data().fielddata(0).stringdata(), "first-of-2017");
}

TEST_F(XStoreTest, RangeInsertToNonexistentDBReturnsError) {
    XStore Store;
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("NOPE");
    XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
    Entry->set_timestamp(TS_2003_MID);
    *Entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);
    EXPECT_EQ(InsertRep.insertstatus(), "[ERROR] [RANGE INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, RangeDeleteSpanningTwoYearBoundaryDeletesBothEndpoints) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &InsertRep);

    // Same shape as RangeQuery's year-boundary bug: a range whose end lands
    // exactly on the first second of a year used to leave that last point
    // (endEpochT itself) undeleted.
    XStore_Proto::RangeDelete_REP DeleteRep = Store.RangeDelete(TS_2016_LAST, TS_2017_FIRST, "TESTDB");
    EXPECT_NE(DeleteRep.deletestatus().find("Deleted 2 items"), std::string::npos) << DeleteRep.deletestatus();

    XStore_Proto::UnaryQuery_REP Rep2016, Rep2017;
    Store.UnaryQuery(TS_2016_LAST, "TESTDB", &Rep2016);
    Store.UnaryQuery(TS_2017_FIRST, "TESTDB", &Rep2017);
    EXPECT_EQ(Rep2016.data().fielddata_size(), 0);
    EXPECT_EQ(Rep2017.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// Aggregates -- Min/Max/Sum/Avg, happy path over a contiguous, fully
// populated numeric column
// ---------------------------------------------------------------------

TEST_F(XStoreTest, AggregatesOverContiguousNumericColumn) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    const std::vector<double> Values = {10.0, 3.0, 7.0, 20.0, 1.0};
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (size_t i = 0; i < Values.size(); i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2024_MID + i);
        *Entry->mutable_data() = MakeRow({FDouble(Values[i])});
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    const unsigned long long Start = TS_2024_MID;
    const unsigned long long End = TS_2024_MID + Values.size() - 1;

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(Start, End, "TESTDB", 0, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(Start, End, "TESTDB", 0, &MaxRep);
    EXPECT_DOUBLE_EQ(MaxRep.opresult().doubledata(), 20.0);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(Start, End, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 41.0);

    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(Start, End, "TESTDB", 0, &AvgRep);
    EXPECT_DOUBLE_EQ(AvgRep.opresult(), 8.2);
}

TEST_F(XStoreTest, MinAggregateColumnIndexOutOfRangeReturnsNoValidData) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2031_MID, MakeRow({FDouble(1.0)}), "TESTDB", &InsertRep);  // only column 0 exists

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2031_MID, TS_2031_MID, "TESTDB", 5, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "ERROR: No valid data found");
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
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_2024_MID, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate the year file

    constexpr int KIterations = 500;
    auto Writer = [&](int threadId) {
        XStore Store;  // each thread gets its own XStore instance, matching ZMQ_Server's one-instance-per-worker design
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            Store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(threadId * 1'000'000LL + i)}), "TESTDB", &Rep);
        }
    };

    std::thread T0(Writer, 0);
    std::thread T1(Writer, 1);
    T0.join();
    T1.join();

    XStore_Proto::UnaryQuery_REP QueryRep;
    SeedStore.UnaryQuery(TS_2024_MID, "TESTDB", &QueryRep);

    // Every write locked the exact same [offset, offset+rowLength) range, so
    // whichever thread wrote last, the row must be a single, cleanly-parsed
    // value from one of the two threads -- never a torn mix of both.
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1) << "row must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t FinalValue = QueryRep.data().fielddata(0).int64data();
    int FinalThread = FinalValue / 1'000'000LL;
    int FinalIter = FinalValue % 1'000'000LL;
    EXPECT_TRUE(FinalThread == 0 || FinalThread == 1) << "finalValue=" << FinalValue;
    EXPECT_GE(FinalIter, 0);
    EXPECT_LT(FinalIter, KIterations);
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentRegionsOfSameFileDoNotInterfere) {
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_2009_MID, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate 2009.XSTORE once

    constexpr int KCount = 100;
    auto Writer = [&](unsigned long long base) {
        XStore Store;
        for (int i = 0; i < KCount; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            Store.UnaryInsert(base + i, MakeRow({FInt64((int64_t)(base + i))}), "TESTDB", &Rep);
        }
    };

    // Both ranges live in the SAME underlying 2009.XSTORE file but never overlap.
    const unsigned long long Base0 = TS_2009_MID + 10;       // offsets [10, 110)
    const unsigned long long Base1 = TS_2009_MID + 100000;   // offsets [100000, 100100)

    std::thread T0(Writer, Base0);
    std::thread T1(Writer, Base1);
    T0.join();
    T1.join();

    for (int i = 0; i < KCount; i++) {
        XStore_Proto::UnaryQuery_REP Rep0, Rep1;
        SeedStore.UnaryQuery(Base0 + i, "TESTDB", &Rep0);
        SeedStore.UnaryQuery(Base1 + i, "TESTDB", &Rep1);
        ASSERT_EQ(Rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(Rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(Rep0.data().fielddata(0).int64data(), (int64_t)(Base0 + i));
        EXPECT_EQ(Rep1.data().fielddata(0).int64data(), (int64_t)(Base1 + i));
    }
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentYearFilesDoNotInterfere) {
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_1998_MID, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate 1998.XSTORE
    SeedStore.UnaryInsert(TS_2031_MID, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate 2031.XSTORE

    constexpr int KCount = 100;
    auto Writer = [&](unsigned long long base) {
        XStore Store;
        for (int i = 0; i < KCount; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            Store.UnaryInsert(base + i, MakeRow({FInt64((int64_t)(base + i))}), "TESTDB", &Rep);
        }
    };

    std::thread T0(Writer, TS_1998_MID);
    std::thread T1(Writer, TS_2031_MID);
    T0.join();
    T1.join();

    for (int i = 0; i < KCount; i++) {
        XStore_Proto::UnaryQuery_REP Rep0, Rep1;
        SeedStore.UnaryQuery(TS_1998_MID + i, "TESTDB", &Rep0);
        SeedStore.UnaryQuery(TS_2031_MID + i, "TESTDB", &Rep1);
        ASSERT_EQ(Rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(Rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(Rep0.data().fielddata(0).int64data(), (int64_t)(TS_1998_MID + i));
        EXPECT_EQ(Rep1.data().fielddata(0).int64data(), (int64_t)(TS_2031_MID + i));
    }

    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "1998.XSTORE"));
    EXPECT_TRUE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB" / "2031.XSTORE"));
}

// Regression tests for CfgMap: it used to be a plain (non-static) member, so
// each worker thread's own `XStore` instance (see ZMQ_Server::WorkerRoutine,
// which constructs one `XStore` per thread) held a private cache. A config
// cached via one thread's instance was never visible to any other thread's
// instance, and concurrent inserts into the map were also unsynchronized.
// CfgMap and its guarding CfgMutex are now static, shared by every instance.

TEST_F(XStoreTest, CfgCachedByOneXStoreInstanceIsVisibleToAnotherInstance) {
    // Constructed before TESTDB exists, so Reader's own InitDB() (which runs
    // in the constructor) cannot have cached TESTDB's config itself.
    XStore Reader;

    XStore Creator;
    Creator.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Make the on-disk .CFG diverge from what Creator just cached. If Reader
    // had to fall back to reading disk itself instead of hitting the shared
    // cache, it would pick up this wrong 4-byte row length instead of 128.
    XStore_Proto::ConfigStruct Divergent;
    Divergent.set_dbname("TESTDB");
    Divergent.set_timeresolution(XStore_Proto::SECOND);
    Divergent.set_rowlength(4);
    fs::path CfgPath = Scratch->TestRoot / "DataStore" / "TESTDB" / "TESTDB.CFG";
    FILE *CFile = std::fopen(CfgPath.string().c_str(), "wb+");
    Divergent.SerializeToFileDescriptor(fileno(CFile));
    std::fclose(CFile);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Reader.UnaryInsert(TS_2016_MID, MakeRow({FDouble(3.14)}), "TESTDB", &InsertRep);

    // Reader must have used the cached 128-byte row length -- not the 4-byte
    // one now sitting on disk -- proving the shared cache, not disk, won.
    ASSERT_TRUE(XStore::CfgMap.count("TESTDB"));
    EXPECT_EQ(XStore::CfgMap.at("TESTDB").rowlength(), 128);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Reader.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(QueryRep.data().fielddata(0).doubledata(), 3.14);
}

TEST_F(XStoreTest, ConcurrentXStoreInstancesShareCfgMapWithoutCorruption) {
    XStore Creator;
    Creator.CreateDB("TESTDB", XStore_Proto::SECOND, 64);

    // Force every worker below to race on a genuine cache miss instead of
    // trivially hitting what CreateDB already cached.
    XStore::CfgMap.clear();

    constexpr int NumThreads = 16;
    std::vector<std::thread> Threads;
    std::vector<XStore_Proto::UnaryQuery_REP> Reps(NumThreads);

    for (int i = 0; i < NumThreads; i++) {
        Threads.emplace_back([i, &Reps]() {
            XStore Worker;  // one instance per thread, matching ZMQ_Server's design
            Worker.UnaryQuery(TS_2003_MID, "TESTDB", &Reps[i]);
        });
    }
    for (auto &T : Threads) T.join();

    // All 16 threads race to populate the same cache entry (both via each
    // constructor's InitDB() and via GetOrLoadCfg's miss path). An unguarded
    // std::map under that many concurrent inserts would risk a corrupted map
    // or a crash; they must instead converge on one consistent entry.
    ASSERT_EQ(XStore::CfgMap.size(), 1u);
    EXPECT_EQ(XStore::CfgMap.at("TESTDB").rowlength(), 64);
}
