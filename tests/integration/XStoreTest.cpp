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
#include <atomic>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <map>
#include <random>
#include <set>
#include <sys/stat.h>
#include <thread>
#include <vector>

#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/Logger/spdlog/fmt/fmt.h"
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

// Shared by the wide-row multi-column tests (Range round-trip + both
// column-targeted aggregate tests): a deterministic, pure function of
// rowIndex so the exact same row can be regenerated at verification time
// instead of carrying the inserted data around separately. 32 columns --
// column 0 is a numeric decoy held constant across every row (so a
// wrong-column read shows up as an obviously wrong aggregate), column 1 is
// always non-numeric (the "can't aggregate this" target), column 2 is
// numeric and varies with rowIndex (the real aggregate target), and the
// remaining columns cycle through several other oneof branches with
// per-row-varying values so the row is a realistic wide row, not padding.
constexpr int kWideRowColumnCount = 32;
constexpr int kWideRowCount = 10'000;

XStore_Proto::RowData BuildWideRow(int rowIndex) {
    std::vector<XStore_Proto::FData> Fields;
    Fields.push_back(FDouble(100.0));                                 // col 0: numeric decoy, constant
    Fields.push_back(FString("label-" + std::to_string(rowIndex)));   // col 1: non-numeric target
    Fields.push_back(FInt64(rowIndex));                                // col 2: numeric target, varies

    while (static_cast<int>(Fields.size()) < kWideRowColumnCount) {
        int i = static_cast<int>(Fields.size());
        switch (i % 6) {
            case 0: Fields.push_back(FFloat(static_cast<float>(i) + rowIndex)); break;
            case 1: Fields.push_back(FInt32(i - rowIndex)); break;
            case 2: Fields.push_back(FUInt64(static_cast<uint64_t>(i) + rowIndex)); break;
            case 3: Fields.push_back(FBool((i + rowIndex) % 2 == 0)); break;
            case 4: Fields.push_back(FBytes("b-" + std::to_string(i))); break;
            default: Fields.push_back(FSInt32(-i)); break;
        }
    }
    return MakeRow(Fields);
}

}  // namespace

class XStoreTest : public ::testing::Test {
 protected:
  std::unique_ptr<ScratchDir> Scratch;

  void SetUp() override {
      Scratch = std::make_unique<ScratchDir>("xstore_test");

      // CfgMap is a process-wide static cache keyed only by dbName (by
      // design -- production's DataStoreDir never changes for a process's
      // lifetime, so it doesn't need to be). Every test here reuses "TESTDB"
      // in its own fresh scratch directory though, so without this a DB
      // cached (and never explicitly RemoveDB'd -- e.g. RemoveDBFiles
      // legitimately leaves it cached, since it protects the .CFG) by an
      // earlier test would still look cached to a later, unrelated test's
      // brand-new (and otherwise empty) scratch directory.
      XStore::CfgMap.clear();
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

TEST_F(XStoreTest, InfoQueryReflectsMultipleCreatedDatabases) {
    XStore Store;
    // Distinct resolutions per DB so each dbinfo entry can be matched back to
    // its creator unambiguously, not just counted.
    Store.CreateDB("ALPHADB", XStore_Proto::SECOND, 64);
    Store.CreateDB("BETADB", XStore_Proto::MILLISECOND, 128);
    Store.CreateDB("GAMMADB", XStore_Proto::MICROSECOND, 256);

    XStore_Proto::SysInfo_REP Info = Store.InfoQuery("127.0.0.1", 3);

    ASSERT_EQ(Info.dbinfo_size(), 3);
    std::map<std::string, XStore_Proto::TimeUnit> ByName;
    for (int i = 0; i < Info.dbinfo_size(); i++) {
        ByName[Info.dbinfo(i).dbname()] = Info.dbinfo(i).dbresolution();
    }
    ASSERT_EQ(ByName.count("ALPHADB"), 1u);
    ASSERT_EQ(ByName.count("BETADB"), 1u);
    ASSERT_EQ(ByName.count("GAMMADB"), 1u);
    EXPECT_EQ(ByName["ALPHADB"], XStore_Proto::SECOND);
    EXPECT_EQ(ByName["BETADB"], XStore_Proto::MILLISECOND);
    EXPECT_EQ(ByName["GAMMADB"], XStore_Proto::MICROSECOND);
}

// ---------------------------------------------------------------------
// CreateDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, CreateDBRejectsRowLengthBelowFour) {
    XStore Store;
    XStore_Proto::CreateDB_REP Rep = Store.CreateDB("TESTDB", XStore_Proto::SECOND, 3);

    EXPECT_EQ(Rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Row length must be >= 4");
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

    EXPECT_EQ(Rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Already existed");
}

TEST_F(XStoreTest, CreateDBRejectsNanosecondResolution) {
    // ResolutionMap (include/XStore/XStore.h) lists NANOSECOND, but the
    // actual gate CreateDB enforces is PermittedTimeResolution, which only
    // allows SECOND/MILLISECOND/MICROSECOND -- confirms NANOSECOND is
    // actually rejected, not just absent from a stale comment.
    XStore Store;
    XStore_Proto::CreateDB_REP Rep = Store.CreateDB("TESTDB", XStore_Proto::NANOSECOND, 128);

    EXPECT_EQ(Rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Invalid time resolution");
    EXPECT_FALSE(fs::exists(Scratch->TestRoot / "DataStore" / "TESTDB"));
}

// ---------------------------------------------------------------------
// RemoveDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::RemoveDB_REP Rep = Store.RemoveDB("TESTDB");

    EXPECT_EQ(Rep.removestatus(), "[WARNING] [REMOVE DB] [DB: TESTDB] - DB does not exists");
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

TEST_F(XStoreTest, RemoveDBThenRecreateSameNameDoesNotReadStaleMmapData) {
    // RemoveDB calls EraseMappedFilesUnderPrefix so a future insert can't
    // keep writing into an orphaned, unlinked mapping (see the comment at
    // XStore::RemoveDB's call site) -- this confirms that actually holds: a
    // fresh DB reusing the old name must not see the previous DB's data
    // through a stale cached mapping the erase should have dropped.
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP FirstInsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("old-generation")}), "TESTDB", &FirstInsertRep);
    ASSERT_NE(FirstInsertRep.insertstatus().find("[INFO]"), std::string::npos);

    Store.RemoveDB("TESTDB");
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Same year, same offset as the old generation's write -- if the old
    // mapping survived, this read would incorrectly find "old-generation".
    XStore_Proto::UnaryQuery_REP StaleCheck;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &StaleCheck);
    EXPECT_EQ(StaleCheck.data().fielddata_size(), 0) << "found data through a stale mapping from the removed DB";

    XStore_Proto::UnaryInsert_REP SecondInsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("new-generation")}), "TESTDB", &SecondInsertRep);
    ASSERT_NE(SecondInsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).stringdata(), "new-generation");
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

TEST_F(XStoreTest, RemoveDBFilesThenReinsertSameYearDoesNotReadStaleMmapData) {
    // Same stale-mapping concern as RemoveDB above, but scoped to a single
    // file via EraseMappedFile (RemoveDBFiles's call site comment references
    // the same reasoning).
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP FirstInsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("old-generation")}), "TESTDB", &FirstInsertRep);
    ASSERT_NE(FirstInsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::RemoveDBFiles_REQ RemoveReq;
    RemoveReq.add_filename("2016.XSTORE");
    XStore_Proto::RemoveDBFiles_REP RemoveRep = Store.RemoveDBFiles(&RemoveReq, "TESTDB");
    ASSERT_NE(RemoveRep.removestatus(0).find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP StaleCheck;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &StaleCheck);
    EXPECT_EQ(StaleCheck.data().fielddata_size(), 0) << "found data through a stale mapping of the removed file";

    XStore_Proto::UnaryInsert_REP SecondInsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("new-generation")}), "TESTDB", &SecondInsertRep);
    ASSERT_NE(SecondInsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2016_MID, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).stringdata(), "new-generation");
}

// ---------------------------------------------------------------------
// UnaryInsert / UnaryQuery / UnaryDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, UnaryInsertOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::UnaryInsert_REP Rep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &Rep);

    EXPECT_EQ(Rep.insertstatus(), "[WARNING] [UNARY INSERT] [DB: TESTDB] - DB does not exist");
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

    EXPECT_EQ(QueryRep.querystatus(), "[WARNING] [UNARY QUERY] [DB: TESTDB] - DB does not exist");
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
    EXPECT_NE(QueryRep.querystatus().find("[WARNING]"), std::string::npos);
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
    EXPECT_EQ(DeleteRep.deletestatus(), "[WARNING] [UNARY DELETE] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, UnaryDeleteOnMissingYearFileReturnsError) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    // DB exists but nothing was ever inserted for this year -> no 2016.XSTORE file.
    XStore_Proto::UnaryDelete_REP DeleteRep = Store.UnaryDelete(TS_2016_MID, "TESTDB");
    EXPECT_EQ(DeleteRep.deletestatus(), "[WARNING] [UNARY DELETE] [DB: TESTDB] - 2016.XSTORE does not exist");
}

TEST_F(XStoreTest, UnaryInsertAtFirstAndLastEpochOfYearRoundTrip) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // TS_2017_FIRST/TS_2016_LAST are exactly TimeStruct's FirstEpoch/LastEpoch
    // for their respective years (see TimeStruct::ExtractTimeComponents).
    // LastEpoch is the risky case: GetOrMapFile pads the mapped file by one
    // extra row specifically because this write lands at offset
    // totalRows*rowLength, one row past (LastEpoch-FirstEpoch)*rowLength.
    XStore_Proto::UnaryInsert_REP FirstRep, LastRep;
    Store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-epoch-2017")}), "TESTDB", &FirstRep);
    Store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-epoch-2016")}), "TESTDB", &LastRep);
    EXPECT_NE(FirstRep.insertstatus().find("[INFO]"), std::string::npos);
    EXPECT_NE(LastRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP FirstQuery, LastQuery;
    Store.UnaryQuery(TS_2017_FIRST, "TESTDB", &FirstQuery);
    Store.UnaryQuery(TS_2016_LAST, "TESTDB", &LastQuery);

    ASSERT_EQ(FirstQuery.data().fielddata_size(), 1);
    EXPECT_EQ(FirstQuery.data().fielddata(0).stringdata(), "first-epoch-2017");
    ASSERT_EQ(LastQuery.data().fielddata_size(), 1);
    EXPECT_EQ(LastQuery.data().fielddata(0).stringdata(), "last-epoch-2016");
}

TEST_F(XStoreTest, UnaryInsertAdjacentTimestampsDoNotOverlap) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    constexpr int KRows = 40;
    XStore_Proto::UnaryInsert_REP InsertRep;
    for (int i = 0; i < KRows; i++) {
        Store.UnaryInsert(TS_2009_MID + i, MakeRow({FInt64(i), FString("row-" + std::to_string(i))}), "TESTDB", &InsertRep);
    }

    for (int i = 0; i < KRows; i++) {
        XStore_Proto::UnaryQuery_REP QueryRep;
        Store.UnaryQuery(TS_2009_MID + i, "TESTDB", &QueryRep);
        ASSERT_EQ(QueryRep.data().fielddata_size(), 2) << "i=" << i;
        EXPECT_EQ(QueryRep.data().fielddata(0).int64data(), i) << "i=" << i;
        EXPECT_EQ(QueryRep.data().fielddata(1).stringdata(), "row-" + std::to_string(i)) << "i=" << i;
    }
}

TEST_F(XStoreTest, UnaryInsertAdjacentTimestampsDoNotOverlapWithTightRowLength) {
    // rowLength set to the actual serialized size of one row (not a generous
    // round number) so adjacent slots sit back-to-back with zero slack --
    // any off-by-one in the offset math would spill into a neighbor.
    constexpr int KRows = 20;  // all values < 128 -> single-byte varint, so every row serializes to the same size
    XStore_Proto::RowData Sample = MakeRow({FInt64(0)});
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, (short)Sample.ByteSizeLong());

    XStore_Proto::UnaryInsert_REP InsertRep;
    for (int i = 0; i < KRows; i++) {
        Store.UnaryInsert(TS_2009_MID + i, MakeRow({FInt64(i)}), "TESTDB", &InsertRep);
        EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos) << "i=" << i;
    }

    for (int i = 0; i < KRows; i++) {
        XStore_Proto::UnaryQuery_REP QueryRep;
        Store.UnaryQuery(TS_2009_MID + i, "TESTDB", &QueryRep);
        ASSERT_EQ(QueryRep.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(QueryRep.data().fielddata(0).int64data(), i) << "i=" << i;
    }
}

TEST_F(XStoreTest, UnaryInsertRowLargerThanRowLengthIsRejectedAndDoesNotCorruptNeighbor) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 8);  // just enough for a short row, not more

    XStore_Proto::UnaryInsert_REP NeighborRep;
    Store.UnaryInsert(TS_2009_MID, MakeRow({FInt32(1)}), "TESTDB", &NeighborRep);
    ASSERT_NE(NeighborRep.insertstatus().find("[INFO]"), std::string::npos);

    // This row's serialized size exceeds the configured 8-byte rowLength.
    // Before the fix, XStore::UnaryInsert never checked SerializeToArray's
    // return value, so this silently truncated instead of erroring.
    XStore_Proto::UnaryInsert_REP OversizedRep;
    Store.UnaryInsert(TS_2009_MID + 1, MakeRow({FString("this string is far too long for an 8 byte row")}), "TESTDB", &OversizedRep);
    EXPECT_NE(OversizedRep.insertstatus().find("[WARNING]"), std::string::npos) << OversizedRep.insertstatus();
    EXPECT_NE(OversizedRep.insertstatus().find("exceeds"), std::string::npos) << OversizedRep.insertstatus();

    // The oversized attempt must not have written anything, and the
    // preceding row's data must be untouched.
    XStore_Proto::UnaryQuery_REP OversizedQuery, NeighborQuery;
    Store.UnaryQuery(TS_2009_MID + 1, "TESTDB", &OversizedQuery);
    EXPECT_EQ(OversizedQuery.data().fielddata_size(), 0);

    Store.UnaryQuery(TS_2009_MID, "TESTDB", &NeighborQuery);
    ASSERT_EQ(NeighborQuery.data().fielddata_size(), 1);
    EXPECT_EQ(NeighborQuery.data().fielddata(0).int32data(), 1);
}

TEST_F(XStoreTest, UnaryInsertRowWith32PlusMixedTypeColumnsRoundTrips) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 1024);

    // Two full passes over all 15 oneof branches (30 columns) plus a couple
    // extra, so this goes well past the 15-column ceiling every existing
    // test stops at.
    std::vector<XStore_Proto::FData> Fields;
    for (int pass = 0; pass < 2; pass++) {
        Fields.push_back(FDouble(1.5 + pass));
        Fields.push_back(FFloat(2.5f + pass));
        Fields.push_back(FInt32(-1 - pass));
        Fields.push_back(FInt64(-100LL - pass));
        Fields.push_back(FUInt32(1u + pass));
        Fields.push_back(FUInt64(100ULL + pass));
        Fields.push_back(FSInt32(-2 - pass));
        Fields.push_back(FSInt64(-200LL - pass));
        Fields.push_back(FFixed32(3u + pass));
        Fields.push_back(FFixed64(300ULL + pass));
        Fields.push_back(FSFixed32(-3 - pass));
        Fields.push_back(FSFixed64(-300LL - pass));
        Fields.push_back(FBool(pass % 2 == 0));
        Fields.push_back(FString("pass-" + std::to_string(pass)));
        Fields.push_back(FBytes("bytes-" + std::to_string(pass)));
    }
    Fields.push_back(FInt32(999));
    Fields.push_back(FString("tail"));
    ASSERT_GE(Fields.size(), 32u);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2003_MID, MakeRow(Fields), "TESTDB", &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos) << InsertRep.insertstatus();

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2003_MID, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.data().fielddata_size(), (int)Fields.size());
    const auto &d = QueryRep.data();
    for (int pass = 0; pass < 2; pass++) {
        int Base = pass * 15;
        EXPECT_DOUBLE_EQ(d.fielddata(Base + 0).doubledata(), 1.5 + pass);
        EXPECT_FLOAT_EQ(d.fielddata(Base + 1).floatdata(), 2.5f + pass);
        EXPECT_EQ(d.fielddata(Base + 2).int32data(), -1 - pass);
        EXPECT_EQ(d.fielddata(Base + 3).int64data(), -100LL - pass);
        EXPECT_EQ(d.fielddata(Base + 4).uint32data(), (uint32_t)(1 + pass));
        EXPECT_EQ(d.fielddata(Base + 5).uint64data(), (uint64_t)(100 + pass));
        EXPECT_EQ(d.fielddata(Base + 6).sint32data(), -2 - pass);
        EXPECT_EQ(d.fielddata(Base + 7).sint64data(), -200LL - pass);
        EXPECT_EQ(d.fielddata(Base + 8).fixed32data(), (uint32_t)(3 + pass));
        EXPECT_EQ(d.fielddata(Base + 9).fixed64data(), (uint64_t)(300 + pass));
        EXPECT_EQ(d.fielddata(Base + 10).sfixed32data(), -3 - pass);
        EXPECT_EQ(d.fielddata(Base + 11).sfixed64data(), -300LL - pass);
        EXPECT_EQ(d.fielddata(Base + 12).booldata(), pass % 2 == 0);
        EXPECT_EQ(d.fielddata(Base + 13).stringdata(), "pass-" + std::to_string(pass));
        EXPECT_EQ(d.fielddata(Base + 14).bytesdata(), "bytes-" + std::to_string(pass));
    }
    EXPECT_EQ(d.fielddata(30).int32data(), 999);
    EXPECT_EQ(d.fielddata(31).stringdata(), "tail");
}

TEST_F(XStoreTest, UnaryInsertCreatesDataFileAsSparseFile) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FInt32(1)}), "TESTDB", &InsertRep);

    fs::path DataFile = Scratch->TestRoot / "DataStore" / "TESTDB" / "2016.XSTORE";
    ASSERT_TRUE(fs::exists(DataFile));

    struct stat St{};
    ASSERT_EQ(stat(DataFile.string().c_str(), &St), 0);

    // Logical size covers a full year's worth of rows (~31.5M seconds *
    // 128 bytes), but only the single row actually written should have
    // real blocks behind it -- everything else must remain a hole.
    unsigned long long AllocatedBytes = static_cast<unsigned long long>(St.st_blocks) * 512ULL;
    EXPECT_GT(St.st_size, 1'000'000'000LL) << "expected a full year's logical span";
    EXPECT_LT(AllocatedBytes, 10ULL * 1024 * 1024) << "expected only a few pages actually allocated, not the full year";
}

TEST_F(XStoreTest, UnaryInsertUnsetFieldDataRoundTripsAsEmpty) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    // FData with no oneof branch set at all -- not FInt32(0), an actually-unset field.
    Store.UnaryInsert(TS_2031_MID, MakeRow({XStore_Proto::FData(), FString("after-unset")}), "TESTDB", &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2031_MID, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.data().fielddata_size(), 2);
    EXPECT_EQ(QueryRep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::ONEOFDATA_NOT_SET);
    EXPECT_EQ(QueryRep.data().fielddata(1).stringdata(), "after-unset");
}

TEST_F(XStoreTest, UnaryInsertMillisecondResolutionRoundTrips) {
    XStore Store;
    // Every other test in this suite uses SECOND resolution -- this exercises
    // TimeStruct's MILLISECOND scaling path instead (NANOSECOND is rejected
    // by CreateDB; PermittedTimeResolution only allows SECOND/MILLISECOND/MICROSECOND).
    Store.CreateDB("TESTDB", XStore_Proto::MILLISECOND, 128);

    const unsigned long long TsMs = TS_2016_MID * 1000ULL + 250;  // same instant, +250ms

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TsMs, MakeRow({FString("ms-resolution")}), "TESTDB", &InsertRep);
    EXPECT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos) << InsertRep.insertstatus();

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TsMs, "TESTDB", &QueryRep);
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).stringdata(), "ms-resolution");

    // A neighboring millisecond within the same second must not alias to
    // the same slot.
    XStore_Proto::UnaryQuery_REP NeighborQuery;
    Store.UnaryQuery(TsMs + 1, "TESTDB", &NeighborQuery);
    EXPECT_EQ(NeighborQuery.data().fielddata_size(), 0);
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
    EXPECT_EQ(InsertRep.insertstatus(), "[WARNING] [BATCH INSERT] [DB: NOPE] - DB does not exist");
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

TEST_F(XStoreTest, BatchDeleteTargetingMissingYearFileStillReportsCount) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    // TESTDB exists but nothing was ever inserted for 2016 -> no 2016.XSTORE.
    // BatchDelete used to silently `continue` here with no status set at all
    // mid-loop (unlike BatchQuery/RangeDelete, which both warn) -- the
    // trailing "Deleted N items" summary always overwrites deletestatus
    // regardless (same as RangeDelete), so the RESPONSE looks the same
    // either way; the fix is log-visibility parity (spdlog::warn) with the
    // sibling functions, not a response change. This just confirms the
    // missing-year branch doesn't crash and still falls through to a correct count.
    XStore_Proto::BatchDelete_REQ DeleteReq;
    DeleteReq.set_dbname("TESTDB");
    DeleteReq.add_timestamp(TS_2016_MID);
    XStore_Proto::BatchDelete_REP DeleteRep = Store.BatchDelete(&DeleteReq);
    EXPECT_NE(DeleteRep.deletestatus().find("Deleted 0 items"), std::string::npos) << DeleteRep.deletestatus();
}

TEST_F(XStoreTest, BatchDeleteWithWidelySeparatedTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (same-year) timestamps in one BatchDelete call.
    // BatchDelete now takes a single lock spanning [min offset, max offset]
    // per year-group instead of one lock per row (mirrors BatchInsert's
    // analogous test) -- confirms that coalesced lock actually covers the
    // far point, not just count*rowLength starting at the near one. The
    // fallocate64 calls themselves stay per-row (can't be coalesced without
    // deleting unrelated rows in the gap), so this only exercises the lock.
    const unsigned long long Near = TS_2009_MID;
    const unsigned long long Far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    XStore_Proto::UnaryInsert_REP SeedRep;
    Store.UnaryInsert(Near, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);
    Store.UnaryInsert(Far, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);

    constexpr int KIterations = 300;
    auto BatchDeleter = [&]() {
        XStore BdStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::BatchDelete_REQ Req;
            Req.set_dbname("TESTDB");
            Req.add_timestamp(Near);
            Req.add_timestamp(Far);
            BdStore.BatchDelete(&Req);
        }
    };
    auto FarWriter = [&]() {
        XStore FwStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            FwStore.UnaryInsert(Far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &Rep);
        }
    };

    std::thread T0(BatchDeleter);
    std::thread T1(FarWriter);
    T0.join();
    T1.join();

    XStore_Proto::UnaryQuery_REP FarQuery;
    Store.UnaryQuery(Far, "TESTDB", &FarQuery);
    // Whatever the final state (deleted, or a clean write from FarWriter),
    // it must be exactly one clean field or nothing -- never torn bytes from
    // an unprotected interleave between the punch and a concurrent write.
    EXPECT_LE(FarQuery.data().fielddata_size(), 1) << "row at the far offset must parse as at most one clean field, not torn/corrupted bytes";
    if (FarQuery.data().fielddata_size() == 1) {
        int64_t FinalValue = FarQuery.data().fielddata(0).int64data();
        EXPECT_TRUE(FinalValue == -1 || (FinalValue >= 3'000'000LL && FinalValue < 3'000'000LL + KIterations))
            << "finalValue=" << FinalValue;
    }
}

TEST_F(XStoreTest, BatchInsertOverwriteWithSmallerRowLeavesNoStaleTrailingBytes) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // First write a wide row directly at this offset...
    XStore_Proto::UnaryInsert_REP SeedRep;
    Store.UnaryInsert(TS_2009_MID, MakeRow({FString("a-fairly-long-first-payload-value")}), "TESTDB", &SeedRep);

    // ...then overwrite the same timestamp via BatchInsert with a much
    // narrower row. Before the fix, BatchInsert's fwrite() didn't zero the
    // slot first, so bytes past the new (shorter) serialization would still
    // belong to the old occupant.
    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
    Entry->set_timestamp(TS_2009_MID);
    *Entry->mutable_data() = MakeRow({FInt32(7)});
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2009_MID, "TESTDB", &QueryRep);

    // If stale bytes leaked through, this would either fail to parse as
    // exactly one field or the field would decode to garbage.
    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::kInt32Data);
    EXPECT_EQ(QueryRep.data().fielddata(0).int32data(), 7);
}

TEST_F(XStoreTest, BatchInsertRepeatedCallsToSameYearFileAccumulateViaCachedMapping) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // First call creates+maps 2016.XSTORE via GetOrMapFile; every later call
    // to the same year must hit that cached mapping instead of re-creating/
    // re-truncating it, which would wipe out rows written by earlier calls.
    for (int i = 0; i < 20; i++) {
        XStore_Proto::BatchInsert_REQ Req;
        Req.set_dbname("TESTDB");
        XStore_Proto::BatchInsert_DATA *Entry = Req.add_batchinsert_data();
        Entry->set_timestamp(TS_2016_MID + i);
        *Entry->mutable_data() = MakeRow({FInt64(i)});
        XStore_Proto::BatchInsert_REP Rep;
        Store.BatchInsert(&Req, &Rep);
    }

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (int i = 0; i < 20; i++) QueryReq.add_timestamp(TS_2016_MID + i);

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 20) << "an earlier call's rows were lost -- cached mapping must have been re-truncated instead of reused";
    std::map<unsigned long long, int64_t> ByTs;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        ByTs[QueryRep.queryresponse(i).timestamp()] = QueryRep.queryresponse(i).data().fielddata(0).int64data();
    }
    for (int i = 0; i < 20; i++) {
        EXPECT_EQ(ByTs[TS_2016_MID + i], i);
    }
}

TEST_F(XStoreTest, BatchInsertWithWidelySeparatedTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (same-year) timestamps in one BatchInsert call.
    // BatchInsert now takes a single lock spanning [min offset, max offset]
    // per year-group instead of one lock per row (mirrors RangeInsert's
    // analogous fix/test below) -- confirms that coalesced lock actually
    // covers the far point, not just count*rowLength starting at the near one.
    const unsigned long long Near = TS_2009_MID;
    const unsigned long long Far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    constexpr int KIterations = 300;
    auto BatchWriter = [&]() {
        XStore BwStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::BatchInsert_REQ Req;
            Req.set_dbname("TESTDB");
            XStore_Proto::BatchInsert_DATA *E0 = Req.add_batchinsert_data();
            E0->set_timestamp(Near);
            *E0->mutable_data() = MakeRow({FInt64(1'000'000LL + i)});
            XStore_Proto::BatchInsert_DATA *E1 = Req.add_batchinsert_data();
            E1->set_timestamp(Far);
            *E1->mutable_data() = MakeRow({FInt64(2'000'000LL + i)});
            XStore_Proto::BatchInsert_REP Rep;
            BwStore.BatchInsert(&Req, &Rep);
        }
    };
    auto FarWriter = [&]() {
        XStore FwStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            FwStore.UnaryInsert(Far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &Rep);
        }
    };

    std::thread T0(BatchWriter);
    std::thread T1(FarWriter);
    T0.join();
    T1.join();

    XStore_Proto::UnaryQuery_REP FarQuery;
    Store.UnaryQuery(Far, "TESTDB", &FarQuery);
    ASSERT_EQ(FarQuery.data().fielddata_size(), 1) << "row at the far offset must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t FinalValue = FarQuery.data().fielddata(0).int64data();
    EXPECT_TRUE((FinalValue >= 2'000'000LL && FinalValue < 2'000'000LL + KIterations) ||
                (FinalValue >= 3'000'000LL && FinalValue < 3'000'000LL + KIterations))
        << "finalValue=" << FinalValue;
}

TEST_F(XStoreTest, BatchQueryRowFillingEntireBufferWithNoZeroByteIsNotTreatedAsHole) {
    // Craft a row whose serialized bytes fill the whole rowLength slot with
    // no zero byte anywhere in it -- regression test for BatchQuery's old
    // strlen(BufferIn)==0 hole-check, which could read past a
    // non-null-terminated buffer in exactly this situation.
    XStore_Proto::RowData Sample = MakeRow({FBytes(std::string(20, '\xFF'))});
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, (short)Sample.ByteSizeLong());

    XStore_Proto::BatchInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    XStore_Proto::BatchInsert_DATA *Entry = InsertReq.add_batchinsert_data();
    Entry->set_timestamp(TS_2016_MID);
    *Entry->mutable_data() = Sample;
    XStore_Proto::BatchInsert_REP InsertRep;
    Store.BatchInsert(&InsertReq, &InsertRep);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    QueryReq.add_timestamp(TS_2016_MID);
    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), 1);
    ASSERT_EQ(QueryRep.queryresponse(0).data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.queryresponse(0).data().fielddata(0).bytesdata(), std::string(20, '\xFF'));
}

TEST_F(XStoreTest, BatchQueryEmptyTimestampListSucceedsWithNoResults) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");  // no timestamps at all

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);
    EXPECT_EQ(QueryRep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQueryAllRequestedTimestampsAreHolesReturnsEmptyResponse) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP SeedRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FInt32(1)}), "TESTDB", &SeedRep);  // creates 2016.XSTORE

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    QueryReq.add_timestamp(TS_2016_MID + 10);
    QueryReq.add_timestamp(TS_2016_MID + 20);

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);
    EXPECT_EQ(QueryRep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQueryRejectsRequestOver2GBLimit) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 32000);

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    // 63000 * 32000 > 2,000,000,000 -- just over BatchQuery's hard limit.
    for (int i = 0; i < 63000; i++) {
        QueryReq.add_timestamp(TS_2016_MID + i);
    }

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);
    EXPECT_NE(QueryRep.querystatus().find("larger than 2GB"), std::string::npos) << QueryRep.querystatus();
    EXPECT_EQ(QueryRep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQuerySortTimeFalseWithUnsortedTimestampsRoundTrips) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // All in the same year, deliberately far apart and inserted/queried in a
    // non-ascending order. With SortTime=false, BatchQuery's per-year index
    // vector is NOT guaranteed sorted, so it can't size its single coalesced
    // lock off front()/back() the way BatchInsert (which always sorts) does
    // -- exercises the min/max scan it uses instead.
    std::map<unsigned long long, std::string> Expected = {
        {TS_2016_MID + 50000, "mid"},
        {TS_2016_MID, "low"},
        {TS_2016_MID + 100000, "high"},
        {TS_2016_MID + 25000, "low-mid"},
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

    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2016_MID + 100000, TS_2016_MID, TS_2016_MID + 50000, TS_2016_MID + 25000}) {
        QueryReq.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep, /*SortTime=*/false);

    ASSERT_EQ(QueryRep.queryresponse_size(), 4);
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        const auto &Entry = QueryRep.queryresponse(i);
        ASSERT_TRUE(Expected.count(Entry.timestamp())) << "unexpected timestamp " << Entry.timestamp();
        ASSERT_EQ(Entry.data().fielddata_size(), 1);
        EXPECT_EQ(Entry.data().fielddata(0).stringdata(), Expected[Entry.timestamp()]);
    }
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

// Unlike the test above, every Unary/Batch/Range* test row up to this point
// used a single column, so a multi-column row never actually round-tripped
// through RangeInsert/RangeQuery -- only through Unary/Batch (see
// UnaryInsertRowWith32PlusMixedTypeColumnsRoundTrips and
// BatchInsertMixedColumnCountsPerTimestamp).
TEST_F(XStoreTest, RangeInsertThenRangeQueryMultiColumnRowsRoundTrip) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 1024);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (int i = 0; i < kWideRowCount; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2024_MID + i);
        *Entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::RangeQuery_REP QueryRep;
    Store.RangeQuery(TS_2024_MID, TS_2024_MID + kWideRowCount - 1, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.queryresponse_size(), kWideRowCount);
    for (int i = 0; i < kWideRowCount; i++) {
        const auto &Data = QueryRep.queryresponse(i).data();
        ASSERT_EQ(Data.fielddata_size(), kWideRowColumnCount);
        // BuildWideRow is a pure function of rowIndex, so regenerating the
        // expected row and comparing serialized bytes checks every one of
        // the 32 columns' type and value in one assertion, rather than
        // duplicating BuildWideRow's per-column logic here to drift from it.
        EXPECT_EQ(Data.SerializeAsString(), BuildWideRow(i).SerializeAsString());
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
    EXPECT_EQ(InsertRep.insertstatus(), "[WARNING] [RANGE INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, RangeDeleteSingleYearMissingFileReturnsCountSummaryNotDbNotExistError) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    // TESTDB directory exists (CreateDB above), but no year file was ever
    // written -- 2009.XSTORE specifically doesn't exist. RangeDelete's old
    // single-year branch special-cased this with an early-return "[ERROR]
    // ... DB does not exist" (a stale/misleading copy-paste of the
    // is_directory check's message -- the DB does exist, only the year file
    // doesn't), skipping the trailing "Deleted N items" summary every other
    // missing-year case falls through to. Unifying with the multi-year
    // handling makes a missing year behave the same everywhere: warn and
    // still report the (zero) count.
    XStore_Proto::RangeDelete_REP DeleteRep = Store.RangeDelete(TS_2009_MID, TS_2009_MID + 10, "TESTDB");
    EXPECT_NE(DeleteRep.deletestatus().find("Deleted 0 items"), std::string::npos) << DeleteRep.deletestatus();
}

TEST_F(XStoreTest, RangeDeleteReclaimsDiskSpaceAndMmapCacheReflectsTheHole) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Wide-enough span (5000 rows) that real block allocation is easily
    // measurable and not noise from filesystem allocation granularity.
    constexpr int KRowCount = 5000;
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (int i = 0; i < KRowCount; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2024_MID + i);
        *Entry->mutable_data() = MakeRow({FInt64(i)});
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    // RangeInsert (and the RangeQuery below) go through the shared mmap
    // cache (GetOrMapFile); RangeDelete's fallocate64 punches the hole via a
    // separate, freshly-opened fd -- confirms that hole is visible through
    // an *already-mapped* MAP_SHARED region too, not just to fresh reads.
    XStore_Proto::RangeQuery_REP PreDeleteQuery;
    Store.RangeQuery(TS_2024_MID, TS_2024_MID + KRowCount - 1, "TESTDB", &PreDeleteQuery);
    ASSERT_EQ(PreDeleteQuery.queryresponse_size(), KRowCount);
    for (int i = 0; i < KRowCount; i++) {
        ASSERT_EQ(PreDeleteQuery.queryresponse(i).data().fielddata_size(), 1) << "i=" << i;
    }

    fs::path DataFile = Scratch->TestRoot / "DataStore" / "TESTDB" / "2024.XSTORE";
    struct stat StBeforeDelete{};
    ASSERT_EQ(stat(DataFile.string().c_str(), &StBeforeDelete), 0);
    unsigned long long AllocatedBeforeDelete = static_cast<unsigned long long>(StBeforeDelete.st_blocks) * 512ULL;
    EXPECT_GT(AllocatedBeforeDelete, 0ULL) << "expected real blocks behind the inserted rows before deleting";

    XStore_Proto::RangeDelete_REP DeleteRep = Store.RangeDelete(TS_2024_MID, TS_2024_MID + KRowCount - 1, "TESTDB");
    EXPECT_NE(DeleteRep.deletestatus().find(fmt::format("Deleted {} items", KRowCount)), std::string::npos) << DeleteRep.deletestatus();

    // Disk space actually reclaimed -- not just "reads now return empty",
    // which fallocate's FALLOC_FL_KEEP_SIZE (logical size unchanged) doesn't
    // by itself guarantee unless FALLOC_FL_PUNCH_HOLE is also set.
    struct stat StAfterDelete{};
    ASSERT_EQ(stat(DataFile.string().c_str(), &StAfterDelete), 0);
    unsigned long long AllocatedAfterDelete = static_cast<unsigned long long>(StAfterDelete.st_blocks) * 512ULL;
    EXPECT_EQ(StAfterDelete.st_size, StBeforeDelete.st_size) << "logical file size (FALLOC_FL_KEEP_SIZE) must be unchanged by the delete";
    EXPECT_LT(AllocatedAfterDelete, AllocatedBeforeDelete / 2) << "expected the punched range's blocks to be actually freed, not just zero-filled";

    // Data actually gone, read back through the same cached mapping RangeInsert/RangeQuery used above.
    XStore_Proto::RangeQuery_REP PostDeleteQuery;
    Store.RangeQuery(TS_2024_MID, TS_2024_MID + KRowCount - 1, "TESTDB", &PostDeleteQuery);
    ASSERT_EQ(PostDeleteQuery.queryresponse_size(), KRowCount);
    for (int i = 0; i < KRowCount; i++) {
        EXPECT_EQ(PostDeleteQuery.queryresponse(i).data().fielddata_size(), 0) << "i=" << i << " -- stale data read through an already-mapped region after fallocate hole-punch on a different fd";
    }
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

TEST_F(XStoreTest, RangeInsertOverwriteWithSmallerRowLeavesNoStaleTrailingBytes) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP SeedRep;
    Store.UnaryInsert(TS_2009_MID, MakeRow({FString("a-fairly-long-first-payload-value")}), "TESTDB", &SeedRep);

    // Same stale-trailing-bytes concern as BatchInsert, but through RangeInsert's fwrite() path.
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
    Entry->set_timestamp(TS_2009_MID);
    *Entry->mutable_data() = MakeRow({FInt32(7)});
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::UnaryQuery_REP QueryRep;
    Store.UnaryQuery(TS_2009_MID, "TESTDB", &QueryRep);

    ASSERT_EQ(QueryRep.data().fielddata_size(), 1);
    EXPECT_EQ(QueryRep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::kInt32Data);
    EXPECT_EQ(QueryRep.data().fielddata(0).int32data(), 7);
}

TEST_F(XStoreTest, RangeInsertWithNonContiguousTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (but ascending) timestamps within the same year --
    // before the fix, RangeInsert's combined lock only covered
    // count*rowLength (2*rowLength) bytes starting at the first offset,
    // leaving the actual second write (far away) unprotected against a
    // concurrent writer targeting that same distant offset.
    const unsigned long long Near = TS_2009_MID;
    const unsigned long long Far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    constexpr int KIterations = 300;
    auto RangeWriter = [&]() {
        XStore RwStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::RangeInsert_REQ Req;
            Req.set_dbname("TESTDB");
            XStore_Proto::RangeInsert_DATA *E0 = Req.add_rangeinsert_data();
            E0->set_timestamp(Near);
            *E0->mutable_data() = MakeRow({FInt64(1'000'000LL + i)});
            XStore_Proto::RangeInsert_DATA *E1 = Req.add_rangeinsert_data();
            E1->set_timestamp(Far);
            *E1->mutable_data() = MakeRow({FInt64(2'000'000LL + i)});
            XStore_Proto::RangeInsert_REP Rep;
            RwStore.RangeInsert(&Req, &Rep);
        }
    };
    auto FarWriter = [&]() {
        XStore FwStore;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            FwStore.UnaryInsert(Far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &Rep);
        }
    };

    std::thread T0(RangeWriter);
    std::thread T1(FarWriter);
    T0.join();
    T1.join();

    XStore_Proto::UnaryQuery_REP FarQuery;
    Store.UnaryQuery(Far, "TESTDB", &FarQuery);
    ASSERT_EQ(FarQuery.data().fielddata_size(), 1) << "row at the far offset must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t FinalValue = FarQuery.data().fielddata(0).int64data();
    EXPECT_TRUE((FinalValue >= 2'000'000LL && FinalValue < 2'000'000LL + KIterations) ||
                (FinalValue >= 3'000'000LL && FinalValue < 3'000'000LL + KIterations))
        << "finalValue=" << FinalValue;
}

TEST_F(XStoreTest, RangeQueryLastRowInSpanIsLockedAgainstConcurrentWriter) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // RegionLength used to be sized as count-1 rows (missing a "+1" to cover
    // the inclusive [start, end] the read loop actually walks), so the very
    // last row of the queried span sat outside the locked byte range. End is
    // that last row.
    //
    // A data-race-based test (racing a writer against many RangeQuery calls
    // and checking for torn reads) turned out unreliable here -- the actual
    // unprotected-memory race window is nanoseconds wide, so it doesn't
    // reproduce on demand under a plain thread scheduler. This instead tests
    // the lock's actual coverage directly: hold a manual WRITE lock on End's
    // exact byte offset (computed the same way XStore does internally) and
    // confirm RangeQuery blocks on it -- a lock region that excludes End
    // would let RangeQuery return immediately without contending at all.
    const unsigned long long Start = TS_2009_MID;
    const unsigned long long End = TS_2009_MID + 5;
    constexpr short RowLength = 128;

    XStore_Proto::UnaryInsert_REP SeedRep;
    Store.UnaryInsert(End, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate the year file

    TimeStruct Ts(static_cast<time_t>(End), XStore_Proto::SECOND);
    const unsigned long long Offset = (End - Ts.FirstEpoch) * RowLength;
    const std::string TargetFile = fmt::format("{}/DataStore/TESTDB/{}.XSTORE", Store.BaseDir, Ts.Year);

    constexpr auto HoldDuration = std::chrono::milliseconds(150);
    std::atomic<bool> LockAcquired{false};
    std::thread Holder([&]() {
        FileRangeLockGuard Guard(TargetFile, Offset, RowLength, FileRangeLock::LockType::WRITE);
        LockAcquired = true;
        std::this_thread::sleep_for(HoldDuration);
    });

    while (!LockAcquired) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // let Holder settle in

    auto QueryStart = std::chrono::steady_clock::now();
    XStore_Proto::RangeQuery_REP Rep;
    Store.RangeQuery(Start, End, "TESTDB", &Rep);
    auto Elapsed = std::chrono::steady_clock::now() - QueryStart;

    Holder.join();

    ASSERT_EQ(Rep.queryresponse_size(), 6);
    EXPECT_GE(Elapsed, HoldDuration - std::chrono::milliseconds(20))
        << "RangeQuery returned without waiting on a WRITE lock held on the span's last row -- its lock region doesn't cover [start, end] inclusive";
}

TEST_F(XStoreTest, RangeQuerySpanningThreeYearsWithMissingMiddleYearFile) {
    XStore Store;
    // A ~31.5M-second span (nearly 2 full years) at the usual 128-byte
    // rowLength would exceed RangeQuery's 2GB result-size guard, so this
    // uses a much narrower row -- still comfortably enough for a short string.
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 32);

    // 1998, 1999 (missing), 2000 -- exercises the multi-year while loop
    // running more than one full iteration, including a year whose
    // .XSTORE file was never created (the "warn and continue" branch).
    constexpr unsigned long long TS_1998_LAST = 915148799ULL;      // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long TS_2000_FIRST_PLUS = 946684810ULL; // 2000-01-01 00:00:10 UTC

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_LAST, MakeRow({FString("end-of-1998")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2000_FIRST_PLUS, MakeRow({FString("start-of-2000")}), "TESTDB", &InsertRep);
    // Deliberately nothing inserted for 1999 -> 1999.XSTORE never gets created.

    XStore_Proto::RangeQuery_REP QueryRep;
    Store.RangeQuery(TS_1998_LAST, TS_2000_FIRST_PLUS, "TESTDB", &QueryRep);

    std::map<unsigned long long, std::string> Found;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        const auto &Entry = QueryRep.queryresponse(i);
        if (Entry.data().fielddata_size() > 0) {
            Found[Entry.timestamp()] = Entry.data().fielddata(0).stringdata();
        }
    }
    EXPECT_EQ(Found[TS_1998_LAST], "end-of-1998");
    EXPECT_EQ(Found[TS_2000_FIRST_PLUS], "start-of-2000");
}

TEST_F(XStoreTest, RangeQueryMissingFinalYearFileReturnsInsteadOfHanging) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 32);

    constexpr unsigned long long TS_1998_LAST = 915148799ULL;   // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long TS_1999_EARLY = 915148810ULL;  // 1999-01-01 00:00:10 UTC -- 1999.XSTORE never created

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_LAST, MakeRow({FString("end-of-1998")}), "TESTDB", &InsertRep);  // creates 1998.XSTORE only

    // Regression test for an infinite loop: the final-year "file missing"
    // branch used to `continue` after a no-op CurrTs recompute
    // (StartEpoch+(endEpochT-StartEpoch) always equals endEpochT, so nothing
    // actually advances) instead of `break`, so a missing FINAL year spun the
    // outer while loop forever. Starts right at the year boundary (not
    // TS_1998_MID) so the legitimate 1998 segment is a handful of rows, not
    // millions -- otherwise that alone could out-run the timeout below and
    // be mistaken for the hang this test is trying to catch. Runs off-thread
    // with a timeout so a regression fails this test instead of hanging the
    // whole binary.
    std::atomic<bool> Completed{false};
    XStore_Proto::RangeQuery_REP QueryRep;
    std::thread Worker([&]() {
        Store.RangeQuery(TS_1998_LAST, TS_1999_EARLY, "TESTDB", &QueryRep);
        Completed = true;
    });

    for (int i = 0; i < 100 && !Completed; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (!Completed) {
        Worker.detach();  // don't join a thread that may be stuck forever
        FAIL() << "RangeQuery hung instead of returning when the final year's file was missing";
        return;
    }
    Worker.join();

    bool Found1998 = false;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        if (QueryRep.queryresponse(i).timestamp() == TS_1998_LAST) {
            Found1998 = QueryRep.queryresponse(i).data().fielddata_size() == 1;
        }
    }
    EXPECT_TRUE(Found1998);
    EXPECT_NE(QueryRep.querystatus().find("1999.XSTORE does not exist"), std::string::npos) << QueryRep.querystatus();
}

TEST_F(XStoreTest, RangeDeleteSpanningThreeYearsWithMissingMiddleYearFileDeletesExistingEndpoints) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long TS_1998_LAST = 915148799ULL;
    constexpr unsigned long long TS_2000_FIRST_PLUS = 946684810ULL;

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_LAST, MakeRow({FString("end-of-1998")}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2000_FIRST_PLUS, MakeRow({FString("start-of-2000")}), "TESTDB", &InsertRep);
    // 1999.XSTORE never created.

    Store.RangeDelete(TS_1998_LAST, TS_2000_FIRST_PLUS, "TESTDB");

    // Confirms the delete reaches (and clears) both endpoints without
    // hanging or skipping past them while walking through the missing
    // middle year.
    XStore_Proto::UnaryQuery_REP Rep1998, Rep2000;
    Store.UnaryQuery(TS_1998_LAST, "TESTDB", &Rep1998);
    Store.UnaryQuery(TS_2000_FIRST_PLUS, "TESTDB", &Rep2000);
    EXPECT_EQ(Rep1998.data().fielddata_size(), 0);
    EXPECT_EQ(Rep2000.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, RangeDeleteMissingFinalYearFileReturnsInsteadOfHanging) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long TS_1998_LAST = 915148799ULL;   // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long TS_1999_EARLY = 915148810ULL;  // 1999-01-01 00:00:10 UTC -- 1999.XSTORE never created

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_LAST, MakeRow({FString("end-of-1998")}), "TESTDB", &InsertRep);  // creates 1998.XSTORE only

    // Same infinite-loop shape as RangeQuery's analogous regression test
    // above -- same reason for starting right at the year boundary too.
    std::atomic<bool> Completed{false};
    XStore_Proto::RangeDelete_REP DeleteRep;
    std::thread Worker([&]() {
        DeleteRep = Store.RangeDelete(TS_1998_LAST, TS_1999_EARLY, "TESTDB");
        Completed = true;
    });

    for (int i = 0; i < 100 && !Completed; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (!Completed) {
        Worker.detach();
        FAIL() << "RangeDelete hung instead of returning when the final year's file was missing";
        return;
    }
    Worker.join();

    // RangeDelete's final status is always the trailing count summary (any
    // mid-loop WARNING gets overwritten by it), so correctness is checked via
    // the count instead: only TS_1998_LAST (the single point in the existing
    // year) was deleted.
    EXPECT_NE(DeleteRep.deletestatus().find("Deleted 1 items"), std::string::npos) << DeleteRep.deletestatus();

    XStore_Proto::UnaryQuery_REP Rep1998;
    Store.UnaryQuery(TS_1998_LAST, "TESTDB", &Rep1998);
    EXPECT_EQ(Rep1998.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, RangeQueryRejectsRequestOver2GBLimit) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 32000);

    XStore_Proto::RangeQuery_REP QueryRep;
    // (end-start) * rowLength > 2GB -- just over RangeQuery's hard limit.
    Store.RangeQuery(TS_2016_MID, TS_2016_MID + 63000, "TESTDB", &QueryRep);
    EXPECT_NE(QueryRep.querystatus().find("larger than 2GB"), std::string::npos) << QueryRep.querystatus();
    EXPECT_EQ(QueryRep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, UnaryBatchAndRangeInsertQueryShareSameCachedMappingWithinAYear) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Write the same year's file through all three insert paths, then read
    // every point back through all three query paths -- Unary, Batch and
    // Range all go through the same GetOrMapFile cache now, so a write via
    // one path must be immediately visible via the others without a stale
    // read (previously Batch/Range wrote via stdio underneath whatever
    // mapping Unary might already have open for that same file).
    XStore_Proto::UnaryInsert_REP UnaryRep;
    Store.UnaryInsert(TS_2009_MID, MakeRow({FString("via-unary")}), "TESTDB", &UnaryRep);

    XStore_Proto::BatchInsert_REQ BatchReq;
    BatchReq.set_dbname("TESTDB");
    XStore_Proto::BatchInsert_DATA *BatchEntry = BatchReq.add_batchinsert_data();
    BatchEntry->set_timestamp(TS_2009_MID + 1);
    *BatchEntry->mutable_data() = MakeRow({FString("via-batch")});
    XStore_Proto::BatchInsert_REP BatchRep;
    Store.BatchInsert(&BatchReq, &BatchRep);

    XStore_Proto::RangeInsert_REQ RangeReq;
    RangeReq.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *RangeEntry = RangeReq.add_rangeinsert_data();
    RangeEntry->set_timestamp(TS_2009_MID + 2);
    *RangeEntry->mutable_data() = MakeRow({FString("via-range")});
    XStore_Proto::RangeInsert_REP RangeRep;
    Store.RangeInsert(&RangeReq, &RangeRep);

    // Read every point back through UnaryQuery.
    for (int i = 0; i < 3; i++) {
        XStore_Proto::UnaryQuery_REP Rep;
        Store.UnaryQuery(TS_2009_MID + i, "TESTDB", &Rep);
        ASSERT_EQ(Rep.data().fielddata_size(), 1) << "i=" << i;
    }

    // Read every point back through BatchQuery.
    XStore_Proto::BatchQuery_REQ QueryReq;
    QueryReq.set_dbname("TESTDB");
    for (int i = 0; i < 3; i++) QueryReq.add_timestamp(TS_2009_MID + i);
    XStore_Proto::BatchQuery_REP QueryRep;
    Store.BatchQuery(&QueryReq, &QueryRep);
    ASSERT_EQ(QueryRep.queryresponse_size(), 3);
    std::map<unsigned long long, std::string> ByTs;
    for (int i = 0; i < QueryRep.queryresponse_size(); i++) {
        ByTs[QueryRep.queryresponse(i).timestamp()] = QueryRep.queryresponse(i).data().fielddata(0).stringdata();
    }
    EXPECT_EQ(ByTs[TS_2009_MID], "via-unary");
    EXPECT_EQ(ByTs[TS_2009_MID + 1], "via-batch");
    EXPECT_EQ(ByTs[TS_2009_MID + 2], "via-range");

    // Read every point back through RangeQuery.
    XStore_Proto::RangeQuery_REP RangeQueryRep;
    Store.RangeQuery(TS_2009_MID, TS_2009_MID + 2, "TESTDB", &RangeQueryRep);
    ASSERT_EQ(RangeQueryRep.queryresponse_size(), 3);
    EXPECT_EQ(RangeQueryRep.queryresponse(0).data().fielddata(0).stringdata(), "via-unary");
    EXPECT_EQ(RangeQueryRep.queryresponse(1).data().fielddata(0).stringdata(), "via-batch");
    EXPECT_EQ(RangeQueryRep.queryresponse(2).data().fielddata(0).stringdata(), "via-range");
}

// ---------------------------------------------------------------------
// Mmap LRU cache -- GetOrMapFile caches PROT_WRITE mappings capped at
// XStore::MmapCacheCap (256, see include/XStore/XStore.h); Unary/Batch/Range
// insert+query all share it (confirmed above).
// ---------------------------------------------------------------------

TEST_F(XStoreTest, GetOrMapFileEvictsBeyondCapAndEvictedFilesStillReadCorrectly) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 8);

    constexpr int KYearCount = 300;  // > MmapCacheCap (256) -- forces eviction
    std::vector<unsigned long long> Timestamps;
    Timestamps.reserve(KYearCount);

    for (int i = 0; i < KYearCount; i++) {
        int Year = 1700 + i;  // distinct year per iteration -> distinct .XSTORE file per insert
        struct tm YearMid{};
        YearMid.tm_year = Year - 1900;
        YearMid.tm_mon = 6;  // July -- far from any year boundary
        YearMid.tm_mday = 1;
        YearMid.tm_hour = 12;
        unsigned long long Ts = static_cast<unsigned long long>(timegm(&YearMid));
        Timestamps.push_back(Ts);

        XStore_Proto::UnaryInsert_REP InsertRep;
        Store.UnaryInsert(Ts, MakeRow({FInt32(i)}), "TESTDB", &InsertRep);
        ASSERT_NE(InsertRep.insertstatus().find("[INFO]"), std::string::npos) << "i=" << i;
    }

    // Cache must never grow past its cap despite KYearCount distinct files
    // having been mapped over the course of the loop above.
    EXPECT_LE(XStore::MmapLru.size(), XStore::MmapCacheCap);
    EXPECT_EQ(XStore::MmapLru.size(), XStore::MmapIndex.size());

    // Read every point back, including the ones whose mapping must have been
    // evicted (and therefore has to be transparently re-created by
    // GetOrMapFile on this read) -- correctness must survive eviction+remap,
    // not just hold for whatever's still in the originally-cached tail.
    for (int i = 0; i < KYearCount; i++) {
        XStore_Proto::UnaryQuery_REP QueryRep;
        Store.UnaryQuery(Timestamps[i], "TESTDB", &QueryRep);
        ASSERT_EQ(QueryRep.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(QueryRep.data().fielddata(0).int32data(), i) << "i=" << i;
    }
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

// Min/Max/Sum/Avg all gate on GetOrLoadCfg the same way every insert/query
// path does, but unlike those paths (each covered by its own *OnMissingDB*
// test above) none of the four aggregates had a missing-DB regression test.
TEST_F(XStoreTest, MinAggregateOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "[WARNING] [MIN AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, MaxAggregateOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &MaxRep);
    EXPECT_EQ(MaxRep.opstatus(), "[WARNING] [MAX AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, SumAggregateOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &SumRep);
    EXPECT_EQ(SumRep.opstatus(), "[WARNING] [SUM AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, AvgAggregateOnMissingDBReturnsError) {
    XStore Store;
    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &AvgRep);
    EXPECT_EQ(AvgRep.opstatus(), "[WARNING] [AVG AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

// All four aggregates gate on startEpochT > endEpochT the same way, right
// after the GetOrLoadCfg check above -- covered for none of them until now.
TEST_F(XStoreTest, MinAggregateRejectsStartAfterEnd) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "[WARNING] [MIN AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, MaxAggregateRejectsStartAfterEnd) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &MaxRep);
    EXPECT_EQ(MaxRep.opstatus(), "[WARNING] [MAX AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, SumAggregateRejectsStartAfterEnd) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &SumRep);
    EXPECT_EQ(SumRep.opstatus(), "[WARNING] [SUM AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, AvgAggregateRejectsStartAfterEnd) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &AvgRep);
    EXPECT_EQ(AvgRep.opstatus(), "[WARNING] [AVG AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, SumAndAvgAggregateWithRemainderDivisor) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    const std::vector<double> Values = {2.0, 3.0, 5.0};  // sum=10, avg=3.333...
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (size_t i = 0; i < Values.size(); i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2009_MID + i);
        *Entry->mutable_data() = MakeRow({FDouble(Values[i])});
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2009_MID, TS_2009_MID + 2, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 10.0);

    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(TS_2009_MID, TS_2009_MID + 2, "TESTDB", 0, &AvgRep);
    EXPECT_NEAR(AvgRep.opresult(), 10.0 / 3.0, 1e-9);
}

TEST_F(XStoreTest, AggregateOverMultiYearRangeSpansAllYears) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Data sits right at the 2016/2017 year boundary (not mid-year) so the
    // aggregate kernels only need to scan a couple of rows to prove the
    // multi-year loop crosses correctly -- a genuinely mid-year span would
    // force a full ~31.5M-row linear scan of any fully-contained year that
    // has data, which is correct behavior but far too slow to use as a test
    // fixture.
    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_LAST, MakeRow({FDouble(5.0)}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2017_FIRST, MakeRow({FDouble(-10.0)}), "TESTDB", &InsertRep);

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), -10.0);

    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &MaxRep);
    EXPECT_DOUBLE_EQ(MaxRep.opresult().doubledata(), 5.0);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), -5.0);

    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &AvgRep);
    EXPECT_DOUBLE_EQ(AvgRep.opresult(), -2.5);
}

TEST_F(XStoreTest, AggregateOverRangeWithMissingMiddleYearFileSkipsItAndReflectsRemainingYears) {
    // Same "warn and continue past a missing year file" shape as
    // RangeQuery/RangeDelete's analogous multi-year tests, but for the
    // aggregate kernels -- 1998, 1999 (missing), 2000, exercising the
    // multi-year while loop's missing-file branch for both
    // ProcessFileRangeSelect (Min, via MinAggregate) and
    // ProcessFileRangeAccumulate (Sum, via SumAggregate).
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long TS_1998_LAST = 915148799ULL;       // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long TS_2000_FIRST_PLUS = 946684810ULL; // 2000-01-01 00:00:10 UTC

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_1998_LAST, MakeRow({FDouble(5.0)}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2000_FIRST_PLUS, MakeRow({FDouble(-10.0)}), "TESTDB", &InsertRep);
    // Deliberately nothing inserted for 1999 -> 1999.XSTORE never gets created.

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_1998_LAST, TS_2000_FIRST_PLUS, "TESTDB", 0, &MinRep);
    EXPECT_EQ(MinRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), -10.0);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_1998_LAST, TS_2000_FIRST_PLUS, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), -5.0);
}

TEST_F(XStoreTest, MinMaxAggregateTieBreakReturnsFirstEncounteredRow) {
    // ProcessFileRangeSelect only replaces the running best on a *strict*
    // Better(candidate, best) -- ties never replace -- so among several rows
    // sharing the winning value, the first one scanned (earliest timestamp)
    // must be the one returned. Column 1 carries a per-row marker so the
    // winning row's identity (not just its column-0 value) can be checked.
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    auto AddRow = [&](int i, double val, const std::string &marker) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2009_MID + i);
        *Entry->mutable_data() = MakeRow({FDouble(val), FString(marker)});
    };
    AddRow(0, 5.0, "first-min");
    AddRow(1, 1.0, "first-tie");   // ties for MIN
    AddRow(2, 1.0, "second-tie");  // same MIN value, must lose to the earlier row
    AddRow(3, 5.0, "second-max");  // ties for MAX with row 0

    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2009_MID, TS_2009_MID + 3, "TESTDB", 0, &MinRep);
    ASSERT_EQ(MinRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(TS_2009_MID, TS_2009_MID + 3, "TESTDB", 0, &MaxRep);
    ASSERT_EQ(MaxRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(MaxRep.opresult().doubledata(), 5.0);
}

TEST_F(XStoreTest, AggregateSkipsUnsetFieldData) {
    // Distinct branch from AggregateSkipsNonNumericColumnValues below: an
    // actually-unset FData (oneOfData_case() == 0) fails the kernels'
    // "case() > 0" check, not the "case() <= 12" one.
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    auto AddRow = [&](int i, const XStore_Proto::FData &f) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2031_MID + i);
        *Entry->mutable_data() = MakeRow({f});
    };
    AddRow(0, FDouble(3.0));
    AddRow(1, XStore_Proto::FData());  // unset -- must not be treated as 0.0
    AddRow(2, FDouble(7.0));

    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2031_MID, TS_2031_MID + 2, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 10.0) << "unset field must not contribute (not even as 0.0)";

    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(TS_2031_MID, TS_2031_MID + 2, "TESTDB", 0, &AvgRep);
    EXPECT_DOUBLE_EQ(AvgRep.opresult(), 5.0) << "divisor must be 2 (valid rows only), not 3";
}

TEST_F(XStoreTest, AggregateTreatsExplicitlyEmptyRowSameAsHole) {
    // The aggregate kernels' hole check is just RowPtr[0] == 0 (the first
    // serialized byte) -- normally safe, since FData's first field always
    // serializes with a non-zero protobuf tag byte. But a row inserted with
    // *zero* fielddata entries serializes to zero bytes, so its slot stays
    // all-zero (indistinguishable from a never-written hole). Confirms that
    // degenerate case doesn't crash and is simply excluded, same as a real
    // hole would be.
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(4.0)}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2016_MID + 1, MakeRow({}), "TESTDB", &InsertRep);  // explicitly zero columns
    Store.UnaryInsert(TS_2016_MID + 2, MakeRow({FDouble(6.0)}), "TESTDB", &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2016_MID, TS_2016_MID + 2, "TESTDB", 0, &SumRep);
    EXPECT_EQ(SumRep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 10.0) << "the zero-column row must not contribute (and must not crash the scan)";
}

TEST_F(XStoreTest, AggregateSkipsNonNumericColumnValues) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Column 0 alternates between numeric and string/bool/bytes across rows
    // -- only the numeric rows may contribute to the aggregate (the kernels
    // only accept oneOfData_case() in (0, 12], i.e. the 12 numeric branches).
    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    auto AddRow = [&](int i, const XStore_Proto::FData &f) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2016_MID + i);
        *Entry->mutable_data() = MakeRow({f});
    };
    AddRow(0, FDouble(4.0));
    AddRow(1, FString("not-a-number"));
    AddRow(2, FDouble(6.0));
    AddRow(3, FBool(true));
    AddRow(4, FBytes("also-not-a-number"));

    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2016_MID, TS_2016_MID + 4, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 10.0) << "only the two numeric rows (4.0 + 6.0) should contribute";

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2016_MID, TS_2016_MID + 4, "TESTDB", 0, &MinRep);
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), 4.0);
}

TEST_F(XStoreTest, AggregateOverAllNonNumericColumnReturnsNoValidData) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2031_MID, MakeRow({FString("only-a-string")}), "TESTDB", &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2031_MID, TS_2031_MID, "TESTDB", 0, &SumRep);
    EXPECT_EQ(SumRep.opstatus(), "ERROR: No valid data found");
}

// Every aggregate test above uses a single-column row, so columnIdx is
// trivially always 0 -- none of them actually prove the aggregate reads the
// COLUMN it was asked for rather than e.g. always column 0, and none run at
// any real scale. This uses the full 32-column/10k-row BuildWideRow fixture:
// column 0 is a numeric decoy held constant across all 10k rows (sum would
// be 1,000,000.0 if the kernels ignored columnIdx), column 2 is the real
// target with a distinct value (rowIndex) per row.
TEST_F(XStoreTest, AggregateOnSpecificColumnAmongMixedTypeColumnsIgnoresOtherColumns) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 1024);
    constexpr unsigned short TargetColumn = 2;

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (int i = 0; i < kWideRowCount; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2024_MID + i);
        *Entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    const unsigned long long Start = TS_2024_MID;
    const unsigned long long End = TS_2024_MID + kWideRowCount - 1;

    // Column 2 holds rowIndex for rowIndex in [0, kWideRowCount).
    const int64_t ExpectedMin = 0;
    const int64_t ExpectedMax = kWideRowCount - 1;
    const double ExpectedSum = (kWideRowCount - 1) * static_cast<double>(kWideRowCount) / 2.0;
    const double ExpectedAvg = ExpectedSum / kWideRowCount;

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(Start, End, "TESTDB", TargetColumn, &MinRep);
    EXPECT_EQ(MinRep.opresult().int64data(), ExpectedMin);

    XStore_Proto::Max_REP MaxRep;
    Store.MaxAggregate(Start, End, "TESTDB", TargetColumn, &MaxRep);
    EXPECT_EQ(MaxRep.opresult().int64data(), ExpectedMax);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(Start, End, "TESTDB", TargetColumn, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), ExpectedSum)
        << "must be column 2's sum, not column 0's decoy sum (1000000)";

    XStore_Proto::Avg_REP AvgRep;
    Store.AvgAggregate(Start, End, "TESTDB", TargetColumn, &AvgRep);
    EXPECT_DOUBLE_EQ(AvgRep.opresult(), ExpectedAvg);
}

// Distinct from AggregateOverAllNonNumericColumnReturnsNoValidData above:
// that row has no numeric data anywhere, so "no valid data" is ambiguous
// between "wrong column type" and "no numeric data in the row at all". Here
// -- same 32-column/10k-row BuildWideRow fixture -- columns 0 and 2 ARE
// valid numeric data, confirming the aggregate rejects based on the
// SPECIFICALLY requested column's type (column 1, always a string), not on
// whether the row has numeric data somewhere.
TEST_F(XStoreTest, AggregateTargetingNonNumericColumnAmongMixedTypeRowReturnsNoValidData) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 1024);

    XStore_Proto::RangeInsert_REQ InsertReq;
    InsertReq.set_dbname("TESTDB");
    for (int i = 0; i < kWideRowCount; i++) {
        XStore_Proto::RangeInsert_DATA *Entry = InsertReq.add_rangeinsert_data();
        Entry->set_timestamp(TS_2024_MID + i);
        *Entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP InsertRep;
    Store.RangeInsert(&InsertReq, &InsertRep);

    const unsigned long long Start = TS_2024_MID;
    const unsigned long long End = TS_2024_MID + kWideRowCount - 1;

    XStore_Proto::Sum_REP SumOnString;
    Store.SumAggregate(Start, End, "TESTDB", 1, &SumOnString);
    EXPECT_EQ(SumOnString.opstatus(), "ERROR: No valid data found");

    XStore_Proto::Min_REP MinOnString;
    Store.MinAggregate(Start, End, "TESTDB", 1, &MinOnString);
    EXPECT_EQ(MinOnString.opstatus(), "ERROR: No valid data found");

    // Sanity: column 0 (the constant 100.0 decoy) in these SAME rows
    // aggregates fine -- proves the ERROR above is really about column 1's
    // type, not the DB/range setup.
    XStore_Proto::Sum_REP SumOnCol0;
    Store.SumAggregate(Start, End, "TESTDB", 0, &SumOnCol0);
    EXPECT_EQ(SumOnCol0.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(SumOnCol0.opresult(), 100.0 * kWideRowCount);
}

TEST_F(XStoreTest, AggregateOverRangeContainingHolesOnlyReflectsPresentRows) {
    XStore Store;
    Store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    // Sparse: only positions 0, 2, 4 of a 5-wide window are written; 1 and 3
    // are holes (never inserted).
    XStore_Proto::UnaryInsert_REP InsertRep;
    Store.UnaryInsert(TS_2003_MID, MakeRow({FDouble(1.0)}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2003_MID + 2, MakeRow({FDouble(9.0)}), "TESTDB", &InsertRep);
    Store.UnaryInsert(TS_2003_MID + 4, MakeRow({FDouble(2.0)}), "TESTDB", &InsertRep);

    XStore_Proto::Sum_REP SumRep;
    Store.SumAggregate(TS_2003_MID, TS_2003_MID + 4, "TESTDB", 0, &SumRep);
    EXPECT_DOUBLE_EQ(SumRep.opresult(), 12.0);

    XStore_Proto::Min_REP MinRep;
    Store.MinAggregate(TS_2003_MID, TS_2003_MID + 4, "TESTDB", 0, &MinRep);
    EXPECT_DOUBLE_EQ(MinRep.opresult().doubledata(), 1.0);
}

TEST_F(XStoreTest, ConcurrentSumAggregateDuringConcurrentInsertsDoesNotCrashOrTear) {
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_2024_MID, MakeRow({FDouble(1.0)}), "TESTDB", &SeedRep);  // pre-allocate the year file

    constexpr int KIterations = 200;
    constexpr int KWidth = 50;

    auto Writer = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            for (int j = 0; j < KWidth; j++) {
                XStore_Proto::UnaryInsert_REP Rep;
                Store.UnaryInsert(TS_2024_MID + j, MakeRow({FDouble((double)(i + j))}), "TESTDB", &Rep);
            }
        }
    };
    auto Aggregator = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::Sum_REP Rep;
            Store.SumAggregate(TS_2024_MID, TS_2024_MID + KWidth - 1, "TESTDB", 0, &Rep);
            // No crash/hang is the primary assertion; if data is present at
            // all it must have parsed as a real numeric sum, never garbage.
            EXPECT_TRUE(Rep.opstatus() == "SUCCESS" || Rep.opstatus() == "ERROR: No valid data found");
        }
    };

    std::thread W(Writer);
    std::thread A(Aggregator);
    W.join();
    A.join();
}

// Sum/Avg go through ProcessFileRangeAccumulate (covered under concurrent
// writes above); Min/Max go through the separate ProcessFileRangeSelect
// template instead -- that path's concurrent-read safety was never
// exercised on its own.
TEST_F(XStoreTest, ConcurrentMinAggregateDuringConcurrentInsertsDoesNotCrashOrTear) {
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_2024_MID, MakeRow({FDouble(1.0)}), "TESTDB", &SeedRep);  // pre-allocate the year file

    constexpr int KIterations = 200;
    constexpr int KWidth = 50;

    auto Writer = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            for (int j = 0; j < KWidth; j++) {
                XStore_Proto::UnaryInsert_REP Rep;
                Store.UnaryInsert(TS_2024_MID + j, MakeRow({FDouble((double)(i + j))}), "TESTDB", &Rep);
            }
        }
    };
    auto Aggregator = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::Min_REP Rep;
            Store.MinAggregate(TS_2024_MID, TS_2024_MID + KWidth - 1, "TESTDB", 0, &Rep);
            // No crash/hang is the primary assertion; if data is present at
            // all it must have parsed as a real numeric value, never garbage.
            EXPECT_TRUE(Rep.opstatus() == "SUCCESS" || Rep.opstatus() == "ERROR: No valid data found");
        }
    };

    std::thread W(Writer);
    std::thread A(Aggregator);
    W.join();
    A.join();
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

TEST_F(XStoreTest, ConcurrentUnaryInsertAndUnaryQuerySameOffsetDoNotRace) {
    XStore SeedStore;
    SeedStore.CreateDB("TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP SeedRep;
    SeedStore.UnaryInsert(TS_2024_MID, MakeRow({FInt64(-1)}), "TESTDB", &SeedRep);  // pre-allocate the year file

    constexpr int KIterations = 500;
    std::atomic<bool> SawParseFailure{false};

    auto Writer = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryInsert_REP Rep;
            Store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(i)}), "TESTDB", &Rep);
        }
    };
    auto Reader = [&]() {
        XStore Store;
        for (int i = 0; i < KIterations; i++) {
            XStore_Proto::UnaryQuery_REP Rep;
            Store.UnaryQuery(TS_2024_MID, "TESTDB", &Rep);
            // The READ lock only guarantees a torn-free read against a
            // concurrent WRITE lock -- it may observe the seed value or any
            // writer iteration, but must always parse as exactly one clean field.
            if (Rep.data().fielddata_size() != 1) {
                SawParseFailure = true;
            }
        }
    };

    std::thread W(Writer);
    std::thread R(Reader);
    W.join();
    R.join();

    EXPECT_FALSE(SawParseFailure) << "query observed a torn/partial write instead of a clean single-field row";
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

    // All 16 threads race to populate the same cache entry: whichever one's
    // constructor wins the per-DataStoreDir InitDB() race populates it first,
    // and every other thread's GetOrLoadCfg() miss path races behind it. An
    // unguarded std::map under that many concurrent inserts would risk a
    // corrupted map or a crash; they must instead converge on one consistent
    // entry.
    ASSERT_EQ(XStore::CfgMap.size(), 1u);
    EXPECT_EQ(XStore::CfgMap.at("TESTDB").rowlength(), 64);
}
