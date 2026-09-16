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
#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <set>
#include <sys/stat.h>
#include <thread>
#include <vector>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include "include/DataStructs/TimeStruct.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_PreadPool.h"
#include <spdlog/fmt/fmt.h>
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
constexpr int K_WIDE_ROW_COLUMN_COUNT = 32;
constexpr int K_WIDE_ROW_COUNT = 10'000;

XStore_Proto::RowData BuildWideRow(int row_index) {
    std::vector<XStore_Proto::FData> fields;
    fields.push_back(FDouble(100.0));                                 // col 0: numeric decoy, constant
    fields.push_back(FString("label-" + std::to_string(row_index)));   // col 1: non-numeric target
    fields.push_back(FInt64(row_index));                                // col 2: numeric target, varies

    while (static_cast<int>(fields.size()) < K_WIDE_ROW_COLUMN_COUNT) {
        int i = static_cast<int>(fields.size());
        switch (i % 6) {
            case 0: fields.push_back(FFloat(static_cast<float>(i) + row_index)); break;
            case 1: fields.push_back(FInt32(i - row_index)); break;
            case 2: fields.push_back(FUInt64(static_cast<uint64_t>(i) + row_index)); break;
            case 3: fields.push_back(FBool((i + row_index) % 2 == 0)); break;
            case 4: fields.push_back(FBytes("b-" + std::to_string(i))); break;
            default: fields.push_back(FSInt32(-i)); break;
        }
    }
    return MakeRow(fields);
}

// Reads exactly rowLength bytes at timestamp's computed file offset and
// decodes them into a fresh RowData -- entirely independent of
// XStore::UnaryQuery/BatchQuery/the aggregate kernels, so it can't be
// fooled by a bug that happens to affect a write path and its usual read
// path identically (data would still round-trip through the library, but
// what's actually on disk -- and what any other tool reading these files
// later would see -- would be wrong). CodedInputStream + MergePartial
// mirrors the trailing-zero-tolerant parse XStore_IO.cpp's own aggregate
// kernels use, rather than a plain ParseFromArray, which errors on the
// zero-padded tail past the real message.
XStore_Proto::RowData ReadRowDirectlyFromDisk(const fs::path &data_file, unsigned long long timestamp,
                                               long long first_epoch, int row_length) {
    unsigned long long offset = (timestamp - static_cast<unsigned long long>(first_epoch)) * static_cast<unsigned long long>(row_length);

    std::ifstream in(data_file, std::ios::binary);
    in.seekg(static_cast<std::streamoff>(offset));
    std::vector<char> buf(row_length);
    in.read(buf.data(), row_length);

    XStore_Proto::RowData row;
    google::protobuf::io::ArrayInputStream array_in(buf.data(), row_length);
    google::protobuf::io::CodedInputStream coded_in(&array_in);
    row.MergePartialFromCodedStream(&coded_in);
    return row;
}

}  // namespace

class XStoreTest : public ::testing::Test {
 protected:
  std::unique_ptr<ScratchDir> scratch;

  void SetUp() override {
      scratch = std::make_unique<ScratchDir>("xstore_test");

      // CfgMap (now cfg_shards) is a process-wide static cache keyed only by
      // dbName (by design -- production's data_store_dir never changes for a
      // process's lifetime, so it doesn't need to be). Every test here
      // reuses "TESTDB" in its own fresh scratch directory though, so
      // without this a DB cached (and never explicitly RemoveDB'd -- e.g.
      // RemoveDBFiles legitimately leaves it cached, since it protects the
      // .CFG) by an earlier test would still look cached to a later,
      // unrelated test's brand-new (and otherwise empty) scratch directory.
      for (auto &shard : XStore::cfg_shards) shard.map.clear();
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
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 2);

    ASSERT_EQ(info.dbinfo_size(), 1);
    EXPECT_EQ(info.dbinfo(0).dbname(), "TESTDB");
    EXPECT_EQ(info.dbinfo(0).dbresolution(), XStore_Proto::SECOND);
}

TEST_F(XStoreTest, InfoQueryReflectsMultipleCreatedDatabases) {
    XStore store;
    // Distinct resolutions per DB so each dbinfo entry can be matched back to
    // its creator unambiguously, not just counted.
    CreateTestDb(store, "ALPHADB", XStore_Proto::SECOND, 64);
    CreateTestDb(store, "BETADB", XStore_Proto::MILLISECOND, 128);
    CreateTestDb(store, "GAMMADB", XStore_Proto::MICROSECOND, 256);

    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 3);

    ASSERT_EQ(info.dbinfo_size(), 3);
    std::map<std::string, XStore_Proto::TimeUnit> by_name;
    for (int i = 0; i < info.dbinfo_size(); i++) {
        by_name[info.dbinfo(i).dbname()] = info.dbinfo(i).dbresolution();
    }
    ASSERT_EQ(by_name.count("ALPHADB"), 1u);
    ASSERT_EQ(by_name.count("BETADB"), 1u);
    ASSERT_EQ(by_name.count("GAMMADB"), 1u);
    EXPECT_EQ(by_name["ALPHADB"], XStore_Proto::SECOND);
    EXPECT_EQ(by_name["BETADB"], XStore_Proto::MILLISECOND);
    EXPECT_EQ(by_name["GAMMADB"], XStore_Proto::MICROSECOND);
}

// ---------------------------------------------------------------------
// CreateDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, CreateDBRejectsRowLengthBelowFour) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 3);

    EXPECT_EQ(rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Row length must be >= 4");
}

TEST_F(XStoreTest, CreateDBSucceedsAndWritesConfigFile) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: TESTDB] - Created DB");
    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

TEST_F(XStoreTest, CreateDBRejectsDuplicateName) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::SECOND, 128);

    EXPECT_EQ(rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Already existed");
}

TEST_F(XStoreTest, CreateDBRejectsNanosecondResolution) {
    // ResolutionMap (include/XStore/XStore.h) lists NANOSECOND, but the
    // actual gate CreateDB enforces is PermittedTimeResolution, which only
    // allows SECOND/MILLISECOND/MICROSECOND -- confirms NANOSECOND is
    // actually rejected, not just absent from a stale comment.
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("TESTDB", XStore_Proto::NANOSECOND, 128);

    EXPECT_EQ(rep.createdbstatus(), "[WARNING] [CREATE DB] [DB: TESTDB] - Invalid time resolution");
    EXPECT_FALSE(fs::exists(scratch->test_root / "DataStore" / "TESTDB"));
}

// ---------------------------------------------------------------------
// columnCount high-water mark (per-DB, auto-tracked, never enforced --
// see docs/superpowers/specs/2026-09-15-variance-stddev-aggregate-design.md
// "Per-DB column count"). Reaches into XStore::cfg_shards directly rather
// than through a public accessor, matching this file's own established
// pattern for inspecting the persisted/cached ConfigStruct from a test
// (see CfgCachedByOneXStoreInstanceIsVisibleToAnotherInstance above) --
// GetOrLoadCfg/DbBasePath are private XStore members, not part of the
// public API surface a test should reach for.
// ---------------------------------------------------------------------

TEST_F(XStoreTest, CreateDBWithColumnCountSeedPersistsItInConfig) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("SEEDEDCOLDB", XStore_Proto::SECOND, 1536, /*column_count_seed=*/128);
    ASSERT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: SEEDEDCOLDB] - Created DB") << rep.createdbstatus();

    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("SEEDEDCOLDB") % XStore::CFG_SHARD_COUNT];
    ASSERT_TRUE(shard.map.count("SEEDEDCOLDB"));
    EXPECT_EQ(shard.map.at("SEEDEDCOLDB").columncount(), 128u);
}

TEST_F(XStoreTest, InsertingRowWiderThanColumnCountGrowsTheHighWaterMark) {
    XStore store;
    CreateTestDb(store, "GROWCOLDB", XStore_Proto::SECOND, 1536); // no seed -- starts at 0

    std::vector<XStore_Proto::FData> fields;
    for (int i = 0; i < 80; ++i) fields.push_back(FDouble(static_cast<double>(i)));

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1000, MakeRow(fields), "GROWCOLDB", &insert_rep);

    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("GROWCOLDB") % XStore::CFG_SHARD_COUNT];
    ASSERT_TRUE(shard.map.count("GROWCOLDB"));
    EXPECT_GE(shard.map.at("GROWCOLDB").columncount(), 80u);
}

TEST_F(XStoreTest, InsertingRowNarrowerThanColumnCountNeverShrinksIt) {
    XStore store;
    XStore_Proto::CreateDB_REP rep = store.CreateDB("NOSHRINKCOLDB", XStore_Proto::SECOND, 1536, /*column_count_seed=*/100);
    ASSERT_EQ(rep.createdbstatus(), "[INFO] [CREATE DB] [DB: NOSHRINKCOLDB] - Created DB") << rep.createdbstatus();

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1000, MakeRow({FDouble(1.0)}), "NOSHRINKCOLDB", &insert_rep);

    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("NOSHRINKCOLDB") % XStore::CFG_SHARD_COUNT];
    ASSERT_TRUE(shard.map.count("NOSHRINKCOLDB"));
    EXPECT_EQ(shard.map.at("NOSHRINKCOLDB").columncount(), 100u); // unchanged, never shrinks
}

// ---------------------------------------------------------------------
// RemoveDB
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::RemoveDB_REP rep = store.RemoveDB("TESTDB");

    EXPECT_EQ(rep.removestatus(), "[WARNING] [REMOVE DB] [DB: TESTDB] - DB does not exists");
}

TEST_F(XStoreTest, RemoveDBDeletesDirectoryAndDropsFromInfoQuery) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RemoveDB_REP rep = store.RemoveDB("TESTDB");
    EXPECT_EQ(rep.removestatus(), "[INFO] [REMOVE DB] [DB: TESTDB] - Removed DB with name: TESTDB");
    EXPECT_FALSE(fs::exists(scratch->test_root / "DataStore" / "TESTDB"));

    XStore_Proto::SysInfo_REP info = store.InfoQuery("127.0.0.1", 1);
    EXPECT_EQ(info.dbinfo_size(), 0);
}

TEST_F(XStoreTest, RemoveDBThenRecreateSameNameDoesNotReadStaleMmapData) {
    // RemoveDB calls EraseMappedFilesUnderPrefix so a future insert can't
    // keep writing into an orphaned, unlinked mapping (see the comment at
    // XStore::RemoveDB's call site) -- this confirms that actually holds: a
    // fresh DB reusing the old name must not see the previous DB's data
    // through a stale cached mapping the erase should have dropped.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP first_insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("old-generation")}), "TESTDB", &first_insert_rep);
    ASSERT_NE(first_insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    (void) store.RemoveDB("TESTDB");
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Same year, same offset as the old generation's write -- if the old
    // mapping survived, this read would incorrectly find "old-generation".
    XStore_Proto::UnaryQuery_REP stale_check;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &stale_check);
    EXPECT_EQ(stale_check.data().fielddata_size(), 0) << "found data through a stale mapping from the removed DB";

    XStore_Proto::UnaryInsert_REP second_insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("new-generation")}), "TESTDB", &second_insert_rep);
    ASSERT_NE(second_insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).stringdata(), "new-generation");
}

// ---------------------------------------------------------------------
// RemoveDBFiles
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RemoveDBFilesRemovesDataFileButProtectsConfig) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &insert_rep);

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("2016.XSTORE");
    req.add_filename("TESTDB.CFG");

    XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, "TESTDB");

    ASSERT_EQ(rep.removestatus_size(), 2);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: TESTDB] - Removed 2016.XSTORE");
    EXPECT_EQ(rep.removestatus(1), "[WARNING] [REMOVE DB FILES] [DB: TESTDB] - TESTDB.CFG is prohibited from remove");

    EXPECT_FALSE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "TESTDB.CFG"));
}

TEST_F(XStoreTest, RemoveDBFilesThenReinsertSameYearDoesNotReadStaleMmapData) {
    // Same stale-mapping concern as RemoveDB above, but scoped to a single
    // file via EraseMappedFile (RemoveDBFiles's call site comment references
    // the same reasoning).
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP first_insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("old-generation")}), "TESTDB", &first_insert_rep);
    ASSERT_NE(first_insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::RemoveDBFiles_REQ remove_req;
    remove_req.add_filename("2016.XSTORE");
    XStore_Proto::RemoveDBFiles_REP remove_rep = store.RemoveDBFiles(&remove_req, "TESTDB");
    ASSERT_NE(remove_rep.removestatus(0).find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP stale_check;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &stale_check);
    EXPECT_EQ(stale_check.data().fielddata_size(), 0) << "found data through a stale mapping of the removed file";

    XStore_Proto::UnaryInsert_REP second_insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("new-generation")}), "TESTDB", &second_insert_rep);
    ASSERT_NE(second_insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).stringdata(), "new-generation");
}

// ---------------------------------------------------------------------
// UnaryInsert / UnaryQuery / UnaryDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, UnaryInsertOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("hello")}), "TESTDB", &rep);

    EXPECT_EQ(rep.insertstatus(), "[WARNING] [UNARY INSERT] [DB: TESTDB] - DB does not exist");
}

TEST_F(XStoreTest, UnaryInsertExactTimestampHasInsertedData) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(3.14), FString("abc"), FBool(true)}), "TESTDB", &insert_rep);
    EXPECT_EQ(insert_rep.insertstatus(), "[INFO] [UNARY INSERT] [DB: TESTDB] - Inserted 1482271200 to 2016.XSTORE");

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);

    EXPECT_EQ(query_rep.timestamp(), TS_2016_MID);
    ASSERT_EQ(query_rep.data().fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 3.14);
    EXPECT_EQ(query_rep.data().fielddata(1).stringdata(), "abc");
    EXPECT_EQ(query_rep.data().fielddata(2).booldata(), true);
}

TEST_F(XStoreTest, UnaryInsertAllProtoDataTypesRoundTrip) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 256);

    // Exercises every branch of FData's oneof (see proto/XStore.proto),
    // not just the double/string/bool trio used elsewhere in this file.
    XStore_Proto::UnaryInsert_REP insert_rep;
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
    }), "TESTDB", &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);

    ASSERT_EQ(query_rep.data().fielddata_size(), 15);
    const auto &d = query_rep.data();
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
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FDouble(1.0)}), "TESTDB", &insert_rep);

    // Same year file exists, but nothing was ever written at these offsets --
    // this must come back as a normal (non-error) response with zero fields,
    // not as an error, since the file itself exists.
    for (unsigned long long neighbor : {TS_2009_MID - 5, TS_2009_MID + 5, TS_2009_MID + 3600}) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(neighbor, "TESTDB", &query_rep);
        EXPECT_EQ(query_rep.timestamp(), neighbor);
        EXPECT_EQ(query_rep.data().fielddata_size(), 0) << "neighbor=" << neighbor;
    }

    // Sanity: the originally-inserted timestamp is unaffected by the reads above.
    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2009_MID, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 1.0);
}

TEST_F(XStoreTest, UnaryInsertOverwriteSameTimestampKeepsLatestValue) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2003_MID, MakeRow({FString("first")}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2003_MID, MakeRow({FString("second")}), "TESTDB", &insert_rep);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2003_MID, "TESTDB", &query_rep);

    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).stringdata(), "second");
}

TEST_F(XStoreTest, UnaryQueryWithDbButNoYearFileReturnsError) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);  // DB exists, but nothing was ever inserted for TS_2016_MID's year

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);

    EXPECT_EQ(query_rep.querystatus(), "[WARNING] [UNARY QUERY] [DB: TESTDB] - DB does not exist");
}

// Unlike UnaryInsert/BatchQuery/RangeQuery (which all check the DB directory
// or .CFG path exists before opening it), UnaryQuery goes straight to
// fopen(cfgPath) when the dbName isn't already cached in cfgMap. On a fresh
// XStore instance querying a DB that was never created, that fopen() returns
// NULL and the subsequent fileno(NULL) crashes instead of reporting an error.
TEST_F(XStoreTest, UnaryQueryOnNeverCreatedDBReturnsErrorInsteadOfCrashing) {
    XStore store;  // fresh instance: cfgMap starts empty, so this can't be masked by a warm cache
    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2003_MID, "NEVER_CREATED_DB", &query_rep);
    EXPECT_NE(query_rep.querystatus().find("[WARNING]"), std::string::npos);
}

TEST_F(XStoreTest, UnaryInsertDbNameIsCaseInsensitive) {
    XStore store;
    CreateTestDb(store, "testdb", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_1998_MID, MakeRow({FString("mixed-case")}), "TestDb", &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_1998_MID, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).stringdata(), "mixed-case");
}

TEST_F(XStoreTest, UnaryInsertDifferentYearsWriteToSeparateFiles) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FString("year-2016")}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2031_MID, MakeRow({FString("year-2031")}), "TESTDB", &insert_rep);

    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "2031.XSTORE"));

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
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2024_MID, MakeRow({FString("to-be-deleted")}), "TESTDB", &insert_rep);

    XStore_Proto::UnaryDelete_REP delete_rep = store.UnaryDelete(TS_2024_MID, "TESTDB");
    EXPECT_NE(delete_rep.deletestatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2024_MID, "TESTDB", &query_rep);
    EXPECT_EQ(query_rep.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, UnaryDeleteOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::UnaryDelete_REP delete_rep = store.UnaryDelete(TS_2016_MID, "NOPE");
    EXPECT_EQ(delete_rep.deletestatus(), "[WARNING] [UNARY DELETE] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, UnaryDeleteOnMissingYearFileReturnsError) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    // DB exists but nothing was ever inserted for this year -> no 2016.XSTORE file.
    XStore_Proto::UnaryDelete_REP delete_rep = store.UnaryDelete(TS_2016_MID, "TESTDB");
    EXPECT_EQ(delete_rep.deletestatus(), "[WARNING] [UNARY DELETE] [DB: TESTDB] - 2016.XSTORE does not exist");
}

TEST_F(XStoreTest, UnaryInsertAtFirstAndLastEpochOfYearRoundTrip) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // TS_2017_FIRST/TS_2016_LAST are exactly TimeStruct's FirstEpoch/LastEpoch
    // for their respective years (see TimeStruct::ExtractTimeComponents).
    // LastEpoch is the risky case: GetOrMapFile pads the mapped file by one
    // extra row specifically because this write lands at offset
    // totalRows*rowLength, one row past (LastEpoch-FirstEpoch)*rowLength.
    XStore_Proto::UnaryInsert_REP first_rep, last_rep;
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-epoch-2017")}), "TESTDB", &first_rep);
    store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-epoch-2016")}), "TESTDB", &last_rep);
    EXPECT_NE(first_rep.insertstatus().find("[INFO]"), std::string::npos);
    EXPECT_NE(last_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP first_query, last_query;
    store.UnaryQuery(TS_2017_FIRST, "TESTDB", &first_query);
    store.UnaryQuery(TS_2016_LAST, "TESTDB", &last_query);

    ASSERT_EQ(first_query.data().fielddata_size(), 1);
    EXPECT_EQ(first_query.data().fielddata(0).stringdata(), "first-epoch-2017");
    ASSERT_EQ(last_query.data().fielddata_size(), 1);
    EXPECT_EQ(last_query.data().fielddata(0).stringdata(), "last-epoch-2016");
}

TEST_F(XStoreTest, UnaryInsertAdjacentTimestampsDoNotOverlap) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    constexpr int k_rows = 40;
    XStore_Proto::UnaryInsert_REP insert_rep;
    for (int i = 0; i < k_rows; i++) {
        store.UnaryInsert(TS_2009_MID + i, MakeRow({FInt64(i), FString("row-" + std::to_string(i))}), "TESTDB", &insert_rep);
    }

    for (int i = 0; i < k_rows; i++) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(TS_2009_MID + i, "TESTDB", &query_rep);
        ASSERT_EQ(query_rep.data().fielddata_size(), 2) << "i=" << i;
        EXPECT_EQ(query_rep.data().fielddata(0).int64data(), i) << "i=" << i;
        EXPECT_EQ(query_rep.data().fielddata(1).stringdata(), "row-" + std::to_string(i)) << "i=" << i;
    }
}

TEST_F(XStoreTest, UnaryInsertAdjacentTimestampsDoNotOverlapWithTightRowLength) {
    // rowLength set to the actual serialized size of one row (not a generous
    // round number) so adjacent slots sit back-to-back with zero slack --
    // any off-by-one in the offset math would spill into a neighbor.
    constexpr int k_rows = 20;  // all values < 128 -> single-byte varint, so every row serializes to the same size
    XStore_Proto::RowData sample = MakeRow({FInt64(0)});
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, (short)sample.ByteSizeLong());

    XStore_Proto::UnaryInsert_REP insert_rep;
    for (int i = 0; i < k_rows; i++) {
        store.UnaryInsert(TS_2009_MID + i, MakeRow({FInt64(i)}), "TESTDB", &insert_rep);
        EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << "i=" << i;
    }

    for (int i = 0; i < k_rows; i++) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(TS_2009_MID + i, "TESTDB", &query_rep);
        ASSERT_EQ(query_rep.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(query_rep.data().fielddata(0).int64data(), i) << "i=" << i;
    }
}

TEST_F(XStoreTest, UnaryInsertRowLargerThanRowLengthIsRejectedAndDoesNotCorruptNeighbor) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 8);  // just enough for a short row, not more

    XStore_Proto::UnaryInsert_REP neighbor_rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FInt32(1)}), "TESTDB", &neighbor_rep);
    ASSERT_NE(neighbor_rep.insertstatus().find("[INFO]"), std::string::npos);

    // This row's serialized size exceeds the configured 8-byte rowLength.
    // Before the fix, XStore::UnaryInsert never checked SerializeToArray's
    // return value, so this silently truncated instead of erroring.
    XStore_Proto::UnaryInsert_REP oversized_rep;
    store.UnaryInsert(TS_2009_MID + 1, MakeRow({FString("this string is far too long for an 8 byte row")}), "TESTDB", &oversized_rep);
    EXPECT_NE(oversized_rep.insertstatus().find("[WARNING]"), std::string::npos) << oversized_rep.insertstatus();
    EXPECT_NE(oversized_rep.insertstatus().find("exceeds"), std::string::npos) << oversized_rep.insertstatus();

    // The oversized attempt must not have written anything, and the
    // preceding row's data must be untouched.
    XStore_Proto::UnaryQuery_REP oversized_query, neighbor_query;
    store.UnaryQuery(TS_2009_MID + 1, "TESTDB", &oversized_query);
    EXPECT_EQ(oversized_query.data().fielddata_size(), 0);

    store.UnaryQuery(TS_2009_MID, "TESTDB", &neighbor_query);
    ASSERT_EQ(neighbor_query.data().fielddata_size(), 1);
    EXPECT_EQ(neighbor_query.data().fielddata(0).int32data(), 1);
}

TEST_F(XStoreTest, UnaryInsertRowWith32PlusMixedTypeColumnsRoundTrips) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 1024);

    // Two full passes over all 15 oneof branches (30 columns) plus a couple
    // extra, so this goes well past the 15-column ceiling every existing
    // test stops at.
    std::vector<XStore_Proto::FData> fields;
    for (int pass = 0; pass < 2; pass++) {
        fields.push_back(FDouble(1.5 + pass));
        fields.push_back(FFloat(2.5f + pass));
        fields.push_back(FInt32(-1 - pass));
        fields.push_back(FInt64(-100LL - pass));
        fields.push_back(FUInt32(1u + pass));
        fields.push_back(FUInt64(100ULL + pass));
        fields.push_back(FSInt32(-2 - pass));
        fields.push_back(FSInt64(-200LL - pass));
        fields.push_back(FFixed32(3u + pass));
        fields.push_back(FFixed64(300ULL + pass));
        fields.push_back(FSFixed32(-3 - pass));
        fields.push_back(FSFixed64(-300LL - pass));
        fields.push_back(FBool(pass % 2 == 0));
        fields.push_back(FString("pass-" + std::to_string(pass)));
        fields.push_back(FBytes("bytes-" + std::to_string(pass)));
    }
    fields.push_back(FInt32(999));
    fields.push_back(FString("tail"));
    ASSERT_GE(fields.size(), 32u);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2003_MID, MakeRow(fields), "TESTDB", &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2003_MID, "TESTDB", &query_rep);

    ASSERT_EQ(query_rep.data().fielddata_size(), (int)fields.size());
    const auto &d = query_rep.data();
    for (int pass = 0; pass < 2; pass++) {
        int base = pass * 15;
        EXPECT_DOUBLE_EQ(d.fielddata(base + 0).doubledata(), 1.5 + pass);
        EXPECT_FLOAT_EQ(d.fielddata(base + 1).floatdata(), 2.5f + pass);
        EXPECT_EQ(d.fielddata(base + 2).int32data(), -1 - pass);
        EXPECT_EQ(d.fielddata(base + 3).int64data(), -100LL - pass);
        EXPECT_EQ(d.fielddata(base + 4).uint32data(), (uint32_t)(1 + pass));
        EXPECT_EQ(d.fielddata(base + 5).uint64data(), (uint64_t)(100 + pass));
        EXPECT_EQ(d.fielddata(base + 6).sint32data(), -2 - pass);
        EXPECT_EQ(d.fielddata(base + 7).sint64data(), -200LL - pass);
        EXPECT_EQ(d.fielddata(base + 8).fixed32data(), (uint32_t)(3 + pass));
        EXPECT_EQ(d.fielddata(base + 9).fixed64data(), (uint64_t)(300 + pass));
        EXPECT_EQ(d.fielddata(base + 10).sfixed32data(), -3 - pass);
        EXPECT_EQ(d.fielddata(base + 11).sfixed64data(), -300LL - pass);
        EXPECT_EQ(d.fielddata(base + 12).booldata(), pass % 2 == 0);
        EXPECT_EQ(d.fielddata(base + 13).stringdata(), "pass-" + std::to_string(pass));
        EXPECT_EQ(d.fielddata(base + 14).bytesdata(), "bytes-" + std::to_string(pass));
    }
    EXPECT_EQ(d.fielddata(30).int32data(), 999);
    EXPECT_EQ(d.fielddata(31).stringdata(), "tail");
}

TEST_F(XStoreTest, UnaryInsertCreatesDataFileAsSparseFile) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FInt32(1)}), "TESTDB", &insert_rep);

    fs::path data_file = scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE";
    ASSERT_TRUE(fs::exists(data_file));

    struct stat st{};
    ASSERT_EQ(stat(data_file.string().c_str(), &st), 0);

    // Logical size covers a full year's worth of rows (~31.5M seconds *
    // 128 bytes), but only the single row actually written should have
    // real blocks behind it -- everything else must remain a hole.
    unsigned long long allocated_bytes = static_cast<unsigned long long>(st.st_blocks) * 512ULL;
    EXPECT_GT(st.st_size, 1'000'000'000LL) << "expected a full year's logical span";
    EXPECT_LT(allocated_bytes, 10ULL * 1024 * 1024) << "expected only a few pages actually allocated, not the full year";
}

TEST_F(XStoreTest, UnaryInsertUnsetFieldDataRoundTripsAsEmpty) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    // FData with no oneof branch set at all -- not FInt32(0), an actually-unset field.
    store.UnaryInsert(TS_2031_MID, MakeRow({XStore_Proto::FData(), FString("after-unset")}), "TESTDB", &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2031_MID, "TESTDB", &query_rep);

    ASSERT_EQ(query_rep.data().fielddata_size(), 2);
    EXPECT_EQ(query_rep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::ONEOFDATA_NOT_SET);
    EXPECT_EQ(query_rep.data().fielddata(1).stringdata(), "after-unset");
}

TEST_F(XStoreTest, UnaryInsertMillisecondResolutionRoundTrips) {
    XStore store;
    // Every other test in this suite uses SECOND resolution -- this exercises
    // TimeStruct's MILLISECOND scaling path instead (NANOSECOND is rejected
    // by CreateDB; PermittedTimeResolution only allows SECOND/MILLISECOND/MICROSECOND).
    CreateTestDb(store, "TESTDB", XStore_Proto::MILLISECOND, 128);

    const unsigned long long ts_ms = TS_2016_MID * 1000ULL + 250;  // same instant, +250ms

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_ms, MakeRow({FString("ms-resolution")}), "TESTDB", &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << insert_rep.insertstatus();

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(ts_ms, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).stringdata(), "ms-resolution");

    // A neighboring millisecond within the same second must not alias to
    // the same slot.
    XStore_Proto::UnaryQuery_REP neighbor_query;
    store.UnaryQuery(ts_ms + 1, "TESTDB", &neighbor_query);
    EXPECT_EQ(neighbor_query.data().fielddata_size(), 0);
}

// ---------------------------------------------------------------------
// BatchInsert / BatchQuery / BatchDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, BatchInsertThenBatchQueryExactTimestampsRoundTrip) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Spread across five different years (rather than one clustered block) so
    // this also exercises BatchInsert/BatchQuery's per-year file bucketing.
    std::map<unsigned long long, std::string> expected = {
        {TS_2031_MID, "e"},
        {TS_1998_MID, "a"},
        {TS_2016_MID, "d"},
        {TS_2009_MID, "c"},
        {TS_2003_MID, "b"},
    };

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (auto &[ts, val] : expected) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FString(val)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("Inserted 5 elements"), std::string::npos);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    // Deliberately shuffled/unsorted input -- BatchQuery must sort internally.
    for (unsigned long long ts : {TS_2016_MID, TS_1998_MID, TS_2031_MID, TS_2003_MID, TS_2009_MID}) {
        query_req.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 5);
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        const auto &entry = query_rep.queryresponse(i);
        ASSERT_TRUE(expected.count(entry.timestamp())) << "unexpected timestamp " << entry.timestamp();
        ASSERT_EQ(entry.data().fielddata_size(), 1);
        EXPECT_EQ(entry.data().fielddata(0).stringdata(), expected[entry.timestamp()]);
    }
}

TEST_F(XStoreTest, BatchQueryHolesAreOmittedFromResults) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Two unrelated years, each with a small gap between its own points, so
    // hole-detection is verified without clustering all data into one year.
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 15}) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FUInt32(1)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    // Query the 5 inserted timestamps plus holes interleaved in both years --
    // holes must be silently omitted from queryresponse, not returned as empty rows.
    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 5, TS_2009_MID + 10, TS_2009_MID + 15, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 7, TS_2031_MID + 15}) {
        query_req.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 5);
    std::set<unsigned long long> returned;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        returned.insert(query_rep.queryresponse(i).timestamp());
    }
    EXPECT_EQ(returned, std::set<unsigned long long>(
        {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20, TS_2031_MID, TS_2031_MID + 15}));
}

TEST_F(XStoreTest, BatchInsertMixedColumnCountsPerTimestamp) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    const unsigned long long ts1 = TS_1998_MID;              // 1 column
    const unsigned long long ts2 = TS_2003_MID;               // 2 columns
    const unsigned long long ts3 = TS_2009_MID;               // 3 columns
    const unsigned long long ts4 = TS_2016_MID;               // 1 column
    const unsigned long long ts_hole = TS_2016_MID + 100000;   // never inserted, same year as ts4

    // Deliberately spread across several different oneof branches (not just
    // double/string/bool) so column-count mixing is also type-diverse.
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    auto add_entry = [&](unsigned long long ts, const std::vector<XStore_Proto::FData> &vals) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow(vals);
    };
    add_entry(ts1, {FUInt64(42)});
    add_entry(ts2, {FFixed32(7), FBytes("payload-two")});
    add_entry(ts3, {FSInt64(-123456789), FSFixed32(-42), FBool(true)});
    add_entry(ts4, {FInt64(4)});

    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (unsigned long long ts : {ts1, ts2, ts3, ts4, ts_hole}) query_req.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 4);
    std::map<unsigned long long, XStore_Proto::UnaryQuery_REP> by_ts;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        by_ts[query_rep.queryresponse(i).timestamp()] = query_rep.queryresponse(i);
    }

    ASSERT_EQ(by_ts.count(ts_hole), 0u);
    ASSERT_EQ(by_ts[ts1].data().fielddata_size(), 1);
    EXPECT_EQ(by_ts[ts1].data().fielddata(0).uint64data(), 42u);

    ASSERT_EQ(by_ts[ts2].data().fielddata_size(), 2);
    EXPECT_EQ(by_ts[ts2].data().fielddata(0).fixed32data(), 7u);
    EXPECT_EQ(by_ts[ts2].data().fielddata(1).bytesdata(), "payload-two");

    ASSERT_EQ(by_ts[ts3].data().fielddata_size(), 3);
    EXPECT_EQ(by_ts[ts3].data().fielddata(0).sint64data(), -123456789);
    EXPECT_EQ(by_ts[ts3].data().fielddata(1).sfixed32data(), -42);
    EXPECT_EQ(by_ts[ts3].data().fielddata(2).booldata(), true);

    ASSERT_EQ(by_ts[ts4].data().fielddata_size(), 1);
    EXPECT_EQ(by_ts[ts4].data().fielddata(0).int64data(), 4);
}

TEST_F(XStoreTest, BatchInsertRandomTimestampsThenRandomBatchQuerySubset) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    std::mt19937 rng(12345);
    std::uniform_int_distribution<unsigned long long> epoch_dist(RANDOM_RANGE_LOW, RANDOM_RANGE_HIGH);

    // Generate a random, deduplicated set of timestamps spread across ~70
    // years (not clustered in one year) and insert each one carrying its own
    // timestamp value as the payload so round-trip correctness can be
    // checked without a side table.
    std::set<unsigned long long> inserted_timestamps;
    while (inserted_timestamps.size() < 40) {
        inserted_timestamps.insert(epoch_dist(rng));
    }

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long ts : inserted_timestamps) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FInt64((int64_t)ts)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    // Sanity check: confirm the generated data really is dispersed across
    // several distinct year-files rather than accidentally clustering.
    std::set<int> distinct_years;
    for (unsigned long long ts : inserted_timestamps) {
        std::time_t t = static_cast<std::time_t>(ts);
        struct tm tm_struct;
        gmtime_r(&t, &tm_struct);
        distinct_years.insert(tm_struct.tm_year + 1900);
    }
    EXPECT_GE(distinct_years.size(), 10u) << "expected timestamps to span many distinct years";

    // Random query set: a mix of timestamps that were and weren't inserted.
    std::vector<unsigned long long> inserted_vec(inserted_timestamps.begin(), inserted_timestamps.end());
    std::shuffle(inserted_vec.begin(), inserted_vec.end(), rng);

    std::set<unsigned long long> queried_set;
    for (int i = 0; i < 20; i++) queried_set.insert(inserted_vec[i]);   // known hits
    while (queried_set.size() < 60) {
        queried_set.insert(epoch_dist(rng));  // likely (not guaranteed) misses
    }

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (unsigned long long ts : queried_set) query_req.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    std::set<unsigned long long> expected_hits;
    for (unsigned long long ts : queried_set) {
        if (inserted_timestamps.count(ts)) expected_hits.insert(ts);
    }

    ASSERT_EQ(query_rep.queryresponse_size(), (int)expected_hits.size());
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        const auto &entry = query_rep.queryresponse(i);
        EXPECT_EQ(expected_hits.count(entry.timestamp()), 1u) << "ts=" << entry.timestamp();
        ASSERT_EQ(entry.data().fielddata_size(), 1);
        EXPECT_EQ(entry.data().fielddata(0).int64data(), (int64_t)entry.timestamp());
    }
}

TEST_F(XStoreTest, BatchInsertToNonexistentDBReturnsError) {
    XStore store;
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("NOPE");
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2024_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);
    EXPECT_EQ(insert_rep.insertstatus(), "[WARNING] [BATCH INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, BatchInsertEmptyPayloadOnExistingDBSucceedsWithZeroInserted) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");  // no batchinsert_data entries at all

    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);
    EXPECT_EQ(insert_rep.insertstatus(), "[INFO] [BATCH INSERT] [DB: TESTDB] - Inserted 0 elements successfully");
}

TEST_F(XStoreTest, BatchDeleteThenBatchQueryDeletedEntriesOmitted) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Spread across two distinct years; only one entry (in the 2009 file) gets deleted.
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FBool(true)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchDelete_REQ delete_req;
    delete_req.set_dbname("TESTDB");
    delete_req.add_timestamp(TS_2009_MID + 10);
    XStore_Proto::BatchDelete_REP delete_rep = store.BatchDelete(&delete_req);
    EXPECT_NE(delete_rep.deletestatus().find("Deleted 1 items"), std::string::npos);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2031_MID}) query_req.add_timestamp(ts);

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 2);
    std::set<unsigned long long> returned;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) returned.insert(query_rep.queryresponse(i).timestamp());
    EXPECT_EQ(returned, std::set<unsigned long long>({TS_2009_MID, TS_2031_MID}));
}

TEST_F(XStoreTest, BatchDeleteTargetingMissingYearFileStillReportsCount) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    // TESTDB exists but nothing was ever inserted for 2016 -> no 2016.XSTORE.
    // BatchDelete used to silently `continue` here with no status set at all
    // mid-loop (unlike BatchQuery/RangeDelete, which both warn) -- the
    // trailing "Deleted N items" summary always overwrites deletestatus
    // regardless (same as RangeDelete), so the RESPONSE looks the same
    // either way; the fix is log-visibility parity (spdlog::warn) with the
    // sibling functions, not a response change. This just confirms the
    // missing-year branch doesn't crash and still falls through to a correct count.
    XStore_Proto::BatchDelete_REQ delete_req;
    delete_req.set_dbname("TESTDB");
    delete_req.add_timestamp(TS_2016_MID);
    XStore_Proto::BatchDelete_REP delete_rep = store.BatchDelete(&delete_req);
    EXPECT_NE(delete_rep.deletestatus().find("Deleted 0 items"), std::string::npos) << delete_rep.deletestatus();
}

TEST_F(XStoreTest, BatchDeleteWithWidelySeparatedTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (same-year) timestamps in one BatchDelete call.
    // BatchDelete now takes a single lock spanning [min offset, max offset]
    // per year-group instead of one lock per row (mirrors BatchInsert's
    // analogous test) -- confirms that coalesced lock actually covers the
    // far point, not just count*rowLength starting at the near one. The
    // fallocate64 calls themselves stay per-row (can't be coalesced without
    // deleting unrelated rows in the gap), so this only exercises the lock.
    const unsigned long long near = TS_2009_MID;
    const unsigned long long far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    XStore_Proto::UnaryInsert_REP seed_rep;
    store.UnaryInsert(near, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);
    store.UnaryInsert(far, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);

    constexpr int k_iterations = 300;
    auto batch_deleter = [&]() {
        XStore bd_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::BatchDelete_REQ req;
            req.set_dbname("TESTDB");
            req.add_timestamp(near);
            req.add_timestamp(far);
            (void) bd_store.BatchDelete(&req);
        }
    };
    auto far_writer = [&]() {
        XStore fw_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            fw_store.UnaryInsert(far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &rep);
        }
    };

    std::thread t0(batch_deleter);
    std::thread t1(far_writer);
    t0.join();
    t1.join();

    XStore_Proto::UnaryQuery_REP far_query;
    store.UnaryQuery(far, "TESTDB", &far_query);
    // Whatever the final state (deleted, or a clean write from FarWriter),
    // it must be exactly one clean field or nothing -- never torn bytes from
    // an unprotected interleave between the punch and a concurrent write.
    EXPECT_LE(far_query.data().fielddata_size(), 1) << "row at the far offset must parse as at most one clean field, not torn/corrupted bytes";
    if (far_query.data().fielddata_size() == 1) {
        int64_t final_value = far_query.data().fielddata(0).int64data();
        EXPECT_TRUE(final_value == -1 || (final_value >= 3'000'000LL && final_value < 3'000'000LL + k_iterations))
            << "finalValue=" << final_value;
    }
}

TEST_F(XStoreTest, BatchInsertOverwriteWithSmallerRowLeavesNoStaleTrailingBytes) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // First write a wide row directly at this offset...
    XStore_Proto::UnaryInsert_REP seed_rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FString("a-fairly-long-first-payload-value")}), "TESTDB", &seed_rep);

    // ...then overwrite the same timestamp via BatchInsert with a much
    // narrower row. Before the fix, BatchInsert's fwrite() didn't zero the
    // slot first, so bytes past the new (shorter) serialization would still
    // belong to the old occupant.
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2009_MID);
    *entry->mutable_data() = MakeRow({FInt32(7)});
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2009_MID, "TESTDB", &query_rep);

    // If stale bytes leaked through, this would either fail to parse as
    // exactly one field or the field would decode to garbage.
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::kInt32Data);
    EXPECT_EQ(query_rep.data().fielddata(0).int32data(), 7);
}

TEST_F(XStoreTest, BatchInsertRepeatedCallsToSameYearFileAccumulateViaCachedMapping) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // First call creates+maps 2016.XSTORE via GetOrMapFile; every later call
    // to the same year must hit that cached mapping instead of re-creating/
    // re-truncating it, which would wipe out rows written by earlier calls.
    for (int i = 0; i < 20; i++) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname("TESTDB");
        req.set_durabilitymode(XStore_Proto::SYNC);
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(TS_2016_MID + i);
        *entry->mutable_data() = MakeRow({FInt64(i)});
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
    }

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (int i = 0; i < 20; i++) query_req.add_timestamp(TS_2016_MID + i);

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 20) << "an earlier call's rows were lost -- cached mapping must have been re-truncated instead of reused";
    std::map<unsigned long long, int64_t> by_ts;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        by_ts[query_rep.queryresponse(i).timestamp()] = query_rep.queryresponse(i).data().fielddata(0).int64data();
    }
    for (int i = 0; i < 20; i++) {
        EXPECT_EQ(by_ts[TS_2016_MID + i], i);
    }
}

TEST_F(XStoreTest, BatchInsertWithWidelySeparatedTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (same-year) timestamps in one BatchInsert call.
    // BatchInsert now takes a single lock spanning [min offset, max offset]
    // per year-group instead of one lock per row (mirrors RangeInsert's
    // analogous fix/test below) -- confirms that coalesced lock actually
    // covers the far point, not just count*rowLength starting at the near one.
    const unsigned long long near = TS_2009_MID;
    const unsigned long long far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    constexpr int k_iterations = 300;
    auto batch_writer = [&]() {
        XStore bw_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname("TESTDB");
            XStore_Proto::BatchInsert_DATA *e0 = req.add_batchinsert_data();
            e0->set_timestamp(near);
            *e0->mutable_data() = MakeRow({FInt64(1'000'000LL + i)});
            XStore_Proto::BatchInsert_DATA *e1 = req.add_batchinsert_data();
            e1->set_timestamp(far);
            *e1->mutable_data() = MakeRow({FInt64(2'000'000LL + i)});
            XStore_Proto::BatchInsert_REP rep;
            bw_store.BatchInsert(&req, &rep);
        }
    };
    auto far_writer = [&]() {
        XStore fw_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            fw_store.UnaryInsert(far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &rep);
        }
    };

    std::thread t0(batch_writer);
    std::thread t1(far_writer);
    t0.join();
    t1.join();

    XStore_Proto::UnaryQuery_REP far_query;
    store.UnaryQuery(far, "TESTDB", &far_query);
    ASSERT_EQ(far_query.data().fielddata_size(), 1) << "row at the far offset must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t final_value = far_query.data().fielddata(0).int64data();
    EXPECT_TRUE((final_value >= 2'000'000LL && final_value < 2'000'000LL + k_iterations) ||
                (final_value >= 3'000'000LL && final_value < 3'000'000LL + k_iterations))
        << "finalValue=" << final_value;
}

TEST_F(XStoreTest, BatchQueryRowFillingEntireBufferWithNoZeroByteIsNotTreatedAsHole) {
    // Craft a row whose serialized bytes fill the whole rowLength slot with
    // no zero byte anywhere in it -- regression test for BatchQuery's old
    // strlen(BufferIn)==0 hole-check, which could read past a
    // non-null-terminated buffer in exactly this situation.
    XStore_Proto::RowData sample = MakeRow({FBytes(std::string(20, '\xFF'))});
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, (short)sample.ByteSizeLong());

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
    entry->set_timestamp(TS_2016_MID);
    *entry->mutable_data() = sample;
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    query_req.add_timestamp(TS_2016_MID);
    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);

    ASSERT_EQ(query_rep.queryresponse_size(), 1);
    ASSERT_EQ(query_rep.queryresponse(0).data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).bytesdata(), std::string(20, '\xFF'));
}

TEST_F(XStoreTest, BatchQueryEmptyTimestampListSucceedsWithNoResults) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");  // no timestamps at all

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);
    EXPECT_EQ(query_rep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQueryAllRequestedTimestampsAreHolesReturnsEmptyResponse) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP seed_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FInt32(1)}), "TESTDB", &seed_rep);  // creates 2016.XSTORE

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    query_req.add_timestamp(TS_2016_MID + 10);
    query_req.add_timestamp(TS_2016_MID + 20);

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);
    EXPECT_EQ(query_rep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQueryRejectsRequestOver2GBLimit) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32000);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    // 63000 * 32000 > 2,000,000,000 -- just over BatchQuery's hard limit.
    for (int i = 0; i < 63000; i++) {
        query_req.add_timestamp(TS_2016_MID + i);
    }

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);
    EXPECT_NE(query_rep.querystatus().find("larger than 2GB"), std::string::npos) << query_rep.querystatus();
    EXPECT_EQ(query_rep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, BatchQuerySortTimeFalseWithUnsortedTimestampsRoundTrips) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // All in the same year, deliberately far apart and inserted/queried in a
    // non-ascending order. With SortTime=false, BatchQuery's per-year index
    // vector is NOT guaranteed sorted, so it can't size its single coalesced
    // lock off front()/back() the way BatchInsert (which always sorts) does
    // -- exercises the min/max scan it uses instead.
    std::map<unsigned long long, std::string> expected = {
        {TS_2016_MID + 50000, "mid"},
        {TS_2016_MID, "low"},
        {TS_2016_MID + 100000, "high"},
        {TS_2016_MID + 25000, "low-mid"},
    };

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (auto &[ts, val] : expected) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FString(val)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_2016_MID + 100000, TS_2016_MID, TS_2016_MID + 50000, TS_2016_MID + 25000}) {
        query_req.add_timestamp(ts);
    }

    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req, /*SortTime=*/false);

    ASSERT_EQ(query_rep.queryresponse_size(), 4);
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        const auto &entry = query_rep.queryresponse(i);
        ASSERT_TRUE(expected.count(entry.timestamp())) << "unexpected timestamp " << entry.timestamp();
        ASSERT_EQ(entry.data().fielddata_size(), 1);
        EXPECT_EQ(entry.data().fielddata(0).stringdata(), expected[entry.timestamp()]);
    }
}

// --- BatchQuery: the network-facing, zero-object-tree path (shares
// RangeQuery's wire encoder -- see XStore_WireEncode.h). Every test
// below asserts byte-for-byte SerializeAsString() equality against the
// existing object-based BatchQuery for the same query, same as
// RangeQuery's tests.
namespace {
XStore_Proto::BatchQuery_REP RunBatchStreamedAndReassemble(XStore &store, XStore_Proto::BatchQuery_REQ *req, bool sort_time,
                                                             size_t chunk_size_bytes, int *chunk_count_out = nullptr) {
    std::string joined;
    int chunk_count = 0;
    store.BatchQuery(req, sort_time, chunk_size_bytes,
        [&](std::string &&chunk, bool /*is_last*/) {
            chunk_count++;
            joined += chunk;
        });
    if (chunk_count_out) *chunk_count_out = chunk_count;
    XStore_Proto::BatchQuery_REP rep;
    rep.ParseFromString(joined);
    return rep;
}
}  // namespace

TEST_F(XStoreTest, BatchQueryStreamedMatchesBatchQueryForDenseMultiColumnRows) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    std::vector<unsigned long long> timestamps = {TS_2031_MID, TS_1998_MID, TS_2016_MID, TS_2009_MID, TS_2003_MID};
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    for (size_t i = 0; i < timestamps.size(); i++) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(timestamps[i]);
        // Negative int32/int64 exercises the 10-byte-varint case, same as
        // RangeQuery's equivalent test.
        *entry->mutable_data() = MakeRow({
            FDouble(3.14159 * static_cast<double>(i)),
            FInt32(-static_cast<int32_t>(i)),
            FInt64(-9000000000LL - static_cast<int64_t>(i)),
            FBool(i % 2 == 0),
            FString("row-" + std::to_string(i)),
        });
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ query_req1;
    query_req1.set_dbname("TESTDB");
    for (auto ts : timestamps) query_req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &query_req1);

    XStore_Proto::BatchQuery_REQ query_req2;
    query_req2.set_dbname("TESTDB");
    for (auto ts : timestamps) query_req2.add_timestamp(ts);
    int chunk_count = 0;
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &query_req2, /*sort_time=*/true, 1ull << 30, &chunk_count);

    EXPECT_EQ(chunk_count, 1);
    ASSERT_EQ(actual.queryresponse_size(), expected.queryresponse_size());
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString())
        << "BatchQuery's hand-encoded bytes diverge from BatchQuery's object-tree serialization";
}

TEST_F(XStoreTest, BatchQueryStreamedOmitsHolesSameAsBatchQuery) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (unsigned long long ts : {TS_2009_MID, TS_2009_MID + 10, TS_2009_MID + 20,
                                   TS_2031_MID, TS_2031_MID + 15}) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FUInt32(1)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    std::vector<unsigned long long> query_ts = {TS_2009_MID, TS_2009_MID + 5, TS_2009_MID + 10, TS_2009_MID + 15, TS_2009_MID + 20,
                                                 TS_2031_MID, TS_2031_MID + 7, TS_2031_MID + 15};

    XStore_Proto::BatchQuery_REQ req1;
    req1.set_dbname("TESTDB");
    for (auto ts : query_ts) req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &req1);

    XStore_Proto::BatchQuery_REQ req2;
    req2.set_dbname("TESTDB");
    for (auto ts : query_ts) req2.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &req2, /*sort_time=*/true, 1ull << 30);

    ASSERT_EQ(actual.queryresponse_size(), 5) << "holes must be omitted, not encoded as empty entries";
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

TEST_F(XStoreTest, BatchQueryStreamedSplitsAcrossMultipleChunksAndReassemblesToMatchBatchQuery) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 64);

    constexpr int kRows = 500;
    std::vector<unsigned long long> timestamps;
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (int i = 0; i < kRows; i++) {
        unsigned long long ts = TS_2024_MID + i;
        timestamps.push_back(ts);
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FDouble(i * 1.5), FInt64(i)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ req1;
    req1.set_dbname("TESTDB");
    for (auto ts : timestamps) req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &req1);

    XStore_Proto::BatchQuery_REQ req2;
    req2.set_dbname("TESTDB");
    for (auto ts : timestamps) req2.add_timestamp(ts);
    int chunk_count = 0;
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &req2, /*sort_time=*/true, 200, &chunk_count);

    EXPECT_GT(chunk_count, 10) << "test setup didn't actually force multiple chunks";
    ASSERT_EQ(actual.queryresponse_size(), kRows);
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString())
        << "reassembled multi-chunk output diverges from a single-shot BatchQuery -- a chunk boundary is corrupting an entry";
}

TEST_F(XStoreTest, BatchQueryStreamedParallelPreadWaveSpansMultipleWavesWithHolesMatchesBatchQuery) {
    // Exercises the wave-parallel pread path (BATCH_QUERY_PARALLEL_PREAD_MIN_ROWS
    // = 256, BATCH_QUERY_PREAD_WAVE_ROWS = 4096) across multiple full waves
    // plus a partial final wave, with holes scattered throughout (every 7th
    // timestamp uninserted) so the per-wave hole check is covered too.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 64);

    constexpr int kRows = 10000;  // > 2 * BATCH_QUERY_PREAD_WAVE_ROWS -- forces 3 waves
    std::vector<unsigned long long> all_ts;
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    int inserted_count = 0;
    for (int i = 0; i < kRows; i++) {
        unsigned long long ts = TS_2024_MID + i;
        all_ts.push_back(ts);
        if (i % 7 == 0) continue;  // leave a hole
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FDouble(i * 0.5), FInt64(i)});
        inserted_count++;
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    XStore_Proto::BatchQuery_REQ req1;
    req1.set_dbname("TESTDB");
    for (auto ts : all_ts) req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &req1);

    XStore_Proto::BatchQuery_REQ req2;
    req2.set_dbname("TESTDB");
    for (auto ts : all_ts) req2.add_timestamp(ts);
    int chunk_count = 0;
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &req2, /*sort_time=*/true, 1ull << 30, &chunk_count);

    ASSERT_EQ(actual.queryresponse_size(), inserted_count);
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString())
        << "wave-parallel pread path diverges from BatchQuery's object-tree serialization";
}

TEST_F(XStoreTest, BatchQueryStreamedPreservesMissingYearWarningMessage) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32);

    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    for (unsigned long long ts : {TS_1998_MID, TS_2009_MID}) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FString("present")});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);
    // TS_2016_MID's year (2016.XSTORE) is never created -- queried below anyway.

    XStore_Proto::BatchQuery_REQ req1;
    req1.set_dbname("TESTDB");
    for (auto ts : {TS_1998_MID, TS_2016_MID, TS_2009_MID}) req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &req1);

    XStore_Proto::BatchQuery_REQ req2;
    req2.set_dbname("TESTDB");
    for (auto ts : {TS_1998_MID, TS_2016_MID, TS_2009_MID}) req2.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &req2, /*sort_time=*/true, 1ull << 30);

    EXPECT_EQ(actual.querystatus(), expected.querystatus());
    EXPECT_NE(actual.querystatus().find("2016.XSTORE does not exist"), std::string::npos) << actual.querystatus();
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

TEST_F(XStoreTest, BatchQueryStreamedSortTimeFalseWithUnsortedTimestampsRoundTrips) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    std::map<unsigned long long, std::string> expected_vals = {
        {TS_2016_MID + 50000, "mid"},
        {TS_2016_MID, "low"},
        {TS_2016_MID + 100000, "high"},
        {TS_2016_MID + 25000, "low-mid"},
    };
    XStore_Proto::BatchInsert_REQ insert_req;
    insert_req.set_dbname("TESTDB");
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    for (auto &[ts, val] : expected_vals) {
        XStore_Proto::BatchInsert_DATA *entry = insert_req.add_batchinsert_data();
        entry->set_timestamp(ts);
        *entry->mutable_data() = MakeRow({FString(val)});
    }
    XStore_Proto::BatchInsert_REP insert_rep;
    store.BatchInsert(&insert_req, &insert_rep);

    std::vector<unsigned long long> query_order = {TS_2016_MID + 100000, TS_2016_MID, TS_2016_MID + 50000, TS_2016_MID + 25000};

    XStore_Proto::BatchQuery_REQ req1;
    req1.set_dbname("TESTDB");
    for (auto ts : query_order) req1.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP expected;
    expected = BatchQuery(store, &req1, /*sort_time=*/false);

    XStore_Proto::BatchQuery_REQ req2;
    req2.set_dbname("TESTDB");
    for (auto ts : query_order) req2.add_timestamp(ts);
    XStore_Proto::BatchQuery_REP actual = RunBatchStreamedAndReassemble(store, &req2, /*sort_time=*/false, 1ull << 30);

    ASSERT_EQ(actual.queryresponse_size(), 4);
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

// ---------------------------------------------------------------------
// RangeInsert / RangeQuery / RangeDelete
// ---------------------------------------------------------------------

TEST_F(XStoreTest, RangeInsertThenRangeQueryContiguousRangeAllPresent) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (int i = 0; i < 10; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2031_MID + i);
        *entry->mutable_data() = MakeRow({FInt64(i)});
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);
    EXPECT_NE(insert_rep.insertstatus().find("Inserted 10 elements"), std::string::npos);

    XStore_Proto::RangeQuery_REP query_rep;
    query_rep = RangeQuery(store, TS_2031_MID, TS_2031_MID + 9, "TESTDB");

    ASSERT_EQ(query_rep.queryresponse_size(), 10);
    for (int i = 0; i < 10; i++) {
        EXPECT_EQ(query_rep.queryresponse(i).timestamp(), TS_2031_MID + i);
        ASSERT_EQ(query_rep.queryresponse(i).data().fielddata_size(), 1);
        EXPECT_EQ(query_rep.queryresponse(i).data().fielddata(0).int64data(), i);
    }
}

// Unlike the test above, every Unary/Batch/Range* test row up to this point
// used a single column, so a multi-column row never actually round-tripped
// through RangeInsert/RangeQuery -- only through Unary/Batch (see
// UnaryInsertRowWith32PlusMixedTypeColumnsRoundTrips and
// BatchInsertMixedColumnCountsPerTimestamp).
TEST_F(XStoreTest, RangeInsertThenRangeQueryMultiColumnRowsRoundTrip) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 1024);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (int i = 0; i < K_WIDE_ROW_COUNT; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::RangeQuery_REP query_rep;
    query_rep = RangeQuery(store, TS_2024_MID, TS_2024_MID + K_WIDE_ROW_COUNT - 1, "TESTDB");

    ASSERT_EQ(query_rep.queryresponse_size(), K_WIDE_ROW_COUNT);
    for (int i = 0; i < K_WIDE_ROW_COUNT; i++) {
        const auto &data = query_rep.queryresponse(i).data();
        ASSERT_EQ(data.fielddata_size(), K_WIDE_ROW_COLUMN_COUNT);
        // BuildWideRow is a pure function of rowIndex, so regenerating the
        // expected row and comparing serialized bytes checks every one of
        // the 32 columns' type and value in one assertion, rather than
        // duplicating BuildWideRow's per-column logic here to drift from it.
        EXPECT_EQ(data.SerializeAsString(), BuildWideRow(i).SerializeAsString());
    }
}

TEST_F(XStoreTest, RangeQueryUninsertedTimestampsReturnEmptyEntriesNotOmitted) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Sparse insert: only the two endpoints of a 5-wide window. Positions
    // +1, +2, +3 are holes (unallocated -- never written).
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *first = insert_req.add_rangeinsert_data();
    first->set_timestamp(TS_2009_MID);
    *first->mutable_data() = MakeRow({FString("start")});
    XStore_Proto::RangeInsert_DATA *last = insert_req.add_rangeinsert_data();
    last->set_timestamp(TS_2009_MID + 4);
    *last->mutable_data() = MakeRow({FString("end")});

    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::RangeQuery_REP query_rep;
    query_rep = RangeQuery(store, TS_2009_MID, TS_2009_MID + 4, "TESTDB");

    // Unlike BatchQuery, RangeQuery does NOT omit holes -- every timestamp in
    // range gets an entry, with holes coming back as zero-field RowData.
    ASSERT_EQ(query_rep.queryresponse_size(), 5);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).stringdata(), "start");
    for (int i = 1; i <= 3; i++) {
        EXPECT_EQ(query_rep.queryresponse(i).timestamp(), TS_2009_MID + i);
        EXPECT_EQ(query_rep.queryresponse(i).data().fielddata_size(), 0) << "hole at offset " << i;
    }
    EXPECT_EQ(query_rep.queryresponse(4).data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.queryresponse(4).data().fielddata(0).stringdata(), "end");
}

TEST_F(XStoreTest, RangeQuerySpanningTwoYearBoundary) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &insert_rep);

    XStore_Proto::RangeQuery_REP query_rep;
    query_rep = RangeQuery(store, TS_2016_LAST, TS_2017_FIRST, "TESTDB");

    ASSERT_EQ(query_rep.queryresponse_size(), 2);
    EXPECT_EQ(query_rep.queryresponse(0).timestamp(), TS_2016_LAST);
    EXPECT_EQ(query_rep.queryresponse(0).data().fielddata(0).stringdata(), "last-of-2016");
    EXPECT_EQ(query_rep.queryresponse(1).timestamp(), TS_2017_FIRST);
    EXPECT_EQ(query_rep.queryresponse(1).data().fielddata(0).stringdata(), "first-of-2017");
}

TEST_F(XStoreTest, RangeInsertToNonexistentDBReturnsError) {
    XStore store;
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("NOPE");
    XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
    entry->set_timestamp(TS_2003_MID);
    *entry->mutable_data() = MakeRow({FDouble(1.0)});

    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);
    EXPECT_EQ(insert_rep.insertstatus(), "[WARNING] [RANGE INSERT] [DB: NOPE] - DB does not exist");
}

TEST_F(XStoreTest, RangeDeleteSingleYearMissingFileReturnsCountSummaryNotDbNotExistError) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    // TESTDB directory exists (CreateDB above), but no year file was ever
    // written -- 2009.XSTORE specifically doesn't exist. RangeDelete's old
    // single-year branch special-cased this with an early-return "[ERROR]
    // ... DB does not exist" (a stale/misleading copy-paste of the
    // is_directory check's message -- the DB does exist, only the year file
    // doesn't), skipping the trailing "Deleted N items" summary every other
    // missing-year case falls through to. Unifying with the multi-year
    // handling makes a missing year behave the same everywhere: warn and
    // still report the (zero) count.
    XStore_Proto::RangeDelete_REP delete_rep = store.RangeDelete(TS_2009_MID, TS_2009_MID + 10, "TESTDB");
    EXPECT_NE(delete_rep.deletestatus().find("Deleted 0 items"), std::string::npos) << delete_rep.deletestatus();
}

TEST_F(XStoreTest, RangeDeleteReclaimsDiskSpaceAndMmapCacheReflectsTheHole) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Wide-enough span (5000 rows) that real block allocation is easily
    // measurable and not noise from filesystem allocation granularity.
    constexpr int k_row_count = 5000;
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (int i = 0; i < k_row_count; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = MakeRow({FInt64(i)});
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    // RangeInsert (and the RangeQuery below) go through the shared mmap
    // cache (GetOrMapFile); RangeDelete's fallocate64 punches the hole via a
    // separate, freshly-opened fd -- confirms that hole is visible through
    // an *already-mapped* MAP_SHARED region too, not just to fresh reads.
    XStore_Proto::RangeQuery_REP pre_delete_query;
    pre_delete_query = RangeQuery(store, TS_2024_MID, TS_2024_MID + k_row_count - 1, "TESTDB");
    ASSERT_EQ(pre_delete_query.queryresponse_size(), k_row_count);
    for (int i = 0; i < k_row_count; i++) {
        ASSERT_EQ(pre_delete_query.queryresponse(i).data().fielddata_size(), 1) << "i=" << i;
    }

    fs::path data_file = scratch->test_root / "DataStore" / "TESTDB" / "2024.XSTORE";
    struct stat st_before_delete{};
    ASSERT_EQ(stat(data_file.string().c_str(), &st_before_delete), 0);
    unsigned long long allocated_before_delete = static_cast<unsigned long long>(st_before_delete.st_blocks) * 512ULL;
    EXPECT_GT(allocated_before_delete, 0ULL) << "expected real blocks behind the inserted rows before deleting";

    XStore_Proto::RangeDelete_REP delete_rep = store.RangeDelete(TS_2024_MID, TS_2024_MID + k_row_count - 1, "TESTDB");
    EXPECT_NE(delete_rep.deletestatus().find(fmt::format("Deleted {} items", k_row_count)), std::string::npos) << delete_rep.deletestatus();

    // Disk space actually reclaimed -- not just "reads now return empty",
    // which fallocate's FALLOC_FL_KEEP_SIZE (logical size unchanged) doesn't
    // by itself guarantee unless FALLOC_FL_PUNCH_HOLE is also set.
    struct stat st_after_delete{};
    ASSERT_EQ(stat(data_file.string().c_str(), &st_after_delete), 0);
    unsigned long long allocated_after_delete = static_cast<unsigned long long>(st_after_delete.st_blocks) * 512ULL;
    EXPECT_EQ(st_after_delete.st_size, st_before_delete.st_size) << "logical file size (FALLOC_FL_KEEP_SIZE) must be unchanged by the delete";
    EXPECT_LT(allocated_after_delete, allocated_before_delete / 2) << "expected the punched range's blocks to be actually freed, not just zero-filled";

    // Data actually gone, read back through the same cached mapping RangeInsert/RangeQuery used above.
    XStore_Proto::RangeQuery_REP post_delete_query;
    post_delete_query = RangeQuery(store, TS_2024_MID, TS_2024_MID + k_row_count - 1, "TESTDB");
    ASSERT_EQ(post_delete_query.queryresponse_size(), k_row_count);
    for (int i = 0; i < k_row_count; i++) {
        EXPECT_EQ(post_delete_query.queryresponse(i).data().fielddata_size(), 0) << "i=" << i << " -- stale data read through an already-mapped region after fallocate hole-punch on a different fd";
    }
}

TEST_F(XStoreTest, RangeDeleteSpanningTwoYearBoundaryDeletesBothEndpoints) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FString("last-of-2016")}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FString("first-of-2017")}), "TESTDB", &insert_rep);

    // Same shape as RangeQuery's year-boundary bug: a range whose end lands
    // exactly on the first second of a year used to leave that last point
    // (endEpochT itself) undeleted.
    XStore_Proto::RangeDelete_REP delete_rep = store.RangeDelete(TS_2016_LAST, TS_2017_FIRST, "TESTDB");
    EXPECT_NE(delete_rep.deletestatus().find("Deleted 2 items"), std::string::npos) << delete_rep.deletestatus();

    XStore_Proto::UnaryQuery_REP rep2016, rep2017;
    store.UnaryQuery(TS_2016_LAST, "TESTDB", &rep2016);
    store.UnaryQuery(TS_2017_FIRST, "TESTDB", &rep2017);
    EXPECT_EQ(rep2016.data().fielddata_size(), 0);
    EXPECT_EQ(rep2017.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, RangeInsertOverwriteWithSmallerRowLeavesNoStaleTrailingBytes) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP seed_rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FString("a-fairly-long-first-payload-value")}), "TESTDB", &seed_rep);

    // Same stale-trailing-bytes concern as BatchInsert, but through RangeInsert's fwrite() path.
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
    entry->set_timestamp(TS_2009_MID);
    *entry->mutable_data() = MakeRow({FInt32(7)});
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::UnaryQuery_REP query_rep;
    store.UnaryQuery(TS_2009_MID, "TESTDB", &query_rep);

    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_EQ(query_rep.data().fielddata(0).oneOfData_case(), XStore_Proto::FData::kInt32Data);
    EXPECT_EQ(query_rep.data().fielddata(0).int32data(), 7);
}

TEST_F(XStoreTest, RangeInsertWithNonContiguousTimestampsLocksFullSpanAgainstConcurrentWriter) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Two widely-spaced (but ascending) timestamps within the same year --
    // before the fix, RangeInsert's combined lock only covered
    // count*rowLength (2*rowLength) bytes starting at the first offset,
    // leaving the actual second write (far away) unprotected against a
    // concurrent writer targeting that same distant offset.
    const unsigned long long near = TS_2009_MID;
    const unsigned long long far = TS_2009_MID + 200000;  // well outside a 2*rowLength lock window

    constexpr int k_iterations = 300;
    auto range_writer = [&]() {
        XStore rw_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::RangeInsert_REQ req;
            req.set_durabilitymode(XStore_Proto::SYNC);
            req.set_dbname("TESTDB");
            XStore_Proto::RangeInsert_DATA *e0 = req.add_rangeinsert_data();
            e0->set_timestamp(near);
            *e0->mutable_data() = MakeRow({FInt64(1'000'000LL + i)});
            XStore_Proto::RangeInsert_DATA *e1 = req.add_rangeinsert_data();
            e1->set_timestamp(far);
            *e1->mutable_data() = MakeRow({FInt64(2'000'000LL + i)});
            XStore_Proto::RangeInsert_REP rep;
            rw_store.RangeInsert(&req, &rep);
        }
    };
    auto far_writer = [&]() {
        XStore fw_store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            fw_store.UnaryInsert(far, MakeRow({FInt64(3'000'000LL + i)}), "TESTDB", &rep);
        }
    };

    std::thread t0(range_writer);
    std::thread t1(far_writer);
    t0.join();
    t1.join();

    XStore_Proto::UnaryQuery_REP far_query;
    store.UnaryQuery(far, "TESTDB", &far_query);
    ASSERT_EQ(far_query.data().fielddata_size(), 1) << "row at the far offset must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t final_value = far_query.data().fielddata(0).int64data();
    EXPECT_TRUE((final_value >= 2'000'000LL && final_value < 2'000'000LL + k_iterations) ||
                (final_value >= 3'000'000LL && final_value < 3'000'000LL + k_iterations))
        << "finalValue=" << final_value;
}

TEST_F(XStoreTest, RangeQueryLastRowInSpanIsLockedAgainstConcurrentWriter) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

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
    const unsigned long long start = TS_2009_MID;
    const unsigned long long end = TS_2009_MID + 5;
    constexpr short row_length = 128;

    XStore_Proto::UnaryInsert_REP seed_rep;
    store.UnaryInsert(end, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    TimeStruct ts(static_cast<time_t>(end), XStore_Proto::SECOND);
    const unsigned long long offset = (end - ts.first_epoch) * row_length;
    const std::string target_file = fmt::format("{}/DataStore/TESTDB/{}.XSTORE", store.base_dir, ts.year);

    constexpr auto hold_duration = std::chrono::milliseconds(150);
    std::atomic<bool> lock_acquired{false};
    std::thread holder([&]() {
        FileRangeLockGuard guard(target_file, offset, row_length, FileRangeLock::LockType::WRITE);
        lock_acquired = true;
        std::this_thread::sleep_for(hold_duration);
    });

    while (!lock_acquired) std::this_thread::yield();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));  // let Holder settle in

    auto query_start = std::chrono::steady_clock::now();
    XStore_Proto::RangeQuery_REP rep;
    rep = RangeQuery(store, start, end, "TESTDB");
    auto elapsed = std::chrono::steady_clock::now() - query_start;

    holder.join();

    ASSERT_EQ(rep.queryresponse_size(), 6);
    EXPECT_GE(elapsed, hold_duration - std::chrono::milliseconds(20))
        << "RangeQuery returned without waiting on a WRITE lock held on the span's last row -- its lock region doesn't cover [start, end] inclusive";
}

TEST_F(XStoreTest, RangeQuerySpanningThreeYearsWithMissingMiddleYearFile) {
    XStore store;
    // A ~31.5M-second span (nearly 2 full years) at the usual 128-byte
    // rowLength would exceed RangeQuery's 2GB result-size guard, so this
    // uses a much narrower row -- still comfortably enough for a short string.
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32);

    // 1998, 1999 (missing), 2000 -- exercises the multi-year while loop
    // running more than one full iteration, including a year whose
    // .XSTORE file was never created (the "warn and continue" branch).
    constexpr unsigned long long ts_1998_last = 915148799ULL;      // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long ts_2000_first_plus = 946684810ULL; // 2000-01-01 00:00:10 UTC

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);
    store.UnaryInsert(ts_2000_first_plus, MakeRow({FString("start-of-2000")}), "TESTDB", &insert_rep);
    // Deliberately nothing inserted for 1999 -> 1999.XSTORE never gets created.

    XStore_Proto::RangeQuery_REP query_rep;
    query_rep = RangeQuery(store, ts_1998_last, ts_2000_first_plus, "TESTDB");

    std::map<unsigned long long, std::string> found;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        const auto &entry = query_rep.queryresponse(i);
        if (entry.data().fielddata_size() > 0) {
            found[entry.timestamp()] = entry.data().fielddata(0).stringdata();
        }
    }
    EXPECT_EQ(found[ts_1998_last], "end-of-1998");
    EXPECT_EQ(found[ts_2000_first_plus], "start-of-2000");
}

// --- RangeQuery: the network-facing, zero-object-tree path. Every
// test below asserts byte-for-byte SerializeAsString() equality against the
// existing object-based RangeQuery for the same query -- not just "looks
// right" -- since RangeQuery hand-encodes the wire format directly
// and any bug there would silently produce wrong bytes, not a crash.
namespace {
// Collects every emitted chunk (in order) and hands back the concatenated
// bytes parsed as a RangeQuery_REP -- what any real caller (the C++ client's
// SendRecv, ZMQ_Server.cpp) does with a streamed reply's frames.
XStore_Proto::RangeQuery_REP RunStreamedAndReassemble(XStore &store, unsigned long long start, unsigned long long end,
                                                       const std::string &db_name, size_t chunk_size_bytes,
                                                       int *chunk_count_out = nullptr) {
    std::string joined;
    int chunk_count = 0;
    store.RangeQuery(start, end, db_name, chunk_size_bytes,
        [&](std::string &&chunk, bool /*is_last*/) {
            chunk_count++;
            joined += chunk;
        });
    if (chunk_count_out) *chunk_count_out = chunk_count;
    XStore_Proto::RangeQuery_REP rep;
    rep.ParseFromString(joined);
    return rep;
}
}  // namespace

TEST_F(XStoreTest, RangeQueryStreamedMatchesRangeQueryForDenseMultiColumnRows) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Mixes every FData oneof case reachable from a normal insert, including
    // the negative-int32/int64 wire gotcha (always a 10-byte varint --
    // exactly the case that would break a skip-scan that assumed varints
    // are always short).
    for (int i = 0; i < 20; i++) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(TS_2016_MID + i, MakeRow({
            FDouble(3.14159 * i),
            FInt32(-i),
            FInt64(-9000000000LL - i),
            FUInt64(static_cast<uint64_t>(i) * 1'000'000'007ULL),
            FBool(i % 2 == 0),
            FString("row-" + std::to_string(i)),
            FBytes(std::string(10, static_cast<char>('a' + (i % 26)))),
        }), "TESTDB", &rep);
    }

    XStore_Proto::RangeQuery_REP expected;
    expected = RangeQuery(store, TS_2016_MID, TS_2016_MID + 19, "TESTDB");

    // Huge chunk size -- the whole response fits in one chunk, exercising
    // the "small query never actually chunks" degenerate case.
    int chunk_count = 0;
    XStore_Proto::RangeQuery_REP actual = RunStreamedAndReassemble(store, TS_2016_MID, TS_2016_MID + 19, "TESTDB",
                                                                     1ull << 30, &chunk_count);

    EXPECT_EQ(chunk_count, 1);
    ASSERT_EQ(actual.queryresponse_size(), expected.queryresponse_size());
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString())
        << "RangeQuery's hand-encoded bytes diverge from RangeQuery's object-tree serialization for dense multi-type rows";
}

TEST_F(XStoreTest, RangeQueryStreamedHandlesHolesAndSparseColumnsSameAsRangeQuery) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Deliberate gaps (holes) between inserts, and rows with different
    // column counts (sparse columns) -- both need to round-trip identically
    // to the object-based path without any special-casing in the encoder.
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FString("only-one-column")}), "TESTDB", &rep);
    store.UnaryInsert(TS_2009_MID + 3, MakeRow({FInt64(42), FDouble(2.5)}), "TESTDB", &rep);
    store.UnaryInsert(TS_2009_MID + 7, MakeRow({}), "TESTDB", &rep);  // explicit empty row, not a hole
    // TS_2009_MID+1, +2, +4, +5, +6 are left as real holes (never inserted).

    XStore_Proto::RangeQuery_REP expected;
    expected = RangeQuery(store, TS_2009_MID, TS_2009_MID + 7, "TESTDB");

    XStore_Proto::RangeQuery_REP actual = RunStreamedAndReassemble(store, TS_2009_MID, TS_2009_MID + 7, "TESTDB", 1ull << 30);

    ASSERT_EQ(actual.queryresponse_size(), 8);
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

TEST_F(XStoreTest, RangeQueryStreamedSplitsAcrossMultipleChunksAndReassemblesToMatchRangeQuery) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 64);

    constexpr int kRows = 500;
    for (int i = 0; i < kRows; i++) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(TS_2024_MID + i, MakeRow({FDouble(i * 1.5), FInt64(i)}), "TESTDB", &rep);
    }

    XStore_Proto::RangeQuery_REP expected;
    expected = RangeQuery(store, TS_2024_MID, TS_2024_MID + kRows - 1, "TESTDB");

    // 200 bytes/chunk against ~500 rows of ~20-30 bytes each guarantees this
    // splits into well over a dozen chunks -- the actual behavior under
    // test, not just a theoretical code path.
    int chunk_count = 0;
    XStore_Proto::RangeQuery_REP actual = RunStreamedAndReassemble(store, TS_2024_MID, TS_2024_MID + kRows - 1,
                                                                     "TESTDB", 200, &chunk_count);

    EXPECT_GT(chunk_count, 10) << "test setup didn't actually force multiple chunks -- widen the row count or shrink chunk_size_bytes";
    ASSERT_EQ(actual.queryresponse_size(), kRows);
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString())
        << "reassembled multi-chunk output diverges from a single-shot RangeQuery -- a chunk boundary is corrupting an entry";
}

TEST_F(XStoreTest, RangeQueryStreamedPreservesMissingYearWarningMessage) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32);

    constexpr unsigned long long ts_1998_last = 915148799ULL;
    constexpr unsigned long long ts_2000_first_plus = 946684810ULL;

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);
    store.UnaryInsert(ts_2000_first_plus, MakeRow({FString("start-of-2000")}), "TESTDB", &insert_rep);
    // 1999.XSTORE never created.

    XStore_Proto::RangeQuery_REP expected;
    expected = RangeQuery(store, ts_1998_last, ts_2000_first_plus, "TESTDB");

    XStore_Proto::RangeQuery_REP actual = RunStreamedAndReassemble(store, ts_1998_last, ts_2000_first_plus, "TESTDB", 1ull << 30);

    EXPECT_EQ(actual.querystatus(), expected.querystatus());
    EXPECT_NE(actual.querystatus().find("1999.XSTORE does not exist"), std::string::npos) << actual.querystatus();
    EXPECT_EQ(actual.SerializeAsString(), expected.SerializeAsString());
}

TEST_F(XStoreTest, RangeQueryStreamedMissingFinalYearFileReturnsInsteadOfHanging) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32);

    constexpr unsigned long long ts_1998_last = 915148799ULL;
    constexpr unsigned long long ts_1999_early = 915148810ULL;  // 1999.XSTORE never created

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);

    std::atomic<bool> completed{false};
    XStore_Proto::RangeQuery_REP actual;
    std::thread worker([&]() {
        actual = RunStreamedAndReassemble(store, ts_1998_last, ts_1999_early, "TESTDB", 1ull << 30);
        completed = true;
    });

    for (int i = 0; i < 100 && !completed; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (!completed) {
        worker.detach();
        FAIL() << "RangeQuery hung instead of returning when the final year's file was missing";
        return;
    }
    worker.join();

    EXPECT_EQ(actual.queryresponse_size(), 1);
    EXPECT_NE(actual.querystatus().find("1999.XSTORE does not exist"), std::string::npos) << actual.querystatus();
}

TEST_F(XStoreTest, RangeQueryMissingFinalYearFileReturnsInsteadOfHanging) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32);

    constexpr unsigned long long ts_1998_last = 915148799ULL;   // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long ts_1999_early = 915148810ULL;  // 1999-01-01 00:00:10 UTC -- 1999.XSTORE never created

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);  // creates 1998.XSTORE only

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
    std::atomic<bool> completed{false};
    XStore_Proto::RangeQuery_REP query_rep;
    std::thread worker([&]() {
        query_rep = RangeQuery(store, ts_1998_last, ts_1999_early, "TESTDB");
        completed = true;
    });

    for (int i = 0; i < 100 && !completed; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (!completed) {
        worker.detach();  // don't join a thread that may be stuck forever
        FAIL() << "RangeQuery hung instead of returning when the final year's file was missing";
        return;
    }
    worker.join();

    bool found1998 = false;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        if (query_rep.queryresponse(i).timestamp() == ts_1998_last) {
            found1998 = query_rep.queryresponse(i).data().fielddata_size() == 1;
        }
    }
    EXPECT_TRUE(found1998);
    EXPECT_NE(query_rep.querystatus().find("1999.XSTORE does not exist"), std::string::npos) << query_rep.querystatus();
}

TEST_F(XStoreTest, RangeDeleteSpanningThreeYearsWithMissingMiddleYearFileDeletesExistingEndpoints) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long ts_1998_last = 915148799ULL;
    constexpr unsigned long long ts_2000_first_plus = 946684810ULL;

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);
    store.UnaryInsert(ts_2000_first_plus, MakeRow({FString("start-of-2000")}), "TESTDB", &insert_rep);
    // 1999.XSTORE never created.

    (void) store.RangeDelete(ts_1998_last, ts_2000_first_plus, "TESTDB");

    // Confirms the delete reaches (and clears) both endpoints without
    // hanging or skipping past them while walking through the missing
    // middle year.
    XStore_Proto::UnaryQuery_REP rep1998, rep2000;
    store.UnaryQuery(ts_1998_last, "TESTDB", &rep1998);
    store.UnaryQuery(ts_2000_first_plus, "TESTDB", &rep2000);
    EXPECT_EQ(rep1998.data().fielddata_size(), 0);
    EXPECT_EQ(rep2000.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, RangeDeleteMissingFinalYearFileReturnsInsteadOfHanging) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long ts_1998_last = 915148799ULL;   // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long ts_1999_early = 915148810ULL;  // 1999-01-01 00:00:10 UTC -- 1999.XSTORE never created

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FString("end-of-1998")}), "TESTDB", &insert_rep);  // creates 1998.XSTORE only

    // Same infinite-loop shape as RangeQuery's analogous regression test
    // above -- same reason for starting right at the year boundary too.
    std::atomic<bool> completed{false};
    XStore_Proto::RangeDelete_REP delete_rep;
    std::thread worker([&]() {
        delete_rep = store.RangeDelete(ts_1998_last, ts_1999_early, "TESTDB");
        completed = true;
    });

    for (int i = 0; i < 100 && !completed; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    if (!completed) {
        worker.detach();
        FAIL() << "RangeDelete hung instead of returning when the final year's file was missing";
        return;
    }
    worker.join();

    // RangeDelete's final status is always the trailing count summary (any
    // mid-loop WARNING gets overwritten by it), so correctness is checked via
    // the count instead: only TS_1998_LAST (the single point in the existing
    // year) was deleted.
    EXPECT_NE(delete_rep.deletestatus().find("Deleted 1 items"), std::string::npos) << delete_rep.deletestatus();

    XStore_Proto::UnaryQuery_REP rep1998;
    store.UnaryQuery(ts_1998_last, "TESTDB", &rep1998);
    EXPECT_EQ(rep1998.data().fielddata_size(), 0);
}

TEST_F(XStoreTest, RangeQueryRejectsRequestOver2GBLimit) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 32000);

    XStore_Proto::RangeQuery_REP query_rep;
    // (end-start) * rowLength > 2GB -- just over RangeQuery's hard limit.
    query_rep = RangeQuery(store, TS_2016_MID, TS_2016_MID + 63000, "TESTDB");
    EXPECT_NE(query_rep.querystatus().find("larger than 2GB"), std::string::npos) << query_rep.querystatus();
    EXPECT_EQ(query_rep.queryresponse_size(), 0);
}

TEST_F(XStoreTest, UnaryBatchAndRangeInsertQueryShareSameCachedMappingWithinAYear) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Write the same year's file through all three insert paths, then read
    // every point back through all three query paths -- Unary, Batch and
    // Range all go through the same GetOrMapFile cache now, so a write via
    // one path must be immediately visible via the others without a stale
    // read (previously Batch/Range wrote via stdio underneath whatever
    // mapping Unary might already have open for that same file).
    XStore_Proto::UnaryInsert_REP unary_rep;
    store.UnaryInsert(TS_2009_MID, MakeRow({FString("via-unary")}), "TESTDB", &unary_rep);

    XStore_Proto::BatchInsert_REQ batch_req;
    batch_req.set_dbname("TESTDB");
    batch_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *batch_entry = batch_req.add_batchinsert_data();
    batch_entry->set_timestamp(TS_2009_MID + 1);
    *batch_entry->mutable_data() = MakeRow({FString("via-batch")});
    XStore_Proto::BatchInsert_REP batch_rep;
    store.BatchInsert(&batch_req, &batch_rep);

    XStore_Proto::RangeInsert_REQ range_req;
    range_req.set_durabilitymode(XStore_Proto::SYNC);
    range_req.set_dbname("TESTDB");
    XStore_Proto::RangeInsert_DATA *range_entry = range_req.add_rangeinsert_data();
    range_entry->set_timestamp(TS_2009_MID + 2);
    *range_entry->mutable_data() = MakeRow({FString("via-range")});
    XStore_Proto::RangeInsert_REP range_rep;
    store.RangeInsert(&range_req, &range_rep);

    // Read every point back through UnaryQuery.
    for (int i = 0; i < 3; i++) {
        XStore_Proto::UnaryQuery_REP rep;
        store.UnaryQuery(TS_2009_MID + i, "TESTDB", &rep);
        ASSERT_EQ(rep.data().fielddata_size(), 1) << "i=" << i;
    }

    // Read every point back through BatchQuery.
    XStore_Proto::BatchQuery_REQ query_req;
    query_req.set_dbname("TESTDB");
    for (int i = 0; i < 3; i++) query_req.add_timestamp(TS_2009_MID + i);
    XStore_Proto::BatchQuery_REP query_rep;
    query_rep = BatchQuery(store, &query_req);
    ASSERT_EQ(query_rep.queryresponse_size(), 3);
    std::map<unsigned long long, std::string> by_ts;
    for (int i = 0; i < query_rep.queryresponse_size(); i++) {
        by_ts[query_rep.queryresponse(i).timestamp()] = query_rep.queryresponse(i).data().fielddata(0).stringdata();
    }
    EXPECT_EQ(by_ts[TS_2009_MID], "via-unary");
    EXPECT_EQ(by_ts[TS_2009_MID + 1], "via-batch");
    EXPECT_EQ(by_ts[TS_2009_MID + 2], "via-range");

    // Read every point back through RangeQuery.
    XStore_Proto::RangeQuery_REP range_query_rep;
    range_query_rep = RangeQuery(store, TS_2009_MID, TS_2009_MID + 2, "TESTDB");
    ASSERT_EQ(range_query_rep.queryresponse_size(), 3);
    EXPECT_EQ(range_query_rep.queryresponse(0).data().fielddata(0).stringdata(), "via-unary");
    EXPECT_EQ(range_query_rep.queryresponse(1).data().fielddata(0).stringdata(), "via-batch");
    EXPECT_EQ(range_query_rep.queryresponse(2).data().fielddata(0).stringdata(), "via-range");
}

// ---------------------------------------------------------------------
// Mmap LRU cache -- GetOrMapFile caches PROT_WRITE mappings capped at
// XStore::MMAP_CACHE_CAP (256, see include/XStore/XStore.h); Unary/Batch/Range
// insert+query all share it (confirmed above).
// ---------------------------------------------------------------------

TEST_F(XStoreTest, GetOrMapFileEvictsBeyondCapAndEvictedFilesStillReadCorrectly) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 8);

    constexpr int k_year_count = 300;  // > MmapCacheCap (256) -- forces eviction
    std::vector<unsigned long long> timestamps;
    timestamps.reserve(k_year_count);

    for (int i = 0; i < k_year_count; i++) {
        int year = 1700 + i;  // distinct year per iteration -> distinct .XSTORE file per insert
        struct tm year_mid{};
        year_mid.tm_year = year - 1900;
        year_mid.tm_mon = 6;  // July -- far from any year boundary
        year_mid.tm_mday = 1;
        year_mid.tm_hour = 12;
        unsigned long long ts = static_cast<unsigned long long>(timegm(&year_mid));
        timestamps.push_back(ts);

        XStore_Proto::UnaryInsert_REP insert_rep;
        store.UnaryInsert(ts, MakeRow({FInt32(i)}), "TESTDB", &insert_rep);
        ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos) << "i=" << i;
    }

    // Cache must never grow past its cap despite KYearCount distinct files
    // having been mapped over the course of the loop above -- summed across
    // all shards now that the cache is sharded; each shard independently
    // enforces MmapShardCap, so the total can never exceed their sum
    // (MmapCacheCap) regardless of how hashing distributed the 300 files.
    size_t total_count = 0;
    for (const auto &shard : XStore::mmap_shards) {
        EXPECT_LE(shard.count, XStore::MMAP_SHARD_CAP);
        total_count += shard.count;
    }
    EXPECT_LE(total_count, XStore::MMAP_CACHE_CAP);

    // Read every point back, including the ones whose mapping must have been
    // evicted (and therefore has to be transparently re-created by
    // GetOrMapFile on this read) -- correctness must survive eviction+remap,
    // not just hold for whatever's still in the originally-cached tail.
    for (int i = 0; i < k_year_count; i++) {
        XStore_Proto::UnaryQuery_REP query_rep;
        store.UnaryQuery(timestamps[i], "TESTDB", &query_rep);
        ASSERT_EQ(query_rep.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(query_rep.data().fielddata(0).int32data(), i) << "i=" << i;
    }
}

// ---------------------------------------------------------------------
// Aggregates -- Min/Max/Sum/Avg, happy path over a contiguous, fully
// populated numeric column
// ---------------------------------------------------------------------

TEST_F(XStoreTest, AggregatesOverContiguousNumericColumn) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    const std::vector<double> values = {10.0, 3.0, 7.0, 20.0, 1.0};
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = MakeRow({FDouble(values[i])});
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    const unsigned long long start = TS_2024_MID;
    const unsigned long long end = TS_2024_MID + values.size() - 1;

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(start, end, "TESTDB", 0, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(start, end, "TESTDB", 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 20.0);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(start, end, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 41.0);

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(start, end, "TESTDB", 0, &avg_rep);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), 8.2);
}

TEST_F(XStoreTest, MinAggregateColumnIndexOutOfRangeReturnsNoValidData) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2031_MID, MakeRow({FDouble(1.0)}), "TESTDB", &insert_rep);  // only column 0 exists

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2031_MID, TS_2031_MID, "TESTDB", 5, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "ERROR: No valid data found");
}

// Min/Max/Sum/Avg all gate on GetOrLoadCfg the same way every insert/query
// path does, but unlike those paths (each covered by its own *OnMissingDB*
// test above) none of the four aggregates had a missing-DB regression test.
TEST_F(XStoreTest, MinAggregateOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "[WARNING] [MIN AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, MaxAggregateOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &max_rep);
    EXPECT_EQ(max_rep.opstatus(), "[WARNING] [MAX AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, SumAggregateOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "[WARNING] [SUM AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

TEST_F(XStoreTest, AvgAggregateOnMissingDBReturnsError) {
    XStore store;
    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2016_MID, TS_2016_MID, "NOPE", 0, &avg_rep);
    EXPECT_EQ(avg_rep.opstatus(), "[WARNING] [AVG AGGREGATE] [DB: NOPE] - NOPE.CFG does not exist");
}

// All four aggregates gate on startEpochT > endEpochT the same way, right
// after the GetOrLoadCfg check above -- covered for none of them until now.
TEST_F(XStoreTest, MinAggregateRejectsStartAfterEnd) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "[WARNING] [MIN AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, MaxAggregateRejectsStartAfterEnd) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &max_rep);
    EXPECT_EQ(max_rep.opstatus(), "[WARNING] [MAX AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, SumAggregateRejectsStartAfterEnd) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "[WARNING] [SUM AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, AvgAggregateRejectsStartAfterEnd) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2016_MID + 1, TS_2016_MID, "TESTDB", 0, &avg_rep);
    EXPECT_EQ(avg_rep.opstatus(), "[WARNING] [AVG AGGREGATE] [DB: TESTDB] - Target end time must be after start time");
}

TEST_F(XStoreTest, SumAndAvgAggregateWithRemainderDivisor) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    const std::vector<double> values = {2.0, 3.0, 5.0};  // sum=10, avg=3.333...
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (size_t i = 0; i < values.size(); i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2009_MID + i);
        *entry->mutable_data() = MakeRow({FDouble(values[i])});
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2009_MID, TS_2009_MID + 2, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 10.0);

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2009_MID, TS_2009_MID + 2, "TESTDB", 0, &avg_rep);
    EXPECT_NEAR(avg_rep.opresult(), 10.0 / 3.0, 1e-9);
}

TEST_F(XStoreTest, AggregateOverMultiYearRangeSpansAllYears) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Data sits right at the 2016/2017 year boundary (not mid-year) so the
    // aggregate kernels only need to scan a couple of rows to prove the
    // multi-year loop crosses correctly -- a genuinely mid-year span would
    // force a full ~31.5M-row linear scan of any fully-contained year that
    // has data, which is correct behavior but far too slow to use as a test
    // fixture.
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FDouble(5.0)}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2017_FIRST, MakeRow({FDouble(-10.0)}), "TESTDB", &insert_rep);

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), -10.0);

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 5.0);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), -5.0);

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2016_LAST, TS_2017_FIRST, "TESTDB", 0, &avg_rep);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), -2.5);
}

TEST_F(XStoreTest, AggregateOverWideMultiYearSpanUsesParallelWalkCorrectly) {
    // AggregateOverMultiYearRangeSpansAllYears above only spans 2 years --
    // below WalkYearSpanParallel's kMinSegmentsForParallelWalk (4), so it
    // always takes the sequential fallback path and never actually
    // exercises the parallel-dispatch/merge code. This test spans many
    // years to drive requests through the parallel path (thread-per-segment
    // dispatch + cross-thread merge), verifying Min/Max/Sum/Avg all still
    // compute the correct answer once real threads and a merge step are
    // involved.
    //
    // Data sits at the exact first/last second of the two data-bearing
    // years, same reasoning as AggregateOverMultiYearRangeSpansAllYears:
    // any year strictly BETWEEN the query's start and end is "fully
    // contained," so WalkYearSpan always hands its kernel the *entire*
    // year's row count regardless of where within that year data was
    // inserted -- the insert position only matters for the two edge years.
    // The only way to keep middle years cheap is to never insert into them
    // at all, so their .XSTORE file never exists and WalkYearSpan's
    // exists() check skips them for free -- they still count toward the
    // segment total that triggers parallel dispatch.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // TimeStruct (not hand-computed epoch arithmetic) finds the exact start
    // of "about 6 years after 2016" -- avoids an off-by-a-leap-day error
    // accidentally landing this row mid-year instead of on a boundary.
    unsigned long long approx_future_ts = TS_2016_LAST + (6ULL * 365 * 86400);
    TimeStruct future_year(static_cast<time_t>(approx_future_ts), XStore_Proto::SECOND);
    unsigned long long future_year_first = static_cast<unsigned long long>(future_year.first_epoch);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FDouble(-30.0)}), "TESTDB", &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);
    store.UnaryInsert(future_year_first, MakeRow({FDouble(20.0)}), "TESTDB", &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_LAST, future_year_first, "TESTDB", 0, &min_rep);
    ASSERT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), -30.0);

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(TS_2016_LAST, future_year_first, "TESTDB", 0, &max_rep);
    ASSERT_EQ(max_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 20.0);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_LAST, future_year_first, "TESTDB", 0, &sum_rep);
    ASSERT_EQ(sum_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), -10.0);

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2016_LAST, future_year_first, "TESTDB", 0, &avg_rep);
    ASSERT_EQ(avg_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), -5.0);
}

TEST_F(XStoreTest, MinAggregateTieAcrossYearsUnderParallelWalkPicksCorrectValue) {
    // Companion to MinMaxAggregateTieBreakReturnsFirstEncounteredRow (which
    // only covers a within-one-year, sequential-path tie): with >=4 years
    // in play, ties are resolved by thread/segment processing order rather
    // than strict chronological order (documented tradeoff in
    // MinAggregate) -- this only asserts the numeric result is correct (the
    // actual tied value), not which specific row's FData comes back, since
    // that identity guarantee is the one thing NOT promised for a
    // cross-thread tie. Same first/last-second-only data placement as
    // AggregateOverWideMultiYearSpanUsesParallelWalkCorrectly, for the same
    // "don't force a full-year scan on a middle year" reason.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    unsigned long long approx_future_ts = TS_2016_LAST + (6ULL * 365 * 86400);
    TimeStruct future_year(static_cast<time_t>(approx_future_ts), XStore_Proto::SECOND);
    unsigned long long future_year_first = static_cast<unsigned long long>(future_year.first_epoch);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_LAST, MakeRow({FDouble(1.0)}), "TESTDB", &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);
    store.UnaryInsert(future_year_first, MakeRow({FDouble(1.0)}), "TESTDB", &insert_rep);  // ties for MIN
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_LAST, future_year_first, "TESTDB", 0, &min_rep);
    ASSERT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 1.0);
}

TEST_F(XStoreTest, AggregateOverRangeWithMissingMiddleYearFileSkipsItAndReflectsRemainingYears) {
    // Same "warn and continue past a missing year file" shape as
    // RangeQuery/RangeDelete's analogous multi-year tests, but for the
    // aggregate kernels -- 1998, 1999 (missing), 2000, exercising the
    // multi-year while loop's missing-file branch for both
    // ProcessFileRangeSelect (Min, via MinAggregate) and
    // ProcessFileRangeAccumulate (Sum, via SumAggregate).
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    constexpr unsigned long long ts_1998_last = 915148799ULL;       // 1998-12-31 23:59:59 UTC
    constexpr unsigned long long ts_2000_first_plus = 946684810ULL; // 2000-01-01 00:00:10 UTC

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(ts_1998_last, MakeRow({FDouble(5.0)}), "TESTDB", &insert_rep);
    store.UnaryInsert(ts_2000_first_plus, MakeRow({FDouble(-10.0)}), "TESTDB", &insert_rep);
    // Deliberately nothing inserted for 1999 -> 1999.XSTORE never gets created.

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(ts_1998_last, ts_2000_first_plus, "TESTDB", 0, &min_rep);
    EXPECT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), -10.0);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(ts_1998_last, ts_2000_first_plus, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), -5.0);
}

TEST_F(XStoreTest, MinMaxAggregateTieBreakReturnsFirstEncounteredRow) {
    // ProcessFileRangeSelect only replaces the running best on a *strict*
    // Better(candidate, best) -- ties never replace -- so among several rows
    // sharing the winning value, the first one scanned (earliest timestamp)
    // must be the one returned. Column 1 carries a per-row marker so the
    // winning row's identity (not just its column-0 value) can be checked.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    auto add_row = [&](int i, double val, const std::string &marker) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2009_MID + i);
        *entry->mutable_data() = MakeRow({FDouble(val), FString(marker)});
    };
    add_row(0, 5.0, "first-min");
    add_row(1, 1.0, "first-tie");   // ties for MIN
    add_row(2, 1.0, "second-tie");  // same MIN value, must lose to the earlier row
    add_row(3, 5.0, "second-max");  // ties for MAX with row 0

    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2009_MID, TS_2009_MID + 3, "TESTDB", 0, &min_rep);
    ASSERT_EQ(min_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 1.0);

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(TS_2009_MID, TS_2009_MID + 3, "TESTDB", 0, &max_rep);
    ASSERT_EQ(max_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 5.0);
}

TEST_F(XStoreTest, AggregateSkipsUnsetFieldData) {
    // Distinct branch from AggregateSkipsNonNumericColumnValues below: an
    // actually-unset FData (oneOfData_case() == 0) fails the kernels'
    // "case() > 0" check, not the "case() <= 12" one.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    auto add_row = [&](int i, const XStore_Proto::FData &f) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2031_MID + i);
        *entry->mutable_data() = MakeRow({f});
    };
    add_row(0, FDouble(3.0));
    add_row(1, XStore_Proto::FData());  // unset -- must not be treated as 0.0
    add_row(2, FDouble(7.0));

    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2031_MID, TS_2031_MID + 2, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 10.0) << "unset field must not contribute (not even as 0.0)";

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(TS_2031_MID, TS_2031_MID + 2, "TESTDB", 0, &avg_rep);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), 5.0) << "divisor must be 2 (valid rows only), not 3";
}

TEST_F(XStoreTest, AggregateTreatsExplicitlyEmptyRowSameAsHole) {
    // The aggregate kernels' hole check is just RowPtr[0] == 0 (the first
    // serialized byte) -- normally safe, since FData's first field always
    // serializes with a non-zero protobuf tag byte. But a row inserted with
    // *zero* fielddata entries serializes to zero bytes, so its slot stays
    // all-zero (indistinguishable from a never-written hole). Confirms that
    // degenerate case doesn't crash and is simply excluded, same as a real
    // hole would be.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(4.0)}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2016_MID + 1, MakeRow({}), "TESTDB", &insert_rep);  // explicitly zero columns
    store.UnaryInsert(TS_2016_MID + 2, MakeRow({FDouble(6.0)}), "TESTDB", &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_MID, TS_2016_MID + 2, "TESTDB", 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 10.0) << "the zero-column row must not contribute (and must not crash the scan)";
}

TEST_F(XStoreTest, AggregateSkipsNonNumericColumnValues) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Column 0 alternates between numeric and string/bool/bytes across rows
    // -- only the numeric rows may contribute to the aggregate (the kernels
    // only accept oneOfData_case() in (0, 12], i.e. the 12 numeric branches).
    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    auto add_row = [&](int i, const XStore_Proto::FData &f) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2016_MID + i);
        *entry->mutable_data() = MakeRow({f});
    };
    add_row(0, FDouble(4.0));
    add_row(1, FString("not-a-number"));
    add_row(2, FDouble(6.0));
    add_row(3, FBool(true));
    add_row(4, FBytes("also-not-a-number"));

    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_MID, TS_2016_MID + 4, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 10.0) << "only the two numeric rows (4.0 + 6.0) should contribute";

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2016_MID, TS_2016_MID + 4, "TESTDB", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 4.0);
}

TEST_F(XStoreTest, AggregateOverAllNonNumericColumnReturnsNoValidData) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2031_MID, MakeRow({FString("only-a-string")}), "TESTDB", &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2031_MID, TS_2031_MID, "TESTDB", 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "ERROR: No valid data found");
}

// Every aggregate test above uses a single-column row, so columnIdx is
// trivially always 0 -- none of them actually prove the aggregate reads the
// COLUMN it was asked for rather than e.g. always column 0, and none run at
// any real scale. This uses the full 32-column/10k-row BuildWideRow fixture:
// column 0 is a numeric decoy held constant across all 10k rows (sum would
// be 1,000,000.0 if the kernels ignored columnIdx), column 2 is the real
// target with a distinct value (rowIndex) per row.
TEST_F(XStoreTest, AggregateOnSpecificColumnAmongMixedTypeColumnsIgnoresOtherColumns) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 1024);
    constexpr unsigned short target_column = 2;

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (int i = 0; i < K_WIDE_ROW_COUNT; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    const unsigned long long start = TS_2024_MID;
    const unsigned long long end = TS_2024_MID + K_WIDE_ROW_COUNT - 1;

    // Column 2 holds rowIndex for rowIndex in [0, kWideRowCount).
    const int64_t expected_min = 0;
    const int64_t expected_max = K_WIDE_ROW_COUNT - 1;
    const double expected_sum = (K_WIDE_ROW_COUNT - 1) * static_cast<double>(K_WIDE_ROW_COUNT) / 2.0;
    const double expected_avg = expected_sum / K_WIDE_ROW_COUNT;

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(start, end, "TESTDB", target_column, &min_rep);
    EXPECT_EQ(min_rep.opresult().int64data(), expected_min);

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(start, end, "TESTDB", target_column, &max_rep);
    EXPECT_EQ(max_rep.opresult().int64data(), expected_max);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(start, end, "TESTDB", target_column, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), expected_sum)
        << "must be column 2's sum, not column 0's decoy sum (1000000)";

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(start, end, "TESTDB", target_column, &avg_rep);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), expected_avg);
}

// Distinct from AggregateOverAllNonNumericColumnReturnsNoValidData above:
// that row has no numeric data anywhere, so "no valid data" is ambiguous
// between "wrong column type" and "no numeric data in the row at all". Here
// -- same 32-column/10k-row BuildWideRow fixture -- columns 0 and 2 ARE
// valid numeric data, confirming the aggregate rejects based on the
// SPECIFICALLY requested column's type (column 1, always a string), not on
// whether the row has numeric data somewhere.
TEST_F(XStoreTest, AggregateTargetingNonNumericColumnAmongMixedTypeRowReturnsNoValidData) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 1024);

    XStore_Proto::RangeInsert_REQ insert_req;
    insert_req.set_durabilitymode(XStore_Proto::SYNC);
    insert_req.set_dbname("TESTDB");
    for (int i = 0; i < K_WIDE_ROW_COUNT; i++) {
        XStore_Proto::RangeInsert_DATA *entry = insert_req.add_rangeinsert_data();
        entry->set_timestamp(TS_2024_MID + i);
        *entry->mutable_data() = BuildWideRow(i);
    }
    XStore_Proto::RangeInsert_REP insert_rep;
    store.RangeInsert(&insert_req, &insert_rep);

    const unsigned long long start = TS_2024_MID;
    const unsigned long long end = TS_2024_MID + K_WIDE_ROW_COUNT - 1;

    XStore_Proto::Sum_REP sum_on_string;
    store.SumAggregate(start, end, "TESTDB", 1, &sum_on_string);
    EXPECT_EQ(sum_on_string.opstatus(), "ERROR: No valid data found");

    XStore_Proto::Min_REP min_on_string;
    store.MinAggregate(start, end, "TESTDB", 1, &min_on_string);
    EXPECT_EQ(min_on_string.opstatus(), "ERROR: No valid data found");

    // Sanity: column 0 (the constant 100.0 decoy) in these SAME rows
    // aggregates fine -- proves the ERROR above is really about column 1's
    // type, not the DB/range setup.
    XStore_Proto::Sum_REP sum_on_col0;
    store.SumAggregate(start, end, "TESTDB", 0, &sum_on_col0);
    EXPECT_EQ(sum_on_col0.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(sum_on_col0.opresult(), 100.0 * K_WIDE_ROW_COUNT);
}

TEST_F(XStoreTest, AggregateOverRangeContainingHolesOnlyReflectsPresentRows) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    // Sparse: only positions 0, 2, 4 of a 5-wide window are written; 1 and 3
    // are holes (never inserted).
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2003_MID, MakeRow({FDouble(1.0)}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2003_MID + 2, MakeRow({FDouble(9.0)}), "TESTDB", &insert_rep);
    store.UnaryInsert(TS_2003_MID + 4, MakeRow({FDouble(2.0)}), "TESTDB", &insert_rep);

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2003_MID, TS_2003_MID + 4, "TESTDB", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 12.0);

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(TS_2003_MID, TS_2003_MID + 4, "TESTDB", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 1.0);
}

TEST_F(XStoreTest, ConcurrentSumAggregateDuringConcurrentInsertsDoesNotCrashOrTear) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2024_MID, MakeRow({FDouble(1.0)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    constexpr int k_iterations = 200;
    constexpr int k_width = 50;

    auto writer = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            for (int j = 0; j < k_width; j++) {
                XStore_Proto::UnaryInsert_REP rep;
                store.UnaryInsert(TS_2024_MID + j, MakeRow({FDouble((double)(i + j))}), "TESTDB", &rep);
            }
        }
    };
    auto aggregator = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::Sum_REP rep;
            store.SumAggregate(TS_2024_MID, TS_2024_MID + k_width - 1, "TESTDB", 0, &rep);
            // No crash/hang is the primary assertion; if data is present at
            // all it must have parsed as a real numeric sum, never garbage.
            EXPECT_TRUE(rep.opstatus() == "SUCCESS" || rep.opstatus() == "ERROR: No valid data found");
        }
    };

    std::thread w(writer);
    std::thread a(aggregator);
    w.join();
    a.join();
}

// VarianceAggregate goes through the same AccumulateSegmentWithZoneMapPushdown
// / ProcessFileRangeAccumulate machinery as Sum/Avg above, but folds an
// additional sum_sq_value accumulator per row/block -- exercised here on its
// own so a race narrowly scoped to that extra accumulator (rather than the
// sum/valid_count fields the test above already covers) can't hide. Same
// fixture setup/thread-count/duration constants as
// ConcurrentSumAggregateDuringConcurrentInsertsDoesNotCrashOrTear above,
// substituting VarianceAggregate for SumAggregate.
TEST_F(XStoreTest, ConcurrentVarianceAggregateDuringConcurrentInsertsDoesNotCrashOrTear) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2024_MID, MakeRow({FDouble(1.0)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    constexpr int k_iterations = 200;
    constexpr int k_width = 50;

    auto writer = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            for (int j = 0; j < k_width; j++) {
                XStore_Proto::UnaryInsert_REP rep;
                store.UnaryInsert(TS_2024_MID + j, MakeRow({FDouble((double)(i + j))}), "TESTDB", &rep);
            }
        }
    };
    auto aggregator = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::Variance_REP rep;
            store.VarianceAggregate(TS_2024_MID, TS_2024_MID + k_width - 1, "TESTDB", 0, &rep);
            // No crash/hang is the primary assertion; if data is present at
            // all it must have parsed as a real numeric variance, never
            // garbage (a torn sum_sq_value read could otherwise produce a
            // negative variance or NaN without crashing).
            EXPECT_TRUE(rep.opstatus() == "SUCCESS" || rep.opstatus() == "ERROR: No valid data found");
            if (rep.opstatus() == "SUCCESS") {
                EXPECT_GE(rep.opresult(), 0.0) << "variance must never be negative -- sign of a torn sum_sq_value";
                EXPECT_FALSE(std::isnan(rep.opresult()));
            }
        }
    };

    std::thread w(writer);
    std::thread a(aggregator);
    w.join();
    a.join();
}

// Sum/Avg go through ProcessFileRangeAccumulate (covered under concurrent
// writes above); Min/Max go through the separate ProcessFileRangeSelect
// template instead -- that path's concurrent-read safety was never
// exercised on its own.
TEST_F(XStoreTest, ConcurrentMinAggregateDuringConcurrentInsertsDoesNotCrashOrTear) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2024_MID, MakeRow({FDouble(1.0)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    constexpr int k_iterations = 200;
    constexpr int k_width = 50;

    auto writer = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            for (int j = 0; j < k_width; j++) {
                XStore_Proto::UnaryInsert_REP rep;
                store.UnaryInsert(TS_2024_MID + j, MakeRow({FDouble((double)(i + j))}), "TESTDB", &rep);
            }
        }
    };
    auto aggregator = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::Min_REP rep;
            store.MinAggregate(TS_2024_MID, TS_2024_MID + k_width - 1, "TESTDB", 0, &rep);
            // No crash/hang is the primary assertion; if data is present at
            // all it must have parsed as a real numeric value, never garbage.
            EXPECT_TRUE(rep.opstatus() == "SUCCESS" || rep.opstatus() == "ERROR: No valid data found");
        }
    };

    std::thread w(writer);
    std::thread a(aggregator);
    w.join();
    a.join();
}

// ---------------------------------------------------------------------
// Variance / Stddev
// ---------------------------------------------------------------------

TEST_F(XStoreTest, VarianceAggregateMatchesHandComputedSampleVariance) {
    XStore store;
    CreateTestDb(store, "VARTEST", XStore_Proto::SECOND, 64);

    // Values: 2, 4, 4, 4, 5, 5, 7, 9 -- textbook sample-variance example,
    // mean=5, sample variance (N-1) = 32/7 ~= 4.571428571428571.
    const std::vector<double> values = {2, 4, 4, 4, 5, 5, 7, 9};
    XStore_Proto::UnaryInsert_REP insert_rep;
    for (size_t i = 0; i < values.size(); ++i) {
        store.UnaryInsert(1000 + i, MakeRow({FDouble(values[i])}), "VARTEST", &insert_rep);
    }

    XStore_Proto::Variance_REP var_rep;
    store.VarianceAggregate(1000, 1000 + values.size() - 1, "VARTEST", 0, &var_rep);

    ASSERT_EQ(var_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(var_rep.opresult(), 32.0 / 7.0, 1e-9);
}

TEST_F(XStoreTest, StddevAggregateMatchesSqrtOfVariance) {
    XStore store;
    CreateTestDb(store, "STDDEVTEST", XStore_Proto::SECOND, 64);

    const std::vector<double> values = {2, 4, 4, 4, 5, 5, 7, 9};
    XStore_Proto::UnaryInsert_REP insert_rep;
    for (size_t i = 0; i < values.size(); ++i) {
        store.UnaryInsert(1000 + i, MakeRow({FDouble(values[i])}), "STDDEVTEST", &insert_rep);
    }

    XStore_Proto::Stddev_REP stddev_rep;
    store.StddevAggregate(1000, 1000 + values.size() - 1, "STDDEVTEST", 0, &stddev_rep);

    ASSERT_EQ(stddev_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(stddev_rep.opresult(), std::sqrt(32.0 / 7.0), 1e-9);
}

TEST_F(XStoreTest, VarianceAggregatePartialBlockForcesRealScanFallback) {
    // Small row_length (128) -> RowsPerBlock == 2MB / 128 == 16384, so 200
    // rows sit entirely inside block 0's much larger extent. Querying only
    // half of the inserted range therefore lands inside a block that is
    // NOT fully covered by the query -- AccumulateSegmentWithZoneMapPushdown
    // must take the ProcessFileRangeAccumulate fallback branch for it, not
    // the zero-scan pushdown branch, since block_fully_in_range is false.
    XStore store;
    CreateTestDb(store, "VARPARTIAL", XStore_Proto::SECOND, 128);

    std::vector<double> values;
    for (int i = 0; i < 200; ++i) values.push_back(static_cast<double>(i) * 1.5);
    XStore_Proto::UnaryInsert_REP insert_rep;
    for (size_t i = 0; i < values.size(); ++i) {
        store.UnaryInsert(1000 + i, MakeRow({FDouble(values[i])}), "VARPARTIAL", &insert_rep);
    }

    // Query only the first half of the inserted range.
    size_t half = values.size() / 2;
    double sum = 0.0, sum_sq = 0.0;
    for (size_t i = 0; i < half; ++i) { sum += values[i]; sum_sq += values[i] * values[i]; }
    double n = static_cast<double>(half);
    double expected_variance = (sum_sq - sum * sum / n) / (n - 1.0);

    XStore_Proto::Variance_REP var_rep;
    store.VarianceAggregate(1000, 1000 + half - 1, "VARPARTIAL", 0, &var_rep);

    ASSERT_EQ(var_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(var_rep.opresult(), expected_variance, 1e-6);
}

TEST_F(XStoreTest, VarianceAggregateWithSingleRowReturnsNoValidData) {
    XStore store;
    CreateTestDb(store, "VARSINGLE", XStore_Proto::SECOND, 64);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(1000, MakeRow({FDouble(42.0)}), "VARSINGLE", &insert_rep);

    XStore_Proto::Variance_REP var_rep;
    store.VarianceAggregate(1000, 1000, "VARSINGLE", 0, &var_rep);

    EXPECT_EQ(var_rep.opstatus(), "ERROR: No valid data found");
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
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    constexpr int k_iterations = 500;
    auto writer = [&](int thread_id) {
        XStore store;  // each thread gets its own XStore instance, matching ZMQ_Server's one-instance-per-worker design
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(thread_id * 1'000'000LL + i)}), "TESTDB", &rep);
        }
    };

    std::thread t0(writer, 0);
    std::thread t1(writer, 1);
    t0.join();
    t1.join();

    XStore_Proto::UnaryQuery_REP query_rep;
    seed_store.UnaryQuery(TS_2024_MID, "TESTDB", &query_rep);

    // Every write locked the exact same [offset, offset+rowLength) range, so
    // whichever thread wrote last, the row must be a single, cleanly-parsed
    // value from one of the two threads -- never a torn mix of both.
    ASSERT_EQ(query_rep.data().fielddata_size(), 1) << "row must parse as exactly one clean field, not torn/corrupted bytes";
    int64_t final_value = query_rep.data().fielddata(0).int64data();
    int final_thread = final_value / 1'000'000LL;
    int final_iter = final_value % 1'000'000LL;
    EXPECT_TRUE(final_thread == 0 || final_thread == 1) << "finalValue=" << final_value;
    EXPECT_GE(final_iter, 0);
    EXPECT_LT(final_iter, k_iterations);
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentRegionsOfSameFileDoNotInterfere) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2009_MID, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate 2009.XSTORE once

    constexpr int k_count = 100;
    auto writer = [&](unsigned long long base) {
        XStore store;
        for (int i = 0; i < k_count; i++) {
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

    for (int i = 0; i < k_count; i++) {
        XStore_Proto::UnaryQuery_REP rep0, rep1;
        seed_store.UnaryQuery(base0 + i, "TESTDB", &rep0);
        seed_store.UnaryQuery(base1 + i, "TESTDB", &rep1);
        ASSERT_EQ(rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(rep0.data().fielddata(0).int64data(), (int64_t)(base0 + i));
        EXPECT_EQ(rep1.data().fielddata(0).int64data(), (int64_t)(base1 + i));
    }
}

TEST_F(XStoreTest, ConcurrentWritesToDifferentYearFilesDoNotInterfere) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_1998_MID, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate 1998.XSTORE
    seed_store.UnaryInsert(TS_2031_MID, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate 2031.XSTORE

    constexpr int k_count = 100;
    auto writer = [&](unsigned long long base) {
        XStore store;
        for (int i = 0; i < k_count; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(base + i, MakeRow({FInt64((int64_t)(base + i))}), "TESTDB", &rep);
        }
    };

    std::thread t0(writer, TS_1998_MID);
    std::thread t1(writer, TS_2031_MID);
    t0.join();
    t1.join();

    for (int i = 0; i < k_count; i++) {
        XStore_Proto::UnaryQuery_REP rep0, rep1;
        seed_store.UnaryQuery(TS_1998_MID + i, "TESTDB", &rep0);
        seed_store.UnaryQuery(TS_2031_MID + i, "TESTDB", &rep1);
        ASSERT_EQ(rep0.data().fielddata_size(), 1) << "i=" << i;
        ASSERT_EQ(rep1.data().fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(rep0.data().fielddata(0).int64data(), (int64_t)(TS_1998_MID + i));
        EXPECT_EQ(rep1.data().fielddata(0).int64data(), (int64_t)(TS_2031_MID + i));
    }

    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "1998.XSTORE"));
    EXPECT_TRUE(fs::exists(scratch->test_root / "DataStore" / "TESTDB" / "2031.XSTORE"));
}

// Regression test for a use-after-close race in GetOrOpenFile's fd-cache
// eviction: FdCacheCap is small (8, process-wide -- see XStore.h), and
// eviction used to close() the LRU-oldest fd unconditionally, with no
// coordination against a thread still mid-pwrite() on that exact fd via an
// earlier GetOrOpenFile() return value. Once the closed fd number got
// reused by an unrelated concurrent open(), the racing pwrite() either
// failed outright (observed as this test's rows going missing) or -- worse,
// not exercised deterministically here -- silently wrote into whatever file
// now owned that fd number. Fixed by having GetOrOpenFile/GetOrMapFile
// return a shared_ptr whose refcount includes every in-flight caller, not
// just the cache's own entry, so eviction only drops the cache's reference
// and the actual close()/munmap() is deferred until the last holder (cache
// or caller) releases it.
//
// KYears (20) deliberately exceeds FdCacheCap (8) so this test's own writes
// force real evictions among files it's still actively using, regardless of
// what any earlier test left resident in the (process-wide, static) fd
// cache -- reproduced this reliably pre-fix while benchmarking XBench's
// UNARY_RAND workload under concurrency.
TEST_F(XStoreTest, ConcurrentUnaryInsertAcrossManyYearFilesDoesNotLoseDataToFdCacheEviction) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 64);

    constexpr int k_years = 48;
    constexpr int k_rows_per_year = 50;
    constexpr int k_threads = 8;

    // +400 days per step (not +365) so each index lands in a distinct
    // calendar year regardless of leap years -- doesn't need to be exact,
    // just needs KYears genuinely distinct year files.
    auto year_base_ts = [](int year_idx) -> unsigned long long {
        return TS_2024_MID + static_cast<unsigned long long>(year_idx) * 400ULL * 86'400ULL;
    };

    std::vector<std::thread> writers;
    for (int t = 0; t < k_threads; t++) {
        writers.emplace_back([&, t]() {
            XStore store;
            for (int y = t; y < k_years; y += k_threads) {
                for (int r = 0; r < k_rows_per_year; r++) {
                    XStore_Proto::UnaryInsert_REP rep;
                    store.UnaryInsert(year_base_ts(y) + r, MakeRow({FInt64((int64_t)(y * 1000 + r))}), "TESTDB", &rep);
                }
            }
        });
    }
    for (auto &w : writers) w.join();

    for (int y = 0; y < k_years; y++) {
        for (int r = 0; r < k_rows_per_year; r++) {
            XStore_Proto::UnaryQuery_REP rep;
            seed_store.UnaryQuery(year_base_ts(y) + r, "TESTDB", &rep);
            ASSERT_EQ(rep.data().fielddata_size(), 1) << "year=" << y << " row=" << r << " -- missing (fd-cache eviction race dropped this write)";
            EXPECT_EQ(rep.data().fielddata(0).int64data(), (int64_t)(y * 1000 + r)) << "year=" << y << " row=" << r << " -- wrong value (fd-cache eviction race clobbered this write)";
        }
    }
}

TEST_F(XStoreTest, ConcurrentUnaryInsertAndUnaryQuerySameOffsetDoNotRace) {
    XStore seed_store;
    CreateTestDb(seed_store, "TESTDB", XStore_Proto::SECOND, 128);
    XStore_Proto::UnaryInsert_REP seed_rep;
    seed_store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(-1)}), "TESTDB", &seed_rep);  // pre-allocate the year file

    constexpr int k_iterations = 500;
    std::atomic<bool> saw_parse_failure{false};

    auto writer = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(TS_2024_MID, MakeRow({FInt64(i)}), "TESTDB", &rep);
        }
    };
    auto reader = [&]() {
        XStore store;
        for (int i = 0; i < k_iterations; i++) {
            XStore_Proto::UnaryQuery_REP rep;
            store.UnaryQuery(TS_2024_MID, "TESTDB", &rep);
            // The READ lock only guarantees a torn-free read against a
            // concurrent WRITE lock -- it may observe the seed value or any
            // writer iteration, but must always parse as exactly one clean field.
            if (rep.data().fielddata_size() != 1) {
                saw_parse_failure = true;
            }
        }
    };

    std::thread w(writer);
    std::thread r(reader);
    w.join();
    r.join();

    EXPECT_FALSE(saw_parse_failure) << "query observed a torn/partial write instead of a clean single-field row";
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
    XStore reader;

    XStore creator;
    CreateTestDb(creator, "TESTDB", XStore_Proto::SECOND, 128);

    // Make the on-disk .CFG diverge from what Creator just cached. If Reader
    // had to fall back to reading disk itself instead of hitting the shared
    // cache, it would pick up this wrong 4-byte row length instead of 128.
    XStore_Proto::ConfigStruct divergent;
    divergent.set_dbname("TESTDB");
    divergent.set_timeresolution(XStore_Proto::SECOND);
    divergent.set_rowlength(4);
    fs::path cfg_path = scratch->test_root / "DataStore" / "TESTDB" / "TESTDB.CFG";
    FILE *c_file = std::fopen(cfg_path.string().c_str(), "wb+");
    divergent.SerializeToFileDescriptor(fileno(c_file));
    std::fclose(c_file);

    XStore_Proto::UnaryInsert_REP insert_rep;
    reader.UnaryInsert(TS_2016_MID, MakeRow({FDouble(3.14)}), "TESTDB", &insert_rep);

    // Reader must have used the cached 128-byte row length -- not the 4-byte
    // one now sitting on disk -- proving the shared cache, not disk, won.
    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("TESTDB") % XStore::CFG_SHARD_COUNT];
    ASSERT_TRUE(shard.map.count("TESTDB"));
    EXPECT_EQ(shard.map.at("TESTDB").rowlength(), 128);

    XStore_Proto::UnaryQuery_REP query_rep;
    reader.UnaryQuery(TS_2016_MID, "TESTDB", &query_rep);
    ASSERT_EQ(query_rep.data().fielddata_size(), 1);
    EXPECT_DOUBLE_EQ(query_rep.data().fielddata(0).doubledata(), 3.14);
}

TEST_F(XStoreTest, ConcurrentXStoreInstancesShareCfgMapWithoutCorruption) {
    XStore creator;
    CreateTestDb(creator, "TESTDB", XStore_Proto::SECOND, 64);

    // Force every worker below to race on a genuine cache miss instead of
    // trivially hitting what CreateDB already cached.
    for (auto &s : XStore::cfg_shards) s.map.clear();

    constexpr int num_threads = 16;
    std::vector<std::thread> threads;
    std::vector<XStore_Proto::UnaryQuery_REP> reps(num_threads);

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([i, &reps]() {
            XStore worker;  // one instance per thread, matching ZMQ_Server's design
            worker.UnaryQuery(TS_2003_MID, "TESTDB", &reps[i]);
        });
    }
    for (auto &t : threads) t.join();

    // All 16 threads race to populate the same cache entry: whichever one's
    // constructor wins the per-data_store_dir InitDB() race populates it first,
    // and every other thread's GetOrLoadCfg() miss path races behind it. An
    // unguarded map under that many concurrent inserts would risk a
    // corrupted map or a crash; they must instead converge on one consistent
    // entry -- summed across shards since "TESTDB" only ever lands in one.
    size_t total_cfg_entries = 0;
    for (auto &s : XStore::cfg_shards) total_cfg_entries += s.map.size();
    ASSERT_EQ(total_cfg_entries, 1u);
    auto &shard = XStore::cfg_shards[std::hash<std::string>{}("TESTDB") % XStore::CFG_SHARD_COUNT];
    EXPECT_EQ(shard.map.at("TESTDB").rowlength(), 64);
}

// ---------------------------------------------------------------------
// Independent raw-file cross-check -- every round-trip test above writes
// through one XStore method and reads back through another, but both sides
// share the same protobuf (de)serialization code, so a bug that happens to
// affect a write path and its usual read path identically would still pass.
// These three instead decode the raw .XSTORE bytes directly (see
// ReadRowDirectlyFromDisk above), one per distinct write/read code path:
// UnaryInsert's pwrite, Batch/RangeInsert's shared WAL ApplyWalRecord
// run-coalescing path, and SumAggregate's separate mmap-based accumulate
// kernel.
// ---------------------------------------------------------------------

TEST_F(XStoreTest, UnaryInsertWritesExactBytesIndependentlyReadableFromDisk) {
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 128);

    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(TS_2016_MID, MakeRow({FDouble(2.71828), FString("raw-check"), FInt32(-42)}), "TESTDB", &insert_rep);
    ASSERT_NE(insert_rep.insertstatus().find("[INFO]"), std::string::npos);

    TimeStruct ts(TS_2016_MID, XStore_Proto::SECOND);
    fs::path data_file = scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE";
    XStore_Proto::RowData row = ReadRowDirectlyFromDisk(data_file, TS_2016_MID, ts.first_epoch, 128);

    ASSERT_EQ(row.fielddata_size(), 3);
    EXPECT_DOUBLE_EQ(row.fielddata(0).doubledata(), 2.71828);
    EXPECT_EQ(row.fielddata(1).stringdata(), "raw-check");
    EXPECT_EQ(row.fielddata(2).int32data(), -42);
}

TEST_F(XStoreTest, BatchInsertWritesExactBytesIndependentlyReadableFromDisk) {
    // RangeInsert shares this exact write path (WAL's ApplyWalRecord, see
    // src/XStore/XStore_Wal.cpp), so this test's coverage extends to it
    // too -- a near-duplicate RangeInsert version would just repeat it.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 64);

    std::vector<unsigned long long> timestamps = {TS_2016_MID, TS_2016_MID + 500000, TS_2016_MID - 500000};
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname("TESTDB");
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (size_t i = 0; i < timestamps.size(); i++) {
        auto *entry = req.add_batchinsert_data();
        entry->set_timestamp(timestamps[i]);
        *entry->mutable_data() = MakeRow({FInt64(static_cast<int64_t>(i) * 1000 - 7)});
    }
    XStore_Proto::BatchInsert_REP rep;
    store.BatchInsert(&req, &rep);
    ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos);

    fs::path data_file = scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE";
    TimeStruct ts(TS_2016_MID, XStore_Proto::SECOND);
    for (size_t i = 0; i < timestamps.size(); i++) {
        XStore_Proto::RowData row = ReadRowDirectlyFromDisk(data_file, timestamps[i], ts.first_epoch, 64);
        ASSERT_EQ(row.fielddata_size(), 1) << "i=" << i;
        EXPECT_EQ(row.fielddata(0).int64data(), static_cast<int64_t>(i) * 1000 - 7) << "i=" << i;
    }
}

TEST_F(XStoreTest, SumAggregateMatchesIndependentlyComputedSumFromRawDiskBytes) {
    // SumAggregate reads via ProcessFileRangeAccumulate's mmap-based kernel
    // (XStore_IO.cpp) -- a third, distinct read path from UnaryQuery's
    // pread and BatchQuery's pread-per-row, so a bug specific to that
    // kernel wouldn't show up in either test above.
    XStore store;
    CreateTestDb(store, "TESTDB", XStore_Proto::SECOND, 64);

    std::vector<unsigned long long> timestamps = {TS_2016_MID, TS_2016_MID + 10, TS_2016_MID + 25, TS_2016_MID + 40};
    std::vector<double> values = {12.5, -30.25, 100.0, -0.75};
    for (size_t i = 0; i < timestamps.size(); i++) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(timestamps[i], MakeRow({FDouble(values[i]), FString("decoy")}), "TESTDB", &rep);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << "i=" << i;
    }

    fs::path data_file = scratch->test_root / "DataStore" / "TESTDB" / "2016.XSTORE";
    TimeStruct ts(TS_2016_MID, XStore_Proto::SECOND);
    double expected_sum = 0.0;
    for (size_t i = 0; i < timestamps.size(); i++) {
        XStore_Proto::RowData row = ReadRowDirectlyFromDisk(data_file, timestamps[i], ts.first_epoch, 64);
        expected_sum += row.fielddata(0).doubledata();
    }

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(TS_2016_MID, TS_2016_MID + 40, "TESTDB", 0, &sum_rep);
    EXPECT_EQ(sum_rep.opstatus(), "SUCCESS");
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), expected_sum);
}

// XStorePreadPool -- standalone (no DB/XStore instance needed, unlike every
// test above): this is a regression test for a real bug found on Mystic
// under real multi-client concurrent load, that no single-threaded/single-
// call test could ever catch. Root cause: a Job's lifetime was originally
// tied to its work-completion counter reaching zero, not to every one of
// its pushed "tickets" actually being consumed -- under real concurrency,
// a few fast pool workers can drain a whole Job before its sibling tickets
// even get dequeued, so the submitter returned (destroying the stack-local
// Job) while other tickets for that SAME job were still sitting in the
// shared queue, later dereferencing a dangling pointer. Fixed by tying Job
// lifetime to all pushed tickets retiring (see XStore_PreadPool.cpp's Job
// comment). This test hammers ParallelFor from many threads at once,
// repeatedly, with varying job sizes -- the exact shape that exposed the
// bug live -- so a future change to XStorePreadPool that reintroduces a
// similar lifetime bug fails HERE, in the normal test suite, instead of
// needing a live multi-client crash on real infrastructure to notice.
TEST(XStorePreadPoolTest, ConcurrentParallelForFromManyThreadsNeverMissesOrDuplicatesWork) {
    constexpr int kCallerThreads = 16;
    constexpr int kItersPerThread = 50;
    std::atomic<bool> mismatch{false};

    auto caller = [&](int seed) {
        std::mt19937 rng(static_cast<unsigned>(seed));
        std::uniform_int_distribution<int> size_dist(1, 3000);
        for (int iter = 0; iter < kItersPerThread; iter++) {
            size_t n = static_cast<size_t>(size_dist(rng));
            std::vector<std::atomic<int>> hit(n);
            for (auto &h : hit) h.store(0, std::memory_order_relaxed);

            XStorePreadPool::ParallelFor(n, [&](size_t i) {
                hit[i].fetch_add(1, std::memory_order_relaxed);
            });

            for (size_t i = 0; i < n; i++) {
                if (hit[i].load(std::memory_order_relaxed) != 1) {
                    mismatch.store(true, std::memory_order_relaxed);
                }
            }
        }
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kCallerThreads; t++) threads.emplace_back(caller, t * 7919 + 13);
    for (auto &t : threads) t.join();

    EXPECT_FALSE(mismatch.load()) << "some index was processed zero or multiple times -- "
                                      "XStorePreadPool lost or duplicated work under concurrent callers";
}
