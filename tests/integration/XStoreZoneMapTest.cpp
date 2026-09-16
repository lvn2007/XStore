#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_ZoneMap.h"
#include "XStoreTestSupport.h"
#include <filesystem>
#include <type_traits>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

// Same anonymous-namespace + using-declaration convention as
// XStoreWalTest.cpp -- keeps this file's helpers/fixture from colliding
// with any same-named symbol in another test translation unit.
namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

TEST(ZoneMapMathTest, RowsPerBlockDerivesFromTargetByteSizeNotFixedRowCount) {
    // Narrow row (8 bytes): target/row_length should give many rows/block.
    EXPECT_EQ(RowsPerBlock(8), XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES / 8);
    // Wide row (larger than the target itself): must still return >= 1, never 0.
    uint16_t huge_row = static_cast<uint16_t>(XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES * 4 > 65535
                                                   ? 65535
                                                   : XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES * 4);
    EXPECT_GE(RowsPerBlock(huge_row), 1u);
}

TEST(ZoneMapMathTest, BlockIndexForRowIsIntegerDivision) {
    EXPECT_EQ(BlockIndexForRow(0, 100), 0u);
    EXPECT_EQ(BlockIndexForRow(99, 100), 0u);
    EXPECT_EQ(BlockIndexForRow(100, 100), 1u);
    EXPECT_EQ(BlockIndexForRow(250, 100), 2u);
}

TEST(ZoneMapEntryLayoutTest, EntryIsFixedWidthPodForRawFileIo) {
    static_assert(std::is_trivially_copyable<ZoneMapEntry>::value,
                  "ZoneMapEntry must be trivially copyable for raw pread/pwrite");
    EXPECT_EQ(sizeof(ZoneMapEntry), 48u);
}

TEST(ZoneMapFileTest, SequentialUpdatesToSameBlockAreVisibleImmediately) {
    ScratchDir scratch("zone_map_file_test");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), /*row_length=*/64));

    // Two rows landing in the same block, column 0 values 5.0 and -3.0.
    zmap.UpdateRow(0, {{0, 5.0, true}});
    zmap.UpdateRow(1, {{0, -3.0, true}});

    // No explicit flush call -- write-through means this is already durable.
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, -3.0);
    EXPECT_DOUBLE_EQ((*stats)[0].max_value, 5.0);
    EXPECT_DOUBLE_EQ((*stats)[0].sum_value, 2.0);
    EXPECT_EQ((*stats)[0].valid_count, 2u);
}

TEST(ZoneMapFileTest, StatsSurviveReopenFromDiskWithNoCachedState) {
    ScratchDir scratch("zone_map_file_test_reopen");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    {
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(path.string(), 64));
        zmap.UpdateRow(0, {{0, 10.0, true}});
        // Destructor runs here -- nothing to flush, already on disk.
    }
    ZoneMapFile reopened;
    ASSERT_TRUE(reopened.Open(path.string(), 64));
    auto stats = reopened.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 10.0);
}

TEST(ZoneMapFileTest, ConcurrentUpdateRowFromMultipleThreadsDoesNotLoseUpdates) {
    // Reproduces the real production crash: XStore::GetOrOpenZoneMap hands
    // out a ZoneMapFile& to one shared, process-wide instance per (db,year)
    // -- every server worker thread touching the same DB/year gets the SAME
    // ZoneMapFile object. UpdateRow mutates cache_ (a plain unordered_map)
    // and does non-atomic read-modify-write on a cached ZoneMapEntry's
    // valid_count/sum_value with zero internal locking. Confirmed live on
    // Mystic: 8 concurrent client machines hammering one XStore server
    // segfaulted ~51s in (dmesg: "segfault at 8"), while 1/2/4-way
    // concurrency had not yet hit it -- a classic timing-dependent race, not
    // a deterministic bug. All threads here hit the same block/column (row 0
    // only) specifically to maximize contention on that one entry.
    ScratchDir scratch("zone_map_file_test_concurrent");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), /*row_length=*/64));

    constexpr int kThreads = 8;
    constexpr int kItersPerThread = 5000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&zmap]() {
            for (int i = 0; i < kItersPerThread; ++i) {
                zmap.UpdateRow(0, {{0, 1.0, true}});
            }
        });
    }
    for (auto &th : threads) th.join();

    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, static_cast<uint64_t>(kThreads * kItersPerThread))
        << "lost update(s) under concurrent UpdateRow -- unsynchronized read-modify-write on cache_";
    EXPECT_DOUBLE_EQ((*stats)[0].sum_value, static_cast<double>(kThreads * kItersPerThread));
}

TEST(ZoneMapFileTest, UntouchedBlockReportsZeroValidCountNotMissingData) {
    ScratchDir scratch("zone_map_file_test_untouched");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), 64));
    zmap.UpdateRow(0, {{0, 1.0, true}}); // touches block 0 only
    auto stats = zmap.TryGetBlockStats(5); // block 5 never touched
    ASSERT_TRUE(stats.has_value()); // a real, safe answer: "zero rows here," not "unknown"
    EXPECT_EQ((*stats)[0].valid_count, 0u);
}

TEST(ZoneMapFileTest, ColumnIndexAtOrPastCapIsSilentlyIgnored) {
    ScratchDir scratch("zone_map_file_test_column_cap");
    std::filesystem::path path = scratch.test_root / "2024.zmap";
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path.string(), 64));
    // Must not crash/UB -- just has no effect on tracked columns.
    zmap.UpdateRow(0, {{XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS, 1.0, true}});
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ(stats->size(), XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);
}

} // namespace

// Shared fixture for every zone-map test in this file (Tasks 3-7 all use
// TEST_F(XStoreZoneMapTest, ...)). Same ScratchDir-per-test pattern as
// XStoreWalTest.cpp -- a fresh temp directory per test avoids collisions
// between tests that reuse gtest's heap-allocated fixture address.
namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

class XStoreZoneMapTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_zone_map_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
    }

    void TearDown() override {
        scratch.reset();
    }

    // db_name must already be upper-case.
    std::string DbPath(std::string_view db_name) const {
        return (scratch->test_root / "DataStore" / std::string(db_name)).string();
    }
};

TEST_F(XStoreZoneMapTest, GetOrOpenZoneMapReturnsSameInstanceForSameDbAndYear) {
    XStore store;
    CreateTestDb(store, "ZMAPREG", XStore_Proto::SECOND, 64);
    ZoneMapFile &a = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2024, 64);
    ZoneMapFile &b = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2024, 64);
    EXPECT_EQ(&a, &b);

    ZoneMapFile &different_year = store.GetOrOpenZoneMap(DbPath("ZMAPREG"), 2025, 64);
    EXPECT_NE(&a, &different_year);
}

} // namespace

namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, GetOrOpenZoneMapEvictsOldestPastRegistryCap) {
    XStore store;
    CreateTestDb(store, "ZMAPCAP", XStore_Proto::SECOND, 64);

    // Touch more distinct (db, year) combinations than the cap allows.
    for (unsigned short year = 2000; year < 2000 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 10; ++year) {
        store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), year, 64);
    }
    EXPECT_LE(store.ZoneMapRegistrySize(), XStoreTunables::ZONE_MAP_REGISTRY_CAP);

    // Evicting one instance must not lose durably-written data -- write-
    // through (Task 2) means eviction is just closing a file handle.
    store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), 2000, 64)
        .UpdateRow(0, {{0, 5.0, true}});
    for (unsigned short year = 2001; year < 2000 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
        store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), year, 64); // evicts year 2000 out
    }
    auto stats = store.GetOrOpenZoneMap(DbPath("ZMAPCAP"), 2000, 64).TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 5.0); // survived eviction + reopen
}

} // namespace

namespace {

using xstore_test::CreateTestDb;

// 2023-01-01T00:00:00Z, SECOND resolution -- the start-of-year epoch XStore's
// own TimeStruct year-bucketing computes internally for any 2023 timestamp
// (docs/XStore_Technical_Reference.md §4.3). Used here so the test computes
// `diff`/block index the exact same way production code does, rather than
// guessing or hardcoding block 0 (wrong: 1700600000 is ~28 million seconds
// into 2023, nowhere near its first block).
constexpr unsigned long long kYear2023StartEpoch = 1672531200ULL;

TEST_F(XStoreZoneMapTest, UnaryInsertUpdatesZoneMapForNumericColumns) {
    XStore store;
    CreateTestDb(store, "ZMAPUNARY", XStore_Proto::SECOND, 64);

    unsigned long long timestamp = 1700600000ULL; // 2023-11-21 -- within year 2023
    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(7.5);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(timestamp, row, "ZMAPUNARY", &rep, XStore_Proto::SYNC);

    // No explicit flush call -- UpdateRow is write-through (Task 2), so this
    // is already durable.
    ZoneMapFile &zmap = store.GetOrOpenZoneMap(DbPath("ZMAPUNARY"), 2023, 64);
    uint64_t diff = timestamp - kYear2023StartEpoch;
    uint64_t block = BlockIndexForRow(diff, zmap.RowsPerBlockValue());
    auto stats = zmap.TryGetBlockStats(block);
    ASSERT_TRUE(stats.has_value());
    EXPECT_DOUBLE_EQ((*stats)[0].min_value, 7.5);
    EXPECT_EQ((*stats)[0].valid_count, 1u);
}

TEST_F(XStoreZoneMapTest, UnaryInsertTreatsHoleAsNullNotZero) {
    XStore store;
    CreateTestDb(store, "ZMAPHOLE", XStore_Proto::SECOND, 64);

    unsigned long long timestamp = 1700600000ULL;
    XStore_Proto::RowData row; // fielddata left empty -- a hole at column 0
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(timestamp, row, "ZMAPHOLE", &rep, XStore_Proto::SYNC);

    ZoneMapFile &zmap = store.GetOrOpenZoneMap(DbPath("ZMAPHOLE"), 2023, 64);
    uint64_t diff = timestamp - kYear2023StartEpoch;
    uint64_t block = BlockIndexForRow(diff, zmap.RowsPerBlockValue());
    auto stats = zmap.TryGetBlockStats(block);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 0u);
    EXPECT_EQ((*stats)[0].null_count, 1u);
}

} // namespace

namespace {

using xstore_test::CreateTestDb;

TEST_F(XStoreZoneMapTest, MinAggregatePrunesBlocksButStillFindsCorrectAnswer) {
    XStore store;
    CreateTestDb(store, "ZMAPMINPRUNE", XStore_Proto::SECOND, 64);

    // Populate enough rows to span several blocks (RowsPerBlock(64) rows/block);
    // plant the true minimum in the LAST block only, everything else higher.
    uint64_t rows_per_block = RowsPerBlock(64);
    unsigned long long base = 1700000000ULL;
    for (uint64_t block = 0; block < 3; ++block) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(block == 2 ? -100.0 : 100.0 + static_cast<double>(block));
        store.UnaryInsert(base + block * rows_per_block, row, "ZMAPMINPRUNE", &rep, XStore_Proto::SYNC);
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(base, base + 3 * rows_per_block, "ZMAPMINPRUNE", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), -100.0);
}

TEST_F(XStoreZoneMapTest, MinAggregateCorrectWhenRangeBoundaryFallsMidBlock) {
    // The queried range covers only part of a block -- that block can never
    // be a fully-prunable or fully-pushdownable candidate, so this exercises
    // the real per-row scan fallback path and must still find the true min.
    XStore store;
    CreateTestDb(store, "ZMAPMINBOUNDARY", XStore_Proto::SECOND, 64);
    unsigned long long base = 1700000000ULL;

    XStore_Proto::UnaryInsert_REP rep1, rep2;
    XStore_Proto::RowData row1, row2;
    row1.add_fielddata()->set_doubledata(42.0);
    row2.add_fielddata()->set_doubledata(-7.0); // the true min, one second later
    store.UnaryInsert(base, row1, "ZMAPMINBOUNDARY", &rep1, XStore_Proto::SYNC);
    store.UnaryInsert(base + 1, row2, "ZMAPMINBOUNDARY", &rep2, XStore_Proto::SYNC);

    // Range ends right after the second row -- both rows are in the same
    // block, so this block is neither fully before nor fully after the
    // range boundary in a way that would let a naive implementation skip it.
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(base, base + 2, "ZMAPMINBOUNDARY", 0, &min_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), -7.0);
}

TEST_F(XStoreZoneMapTest, MaxAggregatePrunesBlocksButStillFindsCorrectAnswer) {
    XStore store;
    CreateTestDb(store, "ZMAPMAXPRUNE", XStore_Proto::SECOND, 64);

    uint64_t rows_per_block = RowsPerBlock(64);
    unsigned long long base = 1700000000ULL;
    for (uint64_t block = 0; block < 3; ++block) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(block == 2 ? 500.0 : 1.0 + static_cast<double>(block));
        store.UnaryInsert(base + block * rows_per_block, row, "ZMAPMAXPRUNE", &rep, XStore_Proto::SYNC);
    }

    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(base, base + 3 * rows_per_block, "ZMAPMAXPRUNE", 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 500.0);
}

} // namespace

namespace {

using xstore_test::CreateTestDb;

// row_length=64 (RowsPerBlock == 32768) is what every other zone-map test
// uses, but these three tests insert one row PER SLOT in a full block to
// actually exercise "fully covered" pushdown -- 32768 individual SYNC
// UnaryInserts (one fsync round-trip each) took ~100s per test. A much
// wider row_length keeps the exact same production RowsPerBlock formula
// (ZONE_MAP_TARGET_BLOCK_BYTES / row_length) but yields a small, still
// real, block size -- not a test-only shortcut. Capped well under 32768:
// CreateTestDb's row_length is a signed short (CreateDB_REQ's wire type),
// so 32768 itself silently wraps negative and CreateDB fails.
constexpr uint16_t kWideRowLength = 16384; // -> RowsPerBlock == 128

TEST_F(XStoreZoneMapTest, SumAggregateUsesPrecomputedSumForFullyCoveredBlocks) {
    XStore store;
    CreateTestDb(store, "ZMAPSUM", XStore_Proto::SECOND, kWideRowLength);
    uint64_t rows_per_block = RowsPerBlock(kWideRowLength);
    unsigned long long base = 1700000000ULL;
    double expected_sum = 0.0;
    for (uint64_t i = 0; i < rows_per_block; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>(i));
        store.UnaryInsert(base + i, row, "ZMAPSUM", &rep, XStore_Proto::SYNC);
        expected_sum += static_cast<double>(i);
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(base, base + rows_per_block, "ZMAPSUM", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), expected_sum);
}

TEST_F(XStoreZoneMapTest, SumAggregateMixesPushdownAndScanForPartialBoundaryBlocks) {
    XStore store;
    CreateTestDb(store, "ZMAPSUMMIX", XStore_Proto::SECOND, kWideRowLength);
    uint64_t rows_per_block = RowsPerBlock(kWideRowLength);
    unsigned long long base = 1700000000ULL;
    double expected_sum = 0.0;
    // Fill 2 full blocks plus 3 extra rows into a third (partial) block.
    uint64_t total_rows = rows_per_block * 2 + 3;
    for (uint64_t i = 0; i < total_rows; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(1.0);
        store.UnaryInsert(base + i, row, "ZMAPSUMMIX", &rep, XStore_Proto::SYNC);
        expected_sum += 1.0;
    }
    // No explicit flush needed -- UpdateRow (Task 2) already made this durable.

    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(base, base + total_rows, "ZMAPSUMMIX", 0, &sum_rep);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), expected_sum);
}

TEST_F(XStoreZoneMapTest, AvgAggregateUsesPushdownSumAndCountForFullyCoveredBlocks) {
    // AvgAggregate needs no separate pushdown code (it already divides
    // SumAggregate's own sum/count), so this exercises that the shared
    // AccumulateSegmentWithZoneMapPushdown path also produces the right
    // divisor, not just the right numerator.
    XStore store;
    CreateTestDb(store, "ZMAPAVG", XStore_Proto::SECOND, kWideRowLength);
    uint64_t rows_per_block = RowsPerBlock(kWideRowLength);
    unsigned long long base = 1700000000ULL;
    double expected_sum = 0.0;
    for (uint64_t i = 0; i < rows_per_block; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(static_cast<double>(i));
        store.UnaryInsert(base + i, row, "ZMAPAVG", &rep, XStore_Proto::SYNC);
        expected_sum += static_cast<double>(i);
    }

    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(base, base + rows_per_block, "ZMAPAVG", 0, &avg_rep);
    EXPECT_DOUBLE_EQ(avg_rep.opresult(), expected_sum / static_cast<double>(rows_per_block));
}

TEST_F(XStoreZoneMapTest, VarianceAggregatePushesDownThroughFullyCoveredBlock) {
    // Same fully-covered-block technique as SumAggregateUsesPrecomputedSumForFullyCoveredBlocks
    // above -- one row per slot in a full block, so AccumulateSegmentWithZoneMapPushdown
    // takes the zero-scan pushdown branch (using ZoneMapEntry.sum_sq_value) rather
    // than falling back to a real ProcessFileRangeAccumulate scan.
    XStore store;
    CreateTestDb(store, "ZMAPVAR", XStore_Proto::SECOND, kWideRowLength);
    uint64_t rows_per_block = RowsPerBlock(kWideRowLength);
    unsigned long long base = 1700000000ULL;
    double sum = 0.0, sum_sq = 0.0;
    for (uint64_t i = 0; i < rows_per_block; ++i) {
        XStore_Proto::UnaryInsert_REP rep;
        XStore_Proto::RowData row;
        double v = static_cast<double>(i);
        row.add_fielddata()->set_doubledata(v);
        store.UnaryInsert(base + i, row, "ZMAPVAR", &rep, XStore_Proto::SYNC);
        sum += v;
        sum_sq += v * v;
    }
    double n = static_cast<double>(rows_per_block);
    double expected_variance = (sum_sq - sum * sum / n) / (n - 1.0);

    XStore_Proto::Variance_REP var_rep;
    store.VarianceAggregate(base, base + rows_per_block, "ZMAPVAR", 0, &var_rep);
    ASSERT_EQ(var_rep.opstatus(), "SUCCESS");
    EXPECT_NEAR(var_rep.opresult(), expected_variance, 1e-6);
}

} // namespace

namespace {

using xstore_test::ScratchDir;

class ZoneMapFileVersionTest : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("zone_map_file_test_fixture");
    }

    void TearDown() override {
        scratch.reset();
    }

    std::string TestFilePath(const std::string &filename) {
        return (scratch->test_root / filename).string();
    }
};

TEST_F(ZoneMapFileVersionTest, UpdateRowFoldsSumOfSquaresIntoEntry) {
    ZoneMapFile zmap;
    std::string path = TestFilePath("sumsq.zmap");
    ASSERT_TRUE(zmap.Open(path, /*row_length=*/64));

    zmap.UpdateRow(0, {{/*column_idx=*/0, /*value=*/3.0, /*is_numeric=*/true}});
    zmap.UpdateRow(1, {{/*column_idx=*/0, /*value=*/4.0, /*is_numeric=*/true}});

    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    const ZoneMapEntry &e = (*stats)[0];
    EXPECT_DOUBLE_EQ(e.sum_value, 7.0);
    EXPECT_DOUBLE_EQ(e.sum_sq_value, 25.0); // 3*3 + 4*4
    EXPECT_EQ(e.valid_count, 2u);
}

TEST_F(ZoneMapFileVersionTest, OpenDiscardsStaleVersionFileInsteadOfMisreadingIt) {
    std::string path = TestFilePath("stale_version.zmap");

    // Hand-write a v1-shaped header directly (bypassing ZoneMapFile
    // entirely) to simulate a file written before sum_sq_value existed.
    {
        ZoneMapFileHeader old_header;
        old_header.version = 1; // stale -- current ZONE_MAP_VERSION is 2
        old_header.tracked_column_count = 64;
        old_header.rows_per_block = 100;
        int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, &old_header, sizeof(old_header), 0), static_cast<ssize_t>(sizeof(old_header)));
        close(fd);
    }

    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, /*row_length=*/64));

    // File must now behave as a fresh, empty, correctly-sized v2 zone map --
    // not crash, not return garbage read through the old 40-byte layout.
    auto stats = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 0u);

    // And it must be fully usable afterward -- a real update roundtrips.
    zmap.UpdateRow(0, {{0, 5.0, true}});
    auto stats2 = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(stats2.has_value());
    EXPECT_DOUBLE_EQ((*stats2)[0].sum_value, 5.0);
    EXPECT_DOUBLE_EQ((*stats2)[0].sum_sq_value, 25.0);
}

} // namespace
