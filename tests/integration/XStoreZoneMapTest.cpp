#include <gtest/gtest.h>
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_ZoneMap.h"
#include "XStoreTestSupport.h"
#include <filesystem>
#include <type_traits>

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
    EXPECT_EQ(sizeof(ZoneMapEntry), 40u);
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
