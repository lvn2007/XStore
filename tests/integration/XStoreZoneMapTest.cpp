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
