// Durability A (docs/superpowers/plans/2026-09-28-durability-a-dirty-index-
// rebuild.md), Task 1: header format + version bumps. Only the on-disk
// layout and version numbers are asserted here -- the `dirty` flag's actual
// set/clear/rebuild behavior arrives in later tasks (2-4), which extend this
// file.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStoreTestSupport.h"
#include <cstddef>
#include <fcntl.h>
#include <unistd.h>

namespace {
using xstore_test::ScratchDir;

TEST(DurabilityHeaderLayoutTest, ZoneMapHeaderLayout) {
    EXPECT_EQ(sizeof(ZoneMapFileHeader), 24u);
    EXPECT_EQ(offsetof(ZoneMapFileHeader, dirty), 12u);
    EXPECT_EQ(ZONE_MAP_VERSION, 4u);

    ScratchDir scratch("durability_zmap_header_layout");
    std::string path = (scratch.test_root / "2024.zmap").string();

    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, /*row_length=*/64, /*tracked_column_count=*/64));

    ZoneMapFileHeader on_disk{};
    int fd = open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, &on_disk, sizeof(on_disk), 0), static_cast<ssize_t>(sizeof(on_disk)));
    close(fd);

    EXPECT_EQ(on_disk.dirty, 0u);
    EXPECT_EQ(on_disk.version, 4u);
}

TEST(DurabilityHeaderLayoutTest, CatIdxHeaderLayout) {
    EXPECT_EQ(sizeof(CatIdxFileHeader), 32u);
    EXPECT_EQ(XStoreTunables::CATIDX_VERSION, 2u);

    ScratchDir scratch("durability_catidx_header_layout");
    std::string path = (scratch.test_root / "2024.catidx").string();

    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, /*row_length=*/64, /*tracked_column_count=*/64));

    CatIdxFileHeader on_disk{};
    int fd = open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, &on_disk, sizeof(on_disk), 0), static_cast<ssize_t>(sizeof(on_disk)));
    close(fd);

    EXPECT_EQ(on_disk.dirty, 0u);
    EXPECT_EQ(on_disk.version, 2u);
}

} // namespace
