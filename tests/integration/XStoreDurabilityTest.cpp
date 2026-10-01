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
#include <vector>

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

// Task 2: dirty flag is set once (before the first index write of a session)
// and cleared only by MarkClean(). Headers are read via pread on the path,
// never through the object under test.
template <typename Header>
uint32_t OnDiskDirty(const std::string &path) {
    Header h{};
    int fd = open(path.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0);
    EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
    close(fd);
    return h.dirty;
}

std::vector<ZoneMapObservation> ZmapObs(double v) {
    return {ZoneMapObservation{0, v, true}};
}

std::vector<CatIdxObservation> CatObs(int64_t v) {
    XStore_Proto::FData val;
    val.set_int64data(v);
    return {CatIdxObservation{0, EncodeFDataForCatIdx(val), true}};
}

TEST(DurabilityMarkDirtyTest, ZoneMapCleanAfterOpenWithoutWrites) {
    ScratchDir scratch("durability_zmap_clean_open");
    std::string path = (scratch.test_root / "2024.zmap").string();
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, 64, 4));
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 0u);
    EXPECT_EQ(zmap.DirtyMarkWritesForTests(), 0u);
}

TEST(DurabilityMarkDirtyTest, ZoneMapMarksDirtyOnceAndMarkCleanResets) {
    ScratchDir scratch("durability_zmap_mark_once");
    std::string path = (scratch.test_root / "2024.zmap").string();
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, 64, 4));

    zmap.UpdateRow(0, ZmapObs(1.0), 1);
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 1u);
    EXPECT_EQ(zmap.DirtyMarkWritesForTests(), 1u);

    for (uint64_t i = 1; i <= 1000; ++i) zmap.UpdateRow(i, ZmapObs(static_cast<double>(i)), 1);
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 1u);
    EXPECT_EQ(zmap.DirtyMarkWritesForTests(), 1u);

    ASSERT_TRUE(zmap.MarkClean());
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 0u);

    zmap.UpdateRow(2000, ZmapObs(2.0), 1);
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 1u);
    EXPECT_EQ(zmap.DirtyMarkWritesForTests(), 2u);
}

TEST(DurabilityMarkDirtyTest, CatIdxCleanAfterOpenWithoutWrites) {
    ScratchDir scratch("durability_catidx_clean_open");
    std::string path = (scratch.test_root / "2024.catidx").string();
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, 64, 4));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 0u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 0u);
}

TEST(DurabilityMarkDirtyTest, CatIdxMarksDirtyOnceAndMarkCleanResets) {
    ScratchDir scratch("durability_catidx_mark_once");
    std::string path = (scratch.test_root / "2024.catidx").string();
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, 64, 4));

    catidx.UpdateRow(0, CatObs(1));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 1u);

    for (uint64_t i = 1; i <= 1000; ++i) catidx.UpdateRow(i, CatObs(static_cast<int64_t>(i)));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 1u);

    ASSERT_TRUE(catidx.MarkClean());
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 0u);

    catidx.UpdateRow(2000, CatObs(2));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 2u);
}

TEST(DurabilityMarkDirtyTest, CatIdxBitmapAppendMarksDirtyAndCommitCarriesIt) {
    ScratchDir scratch("durability_catidx_bitmap");
    std::string path = (scratch.test_root / "2024.catidx").string();
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, 64, 4));

    // Compaction path alone (no prior UpdateRow in this session) must mark dirty.
    catidx.AppendBitmapSection({{0u, {{"a", {1, 2, 3}}}}});
    EXPECT_TRUE(catidx.HasBitmapSection());
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 1u);

    ASSERT_TRUE(catidx.MarkClean());
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 0u);

    // A bloom write into the compacted file rewrites the header to invalidate
    // the bitmap section; that header must carry dirty=1.
    catidx.UpdateRow(0, CatObs(7));
    EXPECT_FALSE(catidx.HasBitmapSection());
    CatIdxFileHeader on_disk{};
    int fd = open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, &on_disk, sizeof(on_disk), 0), static_cast<ssize_t>(sizeof(on_disk)));
    close(fd);
    EXPECT_EQ(on_disk.dirty, 1u);
    EXPECT_EQ(on_disk.bitmap_section_offset, 0u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 2u);

    // Re-compaction in the same dirty session adds no further mark.
    catidx.AppendBitmapSection({{0u, {{"b", {4}}}}});
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 2u);
}

} // namespace
