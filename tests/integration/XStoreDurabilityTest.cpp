// Durability A (docs/XStore_Technical_Reference.md section 4.18), Task 1: header format + version bumps. Only the on-disk
// layout and version numbers are asserted here -- the `dirty` flag's actual
// set/clear/rebuild behavior arrives in later tasks (2-4), which extend this
// file.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_WalRing.h"
#include "XStore/XStore_WalCheckpoint.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_DirLock.h"
#include "XStoreTestSupport.h"
#include "XStoreSyscallFault.h"
#include "include/DataStructs/TimeStruct.h"
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {
using xstore_test::ScratchDir;

TEST(DurabilityHeaderLayoutTest, ZoneMapHeaderLayout) {
    EXPECT_EQ(sizeof(ZoneMapFileHeader), 24u);
    EXPECT_EQ(offsetof(ZoneMapFileHeader, dirty), 12u);
    EXPECT_EQ(ZONE_MAP_VERSION, 5u);

    ScratchDir scratch("durability_zmap_header_layout");
    std::string path = (scratch.test_root / "2024.zmap").string();

    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, /*row_length=*/64, /*tracked_column_count=*/64));

    ZoneMapFileHeader on_disk{};
    int fd = open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, &on_disk, sizeof(on_disk), 0), static_cast<ssize_t>(sizeof(on_disk)));
    close(fd);

    EXPECT_EQ(on_disk.dirty, 1u); // a new file opens dirty (Q2: indistinguishable from a crashed truncate)
    EXPECT_EQ(on_disk.version, 5u);
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

    EXPECT_EQ(on_disk.dirty, 1u); // a new file opens dirty (Q2)
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

// A new file is created dirty (it cannot be told from a rebuild's crashed truncate, see ZoneMapFile::Open) but
// without a synced dirty mark (SF1: no fdatasync in Open); MarkClean still cleans it. A clean existing file reopened
// without writes stays clean and costs no dirty mark.
TEST(DurabilityMarkDirtyTest, ZoneMapCleanAfterOpenWithoutWrites) {
    ScratchDir scratch("durability_zmap_clean_open");
    std::string path = (scratch.test_root / "2024.zmap").string();
    {
        ZoneMapFile created;
        ASSERT_TRUE(created.Open(path, 64, 4));
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 1u);
        EXPECT_EQ(created.DirtyMarkWritesForTests(), 0u);
        ASSERT_TRUE(created.MarkClean());
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(path), 0u);
    }
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

// Same as the zone map: created dirty without a sync, a clean existing file reopens clean.
TEST(DurabilityMarkDirtyTest, CatIdxCleanAfterOpenWithoutWrites) {
    ScratchDir scratch("durability_catidx_clean_open");
    std::string path = (scratch.test_root / "2024.catidx").string();
    {
        CatIdxFile created;
        ASSERT_TRUE(created.Open(path, 64, 4));
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
        EXPECT_EQ(created.DirtyMarkWritesForTests(), 0u);
        ASSERT_TRUE(created.MarkClean());
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 0u);
    }
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

// SF1: opening a NEW index file performs no fdatasync (Open runs under the global registry mutex); the dirty header is
// made durable by the first write, under the file's own mutex.
TEST(DurabilityMarkDirtyTest, NewFileOpenDoesNotSyncAndFirstWriteMakesTheDirtyHeaderDurable) {
    ScratchDir scratch("durability_new_file_no_sync");
    const std::string zpath = (scratch.test_root / "2024.zmap").string();
    const std::string cpath = (scratch.test_root / "2024.catidx").string();
    {
        xstore_fault::ScopedFdFault syncs(xstore_fault::FdOp::kSync, zpath, 1000, 0, EIO); // counts, never fails
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(zpath, 64, 4));
        EXPECT_EQ(syncs.Calls(), 0) << "Open of a new .zmap must not fdatasync";
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(zpath), 1u);
        zmap.UpdateRow(0, ZmapObs(1.0), 1);
        EXPECT_EQ(syncs.Calls(), 1) << "the first write syncs the dirty header";
        EXPECT_EQ(zmap.DirtyMarkWritesForTests(), 1u);
        zmap.UpdateRow(1, ZmapObs(2.0), 1);
        EXPECT_EQ(syncs.Calls(), 1);
    }
    {
        xstore_fault::ScopedFdFault syncs(xstore_fault::FdOp::kSync, cpath, 1000, 0, EIO);
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(cpath, 64, 4));
        EXPECT_EQ(syncs.Calls(), 0) << "Open of a new .catidx must not fdatasync";
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(cpath), 1u);
        catidx.UpdateRow(0, CatObs(1));
        EXPECT_EQ(syncs.Calls(), 1) << "the first write syncs the dirty header";
        EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 1u);
        catidx.UpdateRow(1, CatObs(2));
        EXPECT_EQ(syncs.Calls(), 1);
    }
}

TEST(DurabilityMarkDirtyTest, CatIdxBitmapAppendMarksDirtyAndCommitCarriesIt) {
    ScratchDir scratch("durability_catidx_bitmap");
    std::string path = (scratch.test_root / "2024.catidx").string();
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, 64, 4));

    // Bitmap build path alone (no prior UpdateRow in this session) must mark dirty.
    catidx.AppendBitmapSection({{0u, {{"a", {1, 2, 3}}}}});
    EXPECT_TRUE(catidx.HasBitmapSection());
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 1u);

    ASSERT_TRUE(catidx.MarkClean());
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 0u);

    // A bloom write into the bitmapped file rewrites the header to invalidate
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

    // Re-bitmap build in the same dirty session adds no further mark.
    catidx.AppendBitmapSection({{0u, {{"b", {4}}}}});
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(path), 1u);
    EXPECT_EQ(catidx.DirtyMarkWritesForTests(), 2u);
}

// ---- Task 3: dirty index => rebuild at startup (DD4/DD5) ------------------

// Rows 1000..1299 (one year), column 0 = double 0..299. "Crash" is simulated
// by dropping the registries without MarkClean and zeroing the index bodies
// (lost index writes); InitDB is the restart.
class DurabilityRebuildTest : public ::testing::Test {
protected:
    static constexpr uint64_t kBaseEpoch = 1000;
    static constexpr uint64_t kRows = 300;
    static constexpr uint16_t kRowLength = 64;
    const std::string db_name = "DURREBUILD";
    // LiveWalShards()/CheckpointWal() hand-drive ring checkpoints: no background checkpointer.
    xstore_test::ScopedCheckpointerPause checkpointer_paused_;
    std::unique_ptr<ScratchDir> scratch;
    unsigned short year = 0;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_durability_rebuild_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        year = TimeStruct(kBaseEpoch, XStore_Proto::SECOND).year;
    }
    void TearDown() override {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    // Rings whose WAL file lives under this test's scratch dir.
    std::vector<std::shared_ptr<walring::Ring>> MyRings() const {
        std::vector<std::shared_ptr<walring::Ring>> mine;
        for (auto &ring : walring::AllRings()) {
            if (ring->path().rfind(scratch->test_root.string(), 0) == 0) mine.push_back(ring);
        }
        return mine;
    }

    std::string DbPath() const { return (scratch->test_root / "DataStore" / db_name).string(); }
    std::string Path(const char *ext) const { return DbPath() + "/" + std::to_string(year) + ext; }

    void InsertRows(XStore &store) {
        xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
        for (uint64_t i = 0; i < kRows; ++i) {
            XStore_Proto::RowData row;
            row.add_fielddata()->set_doubledata(static_cast<double>(i));
            XStore_Proto::UnaryInsert_REP rep;
            store.UnaryInsert(kBaseEpoch + i, row, db_name, &rep, XStore_Proto::SYNC);
        }
        // UnaryInsert preallocates the whole calendar year (31.5M rows); the
        // rebuild scans every one. Keep only the rows written (runtime only).
        int fd = open(Path(".XSTORE").c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, static_cast<off_t>((kBaseEpoch + kRows) * kRowLength)), 0);
        close(fd);
    }

    // Checkpoints every WAL ring (checkpoint_lsn := head_lsn, persisted in the
    // ring header) so InitDB's WAL replay cannot force a rebuild -- these tests
    // isolate the dirty flag as the only trigger.
    void CheckpointWal() {
        for (auto &ring : MyRings()) ring->AdvanceCheckpoint(ring->HeadLsn());
    }

    // What a clean shutdown does: data file synced first, then both indexes.
    void CleanShutdown() {
        int fd = open(Path(".XSTORE").c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(fdatasync(fd), 0);
        close(fd);
        ASSERT_TRUE(XStore::GetOrOpenZoneMap(DbPath(), year, kRowLength).MarkClean());
        ASSERT_TRUE(XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength).MarkClean());
    }

    // Number of WAL rings still holding un-checkpointed records, i.e. that
    // InitDB's replay would still have to consult.
    size_t LiveWalShards() const {
        size_t live = 0;
        for (auto &ring : MyRings()) {
            if (ring->HeadLsn() > ring->CheckpointLsn()) ++live;
        }
        return live;
    }

    void DropRegistriesAndZeroBody(const std::string &path, size_t header_size, bool checkpoint_wal = true) {
        if (checkpoint_wal) CheckpointWal();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        int fd = open(path.c_str(), O_RDWR);
        ASSERT_GE(fd, 0);
        off_t size = lseek(fd, 0, SEEK_END);
        ASSERT_GT(size, static_cast<off_t>(header_size));
        std::vector<char> zeros(static_cast<size_t>(size) - header_size, 0);
        ASSERT_EQ(pwrite(fd, zeros.data(), zeros.size(), static_cast<off_t>(header_size)),
                  static_cast<ssize_t>(zeros.size()));
        close(fd);
    }

    ZoneMapEntry ReadZmapEntry0() const {
        ZoneMapEntry e{};
        int fd = open(Path(".zmap").c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        EXPECT_EQ(pread(fd, &e, sizeof(e), sizeof(ZoneMapFileHeader)), static_cast<ssize_t>(sizeof(e)));
        close(fd);
        return e;
    }

    // Returns the timestamps FindByValue reports for column 0 == value.
    std::vector<uint64_t> FindDouble(XStore &store, double value) {
        std::string buf;
        store.FindByValue(kBaseEpoch, kBaseEpoch + kRows - 1, db_name, 0, xstore_test::FDouble(value),
                          XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buf += chunk; });
        XStore_Proto::RangeQuery_REP rep;
        EXPECT_TRUE(rep.ParseFromString(buf));
        std::vector<uint64_t> ts;
        for (int i = 0; i < rep.queryresponse_size(); ++i) ts.push_back(rep.queryresponse(i).timestamp());
        return ts;
    }
};

TEST_F(DurabilityRebuildTest, DirtyZoneMapIsRebuiltAtStartupAndEndsClean) {
    XStore store;
    InsertRows(store);
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u) << "precondition: no MarkClean => dirty";
    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
    ASSERT_EQ(ReadZmapEntry0().valid_count, 0u) << "precondition: stats tampered";

    store.InitDB();

    ZoneMapEntry e = ReadZmapEntry0();
    EXPECT_EQ(e.valid_count, kRows);
    XStore_Proto::Min_REP min_rep;
    XStore_Proto::Max_REP max_rep;
    store.MinAggregate(kBaseEpoch, kBaseEpoch + kRows - 1, db_name, 0, &min_rep);
    store.MaxAggregate(kBaseEpoch, kBaseEpoch + kRows - 1, db_name, 0, &max_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 0.0);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 299.0);
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u) << "rebuild must MarkClean after syncing data";
}

TEST_F(DurabilityRebuildTest, CleanZoneMapIsNotRebuiltAtStartup) {
    XStore store;
    InsertRows(store);
    CleanShutdown();
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));

    store.InitDB();

    // Tampered stats survive => InitDB trusted the clean flag and skipped the rebuild.
    EXPECT_EQ(ReadZmapEntry0().valid_count, 0u);
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
}

TEST_F(DurabilityRebuildTest, DirtyCatIdxIsRebuiltAtStartupAndEndsClean) {
    XStore store;
    InsertRows(store);
    ASSERT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 1u) << "precondition: no MarkClean => dirty";
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));

    store.InitDB();

    EXPECT_EQ(FindDouble(store, 137.0), (std::vector<uint64_t>{kBaseEpoch + 137}));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u) << "rebuild must MarkClean after syncing data";
}

TEST_F(DurabilityRebuildTest, CleanCatIdxIsNotRebuiltAtStartup) {
    XStore store;
    InsertRows(store);
    CleanShutdown();
    ASSERT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));

    store.InitDB();

    // Zeroed bloom survives => no rebuild: the bloom reports definite absence.
    XStore_Proto::FData val = xstore_test::FDouble(137.0);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength).MightContain(0, 0, EncodeFDataForCatIdx(val)));
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
}

TEST_F(DurabilityRebuildTest, DirtyIndexesOverYearWithNoRowsRebuildToCleanWithoutError) {
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    {
        int fd = open(Path(".XSTORE").c_str(), O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, 10 * kRowLength), 0); // all sparse holes, no rows
        close(fd);
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(Path(".zmap"), kRowLength, 64));
        zmap.UpdateRow(0, ZmapObs(1.0), 1);
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(Path(".catidx"), kRowLength, 64));
        catidx.UpdateRow(0, CatObs(1));
    }
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u);
    ASSERT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 1u);

    store.InitDB();

    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
    EXPECT_EQ(std::filesystem::file_size(Path(".zmap")), sizeof(ZoneMapFileHeader)) << "no rows => header-only zmap";
}

// Rows at both ends and in the middle of the full (sparse) year, no ftruncate:
// the hole-skipping rebuild scan must not lose an extent-edge row.
TEST_F(DurabilityRebuildTest, RebuildOverSparseYearKeepsRowsAtBothEnds) {
    const std::vector<uint64_t> epochs = {kBaseEpoch, kBaseEpoch + 63, kBaseEpoch + 64, 15000000, 31535999};
    XStore store;
    xstore_test::CreateTestDb(store, db_name, XStore_Proto::SECOND, kRowLength);
    for (size_t i = 0; i < epochs.size(); ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_doubledata(10.0 * static_cast<double>(i + 1));
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(epochs[i], row, db_name, &rep, XStore_Proto::SYNC);
    }
    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));

    store.InitDB();

    XStore_Proto::Min_REP min_rep;
    XStore_Proto::Max_REP max_rep;
    XStore_Proto::Sum_REP sum_rep;
    store.MinAggregate(0, 31535999, db_name, 0, &min_rep);
    store.MaxAggregate(0, 31535999, db_name, 0, &max_rep);
    store.SumAggregate(0, 31535999, db_name, 0, &sum_rep);
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 10.0);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 50.0);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), 150.0);
    for (size_t i = 0; i < epochs.size(); ++i) {
        std::string buf;
        store.FindByValue(0, 31535999, db_name, 0, xstore_test::FDouble(10.0 * static_cast<double>(i + 1)),
                          XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buf += chunk; });
        XStore_Proto::RangeQuery_REP rep;
        ASSERT_TRUE(rep.ParseFromString(buf));
        ASSERT_EQ(rep.queryresponse_size(), 1) << "epoch " << epochs[i];
        EXPECT_EQ(rep.queryresponse(0).timestamp(), epochs[i]);
    }
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
}

// ---- Rebuild completion failure branches -----------------------------------
//
// A rebuild may clear an index's dirty flag only after the data file and the index itself are synced. Each
// failing step must leave the index marked dirty so the next start rebuilds it again. (Not covered here: the 10 s
// drain-timeout _exit(1) and the second-signal _exit(1) of shutdown, which need a signal test.)

class DurabilityRebuildFaultTest : public DurabilityRebuildTest {
protected:
    // Restart #1 with every fdatasync of `fault_ext`'s file failing; restart #2 without the fault, after the bodies
    // of the indexes that were left dirty were lost again. Both restarts must end with correct, clean indexes
    // wherever the fault could have touched them.
    void RebuildUnderSyncFault(const char *fault_ext) {
        const bool data_fault = std::string(fault_ext) == ".XSTORE";
        const bool zmap_dirty_expected = data_fault || std::string(fault_ext) == ".zmap";
        const bool catidx_dirty_expected = data_fault || std::string(fault_ext) == ".catidx";
        XStore store;
        InsertRows(store);
        DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
        DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));
        {
            xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, Path(fault_ext), /*skip_calls=*/0, /*fail_calls=*/1000, EIO);
            store.InitDB();
            EXPECT_GT(fail.Failures(), 0) << "the fault never hit a " << fault_ext << " sync";
        }
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), zmap_dirty_expected ? 1u : 0u);
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), catidx_dirty_expected ? 1u : 0u);

        if (zmap_dirty_expected) DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
        if (catidx_dirty_expected) DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));
        store.InitDB();
        EXPECT_EQ(ReadZmapEntry0().valid_count, kRows) << "the dirty zone map must have been rebuilt on the next start";
        EXPECT_EQ(FindDouble(store, 137.0), (std::vector<uint64_t>{kBaseEpoch + 137}));
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
    }

    // Incomplete scan: one data-file read fails part-way through the rebuild's row scan (one scan serves both
    // indexes since stage 2a). The rows the scan never saw are missing, so the index must stay dirty. Only the index
    // under test is stale (its sidecar deleted after a clean stop); the other is clean and must not be touched. A zero
    // row at kFarRow gives the file a second data extent, so the scan is two preads (rows near 0, then rows near
    // kFarRow): skip 1 fails the second one, after the first extent was read. (The first-pread case is
    // FirstDataReadFailureLeavesBothIndexesDirty.) Restart #2, without the fault, must rebuild it correctly.
    void RebuildWithOneFailedDataRead(bool zmap_stale) {
        constexpr uint64_t kFarRow = 20000;
        XStore store;
        InsertRows(store);
        {
            const int fd = open(Path(".XSTORE").c_str(), O_WRONLY);
            ASSERT_GE(fd, 0);
            const std::vector<char> zero_row(kRowLength, 0);
            ASSERT_EQ(pwrite(fd, zero_row.data(), zero_row.size(), static_cast<off_t>(kFarRow * kRowLength)),
                      static_cast<ssize_t>(zero_row.size()));
            close(fd);
        }
        CleanShutdown();
        CheckpointWal();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        std::filesystem::remove(Path(zmap_stale ? ".zmap" : ".catidx"));
        {
            xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, Path(".XSTORE"), /*skip_calls=*/1,
                                             /*fail_calls=*/1, EIO);
            store.InitDB();
            ASSERT_EQ(fail.Failures(), 1) << "the fault never hit the data-file scan";
        }
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), zmap_stale ? 1u : 0u);
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), zmap_stale ? 0u : 1u);

        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        store.InitDB();
        EXPECT_EQ(ReadZmapEntry0().valid_count, kRows);
        EXPECT_EQ(FindDouble(store, 137.0), (std::vector<uint64_t>{kBaseEpoch + 137}));
        EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
        EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
    }
};

TEST_F(DurabilityRebuildFaultTest, FailedDataFdatasyncLeavesBothIndexesDirty) { RebuildUnderSyncFault(".XSTORE"); }
TEST_F(DurabilityRebuildFaultTest, FailedZoneMapFdatasyncLeavesItDirty) { RebuildUnderSyncFault(".zmap"); }
TEST_F(DurabilityRebuildFaultTest, FailedCatIdxFdatasyncLeavesItDirty) { RebuildUnderSyncFault(".catidx"); }

TEST_F(DurabilityRebuildFaultTest, IncompleteScanInTheZoneMapRebuildLeavesItDirty) {
    RebuildWithOneFailedDataRead(/*zmap_stale=*/true);
}
TEST_F(DurabilityRebuildFaultTest, IncompleteScanInTheCatIdxRebuildLeavesItDirty) {
    RebuildWithOneFailedDataRead(/*zmap_stale=*/false);
}

// Every data-file read fails from the first one, so neither rebuild folds a single row. The freshly created (empty)
// indexes must still be dirty on disk: an empty index that reads clean would be trusted by the next start.
TEST_F(DurabilityRebuildFaultTest, FirstDataReadFailureLeavesBothIndexesDirty) {
    XStore store;
    InsertRows(store);
    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, Path(".XSTORE"), /*skip_calls=*/0,
                                         /*fail_calls=*/1000, EIO);
        store.InitDB();
        ASSERT_GT(fail.Failures(), 0) << "the fault never hit the data-file scan";
    }
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 1u);

    XStore::ClearZoneMapRegistryForTests();  // the files hold only a header: nothing to zero
    XStore::ClearCatIdxRegistryForTests();
    store.InitDB();
    EXPECT_EQ(ReadZmapEntry0().valid_count, kRows);
    EXPECT_EQ(FindDouble(store, 137.0), (std::vector<uint64_t>{kBaseEpoch + 137}));
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
}

TEST_F(DurabilityRebuildFaultTest, DataFileReadErrorMakesTheRowScanReportIncomplete) {
    XStore store;
    InsertRows(store);
    const int fd = open(Path(".XSTORE").c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    uint64_t visited = 0;
    auto count_rows = [&](uint64_t, const char *) { ++visited; };
    ASSERT_TRUE(ForEachDataRow(fd, kBaseEpoch + kRows, kRowLength, count_rows)) << "control: unfaulted scan";
    EXPECT_GE(visited, kRows);
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, Path(".XSTORE"), 0, 1000, EIO);
        visited = 0;
        EXPECT_FALSE(ForEachDataRow(fd, kBaseEpoch + kRows, kRowLength, count_rows));
        EXPECT_GT(fail.Failures(), 0);
        EXPECT_EQ(visited, 0u);
    }
    close(fd);
}

// MarkClean's three steps one by one: index fdatasync, header pwrite, header fdatasync. Each failure reports false and
// keeps the in-memory flag dirty so a retry writes the header again. (After a failed header fdatasync the page cache
// may already hold the clean header: harmless, the index body was synced by the first step.)
template <typename Header, typename IndexFile>
void ExpectMarkCleanFailuresReportAndRetrySucceeds(IndexFile &index, const std::string &path) {
    using xstore_fault::FdOp;
    using xstore_fault::ScopedFdFault;
    ASSERT_EQ(OnDiskDirty<Header>(path), 1u) << "precondition: an UpdateRow marked it dirty";
    {
        ScopedFdFault fail(FdOp::kSync, path, 0, 1, EIO);
        EXPECT_FALSE(index.MarkClean()) << "index fdatasync";
        EXPECT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(OnDiskDirty<Header>(path), 1u);
    {
        ScopedFdFault fail(FdOp::kPwrite, path, 0, 1, EIO);
        EXPECT_FALSE(index.MarkClean()) << "header pwrite";
        EXPECT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(OnDiskDirty<Header>(path), 1u);
    {
        // No on-disk assertion: the clean header already reached the page cache (the pwrite succeeded), and a
        // failed fdatasync does not undo it, so a read-back sees dirty == 0 here.
        ScopedFdFault fail(FdOp::kSync, path, 1, 1, EIO);
        EXPECT_FALSE(index.MarkClean()) << "header fdatasync";
        EXPECT_EQ(fail.Failures(), 1);
    }
    EXPECT_TRUE(index.MarkClean()) << "retry";
    EXPECT_EQ(OnDiskDirty<Header>(path), 0u);
}

TEST(DurabilityMarkCleanFaultTest, ZoneMapMarkCleanFailuresKeepItDirtyUntilARetrySucceeds) {
    ScratchDir scratch("durability_zmap_markclean_fault");
    const std::string path = (scratch.test_root / "2024.zmap").string();
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open(path, 64, 4));
    zmap.UpdateRow(0, ZmapObs(1.0), 1);
    ExpectMarkCleanFailuresReportAndRetrySucceeds<ZoneMapFileHeader>(zmap, path);
}

TEST(DurabilityMarkCleanFaultTest, CatIdxMarkCleanFailuresKeepItDirtyUntilARetrySucceeds) {
    ScratchDir scratch("durability_catidx_markclean_fault");
    const std::string path = (scratch.test_root / "2024.catidx").string();
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open(path, 64, 64));
    catidx.UpdateRow(0, CatObs(1));
    ExpectMarkCleanFailuresReportAndRetrySucceeds<CatIdxFileHeader>(catidx, path);
}

// ---- Task 4: XStore::Shutdown() (DD7) --------------------------------------

// Shutdown() stops the process-wide WAL writers; ResetShutdownForTests()
// revives them so later tests in the same process can still insert.
class DurabilityShutdownTest : public DurabilityRebuildTest {
protected:
    void TearDown() override {
        XStore::ResetShutdownForTests();
        DurabilityRebuildTest::TearDown();
    }
};

TEST_F(DurabilityShutdownTest, ShutdownMarksBothIndexesCleanSoInitDbSkipsRebuild) {
    XStore store;
    InsertRows(store);
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u) << "precondition: inserts dirty the zmap";
    ASSERT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 1u) << "precondition: inserts dirty the catidx";

    XStore::Shutdown();

    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);

    // Tamper both index bodies; the clean flags must make InitDB trust them.
    // (DropRegistriesAndZeroBody also checkpoints the WAL: a live WAL record
    // would force a rebuild of its year regardless of the flag.)
    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader));
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader));
    store.InitDB();

    EXPECT_EQ(ReadZmapEntry0().valid_count, 0u) << "zmap was rebuilt despite a clean header";
    XStore_Proto::FData val = xstore_test::FDouble(137.0);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength).MightContain(0, 0, EncodeFDataForCatIdx(val)))
        << "catidx was rebuilt despite a clean header";
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
}

// A graceful Shutdown()'s final checkpoint cycle passes every record, so the
// next start replays nothing and therefore force-rebuilds nothing. Unlike the
// test above, nothing here calls CheckpointWal().
TEST_F(DurabilityShutdownTest, ShutdownInvalidatesWalSoNextStartDoesNotRebuild) {
    XStore store;
    InsertRows(store);
    ASSERT_GT(LiveWalShards(), 0u) << "precondition: a SYNC insert leaves its batch in a WAL shard";

    XStore::Shutdown();

    EXPECT_EQ(LiveWalShards(), 0u) << "Shutdown must checkpoint every WAL ring it knows to be fully applied+synced";

    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader), /*checkpoint_wal=*/false);
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader), /*checkpoint_wal=*/false);
    store.InitDB();

    EXPECT_EQ(ReadZmapEntry0().valid_count, 0u) << "zmap was force-rebuilt: the WAL was replayed";
    XStore_Proto::FData val = xstore_test::FDouble(137.0);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength).MightContain(0, 0, EncodeFDataForCatIdx(val)))
        << "catidx was force-rebuilt: the WAL was replayed";
}

// Control for the test above: same shape, but the process is killed instead of
// shut down. The indexes are marked clean by hand (so the dirty flag cannot be
// what triggers a rebuild), the WAL ring still holds the batch, and the next
// start DOES replay it and force-rebuild the year it touched.
TEST_F(DurabilityShutdownTest, WithoutShutdownNextStartReplaysAndForceRebuilds) {
    XStore store;
    InsertRows(store);
    CleanShutdown();   // data synced + both indexes MarkClean; the WAL is NOT checkpointed
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);
    ASSERT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 0u);
    ASSERT_GT(LiveWalShards(), 0u) << "precondition: the ring still holds the batch";

    DropRegistriesAndZeroBody(Path(".zmap"), sizeof(ZoneMapFileHeader), /*checkpoint_wal=*/false);
    DropRegistriesAndZeroBody(Path(".catidx"), sizeof(CatIdxFileHeader), /*checkpoint_wal=*/false);
    ASSERT_EQ(ReadZmapEntry0().valid_count, 0u) << "precondition: stats tampered";
    store.InitDB();

    EXPECT_EQ(ReadZmapEntry0().valid_count, kRows) << "zmap must be force-rebuilt: the WAL was replayed";
    XStore_Proto::FData val = xstore_test::FDouble(137.0);
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(DbPath(), year, kRowLength).MightContain(0, 0, EncodeFDataForCatIdx(val)))
        << "catidx must be force-rebuilt: the WAL was replayed";
}

// A WAL record whose data file cannot be synced must stay replayable. The
// checkpointer's data-sync fault hook fails the final cycle (retryable), so
// the ring checkpoint stays behind the head and the indexes stay dirty. (A
// MISSING file is different: the checkpointer treats ENOENT as a deleted DB
// and moves on -- see WalCheckpointTest.DeletedDataFileDoesNotBlockTheCheckpoint.)
TEST_F(DurabilityShutdownTest, ShutdownLeavesWalIntactWhenADataFileCannotBeSynced) {
    XStore store;
    InsertRows(store);
    const size_t live_before = LiveWalShards();
    ASSERT_GT(live_before, 0u);
    walring::Checkpointer::Instance().FailNextDataSyncsForTests(1000);   // the checkpointer thread is paused: only the final cycle syncs

    XStore::Shutdown();
    walring::Checkpointer::Instance().FailNextDataSyncsForTests(0);

    EXPECT_EQ(LiveWalShards(), live_before) << "WAL was invalidated although its data file could not be synced";
    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u) << "index marked clean over an unsynced data file";
    EXPECT_EQ(OnDiskDirty<CatIdxFileHeader>(Path(".catidx")), 1u);
}

TEST_F(DurabilityShutdownTest, SecondShutdownIsANoOp) {
    XStore store;
    InsertRows(store);
    XStore::Shutdown();
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 0u);

    // Re-dirty the zmap behind Shutdown's back; a second Shutdown that did
    // real work would MarkClean it again.
    XStore::GetOrOpenZoneMap(DbPath(), year, kRowLength).UpdateRow(0, ZmapObs(1.0), 1);
    ASSERT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u);

    XStore::Shutdown();

    EXPECT_EQ(OnDiskDirty<ZoneMapFileHeader>(Path(".zmap")), 1u);
}

TEST_F(DurabilityShutdownTest, InsertsAfterShutdownAreRejected) {
    XStore store;
    InsertRows(store);
    XStore::Shutdown();
    ASSERT_TRUE(XStore::IsShutDown());

    XStore_Proto::RowData row;
    row.add_fielddata()->set_doubledata(1.0);

    XStore_Proto::UnaryInsert_REP unary_rep;
    store.UnaryInsert(kBaseEpoch + kRows, row, db_name, &unary_rep, XStore_Proto::SYNC);
    EXPECT_EQ(unary_rep.insertstatus().rfind("[ERROR]", 0), 0u) << unary_rep.insertstatus();
    EXPECT_NE(unary_rep.insertstatus().find("shut down"), std::string::npos);

    XStore_Proto::BatchInsert_REQ batch_req;
    batch_req.set_dbname(db_name);
    batch_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::BatchInsert_DATA *entry = batch_req.add_batchinsert_data();
    entry->set_timestamp(kBaseEpoch + kRows);
    *entry->mutable_data() = row;
    XStore_Proto::BatchInsert_REP batch_rep;
    store.BatchInsert(&batch_req, &batch_rep);
    EXPECT_EQ(batch_rep.insertstatus().rfind("[ERROR]", 0), 0u) << batch_rep.insertstatus();

    XStore_Proto::RangeInsert_REQ range_req;
    range_req.set_dbname(db_name);
    range_req.set_durabilitymode(XStore_Proto::SYNC);
    XStore_Proto::RangeInsert_DATA *range_entry = range_req.add_rangeinsert_data();
    range_entry->set_timestamp(kBaseEpoch + kRows);
    *range_entry->mutable_data() = row;
    XStore_Proto::RangeInsert_REP range_rep;
    store.RangeInsert(&range_req, &range_rep);
    EXPECT_EQ(range_rep.insertstatus().rfind("[ERROR]", 0), 0u) << range_rep.insertstatus();
}

// One XStore process per data directory (XStore_DirLock.h). A second process is simulated by taking the
// flock on the lock file from a separate open file description, which conflicts exactly like another process.
namespace fs = std::filesystem;

class DirLockTest : public ::testing::Test {
protected:
    ScratchDir scratch{"dirlock"};
    fs::path data_dir = scratch.test_root / "DataStore";

    static size_t OpenFdCount() {
        size_t n = 0;
        for (const auto &entry : fs::directory_iterator("/proc/self/fd")) { (void)entry; ++n; }
        return n;
    }
};

TEST_F(DirLockTest, AcquireIsIdempotentPerProcessAndRecordsTheOwnerPid) {
    const dirlock::Result first = dirlock::Acquire(data_dir);
    ASSERT_TRUE(first.ok) << first.error;
    EXPECT_TRUE(first.warning.empty()) << first.warning;
    // Same directory, other spellings (trailing '/', a symlink): still held by this process, still a success.
    EXPECT_TRUE(dirlock::Acquire(data_dir.string() + "/").ok);
    const fs::path link = scratch.test_root / "alias";
    fs::create_directory_symlink(data_dir, link);
    EXPECT_TRUE(dirlock::Acquire(link).ok);
    EXPECT_EQ(dirlock::LockPathFor(link), dirlock::LockPathFor(data_dir));

    // Two instances in one process (one per worker thread in production) do not conflict with themselves.
    EXPECT_NO_THROW({ XStore a; XStore b; });

    // The lock is really held: another open file description cannot take it, and the file names the owner.
    const fs::path lock_path = dirlock::LockPathFor(data_dir);
    EXPECT_EQ(lock_path, fs::path(fs::weakly_canonical(data_dir).string() + ".lock"));
    const int fd = open(lock_path.c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    EXPECT_NE(flock(fd, LOCK_EX | LOCK_NB), 0);
    EXPECT_EQ(errno, EWOULDBLOCK);
    close(fd);
    std::ifstream in(lock_path);
    long pid = 0;
    in >> pid;
    EXPECT_EQ(pid, static_cast<long>(getpid()));
}

TEST_F(DirLockTest, DirectoryOwnedByAnotherProcessIsRefusedWithoutSideEffects) {
    const fs::path lock_path = dirlock::LockPathFor(data_dir);
    const int other = open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    ASSERT_GE(other, 0);
    ASSERT_EQ(flock(other, LOCK_EX | LOCK_NB), 0);

    const dirlock::Result refused = dirlock::Acquire(data_dir);
    EXPECT_FALSE(refused.ok);
    EXPECT_NE(refused.error.find(fs::weakly_canonical(data_dir).string()), std::string::npos) << refused.error;
    EXPECT_NE(refused.error.find("another XStore process"), std::string::npos) << refused.error;

    // The constructor throws the same message, and runs no startup work: no WAL directory, no rings.
    try {
        XStore store;
        ADD_FAILURE() << "constructing XStore on a directory another process owns did not throw";
    } catch (const std::runtime_error &e) {
        EXPECT_NE(std::string(e.what()).find(fs::weakly_canonical(data_dir).string()), std::string::npos) << e.what();
    }
    EXPECT_TRUE(fs::is_empty(data_dir)) << "a refused start must not touch the data directory (no WAL, no rings)";
    EXPECT_FALSE(dirlock::Acquire(data_dir).ok) << "a refusal must not be remembered as ownership";

    // Once the other owner is gone the very same directory starts: the failed attempt left nothing behind
    // (in particular it is not in the constructor's initialized-directories set).
    close(other);
    EXPECT_TRUE(dirlock::Acquire(data_dir).ok);
    EXPECT_NO_THROW({ XStore store; });
}

// A lock file that cannot be opened read-write (left by another user, read-only mount) must not switch the rule off:
// flock() works on a read-only descriptor. The fault interposer fails open(O_RDWR) with EACCES on that one path
// whatever the uid (chmod would not bind root).
TEST_F(DirLockTest, LockFileThatCannotBeOpenedReadWriteIsStillLockedReadOnly) {
    const fs::path lock_path = dirlock::LockPathFor(data_dir);
    const int other = open(lock_path.c_str(), O_RDONLY | O_CREAT | O_CLOEXEC, 0444);
    ASSERT_GE(other, 0);
    ASSERT_EQ(flock(other, LOCK_EX | LOCK_NB), 0);
    {
        xstore_fault::ScopedPathFault fault(lock_path.string(), /*subtree=*/false, EACCES, xstore_fault::kOpenWrite);
        const dirlock::Result refused = dirlock::Acquire(data_dir);
        EXPECT_GT(fault.OpenHits(), 0u) << "the read-write open was not failed; the test proves nothing";
        EXPECT_FALSE(refused.ok) << "an unopenable-read-write lock file must not disable the rule";
        EXPECT_NE(refused.error.find("another XStore process"), std::string::npos) << refused.error;
    }
    close(other);

    // Nobody else holds it: this process takes the lock through the read-only descriptor.
    {
        xstore_fault::ScopedPathFault fault(lock_path.string(), false, EACCES, xstore_fault::kOpenWrite);
        const dirlock::Result taken = dirlock::Acquire(data_dir);
        EXPECT_TRUE(taken.ok) << taken.error;
        EXPECT_TRUE(taken.warning.empty()) << taken.warning;
        const int probe = open(lock_path.c_str(), O_RDONLY | O_CLOEXEC);
        ASSERT_GE(probe, 0);
        EXPECT_NE(flock(probe, LOCK_EX | LOCK_NB), 0) << "the read-only fallback did not hold the lock";
        close(probe);
    }
}

// Not even a read-only open works: the old behaviour, one warning, the rule not enforced for this process.
TEST_F(DirLockTest, LockFileThatCannotBeOpenedAtAllWarnsAndContinues) {
    const fs::path lock_path = dirlock::LockPathFor(data_dir);
    xstore_fault::ScopedPathFault fault(lock_path.string(), false, EACCES,
                                        xstore_fault::kOpenWrite | xstore_fault::kOpenRead);
    const dirlock::Result result = dirlock::Acquire(data_dir);
    EXPECT_TRUE(result.ok);
    EXPECT_NE(result.warning.find("is not enforced"), std::string::npos) << result.warning;
}

// The test binaries build hundreds of scratch directories in one process; a lock for a deleted one must not
// keep its fd for the rest of the run (the default soft limit is often 1024).
TEST_F(DirLockTest, LocksOfDeletedDirectoriesAreClosedOnTheNextAcquire) {
    ASSERT_TRUE(dirlock::Acquire(data_dir).ok);
    const size_t baseline = OpenFdCount();
    for (int i = 0; i < 40; ++i) {
        const fs::path dir = scratch.test_root / ("churn" + std::to_string(i));
        fs::create_directories(dir);
        ASSERT_TRUE(dirlock::Acquire(dir).ok);
        // Both sweep rules: lock file unlinked (directory still there) / directory deleted.
        if (i % 2 == 0) fs::remove(dirlock::LockPathFor(dir));
        else fs::remove_all(dir);
    }
    ASSERT_TRUE(dirlock::Acquire(scratch.test_root / "last").ok);
    EXPECT_LE(OpenFdCount(), baseline + 2) << "locks of deleted directories piled up";
}

} // namespace
