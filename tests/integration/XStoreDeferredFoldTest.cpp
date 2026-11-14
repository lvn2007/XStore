// FoldStoredRows (async indexer, stage 1): the observations derived from a row's STORED bytes must fold into the
// same zone map and bloom bytes as today's inline path. Oracles: BuildReferenceIndexes (the per-row UpdateRow
// reference of XStoreRebuildTest) and the sidecars the live insert path itself wrote.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_WireEncode.h"
#include "XStore/XStore_WalRing.h"
#include "XStore/XStore_PendingMap.h"
#include "XStore/XStore_Indexer.h"
#include "XStoreSyscallFault.h"
#include "XStoreTestSupport.h"
#include "XStoreRebuildTestSupport.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace {
using namespace xstore_test;
using namespace xstore_rebuild_test;

class XStoreDeferredFoldTest : public ::testing::Test {
protected:
    static constexpr uint16_t kRowLength = 1024;   // 2048 rows per block
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_deferred_fold_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        XStore::ClearLiveWidenRequestsForTests();
    }
    void TearDown() override {
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    std::string DbPath(const std::string &db) const { return (scratch->test_root / "DataStore" / db).string(); }
    std::string YearPath(const std::string &db, unsigned short year, const char *ext) const {
        return DbPath(db) + "/" + std::to_string(year) + ext;
    }
    static void CleanRestartState() {
        XStore::Shutdown();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
    }
    static void Insert(XStore &store, const std::string &db,
                       const std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> &rows) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[ts, row] : rows) {
            XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
            entry->set_timestamp(ts);
            *entry->mutable_data() = row;
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    }

    // The runs of consecutive non-hole rows of a year file (what an indexer would be handed).
    static std::vector<xstore::IndexRun> RunsOf(const std::string &data_path, uint16_t row_length) {
        std::vector<xstore::IndexRun> runs;
        const int fd = open(data_path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0) << data_path;
        const uint64_t total_rows = static_cast<uint64_t>(lseek(fd, 0, SEEK_END)) / row_length;
        EXPECT_TRUE(ForEachDataRow(fd, total_rows, row_length, [&](uint64_t r, const char *buf) {
            if (buf[0] == 0) return;
            if (!runs.empty() && runs.back().first_row + runs.back().row_count == r) {
                ++runs.back().row_count;
            } else {
                runs.push_back({r, 1});
            }
        }));
        close(fd);
        return runs;
    }

    // Fresh index files at (zmap_path, catidx_path), filled by FoldStoredRows only. `runs` null = RunsOf the data file;
    // `limit` 0 = the catidx width.
    static void DeferredBuild(XStore &store, const std::string &data_path, const std::string &zmap_path,
                              const std::string &catidx_path, uint16_t row_length, uint32_t width,
                              const std::vector<xstore::IndexRun> *runs = nullptr, uint32_t limit = 0) {
        std::filesystem::remove(zmap_path);
        std::filesystem::remove(catidx_path);
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(zmap_path, row_length, width));
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(catidx_path, row_length, width));
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0) << data_path;
        const std::vector<xstore::IndexRun> own_runs = runs != nullptr ? std::vector<xstore::IndexRun>() : RunsOf(data_path, row_length);
        ASSERT_TRUE(store.FoldStoredRows(fd, row_length, width, limit != 0 ? limit : catidx.TrackedColumnCount(),
                                         runs != nullptr ? *runs : own_runs, zmap, catidx));
        close(fd);
        ASSERT_TRUE(zmap.MarkClean());
        ASSERT_TRUE(catidx.MarkClean());
    }

    static void InsertDenseAndSparse(XStore &store, const std::string &db, unsigned short dense, unsigned short sparse) {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 5000; ++i) rows.emplace_back(Jan1(dense) + i, MixedRow(i, i % 50 == 0 ? 80 : 40 + static_cast<int>(i % 9)));
        Insert(store, db, rows);
        rows.clear();
        for (uint64_t r : {0ULL, 1ULL, 2047ULL, 2048ULL, 4095ULL, 4096ULL, 100000ULL, 100001ULL, 3000000ULL, 31535999ULL}) {
            rows.emplace_back(Jan1(sparse) + r, MixedRow(r, 1 + static_cast<int>(r % 7)));
        }
        Insert(store, db, rows);
    }

    // The per-row reference and the deferred fold over `runs` of one year file are byte-identical.
    static void ExpectRunsMatchReference(XStore &store, const std::string &data_path, const std::string &tag,
                                         const std::string &root, uint32_t width, const std::vector<xstore::IndexRun> &runs) {
        const std::string ref_zmap = root + "/ref_" + tag + ".zmap", ref_catidx = root + "/ref_" + tag + ".catidx";
        const std::string got_zmap = root + "/got_" + tag + ".zmap", got_catidx = root + "/got_" + tag + ".catidx";
        BuildReferenceIndexes(store, data_path, ref_zmap, ref_catidx, kRowLength, width);
        DeferredBuild(store, data_path, got_zmap, got_catidx, kRowLength, width, &runs);
        EXPECT_TRUE(ReadFileBytes(ref_zmap) == ReadFileBytes(got_zmap)) << tag << ": .zmap differs from the per-row reference";
        EXPECT_TRUE(ReadFileBytes(ref_catidx) == ReadFileBytes(got_catidx)) << tag << ": .catidx differs from the per-row reference";
    }
};

// Dense (mixed widths), sparse and damaged rows: the deferred fold equals the per-row reference, byte for byte.
TEST_F(XStoreDeferredFoldTest, MatchesThePerRowReferenceOnEveryDataShape) {
    const std::string db = "DEFERREDREF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kDense = 1971, kSparse = 1972, kDamaged = 1973;
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 5000; ++i) rows.emplace_back(Jan1(kDense) + i, MixedRow(i, i % 50 == 0 ? 80 : 40 + static_cast<int>(i % 9)));
        Insert(store, db, rows);
    }
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t r : {0ULL, 1ULL, 2047ULL, 2048ULL, 4095ULL, 4096ULL, 100000ULL, 100001ULL, 3000000ULL, 31535999ULL}) {
            rows.emplace_back(Jan1(kSparse) + r, MixedRow(r, 1 + static_cast<int>(r % 7)));
        }
        Insert(store, db, rows);
    }
    Insert(store, db, {{Jan1(kDamaged), MixedRow(1, 10)}});
    CleanRestartState();
    {
        const std::string path = YearPath(db, kDamaged, ".XSTORE");
        PwriteRow(path, 10, kRowLength, DoubleColumn(1.5) + std::string("\x12\x01\x41", 3) + std::string("\x0a\x02\x20\x07", 4));
        PwriteRow(path, 11, kRowLength, std::string("\x08\x05", 2) + DoubleColumn(2.5));
        PwriteRow(path, 12, kRowLength, DoubleColumn(3.5) + std::string("\x0a\xff\x0f", 3));
        PwriteRow(path, 14, kRowLength, std::string("\x0a\x03\x09\x01\x02", 5) + std::string("\x0a\x02\x20\x05", 4));
        PwriteRow(path, 15, kRowLength, DoubleColumn(6.5) + std::string("\xff\x01", 2));
        PwriteRow(path, 16, kRowLength, std::string("\x0a\x00\x0a\x00", 4) + DoubleColumn(7.5));
    }
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kDense, ".zmap")).tracked_column_count;
    ASSERT_EQ(width, 80u) << "the DB grew to its widest row";
    for (unsigned short y : {kDense, kSparse, kDamaged}) {
        SCOPED_TRACE(y);
        const std::string ref_zmap = (scratch->test_root / ("ref_" + std::to_string(y) + ".zmap")).string();
        const std::string ref_catidx = (scratch->test_root / ("ref_" + std::to_string(y) + ".catidx")).string();
        const std::string got_zmap = (scratch->test_root / ("got_" + std::to_string(y) + ".zmap")).string();
        const std::string got_catidx = (scratch->test_root / ("got_" + std::to_string(y) + ".catidx")).string();
        BuildReferenceIndexes(store, YearPath(db, y, ".XSTORE"), ref_zmap, ref_catidx, kRowLength, width);
        DeferredBuild(store, YearPath(db, y, ".XSTORE"), got_zmap, got_catidx, kRowLength, width);
        EXPECT_TRUE(ReadFileBytes(ref_zmap) == ReadFileBytes(got_zmap)) << ".zmap differs from the per-row reference";
        EXPECT_TRUE(ReadFileBytes(ref_catidx) == ReadFileBytes(got_catidx)) << ".catidx differs from the per-row reference";
    }
}

// FoldStoredRows is also handed holes: a single run spanning rows that were never written (whole 1 MiB slices of
// them included) and a run that ends past the end of the file (short and empty reads read as holes).
TEST_F(XStoreDeferredFoldTest, RunsSpanningHolesAndPastEofMatchTheReference) {
    const std::string db = "DEFERREDHOLES";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kDense = 1971, kSparse = 1972;
    InsertDenseAndSparse(store, db, kDense, kSparse);
    CleanRestartState();
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kDense, ".zmap")).tracked_column_count;
    const std::string root = scratch->test_root.string();
    // The runs together cover every written row once (the reference scans the whole file).
    const std::vector<xstore::IndexRun> far_rows = {{3000000, 1}, {31535999, 1}};
    auto with_far = [&](std::vector<xstore::IndexRun> runs, bool with_100000) {
        if (with_100000) runs.push_back({100000, 2});
        runs.insert(runs.end(), far_rows.begin(), far_rows.end());
        return runs;
    };
    // rows 0..4096 in one run: holes between the written rows, 5 slices
    ExpectRunsMatchReference(store, YearPath(db, kSparse, ".XSTORE"), "sparse_span", root, width, with_far({{0, 4097}}, true));
    // rows 0..100001 in one run: slices that are all holes (rows 4097..99999)
    ExpectRunsMatchReference(store, YearPath(db, kSparse, ".XSTORE"), "sparse_allholes", root, width, with_far({{0, 100002}}, false));
    // runs that start and end on holes (rows 2..4094)
    ExpectRunsMatchReference(store, YearPath(db, kSparse, ".XSTORE"), "sparse_inner", root, width,
                             with_far({{0, 2}, {2, 4093}, {4095, 2}}, true));
    // a run past EOF of the dense year: stale scratch bytes must not be read as rows
    const std::string dense_path = YearPath(db, kDense, ".XSTORE");
    const int fd = open(dense_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    const uint64_t total_rows = static_cast<uint64_t>(lseek(fd, 0, SEEK_END)) / kRowLength;
    close(fd);
    ASSERT_GE(total_rows, 5100u);
    // The written rows first (the scratch buffer then holds rows), then a run whose first slice straddles EOF and
    // whose later slices lie wholly past it: the bytes past EOF must read as holes, not as the previous slice's rows.
    ExpectRunsMatchReference(store, dense_path, "dense_eof", root, width, {{0, 5000}, {total_rows - 100, 3100}});
}

// A failed pread folds nothing of the slice and is reported.
TEST_F(XStoreDeferredFoldTest, ReportsAReadFailure) {
    XStore store;
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open((scratch->test_root / "bad.zmap").string(), kRowLength, 4));
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open((scratch->test_root / "bad.catidx").string(), kRowLength, 4));
    EXPECT_FALSE(store.FoldStoredRows(-1, kRowLength, 4, 4, {{0, 10}}, zmap, catidx));
}

// catidx_column_limit L: values in columns below L are folded, values at or above L are not (the bloom answers
// false for them), while the zone map still takes every column.
TEST_F(XStoreDeferredFoldTest, CatIdxColumnLimitFoldsOnlyTheColumnsBelowIt) {
    const std::string db = "DEFERREDLIMIT";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1974;
    const std::vector<int64_t> values = {1111111111111LL, 2222222222222LL, 3333333333333LL, 4444444444444LL};
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 3; ++i) {
            XStore_Proto::RowData row;
            for (int64_t v : values) row.add_fielddata()->set_int64data(v + static_cast<int64_t>(i));
            rows.emplace_back(Jan1(kYear) + i, row);
        }
        Insert(store, db, rows);
    }
    CleanRestartState();
    const std::string data_path = YearPath(db, kYear, ".XSTORE");
    const std::vector<xstore::IndexRun> runs = {{0, 3}};
    auto encoded = [](int64_t v) {
        XStore_Proto::FData f;
        f.set_int64data(v);
        return EncodeFDataForCatIdx(f);
    };
    for (uint32_t limit : {0u, 2u}) {
        SCOPED_TRACE(limit);
        const std::string zmap_path = (scratch->test_root / "lim.zmap").string();
        const std::string catidx_path = (scratch->test_root / "lim.catidx").string();
        DeferredBuild(store, data_path, zmap_path, catidx_path, kRowLength, 4, &runs, limit);
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(catidx_path, kRowLength, 4));
        for (uint32_t c = 0; c < 4; ++c) {
            const bool folded = limit == 0 || c < limit;
            EXPECT_EQ(catidx.MightContain(0, c, encoded(values[c] + 1)), folded) << "column " << c;
        }
    }
}

// The sidecars the LIVE insert path wrote (ApplyWalRecord's inline fold) equal the deferred fold's, for data
// whose width never changes and that is never overwritten or deleted. (The inline path's catidx column limit equals
// the file width for these 40-column rows, so the deferred fold's default limit = the catidx width is the same.)
TEST_F(XStoreDeferredFoldTest, MatchesTheLiveInlinePath) {
    const std::string db = "DEFERREDLIVE";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kDense = 1971, kSparse = 1972;
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 5000; ++i) rows.emplace_back(Jan1(kDense) + i, MixedRow(i, 40));
        Insert(store, db, rows);
    }
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t r : {0ULL, 2047ULL, 2048ULL, 100000ULL, 3000000ULL}) rows.emplace_back(Jan1(kSparse) + r, MixedRow(r, 33));
        Insert(store, db, rows);
    }
    CleanRestartState();
    for (unsigned short y : {kDense, kSparse}) {
        SCOPED_TRACE(y);
        const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, y, ".zmap")).tracked_column_count;
        const std::string live_zmap = ReadFileBytes(YearPath(db, y, ".zmap"));
        const std::string live_catidx = ReadFileBytes(YearPath(db, y, ".catidx"));
        const std::string got_zmap = (scratch->test_root / ("got_" + std::to_string(y) + ".zmap")).string();
        const std::string got_catidx = (scratch->test_root / ("got_" + std::to_string(y) + ".catidx")).string();
        DeferredBuild(store, YearPath(db, y, ".XSTORE"), got_zmap, got_catidx, kRowLength, width);
        // The files start with a non-zero header, so look for a non-zero byte in the DATA area: a header-only or
        // all-zero-data sidecar would compare equal to an empty fold and prove nothing.
        EXPECT_NE(live_catidx.find_first_not_of('\0', sizeof(CatIdxFileHeader)), std::string::npos) << "the live .catidx holds no bloom data: nothing was compared";
        EXPECT_NE(live_zmap.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "the live .zmap holds no block data: nothing was compared";
        EXPECT_TRUE(live_zmap == ReadFileBytes(got_zmap)) << ".zmap differs from the live insert path";
        EXPECT_TRUE(live_catidx == ReadFileBytes(got_catidx)) << ".catidx differs from the live insert path";
    }
}

// An entirely empty RowData serializes to a zero first byte, so every fold (the deferred one, and since the rows-only
// records the live insert path too, like every scan and the startup rebuild) treats it as a hole: the live sidecars
// equal the deferred fold's byte for byte, whether the runs split at the empty rows or span them.
TEST_F(XStoreDeferredFoldTest, AnAllEmptyRowIsAHoleOnTheLiveAndDeferredPaths) {
    const std::string db = "DEFERREDEMPTY";
    constexpr uint64_t kRows = 3000, kEmptyEvery = 10;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < kRows; ++i) {
            rows.emplace_back(Jan1(kYear) + i, i % kEmptyEvery == 0 ? XStore_Proto::RowData() : MixedRow(i, 40));
        }
        Insert(store, db, rows);
    }
    CleanRestartState();
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kYear, ".zmap")).tracked_column_count;
    // The runs an indexer is handed come from the WAL record's rows, so an all-empty row sits INSIDE a run; RunsOf (which
    // splits at every zero-first-byte row) never hands one over. Both must give the same answer.
    const std::vector<xstore::IndexRun> spanning = {{0, kRows}};
    const std::string live_zmap = ReadFileBytes(YearPath(db, kYear, ".zmap"));
    const std::string live_catidx = ReadFileBytes(YearPath(db, kYear, ".catidx"));
    ASSERT_NE(live_zmap.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "an empty zone map proves nothing";
    for (const std::vector<xstore::IndexRun> *runs : {static_cast<const std::vector<xstore::IndexRun> *>(nullptr), &spanning}) {
        SCOPED_TRACE(runs == nullptr ? "runs split at holes" : "one run spanning the empty rows");
        const std::string got_zmap = (scratch->test_root / "got.zmap").string();
        const std::string got_catidx = (scratch->test_root / "got.catidx").string();
        DeferredBuild(store, YearPath(db, kYear, ".XSTORE"), got_zmap, got_catidx, kRowLength, width, runs);
        EXPECT_TRUE(live_zmap == ReadFileBytes(got_zmap)) << ".zmap differs from the live insert path";
        EXPECT_TRUE(live_catidx == ReadFileBytes(got_catidx)) << ".catidx differs from the live insert path";
    }
}

}  // namespace

// The double and empty-column fast paths of AppendRebuildObservations must give byte-identical sidecars to the
// per-row reference for every double bit pattern, including the ones a long-double round trip could alter.
namespace {
std::string RawDoubleColumn(uint64_t bits) {
    std::string out = "\x0a\x09\x09";
    out.append(reinterpret_cast<const char *>(&bits), sizeof(bits));
    return out;
}
const std::string kEmptyColumn("\x0a\x00", 2);   // an unset (null) column
}  // namespace

TEST_F(XStoreDeferredFoldTest, DoubleAndEmptyColumnsOfEveryBitPatternMatchTheReference) {
    const std::string db = "LEANDOUBLES";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    Insert(store, db, {{Jan1(kYear), MixedRow(1, 8)}});   // creates the year file at the final width
    CleanRestartState();
    const std::string path = YearPath(db, kYear, ".XSTORE");
    const std::vector<uint64_t> patterns = {
        0x0000000000000000ULL,   // +0
        0x8000000000000000ULL,   // -0
        0x3FF0000000000000ULL,   // 1.0
        0xBFF0000000000000ULL,   // -1.0
        0x0000000000000001ULL,   // smallest subnormal
        0x000FFFFFFFFFFFFFULL,   // largest subnormal
        0x0010000000000000ULL,   // smallest normal
        0x7FEFFFFFFFFFFFFFULL,   // largest finite
        0xFFEFFFFFFFFFFFFFULL,   // most negative finite
        0x7FF0000000000000ULL,   // +inf
        0xFFF0000000000000ULL,   // -inf
        0x7FF8000000000000ULL,   // quiet NaN
        0x7FF4000000000000ULL,   // signaling NaN
        0xFFF0000000000001ULL,   // negative signaling NaN with a payload
        0x7FF8DEADBEEF1234ULL,   // quiet NaN with a payload
    };
    uint64_t row = 100;
    for (size_t i = 0; i < patterns.size(); ++i) {
        // Each pattern in column 0, then alone in a later column behind empty columns, then repeated.
        PwriteRow(path, row++, kRowLength, RawDoubleColumn(patterns[i]) + RawDoubleColumn(patterns[(i + 1) % patterns.size()]));
        PwriteRow(path, row++, kRowLength, kEmptyColumn + kEmptyColumn + RawDoubleColumn(patterns[i]) + kEmptyColumn);
        PwriteRow(path, row++, kRowLength, kEmptyColumn);   // an all-empty-columns row (non-zero first byte: 0x0a)
    }
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kYear, ".zmap")).tracked_column_count;
    const std::string ref_zmap = (scratch->test_root / "ref.zmap").string();
    const std::string ref_catidx = (scratch->test_root / "ref.catidx").string();
    const std::string got_zmap = (scratch->test_root / "got.zmap").string();
    const std::string got_catidx = (scratch->test_root / "got.catidx").string();
    BuildReferenceIndexes(store, path, ref_zmap, ref_catidx, kRowLength, width);
    DeferredBuild(store, path, got_zmap, got_catidx, kRowLength, width);
    EXPECT_TRUE(ReadFileBytes(ref_zmap) == ReadFileBytes(got_zmap)) << ".zmap differs from the per-row reference";
    EXPECT_TRUE(ReadFileBytes(ref_catidx) == ReadFileBytes(got_catidx)) << ".catidx differs from the per-row reference";
}

// Columns past the tracked width are validated but not recorded; a double or empty column there must behave the same.
TEST_F(XStoreDeferredFoldTest, DoubleAndEmptyColumnsPastTheTrackedWidthMatchTheReference) {
    const std::string db = "LEANWIDTH";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    Insert(store, db, {{Jan1(kYear), MixedRow(1, 8)}});
    CleanRestartState();
    const std::string path = YearPath(db, kYear, ".XSTORE");
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kYear, ".zmap")).tracked_column_count;
    for (uint64_t r = 0; r < 50; ++r) {
        std::string bytes;
        for (uint32_t c = 0; c < width + 20; ++c) {   // 20 columns beyond the tracked width: two doubles, then one empty column, repeating
            bytes += (c % 3 == 2) ? kEmptyColumn : RawDoubleColumn(0x4000000000000000ULL + r * 977 + c);
        }
        ASSERT_LE(bytes.size(), static_cast<size_t>(kRowLength)) << "the crafted row (" << bytes.size() << " B for " << width + 20 << " columns at tracked width " << width << ") must fit its slot of " << kRowLength << " B";
        PwriteRow(path, 10 + r, kRowLength, bytes);
    }
    const std::string ref_zmap = (scratch->test_root / "ref.zmap").string();
    const std::string ref_catidx = (scratch->test_root / "ref.catidx").string();
    const std::string got_zmap = (scratch->test_root / "got.zmap").string();
    const std::string got_catidx = (scratch->test_root / "got.catidx").string();
    BuildReferenceIndexes(store, path, ref_zmap, ref_catidx, kRowLength, width);
    DeferredBuild(store, path, got_zmap, got_catidx, kRowLength, width);
    EXPECT_TRUE(ReadFileBytes(ref_zmap) == ReadFileBytes(got_zmap));
    EXPECT_TRUE(ReadFileBytes(ref_catidx) == ReadFileBytes(got_catidx));
}

// The fast paths record a column only below the tracked count the caller passes, even when the index files are wider:
// FoldStoredRows(tracked 40) into files of width 64 equals the per-row reference at tracked 40 (no zone map entry for
// columns 40..63 beyond what the padding gives them).
TEST_F(XStoreDeferredFoldTest, DoubleAndEmptyColumnsPastTheCallersTrackedCountAreNotFoldedIntoWiderFiles) {
    const std::string db = "LEANNARROW";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    Insert(store, db, {{Jan1(kYear), MixedRow(1, 8)}});
    CleanRestartState();
    const std::string path = YearPath(db, kYear, ".XSTORE");
    const uint32_t file_width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kYear, ".zmap")).tracked_column_count;
    constexpr uint32_t kTracked = 40;
    ASSERT_GT(file_width, kTracked);
    for (uint64_t r = 0; r < 50; ++r) {
        std::string bytes;
        for (uint32_t c = 0; c < file_width; ++c) {
            bytes += (c % 3 == 2) ? kEmptyColumn : RawDoubleColumn(0x4000000000000000ULL + r * 977 + c);
        }
        ASSERT_LE(bytes.size(), static_cast<size_t>(kRowLength));
        PwriteRow(path, 10 + r, kRowLength, bytes);
    }
    const std::string ref_zmap = (scratch->test_root / "ref.zmap").string();
    const std::string ref_catidx = (scratch->test_root / "ref.catidx").string();
    const std::string got_zmap = (scratch->test_root / "got.zmap").string();
    const std::string got_catidx = (scratch->test_root / "got.catidx").string();
    {
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(ref_zmap, kRowLength, file_width));
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(ref_catidx, kRowLength, file_width));
        const int fd = open(path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        const uint64_t total_rows = static_cast<uint64_t>(lseek(fd, 0, SEEK_END)) / kRowLength;
        ASSERT_TRUE(ForEachDataRow(fd, total_rows, kRowLength, [&](uint64_t r, const char *buf) {
            if (buf[0] == 0) return;
            XStore_Proto::RowData row;
            const size_t real_len = XStoreWireEncode::RealRowDataLength(buf, kRowLength);
            if (real_len > 0) static_cast<void>(row.ParseFromArray(buf, static_cast<int>(real_len)));
            zmap.UpdateRow(r, RefZoneMapObservations(row, kTracked), kTracked);
            std::vector<CatIdxObservation> observations;
            for (uint32_t c = 0; c < kTracked; ++c) {
                XStore_Proto::FData f;
                if (store.ExtractColumnByIndexSkipParse(buf, kRowLength, static_cast<unsigned short>(c), f) &&
                    f.oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
                    observations.push_back({c, EncodeFDataForCatIdx(f), true});
                } else {
                    observations.push_back({c, std::string(), false});
                }
            }
            catidx.UpdateRow(r, observations);
        }));
        close(fd);
        ASSERT_TRUE(zmap.MarkClean());
        ASSERT_TRUE(catidx.MarkClean());
    }
    {
        ZoneMapFile zmap;
        ASSERT_TRUE(zmap.Open(got_zmap, kRowLength, file_width));
        CatIdxFile catidx;
        ASSERT_TRUE(catidx.Open(got_catidx, kRowLength, file_width));
        const int fd = open(path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        ASSERT_TRUE(store.FoldStoredRows(fd, kRowLength, kTracked, kTracked, RunsOf(path, kRowLength), zmap, catidx));
        close(fd);
        ASSERT_TRUE(zmap.MarkClean());
        ASSERT_TRUE(catidx.MarkClean());
    }
    EXPECT_TRUE(ReadFileBytes(ref_zmap) == ReadFileBytes(got_zmap)) << ".zmap differs from the per-row reference";
    EXPECT_TRUE(ReadFileBytes(ref_catidx) == ReadFileBytes(got_catidx)) << ".catidx differs from the per-row reference";
}

// A signaling-NaN double must give the same zone map bytes as the live insert path (which quiets it through the
// long double of ZoneMapObservationOf). ZoneMapFile does not skip NaN, so a NaN that is the first or last numeric
// observation of a column in a block lands in min/max/first/last. Every NaN case sits in a block other than block 0
// (2048 rows per block at kRowLength 1024) so that no earlier finite row masks it.
TEST_F(XStoreDeferredFoldTest, SignalingNaNDoublesMatchTheLiveInsertPath) {
    const std::string db = "DEFERREDNAN";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    constexpr uint64_t kBlockRows = 2048;
    constexpr uint64_t kPosSnan = 0x7FF4000000000000ULL, kNegSnan = 0xFFF0000000000001ULL, kQnanPayload = 0x7FF8DEADBEEF1234ULL;
    const auto from_bits = [](uint64_t bits) {
        double d;
        std::memcpy(&d, &bits, sizeof(d));
        return d;
    };
    // Bits of column `c` of row `r`, or 0 with `*set` false for an unset column.
    const auto cell = [&](uint64_t r, int c, bool *set) -> uint64_t {
        const uint64_t block = r / kBlockRows, in_block = r % kBlockRows;
        const bool first = in_block == 0, last = in_block == kBlockRows - 1, mid = in_block == 1000;
        *set = true;
        if (c == 0) {   // NaN as first and last of the block (blocks 1-3), alone first (6), alone last (7), middle (5)
            if (block == 1 && (first || last)) return kPosSnan;
            if (block == 2 && (first || last)) return kNegSnan;
            if (block == 3 && (first || last)) return kQnanPayload;
            if (block == 5 && mid) return kPosSnan;
            if (block == 6 && first) return kNegSnan;
            if (block == 7 && last) return kPosSnan;
        }
        if (c == 1) {   // block 4: the NaN is the ONLY numeric observation of the column in the block
            if (block == 4) {
                *set = in_block == 700;
                return *set ? kNegSnan : 0;
            }
        }
        if (c == 2 && block == 5 && mid) return kNegSnan;
        const double v = static_cast<double>(r) * 0.25 + c;
        uint64_t bits;
        std::memcpy(&bits, &v, sizeof(bits));
        return bits;
    };
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t r = 0; r < 8 * kBlockRows; ++r) {
        XStore_Proto::RowData row;
        for (int c = 0; c < 3; ++c) {
            bool set = true;
            const uint64_t bits = cell(r, c, &set);
            XStore_Proto::FData *f = row.add_fielddata();
            if (set) f->set_doubledata(from_bits(bits));
        }
        if (r == kBlockRows) {   // the bits must survive protobuf serialization, or the test proves nothing
            const std::string wire = row.SerializeAsString();
            ASSERT_GE(wire.size(), 11u);
            uint64_t on_wire;
            std::memcpy(&on_wire, wire.data() + 3, sizeof(on_wire));
            ASSERT_EQ(on_wire, kPosSnan);
        }
        rows.emplace_back(Jan1(kYear) + r, std::move(row));
    }
    for (size_t at = 0; at < rows.size(); at += 4000) {
        Insert(store, db, std::vector<std::pair<unsigned long long, XStore_Proto::RowData>>(
                              rows.begin() + at, rows.begin() + std::min(rows.size(), at + 4000)));
    }
    CleanRestartState();
    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kYear, ".zmap")).tracked_column_count;
    const std::string live_zmap = ReadFileBytes(YearPath(db, kYear, ".zmap"));
    const std::string live_catidx = ReadFileBytes(YearPath(db, kYear, ".catidx"));
    const std::string got_zmap = (scratch->test_root / "got.zmap").string();
    const std::string got_catidx = (scratch->test_root / "got.catidx").string();
    DeferredBuild(store, YearPath(db, kYear, ".XSTORE"), got_zmap, got_catidx, kRowLength, width);
    EXPECT_NE(live_zmap.find_first_not_of('\0', sizeof(ZoneMapFileHeader)), std::string::npos) << "nothing was compared";
    EXPECT_TRUE(live_zmap == ReadFileBytes(got_zmap)) << ".zmap differs from the live insert path";
    EXPECT_TRUE(live_catidx == ReadFileBytes(got_catidx)) << ".catidx differs from the live insert path";
}

// Stage 2 changes no behaviour: every inserted row is marked pending exactly once and cleared again by the inline
// fold, so nothing is ever left pending, and the sidecars are the ones the tests above compared.
TEST_F(XStoreDeferredFoldTest, InlineIndexingMarksEveryRowAndLeavesNothingPending) {
    const std::string db = "PENDINGMARK";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 3000; ++i) rows.emplace_back(Jan1(kYear) + i, MixedRow(i, 40));
    Insert(store, db, rows);
    const auto pending = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(kYear));
    ASSERT_NE(pending, nullptr);
    EXPECT_EQ(pending->TotalMarkedRowsForTests(), 3000u) << "each row is marked exactly once";
    EXPECT_EQ(pending->PendingBlockCount(), 0u) << "and cleared by the inline fold";
    EXPECT_FALSE(xstore::PendingMaps::AnyPending());
    Insert(store, db, {{Jan1(kYear) + 5, MixedRow(99, 40)}});   // an overwrite is marked and cleared too
    EXPECT_EQ(pending->TotalMarkedRowsForTests(), 3001u);
    EXPECT_EQ(pending->PendingBlockCount(), 0u);
}

// A run whose data pwrite fails is not folded (as before), and its pending mark is cleared all the same: a block
// must not stay pending forever.
TEST_F(XStoreDeferredFoldTest, AFailedDataWriteLeavesNothingPending) {
    const std::string db = "PENDINGFAIL";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    Insert(store, db, {{Jan1(kYear), MixedRow(0, 40)}});   // creates the year file and its pending map
    const auto pending = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(kYear));
    ASSERT_NE(pending, nullptr);
    XStore_Proto::BatchInsert_REQ req;
    req.set_dbname(db);
    req.set_durabilitymode(XStore_Proto::SYNC);
    for (uint64_t i = 1; i < 3000; ++i) {   // spans two blocks of 2048 rows
        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
        entry->set_timestamp(Jan1(kYear) + i);
        *entry->mutable_data() = MixedRow(i, 40);
    }
    XStore_Proto::BatchInsert_REP rep;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, kYear, ".XSTORE"), 0, 1000, EIO);
        store.BatchInsert(&req, &rep);
        ASSERT_GT(fail.Failures(), 0) << "the data pwrite was not reached: " << rep.insertstatus();
    }
    EXPECT_EQ(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
    EXPECT_EQ(pending->TotalMarkedRowsForTests(), 3000u) << "the failed run was marked";
    EXPECT_EQ(pending->PendingBlockCount(), 0u) << "and cleared although its write failed";
    EXPECT_FALSE(xstore::PendingMaps::AnyPending());
}

namespace {
std::string FindStatus(XStore &store, const std::string &db, unsigned long long lo, unsigned long long hi, int32_t column) {
    std::string buf;
    store.FindByValue(lo, hi, db, column, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::FindByValue_REP rep;
    EXPECT_TRUE(rep.ParseFromString(buf));
    return rep.querystatus();
}

size_t Occurrences(const std::string &s, const std::string &what) {
    size_t n = 0;
    for (size_t at = s.find(what); at != std::string::npos; at = s.find(what, at + 1)) ++n;
    return n;
}

// The notice is off by default in stage 2 (see PendingMaps::NoticesEnabled); a test that exercises it turns it on for
// its own scope only.
class ScopedNotices {
public:
    explicit ScopedNotices(bool on) : was_(xstore::PendingMaps::NoticesEnabled()) {
        xstore::PendingMaps::SetNoticesEnabledForTests(on);
    }
    ~ScopedNotices() { xstore::PendingMaps::SetNoticesEnabledForTests(was_); }
    ScopedNotices(const ScopedNotices &) = delete;
    ScopedNotices &operator=(const ScopedNotices &) = delete;

private:
    const bool was_;
};
}  // namespace

// A block with an outstanding index update is answered by scan (same value) and the reply says so once.
TEST_F(XStoreDeferredFoldTest, APendingBlockGetsTheNoticeAndTheSameAnswer) {
    const ScopedNotices notices(true);
    const std::string db = "PENDINGNOTICE";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 3000; ++i) rows.emplace_back(Jan1(kYear) + i, MixedRow(i, 40));
    Insert(store, db, rows);
    const auto pending = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(kYear));
    ASSERT_NE(pending, nullptr);
    const unsigned long long lo = Jan1(kYear), hi = lo + 2999;
    const std::string kNotice = "not indexed yet";

    XStore_Proto::Max_REP clean;
    store.MaxAggregate(lo, hi, db, 0, &clean);
    EXPECT_EQ(clean.opstatus().find(kNotice), std::string::npos) << "nothing pending: no notice";
    EXPECT_EQ(Occurrences(FindStatus(store, db, lo, hi, 0), kNotice), 0u);

    pending->MarkRows(0, 1);
    XStore_Proto::Max_REP busy;
    store.MaxAggregate(lo, hi, db, 0, &busy);
    EXPECT_NE(busy.opstatus().find(kNotice), std::string::npos) << "a pending block: the reply says it was not indexed";
    XStore_Proto::Max_REP clean_copy = clean, busy_copy = busy;
    clean_copy.clear_opstatus();
    busy_copy.clear_opstatus();
    EXPECT_EQ(clean_copy.SerializeAsString(), busy_copy.SerializeAsString()) << "the answer is the same, only slower";
    EXPECT_EQ(Occurrences(FindStatus(store, db, lo, hi, 0), kNotice), 1u) << "once per call, not once per block";

    // Only the queried rows count: the pending block 0 is outside [2048, 2999] (block 1), and a column the index does
    // not track at all still carries the notice for a query that reaches the block.
    EXPECT_EQ(Occurrences(FindStatus(store, db, lo + 2048, hi, 0), kNotice), 0u);
    XStore_Proto::Max_REP outside, wide_column;
    store.MaxAggregate(lo + 2048, hi, db, 0, &outside);
    EXPECT_EQ(outside.opstatus().find(kNotice), std::string::npos);
    store.MaxAggregate(lo, hi, db, 200, &wide_column);
    EXPECT_NE(wide_column.opstatus().find(kNotice), std::string::npos);

    pending->ClearRows(0, 1);
    XStore_Proto::Max_REP after;
    store.MaxAggregate(lo, hi, db, 0, &after);
    EXPECT_EQ(after.opstatus().find(kNotice), std::string::npos);
    EXPECT_EQ(Occurrences(FindStatus(store, db, lo, hi, 0), kNotice), 0u);
}

// Two years with a pending block each: FindByValue still says it once (the aggregates say it once per year).
TEST_F(XStoreDeferredFoldTest, FindByValueSaysItOncePerCallAcrossYears) {
    const ScopedNotices notices(true);
    const std::string db = "PENDINGTWOYEARS";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (const unsigned short year : {1971, 1972})
        for (uint64_t i = 0; i < 100; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 40));
    Insert(store, db, rows);
    for (const unsigned short year : {1971, 1972}) {
        const auto pending = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(year));
        ASSERT_NE(pending, nullptr);
        pending->MarkRows(0, 1);
    }
    const std::string status = FindStatus(store, db, Jan1(1971), Jan1(1972) + 99, 0);
    EXPECT_EQ(Occurrences(status, "not indexed yet"), 1u) << status;
    EXPECT_NE(status.find("1971"), std::string::npos) << status;
    for (const unsigned short year : {1971, 1972}) {
        xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(year))->ClearRows(0, 1);
    }
}

// Stage 2 is behaviour-neutral: the only marks a query can see are a writer's in-flight ones, so by default no reply
// carries the notice even while a block is marked; the switch stage 3 flips turns it on.
TEST_F(XStoreDeferredFoldTest, TheNoticeIsOffByDefaultInStage2) {
    const std::string db = "PENDINGOFF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kYear = 1971;
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 100; ++i) rows.emplace_back(Jan1(kYear) + i, MixedRow(i, 40));
    Insert(store, db, rows);
    const auto pending = xstore::PendingMaps::Find(DbPath(db) + "/" + std::to_string(kYear));
    ASSERT_NE(pending, nullptr);
    const unsigned long long lo = Jan1(kYear), hi = lo + 99;
    const std::string kNotice = "not indexed yet";
    // Indexer::Start turns the notice on by design, so the default is on exactly while the pool runs.
    const bool pool_on = xstore::Indexer::Instance().Active();
    ASSERT_EQ(xstore::PendingMaps::NoticesEnabled(), pool_on) << "the stage 2 default";
    // A hint still queued for block 0 from the insert could clear the synthetic mark and make the "on" check flaky.
    xstore::Indexer::Instance().WaitIdle(std::chrono::seconds(30));
    pending->MarkRows(0, 1);

    {
        std::optional<ScopedNotices> switched_off;   // pool on: the default is on, so switch it off to test the off state
        if (pool_on) switched_off.emplace(false);
        XStore_Proto::Max_REP off;
        store.MaxAggregate(lo, hi, db, 0, &off);
        EXPECT_EQ(off.opstatus().find(kNotice), std::string::npos) << off.opstatus();
        EXPECT_EQ(Occurrences(FindStatus(store, db, lo, hi, 0), kNotice), 0u);
        {
            const ScopedNotices notices(true);
            XStore_Proto::Max_REP on;
            store.MaxAggregate(lo, hi, db, 0, &on);
            EXPECT_NE(on.opstatus().find(kNotice), std::string::npos) << on.opstatus();
            EXPECT_EQ(Occurrences(FindStatus(store, db, lo, hi, 0), kNotice), 1u);
        }
    }
    EXPECT_EQ(xstore::PendingMaps::NoticesEnabled(), pool_on) << "restored";
    pending->ClearRows(0, 1);
}
