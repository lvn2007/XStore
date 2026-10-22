// Startup index rebuild (Tech Reference 4.14 / 4.19, M4 stage 2a): one scan per year, batch folds, years in parallel.
// The oracle is the OLD algorithm kept here in test code: a freshly opened file per index, every row folded through
// the write-through UpdateRow (zone map: RowData parse of the row's real bytes + an independent reimplementation of the
// numeric coercion; catidx: ExtractColumnByIndexSkipParse + EncodeFDataForCatIdx for every tracked column). It targets
// the decoder, the batch folds and the chunking: the fold arithmetic itself (FoldRowIntoBlock, the bloom bits) is shared
// with production, so it is checked by the zone-map/catidx unit and aggregate tests, not here.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_WireEncode.h"
#include "XStore/XStore_WalRing.h"
#include "XStoreTestSupport.h"
#include "XStoreRebuildTestSupport.h"
#include "XStoreSyscallFault.h"
#include "include/XStore/XStore_Internal.h" // WalRecord::AppendRow / ReserveRows
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>
#include <random>
#include <set>
#include <string>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace {
using namespace xstore_test;
using namespace xstore_rebuild_test;

// ---- Batch fold == per-row fold -------------------------------------------------------------------------------------

constexpr uint16_t kUnitRowLength = 512; // 4096 rows per block

TEST(RebuildFoldBatchTest, ZoneMapBatchFoldIsByteIdenticalToPerRowFolds) {
    ScratchDir scratch("rebuild_fold_batch_zmap");
    const std::string a_path = (scratch.test_root / "a.zmap").string();
    const std::string b_path = (scratch.test_root / "b.zmap").string();
    constexpr uint32_t kWidth = 6;
    ZoneMapFile per_row;
    ZoneMapFile batched;
    ASSERT_TRUE(per_row.Open(a_path, kUnitRowLength, kWidth));
    ASSERT_TRUE(batched.Open(b_path, kUnitRowLength, kWidth));
    ASSERT_TRUE(per_row.BeginRebuild(kWidth));
    ASSERT_TRUE(batched.BeginRebuild(kWidth));
    const uint64_t rpb = RowsPerBlock(kUnitRowLength);

    std::mt19937_64 rng(1234);
    uint64_t frontier = 0;
    for (int round = 0; round < 4; ++round) {
        ZoneMapFoldBatch batch;
        batch.pad_to = kWidth;
        std::vector<std::pair<uint64_t, std::vector<ZoneMapObservation>>> rows;
        for (int i = 0; i < 1500; ++i) {
            // Mostly ascending runs, plus out-of-order rows, repeats, and rows below the frontier (skipped by both).
            uint64_t row = (i % 7 == 0) ? rng() % (8 * rpb) : frontier + (static_cast<uint64_t>(i) * 3 + rng() % 3);
            if (i < 3 && frontier >= 3) row = frontier - 1 - static_cast<uint64_t>(i); // just below F: skipped by both
            std::vector<ZoneMapObservation> obs;
            const size_t n = rng() % (kWidth + 2); // up to past the width (dropped by both)
            for (size_t c = 0; c < n; ++c) {
                const bool numeric = rng() % 5 != 0;
                obs.push_back({static_cast<uint32_t>(c), static_cast<double>(static_cast<int64_t>(rng() % 2001) - 1000) / 8.0, numeric});
            }
            rows.emplace_back(row, obs);
            batch.observations.insert(batch.observations.end(), obs.begin(), obs.end());
            batch.EndRow(row);
        }
        for (const auto &[row, obs] : rows) per_row.RebuildFoldRow(row, obs, kWidth);
        ASSERT_TRUE(batched.RebuildFoldRows(batch));
        frontier += rpb + rng() % rpb; // F moves to mid-block: the next round's lower rows are below it
        per_row.AdvanceFrontier(frontier);
        batched.AdvanceFrontier(frontier);
    }
    ASSERT_TRUE(per_row.FinishRebuild());
    ASSERT_TRUE(batched.FinishRebuild());
    const std::string a = ReadFileBytes(a_path);
    ASSERT_GT(a.size(), sizeof(ZoneMapFileHeader) + 4 * kWidth * sizeof(ZoneMapEntry));
    EXPECT_TRUE(a == ReadFileBytes(b_path)) << "batch fold differs from per-row folds";
}

TEST(RebuildFoldBatchTest, CatIdxBatchFoldIsByteIdenticalToPerRowFolds) {
    ScratchDir scratch("rebuild_fold_batch_catidx");
    const std::string a_path = (scratch.test_root / "a.catidx").string();
    const std::string b_path = (scratch.test_root / "b.catidx").string();
    constexpr uint32_t kWidth = 5;
    CatIdxFile per_row;
    CatIdxFile batched;
    ASSERT_TRUE(per_row.Open(a_path, kUnitRowLength, kWidth));
    ASSERT_TRUE(batched.Open(b_path, kUnitRowLength, kWidth));
    ASSERT_TRUE(per_row.BeginRebuild(kWidth));
    ASSERT_TRUE(batched.BeginRebuild(kWidth));
    const uint64_t rpb = RowsPerBlock(kUnitRowLength);

    std::mt19937_64 rng(99);
    uint64_t frontier = 0;
    for (int round = 0; round < 4; ++round) {
        CatIdxFoldBatch batch;
        std::vector<std::pair<uint64_t, std::vector<CatIdxObservation>>> rows;
        for (int i = 0; i < 1500; ++i) {
            uint64_t row = (i % 7 == 0) ? rng() % (8 * rpb) : frontier + (static_cast<uint64_t>(i) * 3 + rng() % 3);
            if (i < 3 && frontier >= 3) row = frontier - 1 - static_cast<uint64_t>(i); // just below F: skipped by both
            std::vector<CatIdxObservation> obs;
            for (uint32_t c = 0; c < kWidth + 2; ++c) {
                if (rng() % 3 == 0) {
                    obs.push_back({c, std::string(), false});
                    continue;
                }
                const std::string value = EncodeFDataForCatIdx(FInt64(static_cast<int64_t>(rng() % 50)));
                obs.push_back({c, value, true});
                batch.Add(c, value);
            }
            rows.emplace_back(row, obs);
            batch.EndRow(row);
        }
        for (const auto &[row, obs] : rows) per_row.RebuildFoldRow(row, obs);
        ASSERT_TRUE(batched.RebuildFoldRows(batch));
        frontier += rpb + rng() % rpb;
        per_row.AdvanceFrontier(frontier);
        batched.AdvanceFrontier(frontier);
    }
    ASSERT_TRUE(per_row.FinishRebuild());
    ASSERT_TRUE(batched.FinishRebuild());
    const std::string a = ReadFileBytes(a_path);
    ASSERT_GT(a.size(), sizeof(CatIdxFileHeader) + 4 * kWidth * XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY);
    EXPECT_TRUE(a == ReadFileBytes(b_path)) << "batch fold differs from per-row folds";
}

// ---- Startup rebuild against the reference --------------------------------------------------------------------------

class XStoreRebuildTest : public ::testing::Test {
protected:
    static constexpr uint16_t kRowLength = 1024; // 2048 rows per block, 4096 per rebuild chunk
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_rebuild_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        XStore::ClearLiveWidenRequestsForTests(); // another test's growth would make compaction yield
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

    // Clean stop (WAL checkpointed, indexes clean), then a "new process": empty registries.
    static void CleanRestartState() {
        XStore::Shutdown();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
    }

    static void Insert(XStore &store, const std::string &db, const std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> &rows) {
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
};

// Every data shape the startup rebuild must reproduce exactly, one year each, rebuilt in parallel by one InitDB:
// .zmap byte-identical to the reference (header included: both end clean with the same width and rows_per_block),
// .catidx byte-identical (header, block_count and every bloom block).
TEST_F(XStoreRebuildTest, StartupRebuildMatchesThePerRowReferenceOnEveryDataShape) {
    const std::string db = "REBUILDREF";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const unsigned short kDense = 1971, kSparse = 1972, kOverwrite = 1973, kDelete = 1974, kEmpty = 1975, kDamaged = 1976;
    {   // Dense: 5000 rows (two full blocks and a partial last one); every 50th row 80 columns wide (DB width -> 80).
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 5000; ++i) rows.emplace_back(Jan1(kDense) + i, MixedRow(i, i % 50 == 0 ? 80 : 40 + static_cast<int>(i % 9)));
        Insert(store, db, rows);
    }
    {   // Sparse: rows at block edges and far apart (long holes), narrow rows (fewer columns than the width).
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t r : {0ULL, 1ULL, 2047ULL, 2048ULL, 4095ULL, 4096ULL, 100000ULL, 100001ULL, 3000000ULL, 31535999ULL}) {
            rows.emplace_back(Jan1(kSparse) + r, MixedRow(r, 1 + static_cast<int>(r % 7)));
        }
        Insert(store, db, rows);
    }
    {   // Overwrites: 4500 rows, then 1500 of them rewritten with other values and widths.
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 4500; ++i) rows.emplace_back(Jan1(kOverwrite) + i, MixedRow(i, 30));
        Insert(store, db, rows);
        rows.clear();
        for (uint64_t i = 0; i < 4500; i += 3) rows.emplace_back(Jan1(kOverwrite) + i, MixedRow(i + 999, 12 + static_cast<int>(i % 5)));
        Insert(store, db, rows);
    }
    {   // Deletes of all three kinds (the live index carries inexact blocks; a rebuild starts exact).
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 9000; ++i) rows.emplace_back(Jan1(kDelete) + i, MixedRow(i, 20));
        Insert(store, db, rows);
        for (uint64_t i : {5ULL, 2047ULL, 6000ULL}) ASSERT_NE(store.UnaryDelete(Jan1(kDelete) + i, db).deletestatus().find("[INFO]"), std::string::npos);
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(db);
        for (uint64_t i = 100; i < 4000; i += 37) req.add_timestamp(Jan1(kDelete) + i);
        ASSERT_NE(store.BatchDelete(&req).deletestatus().find("[INFO]"), std::string::npos);
        ASSERT_NE(store.RangeDelete(Jan1(kDelete) + 4096, Jan1(kDelete) + 6143, db).deletestatus().find("[INFO]"), std::string::npos); // a whole block
        ASSERT_NE(store.RangeDelete(Jan1(kDelete) + 7000, Jan1(kDelete) + 7100, db).deletestatus().find("[INFO]"), std::string::npos);
    }
    {   // Damaged / unusual rows written straight into the year file: the one-pass decoder must decline them and the
        // fallback must still give exactly the reference's answer.
        Insert(store, db, {{Jan1(kDamaged), MixedRow(1, 10)}});
    }
    CleanRestartState();
    {
        const std::string path = YearPath(db, kDamaged, ".XSTORE");
        PwriteRow(path, 10, kRowLength, DoubleColumn(1.5) + std::string("\x12\x01\x41", 3) + std::string("\x0a\x02\x20\x07", 4)); // unknown field
        PwriteRow(path, 11, kRowLength, std::string("\x08\x05", 2) + DoubleColumn(2.5));                                           // column with a varint wire type
        PwriteRow(path, 12, kRowLength, DoubleColumn(3.5) + std::string("\x0a\xff\x0f", 3));                                       // column longer than the slot
        PwriteRow(path, 13, kRowLength, DoubleColumn(4.5) + std::string("\x0a\x05\x72\x03\xff\xfe\xfd", 7) + std::string("\x0a\x02\x20\x09", 4)); // bad UTF-8 string
        PwriteRow(path, 14, kRowLength, std::string("\x0a\x03\x09\x01\x02", 5) + std::string("\x0a\x02\x20\x05", 4));            // undecodable column
        PwriteRow(path, 15, kRowLength, DoubleColumn(6.5) + std::string("\xff\x01", 2));                                           // garbage tag after a column
        PwriteRow(path, 16, kRowLength, std::string("\x0a\x00\x0a\x00", 4) + DoubleColumn(7.5));                                   // empty columns (valid)
        PwriteRow(path, 4095, kRowLength, DoubleColumn(8.5));                                                                      // last row of chunk 1
        PwriteRow(path, 4096, kRowLength, DoubleColumn(9.5));                                                                      // first row of chunk 2
        // An empty year: a year file with no row at all.
        const int fd = open(YearPath(db, kEmpty, ".XSTORE").c_str(), O_WRONLY | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, 50000 * kRowLength), 0);
        close(fd);
    }
    const std::vector<unsigned short> years = {kDense, kSparse, kOverwrite, kDelete, kEmpty, kDamaged};
    for (unsigned short y : years) {
        std::filesystem::remove(YearPath(db, y, ".zmap"));
        std::filesystem::remove(YearPath(db, y, ".catidx"));
    }

    store.InitDB();

    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, kDense, ".zmap")).tracked_column_count;
    ASSERT_EQ(width, 80u) << "the DB grew to its widest row";
    XStore::ClearZoneMapRegistryForTests(); // the rebuilt files are complete on disk
    XStore::ClearCatIdxRegistryForTests();
    for (unsigned short y : years) {
        SCOPED_TRACE(y);
        const std::string ref_zmap = (scratch->test_root / ("ref_" + std::to_string(y) + ".zmap")).string();
        const std::string ref_catidx = (scratch->test_root / ("ref_" + std::to_string(y) + ".catidx")).string();
        BuildReferenceIndexes(store, YearPath(db, y, ".XSTORE"), ref_zmap, ref_catidx, kRowLength, width);
        const std::string got_zmap = ReadFileBytes(YearPath(db, y, ".zmap"));
        const std::string got_catidx = ReadFileBytes(YearPath(db, y, ".catidx"));
        EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, y, ".zmap")).dirty, 0u);
        EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, y, ".catidx")).dirty, 0u);
        EXPECT_EQ(got_zmap.size(), ReadFileBytes(ref_zmap).size());
        EXPECT_TRUE(got_zmap == ReadFileBytes(ref_zmap)) << ".zmap differs from the per-row reference";
        EXPECT_EQ(got_catidx.size(), ReadFileBytes(ref_catidx).size());
        EXPECT_TRUE(got_catidx == ReadFileBytes(ref_catidx)) << ".catidx differs from the per-row reference";
    }
    EXPECT_EQ(ReadFileBytes(YearPath(db, kEmpty, ".zmap")).size(), sizeof(ZoneMapFileHeader));
    EXPECT_GT(ReadHeader<CatIdxFileHeader>(YearPath(db, kDense, ".catidx")).block_count, 2u);
}

// Only the stale index is rebuilt: a healthy, compacted catidx (bitmap section) survives a zone-map-only rebuild
// byte for byte, and the reverse.
TEST_F(XStoreRebuildTest, OnlyTheStaleIndexIsRebuilt) {
    const std::string db = "REBUILDONE";
    const unsigned short year = 1980;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 3000; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 16));
    Insert(store, db, rows);
    CleanRestartState();
    {   // Keep the compaction scan short: it reads every row of the file.
        const int fd = open(YearPath(db, year, ".XSTORE").c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, 3000 * kRowLength), 0);
        close(fd);
    }
    const uint32_t width = ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).tracked_column_count;
    store.CompactOneYearCatIdx(DbPath(db), year, kRowLength, width);
    ASSERT_TRUE(XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength, width).HasBitmapSection());
    CleanRestartState(); // the compaction dirtied it: clean again

    const std::string catidx_before = ReadFileBytes(YearPath(db, year, ".catidx"));
    ASSERT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u);
    std::filesystem::remove(YearPath(db, year, ".zmap"));
    store.InitDB();
    EXPECT_TRUE(catidx_before == ReadFileBytes(YearPath(db, year, ".catidx"))) << "a healthy catidx was touched";
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 0u);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Jan1(year), Jan1(year) + 2999, db, 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 2999.0 * 31 * 0.5);
    EXPECT_GT(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).tracked_column_count, 0u);

    // The reverse: a stale catidx only; the .zmap stays byte for byte.
    CleanRestartState();
    const std::string zmap_before = ReadFileBytes(YearPath(db, year, ".zmap"));
    std::filesystem::remove(YearPath(db, year, ".catidx"));
    store.InitDB();
    EXPECT_TRUE(zmap_before == ReadFileBytes(YearPath(db, year, ".zmap"))) << "a healthy zone map was touched";
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength, width).HasBitmapSection());
}

// ---- Insert path (WalRecord::AppendRow + ApplyWalRecord) against the reference ---------------------------------------

// The flat observations AppendRow builds, entry by entry, against the per-row reference: the zone map's present
// columns with the independent numeric coercion, and for each set column the old bloom bit positions of its
// EncodeFDataForCatIdx bytes (RefBloomPositions; unset columns skipped). At width 64, and at 3, which clamps the wider rows. Also the row slot itself.
TEST_F(XStoreRebuildTest, AppendRowObservationsMatchThePerRowReference) {
    XStore store;
    std::vector<XStore_Proto::RowData> rows;
    rows.push_back(MakeRow({FDouble(-2.5), FFloat(0.75f), FInt32(-123456), FInt64(int64_t{1} << 40), FUInt32(4000000000u),
                            FUInt64(0xFFFFFFFFFFFF0001ULL), FSInt32(-77), FSInt64(-(int64_t{1} << 50)), FFixed32(12345u),
                            FFixed64(0x123456789ULL), FSFixed32(-9), FSFixed64(-99999999999LL), FBool(true), FBool(false),
                            XStore_Proto::FData(), FString(""), FString("sso-short"), FString(std::string(40, 'L') + "ong"),
                            FBytes(std::string("\x00\x01\x00\xff", 4)), FDouble(9.0)}));                  // every kind, a hole
    rows.push_back(MakeRow({FInt64(5), XStore_Proto::FData()}));                                       // narrow, unset last
    rows.push_back(XStore_Proto::RowData{});                                                           // no column at all
    rows.push_back(MakeRow({XStore_Proto::FData(), FString(std::string(100, 's')), FDouble(1.0), FBytes("b")}));
    for (uint32_t width : {64u, 3u}) {
        SCOPED_TRACE("width " + std::to_string(width));
        XStore::WalRecord record;
        record.row_length = kRowLength;
        record.tracked_column_count = width;
        record.ReserveRows(rows.size(), 20);
        for (size_t i = 0; i < rows.size(); ++i) {
            ASSERT_LE(rows[i].ByteSizeLong(), kRowLength);   // also caches the sizes AppendRow serializes with
            record.AppendRow(store, 5000 + i, rows[i]);
        }
        ASSERT_EQ(record.row_count, rows.size());
        ASSERT_EQ(record.obs_end.size(), rows.size());
        ASSERT_EQ(record.wire.size(), rows.size() * record.RowStride());
        size_t zmap_pos = 0;
        size_t catidx_pos = 0;   // into catidx_vals
        for (size_t i = 0; i < rows.size(); ++i) {
            SCOPED_TRACE("row " + std::to_string(i));
            EXPECT_EQ(record.EpochAt(i), 5000 + i);
            std::string slot = rows[i].SerializeAsString();
            slot.resize(kRowLength, '\0');
            EXPECT_TRUE(std::string(record.RowAt(i), kRowLength) == slot);

            const std::vector<ZoneMapObservation> want_zmap = RefZoneMapObservations(rows[i], width);
            ASSERT_EQ(record.obs_end[i].zmap_end - zmap_pos, want_zmap.size());
            for (size_t k = 0; k < want_zmap.size(); ++k) {
                const ZoneMapObservation &got = record.zmap_obs[zmap_pos + k];
                EXPECT_EQ(got.column_idx, want_zmap[k].column_idx) << "observation " << k;
                EXPECT_EQ(got.is_numeric, want_zmap[k].is_numeric) << "observation " << k;
                EXPECT_EQ(std::memcmp(&got.value, &want_zmap[k].value, sizeof(double)), 0) << "observation " << k;
            }
            zmap_pos = record.obs_end[i].zmap_end;

            // Each set column's value, as the old bloom positions of its EncodeFDataForCatIdx bytes.
            std::vector<std::pair<uint32_t, std::vector<uint16_t>>> want_catidx;
            const int tracked = std::min(static_cast<int>(width), rows[i].fielddata_size());
            for (int c = 0; c < tracked; ++c) {
                if (rows[i].fielddata(c).oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
                    want_catidx.emplace_back(static_cast<uint32_t>(c), RefBloomPositions(EncodeFDataForCatIdx(rows[i].fielddata(c))));
                }
            }
            ASSERT_GE(record.obs_end[i].catidx_end, catidx_pos);
            ASSERT_LE(record.obs_end[i].catidx_end, record.catidx_vals.size());
            std::vector<std::pair<uint32_t, std::vector<uint16_t>>> got_catidx;
            for (; catidx_pos < record.obs_end[i].catidx_end; ++catidx_pos) {
                const CatIdxBloomValue &v = record.catidx_vals[catidx_pos];
                got_catidx.emplace_back(v.ColumnIdx(), Positions(v));
            }
            EXPECT_TRUE(got_catidx == want_catidx) << got_catidx.size() << " catidx values, want " << want_catidx.size();
        }
        EXPECT_EQ(zmap_pos, record.zmap_obs.size());
        EXPECT_EQ(catidx_pos, record.catidx_vals.size());
    }
}

// End to end: rows inserted through both producers (BatchInsert and UnaryInsert, SYNC) and folded by the writer from
// the flat arrays leave .zmap and .catidx byte-identical to the per-row reference built from the same data file. Mixed
// types, unset columns, narrow rows and string-heavy rows (long values in 8 of 16 columns, so the producer's reused
// encoding buffer grows).
TEST_F(XStoreRebuildTest, LiveInsertIndexesMatchThePerRowReference) {
    const std::string db = "LIVEREF";
    const unsigned short year = 1982;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const auto string_heavy = [](uint64_t i) {
        XStore_Proto::RowData row;
        for (uint64_t c = 0; c < 16; ++c) {
            if (c % 2 == 0) {
                row.add_fielddata()->set_stringdata(std::string(24 + (i + c) % 41, static_cast<char>('a' + c)) + std::to_string(i));
            } else {
                row.add_fielddata()->set_doubledata(static_cast<double>(i * 16 + c));
            }
        }
        return row;
    };
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 3000; ++i) {
        rows.emplace_back(Jan1(year) + i, i % 5 == 0 ? string_heavy(i) : MixedRow(i, 1 + static_cast<int>(i % 40)));
    }
    Insert(store, db, rows);
    for (uint64_t i : {4000ULL, 4001ULL, 6000ULL}) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(Jan1(year) + i, i == 4001 ? MixedRow(i, 33) : string_heavy(i), db, &rep, XStore_Proto::SYNC);
        ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    }
    CleanRestartState(); // WAL checkpointed, both indexes clean on disk

    const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).tracked_column_count;
    ASSERT_EQ(width, ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).tracked_column_count);
    const std::string ref_zmap = (scratch->test_root / "live_ref.zmap").string();
    const std::string ref_catidx = (scratch->test_root / "live_ref.catidx").string();
    BuildReferenceIndexes(store, YearPath(db, year, ".XSTORE"), ref_zmap, ref_catidx, kRowLength, width);
    const std::string got_zmap = ReadFileBytes(YearPath(db, year, ".zmap"));
    const std::string got_catidx = ReadFileBytes(YearPath(db, year, ".catidx"));
    EXPECT_EQ(got_zmap.size(), ReadFileBytes(ref_zmap).size());
    EXPECT_TRUE(got_zmap == ReadFileBytes(ref_zmap)) << ".zmap of the live inserts differs from the per-row reference";
    EXPECT_EQ(got_catidx.size(), ReadFileBytes(ref_catidx).size());
    EXPECT_TRUE(got_catidx == ReadFileBytes(ref_catidx)) << ".catidx of the live inserts differs from the per-row reference";
}

// The writer's per-run views over one record: a request with timestamp gaps (many runs), duplicate timestamps (a run
// boundary on the same row; both values are folded, as before), rows out of order (sorted by MultiRowInsert), block
// edges and rows with no set column. The oracle is UpdateRow (the per-row API) over EVERY request row, duplicates
// included, at the file's width: the bloom ORs, so the fold order does not matter and the bytes must be identical.
TEST_F(XStoreRebuildTest, LiveInsertRunsGapsAndDuplicatesMatchPerRowFolds) {
    const std::string db = "LIVERUNS";
    const unsigned short year = 1983;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    std::mt19937_64 rng(4242);
    uint64_t r = 0;
    for (int i = 0; i < 2500; ++i) {
        r += (i % 9 == 0) ? 1 + rng() % 700 : 1;   // gaps, some across a block edge (2048 rows per block)
        rows.emplace_back(Jan1(year) + r, MixedRow(r, 1 + static_cast<int>(rng() % 30)));
        if (i % 13 == 0) rows.emplace_back(Jan1(year) + r, MixedRow(r + 7777, 1 + static_cast<int>(rng() % 30)));   // duplicate
        if (i % 101 == 0) rows.emplace_back(Jan1(year) + r + 1, XStore_Proto::RowData{});   // no column at all
    }
    std::shuffle(rows.begin() + 100, rows.begin() + 200, rng);   // out of order (a stable sort keeps duplicates in order)
    Insert(store, db, rows);
    CleanRestartState();

    const uint32_t width = ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).tracked_column_count;
    const std::string ref_path = (scratch->test_root / "runs_ref.catidx").string();
    {
        CatIdxFile ref;
        ASSERT_TRUE(ref.Open(ref_path, kRowLength, width));
        for (const auto &[ts, row] : rows) {
            std::vector<CatIdxObservation> obs;
            for (int c = 0; c < std::min(static_cast<int>(width), row.fielddata_size()); ++c) {
                const bool set = row.fielddata(c).oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET;
                obs.push_back({static_cast<uint32_t>(c), set ? EncodeFDataForCatIdx(row.fielddata(c)) : std::string(), set});
            }
            ref.UpdateRow(ts - Jan1(year), obs);
        }
        ASSERT_TRUE(ref.MarkClean());
    }
    const std::string got = ReadFileBytes(YearPath(db, year, ".catidx"));
    const std::string want = ReadFileBytes(ref_path);
    ASSERT_GT(ReadHeader<CatIdxFileHeader>(ref_path).block_count, 2u);
    EXPECT_EQ(got.size(), want.size());
    EXPECT_TRUE(got == want) << ".catidx of the live insert differs from the per-row folds";
}

// A failed index write part-way through the scan: that file stops behind its frontier (blocks it finished stay
// trusted, the rest are scanned, answers stay right), stays DIRTY, and the next start rebuilds it to the reference.
class XStoreRebuildWriteFaultTest : public XStoreRebuildTest {
protected:
    const std::string db = "REBUILDFAULT";
    const unsigned short year = 1981;
    static constexpr uint64_t kRows = 5000; // blocks 0, 1 (chunk 1) and 2 (chunk 2)

    void Seed(XStore &store) {
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < kRows; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 16));
        Insert(store, db, rows);
        CleanRestartState();
        std::filesystem::remove(YearPath(db, year, ".zmap"));
        std::filesystem::remove(YearPath(db, year, ".catidx"));
    }
    void ExpectNextStartRebuildsToTheReference(XStore &store) {
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        store.InitDB();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        const uint32_t width = ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).tracked_column_count;
        const std::string ref_zmap = (scratch->test_root / "ref.zmap").string();
        const std::string ref_catidx = (scratch->test_root / "ref.catidx").string();
        BuildReferenceIndexes(store, YearPath(db, year, ".XSTORE"), ref_zmap, ref_catidx, kRowLength, width);
        EXPECT_TRUE(ReadFileBytes(YearPath(db, year, ".zmap")) == ReadFileBytes(ref_zmap));
        EXPECT_TRUE(ReadFileBytes(YearPath(db, year, ".catidx")) == ReadFileBytes(ref_catidx));
    }
};

TEST_F(XStoreRebuildWriteFaultTest, ZoneMapWriteFailureLeavesItDirtyAndBehindItsFrontier) {
    XStore store;
    Seed(store);
    {
        // pwrites on the new .zmap: Open's header, the dirty mark, BeginRebuild's header, block 0, block 1 (chunk 1),
        // block 2 (chunk 2): fail block 2.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, year, ".zmap"), 5, 1, EIO);
        store.InitDB();
        ASSERT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u) << "the catidx is unaffected";
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(DbPath(db), year, kRowLength);
    EXPECT_TRUE(zmap.Rebuilding());
    auto block0 = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(block0.has_value()) << "chunk 1 was folded and its frontier advanced";
    EXPECT_EQ((*block0)[0].valid_count + ZoneMapNullCount((*block0)[0]), 2048u);
    EXPECT_TRUE(zmap.TryGetBlockStats(1).has_value());
    EXPECT_FALSE(zmap.TryGetBlockStats(2).has_value()) << "the failed block must not be trusted";
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Jan1(year), Jan1(year) + kRows - 1, db, 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), static_cast<double>((kRows - 1) * 31) * 0.5) << "answers stay right (scan)";
    ExpectNextStartRebuildsToTheReference(store);
}

TEST_F(XStoreRebuildWriteFaultTest, CatIdxWriteFailureLeavesItDirtyAndBehindItsFrontier) {
    XStore store;
    Seed(store);
    {
        // pwrites on the new .catidx: Open's header, the dirty mark, BeginRebuild's header, then per block the bloom
        // block and the block_count header (blocks 0 and 1 in chunk 1): fail block 2's bloom write.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, year, ".catidx"), 7, 1, EIO);
        store.InitDB();
        ASSERT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 1u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 0u) << "the zone map is unaffected";
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength);
    EXPECT_TRUE(catidx.Rebuilding());
    // Row 4500 (block 2) column 0 is a double; the bloom must answer "maybe" for its untrusted block.
    XStore_Proto::FData v4500;
    v4500.set_doubledata(static_cast<double>(4500 * 31) * 0.5);
    EXPECT_TRUE(catidx.MightContain(2, 0, EncodeFDataForCatIdx(v4500)));
    std::string buf;
    store.FindByValue(Jan1(year), Jan1(year) + kRows - 1, db, 0, v4500, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buf));
    ASSERT_EQ(rep.queryresponse_size(), 1);
    EXPECT_EQ(rep.queryresponse(0).timestamp(), Jan1(year) + 4500);
    ExpectNextStartRebuildsToTheReference(store);
}

// SF-A: BeginRebuild cannot start (here: the dirty mark's fdatasync fails on a fresh index file, whose header was
// written dirty but not synced). The file must not be served as a trusted, empty index for the session: it is left
// untrusted (queries scan), dirty on disk, and the next start rebuilds it.
TEST_F(XStoreRebuildWriteFaultTest, CatIdxThatCannotStartItsRebuildIsNotTrustedForTheSession) {
    XStore store;
    Seed(store);
    std::filesystem::remove(YearPath(db, year, ".zmap")); // Seed removed both; keep only the catidx case under fault
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, year, ".catidx"), 0, 1000, EIO);
        store.InitDB();
        ASSERT_GT(fail.Failures(), 0);
    }
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 1u);
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength).Rebuilding());
    for (uint64_t r : {7ULL, 2100ULL, 4500ULL}) {
        XStore_Proto::FData value;
        value.set_doubledata(static_cast<double>(r * 31) * 0.5);
        std::string buf;
        store.FindByValue(Jan1(year), Jan1(year) + kRows - 1, db, 0, value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { buf += chunk; });
        XStore_Proto::RangeQuery_REP rep;
        ASSERT_TRUE(rep.ParseFromString(buf));
        ASSERT_EQ(rep.queryresponse_size(), 1) << "row " << r << " must be found by the scan fallback";
        EXPECT_EQ(rep.queryresponse(0).timestamp(), Jan1(year) + r);
    }
    ExpectNextStartRebuildsToTheReference(store);
}

TEST_F(XStoreRebuildWriteFaultTest, ZoneMapThatCannotStartItsRebuildIsNotTrustedForTheSession) {
    XStore store;
    Seed(store);
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, year, ".zmap"), 0, 1000, EIO);
        store.InitDB();
        ASSERT_GT(fail.Failures(), 0);
    }
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, year, ".catidx")).dirty, 0u) << "the catidx is unaffected";
    EXPECT_TRUE(XStore::GetOrOpenZoneMap(DbPath(db), year, kRowLength).Rebuilding());
    // Rows 0..4095 are two fully covered blocks; an empty trusted zone map would skip them all.
    const unsigned long long lo = Jan1(year), hi = Jan1(year) + 4095;
    XStore_Proto::Min_REP min_rep;
    XStore_Proto::Max_REP max_rep;
    XStore_Proto::Sum_REP sum_rep;
    store.MinAggregate(lo, hi, db, 0, &min_rep);
    store.MaxAggregate(lo, hi, db, 0, &max_rep);
    store.SumAggregate(lo, hi, db, 0, &sum_rep);
    double expected_sum = 0;
    for (uint64_t i = 0; i <= 4095; ++i) {
        if (i % 17 != 0) expected_sum += static_cast<double>(i * 31) * 0.5; // column 0 is null when i % 17 == 0
    }
    EXPECT_DOUBLE_EQ(min_rep.opresult().doubledata(), 31 * 0.5) << "row 0 is null, row 1 holds the minimum";
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 4095.0 * 31 * 0.5);
    EXPECT_DOUBLE_EQ(sum_rep.opresult(), expected_sum);
    ExpectNextStartRebuildsToTheReference(store);
}

// SF-B: an exception in one year's rebuild (here: the .catidx cannot be opened) must not be swallowed: every other year
// still finishes, then InitDB rethrows it, so the start fails loudly instead of serving a lazily created empty index.
TEST_F(XStoreRebuildTest, AYearWhoseIndexCannotBeOpenedFailsInitDbAfterTheOtherYearsFinish) {
    const std::string db = "REBUILDTHROW";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    for (unsigned short y : {1982, 1983}) {
        std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
        for (uint64_t i = 0; i < 300; ++i) rows.emplace_back(Jan1(y) + i, MixedRow(i, 8));
        Insert(store, db, rows);
    }
    CleanRestartState();
    for (unsigned short y : {1982, 1983}) {
        std::filesystem::remove(YearPath(db, y, ".zmap"));
        std::filesystem::remove(YearPath(db, y, ".catidx"));
    }
    {
        xstore_fault::ScopedPathFault fail(YearPath(db, 1982, ".catidx"), false, EMFILE, xstore_fault::kOpenWrite);
        EXPECT_THROW(store.InitDB(), std::runtime_error);
        EXPECT_GT(fail.OpenHits(), 0u);
    }
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, 1982, ".catidx"))) << "no index was created for the failed year";
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, 1983, ".zmap")).dirty, 0u) << "the other year finished";
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, 1983, ".catidx")).dirty, 0u);
}

// N1: a zone map whose header carries a corrupt rows_per_block (0) is rebuilt with the value a fresh file gets.
TEST_F(XStoreRebuildTest, CorruptRowsPerBlockInAnOldZoneMapIsNotKept) {
    const std::string db = "REBUILDRPB";
    const unsigned short year = 1984;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 300; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 8));
    Insert(store, db, rows);
    CleanRestartState();
    {
        ZoneMapFileHeader h = ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap"));
        h.rows_per_block = 0;
        h.dirty = 1; // stale: InitDB rebuilds it
        const int fd = open(YearPath(db, year, ".zmap").c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
        close(fd);
    }
    store.InitDB();
    const ZoneMapFileHeader after = ReadHeader<ZoneMapFileHeader>(YearPath(db, year, ".zmap"));
    EXPECT_EQ(after.rows_per_block, RowsPerBlock(kRowLength));
    EXPECT_EQ(after.dirty, 0u);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Jan1(year), Jan1(year) + 299, db, 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 299.0 * 31 * 0.5);
}

// SF-A2: an untrusted .catidx (BeginRebuild could not start: its dirty-mark fdatasync fails) that still holds an old
// bitmap section must not serve it. A compacted, CLEAN year whose catidx is stale because the DB grew wider (a 70-column
// row in another year), and whose directory misses rows 3000..3009 (written straight into the data file, standing in
// for rows whose index writes were lost). (A WAL replay of such rows does not reproduce it: a replayed row's FlushBlock
// invalidates the section itself; a dirty header does not either: BeginRebuild then needs no sync and truncates.)
TEST_F(XStoreRebuildTest, AnUntrustedCatIdxDoesNotServeAStaleBitmapSection) {
    const std::string db = "REBUILDBITMAP";
    const unsigned short year = 1985;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 3000; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 16));
    Insert(store, db, rows);
    CleanRestartState();
    {
        const int fd = open(YearPath(db, year, ".XSTORE").c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, 4000 * kRowLength), 0);
        close(fd);
    }
    const std::string catidx_path = YearPath(db, year, ".catidx");
    const uint32_t width = ReadHeader<CatIdxFileHeader>(catidx_path).tracked_column_count;
    store.CompactOneYearCatIdx(DbPath(db), year, kRowLength, width);
    ASSERT_TRUE(XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength, width).HasBitmapSection());
    CleanRestartState();
    for (uint64_t i = 3000; i < 3010; ++i) PwriteRow(YearPath(db, year, ".XSTORE"), i, kRowLength, MixedRow(i, 16).SerializeAsString());
    {
        XStore_Proto::RowData wide;
        for (int c = 0; c < 70; ++c) wide.add_fielddata()->set_doubledata(c);
        Insert(store, db, {{Jan1(1975), wide}}); // the DB is now 70 wide: the 64-wide catidx of `year` is stale
    }
    const CatIdxFileHeader h = ReadHeader<CatIdxFileHeader>(catidx_path);
    ASSERT_NE(h.bitmap_section_offset, 0u);
    ASSERT_EQ(h.dirty, 0u);
    ASSERT_LT(h.tracked_column_count, 70u);
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, catidx_path, 0, 1000, EIO);
        store.InitDB();
        ASSERT_GT(fail.Failures(), 0);
    }
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db), year, kRowLength, width);
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_FALSE(catidx.HasBitmapSection());
    const uint64_t symbol = (3001 * 31 + 13) % 23; // column 13 of row 3001 (not null: (3001 + 13) % 17 != 0)
    std::set<unsigned long long> want;
    for (uint64_t i = 0; i < 3010; ++i) {
        if ((i + 13) % 17 != 0 && (i * 31 + 13) % 23 == symbol) want.insert(Jan1(year) + i);
    }
    std::string buf;
    store.FindByValue(Jan1(year), Jan1(year) + 3009, db, 13, FString("sym" + std::to_string(symbol)),
                      XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES, [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(buf));
    std::set<unsigned long long> got;
    for (const auto &e : rep.queryresponse()) got.insert(e.timestamp());
    EXPECT_EQ(got.size(), want.size());
    EXPECT_TRUE(got == want) << "rows the stale directory does not list were not found";
}

// Open question 1: a CLEAN zone map of the right width whose header carries rows_per_block 0 is rebuilt at the next
// start (the first aggregate would otherwise divide by zero); a healthy file is not touched.
TEST_F(XStoreRebuildTest, ACleanRightWidthZoneMapWithAWrongBlockSizeIsRebuilt) {
    const std::string db = "REBUILDRPB0";
    const unsigned short year = 1986;
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::vector<std::pair<unsigned long long, XStore_Proto::RowData>> rows;
    for (uint64_t i = 0; i < 300; ++i) rows.emplace_back(Jan1(year) + i, MixedRow(i, 8));
    Insert(store, db, rows);
    CleanRestartState();
    const std::string zmap_path = YearPath(db, year, ".zmap");
    const std::string healthy = ReadFileBytes(zmap_path);
    store.InitDB();
    EXPECT_TRUE(healthy == ReadFileBytes(zmap_path)) << "a healthy zone map was rebuilt";
    {
        ZoneMapFileHeader h = ReadHeader<ZoneMapFileHeader>(zmap_path);
        ASSERT_EQ(h.dirty, 0u);
        h.rows_per_block = 0;
        const int fd = open(zmap_path.c_str(), O_WRONLY);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h)));
        close(fd);
    }
    store.InitDB();
    const ZoneMapFileHeader after = ReadHeader<ZoneMapFileHeader>(zmap_path);
    EXPECT_EQ(after.rows_per_block, RowsPerBlock(kRowLength));
    EXPECT_EQ(after.dirty, 0u);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Jan1(year), Jan1(year) + 299, db, 0, &max_rep);
    EXPECT_DOUBLE_EQ(max_rep.opresult().doubledata(), 299.0 * 31 * 0.5);
}

// Several years fail: InitDB rethrows the LOWEST year's exception, whatever the directory order.
TEST_F(XStoreRebuildTest, InitDbRethrowsTheLowestFailedYear) {
    const std::string db = "REBUILDORDER";
    XStore store;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const std::vector<unsigned short> years = {1990, 1987, 1989, 1988, 1991};
    for (unsigned short y : years) Insert(store, db, {{Jan1(y), MixedRow(1, 4)}});
    CleanRestartState();
    for (unsigned short y : years) {
        std::filesystem::remove(YearPath(db, y, ".zmap"));
        std::filesystem::remove(YearPath(db, y, ".catidx"));
    }
    // The test only discriminates when directory order (which the old first-failure rethrow followed) does not list
    // 1987 first: then only the lowest-year rule passes it. readdir order is filesystem-dependent, so skip otherwise.
    // Every year fails deterministically (all index opens under the DB are refused).
    for (const auto &entry : std::filesystem::directory_iterator(DbPath(db))) {
        if (entry.path().extension() != ".XSTORE") continue;
        if (entry.path().stem().string() == "1987") GTEST_SKIP() << "1987 is listed first here: the test would not discriminate";
        break;
    }
    std::string what;
    {
        xstore_fault::ScopedPathFault fail(DbPath(db), true, EMFILE, xstore_fault::kOpenWrite);
        try {
            store.InitDB();
        } catch (const std::runtime_error &e) {
            what = e.what();
        }
    }
    EXPECT_NE(what.find("/1987."), std::string::npos) << what;
}

} // namespace
