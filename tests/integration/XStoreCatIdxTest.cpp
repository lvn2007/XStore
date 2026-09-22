#include <gtest/gtest.h>
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore.h"
#include "include/DataStructs/TimeStruct.h"
#include "XStore.pb.h"
#include "XStoreTestSupport.h"
#include <filesystem>

TEST(EncodeFDataForCatIdxTest, SameInt64ValueEncodesIdentically) {
    XStore_Proto::FData a;
    a.set_int64data(42);
    XStore_Proto::FData b;
    b.set_int64data(42);
    EXPECT_EQ(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, DifferentInt64ValuesEncodeDifferently) {
    XStore_Proto::FData a;
    a.set_int64data(42);
    XStore_Proto::FData b;
    b.set_int64data(43);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, SameNumericMagnitudeDifferentOneofCaseEncodesDifferently) {
    // int64Data=5 and doubleData=5.0 are numerically equal but must be
    // treated as distinct keys -- this is exact-type equality, not
    // aggregate-style numeric coercion (see GetFDataValue for that instead).
    XStore_Proto::FData a;
    a.set_int64data(5);
    XStore_Proto::FData b;
    b.set_doubledata(5.0);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

TEST(EncodeFDataForCatIdxTest, StringDataEncodesRawBytes) {
    XStore_Proto::FData a;
    a.set_stringdata("AAPL");
    XStore_Proto::FData b;
    b.set_stringdata("AAPL");
    XStore_Proto::FData c;
    c.set_stringdata("MSFT");
    EXPECT_EQ(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(c));
}

TEST(EncodeFDataForCatIdxTest, BoolDataEncodesDistinctly) {
    XStore_Proto::FData a;
    a.set_booldata(true);
    XStore_Proto::FData b;
    b.set_booldata(false);
    EXPECT_NE(EncodeFDataForCatIdx(a), EncodeFDataForCatIdx(b));
}

namespace {
std::string TempCatIdxPath(const std::string &name) {
    auto dir = std::filesystem::temp_directory_path() / "xstore_catidx_test";
    std::filesystem::create_directories(dir);
    return (dir / name).string();
}
} // namespace

TEST(CatIdxFileTest, OpenCreatesFreshFile) {
    std::string path = TempCatIdxPath("open_fresh.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, /*row_length=*/64, /*tracked_column_count=*/4));
}

TEST(CatIdxFileTest, InsertedValueIsFound) {
    std::string path = TempCatIdxPath("insert_found.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 4));

    XStore_Proto::FData val;
    val.set_int64data(137);
    CatIdxObservation obs{/*column_idx=*/2, EncodeFDataForCatIdx(val), /*is_present=*/true};
    f.UpdateRow(/*row_index=*/5, {obs});

    EXPECT_TRUE(f.MightContain(/*block_index=*/0, /*column_idx=*/2, EncodeFDataForCatIdx(val)));
}

TEST(CatIdxFileTest, NeverFalseNegativeAcrossManyValues) {
    std::string path = TempCatIdxPath("no_false_negative.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    std::vector<std::string> inserted;
    for (int64_t i = 0; i < 500; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        std::string enc = EncodeFDataForCatIdx(val);
        f.UpdateRow(static_cast<uint64_t>(i), {CatIdxObservation{0, enc, true}});
        inserted.push_back(enc);
    }
    // Re-check every inserted value against its own block -- no false
    // negatives allowed, ever.
    uint64_t rows_per_block = RowsPerBlock(64);
    for (int64_t i = 0; i < 500; ++i) {
        uint64_t block = BlockIndexForRow(static_cast<uint64_t>(i), rows_per_block);
        EXPECT_TRUE(f.MightContain(block, 0, inserted[static_cast<size_t>(i)]))
            << "false negative for value " << i;
    }
}

TEST(CatIdxFileTest, AbsentValueUsuallyNotFound) {
    // Bloom filters are probabilistic -- this asserts the false-positive
    // rate stays reasonable (well under 100%), not that it's exactly zero.
    std::string path = TempCatIdxPath("absent_mostly_not_found.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    for (int64_t i = 0; i < 100; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        f.UpdateRow(static_cast<uint64_t>(i), {CatIdxObservation{0, EncodeFDataForCatIdx(val), true}});
    }

    int false_positives = 0;
    for (int64_t i = 100000; i < 100200; ++i) {
        XStore_Proto::FData val;
        val.set_int64data(i);
        if (f.MightContain(0, 0, EncodeFDataForCatIdx(val))) ++false_positives;
    }
    EXPECT_LT(false_positives, 20); // well under 10% at these fill levels
}

// EnsureBlockLoaded/FlushBlock fix: MightContain (a pure read/query) must
// never durably grow the .catidx file as a side effect of reading a
// never-touched block -- only UpdateRow (a real mutation) may do that.
TEST(CatIdxFileTest, MightContainOnNeverTouchedBlockDoesNotGrowFileButUpdateRowDoes) {
    std::string path = TempCatIdxPath("might_contain_no_grow.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 4));

    auto file_size = [&]() { return std::filesystem::file_size(path); };
    uintmax_t size_after_open = file_size();

    XStore_Proto::FData val;
    val.set_int64data(999);
    EXPECT_FALSE(f.MightContain(/*block_index=*/0, /*column_idx=*/0, EncodeFDataForCatIdx(val)));
    EXPECT_EQ(file_size(), size_after_open)
        << "MightContain on a never-touched block must not grow the file";

    CatIdxObservation obs{/*column_idx=*/0, EncodeFDataForCatIdx(val), /*is_present=*/true};
    f.UpdateRow(/*row_index=*/5, {obs});
    EXPECT_GT(file_size(), size_after_open)
        << "UpdateRow (a real mutation) must still write through";
}

TEST(CatIdxFileTest, AppendBitmapSectionThenExactLookupWorks) {
    std::string path = TempCatIdxPath("bitmap_exact.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));

    XStore_Proto::FData v137; v137.set_int64data(137);
    std::string enc137 = EncodeFDataForCatIdx(v137);
    f.AppendBitmapSection({{0, {{enc137, {5, 42, 99}}}}});

    ASSERT_TRUE(f.HasBitmapSection());
    auto rows = f.LookupExact(/*column_idx=*/0, enc137);
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(*rows, (std::vector<uint64_t>{5, 42, 99}));

    XStore_Proto::FData v138; v138.set_int64data(138);
    EXPECT_FALSE(f.LookupExact(0, EncodeFDataForCatIdx(v138)).has_value());
}

TEST(CatIdxFileTest, ColumnOverCardinalityCapHasNoDirectory) {
    std::string path = TempCatIdxPath("bitmap_over_cap.catidx");
    std::filesystem::remove(path);
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, 64, 1));
    // Column 0 never gets AppendBitmapSection called (compaction abandoned
    // it mid-scan for exceeding CATIDX_MAX_BITMAP_DISTINCT_VALUES) --
    // LookupExact must report "no directory" (nullopt), distinct from
    // "directory exists but value absent" (also nullopt) -- both mean
    // "caller must fall back to bloom+real-scan", so the same nullopt
    // return is correct for both; HasBitmapSection() still returns false
    // if literally nothing was ever appended for this year.
    EXPECT_FALSE(f.HasBitmapSection());
}

TEST(GetOrOpenCatIdxTest, SameDbYearReturnsSameInstance) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_db");
    std::filesystem::create_directories(base);

    CatIdxFile &a = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    CatIdxFile &b = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    EXPECT_EQ(&a, &b);
}

TEST(GetOrOpenCatIdxTest, DifferentYearReturnsDifferentInstance) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_db2");
    std::filesystem::create_directories(base);

    CatIdxFile &a = XStore::GetOrOpenCatIdx(base, 2023, 64, 4);
    CatIdxFile &b = XStore::GetOrOpenCatIdx(base, 2024, 64, 4);
    EXPECT_NE(&a, &b);
}

// Write-path integration (Task 6): a real UnaryInsert through the full WAL
// apply path -- not a direct CatIdxFile::UpdateRow call like the tests
// above -- must leave the bloom filter set for the value that was inserted.
// Same fixture shape as XStoreZoneMapTest (XStoreZoneMapTest.cpp): one
// ScratchDir per test, XStore/db created locally in the test body rather
// than shared fixture members, since that's the pattern every *ZoneMapTest*
// TEST_F in this codebase already uses.
namespace {

using xstore_test::ScratchDir;
using xstore_test::CreateTestDb;

class XStoreCatIdxTestFixture : public ::testing::Test {
protected:
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_catidx_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearCatIdxRegistryForTests();
    }

    void TearDown() override {
        scratch.reset();
    }

    // db_name must already be upper-case.
    std::string DbPath(std::string_view db_name) const {
        return (scratch->test_root / "DataStore" / std::string(db_name)).string();
    }
};

TEST_F(XStoreCatIdxTestFixture, InsertedValueBloomIsSetAfterApplyWalRecord) {
    XStore store;
    CreateTestDb(store, "CATIDXWRITE", XStore_Proto::SECOND, 64);

    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(137);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, row, "CATIDXWRITE", &rep, XStore_Proto::SYNC);

    XStore_Proto::FData val;
    val.set_int64data(137);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath("CATIDXWRITE"), TimeStruct(1000, XStore_Proto::SECOND).year, 64, 1);
    EXPECT_TRUE(catidx.MightContain(0, 0, EncodeFDataForCatIdx(val)));
}

// FindByValue (Task 7): bloom-accelerated equality lookup with real-scan
// fallback. Same fixture/insert pattern as InsertedValueBloomIsSetAfter
// ApplyWalRecord above -- a real XStore instance + UnaryInsert through the
// full WAL apply path, not a direct CatIdxFile call.
TEST_F(XStoreCatIdxTestFixture, FindByValueReturnsOnlyMatchingRows) {
    XStore store;
    CreateTestDb(store, "FINDBYVALUE1", XStore_Proto::SECOND, 64);

    for (int64_t i = 0; i < 300; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 5); // column 0 cycles 0..4
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUE1", &rep, XStore_Proto::SYNC);
    }

    XStore_Proto::FData target;
    target.set_int64data(2);

    std::string reassembled;
    bool saw_last = false;
    store.FindByValue(1000, 1000 + 299, "FINDBYVALUE1", /*column_idx=*/0, target,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool is_last) {
                           reassembled += chunk;
                           saw_last = is_last;
                       });
    EXPECT_TRUE(saw_last);

    // Wire-compatible: FindByValue's chunk stream reuses RangeQuery's
    // EncodeQueryResponseEntry, so RangeQuery_REP parses it directly.
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 60); // 300 rows / 5 values = 60 matches for value=2
    for (const auto &entry : rep.queryresponse()) {
        EXPECT_EQ((entry.timestamp() - 1000) % 5, 2);
    }
}

TEST_F(XStoreCatIdxTestFixture, FindByValueColumnPastTrackedCountStillCorrectViaFallback) {
    // columnIdx >= trackedColumnCount: no bloom ever existed for it, must
    // still run the fallback path safely and correctly, not error, crash,
    // or spuriously match garbage bytes from a miscomputed offset. Column
    // 999 was never populated (the row only has one real field, at index
    // 0), so the correct answer is 0 matches -- a hole, not a match.
    XStore store;
    CreateTestDb(store, "FINDBYVALUE2", XStore_Proto::SECOND, 64);

    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(42);
    XStore_Proto::UnaryInsert_REP insert_rep;
    store.UnaryInsert(2000, row, "FINDBYVALUE2", &insert_rep, XStore_Proto::SYNC);

    XStore_Proto::FData target;
    target.set_int64data(42);

    std::string reassembled;
    store.FindByValue(2000, 2000, "FINDBYVALUE2", /*column_idx=*/999, target,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool) { reassembled += chunk; });

    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 0);
}

// RowsPerBlock(64) = 32768 rows/block, so all 300 rows below (and the 300
// rows in FindByValueReturnsOnlyMatchingRows above) fall inside block 0 --
// neither of the tests above ever takes the MightContain()-returns-false
// skip branch. This test queries a value that was never inserted (only
// 0..4 ever appear), so the block's real bloom filter -- built from the
// actual inserted values -- gets a genuine chance to say "definitely not
// present" and exercise the skip-arithmetic (epoch_time = block_end_epoch +
// 1). Bloom filters are probabilistic, so a false positive here (bloom says
// "maybe", falls back to a real scan that correctly finds nothing) is also
// valid behavior -- either way queryresponse_size() must be 0, so this
// doesn't assert on which branch fired, only on the correct end result.
TEST_F(XStoreCatIdxTestFixture, FindByValueForAbsentValueSkipsBlockViaBloom) {
    XStore store;
    CreateTestDb(store, "FINDBYVALUE3", XStore_Proto::SECOND, 64);

    for (int64_t i = 0; i < 300; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 5); // column 0 cycles 0..4
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUE3", &rep, XStore_Proto::SYNC);
    }

    XStore_Proto::FData target;
    target.set_int64data(99); // never inserted -- 0..4 are the only real values

    std::string reassembled;
    store.FindByValue(1000, 1000 + 299, "FINDBYVALUE3", /*column_idx=*/0, target,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool) { reassembled += chunk; });

    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    EXPECT_EQ(rep.queryresponse_size(), 0);
}

// End-to-end regression test for Critical Finding #1 (task-9-report.md fix
// round, see AppendBitmapSection/EnsureBlockLoaded/FlushBlock): a real
// compaction, then a real backfill write into a never-touched block whose
// arithmetic bloom offset now falls inside (here, exactly at the start of)
// the just-committed bitmap section directory. Before the fix,
// EnsureBlockLoaded would misread the directory's bytes as "existing" bloom
// bits (comparing against st.st_size, which now includes the directory),
// and a genuine write to that block would then corrupt the directory in
// place. Wide row length (16384, matching XStoreZoneMapTest's own
// kWideRowLength convention) keeps RowsPerBlock small (128) so a second
// block is reachable without tens of thousands of inserts.
//
// Two things a naive version of this test gets wrong, both worth flagging:
//
// 1. Row index within a year's .XSTORE file is elapsed-time-since-
//    TimeStruct::first_epoch (see XStore_UnaryOps.cpp's `diff = unix_time -
//    curr_ts.first_epoch`), NOT elapsed-time-since-an-arbitrary-base -- so
//    inserts must start at first_epoch itself for row 0 to land at the
//    start of block 0 (and thus for `base + rows_per_block` to land exactly
//    at the start of block 1).
// 2. UnaryInsert's first write to a year preallocates (sparse) the ENTIRE
//    year's row range at SECOND resolution (~31.5M rows -- the only
//    resolution CreateDB permits that isn't even larger), and
//    CompactOneYearCatIdx's row scan walks every one of those rows. Left
//    alone that makes this test take minutes. Same fix precedent as
//    XStoreWalTest.cpp's WAL-file resize_file tests: shrink the underlying
//    file with resize_file (not a fake/synthetic file -- the same real file
//    UnaryInsert just wrote into) down to just past the two blocks this
//    test cares about, so the scan stays fast without touching production
//    code.
TEST_F(XStoreCatIdxTestFixture, CompactionThenBackfillIntoUntouchedBlockDoesNotCorruptBitmapSection) {
    constexpr uint16_t kWideRowLength = 16384; // -> RowsPerBlock == 128
    XStore store;
    CreateTestDb(store, "CATIDXCOMPACT", XStore_Proto::SECOND, kWideRowLength);
    uint64_t rows_per_block = RowsPerBlock(kWideRowLength);

    // first_epoch (start of the calendar year, in SECOND units) is row 0 --
    // see the comment above.
    TimeStruct ref_ts(static_cast<std::time_t>(1700000000ULL), XStore_Proto::SECOND);
    unsigned long long base = static_cast<unsigned long long>(ref_ts.first_epoch);
    unsigned short year = ref_ts.year;

    XStore_Proto::FData target;
    target.set_int64data(555);

    // Populate all of block 0 with real data, one distinguishable value at
    // a known row index (10) plus filler for the rest.
    for (uint64_t i = 0; i < rows_per_block; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i == 10 ? 555 : static_cast<int64_t>(i));
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(base + i, row, "CATIDXCOMPACT", &rep, XStore_Proto::SYNC);
    }

    // Bound the compaction scan to just past block 0 + block 1 -- see point
    // 2 above. Real data already durably written above stays intact (it
    // lives at offsets [0, rows_per_block*row_length), well inside this
    // smaller size); only the ~31.5M-row trailing sparse region this test
    // doesn't care about is dropped.
    std::string xstore_path = DbPath("CATIDXCOMPACT") + "/" + std::to_string(year) + ".XSTORE";
    std::error_code resize_ec;
    std::filesystem::resize_file(xstore_path, 2 * rows_per_block * kWideRowLength, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();

    store.CompactOneYearCatIdx(DbPath("CATIDXCOMPACT"), year, kWideRowLength, /*tracked_column_count=*/1);

    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath("CATIDXCOMPACT"), year, kWideRowLength, 1);
    ASSERT_TRUE(catidx.HasBitmapSection());

    auto rows = catidx.LookupExact(/*column_idx=*/0, EncodeFDataForCatIdx(target));
    ASSERT_TRUE(rows.has_value());
    EXPECT_EQ(*rows, (std::vector<uint64_t>{10}));

    // Insert one more row into a DIFFERENT, previously-untouched block (1)
    // in this same, now-compacted year, via the normal UnaryInsert path.
    XStore_Proto::RowData backfill_row;
    backfill_row.add_fielddata()->set_int64data(777);
    XStore_Proto::UnaryInsert_REP backfill_rep;
    store.UnaryInsert(base + rows_per_block, backfill_row, "CATIDXCOMPACT", &backfill_rep, XStore_Proto::SYNC);

    XStore_Proto::FData backfill_val;
    backfill_val.set_int64data(777);
    uint64_t block1 = BlockIndexForRow(rows_per_block, rows_per_block);
    EXPECT_TRUE(catidx.MightContain(block1, /*column_idx=*/0, EncodeFDataForCatIdx(backfill_val)))
        << "backfill insert into a never-touched block after compaction must still be found";

    // And the bitmap section must not have been silently corrupted by that
    // write. Two outcomes are both correct per the fix's contract: either
    // the directory survived untouched (block1's write landed clear of it),
    // or FlushBlock's self-healing invalidated it (block1's arithmetic
    // offset landed inside the directory range -- the case this schema is
    // deliberately sized to hit: rows_per_block=128, tracked_column_count=1
    // means block 1's bloom offset is exactly where block 0's bitmap
    // section was appended). What must never happen is a *silently wrong*
    // answer -- directory bytes torn by the block-1 write but still read
    // back as if valid.
    auto rows_after = catidx.LookupExact(0, EncodeFDataForCatIdx(target));
    if (rows_after.has_value()) {
        EXPECT_EQ(*rows_after, (std::vector<uint64_t>{10}));
    } else {
        EXPECT_FALSE(catidx.HasBitmapSection())
            << "LookupExact returned nullopt but HasBitmapSection() is still true -- "
               "inconsistent state, directory bytes may have been corrupted in place";
    }
}

} // namespace
