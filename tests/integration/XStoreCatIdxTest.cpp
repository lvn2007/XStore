#include <gtest/gtest.h>
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore.h"
#include "include/DataStructs/TimeStruct.h"
#include "XStore.pb.h"
#include "XStoreSyscallFault.h"
#include "XStoreTestSupport.h"
#include "XStoreRebuildTestSupport.h"
#include <algorithm>
#include <cstring>
#include <functional>
#include <limits>
#include <random>
#include <filesystem>
#include <optional>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <spdlog/fmt/fmt.h>

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

TEST(CatIdxFileTest, OpenDiscardsV1FileInsteadOfMisreadingIt) {
    // Same discard-not-migrate contract as ZoneMapFile's version tests: a
    // v1 .catidx (before the Durability A `dirty`/`reserved` fields existed)
    // must never be reinterpreted as v2 -- it must be truncated and
    // recreated fresh.
    std::string path = TempCatIdxPath("stale_v1.catidx");
    std::filesystem::remove(path);
    {
        CatIdxFileHeader old_header;
        old_header.version = 1; // stale -- current CATIDX_VERSION is 2
        old_header.tracked_column_count = 4;
        int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(pwrite(fd, &old_header, sizeof(old_header), 0), static_cast<ssize_t>(sizeof(old_header)));
        close(fd);
    }

    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, /*row_length=*/64, /*tracked_column_count=*/4));

    XStore_Proto::FData val;
    val.set_int64data(137);
    CatIdxObservation obs{/*column_idx=*/2, EncodeFDataForCatIdx(val), /*is_present=*/true};
    f.UpdateRow(/*row_index=*/5, {obs});
    EXPECT_TRUE(f.MightContain(/*block_index=*/0, /*column_idx=*/2, EncodeFDataForCatIdx(val)));

    CatIdxFileHeader on_disk{};
    int fd = open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pread(fd, &on_disk, sizeof(on_disk), 0), static_cast<ssize_t>(sizeof(on_disk)));
    close(fd);
    EXPECT_EQ(on_disk.version, XStoreTunables::CATIDX_VERSION);
    EXPECT_EQ(on_disk.dirty, 1u); // a write happened this session (Durability A Task 2)
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
        XStore::ClearLiveWidenRequestsForTests(); // another test's growth would make compaction yield
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

// A null (unset) target is refused with a warning and no rows, whether or not the column is indexed: the bloom never
// holds unset columns, so an indexed column would answer "none" while the real-scan fallback would match the
// explicitly-unset cells. Column 0 is indexed, column 999 is past the tracked width (real scan).
TEST_F(XStoreCatIdxTestFixture, FindByValueRefusesANullTargetOnIndexedAndUnindexedColumns) {
    XStore store;
    CreateTestDb(store, "FINDBYVALUE4", XStore_Proto::SECOND, 64);

    for (int64_t i = 0; i < 300; ++i) {
        XStore_Proto::RowData row;
        XStore_Proto::FData *c0 = row.add_fielddata();
        if (i % 3 != 0) c0->set_int64data(i); // every third cell stays unset
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUE4", &rep, XStore_Proto::SYNC);
    }

    const XStore_Proto::FData null_target; // no oneof case set
    for (int32_t column : {0, 999}) {
        std::string reassembled;
        store.FindByValue(1000, 1000 + 299, "FINDBYVALUE4", column, null_target,
                          XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [&](std::string &&chunk, bool) { reassembled += chunk; });
        XStore_Proto::RangeQuery_REP rep;
        ASSERT_TRUE(rep.ParseFromString(reassembled)) << "column " << column;
        EXPECT_EQ(rep.queryresponse_size(), 0) << "column " << column;
        EXPECT_NE(rep.querystatus().find("Searching for a null (unset) value is not supported"), std::string::npos)
            << "column " << column << ": " << rep.querystatus();
    }
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

// FindByValue bitmap-aware query path (Task 10), corrected by the final
// whole-branch review's Critical C1. The bitmap directory is a
// COMPACTION-TIME SNAPSHOT -- a candidate filter, never an authority on
// what the rows currently hold. This test's earlier version asserted the
// opposite (that a post-compaction overwrite of 75 rows to the target value
// must still return only the 25 rows the directory recorded), which locked
// the C1 bug in as expected behavior: the true correct answer is 100, since
// all 100 rows really do carry the target value by then.
//
// It now proves three separate things, without ever using a WRONG answer as
// the discriminator for "the bitmap path ran":
//   1. The bitmap path's preconditions genuinely hold after compaction --
//      HasBitmapSection() is true and a direct LookupExact returns the 25
//      recorded rows (asserted directly on the CatIdxFile, not inferred
//      from an end-to-end result).
//   2. With the directory live and the data untouched, FindByValue returns
//      the correct 25.
//   3. After a post-compaction overwrite through the normal write path, the
//      directory is invalidated (FlushBlock self-heal -- C1 part 2) and
//      FindByValue returns the TRUE answer of 100, not the stale 25.
TEST_F(XStoreCatIdxTestFixture, FindByValueUsesBitmapDirectoryNotRealScanAfterCompaction) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    CreateTestDb(store, "FINDBYVALUEBITMAP", XStore_Proto::SECOND, kRowLength);
    uint64_t rows_per_block = RowsPerBlock(kRowLength);

    for (int64_t i = 0; i < 100; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i % 4);
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUEBITMAP", &rep, XStore_Proto::SYNC);
    }

    unsigned short year = TimeStruct(1000, XStore_Proto::SECOND).year;

    // Bound CompactOneYearCatIdx's row scan to block 0 only -- same
    // resize_file trick as CompactionThenBackfillIntoUntouchedBlockDoesNot
    // CorruptBitmapSection above, avoids walking the full ~31.5M-row sparse
    // SECOND-resolution year. GetOrMapFile (queries) grows the file back
    // via ftruncate and UnaryInsert's pwrite extends it as needed once they
    // touch rows past this.
    std::string xstore_path = DbPath("FINDBYVALUEBITMAP") + "/" + std::to_string(year) + ".XSTORE";
    std::error_code resize_ec;
    std::filesystem::resize_file(xstore_path, rows_per_block * kRowLength, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();

    store.CompactOneYearCatIdx(DbPath("FINDBYVALUEBITMAP"), year, kRowLength, /*tracked_column_count=*/1);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath("FINDBYVALUEBITMAP"), year, kRowLength, 1);
    ASSERT_TRUE(catidx.HasBitmapSection());

    XStore_Proto::FData target;
    target.set_int64data(3);
    auto rows_at_compaction = catidx.LookupExact(/*column_idx=*/0, EncodeFDataForCatIdx(target));
    ASSERT_TRUE(rows_at_compaction.has_value());
    ASSERT_EQ(rows_at_compaction->size(), 25u); // 100 rows / 4 values = 25 matches for value=3

    auto find = [&]() {
        std::string reassembled;
        store.FindByValue(1000, 1099, "FINDBYVALUEBITMAP", /*column_idx=*/0, target,
                           XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                           [&](std::string &&chunk, bool) { reassembled += chunk; });
        XStore_Proto::RangeQuery_REP rep;
        EXPECT_TRUE(rep.ParseFromString(reassembled));
        return rep;
    };

    // (2) Directory live, data untouched -> the bitmap path runs and agrees
    // with reality.
    XStore_Proto::RangeQuery_REP before = find();
    EXPECT_EQ(before.queryresponse_size(), 25);
    for (const auto &entry : before.queryresponse()) {
        EXPECT_EQ((entry.timestamp() - 1000) % 4, 3);
    }

    // Overwrite every non-matching row with the target value, through the
    // normal write path.
    for (int64_t i = 0; i < 100; ++i) {
        if (i % 4 == 3) continue;
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(3);
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUEBITMAP", &rep, XStore_Proto::SYNC);
    }

    // (3) Any write into an already-compacted file invalidates the snapshot
    // (C1 part 2), so the stale directory can't be trusted or consulted...
    EXPECT_FALSE(catidx.HasBitmapSection())
        << "a post-compaction write must invalidate the bitmap section -- otherwise the "
           "directory silently drifts from real row data";
    // ...and the answer is the TRUE one: all 100 rows now hold value 3.
    XStore_Proto::RangeQuery_REP after = find();
    EXPECT_EQ(after.queryresponse_size(), 100);
}

// C1 (final whole-branch review), the false-negative half: a row inserted
// AFTER compaction must still be found by FindByValue. Under the old code
// the bitmap directory stayed committed across such a backfill (its bloom
// write never physically overlapped the directory bytes, so FlushBlock's
// overlap-only invalidation check never tripped) and FindByValue trusted
// LookupExact's compaction-time row list as complete -- so the new row was
// never returned, directly violating this feature's own core invariant.
TEST_F(XStoreCatIdxTestFixture, FindByValueFindsRowBackfilledAfterCompaction) {
    constexpr uint16_t kRowLength = 64;
    XStore store;
    CreateTestDb(store, "FINDBYVALUEBACKFILL", XStore_Proto::SECOND, kRowLength);
    uint64_t rows_per_block = RowsPerBlock(kRowLength);

    // Rows 0..49 at epochs 1000..1049; only row 7 carries the target value.
    for (int64_t i = 0; i < 50; ++i) {
        XStore_Proto::RowData row;
        row.add_fielddata()->set_int64data(i == 7 ? 909 : i);
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(1000 + i, row, "FINDBYVALUEBACKFILL", &rep, XStore_Proto::SYNC);
    }

    TimeStruct ref_ts(static_cast<std::time_t>(1000), XStore_Proto::SECOND);
    unsigned short year = ref_ts.year;
    const uint64_t first_epoch = static_cast<uint64_t>(ref_ts.first_epoch);
    // Bound the compaction scan to block 0, same resize_file trick the other
    // compaction tests here use (see their comments).
    std::string xstore_path = DbPath("FINDBYVALUEBACKFILL") + "/" + std::to_string(year) + ".XSTORE";
    std::error_code resize_ec;
    std::filesystem::resize_file(xstore_path, rows_per_block * kRowLength, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();

    store.CompactOneYearCatIdx(DbPath("FINDBYVALUEBACKFILL"), year, kRowLength, /*tracked_column_count=*/1);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath("FINDBYVALUEBACKFILL"), year, kRowLength, 1);
    ASSERT_TRUE(catidx.HasBitmapSection());

    XStore_Proto::FData target;
    target.set_int64data(909);
    auto snapshot = catidx.LookupExact(/*column_idx=*/0, EncodeFDataForCatIdx(target));
    ASSERT_TRUE(snapshot.has_value());
    ASSERT_EQ(*snapshot, (std::vector<uint64_t>{1007ULL - first_epoch})); // only row 7 at compaction time

    // Backfill a NEW row carrying the target value, into the same already-
    // compacted block the directory snapshot covers.
    XStore_Proto::RowData backfill;
    backfill.add_fielddata()->set_int64data(909);
    XStore_Proto::UnaryInsert_REP backfill_rep;
    store.UnaryInsert(1077, backfill, "FINDBYVALUEBACKFILL", &backfill_rep, XStore_Proto::SYNC);

    std::string reassembled;
    store.FindByValue(1000, 1099, "FINDBYVALUEBACKFILL", /*column_idx=*/0, target,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool) { reassembled += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));

    ASSERT_EQ(rep.queryresponse_size(), 2)
        << "a row backfilled after compaction must still be found -- the bitmap directory is a "
           "compaction-time snapshot, not an authority on current row contents";
    std::vector<unsigned long long> found;
    for (const auto &entry : rep.queryresponse()) found.push_back(entry.timestamp());
    std::sort(found.begin(), found.end());
    EXPECT_EQ(found, (std::vector<unsigned long long>{1007, 1077}));
}

// Task 11: proves Task 4's tracked-column-count growth and this task's
// InitDB() rebuild compose correctly end-to-end -- same shape as
// XStoreZoneMapTest's ColumnCountGrowthTriggersRebuildOnNextInitDb, but the
// real assertion here is the false-negative fix specifically, not just that
// tracked_column_count converges. The brief's original (wrong) design would
// have discarded the stale .catidx and left its bloom repopulate "lazily via
// ordinary inserts" -- which is unreachable for a row that gets no further
// insert after the rebuild. Row 1 below (column 0 = 424242, inserted at
// epoch 1000) is exactly that row: it is never touched again after InitDB()
// runs, so it can only still be found if RebuildStaleCatIdxForDb genuinely
// rescanned real row data (RebuildOneYearCatIdxBloom), not merely deleted
// and walked away. Under the buggy design, the recreated .catidx's bloom
// entry for this row's block would be all-zero, and
// CatIdxFile::MightContain returns false (definite absence) the instant it
// hits the first unset bit -- so FindByValue's `if (!maybe) must_scan =
// false;` block-skip (XStore_Aggregates.cpp) would silently skip this block
// entirely, never falling back to a real scan. That makes this a genuine
// RED/GREEN discriminator, not a coincidental-correctness check: it fails
// under the brief's original design and passes under the corrected one.
TEST_F(XStoreCatIdxTestFixture, ColumnCountGrowthTriggersCatIdxRebuildAndPreservesBloomForUntouchedRow) {
    XStore store;
    std::string db_name = "CATIDXCOLGROWTH";
    CreateTestDb(store, db_name, XStore_Proto::SECOND, 1536); // no seed -- starts at global default (64)

    // Row 1: narrow, single column -- also the insert that first creates
    // this year's .catidx, sizing its bloom at whatever
    // EffectiveTrackedColumnCount(cfg) is at that moment (64, unseeded).
    // This row receives NO further insert after the rebuild below.
    XStore_Proto::RowData narrow_row;
    narrow_row.add_fielddata()->set_int64data(424242);
    XStore_Proto::UnaryInsert_REP narrow_rep;
    store.UnaryInsert(1000, narrow_row, db_name, &narrow_rep, XStore_Proto::SYNC);

    // Row 2: column index 69 populated -- past the default 64 tracked cap.
    // MaybeGrowColumnCount grows columnCount to >=70 as a side effect of
    // this insert, but the .catidx created above was already sized at 64
    // and is never resized on the request path.
    XStore_Proto::RowData wide_row;
    for (int i = 0; i < 70; ++i) wide_row.add_fielddata()->set_int64data(i);
    wide_row.mutable_fielddata(69)->set_int64data(999);
    XStore_Proto::UnaryInsert_REP wide_rep;
    store.UnaryInsert(1001, wide_row, db_name, &wide_rep, XStore_Proto::SYNC);

    auto &shard = XStore::cfg_shards[std::hash<std::string>{}(db_name) % XStore::CFG_SHARD_COUNT];
    ASSERT_TRUE(shard.map.count(db_name));
    uint32_t grown_count = shard.map.at(db_name).columncount();
    ASSERT_GE(grown_count, 70u);

    unsigned short year = TimeStruct(1000, XStore_Proto::SECOND).year;
    std::string catidx_path = fmt::format("{}/{}.catidx", DbPath(db_name), year);
    auto read_tracked_column_count = [&]() {
        int fd = open(catidx_path.c_str(), O_RDONLY);
        EXPECT_GE(fd, 0);
        CatIdxFileHeader header{};
        EXPECT_EQ(pread(fd, &header, sizeof(header), 0), static_cast<ssize_t>(sizeof(header)));
        close(fd);
        return header.tracked_column_count;
    };

    // On-disk .catidx was created at the old (narrower) cap -- confirm it's
    // stale before InitDB() rebuilds it.
    EXPECT_LT(read_tracked_column_count(), grown_count);

    // Re-run InitDB() the same way a server restart would -- this is what
    // notices the tracked-column-count mismatch and rebuilds. Deliberately
    // NOT preceded by a manual ClearCatIdxRegistryForTests() call here: this
    // is what proves RebuildOneYearCatIdxBloom's own registry-eviction fix
    // (see its comment in XStore_CatIdx.cpp) actually works even though
    // this test's own inserts above already warmed the registry for
    // (db_name, year) -- same faithful-in-process-restart precedent as the
    // zone-map version of this test.
    store.InitDB();

    EXPECT_EQ(read_tracked_column_count(), grown_count);

    // THE REAL CORRECTNESS CHECK: row 1000's value (column 0 = 424242) was
    // never re-inserted after the rebuild.
    XStore_Proto::FData narrow_val;
    narrow_val.set_int64data(424242);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db_name), year, 1536, grown_count);
    EXPECT_TRUE(catidx.MightContain(/*block_index=*/0, /*column_idx=*/0, EncodeFDataForCatIdx(narrow_val)))
        << "bloom false negative for a row that received no insert after rebuild -- "
           "the discard-and-lazy-repopulate bug is back";

    // And FindByValue end-to-end confirms the same thing through the real
    // query path (which is what actually exercises the
    // `if (!maybe) must_scan = false;` block-skip this bug would trip).
    std::string reassembled;
    store.FindByValue(1000, 1000, db_name, /*column_idx=*/0, narrow_val,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool) { reassembled += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    ASSERT_TRUE(rep.ParseFromString(reassembled));
    ASSERT_EQ(rep.queryresponse_size(), 1);
    EXPECT_EQ(rep.queryresponse(0).timestamp(), 1000u);
}

// Review fix round, Finding 2: a year with real row data but NO .catidx
// sidecar at all (every pre-existing year, the first time this feature
// lands on old data) must get a real, scan-based rebuild during InitDB() --
// not a lazy, all-zero one the first time GetOrOpenCatIdx/FindByValue
// happens to touch it. The old RebuildStaleCatIdxForDb iterated .catidx
// files, so a year with no .catidx was invisible to it entirely.
TEST_F(XStoreCatIdxTestFixture, MissingCatIdxFileForYearWithRealDataGetsRealRebuildOnInitDbNotLazyAllZero) {
    XStore store;
    std::string db_name = "CATIDXMISSINGFILE";
    CreateTestDb(store, db_name, XStore_Proto::SECOND, 64);

    // Live insert -- writes real row data AND, as a side effect of the
    // normal insert path, creates this year's .catidx sidecar.
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(777777);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, row, db_name, &rep, XStore_Proto::SYNC);

    unsigned short year = TimeStruct(1000, XStore_Proto::SECOND).year;
    std::string catidx_path = fmt::format("{}/{}.catidx", DbPath(db_name), year);
    ASSERT_TRUE(std::filesystem::exists(catidx_path)) << "insert should have created the .catidx sidecar";

    // Reproduce the exact starting condition Finding 2 closes: real row
    // data on disk, but no .catidx at all for that year. Deleting it
    // directly (not through RebuildStaleCatIdxForDb) simulates a
    // pre-existing deployment that never had this feature's sidecars.
    ASSERT_TRUE(std::filesystem::remove(catidx_path));
    ASSERT_FALSE(std::filesystem::exists(catidx_path));

    // Real restart simulation -- deliberately not preceded by a manual
    // registry clear, so this also exercises RebuildOneYearCatIdxBloom's
    // own registry-eviction fix against the still-warm entry the insert
    // above left behind (same precedent as the column-count-growth test
    // above).
    store.InitDB();

    // THE DISCRIMINATOR: the .catidx file must already exist again, with
    // real data, the moment InitDB() returns -- BEFORE any query ever
    // touches this year. Under the pre-fix behavior (iterating .catidx
    // files, which found nothing to iterate since none existed), InitDB()
    // would do nothing here at all, and the file would only spring into
    // existence later, all-zero, the first time GetOrOpenCatIdx/
    // FindByValue happened to touch this year -- which then permanently
    // and silently fails to find this exact row (a bloom "no" on an
    // all-zero entry is guaranteed on the very first bit check).
    ASSERT_TRUE(std::filesystem::exists(catidx_path))
        << "InitDB() must rebuild a missing .catidx from real row data, not leave it "
           "for a later lazy/all-zero GetOrOpenCatIdx create";

    XStore_Proto::FData val;
    val.set_int64data(777777);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db_name), year, 64, /*tracked_column_count=*/1);
    EXPECT_TRUE(catidx.MightContain(/*block_index=*/0, /*column_idx=*/0, EncodeFDataForCatIdx(val)))
        << "bloom false negative -- missing .catidx was never really rebuilt, just lazily recreated all-zero";

    std::string reassembled;
    store.FindByValue(1000, 1000, db_name, /*column_idx=*/0, val,
                       XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                       [&](std::string &&chunk, bool) { reassembled += chunk; });
    XStore_Proto::RangeQuery_REP find_rep;
    ASSERT_TRUE(find_rep.ParseFromString(reassembled));
    ASSERT_EQ(find_rep.queryresponse_size(), 1);
    EXPECT_EQ(find_rep.queryresponse(0).timestamp(), 1000u);
}

} // namespace

// M4 stage 1: live widening mechanics in CatIdxFile (same frontier protocol as ZoneMapFile; see
// XStoreZoneMapTest.cpp for the crash-image and concurrent-writer tests, which cover both files together).
namespace {

constexpr uint16_t kLwRowLength = 16384; // -> RowsPerBlock == 128
constexpr uint64_t kLwRpb = 128;

std::string Enc(int64_t v) {
    XStore_Proto::FData f;
    f.set_int64data(v);
    return EncodeFDataForCatIdx(f);
}

std::vector<CatIdxObservation> CatObs(uint32_t col, int64_t v) { return {{col, Enc(v), true}}; }

CatIdxFileHeader ReadCatIdxHeader(const std::string &path) {
    CatIdxFileHeader h{};
    int fd = open(path.c_str(), O_RDONLY);
    if (fd >= 0) {
        if (pread(fd, &h, sizeof(h), 0) != static_cast<ssize_t>(sizeof(h))) h.magic = 0;
        close(fd);
    }
    return h;
}

off_t CatIdxFileSize(const std::string &path) {
    struct stat st{};
    return stat(path.c_str(), &st) == 0 ? st.st_size : -1;
}

class CatIdxLiveWidenTest : public ::testing::Test {
protected:
    std::string path;
    CatIdxFile f;

    // Width 2; value 100+b in column 0 of one row of each of blocks 0..3; a bitmap section; marked clean.
    void SetUp() override {
        path = TempCatIdxPath("live_widen.catidx");
        std::filesystem::remove(path);
        ASSERT_TRUE(f.Open(path, kLwRowLength, 2));
        for (uint64_t b = 0; b < 4; ++b) f.UpdateRow(b * kLwRpb + 1, CatObs(0, 100 + static_cast<int64_t>(b)));
        f.AppendBitmapSection({{0, {{Enc(100), {1}}}}});
        ASSERT_TRUE(f.HasBitmapSection());
        ASSERT_TRUE(f.MarkClean());
    }
};

TEST_F(CatIdxLiveWidenTest, BeginRebuildWidensInPlaceAndAnswersMaybeUntilBlocksAreComplete) {
    ASSERT_FALSE(f.MightContain(0, 0, Enc(777)));
    ASSERT_TRUE(f.BeginRebuild(4));
    EXPECT_TRUE(f.Rebuilding());
    EXPECT_EQ(f.TrackedColumnCount(), 4u);
    EXPECT_FALSE(f.HasBitmapSection()) << "the bitmap section goes with the old content";
    EXPECT_FALSE(f.LookupExact(0, Enc(100)).has_value());
    const CatIdxFileHeader h = ReadCatIdxHeader(path);
    EXPECT_EQ(h.tracked_column_count, 4u);
    EXPECT_EQ(h.dirty, 1u);
    EXPECT_EQ(h.block_count, 0u);
    EXPECT_EQ(h.bitmap_section_offset, 0u);
    EXPECT_EQ(CatIdxFileSize(path), static_cast<off_t>(sizeof(CatIdxFileHeader)));
    for (uint64_t b = 0; b < 4; ++b) EXPECT_TRUE(f.MightContain(b, 0, Enc(777))) << "block " << b << " not rebuilt yet";

    // Builder folds block 0 and half of block 1 (column 3 is new).
    f.RebuildFoldRow(5, CatObs(3, 33));
    f.RebuildFoldRow(kLwRpb + 2, CatObs(0, 101));
    f.AdvanceFrontier(kLwRpb + 64);
    EXPECT_FALSE(f.MightContain(0, 0, Enc(777))) << "complete block: the bloom answers";
    EXPECT_FALSE(f.MightContain(0, 0, Enc(100))) << "old content is gone";
    EXPECT_TRUE(f.MightContain(0, 3, Enc(33)));
    EXPECT_TRUE(f.MightContain(1, 0, Enc(777))) << "straddles F";
    EXPECT_TRUE(f.MightContain(0, 9, Enc(777))) << "past the width";

    // Writers: below F folded, at/above F left to the builder; the builder ignores rows below F.
    f.UpdateRow(10, CatObs(1, 11));
    f.UpdateRow(kLwRpb + 64, CatObs(1, 12));
    f.UpdateRow(3 * kLwRpb, CatObs(1, 13));
    f.RebuildFoldRow(20, CatObs(1, 14));
    f.AdvanceFrontier(4 * kLwRpb);
    EXPECT_TRUE(f.MightContain(0, 1, Enc(11)));
    EXPECT_FALSE(f.MightContain(0, 1, Enc(14)));
    EXPECT_FALSE(f.MightContain(1, 1, Enc(12))) << "a write at F was folded although the builder owns it";
    EXPECT_FALSE(f.MightContain(3, 1, Enc(13)));
    EXPECT_TRUE(f.MightContain(1, 0, Enc(101)));

    // Compaction is refused while rebuilding; MarkClean too.
    f.AppendBitmapSection({{0, {{Enc(101), {kLwRpb + 2}}}}});
    EXPECT_FALSE(f.HasBitmapSection());
    EXPECT_FALSE(f.MarkClean());
    EXPECT_EQ(ReadCatIdxHeader(path).dirty, 1u);

    EXPECT_TRUE(f.FinishRebuild());
    EXPECT_FALSE(f.Rebuilding());
    EXPECT_EQ(ReadCatIdxHeader(path).dirty, 1u);
    EXPECT_FALSE(f.MightContain(7, 0, Enc(777))) << "no rebuild: an untouched block is empty, not unknown";
    f.AppendBitmapSection({{0, {{Enc(101), {kLwRpb + 2}}}}});
    EXPECT_TRUE(f.HasBitmapSection());
    EXPECT_TRUE(f.MarkClean());
    EXPECT_EQ(ReadCatIdxHeader(path).dirty, 0u);

    CatIdxFile reopened;
    ASSERT_TRUE(reopened.Open(path, kLwRowLength, 4));
    EXPECT_EQ(reopened.TrackedColumnCount(), 4u);
    EXPECT_TRUE(reopened.MightContain(0, 3, Enc(33)));
    EXPECT_TRUE(reopened.MightContain(1, 0, Enc(101)));
}

TEST_F(CatIdxLiveWidenTest, FailedDirtyMarkSyncLeavesTheOldIndexInPlace) {
    const off_t size_before = CatIdxFileSize(path);
    {
        xstore_fault::ScopedFdFault fault(xstore_fault::FdOp::kSync, path, 0, 1, EIO);
        EXPECT_FALSE(f.BeginRebuild(4));
        EXPECT_EQ(fault.Failures(), 1);
    }
    EXPECT_FALSE(f.Rebuilding());
    EXPECT_EQ(CatIdxFileSize(path), size_before) << "not truncated";
    EXPECT_EQ(f.TrackedColumnCount(), 2u);
    EXPECT_EQ(ReadCatIdxHeader(path).tracked_column_count, 2u);
    EXPECT_TRUE(f.HasBitmapSection());
    EXPECT_TRUE(f.MightContain(2, 0, Enc(102)));
    EXPECT_FALSE(f.MightContain(2, 0, Enc(777)));
    EXPECT_TRUE(f.BeginRebuild(4));
    EXPECT_TRUE(f.FinishRebuild());
}

// Keep the old index (LIVE_WIDEN_KEEP_OLD_INDEX): blocks not yet rebuilt are answered by the old bloom, the old bitmap
// section serves until the first write invalidates it, and every UpdateRow reaches the old file.
TEST_F(CatIdxLiveWidenTest, KeepOldAnswersFromTheOldIndexUntilBlocksAreRebuilt) {
    ASSERT_EQ(f.BeginLiveRebuild(4, /*keep_old=*/true), BeginRebuildResult::kStarted);
    EXPECT_TRUE(f.Rebuilding());
    EXPECT_EQ(f.TrackedColumnCount(), 4u);
    EXPECT_EQ(f.OldWidth(), 2u);
    const CatIdxFileHeader h = ReadCatIdxHeader(path);
    EXPECT_EQ(h.tracked_column_count, 4u);
    EXPECT_EQ(h.dirty, 1u);
    EXPECT_EQ(h.block_count, 0u);
    EXPECT_EQ(h.bitmap_section_offset, 0u);
    EXPECT_EQ(CatIdxFileSize(path), static_cast<off_t>(sizeof(CatIdxFileHeader)));
    EXPECT_FALSE(std::filesystem::exists(path + ".new"));

    // Old bloom below its width, "maybe" at or past it; the old directory serves.
    for (uint64_t b = 0; b < 4; ++b) {
        EXPECT_TRUE(f.MightContain(b, 0, Enc(100 + static_cast<int64_t>(b)))) << "block " << b;
        EXPECT_FALSE(f.MightContain(b, 0, Enc(777))) << "block " << b << ": the old bloom answers";
        EXPECT_TRUE(f.MightContain(b, 2, Enc(777))) << "column 2 is past the old width";
    }
    EXPECT_TRUE(f.HasBitmapSection());
    ASSERT_TRUE(f.LookupExact(0, Enc(100)).has_value());
    EXPECT_EQ(*f.LookupExact(0, Enc(100)), std::vector<uint64_t>{1});
    f.AppendBitmapSection({{0, {{Enc(101), {kLwRpb + 2}}}}});
    EXPECT_EQ(ReadCatIdxHeader(path).bitmap_section_offset, 0u) << "compaction is refused while rebuilding";

    // Builder: block 0 complete. A write above F reaches the old file (and invalidates its directory).
    f.RebuildFoldRow(5, CatObs(3, 33));
    f.AdvanceFrontier(kLwRpb);
    EXPECT_FALSE(f.MightContain(0, 0, Enc(100))) << "block 0 now from the new file, which has no row with 100 yet";
    EXPECT_TRUE(f.MightContain(0, 3, Enc(33)));
    f.UpdateRow(2 * kLwRpb + 7, CatObs(1, 55));
    EXPECT_TRUE(f.MightContain(2, 1, Enc(55))) << "the old bloom learned the write";
    EXPECT_FALSE(f.HasBitmapSection()) << "a write invalidates the old directory";
    EXPECT_FALSE(f.LookupExact(0, Enc(100)).has_value());
    f.UpdateRow(10, CatObs(1, 56)); // below F: both files
    EXPECT_TRUE(f.MightContain(0, 1, Enc(56)));

    f.AdvanceFrontier(4 * kLwRpb);
    EXPECT_FALSE(f.MightContain(2, 1, Enc(55))) << "rebuilt from the data (the stand-in builder folded nothing there)";
    EXPECT_TRUE(f.FinishRebuild());
    EXPECT_EQ(f.OldWidth(), 0u);
    EXPECT_FALSE(f.HasBitmapSection());
    f.AppendBitmapSection({{0, {{Enc(101), {kLwRpb + 2}}}}});
    EXPECT_TRUE(f.HasBitmapSection()) << "compaction works again after the build";
    EXPECT_TRUE(f.MarkClean());
}

TEST_F(CatIdxLiveWidenTest, KeepOldFailedBeginChangesNothing) {
    const std::string tmp = path + ".new";
    const off_t size_before = CatIdxFileSize(path);
    {
        xstore_fault::ScopedPathFault fault(tmp, false, EXDEV, xstore_fault::kRename);
        EXPECT_EQ(f.BeginLiveRebuild(4, true), BeginRebuildResult::kFailed);
    }
    {
        xstore_fault::ScopedFdFault fault(xstore_fault::FdOp::kSync, tmp, 0, 1, EIO);
        EXPECT_EQ(f.BeginLiveRebuild(4, true), BeginRebuildResult::kFailed);
    }
    EXPECT_FALSE(f.Rebuilding());
    EXPECT_EQ(f.OldWidth(), 0u);
    EXPECT_EQ(f.TrackedColumnCount(), 2u);
    EXPECT_EQ(CatIdxFileSize(path), size_before);
    EXPECT_FALSE(std::filesystem::exists(tmp));
    EXPECT_TRUE(f.HasBitmapSection());
    EXPECT_FALSE(f.MightContain(2, 0, Enc(777)));
}

// S1 (sync flavour): the fresh header was written but its fdatasync failed, and so does every retry. FinishRebuild
// keeps the rebuild (and the pin) until the header is durable.
TEST_F(CatIdxLiveWidenTest, FinishRebuildRefusesWhileTheHeaderIsNotDurable) {
    {
        // sync 1 (the old header's dirty mark) passes; the fresh header's sync and every retry fail.
        xstore_fault::ScopedFdFault fault(xstore_fault::FdOp::kSync, path, 1, 1000, EIO);
        ASSERT_TRUE(f.BeginRebuild(4));
        f.AdvanceFrontier(4 * kLwRpb);
        EXPECT_FALSE(f.FinishRebuild());
        EXPECT_TRUE(f.Rebuilding());
        EXPECT_FALSE(f.MarkClean());
        EXPECT_TRUE(f.MightContain(4, 0, Enc(777))) << "frontier unchanged (F = 4 blocks)";
        EXPECT_GE(fault.Failures(), 2);
    }
    EXPECT_TRUE(f.FinishRebuild());
    EXPECT_FALSE(f.Rebuilding());
    EXPECT_EQ(ReadCatIdxHeader(path).dirty, 1u);
    EXPECT_EQ(ReadCatIdxHeader(path).tracked_column_count, 4u);
    EXPECT_TRUE(f.MarkClean());
    EXPECT_EQ(ReadCatIdxHeader(path).dirty, 0u);
}

// Q2: any invalid header (too short, wrong magic) is replaced by a fresh header marked DIRTY.
TEST(CatIdxFileTest, ColdOpenOfAnInvalidFileWritesADirtyHeader) {
    for (const off_t size : {off_t{0}, off_t{10}, off_t{64}}) {
        SCOPED_TRACE(size);
        const std::string p = TempCatIdxPath("invalid" + std::to_string(size) + ".catidx");
        std::filesystem::remove(p);
        int fd = open(p.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        ASSERT_GE(fd, 0);
        ASSERT_EQ(ftruncate(fd, size), 0); // 64 zero bytes: long enough, magic 0
        close(fd);
        CatIdxFile c;
        ASSERT_TRUE(c.Open(p, 64, 4));
        const CatIdxFileHeader h = ReadCatIdxHeader(p);
        EXPECT_EQ(h.magic, CatIdxFileHeader{}.magic);
        EXPECT_EQ(h.dirty, 1u);
        EXPECT_EQ(c.DirtyMarkWritesForTests(), 0u) << "written dirty in one pwrite, not synced (SF1)";
    }
}

// S3: a pinned entry survives registry pressure and is evictable again once the pin is released.
TEST(GetOrOpenCatIdxTest, RegistryNeverEvictsAPinnedIndex) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_pin");
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    // A fresh file plus one synced dirty mark counts 1; a cold reopen (header already dirty on disk) would count 0.
    std::optional<CatIdxFile::Pin> pin(XStore::PinCatIdx(base, 2000, 64, 4));
    CatIdxFile *pinned = pin->get();
    pinned->MarkDirty();
    ASSERT_EQ(pinned->DirtyMarkWritesForTests(), 1u);
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::CATIDX_REGISTRY_CAP + 10; ++year) {
        XStore::GetOrOpenCatIdx(base, year, 64, 4);
    }
    CatIdxFile &again = XStore::GetOrOpenCatIdx(base, 2000, 64, 4);
    EXPECT_EQ(&again, pinned);
    EXPECT_EQ(again.DirtyMarkWritesForTests(), 1u) << "the pinned entry was evicted and reopened cold";

    pin.reset();
    for (unsigned short year = 2400; year < 2400 + XStoreTunables::CATIDX_REGISTRY_CAP; ++year) {
        XStore::GetOrOpenCatIdx(base, year, 64, 4);
    }
    EXPECT_EQ(XStore::GetOrOpenCatIdx(base, 2000, 64, 4).DirtyMarkWritesForTests(), 0u) << "released: evicted and reopened";
    XStore::ClearCatIdxRegistryForTests();
    std::filesystem::remove_all(base);
}

TEST(GetOrOpenCatIdxTest, RegistryNeverEvictsARebuildingIndex) {
    XStore::ClearCatIdxRegistryForTests();
    std::string base = TempCatIdxPath("registry_live_widen");
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);

    CatIdxFile &pinned = XStore::GetOrOpenCatIdx(base, 2000, 64, 4);
    ASSERT_TRUE(pinned.BeginRebuild(4));
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::CATIDX_REGISTRY_CAP + 10; ++year) {
        XStore::GetOrOpenCatIdx(base, year, 64, 4);
    }
    CatIdxFile &again = XStore::GetOrOpenCatIdx(base, 2000, 64, 4);
    EXPECT_TRUE(again.Rebuilding()) << "the rebuilding entry was evicted and reopened cold";
    EXPECT_EQ(&again, &pinned);

    xstore_test::kLogCapture->Clear(); // the one over-cap warning may come from the loop below already
    std::vector<CatIdxFile *> all;
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::CATIDX_REGISTRY_CAP; ++year) {
        CatIdxFile &c = XStore::GetOrOpenCatIdx(base, year, 64, 4);
        if (!c.Rebuilding()) {
            ASSERT_TRUE(c.BeginRebuild(4));
        }
        all.push_back(&c);
    }
    XStore::GetOrOpenCatIdx(base, 1999, 64, 4);
    EXPECT_TRUE(xstore_test::kLogCapture->Contains("growing past the cap"));
    EXPECT_TRUE(pinned.Rebuilding());
    EXPECT_TRUE(pinned.FinishRebuild());
    for (CatIdxFile *c : all) EXPECT_TRUE(c->FinishRebuild());
    XStore::ClearCatIdxRegistryForTests();
    std::filesystem::remove_all(base);
}

} // namespace

namespace {

using xstore_test::CreateTestDb;

// Compaction must not run on a year whose .catidx is being rebuilt (stage 3 serialises the two on the compaction
// thread; this is the backstop).
TEST_F(XStoreCatIdxTestFixture, CompactionSkipsAnIndexBeingRebuilt) {
    XStore store;
    CreateTestDb(store, "CATIDXLWCOMPACT", XStore_Proto::SECOND, 64);
    XStore_Proto::RowData row;
    row.add_fielddata()->set_int64data(137);
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1000, row, "CATIDXLWCOMPACT", &rep, XStore_Proto::SYNC);
    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
    const unsigned short year = TimeStruct(1000, XStore_Proto::SECOND).year;
    const std::string db = DbPath("CATIDXLWCOMPACT");
    // Bound the compaction scan (same resize trick as FindByValueUsesBitmapDirectoryNotRealScanAfterCompaction).
    std::error_code resize_ec;
    std::filesystem::resize_file(db + "/" + std::to_string(year) + ".XSTORE", 4096 * 64, resize_ec);
    ASSERT_FALSE(resize_ec) << resize_ec.message();

    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(db, year, 64);
    ASSERT_TRUE(catidx.BeginRebuild(64));
    xstore_test::kLogCapture->Clear();
    store.CompactOneYearCatIdx(db, year, 64, 64);
    EXPECT_FALSE(catidx.HasBitmapSection());
    // The early guard stopped it before the scan: AppendBitmapSection's own refusal (which logs this) never ran.
    EXPECT_FALSE(xstore_test::kLogCapture->Contains("Not appending a bitmap section"));
    EXPECT_TRUE(catidx.FinishRebuild());
    store.CompactOneYearCatIdx(db, year, 64, 64);
    EXPECT_TRUE(catidx.HasBitmapSection());
}

} // namespace

// Bloom bit positions computed on the producer (BloomValueOf) against the old fold-time formula, and the three ways a
// value reaches a file (the writer's view, CatIdxFoldBatch::Add, the per-row UpdateRow) against each other and against
// an image built bit by bit with the old formula.
namespace {
using xstore_rebuild_test::Positions;
using xstore_rebuild_test::ReadFileBytes;
using xstore_rebuild_test::RefBloomPositions;

constexpr uint16_t kDiffRowLength = 16384;   // RowsPerBlock == 128
constexpr size_t kMaxValueBytes = kDiffRowLength - 8;   // a value's bytes inside one row slot (tags and lengths aside)

std::string RandomBytes(std::mt19937_64 &rng, size_t n) {
    std::string s(n, '\0');
    for (char &c : s) c = static_cast<char>(rng() % 4 == 0 ? 0 : rng());   // NULs included
    return s;
}

double DoubleBits(uint64_t bits) {
    double d;
    std::memcpy(&d, &bits, sizeof(d));
    return d;
}

// Every FData type with its edge values, then random values of each.
std::vector<XStore_Proto::FData> EveryTypeValues(std::mt19937_64 &rng) {
    using namespace xstore_test;
    using L64 = std::numeric_limits<int64_t>;
    using L32 = std::numeric_limits<int32_t>;
    using LD = std::numeric_limits<double>;
    using LF = std::numeric_limits<float>;
    std::vector<XStore_Proto::FData> v;
    for (int64_t x : {L64::min(), L64::max(), int64_t{0}, int64_t{-1}, int64_t{1}}) {
        v.push_back(FInt64(x));
        v.push_back(FSInt64(x));
        v.push_back(FSFixed64(x));
    }
    for (int32_t x : {L32::min(), L32::max(), 0, -1, 1}) {
        v.push_back(FInt32(x));
        v.push_back(FSInt32(x));
        v.push_back(FSFixed32(x));
    }
    for (uint64_t x : {uint64_t{0}, std::numeric_limits<uint64_t>::max()}) {
        v.push_back(FUInt64(x));
        v.push_back(FFixed64(x));
    }
    for (uint32_t x : {0u, std::numeric_limits<uint32_t>::max()}) {
        v.push_back(FUInt32(x));
        v.push_back(FFixed32(x));
    }
    for (double d : {LD::quiet_NaN(), -LD::quiet_NaN(), DoubleBits(0x7ff8000000000001ULL), LD::signaling_NaN(), 0.0, -0.0,
                     LD::infinity(), -LD::infinity(), LD::denorm_min(), -LD::denorm_min(), LD::min(), LD::max(), LD::lowest()}) {
        v.push_back(FDouble(d));
    }
    for (float f : {LF::quiet_NaN(), 0.0f, -0.0f, LF::infinity(), -LF::infinity(), LF::denorm_min(), LF::max()}) v.push_back(FFloat(f));
    v.push_back(FBool(true));
    v.push_back(FBool(false));
    for (size_t n : {size_t{0}, size_t{1}, size_t{7}, size_t{8}, size_t{15}, size_t{16}, size_t{200}, kMaxValueBytes}) {
        v.push_back(FString(std::string(n, 'q')));
        v.push_back(FBytes(RandomBytes(rng, n)));
    }
    v.push_back(FBytes(std::string("\0", 1)));
    v.push_back(FBytes(std::string("\0\0\xff\0", 4)));
    for (int i = 0; i < 300; ++i) {
        v.push_back(FInt64(static_cast<int64_t>(rng())));
        v.push_back(FInt32(static_cast<int32_t>(rng())));
        v.push_back(FUInt64(rng()));
        v.push_back(FSInt64(static_cast<int64_t>(rng())));
        v.push_back(FFixed32(static_cast<uint32_t>(rng())));
        v.push_back(FDouble(DoubleBits(rng())));   // any bit pattern: NaN payloads, denormals, infinities
        v.push_back(FFloat(static_cast<float>(DoubleBits(rng()))));
        v.push_back(FBool(rng() % 2 == 0));
        v.push_back(FString(std::string(rng() % 64, static_cast<char>('a' + rng() % 26))));
        v.push_back(FBytes(RandomBytes(rng, rng() % (i % 10 == 0 ? kMaxValueBytes : 300))));
    }
    return v;
}

TEST(CatIdxBloomPositionTest, PositionsMatchTheOldFormulaForEveryFDataType) {
    std::mt19937_64 rng(20261022);
    const std::vector<XStore_Proto::FData> values = EveryTypeValues(rng);
    for (size_t i = 0; i < values.size(); ++i) {
        SCOPED_TRACE(i);
        const std::string enc = EncodeFDataForCatIdx(values[i]);
        ASSERT_LE(enc.size(), 1 + kMaxValueBytes);
        const uint32_t col = static_cast<uint32_t>(i % 4096);
        const CatIdxBloomValue v = BloomValueOfEncoded(col, enc.data(), enc.size());
        EXPECT_EQ(v.ColumnIdx(), col);
        EXPECT_EQ(Positions(v), RefBloomPositions(enc));
        for (const uint16_t bit : v.Bits()) EXPECT_LT(bit, XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY * 8);
    }
}

TEST(CatIdxBloomPositionTest, PositionsMatchTheOldFormulaForRawHashes) {
    std::mt19937_64 rng(7);
    std::vector<std::pair<uint64_t, uint64_t>> hashes = {{0, 0}, {0, 1}, {UINT64_MAX, UINT64_MAX}, {UINT64_MAX, 1},
                                                         {1279, 1280}, {1280, 1279}, {0xFFFFFFFFULL, UINT64_MAX / 3}};
    for (int i = 0; i < 20000; ++i) hashes.emplace_back(rng(), rng());
    volatile size_t entry_bytes = XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY;   // a runtime modulus, as the old fold had
    for (const auto &[h1, h2] : hashes) {
        const CatIdxBloomValue v = BloomValueOf(3, h1, h2);
        for (size_t i = 0; i < XStoreTunables::CATIDX_BLOOM_HASH_COUNT; ++i) {
            const uint64_t combined = h1 + i * h2;
            ASSERT_EQ(v.Bits()[i], combined % (entry_bytes * 8)) << h1 << " " << h2 << " " << i;
        }
    }
}

struct DiffRow {
    uint64_t row;
    std::vector<CatIdxObservation> obs;   // absent ones included (UpdateRow skips them)
};

TEST(CatIdxBloomPositionTest, ViewAddAndUpdateRowWriteIdenticalBytesMatchingTheOldFormula) {
    xstore_test::ScratchDir scratch("catidx_bloom_diff");
    constexpr uint32_t kWidth = 6;
    constexpr uint64_t kRpb = 128;
    std::mt19937_64 rng(555);
    const std::vector<XStore_Proto::FData> pool = EveryTypeValues(rng);

    std::vector<DiffRow> rows;
    uint64_t r = 0;
    for (int i = 0; i < 1200; ++i) {
        r += i % 11 == 0 ? rng() % 300 : 1;   // gaps, block edges, a duplicate row now and then (+0)
        DiffRow row{i % 17 == 0 ? rng() % (r + 1) : r, {}};   // and some rows out of order
        for (uint32_t c = 0; c < kWidth + 2; ++c) {   // columns past the width: dropped by every path
            if (rng() % 4 == 0) {
                row.obs.push_back({c, std::string(), false});
            } else {
                row.obs.push_back({c, EncodeFDataForCatIdx(pool[rng() % pool.size()]), true});
            }
        }
        if (i % 50 == 0) row.obs.clear();   // a row with no value
        rows.push_back(std::move(row));
    }

    const std::string view_path = (scratch.test_root / "view.catidx").string();
    const std::string add_path = (scratch.test_root / "add.catidx").string();
    const std::string per_row_path = (scratch.test_root / "per_row.catidx").string();
    {
        CatIdxFile view_file;
        CatIdxFile add_file;
        CatIdxFile per_row_file;
        ASSERT_TRUE(view_file.Open(view_path, kDiffRowLength, kWidth));
        ASSERT_TRUE(add_file.Open(add_path, kDiffRowLength, kWidth));
        ASSERT_TRUE(per_row_file.Open(per_row_path, kDiffRowLength, kWidth));
        // (a) the writer's shape: one value array, folded through views over chunks of rows with chunk-relative ends.
        std::vector<CatIdxBloomValue> values;
        std::vector<size_t> row_value_end;
        for (const DiffRow &row : rows) {
            for (const CatIdxObservation &o : row.obs) {
                if (o.is_present) values.push_back(BloomValueOfEncoded(o.column_idx, o.encoded_value.data(), o.encoded_value.size()));
            }
            row_value_end.push_back(values.size());
        }
        for (size_t begin = 0; begin < rows.size();) {
            const size_t end = std::min(rows.size(), begin + 1 + rng() % 200);
            const size_t value_begin = begin == 0 ? 0 : row_value_end[begin - 1];
            std::vector<uint64_t> row_index;
            std::vector<size_t> value_end;
            for (size_t i = begin; i < end; ++i) {
                row_index.push_back(rows[i].row);
                value_end.push_back(row_value_end[i] - value_begin);
            }
            view_file.UpdateRows(CatIdxFoldView{row_index.data(), value_end.data(), row_index.size(), values.data() + value_begin});
            begin = end;
        }
        // (b) CatIdxFoldBatch::Add, one batch.
        CatIdxFoldBatch batch;
        for (const DiffRow &row : rows) {
            for (const CatIdxObservation &o : row.obs) {
                if (o.is_present) batch.Add(o.column_idx, o.encoded_value);
            }
            batch.EndRow(row.row);
        }
        add_file.UpdateRows(batch);
        // (c) the per-row API.
        for (const DiffRow &row : rows) per_row_file.UpdateRow(row.row, row.obs);

        for (CatIdxFile *f : {&view_file, &add_file, &per_row_file}) {
            for (const DiffRow &row : rows) {
                for (const CatIdxObservation &o : row.obs) {
                    if (o.is_present && o.column_idx < kWidth) {
                        ASSERT_TRUE(f->MightContain(row.row / kRpb, o.column_idx, o.encoded_value)) << "a false negative";
                    }
                }
            }
        }
    }

    const std::string view_bytes = ReadFileBytes(view_path);
    EXPECT_TRUE(view_bytes == ReadFileBytes(add_path)) << "the view path and Add differ";
    EXPECT_TRUE(view_bytes == ReadFileBytes(per_row_path)) << "the view path and UpdateRow differ";

    // The bloom section, bit by bit with the old formula: blocks 0..last, each kWidth entries.
    uint64_t last_block = 0;
    for (const DiffRow &row : rows) last_block = std::max(last_block, row.row / kRpb);
    constexpr size_t kEntry = XStoreTunables::CATIDX_BLOOM_BYTES_PER_ENTRY;
    std::string image((last_block + 1) * kWidth * kEntry, '\0');
    for (const DiffRow &row : rows) {
        for (const CatIdxObservation &o : row.obs) {
            if (!o.is_present || o.column_idx >= kWidth) continue;
            const size_t base = ((row.row / kRpb) * kWidth + o.column_idx) * kEntry;
            for (const uint16_t bit : RefBloomPositions(o.encoded_value)) image[base + bit / 8] |= static_cast<char>(1u << (bit % 8));
        }
    }
    CatIdxFileHeader header{};
    ASSERT_GE(view_bytes.size(), sizeof(header));
    std::memcpy(&header, view_bytes.data(), sizeof(header));
    EXPECT_EQ(header.tracked_column_count, kWidth);
    EXPECT_EQ(header.block_count, last_block + 1);
    EXPECT_GT(last_block, 5u);
    EXPECT_TRUE(view_bytes.substr(sizeof(header)) == image) << "the bloom section differs from the old formula's image";
}

// The snapshot width: a view folds no value at or past its column_limit, even into a file wider than that (the writer's
// catidx width was read before its run loop; a widen since must not get these values from this record). A limit past
// the file's width changes nothing.
TEST(CatIdxBloomPositionTest, ColumnLimitSkipsValuesPastTheSnapshotWidth) {
    xstore_test::ScratchDir scratch("catidx_column_limit");
    constexpr uint32_t kWidth = 6;
    std::vector<CatIdxBloomValue> values;
    std::vector<uint64_t> row_index;
    std::vector<size_t> value_end;
    CatIdxFoldBatch narrow;   // the reference: only columns < 3, via Add
    CatIdxFoldBatch all;      // every column, via Add
    for (uint64_t r = 0; r < 300; r += 7) {
        for (uint32_t c = 0; c < kWidth + 2; ++c) {
            const std::string enc = EncodeFDataForCatIdx(xstore_test::FInt64(static_cast<int64_t>(r * 10 + c)));
            values.push_back(BloomValueOfEncoded(c, enc.data(), enc.size()));
            if (c < 3) narrow.Add(c, enc);
            all.Add(c, enc);
        }
        row_index.push_back(r);
        value_end.push_back(values.size());
        narrow.EndRow(r);
        all.EndRow(r);
    }
    const auto fold = [&](const std::string &name, const std::function<void(CatIdxFile &)> &write) {
        const std::string path = (scratch.test_root / name).string();
        CatIdxFile f;
        EXPECT_TRUE(f.Open(path, kDiffRowLength, kWidth));
        write(f);
        return ReadFileBytes(path);
    };
    const std::string limited = fold("limited.catidx", [&](CatIdxFile &f) {
        f.UpdateRows(CatIdxFoldView{row_index.data(), value_end.data(), row_index.size(), values.data(), 3});
    });
    EXPECT_TRUE(limited == fold("narrow.catidx", [&](CatIdxFile &f) { f.UpdateRows(narrow); }))
        << "a value at or past column_limit was folded";
    const std::string wide_limit = fold("wide_limit.catidx", [&](CatIdxFile &f) {
        f.UpdateRows(CatIdxFoldView{row_index.data(), value_end.data(), row_index.size(), values.data(), kWidth + 10});
    });
    EXPECT_TRUE(wide_limit == fold("all.catidx", [&](CatIdxFile &f) { f.UpdateRows(all); }));
    EXPECT_FALSE(limited == wide_limit) << "precondition: columns 3..5 hold values";
}

// A keep-old rebuild: the old index gets every row of the view under the same column_limit (rows below F go to the new
// index too); without a limit both get the value.
TEST(CatIdxBloomPositionTest, KeepOldRebuildSharesTheViewAndItsColumnLimit) {
    xstore_test::ScratchDir scratch("catidx_keep_old_view");
    constexpr uint64_t kRpb = 128;
    const std::string path = (scratch.test_root / "keep_old.catidx").string();
    CatIdxFile f;
    ASSERT_TRUE(f.Open(path, kDiffRowLength, 4));
    ASSERT_EQ(f.BeginLiveRebuild(8, /*keep_old=*/true), BeginRebuildResult::kStarted);
    ASSERT_EQ(f.OldWidth(), 4u);
    f.AdvanceFrontier(2 * kRpb);   // blocks 0 and 1 rebuilt (new index), blocks 2.. answered by the old one
    const auto enc = [](int64_t v) { return EncodeFDataForCatIdx(xstore_test::FInt64(v)); };
    const auto apply = [&](uint64_t row_low, uint64_t row_high, uint32_t column_limit) {
        std::vector<CatIdxBloomValue> values;
        for (int row = 0; row < 2; ++row) {   // the same three values for both rows
            for (const auto &[c, v] : std::vector<std::pair<uint32_t, int64_t>>{{0, 1}, {2, 2}, {5, 5}}) {
                const std::string e = enc(v);
                values.push_back(BloomValueOfEncoded(c, e.data(), e.size()));
            }
        }
        const std::vector<uint64_t> row_index = {row_low, row_high};
        const std::vector<size_t> value_end = {3, 6};
        f.UpdateRows(CatIdxFoldView{row_index.data(), value_end.data(), 2, values.data(), column_limit});
    };
    apply(5, 3 * kRpb + 5, 2);           // blocks 0 (new) and 3 (old), limit 2
    apply(kRpb + 5, 4 * kRpb + 5, UINT32_MAX);   // blocks 1 (new) and 4 (old), no limit

    EXPECT_TRUE(f.MightContain(0, 0, enc(1)));
    EXPECT_FALSE(f.MightContain(0, 2, enc(2))) << "new index: column 2 is past the limit";
    EXPECT_FALSE(f.MightContain(0, 5, enc(5))) << "new index: column 5 is past the limit";
    EXPECT_TRUE(f.MightContain(3, 0, enc(1)));
    EXPECT_FALSE(f.MightContain(3, 2, enc(2))) << "old index: column 2 is past the limit";
    EXPECT_TRUE(f.MightContain(3, 5, enc(5))) << "past the old width: maybe";

    EXPECT_TRUE(f.MightContain(1, 0, enc(1)));
    EXPECT_TRUE(f.MightContain(1, 2, enc(2)));
    EXPECT_TRUE(f.MightContain(1, 5, enc(5)));
    EXPECT_TRUE(f.MightContain(4, 0, enc(1)));
    EXPECT_TRUE(f.MightContain(4, 2, enc(2))) << "old index: no limit";
    ASSERT_TRUE(f.FinishRebuild());
}

} // namespace
