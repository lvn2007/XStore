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

} // namespace
