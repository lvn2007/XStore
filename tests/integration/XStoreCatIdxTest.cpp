#include <gtest/gtest.h>
#include "XStore/XStore_CatIdx.h"
#include "XStore.pb.h"
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
