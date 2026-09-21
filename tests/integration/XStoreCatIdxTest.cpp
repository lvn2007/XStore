#include <gtest/gtest.h>
#include "XStore/XStore_CatIdx.h"
#include "XStore.pb.h"

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
