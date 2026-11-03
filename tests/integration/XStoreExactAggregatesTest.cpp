// The reductions behind Percentile / DistinctCount / TopK must give exactly what the straightforward versions give (full sort,
// std::unordered_set, std::unordered_map + ranked vector), on every data shape: few and many distinct values, around the point
// where DistinctCount leaves its flat table for sort + unique, duplicates, signed zeros, NaN.
#include <gtest/gtest.h>
#include "XStoreTestSupport.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_ExactAggregates.h"
#include "include/XStore/XStore_RowDecode.h"
#include <algorithm>
#include <atomic>
#include <thread>
#include <cmath>
#include <limits>
#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

using Counted = std::vector<std::pair<double, uint64_t>>;

double ReferencePercentile(std::vector<double> v, double percentile) {
    std::sort(v.begin(), v.end());
    const double rank = percentile * static_cast<double>(v.size() - 1);
    const size_t lower = static_cast<size_t>(std::floor(rank));
    const size_t upper = static_cast<size_t>(std::ceil(rank));
    return v[lower] + (rank - static_cast<double>(lower)) * (v[upper] - v[lower]);
}

uint64_t ReferenceDistinct(const std::vector<double> &v) {
    return std::unordered_set<double>(v.begin(), v.end()).size();
}

Counted ReferenceTopK(const std::vector<double> &v, size_t k) {
    std::unordered_map<double, uint64_t> freq;
    for (double d : v) freq[d]++;
    Counted ranked(freq.begin(), freq.end());
    std::sort(ranked.begin(), ranked.end(), [](const auto &a, const auto &b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });
    ranked.resize(std::min(k, ranked.size()));
    return ranked;
}

// `n` values drawn from `distinct` different ones (every value distinct when distinct >= n), shuffled.
std::vector<double> Values(size_t n, size_t distinct, uint32_t seed) {
    std::mt19937_64 gen(seed);
    std::vector<double> v;
    v.reserve(n);
    for (size_t i = 0; i < n; ++i) v.push_back(static_cast<double>(distinct >= n ? i : gen() % distinct) * 0.25 - 100.0);
    std::shuffle(v.begin(), v.end(), gen);
    return v;
}

TEST(ExactPercentileTest, MatchesAFullSortOnEveryShape) {
    for (size_t n : {1u, 2u, 3u, 10u, 11u, 1000u, 4097u}) {
        for (size_t distinct : {1u, 3u, 50u, 100000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 31 + distinct));
            for (double p : {0.0, 0.01, 0.25, 0.5, 0.75, 0.9, 0.999, 1.0}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactPercentile(work, p), ReferencePercentile(data, p)) << "n=" << n << " distinct=" << distinct << " p=" << p;
            }
        }
    }
}

// A NaN is not less than, greater than or equal to anything, so nth_element over data that holds one is not a sort of anything (the result depended on
// where the NaNs sat). Percentile skips them, as pandas does, and says how many it skipped.
TEST(ExactPercentileTest, NaNsAreSkippedWhereverTheySit) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::mt19937_64 gen(11);
    for (size_t n : {1u, 2u, 3u, 10u, 11u, 1000u}) {
        for (size_t nans : {1u, 2u, 5u, 40u}) {
            for (double p : {0.0, 0.1, 0.25, 0.5, 0.75, 0.99, 1.0}) {
                const std::vector<double> numbers = Values(n, 50, static_cast<uint32_t>(n * 17 + nans));
                std::vector<double> data = numbers;
                for (size_t j = 0; j < nans; ++j) data.insert(data.begin() + static_cast<std::ptrdiff_t>(gen() % (data.size() + 1)), nan);
                uint64_t skipped = 0;
                EXPECT_EQ(xstore::ExactPercentile(data, p, &skipped), ReferencePercentile(numbers, p)) << "n=" << n << " nans=" << nans << " p=" << p;
                EXPECT_EQ(skipped, nans);
            }
        }
    }
}

TEST(ExactPercentileTest, ValuesOfOnlyNaNGiveNaNAndAreAllCounted) {
    std::vector<double> data(7, std::numeric_limits<double>::quiet_NaN());
    uint64_t skipped = 0;
    EXPECT_TRUE(std::isnan(xstore::ExactPercentile(data, 0.5, &skipped)));
    EXPECT_EQ(skipped, 7u);
}

// Interpolating between two equal infinities is inf - inf = NaN; two equal values interpolate to themselves.
TEST(ExactPercentileTest, EqualInfinitiesInterpolateToThemselves) {
    const double inf = std::numeric_limits<double>::infinity();
    std::vector<double> up = {inf, inf, 1.0, inf};
    EXPECT_EQ(xstore::ExactPercentile(up, 0.9), inf);   // ranks 2 and 3 of {1, inf, inf, inf}, fraction .7
    std::vector<double> down = {-inf, -inf, 1.0};
    EXPECT_EQ(xstore::ExactPercentile(down, 0.25), -inf);
}

TEST(ExactPercentileTest, WithoutNaNNothingIsSkipped) {
    std::vector<double> data = Values(100, 100, 5);
    uint64_t skipped = 99;
    (void)xstore::ExactPercentile(data, 0.5, &skipped);
    EXPECT_EQ(skipped, 0u);
}

// Through the aggregate: NaN doubles stored in the column.
class PercentileNaNTest : public ::testing::Test {
protected:
    static constexpr uint64_t kBase = 1704067200;   // 2024-01-01T00:00:00Z
    std::unique_ptr<xstore_test::ScratchDir> scratch;
    std::unique_ptr<XStore> store;
    void SetUp() override {
        scratch = std::make_unique<xstore_test::ScratchDir>("xstore_percentile_nan_test");
        store = std::make_unique<XStore>();
        xstore_test::CreateTestDb(*store, "PNAN", XStore_Proto::SECOND, 100);
    }
    void TearDown() override {
        store.reset();
        scratch.reset();
    }
    void Put(const std::vector<double> &values) {
        for (size_t i = 0; i < values.size(); ++i) {
            XStore_Proto::UnaryInsert_REP rep;
            store->UnaryInsert(kBase + i, xstore_test::MakeRow({xstore_test::FDouble(values[i])}), "PNAN", &rep);
        }
    }
    XStore_Proto::Percentile_REP Median(size_t rows) {
        XStore_Proto::Percentile_REP rep;
        store->PercentileAggregate(kBase, kBase + rows - 1, "PNAN", 0, 0.5, &rep);
        return rep;
    }
};

TEST_F(PercentileNaNTest, StoredNaNsAreLeftOutOfThePercentile) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    Put({1, 2, nan, 4, 5, nan, 7, 8, 9, 10});   // the numbers: 1 2 4 5 7 8 9 10, median (5 + 7) / 2
    const XStore_Proto::Percentile_REP rep = Median(10);
    EXPECT_EQ(rep.opstatus(), "SUCCESS");
    EXPECT_EQ(rep.value(), 6.0);
}

TEST_F(PercentileNaNTest, AColumnOfOnlyNaNHasNoValidData) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    Put({nan, nan, nan});
    EXPECT_EQ(Median(3).opstatus(), "ERROR: No valid data found");
}

TEST(ExactDistinctCountTest, MatchesAHashSetBelowAndAboveTheTableLimit) {
    for (size_t n : {0u, 1u, 5u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 7u, 300u, 5000u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n + distinct * 7));
            // limits below, at and above the number of distinct values, and one that is never reached
            for (size_t limit : {1u, 64u, 300u, 4096u, 1u << 20}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactDistinctCount(work, limit), ReferenceDistinct(data))
                    << "n=" << n << " distinct=" << distinct << " limit=" << limit;
            }
        }
    }
}

TEST(ExactDistinctCountTest, SignedZerosAreOneValueAndEveryNaNIsItsOwn) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const std::vector<double> data = {0.0, -0.0, 1.0, nan, nan, 2.0, -0.0, nan, 1.0};   // 0, 1, 2 and three NaNs
    for (size_t limit : {1u, 2u, 3u, 100u}) {
        std::vector<double> work = data;
        EXPECT_EQ(xstore::ExactDistinctCount(work, limit), ReferenceDistinct(data)) << "limit=" << limit;
        EXPECT_EQ(ReferenceDistinct(data), 6u);
    }
}

TEST(ExactTopKTest, MatchesAHashMapRankingOnEveryShape) {
    for (size_t n : {0u, 1u, 2u, 10u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 2u, 7u, 300u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 13 + distinct));
            for (size_t k : {1u, 3u, 10u, 100000u}) {
                std::vector<double> work = data;
                EXPECT_EQ(xstore::ExactTopK(work, k), ReferenceTopK(data, k)) << "n=" << n << " distinct=" << distinct << " k=" << k;
            }
        }
    }
}

TEST(ExactTopKTest, TiesAreBrokenByAscendingValueAndKZeroGivesNothing) {
    std::vector<double> data = {5.0, 3.0, 5.0, 3.0, 9.0, 1.0, 9.0, 1.0, 7.0};   // 1, 3, 5, 9 twice each; 7 once
    EXPECT_EQ(xstore::ExactTopK(data, 3), (Counted{{1.0, 2}, {3.0, 2}, {5.0, 2}}));
    std::vector<double> again = data;
    EXPECT_TRUE(xstore::ExactTopK(again, 0).empty());
}

TEST(ExactTopKTest, EveryNaNIsOneEntryOfCountOne) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> data = {4.0, 4.0, nan, 2.0, nan, 4.0, 2.0};   // 4 x3, 2 x2, two NaNs x1
    const Counted top = xstore::ExactTopK(data, 10);
    ASSERT_EQ(top.size(), 4u);
    EXPECT_EQ(top[0], (std::pair<double, uint64_t>{4.0, 3}));
    EXPECT_EQ(top[1], (std::pair<double, uint64_t>{2.0, 2}));
    EXPECT_TRUE(std::isnan(top[2].first) && top[2].second == 1);
    EXPECT_TRUE(std::isnan(top[3].first) && top[3].second == 1);
}

// ---- DecodeColumnAsDouble must answer what the protobuf path answers ----

// The protobuf path, as ExtractColumnByIndexSkipParse and ProcessFileRangeCollectValues run it (copied, so this stays the reference).
enum class Ref { kValue, kSkip };
Ref ReferenceDecode(const char *row, size_t row_len, unsigned short column_idx, double *out) {
    using google::protobuf::internal::WireFormatLite;
    google::protobuf::io::ArrayInputStream array_in(row, static_cast<int>(row_len));
    google::protobuf::io::CodedInputStream coded_in(&array_in);
    XStore_Proto::FData data;
    unsigned occurrence = 0;
    uint32_t tag;
    bool found = false;
    while ((tag = coded_in.ReadTag()) != 0) {
        if (WireFormatLite::GetTagFieldNumber(tag) != XStore_Proto::RowData::kFieldDataFieldNumber) {
            if (!WireFormatLite::SkipField(&coded_in, tag)) return Ref::kSkip;
            continue;
        }
        if (occurrence == column_idx) {
            found = WireFormatLite::ReadMessage(&coded_in, &data);
            break;
        }
        if (!WireFormatLite::SkipField(&coded_in, tag)) return Ref::kSkip;
        ++occurrence;
    }
    if (!found || data.oneOfData_case() <= 0 || data.oneOfData_case() > 12) return Ref::kSkip;
    const XStore_Proto::FData &copy = data;
    long double wide = 0;
    switch (copy.oneOfData_case()) {
        case XStore_Proto::FData::kDoubleData: wide = copy.doubledata(); break;
        case XStore_Proto::FData::kFloatData: wide = copy.floatdata(); break;
        case XStore_Proto::FData::kInt32Data: wide = copy.int32data(); break;
        case XStore_Proto::FData::kInt64Data: wide = copy.int64data(); break;
        case XStore_Proto::FData::kUint32Data: wide = copy.uint32data(); break;
        case XStore_Proto::FData::kUint64Data: wide = copy.uint64data(); break;
        case XStore_Proto::FData::kSint32Data: wide = copy.sint32data(); break;
        case XStore_Proto::FData::kSint64Data: wide = copy.sint64data(); break;
        case XStore_Proto::FData::kFixed32Data: wide = copy.fixed32data(); break;
        case XStore_Proto::FData::kFixed64Data: wide = copy.fixed64data(); break;
        case XStore_Proto::FData::kSfixed32Data: wide = copy.sfixed32data(); break;
        default: wide = copy.sfixed64data(); break;
    }
    *out = static_cast<double>(wide);
    return Ref::kValue;
}

bool SameBits(double a, double b) { return std::memcmp(&a, &b, sizeof a) == 0; }

// Checks one row for one column. A kFallback answer is allowed (the protobuf path then decides); any other answer must equal the reference.
xstore::RowDecode CheckRow(const std::string &bytes, size_t row_len, unsigned short column_idx) {
    std::string row = bytes;
    row.resize(row_len, '\0');
    double expected = 0, got = 0;
    const Ref ref = ReferenceDecode(row.data(), row_len, column_idx, &expected);
    const xstore::RowDecode dec = xstore::DecodeColumnAsDouble(row.data(), row_len, column_idx, &got);
    if (dec == xstore::RowDecode::kValue) {
        EXPECT_EQ(ref, Ref::kValue) << "col " << column_idx;
        if (ref == Ref::kValue) {
            EXPECT_TRUE(SameBits(expected, got)) << "col " << column_idx << " expected " << expected << " got " << got;
        }
    } else if (dec == xstore::RowDecode::kSkip) {
        EXPECT_EQ(ref, Ref::kSkip) << "col " << column_idx;
    }
    return dec;
}

std::vector<XStore_Proto::FData> OneOfEveryType(uint64_t bits) {
    std::vector<XStore_Proto::FData> v(12);
    double d;
    std::memcpy(&d, &bits, 8);
    float f;
    const uint32_t low = static_cast<uint32_t>(bits);
    std::memcpy(&f, &low, 4);
    v[0].set_doubledata(d);
    v[1].set_floatdata(f);
    v[2].set_int32data(static_cast<int32_t>(low));
    v[3].set_int64data(static_cast<int64_t>(bits));
    v[4].set_uint32data(low);
    v[5].set_uint64data(bits);
    v[6].set_sint32data(static_cast<int32_t>(low));
    v[7].set_sint64data(static_cast<int64_t>(bits));
    v[8].set_fixed32data(low);
    v[9].set_fixed64data(bits);
    v[10].set_sfixed32data(static_cast<int32_t>(low));
    v[11].set_sfixed64data(static_cast<int64_t>(bits));
    return v;
}

std::string SerializeRow(const std::vector<XStore_Proto::FData> &columns) {
    XStore_Proto::RowData row;
    for (const auto &c : columns) *row.add_fielddata() = c;
    return row.SerializeAsString();
}

// Columns of every kind around the target, so the skip over earlier columns sees every wire type and length.
std::vector<XStore_Proto::FData> Neighbours() {
    std::vector<XStore_Proto::FData> v = OneOfEveryType(0x123456789ABCDEF0ull);
    XStore_Proto::FData b, s, y, hole;
    b.set_booldata(true);
    s.set_stringdata("a string column");
    y.set_bytesdata(std::string("\x00\x01\xff", 3));
    v.push_back(b);
    v.push_back(s);
    v.push_back(y);
    v.push_back(hole);
    return v;
}

TEST(RowDecodeTest, EveryNumericTypeAtEveryPositionMatchesTheProtobufPath) {
    const uint64_t patterns[] = {0, 1, 2, 127, 128, 255, 16383, 16384, 0x7FFFFFFFull, 0x80000000ull, 0xFFFFFFFFull, 0x100000000ull,
                                 0x7FFFFFFFFFFFFFFFull, 0x8000000000000000ull, 0xFFFFFFFFFFFFFFFFull, 0x0020000000000001ull,
                                 0x7FF0000000000000ull /* +inf */, 0x7FF8000000000000ull /* NaN */, 0x8000000000000000ull /* -0 */,
                                 0x0000000000000001ull /* denormal */, 0x3FF0000000000000ull /* 1.0 */, 0x123456789ABCDEF0ull};
    const std::vector<XStore_Proto::FData> neighbours = Neighbours();
    for (uint64_t pattern : patterns) {
        const std::vector<XStore_Proto::FData> targets = OneOfEveryType(pattern);
        for (size_t t = 0; t < targets.size(); ++t) {
            for (size_t position : {0u, 1u, 5u, 11u, 12u, 17u}) {
                std::vector<XStore_Proto::FData> columns;
                for (size_t i = 0; i < position; ++i) columns.push_back(neighbours[i % neighbours.size()]);
                columns.push_back(targets[t]);
                columns.push_back(neighbours[3]);
                const std::string bytes = SerializeRow(columns);
                for (size_t row_len : {bytes.size(), bytes.size() + 1, bytes.size() + 64}) {
                    EXPECT_EQ(CheckRow(bytes, row_len, static_cast<unsigned short>(position)), xstore::RowDecode::kValue)
                        << "type " << t << " position " << position << " pattern " << pattern;
                }
            }
        }
    }
}

TEST(RowDecodeTest, NonNumericAndMissingColumnsAreSkippedNotDecoded) {
    const std::vector<XStore_Proto::FData> neighbours = Neighbours();
    const std::string bytes = SerializeRow(neighbours);
    for (unsigned short column = 0; column < neighbours.size() + 3; ++column) {
        const xstore::RowDecode dec = CheckRow(bytes, bytes.size() + 32, column);
        if (column >= 12) {   // 12 numeric, then bool, string, bytes, hole, then past the end
            EXPECT_EQ(dec, xstore::RowDecode::kSkip) << "column " << column;
        }
    }
    EXPECT_EQ(CheckRow("", 64, 0), xstore::RowDecode::kSkip);                 // an empty slot
    EXPECT_EQ(CheckRow(bytes, bytes.size(), 99), xstore::RowDecode::kSkip);   // column past the last, row exactly full
}

TEST(RowDecodeTest, FieldsTheFastPathDoesNotModelFallBack) {
    std::string good = SerializeRow(OneOfEveryType(42));
    // a RowData field other than fieldData (field 2, varint 7) in front
    EXPECT_EQ(CheckRow(std::string("\x10\x07", 2) + good, good.size() + 8, 0), xstore::RowDecode::kFallback);
    // non-minimal tag for fieldData: 0x8A 0x00
    EXPECT_EQ(CheckRow(std::string("\x8a\x00\x09\x09\x00\x00\x00\x00\x00\x00\x00\x00", 12), 32, 0), xstore::RowDecode::kFallback);
    // length past the row
    EXPECT_EQ(CheckRow(std::string("\x0a\x7f\x09", 3), 3, 0), xstore::RowDecode::kFallback);
    // truncated double inside FData
    EXPECT_EQ(CheckRow(std::string("\x0a\x05\x09\x00\x00\x00\x00", 7), 7, 0), xstore::RowDecode::kFallback);
    // over-long varint (11 bytes) as an int64
    EXPECT_EQ(CheckRow(std::string("\x0a\x0c\x20\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", 14), 14, 0), xstore::RowDecode::kFallback);
    // a group inside FData (field 20, start group)
    EXPECT_EQ(CheckRow(std::string("\x0a\x02\xa3\x01", 4), 8, 0), xstore::RowDecode::kFallback);
}

TEST(RowDecodeTest, TheLastOneofMemberOnTheWireWinsAndUnknownFieldsAreSkipped) {
    auto fdata = [](const std::string &body) { return std::string("\x0a", 1) + static_cast<char>(body.size()) + body; };
    const std::string i32_5 = "\x18\x05", i32_9 = "\x18\x09", bool_true = "\x68\x01", bytes_ab = "\x7a\x02" "ab";
    const std::string unknown_varint = "\xa0\x06\x07";            // field 100, varint
    const std::string wrong_type_double = "\x08\x07";             // field 1 as a varint: unknown to FData, must not set the oneof
    for (const std::string &body : {i32_5 + i32_9, i32_5 + bool_true, bool_true + i32_5, i32_5 + bytes_ab, unknown_varint + i32_9,
                                    i32_9 + unknown_varint, wrong_type_double, i32_5 + wrong_type_double, std::string()}) {
        CheckRow(fdata(body), 64, 0);
    }
    EXPECT_EQ(CheckRow(fdata(i32_5 + i32_9), 64, 0), xstore::RowDecode::kValue);
    EXPECT_EQ(CheckRow(fdata(i32_5 + bool_true), 64, 0), xstore::RowDecode::kSkip);      // bool came last
    EXPECT_EQ(CheckRow(fdata(bool_true + i32_5), 64, 0), xstore::RowDecode::kValue);
    EXPECT_EQ(CheckRow(fdata(i32_5 + wrong_type_double), 64, 0), xstore::RowDecode::kValue);
    EXPECT_EQ(CheckRow(fdata(wrong_type_double), 64, 0), xstore::RowDecode::kSkip);
    // an unknown fixed64 / fixed32 field (field 100) cut short, after a valid int32: protobuf fails the row
    EXPECT_EQ(CheckRow(fdata(i32_5 + std::string("\xa1\x06", 2) + std::string(7, '\x11')), 64, 0), xstore::RowDecode::kFallback);
    EXPECT_EQ(CheckRow(fdata(i32_5 + std::string("\xa5\x06", 2) + std::string(3, '\x11')), 64, 0), xstore::RowDecode::kFallback);
    // a string followed by a number: whether the number counts depends on the string's UTF-8, which only protobuf checks
    const std::string string_ab = "\x72\x02" "ab", string_bad = "\x72\x01\xff";
    EXPECT_EQ(CheckRow(fdata(string_ab + i32_5), 64, 0), xstore::RowDecode::kFallback);
    EXPECT_EQ(CheckRow(fdata(string_bad + i32_5), 64, 0), xstore::RowDecode::kFallback);
    EXPECT_EQ(CheckRow(fdata(i32_5 + string_ab), 64, 0), xstore::RowDecode::kSkip);     // string last: not a number
    EXPECT_EQ(CheckRow(fdata(i32_5 + string_bad), 64, 0), xstore::RowDecode::kSkip);    // bad UTF-8: protobuf drops the row, same
    // field number 0 (tag 0x01 = field 0, 64-bit) before a valid int32: protobuf's call
    EXPECT_EQ(CheckRow(fdata(std::string("\x01\0\0\0\0\0\0\0\0", 9) + i32_5), 64, 0), xstore::RowDecode::kFallback);
}

TEST(RowDecodeTest, NeverDisagreesWithTheProtobufPathOnDamagedRows) {
    const std::vector<XStore_Proto::FData> neighbours = Neighbours();
    const std::string base = SerializeRow(neighbours);
    std::mt19937_64 gen(20261103);
    size_t decided = 0, fell_back = 0;
    for (int round = 0; round < 60000; ++round) {
        std::string row = base;
        row.resize(base.size() + 16, '\0');
        const int edits = 1 + static_cast<int>(gen() % 4);
        for (int e = 0; e < edits; ++e) row[gen() % row.size()] = static_cast<char>(gen());
        if (gen() % 4 == 0) row.resize(gen() % row.size());
        for (unsigned short column : {0, 1, 3, 7, 11, 12, 14}) {
            (CheckRow(row, row.size(), column) == xstore::RowDecode::kFallback ? fell_back : decided)++;
        }
    }
    EXPECT_GT(decided, 100000u);
    EXPECT_GT(fell_back, 1000u);
}

// ---- the reductions over spans (pieces of the collected values, never concatenated), run as tasks ----

xstore::TaskRunner Inline() { return xstore::RunInline; }

// Four real threads pulling tasks, to catch a race between tasks.
xstore::TaskRunner Threaded() {
    return [](size_t tasks, const std::function<void(size_t)> &body) {
        std::atomic<size_t> next{0};
        std::vector<std::thread> threads;
        for (size_t t = 0; t < std::min<size_t>(4, tasks); ++t) {
            threads.emplace_back([&] {
                for (size_t i; (i = next.fetch_add(1)) < tasks;) body(i);
            });
        }
        for (std::thread &t : threads) t.join();
    };
}

// Tiny slices, ranges and gather limit, so a few hundred values already take many slices, ranges and radix levels.
xstore::ReduceTuning Tiny() {
    xstore::ReduceTuning tuning;
    tuning.slice_values = 64;
    tuning.max_ranges = 7;
    tuning.samples_per_slice = 4;
    tuning.gather_max = 16;
    return tuning;
}

// `v` as spans of uneven sizes, empty ones included.
std::vector<xstore::ValueSpan> Cut(std::vector<double> &v, uint32_t seed) {
    std::mt19937 gen(seed);
    std::vector<xstore::ValueSpan> spans;
    spans.push_back({v.data(), 0});
    for (size_t at = 0; at < v.size();) {
        const size_t len = std::min<size_t>(v.size() - at, gen() % 3 == 0 ? 0 : 1 + gen() % (v.size() / 3 + 1));
        spans.push_back({v.data() + at, len});
        at += len;
    }
    spans.push_back({v.data() + v.size(), 0});
    return spans;
}

struct RunnerCase { const char *name; xstore::TaskRunner run; };
std::vector<RunnerCase> Runners() { return {{"inline", Inline()}, {"threaded", Threaded()}}; }
std::vector<xstore::ReduceTuning> Tunings() { return {xstore::ReduceTuning{}, Tiny()}; }

// Values chosen to push a radix select through several levels: they share their top bits, differ only far down, or are all alike.
std::vector<std::vector<double>> AwkwardPercentileData() {
    std::vector<std::vector<double>> sets;
    std::mt19937_64 gen(5);
    sets.push_back(std::vector<double>(500, 3.25));                                         // all alike
    {
        std::vector<double> v;                                                              // differ in the last mantissa bits only
        for (int i = 0; i < 700; ++i) v.push_back(1.0 + static_cast<double>(gen() % 1000) * std::ldexp(1.0, -52));
        sets.push_back(v);
    }
    {
        std::vector<double> v;                                                              // [1, 2): one exponent
        for (int i = 0; i < 900; ++i) v.push_back(1.0 + static_cast<double>(gen() % 100000) / 100000.0);
        sets.push_back(v);
    }
    {
        std::vector<double> v;                                                              // both signs, zeros, subnormals, one infinity
        for (int i = 0; i < 600; ++i) v.push_back((static_cast<double>(gen() % 2001) - 1000.0) * 0.5);
        v.insert(v.end(), {0.0, -0.0, std::numeric_limits<double>::denorm_min(), -std::numeric_limits<double>::denorm_min(),
                           std::numeric_limits<double>::infinity()});
        sets.push_back(v);
    }
    {
        std::vector<double> v;                                                              // a few huge, mostly tiny
        for (int i = 0; i < 400; ++i) v.push_back(static_cast<double>(gen() % 7));
        v.insert(v.end(), {1e300, -1e300, 1e-300, -1e-300});
        sets.push_back(v);
    }
    sets.push_back({42.0});
    sets.push_back({-1.0, 1.0});
    return sets;
}

TEST(ExactSpanPercentileTest, MatchesTheSingleVectorPercentileOnEveryShape) {
    std::vector<std::vector<double>> sets = AwkwardPercentileData();
    for (size_t n : {1u, 2u, 3u, 10u, 1000u, 4097u}) {
        for (size_t distinct : {1u, 3u, 50u, 100000u}) sets.push_back(Values(n, distinct, static_cast<uint32_t>(n * 31 + distinct)));
    }
    uint32_t seed = 1;
    for (const std::vector<double> &data : sets) {
        for (double p : {0.0, 0.01, 0.25, 0.5, 0.75, 0.9, 0.999, 1.0}) {
            std::vector<double> oracle_copy = data;
            const double expected = xstore::ExactPercentile(oracle_copy, p);
            for (const RunnerCase &runner : Runners()) {
                for (const xstore::ReduceTuning &tuning : Tunings()) {
                    std::vector<double> work = data;
                    uint64_t skipped = 99;
                    const double got = xstore::ExactPercentile(Cut(work, ++seed), p, runner.run, &skipped, tuning);
                    EXPECT_EQ(got, expected) << "n=" << data.size() << " p=" << p << " runner " << runner.name << " slice " << tuning.slice_values;
                    EXPECT_EQ(skipped, 0u);
                }
            }
        }
    }
}

TEST(ExactSpanPercentileTest, SkipsNaNsAndAnAllNaNInputIsNaN) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::mt19937_64 gen(9);
    for (size_t nans : {1u, 5u, 300u}) {
        std::vector<double> data = Values(400, 97, 3);
        std::vector<double> numbers = data;
        for (size_t i = 0; i < nans; ++i) data.insert(data.begin() + static_cast<std::ptrdiff_t>(gen() % (data.size() + 1)), i % 2 ? -nan : nan);
        for (double p : {0.0, 0.3, 0.5, 1.0}) {
            std::vector<double> reference = numbers;
            const double expected = xstore::ExactPercentile(reference, p);
            for (const RunnerCase &runner : Runners()) {
                for (const xstore::ReduceTuning &tuning : Tunings()) {
                    std::vector<double> work = data;
                    uint64_t skipped = 0;
                    EXPECT_EQ(xstore::ExactPercentile(Cut(work, 7), p, runner.run, &skipped, tuning), expected) << "nans=" << nans << " p=" << p;
                    EXPECT_EQ(skipped, nans);
                }
            }
        }
    }
    std::vector<double> only_nan(50, nan);
    uint64_t skipped = 0;
    EXPECT_TRUE(std::isnan(xstore::ExactPercentile(Cut(only_nan, 1), 0.5, Inline(), &skipped, Tiny())));
    EXPECT_EQ(skipped, 50u);
    std::vector<double> nothing;
    EXPECT_TRUE(std::isnan(xstore::ExactPercentile(std::vector<xstore::ValueSpan>{}, 0.5, Inline(), &skipped)));
    EXPECT_EQ(skipped, 0u);
}

TEST(ExactSpanDistinctCountTest, MatchesAHashSetOnEveryShape) {
    uint32_t seed = 100;
    for (size_t n : {0u, 1u, 2u, 10u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 2u, 7u, 300u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 7 + distinct));
            for (const RunnerCase &runner : Runners()) {
                for (const xstore::ReduceTuning &tuning : Tunings()) {
                    std::vector<double> work = data;
                    EXPECT_EQ(xstore::ExactDistinctCount(Cut(work, ++seed), runner.run, tuning), ReferenceDistinct(data))
                        << "n=" << n << " distinct=" << distinct << " runner " << runner.name << " slice " << tuning.slice_values;
                }
            }
        }
    }
}

TEST(ExactSpanDistinctCountTest, SignedZerosAreOneValueAndEachNaNIsOne) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (const RunnerCase &runner : Runners()) {
        for (const xstore::ReduceTuning &tuning : Tunings()) {
            std::vector<double> zeros = {0.0, -0.0, 0.0, -0.0, 1.0, -0.0};
            EXPECT_EQ(xstore::ExactDistinctCount(Cut(zeros, 1), runner.run, tuning), 2u);
            std::vector<double> mixed;
            for (int i = 0; i < 300; ++i) mixed.push_back(i % 3 == 0 ? nan : static_cast<double>(i % 11));   // 100 NaNs, 11 numbers
            EXPECT_EQ(xstore::ExactDistinctCount(Cut(mixed, 2), runner.run, tuning), 111u);
            const double inf = std::numeric_limits<double>::infinity();
            std::vector<double> extremes = {inf, -inf, inf, 1e308, -1e308, std::numeric_limits<double>::denorm_min(), 0.0};
            EXPECT_EQ(xstore::ExactDistinctCount(Cut(extremes, 3), runner.run, tuning), 6u);
        }
    }
    EXPECT_EQ(xstore::ExactDistinctCount(std::vector<xstore::ValueSpan>{}, Inline()), 0u);
}

TEST(ExactSpanTopKTest, MatchesAFrequencyMapOnEveryShape) {
    uint32_t seed = 200;
    for (size_t n : {0u, 1u, 2u, 10u, 1000u, 20000u}) {
        for (size_t distinct : {1u, 2u, 7u, 300u, 1000000u}) {
            const std::vector<double> data = Values(n, distinct, static_cast<uint32_t>(n * 13 + distinct));
            for (size_t k : {0u, 1u, 3u, 10u, 100000u}) {
                for (const RunnerCase &runner : Runners()) {
                    for (const xstore::ReduceTuning &tuning : Tunings()) {
                        std::vector<double> work = data;
                        EXPECT_EQ(xstore::ExactTopK(Cut(work, ++seed), k, runner.run, tuning), ReferenceTopK(data, k))
                            << "n=" << n << " distinct=" << distinct << " k=" << k << " runner " << runner.name << " slice " << tuning.slice_values;
                    }
                }
            }
        }
    }
}

TEST(ExactSpanTopKTest, TiesByAscendingValueAndNaNsComeAfterTheNumbers) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    for (const RunnerCase &runner : Runners()) {
        for (const xstore::ReduceTuning &tuning : Tunings()) {
            std::vector<double> ties = {5.0, 3.0, 5.0, 3.0, 9.0, 1.0, 9.0, 1.0, 7.0};   // 1, 3, 5, 9 twice each; 7 once
            EXPECT_EQ(xstore::ExactTopK(Cut(ties, 1), 3, runner.run, tuning), (Counted{{1.0, 2}, {3.0, 2}, {5.0, 2}}));
            std::vector<double> data = {4.0, 4.0, nan, 2.0, nan, 4.0, 2.0};
            const Counted top = xstore::ExactTopK(Cut(data, 2), 10, runner.run, tuning);
            ASSERT_EQ(top.size(), 4u);
            EXPECT_EQ(top[0], (std::pair<double, uint64_t>{4.0, 3}));
            EXPECT_EQ(top[1], (std::pair<double, uint64_t>{2.0, 2}));
            EXPECT_TRUE(std::isnan(top[2].first) && top[2].second == 1);
            EXPECT_TRUE(std::isnan(top[3].first) && top[3].second == 1);
            std::vector<double> few = {nan, 8.0, nan, nan, 8.0, 6.0};
            const Counted two = xstore::ExactTopK(Cut(few, 3), 2, runner.run, tuning);   // 8 x2, then the count-1 numbers beat NaNs
            ASSERT_EQ(two.size(), 2u);
            EXPECT_EQ(two[0], (std::pair<double, uint64_t>{8.0, 2}));
            EXPECT_EQ(two[1], (std::pair<double, uint64_t>{6.0, 1}));
        }
    }
    EXPECT_TRUE(xstore::ExactTopK(std::vector<xstore::ValueSpan>{}, 5, Inline()).empty());
}

// One value in nearly every slice and range, and a value only in the last slice: the merge must add counts across slices.
TEST(ExactSpanTopKTest, CountsAddUpAcrossSlicesAndRanges) {
    std::vector<double> data;
    for (int i = 0; i < 5000; ++i) data.push_back(static_cast<double>(i % 5));
    for (int i = 0; i < 40; ++i) data.push_back(100.0);
    std::mt19937_64 gen(77);
    std::shuffle(data.begin(), data.end(), gen);
    for (const RunnerCase &runner : Runners()) {
        for (const xstore::ReduceTuning &tuning : Tunings()) {
            std::vector<double> work = data;
            EXPECT_EQ(xstore::ExactTopK(Cut(work, 4), 6, runner.run, tuning), ReferenceTopK(data, 6));
        }
    }
}

}  // namespace
