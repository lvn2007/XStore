#include <gtest/gtest.h>
#include "XStore/XStore_RowScan.h"
#include "XStoreTestSupport.h"
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace {
using xstore_test::ScratchDir;

struct Visit {
    std::vector<uint64_t> order;
    std::map<uint64_t, std::string> bytes;
};

// Rows the helper is allowed to hand over although never written (hole edges
// widened to the filesystem block) are all-zero; callers skip them on row[0]==0.
class RowScanTest : public ::testing::Test {
protected:
    ScratchDir scratch{"xstore_rowscan_test"};
    int fd = -1;

    void SetUp() override {
        fd = open((scratch.test_root / "year.XSTORE").c_str(), O_RDWR | O_CREAT, 0644);
        ASSERT_GE(fd, 0);
    }
    void TearDown() override {
        if (fd >= 0) close(fd);
    }

    void SizeTo(uint64_t rows, uint16_t row_length) {
        ASSERT_EQ(ftruncate(fd, static_cast<off_t>(rows * row_length)), 0);
    }
    void WriteRow(uint64_t index, uint16_t row_length, char fill) {
        std::string row(row_length, fill);
        ASSERT_EQ(pwrite(fd, row.data(), row.size(), static_cast<off_t>(index * row_length)),
                  static_cast<ssize_t>(row_length));
    }
    bool Scan(uint64_t total_rows, uint16_t row_length, Visit &v) {
        return ForEachDataRow(fd, total_rows, row_length, [&](uint64_t idx, const char *ptr) {
            v.order.push_back(idx);
            v.bytes[idx].assign(ptr, row_length);
        });
    }
    static void ExpectStrictlyAscending(const Visit &v) {
        for (size_t i = 1; i < v.order.size(); ++i) EXPECT_LT(v.order[i - 1], v.order[i]);
    }
};

TEST_F(RowScanTest, SparseFileVisitsEveryWrittenRowExactlyOnce) {
    constexpr uint16_t kRowLength = 100; // not a divisor of 4096: rows 40 and 81 straddle block edges
    constexpr uint64_t kRows = 200000;
    SizeTo(kRows, kRowLength);
    const std::vector<uint64_t> written = {0, 40, 81, 82, 150000, kRows - 1};
    for (uint64_t r : written) WriteRow(r, kRowLength, static_cast<char>('A' + r % 20));

    Visit v;
    ASSERT_TRUE(Scan(kRows, kRowLength, v));
    ExpectStrictlyAscending(v);
    for (uint64_t r : written) {
        ASSERT_EQ(v.bytes.count(r), 1u) << "row " << r << " not visited";
        EXPECT_EQ(v.bytes[r], std::string(kRowLength, static_cast<char>('A' + r % 20)));
    }
    // Extra rows are block-widening zero rows only; nothing else may appear.
    for (const auto &[idx, bytes] : v.bytes) {
        if (std::find(written.begin(), written.end(), idx) == written.end()) {
            EXPECT_EQ(bytes, std::string(kRowLength, '\0')) << "row " << idx;
        }
    }
    EXPECT_LT(v.order.size(), 400u) << "holes were not skipped";
}

TEST_F(RowScanTest, RowLongerThanBlockWithHoleInsideIsVisitedOnce) {
    constexpr uint16_t kRowLength = 10000; // row 1 spans 10000..19999, hole at 12288..16383
    constexpr uint64_t kRows = 10;
    SizeTo(kRows, kRowLength);
    char first = 'x';
    char last = 'y';
    ASSERT_EQ(pwrite(fd, &first, 1, 10000), 1);
    ASSERT_EQ(pwrite(fd, &last, 1, 19999), 1);

    Visit v;
    ASSERT_TRUE(Scan(kRows, kRowLength, v));
    ExpectStrictlyAscending(v);
    ASSERT_EQ(v.bytes.count(1), 1u);
    EXPECT_EQ(v.bytes[1][0], 'x');
    EXPECT_EQ(v.bytes[1][kRowLength - 1], 'y');
}

TEST_F(RowScanTest, DenseFileSpanningSeveralChunksVisitsAllRows) {
    constexpr uint16_t kRowLength = 100;
    constexpr uint64_t kRows = 30000; // ~3 MiB: three 1 MiB chunks
    std::string all(kRows * kRowLength, '\0');
    for (uint64_t r = 0; r < kRows; ++r) all[r * kRowLength] = static_cast<char>(1 + r % 100);
    ASSERT_EQ(pwrite(fd, all.data(), all.size(), 0), static_cast<ssize_t>(all.size()));

    Visit v;
    ASSERT_TRUE(Scan(kRows, kRowLength, v));
    ASSERT_EQ(v.order.size(), kRows);
    for (uint64_t r = 0; r < kRows; ++r) {
        ASSERT_EQ(v.order[r], r);
        ASSERT_EQ(v.bytes[r][0], static_cast<char>(1 + r % 100));
    }
}

TEST_F(RowScanTest, EmptyFileVisitsNothingAndSucceeds) {
    Visit v;
    EXPECT_TRUE(Scan(0, 64, v));
    EXPECT_TRUE(v.order.empty());
}

TEST_F(RowScanTest, TrailingPartialRowIsNotVisited) {
    constexpr uint16_t kRowLength = 100;
    ASSERT_EQ(ftruncate(fd, 5 * kRowLength + 37), 0);
    WriteRow(4, kRowLength, 'q');
    char tail = 'z';
    ASSERT_EQ(pwrite(fd, &tail, 1, 5 * kRowLength), 1);

    Visit v;
    const uint64_t total_rows = static_cast<uint64_t>(5 * kRowLength + 37) / kRowLength;
    ASSERT_TRUE(Scan(total_rows, kRowLength, v));
    ASSERT_EQ(v.bytes.count(4), 1u);
    EXPECT_EQ(v.bytes[4], std::string(kRowLength, 'q'));
    EXPECT_EQ(v.bytes.count(5), 0u);
    for (uint64_t idx : v.order) EXPECT_LT(idx, 5u);
}

TEST_F(RowScanTest, RowsPastEndOfFileMakeScanIncomplete) {
    constexpr uint16_t kRowLength = 100;
    SizeTo(3, kRowLength);
    WriteRow(0, kRowLength, 'a');
    WriteRow(2, kRowLength, 'c');

    Visit v;
    EXPECT_FALSE(Scan(6, kRowLength, v)) << "caller claimed 3 rows more than the file holds";
    EXPECT_EQ(v.bytes[0], std::string(kRowLength, 'a'));
    EXPECT_EQ(v.bytes[2], std::string(kRowLength, 'c'));
}

TEST_F(RowScanTest, BadFdReturnsFalse) {
    Visit v;
    EXPECT_FALSE(ForEachDataRow(-1, 10, 64, [&](uint64_t, const char *) { v.order.push_back(0); }));
    EXPECT_TRUE(v.order.empty());
}

} // namespace
