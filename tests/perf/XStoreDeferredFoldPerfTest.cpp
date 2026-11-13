// CPU cost per row of FoldStoredRows (deriving the index observations from the stored bytes and folding them), the one
// fold of every live path since the rows-only records (stage 3-4 Task 8). Opt-in (XSTORE_FOLD_PERF=1); prints numbers;
// no timing threshold. Stage 1 measured it against the inline path it replaced (WalRecord::AppendRow's observations,
// since removed): deferred / inline = 0.667 (docs/superpowers/specs/2026-11-04-async-indexer-design.md).
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

#include "include/XStore/XStore.h"

namespace {

using Clock = std::chrono::steady_clock;

double NsPerRow(Clock::time_point start, uint64_t rows) {
    return std::chrono::duration<double, std::nano>(Clock::now() - start).count() / static_cast<double>(rows);
}

XStore_Proto::RowData DoubleRow(uint64_t i, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(i) * 0.5 + c);
    return row;
}

class DeferredFoldPerfTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (std::getenv("XSTORE_FOLD_PERF") == nullptr) GTEST_SKIP() << "set XSTORE_FOLD_PERF=1 to run";
        dir_ = std::filesystem::temp_directory_path() / ("xstore_fold_perf_" + std::to_string(getpid()));
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::filesystem::path dir_;
};

void RunShape(const std::filesystem::path &dir, int columns, uint64_t rows, const char *label) {
    constexpr uint16_t ROW_LENGTH = 1408;
    const uint32_t width = static_cast<uint32_t>(std::max(columns, 64));

    // The rows as stored bytes in a year file (setup, not timed), then FoldStoredRows.
    std::string bytes(rows * ROW_LENGTH, '\0');
    for (uint64_t i = 0; i < rows; ++i) {
        const XStore_Proto::RowData row = DoubleRow(i, columns);
        ASSERT_LE(row.ByteSizeLong(), ROW_LENGTH);
        ASSERT_TRUE(row.SerializeToArray(&bytes[i * ROW_LENGTH], ROW_LENGTH));
    }
    const std::string data_path = (dir / "year.XSTORE").string();
    const int fd = open(data_path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(pwrite(fd, bytes.data(), bytes.size(), 0), static_cast<ssize_t>(bytes.size()));
    ZoneMapFile zmap;
    CatIdxFile catidx;
    ASSERT_TRUE(zmap.Open((dir / "deferred.zmap").string(), ROW_LENGTH, width));
    ASSERT_TRUE(catidx.Open((dir / "deferred.catidx").string(), ROW_LENGTH, width));
    const auto start = Clock::now();
    ASSERT_TRUE(XStore::FoldStoredRows(fd, ROW_LENGTH, width, catidx.TrackedColumnCount(), {{0, rows}}, zmap, catidx));
    const double ns = NsPerRow(start, rows);
    close(fd);
    std::cout << "[fold] " << label << " columns=" << columns << " rows=" << rows << " deferred_ns/row=" << ns << "\n";
}

TEST_F(DeferredFoldPerfTest, CpuPerRowWide128Doubles) { RunShape(dir_, 128, 200000, "bench_db_like"); }
TEST_F(DeferredFoldPerfTest, CpuPerRowNarrow16Doubles) { RunShape(dir_, 16, 400000, "narrow"); }

}  // namespace
