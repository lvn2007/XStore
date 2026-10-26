// livewiden::NextChunkRows (XStore_LiveWidenChunk.h): the live widen's chunk-size rule, pure arithmetic. The chunk size
// must not collapse to one row when the lock hold is long only because the chunk's seeks are slow (Mystic 2026-10-26:
// SEEK_DATA/SEEK_HOLE took 4-32 ms under an insert load, every chunk halved down to one row, a year widened at
// ~30 rows/s). The lock-timeout path (not tested here) still shrinks to one row.
#include "XStore/XStore_LiveWidenChunk.h"

#include "XStore/XStore_Tunables.h"

#include <gtest/gtest.h>

#include <cstdint>

using livewiden::NextChunkRows;

namespace {
constexpr uint64_t kTargetUs = XStoreTunables::LIVE_WIDEN_LOCK_HOLD_TARGET_US;
constexpr uint64_t kFloor = XStoreTunables::LIVE_WIDEN_HOLD_ADAPT_FLOOR_ROWS;
constexpr uint64_t kMax = 744;  // 1 MiB of 1408-byte rows
}  // namespace

TEST(LiveWidenChunkTest, ASlowHoldNeverShrinksTheChunkBelowTheFloor) {
    uint64_t rows = 256;
    for (int i = 0; i < 30; ++i) rows = NextChunkRows(rows, 20 * kTargetUs, kTargetUs, kFloor, kMax);
    EXPECT_EQ(rows, kFloor);
}

TEST(LiveWidenChunkTest, AChunkBelowTheFloorAfterTimeoutsReturnsToTheFloor) {
    EXPECT_EQ(NextChunkRows(1, 20 * kTargetUs, kTargetUs, kFloor, kMax), kFloor);
    EXPECT_EQ(NextChunkRows(1, 1, kTargetUs, kFloor, kMax), kFloor);
}

TEST(LiveWidenChunkTest, AFastHoldDoublesUpToTheMaximum) {
    EXPECT_EQ(NextChunkRows(300, 100, kTargetUs, kFloor, kMax), 600u);
    EXPECT_EQ(NextChunkRows(600, 100, kTargetUs, kFloor, kMax), kMax);
}

TEST(LiveWidenChunkTest, AHoldAtTheTargetKeepsTheChunk) {
    EXPECT_EQ(NextChunkRows(400, kTargetUs, kTargetUs, kFloor, kMax), 400u);
}

TEST(LiveWidenChunkTest, AHoldAboveTheFloorHalvesAtMostOncePerChunk) {
    EXPECT_EQ(NextChunkRows(700, 100 * kTargetUs, kTargetUs, kFloor, kMax), 350u);
}

TEST(LiveWidenChunkTest, TheFloorNeverExceedsTheMaximum) {
    EXPECT_EQ(NextChunkRows(50, 20 * kTargetUs, kTargetUs, kFloor, 100), 100u);
    EXPECT_EQ(NextChunkRows(100, 20 * kTargetUs, kTargetUs, kFloor, 100), 100u);
}

TEST(LiveWidenChunkTest, AZeroHoldIsTreatedAsOneMicrosecond) {
    EXPECT_EQ(NextChunkRows(300, 0, kTargetUs, kFloor, kMax), 600u);
}
