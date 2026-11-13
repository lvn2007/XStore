#include <gtest/gtest.h>
#include "include/XStore/XStore_PendingMap.h"
#include "include/XStore/XStore_ZoneMap.h"
#include "include/XStore/XStore_CatIdx.h"
#include "XStoreTestSupport.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <algorithm>
#include <memory>
#include <random>
#include <set>
#include <thread>
#include <vector>

namespace {
using xstore::IndexRun;
using xstore::PendingMap;
using xstore::PendingMaps;

TEST(PendingMapTest, MarkMakesTheBlockPendingAndClearReleasesIt) {
    PendingMap map(100);
    EXPECT_FALSE(map.IsPending(0));
    map.MarkRows(95, 10);   // rows 95..104: blocks 0 and 1
    EXPECT_TRUE(map.IsPending(0));
    EXPECT_TRUE(map.IsPending(1));
    EXPECT_EQ(map.PendingBlockCount(), 2u);
    map.ClearRows(95, 5);   // block 0 done
    EXPECT_FALSE(map.IsPending(0));
    EXPECT_TRUE(map.IsPending(1));
    EXPECT_EQ(map.PendingBlockCount(), 1u);
    map.ClearRows(100, 5);
    EXPECT_FALSE(map.IsPending(1));
    EXPECT_EQ(map.PendingBlockCount(), 0u);
}

TEST(PendingMapTest, ClearingRowsThatAreNotPendingChangesNothing) {
    PendingMap map(100);
    map.MarkRows(10, 5);
    map.ClearRows(50, 20);
    EXPECT_TRUE(map.IsPending(0));
    EXPECT_EQ(map.PendingBlockCount(), 1u);
}

TEST(PendingMapTest, PendingRunsOfABlockAreTheMaximalRunsOfSetBits) {
    PendingMap map(100);
    map.MarkRows(3, 4);     // 3..6
    map.MarkRows(10, 1);    // 10
    map.MarkRows(11, 2);    // 11..12 joins 10
    map.MarkRows(99, 1);
    const std::vector<IndexRun> runs = map.PendingRunsOfBlock(0);
    ASSERT_EQ(runs.size(), 3u);
    EXPECT_EQ(runs[0].first_row, 3u);  EXPECT_EQ(runs[0].row_count, 4u);
    EXPECT_EQ(runs[1].first_row, 10u); EXPECT_EQ(runs[1].row_count, 3u);
    EXPECT_EQ(runs[2].first_row, 99u); EXPECT_EQ(runs[2].row_count, 1u);
    EXPECT_TRUE(map.PendingRunsOfBlock(5).empty());
}

TEST(PendingMapTest, MarkingARowTwiceKeepsOneBit) {
    PendingMap map(100);
    map.MarkRows(7, 1);
    map.MarkRows(7, 1);
    map.ClearRows(7, 1);
    EXPECT_FALSE(map.IsPending(0));
    EXPECT_EQ(map.PendingBlockCount(), 0u);
    EXPECT_EQ(map.TotalMarkedRowsForTests(), 2u) << "the counter counts MarkRows rows, set or not";
}

TEST(PendingMapTest, AnyPendingInRowsIsExact) {
    PendingMap map(100);
    map.MarkRows(250, 3);   // block 2
    EXPECT_TRUE(map.AnyPendingInRows(0, 1000));
    EXPECT_TRUE(map.AnyPendingInRows(200, 299));
    EXPECT_FALSE(map.AnyPendingInRows(0, 199));
    EXPECT_FALSE(map.AnyPendingInRows(300, 1000));
}

TEST(PendingMapTest, ASummaryCollisionOnlyEverAddsAFalsePositive) {
    PendingMap map(10);
    // Find a second block that shares the summary slot with block 0 by brute force.
    map.MarkRows(0, 1);
    uint64_t colliding = 0;
    for (uint64_t b = 1; b < 10'000'000 && colliding == 0; ++b) {
        if (map.IsPending(b)) colliding = b;
    }
    ASSERT_NE(colliding, 0u) << "no collision found in 10 M blocks";
    EXPECT_TRUE(map.IsPending(colliding)) << "false positive: safe (the query scans)";
    EXPECT_FALSE(map.AnyPendingInRows(colliding * 10, colliding * 10 + 9)) << "the exact check is not fooled";
    map.ClearRows(0, 1);
    EXPECT_FALSE(map.IsPending(0));
    EXPECT_FALSE(map.IsPending(colliding));
}

TEST(PendingMapTest, ConcurrentMarkAndClearOfDisjointRowsEndsEmpty) {
    PendingMap map(512);
    constexpr int THREADS = 8;
    constexpr int ROUNDS = 2000;
    std::vector<std::thread> threads;
    for (int t = 0; t < THREADS; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < ROUNDS; ++i) {
                const uint64_t first = static_cast<uint64_t>(t) * 100000 + static_cast<uint64_t>(i % 50) * 7;
                map.MarkRows(first, 5);
                EXPECT_TRUE(map.IsPending(first / 512));
                map.ClearRows(first, 5);
            }
        });
    }
    for (auto &t : threads) t.join();
    EXPECT_EQ(map.PendingBlockCount(), 0u);
    // Every slot counter is back to zero, so no touched block reads pending (no collision can remain).
    for (int t = 0; t < THREADS; ++t) {
        for (uint64_t b = static_cast<uint64_t>(t) * 100000 / 512; b <= (static_cast<uint64_t>(t) * 100000 + 49 * 7 + 5) / 512; ++b) {
            EXPECT_FALSE(map.IsPending(b)) << "thread " << t << " block " << b;
        }
    }
}

TEST(PendingMapTest, ThreadsMarkingAndClearingDifferentRowsOfTheSameBlockEndNotPending) {
    PendingMap map(1489);
    constexpr int THREADS = 8;
    std::vector<std::thread> threads;
    for (int t = 0; t < THREADS; ++t) {
        threads.emplace_back([&, t] {
            // Thread t owns rows t*100 .. t*100+99 of block 3 (they share words with their neighbours).
            const uint64_t first = 3 * 1489 + static_cast<uint64_t>(t) * 100;
            for (int i = 0; i < 5000; ++i) {
                map.MarkRows(first, 100);
                EXPECT_TRUE(map.IsPending(3));
                map.ClearRows(first, 100);
            }
        });
    }
    for (auto &t : threads) t.join();
    EXPECT_EQ(map.PendingBlockCount(), 0u);
    EXPECT_FALSE(map.IsPending(3));
    EXPECT_TRUE(map.PendingRunsOfBlock(3).empty());
}

TEST(PendingMapsTest, ForCreatesFindFindsForgetMatchingRemoves) {
    PendingMaps::ClearForTests();
    EXPECT_EQ(PendingMaps::Find("/db/1971"), nullptr);
    auto a = PendingMaps::For("/db/1971", 2048);
    EXPECT_EQ(PendingMaps::For("/db/1971", 2048), a) << "same key, same geometry: the same map";
    EXPECT_NE(PendingMaps::For("/db/1971", 4096), a) << "a re-created DB with another geometry gets a fresh map";
    auto b = PendingMaps::For("/other/1971", 2048);
    EXPECT_FALSE(PendingMaps::AnyPending());
    b->MarkRows(0, 1);
    EXPECT_TRUE(PendingMaps::AnyPending());
    PendingMaps::ForgetMatching([](const std::string &key) { return key.rfind("/other/", 0) == 0; });
    EXPECT_EQ(PendingMaps::Find("/other/1971"), nullptr);
    EXPECT_TRUE(PendingMaps::AnyPending()) << "a forgotten map that is still held keeps counting (the safe side)";
    b.reset();
    EXPECT_FALSE(PendingMaps::AnyPending());
    EXPECT_NE(PendingMaps::Find("/db/1971"), nullptr);
    PendingMaps::ClearForTests();
}

TEST(PendingMapsTest, ADestroyedOrForgottenMapLeavesTheProcessWideCountBalanced) {
    PendingMaps::ClearForTests();
    { PendingMap local(100); local.MarkRows(0, 1); local.MarkRows(250, 400); }
    EXPECT_FALSE(PendingMaps::AnyPending()) << "destroying a map with pending blocks must release them";
    auto live = PendingMaps::For("/live/1971", 100);
    auto held = PendingMaps::For("/held/1971", 100);
    live->MarkRows(0, 1);
    held->MarkRows(0, 1);
    PendingMaps::ForgetMatching([](const std::string &key) { return key.rfind("/held/", 0) == 0; });
    held->MarkRows(500, 1);
    held->ClearRows(0, 1);
    held->ClearRows(500, 1);   // the forgotten map is empty again
    held.reset();
    EXPECT_TRUE(PendingMaps::AnyPending()) << "the live map is still pending";
    live->ClearRows(0, 1);
    EXPECT_FALSE(PendingMaps::AnyPending()) << "no double release, no leak";
    // A forgotten map still holding a pending block keeps counting until its last holder lets go.
    held = PendingMaps::For("/held/1971", 100);
    held->MarkRows(0, 1);
    PendingMaps::ClearForTests();
    EXPECT_TRUE(PendingMaps::AnyPending());
    held.reset();
    EXPECT_FALSE(PendingMaps::AnyPending());
}

// The registry must not keep a 64 KiB map for every (db, year) ever opened: past SWEEP_ABOVE entries, For() drops the
// maps nobody holds and nothing is pending in. A map with pending marks, or one a file still holds, always survives.
TEST(PendingMapsTest, ForSweepsMapsNobodyHoldsWithNothingPending) {
    PendingMaps::ClearForTests();
    auto marked = PendingMaps::For("/sweep/marked", 100);
    marked->MarkRows(0, 1);
    PendingMap *const marked_raw = marked.get();
    marked.reset();   // only the registry holds it now, but it has a pending block
    auto held = PendingMaps::For("/sweep/held", 100);
    for (size_t i = 0; i < 2 * PendingMaps::SWEEP_ABOVE; ++i) {
        (void)PendingMaps::For("/sweep/released/" + std::to_string(i), 100);
    }
    EXPECT_LE(PendingMaps::SizeForTests(), PendingMaps::SWEEP_ABOVE + 1) << "released, empty maps are swept";
    EXPECT_EQ(PendingMaps::For("/sweep/held", 100), held) << "a held map survives the sweep";
    const auto again = PendingMaps::For("/sweep/marked", 100);
    EXPECT_EQ(again.get(), marked_raw) << "a map with a pending mark survives the sweep: the same map, marks intact";
    EXPECT_EQ(again->PendingBlockCount(), 1u);
    again->ClearRows(0, 1);
    PendingMaps::ClearForTests();
}

// Below the threshold nothing is swept: a file open stays one hash lookup plus one insert.
TEST(PendingMapsTest, ForDoesNotSweepBelowTheThreshold) {
    PendingMaps::ClearForTests();
    PendingMap *const first = PendingMaps::For("/lazy/first", 100).get();   // released at once
    for (size_t i = 0; i + 2 < PendingMaps::SWEEP_ABOVE; ++i) (void)PendingMaps::For("/lazy/" + std::to_string(i), 100);
    EXPECT_EQ(PendingMaps::SizeForTests(), PendingMaps::SWEEP_ABOVE - 1);
    EXPECT_EQ(PendingMaps::Find("/lazy/first").get(), first) << "kept until the registry passes SWEEP_ABOVE";
    PendingMaps::ClearForTests();
}

TEST(PendingMapTest, RandomRangesMatchABitVectorModel) {
    for (const uint64_t rpb : {1u, 64u, 100u, 128u, 1489u}) {
        std::mt19937_64 rng(rpb * 7919);
        constexpr uint64_t BLOCKS = 12;
        PendingMap map(rpb);
        std::vector<bool> model(BLOCKS * rpb, false);
        for (int op = 0; op < 3000; ++op) {
            const uint64_t first = rng() % (8 * rpb);
            const uint64_t count = 1 + rng() % (3 * rpb);
            const bool mark = (rng() % 5) < 3;   // clears are shorter on average than marks: keep both states busy
            if (mark) {
                map.MarkRows(first, count);
            } else {
                map.ClearRows(first, count);
            }
            for (uint64_t r = first; r < first + count; ++r) model[r] = mark;
            auto block_set = [&](uint64_t b) {
                for (uint64_t r = b * rpb; r < (b + 1) * rpb; ++r) if (model[r]) return true;
                return false;
            };
            size_t pending_blocks = 0;
            for (uint64_t b = 0; b < BLOCKS; ++b) {
                const bool set = block_set(b);
                if (set) {
                    ++pending_blocks;
                    ASSERT_TRUE(map.IsPending(b)) << "rpb " << rpb << " op " << op << " block " << b << ": a pending block read clear";
                }
                std::vector<IndexRun> expect;
                for (uint64_t r = b * rpb; r < (b + 1) * rpb; ++r) {
                    if (!model[r]) continue;
                    if (!expect.empty() && expect.back().first_row + expect.back().row_count == r) {
                        ++expect.back().row_count;
                    } else {
                        expect.push_back({r, 1});
                    }
                }
                const std::vector<IndexRun> got = map.PendingRunsOfBlock(b);
                ASSERT_EQ(got.size(), expect.size()) << "rpb " << rpb << " op " << op << " block " << b;
                for (size_t i = 0; i < got.size(); ++i) {
                    ASSERT_EQ(got[i].first_row, expect[i].first_row) << "rpb " << rpb << " op " << op << " block " << b;
                    ASSERT_EQ(got[i].row_count, expect[i].row_count) << "rpb " << rpb << " op " << op << " block " << b;
                }
            }
            ASSERT_EQ(map.PendingBlockCount(), pending_blocks) << "rpb " << rpb << " op " << op;
            const uint64_t w_first = rng() % (BLOCKS * rpb);
            const uint64_t w_last = w_first + rng() % (BLOCKS * rpb - w_first);
            bool any = false;
            for (uint64_t b = w_first / rpb; b <= w_last / rpb && !any; ++b) any = block_set(b);
            ASSERT_EQ(map.AnyPendingInRows(w_first, w_last), any) << "rpb " << rpb << " op " << op;
        }
    }
}

TEST(PendingMapPerfTest, NsPerMarkAndClearOfARun) {
    if (std::getenv("XSTORE_PENDING_PERF") == nullptr) GTEST_SKIP() << "set XSTORE_PENDING_PERF=1 to run";
    for (const uint64_t rows : {uint64_t{1000}, uint64_t{1}}) {
        PendingMap map(1489);
        constexpr int REPS = 100000;
        const auto start = std::chrono::steady_clock::now();
        for (int i = 0; i < REPS; ++i) {
            const uint64_t first = static_cast<uint64_t>(i % 1000) * 1489 + 17;   // the same blocks again and again
            map.MarkRows(first, rows);
            map.ClearRows(first, rows);
        }
        const double ns = std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now() - start).count();
        std::printf("PENDING_PERF %llu-row run: %.1f ns per MarkRows+ClearRows\n", static_cast<unsigned long long>(rows), ns / REPS);
    }
}

TEST(PendingMapTest, PendingRunsOfABlockAfterTwoMarksAreTheTwoRunsAndStayInsideTheirBlock) {
    PendingMap map(100);
    map.MarkRows(5, 3);
    map.MarkRows(10, 2);
    map.MarkRows(198, 4);   // 198..201 crosses a block boundary: 198-199 of block 1, 200-201 of block 2
    const std::vector<IndexRun> runs = map.PendingRunsOfBlock(0);
    ASSERT_EQ(runs.size(), 2u);
    EXPECT_EQ(runs[0].first_row, 5u);  EXPECT_EQ(runs[0].row_count, 3u);
    EXPECT_EQ(runs[1].first_row, 10u); EXPECT_EQ(runs[1].row_count, 2u);
    EXPECT_TRUE(map.PendingRunsOfBlock(7).empty());
    const std::vector<IndexRun> one = map.PendingRunsOfBlock(1);
    ASSERT_EQ(one.size(), 1u);
    EXPECT_EQ(one[0].first_row, 198u); EXPECT_EQ(one[0].row_count, 2u) << "the run is cut at the block's end";
    const std::vector<IndexRun> two = map.PendingRunsOfBlock(2);
    ASSERT_EQ(two.size(), 1u);
    EXPECT_EQ(two[0].first_row, 200u); EXPECT_EQ(two[0].row_count, 2u);
}

TEST(PendingMapTest, AllPendingRunsAreAscendingAndOneRunNeverSpansBlocks) {
    PendingMap map(10);
    for (uint64_t b : {9u, 2u, 5u, 0u, 7u}) map.MarkRows(b * 10 + 8, 2);   // the tail of each block
    map.MarkRows(30, 3);
    const std::vector<IndexRun> runs = map.AllPendingRuns();
    const std::vector<uint64_t> firsts = {8, 28, 30, 58, 78, 98};
    ASSERT_EQ(runs.size(), firsts.size());
    for (size_t i = 0; i < runs.size(); ++i) {
        EXPECT_EQ(runs[i].first_row, firsts[i]);
        EXPECT_EQ(runs[i].row_count, i == 2 ? 3u : 2u);
    }
    EXPECT_TRUE(PendingMap(10).AllPendingRuns().empty());
}

TEST(PendingMapTest, ClearRunsClearsExactlyTheFoldedRowsAndBalancesTheGlobalCount) {
    PendingMaps::ClearForTests();
    ASSERT_FALSE(PendingMaps::AnyPending());
    {
        PendingMap map(100);
        map.MarkRows(5, 3);    // 5..7
        map.MarkRows(10, 2);   // 10..11
        map.MarkRows(150, 1);  // block 1
        ASSERT_TRUE(PendingMaps::AnyPending());
        map.ClearRuns({{5, 3}});
        const std::vector<IndexRun> left = map.PendingRunsOfBlock(0);
        ASSERT_EQ(left.size(), 1u);
        EXPECT_EQ(left[0].first_row, 10u); EXPECT_EQ(left[0].row_count, 2u);
        EXPECT_EQ(map.PendingBlockCount(), 2u) << "block 0 still holds rows 10-11";
        EXPECT_TRUE(map.IsPending(0));
        map.ClearRuns({{10, 1}});
        EXPECT_EQ(map.PendingBlockCount(), 2u) << "row 11 is still pending";
        map.ClearRuns({{11, 1}, {150, 1}});
        EXPECT_EQ(map.PendingBlockCount(), 0u);
        EXPECT_FALSE(map.IsPending(0));
        EXPECT_FALSE(PendingMaps::AnyPending()) << "the process-wide count balances after the last block clears";
        map.ClearRuns({});
        map.ClearRuns({{5, 3}});   // rows that are not pending: nothing changes
        EXPECT_EQ(map.PendingBlockCount(), 0u);
    }
    EXPECT_FALSE(PendingMaps::AnyPending());
}

// "In range" is per block (a query that touches a pending block scans it): any row of a block that intersects the range.
TEST(PendingMapTest, AnyPendingInRowsMatchesASetModelAtBlockGranularity) {
    for (const uint64_t rpb : {7u, 2048u}) {
        std::mt19937_64 rng(rpb * 104729);
        PendingMap map(rpb);
        std::set<uint64_t> model;
        const uint64_t span = 40 * rpb;
        for (int op = 0; op < 300; ++op) {
            const uint64_t first = rng() % span;
            const uint64_t count = 1 + rng() % (2 * rpb);
            const uint64_t kind = rng() % 3;
            if (kind == 0) {
                map.MarkRows(first, count);
                for (uint64_t r = first; r < first + count; ++r) model.insert(r);
            } else if (kind == 1) {
                map.ClearRows(first, count);
                for (uint64_t r = first; r < first + count; ++r) model.erase(r);
            }
            // Short ranges take the per-block walk, whole-file ranges the scan of the pending blocks.
            const uint64_t q_first = rng() % span;
            const uint64_t q_last = std::min(span - 1, q_first + (rng() % 2 == 0 ? rng() % (2 * rpb) : rng() % span));
            bool expect = false;
            for (const uint64_t row : model) {
                if (row / rpb >= q_first / rpb && row / rpb <= q_last / rpb) { expect = true; break; }
            }
            ASSERT_EQ(map.AnyPendingInRows(q_first, q_last), expect) << "rpb " << rpb << " op " << op << " [" << q_first << "," << q_last << "]";
        }
    }
}

TEST(PendingMapTest, AnyPendingInRowsOfAnInvertedRangeIsFalse) {
    PendingMap map(10);
    map.MarkRows(100, 1);
    EXPECT_FALSE(map.AnyPendingInRows(500, 10));
    EXPECT_FALSE(map.AnyPendingInRows(101, 100));
}

#if defined(__SANITIZE_THREAD__)
#define XSTORE_TEST_NO_PERF_ASSERT 1
#endif

// The query cost follows the blocks of the range or the pending blocks, whichever is fewer.
TEST(PendingMapTest, AnyPendingInRowsIsCheapForAHugeRangeWithOnePendingBlock) {
#ifdef XSTORE_TEST_NO_PERF_ASSERT
    GTEST_SKIP() << "timing assertion skipped under TSan";
#else
    constexpr uint64_t BLOCKS = 1'000'000;
    PendingMap map(64);
    map.MarkRows((BLOCKS - 1) * 64 + 3, 1);
    const auto start = std::chrono::steady_clock::now();
    bool got = false;
    for (int i = 0; i < 100; ++i) got = map.AnyPendingInRows(0, BLOCKS * 64 - 1);
    const double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count() / 100;
    EXPECT_TRUE(got);
    EXPECT_LT(us, 1000.0) << "one pending block must not cost a walk over " << BLOCKS << " block numbers";
#endif
}

TEST(PendingMapTest, AnyPendingInRowsIsCheapForOneBlockAmongManyPendingBlocks) {
#ifdef XSTORE_TEST_NO_PERF_ASSERT
    GTEST_SKIP() << "timing assertion skipped under TSan";
#else
    constexpr uint64_t BLOCKS = 400'000;
    PendingMap map(64);
    for (uint64_t b = 0; b < BLOCKS; b += 2) map.MarkRows(b * 64, 1);   // even blocks pending
    const auto start = std::chrono::steady_clock::now();
    bool odd = true, even = false;
    for (int i = 0; i < 1000; ++i) {
        odd = map.AnyPendingInRows(3 * 64, 4 * 64 - 1);       // block 3: clear
        even = map.AnyPendingInRows(4 * 64, 4 * 64 + 1);      // block 4: pending
    }
    const double us = std::chrono::duration<double, std::micro>(std::chrono::steady_clock::now() - start).count() / 2000;
    EXPECT_FALSE(odd);
    EXPECT_TRUE(even);
    EXPECT_LT(us, 100.0) << "a one-block range must not scan the " << BLOCKS / 2 << " pending blocks";
#endif
}

TEST(PendingMapsTest, RehomeRegroupsTheMarksIntoTheNewBlockSizeAndRegistersTheNewMap) {
    PendingMaps::ClearForTests();
    const auto old_map = PendingMaps::For("/rehome/a", 7);
    std::vector<uint64_t> rows = {3, 8, 1000};
    for (const uint64_t r : rows) old_map->MarkRows(r, 1);
    old_map->MarkRows(13, 3);    // 13-15 crosses the old block boundary at 14
    old_map->MarkRows(500, 3);
    for (const uint64_t r : {13u, 14u, 15u, 500u, 501u, 502u}) rows.push_back(r);
    ASSERT_EQ(old_map->PendingBlockCount(), 5u) << "blocks 0, 1, 2, 71, 142 at 7 rows per block";
    const auto fresh = PendingMaps::Rehome("/rehome/a", 2048);
    ASSERT_NE(fresh, nullptr);
    EXPECT_NE(fresh, old_map);
    EXPECT_EQ(PendingMaps::Find("/rehome/a"), fresh);
    EXPECT_EQ(fresh->RowsPerBlock(), 2048u);
    EXPECT_EQ(fresh->PendingBlockCount(), 1u) << "every marked row is in block 0 of 2048";
    for (uint64_t r = 0; r < 4096; ++r) {
        const bool expect = std::find(rows.begin(), rows.end(), r) != rows.end();
        const std::vector<IndexRun> runs = fresh->PendingRunsOfBlock(r / 2048);
        bool set = false;
        for (const IndexRun &run : runs) set = set || (r >= run.first_row && r < run.first_row + run.row_count);
        ASSERT_EQ(set, expect) << "row " << r;
    }
    EXPECT_TRUE(fresh->IsPending(0));
    EXPECT_EQ(old_map->PendingBlockCount(), 0u) << "the marks moved: the old map is left empty";
    EXPECT_EQ(PendingMaps::Rehome("/rehome/a", 2048), fresh) << "same block size: nothing to move";
    fresh->ClearRows(0, 4096);
    PendingMaps::ClearForTests();
}

TEST(PendingMapsTest, RehomeOfAMapWithNoMarksOrNoEntryIsAPlainReplaceOrCreate) {
    PendingMaps::ClearForTests();
    const auto old_map = PendingMaps::For("/rehome/b", 7);
    const auto fresh = PendingMaps::Rehome("/rehome/b", 100);
    EXPECT_EQ(fresh->RowsPerBlock(), 100u);
    EXPECT_EQ(PendingMaps::Find("/rehome/b"), fresh);
    EXPECT_EQ(fresh->PendingBlockCount(), 0u);
    const auto created = PendingMaps::Rehome("/rehome/none", 64);
    EXPECT_EQ(PendingMaps::Find("/rehome/none"), created);
    EXPECT_EQ(created->RowsPerBlock(), 64u);
    (void)old_map;
    PendingMaps::ClearForTests();
}

// For() on a block-size mismatch: the pending marks are writers' in-flight marks that only the holders of THIS map clear,
// so a map with pending blocks is returned unchanged (no copy that would leak, no replacement that would lose them).
TEST(PendingMapsTest, ForOnABlockSizeMismatchReturnsTheSameMapWhileRowsArePending) {
    PendingMaps::ClearForTests();
    ASSERT_FALSE(PendingMaps::AnyPending());
    const auto held = PendingMaps::For("/mismatch/a", 10);
    held->MarkRows(25, 2);
    ASSERT_TRUE(PendingMaps::AnyPending());
    const auto again = PendingMaps::For("/mismatch/a", 20);
    EXPECT_EQ(again, held) << "same map, not a copy";
    EXPECT_EQ(again->RowsPerBlock(), 10u);
    EXPECT_EQ(held->PendingBlockCount(), 1u);
    EXPECT_EQ(PendingMaps::Find("/mismatch/a"), held);
    held->MarkRows(40, 1);   // a writer marking after the mismatch lands in the one map every file sees
    EXPECT_EQ(PendingMaps::For("/mismatch/a", 20)->PendingBlockCount(), 2u);
    held->ClearRows(25, 2);
    EXPECT_TRUE(PendingMaps::AnyPending());
    held->ClearRows(40, 1);
    EXPECT_FALSE(PendingMaps::AnyPending()) << "the process-wide count balances: nothing leaked";
    // With nothing pending the map is simply replaced.
    const auto empty_old = PendingMaps::For("/mismatch/b", 10);
    const auto empty_new = PendingMaps::For("/mismatch/b", 20);
    EXPECT_NE(empty_new, empty_old);
    EXPECT_EQ(empty_new->RowsPerBlock(), 20u);
    EXPECT_EQ(PendingMaps::Find("/mismatch/b"), empty_new);
    PendingMaps::ClearForTests();
}

// A sweep is O(registry): past SWEEP_ABOVE it runs again only after the registry has doubled.
TEST(PendingMapsTest, ForSweepsOnlyWhenTheRegistryHasDoubledSinceTheLastSweep) {
    PendingMaps::ClearForTests();
    const size_t before = PendingMaps::SweepCountForTests();
    std::vector<std::shared_ptr<PendingMap>> held;   // every map is held: nothing can be swept, the registry only grows
    const size_t total = 2 * PendingMaps::SWEEP_ABOVE + 40;
    for (size_t i = 0; i < total; ++i) held.push_back(PendingMaps::For("/doubling/" + std::to_string(i), 100));
    const size_t sweeps = PendingMaps::SweepCountForTests() - before;
    EXPECT_GE(sweeps, 1u);
    EXPECT_LE(sweeps, 2u) << "registry sizes " << PendingMaps::SWEEP_ABOVE + 1 << " and twice that, not every creation";
    EXPECT_EQ(PendingMaps::SizeForTests(), total);
    held.clear();
    PendingMaps::ClearForTests();
}

TEST(PendingQueryTest, ZoneMapStatsAreUntrustedWhileTheBlockIsPending) {
    xstore_test::ScratchDir scratch("pending_zmap");
    constexpr uint16_t ROW_LENGTH = 512;
    ZoneMapFile zmap;
    ASSERT_TRUE(zmap.Open((scratch.test_root / "a.zmap").string(), ROW_LENGTH, 4));
    auto pending = std::make_shared<PendingMap>(zmap.RowsPerBlockValue());
    zmap.AttachPending(pending);
    zmap.UpdateRow(0, {{0, 5.0, true}}, 4);
    const auto before = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(before.has_value());
    EXPECT_EQ((*before)[0].valid_count, 1u);
    pending->MarkRows(0, 1);
    EXPECT_FALSE(zmap.TryGetBlockStats(0).has_value()) << "a pending block must be scanned";
    pending->ClearRows(0, 1);
    EXPECT_TRUE(zmap.TryGetBlockStats(0).has_value());
}

TEST(PendingQueryTest, BloomSaysMaybeWhileTheBlockIsPending) {
    xstore_test::ScratchDir scratch("pending_catidx");
    constexpr uint16_t ROW_LENGTH = 512;
    CatIdxFile catidx;
    ASSERT_TRUE(catidx.Open((scratch.test_root / "a.catidx").string(), ROW_LENGTH, 4));
    auto pending = std::make_shared<PendingMap>(catidx.RowsPerBlockValue());
    catidx.AttachPending(pending);
    const std::string absent = "never-inserted-value";
    EXPECT_FALSE(catidx.MightContain(0, 0, absent)) << "an empty block's bloom says no";
    pending->MarkRows(0, 1);
    EXPECT_TRUE(catidx.MightContain(0, 0, absent)) << "pending: the answer must be 'maybe' so the query scans";
    pending->ClearRows(0, 1);
    EXPECT_FALSE(catidx.MightContain(0, 0, absent));
}

TEST(PendingRegistryTest, RegistryOpenedIndexesShareOnePendingMapPerYear) {
    xstore_test::ScratchDir scratch("pending_registry");
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    const std::string base = (scratch.test_root / "DataStore" / "PENDREG").string();
    std::filesystem::create_directories(base);
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(base, 1971, 512, 64);
        const CatIdxFile::Pin catidx = XStore::PinCatIdx(base, 1971, 512, 64);
        ASSERT_TRUE(zmap->Pending() != nullptr);
        EXPECT_EQ(zmap->Pending(), catidx->Pending()) << "one map per (db, year), shared by both indexes";
        EXPECT_EQ(zmap->Pending(), PendingMaps::Find(base + "/1971"));
    }
    XStore::ClearZoneMapRegistryForTests();
    EXPECT_EQ(PendingMaps::Find(base + "/1971"), nullptr) << "the zone-map test-clear helper also clears the pending maps";

    {
        const CatIdxFile::Pin catidx = XStore::PinCatIdx(base, 1972, 512, 64);
        ASSERT_TRUE(catidx->Pending() != nullptr);
        EXPECT_EQ(catidx->Pending(), PendingMaps::Find(base + "/1972"));
    }
    XStore::ClearCatIdxRegistryForTests();
    EXPECT_EQ(PendingMaps::Find(base + "/1972"), nullptr) << "the catidx test-clear helper also clears the pending maps";
}

TEST(PendingRegistryTest, RemoveDbDropsThePendingMapsOfEveryYear) {
    xstore_test::ScratchDir scratch("pending_removedb");
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    XStore store;
    xstore_test::CreateTestDb(store, "PENDRMDB", XStore_Proto::SECOND, 128);
    const std::string base = (scratch.test_root / "DataStore" / "PENDRMDB").string();
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1482271200ULL, xstore_test::MakeRow({xstore_test::FString("a")}), "PENDRMDB", &rep);   // 2016
    store.UnaryInsert(1483229800ULL, xstore_test::MakeRow({xstore_test::FString("b")}), "PENDRMDB", &rep);   // 2017
    for (const unsigned short year : {2016, 2017}) {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(base, year, 128);
        const CatIdxFile::Pin catidx = XStore::PinCatIdx(base, year, 128);
        ASSERT_TRUE(PendingMaps::Find(base + "/" + std::to_string(year)) != nullptr) << year;
    }
    (void) store.RemoveDB("PENDRMDB");
    EXPECT_EQ(PendingMaps::Find(base + "/2016"), nullptr);
    EXPECT_EQ(PendingMaps::Find(base + "/2017"), nullptr);
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
}

TEST(PendingRegistryTest, RemoveDbFilesOfOneYearDropsOnlyThatYearsPendingMap) {
    xstore_test::ScratchDir scratch("pending_removeyear");
    for (auto &shard : XStore::cfg_shards) shard.map.clear();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    XStore store;
    xstore_test::CreateTestDb(store, "PENDRMYR", XStore_Proto::SECOND, 128);
    const std::string base = (scratch.test_root / "DataStore" / "PENDRMYR").string();
    XStore_Proto::UnaryInsert_REP rep;
    store.UnaryInsert(1482271200ULL, xstore_test::MakeRow({xstore_test::FString("a")}), "PENDRMYR", &rep);   // 2016
    store.UnaryInsert(1483229800ULL, xstore_test::MakeRow({xstore_test::FString("b")}), "PENDRMYR", &rep);   // 2017
    for (const unsigned short year : {2016, 2017}) {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(base, year, 128);
        const CatIdxFile::Pin catidx = XStore::PinCatIdx(base, year, 128);
        ASSERT_TRUE(PendingMaps::Find(base + "/" + std::to_string(year)) != nullptr) << year;
    }
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("2016.XSTORE");
    (void) store.RemoveDBFiles(&req, "PENDRMYR");
    EXPECT_EQ(PendingMaps::Find(base + "/2016"), nullptr);
    EXPECT_TRUE(PendingMaps::Find(base + "/2017") != nullptr) << "the other year keeps its map";
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
}

// Item D: a live widen swaps the sidecar files' tracked width, not their block size, and keeps the object (and with it
// the attached map) in place; the marks and the sharing survive.
TEST(PendingRegistryTest, MarksSurviveALiveWidenAndBothFilesStillShareTheMap) {
    xstore_test::ScratchDir scratch("pending_widen");
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    const std::string base = (scratch.test_root / "DataStore" / "PENDWIDEN").string();
    std::filesystem::create_directories(base);
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(base, 1971, 512, 64);
        const CatIdxFile::Pin catidx = XStore::PinCatIdx(base, 1971, 512, 64);
        const std::shared_ptr<PendingMap> map = PendingMaps::Find(base + "/1971");
        ASSERT_NE(map, nullptr);
        ASSERT_EQ(zmap->Pending(), map);
        ASSERT_EQ(catidx->Pending(), map);
        const uint64_t rpb = map->RowsPerBlock();
        ASSERT_EQ(rpb, zmap->RowsPerBlockValue());
        ASSERT_EQ(rpb, catidx->RowsPerBlockValue());
        map->MarkRows(5, 3);
        map->MarkRows(2 * rpb + 1, 2);
        ASSERT_EQ(zmap->BeginLiveRebuild(80), BeginRebuildResult::kStarted);
        ASSERT_EQ(catidx->BeginLiveRebuild(80), BeginRebuildResult::kStarted);
        EXPECT_EQ(zmap->RowsPerBlockValue(), rpb);
        EXPECT_EQ(catidx->RowsPerBlockValue(), rpb);
        EXPECT_EQ(zmap->Pending(), map);
        EXPECT_EQ(catidx->Pending(), zmap->Pending()) << "both files of the year still see one map";
        EXPECT_EQ(PendingMaps::Find(base + "/1971"), map);
        EXPECT_EQ(map->PendingBlockCount(), 2u);
        ASSERT_EQ(map->PendingRunsOfBlock(0).size(), 1u);
        EXPECT_EQ(map->PendingRunsOfBlock(0)[0].row_count, 3u);
        map->ClearRows(0, 3 * rpb);
    }
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    PendingMaps::ClearForTests();
}

}  // namespace
