// Live index widening, M4 stage 2b (Tech Reference 4.18 "Live widening"): XStore::LiveWidenOneYear rebuilds a year's
// .zmap and .catidx in place while requests run, chunk by chunk under the data file's READ range lock (timed:
// FileRangeLock::TryLockRangeFor). Nothing in production calls it yet (stage 3 adds the trigger), so these tests drive
// it directly. References: the startup rebuild (InitDB -> RebuildYearIndexes) of a sparse copy of the same year file,
// and full scans of the data file taken under a READ lock.
#include <gtest/gtest.h>
#include "XStore/XStore_ZoneMap.h"
#include "XStore/XStore_CatIdx.h"
#include "XStore/XStore_RowScan.h"
#include "XStore/XStore_WireEncode.h"
#include "XStore/XStore_WalRing.h"
#include "XStore/XStore_Tunables.h"
#include "XStore/XStore_LiveWidenGovernor.h"
#include "XStore/XStore_LiveWidenLoadSource.h"
#include "include/DataStructs/FileRangeLock.h"
#include "XStoreTestSupport.h"
#include "XStoreSyscallFault.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <filesystem>
#include <iterator>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <vector>

namespace {
using namespace xstore_test;
using Result = XStore::LiveWidenResult;
using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;
constexpr auto kRead = FileRangeLock::LockType::READ;
constexpr auto kWrite = FileRangeLock::LockType::WRITE;

// ---- FileRangeLock::TryLockRangeFor ---------------------------------------------------------------------------------

int64_t MsSince(Clock::time_point t0) {
    return std::chrono::duration_cast<milliseconds>(Clock::now() - t0).count();
}

TEST(FileRangeLockTimedTest, SucceedsAtOnceWhenTheRangeIsFree) {
    FileRangeLock lock;
    const Clock::time_point t0 = Clock::now();
    ASSERT_TRUE(lock.TryLockRangeFor(0, 100, kWrite, milliseconds(2000)));
    EXPECT_LT(MsSince(t0), 500);
    lock.UnlockRange(0, 100, kWrite);
}

// The waiter runs on its own thread with a watchdog: a TryLockRangeFor that ignores its timeout must fail the test,
// not hang it.
TEST(FileRangeLockTimedTest, TimesOutWhileAConflictingWriteIsHeld) {
    FileRangeLock lock;
    lock.LockRange(50, 10, kWrite);
    auto waiter = std::async(std::launch::async, [&] {
        const Clock::time_point t0 = Clock::now();
        const bool got = lock.TryLockRangeFor(0, 100, kRead, milliseconds(100));
        return std::make_pair(got, MsSince(t0));
    });
    if (waiter.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        ADD_FAILURE() << "TryLockRangeFor ignored its timeout";
        lock.UnlockRange(50, 10, kWrite);
        if (waiter.get().first) lock.UnlockRange(0, 100, kRead);
        return;
    }
    const auto [got, elapsed_ms] = waiter.get();
    EXPECT_FALSE(got);
    EXPECT_GE(elapsed_ms, 90);
    lock.UnlockRange(50, 10, kWrite);
}

TEST(FileRangeLockTimedTest, SucceedsWhenTheConflictIsReleasedWithinTheTimeout) {
    FileRangeLock lock;
    lock.LockRange(0, 10, kWrite);
    std::thread releaser([&] {
        std::this_thread::sleep_for(milliseconds(50));
        lock.UnlockRange(0, 10, kWrite);
    });
    const Clock::time_point t0 = Clock::now();
    EXPECT_TRUE(lock.TryLockRangeFor(5, 10, kWrite, milliseconds(3000)));
    const int64_t elapsed_ms = MsSince(t0);
    releaser.join();
    EXPECT_GE(elapsed_ms, 40);
    EXPECT_LT(elapsed_ms, 2500);
    lock.UnlockRange(5, 10, kWrite);
}

TEST(FileRangeLockTimedTest, ReadsShareAndDisjointRangesDoNotBlock) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    EXPECT_TRUE(lock.TryLockRangeFor(50, 100, kRead, milliseconds(0)));
    EXPECT_TRUE(lock.TryLockRangeFor(150, 10, kWrite, milliseconds(0)));
    EXPECT_FALSE(lock.TryLockRangeFor(149, 1, kWrite, milliseconds(0))) << "overlaps the second READ";
    lock.UnlockRange(0, 100, kRead);
    lock.UnlockRange(50, 100, kRead);
    lock.UnlockRange(150, 10, kWrite);
}

// A timed-out attempt registers nothing: once the conflict is gone, a WRITE over the whole file ("rest of file",
// length 0) is granted at once. Same for the RAII guard.
TEST(FileRangeLockTimedTest, ATimeoutLeavesNoRegistrationBehind) {
    FileRangeLock lock;
    lock.LockRange(10, 10, kWrite);
    auto waiter = std::async(std::launch::async, [&] {
        return std::make_pair(lock.TryLockRangeFor(0, 100, kRead, milliseconds(20)), lock.TryLockRangeFor(0, 0, kRead, milliseconds(0)));
    });
    if (waiter.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        ADD_FAILURE() << "TryLockRangeFor ignored its timeout";
        lock.UnlockRange(10, 10, kWrite);
        const auto got = waiter.get();
        if (got.first) lock.UnlockRange(0, 100, kRead);
        if (got.second) lock.UnlockRange(0, 0, kRead);
        return;
    }
    const auto [ranged, rest_of_file] = waiter.get();
    EXPECT_FALSE(ranged);
    EXPECT_FALSE(rest_of_file);
    lock.UnlockRange(10, 10, kWrite);
    ASSERT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0)));
    lock.UnlockRange(0, 0, kWrite);

    const std::string key = "/live_widen_test/guard_key";
    FileLockRegistry::Get(key).LockRange(0, 10, kWrite);
    {
        TimedFileRangeLockGuard guard(key, 5, 10, kRead, milliseconds(10));
        EXPECT_FALSE(guard.owns_lock());
    }
    FileLockRegistry::Get(key).UnlockRange(0, 10, kWrite);
    {
        TimedFileRangeLockGuard guard(key, 5, 10, kRead, milliseconds(0));
        EXPECT_TRUE(guard.owns_lock());
    }
    TimedFileRangeLockGuard whole(key, 0, 0, kWrite, milliseconds(0));
    EXPECT_TRUE(whole.owns_lock()) << "a guard leaked its range";
}

// ---- FileRangeLock: the builder's READ yields to waiting writers (S3b-1) ---------------------------------------------

using WriterYield = FileRangeLock::WriterYield;

// Polls (5 s bound) until `n` writers are registered as waiting.
void WaitForWaitingWriters(FileRangeLock &lock, uint32_t n) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(5);
    while (lock.WaitingWritersForTests() != n) {
        ASSERT_LT(Clock::now(), deadline) << "waiting writers never reached " << n;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

// A WRITE on [start, start + len) taken (blocking) on its own thread, held for hold_ms, then released.
std::thread WriterThread(FileRangeLock &lock, unsigned long long start, unsigned long long len, int hold_ms = 5) {
    return std::thread([&lock, start, len, hold_ms] {
        lock.LockRange(start, len, kWrite);
        std::this_thread::sleep_for(milliseconds(hold_ms));
        lock.UnlockRange(start, len, kWrite);
    });
}

TEST(FileRangeLockYieldTest, AWaitingOverlappingWriteMakesTheYieldingReadWaitUntilTheWriterIsDone) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    std::thread writer = WriterThread(lock, 0, 100, 30);
    WaitForWaitingWriters(lock, 1);
    WriterYield yield;
    auto reader = std::async(std::launch::async, [&] { return lock.TryReadLockYieldingFor(50, 10, milliseconds(50), milliseconds(5000), yield); });
    EXPECT_EQ(reader.wait_for(milliseconds(200)), std::future_status::timeout)
        << "a READ the held READ does not block was granted past a waiting writer";
    lock.UnlockRange(0, 100, kRead); // the writer gets in, holds 30 ms, leaves; then the reader
    writer.join();
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(reader.get());
    EXPECT_TRUE(yield.yielded);
    EXPECT_GE(yield.us, 150000u);
    EXPECT_LT(yield.us, 2'000'000u) << "released only by the 5 s cap: the unregister of a waiting writer lost a wakeup";
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u) << "the writer's entry stayed behind after it acquired";
    lock.UnlockRange(50, 10, kRead);
    EXPECT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0))) << "a range leaked";
    lock.UnlockRange(0, 0, kWrite);
}

TEST(FileRangeLockYieldTest, ANonOverlappingWaitingWriteDoesNotDelayTheYieldingRead) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    std::thread writer = WriterThread(lock, 0, 100);
    WaitForWaitingWriters(lock, 1);
    WriterYield yield;
    EXPECT_TRUE(lock.TryReadLockYieldingFor(100, 50, milliseconds(0), milliseconds(5000), yield));
    EXPECT_FALSE(yield.yielded);
    lock.UnlockRange(100, 50, kRead);
    lock.UnlockRange(0, 100, kRead);
    writer.join();
}

// Only the builder's call yields: LockRange(READ) and TryLockRangeFor(READ) are granted past a waiting writer as before.
TEST(FileRangeLockYieldTest, NormalReadsIgnoreWaitingWriters) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    std::thread writer = WriterThread(lock, 0, 100);
    WaitForWaitingWriters(lock, 1);
    auto reader = std::async(std::launch::async, [&] {
        lock.LockRange(50, 10, kRead);
        return lock.TryLockRangeFor(60, 10, kRead, milliseconds(0));
    });
    const bool ready = reader.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    EXPECT_TRUE(ready) << "LockRange(READ) waited for a waiting writer";
    if (!ready) lock.UnlockRange(0, 100, kRead);
    EXPECT_TRUE(reader.get());
    lock.UnlockRange(50, 10, kRead);
    lock.UnlockRange(60, 10, kRead);
    if (ready) lock.UnlockRange(0, 100, kRead);
    writer.join();
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
}

// Progress: a writer that waits forever (it waits on a READ held throughout) delays the yielding READ by the cap only,
// and the yield does not use up the (shorter) timeout.
TEST(FileRangeLockYieldTest, PastTheCapTheYieldingReadProceedsAsBefore) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    std::thread writer = WriterThread(lock, 0, 0);
    WaitForWaitingWriters(lock, 1);
    WriterYield yield;
    const Clock::time_point t0 = Clock::now();
    auto reader = std::async(std::launch::async, [&] { return lock.TryReadLockYieldingFor(500, 10, milliseconds(50), milliseconds(300), yield); });
    const bool ready = reader.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    const int64_t elapsed_ms = MsSince(t0);
    lock.UnlockRange(0, 100, kRead);
    ASSERT_TRUE(ready) << "the yield ignored its cap";
    ASSERT_TRUE(reader.get()) << "the yield used up the timeout";
    EXPECT_GE(elapsed_ms, 290);
    EXPECT_LT(elapsed_ms, 2500);
    EXPECT_TRUE(yield.yielded);
    EXPECT_GE(yield.us, 290000u);
    // The writer still waits on the yielding reader's own (now held) READ.
    EXPECT_EQ(lock.WaitingWritersForTests(), 1u);
    lock.UnlockRange(500, 10, kRead);
    writer.join();
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
}

TEST(FileRangeLockYieldTest, AHeldConflictStillTimesOutAfterTheYield) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kRead);
    std::thread writer = WriterThread(lock, 0, 100);
    WaitForWaitingWriters(lock, 1);
    lock.LockRange(200, 10, kWrite);
    WriterYield yield;
    EXPECT_FALSE(lock.TryReadLockYieldingFor(50, 200, milliseconds(30), milliseconds(5000), yield));
    EXPECT_FALSE(yield.yielded) << "the held WRITE conflicts first: no yield counted";
    lock.UnlockRange(200, 10, kWrite);
    lock.UnlockRange(0, 100, kRead);
    writer.join();
    EXPECT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0)));
    lock.UnlockRange(0, 0, kWrite);
}

TEST(FileRangeLockYieldTest, SeveralWaitingWritersEachHoldTheReadBack) {
    FileRangeLock lock;
    lock.LockRange(0, 1000, kRead);
    std::vector<std::thread> writers;
    writers.push_back(WriterThread(lock, 0, 100, 20));
    writers.push_back(WriterThread(lock, 0, 100, 20)); // same range twice
    writers.push_back(WriterThread(lock, 500, 100, 20));
    WaitForWaitingWriters(lock, 3);
    WriterYield free_yield;
    EXPECT_TRUE(lock.TryReadLockYieldingFor(200, 100, milliseconds(0), milliseconds(5000), free_yield));
    EXPECT_FALSE(free_yield.yielded);
    lock.UnlockRange(200, 100, kRead);
    WriterYield yield;
    auto reader = std::async(std::launch::async, [&] { return lock.TryReadLockYieldingFor(50, 520, milliseconds(50), milliseconds(5000), yield); });
    EXPECT_EQ(reader.wait_for(milliseconds(100)), std::future_status::timeout);
    lock.UnlockRange(0, 1000, kRead);
    for (std::thread &t : writers) t.join();
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_TRUE(reader.get());
    EXPECT_TRUE(yield.yielded);
    EXPECT_LT(yield.us, 2'000'000u) << "released only by the 5 s cap: a wakeup was lost";
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    lock.UnlockRange(50, 520, kRead);
}

// More waiting writers than slots: the overflow counts as a writer waiting on the whole file (a yielding READ anywhere
// waits), and every entry, slot or overflow, is gone once the writers got in.
TEST(FileRangeLockYieldTest, OverflowingTheSlotsYieldsEverywhereAndLeavesNothingBehind) {
    FileRangeLock lock;
    constexpr int kWriters = 40;
    lock.LockRange(0, 1000, kRead);
    std::vector<std::thread> writers;
    for (int i = 0; i < kWriters; ++i) writers.push_back(WriterThread(lock, static_cast<unsigned long long>(i) * 10, 10, 1));
    WaitForWaitingWriters(lock, kWriters);
    WriterYield yield;
    const Clock::time_point t0 = Clock::now();
    EXPECT_TRUE(lock.TryReadLockYieldingFor(5000, 10, milliseconds(0), milliseconds(50), yield));
    EXPECT_GE(MsSince(t0), 45);
    EXPECT_TRUE(yield.yielded) << "overflow did not count as a waiting writer";
    lock.UnlockRange(5000, 10, kRead);
    lock.UnlockRange(0, 1000, kRead);
    for (std::thread &t : writers) t.join();
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    WriterYield after;
    EXPECT_TRUE(lock.TryReadLockYieldingFor(5000, 10, milliseconds(0), milliseconds(5000), after));
    EXPECT_FALSE(after.yielded);
    lock.UnlockRange(5000, 10, kRead);
}

// Many threads, every lock flavour, small file: no deadlock, no lost wakeup (a watchdog), mutual exclusion holds
// (per-cell occupancy counters), and nothing is left registered.
TEST(FileRangeLockYieldTest, StressNoDeadlockNoLostWakeupExclusionHolds) {
    FileRangeLock lock;
    constexpr unsigned kCells = 16;
    std::array<std::atomic<int>, kCells> readers{};
    std::array<std::atomic<int>, kCells> writers_in{};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> violations{0}, ops{0}, yields{0};
    const auto enter = [&](unsigned lo, unsigned hi, bool write) {
        for (unsigned c = lo; c < hi; ++c) {
            if (write) {
                if (writers_in[c].fetch_add(1) != 0 || readers[c].load() != 0) ++violations;
            } else {
                readers[c].fetch_add(1);
                if (writers_in[c].load() != 0) ++violations;
            }
        }
    };
    const auto leave = [&](unsigned lo, unsigned hi, bool write) {
        for (unsigned c = lo; c < hi; ++c) (write ? writers_in[c] : readers[c]).fetch_sub(1);
    };
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 12; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(t * 7919u + 1);
            while (!stop.load()) {
                const unsigned lo = rng() % kCells;
                const unsigned hi = lo + 1 + rng() % (kCells - lo);
                const unsigned long long off = lo * 100ULL, len = (hi - lo) * 100ULL;
                const unsigned kind = t % 4; // 0,1: WRITE (blocking); 2: READ (blocking); 3: yielding READ
                if (kind < 2) {
                    lock.LockRange(off, len, kWrite);
                    enter(lo, hi, true);
                    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 200));
                    leave(lo, hi, true);
                    lock.UnlockRange(off, len, kWrite);
                } else if (kind == 2) {
                    lock.LockRange(off, len, kRead);
                    enter(lo, hi, false);
                    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 200));
                    leave(lo, hi, false);
                    lock.UnlockRange(off, len, kRead);
                } else {
                    WriterYield y;
                    if (lock.TryReadLockYieldingFor(off, len, milliseconds(rng() % 5), milliseconds(rng() % 20), y)) {
                        enter(lo, hi, false);
                        std::this_thread::sleep_for(std::chrono::microseconds(rng() % 200));
                        leave(lo, hi, false);
                        lock.UnlockRange(off, len, kRead);
                    }
                    if (y.yielded) ++yields;
                }
                ++ops;
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    stop.store(true);
    auto joined = std::async(std::launch::async, [&] { for (std::thread &t : threads) t.join(); });
    ASSERT_EQ(joined.wait_for(std::chrono::seconds(30)), std::future_status::ready) << "deadlock or lost wakeup";
    EXPECT_EQ(violations.load(), 0u);
    EXPECT_GT(yields.load(), 0u) << "no yield happened: the test proves little";
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    EXPECT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0))) << "a range leaked";
    lock.UnlockRange(0, 0, kWrite);
    std::printf("[file range lock stress] %llu ops, %llu yielding attempts\n", static_cast<unsigned long long>(ops.load()),
                static_cast<unsigned long long>(yields.load()));
}

// ---- FileRangeLock::WriterWaits (S3b-2 G1) --------------------------------------------------------------------------

constexpr uint64_t kLongWaitUs = XStoreTunables::LIVE_WIDEN_WRITER_LONG_WAIT_US;

// A WRITE on [0, 100) blocked behind a READ the test holds for hold_ms after the writer registered as waiting. Returns
// the writer's own measurement of its LockRange call, in us.
uint64_t BlockedWriteBehindHeldRead(FileRangeLock &lock, int hold_ms) {
    lock.LockRange(0, 100, kRead);
    uint64_t measured_us = 0;
    std::thread writer([&] {
        const Clock::time_point t0 = Clock::now();
        lock.LockRange(50, 10, kWrite);
        measured_us = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count());
        lock.UnlockRange(50, 10, kWrite);
    });
    WaitForWaitingWriters(lock, 1);
    std::this_thread::sleep_for(milliseconds(hold_ms));
    lock.UnlockRange(0, 100, kRead);
    writer.join();
    return measured_us;
}

TEST(FileRangeLockWriterWaitTest, ABlockedWriteAddsOneLongWait) {
    FileRangeLock lock;
    const int hold_ms = static_cast<int>(kLongWaitUs / 1000) + 25;
    const uint64_t measured_us = BlockedWriteBehindHeldRead(lock, hold_ms);
    const FileRangeLock::WriterWaitStats s = lock.WriterWaits();
    EXPECT_EQ(s.waits, 1u);
    EXPECT_GE(s.total_us, static_cast<uint64_t>(hold_ms) * 1000);
    EXPECT_LE(s.total_us, measured_us) << "the lock counted more than the writer spent in LockRange";
    EXPECT_EQ(s.long_waits, 1u);
}

TEST(FileRangeLockWriterWaitTest, AShortBlockedWriteIsNotALongWait) {
    FileRangeLock lock;
    BlockedWriteBehindHeldRead(lock, 2);
    const FileRangeLock::WriterWaitStats s = lock.WriterWaits();
    EXPECT_EQ(s.waits, 1u);
    EXPECT_GE(s.total_us, 2000u);
    // A loaded machine can stretch a 2 ms wait past the threshold; the classification must follow the measured wait.
    EXPECT_EQ(s.long_waits, s.total_us >= kLongWaitUs ? 1u : 0u);
    if (s.total_us >= kLongWaitUs) std::printf("[writer wait] the 2 ms wait took %llu us\n", static_cast<unsigned long long>(s.total_us));
}

// Uncontended WRITEs, a READ that waits behind a WRITE, timed READ/WRITE attempts and a timed yielding READ add nothing.
TEST(FileRangeLockWriterWaitTest, OnlyBlockedBlockingWritesCount) {
    FileRangeLock lock;
    lock.LockRange(0, 100, kWrite);
    lock.LockRange(200, 10, kWrite); // disjoint: no wait
    lock.UnlockRange(200, 10, kWrite);
    EXPECT_FALSE(lock.TryLockRangeFor(10, 10, kRead, milliseconds(20)));
    EXPECT_FALSE(lock.TryLockRangeFor(10, 10, kWrite, milliseconds(20)));
    WriterYield yield;
    EXPECT_FALSE(lock.TryReadLockYieldingFor(10, 10, milliseconds(20), milliseconds(20), yield));
    std::atomic<bool> got{false};
    std::thread reader([&] {
        lock.LockRange(10, 10, kRead);
        got.store(true);
    });
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_FALSE(got.load()) << "the READ did not wait behind the WRITE";
    lock.UnlockRange(0, 100, kWrite);
    reader.join();
    lock.UnlockRange(10, 10, kRead);
    // A timed WRITE that waits and then gets in is not counted either.
    lock.LockRange(0, 10, kRead);
    std::thread releaser([&] {
        std::this_thread::sleep_for(milliseconds(30));
        lock.UnlockRange(0, 10, kRead);
    });
    EXPECT_TRUE(lock.TryLockRangeFor(0, 10, kWrite, milliseconds(3000)));
    releaser.join();
    lock.UnlockRange(0, 10, kWrite);
    const FileRangeLock::WriterWaitStats s = lock.WriterWaits();
    EXPECT_EQ(s.waits, 0u);
    EXPECT_EQ(s.total_us, 0u);
    EXPECT_EQ(s.long_waits, 0u);
}

// Rounds of kWriters writers that are known to block (a READ over all their ranges is held until every one is
// registered as waiting), while other threads take uncontended WRITEs on their own ranges throughout. The count is
// exact; the total lies between what the test held the READ for and what the writers measured around LockRange.
TEST(FileRangeLockWriterWaitTest, StressCountsExactlyTheBlockedWriters) {
    FileRangeLock lock;
    constexpr unsigned kWriters = 8, kRounds = 40, kFree = 4;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> free_ops{0};
    std::vector<std::thread> free_threads;
    for (unsigned t = 0; t < kFree; ++t) {
        free_threads.emplace_back([&, t] {
            const unsigned long long off = 10000 + t * 10ULL;
            while (!stop.load()) {
                lock.LockRange(off, 10, kWrite);
                lock.UnlockRange(off, 10, kWrite);
                ++free_ops;
            }
        });
    }
    std::mt19937 rng(12345);
    uint64_t measured_sum_us = 0, held_sum_us = 0;
    for (unsigned round = 0; round < kRounds; ++round) {
        lock.LockRange(0, 1000, kRead);
        std::array<uint64_t, kWriters> measured{};
        std::vector<std::thread> writers;
        for (unsigned w = 0; w < kWriters; ++w) {
            const unsigned long long off = rng() % 900, len = 1 + rng() % 100;
            writers.emplace_back([&lock, &measured, w, off, len] {
                const Clock::time_point t0 = Clock::now();
                lock.LockRange(off, len, kWrite);
                measured[w] = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count());
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                lock.UnlockRange(off, len, kWrite);
            });
        }
        WaitForWaitingWriters(lock, kWriters);
        const Clock::time_point held_from = Clock::now();
        std::this_thread::sleep_for(milliseconds(1));
        held_sum_us += kWriters * static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - held_from).count());
        lock.UnlockRange(0, 1000, kRead);
        for (std::thread &t : writers) t.join();
        for (uint64_t m : measured) measured_sum_us += m;
    }
    stop.store(true);
    for (std::thread &t : free_threads) t.join();
    const FileRangeLock::WriterWaitStats s = lock.WriterWaits();
    EXPECT_EQ(s.waits, static_cast<uint64_t>(kWriters) * kRounds);
    EXPECT_GE(s.total_us, held_sum_us);
    EXPECT_LE(s.total_us, measured_sum_us);
    EXPECT_LE(s.long_waits, s.waits);
    EXPECT_GT(free_ops.load(), 0u);
    std::printf("[writer wait stress] waits %llu, total %llu us (held %llu, measured %llu), long %llu, free ops %llu\n",
                static_cast<unsigned long long>(s.waits), static_cast<unsigned long long>(s.total_us),
                static_cast<unsigned long long>(held_sum_us), static_cast<unsigned long long>(measured_sum_us),
                static_cast<unsigned long long>(s.long_waits), static_cast<unsigned long long>(free_ops.load()));
}

// ---- FileRangeLock::LockRuns: the WAL apply's per-run WRITE locks ----------------------------------------------------

using LockRun = FileRangeLock::Run;

// A run set taken (blocking) on its own thread; `acquired` set once held; released when `release` is set.
std::thread RunsThread(FileRangeLock &lock, std::vector<LockRun> runs, std::atomic<bool> &acquired, std::atomic<bool> &release,
                       FileRangeLock::LockType type = kWrite) {
    return std::thread([&lock, runs = std::move(runs), &acquired, &release, type] {
        lock.LockRuns(runs.data(), runs.size(), type);
        acquired.store(true);
        while (!release.load()) std::this_thread::sleep_for(std::chrono::microseconds(200));
        lock.UnlockRuns(runs.data(), runs.size(), type);
    });
}

// After a failed wait the set (or writer) is stuck in its lock call: joining it would hang the suite and returning
// would destroy a joinable thread. End the process with the failure printed instead.
[[noreturn]] void AbortAfterAFailedWait() {
    std::fprintf(stderr, "a lock wait failed; aborting instead of joining a blocked thread\n");
    std::fflush(nullptr);
    std::abort();
}

void AbortIfAWaitFailed() {
    if (::testing::Test::HasFatalFailure()) AbortAfterAFailedWait();
}

void WaitForFlag(const std::atomic<bool> &flag, const char *what) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(5);
    while (!flag.load()) {
        ASSERT_LT(Clock::now(), deadline) << what;
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
}

TEST(FileRangeLockRunsTest, TwoRunSetsInEachOthersGapsDoNotBlock) {
    FileRangeLock lock;
    const std::vector<LockRun> a = {{0, 10}, {100, 10}, {200, 10}};
    const std::vector<LockRun> b = {{10, 90}, {150, 10}, {210, 1000}};
    lock.LockRuns(a.data(), a.size());
    EXPECT_EQ(lock.ActiveLocksForTests(), 3u) << "one entry per run";
    auto other = std::async(std::launch::async, [&] { lock.LockRuns(b.data(), b.size()); });
    ASSERT_EQ(other.wait_for(std::chrono::seconds(5)), std::future_status::ready) << "a set waited for one in its gaps";
    EXPECT_EQ(lock.ActiveLocksForTests(), 6u);
    EXPECT_EQ(lock.WriterWaits().waits, 0u);
    lock.UnlockRuns(b.data(), b.size());
    lock.UnlockRuns(a.data(), a.size());
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

TEST(FileRangeLockRunsTest, AHeldSetBlocksWritesAndReadsOnItsRunsButNotReadsInItsGaps) {
    FileRangeLock lock;
    const std::vector<LockRun> runs = {{0, 10}, {100, 10}, {200, 10}};
    lock.LockRuns(runs.data(), runs.size());
    for (const LockRun &r : runs) {
        SCOPED_TRACE(r.start);
        EXPECT_FALSE(lock.TryLockRangeFor(r.start, r.length, kWrite, milliseconds(0)));
        EXPECT_FALSE(lock.TryLockRangeFor(r.start + 5, 1, kRead, milliseconds(0)));
    }
    EXPECT_FALSE(lock.TryLockRangeFor(50, 100, kRead, milliseconds(0))) << "a READ straddling a run";
    EXPECT_FALSE(lock.TryLockRangeFor(0, 0, kRead, milliseconds(0))) << "rest of file covers every run";
    ASSERT_TRUE(lock.TryLockRangeFor(10, 90, kRead, milliseconds(0))) << "a READ in a gap";
    ASSERT_TRUE(lock.TryLockRangeFor(110, 90, kWrite, milliseconds(0))) << "a WRITE in a gap";
    lock.UnlockRange(10, 90, kRead);
    lock.UnlockRange(110, 90, kWrite);
    lock.UnlockRuns(runs.data(), runs.size());
    ASSERT_TRUE(lock.TryLockRangeFor(0, 0, kWrite, milliseconds(0))) << "a run leaked";
    lock.UnlockRange(0, 0, kWrite);
}

// All-or-nothing: a set waiting on one conflicting run holds none of its other runs meanwhile.
TEST(FileRangeLockRunsTest, AWaitingSetHoldsNoneOfItsRuns) {
    FileRangeLock lock;
    lock.LockRange(100, 10, kWrite);
    std::atomic<bool> acquired{false}, release{false};
    std::thread set = RunsThread(lock, {{0, 10}, {100, 10}, {200, 10}}, acquired, release);
    WaitForWaitingWriters(lock, 1);
    AbortIfAWaitFailed();
    EXPECT_EQ(lock.ActiveLocksForTests(), 1u);
    EXPECT_TRUE(lock.TryLockRangeFor(0, 10, kWrite, milliseconds(0))) << "the waiting set holds its first run";
    EXPECT_TRUE(lock.TryLockRangeFor(200, 10, kRead, milliseconds(0))) << "the waiting set holds its last run";
    lock.UnlockRange(100, 10, kWrite);
    std::this_thread::sleep_for(milliseconds(30));
    EXPECT_FALSE(acquired.load()) << "the set got in while its first run was held";
    lock.UnlockRange(0, 10, kWrite);
    std::this_thread::sleep_for(milliseconds(30));
    EXPECT_FALSE(acquired.load()) << "the set got in while its last run was held";
    lock.UnlockRange(200, 10, kRead);
    WaitForFlag(acquired, "the set never got in");
    AbortIfAWaitFailed();
    EXPECT_EQ(lock.ActiveLocksForTests(), 3u);
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    release.store(true);
    set.join();
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_EQ(lock.WriterWaits().waits, 1u) << "a set's wait counts once";
}

// The hull is registered as ONE waiting writer while the set waits and is gone once it got in, through a slot and
// through the overflow (slots all taken).
TEST(FileRangeLockRunsTest, AWaitingSetRegistersItsHullOnceAndLeavesNothingBehind) {
    FileRangeLock lock;
    lock.LockRange(0, 20, kRead);
    {
        std::atomic<bool> acquired{false}, release{false};
        std::thread set = RunsThread(lock, {{10, 10}, {500, 10}}, acquired, release);
        WaitForWaitingWriters(lock, 1);
        AbortIfAWaitFailed();
        WriterYield yield;
        EXPECT_TRUE(lock.TryReadLockYieldingFor(305, 1, milliseconds(0), milliseconds(20), yield)) << "past the cap";
        EXPECT_TRUE(yield.yielded) << "the hull between the runs is not registered";
        lock.UnlockRange(305, 1, kRead);
        lock.UnlockRange(0, 20, kRead);
        WaitForFlag(acquired, "the set never got in");
        AbortIfAWaitFailed();
        EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
        release.store(true);
        set.join();
    }
    // Overflow: 32 single writers take every slot, the set counts in the overflow.
    lock.LockRange(0, 100000, kRead);
    std::vector<std::thread> writers;
    for (unsigned i = 0; i < 32; ++i) writers.push_back(WriterThread(lock, i * 10ULL, 10, 1));
    WaitForWaitingWriters(lock, 32);
    AbortIfAWaitFailed();
    std::atomic<bool> acquired{false}, release{false};
    std::thread set = RunsThread(lock, {{1000, 10}, {5000, 10}}, acquired, release);
    WaitForWaitingWriters(lock, 33);
    AbortIfAWaitFailed();
    lock.UnlockRange(0, 100000, kRead);
    for (std::thread &t : writers) t.join();
    WaitForFlag(acquired, "the set never got in");
    AbortIfAWaitFailed();
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    release.store(true);
    set.join();
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_EQ(lock.WriterWaits().waits, 34u);
}

// The set's conflict check at a large set (4096 runs, the normal cap's size): 4096 runs [100 i, 100 i + 50) against 1000 held READ ranges in random
// gaps (some touching both neighbouring runs exactly), one WRITE in a free gap and one READ past the last run gets in
// at once; adding ONE held range (READ or WRITE) that overlaps a run (each shape: over a run's start, over its end,
// inside, around it, across two runs, rest of file; and the first byte of run 0 and the last byte of the last run)
// keeps it out until that range goes. (The set is WRITE: the held type does not matter to it; the random gaps are
// READs so the overlapping ranges can be taken beside them.)
TEST(FileRangeLockRunsTest, AFullSizeSetConflictsIffAHeldRangeOverlapsOneOfItsRuns) {
    constexpr uint64_t kRuns = 4096;   // the predicate at a large set, not the tunable
    std::vector<LockRun> runs;
    for (uint64_t i = 0; i < kRuns; ++i) runs.push_back({100 * i, 50});
    std::mt19937_64 rng(4096);
    FileRangeLock lock;
    struct Held { unsigned long long start, length; FileRangeLock::LockType type; };
    std::vector<Held> gaps;
    std::set<uint64_t> gap_runs;
    while (gap_runs.size() < 1000) gap_runs.insert(rng() % kRuns);
    for (uint64_t i : gap_runs) {
        const unsigned long long lo = 100 * i + 50 + (i % 3 == 0 ? 0 : rng() % 25);
        const unsigned long long hi = i % 3 == 0 ? 100 * i + 100 : lo + 1 + rng() % (100 * i + 100 - lo - 1);
        gaps.push_back({lo, hi - lo, kRead});
    }
    uint64_t free_gap = rng() % (kRuns - 1);   // a WRITE in a gap nothing else holds
    while (gap_runs.count(free_gap) != 0) free_gap = rng() % (kRuns - 1);
    gap_runs.insert(free_gap);                  // the overlapped run below keeps clear of it too
    gaps.push_back({100 * free_gap + 50, 50, kWrite});
    gaps.push_back({100 * kRuns, 10, kRead});   // after every run: the search finds no run ending past it
    for (const Held &h : gaps) lock.LockRange(h.start, h.length, h.type);

    const auto set_gets_in = [&](std::chrono::milliseconds within) {
        auto f = std::async(std::launch::async, [&] { lock.LockRuns(runs.data(), runs.size()); });
        const bool in = f.wait_for(within) == std::future_status::ready;
        return std::make_pair(in, std::move(f));
    };
    {
        auto [in, f] = set_gets_in(std::chrono::seconds(10));
        if (!in) {   // the set is stuck in LockRuns: f's destructor would hang
            ADD_FAILURE() << "the set waited for ranges in its gaps only";
            AbortAfterAFailedWait();
        }
        lock.UnlockRuns(runs.data(), runs.size());
    }
    uint64_t k = 1 + rng() % (kRuns - 2);   // the overlapped run: the gaps around it hold nothing (no WRITE vs gap READ)
    while (gap_runs.count(k - 1) + gap_runs.count(k) + gap_runs.count(k + 1) != 0) k = 1 + rng() % (kRuns - 2);
    const uint64_t s = 100 * k;
    const std::vector<Held> overlaps = {
        {s - 10, 11, kRead}, {s + 49, 11, kWrite}, {s + 10, 10, kRead},
        {s - 5, 60, kWrite}, {s + 40, 70, kRead}, {0, 1, kRead},                         // run 0's first byte
        {100 * (kRuns - 1) + 49, 1, kRead}, {100 * (kRuns - 1) + 10, 0, kRead}, // last run's last byte; to the end of file
    };
    for (const Held &o : overlaps) {
        SCOPED_TRACE("held [" + std::to_string(o.start) + ", +" + std::to_string(o.length) + ")");
        lock.LockRange(o.start, o.length, o.type);
        auto [in, f] = set_gets_in(milliseconds(0));
        WaitForWaitingWriters(lock, 1); // the set waits as a writer on the held range (not merely slow to start)
        EXPECT_NE(f.wait_for(milliseconds(0)), std::future_status::ready) << "the set got in over a held overlapping range";
        (void)in;
        lock.UnlockRange(o.start, o.length, o.type);
        if (f.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            ADD_FAILURE() << "the set did not get in after the overlapping range went";
            AbortAfterAFailedWait();
        }
        lock.UnlockRuns(runs.data(), runs.size());
    }
    for (const Held &h : gaps) lock.UnlockRange(h.start, h.length, h.type);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// count == 1 is LockRange/UnlockRange: the same entry (either call releases the other's), the same wait accounting.
TEST(FileRangeLockRunsTest, ASetOfOneIsLockRange) {
    FileRangeLock lock;
    const LockRun one{40, 20};
    lock.LockRuns(&one, 1);
    EXPECT_EQ(lock.ActiveLocksForTests(), 1u);
    EXPECT_FALSE(lock.TryLockRangeFor(59, 1, kRead, milliseconds(0)));
    lock.UnlockRange(40, 20, kWrite);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    lock.LockRange(40, 20, kWrite);
    lock.UnlockRuns(&one, 1);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    lock.LockRange(0, 100, kRead);
    std::atomic<bool> acquired{false}, release{true};
    std::thread set = RunsThread(lock, {one}, acquired, release);
    WaitForWaitingWriters(lock, 1);
    AbortIfAWaitFailed();
    lock.UnlockRange(0, 100, kRead);
    set.join();
    EXPECT_TRUE(acquired.load());
    EXPECT_EQ(lock.WriterWaits().waits, 1u);
    lock.LockRuns(nullptr, 0);
    lock.UnlockRuns(nullptr, 0);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// The registered hull is conservative: a yielding READ in a gap of a WAITING set yields to it, like to a single wide
// writer. Once the set holds its runs, the gap READ has no reason to wait and is let go at once (not at the set's
// unlock, not at the cap).
TEST(FileRangeLockRunsTest, AYieldingReadInTheGapOfAWaitingSetYieldsUntilTheSetGotIn) {
    FileRangeLock lock;
    lock.LockRange(0, 10, kRead);
    std::atomic<bool> acquired{false}, release{false};
    std::thread set = RunsThread(lock, {{0, 10}, {500, 10}}, acquired, release);
    WaitForWaitingWriters(lock, 1);
    AbortIfAWaitFailed();
    WriterYield yield;
    auto reader = std::async(std::launch::async, [&] { return lock.TryReadLockYieldingFor(200, 10, milliseconds(50), milliseconds(5000), yield); });
    EXPECT_EQ(reader.wait_for(milliseconds(100)), std::future_status::timeout) << "a gap READ did not yield to the hull";
    lock.UnlockRange(0, 10, kRead);
    ASSERT_EQ(reader.wait_for(std::chrono::seconds(4)), std::future_status::ready) << "the gap READ waited for the set's unlock";
    EXPECT_TRUE(reader.get());
    EXPECT_TRUE(yield.yielded);
    EXPECT_LT(yield.us, 2'000'000u);
    WaitForFlag(acquired, "the set never got in");
    AbortIfAWaitFailed();
    lock.UnlockRange(200, 10, kRead);
    release.store(true);
    set.join();
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// 8 threads for 5 s: run sets of 1-6 runs, blocking single WRITEs and READs, yielding READs over 32 cells. No deadlock
// or lost wakeup (a watchdog), exclusion holds per cell, and nothing is left registered.
TEST(FileRangeLockRunsTest, StressRunSetsAndSingleRangesNoDeadlockExclusionHolds) {
    FileRangeLock lock;
    constexpr unsigned kCells = 32;
    std::array<std::atomic<int>, kCells> readers{};
    std::array<std::atomic<int>, kCells> writers_in{};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> violations{0}, ops{0}, set_ops{0};
    const auto enter = [&](unsigned lo, unsigned hi, bool write) {
        for (unsigned c = lo; c < hi; ++c) {
            if (write) {
                if (writers_in[c].fetch_add(1) != 0 || readers[c].load() != 0) ++violations;
            } else {
                readers[c].fetch_add(1);
                if (writers_in[c].load() != 0) ++violations;
            }
        }
    };
    const auto leave = [&](unsigned lo, unsigned hi, bool write) {
        for (unsigned c = lo; c < hi; ++c) (write ? writers_in[c] : readers[c]).fetch_sub(1);
    };
    const auto hold = [](std::mt19937 &rng) { std::this_thread::sleep_for(std::chrono::microseconds(rng() % 200)); };
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 8; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(t * 104729u + 3);
            std::vector<LockRun> runs;
            std::vector<std::pair<unsigned, unsigned>> cells;
            while (!stop.load()) {
                const unsigned kind = t % 4; // 0,1: run sets; 2: single WRITE or READ; 3: yielding READ
                if (kind < 2) {
                    runs.clear();
                    cells.clear();
                    for (unsigned c = rng() % 4; c < kCells && runs.size() < 6; c += 1 + rng() % 8) {
                        const unsigned hi = std::min(kCells, c + 1 + static_cast<unsigned>(rng() % 2));
                        cells.emplace_back(c, hi);
                        runs.push_back({c * 100ULL, (hi - c) * 100ULL});
                        c = hi;
                    }
                    lock.LockRuns(runs.data(), runs.size());
                    for (const auto &[lo, hi] : cells) enter(lo, hi, true);
                    hold(rng);
                    for (const auto &[lo, hi] : cells) leave(lo, hi, true);
                    lock.UnlockRuns(runs.data(), runs.size());
                    ++set_ops;
                } else {
                    const unsigned lo = rng() % kCells;
                    const unsigned hi = lo + 1 + rng() % std::min(4u, kCells - lo);
                    const unsigned long long off = lo * 100ULL, len = (hi - lo) * 100ULL;
                    if (kind == 2) {
                        const bool write = rng() % 2 == 0;
                        lock.LockRange(off, len, write ? kWrite : kRead);
                        enter(lo, hi, write);
                        hold(rng);
                        leave(lo, hi, write);
                        lock.UnlockRange(off, len, write ? kWrite : kRead);
                    } else {
                        WriterYield y;
                        if (lock.TryReadLockYieldingFor(off, len, milliseconds(rng() % 5), milliseconds(rng() % 20), y)) {
                            enter(lo, hi, false);
                            hold(rng);
                            leave(lo, hi, false);
                            lock.UnlockRange(off, len, kRead);
                        }
                    }
                }
                ++ops;
            }
        });
    }
    std::this_thread::sleep_for(std::chrono::seconds(5));
    stop.store(true);
    auto joined = std::async(std::launch::async, [&] { for (std::thread &t : threads) t.join(); });
    if (joined.wait_for(std::chrono::seconds(30)) != std::future_status::ready) {
        // stop is already set: a thread still not out is blocked for good, and the future's destructor would join it.
        ADD_FAILURE() << "deadlock or lost wakeup";
        std::fflush(nullptr);
        std::abort();
    }
    EXPECT_EQ(violations.load(), 0u);
    EXPECT_GT(set_ops.load(), 100u);
    EXPECT_GT(lock.WriterWaits().waits, 0u) << "nothing ever waited: the test proves little";
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    std::printf("[run lock stress] %llu ops, %llu run sets, %llu writer waits\n", static_cast<unsigned long long>(ops.load()),
                static_cast<unsigned long long>(set_ops.load()), static_cast<unsigned long long>(lock.WriterWaits().waits));
}

// ---- FileRangeLock::LockRuns(READ): BatchQuery's per-run READ locks; BuildLockRuns ----------------------------------

// READ sets share overlapping and identical runs; releasing one leaves exactly the other's entries (an identical READ
// entry of another holder is not taken with it).
TEST(FileRangeLockReadRunsTest, OverlappingReadSetsCoexistAndEachUnlockTakesOnlyItsOwnEntries) {
    FileRangeLock lock;
    const std::vector<LockRun> a = {{0, 10}, {100, 10}, {200, 10}};
    const std::vector<LockRun> b = {{5, 10}, {100, 10}, {300, 10}};
    lock.LockRuns(a.data(), a.size(), kRead);
    auto other = std::async(std::launch::async, [&] {
        lock.LockRuns(b.data(), b.size(), kRead);
        lock.LockRuns(a.data(), a.size(), kRead);   // an identical set too
    });
    if (other.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {   // stuck in LockRuns: f would hang
        ADD_FAILURE() << "a READ set waited for READs";
        AbortAfterAFailedWait();
    }
    EXPECT_EQ(lock.ActiveLocksForTests(), 9u);
    lock.UnlockRuns(a.data(), a.size(), kRead);
    EXPECT_EQ(lock.ActiveLocksForTests(), 6u);
    for (const LockRun &r : a) EXPECT_FALSE(lock.TryLockRangeFor(r.start, r.length, kWrite, milliseconds(0))) << r.start;
    lock.UnlockRuns(a.data(), a.size(), kRead);
    EXPECT_EQ(lock.ActiveLocksForTests(), 3u);
    EXPECT_TRUE(lock.TryLockRangeFor(200, 10, kWrite, milliseconds(0))) << "a's last run was not released";
    lock.UnlockRange(200, 10, kWrite);
    EXPECT_FALSE(lock.TryLockRangeFor(100, 10, kWrite, milliseconds(0))) << "b's shared run was released with a's";
    lock.UnlockRuns(b.data(), b.size(), kRead);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_EQ(lock.WriterWaits().waits, 0u);
}

// A held READ set blocks writes on its runs only; a WRITE held in a gap does not keep it out.
TEST(FileRangeLockReadRunsTest, AHeldWriteInAGapDoesNotBlockAReadSet) {
    FileRangeLock lock;
    const std::vector<LockRun> runs = {{0, 10}, {100, 10}, {200, 10}};
    lock.LockRange(10, 90, kWrite);
    lock.LockRange(110, 90, kRead);
    auto set = std::async(std::launch::async, [&] { lock.LockRuns(runs.data(), runs.size(), kRead); });
    if (set.wait_for(std::chrono::seconds(5)) != std::future_status::ready) {
        ADD_FAILURE() << "a READ set waited for a gap WRITE";
        AbortAfterAFailedWait();
    }
    for (const LockRun &r : runs) {
        SCOPED_TRACE(r.start);
        EXPECT_FALSE(lock.TryLockRangeFor(r.start, r.length, kWrite, milliseconds(0)));
        ASSERT_TRUE(lock.TryLockRangeFor(r.start + 5, 1, kRead, milliseconds(0)));
        lock.UnlockRange(r.start + 5, 1, kRead);
    }
    EXPECT_FALSE(lock.TryLockRangeFor(50, 100, kWrite, milliseconds(0))) << "a WRITE straddling a run";
    lock.UnlockRuns(runs.data(), runs.size(), kRead);
    lock.UnlockRange(10, 90, kWrite);
    lock.UnlockRange(110, 90, kRead);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// A WRITE on one run keeps the READ set out until released; meanwhile the set holds none of its runs and registers
// nothing as a waiting writer, and its wait is not a writer wait.
TEST(FileRangeLockReadRunsTest, AHeldWriteOnARunBlocksTheReadSetWhichHoldsNothingMeanwhile) {
    FileRangeLock lock;
    lock.LockRange(105, 1, kWrite);
    std::atomic<bool> acquired{false}, release{false};
    std::thread set = RunsThread(lock, {{0, 10}, {100, 10}, {200, 10}}, acquired, release, kRead);
    std::this_thread::sleep_for(milliseconds(50));
    EXPECT_FALSE(acquired.load()) << "the READ set got in over a held WRITE";
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    EXPECT_EQ(lock.ActiveLocksForTests(), 1u);
    EXPECT_TRUE(lock.TryLockRangeFor(0, 10, kWrite, milliseconds(0))) << "the waiting set holds its first run";
    EXPECT_TRUE(lock.TryLockRangeFor(200, 10, kWrite, milliseconds(0))) << "the waiting set holds its last run";
    lock.UnlockRange(105, 1, kWrite);
    std::this_thread::sleep_for(milliseconds(30));
    EXPECT_FALSE(acquired.load()) << "the set got in while its first run was held";
    lock.UnlockRange(0, 10, kWrite);
    std::this_thread::sleep_for(milliseconds(30));
    EXPECT_FALSE(acquired.load()) << "the set got in while its last run was held";
    lock.UnlockRange(200, 10, kWrite);
    WaitForFlag(acquired, "the set never got in");
    AbortIfAWaitFailed();
    EXPECT_EQ(lock.ActiveLocksForTests(), 3u);
    release.store(true);
    set.join();
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_EQ(lock.WaitingWritersForTests(), 0u);
    EXPECT_EQ(lock.WriterWaits().waits, 0u) << "a READ set's wait is not a writer wait";
}

// UnlockRuns(READ) wakes waiters: a WRITE set waiting on a held READ set gets in once it goes; a set of one is
// LockRange(READ).
TEST(FileRangeLockReadRunsTest, UnlockingAReadSetWakesAWaitingWriteSet) {
    FileRangeLock lock;
    const std::vector<LockRun> reads = {{0, 10}, {100, 10}};
    lock.LockRuns(reads.data(), reads.size(), kRead);
    std::atomic<bool> acquired{false}, release{true};
    std::thread set = RunsThread(lock, {{50, 10}, {105, 1}}, acquired, release);
    WaitForWaitingWriters(lock, 1);
    AbortIfAWaitFailed();
    lock.UnlockRuns(reads.data(), reads.size(), kRead);
    WaitForFlag(acquired, "the WRITE set never got in after the READ set went");
    AbortIfAWaitFailed();
    set.join();
    const LockRun one{40, 20};
    lock.LockRuns(&one, 1, kRead);
    lock.UnlockRange(40, 20, kRead);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

TEST(BuildLockRunsTest, MergesAdjacentAndDuplicateRowsAndTakesTheHullPastTheCap) {
    std::vector<LockRun> runs = {{1, 1}};   // cleared first
    const auto build = [&](std::vector<unsigned long long> offsets, size_t cap) {
        BuildLockRuns(offsets.data(), offsets.size(), 10, cap, runs);
        std::vector<std::pair<unsigned long long, unsigned long long>> out;
        for (const LockRun &r : runs) out.emplace_back(r.start, r.length);
        return out;
    };
    using V = std::vector<std::pair<unsigned long long, unsigned long long>>;
    EXPECT_EQ(build({}, 4), V{});
    EXPECT_EQ(build({30}, 1), (V{{30, 10}}));
    EXPECT_EQ(build({0, 10, 10, 20, 50, 50, 70, 80}, 3), (V{{0, 30}, {50, 10}, {70, 20}})) << "exactly the cap";
    EXPECT_EQ(build({0, 10, 10, 20, 50, 50, 70, 80}, 2), (V{{0, 90}})) << "one over the cap: the hull";
    EXPECT_EQ(build({0, 20}, 1), (V{{0, 30}}));
    EXPECT_EQ(build({40, 40, 40}, 1), (V{{40, 10}}));
}

// ---- Helpers --------------------------------------------------------------------------------------------------------

unsigned long long Jan1(unsigned short year) {
    unsigned long long days = 0;
    for (unsigned y = 1970; y < year; ++y) days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    return days * 86400ULL;
}

std::string ReadFileBytes(const std::string &path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

template <typename Header>
Header ReadHeader(const std::string &path) {
    Header h{};
    const int fd = open(path.c_str(), O_RDONLY);
    EXPECT_GE(fd, 0) << path;
    EXPECT_EQ(pread(fd, &h, sizeof(h), 0), static_cast<ssize_t>(sizeof(h))) << path;
    close(fd);
    return h;
}

// Copies only the data extents of a sparse year file, keeping its size.
void SparseCopy(const std::string &from, const std::string &to) {
    const int in = open(from.c_str(), O_RDONLY);
    ASSERT_GE(in, 0) << from;
    const int out = open(to.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(out, 0) << to;
    const off_t size = lseek(in, 0, SEEK_END);
    ASSERT_EQ(ftruncate(out, size), 0);
    std::vector<char> buf(1 << 20);
    off_t pos = 0;
    while (pos < size) {
        const off_t data = lseek(in, pos, SEEK_DATA);
        if (data < 0) break;
        const off_t hole = lseek(in, data, SEEK_HOLE);
        for (off_t p = data; p < hole;) {
            const ssize_t n = pread(in, buf.data(), static_cast<size_t>(std::min<off_t>(hole - p, static_cast<off_t>(buf.size()))), p);
            ASSERT_GT(n, 0);
            ASSERT_EQ(pwrite(out, buf.data(), static_cast<size_t>(n), p), n);
            p += n;
        }
        pos = hole;
    }
    close(in);
    close(out);
}

// Numeric coercion of a column, the zone map's rule (FData cases 1..12; bool, string and bytes are not numeric).
bool NumericOf(const XStore_Proto::FData &f, double &out) {
    long double v = 0;
    switch (f.oneOfData_case()) {
        case XStore_Proto::FData::kDoubleData: v = f.doubledata(); break;
        case XStore_Proto::FData::kFloatData: v = f.floatdata(); break;
        case XStore_Proto::FData::kInt32Data: v = f.int32data(); break;
        case XStore_Proto::FData::kInt64Data: v = f.int64data(); break;
        case XStore_Proto::FData::kUint32Data: v = f.uint32data(); break;
        case XStore_Proto::FData::kUint64Data: v = f.uint64data(); break;
        case XStore_Proto::FData::kSint32Data: v = f.sint32data(); break;
        case XStore_Proto::FData::kSint64Data: v = f.sint64data(); break;
        case XStore_Proto::FData::kFixed32Data: v = f.fixed32data(); break;
        case XStore_Proto::FData::kFixed64Data: v = f.fixed64data(); break;
        case XStore_Proto::FData::kSfixed32Data: v = f.sfixed32data(); break;
        case XStore_Proto::FData::kSfixed64Data: v = f.sfixed64data(); break;
        default: return false;
    }
    out = static_cast<double>(v);
    return true;
}

// Every present row of [lo, hi] of an open year file, parsed.
std::vector<std::pair<uint64_t, XStore_Proto::RowData>> ReadRows(int fd, uint64_t lo, uint64_t hi, uint16_t row_length) {
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    const bool complete = ForEachDataRow(fd, hi + 1, row_length, [&](uint64_t r, const char *buf) {
        if (buf[0] == 0) return;
        XStore_Proto::RowData row;
        const size_t real_len = XStoreWireEncode::RealRowDataLength(buf, row_length);
        if (real_len > 0) static_cast<void>(row.ParseFromArray(buf, static_cast<int>(real_len)));
        rows.emplace_back(r, std::move(row));
    }, lo);
    EXPECT_TRUE(complete);
    return rows;
}

// A row of `columns` doubles (base + c), with a null column wherever (base + c) % 13 == 0. Integer values keep every
// sum exact whatever the summation order.
XStore_Proto::RowData ValueRow(uint64_t base, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        const uint64_t v = base + static_cast<uint64_t>(c);
        if (v % 13 != 0) f->set_doubledata(static_cast<double>(v));
    }
    return row;
}

// A row cycling through every FData type, with nulls (the stage-2a reference test's shape).
XStore_Proto::RowData MixedRow(uint64_t i, int columns) {
    XStore_Proto::RowData row;
    for (int c = 0; c < columns; ++c) {
        XStore_Proto::FData *f = row.add_fielddata();
        const uint64_t v = i * 31 + static_cast<uint64_t>(c);
        if ((i + static_cast<uint64_t>(c)) % 17 == 0) continue;
        switch (c % 16) {
            case 0: f->set_doubledata(static_cast<double>(v) * 0.5); break;
            case 1: f->set_floatdata(static_cast<float>(v) * 0.25f); break;
            case 2: f->set_int32data(-static_cast<int32_t>(v % 100000)); break;
            case 3: f->set_int64data(static_cast<int64_t>(v) * 1000003); break;
            case 4: f->set_uint32data(static_cast<uint32_t>(v)); break;
            case 5: f->set_uint64data(0xFFFFFFFFFFFF0000ULL + v); break;
            case 6: f->set_sint32data(-static_cast<int32_t>(v % 777)); break;
            case 7: f->set_sint64data(-static_cast<int64_t>(v)); break;
            case 8: f->set_fixed32data(static_cast<uint32_t>(v * 3)); break;
            case 9: f->set_fixed64data(v * 5); break;
            case 10: f->set_sfixed32data(-static_cast<int32_t>(v % 1000)); break;
            case 11: f->set_sfixed64data(-static_cast<int64_t>(v * 7)); break;
            case 12: f->set_booldata(v % 2 == 0); break;
            case 13: f->set_stringdata("sym" + std::to_string(v % 23)); break;
            case 14: f->set_bytesdata(std::string("\x01\x00\x02", 3) + std::to_string(v % 5)); break;
            default: f->set_doubledata(-static_cast<double>(v)); break;
        }
    }
    return row;
}

using Rows = std::vector<std::pair<unsigned long long, XStore_Proto::RowData>>;

class XStoreLiveWidenTest : public ::testing::Test {
protected:
    static constexpr uint16_t kRowLength = 2048; // 1024 rows per zone-map block; room for 100 double columns
    static constexpr unsigned short kYear = 1980;
    std::unique_ptr<ScratchDir> scratch;

    void SetUp() override {
        scratch = std::make_unique<ScratchDir>("xstore_live_widen_test");
        for (auto &shard : XStore::cfg_shards) shard.map.clear();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        XStore::ClearLiveWidenRequestsForTests(); // other tests' growths are recorded too (nothing runs a pass there)
    }
    void TearDown() override {
        XStore::bg_cancel_.store(false);
        XStore::ClearLiveWidenRequestsForTests();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
        walring::ClearRingRegistryForTests();
        scratch.reset();
    }

    static uint64_t Rpb() { return RowsPerBlock(kRowLength); }
    static unsigned long long Epoch(uint64_t row) { return Jan1(kYear) + row; }
    std::string DbPath(const std::string &db) const { return (scratch->test_root / "DataStore" / db).string(); }
    std::string YearPath(const std::string &db, const char *ext, unsigned short year = kYear) const {
        return DbPath(db) + "/" + std::to_string(year) + ext;
    }

    // `per_request` rows per BatchInsert (one WAL record per year in each).
    static void Insert(XStore &store, const std::string &db, const Rows &rows,
                       XStore_Proto::WalDurabilityMode mode = XStore_Proto::SYNC, size_t per_request = 4000) {
        for (size_t from = 0; from < rows.size(); from += per_request) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(mode);
            for (size_t i = from; i < std::min(rows.size(), from + per_request); ++i) {
                XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                entry->set_timestamp(rows[i].first);
                *entry->mutable_data() = rows[i].second;
            }
            XStore_Proto::BatchInsert_REP rep;
            store.BatchInsert(&req, &rep);
            ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        }
    }

    // A DB whose kYear index files are 64 columns wide (created by a 10-column first row) while its rows, and so its
    // configured width, reach `width` columns: rows 1..rows-1 are ValueRow(row * 7 % 100000), every 5th one only 40
    // columns wide.
    void SeedNarrowIndexWideRows(XStore &store, const std::string &db, uint64_t rows, int width) {
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
        Rows batch;
        for (uint64_t r = 1; r < rows; ++r) batch.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : width));
        Insert(store, db, batch);
        ASSERT_EQ(XStore::GetOrOpenZoneMap(DbPath(db), kYear, kRowLength).TrackedColumnCount(), 64u);
        ASSERT_EQ(XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength).TrackedColumnCount(), 64u);
    }

    // Clean stop (indexes clean on disk), then a "new process": empty registries.
    static void CleanRestartState() {
        XStore::Shutdown();
        XStore::ResetShutdownForTests();
        XStore::ClearZoneMapRegistryForTests();
        XStore::ClearCatIdxRegistryForTests();
    }

    // The startup rebuild of db's kYear file at `width`: a second DB (configured `width` wide by one row in 1975) gets
    // a sparse copy of the year file and no sidecars, and InitDB rebuilds it (RebuildYearIndexes). InitDB also
    // rebuilds whatever else is stale, db's own dirty indexes included.
    void BuildStartupReference(XStore &store, const std::string &db, const std::string &ref_db, int width) {
        CreateTestDb(store, ref_db, XStore_Proto::SECOND, kRowLength);
        Insert(store, ref_db, {{Jan1(1975), ValueRow(1, width)}});
        SparseCopy(YearPath(db, ".XSTORE"), YearPath(ref_db, ".XSTORE"));
        ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle()); // nothing left to replay
        store.InitDB();
        EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(ref_db, ".zmap")).tracked_column_count, static_cast<uint32_t>(width));
    }

    void ExpectSameFilesAs(const std::string &db, const std::string &ref_db) {
        for (const char *ext : {".zmap", ".catidx"}) {
            SCOPED_TRACE(ext);
            const std::string got = ReadFileBytes(YearPath(db, ext));
            const std::string want = ReadFileBytes(YearPath(ref_db, ext));
            EXPECT_EQ(got.size(), want.size());
            EXPECT_TRUE(got == want) << "differs from the startup rebuild";
        }
    }

    void RunIdleWiden(XStore &store, int width, const std::string &db, const std::string &ref_db);
    // The concurrent oracle of section 2, with the builder's worker count and group size.
    void RunConcurrentOracle(const std::string &db, unsigned workers, uint64_t group_bytes,
                             livewiden::LiveWidenGovernor *governor = nullptr,
                             bool keep_old = XStoreTunables::LIVE_WIDEN_KEEP_OLD_INDEX);
    void ExpectIndexesMatchAStartupRebuild(XStore &store, const std::string &db, int width, uint64_t blocks,
                                           uint64_t min_bloom_checked);
    // Per-run WAL apply locks: scattered BatchInserts, BatchDeletes of rows in their gaps and range queries at once.
    enum class Widen { kNone, kDropOld, kKeepOld };
    void RunScatteredInsertsWithGapDeletes(const std::string &db, Widen widen);

    // Answers on [lo, hi] for every listed column, against a scan of the data file (no concurrent writers here).
    void ExpectAnswersMatchTheData(XStore &store, const std::string &db, uint64_t lo, uint64_t hi,
                                   const std::vector<int> &columns, unsigned short year = kYear) {
        const auto at = [year](uint64_t row) { return Jan1(year) + row; };
        const int fd = open(YearPath(db, ".XSTORE", year).c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        const auto rows = ReadRows(fd, lo, hi, kRowLength);
        close(fd);
        for (int c : columns) {
            SCOPED_TRACE("column " + std::to_string(c));
            double max = -INFINITY, sum = 0;
            uint64_t count = 0;
            const XStore_Proto::FData *some_value = nullptr;
            uint64_t some_value_rows = 0;
            for (const auto &[r, row] : rows) {
                double v = 0;
                if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) {
                    max = std::max(max, v);
                    sum += v;
                    ++count;
                    if (some_value == nullptr) some_value = &row.fielddata(c);
                }
            }
            ASSERT_GT(count, 0u);
            for (const auto &[r, row] : rows) {
                if (c < row.fielddata_size() && row.fielddata(c).SerializeAsString() == some_value->SerializeAsString()) ++some_value_rows;
            }
            XStore_Proto::Max_REP max_rep;
            store.MaxAggregate(at(lo), at(hi), db, static_cast<unsigned short>(c), &max_rep);
            double got_max = 0;
            ASSERT_TRUE(NumericOf(max_rep.opresult(), got_max)) << max_rep.opstatus();
            EXPECT_EQ(got_max, max);
            XStore_Proto::Sum_REP sum_rep;
            store.SumAggregate(at(lo), at(hi), db, static_cast<unsigned short>(c), &sum_rep);
            EXPECT_NEAR(sum_rep.opresult(), sum, 1e-9 * std::max(1.0, std::fabs(sum))) << sum_rep.opstatus();
            std::string buf;
            store.FindByValue(at(lo), at(hi), db, c, *some_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                              [&](std::string &&chunk, bool) { buf += chunk; });
            XStore_Proto::RangeQuery_REP rep;
            ASSERT_TRUE(rep.ParseFromString(buf));
            EXPECT_EQ(static_cast<uint64_t>(rep.queryresponse_size()), some_value_rows);
        }
    }
};

constexpr const char *kNotIndexed = "not yet indexed";

// ---- 1. Idle correctness: byte-identical to the startup rebuild -----------------------------------------------------

// Dense mixed-type rows (every 10th one `width` columns wide), an overwrite and a delete (the old index carries an
// inexact block; a rebuild starts exact), far-apart sparse rows, a partial last block of data.
void XStoreLiveWidenTest::RunIdleWiden(XStore &store, int width, const std::string &db, const std::string &ref_db) {
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), MixedRow(0, 10)}}); // index files born 64 wide
    Rows rows;
    for (uint64_t i = 1; i < 5500; ++i) rows.emplace_back(Epoch(i), MixedRow(i, i % 10 == 0 ? width : 40 + static_cast<int>(i % 9)));
    for (uint64_t r : {20000ULL, 20001ULL, 50000ULL, 1000000ULL, 3000000ULL}) rows.emplace_back(Epoch(r), MixedRow(r, width));
    Insert(store, db, rows);
    Insert(store, db, {{Epoch(18), MixedRow(999, 12)}});
    ASSERT_NE(store.UnaryDelete(Epoch(17), db).deletestatus().find("[INFO]"), std::string::npos);

    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 64u);
    ASSERT_EQ(catidx.TrackedColumnCount(), 64u);
    const std::vector<int> columns = {0, 2, 64, 66, width - 4};

    // Before: right answers; a column past 64 is answered by a scan and says so.
    ExpectAnswersMatchTheData(store, db, 0, 5499, columns);
    XStore_Proto::Max_REP before;
    store.MaxAggregate(Epoch(0), Epoch(5499), db, 64, &before);
    EXPECT_NE(before.opstatus().find(kNotIndexed), std::string::npos) << before.opstatus();

    XStore::LiveWidenStats stats;
    ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, static_cast<uint32_t>(width), true, true, nullptr, &stats),
              Result::kDone);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(stats.zmap_began && stats.catidx_began);
    EXPECT_FALSE(stats.zmap_busy || stats.catidx_busy);
    EXPECT_EQ(stats.rows_folded, 5500u + 5u - 1u) << "every present row folded exactly once";
    EXPECT_GT(stats.chunks, 2u);
    EXPECT_EQ(stats.lock_timeouts, 0u);
    // Same objects (registry entries are never replaced), now wide, clean on disk.
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(path, kYear, kRowLength), &zmap);
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(path, kYear, kRowLength), &catidx);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), static_cast<uint32_t>(width));
    EXPECT_EQ(catidx.TrackedColumnCount(), static_cast<uint32_t>(width));
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    auto block0 = zmap.TryGetBlockStats(0);
    ASSERT_TRUE(block0.has_value());
    EXPECT_EQ(block0->size(), static_cast<size_t>(width));
    EXPECT_GT((*block0)[64].valid_count, 0u) << "a new column is summarized";

    // After: right answers, and the new columns are indexed (no scan notice).
    ExpectAnswersMatchTheData(store, db, 0, 5499, columns);
    XStore_Proto::Max_REP after;
    store.MaxAggregate(Epoch(0), Epoch(5499), db, 64, &after);
    EXPECT_EQ(after.opstatus(), "SUCCESS");

    BuildStartupReference(store, db, ref_db, width);
    ExpectSameFilesAs(db, ref_db);
}

TEST_F(XStoreLiveWidenTest, IdleWidenTo70IsByteIdenticalToTheStartupRebuild) {
    XStore store;
    RunIdleWiden(store, 70, "LWIDLE70", "LWIDLE70REF");
}

TEST_F(XStoreLiveWidenTest, IdleWidenTo100IsByteIdenticalToTheStartupRebuild) {
    XStore store;
    RunIdleWiden(store, 100, "LWIDLE100", "LWIDLE100REF");
}

// ---- 2. Concurrent oracle -------------------------------------------------------------------------------------------

// Answers checked while 4 writers (Unary/Batch/Range inserts in SYNC/ASYNC/BYPASS, overwrites, duplicate timestamps,
// all three deletes) race the builder below, inside and above its moving frontier. A query thread holds a READ lock on
// the data-file range it asks about (writers of those rows wait; the builder, also a reader, does not), so its scan of
// the data file is the exact answer the store must give at that instant: before, during and after the widen. At the
// end every tracked column of every exact block equals a startup rebuild of the final data, and the bloom holds every
// present value.
void XStoreLiveWidenTest::RunConcurrentOracle(const std::string &db, unsigned workers, uint64_t group_bytes,
                                              livewiden::LiveWidenGovernor *governor, bool keep_old) {
    XStore store;
    constexpr int kWidth = 70;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 96;
    const uint64_t rows = kBlocks * rpb;
    // Even rows only: most writer traffic is fresh rows in the odd holes, which keeps blocks exact (an overwrite or a
    // delete flags its block and takes it out of the final block comparison).
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    {
        Rows seed;
        for (uint64_t r = 2; r < rows; r += 2) seed.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 10 == 0 ? 40 : kWidth));
        Insert(store, db, seed);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 64u);

    std::atomic<bool> running{true};
    std::atomic<int> writes{0};
    std::atomic<int> queries{0};
    std::atomic<int> mismatches{0};
    std::mutex fresh_mutex;
    std::set<uint64_t> fresh_used;
    auto frontier = [&]() -> uint64_t {
        const uint64_t f = zmap.FrontierForTests();
        return f == ZoneMapFile::kNoRebuild ? (zmap.TrackedColumnCount() == 64 ? 0 : rows) : std::min(f, rows);
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            std::mt19937_64 rng(4242 + t);
            auto value = [&] { return rng() % 90000; };
            while (running.load()) {
                const int64_t f = static_cast<int64_t>(frontier());
                const uint64_t near = static_cast<uint64_t>(std::clamp<int64_t>(f + static_cast<int64_t>(rng() % 3000) - 1000, 1,
                                                                                 static_cast<int64_t>(rows) - 1));
                const uint64_t anywhere = 1 + rng() % (rows - 1);
                const uint64_t target = rng() % 2 ? near : anywhere;
                const XStore_Proto::WalDurabilityMode mode =
                    std::array<XStore_Proto::WalDurabilityMode, 3>{XStore_Proto::SYNC, XStore_Proto::ASYNC, XStore_Proto::BYPASS}[rng() % 3];
                const unsigned op = static_cast<unsigned>(rng() % 100);
                if (op < 55) { // fresh odd rows, one Unary or a scattered Batch
                    std::vector<uint64_t> picked;
                    {
                        std::lock_guard<std::mutex> lock(fresh_mutex);
                        for (uint64_t i = 0; i < 1 + (op % 4) && (target | 1) + i * 64 < rows; ++i) {
                            if (fresh_used.insert((target | 1) + i * 64).second) picked.push_back((target | 1) + i * 64);
                        }
                    }
                    if (picked.empty()) continue;
                    if (picked.size() == 1) {
                        XStore_Proto::UnaryInsert_REP rep;
                        w.UnaryInsert(Epoch(picked[0]), ValueRow(value(), rng() % 4 == 0 ? 40 : kWidth), db, &rep, mode);
                    } else {
                        XStore_Proto::BatchInsert_REQ req;
                        req.set_dbname(db);
                        req.set_durabilitymode(mode);
                        for (uint64_t r : picked) {
                            XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                            e->set_timestamp(Epoch(r));
                            *e->mutable_data() = ValueRow(value(), kWidth);
                        }
                        XStore_Proto::BatchInsert_REP rep;
                        w.BatchInsert(&req, &rep);
                    }
                } else if (op < 63) { // overwrite one row
                    XStore_Proto::UnaryInsert_REP rep;
                    w.UnaryInsert(Epoch(target), ValueRow(value(), kWidth), db, &rep, mode);
                } else if (op < 68) { // a run of consecutive rows (one record, may span blocks)
                    XStore_Proto::RangeInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (uint64_t i = 0; i < 1 + rng() % 30 && target + i < rows; ++i) {
                        XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
                        e->set_timestamp(Epoch(target + i));
                        *e->mutable_data() = ValueRow(value(), kWidth);
                    }
                    XStore_Proto::RangeInsert_REP rep;
                    w.RangeInsert(&req, &rep);
                } else if (op < 71) { // duplicate timestamps inside one request
                    XStore_Proto::BatchInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (int i = 0; i < 2; ++i) {
                        XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                        e->set_timestamp(Epoch(target));
                        *e->mutable_data() = ValueRow(value(), kWidth);
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    w.BatchInsert(&req, &rep);
                } else if (op < 73) {
                    static_cast<void>(w.UnaryDelete(Epoch(target), db));
                } else if (op < 75) {
                    XStore_Proto::BatchDelete_REQ req;
                    req.set_dbname(db);
                    for (uint64_t i = 0; i < 3 && target + i * 50 < rows; ++i) req.add_timestamp(Epoch(target + i * 50));
                    static_cast<void>(w.BatchDelete(&req));
                } else if (op < 77) { // a range: one whole block (the reset path) or a cut range
                    uint64_t lo = target / rpb * rpb, hi = lo + rpb - 1;
                    if (op == 76) {
                        lo = target;
                        hi = std::min(rows - 1, target + rng() % 300);
                    }
                    static_cast<void>(w.RangeDelete(Epoch(std::max<uint64_t>(lo, 1)), Epoch(hi), db));
                } else {
                    std::this_thread::sleep_for(std::chrono::microseconds(200));
                    continue;
                }
                ++writes;
            }
            // Everything this thread enqueued (ASYNC included) is applied before the final comparison.
            WaitForWalDrained(w, db, Epoch(rows + 10 + static_cast<uint64_t>(t)));
        });
    }

    for (int q = 0; q < 2; ++q) {
        threads.emplace_back([&, q] {
            XStore r;
            std::mt19937_64 rng(777 + q);
            const int qfd = open(data_path.c_str(), O_RDONLY);
            ASSERT_GE(qfd, 0);
            const std::vector<int> columns = {0, 7, 39, 63, 64, 65, 69};
            while (running.load()) {
                // Whole blocks (zone-map pushdown) or a cut range, up to 12 blocks.
                const uint64_t first_block = rng() % kBlocks;
                const uint64_t nblocks = 1 + rng() % std::min<uint64_t>(12, kBlocks - first_block);
                uint64_t lo = first_block * rpb, hi = (first_block + nblocks) * rpb - 1;
                if (rng() % 3 == 0) {
                    lo += rng() % rpb;
                    hi = std::max(lo, hi - rng() % rpb);
                }
                const int c = columns[rng() % columns.size()];
                const unsigned op = static_cast<unsigned>(rng() % 9);
                FileRangeLockGuard hold(data_path, lo * kRowLength, (hi - lo + 1) * kRowLength, kRead);
                const auto data = ReadRows(qfd, lo, hi, kRowLength);
                std::vector<std::pair<uint64_t, double>> values;
                for (const auto &[row_index, row] : data) {
                    double v = 0;
                    if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) values.emplace_back(row_index, v);
                }
                double sum = 0, sum_sq = 0, mn = INFINITY, mx = -INFINITY;
                for (const auto &[row_index, v] : values) {
                    sum += v;
                    sum_sq += v * v;
                    mn = std::min(mn, v);
                    mx = std::max(mx, v);
                }
                const double n = static_cast<double>(values.size());
                const double variance = values.size() > 1 ? std::max(0.0, (sum_sq - sum * sum / n) / (n - 1.0)) : 0.0;
                const auto bad = [&](const std::string &what) {
                    if (mismatches.fetch_add(1) < 10) {
                        ADD_FAILURE() << what << " op " << op << " column " << c << " rows [" << lo << ", " << hi
                                      << "] frontier " << frontier() << " valid " << values.size();
                    }
                };
                const auto near = [](double a, double b) { return std::fabs(a - b) <= 1e-9 * std::max(1.0, std::fabs(b)); };
                const bool success_expected = !values.empty();
                const unsigned short col = static_cast<unsigned short>(c);
                switch (op) {
                    case 0: {
                        XStore_Proto::Min_REP rep;
                        r.MinAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult().doubledata() != mn)) bad("MIN " + rep.opstatus());
                        break;
                    }
                    case 1: {
                        XStore_Proto::Max_REP rep;
                        r.MaxAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult().doubledata() != mx)) bad("MAX " + rep.opstatus());
                        break;
                    }
                    case 2: {
                        XStore_Proto::Sum_REP rep;
                        r.SumAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && rep.opresult() != sum)) bad("SUM " + rep.opstatus());
                        break;
                    }
                    case 3: {
                        XStore_Proto::Avg_REP rep;
                        r.AvgAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && !near(rep.opresult(), sum / n))) bad("AVG " + rep.opstatus());
                        break;
                    }
                    case 4: {
                        XStore_Proto::Variance_REP rep;
                        r.VarianceAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != (values.size() > 1) ||
                            (values.size() > 1 && !near(rep.opresult(), variance))) bad("VARIANCE " + rep.opstatus());
                        break;
                    }
                    case 5: {
                        XStore_Proto::Stddev_REP rep;
                        r.StddevAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != (values.size() > 1) ||
                            (values.size() > 1 && !near(rep.opresult(), std::sqrt(variance)))) bad("STDDEV " + rep.opstatus());
                        break;
                    }
                    case 6: {
                        XStore_Proto::Ohlc_REP rep;
                        r.OhlcAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if ((rep.opstatus().rfind("SUCCESS", 0) == 0) != success_expected ||
                            (success_expected && (rep.open() != values.front().second || rep.close() != values.back().second ||
                                                  rep.high() != mx || rep.low() != mn))) {
                            bad("OHLC " + rep.opstatus());
                        }
                        break;
                    }
                    case 7: {
                        XStore_Proto::Exists_REP rep;
                        r.ExistsAggregate(Epoch(lo), Epoch(hi), db, col, &rep);
                        if (rep.opstatus().rfind("SUCCESS", 0) != 0 || rep.exists() != success_expected) bad("EXISTS " + rep.opstatus());
                        break;
                    }
                    default: {
                        if (!success_expected) break;
                        const double target = values[rng() % values.size()].second;
                        std::set<unsigned long long> want;
                        for (const auto &[row_index, v] : values) {
                            if (v == target) want.insert(Epoch(row_index));
                        }
                        std::string buf;
                        r.FindByValue(Epoch(lo), Epoch(hi), db, c, FDouble(target), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                                      [&](std::string &&chunk, bool) { buf += chunk; });
                        XStore_Proto::RangeQuery_REP rep;
                        std::set<unsigned long long> got;
                        if (rep.ParseFromString(buf)) {
                            for (const auto &e : rep.queryresponse()) got.insert(e.timestamp());
                        }
                        if (got != want) bad("FINDBYVALUE got " + std::to_string(got.size()) + " want " + std::to_string(want.size()));
                        break;
                    }
                }
                ++queries;
            }
            close(qfd);
        });
    }

    std::this_thread::sleep_for(milliseconds(400)); // before: the 64-wide index under traffic
    const int queries_before = queries.load();
    const int writes_before = writes.load();
    XStore::LiveWidenStats stats;
    // The floors below must not depend on how fast the widen is: it is held (a WRITE lock on one row in the middle of
    // the year, which the builder can only wait for) until the traffic floors are met during it, 20 s at the latest
    // (the floors then fail and say so). Writers or queries that touch the row wait for it meanwhile.
    const uint64_t held_row = rows / 2 + 1;
    FileRangeLock &held = FileLockRegistry::Get(data_path);
    held.LockRange(held_row * kRowLength, kRowLength, kWrite);
    std::thread releaser([&] {
        const Clock::time_point t0 = Clock::now();
        while (!zmap.Rebuilding() && MsSince(t0) < 20000) std::this_thread::sleep_for(milliseconds(1));
        const int w0 = writes.load(), q0 = queries.load(); // counted from the begin of the rebuild
        while ((writes.load() - w0 <= 30 || queries.load() - q0 <= 10) && MsSince(t0) < 20000) {
            std::this_thread::sleep_for(milliseconds(5));
        }
        held.UnlockRange(held_row * kRowLength, kRowLength, kWrite);
    });
    const Clock::time_point widen_start = Clock::now();
    const uint64_t served_before = zmap.ServedFromOldForTests();
    const Result result = store.LiveWidenOneYear(path, kYear, kRowLength, kWidth, true, true, nullptr, &stats, workers,
                                                 group_bytes, governor, nullptr, keep_old);
    releaser.join();
    const uint64_t served_from_old = zmap.ServedFromOldForTests() - served_before;
    const int64_t widen_ms = MsSince(widen_start);
    const int queries_during = queries.load() - queries_before;
    const int writes_during = writes.load() - writes_before;
    std::this_thread::sleep_for(milliseconds(400)); // after: the widened index under traffic
    running.store(false);
    for (std::thread &t : threads) t.join();
    EXPECT_EQ(result, Result::kDone);
    EXPECT_EQ(mismatches.load(), 0);
    // Writes that raced the builder, counted from the widen's start only. The held row above keeps the widen running
    // until these floors are met (before 2026-10-17 they relied on the widen's own length: 0.3-1.1 s, 83-416 writes).
    EXPECT_GT(writes_during, 30) << "too little concurrent traffic during the widen to mean anything";
    EXPECT_GT(queries_during, 10) << "too few queries during the widen";
    // Keep-old: blocks past the frontier were answered from the old index during the build instead of being scanned.
    // A weak check (random queries); KeepOldAnswersOldColumnsFromTheOldIndexWhileTheWidenIsHeld is the strong proof.
    if (keep_old) {
        EXPECT_GT(served_from_old, 0u);
    } else {
        EXPECT_EQ(served_from_old, 0u);
    }
    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_EQ(catidx.OldWidth(), 0u);
    std::printf("[live widen oracle] keep_old %d: %llu block stats answered from the old index\n", keep_old ? 1 : 0,
                static_cast<unsigned long long>(served_from_old));
    std::printf("[live widen oracle] %u workers: widen %lld ms, %llu chunks, %llu rows, %llu timeouts, max hold %llu us; %d writes (%d during), "
                "%d queries (%d during)\n", workers, static_cast<long long>(widen_ms), static_cast<unsigned long long>(stats.chunks),
                static_cast<unsigned long long>(stats.rows_folded), static_cast<unsigned long long>(stats.lock_timeouts),
                static_cast<unsigned long long>(stats.max_lock_hold_us), writes.load(), writes_during, queries.load(), queries_during);

    ExpectIndexesMatchAStartupRebuild(store, db, kWidth, kBlocks, 1000000);
}

// Every tracked column of every exact block of db's kYear zone map equals a startup rebuild of the final data (an
// inexact block only has to bound it), and the bloom holds every present value. `blocks` covers the data.
void XStoreLiveWidenTest::ExpectIndexesMatchAStartupRebuild(XStore &store, const std::string &db, int width, uint64_t blocks,
                                                            uint64_t min_bloom_checked) {
    const int kWidth = width;
    const uint64_t kBlocks = blocks;
    const uint64_t rpb = Rpb();
    const uint64_t rows = kBlocks * rpb;
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(DbPath(db), kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength);
    // Capture the live index before InitDB (which rebuilds this DB's index again: it is dirty after the writes).
    std::vector<std::vector<ZoneMapEntry>> live(kBlocks);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto st = zmap.TryGetBlockStats(b);
        ASSERT_TRUE(st.has_value()) << "block " << b;
        ASSERT_EQ(st->size(), static_cast<size_t>(kWidth));
        live[b] = *st;
    }
    {   // Bloom: every present value of every tracked column.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        uint64_t checked = 0, misses = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, rows - 1, kRowLength)) {
            for (int c = 0; c < std::min(kWidth, row.fielddata_size()); ++c) {
                if (row.fielddata(c).oneOfData_case() == XStore_Proto::FData::ONEOFDATA_NOT_SET) continue;
                ++checked;
                if (!catidx.MightContain(BlockIndexForRow(r, rpb), static_cast<uint32_t>(c), EncodeFDataForCatIdx(row.fielddata(c)))) {
                    if (++misses < 5) ADD_FAILURE() << "bloom misses row " << r << " column " << c;
                }
            }
        }
        close(fd);
        EXPECT_EQ(misses, 0u);
        EXPECT_GT(checked, min_bloom_checked);
    }

    BuildStartupReference(store, db, db + "REF", kWidth);
    ZoneMapFile &oracle = XStore::GetOrOpenZoneMap(DbPath(db + "REF"), kYear, kRowLength);
    uint64_t exact = 0, inexact = 0;
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto o = oracle.TryGetBlockStats(b);
        ASSERT_TRUE(o.has_value());
        ASSERT_EQ(o->size(), static_cast<size_t>(kWidth));
        for (int c = 0; c < kWidth; ++c) {
            SCOPED_TRACE("block " + std::to_string(b) + " column " + std::to_string(c));
            const ZoneMapEntry &oe = (*o)[static_cast<size_t>(c)];
            const ZoneMapEntry &le = live[b][static_cast<size_t>(c)];
            ASSERT_FALSE(ZoneMapEntryInexact(oe));
            if (ZoneMapEntryInexact(le)) {
                ++inexact;
                if (oe.valid_count > 0) { // pruning still uses min/max: too wide is fine, too narrow is not
                    EXPECT_LE(le.min_value, oe.min_value);
                    EXPECT_GE(le.max_value, oe.max_value);
                }
                continue;
            }
            ++exact;
            EXPECT_EQ(le.valid_count, oe.valid_count);
            if (oe.valid_count == 0) continue;
            EXPECT_EQ(le.min_value, oe.min_value);
            EXPECT_EQ(le.max_value, oe.max_value);
            EXPECT_NEAR(le.sum_value, oe.sum_value, 1e-6 * std::max(1.0, std::fabs(oe.sum_value)));
            EXPECT_EQ(le.first_value, oe.first_value);
            EXPECT_EQ(le.last_value, oe.last_value);
        }
    }
    EXPECT_GT(exact, static_cast<uint64_t>(kWidth) * 10) << "test too weak: almost every block ended inexact";
    std::printf("[live widen oracle] %llu exact and %llu inexact (block, column) entries compared\n",
                static_cast<unsigned long long>(exact), static_cast<unsigned long long>(inexact));
}

TEST_F(XStoreLiveWidenTest, AnswersStayExactUnderConcurrentWritesAndTheResultMatchesAStartupRebuild) {
    RunConcurrentOracle("LWLIVE", 1, XStoreTunables::LIVE_WIDEN_GROUP_BYTES);
}

TEST_F(XStoreLiveWidenTest, KeepOldAnswersStayExactUnderConcurrentWritesAndTheResultMatchesAStartupRebuild) {
    RunConcurrentOracle("LWKEEPOLD", 1, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, /*keep_old=*/true);
}

// ---- 2b. Per-run WAL apply locks: scattered records -----------------------------------------------------------------

// A scattered record WRITE-locks only its runs, so another writer, a reader or the builder in its gaps runs beside it.
// Scattered SYNC/BYPASS BatchInserts of fresh rows (row % 4 == 0) race one-row deletes of seeded rows in their gaps
// (row % 4 == 2, first 8 blocks only, so the other blocks stay exact; the inserts touch those blocks too), range queries and, if asked, a live widen
// (held until inserts ran during it, as in RunConcurrentOracle). The insert and delete sets are disjoint, so the final
// data must equal any serial replay; the zone map and the catidx must equal a startup rebuild.
void XStoreLiveWidenTest::RunScatteredInsertsWithGapDeletes(const std::string &db, Widen widen) {
    XStore store;
    const int width = widen == Widen::kNone ? 64 : 70;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 32;
    const uint64_t rows = kBlocks * rpb;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::map<uint64_t, XStore_Proto::RowData> expected;
    expected[0] = ValueRow(0, 10); // index files born 64 wide
    Insert(store, db, {{Epoch(0), expected[0]}});
    std::vector<uint64_t> deletable;
    {
        Rows seed;
        for (uint64_t r = 2; r < rows; r += 4) {
            expected[r] = ValueRow(r * 7 % 100000, width);
            seed.emplace_back(Epoch(r), expected[r]);
            if (r < 8 * rpb) deletable.push_back(r);
        }
        Insert(store, db, seed);
    }
    std::mt19937_64 rng(31337);
    std::vector<uint64_t> fresh;
    for (uint64_t r = 4; r < rows; r += 4) {
        fresh.push_back(r);
        expected[r] = ValueRow(100000 + r, width);
    }
    std::shuffle(fresh.begin(), fresh.end(), rng);
    std::shuffle(deletable.begin(), deletable.end(), rng);
    deletable.resize(deletable.size() / 2);
    for (uint64_t r : deletable) expected.erase(r);

    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    constexpr size_t kBatchRows = 110;
    std::atomic<int> insert_batches{0}, deletes{0}, queries{0}, writers_left{3};
    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 2; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            for (size_t k = t; k * kBatchRows < fresh.size(); k += 2) {
                XStore_Proto::BatchInsert_REQ req;
                req.set_dbname(db);
                req.set_durabilitymode(k / 2 % 2 == 0 ? XStore_Proto::SYNC : XStore_Proto::BYPASS);
                for (size_t i = k * kBatchRows; i < std::min(fresh.size(), (k + 1) * kBatchRows); ++i) {
                    XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                    e->set_timestamp(Epoch(fresh[i]));
                    *e->mutable_data() = expected.at(fresh[i]);
                }
                XStore_Proto::BatchInsert_REP rep;
                w.BatchInsert(&req, &rep);
                EXPECT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
                ++insert_batches;
            }
            --writers_left;
        });
    }
    // One gap row per call (UnaryDelete or a one-row BatchDelete): its lock fits between two insert rows, so it runs
    // beside an apply that folds into the same zone-map block (the delete flags the block inexact meanwhile).
    threads.emplace_back([&] {
        XStore w;
        // Paced by the inserts, so the deletes spread over the whole insert phase instead of finishing first.
        const size_t total_batches = (fresh.size() + kBatchRows - 1) / kBatchRows;
        for (size_t i = 0; i < deletable.size(); ++i) {
            while (static_cast<size_t>(insert_batches.load()) < i * total_batches / deletable.size() && writers_left.load() > 1) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            std::string status;
            if (i % 2 == 0) {
                status = w.UnaryDelete(Epoch(deletable[i]), db).deletestatus();
            } else {
                XStore_Proto::BatchDelete_REQ req;
                req.set_dbname(db);
                req.add_timestamp(Epoch(deletable[i]));
                status = w.BatchDelete(&req).deletestatus();
            }
            EXPECT_NE(status.find("[INFO]"), std::string::npos) << status;
            ++deletes;
        }
        --writers_left;
    });
    threads.emplace_back([&] {
        XStore r;
        std::mt19937_64 qrng(99);
        while (writers_left.load() > 0) {
            const uint64_t lo = qrng() % rows;
            const uint64_t hi = std::min(rows - 1, lo + qrng() % (2 * rpb));
            const XStore_Proto::RangeQuery_REP rep = RangeQuery(r, Epoch(lo), Epoch(hi), db);
            EXPECT_EQ(static_cast<uint64_t>(rep.queryresponse_size()), hi - lo + 1) << rep.querystatus();
            ++queries;
        }
    });

    if (widen != Widen::kNone) {
        const uint64_t held_row = rows / 2 + 1; // row % 4 == 1: no writer's row; inside nearly every insert's span
        FileRangeLock &held = FileLockRegistry::Get(data_path);
        held.LockRange(held_row * kRowLength, kRowLength, kWrite);
        std::thread releaser([&] {
            const Clock::time_point t0 = Clock::now();
            while (!zmap.Rebuilding() && MsSince(t0) < 20000) std::this_thread::sleep_for(milliseconds(1));
            const int b0 = insert_batches.load();
            while (insert_batches.load() - b0 < 10 && writers_left.load() > 1 && MsSince(t0) < 20000) {
                std::this_thread::sleep_for(milliseconds(2));
            }
            held.UnlockRange(held_row * kRowLength, kRowLength, kWrite);
        });
        XStore::LiveWidenStats stats;
        const int b_before = insert_batches.load();
        const Result result = store.LiveWidenOneYear(path, kYear, kRowLength, static_cast<uint32_t>(width), true, true,
                                                     nullptr, &stats, 4, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr,
                                                     nullptr, widen == Widen::kKeepOld);
        releaser.join();
        const int during = insert_batches.load() - b_before;
        EXPECT_EQ(result, Result::kDone);
        EXPECT_GT(during, 0) << "no scattered insert ran during the widen";
        std::printf("[scattered runs] widen keep_old %d: %d insert batches during, %llu chunks, %llu timeouts\n",
                    widen == Widen::kKeepOld ? 1 : 0, during, static_cast<unsigned long long>(stats.chunks),
                    static_cast<unsigned long long>(stats.lock_timeouts));
    }
    for (std::thread &t : threads) t.join();
    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_GT(queries.load(), 0);
    std::printf("[scattered runs] %d insert batches, %d one-row deletes, %d range queries\n", insert_batches.load(),
                deletes.load(), queries.load());

    {   // Final data == the serial replay.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        const auto got = ReadRows(fd, 0, rows - 1, kRowLength);
        close(fd);
        EXPECT_EQ(got.size(), expected.size());
        uint64_t wrong = 0;
        for (const auto &[r, row] : got) {
            const auto it = expected.find(r);
            if (it == expected.end() || it->second.SerializeAsString() != row.SerializeAsString()) {
                if (++wrong < 5) ADD_FAILURE() << "row " << r << (it == expected.end() ? " should be absent" : " has the wrong value");
            }
        }
        EXPECT_EQ(wrong, 0u);
    }
    ExpectIndexesMatchAStartupRebuild(store, db, width, kBlocks, 100000);
}

TEST_F(XStoreLiveWidenTest, ScatteredInsertsBesideGapDeletesAndQueriesMatchASerialReplayAndAStartupRebuild) {
    RunScatteredInsertsWithGapDeletes("RUNLOCKMIX", Widen::kNone);
}

TEST_F(XStoreLiveWidenTest, ScatteredInsertsDuringAWidenMatchASerialReplayAndAStartupRebuild) {
    RunScatteredInsertsWithGapDeletes("RUNLOCKWIDEN", Widen::kDropOld);
}

TEST_F(XStoreLiveWidenTest, KeepOldScatteredInsertsDuringAWidenMatchASerialReplayAndAStartupRebuild) {
    RunScatteredInsertsWithGapDeletes("RUNLOCKKEEPOLD", Widen::kKeepOld);
}

// Atomicity for readers: one scattered record (110 rows over 20000, one generation value in every row), rewritten
// generation after generation (SYNC through the WAL thread, BYPASS on the caller's thread), while range and batch
// queries over random sub-spans of its rows must see every row of their span at ONE generation.
TEST_F(XStoreLiveWidenTest, ReadersNeverSeeAScatteredRecordHalfApplied) {
    XStore store;
    const std::string db = "RUNLOCKATOM";
    CreateTestDb(store, db, XStore_Proto::SECOND, 64);
    std::mt19937_64 rng(2026);
    std::set<uint64_t> picked;
    while (picked.size() < 110) picked.insert(rng() % 20000);
    const std::vector<uint64_t> record_rows(picked.begin(), picked.end());
    const auto write_generation = [&](XStore &s, int64_t gen, XStore_Proto::WalDurabilityMode mode) {
        XStore_Proto::BatchInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(mode);
        for (uint64_t r : record_rows) {
            XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = MakeRow({FInt64(gen), FInt64(static_cast<int64_t>(r))});
        }
        XStore_Proto::BatchInsert_REP rep;
        s.BatchInsert(&req, &rep);
        return rep.insertstatus().find("successfully") != std::string::npos;
    };
    ASSERT_TRUE(write_generation(store, 0, XStore_Proto::SYNC));

    constexpr int64_t kGenerations = 300;
    std::atomic<bool> done{false};
    std::atomic<uint64_t> mixes{0}, reads{0}, newest_seen{0};
    std::vector<std::thread> readers;
    for (unsigned q = 0; q < 4; ++q) {
        readers.emplace_back([&, q] {
            XStore r;
            std::mt19937_64 qrng(500 + q);
            while (!done.load()) {
                const size_t i = qrng() % record_rows.size();
                const size_t j = i + qrng() % (record_rows.size() - i);
                std::set<int64_t> gens;
                size_t present = 0;
                const auto take = [&](const XStore_Proto::UnaryQuery_REP &e) {
                    if (e.data().fielddata_size() == 0) return;
                    ++present;
                    gens.insert(e.data().fielddata(0).int64data());
                };
                if (q % 2 == 0) {
                    const XStore_Proto::RangeQuery_REP rep = RangeQuery(r, Epoch(record_rows[i]), Epoch(record_rows[j]), db);
                    for (const auto &e : rep.queryresponse()) take(e);
                } else {
                    XStore_Proto::BatchQuery_REQ req;
                    req.set_dbname(db);
                    for (size_t k = i; k <= j; ++k) req.add_timestamp(Epoch(record_rows[k]));
                    const XStore_Proto::BatchQuery_REP rep = BatchQuery(r, &req);
                    for (const auto &e : rep.queryresponse()) take(e);
                }
                if (present != j - i + 1 || gens.size() != 1) {
                    if (mixes.fetch_add(1) < 5) {
                        ADD_FAILURE() << (q % 2 == 0 ? "range" : "batch") << " query over record rows " << i << ".." << j
                                      << " saw " << present << " rows at " << gens.size() << " generations";
                    }
                } else {
                    const uint64_t g = static_cast<uint64_t>(*gens.begin());
                    uint64_t seen = newest_seen.load();
                    while (g > seen && !newest_seen.compare_exchange_weak(seen, g)) {}
                }
                ++reads;
            }
        });
    }
    {
        XStore w;
        for (int64_t gen = 1; gen <= kGenerations; ++gen) {
            if (!write_generation(w, gen, gen % 2 == 0 ? XStore_Proto::SYNC : XStore_Proto::BYPASS)) {
                ADD_FAILURE() << "generation " << gen << " failed";
                break;
            }
        }
    }
    done.store(true);
    for (std::thread &t : readers) t.join();
    EXPECT_EQ(mixes.load(), 0u);
    EXPECT_GT(reads.load(), 100u);
    EXPECT_GT(newest_seen.load(), 0u) << "no reader ever saw a rewrite: the test proves little";
    std::printf("[scattered atomicity] %llu reads, newest generation seen %llu of %lld\n",
                static_cast<unsigned long long>(reads.load()), static_cast<unsigned long long>(newest_seen.load()),
                static_cast<long long>(kGenerations));
}

// WAL_APPLY_MAX_RUN_LOCKS: a record of exactly the cap's runs locks them one by one (a READ held on a gap row does not
// stop it), one more run locks the whole span (the held READ stops it until released). Both paths, SYNC and BYPASS,
// leave the same data and the same index files as the same rows applied as small records.
TEST_F(XStoreLiveWidenTest, ARecordPastTheRunCapLocksItsWholeSpanAndGivesTheSameResult) {
    constexpr uint64_t kCap = XStoreTunables::WAL_APPLY_MAX_RUN_LOCKS;
    // `runs` isolated rows (every other row) from `first`.
    const auto every_other = [](uint64_t first, uint64_t runs, uint64_t base) {
        Rows out;
        for (uint64_t i = 0; i < runs; ++i) out.emplace_back(Epoch(first + 2 * i), ValueRow(base + i, 10));
        return out;
    };
    constexpr uint64_t kStride = 2 * (kCap + 1) + 10; // rows from one record's start to the next one's
    const std::array<std::pair<Rows, XStore_Proto::WalDurabilityMode>, 4> records = {{
        {every_other(10, kCap, 1000), XStore_Proto::SYNC},
        {every_other(10 + kStride, kCap, 1000 + kStride), XStore_Proto::BYPASS},
        {every_other(10 + 2 * kStride, kCap + 1, 1000 + 2 * kStride), XStore_Proto::SYNC},
        {every_other(10 + 3 * kStride, kCap + 1, 1000 + 3 * kStride), XStore_Proto::BYPASS},
    }};
    XStore store;
    const std::string db = "RUNLOCKCAP", ref = "RUNLOCKCAPSMALL";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    Insert(store, ref, {{Epoch(0), ValueRow(0, 10)}});
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    for (size_t k = 0; k < records.size(); ++k) {
        SCOPED_TRACE("record " + std::to_string(k));
        const Rows &record = records[k].first;
        const XStore_Proto::WalDurabilityMode mode = records[k].second;
        const uint64_t gap_row = record.front().first - Jan1(kYear) + 1;
        lock.LockRange(gap_row * kRowLength, kRowLength, kRead);
        auto insert = std::async(std::launch::async, [&] {
            XStore w;
            Insert(w, db, record, mode, record.size()); // one record
        });
        if (record.size() <= kCap) {
            const bool ready = insert.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
            EXPECT_TRUE(ready) << "a record at the cap waited for a READ in its gaps";
            lock.UnlockRange(gap_row * kRowLength, kRowLength, kRead);
        } else {
            WaitForWaitingWriters(lock, 1); // the apply waits as a writer on the gap READ (not merely slow)
            EXPECT_EQ(insert.wait_for(milliseconds(0)), std::future_status::timeout)
                << "a record past the cap did not lock its whole span";
            lock.UnlockRange(gap_row * kRowLength, kRowLength, kRead);
        }
        ASSERT_EQ(insert.wait_for(std::chrono::seconds(10)), std::future_status::ready);
        insert.get();
        for (size_t from = 0; from < record.size(); from += 100) {
            Insert(store, ref, Rows(record.begin() + static_cast<std::ptrdiff_t>(from),
                                    record.begin() + static_cast<std::ptrdiff_t>(std::min(record.size(), from + 100))), mode);
        }
    }
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    const int fd = open(YearPath(db, ".XSTORE").c_str(), O_RDONLY);
    const int ref_fd = open(YearPath(ref, ".XSTORE").c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_GE(ref_fd, 0);
    const auto got = ReadRows(fd, 0, 10 + 4 * kStride, kRowLength);
    const auto want = ReadRows(ref_fd, 0, 10 + 4 * kStride, kRowLength);
    close(fd);
    close(ref_fd);
    ASSERT_EQ(got.size(), 1 + 4 * kCap + 2);
    ASSERT_EQ(got.size(), want.size());
    for (size_t i = 0; i < got.size(); ++i) {
        ASSERT_EQ(got[i].first, want[i].first);
        ASSERT_EQ(got[i].second.SerializeAsString(), want[i].second.SerializeAsString()) << "row " << got[i].first;
    }
    CleanRestartState(); // both indexes clean on disk
    ExpectSameFilesAs(db, ref);
}

// WAL_APPLY_MAX_RUN_LOCKS_KEEP_OLD_REBUILD (512): while a keep-old rebuild is active on either of the year's index
// files, a record of 513 single-row runs (one over 512, under WAL_APPLY_MAX_RUN_LOCKS) locks its whole span (a READ
// held on a gap row stops it) and records of 3 and exactly 512 runs still lock their runs. Without a rebuild, after
// FinishRebuild and during an in-place rebuild the 513-run record locks its runs. The rebuilds are begun by hand (no
// builder): only the apply's cap choice is under test.
TEST_F(XStoreLiveWidenTest, AKeepOldRebuildLowersTheRunCapOfTheWalApply) {
    constexpr uint64_t kKeepOldCap = XStoreTunables::WAL_APPLY_MAX_RUN_LOCKS_KEEP_OLD_REBUILD;
    constexpr uint64_t kOver = kKeepOldCap + 1;
    static_assert(kKeepOldCap < kOver && kOver <= XStoreTunables::WAL_APPLY_MAX_RUN_LOCKS, "kOver runs: between the caps");
    XStore store;
    const std::string db = "RUNLOCKKEEPOLDCAP";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    uint64_t next_first = 10;
    // One SYNC record of `runs` isolated rows (every other row) while a READ is held on its first gap row: per-run
    // locks get in while the READ is held, the hull waits for it.
    const auto expect_record = [&](uint64_t runs, bool hull) {
        SCOPED_TRACE(std::to_string(runs) + " runs, expect " + (hull ? "the hull" : "per-run locks"));
        const uint64_t first = next_first;
        next_first += 2 * runs + 10;
        Rows record;
        for (uint64_t i = 0; i < runs; ++i) record.emplace_back(Epoch(first + 2 * i), ValueRow(first + i, 10));
        lock.LockRange((first + 1) * kRowLength, kRowLength, kRead);
        auto insert = std::async(std::launch::async, [&] {
            XStore w;
            Insert(w, db, record, XStore_Proto::SYNC, record.size()); // one record
        });
        if (hull) {
            WaitForWaitingWriters(lock, 1); // the apply waits as a writer on the gap READ (not merely slow)
            EXPECT_EQ(insert.wait_for(milliseconds(0)), std::future_status::timeout) << "did not lock its whole span";
        } else {
            EXPECT_EQ(insert.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "waited for a gap READ";
        }
        lock.UnlockRange((first + 1) * kRowLength, kRowLength, kRead);
        ASSERT_EQ(insert.wait_for(std::chrono::seconds(10)), std::future_status::ready);
        insert.get();
    };

    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_EQ(catidx.OldWidth(), 0u);
    expect_record(kOver, false); // no rebuild

    // Only the catidx keeps an old index: the lower cap already applies (either file is enough).
    ASSERT_EQ(catidx.BeginLiveRebuild(70, /*keep_old=*/true), BeginRebuildResult::kStarted);
    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_NE(catidx.OldWidth(), 0u);
    expect_record(kOver, true);
    ASSERT_TRUE(catidx.FinishRebuild());
    EXPECT_EQ(catidx.OldWidth(), 0u);
    // Only the zone map keeps one.
    ASSERT_EQ(zmap.BeginLiveRebuild(70, /*keep_old=*/true), BeginRebuildResult::kStarted);
    EXPECT_NE(zmap.OldWidth(), 0u);
    EXPECT_EQ(catidx.OldWidth(), 0u);
    expect_record(kOver, true);
    ASSERT_TRUE(zmap.FinishRebuild());

    // Both files keep-old.
    ASSERT_EQ(zmap.BeginLiveRebuild(80, /*keep_old=*/true), BeginRebuildResult::kStarted);
    ASSERT_EQ(catidx.BeginLiveRebuild(80, /*keep_old=*/true), BeginRebuildResult::kStarted);
    EXPECT_NE(zmap.OldWidth(), 0u);
    EXPECT_NE(catidx.OldWidth(), 0u);
    expect_record(3, false);
    expect_record(kKeepOldCap, false);
    expect_record(kOver, true);
    ASSERT_TRUE(zmap.FinishRebuild());
    ASSERT_TRUE(catidx.FinishRebuild());
    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_EQ(catidx.OldWidth(), 0u);
    expect_record(kOver, false); // after FinishRebuild

    // In-place rebuild of both files: no old index, the normal cap.
    ASSERT_EQ(zmap.BeginLiveRebuild(90, /*keep_old=*/false), BeginRebuildResult::kStarted);
    ASSERT_EQ(catidx.BeginLiveRebuild(90, /*keep_old=*/false), BeginRebuildResult::kStarted);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_EQ(zmap.OldWidth(), 0u);
    EXPECT_EQ(catidx.OldWidth(), 0u);
    expect_record(kOver, false);
    ASSERT_TRUE(zmap.FinishRebuild());
    ASSERT_TRUE(catidx.FinishRebuild());
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// ---- 2c. Per-run locks of BatchQuery (READ) and BatchDelete (WRITE) -------------------------------------------------

// BatchQuery READ-locks only the rows it reads: a WRITE held on a gap row between queried rows does not stop it, one held
// on a queried row does until released. BATCH_QUERY_MAX_RUN_LOCKS runs still lock run by run, one more locks the
// min-to-max span (the gap WRITE stops it). Every answer is in request order (sort_time off), with duplicates repeated
// and holes omitted, exactly the rows present.
TEST_F(XStoreLiveWidenTest, BatchQueryLocksOnlyItsRowsAndAnswersAsBefore) {
    constexpr uint64_t kCap = XStoreTunables::BATCH_QUERY_MAX_RUN_LOCKS;
    XStore store;
    const std::string db = "BQRUNLOCK";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::map<uint64_t, XStore_Proto::RowData> present;
    {
        Rows seed;
        for (uint64_t i = 0; i < kCap + 100; ++i) {
            if (i % 7 == 3) continue;   // a hole
            const uint64_t r = 10 + 2 * i;
            present[r] = ValueRow(r * 3, 10);
            seed.emplace_back(Epoch(r), present[r]);
        }
        Insert(store, db, seed);
    }
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    std::mt19937_64 rng(512);
    const auto query = [&](const std::vector<uint64_t> &rows) {
        return std::async(std::launch::async, [&db, rows] {
            XStore r;
            XStore_Proto::BatchQuery_REQ req;
            req.set_dbname(db);
            for (uint64_t row : rows) req.add_timestamp(Epoch(row));
            return BatchQuery(r, &req, /*sort_time=*/false);
        });
    };
    const auto expect_answer = [&](const std::vector<uint64_t> &rows, const XStore_Proto::BatchQuery_REP &rep) {
        std::vector<uint64_t> want;
        for (uint64_t row : rows) if (present.count(row) != 0) want.push_back(row);
        ASSERT_EQ(static_cast<size_t>(rep.queryresponse_size()), want.size()) << rep.querystatus();
        for (size_t i = 0; i < want.size(); ++i) {
            ASSERT_EQ(rep.queryresponse(static_cast<int>(i)).timestamp(), Epoch(want[i])) << "entry " << i;
            ASSERT_EQ(rep.queryresponse(static_cast<int>(i)).data().SerializeAsString(), present.at(want[i]).SerializeAsString())
                << "entry " << i;
        }
    };
    // `runs` isolated rows (every other row from 10, holes included), shuffled, a few duplicated.
    const auto rows_of = [&](uint64_t runs) {
        std::vector<uint64_t> rows;
        for (uint64_t i = 0; i < runs; ++i) rows.push_back(10 + 2 * i);
        for (uint64_t i = 0; i < 5; ++i) rows.push_back(rows[rng() % runs]);
        std::shuffle(rows.begin(), rows.end(), rng);
        return rows;
    };
    const auto wait_ready = [](auto &f, const char *what) {
        if (f.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            ADD_FAILURE() << what;
            AbortAfterAFailedWait();
        }
    };
    constexpr uint64_t kGapRow = 13;   // between queried rows 12 and 14
    // A bounded WRITE on `row`: a run the store leaked would otherwise block this thread for good.
    const auto hold_write = [&](uint64_t row) {
        if (!lock.TryLockRangeFor(row * kRowLength, kRowLength, kWrite, std::chrono::seconds(10))) {
            ADD_FAILURE() << "a lock leaked";
            AbortAfterAFailedWait();
        }
    };

    {   // A WRITE on a gap row: no wait.
        const std::vector<uint64_t> rows = rows_of(40);
        hold_write(kGapRow);
        auto f = query(rows);
        EXPECT_EQ(f.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "waited for a WRITE in its gaps";
        lock.UnlockRange(kGapRow * kRowLength, kRowLength, kWrite);
        wait_ready(f, "the query never finished");
        expect_answer(rows, f.get());
    }
    {   // A WRITE on a queried row: waits until it goes.
        const std::vector<uint64_t> rows = rows_of(40);
        hold_write(14);
        auto f = query(rows);
        EXPECT_EQ(f.wait_for(milliseconds(300)), std::future_status::timeout) << "read a row a WRITE holds";
        lock.UnlockRange(14 * kRowLength, kRowLength, kWrite);
        wait_ready(f, "the query never got in after the WRITE went");
        expect_answer(rows, f.get());
    }
    for (const uint64_t runs : {kCap, kCap + 1}) {
        SCOPED_TRACE(std::to_string(runs) + " runs");
        const std::vector<uint64_t> rows = rows_of(runs);
        hold_write(kGapRow);
        auto f = query(rows);
        if (runs <= kCap) {
            EXPECT_EQ(f.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "at the cap: waited for a gap WRITE";
        } else {
            EXPECT_EQ(f.wait_for(milliseconds(300)), std::future_status::timeout) << "past the cap: did not lock its span";
        }
        lock.UnlockRange(kGapRow * kRowLength, kRowLength, kWrite);
        wait_ready(f, "the query never finished");
        expect_answer(rows, f.get());
    }
    // An already-sorted request (the lock list needs no sort) and the same rows shuffled, small and past the
    // thread_local release bound (70000 timestamps, mostly holes): the same answers, in request order.
    for (const uint64_t runs : {uint64_t{40}, uint64_t{70000}}) {
        SCOPED_TRACE(std::to_string(runs) + " rows, sorted then shuffled");
        std::vector<uint64_t> rows = rows_of(runs);
        std::sort(rows.begin(), rows.end());
        for (int pass = 0; pass < 2; ++pass) {
            auto f = query(rows);
            wait_ready(f, "the query never finished");
            expect_answer(rows, f.get());
            std::shuffle(rows.begin(), rows.end(), rng);
        }
    }
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
}

// BatchDelete WRITE-locks run by run up to BATCH_DELETE_MAX_RUN_LOCKS runs (1 today: the span lock, so a gap READ stops
// every delete of two or more runs) and the span past it: a READ held on a gap row stops a delete exactly when it has
// more runs than the cap, one held on a deleted row always does until released. The data and the index files equal a
// reference DB given the same deletes as one-row BatchDeletes.
TEST_F(XStoreLiveWidenTest, BatchDeleteLocksOnlyItsRowsAndDeletesAsBefore) {
    constexpr uint64_t kCap = XStoreTunables::BATCH_DELETE_MAX_RUN_LOCKS;
    XStore store;
    const std::string db = "BDRUNLOCK", ref = "BDRUNLOCKREF";
    constexpr uint64_t kRows = 100 + 2 * (2 * kCap + 1) + 20;
    for (const std::string &d : {db, ref}) {
        CreateTestDb(store, d, XStore_Proto::SECOND, kRowLength);
        Rows seed;
        for (uint64_t r = 0; r < kRows; ++r) seed.emplace_back(Epoch(r), ValueRow(r * 3, 10));
        Insert(store, d, seed);
    }
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    std::mt19937_64 rng(513);
    const auto delete_rows = [&](XStore &s, const std::string &d, const std::vector<uint64_t> &rows) {
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(d);
        for (uint64_t row : rows) req.add_timestamp(Epoch(row));
        const std::string status = s.BatchDelete(&req).deletestatus();
        EXPECT_NE(status.find("[INFO]"), std::string::npos) << status;
    };
    // Deletes `rows` (one request, shuffled) from db while `held` is READ-locked; `waits`: whether it must wait for it.
    const auto expect_delete = [&](std::vector<uint64_t> rows, uint64_t held, bool waits) {
        SCOPED_TRACE(std::to_string(rows.size()) + " rows, held row " + std::to_string(held));
        std::shuffle(rows.begin(), rows.end(), rng);
        if (!lock.TryLockRangeFor(held * kRowLength, kRowLength, kRead, std::chrono::seconds(10))) {
            ADD_FAILURE() << "a lock leaked";
            AbortAfterAFailedWait();
        }
        auto f = std::async(std::launch::async, [&] {
            XStore w;
            delete_rows(w, db, rows);
        });
        if (waits) {
            WaitForWaitingWriters(lock, 1); // the delete waits as a writer on the held READ (not merely slow)
            EXPECT_EQ(f.wait_for(milliseconds(0)), std::future_status::timeout) << "deleted a row a READ holds";
        } else {
            EXPECT_EQ(f.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "waited for a READ in its gaps";
        }
        lock.UnlockRange(held * kRowLength, kRowLength, kRead);
        if (f.wait_for(std::chrono::seconds(10)) != std::future_status::ready) {
            ADD_FAILURE() << "the delete never finished";
            AbortAfterAFailedWait();
        }
        f.get();
        for (uint64_t row : rows) delete_rows(store, ref, {row});
    };
    expect_delete({10, 12, 12, 13, 20}, 11, kCap < 3);        // runs [10], [12, 13], [20]; a duplicate: waits iff it has more runs than the cap
    expect_delete({30, 32, 34}, 32, true);
    std::vector<uint64_t> at_cap, over_cap;
    for (uint64_t i = 0; i < kCap; ++i) at_cap.push_back(100 + 2 * i);
    for (uint64_t i = 0; i < kCap + 1; ++i) over_cap.push_back(100 + 2 * kCap + 10 + 2 * i);
    expect_delete(at_cap, 101, false);
    expect_delete(over_cap, over_cap.front() + 1, true);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);

    const int fd = open(YearPath(db, ".XSTORE").c_str(), O_RDONLY);
    const int ref_fd = open(YearPath(ref, ".XSTORE").c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    ASSERT_GE(ref_fd, 0);
    const auto got = ReadRows(fd, 0, kRows - 1, kRowLength);
    const auto want = ReadRows(ref_fd, 0, kRows - 1, kRowLength);
    close(fd);
    close(ref_fd);
    ASSERT_EQ(got.size(), kRows - (4 + 3 + 2 * kCap + 1)) << "rows 10, 12, 13, 20, 30, 32, 34 and the cap rows gone";
    ASSERT_EQ(got.size(), want.size());
    for (size_t i = 0; i < got.size(); ++i) {
        ASSERT_EQ(got[i].first, want[i].first);
        ASSERT_EQ(got[i].second.SerializeAsString(), want[i].second.SerializeAsString()) << "row " << got[i].first;
    }
    CleanRestartState(); // both indexes clean on disk
    ExpectSameFilesAs(db, ref);
}

// One scattered SYNC writer (110-row records of fresh rows, row % 4 == 0), BatchDeletes of seeded rows (row % 4 == 2,
// first 8 blocks) and two BatchQuery threads over rows nobody writes (row % 4 == 3 present, row % 4 == 1 holes) on one
// year: every lock set is under its cap and the row sets are disjoint, so no WRITE ever waits (the writer-wait counter
// does not move; checked only while BATCH_DELETE_MAX_RUN_LOCKS covers the 40-row deletes, with the span lock the deletes
// do wait for the writer) and every query answer is exact. The final data equals the serial replay, the indexes a startup
// rebuild.
TEST_F(XStoreLiveWidenTest, ScatteredWriterBesideBatchQueriesAndBatchDeletesOnDisjointRowsNeverWaits) {
    XStore store;
    const std::string db = "RUNLOCKBATCHMIX";
    constexpr int kWidth = 64;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 32;
    const uint64_t rows = kBlocks * rpb;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    std::map<uint64_t, XStore_Proto::RowData> expected;
    expected[0] = ValueRow(0, 10); // index files born 64 wide
    Insert(store, db, {{Epoch(0), expected[0]}});
    std::vector<uint64_t> deletable, readable;
    {
        Rows seed;
        for (uint64_t r = 2; r < rows; r += 4) {
            expected[r] = ValueRow(r * 7 % 100000, kWidth);
            seed.emplace_back(Epoch(r), expected[r]);
            if (r < 8 * rpb) deletable.push_back(r);
        }
        Insert(store, db, seed);
    }
    for (uint64_t r = 3; r < rows; r += 4) {
        expected[r] = ValueRow(r * 7 % 100000, kWidth);
        readable.push_back(r);
    }
    {
        Rows seed;
        for (uint64_t r = 3; r < rows; r += 4) seed.emplace_back(Epoch(r), expected[r]);
        Insert(store, db, seed);
    }
    const std::map<uint64_t, XStore_Proto::RowData> readable_rows = [&] {
        std::map<uint64_t, XStore_Proto::RowData> m;
        for (uint64_t r = 3; r < rows; r += 4) m[r] = expected[r];
        return m;
    }();
    std::mt19937_64 rng(4242);
    std::vector<uint64_t> fresh;
    for (uint64_t r = 4; r < rows; r += 4) {
        fresh.push_back(r);
        expected[r] = ValueRow(100000 + r, kWidth);
    }
    std::shuffle(fresh.begin(), fresh.end(), rng);
    std::shuffle(deletable.begin(), deletable.end(), rng);
    deletable.resize(deletable.size() / 2);
    for (uint64_t r : deletable) expected.erase(r);

    const std::string data_path = YearPath(db, ".XSTORE");
    FileRangeLock &lock = FileLockRegistry::Get(data_path);
    const uint64_t waits_before = lock.WriterWaits().waits;
    constexpr size_t kBatchRows = 110, kDeleteRows = 40, kQueryRows = 200;
    std::atomic<int> insert_batches{0}, delete_batches{0}, queries{0}, writers_left{2};
    std::vector<std::thread> threads;
    threads.emplace_back([&] {
        XStore w;
        for (size_t from = 0; from < fresh.size(); from += kBatchRows) {
            XStore_Proto::BatchInsert_REQ req;
            req.set_dbname(db);
            req.set_durabilitymode(XStore_Proto::SYNC);
            for (size_t i = from; i < std::min(fresh.size(), from + kBatchRows); ++i) {
                XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
                e->set_timestamp(Epoch(fresh[i]));
                *e->mutable_data() = expected.at(fresh[i]);
            }
            XStore_Proto::BatchInsert_REP rep;
            w.BatchInsert(&req, &rep);
            EXPECT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
            ++insert_batches;
        }
        --writers_left;
    });
    threads.emplace_back([&] {
        XStore w;
        const size_t total_batches = (fresh.size() + kBatchRows - 1) / kBatchRows;
        const size_t delete_calls = (deletable.size() + kDeleteRows - 1) / kDeleteRows;
        for (size_t k = 0; k < delete_calls; ++k) {
            // Paced by the inserts, so the deletes spread over the whole insert phase.
            while (static_cast<size_t>(insert_batches.load()) < k * total_batches / delete_calls && writers_left.load() > 1) {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
            XStore_Proto::BatchDelete_REQ req;
            req.set_dbname(db);
            for (size_t i = k * kDeleteRows; i < std::min(deletable.size(), (k + 1) * kDeleteRows); ++i) {
                req.add_timestamp(Epoch(deletable[i]));
            }
            const std::string status = w.BatchDelete(&req).deletestatus();
            EXPECT_NE(status.find("[INFO]"), std::string::npos) << status;
            ++delete_batches;
        }
        --writers_left;
    });
    for (unsigned q = 0; q < 2; ++q) {
        threads.emplace_back([&, q] {
            XStore r;
            std::mt19937_64 qrng(700 + q);
            while (writers_left.load() > 0) {
                XStore_Proto::BatchQuery_REQ req;
                req.set_dbname(db);
                std::vector<uint64_t> asked;
                for (size_t i = 0; i < kQueryRows; ++i) {
                    const uint64_t row = readable[qrng() % readable.size()] - (qrng() % 2 == 0 ? 0 : 2); // % 4 == 3 or 1
                    asked.push_back(row);
                    req.add_timestamp(Epoch(row));
                }
                const bool sort_time = q == 0;
                const XStore_Proto::BatchQuery_REP rep = BatchQuery(r, &req, sort_time);
                if (sort_time) std::sort(asked.begin(), asked.end());
                std::vector<uint64_t> want;
                for (uint64_t row : asked) if (readable_rows.count(row) != 0) want.push_back(row);
                bool same = static_cast<size_t>(rep.queryresponse_size()) == want.size();
                for (size_t i = 0; same && i < want.size(); ++i) {
                    same = rep.queryresponse(static_cast<int>(i)).timestamp() == Epoch(want[i]) &&
                           rep.queryresponse(static_cast<int>(i)).data().SerializeAsString() ==
                               readable_rows.at(want[i]).SerializeAsString();
                }
                EXPECT_TRUE(same) << "query " << queries.load() << " (sort_time " << sort_time << ") answered wrong";
                ++queries;
            }
        });
    }
    auto joined = std::async(std::launch::async, [&] { for (std::thread &t : threads) t.join(); });
    if (joined.wait_for(std::chrono::seconds(120)) != std::future_status::ready) {
        ADD_FAILURE() << "a thread is stuck (deadlock or lost wakeup)";
        AbortAfterAFailedWait();
    }
    if (XStoreTunables::BATCH_DELETE_MAX_RUN_LOCKS >= kDeleteRows) {   // with the span lock a delete does wait for the writer
        EXPECT_EQ(lock.WriterWaits().waits, waits_before) << "a WRITE waited: some lock set covered rows beyond its own";
    }
    EXPECT_GT(queries.load(), 10);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    std::printf("[batch run locks] %d insert batches, %d batch deletes, %d batch queries, %llu writer waits\n",
                insert_batches.load(), delete_batches.load(), queries.load(),
                static_cast<unsigned long long>(lock.WriterWaits().waits - waits_before));

    {   // Final data == the serial replay.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        const auto got = ReadRows(fd, 0, rows - 1, kRowLength);
        close(fd);
        EXPECT_EQ(got.size(), expected.size());
        uint64_t wrong = 0;
        for (const auto &[r, row] : got) {
            const auto it = expected.find(r);
            if (it == expected.end() || it->second.SerializeAsString() != row.SerializeAsString()) {
                if (++wrong < 5) ADD_FAILURE() << "row " << r << (it == expected.end() ? " should be absent" : " has the wrong value");
            }
        }
        EXPECT_EQ(wrong, 0u);
    }
    ExpectIndexesMatchAStartupRebuild(store, db, kWidth, kBlocks, 100000);
}

// ---- 3. Starvation: a hot head must not stop the builder ------------------------------------------------------------

// Writers hammer the head rows (the newest rows of the year) through the store, and a relay holds WRITE locks on the
// two rows right after the data so that at EVERY instant one of them is locked (each is free part of the time). A
// builder that needs a range covering both at once (a blocking lock) can never pass; the timed lock shrinks the chunk
// to one row and gets through. Bound: 30 s (the expected time is about 1-2 s); a watchdog stops the hammering at the
// bound so a failing builder ends instead of hanging.
TEST_F(XStoreLiveWidenTest, AHotHeadDoesNotStopTheBuilder) {
    XStore store;
    const std::string db = "LWHOT";
    constexpr uint64_t kHead = 20000;
    SeedNarrowIndexWideRows(store, db, kHead, 70);
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    std::atomic<bool> hammer{true};
    std::vector<std::thread> threads;
    for (int t = 0; t < 2; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            uint64_t i = 0;
            while (hammer.load()) {
                XStore_Proto::UnaryInsert_REP rep;
                ++i;
                w.UnaryInsert(Epoch(kHead - 1 - (i % 64) - 64 * static_cast<uint64_t>(t)), ValueRow(i % 1000, 70), db, &rep,
                              XStore_Proto::BYPASS);
            }
        });
    }
    std::atomic<uint64_t> relay_rounds{0};
    threads.emplace_back([&] {
        FileRangeLock &lock = FileLockRegistry::Get(data_path);
        const uint64_t a = kHead * kRowLength, b = (kHead + 1) * kRowLength;
        lock.LockRange(a, kRowLength, kWrite);
        while (hammer.load()) {
            lock.LockRange(b, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(1));
            lock.UnlockRange(a, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(2));
            lock.LockRange(a, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(1));
            lock.UnlockRange(b, kRowLength, kWrite);
            std::this_thread::sleep_for(milliseconds(2));
            ++relay_rounds;
        }
        lock.UnlockRange(a, kRowLength, kWrite);
    });
    std::this_thread::sleep_for(milliseconds(50));

    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    const Clock::time_point t0 = Clock::now();
    const bool in_time = builder.wait_for(std::chrono::seconds(30)) == std::future_status::ready;
    const int64_t elapsed_ms = MsSince(t0);
    hammer.store(false);
    const Result result = builder.get();
    for (std::thread &t : threads) t.join();
    EXPECT_TRUE(in_time) << "the builder did not finish within 30 s under a hot head";
    EXPECT_EQ(result, Result::kDone);
    EXPECT_GT(stats.lock_timeouts, 0u) << "the hot head never blocked the builder: the test proves nothing";
    EXPECT_GT(relay_rounds.load(), 0u);
    std::printf("[live widen hot head] %lld ms, %llu chunks, %llu timeouts (max %llu in a row)\n",
                static_cast<long long>(elapsed_ms), static_cast<unsigned long long>(stats.chunks),
                static_cast<unsigned long long>(stats.lock_timeouts), static_cast<unsigned long long>(stats.max_consecutive_timeouts));
    ExpectAnswersMatchTheData(store, db, 0, kHead - 1, {0, 65});
}

// Waits until the builder has folded some rows (its zone map's frontier passed `rows`), or fails after 20 s.
void WaitForFrontierPast(ZoneMapFile &zmap, uint64_t rows) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(20);
    for (;;) {
        const uint64_t f = zmap.FrontierForTests();
        if (f != ZoneMapFile::kNoRebuild && f > rows) return;
        ASSERT_LT(Clock::now(), deadline) << "the builder never got past row " << rows;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

// ---- 4. Stop flag ---------------------------------------------------------------------------------------------------

// keep_old=false: this test covers the in-place rebuild (a block past F is not trusted); the keep-old counterpart is
// KeepOldStoppedWidenKeepsTheOldIndexServingUntilTheNextStartRebuilds. The flag defaults to true since 2026-10-17.
TEST_F(XStoreLiveWidenTest, StopMidBuildLeavesADirtyHalfTrustedIndexThatTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWSTOP";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    std::atomic<bool> stop{false};
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, &stats, XStoreTunables::LIVE_WIDEN_WORKERS, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, nullptr,
                                  /*keep_old=*/false);
    });
    WaitForFrontierPast(zmap, 3 * Rpb());
    stop.store(true);
    ASSERT_EQ(builder.get(), Result::kStopped);

    const uint64_t f = zmap.FrontierForTests();
    ASSERT_NE(f, ZoneMapFile::kNoRebuild);
    EXPECT_LT(f, kRows) << "stopped too late to mean anything";
    EXPECT_EQ(catidx.FrontierForTests(), f) << "both files advance together";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    const uint64_t complete_blocks = f / Rpb();
    ASSERT_GT(complete_blocks, 0u);
    auto trusted = zmap.TryGetBlockStats(complete_blocks - 1);
    ASSERT_TRUE(trusted.has_value());
    EXPECT_EQ(trusted->size(), 70u);
    EXPECT_FALSE(zmap.TryGetBlockStats(complete_blocks).has_value()) << "a block past F must not be trusted";
    EXPECT_TRUE(catidx.MightContain(complete_blocks, 0, EncodeFDataForCatIdx(FDouble(-1.0))));
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_FALSE(zmap.MarkClean());
    EXPECT_FALSE(catidx.MarkClean());

    XStore::Shutdown(); // clean stop: the half-built index stays dirty
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, "LWSTOPREF", 70); // its InitDB also rebuilds LWSTOP's dirty index
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectSameFilesAs(db, "LWSTOPREF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// ---- 5. Failure injection -------------------------------------------------------------------------------------------

// The third chunk's pread fails: that chunk is not folded (F stays at its start), both files stay in rebuild, dirty,
// answers stay right (scan), and the next start rebuilds them.
TEST_F(XStoreLiveWidenTest, AReadFailureMidScanLeavesTheIndexInRebuildBehindTheFailedChunk) {
    XStore store;
    const std::string db = "LWPREAD";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 2, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    EXPECT_EQ(stats.chunks, 2u) << "two chunks folded before the failed read";
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_EQ(f, stats.rows_folded) << "F is the end of the second chunk (rows 0..F-1 are all present)";
    EXPECT_EQ(catidx.FrontierForTests(), f);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState(); // MarkClean refuses: both stay dirty
    BuildStartupReference(store, db, "LWPREADREF", 70);
    ExpectSameFilesAs(db, "LWPREADREF");
}

// A .zmap block write fails mid-build: the zone map stops behind its frontier (dirty, in rebuild), the catidx carries
// on and finishes clean. The result is kFailed.
TEST_F(XStoreLiveWidenTest, AZoneMapWriteFailureStopsOnlyTheZoneMap) {
    XStore store;
    const std::string db = "LWPWRITE";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState(); // both index files clean on disk: BeginRebuild's dirty mark is one pwrite
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        // .zmap pwrites: the dirty mark, the fresh header, then blocks: fail the fourth block write.
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, ".zmap"), 5, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LT(zmap.FrontierForTests(), kRows);
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(catidx.TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, "LWPWRITEREF", 70);
    ExpectSameFilesAs(db, "LWPWRITEREF");
}

// The final fdatasync of the data file fails: the build is complete and trusted in this session (pruned at the new
// width) but stays dirty on disk, so the next start rebuilds it.
TEST_F(XStoreLiveWidenTest, ADataSyncFailureAtTheEndLeavesTheWidenedIndexDirty) {
    XStore store;
    const std::string db = "LWSYNC";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        ScopedCheckpointerPause no_background_sync; // the fault must hit the builder's fdatasync, not a checkpoint's
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".XSTORE"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore_Proto::Max_REP rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &rep);
    EXPECT_EQ(rep.opstatus(), "SUCCESS") << "indexed at the new width in this session";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// BeginRebuild fails on the zone map (its dirty-mark fdatasync): nothing changed there -- the old 64-column index
// stays in place, trusted and pruning -- while the catidx widens: kPartial. If both refuse: kFailed, both old.
TEST_F(XStoreLiveWidenTest, ABeginRebuildFailureKeepsTheOldIndexInUse) {
    XStore store;
    const std::string db = "LWBEGIN";
    constexpr uint64_t kRows = 10000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState(); // clean on disk: BeginRebuild must make the dirty mark durable first
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kPartial);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_FALSE(stats.zmap_began) << "begin failed: neither began nor busy (a retry is reasonable)";
    EXPECT_FALSE(stats.zmap_busy);
    EXPECT_TRUE(stats.catidx_began);
    EXPECT_FALSE(zmap.Rebuilding());
    EXPECT_EQ(zmap.TrackedColumnCount(), 64u);
    auto block = zmap.TryGetBlockStats(1);
    ASSERT_TRUE(block.has_value()) << "the old index is still trusted";
    EXPECT_EQ(block->size(), 64u);
    EXPECT_GT((*block)[0].valid_count, 0u);
    EXPECT_EQ(catidx.TrackedColumnCount(), 70u);
    EXPECT_FALSE(catidx.Rebuilding());
    XStore_Proto::Max_REP narrow;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &narrow);
    EXPECT_NE(narrow.opstatus().find(kNotIndexed), std::string::npos) << "column 65 is still scanned";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    // Both refuse (the catidx is now clean and 70 wide; ask for 72 so both must rebuild).
    CleanRestartState();
    {
        xstore_fault::ScopedFdFault zfail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1, EIO);
        xstore_fault::ScopedFdFault cfail(xstore_fault::FdOp::kPwrite, YearPath(db, ".catidx"), 0, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 72, true, true, nullptr, &stats), Result::kFailed);
    }
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).TrackedColumnCount(), 64u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_FALSE(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).Rebuilding());
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// A file already in rebuild state at entry is not touched (resuming is the trigger's business).
TEST_F(XStoreLiveWidenTest, AFileAlreadyRebuildingIsLeftAlone) {
    XStore store;
    const std::string db = "LWBUSY";
    SeedNarrowIndexWideRows(store, db, 3000, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    ASSERT_TRUE(zmap.BeginRebuild(64));
    XStore::LiveWidenStats stats;
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kAlreadyRebuilding);
    EXPECT_TRUE(stats.zmap_busy);
    EXPECT_FALSE(stats.zmap_began || stats.catidx_began || stats.catidx_busy) << "the catidx must not even be tried";
    EXPECT_EQ(zmap.FrontierForTests(), 0u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 64u) << "the catidx was touched";
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, false, false, nullptr, nullptr), Result::kNothingToDo);
    EXPECT_EQ(store.LiveWidenOneYear(path, 1999, kRowLength, 70, true, true, nullptr, nullptr), Result::kNothingToDo);
}

// ---- 6. Registry pressure -------------------------------------------------------------------------------------------

// More than the registry cap of other year files opened while a widen runs: the pinned, rebuilding entries survive.
TEST_F(XStoreLiveWidenTest, RegistryPressureDuringAWidenDoesNotEvictTheRebuildingFiles) {
    XStore store;
    const std::string db = "LWREG";
    constexpr uint64_t kRows = 100000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile *zmap = &XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile *catidx = &XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    WaitForFrontierPast(*zmap, Rpb());
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
        XStore::GetOrOpenZoneMap(path, year, kRowLength);
        XStore::GetOrOpenCatIdx(path, year, kRowLength);
    }
    const bool overlapped = zmap->Rebuilding();
    EXPECT_EQ(builder.get(), Result::kDone);
    ASSERT_TRUE(overlapped) << "the widen finished before the registry filled up: the test proves nothing";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(path, kYear, kRowLength), zmap) << "the rebuilding zone map was evicted";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(path, kYear, kRowLength), catidx) << "the rebuilding catidx was evicted";
    EXPECT_EQ(zmap->TrackedColumnCount(), 70u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// ---- 7. Everything about rows >= F happens under the lock -----------------------------------------------------------

// Every data-file read and every SEEK_DATA probe the builder makes must fall inside a range it holds READ on, under the
// writers' lock key (CachedYearFilePath): at that moment a WRITE try-lock on those bytes must fail. Nothing else runs,
// so a WRITE that succeeds means the builder read or probed rows it had not locked (a fold or a hole skip outside the
// lock, or a lock under another spelling of the path).
TEST_F(XStoreLiveWidenTest, EveryReadAndHoleProbeOfTheBuilderIsUnderItsReadLock) {
    XStore store;
    const std::string db = "LWLOCKED";
    SeedNarrowIndexWideRows(store, db, 6000, 70);
    {   // holes between data runs, so the probes and the scan both skip some
        Rows sparse;
        for (uint64_t r : {9000ULL, 9001ULL, 30000ULL, 200000ULL, 5000000ULL}) sparse.emplace_back(Epoch(r), ValueRow(r % 1000, 70));
        Insert(store, db, sparse);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ASSERT_EQ(path, store.data_store_dir + "/" + db) << "must be the spelling the writers use (CachedDbBasePath)";
    FileRangeLock &writers_lock = FileLockRegistry::Get(path + "/" + std::to_string(kYear) + ".XSTORE"); // CachedYearFilePath's format
    std::atomic<uint64_t> reads{0}, probes{0}, unlocked{0};
    Result result = Result::kFailed;
    std::thread builder([&] {
        xstore_fault::ScopedFdObserver observe(data_path, [&](xstore_fault::ObservedOp op, uint64_t offset, uint64_t length) {
            (op == xstore_fault::ObservedOp::kPread ? reads : probes).fetch_add(1);
            const uint64_t bytes = std::max<uint64_t>(length, 1);
            if (writers_lock.TryLockRangeFor(offset, bytes, kWrite, milliseconds(0))) {
                writers_lock.UnlockRange(offset, bytes, kWrite);
                unlocked.fetch_add(1);
            }
        });
        XStore b;
        result = b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr);
    });
    builder.join();
    EXPECT_EQ(result, Result::kDone);
    EXPECT_GT(reads.load(), 0u);
    EXPECT_GT(probes.load(), 2u);
    EXPECT_EQ(unlocked.load(), 0u) << "the builder read or probed data-file bytes it did not hold a READ lock on";
    ExpectAnswersMatchTheData(store, db, 0, 5999, {0, 65});
}

// A WRITE lock held on one hole row R ahead of the data: while it is held the frontier never passes R (passing it would
// mean deciding about a row nobody read under a lock: the hole could be written the next instant); once released the
// build completes. Catches an in-lock probe whose finding (the next data row, past the locked range) is used to move F.
TEST_F(XStoreLiveWidenTest, TheFrontierNeverPassesAHoleRowAWriterHolds) {
    XStore store;
    const std::string db = "LWHOLE";
    SeedNarrowIndexWideRows(store, db, 6000, 70);
    Insert(store, db, {{Epoch(9000), ValueRow(9, 70)}, {Epoch(9001), ValueRow(10, 70)}});
    const std::string path = DbPath(db);
    constexpr uint64_t kHoleRow = 7000;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kHoleRow * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    uint64_t highest = 0;
    bool passed = false;
    const Clock::time_point until = Clock::now() + milliseconds(1500);
    while (Clock::now() < until) {
        const uint64_t f = zmap.FrontierForTests();
        if (f != ZoneMapFile::kNoRebuild) {
            highest = std::max(highest, f);
            passed = passed || f > kHoleRow;
        }
        if (f == ZoneMapFile::kNoRebuild && highest > 0) passed = true; // finished: it went past R
        std::this_thread::sleep_for(milliseconds(1));
    }
    const bool still_running = builder.wait_for(milliseconds(0)) != std::future_status::ready;
    lock.UnlockRange(kHoleRow * kRowLength, kRowLength, kWrite);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_FALSE(passed) << "the frontier passed a hole row a writer held (highest F seen " << highest << ")";
    EXPECT_TRUE(still_running) << "the builder finished while R was locked";
    EXPECT_GE(highest, 6000u) << "the builder never reached the held row: the test proves nothing";
    EXPECT_GT(stats.lock_timeouts, 0u);
    ExpectAnswersMatchTheData(store, db, 0, 9001, {0, 65});
}

// The zone map begins but the catidx is busy (a catidx-only builder owns it, here: a rebuild begun by hand): the zone
// map widens (kPartial) and the catidx is left exactly as its owner has it -- never folded into, advanced, finished
// or cleaned by this builder.
TEST_F(XStoreLiveWidenTest, ABusyCatIdxIsLeftToItsOwnerWhileTheZoneMapWidens) {
    XStore store;
    const std::string db = "LWCATBUSY";
    SeedNarrowIndexWideRows(store, db, 8000, 70);
    const std::string path = DbPath(db);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_TRUE(catidx.BeginRebuild(64));
    XStore::LiveWidenStats stats;
    EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats), Result::kPartial);
    EXPECT_TRUE(stats.zmap_began);
    EXPECT_TRUE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_busy);
    EXPECT_FALSE(stats.catidx_began);
    EXPECT_FALSE(stats.catidx_widened);
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(catidx.FrontierForTests(), 0u) << "the builder advanced a catidx it did not begin";
    EXPECT_EQ(catidx.TrackedColumnCount(), 64u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, 7999, {0, 65});
    BuildStartupReference(store, db, "LWCATBUSYREF", 70); // InitDB also rebuilds LWCATBUSY's (dirty) catidx; not compared
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".zmap")) == ReadFileBytes(YearPath("LWCATBUSYREF", ".zmap")))
        << "the zone map differs from the startup rebuild";
}

// S2 (review): two builders on one year at the same instant (released together from a spin barrier, 5 rounds): exactly
// one rebuilds (kDone) and the other touches nothing (kAlreadyRebuilding), so the loser can never truncate the
// winner's half-built files while the winner keeps advancing F. The result equals a startup rebuild.
TEST_F(XStoreLiveWidenTest, TwoBuildersOnOneYearNeverBothBegin) {
    XStore store;
    const std::string db = "LWTWICE";
    SeedNarrowIndexWideRows(store, db, 20000, 70);
    const std::string path = DbPath(db);
    for (int round = 0; round < 5; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        std::atomic<int> ready{0};
        auto run = [&] {
            XStore b;
            ready.fetch_add(1);
            while (ready.load() < 2) {
            }
            return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr);
        };
        auto first = std::async(std::launch::async, run);
        auto second = std::async(std::launch::async, run);
        const Result a = first.get();
        const Result b = second.get();
        EXPECT_TRUE((a == Result::kDone && b == Result::kAlreadyRebuilding) || (a == Result::kAlreadyRebuilding && b == Result::kDone))
            << "results " << static_cast<int>(a) << " and " << static_cast<int>(b);
    }
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    BuildStartupReference(store, db, "LWTWICEREF", 70);
    ExpectSameFilesAs(db, "LWTWICEREF");
}

// ---- 8. The DB's year file removed mid-widen ------------------------------------------------------------------------

TEST_F(XStoreLiveWidenTest, RemovingTheYearFileMidWidenFailsTheBuildWithoutHarm) {
    XStore store;
    const std::string db = "LWGONE";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    // Held through a pin: the removal retires the year's entries, and a retired object lives only while pinned.
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    WaitForFrontierPast(*zmap, Rpb());
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_GT(rep.removestatus_size(), 0);
    EXPECT_NE(rep.removestatus(0).find("Removed"), std::string::npos) << rep.removestatus(0);
    EXPECT_EQ(builder.get(), Result::kFailed);
    EXPECT_TRUE(zmap->Rebuilding()) << "stays untrusted past F, dirty";
    EXPECT_LT(zmap->FrontierForTests(), kRows);
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".zmap"))) << "the year's index files go with its data";
    EXPECT_FALSE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value()) << "the year's entry was not retired";
}

// ---- 9. Registry lifetime: RemoveDB, pins on the request paths (Tech Reference 4.14) -------------------------------

// S1: RemoveDB must forget the DB's warm index objects. Otherwise a DB re-created under the same name gets the old
// objects back (fd on the unlinked files, stats and blooms of the deleted rows) and fully covered blocks answer with
// the deleted values.
TEST_F(XStoreLiveWidenTest, RemoveDbForgetsTheIndexesOfTheRemovedDb) {
    XStore store;
    const std::string db = "REGREMOVE";
    const uint64_t last = 2 * Rpb() - 1;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r <= last; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r, 2)); // 1000 % 13 != 0
    Insert(store, db, rows);
    XStore_Proto::Min_REP old_min;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &old_min);
    ASSERT_EQ(old_min.opresult().doubledata(), 1000.0);

    ASSERT_NE(store.RemoveDB(db).removestatus().find("[INFO]"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});

    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(last), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(last), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);

    XStore_Proto::FData old_value;
    old_value.set_doubledata(1000.0);
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(last), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    // FindByValue rescans a "maybe" block, so only the bloom itself shows a stale entry.
    CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kYear, kRowLength);
    EXPECT_FALSE(catidx->MightContain(0, 0, EncodeFDataForCatIdx(old_value))) << "the old DB's bloom came back";
    XStore_Proto::FData new_value;
    new_value.set_doubledata(5000.0);
    EXPECT_TRUE(catidx->MightContain(0, 0, EncodeFDataForCatIdx(new_value)));
}

// A pinned entry under a removed DB is retired, not destroyed: its holder keeps a valid object (writing into the unlinked
// file) and lookups never return it. The removal waits for the pin to drop before it finishes (F4), frees the retired
// objects, and a re-created DB gets fresh ones.
TEST_F(XStoreLiveWidenTest, RemoveDbRetiresAPinnedIndexAndLookupsNeverReturnIt) {
    XStore store;
    const std::string db = "REGRETIRE";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(3), ValueRow(700, 2)}});
    std::optional<ZoneMapFile::Pin> zmap(XStore::PinZoneMap(DbPath(db), kYear, kRowLength));
    std::optional<CatIdxFile::Pin> catidx(XStore::PinCatIdx(DbPath(db), kYear, kRowLength));
    ZoneMapFile *old_zmap = zmap->get();
    CatIdxFile *old_catidx = catidx->get();

    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return r.RemoveDB(db).removestatus();
    });
    const Clock::time_point t0 = Clock::now();
    while (std::filesystem::exists(DbPath(db)) && MsSince(t0) < 5000) std::this_thread::sleep_for(milliseconds(1));
    ASSERT_FALSE(std::filesystem::exists(DbPath(db))) << "the removal never got to the files";
    EXPECT_EQ(remove.wait_for(milliseconds(50)), std::future_status::timeout) << "it did not wait for the pins";
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "a lookup returned the retired entry";
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 2u);
    old_zmap->UpdateRow(9, {{0, 1.0, true}}, 1); // the holder may still use it
    EXPECT_TRUE(old_catidx->MightContain(0, 0, EncodeFDataForCatIdx(ValueRow(700, 2).fielddata(0))));

    zmap.reset();
    catidx.reset();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(5)), std::future_status::ready) << "the removal did not finish";
    EXPECT_NE(remove.get().find("[INFO]"), std::string::npos);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u) << "the removal's wait frees the retired objects";

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(4), ValueRow(800, 2)}});
    ZoneMapFile::Pin fresh_zmap = XStore::PinZoneMap(DbPath(db), kYear, kRowLength);
    auto stats = fresh_zmap->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 800.0);
}

// The DB-prefix match is a whole directory: removing DB1 leaves DB10's warm entries alone.
TEST_F(XStoreLiveWidenTest, RemoveDbLeavesADbWhoseNameExtendsItAlone) {
    XStore store;
    CreateTestDb(store, "REGP1", XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGP10", XStore_Proto::SECOND, kRowLength);
    Insert(store, "REGP1", {{Epoch(1), ValueRow(1, 2)}});
    Insert(store, "REGP10", {{Epoch(1), ValueRow(1, 2)}});
    ZoneMapFile *kept = &XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength);
    CatIdxFile *kept_catidx = &XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength);
    // The warm objects synced their dirty mark once; a cold reopen of the (dirty on disk) files would count 0. That
    // tells "the same object" from "a new one at a reused address".
    ASSERT_EQ(kept->DirtyMarkWritesForTests(), 1u);
    ASSERT_EQ(kept_catidx->DirtyMarkWritesForTests(), 1u);
    ASSERT_NE(store.RemoveDB("REGP1").removestatus().find("[INFO]"), std::string::npos);
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath("REGP1"), kYear).has_value());
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath("REGP10"), kYear).has_value());
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength), kept);
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength), kept_catidx);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(DbPath("REGP10"), kYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(DbPath("REGP10"), kYear, kRowLength).DirtyMarkWritesForTests(), 1u);
}

// ---- RemoveDBFiles of one year (F1): only that year's files and registry entry go ----------------------------------

constexpr unsigned short kNextYear = 1981; // XStoreLiveWidenTest::kYear + 1

// Removing YYYY.XSTORE also removes YYYY.zmap/.catidx and retires only the registry key of that year. A re-inserted
// year answers from the new rows only; the other year's warm objects stay the live entries.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesOfOneYearForgetsOnlyThatYearsIndexes) {
    XStore store;
    const std::string db = "REGRMYEAR";
    const uint64_t last = 2 * Rpb() - 1;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r <= last; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r, 2));
    Insert(store, db, rows);
    Insert(store, db, {{Jan1(kNextYear) + 3, ValueRow(300, 2)}, {Jan1(kNextYear) + 4, ValueRow(301, 2)}});
    ZoneMapFile *next_zmap = &XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength);
    CatIdxFile *next_catidx = &XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength);
    ASSERT_EQ(next_zmap->DirtyMarkWritesForTests(), 1u);
    ASSERT_EQ(next_catidx->DirtyMarkWritesForTests(), 1u);

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_EQ(rep.removestatus_size(), 3);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.XSTORE");
    EXPECT_EQ(rep.removestatus(1), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.zmap");
    EXPECT_EQ(rep.removestatus(2), "[INFO] [REMOVE DB FILES] [DB: REGRMYEAR] - Removed 1980.catidx");
    for (const char *ext : {".XSTORE", ".zmap", ".catidx"}) EXPECT_FALSE(std::filesystem::exists(YearPath(db, ext))) << ext;

    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(last), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(last), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(last), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);
    XStore_Proto::FData old_value;
    old_value.set_doubledata(1000.0);
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(last), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    EXPECT_FALSE(XStore::PinCatIdx(DbPath(db), kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(old_value)));

    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), next_zmap) << "the other year was retired";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength), next_catidx);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).DirtyMarkWritesForTests(), 1u);
    XStore_Proto::Max_REP next_max;
    store.MaxAggregate(Jan1(kNextYear), Jan1(kNextYear) + 10, db, 0, &next_max);
    EXPECT_EQ(next_max.opresult().doubledata(), 301.0);
}

// A pin held on another year across the removal: that year's entry stays the live one (a retire would let the next
// cold open create a second object on the same, still existing file, and the two would overwrite each other's blocks).
TEST_F(XStoreLiveWidenTest, RemoveDbFilesKeepsAPinnedEntryOfAnotherYearLive) {
    XStore store;
    const std::string db = "REGRMPIN";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(DbPath(db), kNextYear, kRowLength);
    const CatIdxFile::Pin catidx = XStore::PinCatIdx(DbPath(db), kNextYear, kRowLength);
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, db).removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u);
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), zmap.get()) << "a second object was opened";
    EXPECT_EQ(&XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength), catidx.get());
}

// An untrusted entry of another year (EnterUntrustedRebuildState, no pin) survives the removal and stays untrusted:
// erasing it would let a cold reopen trust its stale file.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesKeepsAnUntrustedEntryOfAnotherYear) {
    XStore store;
    const std::string db = "REGRMUNTRUST";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).EnterUntrustedRebuildState();
    XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).EnterUntrustedRebuildState();
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, db).removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_TRUE(XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength).Rebuilding());
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(DbPath(db), kNextYear, kRowLength).Rebuilding());
}

// An index file alone is refused: a fresh, empty index for a year whose data still exists would be trusted.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesRefusesAnIndexFileOnItsOwn) {
    XStore store;
    const std::string db = "REGRMIDX";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}});
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("1980.zmap");
    req.add_filename("1980.catidx");
    const XStore_Proto::RemoveDBFiles_REP rep = store.RemoveDBFiles(&req, db);
    ASSERT_EQ(rep.removestatus_size(), 2);
    EXPECT_EQ(rep.removestatus(0).rfind("[WARNING] ", 0), 0u) << rep.removestatus(0);
    EXPECT_EQ(rep.removestatus(1).rfind("[WARNING] ", 0), 0u) << rep.removestatus(1);
    EXPECT_TRUE(std::filesystem::exists(YearPath(db, ".zmap")));
    EXPECT_TRUE(std::filesystem::exists(YearPath(db, ".catidx")));
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value());
}

// The year key matches exactly: removing 1980 of DB1 leaves year 19801 of DB1 and year 1980 of DB10 alone.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesMatchesTheYearKeyExactly) {
    XStore store;
    CreateTestDb(store, "REGK1", XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGK10", XStore_Proto::SECOND, kRowLength);
    Insert(store, "REGK1", {{Epoch(1), ValueRow(1, 2)}});
    Insert(store, "REGK10", {{Epoch(1), ValueRow(1, 2)}});
    const ZoneMapFile::Pin long_year = XStore::PinZoneMap(DbPath("REGK1"), 19801, kRowLength);
    ZoneMapFile *other_db = &XStore::GetOrOpenZoneMap(DbPath("REGK10"), kYear, kRowLength);
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename("1980.XSTORE");
    ASSERT_NE(store.RemoveDBFiles(&req, "REGK1").removestatus(0).find("[INFO]"), std::string::npos);
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath("REGK1"), kYear).has_value());
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGK1"), 19801, kRowLength), long_year.get());
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u) << "year 19801 matched the 1980 key";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath("REGK10"), kYear, kRowLength), other_db);
    EXPECT_EQ(other_db->DirtyMarkWritesForTests(), 1u);
}

// Deterministic: an insert stopped inside ApplyWalRecord, after both registry lookups (it waits for the WRITE range
// lock the test holds), keeps both of its index files through more than the cap of other cold opens. Without the pins
// the registry evicts the zone map the writer still holds, and the writer then updates a destroyed object.
TEST_F(XStoreLiveWidenTest, AnInsertHoldsItsIndexesThroughRegistryPressure) {
    XStore store;
    const std::string db = "REGHOLD";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGFILL", XStore_Proto::SECOND, kRowLength);
    auto hold = std::make_unique<FileRangeLockGuard>(YearPath(db, ".XSTORE"), 0, 0, kWrite);
    auto writer = std::async(std::launch::async, [&] {
        XStore w;
        XStore_Proto::UnaryInsert_REP rep;
        w.UnaryInsert(Epoch(5), ValueRow(100, 2), db, &rep, XStore_Proto::BYPASS);
        return rep.insertstatus();
    });
    // The catidx file appears inside the second lookup, under the registry mutex; the storm below takes that mutex,
    // so it starts only after the writer holds both files.
    const Clock::time_point t0 = Clock::now();
    while (!std::filesystem::exists(YearPath(db, ".catidx")) && MsSince(t0) < 10000) {
        std::this_thread::sleep_for(milliseconds(1));
    }
    ASSERT_TRUE(std::filesystem::exists(YearPath(db, ".catidx"))) << "the writer never reached its index lookups";
    for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 10; ++year) {
        XStore::GetOrOpenZoneMap(DbPath("REGFILL"), year, kRowLength);
        XStore::GetOrOpenCatIdx(DbPath("REGFILL"), year, kRowLength);
    }
    EXPECT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "evicted while the writer holds it";
    hold.reset();
    ASSERT_EQ(writer.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    EXPECT_NE(writer.get().find("[INFO]"), std::string::npos);

    const XStore_Proto::FData value = ValueRow(100, 2).fielddata(0);
    EXPECT_TRUE(XStore::PinCatIdx(DbPath(db), kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(value)));
    auto stats = XStore::PinZoneMap(DbPath(db), kYear, kRowLength)->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 100.0);
}

// SMOKE TEST, NOT a regression guard for the pins (it passed 3 of 3 with ApplyWalRecord's pins removed; the guard is
// AnInsertHoldsItsIndexesThroughRegistryPressure above). Writers (BYPASS on their own thread, SYNC through the WAL
// writers) and readers on a year file while a third thread keeps cold-opening more than the cap of other year files:
// no crash, and the final indexes are byte-identical to a startup rebuild of the final data.
TEST_F(XStoreLiveWidenTest, RegistryPressureUnderWritersAndReadersMatchesAStartupRebuild) {
    XStore store;
    const std::string db = "REGSTRESS";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    CreateTestDb(store, "REGSFILL", XStore_Proto::SECOND, kRowLength);
    constexpr int kWriters = 4;
    const uint64_t rows = 8 * Rpb();
    std::atomic<bool> running{true};
    std::atomic<int> writers_left{kWriters};
    std::atomic<uint64_t> fill_passes{0};
    std::vector<std::thread> threads;
    // No null columns: the startup rebuild folds an unset column into the bloom (its tag byte) and a live insert does
    // not, so with nulls the two files differ by a few superset bits that have nothing to do with the registry.
    const auto FullRow = [](uint64_t v) {
        XStore_Proto::RowData row;
        for (uint64_t c = 0; c < 4; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(v + c));
        return row;
    };
    for (int t = 0; t < kWriters; ++t) {
        threads.emplace_back([&, t] {
            XStore w;
            std::mt19937_64 rng(4242 + t);
            std::vector<uint64_t> mine;
            for (uint64_t r = static_cast<uint64_t>(t); r < rows; r += kWriters) mine.push_back(r);
            std::shuffle(mine.begin(), mine.end(), rng);
            for (size_t from = 0; from < mine.size();) {
                const size_t n = std::min<size_t>(mine.size() - from, 1 + rng() % 16);
                const XStore_Proto::WalDurabilityMode mode = rng() % 2 ? XStore_Proto::BYPASS : XStore_Proto::SYNC;
                if (n == 1) {
                    XStore_Proto::UnaryInsert_REP rep;
                    w.UnaryInsert(Epoch(mine[from]), FullRow(mine[from] * 7), db, &rep, mode);
                    ASSERT_NE(rep.insertstatus().find("[INFO]"), std::string::npos) << rep.insertstatus();
                } else {
                    XStore_Proto::BatchInsert_REQ req;
                    req.set_dbname(db);
                    req.set_durabilitymode(mode);
                    for (size_t i = from; i < from + n; ++i) {
                        XStore_Proto::BatchInsert_DATA *entry = req.add_batchinsert_data();
                        entry->set_timestamp(Epoch(mine[i]));
                        *entry->mutable_data() = FullRow(mine[i] * 7);
                    }
                    XStore_Proto::BatchInsert_REP rep;
                    w.BatchInsert(&req, &rep);
                    ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
                }
                from += n;
            }
            writers_left.fetch_sub(1);
        });
    }
    threads.emplace_back([&] { // readers: the aggregate and FindByValue paths under the same pressure
        XStore r;
        std::mt19937_64 rng(99);
        while (running.load()) {
            const uint64_t lo = rng() % rows, hi = std::min(rows - 1, lo + rng() % (3 * Rpb()));
            XStore_Proto::Max_REP max_rep;
            r.MaxAggregate(Epoch(lo), Epoch(hi), db, 0, &max_rep);
            XStore_Proto::Sum_REP sum_rep;
            r.SumAggregate(Epoch(lo), Epoch(hi), db, 1, &sum_rep);
            XStore_Proto::FData v;
            v.set_doubledata(static_cast<double>(lo * 7));
            r.FindByValue(Epoch(lo), Epoch(hi), db, 0, v, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                          [](std::string &&, bool) {});
        }
    });
    threads.emplace_back([&] { // the evictor
        while (running.load()) {
            for (unsigned short year = 2001; year < 2001 + XStoreTunables::ZONE_MAP_REGISTRY_CAP + 20; ++year) {
                XStore::GetOrOpenZoneMap(DbPath("REGSFILL"), year, kRowLength);
                XStore::GetOrOpenCatIdx(DbPath("REGSFILL"), year, kRowLength);
            }
            fill_passes.fetch_add(1);
        }
    });
    while (writers_left.load() > 0) std::this_thread::sleep_for(milliseconds(5));
    running.store(false);
    for (std::thread &t : threads) t.join();
    EXPECT_GT(fill_passes.load(), 0u);

    // Back into the registry (an evicted file is dirty on disk and only registry entries are marked clean at stop).
    XStore::GetOrOpenZoneMap(DbPath(db), kYear, kRowLength);
    XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength);
    CleanRestartState(); // drains the WAL and marks the live indexes clean, so the startup below keeps them as they are
    BuildStartupReference(store, db, "REGSTRESSREF", 64);
    ExpectSameFilesAs(db, "REGSTRESSREF");
}

// Step 0 of stage 3 (registry fix-round review, finding 1): a sidecar whose removal fails (here a non-empty directory
// in place of 1980.zmap; the warm entry keeps its fd on the old file) is reported as an [ERROR] status, no exception
// escapes, and the year's entries are retired anyway (a live entry would let a later insert re-create the year file
// through it with the deleted rows' stats). The other year is untouched.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesRetiresTheYearEvenWhenASidecarCannotBeRemoved) {
    XStore store;
    const std::string db = "REGRMFAIL";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    ASSERT_TRUE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value());
    ZoneMapFile *next_zmap = &XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength);
    const std::string zmap_path = YearPath(db, ".zmap");
    ASSERT_TRUE(std::filesystem::remove(zmap_path));
    ASSERT_TRUE(std::filesystem::create_directory(zmap_path));
    std::ofstream(zmap_path + "/keep") << "x";

    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    XStore_Proto::RemoveDBFiles_REP rep;
    EXPECT_NO_THROW(rep = store.RemoveDBFiles(&req, db));
    ASSERT_EQ(rep.removestatus_size(), 3);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: REGRMFAIL] - Removed 1980.XSTORE");
    EXPECT_EQ(rep.removestatus(1).rfind("[ERROR] [REMOVE DB FILES] [DB: REGRMFAIL] - Unable to remove 1980.zmap", 0), 0u)
        << rep.removestatus(1);
    EXPECT_EQ(rep.removestatus(1).find("emptied"), std::string::npos) << "a directory cannot be truncated";
    EXPECT_EQ(rep.removestatus(2), "[INFO] [REMOVE DB FILES] [DB: REGRMFAIL] - Removed 1980.catidx");
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(DbPath(db), kYear).has_value()) << "the year's entry was not retired";
    EXPECT_EQ(&XStore::GetOrOpenZoneMap(DbPath(db), kNextYear, kRowLength), next_zmap) << "another year was touched";
    std::filesystem::remove_all(zmap_path);
}

// A regular sidecar that cannot be unlinked (the DB directory turns read-only after the data file went) is truncated to
// 0 bytes: still an [ERROR] for the unlink, plus "emptied it instead". A year re-created later opens the empty files as
// fresh indexes, not the deleted rows' stats. The test's pin holds the removal in its wait between the data file and
// the sidecars (the wait for retired objects), which is where the directory is made read-only. Root ignores directory
// permissions, so the test needs a non-root user.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesEmptiesARegularSidecarItCannotRemove) {
    if (geteuid() == 0) GTEST_SKIP() << "root can unlink in a read-only directory";
    XStore store;
    const std::string db = "REGRMTRUNC";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r < 2 * Rpb(); ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r, 2));
    Insert(store, db, rows);
    std::optional<ZoneMapFile::Pin> pin(XStore::PinZoneMap(DbPath(db), kYear, kRowLength));
    ASSERT_GT(std::filesystem::file_size(YearPath(db, ".zmap")), 0u);

    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        return r.RemoveDBFiles(&req, db);
    });
    const Clock::time_point t0 = Clock::now();
    while (std::filesystem::exists(YearPath(db, ".XSTORE")) && MsSince(t0) < 5000) std::this_thread::sleep_for(milliseconds(1));
    ASSERT_FALSE(std::filesystem::exists(YearPath(db, ".XSTORE"))) << "the removal never got to the data file";
    struct RestorePermissions {
        std::string dir;
        ~RestorePermissions() {
            std::error_code ec;
            std::filesystem::permissions(dir, static_cast<std::filesystem::perms>(0755), ec);
        }
    } restore{DbPath(db)};
    std::filesystem::permissions(DbPath(db), static_cast<std::filesystem::perms>(0555));
    pin.reset();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready);
    std::filesystem::permissions(DbPath(db), static_cast<std::filesystem::perms>(0755));
    const XStore_Proto::RemoveDBFiles_REP rep = remove.get();

    ASSERT_EQ(rep.removestatus_size(), 3);
    EXPECT_EQ(rep.removestatus(0), "[INFO] [REMOVE DB FILES] [DB: REGRMTRUNC] - Removed 1980.XSTORE");
    for (int i : {1, 2}) {
        EXPECT_EQ(rep.removestatus(i).rfind("[ERROR] [REMOVE DB FILES] [DB: REGRMTRUNC] - Unable to remove 1980.", 0), 0u)
            << rep.removestatus(i);
        EXPECT_NE(rep.removestatus(i).find("; emptied it instead"), std::string::npos) << rep.removestatus(i);
    }
    for (const char *ext : {".zmap", ".catidx"}) {
        ASSERT_TRUE(std::filesystem::exists(YearPath(db, ext))) << ext;
        EXPECT_EQ(std::filesystem::file_size(YearPath(db, ext)), 0u) << ext;
    }

    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});
    auto stats = XStore::PinZoneMap(DbPath(db), kYear, kRowLength)->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 5000.0);
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(2 * Rpb() - 1), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::FData old_value;
    old_value.set_doubledata(1000.0);
    EXPECT_FALSE(XStore::PinCatIdx(DbPath(db), kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(old_value)));
}

// ---- F4: removal vs requests on the same DB/year ---------------------------------------------------------------------

// Deterministic: while a RemoveDBFiles of a year is under way (held in its wait by a pin on the retired zone map), the
// year's index files cannot be cold-opened: a pin throws, and an insert fails before it writes anything, instead of
// writing its stats into an object that is about to go or into one that pairs with the deleted data file. Once the
// removal finishes the year inserts again, with fresh indexes.
TEST_F(XStoreLiveWidenTest, ARemovalRefusesTheYearsIndexOpensUntilItFinishes) {
    XStore store;
    const std::string db = "REGRMREFUSE";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(1), ValueRow(1, 2)}, {Jan1(kNextYear) + 1, ValueRow(2, 2)}});
    std::optional<ZoneMapFile::Pin> pin(XStore::PinZoneMap(DbPath(db), kYear, kRowLength));

    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        return r.RemoveDBFiles(&req, db);
    });
    const Clock::time_point t0 = Clock::now();
    while (std::filesystem::exists(YearPath(db, ".XSTORE")) && MsSince(t0) < 5000) std::this_thread::sleep_for(milliseconds(1));
    ASSERT_FALSE(std::filesystem::exists(YearPath(db, ".XSTORE"))) << "the removal never got to the data file";

    EXPECT_THROW(XStore::PinZoneMap(DbPath(db), kYear, kRowLength), std::runtime_error);
    EXPECT_THROW(XStore::PinCatIdx(DbPath(db), kYear, kRowLength), std::runtime_error);
    XStore_Proto::UnaryInsert_REP refused;
    store.UnaryInsert(Epoch(7), ValueRow(77, 2), db, &refused, XStore_Proto::BYPASS);
    EXPECT_EQ(refused.insertstatus().rfind("[ERROR] ", 0), 0u) << refused.insertstatus();
    XStore_Proto::UnaryInsert_REP other_year;
    store.UnaryInsert(Jan1(kNextYear) + 2, ValueRow(3, 2), db, &other_year, XStore_Proto::BYPASS);
    EXPECT_NE(other_year.insertstatus().find("[INFO]"), std::string::npos) << "another year was blocked";

    pin.reset();
    ASSERT_EQ(remove.wait_for(std::chrono::seconds(15)), std::future_status::ready);
    EXPECT_NE(remove.get().removestatus(0).find("[INFO]"), std::string::npos);
    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}});
    auto stats = XStore::PinZoneMap(DbPath(db), kYear, kRowLength)->TryGetBlockStats(0);
    ASSERT_TRUE(stats.has_value());
    EXPECT_EQ((*stats)[0].valid_count, 1u);
    EXPECT_EQ((*stats)[0].min_value, 5000.0);
}

namespace {
// No null columns: column 0 is always set, so every row present in the data file is one valid_count of column 0.
XStore_Proto::RowData RemovalRaceRow(uint64_t v) {
    XStore_Proto::RowData row;
    for (uint64_t c = 0; c < 4; ++c) row.add_fielddata()->set_doubledata(static_cast<double>(v + c));
    return row;
}

// The year's zone map counts exactly the rows of its data file (column 0, per block, rows [0, rows_bound]), with their
// min/max, and its bloom has every row's column-0 value (a missing file counts as no rows). Run with nothing in flight.
void ExpectIndexesMatchTheYearFile(const std::string &db_path, unsigned short year, uint16_t row_length,
                                   uint64_t rows_bound) {
    SCOPED_TRACE(db_path + "/" + std::to_string(year));
    std::vector<std::pair<uint64_t, XStore_Proto::RowData>> rows;
    const int fd = open((db_path + "/" + std::to_string(year) + ".XSTORE").c_str(), O_RDONLY);
    if (fd >= 0) {
        rows = ReadRows(fd, 0, rows_bound, row_length);
        close(fd);
    }
    const uint64_t rpb = RowsPerBlock(row_length);
    std::map<uint64_t, std::vector<double>> by_block;
    for (const auto &[r, row] : rows) {
        double v = 0;
        ASSERT_TRUE(row.fielddata_size() > 0 && NumericOf(row.fielddata(0), v)) << "row " << r;
        by_block[r / rpb].push_back(v);
    }
    const ZoneMapFile::Pin zmap = XStore::PinZoneMap(db_path, year, row_length);
    const CatIdxFile::Pin catidx = XStore::PinCatIdx(db_path, year, row_length);
    for (uint64_t b = 0; b <= rows_bound / rpb; ++b) {
        const auto stats = zmap->TryGetBlockStats(b);
        ASSERT_TRUE(stats.has_value()) << "block " << b;
        const auto it = by_block.find(b);
        const uint64_t want = it == by_block.end() ? 0 : it->second.size();
        EXPECT_EQ((*stats)[0].valid_count, want) << "block " << b << ": the zone map does not count the file's rows";
        if (want > 0) {
            EXPECT_EQ((*stats)[0].min_value, *std::min_element(it->second.begin(), it->second.end())) << "block " << b;
            EXPECT_EQ((*stats)[0].max_value, *std::max_element(it->second.begin(), it->second.end())) << "block " << b;
        }
    }
    for (const auto &[r, row] : rows) {
        EXPECT_TRUE(catidx->MightContain(r / rpb, 0, EncodeFDataForCatIdx(row.fielddata(0)))) << "row " << r;
    }
}
} // namespace

// Stress (real concurrency, bounded rounds): in each round two inserters (BYPASS on their own thread, SYNC through the
// WAL writers) append fresh rows to a year while the test removes that year's data file once, mid-stream; the inserts
// go on into the re-created file, then stop. The year's indexes must then describe exactly the rows of the year file
// there is: no stats of rows that went into the deleted file (an old fd paired with a fresh index), no rows of the new
// file whose stats went into a retired object, no cached fd of the deleted file. Inserts that meet the removal may
// fail; they must never throw. Each round ends with a quiet removal, so rows never repeat within one file.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesRacingInsertsLeavesIndexesThatMatchTheData) {
    XStore store;
    const std::string db = "REGRMRACE";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    XStore_Proto::RemoveDBFiles_REQ req;
    req.add_filename(std::to_string(kYear) + ".XSTORE");
    for (int round = 0; round < 40; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        std::atomic<bool> running{true};
        std::atomic<uint64_t> next_row{0};
        std::vector<std::thread> inserters;
        for (int t = 0; t < 2; ++t) {
            inserters.emplace_back([&, t] {
                XStore w;
                const XStore_Proto::WalDurabilityMode mode = t == 0 ? XStore_Proto::BYPASS : XStore_Proto::SYNC;
                while (running.load()) {
                    const uint64_t r = next_row.fetch_add(1);
                    XStore_Proto::UnaryInsert_REP rep;
                    EXPECT_NO_THROW(w.UnaryInsert(Epoch(r), RemovalRaceRow(r * 7), db, &rep, mode));
                }
            });
        }
        std::this_thread::sleep_for(std::chrono::microseconds(300 + 97 * (round % 11)));
        XStore_Proto::RemoveDBFiles_REP rep;
        {
            XStore r;
            EXPECT_NO_THROW(rep = r.RemoveDBFiles(&req, db));
        }
        for (const std::string &s : rep.removestatus()) { // the only acceptable error: no insert had created it yet
            if (s.rfind("[INFO] ", 0) != 0) EXPECT_EQ(s, "[ERROR] [REMOVE DB FILES] [DB: REGRMRACE] - Unable to remove 1980.XSTORE");
        }
        std::this_thread::sleep_for(std::chrono::microseconds(300 + 89 * (round % 7)));
        running.store(false);
        for (std::thread &t : inserters) t.join();

        ExpectIndexesMatchTheYearFile(DbPath(db), kYear, kRowLength, next_row.load());
        if (HasFailure()) return;
        (void)store.RemoveDBFiles(&req, db);
    }
}

// Stress (real concurrency, bounded rounds): the same for RemoveDB + CreateDB under the same name (same row length),
// with the inserts spread over two years. RemoveDB must never throw and must report success (a year file created
// inside the directory while remove_all walks it is removed by its retry). A different row length is not used: an
// insert that read the old .CFG before the removal and applies after the re-create writes the old geometry into the new
// DB, which stays open (it needs a per-DB lock or generation check on the insert path).
TEST_F(XStoreLiveWidenTest, RemoveDbAndCreateDbRacingInsertsLeaveIndexesThatMatchTheData) {
    XStore store;
    const std::string db = "REGRMDBRACE";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    for (int round = 0; round < 30; ++round) {
        SCOPED_TRACE("round " + std::to_string(round));
        std::atomic<bool> running{true};
        std::atomic<uint64_t> next_row{0};
        std::vector<std::thread> inserters;
        for (int t = 0; t < 2; ++t) {
            inserters.emplace_back([&, t] {
                XStore w;
                const XStore_Proto::WalDurabilityMode mode = t == 0 ? XStore_Proto::BYPASS : XStore_Proto::SYNC;
                while (running.load()) {
                    const uint64_t r = next_row.fetch_add(1);
                    const unsigned long long epoch = r % 2 == 0 ? Epoch(r) : Jan1(kNextYear) + r;
                    XStore_Proto::UnaryInsert_REP rep;
                    EXPECT_NO_THROW(w.UnaryInsert(epoch, RemovalRaceRow(r * 7), db, &rep, mode));
                }
            });
        }
        std::this_thread::sleep_for(std::chrono::microseconds(300 + 97 * (round % 11)));
        {
            XStore r;
            // An [ERROR] is allowed (a queued SYNC insert can re-create a year file between remove_all's two attempts);
            // a retry of RemoveDB must then finish the job.
            XStore_Proto::RemoveDB_REP rep;
            for (int attempt = 0; attempt < 5; ++attempt) {
                EXPECT_NO_THROW(rep = r.RemoveDB(db));
                if (rep.removestatus().rfind("[INFO] ", 0) == 0) break;
                EXPECT_EQ(rep.removestatus().rfind("[ERROR] ", 0), 0u) << rep.removestatus();
            }
            EXPECT_EQ(rep.removestatus().rfind("[INFO] ", 0), 0u) << rep.removestatus(); // no ASSERT: inserters run
            EXPECT_FALSE(std::filesystem::exists(DbPath(db)));
            EXPECT_EQ(r.CreateDB(db, XStore_Proto::SECOND, kRowLength).createdbstatus().rfind("[INFO] ", 0), 0u);
        }
        std::this_thread::sleep_for(std::chrono::microseconds(300 + 89 * (round % 7)));
        running.store(false);
        for (std::thread &t : inserters) t.join();

        ExpectIndexesMatchTheYearFile(DbPath(db), kYear, kRowLength, next_row.load());
        ExpectIndexesMatchTheYearFile(DbPath(db), kNextYear, kRowLength, next_row.load());
        if (HasFailure()) return;
        ASSERT_EQ(store.RemoveDB(db).removestatus().rfind("[INFO] ", 0), 0u);
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    }
}

// ---- 10. The trigger (M4 stage 3): RunLiveWidenPass driven with a fake clock ----------------------------------------

using PassResult = std::vector<std::pair<unsigned short, Result>>;
PassResult YearsAndResults(const std::vector<XStore::LiveWidenPassEntry> &pass) {
    PassResult out;
    for (const XStore::LiveWidenPassEntry &e : pass) out.emplace_back(e.year, e.result);
    return out;
}
constexpr const char *kBeingIndexed = "being indexed in the background";
using std::chrono::seconds;

// Waits until the live widen of (path, year)'s zone map (catidx: its catidx) has published a watermark >= done_rows
// (and, with want_slowed, a "slowed" reason), or fails after 20 s.
XStore::LiveRebuildProgress WaitForRebuildProgress(const std::string &path, unsigned short year, uint64_t done_rows,
                                                   bool want_slowed = false, bool catidx = false) {
    const Clock::time_point deadline = Clock::now() + seconds(20);
    XStore::LiveRebuildProgress p;
    while (Clock::now() < deadline) {
        if (XStore::FindRebuildProgress(path, year, catidx, p) && p.done_rows >= done_rows &&
            (!want_slowed || p.slowed != nullptr)) {
            return p;
        }
        std::this_thread::sleep_for(milliseconds(1));
    }
    ADD_FAILURE() << "the live widen never published a watermark >= " << done_rows << (want_slowed ? " slowed" : "");
    return p;
}

std::string MaxStatus(XStore &store, const std::string &db, unsigned long long lo, unsigned long long hi, unsigned short c) {
    XStore_Proto::Max_REP rep;
    store.MaxAggregate(lo, hi, db, c, &rep);
    return rep.opstatus();
}

std::string FindStatus(XStore &store, const std::string &db, unsigned long long lo, unsigned long long hi, int32_t c) {
    std::string buf;
    store.FindByValue(lo, hi, db, c, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::FindByValue_REP rep;
    EXPECT_TRUE(rep.ParseFromString(buf));
    return rep.querystatus();
}

size_t Occurrences(const std::string &s, const std::string &what) {
    size_t n = 0;
    for (size_t at = s.find(what); at != std::string::npos; at = s.find(what, at + 1)) ++n;
    return n;
}

// ---- Rebuild notice (Tech Reference 4.18 "Rebuild notice") ---------------------------------------------------------

TEST_F(XStoreLiveWidenTest, RebuildNoticeTextCarriesTheRoundedDownPercentAndTheSlowedReason) {
    const std::string tail = " (a rebuild that was interrupted finishes at the next start)";
    XStore::LiveRebuildProgress p{2, 3, nullptr};
    EXPECT_EQ(XStore::RebuildingColumnNoticeText("MAX AGGREGATE", "DB", 65, 2009, &p),
              "[WARNING] [MAX AGGREGATE] [DB: DB] - column 65 is being indexed in the background in 2009 (66% done); "
              "the part not yet rebuilt is answered by full scan" + tail);
    p.slowed = "writers waiting";
    EXPECT_EQ(XStore::RebuildingColumnNoticeText("MAX AGGREGATE", "DB", 5, 2009, &p),
              "[WARNING] [MAX AGGREGATE] [DB: DB] - column 5 is being indexed in the background in 2009 (66% done; "
              "slowed: writers waiting); the part not yet rebuilt is answered by scan, results are correct" + tail);
    const auto pct = [](uint64_t done, uint64_t total) {
        XStore::LiveRebuildProgress q{done, total, nullptr};
        const std::string text = XStore::RebuildingColumnNoticeText("SUM AGGREGATE", "DB", 5, 2009, &q);
        const size_t open = text.find("2009 (");
        return text.substr(open + 6, text.find('%') - open - 6);
    };
    EXPECT_EQ(pct(0, 0), "0");
    EXPECT_EQ(pct(0, 1000), "0");
    EXPECT_EQ(pct(9, 1000), "0");
    EXPECT_EQ(pct(999, 1000), "99");
    EXPECT_EQ(pct(1000, 1000), "100");
    EXPECT_EQ(XStore::RebuildingColumnNoticeText("FINDBYVALUE", "DB", 65, 2009),
              "[WARNING] [FINDBYVALUE] [DB: DB] - column 65 is being indexed in the background in 2009; answered by full "
              "scan until it finishes" + tail) << "no live-widen status: the old text";

    using livewiden::GovernorReason;
    EXPECT_STREQ(livewiden::SlowedText(1, 4, GovernorReason::kWriterWait), "writers waiting");
    EXPECT_STREQ(livewiden::SlowedText(1, 4, GovernorReason::kLongWaits), "long writer waits");
    EXPECT_STREQ(livewiden::SlowedText(3, 4, GovernorReason::kForegroundCpu), "foreground CPU");
    EXPECT_EQ(livewiden::SlowedText(1, 4, GovernorReason::kRampUp), nullptr);
    EXPECT_EQ(livewiden::SlowedText(1, 4, GovernorReason::kStart), nullptr);
    EXPECT_EQ(livewiden::SlowedText(1, 4, GovernorReason::kNone), nullptr);
    EXPECT_EQ(livewiden::SlowedText(4, 4, GovernorReason::kWriterWait), nullptr) << "at its maximum it is not slowed";

    // Nothing rebuilt (the fixture dropped every status): the counter is zero and a low column gets no notice.
    EXPECT_EQ(XStore::YearsRebuilding(), 0u);
    XStore store;
    const std::string db = "LWNOTICE0";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(1, 10)}});
    EXPECT_FALSE(XStore::FindRebuildProgress(DbPath(db), kYear, false, p));
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(10), 5), "SUCCESS");
}

// A widen held at a known watermark (a WRITE lock on row kStuck, which the builder can only wait for; no query may
// cover that row while it is held: its READ lock would wait too): (a) a low column over rows past the watermark is
// right and says how far the rebuild is; (b) a range below the watermark is answered from the
// index, no notice; (c) a column >= 64 always says so; (d) another year: nothing; (e) after the widen: nothing, and the
// counter is back where it was. keep_old=false: this is the in-place rebuild's notice (a low column is noticed past the
// watermark); with keep-old (the default since 2026-10-17) low columns are silent, see
// KeepOldAnswersOldColumnsFromTheOldIndexWhileTheWidenIsHeld.
TEST_F(XStoreLiveWidenTest, RebuildNoticeShowsProgressOnTheRebuildingYearOnlyAndEndsWithTheRebuild) {
    XStore store;
    const std::string db = "LWNOTICE";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    Rows other;
    for (uint64_t r = 0; r < 100; ++r) other.emplace_back(Jan1(1979) + r, ValueRow(r + 1, 70)); // born 70 wide
    Insert(store, db, other);
    const std::string path = DbPath(db);
    const uint32_t before = XStore::YearsRebuilding();
    const uint64_t kStuck = 10 * Rpb() + 7;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr, 1, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, nullptr,
                                  /*keep_old=*/false);
    });
    // (no ASSERT while the lock is held: the builder would never end)
    const XStore::LiveRebuildProgress p = WaitForRebuildProgress(path, kYear, kStuck);
    EXPECT_EQ(p.done_rows, kStuck);
    EXPECT_EQ(XStore::YearsRebuilding(), before + 1);
    const uint64_t total = static_cast<uint64_t>(std::filesystem::file_size(YearPath(db, ".XSTORE"))) / kRowLength;
    EXPECT_EQ(p.total_rows, total);
    EXPECT_EQ(p.slowed, nullptr) << "ungoverned";
    const std::string state = " in 1980 (" + std::to_string(kStuck * 100 / total) + "% done)";

    const std::string low = MaxStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 5);                // (a)
    EXPECT_NE(low.find(kBeingIndexed + state + "; the part not yet rebuilt is answered by scan, results are correct"),
              std::string::npos) << low;
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kStuck - 1), 5), "SUCCESS");                         // (b)
    const std::string high = MaxStatus(store, db, Epoch(0), Epoch(10), 65);                              // (c)
    EXPECT_NE(high.find(kBeingIndexed + state + "; the part not yet rebuilt is answered by full scan"), std::string::npos)
        << high;
    EXPECT_NE(FindStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 5).find(kBeingIndexed + state), std::string::npos);
    EXPECT_EQ(FindStatus(store, db, Epoch(0), Epoch(kStuck - 1), 5).find(kBeingIndexed), std::string::npos);
    EXPECT_NE(FindStatus(store, db, Epoch(0), Epoch(10), 65).find(kBeingIndexed + state), std::string::npos);
    EXPECT_EQ(MaxStatus(store, db, Jan1(1979), Jan1(1979) + 99, 5), "SUCCESS");                         // (d)
    EXPECT_EQ(MaxStatus(store, db, Jan1(1979), Jan1(1979) + 99, 65), "SUCCESS");
    const std::string both = MaxStatus(store, db, Jan1(1979), Epoch(10), 65);
    EXPECT_EQ(Occurrences(both, kBeingIndexed), 1u) << both;
    EXPECT_NE(both.find(kBeingIndexed + state), std::string::npos) << both;
    ExpectAnswersMatchTheData(store, db, 0, kStuck - 1, {5, 65});
    ExpectAnswersMatchTheData(store, db, kStuck + 1, kRows - 1, {5, 65});

    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    ASSERT_EQ(builder.wait_for(seconds(60)), std::future_status::ready);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_EQ(XStore::YearsRebuilding(), before) << "a finished year is no longer counted";                // (e)
    XStore::LiveRebuildProgress q;
    EXPECT_FALSE(XStore::FindRebuildProgress(path, kYear, false, q));
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 5), "SUCCESS");
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 65), "SUCCESS");
    EXPECT_EQ(FindStatus(store, db, Epoch(0), Epoch(kRows - 1), 5).find(kBeingIndexed), std::string::npos);
}

// Keep-old (LIVE_WIDEN_KEEP_OLD_INDEX) held at a known watermark: an index born 70 wide, rows 100 wide, widened to 100.
// Columns below the old width (5, 66) are answered from the old index past the watermark (no notice, blocks served from
// it, right answers); a column at or past it (80) still says how far the rebuild is. After the widen nothing is said.
TEST_F(XStoreLiveWidenTest, KeepOldAnswersOldColumnsFromTheOldIndexWhileTheWidenIsHeld) {
    XStore store;
    const std::string db = "LWKEEPNOTICE";
    constexpr uint64_t kRows = 20000;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 70)}}); // index files born 70 wide
    Rows batch;
    for (uint64_t r = 1; r < kRows; ++r) batch.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : 100));
    Insert(store, db, batch);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 70u);
    const uint64_t kStuck = 10 * Rpb() + 7;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 100, true, true, nullptr, nullptr, 1,
                                  XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, nullptr, /*keep_old=*/true);
    });
    // (no ASSERT while the lock is held: the builder would never end)
    const XStore::LiveRebuildProgress p = WaitForRebuildProgress(path, kYear, kStuck);
    EXPECT_EQ(p.old_width, 70u);
    XStore::LiveRebuildProgress pc;
    EXPECT_TRUE(XStore::FindRebuildProgress(path, kYear, /*catidx=*/true, pc));
    EXPECT_EQ(pc.old_width, 70u);
    EXPECT_EQ(zmap.OldWidth(), 70u);
    EXPECT_EQ(zmap.TrackedColumnCount(), 100u);

    const uint64_t served = zmap.ServedFromOldForTests();
    EXPECT_EQ(MaxStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 5), "SUCCESS");
    EXPECT_EQ(MaxStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 66), "SUCCESS");
    const uint64_t blocks_past = (kRows - 1) / Rpb() - (kStuck + 1) / Rpb() + 1;
    EXPECT_GE(zmap.ServedFromOldForTests() - served, 2 * blocks_past) << "every block past the watermark, both queries";
    const std::string high = MaxStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 80); // never over the held row
    EXPECT_NE(high.find(kBeingIndexed), std::string::npos) << high;
    EXPECT_EQ(FindStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 5).find(kBeingIndexed), std::string::npos);
    EXPECT_EQ(FindStatus(store, db, Epoch(0), Epoch(10), 66).find(kBeingIndexed), std::string::npos);
    EXPECT_NE(FindStatus(store, db, Epoch(0), Epoch(10), 80).find(kBeingIndexed), std::string::npos);
    ExpectAnswersMatchTheData(store, db, 0, kStuck - 1, {5, 66, 80});
    ExpectAnswersMatchTheData(store, db, kStuck + 1, kRows - 1, {5, 66, 80});

    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    ASSERT_EQ(builder.wait_for(seconds(60)), std::future_status::ready);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_EQ(zmap.OldWidth(), 0u);
    XStore::LiveRebuildProgress q;
    EXPECT_FALSE(XStore::FindRebuildProgress(path, kYear, false, q));
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 80), "SUCCESS");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {5, 66, 80});
}

// The writer's batch build in ApplyWalRecord (one UpdateRows per run, catidx values hashed only below the file's width)
// through the real insert paths, on index files 64 wide while the rows are 70 wide (before a widen): the index bytes
// equal a reference fed the same rows through UpdateRow one by one, with every observation the producers build
// (all columns up to the record's width; pad = the record's width). Fresh rows only: no overwrite marks.
TEST_F(XStoreLiveWidenTest, InsertsWiderThanTheIndexWriteTheBytesOfTheRowByRowFold) {
    XStore store;
    const std::string db = "LWBATCHBYTES";
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70); // row 0 10 wide, then one BatchInsert record
    {   // one RangeInsert record crossing blocks, then a UnaryInsert
        XStore_Proto::RangeInsert_REQ req;
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t r = kRows; r < kRows + 1500; ++r) {
            XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : 70);
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        ASSERT_NE(rep.insertstatus().find("successfully"), std::string::npos) << rep.insertstatus();
        XStore_Proto::UnaryInsert_REP urep;
        store.UnaryInsert(Epoch(kRows + 2000), ValueRow(5, 70), db, &urep, XStore_Proto::SYNC);
    }
    ASSERT_EQ(XStore::GetOrOpenCatIdx(DbPath(db), kYear, kRowLength).TrackedColumnCount(), 64u) << "still narrow";

    ZoneMapFile zref;
    CatIdxFile cref;
    ASSERT_TRUE(zref.Open((scratch->test_root / "ref.zmap").string(), kRowLength, 64));
    ASSERT_TRUE(cref.Open((scratch->test_root / "ref.catidx").string(), kRowLength, 64));
    const auto fold = [&](uint64_t r, const XStore_Proto::RowData &row, uint32_t record_width) {
        std::vector<ZoneMapObservation> zobs;
        std::vector<CatIdxObservation> cobs;
        for (int c = 0; c < std::min(static_cast<int>(record_width), row.fielddata_size()); ++c) {
            const XStore_Proto::FData &f = row.fielddata(c);
            const bool numeric = f.oneOfData_case() == XStore_Proto::FData::kDoubleData;
            zobs.push_back({static_cast<uint32_t>(c), numeric ? f.doubledata() : 0.0, numeric});
            if (f.oneOfData_case() != XStore_Proto::FData::ONEOFDATA_NOT_SET) {
                cobs.push_back({static_cast<uint32_t>(c), EncodeFDataForCatIdx(f), true});
            }
        }
        zref.UpdateRow(r, zobs, record_width);
        cref.UpdateRow(r, cobs);
    };
    fold(0, ValueRow(0, 10), 64); // a new DB's first record is 64 wide (the default floor)
    for (uint64_t r = 1; r < kRows + 1500; ++r) fold(r, ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : 70), 70);
    fold(kRows + 2000, ValueRow(5, 70), 70);

    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".zmap")) == ReadFileBytes((scratch->test_root / "ref.zmap").string()));
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".catidx")) == ReadFileBytes((scratch->test_root / "ref.catidx").string()));
}

// Keep-old widen stopped through its stop flag: the old index keeps answering the blocks past F (no notice for its
// columns), MarkClean refuses, a clean stop leaves the files dirty, and the next start rebuilds them.
TEST_F(XStoreLiveWidenTest, KeepOldStoppedWidenKeepsTheOldIndexServingUntilTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWKEEPSTOP";
    constexpr uint64_t kRows = 60000;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 70)}}); // index files born 70 wide
    Rows batch;
    for (uint64_t r = 1; r < kRows; ++r) batch.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, r % 5 == 0 ? 40 : 100));
    Insert(store, db, batch);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    ASSERT_EQ(zmap.TrackedColumnCount(), 70u);
    std::atomic<bool> stop{false};
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 100, true, true, &stop, nullptr, 1,
                                  XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, nullptr, /*keep_old=*/true);
    });
    WaitForFrontierPast(zmap, 3 * Rpb());
    stop.store(true);
    ASSERT_EQ(builder.get(), Result::kStopped);

    const uint64_t f = zmap.FrontierForTests();
    ASSERT_NE(f, ZoneMapFile::kNoRebuild);
    ASSERT_LT(f + 2 * Rpb(), kRows) << "stopped too late to mean anything";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_EQ(zmap.OldWidth(), 70u);
    EXPECT_EQ(catidx.OldWidth(), 70u);
    const uint64_t past = f / Rpb() + 1;
    auto old_block = zmap.TryGetBlockStats(past);
    ASSERT_TRUE(old_block.has_value()) << "a block past F is answered from the old index";
    EXPECT_EQ(old_block->size(), 70u);
    const uint64_t served = zmap.ServedFromOldForTests();
    EXPECT_EQ(MaxStatus(store, db, Epoch(f + 1), Epoch(kRows - 1), 5), "SUCCESS");
    EXPECT_EQ(MaxStatus(store, db, Epoch(f + 1), Epoch(kRows - 1), 66), "SUCCESS");
    EXPECT_GT(zmap.ServedFromOldForTests(), served);
    EXPECT_NE(MaxStatus(store, db, Epoch(f + 1), Epoch(kRows - 1), 80).find(kBeingIndexed), std::string::npos);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {5, 66, 80});
    EXPECT_FALSE(zmap.MarkClean());
    EXPECT_FALSE(catidx.MarkClean());

    XStore::Shutdown(); // clean stop: the half-built index stays dirty
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, "LWKEEPSTOPREF", 100); // its InitDB also rebuilds LWKEEPSTOP's dirty index
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).tracked_column_count, 100u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).OldWidth(), 0u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).OldWidth(), 0u);
    ExpectSameFilesAs(db, "LWKEEPSTOPREF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {5, 66, 80});
}

// ---- Progress log (Tech Reference 4.18 "Progress log") -------------------------------------------------------------

// The lines of the live widen's progress reporter, captured through Options::log_line (the sink of the test-only
// `progress` argument of RunLiveWidenPass), with a tiny interval so a widen is sampled many times.
struct ProgressLines {
    std::mutex mutex;
    std::vector<std::string> lines;

    rebuildprogress::Options Options(milliseconds interval = milliseconds(2)) {
        rebuildprogress::Options o;
        o.log_interval = interval;
        o.log_line = [this](const std::string &line) {
            std::lock_guard<std::mutex> lock(mutex);
            lines.push_back(line);
        };
        return o;
    }
    std::vector<std::string> Copy() {
        std::lock_guard<std::mutex> lock(mutex);
        return lines;
    }
    // Waits (20 s at most) for a line containing `what`.
    bool WaitFor(const std::string &what) {
        const Clock::time_point deadline = Clock::now() + std::chrono::seconds(20);
        while (Clock::now() < deadline) {
            for (const std::string &l : Copy()) {
                if (l.find(what) != std::string::npos) return true;
            }
            std::this_thread::sleep_for(milliseconds(1));
        }
        return false;
    }
};

struct ParsedProgress {
    int percent = -1;
    size_t years_done = 0, years_total = 0;
    double rows_done = 0, rows_total = 0; // from the SI text ("13.0M of 41.7M rows"), so rounded
};
double FromSi(const std::string &text) {
    double v = std::stod(text);
    if (const char c = text.back(); c == 'K') v *= 1e3;
    else if (c == 'M') v *= 1e6;
    else if (c == 'G') v *= 1e9;
    return v;
}
ParsedProgress ParseProgress(const std::string &line) {
    static const std::regex re(R"( - (\d+)% \((\S+) of (\S+) rows\), (\d+)/(\d+) years done)");
    std::smatch m;
    ParsedProgress p;
    if (std::regex_search(line, m, re)) {
        p.percent = std::stoi(m[1]);
        p.rows_done = FromSi(m[2]);
        p.rows_total = FromSi(m[3]);
        p.years_done = std::stoul(m[4]);
        p.years_total = std::stoul(m[5]);
    } else {
        ADD_FAILURE() << "not a progress line: " << line;
    }
    return p;
}

// Three years of one DB widened 64 -> 70 (a fourth, born at 70, needs nothing). The second year (1980) is held by a
// WRITE lock on one row until a "1/3 years done" line has been logged: the lines before it stay below 100%.
TEST_F(XStoreLiveWidenTest, ProgressLogCountsTheYearsToWidenAndEndsAt100Percent) {
    XStore store;
    const std::string db = "LWPROG";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    const std::vector<unsigned short> years = {1979, 1980, 1981};
    for (unsigned short y : years) Insert(store, db, {{Jan1(y), ValueRow(1, 10)}}); // index files born 64 wide
    Rows rows;
    for (unsigned short y : years) {
        for (uint64_t r = 1; r < 4000; ++r) rows.emplace_back(Jan1(y) + r, ValueRow((r * 7 + y) % 100000, r % 5 == 0 ? 40 : 70));
    }
    Insert(store, db, rows); // growth 64 -> 70
    Insert(store, db, {{Jan1(1982) + 1, ValueRow(9, 70)}}); // born 70 wide: not widened
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(DbPath(db));
    constexpr uint64_t kStuck = 2000;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE", 1980));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    ProgressLines capture;
    const rebuildprogress::Options options = capture.Options();
    auto future = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 1, nullptr, nullptr, &options);
    });
    // (no ASSERT while the lock is held: the pass would never end)
    const bool one_done = capture.WaitFor(", 1/3 years done");
    const std::vector<std::string> held = capture.Copy();
    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    EXPECT_TRUE(one_done) << "no line with the first year done";
    ASSERT_EQ(future.wait_for(seconds(60)), std::future_status::ready);
    const auto pass = future.get();
    ASSERT_EQ(pass.size(), 4u);
    for (const std::string &line : held) EXPECT_LT(ParseProgress(line).percent, 100) << "held back by the locked row: " << line;

    const std::vector<std::string> lines = capture.Copy();
    ASSERT_FALSE(lines.empty());
    int last_percent = 0;
    size_t last_done = 0;
    for (const std::string &line : lines) {
        SCOPED_TRACE(line);
        EXPECT_EQ(line.rfind("[LIVE WIDEN] [DB: LWPROG] - ", 0), 0u);
        EXPECT_EQ(line.find(" | workers"), std::string::npos) << "ungoverned: no governor text";
        const ParsedProgress p = ParseProgress(line);
        EXPECT_EQ(p.years_total, 3u) << "the years this pass widens";
        EXPECT_LE(p.rows_done, p.rows_total);
        EXPECT_GE(p.percent, last_percent);
        EXPECT_GE(p.years_done, last_done);
        last_percent = p.percent;
        last_done = p.years_done;
    }
    const ParsedProgress last = ParseProgress(lines.back());
    EXPECT_EQ(last.percent, 100);
    EXPECT_EQ(last.years_done, 3u);
}

// Frozen mid-way by a WRITE lock on one row (the technique of the rebuild notice tests): the lines show the partial
// percent of the year and the DB label, then the final line reaches 100%.
TEST_F(XStoreLiveWidenTest, ProgressLogShowsThePartialPercentWhileTheWidenIsHeld) {
    XStore store;
    const std::string db = "LWPROGHELD";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    const uint64_t kStuck = 10 * Rpb() + 7;
    const uint64_t total = static_cast<uint64_t>(std::filesystem::file_size(YearPath(db, ".XSTORE"))) / kRowLength;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    ProgressLines capture;
    const rebuildprogress::Options options = capture.Options();
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 1, nullptr, nullptr, &options);
    });
    // (no ASSERT while the lock is held: the pass would never end)
    const std::string want = "[LIVE WIDEN] [DB: LWPROGHELD] - " + std::to_string(kStuck * 100 / total) + "% (";
    const bool seen = capture.WaitFor(want);
    for (const std::string &line : capture.Copy()) {
        EXPECT_LE(ParseProgress(line).percent, static_cast<int>(kStuck * 100 / total)) << "held back by the locked row: " << line;
        EXPECT_EQ(ParseProgress(line).years_total, 1u);
        EXPECT_LE(ParseProgress(line).rows_done, ParseProgress(line).rows_total) << line;
    }
    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    EXPECT_TRUE(seen) << "no line with the partial percent";
    ASSERT_EQ(pass.wait_for(seconds(60)), std::future_status::ready);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kDone}}));
    const std::vector<std::string> lines = capture.Copy();
    ASSERT_FALSE(lines.empty());
    EXPECT_EQ(ParseProgress(lines.back()).percent, 100);
    EXPECT_EQ(ParseProgress(lines.back()).years_done, 1u);
}

// A DB whose years all have the right width starts no reporter: not even the final line.
TEST_F(XStoreLiveWidenTest, ProgressLogStartsNothingWhenNoYearNeedsWidening) {
    XStore store;
    const std::string db = "LWPROGNONE";
    SeedNarrowIndexWideRows(store, db, 20000, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr), Result::kDone); // widened already
    ProgressLines capture;
    const rebuildprogress::Options options = capture.Options(milliseconds(1));
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g + seconds(30), 1, nullptr, nullptr, &options)),
              (PassResult{{kYear, Result::kNothingToDo}}));
    EXPECT_TRUE(capture.Copy().empty());
}

// The stop flag (RemoveDB's and the shutdown's) ends the pass promptly with the reporter joined: it has emitted its
// final line by the time the pass returns, and a later pass of the same DB (flag cleared) reports again.
TEST_F(XStoreLiveWidenTest, ProgressLogReporterIsJoinedWhenTheStopFlagEndsThePass) {
    XStore store;
    const std::string db = "LWPROGSTOP";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    ProgressLines capture;
    const rebuildprogress::Options options = capture.Options();
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 1, nullptr, nullptr, &options);
    });
    WaitForFrontierPast(zmap, Rpb());
    EXPECT_TRUE(capture.WaitFor("[LIVE WIDEN] [DB: LWPROGSTOP] - "));
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended) << "the pass ignored the cancel flag";
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    // Only the final sample (taken when the reporter is stopped, which joins it) says "stopped before the end": it is
    // the last line by the time the pass has returned.
    const size_t count = capture.Copy().size();
    ASSERT_GT(count, 0u);
    EXPECT_NE(capture.Copy().back().find("stopped before the end"), std::string::npos) << capture.Copy().back();
    std::this_thread::sleep_for(milliseconds(50)); // a reporter thread left running would log more
    EXPECT_EQ(capture.Copy().size(), count) << "the reporter was still running after the pass returned";
}

// Real growth through MaybeGrowColumnCount, debounce, then one pass widens every year of the DB, newest first, to the
// exact width, byte-identical to a startup rebuild of each year. Queries are right before, during (a query thread runs
// while the pass works) and after.
TEST_F(XStoreLiveWidenTest, TheTriggerWidensEveryYearNewestFirstAfterTheQuietPeriod) {
    XStore store;
    const std::string db = "LWTRIG";
    const std::string path = DbPath(db);
    const std::vector<unsigned short> years = {1979, 1980, 1981};
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    for (unsigned short y : years) Insert(store, db, {{Jan1(y), ValueRow(1, 10)}}); // index files born 64 wide
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "a width <= 64 needs no widen";
    Rows rows;
    for (unsigned short y : years) {
        for (uint64_t r = 1; r < 4000; ++r) rows.emplace_back(Jan1(y) + r, ValueRow((r * 7 + y) % 100000, r % 5 == 0 ? 40 : 70));
    }
    Insert(store, db, rows); // growth 64 -> 70
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_TRUE(store.RunLiveWidenPass(g1 + seconds(29)).empty()) << "inside the quiet period";

    Insert(store, db, {{Jan1(1980) + 5000, ValueRow(5, 80)}}); // growth 70 -> 80: restarts the quiet period
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_GT(g2, g1);
    EXPECT_TRUE(store.RunLiveWidenPass(g1 + seconds(30)).empty()) << "the second growth did not restart the quiet period";
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    // A year file created after the growth is born at the new width; it is dirty (as every file written at runtime) and
    // must not be rebuilt: the decision is made on widths only.
    Insert(store, db, {{Jan1(1982) + 1, ValueRow(9, 80)}});
    ASSERT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap", 1982)).dirty, 1u);
    ASSERT_EQ(XStore::GetOrOpenZoneMap(path, 1982, kRowLength).TrackedColumnCount(), 80u);

    // Before: right answers; column 65 is scanned and says so.
    for (unsigned short y : years) ExpectAnswersMatchTheData(store, db, 0, 3999, {0, 65}, y);
    XStore_Proto::Max_REP before;
    store.MaxAggregate(Jan1(1980), Jan1(1980) + 3999, db, 65, &before);
    EXPECT_NE(before.opstatus().find(kNotIndexed), std::string::npos) << before.opstatus();

    // During: a query thread checks MAX/SUM of column 65 per year against values computed from the data up front.
    std::map<unsigned short, std::pair<double, double>> oracle; // year -> (max, sum)
    for (unsigned short y : years) {
        const int fd = open(YearPath(db, ".XSTORE", y).c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        double mx = -INFINITY, sum = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, 3999, kRowLength)) {
            double v = 0;
            if (row.fielddata_size() > 65 && NumericOf(row.fielddata(65), v)) {
                mx = std::max(mx, v);
                sum += v;
            }
        }
        close(fd);
        oracle[y] = {mx, sum};
    }
    std::atomic<bool> running{true};
    std::atomic<int> queries{0}, mismatches{0};
    std::thread querier([&] {
        XStore q;
        while (running.load()) {
            for (unsigned short y : years) {
                XStore_Proto::Max_REP max_rep;
                q.MaxAggregate(Jan1(y), Jan1(y) + 3999, db, 65, &max_rep);
                XStore_Proto::Sum_REP sum_rep;
                q.SumAggregate(Jan1(y), Jan1(y) + 3999, db, 65, &sum_rep);
                if (max_rep.opresult().doubledata() != oracle[y].first || sum_rep.opresult() != oracle[y].second) {
                    if (mismatches.fetch_add(1) < 5) ADD_FAILURE() << "year " << y << ": " << max_rep.opstatus();
                }
                ++queries;
            }
        }
    });
    const auto pass = store.RunLiveWidenPass(g2 + seconds(30));
    running.store(false);
    querier.join();
    EXPECT_EQ(mismatches.load(), 0);
    EXPECT_GT(queries.load(), 0);
    EXPECT_EQ(YearsAndResults(pass), (PassResult{{1982, Result::kNothingToDo}, {1981, Result::kDone}, {1980, Result::kDone}, {1979, Result::kDone}}))
        << "every year, newest first; the year already at the new width untouched";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "the record must go once the DB is widened";
    EXPECT_TRUE(store.RunLiveWidenPass(g2 + seconds(3600)).empty());

    for (unsigned short y : years) {
        SCOPED_TRACE("year " + std::to_string(y));
        EXPECT_EQ(XStore::GetOrOpenZoneMap(path, y, kRowLength).TrackedColumnCount(), 80u) << "the exact width";
        EXPECT_EQ(XStore::GetOrOpenCatIdx(path, y, kRowLength).TrackedColumnCount(), 80u);
        EXPECT_FALSE(XStore::GetOrOpenZoneMap(path, y, kRowLength).Rebuilding());
        ExpectAnswersMatchTheData(store, db, 0, 3999, {0, 65}, y);
    }
    XStore_Proto::Max_REP after;
    store.MaxAggregate(Jan1(1980), Jan1(1980) + 3999, db, 65, &after);
    EXPECT_EQ(after.opstatus(), "SUCCESS");

    // Byte-identical to a startup rebuild of each year (sparse copies in a reference DB 80 wide; its InitDB keeps the
    // live DB's clean, right-width files as they are, and they are read before anyway).
    std::map<std::pair<unsigned short, std::string>, std::string> live;
    for (unsigned short y : years) {
        for (const char *ext : {".zmap", ".catidx"}) live[{y, ext}] = ReadFileBytes(YearPath(db, ext, y));
    }
    const std::string ref = "LWTRIGREF";
    CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
    Insert(store, ref, {{Jan1(1975), ValueRow(1, 80)}});
    for (unsigned short y : years) SparseCopy(YearPath(db, ".XSTORE", y), YearPath(ref, ".XSTORE", y));
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    store.InitDB();
    for (unsigned short y : years) {
        for (const char *ext : {".zmap", ".catidx"}) {
            EXPECT_TRUE((live[{y, ext}] == ReadFileBytes(YearPath(ref, ext, y)))) << y << ext << " differs from the startup rebuild";
        }
    }
}

// A growth while a pass is widening restarts the DB's quiet period and its record survives the pass; the next pass
// re-reads the widths and widens again, to the larger width. A WRITE lock on the hole row after the data keeps the
// builder from finishing until the test has grown the DB (and checked the notices of a rebuilding year).
TEST_F(XStoreLiveWidenTest, AGrowthDuringAPassKeepsTheRecordAndTheNextPassWidensFurther) {
    XStore store;
    const std::string db = "LWTRIGMID";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g1 + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    EXPECT_TRUE(zmap.Rebuilding()); // (no ASSERT while the lock is held: the pass would never end)

    // Notices on a rebuilding year: a column >= 64 says "being indexed in the background"; a lower one says it only
    // when the range reaches the watermark (here held at kRows, the locked row: the data below it is all folded).
    WaitForRebuildProgress(path, kYear, kRows);
    XStore_Proto::Max_REP high;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &high);
    EXPECT_NE(high.opstatus().find(kBeingIndexed), std::string::npos) << high.opstatus();
    XStore_Proto::Max_REP low;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 5, &low);
    EXPECT_EQ(low.opstatus(), "SUCCESS");
    std::string find_buf;
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 65, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { find_buf += chunk; });
    XStore_Proto::FindByValue_REP find_rep;
    EXPECT_TRUE(find_rep.ParseFromString(find_buf));
    EXPECT_NE(find_rep.querystatus().find(kBeingIndexed), std::string::npos) << find_rep.querystatus();
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    XStore_Proto::UnaryInsert_REP grow;
    store.UnaryInsert(Epoch(kRows + 500), ValueRow(3, 90), db, &grow); // 70 -> 90 while the pass runs
    EXPECT_NE(grow.insertstatus().find("[INFO]"), std::string::npos) << grow.insertstatus();
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_GT(g2, g1);
    EXPECT_TRUE(zmap.Rebuilding()) << "the growth did not land during the pass";
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_EQ(pass.wait_for(seconds(60)), std::future_status::ready);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kDone}}));
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "a growth arrived mid-pass: the record must stay";
    EXPECT_EQ(zmap.TrackedColumnCount(), 70u) << "that pass widened to the width it started with";
    XStore_Proto::Max_REP done;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &done);
    EXPECT_EQ(done.opstatus(), "SUCCESS") << "no notice once the rebuild is done";
    find_buf.clear();
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 65, FDouble(1.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { find_buf += chunk; });
    ASSERT_TRUE(find_rep.ParseFromString(find_buf));
    EXPECT_EQ(find_rep.querystatus().find(kBeingIndexed), std::string::npos) << find_rep.querystatus();
    EXPECT_EQ(find_rep.querystatus().find(kNotIndexed), std::string::npos) << find_rep.querystatus();

    EXPECT_TRUE(store.RunLiveWidenPass(g2 + seconds(29)).empty()) << "the new quiet period counts from the growth";
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))), (PassResult{{kYear, Result::kDone}}));
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_EQ(zmap.TrackedColumnCount(), 90u);
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 90u);
    ExpectAnswersMatchTheData(store, db, 0, kRows + 500, {0, 65, 85});
}

// A year whose index is already in rebuild state (a stopped builder; here a rebuild begun by hand) is skipped with one
// log line per year file, never retried in a loop; the DB's other years still widen.
TEST_F(XStoreLiveWidenTest, AYearAlreadyRebuildingIsSkippedAndTheOtherYearsWiden) {
    XStore store;
    const std::string db = "LWTRIGSKIP";
    const std::string path = DbPath(db);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (unsigned short y : {1979, 1980, 1981}) {
        Insert(store, db, {{Jan1(y), ValueRow(1, 10)}});
        for (uint64_t r = 1; r < 2000; ++r) rows.emplace_back(Jan1(y) + r, ValueRow(r, 70));
    }
    Insert(store, db, rows);
    ZoneMapFile &stuck = XStore::GetOrOpenZoneMap(path, 1980, kRowLength);
    ASSERT_TRUE(stuck.BeginRebuild(64));
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    kLogCapture->Clear();
    const auto pass = store.RunLiveWidenPass(g1 + seconds(30));
    EXPECT_EQ(YearsAndResults(pass),
              (PassResult{{1981, Result::kDone}, {1980, Result::kAlreadyRebuilding}, {1979, Result::kDone}}));
    ASSERT_EQ(pass.size(), 3u);
    EXPECT_TRUE(pass[1].skipped_rebuilding);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "a skipped year is not retried";
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1981, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1979, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_TRUE(stuck.Rebuilding());
    EXPECT_EQ(stuck.FrontierForTests(), 0u) << "the stuck year was touched";
    EXPECT_EQ(XStore::GetOrOpenCatIdx(path, 1980, kRowLength).TrackedColumnCount(), 64u) << "its catidx was touched";

    Insert(store, db, {{Jan1(1981) + 3000, ValueRow(2, 75)}}); // another growth: the stuck year is met again
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))),
              (PassResult{{1981, Result::kDone}, {1980, Result::kAlreadyRebuilding}, {1979, Result::kDone}}));
    EXPECT_EQ(kLogCapture->Count("is already being rebuilt"), 1u) << "one line per year file, not one per pass";
    ExpectAnswersMatchTheData(store, db, 0, 1999, {0, 65}, 1980);
}

// BeginRebuild fails (the zone map's dirty-mark fdatasync, every time): the old 64-wide zone map stays in use and
// trusted, and the year is retried after 60 s and 120 s more (fake time), then given up until the next growth.
TEST_F(XStoreLiveWidenTest, AFailedBeginIsRetriedWithBackoffThenGivenUp) {
    XStore store;
    const std::string db = "LWTRIGRETRY";
    constexpr uint64_t kRows = 10000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    CleanRestartState(); // clean on disk: a begin must make the dirty mark durable first
    kLogCapture->Clear();
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kSync, YearPath(db, ".zmap"), 0, 1000000, EIO);
        Clock::time_point t = g + seconds(30);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kPartial}})) << "the catidx widens";
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
        ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
        EXPECT_FALSE(zmap.Rebuilding());
        EXPECT_EQ(zmap.TrackedColumnCount(), 64u);
        auto block = zmap.TryGetBlockStats(1);
        ASSERT_TRUE(block.has_value()) << "the old index must stay trusted";
        EXPECT_EQ(block->size(), 64u);
        EXPECT_EQ(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).TrackedColumnCount(), 70u);
        ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(59)).empty()) << "first retry waits 60 s";
        t += seconds(60);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kFailed}})) << "only the zone map left";
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(119)).empty()) << "second retry waits 120 s";
        t += seconds(120);
        EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(t)), (PassResult{{kYear, Result::kFailed}}));
        EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "given up after 3 attempts";
        EXPECT_TRUE(store.RunLiveWidenPass(t + seconds(100000)).empty());
        EXPECT_GT(fail.Failures(), 2);
    }
    EXPECT_EQ(kLogCapture->Count("giving up"), 1u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).TrackedColumnCount(), 64u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// The cancel flag (shutdown) mid-pass: kStopped, the pass ends, the year's files stay in rebuild (dirty, untrusted past
// F), answers stay right, Shutdown leaves them dirty with the clear message, and a restart rebuilds them correctly.
TEST_F(XStoreLiveWidenTest, ACancelMidPassStopsItAndTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWTRIGSTOP";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended) << "the pass ignored the cancel flag";
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "a stopped pass keeps the record";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(XStore::GetOrOpenCatIdx(path, kYear, kRowLength).Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    kLogCapture->Clear();
    XStore::Shutdown();
    EXPECT_EQ(kLogCapture->Count("an index was still being rebuilt; left dirty, rebuilt at the next start"), 2u);
    EXPECT_FALSE(kLogCapture->Contains("MarkClean failed"));
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, "LWTRIGSTOPREF", 70); // its InitDB also rebuilds LWTRIGSTOP's dirty files
    ExpectSameFilesAs(db, "LWTRIGSTOPREF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g + seconds(31))), (PassResult{{kYear, Result::kNothingToDo}}));
    EXPECT_FALSE(XStore::LiveWidenLastGrowthForTests(path).has_value()); // (the reference DB's own growth is recorded)
}

// RemoveDB while a pass widens one of the DB's years: it cancels the widen, waits for the thread to let go, then
// removes. No crash, no leaked rebuilding entry, the record is dropped, and a DB re-created under the name sees only
// its own values. A watchdog bounds every wait (a RemoveDB that neither cancels nor waits must fail, not hang).
TEST_F(XStoreLiveWidenTest, RemoveDbDuringAWidenCancelsWaitsThenRemoves) {
    XStore store;
    const std::string db = "LWTRIGRMDB";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
        WaitForFrontierPast(*zmap, Rpb());
    }
    EXPECT_EQ(XStore::BackgroundActiveKeyForTests(), path + "/" + std::to_string(kYear));
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return r.RemoveDB(db).removestatus();
    });
    const bool removed_in_time = remove.wait_for(seconds(30)) == std::future_status::ready;
    const bool pass_ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(removed_in_time) << "RemoveDB hung";
    ASSERT_TRUE(pass_ended) << "the pass hung";
    EXPECT_NE(remove.get().find("[INFO]"), std::string::npos);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}})) << "RemoveDB must cancel the widen";
    EXPECT_FALSE(XStore::bg_cancel_.load()) << "the cancel was not reset";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u) << "the removed DB's record must go";
    EXPECT_EQ(XStore::BackgroundActiveKeyForTests(), "");
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value());
    EXPECT_FALSE(std::filesystem::exists(path));

    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    XStore::GetOrOpenZoneMap(path, 1999, kRowLength); // a cold open sweeps unpinned retired entries
    XStore::GetOrOpenCatIdx(path, 1999, kRowLength);
    EXPECT_EQ(XStore::RetiredIndexCountForTests(), 0u) << "a rebuilding entry leaked";
    Insert(store, db, {{Epoch(5), ValueRow(5000, 2)}, {Epoch(Rpb() + 7), ValueRow(5001, 2)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5001.0);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &sum_rep);
    EXPECT_EQ(sum_rep.opresult(), 10001.0);
    const XStore_Proto::FData old_value = FDouble(70.0); // row 10's column 0 in the removed DB
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(kRows - 1), db, 0, old_value, XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 0);
    EXPECT_FALSE(XStore::PinCatIdx(path, kYear, kRowLength)->MightContain(0, 0, EncodeFDataForCatIdx(old_value)));
    EXPECT_EQ(XStore::PinZoneMap(path, kYear, kRowLength)->TrackedColumnCount(), 64u);
    EXPECT_FALSE(XStore::PinZoneMap(path, kYear, kRowLength)->Rebuilding());
}

// RemoveDBFiles of the year a pass is widening: cancelled and waited for the same way; the year's files (half-built
// indexes included) are gone, its entries retired; a re-inserted year answers from its new rows only.
TEST_F(XStoreLiveWidenTest, RemoveDbFilesOfTheYearBeingWidenedCancelsWaitsThenRemoves) {
    XStore store;
    const std::string db = "LWTRIGRMYEAR";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    {
        const ZoneMapFile::Pin zmap = XStore::PinZoneMap(path, kYear, kRowLength);
        WaitForFrontierPast(*zmap, Rpb());
    }
    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        XStore_Proto::RemoveDBFiles_REQ req;
        req.add_filename(std::to_string(kYear) + ".XSTORE");
        return r.RemoveDBFiles(&req, db);
    });
    const bool removed_in_time = remove.wait_for(seconds(30)) == std::future_status::ready;
    const bool pass_ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(removed_in_time) << "RemoveDBFiles hung";
    ASSERT_TRUE(pass_ended) << "the pass hung";
    const XStore_Proto::RemoveDBFiles_REP rep = remove.get();
    ASSERT_EQ(rep.removestatus_size(), 3);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(rep.removestatus(i).rfind("[INFO]", 0), 0u) << rep.removestatus(i);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    EXPECT_FALSE(XStore::bg_cancel_.load());
    for (const char *ext : {".XSTORE", ".zmap", ".catidx"}) EXPECT_FALSE(std::filesystem::exists(YearPath(db, ext))) << ext;
    EXPECT_FALSE(XStore::PeekOpenZoneMapWidth(path, kYear).has_value());
    EXPECT_TRUE(store.RunLiveWidenPass(g + seconds(31)).empty()) << "no year file left to widen";
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);

    Insert(store, db, {{Epoch(5), ValueRow(5000, 70)}, {Epoch(Rpb() + 7), ValueRow(5001, 70)}});
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(Epoch(0), Epoch(kRows - 1), db, 0, &min_rep);
    EXPECT_EQ(min_rep.opresult().doubledata(), 5000.0);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(Epoch(0), Epoch(kRows - 1), db, 65, &max_rep);
    EXPECT_EQ(max_rep.opresult().doubledata(), 5066.0);
    EXPECT_EQ(XStore::PinZoneMap(path, kYear, kRowLength)->TrackedColumnCount(), 70u) << "a new year file at the DB's width";
}

// The TODO "compaction vs RemoveDB" race: a RemoveDB + CreateDB + insert during a compaction scan of a cold year. The
// scan is long (the year file is cut to 2M rows, about seconds of scanning); the test waits until the background
// handshake shows the compaction working on the year, then removes and re-creates. The old year's bitmap section must
// never land on the new DB's catidx, and FindByValue must find the new row.
TEST_F(XStoreLiveWidenTest, RemoveDbDuringACompactionScanNeverPutsTheOldBitmapOnTheNewCatIdx) {
    XStore store;
    const std::string db = "LWCOMPRM";
    const std::string path = DbPath(db);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r < 3000; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r % 7, 2)); // few distinct values
    Insert(store, db, rows);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(2000000) * kRowLength), 0);
    auto compaction = std::async(std::launch::async, [&] {
        XStore c;
        c.CompactColdCatIdxForAllDbs(c.data_store_dir);
    });
    const std::string year_key = path + "/" + std::to_string(kYear);
    const Clock::time_point t0 = Clock::now();
    while (XStore::BackgroundActiveKeyForTests() != year_key && MsSince(t0) < 20000) std::this_thread::yield();
    ASSERT_EQ(XStore::BackgroundActiveKeyForTests(), year_key) << "the compaction never started on the year";

    ASSERT_NE(store.RemoveDB(db).removestatus().find("[INFO]"), std::string::npos);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(5), ValueRow(777, 2)}});
    // Bounds a compaction of the NEW year, should the scan of the data directory reach the re-created DB.
    static_cast<void>(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(4000) * kRowLength));
    ASSERT_EQ(compaction.wait_for(seconds(120)), std::future_status::ready);
    compaction.get();

    CatIdxFile::Pin catidx = XStore::PinCatIdx(path, kYear, kRowLength);
    const auto old_rows = catidx->LookupExact(0, EncodeFDataForCatIdx(FDouble(1000.0)));
    EXPECT_FALSE(old_rows.has_value() && !old_rows->empty()) << "the removed DB's bitmap section landed on the new catidx";
    std::string buf;
    store.FindByValue(Epoch(0), Epoch(2999), db, 0, FDouble(777.0), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP found;
    ASSERT_TRUE(found.ParseFromString(buf));
    EXPECT_EQ(found.queryresponse_size(), 1) << "the new row was not found";
}

// Widening and compaction on the same thread, one after the other: compaction skips a year left rebuilding by a
// stopped widen (no bitmap section written), and after a completed widen it builds the section at the new width.
TEST_F(XStoreLiveWidenTest, CompactionAfterAPassSkipsARebuildingYearAndCompactsAWidenedOne) {
    XStore store;
    const std::string db = "LWTRIGCOMP";
    const std::string path = DbPath(db);
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    // Bound the compaction scans (the year file is preallocated to the whole year).
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(kRows + 1000) * kRowLength), 0);

    // A stopped widen leaves the year rebuilding: compaction must not write a section into it.
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kRows * kRowLength, kRowLength, kWrite);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30));
    });
    WaitForFrontierPast(zmap, Rpb());
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kRows * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended);
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    XStore::ClearLiveWidenRequestsForTests(); // else compaction yields to the kept record and never reaches the guard
    kLogCapture->Clear();
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).bitmap_section_offset, 0u)
        << "compaction wrote into a rebuilding catidx";

    // Next start rebuilds it; then a fresh growth, a pass and a compaction, in that order.
    CleanRestartState();
    store.InitDB();
    Insert(store, db, {{Epoch(kRows + 10), ValueRow(4, 72)}}); // 70 -> 72
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    EXPECT_EQ(YearsAndResults(store.RunLiveWidenPass(g2 + seconds(30))), (PassResult{{kYear, Result::kDone}}));
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    CatIdxFile::Pin catidx = XStore::PinCatIdx(path, kYear, kRowLength, 72);
    EXPECT_EQ(catidx->TrackedColumnCount(), 72u);
    EXPECT_TRUE(catidx->HasBitmapSection()) << "the widened year was not compacted";
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// The fast path stays as it was: inserts at an unchanged width (or growth that stays under the 64-column floor) record
// nothing.
TEST_F(XStoreLiveWidenTest, InsertsAtAnUnchangedWidthRecordNothing) {
    XStore store;
    const std::string db = "LWTRIGFAST";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows narrow;
    for (uint64_t r = 0; r < 500; ++r) narrow.emplace_back(Epoch(r), ValueRow(r, 1 + static_cast<int>(r % 60)));
    Insert(store, db, narrow); // the column count grows 1 -> 60: still under the floor
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    Insert(store, db, {{Epoch(600), ValueRow(1, 70)}});
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 1u);
    XStore::ClearLiveWidenRequestsForTests();
    Rows same;
    for (uint64_t r = 700; r < 1700; ++r) same.emplace_back(Epoch(r), ValueRow(r, r % 2 ? 70 : 30));
    Insert(store, db, same);
    for (uint64_t r = 2000; r < 2050; ++r) {
        XStore_Proto::UnaryInsert_REP rep;
        store.UnaryInsert(Epoch(r), ValueRow(r, 70), db, &rep);
    }
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
}

// ---- 10b. Compaction yields to a pending live widen (S3a) -------------------------------------------------------------

// The capture sink keeps warn+ only; the yield/resume lines are INFO. A sink's level is atomic, so this is safe while
// other threads log.
struct ScopedInfoCapture {
    ScopedInfoCapture() { kLogCapture->set_level(spdlog::level::info); }
    ~ScopedInfoCapture() { kLogCapture->set_level(spdlog::level::warn); }
    ScopedInfoCapture(const ScopedInfoCapture &) = delete;
    ScopedInfoCapture &operator=(const ScopedInfoCapture &) = delete;
};
constexpr const char *kYieldLine = "stopped: a live widen is pending; resumes after it";
constexpr const char *kDeferredLine = "Compaction deferred: a live widen is pending; resumes after it";
constexpr const char *kResumeLine = "No live widen pending any more; compaction resumes in 30 s";

// A widen recorded while the background thread's compaction scans a long cold year: the pass abandons the year at its
// next 64K-row check (its catidx byte-for-byte as before, no section), a year compacted earlier keeps its section, and
// the schedule is not pushed an hour out. The next ticks (fake clock) widen every year after the quiet period, then
// compact again LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC later; the result equals a startup rebuild plus a compaction.
TEST_F(XStoreLiveWidenTest, CompactionYieldsToAWidenRecordedMidScanAndRunsSoonAfterIt) {
    static_assert(XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC == 30, "kResumeLine says 30 s");
    XStore store;
    const std::string db = "LWYIELD";
    const std::string path = DbPath(db);
    constexpr uint64_t kRows = 3000;
    const auto seed = [&](unsigned short y) {
        Insert(store, db, {{Jan1(y), ValueRow(1, 10)}}); // index files born 64 wide
        Rows rows;
        for (uint64_t r = 1; r < kRows; ++r) rows.emplace_back(Jan1(y) + r, ValueRow((r * 7 + y) % 100000, r % 5 == 0 ? 40 : 70));
        Insert(store, db, rows);
    };
    // Year files are preallocated to the whole year: cut them to bound every scan.
    const auto cut = [&](const std::string &d, unsigned short y, uint64_t rows) {
        ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
        ASSERT_EQ(truncate(YearPath(d, ".XSTORE", y).c_str(), static_cast<off_t>(rows) * kRowLength), 0);
    };
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    seed(1979);
    cut(db, 1979, kRows + 1000);
    XStore::ClearLiveWidenRequestsForTests(); // drop the 64 -> 70 growth: the index files stay 64 wide for now
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    ASSERT_TRUE(XStore::PinCatIdx(path, 1979, kRowLength)->HasBitmapSection());
    seed(1980); // no growth: the DB is 70 wide already
    ASSERT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    cut(db, 1980, 8000000); // a sparse year whose scan takes seconds
    const std::string catidx_1979 = ReadFileBytes(YearPath(db, ".catidx", 1979));
    const std::string catidx_1980 = ReadFileBytes(YearPath(db, ".catidx", 1980));

    ScopedInfoCapture info;
    kLogCapture->Clear();
    XStore::BackgroundIndexSchedule schedule;
    Clock::time_point fake = Clock::now();
    const auto now = [&] { return fake; };
    auto tick = std::async(std::launch::async, [&] {
        XStore b;
        b.BackgroundIndexTick(b.data_store_dir, schedule, now);
    });
    const std::string year_key = path + "/1980";
    const Clock::time_point t0 = Clock::now();
    while (XStore::BackgroundActiveKeyForTests() != year_key && MsSince(t0) < 20000) std::this_thread::yield();
    ASSERT_EQ(XStore::BackgroundActiveKeyForTests(), year_key) << "the compaction never started on 1980";
    Insert(store, db, {{Jan1(1981) + 5, ValueRow(3, 80)}}); // growth 70 -> 80 recorded while 1980 is scanned
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ASSERT_EQ(tick.wait_for(seconds(120)), std::future_status::ready);
    tick.get();

    EXPECT_EQ(kLogCapture->Count(std::string("Compaction of ") + year_key + " " + kYieldLine), 1u);
    EXPECT_GE(kLogCapture->Count(kYieldLine), 1u);
    EXPECT_LE(kLogCapture->Count(kYieldLine), 2u) << "one line per yielding year (the abandoned one, and the next cold year the pass stops at), not one per 64K rows; which year is listed first depends on the directory order";
    EXPECT_TRUE(schedule.compaction_yielded);
    EXPECT_EQ(schedule.next_compaction, Clock::time_point{}) << "a yielded pass must not push compaction an hour out";
    EXPECT_EQ(XStore::BackgroundActiveKeyForTests(), "");
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx", 1980)).bitmap_section_offset, 0u);
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".catidx", 1980)) == catidx_1980) << "the abandoned year's catidx changed";
    EXPECT_TRUE(ReadFileBytes(YearPath(db, ".catidx", 1979)) == catidx_1979) << "the completed year's catidx changed";
    EXPECT_TRUE(XStore::PinCatIdx(path, 1979, kRowLength)->HasBitmapSection());
    for (unsigned short y : {1979, 1980}) ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65}, y);

    cut(db, 1980, kRows + 1000);
    cut(db, 1981, 1000);
    fake = g + seconds(29);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "inside the quiet period";
    EXPECT_EQ(kLogCapture->Count(kDeferredLine), 0u) << "already logged as yielded";
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx", 1980)).bitmap_section_offset, 0u)
        << "compaction ran while a widen was recorded";

    fake = g + seconds(30);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now); // widens, then reschedules compaction
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_FALSE(schedule.compaction_yielded);
    EXPECT_EQ(schedule.next_compaction, g + seconds(30 + XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC));
    EXPECT_EQ(kLogCapture->Count(kResumeLine), 1u);
    for (unsigned short y : {1979, 1980, 1981}) {
        SCOPED_TRACE("year " + std::to_string(y));
        EXPECT_EQ(XStore::GetOrOpenZoneMap(path, y, kRowLength).TrackedColumnCount(), 80u);
        EXPECT_EQ(XStore::GetOrOpenCatIdx(path, y, kRowLength).TrackedColumnCount(), 80u);
        EXPECT_FALSE(XStore::PinCatIdx(path, y, kRowLength)->HasBitmapSection()) << "not compacted yet";
    }
    for (unsigned short y : {1979, 1980}) ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65}, y);

    fake = schedule.next_compaction - seconds(1);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_FALSE(XStore::PinCatIdx(path, 1980, kRowLength)->HasBitmapSection()) << "compacted before its time";
    // A query's cold mapping regrows a year file to the whole year: cut again before compaction scans them.
    cut(db, 1979, kRows + 1000);
    cut(db, 1980, kRows + 1000);
    fake = schedule.next_compaction;
    const Clock::time_point compacted_at = fake;
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_EQ(schedule.next_compaction, compacted_at + seconds(XStoreTunables::CATIDX_COMPACTION_POLL_INTERVAL_SEC));
    EXPECT_GE(kLogCapture->Count(kYieldLine), 1u);
    EXPECT_LE(kLogCapture->Count(kYieldLine), 2u);
    for (unsigned short y : {1979, 1980, 1981}) {
        EXPECT_TRUE(XStore::PinCatIdx(path, y, kRowLength)->HasBitmapSection()) << "year " << y << " not compacted";
    }
    for (unsigned short y : {1979, 1980}) ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65}, y);

    // Equal to a startup rebuild plus a compaction (1981 was born at 80 at runtime, never widened: not compared).
    std::map<std::pair<unsigned short, std::string>, std::string> live;
    for (unsigned short y : {1979, 1980}) {
        for (const char *ext : {".zmap", ".catidx"}) live[{y, ext}] = ReadFileBytes(YearPath(db, ext, y));
    }
    const std::string ref = "LWYIELDREF";
    CreateTestDb(store, ref, XStore_Proto::SECOND, kRowLength);
    Insert(store, ref, {{Jan1(1975), ValueRow(1, 80)}});
    for (unsigned short y : {1979, 1980}) SparseCopy(YearPath(db, ".XSTORE", y), YearPath(ref, ".XSTORE", y));
    cut(ref, 1975, 1000);
    for (unsigned short y : {1979, 1980}) cut(ref, y, kRows + 1000); // the size the live years were compacted at
    store.InitDB();
    for (unsigned short y : {1979, 1980}) cut(ref, y, kRows + 1000);
    XStore::ClearLiveWidenRequestsForTests(); // the reference DB's own growth
    store.CompactColdCatIdxForAllDbs(store.data_store_dir);
    for (unsigned short y : {1979, 1980}) {
        ASSERT_TRUE(XStore::PinCatIdx(DbPath(ref), y, kRowLength)->HasBitmapSection());
        for (const char *ext : {".zmap", ".catidx"}) {
            EXPECT_TRUE((live[{y, ext}] == ReadFileBytes(YearPath(ref, ext, y)))) << y << ext << " differs from rebuild + compaction";
        }
    }
}

// A due compaction is held back, not started, while a widen is recorded (one log line however many ticks), and runs
// LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC after the widen. A widen recorded while compaction is not due leaves its
// interval alone.
TEST_F(XStoreLiveWidenTest, AHeldBackCompactionRunsSoonAfterTheWidenAndAnyLaterWidenBringsCompactionForward) {
    XStore store;
    const std::string db = "LWYIELDTICK";
    const std::string path = DbPath(db);
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70); // records the 64 -> 70 growth
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(kRows + 1000) * kRowLength), 0);
    const Clock::time_point g1 = *XStore::LiveWidenLastGrowthForTests(path);
    ScopedInfoCapture info;
    kLogCapture->Clear();
    XStore::BackgroundIndexSchedule schedule;
    Clock::time_point fake = g1 + seconds(1);
    const auto now = [&] { return fake; };
    for (int i = 0; i < 5; ++i) {
        fake = g1 + seconds(1 + i);
        store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    }
    EXPECT_TRUE(schedule.compaction_yielded);
    EXPECT_EQ(kLogCapture->Count(kDeferredLine), 1u) << "one line, not one per tick";
    EXPECT_EQ(kLogCapture->Count(kYieldLine), 0u) << "compaction must not even start";
    EXPECT_FALSE(XStore::PinCatIdx(path, kYear, kRowLength)->HasBitmapSection());

    fake = g1 + seconds(30);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_EQ(XStore::PinCatIdx(path, kYear, kRowLength)->TrackedColumnCount(), 70u);
    EXPECT_FALSE(schedule.compaction_yielded);
    const Clock::time_point resume = g1 + seconds(30 + XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC);
    EXPECT_EQ(schedule.next_compaction, resume);
    EXPECT_EQ(kLogCapture->Count(kResumeLine), 1u);
    fake = resume - seconds(1);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_FALSE(XStore::PinCatIdx(path, kYear, kRowLength)->HasBitmapSection());
    fake = resume;
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_TRUE(XStore::PinCatIdx(path, kYear, kRowLength)->HasBitmapSection());
    const Clock::time_point next = resume + seconds(XStoreTunables::CATIDX_COMPACTION_POLL_INTERVAL_SEC);
    EXPECT_EQ(schedule.next_compaction, next);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});

    // Compaction not due when the next growth comes: nothing is logged as deferred, but the widen drops the widened
    // years' bitmap sections, so the next compaction is brought forward to shortly after it.
    Insert(store, db, {{Epoch(kRows + 10), ValueRow(4, 72)}}); // 70 -> 72
    const Clock::time_point g2 = *XStore::LiveWidenLastGrowthForTests(path);
    ASSERT_LT(g2 + seconds(30), next);
    fake = g2 + seconds(1);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_TRUE(schedule.compaction_yielded);
    fake = g2 + seconds(30);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_EQ(XStore::PinCatIdx(path, kYear, kRowLength)->TrackedColumnCount(), 72u);
    EXPECT_FALSE(schedule.compaction_yielded);
    EXPECT_EQ(schedule.next_compaction, g2 + seconds(30) + seconds(XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC))
        << "any widen brings the next compaction forward";
    EXPECT_EQ(kLogCapture->Count(kResumeLine), 2u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// RemoveDB of the DB whose widen holds compaction back: no hang, the record goes, and compaction resumes soon for the
// other DBs.
TEST_F(XStoreLiveWidenTest, RemoveDbWhileCompactionWaitsForAWidenLetsCompactionResume) {
    XStore store;
    const std::string db = "LWYIELDRM";
    const std::string keep = "LWYIELDKEEP";
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CreateTestDb(store, keep, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (uint64_t r = 0; r < 1000; ++r) rows.emplace_back(Epoch(r), ValueRow(1000 + r % 7, 2));
    Insert(store, keep, rows);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    for (const std::string &d : {db, keep}) {
        ASSERT_EQ(truncate(YearPath(d, ".XSTORE").c_str(), static_cast<off_t>(kRows + 1000) * kRowLength), 0);
    }
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(DbPath(db));
    XStore::BackgroundIndexSchedule schedule;
    Clock::time_point fake = g + seconds(1);
    const auto now = [&] { return fake; };
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    ASSERT_TRUE(schedule.compaction_yielded);

    auto remove = std::async(std::launch::async, [&] {
        XStore r;
        return r.RemoveDB(db).removestatus();
    });
    ASSERT_EQ(remove.wait_for(seconds(30)), std::future_status::ready) << "RemoveDB hung";
    EXPECT_NE(remove.get().find("[INFO]"), std::string::npos);
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 0u);
    EXPECT_FALSE(std::filesystem::exists(DbPath(db)));

    fake = g + seconds(2);
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_FALSE(schedule.compaction_yielded);
    EXPECT_EQ(schedule.next_compaction, fake + seconds(XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC));
    fake = schedule.next_compaction;
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    EXPECT_TRUE(XStore::PinCatIdx(DbPath(keep), kYear, kRowLength)->HasBitmapSection());
    EXPECT_FALSE(std::filesystem::exists(DbPath(db))) << "compaction re-created the removed DB";
}

// Shutdown (StopCatIdxCompactionThread's flags; no thread runs in tests) while compaction waits for a widen: the tick
// returns at once, widens nothing (the record is kept for the next start) and compacts nothing.
TEST_F(XStoreLiveWidenTest, ShutdownWhileCompactionWaitsForAWidenEndsTheTickAtOnce) {
    XStore store;
    const std::string db = "LWYIELDSTOP";
    const std::string path = DbPath(db);
    constexpr uint64_t kRows = 3000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    ASSERT_TRUE(walring::Checkpointer::Instance().RunCycle());
    ASSERT_EQ(truncate(YearPath(db, ".XSTORE").c_str(), static_cast<off_t>(kRows + 1000) * kRowLength), 0);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    XStore::BackgroundIndexSchedule schedule;
    Clock::time_point fake = g + seconds(1);
    const auto now = [&] { return fake; };
    store.BackgroundIndexTick(store.data_store_dir, schedule, now);
    ASSERT_TRUE(schedule.compaction_yielded);

    struct ResetFlags {
        ~ResetFlags() {
            XStore::catidx_compaction_stop_flag_.store(false);
            XStore::bg_cancel_.store(false);
        }
    } reset;
    XStore::StopCatIdxCompactionThread();
    fake = g + seconds(3600);
    auto tick = std::async(std::launch::async, [&] {
        XStore b;
        b.BackgroundIndexTick(b.data_store_dir, schedule, now);
    });
    ASSERT_EQ(tick.wait_for(seconds(30)), std::future_status::ready) << "the tick hung at shutdown";
    tick.get();
    EXPECT_EQ(XStore::LiveWidenRequestCountForTests(), 1u) << "a cancelled pass keeps the record";
    EXPECT_EQ(XStore::PinZoneMap(path, kYear, kRowLength)->TrackedColumnCount(), 64u) << "widened during shutdown";
    EXPECT_FALSE(XStore::PinCatIdx(path, kYear, kRowLength)->HasBitmapSection()) << "compacted during shutdown";
    EXPECT_TRUE(schedule.compaction_yielded);
}

// ---- 11. Parallel live widen (S2): N workers inside one year ---------------------------------------------------------

// Waits (polling, 20 s bound) until pred() holds.
template <class Pred>
void WaitUntil(Pred pred, const char *what) {
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(20);
    while (!pred()) {
        ASSERT_LT(Clock::now(), deadline) << "timed out waiting: " << what;
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
}

// Releases a WRITE lock the test holds on data-file bytes, on every exit path.
struct HeldWrite {
    FileRangeLock &lock;
    uint64_t offset, length;
    bool held = true;
    HeldWrite(FileRangeLock &l, uint64_t off, uint64_t len) : lock(l), offset(off), length(len) { lock.LockRange(offset, length, kWrite); }
    void Release() {
        if (held) lock.UnlockRange(offset, length, kWrite);
        held = false;
    }
    ~HeldWrite() { Release(); }
};

// MIN/MAX/SUM/AVG/EXISTS and FindByValue (of the largest value) of column c on rows [lo, hi], against a scan of the data
// file; an empty range must answer "no data" (EXISTS false).
void ExpectExactAnswers(XStore &store, const std::string &db, const std::string &data_path, uint64_t lo, uint64_t hi, int c,
                        unsigned long long (*at)(uint64_t)) {
    SCOPED_TRACE("rows [" + std::to_string(lo) + ", " + std::to_string(hi) + "] column " + std::to_string(c));
    const int fd = open(data_path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<std::pair<uint64_t, double>> values;
    for (const auto &[r, row] : ReadRows(fd, lo, hi, 2048)) {
        double v = 0;
        if (c < row.fielddata_size() && NumericOf(row.fielddata(c), v)) values.emplace_back(r, v);
    }
    close(fd);
    double mn = INFINITY, mx = -INFINITY, sum = 0;
    for (const auto &[r, v] : values) {
        mn = std::min(mn, v);
        mx = std::max(mx, v);
        sum += v;
    }
    const bool any = !values.empty();
    const unsigned short col = static_cast<unsigned short>(c);
    const auto success = [](const std::string &status) { return status.rfind("SUCCESS", 0) == 0; };
    XStore_Proto::Min_REP min_rep;
    store.MinAggregate(at(lo), at(hi), db, col, &min_rep);
    EXPECT_EQ(success(min_rep.opstatus()), any) << min_rep.opstatus();
    if (any) EXPECT_EQ(min_rep.opresult().doubledata(), mn);
    XStore_Proto::Max_REP max_rep;
    store.MaxAggregate(at(lo), at(hi), db, col, &max_rep);
    EXPECT_EQ(success(max_rep.opstatus()), any) << max_rep.opstatus();
    if (any) EXPECT_EQ(max_rep.opresult().doubledata(), mx);
    XStore_Proto::Sum_REP sum_rep;
    store.SumAggregate(at(lo), at(hi), db, col, &sum_rep);
    EXPECT_EQ(success(sum_rep.opstatus()), any) << sum_rep.opstatus();
    if (any) EXPECT_EQ(sum_rep.opresult(), sum);
    XStore_Proto::Avg_REP avg_rep;
    store.AvgAggregate(at(lo), at(hi), db, col, &avg_rep);
    EXPECT_EQ(success(avg_rep.opstatus()), any) << avg_rep.opstatus();
    if (any) EXPECT_NEAR(avg_rep.opresult(), sum / static_cast<double>(values.size()), 1e-9 * std::max(1.0, std::fabs(sum)));
    XStore_Proto::Exists_REP exists_rep;
    store.ExistsAggregate(at(lo), at(hi), db, col, &exists_rep);
    EXPECT_TRUE(success(exists_rep.opstatus())) << exists_rep.opstatus();
    EXPECT_EQ(exists_rep.exists(), any);
    if (!any) return;
    std::set<unsigned long long> want;
    for (const auto &[r, v] : values) {
        if (v == mx) want.insert(at(r));
    }
    std::string buf;
    store.FindByValue(at(lo), at(hi), db, c, FDouble(mx), XStoreTunables::STREAMED_QUERY_CHUNK_SIZE_BYTES,
                      [&](std::string &&chunk, bool) { buf += chunk; });
    XStore_Proto::RangeQuery_REP rep;
    std::set<unsigned long long> got;
    if (rep.ParseFromString(buf)) {
        for (const auto &e : rep.queryresponse()) got.insert(e.timestamp());
    }
    EXPECT_EQ(got, want) << "FindByValue " << mx;
}

unsigned long long EpochOfRow(uint64_t row) { return Jan1(1980) + row; }

// Byte-identical: the widened .zmap/.catidx equal the startup rebuild of a copy of the same year for 1, 2, 4 and 8
// workers, with one zone-map block per group (so dense data has many groups and the year's empty tail thousands).
class XStoreParallelLiveWidenTest : public XStoreLiveWidenTest {
protected:
    void ExpectEveryWorkerCountMatchesTheStartupRebuild(const std::string &db, int width, const Rows &rows) {
        XStore store;
        CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
        Insert(store, db, {{Epoch(0), MixedRow(0, 10)}}); // index files born 64 wide
        Insert(store, db, rows);
        BuildStartupReference(store, db, db + "REF", width);
        const std::string path = DbPath(db);
        uint64_t rows_folded = 0;
        for (unsigned workers : {1u, 2u, 4u, 8u}) {
            SCOPED_TRACE(std::to_string(workers) + " workers");
            XStore::LiveWidenStats stats;
            ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, static_cast<uint32_t>(width), true, true, nullptr,
                                             &stats, workers, Rpb() * kRowLength), Result::kDone);
            EXPECT_TRUE(stats.zmap_widened && stats.catidx_widened);
            if (workers == 1) rows_folded = stats.rows_folded;
            EXPECT_EQ(stats.rows_folded, rows_folded) << "every present row folded exactly once";
            EXPECT_EQ(stats.lock_hold_us.size(), stats.chunks);
            ExpectSameFilesAs(db, db + "REF");
            EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 0u);
        }
        ExpectAnswersMatchTheData(store, db, 0, 2 * Rpb() - 1, {0, 2, 64, width - 4});
    }
};

TEST_F(XStoreParallelLiveWidenTest, DenseRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 1; i < 20 * Rpb() + 300; ++i) rows.emplace_back(Epoch(i), MixedRow(i, i % 10 == 0 ? 70 : 40 + static_cast<int>(i % 9)));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPDENSE", 70, rows);
}

TEST_F(XStoreParallelLiveWidenTest, SparseRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 37; i < 30 * Rpb(); i += 37) rows.emplace_back(Epoch(i), MixedRow(i, 70));
    for (uint64_t r : {200000ULL, 200001ULL, 1000000ULL, 3000000ULL, 30000000ULL}) rows.emplace_back(Epoch(r), MixedRow(r, 70));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPSPARSE", 70, rows);
}

TEST_F(XStoreParallelLiveWidenTest, WideRowsAreByteIdenticalForEveryWorkerCount) {
    Rows rows;
    for (uint64_t i = 1; i < 12 * Rpb(); ++i) rows.emplace_back(Epoch(i), MixedRow(i, 100));
    ExpectEveryWorkerCountMatchesTheStartupRebuild("LWPWIDE", 100, rows);
}

// THE lost update. The worker that claims group 0 stalls on row 0 (the test holds a WRITE lock on it), so the frontier
// stays at 0 while the other workers fold groups 1..15. Then writes land in those folded groups ABOVE the frontier:
// fresh rows (Unary, Batch), a Range insert and a Unary overwrite, all three deletes (one resets a whole block). Each
// must update the index (or flag/reset its block): the fold already read the old rows. With the old check (skip every
// row >= F) the index keeps the old rows and answers wrong. Then row 0 is released and the widen finishes.
TEST_F(XStoreParallelLiveWidenTest, WritesIntoAFoldedGroupAboveTheFrontierAreNotLost) {
    XStore store;
    const std::string db = "LWPFOLDED";
    constexpr int kWidth = 70;
    const uint64_t rpb = Rpb();
    constexpr uint64_t kBlocks = 16;
    const uint64_t rows = kBlocks * rpb;
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), ValueRow(0, 10)}});
    {
        Rows seed; // even rows: the odd ones are holes for fresh inserts
        for (uint64_t r = 2; r < rows; r += 2) seed.emplace_back(Epoch(r), ValueRow(r * 7 % 100000, kWidth));
        Insert(store, db, seed);
    }
    const std::string path = DbPath(db);
    const std::string data_path = YearPath(db, ".XSTORE");
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    XStore::LiveWidenStats stats;
    std::future<Result> builder; // destroyed (waited for) after the held lock below is released
    HeldWrite row0(FileLockRegistry::Get(data_path), 0, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, kWidth, true, true, nullptr, &stats, 4, rpb * kRowLength);
    });
    WaitUntil([&] {
        if (!zmap.Rebuilding()) return false;
        for (uint64_t b = 1; b < kBlocks; ++b) {
            if (!zmap.WriterUpdatesRowForTests((b + 1) * rpb - 1)) return false;
        }
        return true;
    }, "groups 1..15 folded");
    ASSERT_EQ(zmap.FrontierForTests(), 0u) << "the frontier passed the stalled group 0";
    ASSERT_EQ(catidx.FrontierForTests(), 0u);
    ASSERT_FALSE(zmap.WriterUpdatesRowForTests(5)) << "group 0 is not folded: its writers must leave it to the builder";

    const auto ok = [](const std::string &status) { return status.rfind("[INFO]", 0) == 0; };
    {
        XStore_Proto::UnaryInsert_REP rep; // unfolded group 0: left to the builder
        store.UnaryInsert(Epoch(5), ValueRow(400000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::RangeInsert_REQ req; // crosses from the unfolded group 0 into the folded group 1 (not row 0: it is held)
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t r = rpb - rpb / 4; r < rpb + rpb / 4; ++r) {
            XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = ValueRow(1000000 + r, kWidth);
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    ASSERT_NE(store.RangeDelete(Epoch(rpb / 2), Epoch(rpb + rpb / 2 - 1), db).deletestatus().find("[INFO]"),
              std::string::npos) << "a range delete crossing from the unfolded group 0 into the folded group 1";
    {
        XStore_Proto::UnaryInsert_REP rep; // fresh row, block 3: the new maximum of its block
        store.UnaryInsert(Epoch(3 * rpb + 1), ValueRow(500000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::BatchInsert_REQ req; // fresh rows, blocks 4 and 5
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (const auto &[r, v] : std::vector<std::pair<uint64_t, uint64_t>>{{4 * rpb + 3, 600000}, {5 * rpb + 5, 700000}}) {
            XStore_Proto::BatchInsert_DATA *e = req.add_batchinsert_data();
            e->set_timestamp(Epoch(r));
            *e->mutable_data() = ValueRow(v, kWidth);
        }
        XStore_Proto::BatchInsert_REP rep;
        store.BatchInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::RangeInsert_REQ req; // 20 consecutive rows of block 6: fresh odd rows and overwritten even rows
        req.set_dbname(db);
        req.set_durabilitymode(XStore_Proto::SYNC);
        for (uint64_t i = 0; i < 20; ++i) {
            XStore_Proto::RangeInsert_DATA *e = req.add_rangeinsert_data();
            e->set_timestamp(Epoch(6 * rpb + 10 + i));
            *e->mutable_data() = ValueRow(800000 + i, kWidth);
        }
        XStore_Proto::RangeInsert_REP rep;
        store.RangeInsert(&req, &rep);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    {
        XStore_Proto::UnaryInsert_REP rep; // overwrite, block 7
        store.UnaryInsert(Epoch(7 * rpb + 2), ValueRow(900000, kWidth), db, &rep, XStore_Proto::SYNC);
        ASSERT_TRUE(ok(rep.insertstatus())) << rep.insertstatus();
    }
    // Deletes of the largest values of blocks 8, 9 (and a cut range in 11), and all of block 10 (the reset path).
    const auto max_row_of_block = [&](uint64_t b) {
        uint64_t best = b * rpb + 2;
        for (uint64_t r = b * rpb + 2; r < (b + 1) * rpb; r += 2) {
            if (r * 7 % 100000 > best * 7 % 100000) best = r;
        }
        return best;
    };
    ASSERT_NE(store.UnaryDelete(Epoch(max_row_of_block(8)), db).deletestatus().find("[INFO]"), std::string::npos);
    {
        XStore_Proto::BatchDelete_REQ req;
        req.set_dbname(db);
        req.add_timestamp(Epoch(9 * rpb + 6));
        req.add_timestamp(Epoch(max_row_of_block(9)));
        ASSERT_NE(store.BatchDelete(&req).deletestatus().find("[INFO]"), std::string::npos);
    }
    ASSERT_NE(store.RangeDelete(Epoch(10 * rpb), Epoch(11 * rpb - 1), db).deletestatus().find("[INFO]"), std::string::npos);
    ASSERT_NE(store.RangeDelete(Epoch(11 * rpb + 100), Epoch(11 * rpb + 300), db).deletestatus().find("[INFO]"),
              std::string::npos);
    EXPECT_EQ(zmap.FrontierForTests(), 0u) << "the writes must have landed above the frontier";

    row0.Release();
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(30)), std::future_status::ready) << "the widen did not finish";
    ASSERT_EQ(builder.get(), Result::kDone);
    EXPECT_FALSE(zmap.Rebuilding());

    for (uint64_t b = 0; b < kBlocks; ++b) {
        for (int c : {0, 33, 65, 69}) ExpectExactAnswers(store, db, data_path, b * rpb, (b + 1) * rpb - 1, c, EpochOfRow);
    }
    ExpectExactAnswers(store, db, data_path, 0, rows - 1, 65, EpochOfRow);

    // The live index against a startup rebuild of the final data: blocks the writes kept exact (fresh rows, the reset)
    // are identical entry for entry; flagged ones only bound min/max.
    std::vector<std::vector<ZoneMapEntry>> live(kBlocks);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        auto st = zmap.TryGetBlockStats(b);
        ASSERT_TRUE(st.has_value()) << "block " << b;
        live[b] = *st;
    }
    {   // Bloom: every present value of every tracked column.
        const int fd = open(data_path.c_str(), O_RDONLY);
        ASSERT_GE(fd, 0);
        uint64_t misses = 0;
        for (const auto &[r, row] : ReadRows(fd, 0, rows - 1, kRowLength)) {
            for (int c = 0; c < std::min(kWidth, row.fielddata_size()); ++c) {
                if (row.fielddata(c).oneOfData_case() == XStore_Proto::FData::ONEOFDATA_NOT_SET) continue;
                if (!catidx.MightContain(BlockIndexForRow(r, rpb), static_cast<uint32_t>(c), EncodeFDataForCatIdx(row.fielddata(c)))) {
                    if (++misses < 5) ADD_FAILURE() << "bloom misses row " << r << " column " << c;
                }
            }
        }
        close(fd);
        EXPECT_EQ(misses, 0u);
    }
    BuildStartupReference(store, db, db + "REF", kWidth);
    ZoneMapFile &oracle = XStore::GetOrOpenZoneMap(DbPath(db + "REF"), kYear, kRowLength);
    for (uint64_t b = 0; b < kBlocks; ++b) {
        SCOPED_TRACE("block " + std::to_string(b));
        auto o = oracle.TryGetBlockStats(b);
        ASSERT_TRUE(o.has_value());
        ASSERT_EQ(o->size(), live[b].size());
        const bool flagged = ZoneMapEntryInexact(live[b][0]);
        const bool expect_flagged = (b >= 6 && b <= 11 && b != 10) || b == 1; // block 1: the crossing range overwrite and delete
        EXPECT_EQ(flagged, expect_flagged) << "overwrites and deletes flag their block; fresh rows and the reset do not";
        if (!flagged) {
            EXPECT_EQ(std::memcmp(live[b].data(), o->data(), o->size() * sizeof(ZoneMapEntry)), 0) << "differs from the startup rebuild";
            continue;
        }
        for (size_t c = 0; c < o->size(); ++c) {
            if ((*o)[c].valid_count == 0) continue;
            EXPECT_LE(live[b][c].min_value, (*o)[c].min_value) << "column " << c;
            EXPECT_GE(live[b][c].max_value, (*o)[c].max_value) << "column " << c;
        }
    }
}

// Section 2's oracle with four workers and one block per group: writers race many folded groups above the frontier.
TEST_F(XStoreParallelLiveWidenTest, KeepOldAnswersStayExactUnderConcurrentWritesWithFourWorkers) {
    RunConcurrentOracle("LWKEEPOLD4", 4, Rpb() * kRowLength, nullptr, /*keep_old=*/true);
}

TEST_F(XStoreParallelLiveWidenTest, AnswersStayExactUnderConcurrentWritesWithFourWorkers) {
    RunConcurrentOracle("LWPLIVE", 4, Rpb() * kRowLength);
}

// Stop with four workers: the files stay dirty at the contiguous watermark (a held row in the last data block keeps
// the build from finishing first), never trusted past it, and the next start rebuilds them. keep_old=false: in-place
// rebuild coverage (with keep-old, the default since 2026-10-17, a block past F is served from the old index).
TEST_F(XStoreParallelLiveWidenTest, StopMidBuildLeavesTheContiguousWatermarkAndTheNextStartRebuilds) {
    XStore store;
    const std::string db = "LWPSTOP";
    constexpr uint64_t kRows = 60000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    std::atomic<bool> stop{false};
    XStore::LiveWidenStats stats;
    std::future<Result> builder;
    HeldWrite last(FileLockRegistry::Get(YearPath(db, ".XSTORE")), (kRows - 1) * kRowLength, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, &stats, 4, Rpb() * kRowLength, nullptr,
                                  nullptr, /*keep_old=*/false);
    });
    WaitForFrontierPast(zmap, 3 * Rpb());
    stop.store(true);
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(10)), std::future_status::ready) << "the stop was not honoured";
    ASSERT_EQ(builder.get(), Result::kStopped);
    last.Release();

    const uint64_t f = zmap.FrontierForTests();
    ASSERT_NE(f, ZoneMapFile::kNoRebuild);
    EXPECT_LT(f, kRows);
    EXPECT_EQ(catidx.FrontierForTests(), f) << "both files mark the same chunks";
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    const uint64_t complete_blocks = f / Rpb();
    ASSERT_GT(complete_blocks, 0u);
    EXPECT_TRUE(zmap.TryGetBlockStats(complete_blocks - 1).has_value());
    EXPECT_FALSE(zmap.TryGetBlockStats(complete_blocks).has_value()) << "a block past F must not be trusted";
    EXPECT_TRUE(catidx.MightContain(complete_blocks, 0, EncodeFDataForCatIdx(FDouble(-1.0))));
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    EXPECT_FALSE(zmap.MarkClean());
    EXPECT_FALSE(catidx.MarkClean());

    XStore::Shutdown();
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    XStore::ResetShutdownForTests();
    XStore::ClearZoneMapRegistryForTests();
    XStore::ClearCatIdxRegistryForTests();
    BuildStartupReference(store, db, db + "REF", 70); // its InitDB also rebuilds LWPSTOP's dirty index
    ExpectSameFilesAs(db, db + "REF");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
}

// A read failure with four workers: kFailed, both files in rebuild and dirty behind the failed chunk, answers right,
// the next start rebuilds.
TEST_F(XStoreParallelLiveWidenTest, AReadFailureLeavesTheIndexInRebuildBehindTheFailedChunk) {
    XStore store;
    const std::string db = "LWPPREAD";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPread, YearPath(db, ".XSTORE"), 2, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, nullptr, 4, Rpb() * kRowLength),
                  Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_LT(f, kRows) << "the frontier passed the failed chunk";
    EXPECT_EQ(catidx.FrontierForTests(), f);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_TRUE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// A .zmap block write fails with four workers: the zone map stops behind its frontier (dirty, in rebuild), the catidx
// carries on and finishes clean; kFailed.
TEST_F(XStoreParallelLiveWidenTest, AZoneMapWriteFailureStopsOnlyTheZoneMap) {
    XStore store;
    const std::string db = "LWPPWRITE";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    CleanRestartState();
    const std::string path = DbPath(db);
    XStore::LiveWidenStats stats;
    {
        xstore_fault::ScopedFdFault fail(xstore_fault::FdOp::kPwrite, YearPath(db, ".zmap"), 5, 1, EIO);
        EXPECT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 4, Rpb() * kRowLength),
                  Result::kFailed);
        ASSERT_EQ(fail.Failures(), 1);
    }
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    CatIdxFile &catidx = XStore::GetOrOpenCatIdx(path, kYear, kRowLength);
    EXPECT_FALSE(stats.zmap_widened);
    EXPECT_TRUE(stats.catidx_widened);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LT(zmap.FrontierForTests(), kRows);
    EXPECT_FALSE(catidx.Rebuilding());
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    EXPECT_EQ(ReadHeader<CatIdxFileHeader>(YearPath(db, ".catidx")).dirty, 0u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// A WRITE lock held on one data row of group 5: that worker retries (lock timeouts) while the others fold every later
// group; the frontier never passes the held row; once it is released the widen completes within a bounded wait.
TEST_F(XStoreParallelLiveWidenTest, AHeldRowStallsOnlyItsGroup) {
    XStore store;
    const std::string db = "LWPHELD";
    const uint64_t rpb = Rpb();
    const uint64_t rows = 16 * rpb;
    SeedNarrowIndexWideRows(store, db, rows, 70);
    const std::string path = DbPath(db);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    const uint64_t held_row = 5 * rpb + 3;
    XStore::LiveWidenStats stats;
    std::future<Result> builder;
    HeldWrite held(FileLockRegistry::Get(YearPath(db, ".XSTORE")), held_row * kRowLength, kRowLength);
    builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 4, rpb * kRowLength);
    });
    WaitUntil([&] {
        if (!zmap.Rebuilding()) return false;
        for (uint64_t b = 6; b < 16; ++b) {
            if (!zmap.WriterUpdatesRowForTests((b + 1) * rpb - 1)) return false;
        }
        return true;
    }, "the groups after the held one folded");
    // Longer than one lock wait (LIVE_WIDEN_LOCK_WAIT_MS): the stalled worker has timed out at least once.
    std::this_thread::sleep_for(milliseconds(3 * XStoreTunables::LIVE_WIDEN_LOCK_WAIT_MS));
    const uint64_t f = zmap.FrontierForTests();
    EXPECT_LE(f, held_row) << "the frontier passed a row a writer holds";
    EXPECT_GE(f, 5 * rpb) << "groups 0..4 should be folded";
    EXPECT_NE(builder.wait_for(milliseconds(0)), std::future_status::ready) << "finished while the row was held";
    held.Release();
    ASSERT_EQ(builder.wait_for(std::chrono::seconds(30)), std::future_status::ready);
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_GT(stats.lock_timeouts, 0u);
    ExpectAnswersMatchTheData(store, db, 0, rows - 1, {0, 65});
}

// The trigger with four workers: bg_cancel_ (shutdown, RemoveDB) is honoured within about one chunk (a lock wait plus
// a pause, each at most 50 ms), not after the year; the files stay dirty and the next start rebuilds them.
TEST_F(XStoreParallelLiveWidenTest, ACancelStopsEveryWorkerWithinAChunk) {
    XStore store;
    const std::string db = "LWPCANCEL";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    ZoneMapFile &zmap = XStore::GetOrOpenZoneMap(path, kYear, kRowLength);
    std::future<std::vector<XStore::LiveWidenPassEntry>> pass;
    HeldWrite after_data(FileLockRegistry::Get(YearPath(db, ".XSTORE")), kRows * kRowLength, kRowLength);
    pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 4);
    });
    WaitForFrontierPast(zmap, Rpb());
    const Clock::time_point cancelled = Clock::now();
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    const int64_t stop_ms = MsSince(cancelled);
    after_data.Release();
    ASSERT_TRUE(ended) << "the pass ignored the cancel flag";
    EXPECT_LT(stop_ms, 1000) << "the workers took " << stop_ms << " ms to stop";
    EXPECT_EQ(YearsAndResults(pass.get()), (PassResult{{kYear, Result::kStopped}}));
    XStore::bg_cancel_.store(false);
    EXPECT_TRUE(zmap.Rebuilding());
    EXPECT_LE(zmap.FrontierForTests(), kRows);
    EXPECT_EQ(ReadHeader<ZoneMapFileHeader>(YearPath(db, ".zmap")).dirty, 1u);
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {0, 65});
    CleanRestartState();
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}


// ---- 12. Writers first (S3b-1): the builder's READ yields to a waiting writer ---------------------------------------

// Four workers widen while a writer takes a WRITE over the whole year again and again: its waits stay bounded by the
// yield cap plus a chunk's hold (without the yield, back-to-back worker READs could keep it out for over a second), the
// widen completes, and the files equal the startup rebuild.
TEST_F(XStoreParallelLiveWidenTest, AWholeYearWriterWaitsBoundedWhileFourWorkersWiden) {
    XStore store;
    const std::string db = "LWPYIELD";
    const uint64_t rows = 24 * Rpb();
    SeedNarrowIndexWideRows(store, db, rows, 70);
    const std::string path = DbPath(db);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    std::atomic<bool> running{true};
    std::atomic<int64_t> max_wait_us{0};
    std::atomic<uint64_t> takes{0};
    std::thread writer([&] {
        while (running.load()) {
            const Clock::time_point t0 = Clock::now();
            lock.LockRange(0, 0, kWrite);
            const int64_t waited = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
            if (waited > max_wait_us.load()) max_wait_us.store(waited);
            ++takes;
            std::this_thread::sleep_for(milliseconds(1));
            lock.UnlockRange(0, 0, kWrite);
            std::this_thread::sleep_for(milliseconds(2));
        }
    });
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 4, Rpb() * kRowLength);
    });
    const bool in_time = builder.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    running.store(false);
    writer.join();
    ASSERT_TRUE(in_time) << "the widen did not finish within 60 s";
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_GT(takes.load(), 0u);
    EXPECT_GT(stats.writer_yields, 0u) << "the builder never yielded: the test proves little";
    EXPECT_LT(max_wait_us.load(), static_cast<int64_t>(XStoreTunables::LIVE_WIDEN_YIELD_MAX_MS + 300) * 1000)
        << "a waiting writer was kept out past the yield cap";
    std::printf("[live widen yield] writer: %llu takes, max wait %lld us; builder: %llu chunks, %llu yields (%llu ms), "
                "%llu timeouts\n", static_cast<unsigned long long>(takes.load()), static_cast<long long>(max_wait_us.load()),
                static_cast<unsigned long long>(stats.chunks), static_cast<unsigned long long>(stats.writer_yields),
                static_cast<unsigned long long>(stats.writer_yield_us / 1000), static_cast<unsigned long long>(stats.lock_timeouts));
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// Progress guarantee: a writer that waits for the whole run (it waits on a READ the test holds) makes every chunk
// attempt yield for the full cap, never time out, and the widen still completes and equals the startup rebuild.
TEST_F(XStoreLiveWidenTest, AWriterWaitingForeverDelaysEachChunkByTheCapButTheWidenCompletes) {
    XStore store;
    const std::string db = "LWYIELDCAP";
    SeedNarrowIndexWideRows(store, db, 3000, 70);
    const std::string path = DbPath(db);
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(0, kRowLength, kRead);
    std::thread writer([&] {
        lock.LockRange(0, 0, kWrite);
        lock.UnlockRange(0, 0, kWrite);
    });
    WaitForWaitingWriters(lock, 1);
    XStore::LiveWidenStats stats;
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats);
    });
    const Clock::time_point t0 = Clock::now();
    const bool in_time = builder.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    const int64_t elapsed_ms = MsSince(t0);
    EXPECT_EQ(lock.WaitingWritersForTests(), 1u) << "the writer stopped waiting: the test proves little";
    lock.UnlockRange(0, kRowLength, kRead);
    writer.join();
    ASSERT_TRUE(in_time) << "a standing waiting writer stalled the widen";
    EXPECT_EQ(builder.get(), Result::kDone);
    EXPECT_EQ(stats.writer_yields, stats.chunks) << "every chunk attempt yields to the standing writer";
    EXPECT_EQ(stats.lock_timeouts, 0u) << "a yield used up the lock timeout";
    EXPECT_GE(stats.writer_yield_us, stats.chunks * (XStoreTunables::LIVE_WIDEN_YIELD_MAX_MS - 10) * 1000);
    EXPECT_GE(elapsed_ms, static_cast<int64_t>(stats.chunks * (XStoreTunables::LIVE_WIDEN_YIELD_MAX_MS - 10)));
    std::printf("[live widen yield cap] %lld ms, %llu chunks, %llu yields\n", static_cast<long long>(elapsed_ms),
                static_cast<unsigned long long>(stats.chunks), static_cast<unsigned long long>(stats.writer_yields));
    BuildStartupReference(store, db, db + "REF", 70);
    ExpectSameFilesAs(db, db + "REF");
}

// ---- 13. Load governor (S3b-2 G3): the governed live widen -----------------------------------------------------------

// Fake governor inputs. Every clock read advances fake time by one second, so every Permit evaluates (tick 1 s) and a
// parked worker (one Permit per 50 ms) moves fake time 20 s per real second. The sampler's foreground CPU per fake
// second is fg(n) for the n-th sample; `writers` (optional) supplies the writer counters (a real load source).
struct FakeGovernorInputs {
    std::atomic<int64_t> now_ns{1'000'000'000};
    std::mutex mtx;
    double busy_s = 0;
    int64_t last_sample_ns = 1'000'000'000;
    uint64_t samples = 0;
    std::function<double(uint64_t)> fg = [](uint64_t) { return 0.0; };
    livewiden::LiveWidenLoadSource *writers = nullptr;

    Clock::time_point Now() {
        return Clock::time_point(std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(now_ns += 1'000'000'000)));
    }
    livewiden::Signals Sample() {
        std::lock_guard<std::mutex> lk(mtx);
        livewiden::Signals s = writers != nullptr ? writers->Sample() : livewiden::Signals{};
        const int64_t t = now_ns.load();
        // Large enough that the builder's real thread CPU (reported through AddBuilderCpu) barely moves the figure.
        busy_s += fg(samples++) * static_cast<double>(t - last_sample_ns) * 1e-9;
        last_sample_ns = t;
        s.busy_cpu_s = busy_s;
        return s;
    }
};

livewiden::GovernorParams FakeParams(unsigned cores, unsigned max_workers, unsigned floor_workers) {
    livewiden::GovernorParams p;
    p.cores = cores;
    p.max_workers = max_workers;
    p.floor_workers = floor_workers;
    p.headroom_cores = 2;
    p.tick = std::chrono::seconds(1);
    p.brake_hold = std::chrono::seconds(3);
    p.starvation_guard = std::chrono::seconds(5);
    p.at_floor_warn = std::chrono::seconds(1'000'000);
    p.wait_brake_us = 1000;
    p.long_waits_brake = 1;
    p.smoothing_ticks = 1;   // a raise looks at the last tick only: the levels follow fg() tick by tick
    p.label = "governed test";
    return p;
}

std::unique_ptr<livewiden::LiveWidenGovernor> MakeFakeGovernor(FakeGovernorInputs &in, livewiden::GovernorParams p) {
    return std::make_unique<livewiden::LiveWidenGovernor>(std::move(p), [&in] { return in.Sample(); },
                                                          [&in] { return in.Now(); });
}

// The allowed count changes on every tick: with 8 cores, headroom 2 and max 4, an idle tick raises it by one and a
// tick with 100 foreground CPUs drops it to the floor at once, so the pattern idle x3, busy, idle x2, busy gives
// 1 -> 2 -> 3 -> 4 -> 1 -> 2 -> 3 -> 1 -> ... The widen equals the startup rebuild (= the sequential builder), and no
// more workers ever fold at once than the MOST the governor ever allowed during the run (peak_active counts workers
// between a granted Permit and their Release; only a worker whose index is below `allowed` is granted). This is not
// "never more than allowed at that instant": after a drop, workers finish their current group (parking is between
// groups only). Second run: target 2 (4 cores, headroom 2) with 4 workers started: never more than 2 fold at once.
TEST_F(XStoreParallelLiveWidenTest, GovernedWidenIsByteIdenticalAndNeverRunsMoreWorkersThanTheMostEverAllowed) {
    XStore store;
    const std::string db = "LWGOVBYTES";
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Insert(store, db, {{Epoch(0), MixedRow(0, 10)}});
    Rows rows;
    for (uint64_t i = 1; i < 20 * Rpb() + 300; ++i) rows.emplace_back(Epoch(i), MixedRow(i, i % 10 == 0 ? 70 : 40 + static_cast<int>(i % 9)));
    Insert(store, db, rows);
    BuildStartupReference(store, db, db + "REF", 70);
    const std::string path = DbPath(db);
    {
        SCOPED_TRACE("allowed 1..4, changing every tick");
        FakeGovernorInputs in;
        in.fg = [](uint64_t n) { return n % 4 == 3 ? 100.0 : 0.0; };
        auto gov = MakeFakeGovernor(in, FakeParams(8, 4, 1));
        XStore::LiveWidenStats stats;
        ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 1, Rpb() * kRowLength,
                                         gov.get()), Result::kDone);
        ExpectSameFilesAs(db, db + "REF");
        const auto snap = gov->GetSnapshot();
        EXPECT_EQ(snap.min_allowed_seen, 1u);
        EXPECT_EQ(snap.max_allowed_seen, 4u);
        EXPECT_GE(snap.peak_active, 1u);
        EXPECT_LE(snap.peak_active, snap.max_allowed_seen);
        EXPECT_EQ(snap.active, 0u) << "every grant released";
        EXPECT_GT(snap.evaluations, 100u);
        EXPECT_TRUE(stats.governed);
        EXPECT_EQ(stats.governor_allowed_max, 4u);
        EXPECT_GT(stats.governor_grants, 20u);
        std::printf("[governed widen] grants %llu, denials %llu, peak %u, parked %.2f s\n",
                    static_cast<unsigned long long>(snap.grants), static_cast<unsigned long long>(snap.denials),
                    snap.peak_active, stats.governor_parked_s);
    }
    {
        SCOPED_TRACE("allowed at most 2 of 4 started workers");
        FakeGovernorInputs in;
        auto gov = MakeFakeGovernor(in, FakeParams(4, 4, 1));
        XStore::LiveWidenStats stats;
        ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 1, Rpb() * kRowLength,
                                         gov.get()), Result::kDone);
        ExpectSameFilesAs(db, db + "REF");
        const auto snap = gov->GetSnapshot();
        EXPECT_EQ(snap.max_allowed_seen, 2u);
        EXPECT_LE(snap.peak_active, 2u);
        EXPECT_GT(snap.denials, 0u) << "workers 2 and 3 were started and asked";
    }
    ExpectAnswersMatchTheData(store, db, 0, 2 * Rpb() - 1, {0, 2, 64, 66});
}

// The S2 concurrent oracle (writers, overwrites, deletes, exact answers during the widen, final state equal to a
// startup rebuild) with the widen governed, the allowed count moving 1..4.
TEST_F(XStoreParallelLiveWidenTest, GovernedWidenLosesNoUpdateUnderConcurrentWrites) {
    FakeGovernorInputs in;
    in.fg = [](uint64_t n) { return n % 5 == 4 ? 100.0 : 0.0; };
    auto gov = MakeFakeGovernor(in, FakeParams(8, 4, 1));
    RunConcurrentOracle("LWGOVLIVE", 1, Rpb() * kRowLength, gov.get());
    const auto snap = gov->GetSnapshot();
    EXPECT_EQ(snap.max_allowed_seen, 4u);
    EXPECT_LE(snap.peak_active, 4u);
}

// Floor 0 with the CPU target held at 0 (100 foreground CPUs): no worker may fold until the starvation guard (5 fake
// seconds) has passed, then worker 0 gets one group per guard period, so the year completes through single grants.
// Four groups (group_bytes a quarter of the preallocated year file).
TEST_F(XStoreParallelLiveWidenTest, GovernedFloorZeroProgressesOnlyThroughStarvationGrants) {
    XStore store;
    const std::string db = "LWGOVFLOOR0";
    SeedNarrowIndexWideRows(store, db, 3000, 70);
    BuildStartupReference(store, db, db + "REF", 70);
    const std::string path = DbPath(db);
    const uint64_t year_bytes = static_cast<uint64_t>(std::filesystem::file_size(YearPath(db, ".XSTORE")));
    const uint64_t group_bytes = (year_bytes + 3) / 4;
    FakeGovernorInputs in;
    in.fg = [](uint64_t) { return 100.0; };
    auto gov = MakeFakeGovernor(in, FakeParams(4, 2, 0));
    const int64_t t0 = in.now_ns.load();
    XStore::LiveWidenStats stats;
    ASSERT_EQ(store.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, nullptr, &stats, 1, group_bytes, gov.get()),
              Result::kDone);
    ExpectSameFilesAs(db, db + "REF");
    const auto snap = gov->GetSnapshot();
    EXPECT_EQ(snap.max_allowed_seen, 0u) << "only starvation grants";
    EXPECT_EQ(snap.grants, 4u) << "one grant per group";
    EXPECT_EQ(snap.peak_active, 1u) << "only worker 0 is ever granted";
    EXPECT_GT(snap.denials, 0u);
    EXPECT_GE(in.now_ns.load() - t0, 4 * 5 * 1'000'000'000LL) << "each group waited a full guard period";
    EXPECT_GT(stats.governor_parked_s, 0.0);
}

// Every worker parked (floor 0, guard far away): the stop flag ends the year within a few 50 ms park slices.
TEST_F(XStoreParallelLiveWidenTest, GovernedStopWhileEveryWorkerIsParkedReturnsPromptly) {
    XStore store;
    const std::string db = "LWGOVSTOP";
    SeedNarrowIndexWideRows(store, db, 3000, 70);
    const std::string path = DbPath(db);
    FakeGovernorInputs in;
    in.fg = [](uint64_t) { return 100.0; };
    livewiden::GovernorParams p = FakeParams(4, 2, 0);
    p.starvation_guard = std::chrono::seconds(1'000'000'000);
    auto gov = MakeFakeGovernor(in, p);
    std::atomic<bool> stop{false};
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, nullptr, 1, Rpb() * kRowLength,
                                  gov.get());
    });
    const Clock::time_point deadline = Clock::now() + std::chrono::seconds(10);
    while (gov->GetSnapshot().denials < 10 && Clock::now() < deadline) std::this_thread::sleep_for(milliseconds(5));
    ASSERT_GE(gov->GetSnapshot().denials, 10u) << "the workers never parked";
    EXPECT_EQ(gov->GetSnapshot().grants, 0u);
    const Clock::time_point stopped_at = Clock::now();
    stop.store(true);
    const bool ended = builder.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
    const int64_t stop_ms = MsSince(stopped_at);
    ASSERT_TRUE(ended);
    EXPECT_EQ(builder.get(), Result::kStopped);
    EXPECT_LT(stop_ms, 200) << "parked workers took " << stop_ms << " ms to see the stop flag";
    EXPECT_TRUE(XStore::GetOrOpenZoneMap(path, kYear, kRowLength).Rebuilding());
}

// One governor for a pass over two years (as RunLiveWidenPass shares it): the older year's data-file lock already
// carries long writer waits from before the pass. At the switch to that year LiveWidenOneYear points the load source
// at the new lock and rebaselines, so its (larger) cumulative counters are a baseline, not a writer brake. Writer
// counters come from a real LiveWidenLoadSource (CPU from a missing /proc file: constant).
TEST_F(XStoreParallelLiveWidenTest, GovernedPassOverTwoYearsDoesNotBrakeOnTheNextFilesCounters) {
    XStore store;
    const std::string db = "LWGOVYEARS";
    const std::string path = DbPath(db);
    CreateTestDb(store, db, XStore_Proto::SECOND, kRowLength);
    Rows rows;
    for (unsigned short y : {1979, 1980}) {
        Insert(store, db, {{Jan1(y), ValueRow(1, 10)}});
        for (uint64_t r = 1; r < 3 * Rpb(); ++r) rows.emplace_back(Jan1(y) + r, ValueRow(r, 70));
    }
    Insert(store, db, rows);
    // Two long writer waits on 1979's lock (processed second: newest year first).
    FileRangeLock &old_lock = FileLockRegistry::Get(YearPath(db, ".XSTORE", 1979));
    for (int i = 0; i < 2; ++i) {
        old_lock.LockRange(0, kRowLength, kRead);
        std::thread writer([&] {
            old_lock.LockRange(0, kRowLength, kWrite);
            old_lock.UnlockRange(0, kRowLength, kWrite);
        });
        WaitForWaitingWriters(old_lock, 1);
        std::this_thread::sleep_for(milliseconds(40));
        old_lock.UnlockRange(0, kRowLength, kRead);
        writer.join();
    }
    ASSERT_GE(old_lock.WriterWaits().long_waits, 2u);
    ASSERT_LT(FileLockRegistry::Get(YearPath(db, ".XSTORE", 1980)).WriterWaits().long_waits,
              old_lock.WriterWaits().long_waits) << "the second file's counters must be the larger ones";

    livewiden::LiveWidenLoadSource load((scratch->test_root / "no_such_proc_stat").string());
    FakeGovernorInputs in;
    in.writers = &load;
    auto gov = MakeFakeGovernor(in, FakeParams(8, 4, 1));
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    const auto pass = store.RunLiveWidenPass(g + std::chrono::seconds(30), 1, gov.get(), &load);
    EXPECT_EQ(YearsAndResults(pass), (PassResult{{1980, Result::kDone}, {1979, Result::kDone}}));
    const auto snap = gov->GetSnapshot();
    EXPECT_EQ(snap.brakes, 0u) << "the next file's cumulative counters were taken as writer waits";
    EXPECT_EQ(snap.max_allowed_seen, 4u);
    EXPECT_EQ(load.Progress(), 0.0) << "the year is cleared after its workers are joined";
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1979, kRowLength).TrackedColumnCount(), 70u);
    EXPECT_EQ(XStore::GetOrOpenZoneMap(path, 1980, kRowLength).TrackedColumnCount(), 70u);
}

// LiveWidenLoadSource: busy CPU from /proc/stat never goes backwards while this thread burns CPU; the writer counters
// are read through the year's lock pointer; an unreadable /proc file repeats the previous values; progress follows
// the watermark.
TEST(LiveWidenLoadSourceTest, ReadsCpuAndTheYearsWriterCountersAndSurvivesAMissingProcFile) {
    livewiden::LiveWidenLoadSource load;
    EXPECT_GE(load.Cores(), 1u);
    double prev = load.Sample().busy_cpu_s;
    EXPECT_GT(prev, 0.0);
    volatile uint64_t sink = 0;
    for (int i = 0; i < 20; ++i) {
        const Clock::time_point until = Clock::now() + milliseconds(10);
        while (Clock::now() < until) sink = sink + 1;
        const double cur = load.Sample().busy_cpu_s;
        EXPECT_GE(cur, prev);
        prev = cur;
    }

    FileRangeLock lock;
    std::atomic<uint64_t> done{25};
    load.SetYear(&lock, &done, 100);
    EXPECT_DOUBLE_EQ(load.Progress(), 0.25);
    EXPECT_EQ(load.Sample().long_waits, 0u);
    lock.LockRange(0, 10, kRead);
    std::thread writer([&] {
        lock.LockRange(0, 10, kWrite);
        lock.UnlockRange(0, 10, kWrite);
    });
    WaitForWaitingWriters(lock, 1);
    std::this_thread::sleep_for(milliseconds(30));
    lock.UnlockRange(0, 10, kRead);
    writer.join();
    const livewiden::Signals s = load.Sample();
    EXPECT_EQ(s.writer_waits, 1u);
    EXPECT_EQ(s.long_waits, 1u);
    EXPECT_GE(s.writer_wait_us, 25'000u);
    load.SetYear(nullptr, nullptr, 0);
    EXPECT_EQ(load.Progress(), 0.0);
    EXPECT_EQ(load.Sample().writer_waits, 1u) << "no lock: the previous values repeat";

    livewiden::LiveWidenLoadSource missing("/nonexistent/proc/stat");
    for (int i = 0; i < 3; ++i) EXPECT_EQ(missing.Sample().busy_cpu_s, 0.0);
    // Real values first, then the file goes away: the last good value repeats.
    ScratchDir dir("xstore_load_source_test");
    const std::string fake = (dir.test_root / "stat").string();
    { std::ofstream(fake) << "cpu  100 0 100 1000 0 0 0 0 0 0\n"; }
    livewiden::LiveWidenLoadSource flaky(fake);
    const double good = flaky.Sample().busy_cpu_s;
    EXPECT_GT(good, 0.0);
    std::filesystem::remove(fake);
    EXPECT_EQ(flaky.Sample().busy_cpu_s, good);
    { std::ofstream(fake) << "garbage\n"; }
    EXPECT_EQ(flaky.Sample().busy_cpu_s, good);
}

// (f) A governor whose fake CPU signal holds it at 1 of 4 workers (row kStuck WRITE-locked as above): the notice says "slowed: foreground CPU". (g) After a
// stop the year stays counted and keeps its notice with the last percentage (no "slowed": nothing governs it now) until
// the next start (here: the registries dropped, as a restart does). keep_old=false: in-place rebuild coverage (with
// keep-old, the default since 2026-10-17, a low column is silent; the notice moves to columns >= the old width).
TEST_F(XStoreParallelLiveWidenTest, RebuildNoticeSaysSlowedUnderAGovernorAndKeepsItsPercentAfterAStop) {
    XStore store;
    const std::string db = "LWNOTICEGOV";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const uint32_t before = XStore::YearsRebuilding();
    const uint64_t kStuck = 10 * Rpb() + 7;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    FakeGovernorInputs in;
    in.fg = [](uint64_t) { return 100.0; };
    auto gov = MakeFakeGovernor(in, FakeParams(4, 4, 1));
    std::atomic<bool> stop{false};
    auto builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, true, &stop, nullptr, 1, Rpb() * kRowLength,
                                  gov.get(), nullptr, /*keep_old=*/false);
    });
    const XStore::LiveRebuildProgress p = WaitForRebuildProgress(path, kYear, kStuck, /*want_slowed=*/true);
    EXPECT_EQ(p.done_rows, kStuck);
    EXPECT_STREQ(p.slowed, "foreground CPU");
    const std::string pct = " in 1980 (" + std::to_string(kStuck * 100 / std::max<uint64_t>(1, p.total_rows)) + "% done";
    const std::string governed = MaxStatus(store, db, Epoch(kStuck + 1), Epoch(kRows - 1), 5);
    EXPECT_NE(governed.find(kBeingIndexed + pct + "; slowed: foreground CPU)"), std::string::npos) << governed;

    stop.store(true);
    const bool ended = builder.wait_for(seconds(10)) == std::future_status::ready;
    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    ASSERT_TRUE(ended);
    EXPECT_EQ(builder.get(), Result::kStopped);
    EXPECT_EQ(XStore::YearsRebuilding(), before + 1) << "a stopped widen stays counted";
    const std::string stopped = MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 5);
    EXPECT_NE(stopped.find(kBeingIndexed + pct + ")"), std::string::npos) << stopped;
    EXPECT_NE(MaxStatus(store, db, Epoch(0), Epoch(10), 65).find(kBeingIndexed + pct + ")"), std::string::npos);
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kStuck - 1), 5), "SUCCESS");
    ExpectAnswersMatchTheData(store, db, 0, kRows - 1, {5, 65});

    XStore::ClearZoneMapRegistryForTests();
    EXPECT_EQ(XStore::YearsRebuilding(), before);
}

// Under a governor every progress line carries its workers and, while it holds the widen back, the reason.
TEST_F(XStoreParallelLiveWidenTest, ProgressLogCarriesTheGovernorsWorkersAndSlowedReason) {
    XStore store;
    const std::string db = "LWPROGGOV";
    constexpr uint64_t kRows = 20000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const std::string path = DbPath(db);
    const Clock::time_point g = *XStore::LiveWidenLastGrowthForTests(path);
    const uint64_t kStuck = 10 * Rpb() + 7;
    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kStuck * kRowLength, kRowLength, kWrite);
    FakeGovernorInputs in;
    in.fg = [](uint64_t) { return 100.0; };
    auto gov = MakeFakeGovernor(in, FakeParams(4, 4, 1));
    ProgressLines capture;
    const rebuildprogress::Options options = capture.Options();
    auto pass = std::async(std::launch::async, [&] {
        XStore b;
        return b.RunLiveWidenPass(g + seconds(30), 1, gov.get(), nullptr, &options);
    });
    const bool slowed = capture.WaitFor(" | workers 1/4 | slowed: foreground CPU");
    XStore::bg_cancel_.store(true);
    const bool ended = pass.wait_for(seconds(30)) == std::future_status::ready;
    lock.UnlockRange(kStuck * kRowLength, kRowLength, kWrite);
    XStore::bg_cancel_.store(false);
    ASSERT_TRUE(ended);
    EXPECT_TRUE(slowed);
    pass.get();
    for (const std::string &line : capture.Copy()) {
        SCOPED_TRACE(line);
        EXPECT_EQ(line.rfind("[LIVE WIDEN] [DB: LWPROGGOV] - ", 0), 0u);
        EXPECT_NE(line.find(" | workers "), std::string::npos);
        EXPECT_NE(line.find("/4"), std::string::npos);
    }
}

// Two live widens on one year, one per file (BeginLiveRebuild admits one builder per file): the catidx by a governed
// builder whose workers stay parked (its watermark stays 0), the zone map by an ungoverned one held at row kFar (the
// middle of the preallocated year: it skips the holes up to there) by a WRITE lock. The year counts once; each file
// reports its own watermark and only the governed builder's file says "slowed"; the zone map's builder finishing does
// not end the catidx's notice. keep_old=false on both builders: in-place rebuild coverage (with keep-old, the default
// since 2026-10-17, the low columns these assertions use are silent).
TEST_F(XStoreParallelLiveWidenTest, RebuildNoticeFollowsEachFileOfAYearWithTwoBuilders) {
    XStore store;
    const std::string db = "LWNOTICE2";
    constexpr uint64_t kRows = 8000;
    SeedNarrowIndexWideRows(store, db, kRows, 70);
    const uint64_t total = static_cast<uint64_t>(std::filesystem::file_size(YearPath(db, ".XSTORE"))) / kRowLength;
    const uint64_t kFar = total / 2;
    Insert(store, db, {{Epoch(kFar), ValueRow(1, 70)}, {Epoch(kFar + 5), ValueRow(2, 70)}});
    const std::string path = DbPath(db);
    const uint32_t before = XStore::YearsRebuilding();

    FakeGovernorInputs in;
    in.fg = [](uint64_t) { return 100.0; };
    livewiden::GovernorParams params = FakeParams(4, 2, 0);
    params.starvation_guard = std::chrono::seconds(1'000'000'000);
    auto gov = MakeFakeGovernor(in, params);
    std::atomic<bool> stop_catidx{false};
    auto catidx_builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, false, true, &stop_catidx, nullptr, 1, Rpb() * kRowLength,
                                  gov.get(), nullptr, /*keep_old=*/false);
    });
    WaitForRebuildProgress(path, kYear, 0, false, /*catidx=*/true);

    FileRangeLock &lock = FileLockRegistry::Get(YearPath(db, ".XSTORE"));
    lock.LockRange(kFar * kRowLength, kRowLength, kWrite);
    auto zmap_builder = std::async(std::launch::async, [&] {
        XStore b;
        return b.LiveWidenOneYear(path, kYear, kRowLength, 70, true, false, nullptr, nullptr, 1, XStoreTunables::LIVE_WIDEN_GROUP_BYTES, nullptr, nullptr,
                                  /*keep_old=*/false);
    });
    // (no ASSERT while the lock is held)
    const XStore::LiveRebuildProgress z = WaitForRebuildProgress(path, kYear, kFar);
    XStore::LiveRebuildProgress c;
    EXPECT_TRUE(XStore::FindRebuildProgress(path, kYear, /*catidx=*/true, c));
    EXPECT_EQ(z.done_rows, kFar) << "the zone map's own watermark";
    EXPECT_EQ(c.done_rows, 0u) << "the catidx's own watermark (parked builder)";
    EXPECT_EQ(XStore::YearsRebuilding(), before + 1) << "one year, counted once";
    const std::string zstate = " in 1980 (" + std::to_string(kFar * 100 / total) + "% done)";
    EXPECT_NE(zstate, " in 1980 (0% done)");
    const std::string low = MaxStatus(store, db, Epoch(kFar + 1), Epoch(kFar + 10), 5);
    EXPECT_NE(low.find(kBeingIndexed + zstate), std::string::npos) << low;
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 5), "SUCCESS");
    const std::string find_low = FindStatus(store, db, Epoch(0), Epoch(kRows - 1), 5);
    EXPECT_NE(find_low.find(kBeingIndexed + std::string(" in 1980 (0% done; slowed: foreground CPU)")), std::string::npos)
        << find_low << " (the catidx's builder is the governed one)";

    lock.UnlockRange(kFar * kRowLength, kRowLength, kWrite);
    const bool zmap_ended = zmap_builder.wait_for(std::chrono::seconds(60)) == std::future_status::ready;
    EXPECT_TRUE(zmap_ended);
    if (zmap_ended) EXPECT_EQ(zmap_builder.get(), Result::kDone);
    EXPECT_FALSE(XStore::FindRebuildProgress(path, kYear, /*catidx=*/false, c));
    EXPECT_TRUE(XStore::FindRebuildProgress(path, kYear, /*catidx=*/true, c)) << "the zone map's end ended the catidx's";
    EXPECT_EQ(XStore::YearsRebuilding(), before + 1);
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kFar + 10), 5), "SUCCESS");
    EXPECT_EQ(MaxStatus(store, db, Epoch(0), Epoch(kRows - 1), 65), "SUCCESS");
    EXPECT_NE(FindStatus(store, db, Epoch(0), Epoch(kRows - 1), 5).find(kBeingIndexed), std::string::npos);

    stop_catidx.store(true);
    ASSERT_EQ(catidx_builder.wait_for(std::chrono::seconds(10)), std::future_status::ready);
    EXPECT_EQ(catidx_builder.get(), Result::kStopped);
    EXPECT_EQ(XStore::YearsRebuilding(), before + 1) << "the stopped catidx keeps the year counted";
    XStore::ClearZoneMapRegistryForTests();
    EXPECT_EQ(XStore::YearsRebuilding(), before);
}

} // namespace
