// xstore::MemoryPool: the server-wide pool of memory that large operations reserve before they allocate. A request waits for its turn
// (first come, first served) when the pool, or the host, has no room; a reservation is returned when its last owner lets go.
#include <gtest/gtest.h>
#include "include/XStore/XStore_MemoryPool.h"
#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <random>
#include <thread>
#include <vector>

namespace {

using xstore::MemoryLease;
using xstore::MemoryPool;
using Clock = MemoryPool::Clock;
using namespace std::chrono_literals;
constexpr uint64_t MB = 1024 * 1024;
const auto kForever = Clock::time_point::max();

// Waits (at most 5 s, so a failing test never hangs) until `n` requests are in the line.
bool WaitForWaiting(const MemoryPool &pool, size_t n) {
    for (int i = 0; i < 5000 && pool.WaitingCount() < n; ++i) std::this_thread::sleep_for(1ms);
    return pool.WaitingCount() >= n;
}

std::shared_ptr<MemoryLease> MustAcquire(MemoryPool &pool, uint64_t bytes) {
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(bytes, kForever, lease), MemoryPool::Result::kGranted);
    return lease;
}

TEST(MemoryPoolTest, GrantsWithinTheLimitAndGivesBackWhenTheLastOwnerLetsGo) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    auto a = MustAcquire(pool, 40 * MB);
    auto b = MustAcquire(pool, 50 * MB);
    EXPECT_EQ(pool.UsedBytes(), 90 * MB);
    EXPECT_EQ(pool.PeakBytes(), 90 * MB);
    auto a_second_owner = a;   // a handler and a WAL record co-own one lease
    a.reset();
    EXPECT_EQ(pool.UsedBytes(), 90 * MB) << "released while a second owner still holds the lease";
    a_second_owner.reset();
    EXPECT_EQ(pool.UsedBytes(), 50 * MB);
    b.reset();
    EXPECT_EQ(pool.UsedBytes(), 0u);
    EXPECT_EQ(pool.PeakBytes(), 90 * MB);
}

TEST(MemoryPoolTest, ARequestLargerThanTheWholePoolIsRefusedAtOnce) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(101 * MB, kForever, lease), MemoryPool::Result::kTooLarge);
    EXPECT_EQ(lease, nullptr);
    EXPECT_EQ(pool.UsedBytes(), 0u);
    EXPECT_EQ(pool.Acquire(100 * MB, kForever, lease), MemoryPool::Result::kGranted) << "exactly the limit is admitted";
}

TEST(MemoryPoolTest, AFullPoolMakesTheNextRequestWaitUntilMemoryIsReturned) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    auto holder = MustAcquire(pool, 80 * MB);
    std::promise<void> granted;
    std::shared_ptr<MemoryLease> waiter_lease;
    std::thread waiter([&] {
        EXPECT_EQ(pool.Acquire(60 * MB, kForever, waiter_lease), MemoryPool::Result::kGranted);
        granted.set_value();
    });
    auto done = granted.get_future();
    EXPECT_EQ(done.wait_for(200ms), std::future_status::timeout) << "admitted although the pool had no room";
    EXPECT_EQ(pool.WaitingCount(), 1u);
    holder.reset();
    EXPECT_EQ(done.wait_for(5s), std::future_status::ready) << "not admitted after the memory was returned";
    waiter.join();
    EXPECT_EQ(pool.UsedBytes(), 60 * MB);
}

TEST(MemoryPoolTest, WaitingIsFirstComeFirstServedASmallRequestDoesNotOvertakeABigOne) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    auto holder = MustAcquire(pool, 70 * MB);   // 30 MB free
    std::atomic<int> order{0};
    int big_rank = 0, small_rank = 0;
    std::shared_ptr<MemoryLease> big_lease, small_lease;
    std::thread big([&] { ASSERT_EQ(pool.Acquire(60 * MB, kForever, big_lease), MemoryPool::Result::kGranted); big_rank = ++order; });
    ASSERT_TRUE(WaitForWaiting(pool, 1));
    std::thread small([&] { ASSERT_EQ(pool.Acquire(10 * MB, kForever, small_lease), MemoryPool::Result::kGranted); small_rank = ++order; });
    ASSERT_TRUE(WaitForWaiting(pool, 2));
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(order.load(), 0) << "the small request (it would fit in the 30 MB free) went ahead of the big one waiting longer";
    holder.reset();
    big.join();
    small.join();
    EXPECT_EQ(big_rank, 1);
    EXPECT_EQ(small_rank, 2);
}

TEST(MemoryPoolTest, ADeadlineEndsTheWaitWithNothingReserved) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    auto holder = MustAcquire(pool, 90 * MB);
    std::shared_ptr<MemoryLease> lease;
    const auto t0 = Clock::now();
    EXPECT_EQ(pool.Acquire(50 * MB, t0 + 150ms, lease), MemoryPool::Result::kTimedOut);
    EXPECT_GE(Clock::now() - t0, 140ms);
    EXPECT_LT(Clock::now() - t0, 3s);
    EXPECT_EQ(lease, nullptr);
    EXPECT_EQ(pool.UsedBytes(), 90 * MB);
    EXPECT_EQ(pool.WaitingCount(), 0u) << "a request that gave up is still in the line";
}

TEST(MemoryPoolTest, ARequestThatGaveUpDoesNotBlockTheOnesBehindIt) {
    MemoryPool pool(100 * MB, 0, [] { return MemoryPool::kUnknown; });
    auto holder = MustAcquire(pool, 70 * MB);   // 30 MB free
    std::shared_ptr<MemoryLease> big_lease, small_lease;
    std::thread big([&] { EXPECT_EQ(pool.Acquire(60 * MB, Clock::now() + 300ms, big_lease), MemoryPool::Result::kTimedOut); });
    ASSERT_TRUE(WaitForWaiting(pool, 1));
    std::thread small([&] { EXPECT_EQ(pool.Acquire(10 * MB, kForever, small_lease), MemoryPool::Result::kGranted); });
    big.join();    // the big one gives up after 300 ms; the small one, behind it, is then admitted
    small.join();
    EXPECT_EQ(pool.UsedBytes(), 80 * MB);
}

TEST(MemoryPoolTest, TheHostReserveIsKeptEvenWhenThePoolHasRoom) {
    std::atomic<uint64_t> available{50 * MB};
    MemoryPool pool(1000 * MB, 20 * MB, [&] { return available.load(); });
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(40 * MB, Clock::now() + 150ms, lease), MemoryPool::Result::kTimedOut) << "40 MB + the 20 MB reserve > the 50 MB available";
    EXPECT_EQ(pool.Acquire(30 * MB, Clock::now() + 150ms, lease), MemoryPool::Result::kGranted) << "30 MB + 20 MB reserve fits in 50 MB";
    // Other processes freeing memory lets a waiting request in without anyone calling Release.
    std::shared_ptr<MemoryLease> second;
    std::thread t([&] { EXPECT_EQ(pool.Acquire(100 * MB, kForever, second), MemoryPool::Result::kGranted); });
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(pool.UsedBytes(), 30 * MB);
    available = 500 * MB;
    t.join();
    EXPECT_EQ(pool.UsedBytes(), 130 * MB);
}

TEST(MemoryPoolTest, AReservationNotYetAllocatedIsSubtractedFromTheHostsFigureUntilMarkedResident) {
    // The OS figure does not move when a lease is granted (nothing is allocated yet): two requests that each fit alone must not both
    // be admitted against the same available memory.
    MemoryPool pool(1000 * MB, 0, [] { return 100 * MB; });
    auto first = MustAcquire(pool, 80 * MB);
    EXPECT_EQ(pool.PendingBytes(), 80 * MB);
    std::shared_ptr<MemoryLease> second;
    EXPECT_EQ(pool.Acquire(60 * MB, Clock::now() + 150ms, second), MemoryPool::Result::kTimedOut) << "admitted against memory the first lease will use";
    first->MarkResident();
    EXPECT_EQ(pool.PendingBytes(), 0u);
    first->MarkResident();   // idempotent
    EXPECT_EQ(pool.PendingBytes(), 0u);
    EXPECT_EQ(pool.Acquire(60 * MB, Clock::now() + 150ms, second), MemoryPool::Result::kGranted) << "the provider here is fixed: once resident, the figure is taken as it is";
}

TEST(MemoryPoolTest, ReleasingAnUnmarkedLeaseClearsItsPendingBytes) {
    MemoryPool pool(1000 * MB, 0, [] { return MemoryPool::kUnknown; });
    { auto lease = MustAcquire(pool, 80 * MB); EXPECT_EQ(pool.PendingBytes(), 80 * MB); }
    EXPECT_EQ(pool.PendingBytes(), 0u);
    EXPECT_EQ(pool.UsedBytes(), 0u);
}

TEST(MemoryPoolTest, ManyThreadsNeverExceedTheLimitAndEverythingIsReturned) {
    constexpr uint64_t kLimit = 200 * MB;
    MemoryPool pool(kLimit, 0, [] { return MemoryPool::kUnknown; });
    std::atomic<uint64_t> in_hand{0}, worst{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < 16; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937 rng(t);
            for (int i = 0; i < 200; ++i) {
                const uint64_t bytes = (1 + rng() % 60) * MB;
                std::shared_ptr<MemoryLease> lease;
                ASSERT_EQ(pool.Acquire(bytes, kForever, lease), MemoryPool::Result::kGranted);
                const uint64_t now = in_hand.fetch_add(bytes) + bytes;
                uint64_t prev = worst.load();
                while (now > prev && !worst.compare_exchange_weak(prev, now)) {}
                std::this_thread::yield();
                in_hand.fetch_sub(bytes);
            }
        });
    }
    for (auto &t : threads) t.join();
    EXPECT_LE(worst.load(), kLimit) << "more was held at once than the pool's limit";
    EXPECT_LE(pool.PeakBytes(), kLimit);
    EXPECT_EQ(pool.UsedBytes(), 0u);
    EXPECT_EQ(pool.PendingBytes(), 0u);
    EXPECT_EQ(pool.WaitingCount(), 0u);
}

TEST(MemoryPoolTest, TheProcessWidePoolIsSizedFromTheMachineAndLeavesTheReserve) {
    MemoryPool &pool = MemoryPool::Instance();
    EXPECT_GT(pool.Limit(), 0u);
    EXPECT_GT(pool.Reserve(), 0u);
    EXPECT_LE(pool.Reserve(), 4096 * MB);
}

}  // namespace
