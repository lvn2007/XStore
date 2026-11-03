// xstore::MemoryPool: the server-wide pool of memory that large operations reserve before they allocate. A request waits for its turn
// (first come, first served) when the pool, or the host, has no room; a reservation is returned when its last owner lets go.
#include <gtest/gtest.h>
#include "include/XStore/XStore_MemoryPool.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <unistd.h>

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

// A fake cgroup v2 tree: the pool must read the limit of the cgroup the process is IN (named by /proc/self/cgroup), not of the root
// (a systemd-run scope or a container slice has no limit file at the root at all).
class FakeCgroups {
public:
    FakeCgroups() : root_(std::filesystem::temp_directory_path() / ("xstore_cgroup_test_" + std::to_string(::getpid()) + "_" + std::to_string(counter_++))) {
        std::filesystem::create_directories(root_);
    }
    ~FakeCgroups() { std::error_code ec; std::filesystem::remove_all(root_, ec); }
    void Set(const std::string &cgroup, const std::string &max, uint64_t current) {
        std::filesystem::create_directories(root_ / cgroup.substr(1));
        std::ofstream((root_ / cgroup.substr(1) / "memory.max").string()) << max << "\n";
        std::ofstream((root_ / cgroup.substr(1) / "memory.current").string()) << current << "\n";
    }
    // memory.stat of a cgroup: the page cache the kernel can drop (inactive_file), minus what is dirty or being written back.
    void SetStat(const std::string &cgroup, uint64_t inactive_file, uint64_t file_dirty, uint64_t file_writeback) {
        std::ofstream((root_ / cgroup.substr(1) / "memory.stat").string())
            << "anon 123\nfile 456\ninactive_file " << inactive_file << "\nactive_file 789\nfile_dirty " << file_dirty
            << "\nfile_writeback " << file_writeback << "\n";
    }
    std::string ProcFile(const std::string &cgroup) {
        const std::string path = (root_ / "proc_self_cgroup").string();
        std::ofstream(path) << "0::" << cgroup << "\n";
        return path;
    }
    std::string Root() const { return root_.string(); }

private:
    static inline int counter_ = 0;
    std::filesystem::path root_;
};
constexpr uint64_t GB = 1024 * MB;

TEST(MemoryPoolTest, TheCgroupLimitIsReadFromTheProcessOwnCgroupNotTheRoot) {
    FakeCgroups cg;   // no memory.max at the root, like /sys/fs/cgroup on a systemd host
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 3 * GB);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 8 * GB);
    EXPECT_EQ(m.headroom, 5 * GB);
}

TEST(MemoryPoolTest, ATighterLimitOnAnAncestorWins) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 1 * GB);
    cg.Set("/system.slice", std::to_string(4 * GB), 4 * GB - 512 * MB);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 4 * GB);
    EXPECT_EQ(m.headroom, 512 * MB);
}

TEST(MemoryPoolTest, NoLimitAnywhereMeansUnlimited) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", "max", 3 * GB);
    cg.Set("/system.slice", "max", 3 * GB);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 0u);
    EXPECT_EQ(m.headroom, MemoryPool::kUnknown);
    const xstore::CgroupMemory missing = xstore::ReadCgroupMemory(cg.Root(), cg.Root() + "/no_such_file");
    EXPECT_EQ(missing.max, 0u);
    EXPECT_EQ(missing.headroom, MemoryPool::kUnknown);
}

// A write-heavy process fills its cgroup with page cache; memory.current counts it, but the kernel drops clean cache before it kills anything, so the
// pool must not treat it as used (an insert would wait for memory that is only cache).
TEST(MemoryPoolTest, CleanInactivePageCacheCountsAsHeadroom) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 8 * GB - 100 * MB);
    cg.SetStat("/system.slice/run-1.scope", 5 * GB, 0, 0);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 8 * GB);
    EXPECT_EQ(m.headroom, 100 * MB + 5 * GB);
}

TEST(MemoryPoolTest, DirtyAndWritebackPagesAreNotCountedAsHeadroom) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 8 * GB - 100 * MB);
    cg.SetStat("/system.slice/run-1.scope", 5 * GB, 1 * GB, 512 * MB);   // only 3.5 GiB of the 5 GiB can be dropped without writing it first
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.headroom, 100 * MB + 5 * GB - 1 * GB - 512 * MB);
}

TEST(MemoryPoolTest, MoreDirtyPagesThanInactiveCacheAddsNothing) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 7 * GB);
    cg.SetStat("/system.slice/run-1.scope", 1 * GB, 3 * GB, 2 * GB);   // dirty pages can sit on the active list too
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.headroom, 1 * GB);
}

TEST(MemoryPoolTest, HeadroomNeverExceedsTheLimitWhateverMemoryStatSays) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(1 * GB), 400 * MB);
    cg.SetStat("/system.slice/run-1.scope", 900 * MB, 0, 0);   // more cache than the cgroup holds: a stale or odd file
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.headroom, 1 * GB);
}

TEST(MemoryPoolTest, WithoutMemoryStatTheHeadroomIsTheOldOne) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 3 * GB);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.headroom, 5 * GB);
}

TEST(MemoryPoolTest, EachLimitedLevelUsesItsOwnPageCache) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", std::to_string(8 * GB), 1 * GB);
    cg.Set("/system.slice", std::to_string(4 * GB), 4 * GB - 512 * MB);
    cg.SetStat("/system.slice/run-1.scope", 900 * MB, 0, 0);   // the inner cache does not make room in the outer level
    cg.SetStat("/system.slice", 2 * GB, 0, 0);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 4 * GB);
    EXPECT_EQ(m.headroom, 512 * MB + 2 * GB);
}

TEST(MemoryPoolTest, AnUnlimitedLevelIgnoresItsMemoryStat) {
    FakeCgroups cg;
    cg.Set("/system.slice/run-1.scope", "max", 3 * GB);
    cg.SetStat("/system.slice/run-1.scope", 2 * GB, 0, 0);
    const xstore::CgroupMemory m = xstore::ReadCgroupMemory(cg.Root(), cg.ProcFile("/system.slice/run-1.scope"));
    EXPECT_EQ(m.max, 0u);
    EXPECT_EQ(m.headroom, MemoryPool::kUnknown);
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
    std::atomic<uint64_t> available{60 * MB};
    MemoryPool pool(1000 * MB, 20 * MB, [&] { return available.load(); });
    auto other_work = MustAcquire(pool, 1 * MB);   // something else is running, so the nothing-else-running rule does not apply
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(40 * MB, Clock::now() + 150ms, lease), MemoryPool::Result::kTimedOut) << "40 MB + the 20 MB reserve > the 59 MB left";
    EXPECT_EQ(pool.Acquire(30 * MB, Clock::now() + 150ms, lease), MemoryPool::Result::kGranted) << "30 MB + 20 MB reserve fits in 59 MB";
    // Other processes freeing memory lets a waiting request in without anyone calling Release.
    std::shared_ptr<MemoryLease> second;
    std::thread t([&] { EXPECT_EQ(pool.Acquire(100 * MB, kForever, second), MemoryPool::Result::kGranted); });
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(pool.UsedBytes(), 31 * MB);
    available = 500 * MB;
    t.join();
    EXPECT_EQ(pool.UsedBytes(), 131 * MB);
}

// The server keeps memory it has freed (the allocator does not hand it back), so the OS figure can stay low for good after every
// lease is gone. With nothing else running no memory is coming back, so waiting would never end: the front request goes ahead.
TEST(MemoryPoolTest, WithNothingElseRunningTheFrontRequestIsAdmittedEvenWhenTheHostFigureSaysNo) {
    MemoryPool pool(1000 * MB, 20 * MB, [] { return 50 * MB; });
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(100 * MB, Clock::now() + 500ms, lease), MemoryPool::Result::kGranted) << "100 MB + 20 MB reserve > 50 MB, but nothing is running";
    std::shared_ptr<MemoryLease> second;
    EXPECT_EQ(pool.Acquire(10 * MB, Clock::now() + 150ms, second), MemoryPool::Result::kTimedOut) << "the rule is only for the one running alone";
    lease.reset();
    EXPECT_EQ(pool.Acquire(10 * MB, Clock::now() + 500ms, second), MemoryPool::Result::kGranted);
}

// A request's own received bytes are already in the OS figure by the time it asks; counting them again in the host check makes a
// request wait for memory it is already holding.
TEST(MemoryPoolTest, BytesTheRequestAlreadyHoldsAreNotCountedAgainstTheHostFigure) {
    MemoryPool pool(1000 * MB, 20 * MB, [] { return 100 * MB; });
    auto other_work = MustAcquire(pool, 1 * MB);
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(150 * MB, Clock::now() + 150ms, lease), MemoryPool::Result::kTimedOut) << "without the hint: 150 + 20 > 99";
    EXPECT_EQ(pool.Acquire(150 * MB, Clock::now() + 500ms, lease, 100 * MB), MemoryPool::Result::kGranted) << "50 new + 20 reserve fits in 99";
    EXPECT_EQ(pool.UsedBytes(), 151 * MB) << "the pool limit still counts the whole estimate";
    EXPECT_EQ(pool.PendingBytes(), 51 * MB) << "only the part not yet in the OS figure is pending";
    lease->MarkResident();
    EXPECT_EQ(pool.PendingBytes(), 1 * MB);
    lease.reset();
    EXPECT_EQ(pool.PendingBytes(), 1 * MB);
    EXPECT_EQ(pool.UsedBytes(), 1 * MB);
}

// Reading the host figure means opening /proc/meminfo and the cgroup files; done once per request under the pool's one mutex it
// would serialize every small insert. The figure is reused for `host_cache` instead.
TEST(MemoryPoolTest, TheHostFigureIsReadAtMostOncePerCacheIntervalNotOncePerRequest) {
    std::atomic<int> reads{0};
    std::atomic<uint64_t> available{1000 * MB};
    MemoryPool pool(10000 * MB, 0, [&] { ++reads; return available.load(); }, 300ms);
    auto other_work = MustAcquire(pool, 1 * MB);   // something running, so the host figure is consulted
    reads = 0;
    std::vector<std::shared_ptr<MemoryLease>> leases;
    for (int i = 0; i < 100; ++i) leases.push_back(MustAcquire(pool, 1 * MB));
    EXPECT_LE(reads.load(), 2) << "the host figure was read for every request";
    available = 0;
    std::this_thread::sleep_for(400ms);
    std::shared_ptr<MemoryLease> blocked;
    EXPECT_EQ(pool.Acquire(1 * MB, Clock::now() + 150ms, blocked), MemoryPool::Result::kTimedOut) << "a changed figure was never picked up";
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

// ---- Memory that already exists and cannot wait: the bytes of a reply queued for a slow client (Charge / Uncharge) ----
// They count against the pool's limit like a lease, so a request that would not fit next to them waits, but taking them is never refused or delayed
// (the memory is there already); the pool is then Full() and whoever produces such memory is expected to stop producing.
MemoryPool UnlimitedHostPool(uint64_t limit) {
    return MemoryPool(limit, 0, [] { return MemoryPool::kUnknown; });
}

TEST(MemoryPoolChargeTest, ChargedBytesCountAsUsedAndUnchargingReturnsThem) {
    MemoryPool pool = UnlimitedHostPool(100 * MB);
    pool.Charge(30 * MB);
    EXPECT_EQ(pool.UsedBytes(), 30 * MB);
    auto lease = MustAcquire(pool, 20 * MB);
    EXPECT_EQ(pool.UsedBytes(), 50 * MB);
    pool.Uncharge(30 * MB);
    EXPECT_EQ(pool.UsedBytes(), 20 * MB);
    lease.reset();
    EXPECT_EQ(pool.UsedBytes(), 0u);
}

TEST(MemoryPoolChargeTest, ARequestThatDoesNotFitNextToChargedBytesWaitsUntilTheyAreReturned) {
    MemoryPool pool = UnlimitedHostPool(100 * MB);
    pool.Charge(80 * MB);
    auto waiting = std::async(std::launch::async, [&] {
        std::shared_ptr<MemoryLease> lease;
        const auto result = pool.Acquire(40 * MB, kForever, lease);
        return result == MemoryPool::Result::kGranted;
    });
    ASSERT_TRUE(WaitForWaiting(pool, 1));
    EXPECT_EQ(waiting.wait_for(100ms), std::future_status::timeout);
    pool.Uncharge(80 * MB);
    ASSERT_EQ(waiting.wait_for(2s), std::future_status::ready) << "returning the charged bytes did not wake the waiting request";
    EXPECT_TRUE(waiting.get());
}

// A waiting request re-checks every 50 ms by itself; returning charged bytes must wake it at once, not on that tick.
TEST(MemoryPoolChargeTest, UnchargeWakesAWaitingRequestAtOnceNotOnTheNextPoll) {
    std::vector<std::chrono::microseconds> latencies;
    for (int round = 0; round < 12; ++round) {
        MemoryPool pool = UnlimitedHostPool(100 * MB);
        pool.Charge(80 * MB);
        auto waiting = std::async(std::launch::async, [&] {
            std::shared_ptr<MemoryLease> lease;
            return pool.Acquire(40 * MB, kForever, lease) == MemoryPool::Result::kGranted;
        });
        ASSERT_TRUE(WaitForWaiting(pool, 1));
        std::this_thread::sleep_for(5ms);   // let it settle into its timed wait
        const auto start = Clock::now();
        pool.Uncharge(80 * MB);
        ASSERT_EQ(waiting.wait_for(2s), std::future_status::ready);
        latencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - start));
        EXPECT_TRUE(waiting.get());
    }
    std::sort(latencies.begin(), latencies.end());
    EXPECT_LT(latencies[latencies.size() / 2], 15ms) << "the median wake-up took as long as the poll tick (50 ms): Uncharge did not notify";
}

TEST(MemoryPoolChargeTest, ChargingPastTheLimitIsAllowedAndTheRequestsWaitUntilThereIsRoomAgain) {
    MemoryPool pool = UnlimitedHostPool(100 * MB);
    EXPECT_FALSE(pool.Full());
    pool.Charge(150 * MB);   // never refused: the memory exists
    EXPECT_TRUE(pool.Full());
    std::shared_ptr<MemoryLease> lease;
    EXPECT_EQ(pool.Acquire(1 * MB, Clock::now() + 100ms, lease), MemoryPool::Result::kTimedOut);
    pool.Uncharge(60 * MB);   // 90 MB of 100 MB
    EXPECT_FALSE(pool.Full());
    EXPECT_EQ(pool.Acquire(1 * MB, Clock::now() + 1s, lease), MemoryPool::Result::kGranted);
    pool.Uncharge(90 * MB);
}

TEST(MemoryPoolChargeTest, FullMeansTheLimitIsReachedNotMerelyApproached) {
    MemoryPool pool = UnlimitedHostPool(100 * MB);
    pool.Charge(99 * MB);
    EXPECT_FALSE(pool.Full());
    pool.Charge(1 * MB);
    EXPECT_TRUE(pool.Full());
    pool.Uncharge(100 * MB);
}

TEST(MemoryPoolChargeTest, ManyThreadsChargingAndUnchargingEndWithNothingCharged) {
    MemoryPool pool = UnlimitedHostPool(1000 * MB);
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < 20000; ++i) {
                pool.Charge(512 * 1024);
                pool.Uncharge(512 * 1024);
            }
        });
    }
    for (auto &t : threads) t.join();
    EXPECT_EQ(pool.UsedBytes(), 0u);
    EXPECT_EQ(pool.WaitingCount(), 0u);
}

// Full() is what makes a streamed reply park: besides the pool's own limit, it is true when the OS has less than the reserve left, so that slow
// readers' queues stop growing on real memory pressure (memory the pool does not count: the allocator's retention, caches, other processes).
TEST(MemoryPoolFullTest, FullWhenTheHostHasLessThanTheReserveAvailableEvenWithNothingCharged) {
    std::atomic<uint64_t> available{15 * MB};
    MemoryPool pool(1000 * MB, 20 * MB, [&] { return available.load(); });
    EXPECT_TRUE(pool.Full());
    available = 500 * MB;
    EXPECT_FALSE(pool.Full()) << "memory came back: no longer full";
}

TEST(MemoryPoolFullTest, TheReserveItselfAvailableIsNotFull) {
    MemoryPool pool(1000 * MB, 20 * MB, [] { return 20 * MB; });
    EXPECT_FALSE(pool.Full());
    MemoryPool below(1000 * MB, 20 * MB, [] { return 20 * MB - 1; });
    EXPECT_TRUE(below.Full());
}

TEST(MemoryPoolFullTest, AReservationNotYetAllocatedCountsAgainstTheHostFigureLikeInAcquire) {
    std::atomic<uint64_t> available{100 * MB};
    MemoryPool pool(1000 * MB, 20 * MB, [&] { return available.load(); });
    auto first = MustAcquire(pool, 1 * MB);    // nothing running before it: admitted without the host check
    first->MarkResident();
    auto lease = MustAcquire(pool, 45 * MB);   // fits: 100 MB seen by the OS >= 45 + the 20 MB reserve; the memory is not allocated yet
    EXPECT_FALSE(pool.Full());
    available = 60 * MB;                       // another process took memory meanwhile
    EXPECT_TRUE(pool.Full()) << "60 MB seen by the OS - 45 MB still to be allocated = 15 MB < the 20 MB reserve";
    lease->MarkResident();
    EXPECT_FALSE(pool.Full()) << "allocated: the OS figure reflects it, nothing is pending";
}

TEST(MemoryPoolFullTest, AnUnknownHostFigureNeverMakesItFull) {
    MemoryPool pool(1000 * MB, 20 * MB, [] { return MemoryPool::kUnknown; });
    EXPECT_FALSE(pool.Full());
}

TEST(MemoryPoolFullTest, ThePoolLimitStillMakesItFullWhateverTheHostHas) {
    MemoryPool pool(100 * MB, 20 * MB, [] { return 10000 * MB; });
    pool.Charge(100 * MB);
    EXPECT_TRUE(pool.Full());
    pool.Uncharge(100 * MB);
    EXPECT_FALSE(pool.Full());
}

// Full() is asked at every scan-chunk boundary of every reply that has bytes queued: the OS figure (/proc/meminfo, cgroup files) must not be read each time.
TEST(MemoryPoolFullTest, TheHostFigureIsReadAtMostOncePerCacheIntervalNotOncePerCall) {
    std::atomic<int> reads{0};
    MemoryPool pool(1000 * MB, 20 * MB, [&] { ++reads; return 500 * MB; }, std::chrono::hours(1));
    for (int i = 0; i < 1000; ++i) EXPECT_FALSE(pool.Full());
    EXPECT_EQ(reads.load(), 1);
}

// The reserve left to the host follows the size of the machine: an eighth of its memory, between 128 MiB and 2 GiB, so a 2 GB or 4 GB device keeps a pool.
TEST(MemoryPoolSizingTest, TheDefaultReserveIsAnEighthOfTheMemoryBetween128MBAnd2GB) {
    EXPECT_EQ(xstore::DefaultReserveBytes(64 * GB), 2 * GB);
    EXPECT_EQ(xstore::DefaultReserveBytes(16 * GB), 2 * GB);
    EXPECT_EQ(xstore::DefaultReserveBytes(8 * GB), 1 * GB);
    EXPECT_EQ(xstore::DefaultReserveBytes(4 * GB), 512 * MB);
    EXPECT_EQ(xstore::DefaultReserveBytes(2 * GB), 256 * MB);
    EXPECT_EQ(xstore::DefaultReserveBytes(1 * GB), 128 * MB);
    EXPECT_EQ(xstore::DefaultReserveBytes(256 * MB), 128 * MB);
}

TEST(MemoryPoolSizingTest, ThePoolIsTheMemoryOfTheMachineMinusTheReserve) {
    const xstore::PoolSizing big = xstore::SizePool(64 * GB, 0, 0);
    EXPECT_EQ(big.reserve, 2 * GB);
    EXPECT_EQ(big.limit, 62 * GB);
    const xstore::PoolSizing pi4 = xstore::SizePool(4 * GB, 0, 0);
    EXPECT_EQ(pi4.reserve, 512 * MB);
    EXPECT_EQ(pi4.limit, 4 * GB - 512 * MB);
    const xstore::PoolSizing pi2 = xstore::SizePool(2 * GB, 0, 0);
    EXPECT_EQ(pi2.reserve, 256 * MB) << "a 2 GB device used to get a pool of ALL its memory and a reserve it could never have";
    EXPECT_EQ(pi2.limit, 2 * GB - 256 * MB);
}

TEST(MemoryPoolSizingTest, TheEnvironmentOverridesWinAndAreIndependent) {
    const xstore::PoolSizing reserve_only = xstore::SizePool(4 * GB, 100, 0);
    EXPECT_EQ(reserve_only.reserve, 100 * MB);
    EXPECT_EQ(reserve_only.limit, 4 * GB - 100 * MB);
    const xstore::PoolSizing limit_only = xstore::SizePool(60 * GB, 0, 8192);
    EXPECT_EQ(limit_only.limit, 8 * GB);
    EXPECT_EQ(limit_only.reserve, 2 * GB);
}

TEST(MemoryPoolSizingTest, AnUnreadableMachineSizeFallsBackToASmallPool) {
    const xstore::PoolSizing unknown = xstore::SizePool(0, 0, 0);
    EXPECT_EQ(unknown.reserve, 2 * GB);
    EXPECT_EQ(unknown.limit, 2 * GB);
}

TEST(MemoryPoolSizingTest, AnUnknownMachineSizeGetsTheLargestReserve) {
    EXPECT_EQ(xstore::DefaultReserveBytes(0), 2 * GB);
}

TEST(MemoryPoolSizingTest, ASmallDeviceStillHasAPoolOfItsOwn) {
    const uint64_t total = 2 * GB;
    EXPECT_GT(total, xstore::DefaultReserveBytes(total) * 4) << "the reserve must not eat the machine";
}

TEST(MemoryPoolTest, TheProcessWidePoolIsSizedFromTheMachineAndLeavesTheReserve) {
    MemoryPool &pool = MemoryPool::Instance();
    EXPECT_GT(pool.Limit(), 0u);
    EXPECT_GT(pool.Reserve(), 0u);
    EXPECT_LE(pool.Reserve(), 4096 * MB);
}

}  // namespace
