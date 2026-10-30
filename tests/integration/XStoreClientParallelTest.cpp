// The load-aware job runner (part 1 of XStore_Client.h) every parallel part of the client is built on. Machine load is faked through the
// policy's runnable-thread source, so every case is deterministic about WHAT the controller decides; only timing is real.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

#include "XStore_Client.h"

using namespace std::chrono_literals;
using xstore_client::NextWorkerTarget;
using xstore_client::ParallelismPolicy;
using xstore_client::RunJobs;

namespace {
ParallelismPolicy Policy(size_t fixed, size_t cores, std::function<std::optional<size_t>()> runnable) {
    ParallelismPolicy policy;
    policy.fixed_threads = fixed;
    policy.max_threads = cores;
    policy.cores = cores;
    policy.headroom = 1;
    policy.tick = 1ms;
    policy.runnable = std::move(runnable);
    return policy;
}
const auto kIdle = [] { return std::optional<size_t>(1); };      // just the controller itself is running
const auto kBusy = [] { return std::optional<size_t>(1000); };   // the machine is far over-subscribed

struct Seen {
    std::mutex mutex;
    std::set<std::thread::id> threads;
    void Note() {
        std::lock_guard<std::mutex> lock(mutex);
        threads.insert(std::this_thread::get_id());
    }
};
}  // namespace

TEST(LoadAvgTest, ReadsTheRunnableThreadsFromTheFourthField) {
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg("0.20 0.18 0.15 3/1234 5678\n"), 3u);
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg("12.00 9.50 4.00 27/8890 111"), 27u);
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg("0.20 0.18 0.15 /1234 5678"), std::nullopt);
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg("0.20 0.18 0.15 31234 5678"), std::nullopt);
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg("0.20 0.18"), std::nullopt);
    EXPECT_EQ(xstore_client::ParseRunnableFromLoadAvg(""), std::nullopt);
}

TEST(LoadAvgTest, TheLiveReadingIsPlausible) {
#if defined(__linux__)
    const std::optional<size_t> runnable = xstore_client::ReadRunnableThreads();
    ASSERT_TRUE(runnable.has_value());
    EXPECT_GE(*runnable, 1u);   // this thread
#else
    GTEST_SKIP() << "no /proc/loadavg";
#endif
}

// NextWorkerTarget(target, runnable, cores, headroom, cap, jobs_left)
TEST(NextWorkerTargetTest, DoublesWhileThereIsRoomAndStopsAtTheLimits) {
    EXPECT_EQ(NextWorkerTarget(1, 1, 32, 1, 32, 1000), 2u);     // slow start: the team doubles...
    EXPECT_EQ(NextWorkerTarget(2, 3, 32, 1, 32, 1000), 4u);
    EXPECT_EQ(NextWorkerTarget(8, 9, 32, 1, 32, 1000), 16u);
    EXPECT_EQ(NextWorkerTarget(16, 17, 32, 1, 32, 1000), 30u);  // ...but never past the free cores: 32 - 17 running - 1 headroom = 14
    EXPECT_EQ(NextWorkerTarget(2, 3, 32, 1, 3, 1000), 3u);      // the cap
    EXPECT_EQ(NextWorkerTarget(4, 5, 32, 1, 32, 5), 5u);        // no more workers than jobs left
    EXPECT_EQ(NextWorkerTarget(4, 5, 32, 1, 32, 2), 4u);        // ...and no growth when the jobs left are already covered
}

TEST(NextWorkerTargetTest, HoldsInsideTheBandAndLeavesWhenOverSubscribed) {
    EXPECT_EQ(NextWorkerTarget(8, 31, 32, 1, 32, 1000), 8u);    // 32 - 31 - 1 = 0 free: hold
    EXPECT_EQ(NextWorkerTarget(8, 32, 32, 1, 32, 1000), 8u);    // exactly the cores: still inside the band
    EXPECT_EQ(NextWorkerTarget(8, 35, 32, 1, 32, 1000), 5u);    // 3 over: 3 workers leave
    EXPECT_EQ(NextWorkerTarget(2, 100, 32, 1, 32, 1000), 1u);   // never below the caller
    EXPECT_EQ(NextWorkerTarget(1, 100, 32, 1, 32, 1000), 1u);
}

TEST(RunJobsTest, RunsEveryJobExactlyOnceUnderEveryPolicy) {
    const std::vector<ParallelismPolicy> policies = {
        Policy(1, 8, kIdle), Policy(2, 8, kIdle), Policy(7, 8, kIdle), Policy(64, 8, kIdle),
        Policy(0, 8, kIdle), Policy(0, 8, kBusy), Policy(0, 1, kIdle)};
    for (const ParallelismPolicy &policy : policies) {
        std::vector<std::atomic<int>> runs(500);
        RunJobs(runs.size(), policy, [&](size_t job) { runs[job].fetch_add(1); });
        for (size_t job = 0; job < runs.size(); ++job) ASSERT_EQ(runs[job].load(), 1) << "job " << job << " fixed " << policy.fixed_threads;
    }
}

TEST(RunJobsTest, NoJobsRunsNothing) {
    int calls = 0;
    RunJobs(0, Policy(0, 8, kIdle), [&](size_t) { ++calls; });
    EXPECT_EQ(calls, 0);
}

TEST(RunJobsTest, AFixedTeamHasExactlyThatManyThreads) {
    Seen seen;
    RunJobs(64, Policy(4, 8, kBusy), [&](size_t) { seen.Note(); std::this_thread::sleep_for(5ms); });
    EXPECT_EQ(seen.threads.size(), 4u);   // fixed ignores the load
}

TEST(RunJobsTest, AnIdleMachineGrowsTheTeam) {
    Seen seen;
    RunJobs(200, Policy(0, 8, kIdle), [&](size_t) { seen.Note(); std::this_thread::sleep_for(3ms); });
    EXPECT_GE(seen.threads.size(), 6u);
    EXPECT_LE(seen.threads.size(), 8u);   // the cap
}

TEST(RunJobsTest, ABusyMachineLeavesTheCallerAlone) {
    Seen seen;
    RunJobs(60, Policy(0, 8, kBusy), [&](size_t) { seen.Note(); std::this_thread::sleep_for(2ms); });
    EXPECT_EQ(seen.threads.size(), 1u);
}

TEST(RunJobsTest, TheHeadroomIsLeftFree) {
    Seen seen;
    // 7 runnable on 8 cores with a headroom of 1: nothing is free
    RunJobs(60, Policy(0, 8, [] { return std::optional<size_t>(7); }), [&](size_t) { seen.Note(); std::this_thread::sleep_for(2ms); });
    EXPECT_EQ(seen.threads.size(), 1u);
}

TEST(RunJobsTest, HelpersLeaveWhenTheMachineBecomesBusy) {
    constexpr size_t kJobs = 200, kBusyAfter = 30, kCheckedFrom = 120;
    std::atomic<bool> busy{false};
    std::atomic<size_t> helper_jobs_late{0};
    const auto caller = std::this_thread::get_id();
    RunJobs(kJobs, Policy(0, 8, [&] { return std::optional<size_t>(busy.load() ? 1000 : 1); }), [&](size_t job) {
        if (job == kBusyAfter) busy = true;
        if (job >= kCheckedFrom && std::this_thread::get_id() != caller) helper_jobs_late.fetch_add(1);
        std::this_thread::sleep_for(2ms);
    });
    EXPECT_EQ(helper_jobs_late.load(), 0u);
}

TEST(RunJobsTest, HelpersComeBackWhenTheMachineQuietsDown) {
    constexpr size_t kJobs = 400;
    std::atomic<int> phase{0};   // 0 idle, 1 busy, 2 idle again
    Seen late;
    RunJobs(kJobs, Policy(0, 8, [&] { return std::optional<size_t>(phase.load() == 1 ? 1000 : 1); }), [&](size_t job) {
        if (job == 40) phase = 1;
        if (job == 140) phase = 2;
        if (job >= 300) late.Note();
        std::this_thread::sleep_for(2ms);
    });
    EXPECT_GE(late.threads.size(), 3u);
}

TEST(RunJobsTest, AnUnknownLoadFallsBackToAModestFixedTeam) {
    Seen seen;
    RunJobs(64, Policy(0, 8, [] { return std::optional<size_t>(); }), [&](size_t) { seen.Note(); std::this_thread::sleep_for(3ms); });
    EXPECT_EQ(seen.threads.size(), 4u);   // cores / 2
}

TEST(RunJobsTest, ReturnsAtOnceEvenWithAHugeTick) {
    ParallelismPolicy policy = Policy(0, 8, kIdle);
    policy.tick = std::chrono::hours(1);
    const auto start = std::chrono::steady_clock::now();
    RunJobs(20, policy, [](size_t) { std::this_thread::sleep_for(5ms); });   // long enough for the controller to be waiting
    EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
}

TEST(RunJobsTest, ABodyExceptionStopsTheTeamAndIsRethrown) {
    for (size_t fixed : {size_t{1}, size_t{4}, size_t{0}}) {
        std::atomic<size_t> after{0};
        EXPECT_THROW(RunJobs(10000, Policy(fixed, 8, kIdle), [&](size_t job) {
                         if (job == 57) throw std::runtime_error("boom");
                         if (job > 57) after.fetch_add(1);
                         std::this_thread::sleep_for(100us);
                     }),
                     std::runtime_error);
        EXPECT_LT(after.load(), 9000u) << "fixed " << fixed;   // the team stopped taking jobs
    }
}
