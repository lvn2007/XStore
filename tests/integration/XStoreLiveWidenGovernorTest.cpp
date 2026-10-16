// LiveWidenGovernor (XStore_LiveWidenGovernor.h): the policy on a fake clock and a fake load source. No files, no
// sleeps. The integration into LiveWidenOneYear is tested in XStoreLiveWidenTest.cpp (S3b-2 G3).
#include "XStore/XStore_LiveWidenGovernor.h"

#include "XStore/XStore_Tunables.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

using livewiden::DefaultMaxWorkers;
using livewiden::GovernorParams;
using livewiden::GovernorReason;
using livewiden::LiveWidenGovernor;
using livewiden::Signals;
using Clock = LiveWidenGovernor::Clock;

namespace {

// One second of fake time per Tick(); every load figure is CPUs busy during that second.
struct FakeEnv {
    std::atomic<int64_t> now_ns{1'000'000'000};
    std::mutex mtx;
    Signals sig;
    std::atomic<bool> fail_sample{false};
    std::atomic<int> sample_calls{0};

    Clock::time_point Now() const {
        return Clock::time_point(std::chrono::duration_cast<Clock::duration>(std::chrono::nanoseconds(now_ns.load())));
    }
    Signals Sample() {
        ++sample_calls;
        if (fail_sample)
            throw std::runtime_error("fake sampler failure");
        std::lock_guard<std::mutex> lk(mtx);
        return sig;
    }
    void Set(const Signals &s) {
        std::lock_guard<std::mutex> lk(mtx);
        sig = s;
    }
};

GovernorParams Params(unsigned cores, unsigned floor_workers = 1) {
    GovernorParams p;
    p.cores = cores;
    p.floor_workers = floor_workers;
    p.headroom_cores = 2;
    p.max_workers = DefaultMaxWorkers(cores, p.headroom_cores, 16, floor_workers); // fixed cap: the tests do not follow the tunable
    p.tick = std::chrono::seconds(1);
    p.brake_hold = std::chrono::seconds(10);
    p.starvation_guard = std::chrono::seconds(600);
    p.at_floor_warn = std::chrono::seconds(60);
    p.wait_brake_us = 100000;
    p.long_waits_brake = 1;
    p.smoothing_ticks = 3;
    p.label = "test";
    return p;
}

struct Rig {
    FakeEnv env;
    LiveWidenGovernor gov;

    explicit Rig(GovernorParams p, std::function<double()> progress = {})
        : gov(std::move(p), [this] { return env.Sample(); }, [this] { return env.Now(); }, std::move(progress)) {
        EXPECT_TRUE(gov.Permit(0) || gov.GetSnapshot().floor_workers == 0); // baseline
    }

    // Advances `secs` seconds with `fg` foreground CPUs and the builder burning `builder` CPUs (reported through
    // AddBuilderCpu, as the workers will), plus writer-counter increments; then evaluates. Returns allowed.
    unsigned Tick(double fg, double builder = 0, uint64_t d_wait_us = 0, uint64_t d_long = 0, int64_t secs = 1) {
        Signals s = Peek();
        s.busy_cpu_s += (fg + builder) * static_cast<double>(secs);
        s.writer_wait_us += d_wait_us;
        s.writer_waits += d_wait_us ? 1 : 0;
        s.long_waits += d_long;
        env.Set(s);
        gov.AddBuilderCpu(static_cast<uint64_t>(builder * 1e9 * static_cast<double>(secs)));
        env.now_ns += secs * 1'000'000'000;
        (void)gov.Permit(0);
        return gov.GetSnapshot().allowed;
    }
    Signals Peek() {
        std::lock_guard<std::mutex> lk(env.mtx);
        return env.sig;
    }
};

} // namespace

TEST(LiveWidenGovernorTest, MaxWorkersArithmetic) {
    EXPECT_EQ(DefaultMaxWorkers(32, 2, 16, 1), 16u);
    // The shipped default scales with the machine: cores / LIVE_WIDEN_CPUS_PER_WORKER (Mystic, 32 vCPU: 8).
    static_assert(XStoreTunables::LIVE_WIDEN_CPUS_PER_WORKER == 4, "update the expectations below");
    GovernorParams d = Params(32); d.max_workers = 0;
    EXPECT_EQ(Rig(d).gov.MaxWorkers(), 8u);
    d = Params(16); d.max_workers = 0;
    EXPECT_EQ(Rig(d).gov.MaxWorkers(), 4u);
    d = Params(64); d.max_workers = 0;
    EXPECT_EQ(Rig(d).gov.MaxWorkers(), 16u);
    d = Params(4); d.max_workers = 0;
    EXPECT_EQ(Rig(d).gov.MaxWorkers(), 1u);
    EXPECT_EQ(DefaultMaxWorkers(4, 2, 16, 1), 2u);
    EXPECT_EQ(DefaultMaxWorkers(2, 2, 16, 1), 1u);
    EXPECT_EQ(DefaultMaxWorkers(1, 2, 16, 1), 1u);
    EXPECT_EQ(DefaultMaxWorkers(1, 2, 16, 0), 0u);
    EXPECT_EQ(DefaultMaxWorkers(10, 2, 4, 1), 4u);
    EXPECT_EQ(Rig(Params(32)).gov.MaxWorkers(), 16u);
    EXPECT_EQ(Rig(Params(4)).gov.MaxWorkers(), 2u);
    EXPECT_EQ(Rig(Params(2)).gov.MaxWorkers(), 1u);
    GovernorParams below = Params(32, 3);
    below.max_workers = 2;
    EXPECT_EQ(Rig(below).gov.MaxWorkers(), 3u);
}

TEST(LiveWidenGovernorTest, StartsAtTheFloorAndTheBaselineChangesNothing) {
    Rig rig(Params(32));
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.allowed, 1u);
    EXPECT_EQ(snap.max_workers, 16u);
    EXPECT_TRUE(snap.throttled);
    EXPECT_EQ(snap.reason, GovernorReason::kStart);
    EXPECT_EQ(snap.evaluations, 0u);
    EXPECT_FALSE(rig.gov.Permit(1));
    rig.Tick(0);
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kRampUp);
}

TEST(LiveWidenGovernorTest, ZeroOrNegativeWallTimeSkipsTheEvaluation) {
    GovernorParams p = Params(32);
    p.tick = std::chrono::seconds(0);   // due on every Permit
    Rig rig(p);
    Signals s;
    s.busy_cpu_s = 1e6;                 // would be a huge foreground load over any positive interval
    s.writer_wait_us = 1'000'000'000;
    rig.env.Set(s);
    for (int i = 0; i < 10; ++i)
        (void)rig.gov.Permit(0);
    rig.env.now_ns -= 500'000'000;      // clock goes backwards
    for (int i = 0; i < 10; ++i)
        (void)rig.gov.Permit(0);
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.allowed, 1u);
    EXPECT_EQ(snap.evaluations, 0u);
    EXPECT_FALSE(snap.brake_active);
}

TEST(LiveWidenGovernorTest, RampsOnePerTickWhenIdleNeverAboveMax) {
    Rig rig(Params(32));
    for (unsigned want = 2; want <= 16; ++want) {
        EXPECT_EQ(rig.Tick(0), want);
        const auto snap = rig.gov.GetSnapshot();
        EXPECT_EQ(snap.throttled, want < 16);
        EXPECT_EQ(snap.reason, want < 16 ? GovernorReason::kRampUp : GovernorReason::kNone);
        EXPECT_FALSE(rig.gov.Permit(want));
        EXPECT_TRUE(rig.gov.Permit(want - 1));
    }
    for (int i = 0; i < 20; ++i)
        EXPECT_EQ(rig.Tick(0), 16u);
    EXPECT_EQ(rig.gov.GetSnapshot().evaluations, 35u);
    EXPECT_EQ(rig.gov.GetSnapshot().parked_seconds, 0.0);
}

TEST(LiveWidenGovernorTest, DropsAtOnceAndRaisesOnlyAsTheWindowQuiets) {
    Rig rig(Params(16));                // max 14
    for (int i = 0; i < 20; ++i)
        rig.Tick(0);
    ASSERT_EQ(rig.gov.GetSnapshot().allowed, 14u);

    EXPECT_EQ(rig.Tick(12), 2u);        // floor(16 - 12 - 2), in one evaluation
    auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.reason, GovernorReason::kForegroundCpu);
    EXPECT_TRUE(snap.throttled);
    EXPECT_NEAR(snap.fg_cores, 12.0, 1e-9);

    // The window (3 ticks) still holds the busy tick: target floor(16 - 12/3 - 2) = 10, raised +1 per tick.
    EXPECT_EQ(rig.Tick(0), 3u);
    EXPECT_EQ(rig.gov.GetSnapshot().cpu_target, 10u);
    EXPECT_EQ(rig.Tick(0), 4u);
    EXPECT_EQ(rig.gov.GetSnapshot().cpu_target, 10u);
    EXPECT_EQ(rig.Tick(0), 5u);
    EXPECT_EQ(rig.gov.GetSnapshot().cpu_target, 14u);   // window quiet

    // A steady foreground of 6 CPUs: target 8, never exceeded.
    for (int i = 0; i < 20; ++i)
        EXPECT_LE(rig.Tick(6), 8u);
    EXPECT_EQ(rig.gov.GetSnapshot().allowed, 8u);
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kForegroundCpu);
}

TEST(LiveWidenGovernorTest, BuildersOwnCpuIsExcluded) {
    Rig rig(Params(16));
    // The builder keeps every allowed worker busy, no foreground: busy rises with allowed, the target does not fall.
    for (int i = 0; i < 30; ++i)
        rig.Tick(0, rig.gov.GetSnapshot().allowed);
    EXPECT_EQ(rig.gov.GetSnapshot().allowed, 14u);
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kNone);

    // Foreground 6 on top of the 14 builder CPUs lowers it to floor(16 - 6 - 2).
    EXPECT_EQ(rig.Tick(6, 14), 8u);

    // A sampler that reports part of the builder's CPU itself: the two are added.
    Rig split(Params(16));
    for (int i = 0; i < 30; ++i) {
        Signals s = split.Peek();
        s.builder_cpu_s += 7;           // 14 builder CPUs: 7 seen by the sampler, 7 through AddBuilderCpu in Tick
        s.busy_cpu_s += 7;
        split.env.Set(s);
        split.Tick(0, 7);
    }
    EXPECT_EQ(split.gov.GetSnapshot().allowed, 14u);   // counting only one half would give foreground 7, target 7
}

TEST(LiveWidenGovernorTest, WriterBrakeByWaitTime) {
    Rig rig(Params(16));
    for (int i = 0; i < 20; ++i)
        rig.Tick(0);
    EXPECT_EQ(rig.Tick(0, 0, 99'999), 14u);             // under the threshold
    EXPECT_EQ(rig.Tick(0, 0, 100'000), 1u);             // to the floor in ONE evaluation
    auto snap = rig.gov.GetSnapshot();
    EXPECT_TRUE(snap.brake_active);
    EXPECT_TRUE(snap.throttled);
    EXPECT_EQ(snap.reason, GovernorReason::kWriterWait);
    EXPECT_NE(snap.reason_text.find("100 ms per second"), std::string::npos) << snap.reason_text;
    for (int i = 1; i < 10; ++i)
        EXPECT_EQ(rig.Tick(0), 1u) << "raised inside the hold, tick " << i;
    EXPECT_EQ(rig.Tick(0), 2u);                         // hold (10 s) over
    EXPECT_FALSE(rig.gov.GetSnapshot().brake_active);
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kRampUp);
    EXPECT_EQ(rig.Tick(0), 3u);

    // A second trigger inside a hold restarts it.
    EXPECT_EQ(rig.Tick(0, 0, 500'000), 1u);
    for (int i = 1; i < 5; ++i)
        EXPECT_EQ(rig.Tick(0), 1u);
    EXPECT_EQ(rig.Tick(0, 0, 500'000), 1u);
    for (int i = 1; i < 10; ++i)
        EXPECT_EQ(rig.Tick(0), 1u) << "hold not restarted, tick " << i;
    EXPECT_EQ(rig.Tick(0), 2u);
}

TEST(LiveWidenGovernorTest, WriterBrakeByLongWaits) {
    Rig rig(Params(16));
    for (int i = 0; i < 20; ++i)
        rig.Tick(0);
    EXPECT_EQ(rig.Tick(0, 0, 0, 1), 1u);
    auto snap = rig.gov.GetSnapshot();
    EXPECT_TRUE(snap.brake_active);
    EXPECT_EQ(snap.reason, GovernorReason::kLongWaits);
    for (int i = 1; i < 10; ++i)
        EXPECT_EQ(rig.Tick(0), 1u);
    EXPECT_EQ(rig.Tick(0), 2u);
    EXPECT_EQ(rig.Tick(0, 0, 0, 3), 1u);                // retrigger
    for (int i = 1; i < 10; ++i)
        EXPECT_EQ(rig.Tick(0), 1u);
    EXPECT_EQ(rig.Tick(0), 2u);
}

TEST(LiveWidenGovernorTest, CountersOutOfOrderOrResetCauseNoUnderflow) {
    Rig rig(Params(16));
    for (int i = 0; i < 20; ++i)
        rig.Tick(0);
    // long_waits ahead of waits, total_us ahead of both: each used on its own delta.
    Signals s = rig.Peek();
    s.long_waits += 5;
    s.writer_wait_us += 10'000'000;
    rig.env.Set(s);
    EXPECT_EQ(rig.Tick(0), 1u);
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kWriterWait);
    for (int i = 0; i < 12; ++i)
        rig.Tick(0);
    s = rig.Peek();
    s.writer_wait_us = 50;              // went backwards: delta 0, no brake
    s.writer_waits = 0;
    s.long_waits = 0;
    s.busy_cpu_s -= 100;                // busy backwards and builder ahead of busy: foreground clamps to 0
    rig.env.Set(s);
    const unsigned before = rig.gov.GetSnapshot().allowed;
    EXPECT_EQ(rig.Tick(0, 50), before + 1);
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_FALSE(snap.brake_active);
    EXPECT_TRUE(std::isfinite(snap.fg_cores));
    EXPECT_GE(snap.fg_cores, 0.0);
    EXPECT_GE(snap.allowed, 1u);
    EXPECT_LE(snap.allowed, 14u);
}

TEST(LiveWidenGovernorTest, FloorZeroStarvationGuardGrantsOneGroupPerGuardPeriod) {
    Rig rig(Params(16, 0));
    EXPECT_EQ(rig.gov.GetSnapshot().allowed, 0u);
    for (int round = 0; round < 2; ++round) {
        int grants = 0;
        int granted_at = -1;
        for (int t = 1; t <= 600; ++t) {
            Signals s = rig.Peek();
            s.busy_cpu_s += 16;         // every CPU busy with foreground: target 0
            rig.env.Set(s);
            rig.env.now_ns += 1'000'000'000;
            EXPECT_FALSE(rig.gov.Permit(1));
            if (rig.gov.Permit(0)) {
                ++grants;
                granted_at = t;
            }
        }
        EXPECT_EQ(grants, 1) << "round " << round;
        EXPECT_EQ(granted_at, 600) << "round " << round;
        EXPECT_EQ(rig.gov.GetSnapshot().allowed, 0u);
    }
    EXPECT_FALSE(rig.gov.Permit(0));                    // the grant was one-shot
    EXPECT_EQ(rig.gov.GetSnapshot().active, 0u);        // the granted group ended at the next denied Permit
}

TEST(LiveWidenGovernorTest, FloorOneNeverUsesTheStarvationGuard) {
    Rig rig(Params(16, 1));
    for (int t = 1; t <= 1300; ++t) {
        Signals s = rig.Peek();
        s.busy_cpu_s += 16;
        rig.env.Set(s);
        rig.env.now_ns += 1'000'000'000;
        EXPECT_TRUE(rig.gov.Permit(0));
        EXPECT_FALSE(rig.gov.Permit(1));
    }
    EXPECT_EQ(rig.gov.GetSnapshot().allowed, 1u);
    EXPECT_EQ(rig.gov.GetSnapshot().peak_active, 1u);
}

TEST(LiveWidenGovernorTest, AtFloorWarningOncePerEpisode) {
    double done = 0.25;
    Rig rig(Params(16), [&done] { return done; });
    for (int t = 1; t < 60; ++t)
        rig.Tick(15);
    auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.warnings_emitted, 0u);
    EXPECT_DOUBLE_EQ(snap.parked_seconds, 59.0);
    EXPECT_EQ(snap.allowed, 1u);
    rig.Tick(15);
    EXPECT_EQ(rig.gov.GetSnapshot().warnings_emitted, 1u);
    for (int t = 0; t < 200; ++t)
        rig.Tick(15);
    snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.warnings_emitted, 1u);
    EXPECT_DOUBLE_EQ(snap.parked_seconds, 260.0);
    EXPECT_EQ(snap.reason, GovernorReason::kForegroundCpu);

    for (int t = 0; t < 5; ++t)
        rig.Tick(0);
    snap = rig.gov.GetSnapshot();
    EXPECT_GT(snap.allowed, 1u);        // episode over
    EXPECT_EQ(snap.parked_seconds, 0.0);

    done = 0.5;
    for (int t = 0; t < 70; ++t)
        rig.Tick(15);
    EXPECT_EQ(rig.gov.GetSnapshot().warnings_emitted, 2u);
}

TEST(LiveWidenGovernorTest, NoWarningWhenMaxEqualsFloor) {
    Rig rig(Params(3));                 // max = 1 = floor
    ASSERT_EQ(rig.gov.MaxWorkers(), 1u);
    for (int t = 0; t < 300; ++t)
        rig.Tick(3, 0, 1'000'000, 2);
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.warnings_emitted, 0u);
    EXPECT_EQ(snap.parked_seconds, 0.0);
    EXPECT_FALSE(snap.throttled);
    EXPECT_EQ(snap.allowed, 1u);
}

TEST(LiveWidenGovernorTest, RebaselineOnYearFileSwitchSmallToBig) {
    for (const bool rebaseline : {true, false}) {
        Rig rig(Params(16));
        for (int i = 0; i < 20; ++i)
            rig.Tick(0, 0, 1000);                       // the first file: small counters, no brake
        ASSERT_EQ(rig.gov.GetSnapshot().allowed, 14u);
        Signals s = rig.Peek();                         // the next year file has seen far more waits
        s.writer_wait_us = 5'000'000'000;
        s.writer_waits = 90'000;
        s.long_waits = 300;
        rig.env.Set(s);
        if (rebaseline)
            rig.gov.RebaselineWriterCounters();
        const unsigned allowed = rig.Tick(0);
        if (!rebaseline) {
            EXPECT_EQ(allowed, 1u) << "without a rebaseline the switch is a false brake";
            continue;
        }
        EXPECT_EQ(allowed, 14u);
        EXPECT_FALSE(rig.gov.GetSnapshot().brake_active);
        EXPECT_EQ(rig.Tick(0, 0, 50'000), 14u);         // the new file's own deltas count from there
        EXPECT_EQ(rig.Tick(0, 0, 200'000), 1u);
    }
}

TEST(LiveWidenGovernorTest, RebaselineOnYearFileSwitchBigToSmall) {
    for (const bool rebaseline : {true, false}) {
        Rig rig(Params(16));
        Signals s = rig.Peek();
        s.writer_wait_us = 5'000'000'000;
        s.long_waits = 300;
        rig.env.Set(s);
        rig.gov.RebaselineWriterCounters();
        for (int i = 0; i < 20; ++i)
            rig.Tick(0);
        ASSERT_EQ(rig.gov.GetSnapshot().allowed, 14u);
        s = rig.Peek();                                 // the next year file: small counters
        s.writer_wait_us = 10;
        s.writer_waits = 1;
        s.long_waits = 0;
        rig.env.Set(s);
        if (rebaseline)
            rig.gov.RebaselineWriterCounters();
        EXPECT_EQ(rig.Tick(0), 14u);                    // never a brake at the switch (backwards = delta 0)
        // A real wait on the new file brakes either way: the switch tick already took the small values as prev.
        EXPECT_EQ(rig.Tick(0, 0, 200'000, 1), 1u);
    }
}

TEST(LiveWidenGovernorTest, RebaselineBeforeAnyEvaluationIsHarmless) {
    FakeEnv env;
    LiveWidenGovernor gov(Params(16), [&env] { return env.Sample(); }, [&env] { return env.Now(); });
    gov.RebaselineWriterCounters();
    gov.RebaselineWriterCounters();
    (void)gov.Permit(0);                                // baseline consumes it
    env.now_ns += 1'000'000'000;
    (void)gov.Permit(0);
    EXPECT_EQ(gov.GetSnapshot().allowed, 2u);
    Signals s = env.sig;
    s.writer_wait_us += 200'000;                        // the first real delta is not swallowed
    env.Set(s);
    env.now_ns += 1'000'000'000;
    (void)gov.Permit(0);
    EXPECT_EQ(gov.GetSnapshot().allowed, 1u);
    EXPECT_TRUE(gov.GetSnapshot().brake_active);
}

TEST(LiveWidenGovernorTest, WaitBrakeIsPerSecondOfTheInterval) {
    Rig rig(Params(16));
    for (int i = 0; i < 20; ++i)
        rig.Tick(0);
    EXPECT_EQ(rig.Tick(0, 0, 99'999, 0, 10), 14u);      // 10 ms/s
    EXPECT_EQ(rig.Tick(0, 0, 999'999, 0, 10), 14u);     // just under 100 ms/s
    EXPECT_EQ(rig.Tick(0, 0, 1'000'000, 0, 10), 1u);    // 100 ms/s
    EXPECT_EQ(rig.gov.GetSnapshot().reason, GovernorReason::kWriterWait);
    for (int i = 0; i < 30; ++i)
        rig.Tick(0);
    ASSERT_EQ(rig.gov.GetSnapshot().allowed, 14u);
    EXPECT_EQ(rig.Tick(0, 0, 0, 1, 10), 1u);            // a long wait is a count, whatever the interval
}

TEST(LiveWidenGovernorTest, ThrowingSamplerSkipsTheTick) {
    Rig rig(Params(16));
    for (int i = 0; i < 4; ++i)
        rig.Tick(0);
    ASSERT_EQ(rig.gov.GetSnapshot().allowed, 5u);
    const uint64_t evals = rig.gov.GetSnapshot().evaluations;

    rig.env.fail_sample = true;
    Signals s = rig.Peek();
    s.busy_cpu_s += 6;
    rig.env.Set(s);
    rig.env.now_ns += 1'000'000'000;
    const int calls = rig.env.sample_calls;
    EXPECT_TRUE(rig.gov.Permit(4));                     // the previous decision stands
    EXPECT_FALSE(rig.gov.Permit(5));
    EXPECT_EQ(rig.env.sample_calls, calls + 1);         // tried once: the tick is consumed, not retried per call
    EXPECT_EQ(rig.gov.GetSnapshot().evaluations, evals);
    EXPECT_EQ(rig.gov.GetSnapshot().allowed, 5u);

    rig.env.fail_sample = false;
    rig.Tick(6);                                        // 12 CPU-seconds over the 2 s since the last good sample
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.evaluations, evals + 1);
    EXPECT_NEAR(snap.fg_cores, 6.0, 1e-9);
    EXPECT_EQ(snap.allowed, 6u);                        // target floor(16 - 6 - 2) = 8: one step up
}

TEST(LiveWidenGovernorTest, ThrowingClockOrProgressNeverEscapesPermit) {
    std::atomic<bool> fail{true};
    FakeEnv env;
    LiveWidenGovernor gov(Params(16), [&env] { return env.Sample(); },
                          [&] {
                              if (fail)
                                  throw std::runtime_error("fake clock failure");
                              return env.Now();
                          });
    for (int i = 0; i < 10; ++i)
        EXPECT_TRUE(gov.Permit(0));
    EXPECT_FALSE(gov.Permit(1));
    fail = false;
    (void)gov.Permit(0);
    env.now_ns += 1'000'000'000;
    (void)gov.Permit(0);
    EXPECT_EQ(gov.GetSnapshot().allowed, 2u);

    Rig rig(Params(16), []() -> double { throw std::runtime_error("fake progress failure"); });
    for (int t = 0; t < 120; ++t)
        EXPECT_EQ(rig.Tick(15), 1u);
    EXPECT_TRUE(rig.gov.Permit(0));
}

TEST(LiveWidenGovernorTest, ConcurrentPermitReleaseSmoke) {
    constexpr unsigned THREADS = 10;                    // indices 8 and 9 are beyond max and must never be granted
    FakeEnv env;
    LiveWidenGovernor gov(Params(10), [&env] { return env.Sample(); }, [&env] { return env.Now(); });
    ASSERT_EQ(gov.MaxWorkers(), 8u);
    std::atomic<unsigned> started{0};
    std::atomic<bool> in_brake{false};
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> beyond_max{0};
    std::atomic<uint64_t> above_floor_in_brake{0};
    std::atomic<uint64_t> brake_phase_permits{0};
    std::vector<std::thread> workers;
    for (unsigned w = 0; w < THREADS; ++w) {
        workers.emplace_back([&, w] {
            uint64_t n = 0;
            bool counted = false;
            while (!stop.load()) {
                // in_brake true both before and after Permit: the controller set it only after it saw allowed ==
                // floor and feeds the brake until it clears it, so allowed was the floor throughout this Permit
                // (one read alone races: a worker preempted after reading could Permit after the phase ended).
                const bool braking_before = in_brake.load();
                const bool granted = gov.Permit(w);
                const bool braking = braking_before && in_brake.load();
                if (!counted) {
                    counted = true;
                    ++started;
                }
                if (braking)
                    ++brake_phase_permits;
                if (granted) {
                    if (w >= 8)
                        ++beyond_max;
                    if (braking && w >= 1)
                        ++above_floor_in_brake;
                    gov.AddBuilderCpu(1000);
                    if (++n % 7 == 0)
                        gov.Release(w);
                }
                std::this_thread::yield();
            }
            gov.Release(w);
        });
    }
    while (started.load() < THREADS)
        std::this_thread::yield();

    auto step = [&](uint64_t wait_us, uint64_t long_waits) {
        {
            std::lock_guard<std::mutex> lk(env.mtx);
            env.sig.writer_wait_us += wait_us;
            env.sig.long_waits += long_waits;
        }
        env.now_ns += 1'000'000'000;
        std::this_thread::yield();
    };
    // No ASSERT while the workers are joinable (a return would destroy joinable threads: std::terminate).
    auto stop_and_join = [&] {
        stop = true;
        for (auto &t : workers)
            t.join();
    };
    int guard = 0;
    while (gov.GetSnapshot().allowed < 8 && ++guard < 1'000'000)
        step(0, 0);
    EXPECT_EQ(gov.GetSnapshot().allowed, 8u);
    if (gov.GetSnapshot().allowed != 8u) {
        stop_and_join();
        return;
    }
    guard = 0;
    while (!(gov.GetSnapshot().brake_active && gov.GetSnapshot().allowed == 1) && ++guard < 1'000'000)
        step(300'000, 1);
    EXPECT_EQ(gov.GetSnapshot().allowed, 1u);
    if (gov.GetSnapshot().allowed != 1u) {
        stop_and_join();
        return;
    }
    in_brake = true;
    for (int i = 0; i < 300 || brake_phase_permits.load() < 1000; ++i)
        step(300'000, 1);
    in_brake = false;
    for (int i = 0; i < 300; ++i)
        step(0, 0);
    stop_and_join();

    const auto snap = gov.GetSnapshot();
    EXPECT_EQ(beyond_max.load(), 0u);
    EXPECT_EQ(above_floor_in_brake.load(), 0u);
    EXPECT_GE(brake_phase_permits.load(), 1000u);
    EXPECT_LE(snap.peak_active, 8u);
    EXPECT_GE(snap.peak_active, 2u);
    EXPECT_EQ(snap.active, 0u);
}

TEST(LiveWidenGovernorTest, FailureWarningAfterTheClockRecoversHasNoOverflow) {
    std::atomic<bool> clock_fails{true};
    FakeEnv env;
    LiveWidenGovernor gov(Params(16), [&env] { return env.Sample(); },
                          [&] {
                              if (clock_fails)
                                  throw std::runtime_error("fake clock failure");
                              return env.Now();
                          });
    (void)gov.Permit(0);                                // clock never worked: the NEVER + 1 stand-in is recorded
    (void)gov.Permit(0);
    EXPECT_EQ(gov.GetSnapshot().failure_warnings, 1u);

    clock_fails = false;
    (void)gov.Permit(0);                                // baseline
    env.fail_sample = true;
    env.now_ns += 1'000'000'000;
    (void)gov.Permit(0);                                // a real time far from NEVER + 1: warns, no overflow
    EXPECT_EQ(gov.GetSnapshot().failure_warnings, 2u);
    for (int i = 0; i < 59; ++i) {
        env.now_ns += 1'000'000'000;
        (void)gov.Permit(0);
    }
    EXPECT_EQ(gov.GetSnapshot().failure_warnings, 2u);  // once per minute
    env.now_ns += 1'000'000'000;
    (void)gov.Permit(0);
    EXPECT_EQ(gov.GetSnapshot().failure_warnings, 3u);
}

TEST(LiveWidenGovernorTest, ThrowingProgressCostsOnlyThePercent) {
    Rig rig(Params(16), []() -> double { throw std::runtime_error("fake progress failure"); });
    uint64_t evals = rig.gov.GetSnapshot().evaluations;
    for (int t = 1; t <= 120; ++t) {
        EXPECT_EQ(rig.Tick(15), 1u);
        const auto snap = rig.gov.GetSnapshot();
        EXPECT_EQ(snap.evaluations, evals + 1);
        evals = snap.evaluations;
        EXPECT_DOUBLE_EQ(snap.parked_seconds, static_cast<double>(t));
        EXPECT_EQ(snap.reason, GovernorReason::kForegroundCpu);
    }
    const auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.warnings_emitted, 1u);
    EXPECT_EQ(snap.failure_warnings, 0u);
}

TEST(LiveWidenGovernorTest, BrakeCountsAndAllowedRange) {
    Rig rig(Params(16));
    for (int i = 0; i < 5; ++i)
        rig.Tick(0);
    EXPECT_EQ(rig.Tick(0, 0, 0, 1), 1u);                // brake 1 from 6 workers
    EXPECT_EQ(rig.Tick(0, 0, 0, 1), 1u);                // inside the hold: extended, not a new brake
    auto snap = rig.gov.GetSnapshot();
    EXPECT_EQ(snap.brakes, 1u);
    EXPECT_EQ(snap.min_allowed_seen, 1u);
    EXPECT_EQ(snap.max_allowed_seen, 6u);
    for (int i = 0; i < 11; ++i)
        rig.Tick(15);                                   // hold over, foreground keeps it at the floor
    EXPECT_EQ(rig.Tick(15, 0, 0, 1), 1u);               // a new brake that starts at the floor (logged at debug)
    EXPECT_EQ(rig.gov.GetSnapshot().brakes, 2u);
}
