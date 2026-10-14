// Progress display of the startup index rebuild (XStore_RebuildProgress.h): the pure formatting / ETA arithmetic, and
// the reporter thread's lifecycle on a fake counter. The engine with the display on is in XStoreParallelRebuildTest.cpp.
#include <gtest/gtest.h>
#include "XStore/XStore_RebuildProgress.h"
#include "XStoreRebuildTestSupport.h"
#include <atomic>
#include <chrono>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {
using namespace rebuildprogress;
using xstore_rebuild_test::ProgressCapture;

Sample Make(uint64_t done, uint64_t total, double elapsed_s, size_t years_done = 0, size_t years_total = 1) {
    Sample sample;
    sample.rows_done = done;
    sample.rows_total = total;
    sample.elapsed_s = elapsed_s;
    sample.years_done = years_done;
    sample.years_total = years_total;
    return sample;
}

bool Contains(const std::string &haystack, const std::string &needle) { return haystack.find(needle) != std::string::npos; }

TEST(RebuildProgressFormatTest, NoProgressYetHasNoEta) {
    const Sample sample = Make(0, 1000, 5.0);
    EXPECT_EQ(PercentDone(sample), 0.0);
    EXPECT_FALSE(EstimateSecondsLeft(sample).has_value());
    EXPECT_TRUE(Contains(FormatRebuildProgress(sample), "0% (0 of 1.0K rows), 0/1 years done, 0 rows/s, elapsed 5s, estimating..."))
        << FormatRebuildProgress(sample);
}

TEST(RebuildProgressFormatTest, FullIsOneHundredPercentAndDone) {
    const Sample sample = Make(1000, 1000, 10.0, 1, 1);
    EXPECT_EQ(PercentDone(sample), 100.0);
    ASSERT_TRUE(EstimateSecondsLeft(sample).has_value());
    EXPECT_EQ(*EstimateSecondsLeft(sample), 0.0);
    EXPECT_TRUE(Contains(FormatRebuildProgress(sample), "100% (1.0K of 1.0K rows), 1/1 years done, 100 rows/s, elapsed 10s, done"))
        << FormatRebuildProgress(sample);
}

TEST(RebuildProgressFormatTest, PercentIsFlooredAndClamped) {
    EXPECT_EQ(PercentDone(Make(999, 1000, 1)), 99.0);   // never 100% before the last row
    EXPECT_EQ(PercentDone(Make(1500, 1000, 1)), 100.0); // rows_done above the total
    EXPECT_EQ(PercentDone(Make(0, 0, 0)), 100.0);       // nothing to do
    EXPECT_TRUE(Contains(FormatRebuildProgress(Make(1500, 1000, 1)), "100% (1.5K of 1.0K rows)"));
}

TEST(RebuildProgressFormatTest, ZeroRateHasNoEta) {
    const Sample sample = Make(10, 1000, 0.0); // no time has passed: no rate
    EXPECT_EQ(RowsPerSecond(sample), 0.0);
    EXPECT_FALSE(EstimateSecondsLeft(sample).has_value());
    Sample stalled = Make(10, 1000, 40.0);
    stalled.window_s = 30.0;
    stalled.window_rows = 0; // nothing moved in the window
    EXPECT_FALSE(EstimateSecondsLeft(stalled).has_value());
    EXPECT_TRUE(Contains(FormatRebuildProgress(stalled), "estimating..."));
}

TEST(RebuildProgressFormatTest, WindowRateWinsOverTheAverageAndTheAverageIsTheFallback) {
    Sample sample = Make(3000, 13000, 100.0);
    // No window yet: 3000 rows in 100 s = 30 rows/s, 10000 left = 333.3 s.
    EXPECT_DOUBLE_EQ(RowsPerSecond(sample), 30.0);
    ASSERT_TRUE(EstimateSecondsLeft(sample).has_value());
    EXPECT_NEAR(*EstimateSecondsLeft(sample), 10000.0 / 30.0, 1e-9);
    // A 30 s window with 6000 rows: 200 rows/s, 10000 left = 50 s.
    sample.window_s = 30.0;
    sample.window_rows = 6000;
    EXPECT_DOUBLE_EQ(RowsPerSecond(sample), 200.0);
    EXPECT_NEAR(*EstimateSecondsLeft(sample), 50.0, 1e-9);
    EXPECT_TRUE(Contains(FormatRebuildProgress(sample), "200 rows/s, elapsed 1m40s, about 50s left"))
        << FormatRebuildProgress(sample);
}

TEST(RebuildProgressFormatTest, DurationsRollOver) {
    EXPECT_EQ(FormatDuration(0), "0s");
    EXPECT_EQ(FormatDuration(42), "42s");
    EXPECT_EQ(FormatDuration(59), "59s");
    EXPECT_EQ(FormatDuration(59.6), "1m00s");
    EXPECT_EQ(FormatDuration(60), "1m00s");
    EXPECT_EQ(FormatDuration(190), "3m10s");
    EXPECT_EQ(FormatDuration(3599), "59m59s");
    EXPECT_EQ(FormatDuration(3600), "1h00m");
    EXPECT_EQ(FormatDuration(3720), "1h02m");
    EXPECT_EQ(FormatDuration(-5), "0s");
}

TEST(RebuildProgressFormatTest, SiSuffixes) {
    EXPECT_EQ(FormatSi(0), "0");
    EXPECT_EQ(FormatSi(950), "950");
    EXPECT_EQ(FormatSi(999.4), "999");
    EXPECT_EQ(FormatSi(999.6), "1.0K");
    EXPECT_EQ(FormatSi(1000), "1.0K");
    EXPECT_EQ(FormatSi(13.0e6), "13.0M");
    EXPECT_EQ(FormatSi(41.7e6), "41.7M");
    EXPECT_EQ(FormatSi(999.99e3), "1.0M");
    EXPECT_EQ(FormatSi(452.0e9), "452.0G");
    EXPECT_EQ(FormatSi(2.5e12), "2.5T");
    EXPECT_EQ(FormatSi(5.0e15), "5000.0T");
}

TEST(RebuildProgressFormatTest, YearCountsAndBarPostfix) {
    EXPECT_TRUE(Contains(FormatRebuildProgress(Make(41'700'000, 41'700'000, 20.0, 11, 11)), "11/11 years done"));
    EXPECT_TRUE(Contains(FormatRebuildProgress(Make(0, 0, 0.0, 0, 0)), "0/0 years done"));
    Sample sample = Make(13'000'000, 41'700'000, 190.0, 3, 11);
    sample.window_s = 30;
    sample.window_rows = static_cast<uint64_t>(41.2e6 * 30);
    EXPECT_EQ(FormatBarPostfix(sample), "3/11 years  41.2M rows/s");
    EXPECT_TRUE(Contains(FormatRebuildProgress(sample), "31% (13.0M of 41.7M rows), 3/11 years done, 41.2M rows/s, elapsed 3m10s"))
        << FormatRebuildProgress(sample);
}

TEST(RebuildProgressFormatTest, AStoppedRebuildSaysSo) {
    Sample sample = Make(500, 1000, 10.0);
    sample.finished = true;
    EXPECT_TRUE(Contains(FormatRebuildProgress(sample), "stopped before the end"));
}

// A counter the reporter reads while the test advances it.
struct FakeCounter {
    std::atomic<uint64_t> rows{0};
    uint64_t total = 1000;
    Reporter::Sampler Sampler() {
        return [this] {
            Sample sample;
            sample.rows_done = rows.load(std::memory_order_relaxed);
            sample.rows_total = total;
            sample.years_done = sample.rows_done >= total ? 1 : 0;
            sample.years_total = 1;
            return sample;
        };
    }
};

template <typename Pred>
bool WaitFor(Pred pred) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

TEST(RebuildProgressReporterTest, LogsWhileRunningAndOnceMoreOnStopThenGoesQuiet) {
    FakeCounter counter;
    ProgressCapture capture;
    // A one-hour interval: the join must be immediate, not a wait for the next tick.
    Options options = capture.Options();
    options.log_interval = std::chrono::hours(1);
    const auto begin = std::chrono::steady_clock::now();
    {
        Reporter reporter("T", counter.Sampler(), options);
        counter.rows = 1000;
        reporter.Stop();
        reporter.Stop(); // idempotent
    }
    EXPECT_LT(std::chrono::steady_clock::now() - begin, std::chrono::seconds(5));
    const std::vector<std::string> lines = capture.Lines();
    ASSERT_EQ(lines.size(), 1u) << "only the final line";
    EXPECT_TRUE(Contains(lines[0], "[INDEX REBUILD] [DB: T] - 100% (1.0K of 1.0K rows), 1/1 years done"));

    ProgressCapture ticking;
    {
        Reporter reporter("T", counter.Sampler(), ticking.Options(1));
        ASSERT_TRUE(WaitFor([&] { return ticking.Lines().size() >= 3; })) << "no periodic lines";
    } // the destructor stops it
    const size_t at_stop = ticking.Lines().size();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_EQ(ticking.Lines().size(), at_stop);
}

TEST(RebuildProgressReporterTest, AnExceptionInTheSamplerIsSwallowedAndStopStillJoins) {
    std::atomic<int> calls{0};
    ProgressCapture capture;
    Reporter reporter("T", [&]() -> Sample {
        ++calls;
        throw std::runtime_error("sampler failed");
    }, capture.Options(1));
    ASSERT_TRUE(WaitFor([&] { return calls.load() >= 1; }));
    reporter.Stop(); // must neither hang nor throw
    EXPECT_EQ(calls.load(), 1) << "the display gives up after the first failure";
    EXPECT_TRUE(capture.Lines().empty());
}

TEST(RebuildProgressReporterTest, BarModeDrawsToItsStreamEndsTheLineAndNeverHidesTheCursor) {
    FakeCounter counter;
    std::ostringstream out;
    Options options;
    options.mode = Mode::kBar;
    options.bar_interval = std::chrono::milliseconds(1);
    options.bar_stream = &out;
    {
        Reporter reporter("T", counter.Sampler(), options);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        counter.rows = 1000;
    }
    const std::string text = out.str();
    EXPECT_TRUE(Contains(text, "100%")) << text;
    EXPECT_TRUE(Contains(text, "1/1 years")) << text;
    ASSERT_FALSE(text.empty());
    EXPECT_EQ(text.back(), '\n') << "the next log line must start clean";
    EXPECT_FALSE(Contains(text, "\x1b[?25l")) << "the cursor must stay visible";
}

TEST(RebuildProgressReporterTest, BarModeOfAStoppedRebuildStillEndsTheLine) {
    FakeCounter counter;
    counter.rows = 400;
    std::ostringstream out;
    Options options;
    options.mode = Mode::kBar;
    options.bar_interval = std::chrono::milliseconds(1);
    options.bar_stream = &out;
    {
        Reporter reporter("T", counter.Sampler(), options);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const std::string text = out.str();
    EXPECT_TRUE(Contains(text, "40%")) << text;
    ASSERT_FALSE(text.empty());
    EXPECT_EQ(text.back(), '\n');
}

} // namespace
