// Cost of the checkpointer's paced flush step on the dirty ranges (DirtyTracker) while writers Note() new ones.
// "old step" = what the step did before the flush queue: PeekRanges() copy + scan for the first unflushed gap;
// "pop step" = PopFlushChunk().
// Opt-in at run time (XSTORE_DIRTY_PERF=1) and not a pass/fail gate: it prints numbers for a human doing perf
// work, like tests/perf/XStorePerfTest.cpp. Run it on the benchmark host, never on the dev box.
//
// Shape under test: scattered inserts leave about one dirty range per row; between the ring's soft fill mark and
// its cycle mark the checkpointer runs one paced flush step per millisecond, and each step peeks every range.
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <map>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "include/XStore/XStore_WalRing.h"

namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t ROW_BYTES = 1408;
constexpr uint64_t SLOT_BYTES = 4096;   // gap between rows so neighbouring ranges never merge

double MicrosSince(Clock::time_point start) {
    return std::chrono::duration<double, std::micro>(Clock::now() - start).count();
}

std::string PathOf(size_t file) { return "/dev/shm/xstore_dirty_perf_" + std::to_string(file); }

// `count` disjoint ranges spread over `files` paths, noted in shuffled order like scattered inserts.
void FillRanges(size_t count, size_t files) {
    walring::ClearDirtyForTests();
    std::vector<uint64_t> slots(count);
    std::iota(slots.begin(), slots.end(), 0);
    std::mt19937_64 rng(42);
    std::shuffle(slots.begin(), slots.end(), rng);
    for (size_t i = 0; i < count; ++i) {
        const uint64_t begin = slots[i] * SLOT_BYTES;
        walring::Dirty().Note(nullptr, PathOf(i % files), begin, begin + ROW_BYTES, 0);
    }
}

// The scan the old PacedFlushStep did on a peeked snapshot: first gap of each range against the already-flushed map.
bool FirstGap(const std::map<uint64_t, uint64_t> &m, uint64_t begin, uint64_t end) {
    uint64_t pos = begin;
    auto it = m.upper_bound(pos);
    if (it != m.begin()) {
        const auto p = std::prev(it);
        if (p->second > pos) pos = p->second;
    }
    while (pos < end) {
        it = m.lower_bound(pos);
        if (it != m.end() && it->first == pos) { pos = it->second; continue; }
        return true;
    }
    return false;
}

class DirtyTrackerPerfTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (std::getenv("XSTORE_DIRTY_PERF") == nullptr) GTEST_SKIP() << "set XSTORE_DIRTY_PERF=1 to run";
    }
    void TearDown() override { walring::ClearDirtyForTests(); }
};

TEST_F(DirtyTrackerPerfTest, PeekCostByRangeCount) {
    for (const size_t count : {1000u, 10000u, 100000u}) {
        FillRanges(count, 4);
        constexpr int ITERATIONS = 200;
        size_t sink = 0;
        const auto start = Clock::now();
        for (int i = 0; i < ITERATIONS; ++i) sink += walring::Dirty().PeekRanges().ranges.size();
        const double per_peek = MicrosSince(start) / ITERATIONS;
        std::cout << "[peek] ranges=" << count << " us/peek=" << per_peek << " ns/range=" << per_peek * 1000.0 / count
                  << " (sink " << sink << ")\n";
    }
}

TEST_F(DirtyTrackerPerfTest, ScanCostWhenHalfTheRangesAreFlushed) {
    for (const size_t count : {1000u, 10000u, 100000u}) {
        FillRanges(count, 1);
        const auto snap = walring::Dirty().PeekRanges();
        const auto &ranges = snap.ranges.begin()->second;
        std::map<uint64_t, uint64_t> done;
        for (size_t i = 0; i < ranges.size() / 2; ++i) done.emplace(ranges[i].begin, ranges[i].end);
        size_t first_unflushed = 0;
        const auto start = Clock::now();
        constexpr int ITERATIONS = 50;
        for (int it = 0; it < ITERATIONS; ++it) {
            for (size_t i = 0; i < ranges.size(); ++i) {
                if (FirstGap(done, ranges[i].begin, ranges[i].end)) { first_unflushed = i; break; }
            }
        }
        const double per_scan = MicrosSince(start) / ITERATIONS;
        std::cout << "[scan] ranges=" << count << " us/scan=" << per_scan << " first_unflushed_index=" << first_unflushed << "\n";
    }
}

// Writers hammer Note() on a fixed set of slots (so the range count stays constant) for a few seconds, with and
// without a thread peeking once per millisecond like the checkpointer.
TEST_F(DirtyTrackerPerfTest, NoteLatencyWithAFlushStepAt1kHz) {
    constexpr size_t RANGES = 20000;
    constexpr int WRITERS = 8;
    enum class Step { kNone, kOld, kPop };
    for (const Step step : {Step::kNone, Step::kOld, Step::kPop}) {
        FillRanges(RANGES, 4);
        std::atomic<bool> stop{false};
        std::atomic<uint64_t> total_notes{0};
        std::vector<std::vector<float>> samples(WRITERS);
        std::vector<std::thread> threads;
        for (int w = 0; w < WRITERS; ++w) {
            threads.emplace_back([&, w] {
                std::mt19937_64 rng(1000 + w);
                uint64_t n = 0;
                while (!stop.load(std::memory_order_relaxed)) {
                    const uint64_t slot = rng() % RANGES;
                    const uint64_t begin = slot * SLOT_BYTES;
                    const bool sample = (n & 63) == 0;
                    const auto t0 = sample ? Clock::now() : Clock::time_point{};
                    walring::Dirty().Note(nullptr, PathOf(slot % 4), begin, begin + ROW_BYTES, 0);
                    if (sample) samples[w].push_back(static_cast<float>(MicrosSince(t0)));
                    ++n;
                }
                total_notes.fetch_add(n, std::memory_order_relaxed);
            });
        }
        std::thread stepper;
        std::atomic<uint64_t> steps{0};
        if (step != Step::kNone) {
            stepper = std::thread([&] {
                std::unordered_map<std::string, std::map<uint64_t, uint64_t>> done;
                std::string path;
                walring::DirtyRange chunk;
                while (!stop.load(std::memory_order_relaxed)) {
                    if (step == Step::kPop) {
                        walring::Dirty().PopFlushChunk(4u << 20, path, chunk);
                    } else {
                        const auto snap = walring::Dirty().PeekRanges();
                        for (const auto &[file, ranges] : snap.ranges) {
                            auto &flushed = done[file];
                            for (const walring::DirtyRange &r : ranges) {
                                if (!FirstGap(flushed, r.begin, r.end)) continue;
                                flushed.emplace(r.begin, r.end);   // the flush of that range
                                break;
                            }
                        }
                    }
                    steps.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            });
        }
        std::this_thread::sleep_for(std::chrono::seconds(3));
        stop.store(true);
        for (auto &t : threads) t.join();
        if (stepper.joinable()) stepper.join();
        std::vector<float> all;
        for (const auto &s : samples) all.insert(all.end(), s.begin(), s.end());
        std::sort(all.begin(), all.end());
        const auto pct = [&](double p) { return all[static_cast<size_t>(p * (all.size() - 1))]; };
        const char *name = step == Step::kNone ? "none" : step == Step::kOld ? "old" : "pop";
        std::cout << "[note] step=" << name << " writers=" << WRITERS << " ranges=" << RANGES
                  << " Mnotes/s=" << total_notes.load() / 3.0 / 1e6 << " p50us=" << pct(0.50) << " p99us=" << pct(0.99)
                  << " p999us=" << pct(0.999) << " max_us=" << all.back() << " steps=" << steps.load() << "\n";
    }
}

}  // namespace
