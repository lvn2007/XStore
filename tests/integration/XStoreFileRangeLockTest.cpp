// FileRangeLock's held-lock table (sorted WRITEs + unordered READs) against a brute-force model of the old flat
// table with its linear Conflicts scan, plus a mutual-exclusion stress test of READ rows vs large WRITE run sets.
#include <gtest/gtest.h>
#include "include/DataStructs/FileRangeLock.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <random>
#include <thread>
#include <vector>

namespace {

using LockType = FileRangeLock::LockType;
using LockRun = FileRangeLock::Run;
constexpr std::chrono::milliseconds NO_WAIT{0};

unsigned long long EndOf(const LockRun &r) {
    return r.length == 0 ? std::numeric_limits<unsigned long long>::max() : r.start + r.length;
}

// The old table: one flat list, linear overlap scan, a conflict needs a WRITE on either side.
struct Model {
    struct Entry {
        unsigned long long start;
        unsigned long long end;
        LockType type;
    };
    std::vector<Entry> entries;

    bool Conflicts(unsigned long long start, unsigned long long end, LockType type) const {
        for (const Entry &e : entries) {
            if (start < e.end && e.start < end && (type == LockType::WRITE || e.type == LockType::WRITE)) return true;
        }
        return false;
    }
    bool RunsFree(const std::vector<LockRun> &runs, LockType type) const {
        for (const LockRun &r : runs) {
            if (Conflicts(r.start, EndOf(r), type)) return false;
        }
        return true;
    }
    void Add(const LockRun &r, LockType type) { entries.push_back({r.start, EndOf(r), type}); }
    // Removes one matching entry; false if none.
    bool RemoveOne(const LockRun &r, LockType type) {
        const unsigned long long end = EndOf(r);
        for (size_t i = 0; i < entries.size(); ++i) {
            if (entries[i].start == r.start && entries[i].end == end && entries[i].type == type) {
                entries.erase(entries.begin() + static_cast<std::ptrdiff_t>(i));
                return true;
            }
        }
        return false;
    }
};

// What one caller holds: a single range (UnlockRange) or a run set (UnlockRuns).
struct Holder {
    std::vector<LockRun> runs;
    LockType type;
    bool is_set;
};

}  // namespace

TEST(FileRangeLockTableTest, MatchesBruteForceModel) {
    std::mt19937_64 rng(20261021);
    const auto uniform = [&](uint64_t lo, uint64_t hi) { return std::uniform_int_distribution<uint64_t>(lo, hi)(rng); };
    // Multiples of 5 on a small span: overlaps, exact duplicates and adjacency (end == start) are all common.
    const auto random_range = [&]() -> LockRun {
        const uint64_t pick = uniform(0, 99);
        const unsigned long long start = uniform(0, 40) * 5;
        if (pick < 3) return {start, 0};                // rest of file
        if (pick < 8) return {start, uniform(40, 100) * 5}; // larger than anything else held
        return {start, uniform(1, 10) * 5};
    };
    const auto random_runs = [&]() {
        std::vector<LockRun> runs;
        unsigned long long at = uniform(0, 30) * 5;
        const size_t n = static_cast<size_t>(uniform(2, 7));
        for (size_t i = 0; i < n; ++i) {
            const unsigned long long len = uniform(1, 4) * 5;
            runs.push_back({at, len});
            at += len + uniform(1, 6) * 5;
        }
        return runs;
    };
    const auto random_type = [&] { return uniform(0, 1) == 0 ? LockType::READ : LockType::WRITE; };

    FileRangeLock lock;
    Model model;
    std::vector<Holder> holders;
    size_t tries_ok = 0;
    size_t tries_refused = 0;
    size_t sets_taken = 0;
    size_t sets_refused = 0;
    size_t duplicate_reads = 0;

    for (int step = 0; step < 20000; ++step) {
        const uint64_t op = uniform(0, 99);
        const bool want_unlock = holders.size() > 40 || (op >= 60 && !holders.empty());
        if (want_unlock) {
            const size_t i = static_cast<size_t>(uniform(0, holders.size() - 1));
            const Holder h = holders[i];
            holders.erase(holders.begin() + static_cast<std::ptrdiff_t>(i));
            if (h.is_set) {
                lock.UnlockRuns(h.runs.data(), h.runs.size(), h.type);
            } else {
                lock.UnlockRange(h.runs[0].start, h.runs[0].length, h.type);
            }
            for (const LockRun &r : h.runs) ASSERT_TRUE(model.RemoveOne(r, h.type));
        } else if (op < 25) {
            // Timed try, never blocks: must succeed exactly when the model is free.
            const LockRun r = random_range();
            const LockType type = random_type();
            const bool expected = !model.Conflicts(r.start, EndOf(r), type);
            const bool got = lock.TryLockRangeFor(r.start, r.length, type, NO_WAIT);
            ASSERT_EQ(got, expected) << "step " << step << " [" << r.start << ", +" << r.length << ") "
                                     << (type == LockType::WRITE ? "WRITE" : "READ");
            if (got) {
                model.Add(r, type);
                holders.push_back({{r}, type, false});
                ++tries_ok;
            } else {
                ++tries_refused;
            }
        } else if (op < 35) {
            // Blocking LockRange, only when the model says it will not wait.
            const LockRun r = random_range();
            const LockType type = random_type();
            if (model.Conflicts(r.start, EndOf(r), type)) continue;
            lock.LockRange(r.start, r.length, type);
            model.Add(r, type);
            holders.push_back({{r}, type, false});
        } else if (op < 45) {
            // An identical copy of a held READ (single or set) by another holder.
            std::vector<size_t> reads;
            for (size_t i = 0; i < holders.size(); ++i) {
                if (holders[i].type == LockType::READ) reads.push_back(i);
            }
            if (reads.empty()) continue;
            const Holder h = holders[reads[static_cast<size_t>(uniform(0, reads.size() - 1))]];
            ASSERT_TRUE(model.RunsFree(h.runs, LockType::READ)) << "a held READ conflicts with itself";
            if (h.is_set) {
                ASSERT_TRUE(lock.RunsFreeForTests(h.runs.data(), h.runs.size(), LockType::READ)) << "step " << step;
                lock.LockRuns(h.runs.data(), h.runs.size(), LockType::READ);
            } else {
                ASSERT_TRUE(lock.TryLockRangeFor(h.runs[0].start, h.runs[0].length, LockType::READ, NO_WAIT));
            }
            for (const LockRun &r : h.runs) model.Add(r, LockType::READ);
            holders.push_back(h);
            ++duplicate_reads;
        } else if (op < 55) {
            const std::vector<LockRun> runs = random_runs();
            const LockType type = random_type();
            // Checked on every set, conflicting ones too: LockRuns alone would only ever see free sets here.
            const bool expected = model.RunsFree(runs, type);
            ASSERT_EQ(lock.RunsFreeForTests(runs.data(), runs.size(), type), expected)
                << "step " << step << " " << (type == LockType::WRITE ? "WRITE" : "READ") << " set";
            if (!expected) {
                ++sets_refused;
                continue;
            }
            lock.LockRuns(runs.data(), runs.size(), type);
            for (const LockRun &r : runs) model.Add(r, type);
            holders.push_back({runs, type, true});
            ++sets_taken;
        } else {
            // Unlock of something nobody holds: a no-op.
            const LockRun r = random_range();
            const LockType type = random_type();
            Model probe = model;
            if (probe.RemoveOne(r, type)) continue;
            lock.UnlockRange(r.start, r.length, type);
        }
        ASSERT_EQ(lock.ActiveLocksForTests(), model.entries.size()) << "step " << step;
    }
    for (const Holder &h : holders) {
        if (h.is_set) {
            lock.UnlockRuns(h.runs.data(), h.runs.size(), h.type);
        } else {
            lock.UnlockRange(h.runs[0].start, h.runs[0].length, h.type);
        }
    }
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_GT(tries_ok, 100u);
    EXPECT_GT(tries_refused, 100u);
    EXPECT_GT(sets_taken, 100u);
    EXPECT_GT(sets_refused, 100u);
    EXPECT_GT(duplicate_reads, 100u);
}

TEST(FileRangeLockTableTest, IdenticalReadSetsFromTwoHoldersReleaseOneEntryPerRun) {
    FileRangeLock lock;
    const std::vector<LockRun> runs = {{0, 10}, {50, 10}, {100, 10}, {200, 10}};
    lock.LockRuns(runs.data(), runs.size(), LockType::READ);
    lock.LockRuns(runs.data(), runs.size(), LockType::READ);
    EXPECT_EQ(lock.ActiveLocksForTests(), 8u);
    lock.UnlockRuns(runs.data(), runs.size(), LockType::READ);
    EXPECT_EQ(lock.ActiveLocksForTests(), 4u);
    for (const LockRun &r : runs) {
        EXPECT_FALSE(lock.TryLockRangeFor(r.start, r.length, LockType::WRITE, NO_WAIT)) << "second holder still reads";
    }
    lock.UnlockRuns(runs.data(), runs.size(), LockType::READ);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    ASSERT_TRUE(lock.TryLockRangeFor(0, 0, LockType::WRITE, NO_WAIT));
    lock.UnlockRange(0, 0, LockType::WRITE);
}

TEST(FileRangeLockTableTest, ReadersAndLargeWriteRunSetsExcludeEachOther) {
    constexpr size_t ROWS = 8192;
    constexpr unsigned long long ROW_LEN = 64;
    constexpr int32_t WRITER = 1 << 16;
    FileRangeLock lock;
    // Per row: readers inside it in the low 16 bits, writers inside it in units of WRITER.
    std::vector<std::atomic<int32_t>> inside(ROWS);
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> violations{0};
    std::atomic<uint64_t> read_ops{0};
    std::atomic<uint64_t> set_ops{0};
    std::atomic<uint64_t> single_write_ops{0};

    const auto enter_read = [&](size_t row) {
        if (inside[row].fetch_add(1) >= WRITER) violations.fetch_add(1);
    };
    const auto leave_read = [&](size_t row) { inside[row].fetch_sub(1); };
    const auto enter_write = [&](size_t row) {
        if (inside[row].fetch_add(WRITER) != 0) violations.fetch_add(1);
    };
    const auto leave_write = [&](size_t row) { inside[row].fetch_sub(WRITER); };

    std::vector<std::thread> threads;
    for (unsigned t = 0; t < 6; ++t) {
        threads.emplace_back([&, t] {
            std::mt19937_64 rng(1000 + t);
            std::uniform_int_distribution<size_t> row_of(0, ROWS - 1);
            while (!stop.load(std::memory_order_relaxed)) {
                const size_t row = row_of(rng);
                lock.LockRange(row * ROW_LEN, ROW_LEN, LockType::READ);
                enter_read(row);
                std::this_thread::yield();
                leave_read(row);
                lock.UnlockRange(row * ROW_LEN, ROW_LEN, LockType::READ);
                read_ops.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    // A WAL-apply-like writer: ~1000 scattered rows per set, held while it "writes" each row.
    threads.emplace_back([&] {
        std::mt19937_64 rng(7);
        std::uniform_int_distribution<size_t> row_of(0, ROWS - 1);
        std::vector<unsigned long long> offsets;
        std::vector<LockRun> runs;
        while (!stop.load(std::memory_order_relaxed)) {
            offsets.clear();
            for (int i = 0; i < 1000; ++i) offsets.push_back(row_of(rng) * ROW_LEN);
            std::sort(offsets.begin(), offsets.end());
            BuildLockRuns(offsets.data(), offsets.size(), ROW_LEN, 4096, runs);
            lock.LockRuns(runs.data(), runs.size(), LockType::WRITE);
            for (const LockRun &r : runs) {
                for (unsigned long long o = r.start; o < r.start + r.length; o += ROW_LEN) enter_write(o / ROW_LEN);
            }
            std::this_thread::yield();
            for (const LockRun &r : runs) {
                for (unsigned long long o = r.start; o < r.start + r.length; o += ROW_LEN) leave_write(o / ROW_LEN);
            }
            lock.UnlockRuns(runs.data(), runs.size(), LockType::WRITE);
            set_ops.fetch_add(1, std::memory_order_relaxed);
        }
    });
    // A single-row writer, so `writes` also sees inserts and erases between a set's runs.
    threads.emplace_back([&] {
        std::mt19937_64 rng(11);
        std::uniform_int_distribution<size_t> row_of(0, ROWS - 1);
        while (!stop.load(std::memory_order_relaxed)) {
            const size_t row = row_of(rng);
            lock.LockRange(row * ROW_LEN, ROW_LEN, LockType::WRITE);
            enter_write(row);
            std::this_thread::yield();
            leave_write(row);
            lock.UnlockRange(row * ROW_LEN, ROW_LEN, LockType::WRITE);
            single_write_ops.fetch_add(1, std::memory_order_relaxed);
        }
    });

    // Stop on coverage, not on time; the cap only guards against a hang.
    const auto cap = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while ((set_ops.load() < 200 || read_ops.load() < 100000 || single_write_ops.load() < 1000) &&
           std::chrono::steady_clock::now() < cap) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    stop.store(true);
    for (std::thread &th : threads) th.join();

    EXPECT_EQ(violations.load(), 0u);
    EXPECT_EQ(lock.ActiveLocksForTests(), 0u);
    EXPECT_GE(read_ops.load(), 100000u);
    EXPECT_GE(set_ops.load(), 200u);
    EXPECT_GE(single_write_ops.load(), 1000u);
}
