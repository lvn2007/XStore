#pragma once
// Production inputs of the live widen governor (Tech Reference 4.18 "Governor"): whole-machine busy CPU from
// /proc/stat and the writer-wait counters of the FileRangeLock of the year file being widened.
#include "include/XStore/XStore_LiveWidenGovernor.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>

class FileRangeLock;

namespace livewiden {

class LiveWidenLoadSource {
    public:
        // proc_stat: "/proc/stat" except in tests.
        explicit LiveWidenLoadSource(std::string proc_stat = "/proc/stat");
        LiveWidenLoadSource(const LiveWidenLoadSource &) = delete;
        LiveWidenLoadSource &operator=(const LiveWidenLoadSource &) = delete;

        // The year being widened: its data-file lock (a FileLockRegistry entry: never erased, so the pointer stays
        // valid) and its folded watermark out of total_rows (progress only; nullptr clears it). LiveWidenOneYear
        // calls it on its calling thread before that year's workers start and again after they are all joined, so
        // no Sample()/Progress() (which run inside the governor's Permit) can overlap it; see LiveWidenOneYear.
        void SetYear(const FileRangeLock *lock, const std::atomic<uint64_t> *rows_done, uint64_t total_rows) noexcept;
        // Never throws: a failed read repeats the previous values. One caller at a time (the governor's evaluator).
        [[nodiscard]] Signals Sample() noexcept;
        // Fraction of the current year folded, in [0, 1]; 0 when no year is set.
        [[nodiscard]] double Progress() const noexcept;
        [[nodiscard]] unsigned Cores() const noexcept { return cores_; }

    private:
        const std::string proc_stat_;
        const double seconds_per_jiffy_;
        const unsigned cores_;
        std::atomic<const FileRangeLock *> lock_{nullptr};
        std::atomic<const std::atomic<uint64_t> *> rows_done_{nullptr};
        std::atomic<uint64_t> total_rows_{0};
        Signals last_{};
};

// The production governor: real steady clock, `load` as sampler and progress, the GovernorParams defaults
// (XStoreTunables) with cores = load.Cores(). `load` must outlive it.
[[nodiscard]] std::unique_ptr<LiveWidenGovernor> MakeLiveWidenGovernor(LiveWidenLoadSource &load, std::string label);

} // namespace livewiden
