#pragma once
// Load governor of the parallel live widen (Tech Reference 4.18 "Governor"): decides how many of a year's widen
// workers may claim a group, from foreground CPU use and writer waits. Pure logic over injected inputs (sampler,
// clock, progress), no I/O and no thread of its own: workers evaluate it inside Permit(), at most once per tick.
#include "include/XStore/XStore_Tunables.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace livewiden {

// All cumulative. builder_cpu_s is what the sampler knows of the builder's own CPU (normally 0): the governor adds
// the total reported through AddBuilderCpu to it, and that sum is what it subtracts from busy_cpu_s.
// The writer counters are read independently (FileRangeLock::WriterWaits), so each is used only on its own delta;
// one that goes backwards gives delta 0. They belong to one year file: when the widen moves to the next file, call
// RebaselineWriterCounters() so the next evaluation takes that file's values as the baseline.
struct Signals {
    double busy_cpu_s = 0;      // whole machine, all CPUs (/proc/stat)
    double builder_cpu_s = 0;
    uint64_t writer_wait_us = 0;
    uint64_t writer_waits = 0;
    uint64_t long_waits = 0;
};

struct GovernorParams {
    unsigned cores = std::thread::hardware_concurrency();
    unsigned max_workers = 0;   // 0: DefaultMaxWorkers(cores, headroom_cores, max(1, cores / LIVE_WIDEN_CPUS_PER_WORKER), floor)
    unsigned floor_workers = XStoreTunables::LIVE_WIDEN_FLOOR_WORKERS;
    unsigned headroom_cores = XStoreTunables::LIVE_WIDEN_HEADROOM_CORES;
    std::chrono::seconds tick{XStoreTunables::LIVE_WIDEN_TICK_SEC};
    std::chrono::seconds brake_hold{XStoreTunables::LIVE_WIDEN_GOVERNOR_BRAKE_HOLD_SEC};
    std::chrono::seconds starvation_guard{XStoreTunables::LIVE_WIDEN_STARVATION_GUARD_SEC};
    std::chrono::seconds at_floor_warn{XStoreTunables::LIVE_WIDEN_AT_FLOOR_WARN_SEC};
    uint64_t wait_brake_us = XStoreTunables::LIVE_WIDEN_WRITER_WAIT_BRAKE_US;   // per second of wall time; 0: off
    uint64_t long_waits_brake = XStoreTunables::LIVE_WIDEN_LONG_WAITS_BRAKE;    // 0: this brake is off
    unsigned smoothing_ticks = XStoreTunables::LIVE_WIDEN_GOVERNOR_SMOOTHING_TICKS;
    std::string label;          // log prefix "[LIVE WIDEN] [<label>]"
};

// max(floor, min(cores - headroom, cap)), no unsigned wrap when cores <= headroom.
[[nodiscard]] unsigned DefaultMaxWorkers(unsigned cores, unsigned headroom, unsigned cap, unsigned floor_workers);

enum class GovernorReason { kStart, kNone, kRampUp, kForegroundCpu, kWriterWait, kLongWaits };

class LiveWidenGovernor {
    public:
        using Clock = std::chrono::steady_clock;

        struct Snapshot {
            unsigned allowed = 0;
            unsigned max_workers = 0;
            unsigned floor_workers = 0;
            unsigned cpu_target = 0;
            unsigned active = 0;          // workers between a granted Permit and their next denied Permit / Release
            unsigned peak_active = 0;
            bool throttled = false;       // allowed < max_workers
            bool brake_active = false;    // inside a writer-brake hold
            GovernorReason reason = GovernorReason::kStart;
            std::string reason_text;
            double fg_cores = 0;          // foreground CPUs used, as of the last evaluation (max of tick and window)
            double fg_tick = 0;           // ... over the last interval alone
            double wait_us_per_s = 0;     // writer wait per second over the last interval (0 after a rebaseline)
            uint64_t long_waits_delta = 0; // long writer waits in the last interval
            double parked_seconds = 0;    // length of the current at-floor episode (0 above floor or max == floor)
            uint64_t grants = 0;
            uint64_t denials = 0;
            uint64_t evaluations = 0;     // the baseline and skipped (d_wall <= 0) ones are not counted
            uint64_t warnings_emitted = 0;
            unsigned min_allowed_seen = 0; // over the evaluations so far, starting value (the floor) included
            unsigned max_allowed_seen = 0;
            uint64_t brakes = 0;          // writer brakes started (a trigger inside a running hold only extends it)
            uint64_t failure_warnings = 0; // "evaluation skipped" warnings (at most one a minute)
        };

        // progress (optional) returns the fraction of the year done, in [0, 1]; only used in the at-floor warning.
        LiveWidenGovernor(GovernorParams params, std::function<Signals()> sample,
                          std::function<Clock::time_point()> now, std::function<double()> progress = {});
        LiveWidenGovernor(const LiveWidenGovernor &) = delete;
        LiveWidenGovernor &operator=(const LiveWidenGovernor &) = delete;

        [[nodiscard]] unsigned MaxWorkers() const noexcept { return params_.max_workers; }
        // True iff worker_index < allowed (or a floor-0 starvation grant to worker 0). Never blocks and never throws:
        // the evaluation it may run first is skipped when another thread is evaluating, and a throw from an injected
        // function skips that tick (a WARNING at most once a minute).
        [[nodiscard]] bool Permit(unsigned worker_index);
        void Release(unsigned worker_index);
        void AddBuilderCpu(uint64_t ns) noexcept { builder_cpu_ns_.fetch_add(ns, std::memory_order_relaxed); }
        // The next evaluation re-reads the writer counters as a new baseline and computes no writer delta.
        void RebaselineWriterCounters() noexcept { rebaseline_writers_.store(true, std::memory_order_relaxed); }
        [[nodiscard]] Snapshot GetSnapshot() const;

    private:
        struct Point {
            Clock::time_point t;
            double busy_s;
            double builder_s;
        };

        void MaybeEvaluate() noexcept;
        void WarnFailure(Clock::rep at, const char *what) noexcept;
        void Evaluate(Clock::time_point now);   // caller holds eval_mtx_
        void SetActive(unsigned worker_index, bool active);
        [[nodiscard]] std::string ReasonText(GovernorReason reason) const;

        const GovernorParams params_;
        const std::function<Signals()> sample_;
        const std::function<Clock::time_point()> now_;
        const std::function<double()> progress_;

        std::atomic<unsigned> allowed_;
        std::atomic<bool> starvation_grant_{false};
        std::atomic<bool> rebaseline_writers_{false};
        std::atomic<Clock::rep> last_now_;
        std::atomic<Clock::rep> last_fail_warn_;
        std::atomic<Clock::rep> last_eval_;
        std::atomic<uint64_t> builder_cpu_ns_{0};
        std::atomic<unsigned> active_{0};
        std::atomic<unsigned> peak_active_{0};
        std::atomic<uint64_t> grants_{0};
        std::atomic<uint64_t> denials_{0};
        std::atomic<uint64_t> failure_warnings_{0};
        std::unique_ptr<std::atomic<bool>[]> worker_active_;

        // Evaluator-only state, under eval_mtx_ (taken with try_lock).
        std::mutex eval_mtx_;
        bool have_baseline_ = false;
        Signals prev_{};
        std::vector<Point> window_;
        Clock::time_point brake_until_{};
        GovernorReason brake_reason_ = GovernorReason::kNone;
        uint64_t brake_amount_ = 0;
        uint64_t brakes_ = 0;
        double fg_cores_ = 0;
        unsigned cpu_target_ = 0;
        bool at_floor_ = false;
        Clock::time_point floor_since_{};
        bool floor_warned_ = false;
        Clock::time_point zero_since_{};

        // What GetSnapshot reports besides the atomics; written by the evaluator.
        mutable std::mutex snap_mtx_;
        Snapshot published_{};
};

} // namespace livewiden
