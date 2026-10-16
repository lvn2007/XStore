#include "include/XStore/XStore_LiveWidenGovernor.h"

#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

#include <algorithm>
#include <cmath>
#include <exception>
#include <limits>
#include <utility>

namespace livewiden {

namespace {

constexpr LiveWidenGovernor::Clock::rep NEVER = std::numeric_limits<LiveWidenGovernor::Clock::rep>::min();
constexpr auto FAILURE_WARN_INTERVAL = std::chrono::minutes(1);

double Seconds(LiveWidenGovernor::Clock::duration d) {
    return std::chrono::duration<double>(d).count();
}

uint64_t CounterDelta(uint64_t cur, uint64_t prev) {
    return cur >= prev ? cur - prev : 0;
}

} // namespace

unsigned DefaultMaxWorkers(unsigned cores, unsigned headroom, unsigned cap, unsigned floor_workers) {
    const unsigned usable = cores > headroom ? cores - headroom : 0;
    return std::max(floor_workers, std::min(usable, cap));
}

LiveWidenGovernor::LiveWidenGovernor(GovernorParams params, std::function<Signals()> sample,
                                     std::function<Clock::time_point()> now, std::function<double()> progress)
    : params_([&] {
          if (params.max_workers == 0)
              params.max_workers = DefaultMaxWorkers(params.cores, params.headroom_cores,
                                                     std::max(1u, params.cores / XStoreTunables::LIVE_WIDEN_CPUS_PER_WORKER),
                                                     params.floor_workers);
          params.max_workers = std::max(params.max_workers, params.floor_workers);
          params.smoothing_ticks = std::max(params.smoothing_ticks, 1u);
          return std::move(params);
      }()),
      sample_(std::move(sample)), now_(std::move(now)), progress_(std::move(progress)),
      allowed_(params_.floor_workers), last_now_(NEVER), last_fail_warn_(NEVER), last_eval_(NEVER),
      worker_active_(std::make_unique<std::atomic<bool>[]>(std::max(params_.max_workers, 1u))) {
    for (unsigned i = 0; i < std::max(params_.max_workers, 1u); ++i)
        worker_active_[i].store(false, std::memory_order_relaxed);
    window_.reserve(params_.smoothing_ticks + 1);
    cpu_target_ = params_.floor_workers;
    published_.cpu_target = cpu_target_;
    published_.min_allowed_seen = published_.max_allowed_seen = params_.floor_workers;
    published_.reason = GovernorReason::kStart;
    published_.reason_text = ReasonText(GovernorReason::kStart);
}

bool LiveWidenGovernor::Permit(unsigned worker_index) {
    MaybeEvaluate();
    bool granted = worker_index < allowed_.load(std::memory_order_relaxed);
    if (!granted && worker_index == 0 && starvation_grant_.exchange(false, std::memory_order_relaxed))
        granted = true;
    (granted ? grants_ : denials_).fetch_add(1, std::memory_order_relaxed);
    SetActive(worker_index, granted);
    return granted;
}

void LiveWidenGovernor::Release(unsigned worker_index) {
    SetActive(worker_index, false);
}

void LiveWidenGovernor::SetActive(unsigned worker_index, bool active) {
    if (worker_index >= std::max(params_.max_workers, 1u))
        return;
    if (worker_active_[worker_index].exchange(active, std::memory_order_relaxed) == active)
        return;
    if (!active) {
        active_.fetch_sub(1, std::memory_order_relaxed);
        return;
    }
    const unsigned now_active = active_.fetch_add(1, std::memory_order_relaxed) + 1;
    unsigned peak = peak_active_.load(std::memory_order_relaxed);
    while (now_active > peak && !peak_active_.compare_exchange_weak(peak, now_active, std::memory_order_relaxed)) {
    }
}

void LiveWidenGovernor::WarnFailure(Clock::rep at, const char *what) noexcept {
    if (at == NEVER)
        at = NEVER + 1;   // the clock never worked: still at most one warning until it does
    // No subtraction: `at - last` overflows when last is the NEVER + 1 stand-in and the clock has since recovered.
    constexpr Clock::rep interval = std::chrono::duration_cast<Clock::duration>(FAILURE_WARN_INTERVAL).count();
    Clock::rep last = last_fail_warn_.load(std::memory_order_relaxed);
    if (last != NEVER && last <= std::numeric_limits<Clock::rep>::max() - interval && at < last + interval)
        return;
    if (!last_fail_warn_.compare_exchange_strong(last, at, std::memory_order_relaxed))
        return;
    failure_warnings_.fetch_add(1, std::memory_order_relaxed);
    try {
        spdlog::warn("[LIVE WIDEN] [{}] - Governor evaluation skipped: {} (keeping {} workers)", params_.label, what,
                     allowed_.load(std::memory_order_relaxed));
    } catch (...) {
    }
}

void LiveWidenGovernor::MaybeEvaluate() noexcept {
    Clock::time_point t;
    try {
        t = now_();
    } catch (const std::exception &e) {
        WarnFailure(last_now_.load(std::memory_order_relaxed), e.what());
        return;
    } catch (...) {
        WarnFailure(last_now_.load(std::memory_order_relaxed), "unknown exception from the clock");
        return;
    }
    const Clock::rep rep = t.time_since_epoch().count();
    last_now_.store(rep, std::memory_order_relaxed);
    const Clock::rep tick = std::chrono::duration_cast<Clock::duration>(params_.tick).count();
    auto due = [&] {
        const Clock::rep last = last_eval_.load(std::memory_order_relaxed);
        return last == NEVER || rep - last >= tick;
    };
    if (!due())
        return;
    std::unique_lock<std::mutex> lk(eval_mtx_, std::try_to_lock);
    if (!lk.owns_lock() || !due())
        return;
    try {
        Evaluate(t);
    } catch (const std::exception &e) {
        WarnFailure(rep, e.what());
    } catch (...) {
        WarnFailure(rep, "unknown exception");
    }
    last_eval_.store(rep, std::memory_order_relaxed);
}

std::string LiveWidenGovernor::ReasonText(GovernorReason reason) const {
    switch (reason) {
        case GovernorReason::kStart:
            return "starting (no measurement yet)";
        case GovernorReason::kNone:
            return "not throttled";
        case GovernorReason::kRampUp:
            return "ramping up";
        case GovernorReason::kForegroundCpu:
            return fmt::format("foreground uses {:.1f} of {} CPUs (headroom {})", fg_cores_, params_.cores,
                               params_.headroom_cores);
        case GovernorReason::kWriterWait:
            return fmt::format("writers waited {} ms per second (brake at {} ms/s)", brake_amount_ / 1000,
                               params_.wait_brake_us / 1000);
        case GovernorReason::kLongWaits:
            return fmt::format("{} writer waits of >= {} ms since the last evaluation", brake_amount_,
                               XStoreTunables::LIVE_WIDEN_WRITER_LONG_WAIT_US / 1000);
    }
    return {};
}

void LiveWidenGovernor::Evaluate(Clock::time_point now) {
    Signals s = sample_();
    s.builder_cpu_s += static_cast<double>(builder_cpu_ns_.load(std::memory_order_relaxed)) * 1e-9;
    const unsigned floor_w = params_.floor_workers;
    const unsigned max_w = params_.max_workers;
    const std::string &label = params_.label;

    if (!have_baseline_) {
        have_baseline_ = true;
        prev_ = s;
        rebaseline_writers_.store(false, std::memory_order_relaxed);
        window_.push_back({now, s.busy_cpu_s, s.builder_cpu_s});
        zero_since_ = now;
        at_floor_ = max_w > floor_w;
        floor_since_ = now;
        return;
    }
    const Point last = window_.back();
    const double d_wall = Seconds(now - last.t);
    if (d_wall <= 0)
        return;

    // Each writer counter on its own delta: the three are not a consistent snapshot (Tech Reference 4.18). After a
    // switch to another year file the new file's values are only the baseline.
    uint64_t d_wait_us = 0;
    uint64_t d_long = 0;
    if (!rebaseline_writers_.exchange(false, std::memory_order_relaxed)) {
        d_wait_us = CounterDelta(s.writer_wait_us, prev_.writer_wait_us);
        d_long = CounterDelta(s.long_waits, prev_.long_waits);
    }
    prev_ = s;
    // Evaluations happen only when a worker calls Permit, so the interval varies: the wait brake is a rate.
    const double wait_us_per_s = static_cast<double>(d_wait_us) / d_wall;

    const double fg_tick = std::max(0.0, ((s.busy_cpu_s - last.busy_s) - (s.builder_cpu_s - last.builder_s)) / d_wall);
    window_.push_back({now, s.busy_cpu_s, s.builder_cpu_s});
    if (window_.size() > params_.smoothing_ticks + 1)
        window_.erase(window_.begin());
    const Point &first = window_.front();
    const double d_window = Seconds(now - first.t);   // > 0: every kept point is later than the one before
    const double fg_window =
        std::max(0.0, ((s.busy_cpu_s - first.busy_s) - (s.builder_cpu_s - first.builder_s)) / d_window);
    // Drops follow the last tick at once; a raise needs the whole window quiet.
    fg_cores_ = std::max(fg_tick, fg_window);

    const double raw = std::floor(static_cast<double>(params_.cores) - fg_cores_ -
                                  static_cast<double>(params_.headroom_cores));
    cpu_target_ = raw <= floor_w ? floor_w : raw >= max_w ? max_w : static_cast<unsigned>(raw);

    GovernorReason trigger = GovernorReason::kNone;
    if (params_.wait_brake_us != 0 && wait_us_per_s >= static_cast<double>(params_.wait_brake_us)) {
        trigger = GovernorReason::kWriterWait;
        brake_amount_ = static_cast<uint64_t>(wait_us_per_s);
    } else if (params_.long_waits_brake != 0 && d_long >= params_.long_waits_brake) {
        trigger = GovernorReason::kLongWaits;
        brake_amount_ = d_long;
    }

    const unsigned allowed = allowed_.load(std::memory_order_relaxed);
    unsigned next = allowed;
    if (trigger != GovernorReason::kNone) {
        const bool new_brake = now >= brake_until_;
        brake_reason_ = trigger;
        brake_until_ = now + params_.brake_hold;
        next = floor_w;
        if (new_brake)
            ++brakes_;
        if (allowed != floor_w)
            spdlog::info("[LIVE WIDEN] [{}] - Writer brake: workers {} -> {} for {} s ({})", label, allowed, next,
                         params_.brake_hold.count(), ReasonText(trigger));
        else if (new_brake)   // already at the floor: nothing changes but the hold, so debug only
            spdlog::debug("[LIVE WIDEN] [{}] - Writer brake at the floor of {} worker(s) for {} s ({})", label,
                          floor_w, params_.brake_hold.count(), ReasonText(trigger));
    } else if (allowed > cpu_target_) {
        next = cpu_target_;
        // A one-step drop is usually the target hovering on an integer boundary: debug, so the log does not flap.
        const auto level = next == floor_w || allowed - next >= 2 ? spdlog::level::info : spdlog::level::debug;
        spdlog::log(level, "[LIVE WIDEN] [{}] - Workers {} -> {} ({})", label, allowed, next,
                    ReasonText(GovernorReason::kForegroundCpu));
    } else if (allowed < cpu_target_ && now >= brake_until_) {
        next = allowed + 1;
        if (next == max_w)
            spdlog::info("[LIVE WIDEN] [{}] - Workers {} -> {} (the maximum)", label, allowed, next);
        else
            spdlog::debug("[LIVE WIDEN] [{}] - Workers {} -> {} (ramping up)", label, allowed, next);
    }
    allowed_.store(next, std::memory_order_relaxed);

    // Floor 0 only (next >= floor_w >= 1 otherwise): a one-group grant to worker 0 after starvation_guard at 0.
    if (next != 0) {
        starvation_grant_.store(false, std::memory_order_relaxed);
    } else if (allowed != 0) {
        zero_since_ = now;
    } else if (now - zero_since_ >= params_.starvation_guard &&
               !starvation_grant_.load(std::memory_order_relaxed)) {
        zero_since_ = now;
        starvation_grant_.store(true, std::memory_order_relaxed);
        spdlog::info("[LIVE WIDEN] [{}] - No worker allowed for {} s; granting worker 0 one group", label,
                     params_.starvation_guard.count());
    }

    GovernorReason reason = GovernorReason::kNone;
    if (now < brake_until_)
        reason = brake_reason_;
    else if (next < cpu_target_)
        reason = GovernorReason::kRampUp;
    else if (next < max_w)
        reason = GovernorReason::kForegroundCpu;

    uint64_t warned = 0;
    double parked_s = 0;
    if (next == floor_w && max_w > floor_w) {
        if (!at_floor_) {
            at_floor_ = true;
            floor_since_ = now;
        }
        parked_s = Seconds(now - floor_since_);
        if (!floor_warned_ && now - floor_since_ >= params_.at_floor_warn) {
            std::string left;
            if (progress_) {
                try {   // caught here so a broken progress callback costs only the percent, not the evaluation
                    left = fmt::format("; {:.1f}% of the year left",
                                       100.0 * (1.0 - std::clamp(progress_(), 0.0, 1.0)));
                } catch (...) {
                    left.clear();
                }
            }
            spdlog::warn("[LIVE WIDEN] [{}] - Held at the floor of {} worker(s) for {:.0f} s ({}){}", label, floor_w,
                         parked_s, ReasonText(reason), left);
            floor_warned_ = true;   // after the line: a throw above retries the warning next tick
            warned = 1;
        }
    } else {
        at_floor_ = false;
        floor_warned_ = false;
    }

    std::lock_guard<std::mutex> lk(snap_mtx_);
    published_.cpu_target = cpu_target_;
    published_.brake_active = now < brake_until_;
    published_.reason = reason;
    published_.reason_text = ReasonText(reason);
    published_.fg_cores = fg_cores_;
    published_.fg_tick = fg_tick;
    published_.wait_us_per_s = wait_us_per_s;
    published_.long_waits_delta = d_long;
    published_.parked_seconds = parked_s;
    published_.min_allowed_seen = std::min(published_.min_allowed_seen, next);
    published_.max_allowed_seen = std::max(published_.max_allowed_seen, next);
    published_.brakes = brakes_;
    ++published_.evaluations;
    published_.warnings_emitted += warned;
}

LiveWidenGovernor::Snapshot LiveWidenGovernor::GetSnapshot() const {
    Snapshot snap;
    {
        std::lock_guard<std::mutex> lk(snap_mtx_);
        snap = published_;
    }
    snap.allowed = allowed_.load(std::memory_order_relaxed);
    snap.max_workers = params_.max_workers;
    snap.floor_workers = params_.floor_workers;
    snap.active = active_.load(std::memory_order_relaxed);
    snap.peak_active = peak_active_.load(std::memory_order_relaxed);
    snap.throttled = snap.allowed < snap.max_workers;
    snap.grants = grants_.load(std::memory_order_relaxed);
    snap.denials = denials_.load(std::memory_order_relaxed);
    snap.failure_warnings = failure_warnings_.load(std::memory_order_relaxed);
    return snap;
}

} // namespace livewiden
