#include "include/XStore/XStore_RebuildProgress.h"
#include "include/XStore/XStore_ProgressBar.h"
#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <utility>
#include <unistd.h>

namespace rebuildprogress {

double PercentDone(const Sample &sample) {
    if (sample.rows_total == 0) return 100.0;
    const uint64_t done = std::min(sample.rows_done, sample.rows_total);
    return std::floor(static_cast<long double>(done) * 100.0L / static_cast<long double>(sample.rows_total));
}

double RowsPerSecond(const Sample &sample) {
    if (sample.window_s > 0) return static_cast<double>(sample.window_rows) / sample.window_s;
    return sample.elapsed_s > 0 ? static_cast<double>(sample.rows_done) / sample.elapsed_s : 0.0;
}

std::optional<double> EstimateSecondsLeft(const Sample &sample) {
    if (sample.rows_done == 0) return std::nullopt;
    if (sample.rows_done >= sample.rows_total) return 0.0;
    const double rate = RowsPerSecond(sample);
    if (rate <= 0) return std::nullopt;
    return static_cast<double>(sample.rows_total - sample.rows_done) / rate;
}

std::string FormatDuration(double seconds) {
    const long long s = std::llround(std::max(0.0, seconds));
    if (s < 60) return fmt::format("{}s", s);
    if (s < 3600) return fmt::format("{}m{:02}s", s / 60, s % 60);
    return fmt::format("{}h{:02}m", s / 3600, (s % 3600) / 60);
}

std::string FormatSi(double value) {
    if (!(value > 0)) return "0";
    if (value < 999.5) return fmt::format("{}", std::llround(value));
    static constexpr char SUFFIX[] = {'K', 'M', 'G', 'T'};
    value /= 1000;
    size_t unit = 0;
    while (value >= 999.95 && unit + 1 < sizeof(SUFFIX)) {
        value /= 1000;
        ++unit;
    }
    return fmt::format("{:.1f}{}", value, SUFFIX[unit]);
}

std::string FormatRebuildProgress(const Sample &sample) {
    std::string tail;
    const std::optional<double> left = EstimateSecondsLeft(sample);
    if (sample.finished && sample.rows_done < sample.rows_total) {
        tail = "stopped before the end";
    } else if (!left) {
        tail = "estimating...";
    } else if (*left <= 0) {
        tail = "done";
    } else {
        tail = fmt::format("about {} left", FormatDuration(*left));
    }
    return fmt::format("{:.0f}% ({} of {} rows), {}/{} years done, {} rows/s, elapsed {}, {}", PercentDone(sample),
                       FormatSi(static_cast<double>(sample.rows_done)), FormatSi(static_cast<double>(sample.rows_total)),
                       sample.years_done, sample.years_total, FormatSi(RowsPerSecond(sample)),
                       FormatDuration(sample.elapsed_s), tail);
}

std::string FormatBarPostfix(const Sample &sample) {
    return fmt::format("{}/{} years  {} rows/s", sample.years_done, sample.years_total, FormatSi(RowsPerSecond(sample)));
}

namespace {
bool StdoutIsInteractiveTerminal() {
    const char *term = std::getenv("TERM");
    return isatty(STDOUT_FILENO) != 0 && term != nullptr && std::strcmp(term, "dumb") != 0;
}
} // namespace

Reporter::Reporter(std::string label, Sampler sampler, Options options)
    : label_(std::move(label)), sampler_(std::move(sampler)), options_(std::move(options)) {
    try {
        thread_ = std::thread([this] { Run(); });
    } catch (const std::exception &e) {
        spdlog::warn("[INDEX REBUILD] [DB: {}] - No progress display: {}", label_, e.what());
    }
}

void Reporter::Stop() noexcept {
    try {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        stop_cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    } catch (...) {
        // Nothing to undo: the display is best effort.
    }
}

void Reporter::Run() noexcept {
    try {
        const bool bar_mode = options_.mode == Mode::kBar || (options_.mode == Mode::kAuto && StdoutIsInteractiveTerminal());
        Loop(bar_mode);
    } catch (const std::exception &e) {
        try {
            spdlog::warn("[INDEX REBUILD] [DB: {}] - Progress display stopped: {}", label_, e.what());
        } catch (...) {
        }
    } catch (...) {
        try {
            spdlog::warn("[INDEX REBUILD] [DB: {}] - Progress display stopped", label_);
        } catch (...) {
        }
    }
}

void Reporter::Loop(bool bar_mode) {
    using Clock = std::chrono::steady_clock;
    const Clock::time_point start = Clock::now();
    std::deque<std::pair<double, uint64_t>> window{{0.0, 0}}; // (elapsed_s, rows_done), oldest first

    const auto take = [&](bool finished) {
        Sample sample = sampler_();
        sample.elapsed_s = std::chrono::duration<double>(Clock::now() - start).count();
        sample.finished = finished;
        window.emplace_back(sample.elapsed_s, sample.rows_done);
        // Keep one sample at or before now - RATE_WINDOW_SEC as the baseline.
        while (window.size() > 2 && window[1].first <= sample.elapsed_s - RATE_WINDOW_SEC) window.pop_front();
        sample.window_s = sample.elapsed_s - window.front().first;
        sample.window_rows = sample.rows_done - std::min(sample.rows_done, window.front().second);
        return sample;
    };

    std::optional<indicators::ProgressBar> bar;
    if (bar_mode) {
        bar.emplace(indicators::option::BarWidth{40}, indicators::option::PrefixText{"Index rebuild "},
                    indicators::option::ShowPercentage{true}, indicators::option::ShowElapsedTime{true},
                    indicators::option::ShowRemainingTime{true},
                    indicators::option::Stream{options_.bar_stream != nullptr ? *options_.bar_stream : std::cout});
        bar->set_progress(0); // starts the bar's own clock; the cursor is never hidden
    }
    const auto show = [&](const Sample &sample) {
        if (bar) {
            if (bar->is_completed()) return; // reached 100%: the library already ended the line
            bar->set_option(indicators::option::PostfixText{FormatBarPostfix(sample)});
            bar->set_progress(static_cast<size_t>(PercentDone(sample)));
            if (sample.finished && !bar->is_completed()) bar->mark_as_completed(); // a stopped rebuild: end the line
        } else {
            const std::string line = fmt::format("[INDEX REBUILD] [DB: {}] - {}", label_, FormatRebuildProgress(sample));
            if (options_.log_line) {
                options_.log_line(line);
            } else {
                spdlog::info("{}", line);
            }
        }
    };

    const std::chrono::milliseconds interval = bar_mode ? options_.bar_interval : options_.log_interval;
    std::unique_lock<std::mutex> lock(mutex_);
    while (!stop_cv_.wait_for(lock, interval, [&] { return stop_; })) {
        lock.unlock();
        show(take(false));
        lock.lock();
    }
    lock.unlock();
    show(take(true));
}

} // namespace rebuildprogress
