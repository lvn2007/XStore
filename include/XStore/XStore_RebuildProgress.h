#pragma once
// Progress display of the startup index rebuild (XStore::RebuildYearsIndexes, Tech Reference 4.14): the pure
// formatting / ETA arithmetic, and a reporter thread that only READS the rebuild's counters.
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iosfwd>
#include <optional>
#include <string>
#include <thread>
#include <condition_variable>
#include <mutex>
#include "include/XStore/XStore_Tunables.h"

namespace rebuildprogress {

// Rows are ROWS COVERED (holes included), so a year's rows_done reaches its total_rows.
struct Sample {
    uint64_t rows_done = 0;
    uint64_t rows_total = 0;
    size_t years_done = 0;
    size_t years_total = 0;
    double elapsed_s = 0;
    // Rows gained over the last window_s seconds (the reporter keeps up to RATE_WINDOW_SEC); window_s == 0 means no
    // window yet: the overall average (rows_done / elapsed_s) is used.
    uint64_t window_rows = 0;
    double window_s = 0;
    bool finished = false; // the last sample, taken after the workers stopped
};

inline constexpr double RATE_WINDOW_SEC = 30.0;

[[nodiscard]] double PercentDone(const Sample &sample);             // floor, clamped to [0, 100]
[[nodiscard]] double RowsPerSecond(const Sample &sample);           // window rate, else the overall average
[[nodiscard]] std::optional<double> EstimateSecondsLeft(const Sample &sample); // nullopt: unknown yet
[[nodiscard]] std::string FormatDuration(double seconds);           // 42s, 3m10s, 1h02m
[[nodiscard]] std::string FormatSi(double value);                   // 950, 13.0K, 41.7M, 1.2G
// "31% (13.0M of 41.7M rows), 3/11 years done, 2.1M rows/s, elapsed 3m10s, about 7m00s left"
[[nodiscard]] std::string FormatRebuildProgress(const Sample &sample);
// "3/11 years  41.2M rows/s" (the bar itself shows the percentage, elapsed and remaining time)
[[nodiscard]] std::string FormatBarPostfix(const Sample &sample);

enum class Mode { kAuto, kBar, kLog }; // kAuto: bar on a terminal (not TERM=dumb), spdlog lines elsewhere

struct Options {
    Mode mode = Mode::kAuto;
    std::chrono::milliseconds log_interval{XStoreTunables::REBUILD_PROGRESS_LOG_INTERVAL_SEC * 1000};
    std::chrono::milliseconds bar_interval{XStoreTunables::REBUILD_PROGRESS_BAR_INTERVAL_MS};
    std::function<void(const std::string &)> log_line; // empty: spdlog::info
    std::ostream *bar_stream = nullptr;                // null: std::cout
};

// One extra thread, started by the constructor and joined by Stop() / the destructor (the stop signal is a condition
// variable, so the join is immediate). It calls `sampler` (counters only: rows_*, years_*) every interval and once
// more, with finished = true, when stopped. Nothing it does can change the rebuild's outcome: an exception inside the
// thread is logged once at warn level and swallowed, and a thread that cannot start just means no progress display.
class Reporter {
public:
    using Sampler = std::function<Sample()>;
    Reporter(std::string label, Sampler sampler, Options options);
    Reporter(const Reporter &) = delete;
    Reporter &operator=(const Reporter &) = delete;
    ~Reporter() { Stop(); }

    void Stop() noexcept;

private:
    void Run() noexcept;
    void Loop(bool bar_mode);

    std::string label_;
    Sampler sampler_;
    Options options_;
    std::mutex mutex_;
    std::condition_variable stop_cv_;
    bool stop_ = false;
    std::thread thread_;
};

} // namespace rebuildprogress
