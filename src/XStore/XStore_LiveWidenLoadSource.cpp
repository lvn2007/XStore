#include "include/XStore/XStore_LiveWidenLoadSource.h"
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_CpuJiffies.h"

#include <unistd.h>

#include <algorithm>
#include <thread>
#include <utility>

namespace livewiden {

LiveWidenLoadSource::LiveWidenLoadSource(std::string proc_stat)
    : proc_stat_(std::move(proc_stat)),
      seconds_per_jiffy_(1.0 / static_cast<double>(std::max(1L, sysconf(_SC_CLK_TCK)))),
      cores_(std::max(1u, std::thread::hardware_concurrency())) {}

void LiveWidenLoadSource::SetYear(const FileRangeLock *lock, const std::atomic<uint64_t> *rows_done,
                                  uint64_t total_rows) noexcept {
    lock_.store(lock, std::memory_order_relaxed);
    total_rows_.store(total_rows, std::memory_order_relaxed);
    rows_done_.store(rows_done, std::memory_order_relaxed);
}

Signals LiveWidenLoadSource::Sample() noexcept {
    uint64_t active = 0;
    uint64_t total = 0;
    try {
        sysstats::ReadCpuJiffies(active, total, proc_stat_.c_str());
    } catch (...) {
        total = 0;
    }
    if (total != 0)   // 0: unreadable or unparsable, keep the previous value
        last_.busy_cpu_s = static_cast<double>(active) * seconds_per_jiffy_;
    if (const FileRangeLock *lock = lock_.load(std::memory_order_relaxed)) {
        const FileRangeLock::WriterWaitStats w = lock->WriterWaits();
        last_.writer_wait_us = w.total_us;
        last_.writer_waits = w.waits;
        last_.long_waits = w.long_waits;
    }
    return last_;
}

double LiveWidenLoadSource::Progress() const noexcept {
    const std::atomic<uint64_t> *done = rows_done_.load(std::memory_order_relaxed);
    const uint64_t total = total_rows_.load(std::memory_order_relaxed);
    if (done == nullptr || total == 0) return 0;
    return std::min(1.0, static_cast<double>(done->load(std::memory_order_relaxed)) / static_cast<double>(total));
}

std::unique_ptr<LiveWidenGovernor> MakeLiveWidenGovernor(LiveWidenLoadSource &load, std::string label) {
    GovernorParams p;
    p.cores = load.Cores();
    p.label = std::move(label);
    return std::make_unique<LiveWidenGovernor>(
        std::move(p), [&load] { return load.Sample(); }, [] { return LiveWidenGovernor::Clock::now(); },
        [&load] { return load.Progress(); });
}

} // namespace livewiden
