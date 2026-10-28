#include "include/XStore/XStore_MemoryPool.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

namespace xstore {

namespace {

constexpr uint64_t kBytesPerKB = 1024;
constexpr uint64_t kDefaultReserveMB = 2048;

uint64_t EnvOverrideMB(const char *name) {
    const char *value = std::getenv(name);
    if (!value || !*value) return 0;
    char *end = nullptr;
    const unsigned long long parsed = std::strtoull(value, &end, 10);
    return (end && *end == '\0') ? static_cast<uint64_t>(parsed) : 0;
}

// Reads an "<Key>   <N> kB" line from /proc/meminfo; 0 if not found or unreadable.
uint64_t ReadMeminfoField(const std::string &key) {
    std::ifstream f("/proc/meminfo");
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind(key, 0) == 0) {
            uint64_t kb = 0;
            std::istringstream iss(line.substr(key.size()));
            iss >> kb;
            return kb * kBytesPerKB;
        }
    }
    return 0;
}

uint64_t ReadMemTotalBytes() { return ReadMeminfoField("MemTotal:"); }
uint64_t ReadMemAvailableBytes() { return ReadMeminfoField("MemAvailable:"); }

// cgroup v2 memory.max; 0 when unreadable or "max" (unlimited).
uint64_t ReadCgroupMaxBytes() {
    std::ifstream f("/sys/fs/cgroup/memory.max");
    if (!f) return 0;
    std::string value;
    f >> value;
    if (value.empty() || value == "max") return 0;
    return std::strtoull(value.c_str(), nullptr, 10);
}

uint64_t ReadCgroupCurrentBytes() {
    std::ifstream f("/sys/fs/cgroup/memory.current");
    if (!f) return 0;
    uint64_t value = 0;
    f >> value;
    return value;
}

// /proc/meminfo MemAvailable, capped by the cgroup v2 headroom (memory.max - memory.current) when a limit is set.
uint64_t ReadHostAvailableBytes() {
    const uint64_t available = ReadMemAvailableBytes();
    if (available == 0) return MemoryPool::kUnknown;
    const uint64_t cgroup_max = ReadCgroupMaxBytes();
    if (cgroup_max == 0) return available;
    const uint64_t current = ReadCgroupCurrentBytes();
    const uint64_t headroom = cgroup_max > current ? cgroup_max - current : 0;
    return std::min(available, headroom);
}

}  // namespace

MemoryLease::~MemoryLease() { pool_.Release(bytes_, resident_.load()); }

void MemoryLease::MarkResident() {
    if (!resident_.exchange(true)) pool_.ResidentNow(bytes_);
}

MemoryPool::MemoryPool(uint64_t limit, uint64_t reserve, AvailableFn available)
    : limit_(limit), reserve_(reserve), available_(available ? std::move(available) : AvailableFn(ReadHostAvailableBytes)) {}

MemoryPool &MemoryPool::Instance() {
    static MemoryPool *pool = [] {
        uint64_t reserve_mb = EnvOverrideMB("XSTORE_MEMORY_RESERVE_MB");
        if (reserve_mb == 0) reserve_mb = kDefaultReserveMB;
        const uint64_t reserve = reserve_mb * kBytesPerKB * kBytesPerKB;

        const uint64_t limit_mb = EnvOverrideMB("XSTORE_MEMORY_LIMIT_MB");
        uint64_t limit;
        if (limit_mb > 0) {
            limit = limit_mb * kBytesPerKB * kBytesPerKB;
        } else {
            uint64_t total = ReadMemTotalBytes();
            if (total == 0) total = reserve * 2;  // unreadable host figure: fall back to a small, safe pool
            const uint64_t cgroup_max = ReadCgroupMaxBytes();
            if (cgroup_max > 0) total = std::min(total, cgroup_max);
            limit = total > reserve ? total - reserve : total;
        }
        return new MemoryPool(limit, reserve);
    }();
    return *pool;
}

bool MemoryPool::FitsLocked(uint64_t bytes) {
    if (used_ + bytes > limit_) return false;
    const uint64_t host_available = available_();
    if (host_available == kUnknown) return true;
    const uint64_t effective = host_available > pending_ ? host_available - pending_ : 0;
    return effective >= bytes + reserve_;
}

MemoryPool::Result MemoryPool::Acquire(uint64_t bytes, Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease) {
    lease.reset();
    if (bytes > limit_) return Result::kTooLarge;

    constexpr auto kPollInterval = std::chrono::milliseconds(50);
    std::unique_lock<std::mutex> lk(mutex_);

    if (waiting_.empty() && FitsLocked(bytes)) {
        used_ += bytes;
        pending_ += bytes;
        peak_ = std::max(peak_, used_);
    } else {
        const uint64_t ticket = next_ticket_++;
        waiting_.push_back(ticket);
        for (;;) {
            const bool forever = deadline == Clock::time_point::max();
            if (!forever && Clock::now() >= deadline) {
                waiting_.remove(ticket);
                cv_.notify_all();
                return Result::kTimedOut;
            }
            if (waiting_.front() == ticket && FitsLocked(bytes)) {
                waiting_.pop_front();
                used_ += bytes;
                pending_ += bytes;
                peak_ = std::max(peak_, used_);
                cv_.notify_all();
                break;
            }
            const auto wait_for = forever ? kPollInterval
                                           : std::min(kPollInterval, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()));
            cv_.wait_for(lk, wait_for);
        }
    }

    lk.unlock();
    lease = std::make_shared<MemoryLease>(*this, bytes);
    return Result::kGranted;
}

uint64_t MemoryPool::UsedBytes() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return used_;
}
uint64_t MemoryPool::PendingBytes() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return pending_;
}
uint64_t MemoryPool::PeakBytes() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return peak_;
}
size_t MemoryPool::WaitingCount() const {
    std::lock_guard<std::mutex> lk(mutex_);
    return waiting_.size();
}

void MemoryPool::Release(uint64_t bytes, bool resident) {
    std::lock_guard<std::mutex> lk(mutex_);
    used_ -= bytes;
    if (!resident) pending_ -= bytes;
    cv_.notify_all();
}

void MemoryPool::ResidentNow(uint64_t bytes) {
    std::lock_guard<std::mutex> lk(mutex_);
    pending_ -= bytes;
    cv_.notify_all();
}

}  // namespace xstore
