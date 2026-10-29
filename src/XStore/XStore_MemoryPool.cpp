#include "include/XStore/XStore_MemoryPool.h"
#include "include/XStore/XStore_Tunables.h"

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

namespace xstore {

namespace {

constexpr uint64_t kBytesPerKB = 1024;
constexpr uint64_t kDefaultReserveMB = 2048;
// Waiters re-check every 50 ms anyway, so a reading this old changes no decision that was not already that slow to notice.
constexpr std::chrono::milliseconds kHostFigureCache{50};

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

// /proc/self/cgroup of a cgroup v2 process holds "0::<path>"; "" when absent (cgroup v1 or unreadable).
std::string OwnCgroupPath(const std::string &proc_cgroup_file) {
    std::ifstream f(proc_cgroup_file);
    std::string line;
    while (std::getline(f, line)) {
        if (line.rfind("0::", 0) == 0) return line.substr(3);
    }
    return "";
}

// A cgroup file holding one number; false when absent or "max" (no limit).
bool ReadCgroupNumber(const std::string &path, uint64_t &out) {
    std::ifstream f(path);
    std::string value;
    if (!(f >> value) || value == "max") return false;
    char *end = nullptr;
    out = std::strtoull(value.c_str(), &end, 10);
    return end && *end == '\0';
}

// /proc/meminfo MemAvailable, capped by the headroom of the process's own cgroup (and its ancestors) when a limit is set.
uint64_t ReadHostAvailableBytes() {
    const uint64_t available = ReadMemAvailableBytes();
    if (available == 0) return MemoryPool::kUnknown;
    return std::min(available, ReadCgroupMemory("/sys/fs/cgroup", "/proc/self/cgroup").headroom);
}

}  // namespace

CgroupMemory ReadCgroupMemory(const std::string &cgroup_root, const std::string &proc_cgroup_file) {
    CgroupMemory result;
    std::string path = OwnCgroupPath(proc_cgroup_file);
    for (;;) {
        const std::string dir = cgroup_root + path;
        uint64_t max = 0, current = 0;
        if (ReadCgroupNumber(dir + "/memory.max", max)) {
            ReadCgroupNumber(dir + "/memory.current", current);
            result.max = result.max == 0 ? max : std::min(result.max, max);
            result.headroom = std::min(result.headroom, max > current ? max - current : 0);
        }
        const size_t slash = path.rfind('/');
        if (path.empty() || slash == std::string::npos) break;
        path.resize(slash);   // "/a/b" -> "/a" -> "" (the root itself)
    }
    return result;
}

MemoryLease::~MemoryLease() { pool_.Release(bytes_, unseen_, resident_.load()); }

void MemoryLease::MarkResident() {
    if (!resident_.exchange(true)) pool_.ResidentNow(unseen_);
}

MemoryPool::MemoryPool(uint64_t limit, uint64_t reserve, AvailableFn available, Clock::duration host_cache)
    : limit_(limit), reserve_(reserve), host_cache_(host_cache),
      available_(available ? std::move(available) : AvailableFn(ReadHostAvailableBytes)) {}

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
            const uint64_t cgroup_max = ReadCgroupMemory("/sys/fs/cgroup", "/proc/self/cgroup").max;
            if (cgroup_max > 0) total = std::min(total, cgroup_max);
            limit = total > reserve ? total - reserve : total;
        }
        return new MemoryPool(limit, reserve, nullptr, kHostFigureCache);
    }();
    return *pool;
}

uint64_t MemoryPool::HostAvailableLocked() {
    if (host_cache_ <= Clock::duration::zero()) return available_();
    const Clock::time_point now = Clock::now();
    if (!host_cached_ || now - host_cached_at_ >= host_cache_) {
        host_available_ = available_();
        host_cached_at_ = now;
        host_cached_ = true;
    }
    return host_available_;
}

bool MemoryPool::FitsLocked(uint64_t bytes, uint64_t unseen) {
    if (used_ + bytes > limit_) return false;
    if (used_ == 0) return true;   // nothing running: no memory is coming back, see the header
    const uint64_t host_available = HostAvailableLocked();
    if (host_available == kUnknown) return true;
    const uint64_t effective = host_available > pending_ ? host_available - pending_ : 0;
    return effective >= unseen + reserve_;
}

MemoryPool::Result MemoryPool::Acquire(uint64_t bytes, Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease, uint64_t already_resident) {
    lease.reset();
    if (bytes > limit_) return Result::kTooLarge;
    const uint64_t unseen = bytes - std::min(bytes, already_resident);

    constexpr auto kPollInterval = std::chrono::milliseconds(50);
    std::unique_lock<std::mutex> lk(mutex_);

    if (waiting_.empty() && FitsLocked(bytes, unseen)) {
        used_ += bytes;
        pending_ += unseen;
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
            if (waiting_.front() == ticket && FitsLocked(bytes, unseen)) {
                waiting_.pop_front();
                used_ += bytes;
                pending_ += unseen;
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
    lease = std::make_shared<MemoryLease>(*this, bytes, unseen);
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

void MemoryPool::Release(uint64_t bytes, uint64_t unseen, bool resident) {
    std::lock_guard<std::mutex> lk(mutex_);
    used_ -= bytes;
    if (!resident) pending_ -= unseen;
    cv_.notify_all();
}

void MemoryPool::ResidentNow(uint64_t unseen) {
    std::lock_guard<std::mutex> lk(mutex_);
    pending_ -= unseen;
    cv_.notify_all();
}

MemoryPool::Result AcquireOperationLease(uint64_t estimate, MemoryPool::Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease) {
    lease.reset();
    if (estimate < XStoreTunables::OPERATION_LEASE_MIN_BYTES) return MemoryPool::Result::kGranted;
    const MemoryPool::Result result = MemoryPool::Instance().Acquire(estimate, deadline, lease);
    if (result == MemoryPool::Result::kGranted) lease->MarkResident();
    return result;
}

std::string OperationLeaseRejection(const char *op, const std::string &db_name, MemoryPool::Result result, uint64_t estimate) {
    const char *why = result == MemoryPool::Result::kTooLarge ? "larger than the server's whole memory pool"
                                                              : "the client's deadline passed while waiting for free memory";
    return std::string("[") + op + "] [DB: " + db_name + "] - rejected: " + why + " (~" + std::to_string(estimate) +
           " bytes estimated, pool limit " + std::to_string(MemoryPool::Instance().Limit()) + " bytes)";
}

}  // namespace xstore
