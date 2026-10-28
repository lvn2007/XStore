#pragma once
// One process-wide pool of server memory for XStore's operations. An operation that can allocate a lot (an insert, a query, an
// aggregate, an index rebuild) reserves an estimate of it BEFORE allocating, with Acquire(); if the pool has no room the caller
// waits its turn (first come, first served) instead of the server running the machine out of memory. The reservation is a
// MemoryLease: whoever keeps the memory alive holds it (a request handler and the WAL records it created co-own one through a
// shared_ptr), and the bytes go back to the pool when the last owner lets go.
//
// A request is admitted only if BOTH hold:
//   * the pool's own total stays within `limit` (the memory limit of the machine or container minus `reserve`), and
//   * the OS reports at least `bytes + reserve` available, not counting reservations whose memory is not allocated yet
//     (those have not reached the OS's figure; see MemoryLease::MarkResident).
// so a reserve of 1-2 GB is always left for the host, and other processes using memory make inserts wait instead of fail.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>

namespace xstore {

class MemoryPool;

class MemoryLease {
public:
    MemoryLease(MemoryPool &pool, uint64_t bytes) : pool_(pool), bytes_(bytes) {}
    ~MemoryLease();
    MemoryLease(const MemoryLease &) = delete;
    MemoryLease &operator=(const MemoryLease &) = delete;
    // The memory this lease paid for has now been allocated and touched, so the OS's "available" figure already reflects it
    // and the pool stops subtracting it as pending. Optional (the lease then counts as pending until it is released).
    void MarkResident();
    uint64_t bytes() const { return bytes_; }

private:
    MemoryPool &pool_;
    const uint64_t bytes_;
    std::atomic<bool> resident_{false};
};

class MemoryPool {
public:
    enum class Result { kGranted, kTooLarge, kTimedOut };
    using Clock = std::chrono::steady_clock;
    // Bytes the OS (and the container, if any) still has available; UINT64_MAX when unknown.
    using AvailableFn = std::function<uint64_t()>;
    static constexpr uint64_t kUnknown = UINT64_MAX;

    // `limit`: most the leases may add up to. `reserve`: left to the host. `available`: nullptr = read /proc/meminfo and the cgroup.
    MemoryPool(uint64_t limit, uint64_t reserve, AvailableFn available = nullptr);
    // The pool the server uses; sized on first use from the machine/container memory (env XSTORE_MEMORY_LIMIT_MB,
    // XSTORE_MEMORY_RESERVE_MB override). Never destroyed: a WAL record may outlive static destruction.
    static MemoryPool &Instance();

    // Reserves `bytes`, waiting first come, first served until the pool has room or `deadline` passes (Clock::time_point::max():
    // wait as long as it takes). kTooLarge: more than the pool's limit, it could never be admitted (nothing was reserved).
    Result Acquire(uint64_t bytes, Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease);

    uint64_t Limit() const { return limit_; }
    uint64_t Reserve() const { return reserve_; }
    uint64_t UsedBytes() const;
    uint64_t PendingBytes() const;
    uint64_t PeakBytes() const;
    size_t WaitingCount() const;

private:
    friend class MemoryLease;
    void Release(uint64_t bytes, bool resident);
    void ResidentNow(uint64_t bytes);
    bool FitsLocked(uint64_t bytes);

    const uint64_t limit_;
    const uint64_t reserve_;
    AvailableFn available_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    uint64_t used_ = 0;
    uint64_t pending_ = 0;
    uint64_t peak_ = 0;
    uint64_t next_ticket_ = 0;
    std::list<uint64_t> waiting_;   // tickets in arrival order; only the front may be admitted
};

}  // namespace xstore
