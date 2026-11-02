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
// so a reserve (an eighth of the machine's memory, 128 MiB to 2 GB) is always left for the host, and other processes using memory make inserts wait instead of fail.
// The one exception is progress: with no other lease outstanding the front request is admitted without the second check. The
// allocator keeps memory it has freed, so the OS's figure may never recover once the work that used it is done; waiting for it
// then would never end. The pool limit still applies.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>

namespace xstore {

// The memory limit and headroom of the cgroup (v2) the process runs in: `proc_cgroup_file` (/proc/self/cgroup) names it, and every
// ancestor up to `cgroup_root` (/sys/fs/cgroup) is considered too. `max` is the tightest memory.max (0: no limit anywhere),
// `headroom` the smallest memory.max - memory.current over the limited levels (UINT64_MAX: no limit anywhere).
struct CgroupMemory {
    uint64_t max = 0;
    uint64_t headroom = UINT64_MAX;
};
CgroupMemory ReadCgroupMemory(const std::string &cgroup_root, const std::string &proc_cgroup_file);

// The memory left to the host when the machine (or container) has `total_bytes` (0 = unknown): an eighth of it, between 128 MiB and 2 GiB, so a
// big server keeps 2 GiB and a small device (a Raspberry Pi) keeps a pool.
uint64_t DefaultReserveBytes(uint64_t total_bytes);

// What the process-wide pool is sized to: `limit` (most the leases may add up to) and `reserve` (left to the host). `total_bytes` is the machine's or
// container's memory (0 = unknown); the overrides are XSTORE_MEMORY_RESERVE_MB / XSTORE_MEMORY_LIMIT_MB (0 = not set).
struct PoolSizing {
    uint64_t limit = 0;
    uint64_t reserve = 0;
};
PoolSizing SizePool(uint64_t total_bytes, uint64_t reserve_override_mb, uint64_t limit_override_mb);

class MemoryPool;

class MemoryLease {
public:
    // `unseen`: the part of `bytes` the OS's available figure does not reflect yet (all of it, unless the caller already held some).
    MemoryLease(MemoryPool &pool, uint64_t bytes, uint64_t unseen) : pool_(pool), bytes_(bytes), unseen_(unseen) {}
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
    const uint64_t unseen_;
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
    // `host_cache`: how long a reading of `available` is reused (zero: read it for every decision).
    MemoryPool(uint64_t limit, uint64_t reserve, AvailableFn available = nullptr, Clock::duration host_cache = Clock::duration::zero());
    // The pool the server uses; sized on first use from the machine/container memory (env XSTORE_MEMORY_LIMIT_MB,
    // XSTORE_MEMORY_RESERVE_MB override). Never destroyed: a WAL record may outlive static destruction.
    static MemoryPool &Instance();

    // Reserves `bytes`, waiting first come, first served until the pool has room or `deadline` passes (Clock::time_point::max():
    // wait as long as it takes). kTooLarge: more than the pool's limit, it could never be admitted (nothing was reserved).
    // `already_resident`: bytes of `bytes` the caller already holds in memory (a request's received message), so the OS's figure has
    // them already; they count against the pool limit but not against the host's available memory.
    Result Acquire(uint64_t bytes, Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease, uint64_t already_resident = 0);

    // Memory that exists already and cannot wait for its turn, e.g. the bytes of a reply queued for a slow client. It counts against `limit`
    // like a lease (a request that would not fit next to it waits; UsedBytes() includes it), but taking it is never refused or delayed, so the pool
    // can end up Full() or past its limit: whoever produces such memory is expected to stop producing while Full(). Lock-free; Uncharge wakes the
    // requests in the line. Every Charge must be matched by an Uncharge of the same bytes.
    void Charge(uint64_t bytes) noexcept;
    void Uncharge(uint64_t bytes) noexcept;
    // The leases and the charged bytes together have reached the limit, or the OS has less than `reserve` available (memory the pool does not count:
    // the allocator's retention, caches, other processes). A producer of charged bytes stops while Full(); it is asked at every chunk boundary, so the
    // OS figure is read at most once per `host_cache`.
    bool Full() const;

    uint64_t Limit() const { return limit_; }
    uint64_t Reserve() const { return reserve_; }
    uint64_t UsedBytes() const;
    uint64_t PendingBytes() const;
    uint64_t PeakBytes() const;
    size_t WaitingCount() const;

private:
    friend class MemoryLease;
    void Release(uint64_t bytes, uint64_t unseen, bool resident);
    void ResidentNow(uint64_t unseen);
    bool FitsLocked(uint64_t bytes, uint64_t unseen);
    uint64_t HostAvailableLocked() const;

    const uint64_t limit_;
    const uint64_t reserve_;
    const Clock::duration host_cache_;
    AvailableFn available_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    uint64_t used_ = 0;
    std::atomic<uint64_t> external_{0};   // Charge / Uncharge
    std::atomic<size_t> waiters_{0};      // requests in the line (waiting_.size(), readable without the mutex)
    uint64_t pending_ = 0;
    uint64_t peak_ = 0;
    mutable bool host_cached_ = false;   // the cached OS figure; refreshed by Full() too
    mutable uint64_t host_available_ = 0;
    mutable Clock::time_point host_cached_at_;
    uint64_t next_ticket_ = 0;
    std::list<uint64_t> waiting_;   // tickets in arrival order; only the front may be admitted
};

// Reservation for the memory a query-time operation holds while it runs: a streamed RangeQuery/BatchQuery reply (held in
// memory until its last chunk is sent, ZMQ delivers a multipart reply atomically) or the working set of an exact aggregate
// (every value in the range). `estimate` is reserved for as long as the returned lease lives. An estimate below
// XStoreTunables::OPERATION_LEASE_MIN_BYTES skips the pool (kGranted, `lease` empty). The memory is built gradually while the
// operation runs, so the lease is marked resident at once: the pool limit bounds the sum, the OS figure follows by itself.
MemoryPool::Result AcquireOperationLease(uint64_t estimate, MemoryPool::Clock::time_point deadline, std::shared_ptr<MemoryLease> &lease);
// The status text for an operation refused by AcquireOperationLease, e.g. "[RANGE QUERY] [DB: X] - rejected: ...".
std::string OperationLeaseRejection(const char *op, const std::string &db_name, MemoryPool::Result result, uint64_t estimate);

}  // namespace xstore
