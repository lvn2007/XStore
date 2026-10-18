#ifndef XSTORE_FILERANGELOCK_H
#define XSTORE_FILERANGELOCK_H

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

// In-process byte-range lock for a single file. Two ranges only block each
// other if they overlap AND at least one side is a WRITE. Disjoint ranges
// of the same file never block each other, regardless of type, so unrelated
// rows/years can be written concurrently by different worker threads.
//
// Replaces fcntl() record locks, which are scoped to the process (not the
// thread or fd) and therefore provide no mutual exclusion between the
// worker threads that make up XStore's concurrency model.
//
// It is in-process only by design: two processes on one data directory are
// refused at startup instead (XStore_DirLock.h, one flock per directory).
class FileRangeLock {
    public:
        enum class LockType { READ, WRITE };

        void LockRange(unsigned long long start, unsigned long long length, LockType type);
        void UnlockRange(unsigned long long start, unsigned long long length, LockType type);
        // LockRange that gives up after `timeout`: same semantics and length == 0 convention, waits on the same
        // condition variable. Returns false (nothing registered, nothing to unlock) if the range did not become free
        // in time. For a background job (the live index widener) that must not queue behind hot writers forever.
        [[nodiscard]] bool TryLockRangeFor(unsigned long long start, unsigned long long length, LockType type,
                                           std::chrono::milliseconds timeout);

        // A set of ranges of one type taken and released together (the WAL apply's and BatchDelete's WRITE runs,
        // BatchQuery's READ runs). Runs must be sorted ascending, non-overlapping, length > 0. All-or-nothing: the set
        // waits until no run conflicts, holding none of them meanwhile, so no deadlock and no acquire order exist. A
        // waiting WRITE set registers its hull [first, last end) as ONE waiting writer (yielding readers in the gaps
        // yield too: conservative); a waiting READ set registers nothing, like LockRange(READ). count == 1 is
        // LockRange.
        struct Run {
            unsigned long long start;
            unsigned long long length;
        };
        void LockRuns(const Run *runs, size_t count, LockType type = LockType::WRITE);
        void UnlockRuns(const Run *runs, size_t count, LockType type = LockType::WRITE);

        // What a yielding READ attempt spent on waiting writers: set if it waited for one at least once.
        struct WriterYield {
            bool yielded = false;
            uint64_t us = 0;
        };
        // The live widen builder's READ lock (Tech Reference 4.18, "Writers first"): TryLockRangeFor(READ) that ALSO
        // waits while a WRITE waiting in LockRange overlaps the range, for at most `yield_max` from the start of this
        // call; after that only held locks count, as in TryLockRangeFor. Time spent yielding does not use up
        // `timeout` (the attempt waits at most timeout + yield_max). Every other lock call ignores waiting writers.
        [[nodiscard]] bool TryReadLockYieldingFor(unsigned long long start, unsigned long long length,
                                                  std::chrono::milliseconds timeout, std::chrono::milliseconds yield_max,
                                                  WriterYield &yield);
        // Writers currently registered as waiting (slots plus overflow).
        [[nodiscard]] uint32_t WaitingWritersForTests();
        // Entries in the held-lock table (one per held range or run).
        [[nodiscard]] size_t ActiveLocksForTests();

        // Cumulative since construction, over blocking LockRange(WRITE) calls that had to wait only: total wait, number
        // of waits, and waits >= XStoreTunables::LIVE_WIDEN_WRITER_LONG_WAIT_US. Read by the live widen governor.
        struct WriterWaitStats {
            uint64_t total_us;
            uint64_t waits;
            uint64_t long_waits;
        };
        [[nodiscard]] WriterWaitStats WriterWaits() const noexcept;

    private:
        using Clock = std::chrono::steady_clock;
        struct ActiveLock {
            unsigned long long start;
            unsigned long long end;    // exclusive
            LockType type;
        };
        struct Range {
            unsigned long long start;
            unsigned long long end;    // exclusive
        };

        // Caller must hold mtx.
        bool Conflicts(unsigned long long start, unsigned long long end, LockType type) const;
        bool WaitingWriterOverlaps(unsigned long long start, unsigned long long end) const;
        bool TimedLock(unsigned long long start, unsigned long long end, LockType type, std::chrono::milliseconds timeout,
                       std::chrono::milliseconds yield_max, WriterYield *yield);
        void LockRangeSlow(std::unique_lock<std::mutex> &lk, unsigned long long start, unsigned long long end, LockType type);
        // A WRITE's wait until free(): registers [start, end) as a waiting writer and does the writer-wait accounting.
        template <typename Free>
        void WaitAsWriter(std::unique_lock<std::mutex> &lk, unsigned long long start, unsigned long long end,
                          const Free &free);

        std::mutex mtx;
        std::condition_variable cv;
        // Concurrently held ranges per file are bounded by the worker thread
        // count, so a flat vector + linear scan is cheaper than a node-based
        // container: no per-lock heap allocation and better cache locality.
        std::vector<ActiveLock> active_locks;
        // WRITEs blocked in LockRange, for TryReadLockYieldingFor only. Registered only once a writer has to wait, so
        // the uncontended path never touches them. Fixed slots, never allocated: waiting writers per file are bounded
        // by the worker threads; one that finds every slot taken counts in waiting_writer_overflow, which yielding
        // readers treat as a writer waiting on the whole file (conservative, and only a yield, never a correctness
        // matter).
        static constexpr uint32_t MAX_WAITING_WRITERS = 32;
        uint32_t waiting_writer_count = 0;
        uint32_t waiting_writer_overflow = 0;
        std::array<Range, MAX_WAITING_WRITERS> waiting_writers{};
        // Written under mtx; atomic only so WriterWaits() needs no lock. Last, so no existing member moves.
        std::atomic<uint64_t> writer_wait_us_{0};
        std::atomic<uint64_t> writer_waits_{0};
        std::atomic<uint64_t> long_waits_{0};
};

// Process-wide registry: one FileRangeLock per file path, shared across
// every worker thread (each thread owns its own XStore instance, so the
// lock table cannot live on XStore itself).
//
// Sharded across ShardCount independent {mutex, map} pairs (hash(filePath) %
// ShardCount), same pattern as XStore's cfg_shards/fd_shards/mmap_shards --
// every op (Unary/Batch/Range Insert/Query/Delete) goes through Get() on
// every call, so a single global shared_mutex here was a serialization point
// shared by ALL files, not just contending writers of the same file:
// threads touching completely unrelated files/DBs still bounced the same
// mutex's cache line. Sharding lets unrelated files' lookups proceed fully
// independently.
//
// Within a shard, reads use a shared_mutex: after a file's first access,
// every subsequent lookup takes a shared (read) lock and can proceed fully
// in parallel with other threads' lookups on that shard. Only the first
// time a given file path is touched does its shard serialize (briefly, via
// an exclusive lock).
class FileLockRegistry {
    public:
        static FileRangeLock& Get(const std::string &file_path);

    private:
        struct Shard {
            std::shared_mutex mutex;
            std::unordered_map<std::string, std::unique_ptr<FileRangeLock>> locks;
        };

        static constexpr size_t SHARD_COUNT = 16;
        static std::array<Shard, SHARD_COUNT> shards;
};

// RAII guard -- guarantees UnlockRange() runs even on early return/exception.
// This matters because a missed unlock now means a permanent hang for any
// other thread waiting on that range, instead of (as with the old fcntl
// code) a harmless no-op released automatically on fclose().
class FileRangeLockGuard {
    public:
        FileRangeLockGuard(const std::string &file_path, unsigned long long start,
                            unsigned long long length, FileRangeLock::LockType type);
        ~FileRangeLockGuard();

        FileRangeLockGuard(const FileRangeLockGuard&) = delete;
        FileRangeLockGuard& operator=(const FileRangeLockGuard&) = delete;

    private:
        FileRangeLock &lock;
        unsigned long long start;
        unsigned long long length;
        FileRangeLock::LockType type;
};

// The run list for rows at `offsets` (sorted ascending, duplicates allowed), each row_length bytes: adjacent rows
// merge into one run, a duplicate joins its run. More than max_runs runs -> the single min-to-max hull instead.
// Clears `runs` first; empty offsets leave it empty. (ApplyWalRecord keeps its own inline copy of this loop.)
void BuildLockRuns(const unsigned long long *offsets, size_t count, unsigned long long row_length, size_t max_runs,
                   std::vector<FileRangeLock::Run> &runs);

// RAII guard over LockRuns/UnlockRuns. Keeps a pointer into the caller's list, which must outlive the guard unchanged
// (the callers' thread_local run lists: none of them nests on one thread).
class FileRangeLockRunsGuard {
    public:
        FileRangeLockRunsGuard(const std::string &file_path, const FileRangeLock::Run *runs, size_t count,
                               FileRangeLock::LockType type = FileRangeLock::LockType::WRITE);
        ~FileRangeLockRunsGuard();

        FileRangeLockRunsGuard(const FileRangeLockRunsGuard&) = delete;
        FileRangeLockRunsGuard& operator=(const FileRangeLockRunsGuard&) = delete;

    private:
        FileRangeLock &lock;
        const FileRangeLock::Run *runs;
        size_t count;
        FileRangeLock::LockType type;
};

// RAII guard over TryLockRangeFor: unlocks on destruction only if the timed acquire succeeded (owns_lock()).
class TimedFileRangeLockGuard {
    public:
        TimedFileRangeLockGuard(const std::string &file_path, unsigned long long start, unsigned long long length,
                                FileRangeLock::LockType type, std::chrono::milliseconds timeout);
        // READ over TryReadLockYieldingFor (the live widen builder).
        TimedFileRangeLockGuard(const std::string &file_path, unsigned long long start, unsigned long long length,
                                std::chrono::milliseconds timeout, std::chrono::milliseconds yield_max,
                                FileRangeLock::WriterYield &yield);
        ~TimedFileRangeLockGuard();

        TimedFileRangeLockGuard(const TimedFileRangeLockGuard&) = delete;
        TimedFileRangeLockGuard& operator=(const TimedFileRangeLockGuard&) = delete;

        [[nodiscard]] bool owns_lock() const { return owns; }

    private:
        FileRangeLock &lock;
        unsigned long long start;
        unsigned long long length;
        FileRangeLock::LockType type;
        bool owns;
};

#endif //XSTORE_FILERANGELOCK_H
