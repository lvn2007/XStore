#include <algorithm>
#include <limits>
#include "include/DataStructs/FileRangeLock.h"

bool FileRangeLock::Conflicts(unsigned long long start, unsigned long long end, LockType type) const {
    for (const ActiveLock &lock : active_locks) {
        bool ranges_overlap = start < lock.end && lock.start < end;
        if (!ranges_overlap) {
            continue;
        }
        if (type == LockType::WRITE || lock.type == LockType::WRITE) {
            return true;
        }
        // Both sides READ on an overlapping range -- readers may share it
    }
    return false;
}

bool FileRangeLock::WaitingWriterOverlaps(unsigned long long start, unsigned long long end) const {
    if (waiting_writer_overflow != 0) return true;
    for (uint32_t i = 0; i < waiting_writer_count; ++i) {
        if (start < waiting_writers[i].end && waiting_writers[i].start < end) return true;
    }
    return false;
}

void FileRangeLock::LockRange(unsigned long long start, unsigned long long length, LockType type) {
    // length == 0 means "rest of file" (matches the old fcntl convention),
    // so it conflicts with every other range on this file.
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    std::unique_lock<std::mutex> lk(mtx);
    // Uncontended: one Conflicts scan and the push_back, exactly what cv.wait(lk, pred) did before.
    if (Conflicts(start, end, type)) LockRangeSlow(lk, start, end, type);
    active_locks.push_back({start, end, type});
}

void FileRangeLock::LockRangeSlow(std::unique_lock<std::mutex> &lk, unsigned long long start, unsigned long long end,
                                  LockType type) {
    const auto free = [&] { return !Conflicts(start, end, type); };
    if (type == LockType::READ) {
        cv.wait(lk, free);
        return;
    }
    const bool slotted = waiting_writer_count < MAX_WAITING_WRITERS;
    if (slotted) {
        waiting_writers[waiting_writer_count++] = {start, end};
    } else {
        ++waiting_writer_overflow;
    }
    // Registering only adds a reason for a yielding reader to wait: nobody needs waking for it.
    cv.wait(lk, free);
    if (slotted) {
        // Our entry or an identical one (same range): which one goes does not matter. Swap-and-pop.
        for (uint32_t i = 0; i < waiting_writer_count; ++i) {
            if (waiting_writers[i].start == start && waiting_writers[i].end == end) {
                waiting_writers[i] = waiting_writers[--waiting_writer_count];
                break;
            }
        }
        // No notify: the caller now HOLDS a WRITE on exactly this range, so every yielding reader that overlapped the
        // entry still conflicts and is woken by the UnlockRange that ends it.
    } else {
        --waiting_writer_overflow;
        // The overflow stood for "a writer waits on the whole file": a yielding reader elsewhere may proceed now.
        if (waiting_writer_overflow == 0) cv.notify_all();
    }
}

bool FileRangeLock::TimedLock(unsigned long long start, unsigned long long end, LockType type,
                              std::chrono::milliseconds timeout, std::chrono::milliseconds yield_max, WriterYield *yield) {
    const Clock::time_point started = Clock::now();
    Clock::time_point deadline = started + timeout;
    const Clock::time_point yield_until = started + yield_max;

    std::unique_lock<std::mutex> lk(mtx);
    for (;;) {
        if (!Conflicts(start, end, type)) {
            if (yield == nullptr || !WaitingWriterOverlaps(start, end)) break;
            const Clock::time_point now = Clock::now();
            if (now >= yield_until) break; // the cap: from here only held locks count
            yield->yielded = true;
            // Woken by an unlock (the writer may have got in), a writer that leaves through the overflow, or the cap.
            cv.wait_until(lk, yield_until);
            const Clock::duration waited = Clock::now() - now;
            deadline += waited; // a yield does not use up the timeout
            yield->us += static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(waited).count());
            continue;
        }
        if (Clock::now() >= deadline) return false;
        cv.wait_until(lk, deadline);
    }
    active_locks.push_back({start, end, type});
    return true;
}

bool FileRangeLock::TryLockRangeFor(unsigned long long start, unsigned long long length, LockType type,
                                    std::chrono::milliseconds timeout) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;
    return TimedLock(start, end, type, timeout, std::chrono::milliseconds(0), nullptr);
}

bool FileRangeLock::TryReadLockYieldingFor(unsigned long long start, unsigned long long length,
                                           std::chrono::milliseconds timeout, std::chrono::milliseconds yield_max,
                                           WriterYield &yield) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;
    return TimedLock(start, end, LockType::READ, timeout, yield_max, &yield);
}

uint32_t FileRangeLock::WaitingWritersForTests() {
    std::lock_guard<std::mutex> lk(mtx);
    return waiting_writer_count + waiting_writer_overflow;
}

void FileRangeLock::UnlockRange(unsigned long long start, unsigned long long length, LockType type) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = std::find_if(active_locks.begin(), active_locks.end(), [&](const ActiveLock &l) {
            return l.start == start && l.end == end && l.type == type;
        });
        if (it != active_locks.end()) {
            // Swap-and-pop: O(1) removal, order doesn't matter here.
            *it = active_locks.back();
            active_locks.pop_back();
        }
    }
    // Wakes every waiter on this file to recheck its own range against the
    // now-updated active set. Waiter counts are bounded by the worker
    // thread pool size, so the wake-and-recheck cost stays small.
    cv.notify_all();
}

std::array<FileLockRegistry::Shard, FileLockRegistry::SHARD_COUNT> FileLockRegistry::shards;

FileRangeLock& FileLockRegistry::Get(const std::string &file_path) {
    Shard &s = shards[std::hash<std::string>{}(file_path) % SHARD_COUNT];

    // Fast path: file already registered -- shared lock lets every worker
    // thread look this up concurrently with no serialization, and only
    // contends with other threads hashing to the same shard.
    {
        std::shared_lock<std::shared_mutex> lk(s.mutex);
        auto it = s.locks.find(file_path);
        if (it != s.locks.end()) {
            return *it->second;
        }
    }

    // Slow path: first time this file is touched -- take the exclusive lock
    // and double-check, since another thread may have inserted it in the
    // gap between the two locks.
    std::unique_lock<std::shared_mutex> lk(s.mutex);
    auto it = s.locks.find(file_path);
    if (it == s.locks.end()) {
        it = s.locks.emplace(file_path, std::make_unique<FileRangeLock>()).first;
    }
    return *it->second;
}

FileRangeLockGuard::FileRangeLockGuard(const std::string &file_path, unsigned long long start,
                                        unsigned long long length, FileRangeLock::LockType type)
    : lock(FileLockRegistry::Get(file_path)), start(start), length(length), type(type) {
    lock.LockRange(start, length, type);
}

FileRangeLockGuard::~FileRangeLockGuard() {
    lock.UnlockRange(start, length, type);
}

TimedFileRangeLockGuard::TimedFileRangeLockGuard(const std::string &file_path, unsigned long long start,
                                                 unsigned long long length, FileRangeLock::LockType type,
                                                 std::chrono::milliseconds timeout)
    : lock(FileLockRegistry::Get(file_path)), start(start), length(length), type(type),
      owns(lock.TryLockRangeFor(start, length, type, timeout)) {}

TimedFileRangeLockGuard::TimedFileRangeLockGuard(const std::string &file_path, unsigned long long start,
                                                 unsigned long long length, std::chrono::milliseconds timeout,
                                                 std::chrono::milliseconds yield_max, FileRangeLock::WriterYield &yield)
    : lock(FileLockRegistry::Get(file_path)), start(start), length(length), type(FileRangeLock::LockType::READ),
      owns(lock.TryReadLockYieldingFor(start, length, timeout, yield_max, yield)) {}

TimedFileRangeLockGuard::~TimedFileRangeLockGuard() {
    if (owns) lock.UnlockRange(start, length, type);
}
