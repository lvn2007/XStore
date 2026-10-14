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

void FileRangeLock::LockRange(unsigned long long start, unsigned long long length, LockType type) {
    // length == 0 means "rest of file" (matches the old fcntl convention),
    // so it conflicts with every other range on this file.
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    std::unique_lock<std::mutex> lk(mtx);
    cv.wait(lk, [&] { return !Conflicts(start, end, type); });
    active_locks.push_back({start, end, type});
}

bool FileRangeLock::TryLockRangeFor(unsigned long long start, unsigned long long length, LockType type,
                                    std::chrono::milliseconds timeout) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    std::unique_lock<std::mutex> lk(mtx);
    if (!cv.wait_for(lk, timeout, [&] { return !Conflicts(start, end, type); })) return false;
    active_locks.push_back({start, end, type});
    return true;
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

TimedFileRangeLockGuard::~TimedFileRangeLockGuard() {
    if (owns) lock.UnlockRange(start, length, type);
}
