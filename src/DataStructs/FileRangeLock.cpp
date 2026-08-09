#include <algorithm>
#include <limits>
#include "include/DataStructs/FileRangeLock.h"

bool FileRangeLock::Conflicts(unsigned long long start, unsigned long long end, LockType type) const {
    for (const ActiveLock &lock : activeLocks) {
        bool rangesOverlap = start < lock.end && lock.start < end;
        if (!rangesOverlap) {
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
    activeLocks.push_back({start, end, type});
}

void FileRangeLock::UnlockRange(unsigned long long start, unsigned long long length, LockType type) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    {
        std::lock_guard<std::mutex> lk(mtx);
        auto it = std::find_if(activeLocks.begin(), activeLocks.end(), [&](const ActiveLock &l) {
            return l.start == start && l.end == end && l.type == type;
        });
        if (it != activeLocks.end()) {
            // Swap-and-pop: O(1) removal, order doesn't matter here.
            *it = activeLocks.back();
            activeLocks.pop_back();
        }
    }
    // Wakes every waiter on this file to recheck its own range against the
    // now-updated active set. Waiter counts are bounded by the worker
    // thread pool size, so the wake-and-recheck cost stays small.
    cv.notify_all();
}

std::shared_mutex FileLockRegistry::registryMutex;
std::unordered_map<std::string, std::unique_ptr<FileRangeLock>> FileLockRegistry::locks;

FileRangeLock& FileLockRegistry::Get(const std::string &filePath) {
    // Fast path: file already registered -- shared lock lets every worker
    // thread look this up concurrently with no serialization.
    {
        std::shared_lock<std::shared_mutex> lk(registryMutex);
        auto it = locks.find(filePath);
        if (it != locks.end()) {
            return *it->second;
        }
    }

    // Slow path: first time this file is touched -- take the exclusive lock
    // and double-check, since another thread may have inserted it in the
    // gap between the two locks.
    std::unique_lock<std::shared_mutex> lk(registryMutex);
    auto it = locks.find(filePath);
    if (it == locks.end()) {
        it = locks.emplace(filePath, std::make_unique<FileRangeLock>()).first;
    }
    return *it->second;
}

FileRangeLockGuard::FileRangeLockGuard(const std::string &filePath, unsigned long long start,
                                        unsigned long long length, FileRangeLock::LockType type)
    : lock(FileLockRegistry::Get(filePath)), start(start), length(length), type(type) {
    lock.LockRange(start, length, type);
}

FileRangeLockGuard::~FileRangeLockGuard() {
    lock.UnlockRange(start, length, type);
}
