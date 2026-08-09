#include <algorithm>
#include <limits>
#include "include/DataStructs/FileRangeLock.h"

bool FileRangeLock::Conflicts(unsigned long long start, unsigned long long end, LockType type) const {
    for (const ActiveLock &Lock : ActiveLocks) {
        bool RangesOverlap = start < Lock.End && Lock.Start < end;
        if (!RangesOverlap) {
            continue;
        }
        if (type == LockType::WRITE || Lock.Type == LockType::WRITE) {
            return true;
        }
        // Both sides READ on an overlapping range -- readers may share it
    }
    return false;
}

void FileRangeLock::LockRange(unsigned long long start, unsigned long long length, LockType type) {
    // length == 0 means "rest of file" (matches the old fcntl convention),
    // so it conflicts with every other range on this file.
    unsigned long long End = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    std::unique_lock<std::mutex> Lk(Mtx);
    Cv.wait(Lk, [&] { return !Conflicts(start, End, type); });
    ActiveLocks.push_back({start, End, type});
}

void FileRangeLock::UnlockRange(unsigned long long start, unsigned long long length, LockType type) {
    unsigned long long End = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    {
        std::lock_guard<std::mutex> Lk(Mtx);
        auto It = std::find_if(ActiveLocks.begin(), ActiveLocks.end(), [&](const ActiveLock &l) {
            return l.Start == start && l.End == End && l.Type == type;
        });
        if (It != ActiveLocks.end()) {
            // Swap-and-pop: O(1) removal, order doesn't matter here.
            *It = ActiveLocks.back();
            ActiveLocks.pop_back();
        }
    }
    // Wakes every waiter on this file to recheck its own range against the
    // now-updated active set. Waiter counts are bounded by the worker
    // thread pool size, so the wake-and-recheck cost stays small.
    Cv.notify_all();
}

std::shared_mutex FileLockRegistry::RegistryMutex;
std::unordered_map<std::string, std::unique_ptr<FileRangeLock>> FileLockRegistry::Locks;

FileRangeLock& FileLockRegistry::Get(const std::string &filePath) {
    // Fast path: file already registered -- shared lock lets every worker
    // thread look this up concurrently with no serialization.
    {
        std::shared_lock<std::shared_mutex> Lk(RegistryMutex);
        auto It = Locks.find(filePath);
        if (It != Locks.end()) {
            return *It->second;
        }
    }

    // Slow path: first time this file is touched -- take the exclusive lock
    // and double-check, since another thread may have inserted it in the
    // gap between the two locks.
    std::unique_lock<std::shared_mutex> Lk(RegistryMutex);
    auto It = Locks.find(filePath);
    if (It == Locks.end()) {
        It = Locks.emplace(filePath, std::make_unique<FileRangeLock>()).first;
    }
    return *It->second;
}

FileRangeLockGuard::FileRangeLockGuard(const std::string &filePath, unsigned long long start,
                                        unsigned long long length, FileRangeLock::LockType type)
    : Lock(FileLockRegistry::Get(filePath)), Start(start), Length(length), Type(type) {
    Lock.LockRange(start, length, type);
}

FileRangeLockGuard::~FileRangeLockGuard() {
    Lock.UnlockRange(Start, Length, Type);
}
