#ifndef XSTORE_FILERANGELOCK_H
#define XSTORE_FILERANGELOCK_H

#include <condition_variable>
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
class FileRangeLock {
    public:
        enum class LockType { READ, WRITE };

        void LockRange(unsigned long long start, unsigned long long length, LockType type);
        void UnlockRange(unsigned long long start, unsigned long long length, LockType type);

    private:
        struct ActiveLock {
            unsigned long long start;
            unsigned long long end;    // exclusive
            LockType type;
        };

        // Caller must hold mtx.
        bool Conflicts(unsigned long long start, unsigned long long end, LockType type) const;

        std::mutex mtx;
        std::condition_variable cv;
        // Concurrently held ranges per file are bounded by the worker thread
        // count, so a flat vector + linear scan is cheaper than a node-based
        // container: no per-lock heap allocation and better cache locality.
        std::vector<ActiveLock> activeLocks;
};

// Process-wide registry: one FileRangeLock per file path, shared across
// every worker thread (each thread owns its own XStore instance, so the
// lock table cannot live on XStore itself).
//
// Reads use a shared_mutex: after a file's first access, every subsequent
// lookup takes a shared (read) lock and can proceed fully in parallel with
// other threads' lookups. The registry only serializes (briefly, via an
// exclusive lock) the first time a given file path is touched.
class FileLockRegistry {
    public:
        static FileRangeLock& Get(const std::string &filePath);

    private:
        static std::shared_mutex registryMutex;
        static std::unordered_map<std::string, std::unique_ptr<FileRangeLock>> locks;
};

// RAII guard -- guarantees UnlockRange() runs even on early return/exception.
// This matters because a missed unlock now means a permanent hang for any
// other thread waiting on that range, instead of (as with the old fcntl
// code) a harmless no-op released automatically on fclose().
class FileRangeLockGuard {
    public:
        FileRangeLockGuard(const std::string &filePath, unsigned long long start,
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

#endif //XSTORE_FILERANGELOCK_H
