#include <algorithm>
#include <cassert>
#include <limits>
#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_Tunables.h"

// The first held WRITE ending after `start` is the only candidate: held WRITEs are disjoint, so ends ascend.
bool FileRangeLock::WriteOverlaps(unsigned long long start, unsigned long long end) const {
    const auto w = std::partition_point(writes.begin(), writes.end(), [&](const Range &r) { return r.end <= start; });
    return w != writes.end() && w->start < end;
}

bool FileRangeLock::Conflicts(unsigned long long start, unsigned long long end, LockType type) const {
    if (WriteOverlaps(start, end)) return true;
    if (type == LockType::READ) return false;  // readers share
    for (const Range &r : reads) {
        if (start < r.end && r.start < end) return true;
    }
    return false;
}

void FileRangeLock::Register(unsigned long long start, unsigned long long end, LockType type) {
    if (type == LockType::READ) {
        reads.push_back({start, end});
        return;
    }
    assert(start < end);
    const auto at = std::partition_point(writes.begin(), writes.end(), [&](const Range &r) { return r.start < start; });
    writes.insert(at, {start, end});
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
    // Uncontended: one Conflicts check and the insert, exactly what cv.wait(lk, pred) did before.
    if (Conflicts(start, end, type)) LockRangeSlow(lk, start, end, type);
    Register(start, end, type);
}

void FileRangeLock::LockRangeSlow(std::unique_lock<std::mutex> &lk, unsigned long long start, unsigned long long end,
                                  LockType type) {
    const auto free = [&] { return !Conflicts(start, end, type); };
    if (type == LockType::READ) {
        cv.wait(lk, free);
        return;
    }
    WaitAsWriter(lk, start, end, free);
}

template <typename Free>
void FileRangeLock::WaitAsWriter(std::unique_lock<std::mutex> &lk, unsigned long long start, unsigned long long end,
                                 const Free &free) {
    const bool slotted = waiting_writer_count < MAX_WAITING_WRITERS;
    if (slotted) {
        waiting_writers[waiting_writer_count++] = {start, end};
    } else {
        ++waiting_writer_overflow;
    }
    // Registering only adds a reason for a yielding reader to wait: nobody needs waking for it.
    const Clock::time_point wait_start = Clock::now();
    cv.wait(lk, free);
    const uint64_t waited_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - wait_start).count());
    writer_wait_us_.fetch_add(waited_us, std::memory_order_relaxed);
    writer_waits_.fetch_add(1, std::memory_order_relaxed);
    if (waited_us >= XStoreTunables::LIVE_WIDEN_WRITER_LONG_WAIT_US) long_waits_.fetch_add(1, std::memory_order_relaxed);
    if (slotted) {
        // Our entry or an identical one (same range): which one goes does not matter. Swap-and-pop.
        for (uint32_t i = 0; i < waiting_writer_count; ++i) {
            if (waiting_writers[i].start == start && waiting_writers[i].end == end) {
                waiting_writers[i] = waiting_writers[--waiting_writer_count];
                break;
            }
        }
        // No notify here: a LockRange caller now HOLDS a WRITE on exactly this range, so every yielding reader that
        // overlapped the entry still conflicts and is woken by the UnlockRange that ends it. LockRuns notifies itself.
    } else {
        --waiting_writer_overflow;
        // The overflow stood for "a writer waits on the whole file": a yielding reader elsewhere may proceed now.
        if (waiting_writer_overflow == 0) cv.notify_all();
    }
}

// A WRITE set conflicts with any overlap, a READ set only with a held WRITE. Vs `writes`: both sides sorted, so each
// run's candidate (first WRITE ending after its start) only moves right; search from the previous one. Vs `reads`,
// O(reads * log runs): the runs' ends ascend, so the first run ending after a read's start is its only candidate.
bool FileRangeLock::RunsFree(const Run *runs, size_t count, LockType type) const {
    const Run *runs_end = runs + count;
    auto w = writes.begin();
    for (const Run *r = runs; r != runs_end; ++r) {
        w = std::partition_point(w, writes.end(), [&](const Range &h) { return h.end <= r->start; });
        if (w == writes.end()) break;
        if (w->start < r->start + r->length) return false;
    }
    if (type == LockType::READ) return true;
    for (const Range &l : reads) {
        const Run *r = std::upper_bound(runs, runs_end, l.start,
                                        [](unsigned long long v, const Run &run) { return v < run.start + run.length; });
        if (r != runs_end && r->start < l.end) return false;
    }
    return true;
}

void FileRangeLock::LockRuns(const Run *runs, size_t count, LockType type) {
    if (count == 0) return;
    if (count == 1) {
        LockRange(runs[0].start, runs[0].length, type);
        return;
    }
#ifndef NDEBUG
    for (size_t i = 0; i < count; ++i) {
        assert(runs[i].length > 0);
        assert(i == 0 || runs[i - 1].start + runs[i - 1].length <= runs[i].start);
    }
#endif
    const bool write = type == LockType::WRITE;
    const auto free = [&] { return RunsFree(runs, count, type); };
    std::unique_lock<std::mutex> lk(mtx);
    if (!free()) {
        if (write) {
            WaitAsWriter(lk, runs[0].start, runs[count - 1].start + runs[count - 1].length, free);
            // Unlike LockRange, what is now held is less than the hull that was registered: a yielding reader in a gap
            // no longer has a reason to wait.
            cv.notify_all();
        } else {
            cv.wait(lk, free);
        }
    }
    if (!write) {
        reads.reserve(reads.size() + count); // a bad_alloc must not leave the set partly registered
        for (size_t i = 0; i < count; ++i) reads.push_back({runs[i].start, runs[i].start + runs[i].length});
        return;
    }
    // Merge the sorted runs into `writes` from the back, in place. The resize is the only thing that may throw, and
    // leaves `writes` unchanged if it does; nothing allocates after it (unlike std::inplace_merge's temporary buffer).
    size_t old_size = writes.size();
    writes.resize(old_size + count);
    size_t out = writes.size();
    size_t r = count;
    while (r > 0) {
        if (old_size > 0 && writes[old_size - 1].start > runs[r - 1].start) {
            writes[--out] = writes[--old_size];
        } else {
            --r;
            writes[--out] = {runs[r].start, runs[r].start + runs[r].length};
        }
    }
}

void FileRangeLock::UnlockRuns(const Run *runs, size_t count, LockType type) {
    if (count == 0) return;
    if (count == 1) {
        UnlockRange(runs[0].start, runs[0].length, type);
        return;
    }
    {
        std::lock_guard<std::mutex> lk(mtx);
        const Run *runs_end = runs + count;
        const auto is_ours = [&](const Range &l) {
            const Run *r = std::lower_bound(runs, runs_end, l.start,
                                            [](const Run &run, unsigned long long v) { return run.start < v; });
            return r != runs_end && r->start == l.start && r->start + r->length == l.end;
        };
        if (type == LockType::WRITE) {
            // One pass, O(entries * log runs), stable (`writes` stays sorted): two identical WRITE entries cannot be
            // held at once, so every entry matching a run is that run's own.
            writes.erase(std::remove_if(writes.begin(), writes.end(), is_ours), writes.end());
        } else {
            // Another reader may hold an identical READ entry, so exactly ONE entry per run goes. In place, no
            // allocation (this runs in a destructor): move the matching entries to the tail, sort them so identical
            // ones are adjacent (every run has at least its own), drop the first of each group, keep the rest.
            // Entry order does not matter (UnlockRange swap-and-pops).
            const auto tail = std::partition(reads.begin(), reads.end(), [&](const Range &l) { return !is_ours(l); });
            std::sort(tail, reads.end(), [](const Range &a, const Range &b) { return a.start < b.start; });
            auto out = tail;
            for (auto it = tail; it != reads.end(); ++it) {
                if (it != tail && it->start == (it - 1)->start) *out++ = *it;
            }
            reads.erase(out, reads.end());
        }
    }
    cv.notify_all();
}

void BuildLockRuns(const unsigned long long *offsets, size_t count, unsigned long long row_length, size_t max_runs,
                   std::vector<FileRangeLock::Run> &runs) {
    runs.clear();
    if (count == 0) return;
    runs.push_back({offsets[0], row_length});
    for (size_t i = 1; i < count; ++i) {
        FileRangeLock::Run &last = runs.back();
        if (offsets[i] == last.start + last.length) {
            last.length += row_length;
        } else if (offsets[i] != offsets[i - 1]) {
            if (runs.size() == max_runs) {   // over the cap: the hull
                runs.assign(1, {offsets[0], (offsets[count - 1] + row_length) - offsets[0]});
                return;
            }
            runs.push_back({offsets[i], row_length});
        }
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
    Register(start, end, type);
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

FileRangeLock::WriterWaitStats FileRangeLock::WriterWaits() const noexcept {
    return {writer_wait_us_.load(std::memory_order_relaxed), writer_waits_.load(std::memory_order_relaxed),
            long_waits_.load(std::memory_order_relaxed)};
}

uint32_t FileRangeLock::WaitingWritersForTests() {
    std::lock_guard<std::mutex> lk(mtx);
    return waiting_writer_count + waiting_writer_overflow;
}

bool FileRangeLock::RunsFreeForTests(const Run *runs, size_t count, LockType type) {
    std::lock_guard<std::mutex> lk(mtx);
    return RunsFree(runs, count, type);
}

size_t FileRangeLock::ActiveLocksForTests() {
    std::lock_guard<std::mutex> lk(mtx);
    return writes.size() + reads.size();
}

void FileRangeLock::UnlockRange(unsigned long long start, unsigned long long length, LockType type) {
    unsigned long long end = (length == 0) ? std::numeric_limits<unsigned long long>::max() : start + length;

    {
        std::lock_guard<std::mutex> lk(mtx);
        if (type == LockType::WRITE) {
            const auto it = std::partition_point(writes.begin(), writes.end(),
                                                 [&](const Range &r) { return r.start < start; });
            if (it != writes.end() && it->start == start && it->end == end) writes.erase(it);
        } else {
            // From the back: the caller's own entry is usually recent. Swap-and-pop, order doesn't matter.
            const auto it = std::find_if(reads.rbegin(), reads.rend(),
                                         [&](const Range &r) { return r.start == start && r.end == end; });
            if (it != reads.rend()) {
                *it = reads.back();
                reads.pop_back();
            }
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

FileRangeLockRunsGuard::FileRangeLockRunsGuard(const std::string &file_path, const FileRangeLock::Run *runs, size_t count,
                                               FileRangeLock::LockType type)
    : lock(FileLockRegistry::Get(file_path)), runs(runs), count(count), type(type) {
    lock.LockRuns(runs, count, type);
}

FileRangeLockRunsGuard::~FileRangeLockRunsGuard() {
    lock.UnlockRuns(runs, count, type);
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
