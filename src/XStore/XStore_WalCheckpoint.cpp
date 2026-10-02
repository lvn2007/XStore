// src/XStore/XStore_WalCheckpoint.cpp
#include "include/XStore/XStore_WalCheckpoint.h"
#include "include/XStore/XStore_WalRing.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <vector>
#include <fcntl.h>
#include <unistd.h>

namespace walring {

PaddedAtomicI64 last_writer_activity_ns{0};

namespace {

// First sub-range of [begin, end) not covered by the disjoint map `m`; false if fully covered.
bool FirstGap(const std::map<uint64_t, uint64_t> &m, uint64_t begin, uint64_t end, uint64_t &gap_begin, uint64_t &gap_end) {
    uint64_t pos = begin;
    auto it = m.upper_bound(pos);
    if (it != m.begin()) {
        const auto p = std::prev(it);
        if (p->second > pos) pos = p->second;
    }
    while (pos < end) {
        it = m.lower_bound(pos);
        if (it != m.end() && it->first == pos) { pos = it->second; continue; }
        gap_begin = pos;
        gap_end = it == m.end() ? end : std::min(end, it->first);
        return true;
    }
    return false;
}

// Inserts [begin, end) (known to be a gap), merging with touching neighbours.
void AddRange(std::map<uint64_t, uint64_t> &m, uint64_t begin, uint64_t end) {
    auto it = m.lower_bound(begin);
    if (it != m.end() && it->first == end) {
        end = it->second;
        it = m.erase(it);
    }
    if (it != m.begin()) {
        const auto p = std::prev(it);
        if (p->second == begin) { p->second = end; return; }
    }
    m.emplace_hint(it, begin, end);
}

// Rate limiter for log lines emitted from the loop.
bool Due(std::chrono::steady_clock::time_point &last, std::chrono::seconds every) {
    const auto now = std::chrono::steady_clock::now();
    if (last != std::chrono::steady_clock::time_point{} && now - last < every) return false;
    last = now;
    return true;
}

}  // namespace

Checkpointer &Checkpointer::Instance() {
    static Checkpointer *instance = new Checkpointer();   // leaked on purpose: see header
    return *instance;
}

bool Checkpointer::ConsumeFailure(std::atomic<int> &counter) {
    int n = counter.load(std::memory_order_relaxed);
    while (n > 0) {
        if (counter.compare_exchange_weak(n, n - 1, std::memory_order_relaxed)) return true;
    }
    return false;
}

void Checkpointer::FailNextDataSyncsForTests(int n) { fail_data_syncs_.store(n, std::memory_order_relaxed); }
void Checkpointer::FailNextDirSyncsForTests(int n) { fail_dir_syncs_.store(n, std::memory_order_relaxed); }
void Checkpointer::SetIdleMsForTests(unsigned ms) { idle_ms_override_.store(ms, std::memory_order_relaxed); }
void Checkpointer::SetMaxAgeMsForTests(unsigned ms) { max_age_override_.store(ms, std::memory_order_relaxed); }

void Checkpointer::FailNextDataSyncsStickyForTests(int n) { fail_data_sticky_.store(n, std::memory_order_relaxed); }
void Checkpointer::FailNextDirSyncsStickyForTests(int n) { fail_dir_sticky_.store(n, std::memory_order_relaxed); }

void Checkpointer::ClearPoisonForTests() {
    std::lock_guard<std::mutex> lock(cycle_mutex_);
    fail_data_sticky_.store(0, std::memory_order_relaxed);
    fail_dir_sticky_.store(0, std::memory_order_relaxed);
    poisoned_.store(false, std::memory_order_release);
    poison_logged_.store(false, std::memory_order_relaxed);
}

void Checkpointer::Poison(const char *what, const std::string &path, int err) {
    poisoned_.store(true, std::memory_order_release);
    if (!poison_logged_.exchange(true, std::memory_order_relaxed)) {
        spdlog::critical("[WAL CKPT] - CHECKPOINTER POISONED: {} failed for {} (errno {}: {}). Checkpoints will not advance again "
                         "in this process (a retried sync could report success for pages the kernel dropped); writers will fail as "
                         "overloaded once the ring fills. Restart to replay from the last good checkpoint.",
                         what, path, err, std::strerror(err));
    }
}

void Checkpointer::SetPausedForTests(bool paused) {
    paused_.store(paused, std::memory_order_release);
    if (paused) {
        // Wait out a cycle / paced flush that started before the flag was seen.
        std::lock_guard<std::mutex> wait_for_inflight(cycle_mutex_);
    }
}

std::unordered_set<std::string> Checkpointer::LastCycleSyncedDirsForTests() const {
    std::lock_guard<std::mutex> lock(synced_mutex_);
    return last_synced_dirs_;
}

std::unordered_set<std::string> Checkpointer::LastCycleSyncedPaths() const {
    std::lock_guard<std::mutex> lock(synced_mutex_);
    return last_synced_;
}

void Checkpointer::Start() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (thread_.joinable()) return;
    {
        std::lock_guard<std::mutex> lock(wake_mutex_);
        stop_ = false;
    }
    thread_ = std::thread(&Checkpointer::Loop, this);
}

void Checkpointer::StopAfterFinalCycle() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mutex_);
    if (thread_.joinable()) {
        {
            std::lock_guard<std::mutex> lock(wake_mutex_);
            stop_ = true;
        }
        wake_.notify_all();
        thread_.join();
    }
    // No loop thread any more: this is the last word. Failure is logged by RunCycle;
    // the checkpoint simply stays behind and the next start replays from it.
    static_cast<void>(RunCycle());
}

bool Checkpointer::RunCycle() {
    std::lock_guard<std::mutex> cycle(cycle_mutex_);
    return RunCycleLocked();
}

bool Checkpointer::RunCycleLocked() {
    if (poisoned_.load(std::memory_order_acquire)) return false;   // sticky: never advance again
    const std::vector<std::shared_ptr<Ring>> rings = AllRings();

    // (1) Snapshot FIRST. Every apply with lsn < snapshot has finished and Note'd
    // its file before the Take below, so its bytes are in the set synced here.
    std::vector<uint64_t> snapshot(rings.size());
    for (size_t i = 0; i < rings.size(); ++i) snapshot[i] = rings[i]->AppliedLsn();

    // (2) Merge newly dirty files into whatever a failed cycle left behind.
    for (const auto &entry : Dirty().Take().ranges) carried_.insert(entry.first);

    // (3) fdatasync each file by path (a throwaway fd: an fd-cache eviction cannot hide it).
    bool ok = true;
    std::unordered_set<std::string> synced;
    for (const std::string &path : carried_) {
        const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            if (errno == ENOENT) continue;   // DB deleted: nothing left to make durable
            spdlog::error("[WAL CKPT] - Cannot open {} for sync: {}", path, std::strerror(errno));
            ok = false;
            continue;
        }
        int err = 0;
        bool sticky = false;
        if (ConsumeFailure(fail_data_sticky_)) {
            err = EIO; sticky = true;
        } else if (ConsumeFailure(fail_data_syncs_)) {
            err = EIO;   // injected, retryable
        } else {
            int rc;
            do { rc = fdatasync(fd); } while (rc != 0 && errno == EINTR);
            if (rc != 0) { err = errno; sticky = true; }
        }
        close(fd);
        if (err == 0) {
            synced.insert(path);
            continue;
        }
        ok = false;
        if (sticky) {
            Poison("fdatasync", path, err);
            return false;
        }
        spdlog::error("[WAL CKPT] - fdatasync failed for {}; checkpoint not advanced", path);
    }

    // (4) The directory entry of a newly created year file is not made durable by
    // fdatasync of the file itself.
    std::unordered_set<std::string> dirs, synced_dirs;
    if (ok) {
        for (const std::string &path : synced) {
            std::string dir = std::filesystem::path(path).parent_path().string();
            if (dir.empty()) dir = ".";
            if (!dirs.insert(dir).second) continue;
            const int dfd = open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
            if (dfd < 0) {
                if (errno == ENOENT) continue;
                spdlog::error("[WAL CKPT] - Cannot open directory {}: {}", dir, std::strerror(errno));
                ok = false;
                continue;
            }
            int err = 0;
            bool sticky = false;
            if (ConsumeFailure(fail_dir_sticky_)) {
                err = EIO; sticky = true;
            } else if (ConsumeFailure(fail_dir_syncs_)) {
                err = EIO;   // injected, retryable
            } else {
                int rc;
                do { rc = fsync(dfd); } while (rc != 0 && errno == EINTR);
                if (rc != 0) { err = errno; sticky = true; }
            }
            close(dfd);
            if (err == 0) {
                dir_syncs_.fetch_add(1, std::memory_order_relaxed);
                synced_dirs.insert(dir);
                continue;
            }
            ok = false;
            if (sticky) {
                Poison("fsync(directory)", dir, err);
                return false;
            }
            spdlog::error("[WAL CKPT] - fsync failed for directory {}; checkpoint not advanced", dir);
        }
    }
    if (!ok) return false;   // carried_ kept for the retry; nothing advances

    // (5) Everything below the snapshot is now durable in the data files.
    carried_.clear();
    flushed_.clear();
    {
        std::lock_guard<std::mutex> lock(synced_mutex_);
        last_synced_ = std::move(synced);
        last_synced_dirs_ = std::move(synced_dirs);
    }
    for (size_t i = 0; i < rings.size(); ++i) {
        if (snapshot[i] <= rings[i]->CheckpointLsn()) continue;
        rings[i]->AdvanceCheckpoint(snapshot[i]);
        if (rings[i]->CheckpointLsn() < snapshot[i]) {
            ok = false;   // header write failed; AdvanceCheckpoint logged it
            continue;
        }
        Dirty().NoteCheckpointed(rings[i].get(), snapshot[i]);
        advances_.fetch_add(1, std::memory_order_relaxed);
    }
    if (ok) cycles_.fetch_add(1, std::memory_order_relaxed);
    return ok;
}

bool Checkpointer::PacedFlushStep() {
    std::lock_guard<std::mutex> cycle(cycle_mutex_);
    if (paused_.load(std::memory_order_acquire) || poisoned_.load(std::memory_order_acquire)) return false;
    const DirtyTracker::Snapshot dirty = Dirty().PeekRanges();
    for (const auto &[path, ranges] : dirty.ranges) {
        std::map<uint64_t, uint64_t> &done = flushed_[path];
        for (const DirtyRange &r : ranges) {
            uint64_t begin = 0, end = 0;
            if (!FirstGap(done, r.begin, r.end, begin, end)) continue;
            end = std::min<uint64_t>(end, begin + XStoreTunables::WAL_FLUSH_CHUNK_BYTES);
            const int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
            if (fd < 0) {
                // Missing/unopenable file (deleted DB; the cycle skips ENOENT too): mark the whole
                // file as flushed until the next cycle so it is not re-opened every tick, and
                // move on to the other files. Not work done.
                done.clear();
                done.emplace(0, UINT64_MAX);
                break;
            }
            int rc = 0, err = 0;
            for (int attempt = 0; attempt < 3; ++attempt) {
                rc = sync_file_range(fd, static_cast<off64_t>(begin), static_cast<off64_t>(end - begin),
                                     SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
                err = rc == 0 ? 0 : errno;
                if (rc == 0 || (err != EINTR && err != EAGAIN)) break;
            }
            close(fd);
            // A writeback error here consumes the file's error state, so a later fdatasync could
            // report success for dropped pages: poison (see the header). Other failures (EINVAL,
            // ENOMEM, ...) are best effort -- the cycle's fdatasync is the arbiter.
            if (err == EIO || err == ENOSPC || err == EDQUOT) {
                Poison("sync_file_range", path, err);
                return false;
            }
            AddRange(done, begin, end);
            paced_bytes_.fetch_add(end - begin, std::memory_order_relaxed);
            return true;
        }
    }
    return false;
}

void Checkpointer::Loop() {
    using Clock = std::chrono::steady_clock;
    constexpr auto TICK = std::chrono::milliseconds(1);
    constexpr auto RETRY_BACKOFF = std::chrono::milliseconds(50);
    Clock::time_point last_fail_log{}, last_fill_warn{};
    bool retry = false;           // a cycle failed: try again (after the back-off) whatever the triggers say
    bool back_to_back = false;    // fill >= CYCLE_FILL and the last cycle made progress: do not sleep
    auto wait = TICK;

    for (;;) {
        {
            std::unique_lock<std::mutex> lock(wake_mutex_);
            if (!back_to_back) wake_.wait_for(lock, wait, [&] { return stop_; });
            if (stop_) return;
        }
        wait = TICK;
        back_to_back = false;
        if (paused_.load(std::memory_order_acquire)) continue;
        if (poisoned_.load(std::memory_order_acquire)) { wait = RETRY_BACKOFF; continue; }   // no retry loop

        double fill = 0.0;
        bool pending = false;   // an applied record the checkpoint has not passed yet
        for (const std::shared_ptr<Ring> &ring : AllRings()) {
            fill = std::max(fill, ring->Fill());
            pending = pending || ring->AppliedLsn() > ring->CheckpointLsn();
        }
        const uint64_t age_ms = Dirty().OldestUnsyncedAgeMs();
        const bool dirty = Dirty().UnflushedBytes() != 0 || pending;
        const int64_t now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count();
        const int64_t idle_ms = (now_ns - last_writer_activity_ns.load(std::memory_order_relaxed)) / 1000000;

        const unsigned idle_limit = idle_ms_override_.load(std::memory_order_relaxed) != 0
            ? idle_ms_override_.load(std::memory_order_relaxed) : XStoreTunables::WAL_CKPT_IDLE_MS;
        const unsigned age_limit = max_age_override_.load(std::memory_order_relaxed) != 0
            ? max_age_override_.load(std::memory_order_relaxed) : XStoreTunables::WAL_CKPT_MAX_AGE_MS;
        const bool cycle_fill = fill >= XStoreTunables::WAL_CKPT_CYCLE_FILL;

        if (retry || cycle_fill || age_ms >= age_limit || (dirty && idle_ms >= static_cast<int64_t>(idle_limit))) {
            if (cycle_fill && Due(last_fill_warn, std::chrono::seconds(60))) {
                spdlog::warn("[WAL CKPT] - ring fill {:.0f}% >= {:.0f}%: checkpointing back-to-back; writers will stall near {:.0f}%",
                             fill * 100, XStoreTunables::WAL_CKPT_CYCLE_FILL * 100, XStoreTunables::WAL_STALL_FILL * 100);
            }
            const uint64_t advances_before = advances_.load(std::memory_order_relaxed);
            bool cycle_ok = false;
            {
                // Re-check pause under the cycle mutex: SetPausedForTests(true) returns only
                // once no cycle is in flight, and none may start after that.
                std::lock_guard<std::mutex> cycle(cycle_mutex_);
                if (paused_.load(std::memory_order_acquire)) continue;
                cycle_ok = RunCycleLocked();
            }
            if (cycle_ok) {
                retry = false;
                back_to_back = cycle_fill && advances_.load(std::memory_order_relaxed) != advances_before;
            } else {
                retry = true;
                wait = RETRY_BACKOFF;
                if (Due(last_fail_log, std::chrono::seconds(10))) spdlog::error("[WAL CKPT] - checkpoint cycle failed; retrying in {} ms", RETRY_BACKOFF.count());
            }
        } else if (fill >= XStoreTunables::WAL_CKPT_SOFT_FILL) {
            static_cast<void>(PacedFlushStep());
        }
    }
}

}  // namespace walring
