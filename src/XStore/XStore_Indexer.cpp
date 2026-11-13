#include "include/XStore/XStore_Indexer.h"

#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore.h"
#include <spdlog/fmt/fmt.h>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <sched.h>

namespace xstore {

namespace {

constexpr unsigned long MAX_THREADS = 256;
constexpr size_t PASS_CHUNK_BYTES = size_t{1} << 20;   // rows held under one READ lock: the live widen's bound

std::mutex g_delay_mutex;
std::function<void()> g_delay;            // SetPassDelayForTests; g_delay_mutex
std::atomic<bool> g_has_delay{false};     // one atomic load per chunk in production
std::function<void()> g_open_delay;       // SetPassOpenDelayForTests; g_delay_mutex
std::atomic<bool> g_has_open_delay{false};

void RunHook(const std::atomic<bool> &armed, const std::function<void()> &hook) {
    if (!armed.load(std::memory_order_acquire)) return;
    std::function<void()> cb;
    {
        std::lock_guard<std::mutex> lock(g_delay_mutex);
        cb = hook;
    }
    if (cb) cb();
}
void PassDelay() { RunHook(g_has_delay, g_delay); }
void PassOpenDelay() { RunHook(g_has_open_delay, g_open_delay); }

}  // namespace

IndexerConfig IndexerConfigFromEnv() {
    IndexerConfig c;
    c.threads = std::max<uint32_t>(1, std::thread::hardware_concurrency() / XStoreTunables::INDEXER_THREADS_DIVISOR);
    c.backlog_bytes = XStoreTunables::INDEXER_BACKLOG_MB_DEV_DEFAULT << 20;
    if (const char *env = std::getenv("XSTORE_INDEXER_THREADS")) {
        c.threads = static_cast<uint32_t>(std::clamp<unsigned long>(std::strtoul(env, nullptr, 10), 1, MAX_THREADS));
    }
    if (const char *env = std::getenv("XSTORE_INDEXER_BACKLOG_MB")) {
        c.backlog_bytes = std::min<unsigned long long>(std::strtoull(env, nullptr, 10), UINT64_MAX >> 20) << 20;
    }
    // 0, a sign, or anything but a whole number keeps the default: clamping it to 1 would hand off every record (the
    // measured sparse collapse). Larger values clamp to 2^20.
    if (const char *env = std::getenv("XSTORE_INDEXER_MIN_RUN_ROWS"); env != nullptr && std::isdigit(static_cast<unsigned char>(env[0]))) {
        char *end = nullptr;
        const unsigned long v = std::strtoul(env, &end, 10);
        if (*end == '\0' && v != 0) c.min_run_rows = static_cast<uint32_t>(std::min<unsigned long>(v, 1ul << 20));
    }
    // Exactly "1" turns it on; unset, "0" or anything else keeps the default.
    if (const char *env = std::getenv("XSTORE_INDEXER_IDLE")) c.idle_priority = std::strcmp(env, "1") == 0;
    return c;
}

Indexer &Indexer::Instance() {
    static Indexer instance;
    return instance;
}

Indexer::~Indexer() { StopAndDrain(std::chrono::milliseconds(0)); }

void Indexer::Start(const IndexerConfig &config) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    StopLocked(std::chrono::seconds(10));
    used_bytes_.store(0, std::memory_order_relaxed);
    limit_bytes_.store(config.backlog_bytes, std::memory_order_relaxed);
    // Before active_'s release below: a request that sees Active() also sees these.
    min_run_rows_.store(std::max<uint32_t>(1, config.min_run_rows), std::memory_order_relaxed);
    hand_off_sync_.store(config.hand_off_sync, std::memory_order_relaxed);
    if (config.backlog_bytes == 0) return;
    stopping_.store(false, std::memory_order_relaxed);
    const uint32_t threads = std::max<uint32_t>(1, config.threads);
    workers_.reserve(threads);
    for (uint32_t i = 0; i < threads; ++i) workers_.emplace_back([this, idle = config.idle_priority] { WorkerLoop(idle); });
    PendingMaps::SetNoticesEnabled(true);   // before the first hand-off can leave a row pending (ruling 4)
    active_.store(true, std::memory_order_release);
}

void Indexer::StopAndDrain(std::chrono::milliseconds bound) {
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    StopLocked(bound);
}

void Indexer::StopLocked(std::chrono::milliseconds bound) {
    if (!workers_.empty()) {
        active_.store(false, std::memory_order_release);  // no new reservations or hints from here on
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (outstanding_.load(std::memory_order_acquire) != 0 && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        stopping_.store(true, std::memory_order_release);  // workers drop the rest: the join below is bounded by one chunk
        for (size_t i = 0; i < workers_.size(); ++i) queue_.enqueue(IndexHint{});
        for (std::thread &t : workers_) t.join();
        workers_.clear();
        PendingMaps::SetNoticesEnabled(false);   // no pool, no notice (ruling 4)
    }
    // Also with no workers: a hint that passed Enqueue's Active() check as the pool stopped. The pending bits stay
    // authoritative, so dropping hints is safe; their reservations go back.
    IndexHint leftover;
    while (queue_.try_dequeue(leftover)) {
        if (leftover.map) {
            Release(leftover.reserved_bytes);
            outstanding_.fetch_sub(1, std::memory_order_acq_rel);
        }
        leftover = IndexHint{};
    }
}

bool Indexer::TryReserve(uint64_t bytes) {
    if (!Active()) return false;
    const uint64_t limit = limit_bytes_.load(std::memory_order_relaxed);
    uint64_t used = used_bytes_.load(std::memory_order_relaxed);
    do {
        if (bytes > limit - used) return false;   // used <= limit always; used + bytes could wrap
    } while (!used_bytes_.compare_exchange_weak(used, used + bytes, std::memory_order_relaxed));
    return true;
}

void Indexer::Release(uint64_t bytes) {
    [[maybe_unused]] const uint64_t before = used_bytes_.fetch_sub(bytes, std::memory_order_relaxed);
    assert(before >= bytes && "Indexer::Release of bytes that were never reserved");
}

void Indexer::Enqueue(IndexHint hint) {
    if (!Active() || !hint.map || CoveredByRemoval(hint)) {
        Release(hint.reserved_bytes);
        return;
    }
    outstanding_.fetch_add(1, std::memory_order_acq_rel);
    queue_.enqueue(std::move(hint));
}

void Indexer::SetConfigForTests(const IndexerConfig &config) { Instance().Start(config); }

void Indexer::SetPassDelayForTests(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(g_delay_mutex);
    g_has_delay.store(static_cast<bool>(cb), std::memory_order_release);
    g_delay = std::move(cb);
}

void Indexer::SetPassOpenDelayForTests(std::function<void()> cb) {
    std::lock_guard<std::mutex> lock(g_delay_mutex);
    g_has_open_delay.store(static_cast<bool>(cb), std::memory_order_release);
    g_open_delay = std::move(cb);
}

void Indexer::WaitIdleForTests(std::chrono::milliseconds bound) {
    if (!Active()) return;
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (outstanding_.load(std::memory_order_acquire) != 0) {
        if (std::chrono::steady_clock::now() >= deadline) {
            spdlog::warn("[INDEXER] - still busy after {} ms; the test registries are cleared under its passes", bound.count());
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool Indexer::CoveredByRemoval(const IndexHint &hint) {
    // One relaxed load unless a removal runs. Deliberately NOT atomic with the push in Enqueue (that would need
    // bg_mutex_ around the enqueue while any removal runs): a hint that misses a removal starting right now is still
    // released exactly once, by that removal's DropHints, by its pass (Enter refuses the covered key, or the map is no
    // longer the registered one), or by a stop's drain. The cost: with a plugged pool, BacklogBytes() can stay above 0
    // for a moment after RemoveDB returns.
    if (XStore::bg_removing_count_.load(std::memory_order_relaxed) == 0) return false;
    return XStore::RemovalCovers(YearKey(hint));
}

void Indexer::DropHints(const std::function<bool(const std::string &year_key)> &match) {
    if (outstanding_.load(std::memory_order_acquire) == 0) return;   // nothing queued or running
    // The queue has no remove: take out what is there now and put back what survives (hints only, order is free).
    // Under the lifecycle mutex so a stop's sentinels and leftover drain never interleave; bounded by the size seen at
    // the start, so writers enqueueing meanwhile cannot keep it looping.
    std::lock_guard<std::mutex> lock(lifecycle_mutex_);
    std::vector<IndexHint> keep;
    IndexHint hint;
    for (size_t n = queue_.size_approx(); n != 0 && queue_.try_dequeue(hint); --n) {
        if (hint.map && match(YearKey(hint))) {
            Release(hint.reserved_bytes);
            outstanding_.fetch_sub(1, std::memory_order_acq_rel);
        } else {
            keep.push_back(std::move(hint));
        }
        hint = IndexHint{};
    }
    for (IndexHint &k : keep) queue_.enqueue(std::move(k));
}

void Indexer::WorkerLoop(bool idle_priority) {
    // IndexerConfig::idle_priority (XStoreTunables::INDEXER_IDLE_PRIORITY_DEFAULT): only CPU nobody else wants.
    if (idle_priority) {
        const sched_param param{};
        if (sched_setscheduler(0, SCHED_IDLE, &param) != 0) spdlog::warn("[INDEXER] - SCHED_IDLE refused (errno {})", errno);
    }
    IndexHint hint;
    for (;;) {
        queue_.wait_dequeue(hint);
        if (!hint.map) return;
        if (stopping_.load(std::memory_order_acquire)) {
            Release(hint.reserved_bytes);
        } else {
            RunPass(hint);
        }
        hint = IndexHint{};   // the map may be freed now, not at the next hint
        outstanding_.fetch_sub(1, std::memory_order_acq_rel);
    }
}

std::string Indexer::YearKey(const IndexHint &hint) { return fmt::format("{}/{}", hint.db_base_path, hint.year); }

void Indexer::RunPass(const IndexHint &hint) {
    // The RemoveDB handshake: a year a removal covers is not started (its hints are being dropped), and a removal waits
    // for this key (bounded), so from the check in PassBody to the end of the pass the year's files stay. The key is let
    // go only after the reservation: a removal that returns finds it released.
    const std::string key = YearKey(hint);
    if (!XStore::EnterIndexerYear(key)) {
        Release(hint.reserved_bytes);
        return;
    }
    struct Leave {
        const std::string &key;
        ~Leave() { XStore::LeaveIndexerYear(key); }
    } leave{key};
    if (PassBody(hint, key) == PassEnd::kRequeue) {
        Enqueue(hint);   // carries the reservation along
    } else {
        Release(hint.reserved_bytes);
    }
}

bool Indexer::FirstFailureOf(const std::string &data_path) {
    std::lock_guard<std::mutex> lock(failure_mutex_);
    return failed_files_.insert(data_path).second;
}

Indexer::PassEnd Indexer::PassBody(const IndexHint &hint, const std::string &key) {
    PendingMap &map = *hint.map;
    // Read without the lock: only where pending rows are, to choose the chunks. What is folded is re-read under each
    // chunk's lock.
    const std::vector<IndexRun> runs = map.PendingRunsOfBlock(hint.block);
    if (runs.empty()) return PassEnd::kDone;   // stale: an earlier pass folded these rows
    // The map the writer marked through is no longer the year's: the year was removed (and maybe re-created). Its rows
    // are gone; opening anything now would bring the year's files back (the opens create what is missing). Checked
    // under the year key (RunPass), as ApplyWalRecord checks the cfg epoch again after its open: a removal that has not
    // forgotten the map yet waits for this pass, so nothing it removes is opened (or re-created) here.
    if (PendingMaps::Find(key) != hint.map) return PassEnd::kDone;
    PassOpenDelay();

    ZoneMapFile::Pin zmap_pin;
    CatIdxFile::Pin catidx_pin;
    // The data file through the same fd registry as the writer, so a removal's wait for open handles sees this one.
    // ponytail: the registry opens O_RDWR | O_CREAT. The check above under the year key keeps it from creating a removed
    // year's file, except after a removal whose wait for this key timed out (BACKGROUND_REMOVE_WAIT_MS) and went on: a
    // pass stalled that long could then re-create the year here (the background thread has the same exposure). An
    // open-existing-only path would close that; not added.
    const std::string &data_path = XStore::CachedYearFilePath(hint.db_base_path, hint.year);
    std::shared_ptr<int> fd;
    try {
        zmap_pin = XStore::PinZoneMap(hint.db_base_path, hint.year, hint.row_length, hint.tracked_column_count);
        catidx_pin = XStore::PinCatIdx(hint.db_base_path, hint.year, hint.row_length, hint.tracked_column_count);
        fd = XStore::GetOrOpenFile(data_path, hint.row_length, 0);
    } catch (const std::exception &e) {
        // A removal that registered its scope after this pass took its key refuses the cold opens ("being removed"),
        // or the year went meanwhile: nothing to report.
        if (XStore::RemovalCovers(key) || PendingMaps::Find(key) != hint.map) return PassEnd::kDone;
        if (FirstFailureOf(data_path)) spdlog::error("[INDEXER] - {}: cannot open the year ({}); hint dropped", data_path, e.what());
        return PassEnd::kDone;
    }
    ZoneMapFile &zmap = *zmap_pin;
    CatIdxFile &catidx = *catidx_pin;
    if (!fd) {   // EMFILE, EACCES, ...: nothing folded; the bits stay set, the next start rebuilds
        // Unless the year went meanwhile (a removal whose wait for this key timed out unlinked it): nothing to report.
        if (PendingMaps::Find(key) != hint.map) return PassEnd::kDone;
        if (FirstFailureOf(data_path)) {
            spdlog::error("[INDEXER] - {}: cannot open the data file; the year's indexes are marked dirty and rebuilt at "
                          "the next start", data_path);
        }
        zmap.MarkDirty();
        catidx.MarkDirty();
        return PassEnd::kDone;
    }

    const size_t stripe = (std::hash<const void *>{}(hint.map.get()) ^ (hint.block * 0x9E3779B97F4A7C15ULL)) % PASS_STRIPES;
    // Held while waiting up to ~250 ms for a range lock below: a pass of an unrelated block in the same stripe waits too.
    const std::lock_guard<std::mutex> serial(pass_mutexes_[stripe]);
    const uint64_t row_length = hint.row_length;
    const uint64_t chunk_rows = std::max<uint64_t>(1, PASS_CHUNK_BYTES / row_length);
    const uint64_t end = runs.back().first_row + runs.back().row_count;
    thread_local std::vector<IndexRun> chunk_runs;
    // A block is ZONE_MAP_TARGET_BLOCK_BYTES (2 MiB): at most a few chunks, so a stopping pool's join waits for one pass.
    for (uint64_t from = runs.front().first_row; from < end;) {
        const uint64_t to = std::min(end, from + chunk_rows);
        {
            FileRangeLock::WriterYield yield;
            const TimedFileRangeLockGuard lock(data_path, from * row_length, (to - from) * row_length,
                                               std::chrono::milliseconds(XStoreTunables::LIVE_WIDEN_LOCK_WAIT_MS),
                                               std::chrono::milliseconds(XStoreTunables::LIVE_WIDEN_YIELD_MAX_MS), yield);
            if (!lock.owns_lock()) return PassEnd::kRequeue;   // a writer storm: back of the queue, not a pinned worker
            // Under the lock no writer can set or be setting a bit on [from, to): every bit read here belongs to bytes
            // already written. Bits set after the unlocked read above are included.
            const std::vector<IndexRun> now = map.PendingRunsOfBlock(hint.block);
            chunk_runs.clear();
            for (const IndexRun &r : now) {
                const uint64_t lo = std::max(r.first_row, from), hi = std::min(r.first_row + r.row_count, to);
                if (lo < hi) chunk_runs.push_back({lo, hi - lo});
            }
            if (!chunk_runs.empty()) {
                PassDelay();
                if (!XStore::FoldStoredRows(*fd, hint.row_length, zmap.TrackedColumnCount(), catidx.TrackedColumnCount(),
                                            chunk_runs, zmap, catidx)) {
                    // The bits stay set (the block stays untrusted); the next start rebuilds the dirty sidecars.
                    // Runs before the failed read are already folded and the retry folds them again: count and sum
                    // would be doubled, so the chunk's blocks go inexact (aggregates scan them; min/max/bloom are idempotent).
                    zmap.MarkInexact(chunk_runs.front().first_row, chunk_runs.back().first_row + chunk_runs.back().row_count - 1);
                    if (FirstFailureOf(data_path)) {
                        spdlog::error("[INDEXER] - {}: cannot read rows to index; the year's indexes are marked dirty "
                                      "and rebuilt at the next start", data_path);
                    }
                    zmap.MarkDirty();
                    catidx.MarkDirty();
                    return PassEnd::kDone;
                }
                map.ClearRuns(chunk_runs);   // only after the fold (I1)
            }
        }
        from = to;
    }
    if (chunk_runs.capacity() > (size_t{1} << 16)) chunk_runs = std::vector<IndexRun>{};
    return PassEnd::kDone;
}

}  // namespace xstore
