#pragma once

#include <atomic>
#include <chrono>
#include <array>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>
#include <blockingconcurrentqueue.h>
#include "include/XStore/XStore_PendingMap.h"
#include "include/XStore/XStore_Tunables.h"

namespace xstore {

struct IndexerConfig {
    uint32_t threads = 1;            // >= 1
    uint64_t backlog_bytes = 0;      // pending ROW bytes; 0 = synchronous (no pool)
    // A record is handed off only when ASYNC and its average contiguous run is >= this (HandOffToPool); >= 1.
    uint32_t min_run_rows = XStoreTunables::INDEXER_MIN_RUN_ROWS_DEFAULT;
    // TESTS ONLY (no env variable): SYNC/BYPASS records are handed off too. Most indexer tests insert SYNC (the apply has
    // finished when the call returns) and still need the hand-off path under test.
    bool hand_off_sync = false;
    // Workers run at SCHED_IDLE (env XSTORE_INDEXER_IDLE=1). Last, so the positional test configs above stay as they are.
    bool idle_priority = XStoreTunables::INDEXER_IDLE_PRIORITY_DEFAULT;
};

// Reads XSTORE_INDEXER_THREADS / XSTORE_INDEXER_BACKLOG_MB / XSTORE_INDEXER_MIN_RUN_ROWS / XSTORE_INDEXER_IDLE once; defaults from XStoreTunables.
IndexerConfig IndexerConfigFromEnv();

// One hint: "this block of this year file may have pending rows". The bits are the authority.
struct IndexHint {
    std::shared_ptr<PendingMap> map;     // the map the writer marked through (never look it up again by key); empty = stop sentinel
    std::string db_base_path;
    std::string db_name;
    unsigned short year = 0;
    uint16_t row_length = 0;
    uint32_t tracked_column_count = 0;   // width the writer saw (the fold uses the file's current width)
    uint64_t block = 0;
    // Backlog bytes the writer reserved for this hint (TryReserve). Released exactly once, whatever becomes of the hint
    // (folded, stale, failed, dropped by a stop or by Enqueue); a re-queue carries it along. Per hint, not per folded
    // row: a row overwritten while pending is reserved twice but folded once, and a failed fold leaves its rows pending.
    uint64_t reserved_bytes = 0;
};

class Indexer {
public:
    static Indexer &Instance();
    ~Indexer();
    Indexer(const Indexer &) = delete;
    Indexer &operator=(const Indexer &) = delete;

    // Starts `config.threads` threads when backlog_bytes > 0; backlog_bytes == 0 starts none and Active() is false.
    // An already started pool is stopped and restarted; the backlog counter restarts at 0.
    void Start(const IndexerConfig &config);
    void StopAndDrain(std::chrono::milliseconds bound);   // returns after the queue is empty or the bound passed
    [[nodiscard]] bool Active() const { return active_.load(std::memory_order_acquire); }
    // The config of the last Start (meaningful while Active(): stored before active_ is released).
    [[nodiscard]] uint32_t MinRunRows() const { return min_run_rows_.load(std::memory_order_relaxed); }
    [[nodiscard]] bool HandOffSync() const { return hand_off_sync_.load(std::memory_order_relaxed); }
    // Backlog accounting in ROW BYTES. TryReserve succeeds iff Active() and used + bytes <= backlog_bytes.
    [[nodiscard]] bool TryReserve(uint64_t bytes);
    void Release(uint64_t bytes);
    [[nodiscard]] uint64_t BacklogBytes() const { return used_bytes_.load(std::memory_order_relaxed); }
    // Takes over hint.reserved_bytes; a hint that is not queued (pool inactive, empty map, its year key covered by a
    // removal in progress) releases it at once.
    void Enqueue(IndexHint hint);
    // Drops the queued hints whose year key ("<db_base_path>/<year>", the registry key) matches, releasing each one's
    // reservation once; the others stay queued. A removal calls it before it forgets the years' pending maps.
    void DropHints(const std::function<bool(const std::string &year_key)> &match);
    static void SetConfigForTests(const IndexerConfig &config);   // restarts the pool; tests only

    // Runs one pass synchronously on the calling thread (the pool's threads run the same RunPass).
    void RunPassForTests(const IndexHint &hint) { RunPass(hint); }
    // Called under a pass's READ lock after the chunk's runs are re-read and before they are folded (only when there
    // is something to fold). Null in production.
    static void SetPassDelayForTests(std::function<void()> cb);
    // Called once per pass after it took its year key and found the year still registered, before it opens anything.
    static void SetPassOpenDelayForTests(std::function<void()> cb);
    // Returns once no hint is queued or in a pool pass (at once with no pool), or after `bound` (logs a warning). Not
    // counted: RunPassForTests passes. For callers about to drop registry entries (a drop ignores pins): the
    // Clear*RegistryForTests hooks and BeginYearRebuild (a no-op in production, where InitDB runs before Start).
    void WaitIdle(std::chrono::milliseconds bound);

private:
    enum class PassEnd { kDone, kRequeue };
    static constexpr size_t PASS_STRIPES = 64;

    Indexer() = default;
    void WorkerLoop(bool idle_priority);
    void StopLocked(std::chrono::milliseconds bound);
    void RunPass(const IndexHint &hint);
    [[nodiscard]] PassEnd PassBody(const IndexHint &hint, const std::string &key);
    [[nodiscard]] static std::string YearKey(const IndexHint &hint);   // "<db_base_path>/<year>", the registry key
    [[nodiscard]] static bool CoveredByRemoval(const IndexHint &hint);
    [[nodiscard]] bool FirstFailureOf(const std::string &data_path);

    std::mutex lifecycle_mutex_;                 // Start / StopAndDrain
    std::vector<std::thread> workers_;
    moodycamel::BlockingConcurrentQueue<IndexHint> queue_;
    std::atomic<bool> active_{false};
    std::atomic<bool> stopping_{false};          // the drain bound passed: workers drop what is left
    std::atomic<uint64_t> outstanding_{0};       // hints queued or in a worker's pass
    std::atomic<uint64_t> limit_bytes_{0};
    std::atomic<uint64_t> used_bytes_{0};
    std::atomic<uint32_t> min_run_rows_{XStoreTunables::INDEXER_MIN_RUN_ROWS_DEFAULT};
    std::atomic<bool> hand_off_sync_{false};
    // Passes of one block run one at a time: two READ locks do not exclude each other, so two passes of a block (a
    // duplicate hint) would both re-read the same bits and fold them twice. Striped by (map, block).
    std::array<std::mutex, PASS_STRIPES> pass_mutexes_;
    std::mutex failure_mutex_;
    std::unordered_set<std::string> failed_files_;   // data files whose failure was logged (once per file)
};

}  // namespace xstore
