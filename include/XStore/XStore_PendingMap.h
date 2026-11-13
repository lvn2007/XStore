#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "include/XStore/XStore_IndexRun.h"

namespace xstore {

// Rows of one year file whose index update is outstanding (spec: docs/superpowers/specs/2026-11-04-async-indexer-design.md,
// 4.2). Writers MarkRows under their WRITE range lock before the new bytes become readable; whoever folds the rows
// ClearRows under a READ range lock after the fold. Queries ask IsPending(block) and treat a pending block as
// untrusted. The exact per-row bits sit behind a mutex; queries only read the lock-free summary, a table of counters
// indexed by a hash of the block number. A colliding block can read as pending (a false positive: the query scans it);
// a pending block never reads as clear.
class PendingMap {
public:
    // Each slot is a uint16_t counter of the pending blocks hashing to it. It would overflow only with 65,536 pending
    // blocks in one slot, i.e. a file of about 2^31 blocks (2^16 per slot x 2^15 slots) all pending at once: not reachable.
    static constexpr size_t SUMMARY_SLOTS = size_t{1} << 15;

    explicit PendingMap(uint64_t rows_per_block);
    ~PendingMap();   // the map's pending blocks leave the process-wide count (see PendingMaps::AnyPending)

    void MarkRows(uint64_t first_row, uint64_t row_count);
    void ClearRows(uint64_t first_row, uint64_t row_count);
    // Clears exactly the rows in `runs` (the rows the caller folded); other rows of a block stay pending.
    void ClearRuns(const std::vector<IndexRun> &runs);
    [[nodiscard]] bool IsPending(uint64_t block) const;
    // Whether any block intersecting [first_row, last_row] is pending (block granularity: a query that touches a pending
    // block scans all of it). Costs min(blocks in the range, pending blocks) lookups.
    [[nodiscard]] bool AnyPendingInRows(uint64_t first_row, uint64_t last_row) const;
    // Pending runs of one block, ascending, row indexes absolute to the file. Empty if the block is not pending.
    [[nodiscard]] std::vector<IndexRun> PendingRunsOfBlock(uint64_t block) const;
    // Every pending run of the map, ascending by block (a run never spans blocks).
    [[nodiscard]] std::vector<IndexRun> AllPendingRuns() const;
    [[nodiscard]] size_t PendingBlockCount() const;
    [[nodiscard]] uint64_t TotalMarkedRowsForTests() const;
    [[nodiscard]] uint64_t RowsPerBlock() const { return rows_per_block_; }

private:
    struct BlockBits {
        std::vector<uint64_t> words;   // one bit per row of the block
        uint64_t set_bits = 0;
    };
    [[nodiscard]] static size_t SlotOf(uint64_t block);
    [[nodiscard]] static std::vector<IndexRun> RunsOfBits(uint64_t base_row, const std::vector<uint64_t> &words);
    void ClearRowsLocked(uint64_t first_row, uint64_t row_count);   // mutex_ held

    const uint64_t rows_per_block_;
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, BlockBits> blocks_;
    std::unique_ptr<std::atomic<uint16_t>[]> summary_;
    uint64_t total_marked_ = 0;
};

// Process-wide registry, keyed like the zone-map and catidx registries: "{db_base_path}/{year}".
class PendingMaps {
public:
    // For() sweeps the registry only while it holds more entries than this. The zone-map and catidx registries keep at
    // most 256 files each (ZONE_MAP_REGISTRY_CAP, CATIDX_REGISTRY_CAP), and their files of one year share one map, so
    // twice the larger cap leaves room for the maps those files hold and for evicted files still pinned: a file open
    // stays O(1), and the registry stays bounded (each map holds a 64 KiB summary).
    static constexpr size_t SWEEP_ABOVE = 512;

    // Returns the year's map, creating it if needed. Before it creates one in a registry past SWEEP_ABOVE entries it drops
    // every map nobody else holds (no file, no Find caller) with no pending block; a map with pending marks always stays.
    // The sweep is O(registry), so it runs again only once the registry has doubled since the last one.
    // An existing map with another rows_per_block is replaced if nothing is pending in it; if rows are pending the
    // existing map is returned unchanged (and the mismatch logged once): the marks belong to its holders.
    static std::shared_ptr<PendingMap> For(const std::string &key, uint64_t rows_per_block);
    static std::shared_ptr<PendingMap> Find(const std::string &key);
    // Moves the pending marks of the year's map into a map with `rows_per_block` and registers it under `key` (item D).
    // Rows are absolute indexes, so only the block grouping changes. The caller must be the exclusive user of the year
    // (the widen holds the file; nothing marks during the call) and must re-attach (AttachPending) every file of the year
    // to the returned map before sharing it: a file still holding the old map reads it as empty. A key with no map, or
    // one with no pending block, is a plain create/replace; the same block size returns the existing map.
    static std::shared_ptr<PendingMap> Rehome(const std::string &key, uint32_t rows_per_block);
    // Advisory fast path: false means no block of any registered (or still held) map was pending at this instant. A map
    // dropped from the registry but still held keeps counting until its last holder lets go (over-reports: the safe side).
    static bool AnyPending();
    // `match` runs under the registry mutex: it must not call back into PendingMaps.
    static void ForgetMatching(const std::function<bool(const std::string &)> &match);
    static void ClearForTests();
    [[nodiscard]] static size_t SizeForTests();
    [[nodiscard]] static size_t SweepCountForTests();   // sweeps run since the process started

    // Whether the aggregates and FindByValue add the "not indexed yet" notice: true only while the indexer pool runs
    // (Indexer::Start sets it, its stop clears it). With the pool off indexing is inline, so the only marks a query can
    // see are a writer's in-flight ones (ApplyWalRecord marks a run, writes it and folds it under its WRITE range lock):
    // the scan behind the notice waits on that lock and reads a folded index, so the notice would be false. With the pool
    // on a mark that outlives the writer's lock means "not indexed yet"; the notice may overstate by a block that caught
    // up (or a racing inline fold) during the call, never omit one that was pending when the call began.
    [[nodiscard]] static bool NoticesEnabled();
    static void SetNoticesEnabled(bool enabled);   // the indexer's Start / stop
    static void SetNoticesEnabledForTests(bool enabled);   // the last call of either setter wins
};

}  // namespace xstore
