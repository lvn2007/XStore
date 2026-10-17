#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include "include/XStore/XStore_Tunables.h"

// Zone-map secondary index: per-block min/max/sum/count summary statistics
// for numeric columns, used to prune blocks a range/aggregate query cannot
// possibly need. See docs/XStore_Technical_Reference.md section 4.14 for the
// full design and its rejected alternatives.

inline uint64_t RowsPerBlock(uint16_t row_length) {
    if (row_length == 0) return 1;
    uint64_t rows = XStoreTunables::ZONE_MAP_TARGET_BLOCK_BYTES / row_length;
    return rows < 1 ? 1 : rows;
}

inline uint64_t BlockIndexForRow(uint64_t row_index, uint64_t rows_per_block) {
    return row_index / rows_per_block;
}

// One (block, column) summary. min_value/max_value/sum_sq_value/first_value/
// last_value are only meaningful when valid_count > 0 -- a never-observed
// column in a block has valid_count==0 and these left at their sentinel (see
// ZoneMapFile::ZeroedEntry). Persisted as raw bytes (pread/pwrite), never
// protobuf -- this file never crosses the wire.
struct ZoneMapEntry {
    double   min_value;
    double   max_value;
    double   sum_value;
    double   sum_sq_value;    // sum of squares, for VARIANCE/STDDEV pushdown -- see 2026-09-15 design doc
    double   first_value;     // chronologically-FIRST numeric value in this block, for OHLC's Open -- see 2026-09-19 design doc
    double   last_value;      // chronologically-LAST numeric value in this block, for OHLC's Close
    uint64_t valid_count;     // rows in this block where the column was numeric and set
    uint64_t null_count;      // rows in this block where the column was a hole or non-numeric
    uint64_t first_row_index; // row_index first_value was folded from -- lets an out-of-order (backfill) insert correctly tell whether it's earlier than what's already recorded, not just first-observed-wins
    uint64_t last_row_index;  // row_index last_value was folded from -- same reasoning, for "later than what's recorded"
};
// Bit 63 of null_count marks a block "inexact": a row folded into it was later
// overwritten or deleted, so sum/sum_sq/valid_count/null_count/first/last may
// still count the old row. Set on every entry of the block; consumers must
// then scan the data instead of trusting those fields (min/max stay usable for
// pruning: they can only be too wide). Cleared only by a rebuild, or by a
// RangeDelete that empties the whole block. Part of the v5 format (a real
// null_count never comes near 2^63).
inline constexpr uint64_t ZONE_MAP_INEXACT_BIT = 1ULL << 63;
inline bool ZoneMapEntryInexact(const ZoneMapEntry &e) { return (e.null_count & ZONE_MAP_INEXACT_BIT) != 0; }
inline uint64_t ZoneMapNullCount(const ZoneMapEntry &e) { return e.null_count & ~ZONE_MAP_INEXACT_BIT; }

static_assert(std::is_trivially_copyable<ZoneMapEntry>::value,
              "ZoneMapEntry must be trivially copyable for raw pread/pwrite");
static_assert(sizeof(ZoneMapEntry) == 80, "update size-dependent capacity-planning docs (docs/XStore_Technical_Reference.md §4.14) if this changes");

// Bumped 2 -> 3 when ZoneMapEntry grew first_value/last_value/first_row_index/
// last_row_index (48 -> 80 bytes) for OHLC's Open/Close pushdown. A file
// written under an older version is never migrated in place -- ZoneMapFile::
// Open() truncates and reinitializes it instead (see Open's comment). Named
// out so a future format change is a one-line bump + rebuild, not a magic-
// number hunt across the codebase. 4 -> 5: same layout, but v5 defines bit 63
// of null_count as the inexact flag above; a v4 file may already count
// overwritten or deleted rows with no flag, so it is discarded and rebuilt.
inline constexpr uint32_t ZONE_MAP_VERSION = 5;

struct ZoneMapFileHeader {
    uint32_t magic   = 0x5A4D4150; // 'ZMAP'
    uint32_t version = ZONE_MAP_VERSION;
    uint32_t tracked_column_count = 0;
    // 1 = may hold writes not yet synced alongside the data file (set before
    // the first write of a session, cleared only after data + index are
    // fsynced); 1 at startup => rebuild. See Durability A plan.
    uint32_t dirty = 0;
    uint64_t rows_per_block = 0;
};
static_assert(sizeof(ZoneMapFileHeader) == 24);
static_assert(offsetof(ZoneMapFileHeader, dirty) == 12);

#include <algorithm>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

// One numeric observation to fold into a block's running stats for one
// column. `is_numeric=false` (a hole, or a non-numeric FData branch such as
// stringData/bytesData) increments null_count instead of touching min/max/
// sum -- mirrors AggregateSkipsNonNumericColumnValues' existing semantics
// (XStoreTest.cpp) so a zone map never disagrees with what a real scan
// would have concluded.
struct ZoneMapObservation {
    uint32_t column_idx;
    double   value;
    bool     is_numeric;
};

// Rows for ZoneMapFile::RebuildFoldRows in a flat layout a caller reuses across chunks (no allocation per row once the
// vectors have grown): row i is row_index[i], with observations [obs_end[i-1], obs_end[i]) (obs_end[-1] = 0), each row
// folded exactly as UpdateRow(row_index[i], <its observations>, pad_to) would fold it.
struct ZoneMapFoldBatch {
    std::vector<uint64_t> row_index;
    std::vector<size_t> obs_end;
    std::vector<ZoneMapObservation> observations;
    uint32_t pad_to = 0;
    void Clear() { row_index.clear(); obs_end.clear(); observations.clear(); }
    // Closes the row whose observations were appended since the previous EndRow.
    void EndRow(uint64_t row) { row_index.push_back(row); obs_end.push_back(observations.size()); }
};

enum class BeginRebuildResult { kStarted, kFailed, kAlreadyRebuilding };

// Keep-old begin of a live rebuild (Tech Reference 4.18 "Keep the old index"), shared by ZoneMapFile and CatIdxFile:
// creates "<path>.new" (O_TRUNC) holding exactly the header_bytes at header, fdatasyncs it, renames it over path and
// fsyncs the directory. Returns the new file's fd, or -1 with path untouched and no ".new" left behind. A failed
// directory fsync is only logged: either name a crash leaves holds a dirty index (the old one was marked dirty before),
// which the next start rebuilds.
[[nodiscard]] int CreateIndexReplacement(const std::string &path, const void *header, size_t header_bytes);

// A rebuild cut into groups of consecutive rows that are folded in parallel, each group in row order: end(g) is the end
// of the prefix of group g folded so far, Watermark() the contiguous frontier (every row below it folded). Shared by
// the startup engine (one per year file, under YearRebuild::mutex) and the live builder (inside each index file, under
// the file mutex, where the writers read it too). Not thread-safe by itself.
class RebuildWatermark {
public:
    // group_rows == 0 empties it (Active() false).
    void Reset(uint64_t group_rows, uint64_t total_rows) {
        group_rows_ = group_rows;
        total_rows_ = total_rows;
        prefix_ = 0;
        end_.clear();
        if (group_rows == 0) return;
        for (uint64_t g = 0; g * group_rows < total_rows; ++g) end_.push_back(g * group_rows);
    }
    [[nodiscard]] bool Active() const { return !end_.empty(); }
    // Rows [begin, end) of one group are folded; begin is that group's folded end so far (folds are in row order).
    // Returns the watermark, which never passes the first unfolded row of a group still running (or stopped).
    uint64_t MarkFolded(uint64_t begin, uint64_t end) {
        const size_t g = static_cast<size_t>(begin / group_rows_);
        end_[g] = std::max(end_[g], end);
        while (prefix_ < end_.size() && end_[prefix_] == GroupEnd(prefix_)) ++prefix_;
        return Watermark();
    }
    [[nodiscard]] uint64_t Watermark() const { return prefix_ < end_.size() ? end_[prefix_] : total_rows_; }
    // End of the folded prefix of row's group when row lies in it (> row), else row itself.
    [[nodiscard]] uint64_t FoldedEnd(uint64_t row) const {
        if (end_.empty()) return row;
        const uint64_t g = row / group_rows_;
        return g < end_.size() && row < end_[g] ? end_[g] : row;
    }
    // First row of the group after row's (rows of a group past its folded prefix are not folded).
    [[nodiscard]] uint64_t NextGroupBegin(uint64_t row) const { return (row / group_rows_ + 1) * group_rows_; }
    [[nodiscard]] uint64_t TotalRows() const { return total_rows_; }

private:
    [[nodiscard]] uint64_t GroupEnd(size_t g) const { return std::min(total_rows_, (g + 1) * group_rows_); }
    uint64_t group_rows_ = 0;
    uint64_t total_rows_ = 0;
    size_t prefix_ = 0;
    std::vector<uint64_t> end_;
};

class ZoneMapFile {
public:
    // Creates the file (header only) if it doesn't exist; opens it
    // otherwise. row_length is used only to derive rows_per_block on first
    // creation -- ignored on reopen (rows_per_block is read from the header,
    // so it can never drift from what earlier blocks were written against).
    // tracked_column_count is used on first creation, when reinitializing a
    // stale-version file, AND (Task 6) when it's strictly greater than what
    // an already-current-version file has on disk -- that last case is what
    // lets a rebuild actually widen a zone map whose DB's columnCount grew
    // after the file was first created, instead of leaving it permanently
    // narrow and making ZoneMapNeedsRebuild re-flag it as stale forever.
    // Strictly-greater-only means this can never shrink an existing file:
    // defaulted to the global ZONE_MAP_MAX_TRACKED_COLUMNS so every
    // pre-existing call site (live insert/query paths, which don't yet
    // thread a per-DB columnCount this far) keeps its old behavior
    // unchanged; XStore::RebuildOneYearZoneMap (Task 5) is the one caller
    // that passes the real per-DB EffectiveTrackedColumnCount value, so a
    // rebuilt header converges on that count instead of being reset back to
    // the global default and re-triggering ZoneMapNeedsRebuild on every
    // subsequent InitDB() run.
    [[nodiscard]] bool Open(const std::string &path, uint16_t row_length, uint32_t tracked_column_count = XStoreTunables::ZONE_MAP_MAX_TRACKED_COLUMNS);

    // Folds each observation into the block containing row_index and
    // pwrite()s that block back to disk before returning -- write-through,
    // no buffered state a crash can lose (see the Task 2 design note in the
    // plan for why this isn't a write-back cache). The in-memory cache below
    // only saves a repeat pread() when consecutive calls hit the same block.
    // pad_to is REQUIRED (no default) so every caller is found at compile
    // time (Task 2: observations now only cover present columns, so this
    // tells UpdateRow how far -- e.g. the row's tracked_column_count -- to
    // bump null_count for the columns past observations.size(), clamped to
    // header_.tracked_column_count). Pass observations.size() itself for a
    // caller that built its own already-complete observation vector and
    // wants no additional padding.
    void UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to);

    // The writer's path (ApplyWalRecord, once per run): UpdateRow for every row of the batch, in order, under ONE hold
    // of the mutex, with the same per-row rules (while a rebuild runs: the old index of a keep-old rebuild gets every
    // row, this file only rows below F or in a folded group prefix). Each block is written once per run of consecutive
    // rows in it, before this returns; the bytes on disk are identical to UpdateRow row by row.
    void UpdateRows(const ZoneMapFoldBatch &batch);

    // True when a write of rows [first_row, first_row + row_count) might replace a row already folded into an
    // exact block, so the caller must read the old bytes and call MarkInexact for any it finds. False (no I/O
    // needed) when every touched block is already inexact or provably holds no folded row at or after the run's
    // first row in that block. Caller holds the write range lock for the rows.
    [[nodiscard]] bool NeedsOverwriteCheck(uint64_t first_row, uint64_t row_count);

    // Sets the inexact bit on every block holding a row in [first_row, last_row] that has anything folded into
    // it, and pwrites those blocks (after the dirty mark, like UpdateRow). Call BEFORE changing the rows.
    void MarkInexact(uint64_t first_row, uint64_t last_row);

    // RangeDelete of rows [first_row, last_row]: under mutex_, marks the index dirty, flags the (at most two)
    // partly covered end blocks, runs punch_data (the data-file punch), then resets every fully covered block to
    // the empty, exact state with one fallocate(PUNCH_HOLE) on this file (a hole reads as zeros = a block with no
    // rows), without loading those blocks. If the data punch fails the full blocks are flagged instead; if only
    // this file's punch fails they are flagged too. Returns punch_data's result.
    bool DeleteRows(uint64_t first_row, uint64_t last_row, const std::function<bool()> &punch_data);

    // Live widening, stage 1 mechanics (see docs/XStore_Technical_Reference.md, "Live widening"). While a rebuild runs,
    // rows below the frontier F are folded in this file; rows at or above F are not yet, and the builder will fold
    // them from the data file. Readers trust only blocks that end at or below F; writers (UpdateRow,
    // NeedsOverwriteCheck, MarkInexact, DeleteRows) leave rows at or above F alone. F == kNoRebuild when no rebuild
    // runs, which makes every check above a single compare that is always false.
    // Parallel live builder (SetLiveFoldGroups): groups above F may already be folded (partly, a prefix of each group).
    // F stays the contiguous watermark that readers trust, but writers treat a row in a folded prefix like a row
    // below F (the fold already read the old row; skipping it would lose the update). Checked only after the F
    // compare, so the not-rebuilding path is unchanged.
    static constexpr uint64_t kNoRebuild = UINT64_MAX;

    // Empties this file in place at width new_tracked_column_count and starts a rebuild at F = 0. Order: the old
    // header is marked dirty and fdatasync'd (on failure: returns false, nothing changed); then ftruncate to 0; then a
    // fresh header (new width, dirty=1) + fdatasync; then the block cache is dropped. A crash at any point leaves a
    // file the next start rebuilds (dirty, or too short to hold a header). Same object, same fd: references held by
    // in-flight queries stay valid. False also if the truncate fails (the old index is then still whole and served).
    [[nodiscard]] bool BeginRebuild(uint32_t new_tracked_column_count);
    // Live builder only: BeginRebuild, unless a rebuild already runs on this file -- then kAlreadyRebuilding with nothing
    // touched. The check and the begin are one step under the file mutex, so two builders on one file cannot both begin
    // (the second would truncate the first's work while the first keeps advancing F).
    // keep_old (LIVE_WIDEN_KEEP_OLD_INDEX, Tech Reference 4.18 "Keep the old index"): instead of emptying the file in
    // place, the old index is marked dirty (durably; on failure nothing changes), the new one is created as
    // "<path>.new" at the new width (dirty header, same rows_per_block, fdatasync), renamed over path and the directory
    // fsynced; any failure of those steps returns kFailed with the old index unchanged, trusted and not rebuilding. On
    // success THIS object (same pins, same registry entry) moves to the new file at F = 0 and keeps the old file's
    // state (fd, header, cache) in an inner object until FinishRebuild: blocks not yet complete are answered from the
    // old file at its old width, and every writer call is applied to both files (old first under this mutex; lock order
    // this file's mutex -> the old one's). A stopped or failed build keeps the old file serving.
    [[nodiscard]] BeginRebuildResult BeginLiveRebuild(uint32_t new_tracked_column_count, bool keep_old = false);
    // Width of the old index a keep-old rebuild still serves blocks from; 0 when none. Lock-free.
    [[nodiscard]] uint32_t OldWidth() const { return old_width_.load(std::memory_order_acquire); }
    // Test-visible: TryGetBlockStats calls answered from the old index of a keep-old rebuild.
    [[nodiscard]] uint64_t ServedFromOldForTests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return served_from_old_;
    }
    // Test-visible: block_index as the old index of a keep-old rebuild has it (nullopt without one).
    [[nodiscard]] std::optional<std::vector<ZoneMapEntry>> OldBlockStatsForTests(uint64_t block_index) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (old_ == nullptr) return std::nullopt;
        return old_->TryGetBlockStats(block_index);
    }

    // Startup only, after BeginRebuild failed on a file that must not be trusted (missing, dirty, wrong width or
    // WAL-forced): enters the rebuild state at F = 0 WITHOUT truncating or writing anything. Readers then scan every
    // block, writers skip the index, MarkClean refuses (so it is never cleaned in this session) and the registry never
    // evicts it. Nothing is folded and FinishRebuild is never called, so it stays untrusted until the next start
    // rebuilds it. No-op if a rebuild already runs.
    void EnterUntrustedRebuildState();

    // Builder only, for a row at or above F, while it holds the data-file READ range lock on that row: folds it like
    // UpdateRow does. Ignored for a row below F or when no rebuild runs.
    void RebuildFoldRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to);

    // Builder only: RebuildFoldRow for every row of the batch, in order, under ONE hold of the mutex. A row below F is
    // skipped exactly as RebuildFoldRow skips it. Each block is written once per run of consecutive rows that fall in
    // it (once per chunk when rows ascend), not once per row, with no LRU work per row; the bytes on disk are identical
    // to folding the same rows one by one through RebuildFoldRow. Returns false if a block write failed: that block on
    // disk is then behind the fold, so the caller must not end the rebuild. No-op (true) when no rebuild runs.
    [[nodiscard]] bool RebuildFoldRows(const ZoneMapFoldBatch &batch);

    // Builder only: rows below new_frontier are now folded. Call while still holding the data-file READ range lock
    // over the rows just folded, so no writer can sit on a row F moves across. Never moves F back.
    void AdvanceFrontier(uint64_t new_frontier);

    // Live builder only, right after BeginLiveRebuild and before any fold: the year's total_rows rows are folded in
    // groups of group_rows (whole blocks), possibly in parallel, each group in row order. From now on F moves only
    // through MarkFolded. Cleared by FinishRebuild and by the next BeginRebuild.
    void SetLiveFoldGroups(uint64_t group_rows, uint64_t total_rows);
    // Live builder only, while it still holds the data-file READ range lock over [begin, end) and AFTER folding those
    // rows (or finding them all holes): records them folded (begin is the group's folded end so far) and moves F to
    // the contiguous watermark. Returns F.
    uint64_t MarkFolded(uint64_t begin, uint64_t end);

    // Test-visible: whether a writer of row_index updates this index (below F, or in a folded group prefix).
    [[nodiscard]] bool WriterUpdatesRowForTests(uint64_t row_index) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return row_index < frontier_ || fold_.FoldedEnd(row_index) > row_index;
    }

    // Ends the rebuild (F = kNoRebuild, and the old index of a keep-old rebuild is closed in the same mutex hold) once
    // the dirty header is durable: it retries the header first, and if that still fails returns false with nothing changed (still rebuilding, still pinned, MarkClean still refuses), so a
    // header-less file can never be cleaned and later reopened as an empty, clean index. Retry later. On success the
    // file stays dirty until a later MarkClean, as after any write.
    [[nodiscard]] bool FinishRebuild();

    // Lock-free (the registry calls it under its own mutex, which must never wait on this file's mutex).
    [[nodiscard]] bool Rebuilding() const { return rebuilding_.load(std::memory_order_acquire); }

    // Test-visible: the rebuild frontier F (kNoRebuild when no rebuild runs).
    [[nodiscard]] uint64_t FrontierForTests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return frontier_;
    }

    // Registry pin: while one is alive the registry never evicts this file, so the pointer stays valid. Taken only by
    // XStore::PinZoneMap under the registry mutex; released from any thread.
    class Pin {
    public:
        Pin() = default;
        Pin(Pin &&other) noexcept : file_(other.file_) { other.file_ = nullptr; }
        Pin &operator=(Pin &&other) noexcept {
            if (this != &other) { Release(); file_ = other.file_; other.file_ = nullptr; }
            return *this;
        }
        Pin(const Pin &) = delete;
        Pin &operator=(const Pin &) = delete;
        ~Pin() { Release(); }
        ZoneMapFile &operator*() const { return *file_; }
        ZoneMapFile *operator->() const { return file_; }
        ZoneMapFile *get() const { return file_; }
    private:
        // Only XStore::Pin* create a pin, under the registry mutex (eviction and retire read the count under it).
        friend class XStore;
        explicit Pin(ZoneMapFile *file) : file_(file) { if (file_ != nullptr) file_->pins_.fetch_add(1, std::memory_order_relaxed); }
        void Release() { if (file_ != nullptr) file_->pins_.fetch_sub(1, std::memory_order_release); file_ = nullptr; }
        ZoneMapFile *file_ = nullptr;
    };

    // Registry eviction skips a pinned or rebuilding file. Lock-free, like Rebuilding().
    [[nodiscard]] bool Evictable() const {
        return pins_.load(std::memory_order_acquire) == 0 && !rebuilding_.load(std::memory_order_acquire);
    }

    // A live Pin exists. Pins are taken only under the registry mutex, so a caller holding it that sees false knows
    // nobody holds the file and nobody can start to.
    [[nodiscard]] bool Pinned() const { return pins_.load(std::memory_order_acquire) != 0; }

    // Test-visible: block pwrites made by MarkInexact/DeleteRows, plus one per successful DeleteRows reset punch.
    [[nodiscard]] uint64_t MarkWritesForTests() const;

    // Test-visible: whether block_index carries the inexact bit.
    [[nodiscard]] bool BlockInexact(uint64_t block_index);

    // Always returns real, safe stats for block_index -- a never-touched
    // block simply has valid_count==0 everywhere, which is the true and
    // correct answer for "no rows landed here yet," not a placeholder for
    // "unknown." nullopt means an I/O failure on the read, or a block a
    // running rebuild has not finished yet (callers scan it).
    std::optional<std::vector<ZoneMapEntry>> TryGetBlockStats(uint64_t block_index);

    ~ZoneMapFile();

    uint64_t RowsPerBlockValue() const { return header_.rows_per_block; }

    // Locked: BeginRebuild changes the width of a live object.
    [[nodiscard]] uint32_t TrackedColumnCount() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return header_.tracked_column_count;
    }

    // Clears the on-disk dirty flag: fdatasync(index content), then header
    // dirty=0 + fdatasync. Returns false on any failure, leaving the file
    // marked dirty, and while a rebuild runs (a half-built index must stay
    // dirty). The caller must have fdatasync'd the .XSTORE data file
    // FIRST -- this only makes the index itself durable.
    [[nodiscard]] bool MarkClean();

    // Marks the index dirty on disk now. A rebuild calls this before it touches
    // anything, so an early exit that folded no row cannot leave an empty index
    // marked clean. A failure is logged; the next UpdateRow retries.
    void MarkDirty();

    // Number of successful header dirty-mark writes (test-visible).
    [[nodiscard]] uint64_t DirtyMarkWritesForTests() const;

private:
    struct CachedBlock {
        std::vector<ZoneMapEntry> entries;
        // One past the highest row folded into this block; valid only when fold_end_known. Known for a block
        // loaded with nothing folded, or whose column 0 has no nulls (every folded row counts once in column 0,
        // so its last_row_index is then the highest folded row). Otherwise unknown, also after a reload while column 0
        // still counts a null.
        uint64_t fold_end = 0;
        bool fold_end_known = false;
    };
    // Every method that writes index content calls this first (mutex_ held):
    // one predictable branch once the flag is on disk.
    void EnsureDirtyOnDisk() { if (!dirty_on_disk_) MarkDirtyOnDisk(); }
    bool BeginRebuildLocked(uint32_t new_tracked_column_count); // mutex_ held
    bool BeginKeepOldLocked(uint32_t new_tracked_column_count); // mutex_ held
    // Fresh object only: takes over from's open file (fd, header, dirty flag, cache; not the path, which now names the
    // new file); from is then re-pointed. Moves only: never throws.
    void AdoptOpenState(ZoneMapFile &from) noexcept;
    [[nodiscard]] bool NeedsOverwriteCheckRebuildingLocked(uint64_t first_row, uint64_t row_count);
    void MarkDirtyOnDisk();

    CachedBlock &EnsureBlockLoaded(uint64_t block_index);
    void UpdateRowLocked(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to);
    // The fold of one row into its (loaded) block, shared by UpdateRowLocked and RebuildFoldRows.
    void FoldRowIntoBlock(CachedBlock &block, uint64_t row_index, const ZoneMapObservation *observations, size_t count,
                          uint32_t pad_to);
    // The batch fold shared by UpdateRows and RebuildFoldRows: every row with keep(row) true. mutex_ held.
    template <class Keep>
    [[nodiscard]] bool FoldBatchLocked(const ZoneMapFoldBatch &batch, Keep keep);
    [[nodiscard]] bool WriteBlock(uint64_t block_index, const CachedBlock &block);
    void TouchLru(uint64_t block_index);
    void MarkInexactLocked(uint64_t first_row, uint64_t last_row);
    void MarkInexactRunLocked(uint64_t first_row, uint64_t last_row); // rows all below F or folded
    [[nodiscard]] bool BlocksNeedOverwriteCheck(uint64_t first_row, uint64_t last_row); // rows all below F or folded
    // Calls fn(first, last) for each maximal run of [first_row, last_row] whose rows a writer must update (below F, or
    // in a folded group prefix), in ascending order. mutex_ held.
    template <class Fn>
    void ForEachWriterRun(uint64_t first_row, uint64_t last_row, Fn &&fn) const;
    void EvictIfOverCapacity();
    off_t BlockFileOffset(uint64_t block_index) const;
    // Resets header_ to a fresh, empty header marked dirty (one pwrite, no fdatasync) and writes it to offset 0
    // -- shared by first-time file creation and by Open()'s stale-version
    // discard path, so there's exactly one place that constructs a valid
    // fresh header.
    bool WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count);

    int fd_ = -1;
    ZoneMapFileHeader header_;
    bool dirty_on_disk_ = false; // header_.dirty == 1 is durable on disk (header_.dirty may be 1 without it)
    uint64_t dirty_mark_writes_ = 0;
    uint64_t mark_writes_ = 0;
    uint64_t frontier_ = kNoRebuild; // rebuild frontier F, see kNoRebuild
    RebuildWatermark fold_;          // folded group prefixes of a parallel live rebuild (inactive otherwise)
    std::atomic<bool> rebuilding_{false}; // frontier_ != kNoRebuild; written under mutex_, read without it
    std::atomic<uint32_t> pins_{0};       // live Pin objects
    std::unordered_map<uint64_t, CachedBlock> cache_; // pure read-cache, always disk-coherent
    std::vector<uint64_t> lru_order_; // most-recently-touched at the back
    // GetOrOpenZoneMap hands out a ZoneMapFile& to one shared, process-wide
    // instance per (db,year) -- every server worker thread touching the
    // same DB/year concurrently calls UpdateRow/TryGetBlockStats on this
    // SAME object. Guards cache_/lru_order_/fd_ against exactly that.
    mutable std::mutex mutex_;
    // Appended last so every older member keeps its offset (hot-path layout unchanged by keep-old).
    uint64_t served_from_old_ = 0;
    std::string path_;                    // set by Open; the keep-old begin creates path_ + ".new"
    std::unique_ptr<ZoneMapFile> old_;    // keep-old rebuild only: the old index, until FinishRebuild (under mutex_)
    std::atomic<uint32_t> old_width_{0};  // old_'s width, 0 without old_; written under mutex_, read without it
};
