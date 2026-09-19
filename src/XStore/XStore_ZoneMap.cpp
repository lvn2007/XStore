#include "include/XStore/XStore_ZoneMap.h"
#include "include/Logger/Logger.h"
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

bool ZoneMapFile::Open(const std::string &path, uint16_t row_length, uint32_t tracked_column_count) {
    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) return false;

    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= static_cast<off_t>(sizeof(ZoneMapFileHeader))) {
        ssize_t read_bytes = pread(fd_, &header_, sizeof(header_), 0);
        if (read_bytes != static_cast<ssize_t>(sizeof(header_)) || header_.magic != ZoneMapFileHeader{}.magic) {
            return false; // corrupt/foreign file -- caller falls back to no acceleration
        }
        if (header_.version != ZONE_MAP_VERSION || tracked_column_count > header_.tracked_column_count) {
            // A zone map is always a derived, rebuildable index, never a
            // source of truth (TryGetBlockStats already treats "no
            // coverage" as a valid answer, not an error) -- discarding
            // old-format bytes is always safe: the only effect is losing
            // pushdown acceleration for this file until XStore::InitDB's
            // startup rebuild (see Task 5) repopulates it from raw data.
            // Truncating and rewriting a fresh header is simpler and just
            // as safe as trying to migrate old 40-byte entries into the
            // new 48-byte layout in place.
            //
            // The tracked_column_count > header_.tracked_column_count arm
            // is the Task 6 fix: RebuildOneYearZoneMap (Task 5) passes the
            // real, possibly-grown per-DB EffectiveTrackedColumnCount, but
            // an existing file already at the current ZONE_MAP_VERSION used
            // to short-circuit past this whole branch, silently keeping its
            // old (narrower) on-disk tracked_column_count forever -- so
            // ZoneMapNeedsRebuild's mismatch check would never converge,
            // and every InitDB() would re-scan and re-fold the same rows
            // into a too-narrow header without ever tracking the newly-wide
            // columns. Strictly-greater (never `!=`) matters: it guarantees
            // this can only ever widen, never shrink -- an insert-path
            // caller that (today) still passes the global default has no
            // way to accidentally truncate away a file some earlier rebuild
            // already widened.
            if (ftruncate(fd_, 0) != 0) {
                spdlog::error("[ZONE MAP] - Failed to truncate stale {}", path);
                return false;
            }
            return WriteFreshHeader(row_length, tracked_column_count);
        }
    } else {
        return WriteFreshHeader(row_length, tracked_column_count);
    }
    return true;
}

bool ZoneMapFile::WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count) {
    header_ = ZoneMapFileHeader{};
    header_.tracked_column_count = tracked_column_count;
    header_.rows_per_block = RowsPerBlock(row_length);
    ssize_t written = pwrite(fd_, &header_, sizeof(header_), 0);
    if (written != static_cast<ssize_t>(sizeof(header_))) {
        spdlog::error("[ZONE MAP] - Failed to write header to zone map file");
        return false;
    }
    return true;
}

off_t ZoneMapFile::BlockFileOffset(uint64_t block_index) const {
    return static_cast<off_t>(sizeof(ZoneMapFileHeader)) +
           static_cast<off_t>(block_index) * header_.tracked_column_count * sizeof(ZoneMapEntry);
}

void ZoneMapFile::EnsureBlockLoaded(uint64_t block_index) {
    if (cache_.count(block_index)) return;

    std::vector<ZoneMapEntry> entries(header_.tracked_column_count, ZoneMapEntry{});

    off_t offset = BlockFileOffset(block_index);
    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= offset + static_cast<off_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) {
        // Existing on-disk block -- load its current (always-durable) stats.
        [[maybe_unused]] ssize_t read_bytes =
            pread(fd_, entries.data(), header_.tracked_column_count * sizeof(ZoneMapEntry), offset);
    }
    // Otherwise: never-touched block, zero-initialized entries above are correct.

    cache_.emplace(block_index, std::move(entries));
    lru_order_.push_back(block_index);
    EvictIfOverCapacity();
}

void ZoneMapFile::EvictIfOverCapacity() {
    // Pure memory-bound eviction -- nothing to flush, every cached copy is
    // already disk-coherent by the time UpdateRow returns.
    while (cache_.size() > XStoreTunables::ZONE_MAP_HOT_CACHE_BLOCKS) {
        uint64_t victim = lru_order_.front();
        lru_order_.erase(lru_order_.begin());
        cache_.erase(victim);
    }
}

void ZoneMapFile::UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations) {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t block_index = BlockIndexForRow(row_index, header_.rows_per_block);
    EnsureBlockLoaded(block_index);

    lru_order_.erase(std::remove(lru_order_.begin(), lru_order_.end(), block_index), lru_order_.end());
    lru_order_.push_back(block_index);

    std::vector<ZoneMapEntry> &entries = cache_.at(block_index);
    for (const auto &obs : observations) {
        if (obs.column_idx >= header_.tracked_column_count) continue; // beyond cap -- not tracked, always safe
        ZoneMapEntry &e = entries[obs.column_idx];
        if (obs.is_numeric) {
            e.min_value = (e.valid_count == 0) ? obs.value : std::min(e.min_value, obs.value);
            e.max_value = (e.valid_count == 0) ? obs.value : std::max(e.max_value, obs.value);
            e.sum_value += obs.value;
            e.sum_sq_value += obs.value * obs.value;
            // Compared against the on-disk row_index, not first-observed-wins
            // -- a backfilled (out-of-order) insert must still correctly
            // displace an already-recorded first/last if it's chronologically
            // more extreme, the same way min/max above compare against the
            // running value rather than trusting insertion order.
            if (e.valid_count == 0 || row_index < e.first_row_index) {
                e.first_value = obs.value;
                e.first_row_index = row_index;
            }
            if (e.valid_count == 0 || row_index > e.last_row_index) {
                e.last_value = obs.value;
                e.last_row_index = row_index;
            }
            e.valid_count++;
        } else {
            e.null_count++;
        }
    }

    // Write-through: durable before this call returns, no dirty state left
    // behind for a crash to lose. See the Task 2 design note for why.
    off_t offset = BlockFileOffset(block_index);
    ssize_t written = pwrite(fd_, entries.data(), entries.size() * sizeof(ZoneMapEntry), offset);
    if (written != static_cast<ssize_t>(entries.size() * sizeof(ZoneMapEntry))) {
        // Short/failed write -- the in-memory cache_ entry above is now
        // AHEAD of what's actually durable on disk for this block, which is
        // exactly the false-negative-pruning risk the write-through design
        // exists to prevent (see the Task 2 design note): a crash right
        // after this would leave a corrupted/incomplete on-disk baseline
        // that a later reopen would treat as trustworthy. UpdateRow's
        // signature is fire-and-forget (no failure-reaction path in this
        // plan), so this is a log-and-continue, matching how
        // XStore_Wal.cpp treats an equivalent short/failed durability
        // write it also can't propagate synchronously.
        spdlog::error("[ZONE MAP] - Short/failed write to block {}", block_index);
    }
}

std::optional<std::vector<ZoneMapEntry>> ZoneMapFile::TryGetBlockStats(uint64_t block_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto cached = cache_.find(block_index);
    if (cached != cache_.end()) return cached->second; // always disk-coherent, safe to serve directly

    off_t offset = BlockFileOffset(block_index);
    struct stat st{};
    if (fstat(fd_, &st) != 0) return std::nullopt; // real I/O failure
    if (st.st_size < offset + static_cast<off_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) {
        // Never-touched block -- a real, safe zero'd answer, not an error.
        return std::vector<ZoneMapEntry>(header_.tracked_column_count, ZoneMapEntry{});
    }
    std::vector<ZoneMapEntry> entries(header_.tracked_column_count);
    ssize_t read_bytes = pread(fd_, entries.data(), header_.tracked_column_count * sizeof(ZoneMapEntry), offset);
    if (read_bytes != static_cast<ssize_t>(header_.tracked_column_count * sizeof(ZoneMapEntry))) return std::nullopt;
    return entries;
}

ZoneMapFile::~ZoneMapFile() {
    if (fd_ >= 0) close(fd_);
}
