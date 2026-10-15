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
        dirty_on_disk_ = header_.dirty != 0;
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
            // The discarded file may have described rows that are still in the data file: mark the empty one dirty
            // so a start that finds it (e.g. after a crash before InitDB's rebuild) rebuilds it instead of trusting
            // it. A rebuild never comes here (it truncates the file to 0 bytes before opening it).
            if (!WriteFreshHeader(row_length, tracked_column_count)) return false;
            MarkDirtyOnDisk();
            return true;
        }
    } else {
        // Too short for a header: new, or left by a crash between a rebuild's truncate and its header write. The two
        // look the same, so the fresh header goes out dirty, in ONE pwrite and with NO fdatasync (Open runs under the
        // global registry mutex). Crash-safe unsynced: the dirty header either reached disk (dirty => rebuild) or did
        // not (still too short => rebuild); it can never read clean. dirty_on_disk_ stays false, so the first write's
        // EnsureDirtyOnDisk makes it durable under this file's mutex, and MarkClean still cleans an unwritten file.
        if (!WriteFreshHeader(row_length, tracked_column_count)) return false;
    }
    return true;
}

bool ZoneMapFile::WriteFreshHeader(uint16_t row_length, uint32_t tracked_column_count) {
    header_ = ZoneMapFileHeader{};
    dirty_on_disk_ = false;
    header_.dirty = 1; // never written clean: see Open
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

ZoneMapFile::CachedBlock &ZoneMapFile::EnsureBlockLoaded(uint64_t block_index) {
    auto found = cache_.find(block_index);
    if (found != cache_.end()) return found->second;

    CachedBlock block;
    block.entries.assign(header_.tracked_column_count, ZoneMapEntry{});
    block.fold_end_known = true; // never-touched block: nothing folded

    off_t offset = BlockFileOffset(block_index);
    const size_t block_bytes = header_.tracked_column_count * sizeof(ZoneMapEntry);
    struct stat st{};
    if (fstat(fd_, &st) != 0) {
        block.fold_end_known = false;
    } else if (st.st_size >= offset + static_cast<off_t>(block_bytes)) {
        // Existing on-disk block -- load its current (always-durable) stats.
        if (pread(fd_, block.entries.data(), block_bytes, offset) != static_cast<ssize_t>(block_bytes)) {
            block.fold_end_known = false;
        } else if (!block.entries.empty()) {
            const ZoneMapEntry &e0 = block.entries[0];
            if (ZoneMapNullCount(e0) != 0) {
                block.fold_end_known = false;
            } else if (e0.valid_count != 0) {
                block.fold_end = e0.last_row_index + 1;
            }
        }
    }
    // Otherwise: never-touched block, zero-initialized entries above are correct.

    lru_order_.push_back(block_index);
    CachedBlock &ref = cache_.emplace(block_index, std::move(block)).first->second;
    EvictIfOverCapacity(); // never evicts block_index: it is the newest in lru_order_
    return ref;
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

void ZoneMapFile::UpdateRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to) {
    std::lock_guard<std::mutex> lock(mutex_);
    // A running rebuild folds this row later, from the data file, unless a parallel group already folded it.
    if (row_index >= frontier_ && fold_.FoldedEnd(row_index) <= row_index) return;
    UpdateRowLocked(row_index, observations, pad_to);
}

void ZoneMapFile::RebuildFoldRow(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild || row_index < frontier_) return; // not rebuilding, or already folded
    UpdateRowLocked(row_index, observations, pad_to);
}

void ZoneMapFile::UpdateRowLocked(uint64_t row_index, const std::vector<ZoneMapObservation> &observations, uint32_t pad_to) {
    EnsureDirtyOnDisk(); // covers the block pwrite below
    uint64_t block_index = BlockIndexForRow(row_index, header_.rows_per_block);
    CachedBlock &block = EnsureBlockLoaded(block_index);
    TouchLru(block_index);
    FoldRowIntoBlock(block, row_index, observations.data(), observations.size(), pad_to);
    // Write-through: durable before this call returns, no dirty state left
    // behind for a crash to lose. See the Task 2 design note for why.
    // UpdateRow's signature is fire-and-forget (no failure-reaction path), so
    // a failed write is logged inside WriteBlock and otherwise ignored here.
    static_cast<void>(WriteBlock(block_index, block));
}

bool ZoneMapFile::RebuildFoldRows(const ZoneMapFoldBatch &batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild) return true;
    bool all_written = true;
    CachedBlock *block = nullptr;
    uint64_t block_index = 0;
    size_t obs_begin = 0;
    for (size_t i = 0; i < batch.row_index.size(); obs_begin = batch.obs_end[i], ++i) {
        const uint64_t row_index = batch.row_index[i];
        if (row_index < frontier_) continue; // already folded
        const uint64_t b = BlockIndexForRow(row_index, header_.rows_per_block);
        if (block == nullptr || b != block_index) {
            // Writing the previous block before loading the next keeps every cached block disk-coherent, as the
            // per-row path does, so a reload (cache eviction, out-of-order rows) reads what was folded.
            if (block != nullptr) all_written &= WriteBlock(block_index, *block);
            EnsureDirtyOnDisk();
            block = &EnsureBlockLoaded(b);
            TouchLru(b);
            block_index = b;
        }
        FoldRowIntoBlock(*block, row_index, batch.observations.data() + obs_begin, batch.obs_end[i] - obs_begin,
                         batch.pad_to);
    }
    if (block != nullptr) all_written &= WriteBlock(block_index, *block);
    return all_written;
}

void ZoneMapFile::TouchLru(uint64_t block_index) {
    lru_order_.erase(std::remove(lru_order_.begin(), lru_order_.end(), block_index), lru_order_.end());
    lru_order_.push_back(block_index);
}

void ZoneMapFile::FoldRowIntoBlock(CachedBlock &block, uint64_t row_index, const ZoneMapObservation *observations,
                                   size_t count, uint32_t pad_to) {
    std::vector<ZoneMapEntry> &entries = block.entries;
    if (count > 0 || (pad_to > 0 && header_.tracked_column_count > 0)) {
        block.fold_end = std::max(block.fold_end, row_index + 1);
    }
    for (size_t i = 0; i < count; ++i) {
        const ZoneMapObservation &obs = observations[i];
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

    // Columns beyond the last observation (row narrower than this file's
    // tracked width -- Task 2: observations only cover present columns now)
    // are holes exactly like an explicitly-unset present column above, so
    // they still bump null_count -- clamped to this file's own tracked width,
    // never writing past entries' bounds.
    const uint32_t pad_end = std::min(pad_to, header_.tracked_column_count);
    for (uint32_t col = static_cast<uint32_t>(count); col < pad_end; ++col) {
        entries[col].null_count++;
    }
}

bool ZoneMapFile::WriteBlock(uint64_t block_index, const CachedBlock &block) {
    const std::vector<ZoneMapEntry> &entries = block.entries;
    ssize_t written = pwrite(fd_, entries.data(), entries.size() * sizeof(ZoneMapEntry), BlockFileOffset(block_index));
    if (written == static_cast<ssize_t>(entries.size() * sizeof(ZoneMapEntry))) return true;
    // Short/failed write -- the in-memory cache_ entry is now AHEAD of
    // what's actually durable on disk for this block, which is exactly the
    // false-negative-pruning risk the write-through design exists to prevent
    // (see the Task 2 design note): a crash right after this would leave a
    // corrupted/incomplete on-disk baseline that a later reopen would treat
    // as trustworthy. Logged, matching how XStore_Wal.cpp treats an
    // equivalent short/failed durability write it can't propagate
    // synchronously; RebuildFoldRows reports it to the builder.
    spdlog::error("[ZONE MAP] - Short/failed write to block {}", block_index);
    return false;
}

bool ZoneMapFile::NeedsOverwriteCheck(uint64_t first_row, uint64_t row_count) {
    if (row_count == 0) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (header_.tracked_column_count == 0) return false; // nothing is ever folded
    if (first_row + row_count > frontier_) {
        if (fold_.Active()) {
            bool needs = false;
            ForEachWriterRun(first_row, first_row + row_count - 1,
                             [&](uint64_t first, uint64_t last) { needs = needs || BlocksNeedOverwriteCheck(first, last); });
            return needs;
        }
        if (first_row >= frontier_) return false; // a running rebuild has not folded these rows
        row_count = frontier_ - first_row;
    }
    return BlocksNeedOverwriteCheck(first_row, first_row + row_count - 1);
}

bool ZoneMapFile::BlocksNeedOverwriteCheck(uint64_t first_row, uint64_t last_row) {
    const uint64_t rpb = header_.rows_per_block;
    for (uint64_t b = BlockIndexForRow(first_row, rpb); b <= BlockIndexForRow(last_row, rpb); ++b) {
        const CachedBlock &block = EnsureBlockLoaded(b);
        if (ZoneMapEntryInexact(block.entries[0])) continue;
        if (!block.fold_end_known || std::max(first_row, b * rpb) < block.fold_end) return true;
    }
    return false;
}

template <class Fn>
void ZoneMapFile::ForEachWriterRun(uint64_t first_row, uint64_t last_row, Fn &&fn) const {
    uint64_t row = first_row;
    while (row <= last_row) {
        const uint64_t end = row < frontier_ ? frontier_ : fold_.FoldedEnd(row);
        if (end > row) {
            const uint64_t run_last = std::min(last_row, end - 1);
            fn(row, run_last);
            row = run_last + 1;
        } else {
            if (!fold_.Active() || row >= fold_.TotalRows()) return;
            row = fold_.NextGroupBegin(row);
        }
    }
}

void ZoneMapFile::MarkInexact(uint64_t first_row, uint64_t last_row) {
    std::lock_guard<std::mutex> lock(mutex_);
    MarkInexactLocked(first_row, last_row);
}

void ZoneMapFile::MarkInexactLocked(uint64_t first_row, uint64_t last_row) {
    if (header_.tracked_column_count == 0) return;
    if (last_row >= frontier_) {
        if (fold_.Active()) {
            ForEachWriterRun(first_row, last_row, [this](uint64_t first, uint64_t last) { MarkInexactRunLocked(first, last); });
            return;
        }
        if (first_row >= frontier_) return; // a running rebuild has not folded these rows
        last_row = frontier_ - 1;
    }
    MarkInexactRunLocked(first_row, last_row);
}

void ZoneMapFile::MarkInexactRunLocked(uint64_t first_row, uint64_t last_row) {
    const uint64_t rpb = header_.rows_per_block;
    const size_t block_bytes = header_.tracked_column_count * sizeof(ZoneMapEntry);
    // Blocks past the end of the file were never written, so nothing is folded there: this bounds a huge
    // range to the blocks that exist.
    struct stat st{};
    if (fstat(fd_, &st) != 0) {
        spdlog::error("[ZONE MAP] - fstat failed while marking blocks inexact");
        return;
    }
    if (st.st_size <= BlockFileOffset(0)) return;
    const uint64_t blocks_on_disk = static_cast<uint64_t>(st.st_size - BlockFileOffset(0)) / block_bytes;
    const uint64_t last_block = BlockIndexForRow(last_row, rpb);
    for (uint64_t b = BlockIndexForRow(first_row, rpb); b <= last_block && b < blocks_on_disk; ++b) {
        CachedBlock &block = EnsureBlockLoaded(b);
        std::vector<ZoneMapEntry> &entries = block.entries;
        if (ZoneMapEntryInexact(entries[0])) continue;
        if (entries[0].valid_count == 0 && ZoneMapNullCount(entries[0]) == 0) continue; // nothing folded
        EnsureDirtyOnDisk();
        for (ZoneMapEntry &e : entries) e.null_count |= ZONE_MAP_INEXACT_BIT;
        ++mark_writes_;
        if (pwrite(fd_, entries.data(), block_bytes, BlockFileOffset(b)) != static_cast<ssize_t>(block_bytes)) {
            spdlog::error("[ZONE MAP] - Short/failed write marking block {} inexact", b);
        }
    }
}

bool ZoneMapFile::DeleteRows(uint64_t first_row, uint64_t last_row, const std::function<bool()> &punch_data) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (header_.tracked_column_count == 0) return punch_data();
    EnsureDirtyOnDisk(); // before the data punch: a crash after it must find the index dirty
    const uint64_t rpb = header_.rows_per_block;
    // Fully covered blocks: [full_begin, full_end). Empty when the range sits inside one block.
    const uint64_t full_begin = (first_row + rpb - 1) / rpb;
    uint64_t full_end = (last_row + 1) / rpb;
    // A running rebuild owns every block not yet complete: never reset one (the builder may have folded part of it, or
    // be about to). Such a block falls to the partial-end path below, which flags only what lies under F (or in a
    // folded group prefix). Fully folded blocks of the first such run are reset like blocks below F.
    if (full_end * rpb > frontier_) {
        const uint64_t first = full_begin * rpb;
        const uint64_t owned_end = first < frontier_ ? frontier_ : fold_.FoldedEnd(first);
        full_end = std::max(full_begin, std::min(full_end, owned_end / rpb));
    }
    if (full_begin >= full_end) {
        MarkInexactLocked(first_row, last_row);
        return punch_data();
    }
    if (first_row < full_begin * rpb) MarkInexactLocked(first_row, full_begin * rpb - 1);
    if (last_row >= full_end * rpb) MarkInexactLocked(full_end * rpb, last_row);
    if (!punch_data()) {
        MarkInexactLocked(full_begin * rpb, full_end * rpb - 1); // the rows may be partly gone
        return false;
    }
    const off_t begin = BlockFileOffset(full_begin);
    if (fallocate64(fd_, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, begin, BlockFileOffset(full_end) - begin) != 0) {
        spdlog::error("[ZONE MAP] - Cannot reset blocks {}..{} after a range delete ({}); flagging them instead",
                      full_begin, full_end - 1, strerror(errno));
        MarkInexactLocked(full_begin * rpb, full_end * rpb - 1);
        return true;
    }
    ++mark_writes_;
    for (auto &[b, block] : cache_) {
        if (b < full_begin || b >= full_end) continue;
        std::fill(block.entries.begin(), block.entries.end(), ZoneMapEntry{});
        block.fold_end = 0;
        block.fold_end_known = true;
    }
    return true;
}

uint64_t ZoneMapFile::MarkWritesForTests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return mark_writes_;
}

bool ZoneMapFile::BlockInexact(uint64_t block_index) {
    auto stats = TryGetBlockStats(block_index);
    return stats.has_value() && !stats->empty() && ZoneMapEntryInexact((*stats)[0]);
}

std::optional<std::vector<ZoneMapEntry>> ZoneMapFile::TryGetBlockStats(uint64_t block_index) {
    std::lock_guard<std::mutex> lock(mutex_);
    if ((block_index + 1) * header_.rows_per_block > frontier_) return std::nullopt; // not rebuilt yet
    auto cached = cache_.find(block_index);
    if (cached != cache_.end()) return cached->second.entries; // always disk-coherent, safe to serve directly

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

void ZoneMapFile::MarkDirtyOnDisk() {
    header_.dirty = 1;
    if (pwrite(fd_, &header_, sizeof(header_), 0) == static_cast<ssize_t>(sizeof(header_)) && fdatasync(fd_) == 0) {
        dirty_on_disk_ = true;
        ++dirty_mark_writes_;
        return;
    }
    // Log-and-continue: inserts must not fail; the next write retries (dirty_on_disk_ stays false). header_.dirty
    // stays 1 in memory: a failed pwrite/fdatasync may still have put the 1 on disk, and any later header write
    // must not turn that into a 0 over index content that was not marked.
    spdlog::error("[ZONE MAP] - Failed to persist dirty flag");
}

void ZoneMapFile::MarkDirty() {
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureDirtyOnDisk();
}

bool ZoneMapFile::MarkClean() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) return false; // half-built: must stay dirty
    if (fdatasync(fd_) != 0) return false;
    if (!dirty_on_disk_ && header_.dirty == 0) return true; // dirty=1 may be on disk unsynced (new file, failed mark)
    header_.dirty = 0;
    if (pwrite(fd_, &header_, sizeof(header_), 0) != static_cast<ssize_t>(sizeof(header_)) || fdatasync(fd_) != 0) {
        header_.dirty = 1;
        return false;
    }
    dirty_on_disk_ = false;
    return true;
}

bool ZoneMapFile::BeginRebuild(uint32_t new_tracked_column_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    return BeginRebuildLocked(new_tracked_column_count);
}

BeginRebuildResult ZoneMapFile::BeginLiveRebuild(uint32_t new_tracked_column_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) return BeginRebuildResult::kAlreadyRebuilding; // another builder owns it: touch nothing
    return BeginRebuildLocked(new_tracked_column_count) ? BeginRebuildResult::kStarted : BeginRebuildResult::kFailed;
}

bool ZoneMapFile::BeginRebuildLocked(uint32_t new_tracked_column_count) {
    // Owner rule: the index is dirty on disk before it is truncated. A failure here changes nothing.
    EnsureDirtyOnDisk();
    if (!dirty_on_disk_) {
        spdlog::error("[ZONE MAP] - Cannot mark the index dirty before a rebuild; keeping it as it is");
        return false;
    }
    if (ftruncate(fd_, 0) != 0) {
        spdlog::error("[ZONE MAP] - Cannot truncate the index for a rebuild ({}); keeping it as it is", strerror(errno));
        return false;
    }
    // From here the old content is gone, so the rebuild state is entered whatever happens. rows_per_block is kept (it
    // derives from the row length, which never changes) and is not written to: RowsPerBlockValue reads it unlocked.
    header_.magic = ZoneMapFileHeader{}.magic;
    header_.version = ZONE_MAP_VERSION;
    header_.tracked_column_count = new_tracked_column_count;
    header_.dirty = 1;
    dirty_on_disk_ = false;
    if (pwrite(fd_, &header_, sizeof(header_), 0) == static_cast<ssize_t>(sizeof(header_)) && fdatasync(fd_) == 0) {
        dirty_on_disk_ = true;
        ++dirty_mark_writes_;
    } else {
        // The file may now be too short for a header: a start reads that as "rebuild". Every write retries the header
        // through EnsureDirtyOnDisk, and FinishRebuild refuses to end the rebuild until it is durable.
        spdlog::error("[ZONE MAP] - Failed to write the fresh header of a rebuild");
    }
    cache_.clear();
    lru_order_.clear();
    frontier_ = 0;
    fold_.Reset(0, 0);
    rebuilding_.store(true, std::memory_order_release);
    return true;
}

void ZoneMapFile::EnterUntrustedRebuildState() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) return;
    frontier_ = 0;
    rebuilding_.store(true, std::memory_order_release);
}

void ZoneMapFile::AdvanceFrontier(uint64_t new_frontier) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild && new_frontier > frontier_) frontier_ = new_frontier;
}

void ZoneMapFile::SetLiveFoldGroups(uint64_t group_rows, uint64_t total_rows) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) fold_.Reset(group_rows, total_rows);
}

uint64_t ZoneMapFile::MarkFolded(uint64_t begin, uint64_t end) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild || !fold_.Active()) return frontier_;
    const uint64_t watermark = fold_.MarkFolded(begin, end);
    if (watermark > frontier_) frontier_ = watermark;
    return frontier_;
}

bool ZoneMapFile::FinishRebuild() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild) return true;
    EnsureDirtyOnDisk();
    if (!dirty_on_disk_) {
        spdlog::error("[ZONE MAP] - The rebuilt index has no durable header yet; it stays in rebuild");
        return false;
    }
    frontier_ = kNoRebuild;
    fold_.Reset(0, 0);
    rebuilding_.store(false, std::memory_order_release);
    return true;
}

uint64_t ZoneMapFile::DirtyMarkWritesForTests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dirty_mark_writes_;
}

ZoneMapFile::~ZoneMapFile() {
    if (fd_ >= 0) close(fd_);
}
