#include "XStore/XStore_CatIdx.h"
#include "include/XStore/XStore.h"
#include "include/XStore/XStore_RowScan.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#include <spdlog/spdlog.h>
#include <spdlog/fmt/fmt.h>

#include "include/DataStructs/TimeStruct.h"

namespace {
void BloomSetBit(unsigned char *entry, size_t entry_size, uint64_t h1, uint64_t h2, size_t k) {
    size_t total_bits = entry_size * 8;
    for (size_t i = 0; i < k; ++i) {
        uint64_t combined = h1 + i * h2;
        size_t bit = combined % total_bits;
        entry[bit / 8] |= static_cast<unsigned char>(1u << (bit % 8));
    }
}
bool BloomCheckBit(const unsigned char *entry, size_t entry_size, uint64_t h1, uint64_t h2, size_t k) {
    size_t total_bits = entry_size * 8;
    for (size_t i = 0; i < k; ++i) {
        uint64_t combined = h1 + i * h2;
        size_t bit = combined % total_bits;
        if ((entry[bit / 8] & static_cast<unsigned char>(1u << (bit % 8))) == 0) return false;
    }
    return true;
}
} // namespace

bool CatIdxFile::Open(const std::string &path, uint16_t row_length, uint32_t tracked_column_count) {
    rows_per_block_ = RowsPerBlock(row_length);
    path_ = path;

    fd_ = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0) {
        spdlog::error("[CATIDX] - Unable to open/create {}", path);
        return false;
    }

    struct stat st{};
    if (fstat(fd_, &st) != 0) return false;

    if (st.st_size >= static_cast<off_t>(sizeof(CatIdxFileHeader))) {
        CatIdxFileHeader on_disk{};
        if (pread(fd_, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk)) &&
            on_disk.magic == CatIdxFileHeader{}.magic &&
            on_disk.version == XStoreTunables::CATIDX_VERSION &&
            on_disk.tracked_column_count > 0) {
            header_ = on_disk;
            width_.store(header_.tracked_column_count, std::memory_order_release);
            dirty_on_disk_ = header_.dirty != 0;
            return true;
        }
    }

    // No valid header: new, stale, or left by a crash between a rebuild's truncate and its header write. The fresh
    // header goes out dirty in ONE pwrite with NO fdatasync (Open runs under the global registry mutex): it either
    // reached disk (dirty => rebuild) or did not (too short / invalid => rebuild), so a crashed truncate can never read
    // clean. dirty_on_disk_ stays false: the first write's EnsureDirtyOnDisk syncs it under this file's mutex.
    return WriteFreshHeader(row_length, tracked_column_count);
}

bool CatIdxFile::WriteFreshHeader(uint16_t /*row_length*/, uint32_t tracked_column_count) {
    header_ = CatIdxFileHeader{};
    dirty_on_disk_ = false;
    header_.dirty = 1;
    header_.tracked_column_count = tracked_column_count;
    width_.store(header_.tracked_column_count, std::memory_order_release);
    header_.block_count = 0;
    header_.bitmap_section_offset = 0;

    if (ftruncate(fd_, static_cast<off_t>(sizeof(CatIdxFileHeader))) != 0) return false;
    ssize_t written = pwrite(fd_, &header_, sizeof(header_), 0);
    return written == static_cast<ssize_t>(sizeof(header_));
}

off_t CatIdxFile::BloomSectionFileOffset(uint64_t block_index) const {
    return static_cast<off_t>(sizeof(CatIdxFileHeader)) +
           static_cast<off_t>(block_index * header_.tracked_column_count * sizeof(BloomEntry));
}

void CatIdxFile::EnsureBlockLoaded(uint64_t block_index) {
    if (cache_.count(block_index) > 0) {
        lru_order_.erase(std::remove(lru_order_.begin(), lru_order_.end(), block_index), lru_order_.end());
        lru_order_.push_back(block_index);
        return;
    }

    std::vector<BloomEntry> entries(header_.tracked_column_count);
    off_t offset = BloomSectionFileOffset(block_index);
    size_t bytes_to_read = header_.tracked_column_count * sizeof(BloomEntry);

    // Existence test is header_.block_count, NOT a raw file-size comparison
    // -- st.st_size includes whatever the appended bitmap section directory
    // added past the bloom region, and BloomSectionFileOffset's pure
    // arithmetic can land inside that directory range for a block that was
    // never actually written. header_.block_count is durably maintained
    // only by FlushBlock (real bloom writes), so it's independent of the
    // bitmap section's size.
    if (block_index < header_.block_count) {
        // Existing on-disk block -- load its current (always-durable) bloom bytes.
        [[maybe_unused]] ssize_t read_bytes = pread(fd_, entries.data(), bytes_to_read, offset);
    } else {
        // Never-touched block -- zero-initialize PURELY IN MEMORY. No disk
        // write here: this branch is reached from MightContain (a pure
        // read/query) just as often as from UpdateRow (a real mutation), and
        // a FindByValue query over sparsely-populated data must never
        // durably grow the .catidx file as a side effect of reading (see
        // ZoneMapFile's equivalent method, which has the same "read never
        // writes" contract). The durable write + block_count/header bump for
        // a genuinely never-touched block now happens in FlushBlock, which
        // is only ever called from UpdateRow.
        for (auto &e : entries) e.fill(0);
    }

    cache_[block_index] = std::move(entries);
    lru_order_.push_back(block_index);
    EvictIfOverCapacity();
}

void CatIdxFile::EvictIfOverCapacity() {
    while (cache_.size() > XStoreTunables::CATIDX_HOT_CACHE_BLOCKS && !lru_order_.empty()) {
        uint64_t victim = lru_order_.front();
        lru_order_.erase(lru_order_.begin());
        cache_.erase(victim); // read-cache only, no flush needed -- UpdateRow is already write-through
    }
}

bool CatIdxFile::FlushBlock(uint64_t block_index) {
    auto it = cache_.find(block_index);
    if (it == cache_.end()) return true;
    EnsureDirtyOnDisk(); // covers the invalidation, bloom-block and block_count header writes below
    off_t offset = BloomSectionFileOffset(block_index);
    size_t bytes = header_.tracked_column_count * sizeof(BloomEntry);

    // The bitmap directory is a COMPACTION-TIME SNAPSHOT of real row data, so
    // ANY bloom write to an already-compacted file means the underlying rows
    // changed since that snapshot was taken and the directory can no longer be
    // trusted -- it would report backfilled matches as absent (false negative)
    // and overwritten/deleted rows as still matching (false positive).
    // Invalidate on any write, not just one that would physically overlap the
    // directory bytes: an overlap-only check (what this used to do) never
    // trips for a write into a block index that already existed at compaction
    // time, since bitmap_section_offset sits past every such block, which is
    // exactly the common backfill/overwrite case. Self-heals the same way as
    // before -- the next compaction pass sees HasBitmapSection() == false (its
    // own idempotency check) and rebuilds the directory from scratch over the
    // now-current row data; until then queries correctly fall back to
    // bloom+real-scan. Same "always fully re-derivable from real row data"
    // precedent as the zone-map feature's version-bump-discards elsewhere in
    // this codebase. UpdateRow/FlushBlock on an already-compacted cold year is
    // rare (compaction only ever targets closed years), so the extra header
    // rewrite is not a hot-path cost.
    if (header_.bitmap_section_offset != 0) {
        spdlog::warn("[CATIDX] - Block {} write into an already-compacted file -- invalidating "
                     "the bitmap section; next compaction pass will rebuild it", block_index);
        header_.bitmap_section_offset = 0;
        [[maybe_unused]] ssize_t invalidate_written = pwrite(fd_, &header_, sizeof(header_), 0);
        fdatasync(fd_);
    }

    // pwrite past the current end-of-file is standard sparse-file growth on
    // Linux -- no ftruncate needed first (this codebase already relies on
    // that pattern elsewhere for data files).
    ssize_t written = pwrite(fd_, it->second.data(), bytes, offset);
    if (written != static_cast<ssize_t>(bytes)) {
        // Write-through is the whole point (see UpdateRow) -- a short/failed
        // write here means the in-memory cache_ entry is now ahead of what's
        // durable on disk. Log-and-continue, matching ZoneMapFile::UpdateRow's
        // handling of the equivalent failure (UpdateRow's signature has no
        // failure-reaction path in this plan).
        spdlog::error("[CATIDX] - Short/failed write to block {}", block_index);
    }

    // This is the only place a never-touched block's existence becomes
    // durable -- moved here from EnsureBlockLoaded so a pure read
    // (MightContain) never bumps block_count/rewrites the header. FlushBlock
    // is only ever called from UpdateRow (a genuine mutation), so this is
    // still write-through for every real write. Guarded on the data write
    // actually having succeeded (Minor #6) -- a short/failed write must not
    // be recorded as if it durably landed.
    if (written != static_cast<ssize_t>(bytes)) return false;
    if (block_index >= header_.block_count) {
        header_.block_count = static_cast<uint32_t>(block_index + 1);
        // A lost block_count makes a reopen read this block as never written (all zero: false negatives).
        if (pwrite(fd_, &header_, sizeof(header_), 0) != static_cast<ssize_t>(sizeof(header_))) {
            spdlog::error("[CATIDX] - Short/failed header write recording block {}", block_index);
            return false;
        }
    }
    return true;
}

void CatIdxFile::UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations) {
    std::lock_guard<std::mutex> lock(mutex_);
    // Not rebuilding: this one compare. A keep-old rebuild updates the old index first (it gets every UpdateRow). A
    // running rebuild folds this row later, from the data file, unless a parallel group already folded it.
    if (__builtin_expect(frontier_ != kNoRebuild, 0)) {
        if (old_ != nullptr) old_->UpdateRow(row_index, observations);
        if (row_index >= frontier_ && fold_.FoldedEnd(row_index) <= row_index) return;
    }
    UpdateRowLocked(row_index, observations);
}

void CatIdxFile::RebuildFoldRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild || row_index < frontier_) return; // not rebuilding, or already folded
    UpdateRowLocked(row_index, observations);
}

void CatIdxFile::UpdateRowLocked(uint64_t row_index, const std::vector<CatIdxObservation> &observations) {
    uint64_t block_index = BlockIndexForRow(row_index, rows_per_block_);
    EnsureBlockLoaded(block_index);

    auto &entries = cache_[block_index];
    for (const auto &obs : observations) {
        if (!obs.is_present || obs.column_idx >= header_.tracked_column_count) continue;
        uint64_t h1 = Crc32c(obs.encoded_value.data(), obs.encoded_value.size());
        uint64_t h2 = Fnv1a64(obs.encoded_value.data(), obs.encoded_value.size());
        BloomSetBit(entries[obs.column_idx].data(), entries[obs.column_idx].size(),
                    h1, h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
    }
    static_cast<void>(FlushBlock(block_index)); // write-through, same immediacy as ZoneMapFile::UpdateRow
}

void CatIdxFile::UpdateRows(const CatIdxFoldBatch &batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (__builtin_expect(frontier_ != kNoRebuild, 0)) { // same rules as UpdateRow, row by row
        if (old_ != nullptr) old_->UpdateRows(batch);
        static_cast<void>(FoldBatchLocked(batch, [this](uint64_t row) { return row < frontier_ || fold_.FoldedEnd(row) > row; }));
        return;
    }
    static_cast<void>(FoldBatchLocked(batch, [](uint64_t) { return true; })); // a failed write is logged in FlushBlock
}

bool CatIdxFile::RebuildFoldRows(const CatIdxFoldBatch &batch) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild) return true;
    return FoldBatchLocked(batch, [this](uint64_t row) { return row >= frontier_; }); // below F: already folded
}

template <class Keep>
bool CatIdxFile::FoldBatchLocked(const CatIdxFoldBatch &batch, Keep keep) {
    bool all_written = true;
    std::vector<BloomEntry> *entries = nullptr;
    uint64_t block_index = 0;
    size_t value_begin = 0;
    for (size_t i = 0; i < batch.row_index.size(); value_begin = batch.value_end[i], ++i) {
        const uint64_t row_index = batch.row_index[i];
        if (!keep(row_index)) continue;
        const uint64_t b = BlockIndexForRow(row_index, rows_per_block_);
        if (entries == nullptr || b != block_index) {
            // Flush before loading the next block, as the per-row path does, so a reload reads what was folded.
            if (entries != nullptr) all_written &= FlushBlock(block_index);
            EnsureBlockLoaded(b);
            entries = &cache_[b];
            block_index = b;
        }
        for (size_t v = value_begin; v < batch.value_end[i]; ++v) {
            const CatIdxFoldBatch::Value &value = batch.values[v];
            if (value.column_idx >= header_.tracked_column_count) continue;
            BloomEntry &entry = (*entries)[value.column_idx];
            BloomSetBit(entry.data(), entry.size(), value.h1, value.h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
        }
    }
    if (entries != nullptr) all_written &= FlushBlock(block_index);
    return all_written;
}

bool CatIdxFile::MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (column_idx >= header_.tracked_column_count) return true; // no bloom for this column -- caller must real-scan
    if ((block_index + 1) * rows_per_block_ > frontier_) { // not rebuilt yet: the old bloom, or real-scan
        return old_ == nullptr || old_->MightContain(block_index, column_idx, encoded_value);
    }
    EnsureBlockLoaded(block_index);
    auto &entries = cache_[block_index];
    uint64_t h1 = Crc32c(encoded_value.data(), encoded_value.size());
    uint64_t h2 = Fnv1a64(encoded_value.data(), encoded_value.size());
    return BloomCheckBit(entries[column_idx].data(), entries[column_idx].size(),
                          h1, h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
}

void CatIdxFile::AppendBitmapSection(const std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> &columns) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) {
        spdlog::warn("[CATIDX] - Not appending a bitmap section while the index is being rebuilt");
        return;
    }
    EnsureDirtyOnDisk(); // covers the bitmap-section append and its header commit

    struct stat st{};
    if (fstat(fd_, &st) != 0) {
        // st.st_size is the append offset -- appending at a garbage offset
        // would land directory bytes on top of the bloom section. Bail; the
        // next compaction pass retries (HasBitmapSection() is still false).
        spdlog::error("[CATIDX] - fstat failed before bitmap-section append; not committing");
        return;
    }
    off_t append_offset = st.st_size;

    // One buffer holding every surviving column's directory back-to-back --
    // deliberately ONE pwrite+fsync+header-commit for the whole compaction
    // pass, not one per column. Committing after each column individually
    // used to mean only the FIRST column's write was crash-safe: once
    // header_.bitmap_section_offset went non-zero after that first commit,
    // a crash during a LATER column's write left torn bytes INSIDE the
    // already-committed, scannable range (LookupExact's scan loop has no way
    // to tell a torn block from a real one).
    //
    // Per-column block layout (concatenated, LookupExact already scans
    // multiple such blocks until EOF): [columnIdx u32][entryCount u32]
    //   then per entry, sorted by encoded value bytes for binary search:
    //   [valueLen u32][valueBytes][rowCount u32][rowIndices: u64 * rowCount]
    std::string buf;
    for (const auto &[column_idx, value_to_rows] : columns) {
        buf.append(reinterpret_cast<const char*>(&column_idx), sizeof(column_idx));
        uint32_t entry_count = static_cast<uint32_t>(value_to_rows.size());
        buf.append(reinterpret_cast<const char*>(&entry_count), sizeof(entry_count));

        for (const auto &[value, rows] : value_to_rows) { // std::map iterates sorted by key -- binary-searchable on disk
            uint32_t value_len = static_cast<uint32_t>(value.size());
            buf.append(reinterpret_cast<const char*>(&value_len), sizeof(value_len));
            buf.append(value);
            uint32_t row_count = static_cast<uint32_t>(rows.size());
            buf.append(reinterpret_cast<const char*>(&row_count), sizeof(row_count));
            buf.append(reinterpret_cast<const char*>(rows.data()), rows.size() * sizeof(uint64_t));
        }
    }

    ssize_t data_written = pwrite(fd_, buf.data(), buf.size(), append_offset);
    if (data_written != static_cast<ssize_t>(buf.size())) {
        // pwrite() caps a single transfer at ~0x7ffff000 bytes on Linux, and a single
        // compaction pass over a full year of low-cardinality columns can exceed that --
        // don't let a short/failed write land inside the committed, scannable range.
        spdlog::error("[CATIDX] - Short/failed bitmap-section write ({}/{}); not committing",
                       data_written, buf.size());
        return; // uncommitted tail, safely ignorable -- next compaction pass rebuilds from scratch
    }
    fdatasync(fd_);

    // Commit point: only now does the header admit this section exists.
    if (header_.bitmap_section_offset == 0) {
        header_.bitmap_section_offset = static_cast<uint64_t>(append_offset);
    }
    [[maybe_unused]] ssize_t header_written = pwrite(fd_, &header_, sizeof(header_), 0);
    fdatasync(fd_);
}

std::optional<std::vector<uint64_t>> CatIdxFile::LookupExact(uint32_t column_idx, const std::string &encoded_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) { // see HasBitmapSection
        return old_ != nullptr ? old_->LookupExact(column_idx, encoded_value) : std::nullopt;
    }
    if (header_.bitmap_section_offset == 0) return std::nullopt;

    struct stat st{};
    if (fstat(fd_, &st) != 0) return std::nullopt;
    off_t pos = static_cast<off_t>(header_.bitmap_section_offset);

    // Every read is bounds-checked against the real file size AND its return
    // value verified before the bytes are used. value_len/row_count come
    // straight off disk, so a truncated or corrupt directory would otherwise
    // turn a garbage length into a multi-gigabyte allocation (or bad_alloc)
    // on the query path. Any anomaly bails to nullopt, which every caller
    // already handles as "no directory answer" by falling back to
    // bloom+real-scan -- always correct, just unaccelerated.
    auto fits = [&](size_t n) {
        return pos >= 0 && pos <= st.st_size && n <= static_cast<size_t>(st.st_size - pos);
    };
    auto read_at = [&](void *dst, size_t n) {
        if (!fits(n)) return false;
        if (pread(fd_, dst, n, pos) != static_cast<ssize_t>(n)) return false;
        pos += static_cast<off_t>(n);
        return true;
    };

    while (pos < st.st_size) {
        uint32_t col = 0;
        uint32_t entry_count = 0;
        if (!read_at(&col, sizeof(col))) return std::nullopt;
        if (!read_at(&entry_count, sizeof(entry_count))) return std::nullopt;

        for (uint32_t i = 0; i < entry_count; ++i) {
            uint32_t value_len = 0;
            uint32_t row_count = 0;
            if (!read_at(&value_len, sizeof(value_len))) return std::nullopt;
            if (!fits(value_len)) return std::nullopt; // check BEFORE allocating
            std::string value(value_len, '\0');
            if (!read_at(value.data(), value_len)) return std::nullopt;
            if (!read_at(&row_count, sizeof(row_count))) return std::nullopt;
            const size_t row_bytes = static_cast<size_t>(row_count) * sizeof(uint64_t);
            if (!fits(row_bytes)) return std::nullopt; // check BEFORE allocating

            if (col == column_idx && value == encoded_value) {
                std::vector<uint64_t> rows(row_count);
                if (!read_at(rows.data(), row_bytes)) return std::nullopt;
                return rows;
            }
            pos += static_cast<off_t>(row_bytes);
        }
    }
    return std::nullopt;
}

void CatIdxFile::MarkDirtyOnDisk() {
    header_.dirty = 1;
    if (pwrite(fd_, &header_, sizeof(header_), 0) == static_cast<ssize_t>(sizeof(header_)) && fdatasync(fd_) == 0) {
        dirty_on_disk_ = true;
        ++dirty_mark_writes_;
        return;
    }
    // Log-and-continue: inserts must not fail; the next write retries (dirty_on_disk_ stays false). header_.dirty
    // stays 1 in memory: a failed pwrite/fdatasync may still have put the 1 on disk, and any later header write
    // must not turn that into a 0 over index content that was not marked.
    spdlog::error("[CATIDX] - Failed to persist dirty flag");
}

void CatIdxFile::MarkDirty() {
    std::lock_guard<std::mutex> lock(mutex_);
    EnsureDirtyOnDisk();
}

bool CatIdxFile::MarkClean() {
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

bool CatIdxFile::BeginRebuild(uint32_t new_tracked_column_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    return BeginRebuildLocked(new_tracked_column_count);
}

BeginRebuildResult CatIdxFile::BeginLiveRebuild(uint32_t new_tracked_column_count, bool keep_old) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) return BeginRebuildResult::kAlreadyRebuilding; // another builder owns it: touch nothing
    const bool began = keep_old ? BeginKeepOldLocked(new_tracked_column_count) : BeginRebuildLocked(new_tracked_column_count);
    return began ? BeginRebuildResult::kStarted : BeginRebuildResult::kFailed;
}

void CatIdxFile::AdoptOpenState(CatIdxFile &from) noexcept {
    fd_ = from.fd_;
    header_ = from.header_;
    width_.store(header_.tracked_column_count, std::memory_order_release);
    rows_per_block_ = from.rows_per_block_;
    dirty_on_disk_ = from.dirty_on_disk_;
    cache_ = std::move(from.cache_);
    lru_order_ = std::move(from.lru_order_);
    // path_ stays empty: the name now belongs to the new file (the old one is unlinked and never begins a rebuild).
}

// Same steps as ZoneMapFile::BeginKeepOldLocked. rows_per_block_ is a member derived from the row length, unchanged.
bool CatIdxFile::BeginKeepOldLocked(uint32_t new_tracked_column_count) {
    EnsureDirtyOnDisk();
    if (!dirty_on_disk_ || path_.empty()) {
        spdlog::error("[CATIDX] - Cannot mark the index dirty before a rebuild; keeping it as it is");
        return false;
    }
    CatIdxFileHeader fresh{}; // no blocks, no bitmap section
    fresh.tracked_column_count = new_tracked_column_count;
    fresh.dirty = 1;
    // Allocated before the rename: from the rename to the end of this function nothing may throw (AdoptOpenState only
    // moves; the clears and stores are noexcept), so the object can never be left half switched.
    auto old = std::make_unique<CatIdxFile>();
    const int new_fd = CreateIndexReplacement(path_, &fresh, sizeof(fresh));
    if (new_fd < 0) return false;
    old->AdoptOpenState(*this);
    old_ = std::move(old);
    fd_ = new_fd;
    header_ = fresh;
    width_.store(header_.tracked_column_count, std::memory_order_release);
    dirty_on_disk_ = true;
    ++dirty_mark_writes_;
    cache_.clear();
    lru_order_.clear();
    frontier_ = 0;
    fold_.Reset(0, 0);
    old_width_.store(old_->header_.tracked_column_count, std::memory_order_release);
    rebuilding_.store(true, std::memory_order_release);
    return true;
}

bool CatIdxFile::BeginRebuildLocked(uint32_t new_tracked_column_count) {
    // Owner rule: the index is dirty on disk before it is truncated. A failure here changes nothing.
    EnsureDirtyOnDisk();
    if (!dirty_on_disk_) {
        spdlog::error("[CATIDX] - Cannot mark the index dirty before a rebuild; keeping it as it is");
        return false;
    }
    if (ftruncate(fd_, 0) != 0) {
        spdlog::error("[CATIDX] - Cannot truncate the index for a rebuild ({}); keeping it as it is", strerror(errno));
        return false;
    }
    // From here the old content is gone, so the rebuild state is entered whatever happens. The bitmap section goes
    // with it; compaction rebuilds it once the rebuild is over.
    header_ = CatIdxFileHeader{};
    header_.tracked_column_count = new_tracked_column_count;
    width_.store(header_.tracked_column_count, std::memory_order_release);
    header_.dirty = 1;
    dirty_on_disk_ = false;
    if (pwrite(fd_, &header_, sizeof(header_), 0) == static_cast<ssize_t>(sizeof(header_)) && fdatasync(fd_) == 0) {
        dirty_on_disk_ = true;
        ++dirty_mark_writes_;
    } else {
        // The file may now be too short for a header: a start reads that as stale. Every write retries the header
        // through EnsureDirtyOnDisk, and FinishRebuild refuses to end the rebuild until it is durable.
        spdlog::error("[CATIDX] - Failed to write the fresh header of a rebuild");
    }
    cache_.clear();
    lru_order_.clear();
    frontier_ = 0;
    fold_.Reset(0, 0);
    rebuilding_.store(true, std::memory_order_release);
    return true;
}

void CatIdxFile::EnterUntrustedRebuildState() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) return;
    frontier_ = 0;
    rebuilding_.store(true, std::memory_order_release);
}

void CatIdxFile::AdvanceFrontier(uint64_t new_frontier) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild && new_frontier > frontier_) frontier_ = new_frontier;
}

void CatIdxFile::SetLiveFoldGroups(uint64_t group_rows, uint64_t total_rows) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ != kNoRebuild) fold_.Reset(group_rows, total_rows);
}

uint64_t CatIdxFile::MarkFolded(uint64_t begin, uint64_t end) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild || !fold_.Active()) return frontier_;
    const uint64_t watermark = fold_.MarkFolded(begin, end);
    if (watermark > frontier_) frontier_ = watermark;
    return frontier_;
}

bool CatIdxFile::FinishRebuild() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (frontier_ == kNoRebuild) return true;
    EnsureDirtyOnDisk();
    if (!dirty_on_disk_) {
        spdlog::error("[CATIDX] - The rebuilt index has no durable header yet; it stays in rebuild");
        return false;
    }
    frontier_ = kNoRebuild;
    fold_.Reset(0, 0);
    old_width_.store(0, std::memory_order_release);
    old_.reset(); // closes the old (unlinked) file
    rebuilding_.store(false, std::memory_order_release);
    return true;
}

uint64_t CatIdxFile::DirtyMarkWritesForTests() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return dirty_mark_writes_;
}

CatIdxFile::~CatIdxFile() {
    if (fd_ >= 0) close(fd_);
}

std::mutex XStore::catidx_registry_mutex_;
std::unordered_map<std::string, std::unique_ptr<CatIdxFile>> XStore::catidx_registry_;
std::vector<std::string> XStore::catidx_registry_lru_;
std::vector<std::unique_ptr<CatIdxFile>> XStore::catidx_registry_retired_;

namespace {
bool catidx_over_cap_warned = false; // guarded by catidx_registry_mutex_: one warning per over-cap episode
} // namespace

CatIdxFile& XStore::GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                     uint32_t tracked_column_count) {
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    return GetOrOpenCatIdxLocked(db_base_path, year, row_length, tracked_column_count);
}

CatIdxFile::Pin XStore::PinCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                  uint32_t tracked_column_count) {
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    return CatIdxFile::Pin(&GetOrOpenCatIdxLocked(db_base_path, year, row_length, tracked_column_count));
}

// Same lock rules as GetOrOpenZoneMapLocked: registry -> file order, and this per-insert path never takes a file mutex.
CatIdxFile& XStore::GetOrOpenCatIdxLocked(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                          uint32_t tracked_column_count) {
    std::string key = fmt::format("{}/{}", db_base_path, year);

    auto it = catidx_registry_.find(key);
    if (it != catidx_registry_.end()) {
        catidx_registry_lru_.erase(std::remove(catidx_registry_lru_.begin(), catidx_registry_lru_.end(), key),
                                    catidx_registry_lru_.end());
        catidx_registry_lru_.push_back(key);
        return *it->second;
    }

    if (!catidx_registry_retired_.empty()) SweepRetired(catidx_registry_retired_);

    // Pinned and rebuilding entries are never evicted: their holder has a pointer, and a cold reopen of a half-built
    // file would trust blocks the builder has not reached. Grows past the cap only while every entry is held.
    while (catidx_registry_.size() >= XStoreTunables::CATIDX_REGISTRY_CAP) {
        auto victim = std::find_if(catidx_registry_lru_.begin(), catidx_registry_lru_.end(),
                                   [](const std::string &k) { return catidx_registry_.at(k)->Evictable(); });
        if (victim == catidx_registry_lru_.end()) {
            if (!catidx_over_cap_warned) {
                spdlog::warn("[CATIDX] - Every one of {} open index files is pinned or being rebuilt; growing past the cap",
                             catidx_registry_.size());
                catidx_over_cap_warned = true;
            }
            break;
        }
        catidx_registry_.erase(*victim); // ~CatIdxFile() closes the fd -- no flush needed, already durable (write-through UpdateRow)
        catidx_registry_lru_.erase(victim);
    }
    if (catidx_registry_.size() < XStoreTunables::CATIDX_REGISTRY_CAP) catidx_over_cap_warned = false;

    auto catidx = std::make_unique<CatIdxFile>();
    std::string path = fmt::format("{}/{}.catidx", db_base_path, year);
    if (!catidx->Open(path, row_length, tracked_column_count)) {
        spdlog::error("[CATIDX] - Unable to open {}", path);
        throw std::runtime_error(fmt::format("CatIdxFile::Open failed for {}", path));
    }
    CatIdxFile &ref = *catidx;
    catidx_registry_.emplace(key, std::move(catidx));
    catidx_registry_lru_.push_back(key);
    return ref;
}

// Ignores pins, like the explicit erase in RebuildYearIndexes: neither may run while a builder or another pin holder is
// alive (today they run only in InitDB and in tests).
void XStore::ClearCatIdxRegistryForTests() {
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    catidx_registry_.clear();
    catidx_registry_lru_.clear();
    catidx_registry_retired_.clear();
}

void XStore::CompactOneYearCatIdx(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                   uint32_t tracked_column_count) {
    std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if (!std::filesystem::exists(data_path)) return;

    {
        // Pinned for this idempotency check only, not across the row scan below: the scan can run for minutes and
        // never needs the file, and a pin would keep the entry out of eviction that whole time.
        CatIdxFile::Pin catidx = PinCatIdx(db_base_path, year, row_length, tracked_column_count);
        if (catidx->HasBitmapSection()) return; // already compacted, idempotent no-op
        if (catidx->Rebuilding()) return; // retried next pass; AppendBitmapSection refuses too
    }

    struct stat st{};
    int fd = open(data_path.c_str(), O_RDONLY);
    if (fd < 0) return;
    fstat(fd, &st);
    uint64_t total_rows = row_length > 0 ? static_cast<uint64_t>(st.st_size) / row_length : 0;

    // {column_idx: {encoded_value: [row_index...]}} -- abandoned (erased)
    // for a column the moment its distinct-value count crosses the cap, so
    // an over-cap column never gets AppendBitmapSection called for it and
    // stays bloom+real-scan-only, per the cardinality safety valve.
    std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> accumulators;
    std::unordered_set<uint32_t> abandoned_columns;
    for (uint32_t c = 0; c < tracked_column_count; ++c) accumulators[c];

    std::vector<char> row_buf(row_length);
    for (uint64_t r = 0; r < total_rows; ++r) {
        // Graceful shutdown must not wait out a multi-minute scan: abandon
        // the year (nothing was appended, so it is simply retried next start).
        // bg_cancel_: also a RemoveDB / RemoveDBFiles of this year waiting for the background thread to let go.
        // live_widen_pending_: a live widen was recorded; it runs on this thread, so compaction steps aside.
        if ((r & 0xFFFFu) == 0) {
            if (catidx_compaction_stop_flag_.load(std::memory_order_relaxed) ||
                bg_cancel_.load(std::memory_order_relaxed)) {
                close(fd);
                return;
            }
            if (live_widen_pending_.load(std::memory_order_relaxed)) {
                close(fd);
                spdlog::info("[CATIDX] - Compaction of {}/{} stopped: a live widen is pending; resumes after it",
                             db_base_path, year);
                return;
            }
        }
        [[maybe_unused]] ssize_t row_read = pread(fd, row_buf.data(), row_length, static_cast<off_t>(r) * row_length);
        for (uint32_t c = 0; c < tracked_column_count; ++c) {
            if (abandoned_columns.count(c) > 0) continue;
            XStore_Proto::FData f_data;
            // Non-static member call, this XStore instance's own method --
            // ExtractColumnByIndexSkipParse is non-static (needs `this`),
            // which is exactly why CompactOneYearCatIdx itself is a
            // non-static instance method rather than the brief's original
            // static sketch. Signature: (row_ptr, row_len, unsigned short
            // column_idx, FData& out_data) -> bool.
            if (!ExtractColumnByIndexSkipParse(row_buf.data(), row_length, static_cast<unsigned short>(c), f_data)) continue;
            std::string enc = EncodeFDataForCatIdx(f_data);
            auto &bucket = accumulators[c][enc];
            bucket.push_back(r);
            if (accumulators[c].size() > XStoreTunables::CATIDX_MAX_BITMAP_DISTINCT_VALUES) {
                accumulators[c].clear(); // free immediately, don't keep building a disqualified column
                abandoned_columns.insert(c);
            }
        }
    }
    close(fd);

    std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> surviving;
    for (auto &[col, value_map] : accumulators) {
        if (abandoned_columns.count(col) > 0 || value_map.empty()) continue;
        surviving.emplace(col, std::move(value_map));
    }
    if (surviving.empty()) {
        // Every column abandoned or empty -- still mark this year compacted
        // (empty directory) so it isn't re-scanned every restart.
        surviving[0] = {};
    }

    // A cancel that came after the last check: the removal waiting for this year must not find a section appended. (The
    // background thread holds this year's key in bg_active_key_ throughout, so RemoveDB / RemoveDBFiles cannot remove and
    // re-create the year between the scan and the re-pin below: the bitmap always lands on the file that was scanned.)
    if (bg_cancel_.load(std::memory_order_relaxed)) return;
    // Re-acquired (and pinned) for the append.
    CatIdxFile::Pin catidx = PinCatIdx(db_base_path, year, row_length, tracked_column_count);
    catidx->AppendBitmapSection(surviving);
}

void XStore::RebuildOneYearCatIdxBloom(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                        uint32_t tracked_column_count) {
    RebuildYearIndexes(db_base_path, year, row_length, tracked_column_count, /*need_zmap=*/false, /*need_catidx=*/true);
}

void XStore::CompactColdCatIdxForAllDbs(const std::string &data_store_dir) {
    unsigned short current_year = TimeStruct(static_cast<std::time_t>(std::time(nullptr)), XStore_Proto::SECOND).year;
    if (!std::filesystem::exists(data_store_dir)) return;
    for (auto &p : std::filesystem::directory_iterator(data_store_dir)) {
        if (!p.is_directory()) continue;
        // JoinPath, not p.path(): the spelling every request path uses (CachedDbBasePath), so registry keys and the
        // RemoveDB handshake key below match.
        const std::string db_base_path = JoinPath(data_store_dir, p.path().filename().string());

        // Reuse this XStore instance's own config loader (the same one
        // every other read path uses, e.g. ExistsAggregate/FindByValue in
        // XStore_Aggregates.cpp) rather than hand-rolling a config-file
        // parse. GetOrLoadCfg returns false for a directory that isn't a
        // real DB directory (e.g. the WAL/ sibling directory, which has no
        // matching .CFG file) -- skip those.
        XStore_Proto::ConfigStruct cfg_struct;
        std::string db_name = p.path().filename().string();
        if (!GetOrLoadCfg(db_name, db_base_path, cfg_struct)) continue;

        for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
            // Bound shutdown latency: stop starting new year-scans once a
            // shutdown has been signaled, rather than blocking the
            // atexit-registered thread join until the whole remaining
            // corpus finishes. CompactOneYearCatIdx's row loop also checks
            // the flag (every 64K rows) and abandons the year in progress.
            if (catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) return;
            std::string filename = f.path().filename().string();
            if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;
            unsigned short year;
            try { year = static_cast<unsigned short>(std::stoi(filename.substr(0, filename.size() - 7))); }
            catch (...) { continue; }
            if (year >= current_year) continue; // only cold (closed) years
            if (live_widen_pending_.load(std::memory_order_relaxed)) {
                spdlog::info("[CATIDX] - Compaction of {}/{} stopped: a live widen is pending; resumes after it",
                             db_base_path, year);
                return;
            }

            // Handshake with RemoveDB / RemoveDBFiles (see bg_mutex_): a year being removed is not started, and a
            // removal waits until this year is done or abandoned (bg_cancel_).
            const BgEnter enter = EnterBackgroundYear(fmt::format("{}/{}", db_base_path, year));
            if (enter == BgEnter::kCancelled) return;
            if (enter == BgEnter::kRemoving) continue;
            struct Leave {
                ~Leave() { XStore::LeaveBackgroundYear(); }
            } leave;
            CompactOneYearCatIdx(db_base_path, year, static_cast<uint16_t>(cfg_struct.rowlength()),
                                  EffectiveTrackedColumnCount(cfg_struct));
        }
    }
}

std::thread XStore::catidx_compaction_thread_;
std::atomic<bool> XStore::catidx_compaction_stop_flag_{false};

void XStore::StartCatIdxCompactionThread(const std::string &data_store_dir) {
    // Minor #5: idempotency guard -- unlike StartWalWriters (whose callers
    // wrap it in std::call_once, see XStore_Management.cpp), this has no
    // built-in guard against a second call, which would operator= onto an
    // already-joinable std::thread and crash via std::terminate.
    static std::atomic<bool> already_started{false};
    if (already_started.exchange(true, std::memory_order_relaxed)) {
        spdlog::warn("[CATIDX] - StartCatIdxCompactionThread called more than once; ignoring");
        return;
    }

    catidx_compaction_thread_ = std::thread([data_store_dir]() {
        XStore compaction_store; // one instance for this thread's lifetime, same convention every ZMQ worker already follows
        BackgroundIndexSchedule schedule; // first compaction at start, as before
        const auto now = [] { return std::chrono::steady_clock::now(); };
        while (!XStore::catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) {
            compaction_store.BackgroundIndexTick(data_store_dir, schedule, now);
            std::this_thread::sleep_for(std::chrono::seconds(XStoreTunables::LIVE_WIDEN_TICK_SEC));
        }
    });
    // Same atexit-at-spawn-time ordering argument as StartWalWriters: register
    // the join here so it runs before any static logger/queue teardown.
    std::atexit([]() { XStore::StopCatIdxCompactionThread(); });
}

// Widening and compaction share this one thread, so they never overlap (AppendBitmapSection racing a widen's reset
// would be unsafe); widening has priority. An exception must not end the thread (std::terminate): logged, retried next
// tick / interval.
void XStore::BackgroundIndexTick(const std::string &data_store_dir, BackgroundIndexSchedule &schedule,
                                 const std::function<std::chrono::steady_clock::time_point()> &now) {
    try {
        RunLiveWidenPass(now());
    } catch (const std::exception &e) {
        spdlog::error("[LIVE WIDEN] - Pass failed: {}", e.what());
    }
    if (catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) return;
    // A recorded widen (in its quiet period, or waiting to retry): compaction is not started, so a flood of growths
    // costs nothing here. A record that outlives a pass keeps compaction waiting with it.
    // Any widen, due compaction or not, brings the next compaction forward to shortly after it: a widen drops the
    // widened years' bitmap sections, which only compaction rebuilds (until then FindByValue uses the bloom alone).
    if (live_widen_pending_.load(std::memory_order_relaxed)) {
        if (!schedule.compaction_yielded) {
            schedule.compaction_yielded = true;
            if (now() >= schedule.next_compaction) {
                spdlog::info("[CATIDX] - Compaction deferred: a live widen is pending; resumes after it");
            }
        }
        return;
    }
    if (schedule.compaction_yielded) {
        schedule.compaction_yielded = false;
        schedule.next_compaction = now() + std::chrono::seconds(XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC);
        spdlog::info("[CATIDX] - No live widen pending any more; compaction resumes in {} s",
                     XStoreTunables::LIVE_WIDEN_COMPACTION_RESUME_DELAY_SEC);
    }
    if (now() < schedule.next_compaction) return;
    try {
        CompactColdCatIdxForAllDbs(data_store_dir);
    } catch (const std::exception &e) {
        spdlog::error("[CATIDX] - Compaction pass failed: {}", e.what());
    }
    // A widen recorded during the pass: it abandoned its year (and logged which), or it had just finished. Either way
    // it runs again soon after the widen, which drops the widened years' bitmap sections anyway.
    if (live_widen_pending_.load(std::memory_order_relaxed)) {
        schedule.compaction_yielded = true;
    } else {
        schedule.next_compaction = now() + std::chrono::seconds(XStoreTunables::CATIDX_COMPACTION_POLL_INTERVAL_SEC);
    }
}

void XStore::StopCatIdxCompactionThread() {
    catidx_compaction_stop_flag_.store(true, std::memory_order_relaxed);
    bg_cancel_.store(true, std::memory_order_relaxed); // a running widen stops at its next chunk (left dirty)
    if (catidx_compaction_thread_.joinable()) {
        catidx_compaction_thread_.join();
    }
}
