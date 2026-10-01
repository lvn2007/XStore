#include "XStore/XStore_CatIdx.h"
#include "include/XStore/XStore.h"

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
            dirty_on_disk_ = header_.dirty != 0;
            return true;
        }
    }

    return WriteFreshHeader(row_length, tracked_column_count);
}

bool CatIdxFile::WriteFreshHeader(uint16_t /*row_length*/, uint32_t tracked_column_count) {
    header_ = CatIdxFileHeader{};
    dirty_on_disk_ = false;
    header_.tracked_column_count = tracked_column_count;
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

void CatIdxFile::FlushBlock(uint64_t block_index) {
    auto it = cache_.find(block_index);
    if (it == cache_.end()) return;
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
    if (written == static_cast<ssize_t>(bytes) && block_index >= header_.block_count) {
        header_.block_count = static_cast<uint32_t>(block_index + 1);
        [[maybe_unused]] ssize_t header_written = pwrite(fd_, &header_, sizeof(header_), 0);
    }
}

void CatIdxFile::UpdateRow(uint64_t row_index, const std::vector<CatIdxObservation> &observations) {
    std::lock_guard<std::mutex> lock(mutex_);
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
    FlushBlock(block_index); // write-through, same immediacy as ZoneMapFile::UpdateRow
}

bool CatIdxFile::MightContain(uint64_t block_index, uint32_t column_idx, const std::string &encoded_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (column_idx >= header_.tracked_column_count) return true; // no bloom for this column -- caller must real-scan
    EnsureBlockLoaded(block_index);
    auto &entries = cache_[block_index];
    uint64_t h1 = Crc32c(encoded_value.data(), encoded_value.size());
    uint64_t h2 = Fnv1a64(encoded_value.data(), encoded_value.size());
    return BloomCheckBit(entries[column_idx].data(), entries[column_idx].size(),
                          h1, h2, XStoreTunables::CATIDX_BLOOM_HASH_COUNT);
}

void CatIdxFile::AppendBitmapSection(const std::unordered_map<uint32_t, std::map<std::string, std::vector<uint64_t>>> &columns) {
    std::lock_guard<std::mutex> lock(mutex_);
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
    // Log-and-continue: inserts must not fail; the next write retries.
    header_.dirty = 0;
    spdlog::error("[CATIDX] - Failed to persist dirty flag");
}

bool CatIdxFile::MarkClean() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fdatasync(fd_) != 0) return false;
    if (!dirty_on_disk_) return true;
    header_.dirty = 0;
    if (pwrite(fd_, &header_, sizeof(header_), 0) != static_cast<ssize_t>(sizeof(header_)) || fdatasync(fd_) != 0) {
        header_.dirty = 1;
        return false;
    }
    dirty_on_disk_ = false;
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

CatIdxFile& XStore::GetOrOpenCatIdx(std::string_view db_base_path, unsigned short year, uint16_t row_length,
                                     uint32_t tracked_column_count) {
    std::string key = fmt::format("{}/{}", db_base_path, year);
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);

    auto it = catidx_registry_.find(key);
    if (it != catidx_registry_.end()) {
        catidx_registry_lru_.erase(std::remove(catidx_registry_lru_.begin(), catidx_registry_lru_.end(), key),
                                    catidx_registry_lru_.end());
        catidx_registry_lru_.push_back(key);
        return *it->second;
    }

    if (catidx_registry_.size() >= XStoreTunables::CATIDX_REGISTRY_CAP && !catidx_registry_lru_.empty()) {
        std::string victim = catidx_registry_lru_.front();
        catidx_registry_lru_.erase(catidx_registry_lru_.begin());
        catidx_registry_.erase(victim); // ~CatIdxFile() closes the fd -- no flush needed, already durable (write-through UpdateRow)
    }

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

void XStore::ClearCatIdxRegistryForTests() {
    std::lock_guard<std::mutex> lock(catidx_registry_mutex_);
    catidx_registry_.clear();
    catidx_registry_lru_.clear();
}

void XStore::CompactOneYearCatIdx(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                   uint32_t tracked_column_count) {
    std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if (!std::filesystem::exists(data_path)) return;

    {
        // Reference scoped tightly to this idempotency check only -- do NOT
        // hold it across the row scan below. The scan can run for minutes
        // over a large year file, it never touches the registry (it reads
        // the raw .XSTORE fd directly), and GetOrOpenCatIdx's registry can
        // evict+destroy any CatIdxFile instance (including this one) under
        // CATIDX_REGISTRY_CAP pressure from concurrent activity elsewhere --
        // holding the reference across the whole scan would risk it dangling
        // by the time AppendBitmapSection is finally called.
        CatIdxFile &catidx = GetOrOpenCatIdx(db_base_path, year, row_length, tracked_column_count);
        if (catidx.HasBitmapSection()) return; // already compacted, idempotent no-op
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

    // Re-acquire FRESH here, immediately before the append -- see the scoped
    // block above. This isn't a fully airtight fix (that would need
    // GetOrOpenCatIdx to return shared_ptr instead of a reference, rippling
    // into every already-shipped caller across Tasks 5-9 -- out of scope for
    // this fix round), but it shrinks the dangling-reference exposure window
    // from "the entire multi-minute scan" down to just this one call, and
    // CATIDX_REGISTRY_CAP=256 makes eviction rare in realistic single-node
    // workloads.
    CatIdxFile &catidx = GetOrOpenCatIdx(db_base_path, year, row_length, tracked_column_count);
    catidx.AppendBitmapSection(surviving);
}

void XStore::RebuildOneYearCatIdxBloom(const std::string &db_base_path, unsigned short year, uint16_t row_length,
                                        uint32_t tracked_column_count) {
    std::string data_path = fmt::format("{}/{}.XSTORE", db_base_path, year);
    if (!std::filesystem::exists(data_path)) return;

    // Force a genuine cache miss for this (db_base_path, year) key before
    // reopening -- same registry-staleness fix RebuildOneYearZoneMap applies
    // to zone_map_registry_ (see its comment in XStore_Management.cpp for
    // the full reasoning). The caller (RebuildStaleCatIdxForDb) has just
    // unlinked the on-disk .catidx file; if a warm registry entry for this
    // exact key still exists (e.g. from an insert earlier in this same
    // InitDB() pass, or an earlier compaction), GetOrOpenCatIdx's cache-hit
    // branch below would hand back that instance WITHOUT ever calling
    // Open() -- so no new file would ever be created on disk here at all.
    // Every bit this function sets below would then live only in that
    // instance's still-open fd to the now-unlinked inode: invisible on disk,
    // and lost for good the moment that fd is ever closed (LRU eviction,
    // test teardown, or process exit) -- silently reintroducing the exact
    // all-zero-bloom false-negative bug this function exists to fix, just
    // deferred to the next genuinely cold process instead of firing
    // immediately.
    {
        std::string registry_key = fmt::format("{}/{}", db_base_path, year);
        std::lock_guard<std::mutex> registry_lock(catidx_registry_mutex_);
        catidx_registry_.erase(registry_key);
        catidx_registry_lru_.erase(
            std::remove(catidx_registry_lru_.begin(), catidx_registry_lru_.end(), registry_key),
            catidx_registry_lru_.end());
    }

    // Real disk-backed Open() at the caller's authoritative tracked_column_count.
    // The on-disk file was just deleted by RebuildStaleCatIdxForDb, so this
    // always takes CatIdxFile::Open's WriteFreshHeader (all-zero bloom)
    // branch -- the row scan below is what genuinely repopulates it.
    CatIdxFile &catidx = GetOrOpenCatIdx(db_base_path, year, row_length, tracked_column_count);

    struct stat st{};
    int fd = open(data_path.c_str(), O_RDONLY);
    if (fd < 0) return;
    fstat(fd, &st);
    uint64_t total_rows = row_length > 0 ? static_cast<uint64_t>(st.st_size) / row_length : 0;

    std::vector<char> row_buf(row_length);
    bool scan_complete = true;
    for (uint64_t r = 0; r < total_rows; ++r) {
        ssize_t read_bytes = pread(fd, row_buf.data(), row_length, static_cast<off_t>(r) * row_length);
        if (read_bytes != static_cast<ssize_t>(row_length)) {
            scan_complete = false;
            continue;
        }
        // Never-written sparse hole -- same fast skip RebuildOneYearZoneMap/
        // ProcessFileRangeAccumulate use. Load-bearing for performance here
        // specifically (unlike CompactOneYearCatIdx, which only ever runs
        // off a background thread): this function runs SYNCHRONOUSLY inside
        // InitDB(), before any worker thread can serve a request, so
        // skipping the ~tracked_column_count ExtractColumnByIndexSkipParse
        // calls per row for the vast majority-sparse rows in a real year
        // file is what keeps a restart from stalling for minutes per stale
        // year.
        if (row_buf[0] == 0) continue;

        std::vector<CatIdxObservation> observations;
        observations.reserve(tracked_column_count);
        for (uint32_t c = 0; c < tracked_column_count; ++c) {
            XStore_Proto::FData f_data;
            if (ExtractColumnByIndexSkipParse(row_buf.data(), row_length, static_cast<unsigned short>(c), f_data)) {
                observations.push_back({c, EncodeFDataForCatIdx(f_data), true});
            } else {
                observations.push_back({c, std::string(), false});
            }
        }
        catidx.UpdateRow(r, observations);
    }
    // Durability A (DD4): data file first, then clear the index's dirty flag
    // (see RebuildOneYearZoneMap). `catidx` is the registry-open instance.
    if (!scan_complete) {
        spdlog::error("[CATIDX REBUILD] [{}/{}.XSTORE] - Incomplete scan; leaving catidx dirty", db_base_path, year);
    } else if (fdatasync(fd) != 0) {
        spdlog::error("[CATIDX REBUILD] [{}/{}.XSTORE] - fdatasync of data file failed; leaving catidx dirty", db_base_path, year);
    } else if (!catidx.MarkClean()) {
        spdlog::error("[CATIDX REBUILD] [{}/{}.catidx] - MarkClean failed; leaving catidx dirty", db_base_path, year);
    }
    close(fd);

    spdlog::info("[CATIDX REBUILD] [{}/{}.XSTORE] - Rebuilt bloom for {} rows at tracked_column_count {}",
                 db_base_path, year, total_rows, tracked_column_count);
}

void XStore::RebuildStaleCatIdxForDb(const std::string &db_name, const std::string &db_base_path,
                                      const XStore_Proto::ConfigStruct &cfg_struct,
                                      const std::set<unsigned short> &force_rebuild_years) {
    uint32_t desired_tracked_columns = EffectiveTrackedColumnCount(cfg_struct);
    uint16_t row_length = static_cast<uint16_t>(cfg_struct.rowlength());

    // Pass 1 (read-only): enumerate <YEAR>.XSTORE data files, not .catidx
    // sidecars -- a year with real row data but no .catidx at all (every
    // pre-existing year, the first time this feature lands on old data)
    // would never be seen by a .catidx-driven iteration at all, and would
    // then only get a lazy, all-zero, permanently-false-negative .catidx
    // the first time FindByValue happened to touch it. Collected into a
    // vector here rather than acted on inline -- mutating db_base_path
    // (the remove+rescan in pass 2) while a directory_iterator over that
    // same directory is still alive is unspecified behavior.
    std::vector<unsigned short> years_needing_rebuild;
    for (auto &f : std::filesystem::directory_iterator(db_base_path)) {
        std::string filename = f.path().filename().string();
        if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;
        std::string year_str = filename.substr(0, filename.size() - 7);
        unsigned short year;
        try { year = static_cast<unsigned short>(std::stoi(year_str)); } catch (...) { continue; }

        bool stale;
        if (force_rebuild_years.count(year) > 0) {
            // WAL-replayed year: catidx_observations is never repopulated
            // by ParseWalRecord, so ApplyWalRecord's UpdateRow(diff, {})
            // sets zero bloom bits for these rows -- but the header stays
            // structurally fine (right magic/version/width), so none of
            // the checks below would ever catch it. Unconditional rebuild,
            // same as RebuildStaleZoneMapsForDb's identical fix.
            stale = true;
        } else {
            std::string catidx_path = fmt::format("{}/{}.catidx", db_base_path, year);
            if (!std::filesystem::exists(catidx_path)) {
                // No sidecar at all -- the exact gap this fix round closes.
                stale = true;
            } else {
                int fd = open(catidx_path.c_str(), O_RDONLY);
                if (fd < 0) {
                    stale = true;
                } else {
                    CatIdxFileHeader on_disk{};
                    bool readable = pread(fd, &on_disk, sizeof(on_disk), 0) == static_cast<ssize_t>(sizeof(on_disk));
                    close(fd);
                    stale = !readable || on_disk.magic != CatIdxFileHeader{}.magic ||
                            on_disk.version != XStoreTunables::CATIDX_VERSION ||
                            on_disk.tracked_column_count < desired_tracked_columns;
                    if (!stale && on_disk.dirty != 0) {
                        // Durability A: unclean shutdown -- treat like a stale width.
                        spdlog::info("[CATIDX REBUILD] [{}] - marked dirty (unclean shutdown) -- rebuilding", catidx_path);
                        stale = true;
                    }
                }
            }
        }
        if (stale) years_needing_rebuild.push_back(year);
    }

    // Pass 2 (mutating, sequential -- deliberately not parallelized): the
    // directory_iterator above has gone out of scope, so remove+rescan is
    // now safe. std::filesystem::remove(path, ec) is a documented no-op
    // (returns false, no exception) when the path doesn't exist, so this
    // is correct whether or not this year ever had a .catidx to begin with.
    for (unsigned short year : years_needing_rebuild) {
        std::string catidx_path = fmt::format("{}/{}.catidx", db_base_path, year);
        spdlog::info("[CATIDX REBUILD] [DB: {}] - rebuilding stale/missing {}.catidx", db_name, year);
        std::error_code remove_ec;
        std::filesystem::remove(catidx_path, remove_ec);
        RebuildOneYearCatIdxBloom(db_base_path, year, row_length, desired_tracked_columns);
    }
}

void XStore::CompactColdCatIdxForAllDbs(const std::string &data_store_dir) {
    unsigned short current_year = TimeStruct(static_cast<std::time_t>(std::time(nullptr)), XStore_Proto::SECOND).year;
    if (!std::filesystem::exists(data_store_dir)) return;
    for (auto &p : std::filesystem::directory_iterator(data_store_dir)) {
        if (!p.is_directory()) continue;
        std::string db_base_path = p.path().string();

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
            // corpus finishes. Doesn't interrupt a scan already in
            // progress (CompactOneYearCatIdx's own row loop doesn't check
            // this) -- just "finish the current year, don't start another".
            if (catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) return;
            std::string filename = f.path().filename().string();
            if (filename.size() < 7 || filename.substr(filename.size() - 7) != ".XSTORE") continue;
            unsigned short year;
            try { year = static_cast<unsigned short>(std::stoi(filename.substr(0, filename.size() - 7))); }
            catch (...) { continue; }
            if (year >= current_year) continue; // only cold (closed) years

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
        while (!XStore::catidx_compaction_stop_flag_.load(std::memory_order_relaxed)) {
            compaction_store.CompactColdCatIdxForAllDbs(data_store_dir);
            for (uint64_t waited = 0; waited < XStoreTunables::CATIDX_COMPACTION_POLL_INTERVAL_SEC &&
                 !XStore::catidx_compaction_stop_flag_.load(std::memory_order_relaxed); ++waited) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    });
    // Same atexit-at-spawn-time ordering argument as StartWalWriters: register
    // the join here so it runs before any static logger/queue teardown.
    std::atexit([]() {
        XStore::catidx_compaction_stop_flag_.store(true, std::memory_order_relaxed);
        if (XStore::catidx_compaction_thread_.joinable()) {
            XStore::catidx_compaction_thread_.join();
        }
    });
}
