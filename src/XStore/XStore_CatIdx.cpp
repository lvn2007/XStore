#include "XStore/XStore_CatIdx.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstring>

#include <spdlog/spdlog.h>

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
            return true;
        }
    }

    return WriteFreshHeader(row_length, tracked_column_count);
}

bool CatIdxFile::WriteFreshHeader(uint16_t /*row_length*/, uint32_t tracked_column_count) {
    header_ = CatIdxFileHeader{};
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

    struct stat st{};
    if (fstat(fd_, &st) == 0 && st.st_size >= offset + static_cast<off_t>(bytes_to_read)) {
        // Existing on-disk block -- load its current (always-durable) bloom bytes.
        [[maybe_unused]] ssize_t read_bytes = pread(fd_, entries.data(), bytes_to_read, offset);
    } else {
        // Never-touched block -- zero-initialize and durably extend the file so
        // later block-count math never has to guess whether this block exists.
        for (auto &e : entries) e.fill(0);
        if (ftruncate(fd_, offset + static_cast<off_t>(bytes_to_read)) != 0) {
            spdlog::error("[CATIDX] - Failed to extend file for block {}", block_index);
        }
        ssize_t written = pwrite(fd_, entries.data(), bytes_to_read, offset);
        if (written != static_cast<ssize_t>(bytes_to_read)) {
            spdlog::error("[CATIDX] - Short/failed write zero-filling block {}", block_index);
        }
        if (block_index >= header_.block_count) {
            header_.block_count = static_cast<uint32_t>(block_index + 1);
            [[maybe_unused]] ssize_t header_written = pwrite(fd_, &header_, sizeof(header_), 0);
        }
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
    off_t offset = BloomSectionFileOffset(block_index);
    size_t bytes = header_.tracked_column_count * sizeof(BloomEntry);
    ssize_t written = pwrite(fd_, it->second.data(), bytes, offset);
    if (written != static_cast<ssize_t>(bytes)) {
        // Write-through is the whole point (see UpdateRow) -- a short/failed
        // write here means the in-memory cache_ entry is now ahead of what's
        // durable on disk. Log-and-continue, matching ZoneMapFile::UpdateRow's
        // handling of the equivalent failure (UpdateRow's signature has no
        // failure-reaction path in this plan).
        spdlog::error("[CATIDX] - Short/failed write to block {}", block_index);
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

CatIdxFile::~CatIdxFile() {
    if (fd_ >= 0) close(fd_);
}
