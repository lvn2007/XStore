#pragma once
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <string>

#include "include/DataStructs/FileRangeLock.h"
#include "include/XStore/XStore_Tunables.h"
#include "include/XStore/XStore_ZoneMap.h"

// How a range scan (aggregates, RangeQuery, FindByValue, FindByPattern) holds its READ range lock. A WRITE on an overlapping
// range (the WAL apply of an insert) waits for every READ on it, so a scan that holds its whole range for its whole run holds
// off the inserts into that range for as long as it runs (Tech Reference 11; measured in
// XStoreTunables::SCAN_READ_LOCK_CHUNK_BYTES' comment). ScanUnderReadLock takes the lock one chunk at a time instead. The price:
// a scan that spans a multi-row record may see it applied in one chunk and not yet in the next (each row stays consistent).
namespace xstore::scanlock {

struct Config {
    uint64_t chunk_bytes = XStoreTunables::SCAN_READ_LOCK_CHUNK_BYTES;   // 0 = the whole range under one lock
};

namespace detail {
inline std::mutex &ConfigMutex() { static std::mutex m; return m; }
inline Config &Stored() { static Config c; return c; }
inline Config FromEnv() {
    Config c;
    if (const char *env = std::getenv("XSTORE_SCAN_LOCK_CHUNK_MB")) c.chunk_bytes = std::strtoull(env, nullptr, 10) << 20;
    return c;
}
inline std::function<void(uint64_t)> &Hook() { static std::function<void(uint64_t)> h; return h; }
}  // namespace detail

inline Config Get() {
    static const bool initialised = (detail::Stored() = detail::FromEnv(), true);
    (void)initialised;
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    return detail::Stored();
}
inline void Set(const Config &config) {
    (void)Get();   // the env default must not overwrite this later
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    detail::Stored() = config;
}
// Called with the chunk number after each chunk, with no lock held (tests: a writer asks for a range mid-scan).
inline void SetChunkHookForTests(std::function<void(uint64_t)> hook) {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    detail::Hook() = std::move(hook);
}
inline std::function<void(uint64_t)> ChunkHook() {
    std::lock_guard<std::mutex> lock(detail::ConfigMutex());
    return detail::Hook();
}

// Runs body(rows_base, rows, row_offset) over the `region_length` bytes at `first_offset` of the mapped year file, each call
// under a READ lock on its chunk only. row_offset is the first row of the call counted from the start of the region; the
// calls are in order and together cover the region once. body returns false to stop the scan. after() runs after each
// chunk's lock is released: the place to send output the chunk produced (a slow client must not hold a lock).
// first_row_in_year places the chunk boundaries on multiples of the chunk's rows, which are whole zone-map blocks, so a
// block-pruning kernel still sees whole blocks.
template <typename Body, typename After>
void ScanUnderReadLock(const std::string &file_path, void *mapped_base, unsigned long long first_offset,
                       unsigned long long region_length, uint16_t row_length, unsigned long long first_row_in_year,
                       Body &&body, After &&after) {
    static const long PAGE_SIZE = sysconf(_SC_PAGE_SIZE);
    const auto advise = [&](unsigned long long offset, unsigned long long length) {
        char *span_start = static_cast<char *>(mapped_base) + offset;
        uintptr_t aligned_addr = reinterpret_cast<uintptr_t>(span_start) & ~(static_cast<uintptr_t>(PAGE_SIZE) - 1);
        size_t align_slop = reinterpret_cast<uintptr_t>(span_start) - aligned_addr;
        madvise(reinterpret_cast<void *>(aligned_addr), length + align_slop, MADV_WILLNEED);
    };
    const auto rows_at = [&](unsigned long long offset) { return static_cast<const char *>(mapped_base) + offset; };
    const uint64_t chunk_bytes = Get().chunk_bytes;
    const unsigned long long rows_total = region_length / row_length;

    if (chunk_bytes == 0) {
        {
            FileRangeLockGuard guard(file_path, first_offset, region_length, FileRangeLock::LockType::READ);
            // Bulk prefetch hint before the row-by-row walk: one madvise() covering the whole segment instead of one page fault
            // per first-touched page as the scan goes forward.
            advise(first_offset, region_length);
            body(rows_at(first_offset), rows_total, 0);
        }
        after();
        return;
    }

    const unsigned long long rows_per_block = RowsPerBlock(row_length);
    unsigned long long chunk_rows = std::max<unsigned long long>(1, chunk_bytes / row_length);
    chunk_rows = ((chunk_rows + rows_per_block - 1) / rows_per_block) * rows_per_block;
    const std::function<void(uint64_t)> hook = ChunkHook();
    uint64_t chunk_number = 0;
    for (unsigned long long row = 0; row < rows_total; ++chunk_number) {
        // The chunk ends at the next multiple of chunk_rows of the year's row numbers (or the end of the region).
        const unsigned long long end = std::min(rows_total, row + (chunk_rows - (first_row_in_year + row) % chunk_rows));
        const unsigned long long offset = first_offset + row * row_length;
        const unsigned long long length = (end - row) * row_length;
        bool more = true;
        {
            FileRangeLockGuard guard(file_path, offset, length, FileRangeLock::LockType::READ);
            advise(offset, length);
            more = body(rows_at(offset), end - row, row);
        }
        after();
        if (hook) hook(chunk_number);
        if (!more) return;
        row = end;
    }
}

}  // namespace xstore::scanlock
