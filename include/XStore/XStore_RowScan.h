#pragma once
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <vector>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

// Walks the rows of an open year-file fd in ascending order, calling
// fn(row_index, row_ptr) for every row that overlaps a data extent. Year
// files are sparse (e.g. 700k real rows in 31.5M); rows lying wholly inside
// a hole are never read or visited -- callers keep their own row[0]==0 check
// since a visited row may still be all zero (hole edge inside a filesystem
// block, or a zero-filled preallocated block).
//
// SEEK_DATA/SEEK_HOLE report filesystem-block granularity, so extents are
// widened to whole rows (floor start, ceil end, clipped to total_rows); a
// row straddling two extents is visited once (rows below the cursor are
// never revisited). If SEEK_DATA is unsupported (EINVAL) the remainder is
// treated as one data extent. row_ptr is valid only during the call.
//
// Only rows in [first_row, total_rows) are visited (first_row > 0 scans one
// chunk of a year; the EOF check below still covers all of [0, total_rows)).
// A caller scanning a year in chunks passes total_rows = the chunk's end and a
// `scratch` vector it keeps: the read buffer then lives there (grown to
// CHUNK_BYTES once) instead of being allocated on every call.
//
// Returns true only if every visited row was read in full and every lseek
// succeeded. A row that cannot be read is skipped (scan continues with the
// next row) and the result becomes false.
namespace rowscan {
inline constexpr size_t CHUNK_BYTES = 1u << 20;
}

namespace rowscan {
// Seek policy: lets tests force the SEEK_DATA-unsupported branch; production
// uses the default, which inlines to a bare lseek.
struct SystemSeek {
    static off_t Seek(int fd, off_t offset, int whence) { return lseek(fd, offset, whence); }
};
} // namespace rowscan

template <class Seek = rowscan::SystemSeek, class Fn>
[[nodiscard]] bool ForEachDataRow(int fd, uint64_t total_rows, uint16_t row_length, Fn &&fn, uint64_t first_row = 0,
                                  std::vector<char> *scratch = nullptr) {
    if (total_rows <= first_row) return true;
    if (row_length == 0) return false;

    const uint64_t row_len = row_length;
    const uint64_t total_bytes = total_rows * row_len;
    const uint64_t chunk_rows = std::max<uint64_t>(1, rowscan::CHUNK_BYTES / row_len);
    std::vector<char> local;
    std::vector<char> &buf = scratch != nullptr ? *scratch : local;
    if (buf.size() < chunk_rows * row_len) buf.resize(static_cast<size_t>(chunk_rows * row_len));

    // Rows past EOF can never be read (the per-row loop reported each as a
    // short read); flag the scan incomplete instead of silently ending early.
    const off_t file_size = Seek::Seek(fd, 0, SEEK_END);
    if (file_size < 0) return false;
    bool complete = static_cast<uint64_t>(file_size) >= total_bytes;
    uint64_t cursor = first_row; // first row not yet visited
    while (cursor < total_rows) {
        const uint64_t search_from = cursor * row_len;
        uint64_t data_off = search_from;
        uint64_t hole_off = total_bytes;
        const off_t data = Seek::Seek(fd, static_cast<off_t>(search_from), SEEK_DATA);
        if (data >= 0) {
            data_off = static_cast<uint64_t>(data);
            const off_t hole = Seek::Seek(fd, data, SEEK_HOLE);
            if (hole >= 0) hole_off = std::min<uint64_t>(static_cast<uint64_t>(hole), total_bytes);
        } else if (errno == ENXIO) {
            break;
        } else if (errno != EINVAL) {
            return false;
        }
        if (data_off >= total_bytes) break;

        uint64_t row = std::max(cursor, data_off / row_len);
        const uint64_t end_row = std::min(total_rows, (hole_off + row_len - 1) / row_len);
        cursor = std::max(cursor, end_row);

        while (row < end_row) {
            const uint64_t want_rows = std::min(chunk_rows, end_row - row);
            const size_t want_bytes = static_cast<size_t>(want_rows * row_len);
            size_t got = 0;
            while (got < want_bytes) {
                const ssize_t n = pread(fd, buf.data() + got, want_bytes - got,
                                        static_cast<off_t>(row * row_len + got));
                if (n > 0) {
                    got += static_cast<size_t>(n);
                } else if (n < 0 && errno == EINTR) {
                    continue;
                } else {
                    break;
                }
            }
            const uint64_t full_rows = got / row_len;
            for (uint64_t i = 0; i < full_rows; ++i) {
                fn(row + i, static_cast<const char *>(buf.data() + i * row_len));
            }
            row += full_rows;
            if (full_rows < want_rows) {
                complete = false;
                ++row; // unreadable row: skip it and carry on, as the per-row loop did
            }
        }
    }
    return complete;
}
