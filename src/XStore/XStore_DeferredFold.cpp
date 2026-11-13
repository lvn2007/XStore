#include "include/XStore/XStore.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <unistd.h>

namespace {

constexpr size_t FOLD_SLICE_BYTES = size_t{1} << 20;   // the bound the live widen holds a READ lock for

// Reads len bytes at offset; bytes past the end of the file read as zero (rows beyond EOF are holes).
bool PreadOrZero(int fd, char *buf, size_t len, off_t offset) {
    size_t done = 0;
    while (done < len) {
        const ssize_t n = pread(fd, buf + done, len - done, offset + static_cast<off_t>(done));
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) break;
        done += static_cast<size_t>(n);
    }
    if (done < len) std::memset(buf + done, 0, len - done);
    return true;
}

}  // namespace

bool XStore::FoldStoredRows(int data_fd, uint16_t row_length, uint32_t tracked_column_count, uint32_t catidx_column_limit,
                            const std::vector<xstore::IndexRun> &runs, ZoneMapFile &zmap, CatIdxFile &catidx) {
    thread_local RebuildScratch scratch;
    const uint64_t slice_rows = std::max<uint64_t>(1, FOLD_SLICE_BYTES / row_length);
    for (const xstore::IndexRun &run : runs) {
        for (uint64_t done = 0; done < run.row_count; done += slice_rows) {
            const uint64_t rows = std::min<uint64_t>(slice_rows, run.row_count - done);
            const uint64_t first_row = run.first_row + done;
            const size_t bytes = static_cast<size_t>(rows) * row_length;
            if (scratch.scan_buffer.size() < bytes) scratch.scan_buffer.resize(bytes);
            if (!PreadOrZero(data_fd, scratch.scan_buffer.data(), bytes, static_cast<off_t>(first_row * row_length))) return false;
            scratch.zmap.Clear();
            scratch.catidx.Clear();
            scratch.zmap.pad_to = tracked_column_count;
            for (uint64_t r = 0; r < rows; ++r) {
                const char *row = scratch.scan_buffer.data() + r * row_length;
                if (row[0] == 0) continue;
                AppendRebuildObservations(first_row + r, row, row_length, tracked_column_count, &scratch.zmap, &scratch.catidx,
                                          scratch);
            }
            if (scratch.zmap.row_index.empty()) continue;   // the whole slice was holes
            zmap.UpdateRows(scratch.zmap);
            CatIdxFoldView view = scratch.catidx.View();
            view.column_limit = catidx_column_limit;
            catidx.UpdateRows(view);
        }
    }
    return true;
}
