#include "include/XStore/XStore.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/wire_format_lite.h>

std::array<XStore::MmapShard, XStore::MMAP_SHARD_COUNT> XStore::mmap_shards;
std::array<XStore::FdShard, XStore::FD_SHARD_COUNT> XStore::fd_shards;

namespace {

// Shared helpers for the flat MRU-array shards (MmapShard, FdShard -- see
// XStore.h). SHARD_CAP is tiny (2-4) for both, so a linear scan over one
// contiguous array beats a std::list node + std::unordered_map bucket node
// per entry: no per-touch heap allocation, and the whole shard's live
// entries typically fit in 1-2 cache lines instead of being scattered
// across independently-allocated list/hashmap nodes.

// Returns the index of `key` in `shard.entries[0, shard.count)`, or
// `shard.count` (one past the last live slot) if not found -- callers
// compare the result against `shard.count` the same way they'd compare an
// iterator against `.end()`.
template <typename Shard>
size_t FindIndex(const Shard &shard, const std::string &key) {
    for (size_t i = 0; i < shard.count; ++i) {
        if (shard.entries[i].key == key) {
            return i;
        }
    }
    return shard.count;
}

// Moves entries[idx] to entries[0], shifting entries[0, idx) right by one.
// Every intermediate assignment is a move, so whatever used to live at the
// slot being overwritten is released (its shared_ptr refcount drops) at
// that assignment -- no entry is ever left dangling in a slot >= count.
template <typename Shard>
void PromoteToFront(Shard &shard, size_t idx) {
    if (idx == 0) {
        return;
    }
    typename Shard::Entry hit = std::move(shard.entries[idx]);
    for (size_t j = idx; j > 0; --j) {
        shard.entries[j] = std::move(shard.entries[j - 1]);
    }
    shard.entries[0] = std::move(hit);
}

// Inserts `entry` at entries[0], shifting existing entries right by one. If
// the shard is already at capacity, the last (least-recently-used) slot is
// overwritten by the shift -- its old shared_ptr is released by that move
// assignment, same as a real eviction. Otherwise `shard.count` grows by one.
template <typename Shard>
void InsertAtFront(Shard &shard, typename Shard::Entry entry) {
    size_t shift_end = (shard.count == shard.entries.size()) ? shard.entries.size() - 1 : shard.count;
    for (size_t j = shift_end; j > 0; --j) {
        shard.entries[j] = std::move(shard.entries[j - 1]);
    }
    shard.entries[0] = std::move(entry);
    if (shard.count < shard.entries.size()) {
        ++shard.count;
    }
}

// Removes entries[idx], shifting entries(idx, count) left by one and
// clearing the now-vacated last live slot so its shared_ptr is released
// immediately (the shift loop already does this for every idx except the
// last live slot, which no iteration touches).
template <typename Shard>
void EraseAtIndex(Shard &shard, size_t idx) {
    for (size_t j = idx; j + 1 < shard.count; ++j) {
        shard.entries[j] = std::move(shard.entries[j + 1]);
    }
    shard.entries[shard.count - 1] = typename Shard::Entry{};
    --shard.count;
}

// Drops every entry whose key starts with `prefix`, compacting survivors
// down to entries[0, new_count) in their original relative (MRU) order.
// Slots past the new count are explicitly cleared, whether they held a
// dropped entry or were the move-source of a kept one -- otherwise a
// dropped entry that's never overwritten by a later keep would sit past
// `count` with its shared_ptr still live, holding the resource open
// forever instead of releasing it as soon as it's erased.
template <typename Shard>
void ErasePrefix(Shard &shard, const std::string &prefix) {
    size_t write = 0;
    for (size_t read = 0; read < shard.count; ++read) {
        if (shard.entries[read].key.compare(0, prefix.size(), prefix) == 0) {
            continue;
        }
        if (write != read) {
            shard.entries[write] = std::move(shard.entries[read]);
        }
        ++write;
    }
    for (size_t j = write; j < shard.count; ++j) {
        shard.entries[j] = typename Shard::Entry{};
    }
    shard.count = write;
}

} // namespace

std::shared_ptr<void> XStore::GetOrMapFile(const std::string &target_file, size_t row_length, unsigned long long total_rows) {
    MmapShard &shard = mmap_shards[std::hash<std::string>{}(target_file) % MMAP_SHARD_COUNT];

    // Fast path: already mapped -- only the lookup+MRU-promote touches this
    // shard's state, so the lock is held just long enough for that, and
    // only contends with other threads hashing to the same shard.
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        size_t idx = FindIndex(shard, target_file);
        if (idx != shard.count) {
            std::shared_ptr<MappedFile> owner = shard.entries[idx].value;
            PromoteToFront(shard, idx);
            return std::shared_ptr<void>(owner, owner->base);
        }
    }

    // Cache miss -> open (create if needed), ensure size, map. Done WITHOUT
    // the lock: open/fstat/ftruncate/mmap are syscalls whose cost scales
    // with file size and disk state, and holding this shard's mutex across
    // them would serialize every other thread hashing to the same shard --
    // to any DB, not just this one -- for that syscall's whole duration. Two
    // threads racing to first-touch the same file both pay this cost
    // unlocked; the loser's redundant mapping is dropped below.
    int fd = open(target_file.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return nullptr;
    }

    // +1 row: totalRows is (LastEpoch - FirstEpoch), but a write at
    // unixTime == LastEpoch (the year's final second, inclusive) lands at
    // offset totalRows*rowLength -- one row past that span. The original
    // fwrite()-based writer silently grew the file to cover this via
    // EOF-extension; a fixed-size mapping needs the extra row up front.
    size_t file_len = (static_cast<size_t>(total_rows) + 1) * row_length;

    struct stat st{};
    if (fstat(fd, &st) != 0 || static_cast<size_t>(st.st_size) < file_len) {
        if (ftruncate(fd, file_len) != 0) {
            close(fd);
            return nullptr;
        }
    }

    void *base = mmap(NULL, file_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    // Mapping stays valid after the fd is closed -- no need to keep it open.
    close(fd);
    if (base == MAP_FAILED) {
        return nullptr;
    }

    // Deleter fires munmap() only once every shared_ptr copy -- the cache's
    // own entry AND any in-flight caller still holding one from an earlier
    // GetOrMapFile() -- has been released (see MmapShard's comment).
    std::shared_ptr<MappedFile> owner(new MappedFile{base, file_len}, [](MappedFile *mf) {
        munmap(mf->base, mf->len);
        delete mf;
    });

    // Re-acquire to install into the cache, double-checked: another thread
    // may have raced us to map this same file while we were unlocked.
    std::lock_guard<std::mutex> lock(shard.mutex);
    size_t idx = FindIndex(shard, target_file);
    if (idx != shard.count) {
        // Lost the race -- our redundant Owner isn't stored anywhere else,
        // so it unmaps itself when it goes out of scope below; use theirs.
        std::shared_ptr<MappedFile> winner = shard.entries[idx].value;
        PromoteToFront(shard, idx);
        return std::shared_ptr<void>(winner, winner->base);
    }

    // Insert-at-front overwrites the LRU tail slot once the shard is at
    // MMAP_SHARD_CAP -- that assignment is the eviction: only the cache's
    // reference is dropped there, if some other thread is still holding a
    // copy of that entry's shared_ptr from an earlier GetOrMapFile() call,
    // the deleter (and its munmap()) doesn't run until that copy is
    // released too.
    InsertAtFront(shard, MmapShard::Entry{target_file, owner});

    return std::shared_ptr<void>(owner, owner->base);
}

std::shared_ptr<int> XStore::GetOrOpenFile(const std::string &target_file, size_t row_length, unsigned long long total_rows) {
    FdShard &shard = fd_shards[std::hash<std::string>{}(target_file) % FD_SHARD_COUNT];

    // Fast path: already open -- only the lookup+MRU-promote touches this
    // shard's state, mirrors GetOrMapFile's fast path.
    {
        std::lock_guard<std::mutex> lock(shard.mutex);
        size_t idx = FindIndex(shard, target_file);
        if (idx != shard.count) {
            std::shared_ptr<int> owner = shard.entries[idx].value;
            PromoteToFront(shard, idx);
            return owner;
        }
    }

    // Cache miss -> open (create if needed), ensure size. Done WITHOUT the
    // lock for the same reason as GetOrMapFile: these are syscalls whose
    // cost shouldn't serialize every other thread's inserts.
    int fd = open(target_file.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd < 0) {
        return nullptr;
    }

    // +1 row: see GetOrMapFile for why.
    size_t file_len = (static_cast<size_t>(total_rows) + 1) * row_length;

    struct stat st{};
    if (fstat(fd, &st) != 0 || static_cast<size_t>(st.st_size) < file_len) {
        if (ftruncate(fd, file_len) != 0) {
            close(fd);
            return nullptr;
        }
    }

    // Deleter fires close() only once every shared_ptr copy -- the cache's
    // own entry AND any in-flight caller still holding one from an earlier
    // GetOrOpenFile() -- has been released (see FdShard's comment).
    std::shared_ptr<int> owner(new int(fd), [](int *fd_ptr) {
        close(*fd_ptr);
        delete fd_ptr;
    });

    // Re-acquire to install into the cache, double-checked: another thread
    // may have raced us to open this same file while we were unlocked.
    std::lock_guard<std::mutex> lock(shard.mutex);
    size_t idx = FindIndex(shard, target_file);
    if (idx != shard.count) {
        // Lost the race -- our redundant Owner isn't stored anywhere else,
        // so it closes itself when it goes out of scope below; use theirs.
        std::shared_ptr<int> winner = shard.entries[idx].value;
        PromoteToFront(shard, idx);
        return winner;
    }

    // Insert-at-front overwrites the LRU tail slot once the shard is at
    // FD_SHARD_CAP -- that assignment is the eviction: only the cache's
    // reference is dropped there, if some other thread is still holding a
    // copy of that entry's shared_ptr from an earlier GetOrOpenFile() call,
    // the deleter (and its close()) doesn't run until that copy is
    // released too.
    InsertAtFront(shard, FdShard::Entry{target_file, owner});

    return owner;
}

bool XStore::IsFdCached(const std::string &target_file) {
    FdShard &shard = fd_shards[std::hash<std::string>{}(target_file) % FD_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.mutex);
    return FindIndex(shard, target_file) != shard.count;
}

void XStore::EraseOpenFile(const std::string &target_file) {
    FdShard &shard = fd_shards[std::hash<std::string>{}(target_file) % FD_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.mutex);

    // Only drops the cache's reference -- see FdShard's comment. If a
    // caller is still mid-pwrite()/pread() on a copy of this fd's
    // shared_ptr, close() is deferred until that copy is released too.
    size_t idx = FindIndex(shard, target_file);
    if (idx == shard.count) {
        return;
    }
    EraseAtIndex(shard, idx);
}

void XStore::EraseOpenFilesUnderPrefix(const std::string &db_dir_prefix) {
    // Same rationale as EraseMappedFilesUnderPrefix -- sweep each shard
    // under its own lock rather than one lock for the whole sweep.
    for (FdShard &shard : fd_shards) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        ErasePrefix(shard, db_dir_prefix);
    }
}

void XStore::EraseMappedFile(const std::string &target_file) {
    MmapShard &shard = mmap_shards[std::hash<std::string>{}(target_file) % MMAP_SHARD_COUNT];
    std::lock_guard<std::mutex> lock(shard.mutex);

    // Only drops the cache's reference -- see MmapShard's comment. If a
    // caller is still mid-read/write on a copy of this mapping's
    // shared_ptr, munmap() is deferred until that copy is released too.
    size_t idx = FindIndex(shard, target_file);
    if (idx == shard.count) {
        return;
    }
    EraseAtIndex(shard, idx);
}

void XStore::EraseMappedFilesUnderPrefix(const std::string &db_dir_prefix) {
    // Prefix sweep doesn't know which shard(s) hold matching files, so it
    // walks all of them -- locked one shard at a time (not all at once),
    // so concurrent GetOrMapFile() calls hashing to other shards aren't
    // blocked for the sweep's duration. Rare/administrative path (DB
    // removal), not a hot path, so this is fine.
    for (MmapShard &shard : mmap_shards) {
        std::lock_guard<std::mutex> lock(shard.mutex);
        ErasePrefix(shard, db_dir_prefix);
    }
}

// Shared scan kernels used by MinAggregate/MaxAggregate/SumAggregate/AvgAggregate.
// rowsBase points directly into GetOrMapFile's cached, already-resident
// mapping -- no per-call mmap()/madvise()/munmap() here anymore (see the
// header comment on the declarations for why).
//
// Each row is extracted via ExtractColumnByIndexSkipParse (below) rather than
// a full RowData::MergePartialFromCodedStream: these kernels only ever want
// ONE column (columnIdx), so full-parsing every column in the row -- and
// allocating an FData for each -- was pure waste for every column that
// wasn't the target. Skip-parse walks the row's wire tags, skips
// (cursor-advances past, no allocation) every column except columnIdx, and
// returns as soon as the target is read -- benchmarked ~40-54% faster than
// the old full-parse kernel across N=100/1k/10k
// (tests/perf/XStorePerfTest.cpp's *SkipParseVsFullParse tests were the A/B
// harness that proved this before promotion; removed now that there's only
// one kernel to measure).

// Tried a two-phase version here: Phase 1 extracts each valid row's value
// into a flat, reused double buffer (narrowed from long double, since the
// column's byte offset varies row-to-row under protobuf's variable-length
// encoding, so extraction itself can't be vectorized -- only a reduction
// over an already-flat buffer could be), Phase 2 scans that buffer for
// min/max. Benchmarked SLOWER (~15-20% on MinAggregate, a wash on Max):
// materializing the whole array costs real write bandwidth in Phase 1 and
// read bandwidth in Phase 2 -- for a 1M-row scan that's ~8MB of double[]
// traffic that doesn't fit L2/L3, round-tripping to DRAM. The original
// single-pass design never pays that: it keeps one scalar "current best" in
// a register while streaming through rows once. Since the reduction step
// was never the bottleneck (extraction/parse dominates per-row cost), there
// was nothing worth vectorizing there to begin with -- reverted after
// measuring.
template <typename Cmp>
void XStore::ProcessFileRangeSelect(const char* rows_base, unsigned long long total_rows,
                                     size_t row_len, unsigned short column_idx,
                                     ValueStruct& best_value, XStore_Proto::FData& best_data) {

    if (total_rows == 0) return;

    XStore_Proto::FData curr_data;
    ValueStruct temp_value;
    const Cmp better{}; // stateless functor, no per-call construction cost

    for (unsigned long long i = 0; i < total_rows; ++i) {
        const char* row_ptr = rows_base + (i * row_len);

        // Instant skip for empty slots
        if (row_ptr[0] == 0) continue;

        curr_data.Clear();
        if (!ExtractColumnByIndexSkipParse(row_ptr, row_len, column_idx, curr_data)) continue;

        if (curr_data.oneOfData_case() > 0 && curr_data.oneOfData_case() <= 12) {
            XStore::GetFDataValue(&curr_data, &temp_value);

            if (!best_value.is_initialized || better(temp_value.value, best_value.value)) {
                best_value.value = temp_value.value;
                best_value.is_initialized = true;
                best_data = curr_data;
            }
        }
    }
}

// Explicit instantiation: these are the only two comparators ever used, so the compiler
// generates two fully-inlined, branch-free (on Cmp) versions -- no vtable/std::function overhead.
template void XStore::ProcessFileRangeSelect<std::less<long double>>(
    const char*, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);
template void XStore::ProcessFileRangeSelect<std::greater<long double>>(
    const char*, unsigned long long, size_t, unsigned short, ValueStruct&, XStore_Proto::FData&);

void XStore::ProcessFileRangeAccumulate(const char* rows_base, unsigned long long total_rows,
                                         size_t row_len, unsigned short column_idx,
                                         double& sum_out, unsigned long long& valid_count_out) {

    if (total_rows == 0) return;

    XStore_Proto::FData curr_data;
    ValueStruct temp_value;

    for (unsigned long long i = 0; i < total_rows; ++i) {
        const char* row_ptr = rows_base + (i * row_len);

        if (row_ptr[0] == 0) continue;

        curr_data.Clear();
        if (!ExtractColumnByIndexSkipParse(row_ptr, row_len, column_idx, curr_data)) continue;

        if (curr_data.oneOfData_case() > 0 && curr_data.oneOfData_case() <= 12) {
            XStore::GetFDataValue(&curr_data, &temp_value);
            sum_out += (double)temp_value.value;
            valid_count_out++;
        }
    }
}

// Extracts just columnIdx's FData from a row's wire bytes instead of parsing
// the whole RowData -- ProcessFileRangeSelect/Accumulate above only ever
// want ONE column, so full-parsing every column (and allocating an FData for
// each) was pure waste for every column that wasn't the target. RowData
// wire-encodes as repeated (tag=field1/LEN, length, bytes) triplets, one per
// column, in column order -- Occurrence below counts those triplets to find
// the columnIdx-th one. Non-target columns are skipped by cursor advance
// only (WireFormatLite::SkipField), no FData object built for them.
//
// Edge cases match what the old full-parse kernel did: a hole/short row
// (fewer columns than columnIdx) is caught by the tag==0 end-of-row check
// below, same as the old fielddata_size() bound check; a present-but-wrong-
// type column (string/bytes/unset) still gets fully decoded here so the
// caller's oneOfData_case() filter downstream sees it unchanged. One
// deliberate difference: because this returns as soon as the target column
// is read, corruption in a LATER column no longer fails the read of an
// earlier, valid target column the way a full-row parse would.
bool XStore::ExtractColumnByIndexSkipParse(const char* row_ptr, size_t row_len,
                                            unsigned short column_idx,
                                            XStore_Proto::FData& out_data) {
    using google::protobuf::internal::WireFormatLite;

    google::protobuf::io::ArrayInputStream array_in(row_ptr, (int)row_len);
    google::protobuf::io::CodedInputStream coded_in(&array_in);

    unsigned int occurrence = 0;
    uint32_t tag;
    // ReadTag() returns 0 once it hits the fixed-width slot's zero padding
    // (no valid message ever encodes field number 0) -- same end-of-row
    // signal the old full-parse path relied on.
    while ((tag = coded_in.ReadTag()) != 0) {
        if (WireFormatLite::GetTagFieldNumber(tag) != XStore_Proto::RowData::kFieldDataFieldNumber) {
            // RowData only ever encodes field 1 -- skip rather than assume
            // this can't happen.
            if (!WireFormatLite::SkipField(&coded_in, tag)) return false;
            continue;
        }

        if (occurrence == column_idx) {
            return WireFormatLite::ReadMessage(&coded_in, &out_data);
        }

        if (!WireFormatLite::SkipField(&coded_in, tag)) return false;
        ++occurrence;
    }

    return false;  // ran out of columns before reaching columnIdx
}

void XStore::GetFDataValue(const XStore_Proto::FData *f_data_input, unsigned int column_idx, ValueStruct *return_data) {
    switch (f_data_input->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            return_data->index = column_idx;
            return_data->value = f_data_input->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            return_data->index = column_idx;
            return_data->value = f_data_input->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            return_data->index = column_idx;
            return_data->value = f_data_input->sfixed64data();
            break;
        }
    }
}

void XStore::GetFDataValue(const XStore_Proto::FData *f_data_input, ValueStruct *return_data) {
    switch (f_data_input->oneOfData_case()) {
        case (XStore_Proto::FData::OneOfDataCase::kDoubleData): {
            return_data->value = f_data_input->doubledata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFloatData): {
            return_data->value = f_data_input->floatdata();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt32Data): {
            return_data->value = f_data_input->int32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kInt64Data): {
            return_data->value = f_data_input->int64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint32Data): {
            return_data->value = f_data_input->uint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kUint64Data): {
            return_data->value = f_data_input->uint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint32Data): {
            return_data->value = f_data_input->sint32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSint64Data): {
            return_data->value = f_data_input->sint64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed32Data): {
            return_data->value = f_data_input->fixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kFixed64Data): {
            return_data->value = f_data_input->fixed64data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed32Data): {
            return_data->value = f_data_input->sfixed32data();
            break;
        }

        case (XStore_Proto::FData::OneOfDataCase::kSfixed64Data): {
            return_data->value = f_data_input->sfixed64data();
            break;
        }
    }
}
